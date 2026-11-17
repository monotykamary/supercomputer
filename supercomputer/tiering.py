"""Cold tiering of exported output files; custody metadata never leaves the controller.

A closed, terminal batch's `queue/BATCH.results/` holds content-addressed payloads:
its audit records (snapshot, close receipt, rows, placement/start/cancellation
journals), the rows' own JSON references (row/result/output-manifest) and the
exported output files those manifests name. Only the last kind is tiered: those
files are streamed (tar | zstd) into an archive on a boat VM, verified there
member by member (each payload hashes to its name), saved as a named boat
snapshot, and replaced locally by a small hash-bound reference, `queue/BATCH.tier`:

    {"protocol": 1, "batch": BATCH, "archives": [{"snapshot", "path", "sha256",
     "bytes", "entries", "moved_at", "payloads": {SHA256: SIZE}}]}

Rows, logs, journals, close receipts and output manifests stay local, so closed
history, strict recover-close and audits read exactly what they read before.
A read of a tiered file raises Hydrating: the scheduler answers 503 (the client
retries the same request) while one worker starts a VM from the snapshot, streams
the archive back, checks its sha256, byte count and entry count and every
member's own sha256, and only then installs the payloads. A missing or tampered
archive raises Unavailable: fail closed, never a partial or substituted file.

Never tiered: open admissions, pending strict cleanup, held or recently read
batches, and the files of failed rows (failure receipts stay local). The
reference expires with the batch's closed history (history.SUFFIXES).
"""
import base64
import contextlib
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import uuid

from . import history, resultfiles
from .rules.tiering import tier_allowed, tier_order_low_first

PROTOCOL = 1
SUFFIX = 'tier'
FAILED_STATUSES = frozenset({'LOST', 'NOT_STARTED', 'FAILED', 'ERROR'})
LOW_CLASSES = frozenset({4, 5})  # sweep, filler: the treadmill's
_SHA = re.compile(r'[0-9a-f]{64}')
_SNAPSHOT = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}')
_PATH = re.compile(r'/[A-Za-z0-9_./-]{1,1023}')
_ARCHIVE_FIELDS = {'snapshot', 'path', 'sha256', 'bytes', 'entries', 'moved_at', 'payloads'}


class Hydrating(RuntimeError):
    """A tiered payload is being restored; the same read is safe to repeat."""


class Unavailable(RuntimeError):
    """A tiered payload cannot be restored with matching hashes: fail closed."""


def reference_path(queue, bid):
    return Path(queue) / f'{bid}.{SUFFIX}'


def _validate_archive(archive):
    if not isinstance(archive, dict) or set(archive) != _ARCHIVE_FIELDS:
        raise ValueError('invalid tier archive fields')
    if not isinstance(archive['snapshot'], str) or not _SNAPSHOT.fullmatch(archive['snapshot']):
        raise ValueError('invalid tier snapshot name')
    path = archive['path']
    if not isinstance(path, str) or not _PATH.fullmatch(path) or '/../' in path + '/' or '/./' in path + '/':
        raise ValueError('invalid tier archive path')
    if not isinstance(archive['sha256'], str) or not _SHA.fullmatch(archive['sha256']):
        raise ValueError('invalid tier archive digest')
    for key in ('bytes', 'entries'):
        if type(archive[key]) is not int or archive[key] < 0:
            raise ValueError(f'invalid tier archive {key}')
    if type(archive['moved_at']) not in (int, float) or not math.isfinite(archive['moved_at']):
        raise ValueError('invalid tier time')
    payloads = archive['payloads']
    if not isinstance(payloads, dict) or not payloads or len(payloads) > archive['entries']:
        raise ValueError('invalid tier payload list')
    for sha, size in payloads.items():
        if not _SHA.fullmatch(sha) or type(size) is not int or size < 0:
            raise ValueError('invalid tier payload entry')
    return archive


def validate(reference, bid):
    if (not isinstance(reference, dict) or set(reference) != {'protocol', 'batch', 'archives'}
            or reference['protocol'] != PROTOCOL or reference['batch'] != bid
            or not isinstance(reference['archives'], list) or not reference['archives']):
        raise ValueError('invalid tier reference')
    for archive in reference['archives']:
        _validate_archive(archive)
    return reference


def load(queue, bid):
    """The batch's reference, None when it has none; malformed is an error, never 'not tiered'."""
    try:
        return validate(history.load(reference_path(queue, bid)), bid)
    except FileNotFoundError:
        return None


def lookup(queue, bid, sha, size=None):
    """The archive record holding this payload (with this size), or None."""
    reference = load(queue, bid)
    for archive in (reference or {}).get('archives', ()):
        if sha in archive['payloads'] and (size is None or archive['payloads'][sha] == size):
            return archive
    return None


def record(queue, bid, archive):
    """Durably add one archive to the batch's reference (before any local payload goes)."""
    archive = _validate_archive(dict(archive))
    reference = load(queue, bid) or {'protocol': PROTOCOL, 'batch': bid, 'archives': []}
    if archive not in reference['archives']:
        reference = {**reference, 'archives': reference['archives'] + [archive]}
    history.atomic(reference_path(queue, bid), validate(reference, bid))
    return reference


def drop_local(queue, bid, payloads):
    """Remove local copies that the durable reference names with the same size; returns bytes freed."""
    reference = load(queue, bid)
    named = {}
    for archive in (reference or {}).get('archives', ()):
        named.update(archive['payloads'])
    freed = 0
    store = Path(queue) / f'{bid}.results'
    for sha, size in payloads.items():
        if named.get(sha) != size:
            raise ValueError('payload is not bound by a durable tier reference')
        path = store / f'{sha}.payload'
        try:
            info = path.lstat()
        except FileNotFoundError:
            continue
        if info.st_size == size and not path.is_symlink():
            path.unlink()
            freed += info.st_blocks * 512
    if freed:
        history.sync(store)
    return freed


# ------------------------------------------------------------------------------------------- what may go cold
def _refs(obj, out):
    if isinstance(obj, dict):
        if isinstance(obj.get('sha256'), str) and {'version', 'bytes', 'format'} <= set(obj):
            out.add(obj['sha256'])
        for value in obj.values():
            _refs(value, out)
    elif isinstance(obj, list):
        for value in obj:
            _refs(value, out)


def failed_row(row):
    return (row.get('code') not in (0, None) or row.get('status') in FAILED_STATUSES
            or row.get('lost') is True or bool(row.get('error')))


def output_files(queue, bid, rows, keep=()):
    """(kept, tierable): payloads that stay local, and the exported files {sha: size} that may go cold."""
    store = resultfiles.Store(Path(queue) / f'{bid}.results')
    kept, files = set(keep), {}
    def read_json(ref):
        with store.open(resultfiles.validate_ref(ref)) as source:
            return json.load(source)
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ('row_ref', 'result_ref', 'output_files_ref'):
            if isinstance(row.get(key), dict):
                kept.add(row[key]['sha256'])
        full = row
        if isinstance(row.get('row_ref'), dict):
            full = read_json(row['row_ref'])
            for key in ('result_ref', 'output_files_ref'):
                if isinstance(full.get(key), dict):
                    kept.add(full[key]['sha256'])
        manifest = row.get('output_files_ref') or full.get('output_files_ref')
        if not isinstance(manifest, dict):
            continue
        bad = failed_row(row) or failed_row(full)
        for entry in read_json(manifest):
            if isinstance(entry, dict) and entry.get('format') == 'file' and isinstance(entry.get('sha256'), str):
                if bad:
                    kept.add(entry['sha256'])  # failure receipts stay local
                else:
                    files[entry['sha256']] = entry['bytes']
    return kept, {sha: size for sha, size in files.items() if sha not in kept}


def candidate(queue, bid):
    """A closed batch's tierable files and order key, or None. Reads only immutable closed history."""
    queue = Path(queue)
    meta = history.read(queue, bid, expired=True)
    if meta is None:
        return None
    record_ = history.load(queue / f'{bid}.closed')
    keep = set()
    _refs(meta.get('audit') or {}, keep)
    with contextlib.suppress(FileNotFoundError, ValueError):
        _refs(json.loads((queue / f'{bid}.results' / '.journal-cache.json').read_text()), keep)
    rows = history.payload(queue, bid, 'rows.json', meta)
    _, files = output_files(queue, bid, rows, keep)
    reference = load(queue, bid)
    tiered = {}
    for archive in (reference or {}).get('archives', ()):
        tiered.update(archive['payloads'])
    present = {}
    for sha, size in files.items():
        path = queue / f'{bid}.results' / f'{sha}.payload'
        with contextlib.suppress(FileNotFoundError):
            if path.lstat().st_size == size:
                present[sha] = size
    return {'batch': bid, 'klass': record_.get('klass'), 'closed_at': meta['closed_at'],
            'expires_at': meta['expires_at'], 'payloads': present,
            'rebound': {sha: size for sha, size in present.items() if tiered.get(sha) == size}}


def select(queue, now, *, min_age_s, max_bytes, held=frozenset(), reading=frozenset(), cache=None):
    """Closed batches' files to tier, the treadmill's (sweep, filler) first, then oldest close first."""
    queue = Path(queue)
    cache = {} if cache is None else cache
    chosen = []
    for path in queue.glob('*.closed'):
        bid = path.name[:-len('.closed')]
        pending = (queue / f'{bid}.json').exists()
        if not tier_allowed(True, pending, bid in held, bid in reading):
            continue
        try:
            if bid not in cache:
                cache[bid] = candidate(queue, bid)
            found = cache[bid]
        except (OSError, ValueError, TypeError, KeyError) as error:
            cache[bid] = None
            found = None
            del error  # unreadable history is never tiered
        if not found or not found['payloads'] or now - found['closed_at'] < min_age_s or now >= found['expires_at']:
            continue
        chosen.append(found)
    chosen.sort(key=lambda c: (tier_order_low_first(c['klass'] in LOW_CLASSES), c['closed_at'], c['batch']))
    out, total = [], 0
    for found in chosen:
        size = sum(found['payloads'].values())
        if out and total + size > max_bytes:
            break
        out.append(found)
        total += size
    return out


# ------------------------------------------------------------------------------------------- the archive VM
VERIFY = r"""
import hashlib, json, subprocess, sys, tarfile
archive, want_path = sys.argv[1], sys.argv[2]
want = {m: tuple(v) for m, v in json.load(open(want_path)).items()}
p = subprocess.Popen(['zstd', '-dc', archive], stdout=subprocess.PIPE)
seen, bad, total = set(), 0, 0
with tarfile.open(fileobj=p.stdout, mode='r|') as tf:
    for m in tf:
        if not m.isfile():
            bad += 1; continue
        h, f, n = hashlib.sha256(), tf.extractfile(m), 0
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk); n += len(chunk)
        if want.get(m.name) != (h.hexdigest(), n) or m.name in seen:
            bad += 1
        seen.add(m.name); total += n
rc = p.wait()
h = hashlib.sha256()
with open(archive, 'rb') as f:
    for chunk in iter(lambda: f.read(1 << 20), b''):
        h.update(chunk)
print(json.dumps({'entries': len(seen), 'bad': bad, 'missing': len(set(want) - seen), 'bytes': total,
                  'rc': rc, 'sha256': h.hexdigest()}))
"""


class BoatRemote:
    """Archive VMs on boat: one start per tier run or hydration, deleted after."""

    def __init__(self, vm_type='small'):
        self.vm_type = vm_type

    def new_vm(self, snapshot=None, ttl=4 * 3600):
        from . import boat
        if snapshot is not None and snapshot not in boat.named_snapshots():
            raise Unavailable(f'tier: snapshot {snapshot} is missing')
        out = boat.boat('new', *(['--from', snapshot] if snapshot else []), '--type', self.vm_type, '--ttl', str(ttl),
                        '--no-env', '--no-monitoring', '--json', timeout=900)
        vm = None
        for line in out.splitlines():
            with contextlib.suppress(ValueError):
                vm = vm or json.loads(line).get('id')
        if not vm or not boat.wait_running([vm], 600):
            raise RuntimeError('tier: no archive VM came up')
        boat.note_starts(1)
        return vm

    def _ssh(self, vm, command, **kw):
        from . import boat
        return subprocess.Popen([boat.BOAT, 'ssh', vm, '--', command], env=boat.ENV, **kw)

    def writer(self, vm, path):
        directory = shlex.quote(os.path.dirname(path))
        return self._ssh(vm, f'mkdir -p {directory} && cat > {shlex.quote(path)}', stdin=subprocess.PIPE,
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def reader(self, vm, path):
        return self._ssh(vm, f'cat {shlex.quote(path)}', stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def mkdir(self, vm, directory):
        q = shlex.quote(directory)
        self.run(vm, f'sudo mkdir -p {q} && sudo chown "$(id -un)" {q}')

    def put(self, vm, path, data):
        self.run(vm, f'cat > {shlex.quote(path)}', stdin=data)

    def verify(self, vm, path, want):
        script = base64.b64encode(VERIFY.encode()).decode()
        self.put(vm, '/tmp/sc-tier-want.json', json.dumps(want).encode())
        out = self.run(vm, f'echo {script} | base64 -d > /tmp/sc-tier-verify.py && '
                           f'python3 -I /tmp/sc-tier-verify.py {shlex.quote(path)} /tmp/sc-tier-want.json')
        return json.loads(out.strip().splitlines()[-1])

    def run(self, vm, command, stdin=b'', timeout=7200):
        from . import boat
        p = subprocess.run([boat.BOAT, 'ssh', vm, '--', command], input=stdin, capture_output=True,
                           env=boat.ENV, timeout=timeout)
        if p.returncode != 0:
            raise RuntimeError(f'tier: remote command failed ({p.returncode}): {p.stderr.decode(errors="replace")[-300:]}')
        return p.stdout.decode()

    def snapshot(self, vm, name):
        from . import boat
        boat.boat('snapshot', vm, name, '--json', timeout=3600)
        for _ in range(360):
            found = boat.named_snapshots().get(name)
            if found and found.get('status') == 'ready':
                return found
            time.sleep(10)
        raise RuntimeError('tier: the snapshot is not ready')

    def delete(self, vm):
        from . import boat
        boat.boat('delete', vm, '--yes', '--json', check=False, timeout=300)


def _copy(source, sink, digest):
    count = 0
    for chunk in iter(lambda: source.read(1 << 20), b''):
        digest.update(chunk)
        sink.write(chunk)
        count += len(chunk)
    return count


def push(remote, queue, name, chosen, now=None):
    """Archive the chosen batches' files into one new snapshot; return the archive record per batch.

    Nothing local changes here. The archive is verified on the VM (every member's
    own sha256 and size, the exact member set, and the archive's sha256 as sent)
    before the snapshot is saved."""
    queue = Path(queue)
    now = time.time() if now is None else now
    want = {f"{c['batch']}.results/{sha}.payload": (sha, size) for c in chosen for sha, size in c['payloads'].items()}
    if not want:
        raise ValueError('nothing to tier')
    path = f'/store/{name}/{name}.tar.zst'
    work = queue / '.tier-work'
    work.mkdir(exist_ok=True)
    listing = work / f'{name}.list'
    listing.write_bytes(b''.join(m.encode() + b'\0' for m in want))
    vm = remote.new_vm()
    try:
        remote.mkdir(vm, f'/store/{name}')
        tar = subprocess.Popen(['tar', '-C', str(queue), '--null', '-T', str(listing), '-cf', '-'], stdout=subprocess.PIPE)
        zstd = subprocess.Popen(['zstd', '-T2', '-3', '-q', '-c'], stdin=tar.stdout, stdout=subprocess.PIPE)
        tar.stdout.close()
        sink = remote.writer(vm, path)
        digest = hashlib.sha256()
        try:
            size = _copy(zstd.stdout, sink.stdin, digest)
        finally:
            sink.stdin.close()
        codes = (tar.wait(), zstd.wait(), sink.wait())
        for stream in (zstd.stdout, sink.stderr):
            with contextlib.suppress(OSError, AttributeError):
                stream.close()
        if any(codes):
            raise RuntimeError(f'tier: archive stream failed {codes}')
        check = remote.verify(vm, path, want)
        if (check['bad'] or check['missing'] or check['rc'] or check['entries'] != len(want)
                or check['sha256'] != digest.hexdigest()):
            raise RuntimeError(f'tier: the archive VM\'s copy differs: {check}')
        manifest = {os.path.basename(path): [size, digest.hexdigest()]}
        remote.put(vm, f'/store/{name}/manifest.json', json.dumps(manifest).encode())
        remote.snapshot(vm, name)
    finally:
        remote.delete(vm)
        listing.unlink(missing_ok=True)
    return {c['batch']: {'snapshot': name, 'path': path, 'sha256': digest.hexdigest(), 'bytes': size,
                         'entries': len(want), 'moved_at': now, 'payloads': dict(c['payloads'])} for c in chosen}


def hydrate(remote, queue, bid, needed=None, *, reserve_bytes=2 * 1024 ** 3, attempts=3):
    """Restore a batch's tiered payloads (those in `needed`, or all) with every hash checked; returns a receipt."""
    queue = Path(queue)
    reference = load(queue, bid)
    if reference is None:
        raise Unavailable('no tier reference')
    store = queue / f'{bid}.results'
    receipt = {'batch': bid, 'archives': [], 'installed': 0, 'bytes': 0}
    for archive in reference['archives']:
        missing = {sha: size for sha, size in archive['payloads'].items()
                   if (needed is None or sha in needed) and not (store / f'{sha}.payload').is_file()}
        if not missing:
            continue
        if shutil.disk_usage(queue).free < sum(missing.values()) + reserve_bytes:
            raise Hydrating('tier: not enough controller disk to restore yet')
        vm = remote.new_vm(archive['snapshot'], ttl=2 * 3600)
        try:
            error = None
            for _ in range(attempts):  # (a VM from a snapshot fills lazily: a short first read is retried, then refused)
                stage = queue / '.tier-work' / f'hydrate-{bid}-{uuid.uuid4().hex}'
                stage.mkdir(parents=True, mode=0o700)
                try:
                    _restore(remote, vm, archive, bid, missing, stage)
                    store.mkdir(mode=0o700, exist_ok=True)
                    for sha in missing:
                        os.replace(stage / f'{sha}.payload', store / f'{sha}.payload')
                    history.sync(store)
                    error = None
                    break
                except Unavailable as exc:
                    error = exc
                finally:
                    shutil.rmtree(stage, ignore_errors=True)
            if error is not None:
                raise error
        finally:
            remote.delete(vm)
        receipt['archives'].append({'snapshot': archive['snapshot'], 'sha256': archive['sha256']})
        receipt['installed'] += len(missing)
        receipt['bytes'] += sum(missing.values())
    return receipt


def _restore(remote, vm, archive, bid, missing, stage):
    source = remote.reader(vm, archive['path'])
    zstd = subprocess.Popen(['zstd', '-dc'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    digest, sent = hashlib.sha256(), {}
    def pump():
        try:
            sent['bytes'] = _copy(source.stdout, zstd.stdin, digest)
        except (BrokenPipeError, OSError) as exc:
            sent['error'] = exc
        finally:
            with contextlib.suppress(OSError):
                zstd.stdin.close()
    thread = threading.Thread(target=pump, daemon=True)
    thread.start()
    entries, got = 0, {}
    try:
        with tarfile.open(fileobj=zstd.stdout, mode='r|') as tf:
            for member in tf:
                entries += 1
                name = member.name
                prefix = f'{bid}.results/'
                sha = name[len(prefix):-len('.payload')] if name.startswith(prefix) and name.endswith('.payload') else None
                if sha not in missing or not member.isfile():
                    continue
                h, count = hashlib.sha256(), 0
                with open(stage / f'{sha}.payload', 'xb') as out:
                    reader = tf.extractfile(member)
                    for chunk in iter(lambda: reader.read(1 << 20), b''):
                        h.update(chunk)
                        out.write(chunk)
                        count += len(chunk)
                    out.flush()
                    os.fsync(out.fileno())
                if h.hexdigest() != sha or count != missing[sha]:
                    raise Unavailable('tier: a restored payload does not hash to its name')
                got[sha] = count
            for _ in iter(lambda: zstd.stdout.read(1 << 20), b''):
                pass
    except (tarfile.TarError, EOFError, OSError) as exc:
        source.kill()
        zstd.kill()
        raise Unavailable(f'tier: unreadable archive: {exc}') from exc
    except BaseException:
        source.kill()
        zstd.kill()
        raise
    finally:
        thread.join(timeout=600)
        codes = (source.wait(), zstd.wait())
        for stream in (source.stdout, source.stderr, zstd.stdout):
            with contextlib.suppress(OSError, AttributeError):
                stream.close()
    if (sent.get('error') or any(codes) or digest.hexdigest() != archive['sha256']
            or sent.get('bytes') != archive['bytes'] or entries != archive['entries'] or set(got) != set(missing)):
        raise Unavailable('tier: the archive does not match its reference (sha256, size or entry count)')
    history.sync(stage)
