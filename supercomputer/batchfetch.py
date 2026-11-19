"""Read-only, resumable fetch of an existing completed scheduler admission."""
import contextlib
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import tarfile
import time
import tempfile
import uuid

from . import clientresults, fetchguard, history, resultfiles, toolchain


@contextlib.contextmanager
def _atomic(directory, name, binary=False):
    with clientresults._directory(directory) as parent:
        try:
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("fetch destination is not a regular file")
        temporary = '.fetch-' + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, 'wb' if binary else 'w') as output:
                yield output
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=parent)


def _json_file(directory, name, value):
    with _atomic(directory, name) as output:
        json.dump(value, output, indent=2)
        output.write('\n')


def _read_json(directory, name):
    with clientresults._directory(directory) as parent:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd) as source:
            if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                raise ValueError("fetch marker is not a regular file")
            return json.load(source)


def _info(client, bid, timeout):
    info = client.req('/batch/info', {'batch': bid}, timeout=timeout)
    if not isinstance(info, dict) or info.get('ok') is not True or info.get('batch') != bid:
        raise RuntimeError("batch is missing or unavailable; no results were fetched")
    for key in ('items', 'rows'):
        if type(info.get(key)) is not int or info[key] < 0:
            raise ValueError("invalid batch inventory")
    if info.get('complete') is not True or info['items'] != info['rows']:
        raise RuntimeError("batch is not finished with every result retained; fetch does not wait or close it")
    if not isinstance(info.get('generation'), str) or not info['generation']:
        raise ValueError("batch has no authoritative admission generation")
    admitted = info.get('admitted_at')
    if isinstance(admitted, bool) or not isinstance(admitted, (int, float)) or not math.isfinite(admitted):
        raise ValueError("invalid admission timestamp")
    return info


def _verified_stream(directory, name, *, limit=None):
    def decode(response):
        expected = response.getheader('X-SC-Content-SHA256', '')
        if not re.fullmatch('[0-9a-f]{64}', expected):
            raise ValueError("controller omitted the stream SHA-256; upgrade the controller before fetching")
        try:
            length = int(response.getheader('Content-Length', ''))
        except (TypeError, ValueError) as exc:
            raise ValueError("controller omitted the stream byte count") from exc
        if length < 0 or (limit is not None and length > limit):
            raise ValueError("invalid or excessive stream byte count")
        digest, size = hashlib.sha256(), 0
        with _atomic(directory, name, binary=True) as output:
            while True:
                chunk = response.read(resultfiles.CHUNK_BYTES)
                if not chunk:
                    break
                size += len(chunk)
                if size > length:
                    raise ValueError("stream exceeds its byte count")
                output.write(chunk)
                digest.update(chunk)
            if size != length or digest.hexdigest() != expected:
                raise ValueError("stream size or SHA-256 mismatch")
        return {'file': str(directory / name), 'bytes': size,
                'sha256': expected, 'verified': True}
    return decode


def _logs(client, bid, directory, timeout, never_launched_ok=False):
    decode = _verified_stream(directory, 'logs.tar.gz')
    # No VM fallback: fetch only the controller's retained logs, not a later
    # unrelated worker incarnation. A raw archive is never extracted here.
    return client.req('/logs', {'batch': bid, 'vms': [], 'require_complete': True,
                                **({'never_launched_ok': True} if never_launched_ok else {})},
                      timeout=timeout, decode=decode)


NEVER_LAUNCHED = "no retained log: never launched (the controller's attempts journal holds no start request)"


def _logs_missing(directory, rows_file):
    """Rows placed but absent from the verified archive. The controller's complete snapshot leaves out only the rows
    it proved never launched; they are named here, never given empty logs. Member names are read, nothing extracted."""
    with tarfile.open(directory / 'logs.tar.gz', 'r:gz') as tf:
        present = {tuple(m.name.split('/', 1)) for m in tf.getmembers() if m.isfile() and '/' in m.name}
    missing = []
    with open(rows_file, 'rb') as rows:
        for line in rows:
            row = json.loads(line)
            if row.get('row_file') and not (row.get('vm') or row.get('run_name')):
                with open(row['row_file'], 'rb') as full:  # (a large row's fields: its verified file)
                    row = {**json.load(full), **row}
            run = row.get('run_name') or (row.get('placement') or {}).get('run_name') or row.get('name')
            if not (row.get('vm') or row.get('run_name')):
                continue
            if any((run, stream) not in present for stream in ('out', 'err')):
                missing.append({'name': row.get('name'), 'run_name': run, 'status': row.get('status'),
                                'reason': NEVER_LAUNCHED})
    return missing


def fetch(bid: str, out, *, client=None, timeout=120, allow_large=False, fetch_limit=None):
    """Fetch a finished admission without submit, close, cancel, reap or startup.

    Every row page, referenced row/result/output payload and the log archive is checked
    against its controller-supplied SHA-256. Large JSON stays on disk. Failed
    downloads leave verified payloads reusable, but never publish fetch.json.
    Use an empty directory, or resume a fetch of this same admission generation.
    """
    from . import scheduler
    guard = fetchguard.FetchGuard(fetch_limit, allow_large=allow_large, batch=bid)
    scheduler.Scheduler._valid_batch(bid)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    if client is None:
        client = scheduler.current()  # never connect()/ensure_running()
    if client is None:
        raise RuntimeError("no running scheduler; fetch never starts one")
    caps = client.req('/capabilities', {}, timeout=timeout)
    if not isinstance(caps, dict) or type(caps.get('batch_fetch_protocol')) is not int or caps['batch_fetch_protocol'] != 1:
        try:
            status = client.req('/status', {}, timeout=min(timeout, 5))
        except Exception:
            status = {}
        if not isinstance(status, dict):
            status = {}
        serving = status.get('code') or 'unknown'
        installed = status.get('code_installed') or 'unknown'
        raise RuntimeError(
            f"controller lacks batch_fetch_protocol:1 (serving={serving}, installed={installed}); "
            "fetch requires support in the serving process, not just staged code; "
            "verify the serving version before retrying")
    before = _info(client, bid, timeout)
    directory = Path(out).absolute()
    started = time.time()
    with clientresults._directory(directory) as parent:
        lockfd = os.open('.fetch.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        with os.fdopen(lockfd, 'w') as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
                raise ValueError("fetch lock is not a regular file")
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            identity = {'batch': bid, 'generation': before['generation']}
            try:
                previous = _read_json(directory, 'batch.json')
            except FileNotFoundError:
                if any(name != '.fetch.lock' for name in os.listdir(parent)):
                    raise ValueError("fetch output must be empty or belong to this admission")
                _json_file(directory, 'batch.json', identity)
            else:
                if previous != identity:
                    raise ValueError("fetch output belongs to a different admission")
            # A failed repeat must not leave yesterday's success receipt next
            # to a newly replaced row stream or log archive.
            try:
                previous_receipt = os.stat('fetch.json', dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                if not stat.S_ISREG(previous_receipt.st_mode):
                    raise ValueError("fetch receipt is not a regular file")
                os.unlink('fetch.json', dir_fd=parent)
                os.fsync(parent)
            def read(ref, offset, length):
                return client.req('/results', {'batch': bid, 'ref': ref, 'offset': offset, 'length': length}, timeout=timeout, raw=True)
            names, count, since, row_hash = set(), 0, 0, hashlib.sha256()
            pages = []
            assessment = toolchain.BatchToolchains()
            with tempfile.TemporaryFile(mode='w+b', dir=directory) as inventory, \
                    _atomic(directory, 'rows.jsonl', binary=True) as output:
                while True:
                    page_name = f'{since:012d}.json'
                    page_info = client.req('/batch/rows', {'batch': bid, 'since': since}, timeout=timeout,
                        decode=_verified_stream(directory / 'pages', page_name,
                                                limit=resultfiles.POLL_BYTES + resultfiles.INLINE_BYTES + 1024))
                    pages.append(dict(page_info, since=since))
                    page = _read_json(directory / 'pages', page_name)
                    if not isinstance(page, dict) or page.get('err') or page.get('error') or not isinstance(page.get('rows'), list):
                        raise RuntimeError("batch result page is missing or invalid")
                    rows = page['rows']
                    if not rows:
                        if page.get('done') is not True:
                            raise RuntimeError("batch became incomplete during fetch")
                        break
                    next_since = page.get('next_since')
                    if type(next_since) is not int or next_since != since + len(rows):
                        raise ValueError("non-contiguous batch result cursor")
                    for row in rows:
                        if not isinstance(row, dict):
                            raise ValueError("invalid batch result row")
                        name = clientresults._name(row)
                        if name in names:
                            raise ValueError("duplicate result row")
                        names.add(name)
                        result = clientresults.download(row, read, directory, verify_cached=True,
                                                        guard=guard, download_outputs=False)
                        assessment.observe(result)
                        line = (json.dumps(result, separators=(',', ':')) + '\n').encode()
                        inventory.write(line)
                        count += 1
                    since = next_since
                    if count > before['items']:
                        raise ValueError("batch result inventory grew unexpectedly")
                if count != before['items']:
                    raise RuntimeError("finished batch has missing result rows")
                guard.check()  # Complete batch inventory, before downloading ANY output body.
                inventory.seek(0)
                for line in inventory:
                    result = clientresults.download(json.loads(line), read, directory,
                                                    verify_cached=True, guard=guard)
                    encoded = (json.dumps(result, separators=(',', ':')) + '\n').encode()
                    output.write(encoded)
                    row_hash.update(encoded)
                never_launched_ok = caps.get('batch_fetch_never_launched') == 1
                logs = _logs(client, bid, directory, timeout, never_launched_ok)
                audit = {}
                references = before.get('audit', {})
                if references and (not isinstance(references, dict) or set(references) != set(history.AUDIT_NAMES)):
                    raise ValueError('invalid closed audit inventory')
                for name, ref in references.items():
                    resultfiles.validate_ref(ref)
                    copied = clientresults.download({'name': 'audit', 'result_ref': ref}, read,
                                                    directory / 'audit', verify_cached=True)
                    audit[name] = {'ref': ref, 'file': copied['result_file']}
                after = _info(client, bid, timeout)
                if after.get('audit', {}) != references:
                    raise RuntimeError('closed audit inventory changed during fetch')
                if any(after[k] != before[k] for k in ('generation', 'admitted_at', 'items', 'rows')):
                    raise RuntimeError("admission changed during fetch")
            missing = _logs_missing(directory, directory / 'rows.jsonl') if never_launched_ok else []
            result = {'batch': bid, 'generation': before['generation'], 'read_only': True, 'logs_missing': missing,
                      'complete': True, 'closed_by_fetch': False, 'rows': count,
                      'rows_file': str(directory / 'rows.jsonl'), 'rows_sha256': row_hash.hexdigest(),
                      'logs': logs, 'audit': audit, 'pages': pages, 'before': before, 'after': after,
                      'toolchain': assessment.summary(),
                      'started_at': started, 'finished_at': time.time()}
            _json_file(directory, 'fetch.json', result)
            return result
