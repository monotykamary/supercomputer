"""Closed admission sidecars, never active queue records.

`*.closed` retains immutable, content-addressed audit data. `*.retired` is only
an ID/generation non-reuse fence and survives expiry, like the existing ledger.
Publication first durably fences the admission JSON, then atomically removes it
from the active suffix. A crash can leave a fenced JSON, never executable work.
"""
import contextlib
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import shutil
import stat
import time

from . import resultfiles, resultjson
from .rules.history import history_visible, history_expirable

PROTOCOL = 1
SUFFIXES = ('rows', 'placed', 'attempts', 'cancel', 'logs', 'results')
AUDIT_NAMES = ('snapshot.json', 'close.json', 'rows.json', 'placements.jsonl',
               'attempts.jsonl', 'cancellations.jsonl')


def sync(directory):
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic(path, value):
    temporary = path.with_name('.' + path.name + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(value, output, separators=(',', ':'))
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)
    sync(path.parent)


def load(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd) as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError('history record is not a regular file')
        return json.load(source)


def metadata(record, bid):
    meta = record.get('closed_history') if isinstance(record, dict) else None
    if (not isinstance(meta, dict) or meta.get('protocol') != PROTOCOL
            or meta.get('batch') != bid or record.get('batch') != bid
            or not isinstance(meta.get('generation'), str) or not meta['generation']
            or meta.get('closed') is not True
            or not all(type(meta.get(k)) in (int, float) and math.isfinite(meta[k])
                       for k in ('closed_at', 'expires_at'))
            or meta['expires_at'] < meta['closed_at']):
        raise ValueError('invalid closed history')
    return meta


def read(queue, bid, *, now=None, expired=False):
    path = queue / (bid + '.closed')
    try:
        record = load(path)
    except FileNotFoundError:
        return None
    meta = metadata(record, bid)
    now = time.time() if now is None else now
    if not expired and not history_visible(True, now, meta['expires_at']):
        return None
    return meta


def finish(queue, bid, record):
    """Resume the final publication steps of an already durable close fence."""
    meta = metadata(record, bid)
    retired = queue / (bid + '.retired')
    if retired.exists():
        if load(retired) != {'batch': bid, 'generation': meta['generation']}:
            raise ValueError('closed admission generation conflict')
    else:
        atomic(retired, {'batch': bid, 'generation': meta['generation']})
    closed = queue / (bid + '.closed')
    if closed.exists():
        if metadata(load(closed), bid) != meta:
            raise ValueError('closed history cannot be overwritten or extended')
        (queue / (bid + '.json')).unlink()
    else:
        os.replace(queue / (bid + '.json'), closed)
    sync(queue)
    return meta


def capture(queue, bid, info, snapshot, receipt, hours):
    if isinstance(hours, bool) or not isinstance(hours, (float, int)) or not math.isfinite(hours) or hours < 0:
        raise ValueError('history_retention_hours must be finite and nonnegative')
    prior = read(queue, bid, expired=True)
    if prior is not None:
        if info.get("generation") not in (None, prior["generation"]):
            raise ValueError("closed admission generation conflict")
        return prior
    admission = queue / (bid + '.json')
    record = load(admission)
    if record.get('batch') != bid:
        raise ValueError('admission id does not match closed history')
    if (record.get('cache_generation') and info.get('generation')
            and record['cache_generation'] != info['generation']):
        raise ValueError('admission generation does not match closed history')
    if record.get('closed_history') is not None:
        return finish(queue, bid, record)
    if not isinstance(info.get('generation'), str) or not info['generation']:
        # Pre-generation admissions are bound once to their durable record.
        generation = record.get('cache_generation') or ('legacy-' + hashlib.sha256(
            json.dumps(record, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
        info = {**info, 'generation': generation}
        record['cache_generation'] = generation
    now = time.time()
    if not math.isfinite(now + hours * 3600):
        raise ValueError('history retention deadline is not finite')
    store = resultfiles.Store(queue / (bid + '.results'))
    snapshot = {**snapshot, 'closed': True, 'generation': info['generation']}
    refs = {'snapshot.json': store.put_json(snapshot),
            'close.json': store.put_json(receipt),
            'rows.json': store.put_json(snapshot['rows'])}
    for name, suffix in (('placements.jsonl', 'placed'), ('attempts.jsonl', 'attempts'),
                         ('cancellations.jsonl', 'cancel')):
        path = queue / (bid + '.' + suffix)
        refs[name] = store.put_file(path, format='file') if path.exists() else store._put((), format='file')
    meta = {**info, 'protocol': PROTOCOL, 'closed': True, 'journals_retained': True,
            'closed_at': now, 'expires_at': now + hours * 3600, 'audit': refs}
    record = {**record, 'closed_history': meta, 'cancellation_complete': True}
    # Existing runtimes also understand cancellation_complete as a closed fence.
    atomic(admission, record)
    return finish(queue, bid, record)


def payload(queue, bid, name, meta=None):
    meta = read(queue, bid) if meta is None else meta
    if meta is None:
        raise ValueError('closed history missing or expired')
    store = resultfiles.Store(queue / (bid + '.results'))
    with store.open(meta['audit'][name]) as source:
        return json.load(source)


def distinct_groups(queue, bid):
    """Rebuild closed per-batch exclusions from SHA-checked audit evidence.

    Used only for an old parent's ACK: the archive is not an active admission.
    Expiry eligibility is irrelevant until collection has actually removed it.
    """
    meta = read(queue, bid, expired=True)
    if meta is None:
        raise ValueError('retained distinct history missing')
    record = load(queue / (bid + '.closed'))
    specs = {item['name']: item for item in record['items']}
    ref = resultfiles.validate_ref(meta['audit']['snapshot.json'])
    with resultfiles.Store(queue / (bid + '.results')).open(ref) as source:
        digest = hashlib.sha256()
        for chunk in iter(lambda: source.read(resultfiles.CHUNK_BYTES), b''):
            digest.update(chunk)
        if digest.hexdigest() != ref['sha256']:
            raise ValueError('retained snapshot digest mismatch')
        source.seek(0)
        snapshot = json.load(source)
    groups = {}
    for placement in snapshot['placements']:
        if not isinstance(placement, dict):
            continue  # Legacy ungrouped journals may contain null records.
        spec = specs.get(placement.get('name'), {})
        group = placement.get('distinct_vm_group') or spec.get('distinct_vm_group')
        scope = placement.get('distinct_vm_group_scope') or spec.get('distinct_vm_group_scope')
        if group and scope != 'durable':
            vm = placement.get('vm')
            if not isinstance(vm, str) or not vm:
                raise ValueError('retained distinct placement has no VM')
            groups.setdefault(group, set()).add(vm)
    return {name: sorted(vms) for name, vms in groups.items()}


def rows_page(queue, bid, since, meta):
    store = resultfiles.Store(queue / (bid + '.results'))
    with store.open(meta['audit']['rows.json']) as source:
        # Streaming JSON keeps each page bounded, even for many retained rows.
        candidates = list(itertools.islice(resultjson.iter_array(source), since, since + resultfiles.POLL_ROWS))
    return resultfiles.page(candidates, 0)


def collect(queue, bid, *, now=None, detach=False):
    """Delete payloads last fenced by this generation; the tiny ID fence stays."""
    meta = read(queue, bid, expired=True)
    if meta is None:
        return False
    now = time.time() if now is None else now
    if not history_expirable(True, (queue / (bid + '.json')).exists(), now, meta['expires_at']):
        return False
    trash = queue / '.expired' / bid
    if trash.is_symlink() or trash.parent.is_symlink():
        raise ValueError('unsafe expired history directory')
    trash.mkdir(parents=True, exist_ok=True)
    for suffix in SUFFIXES:
        path = queue / (bid + '.' + suffix)
        if path.exists() or path.is_symlink():
            os.replace(path, trash / path.name)
    # Last: partial failures retain the manifest and a retryable trash directory.
    os.replace(queue / (bid + '.closed'), trash / (bid + '.closed'))
    sync(trash)
    sync(queue)
    if detach:
        return trash
    purge(trash)
    return True


def purge(trash):
    """Potentially large filesystem cleanup, outside the scheduler lock."""
    if trash.is_symlink() or trash.parent.is_symlink():
        raise ValueError('unsafe expired history directory')
    shutil.rmtree(trash)
    with contextlib.suppress(OSError):
        trash.parent.rmdir()
