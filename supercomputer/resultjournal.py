"""One-time, bounded projections of append-only legacy result journals.

Preparing never changes a .rows file. A cache is a disposable accelerator, not
an authority: each read hashes its exact source prefix and validates its compact
payload. Only explicit ``compact`` rewrites the original, after the caller has
activated the new parent, disabled legacy rollback and taken the scheduler lock.
Published result payloads, including cache payloads, have Store's immutability
contract. An interrupted metadata publication leaves the previous cache usable.
"""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import time
import uuid

from . import resultfiles, resultjson

CHUNK_BYTES = resultfiles.CHUNK_BYTES
_METADATA = '.journal-cache.json'
_METADATA_BYTES = 8192
_BATCH = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.~-]{0,199}\Z')


class SourceChanged(ValueError):
    """The source was replaced, truncated or rewritten during an operation."""


def _identity(info):
    # ctime/mtime change on a legitimate append, so cannot identify a generation.
    # dev+ino (and birth time where available) plus a freshly hashed prefix can.
    return [info.st_dev, info.st_ino, getattr(info, 'st_birthtime_ns', None)]


@contextlib.contextmanager
def _open(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    # Every verification must read fresh bytes, not a BufferedReader's stale
    # small-file read-ahead after an in-place rewrite by another process.
    with os.fdopen(fd, 'rb', buffering=0) as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError('journal must be a regular file')
        yield stream


class _Window:
    """Read one frozen extent with bounded reads; optionally continue a hash."""
    def __init__(self, source, start, end, digest=None):
        if not 0 <= start <= end:
            raise SourceChanged('invalid or changed journal extent')
        self.source, self.remaining, self.digest = source, end - start, digest
        source.seek(start)

    def read(self, count):
        if count <= 0:
            raise ValueError('bounded positive read required')
        data = self.source.read(min(count, CHUNK_BYTES, self.remaining))
        if not data and self.remaining:
            raise SourceChanged('journal truncated while reading')
        self.remaining -= len(data)
        if self.digest is not None:
            self.digest.update(data)
        return data


def _hash(source, end):
    digest = hashlib.sha256()
    stream = _Window(source, 0, end, digest)
    while stream.read(CHUNK_BYTES):
        pass
    return digest


def _complete_end(source, end):
    """Find the last complete record without allocating a potentially huge line."""
    while end:
        start = max(0, end - CHUNK_BYTES)
        source.seek(start)
        data = source.read(end - start)
        if len(data) != end - start:
            raise SourceChanged('journal truncated while finding complete prefix')
        last = data.rfind(b'\n')
        if last >= 0:
            return start + last + 1
        end = start
    return 0


def _verify_source(path, source, identity, length, digest):
    # Use the path too: the pinned descriptor alone cannot detect replacement.
    info = os.stat(path, follow_symlinks=False)
    if (not stat.S_ISREG(info.st_mode) or _identity(info) != identity or
            _identity(os.fstat(source.fileno())) != identity or info.st_size < length or
            _hash(source, length).hexdigest() != digest):
        raise SourceChanged('journal changed during migration/read')


def _check_row(row, store):
    if not isinstance(row, dict):
        raise ValueError('result row must be an object')
    resultjson._validate_identity(row)
    for key in ('row_ref', 'result_ref', 'output_files_ref'):
        if key in row:
            ref = resultfiles.validate_ref(row[key])
            if ref['format'] != 'json':
                raise ValueError('row references must contain JSON')
            # Pins a regular file of the declared size without following links.
            with store.open(ref):
                pass
    return row


def _cache_rows(store, metadata):
    ref = metadata['cache']
    digest, count = hashlib.sha256(), 0
    with store.open(ref) as stream:
        while True:
            raw = stream.readline(resultfiles.INLINE_BYTES + 2)
            if not raw:
                break
            if len(raw) > resultfiles.INLINE_BYTES + 1 or not raw.endswith(b'\n'):
                raise ValueError('unbounded or incomplete cached row')
            digest.update(raw)
            row = _check_row(json.loads(raw[:-1]), store)
            count += 1
            yield row
    if digest.hexdigest() != ref['sha256'] or count != metadata['rows']:
        raise ValueError('corrupt journal cache')


def _cache(source, info, store, strict):
    """Return a fully verified cache and its resumable prefix hash, or a miss."""
    try:
        with store._directory() as directory:
            fd = os.open(_METADATA, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=directory)
            with os.fdopen(fd, 'rb') as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    return None
                raw = stream.read(_METADATA_BYTES + 1)
        if len(raw) > _METADATA_BYTES:
            return None
        data = json.loads(raw)
        if (type(data['version']) is not int or data['version'] != 1 or
                type(data['needs_compaction']) is not bool or type(data['strict']) is not bool or
                (strict and not data['strict']) or data['identity'] != _identity(info) or
                type(data['prefix_bytes']) is not int or
                not 0 <= data['prefix_bytes'] <= info.st_size or
                type(data['rows']) is not int or data['rows'] < 0):
            return None
        ref = resultfiles.validate_ref(data['cache'])
        if ref['format'] != 'file':
            return None
        end = data['prefix_bytes']
        if end:
            source.seek(end - 1)
            if source.read(1) != b'\n':
                return None
        digest = _hash(source, end)
        if digest.hexdigest() != data['prefix_sha256']:
            return None
        # Validate everything before a caller can consume any cached row.
        for _ in _cache_rows(store, data):
            pass
        return data, digest
    except (OSError, ValueError, TypeError, KeyError, RecursionError):
        return None


def _publish_metadata(store, metadata):
    raw = json.dumps(metadata, separators=(',', ':'), ensure_ascii=True).encode()
    if len(raw) > _METADATA_BYTES:
        raise ValueError('journal metadata exceeds bound')
    with store._directory(create=True) as directory:
        name = '.journal-' + uuid.uuid4().hex + '.tmp'
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, _METADATA, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(name, dir_fd=directory)


def _publish_cache(output, store):
    """Publish compact bytes, repairing a corrupt disposable cache atomically.

    Store correctly trusts immutable payloads on ordinary reuse. Cache reads
    verify their digest, so repair a previously corrupted same-sized cache here
    rather than declaring a fresh preparation successful with invalid bytes.
    """
    ref = store.put_file(output.name, format='file')
    with store.open(ref) as stored:
        if _hash(stored, ref['bytes']).hexdigest() == ref['sha256']:
            return ref
    with store._directory() as directory:
        name = '.journal-' + uuid.uuid4().hex + '.tmp'
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, 'wb') as target:
                source = _Window(output, 0, ref['bytes'])
                digest = hashlib.sha256()
                while True:
                    data = source.read(CHUNK_BYTES)
                    if not data:
                        break
                    target.write(data)
                    digest.update(data)
                if digest.hexdigest() != ref['sha256']:
                    raise ValueError('cache changed before publication')
                target.flush()
                os.fsync(target.fileno())
            os.replace(name, ref['sha256'] + '.payload', src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(name, dir_fd=directory)
    return ref


def _project(row, store, stats, *, bounded=False):
    row = _check_row(row, store)
    # Reader/cache rows originate from <= INLINE_BYTES encoded records (or a
    # bounded projection). C JSON encoding is therefore bounded by a constant,
    # including ASCII escaping. Do not use this path for arbitrary caller rows.
    if bounded and len(json.dumps(row, separators=(',', ':'), ensure_ascii=True).encode('ascii')) <= resultfiles.INLINE_BYTES:
        return row
    compacted = store.compact_row(row)
    if compacted is not row:
        stats['needs_compaction'] = True
    return _check_row(compacted, store)


def _encode_row(row, store, stats=None, *, bounded=False):
    row = _project(row, store, stats if stats is not None else {}, bounded=bounded)
    # Arbitrary callers retain the streaming encoder; already bounded reader
    # projections avoid a second Python-level traversal of every scalar.
    raw = (json.dumps(row, separators=(',', ':'), ensure_ascii=True).encode('ascii')
           if bounded else b''.join(resultfiles._json_chunks(row)))
    if len(raw) > resultfiles.INLINE_BYTES:
        raise ValueError('compact result row exceeds inline bound')
    return raw + b'\n'


def prepare(path: Path, store: resultfiles.Store, strict: bool = False) -> dict:
    """Durably cache a complete source prefix, without rewriting the original.

    Repeated preparation reuses verified compact rows and payloads and parses
    only the new tail. Malformed records (including a torn strict tail) refuse
    publication. Concurrent appends are left for the next preparation/load.
    """
    started = time.monotonic()
    path = Path(path)
    with _open(path) as source:
        info = os.fstat(source.fileno())
        identity, end = _identity(info), info.st_size
        complete = _complete_end(source, end)
        cached = _cache(source, info, store, strict)
        old, digest = cached if cached else (None, hashlib.sha256())
        start = old['prefix_bytes'] if old else 0
        count, cache_bytes = 0, 0
        stats = dict(needs_compaction=bool(old and old['needs_compaction']))
        # This file holds ONLY compact rows, never the rich source journal.
        with tempfile.NamedTemporaryFile(prefix='supercomputer-journal-', suffix='.rows') as output:
            if old:
                for row in _cache_rows(store, old):
                    raw = _encode_row(row, store, bounded=True)
                    output.write(raw)
                    count += 1
                    cache_bytes += len(raw)
            window = _Window(source, start, complete, digest)
            for row in resultjson.iter_journal(window, store, strict, stats=stats):
                raw = _encode_row(row, store, stats, bounded=True)
                output.write(raw)
                count += 1
                cache_bytes += len(raw)
            prefix_digest = digest.hexdigest()
            prefix_needs_compaction = stats['needs_compaction']
            # A non-newline legacy final row is valid but cannot be cached as an
            # append-safe prefix. Never silently omit it or an invalid owned tail.
            tail = _Window(source, complete, end, digest)
            tail_rows = 0
            for row in resultjson.iter_journal(tail, store, strict, stats=stats):
                _project(row, store, stats, bounded=True)
                tail_rows += 1
            _verify_source(path, source, identity, end, digest.hexdigest())
            if old and start == complete:
                ref = old['cache']
            else:
                output.flush()
                ref = _publish_cache(output, store)
            metadata = dict(version=1, identity=identity, prefix_bytes=complete,
                            prefix_sha256=prefix_digest, rows=count,
                            strict=bool(strict or (old and old['strict'] and start == complete)), cache=ref,
                            needs_compaction=prefix_needs_compaction)
            # Recheck after payload publication, which can yield to an old parent.
            _verify_source(path, source, identity, end, digest.hexdigest())
            if metadata != old:
                _publish_metadata(store, metadata)
    return dict(path=str(path), rows=count + tail_rows, cached_rows=count,
                tail_rows=tail_rows, source_bytes=end, prefix_bytes=complete,
                cache_bytes=cache_bytes, cache_used=old is not None,
                needs_compaction=stats['needs_compaction'], elapsed_s=time.monotonic() - started)


def load_with_receipt(path: Path, store: resultfiles.Store, strict: bool = False):
    """Like load, also return a bounded diagnostic receipt (no global state)."""
    started = time.monotonic()
    path = Path(path)
    try:
        source_context = _open(path)
        with source_context as source:
            info = os.fstat(source.fileno())
            cached = _cache(source, info, store, strict)
            old, digest = cached if cached else (None, hashlib.sha256())
            rows = list(_cache_rows(store, old)) if old else []
            cached_count = len(rows)
            start = old['prefix_bytes'] if old else 0
            tail = _Window(source, start, info.st_size, digest)
            stats = dict(needs_compaction=bool(old and old['needs_compaction']))
            for row in resultjson.iter_journal(tail, store, strict, stats=stats):
                rows.append(_project(row, store, stats, bounded=True))
            _verify_source(path, source, _identity(info), info.st_size, digest.hexdigest())
            return rows, dict(cache_used=old is not None, cached_rows=cached_count,
                              tail_rows=len(rows) - cached_count, source_bytes=info.st_size,
                              needs_compaction=stats['needs_compaction'], elapsed_s=time.monotonic() - started)
    except FileNotFoundError:
        if path.exists():
            raise  # Missing referenced payload, not a missing journal.
        return [], dict(cache_used=False, cached_rows=0, tail_rows=0,
                        source_bytes=0, needs_compaction=False, elapsed_s=time.monotonic() - started)


def load(path: Path, store: resultfiles.Store, strict: bool = False) -> list[dict]:
    """Load a verified cached prefix plus the append tail, or decode from scratch."""
    return load_with_receipt(path, store, strict)[0]


def compact(path: Path, rows, *, bounded: bool = False) -> dict:
    """Atomically replace a journal with ALL supplied rows, durably and boundedly.

    Caller MUST hold the scheduler lock, have activated the new parent and have
    disabled legacy rollback. Rows must represent the complete current journal;
    this function cannot infer missing history from a list. Source changes during
    writing refuse promotion rather than silently losing a concurrent append.
    ``bounded=True`` is only for scheduler rows already produced by this loader
    or Store.compact_row; arbitrary caller data uses the streaming default.
    """
    path = Path(path)
    started = time.monotonic()
    store = resultfiles.Store(path.with_suffix('.results'))
    before = path.stat(follow_symlinks=False)
    if not stat.S_ISREG(before.st_mode):
        raise ValueError('journal must be a regular file')
    name = '.' + path.name + '.' + uuid.uuid4().hex + '.tmp'
    count, size = 0, 0
    # The queue is trusted scheduler state; refuse a symlink as the final parent.
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     stat.S_IMODE(before.st_mode), dir_fd=directory)
        with os.fdopen(fd, 'wb') as output:
            for row in rows:
                raw = _encode_row(row, store, bounded=bounded)
                output.write(raw)
                count += 1
                size += len(raw)
            output.flush()
            os.fsync(output.fileno())
        after = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        if (_identity(before), before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                _identity(after), after.st_size, after.st_mtime_ns, after.st_ctime_ns):
            raise SourceChanged('journal changed before promotion')
        os.replace(name, path.name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
        # The old inode/hash binding already invalidates the old cache. Remove
        # metadata for clarity; immutable payloads may still serve old readers.
        with contextlib.suppress(FileNotFoundError):
            with store._directory() as result_directory:
                os.unlink(_METADATA, dir_fd=result_directory)
                os.fsync(result_directory)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(name, dir_fd=directory)
        os.close(directory)
    return dict(path=str(path), rows=count, source_bytes=before.st_size,
                compact_bytes=size, elapsed_s=time.monotonic() - started)


def prepare_directory(queue: Path) -> dict:
    """Prepare admitted .rows only; never recreate admissions or touch archives.

    Closed/retained admissions are included while their admission exists.
    Per-file errors are reported rather than hidden, so a strict malformed batch
    remains a deployment blocker.
    Directory enumeration and admission reads are bounded independently of rich
    row size; vanished/replaced admissions are not reported as prepared.
    """
    queue, started = Path(queue), time.monotonic()
    summary = dict(prepared=0, skipped=0, rows=0, source_bytes=0, cache_bytes=0,
                   caches_reused=0, receipts=[], errors=[])
    for path in queue.glob('*.rows'):
        if not _BATCH.fullmatch(path.stem):
            summary['skipped'] += 1
            continue
        admission = path.with_suffix('.json')
        try:
            with _open(admission) as source:
                before = os.fstat(source.fileno())
                raw = source.read(16 * 1024 * 1024 + 1)
                if len(raw) > 16 * 1024 * 1024:
                    raise ValueError('admission exceeds bounded metadata limit')
                data = json.loads(raw)
            if not isinstance(data, dict) or data.get('batch', path.stem) != path.stem:
                raise ValueError('invalid admission')
            strict = bool(data.get('strict_once') or data.get('retain_journals') or
                          data.get('admission_closed') or
                          any(item.get('distinct_vm_group') for item in data.get('items', [])))
            receipt = prepare(path, resultfiles.Store(path.with_suffix('.results')), strict)
            after = admission.stat(follow_symlinks=False)
            if (_identity(before), before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                    _identity(after), after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                summary['skipped'] += 1
                continue
            summary['prepared'] += 1
            summary['rows'] += receipt['rows']
            summary['source_bytes'] += receipt['source_bytes']
            summary['cache_bytes'] += receipt['cache_bytes']
            summary['caches_reused'] += int(receipt['cache_used'])
            summary['receipts'].append(receipt)
        except FileNotFoundError as exc:
            if path.exists() and admission.exists():
                # A missing payload of an extant owned row is not batch closure.
                summary['errors'].append(dict(path=str(path), error=str(exc)))
            else:
                summary['skipped'] += 1
        except (OSError, ValueError, TypeError, AttributeError, RecursionError) as exc:
            summary['errors'].append(dict(path=str(path), error=str(exc)))
    summary['elapsed_s'] = time.monotonic() - started
    return summary
