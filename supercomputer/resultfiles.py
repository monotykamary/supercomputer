"""Bounded JSON payloads and content-addressed result files.

Refs contain no paths. SHA-256 is checked before publication/import; reads trust
published content and check type/size without rehashing the file per chunk.
A downloading client must verify the final SHA-256 before accepting its copy.
"""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid

INLINE_BYTES = 65536
CHUNK_BYTES = 262144
POLL_BYTES = 524288
POLL_ROWS = 128
_STRING_CHARS = 65536
_METADATA = ('name', 'code', 'seconds', 'batch', 'run_name', 'vm', 'placement',
             'launch_identity', 'lost', 'preempted', 'error')


def _integer(value, name):
    if type(value) is not int or value < 0:
        raise ValueError(f'{name} must be a nonnegative integer')
    return value


def validate_ref(ref):
    """Validate the versioned, path-free wire representation."""
    if not isinstance(ref, dict) or set(ref) != {'version', 'sha256', 'bytes', 'format'}:
        raise ValueError('invalid result reference')
    if type(ref['version']) is not int or ref['version'] != 1 or ref['format'] not in ('json', 'file'):
        raise ValueError('unsupported result reference')
    if not isinstance(ref['sha256'], str) or not re.fullmatch(r'[0-9a-f]{64}', ref['sha256']):
        raise ValueError('invalid result digest')
    _integer(ref['bytes'], 'bytes')
    return ref


def _json_chunks(value):
    """Compact stdlib-compatible JSON without ever encoding a whole huge string.

    In particular JSONEncoder.iterencode is not sufficient: its string token can
    itself be unbounded. Escape independent character slices inside one pair of
    quotes (ASCII escaping also preserves lone surrogates).
    """
    active = set()

    def string(text):
        yield b'"'
        for i in range(0, len(text), _STRING_CHARS):
            escaped = json.dumps(text[i:i + _STRING_CHARS], ensure_ascii=True)[1:-1].encode('ascii')
            for j in range(0, len(escaped), CHUNK_BYTES):
                yield escaped[j:j + CHUNK_BYTES]
        yield b'"'

    def emit(obj):
        if isinstance(obj, str):
            yield from string(obj)
        elif obj is None or isinstance(obj, (bool, int, float)):
            yield json.dumps(obj, ensure_ascii=True).encode('ascii')
        elif isinstance(obj, (list, tuple, dict)):
            ident = id(obj)
            if ident in active:
                raise ValueError('circular reference')
            active.add(ident)
            try:
                mapping = isinstance(obj, dict)
                yield b'{' if mapping else b'['
                first = True
                for item in obj:
                    if not first:
                        yield b','
                    first = False
                    if mapping:
                        if isinstance(item, str):
                            key = item
                        elif item is None or isinstance(item, (bool, int, float)):
                            key = json.dumps(item)
                        else:
                            raise TypeError('JSON object keys must be scalar')
                        yield from string(key)
                        yield b':'
                        yield from emit(obj[item])
                    else:
                        yield from emit(item)
                yield b'}' if mapping else b']'
            finally:
                active.remove(ident)
        else:
            raise TypeError(f'not JSON serializable: {type(obj).__name__}')
    yield from emit(value)


def _bounded_size(value, limit):
    size = 0
    for part in _json_chunks(value):
        size += len(part)
        if size > limit:
            return None
    return size


class Store:
    def __init__(self, root: Path):
        self.root = Path(root)

    @contextlib.contextmanager
    def _directory(self, create=False):
        # Open each component relative to a pinned directory descriptor, so
        # neither a root symlink nor a symlink in its parents can be followed.
        path = self.root.absolute()
        fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
        try:
            for part in path.parts[1:]:
                if part == '..':
                    raise ValueError('parent traversal in result root')
                if create:
                    try:
                        os.mkdir(part, dir_fd=fd)
                    except FileExistsError:
                        pass
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                os.close(fd)
                fd = child
            yield fd
        finally:
            os.close(fd)

    def _put(self, chunks, expected=None, format='json'):
        with self._directory(create=True) as directory:
            name = '.upload-' + uuid.uuid4().hex
            fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o600, dir_fd=directory)
            try:
                digest, size = hashlib.sha256(), 0
                with os.fdopen(fd, 'wb') as stream:
                    for part in chunks:
                        if not isinstance(part, bytes):
                            raise TypeError('result reader must return bytes')
                        stream.write(part)
                        digest.update(part)
                        size += len(part)
                    ref = dict(version=1, sha256=digest.hexdigest(), bytes=size, format=format)
                    if expected is not None and ref != expected:
                        raise ValueError('result size or digest mismatch')
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, ref['sha256'] + '.payload', src_dir_fd=directory, dst_dir_fd=directory)
                os.fsync(directory)
                return ref
            finally:
                try:
                    os.unlink(name, dir_fd=directory)
                except FileNotFoundError:
                    pass

    def put_file(self, path, *, format='json'):
        """Copy a JSON or output file into this store using bounded reads."""
        if format not in ('json', 'file'):
            raise ValueError('unsupported result format')
        def chunks():
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as source:
                if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                    raise ValueError('result source is not a regular file')
                while True:
                    part = source.read(CHUNK_BYTES)
                    if not part:
                        break
                    yield part
        return self._put(chunks(), format=format)

    def put_json(self, value):
        return self._put(_json_chunks(value))

    def import_ref(self, ref, reader):
        ref = dict(validate_ref(ref))
        def chunks():
            offset = 0
            while offset < ref['bytes']:
                count = min(CHUNK_BYTES, ref['bytes'] - offset)
                part = reader(offset, count)
                if not isinstance(part, bytes):
                    raise TypeError('result reader must return bytes')
                if not part or len(part) > count:
                    raise ValueError('short or oversized result transfer')
                offset += len(part)
                yield part
            if reader(offset, 1) != b'':
                raise ValueError('result transfer exceeds declared size')
        return self._put(chunks(), expected=ref, format=ref['format'])

    def read(self, ref, offset, length):
        ref = validate_ref(ref)
        _integer(offset, 'offset')
        _integer(length, 'length')
        with self._directory() as directory:
            fd = os.open(ref['sha256'] + '.payload', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=directory)
            with os.fdopen(fd, 'rb') as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size != ref['bytes']:
                    raise ValueError('invalid result file type or size')
                if offset >= info.st_size or not length:
                    return b''
                source.seek(offset)
                return source.read(min(length, CHUNK_BYTES, info.st_size - offset))

    @contextlib.contextmanager
    def open(self, ref):
        """Pin an immutable payload for streaming, never following a symlink."""
        ref = validate_ref(ref)
        with self._directory() as directory:
            fd = os.open(ref['sha256'] + '.payload', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=directory)
            with os.fdopen(fd, 'rb') as source:
                info = os.fstat(source.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_size != ref['bytes']:
                    raise ValueError('invalid result file type or size')
                yield source

    def compact_row(self, row):
        return compact_row(row, self)


def compact_row(row, store):
    """Keep a small row as-is, or publish its COMPLETE JSON and project metadata."""
    if not isinstance(row, dict):
        raise TypeError('result row must be an object')
    if _bounded_size(row, INLINE_BYTES) is not None:
        return row
    ref = store.put_json(row)
    out = {key: row[key] for key in _METADATA
           if key in row and _bounded_size(row[key], 4096) is not None}
    for key in ('result_ref', 'output_files_ref'):
        if key in row:
            out[key] = dict(validate_ref(row[key]))
    out['row_ref'] = ref
    if _bounded_size(out, INLINE_BYTES) is None:
        raise ValueError('projected result metadata exceeds inline bound')
    return out


def page(rows, since):
    """Return a contiguous bounded page; the caller owns cursor advancement."""
    _integer(since, 'since')
    out, size = [], 0
    for index in range(since, min(len(rows), since + POLL_ROWS)):
        row = rows[index]
        count = _bounded_size(row, INLINE_BYTES)
        if count is None:
            raise ValueError('page requires compacted rows')
        if size + count > POLL_BYTES:
            break
        out.append(row)
        size += count
    return out
