"""Bounded decoding of legacy result responses and JSON-lines journals.

Only job rows are spilled: response envelopes retain their normal shape. Raw
rows are captured before decoding, so a large field is never passed to json.loads.
"""
import codecs
import json
import re
import tempfile
from contextlib import contextmanager

from .resultfiles import INLINE_BYTES

CHUNK_BYTES = 65536
_METADATA_BYTES = 4096
_WRAPPER_BYTES = 16 * 1024 * 1024
_FIELDS = frozenset(('batch', 'name', 'run_name', 'code', 'seconds', 'vm',
                     'placement', 'launch_identity', 'lost', 'preempted',
                     'error', 'result_ref', 'output_files_ref'))
_SPECIAL = re.compile(rb'["\\\x00-\x1f]')
# Consume runs of ordinary bytes AND complete escapes in C. The fallback below
# handles a single escape split across input chunks and reports malformed ones.
_STRING_RUN = re.compile(rb'(?:[^"\\\x00-\x1f]+|\\["\\/bfnrt]|\\u[0-9a-fA-F]{4})+')
_DIGITS = re.compile(rb'[0-9]+')
_SPACE = re.compile(rb'[ \t\r\n]+')
_ID = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.~-]{0,199}\Z')


class _Capture:
    def __init__(self, limit, file=None):
        self.limit = limit
        self.data = bytearray()
        self.file = file
        self.size = 0

    def write(self, data):
        self.size += len(data)
        if self.file is not None:
            self.file.write(data)
        if self.data is not None:
            if self.size <= self.limit:
                self.data.extend(data)
            else:
                self.data = None


class _Reader:
    def __init__(self, stream):
        self.stream = stream
        self.buf = b''
        self.pos = 0
        self.eof = False
        self.captures = []
        self.row_depth = 0
        self.wrapper_bytes = 0
        self.spilled_rows = 0
        self.utf8 = codecs.getincrementaldecoder('utf-8')()

    def peek(self):
        if self.pos == len(self.buf) and not self.eof:
            self.buf = self.stream.read(CHUNK_BYTES)
            self.pos = 0
            if not isinstance(self.buf, bytes):
                raise TypeError('JSON input must be a binary stream')
            self.eof = not self.buf
            self.utf8.decode(self.buf, final=self.eof)
        return self.buf[self.pos:self.pos + 1]

    def take(self, n=1):
        data = self.buf[self.pos:self.pos + n]
        self.pos += len(data)
        for capture in self.captures:
            capture.write(data)
        if not self.row_depth:
            self.wrapper_bytes += len(data)
            if self.wrapper_bytes > _WRAPPER_BYTES:
                raise ValueError('JSON envelope exceeds bounded decoder limit')
        return data

    def expect(self, expected):
        for c in expected:
            if self.peek() != bytes((c,)):
                raise ValueError('Malformed or truncated JSON')
            self.take()

    def space(self):
        while self.peek():
            match = _SPACE.match(self.buf, self.pos)
            if not match:
                return
            self.take(match.end() - self.pos)

    @contextmanager
    def capture(self, limit, file=None):
        capture = _Capture(limit, file)
        self.captures.append(capture)
        try:
            yield capture
        finally:
            self.captures.pop()

    def string(self):
        self.expect(b'"')
        while self.peek():
            run = _STRING_RUN.match(self.buf, self.pos)
            if run is not None:
                self.take(run.end() - self.pos)
                continue
            match = _SPECIAL.search(self.buf, self.pos)
            if match is None:
                self.take(len(self.buf) - self.pos)
                continue
            self.take(match.start() - self.pos)
            special = self.take()
            if special == b'"':
                return
            if special != b'\\':
                raise ValueError('Unescaped control character in JSON string')
            escape = self.peek()
            if escape == b'u':
                self.take()
                for _ in range(4):
                    if self.peek() not in tuple(bytes((c,)) for c in b'0123456789abcdefABCDEF'):
                        raise ValueError('Invalid JSON unicode escape')
                    self.take()
            elif escape and escape in b'"\\/bfnrt':
                self.take()
            else:
                raise ValueError('Invalid JSON escape')
        raise ValueError('Unterminated JSON string')

    def digits(self, required=True):
        found = False
        while self.peek():
            match = _DIGITS.match(self.buf, self.pos)
            if not match:
                break
            found = True
            self.take(match.end() - self.pos)
        if required and not found:
            raise ValueError('Invalid JSON number')

    def number(self):
        if self.peek() == b'-':
            self.take()
        if self.peek() == b'I':
            self.expect(b'Infinity')  # stdlib JSON permits -Infinity
            return
        if self.peek() == b'0':
            self.take()
        else:
            self.digits()
        if self.peek() == b'.':
            self.take()
            self.digits()
        if self.peek() in (b'e', b'E'):
            self.take()
            if self.peek() in (b'+', b'-'):
                self.take()
            self.digits()

    def scalar(self, materialize):
        with self.capture(INLINE_BYTES if materialize else 0) as raw:
            c = self.peek()
            if c == b'"':
                self.string()
            elif c == b't':
                self.expect(b'true')
            elif c == b'f':
                self.expect(b'false')
            elif c == b'n':
                self.expect(b'null')
            elif c == b'N':
                self.expect(b'NaN')
            elif c == b'I':
                self.expect(b'Infinity')
            elif c and c in b'-0123456789':
                self.number()
            else:
                raise ValueError('Expected JSON value')
        if materialize:
            if raw.data is None:
                raise ValueError('Large scalar outside a job row')
            return json.loads(raw.data)
        return None

    def value(self, materialize=True, factory=None):
        self.space()
        c = self.peek()
        if c == b'{':
            self.take()
            result = {} if materialize else None
            self.space()
            if self.peek() == b'}':
                self.take()
                return result
            while True:
                if materialize:
                    key = self.scalar(True)
                    if not isinstance(key, str):
                        raise ValueError('JSON object key must be a string')
                else:
                    self.string()
                    key = None
                self.space()
                self.expect(b':')
                self.space()
                if materialize and key == 'done' and self.peek() == b'[':
                    value = self.array(True, factory, rows=True)
                else:
                    value = self.value(materialize, factory)
                if materialize:
                    result[key] = value
                self.space()
                if self.peek() == b'}':
                    self.take()
                    return result
                self.expect(b',')
                self.space()
        if c == b'[':
            return self.array(materialize, factory)
        return self.scalar(materialize)

    def array(self, materialize, factory, rows=False):
        self.expect(b'[')
        result = [] if materialize else None
        self.space()
        if self.peek() == b']':
            self.take()
            return result
        while True:
            value = self.row(factory=factory) if rows else self.value(materialize, factory)
            if materialize:
                result.append(value)
            self.space()
            if self.peek() == b']':
                self.take()
                return result
            self.expect(b',')
            self.space()

    def row(self, factory=None, store=None):
        self.space()
        if self.peek() != b'{':
            raise ValueError('Job row must be an object')
        self.row_depth += 1
        try:
            with tempfile.NamedTemporaryFile(prefix='supercomputer-row-', suffix='.json') as tmp:
                with self.capture(INLINE_BYTES, tmp) as raw:
                    self.take()
                    projection = {}
                    self.space()
                    if self.peek() != b'}':
                        while True:
                            with self.capture(_METADATA_BYTES) as keyraw:
                                self.string()
                            key = json.loads(keyraw.data) if keyraw.data is not None else None
                            self.space()
                            self.expect(b':')
                            self.space()
                            with self.capture(_METADATA_BYTES) as field:
                                self.value(False)
                            if key in _FIELDS:
                                if field.data is not None:
                                    projection[key] = json.loads(field.data)
                                else:
                                    projection.pop(key, None)
                                if key in ('name', 'batch') and key not in projection:
                                    raise ValueError('Oversized job identity')
                            self.space()
                            if self.peek() == b'}':
                                break
                            self.expect(b',')
                            self.space()
                    self.expect(b'}')
                for key in ('name', 'batch'):
                    if key in projection and (not isinstance(projection[key], str) or not _ID.fullmatch(projection[key])):
                        raise ValueError('Invalid job ' + key)
                if raw.data is not None:
                    return json.loads(raw.data)
                if factory is not None:
                    if 'batch' not in projection:
                        raise ValueError('Large response job row requires a valid batch')
                    store = factory(projection['batch'])
                if store is None:
                    raise ValueError('Large job row requires a result store')
                tmp.flush()
                projection['row_ref'] = store.put_file(tmp.name)
                self.spilled_rows += 1
                return projection
        finally:
            self.row_depth -= 1


def load_response(stream, store_for_batch):
    """Decode a binary JSON response, spilling large rows in any ``done`` array."""
    reader = _Reader(stream)
    result = reader.value(factory=store_for_batch)
    reader.space()
    if reader.peek():
        raise ValueError('Trailing JSON response data')
    if not isinstance(result, dict):
        raise ValueError('JSON response must be an object')
    return result


def _validate_identity(row):
    for key in ('name', 'batch'):
        if key in row and (not isinstance(row[key], str) or not _ID.fullmatch(row[key])):
            raise ValueError('Invalid job ' + key)
    return row


def iter_journal(stream, store, strict=False, *, stats=None):
    """Yield bounded JSON-lines rows, rejecting malformed/torn owned records.

    Complete small lines take the C JSON decoder path without a per-row spill
    file. A long or split line still uses the bounded streaming row decoder.
    """
    reader = _Reader(stream)
    while True:
        reader.peek()
        # A bounded row may straddle an input chunk. Fill only up to one inline
        # row plus its newline before deciding to use the streaming slow path.
        while (reader.buf.find(b'\n', reader.pos) < 0 and not reader.eof
               and len(reader.buf) - reader.pos <= INLINE_BYTES):
            remaining = len(reader.buf) - reader.pos
            extra = reader.stream.read(min(CHUNK_BYTES, INLINE_BYTES + 1 - remaining))
            if not isinstance(extra, bytes):
                raise TypeError('JSON input must be a binary stream')
            reader.eof = not extra
            reader.utf8.decode(extra, final=reader.eof)
            reader.buf = reader.buf[reader.pos:] + extra
            reader.pos = 0
        end = reader.buf.find(b'\n', reader.pos)
        if end >= 0 and end - reader.pos <= INLINE_BYTES:
            raw = reader.take(end + 1 - reader.pos).strip(b' \t\r\n')
            reader.wrapper_bytes = 0
            if not raw:
                if strict:
                    raise ValueError('Blank owned journal record')
                continue
            row = json.loads(raw)
            if row is None and not strict:
                continue
            if not isinstance(row, dict):
                raise ValueError('Job row must be an object')
            yield _validate_identity(row)
            continue
        # Only horizontal space here: newlines are record boundaries.
        while reader.peek() in (b' ', b'\t', b'\r'):
            reader.take()
        if not reader.peek():
            return
        if reader.peek() == b'\n':
            if strict:
                raise ValueError('Blank owned journal record')
            reader.take()
            continue
        if reader.peek() == b'n' and not strict:
            reader.expect(b'null')
            row = None
        else:
            row = reader.row(store=store)
            if stats is not None and reader.spilled_rows:
                stats['needs_compaction'] = True
        while reader.peek() in (b' ', b'\t', b'\r'):
            reader.take()
        if reader.peek() == b'\n':
            reader.take()
        elif reader.peek() or strict:
            raise ValueError('Journal record must be newline terminated')
        if row is not None:
            yield row
        reader.wrapper_bytes = 0


def load_journal(stream, store, strict=False):
    """Decode JSON-lines rows; strict mode also rejects null and torn records."""
    return list(iter_journal(stream, store, strict))


def iter_array(stream):
    """Yield bounded JSON values from a binary root array (e.g. a manifest).

    The consumer must exhaust the iterator to validate the closing bracket and
    trailing data. Each encoded element is limited to INLINE_BYTES; the array
    itself has no total size limit.
    """
    reader = _Reader(stream)
    reader.space()
    reader.expect(b'[')
    reader.space()
    if reader.peek() != b']':
        while True:
            reader.wrapper_bytes = 0
            with reader.capture(INLINE_BYTES) as raw:
                reader.value(False)
            if raw.data is None:
                raise ValueError('Array element exceeds bounded decoder limit')
            yield json.loads(raw.data)
            reader.space()
            if reader.peek() == b']':
                break
            reader.expect(b',')
            reader.space()
    reader.expect(b']')
    reader.space()
    if reader.peek():
        raise ValueError('Trailing JSON array data')
