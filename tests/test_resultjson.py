"""Focused bounded legacy decoder regression tests (run on native Boat)."""
import hashlib
import io
import json
from pathlib import Path
import sys
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import resultjson as rj
from supercomputer.resultfiles import Store, INLINE_BYTES


class Chunked:
    def __init__(self, stream, chunk=65536):
        self.stream, self.chunk = stream, chunk

    def read(self, n):
        if not 0 < n <= 65536:
            raise AssertionError('unbounded read')
        return self.stream.read(min(n, self.chunk))


class LegacyResultJSON(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / 'store')
        self.batches = []

    def factory(self, batch):
        self.assertEqual(batch, 'batch')
        self.batches.append(batch)
        return self.store

    def response(self, data, chunk=65536):
        return rj.load_response(Chunked(io.BytesIO(data), chunk), self.factory)

    def payload(self, ref):
        return (self.store.root / (ref['sha256'] + '.payload')).read_bytes()

    def test_bounded_journal_lines_crossing_chunks_use_fast_decoder(self):
        rows = [{'name': 'one', 'result': 'x' * 40000},
                {'name': 'two', 'result': ['unicode-☃'] * 2000}]
        data = b''.join(json.dumps(row, ensure_ascii=False).encode() + b'\n' for row in rows)
        for chunk in (1, 17, 65536):
            with self.subTest(chunk=chunk), patch.object(rj._Reader, 'row', side_effect=AssertionError('slow row decoder')):
                self.assertEqual(rj.load_journal(Chunked(io.BytesIO(data), chunk), self.store, True), rows)

    def test_normal_wrappers_and_already_compact_rows(self):
        row = {'batch': 'batch', 'name': 'item', 'result_ref': {'version': 1, 'sha256': 'a' * 64, 'bytes': 100000, 'format': 'json'}}
        original = {'ok': True, 'owned_rpc': {'result': {'done': [row], 'running': []}}, 'misc': [None, False, 2.5]}
        self.assertEqual(self.response(json.dumps(original).encode(), 1), original)
        self.assertEqual(self.batches, [])

    def test_large_row_complete_raw_capture_and_memory_bound(self):
        path = self.root / 'response.json'
        prefix = b'{"owned_rpc":{"result":{"done":[ '
        rowhead = b'{"name":"item","result":{"nested":["'
        rowtail = b'"]},"error":"fine","batch":"batch","code":0,"seconds":1.25,"vm":"vm","placement":{"a":1},"launch_identity":"nonce","lost":false,"preempted":false}'
        digest = hashlib.sha256()
        with path.open('wb') as f:
            f.write(prefix)
            f.write(rowhead)
            digest.update(rowhead)
            for _ in range(128):
                chunk = b'x' * 65536
                f.write(chunk)
                digest.update(chunk)
            f.write(rowtail)
            digest.update(rowtail)
            f.write(b' ]}}}')
        real_loads = json.loads
        def bounded_loads(value, *args, **kwargs):
            self.assertLessEqual(len(value), INLINE_BYTES)
            return real_loads(value, *args, **kwargs)
        tracemalloc.start()
        try:
            with patch.object(rj.json, 'loads', side_effect=bounded_loads), path.open('rb') as stream:
                result = rj.load_response(Chunked(stream), self.factory)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 3 * 1024 * 1024)
        row = result['owned_rpc']['result']['done'][0]
        self.assertNotIn('result', row)
        self.assertEqual(row['row_ref']['sha256'], digest.hexdigest())
        self.assertEqual(row['row_ref']['bytes'], len(rowhead) + 8 * 1024 * 1024 + len(rowtail))
        self.assertEqual(row['batch'], 'batch')
        self.assertEqual(row['placement'], {'a': 1})
        self.assertEqual(self.batches, ['batch'])
        self.assertEqual(hashlib.sha256(self.payload(row['row_ref'])).hexdigest(), digest.hexdigest())

    def test_exact_inline_boundary_and_oversized_metadata(self):
        base = b'{"batch":"batch","name":"item","data":""}'
        for size in (INLINE_BYTES, INLINE_BYTES + 1):
            raw = base[:-2] + b'x' * (size - len(base)) + base[-2:]
            row = self.response(b'{"done":[' + raw + b']}')['done'][0]
            self.assertEqual('row_ref' in row, size > INLINE_BYTES)
            if size > INLINE_BYTES:
                self.assertEqual(self.payload(row['row_ref']), raw)
            else:
                self.assertEqual(row, json.loads(raw))
        original = {'batch': 'batch', 'name': 'item', 'error': 'e' * 5000, 'data': 'x' * 70000,
                    'result_ref': {'version': 1, 'sha256': 'a' * 64, 'bytes': 7, 'format': 'json'}}
        row = self.response(json.dumps({'done': [original]}).encode())['done'][0]
        self.assertNotIn('error', row)
        self.assertEqual(row['result_ref'], original['result_ref'])
        self.assertEqual(json.loads(self.payload(row['row_ref'])), original)

    def test_python_json_nonfinite_scalars_remain_compatible(self):
        import math
        for padding in ('', 'x' * 70000):
            raw = json.dumps({'done': [{'batch': 'batch', 'name': 'item',
                'result': [float('nan'), float('inf'), float('-inf')],
                'padding': padding}]}).encode()
            row = self.response(raw, 7)['done'][0]
            if 'row_ref' in row:
                row = json.loads(self.payload(row['row_ref']))
            self.assertTrue(math.isnan(row['result'][0]))
            self.assertEqual(row['result'][1:], [float('inf'), float('-inf')])
            self.assertEqual(row['padding'], padding)

    def test_chunk_boundaries_escapes_unicode_and_numbers(self):
        row = {'batch': 'batch', 'name': 'item', 'result': ['雪😀\\\"\n\u0000' * 10000, -1.2e34, None, True, {'a': []}]}
        raw = json.dumps({'done': [row]}, ensure_ascii=False).encode()
        for chunk in (1, 7, 65536):
            actual = self.response(raw, chunk)['done'][0]
            self.assertEqual(json.loads(self.payload(actual['row_ref'])), row)

    def test_journal_strict_null_and_torn_records(self):
        raw = b'{"name":"item","result":"' + b'x' * 70000 + b'"}'
        row = rj.load_journal(Chunked(io.BytesIO(raw + b'\n')), self.store, True)[0]
        self.assertEqual(self.payload(row['row_ref']), raw)
        self.assertEqual(rj.load_journal(Chunked(io.BytesIO(b'null\n{}\n')), self.store, False), [{}])
        self.assertEqual(rj.load_journal(Chunked(io.BytesIO(b'{}')), self.store, False), [{}])
        for data in (b'null\n', b'{}', b'{}   ', b'{}{}\n', b'{"x":"torn', b'[]\n'):
            with self.subTest(data=data), self.assertRaises(ValueError):
                rj.load_journal(Chunked(io.BytesIO(data), 1), self.store, True)

    def test_malformed_json_and_temp_cleanup(self):
        bad = [b'{"done":[{"batch":"batch","name":"item","data":"' + b'x' * 70000 + tail for tail in
               (b'"}', b'\\q"}]}', b'\x01"}]}', b'"blah}]}')]
        bad += [b'{"done":[{"x":01}]}', b'{"done":[{"x":1e}]}', b'{"done":[{"x":[1,]}]}',
                b'{"done":[{"x":true false}]}', b'{"done":[{}]} trailing', b'{"done":[null]}',
                b'{"done":[{"x":"\xff"}]}', b'{"done":[{"x":"\\u12Q4"}]}']
        for data in bad:
            with self.subTest(tail=data[-30:]), self.assertRaises((ValueError, UnicodeError)):
                self.response(data, 31)
        tempdir = self.root / 'temps'
        tempdir.mkdir()
        with patch.object(rj.tempfile, 'tempdir', str(tempdir)):
            with self.assertRaises(ValueError):
                self.response(b'{"done":[{"data":"' + b'x' * 70000)
            self.assertEqual(list(tempdir.iterdir()), [])
            with patch.object(self.store, 'put_file', side_effect=OSError('disk')):
                with self.assertRaises(OSError):
                    self.response(b'{"done":[{"batch":"batch","data":"' + b'x' * 70000 + b'"}]}')
            self.assertEqual(list(tempdir.iterdir()), [])

    def test_invalid_identity_never_reaches_factory(self):
        for identity in ('../escape', '/absolute', '', '.', '..', 'a/b', 'a\\b', None, 7):
            for field in ('batch', 'name'):
                row = {'batch': 'batch', 'name': 'item', field: identity, 'result': 'x' * 70000}
                with self.subTest(field=field, identity=identity), self.assertRaises(ValueError):
                    self.response(json.dumps({'done': [row]}).encode())
        with self.assertRaises(ValueError):
            self.response(b'{"done":[{"result":"' + b'x' * 70000 + b'"}]}')
        self.assertEqual(self.batches, [])

    def test_output_manifest_streaming_and_projection(self):
        ref = {'version': 1, 'sha256': 'b' * 64, 'bytes': 500000, 'format': 'json'}
        original = {'batch': 'batch', 'name': 'item', 'output_files_ref': ref, 'data': 'x' * 70000}
        row = self.response(json.dumps({'done': [original]}).encode())['done'][0]
        self.assertEqual(row['output_files_ref'], ref)
        entries = [{'path': 'folder/file-' + str(i), 'ref': {'version': 1, 'sha256': 'a' * 64, 'bytes': i, 'format': 'file'}} for i in range(300)]
        raw = json.dumps(entries).encode()
        for chunk in (1, 71, 65536):
            self.assertEqual(list(rj.iter_array(Chunked(io.BytesIO(raw), chunk))), entries)
        self.assertEqual(list(rj.iter_array(Chunked(io.BytesIO(b'[]')))), [])
        for raw in (b'{}', b'[{]', b'[1,]', b'[1] junk', b'[1', b'["' + b'x' * 70000 + b'"]'):
            with self.subTest(raw=raw[:30]), self.assertRaises(ValueError):
                list(rj.iter_array(Chunked(io.BytesIO(raw))))

    def test_escaped_string_scanning_is_chunked(self):
        raw = b'{"done":[{"batch":"batch","name":"item","data":"' + b'\\n\\u1234\\"' * 8192 + b'"}]}'
        original = rj._Reader.take
        calls = [0]
        def counted(reader, n=1):
            calls[0] += 1
            return original(reader, n)
        with patch.object(rj._Reader, 'take', counted):
            row = self.response(raw)['done'][0]
        self.assertEqual(json.loads(self.payload(row['row_ref'])), json.loads(raw)['done'][0])
        self.assertLess(calls[0], 200, 'escaped payload must be scanned in chunks, not bytewise')

    def test_large_nonrow_scalar_fails_clearly(self):
        with self.assertRaisesRegex(ValueError, 'Large scalar'):
            self.response(b'{"outside":"' + b'x' * 70000 + b'"}')


if __name__ == '__main__':
    unittest.main()
