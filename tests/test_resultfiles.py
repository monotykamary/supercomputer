"""Bounded result payload storage; no provider calls inside these tests."""
import hashlib
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import resultfiles as rf


class ResultFilesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name) / 'results'
        self.store = rf.Store(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def contents(self, ref):
        chunks = []
        for offset in range(0, ref['bytes'], rf.CHUNK_BYTES):
            chunks.append(self.store.read(ref, offset, rf.CHUNK_BYTES * 2))
        return b''.join(chunks)

    def test_json_unicode_escaping_and_complete_rows(self):
        value = {'name': 'job', 'code': 0, 'seconds': 1.5, 'batch': 'batch',
                 'data': ('雪😀\\\"\n\x00\ud800' * 50000), 'nested': [True, None, {'a': 3}]}
        projected = self.store.compact_row(value)
        self.assertEqual(projected['name'], 'job')
        self.assertLessEqual(len(json.dumps(projected).encode()), rf.INLINE_BYTES)
        ref = projected['row_ref']
        content = self.contents(ref)
        self.assertEqual(json.loads(content), value)
        self.assertEqual(ref['sha256'], hashlib.sha256(content).hexdigest())
        self.assertEqual(ref['bytes'], len(content))
        self.assertTrue((self.root / (ref['sha256'] + '.payload')).is_file())
        self.assertFalse(list(self.root.glob('*.json')))

    def test_inline_boundary(self):
        # Compact JSON is exactly INLINE_BYTES at the boundary.
        row = {'data': 'a' * (rf.INLINE_BYTES - len(b'{"data":""}'))}
        self.assertIs(self.store.compact_row(row), row)
        larger = {'data': row['data'] + 'a'}
        self.assertIn('row_ref', self.store.compact_row(larger))

    def test_metadata_projection_and_existing_reference(self):
        existing = self.store.put_json({'result': 1})
        row = dict(name='small', code=0, seconds=2, batch='b', run_name='r', vm='v',
                   placement={'id': 1}, launch_identity='id', lost=False, preempted=False,
                   error='x' * 5000, data='x' * 100000, result_ref=existing)
        out = self.store.compact_row(row)
        self.assertEqual(out['result_ref'], existing)
        self.assertNotIn('error', out)
        self.assertNotIn('data', out)
        self.assertEqual(json.loads(self.contents(out['row_ref'])), row)
        for key in ('name', 'code', 'seconds', 'batch', 'run_name', 'vm', 'placement', 'launch_identity', 'lost', 'preempted'):
            self.assertEqual(out[key], row[key])

    def test_string_encoder_never_passes_huge_string_to_json(self):
        original = json.dumps
        sizes = []
        def bounded(value, *args, **kwargs):
            if isinstance(value, str):
                sizes.append(len(value))
                self.assertLessEqual(len(value), 65536)
            return original(value, *args, **kwargs)
        with patch.object(rf.json, 'dumps', side_effect=bounded):
            ref = self.store.put_json({'huge': '\x00雪' * 300000})
        self.assertTrue(sizes)
        self.assertEqual(json.loads(self.contents(ref)), {'huge': '\x00雪' * 300000})

    def test_file_copy_and_import_are_chunked(self):
        path = pathlib.Path(self.tmp.name) / 'input'
        data = b'123456789' * 100000
        path.write_bytes(data)
        ref = self.store.put_file(path)
        other = rf.Store(pathlib.Path(self.tmp.name) / 'other')
        calls = []
        def reader(offset, count):
            calls.append((offset, count))
            self.assertLessEqual(count, rf.CHUNK_BYTES)
            return self.store.read(ref, offset, count)
        self.assertEqual(other.import_ref(ref, reader), ref)
        self.assertEqual(other.read(ref, 0, 10**8), data[:rf.CHUNK_BYTES])
        self.assertEqual(other.read(ref, len(data) + 1, 1), b'')
        self.assertEqual(other.read(ref, 0, 0), b'')
        self.assertGreater(len(calls), 2)

    def test_bad_transfers_never_publish_and_cleanup(self):
        good = self.store.put_json({'a': 'x' * 100000})
        raw = self.contents(good)
        for kind in ('short', 'corrupt', 'long', 'exception', 'wrongtype'):
            target = rf.Store(pathlib.Path(self.tmp.name) / kind)
            def reader(offset, count):
                if kind == 'exception':
                    raise OSError('broken transfer')
                if kind == 'wrongtype':
                    return 'not bytes'
                data = raw[:-1] if kind == 'short' else b'!' + raw[1:] if kind == 'corrupt' else raw + b'!'
                return data[offset:offset+count]
            with self.assertRaises((ValueError, OSError, TypeError)):
                target.import_ref(good, reader)
            self.assertFalse(list(target.root.glob('*')))

    def test_output_file_format_and_manifest_survive_projection(self):
        source = pathlib.Path(self.tmp.name) / 'output.bin'
        source.write_bytes(b'\x00\xffbinary output')
        ref = self.store.put_file(source, format='file')
        self.assertEqual(ref['format'], 'file')
        self.assertEqual(rf.validate_ref(ref), ref)
        self.assertEqual(self.contents(ref), source.read_bytes())
        other = rf.Store(pathlib.Path(self.tmp.name) / 'imported-file')
        self.assertEqual(other.import_ref(ref, lambda o, n: self.store.read(ref, o, n)), ref)
        manifest = self.store.put_json([dict(path='sub/output.bin', **ref)])
        row = dict(name='job', data='x' * 100000, output_files_ref=manifest)
        compact = self.store.compact_row(row)
        self.assertEqual(compact['output_files_ref'], manifest)
        self.assertEqual(json.loads(self.contents(compact['row_ref'])), row)
        with self.assertRaises(ValueError):
            self.store.put_file(source, format='unsupported')

    def test_empty_and_short_packet_import(self):
        empty = self.store.put_json('')
        # Readers may provide less than the requested chunk without EOF.
        raw = self.contents(empty)
        other = rf.Store(pathlib.Path(self.tmp.name) / 'partial')
        self.assertEqual(other.import_ref(empty, lambda o, n: raw[o:o + min(n, 1)]), empty)
        zero = self.store.put_file(self.root / (empty['sha256'] + '.payload'))
        self.assertEqual(zero, empty)
        blank = pathlib.Path(self.tmp.name) / 'blank'
        blank.write_bytes(b'')
        ref = self.store.put_file(blank)
        self.assertEqual(other.import_ref(ref, lambda o, n: b''), ref)
        self.assertEqual(other.read(ref, 0, 1), b'')

    def test_invalid_refs_offsets_and_lengths(self):
        ref = self.store.put_json(1)
        for change in ({'sha256': '../escape'}, {'sha256': 'A' * 64}, {'version': True},
                       {'bytes': True}, {'bytes': -1}, {'bytes': 1.0}, {'format': 'raw'}, {'path': '/tmp/x'}):
            with self.assertRaises((ValueError, TypeError)):
                self.store.read({**ref, **change}, 0, 1)
        for offset, count in [(-1, 1), (0, -1), (True, 1), (0, True), (0.0, 1), (0, '2')]:
            with self.assertRaises((ValueError, TypeError)):
                self.store.read(ref, offset, count)

    def test_missing_corrupt_and_symlinks(self):
        ref = self.store.put_json({'a': 1})
        path = self.root / (ref['sha256'] + '.payload')
        path.write_bytes(b'X' * (ref['bytes'] - 1))
        with self.assertRaises(ValueError):
            self.store.read(ref, 0, 1)
        path.unlink()
        with self.assertRaises(OSError):
            self.store.read(ref, 0, 1)
        outside = pathlib.Path(self.tmp.name) / 'outside'
        outside.write_bytes(b'{}')
        path.symlink_to(outside)
        with self.assertRaises((OSError, ValueError)):
            self.store.read(ref, 0, 1)
        link = pathlib.Path(self.tmp.name) / 'link'
        link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises((OSError, ValueError)):
            rf.Store(link).put_json(1)

    def test_reads_do_not_rehash_published_payload(self):
        ref = self.store.put_json({'large': 'x' * 1000000})
        with patch.object(rf.hashlib, 'sha256', side_effect=AssertionError('read rehashed')):
            self.assertEqual(len(self.store.read(ref, 0, rf.CHUNK_BYTES)), rf.CHUNK_BYTES)
            self.assertEqual(len(self.store.read(ref, rf.CHUNK_BYTES, rf.CHUNK_BYTES)), rf.CHUNK_BYTES)

    def test_encoding_failure_cleans_temporary_file(self):
        cycle = []
        cycle.append(cycle)
        for bad in ({'not_json': object()}, cycle):
            with self.assertRaises((ValueError, TypeError)):
                self.store.put_json(bad)
            self.assertFalse(list(self.root.glob('*')))

    def test_page_bounds_order_and_progress(self):
        rows = [{'name': str(i), 'x': 'a' * 60000} for i in range(200)]
        page = rf.page(rows, 3)
        self.assertEqual(page, rows[3:3 + len(page)])
        self.assertGreater(len(page), 0)
        self.assertLessEqual(len(page), rf.POLL_ROWS)
        self.assertLessEqual(sum(len(json.dumps(r, separators=(',', ':')).encode()) for r in page), rf.POLL_BYTES)
        self.assertEqual(rf.page([{'name': str(i)} for i in range(200)], 0).__len__(), rf.POLL_ROWS)
        self.assertEqual(rf.page(rows, 200), [])
        for since in (-1, True, 0.1):
            with self.assertRaises((ValueError, TypeError)):
                rf.page(rows, since)


if __name__ == '__main__':
    unittest.main()
