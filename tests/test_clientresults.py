"""Client downloads: bounded transfers, verified bytes and confined output paths."""
import hashlib
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import clientresults as cr


class DownloadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = pathlib.Path(self.temp.name)
        self.dest = self.base / 'download'
        self.blobs = {}
        self.calls = []

    def ref(self, data, format='json'):
        ref = dict(version=1, sha256=hashlib.sha256(data).hexdigest(),
                   bytes=len(data), format=format)
        self.blobs[ref['sha256']] = data
        return ref

    def read(self, ref, offset, count):
        self.assertEqual(set(ref), {'version', 'sha256', 'bytes', 'format'})
        self.assertLessEqual(count, 256 * 1024)
        self.calls.append((offset, count))
        return self.blobs[ref['sha256']][offset:offset + count]

    def output_row(self, path='nested/data.bin', data=None):
        if data is None:
            data = bytes(range(256)) * 1300
        ref = self.ref(data, 'file')
        manifest = self.ref(json.dumps([dict(path=path, **ref)]).encode())
        return dict(name='job.雪', output_files_ref=manifest), ref, data

    def test_small_row_no_filesystem(self):
        row = {'name': 'tiny', 'result': {'value': 1}}
        out = cr.download(row, self.read, self.dest)
        self.assertEqual(out, row)
        self.assertIsNot(out, row)
        self.assertFalse(self.dest.exists())
        self.assertEqual(self.calls, [])

    def test_large_json_refs_not_materialized(self):
        raw = json.dumps({'result': 'x' * 900000}).encode()
        ref = self.ref(raw)
        row = dict(name='job', result_ref=ref, row_ref=ref)
        with patch('json.load', side_effect=AssertionError('bulk load')), \
             patch('json.loads', side_effect=AssertionError('bulk decode')):
            out = cr.download(row, self.read, self.dest)
        self.assertEqual(out['result_ref'], ref)
        self.assertEqual(out['row_ref'], ref)
        self.assertNotIn('result', out)
        for key in ('result_file', 'row_file'):
            path = pathlib.Path(out[key])
            self.assertTrue(path.is_absolute())
            self.assertEqual(path.read_bytes(), raw)
        self.assertNotIn('result_file', row)
        self.assertGreater(len(self.calls), 4)

    def test_binary_nested_output_and_streamed_manifest(self):
        row, ref, data = self.output_row()
        from supercomputer import resultjson
        with patch.object(resultjson, 'iter_array', wraps=resultjson.iter_array) as iterator, \
             patch('json.load', side_effect=AssertionError('bulk load')):
            out = cr.download(row, self.read, self.dest)
        iterator.assert_called_once()
        directory = pathlib.Path(out['output_files_dir'])
        self.assertTrue(directory.is_absolute())
        self.assertEqual(directory.name, 'job.雪.files')
        self.assertEqual((directory / 'nested/data.bin').read_bytes(), data)
        self.assertEqual(out['output_files_ref'], row['output_files_ref'])
        self.assertTrue(pathlib.Path(out['output_files_manifest']).is_absolute())
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual((directory / 'nested').stat().st_mode & 0o777, 0o700)
        self.assertFalse(list(directory.rglob('.download-*')))

    def test_bad_transfers_no_final_file(self):
        for failure in ('hash', 'interrupt', 'short', 'oversized'):
            with self.subTest(failure=failure):
                row, ref, data = self.output_row(path=f'{failure}/file')
                def read(r, offset, count):
                    if r == ref:
                        if failure == 'interrupt' and offset:
                            raise OSError('interrupted')
                        if failure == 'short':
                            return b''
                        if failure == 'oversized':
                            return b'x' * (count + 1)
                        if failure == 'hash':
                            return bytes(b ^ 1 for b in data[offset:offset + count])
                    return self.read(r, offset, count)
                with self.assertRaises((ValueError, OSError)):
                    cr.download(row, read, self.dest)
                self.assertFalse((self.dest / 'job.雪.files' / failure / 'file').exists())
                self.assertFalse((self.dest / '.payloads' / (ref['sha256'] + '.payload')).exists())
                self.assertFalse(list(self.dest.rglob('.upload-*')))

    def test_unsafe_relative_paths(self):
        for path in ('../escape', '/absolute', 'a/../../escape', 'a\\escape',
                     '', '.', '..', 'a//b', 'a/./b', 'a/../b', 'a\x00b'):
            with self.subTest(path=path):
                row, _, _ = self.output_row(path)
                with self.assertRaises((ValueError, OSError)):
                    cr.download(row, self.read, self.dest)
        self.assertFalse((self.base / 'escape').exists())

    def test_unsafe_job_names(self):
        for name in ('.', '..', '../x', '/x', 'a/b', 'a\\b', '', None):
            with self.subTest(name=name):
                row, _, _ = self.output_row()
                row['name'] = name
                with self.assertRaises(ValueError):
                    cr.download(row, self.read, self.dest)

    def test_symlink_directories_and_leaf_are_blocked(self):
        outside = self.base / 'outside'
        outside.mkdir()
        sentinel = outside / 'data.bin'
        sentinel.write_bytes(b'untouched')
        for component in ('root', 'output', 'nested', 'leaf', 'payloads'):
            with self.subTest(component=component):
                dest = self.base / component
                dest.mkdir()
                output = dest / 'job.雪.files'
                if component == 'root':
                    dest.rmdir()
                    dest.symlink_to(outside, target_is_directory=True)
                elif component == 'output':
                    output.symlink_to(outside, target_is_directory=True)
                elif component == 'payloads':
                    (dest / '.payloads').symlink_to(outside, target_is_directory=True)
                else:
                    output.mkdir()
                    if component == 'nested':
                        (output / 'nested').symlink_to(outside, target_is_directory=True)
                    else:
                        (output / 'nested').mkdir()
                        (output / 'nested/data.bin').symlink_to(sentinel)
                row, _, _ = self.output_row()
                with self.assertRaises((ValueError, OSError)):
                    cr.download(row, self.read, dest)
                self.assertEqual(sentinel.read_bytes(), b'untouched')

    def test_manifest_descriptors_and_formats_strict(self):
        row, ref, _ = self.output_row()
        for descriptor in (dict(path='a', **ref, extra=True),
                           dict(path='a', **{**ref, 'format': 'json'}),
                           dict(ref), [], None):
            with self.subTest(descriptor=descriptor):
                row['output_files_ref'] = self.ref(json.dumps([descriptor]).encode())
                with self.assertRaises((ValueError, TypeError)):
                    cr.download(row, self.read, self.dest)

    def test_large_manifest_streams_all_entries(self):
        ref = self.ref(b'', 'file')
        descriptors = [dict(path=f'nested/{i}.bin', **ref) for i in range(2200)]
        raw = json.dumps(descriptors).encode()
        self.assertGreater(len(raw), cr.CHUNK_BYTES)
        manifest = self.ref(raw)
        row = dict(name='many', output_files_ref=manifest)
        # Exercise every parser/dispatch step, not thousands of redundant fsyncs.
        original = cr.resultfiles.Store.import_ref
        def import_ref(store, reference, reader):
            if reference['format'] == 'file':
                return reference
            return original(store, reference, reader)
        with patch('json.load', side_effect=AssertionError('whole manifest load')), \
             patch.object(cr.resultfiles.Store, 'import_ref', new=import_ref), \
             patch.object(cr, '_copy_file') as copy:
            cr.download(row, self.read, self.dest)
        self.assertEqual(copy.call_count, len(descriptors))
        self.assertEqual(copy.call_args.args[-1], ['nested', '2199.bin'])
        self.assertEqual((self.dest / '.payloads').stat().st_mode & 0o777, 0o700)

    def test_cache_corruption_during_copy_never_publishes(self):
        row, ref, data = self.output_row()
        original = cr.resultfiles.Store.import_ref
        def import_ref(store, reference, reader):
            result = original(store, reference, reader)
            if reference['format'] == 'file':
                payload = store.root / (reference['sha256'] + '.payload')
                payload.write_bytes(b'!' + data[1:])
            return result
        with patch.object(cr.resultfiles.Store, 'import_ref', new=import_ref):
            with self.assertRaisesRegex(ValueError, 'digest mismatch'):
                cr.download(row, self.read, self.dest)
        self.assertFalse((self.dest / 'job.雪.files/nested/data.bin').exists())
        self.assertFalse(list(self.dest.rglob('.download-*')))

    def test_short_packets_and_corrupt_result_leave_no_final(self):
        raw = b'"' + b'x' * 300000 + b'"'
        ref = self.ref(raw)
        row = dict(name='json', result_ref=ref)
        def packets(r, offset, count):
            return self.read(r, offset, min(count, 997))
        out = cr.download(row, packets, self.dest)
        self.assertEqual(pathlib.Path(out['result_file']).read_bytes(), raw)
        broken = self.base / 'broken'
        def corrupt(r, offset, count):
            return b'!' * min(count, len(raw) - offset)
        with self.assertRaises(ValueError):
            cr.download(row, corrupt, broken)
        self.assertEqual(list((broken / '.payloads').iterdir()), [])

    def test_empty_manifest_and_empty_file(self):
        row, _, _ = self.output_row(data=b'')
        out = cr.download(row, self.read, self.dest)
        self.assertEqual((pathlib.Path(out['output_files_dir']) / 'nested/data.bin').read_bytes(), b'')
        row['output_files_ref'] = self.ref(b'[]')
        out = cr.download(row, self.read, self.dest)
        self.assertTrue(pathlib.Path(out['output_files_dir']).is_dir())


if __name__ == '__main__':
    unittest.main()
