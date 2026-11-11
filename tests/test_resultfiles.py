"""Bounded result payload storage; no provider calls inside these tests."""
import hashlib
import json
import os
import pathlib
import stat
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

    def test_known_import_reuses_payload_without_reader_hash_or_upload(self):
        for value in ({'large': 'x' * (rf.CHUNK_BYTES + 1)}, ''):
            with self.subTest(value_bytes=len(value)):
                ref = self.store.put_json(value)
                path = self.root / (ref['sha256'] + '.payload')
                before = path.stat()
                # A new Store also reuses published data; no in-memory cache needed.
                reopened = rf.Store(self.root)
                def fail(*args, **kwargs):
                    raise AssertionError('cache hit performed payload work')
                with patch.object(rf.hashlib, 'sha256', side_effect=fail), \
                     patch.object(rf.uuid, 'uuid4', side_effect=fail), \
                     patch.object(rf.os, 'replace', side_effect=fail), \
                     patch.object(rf.os, 'fsync', side_effect=fail):
                    self.assertEqual(reopened.import_ref(ref, fail), ref)
                self.assertEqual(path.stat().st_ino, before.st_ino)
                self.assertEqual(path.stat().st_mtime_ns, before.st_mtime_ns)
                self.assertFalse(list(self.root.glob('.upload-*')))

    def test_duplicate_put_hashes_input_but_preserves_published_inode(self):
        value = {'large': 'x' * (rf.CHUNK_BYTES + 1)}
        ref = self.store.put_json(value)
        path = self.root / (ref['sha256'] + '.payload')
        before = path.stat()
        with patch.object(rf.hashlib, 'sha256', wraps=rf.hashlib.sha256) as digest, \
             patch.object(rf.os, 'replace', side_effect=AssertionError('replaced payload')), \
             patch.object(rf.os, 'fsync', side_effect=AssertionError('synced discarded upload')):
            self.assertEqual(self.store.put_json(value), ref)
            self.assertEqual(self.store.put_file(path), ref)
        self.assertEqual(digest.call_count, 2)
        self.assertEqual(path.stat().st_ino, before.st_ino)
        self.assertEqual(path.stat().st_mtime_ns, before.st_mtime_ns)
        self.assertFalse(list(self.root.glob('.upload-*')))

    def test_payload_reuse_is_independent_of_valid_reference_format(self):
        ref = self.store.put_json({'a': 1})
        file_ref = dict(ref, format='file')
        with patch.object(rf.os, 'replace', side_effect=AssertionError('duplicate publication')):
            self.assertEqual(self.store.put_file(self.root / (ref['sha256'] + '.payload'),
                                                 format='file'), file_ref)
            self.assertEqual(self.store.import_ref(file_ref, None), file_ref)
        with self.assertRaises(ValueError):
            self.store.import_ref(dict(ref, format='unsupported'), None)

    def test_wrong_size_payload_is_repaired_by_import_and_put(self):
        value = {'a': 'x' * (rf.CHUNK_BYTES + 1)}
        ref = self.store.put_json(value)
        raw = self.contents(ref)
        path = self.root / (ref['sha256'] + '.payload')
        for operation in ('import', 'put'):
            for bad in (b'', raw[:-1], raw + b'!'):
                with self.subTest(operation=operation, size=len(bad)):
                    path.write_bytes(bad)
                    calls = []
                    def reader(offset, count):
                        calls.append((offset, count))
                        self.assertLessEqual(count, rf.CHUNK_BYTES)
                        return raw[offset:offset + count]
                    if operation == 'import':
                        self.assertEqual(self.store.import_ref(ref, reader), ref)
                        self.assertGreater(len(calls), 1)
                    else:
                        self.assertEqual(self.store.put_json(value), ref)
                    self.assertEqual(path.read_bytes(), raw)
                    self.assertFalse(list(self.root.glob('.upload-*')))

    def test_symlink_payload_is_replaced_not_trusted_or_followed(self):
        value = {'a': 1}
        ref = self.store.put_json(value)
        raw = self.contents(ref)
        path = self.root / (ref['sha256'] + '.payload')
        outside = pathlib.Path(self.tmp.name) / 'outside'
        outside.write_bytes(b'!' * len(raw))
        for operation in ('import', 'put'):
            with self.subTest(operation=operation):
                path.unlink()
                path.symlink_to(outside)
                calls = []
                def reader(offset, count):
                    calls.append((offset, count))
                    return raw[offset:offset + count]
                if operation == 'import':
                    self.assertEqual(self.store.import_ref(ref, reader), ref)
                    self.assertTrue(calls)
                else:
                    self.assertEqual(self.store.put_json(value), ref)
                self.assertFalse(path.is_symlink())
                self.assertEqual(path.read_bytes(), raw)
                self.assertEqual(outside.read_bytes(), b'!' * len(raw))

    def test_import_rejects_symlink_root_and_parent_before_reader(self):
        ref = self.store.put_json(1)
        link = pathlib.Path(self.tmp.name) / 'link'
        link.symlink_to(self.root, target_is_directory=True)
        def fail(*args):
            raise AssertionError('reader called through unsafe root')
        for root in (link, link / 'child'):
            with self.assertRaises(OSError):
                rf.Store(root).import_ref(ref, fail)

    def test_nonregular_payload_is_not_a_cache_hit(self):
        blank = pathlib.Path(self.tmp.name) / 'blank'
        blank.write_bytes(b'')
        ref = self.store.put_file(blank)
        path = self.root / (ref['sha256'] + '.payload')
        path.unlink()
        os.mkfifo(path)
        calls = []
        def reader(offset, count):
            calls.append((offset, count))
            return b''
        self.assertEqual(self.store.import_ref(ref, reader), ref)
        self.assertEqual(calls, [(0, 1)])
        self.assertTrue(stat.S_ISREG(path.stat().st_mode))
        path.unlink()
        path.mkdir()
        with self.assertRaises(OSError):
            self.store.import_ref(ref, reader)
        self.assertTrue(path.is_dir())
        self.assertFalse(list(self.root.glob('.upload-*')))

    def test_failed_repair_leaves_existing_payload_and_cleans_upload(self):
        ref = self.store.put_json({'a': 1})
        path = self.root / (ref['sha256'] + '.payload')
        bad = b'bad'
        path.write_bytes(bad)
        corrupt = b'!' * ref['bytes']
        with self.assertRaisesRegex(ValueError, 'size or digest mismatch'):
            self.store.import_ref(ref, lambda offset, count: corrupt[offset:offset + count])
        self.assertEqual(path.read_bytes(), bad)
        self.assertFalse(list(self.root.glob('.upload-*')))

    def test_new_publication_and_repair_fsync_file_before_replace_then_directory(self):
        for repair in (False, True):
            with self.subTest(repair=repair):
                value = {'a': 1}
                if repair:
                    ref = self.store.put_json(value)
                    (self.root / (ref['sha256'] + '.payload')).write_bytes(b'bad')
                events = []
                sync, replace = rf.os.fsync, rf.os.replace
                def recording_sync(fd):
                    events.append('directory' if stat.S_ISDIR(os.fstat(fd).st_mode) else 'file')
                    return sync(fd)
                def recording_replace(*args, **kwargs):
                    events.append('replace')
                    return replace(*args, **kwargs)
                with patch.object(rf.os, 'fsync', side_effect=recording_sync), \
                     patch.object(rf.os, 'replace', side_effect=recording_replace):
                    self.store.put_json(value)
                self.assertEqual(events, ['file', 'replace', 'directory'])

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
