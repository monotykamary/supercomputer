"""Persistent legacy journal migration regressions (run on a native gate VM)."""
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import resultfiles, resultjournal as journal, resultjson


class ResultJournal(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'batch.rows'
        self.store = resultfiles.Store(self.root / 'batch.results')

    def write(self, *rows):
        raw = b''.join(json.dumps(row, separators=(',', ':')).encode() + b'\n' for row in rows)
        self.path.write_bytes(raw)
        return raw

    def rich(self, name='item', char='x'):
        return dict(name=name, batch='batch', code=0, result=char * 100000)

    def payload(self, ref):
        with self.store.open(ref) as source:
            return source.read()

    def metadata(self):
        return self.store.root / journal._METADATA

    def test_prepare_preserves_original_and_second_load_never_heavy_parses(self):
        raw = self.write(self.rich(), {'name': 'small', 'code': 0})
        info = self.path.stat()
        receipt = journal.prepare(self.path, self.store, strict=True)
        self.assertEqual(receipt['rows'], 2)
        self.assertFalse(receipt['cache_used'])
        self.assertLess(receipt['cache_bytes'], 1000)
        self.assertEqual(self.path.read_bytes(), raw)
        self.assertEqual((self.path.stat().st_ino, self.path.stat().st_mtime_ns),
                         (info.st_ino, info.st_mtime_ns))
        with patch.object(resultjson._Reader, 'row', side_effect=AssertionError('heavy parse')):
            rows, read = journal.load_with_receipt(self.path, self.store, True)
        self.assertTrue(read['cache_used'])
        self.assertTrue(read['needs_compaction'])
        self.assertEqual(read['cached_rows'], 2)
        self.assertEqual(read['tail_rows'], 0)
        self.assertEqual(json.loads(self.payload(rows[0]['row_ref'])), self.rich())
        self.assertEqual(rows[1], {'name': 'small', 'code': 0})
        self.assertEqual(self.path.read_bytes(), raw)

    def test_prepare_idempotent_reuses_cache_and_payloads(self):
        self.write(self.rich())
        journal.prepare(self.path, self.store, True)
        metadata = self.metadata().read_bytes()
        mtimes = {p.name: p.stat().st_mtime_ns for p in self.store.root.iterdir()}
        with patch.object(resultjson._Reader, 'row', side_effect=AssertionError('heavy parse')), \
                patch.object(self.store, 'put_file', side_effect=AssertionError('republish')):
            receipt = journal.prepare(self.path, self.store, True)
        self.assertTrue(receipt['cache_used'])
        self.assertEqual(self.metadata().read_bytes(), metadata)
        self.assertEqual({p.name: p.stat().st_mtime_ns for p in self.store.root.iterdir()}, mtimes)

    def test_appended_tail_loaded_and_later_folded_into_cache(self):
        original = self.write(self.rich('old'))
        journal.prepare(self.path, self.store, True)
        tail = json.dumps(self.rich('new')).encode() + b'\n'
        with self.path.open('ab') as target:
            target.write(tail)
        original_row = resultjson._Reader.row
        calls = []
        def counted(reader, *args, **kwargs):
            calls.append(True)
            return original_row(reader, *args, **kwargs)
        with patch.object(resultjson._Reader, 'row', counted):
            rows, receipt = journal.load_with_receipt(self.path, self.store, True)
        self.assertTrue(receipt['cache_used'])
        self.assertEqual((receipt['cached_rows'], receipt['tail_rows']), (1, 1))
        self.assertEqual(len(calls), 1)
        self.assertEqual([r['name'] for r in rows], ['old', 'new'])
        receipt = journal.prepare(self.path, self.store, True)
        self.assertTrue(receipt['cache_used'])
        self.assertEqual(receipt['cached_rows'], 2)
        with patch.object(resultjson._Reader, 'row', side_effect=AssertionError('heavy parse')):
            self.assertEqual(len(journal.load(self.path, self.store, True)), 2)
        self.assertEqual(self.path.read_bytes(), original + tail)

    def test_truncation_replacement_and_same_size_rewrite_reject_cache(self):
        for mutation in ('truncate', 'replace', 'rewrite'):
            with self.subTest(mutation=mutation):
                self.write(self.rich(char='x'))
                journal.prepare(self.path, self.store, True)
                new = {'name': 'item', 'code': 7} if mutation == 'truncate' else self.rich(char='y')
                raw = json.dumps(new, separators=(',', ':')).encode() + b'\n'
                if mutation == 'replace':
                    replacement = self.root / 'replacement'
                    replacement.write_bytes(raw)
                    os.replace(replacement, self.path)
                else:
                    self.path.write_bytes(raw)
                rows, receipt = journal.load_with_receipt(self.path, self.store, True)
                self.assertFalse(receipt['cache_used'])
                actual = json.loads(self.payload(rows[0]['row_ref'])) if 'row_ref' in rows[0] else rows[0]
                self.assertEqual(actual, new)

    def test_corrupt_metadata_and_projection_fall_back(self):
        for corruption in ('metadata', 'prefix_digest', 'cache_payload', 'count', 'missing'):
            with self.subTest(corruption=corruption):
                self.write(self.rich())
                journal.prepare(self.path, self.store, True)
                metadata = json.loads(self.metadata().read_bytes())
                if corruption == 'metadata':
                    self.metadata().write_bytes(b'{bad')
                elif corruption == 'prefix_digest':
                    metadata['prefix_sha256'] = '0' * 64
                    self.metadata().write_text(json.dumps(metadata))
                elif corruption == 'count':
                    metadata['rows'] += 1
                    self.metadata().write_text(json.dumps(metadata))
                else:
                    payload = self.store.root / (metadata['cache']['sha256'] + '.payload')
                    if corruption == 'missing':
                        payload.unlink()
                    else:
                        payload.write_bytes(payload.read_bytes().replace(b'"name"', b'"naMe"'))
                rows, receipt = journal.load_with_receipt(self.path, self.store, True)
                self.assertFalse(receipt['cache_used'])
                self.assertEqual(json.loads(self.payload(rows[0]['row_ref'])), self.rich())
                # A missing or corrupt disposable projection must not damage source.
                self.assertEqual(json.loads(self.path.read_bytes()), self.rich())

    def test_prepare_repairs_corrupt_same_size_cache_payload(self):
        self.write(self.rich())
        journal.prepare(self.path, self.store, True)
        metadata = json.loads(self.metadata().read_bytes())
        payload = self.store.root / (metadata['cache']['sha256'] + '.payload')
        payload.write_bytes(payload.read_bytes().replace(b'"name"', b'"naMe"'))
        self.assertFalse(journal.load_with_receipt(self.path, self.store, True)[1]['cache_used'])
        journal.prepare(self.path, self.store, True)
        with patch.object(resultjson._Reader, 'row', side_effect=AssertionError('heavy parse')):
            rows, receipt = journal.load_with_receipt(self.path, self.store, True)
        self.assertTrue(receipt['cache_used'])
        self.assertEqual(rows[0]['name'], 'item')

    def test_missing_or_unsafe_refs_cannot_be_accepted_from_cache(self):
        self.write(self.rich())
        journal.prepare(self.path, self.store, True)
        rows = journal.load(self.path, self.store, True)
        payload = self.store.root / (rows[0]['row_ref']['sha256'] + '.payload')
        payload.unlink()
        rows, receipt = journal.load_with_receipt(self.path, self.store, True)
        self.assertFalse(receipt['cache_used'])
        self.assertEqual(json.loads(self.payload(rows[0]['row_ref'])), self.rich())
        payload.unlink()
        outside = self.root / 'outside'
        outside.write_text('untrusted')
        payload.symlink_to(outside)
        with self.assertRaises(OSError):
            with self.store.open(rows[0]['row_ref']):
                pass
        bad = {'version': 1, 'sha256': '../escape', 'bytes': 7, 'format': 'json'}
        self.write({'name': 'bad', 'row_ref': bad})
        with self.assertRaises(ValueError):
            journal.prepare(self.path, self.store, True)
        with self.assertRaises(ValueError):
            journal.load(self.path, self.store, True)

    def test_interrupted_metadata_publish_preserves_old_valid_cache(self):
        self.write(self.rich('old'))
        journal.prepare(self.path, self.store, True)
        old_metadata = self.metadata().read_bytes()
        with self.path.open('ab') as target:
            target.write(b'{"name":"new"}\n')
        original = self.path.read_bytes()
        real_replace = os.replace
        def interrupted(src, dst, *args, **kwargs):
            if dst == journal._METADATA:
                raise OSError('simulated interruption')
            return real_replace(src, dst, *args, **kwargs)
        with patch.object(journal.os, 'replace', interrupted), self.assertRaises(OSError):
            journal.prepare(self.path, self.store, True)
        self.assertEqual(self.metadata().read_bytes(), old_metadata)
        self.assertEqual(self.path.read_bytes(), original)
        rows, receipt = journal.load_with_receipt(self.path, self.store, True)
        self.assertTrue(receipt['cache_used'])
        self.assertEqual([row['name'] for row in rows], ['old', 'new'])
        self.assertFalse(list(self.store.root.glob('.journal-*.tmp')))

    def test_interrupted_first_publish_falls_back_without_losing_rows(self):
        self.write(self.rich())
        with patch.object(journal, '_publish_metadata', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError):
                journal.prepare(self.path, self.store, True)
        rows, receipt = journal.load_with_receipt(self.path, self.store, True)
        self.assertFalse(receipt['cache_used'])
        self.assertEqual(json.loads(self.payload(rows[0]['row_ref'])), self.rich())

    def test_compact_promotes_every_row_and_retains_complete_rich_payload(self):
        self.write(self.rich('same'), {'name': 'same', 'code': 5}, {'name': 'last'})
        journal.prepare(self.path, self.store, True)
        rows = journal.load(self.path, self.store, True)
        receipt = journal.compact(self.path, iter(rows))
        self.assertEqual(receipt['rows'], 3)
        self.assertLess(receipt['compact_bytes'], 1000)
        self.assertFalse(self.metadata().exists())
        with patch.object(resultjson._Reader, 'row', side_effect=AssertionError('heavy parse')):
            reloaded, receipt = journal.load_with_receipt(self.path, self.store, True)
            self.assertEqual(reloaded, rows)
            self.assertFalse(receipt['needs_compaction'])
        self.assertEqual(json.loads(self.payload(rows[0]['row_ref'])), self.rich('same'))
        self.assertEqual([json.loads(line)['name'] for line in self.path.read_bytes().splitlines()],
                         ['same', 'same', 'last'])

    def test_compact_refuses_partial_generator_or_concurrent_append(self):
        original = self.write({'name': 'old'})
        def failing():
            yield {'name': 'old'}
            raise ValueError('interrupted rows')
        with self.assertRaises(ValueError):
            journal.compact(self.path, failing())
        self.assertEqual(self.path.read_bytes(), original)
        def racing():
            yield {'name': 'old'}
            with self.path.open('ab') as target:
                target.write(b'{"name":"racing"}\n')
        with self.assertRaises(journal.SourceChanged):
            journal.compact(self.path, racing())
        self.assertEqual(self.path.read_bytes(), original + b'{"name":"racing"}\n')
        self.assertFalse(list(self.root.glob('.batch.rows.*.tmp')))
        with self.assertRaises(ValueError):
            journal.compact(self.path, [{'name': '../bad'}])

    def test_strict_malformed_or_incomplete_rows_never_silently_dropped(self):
        for data in (b'null\n', b'\n', b'{}', b'{}\n{"name":', b'{}{}\n', b'[]\n',
                     b'{"name":"item","result":"' + b'x' * 100000 + b'"}'):
            with self.subTest(tail=data[-20:]):
                self.path.write_bytes(data)
                with self.assertRaises(ValueError):
                    journal.prepare(self.path, self.store, True)
                with self.assertRaises(ValueError):
                    journal.load(self.path, self.store, True)
        self.write({'name': 'first'})
        journal.prepare(self.path, self.store, True)
        with self.path.open('ab') as target:
            target.write(b'{"name":"torn"}')
        with self.assertRaises(ValueError):
            journal.load(self.path, self.store, True)
        with self.assertRaises(ValueError):
            journal.prepare(self.path, self.store, True)

    def test_nonstrict_cache_cannot_hide_nulls_from_strict_reader(self):
        self.path.write_bytes(b'null\n{"name":"okay"}\n')
        journal.prepare(self.path, self.store)
        self.assertEqual(journal.load(self.path, self.store), [{'name': 'okay'}])
        with self.assertRaises(ValueError):
            journal.load(self.path, self.store, True)
        with self.assertRaises(ValueError):
            journal.prepare(self.path, self.store, True)

    def test_nonstrict_append_cannot_inherit_strict_cache_validation(self):
        self.write({'name': 'okay'})
        journal.prepare(self.path, self.store, True)
        with self.path.open('ab') as target:
            target.write(b'null\n')
        journal.prepare(self.path, self.store, False)
        self.assertEqual(journal.load(self.path, self.store), [{'name': 'okay'}])
        with self.assertRaises(ValueError):
            journal.load(self.path, self.store, True)

    def test_nonstrict_unterminated_tail_kept_but_not_cached(self):
        self.path.write_bytes(b'{"name":"prefix"}\n{"name":"tail"}')
        receipt = journal.prepare(self.path, self.store)
        self.assertEqual((receipt['cached_rows'], receipt['tail_rows'], receipt['rows']), (1, 1, 2))
        rows, receipt = journal.load_with_receipt(self.path, self.store)
        self.assertTrue(receipt['cache_used'])
        self.assertEqual([row['name'] for row in rows], ['prefix', 'tail'])
        with self.path.open('ab') as target:
            target.write(b'\n{"name":"last"}\n')
        self.assertEqual([row['name'] for row in journal.load(self.path, self.store)],
                         ['prefix', 'tail', 'last'])

    def test_prepare_detects_same_size_source_rewrite_during_publication(self):
        raw = self.write(self.rich())
        real_put = self.store.put_file
        def rewrite(*args, **kwargs):
            result = real_put(*args, **kwargs)
            self.path.write_bytes(raw.replace(b'xxxxx', b'yyyyy'))
            return result
        with patch.object(self.store, 'put_file', rewrite), self.assertRaises(journal.SourceChanged):
            journal.prepare(self.path, self.store, True)
        self.assertFalse(self.metadata().exists())

    def test_small_inplace_rewrite_is_not_hidden_by_read_ahead(self):
        raw = self.write({'name': 'old'})
        real_put = self.store.put_file
        def rewrite(*args, **kwargs):
            result = real_put(*args, **kwargs)
            self.path.write_bytes(raw.replace(b'old', b'new'))
            return result
        with patch.object(self.store, 'put_file', rewrite), self.assertRaises(journal.SourceChanged):
            journal.prepare(self.path, self.store, True)
        self.assertFalse(self.metadata().exists())

    def test_small_rows_do_not_need_compaction_even_with_noncanonical_spaces(self):
        self.path.write_bytes(b' { "name" : "small", "code" : 0 } \n')
        rows, receipt = journal.load_with_receipt(self.path, self.store, True)
        self.assertFalse(receipt['needs_compaction'])
        self.assertEqual(rows, [{'name': 'small', 'code': 0}])
        self.assertFalse(journal.prepare(self.path, self.store, True)['needs_compaction'])
        self.assertFalse(journal.load_with_receipt(self.path, self.store, True)[1]['needs_compaction'])

    def test_prepare_handles_append_only_parent_during_publication(self):
        self.write(self.rich())
        real_put = self.store.put_file
        def append(*args, **kwargs):
            result = real_put(*args, **kwargs)
            with self.path.open('ab') as target:
                target.write(b'{"name":"racing"}\n')
            return result
        with patch.object(self.store, 'put_file', append):
            journal.prepare(self.path, self.store, True)
        rows, receipt = journal.load_with_receipt(self.path, self.store, True)
        self.assertTrue(receipt['cache_used'])
        self.assertEqual([row['name'] for row in rows], ['item', 'racing', 'racing'])

    def test_directory_admission_filtering_strict_errors_and_deleted_batches(self):
        self.write({'name': 'valid'})
        self.path.with_suffix('.json').write_text(json.dumps({'batch': 'batch', 'strict_once': True}))
        (self.root / 'orphan.rows').write_text('{}\n')
        (self.root / 'closed.rows').write_text('{}\n')
        (self.root / 'closed.json').write_text('{"admission_closed":true}')
        (self.root / 'bad.rows').write_text('null\n')
        (self.root / 'bad.json').write_text('{"strict_once":true}')
        summary = journal.prepare_directory(self.root)
        self.assertEqual(summary['prepared'], 2)
        self.assertEqual(summary['rows'], 2)
        self.assertEqual(summary['skipped'], 1)
        self.assertEqual(len(summary['errors']), 1)
        self.assertFalse((self.root / 'orphan.results').exists())
        self.assertTrue((self.root / 'closed.results').exists())
        real_prepare = journal.prepare
        def deleting(path, store, strict=False):
            receipt = real_prepare(path, store, strict)
            path.with_suffix('.json').unlink()
            return receipt
        with patch.object(journal, 'prepare', deleting):
            summary = journal.prepare_directory(self.root)
        self.assertEqual(summary['prepared'], 0)
        self.assertFalse(self.path.with_suffix('.json').exists())
        self.assertTrue(self.path.exists())

    def test_directory_missing_reference_is_error_not_skipped_batch(self):
        ref = dict(version=1, sha256='a' * 64, bytes=10, format='json')
        self.write({'name': 'item', 'result_ref': ref})
        self.path.with_suffix('.json').write_text('{"strict_once":true}')
        summary = journal.prepare_directory(self.root)
        self.assertEqual(summary['prepared'], 0)
        self.assertEqual(summary['skipped'], 0)
        self.assertEqual(len(summary['errors']), 1)

    def test_directory_replaced_admission_is_skipped(self):
        self.write({'name': 'item'})
        admission = self.path.with_suffix('.json')
        admission.write_text('{"strict_once":true}')
        real_prepare = journal.prepare
        def replacing(path, store, strict=False):
            receipt = real_prepare(path, store, strict)
            replacement = self.root / 'next-admission'
            replacement.write_text('{"strict_once":true}')
            os.replace(replacement, admission)
            return receipt
        with patch.object(journal, 'prepare', replacing):
            summary = journal.prepare_directory(self.root)
        self.assertEqual(summary['prepared'], 0)
        self.assertEqual(summary['skipped'], 1)
        self.assertFalse(summary['errors'])

    def test_missing_journal_and_unsafe_source(self):
        self.assertEqual(journal.load(self.path, self.store, True), [])
        target = self.root / 'target'
        target.write_text('{}\n')
        self.path.symlink_to(target)
        with self.assertRaises(OSError):
            journal.load(self.path, self.store, True)
        with self.assertRaises(OSError):
            journal.prepare(self.path, self.store, True)

    def test_bounded_load_does_not_reencode_scalars_with_streaming_encoder(self):
        row = {'name': 'inline', 'result': [{'value': n} for n in range(1000)]}
        self.write(row)
        with patch.object(self.store, 'compact_row', side_effect=AssertionError('streaming size pass')):
            self.assertEqual(journal.load(self.path, self.store), [row])

    def test_small_complete_lines_use_no_tempfile(self):
        raw = b'{"name":"small","result":"okay"}\n{}\nnull\n'
        with patch.object(resultjson.tempfile, 'NamedTemporaryFile', side_effect=AssertionError('spill')):
            rows = resultjson.load_journal(io.BytesIO(raw), self.store)
        self.assertEqual(rows, [{'name': 'small', 'result': 'okay'}, {}])
        for invalid in (b'\v{}\n', b'\f{}\n', b'{"name":"../bad"}\n'):
            with self.assertRaises(ValueError):
                resultjson.load_journal(io.BytesIO(invalid), self.store)

    def test_large_prepare_bounded_memory_and_json_loads(self):
        with self.path.open('wb') as target:
            target.write(b'{"name":"large","result":"')
            for _ in range(128):
                target.write(b'x' * 65536)
            target.write(b'"}\n')
            for i in range(5000):
                target.write(('{"name":"row%d"}\n' % i).encode())
        real_loads = json.loads
        def bounded(value, *args, **kwargs):
            self.assertLessEqual(len(value), resultfiles.INLINE_BYTES)
            return real_loads(value, *args, **kwargs)
        tracemalloc.start()
        try:
            with patch.object(journal.json, 'loads', new=bounded):
                receipt = journal.prepare(self.path, self.store, True)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(receipt['rows'], 5001)
        self.assertLess(peak, 3 * 1024 * 1024)
        self.assertLess(receipt['cache_bytes'], 150000)


if __name__ == '__main__':
    unittest.main()
