"""Bounded, standalone agent result transport (also run directly on native Boat)."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import agent as A


class ResultOffload(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.j = self.root / 'b' / 'batch' / 'jobs' / 'item'
        self.j.mkdir(parents=True)
        self.patch = patch.object(A, 'ROOT', self.root)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.lease = patch.object(A, '_lease_enabled', return_value=False)
        self.lease.start()
        self.addCleanup(self.lease.stop)

    def large(self):
        # Generate in chunks: never allocate the whole (8 MiB) JSON value.
        digest = hashlib.sha256()
        with (self.j / 'result.json').open('wb') as f:
            for chunk in [b'"'] + [b'x' * 65536] * 128 + [b'"']:
                f.write(chunk)
                digest.update(chunk)
        return digest.hexdigest(), 8 * 1024 * 1024 + 2

    def offload(self):
        sha, size = self.large()
        row = {'name': 'item', 'launch_identity': 'original-nonce'}
        with patch.object(A.json, 'loads', side_effect=AssertionError('large result parsed')):
            A._read_result(self.j, row)
        self.assertNotIn('result', row)
        self.assertEqual(row['result_ref'], {'version': 1, 'sha256': sha, 'bytes': size, 'format': 'json'})
        self.assertEqual(row['launch_identity'], 'original-nonce')
        A.write_json(self.j / 'done.json', row)
        return sha, size

    def test_large_is_streamed_fsynced_and_retained(self):
        real = A.os.fsync
        with patch.object(A.os, 'fsync', wraps=real) as sync:
            sha, size = self.offload()
        self.assertGreaterEqual(sync.call_count, 2)  # payload and directory, before done
        payload = self.j / (sha + '.payload')
        self.assertEqual(payload.stat().st_size, size)
        self.assertEqual((self.j / 'result.json').stat().st_size, size)
        self.assertEqual(payload.stat().st_mode & 0o222, 0)
        h = hashlib.sha256()
        with payload.open('rb') as f:
            while chunk := f.read(65536):
                h.update(chunk)
        self.assertEqual(h.hexdigest(), sha)

    def test_inline_boundary_preserves_json_loads(self):
        for size in (32768, 32769):
            (self.j / 'result.json').write_bytes(b'"' + b'x' * (size - 2) + b'"')
            row = {}
            A._read_result(self.j, row)
            self.assertEqual('result' in row, size == 32768)
            self.assertEqual('result_ref' in row, size > 32768)

    def test_chunk_cap_eof_owner_dispatch_and_ack(self):
        sha, size = self.offload()
        args = ['batch', 'item', sha, 0, 999999999]
        got = A.rpc_dispatch('resultpart', args)
        self.assertEqual(len(base64.b64decode(got['data'])), 262144)
        self.assertEqual((got['offset'], got['bytes'], got['sha256'], got['eof']), (0, size, sha, False))
        got = A.resultpart('batch', 'item', sha, size - 1, 262144)
        self.assertEqual(base64.b64decode(got['data']), b'"')
        self.assertTrue(got['eof'])
        self.assertEqual(A.resultpart('batch', 'item', sha, size + 5, 10)['data'], '')
        with patch.object(A, '_owner_readable', return_value=False):
            self.assertFalse(A.resultpart(*args)['ok'])
        A._ack_job(self.j)
        self.assertTrue((self.j / 'acked.json').exists())
        self.assertFalse(A.resultpart(*args)['ok'])

    def test_bad_types_paths_and_symlinks_are_refused(self):
        sha, _ = self.offload()
        args = ['batch', 'item', sha, 0, 100]
        for index, value in [(0, '../batch'), (1, '..'), (2, sha.upper()), (2, '../' + sha),
                             (3, True), (3, -1), (3, '0'), (4, False), (4, -1), (4, 1.5)]:
            bad = args.copy()
            bad[index] = value
            result = A.resultpart(*bad)
            self.assertFalse(result['ok'], bad)
            self.assertLess(len(json.dumps(result)), 1000)
        payload = self.j / (sha + '.payload')
        payload.unlink()
        payload.symlink_to(self.j / 'result.json')
        self.assertFalse(A.resultpart(*args)['ok'])
        payload.unlink()
        actual = self.j.with_name('actual')
        self.j.rename(actual)
        self.j.symlink_to(actual, target_is_directory=True)
        self.assertFalse(A.resultpart(*args)['ok'])

    def test_stream_reads_are_bounded_and_chunks_never_hash_whole_payload(self):
        original = A.os.fdopen
        sizes = []

        class Reader:
            def __init__(self, f):
                self.f = f
            def __getattr__(self, key):
                return getattr(self.f, key)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                return self.f.__exit__(*args)
            def read(self, size=-1):
                self_test.assertGreaterEqual(size, 0)
                self_test.assertLessEqual(size, 262144)
                sizes.append(size)
                return self.f.read(size)

        self_test = self
        with patch.object(A.os, 'fdopen', side_effect=lambda fd, mode: Reader(original(fd, mode))):
            sha, size = self.offload()
            with patch.object(A.hashlib, 'sha256', side_effect=AssertionError('per-chunk hash')):
                got = A.resultpart('batch', 'item', sha, 262144, 262144)
        self.assertEqual(base64.b64decode(got['data']), b'x' * 262144)
        self.assertGreater(len(sizes), 32)
        # The immutable published inode is independent of subsequent raw writes.
        (self.j / 'result.json').write_text('changed')
        self.assertEqual(A.resultpart('batch', 'item', sha, size - 1, 1)['data'], base64.b64encode(b'"').decode())

    def test_exact_lease_owner_even_for_old_epoch_and_foreign_token(self):
        sha, _ = self.offload()
        args = ['batch', 'item', sha, 0, 1]
        with patch.object(A, '_lease_enabled', return_value=True), patch.object(A, '_job_owner', return_value='alice'):
            self.assertFalse(A.rpc_dispatch('resultpart', args)['ok'])
            fence = {'_lease': {'owner': 'alice', 'epoch': 1}}
            self.assertEqual(A.rpc_dispatch('resultpart', args + [fence], token_owner='alice')['bytes'], 8 * 1024 * 1024 + 2)
            self.assertFalse(A.rpc_dispatch('resultpart', args + [fence], token_owner='bob')['ok'])
            self.assertFalse(A.rpc_dispatch('resultpart', args + [{'_lease': {'owner': 'bob'}}], token_owner='bob')['ok'])

    def test_mocked_runner_publishes_before_done_and_keeps_identity(self):
        from types import SimpleNamespace
        sha, size = self.large()
        batch = self.j.parent.parent
        (batch / 'tree').mkdir()
        (batch / 'ready').touch()
        spec = {'name': 'item', 'kind': 'py', 'module': 'runner.py', 'fn': 'work',
                'args': [], 'strict_once': True, 'launch_identity': 'nonce', 'limit': 30}
        (self.j / 'spec.json').write_text(json.dumps(spec))
        real_write = A.write_json

        def write_done(path, row):
            self.assertEqual(row['launch_identity'], 'nonce')
            self.assertEqual(row['result_ref']['sha256'], sha)
            self.assertEqual((self.j / (sha + '.payload')).stat().st_size, size)
            real_write(path, row)

        with patch.object(A, 'ctx_of', return_value={}), patch.object(A, '_strict_job_identity', return_value='nonce'), \
                patch.object(A.os, 'getpid', return_value=1), patch.object(A, 'wrap', return_value=['mock']), \
                patch.object(A.subprocess, 'run', return_value=SimpleNamespace(returncode=0)), \
                patch.object(A, 'oom_kills', return_value=0), patch.object(A, 'out_of_memory', return_value=False), \
                patch.object(A, 'sudo'), patch.object(A, 'write_json', side_effect=write_done):
            A.run('batch', 'item', 'nonce')
        with patch.object(A, '_strict_job_identity', return_value='nonce'):
            done, _ = A.report(batch)
        self.assertEqual(done[0]['result_ref']['bytes'], size)
        self.assertEqual(done[0]['launch_identity'], 'nonce')
        # An identity-mismatched ACK cannot retire an untrusted strict terminal.
        with patch.object(A, '_strict_job_identity', return_value='nonce'):
            A._ack_job(self.j, 'wrong')
            self.assertTrue((self.j / 'done.json').exists())
            A._ack_job(self.j, 'nonce')
            self.assertTrue((self.j / 'acked.json').exists())

    def test_output_files_are_streamed_immutable_and_binary_readable(self):
        outputs = self.j / 'outputs'
        (outputs / 'nested').mkdir(parents=True)
        source = outputs / 'nested' / 'data.bin'
        sha = hashlib.sha256()
        with source.open('wb') as f:
            for _ in range(20):
                chunk = bytes(range(256)) * 256
                f.write(chunk)
                sha.update(chunk)
        (outputs / 'empty').touch()
        outside = self.root / 'secret'
        outside.write_bytes(b'never publish this')
        (outputs / 'symlink').symlink_to(outside)
        (outputs / 'symlink-dir').symlink_to(self.root, target_is_directory=True)
        os.mkfifo(outputs / 'fifo')
        row = {'result': {'kept': True}, 'launch_identity': 'nonce'}
        real_open = A.os.open

        def safe_open(path, *args, **kwargs):
            self.assertNotEqual(str(path), str(outside))
            self.assertNotIn(str(path), ['symlink', 'symlink-dir', 'fifo'])
            return real_open(path, *args, **kwargs)

        with patch.object(A.os, 'open', side_effect=safe_open), patch.object(A.json, 'dumps', side_effect=AssertionError('manifest must stream')):
            A._collect_outputs(self.j, row)
        self.assertEqual(row['result'], {'kept': True})
        self.assertEqual(row['launch_identity'], 'nonce')
        ref = row['output_files_ref']
        self.assertEqual(ref['format'], 'json')
        raw = (self.j / (ref['sha256'] + '.payload')).read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), ref['sha256'])
        self.assertEqual(len(raw), ref['bytes'])
        manifest = {entry['path']: entry for entry in json.loads(raw)}
        self.assertEqual(set(manifest), {'nested/data.bin', 'empty'})
        descriptor = manifest['nested/data.bin']
        self.assertEqual(descriptor, {'path': 'nested/data.bin', 'version': 1,
                                     'sha256': sha.hexdigest(), 'bytes': 20 * 65536, 'format': 'file'})
        A.write_json(self.j / 'done.json', row)
        source.write_bytes(b'mutated after publication')
        check, offset = hashlib.sha256(), 0
        while True:
            part = A.resultpart('batch', 'item', descriptor['sha256'], offset, 9999999)
            chunk = base64.b64decode(part['data'])
            self.assertLessEqual(len(chunk), 262144)
            check.update(chunk)
            offset += len(chunk)
            if part['eof']:
                break
        self.assertEqual(offset, descriptor['bytes'])
        self.assertEqual(check.hexdigest(), descriptor['sha256'])
        payload = self.j / (descriptor['sha256'] + '.payload')
        self.assertEqual(payload.stat().st_mode & 0o222, 0)

    def test_output_directory_reserved_and_empty_omitted(self):
        (self.j / 'cpus').write_text('[0]')
        args = A.wrap(self.j, {'env': {'SUPERCOMPUTER_OUTPUT_DIR': '/wrong'}, 'limit': 10},
                      {'env': {'SUPERCOMPUTER_OUTPUT_DIR': '/also-wrong'}}, self.j / 'work',
                      self.root, '/', ['true'])
        values = [arg for arg in args if arg.startswith('SUPERCOMPUTER_OUTPUT_DIR=')]
        self.assertEqual(values, ['SUPERCOMPUTER_OUTPUT_DIR=' + str(self.j / 'outputs')])
        self.assertEqual((self.j / 'outputs').stat().st_mode & 0o777, 0o700)
        row = {}
        A._collect_outputs(self.j, row)
        self.assertNotIn('output_files_ref', row)
        (self.j / 'outputs').rmdir()
        (self.j / 'outputs').symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(OSError):
            A._collect_outputs(self.j, row)

    def test_logs_use_bounded_tail(self):
        for name in ('out', 'err'):
            with (self.j / name).open('wb') as f:
                for _ in range(64):
                    f.write(b'z' * 65536)
                f.write(b'end')
        with patch.object(Path, 'read_bytes', side_effect=AssertionError('unbounded log read')):
            logs = A.itemlogs('batch', 'item')
            raw = A.logs_tar('batch')
        self.assertEqual(len(logs['out']), 400000)
        self.assertEqual(len(logs['err']), 100000)
        self.assertTrue(logs['out'].endswith('end'))
        with tarfile.open(fileobj=io.BytesIO(raw)) as tf:
            self.assertEqual(tf.getmember('item/out').size, 400000)
            self.assertTrue(tf.extractfile('item/err').read().endswith(b'end'))


if __name__ == '__main__':
    unittest.main()
