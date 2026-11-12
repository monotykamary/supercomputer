"""Focused exec relay tests. Run in a native Boat job, not on the host."""
import base64
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tracemalloc
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import boat, resultrelay as relay, resultjson
from supercomputer.resultfiles import Store


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.client = SimpleNamespace(vm='test-vm', token='private-token')
        self.calls = []
        self.stdout_sizes = []
        self.payload = self.root / 'payload'
        self.payload.write_bytes(b'{"done":[]}')
        self.status = 200
        self.transport_error = None
        self.tamper = None
        self.patcher = patch.object(boat, 'exec_', side_effect=self.remote)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def remote(self, vm, args, timeout):
        self.assertEqual(vm, 'test-vm')
        self.assertEqual(args[:2], ['python3', '-c'])
        op = args[3]
        self.calls.append(op)
        mapped = [str(self.root / a[5:]) if a.startswith('/tmp/') else
                  '@' + str(self.root / a[6:]) if a.startswith('@/tmp/') else a
                  for a in args[4:]]
        # Execute the actual remote program, with only its /tmp directory
        # relocated into this test's private directory. Mock just HTTP I/O.
        setup = '''
import io, os, sys, urllib.request, urllib.error
class Response:
    def __init__(self):
        self.f = open(PAYLOAD, 'rb')
        self.code = STATUS
    def read(self, n):
        assert 0 < n <= 256 * 1024, n
        return self.f.read(n)
    def __enter__(self): return self
    def __exit__(self, *args): self.f.close()
    def close(self): self.f.close()
    def __getattr__(self, name): return getattr(self.f, name)
def urlopen(req, timeout):
    if ERROR == 'refused':
        raise urllib.error.URLError(ConnectionRefusedError(111, 'private-token'))
    if ERROR == 'timeout':
        raise urllib.error.URLError(TimeoutError('private-token'))
    assert req.full_url == 'http://127.0.0.1:8765/rpc'
    assert req.get_header('X-sc-token') == 'private-token'
    assert req.data == BODY
    response = Response()
    if STATUS != 200:
        raise urllib.error.HTTPError(req.full_url, STATUS, 'test', {}, response)
    return response
urllib.request.urlopen = urlopen
'''
        setup = setup.replace('8765', str(boat.AGENT_PORT))
        header = 'PAYLOAD=%r\nSTATUS=%r\nBODY=%r\nERROR=%r\n' % (str(self.payload), self.status, self.body, self.transport_error)
        source = header + setup + args[2].replace("dir='/tmp'", 'dir=' + repr(str(self.root)))
        p = subprocess.run([sys.executable, '-c', source, op, *mapped], capture_output=True, timeout=15)
        stdout = p.stdout.decode().replace(str(self.root) + '/sc-result-', '/tmp/sc-result-')
        self.stdout_sizes.append(len(stdout))
        self.assertLessEqual(len(stdout), 4 * ((relay.CHUNK_BYTES + 2) // 3) + 1)
        if op == 'post' and p.returncode == 0:
            meta = json.loads(stdout)
            local = self.root / Path(meta['path']).name
            self.assertEqual(local.stat().st_mode & 0o777, 0o600)
            if self.tamper:
                stdout = self.tamper(meta, local)
        return {'exitCode': p.returncode, 'stdout': stdout, 'stderr': p.stderr.decode()}

    def run_relay(self, decode, body=b'{"cmd":"poll"}'):
        self.body = body
        return relay.exec_response(self.client, body, 10, decode)

    def assert_clean(self):
        self.assertEqual(list(self.root.glob('sc-result-*')), [])
        self.assertEqual(self.calls.count('post'), 1)
        self.assertEqual(self.calls[-1], 'clean')

    def test_success_and_large_split_request(self):
        body = b'x' * (boat.EXEC_ARG * 2)
        self.assertEqual(self.run_relay(lambda s: json.loads(s.read(65536)), body), (200, {'done': []}))
        self.assertGreater(self.calls.count('stage'), 1)
        self.assert_clean()

    def test_http_error_body_and_status(self):
        self.status = 503
        self.payload.write_bytes(b'{"error":"unavailable"}')
        self.assertEqual(self.run_relay(lambda s: json.loads(s.read(65536))),
                         (503, {'error': 'unavailable'}))
        self.assert_clean()

    def test_decode_failure_cleans_exact_path(self):
        def decode(s):
            s.read(1)
            raise RuntimeError('decode failed')
        with self.assertRaisesRegex(RuntimeError, 'decode failed'):
            self.run_relay(decode)
        self.assert_clean()

    def test_metadata_rejected_and_cleaned(self):
        for field, value in [('path', '/tmp/not-ours'), ('bytes', -1), ('bytes', True),
                             ('bytes', 1.5), ('status', True), ('status', 999)]:
            with self.subTest(field=field, value=value):
                self.calls.clear()
                self.tamper = lambda meta, local: json.dumps({**meta, field: value})
                with self.assertRaisesRegex(ValueError, 'metadata'):
                    self.run_relay(lambda s: None)
                self.assert_clean()

    def test_changed_size_symlink_and_short_chunk_rejected(self):
        def truncate(meta, path):
            path.write_bytes(b'x')
            return json.dumps(meta)
        def symlink(meta, path):
            path.unlink()
            path.symlink_to(self.payload)
            return json.dumps(meta)
        for tamper in (truncate, symlink):
            self.calls.clear()
            self.tamper = tamper
            with self.assertRaises(boat.AgentError):
                self.run_relay(lambda s: s.read(65536))
            self.assert_clean()
        stream = relay._Stream(lambda *a, **k: base64.b64encode(b'x').decode(), '/unused', 20)
        with self.assertRaisesRegex(ValueError, 'chunk length'):
            stream.read(20)

    def test_bounded_stream_and_early_decoder(self):
        stream = relay._Stream(lambda *a, **kw: '', '/unused', 0)
        with self.assertRaises(ValueError):
            stream.read(-1)
        self.assertEqual(stream.read(0), b'')
        with self.assertRaisesRegex(ValueError, 'did not consume'):
            self.run_relay(lambda s: None)
        self.assert_clean()

    def test_eight_megabytes_streamed_to_result_store(self):
        with self.payload.open('wb') as f:
            f.write(b'{"done":[{"batch":"batch","name":"item","result":"')
            for _ in range(128):
                f.write(b'x' * 65536)
            f.write(b'"}]}')
        store = Store(self.root / 'store')
        tracemalloc.start()
        try:
            code, obj = self.run_relay(lambda s: resultjson.load_response(s, lambda batch: store))
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(code, 200)
        row = obj['done'][0]
        self.assertNotIn('result', row)
        self.assertGreater(row['row_ref']['bytes'], 8 * 1024 * 1024)
        self.assertLess(peak, 4 * 1024 * 1024)
        self.assertGreater(self.calls.count('read'), 128)
        self.assert_clean()

    def test_oversized_exec_metadata_and_transport_failure_cleanup(self):
        for mode in ('large', 'failed'):
            self.calls.clear()
            actual = self.remote
            def remote(vm, args, timeout):
                result = actual(vm, args, timeout)
                if args[3] == 'post':
                    if mode == 'large':
                        result['stdout'] = 'x' * (relay._META_BYTES + 1)
                    else:
                        result = {'exitCode': 1, 'stderr': 'x' * 10000}
                return result
            with patch.object(boat, 'exec_', side_effect=remote):
                with self.assertRaises(boat.AgentError) as error:
                    self.run_relay(lambda s: None)
            self.assertLess(len(str(error.exception)), 220)
            self.assert_clean()

    def test_empty_response_still_verifies_remote_eof(self):
        self.payload.write_bytes(b'')
        self.assertEqual(self.run_relay(lambda s: 'empty'), (200, 'empty'))
        self.assertIn('read', self.calls)
        self.assert_clean()

    def test_worker_refusal_is_typed_but_other_url_errors_are_not(self):
        for kind, expected in [('refused', boat.WorkerEndpointUnavailable), ('timeout', boat.AgentError)]:
            self.transport_error = kind
            with self.assertRaises(expected) as error:
                self.run_relay(lambda s: None)
            self.assertEqual(type(error.exception), expected)
            self.assertNotIn('private-token', str(error.exception))

    def test_request_limit_precedes_execution(self):
        with self.assertRaises(ValueError):
            self.run_relay(lambda s: None, b'x' * boat.EXEC_BODY)
        self.assertEqual(self.calls, [])


if __name__ == '__main__':
    unittest.main()
