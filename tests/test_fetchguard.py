"""Results-only fetch guard: fake transports, real verified payload stores."""
import io
import json
import os
import pathlib
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import batchfetch, clientresults, fetchguard, resultfiles
from supercomputer.boat import stop_vm as stop_vm_implementation
from test_batch_fetch import FakeClient, BID
from test_strict_once_client import session, ITEM, requests


class PolicyTests(unittest.TestCase):
    def test_default_override_and_invalid_values(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(fetchguard.resolve_limit(), 1024 ** 3)
        for value, expected in [(0, 0), ('0', 0), (' 12 ', 12), ('2KiB', 2048), ('1 MiB', 1024**2), ('3GiB', 3*1024**3)]:
            self.assertEqual(fetchguard.resolve_limit(value), expected)
        for value in [True, False, -1, 1.5, '', '-1', 'nan', '1.5GiB', 'unlimited']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                fetchguard.resolve_limit(value)
        with self.assertRaises(ValueError):
            fetchguard.resolve_limit(allow_large='False')
        with patch.dict(os.environ, {'SUPERCOMPUTER_FETCH_LIMIT': '23'}):
            self.assertEqual(fetchguard.resolve_limit(), 23)
        with patch.dict(os.environ, {'SUPERCOMPUTER_FETCH_LIMIT': 'invalid'}):
            self.assertIsNone(fetchguard.resolve_limit(allow_large=True))

    def test_boundary_idempotence_and_logical_destination_bytes(self):
        guard = fetchguard.FetchGuard(10)
        ref = {'sha256': 'a'*64, 'bytes': 100}
        entries = [{'path': 'a', 'bytes': 5}, {'path': 'b', 'bytes': 5}]
        guard.observe('one', ref, entries)
        guard.observe('one', ref, iter(()))
        self.assertEqual(guard.total, 10)
        guard.check()
        guard.observe('two', ref, [{'path': 'c', 'bytes': 1}])
        with self.assertRaises(fetchguard.FetchLimitExceeded):
            guard.check()
        with self.assertRaisesRegex(ValueError, 'changed'):
            guard.observe('one', dict(ref, bytes=101), [])

    def test_bounded_largest_paths_and_all_item_sizes(self):
        guard = fetchguard.FetchGuard(0, batch='batch-safe')
        guard.observe('unsafe\nname', {'sha256': 'a'*64, 'bytes': 1},
                      ({'path': f'build/{i}', 'bytes': i} for i in range(50)))
        self.assertEqual(len(guard.largest), 10)
        with self.assertRaises(fetchguard.FetchLimitExceeded) as caught:
            guard.check()
        text = str(caught.exception)
        self.assertIn('1225 bytes (50 files)', text)
        self.assertIn('49 bytes', text)
        self.assertNotIn('unsafe\nname', text)
        self.assertIn('unsafe\\nname', text)
        self.assertIn('nothing was deleted', text)


class FetchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.client = FakeClient(self.root)
        self.out = self.root / 'out'

    def test_batch_aggregate_refuses_before_any_body_and_can_resume(self):
        second = dict(self.client.rows[0], name='two')
        self.client.rows[1] = second
        size = len(self.client.file_bytes)
        with self.assertRaises(fetchguard.FetchLimitExceeded) as caught:
            batchfetch.fetch(BID, self.out, client=self.client, fetch_limit=2*size-1)
        self.assertIn(f'{2*size} bytes', str(caught.exception))
        for name in ('one', 'two'):
            self.assertIn(f'"{name}": {size} bytes', str(caught.exception))
            self.assertFalse((self.out / (name+'.files')).exists())
        self.assertFalse((self.out / 'fetch.json').exists())
        self.assertFalse((self.out / 'rows.jsonl').exists())
        self.assertFalse(any(p == '/results' and m['ref'] == self.client.file_ref for p, m, *_ in self.client.calls))
        self.assertFalse(any(p in ('/close', '/submit', '/cancel') for p, *_ in self.client.calls))
        receipt = batchfetch.fetch(BID, self.out, client=self.client, fetch_limit=2*size)
        self.assertTrue(receipt['complete'])
        self.assertEqual((self.out/'two.files/nested/data.bin').read_bytes(), self.client.file_bytes)

    def test_default_one_gib_single_huge_file_requires_override(self):
        huge = dict(self.client.file_ref, bytes=fetchguard.DEFAULT_LIMIT+1)
        self.client.rows[0] = {'name': 'one', 'output_files_ref': self.client.store.put_json([dict(path='.lake/big', **huge)])}
        with self.assertRaises(fetchguard.FetchLimitExceeded) as caught:
            batchfetch.fetch(BID, self.out, client=self.client)
        self.assertIn('.lake/big', str(caught.exception))
        self.assertFalse(any(p == '/results' and m['ref'] == huge for p, m, *_ in self.client.calls))

    def test_environment_override_and_allow_large(self):
        with patch.dict(os.environ, {'SUPERCOMPUTER_FETCH_LIMIT': '0'}):
            with self.assertRaises(fetchguard.FetchLimitExceeded):
                batchfetch.fetch(BID, self.out, client=self.client)
            result = batchfetch.fetch(BID, self.out, client=self.client, allow_large=True)
        self.assertTrue(result['complete'])
        self.assertEqual((self.out/'one.files/nested/data.bin').read_bytes(), self.client.file_bytes)

    def test_no_output_files_not_blocked_by_zero(self):
        self.client.rows[0].pop('output_files_ref')
        self.assertTrue(batchfetch.fetch(BID, self.out, client=self.client, fetch_limit=0)['complete'])


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.store = resultfiles.Store(self.root/'server')
        f = self.root/'tiny'; f.write_bytes(b'123456')
        self.ref = self.store.put_file(f, format='file')
        self.manifest = self.store.put_json([dict(path='build/result', **self.ref)])
        self.s = session(strict=False)
        self.s.output_dir = self.root/'out'
        self.s.fetch_limit = 11
        self.s.allow_large = False
        original = self.s.client.req.side_effect
        def req(path, msg, **kw):
            if path == '/poll':
                n = msg['since']
                return {'rows': [{'name': ('one', 'two')[n], 'code': 0, 'output_files_ref': self.manifest}], 'done': n == 1}
            if path == '/results':
                return self.store.read(msg['ref'], msg['offset'], msg['length'])
            return original(path, msg, **kw)
        self.s.client.req.side_effect = req
        self.items = [ITEM, dict(ITEM, name='two')]

    def test_scheduled_aggregate_preflight_no_bodies_no_resubmission(self):
        callback = Mock()
        with self.assertRaises(fetchguard.FetchLimitExceeded):
            self.s.run(self.items, callback)
        self.assertFalse(any(m['ref'] == self.ref for m in requests(self.s, '/results')))
        self.assertEqual(len(requests(self.s, '/submit')), 1)
        self.assertEqual(len(requests(self.s, '/close')), 0)
        callback.assert_not_called()
        self.assertFalse(list(self.s.output_dir.glob('*.files')))
        # A refusal does not touch the remote retained body.
        self.assertEqual(self.store.read(self.ref, 0, 6), b'123456')

    def test_scheduled_override_and_exact_boundary(self):
        for allow, limit in [(False, 12), (True, 0)]:
            with self.subTest(allow=allow):
                self.s.allow_large, self.s.fetch_limit = allow, limit
                callback = Mock()
                self.s.run(self.items, callback)
                self.assertEqual(callback.call_count, 2)
                self.assertEqual((self.s.output_dir/'two.files/build/result').read_bytes(), b'123456')

    def direct(self, limit):
        from supercomputer import boat
        s = object.__new__(boat.Session)
        s.batch, s.fetch_limit, s.allow_large = 'legacy-batch', limit, False
        s.output_dir = self.root/'legacy-out'
        s.stop, s.cv = threading.Event(), threading.Condition()
        vm = SimpleNamespace(id='vm', type='large', slots=2, dead=False, released=False, client=object(), busy=[],
                             skipped=False, letting_go=None, t_ready=1, t_running=1, t_request=0,
                             t_end=None, http=None, lease={'owner': 'fixture'}, src=None)
        s.ready, s.vms = [vm], {'vm': vm}
        s.threads, s.early, s.seed_vm, s.short = [], True, None, False
        s._let_go = Mock()
        s.reads, s.acked, s.ticks = [], set(), 0
        def tick(vm, msg):
            s.acked.update(msg['acks'])
            s.ticks += 1
            if s.ticks == 1:
                return {'done': [dict(name='one', output_files_ref=self.manifest)]}
            if s.ticks == 2:
                self.assertNotIn('one', s.acked, 'body-bearing row ACKed before its transfer')
                return {'done': [dict(name=n, output_files_ref=self.manifest) for n in ('one', 'two')]}
            return {'done': []}
        s._tick = tick
        def read(client, bid, name, ref, offset, count):
            self.assertNotIn(name, s.acked)
            s.reads.append(ref)
            return self.store.read(ref, offset, count)
        s._result_chunk = read
        return s

    def run_direct(self, s, callback):
        watchdog = threading.Timer(5, s.stop.set)
        watchdog.start()
        try:
            s.run(self.items, callback)
        finally:
            watchdog.cancel()

    def test_direct_session_also_inventories_whole_batch(self):
        s, callback = self.direct(11), Mock()
        with self.assertRaises(fetchguard.FetchLimitExceeded):
            self.run_direct(s, callback)
        self.assertNotIn(self.ref, s.reads)
        self.assertEqual(s.acked, set())
        callback.assert_not_called()
        s._let_go.assert_not_called()
        self.assertTrue(s._output_fetch_refused)
        self.assertEqual(self.store.read(self.ref, 0, 6), b'123456')

    def test_direct_acks_only_after_all_bodies_have_arrived(self):
        s, callback = self.direct(12), Mock()
        self.run_direct(s, callback)
        self.assertEqual(callback.call_count, 2)
        self.assertEqual(s.acked, {'one', 'two'})
        self.assertEqual((s.output_dir/'two.files/build/result').read_bytes(), b'123456')

    def test_direct_refusal_stops_but_never_deletes_even_with_teardown(self):
        from supercomputer import boat
        s = self.direct(0)
        s._output_fetch_refused, s.teardown, s.creating, s.idle = True, True, 0, 0
        s.counts = dict(created=0, resumed=0, leased_running=1, warm=1, topped_up=0, not_resumed=0, failed=0)
        s.proj, s.t0, s.label = SimpleNamespace(name='test'), 0, 'test'
        s._release = Mock()
        with patch.object(boat, 'stop_vm', return_value=True) as stop, patch.object(boat, 'delete_vm') as delete, patch.object(boat, 'ledger'):
            result = s.close(kill=False)
        stop.assert_called_once_with('vm', {'owner': 'fixture'}, preserve_disk=True)
        delete.assert_not_called()
        self.assertEqual(result['output_files_retained_on_stopped_vms'], ['vm'])

    def test_refused_output_stop_cannot_fall_back_to_delete(self):
        from supercomputer import boat
        with patch.object(boat, 'fence_stop', return_value=True), patch.object(boat, '_stop_budget', return_value=90), \
                patch.object(boat, 'boat', return_value='Final snapshot failed') as api:
            self.assertFalse(stop_vm_implementation('vm', {'owner': 'fixture'}, preserve_disk=True))
        self.assertEqual(len(api.call_args_list), 1)
        self.assertEqual(api.call_args.args[0], 'stop')

    def test_cli_flags(self):
        from supercomputer import __main__ as cli
        with patch.object(cli.config, 'use'), patch.object(cli, '_run', return_value=0) as run:
            cli.main(['run', '--jobs', 'jobs.json', '--out', 'out', '--allow-large'])
            self.assertTrue(run.call_args.args[0].allow_large)
        with patch.object(cli.config, 'use'), patch.object(batchfetch, 'fetch', return_value={}) as fetch, patch('sys.stdout', io.StringIO()):
            cli.main(['batch', 'fetch', BID, '--out', 'out', '--allow-large'])
            self.assertTrue(fetch.call_args.kwargs['allow_large'])


if __name__ == '__main__':
    unittest.main()
