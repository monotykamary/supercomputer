"""No Modal dispatch: exhausted boat work and RunPod work remain terminal."""
import argparse
import contextlib
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

os.environ['SUPERCOMPUTER_LIVE'] = tempfile.mkdtemp(prefix='sc-no-modal-')
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat, config, fanout, runpod, scheduler, treadmill, volumes
from supercomputer import __main__ as cli
from supercomputer.rules import fallback

THREE = 'boat failed it 3 times (last: its row lost on bx_a)'


class NoModalTests(unittest.TestCase):
    def test_lost_routes_never_modal(self):
        for own in (False, True):
            for enabled in (False, True):
                for failures in range(6):
                    self.assertEqual(fallback.lost_route(own, failures, 3, enabled),
                                     'failed' if own or failures >= 3 else 'boat')

    def test_call_budget_is_terminal(self):
        fate = fallback.AutoFallback(3)
        self.assertEqual([fate.outcome('lost') for _ in range(4)], ['again', 'again', 'raise', 'raise'])
        self.assertEqual(fate.boat_fails, 3)
        self.assertFalse(fate.on_modal)

    def test_exhausted_callable_never_calls_modal(self):
        ex = object.__new__(fanout.AutoEx)
        ex.strict_once = False
        ex.bx = Mock()
        ex.bx.remote.side_effect = RuntimeError(THREE)
        ex.fb = fallback.AutoFallback(3)
        ex.where = {'boat': 0}
        fn = Mock()
        with self.assertRaisesRegex(RuntimeError, '3 times'):
            ex.remote(fn, 1)
        fn.remote.assert_not_called()
        self.assertEqual(ex.bx.remote.call_count, 1)

    def run_jobs(self, jobs, **kwargs):
        with tempfile.TemporaryDirectory() as out, patch.object(fanout, 'project', return_value=types.SimpleNamespace(images={})), \
                patch.object(fanout.time, 'sleep'), patch.object(fallback, 'preflight_on', return_value=False):
            code = fanout.run_jobs(None, jobs, out, '', lambda c: c, **kwargs)
            return code, json.loads((pathlib.Path(out) / 'backends.json').read_text())

    def test_exhausted_boat_receipt_is_not_rerouted(self):
        row = {'code': -1, 'seconds': -1, 'lost': True, 'boat_fails': 3, 'error': THREE,
               'out': '', 'err': THREE, 'placement': {'vm': 'bx_final'}}
        modal = Mock(side_effect=AssertionError('Modal invoked'))
        with patch.object(boat, 'boat_jobs', return_value=({'job': row}, {})) as native:
            code, meta = self.run_jobs([{'name': 'job', 'cmd': 'true'}], modal_jobs=modal)
        self.assertEqual(code, 3)
        self.assertEqual(native.call_count, 1)
        self.assertEqual(row['status'], 'LOST')
        self.assertEqual(row['boat_fails'], 3)
        self.assertEqual(meta['where'], {'boat': 1})
        self.assertEqual(meta['fallbacks'], [])
        modal.assert_not_called()

    def test_unavailable_retries_only_on_boat(self):
        modal = Mock(side_effect=AssertionError('Modal invoked'))
        with patch.object(boat, 'boat_jobs', side_effect=boat.Unavailable('unavailable')) as native:
            code, meta = self.run_jobs([{'name': 'job', 'cmd': 'true'}], modal_jobs=modal)
        self.assertEqual(code, 3)
        self.assertEqual(native.call_count, 3)
        self.assertEqual(meta['where'], {'boat': 1})
        modal.assert_not_called()

    def test_cli_refuses_modal_before_sync(self):
        for backend, job_backend, strict in [('modal', None, False), ('auto', 'modal', False), ('modal', None, True)]:
            with self.subTest(backend=backend, job_backend=job_backend, strict=strict), tempfile.TemporaryDirectory() as d:
                path = pathlib.Path(d) / 'jobs.json'
                path.write_text(json.dumps([{'name': 'job', 'cmd': 'true', 'backend': job_backend}]))
                args = argparse.Namespace(jobs=str(path), backend=backend, strict_once=strict)
                with patch('supercomputer.controller.maybe_sync') as sync, self.assertRaisesRegex(SystemExit, 'Modal support removed; use boat or runpod'):
                    cli._run(args)
                sync.assert_not_called()

    def test_native_submit_refuses_modal(self):
        for msg in ({'backend': 'modal'}, {'items': [{'name': 'job', 'backend': 'modal'}]}):
            with self.assertRaisesRegex(ValueError, 'Modal support removed'):
                scheduler.Scheduler._validate_submission(msg)

    def test_callable_cli_refuses_modal(self):
        with patch.object(sys, 'argv', ['runner', '--backend', 'modal']), patch.object(fanout, 'AutoEx') as ex:
            with self.assertRaises(SystemExit):
                fanout.cli(lambda ex: None, 'runner.py')
            ex.assert_not_called()

    def test_gpu_preflight_failure_never_calls_modal(self):
        modal = Mock(side_effect=AssertionError('Modal invoked'))
        with patch.object(runpod, 'preflight', return_value='no RunPod credentials'):
            rows, where, moved = runpod.gpu_jobs([{'name': 'gpu', 'gpu': True}], '/unused', '', lambda c: c, gpu_modal=modal)
        self.assertEqual(rows['gpu']['status'], 'FAILED')
        self.assertEqual(where, {'gpu': 'runpod'})
        self.assertEqual(moved, {})
        modal.assert_not_called()

    def test_gpu_call_retries_two_pods_never_modal(self):
        modal = Mock(side_effect=AssertionError('Modal invoked'))
        with patch.object(runpod, 'preflight', return_value=None), patch.object(runpod, 'call', side_effect=RuntimeError('pod lost')) as call:
            with self.assertRaisesRegex(RuntimeError, 'pod lost'):
                fanout.gpu_call('r.py', 'f', [], modal_fn=modal)
        self.assertEqual(call.call_count, 2)
        modal.remote.assert_not_called()

    def test_reap_preserves_tickets_after_failed_stop(self):
        for stop_code in (0, 1):
            with self.subTest(stop_code=stop_code), tempfile.TemporaryDirectory() as d:
                ticket = pathlib.Path(d) / 'ap-legacy'
                ticket.write_text('999999')
                def call(args, **kw):
                    return types.SimpleNamespace(returncode=stop_code if 'stop' in args else 0, stdout='[]')
                with patch.object(config, 'live_dir', return_value=pathlib.Path(d)), \
                        patch.object(boat, 'pid_alive', return_value=False), patch.object(boat, 'reap'), \
                        patch('shutil.which', side_effect=lambda name: '/test/modal' if name == 'modal' else None), \
                        patch('subprocess.run', side_effect=call):
                    cli._reap(None)
                self.assertEqual(ticket.exists(), stop_code != 0)

    def test_failed_inventory_never_proves_empty(self):
        import io
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as d, patch.object(config, 'live_dir', return_value=pathlib.Path(d)), \
                patch.object(boat, 'reap'), patch('shutil.which', side_effect=lambda name: '/test/modal' if name == 'modal' else None), \
                patch('subprocess.run', return_value=types.SimpleNamespace(returncode=1, stdout='')), \
                contextlib.redirect_stdout(output):
            cli._reap(None)
        self.assertIn('unverified', output.getvalue())
        self.assertNotIn('removable', output.getvalue())

    def test_treadmill_terminal_item_not_replayed_with_partial_retry(self):
        with tempfile.TemporaryDirectory() as d, patch.object(treadmill, 'TM', pathlib.Path(d)):
            root = pathlib.Path(d) / 'subs' / 'one'
            root.mkdir(parents=True)
            (root / 'meta.json').write_text(json.dumps({'name': 'one', 'status': 'queued', 'tree_key': 'tree', 'jobs': 2}))
            (root / 'jobs.json').write_text(json.dumps([{'name': 'exhausted', 'cmd': 'false'}, {'name': 'pending', 'cmd': 'true'}]))
            (root / 'tree.tar.gz').write_bytes(b'fixture')
            with patch.object(treadmill, 'Batch') as batch, patch.object(boat, 'job_items', side_effect=lambda jobs, *args: (jobs, [])):
                first = treadmill.Submission(None, root)
                first.start(1)
                on_row = batch.call_args.args[5]
                on_row({'name': 'exhausted'}, {'code': -1, 'status': 'LOST', 'terminal': True, 'boat_fails': 3})
                second = treadmill.Submission(None, root)
                second.start(1)
                self.assertEqual([it['name'] for it in batch.call_args.args[3]], ['pending'])
            saved = json.loads((root / 'terminal-results.json').read_text())
            self.assertEqual(saved['exhausted']['boat_fails'], 3)

    def test_legacy_execution_helpers_refuse(self):
        for call in (lambda: fanout.ModalCtx(None), lambda: fanout.ModalEx(None),
                     lambda: treadmill.on_modal([], pathlib.Path('.'), None),
                     lambda: runpod.modal_gpu_jobs([], '', None, None),
                     lambda: volumes.modal_run('inventory', [], 1)):
            with self.assertRaisesRegex(ValueError, 'Modal support removed'):
                call()

    def test_treadmill_exhaustion_never_calls_modal(self):
        rows = {}
        class Session:
            scheduled, vms = True, {}
            stop = types.SimpleNamespace(set=lambda: None)
            def ensure(self, n): pass
            def run(self, items, row, on_lost=None, **kw):
                for item in items:
                    if item['name'] == 'exhausted': on_lost(item, THREE)
                    elif item['name'] == 'unserved': on_lost(item, 'no row')
                    else: row(item, {'code': 0})
            def collect(self): return {}
            def close(self): return {}
        with tempfile.TemporaryDirectory() as d, patch.object(treadmill, 'TM', pathlib.Path(d)), \
                patch.object(boat, 'session', return_value=Session()), patch.object(treadmill, 'on_modal') as modal:
            tm = types.SimpleNamespace(max_wait=0, modal=True, main=pathlib.Path('.'))
            items = [{'name': n, 'weight': 1} for n in ('ok', 'exhausted', 'unserved')]
            batch = treadmill.Batch(tm, 'test', 'a', items, 1, lambda it, row: rows.__setitem__(it['name'], row), root=pathlib.Path('.'))
            batch.thread.join(10)
            self.assertFalse(batch.thread.is_alive())
            modal.assert_not_called()
        self.assertTrue(rows['exhausted']['terminal'])
        self.assertEqual(rows['exhausted']['status'], 'LOST')
        self.assertTrue(rows['unserved']['retry'])


if __name__ == '__main__':
    unittest.main()
