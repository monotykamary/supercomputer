"""Boat-only update regressions, including real inherited sockets (no provider API)."""
import contextlib
import itertools
import json
import os
import pathlib
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import agent, scheduler, controller
from supercomputer.rules import updates


class Guards(unittest.TestCase):
    def test_ack_requires_every_identity(self):
        for pool, admissions, items in itertools.product((False, True), repeat=3):
            self.assertEqual(updates.handoff_ready(pool, admissions, items), pool and admissions and items)

    def test_prune_empty_unavailable_holds_but_keep_every_owned_item(self):
        import socket
        import threading
        s = object.__new__(scheduler.Scheduler)
        s.lock, s.wake, s.batches = threading.RLock(), threading.Event(), {}
        names = ['stopped', 'agentless', 'dead', 'running', 'finishing', 'starting', 'foreign', 'usable', 'unknown', 'coming']
        s.vms = {n: scheduler.VMState(n, 'image', 'large', 'warm') for n in names}
        for vm in s.vms.values():
            vm.ready, vm.client = True, object()
        s.vms['agentless'].client = s.vms['coming'].client = None
        s.vms['coming'].ready = False
        s.vms['dead'].dead = True
        s.vms['running'].running['owned'] = object()
        s.vms['finishing'].finishing['owned'] = object()
        s.vms['starting'].starts.append({'start': 'pending'})
        s.vms['foreign'].foreign = 1
        observed = {n: {'state': 'stopped'} for n in ['stopped', 'running', 'finishing', 'starting', 'foreign']}
        observed['usable'] = {'state': 'running'}
        records = {'vms': {n: {'state': 'running', 'lease': {'pid': os.getpid(), 'host': socket.gethostname(),
                                                          'batch': 'scheduler', 'owner': 'stable-owner'}} for n in names}}
        @contextlib.contextmanager
        def locked():
            yield records
        registry = mock.Mock()
        registry.locked = locked
        with mock.patch.object(scheduler, 'Registry', return_value=registry), \
             mock.patch.object(scheduler.boat, 'stop_vm', side_effect=AssertionError('provider stop')), \
             mock.patch.object(scheduler.boat, 'delete_vm', side_effect=AssertionError('provider delete')), \
             mock.patch.object(scheduler.boat, 'shrink_ttl', side_effect=AssertionError('provider release')):
            with s.lock:
                s._prune_handoff_pool(observed)
        self.assertEqual(set(s._handoff_pruned), {'stopped', 'agentless', 'dead'})
        self.assertEqual(set(s.vms), set(names) - set(s._handoff_pruned))
        for name in s._handoff_pruned:
            self.assertIsNone(records['vms'][name]['lease'])
        for name in s.vms:
            self.assertEqual(records['vms'][name]['lease']['owner'], 'stable-owner')

    def test_uncertain_admission_cannot_be_pruned_as_empty(self):
        s = object.__new__(scheduler.Scheduler)
        s.recovery_blocked = True
        vm = scheduler.VMState('held', 'image', 'large', 'warm')
        vm.dead, vm.client = True, None
        self.assertTrue(s._handoff_vm_occupied(vm))
        self.assertTrue(updates.handoff_required(s._handoff_vm_occupied(vm), True))

    def test_rejected_candidate_cannot_start_workers_before_commit(self):
        s = object.__new__(scheduler.Scheduler)
        s._handoff_deferred = (123, [])
        s.recovery_blocked, s.vms = False, {'held': object()}
        s._worker = mock.Mock()
        with mock.patch.object(scheduler.boat, 'pid_alive', return_value=True):
            self.assertFalse(s._activate_handoff())
            s._worker.assert_not_called()
        with mock.patch.object(scheduler.boat, 'pid_alive', return_value=False):
            self.assertTrue(s._activate_handoff())
            self.assertTrue(s._activate_handoff())
            s._worker.assert_called_once_with(s.vms['held'])

    def test_nested_code_changes_hash(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            (root / 'rules').mkdir()
            (root / 'rules' / 'rule.py').write_text('v = 1\n')
            before = scheduler.code_hash(root)
            (root / 'rules' / 'rule.py').write_text('v = 200\n')
            self.assertNotEqual(before, scheduler.code_hash(root))

    def test_configured_controller_never_starts_local(self):
        with mock.patch.object(scheduler, 'controller', return_value={'vm': 'configured'}), \
             mock.patch.object(scheduler, '_remote', return_value=None), \
             mock.patch.object(scheduler.subprocess, 'Popen') as popen:
            with self.assertRaisesRegex((RuntimeError, SystemExit), 'controller'):
                scheduler.connect()
            with self.assertRaisesRegex(RuntimeError, 'local'):
                scheduler.ensure_running()
            popen.assert_not_called()

    def test_legacy_inflight_defers_agent_upgrade(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.object(agent, 'ROOT', pathlib.Path(d)), \
             mock.patch.object(agent, 'strict_unresolved', return_value=False):
            job = agent.ROOT / 'b' / 'batch' / 'jobs' / 'item'
            job.mkdir(parents=True)
            self.assertTrue(agent.upgrade_busy())  # missing PID is unresolved, not idle
            (job / 'done.json').write_text('{}')
            self.assertFalse(agent.upgrade_busy())
            (agent.ROOT / 'owned-upgrading').write_text('new')
            with self.assertRaisesRegex(ValueError, 'fenced'):
                agent.start(job.parents[1], {'name': 'another'})

    def test_stage_install_publishes_source_without_starting_scheduler(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            source = root / 'new-install' / 'supercomputer'
            source.mkdir(parents=True)
            (source / 'scheduler.py').write_text('# candidate\n')
            with mock.patch.object(scheduler, 'DIR', root / 'live'), \
                 mock.patch.object(scheduler, '__file__', str(source / 'scheduler.py')), \
                 mock.patch.object(scheduler, 'Client', side_effect=ConnectionRefusedError), \
                 mock.patch.object(scheduler.subprocess, 'Popen') as spawn:
                receipt = scheduler.stage_install()
                self.assertEqual(scheduler.installed_source(), source.parent)
                self.assertEqual(receipt['staged'], scheduler.code_hash(source))
                self.assertIsNone(receipt['active'])
                self.assertTrue((root / 'live/code' / receipt['staged'] / 'supercomputer/scheduler.py').exists())
                spawn.assert_not_called()

    def test_stage_refuses_a_mismatched_snapshot(self):
        with tempfile.TemporaryDirectory() as d, \
             mock.patch.object(scheduler, 'DIR', pathlib.Path(d)), \
             mock.patch.object(scheduler, 'code_hash', side_effect=['expected', 'different']), \
             mock.patch.object(scheduler.canary.Canary, 'snapshot', return_value=pathlib.Path(d)), \
             mock.patch.object(scheduler, 'Client') as client:
            with self.assertRaisesRegex(RuntimeError, 'source changed'):
                scheduler.stage_install()
            self.assertFalse((pathlib.Path(d) / 'install.json').exists())
            client.assert_not_called()

    def test_controller_stage_refuses_pre_handoff_runtime(self):
        client = mock.Mock()
        client.req.return_value = {'code': 'legacy'}
        with mock.patch.object(controller.scheduler, 'Client', return_value=client), \
             mock.patch.object(controller, 'ssh') as ssh:
            with self.assertRaisesRegex(RuntimeError, 'no hot-handoff protocol'):
                controller.stage('controller-fixture')
            ssh.assert_not_called()

    def test_pending_completion_frees_slot_but_survives_snapshot(self):
        import threading
        import types
        s = object.__new__(scheduler.Scheduler)
        s.lock, s.wake, s.code = threading.RLock(), threading.Event(), 'old'
        s.handing_off = False
        b = types.SimpleNamespace(id='b', closed=False, strict_once=False, slot_s=0, deliver=mock.Mock())
        item = types.SimpleNamespace(batch=b, name='x', run_name='x~s1', state='running',
                                     t_start=0, weight=1, timing=False, peers=0, fate=mock.Mock(),
                                     last_placement={'vm': 'v'}, launch_identity='identity', placed=1)
        b.items = {'x': item}
        vm = scheduler.VMState('v', 'image', 'large', 'warm')
        vm.ready = True
        vm.running[('b', 'x~s1')] = item
        s.vms, s.batches = {'v': vm}, {'b': b}
        captured = []
        def during_logs(*args):
            self.assertEqual(vm.used(), 0)
            captured.append(s._handoff_snapshot())
            s.handing_off = True
        s._finished_logs = during_logs
        row = {'batch': 'b', 'name': 'x~s1', 'code': 0, 'seconds': 0}
        self.assertFalse(s._accept_finished(vm, row))
        self.assertEqual(vm.acks, [])
        b.deliver.assert_not_called()
        self.assertEqual(captured[0]['vms'][0]['items'][0]['launch_identity'], 'identity')
        self.assertEqual(captured[0]['vms'][0]['items'][0]['state'], 'running')
        # The successor observes this same unacknowledged run, not a new start.
        vm.running[('b', 'x~s1')] = vm.finishing.pop(('b', 'x~s1'))
        item.state, s.handing_off = 'running', False
        s._finished_logs = lambda *args: None
        self.assertTrue(s._accept_finished(vm, row))
        b.deliver.assert_called_once()
        self.assertEqual(len(vm.acks), 1)

    def test_busy_agent_keeps_its_wire_version_until_upgrade(self):
        import types
        s = object.__new__(scheduler.Scheduler)
        s.agent_version = 'next-agent'
        vm = types.SimpleNamespace(client=object(), agent='running-agent')
        self.assertEqual(s._owned_client(vm).version, 'running-agent')
        vm.agent = ''
        self.assertEqual(s._owned_client(vm).version, 'next-agent')

    def test_service_tracks_handoff_child(self):
        text = controller.unit('scheduler', 'python3 -m supercomputer scheduler serve', '/tmp/tool', {})
        self.assertIn('NotifyAccess=all', text)
        self.assertIn('Type=notify', text)
        self.assertIn('KillMode=process', text)


class RealSocketHandoff(unittest.TestCase):
    def exercise(self, rejected, blocked=False):
        if os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') != '1':
            self.fail('run this regression on Boat with OWNED_LIFECYCLE_BOAT_TEST=1')
        with tempfile.TemporaryDirectory(prefix='sc-hot-') as tmp:
            root = pathlib.Path(tmp)
            tool = root / 'tool'
            shutil.copytree(pathlib.Path(scheduler.__file__).parent, tool / 'supercomputer',
                            ignore=shutil.ignore_patterns('__pycache__'))
            if blocked:
                with (tool / "supercomputer/scheduler.py").open("a") as f:
                    delay = "        time.sleep(10)\n" if blocked == "timeout" else ""
                    f.write("\n_publish_ack_ok = Scheduler._publish_handoff_ack\ndef _bad_ack(self):\n    if getattr(self, '_handoff_ack', None):\n" + delay + "        self._handoff_ack['items_ready'] = False\n    _publish_ack_ok(self)\nScheduler._publish_handoff_ack = _bad_ack\n")
            with socket.socket() as reserve:
                reserve.bind(('127.0.0.1', 0))
                port = reserve.getsockname()[1]
            env = {**os.environ, 'PYTHONPATH': str(tool), 'SUPERCOMPUTER_HOME': str(root),
                   'SUPERCOMPUTER_SRC': str(tool), 'SUPERCOMPUTER_LIVE': str(root / 'live'),
                   'SUPERCOMPUTER_CONTROLLER': '0', 'SUPERCOMPUTER_SCHEDULER_PORT': str(port),
                   'BOAT_BIN': '/bin/false'}  # never call the live provider, even if adoption wakes
            env.pop('NOTIFY_SOCKET', None)
            env.pop('SUPERCOMPUTER_HANDOFF_FDS', None)
            pids = set()
            proc = None
            log = (root / 'process.log').open('w')
            token = ''
            def req(path):
                data = b'{}' if path != '/health' else None
                r = urllib.request.Request(f'http://127.0.0.1:{port}' + path, data=data,
                                           headers={'X-SC-Token': token})
                with urllib.request.urlopen(r, timeout=1) as response:
                    return json.load(response)
            try:
                proc = subprocess.Popen([sys.executable, '-P', '-m', 'supercomputer', 'scheduler', 'serve',
                                         '--idle-exit', '0'], env=env, cwd=root, stdout=log, stderr=log)
                pids.add(proc.pid)
                deadline = time.monotonic() + 8
                while True:
                    try:
                        token = (root / 'live/scheduler/token').read_text().strip()
                        initial = req('/status')
                        break
                    except Exception:
                        if proc.poll() is not None or time.monotonic() >= deadline:
                            self.fail('initial boot failed: ' + (root / 'process.log').read_text())
                        time.sleep(.02)
                old = initial['code']
                source = tool / 'supercomputer' / 'scheduler.py'
                with source.open('a') as f:
                    f.write('\n# update test candidate\n')
                    if rejected:
                        f.write('\ndef _reject_boot(self):\n    raise RuntimeError("candidate boot rejected")\nScheduler.reload = _reject_boot\n')
                before = {name: (root / "live/scheduler" / name).read_bytes() if (root / "live/scheduler" / name).exists() else None
                          for name in ("handoff.json", "handoff.used.json", "handoff.ack")}
                req('/reload')
                began = last = time.monotonic()
                largest_gap = 0
                switched = False
                while time.monotonic() - began < (13 if blocked else 9):
                    try:
                        current = req('/health')
                        pids.add(current['pid'])
                        now = time.monotonic()
                        largest_gap = max(largest_gap, now - last)
                        last = now
                        if blocked and current['pid'] == proc.pid and "old scheduler resumed" in (root / "process.log").read_text():
                            state = req("/status")
                            switched = state["code"] == old
                            break
                        if not blocked and current['pid'] != proc.pid:
                            state = req('/status')
                            switched = state['code'] == old if rejected else state['code'] != old
                            if switched:
                                break
                    except Exception:
                        pass  # existing HTTP connections may close; the listener must not disappear
                    time.sleep(.02)
                ack_path = root / 'live/scheduler/handoff.ack'
                if ack_path.exists():
                    pids.add(json.loads(ack_path.read_text())['pid'])
                self.assertTrue(switched, (root / 'process.log').read_text())
                self.assertLess(largest_gap, 5, (root / 'process.log').read_text())
                if blocked:
                    self.assertIsNone(proc.poll())
                    self.assertEqual(int((root / "live/scheduler/pid").read_text()), proc.pid)
                    for name, data in before.items():
                        path = root / "live/scheduler" / name
                        self.assertEqual(path.read_bytes() if path.exists() else None, data, name)
                    time.sleep(.6)  # the original run loop must not fall into shutdown
                    self.assertEqual(req("/health")["pid"], proc.pid)
                    self.assertEqual(req("/status")["code"], old)
                else:
                    ack = json.loads(ack_path.read_text())
                    self.assertTrue(ack['items_ready'])
                    self.assertTrue(ack['admissions']['ready'])
                # A contender cannot acquire the inherited flock while the child serves.
                import fcntl
                with (root / 'live/scheduler/serve.lock').open('a') as contender:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                if rejected:
                    self.assertEqual(json.loads((root / 'live/scheduler/canary.json').read_text())['state'], 'rejected')
                print(json.dumps({'socket_handoff': 'blocked-resumed' if blocked else 'rollback' if rejected else 'success',
                                  'max_response_gap_s': round(largest_gap, 3), 'code': state['code']}))
            finally:
                pidfile = root / 'live/scheduler/pid'
                if pidfile.exists():
                    with contextlib.suppress(Exception):
                        pids.add(int(pidfile.read_text()))
                for pid in pids:
                    # Exact PID plus per-test environment identity; no name killing.
                    with contextlib.suppress(ProcessLookupError, FileNotFoundError, PermissionError):
                        if ('SUPERCOMPUTER_HOME=' + str(root)).encode() in pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0'):
                            os.kill(pid, signal.SIGKILL)
                if proc:
                    proc.wait(timeout=5)
                log.close()

    def test_both_candidates_timeout_without_blocking_health(self):
        self.exercise(False, blocked="timeout")

    def test_both_candidates_rejected_restore_original_socket_and_pid(self):
        self.exercise(False, blocked=True)

    def test_successful_socket_handoff(self):
        self.exercise(False)

    def test_rejected_candidate_rolls_back_on_same_socket(self):
        self.exercise(True)


if __name__ == '__main__':
    unittest.main()
