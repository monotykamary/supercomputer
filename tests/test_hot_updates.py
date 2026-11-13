"""Boat-only update regressions, including real inherited sockets (no provider API)."""
import contextlib
import http.client
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
import threading
import time
import unittest
import urllib.request
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import agent, scheduler, controller
from supercomputer.rules import updates


# This code is appended only to the isolated copy used by the real socket test.
# No provider calls: a synthetic held VM carries a legacy in-flight placement
# which exists in the live snapshot but not in the placement journal.
SNAPSHOT_ONLY_FIXTURE = r'''
class _FixtureAgent:
    def rpc(self, op, msg=None, **kw):
        return {"running": [{"batch": "snapshot-only", "name": "work~s3"}], "done": [], "cpus": 8, "model": "fixture-cpu"}
boat.owner_id = lambda: "fixture-owner"
boat.agent_client = lambda vid: _FixtureAgent()
boat.states = lambda *a, **kw: {}
boat.sandboxes = lambda *a, **kw: {}
boat.boat = lambda *a, **kw: ""
Scheduler._worker = lambda *a: None
Scheduler._start_on = lambda *a: None
Scheduler._adopt = lambda *a: None
_fixture_init = Scheduler.__init__
def _init_with_snapshot_only(self, *args, **kw):
    _fixture_init(self, *args, **kw)
    if os.environ.get("SUPERCOMPUTER_HANDOFF_FDS"):
        return
    msg = {"batch": "snapshot-only", "klass": 2, "image": "fixture-image", "ctx_b64": "",
           "tree": {"id": "fixture-tree", "files": {}, "links": {}}, "items": [{"name": "work", "cmd": "true"}]}
    qd = DIR / "queue"
    qd.mkdir(parents=True, exist_ok=True)
    (qd / "snapshot-only.json").write_text(json.dumps(msg))
    b = Batch(msg)
    it = b.items["work"]
    v = VMState("fixture-vm", b.image, "large", "warm")
    v.client, v.ready, v.model = _FixtureAgent(), True, "fixture-cpu"
    v.lease = {"ok": True, "owner": "fixture-owner", "holder": "fixture-owner", "boot": "fixture-boot", "epoch": 7, "vm_id": v.id}
    it.state, it.vm, it.run_name, it.placed = "starting", v, "work~s3", 3
    it.launch_identity = "fixture-launch"
    it.last_placement = {"name": it.name, "run_name": it.run_name, "vm": v.id, "image": v.image, "identity": it.launch_identity}
    it.fate.reloaded(v.id)
    it.fate.reattached(v.id)
    v.running[(b.id, it.run_name)] = it
    self.batches[b.id], self.vms[v.id] = b, v
    with Registry().locked() as d:
        d["vms"][v.id] = {"snapshot": b.image, "type": "large", "state": "running",
            "lease": {"pid": os.getpid(), "host": socket.gethostname(), "batch": "scheduler"}}
    self._refresh_status(blocking=True)
Scheduler.__init__ = _init_with_snapshot_only
_fixture_status = Scheduler._status_read
def _status_with_snapshot_only(self):
    result = _fixture_status(self)
    it = self.batches["snapshot-only"].items["work"]
    result["snapshot_fixture"] = {"placed": it.placed, "run_name": it.run_name, "identity": it.launch_identity}
    return result
Scheduler._status_read = _status_with_snapshot_only
'''


class Guards(unittest.TestCase):
    def test_mutation_guard_requires_an_active_unfrozen_owner(self):
        for flags in itertools.product((False, True), repeat=3):
            self.assertEqual(updates.http_mutation_allowed(*flags), not any(flags))

    def test_handoff_health_rules(self):
        self.assertFalse(updates.handoff_sample_fresh(10, 10, 20))
        self.assertTrue(updates.handoff_sample_fresh(20, 10, 20))
        self.assertFalse(updates.handoff_sample_fresh(21, 10, 20))
        self.assertEqual(updates.handoff_idle_slots(1, 45, 45), 0)
        self.assertEqual(updates.handoff_idle_slots(1, 46, 45), 1)
        self.assertEqual(updates.handoff_health_streak(1, False, 0, 1), 0)
        self.assertEqual(updates.handoff_health_streak(0, True, 1, 0), 2)
        self.assertEqual(updates.handoff_health_streak(0, True, 0, 1), 1)
        self.assertEqual(updates.handoff_health_streak(1, True, 0, 1), 2)
        self.assertFalse(updates.handoff_can_pass(601, 600, 0, True))
        self.assertFalse(updates.handoff_can_pass(601, 600, 1, False))
        self.assertTrue(updates.handoff_can_pass(601, 600, 0, False))

    def test_handoff_idle_uses_grace_and_resets_on_placement_or_demand_loss(self):
        from types import SimpleNamespace
        s = object.__new__(scheduler.Scheduler)
        vm = SimpleNamespace(t_placed_real=0)
        s.vms = {"vm": vm}
        current = {"vm": ("image", 1, "sweep w1")}
        def waste(now, live, **kw):
            self.assertEqual(kw["grace"], 0)
            self.assertTrue(kw["idle_only"])
            kw["by_vm"].update(current)
        s._waste = waste
        with mock.patch.object(scheduler, "SUB_PREEMPT", 45):
            self.assertEqual([s._handoff_idle_slots(t, [vm]) for t in (25, 40, 70, 71)], [0, 0, 0, 1])
            vm.t_placed_real = 72
            self.assertEqual(s._handoff_idle_slots(85, [vm]), 0)
            self.assertEqual(s._handoff_idle_slots(131, [vm]), 1)
            current["vm"] = ("image", 2, "sweep w1")
            self.assertEqual(s._handoff_idle_slots(132, [vm]), 1)  # new slot has no old age
            current.clear()
            self.assertEqual(s._handoff_idle_slots(140, [vm]), 0)
            current["vm"] = ("image", 1, "sweep w1")
            self.assertEqual(s._handoff_idle_slots(200, [vm]), 0)
            self.assertEqual(s._handoff_idle_slots(246, [vm]), 1)

    def test_handoff_idle_observations_continue_after_the_two_ownership_checks(self):
        import threading
        from types import SimpleNamespace
        s = object.__new__(scheduler.Scheduler)
        s.lock, s.vms, s.code = threading.RLock(), {"vm": SimpleNamespace(dead=False)}, "new"
        s._handoff = {"t": 1000, "held": ["vm"], "taken": ["vm"], "checks": 0}
        h = scheduler.health.Health()
        s._h = lambda: h
        s.canary = mock.Mock()
        s.canary.handoff_verdict.return_value = ("", "")
        s._handoff_idle_slots = mock.Mock(side_effect=[0, 0, 1])
        reg = mock.Mock()
        reg.read.return_value = {"vms": {}}
        with mock.patch.object(scheduler, "Registry", return_value=reg), mock.patch.object(scheduler, "log"):
            s._handoff_check(1015)
            s._handoff_check(1030)
            s.vms.clear()  # an intentional later release is not a failed initial takeover
            s._handoff_check(1045)
        self.assertEqual(len(h.ev["handoff"]), 3)
        self.assertEqual(h.ev["handoff"][-1]["idle"], 1)
        self.assertEqual(h.ev["handoff"][-1]["missing"], 0)
        self.assertEqual(s.canary.handoff_verdict.call_count, 2)

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
            (source / '__init__.py').write_text('')
            (source / 'scheduler.py').write_text('# candidate\n')
            with mock.patch.object(scheduler, 'DIR', root / 'live'), \
                 mock.patch.object(scheduler, '__file__', str(source / 'scheduler.py')), \
                 mock.patch.object(scheduler, 'Client', side_effect=ConnectionRefusedError), \
                 mock.patch.object(scheduler, 'ensure_running') as spawn:
                receipt = scheduler.stage_install()
                self.assertEqual(scheduler.installed_source(), root / "live/code" / receipt["staged"])
                self.assertEqual(receipt['staged'], scheduler.code_hash(source))
                self.assertIsNone(receipt['active'])
                self.assertTrue((root / 'live/code' / receipt['staged'] / 'supercomputer/scheduler.py').exists())
                spawn.assert_not_called()
                manifest = json.loads((root / "live/install.json").read_text())
                self.assertEqual(manifest["prepared"]["python"], sys.executable)
                self.assertTrue(list((root / "live/code" / receipt["staged"]).rglob("scheduler.*.pyc")))

    def test_stage_rejects_compile_and_import_errors_before_publication(self):
        for source_text in ('not valid python !!', 'import nonexistent_snapshot_dependency'):
            with self.subTest(source=source_text), tempfile.TemporaryDirectory() as d:
                root = pathlib.Path(d)
                source = root / 'install/supercomputer'
                source.mkdir(parents=True)
                (source / '__init__.py').write_text('')
                (source / 'scheduler.py').write_text(source_text)
                with mock.patch.object(scheduler, 'DIR', root / 'live'), \
                     mock.patch.object(scheduler, '__file__', str(source / 'scheduler.py')), \
                     mock.patch.object(scheduler, 'Client') as client:
                    with self.assertRaisesRegex(RuntimeError, 'compile/import failed'):
                        scheduler.stage_install()
                    self.assertFalse((root / 'live/install.json').exists())
                    client.assert_not_called()

    def test_heartbeat_keeps_leases_alive_during_readiness(self):
        import types
        s = object.__new__(scheduler.Scheduler)
        vm = types.SimpleNamespace(id='held', dead=False, lease={'epoch': 7}, ttl=100)
        s.stop = mock.Mock()
        s.stop.wait.side_effect = [False, True]
        s.handing_off, s.vms = True, {'held': vm}
        s._renew_lease = mock.Mock(return_value=True)
        with mock.patch.object(scheduler.boat, 'boat') as extend:
            s._heartbeat()
            s._renew_lease.assert_called_once_with(vm)
            extend.assert_called_once()
        self.assertEqual(vm.lease, {'epoch': 7})

    def test_failed_paused_renewal_vetoes_commit_without_changing_snapshot(self):
        import types
        s = object.__new__(scheduler.Scheduler)
        s.handing_off, s._handoff_lease_lost = True, False
        s.lock = threading.RLock()
        s._drop_stale = mock.Mock()
        vm = types.SimpleNamespace(id='held', lease={'epoch': 7})
        with mock.patch.object(scheduler.boat, 'lease_vm', return_value={'ok': False, 'stale': True}):
            self.assertFalse(s._renew_lease(vm))
        self.assertTrue(s._handoff_lease_lost)
        s._drop_stale.assert_not_called()

    def test_budget_and_timeout_cooldown(self):
        self.assertEqual(updates.handoff_budget(82, 340, 1500), 31000)
        self.assertEqual(updates.handoff_budget(-1, -1, -1), 10000)
        self.assertEqual(updates.handoff_budget(10000, 100000, 100000), 120000)
        with tempfile.TemporaryDirectory() as d:
            c = scheduler.canary.Canary(pathlib.Path(d))
            c.handoff_failed('slow', 'startup deadline', True, 100)
            self.assertFalse(c.rejected('slow'))
            self.assertFalse(c.retry_ready('slow', 159))
            self.assertTrue(c.retry_ready('slow', 160))
            c.handoff_failed('bad', 'missing identity', False, 100)
            self.assertTrue(c.rejected('bad'))
            self.assertFalse(c.retry_ready('bad', 10000))

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
    def exercise(self, rejected, blocked=False, slow=False, grouped=False, snapshot_only=False, http_io=False, http_shapes=False):
        if os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') != '1':
            self.fail('run this regression on Boat with OWNED_LIFECYCLE_BOAT_TEST=1')
        with tempfile.TemporaryDirectory(prefix='sc-hot-') as tmp:
            root = pathlib.Path(tmp)
            tool = root / 'tool'
            shutil.copytree(pathlib.Path(scheduler.__file__).parent, tool / 'supercomputer',
                            ignore=shutil.ignore_patterns('__pycache__'))
            if http_io or http_shapes:
                with (tool / "supercomputer/scheduler.py").open("a") as f:
                    f.write("\nHTTP_REQUEST_TIMEOUT_S = 1.0\ndef _large_http_reply(self, msg):\n    (DIR / 'slow-reply.ready').touch()\n    return b'x' * (8 * 1024 * 1024)\nScheduler.hint = _large_http_reply\n")
            if snapshot_only:
                with (tool / "supercomputer/scheduler.py").open("a") as f:
                    f.write(SNAPSHOT_ONLY_FIXTURE)
            if slow:
                with (tool / "supercomputer/scheduler.py").open("a") as f:
                    f.write("\n_publish_fast = Scheduler._publish_handoff_ack\ndef _slow_ack(self):\n    if getattr(self, '_handoff_ack', None):\n        time.sleep(5)\n    _publish_fast(self)\nScheduler._publish_handoff_ack = _slow_ack\n")
            if blocked:
                with (tool / "supercomputer/scheduler.py").open("a") as f:
                    delay = "        time.sleep(180)\n" if blocked == "timeout" else ""
                    mutation = "        self._handoff_ack['distinct_vm_groups'] = {}\n" if blocked == "groups" else "        self._handoff_ack['items_ready'] = False\n"
                    f.write("\n_publish_ack_ok = Scheduler._publish_handoff_ack\ndef _bad_ack(self):\n    if getattr(self, '_handoff_ack', None):\n" + delay + mutation + "    _publish_ack_ok(self)\nScheduler._publish_handoff_ack = _bad_ack\n")
            with socket.socket() as reserve:
                reserve.bind(('127.0.0.1', 0))
                port = reserve.getsockname()[1]
            env = {**os.environ, 'PYTHONPATH': str(tool), 'SUPERCOMPUTER_HOME': str(root),
                   'SUPERCOMPUTER_SRC': str(tool), 'SUPERCOMPUTER_LIVE': str(root / 'live'),
                   'SUPERCOMPUTER_CONTROLLER': '0', 'SUPERCOMPUTER_SCHEDULER_PORT': str(port),
                   'BOAT_BIN': '/bin/false'}  # never call the live provider, even if adoption wakes
            env.pop('NOTIFY_SOCKET', None)
            env.pop('SUPERCOMPUTER_HANDOFF_FDS', None)
            if grouped:
                qd = root / "live/scheduler/queue"
                qd.mkdir(parents=True)
                record = {"batch": "completed-group", "klass": 2, "image": "image", "ctx_b64": "",
                          "tree": {"id": "tree", "files": {}, "links": {}},
                          "items": [{"name": "done", "distinct_vm_group": "run/stage/case/rep"}], "placement_contract": 1}
                (qd / "completed-group.json").write_text(json.dumps(record))
                (qd / "completed-group.rows").write_text(json.dumps({"name": "done", "code": 0}) + "\n")
                (qd / "completed-group.placed").write_text(json.dumps({"name": "done", "run_name": "done",
                    "vm": "finished-vm", "image": "image", "distinct_vm_group": "run/stage/case/rep"}) + "\n")
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
                if http_io:
                    self.check_slow_http_clients(port, token, req, root)
                if http_shapes:
                    self.check_legacy_http_bodies(port, token)
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
                while time.monotonic() - began < (45 if blocked == "timeout" else 15):
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
                            switched = switched and (root / "live/scheduler/handoff.ack").exists() and proc.poll() is not None
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
                    if blocked == "timeout":
                        canary_state = json.loads((root / "live/scheduler/canary.json").read_text())
                        self.assertEqual(canary_state["state"], "retryable")
                        self.assertNotIn(scheduler.code_hash(tool / "supercomputer"), canary_state.get("rejected", []))
                else:
                    ack = json.loads(ack_path.read_text())
                    self.assertTrue(ack['items_ready'])
                    self.assertTrue(ack['admissions']['ready'])
                    if snapshot_only:
                        self.assertEqual(state["snapshot_fixture"], {"placed": 3, "run_name": "work~s3", "identity": "fixture-launch"})
                        offered = json.loads((root / "live/scheduler/handoff.used.json").read_text())
                        self.assertEqual(offered["vms"][0]["items"][0]["placed"], 3)
                        self.assertFalse((root / "live/scheduler/queue/snapshot-only.placed").exists())
                    if grouped:
                        expected = {"completed-group": {"run/stage/case/rep": ["finished-vm"]}}
                        self.assertEqual(ack["distinct_vm_groups"], expected)
                        self.assertEqual(json.loads((root / "live/scheduler/handoff.used.json").read_text())["distinct_vm_groups"], expected)
                    self.assertTrue(list((root / "live/scheduler/code" / state["code"]).rglob("scheduler.*.pyc")))
                    if slow:
                        self.assertGreater(time.monotonic() - began, 5)
                # A contender cannot acquire the inherited flock while the child serves.
                import fcntl
                with (root / 'live/scheduler/serve.lock').open('a') as contender:
                    with self.assertRaises(BlockingIOError):
                        fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
                if rejected:
                    self.assertEqual(json.loads((root / 'live/scheduler/canary.json').read_text())['state'], 'rejected')
                    self.assertIn("exited 1 before readiness ACK", (root / "process.log").read_text())
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

    def check_legacy_http_bodies(self, port, token):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        def post(path, body, expected):
            conn.request("POST", path, body=body, headers={"X-SC-Token": token, "User-Agent": "legacy-body-fixture"})
            response = conn.getresponse()
            data = json.loads(response.read())
            self.assertEqual(response.status, expected, data)
            return data, response
        try:
            before, _ = post("/health", b'{}', 200)
            for body in (b'', b'{}', b'null', b'[]', b'false', b'42', b'"text"'):
                post("/poll", body, 400)
                post("/capabilities", body, 200)
            for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-32"):
                body = json.dumps({"id": "legacy-\u00e9", "wait": 0}, ensure_ascii=False).encode(encoding)
                post("/poll", body, 200)
            first_socket = conn.sock
            for key in ("id", "batch"):
                post("/poll", json.dumps({key: "legacy", "wait": 0}).encode(), 200)
                self.assertIs(conn.sock, first_socket)
            for body in (b'{', b'{x', b"{'id':'legacy'}", b'{"id":"legacy"', b' ', b'\xff'):
                _, response = post("/poll", body, 400)
                self.assertEqual(response.getheader("Connection"), "close")
                post("/poll", b'{"id":"legacy","wait":0}', 200)  # same client reconnects cleanly
        finally:
            conn.close()

        def header(body, close=False, declared=None):
            length = len(body) if declared is None else declared
            return (f"POST /poll HTTP/1.1\r\nHost: localhost\r\nX-SC-Token: {token}\r\n"
                    f"User-Agent: fragmented-legacy\r\nContent-Length: {length}\r\n"
                    f"Connection: {'close' if close else 'keep-alive'}\r\n\r\n").encode()
        body = json.dumps({"id": "split", "note": "\u00e9", "wait": 0}, ensure_ascii=False).encode()
        second = b'{"batch":"next","wait":0}'
        with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
            cut = body.index(b'\xc3') + 1  # split a UTF-8 code point as well as the JSON object
            sock.sendall(header(body) + body[:cut])
            time.sleep(.03)
            sock.sendall(body[cut:] + header(second, close=True) + second)
            with sock.makefile("rb") as stream:
                for _ in range(2):
                    status = stream.readline()
                    self.assertIn(b" 200 ", status)
                    headers = {}
                    while True:
                        line = stream.readline()
                        self.assertTrue(line)
                        if line == b"\r\n":
                            break
                        key, value = line.decode().split(":", 1)
                        headers[key.lower()] = value.strip()
                    payload = json.loads(stream.read(int(headers["content-length"])))
                    self.assertTrue(payload.get("done"), payload)
        with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
            sock.sendall(header(b'{', declared=20) + b'{')
            sock.shutdown(socket.SHUT_WR)
            response = http.client.HTTPResponse(sock)
            response.begin()
            response.read()
            self.assertEqual(response.status, 408)

        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        try:
            after, _ = post("/health", b'{}', 200)
            self.assertEqual(after["targets"]["errors"]["value"], before["targets"]["errors"]["value"])
            self.assertGreaterEqual(after["client_4xx"]["count"], 14)
            self.assertEqual(after["client_4xx"]["recent"][-1]["read"], 1)
            self.assertEqual(after["client_4xx"]["recent"][-1]["declared"], "20")
            deadline = time.monotonic() + 3
            while True:
                status, _ = post("/status", b'{}', 200)
                if status["health"]["client_4xx"]["count"] == after["client_4xx"]["count"]:
                    break
                self.assertLess(time.monotonic(), deadline, status["status_snapshot"])
                time.sleep(.02)
            self.assertTrue(status["status_snapshot"]["ready"])
        finally:
            conn.close()
        print(json.dumps({"legacy_http_bodies": "passed", "fragmented_pipelined_keepalive": "passed",
                          "server_errors": after["targets"]["errors"]["value"], "client_4xx": after["client_4xx"]["count"]}))

    def test_legacy_json_bodies_fragmentation_and_keepalive(self):
        self.exercise(False, http_shapes=True)

    def check_slow_http_clients(self, port, token, req, root):
        def request(path, body, sent=None, receive_buffer=None):
            sock = socket.socket()
            if receive_buffer:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
            sock.settimeout(3)
            sock.connect(("127.0.0.1", port))
            header = (f"POST {path} HTTP/1.1\r\nHost: localhost\r\nX-SC-Token: {token}\r\n"
                      f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n").encode()
            sock.sendall(header + (body if sent is None else body[:sent]))
            return sock

        with request("/drain", b'{"on":false}', sent=5) as partial:
            time.sleep(.05)  # let the partial reader begin before the competing mutation
            began = time.monotonic()
            req("/drain")
            read_gap = time.monotonic() - began
            self.assertLess(read_gap, .35, "partial body serialized an unrelated mutation")
            response = partial.recv(4096)
            self.assertTrue(not response or b" 408 " in response, response)

        with request("/hint", b'{}', receive_buffer=1024):
            deadline = time.monotonic() + 2
            while not (root / "live/scheduler/slow-reply.ready").exists():
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.01)
            time.sleep(.05)  # the 8 MiB response cannot fit in this client's receive window
            began = time.monotonic()
            req("/drain")
            write_gap = time.monotonic() - began
            self.assertLess(write_gap, .35, "stalled response serialized an unrelated mutation")
        print(json.dumps({"partial_body_mutation_s": round(read_gap, 4),
                          "stalled_response_mutation_s": round(write_gap, 4)}))

    def test_slow_http_clients_do_not_serialize_mutations(self):
        self.exercise(False, http_io=True)

    def test_slow_cold_candidate_gets_more_than_four_seconds(self):
        self.exercise(False, slow=True)

    def test_both_candidates_timeout_without_blocking_health(self):
        self.exercise(False, blocked="timeout")

    def test_both_candidates_rejected_restore_original_socket_and_pid(self):
        self.exercise(False, blocked=True)

    def test_successful_socket_handoff(self):
        self.exercise(False)

    def test_snapshot_only_inflight_item_without_placement_journal(self):
        self.exercise(False, snapshot_only=True)

    def test_completed_group_history_is_carried_in_snapshot_and_ack(self):
        self.exercise(False, grouped=True)

    def test_missing_group_history_ack_keeps_original_owner(self):
        self.exercise(False, blocked="groups", grouped=True)

    def test_rejected_candidate_rolls_back_on_same_socket(self):
        self.exercise(True)


if __name__ == '__main__':
    unittest.main()
