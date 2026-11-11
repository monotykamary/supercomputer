"""Boat-only bootstrap tests. Actual staged source, local files, no processes.
Only Main submits/runs these centrally; authoring is not execution evidence.
"""
import ast
import base64
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

if sys.platform != 'linux' or os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') != '1':
    raise RuntimeError('Boat-only test; never import/execute on laptop')
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import boat, scheduler


class BootstrapTransport(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='bootstrap-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / '.sc'
        self.root.mkdir(mode=0o700)
        self.old = b'# old agent must survive a failed upload\n'
        (self.root / 'agent.py').write_bytes(self.old)
        self.source = (Path(boat.__file__).parent / 'agent.py').read_bytes()
        self.calls, self.appends, self.failure = [], 0, None
        self.race_after_claim = False
        self.token = 'literal token ; $(touch NEVER) "quoted"'
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.lease = dict(owner='fixture-owner', vm_id='fake-vm', boot=boot, epoch=3)
        import time
        self.record = dict(self.lease, holder=self.lease['owner'], state='held',
                           renewed=time.clock_gettime(time.CLOCK_BOOTTIME), draining=False)
        (self.root / 'lease.json').write_text(json.dumps(self.record))
        for target, value in [('VM_ROOT', str(self.root)), ('LIVE', Path(self.tmp.name) / 'live'),
                              ('AGENTS', Path(self.tmp.name) / 'live' / 'agents'), ('exec_', self.execute)]:
            p = patch.object(boat, target, value); p.start(); self.addCleanup(p.stop)
        p = patch.object(boat, 'owner_id', return_value=self.lease['owner']); p.start(); self.addCleanup(p.stop)
        for target in ('Popen', 'run'):
            p = patch.object(boat.subprocess, target, side_effect=AssertionError('no child processes'))
            p.start(); self.addCleanup(p.stop)
        p = patch.object(boat, 'boat', side_effect=AssertionError('no provider calls')); p.start(); self.addCleanup(p.stop)

    def execute(self, vm, argv, timeout=60, sem=None):
        self.assertEqual(vm, 'fake-vm')
        self.calls.append(list(argv))
        self.assertLessEqual(max(len(a.encode()) for a in argv), boat.EXEC_ARG)
        argv = list(argv)
        if argv[:2] == ['python3', str(self.root / 'agent.py')]:
            # Receipt only: never execute keys/start-daemon or arbitrary commands.
            return {'exitCode': 0, 'stdout': json.dumps({'ok': True, 'exec': argv}), 'stderr': ''}
        self.assertEqual(argv[:3], ['python3', '-c', boat._AGENT_BOOTSTRAP])
        if argv[3] == 'append':
            self.appends += 1
            if self.appends == 2:
                if self.failure == 'timeout': raise TimeoutError('injected append timeout')
                if self.failure == 'exit': return {'exitCode': 1, 'stdout': '', 'stderr': 'injected failure'}
                if self.failure == 'short': argv[8] = argv[8][:-4]
                if self.failure == 'corrupt': argv[8] = ('A' if argv[8][0] != 'A' else 'B') + argv[8][1:]
                if self.failure == 'symlink':
                    payload = self.root / argv[5] / 'payload.b64'
                    payload.unlink(); payload.symlink_to(self.root / 'agent.py')
        if argv[3] == 'commit' and self.failure == 'digest': argv[7] = '0' * 64
        stdout = io.StringIO()
        real_replace = os.replace
        def replace(src, dst, **kw):
            if dst == 'agent.py' and 'dst_dir_fd' in kw:
                staged = sys.modules['sc_staged_agent']
                self.assertGreater(staged._lease_local.depth, 0, 'replacement must be under actual lease lock')
                self.assertTrue(staged._lease_matches(staged._lease_record(), self.install_fence(staged)))
                before = os.stat(dst, dir_fd=kw['dst_dir_fd'], follow_symlinks=False)
                real_replace(src, dst, **kw)
                after = os.stat(dst, dir_fd=kw['dst_dir_fd'], follow_symlinks=False)
                self.assertNotEqual(before.st_ino, after.st_ino)
            else:
                real_replace(src, dst, **kw)
                if self.race_after_claim and Path(dst) == self.root / 'lease.json':
                    path = self.root / 'lease.json'
                    record = json.loads(path.read_text()); record['epoch'] += 1
                    path.write_text(json.dumps(record))
        # The installer executes in this interpreter with HOME scoped to our temp
        # root. No shell, daemon, provider, PID inspection or child is involved.
        previous = sys.modules.get('sc_staged_agent')
        try:
            with patch.object(sys, 'argv', ['-c', *argv[3:]]), patch.object(Path, 'home', return_value=Path(self.tmp.name)), patch.object(os, 'replace', side_effect=replace), contextlib.redirect_stdout(stdout):
                try: exec(compile(argv[2], '<bootstrap-test>', 'exec'), {})
                except SystemExit as exc:
                    if exc.code not in (None, 0): raise
            return {'exitCode': 0, 'stdout': stdout.getvalue(), 'stderr': ''}
        except Exception as exc:
            return {'exitCode': 1, 'stdout': stdout.getvalue(), 'stderr': repr(exc)}
        finally:
            if previous is None: sys.modules.pop('sc_staged_agent', None)
            else: sys.modules['sc_staged_agent'] = previous

    def install_fence(self, staged):
        record = staged._lease_record()
        return dict(owner=record['holder'], vm_id=record['vm_id'], boot=record['boot'], epoch=record['epoch'])

    def candidate(self, payload):
        # Transport payload remains arbitrary/growing, but the staged executable
        # deliberately contains the real lease implementation, never a fake guard.
        return self.source + b'\n' + payload

    def install(self, payload):
        data = self.candidate(payload)
        return boat.bootstrap_agent('fake-vm', base64.b64encode(data).decode(), self.token, lease=self.lease)

    def clean(self, expected):
        self.assertEqual((self.root / 'agent.py').read_bytes(), expected)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['agent.py', 'lease.json', 'lease.lock'])
        self.assertFalse((Path(self.tmp.name) / 'NEVER').exists())

    def test_exact_candidate_agent_crosses_linux_argument_boundary(self):
        self.assertGreater(len(self.source), 100_000)  # transport must cross Linux MAX_ARG_STRLEN
        self.assertGreater(len(base64.b64encode(self.source)), 131072)
        self.assertGreater(len(base64.b64encode(self.source)), boat.EXEC_ARG)
        result = boat.bootstrap_agent('fake-vm', base64.b64encode(self.source).decode(), self.token, lease=self.lease)
        self.assertTrue(result['ok'])
        argv = result['exec']
        self.assertEqual(argv[2:5], ['start-daemon', str(boat.AGENT_PORT), self.token])
        self.assertEqual(json.loads(base64.b64decode(argv[-1])), self.lease)
        self.clean(self.source)
        self.assertEqual(self.appends, (len(base64.b64encode(self.source)) + boat.EXEC_ARG - 1) // boat.EXEC_ARG)

    def test_generic_ship_exec_preserves_arbitrary_command_json_timeout_tries_sem(self):
        data = self.candidate(b'# generic\n')
        args = ['prep', 'batch ; $(literal)', 'source path', 'ctx', 'tree', 'tree', '-']
        semaphore = object()
        with patch.object(boat, 'exec_json', wraps=boat.exec_json) as dispatch:
            result = boat.ship_agent('fake-vm', base64.b64encode(data).decode(), args, 37, 2, semaphore, lease=self.lease)
            self.assertEqual(result['exec'][2:], args)
            self.assertEqual(dispatch.call_args.args[-3:], (37, 2, semaphore))
            for call in dispatch.call_args_list[:-1]: self.assertEqual(call.args[-3:], (37, 1, semaphore))
        self.clean(data)

    def test_growth_many_chunks_not_compression_or_minification(self):
        payload = b'x = 1\n' * boat.EXEC_ARG
        self.install(payload); self.assertGreater(self.appends, 3); self.clean(self.candidate(payload))

    def failed_upload(self, mode):
        self.failure = mode
        with self.assertRaises((RuntimeError, TimeoutError)): self.install(b'x=1\n' * boat.EXEC_ARG)
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)
        self.assertFalse(any(self.root.glob('bootstrap-*')))
        ops = [a[3] for a in self.calls if a[:2] == ['python3', '-c']]
        # A same-length valid-base64 corruption is detected by the commit's
        # verified digest, not by append (which validates offset and type).
        self.assertEqual(ops.count('commit'), 1 if mode in ('digest', 'corrupt') else 0)
        self.assertEqual(ops[-1], 'cleanup')

    def test_nonzero_append_preserves_old_and_cleans(self): self.failed_upload('exit')
    def test_ambiguous_append_timeout_preserves_old_and_cleans(self): self.failed_upload('timeout')
    def test_short_successful_append_detected_and_cleans(self): self.failed_upload('short')
    def test_corrupt_successful_append_detected_and_cleans(self): self.failed_upload('corrupt')
    def test_digest_mismatch_preserves_old_and_cleans(self): self.failed_upload('digest')
    def test_staged_payload_symlink_is_not_followed(self): self.failed_upload('symlink')

    def test_actual_guard_rejects_stale_epoch_wrong_boot_owner_and_fork(self):
        for change in ({'epoch': 2}, {'boot': 'previous-boot'}, {'owner': 'other'}, {'vm_id': 'fork'}):
            with self.subTest(change=change):
                with self.assertRaises(RuntimeError):
                    boat.bootstrap_agent('fake-vm', base64.b64encode(self.source).decode(), self.token, lease=dict(self.lease, **change))
                self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)
                self.assertFalse(any(self.root.glob('bootstrap-*')))
        self.assertFalse(any(a[2:] and a[2] == 'start-daemon' for a in self.calls))

    def test_actual_claim_of_live_other_holder_preserves_installed_source(self):
        with patch.object(boat, 'owner_id', return_value='other'):
            answer = boat.bootstrap_agent('fake-vm', base64.b64encode(self.source).decode(), self.token)
        self.assertFalse(answer['ok'])
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)
        self.assertFalse(any(self.root.glob('bootstrap-*')))

    def test_birth_nonce_first_install_claim_returns_real_grant(self):
        (self.root / 'lease.json').unlink()
        (self.root / 'birth.json').write_text(json.dumps({'owner': self.lease['owner'], 'nonce': 'birth'}))
        # Birth freshness is measured by VM uptime, never the test runner's wall clock.
        with patch.object(boat, 'read_agent', return_value=None), patch.object(boat.time, 'clock_gettime', return_value=10):
            answer = boat.bootstrap_agent('fake-vm', base64.b64encode(self.source).decode(), self.token, nonce='birth')
        self.assertTrue(answer['ok'])
        granted = json.loads(base64.b64decode(answer['exec'][-1]))
        record = json.loads((self.root / 'lease.json').read_text())
        for key in ('boot', 'epoch', 'vm_id'): self.assertEqual(granted[key], record[key])
        self.assertEqual(granted['owner'], record['holder'])
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.source)

    def test_invalid_base64_does_not_contact_transport(self):
        with self.assertRaises(ValueError): boat.bootstrap_agent('fake-vm', 'not-base64!', self.token, lease=self.lease)
        self.assertEqual(self.calls, [])

    def test_agent_symlink_is_not_followed_or_replaced(self):
        dest = self.root / 'agent.py'; dest.unlink()
        target = Path(self.tmp.name) / 'outside.py'; target.write_bytes(self.old); dest.symlink_to(target)
        with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.assertTrue(dest.is_symlink()); self.assertEqual(target.read_bytes(), self.old)

    def test_group_writable_root_is_refused(self):
        self.root.chmod(0o770)
        with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)

    def test_independent_owned_upload_paths(self):
        self.install(b'# first\n'); self.install(b'# second\n')
        paths = [a[5] for a in self.calls if a[:2] == ['python3', '-c'] and a[3] == 'init']
        self.assertEqual(len(paths), 2); self.assertNotEqual(*paths)
        self.clean(self.candidate(b'# second\n'))

    def test_claim_then_epoch_change_cannot_replace_source(self):
        self.race_after_claim = True
        with self.assertRaises(RuntimeError):
            boat.bootstrap_agent('fake-vm', base64.b64encode(self.source).decode(), self.token)
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)
        self.assertFalse(any(self.root.glob('bootstrap-*')))

    def test_birth_fresh_wrong_nonce_or_owner_never_replaces_source(self):
        (self.root / 'lease.json').unlink()
        (self.root / 'birth.json').write_text(json.dumps({'owner': self.lease['owner'], 'nonce': 'birth'}))
        with patch.object(boat.time, 'clock_gettime', return_value=10):
            for owner, nonce in ((self.lease['owner'], ''), (self.lease['owner'], 'wrong'), ('other', 'birth')):
                with patch.object(boat, 'owner_id', return_value=owner):
                    answer = boat.bootstrap_agent('fake-vm', base64.b64encode(self.source).decode(), self.token, nonce=nonce)
                self.assertFalse(answer['ok'])
                self.assertTrue(answer['birth'])
                self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)
                self.assertFalse(any(self.root.glob('bootstrap-*')))

    def test_init_collision_cannot_cleanup_another_upload(self):
        work = self.root / ('bootstrap-' + 'a' * 48); work.mkdir(mode=0o700)
        (work / 'owner').write_text('b' * 48)
        (work / 'payload.b64').write_text('DO NOT DELETE')
        with patch.object(boat.secrets, 'token_hex', side_effect=['a' * 48, 'c' * 48]):
            with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.assertEqual((work / 'owner').read_text(), 'b' * 48)
        self.assertEqual((work / 'payload.b64').read_text(), 'DO NOT DELETE')
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)

    def test_symlink_parent_is_refused(self):
        alias = Path(self.tmp.name) / 'alias'; alias.symlink_to(self.root, target_is_directory=True)
        with patch.object(boat, 'VM_ROOT', str(alias)):
            with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)

    def test_ship_without_lease_or_claim_rejected_before_any_staging(self):
        with self.assertRaises((ValueError, boat.LeaseLost)):
            boat.ship_agent('fake-vm', base64.b64encode(self.source).decode(), [])
        self.assertEqual(self.calls, [])
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)

    def test_arbitrary_source_without_actual_guard_is_not_installable(self):
        with self.assertRaises(RuntimeError):
            boat.bootstrap_agent('fake-vm', base64.b64encode(b'# not an agent\n').decode(), self.token, lease=self.lease)
        self.assertEqual((self.root / 'agent.py').read_bytes(), self.old)
        self.assertFalse(any(self.root.glob('bootstrap-*')))


class SchedulerBootstrapWiring(unittest.TestCase):
    def test_bootstraps_share_helper_but_ambiguous_upgrade_never_bootstraps(self):
        tree = ast.parse(Path(scheduler.__file__).read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'Scheduler')
        names = {'_reattach_vm', '_up', '_repair', '_check_agent'}
        methods = {n.name: n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in names}
        self.assertEqual(set(methods), names)
        for name, node in methods.items():
            helpers = [n for n in ast.walk(node) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                       and isinstance(n.func.value, ast.Name) and n.func.value.id == 'boat'
                       and n.func.attr == 'bootstrap_agent']
            self.assertEqual(len(helpers), 2 if name == '_up' else 0 if name == '_check_agent' else 1, name)
            self.assertFalse(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                                 and n.func.attr == 'exec_json' for n in ast.walk(node)), name)

    def test_fifth_session_ship_route_preserves_args_timeout_tries_sem(self):
        instance = object.__new__(boat.Session)
        instance.agent_b64 = 'EXACT_SOURCE'
        args = ['serve', '/owned/srv', '8765']
        semaphore = object()
        result = {'a': 'generic JSON result'}
        lease = {'owner': 'owner', 'vm_id': 'vm', 'boot': 'boot', 'epoch': 1}
        with patch.object(boat, 'ship_agent', return_value={'ok': True}) as ship, patch.object(boat, 'exec_json', return_value=result) as remote:
            self.assertIs(instance._agent(types.SimpleNamespace(id='vm', lease=lease), args, 37, True, 2, semaphore), result)
            ship.assert_called_once_with('vm', 'EXACT_SOURCE', [], 37, 2, semaphore, lease=lease)
            vm, argv, timeout, tries, sem = remote.call_args.args
            self.assertEqual((vm, timeout, tries, sem), ('vm', 37, 2, semaphore))
            self.assertEqual(argv[:3], ['python3', boat.VM_ROOT + '/agent.py', 'rpc'])
            request = json.loads(base64.b64decode(argv[-1]))
            self.assertEqual(request, {'cmd': 'serve', 'args': [*args[1:], {'_lease': lease}]})

    def test_known_empty_legacy_can_bootstrap_but_adopted_foreign_never_does(self):
        for how in ('warm', 'adopt'):
            with self.subTest(how=how):
                instance = object.__new__(scheduler.Scheduler)
                instance.held_before, instance.agent_b64 = {}, 'EXACT_SOURCE'
                instance.wake = threading.Event()
                instance._drop_stale, instance._let_go, instance.alert = Mock(), Mock(), Mock()
                vm = types.SimpleNamespace(id='legacy-vm', how=how, lease={}, dead=False)
                with patch.object(boat, 'wait_running', return_value=True), \
                     patch.object(boat, 'read_agent', return_value={}), \
                     patch.object(boat, 'owner_key', return_value='local-key'), \
                     patch.object(boat, 'bootstrap_agent', return_value={'ok': False}) as bootstrap, \
                     patch.object(boat, 'lease_vm', return_value={'ok': False}) as claim:
                    instance._up(vm, 3, None)
                    instance._drop_stale.assert_called_once_with(vm)
                    instance._let_go.assert_not_called()
                    if how == 'warm':
                        bootstrap.assert_called_once_with('legacy-vm', 'EXACT_SOURCE', 'local-key', 90, nonce='')
                        claim.assert_not_called()
                    else:
                        bootstrap.assert_not_called()
                        claim.assert_called_once_with('legacy-vm', 'claim')

    def test_strict_unresolved_repair_does_not_upload(self):
        instance = object.__new__(scheduler.Scheduler)
        instance.lock = threading.RLock()
        vm = types.SimpleNamespace(dead=False, running={'item': types.SimpleNamespace(batch=types.SimpleNamespace(strict_once=True))})
        with patch.object(boat, 'bootstrap_agent', side_effect=AssertionError('strict repair attempted')):
            self.assertFalse(instance._repair(vm))
            self.assertIsNone(instance._check_agent(vm))

    def test_repair_preserves_token_and_verifies_ping(self):
        instance = object.__new__(scheduler.Scheduler)
        instance.agent_b64 = 'EXACT_SOURCE'
        vm = types.SimpleNamespace(id='fake-vm', running={}, lease={'boot': 'b', 'epoch': 1}, client=types.SimpleNamespace(token='same-token', ping=lambda **kw: {'ok': True}), failing=3, fails=4)
        with patch.object(instance, '_renew_lease', return_value=True), patch.object(boat, 'bootstrap_agent', return_value={'ok': True}) as bootstrap:
            self.assertTrue(instance._repair(vm))
            bootstrap.assert_called_once_with('fake-vm', 'EXACT_SOURCE', 'same-token', 60, 2, lease=vm.lease)
        self.assertEqual((vm.failing, vm.fails), (0, 0))

    def test_ambiguous_upgrade_never_bootstraps_over_running_agent(self):
        instance = object.__new__(scheduler.Scheduler)
        instance.agent_b64, instance.agent_version = 'EXACT_SOURCE', 'new-hash'
        instance.lock, instance.wake = threading.RLock(), threading.Event()
        replies = iter([{'version': 'old-hash'}, {'version': 'new-hash'}])
        client = types.SimpleNamespace(token='same-token', ping=lambda: next(replies), rpc=lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('front down')))
        vm = types.SimpleNamespace(id='fake-vm', running={}, starts=[], client=client, dead=False, agent='', lease={'boot': 'b', 'epoch': 1})
        with patch.object(instance, '_renew_lease', return_value=True), patch.object(boat, 'bootstrap_agent', return_value={'ok': True}) as bootstrap, patch.object(boat, 'read_agent', return_value={}), patch.object(boat, 'write_agent') as write, patch.object(scheduler.time, 'sleep'):
            instance._check_agent(vm)
            bootstrap.assert_not_called()
            write.assert_not_called()
            self.assertEqual(vm.t_agent, 0)


if __name__ == '__main__': unittest.main()
