"""Boat-only transport regression. Real remote installer/filesystem, never a live agent.
Execution requires Main's later Boat grant; authoring is not test evidence.
"""
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

if sys.platform != 'linux' or os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') != '1':
    raise RuntimeError('Boat-only test; never import/execute on laptop')
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import boat, scheduler

# Exercise the real installer with subprocesses; intercept the separate final
# agent command in the fake transport: no daemon/provider/scheduler is started.
INTERCEPT_EXEC = '''import os,sys,json
_real_replace = os.replace
def checked_replace(src, dst, **kw):
    # The old destination must still be present until this one atomic syscall.
    before = os.stat(dst, dir_fd=kw['dst_dir_fd'], follow_symlinks=False)
    _real_replace(src, dst, **kw)
    after = os.stat(dst, dir_fd=kw['dst_dir_fd'], follow_symlinks=False)
    assert before.st_ino != after.st_ino
os.replace = checked_replace
'''


class BootstrapTransport(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='bootstrap-test-')
        self.root = Path(self.tmp.name) / '.sc'
        self.root.mkdir(mode=0o700)
        self.old = b'# old agent must survive a failed upload\n'
        (self.root / 'agent.py').write_bytes(self.old)
        self.calls = []
        self.appends = 0
        self.failure = None
        self.token = 'literal token ; $(touch NEVER) "quoted"'
        self.stack = [patch.object(boat, 'VM_ROOT', str(self.root)),
                      patch.object(boat, 'exec_', self.execute)]
        for p in self.stack: p.start()

    def tearDown(self):
        for p in reversed(self.stack): p.stop()
        self.tmp.cleanup()

    def execute(self, vm, argv, timeout=60, sem=None):
        self.assertEqual(vm, 'fake-vm')
        self.calls.append(list(argv))
        self.assertLessEqual(max(len(a.encode()) for a in argv), boat.EXEC_ARG)
        argv = list(argv)
        if argv[:2] == ['python3', '-c']:
            self.assertEqual(argv[2], boat._AGENT_BOOTSTRAP)
            if argv[3] == 'append':
                self.appends += 1
                if self.appends == 2:
                    if self.failure == 'timeout': raise TimeoutError('injected append timeout')
                    if self.failure == 'exit':
                        return {'exitCode': 1, 'stdout': '', 'stderr': 'injected append failure'}
                    if self.failure == 'short': argv[8] = argv[8][:-4]
                    if self.failure == 'corrupt': argv[8] = ('A' if argv[8][0] != 'A' else 'B') + argv[8][1:]
                    if self.failure == 'symlink':
                        payload = self.root / argv[5] / 'payload.b64'
                        payload.unlink()
                        payload.symlink_to(self.root / 'agent.py')
            if argv[3] == 'commit' and self.failure == 'digest': argv[7] = '0' * 64
            argv[0] = sys.executable
            argv[2] = INTERCEPT_EXEC + argv[2]
        elif argv[:2] == ['python3', str(self.root / 'agent.py')]:
            # Transport receipt, not execution: NEVER launch the actual daemon.
            return {'exitCode': 0, 'stdout': json.dumps({'ok': True, 'exec': argv}) + '\n', 'stderr': ''}
        else:
            self.fail('unexpected transport command')
        p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout,
                           env={'PATH': os.environ['PATH'], 'HOME': self.tmp.name,
                                'PYTHONDONTWRITEBYTECODE': '1'})
        return {'exitCode': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr}

    def install(self, data):
        return boat.bootstrap_agent('fake-vm', base64.b64encode(data).decode(), self.token)

    def clean(self, expected):
        self.assertEqual((self.root / 'agent.py').read_bytes(), expected)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['agent.py'])
        self.assertFalse((Path(self.tmp.name) / 'NEVER').exists())

    def test_exact_candidate_agent_crosses_linux_argument_boundary(self):
        data = (Path(boat.__file__).parent / 'agent.py').read_bytes()
        self.assertGreater(len(data), 100_000)  # source evolves; transport must still cross MAX_ARG_STRLEN
        self.assertGreater(len(base64.b64encode(data)), 131072)
        result = self.install(data)
        self.assertTrue(result['ok'])
        self.assertEqual(result['exec'][-3:], ['start-daemon', str(boat.AGENT_PORT), self.token])
        self.clean(data)
        self.assertEqual(self.appends, 3)

    def test_generic_ship_exec_preserves_arbitrary_command_json_timeout_tries_sem(self):
        data = b'# generic full source\n' + b'x=1\n' * boat.EXEC_ARG
        args = ['prep', 'batch ; $(literal)', 'source path', 'ctx', 'tree', 'tree', '-']
        semaphore = object()
        original = boat.exec_json
        with patch.object(boat, 'exec_json', wraps=original) as dispatch:
            result = boat.ship_agent('fake-vm', base64.b64encode(data).decode(), args, 37, 2, semaphore)
            self.assertEqual(result['exec'][2:], args)
            self.assertEqual(dispatch.call_args.args[-3:], (37, 2, semaphore))
            for call in dispatch.call_args_list[:-1]:
                self.assertEqual(call.args[-3:], (37, 1, semaphore))
        self.clean(data)
        self.assertNotIn('start-daemon', self.calls[-1])

    def test_growth_many_chunks_not_compression_or_minification(self):
        data = b'# future source growth\n' + b'x = 1\n' * (boat.EXEC_ARG // 2)
        self.install(data)
        self.assertGreater(self.appends, 3)
        self.clean(data)

    def test_small_source_uses_same_verified_atomic_path(self):
        data = b'# small but exact\n'
        self.install(data)
        self.clean(data)
        self.assertEqual(self.appends, 1)

    def failed_upload(self, mode):
        self.failure = mode
        with self.assertRaises((RuntimeError, TimeoutError)):
            self.install(b'# source\n' + b'x=1\n' * boat.EXEC_ARG)
        self.clean(self.old)
        # Failed append has no commit or retry; failed digest has one commit.
        ops = [a[3] for a in self.calls if a[:2] == ['python3', '-c']]
        self.assertEqual(ops.count('commit'), 0 if mode in ('exit', 'timeout', 'short', 'symlink') else 1)
        self.assertEqual(ops[-1], 'cleanup')

    def test_nonzero_append_preserves_old_and_cleans(self): self.failed_upload('exit')
    def test_ambiguous_append_timeout_preserves_old_and_cleans(self): self.failed_upload('timeout')
    def test_short_successful_append_detected_and_cleans(self): self.failed_upload('short')
    def test_corrupt_successful_append_detected_and_cleans(self): self.failed_upload('corrupt')
    def test_digest_mismatch_preserves_old_and_cleans(self): self.failed_upload('digest')

    def test_staged_payload_symlink_is_not_followed(self): self.failed_upload('symlink')

    def test_init_collision_cannot_cleanup_another_upload(self):
        work = self.root / ('bootstrap-' + 'a' * 48)
        work.mkdir(mode=0o700)
        (work / 'owner').write_text('b' * 48)
        (work / 'payload.b64').write_text('DO NOT DELETE')
        with patch.object(boat.secrets, 'token_hex', side_effect=['a' * 48, 'c' * 48]):
            with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.assertEqual((work / 'owner').read_text(), 'b' * 48)
        self.assertEqual((work / 'payload.b64').read_text(), 'DO NOT DELETE')
        (work / 'owner').unlink(); (work / 'payload.b64').unlink(); work.rmdir()
        self.clean(self.old)

    def test_invalid_base64_does_not_contact_transport(self):
        with self.assertRaises(ValueError): boat.bootstrap_agent('fake-vm', 'not-base64!', self.token)
        self.assertEqual(self.calls, [])
        self.clean(self.old)

    def test_agent_symlink_is_not_followed_or_replaced(self):
        dest = self.root / 'agent.py'
        dest.unlink()
        target = Path(self.tmp.name) / 'outside.py'
        target.write_bytes(self.old)
        dest.symlink_to(target)
        with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.assertTrue(dest.is_symlink())
        self.assertEqual(target.read_bytes(), self.old)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ['agent.py'])

    def test_symlink_parent_is_refused(self):
        alias = Path(self.tmp.name) / 'alias'
        alias.symlink_to(self.root, target_is_directory=True)
        with patch.object(boat, 'VM_ROOT', str(alias)):
            with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.clean(self.old)

    def test_group_writable_root_is_refused(self):
        self.root.chmod(0o770)
        with self.assertRaises(RuntimeError): self.install(b'# new\n')
        self.clean(self.old)

    def test_independent_owned_upload_paths(self):
        self.install(b'# first\n'); self.install(b'# second\n')
        paths = [a[5] for a in self.calls if a[:2] == ['python3', '-c'] and a[3] == 'init']
        self.assertEqual(len(paths), 2)
        self.assertNotEqual(paths[0], paths[1])
        self.clean(b'# second\n')


class SchedulerBootstrapWiring(unittest.TestCase):
    def test_all_four_oversized_callers_route_through_shared_helper(self):
        tree = ast.parse(Path(scheduler.__file__).read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'Scheduler')
        names = {'_reattach_vm', '_up', '_repair', '_check_agent'}
        methods = {n.name: n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name in names}
        self.assertEqual(set(methods), names)
        for name, node in methods.items():
            helpers = [n for n in ast.walk(node) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                       and isinstance(n.func.value, ast.Name) and n.func.value.id == 'boat'
                       and n.func.attr == 'bootstrap_agent']
            self.assertEqual(len(helpers), 1, name)
            self.assertFalse(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                                 and n.func.attr == 'exec_json' for n in ast.walk(node)), name)

    def test_fifth_session_ship_route_preserves_args_timeout_tries_sem(self):
        instance = object.__new__(boat.Session)
        instance.agent_b64 = 'EXACT_SOURCE'
        args = ['serve', '/owned/srv', '8765']
        semaphore = object()
        result = {'a': 'generic JSON result'}
        with patch.object(boat, 'ship_agent', return_value=result) as ship:
            self.assertIs(instance._agent(types.SimpleNamespace(id='vm'), args, 37, True, 2, semaphore), result)
            ship.assert_called_once_with('vm', 'EXACT_SOURCE', args, 37, 2, semaphore)

    def test_strict_unresolved_repair_does_not_upload(self):
        instance = object.__new__(scheduler.Scheduler)
        vm = types.SimpleNamespace(running={'item': types.SimpleNamespace(batch=types.SimpleNamespace(strict_once=True))})
        with patch.object(boat, 'bootstrap_agent', side_effect=AssertionError('strict repair attempted')):
            self.assertFalse(instance._repair(vm))
            self.assertIsNone(instance._check_agent(vm))

    def test_repair_preserves_token_and_verifies_ping(self):
        instance = object.__new__(scheduler.Scheduler)
        instance.agent_b64 = 'EXACT_SOURCE'
        vm = types.SimpleNamespace(id='fake-vm', running={}, client=types.SimpleNamespace(token='same-token', ping=lambda **kw: {'ok': True}), failing=3, fails=4)
        with patch.object(boat, 'bootstrap_agent', return_value={'ok': True}) as bootstrap:
            self.assertTrue(instance._repair(vm))
            bootstrap.assert_called_once_with('fake-vm', 'EXACT_SOURCE', 'same-token', 60, 2)
        self.assertEqual((vm.failing, vm.fails), (0, 0))

    def test_ambiguous_upgrade_never_bootstraps_over_running_agent(self):
        instance = object.__new__(scheduler.Scheduler)
        instance.agent_b64, instance.agent_version = 'EXACT_SOURCE', 'new-hash'
        replies = iter([{'version': 'old-hash'}, {'version': 'new-hash'}])
        client = types.SimpleNamespace(token='same-token', ping=lambda: next(replies), rpc=lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('front down')))
        vm = types.SimpleNamespace(id='fake-vm', running={}, client=client)
        with patch.object(boat, 'bootstrap_agent', return_value={'ok': True}) as bootstrap, patch.object(boat, 'read_agent', return_value={}), patch.object(boat, 'write_agent') as write, patch.object(scheduler.time, 'sleep'):
            instance._check_agent(vm)
            bootstrap.assert_not_called()
            write.assert_not_called()
            self.assertEqual(vm.t_agent, 0)


if __name__ == '__main__': unittest.main()
