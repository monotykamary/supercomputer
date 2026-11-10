"""Lease client regressions: temporary files/mocks only, never a provider/process.
Run only in Main's centralized Boat test job; authoring is not test evidence.
"""
import base64
import concurrent.futures
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import ANY, MagicMock, Mock, patch

if sys.platform != 'linux' or os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') != '1':
    raise RuntimeError('Boat-only test; never import/execute on laptop')
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import boat as B
from supercomputer import scheduler as S


class LeaseClientTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='lease-client-')
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        for name, value in [('LIVE', self.root), ('AGENTS', self.root / 'agents')]:
            p = patch.object(B, name, value); p.start(); self.addCleanup(p.stop)
        # A missed mock must fail rather than reach a provider, network or child.
        for target in ('boat', 'exec_', 'bj'):
            p = patch.object(B, target, side_effect=AssertionError('unexpected provider: ' + target))
            p.start(); self.addCleanup(p.stop)
        for target in ('Popen', 'run'):
            p = patch.object(B.subprocess, target, side_effect=AssertionError('unexpected process'))
            p.start(); self.addCleanup(p.stop)
        self.owner = B.owner_id()
        self.lease = dict(owner=self.owner, vm_id='vm', boot='boot', epoch=7)

    def test_owner_stable_across_threads_and_handoff(self):
        (self.root / 'owner').unlink()
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            owners = list(pool.map(lambda _: B.owner_id(), range(64)))
        self.assertEqual(len(set(owners)), 1)
        self.assertEqual(B.owner_id(), owners[0])
        self.assertEqual((self.root / 'owner').read_text().strip(), owners[0])
        self.assertEqual((self.root / 'owner').stat().st_mode & 0o077, 0)

    def test_owner_key_stable_concurrently_and_after_vm_cache_loss(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
            keys = list(pool.map(lambda _: B.owner_key(), range(64)))
        self.assertEqual(len(set(keys)), 1)
        self.assertTrue(keys[0])
        B.write_agent('vm', {'token': keys[0]})
        B.forget_agent('vm')
        self.assertEqual(B.owner_key(), keys[0])

    def test_birth_file_is_live_until_creation_returns_then_removed(self):
        paths, nonces = [], []
        for _ in range(2):
            with B.new_vm_args('image') as (args, nonce):
                self.assertEqual(args[:3], ['--name', 'image~l', '--file'])
                remote, local = args[3].split('=', 1)
                self.assertEqual(remote, B.VM_ROOT + '/birth.json')
                path = Path(local); paths.append(path); nonces.append(nonce)
                self.assertEqual(json.loads(path.read_text()), {'owner': self.owner, 'nonce': nonce})
            self.assertFalse(path.exists())
        self.assertNotEqual(nonces[0], nonces[1])
        self.assertNotEqual(paths[0], paths[1])

    def test_birth_file_removed_on_creation_failure(self):
        with self.assertRaisesRegex(RuntimeError, 'creation failed'):
            with B.new_vm_args('image') as (args, _):
                path = Path(args[3].split('=', 1)[1])
                raise RuntimeError('creation failed')
        self.assertFalse(path.exists())

    def test_exec_lease_binds_actual_owner_vm_and_nonce_without_mutating_input(self):
        supplied = dict(self.lease, owner='foreign', vm_id='fork')
        before = dict(supplied)
        with patch.object(B, 'exec_json', return_value={'ok': True}) as remote:
            B.lease_vm('vm', 'renew', supplied, 'birth-nonce')
        self.assertEqual(supplied, before)
        vm, argv, timeout, tries, sem = remote.call_args.args
        self.assertEqual((vm, timeout, tries, sem), ('vm', 30, 1, None))
        self.assertEqual(argv[:4], ['python3', B.VM_ROOT + '/agent.py', 'lease', 'renew'])
        self.assertEqual(json.loads(base64.b64decode(argv[4])), dict(self.lease, nonce='birth-nonce'))

    def test_explicit_old_cli_lease_command_loss_is_protocol_lost(self):
        for message in ("vm: exit 1: KeyError: 'lease'", 'vm: no command lease', 'vm: unsupported command lease'):
            original = RuntimeError(message)
            with patch.object(B, 'exec_json', side_effect=original):
                with self.assertRaises(B.LeaseProtocolLost) as raised:
                    B.lease_vm('vm', 'renew', self.lease)
            self.assertIs(raised.exception.__cause__, original)
            self.assertIn('old client wins', str(raised.exception))

    def test_lease_transport_timeout_is_not_misclassified_as_old_client(self):
        for message in ('vm: lease exec timed out', 'vm: exit 1: connection unavailable', 'vm: KeyError: unrelated'):
            original = RuntimeError(message)
            with patch.object(B, 'exec_json', side_effect=original):
                with self.assertRaises(RuntimeError) as raised:
                    B.lease_vm('vm', 'renew', self.lease)
            self.assertIs(raised.exception, original)
            self.assertNotIsInstance(raised.exception, B.LeaseProtocolLost)

    def test_stop_delete_require_remote_current_fence_acceptance(self):
        for operation in (B.stop_vm, B.delete_vm):
            for response in ({'ok': False}, {'ok': 1}, {'ok': 'false'}, {'ok': True, 'stale': True}, {'stale': True}, {}):
                with self.subTest(operation=operation.__name__, response=response):
                    with patch.object(B, 'lease_vm', return_value=response) as fence, patch.object(B, 'boat') as provider:
                        self.assertFalse(operation('vm', self.lease))
                        fence.assert_called_once_with('vm', 'fence_stop', self.lease, sem=ANY)
                        provider.assert_not_called()
            with patch.object(B, 'lease_vm', side_effect=TimeoutError('ambiguous')), patch.object(B, 'boat') as provider:
                self.assertFalse(operation('vm', self.lease)); provider.assert_not_called()

    def test_expired_fence_receipt_never_reaches_provider(self):
        for name in ('stop', 'delete', 'shrink'):
            now = [0.0]
            def fence(*args, **kwargs):
                now[0] = 300.0
                return {'ok': True}
            with self.subTest(operation=name), patch.object(B, '_stop_clock', side_effect=lambda: now[0]), \
                 patch.object(B.time, 'monotonic', side_effect=lambda: now[0]), \
                 patch.object(B, 'lease_vm', side_effect=fence), patch.object(B, 'boat') as provider:
                if name == 'shrink':
                    self.assertFalse(B.shrink_ttl('vm', 60, self.lease))
                else:
                    self.assertFalse((B.stop_vm if name == 'stop' else B.delete_vm)('vm', self.lease))
                provider.assert_not_called()

    def test_delete_fallback_rechecks_deadline_after_slow_stop(self):
        now = [0.0]
        def fence(*args, **kwargs):
            now[0] = 260.0
            return {'ok': True}
        def provider(*args, **kwargs):
            self.assertEqual(args[0], 'stop')
            self.assertEqual(kwargs['timeout'], 10.0)
            now[0] = 271.0
            return 'Final snapshot failed'
        with patch.object(B, '_stop_clock', side_effect=lambda: now[0]), \
             patch.object(B.time, 'monotonic', side_effect=lambda: now[0]), \
             patch.object(B, 'lease_vm', side_effect=fence), patch.object(B, 'boat', side_effect=provider) as call, \
             patch.object(B, 'Registry') as registry:
            self.assertFalse(B.stop_vm('vm', self.lease))
            call.assert_called_once()
            registry.assert_not_called()

    def test_stop_budget_is_bounded_even_if_wall_clock_goes_back(self):
        with patch.object(B, '_stop_clock', return_value=10), patch.object(B.time, 'monotonic', return_value=400):
            self.assertEqual(B._stop_budget((100, 100)), 0)
        with patch.object(B, '_stop_clock', return_value=100), patch.object(B.time, 'monotonic', return_value=100):
            self.assertEqual(B._stop_budget((100, 100)), 90)

    def test_missing_or_foreign_lease_never_contacts_provider(self):
        for operation in (B.stop_vm, B.delete_vm):
            for lease in (None, {}, dict(self.lease, owner='someone-else'),
                          dict(self.lease, vm_id='fork'), dict(self.lease, boot=''),
                          dict(self.lease, epoch=True), dict(self.lease, epoch=0),
                          dict(self.lease, epoch='7')):
                with patch.object(B, 'read_agent', return_value=None), patch.object(B, 'lease_vm') as fence, patch.object(B, 'boat') as provider:
                    self.assertFalse(operation('vm', lease))
                    fence.assert_not_called(); provider.assert_not_called()

    def test_cached_lease_still_requires_fresh_stop_fence(self):
        events = []
        def fence(vm, op, lease, **kwargs):
            events.append(op); self.assertEqual(lease, self.lease); return {'ok': True}
        def provider(*args, **kwargs):
            events.append(args[0]); return '{}'
        with patch.object(B, 'read_agent', return_value={'lease': self.lease}), patch.object(B, 'lease_vm', side_effect=fence), patch.object(B, 'boat', side_effect=provider):
            self.assertTrue(B.stop_vm('vm'))
            self.assertTrue(B.delete_vm('vm'))
        self.assertEqual(events, ['fence_stop', 'stop', 'fence_stop', 'delete'])

    def test_stop_snapshot_failure_delete_fallback_is_also_fenced(self):
        events = []
        registry = {'vms': {'vm': {}, 'other': {}}}
        def fence(*args, **kwargs): events.append('fence_stop'); return {'ok': True}
        def provider(command, *args, **kwargs):
            events.append(command)
            return 'Final snapshot failed' if command == 'stop' else '{}'
        fake_registry = MagicMock()
        fake_registry.locked.return_value.__enter__ = Mock(return_value=registry)
        fake_registry.locked.return_value.__exit__ = Mock(return_value=False)
        with patch.object(B, 'lease_vm', side_effect=fence), patch.object(B, 'boat', side_effect=provider), patch.object(B, 'Registry', return_value=fake_registry):
            self.assertFalse(B.stop_vm('vm', self.lease))
        self.assertEqual(events, ['fence_stop', 'stop', 'delete'])
        self.assertEqual(registry['vms'], {'other': {}})

    def test_refused_fence_does_not_mutate_registry_or_forget_agent(self):
        with patch.object(B, 'lease_vm', return_value={'ok': False}), patch.object(B, 'Registry') as registry, patch.object(B, 'forget_agent') as forget:
            self.assertFalse(B.stop_vm('vm', self.lease))
            self.assertFalse(B.delete_vm('vm', self.lease))
            registry.assert_not_called(); forget.assert_not_called()

    def test_provider_capacity_is_reserved_before_fence_and_shared_until_done(self):
        for name, fallback in (('stop', False), ('stop', True), ('delete', False), ('shrink', False)):
            with self.subTest(operation=name, fallback=fallback):
                events, slots = [], []
                class Capacity:
                    held = False
                    def __enter__(capacity):
                        self.assertFalse(capacity.held, 'global provider semaphore must not be reacquired after fence')
                        capacity.held = True; events.append('acquire')
                        return capacity
                    def __exit__(capacity, *exc):
                        capacity.held = False; events.append('release')
                capacity = Capacity()
                def remote(vm, argv, timeout, tries, sem):
                    self.assertTrue(capacity.held, 'fence must execute only after provider capacity is acquired')
                    self.assertIsNotNone(sem)
                    self.assertIsNot(sem, capacity)
                    with sem:
                        self.assertTrue(capacity.held)
                        if slots: self.assertIs(sem, slots[0])
                        slots.append(sem); events.append(argv[3])
                    if argv[3] == 'release':
                        payload = json.loads(base64.b64decode(argv[4]))
                        self.assertEqual(payload['stop_nonce'], 'accepted-stop-receipt')
                    return {'ok': True, 'stop_nonce': 'accepted-stop-receipt'}
                def provider(command, *args, **kwargs):
                    self.assertTrue(capacity.held)
                    self.assertIs(kwargs['sem'], slots[0], 'provider must reuse reserved slot, not queue again')
                    with kwargs['sem']:
                        events.append(command)
                    return 'Final snapshot failed' if fallback and command == 'stop' else '{}'
                registry = MagicMock()
                registry.locked.return_value.__enter__.return_value = {'vms': {'vm': {}}}
                with patch.object(B, '_sem', capacity), patch.object(B, 'exec_json', side_effect=remote), patch.object(B, 'boat', side_effect=provider), patch.object(B, 'Registry', return_value=registry):
                    if name == 'shrink':
                        self.assertTrue(B.shrink_ttl('vm', 60, self.lease))
                    else:
                        result = (B.stop_vm if name == 'stop' else B.delete_vm)('vm', self.lease)
                        self.assertIs(result, not fallback)
                expected = ['acquire', 'fence_stop', 'extend', 'release', 'release'] if name == 'shrink' else ['acquire', 'fence_stop', name, *(['delete'] if fallback else []), 'release']
                self.assertEqual(events, expected)
                self.assertFalse(capacity.held)

    def test_denied_fence_releases_reserved_provider_capacity(self):
        for operation in (B.stop_vm, B.delete_vm):
            capacity = MagicMock()
            with patch.object(B, '_sem', capacity), patch.object(B, 'lease_vm', return_value={'ok': False}), patch.object(B, 'boat') as provider:
                self.assertFalse(operation('vm', self.lease))
            capacity.__enter__.assert_called_once()
            capacity.__exit__.assert_called_once()
            provider.assert_not_called()

    def test_shrink_release_uses_only_fence_receipt_stop_nonce(self):
        caller = dict(self.lease, stop_nonce='old-stop-nonce')
        receipt = dict(self.lease, ok=True, stop_nonce='new-stop-nonce')
        before = dict(caller)
        def remote(vm, op, lease, **kwargs):
            if op == 'fence_stop':
                self.assertEqual(lease, caller)
                return receipt
            self.assertEqual(op, 'release')
            self.assertEqual(lease, dict(caller, stop_nonce='new-stop-nonce'))
            return {'ok': True}
        with patch.object(B, 'lease_vm', side_effect=remote) as lease_rpc, patch.object(B, 'boat', return_value='{}') as provider:
            self.assertTrue(B.shrink_ttl('vm', 60, caller))
        self.assertEqual(caller, before)
        self.assertEqual(lease_rpc.call_count, 2)
        provider.assert_called_once()
        slot = provider.call_args.kwargs['sem']
        self.assertIs(lease_rpc.call_args_list[0].kwargs['sem'], slot)
        self.assertIs(lease_rpc.call_args_list[1].kwargs['sem'], slot)

    def test_legacy_worker_name_changes_only_after_current_fence_check(self):
        with patch.object(B, "lease_vm", return_value={**self.lease, "ok": True}) as check, patch.object(B, "boat", return_value="{}") as provider:
            self.assertTrue(B.rename_leased_vm("vm", "image", self.lease))
            check.assert_called_once_with("vm", "check", self.lease)
            provider.assert_called_once_with("rename", "vm", "image~l", "--json", timeout=60)
        with patch.object(B, "lease_vm") as check:
            self.assertTrue(B.rename_leased_vm("vm", "image", self.lease))
            check.assert_not_called()

    def test_stale_worker_is_not_renamed(self):
        with patch.object(B, "lease_vm", return_value={"ok": False, "stale": True}), patch.object(B, "boat") as provider:
            self.assertFalse(B.rename_leased_vm("vm", "image", self.lease))
            provider.assert_not_called()

    def client(self):
        return B.AgentClient('vm', 'https://invalid.example/agent?token=provider-secret', 'my-own-token', lease=self.lease)

    def test_rpc_and_raw_append_fence_last_and_bind_identity(self):
        client = self.client()
        client.lease = dict(self.lease, owner='foreign', vm_id='fork')
        request = {'start': [], 'acks': []}
        for method, command in ((client.rpc, 'vtick'), (client.raw, 'itemlogs')):
            with patch.object(client, '_req', return_value=(200, b'{}', 'application/json')) as req:
                method(command, request)
            body = json.loads(req.call_args.args[2])
            self.assertEqual(body, {'cmd': command, 'args': [request, {'_lease': self.lease}]})
        self.assertEqual(request, {'start': [], 'acks': []})

    def test_lease_rpc_does_not_append_a_second_fence(self):
        client = self.client()
        with patch.object(client, '_req', return_value=(200, b'{}', 'application/json')) as req:
            client.rpc('lease', 'renew', self.lease)
        self.assertEqual(json.loads(req.call_args.args[2])['args'], ['renew', self.lease])

    def test_front_outage_exec_replays_same_fence_with_clients_own_key(self):
        client = self.client()
        def remote(vm, argv, timeout):
            self.assertEqual(vm, 'vm')
            self.assertEqual(argv[-1], 'my-own-token')
            self.assertNotIn('daemon.json', argv[2])
            self.assertNotIn('provider-secret', argv[2])
            body = json.loads(base64.b64decode(argv[-2]))
            self.assertEqual(body['args'][-1], {'_lease': self.lease})
            self.assertEqual(body['cmd'], 'vtick')
            return {'exitCode': 0, 'stdout': '{"ok":true}'}
        with patch.object(client, '_req', side_effect=B.AgentError('front down')), patch.object(B, 'exec_', side_effect=remote) as transport:
            self.assertEqual(client.rpc('vtick', {'start': []}), {'ok': True})
            transport.assert_called_once()

    def test_unknown_command_cannot_fallback_to_exec(self):
        client = self.client()
        with patch.object(client, '_req', side_effect=B.AgentError('front down')), patch.object(client, '_rpc_exec') as remote:
            with self.assertRaises(B.AgentError): client.rpc('not-replayable')
            remote.assert_not_called()

    def test_bootstrap_denial_cannot_add_key_or_start_daemon(self):
        for answer in ({'stale': True}, {'ok': False}):
            with patch.object(B, 'ship_agent', return_value=answer) as ship, patch.object(B, 'exec_json') as remote:
                self.assertEqual(B.bootstrap_agent('vm', 'source', 'my-token', nonce='birth'), answer)
                remote.assert_not_called()
                self.assertEqual(ship.call_args.kwargs['claim'], {'owner': self.owner, 'vm_id': 'vm', 'nonce': 'birth'})

    def test_heartbeat_does_not_revoke_unclaimed_booting_vm(self):
        for claimed in (False, True):
            with self.subTest(claimed=claimed):
                s = S.Scheduler.__new__(S.Scheduler)
                vm = S.VMState('booting', 'image', 'large', 'created')
                vm.lease = dict(self.lease) if claimed else {}
                self.assertFalse(vm.ready)
                s.vms = {vm.id: vm}
                s.stop = Mock()
                s.stop.wait.side_effect = [False, True]
                with patch.object(s, '_renew_lease', return_value=True) as renew, patch.object(B, 'boat', return_value='{}') as provider:
                    s._heartbeat()
                self.assertFalse(vm.dead)
                self.assertIs(s.vms[vm.id], vm)
                if claimed:
                    renew.assert_called_once_with(vm)
                    provider.assert_called_once()
                else:
                    renew.assert_not_called()
                    provider.assert_not_called()

    def test_startup_budget_allows_fenced_takeover_past_ten_seconds(self):
        clock = [0.0]
        (self.root / 'scheduler').mkdir()
        (self.root / 'scheduler' / 'token').write_text('test-token')
        def sleep(seconds):
            clock[0] += seconds
        with patch.object(S, 'DIR', self.root / 'scheduler'), \
             patch.object(S, 'running', side_effect=lambda: clock[0] >= 15.0), \
             patch.object(S.time, 'monotonic', side_effect=lambda: clock[0]), \
             patch.object(S.time, 'sleep', side_effect=sleep), \
             patch.object(S.subprocess, 'Popen') as spawn:
            self.assertIsInstance(S.ensure_running(), S.Client)
        spawn.assert_called_once()
        self.assertGreaterEqual(clock[0], 15.0)
        self.assertLess(clock[0], S.STARTUP_WAIT_S)

    def test_bootstrap_uses_first_install_grant_without_cached_agent(self):
        grant = dict(self.lease, ok=True)
        with patch.object(B, 'ship_agent', return_value=grant), patch.object(B, 'read_agent', return_value=None), patch.object(B, 'remember_lease'), patch.object(B, 'exec_json', return_value={'ok': True}) as remote:
            B.bootstrap_agent('vm', 'source', 'my-token', nonce='birth')
        calls = remote.call_args_list
        self.assertEqual(calls[0].args[1][2:], ['keys', 'add', self.owner, 'my-token'])
        argv = calls[-1].args[1]
        self.assertEqual(argv[2:5], ['start-daemon', str(B.AGENT_PORT), 'my-token'])
        fence = json.loads(base64.b64decode(argv[-1]))
        for field in ('owner', 'vm_id', 'boot', 'epoch'):
            self.assertEqual(fence[field], self.lease[field])


if __name__ == '__main__':
    unittest.main()
