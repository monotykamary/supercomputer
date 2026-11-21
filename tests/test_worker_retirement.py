"""Provider-driven retirement: only read-only provider calls in these fixtures."""
import pathlib
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))  # (the checkout's package, run as tests/x.py)
from supercomputer import boat, scheduler as S
import test_owned_scheduler as fixture


class WorkerRetirement(unittest.TestCase):
    setUp = fixture.OwnedSchedulerTests.setUp
    write_batch = fixture.OwnedSchedulerTests.write_batch
    place_without_thread = fixture.OwnedSchedulerTests.place_without_thread

    def guard_mutations(self):
        for name in ('stop_vm', 'delete_vm', 'shrink_ttl', 'boat'):
            p = patch.object(boat, name, side_effect=AssertionError(f'provider mutation: {name}'))
            self.addCleanup(p.stop)
            mock = p.start()
            self.addCleanup(mock.assert_not_called)

    def test_renewal_failure_retires_error_even_if_listing_lags(self):
        self.place_without_thread()
        self.guard_mutations()
        self.vm.renewal_pending = True
        self.vm.ready = False
        self.assertFalse(self.s._pool_settled())
        self.assertEqual(self.vm.used(), 1)
        with patch.object(self.s, '_renew_lease', side_effect=TimeoutError), \
             patch.object(boat, 'states', return_value={self.vm.id: {'state': 'idle'}}), \
             patch.object(boat, 'bj', return_value={'sandbox': {'id': self.vm.id, 'state': 'error'}}) as detail:
            self.assertIsNone(self.s._tick(self.vm))
        detail.assert_called_once_with('info', self.vm.id, timeout=10)
        self.assertTrue(self.vm.dead)
        self.assertNotIn(self.vm.id, self.s.vms)
        self.assertEqual(self.vm.used(), 0)
        self.assertTrue(self.s._pool_settled())
        row, = self.b.rows
        self.assertEqual((row['status'], row['vm'], row['boot_id'], row['attempts']),
                         ('LOST', self.vm.id, 'fixture-boot', 1))
        self.assertFalse(row['cleanup_complete'])
        self.assertNotIn('worker_loss', row)  # provider state is not same-boot namespace evidence
        evidence = self.s._load_worker_retirements()
        self.assertEqual(row['worker_retirement'], evidence[self.vm.id])
        self.assertEqual(evidence[self.vm.id]['provider_state'], 'error')
        self.assertTrue(self.it.owned.closed)
        # A fresh scheduler reads the tombstone and refuses even a now-running ID.
        other = S.Scheduler.__new__(S.Scheduler)
        other.vms, other.lock, other.reattach_dead = {}, self.s.lock, set()
        other.worker_retirements = evidence
        with patch.object(boat, 'agent_client') as client, patch.object(S.threading, 'Thread') as thread:
            self.assertEqual(other._reattach_vm(self.vm.id, []), 0)
            other._bring(self.vm.id, self.vm.image, self.vm.type, 'warm', 2, self.b)
        client.assert_not_called()
        thread.assert_not_called()
        self.assertIn(self.vm.id, other.retired())

    def test_strict_loss_journal_failure_keeps_hold_and_retries_without_launching(self):
        self.place_without_thread()
        self.guard_mutations()
        with patch.object(self.s, '_deliver_owned', side_effect=OSError('write failed')):
            with self.assertRaises(OSError):
                self.s._retire_worker(self.vm, 'error', 'test')
        self.assertFalse(self.vm.ready)
        self.assertFalse(self.vm.dead)
        self.assertEqual(self.vm.used(), 1)
        self.assertFalse(self.s._pool_settled())
        self.assertFalse(self.s._attempt_audit(self.vm, [{'batch': self.b.id}], 'start_requested'))
        self.assertIsNone(self.s._tick(self.vm))
        self.vm.client.rpc.assert_not_called()
        self.assertEqual([(row['status'], row['cleanup_complete']) for row in self.b.rows], [('LOST', False)])
        self.assertTrue(self.s._pool_settled())

    def test_retirement_waits_for_fetched_result_to_commit(self):
        self.guard_mutations()
        self.vm.finishing['already-fetched'] = object()
        self.assertFalse(self.s._retire_worker(self.vm, 'error', 'test'))
        self.assertIs(self.s.vms[self.vm.id], self.vm)
        self.vm.finishing.clear()
        self.assertTrue(self.s._retire_worker(self.vm, 'error', 'test'))

    def test_handoff_drops_only_exact_terminal_hold(self):
        self.place_without_thread()
        self.guard_mutations()
        self.vm.ready = False
        boot = S.VMState('live-booting', self.vm.image, self.vm.type, 'resumed')
        self.s.vms[boot.id] = boot
        with self.s.lock, patch.object(S, 'Registry') as registry:
            self.s._prune_handoff_pool({self.vm.id: {'state': 'error'}, boot.id: {'state': 'stopped'},
                                        'another-clients-vm': {'state': 'error'}})
        registry.assert_not_called()
        self.assertTrue(self.vm.dead)
        self.assertFalse(boot.dead)
        self.assertIs(self.s.vms[boot.id], boot)
        self.assertFalse(self.s._pool_settled())
        self.assertEqual(set(self.s._load_worker_retirements()), {self.vm.id})
        self.assertFalse(self.b.rows[0]['cleanup_complete'])

    def test_expired_cold_boot_is_retired_but_elapsed_time_cannot_fence_held_work(self):
        self.guard_mutations()
        self.place_without_thread()
        self.vm.ready = False
        self.assertFalse(self.s._retire_worker(self.vm, None, 'test', expired=True))
        self.assertNotIn(self.vm.id, self.s._load_worker_retirements())
        boot = S.VMState('never-ready', self.vm.image, self.vm.type, 'warm')
        self.s.vms[boot.id] = boot
        with patch.object(boat, 'wait_running', return_value={}) as wait, \
             patch.object(self.s, '_provider_terminal_state', return_value=None):
            self.s._up(boot, 2, self.b)
        wait.assert_called_once_with([boot.id], int(S.BREAK_READY), abort_terminal=True)
        self.assertTrue(boot.dead)
        self.assertNotIn(boot.id, self.s.vms)
        evidence = self.s._load_worker_retirements()[boot.id]
        self.assertEqual(evidence['reason'], 'readiness_timeout')
        self.assertEqual(evidence['readiness_timeout_s'], S.BREAK_READY)

    def test_eight_filler_slots_requeue_once_on_terminal_provider_state(self):
        self.guard_mutations()
        self.s.m['boat_fails'] = {}
        filler = S.Batch({**self.msg, 'batch': 'filler', 'klass': 5, 'strict_once': False,
                          'retain_journals': False, 'items': [{'name': f'f{i}'} for i in range(8)]})
        self.s.batches[filler.id] = filler
        self.vm.slots = 8
        with patch.object(S.threading, 'Thread'):
            for item in filler.items.values():
                self.assertTrue(self.s._place(item, self.vm))
        self.assertEqual(self.vm.used(), 8)
        self.vm.ready = False
        self.s.t_wake = 0  # charge the normal one strike per failed legacy attempt
        self.assertTrue(self.s._retire_worker(self.vm, 'error', 'test'))
        self.assertFalse(self.s._retire_worker(self.vm, 'error', 'test'))
        self.assertEqual(self.vm.used(), 0)
        self.assertTrue(self.s._pool_settled())
        for item in filler.items.values():
            self.assertEqual((item.state, item.fails, item.avoid), ('queued', 1, {self.vm.id}))
        self.assertEqual(self.s._load_worker_retirements()[self.vm.id]['used_slots'], 8)

    def test_nonterminal_and_missing_provider_observations_never_mean_loss(self):
        partial = boat.Listing()
        partial.complete = False
        for listing in (partial, boat.Listing(), {self.vm.id: {'state': 'starting'}}):
            with self.subTest(listing=listing), patch.object(boat, 'states', return_value=listing), \
                 patch.object(boat, 'bj', return_value={'sandbox': {'id': 'wrong-vm', 'state': 'error'}}):
                self.assertIsNone(self.s._provider_terminal_state(self.vm.id, detail=True))

    def test_boot_wait_observes_early_terminal_and_waits_for_live_hydration(self):
        key = self.vm.id
        with patch.object(boat, 'states', return_value={key: {'state': 'error'}}), \
             patch.object(boat, 'exec_') as check:
            with self.assertRaises(boat.WorkerBootTerminated) as fail:
                boat.wait_running([key], 60, abort_terminal=True)
        self.assertEqual(fail.exception.state, 'error')
        check.assert_not_called()
        with patch.object(boat, 'states', side_effect=[{key: {'state': 'starting', 'hydrated': False}},
                                                      {key: {'state': 'ready', 'hydrated': True}}]), \
             patch.object(boat.time, 'sleep'), patch.object(boat, 'exec_', return_value={'exitCode': 0}) as check:
            self.assertIn(key, boat.wait_running([key], 60, abort_terminal=True))
        check.assert_called_once_with(key, ['true'], 30)

    def test_reconcile_never_deletes_provider_error_even_without_snapshot(self):
        key = self.vm.id
        lease = {'pid': 99, 'host': 'other', 'batch': 'another-client'}
        for holder in (None, lease):
            registry = {'vms': {key: {'state': 'running', 'lease': holder}}}
            with patch.object(boat, 'delete_vm') as delete:
                boat.reconcile(registry, {key: {'state': 'error', 'dataExpiredAt': 'yesterday'}})
            delete.assert_not_called()
            self.assertEqual(registry['vms'][key], {'state': 'error', 'lease': holder})


if __name__ == '__main__':
    unittest.main()
