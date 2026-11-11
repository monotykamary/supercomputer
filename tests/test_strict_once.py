"""Strict-once public contract and scheduler receipts (native remote gate only)."""
import http.client
import json
import pathlib
import sys
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S
from supercomputer import __main__ as CLI
from supercomputer.rules.owned_lifecycle import attempt_cap_supported, requeue_after_loss, strict_transport_attempts
import test_owned_scheduler as fixture


class StrictOnceContract(unittest.TestCase):
    setUp = fixture.OwnedSchedulerTests.setUp
    write_batch = fixture.OwnedSchedulerTests.write_batch
    place_without_thread = fixture.OwnedSchedulerTests.place_without_thread
    good_receipt = fixture.OwnedSchedulerTests.good_receipt

    def test_hard_cap_validation_is_not_a_vm_or_strike_limit(self):
        for cap in (0, 2, -1, True, 1.0, '1', None):
            with self.subTest(cap=cap), self.assertRaises(ValueError):
                self.s._validate_submission({**self.msg, 'max_attempts': cap})
        self.s._validate_submission({**self.msg, 'max_attempts': 1})
        with self.assertRaisesRegex(ValueError, 'requires strict_once'):
            self.s._validate_submission({**self.msg, 'strict_once': False, 'retain_journals': False, 'max_attempts': 1})
        self.assertFalse(attempt_cap_supported(True, 2))
        self.assertEqual(strict_transport_attempts(True, 4), 1)
        self.assertEqual(strict_transport_attempts(False, 4), 4)
        self.assertFalse(requeue_after_loss(True))

    def test_older_placement_without_boot_stays_explicitly_unidentified(self):
        self.place_without_thread()
        self.it.last_placement.pop("boot_id")
        with self.s.lock:
            self.s._owned_failed(self.it, "older journal has no boot evidence")
        self.assertTrue(self.b.rows[-1]["identity_unavailable"])
        self.assertIsNone(self.b.rows[-1]["boot_id"])

    def test_unknown_boot_cannot_consume_or_launch_a_strict_attempt(self):
        self.vm.lease.pop("boot")
        self.assertFalse(self.s._place(self.it, self.vm))
        self.assertEqual(self.it.owned.placements, 0)
        self.assertIsNone(self.it.launch_identity)

    def assert_lost_identity(self):
        row = self.b.rows[-1]
        self.assertEqual(self.it.state, 'done')
        self.assertEqual(row['status'], 'LOST')
        self.assertIs(row['terminal'], True)
        self.assertNotIn('lost', row)
        self.assertEqual(row['vm'], self.vm.id)
        self.assertEqual(row['boot_id'], 'fixture-boot')
        self.assertEqual(row['attempt'], 1)
        self.assertEqual(row['max_attempts'], 1)
        self.assertEqual(row['attempts'], 1)
        self.assertEqual(row['launch_identity'], self.it.launch_identity)
        self.assertTrue(self.it.owned.closed)
        self.assertFalse(self.s._place(self.it, self.vm))
        self.assertEqual(self.it.owned.placements, 1)

    def test_failure_is_terminal_lost_with_attempt_identity(self):
        self.place_without_thread()
        self.s._boat_failed(self.it, self.vm.id, 'simulated loss')
        self.assert_lost_identity()

    def test_stale_lease_does_not_even_temporarily_requeue(self):
        self.place_without_thread()
        self.s._drop_stale(self.vm)
        self.assert_lost_identity()
        self.assertNotIn(self.vm.id, self.s.vms)

    def test_slept_through_loss_is_terminal_not_requeued(self):
        self.place_without_thread()
        self.s.t_wake = S.time.time()
        self.s._let_go = Mock()
        self.s._lost_vm(self.vm)
        self.assert_lost_identity()

    def test_preemption_unknown_cleanup_is_terminal_but_not_reaped(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value = {**self.good_receipt(), 'identity': 'wrong'}
        self.s._kill(self.vm, self.it)
        self.assert_lost_identity()
        self.assertFalse(self.b.rows[-1]['cleanup_complete'])
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        receipt = self.s.close(self.b.id)
        self.assertFalse(receipt['cleanup_complete'])
        self.assertTrue(receipt['journals_retained'])
        self.assertEqual(self.it.state, 'done')

    def test_close_reports_every_attempt_and_positive_cleanup(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value = self.good_receipt()
        receipt = self.s.close(self.b.id)
        self.assertTrue(receipt['complete'])
        self.assertTrue(receipt['cleanup_complete'])
        self.assertTrue(receipt['journals_retained'])
        self.assertEqual(receipt['max_attempts'], 1)
        self.assertEqual(receipt['attempts'], [{'name': self.it.name, 'run_name': self.it.run_name,
            'vm': self.vm.id, 'boot_id': 'fixture-boot', 'attempt': 1,
            'launch_identity': self.it.launch_identity, 'cleanup_complete': True}])
        for suffix in ('json', 'placed', 'rows', 'cancel'):
            self.assertTrue((self.root / 'queue' / f'{self.b.id}.{suffix}').exists())
        self.assertEqual(self.s.close(self.b.id)['attempts'], receipt['attempts'])

    def test_zero_launch_close_reports_zero_without_fabricated_identity(self):
        receipt = self.s.close(self.b.id)
        self.assertTrue(receipt['cleanup_complete'])
        self.assertEqual(receipt['attempts'], [])
        self.assertEqual(receipt['unlaunched'], [self.it.name])
        self.assertFalse(self.vm.client.rpc.called)

    def test_lost_result_survives_pending_close_reload(self):
        self.place_without_thread()
        self.s._boat_failed(self.it, self.vm.id, 'lost')
        self.vm.client.rpc.return_value = {**self.good_receipt(), 'identity': 'wrong'}
        self.assertFalse(self.s.close(self.b.id)['cleanup_complete'])
        self.s.batches = {}
        self.s._take_pool = Mock()
        with patch.object(S.threading, 'Thread'):
            self.s.reload()
        restored = self.s.batches[self.b.id]
        self.assertEqual(restored.items[self.it.name].state, 'done')
        self.assertEqual(restored.rows[-1]['status'], 'LOST')
        self.assertEqual(restored.items[self.it.name].owned.placements, 1)

    def test_strict_submit_transport_is_attempted_once(self):
        client = object.__new__(S.Client)
        client.port, client.token = 1, 'private-token'
        connection = Mock()
        connection.sock = None
        connection.getresponse.side_effect = http.client.RemoteDisconnected('lost response')
        for strict, attempts in ((True, 1), (False, 3)):
            client._conns = []
            connection.reset_mock()
            with patch.object(http.client, 'HTTPConnection', return_value=connection), patch.object(S.time, 'sleep'):
                with self.assertRaisesRegex(RuntimeError, "didn't answer"):
                    client._local('/submit', json.dumps({'strict_once': strict}).encode(), 1)
            self.assertEqual(connection.request.call_count, attempts)

    def test_cli_parses_explicit_single_attempt_tuple(self):
        with patch.object(CLI, '_run') as run, patch.object(CLI.config, 'use'):
            CLI.main(['run', '--jobs', 'jobs.json', '--out', 'out', '--backend', 'boat',
                      '--strict-once', '--max-attempts', '1'])
        parsed = run.call_args.args[0]
        self.assertTrue(parsed.strict_once)
        self.assertEqual(parsed.max_attempts, 1)
        self.assertEqual(parsed.backend, 'boat')


if __name__ == '__main__':
    unittest.main()
