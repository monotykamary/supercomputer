"""Preparation retries exercise the real scheduler; mock RPCs are not native proofs."""
import json
import pathlib
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from tests import test_owned_scheduler as fixtures
from supercomputer import scheduler as S


class PrelaunchSchedulerTests(unittest.TestCase):
    setUp = fixtures.OwnedSchedulerTests.setUp
    write_batch = fixtures.OwnedSchedulerTests.write_batch
    place_without_thread = fixtures.OwnedSchedulerTests.place_without_thread
    good_receipt = fixtures.OwnedSchedulerTests.good_receipt

    def configure(self, failures=0, lose_start_reply=False):
        self.place_without_thread()
        self.commands, self.starts = [], []
        self.remaining = failures
        self.s._prepare = Mock()
        def rpc(command, *args, **kwargs):
            self.assertEqual(command, 'owned_rpc')
            request = args[0]
            kind = request['command']
            self.commands.append(kind)
            if kind == 'capabilities' and self.remaining:
                self.remaining -= 1
                raise S.AgentError('HTTP Error 502: fixture pre-launch transport failure')
            if kind == 'vtick':
                for start in request['args'][0].get('start', []):
                    self.starts.append(start['spec']['launch_identity'])
                if lose_start_reply:
                    raise S.AgentError('HTTP Error 502: fixture reply lost AFTER physical start')
            return {'ok': True, 'protocol': 1, 'version': self.s.agent_version,
                    'nonce': request['nonce'], 'features': {'pidfd': True,
                    'private_pid_namespace': 'verified-before-payload'},
                    'result': {'done': [], 'running': []} if kind == 'vtick' else {}}
        self.vm.client.rpc.side_effect = rpc

    def run_start(self):
        with patch.object(S, 'BOAT_TRIES', 3), patch.object(self.s.stop, 'wait', return_value=False):
            self.s._start_on(self.it, self.vm)

    def events(self):
        return S.jlines(self.root / 'queue' / f'{self.b.id}.attempts')

    def test_one_capability_502_retries_same_binding_then_starts_once(self):
        self.configure(failures=1)
        self.run_start()
        self.assertEqual(self.commands, ['capabilities', 'capabilities', 'vtick'])
        self.assertEqual(self.starts, [self.it.launch_identity])
        self.assertEqual(self.it.owned.placements, 1)
        self.assertEqual([r['event'] for r in self.events()],
                         ['prelaunch_reserved', 'prelaunch_not_started', 'start_requested', 'start_response'])
        self.assertTrue(self.s._prelaunch_evidence(self.it)['complete'])
        self.assertTrue(self.s._prelaunch_evidence(self.it)['start_requested'])
        self.assertFalse(self.b.rows)

    def test_prepare_transport_failure_retries_without_replacement(self):
        self.configure()
        self.s._prepare.side_effect = [S.AgentError('HTTP Error 502: prepare'), None]
        self.run_start()
        self.assertEqual(self.s._prepare.call_count, 2)
        self.assertEqual(len(self.starts), 1)
        self.assertEqual(self.it.owned.placements, 1)

    def test_exhaustion_is_not_started_and_retains_cleanup_hold(self):
        self.configure(failures=10)
        self.run_start()
        self.assertEqual(self.commands, ['capabilities'] * 3)
        self.assertFalse(self.starts)
        row = self.b.rows[-1]
        self.assertEqual(row['status'], 'NOT_STARTED')
        self.assertEqual(row['prelaunch_failures'], 3)
        self.assertTrue(row['prelaunch_evidence']['complete'])
        self.assertFalse(row['prelaunch_evidence']['start_requested'])
        self.assertIsNot(row.get('cleanup_complete'), True)
        self.assertEqual(self.it.owned.placements, 1)
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)

    def test_lost_start_reply_never_retries_or_claims_not_started(self):
        self.configure(lose_start_reply=True)
        self.run_start()
        self.assertEqual(self.starts, [self.it.launch_identity])
        self.assertEqual(self.commands, ['capabilities', 'vtick'])
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')
        self.assertNotIn('prelaunch_evidence', self.b.rows[-1])
        self.assertEqual([r['event'] for r in self.events()], ['prelaunch_reserved', 'start_requested'])

    def test_missing_audit_never_even_prepares(self):
        self.configure()
        (self.root / 'queue' / f'{self.b.id}.attempts').unlink()
        self.run_start()
        self.assertFalse(self.commands)
        self.s._prepare.assert_not_called()
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')

    def test_torn_audit_never_even_prepares(self):
        self.configure()
        with (self.root / 'queue' / f'{self.b.id}.attempts').open('ab') as f:
            f.write(b'{"event":"start_requested"')
        self.run_start()
        self.assertFalse(self.commands)
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')

    def test_prior_durable_start_intent_blocks_every_retry(self):
        self.configure()
        self.assertTrue(self.s._attempt_audit(self.vm,
            [{'batch': self.b.id, 'spec': {'name': self.it.run_name,
              'launch_identity': self.it.launch_identity}}], 'start_requested'))
        self.run_start()
        self.assertFalse(self.commands)
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')

    def test_cancellation_during_backoff_prevents_second_rpc(self):
        self.configure(failures=1)
        def close(*args):
            self.b.admission_closed = True
            self.it.owned.closed = True
            return False
        with patch.object(S, 'BOAT_TRIES', 3), patch.object(self.s.stop, 'wait', side_effect=close):
            self.s._start_on(self.it, self.vm)
        self.assertEqual(self.commands, ['capabilities'])
        self.assertFalse(self.starts)
        self.assertEqual(self.it.state, 'cancel_pending')

    def test_failed_failure_journal_prevents_a_second_rpc(self):
        self.configure(failures=1)
        append = self.s._append_owned
        def write(bid, suffix, row):
            if suffix == 'attempts' and row.get('event') == 'prelaunch_not_started':
                raise OSError('fixture fsync failure')
            return append(bid, suffix, row)
        with patch.object(self.s, '_append_owned', side_effect=write):
            self.run_start()
        self.assertEqual(self.commands, ['capabilities'])
        self.assertFalse(self.starts)
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')

    def test_boot_change_during_preparation_prevents_payload_rpc(self):
        self.configure()
        def reboot(*args):
            self.vm.lease = {**self.vm.lease, 'boot': 'different-boot'}
        self.s._prepare.side_effect = reboot
        self.run_start()
        self.assertEqual(self.commands, ['capabilities'])
        self.assertFalse(self.starts)
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')

    def test_failed_start_journal_prevents_payload_rpc(self):
        self.configure()
        append = self.s._append_owned
        def write(bid, suffix, row):
            if suffix == 'attempts' and row.get('event') == 'start_requested':
                raise OSError('fixture fsync failure')
            return append(bid, suffix, row)
        with patch.object(self.s, '_append_owned', side_effect=write):
            self.run_start()
        self.assertEqual(self.commands, ['capabilities'])
        self.assertFalse(self.starts)
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')


if __name__ == '__main__':
    unittest.main()
