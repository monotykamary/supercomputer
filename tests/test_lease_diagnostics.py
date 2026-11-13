"""First-cause diagnostics only: no provider calls or policy changes."""
import ast
import json
import pathlib
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import lease_diagnostics as D, scheduler as S
from test_owned_scheduler import OwnedSchedulerTests as _Fixture


class PublicDiagnostics(unittest.TestCase):
    def test_no_request_arguments_or_credentials_in_event(self):
        client = Mock()
        client.token = 'exact-client-secret'
        client.cookie = 'session-cookie'
        error = RuntimeError('403 token="third-party-key" cookie=session-cookie '
                             'https://private.example/rpc?token=url-key exact-client-secret '
                             'Authorization=Bearer bearer-key')
        event = D.event('vm', 'client.exception', client=client, error=error,
                        reply={'stale': True, 'token': 'reply-secret', 'env': {'KEY': 'env-secret'},
                               'result': {'stale': True, 'args': ['payload-secret'], 'stop_nonce': 'nonce-secret'}})
        text = json.dumps(event)
        for secret in ('third-party-key', 'session-cookie', 'url-key', 'exact-client-secret',
                       'bearer-key', 'reply-secret', 'env-secret', 'payload-secret', 'nonce-secret'):
            self.assertNotIn(secret, text)
        self.assertTrue(event['reply']['result']['stale'])
        self.assertEqual(event['error']['classification_matches'], ['403'])

    def test_event_is_bounded_and_keeps_fence_comparison(self):
        event = D.event('v' * 10000, 's' * 10000, error=RuntimeError('x' * 100000),
                        lease={'boot': 'expected', 'epoch': 3, 'owner': 'a'},
                        reply={'boot': 'actual', 'epoch': 4, 'holder': 'b', 'state': 'held',
                               'error': 'e' * 100000, 'uptime': float('inf'), 'protocol': 10**10000,
                               'unbounded': ['x'] * 10000})
        self.assertLess(len(json.dumps(event)), 3000)
        self.assertEqual(event['expected']['epoch'], 3)
        self.assertEqual(event['reply']['epoch'], 4)
        self.assertNotEqual(event['expected']['owner_sha256'], event['reply']['holder_sha256'])
        self.assertEqual(event['reply']['protocol'], 'out-of-range')
        self.assertEqual(event['reply']['uptime'], 'nonfinite')
        self.assertEqual(len(event['error']['sha256']), 64)

    def test_operation_names_only_never_nested_request_args(self):
        args = ('owned_rpc', {'command': 'begin', 'args': ['secret-context']})
        self.assertEqual(D.operation('rpc', args), 'rpc/owned_rpc/begin')
        self.assertNotIn('secret-context', D.operation('rpc', args))

    def test_context_error_tail_is_not_logged(self):
        event = D.event('vm', 'test', error=RuntimeError('403 env={"UNUSUAL_NAME":"private-value"}'))
        self.assertNotIn('private-value', json.dumps(event))


class SchedulerDiagnostics(unittest.TestCase):
    setUp = _Fixture.setUp
    write_batch = _Fixture.write_batch
    place_without_thread = _Fixture.place_without_thread

    def test_nested_stale_reply_kept_in_log_and_retained_terminal_row(self):
        self.place_without_thread()
        transport = self.vm.client
        transport.rpc.return_value = {'ok': True, 'result': {'stale': True, 'epoch': 9, 'token': 'hidden'}}
        client = S.SchedulerAgentClient(transport, self.s, self.vm)
        self.vm.client = client
        with patch.object(S, 'log') as log:
            with self.assertRaises(S.AgentError):
                client.rpc('owned_rpc', {'command': 'begin', 'args': ['never-log']})
            first = dict(self.vm.lease_relinquishment)
            with self.assertRaises(S.AgentError):
                client.rpc('cancelitem', 'batch', 'name')
        self.assertEqual(log.call_count, 1)
        self.assertEqual(first['operation'], 'rpc/owned_rpc/begin')
        self.assertEqual(first['reply']['result']['epoch'], 9)
        self.assertNotIn('hidden', json.dumps(first))
        self.assertNotIn('never-log', json.dumps(first))
        self.assertEqual(self.b.rows[-1]['lease_relinquishment'], first)
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')
        self.assertFalse(self.b.rows[-1]['cleanup_complete'])
        self.assertEqual(self.vm.lease_relinquishment, first)
        transport.rpc.assert_called_once()
        stored = json.loads((self.root/'queue'/(self.b.id+'.rows')).read_text().splitlines()[-1])
        self.assertEqual(stored['lease_relinquishment'], first)

    def test_initiating_exception_not_replaced_by_dead_client_error(self):
        self.place_without_thread()
        transport = self.vm.client
        error = S.AgentError('vm-test: owned_rpc: 403 denied token=private-value')
        transport.rpc.side_effect = error
        self.vm.client = client = S.SchedulerAgentClient(transport, self.s, self.vm)
        with patch.object(S, 'log'):
            with self.assertRaises(S.AgentError) as caught:
                client.rpc('owned_rpc', {'command': 'capabilities', 'args': []})
            self.assertIs(caught.exception, error)
            with self.assertRaises(S.AgentError):
                client.rpc('cancelitem')
        event = self.vm.lease_relinquishment
        self.assertEqual(event['error']['type'], 'AgentError')
        self.assertIn('403 denied', event['error']['message'])
        self.assertNotIn('private-value', event['error']['message'])
        self.assertNotIn('VM relinquished', event['error']['message'])
        self.assertEqual(event['error']['classification_matches'], ['403'])

    def test_ambiguous_text_classifier_is_observed_not_silently_changed(self):
        # A hypothesis for the historical loss, not evidence that it happened there.
        self.place_without_thread()
        transport = self.vm.client
        transport.rpc.side_effect = RuntimeError('unrelated operation expected 400 bytes')
        client = S.SchedulerAgentClient(transport, self.s, self.vm)
        with patch.object(S, 'log'):
            with self.assertRaises(RuntimeError):
                client.rpc('owned_rpc', {'command': 'begin'})
        self.assertTrue(self.vm.dead)
        self.assertEqual(self.vm.lease_relinquishment['error']['classification_matches'], ['400'])
        self.assertIn('expected 400 bytes', self.vm.lease_relinquishment['error']['message'])

    def test_success_does_not_add_rpc_or_diagnostic(self):
        transport = self.vm.client
        transport.rpc.return_value = {'ok': True}
        client = S.SchedulerAgentClient(transport, self.s, self.vm)
        with patch.object(S, 'log') as log:
            self.assertEqual(client.rpc('ping'), {'ok': True})
        log.assert_not_called()
        transport.rpc.assert_called_once_with('ping')
        self.assertFalse(hasattr(self.vm, 'lease_relinquishment'))

    def test_renew_refusal_records_expected_actual_epoch(self):
        self.place_without_thread()
        with patch.object(S.boat, 'lease_vm', return_value={'ok': False, 'stale': True, 'epoch': 4}), patch.object(S, 'log'):
            self.assertFalse(self.s._renew_lease(self.vm))
        self.assertEqual(self.vm.lease_relinquishment['source'], 'renew.refused')
        self.assertEqual(self.vm.lease_relinquishment['expected']['epoch'], 3)
        self.assertEqual(self.vm.lease_relinquishment['reply']['epoch'], 4)

    def test_handoff_refusal_keeps_frozen_inventory_and_reason(self):
        self.s.handing_off = True
        before = dict(self.vm.lease)
        with patch.object(S.boat, 'lease_vm', return_value={'ok': False, 'stale': True}), patch.object(S, 'log'):
            self.assertFalse(self.s._renew_lease(self.vm))
        self.assertTrue(self.s._handoff_lease_lost)
        self.assertFalse(self.vm.dead)
        self.assertIs(self.s.vms[self.vm.id], self.vm)
        self.assertEqual(self.vm.lease, before)
        self.assertEqual(self.vm.lease_relinquishment['source'], 'renew.refused')

    def test_recovered_live_handoff_failure_cannot_mask_actual_relinquish(self):
        self.s.handing_off = True
        with patch.object(S.boat, 'lease_vm', return_value={'ok': False, 'stale': True}), patch.object(S, 'log'):
            self.assertFalse(self.s._renew_lease(self.vm))
            self.s.handing_off = False
            self.s._drop_stale(self.vm, source='client.exception', error=RuntimeError('403 new failure'))
            self.s._drop_stale(self.vm, source='later.cleanup', error=RuntimeError('stale dead wrapper'))
        self.assertTrue(self.vm.dead)
        self.assertEqual(self.vm.lease_relinquishment['source'], 'client.exception')
        self.assertEqual(self.vm.lease_relinquishment['error']['message'], '403 new failure')

    def test_logging_failure_never_blocks_fencing(self):
        self.place_without_thread()
        with patch.object(S, 'log', side_effect=OSError('full log')):
            self.s._drop_stale(self.vm, source='test', error=RuntimeError('403 denied'))
        self.assertTrue(self.vm.dead)
        self.assertEqual(self.b.rows[-1]['status'], 'LOST')
        self.assertEqual(self.b.rows[-1]['lease_relinquishment']['error']['message'], '403 denied')

    def test_formatter_failure_never_blocks_fencing(self):
        self.place_without_thread()
        with patch.object(D, 'event', side_effect=ValueError('bad diagnostic')), patch.object(S, 'log'):
            self.s._drop_stale(self.vm, source='test')
        self.assertTrue(self.vm.dead)
        self.assertEqual(self.vm.lease_relinquishment['diagnostic_error'], 'ValueError')

    def test_stop_fence_refusal_keeps_reply_without_authorizing_stop(self):
        with patch.object(S.boat, 'lease_vm', return_value={'ok': False, 'stale': True, 'epoch': 8}), \
             patch.object(S.boat, 'log') as log:
            self.assertEqual(S.boat._stop_fence(self.vm.id, self.vm.lease), {})
        message = log.call_args.args[0]
        self.assertIn('LEASE_STOP_REFUSED', message)
        self.assertIn('"epoch": 8', message)
        self.assertIn('"source": "stop_fence.refused"', message)

    def test_stop_fence_error_logging_cannot_turn_failure_into_success(self):
        with patch.object(S.boat, 'lease_vm', side_effect=RuntimeError('transport failed')), \
             patch.object(S.boat, 'log', side_effect=OSError('log failed')):
            self.assertEqual(S.boat._stop_fence(self.vm.id, self.vm.lease), {})

    def test_every_production_drop_site_names_its_source(self):
        tree = ast.parse(pathlib.Path(S.__file__).read_text())
        calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                 and n.func.attr == '_drop_stale']
        self.assertGreater(len(calls), 10)
        for call in calls:
            self.assertIn('source', [k.arg for k in call.keywords], 'unlabelled drop at '+str(call.lineno))


if __name__ == '__main__':
    unittest.main()
