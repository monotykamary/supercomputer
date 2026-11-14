"""Lost-reply delivery uses the real transport and durable worker dispatcher; no providers."""
import http.client
import itertools
import json
import pathlib
import secrets
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import agent as A, boat as B, scheduler as S
from broken_owned_rpc import broken_owned_rpc_decision

FEATURES = {'pidfd': True, 'private_pid_namespace': 'verified-before-payload'}

class OwnedReplayTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.root = pathlib.Path(tmp.name)
        for name, value in [('ROOT', self.root), ('_owned_nonces', set()), ('_owned_children', {})]:
            p = patch.object(A, name, value); p.start(); self.addCleanup(p.stop)
        p = patch.object(A, 'owned_features', return_value=FEATURES); p.start(); self.addCleanup(p.stop)
        self.request = dict(protocol=1, version=A.VERSION, nonce=secrets.token_hex(16), command='vtick', args=[{'start': [], 'acks': [], 'wait': 0}])
        self.result = {'done': [{'name': 'original', 'code': 0}], 'running': []}

    def test_real_transport_lost_accepted_reply_replays_original_result(self):
        client = object.__new__(B.AgentClient)
        client.vm, client.owner, client.token = 'vm', 'owner', 'secret'
        client.lease, client.cookie, client.query, client.base = {}, '', '', ''
        received, responses = [], []
        connection = Mock()
        def send(method, path, body, headers):
            request = json.loads(body)['args'][0]
            received.append(request)
            responses.append(A.owned_rpc(request))
        def receive():
            if len(received) == 1:
                raise http.client.RemoteDisconnected('accepted response lost')
            response = Mock(status=200)
            response.read.return_value = json.dumps(responses[-1]).encode()
            response.getheaders.return_value = []
            response.getheader.return_value = 'application/json'
            return response
        connection.request.side_effect, connection.getresponse.side_effect = send, receive
        with patch.object(A, 'vtick', return_value=self.result) as body, patch.object(client, '_get', return_value=connection), patch.object(client, '_put_back'), patch.object(B.time, 'sleep'), patch.object(client, '_rpc_exec') as fallback:
            result = S.OwnedAgentClient(client, A.VERSION, replay_safe=True).rpc('vtick', self.request['args'][0])
        self.assertEqual(result, self.result)
        self.assertEqual(len(received), 2)
        self.assertEqual(received[0], received[1])
        self.assertEqual(responses[0], responses[1])
        body.assert_called_once(); fallback.assert_not_called()

    def test_old_worker_does_not_get_blind_transport_retry(self):
        client = object.__new__(B.AgentClient)
        client.vm, client.owner, client.token = 'vm', 'owner', 'secret'
        client.lease, client.cookie, client.query, client.base = {}, '', '', ''
        connection = Mock()
        connection.getresponse.side_effect = http.client.RemoteDisconnected('accepted reply lost')
        with patch.object(client, '_get', return_value=connection), patch.object(client, '_rpc_exec') as fallback, patch.object(B.time, 'sleep'):
            with self.assertRaises(B.AgentTransportError):
                S.OwnedAgentClient(client, A.VERSION).rpc('capabilities')
        connection.request.assert_called_once(); fallback.assert_not_called()

    def test_only_observed_idempotency_capability_enables_replay(self):
        from types import SimpleNamespace
        scheduler = object.__new__(S.Scheduler)
        scheduler.agent_version = A.VERSION
        for value, expected in ((1, True), (2, True), (True, False), (0, False), (None, False)):
            vm = SimpleNamespace(client=Mock(), agent=A.VERSION, agent_capabilities={'owned_rpc_idempotency_protocol': value})
            self.assertIs(scheduler._owned_client(vm).replay_safe, expected)

    def test_refusal_diagnostics_only_expose_known_reason_codes(self):
        client = Mock()
        for reason, expected in (('owned_rpc_in_progress', 'owned_rpc_in_progress'), ('secret-token', 'invalid_envelope')):
            client.rpc.return_value = {'ok': False, 'error': reason, 'detail': 'secret-body'}
            with self.assertRaises(S.OwnedAgentRefused) as error:
                S.OwnedAgentClient(client, A.VERSION).rpc('capabilities')
            self.assertIn(expected, str(error.exception))
            self.assertNotIn('secret', str(error.exception))

    def test_authenticated_dispatch_fences_cached_and_fresh_requests(self):
        fence = A.lease('claim', {'owner': 'owner-a', 'vm_id': 'vm-a'})
        self.assertTrue(fence['ok'])
        def call(request, lease=fence, owner='owner-a'):
            return A.rpc_dispatch('owned_rpc', [request, {'_lease': lease}], token_owner=owner)
        with patch.object(A, 'vtick', return_value=self.result) as body:
            first = call(self.request)
            self.assertTrue(first['ok'])
            original = A._lease_record()
            changes = ({'state': 'released'}, {'state': 'stopping'}, {'draining': True},
                       {'epoch': original['epoch'] + 1}, {'holder': 'owner-b'}, {'boot': 'other-boot'})
            for change in changes:
                with self.subTest(change=change):
                    A._lease_save({**original, **change})
                    self.assertTrue(call(self.request)['stale'])
                    self.assertTrue(call({**self.request, 'nonce': secrets.token_hex(16)})['stale'])
            A._lease_save(original)
            self.assertEqual(call(self.request), first)
            body.assert_called_once()

    def test_authenticated_owner_change_cannot_replay_receipt(self):
        fence = A.lease('claim', {'owner': 'owner-a', 'vm_id': 'vm-a'})
        with patch.object(A, 'vtick', return_value=self.result) as body:
            first = A.rpc_dispatch('owned_rpc', [self.request, {'_lease': fence}], token_owner='owner-a')
            self.assertTrue(first['ok'])
            record = A._lease_record()
            A._lease_save({**record, 'holder': 'owner-b', 'owner': 'owner-b', 'epoch': record['epoch'] + 1})
            other = {**fence, 'holder': 'owner-b', 'owner': 'owner-b', 'epoch': record['epoch'] + 1}
            reply = A.rpc_dispatch('owned_rpc', [self.request, {'_lease': other}], token_owner='owner-b')
            self.assertEqual(reply['error'], 'owned_nonce_conflict')
            body.assert_called_once()

    def test_changed_source_cannot_replay_receipt(self):
        with patch.object(A, 'vtick', return_value=self.result) as body:
            A.owned_rpc(self.request)
            with patch.object(A, 'VERSION', 'different-source'):
                self.assertEqual(A.owned_rpc(self.request)['error'], 'owned_nonce_conflict')
            body.assert_called_once()

    def test_changed_body_with_used_nonce_is_refused(self):
        with patch.object(A, 'vtick', return_value=self.result) as body:
            first = A.owned_rpc(self.request)
            for change in ({'command': 'capabilities'}, {'args': [{'start': [], 'acks': [], 'wait': 1}]}, {'extra': True}):
                reply = A.owned_rpc({**self.request, **change})
                self.assertFalse(reply['ok']); self.assertEqual(reply['error'], 'owned_nonce_conflict')
            self.assertEqual(A.owned_rpc(self.request), first)
            body.assert_called_once()

    def test_cached_reply_survives_nonce_memory_reset(self):
        with patch.object(A, 'vtick', return_value=self.result) as body:
            first = A.owned_rpc(self.request)
            A._owned_nonces.clear()
            self.assertEqual(A.owned_rpc(self.request), first)
            body.assert_called_once()
        receipt = json.loads((self.root/'owned-rpc'/(self.request['nonce']+'.json')).read_text())
        self.assertEqual(receipt['state'], 'complete')
        self.assertEqual(receipt['reply'], first)

    def test_in_progress_survives_lost_completion_write_and_never_reexecutes(self):
        real_write = A._owned_write
        def write(path, row):
            if row.get('state') == 'complete':
                raise OSError('completion disk failure')
            real_write(path, row)
        with patch.object(A, 'vtick', return_value=self.result) as body, patch.object(A, '_owned_write', side_effect=write):
            self.assertEqual(A.owned_rpc(self.request)['error'], 'owned_receipt_unavailable')
        A._owned_nonces.clear()
        with patch.object(A, 'vtick') as body_again:
            reply = A.owned_rpc(self.request)
        self.assertEqual(reply['error'], 'owned_rpc_in_progress')
        body.assert_called_once(); body_again.assert_not_called()

    def test_failed_intent_write_never_runs_command(self):
        with patch.object(A, '_owned_write', side_effect=OSError('full disk')), patch.object(A, 'vtick') as body:
            self.assertEqual(A.owned_rpc(self.request)['error'], 'owned_receipt_unavailable')
        body.assert_not_called(); self.assertNotIn(self.request['nonce'], A._owned_nonces)

    def test_corrupt_or_null_receipt_never_reexecutes(self):
        path = self.root/'owned-rpc'/(self.request['nonce']+'.json')
        path.parent.mkdir()
        for raw in ('null', '[]', '{}', '{bad'):
            path.write_text(raw)
            with self.subTest(raw=raw), patch.object(A, 'vtick') as body:
                self.assertEqual(A.owned_rpc(self.request)['error'], 'owned_receipt_unavailable')
                body.assert_not_called()

    def test_cached_reply_does_not_authorize_new_execution_during_upgrade(self):
        with patch.object(A, 'vtick', return_value=self.result) as body:
            first = A.owned_rpc(self.request)
            (self.root/'owned-upgrading').touch()
            with patch.object(A, 'owned_features', return_value=None):
                self.assertEqual(A.owned_rpc(self.request), first)
                fresh = {**self.request, 'nonce': secrets.token_hex(16)}
                self.assertFalse(A.owned_rpc(fresh)['ok'])
            body.assert_called_once()

    def test_changed_boot_cannot_replay_or_execute_an_old_request(self):
        with patch.object(A, 'vtick', return_value=self.result) as body:
            A.owned_rpc(self.request)
            with patch.object(A, '_lease_boot', return_value='another-boot'):
                self.assertEqual(A.owned_rpc(self.request)['error'], 'owned_nonce_conflict')
            body.assert_called_once()

    def test_dispatch_failure_is_also_replayed_without_execution(self):
        with patch.object(A, 'vtick', side_effect=ValueError('fixture')) as body:
            first = A.owned_rpc(self.request)
            self.assertEqual(first['error'], 'owned_dispatch_refused')
            self.assertEqual(A.owned_rpc(self.request), first)
            body.assert_called_once()

    def test_concurrent_identical_requests_share_one_execution(self):
        replies = []
        with patch.object(A, 'vtick', return_value=self.result) as body:
            threads = [threading.Thread(target=lambda: replies.append(A.owned_rpc(self.request))) for _ in range(2)]
            for thread in threads: thread.start()
            for thread in threads: thread.join(3); self.assertFalse(thread.is_alive())
            body.assert_called_once()
        self.assertEqual(len(replies), 2); self.assertEqual(replies[0], replies[1])

    def test_decision_truth_table_and_broken_replay(self):
        for found, request, scope, complete, allowed in itertools.product((False, True), repeat=5):
            expected = ('refuse' if not request or not scope else 'replay' if complete else 'pending') if found else 'execute' if allowed else 'refuse'
            self.assertEqual(A.owned_rpc_decision(found, request, scope, complete, allowed), expected)
        self.assertEqual(broken_owned_rpc_decision(True, False, True, True, True), 'execute')
        self.assertEqual(A.owned_rpc_decision(True, False, True, True, True), 'refuse')

if __name__ == '__main__': unittest.main()
