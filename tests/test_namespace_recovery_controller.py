"""Controller integration: real proof engine, fake authenticated native reads; no provider calls."""
import copy
import json
import unittest
from unittest.mock import patch

from test_owned_scheduler import OwnedSchedulerTests
from supercomputer import namespace_recovery as N
from supercomputer import namespace_recovery_controller as C
from supercomputer import scheduler as S


class ReadOnlyTransport:
    def __init__(self, binding, new_boot='new-kernel-boot'):
        self.binding = binding
        self.new_boot = new_boot
        self.calls = []
        self.row = {'identity': binding['launch_identity'], 'state': 'launched',
                    'reaped': True, 'contained': False, 'returncode': 1,
                    'error': 'private namespace handshake unsupported or timed out; payload not authorized'}

    def read_capsule(self, binding, nonce, fence=None, *, deadline):
        self.calls.append('capsule')
        return {'kind': N.CAPSULE_KIND, 'nonce': nonce, 'binding': copy.deepcopy(binding),
                'complete': True, 'actual_vm_id': binding['vm'], 'boot_id': self.new_boot,
                'owned_record': copy.deepcopy(self.row), 'owned_record_sha256': N.digest(self.row),
                'owner_tombstone': {k: binding[k] for k in ('batch', 'run_name', 'owner')},
                'native_jobs': [{'owner': 'unrelated-new-boot-owner', 'count': 1}],
                'other_unresolved_owned_items': [{'name': 'new-boot-unrelated-job'}],
                'job_dir_absent': True, 'init_absent': True, 'supervisor_absent': True,
                'lease': {'owner': 'unrelated-new-boot-owner'}}

    def read_boot(self, vm, nonce, *, deadline):
        self.calls.append('boot')
        return {'kind': 'native-boot/v1', 'source': 'native', 'nonce': nonce,
                'vm_id': vm, 'boot_id': self.new_boot}

    def __getattr__(self, name):
        raise AssertionError('read-only recovery attempted mutation: ' + name)


class RecoveryControllerTests(unittest.TestCase):
    def setUp(self):
        self.fixture = f = OwnedSchedulerTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        self.addCleanup(f.tmp.cleanup)
        f.place_without_thread()
        self.s, self.b, self.it, self.root = f.s, f.b, f.it, f.root
        self.b.admission_closed = True
        self.it.owned.cancel()
        self.it.state = 'done'
        self.b.rows = [{'name': self.it.name, 'code': -1, 'status': 'LOST', 'original': True}]
        self.binding = C.binding_for(self.it)
        self.transport = ReadOnlyTransport(self.binding)
        self.req = {'protocol': 1, 'batch': self.b.id, 'name': self.it.name,
                    **{k: self.binding[k] for k in ('vm', 'boot_id', 'attempt', 'launch_identity')},
                    'mode': 'observe-boot', 'authorized': True,
                    'authorization_reference': 'test-operator-consent'}
        self.owner = patch.object(S.boat, 'owner_id', return_value=self.binding['owner'])
        self.owner.start(); self.addCleanup(self.owner.stop)

    def test_changed_boot_adopts_distinct_proof_without_rewriting_original_result(self):
        before = copy.deepcopy(self.b.rows)
        row = copy.deepcopy(self.transport.row)
        result = C.recover(self.s, self.root, self.req, self.transport)
        self.assertTrue(result['ok'])
        self.assertTrue(result['close']['cleanup_complete'])
        self.assertEqual(self.transport.calls, ['capsule', 'boot'])
        proof = result['proof']
        self.assertEqual(proof['kind'], 'vm-boot-extinction/v1')
        self.assertEqual(proof['method'], 'observed-boot-replacement')
        self.assertNotIn('namespace_absent', proof)
        self.assertEqual(before, self.b.rows)
        self.assertEqual(row, self.transport.row)
        self.assertTrue(C.validated_proof(self.root, self.it, proof))
        self.assertFalse(self.s._matching_receipt(proof, self.b.id, self.it.run_name, self.it.launch_identity))
        self.assertEqual(result['close']['attempts'][0]['cleanup_evidence'], proof)

    def test_original_boot_and_fake_foreign_identity_remain_incomplete(self):
        self.transport.new_boot = self.binding['boot_id']
        with self.assertRaises(N.RecoveryError):
            C.recover(self.s, self.root, self.req, self.transport)
        self.assertFalse(self.it.owned.reaped)
        self.assertFalse(self.s.close_receipt(self.b.id)['cleanup_complete'])

    def test_wrong_request_binding_or_missing_authorization_never_reads_vm(self):
        for field, value in [('boot_id', 'wrong'), ('launch_identity', 'wrong'),
                             ('vm', 'wrong'), ('attempt', 2), ('authorized', False)]:
            with self.subTest(field=field):
                req = {**self.req, field: value}
                with self.assertRaises(ValueError):
                    C.recover(self.s, self.root, req, self.transport)
        self.assertEqual(self.transport.calls, [])

    def test_missing_persisted_proof_never_validates_for_restart(self):
        result = C.recover(self.s, self.root, self.req, self.transport)
        path = C._storage(self.root, result['proof']['intent_id'], '.proof')
        path.unlink()
        self.assertFalse(C.validated_proof(self.root, self.it, result['proof']))

    def test_journal_write_failure_never_confirms_cleanup(self):
        with patch.object(self.s, '_append_owned', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                C.recover(self.s, self.root, self.req, self.transport)
        self.assertFalse(self.it.owned.reaped)
        self.assertEqual(self.s._namespace_recoveries, {})

    def test_completed_proof_can_be_readopted_after_journal_failure_without_vm_operations(self):
        with patch.object(self.s, '_append_owned', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                C.recover(self.s, self.root, self.req, self.transport)
        calls = list(self.transport.calls)
        result = C.recover(self.s, self.root, self.req, self.transport)
        self.assertTrue(result['close']['cleanup_complete'])
        self.assertEqual(self.transport.calls, calls)

    def test_recovery_blocks_new_placement_and_repair(self):
        self.s._namespace_recoveries = {self.fixture.vm.id: self.binding}
        with patch.object(self.s, '_renew_lease') as renew:
            self.assertFalse(self.s._repair(self.fixture.vm))
            renew.assert_not_called()
        self.assertFalse(self.s._place(self.it, self.fixture.vm))

    def test_reboot_mode_rejects_unrelated_retained_placement_before_transport(self):
        req = {**self.req, 'mode': 'reboot-owned-vm', 'allow_vm_reboot': True}
        from supercomputer.scheduler import Batch
        other = Batch({**self.fixture.msg, 'batch': 'other-owned-batch'})
        another = other.items['item']; another.last_placement = {'vm': self.binding['vm']}
        self.s.batches[other.id] = other
        with self.assertRaisesRegex(ValueError, 'another placement'):
            C.recover(self.s, self.root, req, self.transport)
        self.assertEqual(self.transport.calls, [])


if __name__ == '__main__':
    unittest.main()
