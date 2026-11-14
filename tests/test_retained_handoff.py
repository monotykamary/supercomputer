"""Retained closed evidence is ACKed without pre-commit archive mutation."""
import contextlib
import copy
import json
import os
import pathlib
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S, history as H, canary
from test_wiring import sched
import test_owned_scheduler as fixture


class RetainedHandoff(unittest.TestCase):
    setUp=fixture.OwnedSchedulerTests.setUp
    write_batch=fixture.OwnedSchedulerTests.write_batch
    place_without_thread=fixture.OwnedSchedulerTests.place_without_thread
    good_receipt=fixture.OwnedSchedulerTests.good_receipt

    def closed(self, archive=True):
        self.msg['items'][0]['distinct_vm_group']='retained-group'
        self.it.spec['distinct_vm_group']='retained-group'
        self.s._pin_batch_image(self.b)
        self.write_batch();self.place_without_thread()
        self.vm.client.rpc.return_value=self.good_receipt()
        if archive:
            self.assertTrue(self.s.cancel(self.b.id)['cleanup_complete'])
        else:
            # Stop at the real pre-retention crash boundary: cancel has fenced
            # the admission and persisted reaps, but has not archived or closed
            # its live image pin. Never rewind a modern closed pin.
            with patch.object(self.s,'_retain_closed') as retain:
                self.assertTrue(self.s.cancel(self.b.id)['cleanup_complete'])
                retain.assert_called_once_with(self.b,self.s._close_receipt(self.b))
            record=H.load(self.root/'queue'/(self.b.id+'.json'))
            self.assertTrue(record['admission_closed'])
            self.assertTrue(record['cancellation_complete'])
            self.assertTrue(self.it.owned.reaped)
            self.assertTrue(self.s._pins().get('batch:'+self.b.id,S.time.time())['live'])
        self.groups={self.b.id:{'retained-group':[self.vm.id]}}

    def candidate(self):
        child=sched();child.code='child';child.canary=canary.Canary(self.root)
        snap={'from_pid':os.getpid()+100000,'t':S.time.time(),'from':'parent','vms':[],
              'batches':[self.b.id],'distinct_vm_groups':copy.deepcopy(self.groups),'admission_guard':True}
        (self.root/'handoff.json').write_text(json.dumps(snap))
        registry=SimpleNamespace(locked=lambda:contextlib.nullcontext({'vms':{}}))
        with patch.object(S,'Registry',return_value=registry),patch.object(S.boat,'pid_alive',return_value=True), \
             patch.object(S.threading,'Thread'):
            child.reload()
        return child

    def test_preexisting_archive_is_verified_and_acked_without_becoming_work(self):
        self.closed()
        child=self.candidate();ack=child._handoff_ack
        self.assertTrue(ack['admissions']['ready'],ack)
        self.assertEqual(ack['distinct_vm_groups'],self.groups)
        self.assertIn(self.b.id,ack['admissions']['closed'])
        self.assertNotIn(self.b.id,child.batches)
        with patch.object(S.boat,'pid_alive',return_value=True):
            self.assertFalse(child._activate_handoff())

    def test_legacy_closed_migration_waits_until_parent_commit(self):
        self.closed(archive=False)
        path=self.root/'queue'/(self.b.id+'.json');original=path.read_bytes()
        def queue_tree():
            return {p.relative_to(path.parent):p.read_bytes()
                    for p in path.parent.rglob('*') if p.is_file()}
        original_tree=queue_tree()
        pins=self.root/'image-pins.json';original_pins=pins.read_bytes()
        self.assertFalse((path.parent/(self.b.id+'.closed')).exists())
        self.assertFalse((path.parent/(self.b.id+'.retired')).exists())
        child=self.candidate()
        self.assertTrue(child._handoff_ack['admissions']['ready'])
        self.assertEqual(child._handoff_ack['distinct_vm_groups'],self.groups)
        self.assertEqual(path.read_bytes(),original)
        child._handoff_deferred=(os.getpid()+100000,[])
        with patch.object(S.boat,'pid_alive',return_value=True),patch.object(S.threading,'Thread') as start:
            child._history_tick();start.assert_not_called()
        self.assertEqual(path.read_bytes(),original)
        self.assertEqual(queue_tree(),original_tree)
        self.assertEqual(pins.read_bytes(),original_pins)
        child._handoff_deferred=None
        def inline(*args,**kwargs):
            return SimpleNamespace(start=lambda:kwargs['target']())
        with patch.object(S.threading,'Thread',side_effect=inline):child._history_tick()
        self.assertFalse(path.exists())
        self.assertEqual(H.distinct_groups(self.root/'queue',self.b.id),self.groups[self.b.id])
        self.assertNotIn(self.b.id,child.batches)

    def test_closed_pin_with_resurrected_admission_is_quarantined(self):
        self.closed(archive=False)
        # Deliberately build the invalid hybrid the legacy fixture must avoid:
        # a queue admission survives alongside a modern closed-pin tombstone.
        self.s._pins().mark_batch_closed('batch:'+self.b.id,S.time.time())
        path=self.root/'queue'/(self.b.id+'.json');original=path.read_bytes()
        pins=self.root/'image-pins.json';original_pins=pins.read_bytes()
        child=self.candidate()
        self.assertFalse(child._handoff_ack['admissions']['ready'])
        self.assertTrue(child.recovery_blocked)
        self.assertIn('batch image differs from its durable pin or is already closed',
                      child.recovery_errors[self.b.id])
        self.assertNotIn(self.b.id,child.batches)
        self.assertEqual(path.read_bytes(),original)
        self.assertEqual(pins.read_bytes(),original_pins)
        self.assertFalse(child._history_pending)

    def test_wrong_closed_group_is_not_echoed_as_valid(self):
        self.closed();self.groups[self.b.id]['retained-group']=['wrong-vm']
        child=self.candidate()
        self.assertFalse(child._handoff_ack['admissions']['ready'])
        self.assertIn('retained distinct history mismatch',child.recovery['retained_history_error'])
        self.assertTrue(child.recovery_blocked)

    def test_missing_or_corrupt_archive_never_authorizes_an_ack(self):
        self.closed()
        meta=H.read(self.root/'queue',self.b.id)
        path=self.root/'queue'/(self.b.id+'.results')/(meta['audit']['snapshot.json']['sha256']+'.payload')
        original=path.read_bytes()
        changed=original.replace(b'vm-test',b'vm-fake')
        self.assertNotEqual(changed,original)
        path.write_bytes(changed)  # valid JSON and same length, wrong content hash
        child=self.candidate()
        self.assertFalse(child._handoff_ack['admissions']['ready'])
        self.assertEqual(child.recovery['retained_history_error'],'retained snapshot digest mismatch')
        path.unlink()
        child=self.candidate()
        self.assertFalse(child._handoff_ack['admissions']['ready'])


if __name__=='__main__':unittest.main()
