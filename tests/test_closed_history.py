"""Closed history, expiry and crash recovery; fake scheduler only, remote gate."""
import copy
import json
import pathlib
import tempfile
import time
import unittest
from unittest.mock import Mock, patch
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import history as H, scheduler as S, statebackup, resultfiles
import test_owned_scheduler as fixture


class History(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.q = pathlib.Path(self.tmp.name) / 'queue'
        self.q.mkdir()
        self.bid = 'retained'
        self.info = dict(batch=self.bid, generation='generation-one', admitted_at=50,
                         items=1, rows=1, complete=True, strict_once=False,
                         admission_closed=False)
        self.snapshot = dict(ok=True, batch=self.bid, strict_once=False,
                             placements=[{'name':'item','vm':'first'}],
                             items=[{'name':'item','state':'done'}], rows=[{'name':'item','code':0}])
        self.receipt = {'batch':self.bid, 'cleanup_complete':False}
        (self.q / (self.bid+'.json')).write_text(json.dumps({'batch':self.bid}))
        (self.q / (self.bid+'.placed')).write_text('{"name":"item","vm":"first"}\n')

    def capture(self, hours=48):
        with patch.object(H.time, 'time', return_value=100):
            return H.capture(self.q,self.bid,self.info,self.snapshot,self.receipt,hours)

    def test_configurable_retention_policy_defaults_and_validation(self):
        self.assertEqual(S.policy_defaults()['history_retention_hours'],48)
        with patch.object(S,'POLICY',self.q/'policy.json'):
            self.assertEqual(S.write_policy({'history_retention_hours':'72'})['history_retention_hours'],72)
            self.assertEqual(S.read_policy()['history_retention_hours'],72)
            for bad in (True,-1,'nan','inf'):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    S.write_policy({'history_retention_hours':bad})

    def test_default_window_and_immutable_first_close(self):
        first=self.capture()
        self.assertEqual(first['expires_at'], 100+48*3600)
        self.assertFalse((self.q/(self.bid+'.json')).exists())
        self.assertTrue((self.q/(self.bid+'.retired')).exists())
        with patch.object(H.time, 'time', return_value=200):
            second=H.capture(self.q,self.bid,self.info,self.snapshot,self.receipt,999)
        self.assertEqual(first,second)
        self.assertEqual(H.read(self.q,self.bid,now=100)['generation'],'generation-one')
        self.assertIsNone(H.read(self.q,self.bid,now=first['expires_at']))
        self.assertTrue((self.q/(self.bid+'.closed')).exists())

    def test_pages_and_audit_are_frozen(self):
        meta=self.capture()
        self.snapshot['rows'][0]['code']=99
        (self.q/(self.bid+'.placed')).write_text('mutated')
        self.assertEqual(H.rows_page(self.q,self.bid,0,meta),[{'name':'item','code':0}])
        store=resultfiles.Store(self.q/(self.bid+'.results'))
        with store.open(meta['audit']['placements.jsonl']) as f:
            self.assertEqual(f.read(),b'{"name":"item","vm":"first"}\n')
        self.assertEqual(H.rows_page(self.q,self.bid,1,meta),[])

    def test_expiry_only_deletes_closed_payloads_and_keeps_id_fence(self):
        meta=self.capture(hours=1)
        self.assertFalse(H.collect(self.q,self.bid,now=3699))
        self.assertTrue(H.collect(self.q,self.bid,now=3700))
        self.assertEqual([p.name for p in self.q.iterdir()],[self.bid+'.retired'])
        self.assertIsNone(H.read(self.q,self.bid,now=3700))
        self.assertFalse(H.collect(self.q,self.bid,now=4000))

    def test_pending_active_admission_cannot_be_collected(self):
        self.capture(hours=0)
        active=self.q/(self.bid+'.json')
        active.write_text('{}')
        self.assertFalse(H.collect(self.q,self.bid,now=1000))
        self.assertTrue((self.q/(self.bid+'.results')).exists())

    def test_crash_after_fence_before_rename_is_recoverable(self):
        original=H.finish
        with patch.object(H,'finish',side_effect=OSError('crash')):
            with self.assertRaises(OSError): self.capture()
        record=H.load(self.q/(self.bid+'.json'))
        self.assertIn('closed_history',record)
        self.assertIsNone(H.read(self.q,self.bid))
        original(self.q,self.bid,record)
        self.assertFalse((self.q/(self.bid+'.json')).exists())
        self.assertEqual(H.read(self.q,self.bid,now=200)['closed_at'],100)

    def test_fault_before_fence_keeps_original_admission(self):
        with patch.object(H,'atomic',side_effect=OSError('disk')):
            with self.assertRaises(OSError): self.capture()
        self.assertEqual(H.load(self.q/(self.bid+'.json')),{'batch':self.bid})
        self.assertFalse((self.q/(self.bid+'.closed')).exists())

    def test_fractional_expiry_boundary_is_not_rounded_early(self):
        meta=self.capture(hours=.0000001)
        self.assertIsNotNone(H.read(self.q,self.bid,now=meta['expires_at']-.00000001))
        self.assertFalse(H.collect(self.q,self.bid,now=meta['expires_at']-.00000001))
        self.assertTrue(H.collect(self.q,self.bid,now=meta['expires_at']))

    def test_mismatched_admission_cannot_be_archived(self):
        (self.q/(self.bid+'.json')).write_text(json.dumps({'batch':'other'}))
        with self.assertRaisesRegex(ValueError,'admission id'): self.capture()
        self.assertFalse((self.q/(self.bid+'.closed')).exists())

    def test_existing_archive_cannot_be_overwritten_on_resume(self):
        meta=self.capture()
        record=H.load(self.q/(self.bid+'.closed'))
        changed=copy.deepcopy(record);changed['closed_history']['expires_at']+=1
        H.atomic(self.q/(self.bid+'.json'),changed)
        with self.assertRaisesRegex(ValueError,'overwritten'):H.finish(self.q,self.bid,changed)
        self.assertEqual(H.read(self.q,self.bid,now=100),meta)

    def test_bad_retention_rejected_without_fencing(self):
        for hours in (True,-1,float('inf'),float('nan'),'48'):
            with self.subTest(hours=hours), self.assertRaises(ValueError): self.capture(hours)
        self.assertTrue((self.q/(self.bid+'.json')).exists())

    def test_generation_conflict_never_overwrites_retired_id(self):
        H.atomic(self.q/(self.bid+'.retired'),{'batch':self.bid,'generation':'other'})
        with self.assertRaisesRegex(ValueError,'generation conflict'):self.capture()
        self.assertFalse((self.q/(self.bid+'.closed')).exists())
        self.assertEqual(H.load(self.q/(self.bid+'.retired'))['generation'],'other')


class SchedulerHistory(unittest.TestCase):
    setUp=fixture.OwnedSchedulerTests.setUp
    write_batch=fixture.OwnedSchedulerTests.write_batch
    place_without_thread=fixture.OwnedSchedulerTests.place_without_thread
    good_receipt=fixture.OwnedSchedulerTests.good_receipt

    def test_completed_strict_close_has_read_only_receipt_and_no_replay(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value=self.good_receipt()
        receipt=self.s.cancel(self.b.id)
        self.assertTrue(receipt['cleanup_complete'])
        self.assertNotIn(self.b.id,self.s.batches)
        before=self.vm.client.rpc.call_count
        self.assertEqual(self.s.close_receipt(self.b.id),receipt)
        self.assertTrue(self.s.snapshot(self.b.id)['closed'])
        self.assertEqual(self.s.snapshot(self.b.id)['placements'][0]['identity'],self.it.launch_identity)
        self.assertEqual(self.vm.client.rpc.call_count,before)
        self.assertFalse(self.s.submit(self.msg)['ok'])

    def test_pending_strict_cleanup_has_no_expirable_archive(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value={**self.good_receipt(),'namespace_absent':False}
        self.assertFalse(self.s.cancel(self.b.id,0)['complete'])
        self.assertIn(self.b.id,self.s.batches)
        self.assertFalse((self.root/'queue'/(self.b.id+'.closed')).exists())
        self.assertFalse(H.collect(self.root/'queue',self.b.id,now=time.time()+999999))

    def test_ordinary_close_retains_rows_and_journals_without_reap_claim(self):
        self.b.strict_once=self.b.retain_journals=False
        self.msg['strict_once']=self.msg['retain_journals']=False
        self.write_batch()
        self.it.state='done'
        self.b.rows=[{'name':'item','code':0}]
        self.s.m['batches']=[]
        with patch.object(S.boat,'ledger'):
            self.s.close(self.b.id)
        info=self.s.batch_info(self.b.id)
        self.assertTrue(info['closed'])
        self.assertTrue(info['complete'])
        self.assertFalse(self.s.close_receipt(self.b.id)['cleanup_complete'])
        self.assertEqual(self.s.batch_rows(self.b.id,0)['rows'],self.b.rows)
        self.assertFalse(self.s.submit(self.msg)['ok'])
        self.assertEqual(self.s.close(self.b.id),{})

    def test_reload_finishes_fenced_close_without_publishing_a_batch(self):
        self.b.strict_once=self.b.retain_journals=False
        self.s.m['batches']=[]
        with patch.object(H,'finish',side_effect=OSError('crash')):
            with self.assertRaises(OSError):self.s._retain_closed(self.b,{'cleanup_complete':False})
        self.assertTrue(self.b.closed)
        self.s.batches.clear()
        with patch.object(self.s,'_take_pool'), patch.object(S,'log'):
            self.s.reload()
        self.assertFalse(self.s.recovery_blocked)
        self.assertNotIn(self.b.id,self.s.batches)
        self.assertTrue(self.s.batch_info(self.b.id)['closed'])
        self.assertIn(self.b.id,self.s._client_closed)


if __name__=='__main__':unittest.main()
