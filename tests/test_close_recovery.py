"""Read-only close recovery after lost responses; no provider execution."""
import copy
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import Mock,patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat,config
from test_strict_once_client import session,requests

class Recovery(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        p=patch.object(config,'LIVE',pathlib.Path(self.tmp.name));p.start();self.addCleanup(p.stop)
        self.s=session();self.bid='strict-batch'
        self.s._strict_items={self.bid:{'one'}}
        self.s._strict_placements={}
        self.s._strict_attempted={self.bid}
        self.s.submitted=True;self.s.bids=[self.bid]
        self.s._custody_event(self.bid,'admission_attempt',items=['one'])
        self.placement=dict(name='one',run_name='one',identity='nonce',vm='vm',boot_id='boot',attempt=1)
        self.s._remember_placement(self.bid,self.placement)
        self.snapshot=dict(ok=True,batch=self.bid,strict_once=True,max_attempts=1,placements=[self.placement])
        self.receipt=dict(ok=True,batch=self.bid,complete=True,cleanup_complete=True,strict_once=True,
                          max_attempts=1,admission_closed=True,journals_retained=True,pending=[],unlaunched=[],
                          attempts=[dict(name='one',run_name='one',launch_identity='nonce',vm='vm',boot_id='boot',attempt=1,cleanup_complete=True)])
        self.reads=[]
    def response(self,path,msg,**kw):
        self.reads.append(path)
        if path=='/close-receipt':return copy.deepcopy(self.receipt)
        if path=='/snapshot':return copy.deepcopy(self.snapshot)
        raise AssertionError('mutation or unexpected request: '+path)
    def events(self):
        return [json.loads(x) for x in (config.LIVE/'custody'/(self.bid+'.jsonl')).read_text().splitlines()]

    def test_lost_close_response_and_snapshot_recover_without_second_close(self):
        self.s.client.req.side_effect=RuntimeError('handoff')
        self.assertFalse(self.s.close()['cleanup_complete'])
        self.s.client.req.reset_mock();self.s.client.req.side_effect=self.response
        report=self.s.recover_close(self.bid)
        self.assertTrue(report['cleanup_complete'])
        self.assertTrue(self.s.result['cleanup_complete'])
        self.assertEqual(self.reads,['/close-receipt','/snapshot'])
        self.s.client.push_tree.assert_not_called()
        self.assertIn('close_recovery_complete',[e['event'] for e in self.events()])

    def test_process_resume_uses_original_custody_not_new_controller_names(self):
        self.s.client.req.side_effect=self.response
        with patch.object(boat,'session',side_effect=AssertionError('must not create session')):
            report=boat.recover_close(self.bid,client=self.s.client)
        self.assertTrue(report['cleanup_complete'])
        self.assertEqual(self.reads,['/close-receipt','/snapshot'])

    def test_binding_mismatches_and_pending_cleanup_fail_closed(self):
        for key,value in [('batch','other'),('cleanup_complete',False),('pending',['one']),('unlaunched',['one'])]:
            with self.subTest(key=key):
                before=copy.deepcopy(self.receipt);self.receipt[key]=value
                self.s.client.req.side_effect=self.response
                self.assertFalse(self.s.recover_close(self.bid,attempts=1)['cleanup_complete'])
                self.receipt=before
        for key,value in [('vm','other'),('boot_id','other'),('launch_identity','other'),('attempt',2),('cleanup_complete',False)]:
            with self.subTest(key=key):
                before=copy.deepcopy(self.receipt);self.receipt['attempts'][0][key]=value
                self.assertFalse(self.s.recover_close(self.bid,attempts=1)['cleanup_complete'])
                self.receipt=before

    def test_transient_read_retries_are_bounded_and_journalled(self):
        count=[0]
        def response(path,msg,**kw):
            self.assertGreater(kw['timeout'],0);self.assertLessEqual(kw['timeout'],1)
            count[0]+=1
            if count[0]==1:raise RuntimeError('handoff')
            return self.response(path,msg,**kw)
        self.s.client.req.side_effect=response
        self.assertTrue(self.s.recover_close(self.bid,timeout=1,attempts=3)['cleanup_complete'])
        self.assertEqual(len([e for e in self.events() if e['event']=='close_recovery_attempt']),2)

    def test_deadline_exhaustion_is_explicit_not_a_cleanup_success(self):
        self.s.client.req.side_effect=RuntimeError('offline')
        report=self.s.recover_close(self.bid,timeout=.01,attempts=16)
        self.assertFalse(report['cleanup_complete']);self.assertTrue(report['exhausted'])
        self.assertLess(report['attempts'],16)
        self.assertFalse(requests(self.s,'/close'));self.assertFalse(requests(self.s,'/submit'))

    def test_conflicting_binding_remains_rejected_after_process_resume(self):
        self.s._remember_placement(self.bid,{**self.placement,'boot_id':'other'})
        self.s.client.req.side_effect=self.response
        self.assertFalse(boat.recover_close(self.bid,client=self.s.client,attempts=1)['cleanup_complete'])

    def test_tcp_recovery_has_no_hidden_transport_retry(self):
        from supercomputer import scheduler
        for path in ('/close-receipt','/snapshot'):
            c=object.__new__(scheduler.Client);c.port=1;c.token="test-token";c._conns=[]
            connection=Mock();connection.sock=None
            connection.request.side_effect=ConnectionResetError('handoff')
            with patch('http.client.HTTPConnection',return_value=connection), patch.object(boat.time,'sleep'):
                with self.assertRaises(RuntimeError):
                    c._local(path,b'{"batch":"strict-batch","close_recovery":1}',.1)
            self.assertEqual(connection.request.call_count,1)

    def test_old_or_torn_custody_cannot_invent_original_items(self):
        path=config.LIVE/'custody'/(self.bid+'.jsonl')
        path.write_text(json.dumps(dict(event='admission_attempt',sequence=1))+'\n')
        with self.assertRaisesRegex(ValueError,'original'):boat.recover_close(self.bid,client=self.s.client)
        path.write_text('{"event":"admission_attempt","sequence":1,"items":["one"]}')
        with self.assertRaisesRegex(ValueError,'torn'):boat.recover_close(self.bid,client=self.s.client)
        self.s.client.req.assert_not_called()

if __name__=='__main__':unittest.main()
