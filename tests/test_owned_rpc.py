"""Boat-only fence wiring; no real provider or installed daemon calls."""
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
from supercomputer import agent as A, scheduler as S
from test_owned_scheduler import OwnedSchedulerTests as _Fixture
from broken_owned_rpc import broken_owned_rpc_allowed
F = {'pidfd': True, 'private_pid_namespace': 'verified-before-payload'}

class RuleTests(unittest.TestCase):
    def test_shipped_decision_truth_table_and_broken_variant(self):
        for p,v,n,f,u in itertools.product((False,True), repeat=5):
            self.assertIs(A.owned_rpc_allowed(p,v,n,f,u), p and v and n and f and not u)
        self.assertTrue(broken_owned_rpc_allowed(False,False,False,False,True))

class AgentRPC(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=pathlib.Path(self.tmp.name)
        for name,value in [('ROOT',self.root),('_owned_nonces',set()),('_owned_children',{})]:
            p=patch.object(A,name,value);p.start();self.addCleanup(p.stop)
        p=patch.object(A,'owned_features',return_value=F);p.start();self.addCleanup(p.stop)
    def request(self, **kw):
        return dict(protocol=1,version=A.VERSION,nonce=secrets.token_hex(16),command='vtick',args=[{'start':[],'acks':[],'wait':0}],**kw)
    def test_actual_dispatch_rule_guards_body(self):
        with patch.object(A,'vtick',return_value={'done':[]}) as body:
            for field,value in [('protocol',True),('protocol',0),('version','old'),('nonce','bad'),('nonce',None)]:
                r=self.request();r[field]=value
                self.assertFalse(A.owned_rpc(r)['ok'])
            body.assert_not_called()
            r=self.request();reply=A.owned_rpc(r)
            self.assertTrue(reply['ok']);self.assertEqual(reply['nonce'],r['nonce']);self.assertEqual(reply['features'],F)
            self.assertFalse(A.owned_rpc(r)['ok']);self.assertEqual(body.call_count,1)
    def test_replacement_and_missing_features_refuse_before_body(self):
        with patch.object(A,'vtick') as body:
            (self.root/'owned-upgrading').touch()
            self.assertFalse(A.owned_rpc(self.request())['ok'])
            (self.root/'owned-upgrading').unlink()
            with patch.object(A,'owned_features',return_value=None):
                self.assertFalse(A.owned_rpc(self.request())['ok'])
            body.assert_not_called()
    def test_no_legacy_start_on_owned_rpc(self):
        r=self.request();r['args'][0]['start']=[{'batch':'b','spec':{'name':'n'}}]
        with patch.object(A,'vtick') as body:
            self.assertFalse(A.owned_rpc(r)['ok']);body.assert_not_called()
    def test_durable_unknown_owner_blocks_replacement_before_process_calls(self):
        d=self.root/'owned-items'/'b';d.mkdir(parents=True)
        (d/'n.json').write_text(json.dumps({'state':'launched','reaped':False}))
        with patch.object(A,'alive',return_value=False),patch.object(A,'answers',return_value=False),patch.object(A.subprocess,'Popen') as popen,patch.object(A.os,'kill') as kill:
            self.assertEqual(A.start_daemon('8770','token')['error'],'owned_upgrade_blocked')
            popen.assert_not_called();kill.assert_not_called()
        self.assertTrue(A.strict_unresolved())
        (d/'n.json').write_text('{bad')
        self.assertTrue(A.strict_unresolved())
    def test_wrapper_reap_without_positive_namespace_closure_blocks_replacement(self):
        d=self.root/'owned-items'/'b';d.mkdir(parents=True)
        path=d/'n.json'
        birth={'pid':100,'boot':'boot','start_ticks':1}
        closed={'identity':'nonce','state':'launched','reaped':True,'contained':True,
                'init':dict(birth,pidns=42),'supervisor':dict(birth,pid=99)}
        for field,value in [('contained',False),('contained',None),('init',None),('supervisor',None),('init',birth),('identity',None)]:
            row=dict(closed);row[field]=value;path.write_text(json.dumps(row))
            self.assertTrue(A.strict_unresolved(),field)
        path.write_text(json.dumps(closed));self.assertFalse(A.strict_unresolved())
        path.write_text(json.dumps({'identity':'nonce','state':'never_started','reaped':False,'contained':False}))
        self.assertFalse(A.strict_unresolved())

    def test_live_child_blocks_even_when_durable_row_claims_reaped(self):
        A._owned_children[('b','n')]={'p':Mock()}
        self.assertTrue(A.strict_unresolved())

class SchedulerFence(unittest.TestCase):
    setUp=_Fixture.setUp
    write_batch=_Fixture.write_batch
    place_without_thread=_Fixture.place_without_thread
    def reply(self,r,result=None):
        return dict(ok=True,protocol=1,version=self.s.agent_version,nonce=r['nonce'],features=F,result={'done':[], 'running':[]} if result is None else result)
    def test_stale_legacy_verify_fallback_never_prepare_or_tick(self):
        self.place_without_thread();self.vm.t_agent=0
        def old(command,*args,**kwargs):
            if command=='owned_rpc': raise RuntimeError('no command owned_rpc')
            raise AssertionError('legacy fallback: '+command)
        self.vm.client.rpc.side_effect=old
        with patch.object(self.s,'_prepare') as prep,patch.object(self.s,'_repair') as repair,patch.object(self.s,'_boat_up') as boat:
            self.s._start_on(self.it,self.vm)
            prep.assert_not_called();repair.assert_not_called();boat.assert_not_called()
        self.assertEqual(self.it.state,'owned_refused');self.assertEqual(self.it.owned.placements,1)
        self.assertIn((self.b.id,self.it.run_name),self.vm.running)
    def test_reply_binding_rejects_missing_malformed_replayed_wrong_version(self):
        for field,value in [('protocol',True),('version','old'),('nonce','replayed'),('features',{'pidfd':1,'private_pid_namespace':'verified-before-payload'}),('result',None),('ok',1)]:
            def rpc(command,r,**kwargs):
                response=self.reply(r);response[field]=value;return response
            self.vm.client.rpc.side_effect=rpc
            with self.assertRaises(S.OwnedAgentRefused):S.OwnedAgentClient(self.vm.client,self.s.agent_version).rpc('capabilities')
    def test_good_worker_and_restart_between_caps_and_launch_never_ordinary_tick(self):
        self.place_without_thread();commands=[]
        def rpc(command,r,**kwargs):
            self.assertEqual(command,'owned_rpc');commands.append(r['command'])
            if r['command']=='vtick': raise RuntimeError('no command owned_rpc after rollback')
            return self.reply(r)
        self.vm.client.rpc.side_effect=rpc
        with patch.object(self.s,'_prepare'):
            self.s._start_on(self.it,self.vm)
        self.assertEqual(commands,['capabilities','vtick']);self.assertEqual(self.it.state,'owned_refused')
    def test_good_modern_start_uses_owned_rpc_only(self):
        self.place_without_thread();commands=[]
        def rpc(command,r,**kwargs):
            self.assertEqual(command,'owned_rpc');commands.append(r['command']);return self.reply(r)
        self.vm.client.rpc.side_effect=rpc
        with patch.object(self.s,'_prepare'):
            self.s._start_on(self.it,self.vm)
        self.assertEqual(commands,['capabilities','vtick']);self.assertEqual(self.it.state,'running')
    def test_modern_begin_refusal_does_not_use_legacy_verify_fallback(self):
        self.place_without_thread();commands=[]
        def rpc(command,r,**kwargs):
            self.assertEqual(command,'owned_rpc');commands.append(r['command'])
            return self.reply(r,{'ok':False,'version':'old'} if r['command']=='begin' else {})
        self.vm.client.rpc.side_effect=rpc
        self.s._start_on(self.it,self.vm)
        self.assertEqual(commands,['capabilities','begin'])
        self.assertEqual(self.it.state,'owned_refused')
    def test_mixed_ticks_keep_legacy_results_and_never_downgrade_strict(self):
        self.place_without_thread();self.s.shed=False;self.vm.t_agent=S.time.time()
        strict={'batch':self.b.id,'spec':{'name':self.it.run_name,'strict_once':True,'launch_identity':self.it.launch_identity}}
        legacy={'batch':'legacy','spec':{'name':'old'}}
        self.vm.starts=[strict,legacy]
        self.vm.acks=[{'batch':self.b.id,'name':self.it.run_name,'launch_identity':self.it.launch_identity},{'batch':'legacy','name':'done'}]
        oldrow={'batch':'legacy','name':'done','code':0};calls=[]
        def rpc(command,msg,**kwargs):
            calls.append((command,msg))
            if command=='owned_rpc':raise RuntimeError('no command owned_rpc')
            self.assertEqual(command,'vtick');self.assertEqual(msg['start'],[legacy]);self.assertEqual(msg['acks'],[{'batch':'legacy','name':'done'}])
            return {'done':[oldrow]}
        self.vm.client.rpc.side_effect=rpc
        with patch.object(self.s,'_disk'),patch.object(self.s,'_foreign'),patch.object(self.s,'_accept_finished') as accept:
            self.s._tick(self.vm)
            accept.assert_called_once_with(self.vm,oldrow)
        self.assertEqual([x[0] for x in calls],['owned_rpc','vtick'])
        self.assertEqual(self.it.state,'owned_refused');self.assertEqual(len(self.vm.acks),1)

    def test_no_upgrade_or_repair_with_held_strict_placement(self):
        self.place_without_thread()
        with patch.object(S.boat,'exec_json') as provider:
            self.s._check_agent(self.vm);self.assertFalse(self.s._repair(self.vm))
            provider.assert_not_called();self.vm.client.ping.assert_not_called()

if __name__=='__main__':unittest.main()
