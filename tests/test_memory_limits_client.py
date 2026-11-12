"""Memory constraints survive hooks, transport, worker downgrade and retry."""
import contextlib
import copy
import json
import pathlib
import tempfile
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_wiring import S, boat, batch, sched, vm
from supercomputer import agent, fanout, resultfiles, treadmill
from supercomputer.rules.agent_capabilities import required_agent_capabilities

SPEC={'enforce_memory':True,'memory':64,'memory_limit_mode':'cgroup-v2'}
CAPS={'resource_limits_protocol':1}


def rejects(call,error=ValueError):
    try:call()
    except error:return
    raise AssertionError('expected rejection')


def test_hook_transport_and_saved_spec_preserve_memory_contract():
    project=SimpleNamespace(images={},hook=lambda _:lambda it,j:{'name':it['name'],'cmd':it['cmd']})
    with patch.object(boat,'project',return_value=project):
        items,order=boat.job_items([{'name':'bounded','cmd':'true',**SPEC}],'',lambda x:x,'large',{})
    assert order==['bounded']
    assert all(items[0][k]==v for k,v in SPEC.items())
    assert required_agent_capabilities(items[0])==CAPS
    msg={'batch':'bounded','klass':3,'image':'img','tree':{'id':'t','files':{},'links':{}},'ctx_b64':'','items':items}
    S.Scheduler._validate_submission(msg)
    recovered=S.Batch(json.loads(json.dumps(msg)))
    assert all(recovered.items['bounded'].spec[k]==v for k,v in SPEC.items())
    bad=copy.deepcopy(msg);bad['items'][0]['memory']=True
    rejects(lambda:S.Scheduler._validate_submission(bad))
    rejects(lambda:S.Batch(bad))
    legacy=object.__new__(boat.Session)
    rejects(lambda:legacy.run(items,Mock()),boat.Unavailable)


def test_controller_protocol_required_before_upload():
    for strict in (False,True):
        for protocol in (None,False,True,'1',0,2):
            session=object.__new__(boat.Scheduled)
            session.strict_once=strict;session.batch='bounded';session.bids=[]
            session._bid_lock=threading.Lock();session.client=Mock()
            caps={'required_agent_capabilities_protocol':1,'strict_once_protocol':1,'submit_flags':['strict_once']}
            if protocol is not None:caps['resource_limits_protocol']=protocol
            session.client.req.return_value=caps;session._request=Mock(return_value=caps)
            session._custody_event=Mock();session.custody={'bounded':{}}
            rejects(lambda:session.run([{'name':'job',**SPEC}],Mock()),boat.Unavailable)
            session.client.push_tree.assert_not_called();session.client.push_caches.assert_not_called()
            assert not any(c.args[1]=='/submit' for c in session._request.call_args_list)


def fixture():
    s=sched();b=batch(s,'bounded');it=b.items['i0'];it.spec.update(SPEC)
    v=vm(s,'native');s._record_agent_ping(v,{'ok':True,'version':'agent','agent_capabilities':CAPS})
    s._start_on=lambda *args:None;s._prepare=Mock();s._deliver_owned=lambda b,row:b.rows.append(row)
    v.client=Mock();v.client.ping.return_value={'ok':True,'version':'agent','agent_capabilities':CAPS}
    v.client.rpc.return_value={'done':[]}
    return s,it,v


def test_actual_worker_capability_is_required_and_rechecked_at_launch():
    s,it,v=fixture();v.agent_capabilities={};assert not s._place(it,v)
    v.agent_capabilities=dict(CAPS);assert s._place(it,v)
    with tempfile.TemporaryDirectory() as d,patch.object(S,'DIR',pathlib.Path(d)):
        (pathlib.Path(d)/'queue').mkdir();s._start_on_inner(it,v)
    sent=v.client.rpc.call_args.args[1]['start'][0]['spec']
    assert all(sent[k]==value for k,value in SPEC.items())
    s,it,v=fixture();assert s._place(it,v)
    v.client.ping.side_effect=[v.client.ping.return_value,{'ok':True,'version':'agent','agent_capabilities':{}}]
    s._start_on_inner(it,v);v.client.rpc.assert_not_called()
    assert it.state=='done' and it.batch.rows and not it.batch.rows[0].get('lost')


def test_resource_unaware_handoff_is_refused_for_live_and_done_rows():
    s,it,_=fixture();s.code='current';s.alert=Mock()
    for done in (False,True):
        it.state='done' if done else 'queued'
        with tempfile.TemporaryDirectory() as d:
            root=pathlib.Path(d)
            for code in ('current','legacy'):(root/'code'/code/'supercomputer').mkdir(parents=True)
            with patch.object(S,'DIR',root),patch.object(S,'prepare_snapshot',return_value={'required_agent_capabilities_protocol':True}), \
                 patch.object(boat,'states',return_value={}),patch.object(S.subprocess,'Popen') as spawn:
                s.handoff(back_to='legacy')
            spawn.assert_not_called()
        assert 'resource-aware' in s.alert.call_args.args[0]
        assert not getattr(s,'handing_off',False)
    assert S.Scheduler.capabilities()['resource_limits_protocol']==1


def test_outer_fanout_rejects_non_native_routes():
    for backend,extra in (('modal',{}),('runpod',{}),('auto',{'gpu':True}),('auto',{'backend':'modal'}),('auto',{'image':'foreign'})):
        with tempfile.TemporaryDirectory() as d,patch.object(fanout,'project',return_value=SimpleNamespace(images={})):
            rejects(lambda:fanout.run_jobs(None,[{'name':'job','cmd':'true',**SPEC,**extra}],d,'',lambda x:x,Mock(),backend=backend))


def test_resource_receipt_is_preserved_by_result_projections():
    row={'name':'job','code':42,'resource_verified':True,'resource_limits':{'ok':True,'method':'rlimit-as','effective_bytes':67108864}}
    row['out']='x'*(resultfiles.INLINE_BYTES*2)
    with tempfile.TemporaryDirectory() as d:
        store=resultfiles.Store(pathlib.Path(d)/'source')
        summary=store.compact_row(row)
    assert 'row_ref' in summary
    assert summary['resource_verified'] is True and summary['resource_limits']==row['resource_limits']


def test_same_id_admission_cannot_gain_drop_or_change_a_limit():
    s=sched();s._durably_closed=set()
    msg={'batch':'memory-persist','klass':3,'image':'img','tree':{'id':'t','files':{},'links':{}},
         'ctx_b64':'','items':[{'name':'job',**SPEC}]}
    with tempfile.TemporaryDirectory() as d,patch.object(S,'DIR',pathlib.Path(d)), \
         patch.object(S.blobcache,'locked',return_value=contextlib.nullcontext()):
        assert s.submit(msg)['ok']
        assert s.submit(msg)=={'ok':True,'kept':True}
        for changes in ({'memory':128},{'memory_limit_mode':'rlimit-as'},{'enforce_memory':False}):
            changed=copy.deepcopy(msg);changed['items'][0].update(changes)
            if changes.get('enforce_memory') is False:changed['items'][0].pop('memory_limit_mode')
            assert s.submit(changed)['ok'] is False
        assert s.batches[msg['batch']].items['job'].spec['memory']==64


def test_python_runner_and_treadmill_preserve_caps_and_reserve_memory():
    root=pathlib.Path('/fixture');cfg={**SPEC,'memory':8192}
    with patch.object(agent,'import_runner',return_value=SimpleNamespace(BOAT=cfg)), \
         patch.object(fanout,'project',return_value=SimpleNamespace(root=root)),patch.object(boat,'session',return_value=Mock()):
        ex=fanout.BoatEx(str(root/'runner.py'))
    assert ex.extra['enforce_memory'] is True and ex.weight>=2
    with patch.dict(treadmill._boat_cfg,{(root,'runner.py'):cfg}):
        item=treadmill.py_item('job','runner.py','work',[],10,1,root)
    assert item['weight']>=2 and item['memory']==8192 and item['memory_limit_mode']=='cgroup-v2'
    rejects(lambda:treadmill.on_modal([item],root,Mock()))


if __name__=='__main__':
    for name,fn in list(globals().items()):
        if name.startswith('test_') and callable(fn):fn();print('PASS',name)
