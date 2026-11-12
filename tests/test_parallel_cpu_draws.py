"""One stale known resume cannot serialize fresh CPU candidate draws."""
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, patch
from test_cpu_model import MODEL, OTHER, constrained
from test_wiring import S, boat, batch, sched, vm


def setup(n=3):
    s = sched(); s.pol['start_breaker_enabled'] = False
    b = constrained(s, n=n, klass=2)
    for it in b.items.values():it.spec['cpu_model_max_fruitless'] = 64
    c = s._constraint(b.image, b.vm_type, MODEL); c['fruitless'] = 30
    v = vm(s, 'stale-known', ready=False)
    v.cpu_constraint, v.cpu_known_resume = MODEL, True
    c['draws'] = [{'vm': v.id, 't': S.time.time(), 'model': None, 'done': False, 'known_resume': True}]
    return s,b,c


def test_ten_fresh_candidates_in_addition_to_stale_known_resume():
    s,b,c = setup()
    with patch.object(S,'BRINGUP_MAX',10):
        plan=s._plan(S.time.time())
    assert sum(n for n,*_ in plan.values()) == 10,plan
    target=next(iter(s.target.values()))
    assert target['coming'] == 0 and target['fresh_draws'] == 10
    assert c['fruitless'] == 30


def test_shared_distinct_groups_cannot_duplicate_the_fresh_window():
    s,b,c = setup(2)
    for it in b.items.values():it.spec['distinct_vm_group']='R4'
    other=batch(s,'cause',klass=2)
    for it in other.items.values():it.spec.update(cpu_model=MODEL,vm_type=b.vm_type,cpu_model_max_fruitless=64)
    with patch.object(S,'BRINGUP_MAX',10):
        plan=s._plan(S.time.time())
    assert sum(n for n,*_ in plan.values()) == 10,plan


def test_pending_fresh_candidates_consume_window_and_budget_not_stale_known():
    s,b,c = setup()
    for i in range(3):
        v=vm(s,'fresh-'+str(i),ready=False);v.cpu_constraint=MODEL;v.cpu_known_resume=False
        c['draws'].append({'vm':v.id,'t':S.time.time(),'model':None,'done':False})
    with patch.object(S,'BRINGUP_MAX',10):
        assert sum(n for n,*_ in s._plan(S.time.time()).values()) == 7
    c['fruitless']=60
    with patch.object(S,'BRINGUP_MAX',10):
        assert sum(n for n,*_ in s._plan(S.time.time()).values()) == 1


def test_exhausted_stale_known_does_not_refuse_its_waiting_items():
    s,b,c=setup();c['fruitless']=64
    assert not s._plan(S.time.time())
    assert all(it.state=='queued' for it in b.items.values())


def test_physical_max_vms_still_counts_the_known_resume():
    s,b,c=setup()
    with patch.object(S,'BRINGUP_MAX',10),patch.object(S,'MAX_VMS',4):
        assert sum(n for n,*_ in s._plan(S.time.time()).values()) == 3


def test_acquisition_uses_fresh_candidates_not_more_cached_stopped_vms():
    s,b,c=setup()
    c['observed']['cached']=MODEL
    entry={'snapshot':b.image,'type':b.vm_type,'state':'stopped','lease':None}
    entries={'vms':{'cached':entry}}
    s._bring=Mock()
    with patch.object(S,'Registry',return_value=SimpleNamespace(locked=lambda:nullcontext(entries))), \
            patch.object(boat,'limits',return_value={}),patch.object(boat,'starts_left',return_value=20), \
            patch.object(boat,'budget',return_value=20),patch.object(boat,'sandboxes',return_value={}), \
            patch.object(boat,'reconcile'),patch.object(S.threading,'Thread'):
        assert s._acquire(b.image,b.vm_type,10,2,b,model=MODEL)==10
    s._bring.assert_not_called()
    assert entries['vms']['cached']['lease'] is None
    assert s.creating[(b.image,b.vm_type)]==10


def test_mismatch_discard_requires_confirmed_cpu_and_no_work():
    f=S.rules.cpu_discard_candidate
    assert f(MODEL,OTHER,0,0,0)
    for args in [(MODEL,'',0,0,0),(MODEL,MODEL,0,0,0),('',OTHER,0,0,0),
                 (MODEL,OTHER,1,0,0),(MODEL,OTHER,0,1,0),(MODEL,OTHER,0,0,1)]:
        assert not f(*args),args


if __name__=='__main__':
    for name,fn in list(globals().items()):
        if name.startswith('test_'):
            fn();print('ok',name)
