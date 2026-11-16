"""2026-11-16 10:25: 62 VMs held, 46 of them idle, 49 gates, 16 submissions and 1,505 filler items queued, gate wait p95
~1,150 s. Three faults: abandoned retained batches closed on the placement thread (one pass every ~215 s), idle VMs
that could run none of the queued real work held from release by it, and growth groups that differ only in required
agent capabilities sharing one key (the last group's want shown, its starts asked). Run on boat, like all test_*.py."""
import threading
import time

from test_wiring import S, sched, vm
from supercomputer import boat

CAPS = {"output_files_protocol": 1, "result_files_protocol": 1, "runtime_pins_protocol": 1}


def _batch(s, bid, klass, items, t0=None):
    b = S.Batch({"batch": bid, "klass": klass, "image": "img", "tree": {"id": "t", "files": {}, "links": {}}, "ctx_b64": "",
                 "items": items, "t0": t0 or time.time()})
    s.batches[b.id] = b
    return b


def _vm(s, vid, vm_type):
    v = S.VMState(vid, "img", vm_type, "warm")
    v.ready, v.t_ready, v.agent_capabilities = True, time.time(), dict(CAPS)
    s.vms[vid] = v
    return v


def _wide(n, name, required=None, weight=7, memory=28672, est=45):
    return [{"name": f"{name}{i}", "weight": weight, "memory": memory, "vm_type": "xlarge", "est": est,
             **({"required_agent_capabilities": required} if required else {})} for i in range(n)]


def test_abandoned_close_never_blocks_the_placement_thread():
    """A retained batch's close is its cancel, an RPC per item to VMs that may be gone (~50 s a batch live): the
    sweep runs on its own worker, one at a time, and the loop goes on."""
    s = sched()
    old = _batch(s, "20261114-130946-qc9pjc", 2, [{"name": "a"}], t0=time.time() - 10 * S.ORPHAN_S)
    old.t_poll = old.t0
    gate, calls = threading.Event(), []

    def slow_close(bid):
        calls.append(bid)
        gate.wait(5)
        return {}
    s.close = slow_close
    t0 = time.time()
    s._abandon_tick()
    assert time.time() - t0 < 0.5, "the placement thread waited on an abandoned batch's close"
    s._t_abandon = 0
    s._abandon_tick()  # (a sweep still running: no second one beside it)
    time.sleep(0.2)
    assert calls == [old.id], calls
    gate.set()
    for _ in range(50):
        if not s._abandon_busy:
            break
        time.sleep(0.05)
    assert not s._abandon_busy
    s.close = lambda bid: (_ for _ in ()).throw(RuntimeError("cancel RPC failed"))
    s._t_abandon = 0
    s._abandon_tick()  # (a close that raises is logged; the worker ends, the next sweep comes)
    time.sleep(0.2)
    assert not s._abandon_busy


def test_idle_vm_unfit_for_the_queued_real_work_is_released():
    """Gates each asking an xlarge's memory hold no idle large VM (nothing it could run queued): released as with
    nothing queued. A queued item it could run still holds it."""
    s = sched()
    lets = []
    s._let_go = lambda v, **k: lets.append(v.id)
    s.pol["floor"] = 0
    big = _vm(s, "x", "xlarge")
    small = _vm(s, "l", "large")
    for v in (big, small):
        v.idle_since = v.t_real = time.time() - S.HOLD - 10
    wide = _batch(s, "gate", 2, _wide(3, "g"))
    s._release_idle()
    time.sleep(0.1)
    assert lets == ["l"], lets  # (the xlarge, which fits them, stays)
    s2 = sched()
    lets2 = []
    s2._let_go = lambda v, **k: lets2.append(v.id)
    s2.pol["floor"] = 0
    keep = _vm(s2, "l", "large")
    keep.idle_since = keep.t_real = time.time() - S.HOLD - 10
    _batch(s2, "gate", 2, [{"name": "g0", "weight": 2, "memory": 8192}])
    s2._release_idle()
    time.sleep(0.1)
    assert lets2 == [] and "l" in s2.vms, lets2
    assert wide.items["g0"].state == "queued"


def test_capability_groups_neither_hide_nor_drop_each_others_growth():
    """Gate items needing two capabilities and submissions needing three, both xlarge-only, beside idle large and
    default VMs: both groups' numbers shown, their starts added, of the type that fits them."""
    s = sched()
    s.pol["start_breaker_enabled"] = False
    now = time.time()
    req2 = {"output_files_protocol": 1, "result_files_protocol": 1}
    _batch(s, "gate", 2, _wide(46, "g", req2), t0=now - 2000)
    _batch(s, "sub", 3, _wide(40, "s", CAPS, weight=8, memory=30720, est=600), t0=now - 3000)
    for i in range(4):
        _vm(s, f"d{i}", "default")
        _vm(s, f"l{i}", "large")
    plan = S.Scheduler._plan(s, now)
    keys = [k for k in s.target if k.startswith("img/xlarge")]
    assert len(keys) == 2, s.target
    wants = [s.target[k]["want"] for k in keys]
    assert all(w > 0 for w in wants), s.target
    assert set(plan) == {("img", "xlarge")}, plan
    assert plan[("img", "xlarge")][0] == sum(s.target[k]["start"] for k in keys), (plan, s.target)
    assert plan[("img", "xlarge")][1] == 2  # (the gate asks: the most urgent class)
    assert boat.TYPES["xlarge"][1] - boat.MEM_OVERHEAD >= 30720


def _run(s, v, it, since):
    it.state, it.vm, it.t_start = "running", v, time.time() - since
    v.running[(it.batch.id, it.name)] = it


def test_unconfirmed_cleanups_and_whole_vm_items_earn_growth():
    """11:00: of 12 xlarge, 6 held whole by cancelled strict items whose cleanup is unconfirmed (past their estimates,
    counted as ending now) and 6 by 7-slot gates beside a filler item; 10 whole-VM submissions waited 73+ min with
    the xlarge deficit 0. Slots held so are no capacity, and a slot beside a 7-slot item is none for an 8-slot one."""
    s = sched()
    s.pol["start_breaker_enabled"] = False
    now = time.time()
    req2 = {"output_files_protocol": 1, "result_files_protocol": 1}
    gone = _batch(s, "gone", 2, _wide(6, "z", req2, weight=8, memory=30720, est=60), t0=now - 90000)
    gone.admission_closed = True
    gate = _batch(s, "gate", 2, _wide(37, "g", req2, est=300), t0=now - 600)
    fill = _batch(s, "fill", 5, [{"name": f"f{i}"} for i in range(6)])
    gates = iter(list(gate.items.values())[:6])
    for i, (z, f) in enumerate(zip(gone.items.values(), fill.items.values())):
        _run(s, _vm(s, f"z{i}", "xlarge"), z, 80000)
        z.state = "cancel_pending"
        v = _vm(s, f"x{i}", "xlarge")
        _run(s, v, next(gates), 60)
        _run(s, v, f, 60)
    _batch(s, "sub", 3, _wide(10, "s", CAPS, weight=8, memory=30720, est=600), t0=now - 4400)
    plan = S.Scheduler._plan(s, now)
    sub = next(t for k, t in s.target.items() if "runtime_pins_protocol" in k)
    assert sub["deficit_vms"] > 0 and sub["want"] > 0, s.target
    assert plan.get(("img", "xlarge"), (0,))[0] > 0, plan


def test_slots_beside_long_real_work_are_no_whole_vm_capacity():
    """Four xlarge each running a 2-slot item for the whole break-even: their 24 free slots hold no 8-slot item."""
    s = sched()
    s.pol["start_breaker_enabled"] = False
    now = time.time()
    small = _batch(s, "small", 3, [{"name": f"m{i}", "weight": 2, "est": 7200} for i in range(4)], t0=now - 600)
    for i, it in enumerate(small.items.values()):
        _run(s, _vm(s, f"x{i}", "xlarge"), it, 60)
    _batch(s, "sub", 3, _wide(2, "s", CAPS, weight=8, memory=30720, est=750), t0=now - 4400)
    S.Scheduler._plan(s, now)
    sub = next(t for k, t in s.target.items() if k.startswith("img/xlarge"))
    assert sub["deficit_vms"] > 0, s.target


if __name__ == "__main__":
    for name, test in list(globals().items()):
        if name.startswith("test_"):
            test()
            print("ok", name)
