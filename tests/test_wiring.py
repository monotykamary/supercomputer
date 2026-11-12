"""scheduler.py's decisions through supercomputer/rules/scheduler.py, on a Scheduler with no threads, no boat and no disk of its own
(a temporary LIVE): today's incidents replayed against the wiring. `python3 tests/test_wiring.py` (no boat call is made:
each is made to fail)."""
import os
import sys
import tempfile
import threading
import time

os.environ["SUPERCOMPUTER_LIVE"] = tempfile.mkdtemp(prefix="sc-wiring-")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from supercomputer import boat, scheduler as S  # noqa: E402


def no_boat(*a, **k):
    raise AssertionError(f"a boat call in a wiring test: {a[:2]}")


for name in ("boat", "bj", "sandboxes", "limits", "stop_vm", "exec_json", "states", "agent_client"):
    setattr(boat, name, no_boat)


def sched() -> S.Scheduler:
    s = object.__new__(S.Scheduler)
    s.batches, s.vms, s.lock, s.wake, s.stop = {}, {}, threading.RLock(), threading.Event(), threading.Event()
    s.hints, s.creating, s.backoff, s.asking = {}, {}, {}, set()
    s.starts_log, s.shed, s.target, s.bring_up = [], False, {}, 60.0
    s.br = {"open_until": 0.0, "backoff": S.BREAK_BACKOFF, "half": False, "t_trip": 0.0, "reason": "", "trips": 0}
    s.m = s._metrics_new("test")
    s.pol = {**S.policy_defaults(), "generation": 3}
    s.held_before, s.est_hist, s.class_obs = {}, {}, {}
    return s


def batch(s, bid="b1", image="img", klass=3, n=1, max_wait=0.0):
    b = S.Batch({"batch": bid, "klass": klass, "image": image, "tree": {"id": "t", "files": {}, "links": {}}, "ctx_b64": "",
                 "items": [{"name": f"i{k}"} for k in range(n)], "on_lost": True, "max_wait": max_wait})
    s.batches[b.id] = b
    return b


def vm(s, vid, image="img", ready=True):
    v = S.VMState(vid, image, boat.VM_TYPE, "warm")
    v.ready, v.t_ready = ready, time.time()
    s.vms[vid] = v
    return v


def test_one_strike_per_vm_and_never_back_there():
    """21:40: one VM boat had stopped reported three ways in one second; one strike, and never placed there again."""
    s = sched()
    b = batch(s)
    it = b.items["i0"]
    a = vm(s, "A")
    s._start_on = lambda it, vm: None
    assert s._place(it, a)
    for why in ("its preparation failed", "boat VM lost", "its row lost"):
        with s.lock:
            s._boat_failed(it, "A", why)
    assert (it.fails, it.state, it.avoid) == (1, "queued", {"A"}), (it.fails, it.state, it.avoid)
    assert not s._place(it, a), "placed again on the VM that failed it"
    assert s._slot(it) is None
    bvm = vm(s, "B")
    assert s._slot(it) is bvm and s._place(it, bvm)
    a.dead = True
    it2 = batch(s, "b2").items["i0"]
    assert not s._place(it2, a), "placed on a VM boat reports stopped"


def test_modal_after_three_vms():
    s = sched()
    it = batch(s).items["i0"]
    s._start_on = lambda it, vm: None
    rows = []
    it.batch.deliver = rows.append
    for vid in ("A", "B", "C"):
        assert s._place(it, vm(s, vid))
        with s.lock:
            s._boat_failed(it, vid, "lost")
    time.sleep(0.1)
    assert it.state == "done" and it.fails == 3 and rows and rows[0]["lost"], (it.state, it.fails, rows)


def test_a_wait_the_scheduler_made_is_no_strike():
    """Before acac7d0 a wait with no VM ready was a strike, the breaker open or not."""
    s = sched()
    s._plan = lambda now: {}
    s._release_idle = lambda: None
    b = batch(s, max_wait=5)
    it = b.items["i0"]
    it.t_queued = time.time() - 60
    s.br["open_until"] = time.time() + 600
    s.schedule()
    assert it.fails == 0 and it.state == "queued"
    s.starts_log.append({"t": it.t_queued + 1, "vm": "X", "ok": False, "image": "img"})
    s.schedule()
    assert it.fails == 1, it.fails


def test_plan_breaker_and_one_probe():
    s = sched()
    batch(s, "g1", image="img1", klass=2, n=40)
    batch(s, "g2", image="img2", klass=2, n=40)
    now = time.time()
    s.br.update(open_until=now + 300, half=True, t_trip=now)
    assert s._plan(now) == {}
    s.br["open_until"] = now - 1
    out = s._plan(now)
    assert sum(n for n, *_ in out.values()) == 1, out  # (one probe, both images wanting)
    s.starts_log.append({"t": now + 1, "vm": "P", "ok": None, "image": "img1"})  # (a probe out)
    assert s._plan(now) == {}
    s.br.update(half=False)
    out = s._plan(now)
    assert sum(n for n, *_ in out.values()) <= S.BRINGUP_MAX, out


def test_release_and_hold():
    """A VM running items is never released; within the floor while the treadmill is alive an idle VM stays (until idle
    for HOLD s); past the hold, the surplus above the floor drains in steps (rules.Drain), never at once."""
    s = sched()
    lets = []
    s._let_go = lambda v, **k: lets.append(v.id)
    s._start_on = lambda it, vm: None
    s.pol["floor"] = 2
    s.hints = {"treadmill": {"image": "img", "until": time.time() + 600}}
    vms = [vm(s, f"v{k}") for k in range(10)]
    old = time.time() - S.HOLD - 5
    for k, v in enumerate(vms):
        v.idle_since = time.time() - 10
        v.t_real = old - k  # (v0, v1 the most recent: the floor)
    fill = batch(s, "f", klass=5, n=10)
    for k, v in enumerate(vms):
        assert s._place(fill.items[f"i{k}"], v)
    s._release_idle()
    time.sleep(0.1)
    draining = [v.id for v in vms if v.draining]
    assert len(draining) == 2 and not lets, (draining, lets)  # (8 above the floor: ceil(25% of 8) = 2 drain in this window)
    s._release_idle()
    assert len([v for v in vms if v.draining]) == 2  # (the window's quota spent: no more until DRAIN_EVERY after the last)
    assert all(v.gen == 3 for v in vms)
    # (the floor holds: idle, within the floor, the treadmill alive: kept; nothing queued, nothing runs)
    s2 = sched()
    lets2 = []
    s2._let_go = lambda v, **k: lets2.append(v.id)
    s2.pol["floor"] = 2
    s2.hints = {"treadmill": {"image": "img", "until": time.time() + 600}}
    a, b = vm(s2, "a"), vm(s2, "b")
    for v in (a, b):
        v.idle_since, v.t_real = time.time() - 10, time.time() - 10
    s2._release_idle()
    assert lets2 == [] and "a" in s2.vms and "b" in s2.vms, lets2
    a.idle_since = a.t_real = time.time() - S.HOLD - 1  # (idle for HOLD, no real work in it either: never parked)
    s2._release_idle()
    time.sleep(0.1)
    assert lets2 == ["a"], lets2


def test_breaker_watchdog_handoff_policy_acquire():
    s = sched()
    s.alert = lambda msg: None
    s._metrics_save = lambda: None
    now = time.time()
    s.starts_log = [{"t": now - 10 - k, "vm": f"v{k}", "ok": False if k < 3 else True, "image": "img"} for k in range(4)]
    s._breaker_eval(now)
    assert s.br["half"] and s.br["open_until"] > now and s.br["backoff"] == min(S.BREAK_MAX, 2 * S.BREAK_BACKOFF), s.br  # (3 of 4 failed: tripped)
    first = s.br["open_until"]
    s.trip("again")
    assert s.br["open_until"] == first  # (a trip while open changes nothing)
    s.shed = False
    import io
    from unittest.mock import patch
    real_open = open
    def fixed_memory(path, *args, **kwargs):
        if path == "/proc/meminfo":
            return io.StringIO("MemTotal: 1000 kB\nMemAvailable: 500 kB\n")
        return real_open(path, *args, **kwargs)
    # This case tests load hysteresis, not the gate host's changing free memory.
    with patch.object(S, 'open', side_effect=fixed_memory, create=True), \
            patch.object(S, 'WATCHDOG', True), patch.object(os, 'getloadavg') as load:
        load.return_value = (1.6 * (os.cpu_count() or 1), 0, 0)
        s._watch()
        assert s.shed
        load.return_value = (1.2 * (os.cpu_count() or 1), 0, 0)
        s._watch()
        assert s.shed  # (hysteresis: not cool yet)
        load.return_value = (0.5 * (os.cpu_count() or 1), 0, 0)
        s._watch()
        assert not s.shed
    inst = S.rules.Install("old", 60)
    assert inst.tick(0, "new", True) == "stay" and inst.tick(30, "new", True) == "stay" and inst.tick(61, "new", False) == "stay" and inst.tick(61, "new", True) == "handoff"
    S.POLICY.parent.mkdir(parents=True, exist_ok=True)
    g = S.read_policy().get("generation", 0)
    assert S.write_policy({"hold_s": 60})["generation"] == g + 1
    try:
        S.write_policy({"no_such": 1})
        raise AssertionError("an unknown knob written")
    except SystemExit:
        pass
    assert S.read_policy()["generation"] == g + 1
    a = S.rules.Acquire()
    assert a.plan(5, 2, 4, 2) == 4 and (a.warm, a.resumes, a.creates) == (2, 2, 0)


def test_done_only_after_every_row_is_read():
    """bar-89160: an item of 0.06 s done, its row not yet delivered (its output fetched first): the client was told
    done and stopped with the row unread. Now done waits for every row, and for the client to have read it."""
    s = sched()
    b = batch(s, "fast", klass=2, n=1)
    it = b.items["i0"]
    it.state = "done"  # (done; its row not delivered yet)
    r = s.poll(b.id, 0, 0.05)
    assert r["rows"] == [] and not r["done"], r
    b.rows.append({"name": "i0", "code": 0})  # (delivered: as Batch.deliver, without the disk)
    r = s.poll(b.id, 0, 0.05)
    assert r["rows"] and r["done"], r
    r = s.poll(b.id, 1, 0.05)
    assert r["done"], r


def test_no_placement_on_a_vm_whose_preparations_keep_failing():
    """bx_5wr3bs76 after the 23:40 handoff: 824 failed preparations, an interactive check placed there again and again."""
    s = sched()
    it = batch(s, "q", klass=1).items["i0"]
    bad, good = vm(s, "bad"), vm(s, "good")
    bad.prep_fails = 3
    good.running[("x", "y")] = batch(s, "f", klass=5).items["i0"]  # (good the fuller: the old choice was bad)
    assert s._slot(it) is good
    good.prep_fails = 3
    assert s._slot(it) is None


def test_a_dropped_tree_is_prepared_again_not_struck():
    """23:40-00:50: rows lost because a VM had dropped the batch's tree, each a strike (153 items to Modal): now the
    batch is prepared again there, the item queued with no strike."""
    s = sched()
    s._start_on = lambda it, vm: None
    b = batch(s, "long", klass=2, n=2)
    v = vm(s, "A")
    v.prepared.add(b.id)
    for name in ("i0", "i1"):
        it = b.items[name]
        assert s._place(it, v)
        s._finished(v, {"batch": b.id, "name": it.run_name, "lost": True, "err": "the batch's tree isn't on box-1"})
    a, c = b.items["i0"], b.items["i1"]
    assert (a.fails, a.state) == (0, "queued") and (c.fails, c.state) == (0, "queued") and b.id not in v.prepared, (a.fails, c.fails)


def test_the_agent_keeps_a_prepared_batchs_tree():
    import pathlib
    from supercomputer import agent as A
    root = pathlib.Path(tempfile.mkdtemp(prefix="sc-agent-"))
    A.ROOT, A.TREES, A.BLOBS = root, root / "trees", root / "blobs"
    (root / "b").mkdir(parents=True)
    for k in range(9):
        t = A.TREES / f"t{k}"
        t.mkdir(parents=True)
        os.utime(t, (time.time() - 1000 + k, time.time() - 1000 + k))
    bd = root / "b" / "old"
    bd.mkdir()
    (bd / "ready").write_text("1")
    (bd / "treepath").write_text(str(A.TREES / "t0"))
    A.gc_trees(n=6)
    left = sorted(p.name for p in A.TREES.iterdir())
    assert "t0" in left and "t1" not in left and len(left) == 7, left


def test_the_share_is_work_conserving():
    """01:07: a submission client held at 32 slots, nothing else waiting, 117 slots on filler: now its share binds only
    while another client of its class or a higher one waits."""
    s = sched()
    s._start_on = lambda it, vm: None
    S.SUB_VMS = 1
    v1, v2 = vm(s, "A"), vm(s, "B")
    b = batch(s, "subs", klass=3, n=2 * v1.slots + 2)
    s._plan = lambda now: {}
    s._release_idle = lambda: None
    s.schedule()
    running = sum(1 for it in b.items.values() if it.state == "starting")
    assert running == v1.slots + v2.slots, running  # (alone: every free slot, past its share of 1 VM)
    other = batch(s, "subs2", klass=3, n=4)
    other.client = "someone-else"
    for it in b.items.values():
        it.state = "queued" if it.state != "starting" else it.state
    S.SUB_VMS = 8


def test_a_stale_failure_leaves_an_item_where_it_is():
    """03:13-03:24 (B13): a failure reported for a VM an item had left (a waiter of its old preparation, a VM lost
    late) queued it while its fate stayed placed on its new VM: never placed again, alerted 10 times."""
    s = sched()
    it = batch(s).items["i0"]
    s._start_on = lambda it, vm: None
    a, b = vm(s, "A"), vm(s, "B")
    assert s._place(it, a)
    with s.lock:
        s._boat_failed(it, "A", "its preparation failed")
    assert it.state == "queued" and it.fate.state == "queued"
    assert s._place(it, b)
    with s.lock:
        s._boat_failed(it, "A", "its preparation failed (late)")
    assert it.state == "starting" and it.fate.state == "placed" and it.vm is b, (it.state, it.fate.state)


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"wiring: {n} of {n}")
