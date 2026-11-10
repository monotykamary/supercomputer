"""11-08: usable capacity, empty-VM inventory and a 404 resume (no provider calls)."""
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

ROOT = pathlib.Path(os.environ.get("SC_TEST_CODE_ROOT", pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT / "tests"))
from test_wiring import batch, sched, vm
from supercomputer import boat, health, scheduler as S


def test_foreign_slots_are_not_gate_capacity():
    s = sched()
    v = vm(s, "A")
    v.foreign = v.slots
    b = batch(s, klass=2)
    it = b.items["i0"]
    it.timing, it.est = True, 1
    assert s._slot(it) is None
    assert s._plan(time.time()), "one tiny gate must get a VM even when the cached foreign inventory fills every held VM"


def test_exclusive_capacity_is_not_fragmented_slot_time():
    s = sched()
    for i in range(4):
        v = vm(s, str(i))
        lower = batch(s, bid="sub" + str(i), klass=3).items["i0"]
        lower.state, lower.est, lower.t_start = "running", 3600, time.time()
        v.running[(lower.batch.id, lower.name)] = lower
    gate = batch(s, klass=2).items["i0"]
    gate.timing, gate.est = True, 1
    assert s._slot(gate) is None
    assert s._plan(time.time()), "four partial VMs cannot serve one exclusive gate"


def test_idle_foreign_inventory_is_refreshed():
    s = sched()
    v = vm(s, "A")
    v.foreign = v.slots
    v.t_agent = time.time()
    v.lease = {"owner": boat.owner_id(), "vm_id": v.id, "boot": "test-boot", "epoch": 1}
    v.t_renew = time.monotonic()  # This fixture already holds a freshly verified lease.
    v.client = SimpleNamespace(rpc=lambda *a, **k: {"running": [], "done": []}, ping=lambda **k: {})
    s._tick(v)
    assert v.foreign == 0, "empty VMs must keep polling until foreign slots are actually gone"


def test_low_or_exhausted_prepaid_balance_is_not_a_billing_block():
    for plan, pack in [(0, 1000), (0, 0), (12, 1)]:
        lim = {"canStart": True, "billingStatus": "active", "planIncluded": {"vmHoursRemaining": plan}, "packBalanceHours": pack}
        assert health.billing_problems(lim) == [], "balance is information, not a money alert"
    assert health.billing_problems({"canStart": False, "startBlockedReason": "payment"})


def test_resume_404_replaces_without_retry_or_charge():
    # Exercise the real dedicated bring-up; no scheduler fixture or real provider involved.
    s = object.__new__(boat.Session)
    s.batch, s.stop = "dedicated", threading.Event()
    s.counts = {"resumed": 0}
    s.gate = SimpleNamespace(take=lambda *a: 1)
    s.enough = lambda *a: False
    s.mine = lambda *a: True
    s.reg = boat.Registry()
    with s.reg.locked() as d:
        d["vms"] = {"missing": {"type": "large", "snapshot": "img", "state": "stopped", "lease": {"batch": s.batch, "pid": os.getpid(), "host": boat.socket.gethostname()}}}
    calls, releases, created, charged = [], [], [], []
    s._release = lambda ids, **kw: releases.append((ids, kw))
    s._create = lambda n, typ: created.append((n, typ))
    def run(args, **kw):
        calls.append(args)
        return SimpleNamespace(returncode=1, stdout='{"status":404,"code":"sandbox_not_found","error":"Sandbox not found"}', stderr="")
    v = boat.VM("missing", "large", "resumed")
    with patch.object(boat, "states", lambda: {}), patch.object(boat.subprocess, "run", run), \
         patch.object(boat, "boat", lambda *a, **k: ""), patch.object(boat, "wait_running", lambda *a: []), \
         patch.object(boat, "note_starts", lambda n: charged.append(n)):
        s._bring_up(v, False)
    assert len(calls) == 1, "a confirmed missing ID must not be resumed repeatedly"
    assert created == [(1, "large")], "replace a missing dedicated VM immediately"
    assert "missing" not in s.reg.read()["vms"] and not releases
    assert charged == [-1] and s.counts["resumed"] == 0


def test_missing_cleanup_respects_the_exact_lease():
    reg = boat.Registry()
    for lease in [{"batch": "other", "pid": os.getpid(), "host": boat.socket.gethostname()},
                  {"batch": "mine", "pid": os.getpid() + 1, "host": boat.socket.gethostname()},
                  {"batch": "mine", "pid": os.getpid(), "host": "other-host"}]:
        with reg.locked() as d:
            d["vms"] = {"A": {"lease": lease}}
        assert not boat.forget_missing("A", "mine")
        assert reg.read()["vms"]["A"]["lease"] == lease
    assert not boat.resume_missing('{"status":503,"error":"upstream unavailable"}')
    assert not boat.resume_missing('{"status":403,"error":"forbidden"}')


def test_exclusive_gate_reuses_idle_and_filler():
    for filler in [False, True]:
        s = sched()
        v = vm(s, "A")
        if filler:
            low = batch(s, bid="filler", klass=5).items["i0"]
            low.state = "running"
            v.running[(low.batch.id, low.name)] = low
        it = batch(s, klass=2).items["i0"]
        it.timing, it.est = True, 1
        assert not s._plan(time.time()), "do not start beside reusable exclusive capacity"


def test_report_includes_purchased_pack():
    from supercomputer import skill
    lim = {"starts": {"day": {"used": 1, "limit": 1680}}, "planIncluded": {"vmHoursRemaining": 0, "periodEnd": "2026-11-23"}, "packBalanceHours": 1000}
    with patch.object(boat, "limits", lambda: lim):
        assert "1000 prepaid" in skill._limits()


def test_closed_legacy_completion_releases_its_slot():
    s = sched()
    v = vm(s, "A")
    b = batch(s, klass=2)
    it = b.items["i0"]
    it.timing, it.state = True, "done"
    v.running[(b.id, it.run_name)] = it
    b.closed = True
    del s.batches[b.id]
    assert s._accept_finished(v, {"batch": b.id, "name": it.run_name, "code": -15, "seconds": 1})
    assert not v.running and v.acks, "a closed legacy batch is not an owned cancellation barrier"


def test_detached_strict_completion_keeps_its_barrier():
    s = sched()
    v = vm(s, "A")
    b = batch(s, klass=2)
    b.strict_once, b.admission_closed, b.closed = True, True, True
    it = b.items["i0"]
    v.running[(b.id, it.run_name)] = it
    del s.batches[b.id]
    assert not s._accept_finished(v, {"batch": b.id, "name": it.run_name, "code": 0, "seconds": 1})
    assert v.running and not v.acks, "strict identity/reap requirements must not be bypassed"


def test_exclusive_lanes_respect_each_batch_avoidance():
    s = sched()
    vm(s, "A")
    vm(s, "B")
    a, b = batch(s, "a", klass=2), batch(s, "b", klass=2)
    for it in (a.items["i0"], b.items["i0"]):
        it.timing = True
    a.items["i0"].avoid.update(("A", "B"))
    plan = s._plan(time.time())
    assert plan[("img", boat.VM_TYPE)][0] == 1, plan


def test_exclusive_lanes_reassign_reusable_workers_before_starting():
    s = sched()
    vm(s, "A")
    vm(s, "B")
    a, b = batch(s, "a", klass=2), batch(s, "b", klass=2)
    for it in (a.items["i0"], b.items["i0"]):
        it.timing = True
    b.items["i0"].avoid.add("B")
    assert not s._plan(time.time()), "A must serve b; a can reuse B instead of starting"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
