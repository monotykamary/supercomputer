"""Economic boundaries and scheduler wiring. Run on boat, like all test_*.py."""
import math
from test_wiring import S, batch, sched, vm
from supercomputer.rules import growth as G


def test_economics():
    assert G.break_even_s(2700, 0.03, 0.072) == 2700
    assert math.isclose(G.break_even_s(2700, 0.07, 0.072), 3500)
    assert math.isclose(G.break_even_s(2700, 0.07, 0.036), 7000)
    assert G.break_even_s(8000, 0.07, 0.036) == 8000
    for args in ((-1, .07, .036), (2700, -.01, .036), (2700, .07, 0), (math.inf, .07, .036)):
        try:
            G.break_even_s(*args)
        except ValueError:
            pass
        else:
            raise AssertionError(args)


def test_rounding_boundaries():
    for capacity in (0, 120, 4000):
        for per_vm in (1, 120, 14000):
            for count in range(20):
                for delta in (-.25, 0, .25):
                    work = max(0, capacity + count * per_vm + delta)
                    n = G.economic_vms(work, capacity, per_vm)
                    assert n >= 0 and n * per_vm <= max(0, work - capacity)
                    assert not G.economic_start(work, capacity + n * per_vm, per_vm)
                    g = G.gate_vms(work, capacity, per_vm)
                    assert not G.gate_start(work, capacity + g * per_vm)
                    assert not g or G.gate_start(work, capacity + (g - 1) * per_vm)


def test_held_and_coming_capacity():
    s = sched()
    b = batch(s, n=200)
    for it in b.items.values():
        it.est = 60
    # One held large VM clears the whole queue during break-even.
    vm(s, "held")
    assert s._plan(S.time.time()) == {}
    s.vms.clear()
    s.creating[(b.image, b.vm_type)] = 1
    assert s._plan(S.time.time()) == {}


def test_horizon_policy_is_retired():
    import json
    S.POLICY.write_text(json.dumps({"horizon_s": 900, "hold_s": 2700}))
    assert "horizon_s" not in S.read_policy()
    assert "horizon_s" not in S.policy_defaults()
    try:
        S.write_policy({"horizon_s": 900})
    except SystemExit:
        pass
    else:
        raise AssertionError("retired knob accepted")
    for prices in ({"vm_hour_dollars": 0}, {"start_dollars": -1}, {"hold_s": -1}):
        try:
            S.write_policy(prices)
        except ValueError:
            pass
        else:
            raise AssertionError(prices)


def test_small_submission_gets_a_service_vm():
    s = sched()
    batch(s)
    plan = s._plan(S.time.time())
    assert sum(n for n, *_ in plan.values()) == 1


def test_gate_deadline_and_bringup_cap():
    s = sched()
    b = batch(s, klass=2, n=816)
    for it in b.items.values():
        it.est = 300
    for i in range(28):
        vm(s, str(i))
    plan = s._plan(S.time.time())
    assert sum(n for n, *_ in plan.values()) == S.BRINGUP_MAX
    target = next(iter(s.target.values()))
    assert target["gate_vms"] >= 40 and target["deficit_vms"] == 0, target
    assert target["why"] == "bring-up cap", target
    pool = S.rules.Pool(1000, 100, 300, 3600)
    assert pool.grow(target["gate_vms"], S.time.time()) == target["gate_vms"]


if __name__ == "__main__":
    for name, test in list(globals().items()):
        if name.startswith("test_"):
            test()
            print("ok", name)
