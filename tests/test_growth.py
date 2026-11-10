"""Economic boundaries and scheduler wiring. Run on boat, like all test_*.py."""
import math
from test_wiring import S, batch, sched, vm
from supercomputer.rules import growth as G


def test_economics():
    assert G.break_even_s(2700, 0.03, 0.036, 2) == 1500
    assert math.isclose(G.break_even_s(2700, 0.07, 0.036, 2), 3500)
    assert G.break_even_s(2700, 0.03, 0.036) == 3000
    assert G.break_even_s(8000, 0.07, 0.036) == 8000
    assert G.break_even_s(8000, 0.07, 0.036, 2) == 4000
    for args in ((-1, .07, .036), (2700, -.01, .036), (2700, .07, 0), (math.inf, .07, .036), (0, 0, .036)):
        try:
            G.break_even_s(*args)
        except ValueError:
            pass
        else:
            raise AssertionError(args)


def test_gate_window_consumes_elapsed_time():
    assert G.gate_window(900, 120, 300) == 900
    assert G.gate_window(700, 120, 300) == 700
    assert G.gate_window(0, 120, 300) == 420


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
    b = batch(s, n=80)
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
    assert target["gate_vms"] >= 40 and target["gate_vms"] > target["deficit_vms"], target
    assert target["why"] == "bring-up cap", target
    pool = S.rules.Pool(1000, 100, 300, 3600)
    assert pool.grow(target["gate_vms"], S.time.time()) == target["gate_vms"]


def test_gate_capacity_reserves_the_dispatch_quantum():
    from unittest.mock import patch
    s = sched()
    s.bring_up = 120
    b = batch(s, klass=2, n=816)
    for it in b.items.values():
        it.est = 380  # two jobs fit in 780 seconds, but not after a 30-second dispatch reserve
    for i in range(28):
        vm(s, str(i))
    targets = []
    for interval in (0, 30):
        with patch.object(S, 'FULL_S', interval):
            s._plan(S.time.time())
            targets.append(next(iter(s.target.values()))['gate_vms'])
    assert targets[1] > targets[0], targets


def test_ready_gate_capacity_does_not_include_new_worker_bringup():
    s = sched()
    s.bring_up = 120
    now = S.time.time()
    b = batch(s, klass=2, n=150)
    b.t0 = now - S.GATE_HORIZON + 350
    for it in b.items.values():
        it.est = 300
    for i in range(28):
        vm(s, str(i))
    s._plan(now)
    assert next(iter(s.target.values()))['gate_vms'] > 0, s.target


def test_uniform_gate_does_not_count_fractional_future_slots():
    s = sched()
    s.bring_up = 120
    now = S.time.time()
    b = batch(s, bid='waiting', klass=2, n=3)
    b.t0 = now - S.GATE_HORIZON + 250
    for it in b.items.values():
        it.est = 300
    busy = batch(s, bid='running', klass=2, n=2)
    v = vm(s, 'held')
    for it in busy.items.values():
        it.est, it.t_start, it.state, it.vm = 300, now - 150, 'running', v
        v.running[(busy.id, it.name)] = it
    # Two free slots fit two jobs. Each occupied slot has only 150 seconds
    # left in the item-sized tail, not enough for a third 300-second job.
    s._plan(now)
    assert next(iter(s.target.values()))['gate_vms'] == 1, s.target


def test_uniform_gate_keeps_one_job_after_fractional_bringup_subtraction():
    for duration in (300.1, 1000.3, 100.001, 79.99):
        s = sched()
        s.bring_up = 123.45
        now = S.time.time()
        b = batch(s, klass=2, n=4)
        b.t0 = now - S.GATE_HORIZON + 10
        for it in b.items.values():
            it.est = duration
        plan = s._plan(now)
        assert plan and next(iter(s.target.values()))['gate_vms'] >= 1, (duration, s.target)


def test_held_worker_freed_during_boot_beats_a_new_worker():
    s = sched()
    s.bring_up = 120
    now = S.time.time()
    b = batch(s, bid='waiting', klass=2, n=3)
    b.t0 = now - S.GATE_HORIZON + 250
    for it in b.items.values():
        it.est = 300
    busy = batch(s, bid='running', klass=2, n=2)
    v = vm(s, 'held')
    for it in busy.items.values():
        it.est, it.t_start, it.state, it.vm = 300, now - 200, 'running', v
        v.running[(busy.id, it.name)] = it
    # Both occupied slots free before a new VM would boot and can finish a
    # waiting item earlier. Buying another VM cannot improve this deadline.
    assert not s._plan(now), s.target


if __name__ == "__main__":
    for name, test in list(globals().items()):
        if name.startswith("test_"):
            test()
            print("ok", name)
