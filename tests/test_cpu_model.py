"""CPU pinning in the real scheduler seams; no provider access."""
import json
from unittest.mock import patch
from test_wiring import S, boat, batch, sched, vm

MODEL = "AMD Ryzen 9 9950X 16-Core Processor"
OTHER = "AMD EPYC 4585PX 16-Core Processor"


def constrained(s, n=1, klass=3):
    b = batch(s, klass=klass, n=n)
    for it in b.items.values():
        it.spec.update(cpu_model=MODEL, vm_type=boat.VM_TYPE)
    return b


def test_exact_fit_and_last_line_guard():
    s = sched()
    it = next(iter(constrained(s).items.values()))
    a = vm(s, "a")
    s._start_on = lambda *a: None
    for model in ("", OTHER, MODEL.lower(), MODEL + " "):
        a.model = model
        assert s._slot(it) is None
        assert not s._place(it, a)
        assert it.owned.placements == 0 and it.fate.state == "queued"
    a.model = MODEL
    assert s._slot(it) is a
    assert s._place(it, a)
    assert it.last_placement["cpu"] == MODEL


def test_shape_memo_and_preemption_are_model_specific():
    s = sched()
    it = next(iter(constrained(s, klass=2).items.values()))
    plain = next(iter(batch(s, "ordinary").items.values()))
    assert S.fit_shape(it) != S.fit_shape(plain)
    a = vm(s, "wrong")
    a.model = OTHER
    assert not S.vm_fits(S.fit_shape(it), a, set())
    assert S.vm_fits(S.fit_shape(plain), a, set())
    assert not s._preempt_for(it, S.time.time())


def test_unconstrained_fit_and_plan_ignore_model():
    s = sched()
    it = next(iter(batch(s, n=300).items.values()))
    a = vm(s, "a")
    plans, slots = [], []
    for model in ("", OTHER, MODEL):
        a.model = model
        plans.append(s._plan(1000))
        slots.append(s._slot(it))
    assert plans[0] == plans[1] == plans[2]
    assert slots == [a, a, a]


def test_model_report_backfills_placement_and_handoff():
    s = sched()
    it = next(iter(batch(s).items.values()))
    a = vm(s, "a")
    s._start_on = lambda *a: None
    assert s._place(it, a) and it.last_placement["cpu"] == ""
    s._model_report(a, MODEL)
    assert it.last_placement["cpu"] == MODEL
    s.code = "test"
    assert s._handoff_snapshot()["vms"][0]["model"] == MODEL


def test_growth_and_bounded_refusal():
    s = sched()
    b = constrained(s, n=400, klass=2)
    s.pol["cpu_model_max_fruitless"] = 3
    c = s._constraint(b.image, b.vm_type, MODEL)
    vm(s, "ordinary").model = OTHER
    plan = s._plan(S.time.time())
    assert sum(n for n, *_ in plan.values()) == 3, plan
    for i in range(3):
        a = vm(s, "draw" + str(i))
        a.model = OTHER
        c["draws"].append({"vm": a.id, "t": S.time.time(), "model": None, "done": False})
        s._constraint_result(a.id, OTHER)
    assert not s._plan(S.time.time())
    assert all(it.state == "done" for it in b.items.values())
    assert b.rows and b.rows[0]["unplaceable_by_constraint"] and not b.rows[0].get("lost")
    assert b.rows[0]["constraint"]["models"] == {OTHER: 3}
    assert not s.vms["ordinary"].dead  # mismatches follow ordinary lifecycle
    again = sched()
    again.cpu_constraints = again._constraints_load()
    assert next(iter(again.cpu_constraints.values()))["fruitless"] == 3


def test_pending_draws_consume_remaining_guard_and_matching_resets():
    s = sched()
    b = constrained(s, n=400, klass=2)
    s.pol["cpu_model_max_fruitless"] = 2
    c = s._constraint(b.image, b.vm_type, MODEL)
    c["draws"] = [{"vm": "pending", "t": S.time.time(), "model": None, "done": False}]
    assert sum(n for n, *_ in s._plan(S.time.time()).values()) <= 1
    c["fruitless"] = 1
    assert not s._plan(S.time.time())
    a = vm(s, "pending")
    s._model_report(a, MODEL)
    assert c["fruitless"] == 0 and c["draws"][0]["model"] == MODEL
    assert s._constraint_status()[0]["matching_vms"] == [a.id]


def test_per_item_override_can_resume_a_scarce_constraint():
    s = sched()
    b = constrained(s, n=2, klass=2)
    low, high = b.items.values()
    low.spec["cpu_model_max_fruitless"] = 2
    high.spec["cpu_model_max_fruitless"] = 5
    c = s._constraint(b.image, b.vm_type, MODEL)
    for i in range(2):
        c["draws"].append({"vm": str(i), "t": 0, "model": None, "done": False})
        s._constraint_result(str(i), OTHER)
    plan = s._plan(S.time.time())
    assert low.state == "done" and high.state == "queued"
    assert b.rows[0]["constraint"]["limit"] == 2
    assert b.rows[0]["constraint"]["models"] == {OTHER: 2}
    assert sum(n for n, *_ in plan.values()) == 1
    assert s._constraint_status()[0]["limit"] == 5
    for value in (0, -1, True, 1.5, "20"):
        spec = {"name": "a", "cpu_model": MODEL, "vm_type": "xlarge", "cpu_model_max_fruitless": value}
        try:
            S.Scheduler._validate_submission({"items": [spec]})
        except ValueError:
            pass
        else:
            raise AssertionError(value)


def test_native_specs_preserve_constraints_and_refuse_old_schedulers():
    from types import SimpleNamespace
    from unittest.mock import Mock
    project = SimpleNamespace(images={}, hook=lambda name: None)
    job = {"name": "pin", "cmd": "true", "cpu_model": MODEL, "vm_type": "xlarge", "cpu_model_max_fruitless": 64}
    with patch.object(boat, "project", return_value=project):
        its, order = boat.job_items([job], "", lambda cmd: cmd, "large", {})
    assert its[0]["cpu_model"] == MODEL and its[0]["cpu_model_max_fruitless"] == 64
    assert order == ["pin"]
    project.hook = lambda name: lambda it, job: {"name": it["name"], "cmd": it["cmd"]}
    with patch.object(boat, "project", return_value=project):
        rebuilt, _ = boat.job_items([job], "", lambda cmd: cmd, "large", {})
    assert rebuilt[0]["cpu_model"] == MODEL and rebuilt[0]["cpu_model_max_fruitless"] == 64
    session = object.__new__(boat.Scheduled)
    session.client = Mock()
    session.client.req.return_value = {}
    try:
        session.run(its, lambda *a: None)
    except boat.Unavailable:
        pass
    else:
        raise AssertionError("old scheduler accepted CPU constraint")
    session.client.push_tree.assert_not_called()
    assert session.client.req.call_args.args[0] == "/capabilities"


def test_handoff_refuses_pre_constraint_code_before_freezing():
    import pathlib
    import tempfile
    from unittest.mock import Mock
    s = sched()
    constrained(s)
    s.code = "current"
    s.alert = Mock()
    with tempfile.TemporaryDirectory() as d:
        root = pathlib.Path(d)
        for code in ("current", "legacy"):
            (root / "code" / code / "supercomputer").mkdir(parents=True)
        with patch.object(S, "DIR", root), patch.object(S, "prepare_snapshot", return_value={"cpu_model_exact": False}), \
             patch.object(boat, "states", return_value={}), patch.object(S.subprocess, "Popen") as spawn:
            s.handoff(back_to="legacy")
        spawn.assert_not_called()
    assert "live CPU constraints" in s.alert.call_args.args[0]
    assert not getattr(s, "handing_off", False)


def test_schema():
    base = {"batch": "b", "items": [{"name": "a", "cpu_model": MODEL, "vm_type": "xlarge"}]}
    S.Scheduler._validate_submission(base)
    assert S.Scheduler.capabilities()["cpu_model_exact"]
    for value in (None, "", "   ", 123):
        with patch.dict(base["items"][0], cpu_model=value):
            try:
                S.Scheduler._validate_submission(base)
            except ValueError:
                pass
            else:
                raise AssertionError(value)
    del base["items"][0]["vm_type"]
    try:
        S.Scheduler._validate_submission(base)
    except ValueError:
        pass
    else:
        raise AssertionError("CPU model without type accepted")


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
