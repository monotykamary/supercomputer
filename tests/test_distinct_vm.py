"""Durable, batch-scoped distinct VM placement in real scheduler seams."""
import json
import pathlib
import tempfile
from unittest.mock import patch, Mock
from test_wiring import S, boat, sched, vm
from test_cpu_model import MODEL, OTHER


def group_batch(s, bid="grouped", n=3, cpu=True):
    msg = {"batch": bid, "klass": 2, "image": "img", "tree": {"id": "t", "files": {}, "links": {}}, "ctx_b64": "",
           "items": [{"name": f"i{i}", "distinct_vm_group": "run/stage/case/rep", "est": 300,
                      **({"cpu_model": MODEL, "vm_type": boat.VM_TYPE} if cpu else {})} for i in range(n)]}
    b = S.Batch(msg)
    s.batches[bid] = b
    return b, msg


def test_concurrent_and_completed_members_never_reuse_a_vm():
    s = sched()
    b, _ = group_batch(s)
    a, c = vm(s, "a"), vm(s, "c")
    a.model, c.model = MODEL, MODEL
    a.lease = {"boot": "boot-a"}
    s._start_on = lambda *args: None
    first, second, third = b.items.values()
    with tempfile.TemporaryDirectory() as d, patch.object(S, "DIR", pathlib.Path(d)):
        assert s._place(first, a)
        assert first.last_placement["distinct_vm_group"] == "run/stage/case/rep"
        assert first.last_placement["boot_id"] == "boot-a"
        assert not s._place(second, a)
        first.state = "done"
        a.running.clear()
        assert not s._place(second, a), "completed members must still exclude their VM"
        assert s._slot(second) is c and s._place(second, c)
        assert s._slot(third) is None
        assert s._plan(S.time.time()), "unmet group demand should grow, not spin beside used VMs"
        assert b.distinct_vms["run/stage/case/rep"] == {"a", "c"}
        assert not s._preempt_for(third, S.time.time())
        other, _ = group_batch(s, "other-batch")
        assert S.vm_fits(S.fit_shape(other.items["i0"]), a, set()), "groups are batch scoped"


def test_durable_history_contains_completed_placements_and_snapshot():
    s = sched()
    b, msg = group_batch(s, n=2)
    a = vm(s, "a")
    a.model = MODEL
    s._start_on = lambda *args: None
    with tempfile.TemporaryDirectory() as d, patch.object(S, "DIR", pathlib.Path(d)):
        assert s._place(b.items["i0"], a)
        qd = pathlib.Path(d) / "queue"
        (qd / f"{b.id}.json").write_text(json.dumps(msg))
        (qd / f"{b.id}.rows").write_text(json.dumps({"name": "i0", "code": 0}) + "\n")
        resumed = sched()
        resumed._take_pool = lambda: None
        resumed._reattach = lambda: None
        resumed.reload()
        rb = resumed.batches[b.id]
        assert rb.items["i0"].state == "done"
        assert rb.distinct_vms == b.distinct_vms
        resumed.code = "test"
        expected = {b.id: {"run/stage/case/rep": ["a"]}}
        assert resumed._handoff_snapshot()["distinct_vm_groups"] == expected
        vm(resumed, "a").model = MODEL
        assert resumed._slot(rb.items["i1"]) is None
        assert not S.rules.distinct_history_matches(True, False), "ACK mismatch must fail closed"


def test_failed_write_ahead_never_launches_or_consumes_a_vm():
    s = sched()
    b, _ = group_batch(s)
    a = vm(s, "a")
    a.model = MODEL
    s._start_on = Mock()
    def persist(bid, suffix, row):
        if suffix == "placed":
            raise OSError("disk full")
    with patch.object(s, "_append_owned", side_effect=persist):
        assert not s._place(b.items["i0"], a)
    assert not b.distinct_vms and not a.running
    s._start_on.assert_not_called()


def test_group_shape_and_absent_field_noninterference():
    s = sched()
    b, _ = group_batch(s, cpu=False)
    a = vm(s, "a")
    first, second, _ = b.items.values()
    shape = S.fit_shape(second)
    b.distinct_vms["run/stage/case/rep"] = {"a"}
    assert shape != S.fit_shape(second)
    assert not S.vm_fits(S.fit_shape(second), a, set())
    del first.spec["distinct_vm_group"]
    assert S.vm_fits(S.fit_shape(first), a, set())
    before = s._plan(S.time.time())
    b.distinct_vms["other"] = {"a"}
    assert before == s._plan(S.time.time())


def test_native_group_schema_and_old_scheduler_refusal():
    from types import SimpleNamespace
    job = {"name": "one", "cmd": "true", "distinct_vm_group": "unique"}
    project = SimpleNamespace(images={}, hook=lambda name: lambda it, job: {"name": it["name"], "cmd": it["cmd"]})
    with patch.object(boat, "project", return_value=project):
        items, _ = boat.job_items([job], "", lambda cmd: cmd, "large", {})
    assert items[0]["distinct_vm_group"] == "unique"
    session = object.__new__(boat.Scheduled)
    session.client = Mock()
    session.client.req.return_value = {"cpu_model_exact": True}
    try:
        session.run(items, lambda *args: None)
    except boat.Unavailable:
        pass
    else:
        raise AssertionError("old scheduler accepted a distinct VM group")
    session.client.push_tree.assert_not_called()
    valid = {"items": [{"name": "a", "distinct_vm_group": "unique"}]}
    try:
        S.Scheduler._validate_submission(valid)
    except ValueError as e:
        assert "placement_contract" in str(e)
    else:
        raise AssertionError("old client accepted without an explicit placement contract")
    valid["placement_contract"] = 1
    S.Scheduler._validate_submission(valid)
    for group in (None, "", "   ", 1):
        try:
            S.Scheduler._validate_submission({"items": [{"name": "a", "distinct_vm_group": group}]})
        except ValueError:
            pass
        else:
            raise AssertionError(group)


def test_capabilities_cli_is_read_only_and_returns_deployed_response():
    import io
    import contextlib
    client = Mock()
    deployed = S.Scheduler.capabilities()
    client.req.return_value = deployed
    with patch.object(S, "controller_configured", return_value=False), patch.object(S, "current", return_value=client), \
         patch.object(S, "ensure_running") as spawn, contextlib.redirect_stdout(io.StringIO()) as output:
        S.main(["capabilities"])
    assert json.loads(output.getvalue()) == deployed
    assert deployed["cpu_model_exact"] is True and deployed["distinct_vm_group"] is True
    assert deployed["distinct_vm_group_protocol"] == 1
    client.req.assert_called_once_with("/capabilities", {})
    spawn.assert_not_called()


def test_handoff_refuses_group_unaware_code_before_freeze():
    s = sched()
    group_batch(s, cpu=False)
    s.code, s.alert = "current", Mock()
    with tempfile.TemporaryDirectory() as d:
        root = pathlib.Path(d)
        for code in ("current", "legacy"):
            (root / "code" / code / "supercomputer").mkdir(parents=True)
        with patch.object(S, "DIR", root), patch.object(S, "prepare_snapshot", return_value={"cpu_model_exact": True}), \
             patch.object(boat, "states", return_value={}), patch.object(S.subprocess, "Popen") as spawn:
            s.handoff(back_to="legacy")
        spawn.assert_not_called()
    assert "distinct VM groups" in s.alert.call_args.args[0]


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
