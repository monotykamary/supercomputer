"""treadmill.py's, fanout.py's, runpod.py's, controller.py's and migrate.py's decisions through supercomputer/rules/
(treadmill, fallback, ops), with no boat, RunPod or Modal call (each made to fail) and a temporary LIVE: each bug the
rules fixed replayed. `python3 tests/test_ops_wiring.py`."""
import json
import os
import sys
import tempfile
import time
from unittest.mock import Mock, patch

os.environ["SUPERCOMPUTER_LIVE"] = tempfile.mkdtemp(prefix="sc-ops-wiring-")
os.environ["SUPERCOMPUTER_TREADMILL"] = tempfile.mkdtemp(prefix="sc-ops-tm-")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from supercomputer import boat, config, fanout, migrate, runpod, treadmill  # noqa: E402


def no_call(*a, **k):
    raise AssertionError(f"a provider call in a wiring test: {a[:2]}")


for name in ("boat", "bj", "sandboxes", "limits", "stop_vm", "exec_json", "states", "agent_client"):
    setattr(boat, name, no_call)
runpod.rp = no_call
treadmill.use(treadmill.tm_dir()) if hasattr(treadmill, "use") else None


class FakeBoatEx:
    def __init__(self, outs):
        self.outs, self.closed = list(outs), False

    def remote(self, fn, *args):
        what = self.outs.pop(0)
        if what == "ok":
            return 42
        if what == "own":
            raise fanout.CallFailed("its own exit")
        raise RuntimeError("call: boat lost it: boat failed it 3 times (last: its VM lost)")  # (the scheduler's word for a call it
        # failed on 3 VMs: only that reaches Modal since 248ce2c, rules.fallback.lost_route)

    def close(self):
        self.closed = True
        return {}


class Fn:
    def remote(self, *a):
        return "on modal"


def auto(outs):
    ex = fanout.AutoEx.__new__(fanout.AutoEx)
    ex.app, ex.modal, ex.bx = None, object(), FakeBoatEx(outs)
    ex.where, ex.why, ex.reason = {"boat": 0, "modal": 0}, {}, None
    ex.fb = fanout.fallback.AutoFallback(3)
    fanout.time.sleep = lambda s: None
    return ex


def test_own_failure_is_the_calls():
    """Before: three calls failing by themselves sent the rest of the batch to Modal."""
    ex = auto(["own", "own", "own", "ok"])
    for _ in range(3):
        try:
            ex.remote(Fn())
            raise AssertionError("an own failure not raised")
        except fanout.CallFailed:
            pass
    assert ex.boat_fails == 0 and ex.bx is not None
    assert ex.remote(Fn()) == 42


def test_three_boat_failures_then_modal():
    ex = auto(["lost", "lost", "lost"])
    assert ex.remote(Fn()) == "on modal"
    assert ex.boat_fails == 3 and ex.bx is None and ex.where["modal"] == 1


def test_modal_app_refcount():
    """An app started here stops when its last holder exits; one started elsewhere is never stopped here."""
    fanout.modal_real = lambda: True
    events = []

    class Ctx:
        def __enter__(self):
            events.append("start")

        def __exit__(self, *e):
            events.append("stop")

    class App:
        app_id = None

        def run(self):
            self.app_id = "ap-test"
            return Ctx()
    app = App()
    a, b = fanout.ModalCtx(app), fanout.ModalCtx(app)
    a.__enter__(); b.__enter__()
    a.__exit__(None, None, None)
    assert events == ["start"], events
    b.__exit__(None, None, None)
    assert events == ["start", "stop"], events

    class Elsewhere:
        app_id = "ap-modal-run"

        def run(self):
            raise AssertionError("started again")
    c = fanout.ModalCtx(Elsewhere())
    c.__enter__(); c.__exit__(None, None, None)
    assert events == ["start", "stop"]


def test_cache_first_solve_and_seen():
    c = treadmill.Cache()
    c.put({"commit": "x", "bench": "b", "task": "t1", "config": "c1", "solved": False, "answer": "refused", "t": 1})
    c.put({"commit": "x", "bench": "b", "task": "t1", "config": "c2", "solved": True, "answer": "ok", "t": 2})
    c.put({"commit": "x", "bench": "b", "task": "t1", "config": "c3", "solved": True, "answer": "ok", "t": 3})
    assert c.solved("x", "b") == {"t1": "c2"}
    assert c.seen("x", "b", "c3") == {"t1"} and c.refused("x", "b") == {"t1"}


def test_held_counts_items_not_back_whatever_their_order():
    """Before: the slots still needed summed over items[n_done:], as if rows came back in order."""
    tm = type("TM", (), {"main": None, "modal": False, "max_wait": 0})()
    treadmill.Batch.go = lambda self: None
    b = treadmill.Batch(tm, "t", "a", [{"name": "big", "weight": 3}, {"name": "small", "weight": 1}], 4, lambda it, r: None)
    b.pending.back("big")  # (the big one back first... no: the last one listed back first)
    assert b.pending.need == 1
    b2 = treadmill.Batch(tm, "t", "a", [{"name": "big", "weight": 3}, {"name": "small", "weight": 1}], 4, lambda it, r: None)
    b2.pending.back("small")
    assert b2.pending.need == 3  # (the old count would have said 1: items[1:])


def test_running_submission_left_by_a_dead_treadmill_runs_again():
    d = treadmill.TM / "subs" / "s1"
    d.mkdir(parents=True)
    (d / "meta.json").write_text(json.dumps({"name": "s1", "status": "running", "jobs": 1, "t": time.time()}))
    s = treadmill.Submission(None, d)
    assert s.meta["status"] == "queued" and s.fate.status == "queued"


def test_step_plan_queue_first_pass():
    plan = treadmill.rules.StepPlan(8, 3, 2, 0)
    assert plan.sub_first() == 2 and plan.sub_first() == 0
    assert plan.sub_rest() == 3 and plan.sub_rest() == 0
    assert plan.grow() == 0 and plan.launched == 5


def test_merge_registry_keeps_each_start():
    out = migrate.merge_registry({"starts": [5.0, 5.0, 7.0]}, {"starts": [5.0, 9.0]})
    assert out["starts"] == [5.0, 5.0, 7.0, 9.0], out["starts"]


def test_pod_job_outcomes():
    p = runpod.Pod.__new__(runpod.Pod)
    p.id = "pod"
    p.envs = lambda: ""
    runpod.project = lambda: type("P", (), {"name": "n"})()
    p.sh2 = lambda line, t: (255, "", "Connection closed")
    try:
        p.job({"cmd": "x", "timeout": 100}, "", lambda c: c)
        raise AssertionError("a lost pod taken for the job's exit")
    except RuntimeError as e:
        assert "lost" in str(e)
    p.sh2 = lambda line, t: (1, "out", "")
    assert p.job({"cmd": "x", "timeout": 100}, "", lambda c: c)["code"] == 1


def test_env_precedence():
    os.environ["SUPERCOMPUTER_ZZT"], os.environ["GDPW_BOAT_ZZT"] = "", "old"
    assert config.env("ZZT", "d") == "old"
    os.environ["SUPERCOMPUTER_ZZT"] = "new"
    assert config.env("ZZT", "d") == "new"
    assert config.env("ZZT_NONE", "d") == "d"


def test_handover_failed_or_successful_write_keeps_local_discovery_and_keys():
    from supercomputer import controller
    record = {"snapshot": "img", "type": boat.VM_TYPE, "state": "stopped", "lease": None, "owner": "old-local"}
    with boat.Registry().locked() as d:
        d["vms"] = {"vm1": dict(record)}
    for remote_ok in (False, True):
        with patch.object(boat, "image_name", return_value="img"), \
             patch.object(boat, "read_agent") as read_key, patch.object(boat, "forget_agent") as forget_key, \
             patch.object(controller, "_publish_hints", return_value=remote_ok) as publish:
            controller.handover("controller-fixture", 1)
            publish.assert_called_once_with("controller-fixture", {"vm1": {"snapshot": "img", "type": boat.VM_TYPE, "state": "stopped", "lease": None}})
            assert boat.Registry().read()["vms"]["vm1"] == record
            read_key.assert_not_called()
            forget_key.assert_not_called()


def test_warm_handover_releases_only_accepted_fence_without_exporting_keys():
    from supercomputer import controller
    record = {"snapshot": "img", "type": boat.VM_TYPE, "state": "running", "until": time.time() + 600,
              "lease": None, "owner": "treadmill"}
    fence = {"ok": True, "owner": "fixture-owner", "boot": "fixture-boot", "epoch": 4, "vm_id": "vm1"}
    with boat.Registry().locked() as d:
        d["vms"] = {"vm1": dict(record)}
    for accepted in (False, True):
        with patch.object(boat, "read_agent", return_value={"token": "never-transfer", "lease": fence}), \
             patch.object(boat, "lease_vm", return_value={"ok": accepted, "stale": not accepted}) as release, \
             patch.object(boat, "forget_agent") as forget, \
             patch.object(controller, "_publish_hints", return_value=True) as publish:
            assert controller.handover_warm("controller-fixture") == int(accepted)
            release.assert_called_once_with("vm1", "release", lease=fence)
            if accepted:
                hints = publish.call_args.args[1]
                assert hints["vm1"]["lease"] is None
                assert "token" not in json.dumps(hints) and "owner" not in hints["vm1"]
            else:
                publish.assert_not_called()
            assert boat.Registry().read()["vms"]["vm1"] == record
            forget.assert_not_called()


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"ops wiring: {n} of {n}")
