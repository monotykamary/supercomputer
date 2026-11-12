"""Required agent protocols: deterministic native seams, no provider or live writes.

Run: python3 tests/test_agent_capabilities.py
"""
import contextlib
import json
import pathlib
import tempfile
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_wiring import S, boat, batch, sched, vm
from supercomputer import agent as A
from supercomputer.rules.agent_capabilities import (
    agent_capability_satisfies, agent_capabilities_match, agent_upgrade_allowed,
    normalize_agent_capabilities, required_agent_capabilities,
)

CAPS = {"output_files_protocol": 1, "result_files_protocol": 1}


def constrained(s, bid="cap", **kw):
    b = batch(s, bid, **kw)
    for it in b.items.values():
        it.spec["required_agent_capabilities"] = dict(CAPS)
    return b


def capable(s, name="new"):
    v = vm(s, name)
    s._record_agent_ping(v, {"ok": True, "version": "actual-agent", "agent_capabilities": CAPS})
    return v


def fixture():
    s = sched()
    it = next(iter(constrained(s).items.values()))
    v = capable(s)
    s._start_on = lambda *args: None
    s._prepare = Mock()
    s._deliver_owned = lambda b, row: b.rows.append(row)
    v.client = Mock()
    v.client.ping.return_value = {"ok": True, "version": v.agent, "agent_capabilities": dict(CAPS)}
    v.client.rpc.return_value = {"done": []}
    return s, it, v


def test_versions_and_upgrade_rules():
    for needed in range(4):
        for actual in range(4):
            assert agent_capability_satisfies(needed, actual) == (needed == 0 or actual >= needed)
    for running in range(3):
        for starting in range(3):
            for upgrading in (False, True):
                assert agent_upgrade_allowed(running, starting, upgrading) == (running == starting == 0 and not upgrading)
    assert normalize_agent_capabilities({"good": 1, "bad": True, "negative": -1, "zero": 0, "": 1}) == {"good": 1}
    assert not agent_capabilities_match(CAPS, {})
    assert agent_capabilities_match(CAPS, {k: 2 for k in CAPS})


def test_schema_and_client_preflight_before_upload():
    project = SimpleNamespace(images={}, hook=lambda name: lambda it, j: {"name": it["name"], "cmd": it["cmd"]})
    job = {"name": "artifact", "cmd": "true", "required_agent_capabilities": CAPS}
    with patch.object(boat, "project", return_value=project):
        items, order = boat.job_items([job], "", lambda x: x, "large", {})
    assert items[0]["required_agent_capabilities"] == CAPS and order == ["artifact"]
    S.Scheduler._validate_submission({"items": items})
    assert S.Scheduler.capabilities()["required_agent_capabilities_protocol"] == 1
    for bad in (None, [], True, {"a": 0}, {"a": -1}, {"a": True}, {"a": "1"}, {" ": 1}):
        with contextlib.suppress(ValueError):
            required_agent_capabilities({"required_agent_capabilities": bad})
            raise AssertionError(f"invalid capabilities accepted: {bad}")
    session = object.__new__(boat.Scheduled)
    session.client = Mock()
    session.client.req.return_value = {"output_files_protocol": 1}  # old controller lacks placement enforcement
    try:
        session.run(items, lambda *a: None)
    except boat.Unavailable:
        pass
    else:
        raise AssertionError("old controller accepted requirements")
    session.client.push_tree.assert_not_called()
    assert session.client.req.call_args.args[0] == "/capabilities"


def test_old_unknown_agent_refused_even_when_hash_matches():
    s = sched()
    it = next(iter(constrained(s).items.values()))
    old = vm(s, "old")
    old.agent = s.agent_version = "same-hash"
    s._start_on = lambda *args: None
    assert s._slot(it) is None and not s._place(it, old)
    assert it.owned.placements == 0 and it.fate.state == "queued"
    s._record_agent_ping(old, {"ok": True, "version": "same-hash", "agent_capabilities": CAPS})
    assert s._slot(it) is old and s._place(it, old)


def test_unknown_caps_and_absent_field_preserve_legacy():
    s = sched()
    ordinary = next(iter(batch(s).items.values()))
    old = vm(s, "old")
    s._start_on = lambda *args: None
    assert s._slot(ordinary) is old and s._place(ordinary, old)
    assert ordinary.last_placement["agent_capabilities"] == {}
    assert ordinary.last_placement["agent_version"] == ""
    old.client = Mock()
    s._verify_agent_capabilities(ordinary, old)
    old.client.ping.assert_not_called()


def test_shape_memo_distinguishes_all_capability_versions():
    s = sched()
    it = next(iter(constrained(s).items.values()))
    plain = next(iter(batch(s, "plain").items.values()))
    assert S.fit_shape(it) != S.fit_shape(plain)
    a = capable(s)
    first = S.fit_shape(it)
    it.spec["required_agent_capabilities"]["output_files_protocol"] = 2
    assert S.fit_shape(it) != first
    assert not S.vm_fits(S.fit_shape(it), a, set())
    assert S.vm_fits(first, a, set())
    a.upgrading = True
    assert not S.vm_fits(S.fit_shape(plain), a, set())
    assert not s._place(plain, a)


def test_missing_caps_do_not_count_as_capacity_for_growth():
    s = sched()
    constrained(s, n=200, klass=2)
    vm(s, "old")
    assert s._plan(S.time.time()), "legacy capacity hid constrained demand"


def test_upgrade_fence_wins_race_and_releases():
    s = sched()
    v = vm(s, "old")
    v.agent = "old-hash"
    s.agent_version, s.agent_b64 = "new-hash", "new-code"
    s._renew_lease = lambda v: True
    s._start_on = lambda *a: None
    entered, release = threading.Event(), threading.Event()
    upgraded = []

    def ping():
        assert not s.lock._is_owned(), "network called under scheduler lock"
        return {"ok": True, "version": "new-hash" if upgraded else "old-hash",
                **({"agent_capabilities": CAPS} if upgraded else {})}

    def rpc(cmd, *args, **kw):
        assert cmd == "upgrade" and not s.lock._is_owned()
        entered.set()
        assert release.wait(5)
        upgraded.append(True)
        return {"ok": True}

    v.client = SimpleNamespace(ping=ping, rpc=rpc, token="token")
    it = next(iter(batch(s).items.values()))
    with patch.object(S.time, "sleep"), patch.object(boat, "write_agent"), patch.object(boat, "read_agent", return_value={}):
        worker = threading.Thread(target=s._check_agent, args=(v,))
        worker.start()
        assert entered.wait(5)
        try:
            assert v.upgrading
            assert not s._place(it, v) and s._slot(it) is None
            s._check_agent(v)  # second owner must not enter the upgrade
        finally:
            release.set()
            worker.join(5)
    assert not worker.is_alive() and not v.upgrading and s.wake.is_set()
    assert upgraded == [True] and v.agent_capabilities == CAPS and v.agent == "new-hash"
    assert s._place(it, v)


def test_readonly_probe_keeps_capacity_and_admission_wins_before_upgrade():
    for current in (False, True):
        s = sched()
        v = vm(s, 'probe')
        v.agent = 'current' if current else 'legacy'
        s.agent_version, s.agent_b64 = 'current', 'code'
        s._renew_lease = lambda v: True
        s._start_on = lambda *a: None
        entered, release = threading.Event(), threading.Event()
        def ping():
            assert not s.lock._is_owned()
            entered.set()
            assert release.wait(5)
            return {'ok': True, 'version': v.agent, 'agent_capabilities': {}}
        v.client = SimpleNamespace(ping=ping, rpc=Mock(), token='token')
        it = next(iter(batch(s).items.values()))
        worker = threading.Thread(target=s._check_agent, args=(v,))
        worker.start()
        assert entered.wait(5)
        try:
            assert not v.upgrading
            assert s._place(it, v)
        finally:
            release.set()
            worker.join(5)
        assert not worker.is_alive() and not v.upgrading
        v.client.rpc.assert_not_called()


def test_handoff_freeze_wins_during_readonly_probe():
    s = sched()
    v = vm(s, 'freeze-probe')
    v.agent = 'legacy'
    s.agent_version = 'current'
    s._renew_lease = lambda v: True
    entered, release = threading.Event(), threading.Event()
    def ping():
        entered.set()
        assert release.wait(5)
        return {'ok': True, 'version': 'legacy', 'agent_capabilities': CAPS}
    raw = SimpleNamespace(ping=ping, rpc=Mock(), token='token')
    v.client = S.SchedulerAgentClient(raw, s, v)
    worker = threading.Thread(target=s._check_agent, args=(v,))
    worker.start()
    assert entered.wait(5)
    try:
        with s.lock:
            s.handing_off = True
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive() and not v.upgrading
    assert v.agent_capabilities == {}
    raw.rpc.assert_not_called()


def test_admitted_or_pending_or_foreign_work_blocks_upgrade():
    for kind in ("admitted", "starts", "foreign"):
        s = sched()
        v = vm(s, kind)
        v.client = Mock()
        if kind == "admitted":
            s._start_on = lambda *a: None
            assert s._place(next(iter(batch(s).items.values())), v)
        elif kind == "starts":
            v.starts.append({"spec": {}})
        else:
            v.foreign = 1
        s._check_agent(v)
        assert not v.upgrading
        v.client.ping.assert_not_called()
        v.client.rpc.assert_not_called()


def test_upgrade_failure_releases_fence_without_inventing_capabilities():
    s = sched()
    v = vm(s, "old")
    v.agent = "old-hash"
    s.agent_version, s.agent_b64 = "new", "code"
    s._renew_lease = lambda v: True
    v.client = Mock()
    v.client.ping.return_value = {"ok": True, "version": "old-hash"}
    v.client.rpc.side_effect = OSError("ambiguous upgrade")
    s._check_agent(v)
    assert not v.upgrading and v.agent_capabilities == {} and v.t_agent == 0
    assert v.client.rpc.call_count == 1


def test_prelaunch_rechecks_and_refuses_downgraded_or_unreachable_agent():
    for mode in ("missing", "unreachable", "version"):
        s, it, v = fixture()
        assert s._place(it, v)
        if mode == "missing":
            v.client.ping.side_effect = [v.client.ping.return_value, {"ok": True, "version": v.agent}]
        elif mode == "unreachable":
            v.client.ping.side_effect = OSError("unreachable")
        else:
            v.client.ping.return_value["version"] = "changed"
        s._start_on_inner(it, v)
        v.client.rpc.assert_not_called()
        assert it.state == "done" and not v.running
        assert it.batch.rows and not it.batch.rows[0].get("lost")
        assert "payload not launched" in it.batch.rows[0]["error"]


def test_launch_receipt_precedes_payload_and_survives_snapshot():
    s, it, v = fixture()
    s.code = "test"
    (S.DIR / "queue").mkdir(parents=True, exist_ok=True)
    assert s._place(it, v)
    def rpc(*args, **kw):
        rows = [json.loads(x) for x in (S.DIR / "queue" / f"{it.batch.id}.placed").read_text().splitlines()]
        assert rows[-1]["agent_version"] == "actual-agent"
        assert rows[-1]["agent_capabilities"] == CAPS
        return {"done": []}
    v.client.rpc.side_effect = rpc
    s._start_on_inner(it, v)
    assert v.client.ping.call_count == 2 and v.client.rpc.call_count == 1
    snapshot = s._handoff_snapshot()["vms"][0]
    assert snapshot["agent_capabilities"] == CAPS
    assert snapshot["items"][0]["placement"]["agent_capabilities"] == CAPS


def test_real_advertised_output_protocol_creates_private_directory():
    with tempfile.TemporaryDirectory() as d, patch.object(A, "_lease_enabled", return_value=False), \
            patch.object(A, "clean_disk", return_value={}), patch.object(A, "_lease_boot", return_value="boot"), \
            patch.object(A, "lease", return_value={}):
        advertised = A.rpc_dispatch("ping", [])
        assert advertised["agent_capabilities"] == {**CAPS, "runtime_pins_protocol": 1, "resource_limits_protocol": 1}
        j = pathlib.Path(d) / "job"
        j.mkdir()
        (j / "cpus").write_text("[0]")
        args = A.wrap(j, {"env": {"SUPERCOMPUTER_OUTPUT_DIR": "/wrong"}, "limit": 10},
                      {}, j / "work", pathlib.Path(d), "/", ["true"])
        assert [x for x in args if x.startswith("SUPERCOMPUTER_OUTPUT_DIR=")] == ["SUPERCOMPUTER_OUTPUT_DIR=" + str(j / "outputs")]
        assert (j / "outputs").stat().st_mode & 0o777 == 0o700


def test_handoff_refuses_legacy_successor_before_freezing():
    s = sched()
    constrained(s)
    s.code, s.alert = "current", Mock()
    with tempfile.TemporaryDirectory() as d:
        root = pathlib.Path(d)
        for code in ("current", "legacy"):
            (root / "code" / code / "supercomputer").mkdir(parents=True)
        with patch.object(S, "DIR", root), patch.object(S, "prepare_snapshot", return_value={}), \
                patch.object(boat, "states", return_value={}), patch.object(S.subprocess, "Popen") as spawn:
            s.handoff(back_to="legacy")
        spawn.assert_not_called()
    assert "live agent requirements" in s.alert.call_args.args[0]
    assert not getattr(s, "handing_off", False)


def test_capability_jobs_never_request_modal_after_retries():
    s, it, v = fixture()
    for n in range(S.BOAT_TRIES):
        worker = capable(s, f"worker{n}")
        assert s._place(it, worker)
        with s.lock:
            s._boat_failed(it, worker.id, "lost")
    assert it.state == "done" and it.batch.rows
    assert not it.batch.rows[0].get("lost")
    assert "no fallback" in it.batch.rows[0]["error"]


def test_unsupported_build_capability_does_not_grow_but_actual_agent_can_fit():
    s = sched()
    it = next(iter(constrained(s, n=20, klass=2).items.values()))
    for item in it.batch.items.values():
        item.spec["required_agent_capabilities"] = {"output_files_protocol": 2}
    assert not s._plan(S.time.time())
    assert any("capabilities unavailable" in x["why"] for x in s.target.values())
    real = capable(s)
    real.agent_capabilities = {"output_files_protocol": 2}
    assert s._slot(it) is real  # build knowledge never overrules an actual advertisement


def test_outer_fanout_rejects_backend_gpu_and_foreign_image_bypasses():
    from supercomputer import fanout
    project = SimpleNamespace(images={})
    for backend, extra in (("modal", {}), ("runpod", {}), ("auto", {"gpu": True}),
                           ("auto", {"backend": "modal"}), ("auto", {"image": "foreign"})):
        with tempfile.TemporaryDirectory() as out, patch.object(fanout, "project", return_value=project):
            modal = Mock()
            try:
                fanout.run_jobs(None, [{"name": "cap", "cmd": "true", "required_agent_capabilities": CAPS, **extra}],
                                out, "", lambda x: x, modal, backend=backend)
            except ValueError as error:
                assert "no fallback" in str(error)
            else:
                raise AssertionError(f"capability requirement bypassed through {backend}/{extra}")
            modal.assert_not_called()


def test_outer_fanout_three_strikes_never_route_capability_job_to_modal():
    from supercomputer import fanout
    def failed(jobs, *args, on_lost=None, **kwargs):
        for job in jobs:
            on_lost(job["name"], "boat failed it 3 times (last: its row lost on bx_a)")
        return {}, {}
    with tempfile.TemporaryDirectory() as out, patch.object(fanout, "project", return_value=SimpleNamespace(images={})), \
            patch.object(fanout.fallback, "preflight_on", return_value=False), \
            patch.object(boat, "boat_jobs", side_effect=failed), patch.object(fanout.time, "sleep"):
        modal = Mock()
        with contextlib.suppress(SystemExit):
            fanout.run_jobs(None, [{"name": "cap", "cmd": "true", "required_agent_capabilities": CAPS}],
                            out, "", lambda x: x, modal, backend="auto")
        modal.assert_not_called()
        result = json.loads((pathlib.Path(out) / "backends.json").read_text())
        assert result["where"] == {"boat": 1}


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
