"""Runtime pin transport and native-only admission; no live provider calls.

Run remotely with: python3 tests/test_runtime_pins_client.py
The worker schema/verifier is covered separately in test_runtime_pins.py.
"""
import contextlib
import copy
import json
import pathlib
import tempfile
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_wiring import S, boat, batch, sched, vm
from supercomputer import agent, fanout, treadmill
from supercomputer.rules.agent_capabilities import required_agent_capabilities

PINS = {"bun": {"version": "1.4.3", "sha256": "ab" * 32},
        "lean": {"version": "4.26.0", "sha256": "cd" * 32}}
CAPS = {"runtime_pins_protocol": 1}


def rejects(call, error=ValueError):
    try:
        call()
    except error:
        return
    raise AssertionError("expected rejection")


def test_schema_derives_requirement_without_mutating_explicit_map():
    for pins in (PINS, {"bun": PINS["bun"]}, {"lean": PINS["lean"]}):
        explicit = {}
        assert required_agent_capabilities({"runtime": pins, "required_agent_capabilities": explicit}) == CAPS
        assert explicit == {}
    assert required_agent_capabilities({}) == {}
    assert required_agent_capabilities({"runtime": PINS, "required_agent_capabilities": {"runtime_pins_protocol": 3}}) == {"runtime_pins_protocol": 3}
    for invalid in (None, {}, [], True, {"node": PINS["bun"]}, {"bun": {}},
                    {"bun": {**PINS["bun"], "url": "x"}},
                    {"bun": {"version": "latest", "sha256": "ab" * 32}},
                    {"bun": {"version": "1.4.3", "sha256": "x" * 64}}):
        rejects(lambda: required_agent_capabilities({"runtime": invalid}))
    upper = {"bun": {"version": "1.4.3", "sha256": "AB" * 32}}
    assert agent.normalize_runtime_pins(upper)["bun"]["sha256"] == "ab" * 32


def test_shell_hook_cannot_drop_runtime_and_item_recovery_validates():
    project = SimpleNamespace(images={}, hook=lambda _: lambda it, j: {"name": it["name"], "cmd": it["cmd"]})
    job = {"name": "pinned", "cmd": "true", "runtime": PINS, "required_agent_capabilities": {}}
    with patch.object(boat, "project", return_value=project):
        items, order = boat.job_items([job], "", lambda x: x, "large", {})
    assert order == ["pinned"] and items[0]["runtime"] == PINS
    assert required_agent_capabilities(items[0]) == CAPS
    msg = {"batch": "pins", "klass": 3, "image": "img", "tree": {"id": "t", "files": {}, "links": {}},
           "ctx_b64": "", "items": items}
    S.Scheduler._validate_submission(msg)
    recovered = S.Batch(json.loads(json.dumps(msg)))
    assert recovered.items["pinned"].spec["runtime"] == PINS
    bad = copy.deepcopy(msg)
    bad["items"][0]["runtime"] = {}
    rejects(lambda: S.Scheduler._validate_submission(bad))
    rejects(lambda: S.Batch(bad))


def test_session_call_preserves_extra_and_legacy_run_rejects():
    session = object.__new__(boat.Session)
    seen = []
    def run(items, done):
        seen.extend(items)
        done(items[0], {"result": "ok", "runtime_verification": {"ok": True}})
    session.run = run
    assert session.call("runner.py", "work", [], extra={"runtime": PINS}) == "ok"
    assert seen[0]["runtime"] == PINS and seen[0]["kind"] == "py"
    legacy = object.__new__(boat.Session)
    rejects(lambda: legacy.run(seen, Mock()), boat.Unavailable)


def test_controller_protocol_required_before_upload_including_strict():
    for strict in (False, True):
        for protocol in (None, False, True, "1", 0, 2):
            session = object.__new__(boat.Scheduled)
            session.strict_once = strict
            session.batch, session.bids = "pins", []
            session._bid_lock = threading.Lock()
            session.client = Mock()
            caps = {"required_agent_capabilities_protocol": 1,
                    "strict_once_protocol": 1, "submit_flags": ["strict_once"]}
            if protocol is not None:
                caps["runtime_pins_protocol"] = protocol
            session.client.req.return_value = caps
            session._request = Mock(return_value=caps)
            session._custody_event = Mock()
            session.custody = {"pins": {}}
            rejects(lambda: session.run([{"name": "p", "runtime": PINS}], Mock()), boat.Unavailable)
            session.client.push_tree.assert_not_called()
            session.client.push_caches.assert_not_called()
            assert not any(c.args[1] == "/submit" for c in session._request.call_args_list)


def test_scheduled_submit_transports_runtime_unchanged():
    class Submitted(Exception):
        pass
    session = object.__new__(boat.Scheduled)
    session.stop = threading.Event()
    session.batch, session.bids = "pins", []
    session._bid_lock = threading.Lock()
    session.client = Mock()
    session.client.req.return_value = {"runtime_pins_protocol": 1, "required_agent_capabilities_protocol": 1}
    session.client.remote = None
    session.tree = SimpleNamespace(root="fixture", id="tree", files={}, links={})
    session.caches = []
    session.proj = SimpleNamespace(cache_outputs=Mock())
    session.klass, session.label, session.image, session.vm_type = 3, "pins", "img", "large"
    session.ctx_b64, session.warm_only = "", False
    captured = []
    def request(bid, endpoint, msg):
        assert endpoint == "/submit"
        session.client.push_tree.assert_called_once()
        captured.append(msg)
        raise Submitted()
    session._request = request
    items = [{"name": "p", "kind": "py", "runtime": PINS}]
    rejects(lambda: session.run(items, Mock()), Submitted)
    assert captured[0]["items"][0]["runtime"] == PINS


def pinned_fixture():
    s = sched()
    b = batch(s, "pins")
    it = b.items["i0"]
    it.spec["runtime"] = copy.deepcopy(PINS)
    v = vm(s, "native")
    s._record_agent_ping(v, {"ok": True, "version": "agent", "agent_capabilities": CAPS})
    s._start_on = lambda *args: None
    s._prepare = Mock()
    s._deliver_owned = lambda b, row: b.rows.append(row)
    v.client = Mock()
    v.client.ping.return_value = {"ok": True, "version": "agent", "agent_capabilities": CAPS}
    v.client.rpc.return_value = {"done": []}
    return s, it, v


def test_unknown_agents_refused_and_pins_reach_launch_after_prepare():
    s, it, v = pinned_fixture()
    v.agent_capabilities = {}
    assert not s._place(it, v)
    v.agent_capabilities = dict(CAPS)
    assert s._place(it, v)
    with tempfile.TemporaryDirectory() as d, patch.object(S, "DIR", pathlib.Path(d)):
        (pathlib.Path(d) / "queue").mkdir()
        s._start_on_inner(it, v)
    s._prepare.assert_called_once()
    assert v.client.ping.call_count == 2
    sent = v.client.rpc.call_args.args[1]["start"][0]["spec"]
    assert sent["runtime"] == PINS
    assert S.BUILD_AGENT_CAPABILITIES["runtime_pins_protocol"] == 1


def test_agent_downgrade_before_start_never_launches():
    s, it, v = pinned_fixture()
    assert s._place(it, v)
    v.client.ping.side_effect = [v.client.ping.return_value, {"ok": True, "version": "agent", "agent_capabilities": {}}]
    s._start_on_inner(it, v)
    v.client.rpc.assert_not_called()
    assert it.state == "done" and it.batch.rows
    assert not it.batch.rows[0].get("lost")


def test_handoff_rejects_generic_capable_runtime_unaware_successor():
    s, it, _ = pinned_fixture()
    s.code, s.alert = "current", Mock()
    for done in (False, True):
        it.state = "done" if done else "queued"
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            for code in ("current", "legacy"):
                (root / "code" / code / "supercomputer").mkdir(parents=True)
            with patch.object(S, "DIR", root), patch.object(S, "prepare_snapshot", return_value={"required_agent_capabilities_protocol": True}), \
                    patch.object(boat, "states", return_value={}), patch.object(S.subprocess, "Popen") as spawn:
                s.handoff(back_to="legacy")
            spawn.assert_not_called()
        assert "runtime-aware" in s.alert.call_args.args[0]
        assert not getattr(s, "handing_off", False)
    assert S.Scheduler.capabilities()["runtime_pins_protocol"] == 1


def test_native_failure_never_becomes_provider_fallback():
    s, it, v = pinned_fixture()
    for n in range(S.BOAT_TRIES):
        worker = vm(s, "native" + str(n))
        worker.agent_capabilities = dict(CAPS)
        assert s._place(it, worker)
        with s.lock:
            s._boat_failed(it, worker.id, "lost")
    assert it.state == "done" and not it.batch.rows[0].get("lost")
    assert "no fallback" in it.batch.rows[0]["error"]


def test_outer_fanout_rejects_unsupported_backends():
    for backend, extra in (("modal", {}), ("runpod", {}), ("auto", {"gpu": True}),
                           ("auto", {"backend": "modal"}), ("auto", {"image": "foreign"})):
        with tempfile.TemporaryDirectory() as d, patch.object(fanout, "project", return_value=SimpleNamespace(images={})):
            modal = Mock()
            rejects(lambda: fanout.run_jobs(None, [{"name": "p", "cmd": "true", "runtime": PINS, **extra}],
                                           d, "", lambda x: x, modal, backend=backend))
            modal.assert_not_called()


def test_outer_fanout_three_strikes_never_drops_runtime_into_modal():
    def failed(jobs, *args, on_lost=None, **kwargs):
        for job in jobs:
            on_lost(job["name"], "boat failed it 3 times (last: lost)")
        return {}, {}
    with tempfile.TemporaryDirectory() as d, patch.object(fanout, "project", return_value=SimpleNamespace(images={})), \
            patch.object(fanout.fallback, "preflight_on", return_value=False), patch.object(boat, "boat_jobs", side_effect=failed), \
            patch.object(fanout.time, "sleep"):
        modal = Mock()
        fanout.run_jobs(None, [{"name": "p", "cmd": "true", "runtime": PINS}], d, "", lambda x: x, modal, backend="auto")
        modal.assert_not_called()
        assert pathlib.Path(d, "summary.tsv").read_text().split("\t")[1] == "-1"


def test_python_runner_transports_and_auto_never_falls_back():
    root = pathlib.Path("/fixture")
    cfg = {"runtime": PINS, "path": ["/toolchain/bin"]}
    session = Mock()
    with patch.object(agent, "import_runner", return_value=SimpleNamespace(BOAT=cfg)), \
            patch.object(fanout, "project", return_value=SimpleNamespace(root=root)), patch.object(boat, "session", return_value=session):
        ex = fanout.BoatEx(str(root / "runner.py"))
    assert ex.extra["runtime"] == PINS
    with patch.object(agent, "import_runner", return_value=SimpleNamespace(BOAT=cfg)), patch.object(fanout, "BoatEx") as make:
        make.return_value.remote.side_effect = RuntimeError("boat failed it 3 times")
        auto = fanout.AutoEx(str(root / "runner.py"), None)
        auto._modal = Mock()
        rejects(lambda: auto.remote(Mock(), 1), RuntimeError)
        auto._modal.assert_not_called()
    with patch.dict(treadmill._boat_cfg, {(root, "runner.py"): cfg}):
        it = treadmill.py_item("p", "runner.py", "fn", [], 10, 1, root)
    assert it["runtime"] == PINS
    rejects(lambda: treadmill.on_modal([it], root, Mock()))


def test_treadmill_exception_path_never_calls_modal_for_pins():
    tm = SimpleNamespace(main=pathlib.Path("/fixture"), max_wait=1, modal=True)
    got = []
    with patch.object(treadmill.threading, "Thread"), patch.object(boat, "session", side_effect=boat.Unavailable("native unavailable")), \
            patch.object(treadmill, "on_modal") as modal:
        b = treadmill.Batch(tm, "pins", "chain", [{"name": "p", "runtime": PINS}], 1, lambda it, row: got.append(row))
        b.go()
        modal.assert_not_called()
    assert b.done and got and got[0]["code"] == -1
    assert not got[0].get("retry")


def test_admission_persists_runtime_and_refuses_name_only_pin_replacement():
    s = sched()
    s._durably_closed = set()
    msg = {"batch": "persist-pins", "klass": 3, "image": "img", "tree": {"id": "t", "files": {}, "links": {}},
           "ctx_b64": "", "items": [{"name": "p", "runtime": PINS}]}
    with tempfile.TemporaryDirectory() as d, patch.object(S, "DIR", pathlib.Path(d)), \
            patch.object(S.blobcache, "locked", return_value=contextlib.nullcontext()):
        assert s.submit(msg)["ok"]
        persisted = json.loads(pathlib.Path(d, "queue", "persist-pins.json").read_text())
        assert persisted["items"][0]["runtime"] == PINS
        assert S.Batch(persisted).items["p"].spec["runtime"] == PINS
        assert s.submit(copy.deepcopy(msg))["kept"]
        changed = copy.deepcopy(msg)
        changed["items"][0]["runtime"]["bun"]["version"] = "1.4.4"
        assert s.submit(changed)["ok"] is False
        changed["items"][0].pop("runtime")
        assert s.submit(changed)["ok"] is False
        assert s.batches[msg["batch"]].items["p"].spec["runtime"] == PINS


def test_result_projection_and_download_retain_runtime_observations():
    from supercomputer import clientresults, resultfiles
    verification = {"bun": {"version": "1.4.3", "sha256": "ab" * 32, "path": "/tool/bin/bun"}}
    row = {"name": "p", "code": 78, "seconds": 0, "runtime_verified": False,
           "runtime_error": "mismatch", "runtime_verification": verification,
           "out": "x" * (resultfiles.INLINE_BYTES * 2)}
    with tempfile.TemporaryDirectory() as d:
        store = resultfiles.Store(pathlib.Path(d) / "source")
        compacted = store.compact_row(row)
        assert "row_ref" in compacted
        for key in ("runtime_verified", "runtime_error", "runtime_verification"):
            assert compacted[key] == row[key]
        downloaded = clientresults.download(compacted, store.read, pathlib.Path(d) / "download")
        assert downloaded["runtime_verification"] == verification
        assert downloaded["runtime_verified"] is False


if __name__ == "__main__":
    tests = [fn for name, fn in sorted(globals().items()) if name.startswith("test_") and callable(fn)]
    for test in tests:
        test()
        print("ok", test.__name__)
    print(f"{len(tests)} runtime pin client checks passed")
