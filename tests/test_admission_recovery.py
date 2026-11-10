"""11-08: legacy placement journals and fail-closed admission ownership; no provider calls."""
import json
import os
import pathlib
import sys
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock, patch
from contextlib import nullcontext
import socket
import time

ROOT = pathlib.Path(os.environ.get("SC_TEST_CODE_ROOT", pathlib.Path(__file__).resolve().parents[1])).resolve()
sys.path.insert(0, str(ROOT / "tests"))
from test_wiring import batch, sched, vm
from supercomputer import scheduler as S, canary


def message(b):
    return {"batch": b.id, "klass": b.klass, "image": b.image, "tree": b.tree,
            "ctx_b64": b.ctx_b64, "items": [it.spec for it in b.items.values()]}


def test_legacy_null_placements_do_not_discard_admission():
    s = sched()
    b = batch(s, bid="legacy", klass=2, n=3)
    s.batches.clear()
    s._take_pool = lambda: None
    s._reattach = lambda: None
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        q = d / "queue"
        q.mkdir()
        (q / "legacy.json").write_text(json.dumps(message(b)))
        (q / "legacy.placed").write_text('null\n' * 5 + json.dumps({"name": "i0", "run_name": "i0", "vm": "A", "image": "img"}) + '\n')
        with patch.object(S, "DIR", d):
            s.reload()
    assert "legacy" in s.batches, "057241c silently discards a legacy batch when rollback wrote null placements"
    assert s.batches["legacy"].items["i0"].reattach["vm"] == "A"
    assert s.batches["legacy"].left() == 3


def test_resumed_legacy_start_never_writes_null():
    _check_resumed_legacy_placement(None)
    _check_resumed_legacy_placement({"name": "i0", "run_name": "old-run", "vm": "old-vm"})


def _check_resumed_legacy_placement(prior):
    s = sched()
    b = batch(s)
    v = vm(s, "A")
    it = b.items["i0"]
    it.state, it.vm, it.run_name = "starting", v, "i0"
    it.last_placement = prior
    v.running[(b.id, it.run_name)] = it
    s._prepare = lambda *a: None
    v.client = SimpleNamespace(rpc=lambda *a, **k: {})
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        (d / "queue").mkdir()
        with patch.object(S, "DIR", d):
            s._start_on_inner(it, v)
        rows = [json.loads(x) for x in (d / "queue" / f"{b.id}.placed").read_text().splitlines()]
    assert rows and isinstance(rows[0], dict), "a resumed starting item must not persist JSON null"
    assert rows[0]["name"] == it.name and rows[0]["run_name"] == it.run_name and rows[0]["vm"] == v.id


def test_unclassified_admission_is_owned_and_inert():
    for strict, contents in ((False, '[{"name":"i0"}]\n'), (True, 'null\n')):
        s = sched()
        b = batch(s, bid="unreadable")
        v = vm(s, "A")
        v.client = SimpleNamespace(rpc=Mock())
        s.batches.clear()
        s._take_pool, s._reattach = Mock(), Mock()
        with tempfile.TemporaryDirectory() as tmp:
            d = pathlib.Path(tmp)
            q = d / "queue"
            q.mkdir()
            (q / "unreadable.json").write_text(json.dumps({**message(b), "strict_once": strict}))
            (q / "unreadable.placed").write_text(contents)
            with patch.object(S, "DIR", d):
                s.reload()
                assert s.recovery_blocked and "unreadable" in s.recovery_errors
                assert s.snapshot("unreadable")["owned"] is True
                assert s.poll("unreadable", 0, 0)["done"] is False
                s._kill_run(v, "unreadable", "i0")
                assert not s._accept_finished(v, {"batch": "unreadable", "name": "i0"})
                s._let_go(v)
                s._release_idle()
                s.schedule()
                assert not s.close("unreadable")["ok"]
                assert not s.submit(message(b))["ok"]
                assert (q / "unreadable.placed").read_text() == contents
                s._reattach.assert_not_called()
                v.client.rpc.assert_not_called()


def test_unknown_durable_admission_is_never_stale_work():
    s = sched()
    v = vm(s, "A")
    v.lease = {"ok": True, "owner": "fixture-owner", "holder": "fixture-owner", "boot": "fixture-boot", "epoch": 1, "vm_id": "A"}
    v.client = SimpleNamespace(rpc=Mock())
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        (d / "queue").mkdir()
        (d / "queue" / "unreadable.json").write_text('{')
        with patch.object(S, "DIR", d):
            s._foreign(v, {"running": [{"batch": "unreadable", "name": "i0"}]})
            s._kill_run(v, "unreadable", "i0")
            assert not s._accept_finished(v, {"batch": "unreadable", "name": "i0"})
        assert v.foreign >= v.slots and not v.acks
        v.client.rpc.assert_not_called()


def test_native_canary_checks_identity_not_just_count():
    with tempfile.TemporaryDirectory() as tmp:
        c = canary.Canary(pathlib.Path(tmp))
        for expected, loaded, closed, errors, ready in (
            ({"a", "b"}, {"a"}, set(), {}, False),
            ({"a"}, {"b"}, set(), {}, False),
            ({"a"}, {"a", "unexpected"}, set(), {}, False),
            ({"a"}, {"a"}, set(), {"a": "unknown"}, False),
            ({"a", "closed"}, {"a"}, {"closed"}, {}, True),
        ):
            receipt, _, _ = c.admissions("new", expected, loaded, closed, errors)
            assert receipt["ready"] is ready
            assert c.read()["admissions"] == receipt


def test_native_canary_rejects_before_worker_or_release():
    s = sched()
    s.code, s.recovery_errors = "new", {"missing": "unknown schema"}
    s.alerts = []
    s.retired = lambda: set()
    s._read_client_closes = lambda: set()
    s._worker, s._start_on, s.handoff = Mock(), Mock(), Mock()
    old_pid = os.getpid() + 100000
    host = socket.gethostname()
    reg = {"vms": {"A": {"snapshot": "img", "type": "large", "state": "running",
                           "lease": {"pid": old_pid, "host": host, "batch": "scheduler"}}}}
    grant = {"ok": True, "owner": "fixture-owner", "holder": "fixture-owner", "boot": "fixture-boot", "epoch": 7, "vm_id": "A"}
    client = SimpleNamespace(rpc=Mock())
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        old = d / "code" / "old" / "supercomputer"
        old.mkdir(parents=True)
        (old / "scheduler.py").write_text('def _take_pool(self): pass\n')
        s.canary = canary.Canary(d)
        s.canary.handing_off("old", "new", time.time())
        snap = {"from_pid": old_pid, "from": "old", "t": time.time(), "batches": ["missing"],
                "vms": [{"id": "A", "image": "img", "type": "large", "lease": grant, "items": [
                    {"batch": "missing", "name": "i0", "run_name": "i0", "state": "running"}]}]}
        (d / "handoff.json").write_text(json.dumps(snap))
        registry = SimpleNamespace(locked=lambda: nullcontext(reg))
        with patch.object(S, "DIR", d), patch.object(S, "Registry", lambda: registry), \
             patch.object(S.boat, "pid_alive", lambda *a: True), \
             patch.object(S.boat, "agent_client", lambda *a: client), \
             patch.object(S.boat, "lease_vm", return_value=grant) as renew, \
             patch.object(S.boat, "remember_lease") as remember, \
             patch.object(S.boat, "rename_leased_vm", return_value=True) as rename, \
             patch.object(S.concurrent.futures, "ThreadPoolExecutor", lambda *a, **kw: nullcontext(SimpleNamespace(map=lambda f, xs: map(f, xs)))), \
             patch.object(S.threading, "Thread"):
            s._take_pool()
            assert s.recovery_blocked and s.recovery["missing"] == ["missing"]
            assert reg["vms"]["A"]["lease"]["pid"] == os.getpid()
            assert "A" in s.vms and not s.vms["A"].dead
            assert s.vms["A"].lease == grant and client.lease == grant
            renew.assert_called_once_with("A", "renew", lease=grant)
            remember.assert_called_once_with("A", grant)
            rename.assert_called_once_with("A", "img", grant)
            ack = json.loads((d / "handoff.ack").read_text())
            assert ack["taken"] == ["A"] and not ack["admissions"]["ready"]
            s._worker.assert_not_called()
            s._start_on.assert_not_called()
            assert s.poll("missing", 0, 0)["done"] is False
            s._kill_run(s.vms["A"], "missing", "i0")
            s._let_go(s.vms["A"])
            client.rpc.assert_not_called()
            assert s.canary.read()["state"] == "rejected"
            s._handoff_check(time.time())
            s.handoff.assert_called_once_with(back_to="old")


def test_blocked_recovery_can_never_pass_the_ten_minute_canary():
    with tempfile.TemporaryDirectory() as tmp:
        c = canary.Canary(pathlib.Path(tmp))
        c.write({"from": "old", "to": "new", "state": "watching", "t": 0,
                 "admissions": {"ready": False}})
        result, why = c.tick("new", 1000, None)
        assert result == "watch" and "cannot pass" in why
        assert c.read()["state"] == "watching"


def test_manifest_keeps_queued_only_batches():
    s = sched()
    s.code = "old"
    batch(s, bid="queued-only")
    assert s._handoff_snapshot()["batches"] == ["queued-only"]


def test_client_close_ledger_does_not_hide_a_new_or_unclassified_admission():
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        (d / "queue").mkdir()
        ledger = d / "ledger.jsonl"
        ledger.write_text('\n'.join(json.dumps(r) for r in (
            {"batch": "closed", "scheduled": True, "wall_s": 3, "slot_s": 0},
            {"batch": "reused", "scheduled": True, "wall_s": 3, "slot_s": 0},
            {"batch": "not-a-close", "scheduled": True},
        )) + '\n{')
        (d / "queue" / "reused.json").write_text('{')
        with patch.object(S, "DIR", d), patch.object(S.boat, "LEDGER", ledger):
            assert S.Scheduler._read_client_closes() == {"closed"}

if __name__ == "__main__":
    for name, fn in sorted(list(globals().items())):
        if name.startswith("test_") and callable(fn) and (len(sys.argv) == 1 or name in sys.argv[1:]):
            fn()
            print("ok", name)
