"""11-16 13:44: both successors (907690f5e631, then the saved 0a4ad0999110) refused readiness: the old scheduler held
19 running legacy filler items of three treadmill batches whose admissions had been replaced; reloaded from disk, each
item's verdict was already delivered (its .rows), so the takeover left it out and the item sets differed. A legacy
item whose verdict is delivered carries no ownership: not carried, readiness passes. Run on boat."""
import json
import os
import pathlib
import socket
import tempfile
import time
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_wiring import S, sched


def test_a_delivered_legacy_item_the_old_scheduler_still_held_does_not_refuse_readiness():
    s = sched()
    s.code, s.recovery_errors, s.alerts = "new", {}, []
    s.retired, s._read_client_closes = lambda: set(), lambda: set()
    s._worker = Mock()
    old_pid, host = os.getpid() + 100000, socket.gethostname()
    grant = {"ok": True, "owner": "fixture-owner", "boot": "boot", "epoch": 7, "vm_id": "A"}
    reg = {"vms": {"A": {"snapshot": "img", "type": "large", "state": "running",
                         "lease": {"pid": old_pid, "host": host, "batch": "scheduler"}}}}
    client = SimpleNamespace(rpc=Mock())
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp)
        (d / "queue").mkdir()
        bid = "20261116-131311-a7ht9f"
        (d / "queue" / f"{bid}.json").write_text(json.dumps({
            "batch": bid, "klass": 5, "client": "treadmill", "image": "img", "t0": time.time() - 600,
            "tree": {"id": "t", "files": {}, "links": {}}, "ctx_b64": "",
            "items": [{"name": "kernel-fuzz-1997792", "kind": "sh", "cmd": "true"}]}))
        (d / "queue" / f"{bid}.rows").write_text(json.dumps({"name": "kernel-fuzz-1997792", "code": 0, "seconds": 18.51}) + "\n")
        s.canary = S.canary.Canary(d) if hasattr(S, "canary") else Mock()
        snap = {"from_pid": old_pid, "from": "old", "t": time.time(), "batches": [bid],
                "vms": [{"id": "A", "image": "img", "type": "large", "lease": grant, "items": [
                    {"batch": bid, "name": "kernel-fuzz-1997792", "run_name": "kernel-fuzz-1997792", "state": "running"}]}]}
        (d / "handoff.json").write_text(json.dumps(snap))
        registry = SimpleNamespace(locked=lambda: nullcontext(reg))
        with patch.object(S, "DIR", d), patch.object(S, "Registry", lambda: registry), \
             patch.object(S.boat, "pid_alive", return_value=True), \
             patch.object(S.boat, "owner_id", return_value="fixture-owner"), \
             patch.object(S.boat, "agent_client", return_value=client), \
             patch.object(S.boat, "lease_vm", side_effect=AssertionError("network renewal on handoff")), \
             patch.object(S.boat, "rename_leased_vm", side_effect=AssertionError("rename on handoff")), \
             patch.object(S.threading, "Thread"), \
             patch.object(S.concurrent.futures, "ThreadPoolExecutor", lambda *a, **kw: nullcontext(SimpleNamespace(map=lambda f, xs: map(f, xs)))):
            s.reload()
            assert s.batches[bid].items["kernel-fuzz-1997792"].state == "done"
            s._take_pool()
        ack = json.loads((d / "handoff.ack").read_text())
        assert not s.recovery_blocked, s.recovery
        assert ack["items_ready"] is True, ack
        assert ack["superseded_legacy_items"]["count"] == 1, ack


if __name__ == "__main__":
    test_a_delivered_legacy_item_the_old_scheduler_still_held_does_not_refuse_readiness()
    print("ok superseded takeover")
