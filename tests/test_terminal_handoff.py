"""Real reload/takeover regression: a terminal verdict can still own cleanup.

No provider access. These tests exercise the actual placement/result journals,
passive successor, identity gate and close path, not a simulated replacement rule.
"""
import contextlib
import copy
import json
import os
import pathlib
import socket
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S, canary
from test_wiring import sched
import test_owned_scheduler as fixture


class TerminalHandoff(unittest.TestCase):
    setUp = fixture.OwnedSchedulerTests.setUp
    write_batch = fixture.OwnedSchedulerTests.write_batch
    place_without_thread = fixture.OwnedSchedulerTests.place_without_thread
    good_receipt = fixture.OwnedSchedulerTests.good_receipt

    def terminal(self, status="LOST"):
        self.place_without_thread()
        if status == "LOST":
            self.s._owned_failed(self.it, "fixture: reply lost; cleanup remains owned")
        else:
            self.s._deliver_owned(self.b, {"name": self.it.name, "status": status,
                                          "code": 0, "seconds": 1})
            self.it.state = "done"
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        self.assertFalse(self.it.owned.reaped)

    def snapshot(self):
        return {"from_pid": os.getpid() + 100000, "from": "parent", "t": S.time.time(),
                "batches": [self.b.id], "admission_guard": True,
                "vms": [{"id": self.vm.id, "image": self.vm.image, "type": self.vm.type,
                         "slots": self.vm.slots, "lease": dict(self.vm.lease),
                         "items": [{"batch": self.b.id, "name": self.it.name,
                                    "run_name": self.it.run_name, "state": self.it.state,
                                    "placement": copy.deepcopy(self.it.last_placement),
                                    "launch_identity": self.it.launch_identity,
                                    "placed": self.it.owned.placements}]}]}

    def candidate(self, snap=None):
        snap = snap or self.snapshot()
        (self.root / "handoff.json").write_text(json.dumps(snap))
        child = sched()
        child.code, child.agent_b64 = "child", ""
        child.canary = canary.Canary(self.root)
        child.retired = lambda: set()
        child._worker, child._start_on = Mock(), Mock()
        records = {"vms": {self.vm.id: {"snapshot": self.vm.image, "type": self.vm.type,
                    "state": "running", "lease": {"pid": snap["from_pid"],
                    "host": socket.gethostname(), "batch": "scheduler"}}}}
        registry = SimpleNamespace(locked=lambda: contextlib.nullcontext(records))
        client = SimpleNamespace(rpc=Mock(), lease=dict(self.vm.lease), owner="fixture-owner")
        executor = lambda *a, **kw: contextlib.nullcontext(SimpleNamespace(map=lambda f, xs: map(f, xs)))
        with patch.object(S, "Registry", return_value=registry), \
             patch.object(S.boat, "pid_alive", return_value=True), \
             patch.object(S.boat, "agent_client", return_value=client), \
             patch.object(S.threading, "Thread"), \
             patch.object(S.concurrent.futures, "ThreadPoolExecutor", executor):
            child.reload()
        child._worker.assert_not_called()
        child._start_on.assert_not_called()
        client.rpc.assert_not_called()
        return child, client

    def journal_bytes(self):
        return {p.name: p.read_bytes() for p in (self.root / "queue").iterdir() if p.is_file()}

    def test_lost_pending_reap_survives_without_reopening_or_replaying(self):
        self.terminal()
        before = self.journal_bytes()
        child, client = self.candidate()
        self.assertTrue(child._handoff_ack["items_ready"], child.recovery)
        self.assertTrue(child.recovery["ready"])
        it = child.batches[self.b.id].items[self.it.name]
        vm = child.vms[self.vm.id]
        self.assertIs(vm.running[(self.b.id, self.it.run_name)], it)
        self.assertEqual(it.state, "done")
        self.assertEqual(it.fate.state, "done")
        self.assertTrue(it.owned.closed)
        self.assertFalse(it.owned.reaped)
        self.assertEqual(it.owned.placements, 1)
        self.assertEqual(it.launch_identity, self.it.launch_identity)
        self.assertEqual(it.last_placement, self.it.last_placement)
        self.assertEqual(before, self.journal_bytes())
        with patch.object(S.boat, "pid_alive", return_value=True):
            self.assertFalse(child._activate_handoff())
        child._worker.assert_not_called()
        with patch.object(S.boat, "pid_alive", return_value=False):
            self.assertTrue(child._activate_handoff())
        child._start_on.assert_not_called()
        client.rpc.return_value = {"running": [], "done": []}
        child._reconcile_taken([vm])
        self.assertEqual(it.state, "done")
        self.assertIs(vm.running[(self.b.id, self.it.run_name)], it)
        self.assertFalse(child._place(it, vm))
        self.assertFalse(child._finished(vm, {"batch": self.b.id, "name": it.run_name,
                                           "launch_identity": it.launch_identity, "code": 0}))
        self.assertEqual(before, self.journal_bytes())
        self.assertEqual(child.batches[self.b.id].rows[0]["status"], "LOST")

    def test_successful_terminal_verdict_is_also_retained_without_replay(self):
        self.terminal("SUCCEEDED")
        before = self.journal_bytes()
        child, _ = self.candidate()
        it = child.batches[self.b.id].items[self.it.name]
        self.assertTrue(child.recovery["ready"])
        self.assertEqual(it.state, "done")
        self.assertTrue(it.owned.closed)
        self.assertEqual(child.batches[self.b.id].rows[0]["status"], "SUCCEEDED")
        self.assertEqual(before, self.journal_bytes())

    def test_persisted_terminal_beats_stale_live_snapshot_state(self):
        self.terminal()
        for state in ("starting", "running", "preempting", "done"):
            with self.subTest(state=state):
                snap = self.snapshot()
                snap["vms"][0]["items"][0]["state"] = state
                child, _ = self.candidate(snap)
                self.assertTrue(child.recovery["ready"])
                self.assertEqual(child.batches[self.b.id].items[self.it.name].state, "done")
                child._start_on.assert_not_called()
                self.assertEqual(child._handoff_deferred[1], [])

    def test_closed_admission_with_pending_reap_keeps_its_exact_hold(self):
        self.terminal()
        self.msg["admission_closed"] = True
        self.write_batch()
        before = self.journal_bytes()
        child, _ = self.candidate()
        it = child.batches[self.b.id].items[self.it.name]
        self.assertTrue(child.recovery["ready"])
        self.assertTrue(it.batch.admission_closed)
        self.assertTrue(it.owned.closed)
        self.assertFalse(it.owned.reaped)
        self.assertEqual(it.state, "done")
        self.assertIs(child.vms[self.vm.id].running[(self.b.id, it.run_name)], it)
        self.assertEqual(before, self.journal_bytes())

    def test_native_close_after_takeover_reaps_without_changing_the_lost_row(self):
        self.terminal()
        child, client = self.candidate()
        rows_before = (self.root / "queue" / (self.b.id + ".rows")).read_bytes()
        client.rpc.return_value = self.good_receipt()
        receipt = child.cancel(self.b.id)
        self.assertTrue(receipt["cleanup_complete"], receipt)
        self.assertEqual((self.root / "queue" / (self.b.id + ".rows")).read_bytes(), rows_before)
        self.assertEqual(client.rpc.call_args.args[0], "cancelitem")
        child._start_on.assert_not_called()

    def test_archived_closed_batch_is_not_resurrected(self):
        self.terminal()
        snap = self.snapshot()
        self.vm.client.rpc.return_value = self.good_receipt()
        self.assertTrue(self.s.cancel(self.b.id)["cleanup_complete"])
        before = self.journal_bytes()
        child, _ = self.candidate(snap)
        self.assertTrue(child.recovery["ready"])
        self.assertIn(self.b.id, child.recovery["closed"])
        self.assertNotIn(self.b.id, child.batches)
        self.assertFalse(child.vms[self.vm.id].running)
        self.assertEqual(before, self.journal_bytes())

    def test_terminal_identity_mismatches_refuse_readiness_without_rewriting(self):
        self.terminal()
        cases = [("launch_identity", "wrong"), ("run_name", "other"), ("placed", 2),
                 ("placement.identity", "wrong"), ("placement.vm", "other-vm"),
                 ("placement.boot_id", "other-boot"), ("placement.attempt", 2),
                 ("placement.attempt", True), ("placement.name", "other"),
                 ("placement.run_name", "other"), ("placement", None)]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                snap = self.snapshot()
                offered = snap["vms"][0]["items"][0]
                if "." in field:
                    _, key = field.split(".")
                    offered["placement"][key] = value
                else:
                    offered[field] = value
                before = self.journal_bytes()
                child, _ = self.candidate(snap)
                self.assertFalse(child.recovery["ready"])
                self.assertFalse(child._handoff_ack["items_ready"])
                self.assertEqual(child.recovery["item_mismatch"]["missing_count"], 1)
                self.assertFalse(child.vms[self.vm.id].running)
                self.assertEqual(child.batches[self.b.id].items[self.it.name].state, "done")
                self.assertEqual(before, self.journal_bytes())

    def test_wrong_persisted_result_identity_refuses_readiness(self):
        self.terminal()
        path = self.root / "queue" / (self.b.id + ".rows")
        row = json.loads(path.read_text())
        row["launch_identity"] = "wrong"
        path.write_text(json.dumps(row) + "\n")
        child, _ = self.candidate()
        self.assertFalse(child.recovery["ready"])
        self.assertEqual(child.recovery["item_mismatch"]["missing_count"], 1)

    def test_original_placement_boot_is_kept_when_worker_has_a_new_boot(self):
        self.terminal()
        snap = self.snapshot()
        snap["vms"][0]["lease"]["boot"] = "new-current-worker-boot"
        child, _ = self.candidate(snap)
        self.assertTrue(child.recovery["ready"])
        it = child.batches[self.b.id].items[self.it.name]
        self.assertEqual(it.last_placement["boot_id"], self.it.last_placement["boot_id"])
        self.assertEqual(it.state, "done")
        self.assertTrue(it.owned.closed)
        self.assertFalse(it.owned.reaped)

    def test_ordinary_live_strict_attempt_still_attaches_once(self):
        self.place_without_thread()
        child, _ = self.candidate()
        it = child.batches[self.b.id].items[self.it.name]
        self.assertTrue(child.recovery["ready"])
        self.assertEqual(it.state, "running")
        self.assertEqual(it.owned.placements, 1)
        self.assertEqual(it.launch_identity, self.it.launch_identity)
        self.assertEqual(child._handoff_deferred[1], [])


class ServingCapabilities(unittest.TestCase):
    def test_serving_code_remains_stable_when_a_different_install_is_staged(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            pkg = root / "supercomputer"
            pkg.mkdir()
            source = pkg / "staged.py"
            source.write_text("VERSION = 'first'\n")
            owner = S.Scheduler.__new__(S.Scheduler)
            owner.code = "serving-frozen-code"
            with patch.object(S, "installed_source", return_value=root):
                first = owner.capabilities()
                self.assertEqual(first["code"], owner.code)
                self.assertEqual(first["code_installed"], S.code_hash(pkg))
                source.write_text("VERSION = 'second-and-longer'\n")
                second = owner.capabilities()
            self.assertEqual(second["code"], owner.code)
            self.assertNotEqual(first["code_installed"], second["code_installed"])
            self.assertEqual(first["strict_once_protocol"], second["strict_once_protocol"])

    def test_class_level_probe_reports_imported_source_separately_from_install(self):
        with patch.object(S, "code_hash", side_effect=lambda pkg=None: "staged" if pkg is None else "imported"):
            caps = S.Scheduler.capabilities()
        self.assertEqual(caps["code"], "imported")
        self.assertEqual(caps["code_installed"], "staged")
        self.assertEqual(caps["strict_once_protocol"], 1)

    def test_real_http_capabilities_route_uses_the_serving_instance(self):
        from test_http_admission import handler_class
        owner = S.Scheduler.__new__(S.Scheduler)
        owner.code = "actual-serving-code"
        handler = object.__new__(handler_class(owner))
        handler.path = "/capabilities"
        with patch.object(S, "code_hash", return_value="staged-not-serving"):
            status, raw = handler._post(b"{}")
        caps = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual(caps["code"], owner.code)
        self.assertEqual(caps["code_installed"], "staged-not-serving")


if __name__ == "__main__":
    unittest.main()
