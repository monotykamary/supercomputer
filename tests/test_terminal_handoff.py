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


    def test_missing_empty_or_non_string_terminal_run_name_refuses_cleanly(self):
        self.terminal()
        for value in (None, "", 17, "missing"):
            with self.subTest(value=value):
                snap = self.snapshot()
                offered = snap["vms"][0]["items"][0]
                if value == "missing":
                    del offered["run_name"]
                else:
                    offered["run_name"] = value
                child, _ = self.candidate(snap)
                self.assertFalse(child.recovery["ready"])
                self.assertFalse(child.vms[self.vm.id].running)
        offered = self.snapshot()["vms"][0]["items"][0]
        self.assertFalse(self.s._terminal_handoff_hold(self.it, offered, ""))
        old_name, self.it.name = self.it.name, ""
        try:
            self.assertFalse(self.s._terminal_handoff_hold(self.it, offered, self.vm.id))
        finally:
            self.it.name = old_name

    def test_absent_identity_names_do_not_match_by_none_equality(self):
        self.terminal()
        offered = self.snapshot()["vms"][0]["items"][0]
        del offered["run_name"]
        offered["placement"]["run_name"] = None
        self.it.last_placement["run_name"] = None
        self.b.rows[0]["run_name"] = None
        self.assertFalse(self.s._terminal_handoff_hold(self.it, offered, self.vm.id))

    def test_duplicate_or_conflicting_terminal_rows_refuse_readiness(self):
        self.terminal()
        path = self.root / "queue" / (self.b.id + ".rows")
        original = path.read_bytes()
        for conflict in (False, True):
            with self.subTest(conflict=conflict):
                row = copy.deepcopy(self.b.rows[0])
                if conflict:
                    row["launch_identity"] = "conflicting-identity"
                self.b.rows.append(row)
                try:
                    self.assertFalse(self.s._terminal_handoff_hold(self.it, self.snapshot()["vms"][0]["items"][0], self.vm.id))
                finally:
                    self.b.rows.pop()
                path.write_bytes(original + (json.dumps(row) + "\n").encode())
                before = self.journal_bytes()
                child, _ = self.candidate()
                self.assertFalse(child.recovery["ready"])
                self.assertFalse(child.vms[self.vm.id].running)
                self.assertEqual(before, self.journal_bytes())
        path.write_bytes(original)

    def test_terminal_row_run_name_is_bound_to_the_placement(self):
        self.terminal()
        path = self.root / "queue" / (self.b.id + ".rows")
        row = json.loads(path.read_text())
        row["run_name"] = "wrong-run"
        path.write_text(json.dumps(row) + "\n")
        child, _ = self.candidate()
        self.assertFalse(child.recovery["ready"])
        self.assertFalse(child.vms[self.vm.id].running)

    def test_terminal_takeover_keeps_full_persisted_placement_clock_and_fate(self):
        self.terminal()
        path = self.root / "queue" / (self.b.id + ".placed")
        placement = json.loads(path.read_text())
        placement["extra_provenance"] = {"original": True, "cpu_observation": "kept"}
        path.write_text(json.dumps(placement) + "\n")
        predicate = S.Scheduler._terminal_handoff_hold
        def checked(owner, item, offered, vm):
            item.t_start = 123.0
            item.fate.reloaded, item.fate.reattached = Mock(), Mock()
            return predicate(owner, item, offered, vm)
        with patch.object(S.Scheduler, "_terminal_handoff_hold", checked):
            child, _ = self.candidate()
        item = child.batches[self.b.id].items[self.it.name]
        self.assertTrue(child.recovery["ready"])
        self.assertEqual(item.last_placement, placement)
        self.assertEqual(item.t_start, 123.0)
        self.assertEqual(item.launch_identity, placement["identity"])
        self.assertEqual(item.fate.state, "done")
        item.fate.reloaded.assert_not_called()
        item.fate.reattached.assert_not_called()

    def test_terminal_placement_count_is_not_inflated_by_a_run_suffix(self):
        self.terminal()
        self.it.run_name += "~s99"
        self.it.last_placement["run_name"] = self.it.run_name
        path = self.root / "queue" / (self.b.id + ".placed")
        path.write_text(json.dumps(self.it.last_placement) + "\n")
        path = self.root / "queue" / (self.b.id + ".rows")
        row = json.loads(path.read_text())
        row["run_name"] = self.it.run_name
        row["placement"]["run_name"] = self.it.run_name
        path.write_text(json.dumps(row) + "\n")
        child, _ = self.candidate()
        item = child.batches[self.b.id].items[self.it.name]
        self.assertTrue(child.recovery["ready"])
        self.assertEqual(item.placed, 1)
        self.assertEqual(item.owned.placements, 1)
        self.assertEqual(item.run_name, self.it.run_name)

    def test_vm_loss_is_durable_diagnostic_not_reap_or_a_new_verdict(self):
        self.terminal()
        child, _ = self.candidate()
        item = child.batches[self.b.id].items[self.it.name]
        vm = child.vms[self.vm.id]
        rows = (self.root / "queue" / (self.b.id + ".rows")).read_bytes()
        child._let_go = Mock(side_effect=lambda v, **kw: child.vms.pop(v.id))
        child._lost_vm(vm)
        child._lost_vm(vm)
        loss_path = self.root / "queue" / (self.b.id + ".cancel")
        self.assertTrue(loss_path.exists(), "VM loss must leave durable diagnostic evidence")
        events = [json.loads(x) for x in loss_path.read_text().splitlines()]
        losses = [x for x in events if x.get("phase") == "terminal_vm_lost"]
        self.assertEqual(len(losses), 1)
        self.assertTrue(child._matching_terminal_loss(item, losses[0]))
        self.assertFalse(item.owned.reaped)
        self.assertEqual(item.state, "done")
        self.assertEqual((self.root / "queue" / (self.b.id + ".rows")).read_bytes(), rows)
        receipt = child._close_receipt(item.batch)
        self.assertFalse(receipt["cleanup_complete"])
        self.assertEqual(receipt["attempts"][0]["terminal_loss"], losses[0])
        reloaded, _ = self.candidate()
        restored = reloaded.batches[self.b.id].items[self.it.name]
        self.assertEqual(restored.terminal_loss, losses[0])
        self.assertTrue(restored.owned.closed)
        self.assertFalse(restored.owned.reaped)
        self.assertEqual(restored.state, "done")

    def test_failed_loss_journal_keeps_the_worker_and_cleanup_hold(self):
        self.terminal()
        child, _ = self.candidate()
        item = child.batches[self.b.id].items[self.it.name]
        vm = child.vms[self.vm.id]
        child._let_go = Mock()
        with patch.object(child, "_append_owned", side_effect=OSError("fsync failed")):
            with self.assertRaisesRegex(OSError, "fsync failed"):
                child._lost_vm(vm)
        self.assertFalse(vm.dead)
        self.assertIs(child.vms[vm.id], vm)
        self.assertIs(vm.running[(self.b.id, item.run_name)], item)
        self.assertFalse(item.owned.reaped)
        child._let_go.assert_not_called()

    def test_non_strict_terminal_offer_is_not_a_cleanup_hold(self):
        self.terminal()
        self.msg["strict_once"] = False
        self.write_batch()
        child, _ = self.candidate()
        self.assertFalse(child.recovery["ready"])
        self.assertFalse(any(vm.running for vm in child.vms.values()))


class ServingCapabilities(unittest.TestCase):
    def test_serving_code_remains_stable_when_a_different_install_is_sampled(self):
        owner = S.Scheduler.__new__(S.Scheduler)
        owner.code = "serving-frozen-code"
        owner._status_installed_code = "first-staged-code"
        with patch.object(S, "code_hash", side_effect=AssertionError("request touched disk")):
            first = owner.capabilities()
            owner._status_installed_code = "second-staged-code"
            second = owner.capabilities()
        self.assertEqual(first["code"], owner.code)
        self.assertEqual(second["code"], owner.code)
        self.assertEqual(first["code_installed"], "first-staged-code")
        self.assertEqual(second["code_installed"], "second-staged-code")
        self.assertEqual(first["strict_once_protocol"], second["strict_once_protocol"])

    def test_class_level_probe_never_reads_the_staged_install(self):
        with patch.object(S, "own_code", return_value="imported"), \
             patch.object(S, "code_hash", side_effect=FileNotFoundError("staged swap")) as disk:
            caps = S.Scheduler.capabilities()
        disk.assert_not_called()
        self.assertEqual(caps["code"], "imported")
        self.assertIsNone(caps["code_installed"])
        self.assertEqual(caps["strict_once_protocol"], 1)

    def test_class_level_code_is_frozen_after_first_import_identity_read(self):
        with patch.dict(S._code_cache, {}, clear=True), \
             patch.object(S, "code_hash", side_effect=["original-imported-code", AssertionError("files changed")]) as disk:
            first = S.Scheduler.capabilities()
            second = S.Scheduler.capabilities()
        self.assertEqual(first["code"], "original-imported-code")
        self.assertEqual(second["code"], first["code"])
        self.assertEqual(disk.call_count, 1)

    def test_missing_or_mid_swap_install_cannot_fail_serving_capabilities(self):
        owner = S.Scheduler.__new__(S.Scheduler)
        owner.code = "kept-handback-copy"
        owner._status_installed_code = "last-published-staged-code"
        for error in (FileNotFoundError("removed"), OSError("mid-swap"), RuntimeError("unrelated install")):
            with self.subTest(error=type(error).__name__), patch.object(S, "code_hash", side_effect=error) as disk:
                caps = owner.capabilities()
            disk.assert_not_called()
            self.assertEqual(caps["code"], "kept-handback-copy")
            self.assertEqual(caps["code_installed"], "last-published-staged-code")

    def test_same_owner_status_and_capabilities_use_the_same_code_fields(self):
        owner = sched()
        owner.code, owner.agent_version = "kept-handback-copy", "fixture-agent"
        owner.load, owner.alerts = 0, []
        owner.stale = lambda: {}
        for installed in (None, "new-staged-code"):
            if installed is not None:
                owner._status_installed_code = installed
            with owner.lock, patch.object(S, "code_hash", side_effect=AssertionError("reader touched disk")):
                status = owner._status_read()
                caps = owner.capabilities()
            self.assertEqual(caps["code"], status["code"])
            self.assertEqual(caps["code_installed"], status["code_installed"])

    def test_uninitialized_owner_fails_closed_with_a_clear_error(self):
        owner = S.Scheduler.__new__(S.Scheduler)
        with self.assertRaisesRegex(RuntimeError, "initialized serving scheduler"):
            owner.capabilities()

    def test_real_http_capabilities_route_uses_the_serving_instance_without_io(self):
        from test_http_admission import handler_class
        owner = S.Scheduler.__new__(S.Scheduler)
        owner.code = "actual-serving-code"
        owner._status_installed_code = "staged-not-serving"
        handler = object.__new__(handler_class(owner))
        handler.path = "/capabilities"
        with patch.object(S, "code_hash", side_effect=FileNotFoundError("staged swap")):
            status, raw = handler._post(b"{}")
        caps = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual(caps["code"], owner.code)
        self.assertEqual(caps["code_installed"], "staged-not-serving")


if __name__ == "__main__":
    unittest.main()
