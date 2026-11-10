"""Source-authored lifecycle wiring tests. No providers, daemon or installed state.

Run only in Main's isolated remote harness: python tests/test_owned_scheduler.py.
Mock receipts test scheduler validation, NOT kernel namespace guarantees.
"""
import json
import os
import pathlib
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S
from supercomputer.rules.owned_lifecycle import OwnedLifecycle, retention_supported


class OwnedSchedulerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.p = patch.object(S, "DIR", self.root)
        self.p.start()
        self.addCleanup(self.p.stop)
        blobs_patch = patch.object(S, "BLOBS", self.root / "blobs")
        blobs_patch.start()
        self.addCleanup(blobs_patch.stop)
        (self.root / "queue").mkdir()
        self.msg = {"batch": "owned-test", "klass": 3, "image": "test-image",
                    "tree": {"id": "test-tree", "files": {}}, "ctx_b64": "",
                    "strict_once": True, "retain_journals": True,
                    "items": [{"name": "item", "cmd": "true"}]}
        self.b = S.Batch(self.msg)
        self.it = self.b.items["item"]
        self.s = S.Scheduler.__new__(S.Scheduler)
        self.s.lock = threading.RLock()
        self.s.wake = threading.Event()
        self.s.stop = threading.Event()
        self.s.batches = {self.b.id: self.b}
        self.s.vms = {}
        self.s.pol = {}
        self.s.agent_version = "test-agent-version"
        self.s.m = {"placed": {}, "waits": {}, "placed_warm": 0, "preempted": {}}
        self.s._h = lambda: Mock()
        self.vm = S.VMState("vm-test", "test-image", S.boat.VM_TYPE, "warm")
        self.vm.ready = True
        self.vm.client = Mock()
        self.s.vms[self.vm.id] = self.vm
        self.write_batch()

    def write_batch(self):
        (self.root / "queue" / f"{self.b.id}.json").write_text(json.dumps(self.msg))

    def place_without_thread(self):
        with patch.object(S.threading, "Thread"):
            with self.s.lock:
                self.assertTrue(self.s._place(self.it, self.vm))
        self.it.owned.start_settled()
        self.it.start_done.set()

    def good_receipt(self):
        return {"ok": True, "reaped": True, "namespace_absent": True,
                "identity": self.it.launch_identity, "batch": self.b.id, "name": self.it.run_name}

    def test_queued_cancel_closes_admission_before_placement(self):
        self.assertTrue(self.s.cancel(self.b.id)["complete"])
        self.assertEqual(self.it.state, "done")
        self.assertFalse(self.s._place(self.it, self.vm))
        self.assertFalse(self.vm.client.rpc.called)
        self.assertTrue(json.loads((self.root / "queue" / f"{self.b.id}.json").read_text())["admission_closed"])

    def test_cancel_placement_threads_share_the_admission_lock(self):
        fenced, release = threading.Event(), threading.Event()
        real = self.s._persist_fence
        def persist(b):
            real(b)
            fenced.set()
            self.assertTrue(release.wait(2))
        results = {}
        def cancel():
            results["cancel"] = self.s.cancel(self.b.id, 0)
        def place():
            results["place"] = self.s._place(self.it, self.vm)
        with patch.object(self.s, "_persist_fence", side_effect=persist):
            canceller = threading.Thread(target=cancel)
            placer = threading.Thread(target=place)
            canceller.start()
            self.assertTrue(fenced.wait(2))
            placer.start()
            release.set()
            canceller.join(2)
            placer.join(2)
        self.assertFalse(canceller.is_alive())
        self.assertFalse(placer.is_alive())
        self.assertTrue(results["cancel"]["complete"])
        self.assertFalse(results["place"])
        self.assertEqual(self.it.owned.placements, 0)

    def test_active_start_rpc_must_settle_before_positive_cancel(self):
        self.place_without_thread()
        self.it.owned.inflight = True
        self.it.start_done.clear()
        entered, release = threading.Event(), threading.Event()
        self.s._prepare = Mock()
        def rpc(command, *args, **kwargs):
            if command == "owned_rpc":
                request = args[0]
                if request["command"] == "vtick":
                    entered.set()
                    self.assertTrue(release.wait(2))
                return {"ok": True, "protocol": 1, "version": self.s.agent_version,
                        "nonce": request["nonce"], "features": {"pidfd": True,
                        "private_pid_namespace": "verified-before-payload"}, "result": {}}
            self.assertEqual(command, "cancelitem")
            self.assertTrue(self.it.start_done.is_set())
            return self.good_receipt()
        self.vm.client.rpc.side_effect = rpc
        starter = threading.Thread(target=self.s._start_on, args=(self.it, self.vm))
        starter.start()
        self.assertTrue(entered.wait(2))
        self.assertFalse(self.s.cancel(self.b.id, 0)["complete"])
        self.assertEqual(self.vm.client.rpc.call_count, 2)
        release.set()
        starter.join(2)
        self.assertFalse(starter.is_alive())
        self.assertTrue(self.s.cancel(self.b.id)["complete"])

    def test_placement_journal_precedes_prepare_thread(self):
        def on_thread(*args, **kwargs):
            placements = S.jlines(self.root / "queue" / f"{self.b.id}.placed")
            self.assertEqual(len(placements), 1)
            self.assertEqual(placements[0]["identity"], self.it.launch_identity)
            return Mock()
        with patch.object(S.threading, "Thread", side_effect=on_thread):
            self.assertTrue(self.s._place(self.it, self.vm))

    def test_strict_refuses_every_requeued_state(self):
        self.place_without_thread()
        for why in ("preempt", "lost", "prepare", "restart", "sleep", "tick error"):
            with self.subTest(why=why):
                self.it.state = "queued"
                self.it.fate.requeued()
                self.assertFalse(self.s._place(self.it, self.vm))
                self.assertEqual(self.it.owned.placements, 1)
                self.assertEqual(self.it.state, "done")
                self.assertNotIn("lost", self.b.rows[-1])

    def test_old_rule_allows_replacement_without_flag(self):
        r = OwnedLifecycle(False)
        self.assertTrue(r.place())
        r.start_settled()
        self.assertTrue(r.place())
        self.assertEqual(r.placements, 2)

    def test_strict_boat_failure_never_requests_modal(self):
        self.place_without_thread()
        self.s._boat_failed(self.it, self.vm.id, "lost")
        self.assertEqual(self.it.state, "done")
        self.assertNotIn("lost", self.b.rows[-1])

    def test_preempted_row_is_terminal_failure_not_replacement(self):
        self.place_without_thread()
        self.s._finished(self.vm, {"batch": self.b.id, "name": self.it.run_name,
                                   "launch_identity": self.it.launch_identity, "preempted": True})
        self.assertEqual(self.it.state, "done")
        self.assertNotIn("lost", self.b.rows[-1])

    def test_matching_receipt_reaps_and_repeated_cancel_is_idempotent(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value = self.good_receipt()
        self.assertTrue(self.s.cancel(self.b.id)["complete"])
        calls = self.vm.client.rpc.call_count
        rows = list(self.b.rows)
        self.assertTrue(self.s.cancel(self.b.id)["complete"])
        self.assertEqual(self.vm.client.rpc.call_count, calls)
        self.assertEqual(self.b.rows, rows)
        args = self.vm.client.rpc.call_args.args
        self.assertEqual(args[:3], ("cancelitem", self.b.id, self.it.run_name))
        self.assertEqual(args[4], self.it.launch_identity)

    def test_failed_mismatched_and_timeout_reap_stay_held(self):
        self.place_without_thread()
        for field, value in (("ok", False), ("reaped", False), ("namespace_absent", False),
                             ("identity", "stale"), ("batch", "other"), ("name", "other"), ("ok", 1)):
            with self.subTest(field=field, value=value):
                receipt = self.good_receipt()
                receipt[field] = value
                self.vm.client.rpc.return_value = receipt
                result = self.s.cancel(self.b.id, 0)
                self.assertFalse(result["complete"])
                self.assertEqual(self.it.state, "cancel_pending")
                self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        self.vm.client.rpc.side_effect = TimeoutError("timeout")
        self.assertFalse(self.s.cancel(self.b.id, 0)["complete"])
        self.assertFalse(self.it.owned.reaped)

    def test_inflight_start_cancellation_cannot_acknowledge_early(self):
        self.place_without_thread()
        self.it.owned.inflight = True
        self.it.start_done.clear()
        result = self.s.cancel(self.b.id, 0)
        self.assertFalse(result["complete"])
        self.assertTrue(self.b.admission_closed)
        self.assertFalse(self.vm.client.rpc.called)
        self.it.owned.start_settled()
        self.it.start_done.set()
        self.vm.client.rpc.return_value = self.good_receipt()
        self.assertTrue(self.s.cancel(self.b.id)["complete"])

    def test_preparing_cancel_fences_start_rpc(self):
        self.place_without_thread()
        self.it.owned.inflight = True
        self.it.start_done.clear()
        self.s.cancel(self.b.id, 0)
        self.s._prepare = Mock()
        self.s._start_on(self.it, self.vm)
        self.assertFalse(self.vm.client.rpc.called)
        self.assertTrue(self.it.start_done.is_set())

    def test_cancel_fence_durable_before_agent_rpc(self):
        self.place_without_thread()
        def rpc(*args, **kwargs):
            saved = json.loads((self.root / "queue" / f"{self.b.id}.json").read_text())
            self.assertTrue(saved["admission_closed"])
            self.assertTrue(saved["retain_journals"])
            return self.good_receipt()
        self.vm.client.rpc.side_effect = rpc
        self.assertTrue(self.s.cancel(self.b.id)["complete"])

    def test_fence_disk_failure_does_not_call_agent(self):
        self.place_without_thread()
        with patch.object(self.s, "_persist_fence", side_effect=OSError("disk")):
            with self.assertRaises(OSError):
                self.s.cancel(self.b.id)
        self.assertTrue(self.b.admission_closed)
        self.assertFalse(self.vm.client.rpc.called)

    def test_retain_close_preserves_all_journals_and_reload_fence(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value = self.good_receipt()
        self.assertTrue(self.s.close(self.b.id)["complete"])
        for suffix in ("json", "rows", "placed", "cancel"):
            self.assertTrue((self.root / "queue" / f"{self.b.id}.{suffix}").exists())
        restored = S.Scheduler.__new__(S.Scheduler)
        restored.batches = {}
        restored._take_pool = Mock()
        with patch.object(S.threading, "Thread"):
            restored.reload()
        b = restored.batches[self.b.id]
        self.assertTrue(b.admission_closed)
        self.assertTrue(b.strict_once)
        self.assertEqual(b.items["item"].owned.placements, 1)
        self.assertTrue(b.items["item"].owned.reaped)
        self.assertEqual(b.items["item"].state, "done")

    def test_snapshot_cumulative_placements_without_provider_calls(self):
        self.place_without_thread()
        snap = self.s.snapshot(self.b.id)
        self.assertEqual(len(snap["placements"]), 1)
        self.assertEqual(snap["items"][0]["placements"], 1)
        self.assertNotIn("ctx_b64", snap)
        self.assertFalse(self.vm.client.rpc.called)

    def test_strict_preemption_uses_bound_cancel_not_legacy_kill(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value = self.good_receipt()
        self.s._kill(self.vm, self.it)
        self.assertTrue(self.it.owned.reaped)
        self.assertEqual(self.it.state, "done")
        self.assertEqual(self.vm.client.rpc.call_args.args[0], "cancelitem")
        self.assertNotIn((self.b.id, self.it.run_name), self.vm.running)
        self.assertFalse(self.s._place(self.it, self.vm))

    def test_strict_preemption_failed_receipt_keeps_hold(self):
        self.place_without_thread()
        self.vm.client.rpc.return_value = {**self.good_receipt(), "identity": "wrong"}
        self.s._kill(self.vm, self.it)
        self.assertEqual(self.it.state, "cancel_pending")
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        self.assertEqual(self.vm.client.rpc.call_args.args[0], "cancelitem")
        self.assertFalse(self.it.owned.reaped)
        self.s._finished(self.vm, {"batch": self.b.id, "name": self.it.run_name,
                                   "launch_identity": self.it.launch_identity, "preempted": True})
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)

    def test_strict_stale_kill_never_uses_legacy_rpc(self):
        self.place_without_thread()
        self.s._kill_run(self.vm, self.b.id, "unknown-run")
        self.assertFalse(self.vm.client.rpc.called)
        self.vm.client.rpc.return_value = self.good_receipt()
        self.s._kill_run(self.vm, self.b.id, self.it.run_name)
        self.assertEqual(self.vm.client.rpc.call_args.args[0], "cancelitem")

    def test_owned_journal_corruption_quarantines_id(self):
        self.place_without_thread()
        path = self.root / "queue" / f"{self.b.id}.placed"
        path.write_text(path.read_text() + '{"torn":')
        self.s.batches = {}
        self.s._take_pool = Mock()
        self.s.reload()
        self.assertNotIn(self.b.id, self.s.batches)
        self.assertFalse(self.s.submit(self.msg)["ok"])
        self.assertIn("quarantined", self.s.snapshot(self.b.id)["error"])
        self.assertTrue(path.exists())

    def test_corrupt_admission_cannot_be_reused(self):
        (self.root / "queue" / f"{self.b.id}.json").write_text("{")
        self.s.batches = {}
        self.s._take_pool = Mock()
        self.s.reload()
        self.assertFalse(self.s.submit(self.msg)["ok"])

    def test_snapshot_preserves_failure_context_but_not_credentials(self):
        self.b.rows.append({"name": "item", "code": -1, "error": "prepare refused ownership",
                            "token": "must-not-leak", "details": {"cookie": "hidden", "reason": "uncertain"}})
        row = self.s.snapshot(self.b.id)["rows"][0]
        self.assertEqual(row["error"], "prepare refused ownership")
        self.assertEqual(row["details"], {"reason": "uncertain"})
        self.assertNotIn("token", row)

    def test_legacy_placement_does_not_fsync_or_set_ownership_marker(self):
        self.b.strict_once = False
        self.b.retain_journals = False
        self.it.owned = OwnedLifecycle(False)
        with patch.object(S.os, "fsync", side_effect=AssertionError("legacy fsync")):
            self.place_without_thread()
        self.s._prepare = Mock()
        self.vm.client.rpc.return_value = {}
        self.s._start_on(self.it, self.vm)
        spec = self.vm.client.rpc.call_args.args[1]["start"][0]["spec"]
        self.assertNotIn("strict_once", spec)
        self.assertNotIn("launch_identity", spec)

    def legacy_completion_at_log_boundary(self, log_error=False):
        """C65/f7 is the retained broken variant: it holds this slot at RPC entry.

        This deterministic boundary test does not depend on wall/virtual timing
        or on the scenario's 45s backoff amplifying a 0.15s ordering change.
        """
        self.b.strict_once = False
        self.b.retain_journals = False
        self.it.owned = OwnedLifecycle(False)
        self.place_without_thread()
        row = self.terminal_row(details={"verbatim": ["legacy", 7]})
        row.pop("launch_identity")
        original_name = self.it.run_name
        follower = S.Item(self.b, {"name": "next", "cmd": "true"})
        self.b.items[follower.name] = follower
        observed = []
        expected_ack = {"batch": self.b.id, "name": original_name}
        test = self
        class BoundACKs(list):
            def append(self, ack):
                test.assertEqual(ack, expected_ack)
                test.assertNotIn((test.b.id, original_name), test.vm.running)
                test.assertEqual(test.it.fate.state, "done")
                test.assertEqual(test.it.state, "done")
                test.assertEqual(len(test.b.rows), 1)
                super().append(ack)
        self.vm.acks = BoundACKs()
        def rpc(command, *args, **kwargs):
            self.assertEqual(command, "itemlogs")
            self.assertEqual(args, (self.b.id, original_name))
            self.assertEqual(self.it.state, "finishing")
            self.assertEqual(self.it.fate.state, "done")
            self.assertNotIn((self.b.id, original_name), self.vm.running)
            self.assertIs(self.vm.finishing[(self.b.id, original_name)], self.it)
            self.assertEqual(self.vm.used(), 0)
            self.assertEqual(self.b.rows, [])
            self.assertEqual(self.vm.acks, [])
            # Capacity is reusable while output is pending, but an update must
            # retain this terminal identity until commit, then ACK the old run.
            with patch.object(S.threading, "Thread"):
                self.assertTrue(self.s._place(follower, self.vm))
            observed.append(True)
            if log_error:
                raise TimeoutError("best-effort log retrieval failed")
            return {"out": "exact legacy output", "err": "legacy stderr"}
        self.vm.client.rpc.side_effect = rpc
        self.assertTrue(self.s._accept_finished(self.vm, row))
        self.assertEqual(observed, [True])
        self.assertIs(self.vm.running[(self.b.id, follower.run_name)], follower)
        self.assertEqual(len(self.b.rows), 1)
        self.assertEqual(self.b.rows[0]["details"], row["details"])
        self.assertEqual(self.b.rows[0]["run_name"], original_name)
        self.assertEqual(self.vm.acks, [{"batch": self.b.id, "name": original_name}])
        self.vm.acks = list(self.vm.acks)  # duplicate reports re-ACK an absent old placement
        return self.root / "queue" / f"{self.b.id}.logs" / original_name

    def test_legacy_terminal_releases_slot_before_best_effort_output(self):
        logs = self.legacy_completion_at_log_boundary()
        self.assertEqual((logs / "out").read_text(), "exact legacy output")
        self.assertEqual((logs / "err").read_text(), "legacy stderr")

    def test_legacy_output_failure_does_not_retain_capacity_or_drop_row(self):
        self.legacy_completion_at_log_boundary(log_error=True)

    def test_legacy_completion_duplicate_does_not_redeliver_or_refetch_output(self):
        self.legacy_completion_at_log_boundary()
        self.vm.client.rpc.reset_mock()
        row = self.terminal_row()
        row.pop("launch_identity")
        self.assertTrue(self.s._accept_finished(self.vm, row))
        self.assertEqual(len(self.b.rows), 1)
        self.assertEqual(self.vm.acks, [{"batch": self.b.id, "name": self.it.run_name}] * 2)
        self.vm.client.rpc.assert_not_called()

    def test_legacy_completion_exact_accounting_learning_delivery_order(self):
        self.b.strict_once = self.b.retain_journals = False
        self.it.owned = OwnedLifecycle(False)
        self.place_without_thread()
        self.it.t_start = 100
        self.vm.busy_s, self.vm.idle_since = 5, 90
        self.s.est_hist, self.s.class_obs = {}, {}
        key = (self.b.id, self.it.run_name)
        ack = {"batch": key[0], "name": key[1]}
        events = []
        test = self
        class ACKs(list):
            def append(self, value):
                test.assertEqual(value, ack)
                test.assertNotIn(key, test.vm.running)
                test.assertEqual(test.it.fate.state, "done")
                test.assertEqual(len(test.b.rows), 1)
                events.append("ack")
                super().append(value)
        class Running(dict):
            def pop(self, value, default=None):
                test.assertEqual(test.vm.acks, [])
                test.assertEqual(test.vm.busy_s, 5)
                test.assertEqual(test.vm.idle_since, 90)
                test.assertEqual(test.it.fate.state, "placed")
                events.append("release")
                return super().pop(value, default)
        self.vm.acks, self.vm.running = ACKs(), Running(self.vm.running)
        finished, learn, deliver = self.it.fate.finished, self.s._learn, self.b.deliver
        def fate():
            self.assertNotIn(key, self.vm.running)
            self.assertEqual((self.vm.busy_s, self.vm.idle_since), (15, 110))
            self.assertEqual(self.it.state, "finishing")
            self.assertEqual(self.b.slot_s, 0)
            events.append("fate")
            finished()
        def learning(item, seconds):
            self.assertEqual(item.fate.state, "done")
            self.assertEqual(self.b.slot_s, 7 * item.weight)
            self.assertFalse(hasattr(self.b, "obs_n"))
            events.append("learn")
            learn(item, seconds)
        def logs(command, *args, **kwargs):
            self.assertEqual(command, "itemlogs")
            self.assertEqual(args, (self.b.id, self.it.run_name))
            self.assertEqual((self.b.obs_s, self.b.obs_n), (7, 1))
            self.assertEqual(self.b.rows, [])
            self.assertTrue(self.s.wake.is_set())
            events.append("logs")
            return {"out": "kept", "err": ""}
        def delivery(row):
            self.assertEqual((self.root / "queue" / f"{self.b.id}.logs" / self.it.run_name / "out").read_text(), "kept")
            self.assertEqual(row["run_name"], key[1])
            self.assertEqual(row["seconds"], 7)
            self.assertTrue(self.s.wake.is_set())
            events.append("deliver")
            deliver(row)
        self.s.wake.clear()
        self.vm.client.rpc.side_effect = logs
        with patch.object(S.time, "time", return_value=110), \
             patch.object(self.it.fate, "finished", side_effect=fate), \
             patch.object(self.s, "_learn", side_effect=learning), \
             patch.object(self.b, "deliver", side_effect=delivery):
            self.assertTrue(self.s._accept_finished(self.vm, {"batch": key[0], "name": key[1], "code": 0, "seconds": 7}))
        self.assertEqual(events, ["release", "fate", "learn", "logs", "deliver", "ack"])
        self.assertTrue(self.s.wake.is_set())
        self.assertEqual(self.s.est_hist[self.s.est_key(self.it)], 7)
        self.assertEqual(self.s.class_obs[self.b.klass], [7])
        self.assertEqual(len(self.b.rows), 1)

    def legacy_nonterminal_order(self, ending, transition):
        self.b.strict_once = self.b.retain_journals = False
        self.it.owned = OwnedLifecycle(False)
        self.place_without_thread()
        self.it.t_start = 100
        self.vm.busy_s, self.vm.idle_since = 5, 90
        key = (self.b.id, self.it.run_name)
        original = getattr(self.it.fate, transition)
        self.s.wake.clear()
        seen = []
        def checked_transition():
            self.assertNotIn(key, self.vm.running)
            self.assertEqual((self.vm.busy_s, self.vm.idle_since), (15, 110))
            self.assertEqual(self.vm.acks, [])
            seen.append(transition)
            original()
        with patch.object(S.time, "time", return_value=110), \
             patch.object(self.it.fate, transition, side_effect=checked_transition), \
             patch.object(self.s, "_unprepare"):
            self.assertTrue(self.s._accept_finished(self.vm, {"batch": key[0], "name": key[1], **ending}))
        self.assertEqual(seen, [transition])
        self.assertEqual(self.vm.acks, [{"batch": key[0], "name": key[1]}])
        self.assertEqual(self.it.fate.state, "queued")
        self.assertEqual(self.it.state, "queued")
        self.assertEqual(self.b.rows, [])
        self.assertTrue(self.s.wake.is_set())
        self.vm.client.rpc.assert_not_called()

    def test_legacy_preempted_accounts_before_fate_and_wakes_without_logs(self):
        self.legacy_nonterminal_order({"preempted": True}, "preempted")

    def test_legacy_reprepare_accounts_before_fate_and_wakes_without_logs(self):
        self.legacy_nonterminal_order({"lost": True, "err": "tree isn't on the VM"}, "requeued")

    def terminal_row(self, **extra):
        return {"batch": self.b.id, "name": self.it.run_name, "code": 0,
                "seconds": 0, "launch_identity": self.it.launch_identity, **extra}

    def terminal_rows(self):
        return S.jlines(self.root / "queue" / f"{self.b.id}.rows")

    def test_cancel_wins_while_completion_is_blocked_in_itemlogs(self):
        self.place_without_thread()
        entered, release = threading.Event(), threading.Event()
        outcomes = {}
        def rpc(command, *args, **kwargs):
            if command == "itemlogs":
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("bounded itemlogs barrier")
                return {"out": "exact output", "err": ""}
            self.assertEqual(command, "cancelitem")
            return self.good_receipt()
        self.vm.client.rpc.side_effect = rpc
        def finish():
            outcomes["finish"] = self.s._accept_finished(self.vm, self.terminal_row())
        worker = threading.Thread(target=finish, daemon=True)
        worker.start()
        try:
            self.assertTrue(entered.wait(2))
            self.assertIn((self.b.id, self.it.run_name), self.vm.running)
            self.assertNotEqual(self.it.state, "done")
            self.assertEqual(self.b.rows, [])
            self.assertTrue(self.s.cancel(self.b.id)["complete"])
            saved = list(self.b.rows)
            self.assertTrue(saved[0]["cancelled"])
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertFalse(outcomes["finish"])
        self.assertEqual(self.b.rows, saved)
        self.assertEqual(self.terminal_rows(), saved)
        self.assertEqual(len(saved), 1)
        self.assertEqual(self.vm.acks, [])

    def test_completion_commit_wins_before_concurrent_cancel_fence(self):
        self.place_without_thread()
        entered, release, cancel_attempt = threading.Event(), threading.Event(), threading.Event()
        outcomes = {}
        real_deliver = self.s._deliver_owned
        row = self.terminal_row(details={"verbatim": ["final", 7]})
        self.vm.client.rpc.side_effect = lambda command, *a, **kw: (
            {"out": "exact output", "err": ""} if command == "itemlogs" else self.good_receipt())
        def deliver(b, result):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("bounded terminal commit barrier")
            real_deliver(b, result)
        def finish():
            outcomes["finish"] = self.s._accept_finished(self.vm, row)
        def cancel():
            cancel_attempt.set()
            outcomes["cancel"] = self.s.cancel(self.b.id)
        worker = threading.Thread(target=finish, daemon=True)
        canceller = threading.Thread(target=cancel, daemon=True)
        with patch.object(self.s, "_deliver_owned", side_effect=deliver):
            worker.start()
            try:
                self.assertTrue(entered.wait(2))
                canceller.start()
                self.assertTrue(cancel_attempt.wait(2))
            finally:
                release.set()
                worker.join(3)
                if canceller.ident is not None:
                    canceller.join(3)
        self.assertFalse(worker.is_alive())
        self.assertFalse(canceller.is_alive())
        self.assertTrue(outcomes["finish"])
        self.assertTrue(outcomes["cancel"]["complete"])
        self.assertEqual(len(self.b.rows), 1)
        saved = self.b.rows[0]
        self.assertEqual(saved["details"], row["details"])
        self.assertEqual(saved["code"], 0)
        self.assertNotIn("cancelled", saved)
        self.assertEqual(saved["launch_identity"], self.it.launch_identity)
        self.assertEqual(self.terminal_rows(), self.b.rows)
        self.assertEqual(self.vm.acks, [{"batch": self.b.id, "name": self.it.run_name,
                                        "launch_identity": self.it.launch_identity}])

    def test_strict_result_identity_required_for_every_terminal_kind(self):
        self.place_without_thread()
        for ending in ({}, {"lost": True}, {"preempted": True}, {"code": -1, "err": "failed"}):
            for identity in (None, "old-strict-nonce"):
                with self.subTest(ending=ending, identity=identity):
                    row = self.terminal_row(**ending)
                    if identity is None:
                        row.pop("launch_identity")  # stale legacy done.json
                    else:
                        row["launch_identity"] = identity
                    before = self.it.state
                    self.assertFalse(self.s._accept_finished(self.vm, row))
                    self.assertEqual(self.it.state, before)
                    self.assertIn((self.b.id, self.it.run_name), self.vm.running)
                    self.assertEqual(self.b.rows, [])
                    self.assertEqual(self.vm.acks, [])
                    self.assertIn("placement held", self.s.snapshot(self.b.id)["items"][0]["result_error"])
                    self.assertFalse(self.vm.client.rpc.called)
                    self.assertEqual(self.it.owned.placements, 1)
        self.vm.client.rpc.return_value = {"out": "", "err": ""}
        self.assertTrue(self.s._accept_finished(self.vm, self.terminal_row()))
        self.assertIsNone(self.s.snapshot(self.b.id)["items"][0]["result_error"])
        self.assertEqual(self.b.rows[0]["code"], 0)
        self.assertNotIn((self.b.id, self.it.run_name), self.vm.running)
        self.assertEqual(self.vm.acks[-1]["launch_identity"], self.it.launch_identity)
        stale = self.terminal_row(launch_identity="old-strict-nonce")
        self.assertFalse(self.s._accept_finished(self.vm, stale))
        self.assertEqual(len(self.vm.acks), 1)

    def test_correct_nonce_lost_row_is_identity_bound_terminal_failure(self):
        self.place_without_thread()
        self.assertTrue(self.s._accept_finished(self.vm, self.terminal_row(lost=True)))
        self.assertEqual(self.b.rows[0]["code"], -1)
        self.assertEqual(self.b.rows[0]["launch_identity"], self.it.launch_identity)
        self.assertNotIn("lost", self.b.rows[0])
        self.assertEqual(self.vm.acks[-1]["launch_identity"], self.it.launch_identity)

    def test_retained_complete_cancel_expires_floor_and_restores_closed(self):
        self.place_without_thread()
        self.s.hints = {}
        self.s.pol = {"floor": 8}
        self.vm.t_real = 1
        self.b.klass = 4  # also must cease acting as an implicit treadmill hint
        self.vm.client.rpc.return_value = {**self.good_receipt(), "identity": "wrong"}
        with patch.object(S.time, "time", return_value=1000), \
             patch.object(S, "FLOOR_ALWAYS", False), patch.object(S, "FLOOR_IDLE", 10), \
             patch.object(S, "FLOOR_DEFAULT", 1), patch.object(S, "FLOOR_BY_IMAGE", {}):
            self.assertFalse(self.s.cancel(self.b.id, 0)["complete"])
            self.assertFalse(self.b.closed)
            self.assertTrue(self.s.treadmill_alive(self.b.image))
            self.assertIn((self.b.id, self.it.run_name), self.vm.running)
            self.vm.client.rpc.return_value = self.good_receipt()
            self.assertTrue(self.s.cancel(self.b.id)["complete"])
            self.assertTrue(self.b.closed)
            self.assertFalse(self.s.treadmill_alive(self.b.image))
            self.assertEqual(self.s.floor(self.b.image), 0)
            saved = json.loads((self.root / "queue" / f"{self.b.id}.json").read_text())
            self.assertTrue(saved["cancellation_complete"])
            # Reload the same retained admission; no worker RPC or threads run.
            self.s.batches = {}
            self.s._take_pool = Mock()
            with patch.object(S.threading, "Thread"):
                self.s.reload()
            restored = self.s.batches[self.b.id]
            self.assertTrue(restored.closed)
            self.assertTrue(restored.items["item"].owned.reaped)
            self.assertFalse(self.s.treadmill_alive(restored.image))
            self.assertEqual(self.s.floor(restored.image), 0)

    def test_pending_cancel_reload_preserves_active_hint_and_placement(self):
        self.msg["klass"] = self.b.klass = 4
        self.write_batch()
        self.place_without_thread()
        self.vm.client.rpc.return_value = {**self.good_receipt(), "namespace_absent": False}
        self.assertFalse(self.s.cancel(self.b.id, 0)["complete"])
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        self.s.batches = {}
        self.s.hints = {}
        self.s._take_pool = Mock()
        with patch.object(S.threading, "Thread"):
            self.s.reload()
        restored = self.s.batches[self.b.id]
        self.assertFalse(restored.closed)
        self.assertTrue(restored.admission_closed)
        self.assertFalse(restored.items["item"].owned.reaped)
        self.assertEqual(restored.items["item"].reattach["vm"], self.vm.id)
        self.assertEqual(restored.items["item"].launch_identity, self.it.launch_identity)
        self.assertTrue(self.s.treadmill_alive(restored.image))

    def test_legacy_validation_defers_to_factory_but_rejects_owned_forgeries(self):
        self.s._validate_submission({"tree": {"files": {}}})
        for fields in ({"strict_once": "false"}, {"retain_journals": 1},
                       {"admission_closed": False}, {"cancellation_complete": True},
                       {"launch_identity": "forged"},
                       {"items": [{"name": "legacy", "launch_identity": "forged"}]}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.s._validate_submission({"tree": {}, **fields})

    def test_safe_batch_ids_and_reserved_ownership_fields(self):
        for bid in (None, "../bad", "a/b", "", 1):
            with self.assertRaises(ValueError):
                self.s.snapshot(bid)
        for flag in ("strict_once", "retain_journals"):
            with self.assertRaises(ValueError):
                self.s._validate_submission({**self.msg, flag: "true"})
        with self.assertRaises(ValueError):
            self.s._validate_submission({**self.msg, "items": [{"name": "x", "launch_identity": "forged"}]})


    def test_retention_flag_matrix_requires_owned_cancellation(self):
        for strict, retained, supported in ((False, False, True), (True, False, True),
                                             (True, True, True), (False, True, False)):
            with self.subTest(strict=strict, retained=retained):
                self.assertIs(retention_supported(strict, retained), supported)
                msg = {**self.msg, "strict_once": strict, "retain_journals": retained}
                if supported:
                    self.s._validate_submission(msg)
                    self.assertEqual(S.Batch(msg).strict_once, strict)
                else:
                    with self.assertRaisesRegex(ValueError, "requires strict_once"):
                        self.s.submit(msg)
                    with self.assertRaisesRegex(ValueError, "requires strict_once"):
                        S.Batch(msg)
                self.vm.client.rpc.assert_not_called()

    def test_retention_only_reload_preserves_quarantined_evidence(self):
        unsupported = {**self.msg, "strict_once": False, "retain_journals": True}
        path = self.root / "queue" / f"{self.b.id}.json"
        original = json.dumps(unsupported)
        path.write_text(original)
        self.s.batches = {}
        self.s._take_pool = Mock()
        self.s.reload()
        self.assertNotIn(self.b.id, self.s.batches)
        self.assertEqual(path.read_text(), original)
        self.assertIn("quarantined", self.s.snapshot(self.b.id)["error"])
        self.vm.client.rpc.assert_not_called()

    def test_legacy_cancel_refuses_without_fencing_or_worker_rpc(self):
        msg = {**self.msg, "batch": "legacy", "strict_once": False, "retain_journals": False}
        legacy = S.Batch(msg)
        self.s.batches[legacy.id] = legacy
        result = self.s.cancel(legacy.id)
        self.assertIs(result["ok"], False)
        self.assertIs(result["complete"], False)
        self.assertIs(result["admission_closed"], False)
        self.assertFalse(legacy.admission_closed)
        self.assertFalse(legacy.retain_journals)
        self.assertFalse(legacy.closed)
        self.vm.client.rpc.assert_not_called()


if __name__ == "__main__":
    unittest.main()
