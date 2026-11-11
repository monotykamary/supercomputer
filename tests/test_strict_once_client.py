"""Strict-once public client contract; fake RPCs only, never a local scheduler.

Run these tests on the native warm image using tests/laws.toml. These prove
client forwarding and refusal behavior, not the worker's runtime guarantee.
"""
import contextlib
import http.client
import hashlib
import io
import json
import os
import pathlib
import stat
import sys
import tempfile
import tarfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat, config, fanout, scheduler

CAPS = {"strict_once_protocol": 1, "submit_flags": ["strict_once", "retain_journals"],
        "runtime_verified": False}
ITEM = {"name": "one", "kind": "sh", "cmd": "true", "weight": 1, "est": 1}
JOB = {"name": "one", "cmd": "true"}


def session(strict=True, caps=None, submit=None, poll=None):
    s = object.__new__(boat.Scheduled)
    s.strict_once = strict
    s.client = Mock()
    def req(path, msg, **kw):
        if path == "/capabilities":
            return CAPS if caps is None else caps
        if path == "/submit":
            return submit(msg) if submit else {"ok": True}
        if path == "/poll":
            return poll(msg) if poll else {"done": True, "rows": [{"name": "one", "code": 0}]}
        if path == "/close":
            return {}
        if path == "/snapshot":
            return {"ok": True, "batch": msg["batch"], "strict_once": True, "max_attempts": 1,
                    "placements": [{"name": "one", "run_name": "one", "identity": "nonce", "vm": "vm", "boot_id": "boot", "attempt": 1}]}
        raise AssertionError(path)
    s.client.req.side_effect = req
    s.client.remote = None
    s.sched = Mock(NAMES={2: "submission"})
    s.tree = SimpleNamespace(id="tree", files={"test.py": "hash"}, links={}, gits={})
    s.caches, s.proj = [], SimpleNamespace(cache_outputs=lambda c: [], name="test")
    s._bid_lock, s.bids, s.batch = threading.Lock(), [], "strict-batch"
    s.klass, s.label, s.image, s.vm_type = 2, "test", "image", "large"
    s.ctx_b64, s.warm_only = "", False
    s.stop, s.vm_ids, s.run_names = threading.Event(), set(), {}
    s.logs = {}
    s._admission_lock = threading.Lock()
    s._custody_lock = threading.Lock()
    s._custody_hashes, s.custody = {}, {}
    return s


def requests(s, path):
    return [call.args[1] for call in s.client.req.call_args_list if call.args[0] == path]


class StrictAdmission(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = patch.object(config, "LIVE", pathlib.Path(self.tmp.name))
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_old_controller_refused_before_any_upload(self):
        for caps in ({}, {"submit_flags": ["strict_once"]},
                     {**CAPS, "strict_once_protocol": True},
                     {**CAPS, "strict_once_protocol": 2},
                     {**CAPS, "submit_flags": []}):
            with self.subTest(caps=caps):
                s = session(caps=caps)
                with self.assertRaises(boat.Unavailable):
                    s.run([ITEM], Mock())
                s.client.push_tree.assert_not_called()
                s.client.push_caches.assert_not_called()
                self.assertEqual(requests(s, "/submit"), [])

    def test_wire_opt_in_is_batch_only_without_runtime_verified_claim(self):
        s = session()
        lost = Mock()
        s.run([ITEM], Mock(), on_lost=lost)
        msg, = requests(s, "/submit")
        self.assertIs(msg["strict_once"], True)
        self.assertIs(msg["on_lost"], False)
        self.assertEqual(msg["max_attempts"], 1)
        self.assertNotIn("strict_once", msg["items"][0])
        self.assertNotIn("runtime_verified", msg)
        self.assertNotIn("retain_journals", msg)
        lost.assert_not_called()

    def test_default_wire_and_legacy_fallback_remain_unchanged(self):
        s = session(strict=False, poll=lambda _: {"rows": [{"name": "one", "lost": True}], "done": True})
        lost, done = Mock(), Mock()
        s.run([ITEM], done, on_lost=lost)
        self.assertEqual(requests(s, "/capabilities"), [])
        msg, = requests(s, "/submit")
        self.assertNotIn("strict_once", msg)
        self.assertTrue(msg["on_lost"])
        lost.assert_called_once()
        done.assert_not_called()

    def test_stale_lost_row_never_invokes_fallback_and_keeps_identity(self):
        row = {"name": "one", "code": -1, "lost": True, "status": "LOST", "terminal": True,
               "strict_once": True, "launch_identity": {"nonce": "n"},
               "placement": {"vm": "owned-vm", "boot_id": "boot", "attempt": 1}}
        s = session(poll=lambda _: {"rows": [row], "done": True})
        done, lost = Mock(), Mock()
        s.run([ITEM], done, on_lost=lost)
        lost.assert_not_called()
        for key, value in row.items():
            self.assertEqual(done.call_args.args[1][key], value)

    def test_missing_terminal_row_is_lost_without_invented_identity(self):
        s = session(poll=lambda _: {"rows": [], "done": True, "err": "no such batch"})
        done, lost = Mock(), Mock()
        with patch.object(boat.time, "sleep"):
            s.run([ITEM], done, on_lost=lost)
        lost.assert_not_called()
        done.assert_called_once()
        row = done.call_args.args[1]
        self.assertEqual(row["status"], "LOST")
        self.assertTrue(row["terminal"] and row["strict_once"] and row["identity_unavailable"])
        for key in ("lost", "vm", "boot_id", "launch_identity", "placement"):
            self.assertNotIn(key, row)
        self.assertEqual(len(requests(s, "/submit")), 1)

    def test_lost_submit_ack_polls_the_admitted_id_without_resubmission(self):
        admitted = {}
        def submit(msg):
            admitted[msg["batch"]] = dict(msg)
            raise ConnectionResetError("admission persisted, reply lost")
        def poll(msg):
            self.assertIn(msg["batch"], admitted)
            self.assertEqual(admitted[msg["batch"]]["items"], [ITEM])
            return {"rows": [{"name": "one", "code": 0}], "done": True}
        s = session(submit=submit, poll=poll)
        done, lost = Mock(), Mock()
        s.run([ITEM], done, on_lost=lost)
        self.assertEqual(len(requests(s, "/submit")), 1)
        self.assertEqual(s.bids, ["strict-batch"])
        self.assertEqual(requests(s, "/poll")[0]["batch"], "strict-batch")
        done.assert_called_once()
        lost.assert_not_called()
        self.assertEqual(requests(s, "/close"), [])
        custody = done.call_args.args[1]["custody"]
        self.assertIn("ambiguous_submit", [e["event"] for e in custody["events"]])
        self.assertEqual(custody["sha256"], hashlib.sha256(pathlib.Path(custody["file"]).read_bytes()).hexdigest())

    def test_reconnect_reuses_admission_and_does_not_close_before_result(self):
        s = session()
        old = s.client
        old.remote = "forward"
        old._known.return_value = {"trees": ["tree"]}
        count = 0
        def poll(msg):
            nonlocal count
            count += 1
            if count == 1:
                raise ConnectionResetError("handoff")
            return {"rows": [{"name": "one", "code": 0}], "done": True}
        original = old.req.side_effect
        old.req.side_effect = lambda path, msg, **kw: poll(msg) if path == "/poll" else original(path, msg, **kw)
        s.sched._remote.return_value = old
        with patch.object(boat.time, "sleep"):
            s.run([ITEM], Mock())
        self.assertEqual(len(requests(s, "/submit")), 1)
        self.assertIn("files", requests(s, "/submit")[0]["tree"])
        self.assertEqual({msg["batch"] for msg in requests(s, "/poll")}, {"strict-batch"})
        self.assertEqual(requests(s, "/close"), [])
        s.sched.ensure_running.assert_not_called()
        s.sched._remote.assert_called_once()
        self.assertEqual(s.custody["strict-batch"]["reconnect_count"], 1)

    def test_explicit_missing_tree_retries_only_the_rejected_admission(self):
        calls = []
        def submit(msg):
            calls.append(json.loads(json.dumps(msg)))
            return {"ok": False, "missing": ["hash"]} if len(calls) == 1 else {"ok": True}
        s = session(submit=submit)
        s.run([ITEM], Mock())
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])
        self.assertEqual(s.bids, ["strict-batch"])

    def test_explicit_refusal_never_polls_or_retries(self):
        s = session(submit=lambda _: {"ok": False, "error": "quarantined"})
        with self.assertRaisesRegex(RuntimeError, "quarantined"):
            s.run([ITEM], Mock())
        self.assertEqual(len(requests(s, "/submit")), 1)
        self.assertEqual(requests(s, "/poll"), [])

    def test_new_run_intentionally_creates_new_admission(self):
        s = session()
        s.run([ITEM], Mock())
        s.run([ITEM], Mock())
        self.assertEqual(len(set(msg["batch"] for msg in requests(s, "/submit"))), 2)

    def test_close_during_upload_fences_submit_and_run_after_close(self):
        s = session()
        entered, release = threading.Event(), threading.Event()
        errors = []
        def upload(_):
            entered.set()
            if not release.wait(3):
                raise AssertionError("upload release timed out")
        s.client.push_tree.side_effect = upload
        def run():
            try:
                s.run([ITEM], Mock())
            except BaseException as exc:
                errors.append(exc)
        worker = threading.Thread(target=run)
        worker.start()
        try:
            self.assertTrue(entered.wait(3))
            s.close()
            self.assertEqual(requests(s, "/submit"), [])
        finally:
            release.set()
            worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertIsInstance(errors[0], RuntimeError)
        before = s.client.req.call_count
        with self.assertRaisesRegex(RuntimeError, "closed"):
            s.run([ITEM], Mock())
        self.assertEqual(s.client.req.call_count, before)

    def test_close_waits_for_inflight_admission_not_poll(self):
        s = session()
        entered, release = threading.Event(), threading.Event()
        errors, closed = [], threading.Event()
        original = s.client.req.side_effect
        def req(path, msg, **kw):
            if path == "/submit":
                entered.set()
                if not release.wait(3):
                    raise AssertionError("submit release timed out")
            return original(path, msg, **kw)
        s.client.req.side_effect = req
        def run():
            try:
                s.run([ITEM], Mock())
            except BaseException as exc:
                errors.append(exc)
        worker = threading.Thread(target=run)
        closer = threading.Thread(target=lambda: (s.close(), closed.set()))
        worker.start()
        try:
            self.assertTrue(entered.wait(3))
            closer.start()
            self.assertTrue(s.stop.wait(3))
            self.assertFalse(closed.is_set())
        finally:
            release.set()
            worker.join(3)
            if closer.ident is not None:
                closer.join(3)
        self.assertFalse(worker.is_alive() or closer.is_alive())
        self.assertTrue(closed.is_set())
        self.assertEqual(len(requests(s, "/submit")), 1)
        self.assertEqual(len(requests(s, "/close")), 1)

    def test_overlapping_runs_attribute_transport_to_their_own_batch(self):
        s = session()
        barrier = threading.Barrier(2, timeout=3)
        original = s.client.req.side_effect
        errors = []
        def req(path, msg, **kw):
            if path == "/poll":
                barrier.wait()
                s.client.transport_event("transport_reconnect", path=msg["batch"], attempt=1, error="ConnectionResetError")
            return original(path, msg, **kw)
        s.client.req.side_effect = req
        def run():
            try:
                s.run([ITEM], Mock())
            except BaseException as exc:
                errors.append(exc)
        workers = [threading.Thread(target=run) for _ in range(2)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(4)
        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(errors, [])
        self.assertEqual(len(s.custody), 2)
        for bid, journal in s.custody.items():
            events = [e for e in journal["events"] if e["event"] == "transport_reconnect"]
            self.assertEqual([e["path"] for e in events], [bid])
            self.assertEqual(journal["reconnect_count"], 1)

    def test_running_close_binds_snapshot_without_waiting_for_poll(self):
        s = session()
        entered, release = threading.Event(), threading.Event()
        original = s.client.req.side_effect
        errors = []
        def req(path, msg, **kw):
            if path == "/poll":
                entered.set()
                if not release.wait(3):
                    raise AssertionError("poll release timed out")
                s.client.transport_event("transport_reconnect", path="worker-poll", attempt=1, error="ConnectionResetError")
            if path == "/close":
                s.client.transport_event("transport_error", path="closer", attempt=1, error="ConnectionResetError")
                return {"ok": True, "complete": True, "cleanup_complete": True, "strict_once": True,
                        "batch": msg["batch"], "max_attempts": 1, "admission_closed": True, "journals_retained": True,
                        "attempts": [{"name": "one", "run_name": "one", "launch_identity": "nonce", "vm": "vm",
                                      "boot_id": "boot", "attempt": 1, "cleanup_complete": True}], "unlaunched": [], "pending": []}
            return original(path, msg, **kw)
        s.client.req.side_effect = req
        def run():
            try:
                s.run([ITEM], Mock())
            except BaseException as exc:
                errors.append(exc)
        worker = threading.Thread(target=run)
        worker.start()
        try:
            self.assertTrue(entered.wait(3))
            summary = s.close()
            self.assertTrue(summary["cleanup_complete"])
            self.assertTrue(worker.is_alive())
        finally:
            release.set()
            worker.join(3)
        self.assertEqual(errors, [])
        events = s.custody[s.batch]["events"]
        self.assertEqual(len([e for e in events if e.get("path") == "closer"]), 1)
        self.assertEqual(len([e for e in events if e.get("path") == "worker-poll"]), 1)

    def test_close_receipt_requires_exact_binding_and_item_partition(self):
        s = session()
        placement = {"name": "one", "run_name": "one", "identity": "nonce", "vm": "vm", "boot_id": "boot", "attempt": 1}
        s._strict_items = {"strict-batch": {"one"}}
        s._remember_placement("strict-batch", placement)
        attempt = {"name": "one", "run_name": "one", "launch_identity": "nonce", "vm": "vm", "boot_id": "boot", "attempt": 1, "cleanup_complete": True}
        receipt = {"batch": "strict-batch", "ok": True, "complete": True, "cleanup_complete": True, "strict_once": True,
                   "max_attempts": 1, "admission_closed": True, "journals_retained": True, "pending": [], "attempts": [attempt], "unlaunched": []}
        self.assertTrue(s._valid_close("strict-batch", receipt, None))
        for key, value in (("batch", "foreign"), ("max_attempts", True), ("journals_retained", False),
                           ("strict_once", False), ("pending", ["one"]), ("unlaunched", ["one"]), ("attempts", []),
                           ("attempts", [attempt, attempt])):
            with self.subTest(key=key, value=value):
                self.assertFalse(s._valid_close("strict-batch", {**receipt, key: value}, None))
        for key, value in (("launch_identity", "foreign"), ("vm", "foreign"), ("boot_id", "foreign"),
                           ("name", "foreign"), ("run_name", "foreign"), ("attempt", True), ("cleanup_complete", False)):
            with self.subTest(attempt_key=key):
                self.assertFalse(s._valid_close("strict-batch", {**receipt, "attempts": [{**attempt, key: value}]}, None))

    def test_legacy_admission_survives_later_strict_refusal(self):
        s = session(strict=False, submit=lambda msg: {"ok": not msg.get("strict_once", False)})
        s.run([ITEM], lambda *_: None)
        legacy_bid = s.bids[0]
        with self.assertRaises(RuntimeError):
            s.run([ITEM], lambda *_: None, strict_once=True)
        s.close()
        self.assertEqual(requests(s, "/close"), [{"batch": legacy_bid}])

    def test_custody_fsyncs_new_parent_chain(self):
        s = session()
        with tempfile.TemporaryDirectory() as root:
            live = pathlib.Path(root) / "fresh" / "nested"
            synced = []
            real = os.fsync
            def fsync(fd):
                synced.append(stat.S_ISDIR(os.fstat(fd).st_mode))
                real(fd)
            with patch.object(config, "LIVE", live), patch.object(boat.os, "fsync", side_effect=fsync):
                s._custody_event("strict-batch", "admission_attempt")
            self.assertEqual(synced.count(True), 4)
            self.assertEqual(synced.count(False), 1)

    def test_custody_fsyncs_file_and_new_directory_entries(self):
        s = session()
        synced = []
        real = os.fsync
        def fsync(fd):
            synced.append(stat.S_ISDIR(os.fstat(fd).st_mode))
            real(fd)
        with patch.object(boat.os, "fsync", side_effect=fsync):
            s._custody_event("strict-batch", "admission_attempt")
        self.assertEqual(synced.count(True), 2)
        self.assertEqual(synced.count(False), 1)

    def test_preflight_hidden_reconnect_is_recorded_before_upload(self):
        s = session(caps={**CAPS, "code": "controller-digest"})
        original = s.client.req.side_effect
        def req(path, msg, **kw):
            if path == "/capabilities":
                s.client.transport_event("transport_reconnect", path=path, attempt=1, error="ConnectionResetError")
            return original(path, msg, **kw)
        s.client.req.side_effect = req
        done = Mock()
        s.run([ITEM], done)
        custody = done.call_args.args[1]["custody"]
        self.assertEqual(custody["reconnect_count"], 1)
        self.assertEqual(custody["controller_code"], "controller-digest")
        self.assertEqual(custody["client_sha256"], boat._CLIENT_SHA256)
        event = next(e for e in custody["events"] if e["event"] == "preflight_complete")
        self.assertEqual(event["client_sha256"], boat._CLIENT_SHA256)
        self.assertEqual(event["controller_code"], "controller-digest")

    def test_collect_hidden_reconnect_is_recorded_and_callback_restored(self):
        s = session()
        s.run([ITEM], Mock())
        previous = s.client.transport_event
        s.vm_ids = {"vm"}
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w"):
            pass
        def req(path, msg, **kw):
            self.assertEqual(path, "/logs")
            s.client.transport_event("transport_reconnect", path=path, attempt=1, error="ConnectionResetError")
            return buf.getvalue()
        s.client.req.side_effect = req
        s.collect()
        self.assertEqual(s.custody[s.batch]["reconnect_count"], 1)
        self.assertIs(s.client.transport_event, previous)

    def test_preflight_only_custody_is_never_closed(self):
        s = session(caps={})
        with self.assertRaises(boat.Unavailable):
            s.run([ITEM], Mock())
        self.assertTrue(s.custody)
        s.close()
        self.assertEqual(requests(s, "/close"), [])

    def test_later_refusal_does_not_hide_prior_owned_admission(self):
        s = session()
        s.run([ITEM], Mock())
        admitted = s.batch
        original = s.client.req.side_effect
        s.client.req.side_effect = lambda path, msg, **kw: ({"ok": False, "error": "foreign ID"} if path == "/submit" else original(path, msg, **kw))
        with self.assertRaises(RuntimeError):
            s.run([ITEM], Mock())
        refused = s.batch
        self.assertNotEqual(admitted, refused)
        s.close()
        self.assertEqual([msg["batch"] for msg in requests(s, "/close")], [admitted])

    def test_refused_first_admission_is_never_closed(self):
        s = session(submit=lambda _: {"ok": False, "error": "foreign ID"})
        with self.assertRaises(RuntimeError):
            s.run([ITEM], Mock())
        summary = s.close()
        self.assertEqual(requests(s, "/close"), [])
        self.assertFalse(summary["cleanup_complete"])

    def test_custody_failure_is_not_a_reconnect(self):
        s = session()
        original = s.client.req.side_effect
        s.client.req.side_effect = lambda path, msg, **kw: (_ for _ in ()).throw(boat.CustodyError("disk")) if path == "/poll" else original(path, msg, **kw)
        with self.assertRaises(boat.CustodyError):
            s.run([ITEM], Mock())
        self.assertEqual(len(requests(s, "/poll")), 1)
        self.assertEqual(s.custody[s.batch]["reconnect_count"], 0)

    def test_run_override_and_attempt_limit_validation(self):
        s = session(strict=False)
        s.run([ITEM], Mock(), strict_once=True, max_attempts=1)
        self.assertTrue(requests(s, "/submit")[0]["strict_once"])
        for limit in (0, 2, True, 1.0, "1"):
            s = session()
            with self.subTest(limit=limit), self.assertRaises(ValueError):
                s.run([ITEM], Mock(), strict_once=True, max_attempts=limit)
            s.client.req.assert_not_called()

    def test_custody_tail_is_bounded_full_history_hashed_and_private(self):
        s = session()
        for _ in range(100):
            journal = s._custody_event("strict-batch", "reconnect", error="ConnectionResetError")
        data = pathlib.Path(journal["file"]).read_bytes()
        self.assertEqual(journal["event_count"], 100)
        self.assertEqual(journal["reconnect_count"], 100)
        self.assertEqual(len(journal["events"]), 64)
        self.assertEqual(len(data.splitlines()), 100)
        self.assertEqual(journal["sha256"], hashlib.sha256(data).hexdigest())
        self.assertEqual(pathlib.Path(journal["file"]).stat().st_mode & 0o777, 0o600)
        other = session()
        after = other._custody_event("strict-batch", "admission_ack")
        self.assertEqual(after["event_count"], 101)
        self.assertEqual(after["sha256"], hashlib.sha256(pathlib.Path(after["file"]).read_bytes()).hexdigest())

    def test_strict_close_returns_authoritative_attempts_and_pending_errors(self):
        for complete in (True, False):
            s = session()
            s.run([ITEM], Mock())
            receipt = {"ok": complete, "complete": complete, "cleanup_complete": complete,
                       "journals_retained": True, "batch": s.batch, "max_attempts": 1, "strict_once": True, "admission_closed": True,
                       "attempts": [{"name": "one", "run_name": "one", "launch_identity": "nonce", "vm": "vm", "boot_id": "boot", "attempt": 1,
                                     "cleanup_complete": complete}], "unlaunched": [], "pending": [] if complete else ["one"]}
            original = s.client.req.side_effect
            s.client.req.side_effect = lambda path, msg, **kw: receipt if path == "/close" else original(path, msg, **kw)
            summary = s.close()
            self.assertEqual(summary["batches"][s.batch], receipt)
            self.assertIs(summary["cleanup_complete"], complete)
            self.assertEqual(len(requests(s, "/close")), 1)
        s = session()
        s.run([ITEM], Mock())
        s.client.req.side_effect = ConnectionResetError("secret reply must not be journaled")
        summary = s.close()
        self.assertFalse(summary["cleanup_complete"])
        self.assertEqual(summary["batches"][s.batch]["error"], "ConnectionResetError")
        self.assertEqual(len(requests(s, "/close")), 1)
        self.assertNotIn("secret reply", pathlib.Path(s.custody[s.batch]["file"]).read_text())

    def test_session_factory_refuses_legacy_path_and_nonboolean(self):
        with patch.object(scheduler, "enabled", return_value=False), patch.object(boat, "Session") as legacy:
            with self.assertRaises(boat.Unavailable):
                boat.session("strict", strict_once=True)
            legacy.assert_not_called()
            boat.session("legacy")
            legacy.assert_called_once_with("legacy")
        with self.assertRaises(ValueError):
            boat.session("strict", strict_once="yes")


class StrictFanout(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.proj = SimpleNamespace(images={"native": {}}, root=pathlib.Path(self.tmp.name))
        patch.object(config, "LIVE", pathlib.Path(self.tmp.name) / "live").start()
        self.addCleanup(patch.stopall)
        patch.object(fanout, "project", return_value=self.proj).start()
        patch.object(boat, "project", return_value=self.proj).start()
        patch.object(scheduler, "enabled", return_value=True).start()
        patch.object(scheduler, "controller", return_value=True).start()
        self.modal = patch.object(fanout, "ModalCtx").start()
        self.preflight = patch.object(boat, "preflight3").start()
        self.sleep = patch.object(fanout.time, "sleep").start()

    def run_jobs(self, spec=None, **kw):
        with contextlib.redirect_stdout(io.StringIO()):
            return fanout.run_jobs(None, [JOB] if spec is None else spec, self.tmp.name, "", lambda c: c,
                                   Mock(), backend=kw.pop("backend", "auto"), strict_once=True, **kw)

    def test_strict_jobs_forward_once_and_never_requeue_missing_results(self):
        with patch.object(boat, "boat_jobs", return_value=({}, {})) as jobs:
            self.run_jobs()
        jobs.assert_called_once()
        self.assertIs(jobs.call_args.kwargs["strict_once"], True)
        self.assertIsNone(jobs.call_args.kwargs["on_lost"])
        self.preflight.assert_not_called()
        self.modal.assert_not_called()
        self.sleep.assert_not_called()
        self.assertIn("one\t-1\t-1\tboat", (pathlib.Path(self.tmp.name) / "summary.tsv").read_text())

    def test_preflight_refusal_never_falls_back_or_retries(self):
        with patch.object(boat, "boat_jobs", side_effect=boat.Unavailable("old controller")) as jobs:
            with self.assertRaises(boat.Unavailable):
                self.run_jobs()
        jobs.assert_called_once()
        self.modal.assert_not_called()
        self.sleep.assert_not_called()

    def test_gpu_and_non_native_routes_refused_before_provider_calls(self):
        for backend, extra in (("modal", {}), ("runpod", {}), ("auto", {"gpu": True}),
                               ("boat", {"backend": "modal"}), ("auto", {"backend": "runpod"}),
                               ("auto", {"image": "modal-image"})):
            with self.subTest(backend=backend, extra=extra), patch.object(boat, "boat_jobs") as jobs:
                with self.assertRaises(ValueError):
                    self.run_jobs([{**JOB, **extra}], backend=backend)
                jobs.assert_not_called()
        self.modal.assert_not_called()

    def test_auto_executor_propagates_single_failure_without_fallback(self):
        bx = Mock()
        bx.remote.side_effect = RuntimeError("lost owned attempt")
        bx.starmap.return_value = iter([RuntimeError("lost owned attempt")])
        with patch.object(fanout, "BoatEx", return_value=bx) as constructor:
            ex = fanout.AutoEx("runner.py", None, strict_once=True)
        constructor.assert_called_once()
        self.assertTrue(constructor.call_args.kwargs["strict_once"])
        with self.assertRaisesRegex(RuntimeError, "owned attempt"):
            ex.remote(lambda: None)
        bx.remote.assert_called_once()
        self.assertIsInstance(list(ex.starmap(lambda: None, [()]))[0], RuntimeError)
        bx.starmap.assert_called_once()
        self.assertNotIn("on_lost", bx.starmap.call_args.kwargs)
        self.modal.assert_not_called()
        self.sleep.assert_not_called()

    def test_boat_executor_forwards_flag_and_suppresses_lost_callback(self):
        fake = Mock(scheduled=True, vm_type="large", teardown=False)
        def run(items, done, **kw):
            self.assertIsNone(kw["on_lost"])
            done(items[0], {"code": -1, "seconds": -1, "error": "owned LOST"})
        fake.run.side_effect = run
        with patch.object(boat, "session", return_value=fake) as make, patch("supercomputer.agent.import_runner", return_value=SimpleNamespace(BOAT={})):
            ex = fanout.BoatEx(str(self.proj.root / "runner.py"), strict_once=True)
        self.assertTrue(make.call_args.kwargs["strict_once"])
        lost = Mock()
        results = list(ex.starmap(lambda: None, [()], on_lost=lost))
        self.assertIsInstance(results[0], RuntimeError)
        fake.run.assert_called_once()
        lost.assert_not_called()

    def test_boat_jobs_propagates_strict_unavailable_and_closes_once(self):
        fake = Mock(scheduled=True, klass=2, batch="b", image="image", phases={})
        fake.sched.NAMES = {2: "submission"}
        fake.run.side_effect = boat.Unavailable("unsupported strict_once")
        fake.close.return_value = {"vms": 0, "warm": 0}
        lost = Mock()
        with patch.object(boat, "job_items", return_value=([ITEM], ["one"])), patch.object(boat, "last_durations", return_value={}), patch.object(boat, "session", return_value=fake) as make:
            with self.assertRaises(boat.Unavailable):
                boat.boat_jobs([JOB], self.tmp.name, "", lambda c: c, strict_once=True, on_lost=lost)
        self.assertTrue(make.call_args.kwargs["strict_once"])
        self.assertIsNone(fake.run.call_args.kwargs["on_lost"])
        fake.run.assert_called_once()
        fake.close.assert_called_once()
        lost.assert_not_called()

    def test_runner_cli_flag_is_forwarded_and_modal_rejected(self):
        def run(ex):
            pass
        for backend in ("auto", "boat"):
            with patch.object(fanout, "AutoEx") as auto, patch.object(fanout, "BoatEx") as direct, patch.object(fanout, "reexec_with_modal"), patch.object(sys.modules["__main__"], "app", None, create=True), patch.object(sys, "argv", ["runner.py", "--backend", backend, "--strict-once"]):
                fanout.cli(run, "runner.py")
                make = auto if backend == "auto" else direct
                self.assertTrue(make.call_args.kwargs["strict_once"])
        with patch.object(fanout, "reexec_with_modal"), patch.object(sys, "argv", ["runner.py", "--backend", "modal", "--strict-once"]), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                fanout.cli(run, "runner.py")
        self.assertEqual(error.exception.code, 2)

    def test_boat_jobs_exports_custody_and_complete_result_envelope(self):
        s = session()
        with patch.object(boat, "job_items", return_value=([ITEM], ["one"])), patch.object(boat, "last_durations", return_value={}), patch.object(boat, "note_durations"), patch.object(boat, "session", return_value=s):
            rows, summary = boat.boat_jobs([JOB], self.tmp.name, "", lambda c: c, strict_once=True)
        journal = rows["one"]["custody"]
        copied = pathlib.Path(journal["export_file"])
        self.assertTrue(copied.is_file())
        self.assertEqual(journal["sha256"], hashlib.sha256(copied.read_bytes()).hexdigest())
        envelope = json.loads((pathlib.Path(self.tmp.name) / "results.json").read_text())
        self.assertEqual(envelope["rows"]["one"]["batch"], s.batch)
        self.assertFalse(envelope["cleanup"]["cleanup_complete"])

    def test_real_jobs_cli_parser_and_forwarding(self):
        from supercomputer import __main__ as cli, config, controller
        with patch.object(cli, "_run") as run:
            cli.main(["run", "--jobs", "jobs.json", "--out", self.tmp.name, "--strict-once"])
        args = run.call_args.args[0]
        self.assertTrue(args.strict_once)
        path = pathlib.Path(self.tmp.name) / "jobs.json"
        path.write_text(json.dumps([JOB]))
        args.jobs = str(path)
        self.proj.cfg, self.proj.modal = {}, {}
        with patch.object(config, "project", return_value=self.proj), patch.object(controller, "maybe_sync"), patch.object(fanout, "reexec_with_modal"), patch.object(fanout, "run_jobs") as jobs, patch.object(fanout.fallback, "needs_modal", return_value=False), patch.dict(os.environ):
            cli._run(args)
        self.assertTrue(jobs.call_args.kwargs["strict_once"])
        self.assertEqual(jobs.call_args.kwargs["max_attempts"], 1)


class StrictTransport(unittest.TestCase):
    def test_hidden_transport_retries_are_observable_without_secret_bodies(self):
        c = object.__new__(boat.AgentClient)
        c.cookie, c.query, c.base, c.token, c.vm = "", "", "", "secret", "vm"
        conn = Mock()
        conn.request.side_effect = ConnectionResetError("private body")
        events = []
        with patch.object(c, "_get", return_value=conn), patch.object(boat.time, "sleep"):
            with self.assertRaises(boat.AgentError):
                c._req("POST", "/poll", b"private body", transport_event=lambda event, **details: events.append((event, details)))
        self.assertEqual([event for event, _ in events], ["transport_reconnect"] * 3 + ["transport_error"])
        self.assertEqual(events[0][1], {"path": "/poll", "attempt": 1, "error": "ConnectionResetError"})
        self.assertNotIn("private body", str(events))
        with patch.object(c, "_get", return_value=conn), patch.object(boat.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "custody persistence failed"):
                c._req("POST", "/poll", b"{}", transport_event=Mock(side_effect=OSError("disk full")))

    def test_https_submit_is_single_attempt_but_ordinary_rpc_still_retries(self):
        for path, body, expected in (("/submit", b'{"strict_once":true}', 1),
                                     ("/submit", b'{}', 4), ("/poll", b'{}', 4)):
            with self.subTest(path=path, body=body):
                c = object.__new__(boat.AgentClient)
                c.cookie, c.query, c.base, c.token, c.vm = "", "", "", "token", "vm"
                conn = Mock()
                conn.request.side_effect = ConnectionResetError("reply lost")
                with patch.object(c, "_get", return_value=conn), patch.object(boat.time, "sleep"):
                    with self.assertRaises(boat.AgentError):
                        c._req("POST", path, body)
                self.assertEqual(conn.request.call_count, expected)


if __name__ == "__main__":
    unittest.main(verbosity=2)
