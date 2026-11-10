"""SOURCE tests for Boat only: real Linux namespaces are mandatory, never skipped.

Run `python3 tests/test_owned_agent.py` in an isolated Linux worker with sudo -n, util-linux,
/proc and Python pidfd APIs. No daemon/provider is started. Namespace tests use the exact
production argv, ownedrun PID1 and run/wrap. Fault-injected observation tests are labelled.
Every process is created by this suite and cleaned up by handle/pidfd, never by name.
"""
import json
import os
import pathlib
import secrets
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import agent as A
from broken_owned_agent import BrokenRunnerReapIsNamespaceAbsence, BrokenFence


class OwnedFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="owned-agent-", dir="/var/tmp")
        self.root = pathlib.Path(self.tmp.name)
        self.saved = A.ROOT, A.BLOBS, A.TREES, A._children, A._owned_children
        A.ROOT, A.BLOBS, A.TREES = self.root, self.root / "blobs", self.root / "trees"
        A._children, A._owned_children = [], {}
        shutil.copyfile(A.__file__, self.root / "agent.py")
        self.batch, self.name, self.nonce = "batch", "item", secrets.token_hex(16)
        self.d = self.root / "b" / self.batch
        (self.d / "jobs").mkdir(parents=True)
        self.extra = []

    def tearDown(self):
        try:
            for entry in list(A._owned_children.values()):
                if entry.get("pidfd") is not None:
                    try:
                        signal.pidfd_send_signal(entry["pidfd"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                # Unbound ownedrun refuses payload and self-exits within 15 seconds. Never
                # kill sudo alone and pretend that would clean an unverified namespace.
                entry["p"].wait(timeout=25)
                if entry.get("pidfd") is not None:
                    os.close(entry["pidfd"])
            for p in [*A._children, *self.extra]:
                if p.poll() is None:
                    p.kill()
                p.wait(timeout=5)
        finally:
            A.ROOT, A.BLOBS, A.TREES, A._children, A._owned_children = self.saved
            self.tmp.cleanup()

    def spec(self, cmd="sleep 300"):
        return {"name": self.name, "kind": "sh", "cmd": cmd, "limit": 600,
                "cpus": os.cpu_count() or 1, "strict_once": True, "launch_identity": self.nonce}

    def cancel(self, timeout=10, identity=None):
        return A.cancelitem(self.batch, self.name, timeout, self.nonce if identity is None else identity)

    def receipt_ok(self, r):
        self.assertEqual((r["batch"], r["name"], r["identity"]), (self.batch, self.name, self.nonce))
        for key in ("ok", "reaped", "namespace_absent"):
            self.assertIs(r[key], True, r)

    def wait_file(self, path, timeout=10):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if path.exists():
                return path.read_text()
            time.sleep(0.02)
        self.fail(f"owned child failed to create {path}: " +
                  (self.d / "jobs" / self.name / "agent.err").read_text())


class CancellationDecisions(OwnedFixture):
    def test_rules_and_kept_broken_variants(self):
        r = A.OwnedCancellation()
        r.cancel()
        self.assertFalse(r.may_start())
        r.observe(True, True, False, False)
        self.assertFalse(r.namespace_absent)
        self.assertFalse(r.ok)
        r.observe(True, False, False, True)
        self.assertFalse(r.ok)
        bad = BrokenRunnerReapIsNamespaceAbsence()
        bad.observe(True, False)
        self.assertTrue(bad.namespace_absent)  # the counterexample, not an acceptable receipt
        fence = BrokenFence()
        fence.cancel()
        self.assertTrue(fence.may_start())

    def test_prelaunch_cancel_fences_late_start_and_survives_batch_cleanup(self):
        first = self.cancel()
        self.receipt_ok(first)  # reaped is vacuous: no child was ever created
        shutil.rmtree(self.d)
        (self.d / "jobs").mkdir(parents=True)
        with mock.patch.object(A.subprocess, "Popen", side_effect=AssertionError("late launch")):
            with self.assertRaisesRegex(ValueError, "admission closed"):
                A.start(self.d, self.spec())
        self.assertFalse((self.d / "jobs" / self.name).exists())
        self.assertEqual(first, self.cancel())

    def test_strict_cpu_grant_never_widens_parent_affinity(self):
        with mock.patch.object(A.os, "sched_getaffinity", return_value={3, 7}), \
             mock.patch.object(A.subprocess, "Popen", side_effect=OSError("no child in allocation fixture")):
            A.start(self.d, self.spec())
        j = self.d / "jobs" / self.name
        self.assertEqual(json.loads((j / "taskset").read_text()), [3, 7])
        self.assertEqual(json.loads((j / "cpus").read_text()), [3, 7])
        self.receipt_ok(self.cancel())

    def test_wrong_nonce_never_reuses_success_receipt(self):
        self.receipt_ok(self.cancel())
        wrong = self.cancel(identity="other-launch")
        self.assertIs(wrong["ok"], False)
        self.assertEqual(wrong["identity"], self.nonce)
        self.receipt_ok(self.cancel())

    def test_identifiers_and_waits_are_validated(self):
        for batch, name in (("../b", "x"), ("b", "../x"), ("/abs", "x"), ("b", ""), ("b", ".")):
            with self.assertRaises(ValueError):
                A.cancelitem(batch, name)
        for wait in (-1, float("nan"), float("inf"), 61):
            with self.assertRaises(ValueError):
                self.cancel(timeout=wait)
        with self.assertRaises(ValueError):
            self.cancel(identity="../nonce")

    def test_corrupt_persisted_record_returns_negative_receipt(self):
        path = A._owned_path(self.batch, self.name)
        path.parent.mkdir(parents=True)
        path.write_text('{"torn":')
        r = self.cancel()
        self.assertIs(r["ok"], False)
        self.assertIs(r["reaped"], False)
        self.assertIs(r["namespace_absent"], False)
        self.assertIsNone(r["identity"])

    def test_popen_failure_has_no_child_and_can_be_cancelled(self):
        with mock.patch.object(A.subprocess, "Popen", side_effect=OSError("fixture launch failure")):
            A.start(self.d, self.spec())
        self.receipt_ok(self.cancel())
        self.assertTrue((self.d / "jobs" / self.name / "done.json").exists())

    def test_unknown_nonchild_persisted_identity_never_signals(self):
        p = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        self.extra.append(p)
        A._owned_write(A._owned_path(self.batch, self.name),
                       {"identity": self.nonce, "state": "launched", "reaped": False,
                        "supervisor": A._process_identity(p.pid)})
        r = self.cancel()
        self.assertFalse(r["ok"])
        self.assertFalse(r["reaped"])
        self.assertFalse(r["namespace_absent"])
        self.assertIsNone(p.poll())

    def test_legacy_retry_name_without_nonce_preserves_original_launch(self):
        # Real child, argv substitution explicitly models ONLY a legacy runner (not namespace acceptance).
        real = subprocess.Popen
        self.name = "item~s2"
        def legacy_child(argv, **kwargs):
            self.assertEqual(argv[2:], ["run", self.batch, self.name])
            return real([sys.executable, "-c", "pass"], **kwargs)
        with mock.patch.object(A.subprocess, "Popen", side_effect=legacy_child), \
             mock.patch.object(A, "_owned_write", side_effect=AssertionError("legacy fsync")), \
             mock.patch.object(A, "_ownership_lock", wraps=A._ownership_lock) as admission_gate:
            A.start(self.d, {"name": self.name})  # deliberately no strict_once or launch_identity
            admission_gate.assert_called_once()  # upgrades must fence legacy starts too
            self.assertFalse(A._owned_children)
            self.assertFalse((self.root / "owned-items").exists())
            p = A._children[0]
            p.wait(timeout=5)
            A._reap_children()
            self.assertFalse(A._children)
        r = self.cancel()
        self.assertFalse(r["reaped"])
        self.assertFalse(r["namespace_absent"])
        self.assertFalse(r["ok"])
        self.assertFalse((self.root / "owned-items").exists())

    def test_cancel_without_nonce_or_strict_record_refuses_missing_legacy(self):
        r = A.cancelitem(self.batch, self.name)
        self.assertFalse(r["ok"])
        self.assertFalse(r["reaped"])
        self.assertFalse(r["namespace_absent"])
        self.assertFalse((self.root / "owned-items").exists())


class IdentityFaultFixtures(OwnedFixture):
    """Explicit on-disk fault fixtures, NOT real namespace acceptance evidence."""
    def job(self, spec=None, record=None, result=None):
        j = self.d / "jobs" / self.name
        j.mkdir()
        A.write_json(j / "spec.json", self.spec() if spec is None else spec)
        if record is not None:
            A._owned_write(A._owned_path(self.batch, self.name), record)
        if result is not None:
            A.write_json(j / "done.json", result)
        return j

    def failure(self):
        with mock.patch.object(A.subprocess, "Popen", side_effect=OSError("identity fixture: no child")):
            A.start(self.d, self.spec())
        return self.d / "jobs" / self.name

    def test_stale_legacy_success_cannot_be_new_strict_admission(self):
        old = {"name": self.name, "code": 0, "seconds": 1}
        j = self.job(spec={"name": self.name}, result=old)
        with mock.patch.object(A.subprocess, "Popen", side_effect=AssertionError("replacement")):
            with self.assertRaisesRegex(ValueError, "collision"):
                A.start(self.d, self.spec())
        self.assertFalse(A._owned_path(self.batch, self.name).exists())
        self.assertEqual(A.report(self.d)[0], [old])  # still legacy, never blessed with a nonce
        A.tick(self.batch, {"acks": [{"name": self.name, "launch_identity": self.nonce}], "wait": 0})
        self.assertTrue((j / "done.json").exists())

    def test_legacy_terminal_nonce_is_not_ownership_proof(self):
        old = {"name": self.name, "code": 0, "launch_identity": self.nonce}
        j = self.job(spec={"name": self.name}, result=old)
        row = A.report(self.d)[0][0]
        self.assertNotIn("launch_identity", row)
        self.assertTrue(row["identity_untrusted"])
        self.assertEqual(json.loads((j / "done.json").read_text()), old)
        A._ack_job(j, self.nonce)
        self.assertTrue((j / "done.json").exists())

    def test_zero_exit_supervisor_before_payload_is_failure_not_success(self):
        # Explicit argv fault: a REAL child exits zero without ever becoming PID1.
        # This is not namespace acceptance; it exercises the early-exit production path.
        real = subprocess.Popen
        def exited(*args, **kwargs):
            p = real([sys.executable, "-c", "pass"], **kwargs)
            self.extra.append(p)
            p.wait(timeout=5)
            return p
        with mock.patch.object(A.subprocess, "Popen", side_effect=exited):
            A.start(self.d, self.spec())
        row = A.report(self.d)[0][0]
        self.assertEqual(row["launch_identity"], self.nonce)
        self.assertNotEqual(row["code"], 0)
        self.assertTrue(row["lost"])
        self.assertFalse(self.cancel()["ok"])

    def test_legacy_kill_apis_cannot_emit_unbound_strict_preemption(self):
        j = self.failure()
        (j / "done.json").unlink()
        (j / "pid").write_text("99999999")
        with mock.patch.object(A, "kill_tree", side_effect=AssertionError("unsafe legacy signal")):
            self.assertEqual(A.kill(self.batch)["killed"], 0)
            self.assertEqual(A.kill_item(self.batch, self.name)["killed"], 0)
        self.assertFalse((j / "done.json").exists())

    def test_old_strict_nonce_cannot_be_new_strict_admission(self):
        old_spec = self.spec() | {"launch_identity": "old-launch"}
        old_result = {"name": self.name, "code": 0, "launch_identity": "old-launch"}
        j = self.job(spec=old_spec, record={"identity": "old-launch", "state": "launched",
                     "contained": True, "reaped": True}, result=old_result)
        before = A._owned_path(self.batch, self.name).read_bytes()
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            A.start(self.d, self.spec())
        self.assertEqual(A._owned_path(self.batch, self.name).read_bytes(), before)
        self.assertEqual(json.loads((j / "done.json").read_text()), old_result)
        self.assertEqual(A.report(self.d)[0][0]["launch_identity"], "old-launch")

    def test_old_cancelled_nonce_cannot_silently_fence_new_start(self):
        self.receipt_ok(self.cancel())
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            A.start(self.d, self.spec() | {"launch_identity": "new-launch"})
        self.assertFalse((self.d / "jobs" / self.name).exists())

    def test_missing_terminal_identity_is_not_decorated_or_acked(self):
        j = self.failure()
        old = {"name": self.name, "code": 0, "seconds": 1}
        A.write_json(j / "done.json", old)
        row = A.report(self.d)[0][0]
        self.assertNotIn("launch_identity", row)
        self.assertTrue(row["identity_untrusted"])
        A.vtick({"acks": [{"batch": self.batch, "name": self.name, "launch_identity": self.nonce}], "wait": 0})
        self.assertEqual(json.loads((j / "done.json").read_text()), old)
        self.assertFalse((j / "acked.json").exists())

    def test_wrong_terminal_identity_is_not_relabelled_or_acked(self):
        j = self.failure()
        old = {"name": self.name, "code": 0, "launch_identity": "old-launch"}
        A.write_json(j / "done.json", old)
        self.assertNotIn("launch_identity", A.report(self.d)[0][0])
        A._ack_job(j, self.nonce)
        self.assertEqual(json.loads((j / "done.json").read_text()), old)

    def test_record_spec_mismatch_never_trusts_even_matching_terminal_nonce(self):
        j = self.failure()
        A._owned_write(A._owned_path(self.batch, self.name), {"identity": "old-launch", "state": "launched"})
        row = A.report(self.d)[0][0]
        self.assertNotIn("launch_identity", row)
        self.assertTrue(row["identity_untrusted"])
        A._ack_job(j, self.nonce)
        self.assertTrue((j / "done.json").exists())

    def test_start_failure_row_has_proven_nonce_and_tick_ack_requires_exact_identity(self):
        j = self.failure()
        row = A.report(self.d)[0][0]
        self.assertEqual(row["launch_identity"], self.nonce)
        self.assertNotEqual(row["code"], 0)
        for ack in (self.name, {"name": self.name}, {"name": self.name, "launch_identity": "wrong"},
                    {"batch": "other", "name": self.name, "launch_identity": self.nonce}):
            A.tick(self.batch, {"acks": [ack], "wait": 0})
            self.assertTrue((j / "done.json").exists())
        A.tick(self.batch, {"acks": [{"batch": self.batch, "name": self.name,
                                    "launch_identity": self.nonce}], "wait": 0})
        self.assertTrue((j / "acked.json").exists())

    def test_vtick_ack_requires_exact_identity(self):
        j = self.failure()
        for ack in ({"batch": self.batch, "name": self.name},
                    {"batch": self.batch, "name": self.name, "launch_identity": "wrong"}):
            A.vtick({"acks": [ack], "wait": 0})
            self.assertTrue((j / "done.json").exists())
        A.vtick({"acks": [{"batch": self.batch, "name": self.name, "launch_identity": self.nonce}], "wait": 0})
        self.assertTrue((j / "acked.json").exists())

    def test_lost_row_gets_nonce_only_from_matching_admission(self):
        j = self.failure()
        (j / "done.json").unlink()
        (j / "pid").write_text("99999999")
        row = A.report(self.d)[0][0]
        self.assertEqual(row["launch_identity"], self.nonce)
        self.assertTrue(row["lost"])
        self.assertNotEqual(row["code"], 0)

    def test_uncertain_existing_launch_is_collision_not_idempotence(self):
        self.job(record={"identity": self.nonce, "state": "launched", "contained": False})
        with self.assertRaisesRegex(ValueError, "not proven authorized"):
            A.start(self.d, self.spec())

    def test_legacy_ack_still_works_without_ownership_operations(self):
        j = self.job(spec={"name": self.name}, result={"name": self.name, "code": 0})
        with mock.patch.object(A, "_ownership_lock", side_effect=AssertionError("legacy flock")), \
             mock.patch.object(A, "_owned_write", side_effect=AssertionError("legacy fsync")):
            A.tick(self.batch, {"acks": [self.name], "wait": 0})
        self.assertTrue((j / "acked.json").exists())


class RealPrivateNamespace(OwnedFixture):
    """These are NOT mock acceptance tests and NOT optional/skip-on-failure."""
    def setUp(self):
        super().setUp()
        self.assertTrue(sys.platform.startswith("linux"), "Boat Linux is required; do not run on laptop")
        self.assertTrue(hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal"))
        self.repo = "owned-test-" + secrets.token_hex(8)
        self.src = pathlib.Path("/src") / self.repo
        subprocess.run(["sudo", "-n", "mkdir", "-p", "/work", str(self.src)], check=True, timeout=5)
        (self.d / "tree").mkdir()
        (self.d / "ready").write_text("ready")
        (self.d / "treepath").write_text(str(self.d / "tree"))
        (self.d / "ctx.json").write_text(json.dumps({"name": self.repo}))

    def tearDown(self):
        try:
            super().tearDown()
        finally:
            if hasattr(self, "src"):
                subprocess.run(["sudo", "-n", "rmdir", str(self.src)], check=True, timeout=5)

    def launch(self, cmd="sleep 300"):
        A.start(self.d, self.spec(cmd))
        entry = A._owned_children.get((self.batch, self.name))
        self.assertIsNotNone(entry, "strict supervisor failed to launch")
        self.assertTrue(entry.get("contained"), (self.d / "jobs" / self.name / "agent.err").read_text())
        return entry

    def test_real_cancel_kills_pid1_and_detached_grandchild(self):
        marker = self.root / "grandchild.ready"
        script = self.d / "tree" / "fork.py"
        script.write_text("import os,time,pathlib,json\n"
                          "if os.fork()==0:\n"
                          " os.setsid()\n"
                          " if os.fork()==0:\n"
                          f"  pathlib.Path({str(marker)!r}).write_text(json.dumps(sorted(os.sched_getaffinity(0))))\n"
                          "  time.sleep(300)\n"
                          " os._exit(0)\n"
                          "time.sleep(300)\n")
        entry = self.launch("python3 fork.py")
        self.assertEqual(json.loads(self.wait_file(marker)), sorted(os.sched_getaffinity(0)))
        # Open exact pidfds on this init's live owned descendants before cancellation. This
        # checks detached descendants are gone, not merely our Python parent / sudo wrapper.
        fds = []
        try:
            for pid in A.descendants(entry["init"]["pid"]):
                try:
                    fds.append(os.pidfd_open(pid))
                except ProcessLookupError:
                    pass
            self.assertGreaterEqual(len(fds), 2)
            self.receipt_ok(self.cancel())
            self.assertIsNotNone(entry["p"].returncode)
            for fd in fds:
                self.assertTrue(A._namespace_exited({"pidfd": fd}, 1), "owned descendant survived PID1 teardown")
            self.receipt_ok(self.cancel())
        finally:
            for fd in fds:
                os.close(fd)

    def test_real_natural_completion_reaper_preserves_receipt(self):
        entry = self.launch("true")
        entry["p"].wait(timeout=15)
        rows, running = A.report(self.d)
        self.assertEqual(running, [])
        self.assertEqual(rows[0]["code"], 0, rows)
        self.assertEqual(rows[0]["launch_identity"], self.nonce)
        # Reporting the result must not consume the parent's lifetime capabilities.
        self.assertIs(A._owned_children[(self.batch, self.name)], entry)
        os.fstat(entry["pidfd"])
        self.assertTrue(A._namespace_exited(entry, 1))
        A._reap_children()
        self.assertNotIn((self.batch, self.name), A._owned_children)
        self.receipt_ok(self.cancel())
        self.receipt_ok(self.cancel())

    def test_real_exact_nonce_repeat_is_idempotent_without_replacement(self):
        entry = self.launch()
        before = A._owned_path(self.batch, self.name).read_bytes()
        with mock.patch.object(A.subprocess, "Popen", side_effect=AssertionError("duplicate payload")):
            A.start(self.d, self.spec())
        self.assertIs(A._owned_children[(self.batch, self.name)], entry)
        self.assertEqual(A._owned_path(self.batch, self.name).read_bytes(), before)
        self.receipt_ok(self.cancel())

    def test_real_old_strict_success_refuses_new_nonce_but_exact_repeat_is_idempotent(self):
        entry = self.launch("true")
        entry["p"].wait(timeout=15)
        A._reap_children()
        with mock.patch.object(A.subprocess, "Popen", side_effect=AssertionError("replacement")):
            A.start(self.d, self.spec("true"))
            with self.assertRaisesRegex(ValueError, "identity mismatch"):
                A.start(self.d, self.spec("true") | {"launch_identity": "new-launch"})
        row = A.report(self.d)[0][0]
        self.assertEqual((row["code"], row["launch_identity"]), (0, self.nonce))
        A.vtick({"acks": [{"batch": self.batch, "name": self.name, "launch_identity": "new-launch"}], "wait": 0})
        self.assertTrue((self.d / "jobs" / self.name / "done.json").exists())
        A.vtick({"acks": [{"batch": self.batch, "name": self.name, "launch_identity": self.nonce}], "wait": 0})
        self.assertTrue((self.d / "jobs" / self.name / "acked.json").exists())
        self.receipt_ok(self.cancel())

    def test_real_strict_result_keeps_parent_pidfd_until_cancel_receipt(self):
        entry = self.launch("true")
        entry["p"].wait(timeout=15)
        row = A.report(self.d)[0][0]
        self.assertEqual((row["code"], row["launch_identity"]), (0, self.nonce))
        os.fstat(entry["pidfd"])
        self.assertIs(A._owned_children[(self.batch, self.name)], entry)
        self.receipt_ok(self.cancel())
        self.assertNotIn((self.batch, self.name), A._owned_children)
        with self.assertRaises(OSError):
            os.fstat(entry["pidfd"])

    def test_real_pre_payload_exit_is_failure_not_noop_success(self):
        # Explicit fault: production argv launches a genuine owned namespace, but
        # its PID1 hello is observed with a wrong nonce, so payload is never authorized.
        marker = self.root / "must-not-run"
        bind = A._bind_namespace
        def stale_hello(j, entry):
            raise ValueError("fault fixture: stale hello nonce")
        with mock.patch.object(A, "_bind_namespace", side_effect=stale_hello):
            A.start(self.d, self.spec(f"touch {marker}"))
        j = self.d / "jobs" / self.name
        self.assertFalse((j / "namespace.go").exists())
        self.assertFalse(marker.exists())
        row = A.report(self.d)[0][0]
        self.assertEqual(row["launch_identity"], self.nonce)
        self.assertNotEqual(row["code"], 0)
        self.assertTrue(row["lost"])
        # Recover the real pidfd while the un-authorized PID1 still awaits go;
        # this is cleanup, not a claim that the failed bind proved containment.
        entry = A._owned_children[(self.batch, self.name)]
        bind(j, entry)
        self.receipt_ok(self.cancel())

    def test_real_live_nonce_mismatch_does_not_signal(self):
        entry = self.launch()
        wrong = self.cancel(identity="reused-old-nonce")
        self.assertFalse(wrong["ok"])
        self.assertIsNone(entry["p"].poll())
        self.assertFalse(A._namespace_exited(entry))
        self.receipt_ok(self.cancel())

    def test_real_live_birth_mismatch_cannot_bind_reused_pid(self):
        entry = self.launch()
        real_birth = A._namespace_birth
        def reused_birth(pid):
            birth = real_birth(pid)
            birth["start_ticks"] += 1  # explicit stale/reused-birth observation fault
            return birth
        with mock.patch.object(A, "_namespace_birth", side_effect=reused_birth):
            with self.assertRaises(ValueError):
                A._bind_namespace(self.d / "jobs" / self.name, dict(entry))
        self.assertFalse(A._namespace_exited(entry))
        self.receipt_ok(self.cancel())

    def test_real_start_publication_gap_is_serialized(self):
        # Instrumentation wraps REAL Popen, not a fake process. While publication is held,
        # bounded cancel must not claim success or launch absence; retry fences and reaps.
        entered, release = threading.Event(), threading.Event()
        real = subprocess.Popen
        errors = []
        def gated(*args, **kwargs):
            p = real(*args, **kwargs)
            entered.set()
            if not release.wait(5):
                errors.append("test publication release timed out")
            return p
        def start():
            try:
                A.start(self.d, self.spec())
            except BaseException as e:
                errors.append(e)
        with mock.patch.object(A.subprocess, "Popen", side_effect=gated):
            thread = threading.Thread(target=start)
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertFalse(self.cancel(timeout=0.02)["ok"])
            finally:
                release.set()
                thread.join(10)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.receipt_ok(self.cancel())
        with mock.patch.object(A.subprocess, "Popen", side_effect=AssertionError("late launch")):
            A.start(self.d, self.spec())

    def test_real_namespace_with_fault_injected_wait_timeout_is_not_success(self):
        entry = self.launch()
        with mock.patch.object(A, "_namespace_exited", return_value=False):
            r = self.cancel(timeout=0)
        self.assertFalse(r["ok"])
        self.assertFalse(r["reaped"])
        self.assertFalse(r["namespace_absent"])
        self.receipt_ok(self.cancel())

    def test_real_restart_unknown_child_is_not_signaled(self):
        entry = self.launch()
        # Simulated loss of process-local capabilities, exactly what a fresh agent has.
        saved = A._owned_children
        A._owned_children = {}
        try:
            r = self.cancel()
            self.assertFalse(r["ok"])
            self.assertFalse(r["reaped"])
            self.assertFalse(A._namespace_exited(entry))
        finally:
            A._owned_children = saved
        self.receipt_ok(self.cancel())


if __name__ == "__main__":
    unittest.main(verbosity=2)
