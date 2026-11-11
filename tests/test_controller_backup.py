"""Isolated client backup checks (no scheduler/provider): python3 tests/test_controller_backup.py."""
import json
import multiprocessing
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import config, controller, scheduler
from supercomputer.rules import ops

NOW = 10000.0


def contender(live, start, release, entered, completed, count):
    """Spawned clients share only filesystem, fixed time, and test synchronization."""
    def request(*args, **kwargs):
        record = json.loads((pathlib.Path(live) / "controller-backup.json").read_text())
        assert record["t_attempt"] == NOW
        with count.get_lock():
            count.value += 1
        entered.set()
        assert release.wait(10), "test failed to release download"
        raise TimeoutError("simulated transport failure")

    with patch.object(scheduler.Client, "__init__", return_value=None), \
         patch.object(config, "LIVE", pathlib.Path(live)), \
         patch.object(controller.time, "time", return_value=NOW), \
         patch.object(controller, "_backup_request", side_effect=request):
        start.wait(10)
        controller.backup()
        controller.backup()  # Must skip on failure as well as while in-flight.
    completed.put(True)


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.live = pathlib.Path(self.tmp.name)
        self.patch_live = patch.object(config, "LIVE", self.live)
        self.patch_live.start()
        self.addCleanup(self.patch_live.stop)
        # No forwarding, token lookup, or provider contact in these isolated tests.
        client_init = patch.object(scheduler.Client, "__init__", return_value=None)
        client_init.start()
        self.addCleanup(client_init.stop)
        self.metadata = {"url": "test-only", "vm": "fixture", "t_check": NOW,
                         "t_backup": 0, "main": "preserve", "extra": "preserve"}
        self.meta = self.live / "controller.json"
        self.meta.write_text(json.dumps(self.metadata))

    def record(self):
        return json.loads((self.live / "controller-backup.json").read_text())

    def test_processes_single_flight_and_failed_attempt_cooldown(self):
        ctx = multiprocessing.get_context("spawn")
        n = 5
        start, release, entered = ctx.Barrier(n), ctx.Event(), ctx.Event()
        completed, count = ctx.Queue(), ctx.Value("i", 0)
        children = [ctx.Process(target=contender, args=(str(self.live), start, release,
                                                       entered, completed, count)) for _ in range(n)]
        try:
            for child in children:
                child.start()
            self.assertTrue(entered.wait(10))
            # All losing clients finish while the winning HTTP call is blocked.
            for _ in range(n - 1):
                self.assertTrue(completed.get(timeout=10))
            self.assertEqual(count.value, 1)
            self.assertEqual(self.record()["t_attempt"], NOW)
            release.set()
            self.assertTrue(completed.get(timeout=10))
            for child in children:
                child.join(10)
                self.assertEqual(child.exitcode, 0)
            self.assertEqual(count.value, 1)
            self.assertNotIn("t_success", self.record())
        finally:
            release.set()
            for child in children:
                if child.pid is not None:
                    child.join(2)
                    if child.is_alive():
                        child.terminate()  # Only the exact child created above.
                        child.join(2)
            completed.close()
            completed.join_thread()

    def test_failure_records_attempt_before_call_and_preserves_metadata(self):
        def fail(*args, **kwargs):
            self.assertEqual(self.record()["t_attempt"], 1)
            raise TimeoutError("never logged")
        with patch.object(controller.time, "time", return_value=1), \
             patch.object(controller, "_backup_request", side_effect=fail) as req:
            controller.backup()
            controller.backup()
            self.assertEqual(req.call_count, 1)
        self.assertEqual(json.loads(self.meta.read_text()), self.metadata)
        self.assertNotIn("t_success", self.record())

    def test_maybe_sync_returns_while_backup_blocked_and_force_skips(self):
        entered, release, returned, done = (threading.Event() for _ in range(4))
        errors = []
        real_backup = controller.backup

        def request(*args, **kwargs):
            entered.set()
            if not release.wait(5):
                raise TimeoutError("test download not released")
            return b"archive"

        def backup(*args, **kwargs):
            try:
                real_backup(*args, **kwargs)
            finally:
                done.set()

        def invoke():
            try:
                controller.maybe_sync()
            except BaseException as e:
                errors.append(e)
            finally:
                returned.set()

        with patch.object(controller.time, "time", return_value=NOW), \
             patch.object(scheduler, "controller", return_value={"url": "fixture"}), \
             patch.object(controller, "sync_git") as sync, \
             patch.object(controller, "boat_ok") as key, \
             patch.object(controller, "rekey") as rekey, \
             patch.object(controller, "backup", side_effect=backup), \
             patch.object(controller, "_backup_request", side_effect=request) as req:
            caller = threading.Thread(target=invoke)
            try:
                caller.start()
                self.assertTrue(entered.wait(3))
                self.assertTrue(returned.wait(1), "maybe_sync waited for backup")
                self.assertFalse(done.is_set())
                real_backup(force=True)
                self.assertEqual(req.call_count, 1)
                sync.assert_called_once_with()
                key.assert_not_called()
                rekey.assert_not_called()
                self.assertEqual(errors, [])
            finally:
                release.set()
                caller.join(5)
                self.assertTrue(done.wait(5))

    def test_first_attempt_at_zero_and_archive_retention(self):
        archives = self.live / "controller-state"
        archives.mkdir()
        for n in range(7):
            (archives / f"0000-{n}.tgz").write_bytes(b"old")
        with patch.object(controller.time, "time", return_value=0), \
             patch.object(controller, "_backup_request", return_value=b"new") as req:
            controller.backup()
            controller.backup()
            self.assertEqual(req.call_count, 1)
            self.assertEqual(req.call_args.args[0], self.metadata)
            self.assertIsInstance(req.call_args.args[1], int)
            controller.backup(force=True)
            self.assertEqual(req.call_count, 2)
        kept = sorted(archives.glob("*.tgz"))
        self.assertEqual(len(kept), 5)
        self.assertEqual(sum(p.read_bytes() == b"new" for p in kept), 2)
        self.assertEqual(self.record(), {"t_attempt": 0, "t_success": 0})
        self.assertEqual(json.loads(self.meta.read_text()), self.metadata)

    def test_request_subprocess_has_total_deadline_and_no_secrets_in_argv(self):
        with patch.object(controller.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, b"archive")) as run:
            self.assertEqual(controller._backup_request({"token": "test-secret"}, 123), b"archive")
        args, kwargs = run.call_args
        self.assertNotIn("test-secret", " ".join(args[0]))
        self.assertEqual(json.loads(kwargs["input"]), [{"token": "test-secret"}, controller.BACKUP_TIMEOUT])
        self.assertGreater(kwargs["timeout"], 0)
        self.assertLessEqual(kwargs["timeout"], 30)
        self.assertTrue(kwargs["capture_output"])
        self.assertTrue(kwargs["check"])
        self.assertEqual(kwargs["pass_fds"], (123,))
        self.assertIn("signal.setitimer(signal.ITIMER_REAL, timeout)", controller._BACKUP_REQUEST_CODE)
        self.assertIn("req('/state', None", controller._BACKUP_REQUEST_CODE)
        self.assertIn("raw=True, body=b''", controller._BACKUP_REQUEST_CODE)

    def test_total_deadline_kills_and_reaps_blocked_request_child(self):
        # Actual subprocess timeout, with no network: simulate a client hung in
        # initialization/retries/read. Do not leave a thread or child running.
        children = []
        real_popen = subprocess.Popen

        def capture(*args, **kwargs):
            child = real_popen(*args, **kwargs)
            children.append(child)
            return child

        with patch.object(controller, "BACKUP_TIMEOUT", 0.1), \
             patch.object(controller, "_BACKUP_REQUEST_CODE", "import time; time.sleep(60)"), \
             patch.object(controller.subprocess, "Popen", side_effect=capture):
            with self.assertRaises(subprocess.TimeoutExpired):
                controller._backup_request(self.metadata)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].poll(), "timeout left its child running")
        # The production backup suppresses this same timeout and keeps cooldown.
        with patch.object(controller, "_backup_request", side_effect=subprocess.TimeoutExpired("request", 0.1)) as req:
            controller.backup()
            controller.backup()
            self.assertEqual(req.call_count, 1)
        self.assertNotIn("t_success", self.record())

    def test_scheduler_unit_raises_soft_limit_for_future_starts(self):
        unit = controller.unit("scheduler", "python3 -m supercomputer scheduler serve", "/tmp/fixture", {})
        self.assertIn("LimitNOFILE=524288:524288\n", unit)
        self.assertNotIn("LimitNOFILESoft=", unit)  # systemd show property, not a unit directive

    def test_stage_persists_nofile_without_service_restart(self):
        with patch.object(scheduler.Client, "req", return_value={"hot_handoff": 1}), \
             patch.object(scheduler, "code_hash", return_value="candidate"), \
             patch.object(controller, "tool_tar", return_value=b"tar"), \
             patch.object(controller, "ssh", return_value=(0, '{"active":"old","staged":"candidate"}')) as ssh:
            controller.stage("fixture")
        command = ssh.call_args.args[1]
        self.assertIn("LimitNOFILE=524288:524288", command)
        self.assertIn("supercomputer-scheduler.service.d/10-nofile.conf", command)
        self.assertIn("systemctl daemon-reload", command)
        for forbidden in ("restart", "kill", "systemctl stop", "enable --now"):
            self.assertNotIn(forbidden, command)

    def test_attempt_rule_boundaries(self):
        self.assertTrue(ops.backup_attempt_due(True, False, False, 0, 0, False))
        self.assertFalse(ops.backup_attempt_due(True, False, True, 3599, 0, False))
        self.assertTrue(ops.backup_attempt_due(True, False, True, 3600, 0, False))
        self.assertFalse(ops.backup_attempt_due(True, True, False, 0, 0, True))
        self.assertTrue(ops.backup_attempt_due(True, False, True, 0, 0, True))
        self.assertFalse(ops.backup_attempt_due(False, False, False, 0, 0, True))


if __name__ == "__main__":
    unittest.main()
