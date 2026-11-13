"""Deferred handbacks remain durable until commit, without retrying hard failures."""
import ast
import pathlib
import tempfile
import unittest
from unittest import mock
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import canary, scheduler


class DeferredHandback(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.c = canary.Canary(self.root)
        for code in ("old", "new"):
            package = self.root / "code" / code / "supercomputer"
            package.mkdir(parents=True)
            (package / "scheduler.py").write_text("def _take_pool(self): pass\n")
        self.c.write({"from": "old", "to": "new", "state": "watching", "t": 100})

    def reject(self):
        with mock.patch.object(canary.time, "time", return_value=100):
            self.assertEqual(self.c.handoff_verdict("new", 1, 0, "one orphan")[0], "handback")

    def test_canary_rejection_survives_a_deferred_attempt_and_process_reload(self):
        self.reject()
        self.assertIsNone(self.c.handback_due("new", 114))
        self.assertEqual(self.c.handback_due("new", 115), "old")
        self.assertIsNone(self.c.handback_due("new", 120))
        reloaded = canary.Canary(self.root)
        self.assertEqual(reloaded.handback_due("new", 130), "old")
        self.assertEqual(reloaded.read()["state"], "rejected")

    def test_successor_does_not_repeat_its_parents_handback(self):
        self.reject()
        self.assertIsNone(self.c.handback_due("old", 1000))

    def test_hard_failure_stops_automatic_retry(self):
        self.reject()
        self.c.handoff_failed("old", "missing admission", False, 116)
        self.assertIsNone(self.c.handback_due("new", 1000))
        self.assertEqual(self.c.read()["pending_handback"]["failed"], "missing admission")

    def test_retryable_failure_backs_off_without_losing_intent(self):
        self.reject()
        self.c.handoff_failed("old", "deadline", True, 116)
        self.assertIsNone(self.c.handback_due("new", 175))
        self.assertEqual(self.c.handback_due("new", 176), "old")

    def test_manual_rollback_survives_deferred_attempt(self):
        with mock.patch.object(canary.time, "time", return_value=100):
            self.assertEqual(self.c.rollback_to("new", "old")[0], "old")
        self.assertEqual(self.c.handback_due("new", 115), "old")
        self.assertIsNone(self.c.handback_due("old", 130))

    def test_legacy_rejected_state_is_migrated_without_resetting_the_verdict(self):
        self.c.write({"from": "old", "to": "new", "state": "rejected"})
        self.assertEqual(self.c.handback_due("new", 100), "old")
        self.assertEqual(self.c.read()["state"], "rejected")

    def test_normal_new_handoff_supersedes_stale_intent(self):
        self.reject()
        self.c.handing_off("old", "new", 200)
        self.assertNotIn("pending_handback", self.c.read())
        self.assertIsNone(self.c.handback_due("new", 201))

    def test_missing_target_does_not_retry(self):
        self.reject()
        (self.root / "code" / "old" / "supercomputer" / "scheduler.py").unlink()
        (self.root / "code" / "old" / "supercomputer").rmdir()
        self.assertIsNone(self.c.handback_due("new", 1000))

    def test_sampler_dispatches_due_handbacks_before_normal_installs(self):
        tree = ast.parse(pathlib.Path(scheduler.__file__).read_text())
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Scheduler")
        method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "_sampler")
        due = [n for n in ast.walk(method) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
               and n.func.attr == "handback_due"]
        self.assertEqual(len(due), 1)
        self.assertTrue(any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
                            and n.func.attr == "handoff" and any(k.arg == "back_to" for k in n.keywords)
                            for n in ast.walk(method)))


if __name__ == "__main__":
    unittest.main()
