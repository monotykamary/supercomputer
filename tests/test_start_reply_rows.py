"""A start's reply carries other items' finished rows; importing one is never the starting item's failure.
2026-11-16 11:51: batch 20261116-115109-ibuvsx's strict item started on bx_36km3hj9 (start_response journaled), then
went LOST with "invalid result chunk receipt": another row's result import failed inside its start path (11-13
05:30 and 11-16 11:10 the same, as a boat strike against a non-strict item). The row stays unacknowledged on the
agent, for the poller's next tick. Run on boat, like all test_*.py."""
import time
import unittest
from unittest.mock import Mock, patch

import test_owned_scheduler as fixture
from test_wiring import S, batch, sched, vm


def _failing_import(*_a, **_k):
    raise ValueError("invalid result chunk receipt")


OTHER = {"batch": "other-batch", "name": "filler-1", "code": 0, "seconds": 1.0}


class StartReplyRows(unittest.TestCase):
    def test_a_legacy_start_survives_another_rows_failed_import(self):
        s = sched()
        b = batch(s)
        it = b.items["i0"]
        v = vm(s, "A")
        v.client = Mock()
        v.client.rpc.return_value = {"done": [OTHER]}
        s._prepare = lambda b, vm: None
        s._verify_agent_capabilities = lambda it, vm: None
        s._accept_finished = _failing_import
        with patch.object(S.threading, "Thread"):
            self.assertTrue(s._place(it, v))
        s._start_on_inner(it, v)
        self.assertEqual(it.state, "running")
        self.assertEqual(it.fails, 0)
        self.assertIn((b.id, it.run_name), v.running)

    def test_a_strict_start_is_not_lost_for_another_rows_failed_import(self):
        case = fixture.OwnedSchedulerTests("test_queued_cancel_closes_admission_before_placement")
        case.setUp()
        try:
            s, it, v = case.s, case.it, case.vm
            case.place_without_thread()
            s._strict_prepare = lambda it, vm: True
            s._attempt_audit = lambda vm, starts, event: True
            owned = Mock()
            owned.rpc.return_value = {"done": [OTHER]}
            s._owned_client = lambda vm: owned
            s._accept_finished = _failing_import
            it.state = "starting"
            s._start_on_inner(it, v)
            self.assertEqual(it.state, "running")
            self.assertFalse(any(r.get("status") == "LOST" for r in case.b.rows), case.b.rows)
        finally:
            case.doCleanups()


if __name__ == "__main__":
    unittest.main()
