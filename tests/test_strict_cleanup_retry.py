"""2026-11-16: six xlarge held whole for hours to days by cancelled strict items. A failed cancel RPC left one opaque
line a minute ("cancel RPC failed or timed out"), and a batch closed to admission whose client still polled was never
asked again: 262jtw's one cancel failed at 07:55 while its run was reaped by 08:01. Cleanup completes only on a bound,
matching receipt; ambiguous evidence still holds the slots. Run on boat, like all test_*.py."""
import json
import time
import unittest

import test_owned_scheduler as fixture
from supercomputer import scheduler as S


class StrictCleanupRetry(unittest.TestCase):
    setUp = fixture.OwnedSchedulerTests.setUp
    write_batch = fixture.OwnedSchedulerTests.write_batch
    place_without_thread = fixture.OwnedSchedulerTests.place_without_thread
    good_receipt = fixture.OwnedSchedulerTests.good_receipt

    def _cancels(self):
        path = self.root / "queue" / f"{self.b.id}.cancel"
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]

    def _sweep(self):
        self.s._t_abandon = 0
        self.s._abandon_tick()
        for _ in range(100):
            if not getattr(self.s, "_abandon_busy", False):
                return
            time.sleep(0.02)
        self.fail("cleanup sweep did not finish")

    def test_a_failed_cancel_rpc_keeps_its_cause(self):
        self.place_without_thread()
        self.vm.client.rpc.side_effect = RuntimeError("agent said no")
        self.assertFalse(self.s.cancel(self.b.id)["cleanup_complete"])
        error = self._cancels()[-1]["receipt"]["error"]
        self.assertIn("RuntimeError", error)
        self.assertIn("agent said no", error)

    def test_pending_cleanup_is_asked_again_and_completes_only_on_a_matching_receipt(self):
        self.place_without_thread()
        self.b.t_poll = time.time()  # (its client still polls: not abandoned)
        self.vm.client.rpc.side_effect = RuntimeError("timed out")
        self.assertFalse(self.s.cancel(self.b.id)["cleanup_complete"])
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        # Ambiguous evidence (another launch's receipt) holds the slots.
        self.vm.client.rpc.side_effect = None
        self.vm.client.rpc.return_value = {**self.good_receipt(), "identity": "another-launch"}
        self._sweep()
        self.assertFalse(self.it.owned.reaped)
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        # A bound, matching receipt completes it and frees the VM.
        self.vm.client.rpc.return_value = self.good_receipt()
        self._sweep()
        self.assertTrue(self.it.owned.reaped)
        self.assertNotIn((self.b.id, self.it.run_name), self.vm.running)
        self.assertTrue(self.s.close_receipt(self.b.id)["cleanup_complete"])
        args = self.vm.client.rpc.call_args
        self.assertEqual(args.args[0], "cancelitem")
        self.assertLessEqual(args.args[3], S.CLEANUP_RETRY_WAIT_S)

    def test_status_queues_nothing_of_a_batch_closed_to_admission(self):
        self.assertEqual(self.s._queued_by_class(), {self.b.klass: 1})
        self.b.admission_closed = True
        self.assertEqual(self.s._queued_by_class(), {})


if __name__ == "__main__":
    unittest.main()
