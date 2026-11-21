"""11-20 23:46: gate 8p33he's full-054 finished in 118 s on bx_p6avs5jw (agent current), but that VM's front answered
every request 502 "upstream unavailable": vtick went on over exec, resultpart had no fallback, and every tick threw
in the result import. The item stayed "running" for hours past its 1640 s limit; the batch sat at 68/69 until its
lane cancelled it by hand, and the hold outlived the cancel. The controller now bounds it whatever the worker does:
a finished row it can't take never blocks the VM's tick, an unheld or closed row is never imported, and a legacy
item with no result past its limit + OVERDUE_GRACE_S is failed on boat (queued again elsewhere), its VM quarantined
and reaped. No boat call is made (test_wiring). Run on boat, like all test_*.py."""
import pathlib
import tempfile
import time
import unittest
from unittest.mock import Mock, patch

from test_wiring import S, batch, sched, vm


class Deadline(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        p = patch.object(S, "DIR", pathlib.Path(tmp.name))
        p.start()
        self.addCleanup(p.stop)
        (pathlib.Path(tmp.name) / "queue").mkdir()
        self.s = sched()
        self.s.alerts = []
        self.s._start_on = lambda *a: None
        self.s._h = lambda: Mock()

    def placed(self, bid="b1", n=1, limit=100, strict=False):
        b = batch(self.s, bid, n=n)
        b.strict_once = strict
        for it in b.items.values():
            it.spec = {**it.spec, "limit": limit}
        v = vm(self.s, f"V-{bid}")
        v.client = Mock()
        v.lease = {"owner": S.boat.owner_id(), "vm_id": v.id, "boot": "test-boot", "epoch": 1}
        v.t_renew, v.t_agent = time.monotonic(), time.time()  # (a freshly verified lease, its agent just seen)
        for it in b.items.values():
            self.assertTrue(self.s._place(it, v))
            it.state = "running"
            it.start_done.set()
        return b, v

    def sweep(self, now):
        self.s._t_deadline = 0.0
        with patch.object(S.threading, "Thread") as thread:
            reaped = self.s._deadline_tick(now)
        return reaped, thread

    def test_a_finished_row_that_cannot_be_imported_never_breaks_the_tick(self):
        b, v = self.placed(n=2)
        rows = [{"batch": b.id, "name": "i0", "code": 0, "seconds": 1, "result_ref": {"version": 1, "format": "json", "bytes": 5, "sha256": "a" * 64}},
                {"batch": b.id, "name": "i1", "code": 0, "seconds": 1}]
        def rpc(cmd, *args, **kw):
            if cmd == "vtick":
                return {"done": rows, "running": []}
            if cmd == "resultpart":
                raise S.boat.AgentHTTPError(502, f"{v.id}: resultpart: 502 b'upstream unavailable\\n'")
            if cmd == "itemlogs":
                return {"out": "", "err": ""}
            raise AssertionError(cmd)
        v.client.rpc.side_effect = rpc
        v.t_inventory = time.time()
        self.assertEqual(self.s._tick(v), 5.0)  # (before: the exception escaped the tick, i1 never taken)
        self.assertEqual(b.items["i1"].state, "done")
        self.assertIn((b.id, "i0"), v.running)
        self.assertEqual(v.acks, [{"batch": b.id, "name": "i1"}])
        self.assertIn("502", b.items["i0"].result_error)

    def test_an_unheld_row_is_acknowledged_unread(self):
        b, v = self.placed()
        it = b.items["i0"]
        v.running.pop((b.id, it.run_name))
        v.client.rpc.side_effect = AssertionError("no import for a row nothing holds")
        self.assertTrue(self.s._finished_legacy(v, {"batch": b.id, "name": "i0", "code": 0,
                                                    "output_files_ref": {"version": 1, "format": "json", "bytes": 5, "sha256": "a" * 64}}))

    def test_a_closed_batch_row_releases_the_hold_unread(self):
        b, v = self.placed()
        b.closed = True
        b.items["i0"].state = "done"
        v.client.rpc.side_effect = AssertionError("no import for a closed batch")
        self.assertTrue(self.s._finished_legacy(v, {"batch": b.id, "name": "i0", "code": 0,
                                                    "row_ref": {"version": 1, "format": "json", "bytes": 5, "sha256": "a" * 64}}))
        self.assertEqual(v.running, {})

    def test_not_due_before_its_limit_and_grace(self):
        b, v = self.placed(limit=100)
        it = b.items["i0"]
        reaped, _ = self.sweep(it.t_start + 100 + S.OVERDUE_GRACE_S - 1)
        self.assertEqual(reaped, [])
        self.assertIn((b.id, it.run_name), v.running)
        self.assertFalse(v.quarantine)

    def test_past_its_deadline_failed_on_boat_queued_elsewhere_and_its_vm_reaped(self):
        b, v = self.placed(limit=100)
        it = b.items["i0"]
        it.result_error = "resultpart: 502 upstream unavailable"
        reaped, thread = self.sweep(it.t_start + 100 + S.OVERDUE_GRACE_S + 5)
        self.assertEqual(reaped, [v])
        self.assertEqual(v.running, {})
        self.assertEqual((it.state, it.vm, it.fails), ("queued", None, 1))
        self.assertTrue(v.quarantine.startswith("deadline: "))
        targets = [c.kwargs.get("target") for c in thread.call_args_list]
        self.assertIn(self.s._kill_run, targets)
        self.assertIn(self.s._lost_vm, targets)
        self.assertTrue(any("deadline" in a["msg"] for a in self.s.alerts))
        # never placed there again; another VM takes it
        self.assertFalse(S.vm_fits(S.fit_shape(it), v, set()))
        other = vm(self.s, "W")
        self.assertIs(self.s._slot(it), other)

    def test_a_successful_tick_keeps_a_deadline_quarantine(self):
        b, v = self.placed(limit=100)
        it = b.items["i0"]
        v.running[(b.id, "keep")] = batch(self.s, "strict").items["i0"]  # (a strict hold: no reap yet)
        v.running[(b.id, "keep")].batch.strict_once = True
        self.sweep(it.t_start + 100 + S.OVERDUE_GRACE_S + 5)
        self.assertTrue(v.quarantine)
        v.client.rpc.side_effect = lambda cmd, *a, **k: {"done": [], "running": []}
        self.s._tick(v)
        self.assertTrue(v.quarantine.startswith("deadline: "))

    def test_strict_items_keep_their_custody(self):
        b, v = self.placed(limit=100, strict=True)
        it = b.items["i0"]
        reaped, _ = self.sweep(it.t_start + 100 + S.OVERDUE_GRACE_S + 5)
        self.assertEqual(reaped, [])
        self.assertIn((b.id, it.run_name), v.running)

    def test_a_closed_batch_hold_is_released_past_its_deadline(self):
        b, v = self.placed(limit=100)
        it = b.items["i0"]
        b.closed, it.state = True, "done"  # (close(): marked done, its kill lost, the hold left)
        self.s.batches.pop(b.id)
        reaped, _ = self.sweep(it.t_start + 100 + S.OVERDUE_GRACE_S + 5)
        self.assertEqual(reaped, [v])
        self.assertEqual(v.running, {})
        self.assertEqual(it.state, "done")

    def test_terminal_after_its_boat_tries(self):
        b, v = self.placed(limit=100)
        it = b.items["i0"]
        for k in range(S.BOAT_TRIES):
            if k:
                w = vm(self.s, f"V{k}")
                w.client = Mock()
                self.assertTrue(self.s._place(it, w))
                it.state = "running"
                it.start_done.set()
            self.sweep(it.t_start + 100 + S.OVERDUE_GRACE_S + 5)
        self.assertEqual(it.state, "done")
        self.assertEqual(it.fails, S.BOAT_TRIES)


if __name__ == "__main__":
    unittest.main()
