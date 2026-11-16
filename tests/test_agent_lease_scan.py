"""2026-11-16 bx_ekvw4s47 (agent 5dade6238156, up 31 h): every vtick held the lease lock through one pass over every
job under ~/.sc/b, and under a lease no batch directory was ever dropped: 4,523 of them, 14,155 jobs, ~2.5 s a pass,
the tick thread at 100% CPU. A strict cleanup's cancelitem takes the same lock with a short wait; it lost for 11 min.
Here: the scan runs outside the lock (a lease moved meanwhile still marks the answer stale), and retired batches'
directories go under a lease, leaving a tombstone of their owners. Run on boat, like all test_*.py."""
import json
import os
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import agent as A


class LeaseFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.now = 1000.0
        self.patches = [mock.patch.object(A, "ROOT", self.root),
                        mock.patch.object(A, "_lease_boot", lambda: "boot-a"),
                        mock.patch.object(A, "_lease_now", lambda: self.now),
                        mock.patch.object(A, "clean_disk", return_value={})]
        for p in self.patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self.tmp.cleanup)
        self.fence = A.lease("claim", {"owner": "a", "vm_id": "vm-a"})
        self.assertTrue(self.fence["ok"])

    def rpc(self, cmd, args, fence):
        return A.rpc_dispatch(cmd, [*args, {"_lease": fence}], cli=True)

    def job(self, batch="batch", name="item", owner="a", acked=False, strict=None, age_s=0):
        j = self.root / "b" / batch / "jobs" / name
        j.mkdir(parents=True, exist_ok=True)
        spec = {"name": name, "owner": owner}
        if strict is not None:
            spec.update(strict_once=True, launch_identity=strict["identity"])
            A._owned_write(A._owned_path(batch, name), {"identity": strict["identity"], "state": "launched", **strict["row"]})
        (j / "spec.json").write_text(json.dumps(spec))
        row = {"name": name, "code": 0, **({"launch_identity": strict["identity"]} if strict else {})}
        (j / ("acked.json" if acked else "done.json")).write_text(json.dumps(row))
        if age_s:
            t = time.time() - age_s
            for f in [j.parent.parent, j, *j.iterdir()]:
                os.utime(f, (t, t))
        return j


class ScanOutsideLeaseLock(LeaseFixture):
    def _blocked_vtick(self, fence):
        entered, release, out = threading.Event(), threading.Event(), {}
        real = A.report

        def report(d):
            entered.set()
            self.assertTrue(release.wait(5))
            return real(d)

        def tick():
            out["r"] = self.rpc("vtick", [{"wait": 0}], fence)
        self.job()
        with mock.patch.object(A, "report", side_effect=report):
            t = threading.Thread(target=tick)
            t.start()
            self.assertTrue(entered.wait(5))
            yield
            release.set()
            t.join(5)
        self.assertFalse(t.is_alive())
        yield out["r"]

    def test_a_cleanup_takes_the_lease_lock_while_a_vtick_scans(self):
        steps = self._blocked_vtick(self.fence)
        next(steps)
        try:
            with A.lease_lock(time.monotonic() + 0.5):  # (cancelitem's wait: before, "lease lock timed out")
                pass
        finally:
            r = next(steps)
        self.assertFalse(r.get("stale"))
        self.assertEqual(r["done"][0]["name"], "item")

    def test_a_lease_moved_during_the_scan_marks_the_answer_stale(self):
        steps = self._blocked_vtick(self.fence)
        next(steps)
        try:
            self.assertTrue(A.lease("release", self.fence)["ok"])
            self.assertTrue(A.lease("claim", {"owner": "b", "vm_id": "vm-a"})["ok"])
        finally:
            r = next(steps)
        self.assertTrue(r["stale"])

    def test_a_lost_row_is_judged_again_under_the_lock(self):
        j = self.root / "b" / "batch" / "jobs" / "item"
        j.mkdir(parents=True)
        (j / "spec.json").write_text(json.dumps({"name": "item", "owner": "a"}))
        (j / "pid").write_text("999999999")  # (gone: lost on the first read)
        real = A.lease_lock

        @A.contextlib.contextmanager
        def lock(deadline=None):
            with real(deadline):
                (j / "done.json").write_text(json.dumps({"name": "item", "code": 0}))  # (landed meanwhile)
                yield
        with mock.patch.object(A, "lease_lock", lock), A._lease_request(self.fence):
            done, running = A.report(j.parent.parent)
        self.assertEqual(done[0]["code"], 0)
        self.assertNotIn("lost", done[0])


class RetiredBatchesDropUnderALease(LeaseFixture):
    def drop(self):
        with mock.patch.object(A.subprocess, "Popen") as popen:
            A.drop_old_batches("current", keep_s=60)
        return popen

    def test_an_acked_quiet_batch_goes_and_leaves_its_owners(self):
        self.job(acked=True, age_s=120)
        popen = self.drop()
        self.assertFalse((self.root / "b" / "batch").exists())
        self.assertEqual(json.loads((self.root / "dropped-jobs" / "batch.json").read_text())["owners"], {"item": "a"})
        trash = pathlib.Path(popen.call_args.args[0][-1])
        self.assertTrue((trash / "batch" / "jobs" / "item").exists())
        self.assertEqual(popen.call_args.args[0][:4], ["sudo", "-n", "rm", "-rf"])

    def test_unretired_or_recent_or_current_batches_stay(self):
        self.job(batch="unacked", age_s=120)                       # (its owner hasn't taken it: survives a transfer)
        self.job(batch="recent", acked=True)
        self.job(batch="current", acked=True, age_s=120)
        self.job(batch="uncleaned", acked=True, age_s=120,         # (a strict cleanup not yet asked and answered)
                 strict={"identity": "n" * 32, "row": {"reaped": True}})
        self.drop()
        for b in ("unacked", "recent", "current", "uncleaned"):
            self.assertTrue((self.root / "b" / b / "jobs" / "item").exists(), b)

    def test_a_cleaned_strict_batch_goes_and_a_rebound_ask_still_proves_its_owner(self):
        nonce = "s" * 32
        self.job(acked=True, age_s=120, strict={"identity": nonce, "row": {"reaped": True, "cancelled": True, "contained": True}})
        self.drop()
        self.assertFalse((self.root / "b" / "batch").exists())
        mine = self.rpc("cancelitem", ["batch", "item", 0, nonce, True], self.fence)
        self.assertTrue(mine["ok"] and mine["reaped"] and mine["namespace_absent"], mine)
        A.lease("release", self.fence)
        other = A.lease("claim", {"owner": "b", "vm_id": "vm-a"})
        self.assertTrue(self.rpc("cancelitem", ["batch", "item", 0, nonce, True], other).get("stale"))

    def test_a_thousand_retired_batches_drain_a_bounded_number_a_call(self):
        for i in range(A.DROP_LEASED_MAX + 10):
            self.job(batch=f"b{i:04d}", acked=True, age_s=120)
        self.drop()
        self.assertEqual(len(list((self.root / "b").iterdir())), 10)
        self.drop()
        self.assertEqual(list((self.root / "b").iterdir()), [])


if __name__ == "__main__":
    unittest.main()
