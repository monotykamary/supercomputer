"""11-16 13:15-13:19: treadmill filler batches' submits were answered only after the client's timeout (the request-lock
stall); the client's retry came once the batch had finished, and replaced it: 128 items run again, and the new admission
inherited the old one's .rows, so a reload (both successors at 13:44) read 19 running items as done and refused
readiness. An identical retry keeps the batch; a real replacement starts its own journals. Run on boat."""
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from test_wiring import S, sched
from supercomputer import rules


def msg(bid, names):
    return {"batch": bid, "klass": 5, "client": "treadmill", "image": "img", "tree": {"id": "t", "files": {}, "links": {}},
            "ctx_b64": "", "items": [{"name": n, "kind": "sh", "cmd": "true"} for n in names]}


class Resubmit(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = pathlib.Path(self.tmp.name)
        for target, value in ((S, "DIR"), (S, "BLOBS")):
            p = patch.object(target, value, root / value.lower())
            p.start()
            self.addCleanup(p.stop)
        (root / "dir" / "queue").mkdir(parents=True)
        (root / "blobs").mkdir()
        self.q = root / "dir" / "queue"
        self.s = sched()
        self.s._pin_batch_image = lambda b: None
        self.s._validate_submission = lambda m: None

    def finish_all(self, bid):
        b = self.s.batches[bid]
        for it in b.items.values():
            it.state = "done"
            b.deliver({"name": it.name, "code": 0, "seconds": 1})

    def test_the_rule_keeps_an_identical_retry_live_or_done(self):
        for live in (True, False):
            r = rules.Submit()
            r.exists, r.live = True, live
            self.assertEqual(r.admit(True), "keep")
        r = rules.Submit()
        r.exists, r.live = True, False
        self.assertEqual(r.admit(False), "replace")

    def test_an_identical_retry_of_a_finished_batch_keeps_it_and_its_rows(self):
        self.assertTrue(self.s._submit_locked(msg("b1", ["a", "b"]))["ok"])
        first = self.s.batches["b1"]
        self.finish_all("b1")
        r = self.s._submit_locked(msg("b1", ["a", "b"]))
        self.assertTrue(r.get("kept"), r)
        self.assertIs(self.s.batches["b1"], first)
        self.assertEqual(len((self.q / "b1.rows").read_text().splitlines()), 2)

    def test_a_replacement_starts_its_own_journals(self):
        self.assertTrue(self.s._submit_locked(msg("b2", ["a", "b"]))["ok"])
        self.finish_all("b2")
        self.assertTrue(self.s._submit_locked(msg("b2", ["c"]))["ok"])
        self.assertFalse((self.q / "b2.rows").exists())  # (a reload reads none of the old rows as the new items')
        self.assertEqual(len(list(self.q.glob("b2.replaced-*.rows"))), 1)


if __name__ == "__main__":
    unittest.main()
