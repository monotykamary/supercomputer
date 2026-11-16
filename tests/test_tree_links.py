"""2026-11-16 bx_rejj3ega and bx_pp9ejjmq: the empty file's blob reached ext4's 65,000 hard links (a tree of 72,174
empty files, 114 kept trees); every tree with an empty file then failed EMLINK in make_tree, and two strict batches
went LOST as owned_dispatch_refused. Here: an empty file is a plain file of its own, and a blob at its link limit is
copied. The saturation case makes real links until the filesystem refuses one. Run on boat, like all test_*.py."""
import errno
import hashlib
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import agent as A

LIMIT = 70000  # (ext4's is 65,000; a filesystem with none above this is reported, the link step skipped)


class TreeLinks(unittest.TestCase):
    def setUp(self):
        # (under the home, the disk the agent's ~/.sc is on; /tmp may be a tmpfs with no link limit)
        self.tmp = tempfile.TemporaryDirectory(dir=os.path.expanduser("~"))
        self.addCleanup(self.tmp.cleanup)
        root = pathlib.Path(self.tmp.name)
        for name, value in (("ROOT", root), ("BLOBS", root / "blobs"), ("TREES", root / "trees")):
            p = mock.patch.object(A, name, value)
            p.start()
            self.addCleanup(p.stop)
        A.BLOBS.mkdir(parents=True)
        A.TREES.mkdir(parents=True)
        self.gc = mock.patch.object(A, "gc_trees")
        self.gc.start()
        self.addCleanup(self.gc.stop)

    def blob(self, data: bytes, x=False) -> str:
        key = hashlib.sha256(data).hexdigest() + ("x" if x else "")
        (A.BLOBS / key).write_bytes(data)
        os.chmod(A.BLOBS / key, 0o755 if x else 0o644)
        return key

    def tree(self, files):
        tid = A.tree_id(files, {})
        return tid, A.make_tree(tid, files, {})

    def test_empty_files_never_link_the_empty_blob(self):
        empty, script = self.blob(b""), self.blob(b"", x=True)
        tid, r = self.tree({f"e{i}": empty for i in range(50)} | {"run.sh": script})
        self.assertTrue(r["ok"], r)
        self.assertEqual(os.stat(A.BLOBS / empty).st_nlink, 1)
        self.assertEqual((A.TREES / tid / "e7").read_bytes(), b"")
        self.assertTrue(os.access(A.TREES / tid / "run.sh", os.X_OK))

    def test_a_tree_made_on_a_saturated_blob_copies_it(self):
        key = self.blob(b"shared module\n")
        hold = pathlib.Path(self.tmp.name) / "hold"
        hold.mkdir()
        n = 1
        try:
            while n < LIMIT:
                os.link(A.BLOBS / key, hold / str(n))
                n += 1
            self.skipTest(f"no link limit below {LIMIT} on this filesystem")
        except OSError as e:
            self.assertEqual(e.errno, errno.EMLINK)
        tid, r = self.tree({"a.py": key, "b/c.py": key})
        self.assertTrue(r["ok"], r)
        self.assertEqual((A.TREES / tid / "b" / "c.py").read_bytes(), b"shared module\n")

    def test_emlink_falls_back_to_a_copy_and_other_errors_still_fail(self):
        key = self.blob(b"x = 1\n")
        with mock.patch.object(A.os, "link", side_effect=OSError(errno.EMLINK, "Too many links")):
            tid, r = self.tree({"m.py": key})
        self.assertTrue(r["ok"], r)
        self.assertEqual((A.TREES / tid / "m.py").read_bytes(), b"x = 1\n")
        with mock.patch.object(A.os, "link", side_effect=OSError(errno.EXDEV, "cross-device")):
            with self.assertRaises(OSError):
                self.tree({"n.py": key})

    def test_a_missing_empty_blob_is_asked_for_again(self):
        files = {"e": A.EMPTY}
        tid = A.tree_id(files, {})
        self.assertEqual(A.make_tree(tid, files, {}), {"ok": False, "missing": [A.EMPTY]})


if __name__ == "__main__":
    unittest.main()
