"""A scheduled batch's image: built before submission when missing (the scheduler never builds one), and one image per
repo for all of its worktrees (11-13: a new worktree's batches queued for an hour on an image nobody built)."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import boat, config


def git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                   env={**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"})


class RepoNameTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def test_a_worktree_is_its_main_checkouts_project(self):
        main = self.root / "my-repo"
        main.mkdir()
        git("init", "-q", cwd=main)
        git("commit", "-q", "--allow-empty", "-m", "x", cwd=main)
        git("worktree", "add", "-q", str(self.root / "feature-folder"), "-b", "f", cwd=main)
        self.assertEqual(config.repo_name(main), "my-repo")
        self.assertEqual(config.repo_name(self.root / "feature-folder"), "my-repo")
        p = config.Project(self.root / "feature-folder")
        self.assertEqual((p.name, p.prefix), ("my-repo", "myrepo"))

    def test_a_tree_outside_git_keeps_its_folder(self):
        d = self.root / "plain"
        d.mkdir()
        self.assertEqual(config.repo_name(d), "plain")

    def test_a_submodule_keeps_its_own_folder(self):
        sub = self.root / "lib"
        sub.mkdir()
        git("init", "-q", cwd=sub)
        with mock.patch.object(config.subprocess, "run", return_value=mock.Mock(stdout=str(self.root / "parent/.git/modules/lib") + "\n")):
            self.assertEqual(config.repo_name(sub), "lib")

    def test_a_configured_name_wins(self):
        d = self.root / "folder"
        d.mkdir()
        self.assertEqual(config.Project(d, {"name": "given"}).name, "given")


class ScheduledImageTests(unittest.TestCase):
    def session(self, image="r-img-abc"):
        s = object.__new__(boat.Scheduled)
        s.image, s._image_ok, s._image_lock = image, False, threading.Lock()
        return s

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = mock.patch.object(boat, "LIVE", Path(self.tmp.name))
        patcher.start()
        self.addCleanup(patcher.stop)
        for k in ("SUPERCOMPUTER_NO_BUILD", "GDPW_BOAT_NO_BUILD"):
            self.addCleanup(os.environ.pop, k, None)
            os.environ.pop(k, None)

    def test_a_missing_image_is_built_before_submission(self):
        s = self.session()
        with mock.patch.object(boat, "named_snapshots", return_value={}), \
             mock.patch.object(boat, "ensure_image", return_value="r-img-abc") as ensure, mock.patch.object(boat, "log"):
            s._image_for_scheduler()
        ensure.assert_called_once_with(True)
        self.assertTrue(s._image_ok)

    def test_a_ready_image_is_not_rebuilt_and_checked_once(self):
        s = self.session()
        with mock.patch.object(boat, "named_snapshots", return_value={"r-img-abc": {"status": "ready"}}) as snaps, \
             mock.patch.object(boat, "ensure_image") as ensure:
            s._image_for_scheduler()
            s._image_for_scheduler()
        ensure.assert_not_called()
        self.assertEqual(snaps.call_count, 1)

    def test_no_build_stops_the_batch_with_the_reason(self):
        s = self.session()
        os.environ["SUPERCOMPUTER_NO_BUILD"] = "1"
        with mock.patch.object(boat, "named_snapshots", return_value={}), mock.patch.object(boat, "ensure_image") as ensure:
            with self.assertRaisesRegex(boat.Unavailable, "supercomputer image --build"):
                s._image_for_scheduler()
        ensure.assert_not_called()

    def test_an_unreachable_api_leaves_it_to_the_scheduler(self):
        s = self.session()
        with mock.patch.object(boat, "named_snapshots", side_effect=RuntimeError("boat down")), \
             mock.patch.object(boat, "ensure_image") as ensure, mock.patch.object(boat, "log"):
            s._image_for_scheduler()
        ensure.assert_not_called()
        self.assertFalse(s._image_ok)

    def test_the_build_lock_serialises_two_builders(self):
        order = []

        def builder(tag):
            with boat.image_build_lock("r-img-abc"):
                order.append(f"{tag}+")
                order.append(f"{tag}-")
        ts = [threading.Thread(target=builder, args=(t,)) for t in "ab"]
        [t.start() for t in ts]
        [t.join(10) for t in ts]
        self.assertIn(order, (["a+", "a-", "b+", "b-"], ["b+", "b-", "a+", "a-"]))


if __name__ == "__main__":
    unittest.main()
