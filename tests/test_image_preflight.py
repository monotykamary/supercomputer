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


class LayerAliasTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.addCleanup(os.environ.pop, "SUPERCOMPUTER_IMAGE", None)
        os.environ.pop("SUPERCOMPUTER_IMAGE", None)

    def _project(self, layers, *, prefix="gdpw", final_prefix="sc"):
        return config.Project(self.root, {
            "name": "gdp-wired",
            "boat": {"prefix": prefix, "final_prefix": final_prefix, "image_salt": "salt-v1",
                     "layers": [{"name": n, "script": s} for n, s in layers]},
        })

    def test_appended_layer_reuses_ready_final_prefix_parent(self):
        base_layers = [("tools", "echo tools"), ("langs", "echo langs")]
        prior = self._project(base_layers)
        prior_chain = boat.layers(prior)
        prior_top = prior_chain[-1][1]
        self.assertTrue(prior_top.startswith("sc-"))

        extended = self._project([*base_layers, ("node", "echo node")])
        chain = boat.layers(extended)
        have = {
            prior_chain[0][1]: {"status": "ready"},
            prior_top: {"status": "ready"},
        }
        self.assertEqual(boat.ready_layer_snapshot(chain[1][1], have), prior_top)
        self.assertIsNone(boat.ready_layer_snapshot(chain[2][1], have))

        with mock.patch.object(boat, "project", return_value=extended), \
                mock.patch.object(boat, "named_snapshots", return_value=have), \
                mock.patch.object(boat, "build_layer") as build_layer, \
                mock.patch.object(boat, "prune_snapshots") as prune:
            built = boat.ensure_image(True)

        self.assertEqual(built, chain[-1][1])
        build_layer.assert_called_once_with("node", chain[-1][1], "echo node", prior_top)
        self.assertIn(prior_top, prune.call_args.args[0])

    def test_legacy_prefix_alias_remains_supported_when_final_prefix_absent(self):
        base_layers = [("tools", "echo tools"), ("langs", "echo langs")]
        extended = self._project([*base_layers, ("node", "echo node")])
        chain = boat.layers(extended)
        legacy_alias = next(a for a in boat.layer_aliases(chain[1][1]) if a.startswith("gdpw-"))
        have = {
            chain[0][1]: {"status": "ready"},
            legacy_alias: {"status": "ready"},
        }
        with mock.patch.object(boat, "project", return_value=extended), \
                mock.patch.object(boat, "named_snapshots", return_value=have), \
                mock.patch.object(boat, "build_layer") as build_layer, \
                mock.patch.object(boat, "prune_snapshots"):
            boat.ensure_image(True)

        build_layer.assert_called_once_with("node", chain[-1][1], "echo node", legacy_alias)

    def test_no_build_reports_only_missing_top_layer_when_parent_alias_is_ready(self):
        base_layers = [("tools", "echo tools"), ("langs", "echo langs")]
        prior_chain = boat.layers(self._project(base_layers))
        extended = self._project([*base_layers, ("node", "echo node")])
        chain = boat.layers(extended)
        have = {
            prior_chain[0][1]: {"status": "ready"},
            prior_chain[1][1]: {"status": "ready"},
            chain[1][1]: {"status": "failed"},
        }
        with mock.patch.object(boat, "project", return_value=extended), \
                mock.patch.object(boat, "named_snapshots", return_value=have):
            with self.assertRaisesRegex(boat.Unavailable, chain[-1][1]):
                boat.ensure_image(False)

    def test_image_cli_reports_ready_for_aliased_final_prefix_parent(self):
        import io
        from supercomputer import __main__ as cli
        base_layers = [("tools", "echo tools"), ("langs", "echo langs")]
        prior_chain = boat.layers(self._project(base_layers))
        extended = self._project([*base_layers, ("node", "echo node")])
        chain = boat.layers(extended)
        have = {
            prior_chain[0][1]: {"status": "ready"},
            prior_chain[1][1]: {"status": "ready"},
        }
        buf = io.StringIO()
        with mock.patch.object(config, "use", return_value=extended), \
                mock.patch.object(boat, "project", return_value=extended), \
                mock.patch.object(boat, "named_snapshots", return_value=have), \
                mock.patch.object(sys, "stdout", buf):
            cli.main(["image"])
        lines = [line.split() for line in buf.getvalue().strip().splitlines()]
        self.assertEqual(lines, [
            ["tools", prior_chain[0][1], "ready"],
            ["langs", prior_chain[1][1], "ready"],
            ["node", chain[2][1], "missing"],
        ])


if __name__ == "__main__":
    unittest.main()
