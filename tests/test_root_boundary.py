"""Explicit roots never silently expand; isolated fixtures, run on native boat."""
import io
import os
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat, config


class RootBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="sc-root-boundary-")
        self.base = pathlib.Path(self.tmp.name).resolve()
        self.env = patch.dict(os.environ, {"SUPERCOMPUTER_LIVE": str(self.base / "live")})
        self.env.start()
        self.git_env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        self.old_project = config._current
        self.repo = self.base / "parent"
        self.repo.mkdir()
        self.git(self.repo, "init", "-q")
        self.nested = self.repo / "requested"
        self.nested.mkdir()
        (self.nested / "input.txt").write_text("small intended input")
        (self.repo / "unrelated.txt").write_text("must not widen to here")

    def tearDown(self):
        config._current = self.old_project
        self.env.stop()
        self.tmp.cleanup()

    def git(self, root, *args):
        return subprocess.run(["git", "-C", str(root), *args], env=self.git_env,
                              check=True, capture_output=True, text=True).stdout

    def test_explicit_nested_root_refuses_and_names_both_paths(self):
        with self.assertRaises(ValueError) as raised:
            config.use(self.nested)
        self.assertIn(str(self.nested), str(raised.exception))
        self.assertIn(str(self.repo), str(raised.exception))
        self.assertIs(config._current, self.old_project)

    def test_relative_explicit_root_also_refuses(self):
        with patch("os.getcwd", return_value=str(self.repo)):
            # pathlib resolves against the real process cwd, not os.getcwd's mock.
            relative = os.path.relpath(self.nested)
            with self.assertRaises(ValueError):
                config.use(relative)

    def test_implicit_cwd_keeps_enclosing_git_root(self):
        with patch("os.getcwd", return_value=str(self.nested)):
            self.assertEqual(config.use().root, self.repo)

    def test_explicit_git_root_is_accepted(self):
        self.assertEqual(config.use(self.repo).root, self.repo)

    def test_standalone_directory_stays_exact(self):
        standalone = self.base / "standalone"
        standalone.mkdir()
        self.assertEqual(config.use(standalone).root, standalone)

    def test_worktree_dot_git_file_is_accepted(self):
        self.git(self.repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
                 "commit", "--allow-empty", "-qm", "fixture")
        worktree = self.base / "worktree"
        self.git(self.repo, "worktree", "add", "--detach", str(worktree))
        self.assertTrue((worktree / ".git").is_file())
        self.assertEqual(config.use(worktree).root, worktree)

    def test_nested_gitignore_and_external_symlinks_not_followed(self):
        child = self.repo / "child"
        child.mkdir()
        self.git(child, "init", "-q")
        (child / ".gitignore").write_text("ignored.txt\n")
        (child / "ignored.txt").write_text("not shipped")
        (child / "included.txt").write_text("shipped")
        outside = self.base / "outside"
        outside.mkdir()
        (outside / "secret.txt").write_text("not shipped")
        (self.repo / "outside-link").symlink_to(outside, target_is_directory=True)
        (self.repo / "file-link").symlink_to(outside / "secret.txt")
        config.use(self.repo)
        files = boat.tree_files(self.repo)
        normalized = {str(pathlib.PurePosixPath(name)) for name in files}
        self.assertIn("child/included.txt", normalized)
        self.assertNotIn("child/ignored.txt", normalized)
        self.assertIn("outside-link", files)
        self.assertIn("file-link", files)
        self.assertFalse(any("secret.txt" in name for name in files))
        with tarfile.open(fileobj=io.BytesIO(boat.tree_tarball(self.repo, files))) as archive:
            self.assertTrue(archive.getmember("outside-link").issym())
            self.assertTrue(archive.getmember("file-link").issym())
            self.assertFalse(any("secret.txt" in name for name in archive.getnames()))


if __name__ == "__main__":
    # Fixture git invocations and code under test must never inherit a hook index.
    for key in list(os.environ):
        if key.startswith("GIT_"):
            os.environ.pop(key)
    unittest.main(verbosity=2)
