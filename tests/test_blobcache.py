"""Cache safety and admission regressions; isolated files, no provider calls."""
import hashlib
import io
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import time
import types
import unittest
from unittest.mock import patch

# Hooks inherit the parent repository's index. Never let fixture git commands
# operate on it, even with git -C pointing at a temporary repository.
for name in ("GIT_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY"):
    os.environ.pop(name, None)
os.environ["SUPERCOMPUTER_LIVE"] = tempfile.mkdtemp(prefix="sc-cache-tests-")
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import blobcache, boat, config, scheduler


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = pathlib.Path(self.tmp.name)
        self.root = self.base / "blobs"
        self.root.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def blob(self, data, age=7200):
        key = hashlib.sha256(data).hexdigest()
        blobcache.put(self.root, key, data)
        stamp = time.time() - age
        os.utime(self.root / key, (stamp, stamp))
        return key

    def queue(self, key, by_id=False):
        (self.base / "queue").mkdir(exist_ok=True)
        ident = "a" * 24
        tree = {"id": ident, "files": {"input": key}, "links": {}}
        if by_id:
            (self.base / "trees").mkdir(exist_ok=True)
            (self.base / "trees" / f"{ident}.json").write_text(json.dumps(tree))
            tree = {"id": ident}
        (self.base / "queue" / "job.json").write_text(json.dumps({"tree": tree}))

    def prune(self, budget=0, **kw):
        return blobcache.collect(self.root, budget, 3600, 86400, **kw)

    def test_budget_reclaims_oldest_to_target(self):
        old = self.blob(b"older", 8000)
        new = self.blob(b"newer", 7000)
        result = self.prune(5)
        self.assertFalse((self.root / old).exists())
        self.assertTrue((self.root / new).exists())
        self.assertEqual(result["after_bytes"], 5)
        self.assertEqual(result["over_budget_bytes"], 0)

    def test_pins_survive_pressure_and_expiry(self):
        key = self.blob(b"needed", 100000)
        self.queue(key)
        result = self.prune()
        self.assertTrue((self.root / key).exists())
        self.assertEqual(result["pinned_bytes"], 6)
        self.assertEqual(result["over_budget_bytes"], 6)

    def test_id_only_queue_pins(self):
        key = self.blob(b"needed", 100000)
        self.queue(key, by_id=True)
        self.prune()
        self.assertTrue((self.root / key).exists())

    def test_recent_upload_survives(self):
        key = self.blob(b"upload", 10)
        self.assertEqual(self.prune()["removed_files"], 0)
        self.assertTrue((self.root / key).exists())

    def test_have_renews_old_blob_before_submit(self):
        key = self.blob(b"reused")
        self.assertEqual(blobcache.have(self.root, [key]), [])
        self.assertEqual(self.prune()["removed_files"], 0)

    def test_read_renews_old_blob(self):
        key = self.blob(b"read")
        self.assertEqual(blobcache.read(self.root, key), b"read")
        self.assertEqual(self.prune()["removed_files"], 0)

    def test_expiry_below_budget(self):
        key = self.blob(b"expired", 100000)
        self.assertEqual(self.prune(100)["removed_files"], 1)
        self.assertFalse((self.root / key).exists())

    def test_dry_run_does_not_delete_or_overwrite_report(self):
        key = self.blob(b"old")
        result = self.prune(dry_run=True)
        self.assertEqual(result["selected_bytes"], 3)
        self.assertEqual(result["removed_bytes"], 0)
        self.assertTrue((self.root / key).exists())
        self.assertFalse((self.base / "blob-cache.json").exists())

    def test_malformed_queue_fails_closed(self):
        key = self.blob(b"retain")
        (self.base / "queue").mkdir()
        (self.base / "queue" / "broken.json").write_text("{")
        self.assertIn("error", self.prune())
        self.assertTrue((self.root / key).exists())

    def test_missing_id_manifest_fails_closed(self):
        key = self.blob(b"retain")
        self.queue(key, by_id=True)
        (self.base / "trees" / (("a" * 24) + ".json")).unlink()
        self.assertIn("error", self.prune())
        self.assertTrue((self.root / key).exists())

    def test_symlinks_and_temporary_files_are_never_evicted(self):
        target = self.base / "target"
        target.write_bytes(b"safe")
        (self.root / ("b" * 64)).symlink_to(target)
        (self.root / ".upload-old").write_bytes(b"partial")
        self.prune()
        self.assertEqual(target.read_bytes(), b"safe")
        self.assertTrue((self.root / ("b" * 64)).is_symlink())
        self.assertTrue((self.root / ".upload-old").exists())

    def test_invalid_policy_and_key_are_refused(self):
        for args in [(-1, 3600, 86400), (0, 0, 86400), (0, 3600, 60)]:
            with self.assertRaises(ValueError):
                blobcache.collect(self.root, *args)
        with self.assertRaises(ValueError):
            blobcache.have(self.root, ["../outside"])
        with self.assertRaises(ValueError):
            blobcache.put(self.root, "a" * 64, b"not that hash")

    def test_cancelled_collection_keeps_files(self):
        key = self.blob(b"retain")
        self.assertTrue(self.prune(cancelled=lambda: True)["cancelled"])
        self.assertTrue((self.root / key).exists())

    def test_known_remote_tree_rechecks_and_reuploads_evicted_blob(self):
        data = b"source"
        key = hashlib.sha256(data).hexdigest()
        client = scheduler.Client.__new__(scheduler.Client)
        client.remote = True
        client._known = lambda: {"trees": ["known"]}
        client._know = lambda *args: None
        calls = []
        def req(path, msg, **kw):
            calls.append(path)
            if path == "/blobs/have":
                return {"missing": [key]}
            if path == "/blobs/put":
                with tarfile.open(fileobj=io.BytesIO(kw["body"])) as archive:
                    self.assertEqual(archive.extractfile(key).read(), data)
            return {"ok": True}
        client.req = req
        tree = types.SimpleNamespace(id="known", files={"file": key}, links={}, read=lambda _: data)
        self.assertEqual(client.push_tree(tree), 1)
        self.assertEqual(calls, ["/blobs/have", "/blobs/put", "/tree/put"])

    def test_implicit_generated_inputs_excluded_but_tracked_and_explicit_kept(self):
        repo = self.base / "repo"
        repo.mkdir()
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        (repo / "main.py").write_text("pass\n")
        (repo / ".verification").mkdir()
        (repo / ".verification" / "source.tar").write_bytes(b"generated")
        (repo / ".verification" / "fixture.txt").write_text("tracked fixture")
        subprocess.run(["git", "-C", str(repo), "add", "main.py", ".verification/fixture.txt"], check=True)
        files = boat.tree_files(repo, ignore=[])
        self.assertNotIn(".verification/source.tar", files)
        self.assertIn(".verification/fixture.txt", files)
        self.assertIn("main.py", files)
        project = config.Project(repo, {"boat": {"ignore": []}})
        with patch.object(boat, "project", return_value=project):
            tree = boat.Tree(repo, data={"evidence": repo / ".verification"})
        self.assertIn("evidence/source.tar", tree.files)
        self.assertNotIn(".verification/source.tar", tree.files)

    def test_os_lock_coordinates_separate_processes(self):
        data = b"child upload"
        key = hashlib.sha256(data).hexdigest()
        code = "import pathlib,sys; from supercomputer import blobcache; blobcache.put(pathlib.Path(sys.argv[1]),sys.argv[2],b'child upload')"
        with blobcache.locked(self.root):
            p = subprocess.Popen([sys.executable, "-c", code, str(self.root), key],
                                 cwd=str(pathlib.Path(__file__).resolve().parents[1]))
            try:
                with self.assertRaises(subprocess.TimeoutExpired):
                    p.wait(timeout=0.2)
            finally:
                pass
        self.assertEqual(p.wait(timeout=10), 0)
        self.assertEqual((self.root / key).read_bytes(), data)

    def test_admission_rejects_missing_then_pins_before_accepting(self):
        import threading
        key = hashlib.sha256(b"input").hexdigest()
        batch = types.SimpleNamespace(id="job", tree={"files": {"input": key}}, items={}, t0=time.time(),
                                      klass=1, client="test", image="test", closed=False, left=lambda: 0)
        instance = scheduler.Scheduler.__new__(scheduler.Scheduler)
        instance.batches, instance.lock, instance.wake = {}, threading.RLock(), threading.Event()
        msg = {"tree": batch.tree}
        with patch.object(scheduler, "DIR", self.base), patch.object(scheduler, "BLOBS", self.root), \
             patch.object(scheduler, "Batch", return_value=batch):
            result = instance.submit(msg)
            self.assertEqual(result["missing"], [key])
            self.assertFalse((self.base / "queue" / "job.json").exists())
            self.blob(b"input")
            self.assertTrue(instance.submit(msg)["ok"])
        self.prune()
        self.assertTrue((self.root / key).exists())

    def test_automatic_maintenance_is_wired_and_throttled(self):
        import threading
        instance = scheduler.Scheduler.__new__(scheduler.Scheduler)
        instance.stop, instance.pol = threading.Event(), scheduler.policy_defaults()
        instance.lock, instance.batches = threading.RLock(), {}
        class Immediate:
            def __init__(self, target, daemon):
                self.target = target
            def start(self):
                self.target()
        with patch.object(scheduler.threading, "Thread", Immediate), \
             patch.object(scheduler.blobcache, "collect", return_value={}) as collect:
            instance._blob_cache_tick()
            instance._blob_cache_tick()
            self.assertEqual(collect.call_count, 1)
            self.assertEqual(collect.call_args.args[1], 8 * 1024 ** 3)
            instance._blob_cache_at = 0
            instance.handing_off = True
            instance._blob_cache_tick()
            self.assertEqual(collect.call_count, 1)

    def test_invalid_policy_does_not_replace_policy_file(self):
        policy_file = self.base / "policy.json"
        with patch.object(scheduler, "POLICY", policy_file):
            with self.assertRaises(SystemExit):
                scheduler.write_policy({"blob_cache_grace_s": 0})
            self.assertFalse(policy_file.exists())
            policy = scheduler.write_policy({"blob_cache_bytes": 4096})
            self.assertEqual(policy["blob_cache_bytes"], 4096)
            self.assertEqual(json.loads(policy_file.read_text())["blob_cache_bytes"], 4096)

    def test_fixture_git_does_not_touch_an_inherited_index(self):
        index = self.base / "parent.index"
        index.write_bytes(b"parent index must remain untouched")
        result = subprocess.run([sys.executable, __file__,
                                 "CacheTests.test_implicit_generated_inputs_excluded_but_tracked_and_explicit_kept"],
                                env={**os.environ, "GIT_INDEX_FILE": str(index)}, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(index.read_bytes(), b"parent index must remain untouched")

    def test_finished_generation_releases_pins_but_keeps_results(self):
        key = self.blob(b"finished")
        self.queue(key)
        path = self.base / "queue" / "job.json"
        record = json.loads(path.read_text())
        record["cache_generation"] = "a" * 32
        path.write_text(json.dumps(record))
        rows = self.base / "queue" / "job.rows"
        rows.write_text("result kept\n")
        self.assertTrue(blobcache.completed(self.root, "job", "a" * 32, 0))
        self.prune()
        self.assertFalse((self.root / key).exists())
        self.assertEqual(rows.read_text(), "result kept\n")
        self.assertTrue(path.exists())

    def test_unfinished_generation_keeps_pins(self):
        key = self.blob(b"pending")
        self.queue(key)
        path = self.base / "queue" / "job.json"
        record = json.loads(path.read_text())
        record["cache_generation"] = "a" * 32
        path.write_text(json.dumps(record))
        self.assertFalse(blobcache.completed(self.root, "job", "a" * 32, 1))
        self.prune()
        self.assertTrue((self.root / key).exists())

    def test_old_completion_cannot_release_reused_batch_id(self):
        key = self.blob(b"new admission")
        self.queue(key)
        path = self.base / "queue" / "job.json"
        record = json.loads(path.read_text())
        record.update(cache_generation="b" * 32, cache_completed_generation="a" * 32)
        path.write_text(json.dumps(record))
        self.assertFalse(blobcache.completed(self.root, "job", "a" * 32, 0))
        self.prune()
        self.assertTrue((self.root / key).exists())

    def test_legacy_completion_is_conservatively_pinned(self):
        key = self.blob(b"legacy")
        self.queue(key)
        self.assertFalse(blobcache.completed(self.root, "job", None, 0))
        self.prune()
        self.assertTrue((self.root / key).exists())

    def test_policy_and_cli_registered(self):
        policy = scheduler.policy_defaults()
        self.assertEqual(policy["blob_cache_bytes"], 8 * 1024 ** 3)
        self.assertEqual(policy["blob_cache_grace_s"], 3600)
        self.assertEqual(policy["blob_cache_max_age_s"], 604800)
        result = subprocess.run([sys.executable, "-m", "supercomputer", "cache", "status"],
                                cwd=str(pathlib.Path(__file__).resolve().parents[1]), capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("scanned", json.loads(result.stdout))


if __name__ == "__main__":
    unittest.main()
