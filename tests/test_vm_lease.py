"""VM-local lease persistence and transport fencing; run centrally on boat only."""
import base64
import io
import json
import os
import pathlib
import sys
import tarfile
import tempfile
import threading
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import agent as A


class VMLeasePersistence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.now = 1000.0
        self.boot = "boot-a"
        self.patches = [mock.patch.object(A, "ROOT", self.root),
                        mock.patch.object(A, "_lease_boot", lambda: self.boot),
                        mock.patch.object(A, "_lease_now", lambda: self.now),
                        mock.patch.object(A, "clean_disk", return_value={})]
        for p in self.patches:
            p.start()
        self.addCleanup(self.tmp.cleanup)
        for p in self.patches:
            self.addCleanup(p.stop)

    def claim(self, owner="a", vm_id="vm-a", **extra):
        return A.lease("claim", {"owner": owner, "vm_id": vm_id, **extra})

    def rpc(self, cmd, args, fence):
        return A.rpc_dispatch(cmd, [*args, {"_lease": fence}], cli=True)

    def job(self, owner="a", done=False):
        j = self.root / "b" / "batch" / "jobs" / "item"
        j.mkdir(parents=True, exist_ok=True)
        (j / "spec.json").write_text(json.dumps({"name": "item", "owner": owner}))
        (j / "out").write_text("owner result")
        if done:
            (j / "done.json").write_text(json.dumps({"name": "item", "code": 0}))
        return j

    def test_machine_reclaim_keeps_epoch_and_lapsed_renew_is_allowed(self):
        first = self.claim()
        self.assertTrue(first["ok"])
        self.now += 181
        self.assertTrue(A.lease("who")["lapsed"])
        self.assertTrue(A.lease("renew", first)["ok"])
        self.assertEqual(self.claim()["epoch"], first["epoch"])
        self.assertFalse(self.claim("b")["ok"])

    def test_lapsed_live_work_drains_without_signal_and_blocks_old_placement(self):
        first = self.claim()
        j = self.job()
        self.now += 180
        with mock.patch.object(A, "kill_tree", side_effect=AssertionError("reclaim must not kill")):
            pending = self.claim("b")
        self.assertFalse(pending["ok"])
        self.assertTrue(pending["draining"])
        self.assertTrue(A.lease("renew", first)["stale"])
        self.assertTrue(self.rpc("vtick", [{"start": [{"batch": "batch", "spec": {"name": "next"}}], "wait": 0}], first)["stale"])
        (j / "done.json").write_text(json.dumps({"name": "item", "code": 0}))
        second = self.claim("b")
        self.assertTrue(second["ok"])
        self.assertGreater(second["epoch"], first["epoch"])
        self.assertTrue((j / "out").exists())

    def test_old_owner_reads_and_acks_after_transfer_foreign_owner_cannot(self):
        first = self.claim()
        j = self.job(done=True)
        self.assertTrue(A.lease("release", first)["ok"])
        second = self.claim("b")
        self.assertTrue(second["ok"])
        foreign = self.rpc("vtick", [{"acks": [{"batch": "batch", "name": "item"}], "wait": 0}], second)
        self.assertEqual(foreign["done"], [])
        self.assertTrue((j / "done.json").exists())
        mine = self.rpc("vtick", [{"wait": 0}], first)
        self.assertEqual(mine["done"][0]["owner"], "a")
        self.assertEqual(self.rpc("itemlogs", ["batch", "item"], first)["out"], "owner result")
        self.assertTrue(self.rpc("itemlogs", ["batch", "item"], second)["stale"])
        blob = self.rpc("logs", ["batch"], first)
        with tarfile.open(fileobj=io.BytesIO(blob)) as tf:
            self.assertIn("item/out", tf.getnames())
        self.rpc("vtick", [{"acks": [{"batch": "batch", "name": "item"}], "wait": 0}], first)
        self.assertTrue((j / "acked.json").exists())
        A.drop_old_batches("", keep_s=0)
        self.assertTrue((j / "out").exists())

    def test_boot_and_vm_identity_invalidate_old_fences(self):
        first = self.claim()
        self.boot = "boot-b"
        self.assertTrue(A.lease("renew", first)["stale"])
        resumed = self.claim()
        self.assertTrue(resumed["ok"])
        forked = self.claim(vm_id="fork")
        self.assertTrue(forked["ok"])
        self.assertTrue(A.lease("renew", resumed)["stale"])
        with self.assertRaises(A.LeaseStale):
            with A.lease_guard(first):
                self.fail("stale boot admitted")

    def test_stop_excludes_claims_for_300s_and_release_from_stopping(self):
        first = self.claim()
        self.assertTrue(A.lease("fence_stop", first)["ok"])
        self.now += 299
        self.assertFalse(self.claim("b")["ok"])
        self.now += 1
        self.assertTrue(self.claim("b")["ok"])
        current = A.lease("who")
        receipt = A.lease("fence_stop", current)
        self.assertTrue(receipt["ok"])
        self.assertTrue(receipt["stop_nonce"])
        # A concurrent old same-owner release cannot reopen a fenced provider stop.
        self.assertTrue(A.lease("release", current)["stale"])
        self.assertTrue(A.lease("release", {**current, "stop_nonce": "wrong"})["stale"])
        self.assertFalse(self.claim("a")["ok"])
        self.assertNotIn("stop_nonce", A.lease("who"))
        self.assertTrue(A.lease("release", receipt)["ok"])

    def test_birth_owner_and_nonce_and_snapshot_expiry(self):
        self.now = 5
        (self.root / "birth.json").write_text(json.dumps({"owner": "a", "nonce": "birth-nonce"}))
        self.assertFalse(self.claim("b")["ok"])
        self.assertFalse(self.claim()["ok"])
        self.assertTrue(self.claim(nonce="birth-nonce")["ok"])
        self.assertTrue(self.claim()["ok"])
        self.now = 600
        self.assertTrue(self.claim("b")["ok"])

    def test_bare_mutations_never_bypass_established_lease(self):
        first = self.claim()
        for cmd, args in (("kill", ["batch"]), ("killitem", ["batch", "item"]),
                          ("cancelitem", ["batch", "item", 0, "nonce"]),
                          ("prep", ["batch", "tree:missing"]),
                          ("upgrade", ["", "8000", "token"])):
            self.assertTrue(A.rpc_dispatch(cmd, args, cli=True)["stale"], cmd)
        self.assertTrue(A.start_daemon("8000", "token")["stale"])
        self.assertTrue(A.collect("batch", "1")["stale"])
        self.assertTrue(self.rpc("kill", ["batch"], first).get("killed") == 0)

    def test_dynamic_client_keyring_never_replaces_another_key(self):
        A.keys_add("a", "token-a")
        self.assertEqual(A._token_owner("token-a"), "a")
        A.keys_add("b", "token-b")
        self.assertEqual(A._token_owner("token-b"), "b")
        self.assertEqual(A._token_owner("token-a"), "a")
        with self.assertRaises(ValueError):
            A.keys_add("a", "token-b")
        self.assertTrue(A.keys_add("a", "token-a")["again"])
        lease = self.claim()
        self.assertTrue(A.rpc_dispatch("lease", ["renew", lease], token_owner="b")["stale"])

    def test_same_version_daemon_does_not_fight_tokens(self):
        lease = self.claim()
        (self.root / "daemon.pid").write_text("123")
        (self.root / "daemon.version").write_text(A.VERSION)
        (self.root / "token").write_text("previous-token")
        fence = base64.b64encode(json.dumps(lease).encode()).decode()
        with mock.patch.object(A, "alive", return_value=True), mock.patch.object(A, "answers", return_value=True), mock.patch.object(A.subprocess, "Popen", side_effect=AssertionError("token war")):
            self.assertTrue(A.start_daemon("8000", "client-a-token", fence)["again"])
        self.assertEqual((self.root / "token").read_text(), "previous-token")

    def test_cancellation_is_bounded_while_lease_publication_lock_is_held(self):
        self.claim()
        entered, release = threading.Event(), threading.Event()
        def holder():
            with A.lease_lock():
                entered.set()
                release.wait(5)
        thread = threading.Thread(target=holder)
        thread.start()
        try:
            self.assertTrue(entered.wait(2))
            receipt = A.cancelitem("batch", "item", 0.01, "nonce")
            self.assertFalse(receipt["ok"])
            self.assertFalse(receipt["namespace_absent"])
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_exec_claims_serialize_across_processes_with_flock(self):
        import subprocess
        script = ("import importlib.util,json,pathlib,sys;"
                  "s=importlib.util.spec_from_file_location('lease_agent',sys.argv[1]);"
                  "a=importlib.util.module_from_spec(s);s.loader.exec_module(a);"
                  "a.ROOT=pathlib.Path(sys.argv[2]);a._lease_now=lambda:1000.;"
                  "a._lease_boot=lambda:'boot-a';"
                  "print(json.dumps(a.lease('claim',{'owner':sys.argv[3],'vm_id':'vm-a'})))")
        children = [subprocess.Popen([sys.executable, "-c", script, A.__file__, str(self.root), owner],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    for owner in ("a", "b")]
        replies = []
        try:
            for child in children:
                stdout, stderr = child.communicate(timeout=15)
                self.assertEqual(child.returncode, 0, stderr)
                replies.append(json.loads(stdout))
            self.assertEqual(sum(r["ok"] for r in replies), 1)
            winner = next(r["owner"] for r in replies if r["ok"])
            self.assertEqual(A.lease("who")["holder"], winner)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()  # exact child PID only; no name-based signaling
                    child.wait(timeout=5)

    def test_explicit_never_leased_mode_preserves_runpod(self):
        self.assertFalse(A._lease_enabled())
        self.assertEqual(A.kill("batch"), {"killed": 0})
        with A.lease_guard(None):
            pass


if __name__ == "__main__":
    unittest.main(verbosity=2)
