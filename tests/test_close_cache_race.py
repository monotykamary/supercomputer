"""Close/cache serialization on real files and flock; no providers or scheduler daemon."""
import fcntl
import json
import os
import pathlib
import sys
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_wiring import batch, sched
from supercomputer import blobcache, boat, scheduler as S


class CloseCacheRaceTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = pathlib.Path(tmp.name)
        self.blobs = self.root / "blobs"
        self.queue = self.root / "queue"
        self.queue.mkdir()
        for obj, name, value in ((S, "DIR", self.root), (S, "BLOBS", self.blobs),
                                 (boat, "LEDGER", self.root / "ledger.jsonl")):
            p = patch.object(obj, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.ledger = Mock()
        p = patch.object(boat, "ledger", self.ledger)
        p.start()
        self.addCleanup(p.stop)
        self.s = sched()
        self.b = batch(self.s, "close-cache")
        self.b.cache_generation = "a" * 32
        self.path = self.queue / f"{self.b.id}.json"
        self.path.write_text(json.dumps({"batch": self.b.id, "tree": self.b.tree,
                                         "cache_generation": self.b.cache_generation}))
        for suffix in ("rows", "placed"):
            self.path.with_suffix("." + suffix).write_text("{}\n")
        self.logs = self.path.with_suffix(".logs")
        self.logs.mkdir()
        (self.logs / "out").write_text("fixture")
        self.results = {}
        self.errors = []
        self.threads = []

    def start(self, name, fn, finished=None):
        def run():
            try:
                self.results[name] = fn()
            except BaseException as exc:
                self.errors.append(exc)
            finally:
                if finished is not None:
                    finished.set()
        t = threading.Thread(name=name, target=run)
        self.threads.append(t)
        t.start()

    def join(self):
        for t in self.threads:
            t.join(10)
            self.assertFalse(t.is_alive(), f"{t.name} did not stop")
        if self.errors:
            raise self.errors[0]

    def completion(self):
        return blobcache.completed(self.blobs, self.b.id, self.b.cache_generation, 0)

    def observe_lock(self, name, reached, blocked):
        """Probe the real flock without timing assumptions, then perform its blocking acquire."""
        real = fcntl.flock
        def flock(fd, mode):
            if threading.current_thread().name == "close":
                self.assertTrue(self.s.lock._is_owned(), "scheduler lock must precede cache lock")
                if mode == fcntl.LOCK_UN:
                    self.assertNotIn(self.b.id, self.s.batches, "memory commit must precede cache unlock")
            if threading.current_thread().name == name and mode == fcntl.LOCK_EX:
                try:
                    real(fd, mode | fcntl.LOCK_NB)
                except BlockingIOError:
                    blocked.set()
                else:
                    reached.set()
                    return
                reached.set()
            return real(fd, mode)
        return patch.object(fcntl, "flock", side_effect=flock)

    def assert_removed(self):
        self.assertNotIn(self.b.id, self.s.batches)
        self.assertTrue(self.b.closed)
        self.assertIn(self.b.id, self.s._durably_closed)
        self.assertFalse(self.path.exists())
        for suffix in ("closed", "retired", "rows", "placed", "logs"):
            self.assertTrue(self.path.with_suffix("." + suffix).exists(), suffix)
        self.assertEqual(len(self.s.m["batches"]), 1)
        self.ledger.assert_called_once()

    def test_completion_before_close_cannot_resurrect_admission(self):
        paused, release = threading.Event(), threading.Event()
        reached, blocked = threading.Event(), threading.Event()
        real_replace = os.replace
        def replace(src, dst, **kwargs):
            if pathlib.Path(dst) == self.path:
                paused.set()
                if not release.wait(10):
                    raise AssertionError("completion not released")
            return real_replace(src, dst, **kwargs)
        with patch.object(blobcache.os, "replace", side_effect=replace), \
             self.observe_lock("close", reached, blocked):
            try:
                self.start("complete", self.completion)
                self.assertTrue(paused.wait(10), "completion did not reach replacement")
                # Old close reaches 'reached' by returning; fixed close by contending on flock.
                self.start("close", lambda: self.s.close(self.b.id), reached)
                self.assertTrue(reached.wait(10), "close neither returned nor attempted cache lock")
                reserved_while_waiting = self.s.batches.get(self.b.id) is self.b
                returned_before_release = "close" in self.results
            finally:
                release.set()
                self.join()
        print(f"completion-first: close_returned_before_release={returned_before_release}, "
              f"lock_contended={blocked.is_set()}, disk_exists={self.path.exists()}, "
              f"memory_exists={self.b.id in self.s.batches}", flush=True)
        self.assertFalse(self.path.exists(), "completion resurrected the closed admission")
        self.assertTrue(blocked.is_set(), "close did not serialize with completion")
        self.assertFalse(returned_before_release)
        self.assertTrue(reserved_while_waiting)
        self.assertTrue(self.results["complete"])
        self.assert_removed()

    def test_close_before_completion_holds_lock_until_memory_commit(self):
        paused, release = threading.Event(), threading.Event()
        reached, blocked = threading.Event(), threading.Event()
        real_replace = os.replace
        def replace(src, dst, **kwargs):
            if pathlib.Path(src) == self.path and pathlib.Path(dst) == self.path.with_suffix('.closed'):
                paused.set()
                if not release.wait(10):
                    raise AssertionError("close not released")
            return real_replace(src, dst, **kwargs)
        with patch.object(os, "replace", replace), \
             self.observe_lock("complete", reached, blocked):
            try:
                self.start("close", lambda: self.s.close(self.b.id))
                self.assertTrue(paused.wait(10), "close did not reach deletion")
                self.start("complete", self.completion, reached)
                self.assertTrue(reached.wait(10), "completion did not attempt cache lock")
                reserved_before_delete = self.s.batches.get(self.b.id) is self.b
                if not blocked.is_set():
                    # Deterministically finish the old-code read/replace before allowing unlink.
                    self.threads[-1].join(10)
                    self.assertFalse(self.threads[-1].is_alive())
            finally:
                release.set()
                self.join()
        self.assertTrue(blocked.is_set(), "completion entered during close deletion")
        self.assertTrue(reserved_before_delete)
        self.assertFalse(self.results["complete"])
        self.assert_removed()

    def test_publication_failure_before_fence_keeps_reservation_and_no_close_ledger(self):
        with patch.object(S.history, 'atomic', side_effect=PermissionError('publication failure')):
            with self.assertRaises(PermissionError):
                self.s.close(self.b.id)
        self.assertIs(self.s.batches.get(self.b.id), self.b)
        self.assertFalse(self.b.closed)
        self.assertEqual(self.b.items['i0'].state, 'queued')
        self.assertEqual(self.s.m['batches'], [])
        self.ledger.assert_not_called()
        self.assertNotIn(self.b.id, getattr(self.s, '_durably_closed', set()))
        self.assertFalse(self.s.wake.is_set())
        self.s.close(self.b.id)
        self.assert_removed()

    def test_repeated_close_is_noop_and_kills_only_running_items(self):
        self.b.items.clear()
        # Use real batch/item construction for queued, starting, running and done states.
        self.b = batch(self.s, "close-cache", n=4)
        killed, kill_done = [], threading.Event()
        vm = object()
        for it, state in zip(self.b.items.values(), ("queued", "starting", "running", "done")):
            it.state, it.vm = state, vm
        def kill(vm_arg, bid, name):
            killed.append((vm_arg, bid, name))
            if len(killed) == 2:
                kill_done.set()
        real_thread, kill_threads = threading.Thread, []
        def thread(*args, **kwargs):
            t = real_thread(*args, **kwargs)
            kill_threads.append(t)
            return t
        with patch.object(self.s, "_kill_run", side_effect=kill), \
             patch.object(S.threading, "Thread", side_effect=thread):
            try:
                self.s.close(self.b.id)
                self.assertTrue(kill_done.wait(10))
                self.assertEqual(self.s.close(self.b.id), {})
            finally:
                for t in kill_threads:
                    t.join(10)
                    self.assertFalse(t.is_alive())
        self.assertEqual(set(killed), {(vm, self.b.id, self.b.items[n].run_name) for n in ("i1", "i2")})
        self.assertTrue(all(it.state == "done" for it in self.b.items.values()))
        self.assert_removed()

    def test_owned_close_still_delegates_without_deleting_evidence(self):
        for flag in ("strict_once", "retain_journals", "admission_closed"):
            with self.subTest(flag=flag):
                setattr(self.b, flag, True)
                with patch.object(self.s, "cancel", return_value={"owned": flag}) as cancel:
                    self.assertEqual(self.s.close(self.b.id), {"owned": flag})
                    cancel.assert_called_once_with(self.b.id)
                setattr(self.b, flag, False)
                self.assertIs(self.s.batches[self.b.id], self.b)
                self.assertTrue(self.path.exists())
                self.ledger.assert_not_called()


if __name__ == "__main__":
    unittest.main()
