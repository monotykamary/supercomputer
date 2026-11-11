"""State exports never queue admissions behind archive CPU/I/O (no providers)."""
import contextlib
import io
import http.client
import http.server
import json
import os
import pathlib
import sys
import tarfile
import tempfile
import threading
import time
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from supercomputer import scheduler as S, statebackup as B, treadmill
from test_http_admission import handler_class


def unpack(data):
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tf:
        return {m.name: tf.extractfile(m).read() for m in tf.getmembers()}


class StateFiles(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.live = pathlib.Path(self.tmp.name) / "live"
        self.tm = pathlib.Path(self.tmp.name) / "treadmill"
        self.q = self.live / "scheduler/queue"
        self.q.mkdir(parents=True)
        self.tm.mkdir()

    def test_pinned_replaced_files_and_fixed_complete_journal_prefix(self):
        record = self.q / "batch.json"
        journal = self.q / "batch.placed"
        record.write_text('{"old":true}')
        journal.write_bytes(b'{"vm":"first"}\n')
        lock = threading.RLock()
        with B.capture(self.live, self.tm, lambda: lock) as entries:
            replacement = self.q / "new.tmp"
            replacement.write_text('{"new":true}')
            replacement.replace(record)
            with journal.open("ab") as f:
                f.write(b'{"vm":"second"}\n')
            got = unpack(B.archive(entries))
            self.assertFalse(lock._is_owned())
        self.assertEqual(got["live/scheduler/queue/batch.json"], b'{"old":true}')
        self.assertEqual(got["live/scheduler/queue/batch.placed"], b'{"vm":"first"}\n')
        for entry in entries:
            with self.assertRaises(OSError):
                os.fstat(entry.fd)

    def test_torn_external_append_is_not_exported(self):
        (self.q / "batch.json").write_text("{}")
        (self.q / "batch.rows").write_bytes(b'{"ok":1}\n{"half":')
        got = unpack(B.build(self.live, self.tm))
        self.assertEqual(got["live/scheduler/queue/batch.rows"], b'{"ok":1}\n')

    def test_prunes_secrets_code_blobs_and_recursive_backups_before_walking(self):
        for rel in ("agents/a", "scheduler/blobs/b", "scheduler/code/c", "controller-state/nested/a", "keys/k"):
            p = self.live / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text("must not traverse")
        for name in ("token", "boat-key", "controller.json", "a.tmp", "a.lock"):
            (self.live / name).write_text("secret or transient")
        (self.live / "policy.json").write_text("{}")
        (self.live / "symlink").symlink_to(self.live / "token")
        (self.tm / "status.json").write_text("{}")
        forbidden = {self.live / x for x in ("agents", "scheduler/blobs", "scheduler/code", "controller-state", "keys")}
        original = os.scandir
        def scandir(path):
            self.assertNotIn(pathlib.Path(path), forbidden)
            return original(path)
        with mock.patch.object(B.os, "scandir", side_effect=scandir):
            got = unpack(B.build(self.live, self.tm))
        self.assertEqual(set(got), {"live/policy.json", "treadmill/status.json", "backup-manifest.json"})

    def test_pending_treadmill_source_is_kept_not_completed_source(self):
        for name, status in (("pending", "queued"), ("old", "done")):
            d = self.tm / "subs" / name
            d.mkdir(parents=True)
            (d / "meta.json").write_text(json.dumps({"status": status}))
            (d / "jobs.json").write_text("[]")
            (d / "tree.tar.gz").write_bytes(b"source archive")
        got = unpack(B.build(self.live, self.tm))
        self.assertIn("treadmill/subs/pending/tree.tar.gz", got)
        self.assertNotIn("treadmill/subs/old/tree.tar.gz", got)

    def test_torn_json_metadata_refuses_backup(self):
        (self.q / "a.json").write_text('{"partial":')
        with self.assertRaises(json.JSONDecodeError):
            B.build(self.live, self.tm)

    def test_unacknowledged_job_outputs_are_kept(self):
        (self.q / "batch.json").write_text("{}")
        out = self.q / "batch.logs/work"
        out.mkdir(parents=True)
        (out / "out").write_text("stdout")
        (out / "err").write_text("stderr")
        got = unpack(B.build(self.live, self.tm))
        self.assertEqual(got["live/scheduler/queue/batch.logs/work/out"], b"stdout")
        self.assertEqual(got["live/scheduler/queue/batch.logs/work/err"], b"stderr")

    def test_recovery_closure_and_visible_archive_contents(self):
        (self.q / "a.json").write_text('{"tree":{"id":"needed"}}')
        (self.q / "orphan.rows").write_text('{"old":1}\n')
        trees = self.live / "scheduler/trees"
        trees.mkdir()
        (trees / "needed.json").write_text('{"id":"needed","files":{}}')
        (trees / "unused.json").write_text('{"id":"unused","files":{}}')
        metadata = {}
        cache = B.ArchiveCache()
        data = cache.get(lambda: B.build(self.live, self.tm, metadata=metadata), metadata)
        got = unpack(data)
        self.assertIn("live/scheduler/trees/needed.json", got)
        self.assertNotIn("live/scheduler/trees/unused.json", got)
        self.assertNotIn("live/scheduler/queue/orphan.rows", got)
        status = cache.status()
        self.assertEqual(status["archive_bytes"], len(data))
        self.assertGreater(status["source_bytes"], 0)
        self.assertEqual(status["files"], 2)
        self.assertEqual(status["builds"], 1)
        self.assertTrue(status["contents"])
        self.assertIn("external_requirements", json.loads(got["backup-manifest.json"]))
        self.assertNotIn("limits", json.loads(got["backup-manifest.json"]))
        self.assertEqual(cache.get(lambda: self.fail("cache missed")), data)
        self.assertEqual(cache.status()["cache_hits"], 1)

    def test_closed_outputs_are_excluded_without_deleting_controller_files(self):
        (self.q / "closed.json").write_text('{"cancellation_complete":true}')
        out = self.q / "closed.logs/job"
        out.mkdir(parents=True)
        (out / "out").write_bytes(b"old result")
        @contextlib.contextmanager
        def guard():
            yield {"closed": {"closed"}}
        got = unpack(B.build(self.live, self.tm, guard))
        self.assertIn("live/scheduler/queue/closed.json", got)
        self.assertNotIn("live/scheduler/queue/closed.logs/job/out", got)
        self.assertEqual((out / "out").read_bytes(), b"old result")

    def test_archive_exception_closes_every_pinned_descriptor(self):
        (self.q / "a.json").write_text("{}")
        entries = []
        def broken(xs):
            entries.extend(xs)
            raise OSError("archive failed")
        with mock.patch.object(B, "archive", side_effect=broken):
            with self.assertRaises(OSError):
                B.build(self.live, self.tm)
        self.assertTrue(entries)
        for entry in entries:
            with self.assertRaises(OSError):
                os.fstat(entry.fd)

    def test_payload_reads_are_outside_snapshot_guard(self):
        (self.q / "a.json").write_text("{}")
        lock = threading.RLock()
        original = os.pread
        def read(*args):
            self.assertFalse(lock._is_owned())
            return original(*args)
        with mock.patch.object(B.os, "pread", side_effect=read):
            self.assertTrue(B.build(self.live, self.tm, lambda: lock))


class Cache(unittest.TestCase):
    def test_expiry_and_failed_build_can_retry(self):
        now = [100.0]
        cache = B.ArchiveCache(ttl=30, clock=lambda: now[0])
        build = mock.Mock(side_effect=[b"first", RuntimeError("failed"), b"second"])
        self.assertEqual(cache.get(build), b"first")
        now[0] = 129.9
        self.assertEqual(cache.get(build), b"first")
        self.assertEqual(build.call_count, 1)
        now[0] = 130
        with self.assertRaises(RuntimeError):
            cache.get(build)
        self.assertFalse(cache.building)
        self.assertEqual(cache.get(build), b"second")

    def test_single_flight_and_bounded_wait(self):
        started, release = threading.Event(), threading.Event()
        cache = B.ArchiveCache(wait=.05)
        count = []
        def build():
            count.append(1)
            started.set()
            self.assertTrue(release.wait(2))
            return b"one"
        with ThreadPoolExecutor(max_workers=2) as pool:
            future = pool.submit(cache.get, build)
            self.assertTrue(started.wait(1))
            try:
                with self.assertRaises(TimeoutError):
                    cache.get(build)
            finally:
                release.set()
            self.assertEqual(future.result(timeout=1), b"one")
        self.assertEqual(cache.get(build), b"one")
        self.assertEqual(len(count), 1)


class HTTPIsolation(unittest.TestCase):
    def exercise(self, broken=False):
        with tempfile.TemporaryDirectory() as tmp:
            live = pathlib.Path(tmp) / "live"
            (live / "scheduler/queue").mkdir(parents=True)
            (live / "scheduler/queue/a.json").write_text("{}")
            owner = types.SimpleNamespace(_request_lock=threading.RLock(), lock=threading.RLock(),
                stop=threading.Event(), token="t", _cpu_add=lambda *a: None,
                _state_archive=B.ArchiveCache(), submit=mock.Mock(return_value={}), close=mock.Mock(return_value={}))
            owner._h = lambda: types.SimpleNamespace(record_error=mock.Mock(), record_client_error=mock.Mock())
            owner.batch_of = S.Scheduler.batch_of
            owner.batches = {}
            H = handler_class(owner)
            started, release, admitted = threading.Event(), threading.Event(), threading.Event()
            count = []
            original = B.archive
            def archive(entries):
                count.append(1)
                started.set()
                if not release.wait(3):
                    raise TimeoutError("test archive not released")
                return original(entries)
            class Server(http.server.ThreadingHTTPServer):
                request_queue_size = 64
                daemon_threads = True
            server = Server(("127.0.0.1", 0), H)
            serving = threading.Thread(target=lambda: server.serve_forever(poll_interval=.02))
            serving.start()
            def request(path):
                conn = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=2)
                try:
                    conn.request("POST", path, body=b'{"batch":"a"}', headers={"X-SC-Token": "t"})
                    response = conn.getresponse()
                    return [(response.status, response.read())]
                finally:
                    conn.close()
            def mutate():
                begin = time.monotonic()
                self.assertEqual(request("/submit")[0][0], 200)
                self.assertEqual(request("/close")[0][0], 200)
                elapsed = time.monotonic() - begin
                admitted.set()
                return elapsed
            with contextlib.ExitStack() as stack:
                stack.callback(serving.join, 2)
                stack.callback(server.server_close)
                stack.callback(server.shutdown)
                stack.enter_context(mock.patch.object(S.config, "LIVE", live))
                stack.enter_context(mock.patch.object(treadmill, "use", return_value=pathlib.Path(tmp) / "tm"))
                stack.enter_context(mock.patch.object(B, "archive", side_effect=archive))
                if broken:
                    # Retained old bug: read-only /state routed through _request_lock.
                    stack.enter_context(mock.patch.object(S.rules, "http_request_serialized", return_value=True))
                with ThreadPoolExecutor(max_workers=18) as pool:
                    first = pool.submit(request, "/state?legacy=1")
                    self.assertTrue(started.wait(1))
                    copies = [pool.submit(request, "/state") for _ in range(7)] if not broken else []
                    mutations = [pool.submit(mutate) for _ in range(1 if broken else 8)]
                    try:
                        if broken:
                            self.assertFalse(admitted.wait(.1), "broken archive unexpectedly released the mutation lock")
                        else:
                            self.assertTrue(admitted.wait(.5), "admission/close waited for archive construction")
                            times = [f.result(timeout=.5) for f in mutations]
                            self.assertLess(max(times), .5)
                            print(f"state export isolation: 8 admissions/closes, max {max(times):.4f}s")
                    finally:
                        release.set()
                    self.assertEqual(first.result(timeout=2)[0][0], 200)
                    for future in copies:
                        self.assertEqual(future.result(timeout=2), first.result())
                    for mutation in mutations:
                        mutation.result(timeout=2)
            self.assertEqual(len(count), 1)
            self.assertEqual(owner.submit.call_count, 1 if broken else 8)
            self.assertEqual(owner.close.call_count, 1 if broken else 8)

    def test_admission_and_close_remain_bounded_during_eight_exports(self):
        self.exercise()

    def test_old_archive_under_lock_variant_is_detected(self):
        self.exercise(broken=True)


if __name__ == "__main__":
    unittest.main()
