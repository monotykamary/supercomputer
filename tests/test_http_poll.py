"""Native sockets: long polls release disconnected peers without acknowledging work."""
import http.client
import http.server
import json
import pathlib
import socket
import sys
import threading
import time
import types
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S
from test_http_admission import handler_class


class PollTests(unittest.TestCase):
    def setUp(self):
        self.batch = types.SimpleNamespace(rows=[], items={"pending": types.SimpleNamespace(state="queued")},
            cv=threading.Condition(), t_poll=0, left=lambda: 1)
        self.owner = types.SimpleNamespace(batches={"b": self.batch}, stop=threading.Event(), token="t",
            _cpu_add=lambda *a: None, _request_lock=threading.RLock())
        self.owner.batch_of = S.Scheduler.batch_of
        self.health = types.SimpleNamespace(record_error=mock.Mock(), record_client_error=mock.Mock())
        self.owner._h = lambda: self.health
        self.started, self.closed = threading.Event(), threading.Event()
        self.holder = {}
        def poll(*args, **kw):
            self.started.set()
            return S.Scheduler.poll(self.owner, *args, **kw)
        self.owner.poll = poll
        base = handler_class(self.owner)
        outer = self
        class H(base):
            def setup(self):
                super().setup()
                outer.holder["socket"] = self.connection
        class Server(http.server.ThreadingHTTPServer):
            daemon_threads = True
            def shutdown_request(self, request):
                try: super().shutdown_request(request)
                finally: outer.closed.set()
        self.server = Server(("127.0.0.1", 0), H)
        self.serving = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.01))
        self.serving.start()
        self.addCleanup(self.serving.join, 2)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def raw_poll(self):
        conn = socket.create_connection(self.server.server_address, timeout=2)
        body = b'{"batch":"b","wait":1000000000}'
        conn.sendall(b"POST /poll HTTP/1.1\r\nHost: localhost\r\nX-SC-Token: t\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
        self.assertTrue(self.started.wait(1))
        return conn

    def test_disconnected_poll_releases_socket_within_peer_check_bound(self):
        conn = self.raw_poll()
        begin = time.monotonic()
        conn.close()
        self.assertTrue(self.closed.wait(1), "disconnected long poll retained its request thread")
        elapsed = time.monotonic() - begin
        self.assertLess(elapsed, 1)
        print(f"disconnected poll socket released in {elapsed:.4f}s")
        self.assertEqual(self.holder["socket"].fileno(), -1)
        self.health.record_error.assert_not_called()
        self.assertEqual(self.batch.rows, [])
        self.assertEqual(self.batch.items["pending"].state, "queued")

    def test_huge_wait_is_capped_and_client_can_repoll(self):
        conn = http.client.HTTPConnection(*self.server.server_address, timeout=2)
        self.addCleanup(conn.close)
        # Shorten only the configured server bound; prove the untrusted huge
        # request does not bypass it. Production uses five seconds.
        with mock.patch.object(S, "HTTP_POLL_MAX_WAIT_S", .1):
            for _ in range(2):
                begin = time.monotonic()
                conn.request("POST", "/poll", body=json.dumps({"batch":"b", "wait":1e9}), headers={"X-SC-Token":"t"})
                response = conn.getresponse()
                self.assertEqual(response.status, 200)
                self.assertEqual(json.loads(response.read()), {"rows":[], "done":False, "next_since":0, "running":0, "queued":1})
                self.assertLess(time.monotonic() - begin, 1)
        self.health.record_error.assert_not_called()

    def test_new_row_wakes_poll_without_waiting_for_server_bound(self):
        conn = self.raw_poll()
        self.addCleanup(conn.close)
        with self.batch.cv:
            self.batch.rows.append({"name":"pending", "ok":True})
            self.batch.items["pending"].state = "done"
            self.batch.left = lambda: 0
            self.batch.cv.notify_all()
        response = http.client.HTTPResponse(conn)
        response.begin()
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.read())["rows"], [{"name":"pending", "ok":True}])
        self.health.record_error.assert_not_called()

    def test_live_and_closed_peer_with_fd_above_select_limit(self):
        import fcntl
        import resource
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = max(soft, 4096) if hard == resource.RLIM_INFINITY else min(max(soft, 4096), hard)
        self.assertGreater(target, 2048, "high-FD regression needs a hard limit above 2048")
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        left, right = socket.socketpair()
        try:
            fd = fcntl.fcntl(left.fileno(), fcntl.F_DUPFD, 2048)
            with socket.socket(fileno=fd) as high:
                self.assertGreaterEqual(high.fileno(), 2048)
                handler = object.__new__(handler_class(self.owner))
                handler.connection, handler.close_connection = high, False
                self.assertFalse(handler._peer_closed(), "live high-numbered socket was treated as disconnected")
                right.close()
                self.assertTrue(handler._peer_closed())
                self.assertTrue(handler.close_connection)
        finally:
            left.close()
            right.close()
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))

    def test_poll_wait_rule(self):
        self.assertEqual(S.rules.http_poll_wait(1e9, 5), 5)
        self.assertEqual(S.rules.http_poll_wait(-1, 5), 0)
        self.assertEqual(S.rules.http_poll_wait(0, 5), 0)
        self.assertEqual(S.rules.http_poll_wait(.2, 5), .2)


if __name__ == "__main__":
    unittest.main()
