"""A client mutation refused 503 "scheduler handoff" was never run, so the client sends the same request again until the
handoff ends (11-16 20:03: a 14 s handoff failed four release gates' /blobs/have and /image-pins/register on their
first 503). Other errors still fail at once; the retry is bounded. `python3 tests/test_handoff_client_retry.py`."""
import http.server
import json
import pathlib
import sys
import threading
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S  # noqa: E402

BUSY = json.dumps({"error": "scheduler handoff; retry the same endpoint"}).encode()


def serve(replies):
    """A keep-alive HTTP/1.1 server that, like the scheduler, closes the connection after a handoff 503."""
    seen = []

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self):
            seen.append((self.path, self.rfile.read(int(self.headers["Content-Length"]))))
            code, body = replies.pop(0) if replies else (200, b'{"ok": true}')
            self.close_connection = code == 503
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    client = object.__new__(S.Client)
    client.remote, client.port, client.token, client._conns = None, srv.server_address[1], "t", []
    return srv, client, seen


class HandoffRetry(unittest.TestCase):
    def test_local_mutation_retries_through_handoff_even_strict(self):
        for msg in ({"keys": ["a"]}, {"strict_once": True}):
            srv, client, seen = serve([(503, BUSY), (503, BUSY)])
            try:
                with patch.object(S.time, "sleep"):
                    self.assertEqual(client.req("/submit", msg), {"ok": True})
                self.assertEqual([p for p, _ in seen], ["/submit"] * 3)
                self.assertEqual({b for _, b in seen}, {json.dumps(msg).encode()})
            finally:
                srv.shutdown()

    def test_other_errors_fail_at_once(self):
        srv, client, seen = serve([(503, b'{"error": "overloaded"}'), (500, b"{}")])
        try:
            with patch.object(S.time, "sleep"):
                with self.assertRaises(RuntimeError) as e:
                    client.req("/blobs/have", {"keys": []})
            self.assertNotIsInstance(e.exception, S.HandoffBusy)
            self.assertEqual(len(seen), 1)
        finally:
            srv.shutdown()

    def test_retry_is_bounded(self):
        srv, client, seen = serve([(503, BUSY)] * 50)
        clock = iter(range(0, 10 ** 6, 100))
        try:
            with patch.object(S.time, "sleep"), patch.object(S.time, "monotonic", side_effect=lambda: next(clock)):
                with self.assertRaises(S.HandoffBusy):
                    client.req("/image-pins/register", {"images": ["x"]})
            self.assertLess(len(seen), 5)
        finally:
            srv.shutdown()

    def test_remote_controller_path_retries_too(self):
        client = object.__new__(S.Client)
        client.token = "t"
        client.remote = Mock(spec=S.AgentClient)
        client.remote._req.side_effect = [(503, BUSY, ""), (200, b'{"missing": []}', "")]
        with patch.object(S.time, "sleep"):
            self.assertEqual(client.req("/blobs/have", {"keys": []}), {"missing": []})
        self.assertEqual(client.remote._req.call_count, 2)
        client.remote._req.side_effect = [(503, b'{"error": "front"}', "")]
        with self.assertRaises(RuntimeError) as e:
            client.req("/blobs/have", {"keys": []})
        self.assertNotIsInstance(e.exception, S.HandoffBusy)


if __name__ == "__main__":
    unittest.main()
