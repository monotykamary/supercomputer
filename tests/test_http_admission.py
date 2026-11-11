"""Sub-second HTTP lock-scope tests: no sockets, scheduler process or provider."""
import ast
import functools
import io
import json
import pathlib
import sys
import threading
import types
import unittest
from unittest.mock import Mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S


@functools.lru_cache(maxsize=1)
def handler_code():
    tree = ast.parse(pathlib.Path(S.__file__).read_text())
    serve = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "serve")
    handler = next(n for n in serve.body if isinstance(n, ast.ClassDef) and n.name == "H")
    return compile(ast.Module(body=[handler], type_ignores=[]), "<HTTP handler fixture>", "exec")


def handler_class(owner):
    namespace = {**vars(S), "s": owner, "log": Mock()}
    exec(handler_code(), namespace)
    return namespace["H"]


class HTTPAdmission(unittest.TestCase):
    def make_handler(self, path="/drain", body=b"{}"):
        owner = types.SimpleNamespace(_request_lock=threading.RLock(), stop=threading.Event(),
                                      token="token", _cpu_add=lambda *a: None)
        health = types.SimpleNamespace(record_error=Mock(), record_client_error=Mock())
        owner._h = lambda: health
        cls = handler_class(owner)
        h = object.__new__(cls)
        h.path, h.headers = path, {"Content-Length": str(len(body)), "X-SC-Token": owner.token}
        h.rfile = io.BytesIO(body)
        h.client_address = ("127.0.0.1", 12345)
        h.close_connection = False
        h._send = lambda code, obj=None, raw=None: None
        return h, owner

    def test_body_read_and_response_write_are_outside_mutation_lock(self):
        h, owner = self.make_handler()
        seen = []
        class Body:
            def read(inner, n):
                self.assertFalse(owner._request_lock._is_owned())
                seen.append("read")
                return b"{}"
        h.rfile = Body()
        def mutate(body):
            self.assertTrue(owner._request_lock._is_owned())
            seen.append("mutate")
            return h._response(200, {"ok": True})
        def send(code, obj=None, raw=None):
            self.assertFalse(owner._request_lock._is_owned())
            self.assertEqual((code, raw), (200, b'{"ok": true}'))
            seen.append("write")
        h._post, h._send = mutate, send
        h.do_POST()
        self.assertEqual(seen, ["read", "mutate", "write"])

    def test_freeze_after_body_read_refuses_without_dispatch(self):
        for flag in ("stop", "handing_off", "_handoff_deferred"):
            h, owner = self.make_handler()
            class Body:
                def read(inner, n):
                    if flag == "stop":
                        owner.stop.set()
                    else:
                        setattr(owner, flag, True)
                    return b"{}"
            h.rfile = Body()
            h._post = lambda body: self.fail("mutation after freeze")
            replies = []
            h._send = lambda code, **kw: replies.append(code)
            h.do_POST()
            self.assertEqual(replies, [503])
            self.assertTrue(h.close_connection)

    def test_incomplete_or_timed_out_body_never_locks_or_dispatches(self):
        for timeout in (False, True):
            h, owner = self.make_handler()
            class NoLock:
                def __enter__(inner):
                    self.fail("incomplete input acquired mutation lock")
            class Body:
                def read(inner, n):
                    if timeout:
                        raise TimeoutError("partial client")
                    return b"{"
            owner._request_lock, h.rfile = NoLock(), Body()
            h._post = lambda body: self.fail("incomplete input dispatched")
            replies = []
            h._send = lambda code, **kw: replies.append(code)
            h.do_POST()
            self.assertEqual(replies, [408])
            self.assertTrue(h.close_connection)

    def test_readonly_request_does_not_take_mutation_lock(self):
        h, owner = self.make_handler("/status")
        def dispatch(body):
            self.assertFalse(owner._request_lock._is_owned())
            return h._response(200, {})
        h._post = dispatch
        h.do_POST()

    def dispatch(self, path, body):
        h, owner = self.make_handler(path, body)
        health = owner._h()
        owner.batch_of = S.Scheduler.batch_of
        owner.poll = Mock(return_value={"rows": [], "done": True})
        owner.capabilities = Mock(return_value={"fixture": True})
        owner.status = Mock(return_value={"fixture": True})
        replies = []
        h._send = lambda code, obj=None, raw=None: replies.append((code, json.loads(raw)))
        return h, owner, health, replies

    def test_legacy_poll_shapes_and_json_encodings_are_preserved(self):
        # Golden accepted bodies from the saved handler: no pre-decoding to
        # UTF-8, no new dict-only restriction, and the old id alias survives.
        bodies = [(b'{"batch":"b"}', "b"), (b'{"id":"b"}', "b"),
                  (b'{"batch":null,"id":"b"}', "b"),
                  (b'{"id":"b","since":2,"wait":0}', "b"),
                  (b'', None), (b'{}', None), (b'null', None), (b'[]', None),
                  (b'false', None), (b'42', None), (b'"batch"', None)]
        for encoding in ("utf-8", "utf-8-sig", "utf-16", "utf-32"):
            bodies.append((json.dumps({"id": "b-\u00e9"}, ensure_ascii=False).encode(encoding), "b-\u00e9"))
        for body, bid in bodies:
            with self.subTest(body=body):
                h, owner, health, replies = self.dispatch("/poll", body)
                h.do_POST()
                self.assertEqual(replies[0][0], 200 if bid is not None else 400)
                health.record_error.assert_not_called()
                if bid is not None:
                    self.assertEqual(owner.poll.call_args.args[0], bid)
                else:
                    owner.poll.assert_not_called()

    def test_body_independent_readers_keep_all_json_root_shapes(self):
        for path in ("/status", "/capabilities"):
            for body in (b'', b'{}', b'null', b'[]', b'false', b'42', b'"text"'):
                with self.subTest(path=path, body=body):
                    h, owner, health, replies = self.dispatch(path, body)
                    h.do_POST()
                    self.assertEqual(replies[0][0], 200)
                    health.record_error.assert_not_called()

    def test_invalid_client_json_is_400_not_a_canary_failure(self):
        for body in (b'{', b'{x', b"{'id':'b'}", b'{"id":"b"', b' ', b'{}{}', b'\xff', b'\xff\xfe{'):
            with self.subTest(body=body):
                h, owner, health, replies = self.dispatch("/poll", body)
                h.headers["User-Agent"] = "legacy-client"
                h.do_POST()
                self.assertEqual(replies, [(400, {"error": "invalid JSON request"})])
                self.assertTrue(h.close_connection)
                owner.poll.assert_not_called()
                health.record_error.assert_not_called()
                diagnostic = h._post.__globals__["log"].call_args.args[0]
                self.assertIn(f"read {len(body)}", diagnostic)
                health.record_client_error.assert_called_once()
                self.assertIn("Content-Length", diagnostic)
                self.assertIn("127.0.0.1", diagnostic)
                self.assertIn("legacy-client", diagnostic)

    def test_internal_json_failure_still_counts_as_server_error(self):
        h, owner, health, replies = self.dispatch("/poll", b'{"id":"b"}')
        owner.poll.side_effect = json.JSONDecodeError("broken internal state", "{", 1)
        h.do_POST()
        self.assertEqual(replies[0][0], 500)
        health.record_error.assert_called_once()
        health.record_client_error.assert_not_called()

    def test_raw_endpoints_do_not_decode_json(self):
        h, owner, health, replies = self.dispatch("/queue/add", b'\x00\xff')
        raw_post = Mock(return_value=({"fixture": True},))
        h._post.__globals__["raw_post"] = raw_post
        h.do_POST()
        self.assertEqual(replies, [(200, {"fixture": True})])
        raw_post.assert_called_once_with("/queue/add", b'\x00\xff')
        health.record_error.assert_not_called()

    def test_state_preserves_legacy_binary_body_without_the_mutation_lock(self):
        for body in (b'', b'{}', b'\x00\xff'):
            h, owner = self.make_handler("/state", body)
            replies = []
            def export(s):
                self.assertIs(s, owner)
                self.assertFalse(owner._request_lock._is_owned())
                return b"archive"
            h._post.__globals__["_state_export"] = export
            h._send = lambda code, **kw: replies.append((code, kw["raw"]))
            h.do_POST()
            self.assertEqual(replies, [(200, b"archive")])

    def test_request_socket_has_a_finite_timeout(self):
        h, _ = self.make_handler()
        self.assertGreater(h.timeout, 0)
        self.assertLessEqual(h.timeout, 15)


if __name__ == "__main__":
    unittest.main()
