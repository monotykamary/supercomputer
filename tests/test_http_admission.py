"""Sub-second HTTP lock-scope tests: no sockets, scheduler process or provider."""
import ast
import io
import pathlib
import sys
import threading
import types
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import scheduler as S


def handler_class(owner):
    tree = ast.parse(pathlib.Path(S.__file__).read_text())
    serve = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "serve")
    handler = next(n for n in serve.body if isinstance(n, ast.ClassDef) and n.name == "H")
    namespace = {**vars(S), "s": owner}
    exec(compile(ast.Module(body=[handler], type_ignores=[]), "<HTTP handler fixture>", "exec"), namespace)
    return namespace["H"]


class HTTPAdmission(unittest.TestCase):
    def make_handler(self, path="/drain"):
        owner = types.SimpleNamespace(_request_lock=threading.RLock(), stop=threading.Event(),
                                      token="token", _cpu_add=lambda *a: None)
        cls = handler_class(owner)
        h = object.__new__(cls)
        h.path, h.headers = path, {"Content-Length": "2", "X-SC-Token": owner.token}
        h.rfile = io.BytesIO(b"{}")
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

    def test_request_socket_has_a_finite_timeout(self):
        h, _ = self.make_handler()
        self.assertGreater(h.timeout, 0)
        self.assertLessEqual(h.timeout, 15)


if __name__ == "__main__":
    unittest.main()
