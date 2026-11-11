"""Idle HTTP ownership, expiry, and wire-level regressions (no boat/controller access)."""
import gc
import fcntl
import resource
import http.client
import http.server
import itertools
import pathlib
import select
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import weakref
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat as B
from broken_boat import broken_no_age, broken_no_peer_check


class AgentHTTPPool(unittest.TestCase):
    def setUp(self):
        self.client = B.AgentClient('test', 'https://localhost', 'secret', lease={})
        self.connections = []
        self.peers = []

    def pair(self):
        sock, peer = socket.socketpair()
        c = http.client.HTTPConnection('localhost')
        c.sock = sock
        self.connections.append(c)
        self.peers.append(peer)
        return c, peer

    def tearDown(self):
        with self.client.lock:
            self.client.idle.clear()
            B._AGENT_IDLE_POOLS.discard(self.client)
        for c in self.connections:
            c.close()
        for peer in self.peers:
            peer.close()
        with B._AGENT_IDLE_POOLS.condition:
            thread = B._AGENT_IDLE_POOLS.thread
        if thread is not None:
            thread.join(2)
            self.assertFalse(thread.is_alive(), 'idle sweeper did not stop')

    def test_dead_peer_and_unexpected_bytes_pruned_without_consuming(self):
        for data in (None, b'unexpected'):
            with self.subTest(data=data):
                c, peer = self.pair()
                self.client._put_back(c)
                if data is None:
                    peer.shutdown(socket.SHUT_WR)
                else:
                    peer.sendall(data)
                self.assertTrue(select.select([c.sock], [], [], 1)[0])
                self.client._prune_idle()
                self.assertIsNone(c.sock)
                self.assertEqual(self.client.idle, [])

    def test_closed_connections_not_retained(self):
        c, _ = self.pair()
        c.close()
        self.client._put_back(c)
        self.assertEqual(self.client.idle, [])
        c, _ = self.pair()
        c.sock.close()  # invalid descriptor rather than HTTPConnection.close
        self.client._put_back(c)
        self.assertIsNone(c.sock)
        self.assertEqual(self.client.idle, [])

    def test_expiry_boundary_and_checkout_never_reuses_dead_or_expired(self):
        c, _ = self.pair()
        with patch.object(B.time, 'monotonic', return_value=100):
            self.client._put_back(c)
        with patch.object(B.time, 'monotonic', return_value=130):
            got = self.client._get(17)
        self.connections.append(got)
        self.assertIsNot(got, c)
        self.assertIsNone(c.sock)
        self.assertEqual(got.timeout, 17)
        dead, peer = self.pair()
        self.client._put_back(dead)
        peer.shutdown(socket.SHUT_WR)
        got = self.client._get(19)
        self.connections.append(got)
        self.assertIsNot(got, dead)
        self.assertIsNone(dead.sock)

    def test_high_fd_healthy_socket_is_reused_and_dead_peer_pruned(self):
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if hard <= 2048:
            self.skipTest('hard NOFILE limit cannot hold FD 2048')
        try:
            if soft <= 2048:
                resource.setrlimit(resource.RLIMIT_NOFILE, (min(4096, hard), hard))
            c, peer = self.pair()
            fd = fcntl.fcntl(c.sock.fileno(), fcntl.F_DUPFD_CLOEXEC, 2048)
            c.sock.close()
            c.sock = socket.socket(fileno=fd)
            self.client._put_back(c)
            self.assertIs(self.client._get(7), c)
            self.client._put_back(c)
            peer.shutdown(socket.SHUT_WR)
            self.client._prune_idle()
            self.assertIsNone(c.sock)
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))

    def test_close_oserror_does_not_prevent_other_idle_cleanup(self):
        first, _ = self.pair()
        second, _ = self.pair()
        with patch.object(B.time, 'monotonic', return_value=100):
            self.client._put_back(first)
            self.client._put_back(second)
        original = first.close
        def close_then_raise():
            original()
            raise OSError('injected close failure')
        with patch.object(first, 'close', side_effect=close_then_raise), \
             patch.object(B.time, 'monotonic', return_value=130):
            self.client._prune_idle()
        self.assertIsNone(second.sock)
        self.assertEqual(self.client.idle, [])

    def test_shared_reaper_survives_one_pool_failure(self):
        c, _ = self.pair()
        broken = B.AgentClient('broken', 'https://localhost', 'secret', lease={})
        with patch.object(B.AgentClient, 'IDLE_TIMEOUT', 0.01), \
             patch.object(broken, '_prune_idle', side_effect=OSError('injected prune failure')):
            B._AGENT_IDLE_POOLS.add(broken)
            self.client._put_back(c)
            with B._AGENT_IDLE_POOLS.condition:
                thread = B._AGENT_IDLE_POOLS.thread
            try:
                # Wake a single pass deterministically after expiry, no busy polling.
                threading.Event().wait(0.02)
                # Wait for the actual socket close, irrespective of WeakSet ordering.
                # An Event on close avoids timing assumptions about the background thread.
                done = threading.Event()
                original = c.close
                def close():
                    original()
                    done.set()
                with patch.object(c, 'close', side_effect=close):
                    with B._AGENT_IDLE_POOLS.condition:
                        B._AGENT_IDLE_POOLS.condition.notify()
                    self.assertTrue(done.wait(6))
                self.assertIsNone(c.sock)
                self.assertTrue(thread.is_alive())
            finally:
                B._AGENT_IDLE_POOLS.discard(broken)
                thread.join(2)
            self.assertFalse(thread.is_alive())

    def test_capacity_and_healthy_reuse(self):
        cs = [self.pair()[0] for _ in range(9)]
        for c in cs:
            self.client._put_back(c)
        self.assertEqual(len(self.client.idle), 8)
        self.assertIsNone(cs[-1].sock)
        got = self.client._get(7)
        self.assertIs(got, cs[7])
        self.assertEqual(got.sock.gettimeout(), 7)
        self.client._put_back(got)

    def test_untouched_idle_pool_expires_in_background(self):
        c, _ = self.pair()
        with patch.object(B.AgentClient, 'IDLE_TIMEOUT', 0.01):
            self.client._put_back(c)
            with B._AGENT_IDLE_POOLS.condition:
                thread = B._AGENT_IDLE_POOLS.thread
            thread.join(7)  # one bounded wait; no further pool operation
            self.assertFalse(thread.is_alive())
        self.assertIsNone(c.sock)
        self.assertEqual(self.client.idle, [])

    def test_reaper_does_not_keep_client_alive(self):
        c, _ = self.pair()
        client = B.AgentClient('other', 'https://localhost', 'secret', lease={})
        client._put_back(c)
        ref = weakref.ref(client)
        del client
        gc.collect()
        self.assertIsNone(ref())

    def test_rules_and_retained_broken_variants(self):
        for age, ttl, connected, readable in itertools.product((0, 29, 30, 31), (30,), (False, True), (False, True)):
            self.assertEqual(B.R.agent_http_reusable(age, ttl, connected, readable),
                             connected and not readable and age < ttl)
        self.assertTrue(broken_no_age(30, 30, True, False))
        self.assertFalse(B.R.agent_http_reusable(30, 30, True, False))
        self.assertTrue(broken_no_peer_check(0, 30, True, True))
        self.assertFalse(B.R.agent_http_reusable(0, 30, True, True))
        self.assertTrue(broken_no_peer_check(0, 30, False, False))
        self.assertFalse(B.R.agent_http_reusable(0, 30, False, False))

    def test_active_https_response_survives_idle_cleanup(self):
        self.assertIsNotNone(shutil.which('openssl'), 'TLS fixture requires openssl')
        started, release = threading.Event(), threading.Event()
        errors, result = [], []

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def do_GET(self):
                self.send_response(200)
                self.send_header('Content-Length', '6')
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'abc')
                self.wfile.flush()
                started.set()
                if release.wait(5):
                    self.wfile.write(b'def')
                    self.wfile.flush()

            def log_message(self, *args):
                pass

        with tempfile.TemporaryDirectory() as tmp:
            cert, key = pathlib.Path(tmp) / 'cert.pem', pathlib.Path(tmp) / 'key.pem'
            subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                            '-keyout', str(key), '-out', str(cert), '-days', '1',
                            '-subj', '/CN=localhost'], check=True, capture_output=True, timeout=10)
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            ctx.load_cert_chain(cert, key)
            server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            server.socket = ctx.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            self.client.host = '127.0.0.1:%d' % server.server_port
            checkout = self.client._get

            def connection(timeout):
                c = checkout(timeout)
                self.connections.append(c)
                return c

            def request():
                try:
                    result.append(self.client._req('GET', '/', timeout=5))
                except Exception as exc:
                    errors.append(exc)

            worker = None
            try:
                with patch.object(ssl, '_create_default_https_context',
                                  return_value=ssl._create_unverified_context()), \
                     patch.object(self.client, '_get', side_effect=connection):
                    worker = threading.Thread(target=request)
                    worker.start()
                    self.assertTrue(started.wait(3), f"TLS response did not start: {errors!r}")
                    idle, _ = self.pair()
                    with patch.object(B.time, 'monotonic', return_value=100):
                        self.client._put_back(idle)
                    # The TLS connection is checked out while r.read waits on its body.
                    active = self.connections[0]
                    with patch.object(B.time, 'monotonic', return_value=131):
                        self.client._prune_idle()
                    self.assertIsNone(idle.sock)
                    self.assertIsNotNone(active.sock)
                    release.set()
                    worker.join(5)
                    self.assertFalse(worker.is_alive())
                    self.assertEqual(errors, [])
                    self.assertEqual(result, [(200, b'abcdef', 'text/plain')])
            finally:
                release.set()
                if worker is not None:
                    worker.join(6)
                for c in self.connections:
                    c.close()
                server.shutdown()
                server.server_close()
                thread.join(2)


if __name__ == '__main__':
    unittest.main()
