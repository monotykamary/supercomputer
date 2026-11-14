"""Typed transport evidence, with no providers or real network requests."""
import http.client
import pathlib
import socket
import ssl
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat as B


class AgentTransportTests(unittest.TestCase):
    def client(self):
        c = object.__new__(B.AgentClient)
        c.vm, c.owner, c.token = 'vm', 'owner', 'token'
        c.lease, c.cookie, c.query, c.base = {}, '', '', ''
        return c

    def test_rpc_preserves_http_status_without_owned_exec_fallback(self):
        for status in (400, 401, 403, 404, 408, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                c = self.client()
                with patch.object(c, '_req', return_value=(status, b'{"error":"token"}', 'application/json')) as req, patch.object(c, '_rpc_exec') as fallback:
                    with self.assertRaises(B.AgentHTTPError) as caught:
                        c.rpc('owned_rpc', {})
                self.assertEqual(caught.exception.status, status)
                req.assert_called_once()
                fallback.assert_not_called()

    def test_http_200_malformed_body_is_not_transport_evidence(self):
        c = self.client()
        with patch.object(c, '_req', return_value=(200, b'not json', 'text/plain')):
            with self.assertRaises(B.AgentError) as caught:
                c.rpc('owned_rpc', {})
        self.assertIs(type(caught.exception), B.AgentError)

    def test_http_200_semantic_refusal_is_left_to_the_protocol_layer(self):
        c = self.client()
        with patch.object(c, '_req', return_value=(200, b'{"ok":false}', 'application/json')):
            self.assertEqual(c.rpc('owned_rpc', {}), {'ok': False})

    def test_raw_upload_and_health_preserve_http_status(self):
        for method in ('raw', 'put', 'who'):
            c = self.client()
            with self.subTest(method=method), patch.object(c, '_req', return_value=(403, b'{"error":"token"}', 'application/json')):
                with self.assertRaises(B.AgentHTTPError) as caught:
                    getattr(c, method)(*({'raw':('itemlogs',), 'put':('/put', b'data'), 'who':()}[method]))
                self.assertEqual(caught.exception.status, 403)

    def test_only_recognized_network_failures_get_transport_type(self):
        examples = [(ConnectionResetError('reset'), True), (TimeoutError('timeout'), True),
                    (http.client.IncompleteRead(b''), True),
                    (socket.gaierror(socket.EAI_AGAIN, 'temporary DNS'), True),
                    (socket.gaierror(socket.EAI_NONAME, 'unknown host'), False),
                    (ssl.SSLCertVerificationError('untrusted certificate'), False),
                    (http.client.BadStatusLine('invalid protocol'), False)]
        for error, transient in examples:
            c = self.client()
            connection = Mock()
            connection.request.side_effect = error
            with self.subTest(error=type(error).__name__), patch.object(c, '_get', return_value=connection), patch.object(B.time, 'sleep'):
                with self.assertRaises(B.AgentError) as caught:
                    c._req('POST', '/rpc', b'{}')
            self.assertEqual(isinstance(caught.exception, B.AgentTransportError), transient)
            self.assertIs(caught.exception.__cause__, error)

    def test_redirect_exhaustion_does_not_inherit_an_earlier_network_failure(self):
        c = self.client()
        response = Mock(status=302)
        response.read.return_value = b''
        response.getheaders.return_value = []
        connection = Mock()
        connection.getresponse.side_effect = [ConnectionResetError('reset'), response, response, response]
        with patch.object(c, '_get', return_value=connection), patch.object(c, '_put_back'), patch.object(B.time, 'sleep'):
            with self.assertRaises(B.AgentError) as caught:
                c._req('POST', '/rpc', b'{}')
        self.assertNotIsInstance(caught.exception, B.AgentTransportError)


if __name__ == '__main__':
    unittest.main()
