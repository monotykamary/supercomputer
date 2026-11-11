"""Native HTTP regressions for store metadata larger than transport response limits."""
import hashlib
import http.server
import json
import pathlib
import re
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import store


class ManifestTransport(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="sc-store-metadata-")
        self.root = pathlib.Path(self.temp.name)
        self.payload = b"a verified payload\n"
        self.metadata = {"payload": [len(self.payload), hashlib.sha256(self.payload).hexdigest()]}
        self.files = {"/tok/payload": self.payload}
        self.cap = 1 << 20
        self.ignore_ranges = False
        self.changed_size = False
        self.requests = []
        owner = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append((self.path, self.headers.get("Range"), self.headers.get("Cookie")))
                body = owner.files.get(self.path)
                if body is None:
                    self.send_error(404)
                    return
                match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
                if owner.ignore_ranges:
                    match = None
                start, end = (int(match[1]), min(int(match[2]), len(body) - 1)) if match else (0, len(body) - 1)
                data = body[start:end + 1]
                self.send_response(206 if match else 200)
                if match:
                    size = len(body) + int(owner.changed_size and end > 0)
                    self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Set-Cookie", "store=ready; Path=/")
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(data[:owner.cap])
                self.close_connection = True

            def log_message(self, *args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/tok/"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2)
        self.assertFalse(self.thread.is_alive())
        self.temp.cleanup()

    def pull(self):
        self.files["/tok/manifest.json"] = json.dumps(self.metadata, ensure_ascii=False).encode()
        return store.pull("fixture", str(self.root / "dest"), self.url, self.root / "cache", 2, "payload")

    def test_small_legacy_manifest(self):
        self.assertEqual(self.pull(), self.metadata)
        self.assertEqual((self.root / "dest/payload").read_bytes(), self.payload)

    def test_manifest_over_one_megabyte_uses_bounded_ranges(self):
        self.metadata.update({f"unused/café-雪-{i:05d}": [0, "0" * 64] for i in range(14000)})
        expected = {"payload": self.metadata["payload"]}
        self.assertEqual(self.pull(), expected)
        self.assertGreater(len(self.files["/tok/manifest.json"]), self.cap)
        requests = [r for r in self.requests if r[0] == "/tok/manifest.json"]
        self.assertGreater(len(requests), 1)
        for _, header, _ in requests:
            self.assertIsNotNone(header)
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", header)
            self.assertLessEqual(int(match[2]) - int(match[1]) + 1, 256 << 10)
        self.assertTrue(all(cookie == "store=ready" for _, _, cookie in self.requests[1:]))
        self.assertEqual((self.root / "dest/payload").read_bytes(), self.payload)

    def test_cut_page_resumes_at_first_missing_byte(self):
        self.cap = 4096
        self.metadata.update({f"unused/{i:04d}": [0, "0" * 64] for i in range(200)})
        self.assertEqual(self.pull(), {"payload": self.metadata["payload"]})
        headers = [header for path, header, _ in self.requests if path == "/tok/manifest.json"]
        self.assertIn(f"bytes={self.cap}-{len(self.files['/tok/manifest.json']) - 1}", headers)
        self.assertEqual((self.root / "dest/payload").read_bytes(), self.payload)

    def test_large_links_metadata_is_paged_too(self):
        links = {f"unused-link-{i:05d}": "payload/" + "x" * 80 for i in range(14000)}
        self.files["/tok/links.json"] = json.dumps(links).encode()
        self.assertGreater(len(self.files["/tok/links.json"]), self.cap)
        self.assertEqual(self.pull(), self.metadata)
        headers = [header for path, header, _ in self.requests if path == "/tok/links.json"]
        self.assertGreater(len(headers), 1)
        self.assertTrue(all(header is not None for header in headers))

    def test_server_ignoring_ranges_streams_legacy_json(self):
        self.ignore_ranges = True
        self.metadata.update({f"unused/{i:05d}": [0, "0" * 64] for i in range(14000)})
        self.cap = 4 << 20
        self.assertEqual(self.pull(), {"payload": self.metadata["payload"]})

    def test_truncated_legacy_stream_is_rejected(self):
        self.ignore_ranges = True
        self.cap = 8
        with self.assertRaisesRegex(RuntimeError, "truncated metadata"):
            self.pull()
        self.assertFalse((self.root / "dest/payload").exists())

    def test_no_progress_exhausts_existing_retry_budget(self):
        self.cap = 0
        with patch.object(store.time, "sleep"), self.assertRaisesRegex(RuntimeError, "no bytes"):
            self.pull()
        requests = [r for r in self.requests if r[0] == "/tok/manifest.json"]
        self.assertEqual(len(requests), 10)  # size probe plus nine attempts (limit eight)
        self.assertFalse((self.root / "dest/payload").exists())

    def test_changed_metadata_size_is_rejected_before_placing_files(self):
        self.changed_size = True
        with patch.object(store.time, "sleep"), self.assertRaisesRegex(RuntimeError, "changed metadata"):
            self.pull()
        self.assertFalse((self.root / "dest/payload").exists())

    def test_invalid_links_metadata_is_not_silently_discarded(self):
        self.files["/tok/links.json"] = b"[not-json"
        with self.assertRaises(json.JSONDecodeError):
            self.pull()
        self.assertFalse((self.root / "dest/payload").exists())


if __name__ == "__main__":
    unittest.main()
