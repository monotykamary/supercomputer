"""2026-11-16: `supercomputer batch fetch` of p3's alias69 batch (20261116-220456-eoco71) downloaded 67 outputs, then
failed whole on /logs 409: p3-alias69-0171 went LOST before any start request, so no log of it exists, and the
complete-logs snapshot demanded one. Here: the controller's complete snapshot leaves out only the strict rows its
identity-bound attempts journal proves never launched; fetch names them in fetch.json's logs_missing with the reason
and succeeds; no empty log is made for them; a launched row's missing log still fails. Run on boat, like all
test_*.py."""
import copy
import hashlib
import io
import json
import pathlib
import tarfile
import tempfile
import threading
import types
import unittest
from unittest.mock import patch

import test_batch_fetch as fixture
from supercomputer import batchfetch as F, scheduler as S

BID = fixture.BID


def archive(members):
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as tf:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return out.getvalue()


def strict_row(name, status):
    return {"name": name, "strict_once": True, "status": status, "vm": "bx_a", "run_name": name, "code": -1 if status != "SUCCEEDED" else 0,
            "placement": {"run_name": name, "vm": "bx_a", "boot_id": "boot-a", "identity": f"id-{name}"}}


class Client(fixture.FakeClient):
    def __init__(self, root):
        super().__init__(root)
        self.rows = [strict_row("ran", "SUCCEEDED"), strict_row("never", "LOST")]
        self.info.update(items=2, rows=2)
        self.caps["batch_fetch_never_launched"] = 1
        self.log_bytes = archive({"ran/out": b"ran out", "ran/err": b""})
        self.log_headers = {"X-SC-Content-SHA256": hashlib.sha256(self.log_bytes).hexdigest(),
                            "Content-Length": str(len(self.log_bytes))}

    def req(self, path, msg, timeout=60, raw=False, decode=None):
        if path == "/logs":
            self.calls.append((path, copy.deepcopy(msg), raw, decode is not None))
            assert msg == {"batch": BID, "vms": [], "require_complete": True, "never_launched_ok": True}
            return decode(fixture.Response(self.log_bytes, self.log_headers))
        return super().req(path, msg, timeout, raw, decode)


class FetchNamesNeverLaunchedRows(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.client = Client(self.root)

    def test_a_never_launched_row_is_named_missing_and_the_fetch_succeeds(self):
        result = F.fetch(BID, self.root / "out", client=self.client, timeout=5)
        self.assertEqual(result["logs_missing"], [{"name": "never", "run_name": "never", "status": "LOST",
                                                   "reason": F.NEVER_LAUNCHED}])
        receipt = json.loads((self.root / "out" / "fetch.json").read_text())
        self.assertEqual(receipt["logs_missing"], result["logs_missing"])
        with tarfile.open(self.root / "out" / "logs.tar.gz") as tf:
            self.assertNotIn("never/out", tf.getnames())  # (never an empty log made for it)

    def test_an_old_controller_is_asked_as_before(self):
        del self.client.caps["batch_fetch_never_launched"]
        seen = []
        real = fixture.FakeClient.req

        def old(self_, path, msg, timeout=60, raw=False, decode=None):
            if path == "/logs":
                seen.append(msg)
                return decode(fixture.Response(self_.log_bytes, self_.log_headers))
            return real(self_, path, msg, timeout, raw, decode)
        with patch.object(Client, "req", old):
            result = F.fetch(BID, self.root / "out", client=self.client, timeout=5)
        self.assertEqual(seen, [{"batch": BID, "vms": [], "require_complete": True}])
        self.assertEqual(result["logs_missing"], [])


class ControllerLeavesOutOnlyProvenNeverLaunched(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        (self.root / "queue").mkdir()
        patcher = patch.object(S, "DIR", self.root)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.s = S.Scheduler.__new__(S.Scheduler)
        self.s.lock = threading.RLock()
        self.rows = [strict_row("ran", "SUCCEEDED"), strict_row("never", "LOST")]
        self.s.batches = {BID: types.SimpleNamespace(rows=self.rows)}
        kept = self.root / "queue" / f"{BID}.logs" / "ran"
        kept.mkdir(parents=True)
        (kept / "out").write_text("ran out")
        (kept / "err").write_text("")

    def journal(self, *events):
        lines = []
        for name, event in events:
            pl = next(r for r in self.rows if r["name"] == name)["placement"]
            lines.append(json.dumps({"batch": BID, "name": name, "run_name": pl["run_name"], "vm": pl["vm"],
                                     "boot_id": pl["boot_id"], "launch_identity": pl["identity"], "event": event,
                                     "protocol": 1}) + "\n")
        (self.root / "queue" / f"{BID}.attempts").write_text("".join(lines))

    def members(self, data):
        with tarfile.open(fileobj=io.BytesIO(data)) as tf:
            return sorted(tf.getnames())

    def test_a_row_with_no_start_request_is_left_out(self):
        self.journal(("ran", "prelaunch_reserved"), ("ran", "start_requested"), ("never", "prelaunch_reserved"))
        with self.assertRaises(ValueError):  # (as before, without the flag)
            self.s.logs(BID, [], require_complete=True)
        data = self.s.logs(BID, [], require_complete=True, never_launched_ok=True)
        self.assertEqual(self.members(data), ["ran/err", "ran/out"])

    def test_a_requested_start_or_unknown_evidence_still_demands_its_log(self):
        self.journal(("ran", "prelaunch_reserved"), ("ran", "start_requested"),
                     ("never", "prelaunch_reserved"), ("never", "start_requested"))
        with self.assertRaises(ValueError):
            self.s.logs(BID, [], require_complete=True, never_launched_ok=True)
        (self.root / "queue" / f"{BID}.attempts").unlink()
        with self.assertRaises(ValueError):
            self.s.logs(BID, [], require_complete=True, never_launched_ok=True)


if __name__ == "__main__":
    unittest.main()
