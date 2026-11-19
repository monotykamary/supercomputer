"""Launch receipts distinguish an image's actual tools across boots."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from supercomputer import agent, boat, clientresults, config, fanout, resultfiles, resultjson, toolchain
from supercomputer.rules.agent_capabilities import agent_capabilities_match, required_agent_capabilities


class ToolchainTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(dir=Path.home())
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.node = self.bin / "node"
        self.path = f"{self.bin}:/usr/bin:/bin"
        self.install_node("v20.20.2")

    def install_node(self, version):
        self.node.write_text(f"#!/bin/sh\nprintf '%s\\n' '{version}'\n")
        self.node.chmod(0o755)

    def observe(self, boot, spec=None, extra_env=None):
        with patch.dict(os.environ, {"PATH": self.path, **(extra_env or {})}, clear=True), patch.object(agent, '_lease_boot', return_value=boot):
            return agent.observe_toolchain(spec or {})

    def test_equal_boots_pass_and_changed_node_on_same_path_flags(self):
        first = self.observe("boot-a")
        second = self.observe("boot-b")
        comparison = toolchain.BatchToolchains()
        comparison.observe({"name": "first", "toolchain": first})
        comparison.observe({"name": "second", "toolchain": second})
        result = comparison.summary()
        self.assertEqual((result["status"], result["boots_observed"]), ("consistent", 2))
        self.install_node("v24.22.1")
        third = self.observe("boot-c")
        comparison.observe({"name": "third", "toolchain": third})
        result = comparison.summary()
        self.assertEqual((result["status"], result["boots_observed"]), ("mixed", 3))
        self.assertEqual(len(result["fingerprints"]), 2)
        self.assertEqual(first["path"], third["path"])
        self.assertEqual(first["observed"]["node"]["realpath"], third["observed"]["node"]["realpath"])
        self.assertEqual(third["observed"]["node"]["version"], "v24.22.1")

    def test_declared_tools_are_capability_gated_and_cannot_drop_project_pin(self):
        spec = {"image_tools": {"node": {}, "git": {}}}
        project = config.Project(self.root, {"boat": {"tools": {"node": {"version": "v20.20.2"}}}})
        effective = config.with_image_tools(project, spec, {"tools": {"python3": {}}})
        self.assertEqual(effective["image_tools"], {"node": {"version": "v20.20.2"}, "git": {}, "python3": {}})
        required = required_agent_capabilities(effective)
        self.assertEqual(required, {"toolchain_protocol": 1})
        self.assertFalse(agent_capabilities_match(required, {}))
        self.assertTrue(agent_capabilities_match(required, agent.AGENT_CAPABILITIES))
        with self.assertRaisesRegex(ValueError, "conflicting image tool"):
            config.with_image_tools(project, {"image_tools": {"node": {"version": "v24.22.1"}}})
        for invalid in (None, {"/bin/sh": {}}, {"node": {"sha256": "0" * 64}}, {"node": {"version": ""}}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                required_agent_capabilities({"image_tools": invalid})

    def test_optional_measurements_do_not_create_false_drift(self):
        sample = self.observe("boot-a")
        digest = hashlib.sha256(self.node.read_bytes()).hexdigest()
        extra = self.bin / "demo"
        extra.write_bytes(self.node.read_bytes())
        extra.chmod(0o755)
        measured = self.observe("boot-b", {"image_tools": {"node": {"version": "v20.20.2", "sha256": digest}, "demo": {}}})
        summary = toolchain.BatchToolchains()
        for receipt in (sample, measured):
            summary.observe({"toolchain": receipt})
        self.assertEqual(summary.summary()["status"], "consistent")

    def test_jobs_file_pin_survives_hook_and_is_bound_to_submit(self):
        pin = {"node": {"version": "v20.20.2"}}
        project = config.Project(self.root)
        with patch.object(boat, "project", return_value=project), patch.object(project, "hook", return_value=lambda it, j: dict(it)):
            args, _ = boat.job_items([{"name": "pin", "cmd": "node", "image_tools": pin}], "", lambda s: s, "large", {})
        self.assertEqual(args[0]["image_tools"], pin)
        session = object.__new__(boat.Scheduled)
        session.stop = threading.Event()
        session.batch, session.bids, session._bid_lock = "pins", [], threading.Lock()
        session.client = Mock(remote=None)
        session.client.req.return_value = {"toolchain_protocol": 1, "required_agent_capabilities_protocol": 1}
        session.tree = SimpleNamespace(root="fixture", id="tree", files={}, links={})
        session.caches = []
        session.proj = SimpleNamespace(cache_outputs=Mock())
        session.klass, session.label, session.image, session.vm_type = 3, "pins", "img", "large"
        session.ctx_b64, session.warm_only = "", False
        session._image_for_scheduler = Mock()
        class Submitted(Exception):
            pass
        session._request = Mock(side_effect=Submitted)
        with self.assertRaises(Submitted):
            session.run(args, Mock())
        _, endpoint, body = session._request.call_args.args
        self.assertEqual(endpoint, "/submit")
        self.assertEqual(body["items"][0]["image_tools"], pin)
        self.assertEqual(body["items"][0]["required_agent_capabilities"], {"toolchain_protocol": 1})

    def test_preflight_does_not_spend_payload_deadline(self):
        self.node.write_text("#!/bin/sh\n/bin/sleep 1.25\nprintf 'v20.20.2\\n'\n")
        spec, receipt, runtime, marker = (self.root / f for f in ("spec", "receipt", "runtime", "marker"))
        spec.write_text(json.dumps({"kind": "sh", "limit": 1}))
        timer = [sys.executable, "-I", agent.__file__, "toolchain-exec", str(spec), str(receipt), str(runtime),
                 "/bin/sh", "-c", f'/usr/bin/touch {marker}']
        result = subprocess.run(timer, env={**os.environ, "PATH": self.path}, capture_output=True, timeout=8)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(marker.exists())
        observed = json.loads(receipt.read_text())
        self.assertGreaterEqual(observed["preflight_seconds"], 1.2)
        self.assertEqual(observed["observed"]["node"]["version"], "v20.20.2")

    def test_refuse_hash_mismatch_without_probing_and_refuse_shell_startup(self):
        spec = {"kind": "sh", "image_tools": {"node": {"version": "v20.20.2", "sha256": "0" * 64}}}
        with patch.object(agent, "_image_tool_probe") as probe:
            receipt = self.observe("boot", spec)
            self.assertFalse(receipt["ok"], receipt)
            self.assertEqual(receipt["observed"]["node"]["status"], "hash_mismatch")
            spec["image_tools"]["node"].pop("sha256")
            for key in ("BASH_ENV", "EXECIGNORE"):
                receipt = self.observe("boot", spec, {key: "/some/path"})
                self.assertFalse(receipt["ok"], receipt)
                self.assertEqual(receipt["shell_startup_env"], [key])
            probe.assert_not_called()

    def test_run_flags_mixed_without_changing_native_exits(self):
        first = self.observe("boot-a")
        same = self.observe("boot-b")
        self.install_node("v24.22.1")
        different = self.observe("boot-b")
        specs = [{"name": name, "cmd": "node --version"} for name in ("one", "two")]
        for second, expected in ((same, 0), (different, 3)):
            with self.subTest(expected=expected):
                rows = {name: {"code": 0, "seconds": 1, "toolchain": receipt}
                        for name, receipt in (("one", first), ("two", second))}
                directory = self.root / str(expected)
                with patch.object(fanout, "project", return_value=config.Project(self.root)), \
                        patch.object(fanout.boat, "boat_jobs", return_value=(rows, {})) as runner, \
                        patch("supercomputer.scheduler.enabled", return_value=True), \
                        patch("supercomputer.scheduler.controller", return_value=True), \
                        contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    code = fanout.run_jobs(None, specs, str(directory), "", lambda x: x, backend="boat", cli_env={"PATH": self.path})
                self.assertEqual(runner.call_args.args[0][0]["env"]["PATH"], self.path)
                self.assertEqual(code, expected)
                result = json.loads((directory / "toolchain.json").read_text())
                self.assertEqual(result["batch"]["mixed"], bool(expected))
                self.assertEqual(result["jobs"]["two"], second)
                self.assertEqual([line.split("\t")[1] for line in (directory / "summary.tsv").read_text().splitlines()], ["0", "0"])

    def test_offloaded_receipt_is_recovered_without_materializing_the_row(self):
        receipt = self.observe("boot-a")
        receipt["path"] = "a" * 8000
        raw = json.dumps({"name": "job", "result": "b" * (17 * 1024 * 1024),
                          "toolchain": receipt})
        origin = resultfiles.Store(self.root / "origin")
        small = resultjson.load_journal(io.BytesIO((raw + "\n").encode()), origin, strict=True)[0]
        self.assertIn("row_ref", small)
        self.assertNotIn("toolchain", small)
        restored = clientresults.download(small, origin.read, self.root / "downloaded")
        self.assertEqual(restored["toolchain"], receipt)
        self.assertNotIn("result", restored)

    @unittest.skipUnless(os.environ.get("SCFIX3_BOAT_TEST") == "1", "requires a native Boat mount namespace")
    def test_native_namespace_records_path_and_mismatch_never_runs_payload(self):
        repo = "toolchain-test-" + self.root.name
        src = Path("/src") / repo
        subprocess.run(["sudo", "-n", "mkdir", "-p", "/work", str(src)], check=True, timeout=5)
        self.addCleanup(lambda: subprocess.run(["sudo", "-n", "rmdir", str(src)], check=True, timeout=5))
        root = self.root / "agent"
        root.mkdir()
        (root / "agent.py").write_bytes(Path(agent.__file__).read_bytes())
        for version, expected in (("v20.20.2", 0), ("v24.22.1", 78)):
            with self.subTest(version=version):
                self.install_node(version)
                d = root / "b" / ("test" + str(expected))
                j = d / "jobs" / "job"
                j.mkdir(parents=True)
                (d / "ready").touch()
                (d / "tree").mkdir()
                (j / "cpus").write_text(json.dumps([min(os.sched_getaffinity(0))]))
                spec = {"name": "job", "kind": "sh", "limit": 15, "image_tools": {"node": {"version": "v20.20.2"}},
                        "cmd": 'node --version > "$SUPERCOMPUTER_OUTPUT_DIR/version"', "env": {"PATH": str(self.bin)}}
                (j / "spec.json").write_text(json.dumps(spec))
                with patch.object(agent, "ROOT", root), patch.object(agent, "ctx_of", return_value={"name": repo, "env": {"PATH": "/usr/bin:/bin"}}):
                    agent.run(d.name, "job")
                row = json.loads((j / "done.json").read_text())
                self.assertEqual(row["code"], expected, row)
                self.assertEqual(row["toolchain"]["path"], str(self.bin))
                self.assertEqual(row["toolchain"]["boot_id"], agent._lease_boot())
                self.assertEqual(row["toolchain"]["observed"]["node"]["version"], version)
                self.assertEqual(row["toolchain"]["ok"], expected == 0)
                self.assertEqual((j / "outputs" / "version").exists(), expected == 0)


if __name__ == "__main__":
    unittest.main()
