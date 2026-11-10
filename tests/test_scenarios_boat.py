"""Boat scenario transport stays a gate and keeps opt-in diagnostic receipts."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tests import scenarios


class BoatTransport(unittest.TestCase):
    def test_gate_retains_results_and_missing_names_are_not_passes(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "receipts"
            def run(argv, **kw):
                self.assertEqual(argv[argv.index("--class") + 1], "gate")
                self.assertEqual(argv[argv.index("--backend") + 1], "boat")
                self.assertEqual(kw["env"]["SUPERCOMPUTER_CONTROLLER"], "1")
                jobs = json.loads((out / "jobs.json").read_text())
                self.assertEqual([j["name"] for j in jobs], ["tree_gc", "kill9"])
                self.assertTrue(all(j["cmd"].startswith("set -o pipefail;") for j in jobs))
                (out / "run").mkdir()
                (out / "run/tree_gc.log").write_text(json.dumps({"name": "tree_gc", "ok": True}) + "\n--- stderr ---\ndiagnostic\n")
            with patch.dict(os.environ, {"SC_SCENARIOS_OUT": str(out), "SC_LAWS_IMAGE": "test-image", "SUPERCOMPUTER_CONTROLLER": "1"}), patch.object(scenarios.subprocess, "run", side_effect=run):
                result = scenarios.on_boat(["tree_gc", "kill9"], tmp)
            self.assertEqual(result, {"tree_gc": {"name": "tree_gc", "ok": True}})
            self.assertTrue((out / "jobs.json").exists())
            self.assertTrue((out / "run/tree_gc.log").exists())

    def test_default_temporary_receipts_are_cleaned(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "ephemeral"
            out.mkdir()
            with patch.dict(os.environ, {"SC_SCENARIOS_OUT": "", "SC_LAWS_IMAGE": "test-image"}), patch("tempfile.mkdtemp", return_value=str(out)), patch.object(scenarios.subprocess, "run"):
                self.assertEqual(scenarios.on_boat(["tree_gc"], tmp), {})
            self.assertFalse(out.exists())


if __name__ == "__main__":
    unittest.main()
