"""Controller resource policy: all provider/SSH calls are isolated mocks."""
import contextlib
import io
import pathlib
import subprocess
import unittest
from unittest.mock import patch

import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import controller, scheduler


class ControllerLimitsTests(unittest.TestCase):
    def stage(self, receipt='{"active":"old","staged":"candidate"}', status=None, code=0):
        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(patch.object(controller, "info", return_value={}))
        stack.enter_context(patch.object(scheduler.Client, "__init__", return_value=None))
        stack.enter_context(patch.object(scheduler.Client, "req", return_value=status if status is not None else {"hot_handoff": 1}))
        stack.enter_context(patch.object(scheduler, "code_hash", return_value="candidate"))
        stack.enter_context(patch.object(controller, "tool_tar", return_value=b"tar"))
        ssh = stack.enter_context(patch.object(controller, "ssh", return_value=(code, receipt)))
        return ssh

    def test_setup_unit_scales_memory_and_retains_other_limits(self):
        unit = controller.unit("scheduler", "fixture", "/tmp/fixture", {})
        for directive in ("MemoryHigh=70%", "MemoryMax=82%", "CPUQuota=250%",
                          "TasksMax=1500", "Nice=5", "LimitNOFILE=524288:524288"):
            self.assertIn(directive + "\n", unit)
        self.assertEqual([line for line in unit.splitlines() if line.startswith("Memory")],
                         ["MemoryHigh=70%", "MemoryMax=82%"])
        treadmill = controller.unit("treadmill", "fixture", "/tmp/fixture", {})
        for directive in ("MemoryHigh=1536M", "MemoryMax=2G", "CPUQuota=100%", "TasksMax=400", "Nice=10"):
            self.assertIn(directive + "\n", treadmill)

    def test_stage_persists_exact_dropins_without_touching_runtime(self):
        ssh = self.stage()
        with contextlib.redirect_stdout(io.StringIO()) as output:
            controller.stage("fixture")
        ssh.assert_called_once()
        vm, command, archive = ssh.call_args.args
        self.assertEqual((vm, archive), ("fixture", b"tar"))
        directory = "/etc/systemd/system/supercomputer-scheduler.service.d/"
        self.assertIn("printf '[Service]\nLimitNOFILE=524288:524288\n' | "
                      "sudo tee " + directory + "10-nofile.conf >/dev/null; ", command)
        self.assertIn("printf '%s' '[Service]\nMemoryHigh=70%\nMemoryMax=82%\n' | "
                      "sudo tee " + directory + "20-memory.conf >/dev/null; ", command)
        memory_printf = next(part for part in command.split("; ") if "MemoryHigh=" in part).split(" | ")[0]
        self.assertEqual(subprocess.check_output(["/bin/sh", "-c", memory_printf], timeout=5),
                         b"[Service]\nMemoryHigh=70%\nMemoryMax=82%\n")
        self.assertEqual(command.count("systemctl daemon-reload"), 1)
        self.assertLess(command.index("20-memory.conf"), command.index("systemctl daemon-reload"))
        self.assertIn("SUPERCOMPUTER_CONTROLLER=0 python3 -P -m supercomputer scheduler stage", command)
        for forbidden in ("restart", "systemctl start", "systemctl stop", "enable", "kill",
                          "set-property", "revert", "/run/systemd", "rm ", "--runtime"):
            self.assertNotIn(forbidden, command)
        self.assertIn('"staged": "candidate"', output.getvalue())

    def test_wrong_stage_attestation_is_still_rejected(self):
        for receipt in ('{"active":"old","staged":"wrong"}', '',
                        '{"active":"old","staged":"candidate"}\n{"active":"old","staged":"wrong"}'):
            with self.subTest(receipt=receipt):
                self.stage(receipt)
                with self.assertRaisesRegex(RuntimeError, "did not attest"):
                    controller.stage("fixture")

    def test_missing_handoff_protocol_does_not_write(self):
        ssh = self.stage(status={})
        with self.assertRaisesRegex(RuntimeError, "no hot-handoff protocol"):
            controller.stage("fixture")
        ssh.assert_not_called()

    def test_failed_remote_stage_is_not_success(self):
        self.stage(code=1)
        with self.assertRaisesRegex(RuntimeError, "staging failed"):
            controller.stage("fixture")


if __name__ == "__main__":
    unittest.main()
