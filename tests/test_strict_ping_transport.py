"""2026-11-16 22:05-22:09: eoco71's p3-alias69-0171 (bx_rejj3ega) and 1076 (bx_sxqdhrve), and dmhxxl's
postflight-repro-1-wave4 (bx_ekvw4s47) went LOST at the required-agent capability ping: boat's front and boat exec
both refused ("connect ECONNREFUSED <node>:210xx"), while each VM kept its boot and its agent its process. Nothing
had asked for a start. The ping's failure became a capability verdict (LOST), and rejj3ega's placement was freed,
so a retire let the VM go with 0171's cleanup pending. Here: boat exec's own refusal to reach a node is typed
transport; a strict ping lost to it takes the preparation's retry and ends NOT_STARTED; a strict placement stays held
until its cleanup receipt. Run on boat, like all test_*.py."""
import json
import unittest
from unittest.mock import Mock, patch

import test_owned_scheduler as fixture
from supercomputer import boat
from supercomputer import scheduler as S

REFUSED = {"exitCode": -1, "stdout": "", "stderr": "", "error": "connect ECONNREFUSED 5.196.64.98:21036"}


class ExecUnreachableIsTransport(unittest.TestCase):
    def test_the_providers_refusal_to_reach_a_node_is_typed_transport(self):
        self.assertTrue(boat.exec_unreachable(REFUSED))
        self.assertFalse(boat.exec_unreachable({**REFUSED, "stderr": "connect ECONNREFUSED 1.2.3.4:80"}))
        self.assertFalse(boat.exec_unreachable({"exitCode": 1, "stderr": "Traceback ...", "error": ""}))
        self.assertFalse(boat.exec_unreachable({"exitCode": -1, "error": "Sandbox bx_x is stopped. Run `boat resume`"}))

    def test_rpc_by_exec_raises_transport_for_it_and_plain_agent_error_otherwise(self):
        client = boat.AgentClient.__new__(boat.AgentClient)
        client.vm, client.token = "bx_test", "t"
        with patch.object(boat, "exec_", return_value=REFUSED):
            with self.assertRaises(boat.AgentTransportError):
                client._rpc_exec(b"{}", 5)
        with patch.object(boat, "exec_", return_value={"exitCode": 1, "stdout": "", "stderr": "urlopen error"}):
            with self.assertRaises(boat.AgentError) as caught:
                client._rpc_exec(b"{}", 5)
            self.assertNotIsInstance(caught.exception, boat.AgentTransportError)


class StrictPingTransport(unittest.TestCase):
    setUp = fixture.OwnedSchedulerTests.setUp
    write_batch = fixture.OwnedSchedulerTests.write_batch
    place_without_thread = fixture.OwnedSchedulerTests.place_without_thread

    def start(self, ping_error):
        self.it.spec["required_agent_capabilities"] = {"owned_rpc_idempotency_protocol": 1}
        self.vm.agent = "agent-v"
        self.vm.agent_capabilities = {"owned_rpc_idempotency_protocol": 1}
        self.place_without_thread()
        self.it.last_placement["agent_version"] = "agent-v"
        self.vm.client.ping.side_effect = ping_error
        with patch.object(S, "BOAT_TRIES", 1), patch.object(self.s, "_owned_client", return_value=Mock()), \
                patch.object(self.s, "_prepare"), patch.object(self.s, "_note_strict_front_failure"), \
                patch.object(S.lease_diagnostics, "event", return_value={"error": {"message": str(ping_error)}}):
            self.s._start_on_inner(self.it, self.vm)
        return self.b.rows[-1]

    def test_a_ping_lost_to_transport_is_not_started_and_keeps_its_placement(self):
        row = self.start(boat.AgentTransportError("bx_t: the agent didn't answer, by boat's front nor by boat exec "
                                                  "(connect ECONNREFUSED 5.196.64.98:21036)"))
        self.assertEqual(row["status"], "NOT_STARTED", row)
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)  # (held for its cleanup receipt)
        attempts = (self.root / "queue" / f"{self.b.id}.attempts").read_text().splitlines()
        self.assertFalse(any(json.loads(l).get("event") == "start_requested" for l in attempts))
        self.assertTrue(any(json.loads(l).get("event") == "prelaunch_not_started" for l in attempts))

    def test_an_untyped_ping_failure_stays_lost_and_keeps_its_placement(self):
        row = self.start(boat.AgentError("bx_t: ping: {'ok': False}"))
        self.assertEqual(row["status"], "LOST", row)
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)


if __name__ == "__main__":
    unittest.main()
