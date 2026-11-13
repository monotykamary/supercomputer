"""Required-pool takeover never contacts or relabels unoffered registry extras."""
import contextlib
import copy
import json
import pathlib
import socket
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat, scheduler


class TakePoolExtras(unittest.TestCase):
    def take(self, handoff, extra_state="idle"):
        with tempfile.TemporaryDirectory() as tmp, contextlib.ExitStack() as stack:
            root = pathlib.Path(tmp)
            (root / "queue").mkdir()
            host = socket.gethostname()
            records = {name: {"type": "large", "snapshot": "image", "state": "idle",
                              "lease": {"pid": pid, "host": host, "batch": "scheduler"}}
                       for name, pid in (("offered", 11), ("extra", 22))}
            records["extra"]["state"] = extra_state
            original = copy.deepcopy(records)
            leases = {name: {"owner": "owner", "vm_id": name, "boot": "boot", "epoch": 7}
                      for name in records}
            if handoff:
                (root / "handoff.json").write_text(json.dumps({
                    "from_pid": 11, "t": time.time(), "admission_guard": True, "batches": [],
                    "vms": [{"id": "offered", "image": "image", "type": "large", "slots": 4,
                             "lease": leases["offered"], "items": []}]}))
            s = object.__new__(scheduler.Scheduler)
            s.vms, s.batches, s.recovery_errors, s.held_before = {}, {}, {}, {}
            s.code, s.agent_b64 = "new", ""
            s._reload_started = time.monotonic()
            s.retired = lambda: set()
            s._read_client_closes = lambda: set()
            s._distinct_snapshot = s._durable_snapshot = lambda: {}
            s._h, s._worker = mock.Mock(), mock.Mock()
            s.canary = mock.Mock()
            s.canary.admissions.return_value = ({"ready": True}, "", "")
            s._renew_lease = mock.Mock(return_value=True)
            reg = mock.Mock()
            @contextlib.contextmanager
            def locked():
                yield {"vms": records}
            reg.locked = locked
            agent_client = mock.Mock(side_effect=lambda vid: types.SimpleNamespace(lease=None))
            threads = types.SimpleNamespace(**vars(threading))
            threads.Thread = mock.Mock()  # Suppress background TTL threads, not the acquisition executor.
            patches = [(scheduler, "threading", threads), (scheduler, "DIR", root), (scheduler, "Registry", lambda: reg),
                       (scheduler, "log", lambda *a: None), (boat, "TYPES", {"large": (8, 16384)}),
                       (boat, "pid_alive", lambda pid, host: handoff and pid == 11),
                       (boat, "owner_id", lambda: "owner"), (boat, "agent_client", agent_client),
                       (boat, "read_agent", lambda vid: {"lease": leases[vid]}),
                       (boat, "remember_lease", lambda *a: None), (boat, "rename_leased_vm", lambda *a: None),
                       (boat, "boat", lambda *a, **kw: "")]
            for obj, name, value in patches:
                stack.enter_context(mock.patch.object(obj, name, value))
            s._take_pool()
            return s, records, original, agent_client

    def test_handoff_leaves_extra_registry_lease_and_agent_untouched(self):
        s, records, original, clients = self.take(True)
        self.assertEqual(set(s.vms), {"offered"})
        self.assertEqual(records["extra"], original["extra"])
        self.assertEqual([c.args[0] for c in clients.call_args_list], ["offered"])
        s._renew_lease.assert_not_called()
        self.assertEqual(s._handoff_ack["taken"], ["offered"])
        self.assertEqual(s._handoff_ack["missing"], [])

    def test_stopped_extra_is_in_baseline_before_it_can_become_idle(self):
        s, records, original, clients = self.take(True, "stopped")
        self.assertIn("extra", s._handoff["deferred_extras"])
        self.assertNotIn("extra", s.vms)
        self.assertEqual(records["extra"], original["extra"])
        self.assertEqual([c.args[0] for c in clients.call_args_list], ["offered"])

    def test_cold_restart_still_takes_dead_registry_holds(self):
        s, records, original, clients = self.take(False)
        self.assertEqual(set(s.vms), {"offered", "extra"})
        self.assertEqual(s._renew_lease.call_count, 2)
        self.assertEqual({c.args[0] for c in clients.call_args_list}, {"offered", "extra"})
        self.assertTrue(all(r["lease"]["pid"] != original[n]["lease"]["pid"] for n, r in records.items()))


class DeferredAdoption(unittest.TestCase):
    def candidate(self):
        s = object.__new__(scheduler.Scheduler)
        s._handoff = {"snapshot": True, "from_pid": 11}
        s.recovery_blocked, s.adopting = False, True
        s.stop, s.wake, s.lock = mock.Mock(), mock.Mock(), threading.RLock()
        s.stop.is_set.return_value = False
        s.queued = lambda: []
        return s

    def test_adoption_waits_until_parent_commits(self):
        s = self.candidate()
        s.stop.wait.return_value = False
        with mock.patch.object(boat, "pid_alive", side_effect=[True, False]), \
             mock.patch.object(boat, "sandboxes", return_value={}) as inventory:
            s._adopt()
        s.stop.wait.assert_called_once_with(0.05)
        inventory.assert_called_once()
        self.assertFalse(s.adopting)

    def test_rejected_candidate_never_attempts_adoption(self):
        s = self.candidate()
        s.stop.wait.return_value = True
        with mock.patch.object(boat, "pid_alive", return_value=True), \
             mock.patch.object(boat, "sandboxes") as inventory:
            s._adopt()
        inventory.assert_not_called()
        self.assertFalse(s.adopting)

    def test_canary_excludes_only_pending_extras_not_required_or_acquired_vms(self):
        s = self.candidate()
        s.code = "new"
        s._handoff.update(t=1000, held=["required"], checks=0, deferred_extras=["extra"])
        s.vms = {"required": types.SimpleNamespace(dead=False)}
        s._h, s._handoff_idle_slots = mock.Mock(), mock.Mock(return_value=0)
        s.canary = mock.Mock()
        s.canary.handoff_verdict.return_value = ("", "")
        entries = {name: {"state": "idle", "lease": {"pid": 22, "host": socket.gethostname(), "batch": "scheduler"}}
                   for name in ("required", "extra", "other")}
        registry = mock.Mock()
        registry.read.return_value = {"vms": entries}
        with mock.patch.object(scheduler, "Registry", return_value=registry), \
             mock.patch.object(boat, "pid_alive", return_value=False), mock.patch.object(scheduler, "log"):
            s._handoff_check(1015)
            self.assertEqual(s.canary.handoff_verdict.call_args.args[1], 2)
            s.vms["extra"] = types.SimpleNamespace(dead=False)
            s._handoff_check(1030)
            self.assertEqual(s.canary.handoff_verdict.call_args.args[1], 3)

    def test_required_orphans_and_acquired_extras_are_still_checked(self):
        from supercomputer.rules import updates
        for deferred in (False, True):
            for acquired in (False, True):
                self.assertEqual(updates.handoff_orphan_scope(deferred, acquired), not deferred or acquired)
        for handoff in (False, True):
            for offered in (False, True):
                self.assertEqual(updates.handoff_registry_vm(handoff, offered), not handoff or offered)


class InflightHandoff(unittest.TestCase):
    def test_idle_and_handoff_share_actual_inflight_state(self):
        s=object.__new__(scheduler.Scheduler)
        s.asking,s.creating,s.vms,s.adopting=set(),{},{},False
        self.assertTrue(s._pool_settled())
        s.vms['booting']=types.SimpleNamespace(ready=False,dead=False)
        self.assertFalse(s._pool_settled())
        s.vms['booting'].ready=True
        self.assertTrue(s._pool_settled())
        s.creating[('image','xlarge')]=1
        self.assertFalse(s._pool_settled())
        s.creating.clear();s.asking.add("image")
        self.assertFalse(s._pool_settled())
        s.asking.clear();s.adopting=True
        self.assertTrue(s._pool_settled())  # discovery alone is not an acquisition

    def test_snapshot_waits_for_acquisitions_creates_and_booting_workers(self):
        from supercomputer.rules import updates
        for acquiring in (False, True):
            for creating in (0, 1, 10):
                for booting in (0, 1, 10):
                    self.assertEqual(updates.handoff_pool_settled(acquiring, creating, booting),
                                     not acquiring and creating == 0 and booting == 0)
        for creating, booting in ((-1, 0), (0, -1)):
            with self.assertRaises(ValueError):
                updates.handoff_pool_settled(False, creating, booting)


if __name__ == "__main__":
    unittest.main()
