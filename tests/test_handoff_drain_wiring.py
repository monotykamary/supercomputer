"""Executable bounded-drain wiring tests; all provider and child I/O is fake.

Run on the existing Boat gate image, never against a live controller.  Deliberate
callbacks at I/O boundaries model interleavings without sleeps or racing threads.
The HandoffDrain truth table is tested separately in test_handoff_drain_state.py.
"""
import contextlib
import pathlib
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat, scheduler as S
import test_owned_scheduler as owned_fixture


class HandoffDrainWiring(unittest.TestCase):
    # Reuse setup, not inheritance: do not accidentally run the owned-RPC suite.
    setUp_owned = owned_fixture.OwnedSchedulerTests.setUp
    write_batch = owned_fixture.OwnedSchedulerTests.write_batch

    def patch(self, obj, name, **kwargs):
        p = mock.patch.object(obj, name, **kwargs)
        value = p.start()
        self.addCleanup(p.stop)
        return value

    def setUp(self):
        self.setUp_owned()
        self.clock = 1000.0
        self.target = "new"
        self.s.code = "old"
        self.s.asking, self.s.creating, self.s.backoff = set(), {}, {}
        self.s.adopting = False
        self.s.idle_exit = 60
        self.s.canary = mock.Mock()
        self.s.canary.snapshot.return_value = self.root / "code" / "new"
        self.s.alert = mock.Mock()
        self.s._durable_snapshot = mock.Mock(return_value={})
        self.s._imports = mock.Mock(return_value=True)
        for name in ("old", "new", "other"):
            (self.root / "code" / name / "supercomputer").mkdir(parents=True)
        self.patch(S.time, "monotonic", side_effect=lambda: self.clock)
        self.patch(S, "code_hash", side_effect=lambda *a: self.target)
        self.patch(S, "installed_source", return_value=self.root)
        self.patch(S, "log")
        self.prepared = self.patch(S, "prepare_snapshot", return_value={
            "runtime_pins_protocol": True, "resource_limits_protocol": True,
            "required_agent_capabilities_protocol": True, "cpu_model_exact": True,
            "distinct_vm_group": True, "result_files_protocol": True,
            "durable_distinct_vm_group": True})
        self.states = self.patch(boat, "states", return_value={})
        # A forgotten provider/child call must not turn this into an integration run.
        self.popen = self.patch(S.subprocess, "Popen", side_effect=AssertionError("real child spawn"))
        self.patch(boat, "boat", side_effect=AssertionError("real provider call"))
        self.patch(boat, "exec_json", side_effect=AssertionError("real provider exec"))
        self.patch(boat, "bootstrap_agent", side_effect=AssertionError("real bootstrap"))
        self.patch(boat, "lease_vm", side_effect=AssertionError("real lease"))
        self.patch(boat, "sandboxes", side_effect=AssertionError("real discovery"))
        self.patch(boat, "agent_health", side_effect=AssertionError("real health"))
        self.patch(boat, "limits", side_effect=AssertionError("real limits"))
        self.patch(boat, "remember_lease")
        self.registry_data = {"vms": {}}
        registry = mock.Mock()

        @contextlib.contextmanager
        def locked():
            yield self.registry_data

        registry.locked = locked
        registry.read.return_value = self.registry_data
        self.patch(S, "Registry", return_value=registry)

    def begin_wait(self):
        self.vm.ready = False
        self.s.handoff()
        self.assertTrue(self.s._handoff_gate().pending)
        self.assertFalse(self.s._handoff_gate().freezing)
        return self.s._handoff_pending

    def assert_open(self):
        self.assertIsNone(self.s._handoff_pending)
        self.assertTrue(self.s._handoff_gate().growth_allowed())
        self.assertTrue(self.s._handoff_gate().placement_allowed())
        self.assertFalse(getattr(self.s, "handing_off", False))
        self.assertFalse(self.s._handoff_preparing)

    def place(self):
        with mock.patch.object(S.threading, "Thread"):
            return self.s._place(self.it, self.vm)

    def test_repeated_same_target_preserves_deadline_preparation_and_ready_placement(self):
        pending = self.begin_wait()
        gate = self.s._handoff_gate()
        original = (gate.generation, gate.started, gate.deadline)
        prepared = pending["prepared"]
        for elapsed in (5, 60, 299):
            self.clock = 1000 + elapsed
            self.s.handoff()
            self.assertIs(self.s._handoff_pending, pending)
            self.assertIs(self.s._handoff_pending["prepared"], prepared)
            self.assertEqual((gate.generation, gate.started, gate.deadline), original)
        self.assertEqual(gate.deadline, 1300)
        self.assertEqual(self.prepared.call_count, 2)  # target plus rollback, once
        self.s._imports.assert_called_once_with("new")
        self.s.canary.snapshot.assert_called_once()
        self.vm.ready = True
        self.assertTrue(self.place())  # real placement/journal, not just a gate predicate
        self.assertEqual(self.it.state, "starting")
        self.popen.assert_not_called()

    def test_unready_or_upgrading_at_deadline_defers_without_rejection_or_child(self):
        for blocker in ("boot", "upgrade"):
            with self.subTest(blocker=blocker):
                if getattr(self.s, "_handoff_retry", None):
                    self.s._handoff_retry = None
                self.clock += 1000
                self.begin_wait()
                if blocker == "upgrade":
                    self.vm.ready, self.vm.upgrading = True, True
                self.clock = self.s._handoff_gate().deadline
                self.s.handoff()
                self.assert_open()
                alert = self.s.alert.call_args.args[0]
                self.assertIn("bounded drain expired", alert)
                self.assertIn("retry", alert)
                self.assertIn("60", alert)
                self.assertIn(self.vm.id, alert)
                self.assertEqual(self.s._handoff_retry, ("new", None, self.clock + 60))
                self.assertIn(self.vm.id, self.s.vms)
                self.vm.upgrading = False
        self.s.canary.handoff_failed.assert_not_called()
        self.s.canary.handing_off.assert_not_called()
        self.popen.assert_not_called()

    def test_sixty_second_cooldown_cannot_immediately_refence(self):
        self.begin_wait()
        self.clock = 1300
        self.s.handoff()
        count = self.prepared.call_count
        for now in (1300, 1330, 1359.999):
            self.clock = now
            self.s.handoff()
            self.assert_open()
            self.assertEqual(self.prepared.call_count, count)
        self.clock = 1360
        self.s.handoff()
        self.assertTrue(self.s._handoff_gate().pending)
        self.assertEqual(self.s._handoff_gate().deadline, 1660)
        self.assertEqual(self.prepared.call_count, count + 2)

    def test_stale_capacity_plan_cannot_start_ask_after_pending_gate(self):
        self.s.queued = mock.Mock(return_value=[])
        self.s._release_idle = mock.Mock()

        def plan(now):
            self.begin_wait()  # deterministic interleave after planning, before reservation
            return {(self.b.image, self.vm.type): (1, self.b.klass, self.b, True)}

        self.s._plan = mock.Mock(side_effect=plan)
        with mock.patch.object(S.threading, "Thread") as thread:
            self.s.schedule()
        thread.assert_not_called()
        self.assertEqual(self.s.asking, set())
        self.assertTrue(self.s._handoff_gate().pending)

    def test_new_direct_acquire_is_rejected_while_pending(self):
        self.begin_wait()
        with mock.patch.object(self.s, "_acquire_reserved") as acquire:
            self.assertEqual(self.s._acquire(self.b.image, self.vm.type, 1, 3, self.b), 0)
        acquire.assert_not_called()
        self.assertEqual(getattr(self.s, "_acquiring", 0), 0)

    def test_another_ask_for_same_image_does_not_authorize_a_new_direct_acquire(self):
        self.s.asking.add(self.b.image)
        self.begin_wait()
        with mock.patch.object(self.s, "_acquire_reserved", return_value=1) as acquire:
            self.assertEqual(self.s._acquire(self.b.image, self.vm.type, 1, 3, self.b), 0)
        acquire.assert_not_called()
        self.assertEqual(self.s.asking, {self.b.image})

    def test_reserved_ask_drains_actual_acquire_and_stays_counted_until_publication(self):
        self.s.asking.add(self.b.image)
        self.s.retired = lambda: set()
        self.registry_data["vms"]["warm"] = {
            "snapshot": self.b.image, "type": self.vm.type,
            "state": "running", "until": 10**20}
        checkpoints = []

        def limits():
            self.assertEqual(self.s._acquiring, 1)
            self.begin_wait()  # already reserved _ask may finish despite this new gate
            return {}

        def bring(vid, image, vm_type, how, k, batch, **kw):
            self.assertEqual(self.s._acquiring, 1)
            self.assertFalse(self.s._pool_settled())
            checkpoints.append(vid)
            # Run real publication; suppress only its unrelated bootstrap thread.
            S.Scheduler._bring(self.s, vid, image, vm_type, how, k, batch, **kw)
            self.assertIn(vid, self.s.vms)
            self.assertEqual(self.s._acquiring, 1)
            self.assertFalse(self.s._pool_settled())

        with mock.patch.object(boat, "limits", side_effect=limits), \
             mock.patch.object(boat, "starts_left", return_value=0), \
             mock.patch.object(boat, "budget", return_value=0), \
             mock.patch.object(boat, "sandboxes", return_value={}), \
             mock.patch.object(boat, "reconcile"), \
             mock.patch.object(boat, "lease_free", return_value=True), \
             mock.patch.object(self.s, "_bring", side_effect=bring), \
             mock.patch.object(S.threading, "Thread"):
            self.s._ask(self.b.image, self.vm.type, 1, 3, self.b, True)
        self.assertEqual(checkpoints, ["warm"])
        self.assertEqual(self.s._acquiring, 0)
        self.assertEqual(self.s.asking, set())
        self.assertFalse(self.s.vms["warm"].ready)
        self.assertFalse(self.s._pool_settled())

    def adoption_inventory(self):
        self.s.queued = lambda: [self.it]
        self.s.retired = lambda: set()
        self.s.adopting = True
        return {"adopt": {"name": self.b.image + "~l", "state": "running", "type": self.vm.type}}

    def test_adoption_discovery_racing_pending_does_not_claim(self):
        inventory = self.adoption_inventory()

        def health(vid):
            self.begin_wait()
            return {"lease": {"state": "released"}}

        with mock.patch.object(boat, "sandboxes", return_value=inventory), \
             mock.patch.object(boat, "agent_health", side_effect=health), \
             mock.patch.object(boat, "lease_vm") as lease, \
             mock.patch.object(self.s, "_bring") as bring:
            self.s._adopt()
        lease.assert_not_called()
        bring.assert_not_called()
        self.assertFalse(self.s.adopting)
        self.assertEqual(getattr(self.s, "_adopt_claims", 0), 0)

    def test_reserved_adoption_claim_is_counted_through_real_bring(self):
        inventory = self.adoption_inventory()
        points = []

        def claim(vid, operation):
            self.assertEqual(operation, "claim")
            self.assertEqual(self.s._adopt_claims, 1)
            self.begin_wait()
            self.assertFalse(self.s._pool_settled())
            return {"ok": True, "boot": "boot", "epoch": 1}

        def bring(*args, **kwargs):
            self.assertEqual(self.s._adopt_claims, 1)
            self.assertFalse(self.s._pool_settled())
            S.Scheduler._bring(self.s, *args, **kwargs)
            points.append(self.s._adopt_claims)
            self.assertIn("adopt", self.s.vms)

        with mock.patch.object(boat, "sandboxes", return_value=inventory), \
             mock.patch.object(boat, "agent_health", return_value={"lease": {"state": "released"}}), \
             mock.patch.object(boat, "lease_vm", side_effect=claim), \
             mock.patch.object(self.s, "_bring", side_effect=bring), \
             mock.patch.object(S.threading, "Thread"):
            self.s._adopt()
        self.assertEqual(points, [1])  # catches _adopt swallowing callback assertions
        self.assertEqual(self.s._adopt_claims, 0)
        self.assertFalse(self.s._pool_settled())  # published, still booting
        self.assertFalse(self.s.adopting)

    def test_check_agent_probe_racing_gate_never_starts_upgrade(self):
        self.vm.agent = "old-agent"
        self.s._renew_lease = mock.Mock(return_value=True)

        def ping():
            self.begin_wait()
            return {"ok": True, "version": "old-agent"}

        self.vm.client.ping.side_effect = ping
        self.s._check_agent(self.vm)
        self.vm.client.rpc.assert_not_called()
        self.assertFalse(self.vm.upgrading)
        self.assertEqual(self.vm.agent, "old-agent")

    def test_repair_during_pending_never_bootstraps_or_changes_upgrade_state(self):
        self.begin_wait()
        self.s._renew_lease = mock.Mock(return_value=True)
        self.assertFalse(self.s._repair(self.vm))
        self.s._renew_lease.assert_not_called()
        boat.bootstrap_agent.assert_not_called()
        self.assertFalse(self.vm.upgrading)
        self.assertTrue(self.vm.repair_lock.acquire(blocking=False))
        self.vm.repair_lock.release()

    def freeze_with_start(self):
        self.assertTrue(self.place())
        self.assertFalse(self.it.start_done.is_set())
        self.s.handoff()
        self.assertTrue(self.s._handoff_gate().freezing)
        self.assertFalse(self.s._handoff_gate().placement_allowed())

    def test_strict_start_final_fence_blocks_real_placement_and_expires_open(self):
        self.freeze_with_start()
        second = S.Item(self.b, {"name": "second", "cmd": "true"})
        with mock.patch.object(S.threading, "Thread") as thread:
            self.assertFalse(self.s._place(second, self.vm))
            self.s.schedule()  # final gate must return before planning/asking
        thread.assert_not_called()
        self.clock = self.s._handoff_gate().deadline
        self.s.handoff()
        self.assert_open()
        with mock.patch.object(S.threading, "Thread"):
            self.assertTrue(self.s._place(second, self.vm))
        self.assertFalse(self.it.start_done.is_set())  # expiry did not discard original ownership
        self.assertIn((self.b.id, self.it.run_name), self.vm.running)
        self.popen.assert_not_called()

    def test_already_placed_start_can_settle_while_final_fence_is_closed(self):
        self.freeze_with_start()
        self.s._prepare = mock.Mock()

        def rpc(command, request, **kwargs):
            self.assertEqual(command, "owned_rpc")
            return {"ok": True, "protocol": 1, "version": self.s.agent_version,
                    "nonce": request["nonce"], "features": {
                        "pidfd": True, "private_pid_namespace": "verified-before-payload"},
                    "result": {"done": [], "running": []}}

        self.vm.client.rpc.side_effect = rpc
        self.s._start_on(self.it, self.vm)
        self.assertTrue(self.it.start_done.is_set())
        self.assertEqual(self.it.state, "running")
        self.assertTrue(self.s._handoff_gate().freezing)
        # Getting as far as snapshot demonstrates the settled launch no longer blocks.
        self.s._metrics_save = mock.Mock()
        self.s._handoff_snapshot = mock.Mock(side_effect=RuntimeError("snapshot boundary"))
        with self.assertRaisesRegex(RuntimeError, "snapshot boundary"):
            self.s.handoff()
        self.s._handoff_snapshot.assert_called_once()
        self.assert_open()

    def test_target_generation_supersession_and_withdrawn_source_reset_pending(self):
        old = self.begin_wait()
        old_generation = old["generation"]
        self.clock += 10
        self.target = "other"
        self.s.handoff()
        current = self.s._handoff_pending
        self.assertEqual(current["target"], "other")
        self.assertNotEqual(current["generation"], old_generation)
        self.assertEqual(self.s._handoff_gate().deadline, 1310)
        self.s._clear_handoff_pending(old_generation)
        self.assertIs(self.s._handoff_pending, current)
        self.assertFalse(self.s._handoff_gate().growth_allowed())
        # Exercise actual sampler cancellation when the installed source withdraws.
        self.target = "old"
        self.s.canary.handback_due.return_value = None
        self.s.stop = mock.Mock()
        self.s.stop.wait.side_effect = [False, True]
        for name in ("_flush_status_timings", "_watch", "_handoff_check", "_breaker_eval"):
            self.patch(self.s, name, return_value=None)
        with mock.patch.object(S.time, "time", return_value=10):
            self.s._sampler()
        self.assert_open()
        self.popen.assert_not_called()

    def test_changed_or_withdrawn_install_must_earn_stability_again(self):
        self.s.canary.handback_due.return_value = None
        self.s.adopting = True

        def sampler_once(now):
            self.s.stop = mock.Mock()
            self.s.stop.wait.side_effect = [False, True]
            with mock.patch.object(S.time, "time", return_value=now), \
                 mock.patch.object(self.s, "_flush_status_timings"), \
                 mock.patch.object(self.s, "_watch"), \
                 mock.patch.object(self.s, "_handoff_check"), \
                 mock.patch.object(self.s, "_breaker_eval"):
                self.s._sampler()

        for changed in ("other", "old"):
            with self.subTest(changed=changed):
                self.target = "new"
                self.s._install = S.rules.Install("old", 60)
                self.s._install.tick(100, "new", False)
                self.begin_wait()
                self.target = changed
                sampler_once(200)
                self.assert_open()
                self.assertNotIn("_install", self.s.__dict__)
                self.target = "new"
                with mock.patch.object(self.s, "handoff") as handoff:
                    sampler_once(201)
                    self.assertEqual(self.s._install.t_seen, 201)
                    sampler_once(260.999)
                    handoff.assert_not_called()
                    sampler_once(261)
                    handoff.assert_called_once_with()

    def test_sampler_cannot_cancel_an_in_progress_handoff_preparation(self):
        pending = self.begin_wait()
        self.s.canary.handback_due.return_value = None
        self.s.adopting = True
        observed = []

        def sampler_once(now):
            self.s.stop = mock.Mock()
            self.s.stop.wait.side_effect = [False, True]
            with mock.patch.object(S.time, "time", return_value=now), \
                 mock.patch.object(self.s, "_flush_status_timings"), \
                 mock.patch.object(self.s, "_watch"), \
                 mock.patch.object(self.s, "_handoff_check"), \
                 mock.patch.object(self.s, "_breaker_eval"):
                self.s._sampler()

        def probe(*_args, **_kwargs):
            # A /reload retry is in provider observation while the ordinary
            # sampler observes another install. Cancellation must not open its
            # growth fence beneath the preparing handoff's local state.
            self.assertTrue(self.s._handoff_preparing)
            self.target = "other"
            sampler_once(200)
            observed.append((self.s._handoff_pending is pending,
                             self.s._handoff_gate().pending))
            return {}

        with mock.patch.object(boat, "states", side_effect=probe):
            self.s.handoff()
        self.assertEqual(observed, [(True, True)])
        self.assert_open()  # The preparing retry releases its invalidated fence.
        sampler_once(205)
        self.assert_open()

    def test_source_return_cannot_erase_deferred_invalidation(self):
        self.s._install = S.rules.Install("old", 60)
        self.s._install.tick(100, "new", False)
        pending = self.begin_wait()
        self.s.canary.handback_due.return_value = None
        self.s.adopting = True
        observed = []

        def sampler_once(now):
            self.s.stop = mock.Mock()
            self.s.stop.wait.side_effect = [False, True]
            with mock.patch.object(S.time, "time", return_value=now), \
                 mock.patch.object(self.s, "_flush_status_timings"), \
                 mock.patch.object(self.s, "_watch"), \
                 mock.patch.object(self.s, "_handoff_check"), \
                 mock.patch.object(self.s, "_breaker_eval"):
                self.s._sampler()

        def probe(*_args, **_kwargs):
            self.target = "other"
            sampler_once(200)
            observed.append((self.s._handoff_pending is pending,
                             not self.s._handoff_gate().growth_allowed()))
            self.assertNotIn("_install", self.s.__dict__)
            self.target = "new"  # Returns before the preparing retry gives up.
            return {}

        with mock.patch.object(boat, "states", side_effect=probe):
            self.s.handoff()
        self.assertEqual(observed, [(True, True)])
        self.assert_open()
        with mock.patch.object(self.s, "handoff") as handoff:
            sampler_once(201)
            self.assertEqual(self.s._install.t_seen, 201)
            sampler_once(260.999)
            handoff.assert_not_called()
            sampler_once(261)
            handoff.assert_called_once_with()

    def test_empty_reattachment_pass_clears_the_handoff_dependency(self):
        self.s.reattaching = True
        self.s.reattach_pending = set()
        self.it.reattach = None  # The handoff already attached this item.
        self.s.adopting = True  # No unrelated adoption thread in this test.
        self.assertFalse(self.s._pool_settled())
        self.s._reattach()
        self.assertFalse(self.s.reattaching)
        self.assertEqual(self.s.reattach_pending, set())
        self.assertTrue(self.s._pool_settled())

    def test_candidate_rejection_restores_both_gates_and_keeps_pool(self):
        self.begin_wait()
        self.vm.ready = True
        self.s._metrics_save = mock.Mock()
        self.s._handoff_snapshot = mock.Mock(return_value={"vms": []})
        child = mock.Mock()
        child.poll.return_value = 17  # already dead; no process signal or real wait
        self.popen.side_effect = None
        self.popen.return_value = child
        self.s.handoff()
        self.assertEqual(self.popen.call_count, 2)  # candidate and saved rollback both refused
        self.assertEqual(child.wait.call_count, 2)
        self.assert_open()
        self.assertIs(self.s.vms[self.vm.id], self.vm)
        self.s.canary.handoff_failed.assert_called_once()
        self.assertFalse(self.s.canary.handoff_failed.call_args.args[2])
        self.assertTrue(self.place())


if __name__ == "__main__":
    unittest.main()
