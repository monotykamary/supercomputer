"""Injected recovery contract tests. Run only in the main supercomputer test batch."""
import copy
import hashlib
import itertools
import json
import unittest

from supercomputer import namespace_recovery as R


BINDING = {"batch": "batch-original", "name": "item", "run_name": "item-a1",
           "attempt": 1, "vm": "immutable-vm", "boot_id": "old-kernel",
           "launch_identity": "original-launch-nonce", "owner": "original-owner"}


class MemoryStore:
    def __init__(self):
        self.intents = {}
        self.proofs = {}
        self.events = []
        self.fail_intent = None
        self.fail_proof = False
        self.bad_readback = False

    def persist_intent(self, value):
        self.events.append(("intent", copy.deepcopy(value)))
        if self.fail_intent and self.fail_intent(value):
            raise OSError("intent fsync failed")
        self.intents[value["intent_id"]] = copy.deepcopy(value)

    def load_intent(self, iid):
        if self.bad_readback:
            return None
        return copy.deepcopy(self.intents.get(iid))

    def persist_proof(self, proof):
        self.events.append(("proof", copy.deepcopy(proof)))
        if self.fail_proof:
            raise OSError("proof fsync failed")
        self.proofs[proof["intent_id"]] = copy.deepcopy(proof)


class FakeTransport:
    def __init__(self, store):
        self.store = store
        self.calls = []
        self.row = {"identity": BINDING["launch_identity"], "state": "launched",
                    "reaped": True, "contained": False, "cancelled": True}
        self.original_row = copy.deepcopy(self.row)
        self.lease = {"ok": True, "owner": BINDING["owner"], "vm_id": BINDING["vm"],
                      "boot": BINDING["boot_id"], "epoch": 9, "state": "released",
                      "lapsed": False, "valid_boot": True}
        self.capsule_change = lambda c, fenced: c
        self.boot_change = lambda b: b
        self.stop_change = lambda r: r
        self.stopped_change = lambda r: r
        self.resume_change = lambda r: r
        self.new_boot = "new-kernel"
        self.capsule_boot = BINDING["boot_id"]
        self.fail_stop = False
        self.time = 0
        self.resume_delay = 0

    def record(self, name):
        self.calls.append(name)
        self.store.events.append((name, None))

    def read_capsule(self, binding, nonce, fence=None, *, deadline):
        self.record("fenced_capsule" if fence else "capsule")
        c = {"kind": R.CAPSULE_KIND, "nonce": nonce, "binding": copy.deepcopy(binding),
             "complete": True, "actual_vm_id": BINDING["vm"], "boot_id": self.capsule_boot,
             "owned_record": copy.deepcopy(self.row), "owned_record_sha256": R.digest(self.row),
             "owner_tombstone": {k: BINDING[k] for k in ("batch", "run_name", "owner")},
             "job_dir_absent": True, "init_absent": True, "supervisor_absent": True,
             "native_jobs": [], "other_unresolved_owned_items": [], "lease": copy.deepcopy(self.lease)}
        return self.capsule_change(c, fence is not None)

    def read_lease(self, vm, *, deadline):
        self.record("read_lease")
        return copy.deepcopy(self.lease)

    def acquire_lease(self, vm, owner, *, deadline):
        self.record("claim")
        self.lease.update(state="held", epoch=self.lease["epoch"] + 1)
        return copy.deepcopy(self.lease)

    def fence_stop(self, vm, lease, *, deadline):
        self.record("fence")
        self.lease.update(state="stopping", stop_nonce="exact-stop-fence")
        return copy.deepcopy(self.lease)

    def stop_vm(self, vm, fence, *, preserve_disk, deadline):
        self.record("stop")
        if self.fail_stop:
            raise RuntimeError("provider stop failed")
        return self.stop_change({"vm_id": vm, "operation_id": "local-stop-label",
                                 "accepted": True, "preserve_disk": preserve_disk})

    def observe_stopped(self, vm, stop_receipt, *, deadline):
        self.record("observe")
        return self.stopped_change({"vm_id": vm, "operation_id": stop_receipt["operation_id"],
                                    "source": "provider", "observed": True, "state": "stopped"})

    def resume_vm(self, vm, *, deadline):
        self.record("resume")
        self.time += self.resume_delay
        return self.resume_change({"vm_id": vm, "operation_id": "local-resume-label", "accepted": True})

    def read_boot(self, vm, nonce, *, deadline):
        self.record("boot")
        return self.boot_change({"kind": "native-boot/v1", "vm_id": vm, "nonce": nonce,
                                 "boot_id": self.new_boot, "source": "native"})


class NamespaceRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        self.t = FakeTransport(self.store)
        self.binding = copy.deepcopy(BINDING)
        self.auth = {"explicit": True, "reference": "operator-ticket-123", "binding": self.binding}
        self.counter = itertools.count()

    def kwargs(self):
        return {"binding": self.binding, "authorization": self.auth,
                "persist_intent": self.store.persist_intent, "load_intent": self.store.load_intent,
                "persist_proof": self.store.persist_proof,
                "monotonic": lambda: self.t.time, "clock": lambda: 100,
                "nonce_factory": lambda: "fresh-%d" % next(self.counter)}

    def recover(self):
        return R.recover(self.t, **self.kwargs())

    def replaced(self):
        self.t.capsule_boot = self.t.new_boot
        return R.recover_replaced_boot(self.t, **self.kwargs())

    def intent(self):
        return self.store.load_intent(R.intent_id(self.binding))

    def assert_pending(self):
        self.assertEqual(self.intent()["state"], "pending")
        self.assertNotIn("proof_sha256", self.intent())

    def test_fenced_stop_resume_proof_and_original_row_unchanged(self):
        p = self.recover()
        self.assertTrue(R.validate_proof(p, self.binding, self.intent()))
        self.assertEqual(p["kind"], "vm-boot-extinction/v1")
        self.assertEqual(p["method"], "fenced-stop-resume")
        self.assertEqual(p["new_boot_id"], "new-kernel")
        self.assertEqual(self.t.row, self.t.original_row)
        self.assertEqual(self.t.calls, ["capsule", "read_lease", "claim", "fence", "fenced_capsule",
                                       "stop", "observe", "resume", "boot"])
        for key in ("ok", "contained", "reaped", "namespace_absent", "cleanup_complete"):
            self.assertNotIn(key, p)
        events = [e[0] for e in self.store.events]
        self.assertLess(events.index("intent"), events.index("claim"))
        self.assertLess(events.index("intent"), events.index("stop"))
        self.assertEqual(p, self.store.proofs[p["intent_id"]])

    def test_held_lease_does_not_claim(self):
        self.t.lease["state"] = "held"
        self.recover()
        self.assertNotIn("claim", self.t.calls)

    def test_no_opt_in_no_operation(self):
        for change in ({"explicit": False}, {"reference": ""}, {"binding": {}}):
            with self.subTest(change=change):
                auth = {**self.auth, **change}
                with self.assertRaises(R.RecoveryError):
                    R.recover(self.t, **{**self.kwargs(), "authorization": auth})
        self.assertEqual(self.t.calls, [])

    def test_missing_or_unbound_capsules_never_mutate(self):
        changes = [{"complete": False}, {"kind": ""}, {"nonce": "replay"}, {"actual_vm_id": "foreign"},
                   {"binding": {}}, {"boot_id": "foreign-boot"}, {"owner_tombstone": None},
                   {"owned_record_sha256": "0" * 64}, {"owned_record": {}}, {"job_dir_absent": False},
                   {"init_absent": False}, {"supervisor_absent": False}, {"native_jobs": [{}]},
                   {"other_unresolved_owned_items": [{"reaped": True, "contained": False}]}]
        for change in changes:
            with self.subTest(change=change):
                self.setUp()
                self.t.capsule_change = lambda c, f: {**c, **change}
                with self.assertRaises(R.RecoveryError):
                    self.recover()
                self.assertEqual(self.t.calls, ["capsule"])

    def test_foreign_owner_veto_even_released(self):
        self.t.lease["owner"] = "foreign-owner"
        with self.assertRaises(R.RecoveryError):
            self.recover()
        self.assertEqual(self.t.calls, ["capsule"])

    def test_lapsed_held_lease_veto(self):
        self.t.lease.update(state="held", lapsed=True)
        with self.assertRaises(R.RecoveryError):
            self.recover()
        self.assertNotIn("fence", self.t.calls)
        self.assert_pending()

    def test_fenced_reprobe_vetoes_new_unresolved_and_foreign_active(self):
        for key in ("native_jobs", "other_unresolved_owned_items"):
            with self.subTest(key=key):
                self.setUp()
                self.t.capsule_change = lambda c, f: {**c, key: [{"owner": "foreign"}]} if f else c
                with self.assertRaises(R.RecoveryError):
                    self.recover()
                self.assertNotIn("stop", self.t.calls)
                self.assert_pending()

    def test_exact_fence_epoch_and_stop_nonce_required(self):
        for field, value in (("epoch", 999), ("stop_nonce", "wrong")):
            with self.subTest(field=field):
                self.setUp()
                def change(c, fenced):
                    if fenced:
                        c["lease"][field] = value
                    return c
                self.t.capsule_change = change
                with self.assertRaises(R.RecoveryError):
                    self.recover()
                self.assertNotIn("stop", self.t.calls)

    def test_changed_row_under_fence_is_refused(self):
        def change(c, fenced):
            if fenced:
                c["owned_record"]["cancelled"] = False
                c["owned_record_sha256"] = R.digest(c["owned_record"])
            return c
        self.t.capsule_change = change
        with self.assertRaises(R.RecoveryError):
            self.recover()
        self.assertNotIn("stop", self.t.calls)

    def test_stopped_flag_or_missing_vm_is_not_enough(self):
        for change in ({"state": "missing"}, {"vm_id": "other-vm"}, {"source": "cache"},
                       {"observed": False}, {"operation_id": "wrong"}):
            with self.subTest(change=change):
                self.setUp()
                self.t.stopped_change = lambda r: {**r, **change}
                with self.assertRaises(R.RecoveryError):
                    self.recover()
                self.assertNotIn("resume", self.t.calls)
                self.assert_pending()

    def test_unchanged_kernel_suspend_and_boot_replay_never_prove_extinction(self):
        for change in ({"boot_id": BINDING["boot_id"]}, {"nonce": "fresh-0"}, {"vm_id": "other"},
                       {"source": "provider"}, {"boot_id": ""}):
            with self.subTest(change=change):
                self.setUp()
                self.t.boot_change = lambda b: {**b, **change}
                with self.assertRaises(R.RecoveryError):
                    self.recover()
                self.assert_pending()
                self.assertFalse(self.store.proofs)

    def test_stop_failure_keeps_pending_and_never_resumes(self):
        self.t.fail_stop = True
        with self.assertRaises(R.RecoveryError) as caught:
            self.recover()
        self.assertEqual(caught.exception.intent_id, R.intent_id(self.binding))
        self.assert_pending()
        self.assertNotIn("resume", self.t.calls)
        self.assertFalse(self.store.proofs)
        calls = self.t.calls[:]
        with self.assertRaises(R.RecoveryError):
            self.recover()
        self.assertEqual(self.t.calls, calls)

    def test_intent_failure_before_mutation_and_readback_failure(self):
        for readback in (False, True):
            with self.subTest(readback=readback):
                self.setUp()
                if readback:
                    self.store.bad_readback = True
                else:
                    self.store.fail_intent = lambda i: True
                with self.assertRaises(R.RecoveryError):
                    self.recover()
                self.assertEqual(self.t.calls, ["capsule"])

    def test_proof_and_final_intent_persistence_failures_never_complete(self):
        for failure in ("proof", "complete"):
            with self.subTest(failure=failure):
                self.setUp()
                self.store.fail_proof = failure == "proof"
                self.store.fail_intent = lambda i: failure == "complete" and i["state"] == "complete"
                with self.assertRaises(R.RecoveryError):
                    self.recover()
                self.assert_pending()
                for p in self.store.proofs.values():
                    self.assertFalse(R.validate_proof(p, self.binding, self.intent()))

    def test_timeout_after_resume_allows_only_fresh_readonly_boot_continuation(self):
        self.t.resume_delay = 241
        with self.assertRaises(R.RecoveryError):
            self.recover()
        self.assert_pending()
        self.assertIn("resume", self.intent())
        calls = self.t.calls[:]
        kw = self.kwargs()
        kw.pop("authorization")
        kw.pop("clock")
        p = R.verify_pending_boot(self.t, **kw)
        self.assertEqual(self.t.calls, calls + ["boot"])
        self.assertTrue(R.validate_proof(p, self.binding, self.intent()))

    def test_invalid_budget_does_not_read_vm(self):
        for budget in (241, 269, 270, 1000, 0, -1, float("inf"), float("nan"), True):
            with self.subTest(budget=budget), self.assertRaises(R.RecoveryError):
                R.recover(self.t, **{**self.kwargs(), "budget_s": budget})
        self.assertEqual(self.t.calls, [])

    def test_restart_validation_replay_tamper_and_authoritative_placement(self):
        p = json.loads(json.dumps(self.recover()))
        i = json.loads(json.dumps(self.intent()))
        self.assertTrue(R.validate_proof(p, self.binding, i))
        for key in self.binding:
            wrong = {**self.binding, key: 2 if key == "attempt" else "different"}
            self.assertFalse(R.validate_proof(p, wrong, i), key)
        for key, value in (("owner", "foreign"), ("new_boot_id", "forged"), ("old_boot_id", "new"),
                           ("owned_record_sha256", "0" * 64), ("kind", "native"),
                           ("intent_id", "another"), ("ok", True)):
            self.assertFalse(R.validate_proof({**p, key: value}, self.binding, i), key)
        self.assertFalse(R.validate_proof(p, self.binding, {**i, "state": "pending"}))
        self.assertFalse(R.validate_proof(p, self.binding, {}))
        self.assertFalse(R.validate_proof(p, self.binding, None))

    def test_nonce_factory_replay_vetoes_before_stop(self):
        with self.assertRaises(R.RecoveryError):
            R.recover(self.t, **{**self.kwargs(), "nonce_factory": lambda: "constant"})
        self.assertNotIn("stop", self.t.calls)
        self.assert_pending()

    def test_raw_owned_record_sha_is_preserved(self):
        raw = json.dumps(self.t.row, indent=2) + "\n"
        def change(c, fenced):
            c["owned_record_raw"] = raw
            c["owned_record_sha256"] = hashlib.sha256(raw.encode()).hexdigest()
            return c
        self.t.capsule_change = change
        p = self.recover()
        self.assertEqual(p["owned_record_sha256"], hashlib.sha256(raw.encode()).hexdigest())
        self.assertTrue(R.validate_proof(p, self.binding, self.intent()))

    def test_observed_replacement_never_mutates_or_rejects_unrelated_new_boot_work(self):
        self.t.lease.update(owner="foreign-new-owner", boot="new-kernel", state="held")
        def unrelated(c, fenced):
            c.update(native_jobs=[{"owner": "foreign"}], other_unresolved_owned_items=[{"identity": "new"}],
                     job_dir_absent=False, init_absent=False, supervisor_absent=False)
            return c
        self.t.capsule_change = unrelated
        p = self.replaced()
        self.assertEqual(self.t.calls, ["capsule", "boot"])
        self.assertEqual(p["method"], "observed-boot-replacement")
        self.assertEqual(p["binding"], self.binding)
        self.assertEqual(self.t.row, self.t.original_row)
        self.assertTrue(R.validate_proof(p, self.binding, self.intent()))
        self.assertNotIn("stop", p["evidence"])
        self.assertNotIn("fence", p["evidence"])
        for key in ("ok", "reaped", "contained", "namespace_absent"):
            self.assertNotIn(key, p)

    def test_replacement_original_boot_owner_identity_sha_and_nonce_must_match(self):
        changes = [{"boot_id": BINDING["boot_id"]}, {"actual_vm_id": "clone"}, {"nonce": "replay"},
                   {"complete": False}, {"owned_record": {}}, {"owned_record_sha256": "bad"},
                   {"owner_tombstone": {"batch": BINDING["batch"], "run_name": BINDING["run_name"], "owner": "foreign"}}]
        for change in changes:
            with self.subTest(change=change):
                self.setUp()
                self.t.capsule_change = lambda c, f: {**c, **change}
                with self.assertRaises(R.RecoveryError):
                    self.replaced()
                self.assertTrue(all(c in ("capsule", "boot") for c in self.t.calls))
                self.assertFalse(self.store.proofs)

    def test_replacement_separate_boot_read_must_agree_and_be_fresh(self):
        for change in ({"boot_id": "third-kernel"}, {"boot_id": BINDING["boot_id"]},
                       {"nonce": "fresh-0"}, {"vm_id": "clone"}):
            with self.subTest(change=change):
                self.setUp()
                self.t.boot_change = lambda b: {**b, **change}
                with self.assertRaises(R.RecoveryError):
                    self.replaced()
                self.assertFalse(self.store.proofs)

    def test_replacement_proof_persistence_failure_leaves_pending_intent(self):
        self.store.fail_proof = True
        with self.assertRaises(R.RecoveryError):
            self.replaced()
        self.assert_pending()
        self.assertEqual(self.t.calls, ["capsule", "boot"])

    def test_replacement_validation_rejects_tamper_and_different_original(self):
        p = self.replaced()
        i = self.intent()
        self.assertTrue(R.validate_proof(p, self.binding, i))
        self.assertFalse(R.validate_proof({**p, "new_boot_id": "forged"}, self.binding, i))
        self.assertFalse(R.validate_proof(p, {**self.binding, "owner": "foreign"}, i))
        self.assertFalse(R.validate_proof({**p, "method": "fenced-stop-resume"}, self.binding, i))

    def test_observed_pending_retry_refreshes_both_native_nonces(self):
        self.store.fail_proof = True
        with self.assertRaises(R.RecoveryError):
            self.replaced()
        old = self.intent()
        self.store.fail_proof = False
        p = self.replaced()
        self.assertEqual(self.t.calls, ["capsule", "boot", "capsule", "boot"])
        self.assertNotEqual(old["replacement_capsule"]["nonce"], p["evidence"]["capsule"]["nonce"])
        self.assertNotEqual(old["replacement_boot"]["nonce"], p["boot_read"]["nonce"])
        self.assertTrue(R.validate_proof(p, self.binding, self.intent()))

    def test_observed_pending_explicit_continuation_and_final_commit_failure(self):
        self.store.fail_intent = lambda i: i["state"] == "complete"
        with self.assertRaises(R.RecoveryError):
            self.replaced()
        self.assert_pending()
        self.store.fail_intent = None
        kw = self.kwargs()
        kw.pop("authorization")
        kw.pop("clock")
        p = R.verify_pending_replaced_boot(self.t, **kw)
        self.assertTrue(R.validate_proof(p, self.binding, self.intent()))
        self.assertEqual(self.t.calls, ["capsule", "boot", "capsule", "boot"])

    def test_fenced_pending_proof_persistence_retry_is_read_only(self):
        self.store.fail_proof = True
        with self.assertRaises(R.RecoveryError):
            self.recover()
        self.assert_pending()
        old_calls = self.t.calls[:]
        self.store.fail_proof = False
        kw = self.kwargs()
        kw.pop("authorization")
        kw.pop("clock")
        p = R.verify_pending_boot(self.t, **kw)
        self.assertEqual(self.t.calls, old_calls + ["boot"])
        self.assertTrue(R.validate_proof(p, self.binding, self.intent()))

    def test_missing_committed_artifact_can_be_restored_without_vm_operations(self):
        for observed in (False, True):
            with self.subTest(observed=observed):
                self.setUp()
                p = self.replaced() if observed else self.recover()
                self.store.proofs.clear()
                old_calls = self.t.calls[:]
                repaired = R.restore_committed_proof(binding=self.binding,
                                                     load_intent=self.store.load_intent,
                                                     persist_proof=self.store.persist_proof)
                self.assertEqual(repaired, p)
                self.assertEqual(self.t.calls, old_calls)
                self.assertEqual(self.store.proofs[p["intent_id"]], p)
                # Re-entering the original API also restores the exact proof, no VM work.
                self.assertEqual(self.replaced() if observed else self.recover(), p)
                self.assertEqual(self.t.calls, old_calls)

    def test_corrupted_commit_cannot_restore_an_artifact(self):
        p = self.recover()
        self.store.proofs.clear()
        self.store.intents[p["intent_id"]]["proof_sha256"] = "0" * 64
        with self.assertRaises(R.RecoveryError):
            R.restore_committed_proof(binding=self.binding, load_intent=self.store.load_intent,
                                      persist_proof=self.store.persist_proof)
        self.assertFalse(self.store.proofs)

    def test_observed_retry_cannot_take_over_fenced_pending_intent(self):
        self.t.fail_stop = True
        with self.assertRaises(R.RecoveryError):
            self.recover()
        old_calls = self.t.calls[:]
        with self.assertRaises(R.RecoveryError):
            self.replaced()
        self.assertEqual(self.t.calls, old_calls)

    def test_observed_pending_retry_requires_original_authorization(self):
        self.store.fail_proof = True
        with self.assertRaises(R.RecoveryError):
            self.replaced()
        old_calls = self.t.calls[:]
        self.auth["reference"] = "different-ticket"
        with self.assertRaises(R.RecoveryError):
            self.replaced()
        self.assertEqual(self.t.calls, old_calls)


if __name__ == "__main__":
    unittest.main()
