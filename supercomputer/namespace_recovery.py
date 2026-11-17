"""Explicit, lease-fenced VM boot extinction, never a native cancelitem receipt.

Only injected transports perform operations. Their reads must be authenticated to the
exact VM, complete, and serialized with native lease/ownership locks. A STOPPING
capsule must be read *under* that exact fence, not by relaxing ordinary held-lease
checks. Persist callbacks must atomically durably store detached JSON values and
load_intent must read controller-owned storage, not caller-supplied evidence.

operation_id is an invocation correlation label (possibly locally generated), NOT
an assertion that the provider assigned an operation ID. Neither that label nor a
stopped observation proves extinction: a fresh, different native boot is mandatory.
No original owned record is ever updated. No cleanup/native success is synthesized.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import secrets
import time
from typing import Callable, Protocol

PROOF_KIND = "vm-boot-extinction/v1"
CAPSULE_KIND = "namespace-recovery-capsule/v1"
INTENT_KIND = "namespace-recovery-intent/v1"
_BINDING_KEYS = {"batch", "name", "run_name", "attempt", "vm", "boot_id", "launch_identity", "owner"}


class RecoveryError(RuntimeError):
    """Fail closed. intent_id identifies any durable pending recovery to inspect."""

    def __init__(self, message, intent_id=None):
        super().__init__(message)
        self.intent_id = intent_id


class Transport(Protocol):
    """All deadlines are absolute monotonic times; implementations must bound I/O.

    Native capsule keys: kind, nonce, binding, complete, actual_vm_id, boot_id,
    owned_record, owned_record_sha256, owner_tombstone, job_dir_absent,
    init_absent, supervisor_absent, native_jobs, other_unresolved_owned_items,
    lease. Optional owned_record_raw is the exact UTF-8 record text whose SHA
    is supplied; otherwise SHA covers canonical_json(owned_record).
    """

    def read_capsule(self, binding: dict, nonce: str, fence: dict | None = None, *, deadline: float) -> dict: ...
    def read_lease(self, vm: str, *, deadline: float) -> dict: ...
    def acquire_lease(self, vm: str, owner: str, *, deadline: float) -> dict: ...
    def fence_stop(self, vm: str, lease: dict, *, deadline: float) -> dict: ...
    def stop_vm(self, vm: str, fence: dict, *, preserve_disk: bool, deadline: float) -> dict: ...
    def observe_stopped(self, vm: str, stop_receipt: dict, *, deadline: float) -> dict: ...
    def resume_vm(self, vm: str, *, deadline: float) -> dict: ...
    def read_boot(self, vm: str, nonce: str, *, deadline: float) -> dict: ...


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(value) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def intent_id(binding: dict) -> str:
    _binding(binding)
    return digest(binding)


def _require(condition, message):
    if not condition:
        raise RecoveryError(message)


def _text(value):
    return isinstance(value, str) and bool(value.strip())


def _binding(b):
    _require(isinstance(b, dict) and set(b) == _BINDING_KEYS, "invalid original binding")
    _require(all(_text(b[k]) for k in _BINDING_KEYS - {"attempt"}), "empty original binding")
    _require(type(b["attempt"]) is int and b["attempt"] >= 1, "invalid attempt")


def _authorization(a, b):
    _require(isinstance(a, dict) and a.get("explicit") is True and _text(a.get("reference"))
             and a.get("binding") == b, "explicit identity-bound operator authorization required")


def _lease(lease, b, state):
    _require(isinstance(lease, dict) and lease.get("ok") is True
             and lease.get("owner") == b["owner"] and lease.get("vm_id") == b["vm"]
             and lease.get("boot") == b["boot_id"] and lease.get("state") == state
             and type(lease.get("epoch")) is int and lease["epoch"] > 0,
             "lease identity/state mismatch")
    if state == "held":
        _require(lease.get("lapsed") is False and lease.get("valid_boot") is True,
                 "fresh same-owner held lease required")
    if state == "stopping":
        _require(_text(lease.get("stop_nonce")), "missing stop fence nonce")


def _same_fence(actual, expected):
    return all(actual.get(k) == expected.get(k) for k in
               ("owner", "vm_id", "boot", "epoch", "state", "stop_nonce"))


def _capsule(c, b, nonce, fence=None):
    _require(isinstance(c, dict) and c.get("kind") == CAPSULE_KIND
             and c.get("nonce") == nonce and _text(nonce) and c.get("binding") == b
             and c.get("complete") is True and c.get("actual_vm_id") == b["vm"]
             and c.get("boot_id") == b["boot_id"], "missing or unbound native capsule")
    row = c.get("owned_record")
    _require(isinstance(row, dict) and row.get("identity") == b["launch_identity"]
             and row.get("state") == "launched" and row.get("reaped") is True
             and row.get("contained") is False, "not the original unsupported strict attempt")
    raw = c.get("owned_record_raw")
    if raw is not None:
        _require(isinstance(raw, str) and json.loads(raw) == row, "raw owned record mismatch")
        sha = hashlib.sha256(raw.encode()).hexdigest()
    else:
        sha = digest(row)
    _require(c.get("owned_record_sha256") == sha, "owned record SHA mismatch")
    tomb = c.get("owner_tombstone")
    _require(isinstance(tomb, dict) and all(tomb.get(k) == b[k] for k in ("batch", "run_name", "owner")),
             "missing original owner tombstone")
    _require(all(c.get(k) is True for k in ("job_dir_absent", "init_absent", "supervisor_absent")),
             "job directory or native init/supervisor not absent")
    _require(c.get("native_jobs") == [] and c.get("other_unresolved_owned_items") == [],
             "native jobs or other unresolved ownership veto recovery")
    lease = c.get("lease")
    _require(isinstance(lease, dict) and lease.get("owner") == b["owner"]
             and lease.get("vm_id") == b["vm"] and lease.get("boot") == b["boot_id"],
             "capsule lease belongs to another owner, VM or boot")
    if fence is not None:
        _lease(lease, b, "stopping")
        _require(_same_fence(lease, fence), "STOPPING capsule fence mismatch")
    return sha


def _operation(r, b, stop=False):
    _require(isinstance(r, dict) and r.get("vm_id") == b["vm"]
             and r.get("accepted") is True and _text(r.get("operation_id")),
             "provider operation not accepted for exact VM")
    if stop:
        _require(r.get("preserve_disk") is True, "stop must preserve disk")


def _observation(r, b, stop):
    _require(isinstance(r, dict) and r.get("vm_id") == b["vm"] and r.get("source") == "provider"
             and r.get("observed") is True and r.get("state") == "stopped"
             and r.get("operation_id") == stop["operation_id"], "exact stopped VM not independently observed")


def _boot(r, b, nonce):
    _require(isinstance(r, dict) and r.get("kind") == "native-boot/v1"
             and r.get("source") == "native" and r.get("vm_id") == b["vm"]
             and r.get("nonce") == nonce and _text(nonce) and _text(r.get("boot_id"))
             and r["boot_id"] != b["boot_id"], "fresh changed native boot required (suspend is not extinction)")


def _evidence(i, b):
    _require(i.get("kind") == INTENT_KIND and i.get("intent_id") == intent_id(b)
             and i.get("binding") == b, "persisted intent binding mismatch")
    _authorization(i["authorization"], b)
    first, second = i["before"], i["fenced"]
    sha = _capsule(first, b, first["nonce"])
    _require(first["nonce"] != second["nonce"], "replayed fenced capsule")
    lease, fence = i["lease"], i["fence"]
    _lease(lease, b, "held")
    _lease(fence, b, "stopping")
    _require(all(lease[k] == fence[k] for k in ("owner", "vm_id", "boot", "epoch")), "fence epoch changed")
    _require(_capsule(second, b, second["nonce"], fence) == sha
             and second["owned_record"] == first["owned_record"], "original row changed under fence")
    _operation(i["stop"], b, True)
    _observation(i["stopped"], b, i["stop"])
    _operation(i["resume"], b)
    _require(type(i["budget_s"]) in (int, float) and 0 < i["budget_s"] <= 240, "invalid stop authority budget")
    return sha


def _proof(i, boot):
    b = i["binding"]
    sha = _evidence(i, b)
    _require(boot["nonce"] not in (i["before"]["nonce"], i["fenced"]["nonce"]), "boot nonce replay")
    _boot(boot, b, boot["nonce"])
    return {"kind": PROOF_KIND, "method": "fenced-stop-resume", "binding": copy.deepcopy(b), "intent_id": i["intent_id"],
            "authorization": copy.deepcopy(i["authorization"]), "old_boot_id": b["boot_id"],
            "new_boot_id": boot["boot_id"], "owner": b["owner"], "owned_record_sha256": sha,
            "evidence": {k: copy.deepcopy(i[k]) for k in ("before", "fenced", "lease", "fence", "stop", "stopped", "resume")},
            "boot_read": copy.deepcopy(boot)}


def validate_proof(proof: dict, binding: dict, intent: dict) -> bool:
    """Restart-safe validator: intent MUST be loaded from controller-owned storage.

    A self-consistent caller-created proof/intent pair is not authority. Persisted
    completed intent digest binds the exact proof, placement and authorization.
    This proves old-boot extinction only, not execution/result/native cancellation.
    """
    try:
        _binding(binding)
        _require(intent.get("state") == "complete" and intent.get("proof_sha256") == digest(proof),
                 "proof not committed by controller")
        if intent.get("method") == "observed-boot-replacement":
            return proof == _replacement_proof(intent, binding)
        _evidence(intent, binding)
        return proof["boot_read"] == intent["committed_boot_read"] and proof == _proof(intent, proof["boot_read"])
    except (RecoveryError, KeyError, TypeError, ValueError, AttributeError):
        return False


def _save(i, persist, load):
    persist(copy.deepcopy(i))
    _require(load(i["intent_id"]) == i, "intent persistence readback failed")


def _budget(seconds, monotonic):
    _require(type(seconds) in (int, float) and math.isfinite(seconds) and 0 < seconds <= 240,
             "stop authority must be bounded to at most 240 seconds (below 270)")
    deadline = monotonic() + seconds

    def check():
        _require(monotonic() < deadline, "recovery deadline exceeded; intent remains pending")
    return deadline, check


def recover(transport: Transport, *, binding: dict, authorization: dict,
            persist_intent: Callable, load_intent: Callable, persist_proof: Callable,
            clock=time.time, monotonic=time.monotonic, nonce_factory=lambda: secrets.token_hex(24),
            budget_s=240) -> dict:
    """One explicit stop/resume attempt. Pending intents never authorize a second stop.

    Callers serialize recovery for this binding/VM and persistence callbacks must
    enforce exclusive creation (no concurrent clobbering of an existing intent).
    Exceptions preserve durable pending intent and never imply cleanup completion.
    """
    binding, authorization = copy.deepcopy(binding), copy.deepcopy(authorization)
    _binding(binding)
    _authorization(authorization, binding)
    iid = intent_id(binding)
    try:
        existing = load_intent(iid)
        if existing is not None:
            _require(existing.get("authorization") == authorization, "existing authorization mismatch")
            if existing.get("state") == "complete":
                return restore_committed_proof(binding=binding, load_intent=load_intent, persist_proof=persist_proof)
            raise RecoveryError("existing intent: inspect/verify pending recovery, never stop again")
        deadline, check = _budget(budget_s, monotonic)
        nonce = nonce_factory()
        before = transport.read_capsule(copy.deepcopy(binding), nonce, deadline=deadline)
        check()
        _capsule(before, binding, nonce)
        # No mutation, including lease claim, precedes authenticated native evidence.
        i = {"kind": INTENT_KIND, "intent_id": iid, "state": "pending", "binding": binding,
             "method": "fenced-stop-resume",
             "authorization": authorization, "before": copy.deepcopy(before),
             "created_at": clock(), "budget_s": budget_s}
        _save(i, persist_intent, load_intent)
        check()
        vm = binding["vm"]
        lease = transport.read_lease(vm, deadline=deadline)
        check()
        _require(lease.get("owner") == binding["owner"] and lease.get("vm_id") == vm
                 and lease.get("boot") == binding["boot_id"], "foreign lease cannot be acquired")
        if lease.get("state") == "released":
            lease = transport.acquire_lease(vm, binding["owner"], deadline=deadline)
            check()
        _lease(lease, binding, "held")
        i["lease"] = copy.deepcopy(lease)
        _save(i, persist_intent, load_intent)
        check()
        fence = transport.fence_stop(vm, copy.deepcopy(lease), deadline=deadline)
        check()
        _lease(fence, binding, "stopping")
        _require(all(fence[k] == lease[k] for k in ("owner", "vm_id", "boot", "epoch")), "wrong fenced epoch")
        i["fence"] = copy.deepcopy(fence)
        _save(i, persist_intent, load_intent)
        check()
        fenced_nonce = nonce_factory()
        _require(fenced_nonce != nonce, "nonce factory replay")
        fenced = transport.read_capsule(copy.deepcopy(binding), fenced_nonce, copy.deepcopy(fence), deadline=deadline)
        check()
        _require(_capsule(fenced, binding, fenced_nonce, fence) == before["owned_record_sha256"]
                 and fenced["owned_record"] == before["owned_record"], "original owned record changed")
        i["fenced"] = copy.deepcopy(fenced)
        _save(i, persist_intent, load_intent)
        check()
        stop = transport.stop_vm(vm, copy.deepcopy(fence), preserve_disk=True, deadline=deadline)
        _operation(stop, binding, True)
        i["stop"] = copy.deepcopy(stop)
        _save(i, persist_intent, load_intent)
        check()
        stopped = transport.observe_stopped(vm, copy.deepcopy(stop), deadline=deadline)
        _observation(stopped, binding, stop)
        i["stopped"] = copy.deepcopy(stopped)
        _save(i, persist_intent, load_intent)
        check()
        resumed = transport.resume_vm(vm, deadline=deadline)
        _operation(resumed, binding)
        i["resume"] = copy.deepcopy(resumed)
        _save(i, persist_intent, load_intent)
        check()
        return _finish(transport, i, deadline, check, persist_intent, load_intent, persist_proof, nonce_factory)
    except Exception as exc:
        raise RecoveryError(str(exc), iid) from exc


def _finish(transport, i, deadline, check, persist_intent, load_intent, persist_proof, nonce_factory):
    nonce = nonce_factory()
    _require(nonce not in (i["before"]["nonce"], i["fenced"]["nonce"]), "nonce factory replay")
    check()
    boot = transport.read_boot(i["binding"]["vm"], nonce, deadline=deadline)
    check()
    _boot(boot, i["binding"], nonce)
    proof = _proof(i, boot)
    persist_proof(copy.deepcopy(proof))  # durable success contract; never return before both commits
    check()
    complete = {**i, "state": "complete", "proof_sha256": digest(proof),
                "committed_boot_read": copy.deepcopy(boot)}
    _save(complete, persist_intent, load_intent)
    check()
    _require(validate_proof(proof, i["binding"], load_intent(i["intent_id"])), "proof commit validation failed")
    return proof


def _replacement_proof(i, b):
    _require(i.get("kind") == INTENT_KIND and i.get("intent_id") == intent_id(b)
             and i.get("binding") == b and i.get("method") == "observed-boot-replacement",
             "replacement intent binding mismatch")
    _authorization(i["authorization"], b)
    c = i["replacement_capsule"]
    _require(isinstance(c, dict) and c.get("kind") == CAPSULE_KIND
             and c.get("complete") is True and c.get("binding") == b
             and c.get("actual_vm_id") == b["vm"] and _text(c.get("nonce"))
             and _text(c.get("boot_id")) and c["boot_id"] != b["boot_id"],
             "authenticated same-VM changed-boot capsule required")
    row = c.get("owned_record")
    _require(isinstance(row, dict) and row.get("identity") == b["launch_identity"]
             and row.get("state") == "launched" and row.get("reaped") is True
             and row.get("contained") is False, "replacement original owned record mismatch")
    raw = c.get("owned_record_raw")
    if raw is None:
        sha = digest(row)
    else:
        _require(isinstance(raw, str) and json.loads(raw) == row, "raw owned record mismatch")
        sha = hashlib.sha256(raw.encode()).hexdigest()
    _require(c.get("owned_record_sha256") == sha, "replacement original record SHA mismatch")
    tomb = c.get("owner_tombstone")
    _require(isinstance(tomb, dict) and all(tomb.get(k) == b[k] for k in ("batch", "run_name", "owner")),
             "replacement original owner tombstone mismatch")
    boot = i["replacement_boot"]
    _require(boot["nonce"] != c["nonce"], "replacement boot nonce replay")
    _boot(boot, b, boot["nonce"])
    _require(boot["boot_id"] == c["boot_id"], "replacement capsule and boot read disagree")
    return {"kind": PROOF_KIND, "method": "observed-boot-replacement", "intent_id": i["intent_id"],
            "binding": copy.deepcopy(b), "authorization": copy.deepcopy(i["authorization"]),
            "owner": b["owner"], "old_boot_id": b["boot_id"], "new_boot_id": boot["boot_id"],
            "owned_record_sha256": sha, "evidence": {"capsule": copy.deepcopy(c)},
            "boot_read": copy.deepcopy(boot)}


def recover_replaced_boot(transport: Transport, *, binding: dict, authorization: dict,
                          persist_intent: Callable, load_intent: Callable, persist_proof: Callable,
                          clock=time.time, monotonic=time.monotonic,
                          nonce_factory=lambda: secrets.token_hex(24), budget_s=240) -> dict:
    """Observe already-replaced boot; NEVER claim, fence, stop, resume, or kill.

    The caller may independently resume its VM after verifying provider ownership.
    This method does not assert who caused a replacement or that we stopped it.
    Unrelated jobs/ownership on the new boot are deliberately not vetoes. The
    transport must authenticate the immutable provider VM identity, not an address
    or snapshot name; restoring the same kernel/boot can never satisfy this API.
    """
    binding, authorization = copy.deepcopy(binding), copy.deepcopy(authorization)
    _binding(binding)
    _authorization(authorization, binding)
    iid = intent_id(binding)
    try:
        existing = load_intent(iid)
        if existing is not None:
            _require(existing.get("binding") == binding and existing.get("authorization") == authorization,
                     "existing binding/authorization mismatch")
            if existing.get("state") == "complete":
                return restore_committed_proof(binding=binding, load_intent=load_intent, persist_proof=persist_proof)
            _require(existing.get("state") == "pending"
                     and existing.get("method") == "observed-boot-replacement",
                     "only a matching observed-boot pending intent can be continued read-only")
        deadline, check = _budget(budget_s, monotonic)
        nonce = nonce_factory()
        c = transport.read_capsule(copy.deepcopy(binding), nonce, deadline=deadline)
        check()
        _require(c.get("nonce") == nonce, "replacement capsule nonce mismatch")
        boot_nonce = nonce_factory()
        _require(boot_nonce != nonce, "nonce factory replay")
        boot = transport.read_boot(binding["vm"], boot_nonce, deadline=deadline)
        check()
        _require(boot.get("nonce") == boot_nonce, "replacement boot nonce mismatch")
        i = {"kind": INTENT_KIND, "intent_id": iid, "state": "pending",
             "method": "observed-boot-replacement", "binding": binding,
             "authorization": authorization, "replacement_capsule": copy.deepcopy(c),
             "replacement_boot": copy.deepcopy(boot),
             "created_at": existing["created_at"] if existing else clock(), "budget_s": budget_s}
        proof = _replacement_proof(i, binding)
        _save(i, persist_intent, load_intent)
        check()
        persist_proof(copy.deepcopy(proof))
        check()
        _save({**i, "state": "complete", "proof_sha256": digest(proof)}, persist_intent, load_intent)
        check()
        _require(validate_proof(proof, binding, load_intent(iid)), "replacement proof commit validation failed")
        return proof
    except Exception as exc:
        raise RecoveryError(str(exc), iid) from exc


def restore_committed_proof(*, binding: dict, load_intent: Callable, persist_proof: Callable) -> dict:
    """Repair a missing proof artifact from authoritative committed intent, no VM I/O."""
    iid = intent_id(binding)
    try:
        i = copy.deepcopy(load_intent(iid))
        _require(isinstance(i, dict) and i.get("state") == "complete", "no committed proof intent")
        if i.get("method") == "observed-boot-replacement":
            proof = _replacement_proof(i, binding)
        else:
            proof = _proof(i, i["committed_boot_read"])
        _require(validate_proof(proof, binding, i), "committed intent proof digest mismatch")
        persist_proof(copy.deepcopy(proof))
        return proof
    except Exception as exc:
        raise RecoveryError(str(exc), iid) from exc


def verify_pending_replaced_boot(transport: Transport, *, binding: dict, persist_intent: Callable,
                                 load_intent: Callable, persist_proof: Callable,
                                 monotonic=time.monotonic, nonce_factory=lambda: secrets.token_hex(24),
                                 budget_s=240) -> dict:
    """Fresh read-only continuation using original persisted operator authorization."""
    iid = intent_id(binding)
    try:
        i = load_intent(iid)
        _require(isinstance(i, dict) and i.get("state") == "pending"
                 and i.get("method") == "observed-boot-replacement" and i.get("binding") == binding,
                 "no pending observed-boot intent")
        return recover_replaced_boot(transport, binding=binding, authorization=i["authorization"],
                                     persist_intent=persist_intent, load_intent=load_intent,
                                     persist_proof=persist_proof, monotonic=monotonic,
                                     nonce_factory=nonce_factory, budget_s=budget_s)
    except Exception as exc:
        raise RecoveryError(str(exc), iid) from exc


def verify_pending_boot(transport: Transport, *, binding: dict, persist_intent: Callable,
                        load_intent: Callable, persist_proof: Callable,
                        monotonic=time.monotonic, nonce_factory=lambda: secrets.token_hex(24), budget_s=240) -> dict:
    """Read-only VM continuation after restart/timeout; NEVER stops or resumes.

    Requires durable stop, independent stopped and resume receipts. Otherwise the
    intent remains pending for explicit operator investigation, not a guessed retry.
    """
    iid = intent_id(binding)
    try:
        i = copy.deepcopy(load_intent(iid))
        _require(isinstance(i, dict) and i.get("state") == "pending", "no pending recovery intent")
        _evidence(i, binding)
        deadline, check = _budget(budget_s, monotonic)
        return _finish(transport, i, deadline, check, persist_intent, load_intent, persist_proof, nonce_factory)
    except Exception as exc:
        raise RecoveryError(str(exc), iid) from exc
