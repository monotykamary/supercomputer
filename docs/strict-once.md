# Strict single-attempt batches

`strict_once=True` / `--strict-once` opts an ordinary native batch into a reviewed
single-attempt lifecycle. Ordinary retrying batches are unchanged.

## Guarantee and hard cap

For each item in one persisted admission the scheduler authorizes **at most one
physical payload attempt**. `max_attempts` defaults to `1`; this mode accepts
only the integer `1`. Its cap is enforced under the placement lock, durably
journaled before launch, and backed by the agent's identity-bound launch record.
It is not a strike count or a limit on concurrently allocated VMs.

“Exactly one” means one attempt **if launch succeeds**. Admission/preparation
failure, an ambiguous submission or cancellation can result in zero launches.
This is not a promise of completion, availability or exactly-once external side
effects from arbitrary shell commands.

Loss, preemption, stale ownership and ambiguous launch replies never requeue
or assign a replacement VM, boot or provider. Failure is terminal `status:
"LOST"`, `terminal: true`, not a retry request. Handoff/restart observes the
original attempt and never resends an uncertain strict start. A surviving
attempt may finish on its original worker. The agent rejects a different nonce
for an existing strict job and checks real namespace containment before payload
execution. A new session/batch is a new intentional admission, not a reconnect;
do not add a user-level retry loop for LOST.

## Public tuple

```sh
supercomputer run --jobs jobs.json --out runs/once --backend boat \
  --class gate --strict-once --max-attempts 1
```

```python
from supercomputer import boat
rows = []
s = boat.session("calibration", klass="gate", strict_once=True, max_attempts=1)
try:
    s.run([{"name": "case", "kind": "sh", "cmd": "python3 experiment.py",
            "limit": 600, "weight": 1}], lambda item, row: rows.append(row))
finally:
    cleanup = s.close()
if not cleanup["cleanup_complete"]:
    raise RuntimeError("strict cleanup unconfirmed: retain custody")
```

The controller must advertise `strict_once_protocol: 1`, include `strict_once`
in `submit_flags`, and support the one-attempt cap. Preflight refuses unsupported
controllers before upload. Strict mode refuses Modal, RunPod/GPU and legacy
scheduler-free execution; it never falls back to another worker/provider.

## Reconnect and result custody

An ambiguous strict submit is not silently resubmitted. The client polls the
same batch ID to discover its admission. An explicit pre-admission missing-tree
response may be repaired using that same ID. Poll transport retries and
reconnections are recorded; they never authorize re-execution.

Each strict row's `custody` contains `batch`, `max_attempts`, `reconnect_count`,
`event_count`, `sha256`, `file` and a bounded `events` tail. The complete JSONL
file is append-only and flushed/fsynced before exposing each event. Preserve
that file, not just the tail. It records admission ambiguity, transport errors,
reconnections and close outcomes without credentials or request bodies. A callback
can observe a prefix while the session remains open; bind the final event count
and digest from `close()` after cleanup before sealing a calibration receipt.

Terminal results carry `launch_identity`, `attempt`, `attempts`, `max_attempts`,
`vm`, `boot_id` and `placement`; placement includes the observed agent version.
Before an authoritative placement is known, `identity_unavailable: true` marks
missing evidence rather than inventing a VM/boot. LOST intentionally does not
set the legacy `lost: true` callback/fallback flag.

## Synchronous close and cleanup

Strict close does not take the legacy asynchronous kill path. It fences
admission, waits for any in-flight launch, requests identity-bound namespace
reaping, and retains admission, placement, result and cancellation journals.
Its receipt includes `attempts` with name, run name, VM, boot, attempt number,
launch identity and cleanup completion; `unlaunched` lists zero-placement items.
`journals_retained: true` is explicit.

`cleanup_complete: true` requires positive matching reap evidence for every
attempt, or no placement. An unreachable/dead worker, timeout, mismatched reply
or failed custody write is not success. Terminal LOST and pending cleanup can
coexist; neither permits replacement or re-execution. The public client retains
each raw per-batch close receipt and aggregate completion. The jobs CLI saves
results/cleanup metadata and complete custody sidecars under its output path.

Completed strict histories now expire after the configured retention window
(default 48 hours); pending cleanup never expires. If close evidence was
interrupted, use the bounded read-only `recover_close()` /
`supercomputer batch recover-close` path described in
[closed history and recovery](closed-history.md). The original identity checks
are unchanged.

## Hash binding and verification

Bind a calibration to the package runtime hash, exact client/scheduler source
hashes, observed `placement.agent_version`, protocol version, VM/boot/nonce and
complete custody/cleanup receipts. The controller hash alone does not identify
the agent that executed a payload.

`runtime_verified: true` is published only after the source-bound one-VM native
test passes. Its scope is `native-single-vm`: actual owned RPC, dropped replies,
PID-namespace isolation and exact descendant teardown. Involuntary VM loss and
controller handoff are scenario-tested, not claimed as real-fleet fault injection.
Simulations exercise independent physical-start counters, not kernel
containment. The native fixture runs the candidate public client, private
scheduler and real agent/namespace payload on one Boat VM; provider allocation
is forbidden. Its private attachment and injected transport faults are not a
claim of real provider VM destruction or a production controller rollout. The
release receipt states the exact native and simulated coverage.
