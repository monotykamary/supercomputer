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

## Preparation-only retries and NOT_STARTED

The controller may retry a transport failure **during preparation**, before any
payload start has been requested. It uses the existing `boat_tries` preparation
budget (normally three calls), without a new placement, VM, boot, launch nonce,
physical-attempt allowance or fallback. Cancellation, shutdown and lease loss do
not grant a retry. Eligibility uses typed transport evidence, never error text:
recognized connection/timeouts, incomplete reads and temporary DNS failures, or
HTTP **408, 429, 502, 503, 504**. Other HTTP statuses (including 400/401/403 and
500), certificate/protocol failures, untyped errors and semantic tree/prep
negative replies do not retry and retain their existing LOST outcome. Permanent
owned-protocol/capability refusals likewise keep their existing behavior.

This path requires a complete, identity-bound `*.attempts` journal, opened and
fsynced as `prelaunch_reserved` before the preparation thread starts. Each
failed preparation appends a `prelaunch_not_started` record with `phase:
"prelaunch"`, `status: "NOT_STARTED"`, and the SHA-256/byte length of the complete
preceding journal prefix. That record is terminal for that preparation call,
not necessarily for the item: a later safe preparation call may succeed.
The scheduler still fsyncs `start_requested` before sending the sole launch RPC.

If preparation exhausts its budget with complete evidence and no start request,
the terminal item row is `NOT_STARTED`, code `-1`, with `prelaunch_evidence` and
`prelaunch_failures`. A successful item may also report `prelaunch_failures`.
`attempts: 1` continues to mean the single placement/physical-attempt reservation;
it does not assert that a payload actually ran. Neither NOT_STARTED nor a
preparation record proves namespace cleanup: the existing positive close/reap
requirements still apply.

A persisted `start_requested` is an irreversible no-retry boundary, even when
its reply is absent or failed. Missing, torn, malformed or identity-mismatched
audits are unknown, not proof of zero execution. Those cases remain fail-closed
LOST, as do post-intent transport errors. Older admissions without the new
reservation marker are not upgraded or replayed. Historical LOST rows are never
rewritten; an explicitly authorized manual recovery is separate from this
upstream automatic policy.

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
set the legacy `lost: true` callback/fallback flag. Lease-related terminal rows
also retain `lease_relinquishment`: the initiating operation, bounded/redacted
error or refusal, and expected/observed fence metadata. This diagnostic is not
execution or cleanup proof and does not alter the strict retry/close contract.

## Synchronous close and cleanup

Strict close does not take the legacy asynchronous kill path. It fences
admission, waits for any in-flight launch, requests identity-bound namespace
reaping, and retains admission, placement, result and cancellation journals.
Its receipt includes `attempts` with name, run name, VM, boot, attempt number,
launch identity and cleanup completion; `unlaunched` lists zero-placement items.
`journals_retained: true` is explicit.

`cleanup_complete: true` requires positive matching reap evidence for every
attempt, or no placement. On normal strict terminal delivery, the controller
also requests the existing identity-bound native `cancelitem` barrier **after**
fetching the complete result and logs, **before** acknowledging the row or
releasing its placement. A positive receipt is fsynced to the cancellation
journal as `phase: "terminal"`, bound to the original item/run, VM, boot, attempt
and launch identity. This does not close the admission or change the result.
A later strict close reuses that receipt (`source: "terminal-reap"`), including
after the worker was released or a controller restart. Replaying evidence alone
does not invent a successful result or an admission-close fence.

The terminal probe has a one-second reap budget and a three-second transport
timeout. A failed probe does not suppress a valid result; normal strict close
still requires a positive receipt. Missing agents, release attempts, dead-VM
flags and unqualified 404s are never accepted as cleanup. Previously completed
items without a retained positive receipt remain unknown; this fix does not
retroactively upgrade external audit dispositions or change original rows.

An unreachable/dead worker, timeout, mismatched reply
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

## Private temporary files and caches

Strict jobs keep the installed Bun/Lean toolchains and agent source read-only.
They receive these writable defaults inside the existing per-job scratch tree:

| Variable | Default |
| --- | --- |
| `TMPDIR` | `<job>/tmp` |
| `BUN_TMPDIR` | `<job>/tmp` |
| `XDG_CACHE_HOME` | `<job>/tmp/cache` |
| `BUN_INSTALL_CACHE_DIR` | `<job>/tmp/cache/bun` |

The absolute paths are unique to the physical job, not shared installation
caches. The scratch and cache directories are created with mode `0700` and are
removed by the existing job cleanup. They are not exported as results; copy any
wanted artifacts to `$SUPERCOMPUTER_OUTPUT_DIR`. This does not make toolchain
installation directories writable or change attempt, resource, or cleanup rules.
Ordinary non-strict jobs keep their existing defaults.

These are defaults: job `env` overrides project `env`, which overrides them.
Explicit paths remain the caller's responsibility, including their permissions
and isolation. Overriding `TMPDIR` alone does not override the independent Bun
or XDG cache settings. Strict `HOME` still identifies the worker user and cannot
be redirected by an environment override.

A wrapper that deliberately clears its environment must forward the settings it
needs. For example, do not pass only `HOME` and `TMPDIR` to Bun while discarding
its cache setting: Bun may then select a cache in its read-only installation.

```sh
env -i PATH="$PATH" HOME="$HOME" \
  TMPDIR="$TMPDIR" BUN_TMPDIR="$BUN_TMPDIR" \
  XDG_CACHE_HOME="$XDG_CACHE_HOME" BUN_INSTALL_CACHE_DIR="$BUN_INSTALL_CACHE_DIR" \
  bun install --frozen-lockfile --ignore-scripts
```

The agent cannot restore variables intentionally removed by a payload's
`env -i`. Preflight wrappers should probe their selected temporary/cache paths
for writability and retain that receipt before installing dependencies.

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
