# Opt-in memory enforcement

`memory` alone remains a **placement declaration**, not a kernel memory limit.
Existing jobs and their defaults do not change. To request enforcement, set
`enforce_memory: true` on a native job:

```json
{
  "name": "bounded-lean",
  "cmd": "lake build",
  "cpu": 8,
  "memory": 8192,
  "enforce_memory": true,
  "memory_limit_mode": "cgroup-v2",
  "timeout": 900
}
```

Memory is in MiB (the existing API calls these MB); fractional declarations are
rounded up. It must be a positive finite number. An omitted amount defaults to
4096 MiB. The same fields work in native `Session.run` item specs and Python
runners' `BOAT` settings. Jobs-file and Python-runner adapters reserve slots for
the declared amount. Direct native-item callers retain the existing obligation
to request sufficient slot `weight`. A limit is not a reservation or a promise
of available RAM; it does not replace ordinary placement checks.

## Modes

- `auto` (default when opted in): first try a private cgroup v2 memory scope;
  if unavailable **before entering the scope**, use a hard `RLIMIT_AS` limit.
- `cgroup-v2`: require the cgroup mechanism. If delegation, permissions, controller
  files or readback are unavailable, refuse the payload. **No RLIMIT fallback.**
- `rlimit-as`: explicitly use the address-space mechanism.

A cgroup sets `memory.max`, disables swap for that scope, and enables group OOM
handling. Its cap is **aggregate charged memory for the payload and descendants**.
Only that job enters it; the agent, scheduler and unrelated jobs remain outside.
The worker must already delegate the memory controller. The helper never changes
an ancestor's delegation, memory limit or running processes. If the current
process already has a finite cgroup memory ceiling, moving to a sibling scope
is refused rather than bypassing that ceiling; `auto` retains the existing
cgroup and adds RLIMIT_AS instead if that leaf cannot delegate a child scope. It only creates its
own `supercomputer/<run-directory-hash>` scope under the appropriate cgroup
ancestor. A sibling is used only if the current leaf has no finite memory,
CPU-quota, PID or I/O controls; otherwise delegation below that leaf is required.
All higher ancestor controls remain inherited. Existing scopes are not reused. Empty scopes are collected; a scope with surviving descendants keeps
its cap and reports `cleanup_pending`, rather than claiming cleanup or killing
unrelated processes. Normal strict-owned PID-namespace cleanup still applies.

`RLIMIT_AS` is a **per-process virtual address-space** limit, inherited across
fork/exec, with both soft and hard values set. It is **not aggregate job RAM**,
not RSS, and not equivalent to a cgroup. Multiple child processes each have their
own limit. An already lower inherited hard ceiling is retained and reported.
Runtimes reserving very large virtual ranges (notably some JavaScript engines)
may fail to start under it. Require `cgroup-v2` when aggregate RAM semantics or
those runtimes matter; do not interpret an automatic fallback as equivalent.

## Verification and receipts

After `taskset` and before the user payload, the worker reads back the selected
kernel limit and its actual CPU affinity. It writes `resources.json`, then execs
the original payload with the normal unprivileged UID and environment. Missing,
invalid, mismatched or unsupported verification refuses execution (exit 78).
An uncertain cgroup join cannot become an accepted fallback.

The terminal result row includes:

- `resource_verified`: whether the launch checks succeeded;
- `resource_error` on verification failure;
- `resource_limits`: protocol, requested MiB/bytes/mode, selected `method`,
  `effective_bytes`, `aggregate`, and `affinity.requested` / `affinity.effective`;
- for cgroups, swap limit, available peak/counter observations, per-scope OOM kills,
  and cleanup status; for fallback, the reason cgroups were unavailable and the
  actual soft/hard address-space limits.

These fields survive large-result projection and batch fetch. Affinity is a
**launch-time observed CPU mask**, not a CPU-time quota or a claim that arbitrary
trusted user code cannot subsequently change its own mask. Memory enforcement
also follows the existing trusted-workload model: this is not a security sandbox
against a job intentionally using the VM's passwordless sudo to alter controls.

Cgroup OOM attribution uses that job's `memory.events`, not a host-wide counter.
A caught `MemoryError` under RLIMIT is an application exit, not falsely labeled a
kernel OOM kill. Payload failure does not mean verification failed.

## Fail-closed native admission

Controller and actual worker must support `resource_limits_protocol: 1`.
Unknown workers, a capability loss before launch, a legacy local scheduler, and
Modal/RunPod fallback cannot silently ignore the request. A same-ID resubmission
cannot add, remove or change a retained cap. A controller with such retained
batches cannot hand off to a resource-unaware successor. Strict-once's placement
and attempt limits remain unchanged, including a failed pre-payload verification.
