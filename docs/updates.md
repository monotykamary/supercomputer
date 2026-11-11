# Updates: baseline audit

Audited at `139f5c8` (before the hot-reload lane). References in this section are
baseline `file:line`, not promises that the old implementation is safe.

* `supercomputer/scheduler.py:216-235`: `code_hash` hashes only top-level Python
  modules, cached by name/mtime; changes in `rules/` are invisible. `own_code`
  caches the process hash. `scheduler.py:3365-3371` waits for a stable install
  for 60 seconds, import-checks it, then requests handoff. The automatic pickup
  is real, but it is not a live module reload and misses nested-module updates.
* `scheduler.py:3479-3570`: handoff stops scheduling/pollers, snapshots held VMs,
  closes the listening socket and unlocks BEFORE spawning a successor. The old
  process waits up to 120 seconds for a VM/admission ACK. Thus “a new install
  hands the pool to the new code by itself” does not mean zero placement gap.
  The systemd branch waits for a restart even though the old MainPID has not
  exited. Failed candidates receive SIGTERM (which can release leases) before
  the old snapshot is handed to saved code. Neither path is an atomic transaction.
* `scheduler.py:631-776`: `_take_pool` changes registry PIDs in one write,
  reconstructs running items from the snapshot, and acknowledges admissions and
  VM IDs. It does not acknowledge an exact set of running-item identities.
  Reconciliation with agents happens later; a missing observation can requeue a
  legacy item. `scheduler.py:820-832` preserves launch identity and placement.
* `supercomputer/canary.py:40-160`: snapshots are kept under `scheduler/code/`;
  the canary watches for 600 seconds against the preceding 3600 seconds and
  rejects unhealthy code. `rollback_to` selects saved passing/previous code and
  marks the current code rejected. `scheduler.py:3465-3473` and
  `supercomputer/__main__.py:273-279` expose the same handback as `rollback`.
* `supercomputer/controller.py:157-170`: setup posts `/drain` up to fifteen
  times with three-second sleeps, then restarts scheduler and treadmill. Running
  work relies on shutdown/reattach, not a transferred listener. Services use
  `Restart=always`, `RestartSec=10` (`controller.py:98-117`).
* `scheduler.py:2721` checks stale agents between locally known items, but
  `scheduler.py:3624-3647` itself guards only strict running work and falls back
  to bootstrap on transport failure. `supercomputer/agent.py:2000-2012` writes
  replacement source and schedules start-daemon; `agent.py:2047-2088` replaces
  the exact daemon PID while refusing unresolved STRICT ownership. Legacy live
  workers are not a guard. Worker sessions survive daemon replacement, but
  that is weaker than upgrading only between items.
* `supercomputer/boat.py:2419,2482-2503`: `HANDOFF_QUIET=60` suppresses poll
  errors during recovery; retry continues for fifteen minutes. Remote clients
  reconnect remotely. A client originally given a local scheduler calls
  `ensure_running` after ten seconds, even if a controller has since appeared.
  `scheduler.py:4206-4232` also falls back locally when a configured controller
  fails its health check. Quiet logs are not an endpoint-affinity guarantee.
* `../supercomputer-leases/ROLLOUT.sh:1-65` requires a global quiescence barrier,
  reinstalls three hosts, switches the editable Air checkout and asks for a
  final manual controller step. Removing that barrier is NOT safe for the
  first mixed-version lease-protocol migration. Hot reload must preserve the
  lease identity and refuse incompatible protocol changes, not manufacture a
  new holder because a process PID changed.

## Handoff health: ownership versus sustained idle

The handoff target is still strict zero. The initial takeover and its +15/+30-second
ownership checks record missing VMs and orphans; readiness still requires the exact
admission and running-item identity sets before activation. Those hard faults have
no idle grace and are rejected on their first sample (or refuse readiness before
activation). If saved code cannot safely take over and rollback must be refused,
the observed hard fault still prevents later canary acceptance. No lease, listener,
ACK or wire-protocol field is changed.

Idle-only observations continue every 15 seconds during the 600-second canary.
They use the same placement eligibility as waste, but track a conservative per-VM
lower bound on continuously idle slots beside eligible work. A real placement,
disappearing eligible demand or disappearing VM resets the age; newly free slots
do not inherit an older slot's age. Only age **greater than `sub_preempt_s`** counts.

The canary reads handoff samples in disjoint **`(previous check, current check]`**
intervals. Handoff timestamps are retained exactly so rounding cannot skip a
completed observation across the cursor. Empty or healthy intervals reset its idle streak; two fresh breached
intervals reject. A pending first idle breach at the 600-second pass horizon waits
for the next fresh interval rather than passing before its confirmation. A single recorded idle observation cannot be counted again at
the next minute. The general health display retains its requested history and
worst-check value; other canary targets retain their existing windows.

The separate waste target is unchanged: eligible queued work must have waited
`sub_preempt_s`, and per-VM wasted capacity is the lesser of two 15-second samples
with no real placement between them. Its value is the rounded mean across the
window; the canary also requires at least two waste-bearing samples. Handoff idle
is **not** that mean: it has an explicit continuous-idle age and fresh-interval
confirmation. Missing VMs, orphans, readiness and identity conservation are not
subject to either idle tolerance.

## Target and limits

Use immutable, recursively hashed code snapshots; prepare code while the old
scheduler places; preserve the listener and exclusive lock over the switch;
require exact admission AND running-item ACKs before retiring the old owner.
Agent-side idleness must arbitrate upgrades against starts. Endpoint affinity
must outlive a client process's original module imports.

Tests must distinguish scheduler-state transfer from real OS descriptor and
systemd semantics. The virtual-clock simulator alone cannot prove an inherited
socket stays open or that service management preserves its child. A <5 second
placement-bound claim requires both load scenarios and an actual subprocess
handoff receipt. No deployment was performed by this lane.

## Implemented update protocol

* `scheduler.py:216` reads the explicit install pointer. `scheduler stage`
  snapshots the newly installed source, compiles it with the same interpreter
  that starts the successor, and verifies an import from that exact snapshot.
  Only then does it publish the pointer and report installed/active hashes,
  without starting a scheduler. This addresses
  processes whose original import directory differs from the new installation.
  Recursive source hashing includes `rules/`. Ordinary in-place installs are
  still discovered at the pointed source path.
* `scheduler.py:3532` import-checks and snapshots the candidate while placement
  continues. At the switch it serializes mutable HTTP requests, freezes
  mutations, keeps the old server answering health/status, and passes both
  listener and flock to the child. The old process does not unlock. The child
  does not rebind. Mutating requests retry the same endpoint until committed.
  Request bodies are fully read before entering the mutation lock, with a 15s
  socket timeout (incomplete or timed-out bodies receive 408 or close). Only
  dispatch and response serialization hold the lock; response writes happen
  after release. The frozen/passive/stopped guard is checked under that lock
  after the body arrives, so an earlier-started mutation completes before the
  snapshot while a later one retries without partially committing. A slow
  upload or an unread response cannot block unrelated mutations or handoff.
* The readiness receipt covers every required held VM, admissions and all
  running `(vm, batch, item, run_name, launch_identity)` entries. On the real
  socket path it is published only after the successor serves. Both snapshots
  are precompiled/import-checked before the pause, including old saved copies.
  Readiness uses a monotonic, workload-sized budget: `10s + 0.1s * VMs + 0.02s *
  running items + 4 * measured load time`, capped at 120s. Load time is the larger
  of the latest admission/pool reload and the current registry read/write. The
  selected budget and its inputs are logged and included in the snapshot. Lease
  heartbeat renewal remains active during this longer pause. A failed renewal
  preserves the frozen inventory but vetoes commit and permits a later retry.
  Failure kills only the candidate PID (no release path), restores parent lease
  metadata and tries saved code on the same descriptors. A deadline is retryable
  after a 60s cooldown, not a permanent code rejection. A concrete failed ACK or
  failed boot remains a rejection; the same installed hash automatically retries
  after cooldown if the failure was only the readiness deadline.
  The old process exits only after a complete receipt. Before offering the pool,
  it prunes empty holds known dead, stopped/deleted/errored by a fresh provider
  observation, or ready but lacking an agent. Missing provider observations are
  not evidence of death. Running/finishing items, pending starts/preparations,
  foreign work and unresolved admissions always keep their VM mandatory. This
  only clears the old process's advisory local hold; it never stops a VM or
  releases a VM-side lease. The snapshot records pruned IDs as `unavailable`.
  Extra VMs a successor discovers do not make an otherwise complete ACK fail.
* Handoff is now a reversible pause, not a stop of the scheduler's loops. A
  candidate serves its readiness receipt but does not start workers or placement
  until the parent commits by exiting. Failed candidates therefore cannot consume
  results or start queued work that the parent would miss on resumption. If **both**
  candidate and saved code fail, they are reaped, only their local lease metadata
  is restored, and the original process restores its PID and previous handoff
  files, wakes its existing loops and logs resumption. Its original listener
  continues answering health/status throughout, including candidate timeouts.
  Existing keep-alive connections cannot mutate a frozen snapshot. No third
  successor, provider stop/delete or lease-release operation is used.
* Legacy completion immediately frees the finished worker slot, but retains
  the unacknowledged result identity in a separate `finishing` inventory during
  log I/O. That inventory is included in the snapshot; the successor observes
  the terminal agent row instead of re-running it. Row commit is under the
  snapshot lock before ACK. Strict completion also rechecks the handoff flag
  after log I/O. Holding finished slots through log I/O was tested and rejected:
  it regressed `timing_subs_filler`, so no pool-growth rule was changed.
* `scheduler.py:3869` serves inherited descriptors. The controller unit uses
  `Type=notify`, `NotifyAccess=all`, `KillMode=process`; the old MainPID notifies
  systemd of the acknowledged successor before exiting. Controller setup no
  longer drains or restarts scheduler/treadmill for a code update. The explicit
  `controller stage` path (`controller.py:182`) uploads code only and refuses a
  controller lacking the hot-handoff capability.
* `agent.py:1823` checks unresolved strict ownership and unfinished legacy jobs
  under the same cross-process ownership lock as **all** starts. Upgrade fences
  admission before replacing source. The scheduler defers busy upgrades and
  never treats a refused/ambiguous upgrade RPC as permission to bootstrap a
  replacement. Repair cannot install different agent code under known live work.
* `scheduler.py:4319` fails closed for configured controllers. A long-lived
  client retries its existing endpoint through handoff; `boat.py` will not
  resurrect a local scheduler after a controller appears. An explicitly local
  client retains crash recovery when no controller is configured.

## Rollout boundary and evidence scope

`ROLLOUT.sh` is the replacement for the coordinator's script currently in the
leases worktree. After the reviewed commit is pushed, it publishes/stages code
on the sandbox, Air, mini, anh-1 and controller, printing installed/active hashes
and whether pickup is pending. It does not checkpoint agents, drain the pool,
restart services or wait for running items. **It has not been executed.**

The script requires an explicit compatible-lease-protocol acknowledgement.
This lane cannot safely waive the leases lane's first migration barrier:
old runtimes without a stage pointer/hot listener protocol cannot be taught
those features by replacing files they already imported. Likewise a client
that predates the new affinity guard still runs its original Python functions.
The first adoption of the protocol, and merging with `claude/vm-leases`, must be
handled by the coordinator; subsequent compatible updates need no checkpoint.
The sibling worktree's active script is not overwritten behind that lane's back.

The GDP laws prove production ACK/deadline/rollback-owner guards plus required-VM
selection, failed-handoff resumption, passive-candidate activation, bounded
workload budgets and timeout retry eligibility, not arbitrary
network latency, systemd behavior or semantic equivalence of two versions of
Python code. Broken variants retain a missing-item ACK, an off-by-one deadline
and a rollback that selects the candidate. Load scenarios test the item/VM
invariants. Real subprocess tests separately check listener/flock inheritance
and boot-failure rollback, with provider calls disabled. The isolated real-socket
case is not a real five-machine deployment or a loaded-controller benchmark.

## CPU constraints and endpoint affinity (sc-cpupin)

The CPU-model feature leaves lease identity, listener/flock inheritance, admission and running-item ACKs,
passive-candidate activation, and agent wire protocol unchanged. Its fields are additive: admission specs carry
`cpu_model` and an optional `cpu_model_max_fruitless`, handoff VM records carry `model`, and placements carry
`cpu`. Old snapshots without a model remain loadable; constrained placement waits for the next agent report
rather than assuming a CPU. Draw outcomes live in a separate atomic `cpu-constraints.json`, so a handoff or
daily reset does not reset the scarce-model guard. Native clients require the new `cpu_model_exact` capability
before submitting constrained jobs. Snapshot preparation reads the candidate's capability using the exact
candidate interpreter/import. Before freezing, the parent refuses any successor/rollback candidate lacking
exact-model support while constrained admissions remain live. In-flight CPU acquisitions settle before the
freeze (placement continues during this deferral), so the successor cannot race the old draw accountant.
No new lease protocol is required.

Distinct-VM groups add `distinct_vm_groups` to the snapshot and ACK, including VM IDs consumed by completed
placements. A group placement is journaled and fsynced under the placement lock before launching. Reload
replays all placement records (not only unfinished work). The parent only commits an exact history ACK; a
missing/mismatched history leaves the candidate unready and resumes the parent. Preparation refuses a
group-unaware candidate/rollback while grouped work remains live. Old snapshots with no groups still work.

### State export cannot serialize intake

`/state` is now a read-only route. Its legacy binary body/response contract is
unchanged, including callers already running old `maybe_sync` code. A brief queue
file-generation snapshot still serializes with mutations, but archive traversal,
reads, compression and delivery do not. One archive build serves concurrent
callers, with a 30-second completed-result cache. New clients additionally make
hourly backups single-flight and asynchronous, with the attempt persisted before
network I/O. Existing client processes keep their imported code until they exit;
the server isolation/cache protects their repeated requests in the meantime.

This update changes no lease/listener/ACK protocol and requires no controller
restart. Stage the compatible update through the normal rollout; let the current
scheduler perform the hot handoff. Do not cancel active proof batches to install
this fix.

### Client input errors do not judge server code

The JSON decoder preserves the old wire contract: an empty body means `{}`, legacy `id` and current `batch`
keys work, and Python's JSON byte decoder retains UTF-8/BOM, UTF-16 and UTF-32 support. Body-independent
readers still accept any valid JSON root. Binary upload routes are not JSON-decoded.

Malformed JSON is a **400 client error**, with an explicit connection-close response. The exception handler
surrounds only input decoding: a JSON error raised later by server work is still a 500 recorded against server
health and the canary. Short or timed-out bodies remain 408s and cannot be dispatched as a partial request.

Scheduler POST 4xx responses are counted separately in `health.client_4xx` in status and in `/health`'s
`client_4xx` observation: `count`, `by_status`, and the last four bounded metadata records over the requested
window (30 minutes for status). These records include path, declared/read length, peer and User-Agent, never
body or token. A timed-out buffered read has an unknown read length, not a fabricated zero. These observations
are not canary targets; a client-fault burst stays visible without rejecting healthy server code. Existing server
error history is retained, not retroactively guessed away.

### A live snapshot can lead the placement journal

Legacy (non-strict, ungrouped) placement is journaled after preparation, but the live handoff snapshot can
already contain that `starting` item. Admission replay creates a fresh `Item` before overlaying the live
snapshot, so `Item.placed` must start at zero even when no `.placed` record exists. The overlay takes the
maximum of the replayed count, snapshot count and run-name suffix, retaining launch identity. Previously
this path raised `AttributeError` before the candidate wrote an ACK; even the saved code failed on the same
snapshot. The healthy parent resumes in that case. A normal dead-parent cold start ignores the stale live
snapshot (its writer must still be alive and under ten minutes old), so this exception was not an inevitable
cold-start crash loop. Do not restart as a repair: install/stage the fixed new hash and let the live parent
hand over. A startup crash rejects that specific hash; a new fixed hash does not require editing canary state.

Client endpoint selection now defaults to `live/controller.json`, including fail-closed behavior for an unreadable
configuration. Only `SUPERCOMPUTER_LOCAL=1` explicitly permits a local scheduler when a controller is configured.
The old `SUPERCOMPUTER_CONTROLLER_FALLBACK` flag is not permission. A long-lived client's recovery path does not
resurrect a disappeared local scheduler beside a newly configured controller. **Old clients retain their imported
functions:** staging the scheduler cannot fix those processes; the coordinator must replace/restart those clients
or keep them explicitly on the controller. No rollout was performed by this lane.

Verified 2026-11-10: a docs-only update rolled out hot to all five machines under load (canary batch and live lanes), no item killed or re-run.
