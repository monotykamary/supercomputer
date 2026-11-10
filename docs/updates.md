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
  snapshots the newly installed source, publishes that pointer atomically and
  reports installed/active hashes without starting a scheduler. This addresses
  processes whose original import directory differs from the new installation.
  Recursive source hashing includes `rules/`. Ordinary in-place installs are
  still discovered at the pointed source path.
* `scheduler.py:3532` import-checks and snapshots the candidate while placement
  continues. At the switch it serializes mutable HTTP requests, freezes the
  state, stops accepting on the old server without closing its listening file
  description, and passes both listener and flock to the child. The old process
  does not unlock. The child does not rebind. Queued TCP connections survive.
* The readiness receipt covers the exact held VM set, admissions and all
  running `(vm, batch, item, run_name, launch_identity)` entries. On the real
  socket path it is published only after the successor serves. Failure or a
  four-second startup deadline kills only the candidate PID (no release path),
  restores parent lease metadata and tries saved code on the same descriptors.
  The old process exits only after a complete receipt. If **both** candidates
  fail, the old process retains the listener and ownership and alerts; it does
  not claim placement recovered. This exceptional path still needs operator
  repair and is not a universal bounded-liveness guarantee.
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

The GDP laws prove production ACK/deadline/rollback-owner guards, not arbitrary
network latency, systemd behavior or semantic equivalence of two versions of
Python code. Broken variants retain a missing-item ACK, an off-by-one deadline
and a rollback that selects the candidate. Load scenarios test the item/VM
invariants. Real subprocess tests separately check listener/flock inheritance
and boot-failure rollback, with provider calls disabled. The isolated real-socket
case is not a real five-machine deployment or a loaded-controller benchmark.

Verified 2026-11-10: a docs-only update rolled out hot to all five machines under load (canary batch and live lanes), no item killed or re-run.
