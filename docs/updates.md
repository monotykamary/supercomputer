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
