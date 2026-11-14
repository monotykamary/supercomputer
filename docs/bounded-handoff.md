# Bounded handoff while the pool is growing

A handoff used to wait for the whole current pool to settle while ordinary
scheduling could keep adding acquisitions, booting VMs and agent upgrades.
Continuing work could therefore postpone the update indefinitely, even when
each individual acquisition finished normally.

The outgoing scheduler now prepares the immutable candidate and saved rollback
code, checks admission compatibility, then starts a target-specific drain:

1. **Pool phase:** fence new acquisition reservations, adoption claims and
   agent upgrades/repairs. Already-reserved operations finish and publish their
   VMs normally. Admissions, placement on ready VMs, completions and lease
   renewal continue. Discovery alone is not a pending ownership claim.
2. **Final start phase:** once the existing pool and upgrades settle, fence new
   placement and idle retirement. Existing strict launches must settle before
   snapshotting. This phase does not wait for running jobs to finish.
3. **Existing transfer protocol:** retain the listener and lock until a passive
   successor acknowledges every required VM, admission and running identity.
   Only parent exit activates the successor. Failed candidates retain the
   existing exact-child rejection and saved-code recovery paths.

Both drain phases share **one five-minute monotonic deadline**. Retrying the
same target does not extend it. At the next observation of expiry, the old
scheduler reopens growth and placement, keeps all ownership, logs the blockers,
and waits 60 seconds before another drain attempt. Sampling and already-running
provider calls can add observation latency; this is not a hard real-time bound.
Expiry is a retryable deferral, not a workload failure or a bad-code verdict.
A changed or withdrawn normal install releases its old drain and must pass the
ordinary source-stability check again. Explicit saved-code handback has priority.

This removes starvation caused by *new* arrivals. It cannot promise a successful
handoff if an already-owned VM or strict launch never becomes transferable.
Such a dependency remains required: the scheduler does not omit it, invent an
ACK, transfer an unfinished acquisition, or reinterpret it as an unoffered
registry extra merely to meet a deadline.

Status exposes `handoff_pending` with the target, generation, phase (`pool` or
`starts`), age and remaining drain time. Expiry diagnostics include acquisitions,
creates, unready VMs, upgrades, adoption claims, reattachment and strict starts.
`HandoffDrain` in `supercomputer/rules/updates.py` owns the non-sliding deadline
and generation-scoped reset; its production query predicates and transitions
have native-source laws and broken controls.

The `handoff_acquisition_stream` scenario keeps new-image work arriving through
880 simulated seconds, with ordinary 180-second boots, running anchor processes
and filler. An update requested at 120 must be serving by 600, while arrivals
are still open. It checks exact offered/acknowledged ownership, unchanged anchor
processes and placements, single physical starts, successful rows, continued
acquisitions, no orphaned live items and a passing canary. It fails on the
pre-fix source without changing the pool-settled predicate or stopping traffic.

This code controls the **outgoing** handoff. Installing it from an older running
controller still uses that older controller's handoff implementation. Deployment
remains a separately reviewed operation in a suitable gap; tests do not authorize
a rollout or a change to other running workloads.
