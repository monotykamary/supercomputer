# Lane sc-leases: Stage 1 of docs/multi-client.md, VM-granted leases

Goal (user, 2026-11-10): many supercomputer CLIs on many machines share one boat pool without stepping on each
other. Today each scheduler adopts any up VM of its images that isn't in its own registry, rewrites the agent's
token, and kills items from batches it doesn't know. With two schedulers that turns into a 403-token war, and we
hit it three times today. Stage 1 makes that impossible no matter how many schedulers run. It doesn't need the
controller or the `SUPERCOMPUTER_CONTROLLER` switch.

Branch `claude/vm-leases`, worktree `~/VCS/working-remote/open-source/supercomputer-leases`, base 3661fae (main plus the
horizon/preempt defaults). The design is docs/multi-client.md, Stages 1 and 4 and its laws. Implement Stage 1 only.

## Decisions (coordinator, for the spec's open questions that touch Stage 1)
- Lease timeout 180 s (spec proposal). Lapse is measured on the VM's own clock only.
- A holder that has gone quiet: the new claimant does NOT kill its running items. They finish under the VM's TTL,
  nothing new is placed for the old holder, and the claim takes effect when the VM is empty or the TTL lapses.
  The user's rule: never lose a running job's data.
- Mixed rollout as the spec says: new VMs get a name suffix old clients don't match, and an old client wins any VM
  it contests. Say exactly what an old client sees.
- Per-client agent keys, added through `boat exec`, so the token war ends without sharing a secret.

## Work, in order, committed as each piece lands
1. Commit docs/multi-client.md (already in the worktree) as the design of record.
2. Agent side (agent.py): lease.json under a file lock, a holder id per machine, the fencing pair (boot_id, epoch),
   checks on claim, start, kill, upgrade and stop, and a birth claim written at creation (`boat new --file`).
3. Scheduler and client side: adoption asks the agent and claims only a free or lapsed VM; never stop or delete a VM
   you don't hold; stop and delete only after the agent accepts a stop-fence from the current holder.
4. Laws with gdp-wired (tests/laws.toml, tests/check_laws.sh): at least these.
   - A VM never runs items of two owners at once.
   - A stale epoch's item is never accepted.
   - A stop is never issued by a non-holder.
   - A resumed or forked VM's old lease is void.
   Keep the broken variant of each and assert it's refused (tests/broken_*.py).
5. Scenario tests (tests/scenarios.py with tests/sim): two and three schedulers on one simulated pool. This
   includes today's incidents: a Mac scheduler started by a long-lived batch, a test run starting a local scheduler
   beside the controller, sleep and resume, and a snapshot-restored VM.

## Rules
- Run tests on boat only, through `supercomputer` with SUPERCOMPUTER_CONTROLLER=1 (it's in ~/.profile). Never start
  a local scheduler: check with `ps` that no `scheduler serve` runs here after each submission. The repo's checks:
  `tests/check_scenarios.sh` (units; it deletes its out dir, so keep your own copy as
  ~/VCS/working-remote/open-source/bend/.. scratch or a runs/ dir), `tests/check_laws.sh`. Jobs that run tests/test_*.py
  need OWNED_LIFECYCLE_BOAT_TEST=1.
- Never deploy: don't touch the live controller (bx_gz4zdtbh), the Mac's or mini's installs, or the shared pool's VMs.
  Don't push. The coordinator reviews, merges and rolls out.
- Lane D's final gate for gdp-wired 0.48.0 is running on the shared pool: don't preempt it or restart anything.
- Kill only by exact PID, never pkill. One waiter per thing. Never end a turn waiting on boat.
- Report in LANE.md under "Report" (commits, test receipts, laws proven and refuted, open issues).

## Report

### Implementation and commits

Stage 1 implementation is recorded, but **this lane is NOT rollout-ready**: the full scenario gate is not green and the coordinator halted shared-pool lifecycle testing. Stages 2/3 (shared starts escrow/accounting and cross-client preemption) are not implemented.

- `92bf6c0` — design of record.
- `2378455` — agent-side file-locked leases, boot/epoch fencing, non-killing drain, per-client keys and owner-scoped results.
- `ca769a0` — birth reservations, client/scheduler renewal and adoption, fail-closed stop/delete/TTL changes, lease-loss requeue, and conservative maintenance.
- `1c82208` — executable laws, four broken counterparts, and independent two-/three-client incident simulations.
- `ba312e3` — five-machine quiet-barrier rollout plan and guarded, pinned-revision `ROLLOUT.sh`.
- `ae30f43` — parallel warm-pool lease validation to address startup latency; **dedicated-VM verification still required**.

No deployment, push, local `scheduler serve`, or manual change to shared workers/controller was made. All executable tests were submitted through the installed client with `SUPERCOMPUTER_CONTROLLER=1`, backend Boat, class `submission`. Local checks were limited to file/git operations, `git diff --check`, and `bash -n ROLLOUT.sh`.

### Verification receipts

- `runs/leases-laws2/gate.log` and `runs/leases-laws2/run/summary.tsv`: complete law gate **passed** — **483 proven, 119 broken variants refuted (602/602)**. This includes all **10 VMLease laws** and the explicit LocalItem per-lease progress law. All 9 positive modules exited 0; all 13 negative modules exited 1 with the required refutations.
- `runs/leases-laws1/run/broken.broken_vm_leases.py.log`: all **four deliberately broken counterparts refuted**, each with a counterexample (CLI verdict `broken`).
- `runs/leases-pass3/remote/multi-client.log`: **all three** incident scenarios passed (`lease_two_schedulers`, `lease_three_schedulers`, `lease_sleep_restore`), exercising independent machine identities on one pool, a long-lived client, a local scheduler beside a controller, sleep and restored snapshots.
- The first complete gate exposed legacy tests which assumed unfenced cleanup/token replacement. Those fixtures were changed to assert the new safety contract, not to restore unsafe behavior. The old unconditional LocalItem progress law also exposed endless lease revocation; its replacement explicitly proves progress per lease interval and records the stable-availability assumption.
- `runs/leases-final-units/remote/summary.tsv`: complete unit suite **passed** (exit 0, 24.5 s job time), including 28 bootstrap tests, 26 lease-client tests, and 12 VM-lease persistence tests. This precedes the final `ae30f43` startup follow-up.
- `runs/leases-gate5/gate.log`: full scenario gate **FAILED — 24/75 passed**, including both real assertion failures and Boat-lost/no-result jobs. Several failures report scheduler-start timeout; `ae30f43` addresses sequential warm-pool lease validation, but its corrected rerun was stopped before verification. The three new multi-client scenarios passed.
- `runs/leases-final-small/remote/`: two infrastructure failures (`code=-1`, `seconds=-1`, VM lost); neither is test execution or a passing result. No success is claimed for a merely submitted or queued job.

### Coordinator safety stop

At the coordinator's stop instruction, all further shared-pool submissions ceased. Closed exact owned IDs: `20261110-182226-5rx6vr` (the remaining active batch), `20261110-174331-0pcwiu`, and `20261110-174640-7gqwvr`. Also issued idempotent closes for `20261110-174154-fi3k08`, `20261110-174226-4ovrbm`, and `20261110-180116-pzovar`, already completed/closed. The controller returned **no remaining `supercomputer-leases` batches**. The last local collector was terminated by exact PIDs after remote closure, preventing its automatic retry from resubmitting.

The coordinator reported unforced shared-VM stops and suspects lifecycle tests; that root cause is **not established by this lane**. Do not run any lifecycle test on the shared pool again. All further lifecycle tests require a dedicated, explicitly owned disposable VM (`boat new`, then delete that exact VM after testing), with no shared worker hosting them. No such VM was created here after the stop instruction.

### Required five-machine rollout order (coordinator only)

1. **Freeze launches on all five hosts:** sandbox `bx_uxa4mxjf`, MacBook Air, mini, anh-1, and controller `bx_gz4zdtbh`. Pause automatic launchers/treadmills and new submissions. Let existing jobs, gates and result collection finish; do not kill their work.
2. **Close the global old-runtime barrier:** stop every old scheduler and long-lived batch process only after drain, disable automatic restarts, and verify this on all five hosts. An asleep/unreachable anh-1 or another missing acknowledgement blocks activation. Installing a package is not proof that a running Python process has upgraded.
3. **Keep the barrier closed.** Install the same reviewed, merged **immutable SHA** in order: **sandbox → MacBook Air → mini → anh-1**. Verify all installed code hashes match. Never copy `live/`, `owner`, `owner.key`, or cached agent keys between machines.
4. **Controller last, from the updated Air, between gates:** run the reviewed `controller setup --vms 0` procedure only after its old processes are stopped. Attest running `code`, `code_installed`, and expected `agent_code`; verify all five machine owner IDs are distinct. The script prints this last command but does not execute it.
5. Only after all attestations, reopen **new controller work first**, then sandbox/Air/mini/anh-1 launchers. Any failed install/hash/runtime check leaves the barrier closed. Do not restart an old runtime while new workers exist.

**What old code sees:** new workers are named `<image>~l`, which the old exact-name adopter excludes. Names alone do not protect a VM ID already cached by an old process. Therefore old code may remain on disk during installation, but no old runtime may execute in the mixed-installation window. Known legacy workers are migrated only after that barrier. Unexpected old code is still contained by relinquishing a contested VM without stopping it; this fallback is not the no-contest rollout guarantee.

`ROLLOUT.sh` requires `SC_ROLLOUT_QUIESCED=all-five` plus a 40-character merged commit SHA, fails on installation/hash mismatch, and never automatically reopens launches. That acknowledgement is an operator precondition, not an automatic proof of quiescence. Script syntax was checked; **the rollout was not run**.

### Scope and open issues

- **Do not roll out.** Investigate the shared-pool safety incident and complete the remaining startup/scenario validation on a dedicated VM before merge/rollout. The suspected test causing unforced stops has not been identified.
- Busy pre-lease jobs are deliberately left untouched until they finish or their existing TTL expires; drain them before migration. Incomplete/unclassified work fails closed rather than being killed or treated as success.
- LocalItem progress is per granted-lease interval. Global completion requires eventual stable availability; endless revocations and other clients holding all capacity forever are explicitly excluded.
- Raw account-level commands from an old client cannot be fenced by a new agent; the all-five quiet barrier is mandatory for the no-contest guarantee.


## Rollout (coordinator, 2026-11-10; the user: "make sure the other macs get updated so it follows the same lease protocol")
These all share the one boat pool and must run the lease protocol together:
- the boat sandbox bx_uxa4mxjf (uv tool);
- the MacBook Air (editable ~/.local/share/supercomputer);
- mini (uv tool);
- anh-1, anh's Mac (uv tool, its own scheduler for aip, aipreviewf and midscenegd: same account);
- the controller VM bx_gz4zdtbh (`controller setup`, between gates).

ROLLOUT.sh in this worktree updates each one and prints every machine's code hash. Design the mixed-version window
so an old client never contests a new client's VM, then say in the Report what the rollout order must be.
- 2026-11-10 note: anh-1's running scheduler (pid 31112, up about 1.5 days) runs from a different Python
  (~/.local/share/uv/tools/modal/bin/python), not the uv supercomputer tool that ROLLOUT.sh updates. A rollout has
  to restart that scheduler onto the new install, or prove the handoff to new code happens by itself. Say which.
