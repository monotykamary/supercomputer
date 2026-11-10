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

### Current state

Stage 1 is implemented and rebased onto `origin/main` at `1fa659f` (hot scheduler handoff and controller staging). Tested code tip: `4ee23ef885cc240296be8d5b265ea534c25e5198`. **GREEN: all final gates passed.** Stages 2/3 (shared start escrow/accounting and cross-client preemption) remain out of scope.

### Commits and integration

- `bfa13b1`: design of record.
- `4b1e9c5`, `dc9aabf`: VM-local locked leases, boot/epoch fencing, non-killing drain, per-client keys/results, birth reservations, renewal/adoption, fenced cleanup and lease-loss requeue. Agent upgrades retain and guard `lease.json`; hot handoff carries exact lease/TTL and running/finishing identities. Ambiguous replies do not authorize bootstrap.
- `fa7db22`, `74cfcab`, `f04ab19`: ownership laws and independent-client scenarios, first-migration safety, concurrent warm-pool validation. `ROLLOUT.sh` is upstream's compatible hot-staging script, not the superseded drain/setup script.
- `35d4c59`, `a7a12d4`: preserve unclaimed booting VMs, monotonic startup budget, fenced TTL, mixed timing reuse and stopped-provider recovery, retaining upstream break-even economics.
- `c51f28f`: exact live-parent lease restoration without network calls on the four-second handoff path; explicit drain-parameter invariant and handoff regressions.
- `e9f7f2c`: preserve urgent two-second result polling after the queue drains.
- `92f8a2b`, `d258ec4`, `a594fcc`: whole-job capacity for uniform one-slot gates, floating-point lower-bound protection, and a common feasible horizon for held and incoming workers.
- `b3a22af`: discrete dispatch-boundary regression and a handoff fixture that cannot spawn provider TTL work.
- `ca87795`: bounded default of 12 concurrent bootstraps. A traced isolated gate passed in 1092.56 s with every created VM used; 10 missed the bound, while 16 bought unused workers. Explicit policy overrides remain honored.
- `4ee23ef`: configurable `SC_SCENARIOS_CPU`, default 2. Final scenarios reserve 4 CPUs rather than relaxing the unchanged real-time watchdog or assertions.

### Verification receipts

All execution uses the installed client with `SUPERCOMPUTER_CONTROLLER=1`, backend Boat, class `gate`. Candidate schedulers run only inside the mocked provider simulation, not against actual shared workers. Local work is limited to file/git operations and short read-only controller checks.

Final runs share immutable tree **`b69d577002e5`** from code **`4ee23ef`**:

| Gate | State | Receipt | Batch |
| --- | --- | --- | --- |
| All 80 scenarios, 4 CPU/job | **PASS, 80/80**, no known failures | `runs/leases-hotreload7-scenarios/gate.log` | `20261110-194922-9eqcsw` |
| All laws (`SC_CHECK_CLASS=gate`) | **PASS**, 499 proven + 125 refuted (624/624) | `runs/leases-hotreload7-laws/gate.log` | `20261110-194922-nu935u` |
| Full units, including real socket/flock handoff | **PASS**, 44.52 s | `runs/leases-hotreload7-units/remote/summary.tsv` | `20261110-194922-exyxo3` |

All 80 final scenario jobs returned; no Boat losses or missing results. The large gate finished in **1092.56 s** (bound 1200 s), with **68 created workers, zero unused starts, and zero lost rows**. No assertions or watchdogs were relaxed.

Final unit logs contain no thread/assertion errors. Real socket handoff/rollback response gaps were 0.545 / 1.720 s.

Predecessor `a594fcc` passed all units in 31.37 s (real handoff/rollback response gaps 0.347 / 0.976 s) and **499 laws + 125 broken variants = 624/624**. Its scenario run returned all 80 jobs, with no Boat loss: the large gate was 0.63 s late with two unused starts, and `wide_behind_filler` hit the simulator wall-clock watchdog. The latter passed unchanged in isolation: all 120 wide items delivered in 135.7 s (`runs/leases-wide-isolated/remote/wide_behind_filler.log`). These predecessor receipts are not substituted for the final gates.

### Safety and rollout

No push, deployment, local `scheduler serve`, or manual change to shared workers/controller was performed. Recovery checks found zero remaining owned batches before each new final submission; no duplicate collector was started. The earlier shared-pool stops were diagnosed by the coordinator as its old Mac scheduler/fence race, not this lane's tests; the temporary dedicated-VM requirement was lifted.

New workers carry the `~l` suffix, which old discovery does not match. If an old client nevertheless contests a worker, the new client fails closed and yields without token replacement, kill, stop, delete, or TTL reduction. Account-level commands from an old runtime cannot be fenced by a new agent: the **first incompatible migration still requires all five old runtimes and their automatic launchers quiescent**. Subsequent compatible hot updates preserve the machine's lease holder identity and use upstream's staged rollout without draining jobs or restarting the controller.

`ROLLOUT.sh` requires an immutable merged SHA and explicit protocol-compatibility acknowledgement. It stages the sandbox, Air, mini, anh-1 and controller and prints installed/active/pending state. It was not executed. On anh-1, the old scheduler's different Python installation must be accounted for during the initial migration; replacing the uv tool alone is not proof that the old runtime changed.

LocalItem progress is per granted-lease interval. Eventual completion assumes stable availability; infinite revocation or other clients holding all capacity forever is excluded. Busy pre-lease work is never killed to manufacture a passing rollout.


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
