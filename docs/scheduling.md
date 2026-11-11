# Scheduling

How the one scheduler places work on the pool, grows and shrinks it, and stays within boat's terms. The short version is in the [README](../README.md); every decision here is a rule in `supercomputer/rules/` with its laws.

## One scheduler for all of the pool's work

Every batch (`supercomputer run`, a bar's launches, a runner's calls, the treadmill's rounds) is submitted to the
scheduler (`supercomputer/scheduler.py`), which alone holds the VMs it runs them on. It runs on your machine (below): started by the first
client, 127.0.0.1:8790, gone after two idle minutes. `SUPERCOMPUTER_SCHEDULER=0` makes a batch hold its own VMs, as before. Agents read
`supercomputer --skill` (SKILL.md, live values filled in), the short guide; this page is the depth.

## Scheduling policy

The pool exists to run work with as few starts as possible: warm VMs are kept busy, and a start is made only for
work they can't clear. Knobs are `$SUPERCOMPUTER_<KNOB>`; the defaults in brackets.

**Priority.** `interactive` > `gate` > `submission` > `sweep` > `filler` (`--class`, `$SUPERCOMPUTER_CLASS`; a small
`run` batch is interactive, a bigger one a submission). Higher work takes a lower class's slot at once: an interactive
or gate item kills a filler's or a sweep's item, which is queued again at the head of its class; a submission takes
slots as lower items end (a filler's after waiting SUB_PREEMPT [45 s]). Within a class, clients are served
round-robin. A submission client holds at most SUB_VMS [8] VMs' worth of slots at once while a waiter that could take
the slot exists (rules.contends: of its image, of a shape a VM takes, not backing off; a higher class, or another
submission client under its share); a gate gets what it needs. An item needing several slots (a weight past 1) or a
whole VM (timing) gets slots on one VM set aside, the nearest to empty, beside work of its own class too when the slots
past it fit (rules.may_set_aside): its lower-class items killed, nothing new placed in the slots it needs and only
those, and only while it waits (rules.reservation_holds: the 13:25-13:40 incident, 15 VMs held empty for items that
had run elsewhere). Preemption goes on past an item that finds nothing to take: only the items of its shape are passed
over in that pass (rules.PreemptPass). Health's `waste` target says it directly: slots on filler or idle while real
work that could take them waited past SUB_PREEMPT, want 0.

**Exclusive gate liveness.** An unserved interactive/gate timing batch gets a real service lane: a matching idle VM,
a filler/sweep VM it may preempt, a matching start already coming, or a new start. An estimated end of another
batch is not that lane. Once a batch has a lane, the horizon sizes its remaining work; this is not one start per
item. Foreign run inventory is refreshed even on otherwise empty workers and is never promised as gate capacity.
A terminal legacy row releases its held slot even after its batch was closed; strict cancellation still requires
its identity-bound reap receipt.
A direct resume 404 discards only this process's exact lease and credentials and replans/replaces from the image;
it does not repeatedly resume the missing ID or count a refused resume as a start. Billing health reports provider
blocks, not money thresholds; prepaid balance information includes both plan hours and purchased-pack hours.

**Where capacity comes from,** in order: a free slot on a held VM, a lower class's slot, a free start, a paid start
(every class may pay; boat's limits read live from `boat limits` `startLimitsWithWallet`). Modal only once boat has
failed an item 3 times (a VM lost, its row missing, its preparation failed, no VM ready within the batch's max wait),
retried on another VM each time; a batch run with `--backend modal` stays on Modal.

**Growth is earned by marginal work,** evaluated on every queue change (`_plan`), never on a timer.
For each VM type, let `S` be its slots and `R` its VM-hour price. The economic break-even is

```
hold_dollars = hold_s * vm_hour_dollars / 3600
R = vm_hour_dollars * (type_vcpus / default_vcpus)
B = 3600 * max(hold_dollars, start_dollars) / R
```

Holding an idle default VM for `hold_s` defines the policy's existing opportunity-cost baseline, in the same
**default-VM dollars** as the digest. A start must earn at least that cost, or its replacement price when higher.
Convert money back to busy time using the new VM's rate: a large VM provides twice the default capacity per second,
so do not impose the default VM's wall time on it again. The actual release hold remains 45 minutes for every type;
only growth's cost comparison is normalized.

`start_dollars` defaults to $0.03, the first paid-start bracket (`boat limits` on 2026-11-10 reports $0.03, $0.05 and
$0.07 brackets). `vm_hour_dollars` defaults to $0.036 per default-VM hour, as in that response and the billing digest.
These are opportunity prices, including for free starts and prepaid hours: both consume shared allowances. Set the
price input when the marginal tariff changes, not to tune a queue timeout. With a 2,700-second hold the baseline is
$0.027, so break-even at the first bracket is 1,500 seconds for a large VM and 3,000 for a default VM. At $0.07 it
becomes 3,500 / 7,000 seconds respectively. No boat API is called in the planning loop.

- `W` is queued real slot-time (all classes except filler, each contended submission client counted only up to its
  remaining share). `C` is held and coming usable capacity over `B`, minus running real work within `B`.
- Ordinary growth is `floor(max(0, W - C) / (S * B))`, not a ceiling: each additional VM has a **whole break-even
  workload left after the held/coming pool and earlier starts have been credited**. A partial VM does not buy a
  start. In particular, when the held pool clears the queue within break-even, ordinary growth is zero.
- Gates (and interactive checks) have a separate deadline, `GATE_HORIZON` [15 min]: add the minimum VMs whose
  capacity covers the gate deficit before `batch.t0 + GATE_HORIZON`, after bring-up, using a **ceiling**. The
  remaining deadline shrinks on each replan; it is never reset to another 15 minutes. Held free slots and
  slots reclaimable from filler/sweeps count first; running higher work, foreign runs and coming VMs' bring-up are
  deducted. The target is the maximum of economic growth and gate growth, not their sum. As before, this is an
  estimate-based fluid capacity promise. An indivisible item needs at least its own runtime plus bring-up, so
  `gate_window` retains that feasible tail if the deadline is too close or already missed; it does not promise an
  impossible completion. Indivisible jobs may straddle the horizon, and the bring-up cap/breaker can delay capacity.

**Why the counts have those roundings.** Write `Q = S * B > 0` and `D = max(0, W - C)`. For economic growth,
`k = floor(D / Q)` implies `k * Q <= D < (k + 1) * Q`: every one of the `k` starts is backed by a full workload,
and the next is not. In particular `W <= C` gives zero. For a gate, use its available per-new-VM slot-time `Q > 0`
and `n = ceil(D / Q)`: `C + n * Q >= W`, while one fewer VM cannot cover a positive deficit. `Pool.grow` admits
that request whenever the breaker is closed and it fits the remaining bring-up/max-VM room. The admission
predicates and cap implication are machine checked; the measured-value division boundary is separately tested.
The no-needless-start law is for **economic** starts. A gate whose work the held pool clears in 40 minutes still
needs deadline capacity for 15 minutes. Applying the economic veto to it would contradict the deadline law.
The existing exceptions also remain: an empty/failed pool needs a service VM, and an unserved exclusive batch a
matching lane. Classes, floors, the breaker and the bring-up cap are unchanged. `horizon_s` is retired as a policy
knob: legacy saved values are ignored and cannot be set. Status reports the economic inputs and each target's
`break_even_s`. Only the explicit `SUPERCOMPUTER_HORIZON` environment override remains, for controlled comparisons
and forensic replay of old policies (two historical placement scenarios used 900 seconds). It is unset by default,
is never inferred from a saved policy, and disables the economic guarantee by substituting the requested window;
it is not a way to tune production growth. The new gate and steady-submission scenarios exercise economics, not
that override.
- an interactive check alone starts one VM, and none while any VM of its image is held;
- the existing 3x bring-up usefulness guard (measured, ~10-120 s) still applies to ordinary growth; it cannot veto
  capacity required by a gate deadline or an exclusive service lane;
- bounded by BRINGUP [10] VMs coming up at once (the scheduler's own load guard; the rest queue) and boat's
  per-minute limit and pacing (StartGate). No VM ceiling by default (`max_vms` in the policy sets one if
  needed, and the digest counts when it binds), and no VM-hour or dollar budget: the starts above are the economy.
- estimates are checked against what a batch's items take: once 3 finish, its queued items count at the observed
  ratio (never below their own estimate).

**Shrink, damped.** The floor (the treadmill's hint, 8) stays while the treadmill is alive (its hint, or a sweep or
filler batch of the image open), or always with `floor_always`: a VM within it is never released then, and one with
nothing to run stays at most HOLD before it goes (busy, never parked: the treadmill's filler keeps it busy). A VM above
it runs filler for HOLD [45 min: a paid start's break-even, the allowance shared with other projects] after its last real work (real work takes its slots at once); past its hold the
surplus is shed in steps, a staged drain: at most `drain_step` [0.25] of the VMs above the floor (rounded up) a window,
a window lasting until `drain_every_s` [300 s] after its last shed, so the holds of one big wave ending together never
let the whole surplus go at once. A VM shed drains (its filler queued again, no new filler) and is released once
empty; one above the floor with nothing at all to run is released on its turn of the drain.
**The default floor (1), a deliberate exception to "busy, never parked".** An image with no treadmill keeps one VM
while it has had real work within `floor_idle_s` [2,700 s, the same as the hold]: a project between bursts finds its VM warm instead of
releasing it and starting another (each start costs the shared allowance and a sync: pifovea-img started 3 times in 14
minutes, 11-01). That VM idles between bursts, one per active project, on purpose; past `floor_idle_s` with no real
work it goes like any other. `floor_default` sets the number, `floor_by_image` ({image: n}) an image's own (0: none,
for a project finishing up), applied live; a floor keeps VMs, it never starts one. Law: rules/scheduler.py's
image_floor ("an image with recent work keeps at least its default floor", "an image idle past floor_idle_s holds no
floor VM"). A released VM stops 60 s
later unless a batch leases it first. (The old 10-minute `demand` window is retired: `supercomputer demand` is a no-op
kept for old callers.) Laws: rules/scheduler.py's VMLife (the floor's hold, the drain's turn) and Drain ("in any
drain_every_s window at most ceil(drain_step x above-floor) VMs are shed").

**Never runaway.** A circuit breaker stops every start (free or paid) when over half of at least 4 recent starts
weren't ready within 8 minutes, or when boat's front fails most VMs' agents at once (the 10-31 incident: every tick
502, VMs declared lost and replaced in a loop); it pauses 5 minutes, doubling at each trip in a row, then lets one
probe start through and reopens only once that comes up. During such an outage no VM is declared lost for ~30
minutes and no item is charged a failure. A watchdog pauses starts (and the scheduler's own sends go one at a time)
when its host's load passes 1.5 per CPU or its free memory 15%. Alerts go to the log, `live/scheduler/alerts.jsonl`
and `status`.

**Preparation on the workers.** A VM syncs a batch's tree by hash, pulling the files it lacks from a peer VM that has
the tree (and build caches the same way); the scheduler sends a tree itself only when no VM has it, PREP_MAX [2]
at once.

**Timing.** A timing job (`--timing`, a job's `"timing": true`) runs alone on an emptied VM, its vCPUs pinned;
`<out>/placement.json` records each job's VM type, CPU model, vCPUs and co-tenants for the claim's label.

**Restarts are lossless.** Each batch, its delivered rows, each item's placement and output are on disk
(`live/scheduler/queue/`); a restart re-attaches items still running (their VMs kept at the 15-minute TTL), queues
the rest again, and clients poll through the gap. Deploys drain first. The day's accounts (`live/scheduler/metrics/`)
and the breaker survive too.

**Durable and upgrade-safe.**
- One policy: `live/policy.json`, with a generation (`supercomputer policy` shows it, `supercomputer policy k=v ...`
  sets knobs: floor, floor_default, floor_idle_s, floor_by_image (`floor_by_image=img=0,img2=1`), max_vms (unset: no
  limit), hold_s, gate_horizon_s, start_dollars, vm_hour_dollars, idle_s, ttl_s, bringup, sub_vms, sub_preempt_s, boat_tries). The scheduler reads it every tick: VMs already up and items already running follow a
  change within a tick, no restart (`status` shows any VM or item on an older generation).
- One scheduler, the newest: a lock per pool; a scheduler that finds a new code hash installed (stable 60 s, and
  importing) hands off atomically: it stops, writes what it holds (`live/scheduler/handoff.json`), gives the port and
  the lock to its spawn and waits; the new one takes every VM in one write of the registry before it serves
  (rules.take_over) and says what it took (`handoff.ack`); the old one exits only when the new one holds every VM it
  held, less those boat reports gone (rules.handoff_done), else it keeps them and hands them to its own code's kept
  copy, the new code rejected. Before any worker starts, the native canary also requires the candidate's live batch
  IDs to equal the handoff's complete batch manifest minus client-close receipts (`boat-starts.jsonl`). Old snapshots
  without a manifest use admission filenames plus held-run IDs, independently of whether decoding succeeds. An
  unreadable admission remains owned/quarantined: no kill, result ACK, start, close or VM release is permitted until
  recovery is safe. `status` exposes `recovery` / `recovery_blocked`; `canary.json` and `handoff.ack` contain the ID-level
  verdict. Guard-aware parents own a failed-ACK rollback; a legacy parent leaves hand-back to the native canary,
  never both. A restart with no handoff (a kill -9) takes the dead scheduler's leases the same way.
  Legacy `null` placement markers from interrupted starting-item handoffs are ignored, never written again; strict
  owned journals still fail closed on null/corrupt records. Starting-item snapshots retain their placement identity.
  While its native canary is pending, the candidate retains idle/above-floor VMs and does not expire filler holds.
  Voluntary retirement resumes only after the native verdict, not merely after 600 seconds. Real-work preemption,
  provider-loss recovery, explicit retirement and disk replacement are unchanged. In-flight releases are fenced
  from adoption until their registry update finishes, without being counted as incoming capacity.
  A tick and two after a switch, the canary checks the VMs carried over, orphans and slots idle beside waiting real
  work (health's `handoff`, want 0) and hands back at once on any (11-01 14:33: 43 of 225 VMs held, 176 with no live
  owner for 3 minutes while 1,247 real items waited, the old re-attach taking VMs back one by one).
- No VM without a live owner: adoption at every start and every minute takes in any up VM of the pool's images no
  live client holds while its image has queued work (or it's within its hold), and stops it otherwise.
- The pool rule: with work queued no VM is released; a VM whose agent doesn't answer is repaired in place (the agent
  restarted by boat exec with its token, its items running on, in sessions of their own); "lost" only when boat
  reports the VM stopped or gone. An item is never retried on a VM that failed it, and one VM is one strike.
- Agents follow the code: each reports its code hash; a stale one is upgraded in place between items.
- State on disk written whole then renamed (batches, registry, policy, accounts); appended rows and placements
  tolerate a torn last line. A `kill -9` loses nothing: the next scheduler takes everything back.

**What it reports.** `supercomputer status` (and `scheduler status`): per class the queue, VMs held, waits
p50/p95, starts free/paid, preemptions, boat's failures and items sent to Modal; the pool against its target, starts
per 100 items and items placed on VMs already up. The digest adds the day's VM-hours and dollars (VM-hours at the
$0.036 overage price, nothing billed while prepaid hours last; extra starts from the wallet and the invoice), the
VM-hours by class, the filler hours VMs above the floor ran during holds, how often MAX_VMS held growth, and the gaps between waves of real work (to tune HOLD).
Information only: no spend thresholds ("no limits, but be smart with our money").

A repo whose checkout has no `supercomputer.toml` yet is read from `repos/<main checkout's name>.toml` here.

**Held to its laws.** The decisions above, and every other the tool makes, are `supercomputer/rules/`'s (an item's fate, the pool's starts and breaker,
a VM's owner, hold and release, the policy generation), each class with its laws as `# gdp: law` comments ("an item goes
to Modal only after boat_tries boat failures", "a wait is no boat failure unless boat failed a start of its image during
it", "an item's boat failures are on distinct VMs", "no VM is up without an owner after a tick", "a VM running items is
never released", "no start while the breaker is open (but one probe)", ...). A policy change runs, before it lands:

    tests/check_laws.sh        # every law proven and checked from every state the laws allow (gdp-wired), every broken variant refuted with a run
    python3 tests/test_wiring.py   # scheduler.py's decisions through the rules, today's incidents replayed (no boat call); test_boat_wiring, test_ops_wiring, test_integrity_wiring likewise

Growth admission is in `rules/growth.py`: a start needs a full marginal VM workload, no economic start when
held/coming capacity clears the queue, and a gate capacity deficit requests a start. `tests/broken_growth.py`
keeps both fixed-horizon mistakes (900 seconds starts needlessly; 3,600 seconds withholds a deadline start).
The existing `Pool.grow` laws enforce the bring-up cap and breaker independently: a gate's requested starts are
admitted when there is room and the breaker allows them. Arithmetic over measured prices and variable division
is outside these linear admission proofs; `tests/test_growth.py` checks the floor/ceiling boundaries and actual
scheduler wiring. `gate_68_slot_hours` and `steady_small_submissions` replay full scheduler/client runs. The former also requires
every newly created VM to deliver real gate work; it does not use a no-cap VM-count bound against staggered starts.

`SC_LAWS_OUT=/path/to/receipts tests/check_laws.sh` keeps every proof/refutation receipt.
`CHECK_LAWS_RESULTS_ONLY=1 SC_LAWS_OUT=/path/to/receipts tests/check_laws.sh` revalidates saved receipts without
running any prover. Missing/empty module receipts fail, as do unknown laws or refutations without a run.

`check_laws.sh` uses `npx gdp-wired-cli` (a release after 0.42.0: sets, Literal aliases, laws over ignored arguments, from any state, laws of eventually),
or `GDP_WIRED_CLI="bun <gdp-wired>/core/cli.ts"`. A law that no longer holds comes back broken with the run that breaks
it; `tests/broken_rules.py` keeps the bugs the laws were written for (before acac7d0, the 21:40 strikes, the orphans and
re-attach before 95ec41b, churn), each asserted refuted.

## Where the scheduler runs: your machine

A configured `live/controller.json` selects the controller by default; `SUPERCOMPUTER_LOCAL=1` explicitly
selects a local owner. Without a controller, clients use this machine's scheduler. After a
sleep it notices the time jump, renews every VM's TTL, adopts any VM left without an owner, and lets go VMs boat
stopped meanwhile with no strike to their items.

Running the scheduler on a boat VM instead (`controller.py`, `supercomputer controller ...`) was tried and is off by
default: a small VM overloaded, and the local scheduler proved simpler. What it taught is in the policy above: one
scheduler per pool, an agent daemon that ignores SIGPIPE, a breaker on starts, bounded bring-up, adoption.

A client with a configured controller (`live/controller.json`, or an explicit `SUPERCOMPUTER_CONTROLLER=1`) never falls back
to this machine's scheduler when the controller doesn't answer: it stops with the reason. A second scheduler beside
the controller adopts its VMs, and one inside a job VM (which has no boat login, so `boat forward` can't come up)
stops its own host. Inside a boat VM, reach the controller over its http transport. Only `SUPERCOMPUTER_LOCAL=1` explicitly permits a local scheduler. `SUPERCOMPUTER_CONTROLLER=0` and the
legacy `SUPERCOMPUTER_CONTROLLER_FALLBACK=1` do not override a configured controller. A long-lived local client
whose scheduler disappears never resurrects it while a controller configuration exists, even if unreadable.

## Exact CPU model placement

A native job can pin hardware with `"cpu_model": "AMD Ryzen 9 9950X 16-Core Processor"`
and `"vm_type": "xlarge"` (or a batch's explicit `--vm-type`). Matching is exact and case-sensitive against
`vtick.model`, never a provider label. Blank/unknown agent models are ineligible. The constraint participates in
slot selection, reservations, preemption and the last placement check; it never falls back to a different model
or to Modal. The native client requires `/capabilities.cpu_model_exact == true` before submitting it and refuses
the legacy scheduler-disabled path. Jobs without the field retain their existing placement/growth rules.

Boat cannot choose the CPU behind a new VM. The same break-even and gate calculations apply to constrained
work, crediting matching held capacity and outstanding draws, not mismatches. Starts remain under the normal
bring-up cap, breaker, pacing and watchdog. Mismatched draws remain ordinary pool VMs: they can run ordinary
work or filler and retire under the existing lifecycle; there is no special stop or spend budget.

A job can set `"cpu_model_max_fruitless": 64` to override the policy's default. Mixed limits for the same
constraint share observed draws; an exhausted caller is reported while a caller with a higher limit may continue.
The report includes that item's effective limit and the model distribution across **all** draws.

`cpu_model_max_fruitless` (default 12, positive; environment default
`SUPERCOMPUTER_CPU_MODEL_MAX_FRUITLESS`) limits consecutive fruitless starts **per image/type/model**.
Unresolved draws occupy the remaining guard room. A known matching draw resets the streak; a failed/blank or
mismatched draw increments it in observation order. The streak and model distribution survive restart/handoff.
At exhaustion, items without eligible matching capacity get a terminal `unplaceable_by_constraint: true` row,
error `unplaceable-by-constraint`, and a `constraint` summary (models observed, draws, matching VMs, waiting items).
It is not a `lost`/Modal retry. Work with matching busy capacity can still wait for it. Raising the load guard
explicitly permits further attempts; submitting a fresh batch does not reset it.

`status.constraints` and the digest show each constraint's matching VM IDs/count, waiting items, draw IDs/models,
and fruitless streak/limit. `status.vms[].model` also exposes unknown agents. Timing still means exclusive use
while running, **not** gang scheduling. For distinct workers across sequential jobs, add a nonblank caller-chosen
`"distinct_vm_group":"run/stage/case/rep"`. Each placement consumes its VM ID for that group for the entire
batch, including completed, preempted or failed placements. Other groups/batches may still use that VM.
The placement lock and an fsynced write-ahead journal make the consumption atomic; replay includes completed
placements. Used VMs contribute no capacity to that group's normal break-even/gate growth calculation.
This composes with exact CPU matching and never falls back to an unconstrained backend. Clients require the
`distinct_vm_group` capability and `distinct_vm_group_protocol: 1`.
`SUPERCOMPUTER_CONTROLLER=1 supercomputer scheduler capabilities` reads the live controller JSON without
starting a scheduler; require `cpu_model_exact: true`, `distinct_vm_group: true` and protocol `1` for combined
work. Native submissions include `placement_contract: 1`; grouped admissions from old clients without it
are rejected before dispatch. Jobs start as soon as their own VM is ready; no global start barrier.
Receipts include `placement.distinct_vm_group`, `placement.vm` and `placement.boot_id` (when the agent's
lease exposes it). Hot-handoff snapshots and ACKs carry the complete per-batch group history; a missing or
different history refuses commit before the old scheduler exits.

## The terms (boat's TOS-6 and TOS-10)

VMs stay busy or are released: a released VM stops 60 s later; VMs above the floor run filler for at most 45 minutes
after their last real work; never parked idle to save starts, but for one deliberate exception: an image with no
treadmill keeps one VM (the default floor) for up to 45 min after its last real work (the hold), one per active project, so its
bursts don't churn starts ("Shrink, damped"). No metering tricks, one account. Free starts are spent
first, leaving a reserve for the account's other projects, then paid ones (every class). Every batch's starts go to
`live/boat-starts.jsonl` (`supercomputer starts`). Never stop or delete VMs another client or project leased;
`supercomputer reap` touches only dead clients' leases, VMs left up past their time, and orphaned Modal apps and pods.

