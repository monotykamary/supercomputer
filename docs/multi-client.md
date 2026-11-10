# Multi-client scheduling

A design, not yet built: how several machines (the Mac, a boat sandbox, the controller VM) each run `supercomputer`
against one boat.dev account without taking each other's VMs or spending each other's starts. The short version:
a VM's own agent grants it to one owner at a time, with a fencing number, and everything else the schedulers share
is bookkeeping that merges. Three stages; stage 1 alone makes two schedulers safe.

## What two schedulers do today

Everything a scheduler knows is local: the registry is a JSON file under a local `flock` (`boat.py:538-562`), the pool
lock is another (`scheduler.py:3786-3797`), and the agents' URLs and tokens are files in `live/agents/`
(`boat.py:1057-1078`). Two machines running the same repo build the same image name (`boat.py:353-364`: the repo's
prefix plus a hash of its layer scripts), so each sees the other's VMs as its own image's.

- **Adoption takes them.** `_adopt` (`scheduler.py:3570-3629`) walks every running VM of its images that it doesn't
  hold. One in no registry gets a fresh entry with no lease and `last_used` now (`scheduler.py:3591-3592`), so
  `VMLife.adopt_tick` adopts it on first sight, within its hold (`rules/scheduler.py:312`). A VM this machine once
  released (`released` set, `scheduler.py:3600`) and the other now uses is stopped once its hold is over
  (`scheduler.py:3611-3622`).
- **The agent's token flips.** Bringing up an adopted VM looks for an agent file here (`scheduler.py:2544`), finds
  none, mints a token (`scheduler.py:2552`) and bootstraps the agent with it; `start_daemon` replaces a daemon whose
  token differs (`agent.py:2051-2071`, `Access.keep_daemon` at `agent.py:323`), and the daemon knows one token only
  (`agent.py:1899`, `1922`). The first machine's ticks now fail; after two it repairs the agent in place with its own
  token (`scheduler.py:2755`, `_repair` at `2629-2643`), which locks the second one out. They alternate. `_check_agent`
  (`scheduler.py:3631-3652`) also rewrites any agent of another version, so two installs flip its code too.
- **Foreign runs are killed.** A run on a VM whose batch this scheduler doesn't know is killed as stale
  (`_foreign`, `scheduler.py:2831` and `2840-2845`). The only exemption, `_unclassified` (`scheduler.py:545-555`),
  covers batches with an admission file on this machine. The other machine's items are preempted, re-queued, killed
  again. Strict items survive, because the agent refuses a legacy kill for them (`agent.py:1523-1531`).
- **Releases stop the other's work.** `_let_go` cuts the TTL to 60 s or stops the VM (`scheduler.py:3158-3192`);
  only the holder's heartbeat extends it again, every 300 s (`scheduler.py:3654-3659`), so a VM one scheduler
  releases can stop under the other's items.
- **Starts are miscounted, not unlimited.** Each machine reads boat's account-wide counters every 5 s
  (`StartGate.take`, `boat.py:1346-1352`), and boat refuses starts past its own limits (`scheduler.py:2539`). Neither
  can pass boat's limits. Two things do go wrong. "Ours" is this machine's registry only (`boat.py:229-233`), so
  `free_left` (`rules/boat.py:224-229`) counts the other machine's starts as another project's and reserves twice
  them: too cautious. And two gates read the same remaining count and each grants its pace of it, so together they
  can eat into the reserve by up to one machine's grants per 5 s read.
- **The breaker is per machine** (`scheduler.py:440`, `3194-3227`). A looping start failure trips each one
  separately, after at least `BREAK_MIN` (4) starts of its own.

The controller avoids all this by being the only scheduler, but it is off by default (`scheduler.py:4196-4204`), and
`connect()` falls back to a local scheduler when it doesn't answer (`scheduler.py:4207-4227`). That fallback is
exactly the two-scheduler case.

## What needs to be strong and what can merge

| State | Consistency it needs | Where it lives |
| --- | --- | --- |
| Who holds a VM, which items may start there | linearizable, per VM | the VM's agent, under one `flock` |
| A start of a VM (free starts) | each client exact about its own; no fresh view of others needed | escrow: rights per client, in the shared state |
| Breaker trips | monotone, prompt | shared state: a set of trips |
| Starts ledger | eventual | shared state: per-client logs |
| Policy | eventual, per knob | shared state: last-writer-wins per knob |
| Durations, estimates | eventual | shared state: last-writer-wins per key |
| Demand (what each client has queued) | eventual, fresh for efficiency only | shared state, one slice per client |
| Planning (packing, floors, gate horizon) | none for safety | an optional planner lease per image |

"Shared state" is one document per client, written by that client only, merged by taking each slice's latest
version. Stage 2 picks how it travels.

## Where the starting sketch was wrong

- **Durations don't merge monotonically.** The index keeps each job's last time (`boat.py:2699`) and the scheduler's
  estimate is a moving average (`scheduler.py:2252-2255`). Taking the maximum would never let a job get faster. They
  merge last-writer-wins by a logical version instead.
- **A shared queue can't be claimed through VM leases.** A VM lease stops two owners using one VM. It does not stop
  two clients starting the same item on two VMs they each hold. Stage 3 shares demand and moves VMs, not items.
- **The budget isn't ours to count.** Boat's counters are the authority and boat enforces the hard limits. What the
  escrow protects is the reserve for the account's other projects, and the allowance changes as those projects
  start VMs. The bound in the laws is stated against that.
- **Not all of the policy is shared.** `bringup` and the watchdog guard one host's own load. They stay local.
- **The registry isn't the authority.** It stays as each machine's cache. Who holds a VM is the agent's answer.

## Stage 1: the VM's agent grants leases

**Implementation decisions (2026-11-10).** Stage 1 uses a 180 s lease. Reclaim never kills a previous
holder's running items: a competing claim freezes new placement by the lapsed holder and waits for the
VM to drain (or its existing TTL to expire) before granting. Done rows remain readable by their owner.
This drain rule supersedes kill-on-claim wording in the original sketch below. Mixed rollout uses
`<image>~l`: old adoption sees no exact image-name match; an old client explicitly resuming a VM from
its own registry wins, and the new client relinquishes it without stopping or fighting its agent.

**The lease.** Each VM keeps `~/.sc/lease.json` under `flock(~/.sc/lease.lock)`. It holds the holder (an owner
id), the fencing pair (boot, epoch), when it was last renewed, and a state: held, released or stopping. Boot is the
kernel's `boot_id`, which the agent already reads (`agent.py:1900`). Epoch is a counter within that boot, raised by
every grant. Renewal times are `CLOCK_BOOTTIME` seconds on the VM itself; no other machine's clock is ever read. A
holder that hasn't renewed for `lease_s` (180 s by default, a shared policy knob) is lapsed. A lapsed lease is
claimable by others, but it isn't revoked: if nobody claims it, the holder's next renew still succeeds. The daemon
and a `boat exec python3 ~/.sc/agent.py lease <op>` run the same code under the same lock, so the lease works while
the daemon is dead.

- `claim(owner)` succeeds when the record is from another boot, released, stopping for longer than `STOP_S` (300 s),
  lapsed, or already held by `owner`. A birth claim of another owner within its window (below) blocks it. On
  success it raises the epoch and returns (boot, epoch). Items of any previous owner still alive are fenced: killed,
  or for strict items cancelled through `cancelitem`. Their done rows are kept for that owner.
- `renew`, `release` and `fence_stop` need the current (boot, epoch). Every holder `vtick` renews.
- Starting an item needs the current (boot, epoch), and no live item of another owner on the VM. So does `kill`,
  `killitem`, `cancelitem`, `upgrade` and `start-daemon`. A stale caller gets `{"stale": true}` and never an error that
  looks like a VM fault.
- Each item records its owner. An owner may ack and read its own items' rows and logs at any epoch, so results
  outlive a lost lease. `vtick` returns only the caller's own done rows and tags each running item with its owner.
- `who` is answered on `/health` without a key: holder, lapsed or not, item counts by owner, uptime. `/health` is
  already open (`Access.allows`, `agent.py:319`) and sits behind boat's token-gated URL.

The owner id is one random id per machine (`live/owner`). It isn't a pid, so a handoff between two schedulers on one
machine renews the lease instead of claiming it: no epoch change, no fencing.

**Reaching an agent: a keyring, not one token.** The daemon accepts any token in `~/.sc/keys/` and reads the
directory again when it changes. A client adds its own key through `boat exec` (`agent.py keys add <owner> <token>`),
so the account's own authority is the proof. Nobody ever replaces another client's key, and no secret is shared
between machines, as SKILL.md's "no shared keys" asks. `start_daemon` keeps a live daemon of the same version whatever
the token. That ends the flip above. The controller's `handover_warm`, which moves agent tokens between machines
(`controller.py:229-255`), is no longer needed.

**Bring-up.** `boat new` takes `--file SANDBOX_PATH=LOCAL_FILE` ("written on the sandbox before it reports ready") and
`--name`. The creator writes `~/.sc/birth.json` (its owner id and a nonce) that way. So a VM is claimed from its first
moment, before any agent runs. A birth claim blocks other owners while the VM's uptime is under `BIRTH_S` (600 s, past
`BREAK_READY`'s 480). Only the nonce's owner can turn it into a lease. A stale birth file baked into an image snapshot
blocks others for 10 minutes at most and grants nothing. A resumed VM gets no `--file`. Its old lease belongs to
another boot (or, if boat restores memory, lapses on the VM's own clock), so the resumer claims it with the bootstrap's
`boat exec`. Others leave alone any VM without an answering agent until it has stayed that way across adoption sweeps
`BREAK_READY` apart, timed on their own monotonic clock. If two clients resume one stopped VM, the lock decides and
the loser has spent a start. That race exists only for VMs that two machines' registries both list as stopped. Stage 2's
shared registry hint closes it.

**Adoption asks the agent.** For each running VM of its images that it doesn't hold, a scheduler reads `who`:

- a live holder: not ours. Never adopted, never stopped, never counted as an orphan;
- released, lapsed, another boot's record, or no answer past the grace above: claimed through `boat exec` if its image
  has queued real work here, else left alone;
- a VM nobody holds is never stopped by a non-holder. Its TTL stops it, within `BACKSTOP` (900 s, `boat.py:69`),
  because only a holder extends it. "When in doubt, let the TTL do it": at most 15 minutes of a VM, never a start.

`adopt_tick`'s law "no VM is up without an owner after a tick" becomes "no VM is up past its TTL without an owner".

**A dead agent.** A holder whose daemon stops answering repairs it as today, but through the exec path. That checks the
lease under the lock first, so it never re-bootstraps over another owner. The holder renews through exec while HTTP
fails, at most once per `lease_s / 3`. Others claim a lapsed holder's VM only when they reach its daemon over HTTP
themselves. The point is the 10-31 case, where boat's front failed every agent at once while `boat exec` still worked.
Without that rule, every holder would lose every VM to the first client whose exec went through. A VM no one can reach
by either path stops at its TTL, and `rules.vm_lost` still declares it lost only when boat says so.

**Stops and deletes are fenced.** Boat has no conditional stop, so a stale holder could stop a VM that someone else
now holds. Before `boat stop` or `boat delete` (release with stop, lost, retire, disk replace, `reap`, `teardown`), a
client calls `fence_stop` under its current epoch. That marks the VM stopping, and claims are refused for `STOP_S`. A
refused fence means the VM isn't ours: it's dropped from `self.vms` and nothing is stopped. Extending a TTL needs no
fence, because only holders do it and a stale extend only keeps a VM up longer.

**In the scheduler.** Every agent call carries (boot, epoch). A stale answer drops the VM without stopping it. Its items
of ours go back to the queue with no strike, as `ItemFate.vm_lost` already does for a VM boat stopped during a sleep
(`rules/scheduler.py:122`). `_foreign` counts other owners' runs as taken slots and never kills them. Only our own
stale runs are killed, and only on a VM we hold. `_check_agent` upgrades only agents it holds, so an agent's code
follows its holder. The registry, its lock and `ours()` stay local. The batch-held path (`SUPERCOMPUTER_SCHEDULER=0`,
`Session`, `boat.py:1400-2417`) claims the same way, as an owner of its own.

**Rolling it out with old clients.** Old code can't be fenced: its bootstrap writes `agent.py` unconditionally
(`boat.py:1085-1195`), and its adoption keys on the sandbox's name. So:

- new clients name new VMs `<image>~l`. Old adoption matches the name exactly (`scheduler.py:3589`) and never sees
  them. New code strips the suffix to get the image;
- on handoff to new code, a machine claims the VMs it held, installs the new agent and renames each one with
  `boat rename` (not atomic, and it doesn't need to be). New code never adopts an un-suffixed VM;
- an old client can still resume or lease a VM in its own registry and overwrite the agent. The new holder sees the
  lease protocol gone (`lease` answers 400, or the version changes under it), gives the VM up without stopping it,
  and raises an alert naming the VM. The old client wins every contested VM, with no flip-flop and no kill war;
- old and new clients are then as unsafe as two old clients today, but only on VMs the old one already had in its
  registry. Upgrade every machine together (`uv tool install` on each), and the mix lasts minutes.

## Stage 2: the ledger, the escrow and the shared bookkeeping

**The medium: a small private git repo.** Each client writes only `clients/<owner>/` (its ledger rows, rights,
transfers, breaker trips, demand, policy writes, durations) and pushes every 15 s when something changed. Since
nobody writes another client's files, a rebase after a rejected push never conflicts. Merging is reading every
slice. Why git over the alternatives:

- `supercomputer store` writes named snapshots. Each push takes a VM start and a serve takes another
  (`store.py:100-133`, `172-188`). It would spend the resource being budgeted, and snapshots are immutable.
- gossip through the agents needs a VM up. The budget matters most when the pool is empty and the first start is
  being decided, and agents die with their VMs.
- `boat env` versions and sandbox names are writable but not conditional, and webhooks only notify an HTTPS endpoint.
  None gives more than git does.
- an object store with conditional writes (R2, S3) would give a real compare-and-set and make the escrow a plain
  counter. But it's a new account and its credentials on every machine, and when unreachable it allows no start at
  all. The escrow keeps each client working on its own rights.

Git works with no VM up and offline, costs no start, keeps its history as the audit trail, and the boat sandbox
already carries git credentials if its environment injects them. Latency of a sync or two (15-30 s) only slows
rebalancing and the demand view. No safety property waits on it. Old rows compact into per-day files.

**The starts ledger** is a grow-only set per client: rows `(owner, seq, ...)`. `supercomputer starts` reads the
union. "Ours" in the reserve becomes all clients' starts, which fixes the double reserve. Counting by window uses each
row's writer clock, but only for accounting: a skewed clock moves a start across a window edge in a report, nothing
more. `merged_starts` (`rules/ops.py:106-111`) already states the law for one timestamp; per client it is a union.

**The escrow.** For a window w (boat's hour and day), the allowance for all our clients together is
`A_w = limit_w - others_w - max(reserve_w, 2 * others_w)`. Here `others_w` is the account's use minus the merged
ledger's. That is today's `free_left` with ours meaning every client's. A stale ledger overstates `others`, so a stale
`A` errs low. Each client i holds a share `q_i`, with all shares summing to at most 1. It may start a free VM only while
its own starts in the window, counted exactly on its own monotonic clock, stay under `q_i * A_w`. A client gives
unspent share to another by writing a transfer into its own slice. It may give only what it hasn't spent in the
current window (`d <= q_i - s_i,w / A_w`, for both windows). The receiver uses a transfer once it has seen it. So no
transfer creates rights, and spending needs no fresh view of anyone. Rebalancing follows the demand each client
publishes: a client with no queued real work for `hold_s` gives its share above a small floor to the clients that
have demand, in proportion to it. Getting share back is a request that the holders answer the same way. Paid starts
aren't escrowed (the user's "no limits"). Boat's wallet limits bound them, and the shared breaker guards against loops.

Reclaim is the one place where time matters. A client that is asleep can't give its share away. If client j has seen
i's slice unchanged for `L` (30 min) on j's own clock, it may write a reclaim that moves i's share above the floor to
itself. To keep that safe, i stops spending anything above its floor once its own last successful push is older than
`L / 2` on its own clock, and after a wake it spends nothing above its floor until it has pulled once. If the two
clocks run at roughly the same rate (skew doesn't matter, only rate), the reclaim lands after i has stopped. If not,
the overspend is at most the reclaimed share. Without reclaim, a sleeping laptop's share sits idle.

Boat's windows may be rolling or calendar-aligned. Check which before building. The scheme holds either way, but each
client's own count has to use the same kind of window.

**The breaker** becomes a set of trips. Each client adds its own (reason and pause), honors any trip it has seen for
that trip's pause from the moment it first saw it (its own clock), and probes alone after the pause. A loop on two
machines stops at the first trip either one makes, plus whatever the other started within one sync.

**Policy** is a map of knobs, each a register `(version, owner, value)`. A write sets its version to one more than the
highest it has seen. On merge the larger `(version, owner)` wins, a total order that reads no wall clock. The
generation `status` shows becomes the largest version. A change lands within a sync instead of within a tick. `bringup`
and `watchdog` stay per machine.

**Durations and estimates** are registers per key, `(version, owner, seconds)`, with the same rule. A job that got
faster gets faster everywhere. The class medians (`class_obs`) and bring-up time stay local estimators.

**The registry hint.** Each client publishes the VMs it holds and the stopped ones it last held, as a register per VM.
A client resumes a stopped VM only if it holds the latest register for it. That closes stage 1's double-resume race in
nearly every case, and the agent's lock covers the rest.

## Stage 3: shared demand, moving VMs

Items stay with the client that admitted them: its queue, its admission files, its results. What is shared is
**demand**: each client's slice says what it has queued per image and class, how long the oldest item has waited, and
which VMs it holds idle or on filler or sweeps. VMs move between clients; items don't.

- **Yield.** A holder whose VM runs only filler or sweeps (or nothing) gives it up when another client has queued work
  of a higher class for that image, waiting past `sub_preempt_s`, and no free VM of its own. It kills its lower items
  (re-queued, as `victims` allows), releases the lease, and the waiting client claims the VM. That is today's
  preemption order (`rules/scheduler.py:830-842`), applied across clients. It takes a sync, a release, a claim and a
  tree: tens of seconds, against seconds on one machine.
- **Growth counts the others' idle VMs.** A client's deficit subtracts the VMs other clients advertise as idle for its
  image before it starts any. An idle VM is released to it (yield), never started beside.
- **The planner lease (optional).** For each image, one client may hold a short lease in the shared state: its record
  is renewed every sync, and if two are fresh, the lowest owner id wins. The planner computes what one scheduler sees
  today, over every client's demand: warm packing, the 15-minute gate horizon, one default floor per image across all
  machines instead of one per machine, and which client's VM should yield. It publishes that as advice. Advice is
  followed only where the follower's own rules allow it (it may release anything, but starts only with its own
  rights), and ignored when stale. Two planners, or none, cost efficiency, never safety.

Moving items between clients would need claims on items, and the only atomic places are the VMs. If that is wanted
later, it is a delegation the admitting client writes ("item X may be started by Y"). The admitting client then never
starts X itself, and the results go back to it through the agent's per-owner reads.

## The controller and the local fallback

- **Stage 1:** the controller is one more owner. The local fallback is safe now, because the controller and the
  laptop fence each other at the agents. `handover` becomes a release by one owner and a claim by the other, so the
  registry surgery and its `HandoverVM` law (`rules/ops.py:67-95`) go. The controller is still worth keeping for what
  it was built for: running while the laptop sleeps.
- **Stage 2:** the controller has a slice like any client. Since it is always on, it's the natural receiver of a
  sleeping laptop's reclaimed share. A local fallback spends only its own rights.
- **Stage 3:** the controller is the default planner. Any client can take the lease when the controller doesn't renew
  it. The fallback is simply planning alone.

## Laws to prove

Written as `rules/` classes would carry them. Each is a sentence and a sketch of its rule: the state, the steps, the
law.

**L1. No VM runs items of two owners at once.** Rule `VMLease` (agent side). State: `holder`, `boot`, `epoch`,
`mine` (live items of the holder), `foreign` (live items of other owners), `lapsed`, `released`. Steps:
`claim(by, lapsed, released)`, `start(by, boot, epoch) -> bool`, `ended(foreign: bool)`, `rebooted()`.

    # gdp: law no VM runs items of two owners at once: foreign == 0 or mine == 0
    # gdp: law a claim takes a VM only from nobody, itself, or a holder released or lapsed: not result or old(holder) == "" or old(holder) == by or old(released) or old(lapsed)
    # gdp: law every grant raises the epoch of its boot: not result or boot != old(boot) or epoch > old(epoch)

`claim` by another owner moves `mine` into `foreign` (fenced, being killed). `start` refuses while `foreign > 0`.

**L2. A stale epoch's item is never accepted.** On `VMLease.start`:

    # gdp: law a stale epoch's item is never accepted: not result or (by == holder and b == boot and e == epoch)
    # gdp: law the epoch never falls within a boot: boot != old(boot) or epoch >= old(epoch)

And on the client, rule `Fence`: state `fenced`, `stopped`; steps `fence(accepted)`, `stop()`:

    # gdp: law a client stops or deletes a VM only after its fence was accepted: not stopped or fenced

**L3. Our starts stay within the allowance, plus a stated bound.** Rule `Escrow`: two clients a and b, rights `ra`,
`rb` (starts, for one window and one allowance), spent `sa`, `sb`, `budget`, `reclaimed` (rights moved from a client
that went on spending without seeing it). Steps: `spend(who) -> bool`, `give(frm, to, k) -> bool`,
`reclaim(frm, to, seen_spent)`, `spend_unaware(who)` (the reclaimed client spending its old view).

    # gdp: law the sum of starts never exceeds the budget plus what was reclaimed from a silent client: sa + sb <= budget + reclaimed
    # gdp: law with no reclaim the sum of starts never exceeds the budget: reclaimed > 0 or sa + sb <= budget
    # gdp: law a transfer conserves rights: ra + rb == old(ra) + old(rb)
    # gdp: law a client gives only rights it has not spent: not result or frm != "a" or k <= old(ra) - old(sa)

In prose, for the real case: the sum over all clients of starts in a window is at most the largest allowance any of
them read in it, plus the reclaimed share. Under clocks of nearly equal rate, that share is 0. The allowance itself is
`free_left`'s, so the existing reserve laws (`rules/boat.py:222-223`) carry over with ours meaning every client's.

**L4. Every queued item is eventually placed or reported.** On `ItemFate` at the item's own client, extending
`rules/scheduler.py:51`:

    # gdp: law a queued item is eventually placed or handed back: state == "queued" leads to state != "queued" by boat_tries - fails via offered(vm_free = true); waited(start_failed = true)

The fairness it assumes, stated in the rule's docstring: (F1) the item's client steps infinitely often (a laptop
asleep for ever owes nothing); (F2) every holder of a VM of the image releases it when it has no real work for it
(`VMQuiet`, `rules/scheduler.py:393`) or lapses; (F3) a holder yields a VM running lower-class work to an older waiter
of a higher class (stage 3), or the class aging `rules.aged` raises the waiter; (F4) a start asked for comes up or
fails, and a failure strikes the item (`waited`, `rules/scheduler.py:130-131`). What is excluded: higher-class work of
other clients holding every VM of the image for ever while no start is allowed. Under stage 1 alone, F3 doesn't hold
across clients, and the law holds per client: other clients' VMs count as absent.

**L5. Merges lose nothing and agree.** Rule `Slice` (per client, per key): state `version`, `owner`, `value`; step
`merge(v2, o2, x2)`:

    # gdp: law a merge never moves a register back: version > old(version) or (version == old(version) and owner >= old(owner))
    # gdp: law a merge keeps the larger of the two writes: version > v2 or (version == v2 and owner >= o2)

and for the ledger, per client, `merged_starts`' laws as they stand.

**L6. A trip seen anywhere stops starts here.** Rule `SharedBreaker`: state `seen_until` (own clock), `probing`; steps
`saw_trip(now, pause)`, `grow(now) -> int`:

    # gdp: law no start while a trip seen is within its pause (but one probe): result == 0 or now >= seen_until or (probing and result <= 1)

## Failure cases

- **A partition between clients** (no git): each keeps spending its own rights and its floor. VM safety is unaffected,
  since claims go to the agents. Demand and policy go stale, and stage 3 degrades to stage 1.
- **A partition between a client and its VMs** (boat's front down): its leases lapse unless the exec renew gets
  through. Others can't claim them except over HTTP to the same agents. If the front is down for them too, nobody
  claims, and the breaker's outage rule (`rules.outage`) pauses starts as today.
- **A laptop asleep for hours:** its leases lapse within `lease_s`. VMs with queued work elsewhere are claimed, which
  fences the laptop's items; the rest stop at their TTL, as they do today without heartbeats. On wake it finds stale
  answers or stopped VMs, re-queues with no strike, pulls the shared state before spending above its floor, and finds
  its share reclaimed if it was asleep past `L`.
- **Clock skew:** no safety decision compares two machines' clocks. Lapses are timed on the VM's own boot clock,
  births on its uptime, the grace before claiming a silent VM on the claimant's monotonic clock, and versions are
  logical. Wall clocks only date ledger rows and bucket reports. The one timing assumption is in reclaim, and it is
  about clock rate, not skew. Breaking it costs at most the reclaimed share.
- **A VM resumed, recovered or forked with an old lease file:** a new boot voids every lease, because the fence is
  (boot, epoch), never the epoch alone. So a counter rolled back by an older disk can't match a stale holder's pair. If
  boat restores memory instead (same boot), lease, clock and items are restored together, and the lease lapses on the
  VM's own clock. A stopping mark expires after `STOP_S`, so it can't lock a resumed VM for good. A fork is a new
  sandbox id: the agent records the id it was claimed as (sent with every claim), and a claim naming another id voids
  the copied record.
- **Images carrying lease files:** handled like a resume (another boot), and a baked birth claim expires with uptime.
- **Paid-start loops like 10-31:** the breaker is shared (L6), so the loop stops at the first trip anywhere.
  `BRINGUP` stays per machine, which matters on a 2-vCPU controller. A creator never loses its new VM to a race: the
  birth claim holds it before boot, so no re-start comes from contention. A claim that keeps failing on a VM we
  created counts as a start not ready, so it feeds the breaker. The worst case left is two machines each making up to
  `BREAK_MIN` failing starts plus one sync's worth before they both stop.

## Open questions

1. **Where the shared state lives.** A new private GitHub repo (under which account), a bare repo on the controller VM,
   or an object store with conditional writes instead of git. And does the boat sandbox's environment give it push
   rights?
2. **Shares.** Default shares per machine (equal, or most of them to the always-on controller or sandbox), the floor
   that can't be reclaimed, and whether reclaim is wanted at all. Without it there's no timing assumption, but a
   sleeping laptop's share sits idle.
3. **Paid starts.** Leave them unbudgeted (the breaker only, as now), or add a shared per-hour paid ceiling as a loop
   guard rather than a budget.
4. **A lapsed holder's running items.** Fence (kill) them when another client claims the VM, as designed, or let them
   drain under the TTL first, which keeps a sleeping laptop's work but leaves VMs held longer.
5. **`lease_s`.** 180 s proposed. Longer keeps a slow or briefly cut-off holder's VMs, shorter frees a sleeping one's
   sooner.
6. **Rollout.** Is "upgrade every machine together, an old client wins contested VMs during the mix" acceptable, or
   should stage 1 wait for a flag on every machine?
7. **The controller.** Keep it as an always-on client (default planner, receiver of reclaimed shares), or retire it now
   that the local fallback is safe.
8. **Stage 3.** Is cross-machine preemption wanted (a gate here kills filler there)? Is one default floor per image
   across all machines (rather than one per machine) the intent? Is moving VMs enough, or is item delegation needed?
