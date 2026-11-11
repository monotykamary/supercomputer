# What supercomputer decides, and where each decision is proven

Every function and method of the tool, by module: `rule` (a call of `supercomputer/rules/`, with the laws it holds; each
law proven and checked by gdp-wired from every state the laws allow, `tests/check_laws.sh`), `I/O` (no decision, with
why), or a decision not in a rule (with why: what the rules' subset can't state). The modules' own tables follow.

## CPU pinning and controller affinity

| def | where | laws, or why |
| --- | --- | --- |
| `rules.cpu.cpu_matches` | rule | exact constrained placement, no unknown CPU for a pin, unconstrained eligibility unchanged; used by the shared fit predicate and final placement guard. |
| `rules.cpu.cpu_draws`, `cpu_streak` | rule | starts never exceed earned demand or remaining consecutive-fruitless room; a match resets the streak, a miss increments it. `_plan` uses the effective per-job guard and credits pending draws. |
| `rules.cpu.cpu_handoff_allowed` | rule | live pinned admissions never hand off to code without exact-model support. |
| `rules.affinity.distinct_vm_allowed`, `distinct_history_matches` | rule | a group never reuses a consumed VM; ungrouped eligibility is unchanged; a grouped handoff requires identical durable history in its ACK. Placement consumption is write-ahead/fsynced under the existing lock, replayed even for completed items. |
| `rules.affinity.local_start_allowed`, `local_recovery_allowed` | rule | controller configuration blocks implicit local startup and any automatic local resurrection; explicit `LOCAL=1` is the only local opt-in with a controller. |
| `Scheduler._distinct_snapshot` | I/O | serializes the batch-scoped consumed VM sets, including completed placements, for the snapshot and its exact-history ACK. |
| `Scheduler._constraint*`, `_model_report` | rule + I/O | dispatch to CPU eligibility/draw/streak rules; persist atomic draw observations, decode agent models, backfill receipts and format status/refusal diagnostics. No provider placement/stop decision. |
| `controller_configured`, `controller`, `prepare_snapshot` | I/O + rule | inspect local configuration and exact candidate capabilities; startup/recovery and handoff eligibility use the rules above. |

## blobcache.py

| def | where | laws, or why |
| --- | --- | --- |
| `collect` | rule + I/O | `rules.blobcache.BlobEviction.remove`: pinned/recent blobs are never selected; removal requires pressure or expiry; eligible cold blobs are selected. Filesystem scan, queue-pin resolution, locking, unlinking and reporting are I/O, covered by wiring and process-lock tests. |
| `locked, key_path, have, put, read, pinned, last_report, main` | I/O | OS locking across clients/admission/collection; SHA-256 checked atomic writes; descriptors opened under lock; queue manifests resolve pins fail-closed; reports and CLI dispatch. |
| `completed` | rule + I/O | `CompletionPin.release`: only a finished job of the matching admission generation releases pins. Results remain on disk; a reused batch ID cannot inherit a completion marker. |
| `Scheduler._blob_cache_tick` | I/O | one maintenance worker at a time, checked every 300 seconds and cancelled during stop/handoff; deletion policy is `collect` above. |

## 2026-11-08 unplaced exclusive gates

| def | where | laws, or why |
| --- | --- | --- |
| `rules.scheduler.usable_slots`, `exclusive_lanes` | rule | no promise of foreign slots; an unserved exclusive batch has a reusable, coming or newly requested lane; warm/preemptible lanes before starts. Used by `_plan`. |
| `rules.boat.resume_replaces` | rule | a confirmed missing VM is terminal; transient errors are not replacements. Used by shared and dedicated bring-up. |
| `boat.resume_missing` | I/O | decode the direct resume response, not absence from a paged inventory. |
| `boat.forget_missing` | I/O + ownership guard | atomically discard a confirmed missing VM and its credentials only under this PID/host/batch lease; never stop/delete a live VM. Covered by `test_unplaced`. |
| `_foreign`, `_tick`, `_up`, `_accept_finished`, `_finished` | I/O | refresh inventory before advertising capacity and while otherwise idle; a held legacy item retains its protocol after its batch closes, so its terminal row releases capacity. Strict identity/reap barriers are unchanged. |
| `health.billing_problems`, `skill._limits` | reporting | provider availability is a health fact; balances are information only, plan and pack both shown. No money threshold. |

## scheduler.py

| def | where | laws, or why |
| --- | --- | --- |
| `log` | I/O | a line to stderr |
| `klass` | I/O | a class's name to its number, the default class from the environment; a lookup, no state |
| `_env` | I/O | a knob from $SUPERCOMPUTER_<KNOB> |
| `policy_defaults` | I/O | the knobs' defaults |
| `read_policy` | I/O | the policy file read (missing: generation 0); the generation's rule is Policy |
| `write_policy` | rule | rules.scheduler.Policy.write: each write of the policy raises its generation by exactly one; a knob the policy doesn't have is refused, the generation unchanged |
| `apply_policy` | rule | rules.scheduler.max_vms_of: a VM with no limit set has none |
| `code_hash` | I/O | the installed modules hashed (by mtime) |
| `jlines` | I/O | a JSON-lines file, a torn last line skipped |
| `vm_slots, vm_mult, pct` | I/O | arithmetic of a VM type and a percentile: no state |
| `Item.__init__` | rule | rules.scheduler.ItemFate (every law of ItemFate): an item's fate is its rule's from the start |
| `Item.fails / avoid / gen` | rule | properties of rules.scheduler.ItemFate |
| `Batch.__init__` | I/O | a batch read from its message |
| `Batch.deliver` | I/O | a row appended (and kept on disk) |
| `Batch.left` | I/O | a count of items not done |
| `Batch.scale` | decision, not in a rule | an estimate's scale (observed / estimated, at least 1): a division of two observed numbers, outside the rules' linear arithmetic; it sizes growth, it decides no state |
| `VMState.__init__` | I/O | a VM's record |
| `VMState.used` | rule (by its use) | the slots taken: a timing item takes all; placement's candidates are rules.scheduler.may_slot's (no item is placed where it doesn't fit; a timing item only on an empty VM) |
| `Scheduler.__init__` | I/O | the state read back, the policy and the code hash |
| `_metrics_new, _metrics_load, _metrics_save` | I/O | the day's accounts, kept across restarts |
| `alert` | I/O | an alert logged and kept |
| `reload` | rule | rules.scheduler.ItemFate.delivered / reloaded; legacy null placement markers ignored, all other undecodable admissions reserved in `recovery_errors` and held without destructive actions |
| `_take_pool`, `Canary.admissions` | rule + I/O | rules.scheduler.recovery_ready: exact missing/extra set differences and unclassified count must all be zero before workers; full batch manifest (legacy fallback: admission filenames plus held-run IDs), reconciled against client-close receipts |
| `_read_client_closes`, `_unclassified` | I/O | only scheduled close receipts with wall/slot accounting; a still-present admission wins over an older close. An undecoded durable ID reserves its runs/results |
| `_handoff_snapshot`, `_handoff_check`, `_handoff_idle_slots` | I/O + rule | batch manifest and placement/run identity survive takeover; native admission verdict precedes any worker/kill/release; one failed-ACK rollback owner; continuous idle duration uses `updates.handoff_idle_slots`, reset by real placement or lost demand |
| `Health.check`, `Canary.tick` (handoff) | I/O + rule | `updates.handoff_sample_fresh` selects disjoint `(previous, now]` intervals; `updates.handoff_health_streak` requires new sustained-idle observations in two intervals, but a fresh missing VM/orphan rejects on its first sample |
| `_reattach` | rule | rules.scheduler.VMLife.reattach (re-attach tries a VM at most twice, then leaves it to adoption; never takes back a VM boat reports stopped) and Reattaching (re-attach ends); its items' failures ItemFate.boat_failed |
| `_life` | rule | the VMLife a decision is made on, from the VM as it is |
| `_reattach_vm` | rule | rules.scheduler.ItemFate.reattached; the lease taken only where free (lease_free, rules.boat) |
| `submit` | I/O | a batch written whole and queued |
| `poll` | I/O | rows since an index |
| `close` | rule | rules.scheduler.ItemFate.delivered: every item of a closed batch done |
| `logs` | I/O | the batch's logs merged |
| `hint` | I/O | a tenant's standing demand recorded with its TTL |
| `floor` | decision, plain | the policy's floor while a live hint names the image, else 0: a lookup; the hold's laws (VMLife.hold_tick) take the floor as given |
| `status` | I/O | a report |
| `run` | rule | rules.scheduler.abandoned: a batch with items left is closed only after orphan_s with no poll; the idle exit a report of nothing held |
| `_policy_tick` | rule | rules.scheduler.ItemFate.follow (an item follows the current policy generation after a tick) and VMLife.hold_tick (a VM follows it) |
| `stale` | I/O | a report of what isn't on the generation or the code |
| `queued` | decision, not in a rule | the queue's order (by class, round-robin by client within a class, then age, longest first): a sort over lists, outside the rules' subset; the proposal for aging (rules.scheduler.aged) ranks within it |
| `schedule` | rule | rules.scheduler.within_share (a submission client never holds more than its share of slots), ItemFate.waited (a wait is no boat failure unless boat failed a start of its image during it), Pool.grow through _plan |
| `_slot` | rule | rules.scheduler.may_slot and ItemFate.avoid: no filler on a draining VM, a timing item only on an empty VM, never a VM that failed it |
| `_place` | rule | rules.scheduler.ItemFate.place: no item placed on a VM boat reports stopped, nor on one in its avoid set; a queued item offered a good VM is placed |
| `_start_on` | rule | rules.scheduler.prepare_failed: an agent that doesn't answer on a VM boat reports up costs the item no strike; a preparation that fails on a VM boat stopped loses the VM |
| `_peer` | I/O | a ready VM that has the tree (a lookup) |
| `_prepare` | rule | rules.scheduler.Preparation: a prepared batch is never prepared again; a failed preparation is not kept (52a8f6b, wired at the rebase) |
| `_victimize, _kill` | I/O | a lower item marked and killed (whose: _preempt_for's) |
| `_preempt_for` | rule | rules.scheduler.victims: a submission's item is never taken for another's; only interactive checks and gates take a sweep's slot; a submission takes a filler's only after sub_preempt_s |
| `est_key, est_of, _learn` | decision, not in a rule | estimates (a moving average, a median, a scale): arithmetic with division of observed values; they size growth, they decide no state |
| `_in_flight` | I/O | a count |
| `_plan` | rule (the starts) / decision, not in a rule (the demand) | rules.scheduler.Pool.grow (starts are bounded by the bring-up cap; starts never take the pool past max_vms; no start while the breaker is open but one probe; one probe at a time); rules.growth.economic_start and gate_start prove marginal-work and deadline admission; gate_window consumes the batch deadline rather than resetting it; economic_vms floors fully funded VM workloads and gate_vms ceilings the deadline deficit (division by measured values boundary-tested in test_growth.py); break_even_s derives the economic window from hold and opportunity prices; 3x bring-up still guards ordinary growth |
| `_ask` | I/O | acquisition off the loop, with a backoff |
| `_acquire` | rule | rules.scheduler.Acquire: the starts of one acquisition never exceed its budget; never more than it wants; warm VMs come before any start |
| `_bring` | I/O | a VM recorded and brought up |
| `_count_start` | I/O | the day's accounts and the starts log |
| `_start_failed` | decision, plain | whether a start of the image failed in the window (refused, or not ready in BREAK_READY): a scan of the starts log (a list) feeding ItemFate.waited's start_failed |
| `_start_done` | I/O | a start's outcome recorded |
| `_up` | I/O | a VM brought up (agent installed, ticked); its failure stops it (_let_go) |
| `_create` | I/O | boat new; the gate (rules.boat.StartGate) decides the count |
| `_boat_up` | I/O | boat's word on a VM |
| `_repair` | I/O | an agent restarted in place |
| `_outage` | rule | rules.scheduler.outage: an outage is at least two ready VMs, half of them or more failing |
| `_worker` | rule | rules.scheduler.vm_lost: a VM boat reports up is never declared lost |
| `_finished` | rule | rules.scheduler.ItemFate.preempted / finished / boat_failed (a lost row) |
| `_boat_failed` | rule | rules.scheduler.ItemFate.boat_failed and its strike: one strike per VM, Modal only after boat_tries |
| `_lost_vm` | rule | rules.scheduler.ItemFate.boat_failed; vm_lost(slept) (a VM boat stopped while this machine slept is no strike: a581e86, wired at the rebase) |
| `_release_idle` | rule | rules.scheduler.voluntary_retirement_allowed: no voluntary idle release or filler expiry while this candidate's native canary is pending; explicit retirement/disk replacement unchanged. rules.scheduler.VMLife.hold_tick and release: a VM within the floor never drains; above it, drains only after hold_s with no real work; no VM released while real work is queued; with filler queued only after its hold; a VM running items is never released; VMQuiet: a VM with no work is eventually released |
| `_let_go` | I/O | the VM's TTL set and its lease let go (whether: _release_idle's rule) |
| `trip` | rule | rules.scheduler.Breaker.trip: a trip while the breaker is open changes nothing; a pause never past backoff_max_s |
| `_breaker_eval` | rule | rules.scheduler.Breaker.judge: the breaker trips on starts only when over half of enough of them failed; a probe that came up after the pause closes it |
| `_breaker` | rule | the Breaker a decision is made on |
| `_watch` | rule | rules.scheduler.Watchdog: sheds only when the controller is loaded, stops only when it's cool |
| `_sampler` | rule | rules.scheduler.Install.tick (a handoff only after the install stood unchanged 60 s and imports), woke (a loop over 60 s is a sleep, at the rebase); the adoption cadence and the accounts are I/O |
| `handoff` | I/O | the pool handed over (whether: Install) |
| `_imports` | I/O | whether the installed code imports |
| `_adopt` | rule + I/O | in-flight release IDs are skipped until their registry update finishes (never counted as incoming capacity); rules.scheduler.VMLife.adopt_tick: no VM is up without an owner after a tick; adoption stops a VM only with its image's queue empty and its hold over |
| `_check_agent` | decision, plain | an agent whose code hash differs from ours is upgraded in place (between items, by _worker's guard): an equality test |
| `_heartbeat` | I/O | TTLs renewed every 5 min |
| `shutdown` | rule | rules.scheduler.keep_across_restart: a restart keeps every VM running items |
| `blob_tar` | I/O | a tar of blobs |
| `store_blobs` | decision, same rule as rules.integrity | a blob kept only where its bytes hash to its key: the law of rules.integrity.BlobStore, here a hash test the rules' subset can't compute (sha256) |
| `raw_post` | decision, same rule as rules.integrity | each blob's bytes must hash to its name: as store_blobs |
| `queue_ls, queue_results` | I/O | the treadmill's queue listed |
| `serve` | I/O | one scheduler per pool (a file lock), the HTTP server |
| `rules.updates.http_client_fault` | rule | exactly HTTP 4xx, never 5xx, is classified as client input failure. |
| `Health.record_client_error`, `Health.client_errors` | observational I/O | bounded metadata and windowed client-fault counts, separate from server-error events and all canary targets. |
| `rules.updates.http_mutation_allowed` | rule | stopped, frozen or passive schedulers refuse mutations; active owners admit them. Evaluated under the same lock that freezes a handoff snapshot. |
| `H.log_message, H._send, H._response, H._post, H._h_client_fault, H.do_GET, H.do_POST, H.handle_error` | I/O + rule | bounded socket reads and response writes outside the mutation lock; dispatch and immutable response preparation inside it; constant-time token check. The freeze decision uses `http_mutation_allowed`; latency is exercised by mock and real-socket tests, not claimed as a timing law. |
| `Client.__init__ .. Client.keep (req, _local, _known, _know, push_tree, push_caches, keep)` | I/O | the client's requests (a kept-alive connection, three tries) |
| `_healthy, persistent_forward, forward_supervisor, bye, forward, write_private` | I/O | the forward to the controller and its supervision |
| `controller, connect, _remote, running, current, ensure_running, enabled` | decision, plain | which scheduler a batch goes to (the controller's when set up and answering, else this machine's, started): an ordered choice over what answers, no state of its own |
| `main` | I/O | the command line |

# boat.py's coverage by rules/boat.py

Every def of supercomputer/boat.py (153): in a rule of supercomputer/rules/boat.py (the code calls it; the laws
named are proven by `tests/check_laws.sh`), a decision not yet in a rule (with why), or I/O with no decision (with why).

- in rules/boat.py: 48
- decisions not yet in a rule: 24
- I/O, no decision: 81

All of rules/boat.py's laws are proven and checked from any state since gdp-wired-laneS ec3654e/ca8d263 (boat-2: a product guarded at 0 or above; boat-3: LONG = IDLE; boat-4: a field never shadowed by another method's argument; boat-5: the largest inductive set from any state).

Broken variants (tests/broken_boat.py, each refuted with a run): pid_alive's host "" (holder_alive_before), teardown of
other repos' free VMs (TeardownOthers), CEIL's minute share as a count (ceiling_before), the 10-minute demand window
(release_ttl_window), refresh_old blind (refresh_count_before), Session.run's wait forever (WaitsForever),
Scheduled.run's dropped items (DropsUnreported), the ledger's undercount of resumes (CountsOnce).

| line | def | coverage |
| --- | --- | --- |
| 109 | `log` | I/O, no decision: a stderr line |
| 124 | `boat` | I/O, no decision: runs the boat CLI (a semaphore of 48 at once); raises on a non-zero exit when asked |
| 132 | `bj` | I/O, no decision: boat … --json, parsed |
| 136 | `exec_` | I/O, no decision: boat exec --json; an unparsable answer made a failed one |
| 144 | `exec_json` | **in rules/boat.py**: `Retry`, laws “never more tries than allowed”; “it stops at the first success”; “a retry allows one try at least”; “a retry still trying has a try left”; “a retry succeeds only by a try that worked” |
| 163 | `stop_vm` | **in rules/boat.py**: `stop_fallback`, laws “a VM boat can't stop is deleted, never left up” |
| 176 | `limits` | I/O, no decision: boat limits |
| 180 | `effective` | **in rules/boat.py**: `wallet_in_force`, laws “the wallet's limits only for an urgent batch while extra starts are on and under their cap”; the arithmetic of the wallet's limits (the plan's times boat's multiplier) is I/O of boat's numbers |
| 199 | `starts_left` | **in rules/boat.py**: `free_left, paid_left, starts_left`, laws “the free starts leave the reserve for the account's other projects”; “the free starts leave twice what the others used”; “paid starts only while extra starts are on and wanted”; “paid starts stay within the wallet's limits”; “the starts left never pass a CEIL ceiling”; “the starts left are at most the free and paid ones” |
| 216 | `ceil_count` | **in rules/boat.py**: `ceiling`, laws “a CEIL share of a limit is a share of it, never the share read as a count”; “a CEIL share never exceeds the limit” (not read yet: gap boat-2, a product of two integers) |
| 221 | `ours` | I/O, no decision: counts the registry's start timestamps per window (the count StartLog keeps) |
| 228 | `note_starts` | **in rules/boat.py**: `StartLog`, laws “a start taken back never leaves fewer than none”; “the count moves by the starts noted, never past none” |
| 244 | `ledger` | I/O, no decision: appends a row to the starts ledger |
| 262 | `client` | I/O, no decision: the client's name from the environment or the checkout |
| 268 | `budget` | **in rules/boat.py**: `budget`, laws “a batch's budget is within the starts left”; “a batch's budget is within its cap”; “a batch gets at least 2 starts while that many are left” (not read yet: gap boat-2) |
| 275 | `sandboxes` | I/O, no decision: boat list --all |
| 282 | `named_snapshots` | I/O, no decision: boat snapshot list, cached |
| 296 | `states` | I/O, no decision: boat list, at most every 3 s |
| 305 | `wait_running` | I/O, no decision: polls boat until the VMs are up and answer exec, or a timeout |
| 330 | `layers` | **decision, not yet in rules/boat.py**: each layer's snapshot name is a hash of the image salt and every script up to it, so editing a layer renames it and every layer above, never below: a property of sha256 chaining the prover's arithmetic doesn't read (hashes aren't numbers it models); held by construction |
| 347 | `image_name` | I/O, no decision: $SUPERCOMPUTER_IMAGE or the top layer's name |
| 357 | `build_layer` | **decision, not yet in rules/boat.py**: a build's sequence on one VM (poll its exit every 20 s, give up past 6600 s, refuse to snapshot when the credential check fails, delete the VM always); each a single condition on I/O results, the credential gate the one that matters; left for a follow-up rule |
| 424 | `ensure_image` | **decision, not yet in rules/boat.py**: bottom-up per layer: an alias ready and the snapshot absent → use the alias; absent or not ready → build (if allowed) else Unavailable; left for a follow-up rule (no cross-client lock: two clients may build one layer) |
| 444 | `images_of` | I/O, no decision: the registry's images of a prefix |
| 450 | `prune_snapshots` | **in rules/boat.py**: `keep_snapshot`, laws “the current image, its predecessor and any image under a day old are never removed” |
| 475 | `retire` | **in rules/boat.py**: `Entry.retire`, laws “retire deletes only free VMs of this repo's superseded images”; “nothing takes another live client's lease” |
| 490 | `pid_alive` | **in rules/boat.py**: `holder_alive`, laws “another host's holder is never presumed dead”; “a holder of this host, or of no host (an old record), whose pid is gone is dead” — **fix**: a record naming no host is this host's |
| 506 | `entry` | I/O, no decision: a registry VM read as rules.boat.Entry (its lease from pid_alive): the snapshot every Entry decision is asked on |
| 516 | `Registry.__init__` | I/O, no decision: the registry's paths |
| 522 | `Registry.locked` | I/O, no decision: flock, read, yield, write whole (tmp + rename) |
| 534 | `Registry.read` | I/O, no decision: the registry, or an empty one |
| 541 | `reconcile` | **in rules/boat.py**: `Entry.reconcile`, laws “boat's VM with a live lease leaves the registry only when boat lost it or its disk expired”; “nothing takes another live client's lease” |
| 569 | `refresh_old` | **in rules/boat.py**: `refresh_count`, laws “refreshing spends at most 4 starts, and never more than are left”; “boat's limits not to be had, nothing is refreshed” — **fix**: none refreshed blind |
| 580 | `refresh_old.one` | I/O, no decision: resume then stop |
| 590 | `lease_free` | **in rules/boat.py**: `Entry.free (holder_alive)`, laws “a VM is leased only while its lease is free” |
| 594 | `demand` | I/O, no decision: records a client's declared work (the window is retired: release_ttl) |
| 602 | `undeclare` | **in rules/boat.py**: `Entry.undeclare`, laws “undeclaring cuts a kept VM to its idle time” |
| 620 | `declared` | **in rules/boat.py**: `holder_alive (through pid_alive)`, laws “a holder of this host, or of no host (an old record), whose pid is gone is dead” |
| 626 | `_ignored` | I/O, no decision: fnmatch over the ignore patterns |
| 634 | `tree_files` | I/O, no decision: git ls-files |
| 639 | `tree_files.ls` | I/O, no decision: its recursion |
| 652 | `tree_tarball` | I/O, no decision: tar of the files |
| 661 | `tree_id` | I/O, no decision: a hash of the files and links |
| 671 | `Tree.__init__` | I/O, no decision: the tree's files and data |
| 701 | `Tree._index` | I/O, no decision: hash memo by size, mtime, inode, exec bit |
| 733 | `Tree.tar` | I/O, no decision: the tarball, lazily |
| 745 | `Tree.blobs` | **in rules/boat.py**: `blob_ok`, laws “a blob ships only under its own digest” |
| 767 | `Tree.read` | I/O, no decision: a file's bytes |
| 779 | `cache_local` | I/O, no decision: a path |
| 783 | `cache_files` | I/O, no decision: local files ∩ declared outputs |
| 789 | `cache_tarball` | I/O, no decision: tar of them |
| 801 | `cache_from_modal` | **decision, not yet in rules/boat.py**: fetched only when the local cache lacks outputs, adopted only when larger: a monotone 'the local cache only grows' rule; left for a follow-up |
| 824 | `agent_file` | I/O, no decision: a path |
| 828 | `read_agent` | I/O, no decision: a JSON file |
| 834 | `write_agent` | I/O, no decision: an atomic 0600 write |
| 843 | `forget_agent` | I/O, no decision: unlink |
| 856 | `AgentClient.__init__` | I/O, no decision: the agent's URL parsed |
| 864 | `AgentClient._get` | I/O, no decision: a pooled connection or a new one |
| 874 | `AgentClient._put_back` | **decision, not yet in rules/boat.py**: keeps at most 8 idle connections: a resource bound with no state law worth stating beyond the constant; left |
| 881 | `AgentClient._req` | **in rules/boat.py**: `Retry`, laws “never more tries than allowed”; “it stops at the first success” |
| 916 | `AgentClient.rpc` | **in rules/boat.py**: `by_exec`, laws “only the warm agent's replayable commands, and small ones, go over boat exec” |
| 933 | `AgentClient._rpc_exec` | I/O, no decision: the request from inside the VM over boat exec |
| 942 | `AgentClient.raw` | I/O, no decision: a POST; non-200 raised |
| 948 | `AgentClient.put` | I/O, no decision: a POST of bytes |
| 954 | `AgentClient.ping` | I/O, no decision: rpc ping |
| 960 | `AgentClient.keep` | **decision, not yet in rules/boat.py**: the cookie persisted only when the recorded token is ours and it changed: a one-line guard; left |
| 967 | `agent_client` | I/O, no decision: a client where an agent is recorded |
| 977 | `StartGate.__init__` | I/O, no decision: its counters, and its rule (rules.boat.Gate) |
| 982 | `StartGate.take` | **in rules/boat.py**: `Gate (+ starts_left, ceiling)`, laws “a grant never exceeds what was asked”; “grants since the limits were read stay within the starts left”; “grants stay within the minute's limit”; “grants are paced, a sixth of what's left a minute”; “grants stay within CEIL's minute” — **fix**: CEIL's minute share is a share of the minute's limit (was read as a count: 0.5 → no start) |
| 1013 | `StartGate.take_split` | **in rules/boat.py**: `paid_of`, laws “the paid part of a grant is within the grant” |
| 1023 | `VM.__init__` | I/O, no decision: a VM's slots from its type |
| 1044 | `Session.__init__` | I/O, no decision: a batch's id, tree, caches, counters |
| 1094 | `Session.__init__.day0` | I/O, no decision: the account's day count |
| 1102 | `Session._image` | **decision, not yet in rules/boat.py**: ensure_image once (a latch): see ensure_image |
| 1111 | `Session.cache_tar` | I/O, no decision: memo |
| 1116 | `Session.cache_names` | I/O, no decision: names |
| 1121 | `Session.ensure` | **in rules/boat.py**: `Ensure (+ Entry.lease_stopped, budget)`, laws “resumes and creations of one ensure stay within the batch's budget”; “no start for a batch that yields”; “never more VMs than asked”; “a VM up is leased before one is resumed or made”; “a batch leases only a VM of its image and type that it doesn't hold”; “a VM is leased only while its lease is free” |
| 1177 | `Session._lease_warm` | **in rules/boat.py**: `Entry.lease_warm`, laws “a warm lease never takes a VM about to stop”; “a VM is leased only while its lease is free” |
| 1197 | `Session.lease_up` | **in rules/boat.py**: `Entry.lease_live`, laws “a VM another batch released joins only while it is up for 20 s more”; “a VM is leased only while its lease is free” |
| 1231 | `Session._go` | I/O, no decision: a thread, tracked |
| 1236 | `Session._bring_up_warm` | **decision, not yet in rules/boat.py**: the warm agent's begin, else the cold bring-up (a fallback on an I/O failure): left for a follow-up |
| 1266 | `Session._prepare_warm` | **decision, not yet in rules/boat.py**: the tree and caches a warm VM lacks, uploaded: a sequence of I/O checks; left |
| 1293 | `Session._bring_up` | **in rules/boat.py**: `Resume`, laws “a resume is tried at most 3 times”; “every start boat refused is given back”; “a VM is skipped only before its first try”; “a resume is tried only with a start granted”; “the ledger counts every resume tried, once the bring-up ends” — **fix**: the ledger counts each try (was once per VM) |
| 1363 | `Session._create` | I/O, no decision: the creating count around _create1 |
| 1371 | `Session._create1` | **in rules/boat.py**: `Creation`, laws “every start boat made no VM for is given back”; “creation stops once boat failed 3 times what it made”; “creation never stops while boat makes VMs” (the wait for boat's room, 10 s a pass, unbounded: left) |
| 1431 | `Session._bring_up_new` | I/O, no decision: waits for a new VM, prepares it; on failure releases it |
| 1445 | `Session.mine` | **decision, not yet in rules/boat.py**: the registry's lease still this batch's: Entry.lease == "ours" in effect; a one-line guard; left |
| 1451 | `Session.enough` | **decision, not yet in rules/boat.py**: the run's queue fits the slots of the session's VMs (item count vs slots): an input of Resume (enough) and Creation; the count itself left |
| 1458 | `Session._prepare` | **decision, not yet in rules/boat.py**: seed and peer logic of a batch's tree (one seed a session; via the seed or direct upload): an I/O sequence; left |
| 1524 | `Session._ready` | I/O, no decision: the VM's CPUs and slots recorded |
| 1532 | `Session._warm_agent` | **decision, not yet in rules/boat.py**: start the agent daemon, record its URL: an I/O sequence with one retry; left |
| 1559 | `Session._agent` | I/O, no decision: exec the agent |
| 1564 | `Session._tick` | **decision, not yet in rules/boat.py**: the warm agent, else boat exec for good (a fallback); left |
| 1575 | `Session._upload` | **in rules/boat.py**: `Retry`, laws “never more tries than allowed”; “it stops at the first success” |
| 1592 | `Session._serve` | I/O, no decision: the seed's HTTP server and URL |
| 1606 | `Session.run` | **in rules/boat.py**: `RunEnd (+ LocalItem)`, laws “a run with no VM in hand, items left and nothing coming up never just waits”; “a run revives at most once”; “a run with no VM coming ends” — **fix**: a run that never got a VM and has no --max-wait ends (was forever) |
| 1626 | `Session.run.take` | **in rules/boat.py**: `LocalItem.take`, laws “an item is taken at most once more than it was lost” (the longest-first fit of a VM's free slots: a selection, left) |
| 1645 | `Session.run.finish` | **in rules/boat.py**: `LocalItem`, laws “an item is finished exactly once” |
| 1661 | `Session.run.lose` | **in rules/boat.py**: `LocalItem.done(lost)`, laws “with no fallback an item runs again on boat at most once”; “with a fallback a lost item goes to it at once, never again on boat”; “an item handed to the session is eventually finished (its VMs keep failing it, or report it)” |
| 1684 | `Session.run.worker` | I/O, no decision: the active set around work |
| 1691 | `Session.run.work` | **decision, not yet in rules/boat.py**: a VM's worker: 8 tick failures in a row (or a 404) → the VM dead and its items lost (LocalItem.done(lost)); backoff min(30, 3·fails); the failure count left for a follow-up Retry |
| 1745 | `Session.run.spawn_workers` | I/O, no decision: one worker per ready VM |
| 1754 | `Session.run.give_up` | **in rules/boat.py**: `LocalItem.give_up`, laws “an item is finished exactly once” |
| 1762 | `Session.run.topup` | **decision, not yet in rules/boat.py**: VMs other batches released, leased to cover the queue's shortfall (want = ceil(short / slots), capped), starts again at most once a minute: the division by a VM's slots is by a variable; left |
| 1828 | `Session.call` | I/O, no decision: one runner call through run |
| 1838 | `Session.collect` | I/O, no decision: logs in parallel |
| 1845 | `Session._collect` | I/O, no decision: logs over the agent or exec |
| 1866 | `Session._let_go` | **in rules/boat.py**: `release_ttl, Entry.release`, laws “a demand keeps a released VM no longer than the idle time (the window is retired)”; “a released VM stays up at most its idle time” |
| 1881 | `Session.harvest_caches` | I/O, no decision: per cache, _harvest |
| 1890 | `Session._harvest` | **decision, not yet in rules/boat.py**: the VM with most of a cache, downloaded only when larger than the local one ('the local cache only grows'); left |
| 1897 | `Session._harvest.ls` | I/O, no decision: a VM's cache names |
| 1933 | `Session._heartbeat` | **decision, not yet in rules/boat.py**: every 300 s each held VM's TTL extended to 900 s (a dead client's VMs stop within 900 s): a timer; left |
| 1940 | `Session.close` | **decision, not yet in rules/boat.py**: kill what runs, wind each VM: teardown → delete, a ttl ≤ 0 → stop, else extend; release (Entry.release); the per-VM choice left |
| 1960 | `Session.close.wind` | **decision, not yet in rules/boat.py**: see Session.close |
| 2008 | `Session.warm_ttls` | **in rules/boat.py**: `release_ttl`, laws “a demand keeps a released VM no longer than the idle time (the window is retired)” (not read yet: gap boat-3, LONG = IDLE) |
| 2021 | `Session._release` | **in rules/boat.py**: `Entry.release`, laws “a release never clears another client's lease”; “a released VM stays up at most its idle time” |
| 2045 | `Session.dollars` | I/O, no decision: boat usage |
| 2057 | `Scheduled.__init__` | I/O, no decision: its class |
| 2067 | `Scheduled.ensure` | I/O, no decision: a no-op (the scheduler sizes it) |
| 2070 | `Scheduled.lease_up` | I/O, no decision: 0 |
| 2073 | `Scheduled.run` | **in rules/boat.py**: `Reports`, laws “every item is reported by the time the run ends”; “an item is reported at most once”; “the items are those reported and those left” — **fix**: items the scheduler never reported are reported lost (were dropped) |
| 2139 | `Scheduled.collect` | **in rules/boat.py**: `Retry`, laws “never more tries than allowed” |
| 2158 | `Scheduled.harvest_caches` | **decision, not yet in rules/boat.py**: a cache lacking outputs: the first VM whose agent has more, downloaded; left |
| 2180 | `Scheduled.close` | **in rules/boat.py**: `Retry`, laws “never more tries than allowed”; “it stops at the first success” |
| 2198 | `Scheduled.dollars` | I/O, no decision: None |
| 2202 | `session` | **decision, not yet in rules/boat.py**: Scheduled when the scheduler is on, else Session: a one-line choice; left |
| 2208 | `scheduler_holds` | I/O, no decision: asks the scheduler whether it holds a VM of the image |
| 2219 | `usage_since` | I/O, no decision: boat usage per VM |
| 2223 | `usage_since.one` | I/O, no decision: one VM's |
| 2232 | `union` | I/O, no decision: the measure of a union of intervals |
| 2243 | `signals_as_interrupt` | I/O, no decision: SIGTERM/SIGHUP as KeyboardInterrupt |
| 2245 | `signals_as_interrupt.h` | I/O, no decision: the handler |
| 2256 | `plan_vms` | **in rules/boat.py**: `plan_vms`, laws “a plan asks at least one VM”; “a plan asks no more VMs than are useful (one at least)”; “a plan stays within its cap” (useful and by_rate, sqrt of the start rate, computed before: I/O of boat's numbers) |
| 2289 | `durations_index` | I/O, no decision: a path |
| 2293 | `note_durations` | I/O, no decision: merges a run's seconds into the index |
| 2305 | `last_durations` | **in rules/boat.py**: `durations_source`, laws “durations covering under half of the jobs are never used”; “a tiny batch searches no runs” |
| 2309 | `last_durations.tsv` | I/O, no decision: parses a summary |
| 2338 | `preflight` | **in rules/boat.py**: `preflight`, laws “a warm agent up is enough, no refusal and no API call”; “a batch waits for another's release only with no starts, room, and VMs leased by others”; “with the scheduler deciding, capacity never refuses a batch” |
| 2393 | `preflight3` | **in rules/boat.py**: `Preflight3`, laws “boat's API failing sends a batch to Modal only after 3 tries”; “preflight is asked at most 3 times” |
| 2407 | `warm_up` | **in rules/boat.py**: `Entry.lease_live (as a count)`, laws “a VM another batch released joins only while it is up for 20 s more” |
| 2415 | `warm_agents` | **in rules/boat.py**: `Entry.lease_warm (as a count)`, laws “a warm lease never takes a VM about to stop” |
| 2425 | `job_items` | **in rules/boat.py**: `weight`, laws “a job gets the slots its CPUs and memory need, one at least” (the bigger VM type for a job past a VM's slots: a table lookup, left) |
| 2464 | `boat_jobs` | **in rules/boat.py**: `route, batch_class`, laws “only a tiny batch with a fallback goes to Modal without trying boat”; “with the scheduler on every batch goes to boat first”; “a tiny batch takes boat's VM already up”; “a small batch is an interactive check, a bigger one a submission, unless its class is given” |
| 2515 | `boat_jobs.done` | I/O, no decision: records a row |
| 2559 | `summary_line` | I/O, no decision: text |
| 2571 | `status` | I/O, no decision: reconcile and print |
| 2588 | `sched_status` | **decision, not yet in rules/boat.py**: the orphan count for status (rules/scheduler.py's VMLife.adopt_tick holds the orphan rule itself) |
| 2623 | `reap` | **in rules/boat.py**: `Entry.reap`, laws “reap never takes a live holder's lease”; “reap stops only a VM no live client holds” |
| 2677 | `cold` | **in rules/boat.py**: `Entry.cold`, laws “cold stops only free VMs” |
| 2691 | `teardown` | **in rules/boat.py**: `Entry.teardown`, laws “teardown deletes only free VMs, of this repo's images unless --all or --type” — **fix**: other repos' free VMs only with --all or --type |
| 2702 | `teardown.rm` | I/O, no decision: boat delete |
| 2710 | `starts_summary` | I/O, no decision: a report of the ledger |
| 2738 | `extra_starts` | **decision, not yet in rules/boat.py**: the wallet's samples (a sample kept when none, 5 minutes old, or changed): information only, no threshold; left |

# Coverage: treadmill.py, fanout.py, runpod.py, controller.py, config.py, migrate.py, __main__.py, modal_run.py, skill.py

Every def of these nine files, in file order: **in** a rule of `supercomputer/rules/` (the code calls it, its laws proven
by `tests/check_laws.sh`), or **I/O** (no decision, and why), or **out** (a decision outside the prover's reach, and why).
Laws are named by their text in the rule module. 169 defs: treadmill 60, fanout 34, runpod 29, controller 15, config 14,
migrate 2, `__main__` 4, modal_run 8, skill 3 (the inventory's 167, plus `Cache._note` and `controller._handed`, added by the
wiring).

## treadmill.py (60) — rules/treadmill.py, rules/ops.py

| def | where |
| --- | --- |
| tm_dir | I/O: the treadmill's directory from the environment or the repo's config (a path, no choice of state) |
| use | I/O: sets the module's directory |
| log | I/O: a log line |
| jl | I/O: reads JSON lines |
| append | I/O: appends a line |
| append_many | I/O: appends lines |
| git | I/O: runs git |
| today | I/O: the UTC day |
| Cache.\_\_init\_\_ | in ResultCache: the rows replayed in time order through `put` |
| Cache.\_note | in ResultCache.put: "a verdict put is seen", "a task's first solve is reported once", "a task once solved stays solved", "a key once seen stays seen" |
| Cache.put | in ResultCache.put (through \_note), then an append (I/O) |
| Cache.solved | in ResultCache: the first solving config per task, as `put` reported it |
| Cache.seen | in ResultCache.run: "a key seen is never run again" |
| Cache.refused | in ResultCache.put's refused set (the default answers); any other set of answers a plain filter |
| Batch.\_\_init\_\_ | in PendingSlots.add and BatchFate's start; a thread (I/O) |
| Batch.backlog_s | out: an estimate (slot-seconds by the items' own times), arithmetic for the digest, decides nothing |
| Batch.held | in PendingSlots: "the count is the slots of the items not yet back" (not read yet: gaps/forkA-4), "a row back takes off its own item's slots", "no item counts below zero"; the VMs from the slots, `ceil(need / slots)`, out: a division by a non-constant (nonlinear) |
| Batch.unpacked | I/O: extracts a tree once (a latch on a file) |
| Batch.backlog | in backlog: "a batch through the scheduler never grows itself" |
| Batch.grow | in StepPlan.grow (its k); the session's ensure is I/O |
| Batch.go | in BatchFate: "a batch boat refused whole never goes to Modal", "items go to Modal only when few", "with Modal off, nothing goes to Modal", "a failed batch records nothing", "a batch records its results only when it didn't fail", "before boat's end nothing is decided"; in chain_class: "the queue's submissions are submissions at the scheduler", "the treadmill's work is never above a submission" |
| Batch.go.row | in PendingSlots.back |
| py_item | I/O: an item's fields from the runner's BOAT setting (the larger weight a copy) |
| sh_item | I/O: an item's fields |
| import_real | I/O: imports a module by path |
| modal_app | I/O: runs a Modal app, one at a time (a lock), its ticket |
| on_modal | I/O: items to Modal through the runner's module or the repo's (routing by the item's kind, no state) |
| on_modal.done | I/O: a row and its logs |
| tree_key | I/O: sha256 of a tar |
| job_key | out: the result cache's identity, a sha256 of the tree key and the job's command and resources: a hash, whose law (one key per tree and command) is sha256's, not linear arithmetic |
| submit | in submit_refusal: "a job on Modal's own image or a GPU never enters the queue", "a name taken is never overwritten" |
| results | I/O: prints a submission's rows |
| ls | in ticket_alive (rules/ops.py) through boat.pid_alive with the status's host: "a ticket of this machine whose pid is gone is never alive" (was pid_alive(pid, ""): always alive) |
| Submission.\_\_init\_\_ | in SubmissionFate's constructor: one left "running" by a dead treadmill queued again |
| Submission.start | in SubmissionFate.start: "a submission starts only from the queue", "a submission runs only under this treadmill's batch" |
| Submission.start.on_row | I/O: a row kept |
| Submission.start.after | in cacheable: "a job that failed by boat's or Modal's error is never cached", "a job's own exit, with its log, is cached as its verdict"; in SubmissionFate.finished: "a done submission stays done" |
| Submission.save | I/O: writes meta.json |
| Treadmill.\_\_init\_\_ | I/O: its knobs from arguments and the environment |
| Treadmill.save_state | I/O |
| Treadmill.held | out: a sum of its batches' held (each in PendingSlots) |
| Treadmill.vmh_today | I/O: sums the day's VM-hours from files |
| Treadmill.budget | out, a finding: the VM-hour budget is computed but never applied (step's `over = False`, by the user's "no limits"); the class's docstring still says "within the day's VM-hour budget" |
| Treadmill.backlog_hours_on_floor | out: an estimate for the status, decides nothing |
| Treadmill.elastic | in elastic: "the treadmill's pool is at least its floor", "the pool above the floor is at most max_vms" |
| Treadmill.want | I/O: a filter of its batches |
| Treadmill.poll | I/O: a repo's hook (none here) |
| Treadmill.floor | in StepPlan.hook: "the repo's hooks never give the step more VMs than it had" (the hook's own launches are the repo's) |
| Treadmill.main_chains | in StepPlan.hook (as floor) |
| Treadmill.fill | in StepPlan.hook (as floor) |
| Treadmill.grows | out: a constant of the policy, the chains that grow ("c", "a") |
| Treadmill.status_extra | I/O: a repo's hook |
| Treadmill.step | in StepPlan: "the treadmill launches work for no more VMs than its pool has free", "what is launched and what is free never pass the pool", "once anything is launched, nothing free is below zero", "nothing is launched below zero", "the queue's first pass holds at most subs_vms VMs at once", "the queue's later pass takes at most 4 VMs a submission", "a backlog grows by a quarter of the pool at most"; in SubmissionFate.batch_ended: "a running submission ends or is queued again"; the hint, samples and demand are I/O |
| Treadmill.run | in closes_leftover: "a treadmill closes only its own client's batches"; the loop, digest and stop are I/O |
| pool_section | I/O: the digest's text |
| scheduler_section | I/O |
| vmh_dollars | I/O |
| subs_section | I/O |
| digest | I/O |
| repo_module | I/O: imports the repo's treadmill module |

## fanout.py (34) — rules/fallback.py

| def | where |
| --- | --- |
| log | I/O |
| fname | I/O: a function's name |
| modal_real | I/O: whether the Modal client is importable |
| reexec_with_modal | in reexec_modal: "it re-runs under the Modal CLI's Python at most once" |
| ModalCtx.\_\_init\_\_ | I/O |
| ModalCtx.\_\_enter\_\_ | in ModalRef.enter: "an app held here is running", "an app of ours was started here", "an app of ours runs", "an app started elsewhere keeps running", "an app started elsewhere is never ours" |
| ModalCtx.\_\_exit\_\_ | in ModalRef.exit: "an app started here is stopped only when its last holder exits", "an app started elsewhere is never stopped here" |
| run_jobs | in job_route: "--backend boat never sends a job to Modal but Modal's own", "--backend modal never runs a job on boat", "a job boat ran stays boat's", "a job boat didn't run is failed only under --backend boat"; in preflight_on: "only auto asks boat's preflight, and not where the controller's scheduler takes the batch"; GPU jobs through runpod.gpu_jobs |
| run_jobs.printer | I/O |
| boat_order | I/O: the summary's order |
| count | I/O: a tally |
| where_line | I/O: the summary's text |
| ModalEx.\_\_init\_\_ | I/O |
| ModalEx.remote | I/O: Modal's call |
| ModalEx.starmap | I/O |
| ModalEx.close | I/O |
| BoatEx.\_\_init\_\_ | I/O: its session and the runner's BOAT setting |
| BoatEx.\_est | out: an estimate for the order of calls (longest first), arithmetic, decides no state |
| BoatEx.remote | in row_outcome: "a row with a result and exit 0 is the call's result", "a row boat lost is boat's, never the call's own failure" (its own failure now CallFailed) |
| BoatEx.starmap | in row_outcome (each row); plan_vms is boat.py's |
| BoatEx.starmap.done | in row_outcome |
| BoatEx.starmap.go | I/O: the session's run in a thread |
| BoatEx.close | I/O |
| AutoEx.\_\_init\_\_ | in preflight_on; in AutoFallback.refused: "with boat refused, every call is Modal's" |
| AutoEx.\_modal | I/O: the Modal context, once |
| AutoEx.\_moved | I/O: a tally of reasons |
| AutoEx.remote | in AutoFallback.outcome: "Modal takes the batch only after 3 boat failures", "a call goes to Modal only once boat failed the batch 3 times", "a call's own failure is never a boat failure", "a call's own failure is its result, never retried nor moved" |
| AutoEx.starmap | in AutoFallback (the batch on Modal once refused); each call lost twice to Modal is the session's on_lost (boat.py's) |
| AutoEx.close | I/O |
| cli | out: dispatch on --backend (modal: ModalEx; boat: BoatEx with no Modal fallback; auto: AutoEx); the routes it builds are job_route's and AutoFallback's |
| cli.h | I/O: a signal handler |
| gpu_call | in GpuTries: "at most two pods a batch", "no pod after RunPod said it has no capacity, nor after the jobs are done"; in gpu_leftover: "--backend runpod never sends a GPU job to Modal" |
| gpu_cli | I/O: flags |
| gpu_cli.h | I/O |

## runpod.py (29) — rules/fallback.py, rules/ops.py

| def | where |
| --- | --- |
| \_env | in precedence (rules/ops.py): "the first source given wins", "a later source only where the earlier ones aren't given" |
| log | I/O |
| live | I/O: a path |
| rp | I/O: runpodctl |
| last_json | I/O: parses output |
| balance | I/O |
| preflight | in runpod_ready: "RunPod takes a batch only with its API up and its balance at the floor" |
| gpu_cfg | I/O |
| Pod.\_\_init\_\_ | out: the GPU list's precedence over four sources (argument, environment, config, the default list); a list's choice, outside the subset (lists); the same rule as precedence |
| Pod.\_ticket | I/O |
| Pod.\_\_enter\_\_ | in PodCreation: "at most max_tries pods that never came up", "no pod is tried past the deadline" |
| Pod.ready | I/O: polls until ssh answers or the wait passes (a timeout, no state) |
| Pod.ssh_args | I/O |
| Pod.sh | I/O |
| Pod.sh2 | I/O |
| Pod.envs | I/O |
| Pod.prepare | in Retries (rules/ops.py): "at most max_tries tries", "no try after a success", "a try is made only while tries are left" (the probes); its gates (PREP-OK, the GPU check, TREE-OK) raise, no state |
| Pod.job | in job_outcome: "ssh failing with no output on a lost connection is the pod's, not the job's", "a job killed at its limit is a timeout" |
| Pod.call | I/O: the result's marker or an error |
| Pod.\_delete | in Retries (the delete rounds) |
| Pod.\_\_exit\_\_ | I/O: delete, ticket, cost |
| sh_quote | I/O |
| signals_as_interrupt | I/O |
| signals_as_interrupt.h | I/O |
| call | I/O: a pod for one call |
| gpu_list | out: an ordered list (the jobs' named types first, then the rest), outside the subset (lists) |
| gpu_jobs | in GpuTries and gpu_leftover: "a GPU job is failed only under --backend runpod" |
| modal_gpu_jobs | I/O |
| reap | in pod_reap: "a pod whose client lives is never deleted", "a pod not named as ours is never touched" (the ticket's host now this machine's where a ticket has none) |

## controller.py (15) — rules/ops.py

| def | where |
| --- | --- |
| info | I/O: reads controller.json |
| ssh | in Retries: "at most max_tries tries", "no try after a success" |
| tool_tar | I/O |
| bundle | I/O |
| unit | I/O: a systemd unit's text |
| setup | in copy_state: "the controller's treadmill state is never overwritten"; clone or sync, the drain and the restart are I/O over ssh |
| handover | in handover_count: "the controller is given no more than it lacks", "no more than are free here"; in HandoverVM (through \_handed) |
| \_handed | in HandoverVM: "a VM handed over is in one registry, never in neither", "a VM is never in both registries", "a VM being moved is still here", "a write the controller didn't confirm leaves the VM here" |
| handover_warm | in HandoverVM (through \_handed) |
| rekey | I/O: rotates the key (no decision but the order of its steps) |
| boat_ok | I/O: AUTH-OK |
| sync_git | in sync_due: "main is synced only to a controller of this repo", "main is not synced again where it hasn't moved"; recorded only on SYNC-OK |
| backup | in Cadence.backup_due: "a backup at most hourly unless forced"; the newest five kept is a list op (I/O) |
| maybe_sync | in Cadence.check_due: "the controller's key is checked at most hourly" (was keyed to the last backup's time) |
| main | I/O: dispatch |

## config.py (14) — rules/ops.py

| def | where |
| --- | --- |
| env | in precedence: "the first source given wins" |
| git_root | I/O |
| main_checkout | I/O |
| Project.\_\_init\_\_ | I/O: its fields from the config file with defaults |
| Project.hook | I/O |
| Project.layer_scripts | out: the hooks' layers, else the config's, else the base layer (precedence over lists) |
| Project.cache_key | out, a finding: a sha256 over the salt and the inputs (a hash, not linear arithmetic); a missing input hashes as b"" (indistinguishable from an empty file) |
| Project.cache_outputs | I/O |
| find_config | out: the first of four files that exists ($SUPERCOMPUTER_CONFIG, the root's, the main checkout's, repos/); file existence, the same rule as precedence |
| use | I/O |
| project | I/O |
| set_project | I/O |
| private_umask | I/O |
| live_dir | I/O |

## migrate.py (2) — rules/ops.py

| def | where |
| --- | --- |
| merge_registry | in merged_starts: "merging two registries loses no start either counted", "merging counts no start neither did" (was a set: note_starts(k)'s k starts at one timestamp counted once) |
| main | out: a one-off migration (renames, merges, a symlink) under the old registry's lock, its merge merge_registry's |

## \_\_main\_\_.py (4) — rules/fallback.py, rules/ops.py

| def | where |
| --- | --- |
| \_run | in needs_modal: "--backend boat with no job of Modal's never loads the Modal client" |
| \_remote_queue | I/O |
| \_reap | in reap_modal: "an orphan Modal app (its client's pid gone) is stopped", "a Modal app whose client lives is never stopped" (was pid_alive(pid, ""): never stopped) |
| main | I/O: dispatch, the reap's alarm |

## modal_run.py (8) — rules/ops.py

| def | where |
| --- | --- |
| \_allowed | I/O: the tree's files |
| \_allowed.ignore | I/O |
| \_image | I/O: Modal's image |
| \_run | I/O on Modal (TimeoutExpired as 124: the runtime's classification, as job_outcome's timeout) |
| sh | I/O |
| sh_gpu | I/O |
| modal_jobs | I/O |
| \_modal_gpu | in modal_gpu: "a RunPod type with a Modal match maps to it", "the default GPU only where no other is named" |

## skill.py (3) — rules/ops.py

| def | where |
| --- | --- |
| \_controller | in controller_state: "the controller is reported up only when its status answered" |
| \_limits | I/O |
| text | I/O |

# Coverage: the agent, the store and the volumes

Every def of agent.py (65), store.py (18, its VM scripts' included), volumes.py (30, its VM scripts' included),
modal_volumes.py (20) and modal_pull.py (2), as of 8baa6da: in a rule with its laws, or I/O and why it decides nothing.
The agent's rules are classes in agent.py itself (it ships to each VM alone; supercomputer/rules/agent.py is a link to
it); the integrity rules are supercomputer/rules/integrity.py, which also heads the VM scripts and rides into the
Modal image (one source each). Every law is proven and checked by tests/check_laws.sh, from any state (gdp.toml),
and the bugs they rule out are refuted in tests/broken_integrity.py.

Defs: 135; deciding (in a rule): 46; I/O: 89.

## agent.py (65)

| def | where |
| --- | --- |
| `_Any.__init__` | I/O, no decision: the Modal stub's catch-all object |
| `_Any.__getattr__` | I/O, no decision: the stub answers every attribute with itself |
| `_Any.__call__` | I/O, no decision: the stub's call returns itself |
| `_App.function` | I/O, no decision: a decorator giving the function back |
| `_App.local_entrypoint` | I/O, no decision: the same |
| `_App.cls` | I/O, no decision: the same |
| `modal_stub` | I/O, no decision: installs the stub as `modal` |
| `import_runner` | I/O, no decision: imports a runner module by path |
| `batch_dir` | I/O, no decision: a path |
| `sudo` | I/O, no decision: runs a command under sudo -n |
| `write_json` | I/O, no decision: atomic write (tmp + rename) |
| `alive` | in agent.py's rules (also rules/agent.py): alive_of, with law "a zombie is never alive" (the function gathers kill(0) and /proc's state) |
| `descendants` | I/O, no decision: the process tree from /proc |
| `kill_tree` | I/O, no decision: sudo kill -KILL of a tree |
| `ctx_of` | I/O, no decision: reads ctx.json |
| `file_key` | I/O, no decision: a file's sha256 (+x when executable): the hash the rules compare |
| `have` | I/O, no decision: the keys whose blob file is absent |
| `put_blobs` | in agent.py's rules (also rules/agent.py): BlobStore.put, with law "no blob is stored under a key its bytes don't hash to" |
| `blobs_tar` | I/O, no decision: a tar of the blobs present |
| `peer_req` | in agent.py's rules (also rules/agent.py): PeerTries, with laws "a request is tried at least once", "at most tries attempts", "a request still trying has a try left", "a request ends" (liveness) |
| `pull` | I/O, no decision: batches of keys through peer_req and put_blobs (whose rules decide) |
| `pullcache` | I/O, no decision: a cache's tar from a peer, into cachein (whose rule decides) |
| `make_tree` | I/O, no decision: _make_tree under its tree's lock |
| `_tree_lock` | I/O, no decision: a lock per tree id |
| `_make_tree` | in agent.py's rules (also rules/agent.py): BlobStore.may_make_tree, with law "no tree is made under an id its manifest doesn't hash to (nor with a blob missing)" (fix: the client's id was taken on trust); a tree being made is a dot-directory that gc_trees counts as building |
| `tree_id` | I/O, no decision: the sha256 of a manifest (the id the rule compares) |
| `ingest` | in agent.py's rules (also rules/agent.py): BlobStore.may_make_tree (through make_tree), the same law |
| `in_use_trees` | I/O, no decision: the trees of batches with a live item (an input of BlobStore.drop_tree) |
| `gc_trees` | in agent.py's rules (also rules/agent.py): BlobStore.drop_tree and drop_blob, with laws "a tree in use or among the newest kept is never removed", "no blob is collected while a tree is being made" (fix: the race with a tree being built), "a blob a tree links is never collected" |
| `_quiet.__enter__` | I/O, no decision: a context manager |
| `_quiet.__exit__` | I/O, no decision: swallows an error |
| `fetch` | I/O, no decision: curl or a move |
| `cache_dir` | I/O, no decision: a path |
| `cachein` | in agent.py's rules (also rules/agent.py): CacheFiles.take_in, with law "a cache file there is never overwritten" |
| `cache_names` | I/O, no decision: a directory's names |
| `drop_old_batches` | in agent.py's rules (also rules/agent.py): BatchDirs.drop, with laws "the batch being prepared is never dropped", "a batch with a live item is never dropped", "a batch is dropped only after keep_s quiet" |
| `prep` | in agent.py's rules (also rules/agent.py): Tarballs.stays, with law "at most keep tarballs stay"; its source kinds (tree:, cache:, a URL) are a dispatch on the source's form, refusing a source not on the VM |
| `serve` | I/O, no decision: starts an http.server |
| `cpus_in_use` | I/O, no decision: the CPUs running items hold (CpuGrant's held) |
| `start` | in agent.py's rules (also rules/agent.py): CpuGrant.grant, with laws "CPUs held never exceed the VM's", "a job never holds more CPUs than it asked for" (fix: no more oversubscribed CPUs counted as held); a process that never started is written lost (fix) |
| `report` | in agent.py's rules (also rules/agent.py): JobReport.read, with laws "an acked item is never reported again", "an item whose process died without a word is lost, never running", "an item with no process is running only while it is being started" (fix: no longer running for ever) |
| `tick` | in agent.py's rules (also rules/agent.py): Ticks.ack and Ticks.stop, with laws "an ack takes only a finished item", "the wait never outlasts its limit while something runs and nothing finished" |
| `kill` | in agent.py's rules (also rules/agent.py): Kill.kills, with law "a finished item is never killed" |
| `kill_item` | in agent.py's rules (also rules/agent.py): Kill.kills, the same law (an acked item counts as finished too) |
| `vtick` | in agent.py's rules (also rules/agent.py): Ticks.ack, Ticks.stop and BatchDirs.may_start, with law "no item starts in a batch not prepared here" |
| `cpu_model` | I/O, no decision: /proc/cpuinfo |
| `wrap` | I/O, no decision: the item's command line (namespace, binds, taskset of the CPUs start chose) |
| `caches_in` | I/O, no decision: copies a cache's files in |
| `caches_out` | in agent.py's rules (also rules/agent.py): CacheFiles.save, with law "only an output the job built and the cache lacked is saved" |
| `run` | in agent.py's rules (also rules/agent.py): Outcome.code, with laws "a kill is a timeout only at the item's limit", "an item whose tree is gone is lost" |
| `call` | I/O, no decision: calls a runner's function |
| `logs_tar` | I/O, no decision: the tails of out and err (fixed caps, no state) |
| `parts` | in agent.py's rules (also rules/agent.py): Parts.remake and Parts.count, with laws "part 0 makes the blob again", "a blob has at least one part" (the division rounding up by the part size is the code's: a division by a variable is outside the rules' linear arithmetic) |
| `collect` | I/O, no decision: parts of the logs |
| `cache_tar` | I/O, no decision: a cache's tar |
| `cacheout` | I/O, no decision: names or parts |
| `daemon` | I/O, no decision: the HTTP server's set-up (its door is Access.allows) |
| `daemon.reaper` | I/O, no decision: reaps finished children |
| `daemon.H.log_message` | I/O, no decision: silence |
| `daemon.H._ok` | in agent.py's rules (also rules/agent.py): Access.allows, with law "nothing but /health answers without the token" |
| `daemon.H._send` | I/O, no decision: an HTTP answer |
| `daemon.H._body` | I/O, no decision: reads the body |
| `daemon.H.do_GET` | in agent.py's rules (also rules/agent.py): Access.allows (health), the same law |
| `daemon.H.do_POST` | I/O, no decision: a dispatch on the command's name, after _ok; `begin`'s version check is the scheduler's code-follows rule (the agent answers its version, the client upgrades) |
| `start_daemon` | in agent.py's rules (also rules/agent.py): Access.keep_daemon, with law "a daemon kept is alive and on this version and token" |

## store.py (18)

| def | where |
| --- | --- |
| `log` | I/O, no decision: stderr |
| `sha256` | I/O, no decision: a file's sha256 (what the rules compare) |
| `manifest` | I/O, no decision: {path: [size, sha256]} |
| `CHECK.h (VM script)` | I/O, no decision: a file's hash on the VM; the script reports counts, push decides (Checks.pushed) |
| `new_vm` | I/O, no decision: boat new, then note_starts(1) (boat.py's accounting) |
| `from_modal` | in rules/integrity.py: Sizes.fits, with law "nothing past its limit goes" |
| `push` | in rules/integrity.py: Sizes.fits and Checks.pushed, with laws "nothing past its limit goes", "a push is kept only when the VM's copy matches the manifest whole"; the VM is always deleted (a finally, no choice) |
| `RANGE_SERVER.H.log_message (VM script)` | I/O, no decision: silence |
| `RANGE_SERVER.H.send_head (VM script)` | in rules/integrity.py (shipped at the script's head): Ranges.last and Ranges.length, with laws "a range never ends past the file", "a range served has no negative length" |
| `RANGE_SERVER.H.copyfile (VM script)` | I/O, no decision: copies the bytes send_head allowed |
| `serve` | I/O, no decision: a VM from the snapshot, the server, a record |
| `pull` | in rules/integrity.py: PullFile.cache and PullFile.place, with laws "a fetched file is cached only when its whole sha256 matches its digest", "nothing is placed outside the pull's directory" (fix: `..` and absolute paths refused) |
| `pull.opener` | I/O, no decision: an opener with a cookie jar |
| `pull.full` | I/O, no decision: a URL |
| `pull.get_range` | in rules/integrity.py: RangeFetch, with laws "a fetch gives up only after more than limit failures in a row", "a fetch done has every byte", "a fetch still going has bytes left and failures to spare", "a fetch ends" (liveness) |
| `unserve` | in rules/integrity.py: Serving.unserve, with law "a serving VM goes only when asked for all, for its name, or past its minutes" |
| `verify` | in rules/integrity.py: Manifest.verify, with law "verify passes only when every manifest entry matches" |
| `main` | I/O, no decision: the CLI's dispatch |

## volumes.py (30)

| def | where |
| --- | --- |
| `log` | I/O, no decision: stderr |
| `RECV.St.__init__ (VM script)` | I/O, no decision: the stream's directories and its tar (its order is a PartOrder) |
| `RECV.St.feed (VM script)` | in rules/integrity.py (shipped at the script's head): PartOrder.consume, with laws "a part is consumed only in index order", "nothing is consumed past the stream's end", "an ended stream is complete only once every part was consumed" |
| `RECV.H.log_message (VM script)` | I/O, no decision: silence |
| `RECV.H.reply (VM script)` | I/O, no decision: an HTTP answer |
| `RECV.H.chunks (VM script)` | I/O, no decision: reads a body (a short one is an error) |
| `RECV.H.save (VM script)` | I/O, no decision: writes and hashes |
| `RECV.H.do_GET (VM script)` | in rules/integrity.py: ReceiverGate.admit, with law "no PUT is taken without the token in its path and its header" (status needs both too) |
| `RECV.H.do_PUT (VM script)` | in rules/integrity.py: ReceiverGate.admit and end_ok, PartOrder.arrive and finish, PullFile.cache, with laws "no PUT is taken without the token in its path and its header", "an end is ok only when the stream was consumed whole with no error", "a part whose sha256 differs is never kept", "a part already consumed is a duplicate", "a fetched file is cached only when its whole sha256 matches its digest" (fix: a whole file whose sha256 differs is no longer kept) |
| `CHECK.h (VM script)` | I/O, no decision: the VM's hashes; the counts go to the rules (Checks, CopyGates) |
| `ticket` | I/O, no decision: a ticket of the VM (now with its host, for cleanup) |
| `new_vm` | I/O, no decision: boat new and a ticket |
| `delete_vm` | I/O, no decision: boat delete |
| `sh` | I/O, no decision: boat exec, parsed |
| `run_py` | I/O, no decision: a script on the VM |
| `check_dir` | in rules/integrity.py: Checks.again, with law "a check runs again only on a warm VM where files differed" |
| `keypair` | I/O, no decision: makes the key pair when absent (a file's existence, no state rule) |
| `decrypt_check` | in rules/integrity.py: Checks.decrypted, with law "a decryption passes only when every hash, the magic and the MAC match" (fix: the magic is a check, not an assert dropped under -O) |
| `tree_check` | I/O, no decision: the snapshot's listing against the manifest, as counts (a listing boat cut short judged on what it lists); CopyGates.tree_ok decides |
| `modal_run` | I/O, no decision: `modal run`, waited on by its PID |
| `inventory` | I/O, no decision: modal_run of the inventory |
| `record_path` | I/O, no decision: a path |
| `done` | in rules/integrity.py: Serving.copy_again's input (a record and its snapshot ready) |
| `restore_check` | in rules/integrity.py: Checks.restore, with law "a restore passes only when every file and link matches and the manifest's sha256 is the one recorded" (fix: a record without it no longer passes trivially) |
| `receive` | I/O, no decision: the receiver, the Modal sender, the timeout's arithmetic (a division by --min-mbs, a variable: outside the rules' linear arithmetic, sizing only) |
| `copy_one` | in rules/integrity.py: CopyGates, with laws "a copy is recorded only after every gate passed" (and its seven helpers: each stage past a gate passed it), "a copy's check passes only when ... its manifest's sha256 is the one sent", "a snapshot's tree passes only when every size and link it lists is as recorded" (fixes: the KeyError on manifest_sha256, the tree check that never gated) |
| `verify` | I/O, no decision: restore_check on a new VM, deleted after |
| `cleanup` | in rules/integrity.py: Serving.cleanup, with laws "a copy's VM goes only when its client is dead", "a copy's VM goes once its client is dead" (fix: pid_alive(pid, "") held every client alive) |
| `ls` | I/O, no decision: the records, printed |
| `main` | in rules/integrity.py: Serving.copy_again, with law "a stored name is copied again only when asked or not ready"; the rest the CLI's dispatch |

## modal_volumes.py (20)

| def | where |
| --- | --- |
| `scan` | in rules/integrity.py (shipped into the image): Secrets.scan_content, with law "content is scanned only in small text files within the time allowed"; the patterns are data |
| `mounts` | I/O, no decision: Modal volumes by name |
| `inventory` | I/O, no decision: spawns the scans, collects, cancels |
| `scan_volume` | I/O, no decision: scan of a mounted volume |
| `_client` | in rules/integrity.py: Tries(6), with laws "a call is tried at least once", "at most tries attempts", "a call still trying has an attempt left", "a call ends" (liveness) |
| `_client.call` | I/O, no decision: a PUT |
| `_retry` | in rules/integrity.py: Tries, the same laws |
| `_Parts.__init__` | I/O, no decision: the sink's buffer |
| `_Parts.write` | I/O, no decision: full parts cut as the buffer fills (a size comparison, no choice beyond it) |
| `_Parts.close` | in rules/integrity.py: Secrets.last_part, with law "an empty stream still goes as one part" |
| `_Hashing.__init__` | I/O, no decision |
| `_Hashing.read` | I/O, no decision: reads and hashes |
| `_entries` | I/O, no decision: the files and links, sorted |
| `secret_kinds` | in rules/integrity.py: Secrets.scan_content, the same law; the kinds' patterns are data |
| `_tar` | in rules/integrity.py: Secrets.route, with law "a secret-looking file never enters the plain manifest" |
| `_encrypt` | I/O, no decision: AES-256-CBC and HMAC through openssl |
| `send_volume` | in rules/integrity.py: Sizes.fits, with law "nothing past its limit goes" (1 GiB to encrypt, 512 MiB of secrets); fix: manifest_sha256 sent again (it was inside a comment, so every copy_one hit a KeyError) |
| `send_volume.up` | I/O, no decision: an upload worker; after the first error the rest are skipped (the copy fails as a whole) |
| `send_volume.finish` | I/O, no decision: the stash encrypted, its manifest |
| `copy` | I/O, no decision: spawns send_volume; its memory is sizing arithmetic (a product of two inputs: outside the rules' linear arithmetic) |

## modal_pull.py (2)

| def | where |
| --- | --- |
| `pull` | in rules/integrity.py: Manifest.verify, with law "verify passes only when every manifest entry matches" |
| `main` | I/O, no decision: the local entrypoint |
