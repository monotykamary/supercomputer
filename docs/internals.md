# Internals

Where things live, how a VM is brought up and kept warm, and every environment knob. The short version is in the [README](../README.md).

## Layout

| File | What |
| --- | --- |
| `supercomputer/config.py` | LIVE (the shared state), a repo's `supercomputer.toml` and hooks (`Project`) |
| `supercomputer/boat.py` | the boat backend: the pool and its registry, the starts ledger and pacing, demand, the image's layers, the warm agents' client, a batch (`Session`), `boat_jobs` |
| `supercomputer/agent.py` | the VM side (stdlib only, shipped to each VM and pod): items in mount namespaces, the build caches, the warm agent (`daemon`) |
| `supercomputer/fanout.py` | one interface over the providers: `run_jobs` (a jobs file), `BoatEx`/`AutoEx`/`cli` (a Modal runner's `ex.remote`/`ex.starmap`), `gpu_call` |
| `supercomputer/runpod.py` | the RunPod backend: a pod per batch, GPU jobs and runner functions, deleted when the batch ends; Modal fallback |
| `supercomputer/modal_run.py` | the Modal app for a repo that has none (and for GPU jobs): its image from the repo's config |
| `supercomputer/scheduler.py` | the one queue: classes, placement, the plan, holds, adoption, re-attach, the breaker (its decisions through `rules/`) |
| `supercomputer/rules/` | every decision of the tool, apart from threads, I/O and the providers' APIs, each with its laws (`# gdp: law`): `scheduler.py` (an item's fate, the pool's starts and breaker, a VM's owner, hold and release, generations, the watchdog, the handoff), `boat.py` (leases and the registry, starts accounting, bring-up, the local session), `treadmill.py`, `fallback.py` (boat, RunPod, Modal), `ops.py` (the controller, config, migration), `agent.py` (the VM agent, the file itself), `integrity.py` (hashes and manifests); `COVERAGE.md`: every function of the tool, in a rule or I/O and why |
| `tests/` | `check_laws.sh` (the laws; the pre-commit hook runs it), `broken_*.py` (the bugs they rule out, kept refuted with a run), `test_*wiring.py` (the code's decisions through the rules, no provider called) |
| `supercomputer/treadmill.py` | the generic treadmill: batches, result cache, the queue, the controller, the digest |
| `supercomputer/store.py` | volumes on boat: named snapshots with sha256 manifests, token-gated serving, pulls cached by hash |
| `supercomputer/modal_volumes.py`, `volumes.py` | a Modal volume's inventory and its copy straight to boat |
| `supercomputer/migrate.py` | a repo's old pool directory into the shared one (done for gdp-wired: its `supercomputer/.live` links here) |
| `live/` | the shared state (in `$SUPERCOMPUTER_HOME`, never in git): `boat-pool.json` (+ `.lock`), `boat-starts.jsonl`, `agents/` (0700), `store/`, `durations/`, `index/`, caches by name (`olean/`), Modal app and RunPod tickets, `builds/` |

## A repo's config: `supercomputer.toml` at its root

See `config.py`'s docstring for every key. In short: `name` (the tree is `/src/<name>` and `/work/<name>` on a VM),
`[boat]` (snapshot prefixes, the PATH, env and mount points jobs need, ignore patterns on top of `.gitignore`, the image's
layers or a `hooks` module's `layers(root)`, `images` for jobs that name one), `[[cache]]` (a build cache keyed by the
hash of its sources: gdp-wired's Lean library), `[modal]` (the repo's own Modal module, else `image` for this tool's),
`[gpu]` (the pods' setup, their check, the Modal GPU), `[treadmill]` (its module and directory), `[run] pre`.
A repo with no file gets defaults: its directory name, a base image (curl, git, build-essential), Modal's slim image.

## Warm agents: small checks in seconds

Each VM a batch brings up runs a warm agent (`agent.py daemon`, 127.0.0.1:8770) behind boat's token-gated HTTPS URL
(`boat host --private`) and a token of ours (`X-SC-Token`); the URL, token and boat's cookie are kept in
`live/agents/<vm>.json` (0600). A later batch that finds a VM up, free and not about to stop with an agent leases it
from the registry, sends one request (the agent's version, the tree by id, the batch's directory), and runs its jobs
over the same connection; the only boat API calls are the TTL extensions (in the background at the lease, with the
logs' round trip at the release). The agent keeps every file it was sent in a store by content (sha256), so a changed
tree costs only its changed files; a tree is made by hard links. A VM without an agent (resumed, created) is brought up
as before (a tarball from a seed VM's URL) and gets its agent for the next batch.

Measured on gdp-wired (two trivial jobs, a large VM already up): 36.3 and 39.2 s a batch before (boat API calls,
the tree as a tarball over ssh, exec ticks), 2.4 to 2.8 s with a warm agent; a resumed VM still costs about a minute.

## Local blob cache

`live/scheduler/blobs` is a disposable content-addressed cache, not an evidence archive. Every five minutes, maintenance evicts least-recently-used cold blobs toward the default 8 GiB target, and expires unreferenced blobs after seven days. Queued/running manifests (including cached tree IDs) pin their contents. Completed admissions release their pins with a generation-scoped marker, without deleting results; older queue records without a generation remain conservatively pinned until closed. Uploads and reads renew a one-hour grace period; writes, admission and pruning share an OS lock across processes. Unreadable queue state prevents deletion. Active/recent data may exceed the target; the report exposes that excess rather than deleting required inputs. Remote clients revalidate even known tree IDs and re-upload evicted files.

`supercomputer cache prune --dry-run` previews reclamation; `supercomputer cache prune` applies it; `supercomputer cache status` shows the last automatic/manual report. Policy knobs: `blob_cache_bytes`, `blob_cache_grace_s` (at least 60), and `blob_cache_max_age_s` (at least the grace). Use `supercomputer store` for durable evidence and retain its snapshot/manifest receipt.

Untracked `.verification/` directories are excluded from implicit source trees, including with custom ignore lists. Tracked fixtures still ship. Explicit `--data DIR:DEST` still supplies required verification inputs. This avoids repeatedly storing whole generated source archives as distinct opaque blobs.

## Exact CPU constraints

`Item.spec.cpu_model` survives the existing admission JSON. The placement shape includes that exact string;
slot search, reservations, preemption, waste accounting and the final placement guard all require agent equality.
`VMState.model` is carried in the additive handoff snapshot and refreshed from `vtick`. Placements record the CPU
immediately; late reports backfill unconstrained in-flight placements.

`live/scheduler/cpu-constraints.json` atomically retains each image/type/model's observed workers, start draws,
pending outcomes and consecutive-fruitless streak across restarts and daily metrics rotation. Status and daily
metrics expose the constraint summary. This file is local scheduler bookkeeping, not a lease authority.

`distinct_vm_group` consumes VM IDs in a batch-scoped set, atomically with an fsynced `.placed` record
before launch. Restart replays every record, including completed members. The full `distinct_vm_groups` map
is carried in hot-handoff snapshots and ACKs; a mismatch prevents commit. `distinct_vm_group_protocol: 1`
advertises this contract. The client probes deployed capabilities; grouped admissions require the explicit
integer `placement_contract: 1`, rejecting old clients before dispatch. Placement receipts include the group,
VM and the agent lease boot ID if available. No agent or lease protocol changes are needed.

## Nonblocking controller backups

`run` and remote queue commands schedule the optional controller backup on a daemon
thread; they do not wait for `/state`. A nonblocking `controller-backup.lock` flock
admits one downloader per client state directory. `controller-backup.json` records
`t_attempt` **before** the request, so failures also consume the one-hour cadence.
Explicit `controller sync` bypasses that cadence, not the in-flight lock. The
whole-request subprocess deadline is 30 seconds, covering setup, retries and
slow reads; the newest five
archives remain in `controller-state/`. Backup metadata does not overwrite the
controller's connection or git-sync metadata. A command exiting may abandon its
optional daemon backup; a later hourly attempt or explicit sync can retry.

The server retains the old binary `/state` response and accepts old callers. A
single-flight cache reuses each completed archive for 30 seconds. Directory
traversal, payload reads, gzip level 1, and socket writes are outside the mutation
lock. The short queue snapshot guard pins open file descriptors and lengths under
the request/placement locks, not file contents: atomic replacements keep their
captured inode; append journals keep only the complete prefix captured then.
Unlinking or appending after capture cannot change that export. Other atomic state
records are pinned without the scheduler locks; this is not a cross-process
transaction with the independently running treadmill.

This is a **controller control-plane recovery** archive, not a disk image or a
scientific-results archive. It keeps admissions, ownership/group/cancel journals,
explicit close receipts and accounting, referenced source-tree manifests, pending
treadmill submissions (including their source archive), result-cache metadata,
and unacknowledged job output. Reinstallation supplies scheduler code; credentials
must be recreated; content-addressed inputs are recovered from surviving workers
or their original sources. A simultaneous loss of those inputs needs a separate
durable data backup.

Regenerable commit workspaces, unreferenced tree manifests, build caches, saved
code, closed/orphan job output, diagnostic logs and previous backups are excluded
**from the archive only**. No controller file is deleted. Historical experimental
results remain on disk and have their own retention policy. There is no byte or
file-count cap that discards required state or fails a large backup. The
`state_backup` status field reports compressed/source bytes, file counts, contents
by path, exclusions, build/cache-hit counts, age and last error so growth is visible.
Backup construction/cache failures do not change admission, leases, or handoff.

## Environment

`SUPERCOMPUTER_{LIVE,CLIENT,URGENT,VM,MAX_STARTS,SHARE,RESERVE_DAY,RESERVE_HOUR,CEIL,IMAGE,WARM,VERBOSE,CONFIG,HOME,SRC}`;
the policy's `SUPERCOMPUTER_{BRINGUP,HORIZON,GATE_HORIZON,MAX_VMS,HOLD,SUB_VMS,SUB_PREEMPT,PREP_MAX,BOAT_TRIES,WATCHDOG,CONTROLLER,SCHEDULER}`
(gdp-wired's older `GDPW_BOAT_*` names are read too); `SUPERCOMPUTER_RUNPOD_{GPUS,CLOUDS,MIN_BALANCE,FAIL}`.

