# supercomputer: run work on boat VMs (agents' guide; `supercomputer --skill` prints it, live values filled in)

**Use it for everything but the extremely light** (tests, checks, Lean, bars, sweeps, timing claims). The laptop runs
only quick bounded checks. Tool: github.com/monotykamary/supercomputer (README and docs/ for depth; state in ~/.local/share/supercomputer). Now: {live_now}

**Run a jobs file** from any repo (its git root is the tree; `supercomputer.toml` says what it needs):
- `supercomputer run --jobs jobs.json --out runs/x [--class C] [--timing] [--data DIR[:DEST]] [--env K=V]`
- jobs.json: `[{"name", "cmd", "cwd"?, "cpu"?, "memory"?, "timeout"?, "image"?, "backend"?, "gpu"?, "env"?, "vm_type"?, "cpu_model"?, "cpu_model_max_fruitless"?, "distinct_vm_group"?, "distinct_vm_group_scope"?, "distinct_vm_group_generation"?, "required_agent_capabilities"?}]`;
  `cmd` runs under bash in `<repo>/<cwd>`; logs in `<out>/<name>.log`, `<out>/summary.tsv` (name, exit, seconds, where).
- The tree ships without node_modules: start a bun job with `bun install --frozen-lockfile && ...`.
- `--timing`: each job alone on its VM; `<out>/placement.json` is the label a timing claim needs.
- `--vm-type T` (or a job's `"vm_type"`): every job placed on type T only (a timing claim's fixed hardware); unnamed, the smallest
  free VM that fits, any type.
- A job's `"cpu_model"` pins the exact agent-reported string and requires `"vm_type"`, e.g.
  `"cpu_model":"AMD Ryzen 9 9950X 16-Core Processor", "vm_type":"xlarge"`. Unknown models never match;
  no different-model or Modal fallback. `cpu_model_max_fruitless` (default 12) bounds consecutive unsuccessful
  starts per image/type/model, not spend; a job can override it with `"cpu_model_max_fruitless": 64`.
  Exhausted items report `unplaceable_by_constraint`, their effective limit and all observed model counts.
- `"distinct_vm_group":"run/stage/case/rep"`: each member uses a different VM for this batch's lifetime,
  including after completion, restart or hot handoff. Combines with `cpu_model`; no fallback. Receipts include
  the group, VM ID and agent boot ID when available. Jobs start as their VMs are ready, with no global barrier.
- Opt in across batches/clients of one controller with `"distinct_vm_group_scope":"durable"`; the scheduler
  persists a generation at admission. Optional `"distinct_vm_group_generation":"GEN"` pins that epoch.
  Requires capabilities `distinct_vm_group_scope:"durable"` and `durable_distinct_vm_group_protocol:1`, checked
  before tree upload/submission; no Modal fallback. Missing scope keeps batch-only behavior unchanged.
  `python3 -m supercomputer.boat distinct-groups open|get GROUP` prints the generation/record as JSON;
  `python3 -m supercomputer.boat distinct-groups close GROUP --generation GEN --ttl-s 3600` starts expiry.
  Close is idempotent only for the same generation and TTL. Admissions and VM exclusions continue until
  `expires_at`; afterward new unpinned work/open creates a fresh epoch, but pinned old requests are refused
  and old queued work never silently rebinds. Open groups do not expire automatically. Coordinate close
  with every producer. Durable metadata does not park or reserve VMs; ordinary release policy still applies.

**Classes** (`--class`, else `$SUPERCOMPUTER_CLASS`; a small batch defaults to interactive, a bigger one to submission):
- `interactive`: quick checks; a slot at once (a filler's or sweep's item is killed and re-queued).
- `gate`: bars and suites; lower slots at once, then only the VMs that finish it within {gate_horizon} min.
- `submission`: experiments; up to {sub_vms} VMs at once per client while others could take the slots (else all it can use),
  free slots before sweeps and filler, a filler's slot after {sub_preempt} s. A job needing several slots (`cpu` over 2) gets them set aside on one VM.
- `sweep` / `filler`: the treadmill's chains / its soundness fill. Never pick these for your own work.

**Fallback:** boat first. An item boat fails (a VM lost, a row missing, no VM ready) is retried on boat, on another
VM, up to 3 times; only then does it run once on Modal. `--backend modal` stays on Modal. GPU jobs (`"gpu"`) go to
RunPod first, then Modal.

**Single attempt (native):** add `--strict-once --max-attempts 1 --backend boat` for an ordinary jobs batch,
or use `boat.session(..., strict_once=True, max_attempts=1)`. There is at most one physical attempt per
persisted item: no requeue/replacement/fallback on loss or preemption. Handoff observes the original attempt.
A lost attempt returns terminal `status:"LOST"` with VM/boot/launch identity. Reconnects are recorded in
append-only custody; strict close synchronously reaps, retains journals and reports `cleanup_complete`.
Never interpret incomplete cleanup as success or retry LOST under a new batch ID. Zero launches are possible
on admission/preparation failure. Full custody, hash-binding and cleanup contract: `docs/strict-once.md`.

**Shell hygiene:** one background waiter per thing, killed by its exact PID; never `pkill` or `pgrep -f` loops.

**Cleanup:** `supercomputer status` (the pool), `supercomputer reap` (dead clients' leases, orphaned Modal apps and
pods); `modal app list` after any Modal fallback. Never stop or delete VMs another client or project leased.

**Money:** no limits, be smart. No VM-hour or dollar caps: warm slots and lower classes first, then free starts,
then paid ones (~$0.03-0.07). The pool grows only when queued work beyond held and coming capacity pays for a new VM
(the larger start-or-hold opportunity cost divided by its VM-hour price), gates for what finishes them within {gate_horizon} min; idle VMs go; chains stop when they stop gaining.
No VM ceiling ({max_vms}); {bringup} VMs coming up at once is only the scheduler's own load guard.
`supercomputer starts` and the digest report spend as information. {limits}

**boat's terms:** busy, never parked, with one deliberate exception: a project with no treadmill keeps one warm VM
(the default floor) for up to 45 min after its last real work (the hold), so its bursts don't churn starts; that VM idles between
them (`supercomputer policy floor_by_image=<image>=0` drops it for a project finishing up). VMs above the treadmill's
floor ({floor}) run filler up to {hold} min after their last real work, then go; a released VM stops 60 s later. No metering tricks, one account, no shared keys.
Leave free starts for the account's other projects (the reserve), and never touch their VMs.

**Data:** volumes live on boat (`supercomputer store push|pull|ls`), named snapshots with sha256 manifests, mirrored
to RunPod and Modal on demand, pulls cached by hash.

**Explicit roots:** an explicit nested non-repository root is refused with both the requested and enclosing root
named; it is never silently widened to the parent checkout. An omitted root keeps the usual current-repository
selection. Submission logs show the effective root, file count and link count.

**Output files (native boat jobs):** write artifacts to the reserved `$SUPERCOMPUTER_OUTPUT_DIR`, not stdout,
base64 in JSON, `/tmp`, or the working tree (those directories are cleaned after the job). The directory is private
to the job. Regular files, including subdirectories, are copied by SHA-256 into immutable spools before cleanup;
symlinks and special files are not exported. There is no output-file size limit that rejects a job; available disk
and transport capacity still apply. Example command:
```sh
mkdir -p "$SUPERCOMPUTER_OUTPUT_DIR/proofs"
cp proof.bin "$SUPERCOMPUTER_OUTPUT_DIR/proofs/proof.bin"
```
`supercomputer run --jobs jobs.json --out runs/x` downloads files to `runs/x/JOB.files/<relative-path>`, verifies
SHA-256 before accepting each file, and retains a manifest. `collect()` remains bounded stdout/stderr only.
Programmatic sessions add `output_files_ref`, `output_files_manifest`, and `output_files_dir` to the result row.
Large Python return values (>32 KiB) and rows (>64 KiB) are complete JSON files, not truncated: rows carry
`result_ref`/`row_ref` plus downloaded `result_file`/`row_file`. Small returns are unchanged. `Session.call()` returns
a reference/file envelope for a large return; explicitly parse that file only if your client has enough memory.
Poll responses are paged (128 rows and about 512 KiB maximum), chunks are at most 256 KiB, and clients download
before closing the batch. Native controller capability `output_files_protocol:1` / `result_files_protocol:1` is
required for these features, but controller support alone does not prove worker support. For artifact or large-return
jobs, declare `"required_agent_capabilities":{"output_files_protocol":1,"result_files_protocol":1}` in each job
and run with `--backend boat`. Placement and prelaunch checks require actual agent ping inventory; old/unknown
agents cannot satisfy the declaration. Busy legacy agents are not upgraded; an actual idle upgrade excludes
new placement. Jobs without the declaration keep legacy behavior. Never infer capabilities from a source hash
or assume Modal fallback exports artifacts. See `docs/agent-capabilities.md` for the complete recipe and
`docs/result-files.md` for transport/reference formats.

**Recover a finished admission:** `supercomputer batch fetch BATCH_ID --out NEW_DIRECTORY`
fetches retained rows, logs and exported files by ID with SHA verification, without starting a scheduler,
submitting, closing or reaping. Requires controller `batch_fetch_protocol:1`; missing/incomplete history
fails explicitly. The same output directory resumes the same admission generation. See `docs/batch-fetch.md`.

**The treadmill:** sweeps of unsolved tasks, soundness fuzz and mutants, and an idea queue:
`supercomputer queue add jobs.json [--name N] [--meta k=v]`, `queue ls`, `queue results <name>`; `supercomputer
digest` (VM-hours and dollars by class, starts per 100 items, waits by class, gaps between waves).

**The scheduler** uses the configured `live/controller.json` by default. `SUPERCOMPUTER_LOCAL=1` explicitly
opts into this machine; an unavailable controller never silently starts a local scheduler. Without a configured
controller it runs on this machine and owns the pool: one per pool (a lock), started by the first batch, and
after a laptop sleep it renews its VMs and adopts any left without an owner. `supercomputer status` (the pool, the
policy generation, code hashes, orphans), `supercomputer policy [k=v ...]` (one policy file, in force within a tick),
`supercomputer reap`. A new install hands the pool to the new code by itself: nothing lost, no VM let go.
