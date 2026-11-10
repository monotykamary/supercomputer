# supercomputer: run work on boat VMs (agents' guide; `supercomputer --skill` prints it, live values filled in)

**Use it for everything but the extremely light** (tests, checks, Lean, bars, sweeps, timing claims). The laptop runs
only quick bounded checks. Tool: github.com/monotykamary/supercomputer (README and docs/ for depth; state in ~/.local/share/supercomputer). Now: {live_now}

**Run a jobs file** from any repo (its git root is the tree; `supercomputer.toml` says what it needs):
- `supercomputer run --jobs jobs.json --out runs/x [--class C] [--timing] [--data DIR[:DEST]] [--env K=V]`
- jobs.json: `[{"name", "cmd", "cwd"?, "cpu"?, "memory"?, "timeout"?, "image"?, "backend"?, "gpu"?, "env"?}]`;
  `cmd` runs under bash in `<repo>/<cwd>`; logs in `<out>/<name>.log`, `<out>/summary.tsv` (name, exit, seconds, where).
- The tree ships without node_modules: start a bun job with `bun install --frozen-lockfile && ...`.
- `--timing`: each job alone on its VM; `<out>/placement.json` is the label a timing claim needs.
- `--vm-type T` (or a job's `"vm_type"`): every job placed on type T only (a timing claim's fixed hardware); unnamed, the smallest
  free VM that fits, any type.

**Classes** (`--class`, else `$SUPERCOMPUTER_CLASS`; a small batch defaults to interactive, a bigger one to submission):
- `interactive`: quick checks; a slot at once (a filler's or sweep's item is killed and re-queued).
- `gate`: bars and suites; lower slots at once, then only the VMs that finish it within {gate_horizon} min.
- `submission`: experiments; up to {sub_vms} VMs at once per client while others could take the slots (else all it can use),
  free slots before sweeps and filler, a filler's slot after {sub_preempt} s. A job needing several slots (`cpu` over 2) gets them set aside on one VM.
- `sweep` / `filler`: the treadmill's chains / its soundness fill. Never pick these for your own work.

**Fallback:** boat first. An item boat fails (a VM lost, a row missing, no VM ready) is retried on boat, on another
VM, up to 3 times; only then does it run once on Modal. `--backend modal` stays on Modal. GPU jobs (`"gpu"`) go to
RunPod first, then Modal.

**Shell hygiene:** one background waiter per thing, killed by its exact PID; never `pkill` or `pgrep -f` loops.

**Cleanup:** `supercomputer status` (the pool), `supercomputer reap` (dead clients' leases, orphaned Modal apps and
pods); `modal app list` after any Modal fallback. Never stop or delete VMs another client or project leased.

**Money:** no limits, be smart. No VM-hour or dollar caps: warm slots and lower classes first, then free starts,
then paid ones (~$0.03-0.07). The pool grows only when queued work beyond held and coming capacity pays for a new VM
(at least its hold or start-price / VM-hour-price break-even), gates for what finishes them within {gate_horizon} min; idle VMs go; chains stop when they stop gaining.
No VM ceiling ({max_vms}); {bringup} VMs coming up at once is only the scheduler's own load guard.
`supercomputer starts` and the digest report spend as information. {limits}

**boat's terms:** busy, never parked, with one deliberate exception: a project with no treadmill keeps one warm VM
(the default floor) for up to 45 min after its last real work (the hold), so its bursts don't churn starts; that VM idles between
them (`supercomputer policy floor_by_image=<image>=0` drops it for a project finishing up). VMs above the treadmill's
floor ({floor}) run filler up to {hold} min after their last real work, then go; a released VM stops 60 s later. No metering tricks, one account, no shared keys.
Leave free starts for the account's other projects (the reserve), and never touch their VMs.

**Data:** volumes live on boat (`supercomputer store push|pull|ls`), named snapshots with sha256 manifests, mirrored
to RunPod and Modal on demand, pulls cached by hash.

**The treadmill:** sweeps of unsolved tasks, soundness fuzz and mutants, and an idea queue:
`supercomputer queue add jobs.json [--name N] [--meta k=v]`, `queue ls`, `queue results <name>`; `supercomputer
digest` (VM-hours and dollars by class, starts per 100 items, waits by class, gaps between waves).

**The scheduler** runs on this machine and owns the pool: one per pool (a lock), started by the first batch, and
after a laptop sleep it renews its VMs and adopts any left without an owner. `supercomputer status` (the pool, the
policy generation, code hashes, orphans), `supercomputer policy [k=v ...]` (one policy file, in force within a tick),
`supercomputer reap`. A new install hands the pool to the new code by itself: nothing lost, no VM let go.
