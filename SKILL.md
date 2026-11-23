# supercomputer: run a repo's jobs on instabox boxes (agents' guide; `supercomputer --skill` prints it)

**Use it for everything but the extremely light** (tests, checks, Lean, sweeps, timing claims). The machine you are on
runs only git, edits, few-second commands and watching. Tool: github.com/monotykamary/supercomputer.

**Run a jobs file** from any repo (its git root is the tree; `supercomputer.toml` says what the boxes need):

```sh
supercomputer run --jobs jobs.json --out runs/x [--max-boxes 50] [--size large] [--timing] [--strict-once] [--env K=V]
```

- **jobs.json**: `[{"name", "cmd", "cwd"?, "cpu"?, "memory"? (MiB), "timeout"? (s), "env"?, "vm_type"?, "gpu"?, "image"?, "distinct_vm_group"?}]`.
  `cmd` runs under bash in `<tree>/<cwd>`. What instabox can't honour (`cpu_model`, `backend`, `runtime`) is refused,
  not ignored.
- **Results**: `<out>/<name>.log`, `<out>/<name>.files/` (what the job wrote to `$SUPERCOMPUTER_OUTPUT_DIR`, sha256
  checked), `<out>/summary.tsv` (name, exit or `lost`, seconds, box), `<out>/batch.json` (the batch, its plan).
- **Exit**: 0 only when every job answered 0; 3 when any failed or was lost; 1 on a client error. A nonzero native exit
  is data (a failing test), never retried; only a lost attempt (its box gone) is, up to `--max-attempts` (3).
- The tree ships without `node_modules`: start a bun job with `bun install --frozen-lockfile && ...`.
- `--timing`: each job alone on a fresh box of its own (one job per box, ever). `--strict-once`: one attempt.
- `"vm_type"`: `small` 2x4, `default` 4x8, `large` 8x16, `xlarge` 16x32, `2xlarge` 32x64 (vCPU x GiB), or `NxM`.
  Unnamed, jobs pack onto boxes of `[instabox] size` (or the smallest that fits) by vCPUs and memory.
  A snapshot saved size-locked (`instabox snapshot ls`: sizeLocked) makes boxes of its own size only: every job on it
  gets that size, and one it can't hold (its memory plus the box's 512 MiB) is refused when the batch starts.
- `"gpu": "a100"` (`l4`, `t4`, `h100`, `"a100:2"`): a GPU box from instabox's GPU queue.
- `"image": "runner"`: a box of `[instabox.images.runner]` (another snapshot, its own PATH and env).
  An image with its own `setup` (and `data`) is built on the repo's image by `supercomputer image build NAME`.

**Survives this machine going away.** Each job runs detached on its box and keeps its log, exit and outputs there.
Close the laptop mid-batch and the boxes finish their jobs, then pause themselves. Note the `--out` directory, and
pick the batch up later from any checkout of the repo with:

```sh
supercomputer batch fetch runs/x
```

Finished jobs are fetched (not run again), running ones watched where they are, lost ones tried again within their
attempts.

**Boxes and money.** instabox bills a running box by the second and a paused one for its disk only. A batch's boxes
pause into a warm pool when it ends, and the next batch resumes them in seconds before it creates any.
`supercomputer status` lists the pool; `supercomputer reap --keep 30m` deletes paused boxes past their keep (every
`run` reaps on its way out).

**The repo's image.** `supercomputer image build` runs `[instabox] setup` on a fresh box and saves a snapshot named
by the script's hash; put it in `supercomputer.toml`:

```toml
name = "repo"
[run]
pre = "export X=1; "                   # before every job's cmd
[instabox]
setup = "supercomputer/setup.sh"
snapshot = "sc-repo-0123456789ab"
size = "large"
path = ["~/.bun/bin"]
env = { A = "1" }
ignore = ["runs/**"]
[instabox.images.bench]                # "image": "bench": the repo's image plus this setup and data
setup = "supercomputer/bench.sh"       # (`supercomputer image build bench`; then its snapshot = "..." here)
data = { "/bench" = "data/bench", "/data/models.jsonl" = "runs/models.jsonl" }   # box path = file or folder in the repo
[[cache]]                              # build outputs kept on each box between jobs and batches
name = "olean"
dir = "lean"
modules = ["A", "B"]
inputs = "{m}.lean"
outputs = "{m}.olean"
salt = "leanprover/lean4:v4.34.0"
```

**What it decides is proven, and is what runs.** `src/batch.ts` is the batch as a gdp-wired rule (jobs and boxes,
every act a request); the runner steps its compiled machine and acts only on what it accepts. Its laws (at most
`--max-boxes` live, no box past its vCPUs or memory, attempts within their budget, a group's members on different
boxes, an answer never taken back, a box closed only with nothing on it, a pass only with every job answered 0) are
proven from every state they allow (`bun run laws`).
