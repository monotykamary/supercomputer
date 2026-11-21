# supercomputer on instabox (TypeScript)

The same jobs files as the Python supercomputer, run on [instabox](https://instabox.sh) boxes. There's no controller
VM, no treadmill and no start ledger, because what made them necessary on boat is gone:

| On boat | On instabox |
|---|---|
| Starts rationed (free per day, then paid) and often slow (the breaker's "not ready within 8 min") | A box from a template runs in about 8 s, with no start quota |
| No parked VMs allowed, so held VMs ran filler to stay busy | A paused box costs only its disk and resumes in about 2 s: the warm pool is paused boxes |
| A controller owned the pool and spooled every output | Each box holds its jobs' logs, exits and outputs until they are fetched and verified |
| GPUs on RunPod, a second backend | `"gpu": "a100"` (or `l4`, `t4`, `h100`, `"a100:2"`) on the same API |

## Run something

```sh
cd your-repo
bun /path/to/supercomputer/ts/src/cli.ts run --jobs jobs.json --out runs/x [--max-boxes 50] [--size large]
```

- **Jobs file:** the jobs file has the same format: `name`, `cmd`, `cwd`, `cpu`, `memory` (MiB), `timeout` (s), `env`, `vm_type`, `gpu`, `distinct_vm_group`.
  - A field instabox can't honour is refused, not ignored. That covers `cpu_model` (boxes report only "AMD EPYC"), `backend`, `runtime` and a named `image`.
- **Outputs:** `<out>/<name>.log`, `<out>/<name>.files/` (whatever the job wrote to `$SUPERCOMPUTER_OUTPUT_DIR`, sha256-checked) and `<out>/summary.tsv` (name, exit, seconds, where).
- **Exit code:** 0 only when every job answered 0; 3 when any job failed or was lost; 1 on a collector error.
- **The tree:** what git tracks (with submodules) plus untracked files git doesn't ignore, less `supercomputer.toml`'s `ignore`, sent once per box as a tarball named by its hash. It ships without `node_modules`, so start a bun job with `bun install --frozen-lockfile && ...`.
- **Packing:** jobs pack onto a box by vCPUs and memory, like boat's slots. A box takes the next queued job that fits as soon as a slot frees.
- **Warm pool:** a box pauses into the warm pool once no queued job can use it, and the next batch resumes it before creating a new one. `supercomputer reap --keep 30m` deletes paused boxes past their keep; each `run` reaps on its way out.
- **`--timing`:** each job runs alone on a fresh box.
- **`--strict-once`:** one attempt per job, never retried.

## Surviving this machine going away

A job runs detached on its box (`setsid`) and writes its exit code next to its log. The client only places jobs and
polls, keeping its state in `<out>/batch.json`. Close the laptop mid-batch and the boxes still finish their jobs, then
pause themselves after 5 idle minutes. Run the same command with the same `--out` and it picks the batch up:
- **Finished jobs:** fetched, not run again.
- **Running jobs:** watched where they are.
- **Jobs whose box is gone:** queued again, within their attempts.

## The decisions, proven

What the runner decides lives in `src/boxes.ts` and `src/decide.ts` as plain code with `// gdp: law` comments, proven
by [gdp-wired](https://github.com/monotykamary/gdp-wired) from every state the laws allow:

- **Cap:** at most `--max-boxes` boxes are live.
- **Retries:** only a lost attempt is tried again (a native exit, zero or not, is the job's answer), within its attempts, and strict-once work never.
- **Reuse:** a box takes its next job only after the last one's outputs are fetched and match their sha256.
- **Deletion:** a paused box is deleted only past its keep, and never while it holds unfetched outputs.
- **Success:** the batch succeeds only when every job answered and none failed.

`test/broken_*.ts` keeps the bug each law rules out, and every one of them must be refuted.

```sh
bun install
bun run laws    # every law proven and checked, every broken one refuted
bun test        # jobs files, sizes, supercomputer.toml
bun run check   # types
```

## The repo's image

`supercomputer image build` runs `supercomputer.toml`'s `[instabox] setup` script on a fresh box from `template`
(default `universal`: git, Node 22, Bun, Python with uv, Go, Rust, a JDK, a C toolchain, Docker). It saves the
result as a snapshot named by the script's hash, then deletes the box. Point `[instabox] snapshot` at it:

```toml
[instabox]
setup = "supercomputer/instabox-setup.sh"   # installs what [boat.tools] pinned: Node, elan/Lean, ...
snapshot = "sc-gdp-wired-0123456789ab"
size = "large"                              # optional; else [boat] vm_type
```

`[boat]`'s `path`, `env` and `ignore`, and `[run] pre`, carry over.

## Not here yet

- **Lean caches** (`[[cache]]`) and **data mounts** (`mounts`, `store`).
- **Durable distinct groups** across batches. Within a batch, a group's members already run on different boxes.
- **Image pins and toolchain fingerprints.** A snapshot is named by its setup script's hash; nothing checks the tools inside it.
- **Programmatic sessions** (`boat.session`), **queues** and the **digest**.
- **GPU jobs:** wired to instabox's GPU queue but not yet tried on a real GPU.
- **Warm pool sharing:** one client owns its pool (boxes tagged with a hash of user and host). Two batches of one client can race for the same paused box.
