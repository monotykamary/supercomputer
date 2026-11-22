# supercomputer

A repo's jobs on [instabox](https://instabox.sh) boxes, from a laptop that may close at any moment. TypeScript on
Bun. Every decision it makes is a [gdp-wired](https://github.com/monotykamary/gdp-wired) rule, proven, and that rule
is what runs.

```sh
cd your-repo
supercomputer run --jobs jobs.json --out runs/x
```

`supercomputer --skill` prints the agents' guide ([SKILL.md](SKILL.md)): the jobs file, results, exit codes, sizes,
GPUs, images, caches and `supercomputer.toml`.

## How it works

There is no controller. A batch is this client, its boxes, and `<out>/batch.json`:

- **The tree**: what git tracks (with submodules) plus untracked files git doesn't ignore, less `[instabox] ignore`,
  sent once per box as a tarball named by its hash.
- **A job** runs detached on its box (`setsid`), with its log, exit code and `$SUPERCOMPUTER_OUTPUT_DIR` under
  `~/sc/run/<batch>/<job>` until they are fetched and their sha256 checked. The client only places jobs and polls.
- **Packing**: jobs pack onto a box by vCPUs and memory; a box takes the next queued job that fits once a slot frees.
- **The warm pool**: a box with nothing on it that no queued job can use pauses (it then costs only its disk). The
  next batch resumes a paused box of the same size and image in seconds before it creates one.
  `supercomputer reap --keep 30m` deletes paused boxes past their keep.
- **Picking up**: `batch.json` holds the jobs and the log of requests the batch rule accepted. `supercomputer batch fetch <out>` after the client died
  fetches what finished meanwhile, watches what still runs, and tries again what was lost with its box.

## The batch, as a proven rule

`src/batch.ts` is the batch as a gdp-wired rule: its jobs and boxes as maps of records (a job queued, running,
ended, done or lost; a box's size, load, outputs not yet fetched, its kind), and a request for every act: add a job,
open a box, start a job on it, take its exit, take its fetched outputs, retry or drop an attempt, close a box,
finish. `src/engine.ts` runs that rule's compiled machine; `src/run.ts` only does the I/O, asks the rule before each
act and acts only when it accepts. There is no second copy of the decisions. `batch.json` keeps the log of accepted
requests, and picking a batch up replays it through the rule.

Its laws, proven with `proveSteps(batch, laws, stepLaws, {from: 'any'})` (proven and checked by the kernel, nothing
assumed), in about 3 seconds:

| Law | |
|---|---|
| live boxes stay within the cap, and count the open boxes | `--max-boxes` |
| no box runs past its vCPUs, or its memory | packing |
| attempts stay within their budget; strict-once work is tried at most once | retries |
| a group's members run on different boxes; a timing box runs one job in its life | `distinct_vm_group`, `--timing` |
| a closed box holds nothing | nothing is lost by pausing or deleting |
| the batch passes only with nothing left to run or fetch, and no job failed or lost | exit 0 |
| an answer is never taken back; a lost job stays lost; a verdict is final | step laws |
| a job is tried again only with an attempt left | step law |

`BUGS` in `src/batch.ts` are the bugs these laws rule out, each a variant of the same rule (a box opened past the cap,
jobs packed by vCPUs alone, an answer retried, strict-once work tried twice, a box closed with outputs on it, a group
twice on a box, a batch passed with a failure), and every one is refuted by a run from the start. `crossCheck`
compares the builders' callbacks with the proven expressions on concrete runs. The warm pool's one decision (delete
a paused box only past its keep) is a gdp-wired decision in `src/pool.ts`, proven the same way.

```sh
bun install
bun run laws    # the laws proven, every bug refuted, the builders cross-checked
bun test        # that, the engine, jobs files and config, and the runner end to end on a stand-in instabox (Linux)
bun run check   # types
```

## Install

```sh
./install.sh            # this checkout's commit into ~/.local/share/supercomputer/installs, ~/.local/bin/supercomputer
```

It needs `bun` and a signed-in `instabox` (`instabox login`, or `INSTABOX_API_KEY`).
