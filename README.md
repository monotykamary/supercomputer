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
- **Images**: the repo's (`[instabox] setup`, saved by `supercomputer image build`), and named ones a job picks with
  `"image"`. A named image with its own `setup` and `data` is built on the repo's: `image build NAME` copies each local
  file or folder of `data` to its path on the box, runs the setup, and saves the snapshot under a name that hashes them.
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
| with peers (no controller), work is never left with no box open: the last box is replaced, never just closed | `launch` |

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

## With no controller at all (experimental)

```sh
supercomputer launch --jobs jobs.json --ledger OWNER/REPO       # prints OWNER/REPO#BATCH; this machine may go
supercomputer batch fetch --ledger OWNER/REPO#BATCH --out runs/x
```

The batch's log lives on a branch `sc/<batch>` of a GitHub repo (`src/ledger.ts`). Every box runs `supercomputer peer`
(`src/peer.ts`): it replays the log through the batch rule and appends what the rule accepts with GitHub's
`createCommitOnBranch` and its `expectedHeadOid`, so of two appends at one point exactly one lands (GitHub's REST ref
update takes non-fast-forwards, so it is not used). Whatever the boxes do at once, the log is one run of the rule, and
its laws hold of it. Boxes report their own exits, store each job's log and outputs in the ledger with its answer,
start the queued jobs that fit on them, find other boxes gone (their attempts retried), open boxes for work none can
take, and go when nothing is left for them. What an idle box does (finish, stay to watch the others, hand its slot on,
close) is a proven gdp-wired decision (`src/idle.ts`), and the rule's `peers` mode keeps the last box while work is
left. Boxes get the GitHub token and an instabox key as instabox proxy Secrets (`SC_LEDGER_TOKEN`, `SC_INSTABOX_KEY`):
placeholders in the box, the real values sent only to api.github.com and api.instabox.sh.

What is not proven: that a box is alive. A box is found gone only when instabox says so, five minutes after it was
claimed at the earliest; a box that is up but whose peer is stuck holds its jobs until `delete-after` (6 h).

## Install

```sh
./install.sh            # this checkout's commit into ~/.local/share/supercomputer/installs, ~/.local/bin/supercomputer
```

It needs `bun` and a signed-in `instabox` (`instabox login`, or `INSTABOX_API_KEY`).

**From the Python supercomputer (0.1).** 1.0 replaces it: boat, Modal, the treadmill and RunPod are gone, and so are
`--backend`, `--class` and the `[boat]`, `[modal]`, `[treadmill]`, `[gpu]` and `[runpod]` sections (refused, not
ignored). `uv tool uninstall supercomputer`, then install as above. `uv tool install git+https://github.com/monotykamary/supercomputer`
now installs only a pointer here; the Python version stays at `git+https://github.com/monotykamary/supercomputer@v0.1.0-python`.
