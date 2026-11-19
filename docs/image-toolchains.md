# Native image toolchains

A Boat image name identifies a snapshot, not the runtime selected by a job. A
provider base tool, a different file at the same location, or a PATH override
can change the effective version without changing the snapshot or the worker
agent. Pin important tools in the project’s `supercomputer.toml`:

```toml
[boat]
path = ["/opt/node/bin", "$HOME/.bun/bin"]

[boat.tools]
node = { version = "v24.22.1" }
git = {}  # observe without enforcing a version

[boat.images.runner.tools]
clang = {}   # additional tools for jobs specifying image = "runner"
```

Declare external executables with a read-only `--version` command, not shell
builtins. A pin is the exact trimmed output, including a leading `v`
if it prints one. Add `sha256 = "<64 hex digits>"` beside `version` to pin the
resolved executable file as well. The worker never installs or upgrades the tool;
provision it in the image’s layers and put it on the job PATH. An image named
`runner` above is a job profile on the same snapshot. A profile or hook can add
a declaration, but cannot erase a pin or replace it with a conflicting pin.

Declarations require `toolchain_protocol: 1` on the controller and an actual
worker advertising the capability. Unsupported schedulers/workers cannot run
those jobs silently. A missing, mismatched or unverifiable pin exits **78**
before the payload starts. Existing job-level [Bun/Lean runtime pins](runtime-pins.md)
remain supported and are checked independently.

## Receipts and mixed batches

Updated native workers record the initial PATH, worker boot ID, the executable
selected on PATH, the symlink-resolved file, and the version for each declared
tool. Node is always observed on updated native workers, even if undeclared;
missing undeclared Node is a valid observation. A declared tool’s probe has a
bounded time and output. The receipt records errors or missing measurements
rather than assuming a common runtime. Preflight has its own bounded budget;
the payload watchdog starts afterward.

`supercomputer run` writes per-job receipts and a batch summary to
`<out>/toolchain.json`. A batch with different fingerprints exits **3**, even if
its native job exit codes are all zero; those codes in `summary.tsv` are
unchanged. The fingerprint compares PATH, selected and resolved paths, versions,
and hashes where measured. Extra optional measurements alone do not make a
batch mixed. Different PATH values alone do, even if intentional; use a single
execution profile for a comparable experiment.
`boots_observed` counts distinct measured boots, not placed jobs. Incomplete
coverage (including older workers during rollout) is visible and not a claim
of consistency. `supercomputer batch fetch` puts the assessment in `fetch.json`
and the job receipts in `rows.jsonl`. Fetch preserves evidence and does not
fail just because a batch was mixed. Python sessions expose each row’s
`toolchain` field and can aggregate with `BatchToolchains` in
`supercomputer.toolchain`.

This checks the launch environment after mounts, working directory and env
assembly, not an arbitrary tool invoked later in the payload. Shell `BASH_ENV`, `EXECIGNORE`,
and exported functions shadowing observed tools make the affected launch
unverified; pins refuse applicable shell startup overrides. `run --env PATH=`
sets a literal launch PATH (no shell expansion). `[run].pre` is shell code,
part of the payload: it and the job command can later change PATH, install
tools, or invoke a different absolute path. For comparable
results, do not do that for a declared tool. The launch check is not a security
boundary and does not attest shared libraries or the whole VM.
