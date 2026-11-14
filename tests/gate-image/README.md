# Dedicated real-toolchain fixture image

Opt-in **test infrastructure**, not the production image or controller. It fixes
missing `~/.bun/bin/bun` and Lean files in the real-image fixtures without changing
runtime verification, expected versions, fixture code, scheduler rules, or
`tests/laws.toml`. Do not use this config for a production rollout.

## Build and run (noninteractive)

From an isolated checkout based on the candidate revision:

```sh
supercomputer --skill
bash tests/gate-image/gate.sh build
bash tests/gate-image/gate.sh run /absolute/path/to/fresh-gate-results
```

The wrapper does only orchestration on the host. Installation, compilation, and
both complete fixture files run on Boat. The run forces `--backend boat` (no
Modal fallback), class `gate`, and `OWNED_LIFECYCLE_BOAT_TEST=1`. It refuses image
overrides and sets `SUPERCOMPUTER_NO_BUILD=1` for runs: a missing or pending
image fails closed instead of implicitly starting another build. Both files run
even if the first fails; a failing file or any skipped
test fails the job. It does not run collectors, scenario/simulator suites, or
project proofs. No existing VM is explicitly restarted/stopped/deleted. The
native image builder creates and deletes its own temporary builder. Do not run
pool-wide cleanup, rollout, sync, or teardown for this gate.

### Recovering an interrupted image build

Do not rerun `build` while the provider is saving an earlier attempt. Inspect the
exact `boat-build-<builder-id>.json` record and `builds/<image-name>.log` under
Supercomputer's live directory. `boat --json snapshots` must show that exact
named image with `status: "ready"` and the expected `sourceSandboxId` before
running fixtures. A builder's older `snapshotAvailable: true` or completed base
snapshot is not evidence that the named image is ready. Use a finite read-only
wait; do not bump the salt, retry the snapshot, or start a replacement builder.
After readiness, only the exact owned builder may receive the native fenced
`delete_vm` cleanup; never act on another pool/project VM. If cleanup is refused,
retain the build record and report it instead of bypassing the ownership fence.

## Image construction contract

This uses the repository's real native Boat snapshot API, not a Dockerfile:

- `supercomputer/config.py`: `Project.layer_scripts()` loads `hooks.layers(root)`.
- `supercomputer/boat.py`: `layers()` hashes the salt and complete ordered script
  bytes; `build_layer()` uses `boat new`, runs the installer on its owned builder,
  checks credentials, and saves with `boat snapshot`; `ensure_image()` reuses a
  ready matching snapshot.
- `README.md`, "A repo's config", documents the public layer/config interface;
  `boat snapshot --help` documents named snapshots.

The explicit `sc-gate-tools` / `sc-gate-tools-img` prefixes and image salt separate
this image family from `supercompu-img-*`. The single hook returns the complete
installer text so changing it changes the image identity. Dependencies from apt
and Boat's default base follow their current repositories; this is a reproducible
**toolchain recipe**, not a claim of a bit-for-bit pinned OS base. Runtime archives
are pinned by exact SHA-256, not just release tag. Never replace hashes with hashes
of newly downloaded content automatically.

## Real tools and evidence

`upstream-assets.json` records exact upstream API metadata. `install.sh` checks
both archives before extraction, installs the entire Lean distribution (including
libraries), executes Bun JS and a Lean proof, and checks the exact versions and
Lean prefix. The baseline x86-64 Bun release avoids a host AVX requirement.

Installed paths:

- `/home/user/.bun/bin/bun` — actual Bun 1.4.3 executable;
- `/home/user/.elan/toolchains/leanprover--lean4---v4.34.0/bin/lean` — actual
  Lean 4.34.0 compiler, with the distribution's `lib`, `include`, etc.;
- `~/.elan/bin/{lean,lake,leanc,leanmake}` — direct symlinks to distribution
  binaries where present, not Elan proxy programs.

The config puts Bun and the actual Lean toolchain `bin` before `~/.elan/bin` and
system paths. No shell startup file is required. Built-image receipts live at
`~/.local/share/sc-gate-image/`: archive pins, actual executable hashes, and
version outputs. The runner verifies executable hashes again and exports these
receipts, environment, an `Init.olean` digest, both complete verbose test logs,
and `status.tsv` via `$SUPERCOMPUTER_OUTPUT_DIR`. Supercomputer downloads them
under `real-toolchain-fixtures.files/`, alongside its placement and result
receipts. Executable hashes are deliberately separate from archive hashes; runtime
pins must use the former, never an archive digest or Elan proxy hash.

The pre-existing negative/unit fixtures may themselves make fake tools to test
rejection; they are unmodified. The installed tools and real-image acceptance
paths use only the verified upstream binaries. No skip, stub, relaxed assertion,
or pinned-hash bypass is introduced here.
