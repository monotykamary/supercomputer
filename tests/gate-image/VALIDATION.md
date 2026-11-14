# Dedicated image validation — 2026-11-14

The previously started native Boat build was recovered, not restarted. The
installer and image salt were unchanged during recovery.

- Image: `sc-gate-tools-img-0990ccce1cdc`
- Named snapshot: `4c4d41ff-f8cc-4292-922b-f2da48db45b3`
- Source builder: `bx_pcqj9wpb`
- Exact named snapshot observed `ready`: `2026-11-14T03:07:14Z`
- Native fenced builder cleanup: completed; subsequent exact-ID provider lookup
  returned `404 not_found`. No other VM was manually stopped or deleted.

The original build log records both pinned archive checks passing, actual ELF
binaries, Bun executing JavaScript, and Lean compiling a proof using its full
installed distribution. The archive digests in `upstream-assets.json` are **not**
executable hashes. Installed executable SHA-256 values, verified again in the
fresh fixture job:

| Executable | SHA-256 |
| --- | --- |
| Bun 1.4.3 | `7ce7d6b654eddeec20afddf2396317fba6dc19ab52788a27e894e1806bf8f24b` |
| Lean 4.34.0 | `e8baaa71855a616dc351028f3ad2200051b0671f423a1696a100e809302d5550` |

## Fresh complete fixture run

Command (host orchestration only; both Python files execute on Boat):

```sh
bash tests/gate-image/gate.sh run "$PWD/tests/gate-image/runs/fixtures-20261114T0308Z"
```

- Batch: `20261114-030743-c966gv`
- Uploaded tree: `7e4daebe2e53` (220 files)
- Backend: Boat, class `gate`; no fallback or rebuild
- Worker: `bx_wgkef4x7`, type `large`
- Job: `real-toolchain-fixtures`, exit `0`
- `tests/test_runtime_pins.py`: **14 tests, OK, no skips**
- `tests/test_strict_environment.py`: **8 tests, OK, no skips**

Both fixture files, all assertions, and production code are unchanged. The real
compiler acceptance paths and the deliberate mismatch/rejection paths passed.

## Local retained artifacts (ignored, not included in this commit)

Relative to this directory:

- `runs/fixtures-20261114T0308Z/summary.tsv`
- `runs/fixtures-20261114T0308Z/placement.json`
- `runs/fixtures-20261114T0308Z/real-toolchain-fixtures.log`
- `runs/fixtures-20261114T0308Z/real-toolchain-fixtures.files/` — complete verbose
  logs for both files, `status.tsv`, executable hashes, archive pins, version
  outputs, environment and Lean `Init.olean` digest
- `runs/recovery-20261114T0308Z/` — original build log, finite snapshot-wait log,
  exact target-ready receipt, native builder-cleanup receipt and run log

These paths are local evidence for this run, not a claim that ignored artifacts
are available from a fresh clone.
