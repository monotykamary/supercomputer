# Strict-once toolchain environment

Strict jobs run as the owned Unix user, with `HOME` taken from that UID's passwd
entry (`/home/user` on the boat image). The supervisor restores HOME and its
parent PATH after sudo/setpriv and before importing the standalone agent. The
PID1 runner independently restores the same passwd home; the payload wrapper
uses it for `$HOME` expansion and does not permit a stale HOME override.

This fixes sudo's `HOME=/root` leaking into strict jobs: the old runner restored
its agent ROOT but cached the wrong HOME, redirecting `.bun`, `.elan`, and `.sc`
lookups under `/root`. Explicit job/project PATH entries retain their ordinary
ordering, now expanded against the correct home.

Within the strict job's private mount namespace, installed `~/.bun`, `~/.elan`,
`~/.sc/agent.py`, and the actual standalone agent file are bound read-only.
They remain visible at their normal paths. Missing toolchains are not installed
or fabricated; preinstalled tools are required. Bun's per-job install cache
remains in private `/tmp`. Toolchain upgrades/installations must happen outside
the strict job, not through its read-only Elan/Bun tree.

Private PID/mount namespaces, pidfd binding, launch nonce validation, admission
authorization, CPU constraints and synchronous reap evidence are unchanged.

`tests/test_strict_environment.py` includes a controlled standard-path fixture
(Bun 1.4.3 and Lean 4.34.0), read-only mount checks, and a separate real-image
acceptance scenario that invokes those installed versions without installing
anything. It also verifies the strict supervisor restores HOME/PATH before
Python imports the agent. Run the native tests on boat, not on the laptop.
