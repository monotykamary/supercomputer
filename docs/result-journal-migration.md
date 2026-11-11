# Result-journal migration and hot updates

A legacy queue may hold much more result history than its number of running
items suggests. The bounded JSON reader prevents bulk-memory growth, but parsing
that history again during every candidate startup can exhaust the readiness
budget. Migration therefore belongs to **stage**, not the handoff freeze.

## Preparation and commit

1. `scheduler stage` compiles/imports its immutable code snapshot, then prepares
   every extant admitted `.rows` journal, including retained/closed admissions.
   The old scheduler keeps serving; its original journals remain byte-identical.
2. A private `.journal-cache.json` in the batch's `.results` directory records a
   compact JSON-lines payload, source inode/generation, complete-prefix length,
   source SHA-256, strictness, row count and whether canonical compaction is needed.
   Publication is atomic and fsynced. Invalid preparation refuses publication of
   the new install manifest. No readiness deadline is raised.
3. Only after preparation succeeds does stage publish `install.json`, pointing
   at the immutable snapshot rather than a mutable package directory. A candidate
   verifies cached prefixes and payloads, reads compact projections, and parses
   only the append tail. Source replacement, truncation, same-size rewriting,
   corrupt cache data or incompatible strictness invalidate the cache; it is an
   accelerator, never an alternative authority.
4. After the legacy parent exits and the candidate activates, it atomically
   replaces affected canonical `.rows` journals with the complete compact row
   sequence, under the request/scheduler/batch locks. It does not start deferred
   workers or remove the passive mutation guard before this step. Failed atomic
   publication retains the original and reports a deferred-compaction error.
   Later boots read the small canonical journals, without repeating conversion.

A rejected candidate never rewrites the legacy parent's journals. Full results
remain in immutable content-addressed payloads; projections retain their
references, routing metadata and order. Existing valid known-hash payloads are
reused without re-downloading or re-hashing; unknown input must still be hashed
once to establish its identity. The store assumes its published files remain
immutable (same-size external mutation is outside this trust boundary).

## Installation safety

Replacing a package directory with `uv tool install --force --reinstall`, or
checking out another commit in a running client's source tree, can leave Python
with a mixture of already imported and newly imported modules. Do not use those
operations on live installations. The coordinated rollout builds separate
per-version environments, stages from the new environment, and atomically
switches the executable entry point. Old environments stay intact for running
processes and hand-back. Eager result-transport imports are additional protection,
not a replacement for immutable installation or pre-freeze preparation.

A pre-reference downgrade remains blocked while reference-aware admissions or
payload directories remain. Do not delete payloads to bypass this guard.

## Verification

- `tests/test_resultjournal.py`: cache integrity, append/replace/truncate races,
  strict journals, interrupted publication, compaction and memory bounds.
- `tests/test_journal_handoff.py`: stage-before-publication, unchanged legacy
  journals before commit, tail retention, activation fencing and failure safety.
- `tests/test_client_install.py`: a mid-batch process survives removal/replacement
  of the source package and downloads its original results.
- `tests/real_queue_handoff.py`: an isolated inherited-listener handoff over a
  copied real queue, including preparation responsiveness, failure/retry, bounded
  canonical journals and an independent restart. It explicitly uses a zero-VM
  fake backend: it measures real-state startup/HTTP/handoff, not live VM network
  performance. Run it as a native supercomputer job; never point it at live state.
