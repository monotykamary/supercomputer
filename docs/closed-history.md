# Closed batch history and strict-close recovery

## Retention

An ordinary close no longer deletes the controller's batch history. Ordinary
and successfully reaped strict batches retain their original admission
metadata, rows, available logs/result files, and placement/start/cancellation
journals for **48 hours after the first successful close** by default.

```sh
supercomputer policy history_retention_hours=72
supercomputer batch fetch BATCH_ID --out recovered/BATCH_ID
```

The policy accepts finite nonnegative hours. Zero disables post-close reads.
It applies to future closes; reads, retries, restarts, and policy changes do not
extend an existing deadline. Existing completed strict admissions are migrated
only after the successor has been activated (its old parent has committed by
exiting), never while producing the readiness ACK. If no original close
timestamp exists, retention starts then.

A legacy parent's handoff may still name already archived closed batches.
The successor verifies their per-batch distinct-VM history against retained,
SHA-checked audit snapshots and includes it in the compatibility ACK, without
loading those batches as work. Missing or mismatched evidence fails closed.
This keeps the exact-history guard intact and avoids changing the admission
files beneath a live parent that may need to resume after a rejected candidate.
History already deleted by an older ordinary close cannot be reconstructed.

Closed records use `queue/BATCH_ID.closed`, not the active `.json` suffix, and
are never re-enqueued. Content-addressed audit references freeze the snapshot,
close receipt, rows, placements, start intents/responses, and cancellations.
The ordinary close still kills asynchronously and **does not attest namespace
cleanup**. A start intent or successful RPC response is not proof that a payload
ran; do not infer a physical attempt count from transport records alone.

The close first durably writes a non-executable fence, then publishes the
archive. Interrupted publication finishes on restart; it never reopens work.
Closed history and payloads are included in state backups. Expired reads fail
explicitly, and a single maintenance worker scans at most 128 manifests per
minute. It detaches expired artifacts under the scheduler lock, then deletes
outside the lock. Partial cleanup is retryable. A tiny ID/generation tombstone
remains after payload expiry to prevent reuse; this is not retained job history.
**Pending strict cleanup is not archived or expired.** Its identity and journals
remain protected until positively reaped.

A fetch verifies every downloaded payload and original row page, and succeeds
only with every terminal row and required retained log stream. It does not pin
history against expiry or administrative deletion: a race fails, not silently
returns partial success. Retaining evidence cannot recreate missing outputs.

## Explicit strict-close recovery (issue #1)

A transient close response or snapshot failure remains `cleanup_complete:false`.
Recover **the same admission**, without submitting or cancelling again:

```python
report = original_session.recover_close(batch_id, timeout=30, attempts=3)
assert report['read_only']
if not report['cleanup_complete']:
    raise RuntimeError('cleanup still unconfirmed; retain custody')
```

For a client process that has exited, use its original local custody directory:

```sh
supercomputer batch recover-close BATCH_ID --timeout 30 --attempts 3
```

The CLI loads the original item names and observed placement bindings from
`LIVE/custody/BATCH_ID.jsonl`; it does not instantiate a session or start a
scheduler. New custody journals record these bindings. Older journals without
original item names, refused admissions, torn history, and conflicting observed
identities cannot be certified through this resume path. Preserve original
custody for audit instead of substituting names supplied only by a controller.

Recovery reads only `/close-receipt` and `/snapshot`. Each explicit attempt has
one transport attempt per read, using the remaining timeout budget; there are
no hidden read retries, payload starts, uploads, fallback, replacement VM, or
second close. Every recovery attempt/result is appended to custody. The existing
`_valid_close` validator remains authoritative: exact batch, item partition,
VM, boot, run name, launch identity, one attempt, and positive cleanup are still
required. Missing, contradictory, incomplete, or expired evidence stays false.
The CLI exits nonzero for unconfirmed cleanup. Callers must inspect the returned
`cleanup_complete`, not equate successful transport with successful cleanup.

Capabilities: `closed_history_protocol:1`, `close_recovery_protocol:1`, and the
existing `batch_fetch_protocol:1`. Ordinary request/close retries and the strict
single-attempt execution contract are otherwise unchanged.

## Cold tiering of output files and the controller's disk guard

Exported output files (the payloads named only inside a row's output-file
manifest) of **closed, terminal** batches may go cold. Rows, logs, journals,
close receipts, snapshots and the manifests themselves never leave the
controller, so history pages, audits and `batch recover-close` read exactly
what they read before. Failed rows' files (failure receipts), open admissions,
pending strict cleanup, batches with a `queue/BATCH.hold` marker and batches
read in the last 6 hours are never tiered.

A run streams the chosen files (`tar | zstd`) to a new boat VM, verifies every
member there (each payload hashes to its name; the member set and the archive's
sha256 match what was sent), and refuses an empty compressed stream. It saves
the named snapshot `sc-tier-<UTC>`, boots a second VM from that saved snapshot,
and verifies the same complete member set, payload sizes/hashes and archive
sha256 again. Only after this read-back succeeds are the temporary VMs deleted
and `queue/BATCH.tier` written, a hash-bound reference
(snapshot, archive path, archive sha256, bytes and entry count, moved_at, and
each payload's sha256 and size), before removing the local copies. A read of a
tiered file answers `503 tiered result hydrating`; clients repeat the same read
(up to an hour) while the controller starts a VM from the snapshot, streams the
archive back and installs the files only after the archive's sha256, size and
entry count and every payload's own sha256 match. A missing or tampered archive
is `409 tiered result unavailable`: fail closed, never a partial file. The
reference expires with the batch's closed history. New zero-byte archive
references are rejected. Legacy zero-byte references remain readable for
audit/recovery, but cannot authorize deleting or rebinding local originals.
A failed verification leaves the controller's original payloads untouched;
it does not repair or replace an existing archive.

| policy | default | effect |
| --- | --- | --- |
| `tier_disk_pct` | 70 | tier closed batches older than `tier_age_hours`, the treadmill's (sweep, filler) first, then oldest close first |
| `tier_urgent_pct` | 80 | tier every closed batch, whatever its age |
| `tier_age_hours`, `tier_min_gb` | 24, 2 | below the threshold, age alone triggers a run once at least 2 GB are due |
| `tier_max_gb` | 40 | at most this much per run (one snapshot per run) |
| `disk_alert_pct` | 85 | alert, and the `ctl_disk` health target breaches |
| `disk_refuse_filler_pct` | 90 | new sweep/filler admissions refused (`disk_full: true`) |
| `disk_refuse_pct` | 97 | every new admission refused |

```sh
supercomputer batch tier                  # the plan: a dry run
supercomputer batch tier --run [--max-gb 20] [--min-age-hours 24] [--urgent]
supercomputer batch tier-status           # disk, runs, hydrations
supercomputer batch hydrate BATCH_ID      # restore ahead of a fetch (a fetch also does it by itself)
```

Capabilities: `result_tiering_protocol:1`, `disk_guard_protocol:1`. Tier
snapshots are not deleted automatically; once every reference into one has
expired it can be removed by an operator.
