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
