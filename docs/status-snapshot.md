# Published scheduler status

`POST /status` reads a pre-encoded, immutable snapshot. The response path does
not acquire `Scheduler.lock`, traverse the queue, consult the journal/index,
read the filesystem, or acquire health/archive locks. A dedicated publisher
refreshes approximately once per second; a blocked refresh never holds a
request or discards the last complete view.

All existing status fields remain. The additive `status_snapshot` object has:

- `ready`: whether a complete snapshot has been published (false during an
  incomplete boot, not a claim that missing state is healthy).
- `generation`: monotonically advancing successful publication number.
- `captured_at`: wall-clock time the scheduler state was captured.
- `published_at`: wall-clock time that complete encoded view was published.
- `age_s`: age since capture, measured with the monotonic clock. A blocked or
  failed refresh leaves this age growing; it never relabels old data as fresh.
- `refresh_error`: the last bounded refresh error, cleared on successful publish.

Counts, memory/health summaries and agent metadata refer to the snapshot time.
Consumers needing a fresh observation should check `ready`, `age_s` and the
advancing generation rather than assume every poll recomputes live state.

Status response timings are queued without taking the health lock, then
recorded by the publisher/sampler. The canary's normal `< 1.0 s` status target
remains mandatory even if the predecessor's status history is unhealthy; a
bad baseline is not permission for a slow new status endpoint. The independent
native gate measures full HTTP response latency while an actual idle/handoff
check holds the scheduler lock and while publisher I/O is blocked. Thresholds
are not relaxed, and stale snapshots are explicitly labelled.
