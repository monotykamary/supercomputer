# Fetch a finished batch without closing it

```sh
supercomputer batch fetch 20261111-120000-example --out recovered/example
```

This recovers an **existing, completed scheduler admission**, including retained closed history, after its original
client exits. It never submits work, starts a scheduler, closes/cancels a batch,
reaps a worker, or switches providers. It does not wait for running work. A
missing admission or a completed admission with missing rows is an error, not
an empty successful fetch.

The connected controller must advertise `batch_fetch_protocol: 1`; older
controllers must be upgraded first. On refusal the client makes one best-effort,
read-only `/status` request and names both hashes, for example
`serving=e82e84af3cee, installed=667a5a2ab401`. Staged/installed support is not
serving support: verify the active version before retrying. If status is
unavailable, the hashes are explicitly `unknown`; the capability refusal remains.
No batch endpoint is called and no output directory is created by this diagnostic.
`--timeout SECONDS` bounds each network request (default 120 seconds), not the
job's execution deadline.


Closed history is retained for **48 hours after close** by default. Configure
`supercomputer policy history_retention_hours=72` for future closes. See
[closed history and strict-close recovery](closed-history.md) for expiry and
audit guarantees. The original batch ID and admission generation are preserved.

## Files

- `pages/`: original bounded controller row pages, SHA-verified before parsing;
  `fetch.json` records their checksums.
- `rows.jsonl`: every terminal row, one JSON object per line. Large rows and
  returns remain references to complete files in `.payloads/`, not expanded
  into client memory.
- `JOB.files/`: exported output files, as with `supercomputer run`.
- `logs.tar.gz`: the controller's retained per-run stdout/stderr archive. It is
  downloaded in bounded chunks and checked against the controller's SHA-256
  header. Every row with a recorded worker/run identity must have both retained
  `out` and `err` streams; missing streams fail instead of producing an empty
  successful archive. Rows rejected before a worker/run exists need no streams.
  The archive is **not extracted** automatically.
- `fetch.json`: successful completion receipt, including before/after admission
  identity, row count, local row-stream checksum and verified log checksum.
- `batch.json`: the admission identity used to resume partial downloads.

All referenced payloads and output files are checked against their SHA-256
references. Inline rows are verified as part of the original controller pages;
the generated `rows.jsonl` checksum records the transformed local export. Large
referenced row payloads have their own controller-supplied SHA-256. The admission identity and
complete row inventory are checked again before publishing success.

Use an empty output directory. Repeating the command into that same directory
resumes the **same admission generation**, reusing only verified payloads. A
different batch or generation is refused. Partial failures may leave verified
files, but do not publish a successful `fetch.json`. A repeat fetch invalidates
its previous success marker before modifying published artifacts. Symlink destinations
and unsafe row/output paths are refused.

Closed fetches also return an `audit` inventory in `fetch.json`: hash-verified
snapshot, close receipt, frozen rows, and placement/start/cancellation journals.
The files live under `audit/.payloads`; each inventory entry names its local file
and SHA-256 reference. No raw journal is interpreted as permission to execute.

## Limits

Fetching does not pin history against expiry or administrative deletion. An
ordinary close now retains history instead of deleting it; a close racing an
open-admission fetch may still require restarting the fetch against the closed
metadata. Read-only pagination does not renew the original
client heartbeat or postpone automatic abandonment. If the admission changes or disappears during the
transfer, the command fails without closing anything. It cannot recover rows,
logs or private worker `/tmp` files that were already deleted. Only files
originally exported via `$SUPERCOMPUTER_OUTPUT_DIR` are controller artifacts;
private work/tmp directories are normally cleaned when a worker finishes.
Completeness refers to the retained terminal rows and their recorded runs, not
a certificate of every historical physical attempt or retry.

This is different from `supercomputer queue results NAME`, which reports a
named treadmill queue submission. It is also not a new `Session.run`: a new
session is an admission, not a reattachment. Keep the original batch ID.

Programmatic callers can use `supercomputer.batchfetch.fetch(batch_id, out)`.
The returned receipt states `read_only: true` and `closed_by_fetch: false`.
