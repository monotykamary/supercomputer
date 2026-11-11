# Fetch a finished batch without closing it

```sh
supercomputer batch fetch 20261111-120000-example --out recovered/example
```

This recovers an **existing, completed scheduler admission** after its original
client exits. It never submits work, starts a scheduler, closes/cancels a batch,
reaps a worker, or switches providers. It does not wait for running work. A
missing admission or a completed admission with missing rows is an error, not
an empty successful fetch.

The connected controller must advertise `batch_fetch_protocol: 1`; older
controllers must be upgraded first. `--timeout SECONDS` bounds each network
request (default 120 seconds), not the job's execution deadline.

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

## Limits

Fetching does not pin journals against another client's concurrent close or
administrative deletion. Read-only pagination does not renew the original
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
