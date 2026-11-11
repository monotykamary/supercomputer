# Output files and bounded result delivery

## Returning artifacts from native jobs

Every current native agent gives a job a private, writable `SUPERCOMPUTER_OUTPUT_DIR` environment variable.
The variable is reserved and cannot be redirected by job environment overrides. Write files there before exit:

```sh
mkdir -p "$SUPERCOMPUTER_OUTPUT_DIR/proofs"
cp proof.bin "$SUPERCOMPUTER_OUTPUT_DIR/proofs/proof.bin"
```

The agent streams regular files into immutable, content-addressed payload files before deleting work/tmp.
Nested directories are supported. Symlinks and special files are skipped, not dereferenced. No file-size policy
rejects a legitimate artifact; capacity errors remain real errors and must not be confused with successful delivery.
Artifacts from both successful and unsuccessful jobs can be collected. The controller imports and verifies every
payload before committing the terminal row or acknowledging it to the worker.

`supercomputer run --jobs jobs.json --out runs/x` downloads artifacts before reporting the row, to:

```
runs/x/JOB.files/proofs/proof.bin
```

Each download is SHA-256 verified and atomically published. A manifest is retained under `runs/x/.payloads/`.
Programmatic callbacks receive `output_files_ref`, `output_files_manifest` (local filename) and `output_files_dir`.
`collect()` continues to return bounded stdout/stderr; output files are a separate transport. Only native boat agents
and controllers advertising `output_files_protocol: 1` implement this API; an old controller or Modal fallback is
not an output-file transport.

## Reference format

```json
{"version":1,"sha256":"<64 lowercase hexadecimal characters>","bytes":1048576,"format":"file"}
```

`format` is `file` for arbitrary binary output and `json` for JSON payloads/manifests. References contain no remote
filesystem paths. `output_files_ref` points to a JSON array whose entries add `path`, a safe relative destination,
to the reference above. The manifest can be iterated without loading all entries. Duplicate content shares its hash.

## Large Python returns and rows

Python `result.json` up to 32 KiB retains the existing in-memory `result` behavior. Larger returns are never decoded
by the agent/controller: the row contains `result_ref`, and the client downloads `result_file`. `Session.call()`
returns that reference/file envelope for large values instead of forcing reconstruction in memory. A caller may
explicitly `json.load(open(envelope['result_file']))` if sufficient client memory is available.

A complete row above 64 KiB is spooled as JSON; the bounded projection carries `row_ref` and the downloaded
`row_file`, plus routing/placement metadata and any output manifest reference. Nothing is truncated. Legacy inline
worker responses and old result journals are streamed through the same row-spooling decoder. The exec transport
fallback is also file/chunk backed, so it cannot bypass the bound.

The default client cache for programmatic sessions is `SUPERCOMPUTER_HOME/results/BATCH/.payloads`;
CLI runs use their `--out` directory. Local downloaded files survive batch close and are caller-owned.
A raw protocol client must finish downloading references **before** `/close`; ordinary closed batch spools are
removed. Strict retained admissions keep their spools with their journals. Backup and hot handoff preserve them.
Downgrades to pre-reference controllers are refused while reference-aware admissions or payload directories remain.

## Bounded reads and recovery

`POST /results` accepts `{batch, ref, offset, length}` and returns raw bytes. Authentication is the existing
controller token. A read is capped at 256 KiB; offsets must be nonnegative. The client verifies total size and hash.
`POST /poll` returns contiguous pages: at most 128 rows and 512 KiB of row data plus a small envelope. `next_since`
is the next cursor; `done` is true only when the returned page completes all delivered item rows. Existing clients
that advance by the returned row count remain compatible. References are never expanded by `/snapshot`.

File-backed state archives are reused briefly by concurrent exports. They remain available while a reader owns
them; a later export reclaims only archive files belonging to dead scheduler processes after an abrupt handoff.

Payload publication is atomic and fsynced before row commit/worker ACK. Incomplete/corrupt transfers are not
successful results and do not release their placement. State archives stream from disk; `.payload` files are not
JSON-decoded during capture. This protects result retention and recovery from bulk data memory amplification.

## Persistent journal migration

Staging prepares validated, reusable compact projections before freezing the
parent. Canonical journals are rewritten only after the new owner activates;
failed handoffs leave old-readable journals intact. See
[journal migration and hot updates](result-journal-migration.md) for the format,
installation ordering, downgrade constraints and real-queue regression.

## Regression coverage

`tests/bulk_result_rss.py` exercises a full 1 GiB result, first-class output download, verified byte equality,
restart journal loading, repeated polling and a legacy inline worker response under a 768 MiB address-space limit.
It asserts peak RSS growth below 128 MiB. Other focused tests cover paging, partial/corrupt transfers, unsafe paths,
owner fencing, atomic outputs and backup/restore. Run through native boat, not on the controller being diagnosed.
