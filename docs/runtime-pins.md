# Job runtime pins (native boat only)

A shell job may request either or both runtimes:

```json
{
  "name": "check",
  "cmd": "bun run check && lean Main.lean",
  "runtime": {
    "bun": {"version": "1.4.3", "sha256": "<64 hexadecimal digits>"},
    "lean": {"version": "4.34.0", "sha256": "<64 hexadecimal digits>"}
  }
}
```

Replace the digest placeholders with the hashes of the actual executable files,
not archives, installers, symlink text, or an elan launcher. Python runner `BOAT`
settings and `Session.call(..., extra={"runtime": ...})` accept the same field.
An absent field preserves existing behavior. Explicit null/empty maps, unknown
runtimes/fields, incomplete pins, and invalid versions/digests are errors.

Pins imply `required_agent_capabilities.runtime_pins_protocol >= 1`; an explicit
empty capability map cannot disable that requirement. The client additionally
requires controller `runtime_pins_protocol: 1` **before uploads/submission**.
Legacy sessions and unsupported backends/images fail closed. There is no Modal
or RunPod fallback for pinned jobs, including missing rows and infrastructure
failure. Reusing a live batch ID with changed/added/removed pins is refused.
Recovery validates the pins again, and handoff refuses runtime-unaware successor
builds, including while retained pinned results remain in the batch inventory.

## What is checked

The worker checks before the original payload executes, in the job's mount
namespace and effective environment/PATH, using an absolute isolated Python
interpreter. It never installs a runtime. Missing executables, version/hash
mismatches, and elan shims fail the job with code 78 and runtime observations.
Elan symlinks, hard links and known copied proxies are refused; Lean also must
identify the selected executable as its own `--print-prefix` compiler. Python
items are checked from `/work`, the same initial directory used by `call()`.
Set the job/image PATH to the actual Lean toolchain `bin`, not `~/.elan/bin`,
for example `/home/user/.elan/toolchains/leanprover--lean4---v4.34.0/bin`.
Strict jobs preserve the owned user's passwd HOME and expose installed toolchains
read-only; see [strict-environment.md](strict-environment.md).

PATH is composed from job/image paths, project paths and the base system path;
project/job environment overrides can also supply PATH. Relative paths depend on
the job's working directory, and namespace mounts/overlays can change resolution.
A hash verified on the controller or outside that namespace is not equivalent.

This is a pre-execution observation, not a sandbox security boundary: it does not
force later shell commands to keep PATH unchanged, attest dependencies, or pin
arbitrary absolute executable paths invoked by the payload. Version output and
executable-file SHA-256 are both required. Successful observations and failure
metadata stay in rows, including bounded projections of large result rows;
large complete rows remain available through their result-file references.
