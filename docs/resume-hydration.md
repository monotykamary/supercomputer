# Resumed workers must finish disk hydration before bootstrap

`boat.wait_running()` requires all three observations before returning a worker:

1. Provider state is `ready`, `idle`, or `running`.
2. Provider inventory explicitly reports `hydrated: true`.
3. The existing `true` exec probe succeeds.

A missing, null, malformed, or false hydration flag is not readiness. The check
uses the existing cached provider inventory and the caller's existing timeout;
it does not extend the scheduler's 480-second bring-up/breaker window or add a
new start, retry, provider fallback, lease mutation, or job attempt. The same
helper also protects dedicated-session bootstrap. Warm agent RPC placement and
strict-once ownership remain unchanged. Hydration completion is necessary, not
a guarantee that a later bootstrap cannot fail.

## Incident evidence (2026-11-11/12 UTC)

The controller on `6b8028b15595` recorded these resumed workers on the correct
image `sc-f0d8daeee6c3`:

| Worker | Start recorded | Bootstrap transport error | Provider hydration completed |
|---|---|---|---|
| `bx_y7c7y4yr` | 23:56:33 | 23:58:12, exit 143 | 00:03:17.653 |
| `bx_ewuz4zau` | 23:58:52 | 00:00:31, exit 143 | 00:06:31.822 |

The first caused the 23:58:13 breaker trip. They are not the earlier
`bx_w8mvtuxd`. Read-only inspection found old agent files and released leases
from an earlier boot, not newly installed agents; model inventory was never
reached. The errors occurred roughly 100 seconds after the starts, consistent
with the 90-second bootstrap exec limit plus overhead. Exit 143 alone does not
identify the historical signal sender; no provider command-timeout receipt was
retained in the scheduler log.

The defect is accepting a trivial exec as full disk readiness: Python staging
and agent startup can require pages still being hydrated. Provider hydration
for both workers completed within eight minutes of the respective start, but
after the scheduler had already failed their bootstrap. The resume TTL fix
still applies; this change does not replace it or alter its 900-second backstop.

## Regression coverage

`tests/test_hydration.py` runs the real wait loop with a virtual clock: delayed
hydration, the unchanged eight-minute timeout, unknown/malformed hydration,
non-running states, and failed exec probes. `resumed_hydration` in
`tests/scenarios.py` runs the real scheduler against a provider where a resumed
worker answers `true` immediately but Python bootstrap fails until hydration
finishes. It requires the original worker's single resume, no premature
bootstrap, one original strict attempt, and no breaker trip.

The pure readiness decision is proved in `supercomputer/rules/boat.py`; broken
variants retain both the missing-hydration and missing-running-state defects.
