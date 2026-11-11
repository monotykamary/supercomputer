# Controller scheduler memory policy (next release)

The scheduler service uses `MemoryHigh=70%` and `MemoryMax=82%` of the
controller VM's installed physical memory. On a 16 GiB VM these are about
11.2 GiB and 13.12 GiB. Percentages are valid systemd unit syntax for both
directives (see local `systemd.resource-control(5)`, available since systemd
231). `MemoryHigh` throttles/reclaims; `MemoryMax` is a last-resort hard limit
that can invoke an in-unit OOM kill. These are limits, not reservations.

The previous fixed 4 GiB/5 GiB scheduler caps could cause reclaim stalls even
when a larger controller had spare physical memory. The new policy scales
with the VM while leaving nominal headroom for the OS and other services;
it is not a guarantee against OOM or aggregate memory pressure. CPUQuota=250%,
TasksMax=1500, Nice=5 and LimitNOFILE=524288:524288 remain unchanged. The
separate treadmill service retains its existing limits.

## Persistence without a live resource change

`controller setup` generates the scheduler unit with the percentage limits.
`controller stage` writes these persistent scheduler drop-ins:

```ini
# /etc/systemd/system/supercomputer-scheduler.service.d/10-nofile.conf
[Service]
LimitNOFILE=524288:524288
```

```ini
# /etc/systemd/system/supercomputer-scheduler.service.d/20-memory.conf
[Service]
MemoryHigh=70%
MemoryMax=82%
```

Stage then runs `systemctl daemon-reload` and stages the source release, with
the existing hot-handoff capability check and exact source-hash attestation.
It does **not** start, stop or restart a service, call `set-property`, remove
runtime drop-ins, or revert the operator's emergency runtime configuration.
An existing runtime 11G/13G override is intentionally left in place; it can
continue to take precedence. Code hot handoff alone does not imply that these
new cgroup limits have taken effect. Activation and any runtime-override
cleanup require a separate, explicitly authorized operational procedure.
Installing a new base unit also does not override administrator drop-ins.

The scheduler samples cgroup-v2 `memory.events` and `memory.pressure` every five seconds.
It alerts on any new `high` event, `full avg10 >= 10%`, or `some avg10 >= 20%`, with a
90-second, at-most-64-sample window and deduplication until recovery. The first observation
baselines the lifetime event counter; resets do not become synthetic growth. Missing data
is unknown, not zero. Current metrics and headroom appear in `status.health.memory` and
`status.load.memory`. Memory pressure is not an automatic canary rollback criterion.
Sampling in the scheduler cannot guarantee a warning during a sudden total process stall;
out-of-process infrastructure monitoring remains useful.

Bulk result/row data and cached state exports now live in files, not retained Python JSON
objects or archive byte strings. See [output files](result-files.md) for the first-class
artifact API, reference compatibility, bounded polling, and the 1 GiB regression.

No deployment is part of this change. Controller-only regression coverage is
in `tests/test_controller_limits.py`, alongside `test_controller_backup.py`
and `test_controller_fallback.py`; provider/SSH operations are mocked.
