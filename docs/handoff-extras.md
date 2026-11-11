# Handoff readiness excludes unoffered registry extras

A live scheduler offers its required VM pool and running identities to its successor.
The successor must acquire **every offered VM** with the existing ownership fences,
load every admission, and preserve every running identity before acknowledging readiness.
The acknowledgment is still published only once the inherited listener is serving.
The successor remains passive until its parent commits by exiting.

A stale local registry can also contain VMs the parent does not hold or offer. A provider
exec to one of these VMs can wait for hydration or a transport timeout. Such a VM is not
a dependency of the required-pool acknowledgment:

- During handoff, `_take_pool` leaves eligible unoffered registry entries and their agent
  capabilities untouched. They are recorded as `deferred_extras` in handoff diagnostics,
  not added to `taken` or `missing` in the readiness acknowledgment.
- Ordinary adoption waits for the offered pool's parent to commit before contacting
  extras. It retains the existing agent-health, lease, boot and owner checks.
- A deferred extra that has not been acquired is not reported as an orphan *created by
  this handoff*. Offered VMs, other orphan candidates, and extras once acquired remain
  covered by the existing orphan checks. General pool-health observation is unchanged.
- A cold restart with no live-parent handoff snapshot retains its existing registry
  takeover behavior. No readiness deadline or lease lifetime is increased.

No extra VM is stopped, reaped, resumed, relabeled or granted a lease merely to make a
handoff pass. Required VM or admission failures still fail readiness and roll back.

`tests/test_handoff_extras.py` covers the registry/agent boundary, cold restart,
parent-commit barrier, and orphan scope. The `handoff_blocked_extra` scenario puts a
120-second exec delay on an unoffered stale idle registry VM, checks a sub-five-second
required-pool handoff and a passing canary, and verifies that extra-VM exec happens only
after parent exit with no duplicate workload attempts.
