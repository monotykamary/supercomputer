"""Endpoint and per-batch VM affinity, including persistent group history."""

# gdp: law no two group members use the same VM: not result or not grouped or not used
# gdp: law ungrouped placement is unchanged: grouped or result
def distinct_vm_allowed(grouped: bool, used: bool) -> bool:
    return not grouped or not used

# gdp: law grouped handoff requires identical durable history: not result or not required or identical
def distinct_history_matches(required: bool, identical: bool) -> bool:
    return not required or identical


# gdp: law durable placement requires an active lifetime: not result or active
# gdp: law a queued old generation cannot join a reopened campaign: not result or same_generation
# gdp: law cross-batch VM membership is never reused: not result or not used
def durable_vm_allowed(active: bool, same_generation: bool, used: bool) -> bool:
    return active and same_generation and not used


# gdp: law configured controllers forbid implicit local startup: not result or not configured or explicit_local
# gdp: law an explicit local opt in permits local startup: not explicit_local or result
def local_start_allowed(configured: bool, explicit_local: bool) -> bool:
    return not configured or explicit_local

# gdp: law a lost local scheduler never resurrects beside a controller: not result or not configured
def local_recovery_allowed(configured: bool) -> bool:
    return not configured
