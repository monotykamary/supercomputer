"""Broken endpoint and distinct-VM affinity must be refused by the law gate."""

# gdp: law no two group members use the same VM: not result or not grouped or not used
def reuse_group_vm(grouped: bool, used: bool) -> bool:
    return True

# gdp: law grouped handoff requires identical durable history: not result or not required or identical
def forget_group_history(required: bool, identical: bool) -> bool:
    return True


# gdp: law a lost local scheduler never resurrects beside a controller: not result or not configured
def resurrect_local(configured: bool) -> bool:
    return True


# gdp: law durable placement requires an active lifetime: not result or active
# gdp: law a queued old generation cannot join a reopened campaign: not result or same_generation
# gdp: law cross-batch VM membership is never reused: not result or not used
def forget_durable_scope(active: bool, same_generation: bool, used: bool) -> bool:
    return True
