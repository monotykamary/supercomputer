"""Endpoint affinity: never create a competing implicit local scheduler."""

# gdp: law configured controllers forbid implicit local startup: not result or not configured or explicit_local
# gdp: law an explicit local opt in permits local startup: not explicit_local or result
def local_start_allowed(configured: bool, explicit_local: bool) -> bool:
    return not configured or explicit_local

# gdp: law a lost local scheduler never resurrects beside a controller: not result or not configured
def local_recovery_allowed(configured: bool) -> bool:
    return not configured
