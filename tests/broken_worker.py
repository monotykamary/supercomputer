"""Local faults and stale occupancy must not become global policy."""

# gdp: law quarantined workers do not count toward a pool outage: not result or not quarantined
def count_quarantined(ready: bool, dead: bool, quarantined: bool) -> bool:
    return ready and not dead

# gdp: law live roots or descendants are never ghosts: not result or (not live and not descendants)
def ignore_descendants(strict: bool, complete: bool, recorded: bool, live: bool, descendants: bool, result_exists: bool) -> bool:
    return not strict and complete and recorded and not live and not result_exists

# gdp: law unresolved strict ownership forbids repair: not result or not strict
def upgrade_owned(strict: bool, live_or_unknown: bool, same_agent: bool) -> bool:
    return not live_or_unknown or same_agent

# gdp: law an isolated worker failure is not a pool outage: not result or failing >= 2
def one_worker_outage(ready: int, failing: int) -> bool:
    return ready >= 2 and failing * 2 >= ready
