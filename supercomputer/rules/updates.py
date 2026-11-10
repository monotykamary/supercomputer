"""Hot-update guards used by the scheduler, not a model of OS/network latency."""

# gdp: law an update never discards an unacknowledged running identity: not result or items
# gdp: law an update never loses a held VM: not result or pool
# gdp: law an update never loses an admission: not result or admissions
def handoff_ready(pool: bool, admissions: bool, items: bool) -> bool:
    return pool and admissions and items


# gdp: law a candidate cannot wait beyond its deadline: now < deadline or result
# gdp: law a candidate is not expired before its deadline: now >= deadline or not result
def handoff_expired(now: float, deadline: float) -> bool:
    return now >= deadline


# gdp: law a rolled-back update keeps the original owner: accepted or result == previous
# gdp: law an accepted update uses the acknowledged owner: not accepted or result == candidate
def handoff_owner(previous: int, candidate: int, accepted: bool) -> int:
    return candidate if accepted else previous
