"""Regressions refused by the same hot-update laws."""

# gdp: law an update never discards an unacknowledged running identity: not result or items
def handoff_ready(pool: bool, admissions: bool, items: bool) -> bool:
    return pool and admissions


# gdp: law a candidate cannot wait beyond its deadline: now < deadline or result
def handoff_expired(now: float, deadline: float) -> bool:
    return now > deadline


# gdp: law a rolled-back update keeps the original owner: accepted or result == previous
def handoff_owner(previous: int, candidate: int, accepted: bool) -> int:
    return candidate
