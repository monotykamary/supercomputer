"""Deliberately broken history decisions; every law must be refuted."""

# gdp: law expired history is not readable: now < expires or not result
def history_visible(closed: bool, now: float, expires: float) -> bool:
    return closed


# gdp: law pending cleanup is never expired: not pending or not result
def history_expirable(closed: bool, pending: bool, now: float, expires: float) -> bool:
    return closed


# gdp: law migration never precedes a live parent commit: not result or not guarded or not parent_alive
def migration_before_commit(guarded: bool, parent_alive: bool) -> bool:
    return True


# gdp: law quarantined recovery cannot migrate admissions: not result or not blocked
def migration_during_quarantine(blocked: bool) -> bool:
    return True
