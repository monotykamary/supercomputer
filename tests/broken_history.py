"""Deliberately broken history decisions; every law must be refuted."""

# gdp: law expired history is not readable: now < expires or not result
def history_visible(closed: bool, now: float, expires: float) -> bool:
    return closed


# gdp: law pending cleanup is never expired: not pending or not result
def history_expirable(closed: bool, pending: bool, now: float, expires: float) -> bool:
    return closed
