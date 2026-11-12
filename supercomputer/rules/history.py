"""Time-bounded read-only history; unresolved ownership is never garbage."""

# gdp: law only closed history is readable: not result or closed
# gdp: law expired history is not readable: now < expires or not result
def history_visible(closed: bool, now: float, expires: float) -> bool:
    return closed and now < expires


# gdp: law pending cleanup is never expired: not pending or not result
# gdp: law open admissions are never expired: not result or closed
# gdp: law expiry never runs early: now >= expires or not result
def history_expirable(closed: bool, pending: bool, now: float, expires: float) -> bool:
    return closed and not pending and now >= expires
