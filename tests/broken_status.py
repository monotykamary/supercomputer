"""Deliberately wrong status publication/age policies."""

# gdp: law age is never negative: result >= 0
def negative_age(now_ms: int, published_ms: int) -> int:
    return now_ms - published_ms

# gdp: law elapsed status time is preserved: now_ms < published_ms or result == now_ms - published_ms
def hide_staleness(now_ms: int, published_ms: int) -> int:
    return 0

# gdp: law a backwards clock clamps age: now_ms >= published_ms or result == 0
def backward_age(now_ms: int, published_ms: int) -> int:
    return now_ms - published_ms

# gdp: law an older capture never overwrites a newer publication: not result or started_ms >= published_ms
def accept_old(started_ms: int, published_ms: int) -> bool:
    return True

# gdp: law a current capture may be published: started_ms < published_ms or result
def reject_current(started_ms: int, published_ms: int) -> bool:
    return False
