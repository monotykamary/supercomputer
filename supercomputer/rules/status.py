"""Publication ordering and honest age for the read-only status view."""

# gdp: law a status age is never negative: result >= 0
# gdp: law elapsed status time is preserved: now_ms < published_ms or result == now_ms - published_ms
# gdp: law a backwards clock never makes a negative age: now_ms >= published_ms or result == 0
def status_snapshot_age(now_ms: int, published_ms: int) -> int:
    return max(0, now_ms - published_ms)


# gdp: law an older capture never overwrites a newer publication: not result or started_ms >= published_ms
# gdp: law a current capture may be published: started_ms < published_ms or result
def status_snapshot_newer(started_ms: int, published_ms: int) -> bool:
    return started_ms >= published_ms
