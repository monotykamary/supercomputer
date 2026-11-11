"""Resource-pressure decisions, separate from code-health rollback policy."""

# gdp: law reset never produces growth: current >= previous or result == 0
# gdp: law growth is nonnegative: result >= 0
# gdp: law normal growth is the difference: current < previous or result == current - previous
def high_growth(previous: int, current: int) -> int:
    return max(0, current - previous)

# gdp: law any growth alerts: delta <= 0 or result
# gdp: law full threshold alerts: full < 10 or result
# gdp: law some threshold alerts: some < 20 or result
# gdp: law quiet readings do not alert: delta > 0 or full >= 10 or some >= 20 or not result
def pressure_bad(delta: int, full: float, some: float) -> bool:
    return delta > 0 or full >= 10 or some >= 20
