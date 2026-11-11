"""Old mistakes retained for law-gate regression."""

# gdp: law full threshold alerts: full < 10 or result
def misses_full_threshold(delta: int, full: float, some: float) -> bool:
    return delta > 0 or full > 10 or some >= 20

# gdp: law reset never produces growth: current >= previous or result == 0
def reset_as_growth(previous: int, current: int) -> int:
    return max(0, previous - current)
