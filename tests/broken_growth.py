"""The fixed growth horizons, kept as refused regressions (slot-seconds)."""


# gdp: law no needless start when held pool clears within break-even: work > slots * 2700 or not result
def horizon_900(work: float, slots: int) -> bool:
    if work < 0 or slots < 0:
        raise ValueError("nonnegative work and slots required")
    return work > slots * 900


# gdp: law a gate never waits past its horizon for want of a start: work <= slots * 900 or result
def horizon_3600(work: float, slots: int) -> bool:
    if work < 0 or slots < 0:
        raise ValueError("nonnegative work and slots required")
    return work > slots * 3600
