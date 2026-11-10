"""The fixed growth horizons, kept as refused regressions (slot-seconds)."""


# gdp: law no needless start when held pool clears within break-even: work > slots * 1500 or not result
def horizon_900(work: float, slots: int) -> bool:
    if work < 0 or slots < 0:
        raise ValueError("nonnegative work and slots required")
    return work > slots * 900


# gdp: law a gate never waits past its horizon for want of a start: work <= slots * 900 or result
def horizon_3600(work: float, slots: int) -> bool:
    if work < 0 or slots < 0:
        raise ValueError("nonnegative work and slots required")
    return work > slots * 3600


# gdp: law gate planning uses the remaining deadline whenever a new item can finish: remaining < bring_up + item_s or result == remaining
def reset_gate_horizon(remaining: float, bring_up: float, item_s: float) -> float:
    """Before: every replan granted another whole 15 minutes, including bring-up."""
    if remaining < 0 or bring_up < 0 or item_s < 0:
        raise ValueError("nonnegative gate times required")
    return max(900, bring_up + item_s)
