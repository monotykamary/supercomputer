"""Cold tiering and the controller's disk guard: what may go cold, and what new work a nearly full disk refuses."""


# gdp: law open admissions are never tiered: not result or closed
# gdp: law pending cleanup is never tiered: not pending or not result
# gdp: law held batches are never tiered: not held or not result
# gdp: law batches being read are never tiered: not reading or not result
# gdp: law a closed idle batch may be tiered: not closed or pending or held or reading or result
def tier_allowed(closed: bool, pending: bool, held: bool, reading: bool) -> bool:
    return closed and not pending and not held and not reading


# gdp: law the treadmill's batches go first: not low or result == 0
# gdp: law real work goes after: low or result == 1
def tier_order_low_first(low: bool) -> int:
    return 0 if low else 1


# gdp: law below every threshold nothing is refused: used_pct >= filler_pct or used_pct >= all_pct or not result
# gdp: law past the last threshold everything is refused: used_pct < all_pct or result
# gdp: law the treadmill's work is refused first: not low or used_pct < filler_pct or result
# gdp: law real work is refused only past the last threshold: low or used_pct >= all_pct or not result
def admission_refused(low: bool, used_pct: float, filler_pct: float, all_pct: float) -> bool:
    return used_pct >= all_pct or (low and used_pct >= filler_pct)


# gdp: law nothing is tiered below the threshold without age: used_pct >= tier_pct or aged or result == 0
# gdp: law past both thresholds recent batches go too: used_pct < urgent_pct or used_pct < tier_pct or result == 2
# gdp: law past the threshold old batches go: used_pct < tier_pct or used_pct >= urgent_pct or result == 1
# gdp: law aged batches go on age alone: not aged or result >= 1
def tier_mode(used_pct: float, tier_pct: float, urgent_pct: float, aged: bool) -> int:
    """0: nothing; 1: closed batches past the age; 2: every closed batch (urgent)."""
    if used_pct >= urgent_pct and used_pct >= tier_pct:
        return 2
    if used_pct >= tier_pct or aged:
        return 1
    return 0
