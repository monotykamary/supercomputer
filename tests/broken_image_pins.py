"""Negative controls: either dropped protection must violate its real law."""

# gdp: law every live batch stays protected: not live_batch or result
def drops_live(live_batch: bool, recent_reference: bool) -> bool:
    return recent_reference

# gdp: law every recent reference stays protected: not recent_reference or result
def drops_recent(live_batch: bool, recent_reference: bool) -> bool:
    return live_batch
