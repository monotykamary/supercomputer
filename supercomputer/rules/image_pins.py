"""Retention decision used by the durable controller image-pin registry."""

# gdp: law every live batch stays protected: not live_batch or result
# gdp: law every recent reference stays protected: not recent_reference or result
# gdp: law only a live or recent reference protects: not result or live_batch or recent_reference
def image_pin_retained(live_batch: bool, recent_reference: bool) -> bool:
    return live_batch or recent_reference
