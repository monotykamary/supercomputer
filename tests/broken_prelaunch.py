"""Unsafe pre-launch decisions retained for the refutation gate."""


# gdp: law a start request permanently forbids retry: not result or not requested
def retries_after_request(complete: bool, requested: bool) -> bool:
    return complete


# gdp: law retry requires complete evidence: not result or complete
def retries_missing_journal(complete: bool, requested: bool) -> bool:
    return not requested


# gdp: law cancellation forbids retry: not result or not closed
def retries_closed_admission(closed: bool) -> bool:
    return True
