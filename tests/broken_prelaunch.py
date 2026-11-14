"""Unsafe pre-launch decisions retained for the refutation gate."""

# gdp: law permanent authentication refusals never retry: not result or (status != 400 and status != 401 and status != 403)
def retries_every_http_error(status: int) -> bool:
    return status >= 400


# gdp: law untyped errors never retry: network or status != 0 or not result
def trusts_error_message(network: bool, status: int) -> bool:
    return True



# gdp: law a start request permanently forbids retry: not result or not requested
def retries_after_request(complete: bool, requested: bool) -> bool:
    return complete


# gdp: law retry requires complete evidence: not result or complete
def retries_missing_journal(complete: bool, requested: bool) -> bool:
    return not requested


# gdp: law cancellation forbids retry: not result or not closed
def retries_closed_admission(closed: bool) -> bool:
    return True
