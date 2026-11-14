"""A pre-launch retry never crosses the durable start-request boundary."""


# gdp: law not started requires complete reserved evidence: not result or (complete and reserved)
# gdp: law a requested launch is never not started: not result or not requested
# gdp: law complete absence of a request proves not started: not (complete and reserved and not requested) or result
def prelaunch_not_started(complete: bool, reserved: bool, requested: bool) -> bool:
    return complete and reserved and not requested


# gdp: law retries require complete prelaunch evidence: not result or (complete and reserved)
# gdp: law a start request permanently forbids retry: not result or not requested
# gdp: law only transport failures retry: not result or transient
# gdp: law cancellation and shutdown forbid retry: not result or (not closed and not stopped)
# gdp: law preparation retries respect the existing budget: not result or failures < limit
def prelaunch_can_retry(complete: bool, reserved: bool, requested: bool, transient: bool,
                        closed: bool, stopped: bool, failures: int, limit: int) -> bool:
    return (complete and reserved and not requested and transient and not closed
            and not stopped and failures < limit)
