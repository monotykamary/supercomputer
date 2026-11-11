"""Production admission and reap decisions, extracted directly by gdp-wired.

The host serializes transitions under its scheduler lock and durably records a
placement before sending it. Receipt matching is I/O validation, passed as a fact.
"""

# gdp: law retained evidence is admitted only with owned cancellation: not result or not retained or strict
def retention_supported(strict: bool, retained: bool) -> bool:
    """Legacy has no positive namespace-reap capability; do not admit an unclosable mode."""
    return not retained or strict


# gdp: law strict mode accepts exactly one attempt: not strict or (result == (limit == 1))
def attempt_cap_supported(strict: bool, limit: int) -> bool:
    return not strict or limit == 1


# gdp: law strict submits have one transport attempt: not strict or result == 1
# gdp: law legacy transport retries are unchanged: strict or result == legacy_attempts
def strict_transport_attempts(strict: bool, legacy_attempts: int) -> int:
    return 1 if strict else legacy_attempts


# gdp: law strict loss never requeues: not strict or not result
# gdp: law legacy loss remains retryable: strict or result
def requeue_after_loss(strict: bool) -> bool:
    return not strict


class OwnedLifecycle:
    # gdp: law strict admission permits at most one placement: not strict or placements <= 1
    # gdp: law a cancellation fence never opens: not old(closed) or closed
    # gdp: law a positive cancellation has no start in flight: not reaped or not inflight
    # gdp: law a positive cancellation keeps admission closed: not reaped or closed
    # gdp: law placements never decrease: placements >= old(placements)
    def __init__(self, strict: bool):
        self.strict = strict
        self.placements = 0
        self.closed = False
        self.inflight = False
        self.reaped = False

    # gdp: law closed admission never places: not old(closed) or not result
    def place(self) -> bool:
        if self.closed or (self.strict and self.placements >= 1):
            return False
        self.placements += 1
        self.inflight = True
        return True

    def start_settled(self) -> None:
        self.inflight = False

    def cancel(self) -> None:
        self.closed = True

    # gdp: law reap success requires a matching receipt or no placement: not result or matched or placements == 0
    def confirm(self, matched: bool) -> bool:
        if not self.closed or self.inflight:
            return False
        if self.placements > 0 and not matched:
            return False
        self.reaped = True
        return True
