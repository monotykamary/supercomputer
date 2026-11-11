"""Unsafe alternatives kept for Main's gdp-wired refutation gate; never shipped."""


# gdp: law retained evidence requires owned cancellation: not result or not retained or strict
def accepts_retention_without_ownership(strict: bool, retained: bool) -> bool:
    return True  # broken: retained=True, strict=False cannot obtain an owned reap


# gdp: law strict mode accepts only one attempt: not result or not strict or limit == 1
def accepts_many_attempts(strict: bool, limit: int) -> bool:
    return True


# gdp: law strict submits have one transport attempt: not strict or result == 1
def resubmits_strict(strict: bool, legacy_attempts: int) -> int:
    return legacy_attempts


# gdp: law strict loss never requeues: not strict or not result
def requeues_strict_loss(strict: bool) -> bool:
    return True


class ResetsOnRequeue:
    # gdp: law strict items have at most one lifetime placement: placements <= 1
    def __init__(self):
        self.placements = 0
        self.queued = True

    def place(self) -> None:
        if self.queued:
            self.placements += 1
            self.queued = False

    def requeue(self) -> None:
        self.queued = True


class AcknowledgesBeforeStartSettles:
    # gdp: law positive cancel waits for active start: not reaped or not inflight
    def __init__(self):
        self.inflight = True
        self.reaped = False

    def cancel(self) -> None:
        self.reaped = True


class TrustsUnboundReceipt:
    # gdp: law completion requires matching identity and namespace absence: not result or (identity_matches and namespace_absent)
    def confirm(self, ok: bool, identity_matches: bool, namespace_absent: bool) -> bool:
        return ok
