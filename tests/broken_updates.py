"""Regressions refused by the same hot-update laws."""

# gdp: law a live guarded parent keeps its old-readable journals: not guarded or not parent_alive or not result
def compact_before_parent_commit(parent_alive: bool, guarded: bool, blocked: bool) -> bool:
    return not blocked

# gdp: law uncertain recovery never rewrites journals: not blocked or not result
def compact_uncertain_recovery(parent_alive: bool, guarded: bool, blocked: bool) -> bool:
    return not guarded or not parent_alive

# gdp: law a poll wait never exceeds the server bound: result <= max(0.0, limit)
def uncapped_poll_wait(requested: float, limit: float) -> float:
    return max(0.0, requested)


# gdp: law read-only exports never hold the mutation lock while building: not read_only or not result
def archive_under_mutation_lock(read_only: bool) -> bool:
    """The old /state route compressed the entire archive under this lock."""
    return True


# gdp: law a consumed sample never advances the handoff streak: fresh or result == 0
def double_count_handoff(previous: int, fresh: bool, hard_faults: int, idle: int) -> int:
    if hard_faults > 0:
        return 2
    return min(2, max(0, previous) + 1) if idle > 0 else 0


# gdp: law a new missing VM or orphan rejects on the first check: not fresh or hard_faults <= 0 or result == 2
def ignore_missing_vm(previous: int, fresh: bool, hard_faults: int, idle: int) -> int:
    return min(2, max(0, previous) + 1) if fresh and idle > 0 else 0

# gdp: law a hard handoff fault never passes the canary: not result or not hard_fault
def accept_hard_handoff(elapsed: float, watch_s: float, pending: int, hard_fault: bool) -> bool:
    return elapsed >= watch_s and pending == 0


# gdp: law only HTTP 4xx responses are client faults: not result or (status >= 400 and status < 500)
def server_fault_as_client(status: int) -> bool:
    return status >= 400


# gdp: law frozen or stopped schedulers never begin a mutation: not result or (not stopping and not handing_off and not passive)
def mutate_while_frozen(stopping: bool, handing_off: bool, passive: bool) -> bool:
    return not stopping


# gdp: law an update never discards an unacknowledged running identity: not result or items
def handoff_ready(pool: bool, admissions: bool, items: bool) -> bool:
    return pool and admissions


# gdp: law a candidate cannot wait beyond its deadline: now < deadline or result
def handoff_expired(now: float, deadline: float) -> bool:
    return now > deadline


# gdp: law a rolled-back update keeps the original owner: accepted or result == previous
def handoff_owner(previous: int, candidate: int, accepted: bool) -> int:
    return candidate


# gdp: law an empty unavailable VM never blocks handoff: occupied or not unavailable or not result
def require_stopped_empty_vm(occupied: bool, unavailable: bool) -> bool:
    return True


# gdp: law a running or unresolved item always requires its VM: not occupied or result
def drop_occupied_unavailable_vm(occupied: bool, unavailable: bool) -> bool:
    return not unavailable


# gdp: law a rejected handoff resumes its quiesced parent: accepted or not quiesced or result
def wedge_after_rejection(accepted: bool, quiesced: bool) -> bool:
    return False


# gdp: law an accepted handoff never resumes its parent: not accepted or not result
def resume_after_acceptance(accepted: bool, quiesced: bool) -> bool:
    return quiesced


# gdp: law a guarded candidate stays passive while its parent lives: not guarded or not parent_alive or not result
def candidate_runs_before_commit(parent_alive: bool, guarded: bool) -> bool:
    return True


# gdp: law startup always gets at least ten seconds: result >= 10000
def fixed_four_second_deadline(vms: int, items: int, load_ms: int) -> int:
    return 4000


# gdp: law startup never holds placement beyond the two minute cap: result <= 120000
def unbounded_startup_budget(vms: int, items: int, load_ms: int) -> int:
    return 10000 + 100 * max(0, vms) + 20 * max(0, items) + 4 * max(0, load_ms)


# gdp: law a startup timeout never permanently rejects code: not expired or not result
def blacklist_a_timeout(expired: bool) -> bool:
    return True


# gdp: law a retry waits for its cooldown: now >= retry_after or not result
def retry_without_cooldown(now: float, retry_after: float, rejected: bool) -> bool:
    return not rejected


# gdp: law readiness never commits after observed lease loss: not result or not authority_lost
def ignore_readiness_lease_loss(covered: bool, authority_lost: bool) -> bool:
    return covered


# gdp: law every measured status breach advances regardless of baseline: not measured or healthy or result == previous + 1
def ignore_status_after_unhealthy_predecessor(previous: int, measured: bool, healthy: bool) -> int:
    return 0


# gdp: law unknown status never invents a breach: measured or result == 0
def synthetic_status_breach(previous: int, measured: bool, healthy: bool) -> int:
    return previous + 1 if not healthy else 0


# gdp: law a first measured status breach does not reject: not measured or healthy or previous != 0 or result == 1
def reject_first_status_breach(previous: int, measured: bool, healthy: bool) -> int:
    return 2 if measured and not healthy else 0


# gdp: law healthy status clears consecutive breaches: not healthy or result == 0
def keep_old_status_breach(previous: int, measured: bool, healthy: bool) -> int:
    return previous
