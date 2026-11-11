"""Regressions refused by the same hot-update laws."""

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
