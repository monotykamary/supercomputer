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
