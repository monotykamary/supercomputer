"""Hot-update guards used by the scheduler, not a model of OS/network latency."""

# gdp: law an update never discards an unacknowledged running identity: not result or items
# gdp: law an update never loses a held VM: not result or pool
# gdp: law an update never loses an admission: not result or admissions
def handoff_ready(pool: bool, admissions: bool, items: bool) -> bool:
    return pool and admissions and items


# gdp: law a running or unresolved item always requires its VM: not occupied or result
# gdp: law an empty unavailable VM never blocks handoff: occupied or not unavailable or not result
# gdp: law a usable VM is still required: unavailable or result
def handoff_required(occupied: bool, unavailable: bool) -> bool:
    return occupied or not unavailable


# gdp: law a rejected handoff resumes its quiesced parent: accepted or not quiesced or result
# gdp: law an accepted handoff never resumes its parent: not accepted or not result
# gdp: law a parent that never quiesced needs no resume: quiesced or not result
def handoff_resume(accepted: bool, quiesced: bool) -> bool:
    return quiesced and not accepted


# gdp: law a guarded candidate stays passive while its parent lives: not guarded or not parent_alive or not result
# gdp: law a committed candidate activates when its parent exits: parent_alive or result
# gdp: law a cold start needs no parent commit: guarded or result
def handoff_activate(parent_alive: bool, guarded: bool) -> bool:
    return not guarded or not parent_alive


# gdp: law a candidate cannot wait beyond its deadline: now < deadline or result
# gdp: law a candidate is not expired before its deadline: now >= deadline or not result
def handoff_expired(now: float, deadline: float) -> bool:
    return now >= deadline


# gdp: law a rolled-back update keeps the original owner: accepted or result == previous
# gdp: law an accepted update uses the acknowledged owner: not accepted or result == candidate
def handoff_owner(previous: int, candidate: int, accepted: bool) -> int:
    return candidate if accepted else previous
