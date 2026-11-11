"""Hot-update guards used by the scheduler, not a model of OS/network latency."""

# gdp: law an update never discards an unacknowledged running identity: not result or items
# gdp: law an update never loses a held VM: not result or pool
# gdp: law an update never loses an admission: not result or admissions
def handoff_ready(pool: bool, admissions: bool, items: bool) -> bool:
    return pool and admissions and items


# gdp: law readiness requires every offered VM: not result or covered
# gdp: law readiness never commits after observed lease loss: not result or not authority_lost
def handoff_pool_ready(covered: bool, authority_lost: bool) -> bool:
    return covered and not authority_lost


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


# gdp: law startup always gets at least ten seconds: result >= 10000
# gdp: law startup never holds placement beyond the two minute cap: result <= 120000
# gdp: law VM work gets its budget up to the cap: result >= min(120000, 10000 + 100 * max(0, vms))
# gdp: law item work gets its budget up to the cap: result >= min(120000, 10000 + 20 * max(0, items))
# gdp: law measured load gets four times its budget up to the cap: result >= min(120000, 10000 + 4 * max(0, load_ms))
def handoff_budget(vms: int, items: int, load_ms: int) -> int:
    return min(120000, 10000 + 100 * max(0, vms) + 20 * max(0, items) + 4 * max(0, load_ms))


# gdp: law a startup timeout never permanently rejects code: not expired or not result
# gdp: law a definitive failed readiness rejects code: expired or result
def handoff_reject(expired: bool) -> bool:
    return not expired


# gdp: law a permanently rejected code never retries: not rejected or not result
# gdp: law a retry waits for its cooldown: now >= retry_after or not result
# gdp: law eligible code retries after cooldown: rejected or now < retry_after or result
def handoff_retry_ready(now: float, retry_after: float, rejected: bool) -> bool:
    return not rejected and now >= retry_after


# gdp: law a candidate cannot wait beyond its deadline: now < deadline or result
# gdp: law a candidate is not expired before its deadline: now >= deadline or not result
def handoff_expired(now: float, deadline: float) -> bool:
    return now >= deadline


# gdp: law a rolled-back update keeps the original owner: accepted or result == previous
# gdp: law an accepted update uses the acknowledged owner: not accepted or result == candidate
def handoff_owner(previous: int, candidate: int, accepted: bool) -> int:
    return candidate if accepted else previous
