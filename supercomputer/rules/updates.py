"""Hot-update guards used by the scheduler, not a model of OS/network latency."""

# gdp: law no absent record is quarantined: persisted or not result
# gdp: law no record without durable close authority is quarantined: closed or not result
# gdp: law every persisted durably closed record is quarantined: not persisted or not closed or result
def quarantine_closed_admission(persisted: bool, closed: bool) -> bool:
    return persisted and closed

# gdp: law a poll wait is never negative: result >= 0
# gdp: law a poll wait never exceeds the server bound: result <= max(0.0, limit)
# gdp: law a poll never waits longer than the client requested: result <= max(0.0, requested)
# gdp: law a shorter nonnegative request is preserved: requested < 0 or requested > limit or result == requested
def http_poll_wait(requested: float, limit: float) -> float:
    return max(0.0, min(requested, limit))


# gdp: law an observation never belongs to two adjacent check intervals: not result or (sample > since and sample <= now)
# gdp: law every new observation in the interval is read: sample <= since or sample > now or result
def handoff_sample_fresh(sample: float, since: float, now: float) -> bool:
    return since < sample <= now


# gdp: law no transient idle slot breaches before its grace ends: age > grace or result == 0
# gdp: law persistent idle slots breach after the grace: slots <= 0 or age <= grace or result == slots
# gdp: law an idle observation cannot invent slots: result == 0 or result == slots
def handoff_idle_slots(slots: int, age: float, grace: float) -> int:
    return max(0, slots) if age > grace else 0


# gdp: law a consumed sample never advances the handoff streak: fresh or result == 0
# gdp: law a new missing VM or orphan rejects on the first check: not fresh or hard_faults <= 0 or result == 2
# gdp: law one transient idle observation cannot reject: not fresh or hard_faults > 0 or previous > 0 or result < 2
# gdp: law persistent idle across new check intervals rejects: not fresh or idle <= 0 or previous < 1 or result == 2
def handoff_health_streak(previous: int, fresh: bool, hard_faults: int, idle: int) -> int:
    if not fresh:
        return 0
    if hard_faults > 0:
        return 2
    return min(2, max(0, previous) + 1) if idle > 0 else 0

# gdp: law a hard handoff fault never passes the canary: not result or not hard_fault
# gdp: law an unconfirmed idle breach never passes the canary: not result or pending == 0
# gdp: law the canary cannot pass before its watch ends: not result or elapsed >= watch_s
# gdp: law a completed clean watch passes: elapsed < watch_s or pending != 0 or hard_fault or result
def handoff_can_pass(elapsed: float, watch_s: float, pending: int, hard_fault: bool) -> bool:
    return elapsed >= watch_s and pending == 0 and not hard_fault


# gdp: law only HTTP 4xx responses are client faults: not result or (status >= 400 and status < 500)
# gdp: law every HTTP 4xx response is a client fault: status < 400 or status >= 500 or result
def http_client_fault(status: int) -> bool:
    return status >= 400 and status < 500


# gdp: law read-only exports never hold the mutation lock while building: not read_only or not result
# gdp: law mutations remain serialized: read_only or result
def http_request_serialized(read_only: bool) -> bool:
    return not read_only

# gdp: law frozen or stopped schedulers never begin a mutation: not result or (not stopping and not handing_off and not passive)
# gdp: law an active unfrozen scheduler admits mutations: stopping or handing_off or passive or result
def http_mutation_allowed(stopping: bool, handing_off: bool, passive: bool) -> bool:
    return not stopping and not handing_off and not passive


# gdp: law an update never discards an unacknowledged running identity: not result or items
# gdp: law an update never loses a held VM: not result or pool
# gdp: law an update never loses an admission: not result or admissions
def handoff_ready(pool: bool, admissions: bool, items: bool) -> bool:
    return pool and admissions and items


# gdp: law readiness requires every offered VM: not result or covered
# gdp: law readiness never commits after observed lease loss: not result or not authority_lost
def handoff_pool_ready(covered: bool, authority_lost: bool) -> bool:
    return covered and not authority_lost


# gdp: law unoffered registry VMs never delay a handoff: not handoff or offered or not result
# gdp: law every offered VM remains eligible for fenced takeover: not offered or result
# gdp: law a cold restart retains registry takeover: handoff or result
def handoff_registry_vm(handoff: bool, offered: bool) -> bool:
    return not handoff or offered


# gdp: law pending extras are not new handoff orphans: not deferred or acquired or not result
# gdp: law required VMs remain covered by orphan checks: deferred or result
# gdp: law an extra once acquired is covered by orphan checks: not acquired or result
def handoff_orphan_scope(deferred: bool, acquired: bool) -> bool:
    return not deferred or acquired


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


# gdp: law a live guarded parent keeps its old-readable journals: not guarded or not parent_alive or not result
# gdp: law uncertain recovery never rewrites journals: not blocked or not result
# gdp: law committed healthy candidates may compact: blocked or (guarded and parent_alive) or result
def journal_compaction_allowed(parent_alive: bool, guarded: bool, blocked: bool) -> bool:
    return not blocked and (not guarded or not parent_alive)


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


# gdp: law unknown status never invents a breach: measured or result == 0
# gdp: law healthy status clears consecutive breaches: not healthy or result == 0
# gdp: law every measured status breach advances regardless of baseline: not measured or healthy or result == previous + 1
# gdp: law a first measured status breach does not reject: not measured or healthy or previous != 0 or result == 1
def canary_status_streak(previous: int, measured: bool, healthy: bool) -> int:
    return previous + 1 if measured and not healthy else 0
