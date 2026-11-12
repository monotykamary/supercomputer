"""Worker-local quarantine and conservative legacy ghost recovery."""


# gdp: law quarantined workers do not count toward a pool outage: not result or not quarantined
# gdp: law only ready live workers count toward a pool outage: not result or (ready and not dead)
def worker_outage_member(ready: bool, dead: bool, quarantined: bool) -> bool:
    return ready and not dead and not quarantined


# gdp: law strict ownership is never reclaimed by legacy PID evidence: not result or not strict
# gdp: law ghost reclaim requires a complete process audit and recorded PID: not result or (complete and recorded)
# gdp: law live roots or descendants are never ghosts: not result or (not live and not descendants)
# gdp: law an existing result is never replaced by ghost loss: not result or not result_exists
def ghost_slot(strict: bool, complete: bool, recorded: bool, live: bool, descendants: bool, result_exists: bool) -> bool:
    return not strict and complete and recorded and not live and not descendants and not result_exists


# gdp: law unresolved strict ownership forbids repair: not result or not strict
# gdp: law live or unknown legacy work forbids an agent upgrade: not result or not live_or_unknown or same_agent
def worker_repair_allowed(strict: bool, live_or_unknown: bool, same_agent: bool) -> bool:
    return not strict and (not live_or_unknown or same_agent)
