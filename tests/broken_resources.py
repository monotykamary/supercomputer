"""Resource-limit regressions: every law must be refuted."""

# gdp: law no acceptance above the requested cap: not result or effective <= requested
def over_limit(requested: int, effective: int) -> bool:
    return True

# gdp: law observed affinity must match: not result or affinity_matches
def ignore_affinity(affinity_matches: bool) -> bool:
    return True

# gdp: law missing enforcement cannot pass: not result or enforced
def ignore_enforcement(enforced: bool) -> bool:
    return True

# gdp: law cgroup-only requests cannot fall back: not require_cgroup or not result
def unsafe_fallback(require_cgroup: bool) -> bool:
    return True
