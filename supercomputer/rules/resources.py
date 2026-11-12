"""Verified admission to a payload after kernel resource-limit readback."""

class ResourceLimitGate:
    # gdp: law acceptance requires a positive requested cap: not result or requested > 0
    # gdp: law acceptance requires a positive effective cap: not result or effective > 0
    # gdp: law acceptance never exceeds the requested cap: not result or effective <= requested
    # gdp: law acceptance requires actual matching affinity: not result or affinity_matches
    # gdp: law acceptance requires a successfully applied mechanism: not result or enforced
    def allow(self, requested: int, effective: int, affinity_matches: bool, enforced: bool) -> bool:
        return requested > 0 and effective > 0 and effective <= requested and affinity_matches and enforced

    # gdp: law a required cgroup never falls back: not require_cgroup or not result
    # gdp: law fallback requires a supported address-space limit: not result or rlimit_available
    def fallback(self, require_cgroup: bool, rlimit_available: bool) -> bool:
        return not require_cgroup and rlimit_available
