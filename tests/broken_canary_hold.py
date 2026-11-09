"""The pre-fix idle/filler policy released handoff VMs during a pending canary."""
# gdp: law a pending candidate never retires voluntarily: not watching or not candidate or not result
def retire_during_canary(watching: bool, candidate: bool) -> bool:
    return True
