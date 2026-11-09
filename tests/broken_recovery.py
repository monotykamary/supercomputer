"""The pre-11-08 canary checked only VMs, never missing/unclassified batches."""
# gdp: law no actions with a missing live admission: not result or missing == 0
# gdp: law no actions with an unexpected live admission: not result or extra == 0
# gdp: law no actions with any unclassified admission: not result or unclassified == 0
def vm_only_canary(missing: int, extra: int, unclassified: int) -> bool:
    return True
