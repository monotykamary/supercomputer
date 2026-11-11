"""The pre-11-08 canary checked only VMs, never missing/unclassified batches."""
# gdp: law no actions with a missing live admission: not result or missing == 0
# gdp: law no actions with an unexpected live admission: not result or extra == 0
# gdp: law no actions with any unclassified admission: not result or unclassified == 0
def vm_only_canary(missing: int, extra: int, unclassified: int) -> bool:
    return True


# gdp: law every persisted durably closed record is quarantined: not persisted or not closed or result
def replay_closed_admission(persisted: bool, closed: bool) -> bool:
    return False


# gdp: law no record without durable close authority is quarantined: closed or not result
def quarantine_unclosed_admission(persisted: bool, closed: bool) -> bool:
    return persisted
