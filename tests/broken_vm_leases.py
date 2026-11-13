"""Kept lease regressions: each law must be refuted, never accepted or unknown.

Positive proofs are extracted from the shipped agent.py via rules/agent.py.
These are deliberately broken guards, not an alternative production model.
"""



# gdp: law a previously activated VM can resume without a new birth nonce: not (same_vm and activated and not initial_boot) or not result
def replay_birth_window_on_resume(fresh: bool, same_vm: bool, activated: bool, initial_boot: bool) -> bool:
    return fresh


# gdp: law a fresh unbound VM requires the birth nonce: not fresh or same_vm or result
def skip_birth_nonce_on_fork(fresh: bool, same_vm: bool, activated: bool, initial_boot: bool) -> bool:
    return fresh and not activated


class BrokenMixedOwners:
    def __init__(self):
        self.mine = 0
        self.foreign = 0

    # gdp: law no VM runs items of two owners at once: foreign == 0 or mine == 0
    def start(self):
        self.mine += 1  # broken: forgot the foreign == 0 start guard

    def transfer(self):
        self.foreign += self.mine
        self.mine = 0


class BrokenStaleEpoch:
    def __init__(self):
        self.holder = "owner"
        self.boot = "boot"
        self.vm_id = "vm"
        self.epoch = 1

    # gdp: law a stale epoch start is refused: not result or (by == holder and b == boot and e == epoch and vm == vm_id)
    def start(self, by: str, b: str, e: int, vm: str) -> bool:
        return by == self.holder and b == self.boot and vm == self.vm_id  # broken: epoch omitted


class BrokenNonholderStop:
    def __init__(self):
        self.holder = "owner"
        self.boot = "boot"
        self.vm_id = "vm"
        self.epoch = 1

    # gdp: law a nonholder never fences stop: not result or by == holder
    def fence_stop(self, by: str, b: str, e: int, vm: str) -> bool:
        return b == self.boot and e == self.epoch and vm == self.vm_id  # broken: owner omitted


class BrokenRestoredIdentity:
    def __init__(self):
        self.holder = "owner"
        self.boot = "boot"
        self.vm_id = "vm"
        self.epoch = 1

    # gdp: law a reboot or fork invalidates the copied lease: not result or (b == boot and vm == vm_id)
    def accepts(self, by: str, b: str, e: int, vm: str) -> bool:
        return by == self.holder and e == self.epoch  # broken: boot and sandbox identity omitted
