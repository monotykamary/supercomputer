"""The treadmill's, fanout's, RunPod's and the controller's broken rules, kept: each the bug a law of
supercomputer/rules/{treadmill,fallback,ops}.py rules out, as the code had it, with that law stated again here.
`tests/check_laws.sh` asserts every law below comes back broken with the run that breaks it."""
from supercomputer.rules.fallback import AutoFallback, Outcome
from supercomputer.rules.ops import Cadence, HandoverVM
from supercomputer.rules.treadmill import PendingSlots, SubStatus, SubmissionFate


class OwnExitIsBoatFailure(AutoFallback):
    """fanout.AutoEx before: BoatEx.remote raised alike for a call's own non-zero exit and for boat losing it, and
    AutoEx counted both toward the batch's 3 boat failures: three failing calls sent the rest of the batch to Modal."""

    # gdp: law a call's own failure is never a boat failure: outcome != "own" or boat_fails == old(boat_fails)
    # gdp: law a call's own failure is its result, never retried nor moved: outcome != "own" or (result != "again" and result != "modal") or old(on_modal)
    def outcome(self, outcome: Outcome) -> str:
        if self.on_modal:
            return "modal"
        if outcome == "ok":
            return "done"
        self.boat_fails += 1
        if self.boat_fails < self.tries:
            return "again"
        self.on_modal = True
        return "modal"


# gdp: law a ticket of this machine whose pid is gone is never alive: not result or not same_host or not pid_gone
def ticket_alive_blank_host(same_host: bool, pid_gone: bool) -> bool:
    """treadmill.ls, __main__._reap, volumes.cleanup before: boat.pid_alive(pid, "") (a host of "" is never this
    machine's, so presumed alive): every ticket alive."""
    host_is_ours = False
    return not host_is_ours or not pid_gone


# gdp: law an orphan Modal app (its client's pid gone) is stopped: result or not pid_gone
def reap_modal_blank_host(pid_gone: bool) -> bool:
    """__main__._reap before: its tickets read with pid_alive(pid, ""): no orphan Modal app ever stopped."""
    return not ticket_alive_blank_host(True, pid_gone)


class HandoverPopFirst(HandoverVM):
    """controller.handover before: the VMs popped from this registry, then the controller's written over ssh; an ssh
    that failed left them in neither."""

    # gdp: law a VM handed over is in one registry, never in neither: here or there
    def written(self, remote_ok: bool) -> None:
        if not self.taken:
            return
        self.taken = False
        self.here = False
        self.there = remote_ok


# gdp: law merging two registries loses no start either counted: result >= a and result >= b
def merged_starts_set_union(a: int, b: int) -> int:
    """migrate.merge_registry before: the starts' timestamps merged as a set: note_starts(k)'s k starts at one
    timestamp counted once."""
    if a > 0 or b > 0:
        return 1
    return 0


class RunningLeftRunning(SubmissionFate):
    """Treadmill.step before: a submission a dead treadmill left "running" was taken in as running, and only queued
    ones start: it never ran again."""

    # gdp: law a submission runs only under this treadmill's batch: status != "running" or owned
    def __init__(self, status: SubStatus):
        self.status: SubStatus = status
        self.owned = False


class PendingByPosition(PendingSlots):
    """Batch.held before: the slots still needed summed over items[n_done:], as if rows came back in the items' order:
    a row back took off the slots of the first item in that order, whichever item it was."""

    # gdp: law a row back takes off its own item's slots: not (name in old(pending)) or need == old(need) - old(pending[name])
    def back(self, name: str, first_slots: int) -> None:
        if name not in self.pending:
            return
        self.pending.pop(name)
        self.need -= first_slots


class CheckOnBackup(Cadence):
    """controller.maybe_sync before: the key check keyed to the last backup's time, which a failing backup never
    records: the key checked (ssh, up to 120 s) before every command while backups fail."""

    # gdp: law the controller's key is checked at most hourly: not result or now - old(t_check) >= 3600
    def check_due(self, now: float) -> bool:
        if now - self.t_backup < 3600:
            return False
        self.t_check = now
        return True
