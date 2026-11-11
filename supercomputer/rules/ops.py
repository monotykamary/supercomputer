"""The tool's operational decisions: the controller's cadence and its handovers, the registry's merge, tickets of
clients, the commands' choices (controller.py, migrate.py, __main__.py, config.py, runpod.py, modal_run.py, skill.py).

    ticket_alive     a client's ticket (a lease, a Modal app's, a treadmill's status): alive, by its pid where it is this
                     machine's, else presumed alive (another machine's pid can't be checked)
    Cadence          the controller's backup (at most hourly, unless forced) and its key check (at most hourly)
    sync_due         main synced to the controller when it moved; recorded only on the controller's SYNC-OK
    HandoverVM       a VM handed to the controller's registry: in one registry, never in neither
    handover_count   how many stopped VMs go to the controller (up to n there)
    merged_starts    two registries' starts at one timestamp merged: none lost
    Retries          a retried call (ssh to the controller, a pod deleted, its probes): at most max_tries
    precedence       the first source given wins (environment, then config, then default)
    modal_gpu        a GPU job's Modal GPU: RunPod's type mapped, a Modal name as is, else the default
    controller_state the controller as the skill reports it
    copy_state       the treadmill's state copied to the controller only where it has none
"""
from typing import Literal


# gdp: law a ticket of this machine whose pid is gone is never alive: not result or not same_host or not pid_gone
# gdp: law another machine's ticket is presumed alive: result or same_host
def ticket_alive(same_host: bool, pid_gone: bool) -> bool:
    """A client's ticket alive (boat.pid_alive's rule): this machine's by its pid; another machine's presumed alive."""
    return not same_host or not pid_gone


# gdp: law an orphan Modal app (its client's pid gone) is stopped: result or not pid_gone
# gdp: law a Modal app whose client lives is never stopped: not result or pid_gone
def reap_modal(pid_gone: bool) -> bool:
    """__main__._reap: a Modal app's ticket (this machine's: its client's pid) stopped where that client is gone."""
    return not ticket_alive(True, pid_gone)


# gdp: law an in-flight backup is never duplicated even when forced: not result or not in_flight
# gdp: law a recorded attempt consumes the hour even if it failed: not result or force or not has_attempt or now - last_attempt >= 3600
# gdp: law the first backup is not delayed by epoch zero: result or not has_url or in_flight or has_attempt
# gdp: law an unconfigured controller is never backed up: not result or has_url
def backup_attempt_due(has_url: bool, in_flight: bool, has_attempt: bool,
                       now: float, last_attempt: float, force: bool) -> bool:
    """Called under the client's nonblocking flock; an attempt, not success, starts cooldown."""
    return has_url and not in_flight and (force or not has_attempt or now - last_attempt >= 3600)


class Cadence:
    """The controller's upkeep before a command (controller.maybe_sync, backup): a copy of its state at most hourly
    (unless forced), and its boat key checked at most hourly, whether the backups succeed or not."""

    def __init__(self, t_backup: float, t_check: float):
        self.t_backup = t_backup
        self.t_check = t_check

    # gdp: law a backup at most hourly unless forced: not result or force or now - t_backup >= 3600
    def backup_due(self, now: float, force: bool, has_url: bool) -> bool:
        return has_url and (force or now - self.t_backup >= 3600)

    def backed_up(self, now: float) -> None:
        """A backup made (only then is its time recorded)."""
        self.t_backup = now

    # gdp: law the controller's key is checked at most hourly: not result or now - old(t_check) >= 3600
    def check_due(self, now: float) -> bool:
        """Whether to check the controller's boat key now (a VM restarted since has none: rekeyed)."""
        if now - self.t_check < 3600:
            return False
        self.t_check = now
        return True


# gdp: law main is synced only to a controller of this repo: not result or (has_vm and same_repo)
# gdp: law main is not synced again where it hasn't moved: not result or not (has_head and head_recorded)
def sync_due(has_vm: bool, same_repo: bool, has_head: bool, head_recorded: bool) -> bool:
    """controller.sync_git: the repo's main to the controller when it moved since the last sync (recorded only on the
    controller's SYNC-OK)."""
    return has_vm and same_repo and not (has_head and head_recorded)


class HandoverVM:
    """A VM handed to the controller (controller.handover, handover_warm): taken from this registry (so no client here
    leases it), written into the controller's; when that write fails, put back here."""

    # gdp: law a VM handed over is in one registry, never in neither: here or there
    # gdp: law a VM is never in both registries: not (here and there)
    # gdp: law a VM being moved is still here: not taken or here

    def __init__(self):
        self.here = True
        self.there = False
        self.taken = False

    def take(self) -> bool:
        """Taken from this registry, under its lock (it moves now)."""
        if not self.here or self.taken:
            return False
        self.taken = True
        return True

    # gdp: law a write the controller didn't confirm leaves the VM here: remote_ok or not old(taken) or here
    def written(self, remote_ok: bool) -> None:
        """The controller's registry written (remote_ok: its OK printed)."""
        if not self.taken:
            return
        self.taken = False
        if remote_ok:
            self.here = False
            self.there = True


# gdp: law the controller is given no more than it lacks: result <= n - have or result == 0
# gdp: law no more than are free here: result == 0 or result <= free
def handover_count(n: int, have: int, free: int) -> int:
    """controller.handover: stopped VMs moved so the controller holds n of the image (no more than are free here)."""
    k = max(0, n - have)
    return min(k, max(0, free))


# gdp: law merging two registries loses no start either counted: result >= a and result >= b
# gdp: law merging counts no start neither did: result <= a or result <= b
def merged_starts(a: int, b: int) -> int:
    """migrate.merge_registry: the starts two registries recorded at one timestamp (note_starts(k) records k at one
    timestamp): the larger count (one registry's history is the other's copy where they share it)."""
    return max(a, b)


class Retries:
    """A call tried again on a transient failure (ssh to the controller through boat, a pod deleted, a pod's probes):
    at most max_tries, none after it succeeded."""

    # gdp: law at most max_tries tries: tries <= max_tries
    # gdp: law no try after a success: not ok or tries == old(tries)

    def __init__(self, max_tries: int):
        if max_tries < 1:
            raise ValueError("max_tries is at least 1")
        self.max_tries = max_tries
        self.tries = 0
        self.ok = False

    # gdp: law a try is made only while tries are left: not result or old(tries) < max_tries
    def another(self) -> bool:
        if self.ok or self.tries >= self.max_tries:
            return False
        self.tries += 1
        return True

    def succeeded(self) -> None:
        self.ok = True


# gdp: law the first source given wins: result == 1 or not first
# gdp: law a later source only where the earlier ones aren't given: result != 3 or not (first or second)
def precedence(first: bool, second: bool) -> int:
    """Which of three sources a setting comes from (1, 2, else 3): config.env ($SUPERCOMPUTER_<n>, $GDPW_BOAT_<n>, the
    default), runpod._env, Pod's GPU list (given, environment, config, the default list), find_config."""
    if first:
        return 1
    if second:
        return 2
    return 3


# gdp: law a RunPod type with a Modal match maps to it: result == "mapped" or not (is_str and known)
# gdp: law the default GPU only where no other is named: result != "default" or not is_str or not (known or modal_like)
def modal_gpu(is_str: bool, known: bool, modal_like: bool) -> Literal["mapped", "as_is", "default"]:
    """modal_run._modal_gpu: a job's "gpu" on Modal: RunPod's type's nearest Modal GPU, else a name that reads as
    Modal's (all capitals, no space) as it is, else the default (L4)."""
    if not is_str:
        return "default"
    if known:
        return "mapped"
    if modal_like:
        return "as_is"
    return "default"


# gdp: law the controller is reported up only when its status answered: result != "up" or (configured and forward and status_ok)
def controller_state(configured: bool, forward: bool, status_ok: bool) -> Literal["off", "vm_only", "up", "not_answering"]:
    """skill._controller: off on this machine; no forward (boat's word on its VM only); up (its status answered); not
    answering (commands fall back to this machine's scheduler)."""
    if not configured:
        return "off"
    if not forward:
        return "vm_only"
    if status_ok:
        return "up"
    return "not_answering"


# gdp: law the controller's treadmill state is never overwritten: not result or not vm_has_state
def copy_state(vm_has_state: bool) -> bool:
    """controller.setup: this machine's treadmill state copied to the controller only where it has none yet."""
    return not vm_has_state
