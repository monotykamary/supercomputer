"""boat.py's broken rules, kept: each the bug a law of supercomputer/rules/boat.py rules out, as boat.py had it, with
that law stated again here. `tests/check_laws.sh` asserts every law below comes back broken, with the run that breaks
it, so a change that brings one back is caught."""
from typing import Literal

from supercomputer.rules.boat import Entry, Reports, Resume, RunEnd


# gdp: law a holder of this host, or of no host (an old record), whose pid is gone is dead: not ((host == "" or host == here) and probe == "gone") or not result
def holder_alive_before(host: str, here: str, probe: Literal["alive", "gone", "denied"]) -> bool:
    """pid_alive before: a lease (or a ticket) that names no host was another machine's, never dead: reap's Modal
    tickets, volumes' cleanup and the treadmill's liveness never saw their own dead holders."""
    if host != here:
        return True
    return probe != "gone"


class TeardownOthers(Entry):
    """teardown before: without --all, every free VM whose image isn't this repo's current one deleted, other repos'
    free VMs of the shared registry among them."""

    # gdp: law teardown deletes only free VMs, of this repo's images unless --all or --type: not result or ((old(lease) == "none" or old(lease) == "dead") and (everything or this_repo or type_match))
    def teardown(self, everything: bool, this_repo: bool, current: bool, type_match: bool) -> bool:
        if not self.present or not self.free():
            return False
        if everything or not current or type_match:
            self.present = False
            return True
        return False


# gdp: law a CEIL share of a limit is a share of it, never the share read as a count: share_pct == 0 or limit < 100 or result >= 1
def ceiling_before(share_pct: int, limit: int) -> int:
    """StartGate.take before: int(CEIL[2]), the minute's share read as a count (0.5 of the minute: 0 starts, none ever)."""
    return share_pct // 100


# gdp: law a demand keeps a released VM no longer than the idle time (the window is retired): not declared or result <= 60
def release_ttl_window(declared: bool, idle: int, idle_given: bool) -> int:
    """warm_ttls with the 10-minute demand window (retired: boat's TOS-6, busy, never parked)."""
    if idle_given:
        return idle
    if declared:
        return 600
    return 60


# gdp: law boat's limits not to be had, nothing is refreshed: limits_ok or result == 0
def refresh_count_before(old: int, left: int, limits_ok: bool) -> int:
    """refresh_old before: boat's limits not to be had, every VM stopped 12 days resumed (a start each), uncapped."""
    if not limits_ok:
        return old
    return max(0, min(old, 4, left))


class WaitsForever(RunEnd):
    """Session.run before: `gone` needed a VM once held, so a batch that never got one (no start, --max-wait 0) waited
    forever."""

    # gdp: law a run with no VM in hand, items left and nothing coming up never just waits: result != "wait" or held or not queue or coming or ended or (deadline and not had_vm)
    def tick(self, had_vm: bool, deadline: bool, held: bool, queue: bool, coming: bool, on_lost: bool, max_wait_over: bool, ready: bool) -> str:
        if self.ended:
            return "wait"
        gone = had_vm and not held and queue and not coming
        if gone and self.revives == 0:
            self.revives = 1
            return "revive"
        if max_wait_over and not ready:
            self.ended = True
            return "give_up"
        if gone:
            self.ended = True
            if on_lost:
                return "give_up"
            return "raise"
        return "wait"


class DropsUnreported(Reports):
    """Scheduled.run before: the scheduler done with no rows, the loop broke, and an item it never reported was never
    reported to the client (boat_jobs called it never ran; BoatEx.starmap dropped it)."""

    # gdp: law every item is reported by the time the run ends: not ended or left == 0
    def end(self) -> int:
        self.ended = True
        return 0


class CountsOnce(Resume):
    """_bring_up before: counts["resumed"] += 1 per VM however many tries (each a start): the ledger undercounted."""

    # gdp: law the ledger counts every resume tried, once the bring-up ends: outcome == "trying" or counted == tries
    def attempt(self, enough: bool, start: bool, refused: Literal["no", "minute", "hour"], came_up: bool) -> str:
        if self.outcome != "trying":
            return self.outcome
        if self.tries >= 3:
            self.outcome = "failed"
        elif self.tries == 0 and enough:
            self.outcome = "skipped"
        elif not start:
            self.outcome = "failed"
        else:
            self.granted += 1
            if refused != "no":
                self.refunded += 1
                if refused == "hour":
                    self.outcome = "failed"
            else:
                self.tries += 1
                if came_up:
                    self.outcome = "up"
                elif self.tries >= 3:
                    self.outcome = "failed"
        if self.outcome != "trying":
            self.counted = 1 if self.tries > 0 else 0
        return self.outcome


# gdp: law a VM absent from a list that may be partial is never gone: not result or deleted or complete
def one_page_gone(deleted: bool, absent: bool, complete: bool, absences: int) -> bool:
    """Before: reconcile took a VM missing from `boat list`'s answer for gone, whatever page it read."""
    return deleted or absent
