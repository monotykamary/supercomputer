"""boat.py's decision rules, apart from its threads, its I/O and boat's API: the pool's registry (leases, release,
reconcile, reap, retire, cold, teardown), the starts' accounting (the gate, what's left, a batch's budget), bring-up
(resume and creation, their retries), a local session's items and its run's end, a scheduled run's reports, and the
choices around a batch (its plan, the preflight, Modal for a tiny one, a job's slots, the retries of boat exec and the
warm agents). boat.py calls them for these decisions and acts on what they say.

    Entry        one VM of the registry: its lease (none, this batch's, another live client's, a dead one's) and TTL
    Gate         the StartGate: starts granted since the limits were last read, paced, within the minute and CEIL
    Ensure       Session.ensure: how many free VMs a batch leases and how many it starts
    Resume       _bring_up's resume of a stopped VM: its tries and the starts boat refused, given back
    Creation     _create1: VMs asked of boat, those it made and failed, when it stops asking
    LocalItem    an item of a local session (Session.run): taken, lost and run again once, finished exactly once
    RunEnd       Session.run's main loop: when a run with no VM left revives, gives up or raises
    Reports      Scheduled.run: every item reported once, the run ending only then
    Retry        a bounded retry (exec_json, the warm agent's requests, the scheduler's logs and close, a pull's range)
    Preflight3   preflight3: Modal for boat's API errors only after 3 tries

Times are seconds (time.time()); counts are naturals.
"""
from typing import Literal

Lease = Literal["none", "ours", "theirs", "dead"]
Probe = Literal["alive", "gone", "denied"]
Fate = Literal["queued", "running", "finished"]
Act = Literal["wait", "revive", "give_up", "raise"]
Verdict = Literal["ok", "wait", "api", "billing", "credit", "image", "full", "no_starts", "none"]
Source = Literal["explicit", "out", "index", "search", "none"]
Reap = Literal["keep", "kill", "stop"]
Recon = Literal["keep", "drop", "delete"]

IDLE = 60  # a released VM stays up this long; never longer to save starts (boat's TOS-6): the demand window is retired
LONG = IDLE
WARM_MARGIN = 8  # a warm VM is leased while it has this long before boat stops it


# ------------------------------------------------------------------------------------------------- leases and holders
# gdp: law another host's holder is never presumed dead: host == "" or host == here or result
# gdp: law a holder of this host, or of no host (an old record), whose pid is gone is dead: not ((host == "" or host == here) and probe == "gone") or not result
def holder_alive(host: str, here: str, probe: Probe) -> bool:
    """pid_alive: a lease's holder (pid on host) alive. Another machine's is never presumed dead; this machine's (or a
    record that names no host: written here) is dead when its pid is gone (os.kill(pid, 0): ESRCH); a pid we may not
    signal is alive."""
    if host != "" and host != here:
        return True
    return probe != "gone"


class Entry:
    """One VM of the registry (LIVE/boat-pool.json): in it or not, up or not, its lease (none, this batch's, another
    live client's, or one whose holder died: free), when boat stops it (`until`) and when it was last let go."""

    # gdp: law a VM is leased only while its lease is free: lease == old(lease) or lease != "ours" or old(lease) == "none" or old(lease) == "dead"
    # gdp: law nothing takes another live client's lease: old(lease) != "theirs" or lease == "theirs" or not present

    def __init__(self, up: bool, until: float, last_used: float):
        self.present = True
        self.up = up
        self.lease: Lease = "none"
        self.until = until
        self.last_used = last_used

    def free(self) -> bool:
        return self.lease == "none" or self.lease == "dead"

    # gdp: law a batch leases only a VM of its image and type that it doesn't hold: lease == old(lease) or matches
    def lease_stopped(self, matches: bool) -> bool:
        """Session.ensure: a free VM of the batch's image and type (up, or stopped: a resume, a start) leased."""
        if not self.present or not matches or not self.free():
            return False
        self.lease = "ours"
        return True

    # gdp: law a warm lease never takes a VM about to stop: lease == old(lease) or (self.up and until > now + 8 and agent)
    def lease_warm(self, now: float, matches: bool, agent: bool) -> bool:
        """_lease_warm: a free VM up, with its warm agent, not about to stop: leased with no boat API call."""
        if not self.present or not matches or not self.free() or not self.up or not agent:
            return False
        if self.until <= now + WARM_MARGIN:
            return False
        self.lease = "ours"
        return True

    # gdp: law a VM another batch released joins only while it is up for 20 s more: lease == old(lease) or (live_up and until > now + 20)
    def lease_live(self, now: float, matches: bool, live_up: bool) -> bool:
        """lease_up: a free VM boat reports up (released by another batch less than its idle time ago), no start."""
        if not self.present or not matches or not self.free() or not live_up:
            return False
        if self.until <= now + 20:
            return False
        self.lease = "ours"
        return True

    # gdp: law a released VM stays up at most its idle time: not present or lease != "none" or old(lease) == "none" or until == 0 or until <= now + ttl
    # gdp: law a release never clears another client's lease: old(lease) == "none" or old(lease) == "ours" or lease == old(lease)
    def release(self, now: float, ttl: int, up: bool, deleted: bool) -> bool:
        """_release: the batch's VM let go (only one still leased to it, or to nobody: one it gave back early may be
        another's now); it stays up `ttl` s for a back-to-back batch, or is gone (deleted)."""
        if not self.present or (self.lease != "none" and self.lease != "ours"):
            return False
        if deleted:
            self.present = False
            self.lease = "none"
            return True
        self.lease = "none"
        self.last_used = now
        if up and ttl > 0:
            self.until = now + ttl
            self.up = True
        else:
            self.until = 0.0
        return True

    # gdp: law boat's VM with a live lease leaves the registry only when boat lost it or its disk expired: present or not old(present) or old(lease) == "none" or old(lease) == "dead" or gone or expired
    def reconcile(self, gone: bool, expired: bool, unresumable: bool, stuck: bool) -> Recon:
        """reconcile: gone from boat (or deleted), dropped; its stopped disk expired, deleted whatever its lease; a free
        one stopped with no snapshot, or stuck stopping with no disk, deleted; else its state recorded."""
        if not self.present:
            return "keep"
        if gone:
            self.present = False
            return "drop"
        if expired or (self.free() and (unresumable or stuck)):
            self.present = False
            self.lease = "none"
            return "delete"
        return "keep"

    # gdp: law reap never takes a live holder's lease: (old(lease) != "ours" and old(lease) != "theirs") or lease == old(lease)
    # gdp: law reap stops only a VM no live client holds: result == "keep" or lease == "none"
    def reap(self, now: float, scheduler_lease: bool, scheduler_up: bool) -> Reap:
        """reap: a dead holder's lease released (what it left running killed, the VM stopped); a free VM up past its
        `until` + 60 s (or, with none, 120 s after its release) stopped. A VM a scheduler held is the running one's to
        adopt."""
        if not self.present or (scheduler_lease and scheduler_up):
            return "keep"
        if self.lease == "dead":
            self.lease = "none"
            self.last_used = now
            return "kill"
        if self.lease != "none" or not self.up:
            return "keep"
        past = now > self.until + 60 if self.until > 0 else now - self.last_used > 120
        if past:
            return "stop"
        return "keep"

    # gdp: law cold stops only free VMs: not result or old(lease) == "none" or old(lease) == "dead"
    def cold(self) -> bool:
        """cold: a free VM up stopped now (its idle time cut short)."""
        if not self.present or not self.free() or not self.up:
            return False
        self.until = 0.0
        return True

    # gdp: law teardown deletes only free VMs, of this repo's images unless --all or --type: not result or ((old(lease) == "none" or old(lease) == "dead") and (everything or this_repo or type_match))
    def teardown(self, everything: bool, this_repo: bool, current: bool, type_match: bool) -> bool:
        """teardown: free VMs of this repo's older images deleted (every free VM with --all, every free one of a type
        with --type)."""
        if not self.present or not self.free():
            return False
        if everything or (this_repo and not current) or type_match:
            self.present = False
            return True
        return False

    # gdp: law retire deletes only free VMs of this repo's superseded images: not result or ((old(lease) == "none" or old(lease) == "dead") and this_repo and not kept)
    def retire(self, this_repo: bool, kept: bool) -> bool:
        """retire: a free VM of this repo's images neither current nor the one before deleted."""
        if not self.present or not self.free() or not this_repo or kept:
            return False
        self.present = False
        return True

    # gdp: law undeclaring cuts a kept VM to its idle time: until <= old(until)
    def undeclare(self, now: float, owned: bool) -> bool:
        """undeclare: a VM kept up for a client that declares no more work stops IDLE s from now."""
        if not self.present or not owned or not self.free() or self.until <= now + IDLE:
            return False
        self.until = now + IDLE
        return True


# gdp: law a demand keeps a released VM no longer than the idle time (the window is retired): idle_given or not declared or result <= IDLE
def release_ttl(declared: bool, idle: int, idle_given: bool) -> int:
    """warm_ttls with _let_go and close: how long a released VM stays up. --idle when given; else LONG for a client
    that declared more work (demand), IDLE otherwise; LONG is IDLE (boat's TOS-6: busy, never parked)."""
    if idle_given:
        return idle
    if declared:
        return LONG
    return IDLE


# gdp: law a VM boat can't stop is deleted, never left up: result == "deleted" or not snapshot_failed
def stop_fallback(snapshot_failed: bool) -> Literal["stopped", "deleted"]:
    """stop_vm: a VM whose final snapshot fails can't be stopped: deleted instead."""
    if snapshot_failed:
        return "deleted"
    return "stopped"


# gdp: law the current image, its predecessor and any image under a day old are never removed: result or not (current or last_two or young)
def keep_snapshot(ours: bool, current: bool, last_two: bool, young: bool, base: bool) -> bool:
    """prune_snapshots: a named snapshot of this repo's prefixes kept when it is a layer of the current image, one of
    the last two images, an image first made current less than a day ago, or the base; others removed."""
    if not ours:
        return True
    return current or last_two or young or base


# gdp: law refreshing spends at most 4 starts, and never more than are left: result <= 4 and (not limits_ok or result <= max(0, left))
# gdp: law boat's limits not to be had, nothing is refreshed: limits_ok or result == 0
def refresh_count(old: int, left: int, limits_ok: bool) -> int:
    """refresh_old: VMs stopped REFRESH_DAYS resumed and stopped again (a start each), at most 4 and within the starts
    left; none when boat's limits can't be read (no start spent blind)."""
    if not limits_ok:
        return 0
    return max(0, min(old, 4, left))


# ------------------------------------------------------------------------------------------------------------ starts
# gdp: law the free starts leave the reserve for the account's other projects: result == 0 or (result <= hour_rem - reserve_hour and result <= day_rem - reserve_day)
# gdp: law the free starts leave twice what the others used: result == 0 or (result <= day_rem - 2 * (acct_day - mine_day) and result <= hour_rem - 2 * (acct_hour - mine_hour))
def free_left(hour_rem: int, day_rem: int, acct_hour: int, acct_day: int, mine_hour: int, mine_day: int, reserve_hour: int, reserve_day: int) -> int:
    """starts_left's free part: the free starts left this hour and day, less the reserve for the account's other
    projects (at least twice what they used in the last hour and day)."""
    rd = max(reserve_day, 2 * max(0, acct_day - mine_day))
    rh = max(reserve_hour, 2 * max(0, acct_hour - mine_hour))
    return max(0, min(hour_rem - rh, day_rem - rd))


# gdp: law paid starts only while extra starts are on and wanted: result == 0 or (pay and wallet)
# gdp: law paid starts stay within the wallet's limits: result <= max(0, w_hour_rem - max(0, hour_rem))
def paid_left(pay: bool, wallet: bool, w_hour_rem: int, w_day_rem: int, hour_rem: int, day_rem: int) -> int:
    """starts_left's paid part: while extra starts are on and under their cap (effective's wallet limits), what lies
    past the free limits up to the wallet's."""
    if not pay or not wallet:
        return 0
    return max(0, min(w_hour_rem - max(0, hour_rem), w_day_rem - max(0, day_rem)))


# gdp: law the wallet's limits only for an urgent batch while extra starts are on and under their cap: not result or (urgent and enabled and not cap_reached)
def wallet_in_force(urgent: bool, enabled: bool, cap_reached: bool, has_wallet: bool, has_multiplier: bool) -> bool:
    """effective: the wallet's limits (or the plan's times boat's multiplier, once boat drops them with the wallet
    empty) for an urgent batch while extra starts are on and under their cap; else the free ones."""
    if not (urgent and enabled and not cap_reached):
        return False
    return has_wallet or has_multiplier


# gdp: law a CEIL share of a limit is a share of it, never the share read as a count: share_pct == 0 or limit < 100 or result >= 1
# gdp: law a CEIL share never exceeds the limit: result <= limit or share_pct > 100
def ceiling(share_pct: int, limit: int) -> int:
    """SUPERCOMPUTER_CEIL=day,hour,minute: shares of the free limits as a ceiling of our own, each a count: the share
    (in percent) of the limit, rounded down. Neither is ever below 0."""
    if share_pct < 0 or limit < 0:
        raise ValueError("a share and a limit are at least 0")
    return share_pct * limit // 100


# gdp: law the starts left never pass a CEIL ceiling: not ceil_on or result <= max(ceil_day_left, 0) or result <= free + paid
# gdp: law the starts left are at most the free and paid ones: result <= free + paid
def starts_left(free: int, paid: int, ceil_on: bool, ceil_day_left: int, ceil_hour_left: int) -> int:
    """starts_left: the free starts and the paid ones, capped by CEIL's ceilings less our own starts of the day and
    hour (which can take it below 0: no start)."""
    k = free + paid
    if ceil_on:
        return min(k, ceil_day_left, ceil_hour_left)
    return k


# gdp: law a batch's budget is within the starts left: result <= max(0, left)
# gdp: law a batch's budget is within its cap: cap == 0 or old_policy or result <= cap
# gdp: law a batch gets at least 2 starts while that many are left: old_policy or left < 2 or result >= 2 or (cap > 0 and cap < 2)
def budget(left: int, share_pct: int, cap: int, old_policy: bool) -> int:
    """budget: the starts one batch may spend of those left: its share (SHARE, in percent) but at least MIN_STARTS
    (2), at most its cap (--max-vms, else MAX_STARTS; 0: none); the old policy: all of them."""
    if share_pct < 0 or cap < 0:
        raise ValueError("a share and a cap are at least 0")
    if left <= 0:
        return 0
    if old_policy:
        return left
    k = min(left, max(2, share_pct * left // 100))
    if cap > 0:
        return min(k, cap)
    return k


# gdp: law the paid part of a grant is within the grant: result <= max(0, k)
def paid_of(k: int, free_left_now: int) -> int:
    """take_split: how many of the k starts granted are paid (past the free limits' remaining, as last read)."""
    return max(0, k - max(0, free_left_now))


class Gate:
    """The StartGate: starts granted since boat's limits were last read (`used`), paced (at most a sixth of what's
    left in any minute, unless urgent), within the minute's limit and a CEIL ceiling of the minute; 0 once the hour or
    the day is spent."""

    # gdp: law a grant never exceeds what was asked: result <= max(0, n)
    # gdp: law grants since the limits were read stay within the starts left: result == 0 or used <= left
    # gdp: law grants stay within the minute's limit: result == 0 or used <= minute_rem
    # gdp: law grants are paced, a sixth of what's left a minute: urgent or result == 0 or ours_minute + result <= 1 or 6 * (ours_minute + result) <= left - old(used) + 5
    # gdp: law grants stay within CEIL's minute: not ceil_on or result == 0 or ours_minute + result <= ceil_minute

    def __init__(self):
        self.used = 0

    def refresh(self) -> None:
        """boat's limits read again: what's left is theirs now."""
        self.used = 0

    def take(self, n: int, left: int, minute_rem: int, ours_minute: int, urgent: bool, ceil_on: bool, ceil_minute: int) -> int:
        """take: up to n starts now (0: none to be had now; with what's left 0, none this hour or day)."""
        rest = left - self.used
        if rest <= 0 or n <= 0:
            return 0
        k = min(n, rest, minute_rem - self.used)
        if ceil_on:
            k = min(k, ceil_minute - ours_minute)
        if not urgent:
            k = min(k, max(1, (rest + 5) // 6) - ours_minute)
        if k <= 0:
            return 0
        self.used += k
        return k


class StartLog:
    """The registry's starts (note_starts, ours): every start a client of this pool made, the only count of ours."""

    # gdp: law a start taken back never leaves fewer than none: count >= 0
    # gdp: law the count moves by the starts noted, never past none: count == old(count) + k or (k < 0 and count == 0)

    def __init__(self):
        self.count = 0

    def note(self, k: int) -> None:
        """k starts made now; k < 0: the newest -k taken back (granted, then refused by boat)."""
        if k > 0:
            self.count += k
        elif k < 0:
            self.count = max(0, self.count + k)


# -------------------------------------------------------------------------------------------------------------- a batch
class Ensure:
    """Session.ensure: n VMs asked of the pool; the batch's free VMs of its image leased (those up first, then stopped
    ones, a resume each), the rest created, resumes and creations within the batch's budget of the starts left."""

    def __init__(self):
        self.up_leased = 0
        self.resumed = 0
        self.created = 0
        self.short = False

    # gdp: law resumes and creations of one ensure stay within the batch's budget: old_policy or resumed + created <= max(0, budget_k)
    # gdp: law no start for a batch that yields: not (warm_only or starts <= min_left) or (resumed == 0 and created == 0)
    # gdp: law never more VMs than asked: up_leased + resumed + created <= max(0, need)
    # gdp: law a VM up is leased before one is resumed or made: resumed + created == 0 or up_leased == min(max(0, need), max(0, up))
    def plan(self, need: int, up: int, stopped: int, starts: int, warm_only: bool, min_left: int, budget_k: int, old_policy: bool) -> int:
        """The VMs up leased, the stopped ones resumed, the ones created; `starts` the starts left, `budget_k` the
        batch's budget of them (budget(starts, max_starts), 0 where it yields)."""
        if need <= 0:
            self.up_leased = 0
            self.resumed = 0
            self.created = 0
            return 0
        if warm_only or starts <= min_left:
            starts = 0
            budget_k = 0
        elif need > up and starts <= 0:
            self.short = True
        self.up_leased = min(need, max(0, up))
        self.resumed = min(need - self.up_leased, max(0, stopped), max(0, budget_k))
        rest = need - self.up_leased - self.resumed
        if not old_policy:
            rest = min(rest, max(0, budget_k) - self.resumed)
        if rest > 0 and starts > 0:
            self.created = rest
        else:
            self.created = 0
        return self.up_leased + self.resumed + self.created


# gdp: law a plan asks at least one VM: result >= 1
# gdp: law a plan asks no more VMs than are useful (one at least): result <= max(1, useful)
# gdp: law a plan stays within its cap: cap <= 0 or result <= max(1, cap)
def plan_vms(useful: int, by_rate: int, room: int, up: int, budget_k: int, cap: int, limits_ok: bool) -> int:
    """plan_vms: the VMs a batch asks: no more than help (useful: the longest item bounds the batch), than arrive in
    time at boat's rate, than boat has room for, than the VMs up plus its budget of starts; at most the cap; at least
    one (a batch needs a VM). Boat's limits not to be had: the useful ones, at most 10."""
    if limits_ok:
        n = min(useful, by_rate, room, up + budget_k)
    else:
        n = min(useful, 10)
    if cap > 0:
        n = min(n, cap)
    return max(1, n)


# gdp: law a confirmed missing resume is replaced: not missing or result
# gdp: law a transient resume error is not a replacement: missing or expired or not result
def resume_replaces(missing: bool, expired: bool) -> bool:
    """A direct missing-VM answer or expired disk is terminal, never another resume."""
    return missing or expired

class Resume:
    """_bring_up's resume of a stopped VM leased: no start where the VMs up have slots for all that's left (the first
    try only), a start from the gate each try, a start boat refused (rate-limited) given back and not a try; at most 3
    tries; the minute's refusal waits, the hour's or the day's ends it."""

    # gdp: law a resume is tried at most 3 times: tries <= 3
    # gdp: law every start boat refused is given back: granted == tries + refunded
    # gdp: law a VM is skipped only before its first try: not (outcome == "skipped") or tries == 0
    # gdp: law a resume is tried only with a start granted: tries == old(tries) or granted == old(granted) + 1
    # gdp: law the ledger counts every resume tried, once the bring-up ends: outcome == "trying" or counted == tries

    def __init__(self):
        self.tries = 0
        self.granted = 0
        self.refunded = 0
        self.outcome: Literal["trying", "up", "skipped", "failed"] = "trying"
        self.counted = 0

    def attempt(self, enough: bool, start: bool, refused: Literal["no", "minute", "hour"], came_up: bool) -> str:
        """One pass of the loop: `enough` the VMs up have slots for what's left; `start` the gate granted one; boat's
        answer: refused for the minute's starts (again), the hour's or day's (the end), else tried, up or not. Once
        it ends, the ledger counts its tries (each a start)."""
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
            self.counted = self.tries
        return self.outcome


class Creation:
    """_create1: VMs asked of boat as the gate grants starts; a grant boat made no VM for given back; asking stops
    once boat has failed 3 times what it made (no creation loop)."""

    # gdp: law every start boat made no VM for is given back: refunded == failed
    # gdp: law creation stops once boat failed 3 times what it made: not stopped or failed >= 3 * max(1, created) or done
    # gdp: law creation never stops while boat makes VMs: not stopped or done or created == old(created)

    def __init__(self):
        self.created = 0
        self.failed = 0
        self.refunded = 0
        self.stopped = False
        self.done = False

    def made(self, asked: int, got: int, enough: bool) -> bool:
        """boat new asked `asked` (granted by the gate) and made `got`; `enough`: the batch needs no more. True: ask
        again."""
        if self.stopped:
            return False
        if enough or asked <= 0:
            self.stopped = True
            self.done = True
            return False
        got = max(0, min(got, asked))
        self.failed += asked - got
        self.refunded += asked - got
        self.created += got
        if got == 0 and self.failed >= 3 * max(1, self.created):
            self.stopped = True
            return False
        return True


# --------------------------------------------------------------------------------------------- a local session's run
class LocalItem:
    """An item of a local session (Session.run): taken by a VM's worker, finished with its row; lost (its VM died, its
    runner vanished) it runs once more on boat with no fallback, else goes to the fallback (Modal) at once; a run that
    gives up finishes what's queued. Finished exactly once.

    Progress is per granted-lease interval, not unconditional across revocations.
    Under the existing fairness assumption (queued work is eventually taken when
    this client has capacity; a running VM eventually reports or fails), an active
    interval ends in completion or an explicit lease-loss yield. A successful
    take starts a fresh interval. Fencing returns the item to the queue without
    consuming a failure retry; it must not pretend that its progress rank fell.
    Endless lease revocation can prevent completion, so global eventual finish
    additionally requires eventual stable availability. This is Stage 1's local
    L4 scope: other clients' held VMs are absent, never capacity owed to us.
    """

    # gdp: law an item is finished exactly once: finishes <= 1 and ((state == "finished") == (finishes == 1))
    # gdp: law with no fallback an item runs again on boat at most once: tries <= 1
    # gdp: law with a fallback a lost item goes to it at once, never again on boat: not on_lost or tries == 0
    # gdp: law an item is taken at most once more than it was lost: runs <= tries + 1
    # gdp: law a queued item has run no more than it was lost: state != "queued" or runs <= tries
    # gdp: law a running item has run once more than it was lost: state != "running" or runs == tries + 1
    # gdp: law an item lost once was run once: tries <= runs
    # gdp: law a lease interval eventually finishes the item or explicitly yields its lease: state != "finished" and not lease_lost leads to state == "finished" or lease_lost by 3 - tries - runs via state == "queued" -> take(); state == "running" -> done(lost = true)

    def __init__(self, on_lost: bool):
        self.on_lost = on_lost
        self.state: Fate = "queued"
        self.tries = 0
        self.runs = 0
        self.finishes = 0
        self.lease_lost = False

    def take(self) -> bool:
        if self.state != "queued":
            return False
        self.state = "running"
        self.lease_lost = False
        self.runs += 1
        return True

    def done(self, lost: bool) -> None:
        """Its VM's row: done, or lost (its runner vanished; its VM given up: every item running there)."""
        if self.state != "running":
            return
        if lost:
            self._lose()
        else:
            self._finish()

    # gdp: law lease revocation never charges an attempt failure: tries == old(tries)
    # gdp: law lease revocation never fabricates a completion: finishes == old(finishes)
    def fenced(self) -> None:
        """End this lease interval without a failure or invented result; await a fresh take."""
        if self.state == "running":
            self.state = "queued"
            self.runs -= 1
            self.lease_lost = True

    def give_up(self) -> None:
        """The run gives up (no VM ready in --max-wait, every VM lost): what's queued finishes, to the fallback."""
        if self.state == "queued":
            self._finish()

    def _lose(self) -> None:
        if self.tries + 1 <= 1 and not self.on_lost:
            self.tries += 1
            self.state = "queued"
        else:
            self._finish()

    def _finish(self) -> None:
        self.state = "finished"
        self.finishes += 1


class RunEnd:
    """Session.run's main loop: with no VM in hand (none ever, or every one dead or let go), items queued and nothing
    coming up, one more VM is asked (once), then the run gives up (to the fallback) or raises; with --max-wait and no
    VM ready past it, it gives up."""

    # gdp: law a run with no VM in hand, items left and nothing coming up never just waits: result != "wait" or held or not queue or coming or ended or (deadline and not had_vm)
    # gdp: law a run revives at most once: revives <= 1
    # gdp: law a run with no VM coming ends: not ended leads to ended by 1 - revives via tick(had_vm = false, deadline = false, held = false, queue = true, coming = false, on_lost = true, max_wait_over = false, ready = false)

    def __init__(self):
        self.revives = 0
        self.ended = False

    def tick(self, had_vm: bool, deadline: bool, held: bool, queue: bool, coming: bool, on_lost: bool, max_wait_over: bool, ready: bool) -> Act:
        """One pass (every 0.25 s): `held` a VM of the batch not dead nor let go; `coming` a bring-up thread alive;
        `had_vm` whether the batch ever had one; `deadline` a --max-wait."""
        if self.ended:
            return "wait"
        # (a batch that never had a VM waits for one only while it has a deadline, --max-wait: one another batch
        # releases may come; with none, it would wait forever)
        gone = not held and queue and not coming and (had_vm or not deadline)
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


class Reports:
    """Scheduled.run: the scheduler's rows, each item reported once (its row, or to the fallback when lost); the run
    ends when the scheduler says done, and what it never reported is reported then (lost), never dropped."""

    # gdp: law every item is reported by the time the run ends: not ended or left == 0
    # gdp: law an item is reported at most once: reported <= items
    # gdp: law the items are those reported and those left: reported + left == items

    def __init__(self, items: int):
        if items < 0:
            raise ValueError("items")
        self.items = items
        self.left = items
        self.reported = 0
        self.ended = False

    def row(self, fresh: bool) -> bool:
        """A row of the scheduler's; `fresh`: its item still unreported."""
        if self.ended or not fresh or self.left <= 0:
            return False
        self.left -= 1
        self.reported += 1
        return True

    def end(self) -> int:
        """The scheduler says done with no rows: the items it never reported, reported now as lost; how many."""
        if self.ended:
            return 0
        n = self.left
        self.reported += n
        self.left = 0
        self.ended = True
        return n


# ------------------------------------------------------------------------------------------------- around a batch
# gdp: law a warm agent up is enough, no refusal and no API call: warm <= 0 or result == "ok"
# gdp: law a batch waits for another's release only with no starts, room, and VMs leased by others: result != "wait" or (starts <= 0 and room > 0 and leased > 0 and not old_policy)
# gdp: law with the scheduler deciding, capacity never refuses a batch: starts_check or result == "ok" or result == "api" or result == "billing" or result == "credit" or result == "image"
def preflight(warm: int, api_ok: bool, can_start: bool, credit: bool, image_ok: bool, can: int, starts_check: bool, starts: int, room: int, leased: int, old_policy: bool) -> Verdict:
    """preflight: can boat serve a batch now? A warm agent is enough; else boat's API, billing, credit and the image,
    then what the pool can give (`can`: free VMs up, resumable and creatable within the starts and boat's room). The
    scheduler's call (no starts check): it decides by class. With no start left but room, and VMs leased by others,
    the batch waits within --max-wait for one they release."""
    if warm > 0:
        return "ok"
    if not api_ok:
        return "api"
    if not can_start:
        return "billing"
    if not credit:
        return "credit"
    if not image_ok:
        return "image"
    if can > 0 or not starts_check:
        return "ok"
    if room <= 0:
        return "full"
    if starts <= 0:
        if not old_policy and leased > 0:
            return "wait"
        return "no_starts"
    return "none"


class Preflight3:
    """preflight3: preflight asked again on boat's API errors, 3 tries; Modal only once boat has failed it 3 times. A
    missing image or a billing block is no failure to retry."""

    # gdp: law boat's API failing sends a batch to Modal only after 3 tries: not (outcome == "modal") or tries >= 3 or not api_error_last
    # gdp: law preflight is asked at most 3 times: tries <= 3

    def __init__(self):
        self.tries = 0
        self.outcome: Literal["asking", "boat", "modal"] = "asking"
        self.api_error_last = False

    def ask(self, verdict: Verdict) -> str:
        if self.outcome != "asking" or self.tries >= 3:
            return self.outcome
        self.tries += 1
        self.api_error_last = verdict == "api"
        if verdict == "ok" or verdict == "wait":
            self.outcome = "boat"
        elif verdict != "api" or self.tries >= 3:
            self.outcome = "modal"
        return self.outcome


# gdp: law only a tiny batch with a fallback goes to Modal without trying boat: result == "boat" or (on_lost and (n_items <= 4 or slot_s < 600) and not scheduler)
# gdp: law with the scheduler on every batch goes to boat first: not scheduler or result == "boat"
# gdp: law a tiny batch takes boat's VM already up: not (remote or warm) or result == "boat"
def route(n_items: int, slot_s: int, on_lost: bool, scheduler: bool, remote: bool, warm: bool) -> Literal["boat", "modal"]:
    """boat_jobs: a tiny batch (TINY: 4 jobs or fewer, or under 10 slot-minutes) with a fallback, the scheduler off,
    runs on a VM already up (the controller's, a warm one, one a scheduler holds), else on Modal: a start for it would
    buy little."""
    small = n_items <= 4 or slot_s < 600
    tiny = on_lost and small and not scheduler
    if tiny and not (remote or warm):
        return "modal"
    return "boat"


# gdp: law a small batch is an interactive check, a bigger one a submission, unless its class is given: given or (small and result == "interactive") or (not small and result == "submission")
def batch_class(given: bool, given_class: Literal["interactive", "gate", "submission", "sweep", "filler"], small: bool) -> Literal["interactive", "gate", "submission", "sweep", "filler"]:
    """boat_jobs: the batch's class: --class or $SUPERCOMPUTER_CLASS, else interactive when small, a submission else."""
    if given:
        return given_class
    if small:
        return "interactive"
    return "submission"


# gdp: law a job runs only on a VM type whose usable memory holds what it asks: not result or memory + overhead <= nominal
# gdp: law a VM type whose usable memory holds the job takes it: result or memory + overhead > nominal
def fits_memory(memory: int, nominal: int, overhead: int) -> bool:
    """A job asking `memory` MB on a VM type of `nominal` MB: the type's usable memory, nominal less what its OS, its
    agent and boat's filesystem take (`overhead`, measured 11-01: 1.5-1.9 GB idle on 8 and 4 GB VMs, more on large
    with the trees it holds), must hold it, else the next type up. Before: a job asking a type's whole nominal memory
    got that type (cpu 4, memory 16384: a large VM of 16 GB; 5 of 48 Lean checks OOM-killed)."""
    if overhead < 0:
        raise ValueError("an overhead is at least 0")
    return memory + overhead <= nominal


# gdp: law a job gets the slots its CPUs and memory need, one at least: result >= 1 and 2 * result >= cpu and 4096 * result >= memory
def weight(cpu: int, memory: int) -> int:
    """job_items: a job's slots (2 vCPU and 4096 MB each): enough for its CPUs and its memory, at least one."""
    return max((cpu + 1) // 2, (memory + 4095) // 4096, 1)


# gdp: law durations covering under half of the jobs are never used: result == "none" or result == "search" or (result == "explicit" and explicit_half) or (result == "out" and out_half) or (result == "index" and index_half)
# gdp: law a tiny batch searches no runs: not (n_names <= 4) or result != "search"
def durations_source(explicit_half: bool, out_half: bool, index_half: bool, n_names: int, search_half: bool) -> Source:
    """last_durations: each job's seconds from --durations, else <out>/summary.tsv, else the index of every run's
    times, else (a batch of more than TINY jobs) the newest summary.tsv sharing most of the names; each only where it
    covers at least half of the jobs."""
    if explicit_half:
        return "explicit"
    if out_half:
        return "out"
    if index_half:
        return "index"
    if n_names <= 4:
        return "none"
    if search_half:
        return "search"
    return "none"


class Retry:
    """A bounded retry (exec_json's 4 tries, the warm agent's 4 requests, the scheduler's logs (12) and close (6), a
    pull's range (8 failures in a row)): asked until it works or its tries are spent."""

    # gdp: law never more tries than allowed: tries <= limit
    # gdp: law it stops at the first success: not ok or outcome == "ok"
    # gdp: law a retry allows one try at least: limit >= 1
    # gdp: law a retry still trying has a try left: outcome != "trying" or tries < limit
    # gdp: law a retry succeeds only by a try that worked: outcome != "ok" or ok

    def __init__(self, limit: int):
        if limit < 1:
            raise ValueError("limit")
        self.limit = limit
        self.tries = 0
        self.ok = False
        self.outcome: Literal["trying", "ok", "failed"] = "trying"

    def attempt(self, works: bool) -> str:
        if self.outcome != "trying":
            return self.outcome
        self.tries += 1
        if works:
            self.ok = True
            self.outcome = "ok"
        elif self.tries >= self.limit:
            self.outcome = "failed"
        return self.outcome


# gdp: law only the warm agent's replayable commands, and small ones, go over boat exec: not result or (exec_ok and small)
def by_exec(failed: bool, exec_ok: bool, small: bool) -> bool:
    """AgentClient.rpc: a request the warm agent's URL failed (boat's front: an error, a 502 for a replayable
    command), sent again from inside the VM over boat exec, for the commands that are safe to replay (EXEC_OK) and
    under 200 KB; any other failure is the caller's."""
    return failed and exec_ok and small


# gdp: law a git blob's content key is recorded only from bytes that are that blob: not result or git_of_bytes == blob
# gdp: law bytes that are the blob record their key: result or git_of_bytes != blob
def blob_map_ok(git_of_bytes: str, blob: str) -> bool:
    """Tree._index: a clean tracked file's git blob id -> the sha256 of its bytes, kept for every checkout (blobs.v2.json),
    written only from bytes just hashed whose own git blob id is that blob. Before: written from whatever key the
    per-checkout stat index held for the path, never checked against the blob (11-02 00:56-01:29, 8 entries of
    gdp-wired's tonight's commits mapped to older contents; research/termination/lean-termination.ts shipped as
    main's old content)."""
    return git_of_bytes == blob


# gdp: law a built tree is reused only when its checkouts' git state, its files' stats and its directories' are as built: not result or (same_git and same_files and same_dirs)
# gdp: law a tree as built is reused: result or not (same_git and same_files and same_dirs)
def tree_reusable(same_git: bool, same_files: bool, same_dirs: bool) -> bool:
    """Tree: a client's tree kept per checkout (LIVE/index/<root>.tree.json) and reused while nothing it was made from
    moved: each checkout's HEAD, index, refs and ignore files; each file's size, mtime, ctime, inode and mode (an edit,
    an executable bit); each directory git walks for it (a file added or removed), read with no git process. Before:
    every batch built its tree anew, 3+ git processes forked from the client, ~2,000 files stat'ed, the index rewritten
    (0.8 s of CPU a build, ~10 filler batches a minute: the treadmill's 6.6% of the laptop's CPU, 11-02)."""
    return same_git and same_files and same_dirs


# gdp: law a blob ships only under its own digest: result == (key == digest)
def blob_ok(key: str, digest: str) -> bool:
    """Tree.blobs: a file's bytes ship under their key only where they still hash to it (a file changed while the
    batch shipped it: run again)."""
    return key == digest


# gdp: law a VM absent from a list that may be partial is never gone: not result or deleted or complete
# gdp: law a VM absent from complete lists is gone only after the second: not result or deleted or absences >= 2
def absent_gone(deleted: bool, absent: bool, complete: bool, absences: int) -> bool:
    """reconcile's "gone": boat says deleted, or it's absent from two complete listings in a row. `boat list` pages
    its answer (pageInfo, limit 200): a VM absent from one page is not gone, and the registry never forgets a live VM
    (and its agent's URL and token) for it (B12: 24 VMs' agents unreachable at the 03:16 handoff)."""
    if deleted:
        return True
    return absent and complete and absences >= 2
