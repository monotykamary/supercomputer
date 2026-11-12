"""The scheduler's broken rules, kept: each the bug a law of supercomputer/rules/scheduler.py rules out, as the code had it (or
would have it), with that law stated again here. `tests/check_laws.sh` asserts every law below comes back broken, with
the run that breaks it, so a change that brings one of these bugs back is caught.
"""
from typing import Literal
from supercomputer.rules.scheduler import Breaker, Delivery, Drain, ItemFate, Pool, Strike, Submit, VMLife, may_slot  # noqa: F401


class SingleProbeTrips(Breaker):
    """The 07:14 incident: a lone failed resume escalated a half-open pool to an hour."""

    # gdp: law a failed sample below three cannot escalate the pause: judged >= 3 or probe_up or (backoff == old(backoff) and open_until == old(open_until) and trips == old(trips))
    def judge(self, now: float, judged: int, bad: int, probe_up: bool) -> Literal["closed", "tripped", "kept"]:
        if self.half and now >= self.open_until and probe_up:
            self.half = False
            self.backoff = self.backoff_s
            return "closed"
        need = 1 if self.half else self.min_judged
        if judged >= need and 2 * bad > judged:
            return self.trip(now)
        return "kept"


class ForceRejudgeClosesOutage(Breaker):
    """An operator command must not be a disguised force-close."""

    # gdp: law a real outage is never forced closed: not outage or open_until == old(open_until)
    def rejudge(self, now: float, judged: int, bad: int, outage: bool, known: bool) -> Literal["closed", "kept"]:
        self.open_until = 0.0
        self.half = False
        return "closed"


class InheritedPauseNeverRejudged(Breaker):
    """The old one-of-one pause survived the 08:20 controller handoff."""

    # gdp: law a rejected inherited sample closes with the normal initial backoff: not (now < old(open_until) and known and not outage and (judged < max(3, min_judged) or 2 * bad <= judged)) or (open_until == 0 and not half and backoff == backoff_s)
    def rejudge(self, now: float, judged: int, bad: int, outage: bool, known: bool) -> Literal["closed", "kept"]:
        return "kept"


class WaitCounted(ItemFate):
    """Before acac7d0: each max_wait with no VM ready was one of boat's failures of the item, even while the
    scheduler's own breaker forbade starts; items reached 3 and went to Modal without boat ever running them."""

    # gdp: law a wait is no boat failure unless boat failed a start of its image during it: fails == old(fails) or start_failed
    def waited(self, start_failed: bool) -> Strike:
        if self.state != "queued":
            return "none"
        return self._strike()


class StrikesPerReport(ItemFate):
    """21:40: one item counted "boat failed it 1, 2, 3 of 3" in the same second on one VM boat had stopped: each report
    of one incident a strike, whatever VM and whether the item still ran there."""

    # gdp: law an item's boat failures are on distinct VMs (one strike per VM incident): fails == old(fails) or vm not in old(avoid)
    # gdp: law a boat failure is charged only to an item last placed on that VM: fails == old(fails) or old(self.vm) == vm
    def boat_failed(self, vm: str) -> Strike:
        if self.state == "done" or self.state == "modal":
            return "none"
        self.state = "queued"
        self.avoid.add(vm)
        return self._strike()


class PlacedOnStopped(ItemFate):
    """A retry placed on a VM boat reports stopped (21:40's VM)."""

    # gdp: law no item is placed on a VM boat reports stopped: state != "placed" or old(state) == "placed" or not vm_stopped
    def place(self, vm: str, vm_stopped: bool) -> bool:
        if self.state != "queued" or vm in self.avoid:
            return False
        self.state = "placed"
        self.vm = vm
        return True


class AvoidIgnored(ItemFate):
    """Before P4's 21:40 fix: a retry after a failed preparation sent back to the VM that failed it."""

    # gdp: law a retry never goes to a VM in the item's avoid set (another VM, or one that replaces it): state != "placed" or old(state) == "placed" or vm not in avoid
    def place(self, vm: str, vm_stopped: bool) -> bool:
        if self.state != "queued" or vm_stopped:
            return False
        self.state = "placed"
        self.vm = vm
        return True


class OrphanByTTL(VMLife):
    """Before 95ec41b: adoption left alone every VM whose TTL was ahead, a VM whose holder died included: 26 orphans
    after restarts, 2 after the 21:34 handoff."""

    # gdp: law no VM is up without an owner after a tick: not up or lease == "ours" or lease == "client" or pending or (lease == "none" and until > now)
    def adopt_tick(self, now: float, work: bool, was_held: bool) -> str:
        if not self.up or self.lease == "ours" or self.lease == "client" or self.pending:
            return "keep"
        if self.until > now:
            return "keep"
        if was_held or work or now - self.last_used < self.hold_s:
            self.lease = "ours"
            return "adopt"
        self.up = False
        self.lease = "none"
        return "stop"


class ReattachForever(VMLife):
    """Before 95ec41b: re-attach tried a VM every 30 s for 20 minutes (40 tries), boat reporting it stopped."""

    # gdp: law re-attach tries a VM at most twice, then leaves it to adoption: tries <= 2
    # gdp: law re-attach never takes back a VM boat reports stopped: lease != "ours" or old(lease) == "ours" or up
    def reattach(self, agent_ok: bool) -> bool:
        if not self.pending:
            return False
        self.tries += 1
        if agent_ok:
            self.lease = "ours"
            self.pending = False
            return True
        if self.tries >= 40:
            self.pending = False
        return False


class Churn(VMLife):
    """Pool churn: a VM released whenever it ran nothing, work of its image queued."""

    # gdp: law no VM is released while real work for its image is queued: lease == old(lease) or lease != "none" or not real_queued
    def release(self, now: float, real_queued: bool, filler_queued: bool) -> bool:
        if not self.up or self.lease != "ours" or self.running > 0:
            return False
        self.lease = "none"
        self.until = now + self.idle_s
        return True


class AdoptPastMax(Pool):
    """A finding, not a regression: scheduler.py on main (and rules.Pool, as main) adopts every orphan whatever
    MAX_VMS (no ceiling unless one is set, 1b0553f; "every one of them is ours to keep or stop now"), so with a ceiling
    set, a handoff can hold more VMs than it. The pool's own starts stay within it (rules.py: "starts never take the
    pool past max_vms"). Refuted here until the policy says which wins."""

    # gdp: law held VMs never exceed max_vms: held + coming <= max_vms
    def adopt(self) -> None:
        self.held += 1


class StartThroughBreaker(Pool):
    """A start made while the breaker is open."""

    # gdp: law no start while the breaker is open (but one probe): coming == old(coming) or (now >= open_until and (not half or coming == old(coming) + 1))
    def grow(self, want: int, now: float) -> int:
        if want <= 0:
            return 0
        n = min(want, self.room())
        self.coming += n
        return n


class ProbePerImage(Pool):
    """scheduler.py on main: half-open, each image's plan let its own probe through (two images wanting, two probes in
    one plan: measured 2). The probe's mark not kept across the plan's images."""

    # gdp: law one probe at a time while half-open: not half or not old(probing) or coming == old(coming)
    def grow(self, want: int, now: float) -> int:
        if want <= 0 or now < self.open_until:
            return 0
        n = min(want, self.room())
        if self.half:
            n = min(n, 1)
            if n > 0:
                self.probing = True  # (marked, never read: the next image's plan probes too)
        self.coming += n
        return n


class ReleaseAllAtOnce(Drain):
    """Before the staged drain: every VM above the floor shed as soon as its hold ended (one big wave's surplus all at
    once, its holds ending together)."""

    # gdp: law in any drain_every_s window at most ceil(drain_step x above-floor) VMs are shed: released <= quota
    def turn(self, now: float, above: int) -> bool:
        if above < 0:
            raise ValueError("a count is at least 0")
        if not self.open:
            self.open = True
            self.share = self.step_pct * above
            self.quota = (self.share + 99) // 100
        self.released += 1
        self.t_last = now
        return True


class FloorReleasedIdle(VMLife):
    """Before the floor's hold: release with nothing at all queued (not even filler) let every idle VM go after ~3 s,
    the floor's included."""

    # gdp: law a VM within the floor is never released while the treadmill is alive (one idle for hold_s aside, never parked): lease == old(lease) or lease != "none" or not within_floor or not floor_held or idle_for >= hold_s
    def release(self, now: float, real_queued: bool, filler_queued: bool, within_floor: bool, floor_held: bool, idle_for: float, drain_ok: bool) -> bool:
        if not self.up or self.lease != "ours" or self.running > 0:
            return False
        if real_queued or (filler_queued and not self.draining):
            return False
        self.lease = "none"
        self.until = now + self.idle_s
        self.last_used = now
        return True


class DoneBeforeRead(Delivery):
    """Before: a batch was done to its client as soon as no item was left, its last row maybe not delivered yet (an
    item of 0.06 s: done, its output fetched over an RPC, then its row); the client stopped with it unread (bar-89160's
    `tasks failed on boat: {}`; 36 batches of 10-31 with rows written after their client closed them)."""

    # gdp: law every item's row reaches its client before the batch is done: not result or read == items
    def poll(self, since: int) -> bool:
        if 0 <= since <= self.rows:
            self.read = self.rows
        return self.finished == self.items


class OrphanStaleUntil(VMLife):
    """A guess at the 23:45 report (13 VMs leased to the dead pid, `until` hours past or 0): adoption leaving a VM
    whose holder died to boat's TTL when its `until` has passed. The rule adopts it whatever its until; the 13 were
    re-attach's, taken back at 23:46:37 (121 VMs one at a time: re-attach now 16 at once)."""

    # gdp: law no VM is up without an owner after a tick: not up or lease == "ours" or lease == "client" or pending or (lease == "none" and until > now)
    def adopt_tick(self, now: float, work: bool, was_held: bool) -> str:
        if not self.up or self.lease == "ours" or self.lease == "client" or self.pending:
            return "keep"
        if self.lease == "none" and self.until > now:
            return "keep"
        if self.lease == "dead" and self.until <= now:
            return "keep"
        if was_held or work or now - self.last_used < self.hold_s:
            self.lease = "ours"
            return "adopt"
        self.up = False
        self.lease = "none"
        return "stop"



# gdp: law no item is placed on a VM whose last 3 preparations failed: not result or prep_fails < 3
def slot_on_a_failing_vm(klass: int, weight: int, free_slots: int, prep_fails: int) -> bool:
    """Before: a VM whose preparations kept failing stayed a candidate (bx_5wr3bs76: 824 failed preparations in 25
    minutes, an interactive check placed there again and again)."""
    return weight <= free_slots


# gdp: law the plan waits for re-attach's first pass at most 60 s: not result or waited_s < 60
def plan_waits_the_pass(reattaching: bool, waited_s: float) -> bool:
    """Before: the plan waited for re-attach's whole first pass (00:09 handoff: 3 min 46 s, one VM's agent answering
    neither boat's front nor boat exec; 1,428 gates queued, no start the while)."""
    return reattaching


# gdp: law a row lost because the VM dropped the batch's tree is never a strike: result == "reprepare" or not tree_gone
def every_lost_row_a_strike(tree_gone: bool) -> str:
    """Before: every lost row a strike, the VM's own dropped tree included (23:40-00:55: over 1,400 of them, 203 items
    to Modal)."""
    return "strike"


# gdp: law the share is work-conserving (with no other client of its class or a higher one waiting, a submission takes a free slot): result or contended
def share_always_binds(klass: int, held: int, weight: int, share: int, contended: bool) -> bool:
    """Before: a submission client held at its share whoever else waited (01:07: 32 slots each, 6,160 items queued,
    117 slots running filler, five VMs drained)."""
    return klass != 3 or held + weight <= share


# gdp: law a real item that waited sub_preempt_s takes a filler's slot: klass > 4 or waited_s < sub_preempt_s or result != "none"
def sweeps_wait_for_filler(klass: int, waited_s: float, sub_preempt_s: float) -> str:
    """Before: a sweep's item never took a filler's slot (01:07: 1,073 queued, 36 running)."""
    if klass <= 2:
        return "filler_sweep"
    if klass == 3 and waited_s >= sub_preempt_s:
        return "filler"
    return "none"


class DrainWithRealQueued(VMLife):
    """Before: a VM drained past its hold whatever real work of its image was queued (01:02:28, five VMs, 6,160
    submission items queued)."""

    # gdp: law no VM starts to drain while real work of its image is queued: old(draining) or not draining or not real_queued
    def hold_tick(self, now: float, generation: int, real_running: bool, above_floor: bool, drain_ok: bool, real_queued: bool) -> bool:
        self.gen = generation
        if real_running:
            self.t_real = now
        if not above_floor:
            self.draining = False
            return False
        if now - self.t_real > self.hold_s and not self.draining and drain_ok:
            self.draining = True
            return True
        return False


# gdp: law a row lost for an item being preempted is its preemption, never a strike: result != "strike" or not preempting
def preempted_row_struck(tree_gone: bool, preempting: bool) -> str:
    """Before: kill_item's preempted row raced by report's lost one (its process killed, no done.json for 0.2 s): a
    preempted filler or sweep item struck as boat's failure."""
    return "reprepare" if tree_gone else "strike"


# gdp: law an item that no held VM of its image may take, with nothing coming, gets a start: result >= 1 or not stuck or coming > 0
def stuck_waits_forever(want: int, stuck: bool, coming: int) -> int:
    """Before: the plan counted the held VMs that had failed an item as its capacity: on one VM, its retry never ran."""
    return want


# gdp: law no client path reaches Modal but through boat_tries boat failures on distinct VMs (or a job Modal owns): result != "modal" or own or failures >= tries
def unavailable_to_modal(own: bool, failures: int, tries: int, modal_on: bool, refused: bool) -> str:
    """Before: a batch boat couldn't take (Unavailable: a tree, an image, a scheduler hiccup) went to Modal at once,
    no strike (11-01 05:03, the treadmill's sweep chc 300:clausal)."""
    if own or refused or failures >= tries:
        return "modal"
    return "boat"


# gdp: law a timing item alone on its VM leaves none of it idle when a VM type has exactly its vCPUs: result == need or (need != 2 and need != 4 and need != 8 and need != 16)
def timing_on_the_batchs_type(need: int) -> int:
    """Before: a timing item took a VM of its batch's type (large, 8 vCPUs) whatever its need (06:35, 490 vCPUs idle)."""
    return 8


class SubmitReplaces(Submit):
    """Before this: a submit replaced the batch with its id whatever it held (a session's every run one id), so a
    batch's queued and running items lost their batch."""

    # gdp: law a submit never replaces a batch with live items: not old(live) or result != "replace"
    def admit(self, same: bool):
        self.result = "replace" if self.exists else "new"
        self.exists, self.live = True, True
        return self.result


# gdp: law an image with recent work keeps at least its default floor: treadmill or override >= 0 or since_real >= floor_idle_s or result >= base
def floor_only_with_treadmill(treadmill: bool, policy_floor: int, base: int, override: int, since_real: float, floor_idle_s: float) -> int:
    """Before this: no floor at all without a treadmill, so a project's VMs went at the end of each burst and the next
    burst started them again (pifovea-img, 11:40-11:54: 3 starts)."""
    if override >= 0:
        return override
    return max(0, policy_floor) if treadmill else 0
