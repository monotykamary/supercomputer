"""The scheduler's allocation and handoff rules broken as the code had them before 11-01 R2 (supercomputer/rules/
scheduler.py: reservations, set-asides, preemption, shares, preparations, the take-over and the switch), each law
restated here and asserted refuted with its run by tests/check_laws.sh. Kept apart from broken_rules.py: one file's
search budget for every variant (AvoidIgnored's run went unfound when they shared it).
"""
from typing import Literal

# gdp: law a reservation keeps out only its own need, the slots past it free to others: result or weight + held_for_others > free_slots or not type_ok or prep_fails >= 3 or (timing and (vm_running > 0 or held_for_others > 0)) or (klass == 5 and draining)
def may_slot_whole_vm_kept(klass: int, timing: bool, weight: int, free_slots: int, vm_running: int, draining: bool, held_for_others: int, type_ok: bool, prep_fails: int) -> bool:
    """Before 11-01 R2: a VM set aside for another item kept out every item, whatever the slots past that item's need
    (13:25-13:40: a 2-slot gate item's VM, 4 slots, closed to the 1-slot gate items waiting)."""
    if weight > free_slots or not type_ok or held_for_others > 0 or prep_fails >= 3:
        return False
    if timing and vm_running > 0:
        return False
    if klass == 5 and draining:
        return False
    return True


# gdp: law a reservation whose item left the queue holds no slot: item_queued or result == 0
def reservation_outlives_its_item(own: bool, item_queued: bool, now: float, until: float, need: int) -> int:
    """Before 11-01 R2: a reservation held to its deadline (120 s, 900 s with something to end there) after its item
    ran elsewhere or was done (15 large VMs empty 13:25-13:40, 1,500 gate items queued)."""
    if own or now >= until:
        return 0
    return need


# gdp: law a VM is a candidate whatever its own class runs there, when the slots past what the item may not take fit it: result or slots - keep < need
def set_aside_never_beside_own_class(need: int, slots: int, keep: int, own_class_there: bool) -> bool:
    """Before 11-01 R2: no VM running an item of the item's class or higher was set aside for it (a 2-slot gate item
    found none while gates ran on every VM beside filler)."""
    return slots - keep >= need and not own_class_there


class PreemptStopsAtFirst:
    """Before 11-01 R2: the first item that found nothing lower to take ended the preemption pass for every item after
    it, whatever their shape."""

    def __init__(self):
        self.failed: set[str] = set()
        self.any_failed = False

    # gdp: law an item is passed over only for a shape that failed this pass: result or shape in old(failed)
    def admit(self, shape: str) -> bool:
        return not self.any_failed

    def nothing_for(self, shape: str) -> None:
        self.failed.add(shape)
        self.any_failed = True


class PreemptPassBackoffMarks:
    """Before 11-02 R2: an item backing off marked its shape failed, the rest of its batch passed over with it."""

    def __init__(self):
        self.failed: set[str] = set()

    # gdp: law an item backing off never passes over its shape: not backing_off or (shape in failed) == (shape in old(failed))
    def after(self, shape: str, backing_off: bool, found: bool) -> None:
        if not found:
            self.failed.add(shape)


# gdp: law a waiter of another image never binds a share: not result or same_image
# gdp: law a waiter that can't be placed never binds a share: not result or placeable
def contends_any_waiting_gate(same_image: bool, placeable: bool, klass: int, other_client: bool, under_share: bool) -> bool:
    """Before 11-01 R2: any waiting item of a higher class, of any image and whether or not it could be placed, made
    every submission client's share bind."""
    return klass < 3 or (klass == 3 and other_client and under_share)


class PrepWaiterStruck:
    """Before 11-01 R2: a placement that waited on a preparation and found the batch unprepared failed, a failure
    recorded or not (a finished preparation left marked as running after the VM dropped the batch: 5,748 strikes with
    no reason, 1,107 items to Modal, 11:09-12:21)."""

    def __init__(self):
        self.state: Literal["none", "preparing", "prepared"] = "none"
        self.gone = False

    # gdp: law a waiter fails only on a failure the preparation recorded: result != "fail" or err
    def waited(self, prepared: bool, err: bool) -> Literal["ready", "fail", "again"]:
        if prepared:
            self.state = "prepared"
            return "ready"
        return "fail"


# gdp: law a waiting item never waits for work to end while a VM nearer to empty is there: result or keep_best >= keep_here
def reservation_never_moves(keep_here: int, keep_best: int) -> bool:
    """Before 11-01 R2: an item set aside a VM waited there for its work to end while another VM emptied of all but
    filler stood by (or set aside a second one 20 s later, both held)."""
    return False


# gdp: law after a handoff the plan waits at most one tick: not result or not handoff or waited_s < tick_s
def plan_waits_the_gradual_reattach(reattaching: bool, waited_s: float, handoff: bool = False, tick_s: float = 15.0) -> bool:
    """Before 11-01 R2 (14:33): after a handoff too, the plan waited re-attach's first pass up to 60 s, the pool taken
    back VM by VM."""
    if tick_s <= 0 or tick_s > 60:
        raise ValueError("a tick of 0 to 60 s")
    return reattaching and waited_s < 60


# gdp: law every VM the previous scheduler of this machine held is taken at once: result or not scheduler_lease or not same_host or own or (holder_alive and not holder_is_old)
def take_over_one_by_one(scheduler_lease: bool, same_host: bool, own: bool, holder_alive: bool, holder_is_old: bool, agent_answered: bool, items_there: bool) -> bool:
    """Before 11-01 R2: a VM was taken back only once its agent answered, and only one with items placed there (the
    rest left to adoption's next pass): 14:33, 43 of 225 held, 176 with no live owner."""
    if own:
        return False
    return scheduler_lease and same_host and not holder_alive and agent_answered and items_there


# gdp: law the old scheduler goes only when the new one holds every VM it held but those that died: result != "exit" or (acked and taken + died >= held)
def handoff_exits_at_once(held: int, taken: int, died: int, acked: bool) -> str:
    """Before 11-01 R2: the old scheduler exited as soon as it spawned the new one, holding nothing for it."""
    return "exit"


# gdp: law an item the old scheduler was starting is started by the new one: old_state != "starting" or result == "starting"
def taken_starting_as_running(old_state: Literal["running", "preempting", "starting"]) -> Literal["running", "preempting", "starting"]:
    """Before 11-01 R2 (scenario handoff_1430): a start in flight was taken as running, never sent."""
    return "preempting" if old_state == "preempting" else "running"


# gdp: law the switch never waits for the work in flight a tick or more: result < tick_s
def switch_waits_for_the_pollers(in_flight: int, tick_s: float) -> float:
    """Before 11-01 R2 (15:21:34-15:21:59): the old scheduler joined its pollers, up to 25 s, before its snapshot."""
    if tick_s <= 0:
        raise ValueError("a tick is longer than 0 s")
    return 25.0 if in_flight > 0 else 0.0


# gdp: law a job runs only on a VM type whose usable memory holds what it asks: not result or memory + overhead <= nominal
def fits_nominal_memory(memory: int, nominal: int, overhead: int) -> bool:
    """Before 11-01 R2: a job's memory against a type's nominal memory (cpu 4, memory 16384 on a large VM of 16 GB:
    5 of 48 of the bar's Lean checks OOM-killed after 5-10 min; lane Y's 4 GB jobs on 4 GB VMs the same)."""
    if overhead < 0:
        raise ValueError("an overhead is at least 0")
    return memory <= nominal


# gdp: law a kill short of its limit while the OOM killer killed is reported as out of memory: result or not (code == 137 or code == -9) or seconds >= limit - 1 or oom_after <= oom_before
def oom_a_plain_failure(code: int, seconds: int, limit: int, oom_before: int, oom_after: int) -> bool:
    """Before 11-01 R2: a job the kernel's OOM killer killed was a plain exit 137, nothing said of memory."""
    return False


# gdp: law a slot is waste only when it was on filler or idle beside waiting work at the measure before too: result <= now_n and result <= prev_n
def waste_of_the_instant(now_n: int, prev_n: int) -> int:
    """Before 11-01 R2: the slots of the instant counted, an item's slot freed a moment before the next pass with it."""
    if now_n < 0 or prev_n < 0:
        raise ValueError("a count is at least 0")
    return now_n


# gdp: law a reservation never keeps out an item of a higher class: not higher or result == 0
def reservation_against_any_class(own: bool, item_queued: bool, now: float, until: float, need: int, higher: bool = False) -> int:
    """Before 11-01 R2 (16:26:54, bx_2jse889x): a VM held for a sweep's 2-slot item kept a 2-slot gate item out, the
    sweeps there its to take."""
    if need < 0:
        raise ValueError("a need is at least 0")
    if own or not item_queued or now >= until:
        return 0
    return need


# gdp: law an item is passed over only for a shape that failed with its own memory: not result or failed_memory == memory
def shape_skips_memory_blind(same_rest: bool, failed_memory: int, memory: int) -> bool:
    """Before 11-01 R2 (17:10-18:47): the shape memo left out the item's memory: a 4 GB item that no free small VM held
    passed over a 1-slot item of no stated memory that one did."""
    return same_rest


# gdp: law an item fits no VM it avoids, nor one quarantined, retired or not answering: not result or (not avoided and prep_fails < 3 and not retired and answering)
def vm_fits_memory_only(image_ok: bool, type_ok: bool, memory: int, nominal: int, overhead: int, avoided: bool, prep_fails: int, retired: bool, answering: bool) -> bool:
    """Before 11-01 R2: health's waste read a VM's memory and type but not a placement's other tests."""
    if overhead < 0:
        raise ValueError("an overhead is at least 0")
    return image_ok and type_ok and memory + overhead <= nominal


# gdp: law a VM real work was placed on since the measure before counts no waste: not placed_since or result == 0
def waste_of_a_busy_vm(now_n: int, prev_n: int, placed_since: bool = False) -> int:
    """Before 11-01 R2 (19:14:41, bx_hrp8wttp): a VM whose slots real work filled as they freed counted waste, a slot
    free at each measure, another each time."""
    if now_n < 0 or prev_n < 0:
        raise ValueError("a count is at least 0")
    return min(now_n, prev_n)


# gdp: law a submission client whose share doesn't bind counts all its work as demand: binds or result == work
def sub_demand_capped_beside_any(work: float, share_room: float, binds: bool) -> float:
    """Before 11-01 R2 (19:32): a client's demand capped at its share whenever another client existed, its share
    binding or not: the pool never grew for three clients' 2,400 items."""
    if work < 0 or share_room < 0:
        raise ValueError("slot-time is at least 0")
    return min(work, share_room)


# gdp: law a live treadmill's VM within its hold counts as having filler (kept, never released for a moment's empty filler queue): not treadmill_alive or since_real >= hold_s or result
def filler_only_if_queued(filler_queued: bool, treadmill_alive: bool, since_real: float, hold_s: float) -> bool:
    """Before 11-01 R2 (20:38:15): only filler queued at that moment kept a VM above the floor."""
    return filler_queued


# gdp: law a submission's or a sweep's queued slots are no demand within sub_preempt_s (the policy's wait): klass <= 2 or waited_s >= sub_preempt_s or result == 0
def busy_demand_at_once(klass: int, waited_s: float, sub_preempt_s: float, queued_slots: int, share_ok: bool = True) -> int:
    """Before 11-01 R2 (20:44:59, 21:08-21:38): every queued slot busy's demand at once, a sweep's in its policy's wait
    and a submission its share held back too."""
    if queued_slots < 0:
        raise ValueError("slots are at least 0")
    return queued_slots


# gdp: law a submission its share holds back is no demand: share_ok or result == 0
def busy_demand_past_shares(klass: int, waited_s: float, sub_preempt_s: float, queued_slots: int, share_ok: bool = True) -> int:
    """Before 11-01 R2 (21:08-21:38): a submission its share held back counted as busy's demand."""
    if queued_slots < 0:
        raise ValueError("slots are at least 0")
    if klass <= 2 or waited_s >= sub_preempt_s:
        return queued_slots
    return 0


# gdp: law no VM is started while a free VM of the image that fits the work exists, whatever its type: result <= want and (free_fit <= 0 or result <= want - free_fit or result <= 0)
def start_smaller_beside_free_larger(want: int, free_fit: int) -> int:
    """Before 11-01 R2 (20:46): a group's starts counted its own VM type alone, a free larger VM of the image beside."""
    if want < 0:
        raise ValueError("starts are at least 0")
    return want


# gdp: law a type its batch named binds each item: not named or item_type != "" or result == batch_type
def vm_type_only_for_starts(named: bool, batch_type: str, item_type: str) -> str:
    """Before 11-01 R2 (lane Y's IC3Syn figure batch, 'default' named: large 74, default 32, xlarge 10): a named
    type steered starts only, an item placed on any type that fit."""
    return item_type


# gdp: law a git blob's content key is recorded only from bytes that are that blob: not result or git_of_bytes == blob
def blob_map_from_stat_index(git_of_bytes: str, blob: str) -> bool:
    """Before 11-02 R2: a clean file's blob was mapped to whatever key the per-checkout stat index held for its path,
    never checked against the blob (8 entries wrong, lean-termination.ts shipped as main's old content)."""
    return True


# gdp: law an item fits no VM it avoids, nor one quarantined, retired, not answering or with its disk low: not result or (not avoided and prep_fails < 3 and not retired and answering and disk_fit)
def vm_fits_disk_blind(image_ok: bool, type_ok: bool, memory: int, nominal: int, overhead: int, avoided: bool, prep_fails: int, retired: bool, answering: bool, disk_fit: bool = True) -> bool:
    """Before 11-02 R2: a VM's disk never read; VMs at 99% took items (649 prepares failed, no space left on device)."""
    if not image_ok or not type_ok or avoided or prep_fails >= 3 or retired or not answering:
        return False
    return memory + overhead <= nominal


# gdp: law a VM is replaced for its disk only once empty and low past the grace: not result or (low and empty and low_s >= grace_s)
def disk_replaced_while_running(low: bool, empty: bool, low_s: int, grace_s: int) -> bool:
    """A full VM deleted at once, its running items lost with it."""
    return low


# gdp: law a tree that isn't its manifest is never kept: not result or bad == 0
def tree_kept_always(bad: int) -> bool:
    """Before 11-02 R2: a tree found short of its manifest stayed, ready (bx_ptwy4b7p, 5 files missing)."""
    return True


# gdp: law a VM's partial tree, dropped to be made again, costs the item no strike: result != "strike" or not partial or gone
def prepare_failed_partial_struck(gone: bool, unanswered: bool, in_outage: bool, partial: bool = False) -> str:
    """A VM's partial tree charged to the item, as a stale client key is."""
    if gone:
        return "lost"
    if unanswered or in_outage:
        return "requeue"
    return "strike"


# gdp: law scratch a running job may use is never dropped: not result or mtime < oldest_running
def scratch_drops_all(low: bool, mtime: int, oldest_running: int) -> bool:
    """Every scratch entry dropped with the disk low, a running job's anvil state too."""
    return low


# gdp: law a built tree is reused only when its checkouts' git state, its files' stats and its directories' are as built: not result or (same_git and same_files and same_dirs)
def tree_by_commit(same_git: bool, same_files: bool, same_dirs: bool) -> bool:
    """A cache keyed by the commit alone: a file edited, or one added, since the build ships as built."""
    return same_git


# gdp: law a job runs on a tree only when each file the manifest names a git blob for is that blob: not result or bad == 0
def tree_unchecked(bad: int) -> bool:
    """Before 11-02 R2: a VM ran a batch's jobs on whatever its tree's keys built, no file checked against its blob."""
    return True
