"""The scheduler's decision rules, apart from its threads, its I/O and boat's API: small classes whose methods decide
and record, so each rule is read whole and its laws are proven (`tests/check_laws.sh`: gdp-wired, `# gdp: law` below).
scheduler.py calls them for these decisions and acts on what they say (README, "Scheduling policy").

    ItemFate   one item: queued, placed, done; boat's failures of it (one per VM), and Modal after boat_tries of them
    Pool       the pool's capacity: VMs held and coming, within max_vms and the bring-up cap; the circuit breaker
    VMLife     one VM of the pool: its owner (lease, handoff, adoption, re-attach), the hold above the floor, release
               and stop, the policy generation it follows

Times are the scheduler's clock in seconds (time.time()).
"""
from typing import Literal

# gdp: law usable slots never exceed the VM: 0 <= result and result <= slots
# gdp: law a full foreign VM has no capacity: foreign < slots or result == 0
# gdp: law an exclusive item cannot share foreign work: not timing or foreign == 0 or result == 0
def usable_slots(slots: int, foreign: int, timing: bool) -> int:
    """Capacity a plan can promise, not slots held by unknown runs with no known end."""
    if slots < 0 or foreign < 0:
        raise ValueError("slot counts must be nonnegative")
    if timing and foreign > 0:
        return 0
    return max(0, slots - foreign)

# gdp: law unserved exclusive batches have a lane: result + reusable + coming >= waiting
# gdp: law reusable exclusive lanes do not start VMs: reusable + coming < waiting or result == 0
def exclusive_lanes(waiting: int, reusable: int, coming: int) -> int:
    """A guessed end on another batch is not a reusable exclusive lane."""
    return max(0, waiting - reusable - coming)

# gdp: law a pending candidate never retires voluntarily: not watching or not candidate or not result
# gdp: law completed canaries do not delay retirement: watching or result
# gdp: law another code's canary does not delay retirement: candidate or result
def voluntary_retirement_allowed(watching: bool, candidate: bool) -> bool:
    """Keep the candidate's handoff set until the native verdict, not merely until 600 seconds elapsed."""
    return not (watching and candidate)

Fate = Literal["queued", "placed", "done", "modal"]
Lease = Literal["none", "ours", "client", "dead"]
Strike = Literal["again", "modal", "none"]


class ItemFate:
    """An item's fate. Only a real boat failure counts toward Modal: its VM lost, its row lost, its preparation failed
    (each on a VM, one strike per VM), or a start of its image boat failed while it waited. A wait the scheduler made
    itself (the breaker, a backoff, the bring-up cap) never counts. After boat_tries strikes it goes back to its
    client for Modal."""

    # gdp: law an item goes to Modal only after boat_tries boat failures: state != "modal" or fails >= boat_tries
    # gdp: law an item waiting or placed has a strike left: state == "done" or state == "modal" or fails < boat_tries
    # gdp: law a queued item is eventually placed or handed back (boat failing starts of its image): state == "queued" leads to state != "queued" by boat_tries - fails via waited(start_failed = true)
    # gdp: law an item done or sent to Modal stays so: (old(state) != "done" or state == "done") and (old(state) != "modal" or state == "modal")

    def __init__(self, boat_tries: int):
        if boat_tries < 1:
            raise ValueError("boat_tries is at least 1")
        self.boat_tries = boat_tries
        self.state: Fate = "queued"
        self.fails = 0
        self.vm = ""  # the VM it was last placed on
        self.avoid: set[str] = set()  # VMs that failed it: one strike each, and never placed there again
        self.gen = 0  # the policy generation it follows

    # gdp: law no item is placed on a VM boat reports stopped: state != "placed" or old(state) == "placed" or not vm_stopped
    # gdp: law a queued item offered a VM boat reports up that never failed it is placed: old(state) != "queued" or vm_stopped or vm in old(avoid) or state == "placed"
    # gdp: law a retry never goes to a VM in the item's avoid set (another VM, or one that replaces it): state != "placed" or old(state) == "placed" or vm not in avoid
    def place(self, vm: str, vm_stopped: bool) -> bool:
        """Placed on `vm`: never one boat reports stopped, never one that failed it."""
        if self.state != "queued" or vm_stopped or vm in self.avoid:
            return False
        self.state = "placed"
        self.vm = vm
        return True

    def preempted(self) -> None:
        """Killed for a higher class: queued again at the head of its class, no failure."""
        if self.state == "placed":
            self.state = "queued"

    def finished(self) -> None:
        """Its row came back."""
        if self.state == "placed":
            self.state = "done"

    def reloaded(self, vm: str) -> None:
        """After a restart: queued, its last placement (on `vm`) read back from disk for re-attach to take back."""
        if self.state == "placed" or self.state == "queued":
            self.state = "queued"
            self.vm = vm

    def reattached(self, vm: str) -> None:
        """Re-attach took it back where it runs."""
        if self.state == "queued" and self.vm == vm and vm not in self.avoid:
            self.state = "placed"

    # gdp: law a delivered item is done, or was handed back for Modal: state == "done" or state == "modal"
    def delivered(self) -> None:
        """Its row is in its batch's rows (read back after a restart), or its client closed the batch."""
        if self.state != "modal":
            self.state = "done"

    def requeued(self) -> None:
        """Queued again with no strike: its batch's directory gone from the VM, or boat failing broadly (an outage)."""
        if self.state == "placed":
            self.state = "queued"

    # gdp: law an item's boat failures are on distinct VMs (one strike per VM incident): fails == old(fails) or vm not in old(avoid)
    # gdp: law a boat failure is charged only to an item last placed on that VM: fails == old(fails) or old(self.vm) == vm
    def boat_failed(self, vm: str) -> Strike:
        """Boat failed it on `vm`, the VM it was last placed on (the VM lost or stopped, its row lost, its preparation
        failed, the VM not reached after a restart): queued again, on another VM; a second failure reported for one VM
        is no new strike."""
        if self.state == "done" or self.state == "modal" or self.vm != vm:
            return "none"
        self.state = "queued"
        if vm in self.avoid:
            return "none"
        self.avoid.add(vm)
        return self._strike()

    # gdp: law a VM boat stopped while this machine slept is no strike: fails == old(fails) or not slept
    def vm_lost(self, vm: str, slept: bool) -> Strike:
        """Its VM is lost (boat stopped it, or it stopped answering for good): queued again; a strike unless boat stopped
        it while this machine slept (the TTLs it couldn't renew: our doing, not boat's)."""
        if slept:
            self.requeued()
            return "none"
        return self.boat_failed(vm)

    # gdp: law a wait is no boat failure unless boat failed a start of its image during it: fails == old(fails) or start_failed
    def waited(self, start_failed: bool) -> Strike:
        """Its batch's max_wait passed with no VM of its image ready. A strike only when boat failed a start of its
        image during the wait (refused, or not ready in time); a wait the scheduler made itself keeps its place."""
        if self.state != "queued" or not start_failed:
            return "none"
        return self._strike()

    def _strike(self) -> Strike:
        self.fails += 1
        if self.fails < self.boat_tries:
            self.state = "queued"
            return "again"
        self.state = "modal"
        return "modal"

    # gdp: law an item follows the current policy generation after a tick: gen == generation
    def follow(self, generation: int) -> None:
        self.gen = generation


class Pool:
    """The pool's capacity, every image's: VMs held (ready or being brought up) and starts coming (asked of boat, not
    up yet). Starts are within max_vms (none by default: 10**9) and the bring-up cap; an open circuit breaker allows
    none, and after its pause one start at a time as a probe, until a probe comes up. Adoption takes in every orphan
    whatever max_vms (one already up costs no start)."""

    # gdp: law starts are bounded by the bring-up cap: coming <= bringup

    def __init__(self, max_vms: int, bringup: int, backoff_s: int, backoff_max_s: int):
        if max_vms < 1 or bringup < 1 or backoff_s < 1 or backoff_max_s < backoff_s:
            raise ValueError("a pool needs a VM, a start at a time and a pause")
        self.max_vms = max_vms
        self.bringup = bringup
        self.backoff_s = backoff_s
        self.backoff_max_s = backoff_max_s
        self.held = 0
        self.coming = 0
        self.open_until = 0.0  # no start before this
        self.half = False  # after a pause: one start at a time (a probe) until one comes up
        self.probing = False  # a probe start out
        self.backoff = backoff_s  # the next pause

    def room(self) -> int:
        return max(0, min(self.max_vms - self.held - self.coming, self.bringup - self.coming))

    # gdp: law one probe at a time while half-open: not half or not old(probing) or coming == old(coming)
    # gdp: law starts never take the pool past max_vms: coming == old(coming) or held + coming <= max_vms
    # gdp: law no start while the breaker is open (but one probe): coming == old(coming) or (now >= open_until and (not half or coming == old(coming) + 1))
    def grow(self, want: int, now: float) -> int:
        """How many starts to make now for `want` more VMs; they are counted as coming."""
        if want <= 0 or now < self.open_until:
            return 0
        n = min(want, self.room())
        if self.half:
            if self.probing:
                return 0
            n = min(n, 1)
            if n > 0:
                self.probing = True
        self.coming += n
        return n

    def came_up(self) -> None:
        """A start came up: held; a probe that came up closes the breaker."""
        if self.coming > 0:
            self.coming -= 1
            self.held += 1
        if self.half and self.probing:
            self.half = False
            self.probing = False
            self.backoff = self.backoff_s

    def start_failed(self) -> None:
        """A start refused, or not ready in time."""
        if self.coming > 0:
            self.coming -= 1
        self.probing = False

    def adopt(self) -> None:
        """A VM of the pool with no live owner taken in: every one (no cap; one already up costs no start)."""
        self.held += 1

    def let_go(self) -> None:
        """A VM held is released, stopped or lost."""
        if self.held > 0:
            self.held -= 1

    def trip(self, now: float) -> None:
        """The breaker opens: no start for its backoff (doubled at each trip in a row, up to backoff_max_s), then
        probes."""
        if now < self.open_until:
            return
        self.open_until = now + self.backoff
        self.half = True
        self.probing = False
        self.backoff = min(self.backoff_max_s, self.backoff * 2)


class VMLife:
    """One VM of the pool. Its owner: the scheduler (ours), a client batch, nobody, or a holder that died (a restart,
    a handoff, a kill). Each tick every VM up has a live owner: adoption takes in an orphan when there is work for it
    or the previous scheduler held it or it is within its hold, else stops it; a VM released a moment ago (no lease,
    its TTL ahead) stops by itself. Above the floor a VM
    runs filler for hold_s after its last real work, then drains and is released; a VM running items is never
    released."""

    # gdp: law a VM running items is never released: running == 0 or lease != "none" or not up
    # gdp: law a VM re-attach is taking back has had one try at most: not pending or tries <= 1
    # gdp: law re-attach tries a VM at most twice, then leaves it to adoption: tries <= 2

    def __init__(self, hold_s: int, idle_s: int, ttl_s: int):
        if hold_s < 0 or idle_s < 0 or ttl_s < 1:
            raise ValueError("hold_s and idle_s are at least 0, ttl_s at least 1")
        self.hold_s = hold_s
        self.idle_s = idle_s
        self.ttl_s = ttl_s
        self.up = False
        self.lease: Lease = "none"
        self.until = 0.0  # boat stops it then (its TTL)
        self.last_used = 0.0  # when it was last let go
        self.running = 0  # items running on it
        self.t_real = 0.0  # its last real (non-filler) work
        self.draining = False  # past its hold: no new filler, released once empty
        self.pending = False  # re-attach is taking it back with its items
        self.tries = 0  # re-attach's tries
        self.gen = 0  # the policy generation it follows

    def leased(self, now: float) -> None:
        """The scheduler leased it (a start, a warm VM, a resume)."""
        self.up = True
        self.lease = "ours"
        self.until = now + self.ttl_s
        self.t_real = now
        self.draining = False

    def client_leased(self) -> None:
        """A client batch holds it."""
        if self.up and self.lease == "none":
            self.lease = "client"

    def holder_died(self, items: int) -> None:
        """Its holder is gone (a restart, a handoff, a kill): its items run on, re-attach takes them back."""
        if self.lease != "none":
            self.lease = "dead"
            self.pending = items > 0
            self.tries = 0

    def boat_stopped(self) -> None:
        """boat reports it stopped."""
        self.up = False
        self.running = 0

    # gdp: law re-attach never takes back a VM boat reports stopped: lease != "ours" or old(lease) == "ours" or up
    def reattach(self, agent_ok: bool) -> bool:
        """One try of re-attach: taken back when it is up and its agent answers; after two tries (or once stopped) it is
        left to adoption."""
        if not self.pending:
            return False
        if not self.up:
            self.pending = False
            return False
        self.tries += 1
        if agent_ok:
            self.lease = "ours"
            self.pending = False
            return True
        if self.tries >= 2:
            self.pending = False
        return False

    # gdp: law no VM is up without an owner after a tick: not up or lease == "ours" or lease == "client" or pending or (lease == "none" and until > now)
    # gdp: law adoption stops a VM only with its image's queue empty and its hold over: result != "stop" or (not work and not was_held and now - last_used >= hold_s)
    def adopt_tick(self, now: float, work: bool, was_held: bool) -> Literal["keep", "adopt", "stop"]:
        """Adoption's decision for this VM (every minute and after a restart or handoff): an up VM with no live owner
        is adopted when the previous scheduler held it, its image has queued work, or it is within its hold; else
        stopped. Left alone: one re-attach is taking back, and one released a moment ago (no lease, its TTL ahead:
        boat stops it then, unless a batch leases it). A VM whose holder died is an orphan whatever its TTL."""
        if not self.up or self.lease == "ours" or self.lease == "client" or self.pending:
            return "keep"
        if self.lease == "none" and self.until > now:
            return "keep"
        if was_held or work or now - self.last_used < self.hold_s:
            self.lease = "ours"
            self.t_real = now
            self.draining = False
            return "adopt"
        self.up = False
        self.lease = "none"
        return "stop"

    def placed(self, now: float, real: bool) -> None:
        if not self.up or self.lease != "ours":
            return
        self.running += 1
        if real:
            self.t_real = now
            self.draining = False

    def ended(self) -> None:
        if self.running > 0:
            self.running -= 1

    # gdp: law a VM follows the current policy generation after a tick: gen == generation
    # gdp: law a VM within the floor never drains: not draining or above_floor
    # gdp: law a VM above the floor with no real work for hold_s drains on its turn: old(draining) or not above_floor or real_running or now - old(t_real) <= hold_s or not drain_ok or real_queued or draining
    # gdp: law a VM above the floor drains only after hold_s with no real work: old(draining) or not draining or now - old(t_real) > hold_s
    # gdp: law a VM above the floor starts to drain only on its turn of the staged drain: old(draining) or not draining or drain_ok
    # gdp: law no VM starts to drain while real work of its image is queued: old(draining) or not draining or not real_queued
    def hold_tick(self, now: float, generation: int, real_running: bool, above_floor: bool, drain_ok: bool, real_queued: bool) -> bool:
        """Each tick: the policy generation followed; real work running keeps it; above the floor, hold_s with none
        drains it (its filler queued again) on its turn of the staged drain (Drain.turn). True when it starts to drain."""
        self.gen = generation
        if real_running:
            self.t_real = now
        if not above_floor:
            self.draining = False
            return False
        if now - self.t_real > self.hold_s and not self.draining and drain_ok and not real_queued:
            self.draining = True
            return True
        return False

    # gdp: law no VM is released while real work for its image is queued: lease == old(lease) or lease != "none" or not real_queued
    # gdp: law a VM held here running nothing is released when nothing of its image is queued (the floor's hold and the drain's turn aside): not (old(up) and old(lease) == "ours" and old(running) == 0) or real_queued or filler_queued or (within_floor and floor_held and idle_for < hold_s) or (not within_floor and not old(draining) and not drain_ok) or lease == "none"
    # gdp: law a draining VM running nothing is released when only filler is queued (the floor's hold aside): not (old(up) and old(lease) == "ours" and old(running) == 0 and old(draining)) or real_queued or (within_floor and floor_held and idle_for < hold_s) or lease == "none"
    # gdp: law a VM is released with filler queued only after hold_s on filler: lease == old(lease) or lease != "none" or not filler_queued or old(draining)
    # gdp: law a VM within the floor is never released while the treadmill is alive (one idle for hold_s aside, never parked): lease == old(lease) or lease != "none" or not within_floor or not floor_held or idle_for >= hold_s
    # gdp: law a VM above the floor is released only on its turn of the staged drain: lease == old(lease) or lease != "none" or within_floor or old(draining) or drain_ok
    def release(self, now: float, real_queued: bool, filler_queued: bool, within_floor: bool, floor_held: bool, idle_for: float, drain_ok: bool) -> bool:
        """Released when it runs nothing and nothing of its image is queued, or it drains with only filler queued; it
        stops idle_s later unless a batch leases it first. Within the floor while the treadmill is alive (or floor_always)
        it stays, with nothing to run at most hold_s (busy, never parked: the treadmill's filler keeps it busy); above the
        floor it goes on its turn of the staged drain (Drain.turn: draining was one)."""
        if not self.releasable(real_queued, filler_queued, within_floor, floor_held, idle_for):
            return False
        if not within_floor and not self.draining and not drain_ok:
            return False
        self.lease = "none"
        self.until = now + self.idle_s
        self.last_used = now
        return True

    def releasable(self, real_queued: bool, filler_queued: bool, within_floor: bool, floor_held: bool, idle_for: float) -> bool:
        """Whether release would let it go, the staged drain's turn aside (asked before a turn is taken)."""
        if not self.up or self.lease != "ours" or self.running > 0:
            return False
        if real_queued or (filler_queued and not self.draining):
            return False
        if within_floor and floor_held and idle_for < self.hold_s:
            return False
        return True

    def stop(self) -> None:
        self.up = False
        self.lease = "none"
        self.running = 0


class VMQuiet(VMLife):
    """VMLife in an episode where nothing more is placed on the VM and nothing of its image is queued (gdp.toml: its
    requests are ended, release and boat_stopped): what "eventually" means for a VM with no work."""

    # gdp: law a VM with no work is eventually released (or stopped): up and lease == "ours" leads to lease != "ours" or not up by running via running > 0 -> ended(); running == 0 -> release(now = 0, real_queued = false, filler_queued = false, within_floor = false, floor_held = false, idle_for = 0, drain_ok = true)


class Reattaching(VMLife):
    """VMLife in one re-attach (no further restart: gdp.toml's requests are reattach and boat_stopped)."""

    # gdp: law re-attach ends: pending leads to not pending by 2 - tries via reattach(agent_ok = false)


class Breaker:
    """The circuit breaker (README, "Never runaway"): over half of at least min_judged recent starts not ready in time
    (one, half-open after a pause), or boat's front failing most agents at once, stops every start for its pause,
    doubled at each trip in a row up to backoff_max_s; after the pause, probes (Pool.grow), and a probe that came up
    closes it."""

    # gdp: law a pause never exceeds backoff_max_s: backoff <= backoff_max_s
    # gdp: law a pause is never shorter than the first: backoff >= backoff_s

    def __init__(self, backoff_s: int, backoff_max_s: int, min_judged: int):
        if backoff_s < 1 or backoff_max_s < backoff_s or min_judged < 1:
            raise ValueError("a breaker needs a pause, a longest pause and a sample")
        self.backoff_s = backoff_s
        self.backoff_max_s = backoff_max_s
        self.min_judged = min_judged
        self.open_until = 0.0
        self.half = False
        self.backoff = backoff_s
        self.trips = 0

    # gdp: law the breaker trips on starts only when over half of enough of them failed: trips == old(trips) or (2 * bad > judged and (judged >= min_judged or old(half)))
    # gdp: law a probe that came up after the pause closes the breaker: not (old(half) and now >= old(open_until) and probe_up) or not half
    def judge(self, now: float, judged: int, bad: int, probe_up: bool) -> Literal["closed", "tripped", "kept"]:
        """Every 5 s: the recent starts judged (ready, or not within BREAK_READY), the bad among them."""
        if self.half and now >= self.open_until and probe_up:
            self.half = False
            self.backoff = self.backoff_s
            return "closed"
        need = 1 if self.half else self.min_judged
        if judged >= need and 2 * bad > judged:
            return self.trip(now)
        return "kept"

    # gdp: law a trip while the breaker is open changes nothing: now >= old(open_until) or (open_until == old(open_until) and trips == old(trips))
    def trip(self, now: float) -> Literal["closed", "tripped", "kept"]:
        """The breaker opens (also on an outage of boat's front): no start until now + its pause."""
        if now < self.open_until:
            return "kept"
        self.open_until = now + self.backoff
        self.half = True
        self.trips += 1
        self.backoff = min(self.backoff_max_s, self.backoff * 2)
        return "tripped"


class Watchdog:
    """The controller's own load (README, "Never runaway"): past shed_milli per mille of load per CPU or under 15% memory
    free it sheds (no starts, one send at a time); it resumes once under 1 per CPU and over 25% free (hysteresis)."""

    # gdp: law the watchdog sheds only when the controller is loaded: not shed or old(shed) or per_cpu_milli > shed_milli or mem_pct < 15
    # gdp: law it stops shedding only when the controller is cool: shed or not old(shed) or (per_cpu_milli < 1000 and mem_pct > 25)

    def __init__(self, shed_milli: int):
        if shed_milli < 1000:
            raise ValueError("shed past at least one per CPU")
        self.shed_milli = shed_milli
        self.shed = False

    def sample(self, per_cpu_milli: int, mem_pct: int) -> bool:
        """Every 5 s: the load average per CPU (per mille), the memory free (percent; 100 where unknown)."""
        hot = per_cpu_milli > self.shed_milli or mem_pct < 15
        cool = per_cpu_milli < 1000 and mem_pct > 25
        if hot and not self.shed:
            self.shed = True
        elif cool and self.shed:
            self.shed = False
        return self.shed


class Install:
    """The code-hash handoff: a new install, unchanged for stable_s, that imports, is handed the pool; a broken one
    never (the old scheduler runs on, with an alert)."""

    # gdp: law a handoff comes only after the install stood unchanged stable_s and imports: result != "handoff" or (imports and installed == old(seen) and now - old(t_seen) >= stable_s)
    def __init__(self, code: str, stable_s: int):
        if stable_s < 1:
            raise ValueError("stable_s is at least 1")
        self.code = code
        self.stable_s = stable_s
        self.seen = code
        self.t_seen = 0.0

    def tick(self, now: float, installed: str, imports: bool) -> Literal["stay", "handoff"]:
        if installed == self.code:
            return "stay"
        if installed != self.seen:
            self.seen = installed
            self.t_seen = now
            return "stay"
        if now - self.t_seen >= self.stable_s and imports:
            return "handoff"
        return "stay"


class Policy:
    """The policy file (LIVE/policy.json): its knobs and a generation, written whole and renamed in; every write
    raises the generation by one, and a knob the policy doesn't have is refused."""

    # gdp: law each write of the policy raises its generation by exactly one: generation == old(generation) + 1 or not known
    # gdp: law a knob the policy doesn't have is refused, the generation unchanged: known or generation == old(generation)

    def __init__(self, generation: int):
        if generation < 0:
            raise ValueError("a generation is at least 0")
        self.generation = generation

    def write(self, known: bool) -> bool:
        if not known:
            return False
        self.generation += 1
        return True


class Acquire:
    """The scheduler's VMs for n wanted (Scheduler._acquire): warm free ones of the pool first (no start), then stopped
    ones resumed, then created, the starts within the budget."""

    # gdp: law the starts of one acquisition never exceed its budget: resumes + creates <= budget
    # gdp: law an acquisition never takes more than it wants: warm + resumes + creates <= want
    # gdp: law warm VMs come before any start: resumes + creates == 0 or warm == want or warm == up

    def __init__(self):
        self.want = 0
        self.budget = 0
        self.warm = 0
        self.resumes = 0
        self.creates = 0

    def plan(self, want: int, up: int, stopped: int, budget: int) -> int:
        """`up`: free warm VMs of the image and type; `stopped`: free stopped ones; `budget`: the starts to be had."""
        if want < 0 or up < 0 or stopped < 0 or budget < 0:
            raise ValueError("counts are at least 0")
        self.want = max(0, want)
        self.budget = max(0, budget)
        self.warm = min(self.want, max(0, up))
        self.resumes = max(0, min(max(0, stopped), self.want - self.warm, self.budget))
        self.creates = max(0, min(self.want - self.warm - self.resumes, self.budget - self.resumes))
        return self.warm + self.resumes + self.creates


# gdp: law an outage is at least two ready VMs, half of them or more failing: not result or (ready >= 2 and 2 * failing >= ready)
def outage(ready: int, failing: int) -> bool:
    """boat's front failing most VMs' agents at once (the 10-31 incident): no VM is at fault, none let go."""
    return ready >= 2 and failing * 2 >= ready


# gdp: law a VM boat reports up is never declared lost: not result or stopped or not boat_up
def vm_lost(fails: int, in_outage: bool, boat_up: bool, stopped: bool) -> bool:
    """A VM whose ticks keep failing: lost when boat reports it stopped, or after 12 failed ticks (90 in an outage,
    ~30 minutes) with boat not reporting it up."""
    if stopped:
        return True
    return fails >= (90 if in_outage else 12) and not boat_up


# gdp: law an image with recent work keeps at least its default floor: treadmill or override >= 0 or since_real >= floor_idle_s or result >= base
# gdp: law an image idle past floor_idle_s holds no floor VM: treadmill or since_real < floor_idle_s or result == 0
def image_floor(treadmill: bool, policy_floor: int, base: int, override: int, since_real: float, floor_idle_s: float) -> int:
    """The VMs an image keeps when they have nothing to run: with a treadmill the policy's floor (its filler keeps them
    busy); without one, the default floor (1) while the image had real work within floor_idle_s, so a project between
    bursts keeps one warm VM instead of releasing and starting again (pifovea-img, 11:40-11:54: 3 starts), and none past
    it (a project that stopped holds nothing). `override` (floor_by_image; -1 for none) replaces either, within the
    same decay without a treadmill. A floor keeps VMs; it never starts one."""
    if not treadmill and since_real >= floor_idle_s:
        return 0
    if override >= 0:
        return override
    if treadmill:
        return max(0, policy_floor)
    return max(0, base)


# gdp: law an agent that doesn't answer on a VM boat reports up costs the item no strike: result != "strike" or not unanswered or gone
# gdp: law a preparation that fails on a VM boat stopped loses the VM: result == "lost" or not gone
# gdp: law a VM's partial tree, dropped to be made again, costs the item no strike: result != "strike" or not partial or gone
def prepare_failed(gone: bool, unanswered: bool, in_outage: bool, partial: bool = False) -> Literal["lost", "requeue", "strike"]:
    """A preparation (or a start) failed on a VM: boat stopped it (lost: its items, this one too, struck once); its
    agent didn't answer on a VM boat reports up (repaired in place, the item queued again, no strike); boat failing
    broadly (no strike); the VM's tree of the batch lacking files of its manifest, dropped there and made again at the
    next preparation (agent.tree_kept; no strike: the VM's, not the item's nor the client's); else a strike."""
    if gone:
        return "lost"
    if unanswered or in_outage or partial:
        return "requeue"
    return "strike"


# gdp: law a timing item is placed only on an empty VM: not result or not timing or vm_running <= 0
# gdp: law no filler is placed on a draining VM: not result or klass != 5 or not draining
# gdp: law no item is placed where it doesn't fit: not result or weight <= free_slots
# gdp: law no item is placed on a VM whose last 3 preparations failed: not result or prep_fails < 3
# gdp: law no item takes the slots held for another item's reservation: not result or weight + held_for_others <= free_slots
# gdp: law a timing item never takes a VM with slots held for another: not result or not timing or held_for_others <= 0
# gdp: law a reservation keeps out only its own need, the slots past it free to others: result or weight + held_for_others > free_slots or not type_ok or prep_fails >= 3 or (timing and (vm_running > 0 or held_for_others > 0)) or (klass == 5 and draining)
def may_slot(klass: int, timing: bool, weight: int, free_slots: int, vm_running: int, draining: bool, held_for_others: int, type_ok: bool, prep_fails: int) -> bool:
    """A VM of the item's image, ready, as a candidate for it (Scheduler._slot). Never one whose preparations keep
    failing (bx_5wr3bs76 after the 23:40 handoff: its agent repaired 47 times, 824 preparations failed in 25 minutes,
    an interactive check placed there again and again): its items go elsewhere, and it goes idle, released by its hold.
    `held_for_others`: the slots a reservation holds there for another queued item (reservation_holds); only those are
    kept out (11-01 13:25-13:40: a VM set aside for a 2-slot gate item kept all 4 of its slots from everyone, for 15
    minutes after that item ran elsewhere: 15 VMs, 60 slots idle while 1,500 gate items of 1 slot waited)."""
    if held_for_others < 0:
        raise ValueError("slots held are at least 0")
    if weight + held_for_others > free_slots or not type_ok or prep_fails >= 3:
        return False
    if timing and (vm_running > 0 or held_for_others > 0):
        return False
    if klass == 5 and draining:
        return False
    return True


# gdp: law a reservation whose item left the queue holds no slot: item_queued or result == 0
# gdp: law a reservation past its deadline holds no slot: now < until or result == 0
# gdp: law an item is never kept out of its own reservation: not own or result == 0
# gdp: law a reservation holds at most its item's need: result <= need
# gdp: law a live reservation of another queued item holds its need: own or not item_queued or now >= until or result == need or higher
# gdp: law a reservation never keeps out an item of a higher class: not higher or result == 0
def reservation_holds(own: bool, item_queued: bool, now: float, until: float, need: int, higher: bool = False) -> int:
    """The slots a VM set aside for a wide or timing item (Scheduler._preempt_for) keeps from others: its item's need
    (its weight; a timing item's, the whole VM), only while that item is still queued and before the deadline. Before:
    the reservation outlived its item, placed elsewhere or done (11-01 13:25: 15 large VMs held empty to 13:40, counted
    busy by health's "exclusive")."""
    if need < 0:
        raise ValueError("a need is at least 0")
    if own or not item_queued or now >= until or higher:  # (a lower class's: a gate takes those slots as it takes the
        return 0  # lower items' own; 16:26:54, a 2-slot gate item waited beside a VM held for a sweep's)
    return need


# gdp: law a VM set aside can take the item once what it may not take ends: not result or slots - keep >= need
# gdp: law a VM is a candidate whatever its own class runs there, when the slots past what the item may not take fit it: result or slots - keep < need
def may_set_aside(need: int, slots: int, keep: int, own_class_there: bool) -> bool:
    """A VM as one to set aside for an item needing `need` slots (a weight past 1, or a timing item's whole VM): `keep`
    the slots of what it may not take (its own class or higher, a submission's, foreign runs), left to end. Before: no
    VM running an item of its class or higher was a candidate (11-01 13:25: weight-2 gate items found none on a pool
    where gates ran on every VM beside filler, and the first that failed stopped every preemption after it)."""
    return slots - keep >= need


# gdp: law a waiting item moves only to a VM nearer to empty: not result or keep_best < keep_here
# gdp: law a waiting item never waits for work to end while a VM nearer to empty is there: result or keep_best >= keep_here
def reservation_moves(keep_here: int, keep_best: int) -> bool:
    """An item waiting on the VM set aside for it for `keep_here` slots of what it may not take to end moves to the best
    candidate now (`keep_best` such slots) only when that one is nearer to empty. Before: it stayed, or (every 20 s) set
    aside another VM as well, each held for it."""
    return keep_best < keep_here


# gdp: law a slot is waste only when it was on filler or idle beside waiting work at the measure before too: result <= now_n and result <= prev_n
# gdp: law a slot held so across two measures, no real item placed on its VM between, is waste: placed_since or result >= now_n or result >= prev_n
# gdp: law a VM real work was placed on since the measure before counts no waste: not placed_since or result == 0
def waste_held(now_n: int, prev_n: int, placed_since: bool = False) -> int:
    """Health's waste on one VM: the slots counted now and at the measure before (15 s), the lesser. Before: the slots
    of the instant, those an item had freed a moment before the next pass filled them (16:06-16:10, ~40 items a
    second ending: 1-2 slots, the canary handing back a sound code for them)."""
    if now_n < 0 or prev_n < 0:
        raise ValueError("a count is at least 0")
    if placed_since:  # (its slots filled as they freed: a slot free at each measure, a different one each time; 19:14:41,
        return 0  # bx_hrp8wttp, 3 gates and the fourth slot between two of their items)
    return min(now_n, prev_n)


# gdp: law a VM's disk is fit only with its threshold free: not result or free_mb >= min_mb or free_mb * 10 >= total_mb
# gdp: law a VM with its threshold free is fit: result or (free_mb < min_mb and free_mb * 10 < total_mb)
def disk_ok(free_mb: int, total_mb: int, min_mb: int) -> bool:
    """A VM's free disk (its agent's word with each tick) against the threshold: min_mb, or a tenth of a smaller disk.
    Before: nothing read it (11-02 05:35-09:00, VMs at 99% with 1.8 GB free took items: 649 prepares failed, no space
    left on device, each a strike against an item)."""
    if free_mb < 0 or total_mb < 0 or min_mb < 0:
        raise ValueError("disk sizes are at least 0")
    return free_mb >= min_mb or free_mb * 10 >= total_mb


# gdp: law a VM is replaced for its disk only once empty and low past the grace: not result or (low and empty and low_s >= grace_s)
# gdp: law a VM empty with its disk low past the grace is replaced, never reused: result or not low or not empty or low_s < grace_s
def disk_replaced(low: bool, empty: bool, low_s: int, grace_s: int) -> bool:
    """A VM whose disk stays below the threshold (its agent's cleaning, started at the first low report, didn't free it
    within grace_s) is deleted once nothing runs there, and a VM of the next start replaces it: never let go to the
    pool, where a resume would find it full again."""
    if low_s < 0 or grace_s < 0:
        raise ValueError("durations are at least 0")
    return low and empty and low_s >= grace_s


# gdp: law an item fits no VM whose usable memory can't hold it: not result or memory + overhead <= nominal
# gdp: law an item fits no VM it avoids, nor one quarantined, retired, not answering or with its disk low: not result or (not avoided and prep_fails < 3 and not retired and answering and disk_fit)
# gdp: law an item fits a VM of its image and type that holds its memory, not avoided, quarantined, retired, silent or full: result or not image_ok or not type_ok or memory + overhead > nominal or avoided or prep_fails >= 3 or retired or not answering or not disk_fit
def vm_fits(image_ok: bool, type_ok: bool, memory: int, nominal: int, overhead: int, avoided: bool, prep_fails: int, retired: bool, answering: bool, disk_fit: bool = True) -> bool:
    """Whether a VM can take an item at all, its slots aside: the one test placement (_slot), preemption (its set-asides
    and its victims) and health's waste all read. Before: each had its own (waste one, placement another), and
    placement's shape memo left out the item's memory, so a 4 GB item that fit no free small VM passed over a 1-slot
    item of no stated memory that did (17:10-18:47: small VMs empty, or on a sweep, beside waiting 1-slot gate items)."""
    if overhead < 0:
        raise ValueError("an overhead is at least 0")
    if not image_ok or not type_ok or avoided or prep_fails >= 3 or retired or not answering or not disk_fit:
        return False
    return memory + overhead <= nominal


# gdp: law an item is passed over only for a shape that failed with its own memory: not result or failed_memory == memory
# gdp: law an item of a shape that failed with its own memory is passed over: result or not same_rest or failed_memory != memory
def shape_skips(same_rest: bool, failed_memory: int, memory: int) -> bool:
    """A pass's memo: an item is passed over when an item of its shape found no VM this pass; the shape is all the fit
    reads (image, class, timing, weight, VM type, avoid set) and its memory."""
    return same_rest and failed_memory == memory


# gdp: law a submission client whose share doesn't bind counts all its work as demand: binds or result == work
# gdp: law a client whose share binds counts at most its share's room: not binds or result <= share_room or result <= work
def sub_demand(work: float, share_room: float, binds: bool) -> float:
    """The plan's demand of a submission client's queued slot-time: all of it while its share doesn't bind (no placeable
    waiter it holds back: rules.contends, as placement reads it), else what its share's room clears in the horizon.
    Before: capped whenever another client or a gate existed at all (19:32: three chc-paired clients, 2,400 items,
    waits p50 25 min, the plan's want 0, sweeps on 64 VMs)."""
    if work < 0 or share_room < 0:
        raise ValueError("slot-time is at least 0")
    return min(work, share_room) if binds else work


# gdp: law a live treadmill's VM within its hold counts as having filler (kept, never released for a moment's empty filler queue): not treadmill_alive or since_real >= hold_s or result
# gdp: law filler queued is filler: not filler_queued or result
def filler_coming(filler_queued: bool, treadmill_alive: bool, since_real: float, hold_s: float) -> bool:
    """Whether a VM's release reads filler as queued: filler queued, or a live treadmill (it keeps filler queued for
    the pool's slots, its next round seconds away) within the VM's hold since its last real work. Before: a moment's
    empty filler queue let VMs 2 minutes past their real work go (20:38:15, 39 VMs), the next wave 2-8 minutes later
    starting and resuming others (20:40-20:46, churn 3)."""
    return filler_queued or (treadmill_alive and since_real < hold_s)


# gdp: law busy's demand counts a queued item only once it may take a slot by the policy: result <= queued_slots
# gdp: law a gate's or an interactive check's queued slots are demand at once, any other's after sub_preempt_s: result == queued_slots or not share_ok or (klass > 2 and waited_s < sub_preempt_s)
# gdp: law a submission's or a sweep's queued slots are no demand within sub_preempt_s (the policy's wait): klass <= 2 or waited_s >= sub_preempt_s or result == 0
# gdp: law a submission its share holds back is no demand: share_ok or result == 0
def busy_demand(klass: int, waited_s: float, sub_preempt_s: float, queued_slots: int, share_ok: bool = True) -> int:
    """Busy's demand of a queued item: its slots once the policy lets it take a lower item's (a gate's at once, a
    submission's or a sweep's after sub_preempt_s); before, it waits by the policy, and the filler beside it is not
    real work's to have (20:44:59: 180 sweep slots fresh in the queue, busy 165 of 467 with waste 0)."""
    if queued_slots < 0:
        raise ValueError("slots are at least 0")
    if not share_ok:  # (the policy holds it back for another client's turn: 21:08-21:38, 326 submission slots so)
        return 0
    if klass <= 2 or waited_s >= sub_preempt_s:
        return queued_slots
    return 0


# gdp: law no VM is started while a free VM of the image that fits the work exists, whatever its type: result <= want and (free_fit <= 0 or result <= want - free_fit or result <= 0)
# gdp: law the starts free VMs don't cover are made: free_fit < 0 or result >= want - free_fit
def starts_beside_free(want: int, free_fit: int) -> int:
    """The plan's starts for a group of work (an image and a VM type) less the VMs of the image of other types, empty or
    running only what the work may take, with its slots and usable memory: each one of its before any start, a larger
    one too. Before: a group's starts counted its own type alone (20:46: three default VMs started beside large VMs
    of the image)."""
    if want < 0:
        raise ValueError("starts are at least 0")
    return max(0, want - max(0, free_fit))


# gdp: law a type its batch named binds each item: not named or item_type != "" or result == batch_type
# gdp: law an item's own type binds it: item_type == "" or result == item_type
# gdp: law an item with no type named keeps any VM that fits: named or item_type != "" or result == ""
def placement_type(named: bool, batch_type: str, item_type: str) -> str:
    """The VM type an item may be placed on ("" any that fits): its own, else its batch's when its client named it
    (--vm-type, a job's "vm_type"); a placement constraint, so reuse, starts and preemption are within it. Before:
    a named type steered starts only, placement took any type that fit (lane Y's IC3Syn figure batch, 'default'
    named: large 74, default 32, xlarge 10; a timing claim needs the hardware it names)."""
    if item_type != "":
        return item_type
    if named:
        return batch_type
    return ""


class PreemptPass:
    """One pass of preemption over the queue (Scheduler.schedule, step 2): an item that finds nothing lower to take
    marks its shape (image, class, weight, timing, VM type) failed for the pass, and the items of that shape after it
    are passed over; items of any other shape still try. Before: the first item that found nothing ended the pass
    (a 2-slot gate item with no VM to set aside kept every submission behind it from a filler's slot)."""

    def __init__(self):
        self.failed: set[str] = set()

    # gdp: law an item is passed over only for a shape that failed this pass: result or shape in old(failed)
    # gdp: law a shape that failed is not tried again in the pass: not result or shape not in failed
    def admit(self, shape: str) -> bool:
        return shape not in self.failed

    # gdp: law a shape that found nothing is passed over for the rest of the pass: shape in failed
    def nothing_for(self, shape: str) -> None:
        self.failed.add(shape)

    # gdp: law an item backing off never passes over its shape: not backing_off or (shape in failed) == (shape in old(failed))
    # gdp: law an item that tried and found nothing passes over its shape: backing_off or found or shape in failed
    def after(self, shape: str, backing_off: bool, found: bool) -> None:
        """An item's turn: one that tried and found nothing marks its shape; one backing off didn't try (it can't take a
        VM before its wait ends), so the items of its shape after it still try. Before: a timing item backing off 60 s
        (its preparation failed on an agent that didn't answer: no strike, no avoid set) marked the shape every
        item of its batch has, and none of them got a VM set aside till its wait ended (09:25, 09:55: laneQ's 4-slot
        timing items one set-aside a minute, a filler-only xlarge beside them, health's waste 0.3)."""
        if not backing_off and not found:
            self.failed.add(shape)


# gdp: law a waiter of another image never binds a share: not result or same_image
# gdp: law a waiter that can't be placed never binds a share: not result or placeable
# gdp: law a submission client's own items never bind its share: not result or klass != 3 or other_client
# gdp: law a placeable waiter of the image of a higher class binds a share: result or not same_image or not placeable or klass >= 3
def contends(same_image: bool, placeable: bool, klass: int, other_client: bool, under_share: bool) -> bool:
    """Whether a waiting item makes a submission client's share bind (within_share's `contended`): only an item of the
    same image that could take the slot (its shape found a VM this pass, it isn't backing off), of a higher class, or
    of another submission client under its own share. Before: any waiting gate of any image did (a gate of another
    project's image, or one no VM could take, held every submission client at 8 VMs while filler ran)."""
    if not same_image or not placeable:
        return False
    return klass < 3 or (klass == 3 and other_client and under_share)


# gdp: law a submission client holds no more than its share of slots while another client is waiting: not result or klass != 3 or held + weight <= share or not contended
# gdp: law the share is work-conserving (with no other client of its class or a higher one waiting, a submission takes a free slot): result or contended
def within_share(klass: int, held: int, weight: int, share: int, contended: bool) -> bool:
    """A submission client holds at most SUB_VMS VMs' worth of slots (`share`) at once while another client of its
    class or a higher one is waiting for slots (`contended`); otherwise a free slot goes to whoever has real work queued
    (10-31 01:07: each submission client held at 32 slots, 6,160 items queued, 117 slots running filler). Other classes
    have no share."""
    return klass != 3 or held + weight <= share or not contended


# gdp: law a submission's item is never taken for another's: result == "none" or result == "filler" or result == "filler_sweep"
# gdp: law only interactive checks and gates take a sweep's slot: result != "filler_sweep" or klass <= 2
# gdp: law a submission or a sweep takes a filler's slot only after waiting sub_preempt_s: result != "filler" or klass <= 2 or waited_s >= sub_preempt_s
# gdp: law a real item that waited sub_preempt_s takes a filler's slot: klass > 4 or waited_s < sub_preempt_s or result != "none"
def victims(klass: int, waited_s: float, sub_preempt_s: float) -> Literal["none", "filler", "filler_sweep"]:
    """Whose slot a queued item may take at once (Scheduler._preempt_for): no slot runs filler while a real item
    waits for one past sub_preempt_s (a sweep's too: 01:07, 1,073 sweep items queued, 36 running, 117 slots on filler)."""
    if klass <= 2:
        return "filler_sweep"
    if klass <= 4 and waited_s >= sub_preempt_s:
        return "filler"
    return "none"


# gdp: law a batch with items left is closed only after orphan_s with no poll: not result or left <= 0 or now - t_poll > orphan_s
def abandoned(now: float, t_poll: float, left: int, orphan_s: int) -> bool:
    """A batch no client has polled for orphan_s (4 x that once done) is closed: its client is gone."""
    return now - t_poll > (orphan_s if left > 0 else 4 * orphan_s)


# gdp: law a restart keeps every VM running items: result or running <= 0 or dead
def keep_across_restart(running: int, dead: bool) -> bool:
    """A restart (a deploy, the service restarted): a VM running items keeps them (its TTL renewed, its lease let go
    for the next scheduler to take back); the others are released."""
    return running > 0 and not dead


# gdp: law a VM with no limit set has none: limit > 0 or result >= 1000000000
def max_vms_of(limit: int) -> int:
    """MAX_VMS from the policy: 0 (or unset) is no limit."""
    return limit if limit > 0 else 1000000000



# gdp: law no actions with a missing live admission: not result or missing == 0
# gdp: law no actions with an unexpected live admission: not result or extra == 0
# gdp: law no actions with any unclassified admission: not result or unclassified == 0
# gdp: law a complete decoded live set allows actions: missing != 0 or extra != 0 or unclassified != 0 or result
def recovery_ready(missing: int, extra: int, unclassified: int) -> bool:
    """The startup barrier: ownership is complete before any worker may mutate."""
    return missing == 0 and extra == 0 and unclassified == 0


class Preparation:
    """A batch's directory on one VM (Scheduler._prepare): prepared once; while one placement prepares it the others
    wait for it; a preparation that fails is not kept, so the next placement there tries again (before 52a8f6b a failed
    one was kept: every later placement failed at once, 7,623 strikes in 22:00-22:30)."""

    # gdp: law a prepared batch is never prepared again unless the VM dropped it: not (old(state) == "prepared") or state == "prepared" or gone

    def __init__(self):
        self.state: Literal["none", "preparing", "prepared"] = "none"
        self.gone = False  # the VM dropped its copy (the agent's GC of a quiet batch)

    # gdp: law a placement prepares only a batch not prepared and not being prepared: result != "prepare" or old(state) == "none"
    # gdp: law a placement after a failed preparation prepares again: old(state) != "none" or result == "prepare"
    def begin(self) -> Literal["ready", "wait", "prepare"]:
        if self.state == "prepared":
            return "ready"
        if self.state == "preparing":
            return "wait"
        self.state = "preparing"
        return "prepare"

    # gdp: law a failed preparation is not kept (the next placement tries again): ok or old(state) != "preparing" or state == "none"
    def done(self, ok: bool) -> None:
        if self.state == "preparing":
            self.state = "prepared" if ok else "none"

    # gdp: law a preparation the VM dropped is made again by the next placement, never waited on: state == "none"
    def dropped(self) -> None:
        """The VM's copy gone (the agent's "isn't prepared here", a row lost for a dropped tree)."""
        self.state = "none"
        self.gone = True

    # gdp: law a waiter fails only on a failure the preparation recorded: result != "fail" or err
    # gdp: law a waiter whose preparation ended with nothing recorded prepares again: prepared or err or result == "again"
    # gdp: law a waiter of a preparation that ended prepared goes on: not prepared or result == "ready"
    def waited(self, prepared: bool, err: bool) -> Literal["ready", "fail", "again"]:
        """A placement that waited on another's preparation of the batch there, once it ended. Before: a waiter found
        the batch unprepared with no failure recorded (the VM had dropped it after a preparation that succeeded, its
        finished one left marked as running) and was struck at once, again and again: 11-01 11:09-12:21, 5,748 strikes
        with no reason, 1,107 items of laneP3's three batches sent to Modal."""
        if prepared:
            self.state = "prepared"
            return "ready"
        if err:
            return "fail"
        self.state = "none"  # (the VM dropped it, or the wait ran out: made again)
        self.gone = True
        return "again"


# gdp: law a VM stopped is no strike while this machine woke from a sleep within window_s: result == (now - t_wake < window_s)
def slept_through(now: float, t_wake: float, window_s: int) -> bool:
    """Whether a VM boat stopped is this machine's doing: it slept (its TTLs not renewed) and woke within window_s."""
    return now - t_wake < window_s


# gdp: law a loop that took over gap_s is a sleep: result == (took > gap_s)
def woke(took: float, gap_s: int) -> bool:
    """A scheduler loop that took over gap_s (60): this machine slept; every VM's TTL renewed and adoption now."""
    return took > gap_s


# gdp: law aging never ranks an item above gate: result >= 2 or result == klass
# gdp: law aging never ranks an item below its own class: result <= klass
# gdp: law an item rises at most one class per step_s waited: result >= klass - 3 and (result >= klass - 2 or waited_s >= 3 * step_s) and (result >= klass - 1 or waited_s >= 2 * step_s) and (result == klass or waited_s >= step_s)
# gdp: law a sweep item that waited two steps ranks with gates: klass != 4 or waited_s < 2 * step_s or result == 2
def aged(klass: int, waited_s: float, step_s: int) -> int:
    """A proposal (P4's, not yet wired): a queued item's place in the queue's order rises one class per step_s waited,
    never above gate (2) and never for interactive checks or gates; its class for preemption and growth unchanged."""
    if klass < 1 or klass > 5 or waited_s < 0 or step_s < 1:
        raise ValueError("a class 1 to 5, a wait at least 0, a step at least 1 s")
    if klass <= 2:
        return klass
    up = 3 if waited_s >= 3 * step_s else 2 if waited_s >= 2 * step_s else 1 if waited_s >= step_s else 0
    return max(2, klass - up)



class Drain:
    """The staged drain (README, "Shrink, damped"): the VMs above the floor are shed by at most step_pct percent of
    them (rounded up) a window, a window lasting until every_s after its last shed: a big wave's surplus goes in steps,
    never at once. Each turn is one VM shed (its drain started, or released with nothing queued)."""

    # gdp: law in any drain_every_s window at most ceil(drain_step x above-floor) VMs are shed: released <= quota
    # gdp: law a window's quota is its share of the VMs above the floor, rounded up: 100 * quota <= share + 99 and 100 * quota >= share

    def __init__(self, step_pct: int, every_s: int):
        if step_pct < 1 or step_pct > 100 or every_s < 1:
            raise ValueError("a step of 1 to 100 percent, a window of at least 1 s")
        self.step_pct = step_pct
        self.every_s = every_s
        self.open = False
        self.t_last = 0.0  # the window's last shed
        self.share = 0  # step_pct x the VMs above the floor at the window's start
        self.quota = 0
        self.released = 0

    # gdp: law a shed past a window's quota starts a new window only every_s after the last: not result or old(released) < old(quota) or not old(open) or now - old(t_last) >= every_s
    def turn(self, now: float, above: int) -> bool:
        """A VM above the floor (of `above` now) asks to be shed: True when this is its turn."""
        if above < 0:
            raise ValueError("a count is at least 0")
        if not self.open or (self.released >= self.quota and now - self.t_last >= self.every_s):
            self.open = True
            self.share = self.step_pct * above
            self.quota = (self.share + 99) // 100
            self.released = 0
        if self.released < self.quota:
            self.released += 1
            self.t_last = now
            return True
        return False


class Draining(Drain):
    """Drain while VMs above the floor keep asking (gdp.toml's requests: turn alone)."""

    # gdp: law a VM above the floor is eventually shed while any is above it: open leads to released >= 1 or not open via turn(now = t_last + every_s, above = 1)


class Submit:
    """A submit under a batch id (Scheduler.submit). Before: a submit replaced whatever batch had its id, and a client
    session's runs all used one id; 09:49-09:50 a gate client's later runs replaced its earlier batches (094910-vxp1o7:
    1 item, then 1,406), so a batch with items queued or running would lose them: their rows and output nowhere to go.
    An identical submit (the client's retry after an answer it didn't get) keeps the batch it already made."""

    def __init__(self):
        self.exists = False  # a batch has the id
        self.live = False  # that batch has items queued or running
        self.result: Literal["new", "keep", "refuse", "replace"] = "new"

    # gdp: law a submit never replaces a batch with live items: not old(live) or result != "replace"
    # gdp: law a submit under a free id makes the batch: old(exists) or result == "new"
    def admit(self, same: bool) -> Literal["new", "keep", "refuse", "replace"]:
        """A submit under the id; `same` when it names the same items as the batch already there."""
        if not self.exists:
            self.result = "new"
        elif self.live and same:
            self.result = "keep"
        elif self.live:
            self.result = "refuse"
        else:
            self.result = "replace"  # (every item of the one there done: nothing of it is lost)
        if self.result == "new" or self.result == "replace":
            self.exists, self.live = True, True
        return self.result

    def ended(self) -> None:
        """Every item of the batch done (or the batch closed)."""
        self.live = False


class Delivery:
    """A batch's rows to its client (Scheduler.poll): an item done delivers its row (after its output is kept, which
    takes an RPC: a very fast item is done well before its row is there), and the batch is done to the client only once
    every item's row is in and the client has read them all. (Before: done as soon as no item was left, its row maybe not
    yet delivered: the client stopped with rows unread, 47 successful rows of 36 batches on 10-31.)"""

    # gdp: law rows are delivered only for items done: rows <= finished
    # gdp: law no more items are done than the batch has: finished <= items

    def __init__(self, items: int):
        if items < 0:
            raise ValueError("a count is at least 0")
        self.items = items
        self.finished = 0
        self.rows = 0
        self.read = 0  # the rows the client holds

    def finish(self) -> None:
        if self.finished < self.items:
            self.finished += 1

    def deliver(self) -> None:
        if self.rows < self.finished:
            self.rows += 1

    # gdp: law every item's row reaches its client before the batch is done: not result or read == items
    # gdp: law a client's poll never skips a row: since > old(rows) or since < 0 or read == rows
    def poll(self, since: int) -> bool:
        """A client's poll from its `since`: it is given rows[since:] (then holds every row); done for it only then and
        with every item's row in."""
        if since < 0 or since > self.rows:
            return False
        self.read = self.rows
        return self.read == self.items



# gdp: law the plan waits for re-attach's first pass at most 60 s: not result or waited_s < 60
# gdp: law after a handoff the plan waits at most one tick: not result or not handoff or waited_s < tick_s
def plan_waits(reattaching: bool, waited_s: float, handoff: bool = False, tick_s: float = 15.0) -> bool:
    """No VM leased or started while re-attach's first pass takes back what runs where (a VM taken twice), but at most
    60 s of it: one VM whose agent answers neither boat's front nor boat exec took the 00:09 handoff's first pass past
    4 minutes, 1,428 gates queued and the plan empty the while. After a handoff the pool is in hand at once
    (take_over): one tick at most."""
    if tick_s <= 0 or tick_s > 60:
        raise ValueError("a tick of 0 to 60 s")
    if handoff:
        return reattaching and waited_s < tick_s
    return reattaching and waited_s < 60


# gdp: law a VM another live holder has is never taken: not result or not holder_alive or holder_is_old
# gdp: law every VM the previous scheduler of this machine held is taken at once: result or not scheduler_lease or not same_host or own or (holder_alive and not holder_is_old)
def take_over(scheduler_lease: bool, same_host: bool, own: bool, holder_alive: bool, holder_is_old: bool) -> bool:
    """A starting scheduler (a handoff, a restart, a kill -9) takes every VM the previous one held, all in one write of
    the registry, before it serves: its lease the scheduler's, of this machine, its holder gone or the one handing over
    (alive, waiting for the new one to hold them). Before: re-attach took them one by one, 16 at a time, each after its
    agent answered (14:33:45-14:36:54: 43 of 225 held at 14:33, 176 up with no live owner, 1,247 real items queued)."""
    if own:
        return False
    return scheduler_lease and same_host and (holder_is_old or not holder_alive)


# gdp: law an item the old scheduler was starting is started by the new one: old_state != "starting" or result == "starting"
# gdp: law an item being killed for a higher class stays so (its preempted row comes to the new one): old_state != "preempting" or result == "preempting"
# gdp: law an item running stays running: old_state != "running" or result == "running"
def taken_item_state(old_state: Literal["running", "preempting", "starting"]) -> Literal["running", "preempting", "starting"]:
    """An item in the handoff's snapshot, as the new scheduler holds it: as the old one did, a start in flight sent
    again by the new one (the same run: an agent ignores a start it has). Before: held as running, never started,
    queued again only after its agent's word (20 s of its slot idle, scenario handoff_1430)."""
    return old_state


# gdp: law the switch never waits for the work in flight a tick or more: result < tick_s
# gdp: law the switch's wait is never negative: result >= 0
def switch_wait(in_flight: int, tick_s: float) -> float:
    """How long the old scheduler holds the switch for its work in flight before its snapshot: not at all (it keeps no
    row and acks none from then; the new one takes its items as they are). Before: until its pollers' ticks ended,
    up to 25 s with every slot unplaced (15:21:34-15:21:59)."""
    if tick_s <= 0:
        raise ValueError("a tick is longer than 0 s")
    return 0.0


# gdp: law the old scheduler goes only when the new one holds every VM it held but those that died: result != "exit" or (acked and taken + died >= held)
# gdp: law a handoff the new scheduler took whole completes: result == "exit" or not acked or taken + died < held
def handoff_done(held: int, taken: int, died: int, acked: bool) -> Literal["exit", "keep"]:
    """The old scheduler, after the new one answers (its ack: the VMs it took): it exits only when the new one holds
    every VM it held (less those boat reports gone); else it keeps them and the handoff fails (the new code rejected,
    the pool handed to the old code's kept copy). Before: it exited at once, its VMs left to the next one's re-attach."""
    if acked and taken + died >= held:
        return "exit"
    return "keep"



# gdp: law a row lost because the VM dropped the batch's tree is never a strike: result == "reprepare" or not tree_gone
# gdp: law a row lost for an item being preempted is its preemption, never a strike: result == "preempted" or not preempting or tree_gone
# gdp: law any other lost row is a strike: result == "strike" or tree_gone or preempting
def lost_row(tree_gone: bool, preempting: bool = False) -> Literal["strike", "reprepare", "preempted"]:
    """A row the agent says lost: its VM dropped the batch's tree (our doing: its preparation there gone; prepared
    again, the item queued with no strike); an item the scheduler is killing for a higher class (kill_item's
    preempted row raced by report's "its process gone without a word": the kill, queued again with no strike); any
    other loss (its process gone, never started) a strike of boat's."""
    if tree_gone:
        return "reprepare"
    if preempting:
        return "preempted"
    return "strike"


# gdp: law an item that no held VM of its image may take, with nothing coming, gets a start: result >= 1 or not stuck or coming > 0
# gdp: law a start for a stuck item only when something is stuck: result == want or stuck
def stuck_wants(want: int, stuck: bool, coming: int) -> int:
    """The plan's starts for an image when a queued item may run on none of its held VMs (each failed it: its avoid
    set): the pool's slot-time counts those VMs, but the item can't use them; with nothing coming, one VM (B3:
    the item waited for ever on a pool of one VM that had failed it)."""
    if stuck and coming <= 0:
        return max(want, 1)
    return want


# gdp: law a timing item's VM has the vCPUs it needs: result >= need or need > 16
# gdp: law a timing item alone on its VM leaves none of it idle when a VM type has exactly its vCPUs: result == need or (need != 2 and need != 4 and need != 8 and need != 16)
def timing_vcpus(need: int) -> int:
    """The VM a timing item (alone on its VM) gets: the smallest type with the vCPUs it needs (2, 4, 8, 16), never the
    batch's type by default (06:35: 123 timing items of 4 vCPUs each alone on an 8-vCPU VM, 490 vCPUs held idle)."""
    if need <= 2:
        return 2
    if need <= 4:
        return 4
    if need <= 8:
        return 8
    return 16
