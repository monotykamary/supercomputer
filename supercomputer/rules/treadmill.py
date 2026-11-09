"""The treadmill's decisions (treadmill.py), apart from its threads, sessions and files.

    StepPlan        a step's VMs: the free ones (the pool less what its batches hold), the queue's first pass (at most
                    subs_vms while any wait), the rest (4 at a time), backlogs grown (a quarter of the pool at a time)
    elastic         the pool the treadmill launches work for: its floor, up to max_vms, as big as the scheduler's pool
    BatchFate       a batch's end: done, again later (boat couldn't run it), failed, or its few lost items on Modal
    PendingSlots    the slots of a batch's items not yet back (what it holds of the scheduler's pool)
    backlog         whether a batch would use more VMs (grown by the treadmill: only its own sessions)
    ResultCache     every verdict by (commit, bench, task, config): seen once, never run twice; a task's first solve
    cacheable       a submission's job row kept as its result (never run again for this tree and command)
    SubmissionFate  a submission: queued, running under this treadmill's batch, done; a failed batch queues it again
    submit_refusal  what the queue refuses: jobs on Modal's own image or a GPU, a name taken
    chain_class     a chain's class at the scheduler
    closes_leftover a batch at the scheduler the treadmill closes when it starts (a previous treadmill's)
"""
from typing import Literal

SubStatus = Literal["queued", "running", "done"]
Route = Literal["done", "again", "failed", "modal"]
MODAL_MAX = 64


class StepPlan:
    """One step of the treadmill (Treadmill.step): the VMs free are the pool's less what its batches hold; the queue's
    submissions get up to subs_vms at once while any wait, then (after the repo's chains) up to 4 each of what's left;
    batches with a backlog grow by a quarter of the pool at a time; the repo's hooks (floor, main chains, fill) take
    what they launch and say what's left."""

    # gdp: law the treadmill launches work for no more VMs than its pool has free: launched == 0 or launched <= vms - held
    # gdp: law what is launched and what is free never pass the pool: launched + free <= vms - held
    # gdp: law once anything is launched, nothing free is below zero: launched == 0 or free >= 0
    # gdp: law nothing is launched below zero: launched >= 0

    def __init__(self, vms: int, held: int, subs_vms: int, subs_held: int):
        self.vms = vms
        self.held = held
        self.subs_vms = subs_vms
        self.subs_held = subs_held
        self.free = vms - held
        self.launched = 0

    # gdp: law the queue's first pass holds at most subs_vms VMs at once: result == 0 or subs_held <= subs_vms
    def sub_first(self) -> int:
        """VMs for the next queued submission in the first pass (0: no more of it)."""
        k = min(self.free, self.subs_vms - self.subs_held)
        if k <= 0:
            return 0
        self.free -= k
        self.launched += k
        self.subs_held += k
        return k

    # gdp: law the queue's later pass takes at most 4 VMs a submission: result <= 4
    def sub_rest(self) -> int:
        """VMs for a queued submission after the repo's chains (0: none left)."""
        if self.free <= 0:
            return 0
        k = min(self.free, 4)
        self.free -= k
        self.launched += k
        return k

    # gdp: law a backlog grows by a quarter of the pool at most (one VM at least): result <= 1 or result * 4 <= vms
    def grow(self) -> int:
        """VMs a backlogged batch grows by (0: none left)."""
        if self.free <= 0:
            return 0
        k = min(self.free, max(1, self.vms // 4))
        self.free -= k
        self.launched += k
        return k

    def unused(self, k: int, first: bool) -> None:
        """A submission given k VMs that started no batch (all its jobs cached): the k back."""
        if k <= 0 or k > self.launched:
            return
        self.free += k
        self.launched -= k
        if first and self.subs_held >= k:
            self.subs_held -= k

    # gdp: law the repo's hooks never give the step more VMs than it had: free <= old(free)
    def hook(self, left: int) -> None:
        """What a repo's hook (floor, main chains, fill) says is left after what it launched."""
        if left < self.free:
            self.free = max(left, 0) if self.launched > 0 else left


# gdp: law the treadmill's pool is at least its floor: result >= floor
# gdp: law the pool above the floor is at most max_vms: result <= floor or result <= max_vms
def elastic(floor: int, max_vms: int, pool_now: int) -> int:
    """The VMs the treadmill launches work for (Treadmill.elastic): its floor, up to max_vms, as big as the scheduler's
    pool of its image now (so its filler keeps each one busy)."""
    return max(floor, min(max_vms, pool_now))


class BatchFate:
    """A batch's end (Batch.go): items boat didn't return go to Modal only when they are few (MODAL_MAX) and boat ran
    the batch (it lost them); a batch boat refused whole, or too many lost, runs again later; with Modal off they are
    recorded failed; Modal failing too fails them. A failed batch records nothing (its `after` doesn't run)."""

    # gdp: law a batch boat refused whole never goes to Modal: route != "modal" or not self.boat_err
    # gdp: law items go to Modal only when few: route != "modal" or self.todo <= MODAL_MAX
    # gdp: law a failed batch records nothing: not recorded or not failed
    # gdp: law before boat's end nothing is decided: ended or (route == "done" and not failed and not recorded and self.todo == 0 and not self.boat_err)

    def __init__(self):
        self.route: Route = "done"
        self.todo = 0
        self.boat_err = False
        self.failed = False
        self.recorded = False
        self.ended = False

    # gdp: law with Modal off, nothing goes to Modal: old(ended) or result != "modal" or modal
    def boat_ended(self, todo: int, boat_err: bool, modal: bool) -> Route:
        """Boat's part over: `todo` items not returned, `boat_err` boat refusing or failing the batch whole (once)."""
        if self.ended:
            return self.route
        self.ended = True
        self.todo = todo
        self.boat_err = boat_err
        if todo <= 0:
            self.route = "done"
        elif todo > MODAL_MAX or boat_err:
            self.route = "again"
            self.failed = True
        elif not modal:
            self.route = "failed"
            self.failed = True
        else:
            self.route = "modal"
        return self.route

    def modal_failed(self) -> None:
        if self.route == "modal" and not self.recorded:
            self.failed = True

    # gdp: law a batch records its results only when it didn't fail: not result or not failed
    def record(self) -> bool:
        """Whether its `after` runs (its results recorded)."""
        if self.failed or not self.ended:
            return False
        self.recorded = True
        return True


class PendingSlots:
    """The slots of a batch's items not yet back (Batch.held through the scheduler): each item counted until its own
    row comes back, whatever order the rows come in."""

    # gdp: law the count is the slots of the items not yet back: need == sum(pending)
    # gdp: law no item counts below zero: forall k in pending: pending[k] >= 0

    def __init__(self):
        self.pending: dict[str, int] = {}
        self.need = 0

    def add(self, name: str, slots: int) -> None:
        if slots < 0 or name in self.pending:
            return
        self.pending[name] = slots
        self.need += slots

    # gdp: law a row back takes off its own item's slots: not (name in old(pending)) or need == old(need) - old(pending[name])
    def back(self, name: str) -> None:
        if name not in self.pending:
            return
        self.need -= self.pending.pop(name)


# gdp: law a batch through the scheduler never grows itself (the scheduler sizes the pool): not result or not scheduled
def backlog(scheduled: bool, done: bool, ready: bool, enough: bool) -> bool:
    """More queued than its VMs' slots (Batch.backlog): more VMs would help it."""
    return not scheduled and not done and ready and not enough


class ResultCache:
    """Every verdict by its key (commit, bench, task, config): a key seen is never run again; a task's first solve
    (by any config) is reported once; a task some config refused is known so."""

    # gdp: law a key once seen stays seen: forall k in old(seen): k in seen

    def __init__(self):
        self.seen: set[str] = set()
        self.solved: set[str] = set()  # (commit, bench, task) solved by some config
        self.refused: set[str] = set()  # (commit, bench, task) some config refused or found unsupported

    # gdp: law a verdict put is seen: key in seen
    # gdp: law a task's first solve is reported once: not result or task not in old(self.solved)
    # gdp: law a task once solved stays solved: not (task in old(self.solved)) or task in self.solved
    def put(self, key: str, task: str, solved: bool, refused: bool) -> bool:
        """A verdict; True when it is the task's first solve."""
        self.seen.add(key)
        if refused:
            self.refused.add(task)
        if solved and task not in self.solved:
            self.solved.add(task)
            return True
        return False

    # gdp: law a key seen is never run again: not result or key not in seen
    def run(self, key: str) -> bool:
        """Whether a (commit, bench, task, config) is to run."""
        return key not in self.seen


# gdp: law a job that failed by boat's or Modal's error is never cached (it runs again): not result or code == 0 or not has_error
# gdp: law a job's own exit, with its log, is cached as its verdict: result or not has_log or (code != 0 and has_error)
def cacheable(has_log: bool, code: int, has_error: bool) -> bool:
    """A submission's job row kept (Submission.after): it has its log, and it exited 0 or exited by itself (no error
    of boat's or Modal's): never run again for this tree and command."""
    return has_log and (code == 0 or not has_error)


class SubmissionFate:
    """A submission of the queue (Submission, Treadmill.step): queued; running under a batch of this treadmill; done
    (every job's row in, or all cached). A batch that failed queues it again. One a treadmill found "running" on disk
    is a dead treadmill's: queued again."""

    # gdp: law a submission runs only under this treadmill's batch: status != "running" or owned
    # gdp: law a done submission stays done: old(status) != "done" or status == "done"
    # gdp: law a running submission ends or is queued again: status == "running" leads to status != "running" via batch_ended(failed = true)

    def __init__(self, status: SubStatus):
        self.status: SubStatus = "queued" if status == "running" else status
        self.owned = False

    # gdp: law a submission starts only from the queue: not result or old(status) == "queued"
    def start(self, all_cached: bool) -> bool:
        """Whether a batch is made for it (all its jobs cached: done at once)."""
        if self.status != "queued":
            return False
        if all_cached:
            self.status = "done"
            return False
        self.status = "running"
        self.owned = True
        return True

    def batch_ended(self, failed: bool) -> None:
        """Its batch done; failed (boat couldn't run it now): queued again."""
        if self.status == "running" and self.owned and failed:
            self.status = "queued"
            self.owned = False

    def finished(self) -> None:
        """Its rows recorded (Submission.after)."""
        if self.status == "running":
            self.status = "done"
            self.owned = False


# gdp: law a job on Modal's own image or a GPU never enters the queue: result != "ok" or not (other_image or modal_backend or gpu)
# gdp: law a name taken is never overwritten: result != "ok" or not exists
def submit_refusal(other_image: bool, modal_backend: bool, gpu: bool, exists: bool) -> Literal["ok", "not_here", "exists"]:
    if other_image or modal_backend or gpu:
        return "not_here"
    if exists:
        return "exists"
    return "ok"


# gdp: law the queue's submissions are submissions at the scheduler: result == "submission" or chain != "c"
# gdp: law the treadmill's work is never above a submission: result != "interactive" and result != "gate"
def chain_class(chain: str) -> Literal["sweep", "filler", "submission", "interactive", "gate"]:
    if chain == "b":
        return "filler"
    if chain == "c":
        return "submission"
    return "sweep"


# gdp: law a treadmill closes only its own client's batches: not result or client == mine
def closes_leftover(client: str, mine: str) -> bool:
    return client == mine
