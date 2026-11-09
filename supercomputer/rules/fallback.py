"""Where work runs: boat first, RunPod for GPUs, Modal when they can't; the decisions of fanout.py and runpod.py.

    job_route      a CPU job of a jobs file: boat, Modal, or failed (run_jobs)
    preflight_on   whether a batch asks boat's preflight first
    needs_modal    whether `supercomputer run` loads the Modal client
    row_outcome    a call's row: its result, its own failure, or boat losing it
    AutoFallback   AutoEx: boat for each call until boat failed 3 times, then Modal for the rest
    GpuTries       gpu_jobs and gpu_call: at most two pods, none after "no capacity", then Modal (or failed)
    PodCreation    Pod.__enter__: pods made that never came up, at most MAX_TRIES, within MAX_WAIT
    job_outcome    Pod.job: a job's exit, a timeout, or the pod lost
    runpod_ready   runpod.preflight: its API and its balance
    pod_reap       runpod.reap: a pod kept, deleted or not ours
    ModalRef       ModalCtx: an app started here stopped when its last holder exits, never one started elsewhere
    reexec_modal   fanout.reexec_with_modal: re-run under the Modal CLI's Python, once
"""
from typing import Literal

Backend = Literal["modal", "auto", "boat", "runpod"]
Where = Literal["boat", "modal", "failed"]
Outcome = Literal["ok", "own", "lost"]


# gdp: law --backend boat never sends a job to Modal but Modal's own: result != "modal" or backend != "boat" or own
# gdp: law --backend modal never runs a job on boat: result != "boat" or backend == "auto" or backend == "boat"
# gdp: law a job boat ran stays boat's: result == "boat" or not boat_ran or own or (backend != "auto" and backend != "boat")
# gdp: law a job boat didn't run is failed only under --backend boat: result != "failed" or backend == "boat"
def job_route(backend: Backend, own: bool, boat_refused: bool, boat_ran: bool) -> Where:
    """A CPU job of a jobs file (run_jobs): Modal's own (its image boat lacks, or "backend": "modal") on Modal; under
    auto and boat, on boat unless the preflight refused the batch (auto); one boat didn't finish failed under boat, on
    Modal under auto; under modal and runpod (its CPU jobs) on Modal."""
    if own:
        return "modal"
    if backend == "auto" or backend == "boat":
        if boat_ran:
            return "boat"
        if backend == "boat":
            return "failed"
        return "modal"
    return "modal"


# gdp: law only auto asks boat's preflight, and not where the controller's scheduler takes the batch: not result or (backend == "auto" and not (scheduler and controller))
def preflight_on(backend: Backend, scheduler: bool, controller: bool) -> bool:
    return backend == "auto" and not (scheduler and controller)


# gdp: law --backend boat with no job of Modal's never loads the Modal client: not result or backend != "boat" or modal_job
def needs_modal(backend: Backend, modal_job: bool) -> bool:
    """`supercomputer run`: the Modal app loaded where any job may go to Modal."""
    return backend != "boat" or modal_job


# gdp: law a row with a result and exit 0 is the call's result: result == "ok" or not (has_result and code == 0)
# gdp: law a row boat lost (no seconds, or marked lost) is boat's, never the call's own failure: result != "own" or (ran and not lost)
def row_outcome(has_result: bool, code: int, ran: bool, lost: bool) -> Outcome:
    """A call's row: its result; its own failure (it ran and exited non-zero); or boat losing it (no VM ran it to the
    end: no seconds, or the scheduler's lost row)."""
    if has_result and code == 0:
        return "ok"
    if ran and not lost:
        return "own"
    return "lost"


class AutoFallback:
    """AutoEx: each call on boat; a call boat loses runs again on boat, until boat has failed this batch's calls 3
    times; then Modal takes the rest of the batch. A call's own failure is its result, never boat's."""

    # gdp: law Modal takes the batch only after 3 boat failures: not on_modal or boat_fails >= tries
    # gdp: law a call goes to Modal only once boat failed the batch 3 times: result != "modal" or boat_fails >= tries

    def __init__(self, tries: int):
        if tries < 1:
            raise ValueError("tries is at least 1")
        self.tries = tries
        self.boat_fails = 0
        self.on_modal = False

    # gdp: law a call's own failure is never a boat failure: outcome != "own" or boat_fails == old(boat_fails)
    # gdp: law a call's own failure is its result, never retried nor moved: outcome != "own" or result == "raise" or old(on_modal)
    def outcome(self, outcome: Outcome) -> Literal["done", "raise", "again", "modal"]:
        """A call's outcome on boat: done, raised (its own failure), again on boat, or on Modal from now on."""
        if self.on_modal:
            return "modal"
        if outcome == "ok":
            return "done"
        if outcome == "own":
            return "raise"
        self.boat_fails += 1
        if self.boat_fails < self.tries:
            return "again"
        self.on_modal = True
        return "modal"

    # gdp: law with boat refused, every call is Modal's: on_modal and boat_fails >= tries
    def refused(self) -> None:
        """The preflight refused the batch (or boat had no session for it): Modal from the first call."""
        self.on_modal = True
        self.boat_fails = self.tries


class GpuTries:
    """gpu_jobs and gpu_call: at most two pods; "no capacity" stops at once; another failure gets one more pod."""

    # gdp: law at most two pods a batch: pods <= 2

    def __init__(self):
        self.pods = 0
        self.stopped = False
        self.done = False

    # gdp: law no pod after RunPod said it has no capacity, nor after the jobs are done: not result or (not old(stopped) and not old(done))
    def next_pod(self) -> bool:
        """Whether to make a pod now."""
        if self.stopped or self.done or self.pods >= 2:
            return False
        self.pods += 1
        return True

    def ended(self, how: Literal["ok", "no_capacity", "failed"]) -> None:
        """How the pod ended: its jobs done, no GPU of the list to be had, or the pod failing."""
        if how == "ok":
            self.done = True
        elif how == "no_capacity":
            self.stopped = True


# gdp: law --backend runpod never sends a GPU job to Modal: result != "modal" or backend != "runpod"
# gdp: law a GPU job is failed only under --backend runpod: result != "failed" or backend == "runpod"
def gpu_leftover(backend: Backend) -> Where:
    """A GPU job no pod ran: failed under --backend runpod, else on Modal."""
    if backend == "runpod":
        return "failed"
    return "modal"


class PodCreation:
    """Pod.__enter__: through the clouds and GPU types, a pod made each time; one that never came up (deleted) counts,
    an out-of-stock type doesn't; no more after max_tries of them or past the deadline."""

    # gdp: law at most max_tries pods that never came up: made <= max_tries

    def __init__(self, max_tries: int, deadline: float):
        if max_tries < 0:
            raise ValueError("max_tries is at least 0")
        self.max_tries = max_tries
        self.deadline = deadline  # (its start plus MAX_WAIT)
        self.made = 0

    # gdp: law no pod is tried past the deadline: not result or now <= deadline
    def another(self, now: float) -> bool:
        return self.made < self.max_tries and now <= self.deadline

    def never_up(self) -> None:
        """A pod made that never answered: deleted, and counted."""
        if self.made < self.max_tries:
            self.made += 1


# gdp: law ssh failing with no output on a lost connection is the pod's, not the job's: result == "pod_lost" or not (code == 255 and not has_out and conn_lost)
# gdp: law a job killed at its limit is a timeout: result == "timeout" or code != 137 or secs < limit - 1 or (code == 255 and not has_out and conn_lost)
def job_outcome(code: int, has_out: bool, conn_lost: bool, secs: float, limit: int) -> Literal["pod_lost", "timeout", "exit"]:
    """A GPU job's end on the pod (Pod.job): the pod lost, a timeout (124), or its own exit."""
    if code == 255 and not has_out and conn_lost:
        return "pod_lost"
    if code == 137 and secs >= limit - 1:
        return "timeout"
    return "exit"


# gdp: law RunPod takes a batch only with its API up and its balance at the floor: not result or (api_ok and balance >= floor)
def runpod_ready(api_ok: bool, balance: float, floor: float) -> bool:
    return api_ok and balance >= floor


# gdp: law a pod whose client lives is never deleted: result != "delete" or not (has_ticket and client_alive)
# gdp: law a pod not named as ours is never touched: result == "skip" or ours
def pod_reap(ours: bool, has_ticket: bool, client_alive: bool) -> Literal["skip", "keep", "delete"]:
    if not ours:
        return "skip"
    if has_ticket and client_alive:
        return "keep"
    return "delete"


class ModalRef:
    """ModalCtx: an app this process started (app.run) is shared by the contexts that hold it and stopped when the last
    exits; one `modal run` started is used as it is and never stopped here."""

    # gdp: law an app started elsewhere is never stopped here: not stopped or ours_ever
    # gdp: law an app held here is running: holders == 0 or running
    # gdp: law an app of ours was started here: not ours or ours_ever
    # gdp: law an app of ours runs: not ours or running
    # gdp: law an app started elsewhere keeps running: running or not elsewhere
    # gdp: law an app started elsewhere is never ours: not (elsewhere and ours)

    def __init__(self, elsewhere: bool):
        self.elsewhere = elsewhere  # (started by `modal run`, running already)
        self.running = elsewhere
        self.holders = 0
        self.ours = False
        self.ours_ever = False
        self.stopped = False

    def enter(self) -> None:
        if self.ours and self.holders > 0:
            self.holders += 1
            return
        if not self.running:
            self.running = True
            self.ours = True
            self.ours_ever = True
            self.stopped = False
        self.holders += 1

    # gdp: law an app started here is stopped only when its last holder exits: not stopped or old(stopped) or old(holders) == 1
    def exit(self) -> None:
        if self.holders <= 0:
            return
        self.holders -= 1
        if self.holders == 0 and self.ours:
            self.ours = False
            self.running = False
            self.stopped = True


# gdp: law it re-runs under the Modal CLI's Python at most once: not result or not reexeced
def reexec_modal(modal_real: bool, reexeced: bool, modal_python: bool, same_python: bool) -> bool:
    """fanout.reexec_with_modal: re-run under the Python the Modal CLI runs on, where this one lacks the client."""
    return not modal_real and not reexeced and modal_python and not same_python


# gdp: law no client path reaches Modal but through boat_tries boat failures on distinct VMs (or a job Modal owns): result != "modal" or own or failures >= tries
# gdp: law work boat failed fewer times goes back to boat, whatever kept it off (a tree, an image, a scheduler hiccup): result == "boat" or own or failures >= tries
def lost_route(own: bool, failures: int, tries: int, modal_on: bool) -> Literal["modal", "boat", "failed"]:
    """Where a client sends work boat didn't return (the treadmill's Batch, run, AutoEx): `failures` the scheduler's
    count of real boat failures of it, on distinct VMs (its lost row's boat_fails; 0 for anything else: Unavailable,
    a tree, an image, a scheduler that didn't answer). Modal only at `tries` of them, or a job Modal owns (its image,
    "backend": "modal"); else boat again (11-01 05:03: a sweep's item on Modal for "boat couldn't run them", a tree
    missing at the treadmill's shutdown)."""
    if own:
        return "modal"
    if failures >= tries:
        return "modal" if modal_on else "failed"
    return "boat"
