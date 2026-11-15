"""Boat CPU retries and RunPod GPU outcomes; no execution route reaches Modal.

Legacy ModalRef models remain for cleanup compatibility, not new dispatch.
"""
from typing import Literal

Backend = Literal["modal", "auto", "boat", "runpod"]
Where = Literal["boat", "failed"]
Outcome = Literal["ok", "own", "lost"]


# gdp: law no CPU route selects Modal: result == "boat" or result == "failed"
# gdp: law a returned boat job stays on boat: result != "boat" or (boat_ran and not own and (backend == "auto" or backend == "boat"))
# gdp: law an unavailable CPU job fails closed: result != "failed" or not boat_ran or own or (backend != "auto" and backend != "boat")
def job_route(backend: Backend, own: bool, boat_refused: bool, boat_ran: bool) -> Where:
    """Unknown/legacy images and unsupported backends fail; successful CPU work stays on boat."""
    if not own and (backend == "auto" or backend == "boat") and boat_ran:
        return "boat"
    return "failed"


# gdp: law only auto asks boat's preflight, and not where the controller's scheduler takes the batch: not result or (backend == "auto" and not (scheduler and controller))
def preflight_on(backend: Backend, scheduler: bool, controller: bool) -> bool:
    return backend == "auto" and not (scheduler and controller)


# gdp: law removed Modal execution never loads its client: result == False
def needs_modal(backend: Backend, modal_job: bool) -> bool:
    """Compatibility hook for older callers; Modal is never an execution backend."""
    return False


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
    """Bounded boat-only call retries. Own failures are raised, never retried."""

    # gdp: law Modal is never enabled: on_modal == False
    # gdp: law boat failures stay within the retry budget: boat_fails <= tries
    # gdp: law exhausted calls stop: not stopped or boat_fails >= tries
    # gdp: law an available retry has budget left: stopped or boat_fails < tries

    def __init__(self, tries: int):
        if tries < 1:
            raise ValueError("tries is at least 1")
        self.tries = tries
        self.boat_fails = 0
        self.on_modal = False  # legacy serialized field; never enabled
        self.stopped = False

    # gdp: law a call's own failure is never a boat failure: outcome != "own" or boat_fails == old(boat_fails)
    # gdp: law a call's own failure is raised: outcome != "own" or result == "raise"
    # gdp: law retries stay on boat: result != "again" or boat_fails < tries
    def outcome(self, outcome: Outcome) -> Literal["done", "raise", "again"]:
        if self.on_modal:  # legacy state must never enable execution
            self.on_modal = False
        if self.stopped:
            return "raise"
        if outcome == "ok":
            return "done"
        if outcome == "own":
            return "raise"
        self.boat_fails += 1
        if self.boat_fails < self.tries:
            return "again"
        self.stopped = True
        return "raise"

    # gdp: law refused work stops without enabling Modal: stopped and on_modal == False
    def refused(self) -> None:
        self.boat_fails = self.tries
        self.stopped = True
        self.on_modal = False


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


# gdp: law an unserved GPU job fails instead of changing provider: result == "failed"
def gpu_leftover(backend: Backend) -> Where:
    """RunPod is the only GPU backend, including when it cannot serve a job."""
    return "failed"


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


# gdp: law removed Modal execution never reexecs its interpreter: result == False
def reexec_modal(modal_real: bool, reexeced: bool, modal_python: bool, same_python: bool) -> bool:
    """Legacy compatibility hook: keep the current interpreter."""
    return False


# gdp: law exhausted work never selects Modal: result == "boat" or result == "failed"
# gdp: law work below its boat failure budget stays on boat: result == "boat" or own or failures >= tries
# gdp: law exhausted or unsupported work is terminal: result != "boat" or (not own and failures < tries)
def lost_route(own: bool, failures: int, tries: int, modal_on: bool) -> Literal["boat", "failed"]:
    """Preserve the boat retry budget, but never route exhausted work to another provider.

    own/modal_on are retained for older callers; legacy Modal-only work fails closed.
    """
    if own or failures >= tries:
        return "failed"
    return "boat"
