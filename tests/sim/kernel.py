"""The simulator's kernel: a virtual clock and deterministic threads.

Every thread of the code under test (the scheduler's loop, its workers, its sends; the clients; the fake boat's own
timers) is a real OS thread, but only one runs at a time: it holds the kernel's baton and gives it back only at a
blocking call of the shims below (sleep, an Event's or a Condition's wait, a contended Lock, a join). The kernel then
runs the next thread ready at the current virtual time, or advances the clock to the earliest wake-up. So a run is
the same run every time (a fixed seed, PYTHONHASHSEED fixed by the runner), hours of the pool pass in seconds, and a
lock held across a slow call shows up as virtual time spent waiting (status's latency is measured that way).

The modules under test get `time`, `threading` and `concurrent.futures` shims in place of the real ones (their module
globals are replaced; nothing global is patched). A thread of a killed process (a kill -9, a handoff's exit) is
unwound at its next blocking call by SimKilled, a BaseException: no `except Exception` of the code catches it.
"""
import collections
import heapq
import itertools
import threading as _rt
import time as _rtime
import traceback


class SimKilled(BaseException):
    """The thread's process is gone (kill -9, os._exit): unwound, no handler of the code runs for it."""


class Deadline(BaseException):
    """The run reached its virtual end: every thread unwound."""


class Proc:
    """A process of the simulated machines: the laptop's scheduler, a client, or the fake world (boat, VMs)."""

    def __init__(self, k: "Kernel", pid: int, name: str, laptop: bool):
        self.k, self.pid, self.name, self.laptop = k, pid, name, laptop
        self.alive = True
        self.threads: set = set()
        self.t_start = k.now
        self.env: dict = {}

    def __repr__(self):
        return f"<proc {self.pid} {self.name}{'' if self.alive else ' dead'}>"


class SimThread:
    _ids = itertools.count(1)

    def __init__(self, k: "Kernel", proc: Proc, target, args=(), kwargs=None, name=""):
        self.k, self.proc = k, proc
        self.target, self.args, self.kwargs = target, args, kwargs or {}
        self.id = next(SimThread._ids)
        self.name = name or getattr(target, "__name__", "thread")
        self.baton = _rt.Semaphore(0)
        self.done = False
        self.wait_id = 0  # (a wait's ticket: a wake-up of an older wait is stale)
        self.woken_by = None  # ("timeout" | "signal")
        self.joiners: list = []
        self.os = None
        self.daemon = True
        self.ident = self.id

    def __repr__(self):
        return f"<sim thread {self.id} {self.name} of {self.proc.name}>"


class Kernel:
    def __init__(self, t0: float, seed: int = 0):
        self.now = float(t0)
        self.t0 = float(t0)
        self.seq = itertools.count()
        self.heap: list = []  # (t, seq, thread, wait_id)
        self.ready: collections.deque = collections.deque()
        self.cur: SimThread | None = None
        self.back = _rt.Semaphore(0)  # (the kernel's baton: given back when the running thread blocks or ends)
        self.pids = itertools.count(41000)
        self.procs: dict[int, Proc] = {}
        self.errors: list[str] = []  # (exceptions that ended a thread of the code under test)
        self.switches = 0
        self.frozen_until = 0.0  # (the laptop asleep: its processes run nothing until then)
        self.ending = False
        self.world = self.spawn_proc("world", laptop=False)

    # -- processes and threads
    def spawn_proc(self, name: str, laptop: bool = True) -> Proc:
        p = Proc(self, next(self.pids), name, laptop)
        self.procs[p.pid] = p
        return p

    def current_proc(self) -> Proc:
        return self.cur.proc if self.cur else self.world

    def start(self, proc: Proc, target, args=(), kwargs=None, name="") -> SimThread:
        th = SimThread(self, proc, target, args, kwargs, name)
        proc.threads.add(th)
        if not proc.alive:
            th.done = True
            return th

        def body():
            th.baton.acquire()
            try:
                if not th.proc.alive or self.ending:
                    raise SimKilled()
                th.target(*th.args, **th.kwargs)
            except (SimKilled, Deadline):
                pass
            except SystemExit:
                pass
            except BaseException as e:  # (a thread of the code ended by an exception: recorded, as threading prints it)
                if not self.ending:
                    self.errors.append(f"{self.now - self.t0:.1f}s {th!r}: {e!r}\n{traceback.format_exc()[-1500:]}")
            finally:
                th.done = True
                th.proc.threads.discard(th)
                for j in th.joiners:
                    self._wake(j, "signal")
                th.joiners = []
                self.back.release()

        th.os = _rt.Thread(target=body, daemon=True, name=f"sim-{th.id}")
        th.os.start()
        self.ready.append(th)
        return th

    def kill_proc(self, proc: Proc):
        """kill -9: the process's threads unwound at once (each at its blocking call), nothing of theirs runs again."""
        if not proc.alive:
            return
        proc.alive = False
        for th in list(proc.threads):
            if th is not self.cur and not th.done:
                self._wake(th, "signal")
        if self.cur is not None and self.cur.proc is proc:
            raise SimKilled()

    # -- blocking
    def _block(self):
        """The running thread gives the baton back; it returns when the kernel runs it again."""
        th = self.cur
        if th is None:
            raise RuntimeError("a blocking call outside a sim thread")
        if self.ending:
            raise Deadline()
        if not th.proc.alive:
            raise SimKilled()
        self.back.release()
        th.baton.acquire()
        if not th.proc.alive:
            raise SimKilled()
        if self.ending:
            raise Deadline()

    def _wake(self, th: SimThread, why: str):
        if th.done:
            return
        th.wait_id += 1
        th.woken_by = why
        self.ready.append(th)

    def sleep(self, dt: float):
        th = self.cur
        if th is None:
            raise RuntimeError("sleep outside a sim thread")
        if not th.proc.alive:
            raise SimKilled()
        th.wait_id += 1
        heapq.heappush(self.heap, (self.now + max(0.0, float(dt)), next(self.seq), th, th.wait_id))
        self._block()

    def wait_on(self, waiters: list, timeout: float | None) -> bool:
        """Blocks on a list of waiters until _wake'd (True) or the timeout passes (False)."""
        th = self.cur
        if not th.proc.alive:
            raise SimKilled()
        th.wait_id += 1
        th.woken_by = None
        waiters.append(th)
        if timeout is not None:
            heapq.heappush(self.heap, (self.now + max(0.0, float(timeout)), next(self.seq), th, th.wait_id))
        try:
            self._block()
        finally:
            if th in waiters:
                waiters.remove(th)
        return th.woken_by == "signal"

    def yield_(self):
        self.sleep(0)

    # -- the loop (run: below)
    def shutdown(self):
        """Every thread unwound (the end of a run): each is woken and raises Deadline at its blocking call."""
        self.ending = True
        for p in self.procs.values():
            for th in list(p.threads):
                if not th.done and th is not self.cur:
                    th.wait_id += 1
                    self.cur = th
                    th.baton.release()
                    self.back.acquire()
                    self.cur = None


def _run(self, until: float, real_limit: float = 600.0):
    """Runs every thread until the virtual clock reaches `until` (absolute) or nothing is left to run."""
    t_real = _rtime.monotonic()
    while True:
        if self.ready:
            th = self.ready.popleft()
            if th.done:
                continue
            frozen = max(self.frozen_until, th.proc.env.get("frozen_until", 0.0))
            if th.proc.laptop and self.now < frozen and th.proc.alive:
                th.wait_id += 1
                heapq.heappush(self.heap, (frozen, next(self.seq), th, th.wait_id))
                continue
            self.cur = th
            self.switches += 1
            th.baton.release()
            self.back.acquire()
            self.cur = None
            if self.switches % 4096 == 0 and _rtime.monotonic() - t_real > real_limit:
                raise RuntimeError(f"the simulation took over {real_limit} s of real time (virtual {self.now - self.t0:.0f} s)")
            continue
        entry = None
        while self.heap:
            t, _, th, wid = heapq.heappop(self.heap)
            if th.done or wid != th.wait_id:
                continue
            entry = (t, th)
            break
        if entry is None:
            return
        t, th = entry
        if t > until:
            heapq.heappush(self.heap, (t, next(self.seq), th, th.wait_id))
            self.now = max(self.now, until)
            return
        self.now = max(self.now, t)
        th.woken_by = "timeout"
        th.wait_id += 1
        self.ready.append(th)


Kernel.run = _run


# ------------------------------------------------------------------------------------------- the shims: threading
class _ShimThreading:
    """The `threading` a module under test sees."""

    def __init__(self, k: Kernel):
        self.k = k
        sh = self

        class Thread:
            def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, daemon=None):
                self._target, self._args, self._kwargs, self.name = target, args, kwargs or {}, name or ""
                self.daemon = bool(daemon)
                self._th = None

            def start(self):
                self._th = sh.k.start(sh.k.current_proc(), self.run, name=self.name or getattr(self._target, "__name__", ""))

            def run(self):
                if self._target:
                    self._target(*self._args, **self._kwargs)

            def join(self, timeout=None):
                th = self._th
                if th is None or th.done:
                    return
                sh.k.wait_on(th.joiners, timeout)

            def is_alive(self):
                return self._th is not None and not self._th.done

        class Lock:
            def __init__(self):
                self._owner, self._waiters = None, []

            def acquire(self, blocking=True, timeout=-1):
                me = sh.k.cur
                while self._owner is not None:
                    if not blocking:
                        return False
                    if not sh.k.wait_on(self._waiters, None if timeout is None or timeout < 0 else timeout) and timeout is not None and timeout >= 0:
                        if self._owner is not None:
                            return False
                self._owner = me or True
                return True

            def release(self):
                if self._owner is None:
                    raise RuntimeError("release unlocked lock")
                self._owner = None
                if self._waiters:
                    sh.k._wake(self._waiters[0], "signal")

            def locked(self):
                return self._owner is not None

            __enter__ = lambda self: self.acquire()

            def __exit__(self, *a):
                self.release()

        class RLock:
            def __init__(self):
                self._owner, self._n, self._waiters = None, 0, []

            def acquire(self, blocking=True, timeout=-1):
                me = sh.k.cur
                if self._owner is me and me is not None:
                    self._n += 1
                    return True
                while self._owner is not None:
                    if not blocking:
                        return False
                    sh.k.wait_on(self._waiters, None if timeout is None or timeout < 0 else timeout)
                    if self._owner is not None and timeout is not None and timeout >= 0:
                        return False
                self._owner, self._n = me, 1
                return True

            def release(self):
                self._n -= 1
                if self._n <= 0:
                    self._owner, self._n = None, 0
                    if self._waiters:
                        sh.k._wake(self._waiters[0], "signal")

            def _release_all(self):
                n = self._n
                self._n = 1
                self.release()
                return n

            def _acquire_n(self, n):
                self.acquire()
                self._n = n

            __enter__ = lambda self: self.acquire()

            def __exit__(self, *a):
                self.release()

        class Event:
            def __init__(self):
                self._flag, self._waiters = False, []

            def is_set(self):
                return self._flag

            isSet = is_set

            def set(self):
                self._flag = True
                for th in list(self._waiters):
                    sh.k._wake(th, "signal")

            def clear(self):
                self._flag = False

            def wait(self, timeout=None):
                if self._flag:
                    return True
                if timeout is not None and timeout <= 0:
                    sh.k.yield_()
                    return self._flag
                sh.k.wait_on(self._waiters, timeout)
                return self._flag

        class Condition:
            def __init__(self, lock=None):
                self._lock = lock or RLock()
                self._waiters = []

            def __enter__(self):
                self._lock.acquire()
                return self

            def __exit__(self, *a):
                self._lock.release()

            def acquire(self, *a, **k):
                return self._lock.acquire(*a, **k)

            def release(self):
                self._lock.release()

            def wait(self, timeout=None):
                n = self._lock._release_all() if isinstance(self._lock, RLock) else (self._lock.release() or 1)
                try:
                    ok = sh.k.wait_on(self._waiters, timeout)
                finally:
                    if isinstance(self._lock, RLock):
                        self._lock._acquire_n(n)
                    else:
                        self._lock.acquire()
                return ok

            def wait_for(self, pred, timeout=None):
                end = None if timeout is None else sh.k.now + timeout
                while not pred():
                    left = None if end is None else end - sh.k.now
                    if left is not None and left <= 0:
                        return pred()
                    self.wait(left)
                return True

            def notify(self, n=1):
                for th in list(self._waiters[:n]):
                    sh.k._wake(th, "signal")

            def notify_all(self):
                for th in list(self._waiters):
                    sh.k._wake(th, "signal")

            notifyAll = notify_all

        class Semaphore:
            def __init__(self, value=1):
                self._v, self._waiters, self._max = value, [], value

            def acquire(self, blocking=True, timeout=None):
                while self._v <= 0:
                    if not blocking:
                        return False
                    if not sh.k.wait_on(self._waiters, timeout) and timeout is not None and self._v <= 0:
                        return False
                self._v -= 1
                return True

            def release(self, n=1):
                self._v += n
                for th in list(self._waiters[:n]):
                    sh.k._wake(th, "signal")

            __enter__ = lambda self: self.acquire()

            def __exit__(self, *a):
                self.release()

        class BoundedSemaphore(Semaphore):
            pass

        class _Local:
            pass

        self.Thread, self.Lock, self.RLock, self.Event, self.Condition = Thread, Lock, RLock, Event, Condition
        self.Semaphore, self.BoundedSemaphore, self.local = Semaphore, BoundedSemaphore, _Local
        self.TIMEOUT_MAX = _rt.TIMEOUT_MAX

    def active_count(self):
        return sum(len(p.threads) for p in self.k.procs.values() if p.alive)

    def get_ident(self):
        return self.k.cur.id if self.k.cur else 0

    def current_thread(self):
        return self.k.cur

    def main_thread(self):
        return None


class _ShimTime:
    """The `time` a module under test sees: the virtual clock."""

    def __init__(self, k: Kernel):
        self.k = k
        self.struct_time = _rtime.struct_time

    def time(self):
        return self.k.now

    def time_ns(self):
        return int(self.k.now * 1e9)

    def monotonic(self):
        return self.k.now

    def monotonic_ns(self):
        return int(self.k.now * 1e9)

    perf_counter = monotonic

    def sleep(self, s):
        self.k.sleep(s)

    def gmtime(self, t=None):
        return _rtime.gmtime(self.k.now if t is None else t)

    def localtime(self, t=None):
        return _rtime.localtime(self.k.now if t is None else t)

    def strftime(self, fmt, t=None):
        return _rtime.strftime(fmt, t if t is not None else _rtime.localtime(self.k.now))

    def __getattr__(self, name):
        return getattr(_rtime, name)


class _ShimFutures:
    """concurrent.futures, its ThreadPoolExecutor on sim threads (map in order, `n` at once)."""

    def __init__(self, k: Kernel, threading_shim):
        self.k, self.th = k, threading_shim
        outer = self

        class ThreadPoolExecutor:
            def __init__(self, max_workers=None, **kw):
                self.n = max_workers or 8

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def map(self, fn, it):
                items = list(it)
                out = [None] * len(items)
                sem = outer.th.Semaphore(self.n)
                ths = []
                for i, x in enumerate(items):
                    def one(i=i, x=x):
                        with sem:
                            out[i] = fn(x)
                    t = outer.th.Thread(target=one)
                    t.start()
                    ths.append(t)
                for t in ths:
                    t.join()
                return iter(out)

            def submit(self, fn, *a, **kw):
                box = {}
                ev = outer.th.Event()

                def one():
                    try:
                        box["r"] = fn(*a, **kw)
                    except Exception as e:
                        box["e"] = e
                    ev.set()
                outer.th.Thread(target=one).start()

                class F:
                    def result(self, timeout=None):
                        ev.wait(timeout)
                        if "e" in box:
                            raise box["e"]
                        return box.get("r")
                return F()

            def shutdown(self, wait=True):
                pass

        self.ThreadPoolExecutor = ThreadPoolExecutor
        self.futures = self


def shims(k: Kernel):
    th = _ShimThreading(k)
    return {"threading": th, "time": _ShimTime(k), "concurrent": _ShimFutures(k, th)}
