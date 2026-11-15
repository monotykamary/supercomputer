"""One interface over boat.dev CPU workers and RunPod GPU workers.

Auto selects boat for CPU work and RunPod for GPU work. Provider failures retain
bounded retries on the same provider, then fail with their receipts. Modal is
never an execution route; legacy entry points refuse it explicitly.
"""
import contextlib
import importlib.util
import inspect
import json
import math
import os
import pathlib
import queue
import shutil
import sys
import threading
import time

from . import boat, config
from .rules import fallback
from .rules.agent_capabilities import required_agent_capabilities
from .config import project

HERE = pathlib.Path(__file__).resolve().parent
BACKENDS = ["auto", "boat", "runpod", "modal"]  # modal is parsed only to give an explicit removal error


def log(*a):
    print("[backend]", *a, file=sys.stderr, flush=True)


def fname(fn) -> str:
    for get in (lambda: fn.__name__, lambda: fn._tag_, lambda: fn.get_raw_f().__name__, lambda: fn.info().function_name):
        try:
            n = get()
            if n:
                return n.split(".")[-1]
        except Exception:
            pass
    raise ValueError(f"can't name {fn!r}")


# ------------------------------------------------------------------------------------------------------------- Modal
def modal_real() -> bool:
    m = sys.modules.get("modal")
    return m is not None and not getattr(m, "__gdpw_stub__", False) and not getattr(m, "__sc_stub__", False)


def reexec_with_modal():
    """Compatibility no-op: never switch to a removed provider's interpreter."""
    return None


class ModalCtx:
    """Legacy import compatibility only; new Modal execution is refused."""

    def __init__(self, app):
        raise ValueError(config.MODAL_REMOVED)


# ----------------------------------------------------------------------------------------------------- a jobs file
def run_jobs(app, spec: list[dict], out: str, pre: str, cmd, modal_jobs=None, backend: str = "auto", vms: int = 0, idle: int | None = None, teardown: bool = False,
             durations: str = "", vm_type: str = "large", max_vms: int = 0, max_wait: int = 120, tar: bytes | None = None, gpu_modal=None, strict_once: bool = False, max_attempts: int = 1):
    """Run CPU jobs only on boat and GPU jobs only on RunPod.

    Legacy Modal callback arguments are accepted for API compatibility but are
    never called. Failed providers retain their terminal rows and receipts.
    """
    config.reject_removed_backend(backend, spec)
    if backend not in ("auto", "boat", "runpod"):
        raise ValueError(f"--backend is auto, boat or runpod, not {backend}")
    boat._validate_strict_options(strict_once, max_attempts)
    if strict_once:
        _strict_native(spec, backend)
    images = project().images
    for job in spec:
        effective = job.get("backend") or backend
        if effective not in ("auto", "boat", "runpod"):
            raise ValueError(f"unknown job backend: {effective}")
        required = required_agent_capabilities(job)
        foreign = bool(job.get("image") and job["image"] not in images)
        if ("cpu_model" in job or "distinct_vm_group" in job or required) and (effective not in ("auto", "boat") or job.get("gpu") or foreign):
            raise ValueError("placement constraints require a boat CPU job; no fallback is permitted")
        if job.get("gpu"):
            if effective == "boat":
                raise ValueError("GPU jobs require runpod or auto; boat runs CPU jobs")
        elif effective == "runpod":
            raise ValueError("runpod requires a GPU job; use boat or auto for CPU jobs")
        if not job.get("gpu") and job.get("image") and job["image"] not in images:
            raise ValueError(f"unknown boat image {job['image']!r}; {config.MODAL_REMOVED}")
    outdir = pathlib.Path(out)
    outdir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    rows: dict[str, dict] = {}
    where: dict[str, str] = {}
    reasons: dict[str, str] = {}
    bsum = None
    lock = threading.Lock()

    def printer(name, row):
        with lock:
            print(f"{name:40} exit {row.get('code', -1):>3}  {row.get('seconds', -1):>8} s", flush=True)

    gpu = [job for job in spec if job.get("gpu")]
    if gpu:
        from . import runpod
        # Per-job routing has already been checked. GPU jobs never reach boat.
        got, locations, failures = runpod.gpu_jobs(gpu, out, pre, cmd, backend="runpod" if backend == "runpod" else "auto", on_done=printer)
        rows.update(got)
        where.update(locations)
        reasons.update(failures)
    cpu = [job for job in spec if not job.get("gpu")]
    pending = list(cpu)
    from . import scheduler as _sch
    for attempt in range(1 if strict_once else 3):
        if not pending:
            break
        if attempt:
            log(f"{len(pending)} unserved CPU jobs: retrying on boat in 60 s")
            time.sleep(60)
        if not strict_once and fallback.preflight_on(backend, _sch.enabled(), bool(_sch.enabled() and _sch.controller())):
            reason, _ = boat.preflight3(1, vm_type, starts=not _sch.enabled())
        else:
            reason = None
        if reason is None:
            try:
                got, bsum = boat.boat_jobs(pending, out, pre, cmd, vms=vms, idle=idle, teardown=teardown, durations=durations, vm_type=vm_type, max_vms=max_vms,
                                           on_lost=None, max_wait=max_wait if backend == "auto" else 0, on_done=printer, tar=tar,
                                           **({"strict_once": True, "max_attempts": max_attempts} if strict_once else {}))
            except boat.Unavailable as error:
                if strict_once:
                    raise
                got = {}
                reasons.update({job["name"]: str(error) for job in pending})
            rows.update(got)
            where.update({name: "boat" for name in got})
        else:
            reasons.update({job["name"]: reason for job in pending})
        pending = [job for job in pending if job["name"] not in rows]
    for job in pending:
        rows[job["name"]] = {"code": -1, "seconds": -1, "out": "", "err": reasons.get(job["name"], "boat never ran the job"),
                             "status": "LOST" if strict_once else "FAILED", "terminal": True, "identity_unavailable": True}
        where[job["name"]] = "boat"
    order = [job["name"] for job in boat_order(spec)]
    for name in order:
        row = rows.get(name, {"code": -1, "seconds": -1, "out": "", "err": "never ran", "status": "FAILED", "terminal": True})
        if row.get("lost"):
            row.setdefault("status", "LOST")
            row.setdefault("terminal", True)
        (outdir / f"{name}.log").write_text(row.get("out", "") + ("\n--- stderr ---\n" + row.get("err", "") if row.get("err", "").strip() else ""))
    placed = {name: rows[name]["placement"] for name in order if isinstance(rows.get(name, {}).get("placement"), dict)}
    if placed:
        (outdir / "placement.json").write_text(json.dumps(placed, indent=1))
    (outdir / "summary.tsv").write_text("".join(f"{name}\t{rows.get(name, {}).get('code', -1)}\t{rows.get(name, {}).get('seconds', -1)}\t{where.get(name, '-')}\n" for name in order))
    wall = round(time.time() - t0, 1)
    done = [rows.get(name, {"code": -1, "seconds": -1}) for name in order]
    failed = sum(type(row.get("code")) is not int or row["code"] != 0 for row in done)
    line = f"{len(done)} jobs, {failed} failed, {wall} s wall, {round(sum(max(row.get('seconds', 0), 0) for row in done), 1)} s of job time"
    tail, _ = where_line(where, {})
    print(line + (f"; {tail}" if tail else ""))
    timed = [p for p in placed.values() if p.get("exclusive")]
    if timed:
        kinds = sorted({f"{p['type']} ({p['cpu'] or '?'}, {p['vcpus']} of {p['vm_vcpus']} vCPUs pinned)" for p in timed})
        print(f"timing: {len(timed)} jobs each alone on its VM ({max(p['co_tenants'] for p in timed)} co-tenants): {'; '.join(kinds)}")
    if bsum:
        print(boat.summary_line(bsum))
    (outdir / "backends.json").write_text(json.dumps({"backend": backend, "wall_s": wall, "where": count(where), "fallbacks": [], "failures": reasons, "boat": bsum}, indent=1))
    return 3 if failed else 0


def _strict_native(spec: list[dict], backend: str):
    """Refuse routes that cannot honor the native scheduler's ownership contract."""
    if backend not in ("auto", "boat"):
        raise ValueError("strict_once requires a native boat CPU batch; no Modal or RunPod fallback")
    images = project().images
    for job in spec:
        if (job.get("gpu") or job.get("backend") not in (None, "boat")
                or (job.get("image") and job["image"] not in images)):
            raise ValueError("strict_once requires native boat CPU jobs and images; no provider fallback")


def boat_order(spec):
    """The summary's order: the resource groups, in the order they first appear."""
    groups: dict[tuple, list] = {}
    for j in spec:
        groups.setdefault((j.get("cpu", 2.0), j.get("memory", 4096), j.get("timeout", 900)), []).append(j)
    return [j for js in groups.values() for j in js]


def count(where: dict) -> dict:
    c: dict[str, int] = {}
    for b in where.values():
        c[b] = c.get(b, 0) + 1
    return c


def where_line(where: dict, moved: dict) -> tuple[str, list]:
    """"186 on boat, 12 on modal (fallback: boat VM bx_... lost)", and the fallbacks as rows."""
    why: dict[str, int] = {}
    for n, r in moved.items():
        if where.get(n) == "modal":
            why[r] = why.get(r, 0) + 1
    parts = [f"{k} on {b}" for b, k in sorted(count(where).items(), key=lambda x: (x[0] != "boat" and x[0] != "runpod", x[0]))]
    text = ", ".join(parts)
    if why:
        text += " (fallback: " + "; ".join(f"{k} job{'s' if k > 1 else ''}: {r}" for r, k in why.items()) + ")"
    return text, [{"jobs": k, "to": "modal", "reason": r} for r, k in why.items()]


# ----------------------------------------------------------------------------------------------- the compare runners
class ModalEx:
    """Legacy name retained only to give callers a clear removal error."""

    backend = "modal"

    def __init__(self, app):
        raise ValueError(config.MODAL_REMOVED)


class CallFailed(RuntimeError):
    """A call that ran on boat and failed by itself (its own exit, its own exception): its result, not boat's failure."""


class BoatEx:
    """The pool (boat.py): each call a py item in the runner's setting (its BOAT mounts and PATH, 2 vCPU and 4 GB, the
    function's own timeout)."""

    backend = "boat"

    def __init__(self, runner: str, vms: int = 0, idle: int | None = None, teardown: bool = False, vm_type: str = "large", max_vms: int = 0, durations: str = "",
                 limit: int = 3600, build: bool = True, max_wait: int = 0, strict_once: bool = False, max_attempts: int = 1):
        boat._validate_strict_options(strict_once, max_attempts)
        self.strict_once = strict_once
        from . import agent
        self.boat = boat
        root = project().root
        self.module = str(pathlib.Path(runner).resolve().relative_to(root))
        cfg = getattr(agent.import_runner(str(root / self.module)), "BOAT", {})
        self.extra = {k: v for k, v in cfg.items() if k in ("mounts", "path", "overlays", "env", "runtime", "required_agent_capabilities", "enforce_memory", "memory_limit_mode")}
        if cfg.get('enforce_memory') is True and 'memory' in cfg:
            self.extra['memory'] = cfg['memory']
        required_agent_capabilities(self.extra)
        self.weight = int(cfg.get("weight", 1))  # slots a call takes (a runner whose calls need more than 4 GB says 2)
        if self.extra.get('enforce_memory') is True:
            from .agent import normalize_memory_limit
            self.weight = max(self.weight, math.ceil(normalize_memory_limit(self.extra)['mb'] / boat.SLOT_MEM))
        self.vms, self.max_vms, self.limit, self.max_wait = vms, max_vms, limit, max_wait
        self.past = {}
        if durations:  # a results.jsonl of an earlier run: each (tool, task)'s seconds, to start the longest first
            for l in pathlib.Path(durations).read_text().splitlines():
                with contextlib.suppress(Exception):
                    r = json.loads(l)
                    self.past[(r.get("tool"), r.get("task"))] = float(r.get("seconds", 0))
        self.sess = boat.session(pathlib.Path(runner).name, idle=idle, teardown=teardown, vm_type=vm_type, build=build, oleans=False,
                                 **({"strict_once": True, "max_attempts": max_attempts} if strict_once else {}))
        self.sess.max_starts = max_vms
        self.n = 0
        self.calls = 0
        self.summary = None

    def _est(self, args) -> float:
        tool = args[0] if args and isinstance(args[0], str) else None
        tasks = args[1] if len(args) > 1 else []
        tasks = tasks if isinstance(tasks, list) else [tasks]
        lim = next((a for a in args if isinstance(a, int) and not isinstance(a, bool)), 60)
        name = lambda t: t.get("task") if isinstance(t, dict) else t
        return sum(self.past.get((tool, name(t)), lim / 2) for t in tasks) or 1

    def remote(self, fn, *args):
        self.sess.ensure(1)
        self.calls += 1
        out = {}
        self.sess.run([{"name": f"call-{fname(fn)}-{int(time.time() * 1000)}", "kind": "py", "module": self.module, "fn": fname(fn), "args": list(args),
                        "limit": self.limit, "weight": self.weight if self.extra.get('enforce_memory') is True else 1, "est": 1, **self.extra}], lambda it, row: out.update(row), max_wait=self.max_wait or 600)
        what = fallback.row_outcome("result" in out, int(out.get("code", -1) if isinstance(out.get("code", -1), int) else -1), float(out.get("seconds", -1) or -1) >= 0, bool(out.get("lost")))
        if what == "own":
            raise CallFailed(f"{fname(fn)} failed on boat: {str(out.get('error', out))[-1500:]}")
        if what == "lost":
            raise RuntimeError(f"{fname(fn)}: boat lost it: {str(out.get('error', out))[-1500:]}")
        return out["result"]

    def starmap(self, fn, args: list, on_lost=None, **_):
        """Each call's result as it finishes (an exception for a call that failed); a call lost twice goes to on_lost."""
        name = fname(fn)
        items = []
        for a in args:
            self.n += 1
            items.append({"name": f"{name}-{self.n}", "kind": "py", "module": self.module, "fn": name, "args": list(a), "limit": self.limit,
                          "weight": self.weight, "est": self._est(list(a)), **self.extra})
        slots = self.boat.TYPES[self.sess.vm_type][0] // self.boat.SLOT_CPU
        n = 0 if getattr(self.sess, "scheduled", False) else self.boat.plan_vms(items, slots, self.vms, self.max_vms)
        self.boat.log(f"{len(items)} calls of {name} " + ("to the scheduler" if getattr(self.sess, "scheduled", False) else f"on up to {n} {self.sess.vm_type} VM(s)"))
        self.sess.early = not self.sess.teardown  # (its VMs go back to the pool as the calls run out)
        self.sess.ensure(n)
        q: queue.Queue = queue.Queue()
        END = object()

        def done(it, row):
            if fallback.row_outcome("result" in row, row.get("code") if isinstance(row.get("code"), int) else -1, float(row.get("seconds", -1) or -1) >= 0, bool(row.get("lost"))) == "ok":
                q.put(row["result"])
            else:
                q.put(RuntimeError(f"{it['name']}: exit {row.get('code')}: {str(row.get('error', ''))[-600:]}"))

        def go():
            try:
                self.sess.run(items, done, on_lost=(lambda it, why: on_lost(it["args"], why)) if on_lost and not self.strict_once else None, max_wait=self.max_wait)
            except BaseException as e:
                q.put(e if isinstance(e, Exception) else RuntimeError(repr(e)))
            finally:
                q.put(END)
        threading.Thread(target=go, daemon=True).start()
        while True:
            try:
                r = q.get(timeout=1)
            except queue.Empty:
                continue
            if r is END:
                break
            yield r

    def close(self):
        if self.summary is None:
            self.summary = self.sess.close()
            self.summary["dollars_so_far"] = self.sess.dollars()
            self.summary.setdefault("warm", 0)
            self.summary["batch"] = self.sess.batch
            self.summary["image"] = self.sess.image
            self.boat.log(self.boat.summary_line(self.summary))
            print(f"{self.n + self.calls} calls on boat", flush=True)
            self.summary["where"] = {"boat": self.n + self.calls}
        return self.summary


class AutoEx:
    """Boat-only callable execution; exhausted calls fail without changing provider."""

    backend = "auto"
    strict_once = False  # Preserve legacy/unpickled instances that predate the opt-in field.

    def __init__(self, runner: str, app, max_wait: int = 120, **opts):
        self.strict_once = opts.get("strict_once", False)
        boat._validate_strict_options(self.strict_once, opts.get("max_attempts", 1))
        from . import agent
        cfg = {} if self.strict_once else getattr(agent.import_runner(str(pathlib.Path(runner).resolve())), "BOAT", {})
        self.no_fallback = self.strict_once or bool(required_agent_capabilities(cfg))
        self.app, self.modal, self.bx = app, None, None
        self.where = {"boat": 0}
        self.why: dict[str, int] = {}
        from . import scheduler as _sch
        self.reason, _ = (None, {}) if self.no_fallback or (_sch.enabled() and _sch.controller()) else boat.preflight3(1, opts.get("vm_type", boat.VM_TYPE), starts=not _sch.enabled())
        # Boat owns physical retries; exhaustion is terminal. A call's own
        # failure never spends another call's retry budget.
        self.fb = fallback.AutoFallback(3)
        self._mk = lambda: BoatEx(runner, build=False, max_wait=max_wait, **opts)
        if self.reason is None:
            try:
                self.bx = self._mk()
            except boat.Unavailable as e:
                if self.no_fallback:
                    raise
                self.reason = str(e)
        if self.reason:  # (boat can't take it now: an image, a tree, the scheduler; asked again, never Modal for it: rules.fallback.lost_route)
            log(f"boat can't take this batch now ({self.reason}): asked again")

    def _boat(self) -> bool:
        """boat for this batch: its session, made again (3 tries a minute apart) when boat couldn't take it."""
        for k in range(3):
            if self.bx is not None:
                return True
            if k:
                time.sleep(60)
            try:
                self.bx = self._mk()
                self.reason = None
            except boat.Unavailable as e:
                self.reason = str(e)
        return False

    boat_fails = property(lambda self: self.fb.boat_fails)

    def _modal(self):
        raise ValueError(config.MODAL_REMOVED)

    def _moved(self, k: int, reason: str):
        raise ValueError(config.MODAL_REMOVED)

    def remote(self, fn, *args):
        if getattr(self, "strict_once", False) or getattr(self, "no_fallback", False):
            result = self.bx.remote(fn, *args)
            self.where["boat"] += 1
            return result
        for attempt in range(6):
            if not self._boat():
                raise RuntimeError(f"boat can't run this call now ({self.reason})")
            try:
                result = self.bx.remote(fn, *args)
                self.fb.outcome("ok")
                self.where["boat"] += 1
                return result
            except CallFailed:
                self.fb.outcome("own")
                self.where["boat"] += 1
                raise
            except Exception as error:
                failures = boat.boat_fails_of(str(error))
                if fallback.lost_route(False, failures, 3, False) == "failed":
                    self.fb.refused()
                    raise
                if attempt == 5:
                    raise
                log(f"boat did not return the call ({str(error)[:160]}): retrying on boat")
                time.sleep(5 * (attempt + 1))

    def starmap(self, fn, args: list, **_):
        if getattr(self, "strict_once", False) or getattr(self, "no_fallback", False):
            for result in self.bx.starmap(fn, args):
                self.where["boat"] += 1
                yield result
            return
        pending = list(args)
        for _ in range(3):
            if not pending or not self._boat():
                break
            lost = []
            for result in self.bx.starmap(fn, pending, on_lost=lambda item, why: lost.append((item, why))):
                self.where["boat"] += 1
                yield result
            pending = []
            for item, why in lost:
                if fallback.lost_route(False, boat.boat_fails_of(why), 3, False) == "failed":
                    self.where["boat"] += 1
                    yield RuntimeError(why)
                else:
                    pending.append(item)
        for _ in pending:
            self.where["boat"] += 1
            yield RuntimeError(f"boat couldn't run this call ({self.reason or 'no returned row'})")

    def close(self):
        if getattr(self, "closed", None) is not None:
            return self.closed
        summary = self.bx.close() if self.bx else {}
        print(", ".join(f"{n} calls on {backend}" for backend, n in self.where.items() if n), flush=True)
        self.closed = {**summary, "where": self.where, "fallbacks": []}
        return self.closed


def cli(run, runner: str, doc: str = "", backend: str = "auto"):
    """`python3 <runner> --backend auto|boat|modal ...`: run's own parameters as flags (as `modal run` makes them),
    plus the pool's."""
    import argparse
    import signal
    ap = argparse.ArgumentParser(description=(doc or "").split("\n\n")[0])
    ap.add_argument("--backend", default=os.environ.get("SUPERCOMPUTER_BACKEND") or backend, choices=["modal", "auto", "boat"],
                    help=f"where its calls run (default {backend}: the runner's own, or $SUPERCOMPUTER_BACKEND)")
    ap.add_argument("--vms", type=int, default=0, help="boat VMs for the batch (default: by the work and the plan's limits)")
    ap.add_argument("--max-vms", type=int, default=0, help="at most this many VMs, and the batch's cap on its starts (default 50: GDPW_BOAT_MAX_STARTS)")
    ap.add_argument("--idle", type=int, default=None, help="seconds a released VM stays up before boat stops it (0: stop at once; default 60)")
    ap.add_argument("--teardown", action="store_true", help="delete the batch's VMs at the end instead of stopping them")
    ap.add_argument("--vm-type", default="large", choices=["small", "default", "large", "xlarge"])
    ap.add_argument("--durations", default="", help="an earlier results.jsonl: the longest calls start first")
    ap.add_argument("--max-wait", type=int, default=120, help="auto: seconds to wait for a boat VM before reporting failure")
    ap.add_argument("--class", dest="sc_class", default="", choices=["", *__import__("supercomputer.scheduler", fromlist=["CLASSES"]).CLASSES],
                    help="the scheduler's priority class (default $SUPERCOMPUTER_CLASS, else submission; the bar sets gate)")
    ap.add_argument("--timing", dest="sc_timing", action="store_true", help="each call alone on its VM (a timing claim; $SUPERCOMPUTER_TIMING=1)")
    ap.add_argument("--strict-once", action="store_true", help="zero or one native attempt; no retry or provider fallback (requires a capable scheduler)")
    ap.add_argument("--max-attempts", type=int, default=1, choices=[1], help="strict mode supports exactly one authorized physical attempt")
    params = list(inspect.signature(run).parameters.values())[1:]
    for p in params:
        flag = "--" + p.name.replace("_", "-")
        if p.default is inspect.Parameter.empty:
            ap.add_argument(flag, required=True, dest=p.name)
        elif isinstance(p.default, bool):
            ap.add_argument(flag, action="store_true", default=p.default, dest=p.name)
        else:
            ap.add_argument(flag, type=type(p.default), default=p.default, dest=p.name)
    a = ap.parse_args()
    try:
        config.reject_removed_backend(a.backend)
    except ValueError as error:
        ap.error(str(error))
    if a.sc_class:
        os.environ["SUPERCOMPUTER_CLASS"] = a.sc_class
    if a.sc_timing:
        os.environ["SUPERCOMPUTER_TIMING"] = "1"
    kw = {p.name: getattr(a, p.name) for p in params}
    mod = sys.modules["__main__"]
    t0 = time.time()

    def h(sig, frame):
        raise KeyboardInterrupt(f"signal {sig}")
    for s in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, h)
    opts = dict(vms=a.vms, idle=a.idle, teardown=a.teardown, vm_type=a.vm_type, max_vms=a.max_vms, durations=a.durations)
    if a.strict_once:
        opts.update(strict_once=True, max_attempts=a.max_attempts)
    ex = BoatEx(runner, max_wait=0, **opts) if a.backend == "boat" else AutoEx(runner, getattr(mod, "app", None), max_wait=a.max_wait, **opts)
    s = {}
    try:
        run(ex, **kw)
    finally:
        s = ex.close()
        out = kw.get("out")
        if out:
            pathlib.Path(out).mkdir(parents=True, exist_ok=True)
            (pathlib.Path(out) / "backends.json").write_text(json.dumps({"backend": a.backend, **(s or {}), "wall_s": round(time.time() - t0, 1)}, indent=1))


# -------------------------------------------------------------------------------------------------------- GPU runners
def gpu_call(runner: str, fn: str, args: list, app=None, modal_fn=None, backend: str = "auto", gpus: str = "", label: str = "gpu") -> tuple[dict, dict]:
    """Run a GPU call only on RunPod; exhausted pod retries raise the failure."""
    config.reject_removed_backend(backend)
    if backend not in ("auto", "runpod"):
        raise ValueError("GPU jobs require runpod or auto")
    from . import runpod
    reason = runpod.preflight() if backend == "auto" else None
    if reason is None:
        tries = fallback.GpuTries()
        while tries.next_pod():
            try:
                result = runpod.call(runner, fn, args, gpus, label=label)
                tries.ended("ok")
                return result, {"backend": "runpod", "fallback": None, "runpod": result.pop("runpod", None)}
            except runpod.NoCapacity as error:
                tries.ended("no_capacity")
                reason = f"runpod has no GPU in the list ({str(error)[:300]})"
            except KeyboardInterrupt:
                raise
            except Exception as error:
                tries.ended("failed")
                reason = f"runpod pod failed ({str(error)[:300]})"
    raise RuntimeError(reason or "RunPod did not return a result")


def gpu_cli(main, runner: str):
    """`python3 <GPU runner> --backend auto|runpod|modal ...`: the entrypoint's parameters as flags."""
    import argparse
    import signal
    reexec_with_modal()
    main = getattr(getattr(main, "info", None), "raw_f", None) or main  # (under the Modal client, the entrypoint wraps it)
    ap = argparse.ArgumentParser()
    for p in inspect.signature(main).parameters.values():
        flag = "--" + p.name.replace("_", "-")
        if p.default is inspect.Parameter.empty:
            ap.add_argument(flag, required=True, dest=p.name)
        elif isinstance(p.default, bool):
            ap.add_argument(flag, action="store_true", default=p.default, dest=p.name)
        else:
            ap.add_argument(flag, type=type(p.default), default="auto" if p.name == "backend" else p.default, dest=p.name)
    a = ap.parse_args()
    config.reject_removed_backend(getattr(a, "backend", "auto"))

    def h(sig, frame):
        raise KeyboardInterrupt(f"signal {sig}")
    for s in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, h)
    main(**vars(a))
