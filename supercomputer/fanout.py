"""One interface over three providers: boat.dev VMs for CPU work (boat.py), RunPod pods for GPU work (runpod.py),
Modal for the rest and as the fallback of both.

    --backend modal     Modal, as always (the default until it's flipped)
    --backend auto      CPU work on boat, GPU work on RunPod; Modal takes over when the primary can't serve
    --backend boat | runpod                   force one, no fallback

Fallback is per batch where it can be (the whole batch moves before anything starts) and per job when a VM or pod dies
under it (that job once more on the primary, then on Modal). The reasons: the provider's API unreachable or erroring,
its billing (credit, balance), no capacity (boat's active-VM or start limits leaving no VM within --max-wait, a pool
image that isn't built; RunPod out of every GPU type in the list), a VM or pod lost mid-job. Every fallback is logged
with its reason, in the final line and in <out>/backends.json; a job's results are the same whichever provider ran it.
Whatever happens, each provider's cleanup runs: boat VMs released (stopped after --idle), pods deleted, the Modal app
stopped (its id and this client's pid in LIVE while it runs: `supercomputer reap` stops it if this client dies).

The compare runners write their entrypoint's body as `run(ex, ...)` with `ex.remote(fn, *args)` and
`ex.starmap(fn, args)` (results as they finish, an exception for a failed call); `python3 <runner> --backend ...`
gives it the right `ex` (`fanout.cli`), and `modal run <runner>` still runs it on Modal.
"""
import contextlib
import importlib.util
import inspect
import json
import os
import pathlib
import queue
import shutil
import sys
import threading
import time

from . import boat, config
from .rules import fallback
from .config import project

HERE = pathlib.Path(__file__).resolve().parent
BACKENDS = ["modal", "auto", "boat", "runpod"]


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
    """Plain python3 without the Modal client: run again under the interpreter the `modal` CLI uses, so Modal can take
    over a batch. (Without one, Modal is simply not a fallback.)"""
    reexeced = bool(os.environ.get("GDPW_REEXEC") or os.environ.get("SUPERCOMPUTER_REEXEC"))
    if modal_real() or reexeced:
        return
    # (installed with uv, the Modal client is a dependency: it imports here when needed, and the `modal` CLI's
    # interpreter would not have this package; found, not imported, so a command that never needs it stays quick)
    if "modal" not in sys.modules and importlib.util.find_spec("modal") is not None:
        return
    exe = shutil.which("modal")
    if not exe:
        return
    with contextlib.suppress(Exception):
        first = pathlib.Path(exe).read_text(errors="replace").splitlines()[0]
        py = first[2:].strip().split()[0] if first.startswith("#!") else ""
        # (a venv's python is often a symlink to the same interpreter: compare the paths, not what they resolve to;
        # the decision rules.fallback.reexec_modal's)
        if fallback.reexec_modal(modal_real(), reexeced, bool(py) and os.path.exists(py), os.path.abspath(py or ".") == os.path.abspath(sys.executable)):
            os.environ["GDPW_REEXEC"] = os.environ["SUPERCOMPUTER_REEXEC"] = "1"
            os.execv(py, [py, *sys.argv])


class ModalCtx:
    """The runner's Modal app, running: the one `modal run` started, or one started here (and stopped on exit), its
    id and this client's pid in LIVE while it runs (`supercomputer reap` stops it if this client dies)."""

    _refs: dict = {}  # id(app) -> rules.fallback.ModalRef: who holds it, whether this process started it
    _ctxs: dict = {}  # id(app) -> its run context, for an app this process started

    def __init__(self, app):
        self.app, self.ctx, self.ticket = app, None, None

    def __enter__(self):
        if not modal_real():
            raise RuntimeError("the Modal client isn't installed here, so Modal can't take this over")
        # (rules.fallback.ModalRef: an app started here shared, stopped when the last holder exits; one `modal run`
        # started used as it is, never stopped here)
        ref = ModalCtx._refs.setdefault(id(self.app), fallback.ModalRef(getattr(self.app, "app_id", None) is not None))
        was_ours = ref.ours
        ref.enter()
        if ref.ours and not was_ours:
            self.ctx = self.app.run()
            self.ctx.__enter__()
            ModalCtx._ctxs[id(self.app)] = self.ctx
        elif ref.ours:
            self.ctx = ModalCtx._ctxs[id(self.app)]
        live = config.live_dir()
        self.ticket = live / f"{self.app.app_id}"
        self.ticket.write_text(str(os.getpid()))
        if self.ctx:
            log(f"Modal app {self.app.app_id}")
        return self

    def __exit__(self, *exc):
        ref = ModalCtx._refs.get(id(self.app))
        if ref is None:
            return False
        was_running = ref.running
        ref.exit()
        if ref.holders > 0:
            return False
        if self.ticket:
            self.ticket.unlink(missing_ok=True)
        if self.ctx and was_running and not ref.running:
            ModalCtx._ctxs.pop(id(self.app), None)
            self.ctx.__exit__(*exc)
        return False


# ----------------------------------------------------------------------------------------------------- a jobs file
def run_jobs(app, spec: list[dict], out: str, pre: str, cmd, modal_jobs, backend: str = "modal", vms: int = 0, idle: int | None = None, teardown: bool = False,
             durations: str = "", vm_type: str = "large", max_vms: int = 0, max_wait: int = 120, tar: bytes | None = None, gpu_modal=None):
    """A jobs file's batch on its backend. modal_jobs(jobs, on_done) runs jobs on Modal (inside the running app) and
    returns {name: {code, seconds, out, err}}. Writes <out>/<name>.log and <out>/summary.tsv (name, exit, seconds,
    backend) and prints the final line, with where the jobs ran and why any moved. A job that declares a GPU ("gpu":
    a type or true) runs on a RunPod pod (one for all of them), else on Modal (runpod.py, gpu_jobs)."""
    if backend not in ("modal", "auto", "boat", "runpod"):
        raise SystemExit(f"--backend is modal, auto, boat or runpod, not {backend}")
    outdir = pathlib.Path(out)
    outdir.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    rows: dict[str, dict] = {}
    where: dict[str, str] = {}
    moved: dict[str, str] = {}  # job -> why it went to Modal
    bsum = None
    lock = threading.Lock()

    def printer(name, row):
        with lock:
            print(f"{name:40} exit {row.get('code', -1):>3}  {row.get('seconds', -1):>8} s", flush=True)

    gpu = [j for j in spec if j.get("gpu")]
    if gpu:
        from . import runpod
        got, w, why = runpod.gpu_jobs(gpu, out, pre, cmd, backend=backend, on_done=printer, gpu_modal=gpu_modal)
        rows.update(got)
        where.update(w)
        moved.update(why)
        spec_cpu = [j for j in spec if not j.get("gpu")]
    else:
        spec_cpu = list(spec)
    todo = list(spec_cpu)
    # (a job on an image boat doesn't have, or one that says "backend": "modal": Modal's, whatever the backend)
    images = project().images
    own = [j for j in spec_cpu if (j.get("image") and j["image"] not in images) or j.get("backend") == "modal"]
    if own:
        moved.update({j["name"]: f"its image ({j['image']}) is Modal's" if j.get("image") else "it asks for Modal" for j in own})
    if backend in ("auto", "boat") and len(own) < len(spec_cpu):
        from . import scheduler as _sch
        # (with the scheduler, starts are its call, by class: gates and interactive checks may pay for them, and what
        # it can't place within --max-wait comes back here for Modal; this machine checks only boat's API and image:
        # rules.fallback.preflight_on)
        # (Modal only for what boat failed 3 times on distinct VMs, or a job Modal owns: rules.fallback.lost_route; a
        # batch boat couldn't take now (its preflight, Unavailable: an image, a tree, the scheduler) and every other job
        # boat didn't return run again on boat, 3 passes a minute apart, then fail with why: never Modal for those)
        mine = [j for j in spec_cpu if j not in own]
        for k in range(3):
            if not mine:
                break
            if k:
                log(f"{len(mine)} jobs boat didn't run, not for 3 boat failures ({'; '.join(sorted({str(moved.get(j['name'], ''))[:80] for j in mine})[:2])}): again on boat in 60 s")
                time.sleep(60)
            if fallback.preflight_on(backend, _sch.enabled(), bool(_sch.enabled() and _sch.controller())):
                reason, info = boat.preflight3(1, vm_type, starts=not _sch.enabled())
            else:
                reason, info = None, {}
            if reason is None:
                try:
                    got, bsum = boat.boat_jobs(mine, out, pre, cmd, vms=vms, idle=idle, teardown=teardown, durations=durations, vm_type=vm_type, max_vms=max_vms,
                                               on_lost=lambda n, why: moved.__setitem__(n, why), max_wait=max_wait if backend == "auto" else 0, on_done=printer, tar=tar)
                except boat.Unavailable as e:
                    got = {}
                    moved.update({j["name"]: str(e) for j in mine})
                rows.update(got)
                where.update({n: "boat" for n in got})
            else:
                moved.update({j["name"]: reason for j in mine})
                log(f"boat can't take this batch now ({reason})")
            mine = [j for j in mine if j["name"] not in rows and fallback.lost_route(False, boat.boat_fails_of(moved.get(j["name"])), 3, backend == "auto") == "boat"]
        todo = []
        for j in [j for j in spec_cpu if j["name"] not in rows]:
            r = fallback.lost_route(j in own, boat.boat_fails_of(moved.get(j["name"])), 3, backend == "auto")
            if r == "modal":
                todo.append(j)
            else:  # (boat failed it 3 times under --backend boat, or boat couldn't run it at all: failed, with why)
                rows[j["name"]] = {"code": -1, "seconds": -1, "out": "", "err": moved.get(j["name"], "never ran")}
                where[j["name"]] = "boat"
    if todo:
        with ModalCtx(app):
            got = modal_jobs(todo, printer)
        rows.update(got)
        where.update({n: "modal" for n in got})
    order = [j["name"] for j in boat_order(spec)]
    for name in order:
        r = rows.get(name, {"code": -1, "seconds": -1, "out": "", "err": "never ran"})
        (outdir / f"{name}.log").write_text(r.get("out", "") + ("\n--- stderr ---\n" + r.get("err", "") if r.get("err", "").strip() else ""))
    placed = {n: rows[n]["placement"] for n in order if isinstance(rows.get(n, {}).get("placement"), dict)}
    if placed:  # (where each job ran: the VM type, its CPU, its vCPUs, alone or beside how many others: a timing claim's label)
        (outdir / "placement.json").write_text(json.dumps(placed, indent=1))
    (outdir / "summary.tsv").write_text("".join(f"{n}\t{rows.get(n, {}).get('code', -1)}\t{rows.get(n, {}).get('seconds', -1)}\t{where.get(n, '-')}\n" for n in order))
    wall = round(time.time() - t0, 1)
    done = [rows.get(n, {"code": -1, "seconds": -1}) for n in order]
    line = f"{len(done)} jobs, {sum(r.get('code', -1) != 0 for r in done)} failed, {wall} s wall, {round(sum(max(r.get('seconds', 0), 0) for r in done), 1)} s of job time"
    tail, why = where_line(where, moved)
    print(line + (f"; {tail}" if backend != "modal" else ""))
    timed = [p for p in placed.values() if p.get("exclusive")]
    if timed:
        kinds = sorted({f"{p['type']} ({p['cpu'] or '?'}, {p['vcpus']} of {p['vm_vcpus']} vCPUs pinned)" for p in timed})
        print(f"timing: {len(timed)} jobs each alone on its VM ({max(p['co_tenants'] for p in timed)} co-tenants): {'; '.join(kinds)}")
    if bsum:
        print(boat.summary_line(bsum))
    (outdir / "backends.json").write_text(json.dumps({"backend": backend, "wall_s": wall, "where": count(where), "fallbacks": why, "boat": bsum}, indent=1))


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
    """Modal, as the runners always did (`modal run`, the app already running)."""

    backend = "modal"

    def __init__(self, app):
        self.m = ModalCtx(app).__enter__()

    def remote(self, fn, *args):
        return fn.remote(*args)

    def starmap(self, fn, args: list, **_):
        return fn.starmap(args, order_outputs=False, return_exceptions=True)

    def close(self):
        self.m.__exit__(None, None, None)
        return {}


class CallFailed(RuntimeError):
    """A call that ran on boat and failed by itself (its own exit, its own exception): its result, not boat's failure."""


class BoatEx:
    """The pool (boat.py): each call a py item in the runner's setting (its BOAT mounts and PATH, 2 vCPU and 4 GB, the
    function's own timeout)."""

    backend = "boat"

    def __init__(self, runner: str, vms: int = 0, idle: int | None = None, teardown: bool = False, vm_type: str = "large", max_vms: int = 0, durations: str = "",
                 limit: int = 3600, build: bool = True, max_wait: int = 0):
        from . import agent
        self.boat = boat
        root = project().root
        self.module = str(pathlib.Path(runner).resolve().relative_to(root))
        cfg = getattr(agent.import_runner(str(root / self.module)), "BOAT", {})
        self.extra = {k: v for k, v in cfg.items() if k in ("mounts", "path", "overlays", "env")}
        self.weight = int(cfg.get("weight", 1))  # slots a call takes (a runner whose calls need more than 4 GB says 2)
        self.vms, self.max_vms, self.limit, self.max_wait = vms, max_vms, limit, max_wait
        self.past = {}
        if durations:  # a results.jsonl of an earlier run: each (tool, task)'s seconds, to start the longest first
            for l in pathlib.Path(durations).read_text().splitlines():
                with contextlib.suppress(Exception):
                    r = json.loads(l)
                    self.past[(r.get("tool"), r.get("task"))] = float(r.get("seconds", 0))
        self.sess = boat.session(pathlib.Path(runner).name, idle=idle, teardown=teardown, vm_type=vm_type, build=build, oleans=False)
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
                        "limit": self.limit, "weight": 1, "est": 1, **self.extra}], lambda it, row: out.update(row), max_wait=self.max_wait or 600)
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
                self.sess.run(items, done, on_lost=(lambda it, why: on_lost(it["args"], why)) if on_lost else None, max_wait=self.max_wait)
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
    """boat first; Modal for the whole batch when boat failed it 3 times (its API, 3 tries), and for each call boat
    failed 3 times (the scheduler's count: a VM lost, a row missing, no VM ready)."""

    backend = "auto"

    def __init__(self, runner: str, app, max_wait: int = 120, **opts):
        self.app, self.modal, self.bx = app, None, None
        self.where = {"boat": 0, "modal": 0}
        self.why: dict[str, int] = {}
        from . import scheduler as _sch
        self.reason, _ = (None, {}) if _sch.enabled() and _sch.controller() else boat.preflight3(1, opts.get("vm_type", boat.VM_TYPE), starts=not _sch.enabled())
        # (boat's failures of this batch's calls, a call's own failure never one: Modal takes the rest only at the
        # third; rules.fallback.AutoFallback)
        self.fb = fallback.AutoFallback(3)
        self._mk = lambda: BoatEx(runner, build=False, max_wait=max_wait, **opts)
        if self.reason is None:
            try:
                self.bx = self._mk()
            except boat.Unavailable as e:
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
        if self.modal is None:
            self.modal = ModalCtx(self.app).__enter__()
        return self.modal

    def _moved(self, k: int, reason: str):
        self.why[reason] = self.why.get(reason, 0) + k

    def remote(self, fn, *args):
        tries = 0
        while self.bx or (not self.fb.on_modal and self._boat()):
            try:
                r = self.bx.remote(fn, *args)
                self.fb.outcome("ok")
                self.where["boat"] += 1
                return r
            except CallFailed:
                self.fb.outcome("own")  # (its own failure: its result, raised; never boat's)
                self.where["boat"] += 1
                raise
            except Exception as e:
                tries += 1
                if fallback.lost_route(False, boat.boat_fails_of(str(e)), 3, True) == "boat":  # (not 3 real boat failures: boat again)
                    if tries >= 6:
                        raise
                    log(f"boat didn't return a call ({str(e)[:160]}): again on boat")
                    time.sleep(5 * tries)
                    continue
                if self.fb.outcome("lost") == "again":
                    log(f"boat failed a call ({str(e)[:160]}; {self.boat_fails} of 3): again on boat")
                    time.sleep(5 * self.boat_fails)
                    continue
                self.reason = f"boat failed 3 times (last: {str(e)[:200]})"
                log(f"{self.reason}: Modal runs the rest of this batch")
                with contextlib.suppress(Exception):
                    self.bx.close()
                self.bx = None
        if not self.fb.on_modal:  # (boat couldn't take the batch at all: never Modal for that)
            raise RuntimeError(f"boat can't run this call now ({self.reason})")
        self._modal()
        self._moved(1, self.reason)
        self.where["modal"] += 1
        return fn.remote(*args)

    def starmap(self, fn, args: list, **_):
        rest, to_modal = list(args), []
        for _ in range(3):  # (calls boat lost but not for 3 real boat failures: boat again, 3 passes; rules.fallback.lost_route)
            if not rest or not self._boat():
                break
            lost: list = []
            for r in self.bx.starmap(fn, rest, on_lost=lambda a, why: lost.append((a, why))):
                self.where["boat"] += 1
                yield r
            rest = []
            for a, why in lost:
                if fallback.lost_route(False, boat.boat_fails_of(why), 3, True) == "modal":
                    to_modal.append(a)
                    self._moved(1, why)
                else:
                    rest.append(a)
        for _ in rest:  # (boat couldn't run them, not for 3 real boat failures: their failure, never Modal)
            yield RuntimeError(f"boat couldn't run this call ({self.reason or 'lost, not for 3 boat failures'})")
        rest = to_modal
        if rest:
            self._modal()
            log(f"{len(rest)} calls of {fname(fn)} on Modal")
            for r in fn.starmap(rest, order_outputs=False, return_exceptions=True):
                self.where["modal"] += 1
                yield r

    def close(self):
        if getattr(self, "closed", None) is not None:
            return self.closed
        s = {}
        try:
            if self.bx:
                s = self.bx.close()
        finally:
            if self.modal:
                self.modal.__exit__(None, None, None)
        text = ", ".join(f"{k} calls on {b}" for b, k in self.where.items() if k)
        if self.why:
            text += " (fallback: " + "; ".join(f"{k}: {r}" for r, k in self.why.items()) + ")"
        print(text, flush=True)
        self.closed = {**s, "where": self.where, "fallbacks": [{"calls": k, "to": "modal", "reason": r} for r, k in self.why.items()]}
        return self.closed


def cli(run, runner: str, doc: str = "", backend: str = "auto"):
    """`python3 <runner> --backend auto|boat|modal ...`: run's own parameters as flags (as `modal run` makes them),
    plus the pool's."""
    import argparse
    import signal
    reexec_with_modal()
    ap = argparse.ArgumentParser(description=(doc or "").split("\n\n")[0])
    ap.add_argument("--backend", default=os.environ.get("SUPERCOMPUTER_BACKEND") or backend, choices=["modal", "auto", "boat"],
                    help=f"where its calls run (default {backend}: the runner's own, or $SUPERCOMPUTER_BACKEND)")
    ap.add_argument("--vms", type=int, default=0, help="boat VMs for the batch (default: by the work and the plan's limits)")
    ap.add_argument("--max-vms", type=int, default=0, help="at most this many VMs, and the batch's cap on its starts (default 50: GDPW_BOAT_MAX_STARTS)")
    ap.add_argument("--idle", type=int, default=None, help="seconds a released VM stays up before boat stops it (0: stop at once; default 60)")
    ap.add_argument("--teardown", action="store_true", help="delete the batch's VMs at the end instead of stopping them")
    ap.add_argument("--vm-type", default="large", choices=["small", "default", "large", "xlarge"])
    ap.add_argument("--durations", default="", help="an earlier results.jsonl: the longest calls start first")
    ap.add_argument("--max-wait", type=int, default=120, help="auto: seconds to wait for a boat VM before Modal takes the batch")
    ap.add_argument("--class", dest="sc_class", default="", choices=["", *__import__("supercomputer.scheduler", fromlist=["CLASSES"]).CLASSES],
                    help="the scheduler's priority class (default $SUPERCOMPUTER_CLASS, else submission; the bar sets gate)")
    ap.add_argument("--timing", dest="sc_timing", action="store_true", help="each call alone on its VM (a timing claim; $SUPERCOMPUTER_TIMING=1)")
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
    if a.backend == "modal":
        with ModalCtx(mod.app):
            ex = ModalEx.__new__(ModalEx)
            ex.m = contextlib.nullcontext()
            ex.close = lambda: {}
            return run(ex, **kw)
    ex = BoatEx(runner, max_wait=0, **opts) if a.backend == "boat" else AutoEx(runner, mod.app, max_wait=a.max_wait, **opts)
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
def gpu_call(runner: str, fn: str, args: list, app, modal_fn, backend: str = "auto", gpus: str = "", label: str = "gpu") -> tuple[dict, dict]:
    """A GPU runner's function: on RunPod (auto, runpod), on Modal when RunPod can't (auto) or when asked (modal).
    Returns the function's result (the same either way) and where it ran and why."""
    from . import runpod
    meta: dict = {"backend": None, "fallback": None}
    if backend in ("auto", "runpod"):
        reason = runpod.preflight() if backend == "auto" else None
        if reason is None:
            tries = fallback.GpuTries()  # (two pods at most; none after "no capacity")
            while tries.next_pod():
                try:
                    r = runpod.call(runner, fn, args, gpus, label=label)
                    tries.ended("ok")
                    meta.update(backend="runpod", runpod=r.pop("runpod", None))
                    return r, meta
                except runpod.NoCapacity as e:
                    tries.ended("no_capacity")
                    reason = f"runpod has no GPU in the list ({str(e)[:300]})"
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    tries.ended("failed")
                    reason = f"runpod pod failed ({str(e)[:300]})"
                    log(f"{reason}" + ("; once more on a new pod" if tries.pods < 2 else ""))
                    if backend == "runpod" and tries.pods >= 2:
                        raise
        if fallback.gpu_leftover(backend) == "failed":
            raise RuntimeError(reason)
        log(f"{reason}: Modal runs it")
        meta["fallback"] = reason
    with ModalCtx(app):
        call = modal_fn.spawn(*args)
        try:
            r = call.get()
        finally:
            with contextlib.suppress(Exception):
                call.cancel()
    meta["backend"] = "modal"
    return r, meta


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

    def h(sig, frame):
        raise KeyboardInterrupt(f"signal {sig}")
    for s in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, h)
    main(**vars(a))
