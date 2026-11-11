"""The treadmill: a long-lived client of the boat pool that keeps a base pool of VMs busy with a queue, caches every
result by key so nothing unchanged runs twice, and writes a daily digest. This is its generic part: the batches (boat
first, Modal for what boat loses), the result cache, the queue of submissions (a jobs file and its tree as it is),
the controller's loop (work launched while the chains gain; no VM-hour or dollar cap) and the digest's pool section. A
repo adds its own chains (gdp-wired: benchmark sweeps, soundness fuzz and mutants) by subclassing Treadmill in the
module its supercomputer.toml names ([treadmill] module), which `supercomputer treadmill` runs.

    supercomputer queue add <jobs.json> [--tree DIR] [--name NAME]     a batch for the treadmill's queue
    supercomputer queue ls | results <name>
    supercomputer treadmill run [--vms 8] [--hours 0] [--quiet 2700]
    supercomputer digest [--day YYYY-MM-DD]

Every batch is the pool's (a Session): it leases warm VMs first, starts what's missing, and lets each VM go the moment
it has nothing left to take, so the next batch, the treadmill's or anyone's, leases it without a start. While it has
work running or queued the treadmill declares it (boat.demand), so a released VM stays up for its next batch; with
nothing queued for --quiet seconds it declares the end and its VMs stop a minute later (TOS-6: VMs kept for queued
work, never parked). No VM-hour or dollar cap: its chains stop when they stop gaining, and the pool's size is the
scheduler's (demand-driven).

Its directory ([treadmill] dir, else <main checkout>/runs/treadmill): cache.jsonl, subs/<name>/ (submissions and
their results), subs/.cache (each job's result by its tree and command), vmh.jsonl (each batch's VM-hours),
modal.jsonl (what went to Modal), digest/<day>.md, status.json, treadmill.log.
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import pathlib
import socket
import subprocess
import sys
import tarfile
import threading
import time

from . import agent, boat, config
from .rules import ops
from .rules import treadmill as rules
from .config import project

CHAIN_CLASS = {"a": "sweep", "b": "filler", "c": "submission"}  # the scheduler's class of each chain's batches
MODAL_MAX = rules.MODAL_MAX  # a batch boat refuses whole goes to Modal only when it is this small (a submission, a short round)
YIELD = 60  # the treadmill starts a VM only while more than this many free starts are left this hour (else warm ones)


def tm_dir(p: config.Project | None = None) -> pathlib.Path:
    p = p or project()
    d = os.environ.get("SUPERCOMPUTER_TREADMILL") or os.environ.get("GDPW_TREADMILL") or (p.cfg.get("treadmill", {}) or {}).get("dir")
    if d:
        return pathlib.Path(d) if os.path.isabs(d) else config.main_checkout(p.root) / d
    return config.main_checkout(p.root) / "runs" / "treadmill"


TM = None  # (set by use(): the project's directory)


def use(p: config.Project | None = None) -> pathlib.Path:
    global TM
    TM = tm_dir(p)
    return TM


def log(*a):
    line = f"[treadmill {time.strftime('%H:%M:%S')}] " + " ".join(map(str, a))
    print(line, file=sys.stderr, flush=True)
    with contextlib.suppress(Exception):
        with open(TM / "treadmill.log", "a") as f:
            f.write(line + "\n")


def jl(path: pathlib.Path) -> list[dict]:
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()] if path.exists() else []


def append(path: pathlib.Path, row: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(row) + "\n")


def append_many(path: pathlib.Path, rows: list):
    with open(path, "a") as f:
        f.write("".join(json.dumps(r) + "\n" for r in rows))


def git(*a, cwd=None) -> str:
    return subprocess.run(["git", "-C", str(cwd or config.main_checkout(project().root)), *a], capture_output=True, text=True, check=True).stdout.strip()


def today() -> str:
    return time.strftime("%Y-%m-%d", time.gmtime())


# ------------------------------------------------------------------------------------------------------- the cache
class Cache:
    """Every verdict, by (commit, benchmark, task, config); appended to cache.jsonl as it comes."""

    REFUSED = ("unsupported", "refused")

    def __init__(self):
        self.path = TM / "cache.jsonl"
        self.d: dict = {}
        self.rc = rules.ResultCache()  # (the decisions: a key seen never runs again; a task's first solve, by time)
        self.first: dict[tuple, str] = {}  # (commit, bench, task) -> the first config that solved it
        self.lock = threading.Lock()
        for r in sorted(jl(self.path), key=lambda r: r.get("t", 0)):
            self._note(r)

    def _note(self, row: dict):
        k = (row["commit"], row["bench"], row["task"], row["config"])
        self.d[k] = row
        if self.rc.put("\0".join(map(str, k)), "\0".join(map(str, k[:3])), bool(row.get("solved")), row.get("answer") in self.REFUSED):
            self.first[k[:3]] = row["config"]

    def put(self, row: dict):
        with self.lock:
            self._note(row)
            append(self.path, row)

    def solved(self, commit: str, bench: str) -> dict[str, str]:
        """The tasks some round of this commit solved, each with the first config that did."""
        return {t: cfg for (c, b, t), cfg in self.first.items() if c == commit and b == bench}

    def seen(self, commit: str, bench: str, config_: str) -> set[str]:
        return {t for (c, b, t, cfg) in self.d if c == commit and b == bench and cfg == config_ and not self.rc.run("\0".join(map(str, (c, b, t, cfg))))}

    def refused(self, commit: str, bench: str, refused=REFUSED) -> set[str]:
        if tuple(refused) == self.REFUSED:
            return {t for (c, b, t, cfg) in self.d if c == commit and b == bench and "\0".join(map(str, (c, b, t))) in self.rc.refused}
        return {t for (c, b, t, cfg), r in self.d.items() if c == commit and b == bench and r["answer"] in refused}


# ------------------------------------------------------------------------------------------------------ the batches
class Batch:
    """One Session's worth of items in a thread: `vms` asked for, the session's live VMs counted while it runs."""

    def __init__(self, tm, label: str, chain: str, items: list[dict], vms: int, on_row, root=None, tar=None, after=None, oleans=False):
        self.tm, self.label, self.chain, self.items, self.vms = tm, label, chain, items, vms
        self.on_row, self.root, self.tar, self.after, self.oleans = on_row, root, tar, after, oleans
        self.sess, self.done, self.failed, self.t0 = None, False, False, time.time()
        self.n_done = 0
        # (the slots of its items not yet back, each counted until its own row comes back: rules.treadmill.PendingSlots)
        slots = boat.TYPES[boat.VM_TYPE][0] // boat.SLOT_CPU
        self.pending = rules.PendingSlots()
        for it in items:
            self.pending.add(it["name"], slots if it.get("timing") else it.get("weight", 1))
        self.fate = rules.BatchFate()
        self.thread = threading.Thread(target=self.go, daemon=True)
        self.thread.start()

    def backlog_s(self) -> float:
        """Slot-seconds of its items not yet back (estimated by their own times)."""
        if self.done:
            return 0.0
        left = max(0, len(self.items) - self.n_done)
        slots = boat.TYPES[boat.VM_TYPE][0] // boat.SLOT_CPU  # (a timed item takes a whole VM)
        per = sum((slots if it.get("timing") else it.get("weight", 1)) * it.get("est", 60) for it in self.items) / max(1, len(self.items))
        return left * per

    def held(self) -> int:
        s = self.sess
        if s is None or getattr(s, "scheduled", False):  # (the scheduler holds the VMs: what it was given to fill)
            if self.done:
                return 0
            slots = boat.TYPES[boat.VM_TYPE][0] // boat.SLOT_CPU  # (a VM's worth of slots per VM: a one-item batch isn't a VM)
            need = self.pending.need  # (the items not yet back, whatever order their rows came in)
            return max(1, min(self.vms, -(-need // slots))) if need else 0
        return sum(1 for v in list(s.vms.values()) if not v.dead and not v.released and v.t_end is None) or (0 if self.done else 1)

    def unpacked(self) -> pathlib.Path:
        """A submission's tree as a directory (for Modal), with this checkout's Modal module (the tree as shipped may
        leave it out)."""
        d = TM / "unpacked" / tree_key(self.tar)
        if not (d / ".ready").exists():
            d.mkdir(parents=True, exist_ok=True)
            with tarfile.open(fileobj=io.BytesIO(self.tar)) as tf:
                tf.extractall(d, filter="tar")
            mod = project().modal.get("module")
            if mod:
                src = project().root / mod
                for f in src.parent.glob("*.py"):
                    (d / pathlib.Path(mod).parent).mkdir(parents=True, exist_ok=True)
                    (d / pathlib.Path(mod).parent / f.name).write_bytes(f.read_bytes())
            (d / ".ready").write_text("")
        return d

    def backlog(self) -> bool:
        """More queued than its VMs' slots: more VMs would help it."""
        s = self.sess
        return bool(s) and rules.backlog(bool(getattr(s, "scheduled", False)), self.done, bool(s.ready and getattr(s, "_enough", None)), bool(getattr(s, "_enough", None) and s.enough()))

    def grow(self, k: int):
        """k VMs more for it (warm ones first, else started)."""
        self.vms += k
        self.sess.cap = self.vms
        live = sum(1 for v in list(self.sess.vms.values()) if not v.dead and not v.released and v.t_end is None)
        threading.Thread(target=self.sess.ensure, args=(live + k,), daemon=True).start()

    def go(self):
        """The items on boat; those boat loses (a VM gone, no VM within --max-wait, boat failing) on Modal; then
        `after` with every item's logs."""
        summary, logs, lost, got = {}, {}, {}, set()
        boat_err = False

        def row(it, r):
            got.add(it["name"])
            self.n_done += 1
            self.pending.back(it["name"])
            self.on_row(it, r)
        try:
            if os.environ.get("GDPW_TREADMILL_NO_BOAT") or os.environ.get("SUPERCOMPUTER_TREADMILL_NO_BOAT"):  # (the fallback test)
                raise boat.Unavailable("boat off (SUPERCOMPUTER_TREADMILL_NO_BOAT)")
            # (through the scheduler, the treadmill is its lowest tenant: its sweeps class 4, its soundness fill class 5,
            # its queue's submissions class 3; a higher class takes their slots)
            sess = boat.session(f"treadmill {self.label}", klass=rules.chain_class(self.chain), build=False, oleans=self.oleans, root=self.root or self.tm.main, tar=self.tar)
            sess.cap = self.vms
            sess.min_left = YIELD  # (bars and lanes come first: the treadmill starts VMs only while the hour has plenty)
            self.sess = sess
            sess.ensure(self.vms)
            sess.run(self.items, row, on_lost=lambda it, why: lost.__setitem__(it["name"], why), max_wait=self.tm.max_wait)
            logs = sess.collect()
        except KeyboardInterrupt:  # (the treadmill stopping: its session stopped; the batch runs again later)
            log(f"{self.label}: stopped with the treadmill; its items run again later")
            boat_err = True
        except Exception as e:
            log(f"{self.label}: boat: {str(e)[:300]}")
            boat_err = True
        finally:
            if self.sess:
                summary = self.sess.close()
        todo = [it for it in self.items if it["name"] not in got]
        # (Modal only for items boat failed 3 times: the scheduler's lost rows; a batch boat's API failed whole runs
        # again on boat later, however small: rules.treadmill.BatchFate)
        route = self.fate.boat_ended(len(todo), boat_err, bool(self.tm.modal))
        if route == "again":
            # (boat refused the batch, gave it no VM within --max-wait, its image not built or its API down: a sweep isn't
            # urgent, so it waits for boat rather than send hundreds of items to Modal; a few items lost do go to Modal)
            log(f"{self.label}: boat can't run it now; again later ({len(todo)} items, past MODAL_MAX for Modal)")
            self.failed = True
            for it in todo:
                self.on_row(it, {"code": -1, "error": "boat couldn't run it now", "lost": True, "retry": True})
            todo = []
        if route == "failed":
            log(f"{self.label}: {len(todo)} items boat failed 3 times and --no-modal: recorded as failed ({'; '.join(sorted(set(lost.values()))[:2])[:200]})")
            self.failed = True
            for it in todo:
                self.on_row(it, {"code": -1, "error": lost.get(it["name"], "boat couldn't run it"), "lost": True})
        if route == "modal":  # (Modal only for what boat failed 3 times on distinct VMs: rules.fallback.lost_route; the rest again on boat)
            from .rules import fallback as _fb
            back = [it for it in todo if _fb.lost_route(False, boat.boat_fails_of(lost.get(it["name"])), 3, True) == "boat"]
            if back:
                log(f"{self.label}: {len(back)} items boat didn't run, not for 3 boat failures ({'; '.join(sorted({str(lost.get(it['name'], 'no row'))[:80] for it in back})[:2])}): again on boat later")
                self.failed = True
                for it in back:
                    self.on_row(it, {"code": -1, "error": "boat couldn't run it now", "lost": True, "retry": True})
            todo = [it for it in todo if it not in back]
            if not todo:
                route = "again"
        if route == "modal":
            why = sorted(set(lost.values()))[:3] or ["boat couldn't run them"]
            log(f"{self.label}: {len(todo)} items on Modal ({'; '.join(w[:120] for w in why)})")
            append(TM / "modal.jsonl", {"t": time.time(), "label": self.label, "items": len(todo), "why": why})
            try:
                logs.update(on_modal(todo, self.root or self.unpacked(), row))
            except Exception as e:
                log(f"{self.label}: Modal too: {str(e)[:300]}")
                self.fate.modal_failed()
                self.failed = True
                for it in [it for it in todo if it["name"] not in got]:
                    self.on_row(it, {"code": -1, "error": f"boat and Modal failed: {str(e)[:200]}", "lost": True})
        try:
            if self.after and self.fate.record():  # (a batch to run again later records nothing now)
                self.after(logs)
        finally:
            if summary:
                mult = boat.TYPES[boat.VM_TYPE][0] / boat.TYPES["default"][0]
                append(TM / "vmh.jsonl", {"t": time.time(), "day": today(), "label": self.label, "chain": self.chain, "vmh": round(summary["held_s"] * mult / 3600, 3),
                                         "busy_vmh": round(summary["busy_s"] * mult / 3600, 3), "ready_vmh": round(summary["ready_held_s"] * mult / 3600, 3),
                                         "starts": summary["starts"], "vms": summary["vms"], "items": len(self.items)})
            self.done = True


_boat_cfg: dict = {}


def py_item(name: str, module: str, fn: str, args: list, limit: int, est: float, root: pathlib.Path, weight: int = 1) -> dict:
    """A runner's function as an item (as fanout.BoatEx's), in the runner's boat setting (its mounts, PATH, overlays)."""
    if (root, module) not in _boat_cfg:
        _boat_cfg[(root, module)] = getattr(agent.import_runner(str(root / module)), "BOAT", {})
    cfg = _boat_cfg[(root, module)]
    return {"name": name, "kind": "py", "module": module, "fn": fn, "args": args, "limit": limit, "est": est,
            "weight": max(weight, int(cfg.get("weight", 1))), **{k: v for k, v in cfg.items() if k in ("mounts", "path", "overlays", "env")}}


def sh_item(name: str, cmd: str, cwd: str, limit: int, est: float, weight: int = 1) -> dict:
    return {"name": name, "kind": "sh", "cmd": cmd, "cwd": cwd, "limit": limit, "est": est, "weight": weight}


def import_real(path: pathlib.Path):
    """A module of a tree (a runner, the repo's Modal module) under the real Modal client: its app, functions and image."""
    if getattr(sys.modules.get("modal"), "__sc_stub__", False) or getattr(sys.modules.get("modal"), "__gdpw_stub__", False):
        del sys.modules["modal"]
    import modal  # noqa: F401
    name = "tm_" + hashlib.sha256(str(path).encode()).hexdigest()[:10]
    if name in sys.modules:
        return sys.modules[name]
    sys.path.insert(0, str(path.parent))
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_modal_lock = threading.Lock()


@contextlib.contextmanager
def modal_app(app):
    """A tree's Modal app running for one fallback (one at a time: the client is not for threads at once), its id and
    this client's pid in LIVE meanwhile (`supercomputer reap` stops it if this client dies)."""
    with _modal_lock, app.run():
        t = config.live_dir() / app.app_id
        t.write_text(str(os.getpid()))
        try:
            yield
        finally:
            t.unlink(missing_ok=True)


def on_modal(items: list[dict], root: pathlib.Path, on_row) -> dict:
    """Items boat couldn't run, on Modal: a runner's function through the runner's own app, shell items through the
    repo's Modal module (else the supercomputer's own app); each row to on_row, and the shell items' logs back."""
    logs: dict = {}
    for module in sorted({it["module"] for it in items if it["kind"] == "py"}):
        mod = import_real(root / module)
        group = [it for it in items if it["kind"] == "py" and it["module"] == module]
        with modal_app(mod.app):
            for fn in sorted({it["fn"] for it in group}):
                its = [it for it in group if it["fn"] == fn]
                for it, res in zip(its, list(getattr(mod, fn).starmap([tuple(it["args"]) for it in its], return_exceptions=True, order_outputs=True))):
                    on_row(it, {"code": -1, "error": repr(res)[:400]} if isinstance(res, BaseException) else {"code": 0, "result": res, "where": "modal"})
    sh = [it for it in items if it["kind"] == "sh"]
    if sh:
        mpath = project().modal.get("module")
        cands = [root / mpath] if mpath else []
        cands += [root / "modal" / "run.py"] if mpath and mpath.startswith("supercomputer/") else []  # (a tree from before gdp-wired's rename)
        found = next((c for c in cands if c.exists()), None)
        if found:
            run = import_real(found)
        else:
            from . import modal_run as run
        by = {it["name"]: it for it in sh}
        spec = [{"name": it["name"], "cmd": it["cmd"], "cwd": it.get("cwd", "."), "cpu": 2.0 * it.get("weight", 1), "memory": 4096 * it.get("weight", 1), "timeout": int(it["limit"]) + 30} for it in sh]

        def done(name, r):
            logs[name] = {"out": r.get("out", ""), "err": r.get("err", "")}
            on_row(by[name], {"code": r.get("code", -1), "seconds": r.get("seconds", -1), "where": "modal"})
        with modal_app(run.app):
            run.modal_jobs(spec, "", lambda c: c, done)
    return logs


# --------------------------------------------------------------------------------------------- the queue (submissions)
def tree_key(tar: bytes) -> str:
    return hashlib.sha256(tar).hexdigest()[:16]


def job_key(tk: str, j: dict) -> str:
    return hashlib.sha256((tk + json.dumps({k: j.get(k) for k in ("cmd", "cwd", "cpu", "memory", "timeout")}, sort_keys=True)).encode()).hexdigest()[:24]


def submit(jobs: str, tree: str, name: str, meta: dict | None = None):
    spec = json.loads(pathlib.Path(jobs).read_text())
    root = config.select_root(tree)
    name = name or f"{root.name}-{pathlib.Path(jobs).stem}-{time.strftime('%m%d-%H%M%S')}"
    d = TM / "subs" / name
    why = rules.submit_refusal(any(j.get("image") and j["image"] not in project().images for j in spec), any(j.get("backend") == "modal" for j in spec), any(j.get("gpu") for j in spec), d.exists())
    if why == "not_here":
        raise SystemExit("a job on Modal's own image or a GPU doesn't belong on the treadmill: `supercomputer run` it")
    if why == "exists":
        raise SystemExit(f"{d} exists: another --name")
    tar = boat.tree_tarball(root)
    d.mkdir(parents=True)
    (d / "tree.tar.gz").write_bytes(tar)
    (d / "jobs.json").write_text(json.dumps(spec, indent=1))
    (d / "meta.json").write_text(json.dumps({**(meta or {}), "name": name, "tree": str(root), "sha": git("rev-parse", "HEAD", cwd=root), "tree_key": tree_key(tar), "jobs": len(spec),
                                             "t": time.time(), "status": "queued"}, indent=1))
    print(f"queued {name}: {len(spec)} jobs from {root} (tree {tree_key(tar)}); results: supercomputer queue results {name}")


def results(name: str):
    d = TM / "subs" / name
    meta = json.loads((d / "meta.json").read_text())
    print(f"{name}: {meta['status']}, {meta['jobs']} jobs" + (f", done in {round(meta['wall_s'])} s" if meta.get("wall_s") else ""))
    if (d / "out" / "summary.tsv").exists():
        print((d / "out" / "summary.tsv").read_text(), end="")
        print(f"(logs: {d / 'out'})")


def ls():
    subs = sorted((json.loads(p.read_text()) for p in (TM / "subs").glob("*/meta.json")), key=lambda m: m["t"]) if (TM / "subs").exists() else []
    st = json.loads((TM / "status.json").read_text()) if (TM / "status.json").exists() else {}
    # (its pid checked on the host it ran on, this one's for a status from before hosts were recorded: a host of ""
    # would be presumed alive; rules.ops.ticket_alive)
    alive = bool(st) and boat.pid_alive(st.get("pid", 0), st.get("host") or socket.gethostname()) and time.time() - st.get("t", 0) < 120
    print(f"the treadmill ({TM}): {'running, pid ' + str(st['pid']) if alive else 'not running'}")
    for m in subs[-30:]:
        print(f"  {m['name']:48} {m['status']:8} {m['jobs']:4} jobs  {time.strftime('%m-%d %H:%M', time.localtime(m['t']))}")


class Submission:
    def __init__(self, tm, d: pathlib.Path):
        self.tm, self.d = tm, d
        self.meta = json.loads((d / "meta.json").read_text())
        self.batch: Batch | None = None
        # (rules.treadmill.SubmissionFate: one found "running" on disk is a dead treadmill's: queued again)
        self.fate = rules.SubmissionFate(self.meta["status"])
        self.meta["status"] = self.fate.status

    def start(self, vms: int) -> Batch | None:
        spec = json.loads((self.d / "jobs.json").read_text())
        tar = (self.d / "tree.tar.gz").read_bytes()
        cache = TM / "subs" / ".cache"
        cache.mkdir(parents=True, exist_ok=True)
        keys = {j["name"]: job_key(self.meta["tree_key"], j) for j in spec}
        rows = {n: json.loads((cache / f"{k}.json").read_text()) for n, k in keys.items() if (cache / f"{k}.json").exists()}
        todo = [j for j in spec if j["name"] not in rows]
        items, _ = boat.job_items(todo, "", lambda c: c, boat.VM_TYPE, {})
        made = self.fate.start(not todo)
        self.meta["status"], self.meta["started"] = "running", time.time()
        self.save()

        def on_row(it, row):
            rows[it["name"]] = {k: row.get(k) for k in ("code", "seconds", "error")} | {"where": row.get("where", "boat")}

        def after(logs):
            out = self.d / "out"
            out.mkdir(exist_ok=True)
            for j in spec:
                n, r = j["name"], rows.get(j["name"], {"code": -1, "seconds": -1, "error": "never ran"})
                lg = logs.get(n, {})
                for k in range(1, 3):
                    lg = logs.get(f"{n}~r{k}", lg)
                if "out" in lg or "err" in lg:
                    (out / f"{n}.log").write_text(lg.get("out", "") + ("\n--- stderr ---\n" + lg["err"] + (f"\n{r['error']}" if r.get("error") else "") if lg.get("err", "").strip() else ""))
                    if rules.cacheable(True, int(r.get("code") if isinstance(r.get("code"), int) else -1), bool(r.get("error"))):
                        (cache / f"{keys[n]}.json").write_text(json.dumps({**r, "log": str(out / f"{n}.log")}))
            (out / "summary.tsv").write_text("".join(f"{j['name']}\t{rows.get(j['name'], {}).get('code', -1)}\t{rows.get(j['name'], {}).get('seconds', -1)}\t{rows.get(j['name'], {}).get('where', '-')}\n" for j in spec))
            self.fate.finished()
            self.meta["status"], self.meta["wall_s"] = "done", time.time() - self.meta["started"]
            self.save()
        if not made:
            if self.fate.status == "done":  # (every job's row cached)
                after({})
            return None
        self.batch = Batch(self.tm, f"sub {self.meta['name']}", "c", items, vms, on_row, tar=tar, after=after, oleans=True)
        return self.batch

    def save(self):
        (self.d / "meta.json").write_text(json.dumps(self.meta, indent=1))


# ---------------------------------------------------------------------------------------------- the controller
class Treadmill:
    """The loop: every 15 s, launch what the chains and the queue want within the base pool (--vms) and the day's
    VM-hour budget; declare the demand while anything runs or is queued, and its end after --quiet seconds of none.

    A repo's chains override floor(free), main(free) and fill(free) (each launches batches into self.batches and
    returns the VMs left), poll() (called first) and status_extra(); the queue's submissions are this class's own:
    up to two VMs while any wait (after the floor), then whatever is left after main()."""

    CLIENT = "treadmill"

    def __init__(self, vms: int, budget_vmh: float | str, quiet: int, modal: bool = True, max_wait: int = 900, max_vms: int = 0, step_vms: int = 4,
                 backlog_hours: float = 2.0):
        TM.mkdir(parents=True, exist_ok=True)
        self.main = config.main_checkout(project().root)
        # `vms` is the floor (always busy, the filler keeping it so; sent to the scheduler as a hint); the pool above it
        # is the scheduler's, grown by queued work and shrunk after its hold (README, "Scheduling policy"). step_vms,
        # backlog_hours: kept for the CLI, unused. max_vms caps what the treadmill launches work for.
        e = lambda k, d: type(d)(os.environ.get(f"SUPERCOMPUTER_TREADMILL_{k}") or d)
        budget_vmh = os.environ.get("SUPERCOMPUTER_TREADMILL_BUDGET_VMH") or budget_vmh
        self.floor_vms, self.target = vms, vms
        self.max_vms = e("MAX_VMS", max_vms or 10 ** 9)  # (no ceiling unless set)
        self.step_vms, self.backlog_hours = e("STEP_VMS", step_vms), e("BACKLOG_HOURS", float(backlog_hours))
        self.step_every = e("STEP_EVERY", 300.0)  # seconds between growth steps while the backlog lasts
        self.subs_vms = e("SUBS_VMS", 2)  # VMs the queue's submissions get at once while any wait
        self.budget_auto = str(budget_vmh) == "auto"
        self.vms, self.budget_vmh, self.quiet = vms, (0.0 if self.budget_auto else float(budget_vmh)), quiet
        self.t_grow, self.t_budget = 0.0, 0.0
        self.held_now = 0
        self.pool_now = 0  # (the scheduler's VMs of the image, all classes)
        self.modal, self.max_wait = modal, max_wait
        self.cache = Cache()
        self.state = json.loads((TM / "state.json").read_text()) if (TM / "state.json").exists() else {}
        self.batches: list[Batch] = []
        self.subs: dict[str, Submission] = {}
        self.last_work = time.time()
        self.declared, self.declared_t = False, 0.0

    def save_state(self):
        (TM / "state.json").write_text(json.dumps(self.state, indent=1))

    def held(self) -> int:
        return sum(b.held() for b in self.batches if not b.done)

    def vmh_today(self) -> float:
        """Default-VM hours today: its batches' (holding their own VMs) and, through the scheduler, its VMs sampled."""
        return sum(r["vmh"] for r in jl(TM / "vmh.jsonl") if r["day"] == today()) + sum(r.get("vmh", 0) for r in jl(TM / "vms.jsonl") if r.get("day") == today())

    def budget(self) -> float:
        """The day's VM-hour budget: given, or the plan's prepaid default-VM hours left over the days to its period's end."""
        if not self.budget_auto:
            return self.budget_vmh
        if time.time() - self.t_budget > 3600 or not self.budget_vmh:
            with contextlib.suppress(Exception):
                import datetime
                pi = boat.limits()["planIncluded"]
                end = datetime.datetime.fromisoformat(pi["periodEnd"].replace("Z", "+00:00")).timestamp()
                self.budget_vmh = round(float(pi["vmHoursRemaining"]) / max(1.0, (end - time.time()) / 86400), 1)
                self.t_budget = time.time()
        return self.budget_vmh or 384.0

    def backlog_hours_on_floor(self) -> float:
        """Hours the sweeps' and the queue's work in hand would take on the floor's slots."""
        slots = self.floor_vms * (boat.TYPES[boat.VM_TYPE][0] // boat.SLOT_CPU)
        s = sum(b.backlog_s() for b in self.batches if b.chain in ("a", "c") and not b.done)
        s += 60.0 * sum(s_.meta.get("jobs", 0) for s_ in self.subs.values() if s_.meta.get("status") == "queued")
        return s / max(1, slots) / 3600

    def elastic(self):
        """The pool's size is the scheduler's call (demand-driven: the queued work against what the held VMs clear
        within its horizon; README, "Scheduling policy"), never a step a few minutes. The treadmill sends its floor
        (the hint) and launches work for as many VMs as the pool holds now, so its filler keeps each one busy."""
        self.target = self.floor_vms
        self.vms = rules.elastic(self.floor_vms, self.max_vms, self.pool_now)

    def want(self, chain: str) -> list[Batch]:
        return [b for b in self.batches if b.chain == chain and not b.done]

    # (a repo's chains)
    def poll(self):
        pass

    def floor(self, free: int) -> int:
        return free

    def main_chains(self, free: int) -> int:
        return free

    def fill(self, free: int) -> int:
        return free

    def grows(self) -> tuple[str, ...]:
        return ("c", "a")

    def status_extra(self) -> dict:
        return {}

    def step(self):
        self.batches = [b for b in self.batches if not b.done]
        self.poll()
        for s in self.subs.values():  # (a submission boat couldn't take: queued again)
            if s.batch and s.batch.done and s.batch.failed:
                s.fate.batch_ended(True)
                s.batch, s.meta["status"] = None, s.fate.status
                s.save()
        for d in sorted((TM / "subs").glob("*/meta.json")) if (TM / "subs").exists() else []:
            if d.parent.name not in self.subs and json.loads(d.read_text())["status"] in ("queued", "running"):
                self.subs[d.parent.name] = Submission(self, d.parent)
        from . import scheduler
        if scheduler.enabled():
            self.elastic()
        # (no VM-hour budget: "no limits, but be smart with our money"; chains stop when they stop gaining. The step's
        # VMs: rules.treadmill.StepPlan)
        plan = rules.StepPlan(self.vms, self.held(), self.subs_vms, sum(b.held() for b in self.want("c")))
        plan.hook(self.floor(plan.free))
        for s in [s for s in self.subs.values() if s.meta["status"] == "queued"]:  # the queue: up to two VMs while any wait
            k = plan.sub_first()
            if k <= 0:
                break
            b = s.start(k)
            if b:
                self.batches.append(b)
            else:
                plan.unused(k, True)
        plan.hook(self.main_chains(plan.free))
        for s in [s for s in self.subs.values() if s.meta["status"] == "queued"]:  # the queue in what's left
            k = plan.sub_rest()
            if k <= 0:
                break
            b = s.start(k)
            if b:
                self.batches.append(b)
            else:
                plan.unused(k, False)
        for b in sorted([b for b in self.batches if b.chain in self.grows() and b.backlog()], key=lambda b: b.chain):  # backlogs grow into what's left
            k = plan.grow()
            if k <= 0:
                break
            b.grow(k)
        plan.hook(self.fill(plan.free))
        busy = bool([b for b in self.batches if not b.done])
        if busy:
            self.last_work = time.time()
        if busy and scheduler.enabled() and scheduler.current():  # (its base pool, for its classes, while it has work)
            with contextlib.suppress(Exception):
                c = scheduler.current()
                c.req("/hint", {"client": self.CLIENT, "image": boat.image_name(), "base_vms": self.target, "ttl": 120}, timeout=10)
                st = c.req("/status", {}, timeout=10)
                mine = [v for v in st.get("vms", []) if v["image"] == boat.image_name() and set(v["classes"]) <= {"sweep", "filler", "submission"} and v["classes"]]
                self.held_now = len(mine)
                self.pool_now = sum(1 for v in st.get("vms", []) if v["image"] == boat.image_name())
                dt = time.time() - getattr(self, "t_sample", time.time())
                self.t_sample = time.time()
                mult = boat.TYPES[boat.VM_TYPE][0] / boat.TYPES["default"][0]
                append(TM / "vms.jsonl", {"t": round(time.time()), "day": today(), "vms": self.held_now, "target": self.target,
                                          "vmh": round(self.held_now * mult * min(dt, 120) / 3600, 4)})
        if busy or time.time() - self.last_work < self.quiet:
            if not self.declared or time.time() - self.declared_t > 60:
                boat.demand(self.CLIENT, 300, os.getpid(), "treadmill")
                self.declared, self.declared_t = True, time.time()
        elif self.declared:
            log(f"nothing queued for {self.quiet} s: the pool's VMs stop")
            boat.undeclare(self.CLIENT)
            self.declared = False
        (TM / "status.json").write_text(json.dumps({"t": time.time(), "pid": os.getpid(), "host": socket.gethostname(), "project": project().name, "vms": self.vms, "held": self.held(),
                                                    "vmh_today": round(self.vmh_today(), 2),
                                                    "batches": [{"label": b.label, "chain": b.chain, "held": b.held(), "items": len(b.items), "since": round(time.time() - b.t0)} for b in self.batches if not b.done],
                                                    "queued_subs": [n for n, s in self.subs.items() if s.meta["status"] == "queued"],
                                                    "elastic": {"floor": self.floor_vms, "target": self.target, "max": self.max_vms, "held": self.held_now,
                                                                "backlog_h": round(self.backlog_hours_on_floor(), 2)}, **self.status_extra()}, indent=1))

    def run(self, hours: float, digest_fn=None):
        os.environ["SUPERCOMPUTER_CLIENT"] = os.environ["GDPW_BOAT_CLIENT"] = self.CLIENT
        (TM / "treadmill.pid").write_text(str(os.getpid()))
        from . import scheduler
        with contextlib.suppress(Exception):  # (a treadmill before this one left batches no one waits for: closed)
            c = scheduler.current()
            if c:
                for b in c.req("/status", {}, timeout=30).get("batches", []):
                    if rules.closes_leftover(b.get("client", ""), self.CLIENT):
                        c.req("/close", {"batch": b["id"]}, timeout=30)
        t_end = time.time() + hours * 3600 if hours else None
        last_digest = 0.0
        digest_fn = digest_fn or digest
        log(f"running: a floor of {self.vms} {boat.VM_TYPE} VMs (the pool's size the scheduler's), quiet {self.quiet} s" + (f", for {hours} h" if hours else ""))
        try:
            with boat.signals_as_interrupt():
                while not t_end or time.time() < t_end:
                    try:
                        self.step()
                    except Exception as e:
                        log(f"step: {str(e)[:400]}")
                    if time.time() - last_digest > 3600:
                        last_digest = time.time()
                        with contextlib.suppress(Exception):
                            digest_fn(today(), quiet=True)
                    time.sleep(15)
        except KeyboardInterrupt:
            log("stopping: its batches let go (the scheduler keeps their items; the next treadmill closes them)")
            stopped = True
        else:
            stopped = False
        finally:
            for b in self.batches:
                if b.sess:
                    b.sess.stop.set()
            # (a stop ends within seconds: the batches' threads given 20 s in all, not 600 s each (05:00: SIGINT ignored
            # 90 s, SIGTERM 40 s, a SIGKILL in the end); the scheduler holds their items, and the next treadmill's start
            # closes what no one waits for (rules.closes_leftover))
            t_join = time.time() + 20
            for b in self.batches:
                b.thread.join(timeout=max(0.0, t_join - time.time()))
            boat.undeclare(self.CLIENT)
            if not stopped:  # (the day's digest at a normal end; a stop asked for doesn't wait on it)
                digest_fn(today(), quiet=True)
            (TM / "treadmill.pid").unlink(missing_ok=True)
            log("stopped")


# ------------------------------------------------------------------------------------------------- the digest
def pool_section(day: str, client: str = "") -> list[str]:
    """The digest's last part: the treadmill's batches, VM-hours, busy share, the starts (the ledger's) and the extra
    starts' dollars this month."""
    client = client or os.environ.get("SUPERCOMPUTER_TREADMILL_CLIENT", "treadmill")
    v = [r for r in jl(TM / "vmh.jsonl") if r["day"] == day]
    vmh, busy, ready = sum(r["vmh"] for r in v), sum(r["busy_vmh"] for r in v), sum(r["ready_vmh"] for r in v)
    led = [r for r in jl(boat.LEDGER) if r["utc"][:10] == day]
    ours = sum(r.get("starts", 0) for r in led if r.get("client") == client)
    allst = sum(r.get("starts", 0) for r in led)
    warm = sum(r.get("agents", 0) for r in led)
    samples = [r for r in jl(TM / "vms.jsonl") if r.get("day") == day]
    by_class: dict = {}
    for r in led:
        if r.get("klass"):
            by_class[r["klass"]] = by_class.get(r["klass"], 0.0) + float(r.get("slot_s", 0)) * boat.SLOT_CPU / boat.TYPES["default"][0] / 3600
    out = ["", "## The pool",
           f"- the treadmill's VMs: peak {max([r['vms'] for r in samples] or [0])} (target peak {max([r['target'] for r in samples] or [0])}); "
           f"(the pool's size is the scheduler's: see its section)",
           "- default-VM hours of item time by class (the scheduler's): " + (", ".join(f"{k} {v:.1f}" for k, v in sorted(by_class.items())) or "none"),
           vmh_dollars(vmh + sum(r.get("vmh", 0) for r in samples)),
           f"- treadmill batches: {len(v)}, {vmh:.1f} default-VM hours (busy {100 * busy / max(ready, 1e-9):.0f}% of the hours they held VMs ready), {sum(r['starts'] for r in v)} starts",
           f"- starts today (the ledger): {ours} the treadmill's, {allst} every client's, in {len(led)} batches ({warm} VMs reached by their warm agent)"]
    with contextlib.suppress(Exception):
        out.append(f"- {boat.extra_starts()}")
    return out


def scheduler_section(day: str) -> list[str]:
    """The scheduler's day (LIVE/scheduler/metrics/<day>.json): the pool's VM-hours and what they and the extra starts
    cost, the filler hours VMs above the floor ran in their holds, the gaps between waves of real work (to tune the
    hold), and per class the starts (free, paid), waits and preemptions. Information, no thresholds."""
    from . import scheduler
    p = scheduler.DIR / "metrics" / f"{day}.json"
    if not p.exists():
        return []
    m = json.loads(p.read_text())
    price = 0.036
    with contextlib.suppress(Exception):
        price = float((boat.limits().get("overagePrices") or {}).get("vmHourDollars", price))
    starts = sum(v["free"] + v["paid"] for v in m["starts"].values())
    paid = sum(v["paid"] for v in m["starts"].values())
    items = sum(m["placed"].values())
    gaps = sorted(m.get("gaps", []))
    hold = scheduler.HOLD
    g = (lambda q: f"{gaps[min(len(gaps) - 1, int(q * len(gaps)))] / 60:.0f} min") if gaps else (lambda q: "-")
    vg = sorted(m.get("vm_gaps", []))
    gq = lambda xs, q: f"{xs[min(len(xs) - 1, int(q * len(xs)))] / 60:.0f} min" if xs else "-"
    out = ["", "## The scheduler",
           f"- VM-hours: {m['vmh']:.1f} default-VM hours held (peak {m['peak_vms']} VMs" + (f"; growth held at the {scheduler.MAX_VMS}-VM ceiling {m.get('max_binds', 0)} times" if scheduler.MAX_VMS < 10 ** 9 else "; no VM ceiling") + "); "
           f"${m['vmh'] * price:.2f} at the overage price (${price}/default-VM hour; nothing billed while the plan's prepaid hours last)",
           "- VM-hours by class (= the treadmill's chains: sweep, filler, submission) at the overage price: "
           + (", ".join(f"{k} {v:.1f} (${v * price:.2f})" for k, v in sorted(m.get("vmh_by_class", {}).items(), key=lambda x: -x[1])) or "none"),
           f"- filler during holds: {m['vmh_filler_hold']:.1f} default-VM hours on VMs above the floor that ran only filler ({m['hold_ends']} released at the end of a "
           f"{hold / 60:.0f}-minute hold)",
           f"- waves of real work: {m['waves']}; gaps between them: {len(gaps)}, p50 {g(0.5)}, p90 {g(0.9)}, {sum(1 for x in gaps if x <= hold)} within the hold "
           f"(a VM kept saved its start), {sum(1 for x in gaps if x > hold)} past it",
           f"- a VM's gaps between real work (the hold's test, per VM): {len(vg)}, p50 {gq(vg, 0.5)}, p90 {gq(vg, 0.9)}, max {gq(vg, 1.0)}; "
           f"{sum(1 for x in vg if x <= hold)} within the {hold / 60:.0f}-minute hold (a start saved), {m['hold_ends']} VMs released at its end",
           f"- starts: {starts} ({starts - paid} free, {paid} paid) for {items} items placed: {100 * starts / max(1, items):.1f} per 100 items; "
           f"{m['placed_warm']} items placed on a VM already up; {m['not_ready']} starts never ready; circuit breaker tripped {m['breaker_trips']} times",
           f"- boat's failures of items (a VM lost, a row missing, no VM ready): {sum(m.get('boat_fails', {}).values())}; items boat failed "
           f"{scheduler.BOAT_TRIES} times, so sent back for Modal: {sum(m.get('to_modal', {}).values())}",
           "- by class: " + "; ".join(f"{n} {m['placed'].get(n, 0)} items, starts {m['starts'][n]['free']}+{m['starts'][n]['paid']} paid, "
                                      f"wait p50 {scheduler.pct(m['waits'].get(n, []), 0.5)} s p95 {scheduler.pct(m['waits'].get(n, []), 0.95)} s, "
                                      f"{m['preempted'].get(n, 0)} preempted" for n in scheduler.CLASSES if m["placed"].get(n) or m["preempted"].get(n))]
    for c in m.get("constraints", []):
        out.append(f"- CPU constraint {c['image']}/{c['vm_type']} / {c['cpu_model']}: "
                   f"{len(c['matching_vms'])} matching VMs, {c['waiting_items']} waiting items, "
                   f"{len(c['draws'])} starts drawn; models {json.dumps(c['models'], sort_keys=True)}; "
                   f"fruitless {c['fruitless']}/{c['limit']}")
    with contextlib.suppress(Exception):
        out.append(f"- {boat.extra_starts()}")
    return out


def vmh_dollars(vmh: float) -> str:
    """The day's treadmill VM-hours against the plan's prepaid pace (its hours left over the days to the period's end),
    and what the hours past that pace cost at the overage price once the prepaid hours are spent."""
    with contextlib.suppress(Exception):
        import datetime
        lim = boat.limits()
        pi = lim["planIncluded"]
        end = datetime.datetime.fromisoformat(pi["periodEnd"].replace("Z", "+00:00")).timestamp()
        pace = float(pi["vmHoursRemaining"]) / max(1.0, (end - time.time()) / 86400)
        price = float((lim.get("overagePrices") or {}).get("vmHourDollars", 0.036))
        over = max(0.0, vmh - pace)
        return (f"- dollars: {vmh:.0f} default-VM hours today against the prepaid pace of {pace:.0f} a day ({pi['vmHoursRemaining']:.0f} left to "
                f"{pi['periodEnd'][:10]}); {over:.0f} past it, ${over * price:.2f} at ${price}/VM-hour once the prepaid hours are spent (none billed while they last)")
    return f"- dollars: {vmh:.0f} default-VM hours today"


def subs_section(day: str) -> list[str]:
    subs = [json.loads(p.read_text()) for p in (TM / "subs").glob("*/meta.json")] if (TM / "subs").exists() else []
    subs = [s for s in subs if time.strftime("%Y-%m-%d", time.gmtime(s["t"])) == day]
    return ["", "## Submissions", *(f"- {s['name']}: {s['status']}, {s['jobs']} jobs" for s in subs)] if subs else []


def digest(day: str, quiet: bool = False, sections=None) -> str:
    """The day's digest: the repo's own sections (its chains' solves, regressions, soundness work), the queue, and
    the pool (VM-hours, busy share, starts and extra-start dollars)."""
    out = [f"# Treadmill digest, {day}", ""]
    if sections:
        out += sections(day)
    out += subs_section(day) + pool_section(day) + scheduler_section(day)
    text = "\n".join(out) + "\n"
    (TM / "digest").mkdir(parents=True, exist_ok=True)
    (TM / "digest" / f"{day}.md").write_text(text)
    if not quiet:
        print(text)
    return text


def repo_module():
    """The repo's treadmill module ([treadmill] module in its supercomputer.toml), or None."""
    m = (project().cfg.get("treadmill", {}) or {}).get("module")
    if not m:
        return None
    path = project().root / m
    if not path.exists():
        path = (project().cfg_path.parent if project().cfg_path else project().root) / m
    spec = importlib.util.spec_from_file_location("sc_repo_treadmill", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["sc_repo_treadmill"] = mod
    spec.loader.exec_module(mod)
    return mod
