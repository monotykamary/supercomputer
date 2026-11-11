"""The simulated world around the real scheduler: boat (its CLI, its VMs' lifecycle, TTLs, its front's 502s, its own
stops, slow and refused starts), the VMs' agents (each VM's ~/.sc held in memory, every decision taken by the agent's
own rule classes from the code under test: JobReport, BlobStore.drop_tree, BatchDirs, Ticks, Kill, Access), the
laptop's processes (scheduler processes, handoffs, a kill -9, a sleep) and the clients (boat.Scheduled.run itself,
polling through a fake transport). Nothing here talks to boat, Modal or the network: `boat.boat` (the CLI),
`AgentClient._req` (the HTTP) and every subprocess of the modules under test are replaced, and BOAT_BIN points nowhere.

The world keeps the ground truth the targets are checked against (metrics.py): every VM's states and who owned it,
every start and stop and why, every item's runs and whether boat failed them.
"""
import base64
import hashlib
import importlib
import importlib.util
import inspect
import io
import json
import os
import pathlib
import random
import re
import sys
import tarfile
import types

from . import kernel as K

RUNNING = "running"


# --------------------------------------------------------------------------------------------- loading the code
def load_code(code_dir: str, live: str, name: str = "supercomputer"):
    """The package under test from code_dir (a checkout's root), imported as `name` with LIVE in a temporary dir."""
    os.environ["SUPERCOMPUTER_LIVE"] = live
    os.environ["BOAT_BIN"] = "/nonexistent/boat-simulated"  # (a call that slipped past the fakes reaches no boat)
    os.environ["SUPERCOMPUTER_WATCHDOG"] = "0"
    for k in [k for k in os.environ if k.startswith(("SUPERCOMPUTER_CONTROLLER", "GDPW_BOAT_URGENT", "SUPERCOMPUTER_URGENT"))]:
        os.environ.pop(k)
    pkg = pathlib.Path(code_dir) / "supercomputer"
    spec = importlib.util.spec_from_file_location(name, pkg / "__init__.py", submodule_search_locations=[str(pkg)])
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    sched = importlib.import_module(f"{name}.scheduler")
    boat = importlib.import_module(f"{name}.boat")
    agent_src = (pkg / "agent.py").read_bytes()
    agent_spec = importlib.util.spec_from_file_location(f"{name}_agent_rules", pkg / "agent.py")
    agent = importlib.util.module_from_spec(agent_spec)
    agent_spec.loader.exec_module(agent)
    return types.SimpleNamespace(sched=sched, boat=boat, agent=agent, agent_src=agent_src, pkg=pkg, name=name)


class OsProxy:
    """The `os` a module under test sees: the real one but for its process (getpid, kill, _exit: the simulated ones)."""

    def __init__(self, world):
        self._w = world

    def getpid(self):
        return self._w.k.current_proc().pid

    def getppid(self):
        return 1

    def kill(self, pid, sig):
        p = self._w.k.procs.get(int(pid))
        if p is None or not p.alive:
            raise ProcessLookupError(pid)
        if sig not in (0,):
            self._w.k.kill_proc(p)

    def _exit(self, code):
        self._w.note("exit", proc=self._w.k.current_proc().name, code=code)
        self._w.k.kill_proc(self._w.k.current_proc())

    def getloadavg(self):
        return (0.5, 0.5, 0.5)

    def __getattr__(self, name):
        return getattr(os, name)


class SubprocessProxy:
    """The `subprocess` the scheduler sees: `import supercomputer.scheduler` (a handoff's check) and the next
    scheduler's spawn are simulated; anything else is refused (a test must never run a real command)."""

    def __init__(self, world):
        self._w = world
        import subprocess as sp
        self.DEVNULL, self.PIPE, self.STDOUT, self.CalledProcessError, self.TimeoutExpired = sp.DEVNULL, sp.PIPE, sp.STDOUT, sp.CalledProcessError, sp.TimeoutExpired
        self.CompletedProcess = sp.CompletedProcess

    def run(self, argv, *a, **k):
        if "-c" in argv and any("import" in str(x) and "scheduler" in str(x) for x in argv):
            ok = not self._w.broken_install
            return self.CompletedProcess(argv, 0 if ok else 1, "", "" if ok else "SyntaxError: the simulated broken install")
        raise AssertionError(f"a real subprocess from the code under test: {argv!r}"[:300])

    def Popen(self, argv, *a, **k):
        if "scheduler" in argv and "serve" in argv:
            pp = str((k.get("env") or {}).get("PYTHONPATH", ""))
            label = pp.rsplit("/code/", 1)[1].split("/")[0] if "/code/" in pp else self._w.code_installed
            delay = self._w.cfg.get("scheduler_start_s_by_code", {}).get(label, self._w.cfg.get("scheduler_start_s", 1.0))
            self._w.note("scheduler_spawn", code=label, delay=delay)
            proc = self._w.spawn_scheduler(takeover="--takeover" in argv, after=delay, code=self._w.codes.get(label), label=label)

            class P:
                pid = proc.pid

                def poll(self):
                    return None if proc.alive else 1

                def wait(self, timeout=5):
                    return self.poll()
            return P()
        raise AssertionError(f"a real subprocess from the code under test: {argv!r}"[:300])

    def check_output(self, argv, *a, **k):
        raise AssertionError(f"a real subprocess from the code under test: {argv!r}"[:300])


# ------------------------------------------------------------------------------------------------ a VM's disk
class Job:
    def __init__(self, spec, t, cpus):
        self.spec, self.t, self.mtime, self.cpus = spec, t, t, cpus
        self.pid = None
        self.done = None  # its done.json
        self.acked = False
        self.out = ""  # its out file


class BDir:
    def __init__(self, t, tid, ctx):
        self.mtime, self.tid, self.ctx = t, tid, ctx
        self.ready = False
        self.jobs: dict[str, Job] = {}


class Disk:
    """~/.sc of a VM: kept across a stop and a resume (boat snapshots the disk), its processes not."""

    def __init__(self):
        self.trees: dict[str, float] = {}  # tid -> mtime
        self.blobs: set[str] = set()
        self.batches: dict[str, BDir] = {}
        self.version = ""
        self.token = ""
        self.keys = {}
        self.lease = None
        self.lease_state = "released"
        self.lease_renewed = 0.0
        self.stopping_at = 0.0
        self.stop_nonce = ""
        self.birth = {}
        self.free_mb, self.total_mb = 100_000, 150_000  # (its disk: an agent since 11-02 reports it with each vtick)
        self.scratch: list = []  # [(mtime, mb)]: a job's leftovers in the home (anvil's state), its agent's to clean
        self.partial_first = False  # (the first tree verified here lacks files: a tree left short, its ready mark kept)
        self.partial: set = set()


class VMRec:
    def __init__(self, id, image, vm_type, cpus, t):
        self.id, self.image, self.type, self.cpus = id, image, vm_type, cpus
        self.model = "sim-cpu"
        self.state = "starting"
        self.until = t + 900
        self.t_created = t
        self.t_up = None  # (running since)
        self.t_daemon = None  # (its agent first answered)
        self.disk = Disk()
        self.daemon = "down"  # down | up | hung
        self.boot = 0
        self.procs: dict[int, dict] = {}  # fake pid -> {job, batch, name, alive}
        self.never_ready = False
        self.cv = None  # (a Condition: a job finished)
        self.stopped_by = ""
        self.ups: list = []  # [(t_up, t_down)]
        self.files: dict = {}  # (files written by boat exec: a request's body in parts)


# ------------------------------------------------------------------------------------------------------- the world
class World:
    def __init__(self, k: K.Kernel, code, live: str, seed: int = 1, cfg: dict | None = None):
        self.k, self.code, self.live = k, code, pathlib.Path(live)
        self.rng = random.Random(seed)
        self.cfg = {"boot_s": (60, 120), "resume_s": (30, 60), "api_s": 1.0, "exec_s": 1.5, "rpc_s": 0.15, "cpus": 8, "minute": 60, "hour": 2000, "day": 20000,
                    "refuse_p": 0.0, "never_ready_p": 0.0, **(cfg or {})}
        self.vms: dict[str, VMRec] = {}
        self.events: list[dict] = []
        self.front_down: list[tuple] = []  # (t0, t1, also_exec)
        self.api_down: list[tuple] = []  # (t0, t1)
        self.broken_install = False
        self.code_installed = getattr(code, "label", "c0")  # (scheduler.code_hash: what's installed)
        self.sched_procs: list = []  # [(proc, Scheduler | None)]
        self.pending_spawn = 0
        self.starts_used = 0
        self.items: dict[tuple, dict] = {}  # (batch, name) -> ground truth
        self.batches: dict[str, dict] = {}
        self.log_lines: list[tuple] = []
        self.status_ms: list[float] = []
        self.client_errors: list[str] = []
        self.next_id = 0
        code.label = getattr(code, "label", "c0")
        self.codes = {code.label: code}  # (each code loaded, by the hash the sim gives it)
        self.T = k.shims if False else None
        self.sh = K.shims(k)
        self.th, self.tm = self.sh["threading"], self.sh["time"]
        self.ticker = None
        self.patch(code)

    def share_pool(self, other):
        """Share the provider, never local registries, agent credentials or scheduler locks."""
        self.pool = getattr(other, "pool", other)
        for field in ("vms", "events", "items", "batches", "front_down", "api_down", "client_errors", "log_lines"):
            setattr(self, field, getattr(self.pool, field))

    # -- bookkeeping
    def note(self, kind, **kw):
        self.events.append({"t": round(self.k.now - self.k.t0, 2), "kind": kind, **kw})

    def now(self):
        return self.k.now

    # -- the code under test, its seams replaced
    def patch(self, code):
        S, B = code.sched, code.boat
        osp, subp = OsProxy(self), SubprocessProxy(self)
        for m in (S, B):
            m.time, m.threading, m.os = self.sh["time"], self.sh["threading"], osp
        S.subprocess = subp
        S.concurrent = self.sh["concurrent"]
        for name in ("_sem", "_tick_sem"):
            if hasattr(B, name):
                setattr(B, name, self.th.BoundedSemaphore(48 if name == "_sem" else 200))
        if hasattr(B, "_states_lock"):
            B._states_lock = self.th.Lock()
        B.boat = lambda *a, **kw: self.cli(list(a), **kw)
        B.AgentClient._get = lambda cl, timeout: FakeConn(self, cl, timeout)  # (the HTTP under AgentClient._req: its retries and fallbacks real)
        B.ledger = lambda acct=True, **row: None
        B.client = lambda: self.k.current_proc().name
        if hasattr(B, "Registry"):
            reg_lock = self.th.RLock()
            orig_locked = B.Registry.locked

            import contextlib

            @contextlib.contextmanager
            def locked(reg, _orig=orig_locked):
                with reg_lock:
                    d = reg.read()
                    yield d
                    tmp = reg.path.with_suffix(".tmp")
                    tmp.write_text(json.dumps(d))
                    tmp.rename(reg.path)
            B.Registry.locked = locked
        def prepare(pkg):
            self.note("snapshot_prepared", code=pkg.parent.name)
            return {"python": "sim", "cache_tag": "sim", "cpu_model_exact": True, "distinct_vm_group": True, "distinct_vm_group_protocol": 1,
                    "durable_distinct_vm_group": True, "result_files_protocol": True}
        S.prepare_snapshot = prepare
        if hasattr(S, "memorypressure"):
            # Host cgroups are not virtual controller state; sensor behavior has dedicated tests.
            S.memorypressure.Sampler = lambda: types.SimpleNamespace(sample=lambda: {"known": False})
        S.code_hash = lambda pkg=None: self.code_installed
        S.own_code = lambda: self.k.current_proc().env.get("code", code.label)
        S.log = lambda *a: self.sched_log(" ".join(str(x) for x in a))
        S.connect = lambda: self.ensure_running()
        S.ensure_running = lambda: self.ensure_running()
        S.current = lambda: SimSchedClient(self) if self.live_sched() else None
        S.running = lambda: self.live_sched() is not None
        if hasattr(S, "controller"):
            S.controller = lambda: None
        B.log = lambda *a: self.sched_log("[boat] " + " ".join(str(x) for x in a))

    def sched_log(self, msg: str):
        self.log_lines.append((round(self.k.now - self.k.t0, 2), self.k.current_proc().name, msg))

    # -- boat's CLI
    def _down(self, windows) -> bool:
        return any(a <= self.k.now < b for a, b, *_ in windows)

    def cli(self, args: list, timeout=120, check=True, stdin=None, sem=None) -> str:
        cmd = args[0]
        lat = self.cfg["exec_s"] if cmd == "exec" else self.cfg["api_s"]
        self.k.sleep(lat * (0.7 + 0.6 * self.rng.random()))
        if cmd != "exec" and self._down(self.api_down):
            if check:
                raise RuntimeError(f"boat {cmd} failed (1): 502 Bad Gateway")
            return "error: 502 Bad Gateway"
        out = getattr(self, f"cli_{cmd}", self.cli_other)(args)
        if isinstance(out, tuple):  # (an error)
            if check:
                raise RuntimeError(f"boat {cmd} failed (1): {out[0]}")
            return out[0]
        return out

    def cli_other(self, args):
        return "{}"

    def cli_list(self, args):
        """boat list: a page of at most `page` sandboxes (the oldest first), pageInfo saying whether there's more;
        `--filter G`: that state group's alone."""
        groups = {"r": (RUNNING,), "s": ("stopped",), "p": ("starting",), "t": (), "e": ("error",)}
        vs = [v for v in self.vms.values() if v.state != "deleted"]
        if "--filter" in args:
            want = {st for g in args[args.index("--filter") + 1] for st in groups.get(g, ())}
            vs = [v for v in vs if v.state in want]
        elif "--all" not in args:
            vs = [v for v in vs if v.state == RUNNING]
        page = int(self.cfg.get("list_page", 10 ** 9))
        vs.sort(key=lambda v: (v.state == RUNNING, v.t_created))
        return json.dumps({"sandboxes": [{"id": v.id, "name": getattr(v, "pool_name", v.image), "state": v.state, "type": v.type, "snapshotAvailable": True} for v in vs[:page]],
                           "pageInfo": {"hasMore": len(vs) > page, "limit": page, "nextCursor": "c" if len(vs) > page else None}})

    def cli_snapshots(self, args):
        return json.dumps({"named": [{"name": i, "status": "ready"} for i in {v.image for v in self.vms.values()}]})

    def limits(self):
        now = self.k.now
        used = lambda w: sum(1 for e in self.events if e["kind"] == "start" and now - (self.k.t0 + e["t"]) < w)
        st = {k: {"limit": self.cfg[k], "used": used(w), "remaining": max(0, self.cfg[k] - used(w))} for k, w in (("minute", 60), ("hour", 3600), ("day", 86400))}
        return {"starts": st, "currentLimits": {"activeSandboxes": 5000}, "activeSandboxes": sum(1 for v in self.vms.values() if v.state in ("running", "starting")),
                "keepGoing": {"enabled": True, "capReached": False}, "overagePrices": {"startLimitMultiplier": 3, "startMinuteLimitMultiplier": 1},
                "startLimits": {"perDay": self.cfg["day"], "perHour": self.cfg["hour"], "perMinute": self.cfg["minute"]},
                "startLimitsWithWallet": {"perDay": 3 * self.cfg["day"], "perHour": 3 * self.cfg["hour"], "perMinute": self.cfg["minute"]}}

    def cli_limits(self, args):
        return json.dumps(self.limits())

    def _new_id(self):
        if hasattr(self, "pool"):
            return self.pool._new_id()
        self.next_id += 1
        return "bx_" + hashlib.sha256(f"{self.next_id}".encode()).hexdigest()[:8]

    def cli_new(self, args):
        image = args[args.index("--from") + 1]
        name = args[args.index("--name") + 1] if "--name" in args else image
        vm_type = args[args.index("--type") + 1] if "--type" in args else "large"
        n = int(args[args.index("--count") + 1]) if "--count" in args else 1
        ttl = int(args[args.index("--ttl") + 1]) if "--ttl" in args else 900
        lines = []
        for _ in range(n):
            if self.rng.random() < self.cfg["refuse_p"] or self._down([w for w in self.cfg.get("refuse_windows", [])]):
                self.note("start_refused", image=image)
                lines.append(json.dumps({"event": "error", "error": "rate_limited"}))
                continue
            v = self.add_vm(image, vm_type, ttl)
            v.pool_name = name
            if "--file" in args:
                target, source = args[args.index("--file") + 1].split("=", 1)
                if target.endswith("/birth.json"):
                    v.disk.birth = json.loads(pathlib.Path(source).read_text())
            self.note("start", vm=v.id, image=image, how="created", by=self.k.current_proc().name)
            lines.append(json.dumps({"event": "created", "id": v.id}))
        return "\n".join(lines)

    def add_vm(self, image, vm_type="large", ttl=900, up_now=False, never_ready=None) -> VMRec:
        v = VMRec(self._new_id(), image, vm_type, {"small": 2, "default": 4, "large": 8, "xlarge": 16}.get(vm_type, 8), self.k.now)
        v.until = self.k.now + ttl
        v.cv = self.th.Condition()
        v.never_ready = (self.rng.random() < self.cfg["never_ready_p"] or self.k.now - self.k.t0 < self.cfg.get("never_ready_until", 0)) if never_ready is None else never_ready
        self.vms[v.id] = v
        if up_now:
            self._up(v)
        else:
            self.k.start(self.k.world, self._boot, (v, self.rng.uniform(*self.cfg["boot_s"])), name=f"boot {v.id}")
        return v

    def _boot(self, v: VMRec, s: float):
        self.k.sleep(s)
        if v.state == "starting" and not v.never_ready:
            self._up(v)
        elif v.never_ready:
            self.note("never_ready", vm=v.id, image=v.image)

    def _up(self, v: VMRec):
        v.state, v.t_up, v.boot = RUNNING, self.k.now, v.boot + 1
        v.ups.append([self.k.now, None])
        self.note("up", vm=v.id)

    def cli_resume(self, args):
        v = self.vms.get(args[1])
        if v is None or v.state == "deleted":
            return ("not found",)
        if self.rng.random() < self.cfg["refuse_p"]:
            self.note("start_refused", image=v.image)
            return json.dumps({"event": "error", "error": "rate_limited"})
        if v.state == "stopped":
            v.never_ready = self.rng.random() < self.cfg["never_ready_p"] or self.k.now - self.k.t0 < self.cfg.get("never_ready_until", 0)
            v.state = "starting"
            v.until = self.k.now + 900
            self.note("start", vm=v.id, image=v.image, how="resumed", by=self.k.current_proc().name)
            self.k.start(self.k.world, self._boot, (v, self.rng.uniform(*self.cfg["resume_s"])), name=f"resume {v.id}")
        return json.dumps({"id": v.id, "state": v.state})

    def stop(self, v: VMRec, why: str):
        if v.state in ("stopped", "deleted"):
            return
        served = v.state == RUNNING and v.daemon != "down"
        v.state, v.stopped_by, v.daemon = "stopped", why, "down"
        if v.ups and v.ups[-1][1] is None:
            v.ups[-1][1] = self.k.now
        dead = [p for p in v.procs.values() if p["alive"]]
        for p in dead:
            p["alive"] = False
            if why == "boat":  # (boat stopped it under the item: boat's failure of the item)
                it = self.items.get((p["batch"], p["name"]))
                if it is not None:
                    it["boat_vms"].add(v.id)
        if why == "boat":
            for proc, s in self.sched_procs:  # (placed there and not started yet: boat's failure too)
                vm = getattr(s, "vms", {}).get(v.id) if s is not None and proc.alive else None
                for x in list(getattr(vm, "running", {}).values()) if vm else []:
                    it = self.items.get((x.batch.id, x.name))
                    if it is not None:
                        it["boat_vms"].add(v.id)
        self.note("stop", vm=v.id, image=v.image, why=why, by=self.k.current_proc().name, killed=len(dead), served=served)
        with v.cv:
            v.cv.notify_all()

    def audit_stop(self, v):
        if hasattr(self.code.agent, "VMLease"):
            r = Agent(self, v).lease_rule()
            if self.code.boat.owner_id() != r.holder or self.vms[v.id].disk.lease_state != "stopping":
                self.note("unsafe_stop", vm=v.id, by=self.code.boat.owner_id(), holder=r.holder)

    def cli_stop(self, args):
        v = self.vms.get(args[1])
        if v is None:
            return ("not found",)
        self.audit_stop(v)
        self.stop(v, "us")
        return json.dumps({"id": v.id, "state": "stopped"})

    def cli_delete(self, args):
        v = self.vms.get(args[1])
        if v is not None:
            self.audit_stop(v)
            self.stop(v, "us")
            v.state = "deleted"
        return "{}"

    def cli_extend(self, args):
        v = self.vms.get(args[1])
        if v is None or v.state not in (RUNNING, "starting"):
            return ("sandbox is stopped",)
        ttl = int(args[args.index("--ttl") + 1])
        if ttl <= 120 and v.until - self.k.now > 120:
            self.note("release", vm=v.id, image=v.image, by=self.k.current_proc().name)
        v.until = self.k.now + ttl
        return json.dumps({"id": v.id, "until": v.until})

    def cli_rename(self, args):
        self.vms[args[1]].pool_name = args[2]
        return "{}"

    def cli_host(self, args):
        return json.dumps({"url": f"https://{args[1]}.sim.boat/"})

    def cli_exec(self, args):
        v = self.vms.get(args[1])
        argv = args[args.index("--") + 1:]
        if v is None or v.state == "deleted":
            return json.dumps({"exitCode": -1, "stdout": "", "stderr": f"sandbox {args[1]} not found", "success": False})
        if v.state != RUNNING:
            return json.dumps({"exitCode": -1, "stdout": "", "stderr": f"sandbox {v.id} is {'stopped' if v.state == 'stopped' else 'not running'}", "success": False})
        if any(a <= self.k.now < b and also for a, b, also in self.front_down):
            return json.dumps({"exitCode": -1, "stdout": "", "stderr": "502 Bad Gateway", "success": False})
        if any(len(str(a)) > 131072 for a in argv):  # (the kernel's limit on one argument string)
            return json.dumps({"exitCode": 126, "stdout": "", "stderr": "ascii-exec-spare: line 1: /usr/bin/bash: Argument list too long", "success": False})
        if argv == ["true"]:
            return json.dumps({"exitCode": 0, "stdout": "", "stderr": "", "success": True})
        if len(argv) >= 5 and argv[0] == "bash" and "printf %s" in argv[2]:  # (a part of a body, appended to a file)
            v.files[argv[-1]] = v.files.get(argv[-1], "") + argv[-2]
            return json.dumps({"exitCode": 0, "stdout": "", "stderr": "", "success": True})
        script = " ".join(argv)
        if len(argv) >= 4 and argv[0] == "python3" and argv[2] == "lease":
            payload = json.loads(base64.b64decode(argv[4])) if len(argv) > 4 else {}
            r = Agent(self, v).c_lease(argv[3], payload)
            return json.dumps({"exitCode": 0, "stdout": json.dumps(r), "stderr": "", "success": True})
        if len(argv) >= 6 and argv[0] == "python3" and argv[2:4] == ["keys", "add"]:
            v.disk.keys[argv[4]] = argv[5]
            return json.dumps({"exitCode": 0, "stdout": '{"ok":true}', "stderr": "", "success": True})
        if argv[:2] == ['python3', '-c'] and '# sc-agent-bootstrap:' in argv[2]:
            # Transport only: same full digest and agent rules as the real install.
            # Only source-upload file operations are modeled. No verdict,
            # virtual schedule, capacity or daemon rule is substituted here.
            op, root, name, owner = argv[3:7]
            payload = root + '/' + name + '/payload.b64'
            marker = root + '/' + name + '/owner'
            absent_cleanup = op == 'cleanup' and payload not in v.files and marker not in v.files
            if op != 'init' and not absent_cleanup and v.files.get(marker) != owner:
                return json.dumps({'exitCode': 1, 'stdout': '', 'stderr': 'bootstrap ownership mismatch', 'success': False})
            if op == 'init':
                if payload in v.files:
                    return json.dumps({'exitCode': 1, 'stdout': '', 'stderr': 'upload exists', 'success': False})
                v.files[payload] = ''
                v.files[marker] = owner
                r = {}
            elif op == 'append':
                offset, chunk = argv[7:9]
                if payload not in v.files or len(v.files[payload]) != int(offset):
                    return json.dumps({'exitCode': 1, 'stdout': '', 'stderr': 'agent upload offset/type mismatch', 'success': False})
                v.files[payload] += chunk
                r = {}
            elif op == 'commit':
                digest, size = argv[7:9]
                b64 = v.files.get(payload, '')
                data = base64.b64decode(b64, validate=True)
                if len(data) != int(size) or hashlib.sha256(data).hexdigest() != digest:
                    return json.dumps({'exitCode': 1, 'stdout': '', 'stderr': 'agent upload size/digest mismatch', 'success': False})
                granted = {}
                if len(argv) > 10:
                    fence, claim = [json.loads(base64.b64decode(x)) for x in argv[9:11]]
                    ag = Agent(self, v)
                    granted = ag.c_lease("claim", claim) if claim else ({"ok": True} if ag.lease_rule().accepts(fence.get("owner", ""), fence.get("boot", ""), fence.get("epoch", 0), fence.get("vm_id", "")) else {"stale": True})
                    if not granted.get("ok"):
                        return json.dumps({"exitCode": 0, "stdout": json.dumps(granted), "stderr": "", "success": True})
                v.files[root + '/agent.py'] = data.decode()
                v.files.pop(payload, None)
                v.files.pop(marker, None)
                r = granted
            elif op == 'cleanup':
                v.files.pop(payload, None)
                v.files.pop(marker, None)
                r = {}
            else:
                raise AssertionError('unknown agent upload operation')
            return json.dumps({'exitCode': 0, 'stdout': json.dumps(r) + '\n', 'stderr': '', 'success': True})
        if (len(argv) >= 3 and argv[0] == "python3" and argv[2] == "start-daemon"
                and argv[1] in v.files):
            b64 = base64.b64encode(v.files[argv[1]].encode()).decode()
            if len(argv) > 5:
                fence = json.loads(base64.b64decode(argv[5]))
                r = Agent(self, v).c_lease("renew", fence)
                if not r.get("ok"):
                    return json.dumps({"exitCode": 0, "stdout": json.dumps(r), "stderr": "", "success": True})
            r = self.start_daemon(v, b64, argv[4])
            return json.dumps({"exitCode": 0, "stdout": json.dumps(r) + "\n", "stderr": "", "success": True})
        if "start-daemon" in script:
            b64 = argv[argv.index("_") + 1] if "_" in argv else ""
            tok = argv[-1]
            r = self.start_daemon(v, b64, tok)
            return json.dumps({"exitCode": 0, "stdout": json.dumps(r) + "\n", "stderr": "", "success": True})
        if argv[:2] == ['python3', '-c'] and 'result relay: ' in argv[2]:
            # Model only transport/files; agent_dispatch still runs the real protocol rules.
            op, path = argv[3:5]
            out = ''
            if op == 'clean':
                for p in argv[4:]:
                    v.files.pop(p, None)
            elif op == 'stage':
                offset, chunk = int(argv[5]), argv[6]
                assert len(v.files.get(path, '')) == offset
                assert offset or path not in v.files
                v.files[path] = v.files.get(path, '') + chunk
            elif op == 'post':
                arg = argv[5]
                encoded = v.files[arg[1:]] if arg.startswith('@') else arg
                status, data = self.agent_dispatch(v, '/rpc', base64.b64decode(encoded), float(argv[8]))
                assert path not in v.files
                v.files[path] = data
                out = json.dumps({'path': path, 'bytes': len(data), 'status': status})
            elif op == 'read':
                size, offset, count = map(int, argv[5:8])
                assert len(v.files[path]) == size and 0 <= offset <= size and 0 <= count <= 65536
                out = base64.b64encode(v.files[path][offset:offset + count]).decode()
            else:
                raise AssertionError('unknown result relay operation')
            return json.dumps({'exitCode': 0, 'stdout': out + '\n', 'stderr': '', 'success': True})
        if argv[:2] == ["python3", "-c"] and len(argv) >= 4:  # (an agent call over boat exec)
            if v.daemon != "up":
                return json.dumps({"exitCode": 1, "stdout": "", "stderr": "urlopen error [Errno 111] Connection refused", "success": False})
            body = base64.b64decode(v.files.pop(argv[3][1:], "") if argv[3].startswith("@") else argv[3])
            try:
                code, data = self.agent_dispatch(v, "/rpc", body, 60)
            except OSError as e:
                return json.dumps({"exitCode": -1, "stdout": "", "stderr": f"sandbox {v.id} is stopped ({e})", "success": False})
            return json.dumps({"exitCode": 0 if code == 200 else 1, "stdout": data.decode(), "stderr": "", "success": code == 200})
        return json.dumps({"exitCode": 0, "stdout": "{}\n", "stderr": "", "success": True})

    # -- the agents
    def start_daemon(self, v: VMRec, b64: str, tok: str) -> dict:
        ver = hashlib.sha256(base64.b64decode(b64)).hexdigest()[:12] if b64 else v.disk.version
        A = _Rules(self.code.agent)
        alive = v.daemon == "up" or (v.daemon == "hung" and "answers(" not in _src(getattr(self.code.agent, "start_daemon", None)))
        v.disk.keys.setdefault("legacy", tok)
        what = A.Access().keep_daemon(alive, v.disk.token == tok, v.disk.version == ver)
        if what == "keep":
            return {"ok": True, "again": True}
        v.daemon = "down"
        self.k.sleep(1.0)
        v.disk.token, v.disk.version, v.daemon = tok, ver, "up"
        v.t_daemon = v.t_daemon or self.k.now
        self.note("daemon", vm=v.id, version=ver)
        return {"ok": True}

    def warm_vm(self, image, vm_type="large", ttl=900, token="warm", held: bool = True) -> VMRec:
        """A VM up with its agent (a pool already warm). held: the pool as a previous scheduler left it (its lease a
        dead process's, in the hold clocks it saved: the next scheduler takes it back); else free in the registry."""
        v = self.add_vm(image, vm_type, ttl, up_now=True)
        v.pool_name = image + getattr(self.code.boat, "LEASE_NAME_SUFFIX", "")
        v.disk.token, v.disk.version, v.daemon, v.t_daemon = token, self.code.agent.VERSION, "up", self.k.now
        B = self.code.boat
        info = {"url": f"https://{v.id}.sim.boat/", "token": token, "t": self.k.now, "version": v.disk.version}
        if hasattr(self.code.agent, "VMLease"):
            info["lease"] = Agent(self, v).c_lease("claim", {"owner": B.owner_id(), "vm_id": v.id})
        B.write_agent(v.id, info)
        reg = B.Registry()
        d = reg.read()
        lease = {"pid": 40999, "host": __import__("socket").gethostname(), "batch": "scheduler", "since": self.k.now - 3600, "label": "scheduler (before)"} if held else None
        d.setdefault("vms", {})[v.id] = {"type": vm_type, "snapshot": image, "created": self.k.now, "last_used": self.k.now, "state": RUNNING, "lease": lease, "until": v.until}
        reg.path.write_text(json.dumps(d))
        if held:
            st = self.live / "scheduler" / "metrics" / "state.json"
            st.parent.mkdir(parents=True, exist_ok=True)
            cur = json.loads(st.read_text()) if st.exists() else {}
            cur.setdefault("held", {})[v.id] = {"t_real": self.k.now, "draining": False}
            st.write_text(json.dumps(cur))
        return v

    def agent_req(self, cl, method, path, body, timeout):
        v = self.vms.get(cl.vm)
        self.k.sleep(self.cfg["rpc_s"])
        if any(a <= self.k.now < b for a, b, _ in self.front_down):
            return 502, b"<html>502 Bad Gateway</html>", "text/html"
        if v is None or v.state != RUNNING or v.daemon == "down":
            return 502, b"<html>502 Bad Gateway</html>", "text/html"
        if v.daemon == "hung":
            self.k.sleep(timeout)
            raise TimeoutError("timed out")
        if path.split("?")[0] == "/health":
            return 200, json.dumps({"ok": True, "version": v.disk.version,
                                    "lease": Agent(self, v).c_lease("who", {})}).encode(), "application/json"
        if cl.token != v.disk.token and cl.token not in v.disk.keys.values():
            return 403, b'{"error": "token"}', "application/json"
        code, data = self.agent_dispatch(v, path.split("?")[0], body or b"", timeout)
        if v.state != RUNNING:
            raise ConnectionResetError("[Errno 54] Connection reset by peer")
        for a, b, every in self.cfg.get("lose_start_reply", []):  # (boat's front failing mid-reply: the starts done, their answer lost)
            if a <= self.k.now < b and b'"start": [{' in (body or b""):
                self._lost_replies = getattr(self, "_lost_replies", 0) + 1
                if self._lost_replies % every == 0:
                    raise ConnectionResetError("[Errno 54] Connection reset by peer")
        return code, data, "application/json"

    def agent_dispatch(self, v: VMRec, path: str, body: bytes, timeout: float):
        ag = Agent(self, v)
        try:
            if path == "/put":
                return 200, json.dumps({"stored": ag.put(body)}).encode()
            if path == "/cache":
                return 200, b'{"ok": true}'
            msg = json.loads(body or b"{}")
            r = ag.rpc(msg.get("cmd"), msg.get("args", []), timeout)
            if isinstance(r, bytes):
                return 200, r
            return 200, json.dumps(r).encode()
        except (K.SimKilled, K.Deadline, OSError):
            raise
        except Exception as e:
            return 500, json.dumps({"error": repr(e)[-500:]}).encode()

    # -- the item processes on the VMs
    def run_item(self, v: VMRec, bid: str, name: str, job: Job, pid: int):
        spec = job.spec
        orig = spec.get("sim_name") or re.sub(r"~s\d+$", "", name)
        it = self.items.get((bid, orig))
        p = v.procs[pid]
        self.k.sleep(0.5)
        if not p["alive"]:
            return
        bd = v.disk.batches.get(bid)
        if bd is None or bd.tid not in v.disk.trees:  # (agent.run: the batch's tree gone from this VM: lost, Outcome.code)
            job.done = {"name": name, "code": -1, "seconds": -1, "lost": True, "err": f"the batch's tree isn't on {v.id}"}
            p["alive"] = False
            job.mtime = self.k.now
            self.note("tree_missing", vm=v.id, batch=bid, item=orig)
            if it is not None:
                it["ours"] += 1
            with v.cv:
                v.cv.notify_all()
            return
        if it is not None:
            it["runs"] += 1
            it["running"] += 1
            it.setdefault("first_run", self.k.now)  # (its wait: submitted to its first run)
        dur = float(spec.get("sim_s", 60))
        t0 = self.k.now
        job.out = f"run {name} on {v.id}: started\n"
        die = spec.get("sim_die_first") and it is not None and it["runs"] == 1  # (its process killed under it, no word: OOM, a VM-level kill)
        try:
            self.k.sleep(max(0.0, (dur / 2 if die else dur) - 0.5))
        finally:
            if it is not None:
                it["running"] -= 1
        if die and p["alive"]:
            p["alive"] = False
            job.out += "partial output, then killed\n"
            if it is not None:
                it["boat_vms"].add(v.id)
            self.note("item_died", vm=v.id, item=orig)
            with v.cv:
                v.cv.notify_all()
            return
        if not p["alive"]:  # (killed: preempted, or its VM stopped)
            return
        job.out += f"done code {int(spec.get('sim_code', 0))}\n"
        p["alive"] = False
        job.done = {"name": name, "code": int(spec.get("sim_code", 0)), "seconds": round(self.k.now - t0 + 0.5, 2)}
        job.mtime = self.k.now
        if it is not None:
            it["finished_on"].append(v.id)
        with v.cv:
            v.cv.notify_all()

    # -- the laptop's processes
    def live_sched(self):
        for proc, s in reversed(self.sched_procs):
            if proc.alive and s is not None and not getattr(s, "released_lock", False):  # (a handoff's old one: its port closed)
                return proc, s
        return None

    def spawn_scheduler(self, takeover: bool = False, after: float = 0.5, idle_exit: float = 120, code=None, label: str | None = None):
        """A scheduler process (serve): it waits for the one alive to exit (the lock), then runs."""
        label = label or self.code_installed
        code = code or self.codes.get(label) or self.code
        proc = self.k.spawn_proc(f"{getattr(self, 'machine', 'local')}-scheduler-{len(self.sched_procs)}", laptop=True)
        proc.env["machine"] = getattr(self, "machine", "local")
        proc.env["code"] = label
        entry = [proc, None]
        self.sched_procs.append(entry)
        self.pending_spawn += 1

        def serve():
            try:
                self.k.sleep(after)
                for _ in range(240):  # (the lock: the old one's exit, a takeover waits up to 2 min)
                    if not any(p.alive and s is not None and not getattr(s, "released_lock", False) for p, s in self.sched_procs if p is not proc):
                        break
                    if not takeover:
                        self.note("serve_refused", proc=proc.name)
                        return
                    self.k.sleep(0.5)
                os.environ["SUPERCOMPUTER_CLIENT"] = "scheduler"
                try:
                    s = code.sched.Scheduler(idle_exit)
                except Exception as error:
                    self.note("scheduler_boot_failed", pid=proc.pid, code=label, error=str(error))
                    proc.alive = False
                    return
                entry[1] = s
                self.note("scheduler_up", proc=proc.name, pid=proc.pid, code=self.code_installed)
            finally:
                self.pending_spawn -= 1
            s.run()
            self.note("scheduler_exit", proc=proc.name)
            proc.alive = False
        self.k.start(proc, serve, name="serve")
        return proc

    def ensure_running(self):
        """scheduler.ensure_running: same bounded startup budget as the code under test."""
        if self.live_sched() is None and self.pending_spawn == 0:
            self.spawn_scheduler(after=0.3)
        deadline = self.k.now + getattr(self.code.sched, "STARTUP_WAIT_S", 10.0)
        while self.k.now < deadline:
            if self.live_sched() is not None:
                break
            self.k.sleep(0.1)
        else:
            raise RuntimeError("the scheduler didn't start")
        return SimSchedClient(self)

    def kill9(self):
        ls = self.live_sched()
        if ls:
            self.note("kill9", proc=ls[0].name)
            self.k.kill_proc(ls[0])

    def add_code(self, code, label: str):
        """Another version of the code, loaded beside the first (a canary's new code)."""
        code.label = label
        self.codes[label] = code
        self.patch(code)

    def install(self, code_hash: str, broken: bool = False):
        """A new install (a commit to main): its code hash, importing or not (a hash of no code loaded: the same code)."""
        if code_hash not in self.codes:
            self.codes[code_hash] = self.codes[self.code_installed]
        self.code_installed, self.broken_install = code_hash, broken
        self.note("install", code=code_hash, broken=broken)

    def laptop_sleep(self, seconds: float):
        self.note("sleep", s=seconds, machine=getattr(self, "machine", "local"))
        for proc in self.k.procs.values():
            if proc.env.get("machine") == getattr(self, "machine", "local"):
                proc.env["frozen_until"] = self.k.now + seconds

    # -- the world's own clock: TTLs, boat's own stops, the samples
    def tick(self, every: float = 5.0):
        while True:
            self.k.sleep(every)
            now = self.k.now
            for v in list(self.vms.values()):
                if v.state in (RUNNING, "starting") and v.until < now:
                    self.stop(v, "ttl")

    def boat_stops(self, when: float, vm: VMRec | None = None, pick=None):
        """boat stops a VM by itself at `when` (seconds from now): a real boat failure of what runs there."""
        def go():
            self.k.sleep(when)
            target = vm or (pick(self) if pick else None)
            if target is not None:
                self.stop(target, "boat")
        self.k.start(self.k.world, go, name="boat-stop")


class _OldRules:
    """The agent's decisions as agent.py took them inline before its rule classes (375900f): for older code only."""

    class JobReport:
        def __init__(self, starting_s):
            self.s = starting_s

        def read(self, acked, done_file, readable, has_pid, alive, age_s):
            return "skip" if acked else ("done" if readable else "torn") if done_file else ("running" if alive else "lost") if has_pid else "lost" if age_s > self.s else "running"

    class BlobStore:
        def may_make_tree(self, tid, computed, ready, have_all):
            return not ready and have_all and computed == tid

        def drop_tree(self, rank, keep, in_use):
            return rank >= keep and not in_use

    class BatchDirs:
        def __init__(self, keep_s):
            self.keep_s = keep_s

        def drop(self, current, quiet_s, live_item):
            return not current and quiet_s > self.keep_s and not live_item

        def may_start(self, ready):
            return ready

    class Ticks:
        def ack(self, done_file):
            return done_file

        def stop(self, any_done, any_running, waited_s, wait_s):
            return any_done or not any_running or waited_s >= wait_s

    class Kill:
        def kills(self, has_pid, done):
            return has_pid and not done

    class Access:
        def keep_daemon(self, alive, same_token, same_version):
            return "keep" if alive and same_token and same_version else "replace"

    @staticmethod
    def tree_id(files, links):
        return hashlib.sha256(json.dumps([sorted(files.items()), sorted(links.items())], separators=(",", ":")).encode()).hexdigest()[:24]

    @staticmethod
    def vtick():
        pass


class _Rules:
    """The code under test's agent rules, the older inline ones for what it lacks."""

    def __init__(self, A):
        self._A = A

    def __getattr__(self, n):
        return getattr(self._A, n) if hasattr(self._A, n) else getattr(_OldRules, n)


class Agent:
    """One VM's agent, answering as agent.py's daemon does, its decisions by the agent's own rule classes."""

    STARTING_S = 60

    def __init__(self, w: World, v: VMRec):
        self.w, self.v, self.d, self.A = w, v, v.disk, _Rules(w.code.agent)

    def lease_rule(self):
        """Hydrate the actual shipped rule from the VM's persistent record and live inventory."""
        if self.d.lease is None:
            self.d.lease = self.A.VMLease()
        rule = self.d.lease
        live = [p["job"] for p in self.v.procs.values() if p["alive"]]
        rule.mine = sum(getattr(j, "owner", "") == rule.holder for j in live)
        rule.foreign = len(live) - rule.mine
        return rule

    def c_lease(self, op, payload):
        rule = self.lease_rule()
        now = self.w.k.now
        boot = str(self.v.boot)
        changed = rule.boot != boot or rule.vm_id != self.v.id
        lapsed = now - self.d.lease_renewed >= 180
        counts = {}
        for p in self.v.procs.values():
            if p["alive"]:
                owner = getattr(p["job"], "owner", "")
                counts[owner] = counts.get(owner, 0) + 1
        view = lambda: {"ok": True, "holder": rule.holder, "owner": rule.holder,
                        "boot": rule.boot, "epoch": rule.epoch, "vm_id": self.v.id,
                        "state": self.d.lease_state, "lapsed": lapsed or changed,
                        "draining": rule.draining, "items": counts, "counts": counts, "protocol": 1,
                        "valid_boot": not changed, "lease_s": 180, "uptime": now - (self.v.t_up or now)}
        if op == "who":
            return view()
        by = payload.get("owner", "")
        if op == "claim":
            birth = self.d.birth
            activated = not changed and rule.holder == birth.get("owner")
            if now - (self.v.t_up or now) < 600 and birth and (birth.get("owner") != by or not activated and (not birth.get("nonce") or birth.get("nonce") != payload.get("nonce"))):
                return {"ok": False, "busy": True, "birth": True}
            stopping = self.d.lease_state == "stopping" and now - self.d.stopping_at < 300 and not changed
            if not rule.claim(by, boot, self.v.id, lapsed, self.d.lease_state == "released" or (self.d.lease_state == "stopping" and not stopping), stopping):
                self.w.note("claim_refused", vm=self.v.id, by=by, draining=rule.draining)
                return {"ok": False, "busy": True, "draining": rule.draining}
            self.d.lease_state, self.d.lease_renewed = "held", now
            self.w.note("lease_granted", vm=self.v.id, by=by, boot=boot, epoch=rule.epoch)
            return view()
        if changed or not rule.accepts(by, payload.get("boot", ""), payload.get("epoch", 0), payload.get("vm_id", "")):
            return {"stale": True, "ok": False}
        if op in ("renew", "check", "fence_stop") and self.d.lease_state != "held":
            return {"stale": True, "ok": False}
        if op == "check":
            pass
        elif op == "renew":
            self.d.lease_renewed = now
        elif op == "release":
            if self.d.lease_state == "stopping" and (not self.d.stop_nonce or payload.get("stop_nonce") != self.d.stop_nonce):
                return {"stale": True, "ok": False}
            self.d.lease_state = "released"
        elif op == "fence_stop":
            if not rule.fence_stop(by, payload.get("boot", ""), payload.get("epoch", 0), payload.get("vm_id", "")):
                return {"stale": True, "ok": False}
            self.d.stop_nonce = f"{self.w.rng.getrandbits(192):048x}"
            self.d.lease_state, self.d.stopping_at = "stopping", now
            self.w.note("stop_fenced", vm=self.v.id, by=by)
            return {**view(), "stop_nonce": self.d.stop_nonce}
        else:
            return {"ok": False, "error": "unknown lease op"}
        return view()

    def rpc(self, cmd, a, timeout):
        a = list(a)
        self.fence = a.pop()["_lease"] if a and isinstance(a[-1], dict) and "_lease" in a[-1] else {}
        self.caller = self.fence.get("owner", "")
        if hasattr(self.A._A, "VMLease") and cmd not in ("lease", "ping"):
            r = self.lease_rule()
            writable = cmd not in ("itemlogs", "logs", "have", "cacheout")
            current = r.boot == str(self.v.boot) and r.vm_id == self.v.id
            if writable and (not current or self.d.lease_state != "held" or not r.accepts(self.caller, self.fence.get("boot", ""), self.fence.get("epoch", 0), self.fence.get("vm_id", ""))):
                return {"stale": True}
            if cmd == "vtick":
                self.d.lease_renewed = self.w.k.now
        fn = getattr(self, f"c_{cmd}", None)
        if fn is None:
            return {"error": f"no command {cmd}"}
        return fn(*a) if cmd != "vtick" else self.c_vtick(a[0], timeout)

    def c_ping(self):
        out = {"ok": True, "version": self.d.version, "boot": str(self.v.boot), "cpus": self.v.cpus, "load": 0.3}
        if hasattr(self.A._A, "clean_disk"):
            out["disk"] = self._disk()
        return out

    def _disk(self):
        """agent.clean_disk: the scratch older than every running job goes while low; the disk after."""
        d = self.d
        if self.A.disk_low(d.free_mb, d.total_mb):
            live = [p["job"].t for p in self.v.procs.values() if p["alive"]]
            oldest = int(min(live, default=self.w.k.now))
            for e in [e for e in d.scratch if self.A.scratch_drops(True, int(e[0]), oldest)]:
                d.scratch.remove(e)
                d.free_mb += e[1]
        return {"free_mb": d.free_mb, "total_mb": d.total_mb}

    def c_begin(self, ver, bid, tid, ctx):
        if ver != self.d.version:
            return {"ok": False, "version": self.d.version}
        tr = self.make_tree(tid, None, None)
        if not tr.get("ok"):
            return {"ok": False, "version": self.d.version, "need": "manifest"}
        return {"ok": True, "version": self.d.version, "prep": self.prep(bid, f"tree:{tid}", ctx, tid)}

    def c_verify(self, tid, gits):
        """agent.verify_tree: a tree short of its manifest is bad; the agent since 11-02 drops it (tree_kept)."""
        d = self.d
        if d.partial_first and tid in d.trees:
            d.partial_first = False
            d.partial.add(tid)
        if tid not in d.partial:
            return {"ok": True, "checked": len(gits or {}), "bad": []}
        bad = [[p, b, "missing"] for p, b in list((gits or {}).items())[:5]] or [["README.md", "0" * 40, "missing"]]
        out = {"ok": False, "checked": len(gits or {}), "bad": bad}
        if hasattr(self.A._A, "tree_kept"):
            d.partial.discard(tid)
            d.trees.pop(tid, None)
            for bd in d.batches.values():
                if getattr(bd, "tid", None) == tid:
                    bd.ready = False
            out.update(dropped=True, partial=True)
        return out

    def c_have(self, keys):
        return {"missing": [k for k in keys if k not in self.d.blobs]}

    def c_tree(self, tid, files, links):
        return self.make_tree(tid, files, links)

    def c_prep(self, bid, source, ctx="", tid="", sha="", csrc="-"):
        return self.prep(bid, source, ctx, tid)

    def c_pull(self, url, token, keys):
        peer = next((v for v in self.w.vms.values() if url.startswith(f"https://{v.id}.")), None)
        if peer is None or peer.state != RUNNING or peer.daemon != "up":
            return {"ok": False, "missing": keys}
        self.w.k.sleep(0.5)
        got = [k for k in keys if k in peer.disk.blobs]
        self.d.blobs |= set(got)
        return {"ok": True, "missing": [k for k in keys if k not in self.d.blobs]}

    def c_pullcache(self, *a):
        return {"got": []}

    def c_cacheout(self, *a):
        return {"names": []}

    def c_itemlogs(self, bid, name):
        bd = self.d.batches.get(bid)
        j = bd.jobs.get(name) if bd else None
        return {"out": j.out if j else "", "err": ""}

    def c_logs(self, bid):
        """agent.logs_tar: every item of the batch on this VM, <run name>/out and /err."""
        buf = io.BytesIO()
        bd = self.d.batches.get(bid)
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for name, j in sorted(bd.jobs.items()) if bd else []:
                for f, data in (("out", j.out.encode()), ("err", b"")):
                    ti = tarfile.TarInfo(f"{name}/{f}")
                    ti.size = len(data)
                    tf.addfile(ti, io.BytesIO(data))
        return buf.getvalue()

    def c_upgrade(self, b64, port, tok):
        if any(p["alive"] for p in self.v.procs.values()):
            self.w.note("upgrade_deferred", vm=self.v.id)
            return {"ok": False, "error": "owned_upgrade_blocked"}
        ver = hashlib.sha256(base64.b64decode(b64)).hexdigest()[:12]
        v = self.v

        def restart():
            self.w.k.sleep(0.5)
            v.daemon = "down"
            self.w.k.sleep(0.5)
            if v.state == RUNNING:
                v.disk.version, v.disk.token, v.daemon = ver, tok, "up"
                self.w.note("upgraded", vm=v.id, version=ver)
        self.w.k.start(self.w.k.world, restart, name=f"upgrade {v.id}")
        return {"ok": True, "from": self.d.version, "to": ver}

    def c_killitem(self, bid, name):
        bd = self.d.batches.get(bid)
        j = bd.jobs.get(name) if bd else None
        has_pid = j is not None and j.pid is not None
        done = j is not None and (j.done is not None or j.acked)
        if j is not None and getattr(j, "owner", "") != getattr(self, "caller", ""):
            return {"stale": True}
        if j is not None and self.A.Kill().kills(has_pid, done):
            self.w.k.sleep(0.05)  # (kill_tree: sudo kill)
            p = self.v.procs.get(j.pid)
            if p:
                p["alive"] = False
            with self.v.cv:  # (vtick reads every 0.1 s: it sees the process gone at once)
                self.v.cv.notify_all()
            self.w.k.sleep(0.2 + self.w.cfg.get("kill_slow_s", 0.0))  # (kill_item's sleep; a slow VM's sudo and disk add to it)
            j.done = {"name": name, "code": -1, "seconds": -1, "preempted": True}
            j.mtime = self.w.k.now
            with self.v.cv:
                self.v.cv.notify_all()
            return {"killed": 1}
        return {"killed": 0}

    def put(self, body: bytes) -> int:
        n = 0
        with tarfile.open(fileobj=io.BytesIO(body), mode="r|*") as tf:
            for m in tf:
                if m.isfile():
                    self.d.blobs.add(pathlib.PurePosixPath(m.name).name)
                    n += 1
        return n

    # (agent.make_tree, gc_trees, prep, drop_old_batches, vtick, report: the same steps, their decisions the rules')
    def make_tree(self, tid, files, links):
        if tid in self.d.trees:
            self.d.trees[tid] = self.w.k.now
            return {"ok": True, "again": True, "seconds": 0}
        if files is None:
            return {"ok": False, "need": "manifest"}
        miss = sorted({k for k in files.values() if k not in self.d.blobs})
        if miss:
            return {"ok": False, "missing": miss}
        computed = self.A.tree_id(files, links or {})
        if not self.A.BlobStore().may_make_tree(tid, computed, False, True):
            return {"ok": False, "err": f"the manifest given isn't tree {tid}"}
        self.w.k.sleep(0.3)
        self.d.trees[tid] = self.w.k.now
        self.gc_trees({tid})
        return {"ok": True, "seconds": 0.3, "files": len(files)}

    def _alive(self, j: Job) -> bool:
        p = self.v.procs.get(j.pid) if j.pid is not None else None
        return bool(p and p["alive"])

    def gc_trees(self, keep: set, n: int = 6):
        keep = set(keep) | {bd.tid for bd in self.d.batches.values() if any(self._alive(j) and j.done is None for j in bd.jobs.values())}
        trees = sorted(self.d.trees, key=lambda t: -self.d.trees[t])
        store = self.A.BlobStore()
        params = inspect.signature(store.drop_tree).parameters
        prepared = {bd.tid for bd in self.d.batches.values() if bd.ready} if hasattr(self.A._A, "prepared_trees") else set()
        for rank, t in enumerate(trees):
            args = [rank, n, t in keep] + ([t in prepared] if "prepared" in params else [])
            if store.drop_tree(*args):
                del self.d.trees[t]
                self.w.note("tree_dropped", vm=self.v.id, tree=t)

    def drop_old_batches(self, batch: str, keep_s: float = 1800):
        rule = self.A.BatchDirs(int(keep_s))
        for bid, bd in list(self.d.batches.items()):
            quiet = int(self.w.k.now - max([bd.mtime] + [j.mtime for j in bd.jobs.values()]))
            live = any(self._alive(j) and j.done is None for j in bd.jobs.values())
            if rule.drop(bid == batch, quiet, live):
                del self.d.batches[bid]

    def prep(self, bid, source, ctx, tid):
        self.drop_old_batches(bid)
        bd = self.d.batches.get(bid)
        if bd is not None and bd.ready:
            return {"ok": True, "again": True, "caches": {}, "cpus": self.v.cpus}
        if source.startswith("tree:"):
            t = source[5:]
            if t not in self.d.trees:
                return {"ok": False, "err": "that tree isn't on this VM"}
            self.d.trees[t] = self.w.k.now
        bd = self.d.batches[bid] = BDir(self.w.k.now, source[5:], ctx)
        self.w.k.sleep(0.2 + self.w.cfg.get("prep_s", 0.0))  # (a big tree's caches restored: a preparation that takes a while)
        bd.ready, bd.mtime = True, self.w.k.now
        return {"ok": True, "seconds": 0.2, "caches": {}, "cpus": self.v.cpus}

    def start(self, bid, bd: BDir, spec):
        name = spec["name"]
        if name in bd.jobs:  # (a start sent again)
            return
        j = bd.jobs[name] = Job(spec, self.w.k.now, max(1, int(spec.get("cpus", 2))))
        j.owner = getattr(self, "caller", "")
        pid = len(self.v.procs) + 1 + 1000 * self.v.boot
        while pid in self.v.procs:
            pid += 1
        self.v.procs[pid] = {"job": j, "batch": bid, "name": spec.get("sim_name") or re.sub(r"~s\d+$", "", name), "alive": True}
        j.pid = pid
        owners = {getattr(p["job"], "owner", "") for p in self.v.procs.values() if p["alive"]}
        self.w.note("item_started", vm=self.v.id, batch=bid, item=name, owner=j.owner)
        if len(owners) > 1:
            self.w.note("mixed_owners", vm=self.v.id, owners=sorted(owners))
        self.w.k.start(self.w.k.world, self.w.run_item, (self.v, bid, name, j, pid), name=f"item {name}")

    def report(self, bid, bd: BDir):
        done, running = [], []
        for name, j in sorted(bd.jobs.items()):
            rule = self.A.JobReport(self.STARTING_S)
            what = rule.read(j.acked, j.done is not None, True, j.pid is not None, self._alive(j), int(self.w.k.now - j.mtime))
            if what == "lost":
                self.w.k.sleep(0.2)  # (the runner's done.json may be landing)
            if what == "lost" and j.done is None:
                j.done = {"name": name, "code": -1, "seconds": -1, "lost": True, "err": "its process gone without a word"}
                j.mtime = self.w.k.now
                what = "done"
            if what in ("done", "lost") and getattr(j, "owner", "") == getattr(self, "caller", ""):
                done.append(j.done)
            elif what in ("running", "torn"):
                running.append(name)
        return done, running

    def c_vtick(self, msg, timeout):
        for a in msg.get("acks", []):
            bd = self.d.batches.get(a["batch"])
            j = bd.jobs.get(a["name"]) if bd else None
            if j is not None and getattr(j, "owner", "") == getattr(self, "caller", "") and self.A.Ticks().ack(j.done is not None):
                j.acked, j.done = True, None
        for st in msg.get("start", []):
            bd = self.d.batches.get(st["batch"])
            ready = bd is not None and bd.ready
            if ready and "tp.exists()" in _src(self.A.vtick):  # (the agent checks its tree is here too, since 55b3abf)
                ready = bd.tid in self.d.trees
            if not self.A.BatchDirs(0).may_start(ready):
                return {"err": f"batch {st['batch']} isn't prepared here", "batch": st["batch"]}
            fence = getattr(self, "fence", {})
            if hasattr(self.A._A, "VMLease") and not self.lease_rule().start(fence.get("owner", ""), fence.get("boot", ""), fence.get("epoch", 0), fence.get("vm_id", "")):
                return {"stale": True}
            self.start(st["batch"], bd, st["spec"])
        wait, t0 = float(msg.get("wait", 20)), self.w.k.now
        while True:
            done, running = [], []
            for bid, bd in list(self.d.batches.items()):
                dn, rn = self.report(bid, bd)
                done += [{**r, "batch": bid} for r in dn]
                running += [{"batch": bid, "name": n, "owner": getattr(bd.jobs[n], "owner", "")} for n in rn]
            if self.A.Ticks().stop(bool(done), bool(running), int(self.w.k.now - t0), int(wait)):
                break
            left = t0 + wait - self.w.k.now
            with self.v.cv:
                self.v.cv.wait(max(0.1, left))
            if self.v.state != RUNNING:
                raise ConnectionResetError("[Errno 54] Connection reset by peer")
        out = {"done": done, "running": running, "load": 0.3, "cpus": self.v.cpus, "model": self.v.model}
        if hasattr(self.A._A, "clean_disk"):
            out["disk"] = self._disk()
        return out


_srcs: dict = {}


def _src(fn) -> str:
    if fn not in _srcs:
        try:
            _srcs[fn] = inspect.getsource(fn)
        except Exception:
            _srcs[fn] = ""
    return _srcs[fn]


class FakeResp:
    def __init__(self, status, data, ctype):
        self.status, self._data, self._ctype = status, data, ctype
        self._offset = 0

    def read(self, size=-1):
        end = len(self._data) if size < 0 else self._offset + size
        data = self._data[self._offset:end]
        self._offset += len(data)
        return data

    def getheaders(self):
        return [("Content-Type", self._ctype)]

    def getheader(self, name, default=None):
        return self._ctype if name.lower() == "content-type" else default


class FakeConn:
    """http.client.HTTPSConnection to a VM's agent through boat's front."""

    def __init__(self, w, cl, timeout):
        self.w, self.cl, self.timeout, self.sock, self._r = w, cl, timeout, None, None

    def request(self, method, url, body=None, headers=None):
        path = url[len(self.cl.base):] if url.startswith(self.cl.base) else url
        self._r = FakeResp(*self.w.agent_req(self.cl, method, path, body, self.timeout))

    def getresponse(self):
        return self._r

    def close(self):
        pass


# ------------------------------------------------------------------------------------------------ the transport
class SimSchedClient:
    """scheduler.Client as a client sees it: each request handled by a thread of the scheduler process (it dies with
    it: a client sees the connection reset), the client waiting up to its timeout."""

    remote = None

    def __init__(self, w: World):
        self.w = w

    def push_tree(self, tree):
        return 0

    def push_caches(self, *a):
        return None

    def keep(self):
        pass

    def _known(self):
        return {}

    def req(self, path: str, msg, timeout: float = 60, raw: bool = False, body=None):
        w = self.w
        w.k.sleep(0.002)
        ls = w.live_sched()
        if ls is None or (w.k.now < max(w.k.frozen_until, ls[0].env.get("frozen_until", 0.0))):
            raise ConnectionRefusedError(f"[Errno 61] Connection refused ({path})")
        proc, s = ls
        box = {}
        ev = w.th.Event()
        t0 = w.k.now

        def handle():
            try:
                if path == "/submit":
                    box["r"] = s.submit(msg)
                elif path in ("/poll", "/close", "/logs") and hasattr(s, "batch_of") and s.batch_of(msg) is None:  # (the handler's 400)
                    box["e"] = RuntimeError(f"the scheduler: {path}: 400 no batch named")
                elif path == "/poll":
                    box["r"] = s.poll(s.batch_of(msg) if hasattr(s, "batch_of") else msg["batch"], int(msg.get("since", 0)), float(msg.get("wait", 15)))
                elif path == "/close":
                    box["r"] = s.close(s.batch_of(msg) if hasattr(s, "batch_of") else msg["batch"])
                elif path == "/hint":
                    box["r"] = s.hint(msg)
                elif path == "/status":
                    box["r"] = s.status()
                elif path == "/capabilities":
                    box["r"] = s.capabilities()
                elif path == "/logs":
                    box["r"] = s.logs(s.batch_of(msg) if hasattr(s, "batch_of") else msg["batch"], msg.get("vms", []))
                elif path == "/rollback" and hasattr(s, "rollback"):
                    box["r"] = s.rollback(msg.get("code") or None)
                elif path == "/health" and hasattr(s, "health_check"):
                    box["r"] = s.health_check(float(msg.get("minutes", 30)))
                else:
                    box["e"] = RuntimeError(f"404 {path}")
            except (K.SimKilled, K.Deadline):
                raise
            except Exception as e:
                box["e"] = RuntimeError(f"500 {e!r}"[:300])
                w.__dict__.setdefault("http_500", []).append(f"{path}: {e!r}"[:200])  # (the handler's health error: a request it failed)
            finally:
                ev.set()
        w.k.start(proc, handle, name=f"http {path}")
        try:
            while not ev.is_set():
                if not ev.wait(min(1.0, max(0.01, t0 + timeout - w.k.now))):
                    if not proc.alive:
                        raise ConnectionResetError(f"[Errno 54] Connection reset by peer ({path})")
                    if w.k.now - t0 >= timeout:
                        raise TimeoutError(f"timed out ({path})")
        finally:
            if path == "/status":  # (an answer that never came counts as long as it was waited for)
                w.status_ms.append(1000 * (w.k.now - t0))
        if "e" in box:
            raise box["e"]
        if "r" not in box:  # (its handler died with the scheduler)
            raise ConnectionResetError(f"[Errno 54] Connection reset by peer ({path})")
        r = box.get("r")
        if raw:
            return r if isinstance(r, bytes) else json.dumps(r).encode()
        return json.loads(json.dumps(r))
