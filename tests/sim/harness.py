"""A simulation: the kernel, the world, the code under test, its clients, and the ground truth's samples.

    sim = Sim(code_dir, seed=1, cfg={...})
    sim.at(0, lambda: sim.batch("bar-1", "gate", items(...)))
    sim.treadmill(...)
    sim.run(3600)
    report = metrics.measure(sim)
"""
import datetime
import hashlib
import json
import os
import contextlib
import pathlib
import shutil
import tempfile
import types

from . import kernel as K
from . import world as W

T0 = datetime.datetime(2026, 11, 2, 1, 0, tzinfo=datetime.timezone.utc).timestamp()
IMAGE = "sc-img-sim"


def items(prefix: str, n: int, s: float | list = 60, est: float | None = None, weight: int = 1, code: int = 0, **extra) -> list[dict]:
    """n items of `s` seconds (or a list of durations), their estimate `est` (default: their duration)."""
    out = []
    for i in range(n):
        d = s[i % len(s)] if isinstance(s, list) else s
        out.append({"name": f"{prefix}{i}", "kind": "sh", "cmd": "true", "limit": 7200, "est": d if est is None else est, "weight": weight, "sim_s": d, "sim_code": code, **extra})
    return out


class Sim:
    def __init__(self, code_dir: str, seed: int = 1, cfg: dict | None = None, policy: dict | None = None, live: str | None = None,
                 shared=None, machine: str = "local"):
        self.tmp = live or tempfile.mkdtemp(prefix="sc-sim-")
        self.machine = machine
        self.k = shared.k if shared is not None else K.Kernel(T0, seed)
        package = "supercomputer" if shared is None else "supercomputer_sim_" + machine.replace("-", "_")
        self.code = W.load_code(code_dir, self.tmp, name=package)
        self.w = W.World(self.k, self.code, self.tmp, seed, cfg)
        self.w.machine = machine
        self.peers = shared.peers if shared is not None else []
        self.peers.append(self)
        if shared is not None:
            self.w.share_pool(shared.w)
        self.S, self.B = self.code.sched, self.code.boat
        # (the live policy's values where they differ from the code's defaults: sub_preempt_s 45 since 11-01 17:10)
        policy = {"sub_preempt_s": 45.0, **(policy or {})}
        (pathlib.Path(self.tmp) / "policy.json").write_text(json.dumps({**self.S.policy_defaults(), "generation": 1, **policy}))
        self.samples: list[dict] = []
        self.clients: dict[str, dict] = shared.clients if shared is not None else {}
        self.trees: dict[str, dict] = {}
        self.batch_n = 0
        if shared is None:
            self.k.start(self.k.world, self.w.tick, name="boat ttl")
        self.k.start(self.k.world, self._sampler, name="sampler")

    # -- trees: their blobs in the scheduler's blob store, as a client's push_tree leaves them
    def tree(self, seed: str = "t", n: int = 3) -> dict:
        if seed in self.trees:
            return self.trees[seed]
        blobs = pathlib.Path(self.tmp) / "scheduler" / "blobs"
        blobs.mkdir(parents=True, exist_ok=True)
        files, gits = {}, {}
        for i in range(int(seed.split(":")[1]) if ":" in seed else n):
            data = f"{seed.split(':')[0]}-{i}".encode()  # ("name:n[:x]": n files, their contents the name's; x: other paths, the same blobs)
            key = hashlib.sha256(data).hexdigest()
            (blobs / key).write_bytes(data)
            path = f"{seed.split(':')[2]}/f{i}.txt" if seed.count(":") >= 2 else f"f{i}.txt"
            files[path] = key
            gits[path] = hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()
        from .world import _Rules
        t = {"id": _Rules(self.code.agent).tree_id(files, {}), "files": files, "links": {}, "gits": gits}
        self.trees[seed] = t
        return t

    # -- events at virtual times
    def at(self, t: float, fn, name: str = "event"):
        def go():
            self.k.sleep(max(0.0, t - (self.k.now - self.k.t0)))
            fn()
        self.k.start(self.k.world, go, name=name)

    # -- clients
    def batch(self, client: str, klass: str, its: list[dict], image: str = IMAGE, tree: str = "t", on_lost: bool = True, max_wait: int = 0,
              after: float = 0.0, label: str = "", abandon_after: float | None = None, collect: bool = False, vm_type: str = "large", vm_named: bool = False,
              verify: bool = False) -> str:
        """A client process submitting one batch through boat.Scheduled.run (the real client), then closing it."""
        self.batch_n += 1
        bid = f"{self.machine}-{client}-{self.batch_n}" if len(self.peers) > 1 else f"{client}-{self.batch_n}"
        rec = {"client": client, "klass": klass, "items": {it["name"]: None for it in its}, "lost": {}, "error": None, "t_submit": None, "t_end": None, "image": image}
        self.clients[bid] = rec
        proc = self.k.spawn_proc(client, laptop=True)
        proc.env["machine"] = self.machine
        for it in its:
            self.w.items[(bid, it["name"])] = {"klass": klass, "image": image, "submitted": None, "delivered": None, "runs": 0, "running": 0, "boat_vms": set(), "ours": 0,
                                               "finished_on": [], "est": it.get("est"), "s": it.get("sim_s"), "client": client, "timing": bool(it.get("timing")),
                                               "machine": self.machine, "weight": int(it.get("weight", 1)), "vm_type": it.get("vm_type") or (vm_type if vm_named else None), "memory": it.get("memory"), "cpu_model": it.get("cpu_model")}
        self.w.batches[bid] = rec
        t = self.tree(tree)

        def run():
            self.k.sleep(after)
            s = object.__new__(self.B.Scheduled)
            s.sched, s.klass = self.S, self.S.klass(klass)
            s.vm_ids, s.run_names, s.client, s.result, s.logs = set(), {}, None, {}, {}
            s.tree = types.SimpleNamespace(id=t["id"], files=t["files"], links=t["links"], gits=t["gits"] if verify else {})  # (verify: the manifest names each file's git blob, the VM checks them)
            s.caches, s.proj = [], types.SimpleNamespace(cache_outputs=lambda c: [], name="simrepo", root=pathlib.Path(self.tmp))
            s.batch, s.label, s.image, s.vm_type, s.ctx_b64, s.warm_only = bid, label or f"{client} {klass}", image, vm_type, "", False
            s.vm_named = vm_named
            s.stop = self.w.th.Event()
            s.submitted = False
            s.bids, s._bid_lock = [], self.w.th.Lock()
            rec["t_submit"] = self.k.now
            for it in its:
                self.w.items[(bid, it["name"])]["submitted"] = self.k.now

            def done(it, row):
                rec["items"][it["name"]] = row
                self.w.items[(bid, it["name"])]["delivered"] = self.k.now
                if row.get("error") and "without its row" in str(row.get("error")):
                    rec["lost"][it["name"]] = row["error"]

            def lost(it, why):
                rec["items"][it["name"]] = {"lost": True, "why": why}
                rec["lost"][it["name"]] = why
                self.w.items[(bid, it["name"])]["delivered"] = self.k.now
            if abandon_after is not None:  # (its user gives up: ^C, the client closes the batch with items running)
                self.k.start(proc, lambda: (self.k.sleep(abandon_after), s.stop.set(), rec.__setitem__("abandoned", True)), name="abandon")
            try:
                s.run([dict(it) for it in its], done, on_lost=lost if on_lost else None, max_wait=max_wait)
            except (K.SimKilled, K.Deadline):
                raise
            except KeyboardInterrupt:
                if not rec.get("abandoned"):
                    rec["error"] = "KeyboardInterrupt"
                    self.w.client_errors.append(f"{bid}: KeyboardInterrupt")
            except BaseException as e:
                rec["error"] = repr(e)[:300]
                self.w.client_errors.append(f"{bid}: {e!r}"[:300])
            finally:
                rec["t_end"] = self.k.now
            rec["returned"] = rec["error"] is None
            if collect and rec["returned"]:  # (boat_jobs and the treadmill: the batch's logs, by item, before close)
                try:
                    rec["logs"] = s.collect()
                except (K.SimKilled, K.Deadline):
                    raise
                except Exception as e:
                    self.w.client_errors.append(f"{bid} collect: {e!r}"[:300])
            try:
                s.close()
            except (K.SimKilled, K.Deadline):
                raise
            except Exception as e:
                self.w.client_errors.append(f"{bid} close: {e!r}"[:300])
        self.k.start(proc, run, name=f"client {bid}")
        return bid

    def session(self, client: str, klass: str, runs: list[tuple[float, list[dict]]], image: str = IMAGE, tree: str = "t") -> dict:
        """One client session (one boat.Scheduled) whose runs start at the times given, each in a thread of its own, so
        they overlap; closed once every run returned. The record's "rows" by run: what each run's on_done got."""
        rec = {"client": client, "runs": [], "errors": [], "closed": False}
        proc = self.k.spawn_proc(client, laptop=True)
        proc.env["machine"] = self.machine
        t = self.tree(tree)
        s = object.__new__(self.B.Scheduled)
        s.sched, s.klass = self.S, self.S.klass(klass)
        s.vm_ids, s.run_names, s.client, s.result, s.logs = set(), {}, None, {}, {}
        s.tree = types.SimpleNamespace(id=t["id"], files=t["files"], links=t["links"])
        s.caches, s.proj = [], types.SimpleNamespace(cache_outputs=lambda c: [], name="simrepo", root=pathlib.Path(self.tmp))
        s.batch, s.label, s.image, s.vm_type, s.ctx_b64, s.warm_only = f"{client}-session", f"{client} {klass}", image, "large", "", False
        s.stop = self.w.th.Event()
        s.submitted = False
        s.bids, s._bid_lock = [], self.w.th.Lock()
        done = []

        def one(n, at, its):
            self.k.sleep(at)
            rows = {}
            try:
                s.run([dict(it) for it in its], lambda it, row: rows.__setitem__(it["name"], row), on_lost=lambda it, why: rows.__setitem__(it["name"], {"lost": why}))
            except (K.SimKilled, K.Deadline):
                raise
            except BaseException as e:
                rec["errors"].append(f"run {n}: {e!r}"[:300])
            rec["runs"].append({"n": n, "items": len(its), "rows": len(rows), "lost": sum(1 for r in rows.values() if r.get("lost"))})
            done.append(n)
            if len(done) == len(runs):
                with contextlib.suppress(Exception):
                    s.close()
                rec["closed"] = True
        for n, (at, its) in enumerate(runs):
            self.k.start(proc, lambda n=n, at=at, its=its: one(n, at, its), name=f"{client} run {n}")
        rec["s"] = s
        return rec

    def treadmill(self, image: str = IMAGE, base_vms: int = 8, filler_n: int = 64, filler_s: float = 900, sweep_n: int = 0, sweep_s: float = 600, until: float = 1e9,
                  backlog: bool = False):
        """The treadmill: its hint (the floor) every minute, a filler batch (and a sweep's) kept submitted."""
        proc = self.k.spawn_proc("treadmill", laptop=True)
        proc.env["machine"] = self.machine
        cl = W.SimSchedClient(self.w)

        def hints():
            while self.k.now - self.k.t0 < until:
                try:
                    cl.req("/hint", {"client": "treadmill", "image": image, "base_vms": base_vms, "ttl": 300, "pid": proc.pid})
                except (K.SimKilled, K.Deadline):
                    raise
                except Exception:  # (the scheduler not up yet, or handing off: again soon)
                    self.k.sleep(1)
                    continue
                self.k.sleep(60)
        self.k.start(proc, hints, name="treadmill hints")
        n = [0]

        def chain(kind, cnt, s):
            while self.k.now - self.k.t0 < until:
                n[0] += 1
                bid = self.batch("treadmill", kind, items(f"{kind[0]}{n[0]}-", cnt, s), image=image, tree=f"tm-{kind}")
                rec = self.clients[bid]
                # (the next round submitted while a quarter of this one is left: filler always queued, as the treadmill keeps it)
                while rec["t_end"] is None and sum(1 for x in rec["items"].values() if x is None) > cnt // 4:
                    self.k.sleep(15)
        if filler_n and not backlog:
            self.k.start(proc, chain, ("filler", filler_n, filler_s), name="treadmill filler")

        def keep_backlog():  # (gdp-wired's treadmill since 11-01: filler queued for at least the pool's slots, its day's caps yielding)
            while self.k.now - self.k.t0 < until:
                slots = sum(max(1, v.cpus // 2) for v in self.w.vms.values() if v.state == W.RUNNING and v.image == image)
                queued = sum(1 for (b, n), it in self.w.items.items() if it["klass"] == "filler" and it["image"] == image
                             and it["submitted"] and it["delivered"] is None and it["running"] == 0 and self.clients[b]["t_end"] is None)
                if queued < slots:
                    n[0] += 1
                    self.batch("treadmill", "filler", items(f"f{n[0]}-", max(16, slots - queued), filler_s), image=image, tree="tm-filler")
                self.k.sleep(30)
        if backlog:
            self.k.start(proc, keep_backlog, name="treadmill backlog")
        if sweep_n:
            self.k.start(proc, chain, ("sweep", sweep_n, sweep_s), name="treadmill sweep")

    def status_monitor(self, every: float = 30.0):
        proc = self.k.spawn_proc("status", laptop=True)
        proc.env["machine"] = self.machine
        cl = W.SimSchedClient(self.w)

        def mon():
            while True:
                self.k.sleep(every)
                try:
                    cl.req("/status", {}, timeout=300)
                except (K.SimKilled, K.Deadline):
                    raise
                except Exception:
                    pass
        self.k.start(proc, mon, name="status monitor")

    def scheduler(self, idle_exit: float = 120):
        return self.w.spawn_scheduler(after=0.1, idle_exit=idle_exit)

    # -- the ground truth's samples (every 10 s)
    def _sampler(self, every: float = 10.0):
        while True:
            self.k.sleep(every)
            self.samples.append(self.sample())

    def held_by(self) -> dict:
        """Each VM's owner: a live scheduler holding it, a live lease in the registry, or released a moment ago."""
        owners = {}
        for proc, s in [entry for peer in self.peers for entry in peer.w.sched_procs]:
            if proc.alive and s is not None:
                for vid, vm in list(getattr(s, "vms", {}).items()):
                    if not getattr(vm, "dead", False):
                        owners[vid] = proc.name
        try:
            reg = json.loads((pathlib.Path(self.tmp) / "boat-pool.json").read_text()).get("vms", {})
        except Exception:
            reg = {}
        for vid, v in reg.items():
            if vid in owners:
                continue
            l = v.get("lease") or {}
            p = self.k.procs.get(int(l.get("pid") or 0))
            if l and p is not None and p.alive:
                owners[vid] = p.name
            elif not l and float(v.get("until", 0) or 0) > self.k.now:
                owners[vid] = "released"
        return owners

    def sample(self) -> dict:
        now = self.k.now
        owners = self.held_by()
        per = {}
        for v in self.w.vms.values():
            if v.state != W.RUNNING:
                continue
            p = per.setdefault(v.image, {"vms": 0, "slots": 0, "busy": 0, "busy_real": 0, "orphans": [], "ready_vms": 0})
            p["vms"] += 1
            slots = max(1, v.cpus // 2)
            if v.daemon == "up" or v.t_daemon is not None:
                p["ready_vms"] += 1
                p["slots"] += slots
                busy = real = 0
                for pr in v.procs.values():
                    if pr["alive"]:
                        sp = pr["job"].spec
                        gt = self.w.items.get((pr["batch"], pr["name"])) or {}
                        n = slots if gt.get("timing") else max(1, int(sp.get("cpus", 2)) // 2)
                        busy += n
                        if gt.get("klass") != "filler":
                            real += n
                p["busy"] += min(slots, busy)
                p["busy_real"] = p.get("busy_real", 0) + min(slots, real)
            if v.id not in owners:
                p["orphans"].append(v.id)
        q = {}
        for (bid, name), it in self.w.items.items():
            if it["submitted"] is not None and it["delivered"] is None and it["running"] == 0 and self.w.batches[bid]["t_end"] is None:
                c = "real" if it["klass"] != "filler" else "filler"
                q.setdefault(it["image"], {"real": 0, "filler": 0})[c] += 1
        return {"t": round(now - self.k.t0, 1), "per": per, "queued": q}

    # -- running
    def run(self, seconds: float, real_limit: float = 300.0):
        try:
            self.k.run(self.k.t0 + seconds, real_limit)
        finally:
            self.k.shutdown()

    def peer(self, code_dir: str, machine: str, **kw):
        """An independent real scheduler/client install sharing only Boat and the virtual clock."""
        return Sim(code_dir, shared=self, machine=machine, **kw)

    def cleanup(self):
        for peer in self.peers:
            shutil.rmtree(peer.tmp, ignore_errors=True)
