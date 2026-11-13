"""supercomputer: one job runner over boat.dev VMs (a warm pool shared by every repo on this machine), RunPod pods
(GPU jobs) and Modal (the fallback of both), run from any repo.

    supercomputer --skill    the agents' guide (SKILL.md), live values filled in
    supercomputer run --jobs <jobs.json> --out <dir> [--backend auto|boat|modal|runpod] [--env K=V] ...
    supercomputer status | starts [--since 24h] | reap | cold | teardown [--all] | image [--build] | usage <since>
    supercomputer demand <client> [--for 300] [--pid P] [--done]
    supercomputer queue add <jobs.json> [--tree DIR] [--name N] | ls | results <name>
    supercomputer batch fetch <id> --out <dir>
    supercomputer treadmill run [...]  |  digest [--day D]
    supercomputer store push|serve|pull|verify|ls|unserve ...
    supercomputer volumes inventory|copy|verify|ls ...
    supercomputer runpod status|reap

The repo is the current directory's git root (or --repo); its supercomputer.toml says what it needs (config.py).
"""
import argparse
import json
import os
import pathlib
import sys

from . import config


def _run(a):
    from . import boat, controller, fanout, treadmill
    strict_once = getattr(a, "strict_once", False)
    if strict_once and a.backend not in ("auto", "boat"):
        raise SystemExit("--strict-once requires the native boat backend")
    if strict_once and getattr(a, "max_attempts", 1) != 1:
        raise SystemExit("--strict-once requires --max-attempts 1")
    if not strict_once:
        fanout.reexec_with_modal()
    controller.maybe_sync()
    p = config.project()
    spec = json.loads(pathlib.Path(a.jobs).read_text())
    pre = p.cfg.get("run", {}).get("pre", "") + "".join(f"export {kv}; " for kv in a.env)
    cmd = lambda c: c
    mod = p.modal.get("module")
    app, mjobs = None, None
    from .rules import fallback
    if not strict_once and fallback.needs_modal(a.backend, any(j.get("backend") == "modal" or j.get("image") and j["image"] not in p.images for j in spec)):
        try:
            if mod and (p.root / mod).exists():
                m = treadmill.import_real(p.root / mod)
                app, mjobs = m.app, (lambda js, on_done: m.modal_jobs(js, pre, cmd, on_done))
            else:
                from . import modal_run as m
                app, mjobs = m.app, (lambda js, on_done: m.modal_jobs(js, pre, cmd, on_done))
        except ImportError as e:  # (no Modal client: boat only)
            boat.log(f"(no Modal here: {e})")
    if a.sc_class:
        os.environ["SUPERCOMPUTER_CLASS"] = a.sc_class
    if a.timing:
        os.environ["SUPERCOMPUTER_TIMING"] = "1"
    if a.data:
        os.environ["SUPERCOMPUTER_DATA"] = ",".join(str(pathlib.Path(x.split(":")[0]).resolve()) + (":" + x.split(":", 1)[1] if ":" in x else "") for x in a.data)
    if a.urgent:
        os.environ["SUPERCOMPUTER_URGENT"] = "1"
        boat.URGENT = True
    if a.vm_type:  # (a type named: every job placed on that type only, rules.placement_type)
        os.environ["SUPERCOMPUTER_VM_NAMED"] = a.vm_type
    a.vm_type = a.vm_type or os.environ.get("SUPERCOMPUTER_VM") or "large"
    fanout.run_jobs(app, spec, a.out, pre, cmd, mjobs, backend=a.backend, vms=a.vms, idle=None if a.idle < 0 else a.idle, teardown=a.teardown,
                    durations=a.durations, vm_type=a.vm_type, max_vms=a.max_vms, max_wait=a.max_wait,
                    strict_once=strict_once, max_attempts=getattr(a, "max_attempts", 1))


def _remote_queue(a):
    """The queue on the controller (its treadmill): a submission's jobs and tree go there."""
    import io
    import tarfile
    import time
    from . import boat, controller, scheduler
    controller.maybe_sync()
    c = scheduler.connect()
    if not getattr(c, "remote", None):
        raise SystemExit("the controller doesn't answer: `supercomputer queue` needs it (or SUPERCOMPUTER_CONTROLLER=0 for this machine's)")
    if a.qcmd == "add":
        spec = json.loads(pathlib.Path(a.jobs).read_text())
        root = config.select_root(a.tree)
        tar = boat.tree_tarball(root)
        name = a.name or f"{root.name}-{pathlib.Path(a.jobs).stem}-{time.strftime('%m%d-%H%M%S')}"
        import hashlib
        import subprocess
        meta = {**dict(kv.split("=", 1) for kv in getattr(a, "meta", []) or []), "name": name, "tree": str(root), "sha": subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
                "tree_key": hashlib.sha256(tar).hexdigest()[:16], "jobs": len(spec), "t": time.time(), "status": "queued"}
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tf:
            for n, data in (("tree.tar.gz", tar), ("jobs.json", json.dumps(spec, indent=1).encode())):
                ti = tarfile.TarInfo(n)
                ti.size = len(data)
                tf.addfile(ti, io.BytesIO(data))
        import urllib.parse
        c.req("/queue/add?meta=" + urllib.parse.quote(json.dumps(meta)), None, timeout=600, body=buf.getvalue())
        print(f"queued {name} on the controller: {len(spec)} jobs from {root}; results: supercomputer queue results {name}")
    elif a.qcmd == "ls":
        r = c.req("/queue/ls", {}, timeout=60)
        st = r.get("status", {})
        print(f"the treadmill on the controller: pid {st.get('pid')}, {len(st.get('batches', []))} batches, elastic {st.get('elastic')}")
        for m in r["subs"]:
            print(f"  {m['name']:48} {m['status']:8} {m['jobs']:4} jobs  {time.strftime('%m-%d %H:%M', time.localtime(m['t']))}")
    else:
        r = c.req("/queue/results", {"name": a.name}, timeout=120)
        m = r["meta"]
        print(f"{a.name}: {m['status']}, {m['jobs']} jobs" + (f", done in {round(m['wall_s'])} s" if m.get("wall_s") else ""))
        print(r["summary"], end="")


def _health(a) -> int:
    """`supercomputer health`: the scheduler's /health (health.py); from a scheduler older than it, what its status says.
    --watch S: silent unless the breached targets change (a finding shows once, and once more when it clears)."""
    import time
    if a.watch:
        import contextlib
        import io
        last = None
        while True:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = _health(argparse.Namespace(minutes=a.minutes, json=False, watch=0))
            bad = tuple(l for l in buf.getvalue().splitlines() if l.strip().startswith("BAD"))
            if bad != last:
                print(time.strftime("%H:%M"), "health:", "ok" if rc == 0 else "BREACHED", *bad, sep="\n  ", flush=True)
                last = bad
            time.sleep(a.watch)
    from . import health, scheduler
    c = scheduler.current()
    if c is None:
        print("health: no scheduler running (nothing to check)")
        return 0
    t0 = time.time()
    try:
        r = c.req("/health", {"minutes": a.minutes}, timeout=20)
        if not r.get("targets"):
            raise RuntimeError("no /health")
    except Exception:
        st = c.req("/status", {}, timeout=60)
        took = time.time() - t0
        day = st.get("day", {})
        cls = st.get("classes", {})
        r = {"t": time.time(), "minutes": a.minutes, "targets": {
            "starts": {"what": "VM starts per 100 items placed (today)", "value": day.get("starts_per_100_items"), "limit": "<= 1.0", "detail": day},
            "modal": {"what": "items to Modal today (justified or not: this scheduler predates /health)", "value": sum(v.get("to_modal", 0) for v in cls.values()), "limit": "== 0",
                      "detail": {k: v.get("to_modal") for k, v in cls.items()}},
            "status": {"what": "status's answer, seconds", "value": round(took, 2), "limit": "< 1.0", "detail": {}},
            "env": {"what": "flags a client would need", "value": len(health.env_flags()), "limit": "== 0", "detail": health.env_flags()}}}
        for k, v in r["targets"].items():
            op, lim = v["limit"].split()
            v["ok"] = health._ok(op, v["value"], float(lim))
        r["ok"] = all(v["ok"] for v in r["targets"].values())
        r["note"] = f"scheduler pid {st.get('pid')} (code {st.get('code')}) predates /health: its status only"
    if a.json:
        print(json.dumps(r, indent=1, default=str))
    else:
        print(health.show(r) + (f"\n  ({r['note']})" if r.get("note") else ""))
    return 0 if r.get("ok") else 1


def _reap(a):
    """Modal apps whose client died (their tickets in LIVE), boat leases of dead clients and VMs up past their idle,
    RunPod pods whose client is gone; live ones are listed, never touched."""
    import shutil
    import subprocess
    from . import boat
    live = config.live_dir()
    import socket
    from .rules import ops
    for t in sorted(live.glob("ap-*")):
        pid = int(t.read_text().strip() or 0)
        # (a Modal app's ticket is this machine's: its client's pid checked here; with a host of "" it was presumed
        # alive and no orphan app was ever stopped; rules.ops.reap_modal)
        if not ops.reap_modal(not boat.pid_alive(pid, socket.gethostname())):
            print(f"live: {t.name} (client {pid})")
        else:
            subprocess.run(["modal", "app", "stop", "-y", t.name], capture_output=True)
            t.unlink(missing_ok=True)
            print(f"stopped orphan {t.name}")
    if shutil.which("modal"):
        out = subprocess.run(["modal", "app", "list", "--json"], capture_output=True, text=True).stdout
        try:
            up = [x for x in json.loads(out or "[]") if x["state"] not in ("stopped", "stopping...")]
            print(f"{len(up)} Modal apps up" + "".join(f"\n  {x['app_id']} {x['description']} {x['state']} tasks={x['tasks']}" for x in up))
        except Exception:
            pass
    if os.path.exists(boat.BOAT) or shutil.which("boat"):
        boat.reap()
    if shutil.which("runpodctl"):
        from . import runpod
        runpod.reap()


def main(argv=None):
    if (sys.argv[1:] if argv is None else argv)[:1] in (["--skill"], ["skill"]):  # (the agents' guide, live values filled in)
        from . import skill
        print(skill.text(), end="")
        return
    ap = argparse.ArgumentParser(prog="supercomputer", description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter, epilog=__doc__.split("\n\n", 1)[1])
    ap.add_argument("--repo", default="", help="the repo (default: the current directory's git root)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="a jobs file on boat (GPU jobs on RunPod), Modal for what they can't")
    r.add_argument("--jobs", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--backend", default="auto", choices=["auto", "boat", "modal", "runpod"])
    r.add_argument("--env", action="append", default=[], help="K=V exported before every job's command")
    r.add_argument("--vms", type=int, default=0)
    r.add_argument("--max-vms", type=int, default=0)
    r.add_argument("--idle", type=int, default=-1)
    r.add_argument("--vm-type", default=None, choices=["small", "default", "large", "xlarge"],
                   help="the VM type every job runs on (named: a placement constraint, timing claims need fixed hardware); unnamed, $SUPERCOMPUTER_VM, else large, the smallest that fits taken")
    r.add_argument("--durations", default="")
    r.add_argument("--max-wait", type=int, default=120)
    r.add_argument("--teardown", action="store_true")
    r.add_argument("--urgent", action="store_true", help="extra starts allowed (SUPERCOMPUTER_URGENT=1)")
    r.add_argument("--class", dest="sc_class", default="", choices=["", "interactive", "gate", "submission", "sweep", "filler"],
                   help="the scheduler's priority class (default $SUPERCOMPUTER_CLASS, else interactive for a small batch, submission for a bigger one)")
    r.add_argument("--data", action="append", default=[], help="DIR[:DEST]: a local directory shipped with the tree at DEST (default its name), like Modal's add_local_dir")
    r.add_argument("--strict-once", action="store_true", help="at most one native physical attempt per item; no retry or fallback")
    r.add_argument("--max-attempts", type=int, default=1, help="strict-once physical attempt cap (only 1 is supported)")
    r.add_argument("--timing", action="store_true", help="every job alone on its VM, for a timing claim (or a job's \"timing\": true); placement.json labels it")
    sub.add_parser("status")
    rb = sub.add_parser("rollback", help="hand the pool back to a saved previous code (default: the newest whose canary passed), as the canary does; the install untouched")
    rb.add_argument("code", nargs="?", default="")
    hl = sub.add_parser("health", help="the pool's targets over the last N minutes, from the scheduler's own records (exit 1 on a breach)")
    hl.add_argument("--minutes", type=float, default=30)
    hl.add_argument("--json", action="store_true")
    hl.add_argument("--watch", type=float, default=0, help="check every this many seconds; print only when the set of breached targets changes (the laptop's watcher)")
    sub.add_parser("reap")
    pp = sub.add_parser("policy", help="the scheduler's policy (one file, a generation): show, or set knobs (in force within a tick)")
    pp.add_argument("set", nargs="*", help="k=v ...")
    sub.add_parser("cold")
    t = sub.add_parser("teardown")
    t.add_argument("--all", action="store_true")
    t.add_argument("--type", default="")
    i = sub.add_parser("image")
    i.add_argument("--build", action="store_true")
    u = sub.add_parser("usage")
    u.add_argument("since")
    g = sub.add_parser("starts")
    g.add_argument("--since", default="24h")
    m = sub.add_parser("demand")
    m.add_argument("name")
    m.add_argument("--for", dest="ttl", type=int, default=300)
    m.add_argument("--pid", type=int, default=0)
    m.add_argument("--done", action="store_true")
    q = sub.add_parser("queue")
    qs = q.add_subparsers(dest="qcmd", required=True)
    qa = qs.add_parser("add")
    qa.add_argument("jobs")
    qa.add_argument("--tree", default=None)
    qa.add_argument("--name", default="")
    qa.add_argument("--meta", action="append", default=[], help="k=v: extra metadata kept with the submission")
    qs.add_parser("ls")
    qr = qs.add_parser("results")
    qr.add_argument("name")
    tm = sub.add_parser("treadmill", help="the repo's treadmill (its [treadmill] module), or the queue alone")
    tm.add_argument("args", nargs=argparse.REMAINDER)
    d = sub.add_parser("digest")
    d.add_argument("--day", default="")
    bc = sub.add_parser("cache", help="local blob cache status and reference-aware pruning")
    bc.add_argument("args", nargs=argparse.REMAINDER)
    s = sub.add_parser("store")
    s.add_argument("args", nargs=argparse.REMAINDER)
    v = sub.add_parser("volumes")
    v.add_argument("args", nargs=argparse.REMAINDER)
    rp = sub.add_parser("runpod")
    rp.add_argument("what", choices=["status", "reap"])
    sub.add_parser("migrate", help="move a repo's old pool directory (gdp-wired's supercomputer/.live) into the shared one")
    ct = sub.add_parser("controller", help="the scheduler and the treadmill on a boat VM: setup, status, sync, logs, restart, stop")
    ct.add_argument("args", nargs=argparse.REMAINDER)
    batch = sub.add_parser("batch", help="fetch retained results or recover strict-close evidence; read-only")
    batch_commands = batch.add_subparsers(dest="batch_cmd", required=True)
    fetch = batch_commands.add_parser("fetch", help="download a finished batch by ID; never submit or close")
    fetch.add_argument("id")
    fetch.add_argument("--out", required=True)
    fetch.add_argument("--timeout", type=float, default=120, help="per-request timeout in seconds")
    recover = batch_commands.add_parser("recover-close", help="validate retained strict-close evidence from local custody; read-only")
    recover.add_argument("id")
    recover.add_argument("--timeout", type=float, default=30, help="whole recovery deadline in seconds")
    recover.add_argument("--attempts", type=int, default=3)
    sc = sub.add_parser("scheduler", help="the scheduler: serve (started by clients on demand), status, stop")
    sc.add_argument("args", nargs=argparse.REMAINDER)
    a = ap.parse_args(argv)
    config.use(a.repo or None)
    from . import boat
    if a.cmd == "run":
        _run(a)
    elif a.cmd == "batch":
        from . import batchfetch
        try:
            result = (batchfetch.fetch(a.id, a.out, timeout=a.timeout) if a.batch_cmd == 'fetch'
                      else boat.recover_close(a.id, timeout=a.timeout, attempts=a.attempts))
        except (ValueError, RuntimeError, OSError) as exc:
            raise SystemExit(f"batch fetch: {exc}") from exc
        print(json.dumps(result, indent=2))
        if a.batch_cmd == "recover-close" and not result["cleanup_complete"]:
            raise SystemExit(1)
    elif a.cmd == "status":
        boat.status()
    elif a.cmd == "health":
        sys.exit(_health(a))
    elif a.cmd == "rollback":
        from . import scheduler
        c = scheduler.current()
        if c is None:
            sys.exit("rollback: no scheduler running")
        r = c.req("/rollback", {"code": a.code}, timeout=30)
        print(f"rollback: {r['from']} -> {r['to']}: the scheduler hands the pool back (every VM and item kept)" if r.get("ok") else f"rollback: {r.get('error')}")
        sys.exit(0 if r.get("ok") else 1)
    elif a.cmd == "policy":
        from . import scheduler
        kv = dict(x.split("=", 1) for x in a.set if "=" in x)
        print(json.dumps(scheduler.write_policy(kv) if kv else scheduler.read_policy(), indent=1))
    elif a.cmd == "reap":
        import signal
        signal.signal(signal.SIGALRM, lambda *x: (_ for _ in ()).throw(SystemExit("reap: 180 s passed, stopped (time-boxed)")))
        signal.alarm(180)
        _reap(a)
    elif a.cmd == "cold":
        boat.cold()
    elif a.cmd == "teardown":
        boat.teardown(a.all, a.type)
    elif a.cmd == "image":
        if a.build:
            with boat.signals_as_interrupt(), boat.image_build_lock(boat.image_name()):
                print(boat.ensure_image(True))
        else:
            have = boat.named_snapshots()
            for name, snap, _ in boat.layers():
                ok = have.get(snap, {}).get("status") == "ready" or have.get(boat._ALIAS.get(snap), {}).get("status") == "ready"
                print(f"{name:8} {snap:30} {'ready' if ok else 'missing'}")
    elif a.cmd == "usage":
        import datetime
        tt = datetime.datetime.fromisoformat(a.since.replace("Z", "+00:00")).timestamp()
        ids = list(boat.Registry().read()["vms"])
        print(f"{len(ids)} VMs, ${boat.usage_since(ids, tt)} since {a.since}")
    elif a.cmd == "starts":
        boat.starts_summary(a.since)
        from . import scheduler
        if scheduler.controller():  # (the controller keeps its own ledger: its starts by client and class)
            import time as _t
            c = scheduler.current()
            if c and getattr(c, "remote", None):
                rows = [json.loads(l) for l in c.req("/ledger", {"n": 5000}, timeout=60)["lines"] if l.strip()]
                t0 = _t.time() - (float(a.since[:-1]) * {"m": 60, "h": 3600, "d": 86400}[a.since[-1]] if a.since[-1] in "mhd" else 86400)
                rows = [r for r in rows if r.get("t", 0) >= t0]
                by: dict = {}
                for r in rows:
                    k = f"{r.get('client', '?')} ({r.get('klass', '-')})"
                    e = by.setdefault(k, [0, 0])
                    e[0] += 1
                    e[1] += r.get("starts", 0)
                print(f"the controller's ledger: {len(rows)} batches, {sum(r.get('starts', 0) for r in rows)} starts")
                for k, (n, st) in sorted(by.items()):
                    print(f"  {k:44} {n:4} batches {st:5} starts")
    elif a.cmd == "demand":
        print(f"{a.name}: {len(boat.undeclare(a.name))} VMs kept for it stop in {boat.IDLE} s" if a.done else boat.demand(a.name, a.ttl, a.pid) or f"{a.name}: declared for {a.ttl} s")
    elif a.cmd == "queue" and __import__("supercomputer.scheduler", fromlist=["controller"]).controller():
        _remote_queue(a)
    elif a.cmd in ("queue", "digest", "treadmill"):
        from . import treadmill
        treadmill.use()
        mod = treadmill.repo_module()
        if a.cmd == "queue":
            {"add": lambda: treadmill.submit(a.jobs, a.tree, a.name, dict(kv.split("=", 1) for kv in a.meta)), "ls": treadmill.ls, "results": lambda: treadmill.results(a.name)}[a.qcmd]()
        elif a.cmd == "digest":
            (getattr(mod, "digest", None) or treadmill.digest)(a.day or treadmill.today())
        else:
            if mod and hasattr(mod, "main"):
                mod.main(a.args)
            else:
                ta = argparse.ArgumentParser(prog="supercomputer treadmill")
                ta.add_argument("cmd", choices=["run", "status"])
                ta.add_argument("--vms", type=int, default=4)
                ta.add_argument("--hours", type=float, default=0)
                ta.add_argument("--budget-vmh", default="auto")
                ta.add_argument("--max-vms", type=int, default=0)
                ta.add_argument("--step-vms", type=int, default=2)
                ta.add_argument("--backlog-hours", type=float, default=2.0)
                ta.add_argument("--quiet", type=int, default=2700)
                ta.add_argument("--no-modal", action="store_true")
                ta.add_argument("--max-wait", type=int, default=900)
                x = ta.parse_args(a.args)
                if x.cmd == "status":
                    print((treadmill.TM / "status.json").read_text() if (treadmill.TM / "status.json").exists() else "not running")
                else:
                    treadmill.Treadmill(x.vms, x.budget_vmh, x.quiet, not x.no_modal, x.max_wait, x.max_vms, x.step_vms, x.backlog_hours).run(x.hours)
    elif a.cmd == "cache":
        from . import blobcache
        blobcache.main(a.args)
    elif a.cmd == "store":
        from . import store
        store.main(a.args)
    elif a.cmd == "volumes":
        from . import volumes
        volumes.main(a.args)
    elif a.cmd == "runpod":
        from . import runpod
        if a.what == "reap":
            runpod.reap()
        else:
            print(runpod.rp("pod", "list", "--all"))
    elif a.cmd == "controller":
        from . import controller
        controller.main(a.args)
    elif a.cmd == "scheduler":
        from . import scheduler
        scheduler.main(a.args)
    elif a.cmd == "migrate":
        from . import migrate
        migrate.main()


if __name__ == "__main__":
    main()
