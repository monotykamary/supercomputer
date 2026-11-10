"""The scenario suite: the real scheduler and client, the agents' own rules, against a simulated boat on a virtual
clock (tests/sim/). Each scenario replays an incident (or a hard case of the pool's life) and asserts the targets
(tests/sim/metrics.py) that bind for it. Each runs in its own process, all at once; about a minute on this laptop.

    python3 tests/scenarios.py                       # every scenario, against this checkout's code
    python3 tests/scenarios.py tree_gc kill9         # some
    python3 tests/scenarios.py --code DIR            # against another checkout (an older commit: does it fail?)
    python3 tests/scenarios.py --json                # the numbers

A scenario in XFAIL fails on today's code for a bug reported (its owner, its scenario): the suite passes while it
fails, and says so when it passes (then it leaves XFAIL). Exit 0 when every other scenario passes.
"""
import argparse
import concurrent.futures
import contextlib
import json
import os
import pathlib
import subprocess
import sys
import time

HERE = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

# scenario -> why it is expected to fail on today's code (the bug, reported to Lane S)
XFAIL: dict[str, str] = {
}


def _sim(code, seed=1, **kw):
    from tests.sim.harness import Sim
    return Sim(code, seed=seed, **kw)


def _warm(sim, n, image=None):
    from tests.sim.harness import IMAGE
    return [sim.w.warm_vm(image or IMAGE) for _ in range(n)]


def _busy_vms(sim, n):
    """n VMs running items now (boat's pick: the busiest first)."""
    vs = sorted([v for v in sim.w.vms.values() if v.state == "running"], key=lambda v: -sum(1 for p in v.procs.values() if p["alive"]))
    return vs[:n]


S: dict = {}


def scenario(fix: str, want: list, seconds: float, about: str):
    def deco(f):
        S[f.__name__] = {"fn": f, "fix": fix, "want": want, "seconds": seconds, "about": about}
        return f
    return deco


@scenario("11-08 legacy rollback/reload", ["lost", "clients", "orphans", "strikes", "modal", "threads"], 1600,
          "an old legacy admission with running items and null rollback placement records survives a switch; every original run keeps its worker process")
def legacy_admission_handoff(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 4)
    # Optional immutable source tree for the forensic, real old->new check.
    # The normal suite still tests this checkout's legacy protocol and switch.
    old_code = os.environ.get("SC_LEGACY_BASELINE")
    if old_code:
        from tests.sim.world import load_code
        sim.w.add_code(load_code(old_code, sim.tmp, "legacy_code"), "c0")
        sim.w.add_code(sim.code, "c1")
    sim.scheduler()
    bid = sim.batch("legacy-trackK", "gate", items("k", 12, 600), after=10)
    before = {}

    def old_rollback_artifact():
        # Old _take_pool lost last_placement and _start_on_inner wrote null.
        q = sim.S.DIR / "queue" / f"{bid}.placed"
        q.write_text('null\n' * 5 + q.read_text())
        for v in sim.w.vms.values():
            for key, p in v.procs.items():
                if p["alive"]:
                    before[(v.id, key)] = p
        sim.w.install("c1")
    sim.at(100, old_rollback_artifact)

    def extra(sim, m):
        m["original_running"] = len(before)
        m["run_counts"] = {name: it["runs"] for (batch_id, name), it in sim.w.items.items() if batch_id == bid}
        return {"had_running_items": bool(before), "no_restart": all(n == 1 for n in m["run_counts"].values())}
    return sim, {"extra": extra}

@scenario("11-08 native admission barrier", ["lost", "clients", "orphans", "strikes", "modal", "threads"], 1600,
          "the new decoder refuses one live legacy admission: native canary rejects before any worker starts, original code takes back the pool without killing or restarting an item")
def unclassified_admission_handoff(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 4)
    bid = sim.batch("unclassified-trackK", "gate", items("k", 12, 600), after=10)
    original = sim.S.Batch.__init__

    def decode(self, msg):
        if sim.k.current_proc().env.get("code") == "c1" and msg.get("batch") == bid:
            raise ValueError("new decoder cannot classify legacy admission")
        original(self, msg)
    sim.S.Batch.__init__ = decode
    sim.scheduler()
    sim.at(100, lambda: sim.w.install("c1"))

    def extra(sim, m):
        cs = json.loads((sim.S.DIR / "canary.json").read_text())
        m["run_counts"] = {name: it["runs"] for (batch_id, name), it in sim.w.items.items() if batch_id == bid}
        m["scheduler_codes"] = [p.env.get("code") for p, s in sim.w.sched_procs if s is not None]
        m["canary_rejected"] = cs.get("rejected", [])
        return {"native_rejected": "c1" in m["canary_rejected"],
                "one_replacement": m["scheduler_codes"] == ["c0", "c1", "c0"],
                "no_restarts": all(n == 1 for n in m["run_counts"].values())}
    return sim, {"extra": extra}

@scenario("11-08 native canary holds voluntary capacity", ["orphans", "clients", "threads"], 1100,
          "an above-floor idle VM is retained throughout the native canary, causes no rejection, and is released normally only after the pass")
def canary_holds_idle_capacity(code):
    sim = _sim(code, policy={"floor_default": 4, "floor_idle_s": 3600, "hold_s": 30})
    _warm(sim, 4)
    sim.scheduler(idle_exit=2000)
    sim.at(100, lambda: sim.w.install("c1"))
    observed = {}

    def idle_above_floor():
        live = sim.w.live_sched()
        observed["candidate_at_policy_change"] = live[0].env.get("code") if live else None
        observed["held_at_policy_change"] = sorted(live[1].vms) if live else []
        p = pathlib.Path(sim.tmp) / "policy.json"
        policy = json.loads(p.read_text())
        policy.update(generation=policy["generation"] + 1, floor_default=1)
        p.write_text(json.dumps(policy))
    sim.at(180, idle_above_floor)

    def before_verdict():
        live = sim.w.live_sched()
        observed["held_pending"] = sorted(live[1].vms) if live else []
        observed["pending"] = json.loads((sim.S.DIR / "canary.json").read_text())
    sim.at(700, before_verdict)

    def extra(sim, m):
        cs = json.loads((sim.S.DIR / "canary.json").read_text())
        releases = [{"t": t, "process": pr, "line": line} for t, pr, line in sim.w.log_lines
                    if ": released" in line and t >= 180]
        watch_start = observed.get("pending", {}).get("t", sim.k.t0 + 160) - sim.k.t0
        premature = [e for e in releases if e["t"] < watch_start + 600]
        m["canary_hold"] = {**observed, "final": cs, "releases": releases}
        return {"candidate_observed": observed.get("candidate_at_policy_change") == "c1",
                "holds_four_while_pending": len(observed.get("held_pending", [])) == 4 and observed.get("pending", {}).get("state") == "watching",
                "no_premature_release": not premature,
                "native_passed": cs.get("state") == "passed" and "c1" in cs.get("passed", []) and "c1" not in cs.get("rejected", []),
                "normal_release_resumes": bool(releases)}
    return sim, {"extra": extra}

# ------------------------------------------------------------------------------------------- the incidents
@scenario("cfae955", ["modal", "strikes", "lost", "clients", "threads"], 4800,
          "boat stops two VMs under running items, a third later: one strike per VM, the items retried elsewhere, none to Modal")
def one_vm_one_strike(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 6)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 48, 500)))
    sim.at(300, lambda: [sim.w.stop(v, "boat") for v in _busy_vms(sim, 2)])
    sim.at(900, lambda: [sim.w.stop(v, "boat") for v in _busy_vms(sim, 1)])
    return sim, {}


@scenario("acac7d0", ["modal", "strikes", "lost", "clients", "threads"], 2400,
          "the first starts never come up and the breaker opens; a gate waits through it with max_wait 60: no strike for the scheduler's own wait")
def breaker_wait(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"never_ready_until": 500})
    sim.scheduler()
    sim.at(1, lambda: sim.batch("bar-1", "gate", items("g", 40, 300), max_wait=0, on_lost=False))
    sim.at(560, lambda: sim.batch("bar-2", "gate", items("h", 8, 120), max_wait=60))
    return sim, {}


@scenario("029a7ed", ["churn", "strikes", "modal", "lost", "clients", "threads"], 2400,
          "the agents of two busy VMs die, boat reports the VMs up: repaired in place, never lost and resumed")
def agent_down(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 6)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 48, 600)))

    def kill_agents():
        for v in _busy_vms(sim, 2):
            v.daemon = "down"
            sim.w.note("daemon_died", vm=v.id)
    sim.at(200, kill_agents)
    return sim, {}


@scenario("cfae955", ["churn", "strikes", "modal", "orphans", "lost", "clients", "threads"], 2400,
          "eight free VMs up with agents and work queued at once: adoption and acquisition both reach for them, each brought up once")
def duplicate_bringup(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.at(0.5, lambda: sim.batch("lane-a", "submission", items("s", 64, 600)))
    sim.scheduler()
    return sim, {}


@scenario("95ec41b; B2 (open)", ["orphans", "lost", "strikes", "modal", "clients", "threads"], 4800,
          "a handoff with 20 VMs busy, three of their agents hung across it: no VM left without a live owner past 10 min")
def orphans_after_handoff(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 20)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 160, 1200)))

    def hang():
        for v in _busy_vms(sim, 3):
            v.daemon = "hung"
        sim.w.install("c1")

    def unhang():
        for v in sim.w.vms.values():
            if v.daemon == "hung":
                v.daemon = "up"
    sim.at(600, hang)
    sim.at(1500, unhang)
    return sim, {}


@scenario("f0a9ae9", ["churn", "orphans", "lost", "clients", "threads"], 4200,
          "a handoff with 16 VMs held between two gate waves (filler running, nothing real queued): the pool kept, no start for the next wave")
def pool_after_handoff(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 16)
    sim.treadmill(base_vms=8, filler_n=64, filler_s=600)
    sim.scheduler()
    sim.at(30, lambda: sim.batch("bar-1", "gate", items("g", 192, 240)))
    sim.at(1300, lambda: sim.w.install("c1"))
    sim.at(1700, lambda: sim.batch("bar-2", "gate", items("h", 192, 240)))

    def extra(sim, m):
        # (the staged drain across the handoff: at most ceil(drain_step x above the floor) VMs released in any window)
        rel = sorted(e["t"] for e in sim.w.events if e["kind"] == "release")
        worst = max((sum(1 for u in rel if t <= u < t + 300) for t in rel), default=0)
        m["releases_in_a_window"] = worst
        return {"drain_staged": worst <= 2}
    return sim, {"extra": extra}


@scenario("f0a9ae9", ["lost", "clients", "threads"], 4800,
          "a runner's calls (est 1 s, 120 s each) in two gates of 160: the second sized from the first's history, done within the gate horizon")
def one_second_estimates(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 1)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("bar-1", "gate", items("call-", 160, 120, est=1, fn="check"), label="bar check"))
    sim.at(2400, lambda: sim.batch("bar-2", "gate", items("call-", 160, 120, est=1, fn="check"), label="bar check"))

    def extra(sim, m):
        rec = [r for b, r in sim.clients.items() if b.startswith("bar-2")][0]
        wall = (rec["t_end"] or sim.k.now) - (rec["t_submit"] or sim.k.now)
        m["gate2_wall_s"] = round(wall)
        return {"gate_wall": wall <= 1500}
    return sim, {"extra": extra}


@scenario("7b8ec72", ["lost", "clients", "threads"], 1800,
          "interactive checks of 30 items of 0.06 s, one after another: every row delivered before its batch is done")
def done_before_read(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 2)
    sim.scheduler()
    for i in range(12):
        sim.at(10 + 60 * i, lambda i=i: sim.batch(f"check-{i}", "interactive", items("q", 30, 0.06)))
    return sim, {}


@scenario("55b3abf", ["strikes", "modal", "lost", "clients", "threads"], 3600,
          "a sweep prepared on 4 VMs, preempted by 8 gates of 8 trees: the agents' tree GC keeps the sweep's tree, no row lost")
def tree_gc(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 4)
    sim.scheduler()
    sim.at(5, lambda: sim.batch("treadmill", "sweep", items("w", 64, 400), tree="sweep"))
    for i in range(8):
        sim.at(300 + 20 * i, lambda i=i: sim.batch(f"bar-{i}", "gate", items(f"g{i}-", 16, 60), tree=f"gate-{i}"))
    return sim, {}


@scenario("191802c", ["busy", "churn", "lost", "clients", "threads"], 3600,
          "one submission client with 400 items and 16 VMs, the treadmill's filler on the rest: every slot to the submission, no VM released")
def share_cap(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 16)
    sim.treadmill(base_vms=8, filler_n=64, filler_s=900)
    sim.scheduler()
    sim.at(60, lambda: sim.batch("lane-a", "submission", items("s", 400, 300)))
    return sim, {"busy_from": 400, "busy_to": 1800}


@scenario("e6410b0", ["status", "lost"], 1800,
          "a client gives up with 24 items running on VMs whose agents hang: status answers at once all the while")
def status_latency(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 6)
    sim.scheduler()
    sim.status_monitor(every=5)
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 48, 900), abandon_after=300))

    def hang():
        for v in _busy_vms(sim, 6):
            v.daemon = "hung"
    sim.at(290, hang)
    sim.at(900, lambda: [setattr(v, "daemon", "up") for v in sim.w.vms.values() if v.daemon == "hung"])
    return sim, {}


@scenario("1226a6f", ["lost", "clients", "threads"], 1800,
          "eight items whose first run dies without a word, retried elsewhere: each item's log is the log of the run that delivered its row")
def logs_of_delivering_run(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 6)
    sim.scheduler()
    its = items("s", 96, 120)
    for it in its[:8]:
        it["sim_die_first"] = True
    sim.at(10, lambda: sim.batch("bar-f", "gate", its, collect=True))

    def extra(sim, m):
        wrong = []
        for bid, rec in sim.clients.items():
            for name, row in rec["items"].items():
                if not row or row.get("lost"):
                    continue
                out = (rec.get("logs") or {}).get(name, {}).get("out", "")
                want = f"run {row.get('run_name', name)} on {row.get('vm')}: started"
                if want not in out or "done code" not in out:
                    wrong.append(f"{name}: row from {row.get('run_name')} on {row.get('vm')}, log: {out.strip()[:80]!r}")
        m["logs_wrong"] = {"n": len(wrong), "examples": wrong[:4]}
        return {"logs": not wrong}
    return sim, {"extra": extra}


@scenario("B5 (open)", ["churn", "orphans", "threads"], 2400,
          "eight VMs above the floor past their hold, three handoffs 60 s apart: the staged drain holds across them (at most 2 released in any 5 min)")
def drain_across_handoff(code):
    sim = _sim(code, policy={"hold_s": 120})
    _warm(sim, 16)
    sim.treadmill(base_vms=8, filler_n=64, filler_s=600)
    sim.scheduler()
    for i, t in enumerate((300, 360, 420)):
        sim.at(t, lambda i=i: sim.w.install(f"c{i + 1}"))

    def extra(sim, m):
        # (the drain's sheds, by every scheduler: at most ceil(0.25 x 8) in any 5 min; a VM let go is never taken back
        # and let go again (re-attach of its preempted filler, adoption for its hold))
        shed = sorted(t for t, p, l in sim.w.log_lines if "min on filler only, above the floor of" in l)
        worst = max((sum(1 for u in shed if t <= u < t + 300) for t in shed), default=0)
        rel = [e["vm"] for e in sim.w.events if e["kind"] == "release"]
        twice = sorted({v for v in rel if rel.count(v) > 1})
        m["sheds_in_a_window"], m["released_twice"] = worst, twice
        return {"drain_staged": worst <= 2, "never_taken_back": not twice}
    return sim, {"extra": extra}


@scenario("canary", ["lost", "clients", "orphans", "churn", "strikes", "modal"], 3000,
          "new code whose status takes 2 s is installed under load: the canary hands the pool back to the previous code within its 10 min, nothing lost, never handed to it again")
def canary_hands_back(code):
    import shutil
    import tempfile
    from tests.sim import world as W
    from tests.sim.harness import items
    sim = _sim(code)
    bad_dir = tempfile.mkdtemp(prefix="sc-bad-")
    shutil.copytree(pathlib.Path(code) / "supercomputer", pathlib.Path(bad_dir) / "supercomputer", ignore=shutil.ignore_patterns("__pycache__"))
    p = pathlib.Path(bad_dir) / "supercomputer" / "scheduler.py"
    src = p.read_text()
    bad = src.replace("            live = [v for v in self.vms.values() if not v.dead]\n            per",
                      "            time.sleep(2)  # (the bad code)\n            live = [v for v in self.vms.values() if not v.dead]\n            per", 1)
    assert bad != src, "the bad code's injection point is gone"
    p.write_text(bad)
    sim.w.add_code(W.load_code(bad_dir, sim.tmp, name="supercomputer_bad"), "bad")
    _warm(sim, 8)
    sim.scheduler()
    sim.status_monitor(every=20)
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 64, 600)))
    sim.at(400, lambda: sim.w.install("bad"))

    def extra(sim, m):
        ran = [(p.env.get("code"), round(p.t_start - sim.k.t0)) for p, s in sim.w.sched_procs if s is not None]
        back = [l for t, pr, l in sim.w.log_lines if "canary" in l and "handed back" in l]
        m["schedulers"], m["handback"] = ran, back[:1]
        t_bad = next((t for c, t in ran if c == "bad"), None)
        t_back = next((t for c, t in ran if c != "bad" and t_bad is not None and t > t_bad), None)
        return {"handed_back": bool(back) and t_back is not None and t_back - t_bad <= 600, "stays_back": ran[-1][0] != "bad",
                "sim_status_breached_then": (m["status"]["status_max_s"] or 0) >= 1.0}
    return sim, {"extra": extra}


@scenario("B3 (this branch)", ["lost", "clients", "modal", "threads"], 2400,
          "one VM held, the first run of 4 items dies there: their retries get another VM (a start), never wait for ever")
def retry_needs_another_vm(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 1)
    sim.scheduler()
    its = items("s", 8, 120)
    for it in its[:4]:
        it["sim_die_first"] = True
    sim.at(10, lambda: sim.batch("lane-a", "submission", its))
    return sim, {}


@scenario("race (this branch)", ["strikes", "modal", "lost", "clients", "threads"], 1800,
          "gates preempt filler on slow VMs (kill_item's preempted row lands 0.3 s late): a preempted item is never struck")
def preempt_race(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"kill_slow_s": 0.3})
    _warm(sim, 4)
    sim.treadmill(base_vms=4, filler_n=16, filler_s=900)
    sim.scheduler()
    sim.at(120, lambda: sim.batch("bar-1", "gate", items("g", 16, 60)))
    return sim, {}


@scenario("B8 (this branch)", ["strikes", "modal", "lost", "clients", "threads"], 1800,
          "a batch of 16 placed 4 to a VM while boat's front and exec fail for 3 s (an agent restarting): its preparation's waiters are not struck")
def prep_waiters(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 4)
    sim.scheduler()
    sim.w.front_down.append((sim.k.t0 + 100.0, sim.k.t0 + 103.0, True))
    sim.at(99.0, lambda: sim.batch("lane-a", "submission", items("s", 16, 120), tree="fresh"))
    return sim, {}


@scenario("B10 (this branch)", ["lost", "clients", "strikes", "threads"], 3600,
          "a 25-min gate item on a pool of 40 busy VMs whose agents answer slowly, three handoffs: it is taken back each time, never run again")
def long_item_across_handoffs(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"rpc_s": 0.8})
    _warm(sim, 40)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 100, 900)))
    sim.at(60, lambda: sim.batch("lane-p", "gate", items("core-tests", 1, 1500, weight=4)))
    def install(i):
        sim.w.install(f"c{i + 1}")
        # (the long item's VM answers nothing for 40 s from the handoff: re-attach's first pass misses it, as on 11-01)
        vm = next((v for v in sim.w.vms.values() if any(p["alive"] and p["batch"].startswith("lane-p") for p in v.procs.values())), None)
        if vm is not None:
            sim.at(sim.k.now - sim.k.t0 + 60, lambda: setattr(vm, "daemon", "hung"))
            sim.at(sim.k.now - sim.k.t0 + 100, lambda: setattr(vm, "daemon", "up"))
    for i, t in enumerate((400, 800, 1200)):
        sim.at(t, lambda i=i: install(i))

    def extra(sim, m):
        runs = [it["runs"] for (b, n), it in sim.w.items.items() if b.startswith("lane-p")]
        again = sum(1 for (b, n), it in sim.w.items.items() if it["runs"] > 1 and not it["boat_vms"])
        # (a weight-4 item alone on its VM: nothing else ran beside it, the slots of what runs there counted)
        shared = 0
        for v in sim.w.vms.values():
            spans = [(p["job"].t, p["job"].mtime if p["job"].done is not None else 1e18, p["batch"].startswith("lane-p")) for p in v.procs.values()]
            shared += sum(1 for a0, a1, mine in spans if mine for b0, b1, other in spans if not other and b0 < a1 and a0 < b1)
        m["long_item_runs"], m["items_run_again"], m["beside_the_whole_vm_item"] = runs, again, shared
        return {"long_item_once": runs == [1], "none_run_again": again == 0, "whole_vm_alone": shared == 0}
    return sim, {"extra": extra}


@scenario("handoff invisible (this branch)", ["lost", "clients", "threads"], 2400,
          "three clients polling across two handoffs: none sees an error or logs one, every row comes")
def handoff_invisible(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.scheduler()
    for i in range(3):
        sim.at(10 + i, lambda i=i: sim.batch(f"lane-{i}", "submission", items(f"c{i}-", 24, 300)))
    sim.at(400, lambda: sim.w.install("c1"))
    sim.at(900, lambda: sim.w.install("c2"))

    def extra(sim, m):
        seen = [f"{t}s {p}: {l[:120]}" for t, p, l in sim.w.log_lines if p.startswith("lane-") and ("didn't answer" in l or "refused" in l or "reset" in l)]
        m["client_error_lines"] = {"n": len(seen), "examples": seen[:3]}
        return {"clients_saw_nothing": not seen}
    return sim, {"extra": extra}


@scenario("B12 (this branch)", ["lost", "clients", "strikes", "threads"], 3000,
          "boat list answers one page of 200 (stopped VMs first) for a pool of 40 up and 190 stopped: no live VM forgotten, every item taken back at a handoff")
def b12_paged_list(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"list_page": 200})
    for _ in range(190):  # (stopped VMs of the pool's image, older: listed first)
        v = sim.w.add_vm("sc-img-sim", up_now=True)
        sim.w.stop(v, "us")
    _warm(sim, 40)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 160, 1200)))
    sim.at(60, lambda: sim.batch("bar-1", "gate", items("g", 400, 120)))  # (growth: acquisition reconciles the registry)
    sim.at(700, lambda: sim.w.install("c1"))

    def extra(sim, m):
        nores = [l for t, p, l in sim.w.log_lines if "no agent recorded" in l]
        again = sum(1 for (b, n), it in sim.w.items.items() if it["runs"] > 1 and not it["boat_vms"])
        m["no_agent_recorded"], m["items_run_again"] = len(nores), again
        return {"agents_kept": not nores, "none_run_again": again == 0}
    return sim, {"extra": extra}


@scenario("B14 (this branch)", ["strikes", "modal", "lost", "clients", "threads"], 2400,
          "boat's front down 20 min (exec answers) and a new tree of 2,000 files whose blobs are there: its manifest (over 128 KB) goes over exec in parts, the batch runs through the outage")
def b14_big_body_by_exec(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 4)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 32, 600), tree="big:2000"))  # (its blobs on every VM before the outage)
    sim.w.front_down.append((sim.k.t0 + 300, sim.k.t0 + 1500, False))
    sim.at(360, lambda: sim.batch("lane-b", "submission", items("t", 16, 120), tree="big:2000:moved"))  # (the same files moved: a new tree, its manifest ~180 KB)

    def extra(sim, m):
        done = [it["delivered"] - sim.k.t0 for (b, n), it in sim.w.items.items() if it["delivered"] and b.startswith("lane-b")]
        m["done_in_outage"] = sum(1 for t in done if t < 1500)
        return {"runs_through_the_outage": m["done_in_outage"] >= 16}
    return sim, {"extra": extra}


@scenario("retire (this branch)", ["lost", "clients", "strikes", "threads"], 3600,
          "an operator retires a busy VM (its tokens leaked): nothing new placed there, its items finish, it is let go and never taken back")
def retire_vm(code):
    from tests.sim.harness import items
    sim = _sim(code)
    vs = _warm(sim, 4)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 64, 300)))
    sim.at(100, lambda: (pathlib.Path(sim.tmp) / "scheduler" / "retire").write_text(vs[0].id + "\n"))

    def extra(sim, m):
        v = vs[0]
        late = [p for p in v.procs.values() if p["job"].t > sim.k.t0 + 102]
        rel = [e for e in sim.w.events if e["kind"] in ("release", "stop") and e["vm"] == v.id]
        m["retired_new_items"], m["retired_let_go"] = len(late), len(rel)
        return {"nothing_new_there": not late, "let_go": bool(rel), "stays_gone": v.state != "running" or not any(e["kind"] == "start" for e in sim.w.events if e.get("vm") == v.id)}
    return sim, {"extra": extra}


@scenario("share vs sweeps (this branch)", ["threads"], 2400,
          "three submission clients of weight-4 items at their share with sweeps queued below them on 16 VMs: the slots past their shares go to them, not to sweeps")
def share_vs_sweeps(code):
    from tests.sim.harness import items
    sim = _sim(code, policy={"sub_vms": 2})
    _warm(sim, 16)
    sim.treadmill(base_vms=8, filler_n=0, sweep_n=64, sweep_s=900)
    sim.scheduler()
    for i in range(3):
        sim.at(60 + i, lambda i=i: sim.batch(f"laneP-{i}", "submission", items(f"w{i}-", 120, 120, weight=4)))
    share = []

    def watch():  # (once the sweeps running at their arrival have ended: the submissions' share of the pool's slots)
        sim.k.sleep(1100)
        while sim.k.now - sim.k.t0 < 2400:
            sub = sum(4 for v in sim.w.vms.values() for p in v.procs.values() if p["alive"] and p["batch"].startswith("laneP"))
            waiting = any(it["submitted"] and it["delivered"] is None and it["running"] == 0 for (b, n), it in sim.w.items.items() if b.startswith("laneP"))
            if waiting:
                share.append(sub / 64)
            sim.k.sleep(30)
    sim.k.start(sim.k.world, watch, name="share watch")

    def extra(sim, m):
        m["submission_share"] = round(sum(share) / len(share), 3) if share else None
        return {"submissions_take_the_sweeps_slots": bool(share) and sum(share) / len(share) >= 0.85}
    return sim, {"extra": extra}


@scenario("stray runs (this branch)", ["lost", "threads"], 1800,
          "a client gives up while its VMs' agents hang (the kills lost): its runs go on after; the scheduler kills runs no item owns, never leaves their slots taken")
def stray_runs(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 6)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 24, 6000), abandon_after=300))
    sim.at(20, lambda: sim.batch("lane-b", "submission", items("t", 48, 300)))

    def hang():
        for v in _busy_vms(sim, 6):
            v.daemon = "hung"
    sim.at(295, hang)
    sim.at(420, lambda: [setattr(v, "daemon", "up") for v in sim.w.vms.values() if v.daemon == "hung"])

    def extra(sim, m):
        stray = sum(1 for v in sim.w.vms.values() for p in v.procs.values() if p["alive"] and p["batch"].startswith("lane-a"))
        m["stray_runs_at_end"] = stray
        return {"strays_killed": stray == 0}
    return sim, {"extra": extra}


@scenario("04:24-04:50 churn (gdp-wired gen/laneR-filler)", ["churn", "threads"], 3600,
          "a submission wave on 40 VMs ends; the treadmill keeps filler queued for the pool's slots, so its VMs run filler through their hold and the gate wave 17 min later finds them held: no start; the digest's per-VM gaps between real work recorded")
def drain_then_wave(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 40)
    sim.treadmill(base_vms=8, filler_s=600, backlog=True)  # (the treadmill keeps filler queued for the pool's slots: 11-01's fix)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-p", "submission", items("p", 160, 600)))
    sim.at(1700, lambda: sim.batch("bar", "gate", items("g", 480, 240)))

    def extra(sim, m):
        rel = [e for e in sim.w.events if e["kind"] == "release"]
        m["released"], m["starts"] = len(rel), sum(1 for e in sim.w.events if e["kind"] == "start")
        ls = [s for p, s in sim.w.sched_procs if s is not None]
        gaps = ls[-1].m.get("vm_gaps", []) if ls else []
        m["vm_gaps"], m["waves"] = len(gaps), ls[-1].m.get("waves") if ls else None
        return {"gaps_recorded": bool(gaps) and min(gaps) >= 60}  # (the digest's per-VM gaps between real work: the hold's test; 11-01 showed none)
    return sim, {"extra": extra}


@scenario("B15 (this branch)", ["threads"], 3600,
          "three clients of weight-4 submissions behind the treadmill's filler and sweeps on 16 VMs: each gets whole VMs set aside, the nearest to empty first")
def wide_behind_filler(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 16)
    sim.treadmill(base_vms=8, filler_s=900, backlog=True, sweep_n=16, sweep_s=900)
    sim.scheduler()
    for i in range(3):
        sim.at(120 + i, lambda i=i: sim.batch(f"laneP-{i}", "submission", items(f"w{i}-", 40, 300, weight=4)))

    def extra(sim, m):
        done = sum(1 for (b, n), it in sim.w.items.items() if b.startswith("laneP") and it["delivered"])
        m["wide_done"] = done
        return {"wide_items_run": done >= 100}
    return sim, {"extra": extra}


@scenario("timing VM reuse (this branch)", ["lost", "clients", "threads", "churn"], 3000,
          "timing items of weight 1 and 2 with free large VMs of the image: each takes a free VM that fits, 0 created, 0 churn")
def timing_free_vms(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 12)
    sim.treadmill(base_vms=8, filler_s=600, backlog=True)  # (the pool held, its VMs on filler: what a timing item may empty)
    sim.scheduler()
    sim.at(300, lambda: sim.batch("lane-j", "gate", items("j", 8, 300, timing=True)))
    sim.at(310, lambda: sim.batch("lane-p", "gate", items("p", 4, 300, timing=True, weight=2)))

    def extra(sim, m):
        made = [e for e in sim.w.events if e["kind"] == "start"]
        m["created"] = len(made)
        return {"no_creation": not made}
    return sim, {"extra": extra}


@scenario("needless, mixed (this branch)", ["lost", "clients", "threads"], 3600,
          "timing items needing a default VM beside a submission that asks for default VMs, free large VMs on filler: the timing items take large ones, the submission's starts are its own; health records no needless start (08:17: 4 recorded, the canary handed back)")
def needless_mixed(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 12)
    sim.treadmill(base_vms=8, filler_s=600, backlog=True)
    sim.scheduler()
    sim.at(300, lambda: sim.batch("lane-p", "gate", items("p", 4, 300, timing=True, weight=2)))
    sim.at(300, lambda: sim.batch("lane-d", "submission", items("d", 16, 600, vm_type="default")))

    def extra(sim, m):
        ls = [s for p, s in sim.w.sched_procs if s is not None]
        h = getattr(ls[-1], "health", None) if ls else None
        nl = sum(e.get("n", 1) for e in h.ev.get("needless", [])) if h else 0
        m["needless_recorded"] = nl
        return {"no_false_needless": nl == 0}
    return sim, {"extra": extra}


@scenario("09:50 close (this branch)", ["lost", "threads"], 1500,
          "a treadmill killed with its batches open; the next one closes them, one by an older record ({\"id\": ...}), one by a cut request (no body) then again: every leftover closed, its runs killed, no request the scheduler fails (09:50: three /close answered KeyError('batch'), 500s counted as its errors)")
def close_old_requests(code):
    from tests.sim import world as W
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.scheduler()
    bids = []
    sim.at(10, lambda: bids.append(sim.batch("treadmill", "submission", items("f", 16, 3000))))
    sim.at(12, lambda: bids.append(sim.batch("treadmill", "submission", items("g", 16, 3000))))
    sim.at(200, lambda: [sim.k.kill_proc(p) for p in list(sim.k.procs.values()) if p.name == "treadmill" and p.alive])
    out = {}

    def leftover():
        c = W.SimSchedClient(sim.w)
        for msg in ({"id": bids[0]}, {}, {"batch": bids[1]}):  # (an older record, a cut request, the retry)
            try:
                out[json.dumps(msg)] = c.req("/close", msg, timeout=30)
            except Exception as e:
                out[json.dumps(msg)] = repr(e)[:120]
    sim.at(300, leftover)

    def extra(sim, m):
        ls = [s for p, s in sim.w.sched_procs if s is not None]
        open_ = [b for b in bids if ls and b in ls[-1].batches]
        alive = sum(1 for v in sim.w.vms.values() for p in v.procs.values() if p["alive"] and p["batch"] in bids)
        failed = getattr(sim.w, "http_500", [])
        m["answers"], m["left_open"], m["runs_alive"], m["http_500"] = out, open_, alive, failed[:3]
        return {"leftovers_closed": bool(bids) and not open_, "their_runs_killed": alive == 0, "no_failed_request": not failed}
    return sim, {"extra": extra}


@scenario("batch id reuse (this branch)", ["lost", "threads"], 2400,
          "one client session, two runs overlapping (the second submitted while the first's items run): each its own batch, every row of both delivered, none lost; the scheduler refuses a submit naming a batch with live items (09:49: a session's later runs replaced its earlier batches)")
def session_overlap(code):
    from tests.sim import world as W
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.scheduler()
    rec = {}
    sim.at(10, lambda: rec.update(r=sim.session("bar-1", "gate", [(0, items("a", 12, 400)), (60, items("b", 12, 200))])))
    probe = {}

    def reuse():  # (a submit under a live batch's id, by hand: refused, the batch kept whole)
        s = rec.get("r", {}).get("s")
        if not s or not getattr(s, "bids", None):
            return
        live = s.bids[0]
        ls = [x for p, x in sim.w.sched_procs if x is not None]
        b = ls[-1].batches.get(live) if ls else None
        if b is None:
            return
        msg = {"batch": live, "klass": b.klass, "client": "bar-1", "label": "x", "image": b.image, "vm_type": "large", "tree": dict(b.tree),
               "ctx_b64": "", "caches": [], "on_lost": True, "max_wait": 0, "warm_only": False, "timing": False, "items": items("z", 2, 60)}
        try:
            probe["answer"] = W.SimSchedClient(sim.w).req("/submit", msg, timeout=30)
        except Exception as e:
            probe["answer"] = repr(e)[:160]
        probe["items_after"] = len(ls[-1].batches[live].items) if live in ls[-1].batches else None
    sim.at(150, reuse)

    def extra(sim, m):
        r0 = rec.get("r", {})
        runs = r0.get("runs", [])
        m["runs"], m["errors"], m["probe"], m["bids"] = runs, r0.get("errors", [])[:3], probe, list(getattr(r0.get("s"), "bids", []) or [])
        whole = len(runs) == 2 and all(r["rows"] == r["items"] and r["lost"] == 0 for r in runs) and not r0.get("errors")
        refused = isinstance(probe.get("answer"), dict) and probe["answer"].get("ok") is False and probe.get("items_after") == 12
        return {"both_runs_whole": whole, "live_batch_kept": refused, "closed": bool(r0.get("closed"))}
    return sim, {"extra": extra}


@scenario("default floor (this branch)", ["churn", "lost", "clients", "threads"], 5400,
          "an image with no treadmill, bursts of work that fit one VM, 15 min apart: its one VM kept between them (the default floor of 1), no start after a release (11:40-11:54: pifovea-img started 3 times)")
def floor_default_bursts(code):
    from tests.sim.harness import items
    sim = _sim(code)
    sim.scheduler()
    for j in range(4):
        sim.at(10 + 1200 * j, lambda j=j: sim.batch(f"proj-{j}", "submission", items(f"b{j}", 4, 300)))

    def extra(sim, m):
        starts = [e for e in sim.w.events if e["kind"] == "start"]
        rel = [e for e in sim.w.events if e["kind"] == "release"]
        m["starts"], m["released"] = len(starts), len(rel)
        return {"one_start": len(starts) <= 1}
    return sim, {"extra": extra}


@scenario("default floor decay (this branch)", ["lost", "clients", "threads"], 3600,
          "one burst on an image with no treadmill, floor_idle_s 1800: its VM kept past the burst, released once the image has had no real work for 1800 s (a project that stopped holds nothing)")
def floor_decay(code):
    from tests.sim.harness import items
    sim = _sim(code, policy={"floor_idle_s": 1800.0})
    sim.scheduler()
    sim.at(10, lambda: sim.batch("proj", "submission", items("b", 4, 300)))

    def extra(sim, m):
        rel = [round(e["t"]) for e in sim.w.events if e["kind"] == "release"]
        m["released_at"] = rel
        return {"kept_then_released": len(rel) == 1 and 1800 + 300 <= rel[0] <= 1800 + 900}
    return sim, {"extra": extra}


@scenario("floor override (this branch)", ["lost", "clients", "threads"], 1800,
          "floor_by_image sets an image's floor to 0 (pifovea-img, finishing up): its VM released once its burst ends, as before the default floor")
def floor_override(code):
    from tests.sim.harness import IMAGE, items
    sim = _sim(code, policy={"floor_by_image": {IMAGE: 0}})
    sim.scheduler()
    sim.at(10, lambda: sim.batch("proj", "submission", items("b", 4, 300)))

    def extra(sim, m):
        rel = [round(e["t"]) for e in sim.w.events if e["kind"] == "release"]
        m["released_at"] = rel
        return {"released_after_burst": len(rel) == 1 and rel[0] <= 900}
    return sim, {"extra": extra}


@scenario("rollback command (this branch)", ["lost", "clients", "orphans", "strikes", "modal", "threads"], 2400,
          "code c1 installed and its canary passed, `supercomputer rollback` hands the pool back to c0's saved copy: every item kept, c1 never handed the pool again")
def rollback_command(code):
    from tests.sim import world as W
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 64, 600)))
    sim.at(100, lambda: sim.w.install("c1"))
    out = {}
    sim.at(900, lambda: out.update(r=W.SimSchedClient(sim.w).req("/rollback", {})))

    def extra(sim, m):
        ran = [(p.env.get("code"), round(p.t_start - sim.k.t0)) for p, s in sim.w.sched_procs if s is not None]
        m["schedulers"], m["rollback"] = ran, out.get("r")
        return {"rolled_back": bool(out.get("r", {}).get("ok")) and ran[-1][0] == "c0", "stays": all(c != "c1" for c, t in ran if t > 905)}
    return sim, {"extra": extra}


@scenario("worker pool (this branch)", ["lost", "clients", "threads"], 1500,
          "200 VMs busy: the scheduler's threads stay bounded (a fixed pool of pollers), not one per VM")
def threads_bounded(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 200)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 800, 600)))
    seen = []

    def count():
        sim.k.sleep(400)
        for _ in range(10):
            ls = sim.w.live_sched()
            if ls:
                seen.append(len([t for t in ls[0].threads if not t.done]))
            sim.k.sleep(60)
    sim.k.start(sim.k.world, count, name="thread count")

    def extra(sim, m):
        m["scheduler_threads_max"] = max(seen) if seen else None
        return {"threads_bounded": bool(seen) and max(seen) <= 60}
    return sim, {"extra": extra}


@scenario("stray lag (this branch)", ["lost", "threads"], 1500,
          "gate waves preempt filler on 16 VMs, each kill landing 40 s after it's asked (a loaded VM's sudo), a handoff among them, every third start's reply lost (boat's front failing mid-reply: the run started, its item placed again): runs show on their VMs, no item owning them, till their kill lands; health never calls those slots stray (07:38: the target flapped on a kill's lag), and no unowned run outlives its kill")
def stray_lag(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"kill_slow_s": 40, "lose_start_reply": [(300, 1200, 3)]})
    _warm(sim, 16)
    sim.scheduler()
    sim.treadmill(base_vms=16, filler_n=64, filler_s=3000)
    for j in range(8):
        sim.at(200 + 120 * j, lambda j=j: sim.batch(f"bar-{j}", "gate", items(f"g{j}", 24, 60)))
    sim.at(530, lambda: sim.w.install("c1"))  # (a handoff under it: the new scheduler re-attaches what runs)
    late = []

    def watch():  # (ground truth: a run on a VM that no item owns, alive past its kill's lag, STRAY_S)
        first = {}
        while True:
            sim.k.sleep(10)
            ls = [s for p, s in sim.w.sched_procs if s is not None]
            if not ls:
                continue
            s = ls[-1]
            for v in sim.w.vms.values():
                sv = s.vms.get(v.id)
                for p in v.procs.values():
                    if not p["alive"]:
                        continue
                    run = p["job"].spec["name"]
                    key = (v.id, p["batch"], run)
                    owned = sv is not None and (p["batch"], run) in sv.running
                    if owned:
                        first.pop(key, None)
                    elif sim.k.now - first.setdefault(key, sim.k.now) > 130:
                        late.append(key)
    sim.k.start(sim.k.world, watch, name="stray watch")

    def extra(sim, m):
        ls = [s for p, s in sim.w.sched_procs if s is not None]
        h = getattr(ls[-1], "health", None) if ls else None
        seen = max((sum(p.get("stray", 0) for p in e["per"].values()) for e in h.ev["sample"]), default=0) if h else 0
        m["stray_slots_max"], m["unowned_past_kill"] = seen, sorted(set(late))[:5]
        return {"no_stray_flap": seen == 0, "no_lasting_stray": not late}
    return sim, {"extra": extra}


@scenario("start after close (this branch)", ["lost", "clients", "threads"], 1200,
          "preparations take a minute; a client gives up while its batch is being prepared on the VMs it was placed on: none of its items starts after it closed (no run for nothing), every VM within its CPUs")
def start_after_close(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"prep_s": 60})
    _warm(sim, 8)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("quit", "submission", items("q", 24, 600), abandon_after=30))
    sim.at(20, lambda: sim.batch("lane-b", "submission", items("b", 24, 300)))
    over = []

    def watch():
        while True:
            sim.k.sleep(5)
            for v in sim.w.vms.values():
                used = sum(p["job"].cpus for p in v.procs.values() if p["alive"])
                if used > v.cpus:
                    over.append((v.id, used, v.cpus))
    sim.k.start(sim.k.world, watch, name="cpu watch")

    def extra(sim, m):
        rec = next(r for b, r in sim.clients.items() if b.startswith("quit"))
        late = [p["job"].spec["name"] for v in sim.w.vms.values() for p in v.procs.values()
                if p["batch"].startswith("quit") and rec["t_end"] is not None and p["job"].t > rec["t_end"] + 1]
        m["started_after_close"], m["oversubscribed"] = late[:5], over[:3]
        return {"none_after_close": not late, "within_cpus": not over}
    return sim, {"extra": extra}


# --------------------------------------------------------------------------------- the pool's hard cases
@scenario("7b8ec72", ["churn", "orphans", "lost", "strikes", "modal", "clients", "threads", "status"], 3600,
          "two handoffs under load: 24 VMs, two submission clients, a gate, the treadmill; nothing lost, no VM let go")
def handoff_under_load(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 24)
    sim.treadmill(base_vms=8, filler_n=48, filler_s=600)
    sim.scheduler()
    sim.status_monitor(every=20)
    sim.at(20, lambda: sim.batch("lane-a", "submission", items("a", 160, 400)))
    sim.at(40, lambda: sim.batch("lane-b", "submission", items("b", 160, 300)))
    sim.at(600, lambda: sim.batch("bar-1", "gate", items("g", 120, 90)))
    sim.at(700, lambda: sim.w.install("c1"))
    sim.at(1500, lambda: sim.w.install("c2"))
    return sim, {}


@scenario("6ea38a3", ["lost", "strikes", "modal", "orphans", "clients", "threads"], 3000,
          "kill -9 of the scheduler with 12 VMs busy: the next client's poll starts it again, it takes everything back")
def kill9(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 12)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 96, 600)))
    sim.at(30, lambda: sim.batch("bar-1", "gate", items("g", 48, 120)))
    sim.at(700, sim.w.kill9)
    return sim, {}


@scenario("a581e86", ["lost", "strikes", "modal", "orphans", "clients", "threads"], 6600,
          "the laptop sleeps 20 min with 10 VMs busy (their TTLs run out): on waking nothing is struck, nothing goes to Modal")
def laptop_sleep(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 10)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 80, 900)))
    sim.at(600, lambda: sim.w.laptop_sleep(1200))
    return sim, {}


@scenario("7b8ec72", ["strikes", "modal", "lost", "clients", "threads"], 3000,
          "twelve batches of twelve trees at once over 8 VMs: trees pulled from peers, no row lost")
def many_trees(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.scheduler()
    for i in range(12):
        sim.at(10 + i, lambda i=i: sim.batch(f"lane-{i}", "submission", items(f"t{i}-", 16, 200), tree=f"tree-{i}"))
    return sim, {}


@scenario("1924d80", ["churn", "orphans", "lost", "clients", "threads", "status"], 6000,
          "a gate burst of 600 over the treadmill's sweeps and filler, then a drain: grown for the gate, shed in steps after the hold, no start after a release")
def gate_burst_drain(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.treadmill(base_vms=8, filler_n=48, filler_s=600, sweep_n=16, sweep_s=600)
    sim.scheduler()
    sim.status_monitor(every=30)
    sim.at(300, lambda: sim.batch("bar-1", "gate", items("g", 600, 90)))

    def extra(sim, m):
        held_end = sum(p["vms"] for p in sim.samples[-1]["per"].values()) if sim.samples else 0
        peak = max((sum(p["vms"] for p in s["per"].values()) for s in sim.samples), default=0)
        m["vms_peak"], m["vms_end"] = peak, held_end
        return {"grew": peak > 8, "drained": held_end <= 10}
    return sim, {"extra": extra}


@scenario("7b8ec72", ["busy", "starts", "lost", "clients", "threads"], 3600,
          "four submission clients and a gate at once on 16 VMs: every slot busy with real work, few starts")
def several_clients(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 16)
    sim.scheduler()
    for i in range(4):
        sim.at(10 + 5 * i, lambda i=i: sim.batch(f"lane-{i}", "submission", items(f"c{i}-", 160, 240)))
    sim.at(600, lambda: sim.batch("bar-1", "gate", items("g", 80, 60)))
    return sim, {"busy_from": 120, "busy_to": 2000}


@scenario("ce7af1e", ["churn", "strikes", "modal", "lost", "clients", "threads"], 3000,
          "boat's front 502s for 10 min (boat exec still answers), then exec fails too for 5 min: no VM lost, nothing struck")
def front_failing(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 10)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-a", "submission", items("s", 80, 600)))
    sim.w.front_down.append((sim.k.t0 + 300, sim.k.t0 + 900, False))
    sim.w.front_down.append((sim.k.t0 + 900, sim.k.t0 + 1200, True))
    return sim, {}


@scenario("7b8ec72", ["lost", "strikes", "modal", "clients", "threads"], 2400,
          "twelve timing items among the treadmill's filler on 8 VMs: each alone on an emptied VM")
def timing_items(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.treadmill(base_vms=8, filler_n=32, filler_s=600)
    sim.scheduler()
    sim.at(120, lambda: sim.batch("bench", "gate", items("tm", 12, 120, timing=True)))

    def extra(sim, m):
        shared = 0
        for v in sim.w.vms.values():
            spans = [(p["job"].t, p["job"].mtime, p["job"].spec.get("timing")) for p in v.procs.values()]
            for a0, a1, ta in spans:
                if ta and any(b0 < a1 and a0 < b1 and (b0, b1) != (a0, a1) for b0, b1, _ in spans):
                    shared += 1
        m["timing_shared"] = shared
        return {"timing_alone": shared == 0}
    return sim, {"extra": extra}


# ------------------------------------------------------------------------- allocation (R2, 11-01 13:25-13:40)
REAL = ("interactive", "gate", "submission")


def _waste_watch(sim, grace: float = 90.0, every: float = 10.0, t_from: float = 0.0):
    """The ground truth of health's waste: every `every` s, the slots idle or on filler (a sweep's too, against a gate)
    on VMs up while real items that fit there have waited past `grace` (each slot counted once, the waiters in class
    order). Returns the record the scenario's extra reads: {"samples", "slot_s", "worst"}."""
    from tests.sim import world as W
    rec = {"samples": 0, "slot_s": 0.0, "worst": 0, "waste_samples": 0}
    since: dict = {}

    def watch():
        sim.k.sleep(t_from)
        while True:
            now = sim.k.now
            waiters = []
            for key, it in sim.w.items.items():
                queued = it["submitted"] is not None and it["delivered"] is None and it["running"] == 0 and sim.w.batches[key[0]]["t_end"] is None
                if not queued:
                    since.pop(key, None)
                    continue
                since.setdefault(key, now)
                if it["klass"] in REAL and now - since[key] >= grace:
                    waiters.append(it)
            cap = []
            for v in sim.w.vms.values():
                if v.state != W.RUNNING or not (v.daemon == "up" or v.t_daemon is not None):
                    continue
                slots = max(1, v.cpus // 2)
                used = fil = swp = 0
                for pr in v.procs.values():  # (a run ended within the last 5 s is its report on its way: not idle yet)
                    j = pr["job"]
                    if pr["alive"] or (j.done is not None and not j.acked and now - j.mtime < 5):
                        gt = sim.w.items.get((pr["batch"], pr["name"])) or {}
                        n = slots if gt.get("timing") else max(1, int(pr["job"].spec.get("cpus", 2)) // 2)
                        used += n
                        # A terminal, unacked lower run still holds capacity, but is not
                        # preemptible work: its replacement may already hold these slots.
                        fil += n if pr["alive"] and gt.get("klass") == "filler" else 0
                        swp += n if pr["alive"] and gt.get("klass") == "sweep" else 0
                cap.append([v, max(0, slots - used), fil, swp, slots])
            snap = [f"{c[0].id[-4:]}:idle{c[1]}+f{c[2]}+s{c[3]}" for c in cap if c[1] + c[2] + c[3]][:12]
            idle_ids = [c[0].id for c in cap if c[1]][:3]
            waste = 0
            for it in sorted(waiters, key=lambda it: REAL.index(it["klass"])):
                for c in cap:
                    v, idle, fil, swp, slots = c
                    if v.image != it["image"] or (it.get("vm_type") and v.type != it["vm_type"]):
                        continue
                    avail = idle + fil + (swp if it["klass"] != "submission" else 0)
                    need = slots if it["timing"] else it.get("weight", 1)
                    if avail < need or (it["timing"] and avail < slots):
                        continue
                    waste += need
                    for i in (1, 2, 3):
                        d = min(c[i], need)
                        c[i] -= d
                        need -= d
                    break
            if waste and len(rec.setdefault("examples", [])) < 6:
                ex_w = {}
                for it in waiters:
                    k = f'{it["klass"]}/w{it.get("weight", 1)}{"/timing" if it["timing"] else ""}'
                    ex_w[k] = ex_w.get(k, 0) + 1
                rec["examples"].append({"t": round(now - sim.k.t0), "waste": waste, "waiters": ex_w,
                                        "vms": snap, "sched": _sched_view(sim, idle_ids)})
            rec["samples"] += 1
            rec["slot_s"] += waste * every
            rec["worst"] = max(rec["worst"], waste)
            rec["waste_samples"] += 1 if waste else 0
            sim.k.sleep(every)
    sim.k.start(sim.k.world, watch, name="waste watch")
    return rec


def _sched_view(sim, vids: list) -> list:
    """The scheduler's own view of some VMs (a waste example's): what it holds there, what it set aside."""
    ls = [s for p, s in sim.w.sched_procs if s is not None]
    if not ls:
        return []
    out = []
    for vid in vids:
        v = ls[-1].vms.get(vid)
        if v is None:
            out.append(f"{vid[-4:]}: not held")
            continue
        r = getattr(v, "reserved", None)
        rs = f"rsv {r[0].name} {r[0].state} need {r[2] if len(r) > 2 else '?'} for {round(r[1] - sim.k.now)} s" if r else "no rsv"
        out.append(f"{vid[-4:]}: used {v.used()}/{v.slots} foreign {getattr(v, 'foreign', 0)} ready {v.ready} drain {v.draining} pf {getattr(v, 'prep_fails', 0)} "
                   f"{[(x.batch.klass, x.state, x.weight, x.run_name, round(sim.k.now - (x.t_start or 0)), _gt(sim, vid, x)) for x in v.running.values()]} {rs}")
    return out


def _gt(sim, vid: str, x) -> str:
    """The world's word on a run the scheduler holds: its process on the VM, alive or done."""
    v = sim.w.vms.get(vid)
    ps = [pr for pr in v.procs.values() if pr["batch"] == x.batch.id and pr["job"].spec.get("name") == x.run_name] if v else []
    if not ps:
        bd = v.disk.batches.get(x.batch.id) if v else None
        return f"no proc (batch dir {'yes' if bd else 'no'}, job {'yes' if bd and x.run_name in bd.jobs else 'no'})"
    return "alive" if ps[-1]["alive"] else f"dead done={ps[-1]['job'].done is not None} acked={ps[-1]['job'].acked}"


def _truth_waste(sim, every: float = 15.0) -> dict:
    """The ground truth of health's waste, from the world alone (boat's VMs, their processes, the clients' items), by the
    same definition: every `every` s, the slots of each VM idle or on filler (a sweep's too, for a gate or an
    interactive check) that real work waiting past the policy's sub_preempt_s fits (image, VM type, usable memory; a
    submission past its share while another waits held back, as the policy says), counted only when so at the measure
    before too and no real item started on it between; the mean over the measures with real work queued. Every
    scenario's run measures it beside health's (run_one: health_waste_agrees)."""
    from tests.sim import world as W
    rec = {"n": 0, "sum": 0.0, "worst": 0, "examples": []}
    real = ("interactive", "gate", "submission", "sweep")
    since: dict = {}
    prev: dict = {}
    t_prev = [None]

    def watch():
        while True:
            sim.k.sleep(every)
            now = sim.k.now
            B = sim.code.boat
            ls = sim.w.live_sched()
            if ls is None or now < sim.k.frozen_until:  # (no scheduler to place anything: a switch, the laptop asleep; health
                prev.clear()  # measures nothing then either)
                t_prev[0] = None
                continue
            sched = ls[1]
            starting = {}  # (slots the scheduler gave an item whose process isn't up yet: its tree, its start on the way
            held_by = {}  # (boat's RPCs take seconds), a lower item being killed for one (its kill's word on the way))
            placed = set()  # (items the scheduler has placed, their process not up yet: no longer waiting for a slot)
            for sv in list(getattr(sched, "vms", {}).values()):
                held_by[sv.id] = {(x.batch.id, x.run_name): x.weight for x in list(sv.running.values())}
                placed |= {(x.batch.id, x.name) for x in list(sv.running.values())}
            rf = pathlib.Path(sim.tmp) / "scheduler" / "retire"
            retired = set(rf.read_text().split()) if rf.exists() else set()  # (an operator's: nothing placed there)
            grace = float(getattr(sim.S, "SUB_PREEMPT", 90.0))
            sub_share = int(getattr(sim.S, "SUB_VMS", 8)) * 4
            run_by: dict = {}
            caps: dict = {}
            started: set = set()
            ready = {vid for vid, sv in list(getattr(sched, "vms", {}).items()) if sv.ready and not sv.dead}
            for v in sim.w.vms.values():
                if v.state != W.RUNNING or v.daemon != "up" or v.id in retired or v.id not in ready:
                    continue  # (a VM still coming up, not the scheduler's yet: nothing can go there)
                if v.disk.free_mb < 10240 and v.disk.free_mb * 10 < v.disk.total_mb:
                    continue  # (its disk full: an item there fails its preparation, no space left on device)
                slots = max(1, v.cpus // 2)
                used, fil, swp = 0, 0, 0
                alive_runs = set()
                for pr in v.procs.values():
                    j = pr["job"]
                    gt = sim.w.items.get((pr["batch"], pr["name"])) or {}
                    if t_prev[0] is not None and j.t > t_prev[0] and gt.get("klass") in real:
                        started.add(v.id)
                    if pr["alive"] or (j.done is not None and not j.acked and now - j.mtime < 5):
                        alive_runs.add((pr["batch"], j.spec.get("name")))
                        n = slots if gt.get("timing") else max(1, int(j.spec.get("cpus", 2)) // 2)
                        used += n
                        # A terminal, unacked lower run still holds capacity, but is not
                        # preemptible work: its replacement may already hold these slots.
                        fil += n if pr["alive"] and gt.get("klass") == "filler" else 0
                        swp += n if pr["alive"] and gt.get("klass") == "sweep" else 0
                        if gt.get("klass") == "submission" and pr["alive"]:
                            run_by[gt.get("client")] = run_by.get(gt.get("client"), 0) + n
                used += sum(w for key, w in held_by.get(v.id, {}).items() if key not in alive_runs)
                caps[v.id] = [v, max(0, slots - used), fil, swp, slots]
            waiters = []
            for key, it in sim.w.items.items():
                queued = it["submitted"] is not None and it["delivered"] is None and it["running"] == 0 and sim.w.batches[key[0]]["t_end"] is None
                if not queued or key in placed:
                    since.pop(key, None)
                    continue
                since.setdefault(key, now)
                if it["klass"] in real and now - since[key] >= grace:
                    waiters.append(it)
            any_real = any(it["klass"] in real and it["submitted"] is not None and it["delivered"] is None and it["running"] == 0
                           for it in sim.w.items.values())
            others = lambda c: any(w["klass"] in ("interactive", "gate") or (w["klass"] == "submission" and w["client"] != c and run_by.get(w["client"], 0) < sub_share) for w in waiters)
            cur: dict = {}
            for it in sorted(waiters, key=lambda it: real.index(it["klass"])):
                need = it.get("weight", 1)
                if it["klass"] == "submission" and run_by.get(it["client"], 0) + need > sub_share and others(it["client"]):
                    continue
                for vid, c in caps.items():
                    v, idle, fil, swp, slots = c
                    if v.image != it["image"] or (it.get("vm_type") and v.type != it["vm_type"]):
                        continue
                    if it.get("memory") and not B.R.fits_memory(int(it["memory"]), B.TYPES.get(v.type, (0, 0))[1], B.MEM_OVERHEAD):
                        continue
                    avail = idle + fil + (swp if it["klass"] in ("interactive", "gate") else 0)
                    want = slots if it["timing"] else need
                    if avail < want or slots < need:  # (a VM too small for it: never its)
                        continue
                    cur[vid] = cur.get(vid, 0) + want
                    for i in (1, 2, 3):
                        d = min(c[i], want)
                        c[i] -= d
                        want -= d
                    break
            w = sum(min(n, prev.get(vid, 0)) for vid, n in cur.items() if vid not in started)
            prev.clear()
            prev.update(cur)
            t_prev[0] = now
            if any_real:
                rec["n"] += 1
                rec["sum"] += w
                rec["worst"] = max(rec["worst"], w)
                if w and len(rec["examples"]) < 4:
                    rec["examples"].append({"t": round(now - sim.k.t0), "waste": w, "vms": {vid[-4:]: (n, caps[vid][0].type) for vid, n in cur.items() if vid not in started and prev.get(vid)}})
    sim.k.start(sim.k.world, watch, name="truth waste")
    return rec


def _waits(sim, prefix: str) -> dict:
    """The waits (submitted to first run) of the items of clients named `prefix`...: p50, p95, max, and how many ran."""
    ws = sorted(it["first_run"] - it["submitted"] for (b, n), it in sim.w.items.items() if it["client"].startswith(prefix) and "first_run" in it)
    pct = lambda q: round(ws[min(len(ws) - 1, int(q * len(ws)))], 1) if ws else None
    return {"ran": len(ws), "p50": pct(0.5), "p95": pct(0.95), "max": round(ws[-1], 1) if ws else None}


@scenario("R2: 13:25-13:40 ghost reservations", ["lost", "clients", "threads"], 2400,
          "a 2-slot submission's short items beside the treadmill's filler, then a gate of 2-slot and 1-slot items: VMs set aside for 2-slot items that ran elsewhere stay closed to every item no longer (11-01: 15 VMs, 60 slots idle 15 min while 1,500 gate items waited)")
def ghost_reservation(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 12)
    sim.treadmill(base_vms=8, filler_n=16, filler_s=1800)
    sim.scheduler()
    sim.at(30, lambda: sim.batch("tree-trace", "submission", items("s", 600, 4, weight=2)))
    sim.at(150, lambda: sim.batch("bar-1", "gate", items("w", 120, 60, weight=2)))
    sim.at(155, lambda: sim.batch("bar-1", "gate", items("g", 900, 40)))
    rec = _waste_watch(sim, t_from=150)

    def extra(sim, m):
        m["waste"] = {**rec, "mean": round(rec["slot_s"] / 10 / rec["samples"], 2) if rec["samples"] else None}
        m["gate_waits"] = _waits(sim, "bar-")
        return {"no_idle_behind_reservations": rec["samples"] > 0 and rec["slot_s"] / 10 / rec["samples"] <= 0.5}
    return sim, {"extra": extra}


@scenario("R2: wide items beside gates", ["lost", "clients", "threads"], 3600,
          "every VM runs a long gate item beside filler; a gate of 2-slot items and a submission come: the 2-slot items get VMs set aside beside the gates, and the submission takes the filler's slots after sub_preempt_s (before: no candidate VM, and the first that found none ended every preemption after it)")
def wide_beside_gates(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.treadmill(base_vms=8, filler_s=1800, backlog=True)
    sim.scheduler()
    sim.at(5, lambda: sim.batch("bar-a", "gate", items("a", 8, 500, weight=3)))  # (one a VM, filler in the 4th slot)
    sim.at(200, lambda: sim.batch("bar-b", "gate", items("b", 8, 2800)))  # (each takes a VM's filler slot: a gate on every VM)
    sim.at(800, lambda: sim.batch("bar-c", "gate", items("c", 6, 300, weight=2)))
    sim.at(805, lambda: sim.batch("lane-d", "submission", items("d", 64, 300)))
    rec = _waste_watch(sim, t_from=800)

    def extra(sim, m):
        m["waste"] = {**rec, "mean": round(rec["slot_s"] / 10 / rec["samples"], 2) if rec["samples"] else None}
        m["wide_waits"], m["sub_waits"] = _waits(sim, "bar-c"), _waits(sim, "lane-d")
        w, d = m["wide_waits"], m["sub_waits"]
        return {"wide_placed": w["ran"] == 6 and w["max"] <= 400, "submission_takes_filler": d["ran"] >= 60 and d["p50"] <= 600,
                "no_filler_while_real_waits": rec["samples"] > 0 and rec["slot_s"] / 10 / rec["samples"] <= 0.5}
    return sim, {"extra": extra}


@scenario("R2: a share against another image", ["lost", "threads"], 3000,
          "a submission client at its share on its image's pool of filler while a gate of another image waits for a VM that never comes: the submission takes the filler's slots (before: any waiting gate, of any image, held it at its share)")
def share_other_image(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"never_ready_until": 10 ** 6}, policy={"sub_vms": 1})
    _warm(sim, 8)
    sim.treadmill(base_vms=8, filler_s=1800, backlog=True)
    sim.scheduler()
    sim.at(30, lambda: sim.batch("pi-other", "gate", items("o", 4, 60), image="sc-img-other"))
    sim.at(60, lambda: sim.batch("lane-s", "submission", items("s", 240, 300)))
    rec = _waste_watch(sim, t_from=60)

    def extra(sim, m):
        m["waste"] = {**rec, "mean": round(rec["slot_s"] / 10 / rec["samples"], 2) if rec["samples"] else None}
        m["sub_waits"] = _waits(sim, "lane-s")
        return {"no_filler_while_real_waits": rec["samples"] > 0 and rec["slot_s"] / 10 / rec["samples"] <= 0.5}
    return sim, {"extra": extra}


@scenario("R2: 11:09-12:21 prepared, dropped, struck", ["strikes", "modal", "lost", "clients", "threads"], 2400,
          "a submission prepared on 4 VMs waits behind a gate; the agents drop its quiet directory (their GC after 30 min quiet, here sooner); its items placed there again: prepared again, never struck (11-01: 5,748 strikes with no reason, 1,107 items to Modal)")
def prep_dropped(code):
    from tests.sim.harness import items
    sim = _sim(code, policy={"max_vms": 4})  # (no growth: the gate waits on the pool, as the 11-01 pool was busy)
    _warm(sim, 4)
    sim.scheduler()
    bid = []
    sim.at(5, lambda: bid.append(sim.batch("laneP3", "submission", items("p", 240, 60), tree="lane")))
    sim.at(90, lambda: sim.batch("bar-1", "gate", items("g", 16, 600), tree="g1"))

    def gc():  # (the agents' BatchDirs.drop: a batch with no live item there, quiet past keep_s)
        for v in sim.w.vms.values():
            bd = v.disk.batches.get(bid[0])
            if bd is not None and not any(v.procs.get(j.pid, {}).get("alive") for j in bd.jobs.values() if j.pid is not None):
                del v.disk.batches[bid[0]]
                sim.w.note("batch_dropped", vm=v.id, batch=bid[0])
    sim.at(500, gc)

    def extra(sim, m):
        m["dropped_on"] = sum(1 for e in sim.w.events if e["kind"] == "batch_dropped")
        return {"dropped": m["dropped_on"] >= 2}
    return sim, {"extra": extra}


@scenario("R2: 10:35-14:25 a 2-slot timing item and 1-slot VMs", ["lost", "clients", "threads"], 1800,
          "small VMs on filler and large ones busy with long gates; a sweep's 2-slot timing item: no 1-slot VM is set aside for it (it never fits there), it runs (11-01: speed-tip-0 set small VMs aside 58 times, killing their filler, never placed)")
def timing_too_small(code):
    from tests.sim.harness import items
    sim = _sim(code)
    from tests.sim.harness import IMAGE
    for _ in range(4):
        sim.w.warm_vm(IMAGE, vm_type="small")
    for _ in range(2):
        sim.w.warm_vm(IMAGE, vm_type="default")
    sim.treadmill(base_vms=8, filler_s=1800, backlog=True)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("bar-long", "gate", items("g", 4, 1500, vm_type="default")))  # (the VMs it fits, busy with a gate)
    sim.at(120, lambda: sim.batch("treadmill", "sweep", items("tip", 1, 60, timing=True, weight=2), tree="sweep"))

    def extra(sim, m):
        ls = [s for p, s in sim.w.sched_procs if s is not None]
        aside = sum(1 for t, p, msg in sim.w.log_lines if "set aside for a timing item" in msg)
        m["timing_set_asides"] = aside
        ran = any(it["client"] == "treadmill" and it["klass"] == "sweep" and it["delivered"] for it in sim.w.items.values())
        return {"timing_item_ran": ran, "no_vm_set_aside_it_cannot_fit": aside <= 2}
    return sim, {"extra": extra}


@scenario("R2: 14:30 the handoff that dropped the pool", ["lost", "clients", "threads", "strikes", "modal"], 2100,
          "a pool of 32 VMs (submissions running, filler, some idle), a gate burst queued, then a new install: the new scheduler holds every VM at once, no VM without a live owner, no slot idle beside waiting gate work past a tick (11-01 14:33: 43 of 225 held, 176 with no live owner for 3 min)")
def handoff_1430(code):
    from tests.sim.harness import items
    sim = _sim(code, cfg={"rpc_s": 2.0})  # (an agent's answer through boat's front: seconds, as live)
    _warm(sim, 32)
    sim.treadmill(base_vms=8, filler_s=900, backlog=True)
    sim.scheduler()
    sim.at(20, lambda: sim.batch("lane-s", "submission", items("s", 48, 1400)))
    sim.at(595, lambda: sim.batch("bar-1", "gate", items("g", 900, [20, 35, 50, 65])))  # (slots freeing all the while)
    sim.at(600, lambda: sim.w.install("c1"))

    def slow():  # (a few agents slow to answer as the switch comes (15:21: the old one's ticks in flight held it 25 s))
        vs = [v for v in sim.w.vms.values() if v.state == "running"][:8]
        for v in vs:
            v.daemon = "hung"
        sim.at(sim.k.now - sim.k.t0 + 40, lambda: [setattr(v, "daemon", "up") for v in vs])
    sim.at(656, slow)
    rec = {"orphans_max": 0, "orphan_s": 0.0, "held_min": None, "held_before": None, "samples": 0}

    def watch():
        sim.k.sleep(590)
        ls = sim.w.live_sched()
        rec["held_before"] = len([v for v in ls[1].vms.values() if not v.dead]) if ls else None
        while sim.k.now - sim.k.t0 < 1100:
            owners = sim.held_by()
            up = [v for v in sim.w.vms.values() if v.state == "running" and (v.daemon == "up" or v.t_daemon is not None)]
            orph = [v.id for v in up if v.id not in owners or owners[v.id] == "released"]
            rec["orphans_max"] = max(rec["orphans_max"], len(orph))
            rec["orphan_s"] += 2.0 * len(orph)
            ls = sim.w.live_sched()
            if ls and sim.k.now - sim.k.t0 > 605:
                held = len([v for v in ls[1].vms.values() if not v.dead])
                rec["held_min"] = held if rec["held_min"] is None else min(rec["held_min"], held)
            rec["samples"] += 1
            sim.k.sleep(2.0)
    sim.k.start(sim.k.world, watch, name="handoff watch")
    waste = _waste_watch(sim, grace=15.0, every=5.0, t_from=600)

    def extra(sim, m):
        m["handoff"] = rec
        m["waste"] = {**{k: v for k, v in waste.items() if k != "examples"}, "examples": waste.get("examples", [])[:2]}
        # (the switch's pause: from the old scheduler's last pass (its handoff) to the new one holding the pool, nobody
        # places; 15:21:34-15:21:59 live, 25 s, the old one joining its pollers' ticks to agents slow to answer)
        t_off = next((t for t, p, msg in sim.w.log_lines if "handing the pool over" in msg), None)
        t_on = next((t for t, p, msg in sim.w.log_lines if t_off is not None and t >= t_off and ("took over" in msg or "re-attached" in msg)), None)
        m["switch_pause_s"] = round(t_on - t_off, 1) if t_off is not None and t_on is not None else None
        return {"no_orphan": rec["orphans_max"] == 0, "pool_held_whole": rec["held_min"] is not None and rec["held_before"] is not None and rec["held_min"] >= rec["held_before"],
                "placement_never_pauses_a_tick": m["switch_pause_s"] is not None and m["switch_pause_s"] < 15}
    return sim, {"extra": extra}


@scenario("R2: d783ef9 Lean checks asking a large VM's whole memory", ["lost", "clients", "threads"], 2400,
          "a pool of large VMs on filler; a gate of 4 jobs asking 16384 MB (the bar's Lean checks): none runs on a large VM, whose 16 GB less its OS can't hold it; each on a VM type that can (xlarge), started for them (11-01: 5 of 48 OOM-killed on large VMs)")
def lean_memory(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.treadmill(base_vms=8, filler_s=900, backlog=True)
    sim.scheduler()
    sim.at(60, lambda: sim.batch("bar-lean", "gate", items("lean", 4, 300, weight=4, memory=16384)))

    def extra(sim, m):
        on = [v.type for v in sim.w.vms.values() for p in v.procs.values() if p["batch"].startswith("bar-lean")]
        m["lean_vm_types"] = on
        return {"ran": len(on) >= 4, "never_on_a_vm_too_small": bool(on) and all(t == "xlarge" for t in on)}
    return sim, {"extra": extra}


@scenario("R2: 16:06 waste of the instant", ["lost", "clients", "threads"], 1800,
          "2,400 gate items of 5-12 s over the treadmill's filler on 16 VMs (items ending every instant): every slot placed within a pass, the ground truth's waste 0, and health's waste 0 with it (16:06-16:10: 1-2 slots freed between passes counted, the canary handing a sound code back)")
def churn_waste(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 16)
    sim.treadmill(base_vms=8, filler_s=900, backlog=True)
    sim.scheduler()
    sim.at(60, lambda: sim.batch("bar-c", "gate", items("c", 2400, [5, 7, 9, 12])))
    rec = _waste_watch(sim, grace=90.0, every=10.0, t_from=60)

    def extra(sim, m):
        ls = [s for p, s in sim.w.sched_procs if s is not None]
        hv = ls[-1].health_check(25)["targets"].get("waste", {}).get("value") if ls else None
        m["health_waste"], m["truth_waste_slot_s"] = hv, rec["slot_s"]
        return {"truth_none": rec["slot_s"] <= 40, "health_agrees": hv is not None and hv == 0}
    return sim, {"extra": extra}


@scenario("R2: 16:26 a gate behind a sweep's reservation", ["lost", "threads"], 1800,
          "one VM full of a gate's long items, another of sweeps and filler set aside for a sweep's waiting 2-slot item; a gate of 2-slot items comes: it takes the sweep VM's slots at once (16:26:54, bx_2jse889x: the gate item waited, the VM held for the sweep's)")
def gate_behind_sweep_reservation(code):
    from tests.sim.harness import items
    sim = _sim(code, policy={"max_vms": 2})
    _warm(sim, 2)
    sim.treadmill(base_vms=2, filler_s=1800, backlog=True)
    sim.scheduler()
    sim.at(5, lambda: sim.batch("bar-long", "gate", items("a", 4, 1700)))
    sim.at(30, lambda: sim.batch("treadmill-sw", "sweep", items("w", 2, 1700)))
    sim.at(100, lambda: sim.batch("treadmill-sw2", "sweep", items("v", 2, 300, weight=2)))

    def hold():  # (the sweep's 2-slot item waiting, the VM of sweeps held for it as on 11-01)
        ls = [s for p, s in sim.w.sched_procs if s is not None][-1]
        with ls.lock:
            it = next((x for b in ls.batches.values() if b.client == "treadmill-sw2" for x in b.items.values() if x.state == "queued"), None)
            vm = next((v for v in ls.vms.values() if any(x.batch.client == "treadmill-sw" for x in v.running.values())), None)
            if it is not None and vm is not None:
                vm.reserved, it.rsv_vm = (it, sim.S.time.time() + 900, 2), vm
    sim.at(150, hold)
    sim.at(160, lambda: sim.batch("bar-g", "gate", items("g", 2, 300, weight=2)))

    def extra(sim, m):
        w = _waits(sim, "bar-g")
        m["gate_waits"] = w
        return {"gate_takes_the_sweeps": w["ran"] == 2 and w["max"] is not None and w["max"] <= 120}
    return sim, {"extra": extra}


@scenario("R2: 17:10 a memory-blind memo", ["lost", "threads"], 1800,
          "large VMs full of a gate's long items, small VMs free; a gate client's 4 GB items (no small VM holds them) queued ahead of another's 1-slot items of no stated memory: those run on the small VMs at once (17:10-18:47: small VMs empty, or on a sweep, beside waiting 1-slot gate items)")
def memo_memory(code):
    from tests.sim.harness import items, IMAGE
    sim = _sim(code, policy={"max_vms": 4})
    _warm(sim, 2)
    for _ in range(2):
        sim.w.warm_vm(IMAGE, vm_type="small")
    sim.scheduler()
    sim.at(2, lambda: sim.batch("bar-c", "gate", items("s", 2, 100)))  # (on the small VMs: the fullest first)
    sim.at(5, lambda: sim.batch("bar-c", "gate", items("c", 8, 1500)))  # (the large VMs, full)
    sim.at(50, lambda: sim.batch("bar-a", "gate", items("a", 4, 300, memory=4096)))
    sim.at(60, lambda: sim.batch("bar-c", "gate", items("d", 2, 300)))  # (bar-c's next: 10 running, so after bar-a's in the queue)

    def extra(sim, m):
        late = [it["first_run"] - sim.k.t0 for (b, n), it in sim.w.items.items() if it["client"] == "bar-c" and n.startswith("d") and "first_run" in it]
        m["c_late_starts"] = sorted(round(x) for x in late)
        return {"fitting_items_not_passed_over": len(late) == 2 and max(late) < 250}
    return sim, {"extra": extra}


@scenario("R2: 19:32 three clients past their shares", ["lost", "threads"], 2400,
          "three submission clients of 300 items each on 6 VMs with the treadmill's sweeps: their shares don't bind (no one waits under its own), so their work is the plan's demand and the pool grows for it (19:32: 2,400 items, waits p50 25 min, want 0)")
def subs_past_shares(code):
    from tests.sim.harness import items
    sim = _sim(code, policy={"sub_vms": 1})
    sim.S.BREAK_EVEN_OVERRIDE_S = 900  # replay this incident's explicit old horizon, not the production default
    _warm(sim, 6)
    sim.treadmill(base_vms=6, filler_n=0, sweep_n=24, sweep_s=600)
    sim.scheduler()
    for i in range(3):
        sim.at(60 + i, lambda i=i: sim.batch(f"chc-{i}", "submission", items(f"p{i}-", 300, 120)))

    def extra(sim, m):
        w = _waits(sim, "chc-")
        m["sub_waits"], m["starts"] = w, sum(1 for e in sim.w.events if e["kind"] == "start")
        return {"grew": m["starts"] > 0, "waits_minutes": w["p50"] is not None and w["p50"] < 900}
    return sim, {"extra": extra}


@scenario("R2: 20:38 released for a moment's empty filler queue", ["churn", "lost", "threads"], 2400,
          "a submission wave on 16 VMs ends while the treadmill's filler is small; a gate wave 4 minutes later: the VMs within their hold are kept for the treadmill's filler (seconds away), no start after a release (20:38:15: 39 VMs let go 2 minutes past their real work, the next wave's starts and resumes 2-8 minutes later, churn 3)")
def release_filler_gap(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 16)
    sim.treadmill(base_vms=4, filler_n=8, filler_s=60)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("lane-p", "submission", items("p", 64, 400)))
    sim.at(1100, lambda: sim.batch("bar-w", "gate", items("w", 256, 300)))

    def extra(sim, m):
        m["released"] = sum(1 for e in sim.w.events if e["kind"] == "release")
        m["started"] = sum(1 for e in sim.w.events if e["kind"] == "start")
        return {"no_start_after_release": not (m["released"] and m["started"])}
    return sim, {"extra": extra}


@scenario("R2: 20:46 reuse before create, across types", ["lost", "threads"], 1800,
          "large VMs on filler and a submission asking default VMs (4 vCPU, 4 GB): the free large VMs take it before any start of a default one (20:46: three default VMs started)")
def reuse_larger(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 8)
    sim.treadmill(base_vms=2, filler_s=900, backlog=True)
    sim.scheduler()
    sim.at(200, lambda: sim.batch("lane-d", "submission", items("d", 16, 600, weight=2, memory=4096), ))

    def setup_type():  # (the batch's own type default: its plan's group, as a jobs file of 4-vCPU jobs asks)
        ls = [s for p, s in sim.w.sched_procs if s is not None][-1]
        for b in ls.batches.values():
            if b.client == "lane-d":
                b.vm_type = "default"
                for it in b.items.values():
                    it.need_type = "default"
    sim.at(201, setup_type)

    def extra(sim, m):
        m["started"] = [e.get("vm") for e in sim.w.events if e["kind"] == "start"]
        on = {v.type for v in sim.w.vms.values() for p in v.procs.values() if p["batch"].startswith("lane-d")}
        m["lane_d_on"] = sorted(on)
        return {"no_start_beside_free_larger": not m["started"], "ran_on_large": "large" in on}
    return sim, {"extra": extra}


@scenario("R2: lane Y's named type", ["lost", "clients", "threads"], 2400,
          "large VMs on filler and two default VMs; a timing batch of 30 jobs naming the default type: every job on a default VM, alone, the plan starting default VMs for the rest (lane Y's IC3Syn figures: large 74, default 32, xlarge 10)")
def named_type(code):
    from tests.sim.harness import items, IMAGE
    sim = _sim(code)
    _warm(sim, 8)
    for _ in range(2):
        sim.w.warm_vm(IMAGE, vm_type="default")
    sim.treadmill(base_vms=8, filler_s=900, backlog=True)
    sim.scheduler()
    sim.at(60, lambda: sim.batch("lane-y", "gate", items("f", 30, 120, timing=True, weight=2), vm_type="default", vm_named=True))

    def extra(sim, m):
        on = [v.type for v in sim.w.vms.values() for p in v.procs.values() if p["batch"].startswith("lane-y")]
        m["lane_y_on"] = {t: on.count(t) for t in set(on)}
        return {"all_on_the_named_type": len(on) >= 30 and set(on) == {"default"}}
    return sim, {"extra": extra}


@scenario("R2: 11-02 full disks", ["lost", "clients", "threads"], 1800,
          "two of eight VMs at 1.5 GB free, one with 100 GB of anvil's state older than any job, one with nothing to clean; the treadmill's filler and a submission: nothing placed on either while low, the first cleaned and working again, the second deleted once empty and never reused (11-02 05:35-09:00: VMs at 99% took items, 649 prepares failed)")
def full_disks(code):
    from tests.sim.harness import items
    sim = _sim(code)
    vs = _warm(sim, 8)
    for v in vs[:2]:
        v.disk.free_mb = 1500
    vs[0].disk.scratch = [(sim.k.now - 3600, 100_000)]
    sim.treadmill(base_vms=8, filler_s=900, backlog=True)
    sim.scheduler()
    sim.at(60, lambda: sim.batch("lane-q", "submission", items("q", 48, 300)))

    def extra(sim, m):
        late = lambda v, t: [p for p in v.procs.values() if p["job"].t > sim.k.t0 + t]
        m["full_vm_items"] = len(late(vs[1], 40))
        m["cleaned_vm_items"], m["cleaned_free_mb"] = len(late(vs[0], 300)), vs[0].disk.free_mb
        m["full_vm_state"] = vs[1].state
        return {"nothing_on_the_full_vm": not late(vs[1], 40), "full_vm_deleted": vs[1].state == "deleted",
                "cleaned_vm_works_again": bool(late(vs[0], 300)) and vs[0].disk.free_mb > 10240}
    return sim, {"extra": extra}


@scenario("R2: 09:25/09:55 timing submissions beside filler-only VMs", ["lost", "clients", "threads"], 2400,
          "the treadmill's filler on 12 large and 2 xlarge VMs; laneQ's 40 timing items of 4 slots and 14 GB, and another client's 1-slot items: each laneQ item within its share gets a filler-only VM at once, never ~30 s later one at a time (09:25:32, 09:55:56: an xlarge and a large on filler beside them, health's waste 0.3)")
def timing_subs_filler(code):
    from tests.sim.harness import items, IMAGE
    sim = _sim(code)
    _warm(sim, 12)
    for _ in range(2):
        sim.w.warm_vm(IMAGE, vm_type="xlarge")
    sim.treadmill(base_vms=14, filler_s=900, backlog=True)
    sim.scheduler()
    sim.at(60, lambda: sim.batch("laneQ", "submission", items("q", 40, 240, timing=True, weight=4, memory=14336)))
    sim.at(61, lambda: sim.batch("laneR", "submission", items("r", 200, 120)))

    def back_off():  # (laneQ's first waiting item's preparation failed on an agent that didn't answer: 60 s back, no strike)
        while True:
            sim.k.sleep(45)
            ls = sim.w.live_sched()
            if ls is None:
                continue
            q = [it for b in ls[1].batches.values() if b.client == "laneQ" for it in b.items.values() if it.state == "queued"]  # (the queue's order)
            if q and q[0].not_before < sim.k.now:
                q[0].not_before = sim.k.now + 60
    sim.k.start(sim.k.world, back_off, name="laneQ back-off")
    truth = _truth_waste(sim)

    def extra(sim, m):
        m["truth"] = {"n": truth["n"], "mean": round(truth["sum"] / truth["n"], 2) if truth["n"] else None, "worst": truth["worst"], "examples": truth["examples"]}
        m["laneQ_waits"] = _waits(sim, "laneQ")
        return {"no_waste_beside_them": truth["n"] > 0 and truth["sum"] / truth["n"] <= 0.1}
    return sim, {"extra": extra}


@scenario("R2: 11-02 12:45 a VM's tree short of its manifest", ["lost", "clients", "threads"], 1200,
          "a VM whose tree of the batch lacks files its manifest names (made short, its ready mark kept: bx_ptwy4b7p, 5 files of tree 9810b283e4ba): the tree dropped there and made again, the batch's items run on that VM too, no strike charged to any item")
def partial_tree(code):
    from tests.sim.harness import items
    sim = _sim(code)
    vs = _warm(sim, 2)
    vs[0].disk.partial_first = True
    sim.scheduler()
    sim.at(30, lambda: sim.batch("lane-p", "submission", items("p", 24, 120), verify=True))

    def extra(sim, m):
        on0 = [p for p in vs[0].procs.values() if p["batch"].startswith("lane-p")]
        ours = sum(it.get("ours", 0) for k, it in sim.w.items.items() if k[0].startswith("lane-p"))
        ls = [s for p, s in sim.w.sched_procs if s is not None]
        strikes = sum(it.fate.fails for b in ls[-1].batches.values() if b.client == "lane-p" for it in b.items.values()) if ls else None
        m["on_short_vm"], m["strikes"] = len(on0), strikes
        return {"runs_there_after_remade": len(on0) >= 4, "no_strike": not strikes}
    return sim, {"extra": extra}


@scenario("R2: 13:25 submissions behind gates and filler", ["lost", "clients", "threads"], 3000,
          "the treadmill's filler on 16 VMs; two submission clients of 2-slot items, then a bar of 2-slot and 1-slot gate items over them: every slot to real work while it waits, the submissions' waits minutes, not hours")
def starvation_1325(code):
    from tests.sim.harness import items
    sim = _sim(code)
    sim.S.BREAK_EVEN_OVERRIDE_S = 900  # this incident used an explicit 900-second growth override
    _warm(sim, 16)
    sim.treadmill(base_vms=8, filler_s=900, backlog=True, sweep_n=32, sweep_s=600)
    sim.scheduler()
    sim.at(60, lambda: sim.batch("tree-trace", "submission", items("t", 240, 30, weight=2)))
    sim.at(61, lambda: sim.batch("laneU", "submission", items("u", 240, 30, weight=2)))
    sim.at(72, lambda: sim.batch("bar-86406", "gate", items("w", 332, 30, weight=2)))
    sim.at(73, lambda: sim.batch("bar-86406", "gate", items("x", 332, 30, weight=2)))
    sim.at(90, lambda: sim.batch("bar-86406", "gate", items("g", 600, 15)))
    rec = _waste_watch(sim, t_from=60)

    def extra(sim, m):
        m["waste"] = {**rec, "mean": round(rec["slot_s"] / 10 / rec["samples"], 2) if rec["samples"] else None}
        m["sub_waits"], m["gate_waits"] = _waits(sim, "tree-trace"), _waits(sim, "bar-")
        s = _waits(sim, "laneU")
        m["laneU_waits"] = s
        return {"submissions_in_minutes": (m["sub_waits"]["p95"] or 1e9) <= 1200 and (s["p95"] or 1e9) <= 1200,
                "no_filler_while_real_waits": rec["samples"] > 0 and rec["slot_s"] / 10 / rec["samples"] <= 0.5}
    return sim, {"extra": extra}


# 11-08: a cached foreign inventory on an empty worker used to block placement forever,
# while the planner counted those very slots as capacity. Keep the provider simulated.
@scenario("11-08 unplaced", ["lost", "clients", "threads"], 600,
          "a large exclusive gate behind an empty worker's stale foreign inventory: refresh it, or start usable capacity")
def exclusive_foreign_inventory(code):
    from tests.sim.harness import items
    sim = _sim(code)
    vs = _warm(sim, 2)
    sim.scheduler()

    def stale():
        s = sim.w.live_sched()[1]
        for v in s.vms.values():
            v.foreign = v.slots  # last observation; the agent now reports no runs
        sim.batch("gate", "gate", items("g", 1, 5, timing=True), vm_named=True)
    sim.at(30, stale)

    def extra(sim, m):
        rows = [v for k, v in sim.w.items.items() if k[0].startswith("gate-")]
        m["gate_wait"] = min((x["delivered"] for x in rows if x["delivered"] is not None), default=sim.k.now) - sim.k.t0 - 30
        return {"gate_not_stranded": bool(rows) and all(x["delivered"] is not None for x in rows) and m["gate_wait"] < 180}
    return sim, {"extra": extra}


@scenario("11-08 exclusive fragmentation", ["lost", "threads"], 600,
          "four large VMs each run a non-preemptible submission; one tiny exclusive gate needs a new VM, not their fragmented free slots")
def exclusive_fragmented_capacity(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 4)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("sub", "submission", items("s", 4, 3000, weight=3), vm_named=True))
    sim.at(45, lambda: sim.batch("gate", "gate", items("g", 1, 5, timing=True), vm_named=True))

    def extra(sim, m):
        rows = [v for k, v in sim.w.items.items() if k[0].startswith("gate-")]
        return {"tiny_gate_gets_capacity": bool(rows) and all(x["delivered"] is not None for x in rows)}
    return sim, {"extra": extra}


@scenario("11-08 guessed ends", ["lost", "threads"], 600,
          "four running gates overrun a 60-second guess; a new seven-item exclusive gate gets a lane without waiting on their guessed ends")
def exclusive_guessed_end(code):
    from tests.sim.harness import items
    sim = _sim(code)
    _warm(sim, 4)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("older-gate", "gate", items("s", 4, 1200, est=60, timing=True), vm_named=True))
    sim.at(100, lambda: sim.batch("new-gate", "gate", items("g", 7, 30, est=60, timing=True), vm_named=True))

    def extra(sim, m):
        rows = [v for k, v in sim.w.items.items() if k[0].startswith("new-gate-")]
        first = min((x.get("first_run", sim.k.now) for x in rows), default=sim.k.now) - sim.k.t0
        m["exclusive_first_run_s"] = first
        return {"new_gate_has_a_lane": first < 280 and all(x["delivered"] is not None for x in rows)}
    return sim, {"extra": extra}


@scenario("11-08 missing resume", ["threads"], 600,
          "the dedicated fallback receives a direct 404: discard only its own lease, refund the non-start, create once, never retry the missing ID")
def missing_dedicated_resume(code):
    from types import SimpleNamespace
    from tests.sim.harness import IMAGE
    sim = _sim(code)
    B = sim.B
    rec = {"tries": 0, "created": 0}

    def resume():
        s = object.__new__(B.Session)
        s.batch, s.stop = "dedicated", sim.w.th.Event()
        s.reg, s.counts = B.Registry(), {"resumed": 0}
        s.enough = lambda *a: False
        s.gate = SimpleNamespace(take=lambda *a: 1)
        with s.reg.locked() as d:
            d["vms"] = {"missing": {"type": "large", "snapshot": IMAGE, "state": "stopped",
                                   "lease": {"batch": s.batch, "pid": B.os.getpid(), "host": B.socket.gethostname()}}}
        def attempt(*a, **kw):
            rec["tries"] += 1
            return SimpleNamespace(returncode=1, stdout='{"status":404,"code":"sandbox_not_found"}', stderr="")
        B.subprocess = SimpleNamespace(run=attempt)
        def create(n, kind):
            rec["created"] += n
            sim.w.note("replacement", vm_type=kind)
        s._create = create
        s._bring_up(B.VM("missing", "large", "resumed"), False)
        rec["forgotten"] = "missing" not in s.reg.read()["vms"]
        rec["counted"] = s.counts["resumed"]
    sim.at(1, resume)

    def extra(sim, m):
        m["resume"] = rec
        return {"missing_replaced_once": rec == {"tries": 1, "created": 1, "forgotten": True, "counted": 0}}
    return sim, {"extra": extra}


@scenario("11-08 billing balance", ["threads"], 120,
          "plan hours exhausted, purchased pack available and starts allowed: no billing breach or money threshold")
def billing_purchased_pack(code):
    sim = _sim(code)
    limits = sim.w.limits
    sim.w.limits = lambda: {**limits(), "canStart": True, "billingStatus": "active", "planIncluded": {"vmHoursRemaining": 0}, "packBalanceHours": 1000}
    sim.scheduler()
    def observe():
        s = sim.w.live_sched()[1]
        s._h().record_billing(sim.k.now, sim.S.health.billing_problems(sim.w.limits()))
    sim.at(30, observe)
    def extra(sim, m):
        hs = [s for _, s in sim.w.sched_procs if s is not None][-1]._h()
        v = hs.check(sim.k.now, 30)["targets"]["billing"]["value"]
        m["billing_blocks"] = v
        return {"purchased_pack_not_a_block": v == 0}
    return sim, {"extra": extra}


@scenario("11-08 closed legacy ghost", ["lost", "threads"], 300,
          "a legacy timing client closes while running: its final row must free the held slot, not enter strict cancellation forever")
def closed_legacy_slot(code):
    from tests.sim.harness import items
    sim = _sim(code)
    vs = _warm(sim, 1)
    sim.scheduler()
    sim.at(10, lambda: sim.batch("closed", "gate", items("g", 1, 900, timing=True), abandon_after=10, vm_named=True))
    def extra(sim, m):
        s = [s for _, s in sim.w.sched_procs if s is not None][-1]
        stale = sum(len(v.running) for v in s.vms.values() if v.id == vs[0].id)
        m["closed_legacy_slots"] = stale
        return {"closed_slot_released": stale == 0}
    return sim, {"extra": extra}


@scenario("11-10 68 slot-hour gate", ["lost", "clients", "threads"], 1800,
          "one client queues 68 slot-hours on 28 large VMs: grow for the gate deadline, not the economic window")
def gate_68_slot_hours(code):
    from tests.sim.harness import items
    sim = _sim(code, policy={"floor_default": 28})
    _warm(sim, 28)
    sim.scheduler()
    bid = sim.batch("large-gate", "gate", items("g", 816, 300), after=30)

    def extra(sim, m):
        c = sim.clients[bid]
        elapsed = c["t_end"] - c["t_submit"] if c["t_end"] else 1e9
        made = [e for e in sim.w.events if e["kind"] == "start"]
        scheds = [s for _, s in sim.w.sched_procs if s is not None]
        h = getattr(scheds[-1], "health", None) if scheds else None
        needless = sum(e.get("n", 1) for e in h.ev.get("needless", [])) if h else 0
        m["gate_elapsed_s"], m["created"], m["needless_recorded"] = elapsed, len(made), needless
        used = {v for it in sim.w.items.values() for v in it["finished_on"]}
        unused = [e["vm"] for e in made if e["vm"] not in used]
        m["unused_starts"] = unused
        # The bring-up cap staggers arrivals: a no-cap VM-count bound would
        # falsely call deadline capacity needless. Every start must run work.
        # Work is indivisible: the last 300-second item can straddle the fluid
        # horizon. Allow one item, not another hour; all jobs must be delivered.
        return {"near_gate_horizon": elapsed <= sim.S.GATE_HORIZON + 300,
                "grew": bool(made), "no_needless_start": needless == 0 and not unused}
    return sim, {"extra": extra}


@scenario("economic growth: steady small submissions", ["lost", "clients", "threads"], 1500,
          "a held four-VM pool clears repeated small submissions without any start")
def steady_small_submissions(code):
    from tests.sim.harness import items
    sim = _sim(code, policy={"floor_default": 4})
    _warm(sim, 4)
    sim.scheduler()
    for i in range(20):
        sim.batch("steady", "submission", items(f"s{i}-", 8, 45), after=30 + i * 60)

    def extra(sim, m):
        made = [e for e in sim.w.events if e["kind"] == "start"]
        m["created"] = len(made)
        return {"no_start": not made}
    return sim, {"extra": extra}


# ------------------------------------------------------------------------------------------------ running
def run_one(name: str, code: str) -> dict:
    from tests.sim import metrics
    sc = S[name]
    t0 = time.time()
    sim, opts = sc["fn"](code)
    truth = _truth_waste(sim)  # (health's waste against the world's, every scenario: health_waste_agrees)
    err = None
    try:
        sim.run(sc["seconds"], real_limit=float(os.environ.get("SIM_REAL_LIMIT", "240")))
    except Exception as e:
        err = repr(e)[:300]
    m = metrics.measure(sim, opts.get("busy_from", 0), opts.get("busy_to", 1e18))
    v = metrics.verdict(m, sc["want"])
    if "extra" in opts:
        v.update(opts["extra"](sim, m))
    # (the scheduler's own health checks, where it has them, against the ground truth: no target it calls breached
    # that the run met, nor one it calls met that the run breached)
    ls = [s for p, s in sim.w.sched_procs if s is not None]
    if ls and hasattr(ls[-1], "health_check"):
        hs = ls[-1].health_check(sc["seconds"] / 60)
        m["health"] = {k: {"value": x["value"], "ok": x["ok"]} for k, x in hs.get("targets", {}).items()}
        pairs = {"busy": "busy", "churn": "churn", "orphans": "orphans", "modal": "modal", "strikes": "ours", "status": "status"}
        disagree = [k for k, hk in pairs.items() if k in v and hk in hs.get("targets", {}) and hs["targets"][hk]["ok"] != v[k] and k != "status"]
        m["health_disagrees"] = disagree
        if "health" in sc["want"] or opts.get("health"):
            v["health_agrees"] = not disagree
        hw = hs.get("targets", {}).get("waste", {}).get("value")
        tw = round(truth["sum"] / truth["n"], 1) if truth["n"] else None
        m["waste_health"], m["waste_truth"], m["waste_truth_examples"] = hw, tw, truth["examples"]
        if "waste" in hs.get("targets", {}):  # (a code with health's waste: it agrees with the world's, to 0.3 of a slot)
            v["health_waste_agrees"] = (hw or 0.0) <= (tw or 0.0) + 0.3 and (tw or 0.0) <= (hw or 0.0) + 0.3
    if err:
        v["ran"] = False
    out = {"name": name, "ok": all(v.values()), "verdict": v, "metrics": m, "real_s": round(time.time() - t0, 1), "err": err,
           "events": len(sim.w.events), "switches": sim.k.switches}
    if os.environ.get("SIM_LOG"):
        pathlib.Path(os.environ["SIM_LOG"]).mkdir(parents=True, exist_ok=True)
        with open(pathlib.Path(os.environ["SIM_LOG"]) / f"{name}.log", "w") as f:
            for t, p, msg in sim.w.log_lines:
                f.write(f"{t:9.1f} {p} {msg}\n")
            for e in sim.w.events:
                f.write(json.dumps(e) + "\n")
            for e in sim.k.errors:
                f.write(e + "\n")
    sim.cleanup()
    return out


def on_boat(names: list[str], code: str) -> dict:
    """Every scenario a job of one interactive batch on boat, on the pool's image (python3 there): the checkout shipped
    as tests/laws.toml says, each job's last line its result (08:25: 4 scenarios at once here held the laptop's load at 65)."""
    import tempfile
    sc = os.path.expanduser("~/.local/bin/supercomputer")
    out = pathlib.Path(tempfile.mkdtemp(prefix="sc-scen-"))
    try:
        jobs = [{"name": n, "cmd": f"PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1 python3 -P tests/scenarios.py --one {n} --code . 2>/dev/null | tail -1", "timeout": 900, "cpu": 2}
                for n in names]
        (out / "jobs.json").write_text(json.dumps(jobs))
        img = os.environ.get("SC_LAWS_IMAGE", "")
        if not img:
            with contextlib.suppress(Exception):
                t = subprocess.run([sc, "scheduler", "status"], capture_output=True, text=True, timeout=30).stdout
                img = next((v["image"] for v in json.loads(t[t.index("{"):]).get("vms", [])), "")
        subprocess.run([sc, "run", "--jobs", str(out / "jobs.json"), "--out", str(out / "run"), "--class", "interactive", "--backend", "boat"],
                       cwd=code, env={**os.environ, "SUPERCOMPUTER_CONFIG": str(pathlib.Path(code) / "tests" / "laws.toml"), "SUPERCOMPUTER_IMAGE": img},
                       stdout=sys.stderr, timeout=3600)
        res = {}
        for n in names:
            f = out / "run" / f"{n}.log"
            with contextlib.suppress(Exception):
                line = [l for l in f.read_text().split("--- stderr ---")[0].strip().splitlines() if l.startswith("{")][-1]
                res[n] = json.loads(line)
        return res
    finally:
        import shutil
        shutil.rmtree(out, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("names", nargs="*")
    ap.add_argument("--code", default=str(HERE), help="the checkout whose supercomputer/ is tested")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--one", help=argparse.SUPPRESS)
    ap.add_argument("-j", type=int, default=1)  # (here: one at a time, niced; the laptop runs light work)
    ap.add_argument("--boat", action="store_true", help="each scenario a job of one interactive batch on boat (the gate's way)")
    a = ap.parse_args()
    if a.one:
        print(json.dumps(run_one(a.one, a.code), default=str))
        return 0
    names = a.names or list(S)
    env = {**os.environ, "PYTHONHASHSEED": "0", "PYTHONDONTWRITEBYTECODE": "1"}
    t0 = time.time()

    def child(n):
        if a.boat:
            return boat_res.get(n) or {"name": n, "ok": False, "verdict": {"ran": False}, "err": "no result from boat", "metrics": {}}
        p = subprocess.run(["nice", "-n", "19", sys.executable, "-P", __file__, "--one", n, "--code", a.code], capture_output=True, text=True, env=env, timeout=600)
        try:
            return json.loads(p.stdout.strip().splitlines()[-1])
        except Exception:
            return {"name": n, "ok": False, "verdict": {"ran": False}, "err": (p.stderr or p.stdout)[-800:], "metrics": {}}
    boat_res = on_boat(names, a.code) if a.boat else {}
    with concurrent.futures.ThreadPoolExecutor(a.j) as ex:
        res = list(ex.map(child, names))
    bad = 0
    for r in res:
        n = r["name"]
        x = n in XFAIL
        status = ("XPASS" if x else "PASS") if r["ok"] else ("xfail" if x else "FAIL")
        if status in ("FAIL",):
            bad += 1
        failed = [k for k, ok in r["verdict"].items() if not ok]
        print(f"{status:5} {n:24} {r.get('real_s', '?'):>5}s  {'failed: ' + ', '.join(failed) if failed else ''}{'  (' + XFAIL[n] + ')' if x and not r['ok'] else ''}")
        if failed and not a.json:
            for k in failed:
                mk = {"strikes": "strikes", "modal": "modal", "lost": "lost", "churn": "churn", "orphans": "orphans", "busy": "busy", "starts": "starts",
                      "status": "status", "clients": "clients", "threads": "thread_errors"}.get(k)
                if mk and mk in r.get("metrics", {}):
                    print(f"        {k}: {json.dumps(r['metrics'][mk], default=str)[:600]}")
            if r.get("err"):
                print(f"        error: {r['err'][:600]}")
    if a.json:
        print(json.dumps(res, default=str, indent=1))
    print(f"scenarios: {sum(1 for r in res if r['ok'])} of {len(res)} pass ({len([r for r in res if not r['ok'] and r['name'] in XFAIL])} known failures), {time.time() - t0:.0f} s")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
