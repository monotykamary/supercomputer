"""The targets, over a simulated run's ground truth (the world's events, the samples, the clients' outcomes).

    busy        busy slot-time / held slot-time of an image while real work of it is queued   >= 0.85
    starts      VM starts per 100 items run                                                   <= 1
    churn       starts of an image within 25 min after one of its VMs stopped                 == 0
    orphans     VMs up with no live owner for more than 10 min                                == 0
    modal       items handed back for Modal without 3 real boat failures on distinct VMs      == 0
    strikes     strikes the scheduler charged for its own failures (no boat failure there)    == 0
    lost        rows lost to clients (lost, or the batch done without them), not for Modal    == 0
    status      the scheduler's status, its slowest answer                                    < 1 s
    clients     clients failed by the tool (an exception, an item never reported)             == 0

Each check takes the run's numbers and says ok or not; a scenario picks the checks that bind for it (`want`).
"""
import re

HOLD_CHURN_S = 1500
ORPHAN_S = 600


def busy(sim, t_from: float = 0, t_to: float = 1e18) -> dict:
    """While real work of an image is queued: its VMs' slots running real work (a filler's slot is idle then: it
    should have been given up), over its VMs' slots (every VM up with its agent: paid for)."""
    held = busy_ = any_ = 0
    for s in sim.samples:
        if not t_from <= s["t"] <= t_to:
            continue
        for image, p in s["per"].items():
            if s["queued"].get(image, {}).get("real", 0) > 0 and p["slots"]:
                held += p["slots"]
                busy_ += p["busy_real"]
                any_ += p["busy"]
    return {"busy": round(busy_ / held, 3) if held else None, "busy_any": round(any_ / held, 3) if held else None, "held_slot_samples": held}


def starts(sim) -> dict:
    st = [e for e in sim.w.events if e["kind"] == "start"]
    ran = sum(1 for it in sim.w.items.values() if it["finished_on"])
    return {"starts": len(st), "items": ran, "per_100": round(100 * len(st) / ran, 2) if ran else None}


def churn(sim) -> dict:
    # (a stop that counts: a VM up and serving let go by us or by its TTL; not boat's own stop, not a start that never
    # came up, not a TTL run out while the laptop slept)
    ups = {e["vm"] for e in sim.w.events if e["kind"] == "daemon"} | {v.id for v in sim.w.vms.values() if v.t_daemon is not None}
    sleeps = [(e["t"], e["t"] + e["s"]) for e in sim.w.events if e["kind"] == "sleep"]
    stops = [e for e in sim.w.events if e["kind"] == "stop" and e.get("why") != "boat" and e.get("served", e["vm"] in ups)
             and not (e.get("why") == "ttl" and any(a <= e["t"] <= b + 30 for a, b in sleeps))]
    out, used = [], set()
    for e in sim.w.events:
        if e["kind"] != "start":
            continue
        prior = [i for i, s in enumerate(stops) if i not in used and s["image"] == e["image"] and 0 <= e["t"] - stops[i]["t"] <= HOLD_CHURN_S]
        if prior:  # (each stop matched to one start at most)
            used.add(prior[-1])
            s = stops[prior[-1]]
            out.append({"start": e["vm"], "t": e["t"], "after_stop_of": s["vm"], "stopped_t": s["t"], "why": s["why"]})
    lost_resumed = [e for e in sim.w.events if e["kind"] == "start" and e.get("how") == "resumed" and any(s["vm"] == e["vm"] for s in stops if s["t"] <= e["t"])]
    return {"churn": len(out), "examples": out[:4], "lost_then_resumed": len(lost_resumed)}


def orphans(sim) -> dict:
    run: dict = {}
    worst: dict = {}
    for s in sim.samples:
        now = {vid for p in s["per"].values() for vid in p["orphans"]}
        for vid in list(run):
            if vid not in now:
                run.pop(vid)
        for vid in now:
            run.setdefault(vid, s["t"])
            worst[vid] = max(worst.get(vid, 0), s["t"] - run[vid])
    bad = {v: round(t) for v, t in worst.items() if t > ORPHAN_S}
    return {"orphans_over_10min": len(bad), "longest_s": round(max(worst.values(), default=0)), "examples": dict(list(bad.items())[:4])}


def modal(sim) -> dict:
    bad, ok = [], 0
    for bid, rec in sim.clients.items():
        for name, why in rec["lost"].items():
            it = sim.w.items[(bid, name)]
            fails = len(it["boat_vms"])
            refusals = sum(1 for e in sim.w.events if e["kind"] in ("start_refused", "never_ready") and e.get("image") == it["image"]
                           and it["submitted"] is not None and e["t"] >= it["submitted"] - sim.k.t0 and (it["delivered"] is None or e["t"] <= it["delivered"] - sim.k.t0))
            if fails + min(refusals, 3) >= 3:
                ok += 1
            else:
                bad.append({"item": f"{bid}/{name}", "boat_failed_vms": fails, "start_failures": refusals, "why": str(why)[:120]})
    return {"modal_bad": len(bad), "modal_ok": ok, "examples": bad[:4]}


STRIKE = re.compile(r"^(\S+): (\S+): boat failed it")


def strikes(sim) -> dict:
    """Strikes the scheduler logged (its own words: "<batch>: <item>: boat failed it ...") on a VM where boat failed
    nothing of that item (ours), or with no VM named and no start of its image failing before it."""
    ours, total, ex = 0, 0, []
    for t, proc, msg in sim.w.log_lines:
        m = STRIKE.match(msg)
        if not m:
            continue
        total += 1
        it = sim.w.items.get((m.group(1), m.group(2)))
        if it is None:
            continue
        vm = re.search(r"(bx_\w+)", msg[m.end():])
        if vm and vm.group(1) in it["boat_vms"]:
            continue
        if not vm and any(e["kind"] in ("start_refused", "never_ready") and e.get("image") == it["image"] and e["t"] <= t for e in sim.w.events):
            continue
        ours += 1
        if len(ex) < 4:
            ex.append(f"{t}s {msg[:180]}")
    return {"strikes": total, "strikes_ours": ours, "examples": ex}


def lost(sim) -> dict:
    n, ex = 0, []
    for bid, rec in sim.clients.items():
        for name, why in rec["lost"].items():
            if "without its row" in str(why) or "lost" in str(why) and "boat failed it" not in str(why):
                n += 1
                if len(ex) < 4:
                    ex.append(f"{bid}/{name}: {str(why)[:100]}")
    never = [f"{bid}/{n_}" for bid, rec in sim.clients.items() if rec.get("returned") and not rec.get("abandoned") for n_, r in rec["items"].items() if r is None]
    return {"lost_rows": n, "never_reported": len(never), "examples": ex + never[:4]}


def status(sim) -> dict:
    ms = sim.w.status_ms
    return {"status_max_s": round(max(ms) / 1000, 2) if ms else None, "status_n": len(ms)}


def clients(sim) -> dict:
    unfinished = [f"{bid} ({sum(1 for x in r['items'].values() if x is None)} items unreported)" for bid, r in sim.clients.items()
                  if r["t_submit"] is not None and not r.get("returned") and not r.get("abandoned") and not r.get("error") and r["client"] != "treadmill"]
    return {"client_errors": len(sim.w.client_errors), "unfinished": len(unfinished), "examples": sim.w.client_errors[:4] + unfinished[:2]}


def measure(sim, busy_from: float = 0, busy_to: float = 1e18) -> dict:
    out = {}
    for f in (starts, churn, orphans, modal, strikes, lost, status, clients):
        out[f.__name__] = f(sim)
    out["busy"] = busy(sim, busy_from, busy_to)
    out["thread_errors"] = {"n": len(sim.k.errors), "examples": [e[:400] for e in sim.k.errors[:3]]}
    return out


CHECKS = {
    "busy": lambda m: m["busy"]["busy"] is None or m["busy"]["busy"] >= 0.85,
    "starts": lambda m: m["starts"]["per_100"] is None or m["starts"]["per_100"] <= 1.0,
    "churn": lambda m: m["churn"]["churn"] == 0 and m["churn"]["lost_then_resumed"] == 0,
    "orphans": lambda m: m["orphans"]["orphans_over_10min"] == 0,
    "modal": lambda m: m["modal"]["modal_bad"] == 0,
    "strikes": lambda m: m["strikes"]["strikes_ours"] == 0,
    "lost": lambda m: m["lost"]["lost_rows"] == 0 and m["lost"]["never_reported"] == 0,
    "status": lambda m: m["status"]["status_max_s"] is None or m["status"]["status_max_s"] < 1.0,
    "clients": lambda m: m["clients"]["client_errors"] == 0 and m["clients"]["unfinished"] == 0,
    "threads": lambda m: m["thread_errors"]["n"] == 0,
}


def verdict(m: dict, want: list[str]) -> dict:
    return {k: bool(CHECKS[k](m)) for k in want}
