"""The pool's health: the targets the user set ("seamless and not wasteful"), checked live over the last N minutes from
the scheduler's own records (never by reading its log), and the same checks the scenario suite holds the code to
(tests/scenarios.py, tests/sim/metrics.py).

    busy      real work's slot-time over the slot-time it could use (the held VMs' slots, at most what runs and
              what's queued of it needs), while real work of their image is queued                         >= 0.85
    handoff   across a code switch: VMs the old scheduler held not held by the new one, VMs with no live owner,
              slots idle beside real work waiting past a tick (a tick and two after the switch)               == 0
    waste     slots held by filler (or a sweep, against a gate) or idle while real work that could take them
              has waited past sub_preempt_s, the mean over the window's samples                            == 0
    starts    VM starts per 100 items placed                                                              <= 1
    churn     starts of an image within 25 min after this scheduler released one of its VMs               == 0
    orphans   VMs up of the pool's images with no live owner for over 10 min (adoption's findings)        == 0
    modal     items handed back for Modal before boat_tries strikes on distinct VMs                         == 0
    ours      strikes for our own failures (a tree gone, a batch not prepared, an upgrade, a re-attach)    == 0
    status    the scheduler's status, its slowest answer                                                   < 1 s
    errors    requests the scheduler failed (500) and threads of it that died                              == 0
    env       a client needs no flag: the controller lookup off, no URGENT, the scheduler on                == ok

The scheduler records into a Health (record_*), samples it every 15 s, checks it every minute and alerts (its log,
alerts.jsonl, status) when a target is breached in 2 checks in a row; `supercomputer health [--minutes N]` prints the
checks (exit 1 on a breach). The records outlive a handoff (LIVE/scheduler/metrics/health.json), so a canary
(canary.py) judges new code against the hour before it.
"""
import json
import os
import pathlib
import re
import threading
import time

KEEP_S = 3 * 3600
CHURN_S = 1500
ORPHAN_S = 600
# (a preparation failure with no reason recorded is ours too: 11-01 11:09-12:21, 5,748 such strikes, a finished
# preparation left marked running)
OURS = re.compile(r"tree isn't on|isn't prepared|not reached after the restart|upgrad|re-attach|repair|its preparation failed: *$|never ended|isn't the commit's", re.I)

TARGETS = {
    "busy": ("real work's share of the slots it could use (held, at most its running and queued need) while real work is queued", ">=", 0.85),
    "waste": ("slots held by filler or idle while placeable real work waited past sub_preempt_s (mean over the window)", "==", 0),
    "handoff": ("across a code switch: VMs not carried over, orphans, slots idle beside waiting real work (worst check)", "==", 0),
    "starts": ("VM starts per 100 items placed", "<=", 1.0),
    "churn": ("starts within 25 min after a release of the image", "==", 0),
    "orphans": ("VMs with no live owner past 10 min", "==", 0),
    "modal": ("items to Modal before 3 strikes on distinct VMs", "==", 0),
    "ours": ("strikes for our own failures", "==", 0),
    "status": ("status's slowest answer, seconds", "<", 1.0),
    "errors": ("failed requests and dead threads", "==", 0),
    "env": ("flags a client would need", "==", 0),
    "needless": ("starts made while a free VM that fits their work idled", "==", 0),
    "stray": ("slots held by runs no item owns past 2 min (a kill retried once: sooner, a kill landing), now", "==", 0),
    "disk": ("VMs held with their free disk below the threshold (min(10 GB, a tenth of the disk)), now", "==", 0),
    "billing": ("boat's billing blocks: starts refused or billing inactive (balances are information only)", "==", 0),
}


def _ok(op: str, value, limit) -> bool:
    if value is None:
        return True
    return {">=": value >= limit, "<=": value <= limit, "<": value < limit, "==": value == limit}[op]


class Health:
    """The scheduler's records for its targets: events with their times, kept KEEP_S."""

    def __init__(self, path: pathlib.Path | None = None):
        self.path = path
        self.lock = threading.Lock()
        self.ev: dict[str, list] = {k: [] for k in ("start", "release", "placed", "strike", "modal", "sample", "orphan", "status", "error", "billing", "needless", "handoff")}
        self.streak: dict[str, int] = {}
        self.alerted: set = set()
        self.last: dict = {}
        if path and path.exists():
            try:
                d = json.loads(path.read_text())
                for k in self.ev:
                    self.ev[k] = d.get("ev", {}).get(k, [])
            except Exception:
                pass

    def _add(self, kind: str, t: float, **kw):
        with self.lock:
            self.ev[kind].append({"t": round(t, 2), **kw})
            if len(self.ev[kind]) > 20000:
                self.ev[kind] = self.ev[kind][-10000:]

    # -- what the scheduler records
    def record_start(self, t, image, vm, vm_type=""):
        self._add("start", t, image=image, vm=vm, type=vm_type)

    def record_needless(self, t, n, image, vm_type):
        """Starts the plan made while a free VM that fits its work idled (a VM of another type)."""
        self._add("needless", t, n=n, image=image, type=vm_type)

    def record_handoff(self, t, held_before, held_after, missing, orphans, idle):
        """A code switch's check (at the take-over, a tick and two after): what the new scheduler holds of the old one's."""
        self._add("handoff", t, before=held_before, after=held_after, missing=missing, orphans=orphans, idle=idle)

    def record_release(self, t, image, vm, vm_type=""):
        self._add("release", t, image=image, vm=vm, type=vm_type)

    def record_placed(self, t, n=1):
        with self.lock:  # (a count a minute: one event per item placed made the records, and their saves, too big)
            p = self.ev["placed"]
            if p and int(p[-1]["t"] // 60) == int(t // 60):
                p[-1]["n"] = p[-1].get("n", 1) + n
                return
        self._add("placed", t, n=n)

    def record_strike(self, t, item, vm, why):
        self._add("strike", t, item=item, vm=vm or "", ours=bool(OURS.search(why or "")), why=(why or "")[:160])

    def record_modal(self, t, item, fails, vms, tries):
        self._add("modal", t, item=item, fails=fails, vms=vms, ok=fails >= tries)

    def record_sample(self, t, per: dict):
        """per image: {"slots": held slots, "busy": slots on real work, "queued": real items queued}"""
        self._add("sample", t, per=per)

    def record_orphans(self, t, ids: list):
        self._add("orphan", t, ids=sorted(ids))

    def record_status(self, t, seconds):
        self._add("status", t, s=round(seconds, 3))

    def record_billing(self, t, problems: list):
        self._add("billing", t, problems=list(problems))

    def record_error(self, t, what):
        self._add("error", t, what=str(what)[:200])

    # -- the checks
    def check(self, now: float, minutes: float = 30, env: dict | None = None, released_since: float = 0.0) -> dict:
        """The targets over the last `minutes`; churn counts starts after releases made since `released_since` (a
        canary: its own code's releases, not the previous code's)."""
        lo = now - 60 * minutes
        with self.lock:
            ev = {k: [e for e in v if lo <= e["t"] <= now] for k, v in self.ev.items()}
            orph_all = [e for e in self.ev["orphan"] if e["t"] <= now]
            rel_all = [e for e in self.ev["release"] if released_since <= e["t"] <= now]
        held = busy = n_busy = pool = 0
        waste, n_waste = 0.0, 0
        for s in ev["sample"]:
            if any("waste" in p or "qslots" in p for p in s["per"].values()):
                n_waste += 1
                waste += sum(p.get("waste", 0) for p in s["per"].values())
            for p in s["per"].values():
                if p.get("queued", 0) > 0 and p.get("slots", 0) > 0:
                    # (the slots real work could use: the held ones, at most what runs and what's queued needs. A sample
                    # with one item queued and the rest of a pool held on filler by the hold is no waste of real work's;
                    # waste, above, is the measure of slots it could have had. Samples before qslots: every held slot)
                    dem = min(p["slots"], p.get("busy", 0) + p["qslots"]) if "qslots" in p else p["slots"]
                    held += dem
                    pool += p["slots"]
                    busy += min(dem, p.get("busy", 0))
                    n_busy += 1
        placed = sum(e.get("n", 1) for e in ev["placed"])
        # (churn: a release followed by a start of its image within CHURN_S, each release matched to one start at
        # most: one VM let go and thirty started for new work is one start too many, not thirty (06:53))
        churn, used = [], set()
        for s in sorted(ev["start"], key=lambda e: e["t"]):
            r = next((i for i, r in enumerate(rel_all) if i not in used and r["image"] == s["image"] and 0 <= s["t"] - r["t"] <= CHURN_S
                      and vcpus(r.get("type", "")) >= vcpus(s.get("type", ""))), None)  # (by image and type: a VM let go that would have done)
            if r is not None:
                used.add(r)
                churn.append(s)
        first: dict = {}  # (an orphan's first sighting in a run of sightings)
        worst = 0.0
        for o in sorted(orph_all, key=lambda e: e["t"]):
            ids = set(o["ids"])
            for vid in list(first):
                if vid not in ids:
                    first.pop(vid)
            for vid in ids:
                first.setdefault(vid, o["t"])
                if o["t"] >= lo:
                    worst = max(worst, o["t"] - first[vid])
        long_orphans = sorted({vid for vid, t0 in first.items() if now - t0 > ORPHAN_S})
        env = env if env is not None else env_flags()
        vals = {
            "busy": round(busy / held, 3) if n_busy >= 20 else None,  # (5 minutes of real work queued, at least)
            "starts": round(100 * len(ev["start"]) / placed, 2) if placed >= 50 else None,
            "churn": len(churn),
            "orphans": len(long_orphans),
            "modal": sum(1 for m in ev["modal"] if not m["ok"]),
            "ours": sum(1 for s in ev["strike"] if s["ours"]),
            "status": max((s["s"] for s in ev["status"]), default=None),
            "errors": len(ev["error"]),
            "env": len(env),
            "billing": len(ev["billing"][-1]["problems"]) if ev["billing"] else None,
            "stray": sum(p.get("stray", 0) for p in ev["sample"][-1]["per"].values()) if ev["sample"] else None,
            "needless": sum(e.get("n", 1) for e in ev["needless"]),
            "disk": sum(len(p.get("disk_low", [])) for p in ev["sample"][-1]["per"].values()) if ev["sample"] else None,
            "waste": round(waste / n_waste, 1) if n_waste else None,
            "handoff": max((e.get("missing", 0) + e.get("orphans", 0) + e.get("idle", 0) for e in ev["handoff"]), default=None),
        }
        detail = {"busy": {"held_slot_samples": held, "pool_share": round(busy / pool, 3) if pool else None}, "starts": {"starts": len(ev["start"]), "placed": placed},
                  "churn": churn[:4], "orphans": long_orphans[:6] + ([f"longest {round(worst)} s"] if worst else []),
                  "modal": [m for m in ev["modal"] if not m["ok"]][:4], "ours": [s["why"] for s in ev["strike"] if s["ours"]][:4],
                  "status": {"answers": len(ev["status"])}, "errors": [e["what"] for e in ev["error"]][:4], "env": env,
                  "billing": ev["billing"][-1]["problems"] if ev["billing"] else "boat limits not read in the window",
                  "stray": {"slots": sum(p.get("stray", 0) for p in ev["sample"][-1]["per"].values()) if ev["sample"] else None},
                  "needless": ev["needless"][-4:],
                  "disk": [x for p in ev["sample"][-1]["per"].values() for x in p.get("disk_low", [])][:8] if ev["sample"] else [],
                  "handoff": ev["handoff"][-4:],
                  "waste": {"samples": n_waste, "worst": max((sum(p.get("waste", 0) for p in s["per"].values()) for s in ev["sample"]), default=0),
                            "with_waste": sum(1 for s in ev["sample"] if any(p.get("waste", 0) for p in s["per"].values()))}}
        out = {k: {"what": TARGETS[k][0], "value": vals[k], "limit": f"{TARGETS[k][1]} {TARGETS[k][2]}", "ok": _ok(TARGETS[k][1], vals[k], TARGETS[k][2]),
                   "detail": detail[k]} for k in TARGETS}
        return {"t": now, "minutes": minutes, "ok": all(v["ok"] for v in out.values()), "targets": out}

    def tick(self, now: float, minutes: float = 30) -> list[str]:
        """A check (every minute): the targets breached in 2 checks in a row, each said once until it clears."""
        r = self.check(now, minutes)
        self.last = r
        alerts = []
        for k, v in r["targets"].items():
            self.streak[k] = self.streak.get(k, 0) + 1 if not v["ok"] else 0
            if self.streak[k] >= 2 and k not in self.alerted:
                self.alerted.add(k)
                alerts.append(f"health: {k} breached ({v['what']}: {v['value']}, want {v['limit']}): {json.dumps(v['detail'], default=str)[:240]}")
            if v["ok"]:
                self.alerted.discard(k)
        return alerts

    def save(self):
        if not self.path:
            return
        try:
            with self.lock:  # (a copy under the lock, written out of it: status's records never wait on the disk)
                d = {"ev": {k: list(v) for k, v in self.ev.items()}}
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(d))
            tmp.rename(self.path)
        except Exception:
            pass

    def prune(self, now: float):
        with self.lock:
            for k in self.ev:
                self.ev[k] = [e for e in self.ev[k] if now - e["t"] < KEEP_S]


def billing_problems(lim: dict) -> list[str]:
    """What in `boat limits` would stop starts or VMs for billing (the user: then auto top-up goes on, and they're told)."""
    out = []
    if lim.get("canStart") is False:
        out.append("can't start")
    if lim.get("startBlockedReason"):
        out.append(f"start blocked: {lim['startBlockedReason']}")
    if lim.get("blockedReason") and lim.get("blockedReason") != lim.get("startBlockedReason"):
        out.append(f"blocked: {lim['blockedReason']}")
    if lim.get("billingStatus") not in (None, "active"):
        out.append(f"billing {lim.get('billingStatus')}")
    return out


def vcpus(vm_type: str) -> int:
    """A VM type's vCPUs (an unknown type: as many as any, so it matches every start)."""
    return {"small": 2, "default": 4, "large": 8, "xlarge": 16}.get(vm_type, 99)


def client_gone(e: BaseException) -> bool:
    """A request's client gone mid-answer (a poll cut by its client's exit, a handoff): not the scheduler failing."""
    return isinstance(e, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, TimeoutError))


def env_flags() -> list[str]:
    """Flags a client would have to set (or unset) for the tool to work: none, by default."""
    bad = []
    if os.environ.get("SUPERCOMPUTER_CONTROLLER", "0") not in ("0", ""):
        bad.append("SUPERCOMPUTER_CONTROLLER is set: clients look for the parked controller")
    if os.environ.get("SUPERCOMPUTER_SCHEDULER", "1") in ("0", ""):
        bad.append("SUPERCOMPUTER_SCHEDULER=0: batches hold their own VMs")
    for k in ("SUPERCOMPUTER_URGENT", "GDPW_BOAT_URGENT"):
        if os.environ.get(k, "") not in ("", "0"):
            bad.append(f"{k} is set: no longer needed (every class may pay)")
    return bad


class Null:
    """No records (a Scheduler built without __init__, as the wiring tests build one)."""

    def __getattr__(self, name):
        return lambda *a, **k: None


def show(r: dict) -> str:
    lines = [f"health over the last {r['minutes']:.0f} min: {'ok' if r['ok'] else 'BREACHED'}"]
    for k, v in r["targets"].items():
        val = "-" if v["value"] is None else v["value"]
        d = v["detail"]
        extra = "" if v["ok"] or not d else f"   {json.dumps(d, default=str)[:200]}"
        lines.append(f"  {'ok ' if v['ok'] else 'BAD'} {k:8} {str(val):>7}  (want {v['limit']}: {v['what']}){extra}")
    return "\n".join(lines)
