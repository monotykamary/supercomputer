"""The canary: a handoff to new code is watched for WATCH_S (10 min), and handed back to the previous code by itself
when a target (health.py) is breached in 2 checks in a row that held in the hour before the handoff.
Status is mandatory whenever measured, regardless of the previous code's health. Handoff idle
uses disjoint observation intervals; a missing VM/orphan is a hard fault on its first sample. A hand-back is a
handoff: nothing let go, every VM and item taken back by the previous code's scheduler; the new code is marked
rejected, so no scheduler hands the pool to it again until another install replaces it.

    LIVE/scheduler/code/<hash>/supercomputer/   each code a scheduler ran, copied at its start (the hand-back runs it)
    LIVE/scheduler/canary.json                  {"from", "to", "t", "state": watching | passed | rejected, "rejected": [...]}
"""
import json
import pathlib
import shutil
import time

from . import rules

WATCH_S = 600.0
BASELINE_S = 3600.0
# (the targets new code can break in 10 minutes; busy and starts need the workload's hours to say anything)
JUDGED = ("orphans", "modal", "ours", "status", "errors", "churn", "needless", "waste", "handoff", "disk")
# With no records before the handoff, these other targets need no baseline.
# Status is mandatory separately: predecessor latency never excuses a slow current response.
JUDGED_BLIND = ("orphans", "modal", "ours", "errors", "churn", "needless")


class Canary:
    def __init__(self, dir_: pathlib.Path):
        self.dir = dir_
        self.path = dir_ / "canary.json"
        self.streak: dict = {}
        self.baseline: set | None = None
        self._handoff_checked_at: float | None = None
        self._handoff_hard_fault = False

    def read(self) -> dict:
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return {"rejected": []}

    def write(self, d: dict):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(d))
        tmp.rename(self.path)

    def snapshot(self, code: str, pkg: pathlib.Path) -> pathlib.Path | None:
        """This code's package, kept for a hand-back (once a code)."""
        dst = self.dir / "code" / code / "supercomputer"
        if dst.exists():
            return dst
        try:
            tmp = self.dir / "code" / f".{code}.tmp"
            shutil.rmtree(tmp, ignore_errors=True)
            shutil.copytree(pkg, tmp / "supercomputer", ignore=shutil.ignore_patterns("__pycache__"))
            tmp.rename(self.dir / "code" / code)
            return dst
        except Exception:
            return None

    def rejected(self, code: str) -> bool:
        return code in self.read().get("rejected", [])

    def retry_ready(self, code: str, now: float) -> bool:
        from . import rules
        d = self.read()
        return rules.handoff_retry_ready(now, float(d.get("retry_after", {}).get(code, 0)),
                                         code in d.get("rejected", []))

    def handoff_failed(self, code: str, why: str, retryable: bool, now: float):
        """Startup deadlines and authority loss are not bad-code verdicts."""
        from . import rules
        d = self.read()
        retry = d.setdefault("retry_after", {})
        if rules.handoff_reject(retryable):
            d.update(state="rejected", rejected=sorted(set(d.get("rejected", [])) | {code}))
            retry.pop(code, None)
        else:
            d["state"] = "retryable"
            retry[code] = now + 60  # not a five-second retry storm while the controller is loaded
        pending = d.get("pending_handback", {})
        if pending.get("to") == code:
            pending["retry_after"] = now + 60
            if not retryable:
                pending["failed"] = why
        d["why"] = why
        self.write(d)

    def handback_due(self, me: str, now: float) -> str | None:
        """Retry a persisted handback after deferral; only its current owner may act."""
        d = self.read()
        pending = d.get("pending_handback")
        if not isinstance(pending, dict):
            target = (d.get("from") if d.get("state") == "rejected" and d.get("to") == me
                      else d.get("to") if d.get("state") == "rolled back" and d.get("from") == me else None)
            pending = {"from": me, "to": target, "retry_after": 0.0}
        target = pending.get("to")
        available = isinstance(target, str) and bool(target) and target != me and (self.dir / "code" / target / "supercomputer").is_dir()
        if not rules.handback_due(pending.get("from") == me, available, bool(pending.get("failed")),
                                  now, float(pending.get("retry_after", 0))):
            return None
        d["pending_handback"] = {**pending, "retry_after": now + 15}
        self.write(d)
        return target

    def handing_off(self, old: str, new: str, now: float):
        self.baseline, self.streak, self._handoff_checked_at = None, {}, now
        self._handoff_hard_fault = False
        d = self.read()
        d.pop("pending_handback", None)
        d.update({"from": old, "to": new, "t": now, "state": "watching"})
        self.write(d)

    def rollback_to(self, me: str, code: str | None = None) -> tuple[str | None, str]:
        """`supercomputer rollback [code]`: the code to hand the pool back to (given, else the newest whose canary
        passed that isn't this one, else the one this code took over from), kept here; this code marked rejected so
        no scheduler hands the pool to it again until another install. (None, why) when there's none to go to."""
        d = self.read()
        cands = [code] if code else [c for c in reversed(d.get("passed", [])) if c != me] + ([d["from"]] if d.get("from") and d.get("from") != me else [])
        for c in cands:
            if c and c != me and (self.dir / "code" / c / "supercomputer").exists():
                now = time.time()
                d.update({"from": me, "to": c, "t": now, "state": "rolled back", "rejected": sorted(set(d.get("rejected", [])) | {me}),
                          "pending_handback": {"from": me, "to": c, "retry_after": now + 15},
                          "why": f"supercomputer rollback{' ' + code if code else ''}"})
                self.write(d)
                return c, ""
        return None, f"no saved copy of {code}" if code else "no previous code saved to roll back to"

    def takes_over(self, code: str) -> bool:
        """Whether a kept code takes the pool at once at its start (the atomic handoff): a hand-back to one that
        doesn't re-attaches VM by VM, the very drop a handoff check guards against (15:06:48: d2558009cfe2 handed
        back for 2 idle slots to b4cc5dd4fdbd, whose re-attach left VMs without a live owner for 56 s)."""
        with __import__("contextlib").suppress(Exception):
            return "def _take_pool" in (self.dir / "code" / code / "supercomputer" / "scheduler.py").read_text()
        return False

    def handoff_verdict(self, me: str, dropped: int, idle: int, what: str) -> tuple[str, str]:
        """A tick (and two) after the switch to `me`: a VM not carried over or an orphan hands the pool back at once (the
        user, 11-01 14:30: "make sure this never happens again"), to a code that takes it over whole; slots idle beside
        waiting real work are health's handoff target, judged as the other targets are (two checks in a row)."""
        d = self.read()
        if d.get("to") != me or d.get("state") != "watching" or dropped <= 0:
            return "", ""
        self._handoff_hard_fault = True
        frm = d.get("from")
        if not (frm and (self.dir / "code" / frm / "supercomputer").exists()):
            return "", ""
        if not self.takes_over(frm):
            return "alert", f"code {me}: the handoff left {what}; no hand-back to {frm}: it re-attaches VM by VM"
        why = f"code {me}: the handoff left {what}: handed back to {frm}"
        d.update({"state": "rejected", "rejected": sorted(set(d.get("rejected", [])) | {me}), "why": why,
                  "pending_handback": {"from": me, "to": frm, "retry_after": time.time() + 15}})
        self.write(d)
        return "handback", why

    def admissions(self, me: str, expected: set[str], loaded: set[str], closed: set[str], errors: dict) -> tuple[dict, str, str]:
        """Before workers/starts/releases: the live set must equal old minus client closes.

        Persist the ID-level receipt. An undecodable admission remains reserved
        by the scheduler, but cannot authorize any mutation.
        """
        from . import rules
        wanted = expected - closed
        missing, extra = sorted(wanted - loaded), sorted(loaded - wanted)
        ready = rules.recovery_ready(len(missing), len(extra), len(errors))
        receipt = {"expected": sorted(expected), "loaded": sorted(loaded), "closed": sorted(closed),
                   "missing": missing, "extra": extra, "errors": dict(errors), "ready": ready}
        d = self.read()
        d["admissions"] = receipt
        self.write(d)
        if ready:
            return receipt, "", ""
        reason = f"{len(missing)} missing batches {missing}, {len(extra)} extra batches {extra}, {len(errors)} unclassified admissions {sorted(errors)}"
        what, why = self.handoff_verdict(me, 1, 0, reason)
        return receipt, what or "alert", why or ("recovery held: " + reason)

    def tick(self, me: str, now: float, health) -> tuple[str, str]:
        """Every minute on the new code's scheduler: ("watch" | "passed" | "handback" | "", why)."""
        d = self.read()
        if d.get("to") != me or d.get("state") != "watching":
            return "", ""
        if d.get("admissions", {}).get("ready") is False:
            return "watch", f"code {me}: admission recovery still blocked; canary cannot pass"
        t0 = float(d.get("t", now))
        if self.baseline is None:  # Other targets must have held before the handoff; status is mandatory.
            minutes = BASELINE_S / 60
            b = health.check(t0, minutes)
            seen = any(e["t"] <= t0 for v in getattr(health, "ev", {}).values() for e in v)
            self.baseline = {k for k, v in b.get("targets", {}).items() if v["ok"] and v["value"] is not None and (seen or k in JUDGED_BLIND)}  # (measured and held)
            self.baseline.add("status")
        since = max(t0, self._handoff_checked_at if self._handoff_checked_at is not None else t0)
        r = health.check(now, max(1.0, (now - t0) / 60), released_since=t0, handoff_since=since)
        self._handoff_checked_at = max(since, now)
        bad = []
        for k in JUDGED:
            v = r.get("targets", {}).get(k)
            if v is None:
                continue
            breached = not v["ok"]
            if k == "waste" and breached and (v.get("detail") or {}).get("with_waste", 2) < 2:
                breached = False  # (one measure's waste is no code's: 11-02 02:25:58, one 8-slot sample handed a tree fix back)
            if k == "handoff":
                self._handoff_hard_fault |= v.get("hard_faults", 0) > 0
                self.streak[k] = rules.handoff_health_streak(self.streak.get(k, 0), v["value"] is not None,
                                                             v.get("hard_faults", 0), v["value"] or 0)
            elif k == "status":
                self.streak[k] = rules.canary_status_streak(self.streak.get(k, 0), v["value"] is not None, v["ok"])
            else:
                self.streak[k] = self.streak.get(k, 0) + 1 if breached and k in self.baseline else 0
            if self.streak[k] >= 2:
                bad.append(f"{k} {v['value']} (want {v['limit']})")
        if bad and all(b.startswith("handoff") for b in bad) and not self.takes_over(d.get("from") or ""):
            return "watch", ""  # (a hand-back to a code that re-attaches VM by VM would drop the pool: the breach alerted, not handed back)
        if bad and d.get("from") and (self.dir / "code" / d["from"] / "supercomputer").exists():
            d["state"] = "rejected"
            d["pending_handback"] = {"from": me, "to": d["from"], "retry_after": now + 15}
            d["rejected"] = sorted(set(d.get("rejected", [])) | {me})
            d["why"] = "; ".join(bad)
            self.write(d)
            return "handback", f"code {me} breached {'; '.join(bad)} within {(now - t0) / 60:.0f} min of the handoff: handed back to {d['from']}"
        if rules.handoff_can_pass(now - t0, WATCH_S, max(self.streak.get("handoff", 0), self.streak.get("status", 0)), self._handoff_hard_fault):
            d["state"] = "passed"
            d["passed"] = (d.get("passed", []) + [me])[-20:]  # (the codes whose canary passed, newest last: rollback's default)
            self.write(d)
            return "passed", f"code {me}: {WATCH_S / 60:.0f} min after the handoff, every target held"
        return "watch", ""
