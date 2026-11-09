"""`supercomputer --skill`: SKILL.md (the agents' guide, the source of truth for them) with live values filled in:
the policy's knobs as the controller runs them, today's starts from `boat limits`, and whether the controller is up.
Each value that can't be had cheaply (a few seconds at most) prints as "?"."""
import concurrent.futures as cf
import contextlib
import json
import pathlib
import re

from . import config


def _controller() -> dict:
    """The controller as the skill reports it (rules.ops.controller_state): off here, its VM only (no forward), up (its
    status answered), or not answering."""
    from . import scheduler
    from .rules import ops
    c = scheduler.controller()
    forward = bool(c) and scheduler._healthy(scheduler.FWD_PORT, 2)
    st = None
    if c and forward:
        with contextlib.suppress(Exception):
            st = scheduler.Client(c).req("/status", {}, timeout=6)
    what = ops.controller_state(bool(c), forward, st is not None)
    if what == "off":  # (the scheduler on this machine owns the pool)
        with contextlib.suppress(Exception):
            if scheduler.running():
                st = scheduler.Client().req("/status", {}, timeout=6)
                p = st.get("pool", {})
                return {"state": f"this machine's scheduler (pid {st.get('pid')}) owns the pool: {p.get('ready', '?')} VMs ready"
                        + (", circuit breaker OPEN" if st.get("breaker", {}).get("open") else ""), "knobs": st.get("knobs", {}),
                        "floor": (st.get("knobs") or {}).get("floor", "?")}
        return {"state": "this machine's scheduler owns the pool (not running now: the next batch starts it)"}
    if what == "vm_only":  # (no forward up: boat's view of the VM only, no forward started for this)
        from . import boat
        with contextlib.suppress(Exception):
            i = boat.bj("info", c["vm"], timeout=10)
            return {"state": f"its VM {(i.get('sandbox') or i).get('state', '?')} (`supercomputer controller status` for its services)"}
        return {"state": "?"}
    if what == "up":
        p = st.get("pool", {})
        return {"state": f"up, {p.get('ready', '?')} VMs ready" + (", circuit breaker OPEN" if st.get("breaker", {}).get("open") else "")
                + (", shedding load" if st.get("shed") else ""), "knobs": st.get("knobs", {}), "floor": max((p.get("floor") or {"": "?"}).values(), default="?")}
    return {"state": "not answering (commands fall back to this machine's scheduler)"}


def _limits() -> str:
    from . import boat
    lim = boat.limits()
    st, w = lim["starts"], lim.get("startLimitsWithWallet") or {}
    plan = lim.get("planIncluded") or {}
    hours = float(plan.get("vmHoursRemaining") or 0) + float(lim.get("packBalanceHours", lim.get("creditBalanceHours", 0)) or 0)
    return (f"Today: {st['day']['used']} of {st['day']['limit']} free starts used (past them, paid up to {w.get('perDay', '?')} a day), "
            f"{hours:.0f} prepaid VM-hours left (plan + pack; plan period ends {plan.get('periodEnd', 'unknown')[:10]}).")


def text() -> str:
    from . import scheduler
    c = config.LIVE / "controller.json"
    info = json.loads(c.read_text()) if c.exists() else {}
    ex = cf.ThreadPoolExecutor(2)
    fc, fl = ex.submit(_controller), ex.submit(_limits)
    try:
        ctl = fc.result(timeout=10)
    except Exception:
        ctl = {"state": "?"}
    try:
        lim = fl.result(timeout=8)
    except Exception:
        lim = "Today's limits: ? (`boat limits`)."
    ex.shutdown(wait=False)
    k = ctl.get("knobs") or {}
    vals = {"live_now": f"{ctl['state']}.", "controller_vm": info.get("vm", "?"), "controller_state": ctl["state"], "key_id": info.get("key_id", "<id>"),
            "floor": ctl.get("floor", "8, the treadmill's default"), "max_vms": "none set" if not k.get("max_vms") else f"a ceiling of {k['max_vms']} set", "bringup": k.get("bringup", scheduler.BRINGUP_MAX),
            "gate_horizon": round(k.get("gate_horizon_s", scheduler.GATE_HORIZON) / 60), "horizon": round(k.get("horizon_s", scheduler.HORIZON) / 60),
            "hold": round(k.get("hold_s", scheduler.HOLD) / 60), "sub_vms": k.get("sub_vms", scheduler.SUB_VMS), "sub_preempt": round(k.get("sub_preempt_s", scheduler.SUB_PREEMPT)), "limits": lim}
    # (a checkout: SKILL.md at its root; an installed wheel: inside the package)
    skill = next((p for p in (config.SRC / "SKILL.md", pathlib.Path(__file__).parent / "SKILL.md") if p.exists()), config.SRC / "SKILL.md")
    return re.sub(r"\{([a-z_]+)\}", lambda m: str(vals.get(m.group(1), m.group(0))), skill.read_text())
