"""Plan tracing and an opt-in test-process gate mutant; execute only on Boat."""
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))
from tests import scenarios

rows = []
original_sim = scenarios._sim


def traced_sim(code, **kwargs):
    if os.environ.get("SIM_VARIABLE_BOOT"):
        kwargs["cfg"] = {**kwargs.get("cfg", {}), "boot_s": (60, 120)}
    sim = original_sim(code, **kwargs)
    if os.environ.get("SIM_GATE_ECONOMIC_ONLY"):
        # Test-process-only mutant: removing deadline capacity must fail the
        # original scenario assertions. No production file is changed.
        sim.S.rules.gate_vms = lambda work, capacity, vm_work: 0
    original = sim.S.Scheduler._plan

    def plan(s, now):
        result = original(s, now)
        t = now - sim.k.t0
        if result or 650 <= t <= 900:
            rows.append({"t": t, "bring_up": s.bring_up, "target": s.target,
                         "output": [[str(key), value[0]] for key, value in result.items()],
                         "queue": len(s._pass_queue()),
                         "vms": [{"id": v.id, "ready": v.ready, "dead": v.dead,
                                  "lease_age": now-v.t_lease,
                                  "running": [{"name": x.name, "state": x.state,
                                               "age": now-(x.t_start or now), "est": s.est_of(x)}
                                              for x in v.running.values()]}
                                 for v in s.vms.values()]})
        return result

    sim.S.Scheduler._plan = plan
    return sim


scenarios._sim = traced_sim
out = pathlib.Path(os.environ["SUPERCOMPUTER_OUTPUT_DIR"])
try:
    result = scenarios.run_one("gate_68_slot_hours", str(pathlib.Path.cwd()))
    (out / "result.json").write_text(json.dumps(result, indent=2))
    print(json.dumps({"ok": result["ok"], "verdict": result["verdict"],
                      "created": result["metrics"]["created"],
                      "unused": result["metrics"]["unused_starts"]}))
    if os.environ.get("SIM_GATE_ECONOMIC_ONLY"):
        assert not result["verdict"]["near_gate_horizon"], "gate-only capacity mutant escaped detection"
        assert not result["ok"]
        print("PASS negative control: economic-only mutant violates gate horizon")
finally:
    (out / "plans.json").write_text(json.dumps(rows))
