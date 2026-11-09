"""The scheduler's pass at the live pool's size (200 VMs, ~8.8k items queued: 11-01 08:41) in the simulator, its stacks sampled
every 2 ms: CPU by function of supercomputer/ (self and inclusive). Run it on boat (a job): `python3 tests/sim/profile_pass.py . 1200`."""
import sys, os, threading, time, collections
code = os.path.abspath(sys.argv[1]); vseconds = float(sys.argv[2]) if len(sys.argv) > 2 else 1800
sys.path.insert(0, code)
os.chdir(code)
import tests.scenarios as S
from tests.sim.harness import items
sim = S._sim(code)
S._warm(sim, 200)
sim.treadmill(base_vms=200, filler_n=400, filler_s=900, backlog=True)
sim.scheduler()
sim.status_monitor(every=30)
for j in range(7):
    sim.at(30 + j, lambda j=j: sim.batch(f"lane-{j}", "submission", items(f"s{j}", 500, 600)))
for j in range(70):
    sim.at(40 + j, lambda j=j: sim.batch(f"fill-{j}", "filler", items(f"f{j}", 75, 900)))
for j in range(10):
    sim.at(300 + 60 * j, lambda j=j: sim.batch(f"bar-{j}", "gate", items(f"g{j}", 100, 90)))
selfc, incl, total = collections.Counter(), collections.Counter(), [0, 0]
stop = False
me = threading.get_ident()
def sampler():
    while not stop:
        time.sleep(0.002)
        for tid, f in sys._current_frames().items():
            if tid == me or tid == threading.get_ident():
                continue
            fr, stack = f, []
            while fr:
                stack.append(fr); fr = fr.f_back
            top = stack[0].f_code.co_filename
            if "kernel.py" in top or "threading.py" in top and stack[0].f_code.co_name in ("wait", "_wait_for_tstate_lock"):
                continue
            total[0] += 1
            seen = set(); first = True
            for fr in stack:
                fn = fr.f_code.co_filename
                if "/supercomputer/" in fn and "/tests/" not in fn:
                    key = f"{os.path.basename(fn)}:{fr.f_code.co_name}"
                    if first:
                        selfc[key] += 1; first = False
                    if key not in seen:
                        incl[key] += 1; seen.add(key)
            if first:
                total[1] += 1
threading.Thread(target=sampler, daemon=True).start()
t0 = time.time(); c0 = time.process_time()
sim.run(vseconds, real_limit=float(os.environ.get("SIM_REAL_LIMIT", "1500")))
stop = True
print(f"real {time.time()-t0:.1f}s cpu {time.process_time()-c0:.1f}s samples {total[0]} (non-supercomputer {total[1]})")
print("inclusive:"); [print(f"  {n:6d} {k}") for k, n in incl.most_common(25)]
print("self (deepest supercomputer frame):"); [print(f"  {n:6d} {k}") for k, n in selfc.most_common(20)]
sim.cleanup()
