"""The supervisor: a process of its own that watches the scheduler through /alive and heals it without a manual
restart (rules.supervise_action): a hot handoff for a stuck main loop or a poisoned subsystem, a restart of the exact
pid when it doesn't answer or the handoff didn't take. `python3 tests/test_supervisor.py`."""
import time
from unittest.mock import patch
from test_wiring import S, sched


class Fake:
    def __init__(self, alive):
        self.alive, self.calls = alive, []

    def req(self, path, msg, timeout=60):
        self.calls.append(path)
        if path == "/alive":
            if self.alive is None:
                raise RuntimeError("no answer")
            return dict(self.alive)
        return {"ok": True}


def run(fake, t, st=None):
    """One check at virtual time t, the supervisor's state carried across checks (as its loop does)."""
    with patch.object(S, "_restart_exact", lambda pid: f"restarted {pid}"):
        return S.supervise(once=True, client=fake, now=lambda: t)


def test_healthy_scheduler_is_left_alone():
    fake = Fake({"pid": 10, "pass_age_s": 1.0, "poisoned": [], "recovery_blocked": False})
    assert run(fake, 1000.0) == [] and fake.calls == ["/alive"]


def test_stuck_loop_gets_a_hot_handoff_first():
    fake = Fake({"pid": 10, "pass_age_s": 900.0, "poisoned": [], "recovery_blocked": False})
    acts = run(fake, 1000.0)
    assert [a["action"] for a in acts] == ["handoff"] and "/reload" in fake.calls, acts


def test_poisoned_subsystem_gets_a_hot_handoff():
    fake = Fake({"pid": 10, "pass_age_s": 1.0, "poisoned": ["thread sampler: 5 restarts"], "recovery_blocked": False})
    assert [a["action"] for a in run(fake, 1000.0)] == ["handoff"]


def test_quarantined_recovery_is_never_touched():
    fake = Fake({"pid": 10, "pass_age_s": 900.0, "poisoned": ["x"], "recovery_blocked": True})
    assert run(fake, 1000.0) == []


def test_silent_scheduler_is_restarted_after_the_miss_limit():
    R = S.rules
    assert R.supervise_action(False, S.SUP_MISSES, S.SUP_MISSES, False, False, False, -1, 240, 10 ** 6, 300) == "restart"
    assert R.supervise_action(False, S.SUP_MISSES - 1, S.SUP_MISSES, False, False, False, -1, 240, 10 ** 6, 300) == "wait"
    # (a handoff that didn't take: a restart; within its wait: wait; within the cooldown: wait)
    assert R.supervise_action(True, 0, 8, True, False, False, 300, 240, 10 ** 6, 300) == "restart"
    assert R.supervise_action(True, 0, 8, True, False, False, 100, 240, 10 ** 6, 300) == "wait"
    assert R.supervise_action(True, 0, 8, True, False, False, -1, 240, 10, 300) == "wait"


def test_alive_report_reads_no_lock_and_names_poisoned_threads():
    s = sched()
    s._t_alive = time.time() - 42
    s._supervised = {"sampler": {"thread": None, "target": None, "t": 0, "restarts": S.SUP_POISONED_RESTARTS}}
    s.lock.acquire()  # (a wedged loop holding the lock: /alive still answers)
    try:
        r = s.alive_report()
    finally:
        s.lock.release()
    assert r["pass_age_s"] >= 42 and r["poisoned"] and r["pid"] > 0, r


def test_controller_scheduler_keeps_one_supervisor_of_its_code():
    s = sched()
    s.idle_exit, s.code = 0, "abc"
    with patch.object(S.subprocess, "Popen") as popen, patch.object(S, "_is_supervisor", return_value=False):
        s._supervisor_tick(time.time())
    assert popen.called and "supervise" in popen.call_args[0][0]
    s2 = sched()
    s2.idle_exit, s2.code = 120, "abc"  # (a laptop's scheduler: none)
    with patch.object(S.subprocess, "Popen") as popen:
        s2._supervisor_tick(time.time())
    assert not popen.called


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"supervisor: {n} of {n}")
