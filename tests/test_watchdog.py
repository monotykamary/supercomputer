"""The watchdog: a long-lived scheduler thread that dies is started again (rules.respawn_due). `python3 tests/test_watchdog.py`."""
import threading
import time
from test_wiring import S, sched


def test_dead_thread_started_again_with_backoff():
    s = sched()
    runs, gate = [], threading.Event()

    def flaky():
        runs.append(1)
        if len(runs) == 1:
            raise RuntimeError("boom")  # (the first run dies of an exception)
        gate.wait(5)

    hook, threading.excepthook = threading.excepthook, lambda a: None
    try:
        s._supervise("sampler", flaky)
        s._supervised["sampler"]["thread"].join(2)
        assert not s._supervised["sampler"]["thread"].is_alive()
        now = time.time()
        s._watchdog_tick(now)  # (within its 5 s backoff: not yet)
        assert len(runs) == 1 and s._supervised["sampler"]["restarts"] == 0
        s._watchdog_tick(now + 6)
        assert s._supervised["sampler"]["restarts"] == 1
        for _ in range(100):
            if len(runs) == 2:
                break
            time.sleep(0.01)
        assert len(runs) == 2 and s._supervised["sampler"]["thread"].is_alive()
        s._watchdog_tick(now + 600)  # (alive: never started twice)
        assert len(runs) == 2 and s._supervised["sampler"]["restarts"] == 1
    finally:
        gate.set()
        threading.excepthook = hook


def test_no_restart_while_stopping_or_handing_off():
    s = sched()
    s._supervise("heartbeat", lambda: None)  # (ends at once, as if dead)
    s._supervised["heartbeat"]["thread"].join(2)
    s.handing_off = True
    s._watchdog_tick(time.time() + 600)
    assert s._supervised["heartbeat"]["restarts"] == 0
    s.handing_off = False
    s.stop.set()
    s._watchdog_tick(time.time() + 600)
    assert s._supervised["heartbeat"]["restarts"] == 0


def test_dead_poller_replaced_in_the_pool():
    s = sched()
    s.POLLERS = 2
    s._pcv()
    dead = s._pollers[0]
    s.stop.set()  # (the pollers return)
    for t in s._pollers:
        t.join(3)
    s.stop.clear()
    s._watchdog_tick(time.time() + 600)
    assert dead not in s._pollers and len(s._pollers) == 2 and all(t.is_alive() for t in s._pollers)
    s.stop.set()


def test_respawn_rule():
    R = S.rules
    assert R.respawn_due(False, False, 10, 5)
    assert not R.respawn_due(True, False, 10, 5)
    assert not R.respawn_due(False, True, 10, 5)
    assert not R.respawn_due(False, False, 1, 5)


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"watchdog: {n} of {n}")
