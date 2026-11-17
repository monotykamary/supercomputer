"""11-17 03:25: a restart of the controller's scheduler released every idle VM, the pool down to 6 beside queued work.
A signalled exit (a restart) keeps idle VMs too, on a short TTL, for the next process to take back; an idle exit lets
them go (rules.keep_across_restart). And /policy: the controller's own policy, by its code. `python3 tests/test_restart_keep.py`."""
import json
from test_wiring import S, batch, sched, vm


def _calls(s):
    out = {}
    s._metrics_save = lambda: None
    s._let_go = lambda v, keep=False, keep_ttl=None, stop=False: out.__setitem__(v.id, (keep, keep_ttl))
    return out


def test_restart_keeps_idle_vms_on_a_short_ttl():
    s = sched()
    b = batch(s)
    busy, idle, dead = vm(s, "busy"), vm(s, "idle"), vm(s, "dead")
    busy.running[(b.id, "i0")] = b.items["i0"]
    dead.dead = True
    out = _calls(s)
    s.shutdown(restart=True)
    assert out["busy"] == (True, None), out  # (its items' TTL: the backstop, as before)
    assert out["idle"] == (True, S.RESTART_KEEP_S), out
    assert out["dead"][0] is False, out


def test_idle_exit_lets_idle_vms_go():
    s = sched()
    b = batch(s)
    busy, idle = vm(s, "busy"), vm(s, "idle")
    busy.running[(b.id, "i0")] = b.items["i0"]
    out = _calls(s)
    s.shutdown()
    assert out["busy"][0] is True and out["idle"][0] is False, out


def test_keep_rule():
    R = S.rules
    assert R.keep_across_restart(1, False) and not R.keep_across_restart(0, False)
    assert R.keep_across_restart(0, False, True) and not R.keep_across_restart(3, True, True)


def test_policy_request_reads_and_writes_the_policy_where_it_runs():
    S.POLICY.write_text(json.dumps({"hold_s": 2700, "generation": 4}))
    assert S.policy_request({})["generation"] == 4
    p = S.policy_request({"set": {"break_even_by_class": "submission=300"}})
    assert p["generation"] == 5 and p["break_even_by_class"] == {"submission": 300.0}
    for bad in ({"set": {"nope": 1}}, {"set": ["x"]}):
        try:
            S.policy_request(bad)
        except (SystemExit, ValueError):
            pass
        else:
            raise AssertionError(bad)


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"restart keep: {n} of {n}")
