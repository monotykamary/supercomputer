"""health.py and canary.py on their own: a client gone mid-answer is no error (02:38: a canary handed back for one
BrokenPipe on /poll); a breach alerts after 2 checks in a row, once; the canary hands back only on a target that held
before the handoff, passes after its 10 minutes, and rejects the code it handed back. `python3 tests/test_health.py`."""
import os
import pathlib
import sys
import tempfile
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from supercomputer import canary, health  # noqa: E402


def test_client_gone_is_no_error():
    assert health.client_gone(BrokenPipeError(32, "Broken pipe")) and health.client_gone(ConnectionResetError())
    assert not health.client_gone(KeyError("batch")) and not health.client_gone(RuntimeError("x"))


def test_alert_after_two_checks():
    # This fixture measures status alerts, not the validation runner's flags.
    with patch.object(health, "env_flags", return_value=[]):
        h = health.Health()
        h.record_status(1000.0, 2.5)
        assert h.tick(1001.0) == []
        a = h.tick(1061.0)
        assert len(a) == 1 and "status" in a[0], a
        assert h.tick(1121.0) == []  # (said once until it clears)


def test_controller_flag_is_an_independent_breach():
    with patch.dict(os.environ, {"SUPERCOMPUTER_CONTROLLER": "1", "SUPERCOMPUTER_SCHEDULER": "1",
                                 "SUPERCOMPUTER_URGENT": "0", "GDPW_BOAT_URGENT": "0"}):
        h = health.Health()
        h.record_status(1000.0, 2.5)
        assert h.tick(1001.0) == []
        alerts = h.tick(1061.0)
        assert len(alerts) == 2, alerts
        assert any("status breached" in a for a in alerts), alerts
        assert any("env breached" in a for a in alerts), alerts
        assert h.tick(1121.0) == []


def test_canary_hands_back_on_a_new_breach_only():
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "code" / "old" / "supercomputer").mkdir(parents=True)
    c = canary.Canary(d)
    h = health.Health()
    h.record_status(500.0, 0.05)  # (the hour before: status held)
    c.handing_off("old", "new", 1000.0)
    assert c.tick("new", 1060.0, h) == ("watch", "")
    h.record_status(1070.0, 3.0)
    assert c.tick("new", 1120.0, h)[0] == "watch"
    what, why = c.tick("new", 1180.0, h)
    assert what == "handback" and "status" in why, (what, why)
    assert c.rejected("new") and c.read()["state"] == "rejected"


def test_canary_passes_and_ignores_old_breaches():
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "code" / "old" / "supercomputer").mkdir(parents=True)
    c = canary.Canary(d)
    h = health.Health()
    h.record_status(500.0, 4.0)  # (status breached before the handoff already: not the new code's)
    c.handing_off("old", "new", 1000.0)
    h.record_status(1010.0, 4.0)
    for t in (1060.0, 1120.0, 1180.0):
        assert c.tick("new", t, h)[0] == "watch"
    assert c.tick("new", 1601.0, h)[0] == "passed" and not c.rejected("new")


def test_canary_without_records_before_judges_no_load_target():
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "code" / "old" / "supercomputer").mkdir(parents=True)
    c = canary.Canary(d)
    h = health.Health()  # (the previous code recorded nothing)
    c.handing_off("old", "new", 1000.0)
    h.record_status(1010.0, 1.2)
    assert [c.tick("new", t, h)[0] for t in (1060.0, 1120.0, 1180.0)] == ["watch"] * 3
    h.record_error(1190.0, "/submit: KeyError('batch')")
    assert c.tick("new", 1240.0, h)[0] == "watch" and c.tick("new", 1300.0, h)[0] == "handback"


def test_placed_counted_by_the_minute():
    h = health.Health()
    for t in range(600, 720):
        h.record_placed(float(t))
    assert len(h.ev["placed"]) == 2 and sum(e["n"] for e in h.ev["placed"]) == 120


def test_canary_churn_only_of_its_own_releases():
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "code" / "old" / "supercomputer").mkdir(parents=True)
    c = canary.Canary(d)
    h = health.Health()
    h.record_status(500.0, 0.05)
    h.record_release(900.0, "img", "v1")  # (the previous code's release)
    c.handing_off("old", "new", 1000.0)
    h.record_start(1030.0, "img", "v1")
    assert [c.tick("new", t, h)[0] for t in (1060.0, 1120.0, 1180.0)] == ["watch"] * 3
    h.record_release(1200.0, "img", "v2")  # (its own)
    h.record_start(1230.0, "img", "v2")
    assert c.tick("new", 1260.0, h)[0] == "watch" and c.tick("new", 1320.0, h)[0] == "handback"


def test_churn_counts_each_release_once():
    h = health.Health()
    h.record_release(1000.0, "img", "v1")
    for k in range(30):
        h.record_start(1100.0 + k, "img", f"n{k}")
    assert h.check(1200.0, 30)["targets"]["churn"]["value"] == 1


def test_billing_breaches():
    ok = {"canStart": True, "billingStatus": "active", "keepGoing": {"capReached": False}, "wallet": {"balanceDollars": 12.5}, "planIncluded": {"vmHoursRemaining": 11000}}
    assert health.billing_problems(ok) == []
    bad = {**ok, "canStart": False, "startBlockedReason": "payment", "keepGoing": {"capReached": True}, "billingStatus": "past_due",
           "wallet": {"balanceDollars": -3}, "planIncluded": {"vmHoursRemaining": 120}}
    assert len(health.billing_problems(bad)) == 3
    h = health.Health()
    h.record_billing(1000.0, health.billing_problems(bad))
    r = h.check(1010.0, 30)["targets"]["billing"]
    assert not r["ok"] and r["value"] == 3, r



def test_busy_over_what_real_work_could_use_and_waste():
    """11-01 R2: busy is over what real work could use (one item queued, two running: 3 slots, not the 100 the hold keeps);
    slots idle or on filler while work that fits waited are waste, its own target; samples before qslots count every
    held slot, as before."""
    h = health.Health()
    for i in range(30):  # (one item queued, 2 slots on real work of 100, the rest the hold's filler)
        h.record_sample(1000.0 + 15 * i, {"img": {"slots": 100, "busy": 2, "queued": 1, "qslots": 1}})
    r = h.check(1460.0, 30)["targets"]
    assert r["busy"]["value"] == 0.667 and r["busy"]["detail"]["pool_share"] == 0.02, r["busy"]  # (2 of the 3 it needs; of the pool, 0.02)
    assert r["waste"]["value"] == 0 and r["waste"]["ok"], r["waste"]
    h2 = health.Health()
    for i in range(30):  # (a queue past the pool, 40 slots idle or on filler while it waited: busy 0.6, waste 40)
        h2.record_sample(1000.0 + 15 * i, {"img": {"slots": 100, "busy": 60, "queued": 500, "qslots": 500, "waste": 40}})
    r2 = h2.check(1460.0, 30)["targets"]
    assert not r2["busy"]["ok"] and r2["busy"]["value"] == 0.6, r2["busy"]
    assert not r2["waste"]["ok"] and r2["waste"]["value"] == 40, r2["waste"]
    h3 = health.Health()
    for i in range(30):  # (an older code's samples: every held slot, no waste measured)
        h3.record_sample(1000.0 + 15 * i, {"img": {"slots": 100, "busy": 2, "queued": 1}})
    r3 = h3.check(1460.0, 30)["targets"]
    assert r3["busy"]["value"] == 0.02 and r3["waste"]["value"] is None, (r3["busy"], r3["waste"])


def test_canary_judges_only_measured_targets():
    """A target with no value in the hour before (waste, before its code) is not judged by the canary."""
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "code" / "old" / "supercomputer").mkdir(parents=True)
    c = canary.Canary(d)
    h = health.Health()
    h.record_status(500.0, 0.05)
    h.record_sample(600.0, {"img": {"slots": 4, "busy": 4, "queued": 1}})
    c.handing_off("old", "new", 1000.0)
    assert c.tick("new", 1060.0, h) == ("watch", "")
    assert "waste" not in c.baseline, c.baseline
    for i in range(8):
        h.record_sample(1065.0 + 15 * i, {"img": {"slots": 4, "busy": 2, "queued": 3, "qslots": 3, "waste": 2}})
    assert c.tick("new", 1120.0, h)[0] == "watch"
    assert c.tick("new", 1180.0, h)[0] == "watch"


def test_reasonless_preparation_strike_is_ours():
    h = health.Health()
    h.record_strike(1000.0, "b/x", "bx_1", "prepare on bx_1 failed: its preparation failed: ")
    h.record_strike(1000.0, "b/y", "bx_1", "prepare on bx_1 failed: its preparation failed: bx_1: prep: 500 b'boom'")
    assert h.check(1010.0, 30)["targets"]["ours"]["value"] == 1



def test_canary_hands_back_a_handoff_that_dropped_vms_at_once():
    """11-01 14:30: a switch that leaves VMs not carried over or orphaned is handed back at its first check, whatever
    the hour before held; one that carried every VM is not."""
    d = pathlib.Path(tempfile.mkdtemp())
    (d / "code" / "old" / "supercomputer").mkdir(parents=True)
    c = canary.Canary(d)
    c.handing_off("old", "new", 1000.0)
    assert c.handoff_verdict("new", 0, 2, "2 idle") == ("", "")  # (idle slots: judged with the other targets, not at once)
    what, why = c.handoff_verdict("new", 176, 0, "176 orphans")
    assert what == "alert" and "re-attaches VM by VM" in why, (what, why)  # (the old code doesn't take over whole: no hand-back to it)
    (d / "code" / "old" / "supercomputer" / "scheduler.py").write_text("def _take_pool(self): pass")
    what, why = c.handoff_verdict("new", 176, 0, "176 orphans")
    assert what == "handback" and "176 orphans" in why, (what, why)
    assert c.rejected("new")
    h = health.Health()
    h.record_handoff(1000.0, 225, 43, 0, 176, 0)
    r = h.check(1010.0, 30)["targets"]["handoff"]
    assert not r["ok"] and r["value"] == 176, r
    h2 = health.Health()
    h2.record_handoff(1000.0, 225, 225, 0, 0, 0)
    assert h2.check(1010.0, 30)["targets"]["handoff"]["ok"]


def test_client_4xx_visible_but_never_server_errors_or_canary_targets():
    with tempfile.TemporaryDirectory() as tmp, patch.object(health, "env_flags", return_value=[]):
        root = pathlib.Path(tmp)
        (root / "code/old/supercomputer").mkdir(parents=True)
        h = health.Health(root / "health.json")
        c = canary.Canary(root)
        c.handing_off("old", "new", 1000.0)
        for code in (400, 403, 404, 408, 429):
            for n in range(30):
                h.record_client_error(1010 + n, code, "/poll", "1", 1, "local", "legacy")
        report = h.check(1060)
        assert report["ok"] and report["targets"]["errors"]["value"] == 0
        assert report["client_4xx"]["count"] == 150
        assert report["client_4xx"]["by_status"] == {str(code): 30 for code in (400, 403, 404, 408, 429)}
        assert len(report["client_4xx"]["recent"]) == 4
        assert "150" in health.show(report)
        assert [c.tick("new", t, h)[0] for t in (1060, 1120, 1180)] == ["watch"] * 3
        assert c.tick("new", 1601, h)[0] == "passed"
        h.save()
        loaded = health.Health(h.path)
        assert loaded.client_errors(1601)["count"] == 150
        assert loaded.client_errors(4000)["count"] == 0


def test_client_error_metadata_is_bounded_and_5xx_cannot_hide_there():
    h = health.Health()
    h.record_client_error(1000, 400, "/" * 1000, "9" * 1000, 1, "p" * 1000, "a" * 1000)
    row = h.client_errors(1010)["recent"][0]
    assert len(row["path"]) == 120 and len(row["declared"]) == 32
    assert len(row["peer"]) == 80 and len(row["agent"]) == 120
    for code in (200, 302, 500, 503):
        try:
            h.record_client_error(1000, code, "/poll", "1", 1, "local", "legacy")
        except ValueError:
            pass
        else:
            raise AssertionError(f"non-client HTTP {code} hidden from server health")
    assert h.client_errors(1010)["count"] == 1


def _watching_canary(root, started=1000.0):
    (root / "code/old/supercomputer").mkdir(parents=True)
    (root / "code/old/supercomputer/scheduler.py").write_text("def _take_pool(self): pass")
    c = canary.Canary(root)
    c.handing_off("old", "new", started)
    return c


def test_exact_0554_handoff_trace_is_not_double_counted():
    with tempfile.TemporaryDirectory() as tmp:
        c = _watching_canary(pathlib.Path(tmp), 1794376440.6326313)
        h = health.Health()
        # Actual 05:54:25 idle=0, 05:54:40 idle=1; the old rolling maximum
        # reused the same 05:54:40 event at 05:55:10 and 05:56:10.
        h.record_handoff(1794376465.13, 128, 128, 0, 0, 0)
        h.record_handoff(1794376480.15, 128, 128, 0, 0, 1)
        assert c.tick("new", 1794376510, h)[0] == "watch"
        assert c.streak["handoff"] == 1
        assert c.tick("new", 1794376570, h)[0] == "watch"
        assert c.streak["handoff"] == 0
        assert h.check(1794376570)["targets"]["handoff"]["value"] == 1  # history remains visible
        assert c.tick("new", 1794377100, h)[0] == "passed"
        assert not c.rejected("new")


def test_completed_handoff_and_client_events_are_never_rounded_into_future():
    h = health.Health()
    for stamp in (1000.004, 1000.006):
        h.record_client_error(stamp, 400, "/poll", "1", 1, "p", "a")
        h.record_handoff(stamp, 1, 1, 0, 0, 1)
        r = h.check(stamp, handoff_since=stamp - 0.001)["targets"]["handoff"]
        assert r["value"] == 1 and r["detail"][-1]["t"] == stamp
    assert h.client_errors(1000.006)["count"] == 2


def test_handoff_check_intervals_exclude_the_previous_boundary():
    h = health.Health()
    h.record_handoff(1050, 1, 1, 0, 0, 1)
    h.record_handoff(1060, 1, 1, 0, 0, 1)
    assert h.check(1060, handoff_since=1000)["targets"]["handoff"]["value"] == 1
    assert h.check(1120, handoff_since=1060)["targets"]["handoff"]["value"] is None
    assert h.check(1050, handoff_since=1000)["targets"]["handoff"]["detail"][-1]["t"] == 1050


def test_persistent_handoff_idle_requires_two_fresh_intervals():
    with tempfile.TemporaryDirectory() as tmp:
        c, h = _watching_canary(pathlib.Path(tmp)), health.Health()
        h.record_handoff(1055, 128, 128, 0, 0, 1)
        assert c.tick("new", 1060, h)[0] == "watch"
        h.record_handoff(1115, 128, 128, 0, 0, 1)
        what, why = c.tick("new", 1120, h)
        assert what == "handback" and "handoff 1" in why


def test_missing_or_orphan_handoff_rejects_on_first_sample_even_at_deadline():
    for missing, orphan in ((1, 0), (0, 1)):
        for check_at in (1060, 1601):
            with tempfile.TemporaryDirectory() as tmp:
                c, h = _watching_canary(pathlib.Path(tmp)), health.Health()
                h.record_handoff(check_at - 1, 128, 128 - missing, missing, orphan, 0)
                assert c.tick("new", check_at, h)[0] == "handback"
                assert c.rejected("new")


def test_unsafe_rollback_never_turns_a_hard_fault_into_later_acceptance():
    for direct in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            c, h = _watching_canary(root), health.Health()
            (root / "code/old/supercomputer/scheduler.py").write_text("# cannot take over atomically")
            if direct:
                assert c.handoff_verdict("new", 1, 0, "missing VM")[0] == "alert"
            else:
                h.record_handoff(1055, 1, 0, 1, 0, 0)
                assert c.tick("new", 1060, h)[0] == "watch"
            assert c.tick("new", 1120, h)[0] == "watch"
            assert c.tick("new", 1601, h)[0] == "watch"
            assert c.read()["state"] != "passed"


def test_pending_idle_at_pass_horizon_is_confirmed_before_acceptance():
    for persists in (False, True):
        with tempfile.TemporaryDirectory() as tmp:
            c, h = _watching_canary(pathlib.Path(tmp)), health.Health()
            h.record_handoff(1599, 1, 1, 0, 0, 1)
            assert c.tick("new", 1601, h)[0] == "watch"
            if persists:
                h.record_handoff(1659, 1, 1, 0, 0, 1)
            assert c.tick("new", 1661, h)[0] == ("handback" if persists else "passed")


def test_restarted_canary_cannot_make_one_idle_event_a_streak():
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        c, h = _watching_canary(root), health.Health()
        h.record_handoff(1055, 1, 1, 0, 0, 1)
        assert c.tick("new", 1060, h)[0] == "watch"
        restored = canary.Canary(root)
        assert restored.tick("new", 1120, h)[0] == "watch"
        assert restored.tick("new", 1180, h)[0] == "watch"
        assert restored.streak["handoff"] == 0


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"health: {n} of {n}")
