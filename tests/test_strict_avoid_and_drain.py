"""11-16 13:03: strict nzdqwp placed on bx_zfa9fyk2 (old agent d235bb2fd3ac, boat's front failing it) went NOT_STARTED
after three owned_rpc 502s; and five loaded old-agent xlarge never upgraded (refilled whenever freed, the upgrade
waits for empty). A strict pre-launch the front failed keeps strict items off the VM a while; one loaded old-agent VM
at a time drains for its upgrade, bounded. Run on boat, like all test_*.py."""
import time
import unittest
from unittest.mock import patch

import test_owned_scheduler as fixture
from test_wiring import S, batch, sched, vm


class StrictFrontAvoid(unittest.TestCase):
    def test_a_front_failed_strict_prelaunch_keeps_strict_items_off_the_vm_a_while(self):
        case = fixture.OwnedSchedulerTests("test_queued_cancel_closes_admission_before_placement")
        case.setUp()
        try:
            s, it, v = case.s, case.it, case.vm
            case.place_without_thread()
            it.state = "starting"
            s._strict_prepare = lambda it, vm: (_ for _ in ()).throw(
                S.PrelaunchNotStarted("owned_rpc: 502 b'upstream unavailable\\n'"))
            s._start_on_inner(it, v)
            self.assertGreater(getattr(v, "strict_avoid_until", 0), time.time())
            self.assertTrue(S.Scheduler._strict_avoided(it, v, time.time()))
            legacy = batch(s, "legacy").items["i0"]
            self.assertFalse(S.Scheduler._strict_avoided(legacy, v, time.time()))
            self.assertFalse(S.Scheduler._strict_avoided(it, v, time.time() + S.STRICT_FRONT_AVOID_S + 1))
        finally:
            case.doCleanups()

    def test_an_unrelated_prelaunch_failure_avoids_nothing(self):
        s = sched()
        v = vm(s, "A")
        s._note_strict_front_failure(v, S.PrelaunchNotStarted("runtime pin mismatch"))
        self.assertEqual(getattr(v, "strict_avoid_until", 0), 0)


class AgentDrain(unittest.TestCase):
    def _pool(self):
        s = sched()
        s.agent_version = "new"
        b = batch(s, n=3)
        vms = []
        for i, it in enumerate(b.items.values()):
            v = vm(s, f"v{i}")
            v.agent = "old" if i < 2 else "new"
            with patch.object(S.threading, "Thread"):
                self.assertTrue(s._place(it, v))
            vms.append(v)
        return s, vms

    def test_one_loaded_old_agent_vm_at_a_time_drains_then_returns(self):
        s, (a, b_, c) = self._pool()
        now = time.time()
        s._agent_drain_tick(now)
        draining = [v.id for v in (a, b_, c) if getattr(v, "agent_draining", 0)]
        self.assertEqual(len(draining), 1, draining)
        d = a if a.id in draining else b_
        shape = S.fit_shape(batch(s, "next").items["i0"])
        self.assertFalse(S.vm_fits(shape, d, set()))  # (no new placement while it drains)
        s._agent_drain_tick(now + 1)
        self.assertEqual([v.id for v in (a, b_, c) if getattr(v, "agent_draining", 0)], [d.id])  # (still one)
        d.agent = "new"  # (its items ended, the empty-agent upgrade done)
        s._agent_drain_tick(now + 2)
        self.assertFalse(getattr(d, "agent_draining", 0))
        self.assertTrue(S.vm_fits(shape, d, set()))

    def test_a_drain_is_bounded(self):
        s, (a, b_, c) = self._pool()
        now = time.time()
        s._agent_drain_tick(now)
        d = next(v for v in (a, b_) if getattr(v, "agent_draining", 0))
        s._agent_drain_tick(now + S.AGENT_DRAIN_MAX_S + 1)
        self.assertFalse(getattr(d, "agent_draining", 0) and getattr(d, "agent_draining") <= now)
        self.assertNotEqual(getattr(d, "agent_draining", 0), now)  # (that drain ended unupgraded)


if __name__ == "__main__":
    unittest.main()
