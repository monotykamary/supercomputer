"""Deterministic unit sequences for the scheduler's handoff drain state."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer.rules import HandoffDrain


class HandoffDrainStateTests(unittest.TestCase):
    def assert_state(self, drain, *, pending, freezing, started, deadline, generation):
        self.assertEqual(vars(drain), {
            "pending": pending, "freezing": freezing, "started": started,
            "deadline": deadline, "generation": generation,
        })

    def test_initial_state_and_idle_operations(self):
        drain = HandoffDrain()
        self.assert_state(drain, pending=False, freezing=False, started=0.0,
                          deadline=0.0, generation=0)
        self.assertTrue(drain.growth_allowed())
        self.assertTrue(drain.placement_allowed())
        for now in (-1.0, 0.0, 1000.0):
            self.assertFalse(drain.expired(now))
        self.assertFalse(drain.freeze())
        self.assertFalse(drain.finish(1))
        self.assertTrue(drain.finish(0))
        self.assert_state(drain, pending=False, freezing=False, started=0.0,
                          deadline=0.0, generation=0)

    def test_repeated_target_observations_never_extend_deadline(self):
        # The target belongs to Scheduler, not HandoffDrain. Each observation
        # below represents another begin for the same still-pending target.
        for started in (-500.0, 0.0, 123.25):
            with self.subTest(started=started):
                drain = HandoffDrain()
                generation = drain.begin(started)
                self.assertEqual(generation, 1)
                expected = vars(drain).copy()
                for now in (started, started + 1.0, started + 299.75,
                            started + 300.0, started + 10000.0):
                    self.assertEqual(drain.begin(now), generation)
                    self.assertEqual(vars(drain), expected)
                    self.assertEqual(drain.expired(now), now >= started + 300.0)
                    self.assertFalse(drain.growth_allowed())
                    self.assertTrue(drain.placement_allowed())
                self.assertTrue(drain.freeze())
                frozen = vars(drain).copy()
                self.assertEqual(drain.begin(started + 20000.0), generation)
                self.assertEqual(vars(drain), frozen)
                self.assertFalse(drain.placement_allowed())

    def test_expiry_is_inclusive_pending_and_nonmutating(self):
        drain = HandoffDrain()
        generation = drain.begin(10.25)
        expected = vars(drain).copy()
        for now, expired in ((310.0, False), (310.25, True), (311.0, True)):
            self.assertEqual(drain.expired(now), expired)
            self.assertEqual(vars(drain), expected)
        # Time alone neither freezes placement nor resumes growth.
        self.assertTrue(drain.placement_allowed())
        self.assertFalse(drain.growth_allowed())
        self.assertTrue(drain.finish(generation))
        self.assertFalse(drain.expired(10000.0))

    def test_freeze_requires_pending_and_is_idempotent(self):
        drain = HandoffDrain()
        self.assertFalse(drain.freeze())
        generation = drain.begin(50.0)
        for _ in range(2):
            self.assertTrue(drain.freeze())
            self.assertTrue(drain.pending)
            self.assertFalse(drain.growth_allowed())
            self.assertFalse(drain.placement_allowed())
        self.assertTrue(drain.finish(generation))
        self.assertFalse(drain.freeze())
        self.assertTrue(drain.placement_allowed())

    def test_stale_finish_preserves_open_and_frozen_drain(self):
        for frozen in (False, True):
            with self.subTest(frozen=frozen):
                drain = HandoffDrain()
                generation = drain.begin(42.0)
                if frozen:
                    drain.freeze()
                expected = vars(drain).copy()
                for stale in (-1, 0, generation + 1):
                    self.assertFalse(drain.finish(stale))
                    self.assertEqual(vars(drain), expected)
                self.assertTrue(drain.finish(generation))
                self.assert_state(drain, pending=False, freezing=False, started=42.0,
                                  deadline=342.0, generation=generation)
                self.assertTrue(drain.growth_allowed())
                self.assertTrue(drain.placement_allowed())
                self.assertTrue(drain.finish(generation))

    def test_safe_resumption_uses_new_generation_and_window(self):
        drain = HandoffDrain()
        previous = drain.begin(5.0)
        drain.freeze()
        self.assertTrue(drain.finish(previous))
        current = drain.begin(1000.0)
        self.assertEqual(current, previous + 1)
        self.assert_state(drain, pending=True, freezing=False, started=1000.0,
                          deadline=1300.0, generation=current)
        expected = vars(drain).copy()
        self.assertFalse(drain.finish(previous))
        self.assertEqual(vars(drain), expected)
        self.assertFalse(drain.expired(1299.0))
        self.assertTrue(drain.expired(1300.0))
        self.assertTrue(drain.finish(current))
        self.assertTrue(drain.growth_allowed())
        self.assertTrue(drain.placement_allowed())

    def test_drain_never_mutates_external_ongoing_work_counters(self):
        drain = HandoffDrain()
        # Representative Scheduler-owned reservations/attempt counters: drain
        # state must not reset them to make in-flight work appear settled.
        counters = {"creating": 2, "booting": 3, "acquiring": 1,
                    "starting": 4, "running": 5, "completed": 6}
        expected = counters.copy()
        generation = drain.begin(20.0)
        self.assertEqual(counters, expected)
        drain.begin(400.0)
        self.assertEqual(counters, expected)
        self.assertTrue(drain.expired(400.0))
        self.assertFalse(drain.growth_allowed())
        self.assertTrue(drain.placement_allowed())
        # Existing work can progress independently while new growth is gated.
        counters["starting"] -= 1
        counters["running"] += 1
        expected = counters.copy()
        drain.freeze()
        self.assertEqual(counters, expected)
        self.assertFalse(drain.finish(generation - 1))
        self.assertEqual(counters, expected)
        self.assertTrue(drain.finish(generation))
        self.assertEqual(counters, expected)
        self.assertTrue(drain.growth_allowed())
        self.assertTrue(drain.placement_allowed())
        self.assertEqual(drain.begin(500.0), generation + 1)
        self.assertEqual(counters, expected)


if __name__ == "__main__":
    unittest.main()
