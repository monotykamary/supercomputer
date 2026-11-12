"""Readiness regression: exec true may work before resumed disks finish hydration."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import boat


class HydrationReadiness(unittest.TestCase):
    def run_wait(self, observation, timeout=480, probe=None):
        now, calls = [0.0], []
        def execute(vm, argv, limit):
            calls.append((now[0], vm, argv, limit))
            return probe(len(calls)) if probe else {"exitCode": 0}
        def sleep(seconds):
            now[0] += seconds
        with patch.object(boat.time, 'time', side_effect=lambda: now[0]), \
             patch.object(boat.time, 'sleep', side_effect=sleep), \
             patch.object(boat, 'states', side_effect=lambda: observation(now[0])), \
             patch.object(boat, 'exec_', side_effect=execute):
            result = boat.wait_running(['resumed'], timeout)
        return result, calls, now[0]

    def test_waits_for_resumed_disk_not_just_true_exec(self):
        result, calls, elapsed = self.run_wait(lambda t: {'resumed': {'state': 'running', 'hydrated': t >= 420}})
        self.assertEqual(result, {'resumed': 420.0})
        self.assertEqual(calls, [(420.0, 'resumed', ['true'], 30)])
        self.assertEqual(elapsed, 420.0)

    def test_unhydrated_disk_expires_with_existing_eight_minute_budget(self):
        result, calls, elapsed = self.run_wait(lambda t: {'resumed': {'state': 'idle', 'hydrated': False}})
        self.assertEqual(result, {})
        self.assertEqual(calls, [])
        self.assertEqual(elapsed, 480.0)

    def test_unknown_hydration_is_not_readiness(self):
        for value in (None, 1, 'true', 'false'):
            with self.subTest(value=value):
                result, calls, _ = self.run_wait(lambda t: {'resumed': {'state': 'ready', 'hydrated': value}}, timeout=6)
                self.assertEqual((result, calls), ({}, []))
        result, calls, _ = self.run_wait(lambda t: {'resumed': {'state': 'running'}}, timeout=6)
        self.assertEqual((result, calls), ({}, []))

    def test_hydration_does_not_replace_running_state_or_exec_probe(self):
        for state in ('stopped', 'starting', 'error'):
            with self.subTest(state=state):
                result, calls, _ = self.run_wait(lambda t: {'resumed': {'state': state, 'hydrated': True}}, timeout=6)
                self.assertEqual((result, calls), ({}, []))
        result, calls, elapsed = self.run_wait(
            lambda t: {'resumed': {'state': 'idle', 'hydrated': True}},
            probe=lambda n: {'exitCode': 1 if n == 1 else 0})
        self.assertEqual(result, {'resumed': 3.0})
        self.assertEqual(len(calls), 2)
        self.assertEqual(elapsed, 3.0)


if __name__ == '__main__':
    unittest.main()
