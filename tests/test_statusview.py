"""Published status is immutable, honestly aged and independent of contended locks."""
import collections
import json
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import health, scheduler, statusview


class StatusView(unittest.TestCase):
    def view(self):
        v = statusview.PublishedStatus({'pid': 1, 'code': 'candidate'})
        v.publish({'pid': 1, 'code': 'candidate', 'values': [1]}, time.monotonic_ns() // 1000000, time.time())
        return v

    def scheduler(self):
        s = object.__new__(scheduler.Scheduler)
        s.code = 'candidate'
        s.lock = threading.RLock()
        s._status_view = self.view()
        s._status_timings = collections.deque(maxlen=20000)
        s._status_refresh_lock = threading.Lock()
        s._status_read = Mock(return_value={'code': 'candidate', 'values': [2]})
        s.health = health.Health()
        return s

    def test_boot_response_is_explicitly_unready(self):
        r = json.loads(statusview.PublishedStatus({'pid': 1}).response())
        self.assertEqual(r['pid'], 1)
        self.assertFalse(r['status_snapshot']['ready'])
        self.assertIsNone(r['status_snapshot']['age_s'])

    def test_publication_freezes_nested_data_and_reserved_metadata(self):
        v = self.view()
        data = {'nested': {'rows': [1]}, 'status_snapshot': {'ready': False}}
        v.publish(data, time.monotonic_ns() // 1000000, time.time())
        data['nested']['rows'].append(2)
        r = json.loads(v.response())
        self.assertEqual(r['nested']['rows'], [1])
        self.assertTrue(r['status_snapshot']['ready'])

    def test_age_is_monotonic_and_failed_refresh_does_not_reset_it(self):
        v = statusview.PublishedStatus({})
        with patch.object(statusview.time, 'time', return_value=500):
            v.publish({'value': 'old'}, 1000, 100)
        v.failed('blocked')
        with patch.object(statusview.time, 'monotonic_ns', return_value=4500000000):
            r = json.loads(v.response())
        self.assertEqual(r['status_snapshot']['age_s'], 3.5)
        self.assertEqual(r['status_snapshot']['captured_at'], 100)
        self.assertEqual(r['status_snapshot']['published_at'], 500)
        self.assertEqual(r['status_snapshot']['refresh_error'], 'blocked')
        with patch.object(statusview.time, 'monotonic_ns', return_value=500000000):
            self.assertEqual(json.loads(v.response())['status_snapshot']['age_s'], 0)

    def test_out_of_order_and_unencodable_refresh_keep_previous_view(self):
        v = statusview.PublishedStatus({})
        self.assertTrue(v.publish({'value': 2}, 2000, 2))
        self.assertFalse(v.publish({'value': 1}, 1000, 1))
        with self.assertRaises(TypeError):
            v.publish({'value': object()}, 3000, 3)
        r = json.loads(v.response())
        self.assertEqual(r['value'], 2)
        self.assertEqual(r['status_snapshot']['generation'], 1)

    def test_reader_never_touches_live_state_io_or_health_locks(self):
        s = self.scheduler()
        s._status_read.side_effect = AssertionError('live status traversal')
        s._h = Mock(side_effect=AssertionError('health lock'))
        s.lock = Mock()
        s.lock.acquire.side_effect = AssertionError('scheduler lock')
        with patch.object(scheduler, 'code_hash', side_effect=AssertionError('filesystem')), \
             patch.object(scheduler.blobcache, 'last_report', side_effect=AssertionError('disk')):
            r = s.status()
        self.assertEqual(r['values'], [1])
        self.assertTrue(r['status_snapshot']['ready'])
        s.lock.acquire.assert_not_called()

    def test_busy_scheduler_lock_does_not_delay_readers_or_reset_age(self):
        s = self.scheduler()
        entered, release = threading.Event(), threading.Event()
        def holder():
            with s.lock:
                entered.set()
                release.wait(4)
        t = threading.Thread(target=holder)
        t.start()
        self.assertTrue(entered.wait(1))
        try:
            with patch.object(scheduler, 'code_hash', return_value='candidate'), \
                 patch.object(scheduler.blobcache, 'last_report', return_value={}):
                self.assertFalse(s._refresh_status())
            started = time.monotonic()
            responses = [s.status() for _ in range(100)]
            self.assertLess(time.monotonic() - started, .5)
            self.assertEqual({r['status_snapshot']['generation'] for r in responses}, {1})
            s._status_read.assert_not_called()
        finally:
            release.set()
            t.join(2)
        self.assertFalse(t.is_alive())

    def test_blocked_publisher_filesystem_does_not_delay_response(self):
        s = self.scheduler()
        entered, release = threading.Event(), threading.Event()
        def slow_hash():
            entered.set()
            self.assertTrue(release.wait(4))
            return 'candidate'
        with patch.object(scheduler, 'code_hash', side_effect=slow_hash), \
             patch.object(scheduler.blobcache, 'last_report', return_value={}):
            t = threading.Thread(target=s._refresh_status)
            t.start()
            self.assertTrue(entered.wait(1))
            try:
                started = time.monotonic()
                self.assertEqual(s.status()['values'], [1])
                self.assertLess(time.monotonic() - started, .5)
            finally:
                release.set()
                t.join(2)
        self.assertFalse(t.is_alive())
        self.assertEqual(s.status()['values'], [2])

    def test_failed_refresh_preserves_generation_and_exposes_error(self):
        s = self.scheduler()
        with patch.object(scheduler, 'code_hash', side_effect=OSError('unreadable')):
            self.assertFalse(s._refresh_status())
        r = s.status()
        self.assertEqual(r['values'], [1])
        self.assertEqual(r['status_snapshot']['generation'], 1)
        self.assertEqual(r['status_snapshot']['refresh_error'], 'unreadable')

    def test_health_timings_are_drained_off_the_response_path(self):
        s = self.scheduler()
        for _ in range(3):
            s.status_response()
        self.assertEqual(s.health.ev['status'], [])
        s._flush_status_timings()
        self.assertEqual(len(s.health.ev['status']), 3)
        self.assertLess(max(r['s'] for r in s.health.ev['status']), 1)
        self.assertEqual(len(s._status_timings), 0)

    def test_simulator_clock_drives_age_without_wall_clock_leakage(self):
        from sim.kernel import Kernel, shims
        kernel = Kernel(100, 1)
        clock = shims(kernel)['time']
        self.assertEqual(clock.monotonic_ns(), 100000000000)
        with patch.object(statusview, 'time', clock):
            v = statusview.PublishedStatus({})
            v.publish({'value': 1}, clock.monotonic_ns() // 1000000, clock.time())
            kernel.now += 3.5
            self.assertEqual(json.loads(v.response())['status_snapshot']['age_s'], 3.5)

    def test_complete_snapshots_do_not_mix_during_publication(self):
        v = self.view()
        errors = []
        done = threading.Event()
        def read():
            while not done.is_set():
                r = json.loads(v.response())
                if 'n' in r and r['values'] != [r['n']] * 10:
                    errors.append(r)
        t = threading.Thread(target=read)
        t.start()
        try:
            for n in range(100):
                v.publish({'n': n, 'values': [n] * 10}, time.monotonic_ns() // 1000000, time.time())
        finally:
            done.set()
            t.join(2)
        self.assertFalse(t.is_alive())
        self.assertEqual(errors, [])


if __name__ == '__main__':
    unittest.main()
