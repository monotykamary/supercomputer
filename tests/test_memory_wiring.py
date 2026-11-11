"""Resource warnings are observed by the controller without restarting it."""
import unittest
import collections
import threading
from unittest.mock import Mock, patch
from test_wiring import S, sched
from supercomputer import statusview


class MemoryWiring(unittest.TestCase):
    def test_watch_records_pressure_and_delivers_alert_even_without_load_watchdog(self):
        s = sched()
        s.health = Mock()
        s.alert = Mock()
        sample = {'available':True, 'high':7, 'current':4000, 'full_avg10':20.0}
        s._memory_sampler = Mock()
        s._memory_sampler.sample.return_value = sample
        s.health.memory_tick.return_value = ['health: memory pressure']
        with patch.object(S, 'WATCHDOG', False):
            s._watch()
        s._memory_sampler.sample.assert_called_once_with()
        s.health.record_memory.assert_called_once()
        self.assertEqual(s.health.record_memory.call_args.args[1], sample)
        s.alert.assert_called_once_with('health: memory pressure')
        self.assertEqual(s.load['memory'], sample)
        self.assertFalse(s.shed, 'memory alert alone must not mutate placement or lease policy')

    def test_status_exposes_current_memory_target_without_full_health_scan(self):
        s = sched()
        s.health = Mock()
        s.health.last = {}
        target = {'ok':False,'value':1,'detail':{'high_delta':2}}
        s.health.memory_status.return_value = target
        s.health.client_errors.return_value = {"count": 0, "by_status": {}, "recent": []}
        s._status_read = Mock(return_value={})
        s._status_view = statusview.PublishedStatus({})
        s._status_timings = collections.deque(maxlen=20000)
        s._status_refresh_lock = threading.Lock()
        with patch.object(S.blobcache, 'last_report', return_value={}), \
             patch.object(S, 'code_hash', return_value='candidate'):
            self.assertTrue(s._refresh_status(blocking=True))
        s.health.memory_status.assert_called_once()
        s.health.reset_mock()
        response = s.status()
        self.assertEqual(response['health']['memory'], target)
        s.health.check.assert_not_called()
        s.health.memory_status.assert_not_called()


if __name__ == '__main__': unittest.main()
