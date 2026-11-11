"""Resource warnings are observed by the controller without restarting it."""
import unittest
from unittest.mock import Mock, patch
from test_wiring import S, sched


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
        s._status = Mock(return_value={})
        with patch.object(S.blobcache, 'last_report', return_value={}):
            response = s.status()
        self.assertEqual(response['health']['memory'], target)
        s.health.check.assert_not_called()
        s.health.memory_status.assert_called_once()


if __name__ == '__main__': unittest.main()
