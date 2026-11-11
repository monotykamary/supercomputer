"""Memory readout and alert regressions; runnable without pytest."""
import pathlib
import sys
import tempfile
from unittest.mock import patch
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import health, canary, memorypressure as mp


def fixture(root, high=87449, psi='some avg10=0.00 avg60=0.00 avg300=0.00 total=0\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=0\n'):
    (root / 'proc/self').mkdir(parents=True, exist_ok=True)
    (root / 'proc/self/cgroup').write_text('0::/service\n')
    cg = root / 'cg/service'
    cg.mkdir(parents=True, exist_ok=True)
    for name, value in {'cgroup.controllers':'memory cpu', 'service/memory.events':f'low 0\nhigh {high}\nmax 0\n', 'service/memory.pressure':psi, 'service/memory.current':'1234', 'service/memory.high':'4294967296', 'service/memory.max':'5368709120', 'service/memory.swap.current':'0', 'service/memory.swap.max':'max'}.items():
        (root / 'cg' / name).write_text(value)
    return cg


def test_read_baseline_growth_reset_and_limits():
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp); cg = fixture(root)
        s = mp.Sampler(proc_root=root / 'proc', cgroup_root=root / 'cg')
        a = s.sample()
        assert a['known'] and a['high_events'] == 87449 and a['high_delta'] == 0
        assert a['current'] == 1234 and a['high'] == 4294967296 and a['max'] == 5368709120
        assert a['swap_current'] == 0 and a['swap_max'] == 'max'
        (cg / 'memory.events').write_text('high 87450\n')
        assert s.sample()['high_delta'] == 1
        (cg / 'memory.events').write_text('high 2\n')
        assert s.sample()['high_delta'] == 0
        (cg / 'memory.events').write_text('high 3\n')
        assert s.sample()['high_delta'] == 1


def test_unknown_partial_malformed_and_nonlinux():
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp); cg = fixture(root)
        s = mp.Sampler(proc_root=root / 'proc', cgroup_root=root / 'cg')
        (cg / 'memory.events').write_text('high nope\nhigh -1\n')
        (cg / 'memory.pressure').write_text('some avg10=nan\nfull avg10=101\n')
        a = s.sample()
        assert not a['known'] and a['high_events'] is None and a['some_avg10'] is None and a['full_avg10'] is None
        (cg / 'memory.events').unlink()
        assert not s.sample()['known']
        (root / 'proc/self/cgroup').write_text('0::/../../escape\n')
        assert not s.sample()['known']
        with patch.object(mp.sys, 'platform', 'darwin'):
            assert not s.sample()['known']


def sample(delta=0, some=0, full=0):
    return dict(known=True, high_delta=delta, some_avg10=some, full_avg10=full, current=1, high=4, max=5)


def test_first_alert_dedupe_recovery_short_window_and_other_targets():
    with patch.object(health, 'env_flags', return_value=[]):
        h = health.Health()
        h.record_memory(1000, sample(delta=1))
        assert len(h.memory_tick(1000)) == 1
        assert h.memory_tick(1001) == [] and h.tick(1002) == []
        assert not h.check(1002)['targets']['memory']['ok']
        h.record_memory(1005, sample())
        assert not h.check(1005)['targets']['memory']['ok']  # retain a short pressure episode
        assert h.check(1091)['targets']['memory']['ok']
        h.memory_tick(1091)
        h.record_memory(1092, sample(full=10))
        assert len(h.memory_tick(1092)) == 1
        h.record_status(1092, 2)
        assert not any('status breached' in a for a in h.tick(1092))
        assert any('status breached' in a for a in h.tick(1093))
        assert 'memory' in health.show(h.check(1093))
        for t in range(2000, 3000):
            h.record_memory(t, sample())
        assert len(h.ev['memory']) <= 64
        assert all(e['t'] >= 2999 - 90 for e in h.ev['memory'])


def test_thresholds_unknown_and_no_canary_rollback():
    with tempfile.TemporaryDirectory() as tmp, patch.object(health, 'env_flags', return_value=[]):
        root = pathlib.Path(tmp); (root / 'code/old/supercomputer').mkdir(parents=True)
        h = health.Health(); c = canary.Canary(root)
        h.record_memory(999, sample()); c.handing_off('old', 'new', 1000)
        for t in (1001, 1060, 1120):
            h.record_memory(t, sample(some=20))
            assert not h.check(t)['targets']['memory']['ok']
            assert c.tick('new', t, h)[0] == 'watch'
        assert 'memory' not in canary.JUDGED
        h = health.Health(); h.record_memory(1000, dict(known=False, high_delta=None, some_avg10=None, full_avg10=None))
        r = h.check(1000)['targets']['memory']
        assert r['ok'] and r['value'] is None
        h.record_memory(1001, sample(some=19.99, full=9.99))
        assert h.check(1001)['targets']['memory']['ok']


if __name__ == '__main__':
    for name, f in list(globals().items()):
        if name.startswith('test_'):
            f(); print('ok', name)
