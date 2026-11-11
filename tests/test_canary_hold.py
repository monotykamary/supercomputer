"""Pending native canaries retain voluntary capacity, never suppress safety/work actions."""
import os
import pathlib
import sys
import tempfile
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock, patch

ROOT = pathlib.Path(os.environ.get('SC_TEST_CODE_ROOT', pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT / 'tests'))
from test_wiring import sched, vm, batch
from supercomputer import canary, health, scheduler as S, rules


def fixture(state='watching', target='candidate'):
    s = sched()
    s.code = 'candidate'
    s.canary = SimpleNamespace(read=lambda: {'state': state, 'to': target, 't': time.time() - 3600})
    s.floor = lambda image: 1
    s.retired = lambda: set()
    s.treadmill_alive = lambda image: False
    s._let_go = Mock()
    a, b = vm(s, 'floor'), vm(s, 'above')
    a.t_real, b.t_real = time.time() - 30, time.time() - 60
    a.idle_since = b.idle_since = time.time() - 10
    return s, a, b


def tick(s):
    def immediate(target, args=(), kwargs=None, **ignored):
        return SimpleNamespace(start=lambda: target(*args, **(kwargs or {})))
    with patch.object(S.threading, 'Thread', side_effect=immediate):
        s._release_idle()


def test_pending_candidate_holds_idle_even_after_wall_clock_deadline():
    s, a, b = fixture()
    tick(s)
    assert set(s.vms) == {a.id, b.id}
    s._let_go.assert_not_called()


def test_pending_candidate_does_not_expire_filler():
    s, a, b = fixture()
    low = batch(s, 'filler', klass=5).items['i0']
    low.state = 'running'
    b.running[(low.batch.id, low.run_name)] = low
    b.t_real = time.time() - S.HOLD - 60
    s._kill = Mock()
    tick(s)
    assert not b.draining and low.state == 'running'
    s._kill.assert_not_called()
    s._let_go.assert_not_called()


def test_pending_candidate_holds_a_previously_draining_vm():
    s, a, b = fixture()
    b.draining = True
    tick(s)
    assert b.id in s.vms
    s._let_go.assert_not_called()


def test_pending_candidate_still_tracks_real_work_and_generation():
    s, a, b = fixture()
    real = batch(s, 'real', klass=2).items['i0']
    real.state = 'running'
    b.running[(real.batch.id, real.run_name)] = real
    before = time.time()
    tick(s)
    assert b.t_real >= before and b.gen == s.pol['generation']
    s._let_go.assert_not_called()


def test_pending_candidate_does_not_block_a_genuinely_lost_vm():
    s, a, b = fixture()
    s._lost_vm(b)
    assert b.dead
    s._let_go.assert_called_once_with(b, stop=True)


def test_pass_reject_or_another_candidates_watch_restores_normal_release():
    for state, target in [('passed', 'candidate'), ('rejected', 'candidate'), ('rolled back', 'candidate'), ('watching', 'other')]:
        s, a, b = fixture(state, target)
        tick(s)
        assert b.id not in s.vms, (state, target)
        s._let_go.assert_called_once_with(b)


def test_native_pass_unblocks_the_same_idle_vm():
    s, a, b = fixture()
    tick(s)
    s.canary.read = lambda: {'state': 'passed', 'to': 'candidate'}
    tick(s)
    s._let_go.assert_called_once_with(b)


def test_pending_canary_does_not_block_explicit_retirement_or_disk_replacement():
    for safety in ('retired', 'disk'):
        s, a, b = fixture()
        if safety == 'retired':
            s.retired = lambda: {b.id}
        else:
            b.disk_low, b.t_low = True, time.time() - S.DISK_GRACE_S - 60
        tick(s)
        assert b.id not in s.vms
        assert bool(getattr(b, 'replace', False)) == (safety == 'disk')
        s._let_go.assert_called_once_with(b)


def test_pending_canary_does_not_block_real_work_preemption():
    s, a, b = fixture()
    s.vms.pop(a.id)
    low = batch(s, 'filler', klass=5).items['i0']
    low.state = 'running'
    b.running[(low.batch.id, low.run_name)] = low
    gate = batch(s, 'gate', klass=2).items['i0']
    gate.timing = True
    s._kill = Mock()
    def immediate(target, args=(), kwargs=None, **ignored):
        return SimpleNamespace(start=lambda: target(*args, **(kwargs or {})))
    with patch.object(S.threading, 'Thread', side_effect=immediate):
        assert s._preempt_for(gate, time.time())
    s._kill.assert_called_once_with(b, low)


def test_inflight_release_is_not_capacity_and_always_clears_its_marker():
    for fail in (False, True):
        s, a, b = fixture('passed')
        queued = []
        def delayed(target, args=(), **kw):
            return SimpleNamespace(start=lambda: queued.append((target, args)))
        with patch.object(S.threading, 'Thread', side_effect=delayed):
            s._release_idle()
        assert b.id not in s.vms and b.id in s._releasing
        assert s._in_flight() == 0, 'an outgoing VM must not block real-work capacity'
        if fail:
            s._let_go.side_effect = RuntimeError('transport failed')
        try:
            target, args = queued[0]
            target(*args)
        except RuntimeError:
            assert fail
        assert b.id not in s._releasing


def test_adoption_skips_inflight_release_with_an_old_local_lease():
    from contextlib import nullcontext
    s, a, b = fixture('passed')
    s.vms.pop(b.id)
    s._releasing = {b.id}
    s.held_before[b.id] = {'t_real': b.t_real}
    s._bring = Mock()
    s._h = lambda: SimpleNamespace(record_orphans=Mock())
    record = {'snapshot': 'img', 'type': b.type, 'state': 'running',
              'lease': {'pid': os.getpid(), 'host': S.socket.gethostname(), 'batch': 'scheduler', 'since': time.time() - 600}}
    with patch.object(S.boat, 'sandboxes', return_value={b.id: {'name': 'img', 'state': 'running'}}), \
         patch.object(S, 'Registry', return_value=SimpleNamespace(locked=lambda: nullcontext({'vms': {b.id: record}}))):
        s._adopt()
    s._bring.assert_not_called()


def test_permission_rule_truth_table():
    for watching in (False, True):
        for candidate in (False, True):
            assert rules.voluntary_retirement_allowed(watching, candidate) is not (watching and candidate)


@contextmanager
def status_canary(previous=None, rollback=True):
    with tempfile.TemporaryDirectory() as tmp:
        root = pathlib.Path(tmp)
        if rollback:
            (root / 'code' / 'old' / 'supercomputer').mkdir(parents=True)
        c, h = canary.Canary(root), health.Health()
        if previous is not None:
            h.record_status(900.0, previous)
        c.handing_off('old', 'candidate', 1000.0)
        yield c, h


def test_bad_baseline_slow_current_status_rejects_after_two_checks():
    with status_canary(previous=3.032) as (c, h):
        h.record_status(1010.0, 3.032)
        assert c.tick('candidate', 1060.0, h)[0] == 'watch'
        assert 'status' in c.baseline and c.streak['status'] == 1
        verdict, why = c.tick('candidate', 1120.0, h)
        assert verdict == 'handback' and 'status 3.032 (want < 1.0)' in why
        assert c.rejected('candidate')


def test_bad_baseline_fast_current_status_passes_only_after_watch():
    with status_canary(previous=3.032) as (c, h):
        h.record_status(1010.0, 0.032)
        for now in (1060.0, 1120.0, 1599.0):
            assert c.tick('candidate', now, h)[0] == 'watch'
            assert 'status' in c.baseline and c.streak['status'] == 0
        assert c.tick('candidate', 1600.0, h)[0] == 'passed'


def test_no_history_slow_current_status_rejects():
    with status_canary() as (c, h):
        h.record_status(1010.0, 3.032)
        assert c.tick('candidate', 1060.0, h)[0] == 'watch'
        assert 'status' in c.baseline
        assert c.tick('candidate', 1120.0, h)[0] == 'handback'


def test_unknown_status_never_creates_synthetic_observations():
    for previous in (None, 3.032):
        with status_canary(previous=previous) as (c, h):
            before = list(h.ev['status'])
            for now in (1060.0, 1120.0):
                assert c.tick('candidate', now, h)[0] == 'watch'
                assert 'status' in c.baseline and c.streak['status'] == 0
            assert c.tick('candidate', 1600.0, h)[0] == 'passed'
            assert h.ev['status'] == before


def test_first_status_breach_at_watch_end_cannot_pass():
    with status_canary(previous=3.032) as (c, h):
        h.record_status(1590.0, 3.032)
        assert c.tick('candidate', 1600.0, h)[0] == 'watch'
        assert c.read()['state'] == 'watching' and c.streak['status'] == 1
        assert c.tick('candidate', 1660.0, h)[0] == 'handback'


def test_slow_status_cannot_pass_without_a_rollback_snapshot():
    with status_canary(rollback=False) as (c, h):
        h.record_status(1010.0, 3.032)
        for now in (1060.0, 1120.0, 1600.0):
            assert c.tick('candidate', now, h)[0] == 'watch'
        assert c.read()['state'] == 'watching'


def test_nonstatus_bad_or_unknown_baselines_still_do_not_gate():
    with status_canary(previous=3.032) as (c, h):
        h.record_error(900.0, 'predecessor failed')
        h.record_error(1010.0, 'current failed')
        h.record_status(1010.0, 0.032)
        for now in (1060.0, 1120.0):
            assert c.tick('candidate', now, h)[0] == 'watch'
            assert 'errors' not in c.baseline and 'waste' not in c.baseline
            assert c.streak['errors'] == 0
        assert c.tick('candidate', 1600.0, h)[0] == 'passed'


def test_status_threshold_is_still_strictly_under_one_second():
    for seconds, verdict in ((0.999, 'watch'), (1.0, 'handback'), (3.032, 'handback')):
        with status_canary(previous=3.032) as (c, h):
            h.record_status(1010.0, seconds)
            assert c.tick('candidate', 1060.0, h)[0] == 'watch'
            assert c.tick('candidate', 1120.0, h)[0] == verdict


def test_status_streak_uses_measurements_and_resets_on_health_or_unknown():
    for previous in (0, 1, 2):
        for measured in (False, True):
            for healthy in (False, True):
                assert rules.canary_status_streak(previous, measured, healthy) == (
                    previous + 1 if measured and not healthy else 0)


if __name__ == '__main__':
    for name, fn in sorted(list(globals().items())):
        if name.startswith('test_'):
            fn()
            print('ok', name)
