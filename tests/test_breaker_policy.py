"""The 07:14 lone failed-resume incident, through the shipped rules and controller."""
import json
import pathlib
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_cpu_model import MODEL, constrained
from test_wiring import S, boat, sched, vm


def test_lone_failure_never_escalates_even_a_half_open_hour_backoff():
    for half in (False, True):
        for minimum in (1, 3, 5):
            for judged in (0, 1, 2):
                rule = S.rules.Breaker(300, 3600, minimum)
                rule.half, rule.open_until, rule.backoff, rule.trips = half, 10, 3600, 4
                assert rule.judge(11, judged, judged, False) == 'kept'
                assert (rule.half, rule.open_until, rule.backoff, rule.trips) == (half, 10, 3600, 4)


def test_real_multi_start_failure_still_trips_and_success_closes():
    for half in (False, True):
        rule = S.rules.Breaker(300, 3600, 3)
        rule.half = half
        assert rule.judge(10, 3, 1, False) == 'kept'
        assert rule.judge(10, 4, 2, False) == 'kept'
        assert rule.judge(10, 3, 2, False) == 'tripped'
        assert rule.trips == 1 and rule.open_until == 310 and rule.backoff == 600
        assert rule.judge(311, 1, 0, True) == 'closed'
        assert not rule.half and rule.backoff == 300


def test_failed_known_resume_is_quarantined_charged_and_not_a_global_trip():
    s = sched()
    s._metrics_save = Mock()
    b = constrained(s)
    worker = vm(s, 'failed-known', ready=False)
    worker.how, worker.cpu_constraint, worker.cpu_known_resume = 'resumed', MODEL, True
    s.gate = SimpleNamespace(take_split=lambda *a: (1, 0))
    s._let_go = Mock()
    constraint = s._constraint(b.image, b.vm_type, MODEL)
    constraint['fruitless'] = 25
    now = S.time.time()
    s.br.update(half=True, open_until=now - 1, t_trip=now - 2, backoff=3600, trips=4)
    with patch.object(boat, 'boat', side_effect=RuntimeError('specific stopped VM failed')):
        s._up(worker, 2, b)
    before = dict(s.br)
    s._breaker_eval(S.time.time())
    assert s.br == before
    assert worker.dead and not worker.ready and worker.quarantine
    assert worker.id in s.resume_quarantine and 'specific stopped VM failed' in s.resume_quarantine[worker.id]['reason']
    assert len(s.starts_log) == 1 and s.starts_log[0]['ok'] is False
    assert constraint['fruitless'] == 26 and constraint['draws'][0]['counted']
    s._metrics_save.assert_called()
    # Registry reconciliation or an old observed CPU must never offer it again.
    registry = {'vms': {worker.id: {'snapshot': b.image, 'type': b.vm_type, 'state': 'stopped', 'lease': None}}}
    s.vms.clear()
    constraint['observed'][worker.id] = MODEL
    assert not s._known_cpu_reuse(registry, b.image, b.vm_type, MODEL)
    with patch.object(S, 'Registry') as registry_type, patch.object(boat, 'limits', return_value={}), \
            patch.object(boat, 'starts_left', return_value=1), patch.object(boat, 'budget', return_value=1), \
            patch.object(boat, 'sandboxes', return_value={}), patch.object(boat, 'reconcile'), \
            patch.object(s, '_bring') as bring:
        registry_type.return_value.locked.return_value.__enter__.return_value = registry
        assert s._acquire(b.image, b.vm_type, 1, 2, b, True, MODEL) == 0
        bring.assert_not_called()


def test_resume_quarantine_survives_disk_reload_without_resetting_breaker():
    s = sched()
    s.wave = {'on': False, 't': 0.0, 't_end': 0.0}
    s.resume_quarantine = {'failed-known': {'reason': 'resume failed', 't': 10, 'image': 'img', 'type': 'large'}}
    s.br.update(half=True, open_until=9999999999, backoff=3600, trips=4)
    with tempfile.TemporaryDirectory() as temp, patch.object(S, 'DIR', pathlib.Path(temp)):
        (S.DIR / 'metrics').mkdir()
        s._metrics_save()
        state = json.loads((S.DIR / 'metrics/state.json').read_text())
        assert state['resume_quarantine'] == s.resume_quarantine
        replacement = sched()
        replacement.wave = {}
        replacement._metrics_load()
        assert replacement.resume_quarantine == s.resume_quarantine
        assert replacement.br == s.br
        # Even a now-running stale registry entry cannot trigger automatic adoption.
        b = constrained(replacement)
        replacement.queued = lambda: list(b.items.values())
        with patch.object(boat, "sandboxes", return_value={"failed-known": {"state": "running", "name": b.image + "~l"}}), \
                patch.object(boat, "agent_health") as health:
            replacement._adopt()
            health.assert_not_called()


if __name__ == '__main__':
    for name, function in list(globals().items()):
        if name.startswith('test_'):
            function()
            print('ok', name)
