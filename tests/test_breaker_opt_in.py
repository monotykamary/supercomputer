"""The default policy buys starts instead of blocking work; per-worker safety remains."""
import copy
import json
import pathlib
import tempfile
from unittest.mock import Mock, patch
from test_wiring import S, sched, batch, vm


def disabled():
    s = sched()
    s.pol.pop('start_breaker_enabled', None)
    return s


def test_default_is_off_and_requires_a_boolean_opt_in():
    assert S.policy_defaults()['start_breaker_enabled'] is False
    s = disabled()
    assert not s._breaker_enabled()
    for value in (False, 'true', 1, None):
        s.pol['start_breaker_enabled'] = value
        assert not s._breaker_enabled()
    s.pol['start_breaker_enabled'] = True
    assert s._breaker_enabled()


def test_existing_policy_without_flag_is_off_and_policy_can_opt_in():
    with tempfile.TemporaryDirectory() as d, patch.object(S, 'POLICY', pathlib.Path(d) / 'policy.json'):
        S.POLICY.write_text(json.dumps({'generation': 7}))
        assert S.read_policy()['start_breaker_enabled'] is False
        assert S.write_policy({'start_breaker_enabled': 'true'})['start_breaker_enabled'] is True
        assert S.write_policy({'start_breaker_enabled': 'false'})['start_breaker_enabled'] is False


def test_no_readiness_or_front_trip_by_default():
    s = disabled()
    s._metrics_save = Mock()
    s.alert = Mock()
    now = S.time.time()
    s.starts_log = [{'t': now - 10, 'vm': str(i), 'ok': False} for i in range(8)]
    before = copy.deepcopy(s.br)
    s._breaker_eval(now)
    s.trip("boat's front: the agents of 2 of 2 VMs don't answer")
    assert s.br == before and s.m['breaker_trips'] == 0
    s.alert.assert_not_called()
    s._metrics_save.assert_not_called()


def test_stored_open_and_half_open_state_cannot_limit_default_planner():
    s = disabled()
    batch(s, 'g1', image='img1', klass=2, n=40)
    batch(s, 'g2', image='img2', klass=2, n=40)
    now = S.time.time()
    s.br.update(open_until=now + 3600, half=True, t_trip=now)
    s.starts_log.append({'t': now + 1, 'vm': 'P', 'ok': None, 'image': 'img1'})
    plan = s._plan(now)
    assert sum(n for n, *_ in plan.values()) > 1, plan
    s.pol['start_breaker_enabled'] = True
    assert s._plan(now) == {}


def test_disabled_policy_rejudges_even_an_actual_outage_without_dropping_workers():
    s = disabled()
    s._metrics_save = Mock()
    for i in range(2):
        vm(s, f'bad-{i}').failing = True
    assert s._outage()  # still protects ownership / per-worker failure classification
    s.br.update(open_until=S.time.time() + 3600, half=True, reason="boat's front: the agents of 2 of 2 VMs don't answer", trips=4)
    before_workers = dict(s.vms)
    with tempfile.TemporaryDirectory() as d, patch.object(S, 'DIR', pathlib.Path(d)):
        r = s.rejudge_breaker(reason='disabled start breaker')
        assert r['decision'] == 'closed' and r['evidence']['enabled'] is False
        assert s.vms == before_workers and s.br['trips'] == 4


def test_failed_resume_quarantine_and_fruitless_counting_still_work_when_off():
    import test_breaker_policy as tests
    original = tests.sched
    def off():
        s = original()
        s.pol['start_breaker_enabled'] = False
        return s
    with patch.object(tests, 'sched', side_effect=off):
        tests.test_failed_known_resume_is_quarantined_charged_and_not_a_global_trip()


def test_opt_in_retains_sampled_trip_and_escalation():
    s = sched()
    s._metrics_save = Mock()
    s.alert = Mock()
    now = S.time.time()
    s.starts_log = [{'t': now - 10, 'vm': str(i), 'ok': False} for i in range(4)]
    s._breaker_eval(now)
    assert s.br['trips'] == 1 and s.br['backoff'] == 600
    assert abs(s.br['open_until'] - (now + 300)) < 1


if __name__ == '__main__':
    for name, function in list(globals().items()):
        if name.startswith('test_'):
            function()
            print('ok', name)
