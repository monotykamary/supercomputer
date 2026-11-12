"""Inherited 08:17 single-sample trip: shared audited startup/operator policy."""
import copy
import json
import pathlib
import tempfile
from contextlib import contextmanager
from unittest.mock import Mock, patch

from test_wiring import S, sched, vm

NOW = 1000.0

@contextmanager
def fixture(n=1):
    s = sched()
    s._metrics_save = Mock()
    s.br.update(open_until=4500, half=True, backoff=3600, t_trip=900,
                reason=f'{n} of the last {n} starts not ready within 8 min', trips=31,
                history=[{'t': 900, 'reason': 'inherited'}])
    s.starts_log = [{'t': 800 + i, 'vm': f'failed-{i}', 'ok': False} for i in range(n)]
    with tempfile.TemporaryDirectory() as d, patch.object(S, 'DIR', pathlib.Path(d)), patch.object(S.time, 'time', return_value=NOW):
        yield s, pathlib.Path(d)

def test_single_inherited_trip_closes_and_audits_original_record():
    with fixture() as (s, d):
        before = copy.deepcopy(s.br)
        r = s.rejudge_breaker(source='startup', actor='controller', reason='startup/handoff')
        assert r['decision'] == 'closed' and not r['breaker']['open']
        assert not s.br['half'] and s.br['open_until'] == 0 and s.br['backoff'] == S.BREAK_BACKOFF
        assert s.br['trips'] == 31 and s.br['history'] == before['history']
        receipt = json.loads((d / 'breaker-rejudge.jsonl').read_text())
        assert receipt['inherited'] == before and receipt['source'] == 'startup'
        s._metrics_save.assert_called_once()
        assert s.wake.is_set()

def test_real_sample_and_expired_sample_records_remain_open():
    for keep_journal in (True, False):
        with fixture(4) as (s, _):
            if not keep_journal:
                s.starts_log = []
            before = copy.deepcopy(s.br)
            r = s.rejudge_breaker(reason='check current rules')
            assert r['decision'] == 'kept' and s.br == before

def test_current_multi_start_outage_overrides_invalid_old_trip():
    with fixture() as (s, _):
        s.starts_log += [{'t': 920 + i, 'vm': f'new-{i}', 'ok': False} for i in range(4)]
        assert s.rejudge_breaker(reason='check')['decision'] == 'kept'

def test_quarantined_starts_do_not_count_for_rejudge_or_future_judgments():
    with fixture(4) as (s, _):
        s.resume_quarantine = {f'failed-{i}': {'reason': 'known resume failed'} for i in range(4)}
        assert s.rejudge_breaker(reason='exclude known local failures')['decision'] == 'closed'
        s.br['t_trip'] = 0
        before = copy.deepcopy(s.br)
        s._breaker_eval(NOW)
        assert s.br == before

def test_live_and_recorded_front_outages_are_never_forced_closed():
    with fixture() as (s, _):
        for i in range(2):
            vm(s, f'healthy-before-{i}').failing = True
        assert s.rejudge_breaker(reason='real front outage')['decision'] == 'kept'
    with fixture() as (s, _):
        s.br['reason'] = "boat's front: the agents of 2 of 2 eligible VMs don't answer (502)"
        assert s.rejudge_breaker(reason='preserve recorded front outage')['decision'] == 'kept'

def test_unknown_evidence_is_fail_closed():
    with fixture() as (s, _):
        s.br['reason'] = 'unrecognized operator pause'
        assert s.rejudge_breaker(reason='do not guess')['decision'] == 'kept'

def test_rejudging_is_idempotent_and_every_operator_request_is_audited():
    with fixture() as (s, d):
        first = s.rejudge_breaker(actor='alice', reason='legacy minimum fixed')
        after = copy.deepcopy(s.br)
        second = s.rejudge_breaker(actor='alice', reason='repeat check')
        assert first['decision'] == 'closed' and second['decision'] == 'kept'
        assert s.br == after
        records = [json.loads(x) for x in (d / 'breaker-rejudge.jsonl').read_text().splitlines()]
        assert len(records) == 2 and all(r['actor'] == 'alice' for r in records)

def test_no_state_change_when_audit_cannot_be_written_or_ownership_unstable():
    with fixture() as (s, _):
        before = copy.deepcopy(s.br)
        with patch.object(pathlib.Path, 'open', side_effect=OSError('disk full')):
            try:
                s.rejudge_breaker(reason='check')
                raise AssertionError('audit failure was ignored')
            except OSError:
                pass
        assert s.br == before
        s.handing_off = True
        try:
            s.rejudge_breaker(reason='check')
            raise AssertionError('ownership race allowed')
        except ValueError:
            pass
        assert s.br == before

def test_rule_minimum_and_idempotence():
    for n in (1, 2, 3, 4):
        b = S.rules.Breaker(300, 3600, 4)
        b.open_until, b.half, b.backoff, b.trips = 4500, True, 3600, 31
        result = b.rejudge(NOW, n, n, False, True)
        assert (result == 'closed') == (n < 4)
        after = vars(b).copy()
        assert b.rejudge(NOW, n, n, False, True) == 'kept'
        assert vars(b) == after

def test_operator_command_capability_guard_and_exact_request():
    from supercomputer import controller
    client = Mock()
    client.req.side_effect = [{'breaker_rejudge_protocol': 1}, {'ok': True, 'decision': 'closed'}]
    with patch.object(controller, 'info', return_value={'vm': 'controller', 'url': 'test'}), patch.object(S, 'Client', return_value=client):
        controller.main(['breaker', 'rejudge', '--reason', 'legacy single sample'])
    assert client.req.call_args.args[0] == '/breaker/rejudge'
    assert client.req.call_args.args[1]['reason'] == 'legacy single sample'
    assert client.req.call_args.args[1]['protocol'] == 1
    client.reset_mock()
    client.req.side_effect = [{}]
    with patch.object(controller, 'info', return_value={'vm': 'controller'}), patch.object(S, 'Client', return_value=client):
        try:
            controller.main(['breaker', 'rejudge', '--reason', 'check'])
            raise AssertionError('old controller was called')
        except SystemExit:
            pass
    assert client.req.call_count == 1

def test_old_single_worker_front_record_is_rejected_by_current_rule():
    with fixture() as (s, _):
        s.br['reason'] = "boat's front: the agents of 1 of 2 VMs don't answer (502)"
        assert s.rejudge_breaker(reason='current front minimum')['decision'] == 'closed'


def test_http_rejudge_is_authenticated_protocol_checked_and_dispatched():
    from test_http_admission import HTTPAdmission
    make = HTTPAdmission().make_handler
    body = json.dumps({'protocol': 1, 'actor': 'alice', 'reason': 'legacy trip'}).encode()
    h, owner = make('/breaker/rejudge', body)
    owner.rejudge_breaker = Mock(return_value={'ok': True, 'decision': 'closed'})
    h._send = Mock()
    h.do_POST()
    owner.rejudge_breaker.assert_called_once_with(actor='alice', reason='legacy trip')
    assert h._send.call_args.args[0] == 200
    h, owner = make('/breaker/rejudge', body)
    owner.rejudge_breaker = Mock()
    h.headers['X-SC-Token'] = 'wrong'
    h._send = Mock()
    h.do_POST()
    owner.rejudge_breaker.assert_not_called()
    assert h._send.call_args.args[0] == 403
    h, owner = make('/breaker/rejudge', b'{"protocol":0}')
    owner.rejudge_breaker = Mock()
    h._send = Mock()
    h.do_POST()
    owner.rejudge_breaker.assert_not_called()
    assert h._send.call_args.args[0] == 400


if __name__ == '__main__':
    for name, function in list(globals().items()):
        if name.startswith('test_'):
            function()
            print('ok', name)
