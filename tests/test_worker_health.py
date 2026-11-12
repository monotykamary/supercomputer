"""Worker quarantine and ghost recovery through the production scheduler seams."""
import itertools
from types import SimpleNamespace
from unittest.mock import Mock, patch
from test_wiring import S, boat, batch, sched, vm


def test_local_quarantine_is_excluded_from_outage_and_placement():
    s = sched()
    b = batch(s, klass=2)
    a, healthy = vm(s, 'a'), vm(s, 'healthy')
    a.failing, a.quarantine = 100, 'worker-local agent connection refused'
    assert s._outage_vms() == [healthy]
    assert not s._outage()
    assert not S.vm_fits(S.fit_shape(next(iter(b.items.values()))), a, set())
    assert s._slot(next(iter(b.items.values()))) is healthy
    # Even an unclassified single failure is not evidence of a pool-wide fault.
    a.quarantine = ''
    assert not s._outage()
    healthy.failing = 1
    assert s._outage()


def test_ghost_predicate_never_reclaims_live_strict_uncertain_or_completed_runs():
    for strict, complete, recorded, live, descendants, done in itertools.product((False, True), repeat=6):
        assert S.rules.ghost_slot(strict, complete, recorded, live, descendants, done) == (
            not strict and complete and recorded and not live and not descendants and not done)


def test_reclaim_delivers_terminal_loss_without_controller_retry():
    from supercomputer import workerprobe
    s = sched()
    b = batch(s, n=8, klass=4)
    a = vm(s, 'a')
    a.type, a.slots = 'xlarge', S.vm_slots('xlarge')
    s._start_on = lambda *a: None
    for it in b.items.values():
        assert s._place(it, a)
        it.state = 'running'
    def audit(v, runs, reclaim):
        assert reclaim and len(runs) == 8
        return {'protocol': 1, 'runs': [{**r, 'reclaimed': True, 'result': {
            'batch': r['batch'], 'name': r['name'], 'code': -1, 'seconds': -1,
            'lost': True, 'status': 'LOST', 'ghost_reclaimed': True}} for r in runs]}
    with patch.object(workerprobe, 'audit', side_effect=audit):
        s._reclaim_ghosts(a)
    assert not a.running and len(b.rows) == 8
    assert all(r['lost'] and r['status'] == 'LOST' for r in b.rows)
    assert all(it.state == 'done' and it.fails == 0 and it.tries == 1 for it in b.items.values())


def test_repair_preserves_live_unknown_and_strict_runs():
    for strict, reclaimed in ((False, False), (True, False), (False, True)):
        s = sched()
        b = batch(s)
        b.strict_once = strict
        a = vm(s, 'a')
        a.agent = 'legacy'
        s.agent_version, s.agent_b64 = 'current', 'source'
        it = next(iter(b.items.values()))
        a.running[(b.id, it.run_name)] = it
        def reclaim(v):
            if reclaimed:
                v.running.clear()
            return {'protocol': 1, 'runs': []}
        s._reclaim_ghosts = Mock(side_effect=reclaim)
        s._renew_lease = Mock(return_value=True)
        a.client = SimpleNamespace(token='test', ping=lambda **kw: {'ok': True, 'version': 'current'})
        def boot(*args, **kw):
            assert a.upgrading  # no placement while evidence and bootstrap run
            return {'ok': True}
        assert not s._repair(a)
        s._reclaim_ghosts.assert_not_called()
        with patch.object(boat, 'bootstrap_agent', side_effect=boot) as bootstrap:
            assert s._repair(a, reclaim_ghosts=True) == (reclaimed and not strict)
            assert bootstrap.called == (reclaimed and not strict)
        if strict:
            s._reclaim_ghosts.assert_not_called()


def test_targeted_recovery_requires_the_current_agent_binding():
    s = sched()
    a = vm(s, 'only-this-worker')
    s.agent_version = 'current-target'
    s._repair = Mock(return_value=True)
    try:
        s.recover_worker(a.id, 'stale-target')
    except ValueError:
        pass
    else:
        raise AssertionError('stale agent binding accepted')
    s._repair.assert_not_called()
    result = s.recover_worker(a.id, 'current-target')
    assert result['ok'] and result['vm'] == a.id and result['target_agent'] == 'current-target'
    s._repair.assert_called_once_with(a, reclaim_ghosts=True)


def test_ghost_journal_failure_keeps_ownership_and_slot():
    s = sched()
    b = batch(s)
    a = vm(s, 'a')
    s._start_on = lambda *a: None
    it = next(iter(b.items.values()))
    assert s._place(it, a)
    it.state = 'running'
    row = {'batch': b.id, 'name': it.run_name, 'lost': True, 'ghost_reclaimed': True, 'code': -1}
    with patch.object(s, '_append_owned', side_effect=OSError('disk full')):
        try:
            s._finished_ghost(a, row)
        except OSError:
            pass
        else:
            raise AssertionError('failed persistence accepted')
    assert a.running[(b.id, it.run_name)] is it and it.state == 'running' and not b.rows


if __name__ == '__main__':
    for name, fn in list(globals().items()):
        if name.startswith('test_'):
            fn()
            print('ok', name)
