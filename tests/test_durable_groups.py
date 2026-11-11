"""Native durable cross-batch group contracts; no provider calls."""
import copy
import json
import pathlib
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch
from test_wiring import S, boat, sched, vm


class DurableGroups(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        for target, name, value in ((S, 'DIR', self.root), (S, 'BLOBS', self.root / 'blobs'),
                                    (boat, 'LEDGER', self.root / 'ledger')):
            p = patch.object(target, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.s = sched()
        self.s.code = 'fixture'
        self.s._start_on = Mock()

    def message(self, bid, group='campaign/case/variant', **extra):
        return {'batch': bid, 'klass': 3, 'image': 'img', 'tree': {'id':'t','files':{},'links':{}},
                'ctx_b64':'', 'placement_contract':1,
                'items':[{'name':'i', 'est':300, 'distinct_vm_group':group,
                          'distinct_vm_group_scope':'durable', **extra}]}

    def batch(self, bid, group='campaign/case/variant', **extra):
        msg = self.s._bind_durable(self.message(bid, group, **extra))
        b = S.Batch(msg)
        self.s._attach_durable(b)
        self.s.batches[bid] = b
        return b.items['i'], msg

    def machine(self, name='A'):
        v = vm(self.s, name)
        v.lease = {'boot':'boot-' + name}
        return v

    def test_cross_batch_reuse_refused_and_other_group_allowed(self):
        first, _ = self.batch('a')
        second, _ = self.batch('b')
        other, _ = self.batch('c', 'other')
        v = self.machine()
        self.assertTrue(self.s._place(first, v))
        first.state = 'done'
        v.running.clear()
        self.s.batches.pop('a')
        self.assertFalse(self.s._place(second, v))
        self.assertFalse(S.vm_fits(S.fit_shape(second), v, set()))
        self.assertIsNone(self.s._slot(second))
        self.assertTrue(self.s._place(other, v))

    def test_concurrent_assignments_have_one_winner(self):
        items = [self.batch('b' + str(i))[0] for i in range(12)]
        v = self.machine()
        barrier = threading.Barrier(len(items))
        results = []
        def place(it):
            barrier.wait(timeout=5)
            results.append(self.s._place(it, v))
        threads = [threading.Thread(target=place, args=(it,)) for it in items]
        for t in threads: t.start()
        for t in threads: t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(sum(results), 1)
        self.assertEqual(self.s._start_on.call_count, 1)

    def test_registry_survives_closed_batch_and_restart(self):
        it, msg = self.batch('a')
        self.assertTrue(self.s._place(it, self.machine()))
        self.s.batches.clear()
        before = self.s._durable_snapshot()
        resumed = sched()
        self.assertEqual(resumed._durable_snapshot(), before)
        self.s = resumed
        other, _ = self.batch('b')
        self.assertFalse(self.s._place(other, self.machine()))
        self.assertEqual(before['campaign/case/variant']['used']['A']['boot_id'], 'boot-A')

    def test_expiry_boundary_and_old_queued_epoch_never_rebinds(self):
        with patch.object(S.time, 'time', return_value=100):
            first, _ = self.batch('a')
            queued, _ = self.batch('b')
            self.assertTrue(self.s._place(first, self.machine()))
            opened = self.s.group_get('campaign/case/variant')
            closed = self.s.group_close(opened['group'], opened['generation'], 10)
            self.assertEqual(closed['expires_at'], 110)
        with patch.object(S.time, 'time', return_value=109.999):
            self.assertFalse(self.s._place(queued, self.machine()))
            self.assertEqual(self.s.group_open(opened['group'])['generation'], opened['generation'])
        with patch.object(S.time, 'time', return_value=110):
            self.assertFalse(self.s._place(queued, self.machine('B')))
            fresh, _ = self.batch('fresh')
            self.assertNotEqual(fresh.spec['distinct_vm_group_generation'], queued.spec['distinct_vm_group_generation'])
            self.assertTrue(self.s._place(fresh, self.machine()))
            self.assertFalse(self.s._place(queued, self.machine('C')))

    def test_close_is_idempotent_cannot_extend_or_target_reopened_group(self):
        opened = self.s.group_open('g')
        closed = self.s.group_close('g', opened['generation'], 5)
        self.assertEqual(self.s.group_close('g', opened['generation'], 5)['expires_at'], closed['expires_at'])
        with self.assertRaises(ValueError): self.s.group_close('g', opened['generation'], 6)
        with patch.object(S.time, 'time', return_value=closed['expires_at']):
            fresh = self.s.group_open('g')
            self.assertNotEqual(fresh['generation'], opened['generation'])
            with self.assertRaises(ValueError): self.s.group_close('g', opened['generation'], 0)

    def test_bad_ttl_and_pinned_epoch_refused(self):
        opened = self.s.group_open('g')
        for ttl in (-1, float('nan'), float('inf'), True, '1'):
            with self.subTest(ttl=ttl), self.assertRaises(ValueError): self.s.group_close('g', opened['generation'], ttl)
        with self.assertRaises(ValueError): self.batch('bad', 'g', distinct_vm_group_generation='stale')

    def test_receipt_contains_scope_epoch_boot_vm_and_attempt(self):
        it, _ = self.batch('a')
        self.assertTrue(self.s._place(it, self.machine()))
        row = it.last_placement
        self.assertEqual(row['distinct_vm_group_scope'], 'durable')
        self.assertEqual(row['distinct_vm_group_generation'], it.spec['distinct_vm_group_generation'])
        self.assertEqual((row['vm'], row['boot_id'], row['attempt']), ('A', 'boot-A', 1))
        self.assertEqual(self.s._durable_snapshot()['campaign/case/variant']['used']['A'], row)

    def test_failed_registry_write_never_launches(self):
        it, _ = self.batch('a')
        v = self.machine()
        with patch.object(self.s._groups(), 'save', side_effect=OSError('full')):
            self.assertFalse(self.s._place(it, v))
        self.s._start_on.assert_not_called()
        self.assertFalse(v.running)

    def test_no_demand_or_vm_parking_after_batches_close(self):
        it, _ = self.batch('a')
        self.assertTrue(self.s._place(it, self.machine()))
        self.s.batches.clear()
        self.s.vms.clear()
        self.assertTrue(self.s._durable_snapshot())
        self.assertEqual(self.s._plan(S.time.time()), {})
        self.assertFalse(self.s.vms)

    def test_reload_uses_pinned_admission_and_not_new_epoch(self):
        with patch.object(S.time, 'time', return_value=10):
            it, msg = self.batch('a')
            g = self.s.group_get('campaign/case/variant')
            self.s.group_close(g['group'], g['generation'], 1)
        q = self.root / 'queue'
        q.mkdir(exist_ok=True)
        (q / 'a.json').write_text(json.dumps(msg))
        with patch.object(S.time, 'time', return_value=12):
            self.s.group_open(g['group'])
            resumed = sched()
            resumed._take_pool = Mock()
            resumed._reattach = Mock()
            with patch.object(S.threading, 'Thread'): resumed.reload()
            old = resumed.batches['a'].items['i']
            self.assertEqual(old.spec['distinct_vm_group_generation'], it.spec['distinct_vm_group_generation'])
            self.assertFalse(resumed._place(old, vm(resumed, 'fresh')))

    def test_handoff_snapshot_keeps_groups_without_batches(self):
        it, _ = self.batch('a')
        self.assertTrue(self.s._place(it, self.machine()))
        expected = self.s._durable_snapshot()
        self.s.batches.clear()
        self.s.vms.clear()
        self.assertEqual(self.s._handoff_snapshot()['durable_distinct_vm_groups'], expected)

    def test_registry_corruption_fails_closed_not_empty_history(self):
        self.s.group_open('g')
        self.s._groups().path.write_text('{')
        resumed = sched()
        with self.assertRaises((ValueError, RuntimeError)):
            resumed._durable_snapshot()

    def test_old_batch_scope_unchanged_and_independent(self):
        from test_distinct_vm import group_batch
        b, _ = group_batch(self.s, 'a', cpu=False)
        c, _ = group_batch(self.s, 'b', cpu=False)
        v = self.machine()
        shape = S.fit_shape(c.items['i0'])
        self.assertEqual(shape[-1], ('b', 'run/stage/case/rep', frozenset()))
        self.assertTrue(self.s._place(b.items['i0'], v))
        self.assertTrue(self.s._place(c.items['i0'], v))
        self.assertFalse((self.root / 'durable-groups.json').exists())

    def test_exact_handoff_ack_includes_membership_epoch_and_close_deadline(self):
        import os
        from contextlib import nullcontext
        from types import SimpleNamespace
        from supercomputer import canary
        it, _ = self.batch('a')
        self.assertTrue(self.s._place(it, self.machine()))
        g = self.s.group_get('campaign/case/variant')
        self.s.group_close(g['group'], g['generation'], 500)
        expected = self.s._durable_snapshot()
        variants = [('exact', copy.deepcopy(expected)), ('missing', None)]
        for field in ('used', 'generation', 'expires_at'):
            altered = copy.deepcopy(expected)
            altered[g['group']][field] = {} if field == 'used' else 'wrong' if field == 'generation' else 0
            variants.append((field, altered))
        for label, manifest in variants:
            with self.subTest(label=label):
                child = sched()
                child.code, child.recovery_errors = 'child', {}
                child.canary = canary.Canary(self.root)
                child._worker = Mock()
                snap = {'from_pid':os.getpid()+100000, 't':S.time.time(), 'from':'parent',
                        'vms':[], 'batches':[], 'admission_guard':True}
                if manifest is not None: snap['durable_distinct_vm_groups'] = manifest
                (self.root / 'handoff.json').write_text(json.dumps(snap))
                registry = SimpleNamespace(locked=lambda: nullcontext({'vms':{}}))
                with patch.object(S, 'Registry', return_value=registry), \
                     patch.object(boat, 'pid_alive', return_value=True), \
                     patch.object(S.threading, 'Thread'):
                    child._take_pool()
                ack = child._handoff_ack
                self.assertEqual(ack['durable_distinct_vm_groups'], expected)
                self.assertEqual(ack['admissions']['ready'], label == 'exact')
                self.assertEqual(child.recovery_blocked, label != 'exact')
                child._worker.assert_not_called()

    def test_successor_capability_required_even_after_batches_close(self):
        self.s.group_open('history-only')
        self.s.alert = Mock()
        for code in ('fixture', 'legacy'):
            (self.root / 'code' / code / 'supercomputer').mkdir(parents=True)
        with patch.object(S, 'prepare_snapshot', return_value={'cpu_model_exact':True, 'distinct_vm_group':True}), \
             patch.object(boat, 'states', return_value={}), patch.object(S.subprocess, 'Popen') as spawn:
            self.s.handoff(back_to='legacy')
        spawn.assert_not_called()
        self.assertIn('durable', self.s.alert.call_args.args[0])

    def test_cold_reload_refuses_missing_membership_even_with_valid_json(self):
        it, msg = self.batch('a')
        self.assertTrue(self.s._place(it, self.machine()))
        q = self.root / 'queue'
        (q / 'a.json').write_text(json.dumps(msg))
        registry = self.s._groups()
        damaged = registry.snapshot()
        damaged['campaign/case/variant']['used'].clear()
        registry.save(damaged)
        resumed = sched()
        resumed._take_pool, resumed._reattach = Mock(), Mock()
        with patch.object(S.threading, 'Thread'): resumed.reload()
        self.assertIn('a', resumed.recovery_errors)
        self.assertNotIn('a', resumed.batches)
        self.assertTrue(resumed.recovery_blocked)

    def test_reserved_vm_remains_excluded_if_batch_journal_write_fails(self):
        first, _ = self.batch('a')
        v = self.machine()
        original = self.s._append_owned
        def fail_placed(bid, suffix, record):
            if suffix == 'placed': raise OSError('full')
            return original(bid, suffix, record)
        with patch.object(self.s, '_append_owned', side_effect=fail_placed):
            self.assertFalse(self.s._place(first, v))
        self.s._start_on.assert_not_called()
        second, _ = self.batch('b')
        self.assertFalse(self.s._place(second, v))

    def test_expired_queued_work_terminates_without_fallback_or_growth(self):
        it, _ = self.batch('a')
        g = self.s.group_get('campaign/case/variant')
        self.s.group_close(g['group'], g['generation'], 0)
        self.s._release_idle = Mock()
        self.s.schedule()
        self.assertEqual(it.state, 'done')
        self.assertNotIn('lost', it.batch.rows[0])
        self.assertIn('expired', it.batch.rows[0]['error'])
        self.s._start_on.assert_not_called()
        self.assertEqual(self.s._plan(S.time.time()), {})

    def test_submit_persists_generation_before_publication(self):
        self.assertTrue(self.s.submit(self.message('submitted'))['ok'])
        disk = json.loads((self.root / 'queue' / 'submitted.json').read_text())
        spec = disk['items'][0]
        self.assertEqual(spec['distinct_vm_group_generation'], self.s.group_get(spec['distinct_vm_group'])['generation'])
        second = self.message('pinned-bad')
        second['items'][0]['distinct_vm_group_generation'] = 'stale'
        with self.assertRaises(ValueError): self.s.submit(second)
        self.assertNotIn('pinned-bad', self.s.batches)

    def test_uncertain_post_rename_write_poison_prevents_further_placement(self):
        from supercomputer import durable_groups
        self.s.group_open('g')
        registry = self.s._groups()
        data = registry.snapshot()
        data['g']['generation'] = 'new-persisted-generation'
        with patch.object(durable_groups.os, 'fsync', side_effect=[None, OSError('directory fsync')]):
            with self.assertRaises(OSError): registry.save(data)
        with self.assertRaises(RuntimeError): registry.allowed('g', 'new-persisted-generation', 'A', S.time.time())
        self.assertEqual(sched()._durable_snapshot()['g']['generation'], 'new-persisted-generation')

    def test_terminal_infrastructure_failure_carries_attempt_and_never_falls_back(self):
        it, _ = self.batch('a')
        for name in ('A', 'B', 'C'):
            v = self.machine(name)
            self.assertTrue(self.s._place(it, v))
            self.s._boat_failed(it, name, 'lost')
        row = it.batch.rows[-1]
        self.assertNotIn('lost', row)
        self.assertEqual(row['placement']['attempt'], 3)
        self.assertEqual(row['placement']['vm'], 'C')
        self.assertEqual(row['placement']['boot_id'], 'boot-C')

    def test_bad_scope_or_generation_without_scope_refused(self):
        for extra in ({'distinct_vm_group_scope':'typo'}, {'distinct_vm_group_generation':'x','distinct_vm_group_scope':None}):
            with self.assertRaises(ValueError): S.Scheduler._validate_submission(self.message('a', **extra))


if __name__ == '__main__': unittest.main()
