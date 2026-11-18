"""Liveness detection and same-boot close custody, without touching provider VMs."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

import test_owned_scheduler as fixtures
from supercomputer import namespace_recovery as N
from supercomputer import namespace_recovery_controller as C
from supercomputer import scheduler as S
from supercomputer import worker_loss_probe as P


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value if isinstance(value, str) else json.dumps(value))


class WorkerLossTests(unittest.TestCase):
    def setUp(self):
        self.fixture = f = fixtures.OwnedSchedulerTests()
        f.setUp()
        self.addCleanup(f.doCleanups)
        self.addCleanup(f.tmp.cleanup)
        f.place_without_thread()
        self.s, self.b, self.it, self.vm = f.s, f.b, f.it, f.vm
        self.it.state = 'running'
        self.vm.quarantine = 'worker-local agent connection refused'
        self.binding = b = C.binding_for(self.it)
        self.guest, self.proc = f.root / 'guest', f.root / 'proc'
        put(self.guest / 'lease.lock', '')
        put(self.guest / 'owned-items.lock', '')
        put(self.guest / 'lease.json', {'vm_id': b['vm'], 'boot': b['boot_id']})
        put(self.proc / 'sys/kernel/random/boot_id', b['boot_id'])
        self.host = self.proc / '1/ns/pid'
        put(self.host, '')
        self.ns = f.root / 'pidns'
        put(self.ns, '')
        self.sup = {'pid': 701, 'boot': b['boot_id'], 'start_ticks': 193}
        self.init = {'pid': 702, 'boot': b['boot_id'], 'start_ticks': 203, 'pidns': self.ns.stat().st_ino}
        self.rowfile = self.guest / 'owned-items' / b['batch'] / (b['run_name'] + '.json')
        self.row = {'identity': b['launch_identity'], 'state': 'launched', 'contained': True,
                    'reaped': False, 'supervisor': self.sup, 'init': self.init}
        put(self.rowfile, self.row)
        self.job = self.guest / 'b' / b['batch'] / 'jobs' / b['run_name']
        put(self.job / 'spec.json',
            {'owner': b['owner'], 'strict_once': True, 'name': b['run_name'], 'launch_identity': b['launch_identity']})
        self.probes = 0

    def read_namespace_absence(self, binding, nonce, *, deadline, allow_published=False):
        self.probes += 1
        return P.observe(self.guest, self.proc, {'binding': binding, 'nonce': nonce,
                                                 'allow_published': allow_published})

    def task(self, pid, ticks, ns=None, state='S', nested_pid=None):
        entry = self.proc / str(pid)
        put(entry / 'stat', f'{pid} (test worker) {state}' + ' 0' * 18 + f' {ticks}')
        put(entry / 'status', f'NSpid:\t{pid}' + (f'\t{nested_pid}' if nested_pid else ''))
        if ns is not None:
            (entry / 'ns').mkdir()
            os.link(ns, entry / 'ns/pid')

    def check(self):
        self.s._check_unreachable_item(self.vm, transport=self)

    def test_tick_fences_quarantined_dead_supervisor_and_recovery_closes_same_boot(self):
        # PID reuse is not the original supervisor. A zombie with no namespace
        # symlink and a live unrelated namespace must not prevent recovery.
        self.task(701, 199, self.host)
        self.task(900, 310, state='Z')
        self.task(901, 320, self.host)
        self.vm.client.rpc.side_effect = S.boat.WorkerEndpointUnavailable('test refused')
        self.s.br = {'open_until': 0}
        with patch.object(S.namespace_recovery, 'NativeTransport', return_value=self), patch.object(self.s, '_outage', return_value=False):
            self.s._tick(self.vm)
        self.assertEqual(self.it.state, 'done')
        self.assertTrue(self.it.owned.closed)
        self.assertFalse(self.it.owned.reaped)
        self.assertIs(self.vm.running[(self.b.id, self.it.run_name)], self.it)
        self.assertEqual(self.probes, 1)
        [row] = S.jlines(self.fixture.root / 'queue' / (self.b.id + '.rows'))
        self.assertEqual((row['status'], row['terminal_reason']), ('LOST', 'worker-lost'))
        self.assertEqual(row['worker_loss']['supervisor'], self.sup)
        self.assertFalse(row['cleanup_complete'])
        self.assertFalse(self.s._place(self.it, self.vm))
        req = {'protocol': 1, 'mode': 'observe-namespace', 'authorized': True,
               'authorization_reference': 'worker-loss-test', **self.binding}
        with self.assertRaisesRegex(ValueError, 'fenced terminal'):
            C.recover(self.s, self.fixture.root, req, self)
        self.assertFalse(self.s.cancel(self.b.id, 0)['cleanup_complete'])
        result = C.recover(self.s, self.fixture.root, req, self)
        self.assertTrue(result['close']['cleanup_complete'])
        self.assertEqual(result['proof']['kind'], 'same-boot-namespace-absence')
        self.assertEqual(result['proof']['evidence']['boot_id'], self.binding['boot_id'])
        self.assertEqual(result['close']['attempts'][0]['cleanup_evidence'], result['proof'])
        self.assertEqual(S.jlines(self.fixture.root / 'queue' / (self.b.id + '.rows')), [row])
        self.assertEqual(json.loads(self.rowfile.read_text()), self.row)  # no guest writes

    def test_live_exact_supervisor_is_not_lost(self):
        self.task(self.sup['pid'], self.sup['start_ticks'], self.host)
        self.check()
        self.assertEqual(self.probes, 1)
        self.assertEqual(self.it.state, 'running')
        self.assertFalse(self.it.owned.closed)
        self.assertEqual(self.b.rows, [])

    def test_live_supervisor_from_nested_pid_view_is_not_lost(self):
        self.task(801, self.sup['start_ticks'], self.host, nested_pid=self.sup['pid'])
        self.check()
        self.assertEqual(self.it.state, 'running')
        self.assertEqual(self.b.rows, [])

    def test_probe_never_discards_an_already_published_result(self):
        put(self.job / 'done.json', {'code': 0, 'launch_identity': self.it.launch_identity})
        self.check()
        self.assertEqual(self.it.state, 'running')
        self.assertEqual(self.b.rows, [])

    def test_unrestricted_initial_procfs_is_required(self):
        self_dir = self.proc / 'self'
        (self_dir / 'ns').mkdir(parents=True)
        os.link(self.host, self_dir / 'ns/pid')
        table = self_dir / 'mountinfo'
        def mounts(options):
            put(table, f'26 1 0:24 / {self.proc} rw - proc proc {options}\n')
        mounts('rw')
        self.assertFalse(P.full_pid_view(self.proc))
        with patch.object(P, 'INITIAL_PID_NS', self.host.stat().st_ino):
            self.assertTrue(P.full_pid_view(self.proc))
            mounts('rw,hidepid=2')
            self.assertFalse(P.full_pid_view(self.proc))

    def test_namespace_member_is_not_lost_even_when_supervisor_and_init_are_absent(self):
        self.task(801, 300, self.ns)
        self.check()
        self.assertEqual(self.probes, 1)
        self.assertEqual(self.b.rows, [])

    def test_missing_namespace_link_for_live_task_is_not_absence(self):
        self.task(801, 300)
        self.check()
        self.assertEqual(self.b.rows, [])

    def test_unknown_custody_is_not_loss(self):
        put(self.rowfile, {**self.row, 'init': None})
        self.check()
        self.assertEqual(self.b.rows, [])

    def test_healthy_worker_and_inflight_start_do_not_probe(self):
        self.vm.quarantine = ''
        self.check()
        self.assertEqual(self.probes, 0)
        self.vm._item_failure_since = S.time.monotonic() - 121
        self.it.start_done.clear()
        self.check()
        self.assertEqual(self.probes, 0)
        self.it.start_done.set()
        self.check()
        self.assertEqual(self.probes, 1)
        self.assertEqual(self.it.state, 'done')

    def test_unreachable_open_close_admission_can_recover_after_loss(self):
        self.assertFalse(self.s.cancel(self.b.id, 0)['cleanup_complete'])
        self.assertEqual(self.it.state, 'cancel_pending')
        self.check()
        self.assertEqual(self.it.state, 'done')
        self.assertEqual(self.b.rows[0]['status'], 'LOST')

    def test_fsync_failure_does_not_expose_loss_or_drop_hold(self):
        with patch.object(self.s, '_append_owned', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.check()
        self.assertEqual(self.it.state, 'running')
        self.assertFalse(self.it.owned.closed)
        self.assertEqual(self.b.rows, [])

    def test_tick_preserves_unsent_acks_when_loss_row_cannot_be_written(self):
        ack = {'batch': 'another-batch', 'name': 'finished-item'}
        self.vm.acks = [ack]
        self.vm.client.rpc.side_effect = S.boat.WorkerEndpointUnavailable('test refused')
        with (patch.object(S.namespace_recovery, 'NativeTransport', return_value=self),
              patch.object(self.s, '_outage', return_value=False),
              patch.object(self.s, '_append_owned', side_effect=OSError('disk full'))):
            with self.assertRaises(OSError):
                self.s._tick(self.vm)
        self.assertEqual(self.vm.acks, [ack])
        self.assertEqual(self.it.state, 'running')

    def test_worker_loss_round_trips_without_reaping_or_requeuing(self):
        self.check()
        with (patch.object(self.s, '_take_pool'), patch.object(S.threading, 'Thread'),
              patch.object(self.s, '_pin_batch_image')):
            self.s.reload()
        self.assertFalse(self.s.recovery_blocked, self.s.recovery_errors)
        b = self.s.batches[self.b.id]
        it = b.items[self.it.name]
        self.assertEqual(b.rows, self.b.rows)
        self.assertEqual(it.state, 'done')
        self.assertTrue(it.owned.closed)
        self.assertFalse(it.owned.reaped)
        self.assertIsNone(getattr(it, 'reattach', None))
        key = (b.id, self.it.run_name)
        self.vm.running.pop(key)
        row = {'batch': b.id, 'name': it.name, 'launch_identity': it.launch_identity}
        self.assertFalse(self.s._finished(self.vm, row), 'no ACK without a positive cleanup proof')
        self.s._restore_worker_loss_hold(self.vm, it)
        self.assertIs(self.vm.running[key], it)
        self.assertEqual(it.state, 'done')

    def test_same_boot_close_refuses_an_incomplete_proc_observation(self):
        self.check()
        self.assertFalse(self.s.cancel(self.b.id, 0)['cleanup_complete'])
        put(self.rowfile, {**self.row, 'init': None})
        req = {'protocol': 1, 'mode': 'observe-namespace', 'authorized': True,
               'authorization_reference': 'worker-loss-test', **self.binding}
        with self.assertRaises(N.RecoveryError):
            C.recover(self.s, self.fixture.root, req, self)
        self.assertFalse(self.it.owned.reaped)


if __name__ == '__main__':
    unittest.main()
