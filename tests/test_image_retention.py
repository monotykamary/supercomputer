"""Controller pins and non-destructive client pruning; no provider operations."""
import contextlib
import io
import json
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from test_wiring import S, boat, sched, vm
from supercomputer.image_pins import ImagePinsUnavailable, RETENTION_SECONDS


class ImageRetention(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        for target, name, value in ((S, 'DIR', self.root), (S, 'BLOBS', self.root / 'blobs'),
                                    (boat, 'LEDGER', self.root / 'ledger')):
            p = patch.object(target, name, value)
            p.start()
            self.addCleanup(p.stop)
        self.s = sched()
        self.s.code = 'fixture'

    def message(self, image='sc-original'):
        return {'batch': 'test-batch', 'klass': 3, 'image': image,
                'tree': {'id': 't', 'files': {}, 'links': {}}, 'ctx_b64': '',
                'items': [{'name': 'i', 'est': 300}]}

    def test_pin_is_durable_before_admission_rename_or_ack(self):
        original = Path.rename
        def fail_queue(path, target):
            if path.name == '.test-batch.json.tmp':
                self.assertTrue((self.root / 'image-pins.json').is_file())
                self.assertIn('sc-original', self.s._pins().protected(10**10))
                raise OSError('simulated admission publication failure')
            return original(path, target)
        with patch.object(Path, 'rename', fail_queue), self.assertRaises(OSError):
            self.s.submit(self.message())
        self.assertFalse((self.root / 'queue' / 'test-batch.json').exists())
        self.assertNotIn('test-batch', self.s.batches)
        self.assertIn('sc-original', self.s._pins().protected(10**10))

    def test_recovery_backfills_legacy_admissions_and_quarantines_conflicting_pins(self):
        q = self.root / 'queue'
        q.mkdir()
        (q / 'test-batch.json').write_text(json.dumps(self.message()))
        for conflict in (False, True):
            with self.subTest(conflict=conflict):
                if conflict:
                    message = self.message('sc-conflict')
                    (q / 'test-batch.json').write_text(json.dumps(message))
                child = sched()
                child._take_pool = Mock()
                child._reattach = Mock()
                with patch.object(S.threading, 'Thread'):
                    child.reload()
                self.assertEqual(child.recovery_blocked, conflict)
                self.assertEqual('test-batch' in child.batches, not conflict)
                self.assertEqual('test-batch' in child.recovery_errors, conflict)
        self.assertIn('sc-original', self.s._pins().protected(10**10))

    def test_only_positive_durable_cleanup_closes_the_batch_pin(self):
        for positive in (False, True):
            with self.subTest(positive=positive):
                b = SimpleNamespace(id='closed-' + str(positive), image='sc-original', closed=False)
                self.s.batches[b.id] = b
                self.s._pin_batch_image(b)
                self.s.batch_info = Mock(return_value={})
                self.s.snapshot = Mock(return_value={})
                with patch.object(S.history, 'capture', return_value={}):
                    self.s._retain_closed(b, {'cleanup_complete': positive})
                record = self.s._pins().get('batch:' + b.id, S.time.time())
                self.assertEqual(record['live'], not positive)
                self.assertTrue(record['protected'])
                if positive:
                    self.assertGreaterEqual(record['protected_until'] - record['closed_at'], RETENTION_SECONDS)
        b = SimpleNamespace(id='capture-failed', image='sc-original', closed=False)
        self.s.batches[b.id] = b
        self.s._pin_batch_image(b)
        with patch.object(S.history, 'capture', side_effect=OSError('disk full')), self.assertRaises(OSError):
            self.s._retain_closed(b, {'cleanup_complete': True})
        self.assertTrue(self.s._pins().get('batch:' + b.id, S.time.time())['live'])
        self.assertIn(b.id, self.s.batches)

    def test_successor_rejects_changed_pin_history(self):
        import copy
        import os
        from supercomputer import canary
        self.s.image_pin_register(['sc-original'], 'go', 'go-1')
        expected = self.s._image_pins_manifest()
        wrong = copy.deepcopy(expected)
        wrong['records']['go:go-1']['images'] = ['sc-other']
        for label, manifest in [('exact', expected), ('empty', {}), ('changed', wrong)]:
            with self.subTest(label=label):
                child = sched()
                child.code, child.recovery_errors = 'child', {}
                child.canary = canary.Canary(self.root)
                child._worker = Mock()
                snap = {'from_pid': os.getpid() + 100000, 't': S.time.time(), 'from': 'parent',
                        'vms': [], 'batches': [], 'admission_guard': True, 'image_pins': manifest}
                (self.root / 'handoff.json').write_text(json.dumps(snap))
                registry = SimpleNamespace(locked=lambda: contextlib.nullcontext({'vms': {}}))
                with patch.object(S, 'Registry', return_value=registry), \
                     patch.object(boat, 'pid_alive', return_value=True), patch.object(S.threading, 'Thread'):
                    child._take_pool()
                self.assertEqual(child._handoff_ack['image_pins'], expected)
                self.assertEqual(child._handoff_ack['admissions']['ready'], label == 'exact')
                self.assertEqual(child.recovery_blocked, label != 'exact')
                child._worker.assert_not_called()

    def test_http_pin_registration_requires_auth_and_mutation_gate(self):
        import threading
        from test_http_admission import handler_class
        self.s._request_lock = threading.RLock()
        self.s.token = 'token'
        self.s._cpu_add = Mock()
        self.s._h = Mock(return_value=Mock())
        cls = handler_class(self.s)
        for token, kind, frozen, expected in [('wrong', 'go', False, 403),
                                               ('token', 'bad', False, 400),
                                               ('token', 'go', True, 503),
                                               ('token', 'go', False, 200)]:
            body = json.dumps({'images': ['sc-original'], 'kind': kind, 'reference': 'http-go'}).encode()
            h = object.__new__(cls)
            h.path = '/image-pins/register'
            h.headers = {'Content-Length': str(len(body)), 'X-SC-Token': token}
            h.rfile, h.client_address, h.close_connection = io.BytesIO(body), ('127.0.0.1', 1), False
            replies = []
            h._send = lambda code, obj=None, raw=None: replies.append((code, raw))
            self.s.handing_off = frozen
            h.do_POST()
            self.assertEqual(replies[0][0], expected)
            if expected == 200:
                self.assertTrue(json.loads(replies[0][1])['ok'])
        self.assertEqual(len(self.s._pins().snapshot(S.time.time())['records']), 1)

    def test_kept_submission_cannot_change_its_image(self):
        self.assertTrue(self.s.submit(self.message())['ok'])
        self.assertTrue(self.s.submit(self.message())['kept'])
        reply = self.s.submit(self.message('sc-other'))
        self.assertFalse(reply['ok'])
        self.assertIn('image differs', reply['error'])
        self.assertEqual(self.s.batches['test-batch'].image, 'sc-original')
        self.assertNotIn('sc-other', self.s._pins().protected(10**10))

    def test_broken_registry_refuses_admission(self):
        (self.root / 'image-pins.json').write_text('{bad')
        with self.assertRaises(ImagePinsUnavailable):
            self.s.submit(self.message())
        self.assertFalse((self.root / 'queue' / 'test-batch.json').exists())
        self.assertNotIn('test-batch', self.s.batches)

    def test_public_registration_uses_server_time_and_separate_namespaces(self):
        with patch.object(S.time, 'time', return_value=1000):
            packet = self.s.image_pin_register(['sc-original'], 'packet', 'test-batch')
            go = self.s.image_pin_register(['sc-original'], 'go', 'test-batch')
            self.assertTrue(self.s.submit(self.message())['ok'])
        self.assertEqual(packet['accepted_at'], 1000)
        self.assertGreaterEqual(packet['pin']['protected_until'], 1000 + RETENTION_SECONDS)
        self.assertNotEqual(packet['pin']['ref'], go['pin']['ref'])
        self.assertEqual(len(self.s._pins().snapshot(1000)['records']), 3)
        self.assertFalse(packet['gc_enabled'])
        with self.assertRaises(ValueError):
            self.s.image_pin_register(['sc-original'], 'batch', 'test-batch')

    def test_manifest_survives_restart_and_does_not_change_just_with_time(self):
        with patch.object(S.time, 'time', return_value=1000):
            self.s.image_pin_register(['sc-original'], 'go', 'go-1')
            first = self.s._image_pins_manifest()
        with patch.object(S.time, 'time', return_value=1000 + 2 * RETENTION_SECONDS):
            self.assertEqual(first, self.s._image_pins_manifest())
            self.assertEqual(first, sched()._image_pins_manifest())

    def test_controller_lists_residents_pending_creates_and_expired_live_pins(self):
        with patch.object(S.time, 'time', return_value=1000):
            self.s.submit(self.message())
        self.s.batches.clear()  # disappearance is NOT a cleanup receipt
        vm(self.s, 'resident', image='sc-worker').dead = True  # uncertainty also protects
        self.s.creating[('sc-creating', 'large')] = 1
        registry = {'vms': {'stopped': {'snapshot': 'sc-stopped', 'state': 'stopped'},
                            'deleted': {'snapshot': 'sc-deleted', 'state': 'deleted'}}}
        with patch.object(boat.Registry, 'read', return_value=registry), patch.object(S.time, 'time', return_value=10**10):
            result = self.s.image_pin_list()
        self.assertFalse(result['gc_enabled'])
        self.assertFalse(result['inventory_complete'])
        self.assertEqual(set(result['protected']), {'sc-original', 'sc-worker', 'sc-creating', 'sc-stopped'})

    def test_client_never_deletes_even_unpinned_or_unknown_images(self):
        pool = {'images': ['sc-original', 'sc-newer'], 'image_t': {'sc-original': 1, 'sc-newer': 2}}
        @contextlib.contextmanager
        def locked(_registry):
            yield pool
        with patch.object(boat, 'project', return_value=SimpleNamespace(prefix='gdpw', final_prefix='sc')), \
             patch.object(boat.Registry, 'locked', locked), \
             patch.object(boat, 'named_snapshots', return_value={'sc-original': {'status': 'ready'}}) as inventory, \
             patch.object(boat, 'boat') as provider:
            boat.prune_snapshots(set(), 'sc-current')
        inventory.assert_not_called()
        provider.assert_not_called()

    def test_client_registry_failure_cannot_turn_into_deletion(self):
        with patch.object(boat, 'project', return_value=SimpleNamespace(prefix='gdpw', final_prefix='sc')), \
             patch.object(boat.Registry, 'locked', side_effect=OSError('bad registry')), \
             patch.object(boat, 'boat') as provider:
            boat.prune_snapshots(set(), 'sc-current')
        provider.assert_not_called()

    def test_cli_register_and_list_return_json_without_building(self):
        from supercomputer import __main__ as cli
        reply = {'ok': True, 'gc_enabled': False}
        for args, expected in [
            (['image', 'pin', 'sc-one', '--kind', 'go', '--reference', 'go-1'],
             ('register', {'images': ['sc-one'], 'kind': 'go', 'reference': 'go-1'})),
            (['image', 'pins'], ('list', {})),
        ]:
            with patch.object(cli.config, 'use'), patch.object(boat, 'image_pins', return_value=reply) as call, \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                cli.main(args)
            self.assertEqual(json.loads(output.getvalue()), reply)
            self.assertEqual(call.call_args.args, (expected[0],))
            self.assertEqual(call.call_args.kwargs, expected[1])
        with patch.object(cli.config, 'use'), contextlib.redirect_stderr(io.StringIO()), \
             self.assertRaises(SystemExit) as error:
            cli.main(['image', 'pins', '--kind', 'go'])
        self.assertEqual(error.exception.code, 2)

    def test_pin_client_requires_capability_and_never_starts_a_controller(self):
        with patch.object(S, 'current', return_value=None):
            with self.assertRaises(boat.Unavailable):
                boat.image_pins()
        connection = Mock()
        connection.req.return_value = {'image_pins_protocol': True}  # bool is not version 1
        with patch.object(S, 'current', return_value=connection), self.assertRaises(boat.Unavailable):
            boat.image_pins()
        connection.req.side_effect = [{'image_pins_protocol': 1}, {'ok': True}]
        with patch.object(S, 'current', return_value=connection):
            self.assertTrue(boat.image_pins('register', images=['sc-one'], kind='go', reference='g')['ok'])
        self.assertEqual(connection.req.call_args.args,
                         ('/image-pins/register', {'images': ['sc-one'], 'kind': 'go', 'reference': 'g'}))


if __name__ == '__main__':
    unittest.main()
