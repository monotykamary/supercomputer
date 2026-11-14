"""Pure tempfile/mock contracts for image pins; no controller or worker imports."""
import copy
import json
import os
from pathlib import Path
import tempfile
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from unittest.mock import patch

from supercomputer.image_pins import (
    ImagePins, ImagePinsUnavailable, RETENTION_SECONDS, retain_image,
)


class ImagePinTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'nested' / 'pins.json'
        self.pins = ImagePins(self.path)

    def test_two_instances_never_overwrite_each_others_pins(self):
        other = ImagePins(self.path)
        self.public(ref='first')
        other.register('second', ['sc-two'], 'go', 1000)
        self.public(ref='third')
        self.assertEqual(set(other.snapshot(1000)['records']), {'first', 'second', 'third'})

    def test_concurrent_instances_serialize_read_modify_write(self):
        import concurrent.futures
        import threading
        barrier = threading.Barrier(8)
        def register(index):
            pins = ImagePins(self.path)
            barrier.wait(timeout=5)
            pins.register('parallel-' + str(index), ['sc-one'], 'packet', 1000)
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            list(pool.map(register, range(8)))
        self.assertEqual(len(self.pins.snapshot(1000)['records']), 8)

    def test_independent_processes_preserve_all_acknowledged_pins(self):
        import subprocess
        code = ('import sys; from supercomputer.image_pins import ImagePins; '
                'ImagePins(sys.argv[1]).register(sys.argv[2], ["sc-one"], "go", 1000)')
        processes = [subprocess.Popen([sys.executable, '-c', code, str(self.path), 'process-' + str(i)],
                                      cwd=str(Path(__file__).resolve().parents[1]),
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE) for i in range(6)]
        try:
            for process in processes:
                out, err = process.communicate(timeout=15)
                self.assertEqual(process.returncode, 0, (out, err))
        finally:
            for process in processes:
                if process.poll() is None:
                    process.kill()  # exact owned PID only
                    process.wait(timeout=5)
        self.assertEqual(len(self.pins.snapshot(1000)['records']), 6)

    def test_lost_previously_observed_file_poisoned_not_reinitialized(self):
        self.public()
        self.path.unlink()
        with self.assertRaises(ImagePinsUnavailable):
            self.pins.snapshot(1000)
        self.assert_poisoned()

    def test_stale_restoration_refused_by_existing_owner(self):
        self.public()
        prior = self.path.read_text()
        self.public(ref='second')
        self.path.write_text(prior)
        with self.assertRaises(ImagePinsUnavailable):
            self.pins.snapshot(1000)
        self.assert_poisoned()

    def public(self, ref='event-1', images=None, kind='packet', now=1000):
        return self.pins.register(ref, ['sc-one'] if images is None else images, kind, now)

    def assert_poisoned(self):
        operations = [lambda: self.pins.snapshot(1000),
                      lambda: self.pins.get('event-1', 1000),
                      lambda: self.pins.protected(1000),
                      lambda: self.public(),
                      lambda: self.pins.pin_batch('batch', ['sc-one'], 1000),
                      lambda: self.pins.mark_batch_closed('batch', 1000),
                      lambda: self.pins.epoch, lambda: self.pins.revision]
        for operation in operations:
            with self.assertRaises(ImagePinsUnavailable):
                operation()
        self.assertIsNone(self.pins._data)

    def test_persistence_reload_epoch_revision_and_detached_views(self):
        initial = self.pins.snapshot(0)
        self.assertEqual(initial['revision'], 0)
        self.public(images=['sc-one', 'sc-two'])
        self.public('event-2', ['sc-one'], 'go')
        self.pins.pin_batch('batch-1', ['sc-three'], 1000)
        snapshot = self.pins.snapshot(1001)
        self.assertEqual(snapshot['revision'], 3)
        self.assertEqual(len(snapshot['protected']['sc-one']), 2)
        reloaded = ImagePins(self.path)
        self.assertEqual(reloaded.snapshot(1001), snapshot)
        self.assertEqual(initial['epoch'], reloaded.epoch)
        snapshot['records']['event-1']['images'].clear()
        snapshot['protected']['sc-one'][0]['images'].clear()
        self.assertEqual(self.pins.get('event-1', 1001)['images'], ['sc-one', 'sc-two'])
        got = self.pins.get('event-1', 1001)
        got['images'].clear()
        self.assertEqual(self.pins.get('event-1', 1001)['images'], ['sc-one', 'sc-two'])
        self.assertIsNone(self.pins.get('missing', 1000))

    def test_seven_day_boundary_and_no_early_delete(self):
        record = self.public(now=10)
        until = 10 + RETENTION_SECONDS
        self.assertGreaterEqual(record['protected_until'], until)
        for now in (0, 10, until - 1, until):
            self.assertIn('sc-one', self.pins.protected(now))
        self.assertNotIn('sc-one', self.pins.protected(until + .001))
        # Expiry is a computed view, not deletion of the immutable binding.
        self.assertIsNotNone(self.pins.get('event-1', until + 1))
        self.assertFalse(hasattr(self.pins, 'unpin'))

    def test_replay_extension_clock_rollback_and_reload(self):
        first = self.public(now=1000)
        revision = self.pins.revision
        self.assertEqual(self.public(now=1000), first)
        self.assertEqual(self.pins.revision, revision)
        self.assertEqual(self.public(now=5)['protected_until'], first['protected_until'])
        extended = self.public(now=2000)
        self.assertEqual(extended['first_observed_at'], 1000)
        self.assertEqual(extended['last_observed_at'], 2000)
        self.assertEqual(extended['protected_until'], 2000 + RETENTION_SECONDS)
        self.assertEqual(self.pins.revision, revision + 1)
        self.pins = ImagePins(self.path)
        self.assertEqual(self.public(now=2)['protected_until'], extended['protected_until'])
        self.assertEqual(self.pins.revision, revision + 1)

    def test_fractional_observation_never_rounds_grace_down(self):
        for n, now in enumerate((.1, 1000.3, 999999999999.9)):
            record = self.public(ref='e-' + str(n), now=now)
            self.assertGreaterEqual(record['protected_until'] - now, RETENTION_SECONDS)
        self.assertEqual(ImagePins(self.path).snapshot(1), self.pins.snapshot(1))

    def test_immutable_exact_ordered_binding_even_after_expiry(self):
        self.public(images=['sc-one', 'sc-two'])
        before = self.pins.snapshot(1000)
        for images, kind in [(['sc-three'], 'packet'), (['sc-one'], 'packet'),
                             (['sc-two', 'sc-one'], 'packet'),
                             (['sc-one', 'sc-two'], 'go')]:
            with self.assertRaises(ValueError):
                self.public(images=images, kind=kind, now=10 * RETENTION_SECONDS)
        with self.assertRaises(ValueError):
            self.pins.pin_batch('event-1', ['sc-one', 'sc-two'], 1000)
        self.assertEqual(self.pins.snapshot(1000), before)

    def test_live_batch_survives_ttl_and_only_explicit_cleanup_closes(self):
        record = self.pins.pin_batch('batch-1', ['sc-one'], 10)
        self.assertTrue(record['live'])
        self.assertIsNone(record['protected_until'])
        far_future = 100 * RETENTION_SECONDS
        self.assertIn('sc-one', self.pins.protected(far_future))
        self.pins = ImagePins(self.path)
        self.assertTrue(self.pins.get('batch-1', far_future)['protected'])
        closed = self.pins.mark_batch_closed('batch-1', far_future)
        self.assertFalse(closed['live'])
        self.assertEqual(closed['reason'], 'batch_grace')
        self.assertIn('sc-one', self.pins.protected(far_future + RETENTION_SECONDS))
        self.assertNotIn('sc-one', self.pins.protected(far_future + RETENTION_SECONDS + 1))
        with self.assertRaises(ValueError):
            self.pins.pin_batch('batch-1', ['sc-one'], far_future + 1)

    def test_closure_clock_rollback_and_duplicate_closure_cannot_shorten(self):
        self.pins.pin_batch('batch', ['sc-one'], 1000)
        closed = self.pins.mark_batch_closed('batch', 2)
        self.assertEqual(closed['closed_at'], 1000)
        self.assertGreaterEqual(closed['protected_until'], 1000 + RETENTION_SECONDS)
        revision = self.pins.revision
        self.assertEqual(self.pins.mark_batch_closed('batch', 1), closed)
        self.assertEqual(self.pins.revision, revision)
        extended = self.pins.mark_batch_closed('batch', 2000)
        self.assertGreaterEqual(extended['protected_until'], 2000 + RETENTION_SECONDS)
        self.assertEqual(extended['closed_at'], 1000)
        self.assertEqual(ImagePins(self.path).get('batch', 2000), extended)
        with self.assertRaises(ValueError):
            self.pins.mark_batch_closed('unknown', 2000)
        self.public()
        with self.assertRaises(ValueError):
            self.pins.mark_batch_closed('event-1', 2000)

    def test_independent_reasons_keep_image_retained(self):
        self.public()
        self.pins.pin_batch('batch', ['sc-one'], 1000)
        self.assertEqual(len(self.pins.protected(1000)['sc-one']), 2)
        reasons = self.pins.protected(10 * RETENTION_SECONDS)['sc-one']
        self.assertEqual([r['reason'] for r in reasons], ['live_batch'])

    def test_strict_names_lists_kinds_and_times(self):
        self.pins.snapshot(0)
        for bad in ('', '.', '../image', '/abs', 'a/b', 'a b', 'a\n', '-flag',
                    'a;rm', 'é', 'a' * 257, None, 12, True):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    self.public(ref=bad)
                with self.assertRaises(ValueError):
                    self.public(images=[bad])
        for images in ([], (), 'sc-one', ['sc-one', 'sc-one'], ['i'] * 4097):
            with self.assertRaises(ValueError):
                self.public(images=images)
        for now in (True, False, float('nan'), float('inf'), -float('inf'),
                    -1, '10', None, 10**500, 1e13):
            for operation in (lambda: self.public(now=now),
                              lambda: self.pins.pin_batch('batch', ['sc-one'], now),
                              lambda: self.pins.mark_batch_closed('batch', now),
                              lambda: self.pins.snapshot(now),
                              lambda: self.pins.get('event-1', now),
                              lambda: self.pins.protected(now)):
                with self.assertRaises(ValueError):
                    operation()
        for kind in ('batch', 'PACKET', '', None, True):
            with self.assertRaises(ValueError):
                self.public(kind=kind)
        self.assertEqual(self.pins.revision, 0)

    def test_malformed_load_is_sticky_and_never_becomes_empty_success(self):
        self.path.parent.mkdir()
        self.path.write_text('{invalid')
        with self.assertRaises(ImagePinsUnavailable):
            self.pins.snapshot(1000)
        self.path.write_text('{"version":1}')
        self.assert_poisoned()

    def test_strict_schema_and_record_invariants_on_load(self):
        self.public()
        valid = json.loads(self.path.read_text())
        variants = []
        for key, value in [('version', True), ('version', 2), ('epoch', ''),
                           ('revision', True), ('revision', -1), ('revision', 0),
                           ('records', [])]:
            obj = copy.deepcopy(valid)
            obj[key] = value
            variants.append(obj)
        for key, value in [('images', []), ('images', ['bad/image']), ('kind', 'oops'),
                           ('ref', 'other'), ('first_observed_at', True),
                           ('last_observed_at', 999), ('closed_at', 1000),
                           ('protected_until', 1001), ('protected_until', None),
                           ('protected_until', float('nan'))]:
            obj = copy.deepcopy(valid)
            obj['records']['event-1'][key] = value
            variants.append(obj)
        for obj in variants:
            with self.subTest(obj=obj):
                self.path.write_text(json.dumps(obj))
                with self.assertRaises(ImagePinsUnavailable):
                    ImagePins(self.path).snapshot(1000)
        self.path.write_text('{"version":1,"version":1}')
        with self.assertRaises(ImagePinsUnavailable):
            ImagePins(self.path).snapshot(1000)

    def test_malformed_batch_active_expiry_and_short_closed_grace(self):
        self.pins.pin_batch('batch', ['sc-one'], 1000)
        valid = json.loads(self.path.read_text())
        for closed, until in ((None, 999999), (1000, 1001), (True, 999999),
                              (999, 999999), (1000, None)):
            obj = copy.deepcopy(valid)
            obj['records']['batch'].update(closed_at=closed, protected_until=until)
            self.path.write_text(json.dumps(obj))
            with self.assertRaises(ImagePinsUnavailable):
                ImagePins(self.path).snapshot(1000)

    def test_fsync_and_replace_order_before_publication(self):
        self.public()
        events = []
        real_fsync, real_replace = os.fsync, os.replace

        def fsync(fd):
            events.append('fsync')
            self.assertEqual(self.pins._data['revision'], 1)
            return real_fsync(fd)

        def replace(src, dest):
            events.append('replace')
            self.assertEqual(self.pins._data['revision'], 1)
            return real_replace(src, dest)

        with patch('supercomputer.image_pins.os.fsync', side_effect=fsync), \
                patch('supercomputer.image_pins.os.replace', side_effect=replace):
            self.public(now=2000)
        self.assertEqual(events, ['fsync', 'replace', 'fsync'])
        self.assertEqual(self.pins.revision, 2)

    def test_uncertain_persistence_poison_before_and_after_replace(self):
        self.public()
        original = self.path.read_text()
        real_fsync = os.fsync
        for stage in ('serialize', 'file_fsync', 'replace', 'parent_fsync'):
            with self.subTest(stage=stage):
                self.path.write_text(original)
                self.pins = ImagePins(self.path)
                self.pins.snapshot(1000)
                calls = 0

                def fsync(fd):
                    nonlocal calls
                    calls += 1
                    if (stage == 'file_fsync' and calls == 1) or (stage == 'parent_fsync' and calls == 2):
                        raise OSError('simulated durability uncertainty')
                    return real_fsync(fd)

                target = ('supercomputer.image_pins.json.dump' if stage == 'serialize'
                          else 'supercomputer.image_pins.os.replace' if stage == 'replace'
                          else 'supercomputer.image_pins.os.fsync')
                effect = OSError('simulated failure') if stage in ('serialize', 'replace') else fsync
                with patch(target, side_effect=effect):
                    with self.assertRaises(ImagePinsUnavailable):
                        self.public(now=2000)
                self.assert_poisoned()
                # A new owner can read the actual disk, but the poisoned owner cannot.
                on_disk = ImagePins(self.path).get('event-1', 2000)
                expected = 2000 if stage == 'parent_fsync' else 1000
                self.assertEqual(on_disk['last_observed_at'], expected)

    def test_load_io_error_and_initial_write_error_fail_closed(self):
        with patch('pathlib.Path.open', side_effect=PermissionError('no access')):
            with self.assertRaises(ImagePinsUnavailable):
                self.pins.snapshot(0)
        self.assert_poisoned()
        self.pins = ImagePins(self.path)
        with patch('supercomputer.image_pins.os.replace', side_effect=OSError('no replace')):
            with self.assertRaises(ImagePinsUnavailable):
                self.pins.snapshot(0)
        self.assert_poisoned()

    def test_pure_retention_decision(self):
        self.assertTrue(retain_image(True, None, 10**9))
        self.assertTrue(retain_image(False, 20, 20))
        self.assertFalse(retain_image(False, 20, 21))
        self.assertFalse(retain_image(False, None, 20))


if __name__ == '__main__':
    unittest.main()
