"""Cold tiering: closed batches' output files into a snapshot and back, byte for byte; fail closed otherwise.

A local fake stands in for boat (VM roots and snapshots are directories); the archive is the real tar | zstd
stream and the remote verifier is the real script. No provider is touched.
"""
import copy
import errno
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat, config, history, image_pins, resultfiles, scheduler as S, tiering
from supercomputer.rules import tiering as R

HAVE_ZSTD = shutil.which('zstd') is not None


class FakeRemote:
    """VMs and snapshots as directories; the same stream/verify contract as BoatRemote."""

    def __init__(self, root):
        self.root = pathlib.Path(root)
        self.snapshots = {}
        self.vms = {}
        self.deleted = []
        self.starts = 0

    def _at(self, vm, path):
        return self.vms[vm] / path.lstrip('/')

    def new_vm(self, snapshot=None, ttl=0):
        if snapshot is not None and snapshot not in self.snapshots:
            raise tiering.Unavailable(f'tier: snapshot {snapshot} is missing')
        self.starts += 1
        vm = f'vm{self.starts}'
        self.vms[vm] = self.root / vm
        if snapshot is None:
            self.vms[vm].mkdir(parents=True)
        else:
            shutil.copytree(self.snapshots[snapshot], self.vms[vm])
        return vm

    def mkdir(self, vm, directory):
        self._at(vm, directory).mkdir(parents=True, exist_ok=True)

    def writer(self, vm, path):
        target = self._at(vm, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        return subprocess.Popen(['sh', '-c', 'cat > "$1"', 'sh', str(target)], stdin=subprocess.PIPE,
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

    def reader(self, vm, path):
        return subprocess.Popen(['cat', str(self._at(vm, path))], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def put(self, vm, path, data):
        target = self._at(vm, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    def verify(self, vm, path, want):
        want_file = self._at(vm, '/tmp/want.json')
        want_file.parent.mkdir(parents=True, exist_ok=True)
        want_file.write_text(json.dumps(want))
        out = subprocess.run([sys.executable, '-I', '-c', tiering.VERIFY, str(self._at(vm, path)), str(want_file)],
                             capture_output=True, text=True, check=True).stdout
        return json.loads(out.strip().splitlines()[-1])

    def snapshot(self, vm, name):
        shutil.copytree(self.vms[vm], self.root / ('snap-' + name))
        self.snapshots[name] = self.root / ('snap-' + name)

    def delete(self, vm):
        self.deleted.append(vm)
        shutil.rmtree(self.vms.pop(vm), ignore_errors=True)


def ref(store, data, fmt='file'):
    r = store._put([data], format=fmt)
    return r


@unittest.skipUnless(HAVE_ZSTD, 'zstd is required for the archive stream')
class Tiering(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.q = pathlib.Path(self.tmp.name) / 'queue'
        self.q.mkdir()
        self.remote = FakeRemote(pathlib.Path(self.tmp.name) / 'boat')
        self.bid = '20261117-000000-tiered'
        self.snapshot = None
        self.receipt = None
        self.t0 = time.time() - 30 * 3600  # closed 30 h ago: inside the 48 h history window

    def make_closed(self, bid=None, klass=5, strict=False, closed_at=None):
        """A closed batch: one good row with two output files, one failed row with its own file."""
        bid = bid or self.bid
        closed_at = self.t0 if closed_at is None else self.t0 + closed_at
        store = resultfiles.Store(self.q / f'{bid}.results')
        good = [os.urandom(4096) * 8, b'second output\n' * 1000]
        bad = b'traceback of the failed row\n'
        files = [ref(store, data) for data in good]
        manifest = store.put_json([{**f, 'path': f'out/{i}.bin'} for i, f in enumerate(files)])
        bad_file = ref(store, bad)
        bad_manifest = store.put_json([{**bad_file, 'path': 'err.txt'}])
        rows = [{'name': 'ok', 'code': 0, 'output_files_ref': manifest},
                {'name': 'broken', 'code': 1, 'output_files_ref': bad_manifest}]
        (self.q / f'{bid}.json').write_text(json.dumps({'batch': bid, 'klass': klass, 'strict_once': strict}))
        placement = dict(name='ok', run_name='ok', identity='nonce', vm='vm', boot_id='boot', attempt=1)
        self.snapshot = dict(ok=True, batch=bid, strict_once=strict, max_attempts=1, placements=[placement],
                             items=[{'name': 'ok', 'state': 'done'}, {'name': 'broken', 'state': 'done'}], rows=rows)
        self.receipt = dict(ok=True, batch=bid, complete=True, cleanup_complete=True, strict_once=strict,
                            max_attempts=1, admission_closed=True, journals_retained=True, pending=[], unlaunched=['broken'],
                            attempts=[dict(name='ok', run_name='ok', launch_identity='nonce', vm='vm', boot_id='boot',
                                           attempt=1, cleanup_complete=True)])
        info = dict(batch=bid, generation='gen-' + bid, items=2, rows=2, complete=True, strict_once=strict)
        with patch.object(history.time, 'time', return_value=closed_at):
            history.capture(self.q, bid, info, self.snapshot, self.receipt, 48)
        return {'good': {f['sha256']: d for f, d in zip(files, good)}, 'bad': bad_file['sha256'],
                'manifest': manifest['sha256'], 'bad_manifest': bad_manifest['sha256'], 'files': files}

    def payload_path(self, sha, bid=None):
        return self.q / f'{bid or self.bid}.results' / f'{sha}.payload'

    def tier(self, now=None, **kw):
        now = time.time() if now is None else now
        chosen = tiering.select(self.q, now, min_age_s=kw.get('min_age_s', 0), max_bytes=10 ** 12,
                                held=kw.get('held', frozenset()), reading=kw.get('reading', frozenset()))
        if not chosen:
            return chosen, {}
        archives = tiering.push(self.remote, self.q, 'sc-tier-test', chosen, now)
        for c in chosen:
            tiering.record(self.q, c['batch'], archives[c['batch']])
            tiering.drop_local(self.q, c['batch'], archives[c['batch']]['payloads'])
        return chosen, archives

    def test_only_successful_output_files_go_cold_and_metadata_stays(self):
        made = self.make_closed()
        audit = history.read(self.q, self.bid)['audit']
        before = {name: history.payload(self.q, self.bid, name) for name in ('snapshot.json', 'close.json', 'rows.json')}
        chosen, archives = self.tier()
        self.assertEqual(set(chosen[0]['payloads']), set(made['good']))
        for sha in made['good']:
            self.assertFalse(self.payload_path(sha).exists())
        # Failure receipts, manifests and every audit record stay local and readable as before.
        for sha in (made['bad'], made['manifest'], made['bad_manifest'], *(r['sha256'] for r in audit.values())):
            self.assertTrue(self.payload_path(sha).exists(), sha)
        for name, value in before.items():
            self.assertEqual(history.payload(self.q, self.bid, name), value)
        reference = tiering.load(self.q, self.bid)
        archive = reference['archives'][0]
        self.assertEqual(archive['snapshot'], 'sc-tier-test')
        self.assertEqual(archive['entries'], 2)
        self.assertEqual(set(archive['payloads']), set(made['good']))
        self.assertEqual(self.remote.deleted, ['vm1'])  # the archive VM is deleted after the snapshot

    def test_tier_then_hydrate_round_trips_byte_identical(self):
        made = self.make_closed()
        self.tier()
        store = resultfiles.Store(self.q / f'{self.bid}.results')
        for f in made['files']:
            with self.assertRaises(FileNotFoundError):
                store.read(f, 0, 1)
        receipt = tiering.hydrate(self.remote, self.q, self.bid)
        self.assertEqual(receipt['installed'], 2)
        for f in made['files']:
            with store.open(f) as source:
                self.assertEqual(source.read(), made['good'][f['sha256']])
        self.assertEqual(tiering.hydrate(self.remote, self.q, self.bid)['installed'], 0)  # nothing left to restore

    def test_tampered_or_missing_snapshot_fails_closed(self):
        made = self.make_closed()
        _, archives = self.tier()
        archive = archives[self.bid]
        tar = self.remote.snapshots['sc-tier-test'] / archive['path'].lstrip('/')
        data = bytearray(tar.read_bytes())
        data[len(data) // 2] ^= 0xFF
        tar.write_bytes(bytes(data))
        with self.assertRaises(tiering.Unavailable):
            tiering.hydrate(self.remote, self.q, self.bid)
        for sha in made['good']:
            self.assertFalse(self.payload_path(sha).exists())  # never a partial or substituted file
        self.assertEqual(list((self.q / '.tier-work').glob('hydrate-*')), [])
        shutil.rmtree(self.remote.snapshots.pop('sc-tier-test'))
        with self.assertRaises(tiering.Unavailable):
            tiering.hydrate(self.remote, self.q, self.bid)

    def test_a_reference_naming_other_bytes_fails_closed(self):
        made = self.make_closed()
        self.tier()
        path = tiering.reference_path(self.q, self.bid)
        reference = json.loads(path.read_text())
        reference['archives'][0]['sha256'] = '0' * 64
        path.write_text(json.dumps(reference))
        with self.assertRaises(tiering.Unavailable):
            tiering.hydrate(self.remote, self.q, self.bid)
        path.write_text('{"protocol": 1}')
        with self.assertRaises(ValueError):
            tiering.load(self.q, self.bid)
        self.assertTrue(made)

    def test_open_held_read_and_young_batches_are_not_tiered(self):
        self.make_closed()
        (self.q / f'{self.bid}.json').write_text('{}')  # reopened: an open admission is never tiered
        now = self.t0 + 100
        self.assertEqual(tiering.select(self.q, now, min_age_s=0, max_bytes=10 ** 12), [])
        (self.q / f'{self.bid}.json').unlink()
        self.assertEqual(tiering.select(self.q, now, min_age_s=0, max_bytes=10 ** 12, held={self.bid}), [])
        self.assertEqual(tiering.select(self.q, now, min_age_s=0, max_bytes=10 ** 12, reading={self.bid}), [])
        self.assertEqual(tiering.select(self.q, now, min_age_s=3600, max_bytes=10 ** 12), [])
        self.assertEqual(len(tiering.select(self.q, self.t0 + 3600, min_age_s=3600, max_bytes=10 ** 12)), 1)
        self.assertEqual(tiering.select(self.q, self.t0 + 49 * 3600, min_age_s=0, max_bytes=10 ** 12), [])  # expired

    def test_treadmill_batches_go_first_then_oldest(self):
        self.make_closed('20261117-000000-submit', klass=3, closed_at=50.0)
        self.make_closed('20261117-000000-filler', klass=5, closed_at=90.0)
        self.make_closed('20261117-000000-sweepy', klass=4, closed_at=80.0)
        now = self.t0 + 200
        order = [c['batch'] for c in tiering.select(self.q, now, min_age_s=0, max_bytes=10 ** 12)]
        self.assertEqual(order, ['20261117-000000-sweepy', '20261117-000000-filler', '20261117-000000-submit'])
        one = sum(tiering.select(self.q, now, min_age_s=0, max_bytes=10 ** 12)[0]['payloads'].values())
        self.assertEqual(len(tiering.select(self.q, now, min_age_s=0, max_bytes=one)), 1)

    def test_reference_expires_with_closed_history(self):
        self.make_closed()
        self.tier()
        self.assertTrue(tiering.reference_path(self.q, self.bid).exists())
        self.assertTrue(history.collect(self.q, self.bid, now=self.t0 + 48 * 3600))
        self.assertEqual(sorted(p.name for p in self.q.iterdir() if not p.name.startswith('.')), [self.bid + '.retired'])

    def test_strict_recover_close_still_validates_on_a_tiered_batch(self):
        from test_strict_once_client import session
        self.make_closed(strict=True)
        self.tier()
        live = pathlib.Path(self.tmp.name) / 'live'
        with patch.object(config, 'LIVE', live):
            s = session()
            s._strict_items = {self.bid: {'ok', 'broken'}}
            s._strict_placements = {}
            s._strict_attempted = {self.bid}
            s.submitted, s.bids = True, [self.bid]
            s._custody_event(self.bid, 'admission_attempt', items=['ok', 'broken'])
            s._remember_placement(self.bid, self.snapshot['placements'][0])
            meta = history.read(self.q, self.bid)

            def reply(path, msg, **kw):  # the controller's read path for retained history
                if path == '/close-receipt':
                    return history.payload(self.q, self.bid, 'close.json', meta)
                if path == '/snapshot':
                    return history.payload(self.q, self.bid, 'snapshot.json', meta)
                raise AssertionError('unexpected request ' + path)
            s.client.req.side_effect = reply
            report = s.recover_close(self.bid)
        self.assertTrue(report['cleanup_complete'])

    def test_scheduler_result_read_hydrates_then_serves(self):
        made = self.make_closed()
        self.tier()
        sched = S.Scheduler.__new__(S.Scheduler)
        sched.batches, sched.wake = {}, threading.Event()
        f = made['files'][0]
        with patch.object(S, 'DIR', self.q.parent), patch.object(tiering, 'BoatRemote', lambda: self.remote):
            with self.assertRaises(tiering.Hydrating):
                sched.result_part(self.bid, f, 0, 64)
            self.assertIn(self.bid, sched._tier_state()['hydrate'])
            sched._tier_hydrate(self.bid)
            self.assertEqual(sched.result_part(self.bid, f, 0, 64), made['good'][f['sha256']][:64])
            # A tampered archive: the batch is unavailable (409), not retried forever.
            self.tier_again_tampered(made)
            sched.alert = lambda msg: None
            with self.assertRaises(tiering.Hydrating):
                sched.result_part(self.bid, f, 0, 64)
            sched._tier_hydrate(self.bid)
            with self.assertRaises(tiering.Unavailable):
                sched.result_part(self.bid, f, 0, 64)
            self.assertFalse(self.payload_path(f['sha256']).exists())

    def tier_again_tampered(self, made):
        for sha in made['good']:
            self.payload_path(sha).unlink()
        tar = next(self.remote.snapshots['sc-tier-test'].rglob('*.tar.zst'))
        tar.write_bytes(tar.read_bytes()[:-7] + b'garbage')

    def test_policy_thresholds_must_rise(self):
        with patch.object(S, 'POLICY', self.q / 'policy.json'):
            self.assertEqual(S.write_policy({'tier_disk_pct': 75})['tier_disk_pct'], 75)
            for bad in ({'tier_disk_pct': 95}, {'disk_refuse_pct': 101}, {'tier_age_hours': -1}, {'disk_alert_pct': 'nan'}):
                with self.subTest(bad=bad), self.assertRaises(ValueError):
                    S.write_policy(bad)


class DiskGuard(unittest.TestCase):
    def test_rules(self):
        self.assertFalse(R.admission_refused(True, 89.9, 90, 97))
        self.assertTrue(R.admission_refused(True, 90, 90, 97))
        self.assertFalse(R.admission_refused(False, 96.9, 90, 97))
        self.assertTrue(R.admission_refused(False, 97, 90, 97))
        self.assertEqual(R.tier_mode(60, 70, 80, False), 0)
        self.assertEqual(R.tier_mode(60, 70, 80, True), 1)
        self.assertEqual(R.tier_mode(75, 70, 80, False), 1)
        self.assertEqual(R.tier_mode(85, 70, 80, False), 2)
        self.assertTrue(R.tier_allowed(True, False, False, False))
        self.assertFalse(R.tier_allowed(True, True, False, False))

    def refusal(self, used, klass):
        sched = S.Scheduler.__new__(S.Scheduler)
        total = 1000
        usage = shutil._ntuple_diskusage(total, int(total * used / 100), total - int(total * used / 100))
        with tempfile.TemporaryDirectory() as tmp, patch.object(S, 'POLICY', pathlib.Path(tmp) / 'policy.json'), \
                patch('shutil.disk_usage', return_value=usage):
            return sched._disk_refusal(klass)

    def test_filler_is_refused_first_then_everything(self):
        self.assertIsNone(self.refusal(85, 5))
        self.assertIn('sweep/filler', self.refusal(92, 5))
        self.assertIn('sweep/filler', self.refusal(92, 4))
        self.assertIsNone(self.refusal(92, 3))
        self.assertIn('every new batch', self.refusal(98, 2))

    def test_health_reports_the_controller_disk(self):
        from supercomputer import health
        h = health.Health.__new__(health.Health)
        h.lock = threading.Lock()
        h.record_ctl_disk(100.0, 91.0, 85.0, {'free_gb': 13})
        self.assertFalse(h.ctl_disk_status(101.0)['ok'])
        h.record_ctl_disk(100.0, 60.0, 85.0, {})
        self.assertTrue(h.ctl_disk_status(101.0)['ok'])
        self.assertIsNone(h.ctl_disk_status(10000.0)['value'])  # stale is unknown, not a reading


class ClientRetry(unittest.TestCase):
    def test_tiered_read_is_retried_then_returns(self):
        c = S.Client.__new__(S.Client)
        answers = [S.TierHydrating('503 tiered result hydrating'), S.TierHydrating('again'), b'bytes']
        def once(*a):
            x = answers.pop(0)
            if isinstance(x, Exception):
                raise x
            return x
        with patch.object(c, '_req_once', side_effect=once, create=True), patch.object(S.time, 'sleep') as sleep:
            self.assertEqual(c.req('/results', {}, raw=True), b'bytes')
        self.assertEqual(sleep.call_count, 2)
        self.assertTrue(S._tier_busy(503, b'{"error":"tiered result hydrating; retry the same endpoint"}'))
        self.assertFalse(S._tier_busy(409, b'{"error":"tiered result unavailable"}'))


class ImagePinRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = pathlib.Path(self.tmp.name) / 'image-pins.json'
        self.pins = image_pins.ImagePins(self.path)
        self.pins.pin_batch('batch:one', ['img-a'], 100.0)

    def fill_disk(self):
        real = os.replace
        def full(*a, **k):
            raise OSError(errno.ENOSPC, 'No space left on device')
        with patch.object(image_pins.os, 'replace', side_effect=full):
            with self.assertRaises(image_pins.ImagePinsUnavailable):
                self.pins.pin_batch('batch:two', ['img-b'], 101.0)
        self.assertIs(image_pins.os.replace, real)

    def test_disk_full_poison_recovers_once_space_returns(self):
        self.fill_disk()
        tight = shutil._ntuple_diskusage(10 ** 9, 10 ** 9 - 1024, 1024)
        with patch('shutil.disk_usage', return_value=tight):
            with self.assertRaises(image_pins.ImagePinsUnavailable):
                self.pins.get('batch:one', 102.0)
        self.pins._recover_at = 0.0
        record = self.pins.get('batch:one', 102.0)
        self.assertEqual(record['images'], ['img-a'])
        self.assertIsNone(self.pins.get('batch:two', 102.0))  # the failed write did not happen
        self.assertEqual(self.pins.pin_batch('batch:two', ['img-b'], 103.0)['images'], ['img-b'])

    def test_recovery_refuses_regressed_or_missing_state(self):
        old = self.path.read_text()
        self.pins.pin_batch('batch:three', ['img-c'], 101.0)
        self.fill_disk()
        self.path.write_text(old)  # an older revision is never accepted
        self.pins._recover_at = 0.0
        with self.assertRaises(image_pins.ImagePinsUnavailable):
            self.pins.get('batch:one', 102.0)
        other = image_pins.ImagePins(pathlib.Path(self.tmp.name) / 'other.json')
        other.pin_batch('batch:x', ['img-x'], 100.0)
        with patch.object(image_pins.os, 'replace', side_effect=OSError(errno.ENOSPC, 'full')):
            with self.assertRaises(image_pins.ImagePinsUnavailable):
                other.pin_batch('batch:y', ['img-y'], 101.0)
        other.path.unlink()
        other._recover_at = 0.0
        with self.assertRaises(image_pins.ImagePinsUnavailable):
            other.get('batch:x', 102.0)

    def test_other_poisons_do_not_self_recover(self):
        self.path.write_text('{"version": 1}')
        fresh = image_pins.ImagePins(self.path)
        with self.assertRaises(image_pins.ImagePinsUnavailable):
            fresh.get('batch:one', 102.0)
        self.path.write_text(json.dumps({'version': 1, 'epoch': '0' * 32, 'revision': 0, 'records': {}}))
        fresh._recover_at = 0.0
        with self.assertRaises(image_pins.ImagePinsUnavailable):
            fresh.get('batch:one', 102.0)


if __name__ == '__main__':
    unittest.main()
