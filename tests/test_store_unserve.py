"""Native-filesystem/fork regressions; all Boat operations are mocked."""
import json
import multiprocessing
import os
import pathlib
import queue
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import store


def client(root, start, entered, second, calls, errors):
    store.STORE = pathlib.Path(root)

    def delete(*args, **kwargs):
        calls.put(args)
        if entered.is_set():
            second.set()
        else:
            entered.set()
            # Old clients both reach delete; a locked client cannot get here
            # until the first has removed the record. Bounded in both cases.
            second.wait(1)
        return '{}'

    try:
        with mock.patch.object(store.boat, 'boat', side_effect=delete):
            start.wait(5)
            store.unserve(all_=True)
        errors.put(None)
    except BaseException as exc:
        errors.put(repr(exc))


class UnserveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.patch = mock.patch.object(store, 'STORE', self.root)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.serving = self.root / 'serving'
        self.serving.mkdir()

    def record(self, vm='vm-one', name='wanted', until=None):
        path = self.serving / (vm + '.json')
        path.write_text(json.dumps(dict(vm=vm, name=name, until=time.time() + 3600 if until is None else until)))
        return path

    def test_concurrent_fork_clients_delete_once(self):
        self.record()
        ctx = multiprocessing.get_context('fork')
        start, entered, second = ctx.Event(), ctx.Event(), ctx.Event()
        calls, errors = ctx.Queue(), ctx.Queue()
        children = [ctx.Process(target=client, args=(str(self.root), start, entered, second, calls, errors)) for _ in range(2)]
        try:
            for child in children:
                child.start()
            start.set()
            for child in children:
                child.join(10)
                self.assertFalse(child.is_alive(), 'client hung')
                self.assertEqual(child.exitcode, 0)
            results = [errors.get(timeout=2) for _ in children]
            self.assertEqual(results, [None, None], results)
            self.assertEqual(calls.get(timeout=2)[1], 'vm-one')
            with self.assertRaises(queue.Empty):
                calls.get(timeout=0.1)
            self.assertEqual(list(self.serving.glob('*.json')), [])
        finally:
            for child in children:
                if child.is_alive():
                    child.terminate()
                child.join(5)
            calls.close()
            errors.close()

    def test_repeat_is_idempotent(self):
        self.record()
        with mock.patch.object(store.boat, 'boat', return_value='{}') as delete:
            store.unserve(all_=True)
            store.unserve(all_=True)
        self.assertEqual(delete.call_count, 1)

    def test_failure_retains_record_for_retry(self):
        path = self.record()
        original = path.read_bytes()

        def delete(*args, **kwargs):
            if kwargs.get('check', True):
                raise RuntimeError('delete failed')
            return ''  # boat(check=False) hides the nonzero exit

        with mock.patch.object(store.boat, 'boat', side_effect=delete):
            with self.assertRaisesRegex(RuntimeError, 'delete failed'):
                store.unserve(all_=True)
        self.assertEqual(path.read_bytes(), original)
        with mock.patch.object(store.boat, 'boat', return_value='{}'):
            store.unserve(all_=True)
        self.assertFalse(path.exists())

    def test_already_deleted_vm_clears_stale_record(self):
        path = self.record()
        # Exact installed CLI output from a local HTTP DELETE/404 fixture.
        envelope = dict(code='Sandbox not found', error='Sandbox not found (404)', event='error', status=404)
        result = subprocess.CompletedProcess([], 1, stdout=json.dumps(envelope) + '\n', stderr='')
        with mock.patch.object(store.boat.subprocess, 'run', return_value=result) as delete:
            store.unserve(all_=True)
            store.unserve(all_=True)
        self.assertFalse(path.exists())
        self.assertEqual(delete.call_count, 1)

    def test_other_delete_errors_retain_record(self):
        envelopes = [
            dict(code='unauthorized', error='Unauthorized (401)', event='error', status=401),
            dict(code='forbidden', error='Forbidden (403)', event='error', status=403),
            dict(code='Not found', error='Not found (404)', event='error', status=404),
            dict(code='Sandbox not found', error='Sandbox not found (500)', event='error', status=500),
            dict(code='Sandbox not found', error='Sandbox not found (404)', status=404),
        ]
        messages = ['boat delete failed (1): ' + json.dumps(e) for e in envelopes]
        messages += ['boat delete failed (1): network unavailable', 'boat delete failed (1): {"status":404,']
        for message in messages:
            with self.subTest(message=message):
                path = self.record()
                original = path.read_bytes()
                with mock.patch.object(store.boat, 'boat', side_effect=RuntimeError(message)):
                    with self.assertRaises(RuntimeError):
                        store.unserve(all_=True)
                self.assertEqual(path.read_bytes(), original)

    def test_selection(self):
        self.record('vm-named')
        self.record('vm-expired', 'other', 0)
        kept = self.record('vm-kept', 'other')
        with mock.patch.object(store.boat, 'boat', return_value='{}') as delete:
            store.unserve('wanted')
        self.assertEqual({c.args[1] for c in delete.call_args_list}, {'vm-named', 'vm-expired'})
        self.assertTrue(kept.exists())
        with mock.patch.object(store.boat, 'boat', return_value='{}') as delete:
            store.unserve()
            delete.assert_not_called()
            store.unserve(all_=True)
            self.assertEqual(delete.call_args.args[1], 'vm-kept')

    def test_missing_registry(self):
        self.serving.rmdir()
        with mock.patch.object(store.boat, 'boat') as delete:
            store.unserve(all_=True)
            delete.assert_not_called()

    def test_record_removed_by_old_client_before_read(self):
        path = self.record()
        original = pathlib.Path.read_text

        def read(p, *args, **kwargs):
            if p == path:
                p.unlink(missing_ok=True)
            return original(p, *args, **kwargs)

        with mock.patch.object(pathlib.Path, 'read_text', read), mock.patch.object(store.boat, 'boat') as delete:
            store.unserve(all_=True)
            delete.assert_not_called()

    def test_old_client_removes_record_during_delete(self):
        path = self.record()
        with mock.patch.object(store.boat, 'boat', side_effect=lambda *a, **k: path.unlink()):
            store.unserve(all_=True)
        self.assertFalse(path.exists())

    def test_replacement_record_survives_delete(self):
        path = self.record()
        replacement = dict(vm='vm-one', name='replacement', until=time.time() + 5000)
        with mock.patch.object(store.boat, 'boat', side_effect=lambda *a, **k: path.write_text(json.dumps(replacement))):
            store.unserve(all_=True)
        self.assertEqual(json.loads(path.read_text()), replacement)

    def test_lock_inode_persists_and_timeout_keeps_record(self):
        path = self.record()
        lock = path.with_suffix('.lock')
        with mock.patch.object(store.boat, 'boat', side_effect=subprocess.TimeoutExpired('boat', 300)):
            with self.assertRaises(subprocess.TimeoutExpired):
                store.unserve(all_=True)
        inode = lock.stat().st_ino
        self.assertTrue(path.exists())
        with mock.patch.object(store.boat, 'boat', return_value='{}'):
            store.unserve(all_=True)
        self.assertEqual(lock.stat().st_ino, inode)
        store._publish_serving(dict(vm='vm-one', name='new', until=0))
        self.assertEqual(lock.stat().st_ino, inode)

    def test_atomic_publication_and_failure_cleanup(self):
        path = self.record()
        old = path.read_bytes()
        record = dict(vm='vm-one', name='new', until=0)
        replace = os.replace

        def inspect(src, dst):
            self.assertEqual(path.read_bytes(), old)
            self.assertEqual(json.loads(pathlib.Path(src).read_text()), record)
            self.assertEqual(list(self.serving.glob('*.json')), [path])
            replace(src, dst)

        with mock.patch.object(store.os, 'replace', side_effect=inspect) as publish:
            store._publish_serving(record)
            publish.assert_called_once()
        self.assertEqual(json.loads(path.read_text()), record)
        with mock.patch.object(store.os, 'replace', side_effect=OSError('disk error')):
            with self.assertRaisesRegex(OSError, 'disk error'):
                store._publish_serving(dict(vm='vm-one', name='failed', until=0))
        self.assertEqual(json.loads(path.read_text()), record)
        self.assertEqual(list(self.serving.glob('*.tmp')), [])

    def test_serve_publishes_complete_record(self):
        with mock.patch.object(store, 'new_vm', return_value='vm-new'), \
                mock.patch.object(store.boat, 'exec_'), \
                mock.patch.object(store.boat, 'bj', return_value={'url': 'https://example.invalid/'}):
            store.serve('wanted', 2)
        record = json.loads((self.serving / 'vm-new.json').read_text())
        self.assertEqual(record['vm'], 'vm-new')
        self.assertEqual(record['name'], 'wanted')
        self.assertGreater(record['until'], time.time())
        self.assertTrue((self.serving / 'vm-new.lock').exists())

    def test_invalid_records_do_not_delete_unrelated_vm(self):
        (self.serving / 'vm-one.json').write_text(json.dumps(dict(vm='unrelated-vm', name='wanted', until=0)))
        (self.serving / 'partial.json').write_text('{')
        (self.serving / 'missing-fields.json').write_text('{}')
        for vm in ('self', 'current', ''):
            self.record(vm=vm)
        (self.serving / 'non-utf8.json').write_bytes(b'\xff')
        with mock.patch.object(store.boat, 'boat') as delete:
            store.unserve(all_=True)
            delete.assert_not_called()
        self.assertEqual(len(list(self.serving.glob('*.json'))), 7)


if __name__ == '__main__':
    unittest.main(verbosity=2)
