"""Preparation does not mutate legacy journals; activation owns publication."""
import itertools
import json
import pathlib
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import resultfiles, resultjournal, scheduler
from supercomputer.rules import updates


class JournalHandoff(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.live = self.root / 'live'
        self.queue = self.live / 'queue'
        self.queue.mkdir(parents=True)
        self.patch = mock.patch.object(scheduler, 'DIR', self.live)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.path = self.queue / 'batch.rows'
        self.original = (json.dumps({'name': 'job', 'code': 0, 'result': {'bulk': 'x' * 100000}}) + '\n').encode()
        self.path.write_bytes(self.original)
        (self.queue / 'batch.json').write_text(json.dumps({'batch': 'batch', 'items': [{'name': 'job'}]}))
        self.store = resultfiles.Store(self.queue / 'batch.results')

    def candidate(self, blocked=False):
        resultjournal.prepare(self.path, self.store)
        rows, receipt = resultjournal.load_with_receipt(self.path, self.store)
        self.assertTrue(receipt['needs_compaction'])
        s = object.__new__(scheduler.Scheduler)
        s.lock = threading.RLock()
        s._request_lock = threading.RLock()
        s._handoff_deferred = (123, [])
        s.recovery_blocked = blocked
        s._journal_compact_pending = {'batch'}
        s._journal_compaction_errors = {}
        s.batches = {'batch': SimpleNamespace(rows=rows, cv=threading.Condition())}
        s.vms = {'worker': object()}
        s._worker = mock.Mock()
        return s

    def test_stage_prepares_before_publishing_and_pins_snapshot(self):
        source = self.root / 'install' / 'supercomputer'
        source.mkdir(parents=True)
        (source / '__init__.py').write_text('')
        (source / 'scheduler.py').write_text('# immutable test candidate\n')
        manifest = self.live / 'install.json'
        old = '{"source":"/old/immutable","code":"old"}'
        manifest.write_text(old)
        original_prepare = resultjournal.prepare_directory
        def prepare(queue):
            self.assertEqual(manifest.read_text(), old)
            self.assertEqual(self.path.read_bytes(), self.original)
            return original_prepare(queue)
        with mock.patch.object(scheduler, '__file__', str(source / 'scheduler.py')), \
             mock.patch.object(scheduler, 'prepare_snapshot', return_value={}), \
             mock.patch.object(resultjournal, 'prepare_directory', side_effect=prepare), \
             mock.patch.object(scheduler, 'Client', side_effect=ConnectionRefusedError):
            receipt = scheduler.stage_install()
        installed = json.loads(manifest.read_text())
        self.assertEqual(installed['source'], installed['snapshot'])
        self.assertNotEqual(installed['source'], str(source.parent))
        self.assertEqual(installed['prepared']['result_journals']['prepared'], 1)
        self.assertEqual(receipt['staged'], installed['code'])
        self.assertEqual(self.path.read_bytes(), self.original)

    def test_failed_preparation_never_publishes_candidate(self):
        old = '{"source":"/old/immutable","code":"old"}'
        (self.live / 'install.json').write_text(old)
        (self.queue / 'batch.json').write_text(json.dumps({'batch': 'batch', 'strict_once': True, 'items': []}))
        self.path.write_bytes(b'{bad}\n')
        with mock.patch.object(scheduler, 'prepare_snapshot', return_value={}), \
             self.assertRaisesRegex(RuntimeError, 'result-journal preparation failed'):
            scheduler.stage_install()
        self.assertEqual((self.live / 'install.json').read_text(), old)
        self.assertEqual(self.path.read_bytes(), b'{bad}\n')

    def test_live_parent_keeps_original_then_commit_compacts_once(self):
        s = self.candidate()
        with mock.patch.object(scheduler.boat, 'pid_alive', return_value=True):
            self.assertFalse(s._activate_handoff())
        self.assertEqual(self.path.read_bytes(), self.original)
        s._worker.assert_not_called()
        compact = resultjournal.compact
        def fenced(path, rows, **kwargs):
            self.assertIsNotNone(s._handoff_deferred)
            s._worker.assert_not_called()
            return compact(path, rows, **kwargs)
        with mock.patch.object(scheduler.boat, 'pid_alive', return_value=False), \
             mock.patch.object(resultjournal, 'compact', side_effect=fenced) as publish:
            self.assertTrue(s._activate_handoff())
            self.assertTrue(s._activate_handoff())
            self.assertEqual(publish.call_count, 1)
        self.assertLess(self.path.stat().st_size, 2000)
        s._worker.assert_called_once_with(s.vms['worker'])
        rows, receipt = resultjournal.load_with_receipt(self.path, self.store)
        self.assertEqual(rows, s.batches['batch'].rows)
        self.assertFalse(receipt['needs_compaction'])

    def test_current_tail_rows_are_not_lost_on_publication(self):
        s = self.candidate()
        tail = {'name': 'late', 'code': 0, 'seconds': 1}
        with self.path.open('ab') as f:
            f.write((json.dumps(tail) + '\n').encode())
        s.batches['batch'].rows.append(tail)
        with mock.patch.object(scheduler.boat, 'pid_alive', return_value=False):
            self.assertTrue(s._activate_handoff())
        self.assertEqual(resultjournal.load(self.path, self.store)[-1], tail)

    def test_uncertain_recovery_never_rewrites(self):
        s = self.candidate(blocked=True)
        with mock.patch.object(scheduler.boat, 'pid_alive', return_value=False):
            self.assertTrue(s._activate_handoff())
        self.assertEqual(self.path.read_bytes(), self.original)
        s._worker.assert_not_called()

    def test_failed_atomic_publication_keeps_original_and_diagnostic(self):
        s = self.candidate()
        with mock.patch.object(scheduler.boat, 'pid_alive', return_value=False), \
             mock.patch.object(resultjournal, 'compact', side_effect=OSError('disk unavailable')):
            self.assertTrue(s._activate_handoff())
        self.assertEqual(self.path.read_bytes(), self.original)
        self.assertEqual(s._journal_compaction_errors['batch'], 'disk unavailable')
        self.assertIn('batch', s._journal_compact_pending)

    def test_production_guard_truth_table(self):
        for alive, guarded, blocked in itertools.product((False, True), repeat=3):
            self.assertEqual(updates.journal_compaction_allowed(alive, guarded, blocked),
                             not blocked and (not guarded or not alive))


if __name__ == '__main__':
    unittest.main()
