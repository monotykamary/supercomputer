"""Closed disk admissions are archived before reload or offering a handoff."""
import json
import os
import time
import pathlib
import sys
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from test_wiring import batch, sched, vm
from supercomputer import canary, scheduler as S


def message(b):
    return {"batch": b.id, "klass": b.klass, "image": b.image, "tree": b.tree,
            "ctx_b64": b.ctx_b64, "items": [i.spec for i in b.items.values()]}


class QuarantineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = pathlib.Path(self.tmp.name)
        self.q = self.d / 'queue'
        self.q.mkdir()
        self.ledger = self.d / 'ledger.jsonl'
        self.ledger.write_text('')
        self.patches = ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(patch.object(S, 'DIR', self.d))
        self.patches.enter_context(patch.object(S, 'BLOBS', self.d / 'blobs'))
        self.patches.enter_context(patch.object(S.boat, 'LEDGER', self.ledger))
        self.s = sched()
        self.s.code = 'fixture'
        self.s._take_pool, self.s._reattach = Mock(), Mock()

    def admission(self, bid='closed', raw=None):
        b = batch(self.s, bid=bid)
        data = json.dumps(message(b)) if raw is None else raw
        (self.q / f'{bid}.json').write_text(data)
        return b, data

    def close_receipt(self, bid='closed'):
        with self.ledger.open('a') as f:
            f.write(json.dumps({'batch': bid, 'scheduled': True, 'wall_s': 3, 'slot_s': 0}) + '\n')

    def archived(self, name):
        return list((self.d / 'quarantine').rglob(name))

    def test_reload_archives_resurrected_closed_record_and_all_evidence(self):
        b, _ = self.admission()
        original = json.dumps({**message(b), "cache_generation": "a" * 32,
                               "cache_completed_generation": "a" * 32})
        (self.q / "closed.json").write_text(original)
        self.s.batches.clear()
        self.close_receipt()
        (self.q / 'closed.rows').write_text('result bytes\n')
        (self.q / 'closed.placed').write_text('placement bytes\n')
        (self.q / 'closed.cancel').write_text('cancellation bytes\n')
        logs = self.q / 'closed.logs' / 'worker'
        logs.mkdir(parents=True)
        (logs / 'out').write_text('output evidence')
        self.s.reload()
        self.assertNotIn(b.id, self.s.batches)
        self.assertEqual(list(self.q.iterdir()), [])
        self.assertEqual(self.archived('closed.json')[0].read_text(), original)
        self.assertEqual(self.archived('closed.rows')[0].read_text(), 'result bytes\n')
        self.assertEqual(self.archived('closed.placed')[0].read_text(), 'placement bytes\n')
        self.assertEqual(self.archived('closed.cancel')[0].read_text(), 'cancellation bytes\n')
        self.assertEqual(self.archived('out')[0].read_text(), 'output evidence')
        self.assertRegex(self.archived('closed.json')[0].parent.name, r'closed-\d{8}T\d{6}')
        self.s._reattach.assert_not_called()

    def test_parent_quarantines_before_snapshot_and_candidate_agrees(self):
        self.admission('live')
        closed, original = self.admission()
        self.s.batches.pop(closed.id)
        self.close_receipt()
        snap = self.s._handoff_snapshot()
        self.assertFalse((self.q / 'closed.json').exists())
        self.assertEqual(snap['batches'], ['live'])
        candidate = sched()
        candidate.code, candidate.canary = 'next', canary.Canary(self.d)
        candidate._reattach = Mock()
        receipts = []
        def ack():
            receipts.append(candidate.canary.admissions('next', set(snap['batches']),
                set(candidate.batches), candidate._client_closed, candidate.recovery_errors)[0])
        candidate._take_pool = ack
        candidate.reload()
        self.assertTrue(receipts[0]['ready'])
        self.assertEqual(receipts[0]['extra'], [])
        self.assertEqual(set(candidate.batches), {'live'})
        self.assertEqual(self.archived('closed.json')[0].read_text(), original)

    def test_incomplete_or_false_close_receipt_leaves_unclassified_record_guarded(self):
        b, _ = self.admission('unclassified', raw='{')
        self.ledger.write_text('\n'.join(json.dumps(r) for r in (
            {'batch': b.id, 'scheduled': True},
            {'batch': b.id, 'scheduled': False, 'wall_s': 3, 'slot_s': 0},
        )) + '\n{"batch":')
        self.s.batches.clear()
        self.s.reload()
        self.assertTrue((self.q / f'{b.id}.json').exists())
        self.assertTrue(self.s.recovery_blocked)
        self.assertIn(b.id, self.s.recovery_errors)
        self.assertEqual(self.archived(f'{b.id}.json'), [])

    def test_completion_marker_alone_is_not_close_authority(self):
        b, _ = self.admission('done-but-open')
        (self.q / f'{b.id}.json').write_text(json.dumps({**message(b),
            'cache_generation': 'a' * 32, 'cache_completed_generation': 'a' * 32}))
        (self.q / f'{b.id}.rows').write_text(json.dumps({'name': 'i0', 'code': 0}) + '\n')
        self.s.batches.clear()
        self.s.reload()
        self.assertIn(b.id, self.s.batches)
        self.assertEqual(self.s.batches[b.id].left(), 0)
        self.assertTrue((self.q / f'{b.id}.json').exists())
        self.assertEqual(self.archived(f'{b.id}.json'), [])

    def test_partial_archive_is_recoverable_without_overwriting_any_evidence(self):
        self.admission()
        self.s.batches.clear()
        self.close_receipt()
        (self.q / 'closed.rows').write_text('original result\n')
        replace = S.os.replace
        def fail_journal(src, dest):
            if pathlib.Path(src).suffix == '.rows':
                raise OSError('journal move failed')
            return replace(src, dest)
        with patch.object(S.os, 'replace', side_effect=fail_journal):
            with self.assertRaises(OSError):
                self.s.reload()
        self.assertEqual(len(self.archived('closed.json')), 1)
        self.assertTrue((self.q / 'closed.rows').exists())
        self.s.reload()
        self.assertNotIn('closed', self.s.batches)
        self.assertEqual(self.archived('closed.rows')[0].read_text(), 'original result\n')
        self.assertFalse((self.q / 'closed.rows').exists())
        self.assertEqual(len(self.archived('closed.json')), 1)

    def test_malformed_closed_record_is_preserved_not_parsed_or_replayed(self):
        self.admission(raw='{malformed')
        self.s.batches.clear()
        self.close_receipt()
        self.s.reload()
        self.assertNotIn('closed', self.s.batches)
        self.assertNotIn('closed', self.s.recovery_errors)
        self.assertEqual(self.archived('closed.json')[0].read_text(), '{malformed')

    def test_repeated_quarantine_never_overwrites_prior_evidence(self):
        self.close_receipt()
        for raw in ('first version', 'second version'):
            self.admission(raw=raw)
            self.s.batches.clear()
            self.s.reload()
        self.assertEqual(sorted(p.read_text() for p in self.archived('closed.json')),
                         ['first version', 'second version'])
        self.s.reload()
        self.assertEqual(len(self.archived('closed.json')), 2)

    def test_archive_failure_aborts_snapshot_without_dropping_reservation(self):
        b, _ = self.admission()
        b.closed = True
        b.items["i0"].state = "done"
        self.close_receipt()
        with patch.object(S.os, 'replace', side_effect=OSError('archive unavailable')):
            with self.assertRaises(OSError):
                self.s._handoff_snapshot()
        self.assertIn('closed', self.s.batches)
        self.assertTrue((self.q / 'closed.json').exists())

    def test_a_close_receipt_cannot_drop_a_held_running_identity(self):
        b, _ = self.admission()
        self.close_receipt()
        v = vm(self.s, 'held')
        i = b.items['i0']
        i.vm, i.state, i.run_name = v, 'running', 'i0'
        v.running[(b.id, i.run_name)] = i
        with self.assertRaisesRegex(RuntimeError, 'held|running'):
            self.s._handoff_snapshot()
        self.assertIn('closed', self.s.batches)
        self.assertTrue((self.q / 'closed.json').exists())
        self.assertIs(v.running[(b.id, 'i0')], i)

    def test_incoming_handoff_identity_is_not_quarantined(self):
        self.admission()
        self.s.batches.clear()
        self.close_receipt()
        (self.d / 'handoff.json').write_text(json.dumps({
            'from_pid': os.getpid() + 100000, 't': time.time(),
            'vms': [{'id': 'held', 'items': [{'batch': 'closed', 'name': 'i0'}]}],
        }))
        with patch.object(S.boat, 'pid_alive', return_value=True):
            with self.assertRaisesRegex(RuntimeError, 'running|held'):
                self.s.reload()
        self.assertTrue((self.q / 'closed.json').exists())
        self.assertEqual(self.archived('closed.json'), [])

    def test_closed_id_is_not_readmitted_after_quarantine(self):
        b, _ = self.admission()
        self.s.batches.clear()
        self.close_receipt()
        self.s.reload()
        result = self.s.submit(message(b))
        self.assertFalse(result['ok'])
        self.assertIn('closed', result['error'])
        self.assertFalse((self.q / 'closed.json').exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
