"""Result payloads survive recovery without an in-memory archive copy."""
import contextlib
import gc
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from supercomputer import statebackup as B

class ResultBackup(unittest.TestCase):
    def test_payload_pinned_with_admission_and_archived_from_file(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root); live=root/'live'; queue=live/'scheduler/queue'; queue.mkdir(parents=True)
            (queue/'b.json').write_text(json.dumps({'tree':{'id':'x','files':{}}}))
            (queue/'b.rows').write_text('{"name":"i0","row_ref":{}}\n')
            payload=queue/'b.results'/('a'*64+'.payload'); payload.parent.mkdir(); payload.write_bytes(b'not JSON\x00'*100000)
            (payload.parent/'.upload-uncommitted').write_bytes(b'partial')
            @contextlib.contextmanager
            def guard(): yield {'closed':{'b'}}
            archive=B.build(live,root/'treadmill',guard,file_backed=True)
            self.assertIsInstance(archive,B.FilePayload)
            with archive.open() as source,tarfile.open(fileobj=source,mode='r:gz') as tf:
                names=tf.getnames()
                expected='live/scheduler/queue/b.results/'+payload.name
                self.assertIn(expected,names)
                self.assertEqual(tf.extractfile(expected).read(),payload.read_bytes())
                self.assertFalse(any('upload-uncommitted' in n for n in names))
            with archive.open() as a,archive.open() as b:
                self.assertEqual(a.read(17),b.read(17))
            path=archive.path
            del archive;gc.collect()
            self.assertFalse(path.exists())

    def test_cache_retains_file_until_last_reader_reference(self):
        with tempfile.TemporaryDirectory() as root:
            root=Path(root); live=root/'live'; live.mkdir()
            clock=[0.0]; cache=B.ArchiveCache(ttl=1,clock=lambda:clock[0])
            make=lambda:B.build(live,root/'tm',file_backed=True)
            first=cache.get(make); path=first.path
            clock[0]=2; second=cache.get(make)
            self.assertNotEqual(first.path,second.path)
            self.assertTrue(path.exists())
            del first;gc.collect();self.assertFalse(path.exists())

    def test_file_backed_build_reclaims_only_dead_process_archives(self):
        import os
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as root:
            root=Path(root); live=root/'live'; directory=live/'scheduler/state-archives'
            directory.mkdir(parents=True)
            dead=directory/'state-99999999-stale.tar.gz'; dead.write_bytes(b'stale')
            active=directory/f'state-{os.getpid()}-active.tar.gz'; active.write_bytes(b'active')
            unrelated=directory/'not-an-archive'; unrelated.write_bytes(b'keep')
            with patch.object(B.os, 'kill', side_effect=ProcessLookupError):
                archive=B.build(live,root/'tm',file_backed=True)
            self.assertFalse(dead.exists())
            self.assertEqual(active.read_bytes(),b'active')
            self.assertEqual(unrelated.read_bytes(),b'keep')
            self.assertTrue(archive.path.name.startswith(f'state-{os.getpid()}-'))
            with archive.open() as source, tarfile.open(fileobj=source,mode='r:gz') as tf:
                self.assertIn('backup-manifest.json',tf.getnames())

    def test_default_small_archive_stays_bytes(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertIsInstance(B.build(Path(root)/'live',Path(root)/'tm'),bytes)

if __name__=='__main__': unittest.main()
