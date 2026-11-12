"""The shipped process probe on real Linux processes and private fake agent storage."""
import contextlib
import json
import shutil
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import workerprobe

AGENT = '''import contextlib, json, os, pathlib
ROOT = pathlib.Path(__file__).parent
@contextlib.contextmanager
def _lease_request(value):
    if value.get('owner') != 'test-owner': raise PermissionError('wrong owner')
    yield
@contextlib.contextmanager
def lease_guard(value):
    yield
@contextlib.contextmanager
def _ownership_lock():
    yield
def write_json(path, value):
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as f:
        f.write(json.dumps(value)); f.flush(); os.fsync(f.fileno())
    tmp.replace(path)
'''


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'agent.py').write_text(AGENT)
        self.job = self.root / 'b' / 'batch' / 'jobs' / 'job'
        self.job.mkdir(parents=True)
        (self.job / 'spec.json').write_text('{}')
        (self.job / 'pid').write_text('2147483647')

    def probe(self, reclaim=True, strict=False, owner='test-owner'):
        source = workerprobe._REMOTE.replace("ROOT = pathlib.Path('/home/user/.sc')", 'ROOT = pathlib.Path(' + repr(str(self.root)) + ')')
        request = {'lease': {'owner': owner}, 'runs': [{'batch': 'batch', 'name': 'job', 'strict': strict}], 'reclaim': reclaim}
        p = subprocess.run([sys.executable, '-c', source, json.dumps(request)], capture_output=True, text=True, timeout=30)
        self.assertEqual(p.stderr, '')
        return p.returncode, json.loads(p.stdout)

    def test_dead_root_becomes_a_durable_terminal_loss_without_removing_outputs(self):
        out = self.job / 'outputs'
        out.mkdir()
        (out / 'partial.txt').write_text('preserve')
        rc, value = self.probe(False)
        row = value['runs'][0]
        self.assertEqual(rc, 0)
        self.assertTrue(row['complete'])
        self.assertFalse(row['live'] or row['descendants'] or row['reclaimed'])
        self.assertFalse((self.job / 'done.json').exists())
        rc, value = self.probe()
        self.assertTrue(value['runs'][0]['reclaimed'])
        self.assertEqual(json.loads((self.job / 'done.json').read_text())['status'], 'LOST')
        self.assertEqual((out / 'partial.txt').read_text(), 'preserve')
        rc, again = self.probe()
        self.assertTrue(again['runs'][0]['reclaimed'])

    def test_live_root_and_orphaned_session_child_are_preserved(self):
        child = subprocess.Popen(['sleep', '30'], start_new_session=True)
        try:
            (self.job / 'pid').write_text(str(child.pid))
            _, value = self.probe()
            self.assertTrue(value['runs'][0]['live'])
            self.assertFalse(value['runs'][0]['reclaimed'])
        finally:
            child.terminate()
            child.wait(timeout=5)
        parent = subprocess.Popen([sys.executable, '-c',
            "import subprocess; p=subprocess.Popen(['sleep','30'],stdout=subprocess.DEVNULL); print(p.pid,flush=True)"],
            start_new_session=True, stdout=subprocess.PIPE, text=True)
        orphan = int(parent.stdout.readline())
        parent.wait(timeout=5)
        try:
            (self.job / 'pid').write_text(str(parent.pid))
            _, value = self.probe()
            self.assertFalse(value['runs'][0]['live'])
            self.assertIn(orphan, value['runs'][0]['descendants'])
            self.assertFalse(value['runs'][0]['reclaimed'])
        finally:
            os.kill(orphan, signal.SIGTERM)
            parent.stdout.close()

    def test_changed_uid_and_session_with_only_output_identity_is_preserved(self):
        if os.getuid() != 0:
            self.skipTest('UID-changing process test needs the private root PID namespace')
        env = {**os.environ, 'SUPERCOMPUTER_OUTPUT_DIR': str(self.job / 'outputs')}
        code = ("import os,subprocess,sys; "
                "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)'],"
                "cwd='/',start_new_session=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,"
                "preexec_fn=lambda:(os.setgid(65534),os.setuid(65534))); print(p.pid,flush=True)")
        parent = subprocess.Popen([sys.executable, '-c', code], env=env,
                                  stdout=subprocess.PIPE, text=True)
        orphan = int(parent.stdout.readline())
        parent.wait(timeout=5)
        try:
            (self.job / 'pid').write_text(str(parent.pid))
            _, value = self.probe()
            row = value['runs'][0]
            self.assertFalse(row['live'])
            self.assertFalse(row['reclaimed'])
            self.assertTrue(orphan in row['descendants'] or not row['complete'])
        finally:
            os.kill(orphan, signal.SIGTERM)
            parent.stdout.close()

    def test_strict_results_missing_pid_and_wrong_lease_fail_closed(self):
        for mode in ('strict', 'stored-strict', 'result', 'pid', 'lease'):
            with self.subTest(mode=mode):
                (self.job / 'pid').write_text('2147483647')
                (self.job / 'spec.json').write_text(json.dumps({'strict_once': mode == 'stored-strict'}))
                (self.job / 'done.json').unlink(missing_ok=True)
                if mode == 'result':
                    (self.job / 'done.json').write_text('{"code":0}')
                if mode == 'pid':
                    (self.job / 'pid').unlink()
                rc, value = self.probe(strict=mode == 'strict', owner='wrong' if mode == 'lease' else 'test-owner')
                if mode == 'lease':
                    self.assertNotEqual(rc, 0)
                else:
                    self.assertFalse(value['runs'][0]['reclaimed'])


if __name__ == '__main__':
    if not os.environ.get('SC_PROBE_PIDNS') and shutil.which('sudo') and shutil.which('unshare'):
        # A private proc mount gives these real-process tests complete visibility.
        sys.exit(subprocess.call(['sudo', '-n', 'unshare', '--pid', '--fork', '--mount-proc',
                                  'env', 'SC_PROBE_PIDNS=1', sys.executable, __file__]))
    unittest.main()
