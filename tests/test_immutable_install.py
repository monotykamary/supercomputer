"""Exercise rollout ordering with fake uv/reflex, never a real installation."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
FAKE_UV = r'''#!/usr/bin/env python3
import json,os,pathlib,sys
p=pathlib.Path(os.environ['SC_TEST_TRACE'])
with p.open('a') as f:f.write(json.dumps(['uv',str(pathlib.Path.home()),*sys.argv[1:]])+'\n')
if sys.argv[1]=='venv':
 r=pathlib.Path(sys.argv[-1]);(r/'bin').mkdir(parents=True,exist_ok=True)
 (r/'bin/python').write_text('test-python')
 s=r/'bin/supercomputer';s.write_text(os.environ['SC_TEST_CLI']);s.chmod(0o755)
elif os.environ.get('SC_TEST_FAIL')=='pip':sys.exit(41)
'''
FAKE_CLI = r'''#!/usr/bin/env python3
import json,os,pathlib,sys
entry=pathlib.Path.home()/'.local/bin/supercomputer'
with open(os.environ['SC_TEST_TRACE'],'a') as f:
 f.write(json.dumps(['cli',str(pathlib.Path.home()),*sys.argv[1:],str(entry.resolve()) if entry.exists() else None])+'\n')
if sys.argv[1:]==['scheduler','stage'] and os.environ.get('SC_TEST_FAIL')=='stage':sys.exit(42)
print('{"active":"old","staged":"new","pending":true}')
'''
FAKE_REFLEX = r'''#!/usr/bin/env python3
import json,os,pathlib,subprocess,sys
args=sys.argv[1:]; machine='air' if args[0]=='home' else args[1]
with open(os.environ['SC_TEST_TRACE'],'a') as f:f.write(json.dumps(['reflex',machine,*args])+'\n')
home=pathlib.Path(os.environ['SC_TEST_MACHINES'])/machine;home.mkdir(parents=True,exist_ok=True)
env=dict(os.environ,HOME=str(home));env.pop('XDG_DATA_HOME',None)
sys.exit(subprocess.run(['bash','-c',args[-1]],env=env).returncode)
'''


class ImmutableInstall(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='sc install ')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.home = self.root / 'home'
        self.bin = self.home / '.local/bin'
        self.bin.mkdir(parents=True)
        self.trace = self.root / 'trace.jsonl'
        self.env = dict(os.environ, HOME=str(self.home),
                        PATH=str(self.bin) + os.pathsep + os.environ['PATH'],
                        SC_ROLLOUT_PROTOCOL_COMPATIBLE='1', SC_TEST_TRACE=str(self.trace),
                        SC_TEST_CLI=FAKE_CLI, SC_TEST_MACHINES=str(self.root/'machines'))
        for key in ('XDG_DATA_HOME', 'SUPERCOMPUTER_HOME', 'SUPERCOMPUTER_LIVE', 'SUPERCOMPUTER_SRC', 'SC_TEST_FAIL'):
            self.env.pop(key, None)
        self.fake = self.root / 'fake-bin'
        self.fake.mkdir()
        for name, text in (('uv', FAKE_UV), ('reflex', FAKE_REFLEX)):
            p = self.fake/name
            p.write_text(text)
            p.chmod(0o755)
        self.env['PATH'] = str(self.fake) + os.pathsep + self.env['PATH']
        self.old = self.root/'old-version'
        self.old.write_text('old tree remains intact')
        self.entry = self.bin/'supercomputer'
        self.entry.symlink_to(self.old)

    def run_script(self, script='INSTALL_VERSION.sh', ref='a'*40, **env):
        return subprocess.run(['bash', str(ROOT/script), ref], env=dict(self.env, **env),
                              text=True, capture_output=True, timeout=20)

    def records(self):
        return [json.loads(x) for x in self.trace.read_text().splitlines()]

    def test_stage_before_switch_and_old_versions_are_immutable(self):
        r = self.run_script()
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        first = self.entry.resolve()
        self.assertIn('a'*40, str(first))
        self.assertEqual(self.old.read_text(), 'old tree remains intact')
        stage = [x for x in self.records() if x[0]=='cli'][0]
        self.assertEqual(stage[2:4], ['scheduler', 'stage'])
        self.assertEqual(stage[-1], str(self.old))
        code = first.read_bytes()
        r = self.run_script(ref='b'*40)
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        self.assertNotEqual(first, self.entry.resolve())
        self.assertEqual(first.read_bytes(), code)
        self.assertEqual([x for x in self.records() if x[0]=='cli'][-1][-1], str(first))

    def test_pip_or_stage_failure_preserves_selected_version(self):
        for failure in ('pip', 'stage'):
            with self.subTest(failure=failure):
                r = self.run_script(SC_TEST_FAIL=failure)
                self.assertNotEqual(r.returncode, 0)
                self.assertEqual(self.entry.resolve(), self.old)
                self.assertEqual(self.old.read_text(), 'old tree remains intact')

    def test_ref_and_protocol_guard_precede_any_install(self):
        self.assertEqual(self.run_script(ref='main').returncode, 2)
        self.assertEqual(self.run_script(SC_ROLLOUT_PROTOCOL_COMPATIBLE='0').returncode, 2)
        self.assertFalse(self.trace.exists())
        self.assertEqual(self.entry.resolve(), self.old)

    def test_rollout_uses_same_immutable_helper_on_all_four_clients(self):
        r = self.run_script(script='ROLLOUT.sh')
        self.assertEqual(r.returncode, 0, r.stdout+r.stderr)
        records = self.records()
        stages = [x for x in records if x[0]=='cli' and x[2:4]==['scheduler','stage']]
        self.assertEqual(len(stages), 4)
        self.assertEqual({Path(x[1]).name for x in stages}, {'home','air','mini','anh-1'})
        self.assertEqual(len([x for x in records if x[0]=='cli' and x[2:4]==['controller','stage']]), 1)
        self.assertFalse(any('checkout' in x or 'reinstall' in x for record in records for x in record if isinstance(x,str)))
        self.assertEqual(self.old.read_text(), 'old tree remains intact')


if __name__ == '__main__':
    unittest.main()
