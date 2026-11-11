"""Strict jobs preserve passwd HOME and standard tool paths, with read-only mounts."""
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import agent as A
import test_owned_agent as fixture


class EnvironmentSourceTests(unittest.TestCase):
    def test_supervisor_env_is_restored_before_python_import(self):
        home = pwd.getpwuid(os.getuid()).pw_dir
        with mock.patch.object(A, 'HOME', '/root'), mock.patch.dict(os.environ, {'PATH':'/owned/test/path'}):
            argv = A._owned_argv('batch', {'name':'item','launch_identity':'nonce'})
        python_at = argv.index(sys.executable)
        self.assertIn('HOME=' + home, argv[:python_at])
        self.assertIn('PATH=/owned/test/path', argv[:python_at])
        self.assertIn('--pid', argv)
        self.assertIn('--kill-child=KILL', argv)


@unittest.skipUnless(os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') == '1', 'native scenario requires boat')
class StrictEnvironmentScenario(fixture.OwnedFixture):
    def setUp(self):
        super().setUp()
        self.repo = 'strict-env-' + self.root.name
        self.src = Path('/src') / self.repo
        subprocess.run(['sudo','-n','mkdir','-p','/work',str(self.src)], check=True, timeout=5)
        (self.d/'tree').mkdir()
        (self.d/'ready').write_text('ready')
        (self.d/'ctx.json').write_text(json.dumps({'name':self.repo, 'path':['$HOME/.bun/bin','$HOME/.elan/bin']}))

    def tearDown(self):
        try:
            super().tearDown()
        finally:
            subprocess.run(['sudo','-n','rmdir',str(self.src)], check=True, timeout=5)

    def launch_and_read(self, spec):
        A.start(self.d, spec)
        entry = A._owned_children[(self.batch,self.name)]
        self.assertTrue(entry.get('contained'), (self.d/'jobs'/self.name/'agent.err').read_text())
        entry['p'].wait(timeout=30)
        row = json.loads((self.d/'jobs'/self.name/'done.json').read_text())
        self.assertEqual(row['code'], 0, row | {'stderr':(self.d/'jobs'/self.name/'err').read_text()})
        self.assertEqual(row['launch_identity'], self.nonce)
        self.receipt_ok(self.cancel())
        return json.loads((self.d/'jobs'/self.name/'out').read_text())

    def test_standard_paths_fixture_resolves_bun_and_lean_read_only(self):
        home = pwd.getpwuid(os.getuid()).pw_dir
        self.assertEqual(home, '/home/user')
        for directory in ('.bun','.elan'):
            self.assertTrue((Path(home)/directory).is_dir(), directory)
        bun, elan = self.root/'fixture-bun', self.root/'fixture-elan'
        (bun/'bin').mkdir(parents=True)
        lean = elan/'toolchains'/'leanprover--lean4---v4.34.0'/'bin'/'lean'
        lean.parent.mkdir(parents=True)
        (elan/'bin').mkdir()
        (bun/'bin'/'bun').write_text('#!/bin/sh\nprintf "1.4.3\\n"\n')
        lean.write_text('#!/bin/sh\nprintf "Lean (version 4.34.0, x86_64-unknown-linux-gnu, commit fixture)\\n"\n')
        (bun/'bin'/'bun').chmod(0o755)
        lean.chmod(0o755)
        (elan/'bin'/'lean').symlink_to('../toolchains/leanprover--lean4---v4.34.0/bin/lean')
        script = self.d/'tree'/'probe.py'
        script.write_text('import json,os,pathlib,pwd,shutil,subprocess\n'
            'h=os.environ["HOME"]; assert h==pwd.getpwuid(os.getuid()).pw_dir=="/home/user"\n'
            'assert shutil.which("bun")==h+"/.bun/bin/bun"\n'
            'assert shutil.which("lean")==h+"/.elan/bin/lean"\n'
            'b=subprocess.check_output(["bun","--version"],text=True).strip(); assert b=="1.4.3"\n'
            'l=subprocess.check_output(["lean","--version"],text=True).strip(); assert "version 4.34.0," in l\n'
            'assert pathlib.Path(h+"/.sc/agent.py").is_file()\n'
            'mounts={line.split()[4]:line.split()[5].split(",") for line in pathlib.Path("/proc/self/mountinfo").read_text().splitlines()}\n'
            'assert "ro" in mounts[h+"/.bun"] and "ro" in mounts[h+"/.elan"]\n'
            'assert "ro" in mounts[h+"/.sc/agent.py"]\n'
            'for p in (h+"/.bun/bin/bun",h+"/.elan/bin/lean"):\n'
            ' try: f=open(p,"ab")\n'
            ' except OSError as e: assert e.errno==30\n'
            ' else: f.close(); raise AssertionError("toolchain writable")\n'
            'print(json.dumps({"home":h,"path":os.environ["PATH"],"bun":b,"lean":l,"read_only":True}))\n')
        spec = self.spec('python3 probe.py')
        spec['mounts'] = {home+'/.bun':str(bun), home+'/.elan':str(elan)}
        # Even a stale daemon global and explicit stale HOME cannot redirect strict jobs.
        spec['env'] = {'HOME':'/root'}
        with mock.patch.object(A,'HOME','/root'):
            result = self.launch_and_read(spec)
        self.assertEqual(result['home'],home)
        self.assertTrue(result['read_only'])

    def test_actual_installed_toolchains_in_strict_namespace(self):
        # A real-image acceptance scenario; no installers or invented compiler output.
        script = self.d/'tree'/'installed.py'
        script.write_text('import json,os,pathlib,pwd,shutil,subprocess\n'
            'h=os.environ["HOME"]; assert h==pwd.getpwuid(os.getuid()).pw_dir=="/home/user"\n'
            'assert pathlib.Path(h+"/.sc/agent.py").is_file()\n'
            'assert shutil.which("bun")==h+"/.bun/bin/bun"\n'
            'b=subprocess.check_output(["bun","--version"],text=True).strip(); assert b=="1.4.3",b\n'
            'p=h+"/.elan/toolchains/leanprover--lean4---v4.34.0/bin/lean"\n'
            'l=subprocess.check_output([p,"--version"],text=True).strip(); assert "version 4.34.0," in l,l\n'
            'print(json.dumps({"home":h,"path":os.environ["PATH"],"bun":b,"lean":l,"agent":h+"/.sc/agent.py"}))\n')
        result = self.launch_and_read(self.spec('python3 installed.py'))
        self.assertEqual(result['bun'],'1.4.3')


if __name__ == '__main__':
    unittest.main()
