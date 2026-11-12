"""Strict jobs preserve passwd HOME and standard tool paths, with read-only mounts."""
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tarfile
import tempfile
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


class PrivateDirectoryDefaults(unittest.TestCase):
    def wrapped_env(self, root, strict=True, ctx_env=None, spec_env=None):
        job = root / 'job'
        job.mkdir()
        (job / 'cpus').write_text('[0]')
        work, tree = job / 'work', root / 'tree'
        work.mkdir()
        tree.mkdir()
        with mock.patch.object(A, 'ROOT', root / 'agent'):
            argv = A.wrap(job, {'strict_once': strict, 'limit': 30, 'env': spec_env or {}},
                          {'name': 'repo', 'env': ctx_env or {}}, work, tree, '/work/repo', ['true'])
        env = dict(value.split('=', 1) for value in argv[argv.index('env') + 2:] if '=' in value)
        return job, env

    def test_strict_defaults_are_absolute_private_and_inside_existing_cleanup_tree(self):
        with tempfile.TemporaryDirectory() as directory:
            job, env = self.wrapped_env(Path(directory))
            self.assertEqual(env['TMPDIR'], str(job / 'tmp'))
            self.assertEqual(env['BUN_TMPDIR'], env['TMPDIR'])
            self.assertEqual(env['XDG_CACHE_HOME'], str(job / 'tmp/cache'))
            self.assertEqual(env['BUN_INSTALL_CACHE_DIR'], str(job / 'tmp/cache/bun'))
            for path in (job / 'tmp', job / 'tmp/cache'):
                self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            self.assertEqual(env['HOME'], pwd.getpwuid(os.getuid()).pw_dir)

    def test_explicit_job_values_override_context_and_defaults(self):
        keys = ('TMPDIR', 'BUN_TMPDIR', 'XDG_CACHE_HOME', 'BUN_INSTALL_CACHE_DIR')
        with tempfile.TemporaryDirectory() as directory:
            _, env = self.wrapped_env(Path(directory), ctx_env={k: '/context/' + k for k in keys},
                                      spec_env={k: '/explicit/' + k for k in keys})
            for key in keys:
                self.assertEqual(env[key], '/explicit/' + key)
        with tempfile.TemporaryDirectory() as directory:
            _, env = self.wrapped_env(Path(directory), ctx_env={'TMPDIR': '/context/tmp'})
            self.assertEqual(env['TMPDIR'], '/context/tmp')
            self.assertNotEqual(env['BUN_TMPDIR'], '/context/tmp')

    def test_ordinary_defaults_are_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            job, env = self.wrapped_env(Path(directory), strict=False)
            self.assertNotIn('TMPDIR', env)
            self.assertNotIn('BUN_TMPDIR', env)
            self.assertNotIn('XDG_CACHE_HOME', env)
            self.assertEqual(env['BUN_INSTALL_CACHE_DIR'], '/tmp/.bun-install-cache')
            self.assertFalse((job / 'tmp/cache').exists())


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

    def test_private_temp_and_cache_are_writable_without_unprotecting_tools(self):
        script = self.d / 'tree' / 'private_dirs.py'
        script.write_text('import errno,json,os,pathlib,tempfile\n'
            'paths={k:os.environ[k] for k in ("TMPDIR","BUN_TMPDIR","XDG_CACHE_HOME","BUN_INSTALL_CACHE_DIR")}\n'
            'root=pathlib.Path(paths["TMPDIR"]); assert root.is_absolute()\n'
            'assert paths["BUN_TMPDIR"]==str(root)\n'
            'assert pathlib.Path(paths["XDG_CACHE_HOME"])==root/"cache"\n'
            'assert pathlib.Path(paths["BUN_INSTALL_CACHE_DIR"])==root/"cache/bun"\n'
            'for value in paths.values():\n'
            ' p=pathlib.Path(value); p.mkdir(parents=True,exist_ok=True); (p/"write-probe").write_text("private")\n'
            'with tempfile.NamedTemporaryFile() as f: assert pathlib.Path(f.name).parent==root\n'
            'try: open(os.environ["HOME"]+"/.bun/strict-write-probe","w")\n'
            'except OSError as e: assert e.errno==errno.EROFS,e\n'
            'else: raise AssertionError("installed toolchain was writable")\n'
            'print(json.dumps(paths))\n')
        result = self.launch_and_read(self.spec('python3 private_dirs.py'))
        job = self.d / 'jobs' / self.name
        self.assertEqual(result['TMPDIR'], str(job / 'tmp'))
        self.assertFalse((job / 'tmp').exists(), 'private scratch must use existing terminal cleanup')

    def test_actual_bun_frozen_install_with_private_defaults_and_sanitized_env(self):
        # Fully offline fixture: a local tarball dependency and a lock generated
        # by the actual installed Bun, never a fake installer or registry fetch.
        tree = self.d / 'tree'
        package = self.root / 'package'
        package.mkdir()
        (package / 'package.json').write_text(json.dumps({'name':'strict-local-dependency','version':'1.0.0'}))
        (package / 'index.js').write_text('module.exports = 42;\n')
        with tarfile.open(tree / 'dependency.tgz', 'w:gz') as archive:
            archive.add(package, arcname='package')
        (tree / 'package.json').write_text(json.dumps({'name':'strict-install-fixture',
            'dependencies':{'strict-local-dependency':'file:./dependency.tgz'}}))
        bun = str(Path(pwd.getpwuid(os.getuid()).pw_dir) / '.bun/bin/bun')
        seed = self.root / 'seed-tmp'
        seed.mkdir()
        env = {**os.environ, 'TMPDIR':str(seed), 'BUN_TMPDIR':str(seed),
               'BUN_INSTALL_CACHE_DIR':str(seed / 'bun'), 'XDG_CACHE_HOME':str(seed / 'cache')}
        prepared = subprocess.run([bun,'install','--ignore-scripts'], cwd=tree, env=env,
                                  capture_output=True, text=True, timeout=20)
        self.assertEqual(prepared.returncode, 0, prepared.stdout + prepared.stderr)
        import shutil
        shutil.rmtree(tree / 'node_modules')
        script = tree / 'install_probe.py'
        script.write_text('import json,os,pathlib,subprocess\n'
            'keys=("TMPDIR","BUN_TMPDIR","XDG_CACHE_HOME","BUN_INSTALL_CACHE_DIR")\n'
            'paths={k:os.environ[k] for k in keys}\n'
            'bun=os.environ["HOME"]+"/.bun/bin/bun"\n'
            'clean={k:os.environ[k] for k in ("PATH","HOME",*keys)}\n'
            'results=[]\n'
            'for env in (os.environ.copy(),clean):\n'
            ' r=subprocess.run([bun,"install","--frozen-lockfile","--ignore-scripts"],env=env,capture_output=True,text=True,timeout=20)\n'
            ' assert r.returncode==0,r.stdout+r.stderr\n'
            ' assert pathlib.Path("node_modules/strict-local-dependency/index.js").is_file()\n'
            ' results.append({"code":r.returncode,"stdout":r.stdout,"stderr":r.stderr})\n'
            ' import shutil; shutil.rmtree("node_modules")\n'
            'print(json.dumps({"paths":paths,"installs":results}))\n')
        result = self.launch_and_read(self.spec('python3 install_probe.py'))
        self.assertEqual(len(result['installs']), 2)
        self.assertTrue(all(row['code'] == 0 for row in result['installs']))

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
