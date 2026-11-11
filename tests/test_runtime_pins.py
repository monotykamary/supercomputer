"""Runtime pins: exact executable observations before any user payload."""
import hashlib
import inspect
import itertools
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import agent as A
from supercomputer.rules.runtime import RuntimePinGate


class RuntimePinsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=Path.home())
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        self.agent = self.root / 'agent.py'
        shutil.copyfile(A.__file__, self.agent)

    def tool(self, name='bun', version='1.4.3', body=None, directory=None):
        path = (directory or self.bin) / name
        path.parent.mkdir(parents=True, exist_ok=True)
        text = version if name == 'bun' else f'Lean (version {version}, x86_64-unknown-linux-gnu, commit test)'
        if name == 'lean' and body is None:
            body = f'if [ "$1" = "--print-prefix" ]; then printf "%s\\n" "{path.parent.parent}"; else printf "%s\\n" "{text}"; fi\n'
        path.write_text('#!/bin/sh\n' + (body if body is not None else f"printf '%s\\n' '{text}'\n"))
        path.chmod(0o755)
        return path, {'version': version, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}

    def verify(self, pins):
        with mock.patch.dict(os.environ, {'PATH': str(self.bin) + ':' + os.defpath}):
            return A.verify_runtime_pins(pins)

    def test_gate_exact_and_standalone_copy_cannot_drift(self):
        self.assertEqual(inspect.getsource(A.RuntimePinGate), inspect.getsource(RuntimePinGate))
        for values in itertools.product((False, True), repeat=5):
            self.assertEqual(A.RuntimePinGate().allow(*values), all(values))
            self.assertEqual(RuntimePinGate().allow(*values), all(values))

    def test_schema_rejects_unknown_or_inexact_declarations(self):
        _, pin = self.tool()
        for value in (None, {}, [], {'python': pin}, {'bun': None}, {'bun': {'version':'1.4.3'}},
                      {'bun':dict(pin, version='1.4')}, {'bun':dict(pin, version='>=1.4.3')},
                      {'bun':dict(pin, version=True)}, {'bun':dict(pin, sha256='x'*64)},
                      {'bun':dict(pin, sha256='a'*63)}, {'bun':dict(pin, extra=True)}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                A.normalize_runtime_pins(value)
        self.assertEqual(A.normalize_runtime_pins({'bun':dict(pin, sha256=pin['sha256'].upper())}), {'bun':pin})

    def test_bun_and_lean_both_verified(self):
        _, bun = self.tool()
        _, lean = self.tool('lean', '4.26.0')
        result = self.verify({'bun':bun, 'lean':lean})
        self.assertTrue(result['ok'], result)
        for name, pin in (('bun',bun), ('lean',lean)):
            self.assertEqual(result['observed'][name]['version'], pin['version'])
            self.assertEqual(result['observed'][name]['sha256'], pin['sha256'])
            self.assertTrue(result['observed'][name]['stable'])

    def test_hash_mismatch_never_even_runs_version_probe(self):
        marker = self.root / 'version-started'
        _, pin = self.tool(body=f'touch {marker}\nprintf 1.4.3\n')
        pin['sha256'] = '0' * 64
        result = self.verify({'bun':pin})
        self.assertFalse(result['ok'])
        self.assertFalse(marker.exists())
        self.assertFalse(result['observed']['bun']['hash_matches'])

    def test_version_mismatch_and_nonzero_probe_refuse(self):
        _, pin = self.tool()
        self.assertFalse(self.verify({'bun':dict(pin, version='1.4.2')})['ok'])
        _, pin = self.tool(body='printf 1.4.3\nexit 1\n')
        self.assertFalse(self.verify({'bun':pin})['ok'])

    def test_missing_nonexecutable_and_proxy_refuse(self):
        path, pin = self.tool()
        path.chmod(0o644)
        self.assertFalse(self.verify({'bun':pin})['ok'])
        path.unlink()
        self.assertFalse(self.verify({'bun':pin})['ok'])
        elan, lean = self.tool('lean', '4.26.0')
        target = self.bin / 'elan'
        elan.rename(target)
        elan.symlink_to(target)
        result = self.verify({'lean':lean})
        self.assertFalse(result['ok'])
        self.assertIn('elan proxy', result['error'])

    def test_hardlinked_and_copied_elan_shims_are_not_compilers(self):
        marker = self.root/'proxy-ran'
        compiler = self.root/'compiler/bin'
        self.tool('lean', '4.26.0', directory=compiler)
        proxy = self.bin/'elan'
        proxy.write_text(f'#!/bin/sh\ntouch {marker}\nexec {compiler}/lean "$@"\n')
        proxy.chmod(0o755)
        for hardlink in (True, False):
            target = self.bin/'lean'
            if hardlink:
                os.link(proxy, target)
            else:
                shutil.copyfile(proxy, target)
                target.chmod(0o755)
            result = self.verify({'lean':{'version':'4.26.0','sha256':hashlib.sha256(target.read_bytes()).hexdigest()}})
            self.assertFalse(result['ok'], result)
            self.assertIn('elan proxy', result['error'])
            self.assertFalse(marker.exists())
            target.unlink()

    def test_unknown_launcher_cannot_claim_its_delegated_compiler_hash(self):
        compiler = self.root/'compiler/bin'
        self.tool('lean', '4.26.0', directory=compiler)
        target, pin = self.tool('lean', '4.26.0', body=f'exec {compiler}/lean "$@"\n')
        result = self.verify({'lean':pin})
        self.assertFalse(result['ok'], result)
        self.assertIn('compiler prefix', result['error'])

    def test_hash_change_during_version_probe_refuses(self):
        _, pin = self.tool(body='printf 1.4.3\nprintf "#changed\\n" >> "$0"\n')
        result = self.verify({'bun':pin})
        self.assertFalse(result['ok'])
        self.assertFalse(result['observed']['bun']['stable'])

    def test_version_probe_output_is_bounded_and_timeout_refuses(self):
        _, pin = self.tool(body='printf 1.4.3\n')
        with mock.patch.object(A.subprocess, 'run', side_effect=subprocess.TimeoutExpired('bun', 10)):
            self.assertFalse(self.verify({'bun':pin})['ok'])
        _, pin = self.tool(body='head -c 17000 /dev/zero\n')
        result = self.verify({'bun':pin})
        self.assertFalse(result['ok'])
        self.assertIn('16 KiB', result['error'])

    def test_runtime_exec_never_starts_payload_on_failure(self):
        _, pin = self.tool()
        spec, receipt, marker = self.root/'spec.json', self.root/'runtime.json', self.root/'payload'
        spec.write_text(json.dumps({'runtime': {'bun': dict(pin, version='1.4.2')}}))
        command = [sys.executable, '-I', str(self.agent), 'runtime-exec', str(spec), str(receipt),
                   sys.executable, '-c', f'from pathlib import Path;Path({str(marker)!r}).touch()']
        process = subprocess.run(command, env={**os.environ, 'PATH':str(self.bin)+':'+os.defpath},
                                 capture_output=True, timeout=20)
        self.assertEqual(process.returncode, 78, process.stderr)
        self.assertFalse(marker.exists())
        self.assertFalse(json.loads(receipt.read_text())['ok'])
        spec.write_text(json.dumps({'runtime': {'bun':pin}}))
        process = subprocess.run(command, env={**os.environ, 'PATH':str(self.bin)+':'+os.defpath},
                                 capture_output=True, timeout=20)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertTrue(marker.exists())
        self.assertTrue(json.loads(receipt.read_text())['ok'])

    @unittest.skipUnless(os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') == '1', 'native namespace test runs only on boat')
    def test_real_agent_namespace_checks_effective_path_and_records_terminal_mismatch(self):
        _, pin = self.tool()
        wrong = self.root/'wrong'
        self.tool(version='1.3.14', directory=wrong)
        repo = 'runtime-test-' + self.root.name
        src = Path('/src') / repo
        subprocess.run(['sudo', '-n', 'mkdir', '-p', '/work', str(src)], check=True, timeout=5)
        self.addCleanup(lambda: subprocess.run(['sudo', '-n', 'rmdir', str(src)], check=True, timeout=5))
        agent_root = self.root/'state'
        agent_root.mkdir()
        shutil.copyfile(A.__file__, agent_root/'agent.py')
        for success in (True, False):
            with self.subTest(success=success):
                bid = 'good' if success else 'bad'
                d = agent_root/'b'/bid
                j = d/'jobs'/'job'
                j.mkdir(parents=True)
                (d/'tree').mkdir()
                (d/'ready').write_text('ready')
                declared = pin if success else dict(pin, version='1.4.2')
                spec = {'name':'job', 'kind':'sh', 'cmd':'bun --version > "$SUPERCOMPUTER_OUTPUT_DIR/version.txt"',
                        'cwd':'.', 'limit':20, 'runtime':{'bun':declared}}
                (j/'spec.json').write_text(json.dumps(spec))
                (j/'cpus').write_text(json.dumps([min(os.sched_getaffinity(0))]))
                ctx = {'name':repo, 'path':[str(self.bin)]}
                with mock.patch.object(A, 'ROOT', agent_root), mock.patch.object(A, 'ctx_of', return_value=ctx), \
                     mock.patch.dict(os.environ, {'PATH':str(wrong)+':'+os.defpath}):
                    A.run(bid, 'job')
                row = json.loads((j/'done.json').read_text())
                self.assertEqual(row['runtime_verified'], success, row)
                self.assertEqual(row['code'], 0 if success else 78, row)
                self.assertEqual((j/'outputs'/'version.txt').exists(), success)
                self.assertEqual(row['runtime_verification']['observed']['bun']['realpath'], str(self.bin/'bun'))
                if not success:
                    self.assertEqual(row['runtime_error'], 'runtime_verification_failed')
    @unittest.skipUnless(os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') == '1', 'native Python cwd scenario runs on boat')
    def test_python_relative_path_is_verified_from_payload_work_directory(self):
        _, pin = self.tool()
        repo = 'runtime-python-' + self.root.name
        src = Path('/src')/repo
        subprocess.run(['sudo','-n','mkdir','-p','/work',str(src)], check=True, timeout=5)
        self.addCleanup(lambda: subprocess.run(['sudo','-n','rmdir',str(src)], check=True, timeout=5))
        root = self.root/'python-state'
        root.mkdir()
        shutil.copyfile(A.__file__,root/'agent.py')
        d = root/'b'/'python'
        j = d/'jobs'/'job'
        j.mkdir(parents=True)
        tree = d/'tree'
        tree.mkdir()
        (d/'ready').touch()
        (tree/'runner.py').write_text('def work():\n import os,subprocess\n return {"cwd":os.getcwd(),"version":subprocess.check_output(["bun","--version"],text=True).strip()}\n')
        spec = {'name':'job','kind':'py','module':'runner.py','fn':'work','args':[],
                'limit':20,'runtime':{'bun':pin},'path':['bin']}
        (j/'spec.json').write_text(json.dumps(spec))
        (j/'cpus').write_text(json.dumps([min(os.sched_getaffinity(0))]))
        wrap = A.wrap
        def prepared(j, spec, ctx, work, tree, cwd, argv):
            (work/'bin').mkdir()
            shutil.copyfile(self.bin/'bun', work/'bin/bun')
            (work/'bin/bun').chmod(0o755)
            self.assertEqual(cwd, '/work')
            return wrap(j,spec,ctx,work,tree,cwd,argv)
        with mock.patch.object(A,'ROOT',root), mock.patch.object(A,'ctx_of',return_value={'name':repo}), mock.patch.object(A,'wrap',side_effect=prepared):
            A.run('python','job')
        row = json.loads((j/'done.json').read_text())
        self.assertEqual(row['code'],0,row)
        self.assertTrue(row['runtime_verified'],row)
        self.assertEqual(row['result'],{'cwd':'/work','version':'1.4.3'})


@unittest.skipUnless(os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') == '1', 'real pinned strict scenario runs on boat')
class StrictRuntimeScenario(unittest.TestCase):
    def test_real_bun_and_lean_pins_before_strict_payload(self):
        import test_owned_agent as lifecycle
        case = lifecycle.OwnedFixture('runTest')
        case.setUp()
        self.addCleanup(case.tearDown)
        repo = 'runtime-strict-' + case.root.name
        src = Path('/src') / repo
        subprocess.run(['sudo','-n','mkdir','-p','/work',str(src)], check=True, timeout=5)
        self.addCleanup(lambda: subprocess.run(['sudo','-n','rmdir',str(src)], check=True, timeout=5))
        home = Path(A.pwd.getpwuid(os.getuid()).pw_dir)
        bun = home/'.bun/bin/bun'
        leanbin = home/'.elan/toolchains/leanprover--lean4---v4.34.0/bin'
        pins = {'bun':{'version':'1.4.3','sha256':A._runtime_hash(bun.resolve())[0]},
                'lean':{'version':'4.34.0','sha256':A._runtime_hash((leanbin/'lean').resolve())[0]}}
        (case.d/'tree').mkdir()
        (case.d/'ready').write_text('ready')
        (case.d/'ctx.json').write_text(json.dumps({'name':repo,'path':[str(bun.parent),str(leanbin)]}))
        for success in (True, False):
            with self.subTest(success=success):
                case.name = 'good' if success else 'bad'
                declared = json.loads(json.dumps(pins))
                if not success:
                    declared['bun']['sha256'] = '0'*64
                spec = case.spec('printf payload > "$SUPERCOMPUTER_OUTPUT_DIR/started"; bun --version; lean --version')
                spec['runtime'] = declared
                A.start(case.d, spec)
                entry = A._owned_children[(case.batch,case.name)]
                self.assertTrue(entry['contained'])
                entry['p'].wait(timeout=30)
                j = case.d/'jobs'/case.name
                row = json.loads((j/'done.json').read_text())
                self.assertEqual(row['code'], 0 if success else 78, row)
                self.assertEqual(row['runtime_verified'], success, row)
                self.assertEqual((j/'outputs/started').exists(), success)
                if success:
                    self.assertEqual(row['runtime_verification']['observed']['lean']['version'], '4.34.0')
                    self.assertEqual(row['runtime_verification']['observed']['bun']['sha256'], pins['bun']['sha256'])
                case.receipt_ok(case.cancel())


if __name__ == '__main__':
    unittest.main()
