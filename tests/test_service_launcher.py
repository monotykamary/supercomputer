"""Manifest-first service boot, including the actual embedded unit command."""
import contextlib
import io
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from supercomputer import controller, launcher, scheduler


class ServiceLauncherTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = self.root / 'live/scheduler'
        self.state.mkdir(parents=True)
        self.manifest = self.state / 'install.json'
        self.key = self.root / 'key'
        self.key.write_text('fixture-key\n')
        self.source = self.publish('one')
        self.mutable = self.root / 'tool'
        decoy = self.mutable / 'supercomputer'
        decoy.mkdir(parents=True)
        (decoy / '__init__.py').write_text("raise RuntimeError('MUTABLE PACKAGE IMPORTED')\n")
        (decoy / '__main__.py').write_text("raise RuntimeError('MUTABLE PACKAGE RUN')\n")

    def publish(self, label):
        source = self.root / ('prepare-' + label)
        package = source / 'supercomputer'
        package.mkdir(parents=True)
        (package / '__init__.py').write_text('')
        (package / 'scheduler.py').write_text('label = ' + repr(label) + '\n')
        (package / '__main__.py').write_text(
            "import json, os, pathlib, sys\n"
            "print(json.dumps({'source':str(pathlib.Path(__file__).parent.parent),"
            "'src':os.environ.get('SUPERCOMPUTER_SRC'),'path':os.environ.get('PYTHONPATH'),"
            "'key_ok':os.environ.get('BOAT_API_KEY')=='fixture-key','args':sys.argv[1:]}))\n")
        code = launcher.source_hash(package)
        final = self.state / 'code' / code
        final.parent.mkdir(exist_ok=True)
        source.rename(final)
        self.record = {'code': code, 'source': str(final), 'snapshot': str(final),
                       'prepared': {'python': sys.executable}}
        self.manifest.write_text(json.dumps(self.record))
        return final

    def run_launcher(self, embedded=False):
        if embedded:
            with mock.patch.object(controller, 'SC', str(self.root)):
                unit = controller.scheduler_service_override()
            line = next(line for line in unit.splitlines() if line.startswith('ExecStart=/'))
            command = shlex.split(line.partition('=')[2])
            self.assertEqual(command[:3], ['/usr/bin/python3', '-I', '-c'])
            command[-1] = str(self.key)  # never read a real worker's tmpfs key
        else:
            command = [sys.executable, '-I', str(Path(launcher.__file__)), str(self.manifest), '--key-file', str(self.key)]
        env = {**os.environ, 'PYTHONPATH': str(self.mutable), 'SUPERCOMPUTER_SRC': str(self.mutable)}
        return subprocess.run(command, cwd=self.mutable, env=env, text=True, capture_output=True, timeout=10)

    def assert_selected(self, result, source):
        self.assertEqual(result.returncode, 0, result.stderr)
        record = json.loads(result.stdout)
        self.assertEqual(record['source'], str(source))
        self.assertEqual(record['src'], str(source))
        self.assertEqual(record['path'], str(source))
        self.assertTrue(record['key_ok'])
        self.assertEqual(record['args'], ['scheduler', 'serve', '--idle-exit', '0'])

    def test_real_process_ignores_mutable_package_and_pythonpath(self):
        self.assert_selected(self.run_launcher(), self.source)

    def test_embedded_systemd_command_resolves_current_manifest_every_start(self):
        self.assert_selected(self.run_launcher(embedded=True), self.source)
        newer = self.publish('two')
        self.assert_selected(self.run_launcher(embedded=True), newer)

    def test_systemd_accepts_the_generated_unit(self):
        tool = shutil.which('systemd-analyze')
        if not tool:
            self.skipTest('systemd-analyze unavailable; embedded command still tested')
        with mock.patch.object(controller, 'SC', str(self.root)):
            text = controller.unit('scheduler', 'unused mutable command', str(self.mutable), {})
        path = self.root / 'supercomputer-launcher-fixture.service'
        path.write_text(text)
        result = subprocess.run([tool, 'verify', '--man=no', str(path)], text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_hash_is_the_scheduler_recursive_hash(self):
        self.assertEqual(launcher.source_hash(Path(scheduler.__file__).parent),
                         scheduler.code_hash(Path(scheduler.__file__).parent))

    def test_missing_or_invalid_manifest_never_imports_mutable_code(self):
        for contents in (None, 'not json', '{}', '[]'):
            with self.subTest(contents=contents):
                if contents is None:
                    self.manifest.unlink()
                else:
                    self.manifest.write_text(contents)
                result = self.run_launcher()
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('scheduler bootstrap refused', result.stderr)
                self.assertNotIn('MUTABLE PACKAGE', result.stderr)
                self.assertEqual(result.stdout, '')

    def test_manifest_cannot_select_mutable_checkout(self):
        self.record.update(source=str(self.mutable), snapshot=str(self.mutable))
        self.manifest.write_text(json.dumps(self.record))
        with self.assertRaisesRegex(ValueError, 'immutable'):
            launcher.installed(self.manifest)

    def test_missing_snapshot_is_refused(self):
        (self.source / 'supercomputer/scheduler.py').unlink()
        with self.assertRaisesRegex(ValueError, 'lacks scheduler.py'):
            launcher.installed(self.manifest)

    def test_tampered_and_nested_modules_are_refused(self):
        package = self.source / 'supercomputer'
        (package / 'rules').mkdir()
        (package / 'rules/policy.py').write_text('changed = True\n')
        with self.assertRaisesRegex(ValueError, 'hash verification'):
            launcher.installed(self.manifest)

    def test_symlinked_snapshot_is_refused(self):
        moved = self.root / 'moved'
        self.source.rename(moved)
        self.source.symlink_to(moved, target_is_directory=True)
        with self.assertRaises(ValueError):
            launcher.installed(self.manifest)

    def test_prepared_interpreter_must_exist(self):
        self.record['prepared']['python'] = str(self.root / 'missing-python')
        self.manifest.write_text(json.dumps(self.record))
        with self.assertRaisesRegex(ValueError, 'prepared interpreter'):
            launcher.installed(self.manifest)

    def test_changed_manifest_never_executes_old_selection(self):
        selected = launcher.installed(self.manifest)
        self.publish('new-generation')
        with mock.patch.object(launcher, 'installed', return_value=selected), \
             mock.patch.object(launcher.os, 'execve') as execute:
            with self.assertRaisesRegex(ValueError, 'changed during'):
                launcher.launch(self.manifest, self.key)
        execute.assert_not_called()

    def test_setup_publishes_before_enabling_and_installs_override(self):
        self.check_setup(only='')

    def test_treadmill_only_setup_does_not_change_scheduler(self):
        self.check_setup(only='treadmill')

    def check_setup(self, only):
        calls = []
        def ssh(vm, command, *args, **kwargs):
            calls.append(command if command != 'bash -s' else args[0].decode())
            if 'TOOL-OK' in command:
                return 0, 'TOOL-OK'
            if command.startswith('test '):
                return 0, 'HAVE'
            if command.startswith('cat '):
                return 0, 'fixture-token'
            return 0, 'UNITS-OK'
        project = SimpleNamespace(root=self.root, name='fixture')
        with contextlib.ExitStack() as stack:
            for obj, name, value in ((controller.config, 'project', project),
                                     (controller.config, 'main_checkout', self.root),
                                     (controller, 'tool_tar', b'tar'),
                                     (controller, 'info', {}),
                                     (controller.boat, 'bj', {'url': 'fixture-url'})):
                stack.enter_context(mock.patch.object(obj, name, return_value=value))
            for name in ('sync_git', 'handover'):
                stack.enter_context(mock.patch.object(controller, name))
            stack.enter_context(mock.patch.object(controller.scheduler, 'write_private'))
            stack.enter_context(mock.patch.object(controller, 'ssh', side_effect=ssh))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            controller.setup('fixture', only=only)
        script = next(call for call in calls if 'UNITS-OK' in call)
        if only:
            self.assertNotIn('scheduler stage', script)
            self.assertNotIn('supercomputer-scheduler.service', script)
            self.assertNotIn('30-install-pinned.conf', script)
        else:
            self.assertLess(script.index('scheduler stage'), script.index('systemctl enable --now supercomputer-scheduler'))
            self.assertIn('30-install-pinned.conf', script)
            self.assertLess(script.index('30-install-pinned.conf'), script.index('systemctl daemon-reload'))
            self.assertLess(script.index('systemctl daemon-reload'), script.index('scheduler stage'))
            self.assertIn('python3 -I -c', script)
        subprocess.run(['/bin/bash', '-n'], input=script, text=True, check=True, timeout=5)


if __name__ == '__main__':
    unittest.main()
