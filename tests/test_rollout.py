"""Shell integration tests: run ROLLOUT.sh only against fake executables."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


INVENTORY = '''Machines Reflex reaches:
here (Tom's MacBook Air, where Reflex runs)
anh-1
mini
boat-example (boat sandbox)
'''


class Rollout(unittest.TestCase):
    def run_case(self, *, inventory=INVENTORY, unreachable='anh-1', failure='',
                 dropped='', discovery_failure='', override=None):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            bin = root / 'bin'
            bin.mkdir()
            log = root / 'calls'
            shutil.copyfile(Path(__file__).resolve().parents[1] / 'ROLLOUT.sh', root / 'ROLLOUT.sh')
            scripts = {
                root / 'INSTALL_VERSION.sh': '#!/bin/sh\necho local-install >> "$CALLS"\n',
                bin / 'reflex': '''#!/bin/sh
echo "reflex $*" >> "$CALLS"
if [ "$1 $2" = 'home machines' ]; then
    [ "$FAIL_DISCOVERY" != 1 ] || exit 70
    printf '%s\n' "$INVENTORY"
    exit 0
fi
[ "$1" = on ] || exit 64
machine=$2
case " $UNREACHABLE " in *" $machine "*) exit 69;; esac
[ ! -f "$DROP_MARKER" ] || [ "$machine" != "$DROP_INSTALL" ] || exit 69
if [ "$5" = 600 ]; then
    [ "$machine" != "$FAIL_INSTALL" ] || exit 42
    if [ "$machine" = "$DROP_INSTALL" ]; then
        touch "$DROP_MARKER"
        exit 69
    fi
fi
exit 0
''',
                bin / 'supercomputer': '#!/bin/sh\necho "controller $*" >> "$CALLS"\n',
            }
            for p, text in scripts.items():
                p.write_text(text)
                p.chmod(0o755)
            home = root / 'home'
            (home / '.local/bin').mkdir(parents=True)
            shutil.copy2(bin / 'supercomputer', home / '.local/bin/supercomputer')
            env = {**os.environ, 'HOME': str(home), 'PATH': str(bin) + ':' + os.environ['PATH'],
                   'CALLS': str(log), 'INVENTORY': inventory, 'UNREACHABLE': unreachable,
                   'FAIL_INSTALL': failure, 'DROP_INSTALL': dropped, 'DROP_MARKER': str(root / 'dropped'),
                   'FAIL_DISCOVERY': discovery_failure, 'SC_ROLLOUT_PROTOCOL_COMPATIBLE': '1'}
            env.pop('SC_ROLLOUT_MACHINES', None)
            if override is not None:
                env['SC_ROLLOUT_MACHINES'] = override
            result = subprocess.run(['bash', str(root / 'ROLLOUT.sh'), 'a' * 40],
                                    capture_output=True, text=True, timeout=10, env=env)
            return result, log.read_text() if log.exists() else ''

    def test_home_air_and_named_mini_are_both_discovered(self):
        result, calls = self.run_case()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('reflex home machines\n', calls)
        self.assertIn('reflex on here run --timeout 600', calls)
        self.assertIn('reflex on mini run --timeout 600', calls)
        self.assertNotIn('reflex on air ', calls)
        self.assertNotIn('reflex on boat-example ', calls)
        self.assertEqual(calls.count('local-install\n'), 1)
        self.assertIn('SKIP anh-1', result.stderr)
        self.assertIn('controller controller stage', calls)
        self.assertIn('Staged on 4 reachable machines', result.stdout)

    def test_every_new_named_machine_is_included_even_when_home_is_unreachable(self):
        inventory = INVENTORY.replace('mini\n', 'mini\nstudio-new (another Mac)\n')
        result, calls = self.run_case(inventory=inventory, unreachable='here anh-1')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('SKIP here', result.stderr)
        self.assertIn('SKIP anh-1', result.stderr)
        for machine in ('mini', 'studio-new'):
            self.assertIn(f'reflex on {machine} run --timeout 600', calls)
        self.assertIn('Staged on 4 reachable machines', result.stdout)

    def test_reachable_install_failure_is_not_misreported_as_unreachable(self):
        result, calls = self.run_case(failure='mini')
        self.assertEqual(result.returncode, 42, result.stderr)
        self.assertIn('reachable, but installation failed', result.stderr)
        self.assertNotIn('controller controller stage', calls)

    def test_lost_connection_during_install_is_skipped(self):
        result, calls = self.run_case(dropped='mini')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('SKIP mini: lost Reflex connection during staging', result.stderr)
        self.assertIn('controller controller stage', calls)
        self.assertIn('Staged on 3 reachable machines', result.stdout)

    def test_failed_discovery_stages_nothing(self):
        result, calls = self.run_case(discovery_failure='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('could not discover Reflex machines', result.stderr)
        self.assertNotIn('local-install', calls)
        self.assertNotIn('reflex on ', calls)
        self.assertNotIn('controller controller stage', calls)

    def test_malformed_or_empty_inventory_stages_nothing(self):
        for inventory in ('', 'Machines Reflex reaches:\n', 'unexpected format\nmini\n',
                          'Machines Reflex reaches:\nmini\n--bad\n'):
            with self.subTest(inventory=inventory):
                result, calls = self.run_case(inventory=inventory)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('local-install', calls)
                self.assertNotIn('reflex on ', calls)
                self.assertNotIn('controller controller stage', calls)

    def test_duplicate_entries_stage_once(self):
        result, calls = self.run_case(inventory=INVENTORY.replace('mini\n', 'mini\nmini\n'))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(calls.count('reflex on mini run --timeout 600'), 1)

    def test_local_sandbox_only_still_stages_the_controller(self):
        result, calls = self.run_case(inventory='Machines Reflex reaches:\nboat-example (boat sandbox)\n')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('reflex on ', calls)
        self.assertIn('local-install', calls)
        self.assertIn('controller controller stage', calls)
        self.assertIn('Staged on 2 reachable machines', result.stdout)

    def test_explicit_machine_override_does_not_require_discovery(self):
        result, calls = self.run_case(override='mini here', discovery_failure='1')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('reflex home machines', calls)
        self.assertIn('reflex on mini run --timeout 600', calls)
        self.assertIn('reflex on here run --timeout 600', calls)
        self.assertIn('Staged on 4 reachable machines', result.stdout)


if __name__ == '__main__':
    unittest.main()
