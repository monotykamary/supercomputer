"""Run ROLLOUT.sh only against fake executables; never stage a real machine."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

class Rollout(unittest.TestCase):
    def run_case(self, failure=''):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);bin=root/'bin';bin.mkdir();log=root/'calls'
            shutil.copyfile(Path(__file__).resolve().parents[1]/'ROLLOUT.sh',root/'ROLLOUT.sh')
            scripts={root/'INSTALL_VERSION.sh':'#!/bin/sh\necho local-install >> "$CALLS"\n',
                bin/'reflex':'''#!/bin/sh
echo "reflex $*" >> "$CALLS"
case "$2" in
 air|anh-1) exit 69;;
 here) case "$*" in *"--timeout 600"*) [ "$FAIL_INSTALL" != 1 ] || exit 42;; esac;;
esac
exit 0
''',bin/'supercomputer':'#!/bin/sh\necho "controller $*" >> "$CALLS"\n'}
            for p,text in scripts.items():p.write_text(text);p.chmod(0o755)
            home=root/'home';(home/'.local/bin').mkdir(parents=True)
            shutil.copy2(bin/'supercomputer',home/'.local/bin/supercomputer')
            result=subprocess.run(['bash',str(root/'ROLLOUT.sh'),'a'*40],capture_output=True,text=True,timeout=10,
                env={**os.environ,'HOME':str(home),'PATH':str(bin)+':'+os.environ['PATH'],
                     'CALLS':str(log),'FAIL_INSTALL':failure,'SC_ROLLOUT_PROTOCOL_COMPATIBLE':'1'})
            return result,log.read_text()

    def test_unreachable_air_and_anh_are_skipped_and_controller_is_staged(self):
        result,calls=self.run_case()
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('SKIP air',result.stderr);self.assertIn('SKIP anh-1',result.stderr)
        self.assertIn('reflex on here run --timeout 600',calls)
        self.assertNotIn('reflex on mini ',calls);self.assertNotIn('reflex home ',calls)
        self.assertIn('controller controller stage',calls)
        self.assertIn('Staged on 3 reachable machines',result.stdout)

    def test_reachable_install_failure_is_not_misreported_as_unreachable(self):
        result,calls=self.run_case('1')
        self.assertEqual(result.returncode,42,result.stderr)
        self.assertIn('reachable, but installation failed',result.stderr)
        self.assertNotIn('controller controller stage',calls)

if __name__=='__main__':unittest.main()
