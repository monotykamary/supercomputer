"""Opt-in actual memory limits; real subprocesses only on the native gate."""
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
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from supercomputer import agent as A
from supercomputer.rules.resources import ResourceLimitGate
from supercomputer.rules.agent_capabilities import required_agent_capabilities


class MemoryLimits(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(dir=Path.home());self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.agent=self.root/'agent.py'
        shutil.copyfile(A.__file__,self.agent)
        self.spec={'enforce_memory':True,'memory':64,'memory_limit_mode':'rlimit-as'}
        (self.root/'taskset').write_text(json.dumps(sorted(os.sched_getaffinity(0))))

    def invoke(self, spec, body):
        (self.root/'spec.json').write_text(json.dumps(spec))
        return subprocess.run([sys.executable,'-I',str(self.agent),'resource-exec',
            str(self.root/'spec.json'),str(self.root/'resources.json'),sys.executable,'-c',body],
            capture_output=True,text=True,timeout=20)

    def test_schema_default_and_capability_derivation(self):
        self.assertIsNone(A.normalize_memory_limit({}))
        self.assertIsNone(A.normalize_memory_limit({'enforce_memory':False}))
        self.assertEqual(A.normalize_memory_limit(self.spec),{'mb':64,'bytes':64*1024**2,'mode':'rlimit-as'})
        self.assertEqual(A.normalize_memory_limit({**self.spec,'memory':64.25})['mb'],65)
        explicit={}
        self.assertEqual(required_agent_capabilities({**self.spec,'required_agent_capabilities':explicit}),{'resource_limits_protocol':1})
        self.assertEqual(explicit,{})
        for value in (True,0,-1,'64',float('nan'),float('inf'),10**100):
            with self.subTest(value=value),self.assertRaises(ValueError):A.normalize_memory_limit({**self.spec,'memory':value})
        for value in ('true',1,None,{}):
            with self.subTest(enabled=value),self.assertRaises(ValueError):A.normalize_memory_limit({'enforce_memory':value})
        with self.assertRaises(ValueError):A.normalize_memory_limit({'memory_limit_mode':'auto'})
        with self.assertRaises(ValueError):A.normalize_memory_limit({**self.spec,'memory_limit_mode':'ignore'})

    def test_checked_gate_and_standalone_copy_match(self):
        self.assertEqual(inspect.getsource(A.ResourceLimitGate),inspect.getsource(ResourceLimitGate))
        for requested,effective,affinity,enforced in itertools.product((0,1,10),(0,1,11),(False,True),(False,True)):
            expected=0<effective<=requested and affinity and enforced
            self.assertEqual(A.ResourceLimitGate().allow(requested,effective,affinity,enforced),expected)

    def test_real_rlimit_blocks_allocation_and_reports_effective_affinity(self):
        marker=self.root/'payload'
        body=f'''import os,resource,sys
from pathlib import Path
Path({str(marker)!r}).touch()
assert resource.getrlimit(resource.RLIMIT_AS)==({64*1024**2},{64*1024**2})
try: allocation=bytearray(128*1024**2)
except MemoryError: sys.exit(42)
sys.exit(0)
'''
        result=self.invoke(self.spec,body)
        self.assertEqual(result.returncode,42,result.stderr)
        self.assertTrue(marker.exists())
        receipt=json.loads((self.root/'resources.json').read_text())
        self.assertTrue(receipt['ok']);self.assertEqual(receipt['method'],'rlimit-as')
        self.assertFalse(receipt['aggregate'])
        self.assertEqual(receipt['effective_bytes'],64*1024**2)
        self.assertEqual(receipt['affinity']['effective'],sorted(os.sched_getaffinity(0)))
        unlimited=subprocess.run([sys.executable,'-c','x=bytearray(128*1024**2)'],timeout=20)
        self.assertEqual(unlimited.returncode,0)

    def test_affinity_mismatch_refuses_before_payload(self):
        marker=self.root/'must-not-exist'
        (self.root/'taskset').write_text(json.dumps([max(os.sched_getaffinity(0))+1000]))
        result=self.invoke(self.spec,f'from pathlib import Path;Path({str(marker)!r}).touch()')
        self.assertEqual(result.returncode,78,result.stderr)
        self.assertFalse(marker.exists())
        self.assertFalse(json.loads((self.root/'resources.json').read_text())['ok'])

    def test_required_cgroup_failure_never_uses_rlimit_or_executes(self):
        (self.root/'spec.json').write_text(json.dumps({**self.spec,'memory_limit_mode':'cgroup-v2'}))
        with patch.object(A,'_resource_cgroup',side_effect=OSError('unavailable')), \
             patch.object(A,'_resource_rlimit') as fallback,patch.object(A.os,'execvpe') as execute:
            with self.assertRaises(SystemExit) as outcome:
                A.resource_exec(self.root/'spec.json',self.root/'resources.json',['payload'])
        self.assertEqual(outcome.exception.code,78)
        fallback.assert_not_called();execute.assert_not_called()
        self.assertFalse(json.loads((self.root/'resources.json').read_text())['ok'])

    def test_auto_falls_back_but_never_after_unverified_cgroup_join(self):
        spec={**self.spec,'memory_limit_mode':'auto'}
        returned={'method':'rlimit-as','effective_bytes':64*1024**2,'aggregate':False}
        with patch.object(A,'_resource_cgroup',side_effect=OSError('not delegated')), \
             patch.object(A,'_resource_rlimit',return_value=returned) as fallback:
            receipt=A.apply_memory_limit(self.root,spec)
            self.assertTrue(receipt['ok']);self.assertIn('not delegated',receipt['cgroup_error'])
            fallback.assert_called_once()
        with patch.object(A,'_resource_cgroup',side_effect=A.MemoryScopeUnverified('unknown')), \
             patch.object(A,'_resource_rlimit') as fallback:
            with self.assertRaises(A.MemoryScopeUnverified):A.apply_memory_limit(self.root,spec)
            fallback.assert_not_called()

    def test_lower_parent_hard_limit_is_not_relaxed(self):
        import resource
        with patch.object(resource,'getrlimit',side_effect=[(1024,1024),(1024,1024)]), \
             patch.object(resource,'setrlimit') as setlimit:
            receipt=A._resource_rlimit(2048)
        setlimit.assert_called_once_with(resource.RLIMIT_AS,(1024,1024))
        self.assertEqual(receipt['effective_bytes'],1024)

    def test_cgroup_writes_and_readback_are_private_and_not_reused(self):
        parent=self.root/'cgroups'/'supercomputer';parent.mkdir(parents=True)
        (parent.parent/'cgroup.subtree_control').write_text('memory')
        (parent/'cgroup.subtree_control').write_text('memory')
        original_read=Path.read_text;original_mkdir=Path.mkdir
        def read(path,*args,**kwargs):
            return '0::/\n' if str(path)=='/proc/self/cgroup' else original_read(path,*args,**kwargs)
        def mkdir(path,*args,**kwargs):
            original_mkdir(path,*args,**kwargs)
            if path.parent==parent and len(path.name)==64:
                for name,value in {'memory.max':'max','memory.swap.max':'max','memory.oom.group':'0','cgroup.procs':''}.items():
                    (path/name).write_text(value)
        with patch.object(A,'RESOURCE_CGROUP_ROOT',parent),patch.object(Path,'read_text',read),patch.object(Path,'mkdir',mkdir):
            receipt=A._resource_cgroup(self.root,64*1024**2)
            group=Path(receipt['cgroup'])
            self.assertEqual(group.parent,parent)
            self.assertTrue(receipt['aggregate'])
            self.assertEqual((group/'memory.max').read_text(),str(64*1024**2))
            self.assertEqual((group/'memory.swap.max').read_text(),'0')
            self.assertEqual((group/'memory.oom.group').read_text(),'1')
            with self.assertRaises(A.MemoryScopeUnverified):A._resource_cgroup(self.root,128*1024**2)
            self.assertEqual((group/'memory.max').read_text(),str(64*1024**2))

    def test_cgroup_does_not_bypass_existing_finite_memory_ancestor(self):
        parent=self.root/'cgroups'/'supercomputer';leaf=parent.parent/'leaf';leaf.mkdir(parents=True)
        (leaf/'memory.max').write_text('33554432')
        (leaf/'cgroup.subtree_control').write_text('')
        original_read=Path.read_text
        def read(path,*args,**kwargs):
            return '0::/leaf\n' if str(path)=='/proc/self/cgroup' else original_read(path,*args,**kwargs)
        with patch.object(A,'RESOURCE_CGROUP_ROOT',parent),patch.object(Path,'read_text',read):
            with self.assertRaisesRegex(ValueError,'not delegated'):A._resource_cgroup(self.root,64*1024**2)
        self.assertFalse(parent.exists())

    @unittest.skipUnless(os.environ.get('OWNED_LIFECYCLE_BOAT_TEST')=='1','real cgroup test runs on boat')
    def test_real_cgroup_enforces_aggregate_memory_and_reports_own_oom(self):
        spec={**self.spec,'memory_limit_mode':'cgroup-v2'}
        (self.root/'spec.json').write_text(json.dumps(spec))
        marker=self.root/'cgroup-payload'
        body=f'from pathlib import Path;Path({str(marker)!r}).touch();allocation=bytearray(128*1024**2)'
        cmd=['sudo','-n',sys.executable,'-I',str(self.agent),'resource-exec',str(self.root/'spec.json'),
             str(self.root/'resources.json'),'setpriv',f'--reuid={os.getuid()}',f'--regid={os.getgid()}',
             '--init-groups','--',sys.executable,'-c',body]
        result=subprocess.run(cmd,capture_output=True,text=True,timeout=20)
        receipt=json.loads((self.root/'resources.json').read_text())
        if result.returncode==78:
            self.assertFalse(marker.exists())
            self.assertFalse(receipt['ok'])
            reason=receipt.get('error','')
            allowed=('not delegated','Read-only file system','Permission denied','must not be bypassed','not visible')
            self.assertTrue(any(text in reason for text in allowed),reason)
            print('native cgroup unavailable: '+reason,flush=True)
            self.skipTest('worker cgroup unavailable: '+reason)
        self.assertIn(result.returncode,(-9,137),result.stderr)
        self.assertTrue(marker.exists());self.assertTrue(receipt['ok'])
        group=Path(receipt['cgroup'])
        try:
            self.assertEqual(receipt['effective_bytes'],64*1024**2)
            self.assertTrue(receipt['aggregate'])
            self.assertEqual((group/'memory.max').read_text().strip(),str(64*1024**2))
        finally:
            subprocess.run(['sudo','-n',sys.executable,'-I',str(self.agent),'resource-finalize',str(self.root)],
                           capture_output=True,text=True,check=True,timeout=5)
        receipt=json.loads((self.root/'resources.json').read_text())
        self.assertGreater(receipt['oom_kill'],0)
        self.assertFalse(receipt['cleanup_pending']);self.assertFalse(group.exists())

    @unittest.skipUnless(os.environ.get('OWNED_LIFECYCLE_BOAT_TEST')=='1','real namespace test runs on boat')
    def test_real_namespace_row_reports_applied_limit_and_affinity(self):
        repo='memory-test-'+self.root.name
        src=Path('/src')/repo
        subprocess.run(['sudo','-n','mkdir','-p','/work',str(src)],check=True,timeout=5)
        self.addCleanup(lambda:subprocess.run(['sudo','-n','rmdir',str(src)],check=True,timeout=5))
        root=self.root/'state';root.mkdir();shutil.copyfile(A.__file__,root/'agent.py')
        d=root/'b'/'memory';j=d/'jobs'/'job';j.mkdir(parents=True)
        (d/'tree').mkdir();(d/'ready').touch()
        expected=[min(os.sched_getaffinity(0))]
        (j/'taskset').write_text(json.dumps(expected));(j/'cpus').write_text(json.dumps(expected))
        body='import resource; assert resource.getrlimit(resource.RLIMIT_AS)[0] == 67108864; print("bounded")'
        spec={**self.spec,'name':'job','kind':'sh','cmd':f"python3 -c {__import__('shlex').quote(body)}",'cwd':'.','limit':20}
        (j/'spec.json').write_text(json.dumps(spec))
        with patch.object(A,'ROOT',root),patch.object(A,'ctx_of',return_value={'name':repo}):A.run('memory','job')
        row=json.loads((j/'done.json').read_text())
        self.assertEqual(row['code'],0,row)
        self.assertTrue(row['resource_verified'],row)
        self.assertEqual(row['resource_limits']['effective_bytes'],64*1024**2)
        self.assertEqual(row['resource_limits']['affinity']['effective'],expected)
        self.assertNotIn('oom',row)


if __name__=='__main__':unittest.main()
