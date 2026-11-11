"""A running client must outlive removal/replacement of its installed package.

Each case boots a fresh interpreter against a private package copy. The parent
removes that copy only after the child has submitted its simulated batch. No
real scheduler, VM, forward, queue, or network is used. Run via supercomputer.
"""
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


def child(site, work, scenario):
    # -I prevents either the checkout or another installed version masking the
    # removed package; only this explicitly selected copy can supply modules.
    sys.path.insert(0, site)
    import base64
    import hashlib
    import io
    import threading
    from types import SimpleNamespace
    from supercomputer import boat

    assert Path(boat.__file__).parent == Path(site) / 'supercomputer'
    work = Path(work)
    blobs = {}
    reads = []
    payload = b'{"value":"' + b'x' * 700000 + b'"}'
    artifact = bytes(range(256)) * 2100

    def ref(data, format='json'):
        value = dict(version=1, sha256=hashlib.sha256(data).hexdigest(),
                     bytes=len(data), format=format)
        blobs[value['sha256']] = data
        return value

    manifest = ref(json.dumps([dict(path='nested/data.bin', **ref(artifact, 'file'))]).encode())
    row = dict(name='first', result_ref=ref(payload), row_ref=ref(payload),
               output_files_ref=manifest)

    def read(value, offset, count):
        assert 0 < count <= 256 * 1024
        reads.append((offset, count))
        return blobs[value['sha256']][offset:offset + count]

    def reinstall():
        print('SUBMITTED', flush=True)
        assert sys.stdin.readline().strip() == 'continue'
        # Nothing remaining on disk can supply a missing old module.
        assert not (Path(site) / 'supercomputer' / 'boat.py').exists()

    def verify(out):
        assert Path(out['result_file']).read_bytes() == payload
        assert Path(out['row_file']).read_bytes() == payload
        assert (Path(out['output_files_dir']) / 'nested/data.bin').read_bytes() == artifact
        assert out['result_ref'] == row['result_ref']
        assert Path(out['output_files_manifest']).read_bytes() == blobs[manifest['sha256']]

    if scenario == 'scheduled':
        # Scheduled normally imports scheduler during construction, before
        # submission. Skip constructor I/O, not the production run/download loop.
        from supercomputer import scheduler
        counts = dict(submit=0, poll=0, reconnect=0, close=0)
        class Client:
            remote = True
            def push_tree(self, tree): pass
            def push_caches(self, caches, outputs): pass
            def _known(self): return {'trees': ['tree']}
            def req(self, path, msg, **kw):
                assert msg.get('batch', 'existing') == 'existing'
                if path == '/submit':
                    counts['submit'] += 1
                    return {'ok': True}
                if path == '/poll':
                    counts['poll'] += 1
                    if counts['poll'] == 1:
                        reinstall()
                        raise OSError('simulated controller handoff during reinstall')
                    assert msg['since'] == 0
                    return {'rows': [row, {'name': 'second', 'result': 2}], 'done': True}
                if path == '/results':
                    assert kw['raw'] is True
                    return read(msg['ref'], msg['offset'], msg['length'])
                if path == '/close':
                    counts['close'] += 1
                    return {}
                raise AssertionError(path)
        client = Client()
        def reconnect():
            counts['reconnect'] += 1
            return client
        scheduler._remote = reconnect
        boat.time.sleep = lambda seconds: None
        session = object.__new__(boat.Scheduled)
        session.batch, session.label = 'existing', 'install-regression'
        session.sched, session.client, session.klass = scheduler, client, 1
        session.tree = SimpleNamespace(root=work, files={}, links={}, id='tree')
        session.proj = SimpleNamespace(name='test', cache_outputs=lambda c: [])
        session.caches, session.bids = [], []
        session.image, session.vm_type, session.ctx_b64 = 'test', 'small', ''
        session.warm_only, session.submitted = False, False
        session._bid_lock, session.stop = threading.Lock(), threading.Event()
        session.vm_ids, session.run_names = set(), {}
        session.output_dir = work / 'scheduled'
        outputs = []
        session.run([{'name': 'first'}, {'name': 'second'}],
                    lambda item, value: outputs.append(value))
        verify(outputs[0])
        assert outputs[1] == {'name': 'second', 'result': 2}
        # A retry/read must never resubmit an already-owned batch.
        assert counts == dict(submit=1, poll=2, reconnect=1, close=0), counts
        session.close()
        assert counts['close'] == 1
    else:
        reinstall()
        session = object.__new__(boat.Session)
        session.batch, session.output_dir = 'existing', work / scenario
        agent = object.__new__(boat.AgentClient)
        agent.vm, agent.owner, agent.lease, agent.token = 'fake-vm', 'owner', {}, 'test-token'
        # Exercise the real legacy streaming decoder and its exec recovery path,
        # not only the top-level downloader. Its store dependency is first used
        # after the package disappears.
        from supercomputer.resultfiles import Store
        agent.result_store = lambda bid: Store(work / 'legacy')
        response = json.dumps({'done': [dict(name='legacy', batch='existing', result='y' * 700000)]}).encode()
        relay_ops = []
        def remote(vm, argv, timeout):
            op, path = argv[3:5]
            relay_ops.append(op)
            if op == 'post':
                text = json.dumps({'path': path, 'bytes': len(response), 'status': 200})
            elif op == 'read':
                size, offset, count = map(int, argv[5:8])
                assert size == len(response) and count <= 65536
                text = base64.b64encode(response[offset:offset + count]).decode()
            elif op == 'clean':
                text = ''
            else:
                raise AssertionError(op)
            return {'exitCode': 0, 'stdout': text, 'stderr': ''}
        boat.exec_ = remote
        def request(method, path, body, timeout, **kw):
            command = json.loads(body)['cmd']
            if command == 'resultpart':
                bid, name, sha, offset, count = json.loads(body)['args'][:5]
                data = read({'sha256': sha}, offset, count)
                return 200, json.dumps(dict(offset=offset, bytes=len(blobs[sha]), sha256=sha,
                                           data=base64.b64encode(data).decode())).encode(), ''
            if scenario == 'exec-recovery':
                raise boat.AgentError('simulated HTTP transport failure')
            return 200, kw['decode'](io.BytesIO(response)), ''
        agent._req = request
        legacy = agent.rpc('vtick', 'existing')
        assert 'row_ref' in legacy['done'][0] or 'result_ref' in legacy['done'][0]
        if scenario == 'exec-recovery':
            assert relay_ops[0] == 'post' and relay_ops[-1] == 'clean'
            assert relay_ops.count('post') == 1
        else:
            assert not relay_ops
        verify(session._download_results(row, lambda value, offset, count:
            session._result_chunk(agent, 'existing', row['name'], value, offset, count)))
    assert len(reads) > 6
    print(json.dumps({'ok': True, 'scenario': scenario, 'reads': len(reads)}), flush=True)


class ClientInstallTests(unittest.TestCase):
    def run_case(self, replacement, scenario):
        with tempfile.TemporaryDirectory(prefix='sc-client-install-') as tmp:
            root = Path(tmp)
            site = root / 'site'
            package = site / 'supercomputer'
            shutil.copytree(ROOT / 'supercomputer', package,
                            ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
            env = {**os.environ, 'SUPERCOMPUTER_HOME': str(root / 'home'),
                   'SUPERCOMPUTER_LOCAL': '1', 'SUPERCOMPUTER_CONTROLLER': '0'}
            env.pop('PYTHONPATH', None)
            process = subprocess.Popen(
                [sys.executable, '-I', str(Path(__file__).resolve()), '--child',
                 str(site), str(root / 'work'), scenario], cwd=root, env=env,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True)
            try:
                with selectors.DefaultSelector() as ready:
                    ready.register(process.stdout, selectors.EVENT_READ)
                    self.assertTrue(ready.select(20), 'client never submitted')
                self.assertEqual(process.stdout.readline().strip(), 'SUBMITTED')
                shutil.rmtree(package)
                if replacement:
                    package.mkdir()
                    # Poison every relevant replacement: accidentally importing
                    # a new module cannot quietly pass as a successful old run.
                    for name in ('__init__', 'clientresults', 'resultfiles', 'resultjson', 'resultrelay'):
                        (package / (name + '.py')).write_text(
                            'raise AssertionError("replacement package was imported")\n')
                stdout, stderr = process.communicate('continue\n', timeout=30)
                self.assertEqual(process.returncode, 0, stdout + stderr)
                receipt = json.loads(stdout.strip().splitlines()[-1])
                self.assertTrue(receipt['ok'])
                self.assertEqual(receipt['scenario'], scenario)
            finally:
                if process.poll() is None:
                    process.kill()  # exact owned child only
                process.communicate(timeout=5)

    def test_scheduled_package_removed(self): self.run_case(False, 'scheduled')
    def test_scheduled_package_replaced(self): self.run_case(True, 'scheduled')
    def test_direct_package_removed(self): self.run_case(False, 'direct')
    def test_direct_package_replaced(self): self.run_case(True, 'direct')
    def test_exec_recovery_package_removed(self): self.run_case(False, 'exec-recovery')
    def test_exec_recovery_package_replaced(self): self.run_case(True, 'exec-recovery')


if __name__ == '__main__':
    if sys.argv[1:2] == ['--child']:
        child(*sys.argv[2:])
    else:
        unittest.main()
