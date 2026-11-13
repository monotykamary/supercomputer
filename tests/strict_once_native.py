"""One-VM, real-kernel acceptance probe; never run native work on the submit host.

Run as ONE native boat job, image sc-f0d8daeee6c3, config tests/laws.toml:
  OWNED_LIFECYCLE_BOAT_TEST=1 python3 tests/strict_once_native.py
Artifacts go to SUPERCOMPUTER_OUTPUT_DIR. No provider allocation, resume, replacement,
or stop is permitted. The outer placement receipt supplies the provider VM identity;
inside, hostname is explicitly only a same-host label, bound to the kernel boot UUID.
The private scheduler/agent use real production HTTP, leases, owned RPC, and PID1.
Only provider attachment and loopback (not TLS/boat front) transport are substituted.
"""
import base64
import contextlib
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import urllib.request

REPO = Path(__file__).resolve().parents[1]
TIMEOUT = 150


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')


def append(path, value):
    with path.open('a') as f:
        f.write(json.dumps(value, sort_keys=True) + '\n')
        f.flush()
        os.fsync(f.fileno())


def lines(path):
    return [json.loads(x) for x in path.read_text().splitlines()] if path.exists() else []


def wait_for(fn, description, seconds=20):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        value = fn()
        if value:
            return value
        time.sleep(.04)
    raise AssertionError('timed out: ' + description)


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def production():
    sys.path.insert(0, str(REPO))
    from supercomputer import agent, boat, scheduler
    return agent, boat, scheduler


def loopback_client(B, root, vm):
    """Real AgentClient framing/fencing; only HTTPS -> local HTTP is adapted."""
    class Loopback(B.AgentClient):
        def rpc(self, command, *args, **kwargs):
            # A test-only outage AFTER real terminal proof was captured. Do not
            # replace a native response: any attempted late cleanup is a failure.
            request = args[0] if command == 'owned_rpc' and args else {}
            op = request.get('command') if isinstance(request, dict) else None
            op_args = request.get('args', []) if isinstance(request, dict) else []
            if op == 'cancelitem' and op_args and (root / ('block-cleanup-' + op_args[0])).exists():
                append(root / 'blocked-cleanup-rpcs.jsonl', {'command': op, 'batch': op_args[0]})
                raise ConnectionError('native fixture: completed worker cleanup transport unavailable')
            return super().rpc(command, *args, **kwargs)

        def _get(self, timeout):
            return http.client.HTTPConnection(self.host, timeout=timeout)

        def _put_back(self, conn):
            conn.close()

    settings = json.loads((root / 'agent-endpoint.json').read_text())
    return Loopback(vm, settings['url'], settings['token'], lease=settings['lease'])


def agent_child(root, port):
    # Import the exact copied candidate source, not a replacement agent.
    spec = importlib.util.spec_from_file_location('native_candidate_agent', root / 'agent' / 'agent.py')
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    A.ROOT = root / 'agent'
    A.BLOBS, A.TREES = A.ROOT / 'blobs', A.ROOT / 'trees'
    owner, token = os.environ['STRICT_OWNER'], os.environ['STRICT_TOKEN']
    A.keys_add(owner, token)
    lease = A.lease('claim', {'owner': owner, 'vm_id': os.environ['STRICT_HOST_LABEL']})
    if lease.get('ok') is not True:
        raise AssertionError(lease)
    (A.ROOT / 'token').write_text(token)
    dump(root / 'agent-endpoint.json', {'url': f'http://127.0.0.1:{port}', 'token': token, 'lease': lease})
    # Observe REAL physical Popen calls and REAL namespace ownership after start.
    real_popen, real_start = A.subprocess.Popen, A.start

    def popen(argv, *args, **kw):
        proc = real_popen(argv, *args, **kw)
        if isinstance(argv, list) and 'ownedrun' in argv:
            append(root / 'physical-launches.jsonl', {'pid': proc.pid, 'argv': argv,
                   'boot': A._lease_boot(), 'time': time.time()})
        return proc

    def start(directory, item):
        answer = real_start(directory, item)
        key = (directory.name, item['name'])
        entry = A._owned_children.get(key)
        append(root / 'start-observations.jsonl', {'batch': directory.name, 'spec': item,
            'record': json.loads(A._owned_path(*key).read_text()),
            'contained': bool(entry and entry.get('contained')),
            'init': entry.get('init') if entry else None,
            'supervisor': entry.get('supervisor') if entry else None})
        return answer

    A.subprocess.Popen, A.start = popen, start
    A.daemon(str(port))


def install_private_backend(B, S, root):
    """Register precisely this pre-existing host. All provider mutators fail closed."""
    vm_id = os.environ['STRICT_HOST_LABEL']
    B.owner_id = lambda: os.environ['STRICT_OWNER']
    client = loopback_client(B, root, vm_id)
    settings = json.loads((root / 'agent-endpoint.json').read_text())

    def forbidden(*args, **kw):
        append(root / 'forbidden-provider.jsonl', {'args': repr(args)[:400]})
        raise AssertionError('private acceptance backend forbids provider allocation/start/stop/replacement')

    for name in ('boat', 'exec_', 'create_vm', 'start_vm', 'stop_vm', 'delete_vm',
                 'shrink_ttl', 'ensure_image', 'prepare_agent', 'start_agent'):
        if hasattr(B, name):
            setattr(B, name, forbidden)
    B.agent_client = lambda vid: client if vid == vm_id else forbidden(vid)
    B.states = lambda *a, **kw: {vm_id: {'state': 'running'}}
    B.sandboxes = lambda *a, **kw: {vm_id: {'state': 'running'}}

    def lease_vm(vid, op='claim', lease=None, **kw):
        if vid != vm_id or op not in ('renew', 'who'):
            return forbidden(vid, op)
        return client.rpc('lease', op, {**(lease or {}), 'owner': B.owner_id(), 'vm_id': vid})

    B.lease_vm = lease_vm
    # These are provider pool lifecycle operations, not scheduling/owned execution.
    S.Scheduler._adopt = lambda self: setattr(self, 'adopting', False)
    S.Scheduler._release_idle = lambda self: None
    S.Scheduler._let_go = lambda self, *a, **kw: None
    S.Scheduler._acquire = lambda self, *a, **kw: forbidden(*a, **kw)
    S.Scheduler._create = lambda self, *a, **kw: forbidden(*a, **kw)
    S.Scheduler._billing = lambda self: None
    S.Scheduler._heartbeat = lambda self: self.stop.wait(TIMEOUT)
    original = S.Scheduler.__init__

    def init(self, *args, **kw):
        original(self, *args, **kw)
        vm = S.VMState(vm_id, os.environ['SUPERCOMPUTER_IMAGE'], 'large', 'warm')
        vm.lease = settings['lease']
        vm.client = S.SchedulerAgentClient(client, self, vm)
        reply = vm.client.ping()
        if reply['boot'] != os.environ['STRICT_BOOT']:
            raise AssertionError('agent not on same kernel boot')
        vm.agent = reply['version']
        vm.agent_capabilities = reply['agent_capabilities']
        vm.ready, vm.t_ready, vm.t_renew = True, time.time(), time.monotonic()
        vm.t_agent, vm.t_inventory = time.time(), time.time()
        self.vms[vm_id] = vm
        with B.Registry().locked() as registry:
            registry['vms'][vm_id] = {'snapshot': vm.image, 'type': vm.type, 'state': 'running',
                'lease': {'pid': os.getpid(), 'host': socket.gethostname(), 'batch': 'scheduler', 'owner': B.owner_id()}}
        self.adopting = False
        self._worker(vm)
        self._refresh_status(blocking=True)
        dump(root / 'attachment.json', {'host_label': vm_id, 'kernel_boot': reply['boot'],
             'lease': vm.lease, 'agent_version': vm.agent, 'scheduler_code': self.code,
             'registered_vms': list(self.vms)})

    S.Scheduler.__init__ = init


def scheduler_child(root):
    A, B, S = production()
    install_private_backend(B, S, root)
    S.serve(idle_exit=0)


def main():
    if os.environ.get('OWNED_LIFECYCLE_BOAT_TEST') != '1':
        raise SystemExit('Refusing native execution: submit ONE boat job with OWNED_LIFECYCLE_BOAT_TEST=1')
    if not sys.platform.startswith('linux') or not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise SystemExit('Linux /proc, pidfds, sudo and unshare are required; never skip')
    output = Path(os.environ['SUPERCOMPUTER_OUTPUT_DIR']) / 'strict-once-native'
    output.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='strict-once-native-', dir='/var/tmp'))
    receipt = {'schema': 1, 'ok': False, 'runtime_verified': False, 'assertions': [],
        'limitations': ['single real VM; no provider creation or restart',
            'loopback HTTP replaces boat front/TLS only',
            'hostname is a host label, NOT a provider VM ID; bind via outer placement.json',
            'reconnect is client transport reconnect, not controller/agent process restart',
            'no involuntary VM loss; cancellation tests real PID1 and detached descendant teardown',
            'runtime_verified applies only to the exact hashes in this receipt']}
    processes, waiters, sessions, clients, descendant_fds = [], [], [], [], []
    session_threads = []
    def deadline(_sig, _frame):
        raise TimeoutError('native probe exceeded its bounded 150 second deadline')
    old_alarm = signal.signal(signal.SIGALRM, deadline)
    signal.alarm(TIMEOUT)
    errors = []
    start_time = time.monotonic()
    original_env = dict(os.environ)

    def check(name, condition, evidence=None):
        receipt['assertions'].append({'name': name, 'ok': bool(condition), 'evidence': evidence})
        if not condition:
            raise AssertionError(name + ': ' + repr(evidence)[:1200])

    try:
        subprocess.run(['sudo', '-n', 'true'], check=True, timeout=5)
        for tool in ('sudo', 'unshare', 'setpriv', 'taskset'):
            check('prerequisite:' + tool, bool(shutil.which(tool)))
        boot = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        nonce = secrets.token_hex(16)
        label = 'same-host:' + socket.gethostname()
        receipt.update(kernel_boot=boot, host_label=label, run_nonce=nonce,
                       outer_provider_vm_id=os.environ.get('STRICT_ONCE_PROVIDER_VM_ID'),
                       outer_native_pid_namespace=os.stat('/proc/self/ns/pid').st_ino)
        # A unique project name avoids modifying /src or /work entries of the outer job.
        project = 'strict-native-' + nonce[:12]
        tree = root / 'tree'
        tree.mkdir()
        # Public Tree intentionally requires a real repository for git ls-files.
        # A new isolated repo avoids reading or modifying the outer checkout.
        subprocess.run(['git', 'init', '-q', str(tree)], check=True, timeout=5)
        config = root / 'project.toml'
        config.write_text('name = ' + json.dumps(project) + '\n[boat]\nvm_type = "large"\n')
        (root / 'agent').mkdir()
        shutil.copyfile(REPO / 'supercomputer/agent.py', root / 'agent/agent.py')
        aport, sport = free_port(), free_port()
        os.environ.update(SUPERCOMPUTER_HOME=str(root / 'home'), SUPERCOMPUTER_LIVE=str(root / 'live'),
            SUPERCOMPUTER_SRC=str(REPO), SUPERCOMPUTER_CONFIG=str(config), SUPERCOMPUTER_CONTROLLER='0',
            SUPERCOMPUTER_LOCAL='1', SUPERCOMPUTER_SCHEDULER_PORT=str(sport), SUPERCOMPUTER_SCHEDULER='1',
            SUPERCOMPUTER_CLIENT='strict-native-' + nonce, SUPERCOMPUTER_IMAGE='sc-f0d8daeee6c3',
            STRICT_OWNER='strict-native-' + nonce, STRICT_TOKEN=secrets.token_urlsafe(32),
            STRICT_HOST_LABEL=label, STRICT_BOOT=boot, BOAT_BIN='/bin/false')
        for key in ('SUPERCOMPUTER_HANDOFF_FDS', 'NOTIFY_SOCKET'):
            os.environ.pop(key, None)
        A, B, S = production()
        B.owner_id = lambda: os.environ['STRICT_OWNER']
        from supercomputer import config as C
        C.use(tree)
        receipt['candidate'] = {'agent_version': A.VERSION, 'scheduler_code': S.code_hash(REPO / 'supercomputer'),
             'sha256': {str(p.relative_to(REPO)): hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in [REPO / 'supercomputer/agent.py', REPO / 'supercomputer/scheduler.py',
                                  REPO / 'supercomputer/boat.py', Path(__file__).resolve()]}}

        def spawn(mode, *args):
            log = (root / (mode + '.log')).open('w')
            proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), mode, str(root), *map(str, args)],
                                    stdout=log, stderr=log, env=os.environ.copy(), cwd=root)
            log.close()
            processes.append(proc)
            return proc

        spawn('--agent', aport)
        wait_for(lambda: (root / 'agent-endpoint.json').exists(), 'real agent lease')
        cl = loopback_client(B, root, label)
        wait_for(lambda: health(aport), 'agent HTTP ready')
        ping = cl.ping()
        check('agent boot and candidate source', ping['boot'] == boot and ping['version'] == A.VERSION, ping)
        spawn('--scheduler')
        wait_for(lambda: health(sport), 'private scheduler HTTP ready')
        attach = json.loads((root / 'attachment.json').read_text())
        check('exactly one same-host registered VM', attach['registered_vms'] == [label], attach)
        check('lease binds real kernel boot', attach['lease'].get('boot') == boot, attach['lease'])

        class RecordingClient(S.Client):
            def __init__(self):
                super().__init__()
                self.submits, self.polls, self.dropped = 0, 0, False
                self.poll_dropped = False
                self.close_receipt = None
            def req(self, path, msg, *args, **kw):
                result = super().req(path, msg, *args, **kw)
                if path == '/submit':
                    self.submits += 1
                    if not self.dropped:
                        self.dropped = True
                        for conn in getattr(self, '_conns', []):
                            conn.close()
                        self._conns = []
                        append(root / 'client-events.jsonl', {'event': 'discarded real submit reply', 'batch': msg['batch']})
                        raise ConnectionResetError('probe discarded committed submit reply')
                if path == '/poll':
                    self.polls += 1
                    if not self.poll_dropped:
                        self.poll_dropped = True
                        for conn in getattr(self, '_conns', []):
                            conn.close()
                        self._conns = []
                        append(root / 'client-events.jsonl', {'event': 'discarded real poll reply', 'batch': msg['batch']})
                        raise ConnectionResetError('probe reconnects after real poll reply')
                if path == '/close':
                    self.close_receipt = result
                return result

        def launch(name, linger):
            marker = root / (name + '-markers.jsonl')
            gate = root / (name + '-finish')
            # Grandchild detaches from both process group and Python parent. Namespace
            # PID1 teardown, not group kill or a fake reaped wrapper, must remove it.
            script = ('import os,time,json,pathlib\n'
                'def event(role):\n'
                f' with open({str(marker)!r},"a") as f:\n'
                '  f.write(json.dumps({"role":role,"pid":os.getpid(),"ppid":os.getppid(),'
                '"pidns":os.stat("/proc/self/ns/pid").st_ino,"boot":pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip()})+"\\n");f.flush();os.fsync(f.fileno())\n'
                'event("payload")\n'
                + ('if os.fork()==0:\n os.setsid()\n if os.fork()==0:\n  event("detached")\n  time.sleep(120)\n os._exit(0)\n' if linger else '')
                + f'while not pathlib.Path({str(gate)!r}).exists(): time.sleep(.03)\n')
            (tree / (name + '.py')).write_text(script)
            before_threads = set(threading.enumerate())
            session = B.session('native-' + name, strict_once=True, max_attempts=1, klass='gate',
                                root=tree, build=False, caches=False)
            sessions.append(session)
            session_threads.extend(t for t in threading.enumerate() if t not in before_threads)
            client = RecordingClient()
            clients.append(client)
            session.client = client
            rows, faults = [], []
            def run():
                try:
                    session.run([{'name': name, 'kind': 'sh', 'cmd': 'python3 ' + name + '.py',
                                  'limit': 120, 'cpus': 1}], lambda _, row: rows.append(row), max_wait=45,
                                on_lost=lambda *_: faults.append('strict callback requested fallback'))
                except BaseException as e:
                    faults.append(type(e).__name__ + ': ' + str(e))
            waiter = threading.Thread(target=run, name='strict-native-waiter-' + name)
            waiters.append(waiter)
            waiter.start()
            wait_for(lambda: marker.exists() and len(lines(marker)) >= (2 if linger else 1), name + ' kernel payload marker', 45)
            obs = wait_for(lambda: [x for x in lines(root / 'start-observations.jsonl') if x['batch'] == session.batch], name + ' owned launch')[0]
            events = lines(marker)
            check(name + ': one payload marker', sum(x['role'] == 'payload' for x in events) == 1, events)
            check(name + ': real nested PID namespace', all(x['boot'] == boot and x['pidns'] != receipt['outer_native_pid_namespace'] for x in events), events)
            check(name + ': bound init and supervisor', obs['contained'] and obs['init']['pidns'] == events[0]['pidns'] and obs['init']['boot'] == boot, obs)
            snapshot = S.Client().req('/snapshot', {'batch': session.batch})
            check(name + ': one durable placement', len(snapshot['placements']) == 1, snapshot)
            placement = snapshot['placements'][0]
            check(name + ': placement identity', placement['vm'] == label and placement['boot_id'] == boot and placement['attempt'] == 1 and placement['identity'] == obs['spec']['launch_identity'], placement)
            # Fresh transport, same owned attempt. Also test wire nonce replay refusal.
            fresh = loopback_client(B, root, label)
            wire = {'protocol': 1, 'version': A.VERSION, 'nonce': secrets.token_hex(16), 'command': 'vtick',
                    'args': [{'start': [{'batch': session.batch, 'spec': obs['spec']}], 'wait': 0}]}
            accepted = fresh.rpc('owned_rpc', wire)
            rejected = fresh.rpc('owned_rpc', wire)
            check(name + ': exact start replays idempotently', accepted.get('ok') is True, accepted)
            check(name + ': reused RPC nonce rejected', rejected.get('ok') is False, rejected)
            physical = [x for x in lines(root / 'physical-launches.jsonl') if session.batch in x['argv']]
            check(name + ': one physical launch after replay', len(physical) == 1, physical)
            receipt.setdefault('scenarios', {})[name] = {'batch': session.batch, 'placement': placement,
                'observation': obs, 'markers': events, 'physical_launches': physical,
                'wire_nonce': wire['nonce'], 'replay_refusal': rejected}
            return session, client, rows, faults, waiter, marker, gate, obs

        success = launch('success', False)
        success[6].touch()
        success[4].join(25)
        check('success waiter completed', not success[4].is_alive(), success[3])
        check('success exactly one terminal row', len(success[2]) == 1 and success[2][0]['code'] == 0, success[2])
        check('success no fallback', not success[3], success[3])
        completed = S.Client().req('/snapshot', {'batch': success[0].batch})
        terminal = [r for r in completed['cancellation_receipts'] if r.get('phase') == 'terminal']
        placement = completed['placements'][0]
        check('success native reap persisted before close', len(terminal) == 1 and
              terminal[0].get('matched') is True and terminal[0]['receipt'].get('reaped') is True and
              terminal[0]['receipt'].get('namespace_absent') is True and
              all(terminal[0].get(k) == placement.get(k) for k in
                  ('run_name', 'vm', 'boot_id', 'attempt', 'identity')), terminal)
        blocked = root / ('block-cleanup-' + success[0].batch)
        blocked.touch()
        try:
            closed = success[0].close()
        finally:
            blocked.unlink()
        check('success close proves cleanup', closed.get('cleanup_complete') is True, closed)
        check('success close reused proof without contacting completed worker',
              not lines(root / 'blocked-cleanup-rpcs.jsonl'))
        after = S.Client().req('/snapshot', {'batch': success[0].batch})
        check('success retained native proof source', any(r.get('source') == 'terminal-reap'
              for r in after['cancellation_receipts']), after['cancellation_receipts'])
        receipt['scenarios']['success']['terminal_reap'] = terminal[0]
        check('ambiguous ACK never resubmitted', success[1].submits == 1 and success[1].polls > 0, {'submits': success[1].submits, 'polls': success[1].polls})
        custody = closed.get('custody', {}).get(success[0].batch, {})
        check('reconnect custody recorded', custody.get('reconnect_count', 0) >= 1, custody)
        receipt['scenarios']['success'].update(rows=success[2], close=closed, raw_close=success[1].close_receipt)
        cancelled = launch('cancel', True)
        init_pid = cancelled[7]['init']['pid']
        # Host PIDs of the actual owned namespace, opened before cancellation.
        owned_pids = [init_pid, *A.descendants(init_pid)]
        for pid in owned_pids:
            try:
                descendant_fds.append((pid, os.pidfd_open(pid)))
            except ProcessLookupError:
                pass
        check('observed actual namespace descendants', len(descendant_fds) >= 3, owned_pids)
        close = cancelled[0].close()
        cancelled[4].join(20)
        check('cancel waiter completed', not cancelled[4].is_alive(), cancelled[3])
        check('cancel close synchronously reaps', close.get('cleanup_complete') is True, close)
        import select
        check('all observed namespace PIDs exited', all(select.select([fd], [], [], 0)[0] for _, fd in descendant_fds), [p for p, _ in descendant_fds])
        check('cancel one marker despite replay', sum(x['role'] == 'payload' for x in lines(cancelled[5])) == 1, lines(cancelled[5]))
        snapshot = S.Client().req('/snapshot', {'batch': cancelled[0].batch})
        check('closed admission retains placement and reap receipts', snapshot.get('admission_closed') is True and len(snapshot['placements']) == 1 and bool(snapshot['cancellation_receipts']), snapshot)
        check('cancellation never requests retry', all(not r.get('lost') for r in snapshot['rows']), snapshot['rows'])
        receipt['scenarios']['cancel'].update(rows=snapshot['rows'], close=close, raw_close=cancelled[1].close_receipt,
                                            after=snapshot, observed_host_pids=owned_pids)
        check('no forbidden provider calls', not lines(root / 'forbidden-provider.jsonl'))
        check('two scenarios each launched once', len(lines(root / 'physical-launches.jsonl')) == 2, lines(root / 'physical-launches.jsonl'))
        receipt['payload_marker_count'] = sum(sum(e['role'] == 'payload' for e in lines(root / (n + '-markers.jsonl'))) for n in ('success', 'cancel'))
        receipt['ok'] = True
    except BaseException:
        receipt['error'] = traceback.format_exc()
    finally:
        signal.alarm(0)
        # Exactly these admissions, these child handles, and these pidfds only.
        for session in sessions:
            try:
                if not session.stop.is_set():
                    session.close()
            except BaseException as e:
                errors.append('session cleanup: ' + repr(e))
        for waiter in waiters:
            waiter.join(10)
            if waiter.is_alive():
                errors.append('waiter survived: ' + waiter.name)
        for thread in session_threads:
            thread.join(5)
            if thread.is_alive():
                errors.append('session-owned thread survived: ' + thread.name)
        for client in clients:
            for conn in getattr(client, '_conns', []):
                conn.close()
        for pid, fd in descendant_fds:
            os.close(fd)
        # A failed assertion or controller close must not strand an owned PID1.
        # The daemon still has the original child handles; ask it first.
        if 'cl' in locals():
            owned_records = []
            for path in (root / 'agent/owned-items').glob('*/*.json'):
                try:
                    record = json.loads(path.read_text())
                    owned_records.append({'batch': path.parent.name,
                        'spec': {'name': path.stem, 'launch_identity': record['identity']},
                        'init': record.get('init')})
                except (ValueError, KeyError) as exc:
                    errors.append('unreadable owned cleanup identity: ' + repr(exc))
            for obs in owned_records:
                try:
                    result = cl.rpc('cancelitem', obs['batch'], obs['spec']['name'], 5, obs['spec']['launch_identity'], timeout=8)
                    if result.get('ok') is not True:
                        errors.append('emergency cancel unproven: ' + repr(result))
                except BaseException as exc:
                    errors.append('emergency cancel failed: ' + repr(exc))
                    init = obs.get('init')
                    if init:
                        with contextlib.suppress(OSError):
                            fd = os.pidfd_open(init['pid'])
                            try:
                                if A._namespace_birth(init['pid']) == init:
                                    signal.pidfd_send_signal(fd, signal.SIGKILL)
                            finally:
                                os.close(fd)
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.kill()  # exact Popen handle; kills all its Python threads too
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                errors.append('child survived: ' + str(proc.pid))
        receipt['cleanup_errors'] = errors
        receipt['children'] = [{'pid': p.pid, 'exit': p.returncode} for p in processes]
        receipt['elapsed_s'] = round(time.monotonic() - start_time, 3)
        receipt['ok'] = receipt['ok'] and not errors
        receipt['runtime_verified'] = receipt['ok']
        # Do not publish endpoint credentials or context containing secrets.
        for path in root.glob('*.log'):
            shutil.copyfile(path, output / path.name)
        for name in ('physical-launches.jsonl', 'start-observations.jsonl', 'client-events.jsonl', 'forbidden-provider.jsonl'):
            if (root / name).exists():
                shutil.copyfile(root / name, output / name)
        q = root / 'live/scheduler/queue'
        if q.exists():
            for suffix in ('placed', 'cancel', 'rows'):
                for path in q.glob('*.' + suffix):
                    shutil.copyfile(path, output / path.name)
        custody_dir = root / 'live/custody'
        if custody_dir.exists():
            shutil.copytree(custody_dir, output / 'custody', dirs_exist_ok=True)
        # Remove only the uniquely named mount placeholders made by candidate prep.
        if 'project' in locals():
            for base in ('/src', '/work'):
                path = Path(base) / project
                if path.exists():
                    result = subprocess.run(['sudo', '-n', 'rmdir', str(path)], capture_output=True, timeout=5)
                    if result.returncode:
                        errors.append('project placeholder cleanup: ' + str(path))
        receipt['ok'] = receipt['ok'] and not errors
        receipt['runtime_verified'] = receipt['ok']
        dump(output / 'receipt.json', receipt)
        print(json.dumps(receipt, sort_keys=True), flush=True)
        shutil.rmtree(root)
        os.environ.clear()
        os.environ.update(original_env)
        signal.signal(signal.SIGALRM, old_alarm)
    return 0 if receipt['ok'] and not errors else 1


def health(port):
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}/health', timeout=.5) as r:
            return json.load(r).get('ok', True)
    except (OSError, ValueError):
        return False


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--agent':
        agent_child(Path(sys.argv[2]), int(sys.argv[3]))
    elif len(sys.argv) > 1 and sys.argv[1] == '--scheduler':
        scheduler_child(Path(sys.argv[2]))
    else:
        raise SystemExit(main())
