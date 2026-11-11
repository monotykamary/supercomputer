#!/usr/bin/env python3
"""Native real-state inherited-listener gate; never uses the fixture's providers.

Run ONLY as a finite supercomputer job (not on a controller):
  REAL_QUEUE_NATIVE=1 python3 tests/real_queue_handoff.py --data realqueue \
      --output realqueue-report.json
Ship the two archives with `supercomputer run ... --data /tmp/sc-realqueue-data:realqueue`.
`--data /tmp/sc-realqueue-data:realqueue` is also accepted locally as a source spec.
The directory contains queue.tar.gz and parent-f1b4.tar. --synthetic substitutes
small generated admissions for queue.tar.gz, but still tests the archived parent.

This deliberately measures real retained-state startup/HTTP/handoff with ZERO
backend VMs, not a 155-VM registry or real agent/network reconciliation. Only
isolated copies receive the hooks below. No live configuration or credentials
are passed to any subprocess. No queued command is ever executed.
"""
import argparse
import contextlib
import hashlib
import json
import os
import posixpath
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request

# Appended before either Scheduler is instantiated; not installed into the repo.
FIXTURE = r'''
# real_queue_handoff.py: PRIVATE ZERO-BACKEND FIXTURE
import hashlib as _fixture_hashlib
_fixture_root = pathlib.Path(os.environ['SC_REALQUEUE_ROOT'])
def _fixture_event(kind, **extra):
    row = dict(kind=kind, pid=os.getpid(), at=time.time(), mono=time.monotonic(), **extra)
    with (_fixture_root / 'events.jsonl').open('a') as f:
        f.write(json.dumps(row, separators=(',', ':')) + '\n')
def _fixture_forbidden(*a, **kw):
    import traceback as _fixture_traceback
    _fixture_event('FORBIDDEN', args=repr(a)[:200], stack=_fixture_traceback.format_stack(limit=8))
    raise AssertionError('real-queue fixture prohibits backend access/job execution')
def _fixture_noop(*a, **kw):
    return None
boat.owner_id = lambda: 'isolated-realqueue-fixture'
boat.states = lambda *a, **kw: {}
boat.sandboxes = lambda *a, **kw: {}
for _name in ('boat', 'create', 'agent_client', 'agent_health', 'lease_vm', 'wait_running'):
    setattr(boat, _name, _fixture_forbidden)
# Deny non-loopback networking even if a new code path misses a provider stub.
_fixture_connect = socket.socket.connect
_fixture_connect_ex = socket.socket.connect_ex
def _fixture_address(address):
    if not isinstance(address, tuple) or address[0] not in ('127.0.0.1', '::1', 'localhost'):
        _fixture_forbidden('network', address)
def _fixture_local_connect(self, address):
    _fixture_address(address)
    return _fixture_connect(self, address)
def _fixture_local_connect_ex(self, address):
    _fixture_address(address)
    return _fixture_connect_ex(self, address)
socket.socket.connect = _fixture_local_connect
socket.socket.connect_ex = _fixture_local_connect_ex
_fixture_popen = subprocess.Popen
def _fixture_spawn(argv, *a, **kw):
    if (not isinstance(argv, (list, tuple)) or not argv or argv[0] != sys.executable
            or kw.get('shell')):
        _fixture_forbidden('subprocess', argv)
    child = _fixture_popen(argv, *a, **kw)
    _fixture_event('child', child_pid=child.pid)
    return child
subprocess.Popen = _fixture_spawn
for _name in ('_worker', '_adopt', '_reattach', '_reattach_vm', '_heartbeat', '_watch', '_blob_cache_tick', '_billing'):
    setattr(Scheduler, _name, _fixture_noop)
for _name in ('_start_on', '_place', '_place_locked', '_create', '_prepare', '_modal'):
    setattr(Scheduler, _name, _fixture_forbidden)
# Keep the real run/activation/sampler/HTTP/handoff paths. Only placement and
# stale-client retirement are disabled: this snapshot has intentionally no clients.
def _fixture_schedule(self, *a, **kw):
    for b in self.batches.values():
        b.t_poll = time.time()
Scheduler.schedule = _fixture_schedule
Scheduler.close = _fixture_forbidden
_fixture_status = Scheduler._status_read
def _fixture_status_read(self):
    result = _fixture_status(self)
    inv = {}
    for bid, b in self.batches.items():
        names = json.dumps(sorted(b.items), separators=(',', ':')).encode()
        rows = json.dumps([r.get('name') for r in b.rows], separators=(',', ':')).encode()
        inv[bid] = dict(items=len(b.items), rows=len(b.rows),
                        item_names=_fixture_hashlib.sha256(names).hexdigest(),
                        row_names=_fixture_hashlib.sha256(rows).hexdigest())
    result['realqueue_fixture'] = dict(inventory=inv,
        recovery_errors=getattr(self, 'recovery_errors', {}),
        compact_pending=sorted(getattr(self, '_journal_compact_pending', [])),
        compact_errors=getattr(self, '_journal_compaction_errors', {}),
        reload_ms=getattr(self, '_reload_ms', None),
        journal_cache_hits=sum(int(r.get('cache_used', False)) for r in getattr(self, '_journal_load_receipts', {}).values()),
        inherited=getattr(self, '_fixture_inherited', False))
    return result
Scheduler._status_read = _fixture_status_read
_fixture_init = Scheduler.__init__
def _fixture_initialize(self, *a, **kw):
    inherited = bool(os.environ.get('SUPERCOMPUTER_HANDOFF_FDS'))
    _fixture_event('init', inherited=inherited)
    _fixture_init(self, *a, **kw)
    self._fixture_inherited = inherited
    _fixture_event('loaded', inherited=inherited)
Scheduler.__init__ = _fixture_initialize
_fixture_publish = Scheduler._publish_handoff_ack
def _fixture_ack(self):
    if getattr(self, '_handoff_ack', None):
        if (_fixture_root / 'reject-before-ack').exists():
            _fixture_event('reject-before-ack')
            os._exit(71)
        _fixture_publish(self)
        _fixture_event('ack', of=self._handoff_ack.get('of'))
    else:
        _fixture_publish(self)
Scheduler._publish_handoff_ack = _fixture_ack
'''


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def jsonhash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     ensure_ascii=True).encode()).hexdigest()


def extract(archive, target, prefix=None):
    # No symlinks, devices, absolute paths or .. can escape the private temp root.
    with tarfile.open(archive) as tar:
        for member in tar:
            parts = Path(member.name).parts
            check(not Path(member.name).is_absolute() and '..' not in parts,
                  'unsafe archive path: ' + member.name)
            if prefix and (not parts or parts[0] != prefix):
                continue
            dest = target / member.name
            if member.isdir():
                dest.mkdir(parents=True, exist_ok=True)
            elif member.isfile() or member.issym() or member.islnk():
                source = member
                if not member.isfile():
                    # git archives contain internal rules -> implementation
                    # symlinks. Materialize only a checked regular archive member;
                    # never create or follow filesystem symlinks in this fixture.
                    link = posixpath.normpath(posixpath.join(posixpath.dirname(member.name), member.linkname)
                                              if member.issym() else member.linkname)
                    check(not link.startswith('/') and '..' not in Path(link).parts
                          and (not prefix or Path(link).parts[0] == prefix),
                          'unsafe archive link: ' + member.name)
                    source = tar.getmember(link)
                    check(source.isfile(), 'archive link must name a regular member: ' + member.name)
                dest.parent.mkdir(parents=True, exist_ok=True)
                with tar.extractfile(source) as src, dest.open('wb') as out:
                    shutil.copyfileobj(src, out)
            else:
                raise AssertionError('non-regular archive member: ' + member.name)


def legacy_bytes(state):
    paths = [p for p in (state / 'queue').iterdir()
             if p.is_file() and p.suffix in ('.json', '.rows', '.placed', '.cancel')]
    paths += list(state.glob('durable-groups.json'))
    return {str(p.relative_to(state)): (p.stat().st_size, digest(p)) for p in paths}


def audit(state, bounded=False):
    """Independent stdlib oracle; no candidate parser/cache touches the baseline.

    Expand row_ref to the COMPLETE original row, verify all referenced payload
    bytes, and compare canonical semantic hashes, preserving row order/duplicates.
    Existing reference objects retain their exact metadata in the semantic hash.
    """
    queue = state / 'queue'
    payloads, inventory, semantics = {}, {}, {}
    max_line, total_rows = 0, 0
    def validate_refs(value, store):
        if isinstance(value, dict):
            if set(value) == {'version', 'sha256', 'bytes', 'format'}:
                sha = value['sha256']
                check(isinstance(sha, str) and len(sha) == 64 and
                      all(c in '0123456789abcdef' for c in sha), 'invalid reference digest')
                p = store / (sha + '.payload')
                check(p.is_file() and p.stat().st_size == value['bytes'], 'missing/short payload ' + str(p))
                key = str(p.relative_to(state))
                if key not in payloads:
                    payloads[key] = digest(p)
                check(payloads[key] == sha, 'payload hash mismatch ' + str(p))
            else:
                for item in value.values():
                    validate_refs(item, store)
        elif isinstance(value, list):
            for item in value:
                validate_refs(item, store)
    for path in sorted(queue.glob('*.json')):
        admission = json.loads(path.read_text())
        bid = admission.get('batch', path.stem)
        check(bid == path.stem, 'admission/file mismatch')
        names = sorted(item['name'] for item in admission['items'])
        rows, row_names = [], []
        rp, store = path.with_suffix('.rows'), path.with_suffix('.results')
        if rp.exists():
            with rp.open('rb') as f:
                for line in f:
                    max_line = max(max_line, len(line))
                    if not line.strip():
                        continue
                    if bounded:
                        check(len(line) <= 65537, 'noncompact row: ' + str(rp))
                    row = json.loads(line)
                    validate_refs(row, store)
                    seen = set()
                    while 'row_ref' in row:
                        ref = row['row_ref']
                        check(ref['sha256'] not in seen, 'cyclic row reference')
                        seen.add(ref['sha256'])
                        row = json.loads((store / (ref['sha256'] + '.payload')).read_bytes())
                        validate_refs(row, store)
                    row_names.append(row.get('name'))
                    rows.append(jsonhash(row))
        inventory[bid] = dict(items=len(names), rows=len(rows),
                              item_names=jsonhash(names), row_names=jsonhash(row_names))
        semantics[bid] = jsonhash(rows)
        total_rows += len(rows)
    # Also verify old unreferenced payloads, which must not disappear or mutate.
    for p in queue.glob('*.results/*.payload'):
        key = str(p.relative_to(state))
        if key not in payloads:
            payloads[key] = digest(p)
        check(payloads[key] == p.stem, 'invalid content-addressed payload: ' + str(p))
    return dict(inventory=inventory, semantics=semantics, payloads=payloads,
                max_line_bytes=max_line, rows=total_rows,
                items=sum(x['items'] for x in inventory.values()), admissions=len(inventory))


def events(root):
    try:
        return [json.loads(line) for line in (root / 'events.jsonl').read_text().splitlines() if line]
    except FileNotFoundError:
        return []


def wait_for(fn, seconds, label):
    deadline, error = time.monotonic() + seconds, None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except (OSError, ValueError) as e:
            error = repr(e)
        time.sleep(.05)
    raise AssertionError('timeout waiting for ' + label + ': ' + str(error))


def private_env(root, source, port):
    # Allowlist, NOT a copy of os.environ: no BOAT/MODAL/AWS tokens, proxy,
    # config path, notify socket, SSH agent, user PYTHONPATH or inherited FDs.
    home = root / 'home'
    home.mkdir(exist_ok=True)
    return dict(PATH='/usr/bin:/bin', HOME=str(home), LANG='C.UTF-8',
                TMPDIR=str(root), PYTHONPATH=str(source), PYTHONUNBUFFERED='1',
                SUPERCOMPUTER_HOME=str(root), SUPERCOMPUTER_LIVE=str(root / 'live'),
                SUPERCOMPUTER_SRC=str(root / 'parent'), SUPERCOMPUTER_CONTROLLER='0',
                SUPERCOMPUTER_LOCAL='1', SUPERCOMPUTER_SCHEDULER_PORT=str(port),
                SUPERCOMPUTER_WATCHDOG='0', BOAT_BIN='/bin/false',
                SC_REALQUEUE_ROOT=str(root))


def worker(args):
    # This mode is called only by the already-native driver, in its sanitized env.
    source = Path(args.source).resolve()
    sys.path.insert(0, str(source))
    from supercomputer import scheduler as s
    check(Path(s.__file__).resolve().is_relative_to(source), 'wrong imported candidate')
    from supercomputer import resultjournal, resultjson
    parser_bytes = []
    original_iter = resultjson.iter_journal
    def measured_iter(stream, *a, **kw):
        parser_bytes.append(getattr(stream, 'remaining', -1))
        yield from original_iter(stream, *a, **kw)
    resultjson.iter_journal = measured_iter
    began = time.monotonic()
    if args.worker == 'stage':
        original_prepare = resultjournal.prepare_directory
        def prepare(queue):
            s._fixture_event('prewarm-start')
            receipt = original_prepare(queue)
            s._fixture_event('prewarm-done', errors=receipt['errors'])
            return receipt
        resultjournal.prepare_directory = prepare
        receipt = s.stage_install()
    else:
        receipt = resultjournal.prepare_directory(s.DIR / 'queue')
    result = dict(receipt=receipt, elapsed_s=time.monotonic() - began,
                  parser_input_bytes=sum(max(0, n) for n in parser_bytes),
                  parser_calls=len(parser_bytes))
    Path(args.receipt).write_text(json.dumps(result))


def synthetic(state):
    q = state / 'queue'
    q.mkdir(parents=True)
    for n in range(4):
        bid = 'fixture-' + str(n)
        msg = dict(batch=bid, klass=2, image='no-such-fixture-image', ctx_b64='',
                   tree=dict(id='fixture-tree', files={}, links={}),
                   items=[dict(name='job-' + str(i), cmd='exit 99') for i in range(5)])
        (q / (bid + '.json')).write_text(json.dumps(msg))
        with (q / (bid + '.rows')).open('w') as f:
            for i in range(3):
                f.write(json.dumps(dict(name='job-' + str(i), code=0,
                                       result=dict(blob='fixture' * 30000))) + '\n')
    (state / 'durable-groups.json').write_text('{"version":1,"groups":{}}')


def exercise(args):
    check(os.environ.get('REAL_QUEUE_NATIVE') == '1',
          'Run this gate via supercomputer with REAL_QUEUE_NATIVE=1, not on the controller')
    data = Path(args.data.split(':', 1)[0]).resolve()
    check((data / 'parent-f1b4.tar').is_file(), 'missing parent-f1b4.tar in --data')
    if not args.synthetic:
        check((data / 'queue.tar.gz').is_file(), 'missing queue.tar.gz in --data')
    root = Path(tempfile.mkdtemp(prefix='sc-realqueue-'))
    print('private fixture root: ' + str(root), flush=True)
    state = root / 'live' / 'scheduler'
    state.mkdir(parents=True)
    report = dict(mode='ZERO backend VMs; real-state startup/HTTP/handshake, NOT 155-VM network',
                  synthetic=args.synthetic, root=str(root), native=True)
    processes, owned, log_handles = [], set(), []
    sampler_stop = threading.Event()
    samples, sample_thread = [], None
    def own(pid):
        try:
            env = Path('/proc/' + str(pid) + '/environ').read_bytes().split(b'\0')
            return ('SC_REALQUEUE_ROOT=' + str(root)).encode() in env
        except (OSError, ValueError):
            return False
    def kill(pid):
        if own(pid):
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)
    def spawn(argv, env, name):
        log = (root / (name + '.log')).open('w')
        log_handles.append(log)
        p = subprocess.Popen(argv, env=env, cwd=root, stdin=subprocess.DEVNULL,
                             stdout=log, stderr=log, start_new_session=True)
        processes.append(p)
        owned.add(p.pid)
        return p
    try:
        if args.synthetic:
            synthetic(state)
        else:
            extract(data / 'queue.tar.gz', state)
            # Deduplicated admissions reference these immutable tree indexes.
            # They are metadata only: no tree blobs or job commands are executed.
            if (data / 'trees').is_dir():
                shutil.copytree(data / 'trees', state / 'trees')
        # Preserve and report the supplied metadata without assuming its counts.
        manifest = state / 'fixture-manifest.json'
        if manifest.exists():
            report['fixture_manifest'] = json.loads(manifest.read_text())
        extract(data / 'parent-f1b4.tar', root / 'parent', prefix='supercomputer')
        repo = Path(__file__).resolve().parents[1]
        shutil.copytree(repo / 'supercomputer', root / 'candidate' / 'supercomputer',
                        ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
        for src in ('parent', 'candidate'):
            with (root / src / 'supercomputer' / 'scheduler.py').open('a') as f:
                f.write('\n' + FIXTURE)
        baseline = audit(state)
        before = legacy_bytes(state)
        original_payloads = baseline['payloads']
        report['baseline'] = {k: baseline[k] for k in ('admissions', 'items', 'rows', 'max_line_bytes')}
        report['baseline'].update(journals=len(list((state / 'queue').glob('*.rows'))),
                                  journal_bytes=sum(p.stat().st_size for p in (state / 'queue').glob('*.rows')))
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        env = private_env(root, root / 'parent', port)
        token = ''
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        def req(path='/status'):
            r = urllib.request.Request('http://127.0.0.1:' + str(port) + path,
                                       data=b'{}', headers={'X-SC-Token': token})
            with opener.open(r, timeout=args.request_timeout) as response:
                return json.load(response)
        def start(source, name):
            e = dict(env, PYTHONPATH=str(root / source))
            return spawn([sys.executable, '-P', '-m', 'supercomputer', 'scheduler',
                          'serve', '--idle-exit', '0'], e, name)
        parent = start('parent', 'parent')
        def booted():
            nonlocal token
            check(parent.poll() is None, 'parent exited; see ' + str(root / 'parent.log'))
            token = (state / 'token').read_text().strip()
            return req()
        initial = wait_for(booted, args.timeout, 'legacy boot')
        check(initial['realqueue_fixture']['inventory'] == baseline['inventory'], 'legacy in-memory inventory mismatch')
        check(not initial['realqueue_fixture']['recovery_errors'], 'legacy recovery errors')
        old_code = initial['code']
        report['legacy_reload_ms'] = initial['realqueue_fixture']['reload_ms']
        check(legacy_bytes(state) == before, 'legacy boot modified fixture journals')
        def sample():
            while not sampler_stop.is_set():
                began = time.monotonic()
                try:
                    st = req()
                    samples.append(dict(start=began, elapsed_s=time.monotonic() - began,
                                        pid=st['pid'], code=st['code']))
                except Exception as e:
                    samples.append(dict(start=began, elapsed_s=time.monotonic() - began, error=repr(e)))
                sampler_stop.wait(.02)
        sample_thread = threading.Thread(target=sample, daemon=True)
        sample_thread.start()
        def work(kind):
            receipt = root / (kind + '.json')
            return spawn([sys.executable, '-P', str(Path(__file__).resolve()), '--worker', kind,
                          '--source', str(root / 'candidate'), '--receipt', str(receipt)],
                         dict(env, PYTHONPATH=str(root / 'candidate')), kind), receipt
        stage, stage_receipt = work('stage')
        stage_started = time.monotonic()
        def stage_done():
            ev = events(root)
            warming = any(e['kind'] == 'prewarm-start' for e in ev) and not any(e['kind'] == 'prewarm-done' for e in ev)
            if warming:
                check(not (state / 'install.json').exists(), 'candidate published before heavy prewarm finished')
                check(parent.poll() is None, 'old server exited during staging')
            rc = stage.poll()
            if rc is not None:
                check(rc == 0, 'stage failed; see ' + str(root / 'stage.log'))
                return True
            return False
        wait_for(stage_done, args.timeout, 'stage/prewarm')
        stage_result = json.loads(stage_receipt.read_text())
        check(stage_result['receipt']['active'] == old_code, 'stage replaced parent before publication receipt')
        installed = json.loads((state / 'install.json').read_text())
        installed_path = Path(installed['source']).resolve()
        check(installed_path.is_relative_to(state / 'code') and
              (installed_path / 'supercomputer' / 'scheduler.py').is_file(),
              'stage did not publish an immutable private code snapshot')
        prewarm = [e for e in events(root) if e['kind'].startswith('prewarm-')]
        check(len(prewarm) == 2 and not prewarm[1]['errors'], 'prewarm event/receipt missing')
        time.sleep(.15)
        during = [s for s in samples if prewarm[0]['mono'] <= s['start'] <= prewarm[1]['mono']]
        check(during, 'no asynchronous /status sample inside heavy prewarm')
        check(all('error' not in s and s['pid'] == parent.pid and s['code'] == old_code for s in during),
              'legacy /status failed or switched during heavy prewarm: ' + repr(during))
        maximum = max(s['elapsed_s'] for s in during)
        check(maximum < args.max_status_latency, 'stage /status latency exceeded bound')
        report['stage'] = dict(stage_result, status_samples=len(during), max_status_latency_s=maximum,
                               elapsed_wall_s=time.monotonic() - stage_started)
        check(legacy_bytes(state) == before, 'prepare/stage changed legacy journal bytes')
        # Second prepare MUST use verified cache: no rich-source bytes parsed.
        cache, cache_receipt = work('cache')
        check(cache.wait(timeout=args.timeout) == 0, 'repeat prepare failed')
        cached = json.loads(cache_receipt.read_text())
        cr = cached['receipt']
        check(not cr['errors'] and cr['prepared'] > 0 and cr['caches_reused'] == cr['prepared'], 'repeat preparation missed cache')
        check(cached['parser_input_bytes'] == 0, 'repeat prepare reparsed legacy bytes')
        report['repeat_prepare'] = cached
        # Both candidate and legacy fallback deliberately exit AFTER loading,
        # immediately BEFORE readiness ACK. The original listener must resume.
        (root / 'reject-before-ack').touch()
        artifacts = {n: (state / n).read_bytes() if (state / n).exists() else None
                     for n in ('handoff.json', 'handoff.used.json', 'handoff.ack')}
        failure_start = time.monotonic()
        req('/reload')
        def resumed():
            rejects = [e for e in events(root) if e['kind'] == 'reject-before-ack']
            if len(rejects) < 2:
                return False
            text = (root / 'parent.log').read_text()
            if 'old scheduler resumed' not in text:
                return False
            st = req()
            return st if st['pid'] == parent.pid and st['code'] == old_code else False
        resumed_state = wait_for(resumed, args.timeout, 'failed pre-ACK old listener resumption')
        check(parent.poll() is None, 'failed pre-ACK lost parent')
        check(legacy_bytes(state) == before, 'failed pre-ACK rewrote legacy journal bytes')
        for name, value in artifacts.items():
            check(((state / name).read_bytes() if (state / name).exists() else None) == value,
                  'failed pre-ACK did not restore ' + name)
        check(resumed_state['realqueue_fixture']['inventory'] == baseline['inventory'], 'failed pre-ACK changed old memory')
        report['failed_pre_ack'] = dict(elapsed_s=time.monotonic() - failure_start,
                                        legacy_bytes_unchanged=True, original_pid_resumed=True)
        (root / 'reject-before-ack').unlink()
        # Explicit reload avoids waiting for the production 60s stabilization.
        handoff_start = time.monotonic()
        req('/reload')
        def activated():
            if parent.poll() is None or not (state / 'handoff.ack').exists():
                return False
            st = req()
            if st['pid'] == parent.pid or st['code'] == old_code:
                return False
            fx = st['realqueue_fixture']
            return st if not fx['compact_pending'] else False
        active = wait_for(activated, args.timeout, 'successful handoff and compaction')
        parent.wait(timeout=5)
        owned.add(active['pid'])
        ack = json.loads((state / 'handoff.ack').read_text())
        snap = json.loads((state / 'handoff.used.json').read_text())
        check(ack['pid'] == active['pid'] and ack['of'] == snap['t'], 'ACK does not match offer/serving PID')
        check(ack['items_ready'] and ack['admissions']['ready'], 'readiness not complete')
        check(active['realqueue_fixture']['inherited'], 'successor did not inherit listener descriptors')
        check(not ack['taken'] and not ack['missing'], 'fixture unexpectedly used backend VMs')
        acks = [e for e in events(root) if e['kind'] == 'ack' and e['pid'] == active['pid']]
        check(acks, 'no observed ACK event')
        ready_s = acks[-1]['at'] - snap['t']
        check(0 <= ready_s < args.max_ready, 'READY/ACK exceeded comfortable 72.544s-budget margin')
        check(ready_s < snap['readiness_budget']['seconds'], 'ACK exceeded actual computed fixture budget')
        check(active['realqueue_fixture']['inventory'] == baseline['inventory'], 'candidate memory lost rows/items')
        check(not active['realqueue_fixture']['recovery_errors'] and not active['realqueue_fixture']['compact_errors'], 'candidate recovery/compaction errors')
        check(active['realqueue_fixture']['journal_cache_hits'] == report['baseline']['journals'], 'handoff did not load every prepared cache')
        report['handoff'] = dict(ready_ack_s=ready_s, reference_budget_s=72.544,
                                 required_below_s=args.max_ready,
                                 actual_fixture_budget_s=snap['readiness_budget']['seconds'],
                                 until_compaction_observed_s=time.monotonic() - handoff_start,
                                 reload_ms=ack.get('reload_ms'), inherited_listener=True,
                                 taken_vms=len(ack['taken']), cache_hits=active['realqueue_fixture']['journal_cache_hits'])
        after = audit(state, bounded=True)
        check(after['inventory'] == baseline['inventory'] and after['semantics'] == baseline['semantics'], 'compaction changed exact admitted rows/items')
        check(all(after['payloads'].get(k) == v for k, v in original_payloads.items()), 'old content references disappeared/changed')
        # Non-row authority must stay byte-for-byte, including durable generations.
        unchanged = legacy_bytes(state)
        check(all(unchanged.get(k) == v for k, v in before.items() if not k.endswith('.rows')), 'non-row authority changed')
        report['compaction'] = dict(semantic_rows_equal=True, exact_item_names_equal=True,
                                    old_payloads_preserved=len(original_payloads), verified_payloads=len(after['payloads']),
                                    max_line_bytes=after['max_line_bytes'],
                                    original_bytes=report['baseline']['journal_bytes'],
                                    compact_bytes=sum(p.stat().st_size for p in (state / 'queue').glob('*.rows')))
        # A fresh exec reads canonical .rows without inheriting any loaded memory.
        kill(active['pid'])
        fresh_start = time.monotonic()
        fresh = start('candidate', 'fresh')
        def fresh_booted():
            st = req()
            return st if st['pid'] == fresh.pid else False
        fresh_status = wait_for(fresh_booted, args.timeout, 'fresh canonical boot')
        fresh_s = time.monotonic() - fresh_start
        check(fresh_s < args.max_fresh_boot, 'canonical fresh boot not cheap')
        check(fresh_status['realqueue_fixture']['inventory'] == baseline['inventory'], 'fresh boot lost rows/items')
        check(not fresh_status['realqueue_fixture']['recovery_errors'], 'fresh recovery errors')
        check(not fresh_status['realqueue_fixture']['inherited'], 'fresh boot was not independent')
        report['fresh_boot'] = dict(elapsed_s=fresh_s, reload_ms=fresh_status['realqueue_fixture']['reload_ms'],
                                    required_below_s=args.max_fresh_boot, independent_exec=True)
        check(not any(e['kind'] == 'FORBIDDEN' for e in events(root)), 'attempted backend RPC or queued command execution')
        report.update(ok=True, no_backend_calls=True, no_job_commands_run=True)
    except BaseException as exc:
        report.update(ok=False, error=repr(exc),
                      forbidden_events=[e for e in events(root) if e['kind'] == 'FORBIDDEN'])
        # Logs belong to this private fixture only; never print actual payloads.
        for name in ('parent.log', 'stage.log', 'cache.log', 'fresh.log', 'live/scheduler/log'):
            p = root / name
            if p.exists():
                print(name + ' tail:\n' + p.read_text(errors='replace')[-5000:], file=sys.stderr)
        raise
    finally:
        sampler_stop.set()
        if sample_thread:
            sample_thread.join(timeout=args.request_timeout + 1)
        for e in events(root):
            owned.add(e['pid'])
            if 'child_pid' in e:
                owned.add(e['child_pid'])
        pidfile = state / 'pid'
        if pidfile.exists():
            with contextlib.suppress(ValueError):
                owned.add(int(pidfile.read_text()))
        for pid in owned:
            kill(pid)
        # Exactly one owner/waiter for each direct child. Grandchildren are killed
        # only by exact PID plus fixture env identity; no process-name matching.
        for p in processes:
            with contextlib.suppress(subprocess.TimeoutExpired):
                p.wait(timeout=5)
        for f in log_handles:
            f.close()
        report['status_observation'] = dict(samples=len(samples), errors=sum('error' in s for s in samples),
            max_latency_s=max((s['elapsed_s'] for s in samples), default=0))
        if args.output:
            out = Path(args.output).resolve()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, sort_keys=True), flush=True)
        if not args.keep:
            shutil.rmtree(root)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--data', default='realqueue')
    p.add_argument('--output', default='realqueue-report.json')
    p.add_argument('--keep', action='store_true', help='retain ONLY the private temp fixture and logs')
    p.add_argument('--synthetic', action='store_true')
    p.add_argument('--timeout', type=float, default=180, help='finite per-phase deadline (seconds)')
    p.add_argument('--request-timeout', type=float, default=2)
    p.add_argument('--max-status-latency', type=float, default=1.5)
    p.add_argument('--max-ready', type=float, default=72.544 * .75)
    p.add_argument('--max-fresh-boot', type=float, default=10)
    p.add_argument('--worker', choices=('stage', 'cache'), help=argparse.SUPPRESS)
    p.add_argument('--source', help=argparse.SUPPRESS)
    p.add_argument('--receipt', help=argparse.SUPPRESS)
    args = p.parse_args()
    check(all(x > 0 for x in (args.timeout, args.request_timeout, args.max_status_latency,
                              args.max_ready, args.max_fresh_boot)), 'deadlines must be positive')
    if args.worker:
        worker(args)
    else:
        exercise(args)


if __name__ == '__main__':
    main()
