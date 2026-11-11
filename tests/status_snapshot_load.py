#!/usr/bin/env python3
"""Independent native HTTP gate for published status under real queue lock load.

Run only through supercomputer, e.g. one finite --class gate --timing job:
  STATUS_SNAPSHOT_NATIVE=1 python3 tests/status_snapshot_load.py --data realqueue \
      --output "$SUPERCOMPUTER_OUTPUT_DIR/status-snapshot.json"
Ship --data /tmp/sc-realqueue-data:realqueue (queue.tar.gz and optional trees/).
--synthetic instead creates 30,000 queued items; it is not production evidence.

Only a private candidate source/state copy is instrumented. The original
_handoff_idle_slots -> _waste -> retired call holds Scheduler.lock for five
seconds. A second phase blocks code_hash in the actual status publisher. Neither
phase replaces status_response, HTTP handling, snapshot encoding, or refresh.
ZERO backend VMs: no provider access, external connections, or job commands.
This is not a fleet reconciliation or canary-policy correctness test; existing
canary thresholds/tests are unchanged. Wall latency includes a fresh connection,
full response read and JSON decode, not just a server-side timing statistic.
"""
import argparse
import concurrent.futures
import contextlib
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

# Reuse only the independent archive/state oracle and ZERO-provider fixture.
# No edits to that gate, its thresholds, or its archived parent are needed.
from real_queue_handoff import FIXTURE, audit, check, events, extract, private_env


HOOKS = r'''
# status_snapshot_load.py: PRIVATE COPY ONLY, appended after ZERO-provider guard.
# Deny even Python children; this gate never hands off to another process.
subprocess.Popen = _fixture_forbidden
_load_original_hash = code_hash
_load_original_refresh = Scheduler._refresh_status
_load_original_read = Scheduler._status_read
_load_original_init = Scheduler.__init__
_load_original_run = Scheduler.run
_load_original_retired = Scheduler.retired
_load_original_waste = Scheduler._waste
_load_original_idle = Scheduler._handoff_idle_slots

def _load_request_guard(helper):
    frame = sys._getframe(1)
    while frame is not None:
        if frame.f_code.co_name == 'process_request_thread':
            _fixture_event('request_slow_helper', helper=helper)
            raise AssertionError('HTTP request invoked ' + helper)
        frame = frame.f_back

def _load_hash(*a, **kw):
    _load_request_guard('code_hash')
    # Only the production publisher consumes this private one-shot trigger.
    if threading.current_thread().name == 'status-publisher':
        arm = _fixture_root / 'hash.arm'
        if arm.exists():
            arm.unlink()
            _fixture_event('hash-enter', thread=threading.current_thread().name)
            try:
                _load_instance.stop.wait(5.0)
            finally:
                _fixture_event('hash-exit')
    return _load_original_hash(*a, **kw)
code_hash = _load_hash

def _load_refresh(self, *a, **kw):
    _load_request_guard('_refresh_status')
    return _load_original_refresh(self, *a, **kw)
Scheduler._refresh_status = _load_refresh

def _load_read(self, *a, **kw):
    _load_request_guard('_status_read')
    return _load_original_read(self, *a, **kw)
Scheduler._status_read = _load_read

def _load_retired(self):
    if getattr(self, '_load_holder', None) == threading.get_ident():
        # Fail closed if the pause is not really inside both original methods.
        caller = sys._getframe(1)
        assert caller.f_code is _load_original_waste.__code__
        assert caller.f_back.f_code is _load_original_idle.__code__
        self._load_holder = None
        _fixture_event('lock-enter', path='_handoff_idle_slots/_waste/retired',
                       batches=len(self.batches), items=sum(len(b.items) for b in self.batches.values()))
        try:
            self.stop.wait(5.0)
        finally:
            _fixture_event('lock-exit')
    return _load_original_retired(self)
Scheduler.retired = _load_retired

def _load_control(self):
    try:
        while not self.stop.wait(.02):
            if (_fixture_root / 'stop.request').exists():
                self.stop.set()
                return
            arm = _fixture_root / 'lock.arm'
            if arm.exists():
                arm.unlink()
                with self.lock:
                    self._load_holder = threading.get_ident()
                    self._handoff_idle_slots(time.time(), list(self.vms.values()))
                    assert self._load_holder is None, 'actual waste path was not entered'
                _fixture_event('lock-released')
    except BaseException as exc:
        _fixture_event('hook-error', error=repr(exc))
        self.stop.set()

def _load_init(self, *a, **kw):
    global _load_instance
    _load_original_init(self, *a, **kw)
    _load_instance = self
    _fixture_event('snapshot-initial', ready=self.status()['status_snapshot']['ready'])
    self._load_control_thread = threading.Thread(target=_load_control, args=(self,),
                                                name='snapshot-load-control', daemon=True)
    self._load_control_thread.start()
Scheduler.__init__ = _load_init

def _load_run(self):
    try:
        return _load_original_run(self)
    finally:
        self.stop.set()
        # The enclosing serve() will also shut this down; doing it here lets
        # this fixture join the server, publisher, sampler and control threads.
        if getattr(self, '_srv', None) is not None:
            self._srv.shutdown()
        deadline = time.monotonic() + 8
        for thread in threading.enumerate():
            if thread is not threading.current_thread():
                thread.join(max(0, deadline - time.monotonic()))
        _fixture_event('threads-stopped', alive=[t.name for t in threading.enumerate()
                                                if t is not threading.current_thread()])
Scheduler.run = _load_run
'''


def synthetic(state):
    queue = state / 'queue'
    queue.mkdir(parents=True)
    for n in range(100):
        msg = dict(batch=f'snapshot-{n}', klass=2, image='no-provider-fixture', ctx_b64='',
                   tree=dict(id='fixture-tree', files={}, links={}),
                   items=[dict(name=f'item-{i}', cmd='exit 99') for i in range(300)])
        (queue / (msg['batch'] + '.json')).write_text(json.dumps(msg))
    (state / 'durable-groups.json').write_text('{"version":1,"groups":{}}')


def wait_for(fn, seconds, label):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = fn()
            if value:
                return value
        except (OSError, ValueError, http.client.HTTPException) as exc:
            last = repr(exc)
        time.sleep(.04)
    raise AssertionError(f'timeout waiting for {label}: {last}')


def exercise(args):
    check(os.environ.get('STATUS_SNAPSHOT_NATIVE') == '1',
          'run only as a finite supercomputer job with STATUS_SNAPSHOT_NATIVE=1')
    root = Path(tempfile.mkdtemp(prefix='sc-status-snapshot-'))
    state, source = root / 'live' / 'scheduler', root / 'candidate'
    state.mkdir(parents=True)
    proc, log = None, None
    report = dict(ok=False, native=True, synthetic=args.synthetic, backend_vms=0,
                  limitations='Private ZERO-provider state copy; not fleet RPC, deployment, or canary-policy proof',
                  assertions={}, phases={})
    failures = []

    def assert_that(name, condition, detail=''):
        report['assertions'][name] = bool(condition)
        if not condition:
            failures.append(name + (': ' + detail if detail else ''))

    def event(kind):
        return next((e for e in events(root) if e['kind'] == kind), None)

    try:
        if args.synthetic:
            synthetic(state)
        else:
            data = Path(args.data.split(':', 1)[0]).resolve()
            check((data / 'queue.tar.gz').is_file(), 'missing queue.tar.gz')
            extract(data / 'queue.tar.gz', state)
            if (data / 'trees').is_dir():
                shutil.copytree(data / 'trees', state / 'trees')
        baseline = audit(state)
        report['state'] = {k: baseline[k] for k in ('admissions', 'items', 'rows')}
        report['state']['journal_bytes'] = sum(p.stat().st_size for p in (state / 'queue').glob('*.rows'))
        repo = Path(__file__).resolve().parents[1]
        shutil.copytree(repo / 'supercomputer', source / 'supercomputer',
                        ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
        original = (source / 'supercomputer' / 'scheduler.py').read_bytes()
        report['scheduler_sha256'] = hashlib.sha256(original).hexdigest()
        with (source / 'supercomputer' / 'scheduler.py').open('a') as f:
            f.write('\n' + FIXTURE + '\n' + HOOKS)
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        env = private_env(root, source, port)
        env['SUPERCOMPUTER_SRC'] = str(source)
        token = ''
        log = (root / 'candidate.log').open('w')
        proc = subprocess.Popen([sys.executable, '-P', '-m', 'supercomputer',
                                 'scheduler', 'serve', '--idle-exit', '0'],
                                cwd=root, env=env, stdin=subprocess.DEVNULL,
                                stdout=log, stderr=log, start_new_session=True)
        report['pid'] = proc.pid

        def request():
            started = time.monotonic()
            connection = http.client.HTTPConnection('127.0.0.1', port, timeout=.8)
            try:
                connection.request('POST', '/status', body=b'{}',
                                   headers={'X-SC-Token': token, 'Content-Type': 'application/json'})
                response = connection.getresponse()
                raw = response.read()
                body = json.loads(raw)
                ended = time.monotonic()
                check(response.status == 200, f'HTTP {response.status}')
                return dict(start=started, end=ended, elapsed_s=ended - started,
                            bytes=len(raw), status=body)
            finally:
                connection.close()

        def booted():
            nonlocal token
            check(proc.poll() is None, 'candidate exited during boot')
            token = (state / 'token').read_text().strip()
            r = request()
            return r if r['status'].get('status_snapshot', {}).get('ready') else None

        initial = wait_for(booted, args.timeout, 'initial ready HTTP snapshot')
        init = initial['status']
        report['code'] = init['code']
        assert_that('constructor_ready', bool(event('snapshot-initial') and event('snapshot-initial')['ready']))
        assert_that('initial_inventory', init['realqueue_fixture']['inventory'] == baseline['inventory'])
        assert_that('initial_recovery_clear', not init['realqueue_fixture']['recovery_errors']
                    and not init.get('recovery_blocked', False))
        assert_that('initial_pid', init['pid'] == proc.pid)

        def phase(kind):
            before = request()
            (root / (kind + '.arm')).touch()
            entered = wait_for(lambda: event(kind + '-enter'), args.timeout, kind + ' entry')
            # Five independent clients: 20 serial samples plus four streams of 5.
            # All streams span two seconds and include fresh TCP connections.
            start = time.monotonic() + .05
            def stream(count):
                rows = []
                for i in range(count):
                    delay = start + 2.0 * i / (count - 1) - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    try:
                        rows.append(request())
                    except Exception as exc:
                        rows.append(dict(start=time.monotonic(), error=repr(exc)))
                return rows
            with concurrent.futures.ThreadPoolExecutor(max_workers=5,
                                                       thread_name_prefix='status-client') as pool:
                futures = [pool.submit(stream, n) for n in (20, 5, 5, 5, 5)]
                rows = [r for future in futures for r in future.result()]
            exited = wait_for(lambda: event(kind + '-exit'), args.timeout, kind + ' exit')
            if kind == 'lock':
                wait_for(lambda: event('lock-released'), args.timeout, 'scheduler lock release')
            good = sorted((r for r in rows if 'error' not in r), key=lambda r: r['start'])
            meta = [r['status']['status_snapshot'] for r in good]
            latencies = sorted(r['elapsed_s'] for r in good)
            p = dict(samples=len(rows), successful=len(good), serial_samples=20, parallel_samples=20,
                     hold_seconds=exited['mono'] - entered['mono'],
                     max_s=max(latencies, default=None),
                     p95_s=latencies[math.ceil(.95 * len(latencies)) - 1] if latencies else None,
                     age_before=before['status']['status_snapshot']['age_s'],
                     age_first=meta[0]['age_s'] if meta else None,
                     age_after=meta[-1]['age_s'] if meta else None,
                     generations=sorted({m['generation'] for m in meta}),
                     errors=[r['error'] for r in rows if 'error' in r])
            report['phases'][kind] = p
            assert_that(kind + '_all_http_succeeded', len(good) == 40)
            assert_that(kind + '_external_under_1s', len(good) == 40 and max(latencies) < 1.0)
            assert_that(kind + '_held_five_seconds', 5.0 <= p['hold_seconds'] < 8)
            assert_that(kind + '_samples_inside_hold', len(good) == 40 and
                        all(entered['mono'] <= r['start'] <= r['end'] <= exited['mono'] for r in good))
            assert_that(kind + '_generation_frozen', len({m['generation'] for m in meta}) == 1)
            assert_that(kind + '_timestamps_frozen', len({(m['captured_at'], m['published_at']) for m in meta}) == 1)
            assert_that(kind + '_honest_age', len(meta) == 40 and
                        # Parallel requests can finish in a different order to their start.
                        all(b['age_s'] + .05 >= a['age_s'] for a, b in zip(meta, meta[1:])) and
                        abs((meta[-1]['age_s'] - meta[0]['age_s']) -
                            (good[-1]['end'] - good[0]['end'])) < .15 and
                        meta[-1]['age_s'] - meta[0]['age_s'] > 1.8)
            assert_that(kind + '_identity_inventory', len(good) == 40 and all(
                r['status']['pid'] == proc.pid and r['status']['code'] == report['code'] and
                r['status']['realqueue_fixture']['inventory'] == baseline['inventory'] and
                r['status']['status_snapshot']['ready'] for r in good))
            frozen_generation = meta[-1]['generation'] if meta else -1
            def recovered():
                r = request()
                m = r['status']['status_snapshot']
                return r if m['generation'] > frozen_generation and m['age_s'] < 1.5 else None
            recovery = wait_for(recovered, args.timeout, kind + ' fresh publication')
            rm = recovery['status']['status_snapshot']
            p['recovery'] = dict(generation=rm['generation'], age_s=rm['age_s'],
                                 after_release_s=recovery['end'] - exited['mono'])
            assert_that(kind + '_recovered', rm['generation'] > frozen_generation and
                        rm['age_s'] < 1.5 and recovery['elapsed_s'] < 1.0)

        phase('lock')
        phase('hash')
        after = audit(state)
        assert_that('retained_state_semantics', after['semantics'] == baseline['semantics'] and
                    after['inventory'] == baseline['inventory'])
        assert_that('actual_queue_scan', event('lock-enter')['items'] == baseline['items'] and
                    event('lock-enter')['batches'] == baseline['admissions'])
        assert_that('no_slow_helpers_in_http', not any(e['kind'] == 'request_slow_helper' for e in events(root)))
        assert_that('zero_backend_or_job_commands', not any(e['kind'] == 'FORBIDDEN' for e in events(root)))
        assert_that('no_hook_errors', not any(e['kind'] == 'hook-error' for e in events(root)))
    except BaseException as exc:
        failures.append(repr(exc))
    finally:
        # One owner/waiter, one exact direct child. Never process-name/group kills.
        if proc is not None:
            (root / 'stop.request').touch()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.send_signal(signal.SIGTERM)
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
            assert_that('clean_process_exit', proc.returncode == 0, str(proc.returncode))
            stopped = event('threads-stopped')
            assert_that('threads_stopped', stopped is not None and stopped['alive'] == [])
        if log is not None:
            log.close()
        forbidden = [e for e in events(root) if e['kind'] in ('FORBIDDEN', 'request_slow_helper', 'hook-error')]
        if forbidden:
            report['guard_events'] = forbidden
            failures.append('private guard recorded forbidden behavior')
        report['failures'] = failures
        report['ok'] = not failures
        if not report['ok']:
            p = root / 'candidate.log'
            if p.exists():
                print('private candidate log tail:\n' + p.read_text(errors='replace')[-4000:], file=sys.stderr)
        if args.keep:
            report['private_root'] = str(root)
        if args.output:
            out = Path(args.output).resolve()
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(report, sort_keys=True) + '\n')
        print(json.dumps(report, sort_keys=True), flush=True)
        if not args.keep:
            shutil.rmtree(root)
    return 0 if report['ok'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', default='realqueue')
    parser.add_argument('--synthetic', action='store_true')
    parser.add_argument('--timeout', type=float, default=120, help='finite readiness/recovery deadline')
    parser.add_argument('--output', default='')
    parser.add_argument('--keep', action='store_true', help='keep only private fixture/logs for debugging')
    args = parser.parse_args()
    check(0 < args.timeout <= 300, 'timeout must be in (0, 300]')
    return exercise(args)


if __name__ == '__main__':
    raise SystemExit(main())
