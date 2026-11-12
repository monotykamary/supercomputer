"""Lease-fenced, bounded legacy process audit/reclaim over the existing Boat exec transport.

No signals, releases, or payload execution. Strict-owned runs are never reclaimed here.
The exact proven ghost predicate is shipped with the static remote program.
"""
import inspect
import json

from .rules.worker import ghost_slot

_REMOTE = inspect.getsource(ghost_slot) + r'''
import contextlib, importlib.util, json, os, pathlib, sys, time
ROOT = pathlib.Path('/home/user/.sc')
os.environ['HOME'] = str(ROOT.parent)
request = json.loads(sys.argv[1])
spec = importlib.util.spec_from_file_location('sc_ghost_agent', ROOT / 'agent.py')
agent = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent)

def component(value):
    return isinstance(value, str) and value not in ('', '.', '..') and pathlib.Path(value).name == value and '\x00' not in value

def processes():
    found, complete = {}, True
    for p in pathlib.Path('/proc').iterdir():
        if not p.name.isdigit() or int(p.name) == os.getpid():
            continue
        try:
            fields = (p / 'stat').read_text().rsplit(') ', 1)[1].split()
            row = {'pid': int(p.name), 'state': fields[0], 'parent': int(fields[1]),
                   'group': int(fields[2]), 'session': int(fields[3])}
            # Group/session checks include privileged children; path/env checks
            # catch reparented children that remain in the job's working tree.
            row['cwd'], row['argv'], row['env'] = '', [], b''
            try:
                row['cwd'] = str((p / 'cwd').readlink())
                row['argv'] = (p / 'cmdline').read_bytes().split(b'\0')
                row['env'] = (p / 'environ').read_bytes()
            except FileNotFoundError:
                pass
            except PermissionError:
                if fields[0] != 'Z':
                    complete = False
            found[row['pid']] = row
        except FileNotFoundError:
            pass
        except (OSError, ValueError, IndexError):
            complete = False
    return found, complete

def probe():
    runs = request['runs']
    if not isinstance(runs, list) or len(runs) > 256:
        raise ValueError('invalid runs')
    rows = []
    with agent._lease_request(request['lease']), agent.lease_guard(request['lease']), agent._ownership_lock():
        before, complete = processes()
        for run in runs:
            bid, name = run['batch'], run['name']
            if not component(bid) or not component(name):
                raise ValueError('invalid run path')
            j = ROOT / 'b' / bid / 'jobs' / name
            row = {'batch': bid, 'name': name, 'strict': run.get('strict') is not False,
                   'complete': complete, 'pid': None, 'recorded': False, 'live': False,
                   'descendants': [], 'result_exists': (j / 'done.json').exists(), 'reclaimed': False}
            if not j.is_dir() or j.is_symlink() or j.resolve() != j.absolute():
                row['complete'] = False
                rows.append(row)
                continue
            try:
                stored = json.loads((j / 'spec.json').read_text())
                row['strict'] = row['strict'] or stored.get('strict_once') is True
            except (OSError, ValueError, TypeError):
                row['complete'] = False
            try:
                value = (j / 'pid').read_text().strip()
                row['pid'] = int(value) if value.isdecimal() and int(value) > 0 else None
                row['recorded'] = row['pid'] is not None
            except OSError:
                row['complete'] = False
            pid = row['pid']
            prefix = str(j) + '/'
            for proc in before.values():
                if proc['state'] == 'Z':
                    continue
                if proc['pid'] == pid:
                    row['live'] = True
                elif (pid and (proc['group'] == pid or proc['session'] == pid)
                      or proc['cwd'] == str(j) or proc['cwd'].startswith(prefix)
                      or any(str(j).encode() in arg for arg in proc['argv'])
                      or (b'SUPERCOMPUTER_OUTPUT_DIR=' + str(j / 'outputs').encode() + b'\0') in proc['env']):
                    row['descendants'].append(proc['pid'])
            # The ownership lock prevents new starts; recheck the exact PID file
            # and completion immediately before the atomic terminal journal write.
            try:
                stable = row['recorded'] and (j / 'pid').read_text().strip() == str(pid)
            except OSError:
                stable = False
            row['complete'] = row['complete'] and stable
            row['result_exists'] = (j / 'done.json').exists() or (j / 'acked.json').exists()
            ghost = ghost_slot(row['strict'], row['complete'], row['recorded'], row['live'],
                               bool(row['descendants']), row['result_exists'])
            if ghost and request.get('reclaim') is True:
                result = {'batch': bid, 'name': name, 'code': -1, 'seconds': -1,
                          'lost': True, 'status': 'LOST', 'ghost_reclaimed': True,
                          'error': 'agent dead with no result: recorded PID and descendants absent',
                          'ghost_evidence': {k: row[k] for k in ('pid', 'complete', 'live', 'descendants', 'result_exists')}}
                agent.write_json(j / 'done.json', result)
                # A terminal reply never precedes durable worker evidence.
                fd = os.open(j / 'done.json', os.O_RDONLY | os.O_NOFOLLOW)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                fd = os.open(j, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
                row['reclaimed'], row['result'] = True, result
            elif (j / 'done.json').exists():
                # A previous reclaim can be delivered again after a controller crash.
                result = json.loads((j / 'done.json').read_text())
                if result.get('ghost_reclaimed') is True:
                    row['reclaimed'], row['result'] = True, result
            rows.append(row)
    return {'protocol': 1, 'boot': pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'runs': rows}

try:
    print(json.dumps(probe()))
except Exception as error:
    print(json.dumps({'protocol': 1, 'error': type(error).__name__}))
    sys.exit(1)
'''


def audit(vm, runs, *, reclaim=False):
    from . import boat
    lease = {**vm.lease, 'owner': vm.client.owner, 'vm_id': vm.id}
    request = {'lease': lease, 'runs': runs, 'reclaim': reclaim}
    # Root sees all process groups/cwds, not a partial hidepid/ptrace view.
    # The exact controller lease is still required before any journal write.
    result = boat.exec_json(vm.id, ['sudo', '-n', '--', 'python3', '-c', _REMOTE, json.dumps(request)], timeout=30, tries=1)
    if not isinstance(result, dict) or result.get('protocol') != 1 or result.get('error'):
        raise boat.AgentError('worker process audit failed closed')
    return result
