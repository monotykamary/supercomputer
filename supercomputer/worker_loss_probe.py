"""Stdlib-only, read-only probe run as root through the provider's VM SSH path.

Do not import anything from the worker's user-writable agent installation here.
The worker's existing locks serialize these reads with launch and ownership writes.
"""
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sys


def stat_fields(entry):
    fields = (entry / 'stat').read_text().rsplit(') ', 1)[1].split()
    return fields[0], int(fields[19])


# Linux include/linux/proc_ns.h: PROC_PID_INIT_INO. From the initial PID
# namespace, NSpid lists each descendant view in which an old birth was recorded.
INITIAL_PID_NS = 0xEFFFFFFC


def full_pid_view(proc):
    if any((proc / f'{pid}/ns/pid').stat().st_ino != INITIAL_PID_NS for pid in ('1', 'self')):
        return False
    mounts = [(before.split()[4], after.split()) for line in (proc / 'self/mountinfo').read_text().splitlines()
              for before, after in [line.split(' - ', 1)]]
    entries = [opts for path, opts in mounts if path == str(proc)]
    return (len(entries) == 1 and entries[0][0] == 'proc'
            and all(o in ('hidepid=0', 'hidepid=off') or not o.startswith('hidepid=')
                    for o in entries[0][2].split(','))
            and not any(re.match(r'^/proc/(?:[0-9]+|self|thread-self)(?:/|$)', path) for path, _ in mounts))


def scan_absence(proc, supervisor, init):
    # In addition to a complete birth scan, veto any member of the verified
    # namespace. PID1 death makes new members impossible and Linux tears down
    # nested PID namespaces too. An inaccessible live entry is inconclusive.
    inode = init['pidns']
    for entry in proc.iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            _, ticks = stat_fields(entry)
            same_tick = [b for b in (supervisor, init) if b['start_ticks'] == ticks]
            if same_tick:
                line = next(line for line in (entry / 'status').read_text().splitlines() if line.startswith('NSpid:'))
                pids = [int(pid) for pid in line.split()[1:]]
                if not pids or pids[0] != int(entry.name) or any(b['pid'] in pids for b in same_tick):
                    return False
            if (entry / 'ns' / 'pid').stat().st_ino == inode:
                return False
        except (FileNotFoundError, ProcessLookupError):
            try:
                if stat_fields(entry)[0] not in ('Z', 'X'):
                    # A PID could have been reused between the two reads.
                    if (entry / 'ns' / 'pid').stat().st_ino == inode:
                        return False
            except (FileNotFoundError, ProcessLookupError):
                try:
                    stat_fields(entry)
                except (FileNotFoundError, ProcessLookupError):
                    continue
                return False
    return True


def _birth_valid(birth, boot):
    return (isinstance(birth, dict) and birth.get('boot') == boot
            and type(birth.get('pid')) is int and birth['pid'] > 0
            and type(birth.get('start_ticks')) is int and birth['start_ticks'] > 0)


def observe(root, proc, request):
    b = request['binding']
    for key in ('batch', 'run_name'):
        if not re.fullmatch(r'[A-Za-z0-9_][A-Za-z0-9_.~-]{0,199}', b[key]):
            raise ValueError('invalid item path')
    with (root / 'lease.lock').open('rb') as lease_lock, (root / 'owned-items.lock').open('rb') as owned_lock:
        fcntl.flock(lease_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(owned_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        boot = (proc / 'sys/kernel/random/boot_id').read_text().strip()
        lease = json.loads((root / 'lease.json').read_text())
        if boot != b['boot_id'] or lease.get('boot') != boot or lease.get('vm_id') != b['vm']:
            return {'ok': False, 'reason': 'original VM/boot not observed'}
        raw = (root / 'owned-items' / b['batch'] / (b['run_name'] + '.json')).read_bytes()
        row = json.loads(raw)
        job = root / 'b' / b['batch'] / 'jobs' / b['run_name']
        spec = json.loads((job / 'spec.json').read_text())
        identity = b['launch_identity']
        supervisor, init = row.get('supervisor'), row.get('init')
        if (row.get('identity') != identity or row.get('state') != 'launched'
                or not (row.get('namespace_verified') is True or row.get('contained') is True)
                or not _birth_valid(supervisor, boot) or not _birth_valid(init, boot)
                or supervisor['pid'] == init['pid'] or type(init.get('pidns')) is not int
                or init['pidns'] <= 0 or init['pidns'] == (proc / '1/ns/pid').stat().st_ino
                or spec.get('owner') != b['owner'] or spec.get('name') != b['run_name']
                or spec.get('strict_once') is not True or spec.get('launch_identity') != identity):
            return {'ok': False, 'reason': 'missing or mismatched launch custody'}
        if not scan_absence(proc, supervisor, init):
            return {'ok': False, 'reason': 'original birth or namespace member still present, or scan incomplete'}
        # Inspect the result *after* the original writers have been proven gone.
        if request.get('allow_published') is not True:
            for marker in ('done.json', 'acked.json'):
                try:
                    (job / marker).lstat()
                except FileNotFoundError:
                    continue
                return {'ok': False, 'reason': 'native result already published; do not replace with loss'}
        return {'ok': True, 'kind': 'native-namespace-absence/v1', 'binding': b,
                'nonce': request['nonce'], 'boot_id': boot, 'vm_id': lease['vm_id'],
                'supervisor': supervisor, 'init': init, 'owned_record_sha256': hashlib.sha256(raw).hexdigest(),
                'pid_view': 'initial-procfs', 'allow_published': request.get('allow_published') is True,
                'supervisor_birth_absent': True, 'init_birth_absent': True, 'namespace_empty': True}


if __name__ == '__main__':
    if os.geteuid() != 0:
        raise SystemExit('complete VM proc visibility requires root')
    try:
        proc = Path('/proc')
        if not full_pid_view(proc):
            raise ValueError('only unrestricted initial PID namespace procfs is authoritative')
        result = observe(Path('/home/user/.sc'), proc, json.loads(base64.b64decode(sys.argv[2])))
    except (OSError, ValueError, IndexError, KeyError, TypeError, StopIteration):
        result = {'ok': False, 'reason': 'incomplete native custody or proc visibility'}
    print(json.dumps(result))
