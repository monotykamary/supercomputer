"""Native transport for explicitly authorized boot-extinction recovery.

No PID scan, unqualified missing VM, or successful stop invocation is an
extinction proof. The recovery engine requires a fresh changed kernel boot.
"""
import base64
import hashlib
import json
import re
import time
import uuid

from . import boat

CAPSULE = r'''
import hashlib, importlib.util, json, pathlib, sys
request = json.loads(__import__('base64').b64decode(sys.argv[2]))
b = request['binding']
spec = importlib.util.spec_from_file_location('sc_custody_probe', '/home/user/.sc/agent.py')
a = importlib.util.module_from_spec(spec); spec.loader.exec_module(a)
with a.lease_lock(), a._ownership_lock():
    p = a._owned_path(b['batch'], b['run_name'])
    raw = p.read_bytes(); row = json.loads(raw)
    j = a.batch_dir(b['batch']) / 'jobs' / b['run_name']
    owner = a._job_owner(j) if j.exists() else a._dropped_owner(b['batch'], b['run_name'])
    lease = {'ok': True, **a._lease_who()}
    if request.get('fence') is not None:
        lease['stop_nonce'] = a._lease_record().get('stop_nonce')
    native_jobs = [{'owner': owner, 'count': count} for owner, count in a._lease_inventory().items() if count]
    unresolved = []
    for f in (a.ROOT / 'owned-items').glob('*/*.json'):
        if f == p: continue
        r = json.loads(f.read_text())
        init, supervisor = r.get('init'), r.get('supervisor')
        identified = (r.get('state') == 'launched' and a._birth_record(init, True)
                      and a._birth_record(supervisor) and init['boot'] == supervisor['boot'])
        if not a.owned_replacement_safe(r.get('state') == 'never_started',
                                       r.get('reaped') is True, r.get('contained') is True, identified):
            unresolved.append({'batch': f.parent.name, 'name': f.stem, 'identity': r.get('identity')})
    out = {'kind': 'namespace-recovery-capsule/v1', 'nonce': request['nonce'], 'binding': b,
           'complete': True, 'actual_vm_id': lease.get('vm_id'), 'boot_id': a._lease_boot(),
           'owned_record': row, 'owned_record_sha256': hashlib.sha256(raw).hexdigest(),
           'owned_record_raw': raw.decode(),
           'owner_tombstone': {'batch': b['batch'], 'run_name': b['run_name'], 'owner': owner},
           'job_dir_absent': not j.exists(), 'init_absent': row.get('init') is None,
           'supervisor_absent': row.get('supervisor') is None,
           'native_jobs': native_jobs, 'other_unresolved_owned_items': unresolved, 'lease': lease}
    print(json.dumps(out))
'''

BOOT = r'''
import json,pathlib,sys
r=json.loads(__import__('base64').b64decode(sys.argv[2]))
lease=json.loads(pathlib.Path('/home/user/.sc/lease.json').read_text())
print(json.dumps({'kind':'native-boot/v1','source':'native','vm_id':lease.get('vm_id'),
                  'nonce':r['nonce'],'boot_id':pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()}))
'''


class NativeTransport:
    """The caller owns controller admission quarantine; agent leases fence workers."""

    @staticmethod
    def _remaining(deadline, cap=30):
        left = deadline - time.monotonic()
        if left < 0.1:
            raise TimeoutError('namespace recovery deadline expired')
        return min(cap, left)

    def _exec(self, vm, script, data, deadline):
        # Only immutable provider IDs, never aliases, names, hostnames or an
        # address copied from a snapshot. Authenticate the provider binding
        # independently of the guest-writable lease file before every read.
        if not isinstance(vm, str) or not re.fullmatch(r'bx_[A-Za-z0-9]+', vm):
            raise ValueError('immutable provider VM ID required')
        observed = boat.bj('info', vm, timeout=self._remaining(deadline, 15)).get('sandbox', {})
        if observed.get('id') != vm or observed.get('access') != 'owner':
            raise ValueError('provider VM identity/ownership did not match')
        encoded = base64.b64encode(script.encode()).decode()
        request = base64.b64encode(json.dumps(data).encode()).decode()
        out = boat.boat('ssh', vm, '--', 'python3', '-c',
                        'import base64,sys;exec(base64.b64decode(sys.argv[1]))', encoded, request,
                        timeout=self._remaining(deadline))
        return json.loads(out.strip().splitlines()[-1])

    def read_capsule(self, binding, nonce, fence=None, *, deadline):
        # Fence validation belongs to the engine, against this fresh native
        # lease snapshot; metadata-absent flags are NOT process-absence proof.
        return self._exec(binding['vm'], CAPSULE, {'binding': binding, 'nonce': nonce, 'fence': fence}, deadline)

    def read_lease(self, vm, *, deadline):
        return self._exec(vm, "import json,importlib.util; s=importlib.util.spec_from_file_location('a','/home/user/.sc/agent.py'); a=importlib.util.module_from_spec(s); s.loader.exec_module(a); print(json.dumps(a.lease('who')))", {}, deadline)

    def acquire_lease(self, vm, owner, *, deadline):
        if owner != boat.owner_id():
            raise ValueError('recovery cannot acquire another owner\'s lease')
        self._remaining(deadline)
        return boat.lease_vm(vm, 'claim')

    def fence_stop(self, vm, lease, *, deadline):
        self._remaining(deadline)
        return boat._stop_fence(vm, lease)

    def stop_vm(self, vm, fence, *, preserve_disk, deadline):
        if preserve_disk is not True or fence.get('state') != 'stopping' or not fence.get('stop_nonce'):
            raise ValueError('positive disk-preserving stop reservation required')
        raw = boat.boat('stop', vm, '--yes', '--json', timeout=self._remaining(deadline, 90))
        return {'vm_id': vm, 'operation_id': uuid.uuid4().hex, 'accepted': True,
                'preserve_disk': True, 'provider_response': raw, 'operation_id_scope': 'local-invocation'}

    def observe_stopped(self, vm, receipt, *, deadline):
        while True:
            response = boat.bj('info', vm, timeout=self._remaining(deadline, 15))
            value = response.get('sandbox', {})
            if value.get('id') != vm:
                raise ValueError('provider VM identity mismatch')
            if value.get('state') == 'stopped':
                return {'vm_id': vm, 'operation_id': receipt['operation_id'], 'state': 'stopped',
                        'observed': True, 'source': 'provider', 'provider_observation': value}
            time.sleep(min(1, self._remaining(deadline, 1)))

    def resume_vm(self, vm, *, deadline):
        raw = boat.boat('resume', vm, '--json', timeout=self._remaining(deadline, 90))
        return {'vm_id': vm, 'operation_id': uuid.uuid4().hex, 'accepted': True,
                'provider_response': raw, 'operation_id_scope': 'local-invocation'}

    def read_boot(self, vm, nonce, *, deadline):
        while True:
            try:
                return self._exec(vm, BOOT, {'nonce': nonce}, deadline)
            except (RuntimeError, ValueError, IndexError):
                time.sleep(min(1, self._remaining(deadline, 1)))
