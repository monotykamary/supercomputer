"""Explicit controller adoption of separately verified original-boot extinction.

This never converts a stopped/missing worker or an ordinary negative cancel
receipt into success. A fresh, exact-VM, different-kernel-boot proof is required.
"""
import fcntl
import hashlib
import os
import re

from . import boat, history, namespace_recovery
from .namespace_recovery_transport import NativeTransport


def binding_for(item):
    p = item.last_placement or {}
    b = {'batch': item.batch.id, 'name': item.name, 'run_name': p.get('run_name'),
         'attempt': p.get('attempt'), 'vm': p.get('vm'), 'boot_id': p.get('boot_id'),
         'launch_identity': item.launch_identity, 'owner': p.get('owner')}
    if (not item.batch.strict_once or item.owned.placements != 1 or type(b['attempt']) is not int
            or b['attempt'] != 1 or p.get('identity') != b['launch_identity']
            or any(not isinstance(v, str) or not v for k, v in b.items() if k != 'attempt')):
        raise ValueError('original strict placement is incomplete or ambiguous')
    return b


def _storage(root, value, suffix=''):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise ValueError('invalid namespace recovery intent id')
    return root / 'namespace-recovery' / (value + suffix + '.json')


def validated_proof(root, item, proof):
    try:
        intent_id = proof['intent_id']
        intent = history.load(_storage(root, intent_id))
        stored = history.load(_storage(root, intent_id, '.proof'))
        return stored == proof and namespace_recovery.validate_proof(proof, binding_for(item), intent)
    except (KeyError, ValueError, TypeError, OSError):
        return False


def recover(scheduler, root, request, transport=None):
    if (request.get('protocol') != 1 or request.get('mode') not in ('observe-boot', 'reboot-owned-vm')
            or request.get('authorized') is not True
            or not isinstance(request.get('authorization_reference'), str)
            or not request['authorization_reference'].strip()):
        raise ValueError('explicit namespace-recovery protocol, mode and authorization required')
    mode = request['mode']
    if mode == 'reboot-owned-vm' and request.get('allow_vm_reboot') is not True:
        raise ValueError('VM reboot requires separate explicit consent')
    bid, name = request.get('batch'), request.get('name')
    scheduler._valid_batch(bid)
    with scheduler.lock:
        if getattr(scheduler, 'handing_off', False) or getattr(scheduler, 'recovery_blocked', False):
            raise ValueError('controller ownership is not stable')
        batch = scheduler.batches.get(bid)
        if batch is None:
            return {'ok': False, 'error': 'active retained strict batch required', 'close': scheduler.close_receipt(bid)}
        item = batch.items.get(name)
        if item is None:
            raise ValueError('item is not in the original batch')
        binding = binding_for(item)
        if (type(request.get('attempt')) is not int
                or any(request.get(k) != binding[k] for k in ('vm', 'boot_id', 'attempt', 'launch_identity'))):
            raise ValueError('request differs from the original placement')
        if binding['owner'] != boat.owner_id():
            raise ValueError('original owner is not this controller')
        if (not batch.admission_closed or not item.start_done.is_set() or item.state != 'done'):
            raise ValueError('original admission/launch has not reached a fenced terminal state')
        active = scheduler.__dict__.setdefault('_namespace_recoveries', {})
        if binding['vm'] in active:
            raise ValueError('namespace recovery already in progress for this VM')
        if mode == 'reboot-owned-vm':
            for b in scheduler.batches.values():
                for other in b.items.values():
                    if other is item:
                        continue
                    p = other.last_placement or {}
                    if p.get('vm') == binding['vm'] and not other.owned.reaped:
                        raise ValueError('another placement retains custody of this VM')
            vm = scheduler.vms.get(binding['vm'])
            if vm is not None and (getattr(vm, 'upgrading', False) or vm.starts
                                   or any(k != (bid, binding['run_name']) for k in vm.running)):
                raise ValueError('other work is queued or running on this VM')
        active[binding['vm']] = binding
    lock_fd = None
    try:
        directory = root / 'namespace-recovery'
        directory.mkdir(mode=0o700, exist_ok=True)
        history.sync(root)
        # A durable per-VM lock serializes old/new controller processes too.
        lock_path = directory / (hashlib.sha256(binding['vm'].encode()).hexdigest() + '.lock')
        lock_fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        def load_intent(key):
            try:
                return history.load(_storage(root, key))
            except FileNotFoundError:
                return None

        def persist_intent(intent):
            history.atomic(_storage(root, intent['intent_id']), intent)

        def persist_proof(proof):
            history.atomic(_storage(root, proof['intent_id'], '.proof'), proof)

        authorization = {'explicit': True, 'reference': request['authorization_reference'], 'binding': binding}
        fn = namespace_recovery.recover_replaced_boot if mode == 'observe-boot' else namespace_recovery.recover
        with scheduler._unserialized():
            existing = load_intent(namespace_recovery.intent_id(binding))
            if existing and existing.get('state') == 'pending' and existing.get('method') != 'observed-boot-replacement':
                # A pending stop intent can only be observed, never stopped again.
                proof = namespace_recovery.verify_pending_boot(transport or NativeTransport(), binding=binding,
                    persist_intent=persist_intent, load_intent=load_intent, persist_proof=persist_proof)
            else:
                proof = fn(transport or NativeTransport(), binding=binding, authorization=authorization,
                           persist_intent=persist_intent, load_intent=load_intent, persist_proof=persist_proof)
        with scheduler.lock:
            if (getattr(scheduler, 'handing_off', False) or getattr(scheduler, 'recovery_blocked', False)
                    or scheduler.batches.get(bid) is not batch or binding_for(item) != binding):
                raise ValueError('original placement changed while recovering')
            if not validated_proof(root, item, proof):
                raise ValueError('durable boot-extinction proof did not validate')
            scheduler._append_owned(bid, 'cancel', {'name': name, 'run_name': binding['run_name'],
                'identity': binding['launch_identity'], 'admission_closed': True, 'matched': True,
                'source': 'vm-boot-extinction', 'proof': proof})
            item.namespace_recovery = proof
            item.owned.cancel()
            item.owned.confirm(True)
            vm = scheduler.vms.get(binding['vm'])
            if vm is not None:
                vm.running.pop((bid, binding['run_name']), None)
                if mode == 'reboot-owned-vm':
                    vm.dead, vm.ready = True, False
            scheduler.wake.set()
            complete = all(i.owned.reaped for i in batch.items.values())
        # Only finish an already fenced batch once every original item has its
        # own proof. Never cancel another unresolved item to complete this one.
        close = scheduler.cancel(bid) if complete else scheduler.close_receipt(bid)
        return {'ok': True, 'proof': proof, 'close': close}
    finally:
        if lock_fd is not None:
            os.close(lock_fd)
        with scheduler.lock:
            scheduler.__dict__.get('_namespace_recoveries', {}).pop(binding['vm'], None)
