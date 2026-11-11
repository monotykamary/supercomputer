"""Cross-batch VM exclusions. Mutations require Scheduler.lock; no VM holds.

Expired generations remain tombstones until the name is reopened. All writes
are copy-on-write and fsynced before publication or worker launch.
"""
import copy
import json
import math
import os
import secrets

from . import rules


def active(record, now):
    return record is not None and (record['expires_at'] is None or now < record['expires_at'])


class DurableGroups:
    def __init__(self, path):
        self.path, self.data, self.error = path, None, None

    @staticmethod
    def name(value):
        if not isinstance(value, str) or not value.strip() or len(value) > 1024:
            raise ValueError('durable group must be a nonblank name of at most 1024 characters')
        return value

    def load(self):
        if self.error:
            raise RuntimeError('durable group registry unavailable: ' + self.error)
        if self.data is None:
            try:
                if not self.path.exists():
                    self.data = {}
                else:
                    obj = json.loads(self.path.read_text())
                    if obj.get('version') != 1 or not isinstance(obj.get('groups'), dict):
                        raise ValueError('invalid durable group registry')
                    for name, r in obj['groups'].items():
                        self.name(name)
                        if (not isinstance(r, dict) or r.get('group') != name
                                or not isinstance(r.get('generation'), str) or not r['generation']
                                or not isinstance(r.get('used'), dict)
                                or 'closed_at' not in r or 'expires_at' not in r):
                            raise ValueError('invalid durable group record')
                        closed, expires = r['closed_at'], r['expires_at']
                        if (closed is None) != (expires is None):
                            raise ValueError('incomplete durable group expiry')
                        if closed is not None and (type(closed) not in (int, float) or type(expires) not in (int, float)
                                or not math.isfinite(closed) or not math.isfinite(expires) or expires < closed
                                or type(r.get('ttl_s')) not in (int, float) or not math.isfinite(r['ttl_s'])
                                or r['ttl_s'] < 0 or expires != closed + r['ttl_s']):
                            raise ValueError('invalid durable group expiry')
                        for vm, receipt in r['used'].items():
                            if (not isinstance(vm, str) or not vm or not isinstance(receipt, dict)
                                    or receipt.get('vm') != vm or receipt.get('distinct_vm_group') != name
                                    or receipt.get('distinct_vm_group_generation') != r['generation']
                                    or receipt.get('distinct_vm_group_scope') != 'durable'
                                    or type(receipt.get('attempt')) is not int or receipt['attempt'] < 1
                                    or not isinstance(receipt.get('boot_id'), str)):
                                raise ValueError('invalid durable group membership')
                    self.data = obj['groups']
            except Exception as error:
                self.error = str(error)
                raise RuntimeError('durable group registry unavailable: ' + self.error) from error
        return self.data

    def save(self, data):
        self.load()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_name('.durable-groups-' + secrets.token_hex(8) + '.tmp')
        try:
            with open(tmp, 'x') as f:
                json.dump({'version': 1, 'groups': data}, f, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            fd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            self.data = data
        except Exception as error:
            # Rename may already have succeeded: never continue from stale memory.
            self.error = str(error)
            raise
        finally:
            tmp.unlink(missing_ok=True)

    def snapshot(self):
        return copy.deepcopy(self.load())

    def open(self, name, now, generation=None):
        self.name(name)
        r = self.load().get(name)
        if generation is not None:
            if not isinstance(generation, str) or not generation:
                raise ValueError('invalid durable group generation')
            if not active(r, now) or r['generation'] != generation:
                raise ValueError('durable group generation expired or mismatched')
        if not active(r, now):
            r = {'group': name, 'generation': secrets.token_hex(24),
                 'closed_at': None, 'expires_at': None, 'used': {}}
            self.save({**self.load(), name: r})
        return copy.deepcopy(r)

    def close(self, name, generation, ttl_s, now):
        self.name(name)
        if type(ttl_s) not in (int, float) or not math.isfinite(ttl_s) or ttl_s < 0 or not math.isfinite(now + ttl_s):
            raise ValueError('ttl_s must be finite and nonnegative')
        r = self.load().get(name)
        if not isinstance(generation, str) or not generation or r is None or r['generation'] != generation:
            raise ValueError('durable group generation mismatched')
        if r['closed_at'] is not None:
            if r['ttl_s'] != ttl_s:
                raise ValueError('durable group close cannot change its TTL')
            return copy.deepcopy(r)
        r = {**r, 'closed_at': now, 'expires_at': now + ttl_s, 'ttl_s': ttl_s}
        self.save({**self.load(), name: r})
        return copy.deepcopy(r)

    def allowed(self, name, generation, vm, now):
        r = self.load().get(name)
        return rules.durable_vm_allowed(active(r, now), r is not None and r['generation'] == generation,
                                        r is not None and vm in r['used'])

    def reserve(self, receipt, now):
        name, generation, vm = receipt['distinct_vm_group'], receipt['distinct_vm_group_generation'], receipt['vm']
        if not self.allowed(name, generation, vm, now):
            raise ValueError('durable VM exclusion or expired generation')
        r = self.load()[name]
        self.save({**self.load(), name: {**r, 'used': {**r['used'], vm: copy.deepcopy(receipt)}}})
