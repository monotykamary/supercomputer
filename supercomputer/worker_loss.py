"""Evidence and bounded eligibility for strict items on unreachable workers."""
import re
import secrets
import time

PROBE_INTERVAL_S = 60
PROBE_TIMEOUT_S = 10
SOURCE = 'same-boot-namespace-absence'


def matches(evidence, binding, nonce=None, *, allow_published=False):
    if (not isinstance(evidence, dict) or evidence.get('ok') is not True
            or evidence.get('kind') != 'native-namespace-absence/v1' or evidence.get('binding') != binding
            or evidence.get('boot_id') != binding['boot_id'] or evidence.get('vm_id') != binding['vm']
            or evidence.get('pid_view') != 'initial-procfs'
            or evidence.get('allow_published') is not allow_published
            or not isinstance(evidence.get('nonce'), str) or not re.fullmatch('[0-9a-f]{32}', evidence['nonce'])
            or (nonce is not None and evidence['nonce'] != nonce)
            or not isinstance(evidence.get('owned_record_sha256'), str)
            or not re.fullmatch('[0-9a-f]{64}', evidence['owned_record_sha256'])
            or any(evidence.get(k) is not True for k in
                   ('supervisor_birth_absent', 'init_birth_absent', 'namespace_empty'))):
        return False
    for key in ('supervisor', 'init'):
        birth = evidence.get(key)
        if (not isinstance(birth, dict) or birth.get('boot') != binding['boot_id']
                or type(birth.get('pid')) is not int or birth['pid'] <= 0
                or type(birth.get('start_ticks')) is not int or birth['start_ticks'] <= 0):
            return False
    sup, init = evidence['supervisor'], evidence['init']
    return sup['pid'] != init['pid'] and type(init.get('pidns')) is int and init['pidns'] > 0


def probe(transport, binding, *, allow_published=False):
    nonce = secrets.token_hex(16)
    evidence = transport.read_namespace_absence(binding, nonce, allow_published=allow_published,
                                                deadline=time.monotonic() + PROBE_TIMEOUT_S)
    if not matches(evidence, binding, nonce, allow_published=allow_published):
        return None
    return evidence
