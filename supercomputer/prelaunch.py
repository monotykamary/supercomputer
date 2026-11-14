"""Strict, identity-bound launch-audit reads. Missing/torn evidence fails closed.

An open reservation is written/fsynced before its start thread exists. The only
payload-launch call follows a separately fsynced start_requested record. A
NOT_STARTED event binds the complete prefix that preceded that preparation
failure; later success does not turn that event into a terminal payload result.
"""
import hashlib
import json


BINDING = ('batch', 'name', 'run_name', 'vm', 'boot_id', 'launch_identity')


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate audit key")
        value[key] = item
    return value


def inspect(raw: bytes, binding: dict) -> dict:
    unknown = {'complete': False, 'reserved': False, 'start_requested': True,
               'failures': 0}
    if not raw or not raw.endswith(b'\n') or any(not binding.get(k) for k in BINDING):
        return unknown
    opened = requested = responded = False
    failures = offset = 0
    try:
        for line in raw.splitlines(keepends=True):
            record = json.loads(line.decode("utf-8"), object_pairs_hook=_unique_object)
            if (not isinstance(record, dict) or any(not record.get(k) for k in BINDING)
                    or record.get('batch') != binding['batch']
                    or record.get('event') not in ('prelaunch_reserved', 'prelaunch_not_started',
                                                  'start_requested', 'start_response')):
                return unknown
            if record.get('name') == binding['name']:
                if any(record.get(k) != binding[k] for k in BINDING):
                    return unknown
                event = record.get('event')
                if event == 'prelaunch_reserved':
                    if opened or type(record.get('protocol')) is not int or record.get('protocol') != 1:
                        return unknown
                    opened = True
                elif event == 'prelaunch_not_started':
                    proof = record.get('audit') or {}
                    if (not isinstance(proof, dict) or not opened or requested or record.get('status') != 'NOT_STARTED'
                            or record.get('phase') != 'prelaunch' or record.get('terminal') is not True
                            or type(record.get('prelaunch_failure')) is not int
                            or record.get('prelaunch_failure') != failures + 1
                            or proof.get('complete') is not True or proof.get('reserved') is not True
                            or proof.get('start_requested') is not False
                            or proof.get('prefix_bytes') != offset
                            or proof.get('prefix_sha256') != hashlib.sha256(raw[:offset]).hexdigest()):
                        return unknown
                    failures += 1
                elif event == 'start_requested':
                    if not opened or requested:
                        return unknown
                    requested = True
                elif event == 'start_response':
                    if not requested or responded:
                        return unknown
                    responded = True
                else:
                    return unknown
            offset += len(line)
    except (ValueError, TypeError, UnicodeError):
        return unknown
    if not opened:
        return unknown
    return {'complete': True, 'reserved': True, 'start_requested': requested,
            'failures': failures, 'prefix_bytes': len(raw),
            'prefix_sha256': hashlib.sha256(raw).hexdigest()}


def read(path, binding: dict) -> dict:
    try:
        return inspect(path.read_bytes(), binding)
    except OSError:
        return inspect(b'', binding)
