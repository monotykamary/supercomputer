"""Bounded first-cause lease diagnostics; never an authority or retry decision."""
import hashlib
import math
import re


_FIELDS = frozenset(('ok', 'stale', 'busy', 'pending', 'birth', 'valid_boot',
                     'lapsed', 'draining', 'state', 'boot', 'epoch', 'vm_id',
                     'protocol', 'version', 'lease_s', 'renewed', 'uptime'))
_PRIVATE = r'(?:token|secret|password|authorization|cookie|ctx_b64|nonce|stop_nonce|api[_-]?key)'


def _text(value, secrets=(), limit=768):
    text = str(value)
    for secret in sorted((v for v in secrets if isinstance(v, str) and len(v) >= 4), key=len, reverse=True):
        text = text.replace(secret, '[redacted]')
    text = re.sub(r'https?://[^\s\x22\x27<>]+', '[redacted-url]', text)
    text = re.sub(r'(?i)\bBearer\s+[^\s\x22\x27,;]+', 'Bearer [redacted]', text)
    # Error bodies may be JSON, Python reprs, or key=value provider messages.
    text = re.sub(r'(?i)(\b' + _PRIVATE + r'[\x22\x27]?\s*[:=]\s*)([\x22\x27])[^\x22\x27]*[\x22\x27]',
                  r'\1"[redacted]"', text)
    text = re.sub(r'(?i)(\b' + _PRIVATE + r'[\x22\x27]?\s*[:=]\s*)[^\s,;\x7d\x5d]+',
                  r'\1[redacted]', text)
    # Do not copy an arbitrary environment/context payload into a scheduler log.
    text = re.sub(r'(?is)\b(?:env|environment|context)[\x22\x27]?\s*[:=].*', '[redacted-context]', text)
    return text[:limit]


def client_secrets(client):
    """Inspect only existing instance fields: no properties, RPCs or key reads."""
    result, seen = [], set()
    while client is not None and id(client) not in seen:
        seen.add(id(client))
        fields = vars(client) if hasattr(client, '__dict__') else {}
        result.extend(fields.get(k) for k in ('token', 'cookie', 'url', 'query'))
        client = fields.get('client')
    return tuple(v for v in result if isinstance(v, str))


def _public(value, secrets):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key in sorted(_FIELDS & value.keys()):
        item = value[key]
        if item is None or type(item) is bool:
            result[key] = item
        elif type(item) is int:
            result[key] = item if -(1 << 63) <= item < (1 << 63) else "out-of-range"
        elif type(item) is float:
            result[key] = item if math.isfinite(item) else "nonfinite"
        elif isinstance(item, str):
            result[key] = _text(item, secrets, 128)
    for key in ('owner', 'holder'):
        if isinstance(value.get(key), str):
            result[key + '_sha256'] = hashlib.sha256(value[key].encode()).hexdigest()[:16]
    for key in ('error', 'detail'):
        if isinstance(value.get(key), str):
            result[key] = _text(value[key], secrets)
    if isinstance(value.get('result'), dict):
        result['result'] = _public({k: v for k, v in value['result'].items() if k != 'result'}, secrets)
    return result


def operation(method, args):
    name = method
    if method in ('rpc', 'raw') and args and isinstance(args[0], str):
        name += '/' + args[0]
        if args[0] == 'owned_rpc' and len(args) > 1 and isinstance(args[1], dict):
            command = args[1].get('command')
            if isinstance(command, str):
                name += '/' + command
    return name


def event(vm, source, *, lease=None, client=None, error=None, reply=None, operation=''):
    """Only bounded scalar metadata; no request arguments, payloads or credentials."""
    secrets = client_secrets(client)
    out = {'event': 'lease_relinquish', 'vm': _text(vm, secrets, 128),
           'source': _text(source, secrets, 128), 'operation': _text(operation, secrets, 128),
           'expected': _public(lease, secrets)}
    if error is not None:
        raw = str(error)
        out['error'] = {'type': type(error).__name__, 'message': _text(raw, secrets),
                        'sha256': hashlib.sha256(raw.encode('utf-8', errors='replace')).hexdigest(),
                        'classification_matches': [word for word in ('stale', '403', '400') if word in raw.lower()]}
    if reply is not None:
        out['reply'] = _public(reply, secrets)
    return out
