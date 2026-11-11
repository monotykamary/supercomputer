"""Bounded, read-only Linux cgroup-v2 memory telemetry. Never mutates limits.

Unknown fields are None (not zero); 'max' denotes an unlimited cgroup limit.
A sampler keeps only one counter baseline, reset on missing data or cgroup change.
"""
import math
import pathlib
import sys

from .rules.memory import high_growth, pressure_bad

_FIELDS = ('current', 'high', 'max', 'swap_current', 'swap_max',
           'high_events', 'high_delta', 'some_avg10', 'full_avg10',
           'high_headroom', 'max_headroom')


def _read(path):
    try:
        with path.open() as f:
            text = f.read(16385)
        return text if len(text) <= 16384 else None
    except (OSError, UnicodeError):
        return None


def _number(text, unlimited=False):
    if text is None:
        return None
    text = text.strip()
    if unlimited and text == 'max':
        return 'max'
    try:
        return int(text) if text.isascii() and text.isdecimal() else None
    except ValueError:  # Python's integer digit limit: malformed proc fixtures must be unknown.
        return None


def _events(text):
    if text is None:
        return None
    rows = [line.split() for line in text.splitlines() if line.split() and line.split()[0] == 'high']
    return _number(rows[0][1]) if len(rows) == 1 and len(rows[0]) == 2 else None


def _psi(text, kind):
    if text is None:
        return None
    rows = [line.split() for line in text.splitlines() if line.split() and line.split()[0] == kind]
    if len(rows) != 1:
        return None
    vals = [v[6:] for v in rows[0][1:] if v.startswith('avg10=')]
    try:
        value = float(vals[0]) if len(vals) == 1 else float('nan')
        return value if math.isfinite(value) and 0 <= value <= 100 else None
    except ValueError:
        return None


class Sampler:
    def __init__(self, proc_root='/proc', cgroup_root='/sys/fs/cgroup'):
        self.proc_root = pathlib.Path(proc_root)
        self.cgroup_root = pathlib.Path(cgroup_root)
        self._baseline = None

    def sample(self):
        try:
            return self._sample()
        except (OSError, ValueError, RuntimeError):  # membership vanished, denied, or a symlink loop
            self._baseline = None
            return dict.fromkeys(_FIELDS) | {"known": False, "cgroup": None}

    def _sample(self):
        out = dict.fromkeys(_FIELDS)
        out.update(known=False, cgroup=None)
        if sys.platform != 'linux':
            self._baseline = None
            return out
        membership = _read(self.proc_root / 'self/cgroup')
        paths = [line[3:] for line in (membership or '').splitlines() if line.startswith('0::')]
        if len(paths) != 1 or not paths[0].startswith('/') or '..' in paths[0].split('/'):
            self._baseline = None
            return out
        path = paths[0]
        root = self.cgroup_root.resolve()
        directory = (root / path.lstrip('/')).resolve()
        if not directory.is_relative_to(root) or _read(root / 'cgroup.controllers') is None:
            self._baseline = None
            return out
        out['cgroup'] = path
        for key in ('current', 'high', 'max', 'swap_current', 'swap_max'):
            out[key] = _number(_read(directory / ('memory.' + key.replace('_', '.'))), key in ('high', 'max', 'swap_max'))
        out['high_events'] = _events(_read(directory / 'memory.events'))
        pressure = _read(directory / 'memory.pressure')
        out['some_avg10'] = _psi(pressure, 'some')
        out['full_avg10'] = _psi(pressure, 'full')
        count = out['high_events']
        if count is not None:
            out['high_delta'] = high_growth(self._baseline[1], count) if self._baseline and self._baseline[0] == path else 0
            self._baseline = (path, count)
        else:
            self._baseline = None
        out['known'] = all(out[k] is not None for k in ('high_events', 'some_avg10', 'full_avg10'))
        for limit in ("high", "max"):
            bound, current = out[limit], out["current"]
            out[limit + "_headroom"] = max(0, bound - current) if isinstance(bound, int) and isinstance(current, int) else None
        return out


def breached(sample):
    """Partial readings may still establish pressure; absent metrics never invent it."""
    return pressure_bad(sample.get('high_delta') or 0, sample.get('full_avg10') or 0,
                        sample.get('some_avg10') or 0)
