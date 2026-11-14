"""Durable, fail-closed controller image retention (wire format version 1).

Integration API: ImagePins(path).register(ref, images, kind, now) for public
packet/go observations; pin_batch(ref, images, now) for internal live batches;
mark_batch_closed(ref, now) ONLY after the caller positively completes cleanup.
There is deliberately no unpin, expiry sweep, or deletion authorization API.

snapshot(now) returns version/epoch/revision, computed records and protected
images. get(ref, now) returns a computed record or None. protected(now) maps
images to lists of reason records. All results are detached dictionaries.
Times MUST come from the controller, never an untrusted client. Deadlines are
inclusive (even at exactly seven days we retain). Replays preserve the exact
ordered image list and kind. Retired records remain immutable tombstones.

One controller owns this path; advisory locking also serializes local processes.
Every operation reloads under that lock so a failed handoff cannot use a stale cache.
This does not fence separate hosts with independent state copies.
Persist/restore this file with controller state. An unreadable/malformed file
or uncertain write poisons the instance: every operation raises, so callers
must fail closed, not substitute an empty protected set. Client auto-deletion
must remain disabled; this registry is not a cross-network deletion lease.
"""

import contextlib
import copy
import fcntl
import json
import math
import os
from pathlib import Path
import re
import tempfile
import threading
import uuid

from .rules.image_pins import image_pin_retained

RETENTION_SECONDS = 604800
MAX_NAME_LENGTH = 256
MAX_IMAGES = 4096
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}", re.ASCII)


class ImagePinsUnavailable(RuntimeError):
    """The registry cannot safely answer retention questions."""


def _name(value):
    if not isinstance(value, str) or not _NAME.fullmatch(value):
        raise ValueError('unsafe image/ref (1..256 ASCII letters/digits/_.:-)')
    return value


def _time(value):
    if type(value) not in (int, float):
        raise ValueError('time must be a finite nonnegative number, not boolean')
    try:
        valid = math.isfinite(value) and 0 <= value <= 1e12
    except (OverflowError, TypeError):
        valid = False
    if not valid:
        raise ValueError('time outside supported range [0, 1e12]')
    return value


def _deadline(now):
    result = now + RETENTION_SECONDS
    # Floating-point rounding must never take even a fraction off the grace.
    if result - now < RETENTION_SECONDS:
        result = math.nextafter(result, math.inf)
    return result


def _images(images):
    if not isinstance(images, list) or not 1 <= len(images) <= MAX_IMAGES:
        raise ValueError('images must be a nonempty bounded list')
    for image in images:
        _name(image)
    if len(set(images)) != len(images):
        raise ValueError('duplicate image')
    return list(images)


def retain_image(live, protected_until, now):
    return image_pin_retained(bool(live), protected_until is not None and now <= protected_until)


def _unique_object(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError('duplicate JSON key')
        obj[key] = value
    return obj


class ImagePins:
    def __init__(self, path):
        self.path = Path(path)
        self._data = None
        self._error = None
        self._lock = threading.RLock()

    @contextlib.contextmanager
    def _guard(self):
        with self._lock:
            if self._error is not None:
                raise ImagePinsUnavailable('image pin registry unavailable: ' + self._error)
            try:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                with self.path.with_name(self.path.name + '.lock').open('a') as lock:
                    fcntl.flock(lock, fcntl.LOCK_EX)
                    try:
                        yield
                    finally:
                        fcntl.flock(lock, fcntl.LOCK_UN)
            except OSError as error:
                self._poison(error)

    def _poison(self, error):
        self._data = None  # Never serve cached pre-write state after uncertainty.
        self._error = str(error) or type(error).__name__
        raise ImagePinsUnavailable('image pin registry unavailable: ' + self._error) from error

    def _load(self):
        if self._error is not None:
            raise ImagePinsUnavailable('image pin registry unavailable: ' + self._error)
        previous = self._data
        try:
            try:
                with self.path.open(encoding='utf-8') as stream:
                    data = json.load(stream, object_pairs_hook=_unique_object)
            except FileNotFoundError:
                # A disappeared known registry or dangling link is not empty state.
                if previous is not None or self.path.is_symlink():
                    raise ValueError('dangling registry symlink')
                data = {'version': 1, 'epoch': uuid.uuid4().hex,
                        'revision': 0, 'records': {}}
                self._persist(data)
                return self._data
            self._validate(data)
            if previous is not None and (data["epoch"] != previous["epoch"] or data["revision"] < previous["revision"]):
                raise ValueError("image pin registry epoch/revision regressed")
            self._data = data
            return data
        except Exception as error:
            self._poison(error)

    @staticmethod
    def _validate(data):
        if not isinstance(data, dict) or set(data) != {'version', 'epoch', 'revision', 'records'}:
            raise ValueError('invalid registry fields')
        if type(data['version']) is not int or data['version'] != 1:
            raise ValueError('unsupported registry version')
        if not isinstance(data['epoch'], str) or not re.fullmatch('[0-9a-f]{32}', data['epoch']):
            raise ValueError('invalid registry epoch')
        if type(data['revision']) is not int or data['revision'] < 0 or not isinstance(data['records'], dict):
            raise ValueError('invalid registry revision/records')
        if len(data['records']) > data['revision']:
            raise ValueError('records exceed revision')
        fields = {'ref', 'images', 'kind', 'first_observed_at', 'last_observed_at',
                  'closed_at', 'protected_until'}
        for ref, record in data['records'].items():
            _name(ref)
            if not isinstance(record, dict) or set(record) != fields or record['ref'] != ref:
                raise ValueError('invalid record fields/ref')
            _images(record['images'])
            if record['kind'] not in ('packet', 'go', 'batch'):
                raise ValueError('invalid pin kind')
            first = _time(record['first_observed_at'])
            last = _time(record['last_observed_at'])
            if last < first:
                raise ValueError('observation moved backwards')
            closed, until = record['closed_at'], record['protected_until']
            if record['kind'] == 'batch' and closed is None:
                if until is not None:
                    raise ValueError('live batch cannot expire')
            else:
                if record['kind'] != 'batch' and closed is not None:
                    raise ValueError('public pin cannot close')
                if closed is not None and not first <= _time(closed) <= last:
                    raise ValueError('invalid batch closure')
                # Deadlines may be up to seven days beyond the time bound.
                if type(until) not in (int, float) or not math.isfinite(until):
                    raise ValueError('invalid deadline')
                if until < _deadline(last):
                    raise ValueError('retention shorter than seven days')

    def _persist(self, data):
        temporary = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                             prefix='.' + self.path.name + '-',
                                             suffix='.tmp', dir=self.path.parent,
                                             delete=False) as stream:
                temporary = stream.name
                json.dump(data, stream, sort_keys=True, allow_nan=False)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            temporary = None
            fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
            self._data = data
        except Exception as error:
            self._poison(error)
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass  # Unpublished temporary file is never read as the registry.

    @property
    def epoch(self):
        with self._guard():
            return self._load()['epoch']

    @property
    def revision(self):
        with self._guard():
            return self._load()['revision']

    @staticmethod
    def _view(record, now):
        result = copy.deepcopy(record)
        result['live'] = record['kind'] == 'batch' and record['closed_at'] is None
        result['protected'] = retain_image(result['live'], record['protected_until'], now)
        result['reason'] = ('live_batch' if result['live'] else
                            'batch_grace' if record['kind'] == 'batch' else record['kind'])
        return result

    def get(self, ref, now):
        with self._guard():
            data = self._load()
            _name(ref)
            _time(now)
            record = data['records'].get(ref)
            return None if record is None else self._view(record, now)

    def snapshot(self, now):
        with self._guard():
            data = self._load()
            _time(now)
            result = {key: data[key] for key in ('version', 'epoch', 'revision')}
            result['records'] = {ref: self._view(r, now) for ref, r in data['records'].items()}
            result['protected'] = {}
            for record in result['records'].values():
                if record['protected']:
                    for image in record['images']:
                        result['protected'].setdefault(image, []).append(copy.deepcopy(record))
            return result

    def protected(self, now):
        return self.snapshot(now)['protected']

    def _register(self, ref, images, kind, now):
        with self._guard():
            data = self._load()
            _name(ref)
            images = _images(images)
            _time(now)
            old = data['records'].get(ref)
            if old is not None:
                if old['images'] != images or old['kind'] != kind:
                    raise ValueError('ref already bound to different images/kind')
                if kind == 'batch' and old['closed_at'] is not None:
                    raise ValueError('closed batch cannot reopen')
                record = copy.deepcopy(old)
                record['last_observed_at'] = max(old['last_observed_at'], now)
                if kind != 'batch':
                    record['protected_until'] = max(old['protected_until'], _deadline(now))
            else:
                record = {'ref': ref, 'images': images, 'kind': kind,
                          'first_observed_at': now, 'last_observed_at': now,
                          'closed_at': None,
                          'protected_until': None if kind == 'batch' else _deadline(now)}
            if old != record:
                updated = copy.deepcopy(data)
                updated['records'][ref] = record
                updated['revision'] += 1
                self._persist(updated)
            return self._view(record, now)

    def register(self, ref, images, kind, now):
        """Observe a public packet/go binding; no caller-supplied expiry."""
        if kind not in ('packet', 'go'):
            raise ValueError('public kind must be packet or go')
        return self._register(ref, images, kind, now)

    def pin_batch(self, ref, images, now):
        """Pin an internal batch indefinitely until positive cleanup."""
        return self._register(ref, images, 'batch', now)

    def mark_batch_closed(self, ref, now):
        """Caller attests positive cleanup; retain for >=7 days from observation."""
        with self._guard():
            data = self._load()
            _name(ref)
            _time(now)
            old = data['records'].get(ref)
            if old is None or old['kind'] != 'batch':
                raise ValueError('unknown internal batch')
            record = copy.deepcopy(old)
            observed = max(old['last_observed_at'], now)
            record['last_observed_at'] = observed
            if record['closed_at'] is None:
                record['closed_at'] = observed
            record['protected_until'] = max(old['protected_until'] or 0, _deadline(observed))
            if record != old:
                updated = copy.deepcopy(data)
                updated['records'][ref] = record
                updated['revision'] += 1
                self._persist(updated)
            return self._view(record, now)
