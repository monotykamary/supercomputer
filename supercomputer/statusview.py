"""An immutable, pre-encoded status view. Readers never take publisher locks.

Only complete snapshots are published. Failed, blocked or out-of-order refreshes
keep the old snapshot and its original capture time, so staleness stays visible.
"""
import json
import time

from .rules.status import status_snapshot_age, status_snapshot_newer


class PublishedStatus:
    def __init__(self, initial):
        self._record = (0, -1, None, None, self._prefix(initial), None)

    @staticmethod
    def _prefix(value):
        value = dict(value)
        value.pop('status_snapshot', None)
        raw = json.dumps(value, separators=(',', ':')).encode()
        return raw[:-1] + (b',' if len(raw) > 2 else b'')

    def publish(self, value, started_ms, captured_at):
        prefix = self._prefix(value)
        old = self._record
        if not status_snapshot_newer(started_ms, old[1]):
            return False
        # One tuple assignment publishes bytes and capture metadata together.
        self._record = (old[0] + 1, started_ms, captured_at, time.time(), prefix, None)
        return True

    def failed(self, error):
        self._record = (*self._record[:-1], str(error)[:300])

    def response(self):
        generation, started_ms, captured_at, published_at, prefix, error = self._record
        metadata = dict(ready=generation > 0, generation=generation,
                        published_at=published_at, captured_at=captured_at,
                        age_s=(status_snapshot_age(time.monotonic_ns() // 1000000, started_ms) / 1000
                               if generation else None),
                        refresh_error=error)
        return prefix + b'"status_snapshot":' + json.dumps(metadata, separators=(',', ':')).encode() + b'}'
