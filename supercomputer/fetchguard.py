"""Client-side output inventory policy; never deletes or mutates remote data."""
import heapq
import json
import os
import re
import threading

DEFAULT_LIMIT = 1024 ** 3


def resolve_limit(value=None, *, allow_large=False):
    """Bytes, or a binary KiB/MiB/GiB suffix. Zero permits only empty files."""
    if type(allow_large) is not bool:
        raise ValueError("allow_large must be boolean")
    if allow_large:
        return None
    if value is None:
        value = os.environ.get('SUPERCOMPUTER_FETCH_LIMIT', str(DEFAULT_LIMIT))
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str):
        match = re.fullmatch(r'\s*(\d+)\s*(B|KiB|MiB|GiB)?\s*', value, re.IGNORECASE)
        if match:
            factors = {'b': 1, 'kib': 1024, 'mib': 1024**2, 'gib': 1024**3}
            return int(match[1]) * factors.get((match[2] or 'B').lower(), 1)
    raise ValueError('SUPERCOMPUTER_FETCH_LIMIT must be nonnegative bytes or an integer KiB/MiB/GiB size; use --allow-large to bypass')


class FetchLimitExceeded(RuntimeError):
    """Output bodies were not fetched. Verified metadata may already be local."""


class FetchGuard:
    def __init__(self, limit=None, *, allow_large=False, batch=''):
        self.limit = resolve_limit(limit, allow_large=allow_large)
        self.batch = batch
        self.items = {}
        self.total = 0
        self.largest = []
        self.lock = threading.RLock()

    def observe(self, name, manifest_ref, descriptors):
        """Account logical destination bytes, including identical content at two paths.

        A re-read of the same item/manifest is idempotent. Callers validate all
        descriptors before transfer; memory is O(items + ten largest paths).
        """
        identity = (manifest_ref['sha256'], manifest_ref['bytes'])
        with self.lock:
            previous = self.items.get(name)
            if previous is not None:
                if previous['identity'] != identity:
                    raise ValueError('output manifest changed for item ' + repr(name))
                return
            total, count, largest = 0, 0, list(self.largest)
            for entry in descriptors:
                size = entry['bytes']
                total += size
                count += 1
                heapq.heappush(largest, (size, name, entry['path']))
                if len(largest) > 10:
                    heapq.heappop(largest)
            self.items[name] = {'identity': identity, 'bytes': total, 'files': count}
            self.total += total
            self.largest = largest

    def check(self):
        with self.lock:
            if self.limit is None or self.total <= self.limit:
                return
            label = ' for batch ' + self.batch if self.batch else ''
            lines = [f'Refusing output-file download{label}: {self.total} bytes exceed fetch limit {self.limit} bytes.',
                     'Per-item output sizes:']
            for name, item in sorted(self.items.items(), key=lambda pair: (-pair[1]['bytes'], pair[0])):
                lines.append(f'  {json.dumps(name, ensure_ascii=True)}: {item["bytes"]} bytes ({item["files"]} files)')
            lines.append('Largest output paths (up to 10):')
            for size, name, path in sorted(self.largest, reverse=True):
                lines.append(f'  {size} bytes  {json.dumps(name + "/" + path, ensure_ascii=True)}')
            lines.append('Outputs remain retrievable remotely; nothing was deleted. Export results only, or retry with --allow-large / a larger SUPERCOMPUTER_FETCH_LIMIT.')
            if self.batch:
                lines.append(f'For retained scheduler admissions, recover with: supercomputer batch fetch {self.batch} --out NEW_DIRECTORY --allow-large')
            raise FetchLimitExceeded('\n'.join(lines))
