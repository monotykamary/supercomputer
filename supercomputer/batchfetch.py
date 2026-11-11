"""Read-only, resumable fetch of an existing completed scheduler admission."""
import contextlib
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import time
import uuid

from . import clientresults, resultfiles


@contextlib.contextmanager
def _atomic(directory, name, binary=False):
    with clientresults._directory(directory) as parent:
        try:
            info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISREG(info.st_mode):
                raise ValueError("fetch destination is not a regular file")
        temporary = '.fetch-' + uuid.uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, 'wb' if binary else 'w') as output:
                yield output
                output.flush()
                os.fsync(output.fileno())
            os.replace(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=parent)


def _json_file(directory, name, value):
    with _atomic(directory, name) as output:
        json.dump(value, output, indent=2)
        output.write('\n')


def _read_json(directory, name):
    with clientresults._directory(directory) as parent:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd) as source:
            if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
                raise ValueError("fetch marker is not a regular file")
            return json.load(source)


def _info(client, bid, timeout):
    info = client.req('/batch/info', {'batch': bid}, timeout=timeout)
    if not isinstance(info, dict) or info.get('ok') is not True or info.get('batch') != bid:
        raise RuntimeError("batch is missing or unavailable; no results were fetched")
    for key in ('items', 'rows'):
        if type(info.get(key)) is not int or info[key] < 0:
            raise ValueError("invalid batch inventory")
    if info.get('complete') is not True or info['items'] != info['rows']:
        raise RuntimeError("batch is not finished with every result retained; fetch does not wait or close it")
    if not isinstance(info.get('generation'), str) or not info['generation']:
        raise ValueError("batch has no authoritative admission generation")
    admitted = info.get('admitted_at')
    if isinstance(admitted, bool) or not isinstance(admitted, (int, float)) or not math.isfinite(admitted):
        raise ValueError("invalid admission timestamp")
    return info


def _verified_stream(directory, name, *, limit=None):
    def decode(response):
        expected = response.getheader('X-SC-Content-SHA256', '')
        if not re.fullmatch('[0-9a-f]{64}', expected):
            raise ValueError("controller omitted the stream SHA-256; upgrade the controller before fetching")
        try:
            length = int(response.getheader('Content-Length', ''))
        except (TypeError, ValueError) as exc:
            raise ValueError("controller omitted the stream byte count") from exc
        if length < 0 or (limit is not None and length > limit):
            raise ValueError("invalid or excessive stream byte count")
        digest, size = hashlib.sha256(), 0
        with _atomic(directory, name, binary=True) as output:
            while True:
                chunk = response.read(resultfiles.CHUNK_BYTES)
                if not chunk:
                    break
                size += len(chunk)
                if size > length:
                    raise ValueError("stream exceeds its byte count")
                output.write(chunk)
                digest.update(chunk)
            if size != length or digest.hexdigest() != expected:
                raise ValueError("stream size or SHA-256 mismatch")
        return {'file': str(directory / name), 'bytes': size,
                'sha256': expected, 'verified': True}
    return decode


def _logs(client, bid, directory, timeout):
    decode = _verified_stream(directory, 'logs.tar.gz')
    # No VM fallback: fetch only the controller's retained logs, not a later
    # unrelated worker incarnation. A raw archive is never extracted here.
    return client.req('/logs', {'batch': bid, 'vms': [], 'require_complete': True}, timeout=timeout, decode=decode)


def fetch(bid: str, out, *, client=None, timeout=120):
    """Fetch a finished admission without submit, close, cancel, reap or startup.

    Every row page, referenced row/result/output payload and the log archive is checked
    against its controller-supplied SHA-256. Large JSON stays on disk. Failed
    downloads leave verified payloads reusable, but never publish fetch.json.
    Use an empty directory, or resume a fetch of this same admission generation.
    """
    from . import scheduler
    scheduler.Scheduler._valid_batch(bid)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    if client is None:
        client = scheduler.current()  # never connect()/ensure_running()
    if client is None:
        raise RuntimeError("no running scheduler; fetch never starts one")
    caps = client.req('/capabilities', {}, timeout=timeout)
    if not isinstance(caps, dict) or type(caps.get('batch_fetch_protocol')) is not int or caps['batch_fetch_protocol'] != 1:
        raise RuntimeError("controller lacks batch_fetch_protocol:1; upgrade it before fetching")
    before = _info(client, bid, timeout)
    directory = Path(out).absolute()
    started = time.time()
    with clientresults._directory(directory) as parent:
        lockfd = os.open('.fetch.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=parent)
        with os.fdopen(lockfd, 'w') as lock:
            if not stat.S_ISREG(os.fstat(lock.fileno()).st_mode):
                raise ValueError("fetch lock is not a regular file")
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            identity = {'batch': bid, 'generation': before['generation']}
            try:
                previous = _read_json(directory, 'batch.json')
            except FileNotFoundError:
                if any(name != '.fetch.lock' for name in os.listdir(parent)):
                    raise ValueError("fetch output must be empty or belong to this admission")
                _json_file(directory, 'batch.json', identity)
            else:
                if previous != identity:
                    raise ValueError("fetch output belongs to a different admission")
            # A failed repeat must not leave yesterday's success receipt next
            # to a newly replaced row stream or log archive.
            try:
                previous_receipt = os.stat('fetch.json', dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                if not stat.S_ISREG(previous_receipt.st_mode):
                    raise ValueError("fetch receipt is not a regular file")
                os.unlink('fetch.json', dir_fd=parent)
                os.fsync(parent)
            names, count, since, row_hash = set(), 0, 0, hashlib.sha256()
            pages = []
            with _atomic(directory, 'rows.jsonl', binary=True) as output:
                while True:
                    page_name = f'{since:012d}.json'
                    page_info = client.req('/batch/rows', {'batch': bid, 'since': since}, timeout=timeout,
                        decode=_verified_stream(directory / 'pages', page_name,
                                                limit=resultfiles.POLL_BYTES + resultfiles.INLINE_BYTES + 1024))
                    pages.append(dict(page_info, since=since))
                    page = _read_json(directory / 'pages', page_name)
                    if not isinstance(page, dict) or page.get('err') or page.get('error') or not isinstance(page.get('rows'), list):
                        raise RuntimeError("batch result page is missing or invalid")
                    rows = page['rows']
                    if not rows:
                        if page.get('done') is not True:
                            raise RuntimeError("batch became incomplete during fetch")
                        break
                    next_since = page.get('next_since')
                    if type(next_since) is not int or next_since != since + len(rows):
                        raise ValueError("non-contiguous batch result cursor")
                    for row in rows:
                        if not isinstance(row, dict):
                            raise ValueError("invalid batch result row")
                        name = clientresults._name(row)
                        if name in names:
                            raise ValueError("duplicate result row")
                        names.add(name)
                        def read(ref, offset, length):
                            return client.req('/results', {'batch': bid, 'ref': ref, 'offset': offset, 'length': length}, timeout=timeout, raw=True)
                        result = clientresults.download(row, read, directory, verify_cached=True)
                        line = (json.dumps(result, separators=(',', ':')) + '\n').encode()
                        output.write(line)
                        row_hash.update(line)
                        count += 1
                    since = next_since
                    if count > before['items']:
                        raise ValueError("batch result inventory grew unexpectedly")
                if count != before['items']:
                    raise RuntimeError("finished batch has missing result rows")
                logs = _logs(client, bid, directory, timeout)
                after = _info(client, bid, timeout)
                if any(after[k] != before[k] for k in ('generation', 'admitted_at', 'items', 'rows')):
                    raise RuntimeError("admission changed during fetch")
            result = {'batch': bid, 'generation': before['generation'], 'read_only': True,
                      'complete': True, 'closed_by_fetch': False, 'rows': count,
                      'rows_file': str(directory / 'rows.jsonl'), 'rows_sha256': row_hash.hexdigest(),
                      'logs': logs, 'pages': pages, 'before': before, 'after': after,
                      'started_at': started, 'finished_at': time.time()}
            _json_file(directory, 'fetch.json', result)
            return result
