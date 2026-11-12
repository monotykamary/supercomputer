"""Bounded, single-flight state exports; no compression under scheduler locks.

Atomic-replace files are pinned by descriptor; append journals use a fixed,
complete-record prefix. Only the scheduler queue's pinning needs its short
snapshot guard. This is a recovery-file snapshot, not a transaction with the
independently running treadmill. Disposable blobs, code and earlier backups
are pruned before traversal, never recursively scanned and then filtered.
"""
import collections
import contextlib
import io
import json
import os
import pathlib
import stat
import tarfile
import threading
import time
import tempfile
import weakref

CACHE_SECONDS = 30.0
WAIT_SECONDS = 60.0
_SKIP_DIRS = {"agents", "keys", "blobs", "index", "code", "controller-state", "state-archives", "__pycache__", "olean"}
_SKIP_FILES = {"token", "boat-key", "controller.json", "log", "treadmill.log"}
_JOURNALS = {".jsonl", ".placed", ".rows", ".cancel"}


class _Entry:
    def __init__(self, fd, name, st):
        self.fd, self.name, self.size = fd, name, st.st_size
        self.mode, self.mtime = stat.S_IMODE(st.st_mode), st.st_mtime
        self.payload = None

    def close(self):
        os.close(self.fd)

    def freeze_json(self):
        if pathlib.PurePosixPath(self.name).suffix == ".json":
            self.payload = os.pread(self.fd, self.size, 0)
            if len(self.payload) != self.size:
                raise ValueError(f"state record changed while captured: {self.name}")
            json.loads(self.payload)  # never publish a torn in-place metadata write

    def complete_prefix(self):
        # A different process may have been midway through an append at pin time.
        # Never export a torn JSONL tail or include bytes appended after that time.
        if pathlib.PurePosixPath(self.name).suffix not in _JOURNALS or not self.size:
            return
        end = self.size
        while end:
            start = max(0, end - 65536)
            chunk = os.pread(self.fd, end - start, start)
            last = chunk.rfind(b"\n")
            if last >= 0:
                self.size = start + last + 1
                return
            end = start
        self.size = 0


class _Reader:
    def __init__(self, entry):
        self.entry, self.offset = entry, 0

    def read(self, n=-1):
        remaining = self.entry.size - self.offset
        n = remaining if n < 0 else min(n, remaining)
        data = os.pread(self.entry.fd, n, self.offset)
        self.offset += len(data)
        return data


def _paths(root, *, treadmill=False):
    if not root.exists():
        return
    for directory, dirs, files in os.walk(root, followlinks=False):
        dirs[:] = sorted(d for d in dirs if d not in _SKIP_DIRS
                         and not (treadmill and d in {"trees", "unpacked", "commits"})
                         and not pathlib.Path(directory, d).is_symlink()
                         and pathlib.Path(directory, d) not in {root / "scheduler" / "queue", root / "scheduler" / "trees"})
        for name in sorted(files):
            if name in _SKIP_FILES or name.endswith((".lock", ".tmp", ".bundle", ".log")):
                continue
            if treadmill and name.endswith(".tar.gz"):
                if name != "tree.tar.gz":
                    continue
                meta = pathlib.Path(directory, "meta.json")
                if not meta.exists() or json.loads(meta.read_text()).get("status") not in {"queued", "running"}:
                    continue
            yield pathlib.Path(directory, name)


def _pin(path, name, entries, *, required=False):
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode):
            return
        entries.append(_Entry(fd, name, st))
        fd = None
    except FileNotFoundError:
        if required:
            raise
    except OSError:
        # In-flight/non-regular links are not durable backup inputs. Fail closed
        # for queue records rather than return an apparently complete backup.
        if required or not path.is_symlink():
            raise
    finally:
        if fd is not None:
            os.close(fd)


@contextlib.contextmanager
def capture(live: pathlib.Path, treadmill: pathlib.Path, guard=contextlib.nullcontext):
    entries = []
    try:
        for root, prefix, tm in ((live, "live", False), (treadmill, "treadmill", True)):
            for path in _paths(root, treadmill=tm):
                if not tm and path == live / "scheduler" / "durable-groups.json":
                    continue  # pinned with admissions under the placement lock below
                _pin(path, f"{prefix}/{path.relative_to(root).as_posix()}", entries)
        # This flat directory contains admission JSON and append-only journals.
        # Pin their generations/lengths together, without reading their payloads.
        queue = live / "scheduler" / "queue"
        with guard() as state:
            closed = state.get("closed", set()) if isinstance(state, dict) else set()
            registry = live / "scheduler" / "durable-groups.json"
            if registry.exists():
                _pin(registry, "live/scheduler/durable-groups.json", entries, required=True)
            if queue.exists():
                paths = sorted(queue.iterdir())
                admitted = {p.stem for p in paths if p.suffix in {".json", ".closed", ".retired"}}
                for path in paths:
                    if path.stem not in admitted:
                        continue  # orphan history is not read by admission recovery
                    if path.suffix in {".json", ".closed", ".retired", ".placed", ".attempts", ".rows", ".cancel"}:
                        _pin(path, f"live/scheduler/queue/{path.name}", entries, required=True)
                    elif path.suffix == '.results' and not path.is_symlink():
                        # These immutable files are required even by closed retained admissions.
                        for payload in _paths(path):
                            if payload.suffix == '.payload':
                                _pin(payload, f'live/scheduler/queue/{payload.relative_to(queue).as_posix()}', entries, required=True)
                    elif path.suffix == ".logs" and (path.stem not in closed or (queue / (path.stem + ".closed")).exists()) and not path.is_symlink():
                        # Keep unacknowledged results, not outputs of closed batches.
                        for output in _paths(path):
                            _pin(output, f"live/scheduler/queue/{output.relative_to(queue).as_posix()}", entries, required=True)
        trees = set()
        for entry in entries:
            entry.complete_prefix()
            entry.freeze_json()
            if entry.name.startswith("live/scheduler/queue/") and entry.name.endswith(".json"):
                tree = json.loads(entry.payload).get("tree", {})
                if isinstance(tree, dict) and "files" not in tree and tree.get("id"):
                    ident = tree["id"]
                    if not isinstance(ident, str) or not ident or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in ident):
                        raise ValueError("unsafe state tree identifier")
                    trees.add(ident)
        for ident in sorted(trees):
            path = live / "scheduler" / "trees" / f"{ident}.json"
            _pin(path, f"live/scheduler/trees/{ident}.json", entries, required=True)
            entries[-1].freeze_json()
        yield entries
    finally:
        for entry in entries:
            entry.close()


class FilePayload:
    """An immutable temporary archive; each consumer opens an independent stream."""
    def __init__(self, path):
        self.path = pathlib.Path(path)
        self.size = self.path.stat().st_size
        self._cleanup = weakref.finalize(self, self.path.unlink, missing_ok=True)

    def __len__(self):
        return self.size

    def open(self):
        return self.path.open('rb')


def _prune_archives(directory):
    """Reclaim only this cache's files left by dead processes (including os._exit)."""
    for path in directory.glob('state-*-*.tar.gz'):
        owner = path.name.split('-', 2)[1]
        if not owner.isdecimal() or int(owner) < 1 or int(owner) == os.getpid():
            continue
        try:
            os.kill(int(owner), 0)  # existence probe only; never signal a process
        except ProcessLookupError:
            with contextlib.suppress(FileNotFoundError):
                if stat.S_ISREG(path.lstat().st_mode):
                    path.unlink(missing_ok=True)
        except OSError:
            pass  # uncertain ownership or permission: preserve it


def archive(entries, *, file_backed=False, directory=None):
    if file_backed:
        with tempfile.NamedTemporaryFile(prefix=f'state-{os.getpid()}-', suffix='.tar.gz', dir=directory, delete=False) as buf:
            name = buf.name
            try:
                _archive(entries, buf)
                buf.flush()
                os.fsync(buf.fileno())
            except BaseException:
                os.unlink(name)
                raise
        return FilePayload(name)
    buf = io.BytesIO()
    _archive(entries, buf)
    return buf.getvalue()


def _archive(entries, buf):
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=1) as tf:
        for entry in entries:
            info = tarfile.TarInfo(entry.name)
            info.size, info.mode, info.mtime = entry.size, entry.mode, entry.mtime
            tf.addfile(info, io.BytesIO(entry.payload) if entry.payload is not None else _Reader(entry))
        manifest = json.dumps({"format": 1, "purpose": "controller control-plane recovery",
            "files": len(entries), "source_bytes": sum(e.size for e in entries),
            "external_requirements": ["reinstall recorded code", "recreate credentials",
                "recover content-addressed inputs from surviving workers or their original sources"]}).encode()
        info = tarfile.TarInfo("backup-manifest.json")
        info.size = len(manifest)
        tf.addfile(info, io.BytesIO(manifest))


def build(live, treadmill, guard=contextlib.nullcontext, metadata=None, *, file_backed=False):
    directory = pathlib.Path(live) / 'scheduler' / 'state-archives'
    if file_backed:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        _prune_archives(directory)
    with capture(pathlib.Path(live), pathlib.Path(treadmill), guard) as entries:
        data = archive(entries, file_backed=file_backed, directory=directory if file_backed else None)
        if metadata is not None:
            sizes, counts = collections.Counter(), collections.Counter()
            for entry in entries:
                group = "/".join(entry.name.split("/")[:3])
                sizes[group] += entry.size
                counts[group] += 1
            metadata.update(archive_bytes=len(data), source_bytes=sum(e.size for e in entries), files=len(entries),
                contents=[{"path": key, "bytes": size, "files": counts[key]} for key, size in sizes.most_common()],
                excluded=["regenerable commit workspaces", "unreferenced tree manifests", "saved code", "build caches",
                          "diagnostic logs", "closed/orphan batch outputs", "previous backups", "credentials and blobs"])
        return data


class ArchiveCache:
    """One in-flight build and one immutable result per scheduler process."""
    def __init__(self, ttl=CACHE_SECONDS, wait=WAIT_SECONDS, clock=time.monotonic):
        self.ttl, self.wait, self.clock = ttl, wait, clock
        self.condition = threading.Condition()
        self.building, self.data, self.at = False, None, 0.0
        self.metadata, self.error = {}, None
        self.builds, self.hits = 0, 0

    def status(self):
        with self.condition:
            return {**self.metadata, "building": self.building, "builds": self.builds, "cache_hits": self.hits,
                    "age_s": round(max(0, self.clock() - self.at), 1) if self.data is not None else None,
                    "error": self.error}

    def get(self, builder, metadata=None):
        deadline = self.clock() + self.wait
        with self.condition:
            while True:
                now = self.clock()
                if self.data is not None and 0 <= now - self.at < self.ttl:
                    self.hits += 1
                    return self.data
                if not self.building:
                    self.building = True
                    break
                remaining = deadline - now
                if remaining <= 0:
                    raise TimeoutError("state archive build still in progress")
                self.condition.wait(remaining)
        try:
            data = builder()
        except BaseException as exc:
            with self.condition:
                self.building = False
                self.error = str(exc)[-300:]
                self.condition.notify_all()
            raise
        with self.condition:
            self.data, self.at, self.building = data, self.clock(), False
            self.metadata, self.error = dict(metadata or {"archive_bytes": len(data)}), None
            self.builds += 1
            self.condition.notify_all()
            return data
