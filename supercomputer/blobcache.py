"""Bounded, reference-aware local CAS. File bytes and queued manifests share an OS lock.

No evidence is archived here: use `supercomputer store` for durable retention.
A missing blob is uploaded again by its client. Queued manifests pin their blobs;
recent access also pins uploads not submitted yet. The byte target is soft when
active/recent data alone exceeds it; status reports that debt, never deletes it.
"""
import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import pathlib
import re
import stat
import tempfile
import time

from .rules.blobcache import BlobEviction, CompletionPin

KEY = re.compile(r"[0-9a-f]{64}x?\Z")


def key_path(root: pathlib.Path, key: str) -> pathlib.Path:
    if not isinstance(key, str) or not KEY.fullmatch(key):
        raise ValueError("invalid blob key")
    return root / key


@contextlib.contextmanager
def locked(root: pathlib.Path):
    """Shared by clients, admission and collection, including separate processes."""
    root.parent.mkdir(parents=True, exist_ok=True)
    with open(root.parent / "blob-cache.lock", "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def have(root: pathlib.Path, keys: list[str]) -> list[str]:
    """Renew access before returning a hit: even an old tree must check its blobs."""
    missing = []
    with locked(root):
        for key in keys:
            p = key_path(root, key)
            try:
                if not stat.S_ISREG(p.lstat().st_mode):
                    raise FileNotFoundError(key)
                os.utime(p, None, follow_symlinks=False)
            except FileNotFoundError:
                missing.append(key)
    return missing


def put(root: pathlib.Path, key: str, data: bytes):
    p = key_path(root, key)
    if hashlib.sha256(data).hexdigest() != key.rstrip("x"):
        raise ValueError("blob bytes do not match their key")
    with locked(root):
        root.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=".upload-", dir=root)
        try:
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            os.chmod(name, 0o755 if key.endswith("x") else 0o644)
            os.replace(name, p)
        finally:
            pathlib.Path(name).unlink(missing_ok=True)


def read(root: pathlib.Path, key: str) -> bytes:
    # Open while locked; unlink after unlocking cannot change an open descriptor.
    with locked(root):
        p = key_path(root, key)
        fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise ValueError("blob is not a regular file")
            os.utime(p, None, follow_symlinks=False)
        except BaseException:
            os.close(fd)
            raise
    with os.fdopen(fd, "rb") as f:
        return f.read()


def completed(root: pathlib.Path, bid: str, generation: str | None, pending: int) -> bool:
    """Release only this exact completed admission, without deleting its results."""
    if not generation or not re.fullmatch(r"[0-9a-f]{32}", generation):
        return False  # legacy queue records remain conservatively pinned
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", bid):
        raise ValueError("invalid batch id")
    with locked(root):
        path = root.parent / "queue" / f"{bid}.json"
        try:
            record = json.loads(path.read_text())
        except FileNotFoundError:
            return False
        if not CompletionPin().release(pending, record.get("cache_generation") == generation):
            return False
        if record.get("cache_completed_generation") != generation:
            record["cache_completed_generation"] = generation
            tmp = path.with_suffix(f".{os.getpid()}.complete.tmp")
            tmp.write_text(json.dumps(record))
            os.replace(tmp, path)
        return True


def pinned(root: pathlib.Path) -> set[str]:
    """All persisted batches, including id-only trees; malformed state fails closed."""
    keys = set()
    for p in (root.parent / "queue").glob("*.json"):
        try:
            record = json.loads(p.read_text())
        except FileNotFoundError:  # a completed batch closed meanwhile
            continue
        generation = record.get("cache_generation")
        if isinstance(generation, str) and re.fullmatch(r"[0-9a-f]{32}", generation) and record.get("cache_completed_generation") == generation:
            continue
        tree = record["tree"]
        if "files" not in tree:
            ident = tree["id"]
            if not isinstance(ident, str) or not re.fullmatch(r"[0-9a-f]{24}", ident):
                raise ValueError("invalid queued tree id")
            tree = json.loads((root.parent / "trees" / f"{ident}.json").read_text())
        files = tree["files"]
        if not isinstance(files, dict):
            raise ValueError("invalid queued tree manifest")
        for key in files.values():
            key_path(root, key)
            keys.add(key)
    return keys


def last_report(root: pathlib.Path) -> dict:
    try:
        return json.loads((root.parent / "blob-cache.json").read_text())
    except (OSError, ValueError):
        return {"scanned": False}


def collect(root: pathlib.Path, budget: int, grace: float, max_age: float,
            dry_run: bool = False, cancelled=lambda: False) -> dict:
    if budget < 0 or grace < 60 or max_age < grace:
        raise ValueError("cache budget must be nonnegative; grace >= 60s; max age >= grace")
    rule = BlobEviction()
    now = time.time()
    report = {"time": now, "budget_bytes": budget, "grace_s": grace, "max_age_s": max_age,
              "dry_run": dry_run, "removed_files": 0, "removed_bytes": 0}
    with locked(root):
        entries = []
        for p in root.iterdir() if root.exists() else []:
            if not KEY.fullmatch(p.name):
                continue
            try:
                s = p.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISREG(s.st_mode):
                entries.append((s.st_mtime, p.name, s.st_size, p, s.st_ino))
        total = sum(e[2] for e in entries)
        report.update(scanned=True, files=len(entries), before_bytes=total)
        try:
            protected = pinned(root)
        except (OSError, ValueError, KeyError, TypeError) as e:
            report.update(error=f"cannot resolve queued pins: {e}", after_bytes=total,
                          projected_bytes=total, over_budget_bytes=max(0, total - budget))
        else:
            report["pinned_bytes"] = sum(e[2] for e in entries if e[1] in protected)
            report["recent_bytes"] = sum(e[2] for e in entries if e[1] not in protected and now - e[0] < grace)
            selected, selected_bytes = 0, 0
            for mtime, key, size, p, inode in sorted(entries):
                if cancelled():
                    report["cancelled"] = True
                    break
                age = now - mtime
                if not rule.remove(key in protected, age < grace, total, budget, age >= max_age):
                    continue
                # An old client may not use the new lock yet: recheck its atomic replacement/touch.
                try:
                    s = p.lstat()
                    if (s.st_ino, s.st_size, s.st_mtime) != (inode, size, mtime):
                        continue
                    if not dry_run:
                        p.unlink()
                except FileNotFoundError:
                    continue
                total -= size
                selected += 1
                selected_bytes += size
            report.update(selected_files=selected, selected_bytes=selected_bytes,
                          removed_files=0 if dry_run else selected,
                          removed_bytes=0 if dry_run else selected_bytes,
                          after_bytes=report["before_bytes"] if dry_run else total,
                          projected_bytes=total, over_budget_bytes=max(0, total - budget))
        if not dry_run:
            dest = root.parent / "blob-cache.json"
            tmp = dest.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(report, indent=1))
            os.replace(tmp, dest)
    return report


def main(argv: list[str]):
    from . import config, scheduler
    ap = argparse.ArgumentParser(prog="supercomputer cache")
    sub = ap.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="last collection report, including protected over-budget data")
    prune = sub.add_parser("prune", help="reclaim unpinned, cold local blobs under the cache policy")
    prune.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    root = config.LIVE / "scheduler" / "blobs"
    if args.command == "status":
        report = last_report(root)
    else:
        p = scheduler.read_policy()
        report = collect(root, int(p["blob_cache_bytes"]), float(p["blob_cache_grace_s"]),
                         float(p["blob_cache_max_age_s"]), args.dry_run)
    print(json.dumps(report, indent=1))
    if report.get("error"):
        raise SystemExit(1)
