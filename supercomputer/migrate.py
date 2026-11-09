"""Once per repo that ran the pool before the tool existed (gdp-wired: its main checkout's supercomputer/.live): its
registry, starts ledger, Modal tickets, build logs, caches and store manifests move into the shared LIVE, and the old
directory becomes a link to it, so clients still on the old code share the one registry and its lock.

Under the old registry's lock (every client's registry and starts go through it): each entry is renamed into LIVE
(the same disk: the lock file keeps its inode, so a client waiting on it waits on the same lock), a file both have is
merged (the registry's VMs and starts, the ledger's lines), then the old directory gives way to the link.
"""
import collections
import fcntl
import json
import os
import pathlib
import shutil
import time

from . import config
from .rules import ops
from .config import project


def merge_registry(a: dict, b: dict) -> dict:
    out = dict(b)
    out["vms"] = {**a.get("vms", {}), **b.get("vms", {})}
    # (each timestamp's starts counted as the larger of the two registries' counts: note_starts(k) records k at one
    # timestamp, which a set would count once; rules.ops.merged_starts)
    ca, cb = collections.Counter(a.get("starts", [])), collections.Counter(b.get("starts", []))
    out["starts"] = sorted(t for t in set(ca) | set(cb) for _ in range(ops.merged_starts(ca[t], cb[t])))
    for k in ("demand", "image_t", "images_of"):
        out[k] = {**a.get(k, {}), **b.get(k, {})}
    out["images"] = b.get("images") or a.get("images", [])
    return out


def main(src: str = ""):
    m = config.main_checkout(project().root)
    cands = [pathlib.Path(src)] if src else [m / "supercomputer" / ".live", m / "modal" / ".live"]
    old = next((c for c in cands if c.exists() and not c.is_symlink() and c.is_dir()), None)
    live = config.LIVE
    if old is None:
        links = [c for c in cands if c.is_symlink()]
        print("nothing to move" + "".join(f"; {c} -> {os.readlink(c)}" for c in links))
        return
    if old.resolve() == live.resolve():
        print(f"{old} is LIVE already")
        return
    live.mkdir(parents=True, exist_ok=True)
    lock = old / "boat-pool.lock"
    lock.touch()
    t0 = time.time()
    moved, merged = [], []
    with open(lock, "a") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        for e in sorted(old.iterdir()):
            dst = live / e.name
            if not dst.exists():
                os.rename(e, dst)
                moved.append(e.name)
            elif e.name == "boat-pool.json":
                a, b = json.loads(e.read_text()), json.loads(dst.read_text())
                dst.write_text(json.dumps(merge_registry(b, a), indent=1))
                e.unlink()
                merged.append(e.name)
            elif e.name == "boat-starts.jsonl":
                with open(dst, "a") as f:
                    f.write(e.read_text())
                e.unlink()
                merged.append(e.name)
            elif e.is_dir():
                for f in e.rglob("*"):
                    if f.is_file():
                        t = dst / f.relative_to(e)
                        t.parent.mkdir(parents=True, exist_ok=True)
                        if not t.exists():
                            os.rename(f, t)
                shutil.rmtree(e, ignore_errors=True)
                merged.append(e.name + "/")
            elif e.name == "boat-pool.lock":
                pass  # (LIVE's own lock stays; this one is held now, and goes with the directory)
            else:
                e.unlink()
        aside = old.with_name(old.name + f".moved-{int(t0)}")
        os.rename(old, aside)
        os.symlink(live, old)
        fcntl.flock(lf, fcntl.LOCK_UN)
    # (a client that made the directory again in the instant between: its files join LIVE, and the link replaces it)
    if not old.is_symlink():
        for f in old.rglob("*"):
            if f.is_file() and not (live / f.relative_to(old)).exists():
                (live / f.relative_to(old)).parent.mkdir(parents=True, exist_ok=True)
                os.rename(f, live / f.relative_to(old))
        shutil.rmtree(old)
        os.symlink(live, old)
    shutil.rmtree(aside, ignore_errors=True)
    ex = m / ".git" / "info" / "exclude"
    rel = str(old.relative_to(m))
    if ex.parent.is_dir() and rel not in (ex.read_text() if ex.exists() else "").split():
        with open(ex, "a") as f:
            f.write(rel + "\n")
    print(f"{old} -> {live}: moved {len(moved)} ({', '.join(moved)}), merged {len(merged)} ({', '.join(merged)}); {old} links there now")
