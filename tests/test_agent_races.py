"""agent.py's tree and prep against a tree collected between their check and their touch (B6: begin answered 500,
FileNotFoundError, 11 times on 11-01): a tree not there, its manifest asked for, never an exception.
`python3 tests/test_agent_races.py` (a temporary ~/.sc, no VM)."""
import os
import pathlib
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from supercomputer import agent as A  # noqa: E402

root = pathlib.Path(tempfile.mkdtemp())
A.ROOT, A.BLOBS, A.TREES = root, root / "blobs", root / "trees"
A.sudo = lambda *a, **k: None


def collected_on_touch():
    real = os.utime

    def utime(p, *a, **k):
        import shutil
        shutil.rmtree(p, ignore_errors=True)  # (another thread's gc_trees, just now)
        return real(p, *a, **k)
    return real, utime


def test_make_tree_of_a_tree_collected_meanwhile():
    (A.TREES / "t1").mkdir(parents=True)
    (A.TREES / "t1" / ".sc-ready").write_text("t1")
    real, A.os.utime = collected_on_touch()
    try:
        r = A.make_tree("t1", None, None)
    finally:
        A.os.utime = real
    assert r == {"ok": False, "need": "manifest"}, r


def test_prep_of_a_tree_collected_meanwhile():
    (A.TREES / "t2").mkdir(parents=True)
    (A.TREES / "t2" / ".sc-ready").write_text("t2")
    real, A.os.utime = collected_on_touch()
    try:
        r = A.prep("b2", "tree:t2", "", "t2")
    finally:
        A.os.utime = real
    assert not r["ok"] and "isn't on this VM" in r["err"], r


def test_a_blob_gone_under_a_tree_build():
    A.BLOBS.mkdir(parents=True, exist_ok=True)
    files = {"a.txt": "k" * 64}
    real = A.have
    A.have = lambda keys: []  # (present when asked, gone when linked)
    try:
        r = A._make_tree(A.tree_id(files, {}), files, {})
    finally:
        A.have = real
    assert r["ok"] is False and r.get("missing") == ["k" * 64], r


def test_an_emptied_blob_is_missing():
    """bx_bxf2qcpq, 11-01: 232 blobs emptied under their keys, a tree of them, 98 items failed on an empty module."""
    A.BLOBS.mkdir(parents=True, exist_ok=True)
    (A.BLOBS / ("d" * 64)).write_bytes(b"")
    (A.BLOBS / A.EMPTY).write_bytes(b"")
    assert A.have(["d" * 64, A.EMPTY]) == ["d" * 64] and not (A.BLOBS / ("d" * 64)).exists()


def test_clean_disk_keeps_running_jobs_scratch():
    """11-02: 615 of anvil's state folders (107 GB) filled bx_sfrge946's disk. agent._clean_disk_once removes the scratch
    older than every running job (scratch_drops) and ended jobs' leftovers; a running job's own stay."""
    import shutil
    import time
    home = pathlib.Path(tempfile.mkdtemp())
    rm = []
    A.sudo = lambda *a, **k: rm.append(a) or [shutil.rmtree(x, ignore_errors=True) for x in a[2:]]
    A.SCRATCH = [home / ".foundry" / "anvil" / "tmp"]
    old, new = A.SCRATCH[0] / "anvil-state-old", A.SCRATCH[0] / "anvil-state-new"
    for d in (old, new):
        d.mkdir(parents=True)
    run = root / "b" / "bat" / "jobs" / "running"
    run.mkdir(parents=True)
    (run / "spec.json").write_text("{}")
    (run / "pid").write_text(str(os.getpid()))  # (alive)
    t = time.time()
    os.utime(run / "spec.json", (t - 100, t - 100))
    os.utime(old, (t - 3600, t - 3600))  # (older than the running job: no running job made it)
    os.utime(new, (t - 10, t - 10))  # (younger: the running job's, maybe)
    ended = root / "b" / "bat" / "jobs" / "ended"
    (ended / "work").mkdir(parents=True)
    (ended / "done.json").write_text("{}")
    alive, A.alive = A.alive, lambda pid: pid == os.getpid()  # (no /proc here)
    try:
        A._clean_disk_once()
    finally:
        A.alive = alive
    assert not old.exists() and new.exists(), (old.exists(), new.exists())
    assert not (ended / "work").exists() and (run / "spec.json").exists()
    assert A.disk_low(1500, 150_000) and not A.disk_low(50_000, 150_000)
    A.sudo = lambda *a, **k: None


def test_a_short_tree_is_dropped_and_its_batches_unprepared():
    """11-02 12:45, bx_ptwy4b7p: tree 9810b283e4ba lacked 5 files of its manifest, its ready mark kept. verify_tree
    drops such a tree (tree_kept) and the batches prepared on it, so the next preparation makes it again."""
    import hashlib
    A.BLOBS.mkdir(parents=True, exist_ok=True)
    files, gits = {}, {}
    for name, data in (("README.md", b"readme\n"), ("core/cli.ts", b"cli\n")):
        k = hashlib.sha256(data).hexdigest()
        (A.BLOBS / k).write_bytes(data)
        files[name], gits[name] = k, hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()
    tid = A.tree_id(files, {})
    assert A.make_tree(tid, files, {})["ok"]
    b = A.ROOT / "b" / "short"
    b.mkdir(parents=True)
    (b / "treepath").write_text(str(A.TREES / tid))
    (b / "ready").write_text("1")
    assert A.verify_tree(tid, gits)["ok"]
    (A.TREES / tid / "README.md").unlink()  # (short: its ready mark kept)
    r = A.verify_tree(tid, gits)
    assert not r["ok"] and r.get("dropped") and r.get("partial"), r
    assert not (A.TREES / tid).exists() and not (b / "ready").exists()
    assert A.make_tree(tid, files, {})["ok"] and A.verify_tree(tid, gits)["ok"]


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"agent races: {n} of {n}")
