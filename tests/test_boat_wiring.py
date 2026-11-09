"""boat.py's decisions through supercomputer/rules/boat.py, with no boat call (each boat function made to fail, or to
record what it was asked) and a temporary LIVE: the bugs the inventory found, replayed against the wiring.
`python3 tests/test_boat_wiring.py`."""
import json
import os
import sys
import tempfile
import threading
import time

os.environ["SUPERCOMPUTER_LIVE"] = tempfile.mkdtemp(prefix="sc-boat-wiring-")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from supercomputer import boat, scheduler  # noqa: E402

CALLS: list = []


def no_boat(*a, **k):
    raise AssertionError(f"a boat call in a wiring test: {a[:3]}")


for name in ("boat", "bj", "exec_", "limits", "sandboxes", "named_snapshots", "states", "wait_running", "stop_vm"):
    setattr(boat, name, no_boat)
scheduler.enabled = lambda: False
DEAD = 2 ** 22 + 12345  # (a pid no process has here)
HERE = boat.socket.gethostname()


def reg(vms: dict):
    with boat.Registry().locked() as d:
        d["vms"] = vms


def test_pid_alive_and_leases():
    """pid_alive before: a record naming no host was another machine's, never dead."""
    assert boat.pid_alive(DEAD, "") is False
    assert boat.pid_alive(DEAD, HERE) is False
    assert boat.pid_alive(DEAD, "another-host") is True
    assert boat.pid_alive(os.getpid(), "") is True
    assert boat.lease_free({"lease": {"pid": DEAD, "host": ""}})
    assert not boat.lease_free({"lease": {"pid": os.getpid(), "host": HERE}})
    assert boat.lease_free({"lease": None})


def test_teardown_keeps_other_repos():
    """teardown before: without --all, every free VM not of this repo's current image deleted, other repos' too."""
    p = boat.project()
    mine_old, mine_cur, theirs = f"{p.prefix}-old-1", f"{p.prefix}-cur-2", "otherrepo-x-3"
    boat.image_name = lambda: mine_cur
    boat.sandboxes = lambda: {i: {"state": "running"} for i in ("a", "b", "c", "d")}
    deleted = []
    boat.boat = lambda *a, **k: deleted.append(a[1]) if a[0] == "delete" else ""
    reg({"a": {"snapshot": mine_old, "type": "large", "lease": None}, "b": {"snapshot": mine_cur, "type": "large", "lease": None},
         "c": {"snapshot": theirs, "type": "large", "lease": None}, "d": {"snapshot": mine_old, "type": "large", "lease": {"pid": os.getpid(), "host": HERE, "batch": "x"}}})
    boat.teardown()
    assert deleted == ["a"], deleted
    boat.boat, boat.sandboxes = no_boat, no_boat


def test_ceil_minute_share():
    """StartGate.take before: CEIL's minute share (0.5) read as a count: int(0.5) = 0, no start ever."""
    boat.CEIL = [1.0, 1.0, 0.5]
    boat.limits = lambda: {"starts": {"minute": {"limit": 10, "used": 0, "remaining": 10}, "hour": {"limit": 100, "used": 0, "remaining": 100},
                                      "day": {"limit": 1000, "used": 0, "remaining": 1000}}, "currentLimits": {"activeSandboxes": 50}, "activeSandboxes": 0}
    reg({})
    stop = threading.Event()
    threading.Timer(5, stop.set).start()
    try:
        g = boat.StartGate()
        k = g.take(3, stop, urgent=False)
        assert k >= 1, k
    finally:
        boat.CEIL = None
        boat.limits = no_boat
        stop.set()


def test_note_starts():
    with boat.Registry().locked() as d:
        d["starts"] = []
        boat.note_starts(3, d)
        assert len(d["starts"]) == 3
        boat.note_starts(-5, d)
        assert d["starts"] == []


def session() -> boat.Session:
    s = object.__new__(boat.Session)
    s.vms, s.threads, s.ready, s.counts = {}, [], [], {"topped_up": 0, "failed": 0, "created": 0}
    s.cv, s.stop = threading.Condition(), threading.Event()
    s.early, s.seed_vm, s.short, s.last_error, s.cap, s.max_starts, s.vm_type = False, None, False, "", 0, 0, "large"
    s.ensure = lambda *a, **k: None
    s.lease_up = lambda *a, **k: 0
    return s


def test_run_with_no_vm_and_no_deadline_ends():
    """Session.run before: with no VM ever leased and --max-wait 0, it waited forever."""
    s = session()
    lost, done = [], threading.Event()

    def go():
        s.run([{"name": "j1", "weight": 1, "est": 1}, {"name": "j2", "weight": 1, "est": 1}], lambda it, row: None, on_lost=lambda it, why: lost.append(it["name"]), max_wait=0)
        done.set()
    threading.Thread(target=go, daemon=True).start()
    assert done.wait(15), "the run never ended"
    assert sorted(lost) == ["j1", "j2"], lost


def test_run_no_fallback_raises():
    s = session()
    try:
        s.run([{"name": "j1", "weight": 1, "est": 1}], lambda it, row: None, max_wait=0)
        raise AssertionError("no raise")
    except boat.Unavailable:
        pass


class FakeClient:
    remote = None

    def __init__(self):
        self.polls = 0

    def push_tree(self, tree):
        return 0

    def push_caches(self, caches, outputs):
        return 0

    def req(self, path, msg, timeout=60, raw=False):
        if path == "/submit":
            return {"ok": True}
        self.polls += 1
        return {"rows": [{"name": "a", "code": 0}] if self.polls == 1 else [], "done": self.polls > 1}


def test_scheduled_reports_every_item():
    """Scheduled.run before: the scheduler done with no rows, an item it never reported was dropped."""
    s = object.__new__(boat.Scheduled)
    s.client, s.stop, s.vm_ids, s.run_names = FakeClient(), threading.Event(), set(), {}
    s.tree = type("T", (), {"id": "t", "files": {}, "links": {}})()
    s.caches, s.batch, s.klass, s.label, s.image, s.vm_type, s.ctx_b64, s.warm_only = [], "b", 3, "l", "img", "large", "", False
    s.proj = type("P", (), {"cache_outputs": lambda self, c: []})()
    s.bids, s._bid_lock = [], threading.Lock()  # (a session's runs, each its own batch id: 179c531)
    got, lost = [], []
    s.run([{"name": "a"}, {"name": "b"}], lambda it, row: got.append(it["name"]), on_lost=lambda it, why: lost.append(it["name"]))
    assert got == ["a"] and lost == ["b"], (got, lost)


def test_reap_releases_dead_holder():
    boat.sandboxes = lambda: {"v1": {"state": "running"}, "v2": {"state": "running"}}
    boat.exec_ = lambda *a, **k: {"exitCode": 0}
    stopped = []
    boat.stop_vm = lambda i: stopped.append(i) or True
    reg({"v1": {"snapshot": "s", "type": "large", "state": "running", "lease": {"pid": DEAD, "host": "", "batch": "gone"}, "until": time.time() + 900},
         "v2": {"snapshot": "s", "type": "large", "state": "running", "lease": {"pid": os.getpid(), "host": HERE, "batch": "live"}, "until": 0, "last_used": 0}})
    boat.reap()
    d = boat.Registry().read()
    assert d["vms"]["v1"]["lease"] is None and "v1" in stopped, (d["vms"]["v1"], stopped)
    assert d["vms"]["v2"]["lease"]["batch"] == "live" and "v2" not in stopped
    boat.sandboxes, boat.exec_, boat.stop_vm = no_boat, no_boat, no_boat


def test_refresh_old_blind():
    """refresh_old before: boat's limits not to be had, every VM stopped 12 days resumed, uncapped."""
    old = time.time() - 13 * 86400
    d = {"vms": {f"v{i}": {"snapshot": "s", "type": boat.VM_TYPE, "state": "stopped", "lease": None, "last_used": old} for i in range(6)}}
    boat.refresh_old(d, "s")
    assert all(v["last_used"] == old for v in d["vms"].values())



def test_job_memory_fits_a_types_usable_memory():
    """11-01: cpu 4 and memory 16384 got a large VM (16 GB nominal, less its OS's): 5 of 48 Lean checks OOM-killed."""
    R = boat.R
    assert not R.fits_memory(16384, boat.TYPES["large"][1], boat.MEM_OVERHEAD) and R.fits_memory(16384, boat.TYPES["xlarge"][1], boat.MEM_OVERHEAD)
    assert R.fits_memory(4096, boat.TYPES["large"][1], boat.MEM_OVERHEAD) and not R.fits_memory(4096, boat.TYPES["small"][1], boat.MEM_OVERHEAD)
    pick = lambda w, mem: next(t for t, (c, m) in sorted(boat.TYPES.items(), key=lambda x: x[1]) if c // boat.SLOT_CPU >= w and R.fits_memory(mem, m, boat.MEM_OVERHEAD))
    assert pick(R.weight(4, 16384), 16384) == "xlarge" and pick(1, 4096) == "default" and pick(1, 2048) == "small"



def test_a_committed_change_ships_its_own_bytes():
    """11-02 00:56-01:29: the blob map recorded a commit's blob under main's old content (from the stat index's key for
    the path, its signature unchanged), so lean-termination.ts shipped as main's old content. Replayed: a file's
    content changes with its size, mtime and inode kept (an in-place write, its mtime set back), the change committed;
    the tree's key for it is the new bytes', the map's entry the new blob's, and a poisoned entry is never trusted."""
    import hashlib
    import pathlib
    import subprocess
    saved = {k: os.environ.pop(k) for k in list(os.environ) if k.startswith("GIT_")}  # (in a hook, GIT_INDEX_FILE is the outer repo's: the Tree's git too)
    try:
        _committed_change(hashlib, pathlib, subprocess)
    finally:
        os.environ.update(saved)


def _committed_change(hashlib, pathlib, subprocess):
    d = pathlib.Path(tempfile.mkdtemp(prefix="sc-tree-"))
    clean = dict(os.environ)
    git = lambda *a: subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-C", str(d), *a], check=True, capture_output=True, env=clean)
    git("init", "-q")
    git("config", "user.email", "t@t")
    git("config", "user.name", "t")
    f = d / "lean-termination.ts"
    f.write_bytes(b"old content A\n")
    git("add", "-A")
    git("commit", "-qm", "main")
    t1 = boat.Tree(d)
    assert t1.files["lean-termination.ts"] == hashlib.sha256(b"old content A\n").hexdigest()
    st = f.stat()
    with open(f, "r+b") as fh:  # (same size, same inode)
        fh.write(b"new content B\n")
    os.utime(f, ns=(st.st_atime_ns, st.st_mtime_ns))  # (its mtime set back: the stat signature unchanged)
    git("add", "-A")
    git("commit", "-qm", "the change")
    t2 = boat.Tree(d)
    want = hashlib.sha256(b"new content B\n").hexdigest()
    assert t2.files["lean-termination.ts"] == want, ("stale key", t2.files)
    blob = subprocess.run(["git", "-C", str(d), "rev-parse", "HEAD:lean-termination.ts"], capture_output=True, text=True, env=clean).stdout.strip()
    assert t2._by_blob().get(blob) == want and t2.gits.get("lean-termination.ts") == blob
    # (a wrong entry in the old map, as 11-02's: never read; the v2 map holds only checked entries)
    (boat.LIVE / "index").mkdir(parents=True, exist_ok=True)
    (boat.LIVE / "index" / "blobs.json").write_text(json.dumps({blob: hashlib.sha256(b"old content A\n").hexdigest()}))
    t3 = boat.Tree(d)
    assert t3.files["lean-termination.ts"] == want


def test_a_kept_tree_moves_with_its_files():
    """11-02: each treadmill batch built its tree anew (git forked 3+ times from the client, every file stat'ed, the
    index rewritten: 0.8 s of CPU a build, 6.6% of the laptop). A tree is kept per checkout and reused with no git
    process while nothing it was made from moved (rules.boat.tree_reusable); an edit, a file added (in a directory of
    only ignored files too), an executable bit, a commit each make it anew, with each file's own bytes and blob."""
    import hashlib
    import pathlib
    import subprocess
    saved = {k: os.environ.pop(k) for k in list(os.environ) if k.startswith("GIT_")}  # (a hook's index isn't this repo's)
    try:
        d = pathlib.Path(tempfile.mkdtemp(prefix="sc-tree-kept-"))
        git = lambda *a: subprocess.run(["git", "-c", "core.hooksPath=/dev/null", "-C", str(d), *a], check=True, capture_output=True, text=True).stdout
        git("init", "-q")
        git("config", "user.email", "t@t")
        git("config", "user.name", "t")
        (d / ".gitignore").write_text("*.log\n")
        (d / "a.ts").write_text("a1\n")
        (d / "logs").mkdir()
        (d / "logs" / "x.log").write_text("ignored\n")
        git("add", "-A")
        git("commit", "-qm", "c1")
        t1 = boat.Tree(d)
        forks, real = [], subprocess.run
        boat.subprocess.run = lambda *a, **k: forks.append(a) or real(*a, **k)
        try:
            t2 = boat.Tree(d)
        finally:
            boat.subprocess.run = real
        assert not forks and t2.id == t1.id and t2.gits == t1.gits, ("a kept tree forked git or moved", forks)
        sha = lambda b: hashlib.sha256(b).hexdigest()
        (d / "a.ts").write_text("a2 longer\n")  # (an edit)
        t3 = boat.Tree(d)
        assert t3.files["a.ts"] == sha(b"a2 longer\n") and "a.ts" not in t3.gits, t3.files
        (d / "logs" / "new.ts").write_text("n\n")  # (a file added where only ignored files were)
        t4 = boat.Tree(d)
        assert t4.files.get("logs/new.ts") == sha(b"n\n"), t4.names
        os.chmod(d / "a.ts", 0o755)
        assert boat.Tree(d).files["a.ts"] == sha(b"a2 longer\n") + "x"
        git("add", "-A")
        git("commit", "-qm", "c2")
        t5 = boat.Tree(d)
        assert t5.gits.get("a.ts") == git("rev-parse", "HEAD:a.ts").strip() and t5.files["logs/new.ts"] == sha(b"n\n")
        (d / "logs" / "new.ts").unlink()  # (a file removed)
        assert "logs/new.ts" not in boat.Tree(d).files
        boat._TREES.clear()  # (another process: the kept tree read from LIVE/index, checked the same way)
        assert boat.Tree(d).id == boat.Tree(d).id and "logs/new.ts" not in boat.Tree(d).files
    finally:
        os.environ.update(saved)


def test_vm_checks_each_file_is_its_git_blob():
    import hashlib
    import pathlib
    from supercomputer import agent
    root = pathlib.Path(tempfile.mkdtemp(prefix="sc-trees-"))
    old = agent.TREES
    try:
        agent.TREES = root
        (root / "t1").mkdir()
        (root / "t1" / "a.ts").write_bytes(b"main's old content\n")
        good = hashlib.sha1(b"blob %d\0" % 19 + b"main's old content\n").hexdigest()
        stale = hashlib.sha1(b"blob 12\0" + b"new content\n").hexdigest()
        assert agent.verify_tree("t1", {"a.ts": good})["ok"]
        r = agent.verify_tree("t1", {"a.ts": stale})
        assert not r["ok"] and r["bad"][0][0] == "a.ts", r
    finally:
        agent.TREES = old


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"boat wiring: {n} of {n}")
