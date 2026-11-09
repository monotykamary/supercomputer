"""The agent's, the store's and the volumes' decisions through their rules (supercomputer/agent.py's rule classes,
supercomputer/rules/integrity.py), on this machine only: a temporary HOME for the agent, the store's range server and
the volumes' receiver run here on 127.0.0.1 (the scripts as they ship, the rules at their head), modal stubbed. No
boat, Modal or RunPod call is made. `python3 tests/test_integrity_wiring.py`."""
import base64
import hashlib
import io
import json
import os
import pathlib
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import types
import urllib.request

HOME = tempfile.mkdtemp(prefix="sc-integrity-")
os.environ["HOME"] = HOME
os.environ["SUPERCOMPUTER_LIVE"] = tempfile.mkdtemp(prefix="sc-integrity-live-")
REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.modules["modal"] = types.SimpleNamespace(App=lambda *a, **k: types.SimpleNamespace(function=lambda *a, **k: (lambda f: f), local_entrypoint=lambda *a, **k: (lambda f: f)),
                                             Image=types.SimpleNamespace(debian_slim=lambda **k: types.SimpleNamespace(apt_install=lambda *a: types.SimpleNamespace(add_local_file=lambda *a: None))),
                                             Volume=None)
from supercomputer import agent, modal_volumes, store, volumes  # noqa: E402


def port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def blob_tar(entries: dict) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        for k, data in entries.items():
            ti = tarfile.TarInfo(k)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def test_blobs_and_trees():
    good = b"hello"
    k = hashlib.sha256(good).hexdigest()
    assert agent.put_blobs(io.BytesIO(blob_tar({k: good}))) == 1
    try:
        agent.put_blobs(io.BytesIO(blob_tar({k[:-1] + ("0" if k[-1] != "0" else "1"): good})))
        raise AssertionError("a blob stored under a key its bytes don't hash to")
    except ValueError:
        pass
    files, links = {"a.txt": k}, {}
    tid = agent.tree_id(files, links)
    assert agent.make_tree("0" * 24, files, links)["ok"] is False  # (an id that isn't its manifest's)
    assert agent.make_tree(tid, files, links)["ok"] is True
    assert (agent.TREES / tid / "a.txt").read_bytes() == good


def test_report_and_start():
    d = agent.batch_dir("b1")
    (d / "jobs").mkdir(parents=True)
    (d / "ready").write_text("1")
    real = subprocess.Popen

    class Fails:
        def __init__(self, *a, **k):
            raise OSError("no fork")
    subprocess.Popen = Fails
    try:
        agent.start(d, {"name": "j1", "cpus": 2})
    finally:
        subprocess.Popen = real
    done, running = agent.report(d)
    assert [r["name"] for r in done] == ["j1"] and done[0]["lost"], (done, running)  # (its process never started: lost)
    j2 = d / "jobs" / "j2"
    j2.mkdir()
    old = time.time() - agent.STARTING_S - 5
    os.utime(j2, (old, old))
    done, running = agent.report(d)
    assert "j2" in [r["name"] for r in done] and "j2" not in running, (done, running)  # (no process STARTING_S on: lost)
    (d / "jobs" / "j3").mkdir()
    done, running = agent.report(d)
    assert "j3" in running  # (being started)


def test_cpu_grant():
    d = agent.batch_dir("b2")
    (d / "jobs").mkdir(parents=True)
    n = os.cpu_count() or 1

    class P:
        pid = 999999

        def __init__(self, *a, **k):
            pass
    real = subprocess.Popen
    subprocess.Popen = P
    try:
        agent.start(d, {"name": "all", "cpus": n})
        agent.start(d, {"name": "more", "cpus": 2})
    finally:
        subprocess.Popen = real
    held = json.loads((d / "jobs" / "all" / "cpus").read_text())
    more = json.loads((d / "jobs" / "more" / "cpus").read_text())
    assert len(held) == n and more == [], (held, more)  # (none free: it shares CPUs and holds none)


def test_range_server_and_pull():
    src = pathlib.Path(tempfile.mkdtemp(prefix="sc-store-src-"))
    (src / "x").mkdir()
    (src / "x" / "f.bin").write_bytes(os.urandom(5 << 20))
    (src / "g.txt").write_text("g")
    m = store.manifest(src)
    (src / "manifest.json").write_text(json.dumps(m))
    srv = pathlib.Path(tempfile.mkdtemp(prefix="sc-store-srv-"))
    (srv / "tok").symlink_to(src)
    rs = srv.parent / f"rs-{os.getpid()}.py"
    rs.write_text(store.RANGE_SERVER)
    p = port()
    proc = subprocess.Popen([sys.executable, str(rs), str(p), str(srv)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{p}/tok/g.txt", timeout=1).read()
                break
            except Exception:
                time.sleep(0.1)
        r = urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{p}/tok/g.txt", headers={"Range": "bytes=0-99"}), timeout=5)
        assert r.status == 206 and r.read() == b"g" and r.headers["Content-Range"] == "bytes 0-0/1"  # (Ranges: never past the file)
        dest = pathlib.Path(tempfile.mkdtemp(prefix="sc-store-dest-"))
        got = store.pull("t", str(dest), f"http://127.0.0.1:{p}/tok/", pathlib.Path(tempfile.mkdtemp()), 4)
        assert got == m and store.verify.__name__
        assert store.Manifest().verify(0, len(m), len(m)) and store.manifest(dest) == m
        (src / "manifest.json").write_text(json.dumps({**m, "../escape.txt": m["g.txt"]}))
        try:
            store.pull("t", str(dest), f"http://127.0.0.1:{p}/tok/", pathlib.Path(tempfile.mkdtemp()), 4)
            raise AssertionError("a pull placed a file outside its directory")
        except RuntimeError as e:
            assert "outside" in str(e)
        assert not (dest.parent / "escape.txt").exists()
    finally:
        proc.kill()  # (the server this test started, by its handle)
        proc.wait()


def put(p, tok, path, data=b"", **h):
    req = urllib.request.Request(f"http://127.0.0.1:{p}/{tok}/{path}", data=data, method="PUT", headers={"X-Token": tok, **h})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def test_receiver():
    root = pathlib.Path(tempfile.mkdtemp(prefix="sc-recv-"))
    script = root.parent / f"recv-{os.getpid()}.py"
    script.write_text(volumes.RECV)
    p, tok = port(), "T0k"
    proc = subprocess.Popen([sys.executable, str(script), str(p), tok, str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{p}/", timeout=1).read()
                break
            except Exception:
                time.sleep(0.1)
        assert put(p, "wrong", "begin", **{"X-Name": "v"})[0] == 403  # (ReceiverGate: the token in the path and header)
        assert put(p, tok, "begin", **{"X-Name": "v", "X-Mode": "tar"})[0] == 200
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tf:
            data = os.urandom(300_000)
            ti = tarfile.TarInfo("d.bin")
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
        b = buf.getvalue()
        parts = [b[i:i + 100_000] for i in range(0, len(b), 100_000)]
        sha = lambda x: hashlib.sha256(x).hexdigest()  # noqa: E731
        assert put(p, tok, "part", parts[1], **{"X-Name": "v", "X-Part": "1", "X-Sha256": sha(b"other")})[0] == 400  # (a part whose sha256 differs)
        for i in reversed(range(len(parts))):  # (out of order: consumed in index order)
            assert put(p, tok, "part", parts[i], **{"X-Name": "v", "X-Part": str(i), "X-Sha256": sha(parts[i])})[0] == 200
        code, r = put(p, tok, "end", **{"X-Name": "v", "X-Parts": str(len(parts))})
        assert code == 200 and r["ok"] and r["next"] == len(parts), r
        assert (root / "v" / "d.bin").read_bytes() == data
        assert put(p, tok, "file", b"abc", **{"X-Name": "v", "X-Path": "k.txt", "X-Sha256": sha(b"abd")})[0] == 400
        assert not (root / "v" / "k.txt").exists()  # (a file whose sha256 differs isn't kept)
        assert put(p, tok, "file", b"abc", **{"X-Name": "v", "X-Path": "k.txt", "X-Sha256": sha(b"abc")})[0] == 200
    finally:
        proc.kill()
        proc.wait()


def test_modal_side():
    q = []
    sink = modal_volumes._Parts(4, types.SimpleNamespace(put=q.append))
    sink.close()
    assert q == [(0, b"")]  # (an empty stream still one part)
    q.clear()
    sink = modal_volumes._Parts(4, types.SimpleNamespace(put=q.append))
    sink.write(b"0123456789")
    sink.close()
    assert [i for i, _ in q] == [0, 1, 2] and b"".join(x for _, x in q) == b"0123456789"
    calls = []
    real_sleep = time.sleep
    time.sleep = lambda s: None
    try:
        modal_volumes._retry(lambda: calls.append(1) or {"ok": False}, "x", 3)
        raise AssertionError("no give up")
    except RuntimeError:
        assert len(calls) == 3  # (Tries: at most 3 attempts)
    finally:
        time.sleep = real_sleep
    d = pathlib.Path(tempfile.mkdtemp(prefix="sc-vol-"))
    (d / ".env").write_text("A=1\n")
    (d / "data.txt").write_text("plain\n")
    stash_buf = io.BytesIO()
    stash = tarfile.open(fileobj=stash_buf, mode="w")
    m, _ = modal_volumes._tar(d, [], io.BytesIO(), "w", split=(stash, lambda: {}))
    stash.close()
    assert "data.txt" in m and ".env" not in m  # (a secret-looking file never in the plain manifest)
    with tarfile.open(fileobj=io.BytesIO(stash_buf.getvalue())) as t:
        assert [x.name for x in t.getmembers()] == [".env"]


def test_checks():
    assert not volumes.Checks().restore(0, 0, 3, 3, False, True)  # (no recorded manifest sha256: not passed)
    g = volumes.CopyGates()
    assert g.restore(False, True) and g.disk(10, 5) and g.receive(True)
    assert not g.check(0, 0, 3, 3, 3, False, False) and g.state == "failed"  # (no sha256 sent: the check fails)
    assert volumes.Serving().cleanup(False) and not volumes.Serving().cleanup(True)


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"integrity wiring: {n} of {n}")
