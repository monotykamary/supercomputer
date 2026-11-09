"""Modal volumes straight to boat: each becomes the store's snapshot `store-<name>` (its files at /store/<name>/ beside
manifest.json, as store.py keeps them), with no byte through this machine.

    supercomputer volumes inventory <name,...>                  sizes, files, secret-looking files by kind (Modal)
    supercomputer volumes copy <name> [<name> ...] [--encrypt a,b] [--after <stored name>] [--only top,top --as <name>]
    supercomputer volumes verify <name>                         a VM from store-<name>, every sha256 again
    supercomputer volumes ls | cleanup

copy: a boat VM receives (RECV below: a stdlib HTTP server on localhost behind `boat host --private`, our own token
too) the tar stream that a Modal function (modal_volumes.send_volume, the volume mounted read-only) cuts into parts,
each checked by sha256 as it lands and fed in order into `tar -x`; the files are checked against the manifest the
Modal side made as it read them, the snapshot is saved, its tree (names, sizes) checked against the manifest, and the
VM deleted. Several names in one command go as a chain: the VM for the next starts from the previous snapshot and
re-hashes it first (that start is the previous one's restore check), so n volumes cost n starts and one `verify`.
--encrypt: the volume leaves Modal only as AES-256 ciphertext whose keys are wrapped with the RSA key in
LIVE/keys/volumes.pub.pem (its private half, here only, 0600); the record keeps the ciphertext's and the plaintext
manifest's sha256, never a name or a byte of what's inside, and the round trip (unwrap, HMAC, decrypt, every sha256)
is checked here in memory.

Every VM made is noted in the starts ledger and has a ticket in LIVE/store/volumes-vms/ until it's deleted
(`cleanup` deletes those left by a crash). The receiver's URL carries boat's token: it lives in memory and in a 0600
file Modal's client reads, deleted after.
"""
import argparse
import base64
import contextlib
import hashlib
import hmac
import json
import os
import pathlib
import secrets
import signal
import subprocess
import sys
import tarfile
import io
import time

import socket

from . import boat
from .rules import integrity as rules
from .rules.integrity import Checks, CopyGates, Serving

STORE = boat.LIVE / "store"
TICKETS = STORE / "volumes-vms"
KEYS = boat.LIVE / "keys"
TMP = pathlib.Path(os.environ.get("SUPERCOMPUTER_VOLUMES_TMP") or boat.LIVE / "volumes-tmp")
HERE = pathlib.Path(__file__).resolve().parent
PORT = 8767
CLIENT = "volumes"


def log(*a):
    print("[volumes]", *a, file=sys.stderr, flush=True)


# ------------------------------------------------------------------------------------------- the receiver, on the VM
# (rules/integrity.py at its head: the receiver keeps and consumes parts by PartOrder, admits by ReceiverGate)
RECV = pathlib.Path(rules.__file__).read_text() + r'''
import hashlib, http.server, json, os, pathlib, re, shutil, subprocess, sys, threading
PORT, TOK = int(sys.argv[1]), sys.argv[2]
ROOT = pathlib.Path(sys.argv[3] if len(sys.argv) > 3 else "/store"); INC = ROOT / ".in"
S = {}
OKN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")

class St:
    def __init__(s, name, mode, path):
        s.dir, s.inc, s.order, s.err, s.bytes, s.done = ROOT / name, INC / name, PartOrder(), None, 0, False
        shutil.rmtree(s.inc, ignore_errors=True); s.inc.mkdir(parents=True); s.dir.mkdir(parents=True, exist_ok=True)
        if mode == "tar":
            s.p = subprocess.Popen(["tar", "-x", "--no-same-owner", "-C", str(s.dir), "-f", "-"], stdin=subprocess.PIPE, stderr=open(s.inc / "tar.err", "wb"))
            s.sink = s.p.stdin
        else:
            s.p, s.sink = None, open(s.dir / path, "wb")
        s.cv = threading.Condition()
        threading.Thread(target=s.feed, daemon=True).start()
    def feed(s):
        try:
            while True:
                with s.cv:
                    while not (s.inc / f"{s.order.next}.part").exists() and not s.order.complete:
                        s.cv.wait(5)
                    if s.order.complete:
                        break
                i = s.order.next
                f = s.inc / f"{i}.part"
                with open(f, "rb") as r:
                    for b in iter(lambda: r.read(1 << 20), b""):
                        s.sink.write(b); s.bytes += len(b)
                f.unlink()
                with s.cv:
                    s.order.consume(i); s.cv.notify_all()
            s.sink.close()
            if s.p:
                rc = s.p.wait()
                if rc:
                    s.err = f"tar exit {rc}: " + (s.inc / "tar.err").read_text()[-300:]
        except Exception as e:
            s.err = repr(e)[:300]
        finally:
            with s.cv:
                s.done = True; s.cv.notify_all()

class H(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def log_message(s, *a): pass
    def reply(s, code, obj, close=False):
        b = json.dumps(obj).encode()
        s.send_response(code); s.send_header("Content-Type", "application/json"); s.send_header("Content-Length", str(len(b)))
        if close: s.send_header("Connection", "close"); s.close_connection = True
        s.end_headers(); s.wfile.write(b)
    def chunks(s):
        if s.headers.get("Transfer-Encoding", "").lower() == "chunked":
            while True:
                n = int(s.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if n == 0:
                    while s.rfile.readline() not in (b"\r\n", b"\n", b""): pass
                    return
                left = n
                while left:
                    b = s.rfile.read(min(left, 1 << 20)); left -= len(b); yield b
                s.rfile.readline()
        else:
            left = int(s.headers.get("Content-Length") or 0)
            while left > 0:
                b = s.rfile.read(min(left, 1 << 20))
                if not b: raise IOError("short body")
                left -= len(b); yield b
    def save(s, path):
        h, n = hashlib.sha256(), 0
        with open(path, "wb") as f:
            for b in s.chunks():
                h.update(b); f.write(b); n += len(b)
        return h.hexdigest(), n
    def do_GET(s):
        if ReceiverGate().admit(s.path.split("?")[0] == f"/{TOK}/status", s.headers.get("X-Token") == TOK, True):
            return s.reply(200, {k: {"next": v.order.next, "bytes": v.bytes, "done": v.done, "err": v.err} for k, v in S.items()})
        s.reply(200, {"ok": True})
    def do_PUT(s):
        parts = s.path.split("?")[0].strip("/").split("/")
        name = s.headers.get("X-Name", "")
        if not ReceiverGate().admit(len(parts) == 2 and parts[0] == TOK, s.headers.get("X-Token") == TOK, bool(OKN.match(name))):
            for _ in s.chunks(): pass
            return s.reply(403, {"ok": False, "err": "no"}, True)
        op = parts[1]
        try:
            if op == "begin":
                for _ in s.chunks(): pass
                old = S.get(name)
                if old and old.p and old.p.poll() is None: old.p.kill()
                path = s.headers.get("X-Path", "")
                if s.headers.get("X-Mode") != "tar" and not OKN.match(path): return s.reply(400, {"ok": False, "err": "path"})
                S[name] = St(name, s.headers.get("X-Mode", "tar"), path)
                return s.reply(200, {"ok": True})
            st = S.get(name)
            if op == "part":
                i, want = int(s.headers["X-Part"]), s.headers["X-Sha256"]
                if st is None or st.order.arrive(i, True) == "dup":
                    for _ in s.chunks(): pass
                    return s.reply(200 if st else 409, {"ok": st is not None, "dup": True})
                tmp = st.inc / f"{i}.tmp"
                got, n = s.save(tmp)
                if st.order.arrive(i, got == want) != "kept":
                    tmp.unlink(); return s.reply(400, {"ok": False, "err": f"part {i}: sha256 differs ({n} bytes)"})
                tmp.rename(st.inc / f"{i}.part")
                with st.cv: st.cv.notify_all()
                return s.reply(200, {"ok": True, "n": n})
            if op == "end":
                for _ in s.chunks(): pass
                with st.cv:
                    st.order.finish(int(s.headers["X-Parts"])); st.cv.notify_all()
                    st.cv.wait_for(lambda: st.done, 3000)
                ok = ReceiverGate().end_ok(st.done and st.order.complete, st.err is not None)
                return s.reply(200, {"ok": ok, "err": st.err, "bytes": st.bytes, "next": st.order.next})
            if op == "file":
                path = s.headers.get("X-Path", "")
                if not OKN.match(path): return s.reply(400, {"ok": False, "err": "path"})
                (ROOT / name).mkdir(parents=True, exist_ok=True)
                tmp = ROOT / name / f".in-{path}"
                got, n = s.save(tmp)
                if not PullFile().cache(s.headers.get("X-Sha256") or "", got):  # (a file kept only when its bytes hash to the sha256 sent)
                    tmp.unlink(); return s.reply(400, {"ok": False, "n": n})
                tmp.rename(ROOT / name / path)
                return s.reply(200, {"ok": True, "n": n})
            return s.reply(404, {"ok": False, "err": "op"})
        except Exception as e:
            return s.reply(500, {"ok": False, "err": repr(e)[:300]}, True)

class Srv(http.server.ThreadingHTTPServer):
    daemon_threads = True
Srv(("127.0.0.1", PORT), H).serve_forever()
'''

# The check on a VM: every file (and a symlink to one, as rglob sees it) of /store/<name> against its manifest.json,
# hashed in parallel; links.json's symlinks as they should be; the manifest's own sha256.
CHECK = r'''
import concurrent.futures as cf, hashlib, json, os, pathlib, sys, time
d = pathlib.Path(sys.argv[1]); t0 = time.time()
mb = (d / "manifest.json").read_bytes(); want = json.loads(mb)
links = json.loads((d / "links.json").read_text()) if (d / "links.json").exists() else {}
def h(p):
    x = hashlib.sha256(); n = 0
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""): x.update(b); n += len(b)
    return [n, x.hexdigest()]
paths = [p for p in sorted(d.rglob("*")) if p.is_file() and str(p.relative_to(d)) not in ("manifest.json", "links.json")]
real = {}
for p in paths: real.setdefault(os.path.realpath(p), []).append(str(p.relative_to(d)))
have = {}
with cf.ThreadPoolExecutor(int(sys.argv[2]) if len(sys.argv) > 2 else 8) as ex:
    for rp, v in zip(real, ex.map(h, list(real))):
        for rel in real[rp]: have[rel] = v
bad = sorted(k for k in set(want) | set(have) if want.get(k) != have.get(k))
badl = sorted(k for k, t in links.items() if not os.path.islink(d / k) or os.readlink(d / k) != t)
print(json.dumps({"files": len(have), "want": len(want), "bytes": sum(v[0] for k, v in have.items() if not os.path.islink(d / k)),
                  "nbad": len(bad), "bad": bad[:10], "links": len(links), "nbadlinks": len(badl), "manifest_sha256": hashlib.sha256(mb).hexdigest(),
                  "seconds": round(time.time() - t0, 1)}))
'''


# ------------------------------------------------------------------------------------------------------- VMs, tickets
def ticket(vm: str, **kw):
    TICKETS.mkdir(parents=True, exist_ok=True)
    f = TICKETS / f"{vm}.json"
    d = json.loads(f.read_text()) if f.exists() else {"vm": vm, "created": time.time(), "pid": os.getpid(), "host": socket.gethostname()}
    d.update(kw)
    f.write_text(json.dumps(d))


def new_vm(src: str | None, label: str, ttl: int = 4 * 3600, vm_type: str = "large") -> str:
    out = boat.boat("new", *(["--from", src] if src else []), "--type", vm_type, "--ttl", str(ttl), "--no-env", "--no-monitoring",
                    "--name", f"sc-volumes-{label}"[:100], "--json", timeout=900, check=False)
    vm = None
    for line in out.splitlines():
        with contextlib.suppress(Exception):
            vm = vm or json.loads(line).get("id")
    if not vm:
        raise RuntimeError(f"boat new gave no VM: {out[-300:]}")
    boat.note_starts(1)
    ticket(vm, label=label, src=src)
    boat.ledger(label=f"volumes {label}", starts=1, created=1, resumed=0, vms=1, types=[vm_type], client=CLIENT)
    log(f"{vm}: {vm_type} VM {'from ' + src if src else 'fresh'} ({label})")
    if not boat.wait_running([vm], 900):
        raise RuntimeError(f"{vm} didn't come up")
    return vm


def delete_vm(vm: str):
    boat.boat("delete", vm, "--yes", "--json", check=False, timeout=300)
    (TICKETS / f"{vm}.json").unlink(missing_ok=True)
    log(f"{vm}: deleted")


def sh(vm: str, cmd: str, timeout: int = 600) -> dict:
    """A command on the VM: {exitCode, stdout, stderr}. (A long --timeout makes `boat exec --json` stream JSON lines:
    attached, stdout/stderr data, exit; a short one answers one object.)"""
    out = boat.boat("exec", vm, "--timeout", str(timeout), "--json", "--", "bash", "-c", cmd, timeout=timeout + 120, check=False)
    with contextlib.suppress(json.JSONDecodeError):
        return json.loads(out)
    r = {"exitCode": -1, "stdout": "", "stderr": ""}
    for line in out.splitlines():
        with contextlib.suppress(json.JSONDecodeError):
            e = json.loads(line)
            if e.get("type") in ("stdout", "stderr"):
                r[e["type"]] += e.get("data", "")
            elif e.get("type") == "exit":
                r["exitCode"] = e.get("exitCode", -1)
    if r["exitCode"] == -1 and not r["stdout"]:
        r["stderr"] += out[-400:]
    return r


def run_py(vm: str, code: str, args: str, timeout: int) -> dict:
    """A python script on the VM (sent base64 in the command), its last stdout line as JSON."""
    b64 = base64.b64encode(code.encode()).decode()
    nm = hashlib.sha256(code.encode()).hexdigest()[:10]
    r = sh(vm, f"echo {b64} | base64 -d > /tmp/sc-{nm}.py && python3 /tmp/sc-{nm}.py {args}", timeout)
    try:
        return json.loads(r["stdout"].strip().splitlines()[-1])
    except Exception:
        raise RuntimeError(f"{vm}: no answer ({r.get('exitCode')}): {r.get('stderr', '')[-300:]}")


def check_dir(vm: str, name: str, warm: bool = False, timeout: int = 7200) -> dict:
    if warm:  # (a VM from a snapshot fills its disk lazily: everything read once first, as store.serve does)
        sh(vm, f"find /store/{name} -type f -print0 | xargs -0 -P 8 -n 64 cat > /dev/null", timeout)
    c = run_py(vm, CHECK, f"/store/{name} 8", timeout)
    if Checks().again(warm, c["nbad"]):  # (once more, in case a file was read while it was filling)
        log(f"{name}: {c['nbad']} differ on the first pass; once more")
        c = run_py(vm, CHECK, f"/store/{name} 8", timeout)
    return c


# ----------------------------------------------------------------------------------------------------- the key pair
def keypair() -> tuple[pathlib.Path, pathlib.Path]:
    KEYS.mkdir(parents=True, exist_ok=True)
    os.chmod(KEYS, 0o700)
    priv, pub = KEYS / "volumes.pem", KEYS / "volumes.pub.pem"
    if not priv.exists():
        old = os.umask(0o077)
        try:
            subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:4096", "-out", str(priv)], check=True, capture_output=True)
        finally:
            os.umask(old)
        subprocess.run(["openssl", "pkey", "-in", str(priv), "-pubout", "-out", str(pub)], check=True, capture_output=True)
        log(f"a new RSA-4096 key pair in {KEYS} (the private half 0600, here only)")
    os.chmod(priv, 0o600)
    return priv, pub


def decrypt_check(vm: str, name: str, res: dict, fname: str = "data.tar.enc") -> dict:
    """The ciphertext from the VM (small), unwrapped and decrypted here in memory, every file's sha256 against the
    plaintext manifest inside: only counts come out."""
    priv, _ = keypair()
    p = subprocess.run([boat.BOAT, "ssh", vm, "--", f"cat /store/{name}/{fname}"], capture_output=True, env=boat.ENV, timeout=600)
    blob = p.stdout
    cipher_ok = hashlib.sha256(blob).hexdigest() == res["ciphertext_sha256"]
    if not cipher_ok:
        raise RuntimeError(f"{name}: the ciphertext fetched differs ({len(blob)} bytes)")
    wrapped = base64.b64decode(res["wrapped_key_b64"])
    keys = subprocess.run(["openssl", "pkeyutl", "-decrypt", "-inkey", str(priv), "-pkeyopt", "rsa_padding_mode:oaep", "-pkeyopt", "rsa_oaep_md:sha256",
                           "-pkeyopt", "rsa_mgf1_md:sha256"], input=wrapped, capture_output=True, check=True).stdout
    k_enc, k_mac = keys[:32], keys[32:64]
    if blob[:4] != b"SCV1":  # (a check, not an assert: python -O drops asserts)
        raise RuntimeError(f"{name}: not an SCV1 blob")
    iv, ct, mac = blob[4:20], blob[20:-32], blob[-32:]
    if not hmac.compare_digest(hmac.new(k_mac, iv + ct, hashlib.sha256).digest(), mac):
        raise RuntimeError(f"{name}: the HMAC doesn't match")
    plain = subprocess.run(["openssl", "enc", "-d", "-aes-256-cbc", "-K", k_enc.hex(), "-iv", iv.hex()], input=ct, capture_output=True, check=True).stdout
    ok = hashlib.sha256(plain).hexdigest() == res["plain_tar_sha256"]
    files = bad = 0
    with tarfile.open(fileobj=io.BytesIO(plain)) as t:
        mm = t.extractfile("manifest.plain.json").read()
        want = json.loads(mm)
        syms = {ti.name for ti in t.getmembers() if ti.issym()}
        for ti in t.getmembers():
            if ti.isfile() and ti.name in want:
                files += 1
                b = t.extractfile(ti).read()
                bad += [len(b), hashlib.sha256(b).hexdigest()] != want[ti.name]
    del plain
    nbad = bad + len([k for k in want if k not in syms]) - files
    mok = hashlib.sha256(mm).hexdigest() == res["plain_manifest_sha256"]
    return {"decrypted": ok, "files": files, "nbad": nbad, "plain_manifest_sha256_ok": mok,
            "ok": Checks().decrypted(cipher_ok, True, True, ok, nbad, mok)}


def tree_check(snap: str, name: str, manifest: dict, links: dict) -> dict:
    """The saved snapshot's file tree (boat's listing: names and sizes, no start) against the manifest."""
    sid = boat.named_snapshots(0)[snap]["snapshotId"]
    t = boat.bj("snapshot", "tree", sid, timeout=600)
    pre = f"__system__/store/{name}/"
    files = {e["path"][len(pre):]: e.get("size") for e in t.get("entries", []) if e["path"].startswith(pre) and e["kind"] == "file"}
    syml = {e["path"][len(pre):] for e in t.get("entries", []) if e["path"].startswith(pre) and e["kind"] == "symlink"}
    want = {k: v[0] for k, v in manifest.items() if k not in links}
    cut = bool(t.get("truncated"))  # (a listing boat cut short: judged on what it lists)
    extra = set(files) - set(want) - {"manifest.json", "links.json"}
    bad = [k for k in want if (k in files or not cut) and files.get(k) != want[k]]
    badl = (set(links) ^ syml) if not cut else (syml - set(links))
    return {"truncated": cut, "files": len(files), "nbad": len(bad), "bad": bad[:5], "extra": len(extra),
            "links": len(syml), "nbadlinks": len(badl)}


# ---------------------------------------------------------------------------------------------------------- Modal
def modal_run(entry: str, args: list[str], timeout: int) -> int:
    """`modal run modal_volumes.py::<entry>`, its client waited on (and killed) by its exact PID."""
    TMP.mkdir(parents=True, exist_ok=True)
    p = subprocess.Popen(["modal", "run", str(HERE / "modal_volumes.py") + "::" + entry, *args], start_new_session=True)
    ticket_f = TMP / f"modal-{p.pid}.pid"
    ticket_f.write_text(str(p.pid))
    try:
        return p.wait(timeout=timeout)
    except BaseException:
        with contextlib.suppress(Exception):
            os.kill(p.pid, signal.SIGINT)
            p.wait(30)
        with contextlib.suppress(Exception):
            os.kill(p.pid, signal.SIGKILL)
        raise
    finally:
        ticket_f.unlink(missing_ok=True)


def inventory(names: list[str]) -> list:
    TMP.mkdir(parents=True, exist_ok=True)
    out = TMP / f"inventory-{int(time.time())}.json"
    modal_run("inventory", ["--names", ",".join(names), "--out", str(out)], 2400)
    return json.loads(out.read_text()) if out.exists() else []


# ------------------------------------------------------------------------------------------------------------ copy
def record_path(name: str) -> pathlib.Path:
    return STORE / f"{name}.json"


def done(name: str) -> bool:
    f = record_path(name)
    return f.exists() and boat.named_snapshots(0).get(json.loads(f.read_text())["snapshot"], {}).get("status") == "ready"


def restore_check(vm: str, name: str) -> dict:
    """On a VM from store-<name>: every sha256 again; the result into the record."""
    rec = json.loads(record_path(name).read_text())
    c = check_dir(vm, name, warm=True, timeout=4 * 3600)
    # (Checks.restore: the manifest's sha256 against the one recorded at the copy; with none recorded, not passed)
    ok = Checks().restore(c["nbad"], c["nbadlinks"], c["files"], c["want"], "manifest_sha256" in rec, c["manifest_sha256"] == rec.get("manifest_sha256"))
    rec["verify"] = {"t": time.time(), "vm_from": rec["snapshot"], "ok": ok, **{k: c[k] for k in ("files", "bytes", "nbad", "links", "nbadlinks", "seconds")}}
    record_path(name).write_text(json.dumps(rec, indent=1))
    log(f"{name}: restored from {rec['snapshot']}: {c['files']} files, {c['bytes'] / 1e9:.3f} GB, {c['nbad']} differ, {c['nbadlinks']} links differ "
        f"({'OK' if ok else 'BAD'}, {c['seconds']} s)")
    return rec["verify"]


def receive(vm: str, volume: str, name: str, encrypt: bool, only: list[str], part_mb: int, workers: int, size_hint: float, min_mbs: float = 15,
            split: bool = False) -> dict:
    """The receiver on vm, the Modal sender pointed at it, the copy checked on the VM."""
    tok = secrets.token_urlsafe(24)
    # (a receiver left by a failed try still holds the port: it goes first, on this VM of ours)
    sh(vm, f"pkill -f '[s]c-recv.py'; sleep 1; sudo mkdir -p /store && sudo chown $(id -un) /store && rm -rf /store/{name} /store/.in/{name}", 300)
    b64 = base64.b64encode(RECV.encode()).decode()
    r = sh(vm, f"echo {b64} | base64 -d > /tmp/sc-recv.py && (setsid nohup python3 /tmp/sc-recv.py {PORT} {tok} > /tmp/sc-recv.log 2>&1 &) && sleep 1 && "
               f"curl -s -o /dev/null -w '%{{http_code}}' http://127.0.0.1:{PORT}/", 60)
    if r.get("stdout", "").strip() != "200":
        raise RuntimeError(f"{vm}: the receiver didn't start: {r.get('stderr', '')[-200:]}")
    url = boat.bj("host", vm, str(PORT), "--localhost", "--private")["url"]
    conf = {"volume": volume, "dest": name, "url": url, "token": tok, "part_mb": part_mb, "workers": workers, "only": only,
            "timeout": int(min(8 * 3600, max(900, 600 + size_hint / (min_mbs * 1e6))))}  # (sized at --min-mbs, 15 MB/s: the pilots' worst was 7)
    if encrypt:
        conf["pubkey"] = keypair()[1].read_text()
    if split:
        conf["split"] = keypair()[1].read_text()
    TMP.mkdir(parents=True, exist_ok=True)
    os.chmod(TMP, 0o700)
    cf, of = TMP / f"conf-{name}-{os.getpid()}.json", TMP / f"result-{name}.json"
    old = os.umask(0o077)
    try:
        cf.write_text(json.dumps(conf))
    finally:
        os.umask(old)
    of.unlink(missing_ok=True)
    t0 = time.time()
    try:
        modal_run("copy", ["--conf", str(cf), "--out", str(of)], conf["timeout"] + 600)
    finally:
        cf.unlink(missing_ok=True)
        del conf["url"]
    res = json.loads(of.read_text()) if of.exists() else {"error": "no result"}
    if res.get("error"):
        raise RuntimeError(f"{volume}: the Modal side failed: {res['error']}")
    log(f"{volume} -> {vm}:/store/{name}: {res['files']} files, {res['stream_bytes'] / 1e9:.3f} GB streamed in {res['parts']} parts, "
        f"{res['seconds']} s on Modal ({res['mb_s']} MB/s), {time.time() - t0:.0f} s here")
    sh(vm, "pkill -f '[s]c-recv.py'; rm -rf /store/.in; sync", 120)
    return res


def copy_one(volume: str, name: str, after: str | None, encrypt: bool, only: list[str], part_mb: int, workers: int, vm_type: str, size_hint: float,
             vm: str = "", keep: bool = False, min_mbs: float = 15, ttl: int = 4 * 3600, split: bool = False):
    vm = vm or new_vm(f"store-{after}" if after else None, name, ttl=ttl, vm_type=vm_type)
    ok, gates = False, CopyGates()  # (rules.integrity.CopyGates: recorded only after every gate, in order)
    try:
        if after and not (TICKETS / f"{vm}.json").exists() or after and json.loads((TICKETS / f"{vm}.json").read_text()).get("checked") != after:
            v = restore_check(vm, after)
            ticket(vm, checked=after)
            if not gates.restore(True, v["ok"]):
                raise RuntimeError(f"{after}: its snapshot didn't restore whole: {v}")
            sh(vm, f"sudo rm -rf /store/{after} && sync", 600)
        else:
            gates.restore(False, True)
        df = sh(vm, f"pkill -f '[s]c-recv.py'; rm -rf /store/{name} /store/.in; df -B1 --output=avail / | tail -1", 600).get("stdout", "0").strip().splitlines()[-1]
        log(f"{vm}: {int(df or 0) / 1e9:.0f} GB free")
        if not gates.disk(int(df or 0), int(size_hint * 1.05 + 2e9) if size_hint else 0):
            raise RuntimeError(f"{vm}: {int(df) / 1e9:.0f} GB free, {size_hint / 1e9:.1f} GB to receive: split it (--only)")
        res = receive(vm, volume, name, encrypt, only, part_mb, workers, size_hint, min_mbs, split)
        gates.receive(not res.get("error"))
        c = check_dir(vm, name)
        log(f"{name}: on {vm}, {c['files']} files, {c['nbad']} differ from Modal's manifest, {c['nbadlinks']} links differ ({c['seconds']} s)")
        if not gates.check(c["nbad"], c["nbadlinks"], c["files"], c["want"], res["files"], "manifest_sha256" in res, c["manifest_sha256"] == res.get("manifest_sha256")):
            raise RuntimeError(f"{name}: the copy on {vm} differs: {c} (Modal's manifest sha256: {res.get('manifest_sha256', 'none sent')})")
        dec = decrypt_check(vm, name, res) if encrypt else None
        sec = res.get("secrets")
        if sec:
            sd = decrypt_check(vm, name, sec, "secrets.tar.enc")
            log(f"{name}: {sec['files']} secret-looking files kept apart, encrypted; decrypted here: {sd['files']} files, {sd['nbad']} differ")
            if not sd["ok"]:
                raise RuntimeError(f"{name}: the secrets' round trip failed: {sd}")
            KEYS.mkdir(parents=True, exist_ok=True)
            (KEYS / f"{name}.secrets.wrapped.key").write_text(sec["wrapped_key_b64"])
            sec = {k: v for k, v in sec.items() if k != "wrapped_key_b64"} | {"roundtrip": sd, "wrapped_key": f"{name}.secrets.wrapped.key (LIVE/keys; also /store/{name}/secrets.wrapped.key)"}
        if dec:
            log(f"{name}: decrypted here: {dec['files']} files, {dec['nbad']} differ, tar {'OK' if dec['decrypted'] else 'BAD'}")
        if not gates.decrypt(bool(dec), bool(dec and dec["ok"])):
            raise RuntimeError(f"{name}: the round trip failed: {dec}")
        snap = f"store-{name}"
        sh(vm, "sync", 300)
        boat.boat("snapshot", vm, snap, "--json", timeout=3 * 3600)
        for _ in range(1080):
            s = boat.named_snapshots(0).get(snap)
            if s and s.get("status") == "ready":
                break
            time.sleep(10)
        else:
            gates.saved(False)
            raise RuntimeError(f"{snap} isn't ready")
        gates.saved(True)
        m = res["manifest"]
        tc = tree_check(snap, name, m, res.get("links_map") or {})
        log(f"{snap}: ready; its tree: {tc['files']} files, {tc['nbad']} sizes differ, {tc['extra']} extra{' (truncated)' if tc['truncated'] else ''}")
        if not gates.tree_ok(tc["nbad"], tc["extra"], tc["nbadlinks"]):  # (the snapshot's tree now gates the record)
            raise RuntimeError(f"{snap}: its tree differs from the manifest: {tc}")
        rec = {"name": name, "form": "snapshot", "snapshot": snap, "source": f"modal:{volume}" + (f" ({','.join(only)})" if only else ""),
               "files": m, "bytes": res["bytes"],
               "created": time.time(), "manifest_sha256": res["manifest_sha256"], "links": res.get("links_map") or {}, **({"secrets": sec} if sec else {}),
               "copy": {"vm": vm, "parts": res["parts"], "stream_bytes": res["stream_bytes"], "stream_sha256": res["stream_sha256"], "modal_seconds": res["seconds"],
                        "mb_s": res["mb_s"], "on_vm": {k: c[k] for k in ("files", "bytes", "nbad", "nbadlinks")}, "tree": tc}}
        if encrypt:
            KEYS.mkdir(parents=True, exist_ok=True)
            (KEYS / f"{name}.wrapped.key").write_text(res["wrapped_key_b64"])
            rec.update(encrypted=True, ciphertext_sha256=res["ciphertext_sha256"], plain_manifest_sha256=res["plain_manifest_sha256"],
                       plain_files=res["plain_files"], plain_bytes=res["plain_bytes"], wrapped_key=f"{name}.wrapped.key (LIVE/keys; also /store/{name}/wrapped.key)",
                       key="LIVE/keys/volumes.pem (RSA-4096, OAEP-SHA256; AES-256-CBC + HMAC-SHA256)", roundtrip=dec)
        if not gates.record():
            raise RuntimeError(f"{name}: not every gate passed ({gates.state})")
        STORE.mkdir(parents=True, exist_ok=True)
        record_path(name).write_text(json.dumps(rec, indent=1))
        log(f"{name}: recorded in {record_path(name)}")
        ok = True
    finally:
        if ok or not keep:
            delete_vm(vm)
        else:
            log(f"{vm}: kept after the failure (--keep): `copy ... --vm {vm}` again, or `cleanup`")


def verify(name: str, vm_type: str = "large"):
    vm = new_vm(f"store-{name}", f"verify-{name}", vm_type=vm_type)
    try:
        return restore_check(vm, name)
    finally:
        delete_vm(vm)


def cleanup():
    for f in sorted(TICKETS.glob("*.json")) if TICKETS.exists() else []:
        t = json.loads(f.read_text())
        # (Serving.cleanup: its client's pid on the host that wrote the ticket; an older ticket without one, this host's)
        if not Serving().cleanup(boat.pid_alive(int(t.get("pid", 0)), t.get("host") or socket.gethostname())):
            log(f"{t['vm']}: its client {t.get('pid')} is alive; left")
            continue
        delete_vm(t["vm"])


def ls():
    snaps = boat.named_snapshots(0)
    for f in sorted(STORE.glob("*.json")) if STORE.exists() else []:
        m = json.loads(f.read_text())
        if not m.get("source", "").startswith("modal:") or "copy" not in m:
            continue
        v = m.get("verify") or {}
        n, b = m.get("plain_files") or len(m.get("files") or {}), m.get("plain_bytes") or m["bytes"]
        print(f"{m['name']:30} {n:6} files {b / 1e9:9.3f} GB  {m['snapshot']} ({snaps.get(m['snapshot'], {}).get('status', 'missing')})"
              f"{'  encrypted' if m.get('encrypted') else ''}  verify: {('OK' if v.get('ok') else 'BAD') if v else '-'}")


def main(argv: list[str]):
    ap = argparse.ArgumentParser(prog="supercomputer volumes", description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("inventory")
    s.add_argument("names")
    s = sub.add_parser("copy")
    s.add_argument("names", nargs="+")
    s.add_argument("--encrypt", default="", help="names whose volume goes encrypted")
    s.add_argument("--split", default="", help="names whose secret-looking files go apart, encrypted (secrets.tar.enc), the rest as files")
    s.add_argument("--after", default="", help="a stored name: the first VM starts from its snapshot and re-hashes it")
    s.add_argument("--only", default="", help="these top-level entries of the (one) volume")
    s.add_argument("--as", dest="as_", default="", help="the stored name for --only")
    s.add_argument("--part-mb", type=int, default=256)
    s.add_argument("--workers", type=int, default=3)
    s.add_argument("--vm-type", default="large", choices=["small", "default", "large", "xlarge"])
    s.add_argument("--sizes", default="", help="name=bytes,... (the inventory's), to size timeouts and check the disk")
    s.add_argument("--redo", action="store_true")
    s.add_argument("--min-mbs", type=float, default=15, help="the slowest throughput expected (MB/s): sizes the Modal timeout")
    s.add_argument("--ttl", type=int, default=4 * 3600, help="the receiving VM's TTL (s)")
    s.add_argument("--vm", default="", help="a VM of ours (a ticket) to receive the first name, kept by --keep")
    s.add_argument("--keep", action="store_true", help="keep the VM when a copy fails (a retry with --vm costs no start)")
    s = sub.add_parser("verify")
    s.add_argument("name")
    s.add_argument("--vm-type", default="large", choices=["small", "default", "large", "xlarge"])
    sub.add_parser("ls")
    sub.add_parser("cleanup")
    a = ap.parse_args(argv)
    if a.cmd == "inventory":
        for r in inventory(a.names.split(",")):
            print(json.dumps({k: v for k, v in r.items() if k != "flagged"}))
    elif a.cmd == "copy":
        enc = set(filter(None, a.encrypt.split(",")))
        sizes = {k: float(v) for k, v in (x.split("=") for x in a.sizes.split(",") if x)}
        only = list(filter(None, a.only.split(",")))
        if only and (len(a.names) != 1 or not a.as_):
            raise SystemExit("--only takes one volume and --as")
        prev = a.after or None
        for vol in a.names:
            name = a.as_ or vol
            if not Serving().copy_again(done(name), a.redo):
                log(f"{name}: already stored ({record_path(name).name}); next")
                prev = name
                continue
            copy_one(vol, name, prev, vol in enc, only, a.part_mb, a.workers, a.vm_type, sizes.get(name, sizes.get(vol, 0.0)), a.vm, a.keep, a.min_mbs, a.ttl, vol in set(filter(None, a.split.split(","))))
            prev, a.vm = name, ""
        log(f"done; `supercomputer volumes verify {prev}` checks the last one restored")
    elif a.cmd == "verify":
        v = verify(a.name, a.vm_type)
        sys.exit(0 if v["ok"] else 1)
    elif a.cmd == "ls":
        ls()
    elif a.cmd == "cleanup":
        cleanup()
