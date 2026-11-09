"""Modal's side of moving volumes to boat: an inventory (sizes, files, secret-looking files by kind and count, never
their contents) and a streaming copy of one volume to a boat receiver (supercomputer/volumes.py), so no byte passes
through the laptop.

    modal run modal_volumes.py::inventory --names a,b,c           (or through `supercomputer volumes inventory`)

Each function mounts the volume read-only, has scaledown_window=2 and a timeout sized to its job.
"""
import hashlib
import json
import os
import pathlib
import re
import time

import modal

app = modal.App("supercomputer-volumes")
# (rules/integrity.py rides along as sc_integrity: the container decides by the very rules proven here)
RULES = pathlib.Path(__file__).resolve().parent / "rules" / "integrity.py"
image = modal.Image.debian_slim(python_version="3.12").apt_install("openssl", "curl", "zstd", "ca-certificates").add_local_file(str(RULES), "/root/sc_integrity.py")


def _rules():
    """rules/integrity.py: sc_integrity in the container, the file beside this one here (one source, never a copy)."""
    try:
        import sc_integrity
        return sc_integrity
    except ImportError:
        import importlib.util
        import sys
        if "sc_integrity" not in sys.modules:
            spec = importlib.util.spec_from_file_location("sc_integrity", RULES)
            mod = importlib.util.module_from_spec(spec)
            sys.modules["sc_integrity"] = mod
            spec.loader.exec_module(mod)
        return sys.modules["sc_integrity"]

# A file whose name says it holds a credential, by kind (the kinds are reported, the names and contents never).
NAME_KINDS = [
    ("cookies", re.compile(r"(?i)(^|/)(cookies?(\.sqlite|\.json|\.txt)?|cookie[-_]?jar.*)$")),
    ("browser-profile-secret", re.compile(r"(?i)(^|/)(login data|web data|local state|local storage|session storage|indexeddb|token\.dat)(/|$)")),
    ("session/state", re.compile(r"(?i)(session|storage[-_]?state|auth[-_]?state)[^/]*\.(json|db|sqlite|txt|bin|pkl)$")),
    ("env-file", re.compile(r"(?i)(^|/)\.env(\.[^/]*)?$")),
    ("key/cert", re.compile(r"(?i)(\.pem|\.key|\.p12|\.pfx|id_rsa|id_ed25519)$")),
    ("credentials", re.compile(r"(?i)(credential|secret|password|passwd|\.netrc|token|oauth|api[-_]?key)[^/]*$")),
    ("hf-token", re.compile(r"(^|/)(token|stored_tokens)$")),  # (Hugging Face's login: HF_HOME/token, HF_HOME/stored_tokens)
]
# ... but not a model's tokenizer files (tokenizer.json, tokenizer_config.json, special_tokens_map.json, added_tokens.json,
# *.tiktoken, ...), which "token" in the credentials pattern would take for one.
NAME_FINE = re.compile(r"(?i)(^|/)([^/]*tokeniz[^/]*|[^/]*_tokens?(_map|_config)?\.json|added_tokens[^/]*|[^/]*\.tiktoken|[^/]*tokens?\.(txt|model|vocab))$")
# A text file's content that looks like a credential (counted by kind, never printed).
CONTENT_KINDS = [
    ("private-key", re.compile(rb"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("api-key", re.compile(rb"\b(sk-[A-Za-z0-9_-]{20,}|sk_live_[A-Za-z0-9]{16,}|ghp_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|AKIA[0-9A-Z]{16}|xox[abp]-[A-Za-z0-9-]{10,}|hf_[A-Za-z0-9]{30,}|AIza[0-9A-Za-z_-]{30,})")),
    ("bearer/jwt", re.compile(rb"(Bearer [A-Za-z0-9._-]{20,}|eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.)")),
    ("cookie-values", re.compile(rb"\"(httpOnly|sameSite)\"\s*:")),
    ("password-field", re.compile(rb"(?i)\"(password|passwd|secret|access_token|refresh_token|client_secret|api_key)\"\s*:\s*\"[^\"]{4,}\"")),
]
TEXT_MAX = 4 << 20  # content is scanned in files up to this size


def scan(root: pathlib.Path, budget: float = 1500) -> dict:
    """... with the files' contents read for `budget` seconds at most (past it, names only: content_partial)."""
    t0 = time.time()
    partial = False
    files = nbytes = 0
    biggest: list = []
    top: dict = {}
    kinds: dict = {}
    flagged = 0
    names: list = []
    links = 0
    for dirpath, dirnames, filenames in os.walk(root):
        for fn in filenames:
            p = pathlib.Path(dirpath) / fn
            try:
                st = p.lstat()
            except OSError:
                continue
            if p.is_symlink():
                links += 1
                continue
            if not p.is_file():
                continue
            rel = str(p.relative_to(root))
            files += 1
            nbytes += st.st_size
            first = rel.split("/", 1)[0]
            top[first] = top.get(first, 0) + st.st_size
            biggest.append((st.st_size, rel))
            if len(biggest) > 200:
                biggest = sorted(biggest, reverse=True)[:20]
            hit = set()
            for k, rx in NAME_KINDS:
                if rx.search(rel) and not (k == "credentials" and NAME_FINE.search(rel)):
                    hit.add(k)
            if not partial and time.time() - t0 > budget:
                partial = True
            if _rules().Secrets().scan_content(st.st_size, TEXT_MAX, False, partial):
                try:
                    data = p.read_bytes()
                except OSError:
                    data = b""
                if _rules().Secrets().scan_content(st.st_size, TEXT_MAX, b"\0" in data[:4096], partial):
                    for k, rx in CONTENT_KINDS:
                        if rx.search(data):
                            hit.add("content:" + k)
            if hit:
                flagged += 1
                if len(names) < 60:
                    names.append([sorted(hit), rel])  # (a name and its kinds, for the classification: never content)
                for k in hit:
                    kinds[k] = kinds.get(k, 0) + 1
    big = sorted(biggest, reverse=True)[:5]
    return {"files": files, "bytes": nbytes, "top": dict(sorted(top.items(), key=lambda x: -x[1])[:12]), "largest": [s for s, _ in big],
            "links": links, "content_partial": partial, "secret_files": flagged, "secret_kinds": kinds, "flagged": names}


def mounts(names: list[str]) -> dict:
    return {f"/v/{n}": modal.Volume.from_name(n) for n in names}


@app.local_entrypoint()
def inventory(names: str, out: str = "", budget: float = 1500):
    """Every named volume scanned in a container of its own (mounted read-only)."""
    rows = []
    calls = []
    for n in [x for x in names.split(",") if x]:
        fn = scan_volume.with_options(volumes={f"/v/{n}": modal.Volume.from_name(n).read_only()})
        calls.append((n, fn.spawn(n, budget)))
    try:
        for n, c in calls:
            try:
                r = c.get(timeout=1900)
            except Exception as e:
                r = {"name": n, "error": repr(e)[:300]}
            rows.append(r)
            print(json.dumps({k: v for k, v in r.items() if k != "flagged"}), flush=True)
    finally:
        for _, c in calls:
            try:
                c.cancel()
            except Exception:
                pass
    if out:
        pathlib.Path(out).write_text(json.dumps(rows, indent=1))


@app.function(image=image, scaledown_window=2, timeout=1800, cpu=2.0, memory=4096)
def scan_volume(name: str, budget: float = 1500) -> dict:
    t0 = time.time()
    root = pathlib.Path(f"/v/{name}")
    if not root.exists():
        return {"name": name, "error": "not mounted"}
    return {"name": name, **scan(root, budget), "seconds": round(time.time() - t0, 1)}


# ------------------------------------------------------------------------------------------------- the copy to boat
# The volume, mounted read-only, goes as one tar stream cut into parts, each PUT (with its sha256) to the receiver on a
# boat VM (volumes.py's RECV, behind `boat host --private`); the files' sha256 are taken as the tar reads them, so the
# volume is read once. The manifest ({path: [size, sha256]}, a symlink to a file in the volume listed with its
# target's) and links.json ({path: target}) end the stream. --encrypt: the tar (manifest inside) in memory, AES-256-CBC
# with a fresh key and HMAC-SHA256 over iv and ciphertext, both keys wrapped with the RSA public key given (OAEP,
# SHA-256); only the ciphertext, the wrapped keys and the ciphertext's manifest leave.

def _client(url: str, tok: str):
    import http.cookiejar
    import urllib.parse
    import urllib.request
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    u = urllib.parse.urlparse(url)
    tries = _rules().Tries(6)
    for k in range(6):  # (boat's first answer sets a cookie and redirects: the cookie, once; Tries, 6)
        try:
            op.open(url, timeout=60).read()
            tries.attempt(True)
            break
        except Exception as e:
            tries.attempt(False)
            print(f"(the receiver didn't answer: {type(e).__name__}; again)", flush=True)
            time.sleep(2 + 3 * k)

    def call(what: str, data: bytes = b"", headers: dict | None = None, timeout: int = 900) -> dict:
        import urllib.request
        # (the cookie alone: a request with boat's token in its query is redirected, which a PUT can't follow)
        req = urllib.request.Request(urllib.parse.urlunparse(u._replace(path=f"/{tok}/{what}", query="")), data=data, method="PUT",
                                     headers={"X-Token": tok, "Content-Type": "application/octet-stream", **(headers or {})})
        with op.open(req, timeout=timeout) as r:
            return json.loads(r.read() or b"{}")
    return call


def _retry(f, what: str, tries: int = 10):
    t = _rules().Tries(tries)  # (Tries: at most `tries` attempts, then given up)
    k = 0
    while t.state == "trying":
        try:
            r = f()
            if r.get("ok"):
                t.attempt(True)
                return r
            err = r.get("err") or r
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:160]}"
        t.attempt(False)
        print(f"({what}: {err}; again)", flush=True)
        if t.state == "trying":
            time.sleep(min(60, 2 + 2 ** k))
        k += 1
    raise RuntimeError(f"{what}: failed {tries} times")


class _Parts:
    """A sink for the tar stream: cut into parts of `size` bytes, each queued for an upload worker."""

    def __init__(self, size: int, q):
        self.size, self.q, self.buf, self.i, self.total, self.blocked = size, q, bytearray(), 0, 0, 0.0
        self.h = hashlib.sha256()

    def write(self, b) -> int:
        self.buf += b
        self.total += len(b)
        self.h.update(b)
        while len(self.buf) >= self.size:
            t = time.time()
            self.q.put((self.i, bytes(self.buf[:self.size])))
            self.blocked += time.time() - t  # (the reader waiting on the uploads: time the uploads bound)
            del self.buf[:self.size]
            self.i += 1
        return len(b)

    def close(self):
        if _rules().Secrets().last_part(len(self.buf), self.i):  # (an empty stream still one part)
            self.q.put((self.i, bytes(self.buf)))
            self.buf = bytearray()
            self.i += 1


class _Hashing:
    def __init__(self, f):
        self.f, self.h, self.n = f, hashlib.sha256(), 0

    def read(self, n=-1):
        b = self.f.read(n)
        self.h.update(b)
        self.n += len(b)
        return b


def _entries(root: pathlib.Path, only: list[str]):
    """(rel, path, is_link) of every regular file and symlink under root, sorted; `only`: these top-level entries."""
    out = []
    tops = sorted(only) if only else sorted(os.listdir(root))
    for t in tops:
        p = root / t
        if p.is_symlink() or p.is_file():
            out.append((t, p, p.is_symlink()))
            continue
        for dirpath, dirnames, filenames in os.walk(p):
            dirnames.sort()
            for fn in sorted(filenames + [d for d in dirnames if (pathlib.Path(dirpath) / d).is_symlink()]):
                q = pathlib.Path(dirpath) / fn
                if q.is_symlink() or q.is_file():
                    out.append((str(q.relative_to(root)), q, q.is_symlink()))
    return out


SECRET_BY_NAME = {"cookies", "browser-profile-secret", "env-file", "key/cert", "hf-token"}


def secret_kinds(rel: str, p: pathlib.Path, size: int, data: bytes | None = None) -> set:
    """What makes a file a secret, for --split: a content pattern; a name that only a secret has; or a credential or
    session name on a small file (a credential is a few KB of text: a video named *Tokens.mp4 is none)."""
    names = {k for k, rx in NAME_KINDS if rx.search(rel) and not (k == "credentials" and NAME_FINE.search(rel))}
    hit = names & SECRET_BY_NAME
    if _rules().Secrets().scan_content(size, TEXT_MAX, False, False):
        if data is None:
            try:
                data = p.read_bytes()
            except OSError:
                data = b""
        if _rules().Secrets().scan_content(size, TEXT_MAX, b"\0" in data[:4096], False):
            hit |= {"content:" + k for k, rx in CONTENT_KINDS if rx.search(data)}
            hit |= names & {"credentials", "session/state"}
    return hit


def _tar(root: pathlib.Path, only: list[str], sink, mode: str, extra_name: str = "manifest.json", split=None):
    """Tar of root (or its `only` entries) into sink; returns (manifest, links). split: (stash, finish): a file
    secret_kinds flags goes into the stash (a tarfile) instead, and finish() gives the manifest's extra entries (the
    stash, encrypted) before the manifest is written."""
    import tarfile
    m, links = {}, {}
    # (copybufsize: tarfile's default reads 16 KB at a time, which a network filesystem answers at a few MB/s)
    with tarfile.open(fileobj=sink, mode=mode, format=tarfile.PAX_FORMAT, copybufsize=8 << 20) as t:
        for rel, p, is_link in _entries(root, only):
            ti = t.gettarinfo(str(p), arcname=rel)
            ti.uid = ti.gid = 0
            ti.uname = ti.gname = ""
            if is_link:
                t.addfile(ti)
                links[rel] = os.readlink(p)
                continue
            if split and ti.size <= TEXT_MAX:  # (a small file read once: its kinds, then the one tar or the other)
                import io
                data = p.read_bytes()
                ti.size = len(data)
                if _rules().Secrets().route(bool(secret_kinds(rel, p, ti.size, data))) == "stash":
                    split[0].addfile(ti, io.BytesIO(data))
                else:
                    t.addfile(ti, io.BytesIO(data))
                    m[rel] = [ti.size, hashlib.sha256(data).hexdigest()]
                continue
            if split and _rules().Secrets().route(bool(secret_kinds(rel, p, ti.size))) == "stash":
                split[0].add(str(p), arcname=rel, recursive=False)
                continue
            with open(p, "rb") as f:
                hf = _Hashing(f)
                t.addfile(ti, hf)
            m[rel] = [ti.size, hf.h.hexdigest()]
        if split:
            m.update(split[1]())
        for rel, target in links.items():  # (a link to a file of the volume: its target's size and sha256)
            q = os.path.normpath(os.path.join(os.path.dirname(rel), target))
            if q in m:
                m[rel] = m[q]
        for nm, obj in ((extra_name, m), ("links.json", links)):
            b = json.dumps(obj, sort_keys=True).encode()
            ti = tarfile.TarInfo(nm)
            ti.size, ti.mtime = len(b), int(time.time())
            import io
            t.addfile(ti, io.BytesIO(b))
    return m, links


def _encrypt(plain: bytes, pubkey_pem: str) -> tuple[bytes, bytes]:
    """AES-256-CBC (fresh key and iv) then HMAC-SHA256 over iv and ciphertext; the two keys wrapped with RSA-OAEP."""
    import hmac
    import subprocess
    import tempfile
    k_enc, k_mac, iv = os.urandom(32), os.urandom(32), os.urandom(16)
    ct = subprocess.run(["openssl", "enc", "-aes-256-cbc", "-K", k_enc.hex(), "-iv", iv.hex()], input=plain, capture_output=True, check=True).stdout
    mac = hmac.new(k_mac, iv + ct, hashlib.sha256).digest()
    with tempfile.NamedTemporaryFile("w", suffix=".pem") as f:
        f.write(pubkey_pem)
        f.flush()
        wrapped = subprocess.run(["openssl", "pkeyutl", "-encrypt", "-pubin", "-inkey", f.name, "-pkeyopt", "rsa_padding_mode:oaep",
                                  "-pkeyopt", "rsa_oaep_md:sha256", "-pkeyopt", "rsa_mgf1_md:sha256"], input=k_enc + k_mac, capture_output=True, check=True).stdout
    return b"SCV1" + iv + ct + mac, wrapped


# (nonpreemptible: a preempted copy restarts from its first byte, as a 110 GB one did 32 GB in; it costs 3x a CPU's
# rate, on 2 CPUs: the tar and the hashing keep up with the upload on them)
@app.function(image=image, scaledown_window=2, timeout=3600, cpu=2.0, memory=6144, nonpreemptible=True)
def send_volume(name: str, conf: dict) -> dict:
    import queue
    import threading
    t0 = time.time()
    root = pathlib.Path(f"/v/{name}")
    if not root.exists():
        return {"name": name, "error": "not mounted"}
    dest, tok = conf["dest"], conf["token"]
    call = _client(conf["url"], tok)
    part = int(conf.get("part_mb", 256)) << 20
    workers = int(conf.get("workers", 3))
    q: queue.Queue = queue.Queue(maxsize=workers)
    sent = {"bytes": 0, "parts": 0}
    errors: list = []

    def up():
        while True:
            it = q.get()
            if it is None:
                return
            i, data = it
            if errors:
                continue
            h = hashlib.sha256(data).hexdigest()
            try:
                _retry(lambda: call("part", data, {"X-Name": dest, "X-Part": str(i), "X-Sha256": h}), f"part {i}")
                sent["bytes"] += len(data)
                sent["parts"] += 1
                if sent["parts"] % 8 == 0:
                    el = time.time() - t0
                    print(f"{dest}: {sent['parts']} parts, {sent['bytes'] / 1e9:.2f} GB in {el:.0f} s ({sent['bytes'] / 1e6 / max(el, 1):.0f} MB/s)", flush=True)
            except Exception as e:
                errors.append(repr(e)[:300])

    ths = [threading.Thread(target=up, daemon=True) for _ in range(workers)]
    for t in ths:
        t.start()
    out: dict = {"name": name, "dest": dest}
    try:
        if conf.get("pubkey"):
            _retry(lambda: call("begin", b"", {"X-Name": dest, "X-Mode": "file", "X-Path": "data.tar.enc"}), "begin")
            import io
            buf = io.BytesIO()
            m, links = _tar(root, conf.get("only") or [], buf, "w", "manifest.plain.json")
            plain = buf.getvalue()
            if not _rules().Sizes().fits(len(plain), 1 << 30):
                raise RuntimeError("too big to encrypt in memory")
            blob, wrapped = _encrypt(plain, conf["pubkey"])
            pm = json.dumps(m, sort_keys=True).encode()
            out.update(encrypted=True, plain_files=len(m), plain_bytes=sum(v[0] for v in m.values()), plain_links=len(links),
                       plain_manifest_sha256=hashlib.sha256(pm).hexdigest(), plain_tar_sha256=hashlib.sha256(plain).hexdigest())
            del plain, buf
            sink = _Parts(part, q)
            sink.write(blob)
            sink.close()
            wb = __import__("base64").b64encode(wrapped)
            cm = {"data.tar.enc": [len(blob), hashlib.sha256(blob).hexdigest()], "wrapped.key": [len(wb), hashlib.sha256(wb).hexdigest()]}
            out.update(ciphertext_sha256=cm["data.tar.enc"][1], wrapped_key_b64=wb.decode())
            m, links = cm, {}
            files = {"wrapped.key": wb}
        else:
            _retry(lambda: call("begin", b"", {"X-Name": dest, "X-Mode": "tar"}), "begin")
            sink = _Parts(part, q)
            files = {}
            split = None
            if conf.get("split"):  # (the secrets of the volume: kept apart, encrypted, beside the rest)
                import io
                import tarfile
                sbuf = io.BytesIO()
                stash = tarfile.open(fileobj=sbuf, mode="w", format=tarfile.PAX_FORMAT)

                def finish():
                    import base64
                    sm = {}
                    stash.close()
                    plain = sbuf.getvalue()
                    kinds: dict = {}
                    with tarfile.open(fileobj=io.BytesIO(plain)) as tt:
                        for ti in tt.getmembers():
                            if ti.isfile():
                                b = tt.extractfile(ti).read()
                                sm[ti.name] = [len(b), hashlib.sha256(b).hexdigest()]
                                for k in secret_kinds(ti.name, root / ti.name, len(b), b if len(b) <= TEXT_MAX else None):
                                    kinds[k] = kinds.get(k, 0) + 1
                    mb = json.dumps(sm, sort_keys=True).encode()
                    full = io.BytesIO()
                    with tarfile.open(fileobj=full, mode="w", format=tarfile.PAX_FORMAT) as tt:
                        with tarfile.open(fileobj=io.BytesIO(plain)) as src:
                            for ti in src.getmembers():
                                tt.addfile(ti, src.extractfile(ti) if ti.isfile() else None)
                        ti = tarfile.TarInfo("manifest.plain.json")
                        ti.size, ti.mtime = len(mb), int(time.time())
                        tt.addfile(ti, io.BytesIO(mb))
                    plain = full.getvalue()
                    if not _rules().Sizes().fits(len(plain), 512 << 20):
                        raise RuntimeError(f"the secrets are {len(plain) >> 20} MB: too many to keep apart")
                    blob, wrapped = _encrypt(plain, conf["split"])
                    wb = base64.b64encode(wrapped)
                    files.update({"secrets.tar.enc": blob, "secrets.wrapped.key": wb})
                    out.update(secrets={"files": len(sm), "bytes": sum(v[0] for v in sm.values()), "kinds": kinds, "plain_manifest_sha256": hashlib.sha256(mb).hexdigest(),
                                        "plain_tar_sha256": hashlib.sha256(plain).hexdigest(), "ciphertext_sha256": hashlib.sha256(blob).hexdigest(),
                                        "wrapped_key_b64": wb.decode()})
                    return {k: [len(v), hashlib.sha256(v).hexdigest()] for k, v in files.items()}
                split = (stash, finish)
            m, links = _tar(root, conf.get("only") or [], sink, "w|", split=split)
            sink.close()
            out["read_s"] = round(time.time() - t0, 1)
        for _ in ths:
            q.put(None)
        for t in ths:
            t.join()
        if errors:
            raise RuntimeError(f"upload: {errors[0]}")
        r = _retry(lambda: call("end", b"", {"X-Name": dest, "X-Parts": str(sink.i)}, timeout=3600), "end")
        mb = json.dumps(m, sort_keys=True).encode()
        files.update({"manifest.json": mb} if conf.get("pubkey") else {})
        for nm, b in files.items():
            _retry(lambda: call("file", b, {"X-Name": dest, "X-Path": nm, "X-Sha256": hashlib.sha256(b).hexdigest()}), f"file {nm}")
        el = time.time() - t0
        # (a link's target counted once; the manifest's sha256 for the receiving side's check, which needs it)
        out.update(files=len(m), bytes=sum(v[0] for k, v in m.items() if k not in links), links=len(links), manifest_sha256=hashlib.sha256(mb).hexdigest(),
                   manifest=m, links_map=links, stream_bytes=sink.total, stream_sha256=sink.h.hexdigest(),
                   parts=sink.i, received=r.get("bytes"), upload_bound_s=round(sink.blocked, 1), seconds=round(el, 1), mb_s=round(sink.total / 1e6 / max(el, 1e-3), 1))
        return out
    finally:
        errors.append("stop")
        for _ in ths:
            with __import__("contextlib").suppress(Exception):
                q.put_nowait(None)


@app.local_entrypoint()
def copy(conf: str, out: str):
    """One volume to a boat receiver: conf is a local 0600 JSON (the receiver's URL and token, the volume, the
    encryption's public key); the result (no URL, no key) into out."""
    c = json.loads(pathlib.Path(conf).read_text())
    n = c["volume"]
    # (memory: the parts queued and in flight, 2 per worker, and room)
    mem = max(6144, int(2.2 * int(c.get("part_mb", 256)) * int(c.get("workers", 3))) + 2048)
    fn = send_volume.with_options(volumes={f"/v/{n}": modal.Volume.from_name(n).read_only()}, timeout=int(c.get("timeout", 3600)), memory=mem)
    call = fn.spawn(n, c)
    try:
        r = call.get(timeout=int(c.get("timeout", 3600)) + 120)
    except BaseException as e:
        call.cancel()
        r = {"name": n, "error": repr(e)[:400]}
    pathlib.Path(out).write_text(json.dumps(r))
    print(json.dumps({k: v for k, v in r.items() if k not in ("manifest", "links_map", "wrapped_key_b64")}), flush=True)
