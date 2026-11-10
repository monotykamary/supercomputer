"""The VM side of the supercomputer: one file, the Python standard library only, sent to every VM (boat) or pod
(RunPod) a batch uses. It knows nothing of any repo: what a batch needs (its project's name, PATH, mount points, build
caches) comes with the batch.

    python3 agent.py prep <batch> <tree source> <context, base64 JSON> [tree id]   a batch's tree, ready
    python3 agent.py tick <batch> <base64 JSON, in parts>       start items, ack reported ones, wait (<= 25 s) for a finish
    python3 agent.py kill <batch>                               kill every running item of the batch
    python3 agent.py collect <batch> <part>                     the batch's logs, as base64 tar.gz parts of 5 MB
    python3 agent.py cacheout <name> <key> [part]               a build cache's files for a key ("-": their names)
    python3 agent.py cachein <name> <key> <tar>                 files of a build cache, into this VM's
    python3 agent.py daemon <port>                              the warm agent (below), detached
    python3 agent.py run <batch> <name>                         (internal) one item, detached

The warm agent: a long-lived HTTP server on 127.0.0.1:<port>, reached through boat's token-gated HTTPS URL
(`boat host --private`) and a token of the client's own (~/.sc/token, sent in X-SC-Token), so a batch on a VM that is
already up needs no boat API call. It keeps every file it was ever sent in a store by content (~/.sc/blobs/<sha256>
and an x for an executable), so a tree is synced by hash (the client asks which are missing and sends only those) and
made (~/.sc/trees/<id>) by hard links; the same commands as above answer as JSON over POST /rpc.

An item runs in a mount namespace of its own: a private /tmp, its own /work (a fresh copy of the batch's tree at
/work/<name> for a shell job), the batch's tree read-only at /src/<name> (for the runners that copy it themselves) and
the mounts its runner names; then as the VM's user on its own CPUs (`taskset`, so `nproc` is the job's share), under
`timeout -s KILL`. A shell job gets the batch's build caches (files keyed by the hash of their sources) before it runs
and the files it built are saved after. A Python item (`py`) imports a Modal runner with the stub below as `modal`
(each decorator gives the function back) and calls one of its functions.

Opt-in strict_once specs additionally supply a unique launch_identity nonce. Their runner is PID1 of a private
PID namespace created by unshare --pid --fork --kill-child=KILL --mount-proc, with a private mount namespace.
The agent binds that init to a pidfd before authorizing payload. cancelitem(batch, name, wait_seconds=10,
expected_identity=None) fences future starts and requires both the exact init's exit and its owned Popen wait
for a positive receipt. Receipts bind batch/name/identity; legacy mount-only jobs cannot prove namespace absence.
Never-started fenced jobs have no child to reap (reaped is vacuously true). Repeated receipts survive batch cleanup;
a restarted agent refuses live non-child ownership rather than signaling a recovered PID. Existing job/ownership
collisions refuse admission unless the exact original strict spec and durable authorized identity prove a repeat.
Strict terminal rows carry their admission nonce at creation; report never upgrades old rows. Strict tick/vtick
acks require {batch, name, launch_identity} (batch may be omitted in a batch tick); legacy string acks remain legacy.
Legacy kill APIs refuse strict jobs: use the identity-bound cancelitem API. Strict ownership is a
trusted-workload lifecycle guarantee, not a sandbox against malicious jobs using the VM's passwordless sudo.
"""
import base64
import contextlib
import hashlib
import hmac
import io
import json
import os
import pathlib
import re
import secrets
import math
import shutil
import signal
import subprocess
import sys
import tarfile
import threading
import time
from typing import Literal

ROOT = pathlib.Path.home() / ".sc"
HOME = str(pathlib.Path.home())
BASE_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
VERSION = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()[:12]  # (its code: the scheduler upgrades a stale one in place)


# ------------------------------------------------------------------------------------------------- the agent's rules
# Its decisions, apart from processes, files and the network: each class decides and records, the functions below act
# on what it says. They are here, in the agent's one file, because the agent ships alone to each VM; supercomputer/
# rules/agent.py is this file, and their laws (`# gdp: law`) are proven by gdp-wired with the rest
# (tests/check_laws.sh). Each is made from what the files say (a snapshot) and asked once.
Report = Literal["done", "running", "lost", "torn", "skip"]


class JobReport:
    """What an item of a batch on this VM is, from its files: acked (reported and taken: never again), done (its
    done.json), torn (a done.json half written: running, read again next time), running (its process alive, or being
    started), lost (its process gone without a word, or no process STARTING_S after its start: never running for
    ever)."""

    # gdp: law an acked item is never reported again: not acked or result == "skip"
    # gdp: law an item whose process died without a word is lost, never running: not (has_pid and not alive and not done_file and not acked) or result == "lost"
    # gdp: law an item with no process is running only while it is being started: has_pid or done_file or acked or age_s <= starting_s or result == "lost"

    def __init__(self, starting_s: int):
        if starting_s < 1:
            raise ValueError("starting_s is at least 1")
        self.starting_s = starting_s
        self.result: Report = "running"

    def read(self, acked: bool, done_file: bool, readable: bool, has_pid: bool, alive: bool, age_s: int) -> Report:
        if acked:
            self.result = "skip"
        elif done_file:
            self.result = "done" if readable else "torn"
        elif has_pid:
            self.result = "running" if alive else "lost"
        elif age_s > self.starting_s:
            self.result = "lost"
        else:
            self.result = "running"
        return self.result


class CpuGrant:
    """start's CPUs: a job gets the CPUs it asks for that no running job holds; with fewer free, the free ones (at least
    one); with none free, it runs on CPUs it shares (held by none: never counted as held twice)."""

    # gdp: law CPUs held never exceed the VM's: held <= cpus
    # gdp: law a job never holds more CPUs than it asked for: result <= want or want < 1

    def __init__(self, cpus: int):
        if cpus < 1:
            raise ValueError("a VM has a CPU")
        self.cpus = cpus
        self.held = 0  # CPUs held by running jobs (cpus_in_use)

    def grant(self, want: int) -> int:
        """How many free CPUs the job holds (0: it shares CPUs 0..want-1)."""
        k = min(max(1, want), self.cpus - self.held)
        if k < 1:
            return 0
        self.held += k
        return k

    def free(self, k: int) -> None:
        self.held -= min(max(0, k), self.held)


class BlobStore:
    """The store by content (~/.sc/blobs/<sha256>[x]) and the trees made of it. A blob is kept only under the key its
    bytes hash to; a tree is made only under the id its manifest hashes to (the client's id is never taken on trust);
    no blob is collected while a tree is being made from the store."""

    def __init__(self):
        self.stored = 0
        self.trees = 0
        self.building = 0  # trees being made from the store now

    # gdp: law no blob is stored under a key its bytes don't hash to: result == (digest == key_hash)
    def put(self, key_hash: str, digest: str) -> bool:
        """A blob named by its key (the key's hash part) whose bytes hash to `digest`: stored, or refused."""
        if digest != key_hash:
            return False
        self.stored += 1
        return True

    def begin_tree(self) -> None:
        self.building += 1

    def end_tree(self) -> None:
        if self.building > 0:
            self.building -= 1

    # gdp: law no tree is made under an id its manifest doesn't hash to (nor with a blob missing): not result or (have_all and computed == tid)
    def may_make_tree(self, tid: str, computed: str, ready: bool, have_all: bool) -> bool:
        """~/.sc/trees/<tid> made from the store (`computed`: the id of the manifest given, as tree_id computes it);
        False where it is there already (ready), a blob is missing, or the id isn't the manifest's."""
        if ready or not have_all or computed != tid:
            return False
        self.trees += 1
        return True

    # gdp: law a tree in use or among the newest kept is never removed: not result or (not in_use and rank >= keep)
    # gdp: law a tree a batch prepared here uses is never removed: not result or not prepared
    def drop_tree(self, rank: int, keep: int, in_use: bool, prepared: bool) -> bool:
        """gc_trees: a tree (rank: its place by recency, 0 the newest) is removed past the newest `keep` unless an item uses it
        or a batch prepared here does (the scheduler places that batch's items here on its word; 10-31 23:40-00:50: 1,243
        rows lost, "the batch's tree isn't on" this VM, collected while the batch had nothing running)."""
        return rank >= keep and not in_use and not prepared

    # gdp: law no blob is collected while a tree is being made: not result or building == 0
    # gdp: law a blob a tree links is never collected: not result or links == 1
    def drop_blob(self, links: int, age_s: int, many_trees: bool) -> bool:
        """gc_trees: a blob no tree links (one link: the store's own), a day old, goes when trees were collected, and
        never while a tree is being made (it may be the blob that tree links next)."""
        return links == 1 and age_s > 86400 and many_trees and self.building == 0


class BatchDirs:
    """drop_old_batches and vtick's starts: another batch's directory goes once it has been quiet keep_s and nothing of
    it runs; an item starts only in a batch prepared on this VM."""

    def __init__(self, keep_s: int):
        if keep_s < 0:
            raise ValueError("keep_s is at least 0")
        self.keep_s = keep_s

    # gdp: law the batch being prepared is never dropped: not result or not current
    # gdp: law a batch with a live item is never dropped: not result or not live_item
    # gdp: law a batch is dropped only after keep_s quiet: not result or quiet_s > keep_s
    def drop(self, current: bool, quiet_s: int, live_item: bool) -> bool:
        return not current and quiet_s > self.keep_s and not live_item

    # gdp: law no item starts in a batch not prepared here: not result or ready
    def may_start(self, ready: bool) -> bool:
        return ready


class Tarballs:
    """prep's cache of the tarballs this VM unpacked lately (by the tree's hash): the newest `keep` stay."""

    # gdp: law at most keep tarballs stay: not result or rank < keep

    def __init__(self, keep: int):
        if keep < 1:
            raise ValueError("keep is at least 1")
        self.keep = keep

    def stays(self, rank: int) -> bool:
        return rank < self.keep


class CacheFiles:
    """A build cache's files for a key: cachein adds a file only when it isn't there (never overwrites one, never a
    dot-file); caches_out saves an output the job produced that the cache didn't have."""

    # gdp: law a cache file there is never overwritten: not result or not present
    def take_in(self, present: bool, dotted: bool) -> bool:
        return not present and not dotted

    # gdp: law only an output the job built and the cache lacked is saved: not result or (produced and not had)
    def save(self, produced: bool, had: bool) -> bool:
        return produced and not had


class Outcome:
    """run's code for an item: killed by `timeout -s KILL` at its limit, 124 (as Modal's runner reports its own limit);
    an item whose batch's tree is gone from the VM, lost (it runs elsewhere)."""

    # gdp: law a kill is a timeout only at the item's limit: result != 124 or code == 124 or seconds >= limit - 1
    # gdp: law an item whose tree is gone is lost: tree_ok or result == -1

    def __init__(self):
        self.result = 0

    def code(self, tree_ok: bool, code: int, seconds: int, limit: int) -> int:
        if not tree_ok:
            self.result = -1
        elif (code == 137 or code == -9) and seconds >= limit - 1:
            self.result = 124
        else:
            self.result = code
        return self.result


# gdp: law a job is reported killed for memory only when the kernel's OOM killer killed while it ran: not result or oom_after > oom_before
# gdp: law a kill short of its limit while the OOM killer killed is reported as out of memory: result or not (code == 137 or code == -9) or seconds >= limit - 1 or oom_after <= oom_before
def out_of_memory(code: int, seconds: int, limit: int, oom_before: int, oom_after: int) -> bool:
    """A job's SIGKILL (137) before its limit, while the kernel's OOM killer killed on its VM (/proc/vmstat oom_kill
    went up): out of memory, reported as such, not as a plain failure (11-01: 5 of 48 Lean checks of the bar, cpu 4
    and memory 16384 on a large VM, exit 137 after 5-10 min)."""
    return (code == 137 or code == -9) and seconds < limit - 1 and oom_after > oom_before


def oom_kills() -> int:
    """The kernel's OOM kills on this VM since its boot."""
    with contextlib.suppress(Exception):
        for line in pathlib.Path("/proc/vmstat").read_text().splitlines():
            if line.startswith("oom_kill "):
                return int(line.split()[1])
    return 0


def mem_total_mb() -> int:
    with contextlib.suppress(Exception):
        for line in pathlib.Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) // 1024
    return 0


# gdp: law scratch a running job may use is never dropped: not result or mtime < oldest_running
# gdp: law scratch older than every running job goes when the disk is low: result or not low or mtime >= oldest_running
def scratch_drops(low: bool, mtime: int, oldest_running: int) -> bool:
    """clean_disk: a scratch entry (a job's leftovers outside its own /tmp: anvil's state folders) goes when the disk is
    low and it is older than the oldest running job's start, so no running job made it. Before: none went (11-02, 615 of
    anvil's state folders, 107 GB, filled bx_sfrge946's 147 GB disk: 649 prepares failed, no space left on device)."""
    return low and mtime < oldest_running


def disk() -> dict:
    """Free and total MB of the disk the agent's files and the jobs' home are on (each vtick's answer)."""
    u = shutil.disk_usage(HOME)
    return {"free_mb": u.free >> 20, "total_mb": u.total >> 20}


def disk_low(free_mb: int, total_mb: int) -> bool:
    """The agent cleans below twice the scheduler's threshold (rules.disk_ok: min(10 GB, a tenth of the disk))."""
    return free_mb < 20480 and free_mb * 5 < total_mb


class Ticks:
    """tick and vtick: an ack takes only an item that finished (its done.json, then acked.json); the wait for a finish
    ends at the first finish, when nothing runs, or after `wait` seconds."""

    # gdp: law an ack takes only a finished item: not result or done_file
    def ack(self, done_file: bool) -> bool:
        return done_file

    # gdp: law the wait never outlasts its limit while something runs and nothing finished: result or (not any_done and any_running and waited_s < wait_s)
    def stop(self, any_done: bool, any_running: bool, waited_s: int, wait_s: int) -> bool:
        return any_done or not any_running or waited_s >= wait_s


class Kill:
    """kill and kill_item: only an item with a process that hasn't finished (nor been acked) is killed; a killed item
    is reported preempted (the scheduler queues it again)."""

    # gdp: law a finished item is never killed: not result or (has_pid and not done)
    def kills(self, has_pid: bool, done: bool) -> bool:
        return has_pid and not done


class Access:
    """The warm agent's door: /health answers anyone (it says only that it's up); everything else needs the client's
    token. start_daemon keeps a daemon that answers with this version and token, and replaces any other."""

    # gdp: law nothing but /health answers without the token: health or token_ok or not result
    def allows(self, health: bool, token_ok: bool) -> bool:
        return health or token_ok

    # gdp: law a daemon kept is alive and on this version and token: result != "keep" or (alive and same_token and same_version)
    def keep_daemon(self, alive: bool, same_token: bool, same_version: bool) -> Literal["keep", "replace", "start"]:
        if alive and same_token and same_version:
            return "keep"
        if alive:
            return "replace"
        return "start"


class PeerTries:
    """peer_req: a request to another VM's agent, tried at most `tries` times (a redirect, a refusal or a network error
    each one), then given up."""

    # gdp: law a request is tried at least once: tries >= 1
    # gdp: law at most tries attempts: used <= tries
    # gdp: law a request still trying has a try left: state != "trying" or used < tries

    def __init__(self, tries: int):
        if tries < 1:
            raise ValueError("tries is at least 1")
        self.tries = tries
        self.used = 0
        self.state: Literal["trying", "done", "failed"] = "trying"

    def attempt(self, ok: bool) -> None:
        if self.state != "trying":
            return
        self.used += 1
        if ok:
            self.state = "done"
        elif self.used >= self.tries:
            self.state = "failed"

    # gdp: law a request ends: state == "trying" leads to state != "trying" by tries - used via attempt(ok = false)


class Parts:
    """collect and cacheout's parts of a blob: made again for part 0 (a new collection) or when missing; at least one
    part, each `size` bytes."""

    def __init__(self, size: int):
        if size < 1:
            raise ValueError("size is at least 1")
        self.size = size

    # gdp: law a blob has at least one part: result >= 1
    def count(self, whole: int) -> int:
        """The parts of a blob whose size divided by `size`, rounded up, is `whole` (a division by a variable is the
        code's; the rule is that there is always one)."""
        return max(1, whole)

    # gdp: law part 0 makes the blob again: not first or result
    def remake(self, first: bool, exists: bool) -> bool:
        return first or not exists


# gdp: law a zombie is never alive: not zombie or not result
def alive_of(exists: bool, permission: bool, zombie: bool) -> bool:
    """alive: a process that exists (or that we may not signal) and isn't a zombie."""
    return (exists or permission) and not zombie


# ---------------------------------------------------------------------------------------------------------------- stub
class _Any:
    """Whatever a Modal image or volume definition chains (`.apt_install(...).run_commands(...)`): nothing."""

    def __init__(self, *a, **k):
        pass

    def __getattr__(self, name):
        return _Any()

    def __call__(self, *a, **k):
        return _Any()


class _App(_Any):
    app_id = None

    def function(self, *a, **k):
        return lambda f: f

    def local_entrypoint(self, *a, **k):
        return lambda f: f

    def cls(self, *a, **k):
        return lambda c: c


def modal_stub():
    import types
    m = types.ModuleType("modal")
    m.__gdpw_stub__ = True
    m.__sc_stub__ = True
    m.App = _App
    m.Image = _Any()
    m.Volume = _Any()
    m.Secret = _Any()
    m.Mount = _Any()
    m.gpu = _Any()
    return m


def import_runner(path: str):
    import importlib.util
    sys.modules.setdefault("modal", modal_stub())
    spec = importlib.util.spec_from_file_location(pathlib.Path(path).stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# -------------------------------------------------------------------------------------------------------------- helpers
def batch_dir(batch: str) -> pathlib.Path:
    return ROOT / "b" / batch


def sudo(*cmd, check=True):
    return subprocess.run(["sudo", "-n", *cmd], check=check, capture_output=True, text=True)


def write_json(p: pathlib.Path, obj):
    tmp = p.with_name(p.name + f".{os.getpid()}.{threading.get_ident()}.tmp")
    tmp.write_text(json.dumps(obj))
    tmp.rename(p)


def alive(pid: int) -> bool:
    """alive_of: a process that exists (or we may not signal) and isn't a zombie."""
    exists, permission = True, False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        exists = False
    except PermissionError:
        permission = True
    zombie = False
    if exists and not permission:
        try:
            zombie = pathlib.Path(f"/proc/{pid}/stat").read_text().split(") ")[1].split()[0] == "Z"
        except Exception:
            zombie = True  # (gone between the two looks)
    return alive_of(exists, permission, zombie)


def descendants(pid: int) -> list[int]:
    kids: dict[int, list[int]] = {}
    for p in pathlib.Path("/proc").iterdir():
        if p.name.isdigit():
            try:
                ppid = int((p / "stat").read_text().split(") ")[1].split()[1])
            except Exception:
                continue
            kids.setdefault(ppid, []).append(int(p.name))
    out, todo = [], [pid]
    while todo:
        q = todo.pop()
        for c in kids.get(q, []):
            out.append(c)
            todo.append(c)
    return out


def kill_tree(pid: int):
    sudo("kill", "-KILL", *map(str, [pid, *descendants(pid)]), check=False)


def ctx_of(batch: str) -> dict:
    p = batch_dir(batch) / "ctx.json"
    return json.loads(p.read_text()) if p.exists() else {}


# ------------------------------------------------------------------------------------------------- the blob store
BLOBS = ROOT / "blobs"
TREES = ROOT / "trees"


def file_key(p: pathlib.Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest() + ("x" if os.stat(p).st_mode & 0o111 else "")


EMPTY = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def have(keys: list[str]) -> list[str]:
    """The keys this VM lacks: a blob not there, or one emptied under a key that isn't the empty file's (truncated:
    bx_bxf2qcpq kept 232 such since 10-31, and a tree of them failed 98 items with an empty module); those removed,
    sent again."""
    out = []
    for k in keys:
        p = BLOBS / k
        try:
            if p.stat().st_size == 0 and k.rstrip("x") != EMPTY:
                p.unlink()
                out.append(k)
        except FileNotFoundError:
            out.append(k)
    return out


def put_blobs(stream) -> int:
    """Blobs from a tar stream (each member named by its key), each checked against its key as it lands."""
    BLOBS.mkdir(parents=True, exist_ok=True)
    n = 0
    with tarfile.open(fileobj=stream, mode="r|*") as tf:
        for m in tf:
            if not m.isfile():
                continue
            key = pathlib.PurePosixPath(m.name).name
            data = tf.extractfile(m).read()
            if not BlobStore().put(key.rstrip("x"), hashlib.sha256(data).hexdigest()):  # (BlobStore.put)
                raise ValueError(f"blob {key[:12]}: its bytes don't hash to its name")
            tmp = BLOBS / f".{key}.{threading.get_ident()}"
            tmp.write_bytes(data)
            os.chmod(tmp, 0o755 if key.endswith("x") else 0o644)
            tmp.rename(BLOBS / key)
            n += 1
    return n


def blobs_tar(keys: list[str]) -> bytes:
    """The blobs this VM has of `keys`, as a tar (fast gzip): what a peer VM pulls instead of the controller sending."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=1) as tf:
        for k in keys:
            f = BLOBS / k
            if f.exists():
                tf.add(f, arcname=k)
    return buf.getvalue()


def peer_req(url: str, token: str, path: str, body: bytes, timeout: float = 300) -> bytes:
    """POST to another VM's agent through boat's token-gated URL (boat's token in the query; the cookie its first
    answer sets is kept for the redirect) with the agent's token in X-SC-Token."""
    import http.client
    import urllib.parse
    u = urllib.parse.urlparse(url)
    cookie, last, tries = "", None, PeerTries(6)
    for k in range(6):  # (PeerTries: each pass one attempt, at most 6)
        c = http.client.HTTPSConnection(u.netloc, timeout=timeout)
        hdr = {"X-SC-Token": token, "Content-Type": "application/octet-stream"}
        if cookie:
            hdr["Cookie"] = cookie
        q = "" if cookie else ("?" + u.query if u.query else "")
        try:
            c.request("POST", u.path.rstrip("/") + path + q, body=body, headers=hdr)
            r = c.getresponse()
            data = r.read()
            for h, v in r.getheaders():
                if h.lower() == "set-cookie":
                    cookie = "; ".join(x for x in [cookie, v.split(";")[0]] if x)
            if r.status in (301, 302, 303, 307, 308):
                tries.attempt(False)
                continue
            if r.status != 200:
                tries.attempt(False)
                last = f"{r.status} {data[:200]!r}"
                time.sleep(1 + k)
                continue
            tries.attempt(True)
            return data
        except (OSError, http.client.HTTPException) as e:
            tries.attempt(False)
            last = e
            time.sleep(1 + k)
        finally:
            c.close()
    raise RuntimeError(f"peer {path}: {last}")


def pull(url: str, token: str, keys: list[str]) -> dict:
    """Blobs this VM lacks, pulled from a peer VM's agent (so the controller sends a tree once, not once a VM)."""
    t0, want, got = time.time(), have(keys), 0
    for i in range(0, len(want), 600):
        got += put_blobs(io.BytesIO(peer_req(url, token, "/blobs", json.dumps({"keys": want[i:i + 600]}).encode())))
    return {"ok": True, "got": got, "missing": have(keys), "seconds": round(time.time() - t0, 2)}


def pullcache(url: str, token: str, name: str, key: str) -> dict:
    """A build cache's files for a key, from a peer VM's agent."""
    data = peer_req(url, token, "/rpc", json.dumps({"cmd": "cacheout", "args": [name, key, "tar"]}).encode())
    tmp = ROOT / f"cache-in-{threading.get_ident()}.tar.gz"
    tmp.write_bytes(data)
    return cachein(name, key, str(tmp))


def make_tree(tid: str, files: dict, links: dict) -> dict:
    """~/.sc/trees/<id>: each file a hard link to its blob, each symlink as given (made once; the id names the whole)."""
    with _tree_lock(tid):
        return _make_tree(tid, files, links)


_tree_locks: dict = {}
_tree_locks_lock = threading.Lock()


def _tree_lock(tid: str):
    with _tree_locks_lock:
        return _tree_locks.setdefault(tid, threading.Lock())


def _make_tree(tid: str, files, links) -> dict:
    t0 = time.time()
    dst = TREES / tid
    if (dst / ".sc-ready").exists():
        try:
            os.utime(dst)
            return {"ok": True, "again": True, "seconds": 0}
        except FileNotFoundError:  # (collected meanwhile by another tree's gc_trees: not here, made again; B6, begin's 500s)
            pass
    if files is None:
        return {"ok": False, "need": "manifest"}
    miss = have(sorted(set(files.values())))
    if miss:
        return {"ok": False, "missing": miss}
    # (BlobStore.may_make_tree: the tree is made under its id only when its manifest hashes to it, never on trust)
    if not BlobStore().may_make_tree(tid, tree_id(files, links or {}), False, True):
        return {"ok": False, "err": f"the manifest given isn't tree {tid}"}
    # (a tree being made is a dot-directory in TREES until it is renamed in: gc_trees collects no blob meanwhile)
    tmp = TREES / f".{tid}.{os.getpid()}.{threading.get_ident()}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    dirs = set()
    try:
        for path, key in files.items():
            p = tmp / path
            if p.parent not in dirs:
                p.parent.mkdir(parents=True, exist_ok=True)
                dirs.add(p.parent)
            os.link(BLOBS / key, p)
    except FileNotFoundError:  # (a blob gone under the build: missing, sent again, never a 500; B6's sibling, 05:3x "tree: 500")
        shutil.rmtree(tmp, ignore_errors=True)
        return {"ok": False, "missing": have(sorted(set(files.values()))) or sorted(set(files.values()))}
    for path, target in links.items():
        p = tmp / path
        p.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(target, p)
    (tmp / ".sc-ready").write_text(tid)
    if dst.exists():  # (a half-made one from an agent that died: never one that's ready, which another batch may use)
        shutil.rmtree(dst, ignore_errors=True)
    try:
        tmp.rename(dst)
    except OSError:
        shutil.rmtree(tmp, ignore_errors=True)
        if not (dst / ".sc-ready").exists():
            raise
    gc_trees(keep={tid})
    return {"ok": True, "seconds": round(time.time() - t0, 2), "files": len(files)}


# gdp: law a job runs on a tree only when each file the manifest names a git blob for is that blob: not result or bad == 0
def tree_verified(bad: int) -> bool:
    """A batch's tree on this VM, its files checked against the git blob ids its client's manifest gives (a file's
    bytes hashed as git hashes a blob): any other, and no job runs there (11-02: a committed change shipped as main's
    old content, the client's key for it wrong)."""
    return bad == 0


# gdp: law a tree that isn't its manifest is never kept: not result or bad == 0
def tree_kept(bad: int) -> bool:
    """verify_tree: a tree some of whose files aren't the blobs named (or aren't there) is removed, and the batches
    prepared on it unprepared, so the next preparation makes it again from the store (a blob missing, sent again).
    Before: it stayed, ready, and every batch of its id on the VM failed (11-02 12:45, bx_ptwy4b7p: tree
    9810b283e4ba made 11-01 11:11 lacked 5 files of its manifest, README.md and core/cli.ts among them, the VM lost at
    15:30 and resumed from its snapshot today; its ready mark trusted)."""
    return bad == 0


def verify_tree(tid: str, gits: dict) -> dict:
    """The files of ~/.sc/trees/<tid> that aren't the git blobs the manifest names (path: blob id); such a tree dropped
    (tree_kept), "partial" when every bad one is only missing here (the VM's tree, not the client's keys)."""
    bad = []
    for path, blob in (gits or {}).items():
        p = TREES / tid / path
        try:
            data = p.read_bytes()
        except OSError:
            bad.append([path, blob, "missing"])
            continue
        got = hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()
        if got != blob:
            bad.append([path, blob, got])
    out = {"ok": tree_verified(len(bad)), "checked": len(gits or {}), "bad": bad[:20]}
    if not tree_kept(len(bad)):
        t = TREES / tid
        for b in (ROOT / "b").glob("*") if (ROOT / "b").exists() else []:
            with _quiet():
                if (b / "treepath").exists() and pathlib.Path((b / "treepath").read_text()) == t:
                    (b / "ready").unlink(missing_ok=True)  # (prepared again: the tree made again first)
        with _tree_lock(tid):
            shutil.rmtree(t, ignore_errors=True)
        out.update(dropped=True, partial=all(x[2] == "missing" for x in bad))
    return out


def tree_id(files: dict, links: dict) -> str:
    """A tree's id: its files' keys and its links' targets, by path (the client computes the same)."""
    return hashlib.sha256(json.dumps([sorted(files.items()), sorted(links.items())], separators=(",", ":")).encode()).hexdigest()[:24]


def ingest(tree: pathlib.Path, tid: str):
    """A tree unpacked from a tarball, into the blob store and ~/.sc/trees/<id> (so the next batch syncs by hash),
    when its content is what the id says."""
    BLOBS.mkdir(parents=True, exist_ok=True)
    files, links = {}, {}
    for p in sorted(tree.rglob("*")):
        rel = str(p.relative_to(tree))
        if p.is_symlink():
            links[rel] = os.readlink(p)
        elif p.is_file():
            k = file_key(p)
            files[rel] = k
            if not (BLOBS / k).exists():
                tmp = BLOBS / f".{k}.{os.getpid()}"
                shutil.copy2(p, tmp)
                os.chmod(tmp, 0o755 if k.endswith("x") else 0o644)
                tmp.rename(BLOBS / k)
    if tree_id(files, links) == tid:
        make_tree(tid, files, links)


def prepared_trees() -> set[str]:
    """The trees of the batches prepared here (their directory ready): kept while the batch is (drop_old_batches lets a
    quiet one go)."""
    out = set()
    for b in (ROOT / "b").glob("*") if (ROOT / "b").exists() else []:
        with _quiet():
            if (b / "ready").exists() and (b / "treepath").exists():
                out.add(pathlib.Path((b / "treepath").read_text()).name)
    return out


def in_use_trees() -> set[str]:
    used = set()
    for b in (ROOT / "b").glob("*") if (ROOT / "b").exists() else []:
        t = b / "treepath"
        if t.exists() and any(alive(int(j.read_text())) for j in b.glob("jobs/*/pid") if j.read_text().strip().isdigit() and not (j.parent / "done.json").exists()):
            used.add(pathlib.Path(t.read_text()).name)
    return used


def gc_trees(keep: set[str] = frozenset(), n: int = 6):
    """The newest n trees stay, and any a running item uses; blobs no tree links go once they're a day old."""
    if not TREES.exists():
        return
    keep = set(keep) | in_use_trees()
    trees = sorted([p for p in TREES.iterdir() if not p.name.startswith(".")], key=lambda p: -p.stat().st_mtime)
    store = BlobStore()
    store.building = sum(1 for p in TREES.iterdir() if p.name.startswith("."))  # (trees being made, by any process)
    for rank, p in enumerate(trees):
        if store.drop_tree(rank, n, p.name in keep, p.name in prepared_trees()):
            shutil.rmtree(p, ignore_errors=True)
    if BLOBS.exists():
        now = time.time()
        for b in BLOBS.iterdir():
            with _quiet():
                st = b.stat()
                if store.drop_blob(st.st_nlink, int(now - st.st_mtime), len(trees) > n):
                    b.unlink()


class _quiet:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return True


# ----------------------------------------------------------------------------------------------------------------- prep
def fetch(source: str, dst: pathlib.Path, jar: pathlib.Path) -> str:
    """A file from a boat host URL (its token sets a cookie on the first response, then redirects) or a local path."""
    if source.startswith("https://"):
        # (HTTP/1.1: many VMs at once through boat's HTTP/2 front broke streams)
        p = subprocess.run(["curl", "-sSfL", "--http1.1", "--retry", "5", "--retry-all-errors", "--retry-delay", "1", "-c", str(jar), "-b", str(jar), "-o", str(dst), source],
                           capture_output=True, text=True, timeout=300)
        return "" if p.returncode == 0 else "download failed: " + p.stderr[-300:]
    shutil.move(source, dst)
    return ""


def cache_dir(name: str, key: str) -> pathlib.Path:
    return ROOT / "cache" / name / key


def cachein(name: str, key: str, tar: str) -> dict:
    """A build cache's files for a key, into this VM's (each written whole, then renamed in)."""
    d = cache_dir(name, key)
    d.mkdir(parents=True, exist_ok=True)
    got = []
    with tarfile.open(tar) as tf:
        for m in tf.getmembers():
            n = pathlib.Path(m.name).name
            if m.isfile() and CacheFiles().take_in((d / n).exists(), n.startswith(".")):
                (d / f".{n}.in").write_bytes(tf.extractfile(m).read())
                (d / f".{n}.in").rename(d / n)
                got.append(n)
    pathlib.Path(tar).unlink(missing_ok=True)
    return {"ok": True, "got": got}


def cache_names(name: str, key: str) -> list[str]:
    d = cache_dir(name, key)
    return sorted(f.name for f in d.iterdir() if not f.name.startswith(".")) if d.exists() else []


def drop_old_batches(batch: str, keep_s: float = 1800):
    # other batches' directories go once nothing of theirs runs and they've been quiet keep_s (the scheduler runs
    # several batches on one VM at once; a client's batch had the VM alone, and a late call of an older one keeps its tree)
    now, rule = time.time(), BatchDirs(int(keep_s))
    for old in (ROOT / "b").glob("*") if (ROOT / "b").exists() else []:
        try:
            quiet_s = int(now - max(f.stat().st_mtime for f in [old, *old.glob("jobs/*")]))
        except Exception:
            quiet_s = int(keep_s) + 1
        live = any(alive(int(j.read_text())) for j in old.glob("jobs/*/pid") if j.read_text().strip().isdigit() and not (j.parent / "done.json").exists())
        if rule.drop(old.name == batch, quiet_s, live):  # (BatchDirs.drop)
            sudo("rm", "-rf", str(old), check=False)


SCRATCH = [pathlib.Path(HOME) / ".foundry" / "anvil" / "tmp"]  # (known scratch jobs leave in the home: anvil's state, killed before its own cleanup)
_cleaning = {"t": 0.0, "on": False}


def clean_disk(force: bool = False) -> dict:
    """The disk as vtick reports it; with it low, a cleaning started (in the background, once a minute at most): see
    _clean_disk."""
    d = disk()
    if _cleaning["on"] or not (force or (disk_low(d["free_mb"], d["total_mb"]) and time.time() - _cleaning["t"] >= 60)):
        return {**d, "cleaning": _cleaning["on"]}
    _cleaning.update(t=time.time(), on=True)
    threading.Thread(target=_clean_disk, daemon=True).start()
    return {**d, "cleaning": True}


def _clean_disk():
    """The known scratch older than every running job (scratch_drops), the leftovers of jobs that ended (their /work,
    /tmp and overlays: a run killed before its own cleanup), stale trees (the newest two and those in use stay) and
    quiet batches' directories (10 min)."""
    try:
        _clean_disk_once()
    finally:
        _cleaning.update(t=time.time(), on=False)


def _clean_disk_once():
    oldest = int(time.time())
    for j in (ROOT / "b").glob("*/jobs/*") if (ROOT / "b").exists() else []:
        with _quiet():
            pid = (j / "pid").read_text().strip() if (j / "pid").exists() else ""
            live = pid.isdigit() and alive(int(pid)) and not (j / "done.json").exists() and not (j / "acked.json").exists()
            if live:
                oldest = min(oldest, int((j / "spec.json").stat().st_mtime))
            elif (j / "done.json").exists() or (j / "acked.json").exists() or (pid.isdigit() and not alive(int(pid))):
                gone = [str(x) for x in (j / "work", j / "tmp", *j.glob("ov*")) if x.exists()]
                if gone:
                    sudo("rm", "-rf", *gone, check=False)
    for sd in SCRATCH:
        for e in sd.iterdir() if sd.exists() else []:
            with _quiet():
                if scratch_drops(True, int(e.lstat().st_mtime), oldest):
                    sudo("rm", "-rf", str(e), check=False)
    with _quiet():
        gc_trees(n=2)
    with _quiet():
        drop_old_batches("", keep_s=600)


def prep(batch: str, source: str, ctx_b64: str = "", tid: str = "", sha: str = "", csrc_b64: str = "-") -> dict:
    """The batch's directory and tree: from ~/.sc/trees/<id> (`tree:<id>`, synced by the warm agent), this VM's tarball
    cache (`cache:<sha>`), a URL (the seed VM's) or a file uploaded here. The context names the mount points and the
    build caches; the answer says which cache files this VM has for their keys."""
    t0 = time.time()
    ctx = json.loads(base64.b64decode(ctx_b64)) if ctx_b64 and ctx_b64 != "-" else {}
    drop_old_batches(batch)
    d = batch_dir(batch)
    if (d / "ready").exists():
        return {"ok": True, "again": True, "caches": {c["name"]: cache_names(c["name"], c["key"]) for c in ctx.get("caches", [])}, "cpus": os.cpu_count()}
    name = ctx.get("name", "repo")
    sudo("mkdir", "-p", "/work", f"/src/{name}", *ctx.get("mounts", []))
    (d / "jobs").mkdir(parents=True, exist_ok=True)
    write_json(d / "ctx.json", ctx)
    if source.startswith("tree:"):
        tree = TREES / source[5:]
        try:
            os.utime(tree)
            ready = (tree / ".sc-ready").exists()
        except FileNotFoundError:  # (collected between its check and its touch: B6)
            ready = False
        if not ready:
            return {"ok": False, "err": "that tree isn't on this VM"}
    else:
        tree = d / "tree"
        shutil.rmtree(tree, ignore_errors=True)
        tree.mkdir(parents=True)
        tar = d / "tree.tar.gz"
        trees = ROOT / "tarballs"  # (the tarballs this VM unpacked lately, by hash: the same tree again needs no upload)
        if source.startswith("cache:"):
            if not (trees / f"{source[6:]}.tar.gz").exists():
                return {"ok": False, "err": "not in this VM's tree cache"}
            shutil.copy(trees / f"{source[6:]}.tar.gz", tar)
        else:
            err = fetch(source, tar, d / "cookies")
            if err:
                return {"ok": False, "err": err}
        with tarfile.open(tar) as tf:
            tf.extractall(tree, filter="tar")
        if sha and not source.startswith("cache:"):
            trees.mkdir(parents=True, exist_ok=True)
            shutil.move(tar, trees / f"{sha}.tar.gz")
            for rank, old in enumerate(sorted(trees.glob("*.tar.gz"), key=lambda p: -p.stat().st_mtime)):
                if not Tarballs(4).stays(rank):
                    old.unlink(missing_ok=True)
        else:
            tar.unlink(missing_ok=True)
        if tid:  # (the tree into the blob store, in the background: the next batch on this VM syncs by hash)
            subprocess.Popen([sys.executable, str(ROOT / "agent.py"), "ingest", str(tree), tid], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, start_new_session=True)
    # the build caches the client has for their keys, from the seed VM's URL, when this VM lacks some of their files
    csrc = json.loads(base64.b64decode(csrc_b64)) if csrc_b64 and csrc_b64 != "-" else {}
    for c in ctx.get("caches", []):
        url, names = csrc.get(c["name"], [None, []])
        if url and set(names) - set(cache_names(c["name"], c["key"])):
            tmp = d / f"cache-{c['name']}.tar.gz"
            if not fetch(url, tmp, d / "cookies"):
                cachein(c["name"], c["key"], str(tmp))
    (d / "treepath").write_text(str(tree))
    (d / "ready").write_text(str(time.time()))
    return {"ok": True, "seconds": round(time.time() - t0, 2), "caches": {c["name"]: cache_names(c["name"], c["key"]) for c in ctx.get("caches", [])}, "cpus": os.cpu_count()}


def serve(directory: str, port: int) -> dict:
    """A file server for the other VMs (behind boat's token-gated URL), detached."""
    subprocess.Popen(["python3", "-m", "http.server", str(port), "--bind", "127.0.0.1", "--directory", directory],
                     stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    return {"ok": True}


# ----------------------------------------------------------------------------------------------------------------- tick
def cpus_in_use(d: pathlib.Path) -> set[int]:
    used = set()
    for j in [j for b in (ROOT / "b").iterdir() if (b / "jobs").exists() for j in (b / "jobs").iterdir()]:
        if (j / "spec.json").exists() and not (j / "done.json").exists() and not (j / "acked.json").exists():
            try:
                used |= set(json.loads((j / "cpus").read_text()))
            except Exception:
                pass
    return used


_children: list = []
_start_lock = threading.Lock()
_owned_children: dict = {}  # retained until reap evidence has been durably recorded


class OwnedCancellation:
    """The shipped cancellation decision. I/O supplies evidence, never a PID's apparent absence.

    A never-launched item has no child to reap; reaped is vacuously true only in that case.
    Only a verified private PID namespace and its owned supervisor wait prove descendant teardown.
    """

    def __init__(self):
        self.cancelled: bool = False
        self.reaped: bool = False
        self.namespace_absent: bool = False
        self.ok: bool = False

    # gdp: law a cancellation fences subsequent starts: cancelled == True
    def cancel(self) -> None:
        self.cancelled = True

    # gdp: law a cancelled item cannot start: not result or not cancelled
    def may_start(self) -> bool:
        return not self.cancelled

    # gdp: law success requires reap and namespace evidence: not ok or (reaped and namespace_absent)
    # gdp: law a launched namespace requires containment and wait evidence: not launched or not namespace_absent or (contained and waited)
    # gdp: law a launched item is reaped only on owned wait evidence: not launched or not reaped or waited
    def observe(self, launched: bool, waited: bool, never_started: bool, contained: bool) -> None:
        self.reaped = waited if launched else never_started
        self.namespace_absent = (contained and waited) if launched else never_started
        self.ok = self.cancelled and self.reaped and self.namespace_absent


def _item_ids(batch: str, name: str):
    for value in (batch, name):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.~-]{0,199}", value):
            raise ValueError("batch and name must be single safe identifiers (1..200 characters)")


def _owned_path(batch: str, name: str) -> pathlib.Path:
    # Outside b/: drop_old_batches must never erase a cancellation fence or its evidence.
    return ROOT / "owned-items" / batch / (name + ".json")


def _owned_write(path: pathlib.Path, row: dict):
    """Atomic and fsynced, including directory metadata; no power-loss promise beyond the filesystem."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")  # callers hold both ownership locks
    with open(tmp, "w") as f:
        json.dump(row, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    # Include newly created batch/ownership directories, not only the renamed file entry.
    for directory in (path.parent, path.parent.parent, ROOT):
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


@contextlib.contextmanager
def _ownership_lock(deadline=None):
    """Serialize start/publication/cancel/reap, including overlapping CLI and daemon agents."""
    import fcntl
    acquired = _start_lock.acquire() if deadline is None else _start_lock.acquire(timeout=max(0, deadline - time.monotonic()))
    if not acquired:
        raise TimeoutError("start/publication lock timed out")
    try:
        ROOT.mkdir(parents=True, exist_ok=True)
        with open(ROOT / "owned-items.lock", "a") as f:
            while True:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise TimeoutError("ownership file lock timed out")
                    time.sleep(0.01)
            try:
                yield
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)
    finally:
        _start_lock.release()


def _process_identity(pid: int) -> dict:
    """Linux boot and start ticks disambiguate PID reuse; failures are unknown, never absence."""
    fields = pathlib.Path(f"/proc/{pid}/stat").read_text().rsplit(") ", 1)[1].split()
    return {"pid": pid, "boot": pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "start_ticks": int(fields[19])}


def _owned_receipt(batch, name, row, error=""):
    rule = OwnedCancellation()
    if row.get("cancelled"):
        rule.cancel()
    rule.observe(row.get("state") != "never_started", bool(row.get("reaped")),
                 row.get("state") == "never_started", bool(row.get("contained")))
    return {"ok": rule.ok, "reaped": rule.reaped, "namespace_absent": rule.namespace_absent,
            "identity": row.get("identity"), "batch": batch, "name": name,
            "error": error or ("" if rule.ok else "namespace absence unproven (unsupported containment or unknown ownership)")}


def _launch_nonce(value):
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,200}", value):
        raise ValueError("launch_identity must be a nonempty safe nonce, at most 200 characters")
    return value


def _namespace_birth(pid):
    birth = _process_identity(pid)
    status = pathlib.Path(f"/proc/{pid}/status").read_text().splitlines()
    nspid = next(line.split()[1:] for line in status if line.startswith("NSpid:"))
    uids = next(line.split()[1:] for line in status if line.startswith("Uid:"))
    if len(nspid) < 2 or nspid[-1] != "1" or any(int(uid) != os.getuid() for uid in uids):
        raise ValueError("not an owned private namespace init")
    birth["pidns"] = os.stat(f"/proc/{pid}/ns/pid").st_ino
    if birth["pidns"] == os.stat("/proc/self/ns/pid").st_ino:
        raise ValueError("init is in the agent's namespace")
    return birth


def _bind_namespace(j, entry):
    """Open a pidfd only for the handshake's PID1, still descended from our exact Popen.

    Checking before and after open binds the descriptor to birth ticks, boot, inode, uid and
    ancestry; no name-based signaling and no absence inferred from a /proc scan.
    """
    hello = json.loads((j / "namespace.json").read_text())
    if hello["identity"] != entry["identity"] or hello["batch"] != j.parent.parent.name or hello["name"] != j.name:
        raise ValueError("namespace handshake identity mismatch")
    p = entry["p"]
    if _process_identity(p.pid) != entry["birth"]:
        raise ValueError("supervisor identity changed")
    for pid in descendants(p.pid):
        try:
            before = _namespace_birth(pid)
            if before["start_ticks"] != hello["start_ticks"] or before["pidns"] != hello["pidns"]:
                continue
            fd = os.pidfd_open(pid, 0)
            try:
                if _namespace_birth(pid) != before or pid not in descendants(p.pid):
                    raise ValueError("init identity changed during pidfd open")
                signal.pidfd_send_signal(fd, 0)  # verify signal permission before authorizing payload
                entry.update(pidfd=fd, init=before, contained=True)
                return
            except BaseException:
                os.close(fd)
                raise
        except (OSError, ValueError, StopIteration):
            continue
    raise ValueError("owned namespace init not found")


def _namespace_exited(entry, wait_seconds=0):
    import select
    if entry.get("pidfd") is None:
        return False
    poller = select.poll()
    poller.register(entry["pidfd"], select.POLLIN)
    return any(events & select.POLLIN for _, events in poller.poll(max(0, math.ceil(wait_seconds * 1000))))


def _finish_owned(key, entry, row, code):
    row.update(reaped=True, returncode=code, contained=bool(entry.get("contained") and _namespace_exited(entry)))
    _owned_write(_owned_path(*key), row)  # persist proof before retiring the only Popen handle
    if entry.get("pidfd") is not None:
        os.close(entry["pidfd"])
    _owned_children.pop(key, None)
    if entry["p"] in _children:
        _children.remove(entry["p"])


def cancelitem(batch: str, name: str, wait_seconds=10, expected_identity=None) -> dict:
    """Fence the launch, SIGKILL verified PID1 via pidfd, then wait the owned supervisor.

    Linux's PID-namespace init death kills all tasks in that namespace (including nested PID
    namespaces). unshare --fork waits that init, sudo waits unshare, and Popen.wait reaps sudo.
    A pidfd POLLIN observation on the verified init is also mandatory: killing sudo alone
    must not let an early wrapper exit masquerade as namespace teardown. These two barriers,
    not a Python runner's apparent death or a broad /proc absence scan, establish the receipt.
    No claims about host processes outside this namespace, namespace handles, or hostile root
    escapes. Jobs are trusted same-user workloads, not a sandbox against malicious VM root.
    On restart non-child identities are never signaled; only persisted positive evidence survives.
    """
    _item_ids(batch, name)
    if expected_identity is not None:
        _launch_nonce(expected_identity)
    wait_seconds = float(wait_seconds)
    if not math.isfinite(wait_seconds) or not 0 <= wait_seconds <= 60:
        raise ValueError("wait_seconds must be finite and between 0 and 60")
    deadline = time.monotonic() + wait_seconds
    key, path, row = (batch, name), _owned_path(batch, name), {}
    try:
        with _ownership_lock(deadline):
            if path.exists():
                row = json.loads(path.read_text())
            else:
                j = batch_dir(batch) / "jobs" / name
                if expected_identity is None or j.exists():
                    # No strict ownership record: an optional nonce cannot upgrade a legacy
                    # runner (or its missing/cleaned-up PID) into positive ownership evidence.
                    return _owned_receipt(batch, name, {}, "no strict ownership record; legacy/unknown launch refused")
                row = {"identity": expected_identity, "state": "never_started", "reaped": False}
            if expected_identity is not None and row.get("identity") != expected_identity:
                # Do not fence, signal, or return success for another launch's receipt.
                out = _owned_receipt(batch, name, row, "launch identity mismatch")
                out.update(ok=False, reaped=False, namespace_absent=False)
                return out
            row["cancelled"] = True
            _owned_write(path, row)
            entry = _owned_children.get(key)
            if entry is not None:
                if entry["identity"] != row.get("identity"):
                    return _owned_receipt(batch, name, {}, "live ownership identity mismatch")
                p = entry["p"]
                code = p.poll()
                if entry.get("contained") and entry.get("pidfd") is not None:
                    try:
                        signal.pidfd_send_signal(entry["pidfd"], signal.SIGKILL)
                    except ProcessLookupError:
                        pass  # exact pidfd task exited naturally; require both barriers below
                    if not _namespace_exited(entry, max(0, deadline - time.monotonic())):
                        return _owned_receipt(batch, name, row, "kill requested; namespace init exit timed out")
                elif code is None:
                    return _owned_receipt(batch, name, row, "no verified private namespace; not signaled")
                try:
                    code = p.wait(timeout=max(0, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    return _owned_receipt(batch, name, row, "kill requested; owned supervisor wait timed out")
                _finish_owned(key, entry, row, code)
            return _owned_receipt(batch, name, row)
    except (OSError, ValueError, KeyError, TypeError) as e:
        # Failure to read durable evidence, signal, wait or persist is NOT a successful receipt.
        return _owned_receipt(batch, name, {}, str(e))


def _reap_children():
    if _owned_children:
        with _ownership_lock():
            for key, entry in list(_owned_children.items()):
                code = entry["p"].poll()
                if code is None or (entry.get("contained") and not _namespace_exited(entry)):
                    continue
                path = _owned_path(*key)
                row = json.loads(path.read_text())
                if row.get("identity") == entry["identity"]:
                    _finish_owned(key, entry, row, code)
    # The original legacy reaper: no ownership filesystem operations for legacy-only work.
    owned = [entry["p"] for entry in _owned_children.values()]
    for p in list(_children):
        if p not in owned and p.poll() is not None:
            _children.remove(p)


def _owned_argv(batch, spec):
    return ["sudo", "-n", "unshare", "--mount", "--propagation", "private", "--pid", "--fork",
            "--kill-child=KILL", "--mount-proc", "--", "setpriv", f"--reuid={os.getuid()}",
            f"--regid={os.getgid()}", "--init-groups", "--", sys.executable, str(ROOT / "agent.py"),
            "ownedrun", batch, spec["name"], str(ROOT), spec["launch_identity"]]


def ownedrun(batch, name, root, identity):
    """Internal namespace PID1. No payload until the parent verifies and opens our pidfd."""
    global ROOT
    ROOT = pathlib.Path(root)
    _item_ids(batch, name)
    _launch_nonce(identity)
    if os.getpid() != 1:
        raise RuntimeError("ownedrun requires private PID namespace PID1")
    j = batch_dir(batch) / "jobs" / name
    hello = {"batch": batch, "name": name, "identity": identity,
             "pidns": os.stat("/proc/self/ns/pid").st_ino,
             "start_ticks": _process_identity(1)["start_ticks"]}
    write_json(j / "namespace.json", hello)
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        row = json.loads(_owned_path(batch, name).read_text())
        if row.get("identity") != identity or row.get("cancelled"):
            return
        go = j / "namespace.go"
        if go.exists() and go.read_text() == identity:
            run(batch, name, identity)
            return  # exiting PID1 also tears down background/double-fork descendants on natural completion
        time.sleep(0.01)
    raise TimeoutError("namespace supervisor never authorized payload")


def _legacy_start(d: pathlib.Path, spec: dict):
    """Original non-opt-in launch path, including retry names and asynchronous publication."""
    j = d / "jobs" / spec["name"]
    with _start_lock:
        if j.exists():
            return
        j.mkdir(parents=True)
        n = os.cpu_count() or 1
        want = min(n, max(1, int(spec.get("cpus", 2))))
        used = cpus_in_use(d)
        free = [c for c in range(n) if c not in used]
        grant = CpuGrant(n)
        grant.held = min(n, len(used))
        k = grant.grant(want)
        cpus = free[:k] if k > 0 else list(range(n))[:want]
        (j / "cpus").write_text(json.dumps(cpus if k > 0 else []))
        (j / "taskset").write_text(json.dumps(cpus))
        write_json(j / "spec.json", spec)
    try:
        with open(j / "agent.err", "w") as err:
            p = subprocess.Popen([sys.executable, str(ROOT / "agent.py"), "run", d.name, spec["name"]], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=err, start_new_session=True, close_fds=True)
    except Exception as e:
        write_json(j / "done.json", {"name": spec["name"], "code": -1, "seconds": -1, "lost": True, "err": f"its process didn't start: {e!r}"})
        return
    (j / "pid").write_text(str(p.pid))
    _children.append(p)


def start(d: pathlib.Path, spec: dict):
    if spec.get("strict_once") is not True:
        with _rpc_gate_lock, _ownership_lock():
            if (ROOT / "owned-upgrading").exists():
                raise ValueError("admission fenced for agent replacement")
            return _legacy_start(d, spec)
    _item_ids(d.name, spec["name"])
    strict = True
    identity = _launch_nonce(spec["launch_identity"]) if strict else secrets.token_hex(16)
    j, path = d / "jobs" / spec["name"], _owned_path(d.name, spec["name"])
    with _ownership_lock():
        if (ROOT / "owned-upgrading").exists():
            raise ValueError("owned admission fenced for agent replacement")
        if path.exists():
            previous = json.loads(path.read_text())
            # Check identity BEFORE cancellation/state: neither old success nor a fence
            # for another launch may silently admit this request.
            if previous.get("identity") != identity:
                raise ValueError("strict ownership collision: launch identity mismatch")
            if j.exists():
                stored = json.loads((j / "spec.json").read_text())
                if stored != spec or _strict_job_identity(j) != identity:
                    raise ValueError("strict job collision: specification mismatch")
                if (previous.get("state") == "launched" and previous.get("contained") is True
                        and previous.get("init") and previous.get("supervisor")
                        and (j / "namespace.go").read_text() == identity):
                    return  # exact, durably authorized strict admission; no replacement
                raise ValueError("strict job collision: prior launch not proven authorized")
            if previous.get("cancelled") and previous.get("state") == "never_started":
                raise ValueError("strict admission closed: item was cancelled before launch")
            raise ValueError("strict ownership collision: missing job or uncertain prior launch")
        if j.exists():
            raise ValueError("strict job collision: existing job has no ownership record")
        j.mkdir(parents=True)
        # Strict children must not widen an enclosing allocation through their taskset.
        # A production daemon normally sees the VM's whole CPU set; nested verification
        # sees only its reserved CPUs. CPU IDs need not be contiguous or start at zero.
        allowed = sorted(os.sched_getaffinity(0))
        n = len(allowed)
        want = min(n, max(1, int(spec.get("cpus", 2))))
        used = cpus_in_use(d) & set(allowed)
        free = [c for c in allowed if c not in used]
        grant = CpuGrant(n)
        grant.held = min(n, len(used))
        k = grant.grant(want)
        cpus = free[:k] if k > 0 else allowed[:want]
        (j / "cpus").write_text(json.dumps(cpus if k > 0 else []))
        (j / "taskset").write_text(json.dumps(cpus))
        write_json(j / "spec.json", spec)
        row = {"identity": identity, "state": "launching", "reaped": False, "contained": False}
        _owned_write(path, row)  # crash here is unknown, never proof that no child launched
        try:
            argv = _owned_argv(d.name, spec) if strict else [sys.executable, str(ROOT / "agent.py"), "run", d.name, spec["name"]]
            if strict and (not hasattr(os, "pidfd_open") or not hasattr(signal, "pidfd_send_signal")):
                raise RuntimeError("strict ownership requires Linux pidfd APIs")
            with open(j / "agent.err", "w") as err:
                p = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                     stderr=err, start_new_session=True, close_fds=True)
        except Exception as e:
            row.update(state="never_started")
            _owned_write(path, row)
            write_json(j / "done.json", {"name": spec["name"], "code": -1, "seconds": -1,
                                         "lost": True, "launch_identity": identity, "owned_unsupported": strict, "err": repr(e)})
            return
        entry = {"p": p, "identity": identity, "contained": False}
        _owned_children[(d.name, spec["name"])] = entry
        _children.append(p)
        (j / "pid").write_text(str(p.pid))
        row["state"] = "launched"
        _owned_write(path, row)
        try:
            entry["birth"] = _process_identity(p.pid)
            if strict:
                deadline = time.monotonic() + 5
                while p.poll() is None and time.monotonic() < deadline:
                    try:
                        _bind_namespace(j, entry)
                        break
                    except (OSError, ValueError, KeyError, StopIteration):
                        time.sleep(0.01)
                if entry.get("contained"):
                    row.update(contained=True, init=entry["init"], supervisor=entry["birth"])
                    _owned_write(path, row)
                    (j / "namespace.go").write_text(identity)
                else:
                    row["error"] = "private namespace handshake unsupported or timed out; payload not authorized"
                    _owned_write(path, row)
                    write_json(j / "done.json", {"name": spec["name"], "code": -1, "seconds": -1,
                                                 "lost": True, "launch_identity": identity, "err": row["error"]})
        except (OSError, ValueError, IndexError) as e:
            row["error"] = f"supervisor identity unavailable: {e!r}"
            _owned_write(path, row)
            write_json(j / "done.json", {"name": spec["name"], "code": -1, "seconds": -1,
                                         "lost": True, "launch_identity": identity, "err": row["error"]})


STARTING_S = 60  # an item with no process this long after its start is lost


def _strict_job_identity(j):
    """Provenance from the admission, never from a claimed terminal row.

    Ownership without its original strict spec (or vice versa) is uncertain. A
    legacy job has neither. This does not upgrade any pre-existing done.json.
    """
    path = _owned_path(j.parent.parent.name, j.name)
    spec = json.loads((j / "spec.json").read_text()) if (j / "spec.json").exists() else {}
    if spec.get("strict_once") is not True:
        if path.exists():
            raise ValueError("strict ownership without its strict specification")
        return None
    identity = _launch_nonce(spec["launch_identity"])
    if spec.get("name") != j.name:
        raise ValueError("strict specification/job name mismatch")
    record = json.loads(path.read_text())
    if record.get("identity") != identity or record.get("state") not in ("launching", "launched", "never_started"):
        raise ValueError("strict specification/ownership identity mismatch")
    return identity


def _ack_job(j, expected_identity=None):
    """Only an exact identity-bound strict result can be retired; legacy is unchanged."""
    if not Ticks().ack((j / "done.json").exists()):
        return
    try:
        identity = _strict_job_identity(j)
        row = json.loads((j / "done.json").read_text()) if identity is not None else {}
        if identity is not None and (expected_identity != identity or row.get("launch_identity") != identity
                                     or row.get("name") != j.name):
            return
        if identity is None and expected_identity is not None:
            return  # an identity-bearing ack cannot upgrade/retire an old legacy result
        (j / "done.json").rename(j / "acked.json")
    except (OSError, ValueError, KeyError, TypeError):
        return  # unknown evidence stays visible and unacknowledged


def report(d: pathlib.Path) -> tuple[list, list]:
    """Each item of the batch as JobReport reads its files: done, running, or lost (its process gone without a word,
    or none STARTING_S after its start)."""
    done, running = [], []
    for j in sorted((d / "jobs").iterdir()):
        rule = JobReport(STARTING_S)
        row = None
        identity, identity_error = None, ""
        try:
            identity = _strict_job_identity(j)
        except (OSError, ValueError, KeyError, TypeError) as e:
            identity_error = str(e)
        dj = j / "done.json"
        if dj.exists():
            try:
                row = json.loads(dj.read_text())
            except Exception:
                row = None
        pid = (j / "pid").read_text().strip() if (j / "pid").exists() else ""
        live = bool(pid.isdigit()) and alive(int(pid))
        try:
            age = int(time.time() - j.stat().st_mtime)
        except FileNotFoundError:
            age = 0
        what = rule.read((j / "acked.json").exists(), dj.exists(), row is not None, bool(pid.isdigit()), live, age)
        if what == "lost":
            time.sleep(0.2)  # (the runner's done.json may be landing)
            if not dj.exists():
                err = (j / "agent.err").read_text()[-2000:] if (j / "agent.err").exists() else ""
                lost = {"name": j.name, "code": -1, "seconds": -1, "lost": True, "err": err or "its process gone without a word"}
                if identity is not None:
                    lost["launch_identity"] = identity  # a NEW failure, proven from this admission
                write_json(dj, lost)
            what = "done"
            try:
                row = json.loads(dj.read_text())
            except Exception:
                what = "torn"
        if what == "done":
            if (identity_error or (identity is None and "launch_identity" in row)
                    or (identity is not None and (row.get("launch_identity") != identity or row.get("name") != j.name))):
                # Never relabel old/legacy terminal evidence with the current nonce. An
                # untrusted row must not release or acknowledge a strict scheduler hold.
                row = dict(row)
                row.pop("launch_identity", None)
                row.update(identity_untrusted=True, identity_error=identity_error or "terminal launch identity mismatch")
            done.append(row)
        elif what in ("running", "torn"):
            running.append(j.name)
    return done, running


def tick(batch: str, msg: dict) -> dict:
    d = batch_dir(batch)
    if not (d / "jobs").exists():
        return {"done": [], "running": [], "load": os.getloadavg()[0], "err": "no such batch here"}
    for ack in msg.get("acks", []):
        name = ack["name"] if isinstance(ack, dict) else ack
        _item_ids(batch, name)
        if isinstance(ack, dict) and ack.get("batch", batch) != batch:
            continue
        _ack_job(d / "jobs" / name, ack.get("launch_identity") if isinstance(ack, dict) else None)
    for spec in msg.get("start", []):
        start(d, spec)
    wait = float(msg.get("wait", 25))
    t0 = time.time()
    while True:
        done, running = report(d)
        if Ticks().stop(bool(done), bool(running), int(time.time() - t0), int(wait)):
            break
        time.sleep(0.1)
    return {"done": done, "running": running, "load": os.getloadavg()[0]}


def kill(batch: str) -> dict:
    d = batch_dir(batch)
    n = 0
    if (d / "jobs").exists():
        for j in (d / "jobs").iterdir():
            try:
                if _strict_job_identity(j) is not None:
                    continue  # strict jobs require the identity-bound cancelitem API
            except (OSError, ValueError, KeyError, TypeError):
                continue  # unknown ownership is not permission to signal a recovered PID
            pid = (j / "pid").read_text().strip() if (j / "pid").exists() else ""  # (a pid file being written: empty, no process yet)
            if Kill().kills(pid.isdigit(), (j / "done.json").exists() or (j / "acked.json").exists()):
                kill_tree(int(pid))
                n += 1
    return {"killed": n}


def kill_item(batch: str, name: str) -> dict:
    """One item stopped (the scheduler takes its slot back for a higher class): its process tree killed, reported as
    preempted (the scheduler queues it again)."""
    j = batch_dir(batch) / "jobs" / name
    try:
        if _strict_job_identity(j) is not None:
            return {"killed": 0, "error": "strict launch requires identity-bound cancelitem"}
    except (OSError, ValueError, KeyError, TypeError):
        return {"killed": 0, "error": "unknown strict ownership; not signaled"}
    pid = (j / "pid").read_text().strip() if (j / "pid").exists() else ""  # (a pid file being written: empty, no process yet)
    if Kill().kills(pid.isdigit(), (j / "done.json").exists() or (j / "acked.json").exists()):
        kill_tree(int(pid))
        time.sleep(0.2)
        write_json(j / "done.json", {"name": name, "code": -1, "seconds": -1, "preempted": True})
        return {"killed": 1}
    return {"killed": 0}


def vtick(msg: dict) -> dict:
    """The scheduler's tick for a whole VM: starts and acks by batch, then a wait (<= msg.wait) for any finish; every
    batch's finished items, each with its batch."""
    for a in msg.get("acks", []):
        _item_ids(a["batch"], a["name"])
        j = batch_dir(a["batch"]) / "jobs" / a["name"]
        _ack_job(j, a.get("launch_identity"))
    for st in msg.get("start", []):
        d = batch_dir(st["batch"])
        tp = d / "treepath"
        ready = (d / "ready").exists() and (not tp.exists() or pathlib.Path(tp.read_text()).exists())  # (its tree here too)
        if not BatchDirs(0).may_start(ready):
            return {"err": f"batch {st['batch']} isn't prepared here", "batch": st["batch"]}
        start(d, st["spec"])
    wait, t0 = float(msg.get("wait", 20)), time.time()
    batches = [b for b in (ROOT / "b").iterdir()] if (ROOT / "b").exists() else []
    while True:
        done, running = [], []
        for d in batches:
            if (d / "jobs").exists():
                try:
                    dn, rn = report(d)
                except FileNotFoundError:  # (an old batch's directory dropped meanwhile)
                    continue
                done += [{**r, "batch": d.name} for r in dn]
                running += [{"batch": d.name, "name": n} for n in rn]
        if Ticks().stop(bool(done), bool(running), int(time.time() - t0), int(wait)):
            break
        time.sleep(0.1)
    dk = {}
    with _quiet():
        dk = clean_disk()  # (its free disk with each tick: the scheduler places nothing on a VM below its threshold)
    return {"done": done, "running": running, "load": os.getloadavg()[0], "cpus": os.cpu_count(), "model": cpu_model(), "disk": dk}


def cpu_model() -> str:
    try:
        return next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
    except Exception:
        return ""


# ------------------------------------------------------------------------------------------------------------------ run
def wrap(j: pathlib.Path, spec: dict, ctx: dict, work: pathlib.Path, tree: pathlib.Path, cwd: str, argv: list[str]) -> list[str]:
    tmp = j / "tmp"
    tmp.mkdir(exist_ok=True)
    src = f"/src/{ctx.get('name', 'repo')}"
    binds = [(str(tmp), "/tmp"), (str(work), "/work"), (str(tree), src)]
    binds += [(s, t) for t, s in spec.get("mounts", {}).items()]
    lines = ["set -e"] + [f"mount --bind {json.dumps(s)} {json.dumps(t)}" for s, t in binds]
    # a directory a tool writes into (as root on Modal, each in its own container): a private overlay per item
    # (its upper and work dirs on a tmpfs: a VM whose root is itself an overlay refuses an upper dir there)
    for k, o in enumerate(spec.get("overlays", [])):
        ov = j / f"ov{k}"
        ov.mkdir(parents=True, exist_ok=True)
        lines += [f"mount -t tmpfs tmpfs {ov}", f"mkdir -p {ov}/up {ov}/work", f"mount -t overlay overlay -o lowerdir={o},upperdir={ov}/up,workdir={ov}/work {o}"]
    lines.append(f"mount -o remount,bind,ro {src}")
    lines.append(f"cd {json.dumps(cwd)}")
    lines.append('exec "$@"')
    (j / "ns.sh").write_text("\n".join(lines) + "\n")
    path = ":".join([*spec.get("path", []), *[p.replace("$HOME", HOME) for p in ctx.get("path", [])], BASE_PATH])
    # (bun's install cache in the item's own /tmp: two `bun install`s at once in one shared cache failed, silently,
    # where each Modal container has a HOME of its own)
    env = ["env", "-i", f"PATH={path}", f"HOME={HOME}", "USER=user", "LOGNAME=user", "SHELL=/bin/bash", "LANG=C.UTF-8", "BUN_INSTALL_CACHE_DIR=/tmp/.bun-install-cache",
           *[f"{k}={v}" for k, v in {**ctx.get("env", {}), **spec.get("env", {})}.items()]]
    cpus = ",".join(map(str, json.loads((j / "taskset" if (j / "taskset").exists() else j / "cpus").read_text())))
    return ["sudo", "-n", "unshare", "-m", "--propagation", "private", "--", "sh", str(j / "ns.sh"),
            "setpriv", f"--reuid={os.getuid()}", f"--regid={os.getgid()}", "--init-groups", "--",
            *env, "taskset", "-c", cpus, "timeout", "-s", "KILL", str(int(spec["limit"])), *argv]


def caches_in(ctx: dict, top: pathlib.Path) -> dict:
    """Each build cache's files for its key, into the job's tree; what it had, by cache."""
    had = {}
    for c in ctx.get("caches", []):
        cd = cache_dir(c["name"], c["key"])
        names = {f.name for f in cd.iterdir() if not f.name.startswith(".")} if cd.exists() else set()
        for f in names:
            shutil.copy2(cd / f, top / c["dir"] / f)
        had[c["name"]] = names
    return had


def caches_out(ctx: dict, top: pathlib.Path, had: dict, tag: str) -> list[str]:
    """The cache files this job built and the VM's cache lacks (each written whole, then renamed in)."""
    saved = []
    for c in ctx.get("caches", []):
        cd = cache_dir(c["name"], c["key"])
        new = [f for f in c.get("outputs", []) if CacheFiles().save((top / c["dir"] / f).exists(), f in had.get(c["name"], set()))]
        if new:
            cd.mkdir(parents=True, exist_ok=True)
            for f in new:
                tmp = cd / f".{f}.{tag}"
                shutil.copy2(top / c["dir"] / f, tmp)
                tmp.rename(cd / f)
            saved += [f"{c['name']}:{f}" for f in new]
    return saved


def run(batch: str, name: str, launch_identity=None):
    d = batch_dir(batch)
    j = d / "jobs" / name
    spec = json.loads((j / "spec.json").read_text())
    ctx = ctx_of(batch)
    repo = ctx.get("name", "repo")
    tree = pathlib.Path((d / "treepath").read_text()) if (d / "treepath").exists() else d / "tree"
    work = j / "work"
    work.mkdir()
    row: dict = {"name": name}
    if spec.get("strict_once") is True:
        identity = _strict_job_identity(j)
        if launch_identity != identity or os.getpid() != 1:
            raise ValueError("strict payload requires its identity-bound namespace init")
        row["launch_identity"] = identity
    if Outcome().code(tree.exists() and (d / "ready").exists(), 0, 0, 0) == -1:  # (Outcome: the batch's tree gone from this VM: lost, it runs elsewhere)
        write_json(j / "done.json", row | {"code": -1, "seconds": -1, "lost": True, "err": f"the batch's tree isn't on {os.uname().nodename}"})
        return
    try:
        if spec["kind"] == "sh":
            top = work / repo
            top.mkdir()
            subprocess.run(["cp", "-a", f"{tree}/.", str(top) + "/"], check=True)
            (top / ".sc-ready").unlink(missing_ok=True)
            had = caches_in(ctx, top)
            argv = wrap(j, spec, ctx, work, tree, f"/work/{repo}/{spec.get('cwd', '.')}", ["bash", "-c", spec["cmd"]])
            t0, oom0 = time.time(), oom_kills()
            with open(j / "out", "wb") as out, open(j / "err", "wb") as err:
                p = subprocess.run(argv, stdout=out, stderr=err, stdin=subprocess.DEVNULL)
            secs = round(time.time() - t0, 2)
            code = Outcome().code(True, p.returncode, int(secs), int(spec["limit"]))  # (a kill at its limit: 124, as Modal's runner reports its own)
            if code == 124 and p.returncode != 124:
                (j / "err").write_text(f"timed out after {spec['limit']} s")
            row.update(code=code, seconds=secs)
            if out_of_memory(p.returncode, int(secs), int(spec["limit"]), oom0, oom_kills()):
                row["oom"] = True
                with open(j / "err", "a") as err:
                    err.write(f"\nout of memory: killed by the kernel's OOM killer (exit {p.returncode}) on a VM of {mem_total_mb()} MB, "
                              f"{spec.get('memory', '?')} MB asked: ask for more memory (a bigger VM type)\n")
            saved = caches_out(ctx, top, had, name)
            if saved:
                row["saved"] = saved
        else:  # py: a function of a Modal runner, its arguments and result as JSON
            (j / "args.json").write_text(json.dumps(spec["args"]))
            argv = wrap(j, spec, ctx, work, tree, "/", ["python3", "-I", f"{ROOT}/agent.py", "call", f"/src/{repo}/{spec['module']}", spec["fn"], str(j / "args.json"), str(j / "result.json")])
            t0, oom0 = time.time(), oom_kills()
            with open(j / "out", "wb") as out, open(j / "err", "wb") as err:
                p = subprocess.run(argv, stdout=out, stderr=err, stdin=subprocess.DEVNULL)
            row.update(code=p.returncode, seconds=round(time.time() - t0, 2))
            if out_of_memory(p.returncode, int(row["seconds"]), int(spec.get("limit", 10 ** 6)), oom0, oom_kills()):
                row["oom"] = True
                with open(j / "err", "a") as err:
                    err.write(f"\nout of memory: killed by the kernel's OOM killer (exit {p.returncode}) on a VM of {mem_total_mb()} MB\n")
            if (j / "result.json").exists():
                row["result"] = json.loads((j / "result.json").read_text())
            else:
                row["error"] = ((j / "err").read_bytes()[-3000:].decode("utf-8", "replace") or f"exit {p.returncode}")
    except Exception as e:
        row.update(code=-1, seconds=-1, error=repr(e))
    finally:
        sudo("rm", "-rf", str(work), str(j / "tmp"), *map(str, j.glob("ov*")), check=False)
        write_json(j / "done.json", row)


def call(module: str, fn: str, args_file: str, out_file: str):
    os.chdir("/work") if os.path.isdir("/work") else None
    sys.path.insert(0, str(pathlib.Path(module).parent))
    mod = import_runner(module)
    args = json.loads(pathlib.Path(args_file).read_text())
    res = getattr(mod, fn)(*args) if isinstance(args, list) else getattr(mod, fn)(**args)
    pathlib.Path(out_file).write_text(json.dumps(res))


# -------------------------------------------------------------------------------------------------------------- collect
def logs_tar(batch: str) -> bytes:
    d = batch_dir(batch)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for j in sorted((d / "jobs").iterdir()) if (d / "jobs").exists() else []:
            for f, cap in (("out", 400_000), ("err", 100_000)):
                p = j / f
                if p.exists():
                    data = p.read_bytes()
                    data = data[-cap:] if len(data) > cap else data
                    ti = tarfile.TarInfo(f"{j.name}/{f}")
                    ti.size = len(data)
                    tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def parts(blob: pathlib.Path, make, part: str) -> dict:
    rule = Parts(5_000_000)
    if rule.remake(part == "0", blob.exists()):
        blob.write_bytes(make())
    data = blob.read_bytes()
    size, k = rule.size, int(part)
    return {"parts": rule.count(-(-len(data) // size)), "data": base64.b64encode(data[k * size:(k + 1) * size]).decode()}


def collect(batch: str, part: str) -> dict:
    return parts(batch_dir(batch) / "logs.tar.gz", lambda: logs_tar(batch), part)


def cache_tar(name: str, key: str) -> bytes:
    buf = io.BytesIO()
    d = cache_dir(name, key)
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for f in cache_names(name, key):
            tf.add(d / f, arcname=f)
    return buf.getvalue()


def cacheout(name: str, key: str, part: str = "-") -> dict:
    """A build cache's files for a key: their names (part "-"), or base64 tar.gz parts of 5 MB."""
    if part == "-":
        return {"names": cache_names(name, key)}
    return parts(ROOT / f"cache-{name}-{key}.tar.gz", lambda: cache_tar(name, key), part)


# This production Python decision is extracted directly by GDP (rules/agent.py symlink).
# gdp: law owned RPC requires exact protocol and source: not result or (protocol and version)
# gdp: law owned RPC requires fresh nonce and actual features: not result or (nonce and features)
# gdp: law replacement fence closes owned dispatch: not result or not upgrading
def owned_rpc_allowed(protocol: bool, version: bool, nonce: bool, features: bool, upgrading: bool) -> bool:
    return protocol and version and nonce and features and not upgrading


_rpc_gate_lock = threading.RLock()
_owned_nonces = set()


def owned_features():
    """Actual prerequisite probes, NOT a claim that namespace launch already succeeded."""
    import shutil
    try:
        if hashlib.sha256((ROOT / "agent.py").read_bytes()).hexdigest()[:12] != VERSION:
            return None  # running module and executable worker source must be identical
        if sys.platform != 'linux' or not hasattr(signal, 'pidfd_send_signal'):
            return None
        fd = os.pidfd_open(os.getpid())
        os.close(fd)
        if not pathlib.Path('/proc/self/ns/pid').exists() or not shutil.which('unshare') or not shutil.which('sudo'):
            return None
        return {'pidfd': True, 'private_pid_namespace': 'verified-before-payload'}
    except (OSError, AttributeError):
        return None


# gdp: law launched closure needs reaping and containment: not result or never_started or (reaped and contained)
# gdp: law launched closure needs namespace and supervisor identity: not result or never_started or identified
# gdp: law never started admission permits safe replacement: not never_started or result
def owned_replacement_safe(never_started: bool, reaped: bool, contained: bool, identified: bool) -> bool:
    return never_started or (reaped and contained and identified)


def _birth_record(value, namespace=False):
    return (isinstance(value, dict) and type(value.get('pid')) is int and value['pid'] > 0
            and isinstance(value.get('boot'), str) and bool(value['boot'])
            and type(value.get('start_ticks')) is int and value['start_ticks'] >= 0
            and (not namespace or (type(value.get('pidns')) is int and value['pidns'] > 0)))

def strict_unresolved():
    """Durable unknowns block replacement even in another/restarted interpreter."""
    if _owned_children:
        return True
    try:
        for path in (ROOT / 'owned-items').glob('*/*.json'):
            row = json.loads(path.read_text())
            _launch_nonce(row.get('identity'))
            init, supervisor = row.get('init'), row.get('supervisor')
            identified = (row.get('state') == 'launched' and _birth_record(init, True)
                          and _birth_record(supervisor) and init['boot'] == supervisor['boot'])
            if not owned_replacement_safe(row.get('state') == 'never_started',
                                          row.get('reaped') is True, row.get('contained') is True, identified):
                return True
        return False
    except (OSError, ValueError, TypeError):
        return True


def upgrade_busy():
    """Unfinished legacy jobs and unresolved strict ownership both defer updates.

    Called under the ownership lock shared with start(), including bootstrap
    from another interpreter. Missing/malformed completion is not proof of idle.
    """
    if strict_unresolved():
        return True
    try:
        for job in (ROOT / "b").glob("*/jobs/*"):
            if job.is_dir() and not (job / "done.json").exists() and not (job / "acked.json").exists():
                return True
        return False
    except OSError:
        return True


def owned_cacheput(name, key, data):
    import tempfile
    with tempfile.NamedTemporaryFile(dir=ROOT, suffix=".tar.gz", delete=False) as f:
        filename = f.name
        f.write(base64.b64decode(data, validate=True))
    try:
        return cachein(name, key, filename)
    finally:
        pathlib.Path(filename).unlink(missing_ok=True)


def owned_rpc(request):
    """Called ONLY behind daemon token auth. No legacy alias or downgrade path.

    Fresh request binding and exact source check occur in the same dispatcher as
    execution. Namespace support itself is verified by start before namespace.go.
    A lost reply is unknown, not permission to replace/retry a strict launch.
    """
    with _rpc_gate_lock:
        if not isinstance(request, dict):
            return {'ok': False, 'error': 'owned_protocol_refused'}
        nonce = request.get('nonce')
        features = owned_features()
        fresh = (isinstance(nonce, str) and re.fullmatch('[0-9a-f]{32}', nonce) is not None
                 and nonce not in _owned_nonces and len(_owned_nonces) < 65536)
        # This exact shipped decision, not a test-only model, guards ALL commands below.
        if not owned_rpc_allowed(type(request.get('protocol')) is int and request['protocol'] == 1,
                                 request.get('version') == VERSION, fresh, features is not None,
                                 (ROOT / 'owned-upgrading').exists()):
            return {'ok': False, 'error': 'owned_protocol_refused', 'protocol': 1,
                    'version': VERSION, 'nonce': nonce, 'features': features}
        _owned_nonces.add(nonce)
        cmd, args = request.get('command'), request.get('args')
        if not isinstance(args, list):
            return {'ok': False, 'error': 'owned_arguments_refused'}
        try:
            if cmd == 'vtick':
                msg, = args
                if not isinstance(msg, dict) or msg.get('wait', 0) != 0:
                    raise ValueError('owned tick must not wait')
                for st in msg.get('start', []):
                    if st['spec'].get('strict_once') is not True:
                        raise ValueError('owned RPC never launches legacy payload')
                    _launch_nonce(st['spec']['launch_identity'])
                    _item_ids(st['batch'], st['spec']['name'])
                for ack in msg.get('acks', []):
                    _launch_nonce(ack['launch_identity'])
                result = vtick(msg)
            elif cmd == 'capabilities':
                result = {'supported': True}
            elif cmd == 'begin':
                version, batch, tree, context = args
                if version != VERSION:
                    raise ValueError('begin source mismatch')
                tr = make_tree(tree, None, None)
                result = ({'ok': True, 'prep': prep(batch, 'tree:'+tree, context, tree, '', '-')}
                          if tr.get('ok') else {'ok': False, 'version': VERSION, 'need': 'manifest'})
            else:
                commands = {'tree': make_tree, 'prep': prep, 'have': lambda keys: {'missing': have(keys)},
                            'pull': pull, 'pullcache': pullcache, 'verify': verify_tree,
                            'put': lambda data: {'stored': put_blobs(io.BytesIO(base64.b64decode(data, validate=True)))},
                            'cacheput': owned_cacheput}
                if cmd not in commands:
                    raise ValueError('unsupported owned command')
                result = commands[cmd](*args)
            return {'ok': True, 'protocol': 1, 'version': VERSION, 'nonce': nonce,
                    'features': features, 'result': result}
        except Exception as error:
            return {'ok': False, 'error': 'owned_dispatch_refused', 'detail': repr(error),
                    'protocol': 1, 'version': VERSION, 'nonce': nonce, 'features': features}


# ------------------------------------------------------------------------------------------------------- the daemon
def daemon(port: str):
    """The warm agent: every command over HTTP, each request carrying the client's token."""
    import http.server
    # (SIGPIPE ignored: the CLI's default action would kill the whole daemon when a client goes away mid-answer, a
    # scheduler handing off or boat's relay dropping a connection; ignored, that's one request's BrokenPipeError)
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
    token = (ROOT / "token").read_text().strip()
    boot = pathlib.Path("/proc/sys/kernel/random/boot_id").read_text().strip() if pathlib.Path("/proc/sys/kernel/random/boot_id").exists() else ""

    with _ownership_lock():
        if strict_unresolved():
            raise RuntimeError("owned daemon restart refused: unresolved strict ownership")
        (ROOT / "owned-upgrading").unlink(missing_ok=True)
        (ROOT / "daemon.pid").write_text(str(os.getpid()))

    def reaper():
        while True:
            with contextlib.suppress(OSError, ValueError):
                _reap_children()
            time.sleep(1)
    threading.Thread(target=reaper, daemon=True).start()

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _ok(self, health: bool = False) -> bool:
            if not Access().allows(health, hmac.compare_digest(self.headers.get("X-SC-Token", ""), token)):
                self._send(403, {"error": "token"})
                return False
            return True

        def _send(self, code: int, obj=None, raw: bytes | None = None):
            body = raw if raw is not None else json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/octet-stream" if raw is not None else "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self) -> bytes:
            n = int(self.headers.get("Content-Length", "0") or 0)
            return self.rfile.read(n) if n else b""

        def do_GET(self):
            health = self.path.split("?")[0] == "/health"  # (no token: says only that it's up)
            if not self._ok(health):
                return
            if health:
                return self._send(200, {"ok": True})
            self._send(404, {"error": "GET /health, POST /rpc /put"})

        def do_POST(self):
            body = self._body()
            if not self._ok():
                return
            try:
                p = self.path.split("?")[0]
                if p == "/put":
                    return self._send(200, {"stored": put_blobs(io.BytesIO(body))})
                if p == "/blobs":  # (a peer VM pulling a tree's files: {"keys": [...]})
                    return self._send(200, raw=blobs_tar(json.loads(body)["keys"]))
                if p == "/cache":  # ?name=&key=
                    from urllib.parse import parse_qs, urlparse
                    q = parse_qs(urlparse(self.path).query)
                    tmp = ROOT / f"cache-in-{threading.get_ident()}.tar.gz"
                    tmp.write_bytes(body)
                    return self._send(200, cachein(q["name"][0], q["key"][0], str(tmp)))
                msg = json.loads(body or b"{}")
                cmd, a = msg.get("cmd"), msg.get("args", [])
                if cmd == "begin":  # [client version, batch, tree id, context]: the version, and the batch prepared if the tree is here
                    if a[0] != VERSION:
                        return self._send(200, {"ok": False, "version": VERSION})
                    tr = make_tree(a[2], None, None)
                    if not tr.get("ok"):
                        return self._send(200, {"ok": False, "version": VERSION, "need": "manifest"})
                    pr = prep(a[1], f"tree:{a[2]}", a[3], a[2], "", "-")
                    if not pr.get("ok") and "isn't on this VM" in str(pr.get("err")):  # (collected meanwhile: its manifest asked for, B6)
                        return self._send(200, {"ok": False, "version": VERSION, "need": "manifest"})
                    return self._send(200, {"ok": True, "version": VERSION, "prep": pr})
                if cmd == "ping":
                    dk = {}
                    with _quiet():
                        dk = clean_disk()  # (an idle VM's disk asked by ping: no starts, no reports)
                    return self._send(200, {"ok": True, "version": VERSION, "boot": boot, "cpus": os.cpu_count(), "load": os.getloadavg()[0], "disk": dk})
                if cmd == "have":
                    return self._send(200, {"missing": have(a[0])})
                if cmd == "verify":  # [tree id, {path: git blob id}]: the files of the tree that aren't the blobs named
                    return self._send(200, verify_tree(a[0], a[1]))
                if cmd == "tree":
                    return self._send(200, make_tree(a[0], a[1], a[2]))
                if cmd == "prep":
                    return self._send(200, prep(*a))
                if cmd == "tick":
                    if any(st.get("strict_once") is True for st in a[1].get("start", [])):
                        return self._send(200, {"ok": False, "error": "owned_rpc_required"})
                    return self._send(200, tick(a[0], a[1]))
                if cmd == "kill":
                    return self._send(200, kill(a[0]))
                if cmd == "itemlogs":  # [batch, name]: one item's out and err (their tails), for the scheduler to keep
                    j = batch_dir(a[0]) / "jobs" / a[1]
                    out = {f: (lambda p, cap: p.read_bytes()[-cap:].decode("utf-8", "replace") if p.exists() else "")(j / f, c) for f, c in (("out", 400_000), ("err", 100_000))}
                    return self._send(200, out)
                if cmd == "owned_rpc":
                    return self._send(200, owned_rpc(a[0]))
                if cmd == "upgrade":  # idle is decided atomically against ALL starts
                    data = base64.b64decode(a[0], validate=True)
                    compile(data, "agent.py.new", "exec")
                    with _rpc_gate_lock, _ownership_lock():
                        if upgrade_busy():
                            return self._send(200, {"ok": False, "error": "owned_upgrade_blocked"})
                        (ROOT / "owned-upgrading").write_text(VERSION)
                    (ROOT / "agent.py.new").write_bytes(data)
                    os.replace(ROOT / "agent.py.new", ROOT / "agent.py")
                    subprocess.Popen([sys.executable, "-c", "import subprocess,sys,time; time.sleep(0.5); subprocess.run([sys.executable, sys.argv[1], 'start-daemon', sys.argv[2], sys.argv[3]])",
                                      str(ROOT / "agent.py"), a[1], a[2]], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
                    return self._send(200, {"ok": True, "from": VERSION, "to": hashlib.sha256(data).hexdigest()[:12]})
                if cmd == "pull":  # [peer url, peer token, keys]: blobs from a peer VM
                    return self._send(200, pull(a[0], a[1], a[2]))
                if cmd == "pullcache":  # [peer url, peer token, name, key]
                    return self._send(200, pullcache(a[0], a[1], a[2], a[3]))
                if cmd == "cancelitem":
                    return self._send(200, cancelitem(*a))
                if cmd == "killitem":
                    return self._send(200, kill_item(a[0], a[1]))
                if cmd == "vtick":
                    if any(st.get("spec", {}).get("strict_once") is True for st in a[0].get("start", [])):
                        return self._send(200, {"ok": False, "error": "owned_rpc_required"})
                    return self._send(200, vtick(a[0]))
                if cmd == "logs":
                    return self._send(200, raw=logs_tar(a[0]))
                if cmd == "cacheout":
                    return self._send(200, raw=cache_tar(a[0], a[1])) if len(a) > 2 and a[2] == "tar" else self._send(200, {"names": cache_names(a[0], a[1])})
                return self._send(400, {"error": f"no command {cmd}"})
            except Exception as e:
                return self._send(500, {"error": repr(e)[-2000:]})

    class S(http.server.ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 64

    S(("127.0.0.1", int(port)), H).serve_forever()


def answers(port: str) -> bool:
    """The daemon on the port answers /health within 3 s (alive is not enough: a hung one is replaced)."""
    try:
        import urllib.request
        urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=3).read()
        return True
    except Exception:
        return False


def start_daemon(port: str, token: str) -> dict:
    """The warm agent, started (or kept: one already answering with this version and token stays)."""
    ROOT.mkdir(parents=True, exist_ok=True)
    old = (ROOT / "token").read_text().strip() if (ROOT / "token").exists() else ""
    pid = int((ROOT / "daemon.pid").read_text()) if (ROOT / "daemon.pid").exists() else 0
    same_version = (ROOT / "daemon.version").exists() and (ROOT / "daemon.version").read_text() == VERSION
    what = Access().keep_daemon(bool(pid) and alive(pid) and answers(port), old == token, same_version)  # (Access.keep_daemon; a daemon alive but not answering is replaced: B2)
    if what == "keep":
        return {"ok": True, "again": True}
    with _ownership_lock():
        if upgrade_busy():
            return {"ok": False, "error": "owned_upgrade_blocked"}
        # Published while holding the same lock as strict start: no new launch can race replacement.
        (ROOT / "owned-upgrading").write_text(VERSION)
    # Never name-kill other daemons/clients. Only the exact pidfile process, with command identity.
    if pid and alive(pid):
        fd = None
        try:
            fd = os.pidfd_open(pid)
            argv = pathlib.Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
            if str(ROOT / "agent.py").encode() not in argv or b"daemon" not in argv or port.encode() not in argv:
                return {"ok": False, "error": "daemon_identity_unknown"}
            if what == "replace":
                signal.pidfd_send_signal(fd, signal.SIGKILL)
        except (OSError, AttributeError):
            return {"ok": False, "error": "daemon_identity_unknown"}
        finally:
            if fd is not None:
                os.close(fd)
    time.sleep(0.2)
    fd = os.open(ROOT / "token", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    os.write(fd, token.encode())
    os.close(fd)
    (ROOT / "daemon.version").write_text(VERSION)
    subprocess.Popen([sys.executable, str(ROOT / "agent.py"), "daemon", port], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                     stderr=open(ROOT / "daemon.err", "w"), start_new_session=True, close_fds=True)
    for _ in range(50):
        try:
            import urllib.request
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1).read()
            return {"ok": True}
        except Exception:
            time.sleep(0.1)
    return {"ok": False, "err": (ROOT / "daemon.err").read_text()[-500:]}


if __name__ == "__main__":
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    cmd, *rest = sys.argv[1:]
    if cmd == "run":
        run(*rest)
    elif cmd == "ownedrun":
        ownedrun(*rest)
    elif cmd == "call":
        call(*rest)
    elif cmd == "daemon":
        daemon(*rest)
    elif cmd == "ingest":
        ingest(pathlib.Path(rest[0]), rest[1])
    else:
        out = {"prep": lambda *a: prep(*a),
               "cachein": lambda n, k, t: cachein(n, k, t),
               "tick": lambda b, *p: tick(b, json.loads(base64.b64decode("".join(p))) if p and p[0] != "-" else {}),
               "kill": kill, "collect": collect, "cacheout": cacheout,
               "serve": lambda d, p: serve(d, int(p)),
               "start-daemon": start_daemon}[cmd](*rest)
        print(json.dumps(out))
