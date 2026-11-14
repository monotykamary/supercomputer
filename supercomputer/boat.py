"""boat.dev VMs as a backend of the supercomputer: a pool of VMs that lives across batches and across repos, kept
stopped between them (a stopped VM bills no compute and keeps its disk), resumed per batch, stopped again a short idle
time after it, and deleted only on teardown or when an image changes.

    supercomputer run --jobs jobs.json --out <dir> [--backend auto|boat|modal]
    supercomputer status | reap | cold | teardown [--all] | image [--build] | usage <since ISO> | starts [--since 24h]
    supercomputer demand <client> [--for 300] [--pid P] [--done]   declare (or end) work about to be launched

A repo's image is one named snapshot, built in layers (its supercomputer.toml's, or its hooks' layers()), each layer a
snapshot of its own, so changing a layer rebuilds it and the ones above only. The pool is a registry of VM ids in
LIVE/boat-pool.json (~/.local/share/supercomputer/live: every repo on this machine shares one, as they share the boat
account and its start limits), guarded by a local lock as a cache. The VM agent grants the actual
owner/boot/epoch lease under its own file lock; a batch records its pid locally, resumes
the stopped ones, creates the rest within the plan's limits (`boat limits`: active VMs, starts per minute, hour and
day), and releases them at the end. Every VM a batch holds has a TTL renewed while the client lives (a VM whose client
died stops by itself within 15 minutes); a released VM's TTL is the idle time, after which boat stops it.

Starts are what the plan rations (a resume is one), and the account shares them with other projects: we spend within
the limits in force (the free ones, or the wallet's with extra starts on and SUPERCOMPUTER_URGENT=1: effective()), never
the reserve (60 a day, 15 an hour, or twice what the others used in the last day and hour, if more), and a batch at
most a share of what's left (budget()). VMs are large (4 slots a start). A tiny batch (TINY) runs on a VM that's
already up, else on Modal. With no start left a batch waits for released VMs within --max-wait, then goes to Modal.
Every start is noted in the registry and every batch in the ledger (LIVE/boat-starts.jsonl; `starts`).

How long a released VM stays up (boat's terms: TOS-6 forbids parking idle compute and bypassing rate limits, TOS-10
evading usage limits). A released VM stops IDLE (60 s) later: a back-to-back batch reuses it; never longer (the
old 10-minute `demand` window is gone). The scheduler holds VMs only while they run work: VMs above the floor run
filler for up to 45 minutes after their last real work, then go (README, "Scheduling policy").

A batch: the repo as git sees it (tracked and untracked files that aren't ignored, the submodules' too, minus the
config's ignore patterns). On a VM that is already up with its warm agent (agent.py's daemon), the batch talks to the
agent directly over boat's token-gated HTTPS URL and a token of ours: the tree is synced by hash (only files the VM
has never seen move). Claims and fallback renewal use boat exec; healthy item RPCs and tree sync go
directly to the agent. Otherwise (a VM resumed or
created) the tree goes as a tarball to one VM, which serves it to the others through a token-gated boat URL, and the
warm agent is started on each VM for the next batch. Items run as agent.py describes, longest first by the last run's
times, each VM taking what fits its free CPUs (2 vCPU and 4 GB a slot, as Modal's default container).
"""
import base64
import contextlib
import fcntl
import hashlib
import http.client
import io
import json
import math
import os
import pathlib
import random
import secrets
import selectors
import signal
import stat
import socket
import subprocess
import sys
import tarfile
import threading
import time
import tempfile
import urllib.parse
import weakref

# Keep the complete result-download/legacy-transport dependency chain in memory
# before submitting work. A concurrent `uv tool install --force --reinstall`
# may remove or replace this package directory while an existing client waits.
# Lazy imports at delivery (including error recovery) would then fail or mix
# code from two installs. Both CLI and programmatic clients import boat first.
from . import clientresults, config, lease_diagnostics, resultfiles, resultjson, resultrelay
from .config import env, project
from .rules import boat as R

HERE = pathlib.Path(__file__).resolve().parent
_CLIENT_SHA256 = hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest()
_CUSTODY_TRANSPORT_LOCK = threading.Lock()
BOAT = os.environ.get("BOAT_BIN") or next((p for p in [str(pathlib.Path.home() / ".ascii/bin/boat")] if os.path.exists(p)), "boat")
ENV = {**os.environ, "BOAT_NO_UPDATE": "1", "NO_COLOR": "1"}
SLOT_CPU, SLOT_MEM = 2, 4096  # a slot: Modal's default container
MEM_OVERHEAD = 2048  # MB a VM's OS, agent and boat's filesystem take of its nominal memory (11-01: 1.5-1.9 GB idle; rules.boat.fits_memory)
TYPES = {"small": (2, 4096), "default": (4, 8192), "large": (8, 16384), "xlarge": (16, 32768)}  # vCPU, MB
VM_TYPE = env("VM", "large")  # a batch's VMs: large ones, so a start brings 4 slots, not 2
BACKSTOP = 900  # the TTL a held VM keeps renewing: a VM whose client is gone stops within this
BRING_UP = 120  # seconds from a start to a VM ready for items (resume or creation, then the tree), typically
# The account's starts are shared with other projects (their setups take a few to a few dozen a day): this pool never
# spends the last RESERVE_DAY of the rolling day nor the last RESERVE_HOUR of the hour, nor what the others may need,
# twice what they used in the last day (hour): the account's used starts less ours (the registry's).
RESERVE_DAY = int(env("RESERVE_DAY", "60"))
RESERVE_HOUR = int(env("RESERVE_HOUR", "15"))
# The plan's limits are its free starts and, with extra starts on (`boat limits` keepGoing, $0.03-0.07 each past the
# free ones, capped a month), the wallet's: up to 6x the day and hour, 3x the minute. SUPERCOMPUTER_CEIL=day,hour,min
# shares of the free limits sets a ceiling of our own. Extra-start dollars are reported, never alerted on.
CEIL = [float(x) for x in env("CEIL").split(",")] if env("CEIL") else None
# Paced: a batch spends the FREE starts (the hour's and the day's, less the reserve), and extra starts only when it is
# urgent (SUPERCOMPUTER_URGENT=1): bursts of baselines, bars and a treadmill at once once spent 708 in an hour and $21
# of them. Within the hour a batch takes at most a sixth of what's left in any minute (PACE).
URGENT = env("URGENT", os.environ.get("URGENT", "")) not in ("", "0")
PACE = 6
# How a batch spends the plan's starts. SUPERCOMPUTER_POLICY=old: as many VMs as speed a batch up, no waiting for a
# released VM when no start is left (to compare).
OLD = env("POLICY") == "old"
SHARE = float(env("SHARE", "0.25"))  # a batch starts at most this share of the starts left
MIN_STARTS = 2  # (but this many, while any are left)
MAX_STARTS = int(env("MAX_STARTS", "50"))  # and at most this many a batch (0: no cap; --max-vms sets it for a run)
IDLE = 60  # a released VM stays up this long, for a back-to-back batch; never longer to save starts (boat's TOS-6)
# (was 600: a client that declared more work, demand(), kept its released VMs up 10 minutes. Superseded, 10-31: the
# scheduler keeps VMs busy while anything is queued and holds those above the floor on filler, never idle; so no path
# keeps an idle VM past IDLE. demand() stays as a no-op for old callers.)
LONG = IDLE
TINY = (4, 10)  # a batch of this many jobs or fewer, or under this many slot-minutes: a VM already up, else Modal
AGENT_PORT = 8770  # the warm agent's port on each VM (localhost, behind boat's URL)
WARM = env("WARM", "1") not in ("0", "")  # the warm agents (0: every batch by boat exec, as before)
VM_ROOT = "/home/user/.sc"  # the agent's directory on a VM
WARM_MARGIN = 8  # a warm VM is leased while it has this long before boat stops it (its TTL is extended at once)


def boat_fails_of(why) -> int:
    """The real boat failures a lost row reports (the scheduler's "boat failed it N times"), 0 for any other loss."""
    import re as _re
    m = _re.search(r"boat failed it (\d+) times", str(why or ""))
    return int(m.group(1)) if m else 0


class CustodyError(RuntimeError):
    """Required client evidence could not be persisted; never a reconnect signal."""


class Unavailable(RuntimeError):
    """boat can't serve this batch (API, billing, limits, image): the caller may move it to Modal."""


T0 = time.time()


def log(*a):
    print("[boat]" + (f" +{time.time() - T0:.2f}s" if env("VERBOSE") else ""), *a, file=sys.stderr, flush=True)


LIVE = config.LIVE
LEDGER = LIVE / "boat-starts.jsonl"
AGENTS = LIVE / "agents"  # each VM's warm agent: its URL (boat's token in it) and our token, 0600


# ------------------------------------------------------------------------------------------------------------- the CLI
_sem = threading.BoundedSemaphore(int(env("PARALLEL", "48")))
# the ticks wait on the VM up to 20 s for a finish: their own, wider bound (a `boat` process holds about 12 MB)
_tick_sem = threading.BoundedSemaphore(int(env("TICKS", "200")))


def boat(*args, timeout=120, check=True, stdin=None, sem=None) -> str:
    with sem or _sem:
        p = subprocess.run([BOAT, *args], capture_output=True, text=True, timeout=timeout, env=ENV, input=stdin)
    if check and p.returncode != 0:
        raise RuntimeError(f"boat {args[0]} failed ({p.returncode}): {(p.stderr or p.stdout)[-400:]}")
    return p.stdout


def bj(*args, timeout=120, check=True):
    return json.loads(boat(*args, "--json", timeout=timeout, check=check))


def exec_(vm: str, argv: list[str], timeout: int = 60, sem=None) -> dict:
    out = boat("exec", vm, "--timeout", str(timeout), "--json", "--", *argv, timeout=timeout + 60, check=False, sem=sem)
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"exitCode": -1, "stdout": "", "stderr": out[-400:], "success": False}


def exec_json(vm: str, argv: list[str], timeout: int = 60, tries: int = 4, sem=None):
    last, retry = None, R.Retry(max(1, tries))  # (its tries: rules.boat.Retry)
    for k in range(tries):
        r = exec_(vm, argv, timeout, sem)
        if r.get("exitCode") == 0:
            try:
                out = json.loads(r["stdout"].strip().splitlines()[-1])
                retry.attempt(True)
                return out
            except Exception as e:
                last = f"bad answer: {e}: {r.get('stdout', '')[-300:]} {r.get('stderr', '')[-300:]}"
        else:
            last = f"exit {r.get('exitCode')}: {r.get('stderr', '')[-400:]}{r.get('error', '')}"
        if retry.attempt(False) != "trying":
            break
        time.sleep(1 + 2 * k)
    raise RuntimeError(f"{vm}: {last}")


def _stop_clock() -> float:
    # Linux includes suspend; on macOS the wall clock also includes laptop sleep.
    clock = getattr(time, "CLOCK_BOOTTIME", None)
    return time.clock_gettime(clock) if clock is not None else time.time()


def _stop_budget(issued: tuple[float, float]) -> float:
    """Count fence round trip, suspend, and both provider calls inside STOP_S.

    The monotonic companion protects against wall-clock rollback on macOS.
    Keep 30 seconds unused, and never give one provider call over 90 seconds.
    """
    elapsed = max(0.0, _stop_clock() - issued[0], time.monotonic() - issued[1])
    return min(90.0, max(0.0, 270.0 - elapsed))

def stop_vm(vm: str, lease: dict | None = None) -> bool:
    """Reserve provider capacity BEFORE fencing; never queue past STOP_S."""
    with _sem:
        slot = contextlib.nullcontext()
        issued = (_stop_clock(), time.monotonic())
        if not fence_stop(vm, lease, sem=slot):
            return False
        budget = _stop_budget(issued)
        if budget < 1:
            return False
        out = boat("stop", vm, "--yes", "--json", check=False, timeout=budget, sem=slot)
        if R.stop_fallback("Final snapshot failed" in out or "stop aborted" in out) == "deleted":
            log(f"{vm}: boat cannot snapshot-stop it; fenced delete fallback")
            budget = _stop_budget(issued)
            if budget < 1:
                return False
            boat("delete", vm, "--yes", "--json", check=False, timeout=budget, sem=slot)
            with contextlib.suppress(Exception):
                with Registry().locked() as d:
                    d["vms"].pop(vm, None)
            return False
        return True


def delete_vm(vm: str, lease: dict | None = None) -> bool:
    """A missing/unreachable lease is never permission to delete a sandbox."""
    with _sem:
        slot = contextlib.nullcontext()
        issued = (_stop_clock(), time.monotonic())
        if not fence_stop(vm, lease, sem=slot):
            return False
        budget = _stop_budget(issued)
        if budget < 1:
            return False
        boat("delete", vm, "--yes", "--json", check=False, timeout=budget, sem=slot)
        return True


def limits() -> dict:
    return bj("limits")


def effective(lim: dict, urgent: bool | None = None) -> dict:
    """The starts' limits in force: the free ones, or, for an urgent batch (or a class the scheduler lets pay: gates
    and interactive checks once the free starts are spent), the wallet's while extra starts are on and under their cap."""
    st, kg, w = lim["starts"], lim.get("keepGoing") or {}, lim.get("startLimitsWithWallet")
    has_mult = bool((lim.get("overagePrices") or {}).get("startLimitMultiplier"))
    if not R.wallet_in_force(bool(URGENT if urgent is None else urgent), bool(kg.get("enabled")), bool(kg.get("capReached")), bool(w), has_mult):
        return st
    if not w:  # (boat drops startLimitsWithWallet once the wallet is empty; extra starts still go to the invoice)
        op, sl = lim.get("overagePrices") or {}, lim.get("startLimits") or {"perDay": st["day"]["limit"], "perHour": st["hour"]["limit"], "perMinute": st["minute"]["limit"]}
        if not op.get("startLimitMultiplier"):  # (no multiplier in the account's own limits: no paid starts guessed)
            return st
        m, mm = op["startLimitMultiplier"], op.get("startMinuteLimitMultiplier", 1)
        w = {"perDay": sl["perDay"] * m, "perHour": sl["perHour"] * m, "perMinute": sl["perMinute"] * mm}
    return {k: {"limit": w[p], "used": st[k]["used"], "remaining": w[p] - st[k]["used"]} for k, p in (("day", "perDay"), ("hour", "perHour"), ("minute", "perMinute"))}


PAID = env("PAID", "1") not in ("0", "")  # paid extra starts once the free ones are spent (the user, 10-31: "land it on boat")


def starts_left(lim: dict, mine: dict | None = None, urgent: bool | None = None) -> int:
    """The starts we may spend now. The free ones left this hour and day, less the reserve for the account's other
    projects (at least twice their last day's and hour's), which applies to the free starts only; then, while extra
    starts are on and under their cap, the paid ones up to the wallet's limits (3x the plan's: boat's multiplier), so
    the reserve never sends a batch to Modal. `urgent=False` (or SUPERCOMPUTER_PAID=0): the free ones only. A ceiling
    of our own (CEIL) still caps it."""
    # (rules.boat: free_left, paid_left, starts_left, ceiling)
    free, m = lim["starts"], mine or ours()
    k = R.free_left(free["hour"]["remaining"], free["day"]["remaining"], free["hour"]["used"], free["day"]["used"], m[3600], m[86400], RESERVE_HOUR, RESERVE_DAY)
    pay = (PAID or URGENT) if urgent is None else urgent
    paid = 0
    if pay and "keepGoing" in lim:
        w = effective(lim, True)  # (extra starts on, under their cap: what lies past the free limits, up to the wallet's)
        paid = R.paid_left(True, w is not free, w["hour"]["remaining"], w["day"]["remaining"], free["hour"]["remaining"], free["day"]["remaining"])
    return R.starts_left(k, paid, bool(CEIL), ceil_count(0, free["day"]["limit"]) - m[86400] if CEIL else 0, ceil_count(1, free["hour"]["limit"]) - m[3600] if CEIL else 0)


def ceil_count(i: int, limit: int) -> int:
    """CEIL's i-th share (day, hour, minute) of a limit, as a count (rules.boat.ceiling)."""
    return R.ceiling(int(round(CEIL[i] * 100)), int(limit))


def ours(d: dict | None = None) -> dict:
    """Our starts in the last minute, hour and day (the registry's `starts`: every start a client of this pool made)."""
    ts = (d or Registry().read()).get("starts", [])
    now = time.time()
    return {w: sum(1 for t in ts if t > now - w) for w in (60, 3600, 86400)}


def note_starts(k: int, d: dict | None = None):
    """k starts made now, into the registry (within a locked registry `d`, or locking it); k < 0 takes back the
    newest -k (granted, then refused by boat)."""
    if k == 0:
        return
    if d is None:
        with Registry().locked() as d:
            return note_starts(k, d)
    now = time.time()
    ts = [t for t in d.get("starts", []) if t > now - 86400]
    log_ = R.StartLog()  # (the count: rules.boat.StartLog; the newest taken back)
    log_.count = len(ts)
    log_.note(k)
    d["starts"] = ts + [round(now, 1)] * (log_.count - len(ts)) if log_.count >= len(ts) else ts[:log_.count]


def ledger(acct: bool = True, **row):
    """One line of the starts ledger (LIVE/boat-starts.jsonl): a batch's starts, VMs and slots, its client and
    project, and our and the account's starts of the hour and day after it (the account's asked of boat only for a
    batch that started VMs: a warm one makes no API call); a batch that went to Modal says why, with no start."""
    with contextlib.suppress(Exception):
        m = ours()
        row.setdefault("project", project().name)
        if acct:
            with contextlib.suppress(Exception):
                st = limits()["starts"]
                acct = {"acct_hour": st["hour"]["used"], "acct_day": st["day"]["used"]}
        acct = acct if isinstance(acct, dict) else {}
        row = {"t": round(time.time(), 1), "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "client": client(), **row, "our_hour": m[3600], "our_day": m[86400], **acct}
        LIVE.mkdir(parents=True, exist_ok=True)
        with open(LEDGER, "a") as f:
            f.write(json.dumps(row) + "\n")


def client() -> str:
    """Who runs this batch: $SUPERCOMPUTER_CLIENT ($GDPW_BOAT_CLIENT: gdp-wired's bar, bar-<pid>), else the checkout's
    directory name (a lane's worktree)."""
    return env("CLIENT") or project().root.name


def budget(left: int, cap: int = 0) -> int:
    """The starts one batch may spend of those left: a share, so that the tracks running at once all get some and
    the hour isn't spent by the first big batch, and at most `cap` (--max-vms for a run, else MAX_STARTS; the old
    policy: all of them)."""
    return R.budget(int(left), int(round(SHARE * 100)), int(cap or MAX_STARTS), OLD)  # (rules.boat.budget; MIN_STARTS 2 there)


class Listing(dict):
    """boat's sandboxes by id, and whether the listing is whole (`complete`: no page of it said there was more)."""
    complete = True


def sandboxes() -> dict:
    """Every sandbox of the account: `boat list --all`, and when its answer says there's more (pageInfo.hasMore),
    each state group on its own (each one smaller); still partial if one of those has more (B12)."""
    d = bj("list", "--all")
    out = Listing({s["id"]: s for s in d["sandboxes"]})
    if (d.get("pageInfo") or {}).get("hasMore"):
        out.complete = True
        for g in "rspte":
            dd = bj("list", "--filter", g)
            out.update({s["id"]: s for s in dd["sandboxes"]})
            out.complete = out.complete and not (dd.get("pageInfo") or {}).get("hasMore")
    return out


_snaps = {"t": 0.0, "d": {}}


def named_snapshots(max_age: float = 0) -> dict:
    """boat's named snapshots (at most max_age seconds old: preflight's and the session's image check share one call)."""
    if time.time() - _snaps["t"] > max_age:
        _snaps["d"], _snaps["t"] = {s["name"]: s for s in bj("snapshots").get("named", [])}, time.time()
    return _snaps["d"]


RUNNING = {"ready", "idle", "running"}


_states = {"t": 0.0, "d": {}}
_states_lock = threading.Lock()


def states(max_age: float = 3.0) -> dict:
    """`boat list` at most every few seconds, however many threads ask."""
    with _states_lock:
        if time.time() - _states["t"] > max_age:
            _states["d"] = sandboxes()
            _states["t"] = time.time()
        return _states["d"]


def wait_running(ids: list[str], timeout: int = 600) -> dict:
    """Wait for hydrated, running VMs that answer exec, within the caller's existing budget."""
    t0, ok = time.time(), {}
    pending = list(ids)
    while pending and time.time() - t0 < timeout:
        try:
            states_ = states()
        except Exception:
            time.sleep(3)
            continue
        for i in list(pending):
            status = states_.get(i, {})
            if R.bootstrap_exec_allowed(status.get("state") in RUNNING, status.get("hydrated") is True):
                r = exec_(i, ["true"], 30)
                if r.get("exitCode") == 0:
                    ok[i] = round(time.time() - t0, 1)
                    pending.remove(i)
        if pending:
            time.sleep(3)
    return ok


# ---------------------------------------------------------------------------------------------------------- the image
_ALIAS: dict = {}


def layers(p: config.Project | None = None) -> list[tuple[str, str, str]]:
    """(layer, its snapshot's name, its script), each name hashing every script below it too; the top layer's name is
    the image's (<final_prefix>-<hash>)."""
    p = p or project()
    out, h = [], hashlib.sha256(p.image_salt.encode() + b"\n")
    for name, script in p.layer_scripts():
        h.update(name.encode() + b"\0" + script.encode() + b"\0")
        out.append((name, f"{p.prefix}-{name}-{h.hexdigest()[:10]}", script))
        _ALIAS[out[-1][1]] = f"{p.prefix}-{h.hexdigest()[:12]}"  # (the name it had when it was the top layer: the same snapshot)
    # (the image's own name has a prefix of its own: a client of older code pruned every <prefix>- name it didn't use)
    out[-1] = (out[-1][0], f"{p.final_prefix}-{h.hexdigest()[:12]}", out[-1][2])
    return out


_image: dict = {}


def image_name() -> str:
    """The repo's image; $SUPERCOMPUTER_IMAGE names another (a test of the fallback names one that doesn't exist)."""
    if env("IMAGE"):
        return env("IMAGE")
    k = str(project().root)
    if k not in _image:
        _image[k] = layers()[-1][1]
    return _image[k]


def build_layer(name: str, snap: str, script: str, parent: str | None):
    log(f"building layer {name} as {snap}" + (f" from {parent}" if parent else ""))
    args = ["new", "--ttl", "7200", "--no-env", "--no-monitoring", "--json"] + (["--from", parent] if parent else [])
    vm = None
    # A maintenance builder is not a pool worker of the image it is creating.
    with new_vm_args(snap + "-build") as (birth_args, nonce):
        output = boat(*args, *birth_args, timeout=900)
    for line in output.splitlines():
        with contextlib.suppress(Exception):
            vm = vm or json.loads(line).get("id")
    if not vm:
        raise RuntimeError("no VM for the build")
    note_starts(1)  # (a build's VM is a start of ours too)
    ticket = LIVE / f"boat-build-{vm}.json"
    LIVE.mkdir(parents=True, exist_ok=True)
    ticket.write_text(json.dumps({"vm": vm, "pid": os.getpid(), "host": socket.gethostname(), "snapshot": snap}))
    try:
        wait_running([vm])
        source = base64.b64encode((HERE / "agent.py").read_bytes()).decode()
        claim = {"owner": owner_id(), "vm_id": vm, "nonce": nonce}
        granted = ship_agent(vm, source, [], claim=claim)
        if not granted.get("ok"):
            raise LeaseLost(f"{vm}: build claim refused")
        path = ":".join([*project().path, "$HOME/.local/bin", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"])
        b64 = base64.b64encode((f"set -ex\nexport DEBIAN_FRONTEND=noninteractive PATH={path}\n" + script).encode()).decode()
        exec_json(vm, ["bash", "-c", 'rm -rf ~/.sc/build && mkdir -p ~/.sc/build && echo "$1" | base64 -d > ~/.sc/build/l.sh && (setsid nohup bash -c "bash ~/.sc/build/l.sh > ~/.sc/build/l.log 2>&1; echo \\$? > ~/.sc/build/l.rc" > /dev/null 2>&1 &) && echo "{}"', "_", b64])
        t0 = time.time()
        while True:
            time.sleep(20)
            renewed = lease_vm(vm, "renew", granted)
            if renewed.get("ok") is not True or renewed.get("stale"):
                raise LeaseLost(f"{vm}: builder lease lost; no further writes or snapshot")
            r = exec_(vm, ["bash", "-c", "cat ~/.sc/build/l.rc 2>/dev/null; echo; tail -c 600 ~/.sc/build/l.log"], 60)
            out = r.get("stdout", "")
            rc = out.split("\n", 1)[0].strip()
            if int(time.time() - t0) % 120 < 20:
                log(f"  {name}: {int(time.time() - t0)} s, {out.splitlines()[-1][-120:] if out.strip() else ''}")
            if rc:
                break
            if time.time() - t0 > 6600:
                raise RuntimeError(f"layer {name} still building after {int(time.time() - t0)} s")
        full = exec_(vm, ["bash", "-c", "tail -c 400000 ~/.sc/build/l.log"], 120).get("stdout", "")
        (LIVE / "builds").mkdir(parents=True, exist_ok=True)
        (LIVE / "builds" / f"{snap}.log").write_text(full)
        if rc != "0":
            raise RuntimeError(f"layer {name} failed ({rc}), its log in {LIVE / 'builds' / (snap + '.log')}:\n{out[-1500:]}")
        exec_(vm, ["bash", "-c", "sudo apt-get clean; rm -rf ~/.cache/pip ~/.sc/build; rm -f ~/.config/ascii/boat/config.json ~/.sc/controller/boat-key; sync"], 120)
        r = exec_(vm, ["bash", "-c", CRED_CHECK], 300)  # (an image never carries a credential: the build fails if one is found)
        if r.get("exitCode") != 0:
            raise RuntimeError(f"layer {name}: credential-looking files on the build VM, not saved: {r.get('stdout', '')[-600:]}")
        log(f"  {name} built in {int(time.time() - t0)} s; saving {snap}")
        renewed = lease_vm(vm, "renew", granted)
        if renewed.get("ok") is not True or renewed.get("stale"):
            raise LeaseLost(f"{vm}: builder lease lost before snapshot")
        boat("snapshot", vm, snap, "--json", timeout=1800)
        for _ in range(180):
            s = named_snapshots().get(snap)
            if s and s.get("status") == "ready":
                break
            time.sleep(10)
        else:
            raise RuntimeError(f"snapshot {snap} not ready")
    finally:
        if delete_vm(vm):
            ticket.unlink(missing_ok=True)


# What may not be in an image (names only are printed): boat's own login (config.json with a token), our keys, git and
# cloud credentials, private keys, env files in the home directory.
CRED_CHECK = r"""
found=0
for f in ~/.config/ascii/boat/*.json; do [ -f "$f" ] && grep -q '"token": *"[^"]' "$f" && { echo "boat login: $f"; found=1; }; done
for f in ~/.sc/controller/boat-key ~/.netrc ~/.git-credentials ~/.aws/credentials ~/.config/gh/hosts.yml ~/.docker/config.json ~/.npmrc ~/.pypirc; do
  [ -s "$f" ] && { echo "credential file: $f"; found=1; }; done
for f in $(find ~ -maxdepth 3 \( -name '*.pem' -o -name 'id_rsa' -o -name 'id_ed25519' -o -name '.env' -o -name '.env.*' \) -type f 2>/dev/null | grep -v '/\.ssh/authorized_keys'); do
  grep -q 'PRIVATE KEY' "$f" 2>/dev/null && { echo "private key: $f"; found=1; }
  case "$f" in *.env|*.env.*) echo "env file: $f"; found=1;; esac; done
exit $found
"""


@contextlib.contextmanager
def image_build_lock(image: str):
    """One build of an image at a time on this machine (two agents in one worktree, or `image --build` beside a batch):
    the second waits, then finds it ready instead of building it again."""
    LIVE.mkdir(parents=True, exist_ok=True)
    with open(LIVE / f"image-build-{image}.lock", "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def ensure_image(build: bool = True) -> str:
    have = named_snapshots(60)
    if env("IMAGE"):
        if have.get(image_name(), {}).get("status") != "ready":
            raise Unavailable(f"boat image {image_name()} doesn't exist")
        return image_name()
    chain = layers()
    parent = None
    for name, snap, script in chain:
        if have.get(_ALIAS.get(snap), {}).get("status") == "ready" and snap not in have:
            snap = _ALIAS[snap]  # (a layer that was the image's top before another went on it)
        if snap not in have or have[snap].get("status") != "ready":
            if not build:
                raise Unavailable(f"boat image {chain[-1][1]} isn't built (layer {snap} missing): supercomputer image --build")
            build_layer(name, snap, script, parent)
        parent = snap
    prune_snapshots({s for _, s, _ in chain} | {_ALIAS[s] for _, s, _ in chain if s in _ALIAS}, chain[-1][1])
    return chain[-1][1]


def images_of(d: dict, p: config.Project | None = None) -> list[str]:
    """The repo's images in the registry, oldest first (gdp-wired's are `images`, as its older clients keep them)."""
    p = p or project()
    return d.get("images", []) if p.prefix == "gdpw" else d.get("images_of", {}).get(p.prefix, [])


def prune_snapshots(keep: set[str], current: str):
    """Record image ordering, but NEVER automatically delete provider snapshots.

    Prefix/age/last-two selection is not ownership: another checkout may share
    the prefix, an older image may have resident workers, or a packet may still
    pin it. Reading controller pins then deleting here would also race admission.
    Keep this compatibility entry point non-destructive until a controller-owned
    deletion protocol can fence every creator and credential holder. Unknown or
    expired pins do not authorize client-side deletion either.
    """
    p = project()
    try:
        with Registry().locked() as d:
            seen = d.setdefault("image_t", {})
            seen.setdefault(current, time.time())
            imgs = sorted(set(images_of(d, p)) | {current}, key=lambda i: seen.get(i, 0))
            if p.prefix == "gdpw":
                d["images"] = imgs[-2:]
            else:
                d.setdefault("images_of", {})[p.prefix] = imgs[-2:]
        log("automatic snapshot deletion disabled; image retention is controller-owned")
    except Exception as e:
        log(f"(image bookkeeping skipped; snapshot deletion remains disabled: {e})")


def retire(d: dict, image: str):
    """Free VMs of this repo's images neither current nor the one before: deleted (the toolchain changed)."""
    p = project()
    keep = set(images_of(d, p)[-2:]) | {image}
    mine = lambda snap: snap.startswith((f"{p.prefix}-", f"{p.final_prefix}-"))
    old = [i for i, v in d["vms"].items() if entry(v).retire(mine(v["snapshot"]), v["snapshot"] in keep)]  # (rules.boat.Entry.retire)
    for i in old:
        if delete_vm(i):
            d["vms"].pop(i)
            forget_agent(i)
    if old:
        log(f"deleted {len(old)} VMs of superseded images")


# ------------------------------------------------------------------------------------------------------- the registry
def pid_alive(pid: int, host: str) -> bool:
    """A holder alive (rules.boat.holder_alive): another machine's never presumed dead; this machine's, or a record
    that names no host (written here), dead once its pid is gone."""
    here = socket.gethostname()
    if host and host != here:
        return R.holder_alive(host, here, "alive")
    try:
        os.kill(pid, 0)
        probe = "alive"
    except ProcessLookupError:
        probe = "gone"
    except PermissionError:
        probe = "denied"
    return R.holder_alive(host or "", here, probe)


def entry(v: dict, now: float | None = None, batch: str = "") -> R.Entry:
    """A registry VM as rules.boat.Entry sees it: up or not, its lease (none, this batch's, another live client's, or a
    dead holder's), until and last_used."""
    e = R.Entry(v.get("state") in RUNNING, float(v.get("until", 0) or 0), float(v.get("last_used", 0) or 0))
    l = v.get("lease")
    e.lease = "none" if not l else "dead" if not pid_alive(l["pid"], l.get("host", "")) else "ours" if batch and l.get("batch") == batch else "theirs"
    return e


class Registry:
    def __init__(self):
        LIVE.mkdir(parents=True, exist_ok=True)
        self.path = LIVE / "boat-pool.json"
        self.lockf = LIVE / "boat-pool.lock"

    @contextlib.contextmanager
    def locked(self):
        with open(self.lockf, "a") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                d = self.read()
                yield d
                tmp = self.path.with_suffix(".tmp")
                tmp.write_text(json.dumps(d, indent=1))
                tmp.rename(self.path)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def read(self) -> dict:
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return {"vms": {}}


def resume_missing(output: str) -> bool:
    """A direct resume's structured 404, not an absent row in a possibly partial list."""
    for line in output.splitlines():
        with contextlib.suppress(ValueError, TypeError):
            row = json.loads(line)
            if isinstance(row, dict) and (row.get("status") == 404 or row.get("code") == "sandbox_not_found"):
                return True
    return False

def forget_missing(vm_id: str, batch: str) -> bool:
    """Drop a confirmed 404 atomically with its credentials, only under our exact lease.
    No provider stop/delete is needed for a VM that does not exist."""
    with Registry().locked() as d:
        v = d["vms"].get(vm_id)
        lease = (v or {}).get("lease") or {}
        if lease.get("batch") != batch or lease.get("pid") != os.getpid() or lease.get("host") != socket.gethostname():
            return False
        d["vms"].pop(vm_id)
        forget_agent(vm_id)
        return True

def reconcile(d: dict, live: dict):
    """Drops VMs boat no longer has, deletes those whose stopped disk expired (boat keeps it 14 days; the pool never
    counts on one), and records each one's state."""
    complete = getattr(live, "complete", True)
    for i in list(d["vms"]):
        s = live.get(i) or {}
        if s:
            d["vms"][i].pop("absent", None)
        elif complete:
            d["vms"][i]["absent"] = d["vms"][i].get("absent", 0) + 1
        # (gone: rules.boat.absent_gone, deleted or absent from two complete listings, never from one page; B12)
        gone = R.absent_gone(s.get("state") in ("deleted", "deleting"), not s, complete, d["vms"][i].get("absent", 0))
        if not s and not gone:
            continue
        # (rules.boat.Entry.reconcile)
        what = entry(d["vms"][i]).reconcile(gone, bool(s.get("dataExpiredAt")),
                                            s.get("state") in ("error", "stopped") and not s.get("snapshotAvailable"),
                                            (s.get("stop") or {}).get("status") == "failing" and s.get("hydrated") is False)
        if what == "drop":
            d["vms"].pop(i)
            forget_agent(i)
        elif what == "delete":
            # (a stopped VM with no snapshot can't be resumed: boat stops a VM that young without one; one whose disk
            # isn't there and that boat can't stop, for want of a snapshot to stop to, answers no exec and stays up,
            # billed: a batch would take it as up and wait for it in vain)
            if not s.get("dataExpiredAt"):
                log(f"{i}: {'stuck stopping, no disk' if s.get('hydrated') is False else 'stopped without a snapshot'}: deleted")
            if delete_vm(i):
                d["vms"].pop(i)
                forget_agent(i)
        else:
            d["vms"][i]["state"] = s.get("state")


REFRESH_DAYS = 12  # a VM stopped this long is resumed and stopped again when a batch runs, before boat drops its disk


def refresh_old(d: dict, image: str):
    """Free VMs of the pool's type stopped for REFRESH_DAYS: resumed and stopped again (in the background, a start each,
    within the starts left), which restarts boat's clock; others boat drops after 14 days."""
    # Stage 1 cannot reserve stopped disks account-wide. Do not spend a start
    # solely to refresh one, or stop a VM a concurrent resumer may now hold.
    return


def lease_free(v: dict) -> bool:
    return entry(v).free()


def demand(name: str, ttl: int = 300, pid: int = 0, label: str = ""):
    """A client declares work it is about to launch (the bar between its groups, a controller with a queue): until
    `ttl` s from now, while its pid lives, the VMs its batches release stay up LONG s, not IDLE (warm_ttls)."""
    with Registry().locked() as d:
        dm = d.setdefault("demand", {})
        dm[name] = {**dm.get(name, {}), "pid": pid or os.getppid(), "host": socket.gethostname(), "until": time.time() + ttl, "label": label}


def undeclare(name: str, d: dict | None = None) -> list[str]:
    """The client's work is done (or it died): its declaration goes, and the VMs kept up for it stop IDLE s from now."""
    if d is None:
        with Registry().locked() as d:
            ids = undeclare(name, d)
        for i in ids:
            threading.Thread(target=shrink_ttl, args=(i, IDLE)).start()
        return ids
    d.get("demand", {}).pop(name, None)
    now, ids = time.time(), []
    for i, v in d["vms"].items():
        e = entry(v)
        if e.undeclare(now, v.get("owner") == name):  # (rules.boat.Entry.undeclare)
            v["until"], v["owner"] = e.until, None
            ids.append(i)
    return ids


def declared(d: dict, name: str) -> dict | None:
    dm = d.get("demand", {}).get(name)
    return dm if dm and dm["until"] > time.time() and pid_alive(dm["pid"], dm.get("host", "")) else None


# ---------------------------------------------------------------------------------------------------------- the tree
def _ignored(path: str, patterns: list[str]) -> bool:
    import fnmatch
    for p in patterns:
        if fnmatch.fnmatch(path, p) or (p.startswith("**/") and fnmatch.fnmatch(path, p[3:])):
            return True
    return False


def tree_files(root: pathlib.Path | None = None, ignore: list[str] | None = None) -> list[str]:
    """What git sees: tracked and untracked-not-ignored files, the submodules' too, minus the repo's ignore patterns."""
    root = root or project().root
    ignore = project().ignore if ignore is None else ignore

    def ls(d: pathlib.Path, prefix: str) -> list[str]:
        out = subprocess.run(["git", "-C", str(d), "ls-files", "-z", "-co", "--exclude-standard"], capture_output=True, check=True).stdout.decode().split("\0")
        files = []
        generated = {f for f in filter(None, out) if ".verification" in pathlib.PurePosixPath(f).parts}
        tracked = set()
        if generated:
            tracked = set(subprocess.run(["git", "-C", str(d), "ls-files", "-z", "--cached"],
                                         capture_output=True, check=True).stdout.decode().split("\0"))
        for f in filter(None, out):
            # Generated evidence archives are not implicit source inputs. Tracked fixtures
            # remain source; --data is the explicit route for untracked verification inputs.
            if f in generated and f not in tracked:
                continue
            p = d / f
            if p.is_dir() and not p.is_symlink() and (p / ".git").exists():
                files += ls(p, prefix + f + "/")
            elif p.is_file() or p.is_symlink():
                files.append(prefix + f)
        return files
    return sorted(f for f in set(ls(root, "")) if not _ignored(f, ignore))


def tree_tarball(root: pathlib.Path | None = None, files: list[str] | None = None) -> bytes:
    root = root or project().root
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tf:
        for f in files if files is not None else tree_files(root):
            tf.add(root / f, arcname=f, recursive=False)
    return buf.getvalue()


def tree_id(files: dict, links: dict) -> str:
    """agent.tree_id: the same id on both sides."""
    return hashlib.sha256(json.dumps([sorted(files.items()), sorted(links.items())], separators=(",", ":")).encode()).hexdigest()[:24]


# (a tree kept per checkout and reused while nothing it was made from moved, read with no git process:
# rules.boat.tree_reusable. Before, each batch's tree forked git 3+ times from the client and stat'ed every file:
# 0.8 s of CPU a build, the treadmill's 6.6% of the laptop, 11-02)
TREE_V = 2
_TREES: dict = {}
_GIT_ENV = ("GIT_DIR", "GIT_INDEX_FILE", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY")


def _st(p) -> list | None:
    try:
        s = os.lstat(p)
    except OSError:
        return None
    return [s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_ino, s.st_mode]


def _git_state(d: pathlib.Path, excludes: str) -> list:
    """A checkout's git state with no git process: its HEAD, index, ref, packed refs, configs and ignore files."""
    g = d / ".git"
    if g.is_file():  # (a worktree's or a submodule's: a file naming its git dir)
        g = (d / g.read_text().partition("gitdir:")[2].strip()).resolve()
    c = g
    with contextlib.suppress(OSError):
        c = (g / (g / "commondir").read_text().strip()).resolve()
    head = (g / "HEAD").read_text().strip()
    ref = head[4:].strip() if head.startswith("ref:") else ""
    home = pathlib.Path.home()
    xdg = pathlib.Path(os.environ.get("XDG_CONFIG_HOME") or home / ".config")
    return [str(g), head, _st(g / "index"), _st(c / ref) if ref else None, _st(c / "packed-refs"), _st(c / "config"), _st(g / "config.worktree"),
            _st(c / "info" / "exclude"), _st(home / ".gitconfig"), _st(xdg / "git" / "config"), _st(xdg / "git" / "ignore"), _st(excludes) if excludes else None]


def _tree_dirs(root: pathlib.Path) -> tuple[list[str], list[str], str]:
    """The directories git walks for a tree (every one not ignored, nested checkouts' too: a file added to or removed
    from any changes its mtime), the checkouts, the global excludes file. Git runs here only on a build."""
    clean = {k: v for k, v in os.environ.items() if k not in _GIT_ENV}
    excludes = subprocess.run(["git", "-C", str(root), "config", "--path", "--get", "core.excludesFile"], capture_output=True, text=True, env=clean).stdout.strip()
    dirs, repos = [], []

    def walk(repo: pathlib.Path):
        repos.append(str(repo.relative_to(root)))
        out = subprocess.run(["git", "-C", str(repo), "ls-files", "-z", "-o", "-i", "--exclude-standard", "--directory"], capture_output=True, check=True, env=clean, timeout=60).stdout.decode()
        # (a directory git lists as ignored is one only when a pattern excludes it: one holding only ignored files is
        # listed too, and a file added there is in the tree)
        cand = [x for x in out.split("\0") if x.endswith("/")]
        ex = subprocess.run(["git", "-C", str(repo), "check-ignore", "-z", "--stdin"], input="\0".join(cand).encode(), capture_output=True, env=clean, timeout=60).stdout.decode() if cand else ""
        ign = {os.path.normpath(repo / x) for x in ex.split("\0") if x}
        stack = [repo]
        while stack:
            d = stack.pop()
            dirs.append(str(d.relative_to(root)))
            with os.scandir(d) as it:
                for e in it:
                    if e.name == ".git" or not e.is_dir(follow_symlinks=False) or os.path.normpath(e.path) in ign:
                        continue
                    p = pathlib.Path(e.path)
                    if (p / ".git").exists():
                        walk(p)
                    else:
                        stack.append(p)
    walk(root)
    return dirs, repos, excludes


def _tree_moved(root: pathlib.Path, rec: dict) -> tuple[bool, bool, bool]:
    """(same git, same files, same dirs) against a kept tree."""
    try:
        same_git = [_git_state(root / r, rec["excludes"]) for r in rec["repos"]] == rec["git"]
    except OSError:
        same_git = False
    r, j = str(root), os.path.join  # (strings, not paths: ~2,000 stats a reuse)
    same_files = same_git and all(_st(j(r, f)) == s for f, s in zip(rec["names"], rec["fst"]))
    same_dirs = same_files and all((_st(j(r, d)) or [0, 0, 0])[1:4] == s for d, s in zip(rec["dirs"], rec["dst"]))
    return same_git, same_files, same_dirs


class Tree:
    """A batch's tree: its files by content key (sha256, and an x for an executable), its links, its id; the tarball
    only when a VM without the warm agent needs it. Keys come from an index of (size, mtime, inode) here, so an
    unchanged file isn't read again."""

    def __init__(self, root: pathlib.Path | None = None, tar: bytes | None = None, data: dict | None = None):
        self.root = config.select_root(root) if root is not None else project().root
        self._tar = tar
        self.files: dict[str, str] = {}
        self.gits: dict[str, str] = {}  # (a clean tracked file's git blob id: what the VM checks the file it builds is)
        self.links: dict[str, str] = {}
        # local data directories shipped with the tree ({dest in the tree: a directory here}; $SUPERCOMPUTER_DATA as
        # "dir:dest,...", `run --data DIR[:DEST]`): as Modal's add_local_dir, by hash like the rest
        self.src: dict[str, pathlib.Path] = {}
        if data is None and env("DATA"):
            data = {}
            for part in env("DATA").split(","):
                d, _, dest = part.partition(":")
                data[dest or pathlib.Path(d).name] = pathlib.Path(d).expanduser()
        for dest, d in (data or {}).items():
            for f in sorted(pathlib.Path(d).rglob("*")):
                if f.is_file() and not f.is_symlink():
                    self.src[f"{dest.strip('/')}/{f.relative_to(d)}"] = f.resolve()
        if tar is not None:
            with tarfile.open(fileobj=io.BytesIO(tar)) as tf:
                for m in tf.getmembers():
                    if m.issym():
                        self.links[m.name] = m.linkname
                    elif m.isfile():
                        self.files[m.name] = hashlib.sha256(tf.extractfile(m).read()).hexdigest() + ("x" if m.mode & 0o111 else "")
            self.names = sorted(self.files) + sorted(self.links)
        elif not self._reuse():
            self._pre = None
            if self._cacheable():
                with contextlib.suppress(Exception):
                    dirs, repos, excludes = _tree_dirs(self.root)
                    self._pre = (dirs, [(_st(self.root / d) or [0, 0, 0])[1:4] for d in dirs], repos, excludes, [_git_state(self.root / r, excludes) for r in repos])
            self.names = tree_files(self.root) + sorted(self.src)
            self._index()
            self._keep()
        self.id = tree_id(self.files, self.links)

    def _ip(self, ext: str) -> pathlib.Path:
        return LIVE / "index" / (hashlib.sha256(str(self.root).encode()).hexdigest()[:16] + ext)

    def _cacheable(self) -> bool:
        return not self.src and not any(k in os.environ for k in _GIT_ENV)  # (a hook's own index: built anew)

    def _reuse(self) -> bool:
        """The tree kept for this checkout, when nothing it was made from moved (rules.boat.tree_reusable)."""
        if not self._cacheable():
            return False
        root = str(self.root.resolve())
        rec = _TREES.get(root)
        if rec is None:
            with contextlib.suppress(Exception):
                rec = json.loads(self._ip(".tree.json").read_text())
        if not rec or rec.get("v") != TREE_V or rec.get("root") != root or rec.get("ignore") != list(project().ignore):
            return False
        if not R.tree_reusable(*_tree_moved(self.root, rec)):
            _TREES.pop(root, None)
            return False
        _TREES[root] = rec
        self.names, self.files, self.gits, self.links = list(rec["names"]), dict(rec["files"]), dict(rec["gits"]), dict(rec["links"])
        return True

    def _keep(self):
        """This build kept for the next batch's (its directories and git state read before git listed its files, its
        files' stats before their bytes: a change during the build is a change the next one sees)."""
        if not self._cacheable() or not getattr(self, "_pre", None):
            return
        dirs, dst, repos, excludes, git = self._pre
        root = str(self.root.resolve())
        rec = {"v": TREE_V, "root": root, "ignore": list(project().ignore), "repos": repos, "excludes": excludes, "git": git, "dirs": dirs, "dst": dst,
               "names": self.names, "fst": [self._fst.get(f) for f in self.names], "files": self.files, "gits": self.gits, "links": self.links}
        _TREES[root] = rec
        with contextlib.suppress(Exception):
            ip = self._ip(".tree.json")
            ip.parent.mkdir(parents=True, exist_ok=True)
            tmp = ip.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(rec, separators=(",", ":")))
            tmp.rename(ip)

    def _index(self):
        ip = LIVE / "index" / (hashlib.sha256(str(self.root).encode()).hexdigest()[:16] + ".json")
        try:
            idx = json.loads(ip.read_text())
        except Exception:
            idx = {}
        new, self._fst = {}, {}
        for f in self.names:
            p = self.src.get(f) or self.root / f
            st = p.lstat()
            self._fst[f] = [st.st_size, st.st_mtime_ns, st.st_ctime_ns, st.st_ino, st.st_mode]
            if p.is_symlink():
                self.links[f] = os.readlink(p)
                continue
            sig = [st.st_size, st.st_mtime_ns, st.st_ino, st.st_mode & 0o111 != 0]
            x = "x" if sig[3] else ""
            blob = self._clean().get(f)
            old = idx.get(f)
            if blob and blob + x in self._by_blob():
                k = self._by_blob()[blob + x]  # (a clean tracked file: its blob's key, recorded only from its own bytes)
            elif not blob and old and old[0] == sig:
                k = old[1]  # (a file git doesn't vouch for: its stat signature here, as before)
            else:
                h, g, n = hashlib.sha256(), hashlib.sha1(), st.st_size
                g.update(b"blob %d\0" % n)
                with open(p, "rb") as fh:
                    for chunk in iter(lambda: fh.read(1 << 20), b""):
                        h.update(chunk)
                        g.update(chunk)
                k = h.hexdigest() + x
                # (rules.boat.blob_map_ok: its blob's key recorded only from these bytes, when they are that blob)
                if blob and R.blob_map_ok(g.hexdigest(), blob):
                    self._by_blob()[blob + x] = k
                    self.gits[f] = blob
            if blob and self._by_blob().get(blob + x) == k:
                self.gits[f] = blob  # (the manifest's word: this file is that blob; the VM checks it before a job)
            self.files[f] = k
            new[f] = [sig, k]
        with contextlib.suppress(Exception):
            ip.parent.mkdir(parents=True, exist_ok=True)
            tmp = ip.with_suffix(f".{os.getpid()}.tmp")
            tmp.write_text(json.dumps(new))
            tmp.rename(ip)
        if hasattr(self, "_by_blob_m") and len(self._by_blob_m) != self._by_blob_n0:
            with contextlib.suppress(Exception):
                bp = LIVE / "index" / "blobs.v2.json"  # (only entries recorded from their own bytes: blobs.json's, unchecked, never read)
                tmp = bp.with_suffix(f".{os.getpid()}.tmp")
                tmp.write_text(json.dumps(dict(list(self._by_blob().items())[-200_000:])))
                tmp.rename(bp)

    # (a file's key from its git blob id: the index above is per checkout, so a new worktree or a gate's snapshot read
    # and hashed every file once, ~40 s of a core at a client's start (11-01 08:25); a clean tracked file's blob id,
    # git's, says what it holds whichever checkout it's in, and its sha256 is kept once for all of them)
    def _clean(self) -> dict:
        if not hasattr(self, "_clean_m"):
            self._clean_m = {}
            with contextlib.suppress(Exception):
                def walk(d: pathlib.Path, prefix: str):
                    run = lambda *a: subprocess.run(["git", "-C", str(d), *a], capture_output=True, check=True, timeout=60).stdout.decode()
                    dirty = set(filter(None, run("ls-files", "-z", "-m").split("\0")))
                    for line in filter(None, run("ls-files", "-z", "-s").split("\0")):
                        meta, _, name = line.partition("\t")
                        mode, sha, _ = meta.split(" ", 2)
                        if mode == "160000":  # (a submodule: its own files, its own blobs)
                            walk(d / name, prefix + name + "/")
                        elif mode.startswith("100") and name not in dirty:
                            self._clean_m[prefix + name] = sha
                walk(self.root, "")
        return self._clean_m

    def _by_blob(self) -> dict:
        if not hasattr(self, "_by_blob_m"):
            self._by_blob_m = {}
            with contextlib.suppress(Exception):
                self._by_blob_m = json.loads((LIVE / "index" / "blobs.v2.json").read_text())
            self._by_blob_n0 = len(self._by_blob_m)
        return self._by_blob_m

    @property
    def tar(self) -> bytes:
        if self._tar is None:
            if self.src:
                buf = io.BytesIO()
                with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tf:
                    for f in self.names:
                        tf.add(self.src.get(f) or self.root / f, arcname=f, recursive=False)
                self._tar = buf.getvalue()
            else:
                self._tar = tree_tarball(self.root, self.names)
        return self._tar

    def blobs(self, keys: list[str]) -> bytes:
        """The files of these keys, as a tar named by key (gz: what changed is often source text)."""
        by = {}
        for f, k in self.files.items():
            by.setdefault(k, f)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tf:
            for k in keys:
                if by[k] in self.src:
                    data = self.src[by[k]].read_bytes()
                elif self._tar is not None and not (self.root / by[k]).exists():
                    data = tarfile.open(fileobj=io.BytesIO(self._tar)).extractfile(by[k]).read()
                else:
                    data = (self.root / by[k]).read_bytes()
                if not R.blob_ok(k.rstrip("x"), hashlib.sha256(data).hexdigest()):  # (rules.boat.blob_ok)
                    raise RuntimeError(f"{by[k]} changed while the batch shipped it: run again")
                ti = tarfile.TarInfo(k)
                ti.size = len(data)
                ti.mode = 0o755 if k.endswith("x") else 0o644
                tf.addfile(ti, io.BytesIO(data))
        return buf.getvalue()

    def read(self, rel: str) -> bytes | None:
        if rel in self.src:
            return self.src[rel].read_bytes()
        if self._tar is not None and not (self.root / rel).exists():
            with contextlib.suppress(KeyError, AttributeError), tarfile.open(fileobj=io.BytesIO(self._tar)) as tf:
                return tf.extractfile(rel).read()
            return None
        p = self.root / rel
        return p.read_bytes() if p.exists() else None


# ------------------------------------------------------------------------------------------------- the build caches
def cache_local(c: dict, key: str) -> pathlib.Path:
    return LIVE / c["name"] / key  # (gdp-wired's Lean library: LIVE/olean/<key>, as before)


def cache_files(c: dict, key: str) -> list[pathlib.Path]:
    d = cache_local(c, key)
    outs = set(project().cache_outputs(c))
    return sorted(f for f in d.iterdir() if f.name in outs) if d.exists() else []


def cache_tarball(c: dict, key: str) -> tuple[bytes | None, list[str]]:
    """The cache files an earlier batch brought back for this key, for the VMs that lack them."""
    files = cache_files(c, key)
    if not files:
        return None, []
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=1) as tf:
        for f in files:
            tf.add(f, arcname=f.name)
    return buf.getvalue(), [f.name for f in files]


def cache_from_modal(c: dict, key: str):
    """A cache's files for this key from the Modal volume the repo's Modal jobs keep it in, when this machine has fewer
    of them (gdp-wired's Lean library: a boat batch on a new key would otherwise compile it in every job, which slows
    the jobs that time a kernel check and fails one that imports a module it doesn't build)."""
    vol = c.get("modal_volume")
    outs = project().cache_outputs(c)
    if not vol or len(cache_files(c, key)) >= len(outs):
        return
    import shutil
    import tempfile
    d = cache_local(c, key)
    with tempfile.TemporaryDirectory() as tmp, contextlib.suppress(Exception):
        subprocess.run(["modal", "volume", "get", vol, f"/{key}", tmp, "--force"], capture_output=True, timeout=120)
        got = [f for f in pathlib.Path(tmp).rglob("*") if f.is_file() and f.name in outs]
        if len(got) > len(cache_files(c, key)):
            d.mkdir(parents=True, exist_ok=True)
            for f in got:
                shutil.copy2(f, d / f".{f.name}.in")
                (d / f".{f.name}.in").rename(d / f.name)
            log(f"{len(got)} files of cache {c['name']} {key[:12]} from Modal's volume {vol}")


# ------------------------------------------------------------------------------------------------ the warm agents
def agent_file(vm: str) -> pathlib.Path:
    return AGENTS / f"{vm}.json"


def read_agent(vm: str) -> dict | None:
    with contextlib.suppress(Exception):
        return json.loads(agent_file(vm).read_text())
    return None


def write_agent(vm: str, info: dict):
    AGENTS.mkdir(parents=True, exist_ok=True)
    os.chmod(AGENTS, 0o700)
    with config.private_umask():
        tmp = agent_file(vm).with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(info))
        tmp.rename(agent_file(vm))


def forget_agent(vm: str):
    agent_file(vm).unlink(missing_ok=True)


LEASE_NAME_SUFFIX = "~l"


def owner_id() -> str:
    """Stable random identity per machine; process handoff must not change owner."""
    LIVE.mkdir(parents=True, exist_ok=True)
    with open(LIVE / "owner.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = LIVE / "owner"
        if path.exists():
            value = path.read_text().strip()
            if value:
                return value
        value = secrets.token_hex(24)
        with config.private_umask():
            path.write_text(value + "\n")
        return value


def owner_key() -> str:
    """One local credential per machine; losing a VM cache must not rotate it."""
    owner_id()
    with open(LIVE / "owner.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = LIVE / "owner.key"
        if path.exists():
            return path.read_text().strip()
        token = secrets.token_urlsafe(32)
        with config.private_umask():
            path.write_text(token + "\n")
        return token


@contextlib.contextmanager
def new_vm_args(image: str):
    """Birth reservation is present before boat reports the new VM ready."""
    nonce = secrets.token_hex(24)
    with tempfile.TemporaryDirectory(prefix="sc-birth-") as tmp:
        path = pathlib.Path(tmp) / "birth.json"
        path.write_text(json.dumps({"owner": owner_id(), "nonce": nonce}))
        yield ["--name", image + LEASE_NAME_SUFFIX, "--file", f"{VM_ROOT}/birth.json={path}"], nonce


def lease_vm(vm: str, op: str = "claim", lease: dict | None = None, nonce: str = "", sem=None) -> dict:
    payload = {**(lease or {}), "owner": owner_id(), "vm_id": vm}
    if nonce:
        payload["nonce"] = nonce
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    try:
        return exec_json(vm, ["python3", VM_ROOT + "/agent.py", "lease", op, encoded], 30, 1, sem)
    except RuntimeError as exc:
        message = str(exc)
        if "lease" in message and any(word in message for word in ("KeyError", "no command", "unsupported command")):
            raise LeaseProtocolLost(f"{vm}: lease protocol disappeared; old client wins") from exc
        raise


def remember_lease(vm: str, lease: dict):
    if lease.get("ok") and not lease.get("stale") and lease.get("boot") and lease.get("epoch"):
        write_agent(vm, {**(read_agent(vm) or {}), "lease": {**lease, "owner": owner_id(), "vm_id": vm}})


def _stop_fence(vm: str, lease: dict | None = None, sem=None) -> dict:
    lease = lease or (read_agent(vm) or {}).get("lease")
    if (not lease or lease.get("owner", lease.get("holder")) != owner_id()
            or lease.get("vm_id") != vm or not lease.get("boot")
            or type(lease.get("epoch")) is not int or lease["epoch"] <= 0):
        return {}
    try:
        answer = lease_vm(vm, "fence_stop", lease, sem=sem) if sem is not None else lease_vm(vm, "fence_stop", lease)
        if answer.get("ok") is True and not answer.get("stale"):
            return answer
        with contextlib.suppress(Exception):
            log('LEASE_STOP_REFUSED ' + json.dumps(lease_diagnostics.event(
                vm, 'stop_fence.refused', lease=lease, reply=answer, operation='lease/fence_stop'), sort_keys=True))
        return {}
    except Exception as exc:
        with contextlib.suppress(Exception):
            log('LEASE_STOP_REFUSED ' + json.dumps(lease_diagnostics.event(
                vm, 'stop_fence.exception', lease=lease, error=exc, operation='lease/fence_stop'), sort_keys=True))
        return {}


def fence_stop(vm: str, lease: dict | None = None, sem=None) -> bool:
    return bool(_stop_fence(vm, lease, sem))


def shrink_ttl(vm: str, ttl: int, lease: dict | None = None) -> bool:
    lease = lease or (read_agent(vm) or {}).get("lease")
    with _sem:
        slot = contextlib.nullcontext()
        issued = (_stop_clock(), time.monotonic())
        fence = _stop_fence(vm, lease, sem=slot)
        if not fence:
            return False
        budget = _stop_budget(issued)
        if budget < 1:
            return False
        boat("extend", vm, "--ttl", str(ttl), "--json", check=False, timeout=budget, sem=slot)
        with contextlib.suppress(Exception):
            lease_vm(vm, "release", {**lease, "stop_nonce": fence.get("stop_nonce")}, sem=slot)
        return True


def rename_leased_vm(vm: str, image: str, lease: dict) -> bool:
    """A handed-off worker stops matching old clients' exact image name."""
    name = image + LEASE_NAME_SUFFIX
    if (read_agent(vm) or {}).get("lease_name") == name:
        return True
    try:
        check = lease_vm(vm, "check", lease)
        if check.get("ok") is not True or check.get("stale"):
            return False
        boat("rename", vm, name, "--json", timeout=60)
        write_agent(vm, {**(read_agent(vm) or {}), "lease_name": name})
        return True
    except Exception as exc:
        log(f"{vm}: lease name not confirmed; old client wins any contest ({str(exc)[:120]})")
        return False

def agent_health(vm: str, timeout: float = 8) -> dict:
    """Public HTTP only: an exec-only view must not steal during a front outage."""
    info = read_agent(vm) or {}
    url = info.get("url")
    if not url:
        url = bj("host", vm, str(AGENT_PORT), "--localhost", "--private")["url"]
        write_agent(vm, {**info, "url": url})
    return AgentClient(vm, url, "").who(timeout)


EXEC_ARG = 60_000  # (bytes of one boat exec argument: well under the kernel's 128 KB a string)
EXEC_BODY = 4_000_000  # (a request sent over boat exec when the front fails, in parts of EXEC_ARG: a 4,000-file manifest)

# sc-agent-bootstrap: remote file transport, not an alternate agent/protocol.
# Static program + positional arguments only. No source, path or token is shell code.
_AGENT_BOOTSTRAP = r'''
# sc-agent-bootstrap: bounded staged source install
import base64, hashlib, json, os, re, stat, sys, types
op, root, name, owner = sys.argv[1:5]
answer = {}
if (not os.path.isabs(root) or not re.fullmatch(r'bootstrap-[0-9a-f]{48}', name)
        or not re.fullmatch(r'[0-9a-f]{48}', owner)):
    raise ValueError('invalid bootstrap path')
# Walk with directory FDs: no symlink in any parent, including ~/.sc.
fd = os.open('/', os.O_RDONLY | os.O_DIRECTORY)
try:
    parts = root.split('/')[1:]
    if not parts or any(p in ('', '.', '..') for p in parts):
        raise ValueError('invalid agent root')
    for i, part in enumerate(parts):
        if i == len(parts)-1:
            try: os.mkdir(part, 0o700, dir_fd=fd)
            except FileExistsError: pass
        nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        os.close(fd); fd = nxt
    st = os.fstat(fd)
    if st.st_uid != os.getuid() or st.st_mode & 0o022:
        raise ValueError('agent root is not private to its owner')
    if op == 'init':
        os.mkdir(name, 0o700, dir_fd=fd)
    try:
        work = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
    except FileNotFoundError:
        if op != 'cleanup': raise
        print('{}'); sys.exit(0)
    try:
        st = os.fstat(work)
        if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700:
            raise ValueError('unsafe bootstrap directory')
        if op == 'init':
            marker = os.open('owner', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=work)
            with os.fdopen(marker, 'w') as stream: stream.write(owner)
        marker = os.open('owner', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=work)
        with os.fdopen(marker) as stream:
            if stream.read() != owner: raise ValueError('bootstrap ownership mismatch')
        if op == 'init':
            out = os.open('payload.b64', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=work)
            os.close(out)
        elif op == 'append':
            offset, chunk = sys.argv[5:7]
            out = os.open('payload.b64', os.O_WRONLY | os.O_APPEND | os.O_NOFOLLOW, dir_fd=work)
            with os.fdopen(out, 'ab') as stream:
                st = os.fstat(stream.fileno())
                if not stat.S_ISREG(st.st_mode) or st.st_size != int(offset):
                    raise ValueError('agent upload offset/type mismatch')
                stream.write(chunk.encode('ascii')); stream.flush(); os.fsync(stream.fileno())
        elif op == 'commit':
            digest, size = sys.argv[5:7]
            lease = json.loads(base64.b64decode(sys.argv[7])) if len(sys.argv) > 7 else {}
            claim = json.loads(base64.b64decode(sys.argv[8])) if len(sys.argv) > 8 else {}
            src = os.open('payload.b64', os.O_RDONLY | os.O_NOFOLLOW, dir_fd=work)
            with os.fdopen(src, 'rb') as stream: data = base64.b64decode(stream.read(), validate=True)
            if len(data) != int(size) or hashlib.sha256(data).hexdigest() != digest:
                raise ValueError('agent upload size/digest mismatch')
            out = os.open('agent.py', os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=work)
            with os.fdopen(out, 'wb') as stream:
                stream.write(data); stream.flush(); os.fsync(stream.fileno())
            try: old = os.stat('agent.py', dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError: old = None
            if old is not None and not stat.S_ISREG(old.st_mode):
                raise ValueError('unsafe installed agent')
            # Verified staged code acquires/checks the lease BEFORE replacement.
            module = types.ModuleType('sc_staged_agent')
            module.__file__ = os.path.join(root, name, 'agent.py')
            sys.modules[module.__name__] = module
            exec(compile(data, module.__file__, 'exec'), module.__dict__)
            module.ROOT = module.pathlib.Path(root)
            if claim:
                answer = module.lease('claim', claim)
                if not answer.get('ok') or answer.get('stale'):
                    print(json.dumps(answer)); sys.exit(0)
                lease = dict(answer, owner=claim['owner'], vm_id=claim['vm_id'])
            with module.lease_guard(lease):
                os.replace('agent.py', 'agent.py', src_dir_fd=work, dst_dir_fd=fd)
                os.fsync(fd)
        elif op != 'cleanup':
            raise ValueError('invalid bootstrap operation')
        if op in ('commit', 'cleanup'):
            # Only this upload's known files; never recursive removal or a glob.
            for leaf in ('payload.b64', 'agent.py', 'owner'):
                try: os.unlink(leaf, dir_fd=work)
                except FileNotFoundError: pass
            os.rmdir(name, dir_fd=fd)
    finally:
        os.close(work)
finally:
    os.close(fd)
print(json.dumps(answer))
'''


def ship_agent(vm: str, agent_b64: str, args: list[str], timeout: int = 60, tries: int = 4, sem=None, lease: dict | None = None, claim: dict | None = None):
    """Stage bounded argv chunks; verify the full source before atomic install/start.

    No append/commit is retried: an ambiguous append cannot silently duplicate a
    chunk. A failed upload never touches the installed agent. Cleanup owns only
    one cryptographically random directory, on success as well as failure.
    """
    if not lease and not claim:
        raise LeaseLost(f"{vm}: source installation requires a lease or birth claim")
    source = base64.b64decode(agent_b64, validate=True)
    digest = hashlib.sha256(source).hexdigest()
    name = 'bootstrap-' + secrets.token_hex(24)
    owner = secrets.token_hex(24)
    prefix = ['python3', '-c', _AGENT_BOOTSTRAP]
    def remote(op, *args):
        return exec_json(vm, [*prefix, op, VM_ROOT, name, owner, *args], timeout, 1, sem)
    try:
        remote('init')
        for k in range(0, len(agent_b64), EXEC_ARG):
            remote('append', str(k), agent_b64[k:k + EXEC_ARG])
        encode = lambda value: base64.b64encode(json.dumps(value or {}).encode()).decode()
        granted = remote('commit', digest, str(len(source)), encode(lease), encode(claim))
        if granted.get('stale') or (claim and not granted.get('ok')):
            return granted
        if claim:
            lease = {**granted, 'owner': claim['owner'], 'vm_id': vm}
            remember_lease(vm, lease)
    finally:
        # Also handles an init/commit whose response was lost. O_NOFOLLOW and the
        # random owned basename prevent cleanup from following another path.
        remote('cleanup')
    # Retry only the original command, never append or partially repeat an
    # install. Preserve arbitrary commands, JSON results, timeout/tries and sem.
    if not args:
        return {**(lease or {}), 'ok': True}
    return exec_json(vm, ['python3', VM_ROOT + '/agent.py', *args], timeout, tries, sem)


def bootstrap_agent(vm: str, agent_b64: str, token: str, timeout: int = 90, tries: int = 3,
                    lease: dict | None = None, nonce: str = ""):
    # Ask the VM through verified staged source before installing any code.
    claim = None if lease else {"owner": owner_id(), "vm_id": vm, "nonce": nonce}
    result = ship_agent(vm, agent_b64, [], timeout, tries, lease=lease, claim=claim)
    if result.get("stale") or not result.get("ok"):
        return result
    lease = lease or {**result, "owner": owner_id(), "vm_id": vm}
    remember_lease(vm, {**lease, "ok": True})
    exec_json(vm, ["python3", VM_ROOT + "/agent.py", "keys", "add", owner_id(), token], timeout, tries)
    fence = base64.b64encode(json.dumps(lease).encode()).decode()
    result = exec_json(vm, ["python3", VM_ROOT + "/agent.py", "start-daemon", str(AGENT_PORT), token, fence], timeout, tries)
    if result.get("ok") is True:
        write_agent(vm, {**(read_agent(vm) or {}), "token": token, "lease": lease})
    return result


class AgentError(RuntimeError):
    pass


class AgentHTTPError(AgentError):
    """A non-success HTTP response, with the status retained as typed evidence."""
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


class AgentTransportError(AgentError):
    """A recognized transient connection failure; not an HTTP or semantic refusal."""


def _transient_transport_error(error) -> bool:
    return (isinstance(error, (TimeoutError, ConnectionError, http.client.IncompleteRead))
            or (isinstance(error, socket.gaierror) and error.errno == socket.EAI_AGAIN))


class WorkerEndpointUnavailable(AgentError):
    """The worker-local agent listener refused the exec relay connection."""


class LeaseProtocolLost(AgentError):
    """An old client replaced the lease protocol; do not bootstrap it back."""
    pass


class LeaseLost(AgentError):
    """Expected contention, not a VM failure; never repair or stop the new holder."""
    pass


class _AgentIdlePools:
    """One short-lived sweeper for all nonempty pools; never owns an in-flight connection.

    The registry is weak so a cached client is not kept alive by cleanup itself.
    Pool locks precede this condition; the sweeper drops the condition before taking a pool lock.
    """

    def __init__(self):
        self.condition = threading.Condition()
        self.clients = weakref.WeakSet()
        self.thread = None

    def add(self, client):
        with self.condition:
            self.clients.add(client)
            if self.thread is None:
                self.thread = threading.Thread(target=self._run, name="agent-http-idle", daemon=True)
                self.thread.start()

    def discard(self, client):
        with self.condition:
            self.clients.discard(client)
            if not self.clients:
                self.condition.notify()

    def _run(self):
        try:
            while True:
                with self.condition:
                    if not self.clients:
                        # Clear under the condition so add() cannot miss a restart.
                        self.thread = None
                        return
                    self.condition.wait(5)
                    clients = [weakref.ref(c) for c in self.clients]
                for ref in clients:
                    client = ref()
                    try:
                        if client is not None:
                            client._prune_idle()
                    except Exception:
                        # One broken pool must not disable cleanup for every VM.
                        pass
                    finally:
                        del client
        finally:
            with self.condition:
                if self.thread is threading.current_thread():
                    self.thread = None


_AGENT_IDLE_POOLS = _AgentIdlePools()


class AgentClient:
    """The warm agent of one VM over boat's token-gated URL: boat's token in the URL's query (its first answer sets a
    cookie and redirects; the cookie is kept with the URL, 0600, so the next process skips that round trip) and ours
    in X-SC-Token. Kept-alive connections, shared by the batch's threads."""

    def __init__(self, vm: str, url: str, token: str, cookie: str = "", lease: dict | None = None):
        self.vm, self.token = vm, token
        self.owner = owner_id()
        self.lease = lease if lease is not None else ((read_agent(vm) or {}).get("lease") or {})
        u = urllib.parse.urlparse(url)
        self.url, self.host, self.base, self.query = url, u.netloc, u.path.rstrip("/"), u.query
        self.cookie = cookie
        self.idle: list[tuple[http.client.HTTPSConnection, float]] = []
        self.lock = threading.Lock()

    IDLE_TIMEOUT = 30.0  # monotonic seconds; untouched pools swept within a further 5 seconds

    @staticmethod
    def _reusable(c, idle_s):
        sock = c.sock
        try:
            # Do not recv/peek: SSLSocket does not support MSG_PEEK, and reads could
            # consume protocol bytes. Even unexpected readable data is safer retired.
            readable = False
            if sock is not None:
                with selectors.DefaultSelector() as selector:
                    selector.register(sock, selectors.EVENT_READ)
                    readable = bool(getattr(sock, "pending", lambda: 0)()) or bool(selector.select(0))
            return R.agent_http_reusable(idle_s, AgentClient.IDLE_TIMEOUT, sock is not None, readable)
        except (OSError, ValueError):  # closed/invalid descriptor
            return False

    def _prune_idle_locked(self):
        now = time.monotonic()
        kept = []
        for c, returned in self.idle:
            if self._reusable(c, now - returned):
                kept.append((c, returned))
            else:
                with contextlib.suppress(OSError):
                    c.close()
        self.idle = kept
        if not kept:
            _AGENT_IDLE_POOLS.discard(self)

    def _prune_idle(self):
        with self.lock:
            self._prune_idle_locked()

    def _get(self, timeout: float) -> http.client.HTTPSConnection:
        with self.lock:
            self._prune_idle_locked()
            c = self.idle.pop()[0] if self.idle else None
            if not self.idle:
                _AGENT_IDLE_POOLS.discard(self)
        if c is None:
            c = http.client.HTTPSConnection(self.host, timeout=timeout)
        c.timeout = timeout
        if c.sock is not None:
            c.sock.settimeout(timeout)
        return c

    def _put_back(self, c):
        with self.lock:
            self._prune_idle_locked()
            if len(self.idle) < 8 and self._reusable(c, 0):
                self.idle.append((c, time.monotonic()))
                _AGENT_IDLE_POOLS.add(self)
                return
        with contextlib.suppress(OSError):
            c.close()

    def _req(self, method: str, path: str, body: bytes | None = None, timeout: float = 60, ctype: str = "application/json", decode=None, transport_event=None) -> tuple[int, object, str]:
        from .rules.owned_lifecycle import strict_transport_attempts
        strict = False
        if method == "POST" and path in ("/submit", "/snapshot", "/close-receipt") and body:
            with contextlib.suppress(ValueError, TypeError, UnicodeDecodeError):
                msg = json.loads(body)
                strict = isinstance(msg, dict) and (
                    (path == "/submit" and msg.get("strict_once") is True) or
                    (path in ("/snapshot", "/close-receipt") and type(msg.get("close_recovery")) is int and msg["close_recovery"] == 1))
        last, retry = None, R.Retry(strict_transport_attempts(strict, 4))
        def notify(event, **details):
            if transport_event:
                try:
                    transport_event(event, **details)
                except Exception as exc:
                    raise CustodyError("transport custody persistence failed") from exc
        while retry.outcome == "trying":
            k = retry.tries
            c = self._get(timeout)
            hdr = {"X-SC-Token": self.token, "Content-Type": ctype}
            if self.cookie:
                hdr["Cookie"] = self.cookie
            q = "" if self.cookie else ("?" + self.query if self.query else "")
            try:
                c.request(method, self.base + path + q, body=body, headers=hdr)
                r = c.getresponse()
                try:
                    data = decode(r) if decode is not None and r.status == 200 else r.read()
                except Exception:
                    c.close()
                    raise
                for h, v in r.getheaders():
                    if h.lower() == "set-cookie":
                        with self.lock:
                            self.cookie = "; ".join(x for x in [self.cookie, v.split(";")[0]] if x)
                self._put_back(c)
                if r.status in (301, 302, 303, 307, 308) or (r.status in (401, 403) and b"token" not in data[:200] and k == 0):
                    last = None  # redirects/authentication are not a prior connection failure
                    if r.status in (401, 403):  # (boat's cookie gone stale: its token again)
                        self.cookie = ""
                    retry.attempt(False)
                    notify("transport_reconnect" if retry.outcome == "trying" else "transport_error",
                           path=path, attempt=k + 1, error="AuthenticationRedirect")
                    continue
                retry.attempt(True)
                return r.status, data, r.getheader("Content-Type", "")
            except (OSError, http.client.HTTPException) as e:
                last = e
                with contextlib.suppress(Exception):
                    c.close()
                retry.attempt(False)
                notify("transport_reconnect" if retry.outcome == "trying" else "transport_error",
                       path=path, attempt=k + 1, error=type(e).__name__)
                if retry.outcome == "trying" or not strict:
                    time.sleep(0.2 * (k + 1))
        error_type = AgentTransportError if _transient_transport_error(last) else AgentError
        raise error_type(f"{self.vm}: the warm agent didn't answer ({last})") from last

    EXEC_OK = {"ping", "vtick", "killitem", "itemlogs", "begin", "prep", "tree", "have", "pull", "pullcache", "upgrade"}

    def rpc(self, cmd: str, *args, timeout: float = 60):
        args = [*args, {"_lease": {**self.lease, "owner": self.owner, "vm_id": self.vm}}] if cmd != "lease" else list(args)
        body = json.dumps({"cmd": cmd, "args": args}).encode()
        try:
            decode = None
            store = getattr(self, 'result_store', None)
            polling = cmd in ('tick', 'vtick') or (cmd == 'owned_rpc' and args
                and isinstance(args[0], dict) and args[0].get('command') in ('tick', 'vtick'))
            if store is not None and polling:
                decode = lambda stream: resultjson.load_response(stream, store)
            code, data, _ = self._req("POST", "/rpc", body, timeout, **({'decode': decode} if decode else {}))
            if code == 502 and cmd in self.EXEC_OK and len(body) < EXEC_BODY:
                raise AgentHTTPError(502, f"{self.vm}: {cmd}: 502 from boat's front")
        except AgentError:
            # (boat's front failing, the agent itself fine: the same request from inside the VM, over boat exec;
            # work on the VMs we hold goes on through an outage of the front, at an API call a request;
            # rules.boat.by_exec: only the replayable commands, under EXEC_BODY, sent in parts: B14)
            if not R.by_exec(True, cmd in self.EXEC_OK, len(body) < EXEC_BODY):
                raise
            code, data = self._rpc_exec(body, timeout, **({'decode': decode} if decode else {}))
        if code == 200 and isinstance(data, dict):
            return data
        if code != 200:
            raise AgentHTTPError(code, f"{self.vm}: {cmd}: {code} {data[:300]!r}")
        if data[:1] not in (b"{", b"["):
            raise AgentError(f"{self.vm}: {cmd}: {code} {data[:300]!r}")
        return json.loads(data)

    def _rpc_exec(self, body: bytes, timeout: float, decode=None):
        if decode is not None:
            return resultrelay.exec_response(self, body, timeout, decode)
        script = ("import base64,sys,urllib.request,os;t=sys.argv[2];a=sys.argv[1];"
                  "b=(open(a[1:]).read() if a.startswith('@') else a);a.startswith('@') and os.unlink(a[1:]);"
                  f"r=urllib.request.Request('http://127.0.0.1:{AGENT_PORT}/rpc',data=base64.b64decode(b),headers={{'X-SC-Token':t,'Content-Type':'application/json'}});"
                  f"sys.stdout.write(urllib.request.urlopen(r,timeout={int(timeout)}).read().decode())")
        b64 = base64.b64encode(body).decode()
        arg = b64
        if len(b64) > EXEC_ARG:  # (one argument past the kernel's 128 KB: "Argument list too long"; the body sent in parts to a file: B14)
            arg = f"@/tmp/sc-rpc-{os.getpid()}-{threading.get_ident()}-{int(time.time() * 1000)}.b64"
            for k in range(0, len(b64), EXEC_ARG):
                w = exec_(self.vm, ["bash", "-c", 'printf %s "$1" >> "$2"', "_", b64[k:k + EXEC_ARG], arg[1:]], 60)
                if w.get("exitCode") != 0:
                    raise AgentError(f"{self.vm}: the agent didn't answer, by boat's front nor by boat exec (its body's part: {str(w.get('stderr') or w.get('error'))[-160:]})")
        r = exec_(self.vm, ["python3", "-c", script, arg, self.token], int(timeout) + 15)
        if r.get("exitCode") != 0:
            raise AgentError(f"{self.vm}: the agent didn't answer, by boat's front nor by boat exec ({str(r.get('stderr') or r.get('error'))[-160:]})")
        return 200, r.get("stdout", "").encode()

    def raw(self, cmd: str, *args, timeout: float = 120) -> bytes:
        code, data, _ = self._req("POST", "/rpc", json.dumps({"cmd": cmd, "args": [*args, {"_lease": {**self.lease, "owner": self.owner, "vm_id": self.vm}}]}).encode(), timeout)
        if code != 200:
            raise AgentHTTPError(code, f"{self.vm}: {cmd}: {code} {data[:300]!r}")
        return data

    def put(self, path: str, data: bytes, timeout: float = 300) -> dict:
        code, out, _ = self._req("POST", path, data, timeout, "application/octet-stream")
        if code != 200:
            raise AgentHTTPError(code, f"{self.vm}: {path}: {code} {out[:300]!r}")
        return json.loads(out)

    def who(self, timeout: float = 8) -> dict:
        code, data, _ = self._req("GET", "/health", timeout=timeout)
        if code != 200:
            raise AgentHTTPError(code, f"{self.vm}: health: {code}")
        answer = json.loads(data)
        if not answer.get("lease"):
            raise AgentError(f"{self.vm}: lease protocol missing; old client wins")
        return answer

    def ping(self, timeout: float = 8) -> dict:
        r = self.rpc("ping", timeout=timeout)
        if not isinstance(r, dict) or not r.get("ok"):
            raise AgentError(f"{self.vm}: ping: {r!r}"[:300])
        return r

    def keep(self):
        """The cookie, with the URL and token (a file of ours, 0600), for the next process."""
        a = read_agent(self.vm) or {}
        if self.cookie and a.get("token") == self.token and a.get("cookie") != self.cookie:
            write_agent(self.vm, {**a, "cookie": self.cookie})


def agent_client(vm: str) -> AgentClient | None:
    a = read_agent(vm)
    return AgentClient(vm, a["url"], a["token"], a.get("cookie", "")) if a and a.get("url") and a.get("token") else None


# ------------------------------------------------------------------------------------------------------------- a batch
class StartGate:
    """boat's start limits (per minute, hour and day; a creation and a resume each one start), shared by a batch's
    resumes and creations: take(n) waits for the minute's room and grants up to n, 0 once the hour or the day is spent."""

    def __init__(self):
        self.lock = threading.Lock()
        self.t, self.used, self.st, self.room = 0.0, 0, None, 0
        self.rule = R.Gate()  # (its grants: rules.boat.Gate; `used` mirrors rule.used)

    def take(self, n: int, stop: threading.Event, urgent: bool | None = None) -> int:
        urgent = (PAID or URGENT) if urgent is None else urgent
        while not stop.is_set():
            with self.lock:
                if time.time() - self.t > 5:
                    try:
                        lim = limits()
                        self.lim, self.room = lim, lim["currentLimits"]["activeSandboxes"] - lim["activeSandboxes"]
                        self.st = effective(lim, False)
                        self.t, self.used = time.time(), 0
                        self.rule.refresh()
                    except Exception:
                        pass
                if self.st:
                    lim = getattr(self, "lim", None) or {"starts": self.st}
                    st = effective(lim, urgent) if urgent and "keepGoing" in lim else self.st
                    mine = ours()
                    left = starts_left(lim, mine, urgent)
                    if left - self.used <= 0:
                        return 0
                    # (paced: at most a sixth of what's left in any minute, and the minute's own limit, the wallet's
                    # 3x when paying; URGENT lifts the pace, never the limits; CEIL's minute a share of its limit)
                    k = self.rule.take(n, left, st["minute"]["remaining"], mine[60], URGENT, bool(CEIL), ceil_count(2, self.st["minute"]["limit"]) if CEIL else 0)
                    self.used = self.rule.used
                    if k > 0:
                        note_starts(k)
                        return k
            time.sleep(3)
        return 0


    def take_split(self, n: int, stop: threading.Event, urgent: bool | None = None) -> tuple[int, int]:
        """take(), and how many of the starts granted are paid (past the free limits' remaining, as last read)."""
        k = self.take(n, stop, urgent)
        with self.lock:
            st = self.st
            free_left = min(st["hour"]["remaining"], st["day"]["remaining"], st["minute"]["remaining"]) - (self.used - k) if st else k
        return k, R.paid_of(k, free_left)


class VM:
    def __init__(self, id: str, type: str, how: str):
        self.id, self.type, self.how = id, type, how
        self.cpus = TYPES[type][0]
        self.slots = max(1, self.cpus // SLOT_CPU)
        self.t_request = time.time()
        self.t_running = None
        self.t_ready = None
        self.t_end = None
        self.src = ""
        self.busy: list[list[float]] = []  # [start, end] per item
        self.dead = False
        self.released = False  # let go before the batch's end (nothing left for it): another batch may lease it
        self.skipped = False  # leased stopped, then not resumed: the batch no longer needed it
        self.http: AgentClient | None = None  # its warm agent, once it answers: ticks, logs and kills go there
        self.lease: dict = {}
        self.birth_nonce = ""
        self.letting_go: threading.Thread | None = None  # (the worker giving it back early: close() waits for it)


class Session:
    """The VMs a batch holds: leased from the pool (warm ones first, each with its agent answering), resumed, or
    created, each prepared with the batch's tree."""

    def __init__(self, label: str, idle: int | None = None, teardown: bool = False, vm_type: str = VM_TYPE, build: bool = True, oleans: bool = True,
                 root: pathlib.Path | None = None, tar: bytes | None = None, caches: bool | None = None):
        self.batch = time.strftime("%Y%m%d-%H%M%S") + "-" + "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(6))
        self.label, self.idle, self.teardown, self.vm_type = label, idle, teardown, vm_type
        self.proj = project()
        self.reg = Registry()
        self.build = build
        self.image = image_name()  # (checked against boat, and built, only when a VM has to be started: _image())
        self._image_ok = False
        self._image_lock = threading.Lock()
        # the tree: this checkout's, another directory's (a commit's worktree), or a tarball taken earlier (a submission)
        self.tree = Tree(root, tar)
        use = oleans if caches is None else caches
        self.caches = [{**c, "key": self.proj.cache_key(c, read=self.tree.read)} for c in self.proj.caches] if use else []
        for c in self.caches:
            cache_from_modal(c, c["key"])
        self._cache_tars: dict = {}
        self.cache_urls: dict[str, str] = {}
        self.ctx = {"name": self.proj.name, "path": self.proj.path, "mounts": self.proj.mounts, **({"env": self.proj.env} if getattr(self.proj, "env", None) else {}),
                    "caches": [{"name": c["name"], "key": c["key"], "dir": c["dir"], "outputs": self.proj.cache_outputs(c)} for c in self.caches]}
        self.ctx_b64 = base64.b64encode(json.dumps(self.ctx).encode()).decode()
        from . import agent as _agent_mod
        self.agent_version = _agent_mod.VERSION
        self.agent_b64 = base64.b64encode((HERE / "agent.py").read_bytes()).decode()
        self.vms: dict[str, VM] = {}
        self.ready: list[VM] = []
        self.cv = threading.Condition()
        self.stop = threading.Event()
        self.seed_url = None
        self.seed_vm = None
        self.seed_done = threading.Event()
        self.wanted = 0
        self.creating = 0
        self.t0 = time.time()
        self.threads: list[threading.Thread] = []
        self.counts = {"leased_running": 0, "warm": 0, "resumed": 0, "created": 0, "topped_up": 0, "not_resumed": 0, "failed": 0}
        self.gate = StartGate()
        self.last_error = ""
        self.max_starts = 0  # this batch's cap on its starts (--max-vms), else MAX_STARTS
        self.short = False  # no start was to be had for VMs the batch wanted: run() waits longer for released ones
        self.warm_only = False  # a tiny batch: VMs already up only, never a start
        # a VM with nothing left to take goes back to the pool at once, its logs collected: for a session of one run()
        # (run.py's batches, the treadmill's); fanout.BoatEx turns it on for its starmap, after its single calls
        self.early = False
        self.cap = 0  # at most this many VMs at once (a client that shares a fixed pool among its batches: the treadmill)
        self.min_left = 0  # starts only while more than this many are left (the treadmill leaves the rest to bars and lanes)
        self.logs: dict[str, dict] = {}
        self.collected: set[str] = set()
        self.day0 = None

        def day0():  # the account's starts of the day, before (and after, in close): off the batch's path
            with contextlib.suppress(Exception):
                self.day0 = limits()["starts"]["day"]["used"]
        self._day0 = threading.Thread(target=day0, daemon=True)
        threading.Thread(target=self._heartbeat, daemon=True).start()
        log(f"batch {self.batch}: {self.proj.name}, image {self.image}, tree {self.tree.id[:12]} ({len(self.tree.files)} files)"
            + "".join(f", cache {c['name']} {c['key'][:12]} ({len(cache_files(c, c['key']))} here)" for c in self.caches))

    def _image(self):
        """The image, checked (and built when allowed) before the batch starts or resumes a VM."""
        with self._image_lock:
            if not self._image_ok:
                self.image = ensure_image(self.build)  # (raises Unavailable when it's missing and build is off)
                self._image_ok = True
                if not self._day0.is_alive() and self.day0 is None:
                    self._day0.start()

    def cache_tar(self, c: dict) -> tuple[bytes | None, list[str]]:
        if c["name"] not in self._cache_tars:
            self._cache_tars[c["name"]] = cache_tarball(c, c["key"])
        return self._cache_tars[c["name"]]

    def cache_names(self, c: dict) -> list[str]:
        """The cache's files here for its key (no tarball made: that's for a VM that lacks some)."""
        return [f.name for f in cache_files(c, c["key"])]

    # -- acquiring
    def ensure(self, n: int, vm_type: str | None = None):
        """Asks for n VMs of a type in all (warm, leased, resumed or created); returns at once, VMs join as they're ready."""
        vm_type = vm_type or self.vm_type
        have = sum(1 for v in list(self.vms.values()) if v.type == vm_type and not v.dead and not v.released)
        need = n - have
        if need <= 0:
            return
        self.wanted += need
        need -= len(self._lease_warm(need, vm_type))
        if need <= 0:
            return
        try:
            self._image()
        except Unavailable:
            if self.vms:  # (warm VMs came: the batch runs on them)
                log("no more VMs: the image isn't built")
                self.wanted -= need
                return
            raise
        mine = []
        with self.reg.locked() as d:
            try:
                reconcile(d, sandboxes())
            except Exception as e:
                log(f"(reconcile skipped: {e})")
            refresh_old(d, self.image)
            retire(d, self.image)
            free = [i for i, v in d["vms"].items() if v["snapshot"] == self.image and v["type"] == vm_type and lease_free(v) and i not in self.vms]
            free.sort(key=lambda i: (d["vms"][i].get("state") not in RUNNING, -d["vms"][i].get("last_used", 0)))
            up = [i for i in free if d["vms"][i].get("state") in RUNNING]
            try:  # a stopped VM takes a start to resume: no more of them than the batch may start
                starts = max(0, starts_left(limits()))
            except Exception:
                starts = need
            # (how many up leased, stopped resumed, created: rules.boat.Ensure; each lease rules.boat.Entry.lease_stopped)
            yields = self.warm_only or starts <= self.min_left  # (a client that yields: no start while few are left)
            plan = R.Ensure()
            stopped = [i for i in free if i not in up]
            plan.plan(need, len(up), len(stopped), starts, self.warm_only, self.min_left, budget(0 if yields else starts, self.max_starts), OLD)
            if yields:
                starts = 0
            self.short = self.short or plan.short
            for i in up[:plan.up_leased] + stopped[:plan.resumed]:
                if entry(d["vms"][i]).lease_stopped(True):
                    d["vms"][i]["lease"] = {"pid": os.getpid(), "host": socket.gethostname(), "batch": self.batch, "since": time.time(), "label": self.label}
                    mine.append((i, d["vms"][i].get("state")))
        for i, state in mine:
            vm = VM(i, vm_type, "running" if state in RUNNING else "resumed")
            self.vms[i] = vm
            self._go(self._bring_up, vm, state in RUNNING)
        rest = plan.created  # (what the batch starts in all, its resumes and creations, stays within its budget)
        if rest > 0 and starts > 0:
            self._go(self._create, rest, vm_type)
        elif need > len(mine):
            log("no starts left this hour or day: no more VMs for this batch" if starts <= 0 else f"{need - len(mine)} VMs fewer: the batch's share of the starts left ({budget(starts, self.max_starts)} of {starts})")

    def _lease_warm(self, k: int, vm_type: str) -> list[str]:
        """Up to k VMs of the pool that are up, free, not about to stop and have a warm agent: leased with no boat
        API call (the agent answers for the VM)."""
        if not WARM or k <= 0 or self.stop.is_set():
            return []
        mine = []
        with self.reg.locked() as d:
            now = time.time()
            for i, v in sorted(d["vms"].items(), key=lambda x: -x[1].get("last_used", 0)):
                if len(mine) >= k:
                    break
                if entry(v).lease_warm(now, i not in self.vms and v["snapshot"] == self.image and v["type"] == vm_type, bool(read_agent(i))):  # (rules.boat.Entry.lease_warm)
                    v["lease"] = {"pid": os.getpid(), "host": socket.gethostname(), "batch": self.batch, "since": now, "label": self.label}
                    mine.append(i)
        for i in mine:
            vm = VM(i, vm_type, "warm")
            self.vms[i] = vm
            self._go(self._bring_up_warm, vm)
        return mine

    def lease_up(self, k: int, vm_type: str) -> int:
        """Leases up to k pool VMs that are up and free (released by another batch less than its idle time ago), so
        without a start; returns how many."""
        if k <= 0 or self.stop.is_set():
            return 0
        got = len(self._lease_warm(k, vm_type))
        if got:
            self.wanted += got
            self.counts["topped_up"] += got
        if got >= k:
            return got
        try:
            live = states()
        except Exception:
            return got
        mine = []
        with self.reg.locked() as d:
            now = time.time()
            for i, v in sorted(d["vms"].items(), key=lambda x: -x[1].get("last_used", 0)):
                if len(mine) >= k - got:
                    break
                if entry(v).lease_live(now, i not in self.vms and v["snapshot"] == self.image and v["type"] == vm_type, live.get(i, {}).get("state") in RUNNING):  # (rules.boat.Entry.lease_live)
                    v["lease"] = {"pid": os.getpid(), "host": socket.gethostname(), "batch": self.batch, "since": now, "label": self.label}
                    mine.append(i)
        for i in mine:
            vm = VM(i, vm_type, "running")
            self.vms[i] = vm
            self.wanted += 1
            self.counts["topped_up"] += 1
            self._go(self._bring_up, vm, True)
        if mine or got:
            log(f"{len(mine) + got} VMs another batch released join this one")
        return len(mine) + got

    def _go(self, f, *a):
        t = threading.Thread(target=f, args=a, daemon=True)
        t.start()
        self.threads.append(t)

    def _bring_up_warm(self, vm: VM):
        """A warm VM: one round trip to its agent (its version, the tree by id, the batch's directory), its TTL extended
        in the background; else the VM is brought up as any other that's up."""
        try:
            cl = agent_client(vm.id)
            if cl is None:
                raise AgentError("no agent recorded")
            self._claim_vm(vm)
            cl.lease = vm.lease
            t0 = time.time()
            r = cl.rpc("begin", self.agent_version, self.batch, self.tree.id, self.ctx_b64, timeout=60)
            if r.get("version") != self.agent_version:
                raise AgentError(f"its agent is version {r.get('version')}, not {self.agent_version}")
            vm.http = cl
            vm.t_running = time.time()
            self.counts["leased_running"] += 1
            self.counts["warm"] += 1
            self._go(lambda: boat("extend", vm.id, "--ttl", str(BACKSTOP), "--json", check=False))
            self._prepare_warm(vm, r.get("prep") if r.get("ok") else None, t0)
            threading.Thread(target=cl.keep, daemon=True).start()
        except Exception as e:
            if vm.t_ready:
                return
            if isinstance(e, LeaseLost):
                vm.dead = True
                self._release([vm.id])
                return
            log(f"{vm.id}: its warm agent: {str(e)[:200]}; by boat exec")
            vm.http = None
            vm.how = "running"
            if vm.t_running:
                self.counts["warm"] -= 1
                self.counts["leased_running"] -= 1
            vm.t_running = None
            self._bring_up(vm, True)

    def _prepare_warm(self, vm: VM, r: dict | None, t0: float):
        if not self.mine(vm.id):
            raise RuntimeError("no longer this batch's (it ended, or let the VM go)")
        cl = vm.http
        sent = 0
        if r is None:  # (a tree this VM hasn't seen: its manifest, the files it lacks, then the batch)
            r = cl.rpc("tree", self.tree.id, self.tree.files, self.tree.links, timeout=120)
            if not r.get("ok") and r.get("missing"):
                blob = self.tree.blobs(r["missing"])
                sent = len(blob)
                cl.put("/put", blob)
                r = cl.rpc("tree", self.tree.id, self.tree.files, self.tree.links, timeout=120)
            if not r.get("ok"):
                raise AgentError(f"tree: {str(r)[:300]}")
            r = cl.rpc("prep", self.batch, f"tree:{self.tree.id}", self.ctx_b64, self.tree.id, "", "-", timeout=300)
        if not r.get("ok"):
            raise AgentError(f"prep: {str(r)[:300]}")
        for c in self.caches:
            if set(self.cache_names(c)) - set(r.get("caches", {}).get(c["name"], [])):
                tar, _ = self.cache_tar(c)
                cl.put(f"/cache?name={urllib.parse.quote(c['name'])}&key={c['key']}", tar)
                sent += len(tar)
        vm.src = "warm"
        if env("VERBOSE"):
            log(f"{vm.id} (warm) prepared in {round(time.time() - t0, 2)} s, {sent // 1024} KB sent")
        self._ready(vm, r)

    def _bring_up(self, vm: VM, running: bool):
        try:
            if running:
                self.counts["leased_running"] += 1
                boat("extend", vm.id, "--ttl", str(BACKSTOP), "--json", check=False)
                up = wait_running([vm.id], 300 if OLD else 90)  # (one that's up answers in seconds)
            else:
                up = False
                res = R.Resume()  # (its tries, the starts boat refused given back, the ledger's count: rules.boat.Resume)
                while res.outcome == "trying":
                    # a VM still stopping (the last batch just let it go) takes a resume only once it has stopped
                    for _ in range(60):
                        if states().get(vm.id, {}).get("state") not in ("stopping", "pausing", "snapshotting"):
                            break
                        time.sleep(3)
                    enough = not OLD and res.tries == 0 and self.enough(vm)
                    if enough:
                        if res.attempt(True, False, "no", False) == "skipped":
                            # the VMs already up have slots for all that's left: this one isn't worth a start
                            self.counts["not_resumed"] += 1
                            vm.dead = vm.skipped = True
                            self._release([vm.id])
                            return
                    granted = bool(self.gate.take(1, self.stop))
                    if not granted:
                        res.attempt(False, False, "no", False)
                        self.counts["resumed"] += res.counted
                        raise RuntimeError("no boat starts left this hour or day (a resume is a start)")
                    vm.t_request = time.time()  # (billing starts at the resume, not at the wait for a start)
                    with _sem:
                        # A released VM keeps its 60s TTL unless resume replaces it atomically.
                        p = subprocess.run([BOAT, "resume", vm.id, "--ttl", str(BACKSTOP), "--json"], capture_output=True, text=True, timeout=300, env=ENV)
                    said = p.stdout + p.stderr
                    if p.returncode != 0 and ("rate_limited" in said or '"status":429' in said):
                        note_starts(-1)  # (refused: not a start)
                        hour = "per hour" in said or "per day" in said
                        res.attempt(False, True, "hour" if hour else "minute", False)
                        if hour:
                            self.counts["resumed"] += res.counted
                            raise RuntimeError("boat's start limit (a resume counts as a start): " + said.split('"error":"')[-1][:120])
                        time.sleep(15)  # the minute's starts: the next minute
                        continue
                    if p.returncode != 0 and R.resume_replaces(resume_missing(said), False):
                        # A direct 404 is authoritative, unlike an absent row in a paged list.
                        # No VM started: refund the grant and replace only while still ours.
                        note_starts(-1)
                        log(f"{vm.id}: boat no longer has it; a new VM instead")
                        vm.dead = True
                        if forget_missing(vm.id, self.batch) and not self.stop.is_set():
                            self._create(1, vm.type)
                        return
                    if p.returncode != 0 and R.resume_replaces(False, "snapshot_data_expired" in said):
                        res.attempt(False, True, "no", False)
                        self.counts["resumed"] += res.tries
                        # a stopped disk boat no longer keeps (14 days): the VM goes, a new one from the image takes its place
                        log(f"{vm.id}: its stopped disk expired; a new VM instead")
                        delete_vm(vm.id, vm.lease)
                        vm.dead = True
                        self._release([vm.id], deleted=True)
                        self._create(1, vm.type)
                        return
                    boat("extend", vm.id, "--ttl", str(BACKSTOP), "--json", check=False)
                    up = bool(wait_running([vm.id], 150))
                    res.attempt(False, True, "no", up)
                    if up:
                        break
                    log(f"{vm.id}: not up after resume ({said.strip()[-160:]}); again")
                self.counts["resumed"] += res.counted  # (every try a start: the ledger counts each)
            if not up:
                raise RuntimeError("never came up")
            vm.t_running = time.time()
            self._prepare(vm)
        except Exception as e:
            log(f"{vm.id}: {e}")
            self.last_error = f"{vm.id}: {str(e)[:200]}"
            vm.dead = True
            if not isinstance(e, LeaseLost) and vm.t_running and self.mine(vm.id):  # only our current lease may stop
                stop_vm(vm.id)
            self._release([vm.id])

    def _create(self, n: int, vm_type: str):
        """Creates n VMs within the plan's limits, as many per request as the minute allows, until enough or not useful."""
        self.creating += 1
        try:
            self._create1(n, vm_type)
        finally:
            self.creating -= 1

    def _create1(self, n: int, vm_type: str):
        made = R.Creation()  # (when to stop asking: rules.boat.Creation; the batch's counts carried in)
        made.created, made.failed, made.refunded = self.counts["created"], self.counts["failed"], self.counts["failed"]
        while n > 0 and not self.stop.is_set():
            if self.enough():
                made.made(0, 0, True)
                break
            try:
                lim = limits()
            except Exception:
                time.sleep(5)
                continue
            room = min(lim["currentLimits"]["activeSandboxes"] - lim["activeSandboxes"], n)
            if not lim.get("canStart", True) or room <= 0:
                time.sleep(10)
                continue
            room = self.gate.take(room, self.stop)
            if room <= 0:
                log("no starts left this hour or day: no more VMs for this batch")
                self.short = True
                break
            with new_vm_args(self.image) as (birth_args, birth_nonce):
                out = boat("new", "--from", self.image, "--type", vm_type, "--count", str(room), "--ttl", str(BACKSTOP), "--no-env", "--no-monitoring", *birth_args, "--json", timeout=600, check=False)
            ids = []
            for line in out.splitlines():
                with contextlib.suppress(Exception):
                    e = json.loads(line)
                    if e.get("id") and e["id"] not in ids and e.get("event") != "error":
                        ids.append(e["id"])
            again = made.made(room, len(ids), False)
            if len(ids) < room:  # (starts the gate granted that made no VM: not ours to count; the ledger has them as failed)
                note_starts(-(room - len(ids)))
                self.counts["failed"] += room - len(ids)
            if not ids:
                self.last_error = f"boat new: {out.strip()[-200:]}"
                if not again:  # boat refusing (3 times what it made): no creation loop
                    log("boat refuses creations: no more for this batch")
                    break
                log(f"boat new gave no VM: {out[-300:]}")
                time.sleep(15)
                continue
            if self.stop.is_set():  # the batch ended while they were made: into the pool, stopped after the idle time
                ttl = max(60, self.idle if self.idle is not None else IDLE)
                with self.reg.locked() as d:
                    for i in ids:
                        d["vms"][i] = {"type": vm_type, "snapshot": self.image, "created": time.time(), "last_used": time.time(), "state": "starting", "lease": None,
                                       "until": time.time() + ttl}
                for i in ids:
                    # No granted lease yet: keep the original birth TTL.
                    pass
                break
            with self.reg.locked() as d:
                for i in ids:
                    d["vms"][i] = {"type": vm_type, "snapshot": self.image, "created": time.time(), "last_used": time.time(), "state": "starting",
                                   "lease": {"pid": os.getpid(), "host": socket.gethostname(), "batch": self.batch, "since": time.time(), "label": self.label}}
            self.counts["created"] += len(ids)
            for i in ids:
                vm = VM(i, vm_type, "created")
                vm.birth_nonce = birth_nonce
                self.vms[i] = vm
                self._go(self._bring_up_new, vm)
            n -= len(ids)

    def _bring_up_new(self, vm: VM):
        try:
            if not wait_running([vm.id], 600):
                raise RuntimeError("never came up")
            vm.t_running = time.time()
            self._prepare(vm)
        except Exception as e:
            log(f"{vm.id}: {e}")
            self.last_error = f"{vm.id}: {str(e)[:200]}"
            vm.dead = True
            if not isinstance(e, LeaseLost) and self.mine(vm.id):
                stop_vm(vm.id, vm.lease)
            self._release([vm.id])

    def mine(self, vm_id: str) -> bool:
        """Whether the registry still has the VM leased to this batch (a late thread of a batch that has ended must
        not touch a VM another batch leased since)."""
        l = self.reg.read()["vms"].get(vm_id, {}).get("lease") or {}
        return l.get("batch") == self.batch and not self.stop.is_set()

    def enough(self, without=None) -> bool:
        """Whether the run under way has slots for all it still has to start (no more VMs worth starting), counting
        the session's VMs but `without`."""
        f = getattr(self, "_enough", None)
        return bool(f and f(without))

    # -- preparing (a VM without a warm agent: the tree as a tarball, from the seed VM's URL or from here)
    def _prepare(self, vm: VM):
        self._claim_vm(vm)
        if not self.mine(vm.id):
            raise RuntimeError("no longer this batch's (it ended, or let the VM go)")
        with self.cv:
            seed = self.seed_vm is None and self.wanted > 2
            if seed:
                self.seed_vm = vm
        base = f"{VM_ROOT}/in-{self.batch}.tar.gz"
        src = base
        tars = [(c, *self.cache_tar(c)) for c in self.caches if self.cache_names(c)]
        tars = [(c, t, n) for c, t, n in tars if t]
        if not seed and exec_(vm.id, ["test", "-f", f"{VM_ROOT}/tarballs/{self.tree.id}.tar.gz"], 30).get("exitCode") == 0:
            src = f"cache:{self.tree.id}"  # (this VM unpacked this very tree before: no upload)
        elif seed or self.wanted <= 2:
            try:
                if seed and tars:  # the tree and the caches in one upload (each ssh session costs seconds to open)
                    buf = io.BytesIO()
                    with tarfile.open(fileobj=buf, mode="w") as tf:
                        for name, data in [(f"in-{self.batch}.tar.gz", self.tree.tar), *[(f"cache-{c['name']}-{self.batch}.tar.gz", t) for c, t, _ in tars]]:
                            ti = tarfile.TarInfo(name)
                            ti.size = len(data)
                            tf.addfile(ti, io.BytesIO(data))
                    self._upload(vm, f"{VM_ROOT}/bundle-{self.batch}.tar", buf.getvalue())
                    exec_json(vm.id, ["bash", "-c", f"cd {VM_ROOT} && tar xf bundle-{self.batch}.tar && rm bundle-{self.batch}.tar && echo '{{}}'"], 60)
                else:
                    self._upload(vm, src)
            except Exception:
                if seed:
                    self.seed_vm = None
                raise
            if seed:
                try:
                    self._serve(vm, [c for c, _, _ in tars])
                except Exception as e:
                    log(f"seed {vm.id} can't serve ({e}); each VM gets the tree from here")
                self.seed_done.set()
        else:
            self.seed_done.wait(900)
            if self.seed_url:
                src = self.seed_url
            else:
                self._upload(vm, base)
        from_seed = (src == self.seed_url or vm is self.seed_vm) and self.cache_urls
        csrc = {c["name"]: [self.cache_urls[c["name"]], n] for c, _, n in tars if c["name"] in self.cache_urls} if from_seed else {}
        csrc_b64 = base64.b64encode(json.dumps(csrc).encode()).decode() if csrc else "-"
        r = self._agent(vm, ["prep", self.batch, src, self.ctx_b64, self.tree.id, self.tree.id, csrc_b64], 600, ship=True)
        if not r.get("ok") and (src == self.seed_url or src.startswith("cache:")):  # the URL (or the cache) failed: straight from here
            log(f"{vm.id}: the seed's URL failed ({str(r.get('err'))[:200]}); the tree from here")
            src, csrc = base, {}
            self._upload(vm, src)
            r = self._agent(vm, ["prep", self.batch, src, self.ctx_b64, self.tree.id, self.tree.id, "-"], 600, ship=True)
        if not r.get("ok"):
            raise RuntimeError(f"prep failed: {r}")
        for c, t, names in tars:
            if set(names) - set(r.get("caches", {}).get(c["name"], [])) and c["name"] not in csrc:
                dst = f"{VM_ROOT}/cache-{c['name']}-{self.batch}.tar.gz"
                if vm is not self.seed_vm:
                    self._upload(vm, dst, t)
                self._agent(vm, ["cachein", c["name"], c["key"], dst], 120)
        vm.src = "seed" if src == self.seed_url else "direct"
        if env("VERBOSE"):
            log(f"{vm.id} ({vm.how}) running after {round(vm.t_running - vm.t_request, 1)} s, prepared {round(time.time() - vm.t_running, 1)} s later ({vm.src})")
        self._ready(vm, r)
        if WARM:  # its warm agent, for this batch's ticks and the next batch on this VM
            self._go(self._warm_agent, vm)

    def _ready(self, vm: VM, r: dict):
        vm.cpus = r.get("cpus") or vm.cpus
        vm.slots = max(1, vm.cpus // SLOT_CPU)
        vm.t_ready = time.time()
        with self.cv:
            self.ready.append(vm)
            self.cv.notify_all()

    def _claim_vm(self, vm: VM):
        if vm.lease:
            return
        old = (read_agent(vm.id) or {}).get("lease") or {}
        if old and vm.how != "resumed":
            try:
                # claim is idempotent for our held lease, and reopens our
                # released lease with a fresh epoch for the next local batch.
                reply = lease_vm(vm.id, "claim", old)
            except Exception as exc:
                raise LeaseLost(f"{vm.id}: lease protocol unavailable; leaving VM alone") from exc
            if not reply.get("ok") or reply.get("stale"):
                raise LeaseLost(f"{vm.id}: lease lost; leaving VM alone")
            vm.lease = {**reply, "owner": owner_id(), "vm_id": vm.id}
        else:
            token = (read_agent(vm.id) or {}).get("token") or owner_key()
            reply = bootstrap_agent(vm.id, self.agent_b64, token, nonce=vm.birth_nonce)
            if not reply.get("ok") or reply.get("stale"):
                raise LeaseLost(f"{vm.id}: claim refused; leaving running items alone")
            vm.lease = (read_agent(vm.id) or {}).get("lease") or {}
            write_agent(vm.id, {**(read_agent(vm.id) or {}), "token": token})
        remember_lease(vm.id, {**vm.lease, "ok": True})
        if vm.how != "created":
            rename_leased_vm(vm.id, self.image, vm.lease)

    def _warm_agent(self, vm: VM):
        """The VM's warm agent started (the agent.py the batch shipped, a token of ours) and its boat URL recorded; the
        batch's ticks go there from now on."""
        try:
            tok = (read_agent(vm.id) or {}).get("token") or owner_key()
            r = bootstrap_agent(vm.id, self.agent_b64, tok, 60, 2, lease=vm.lease)
            if not r.get("ok"):
                raise AgentError(str(r)[:300])
            old = read_agent(vm.id) or {}
            url = old.get("url")
            if not url:
                url = bj("host", vm.id, str(AGENT_PORT), "--localhost", "--private")["url"]
            cl = AgentClient(vm.id, url, tok)
            try:
                cl.ping()
            except AgentError:
                if not old.get("url"):
                    raise
                url = bj("host", vm.id, str(AGENT_PORT), "--localhost", "--private")["url"]
                cl = AgentClient(vm.id, url, tok)
                cl.ping()
            write_agent(vm.id, {**(read_agent(vm.id) or {}), "url": url, "token": tok, "t": time.time(), "version": self.agent_version, "lease": vm.lease})
            if not vm.dead and not vm.released:
                vm.http = cl
        except Exception as e:
            log(f"{vm.id}: no warm agent ({str(e)[:200]})")

    def _agent(self, vm: VM, args: list[str], timeout: int = 60, ship: bool = False, tries: int = 4, sem=None):
        if ship:
            result = ship_agent(vm.id, self.agent_b64, [], timeout, tries, sem, lease=vm.lease)
            if result.get("stale"):
                raise LeaseLost(f"{vm.id}: stale install fence")
        cmd, *values = args
        if cmd == "tick":
            values = [values[0], json.loads(base64.b64decode("".join(values[1:])))]
        request = {"cmd": cmd, "args": [*values, {"_lease": vm.lease}]}
        payload = base64.b64encode(json.dumps(request).encode()).decode()
        result = exec_json(vm.id, ["python3", f"{VM_ROOT}/agent.py", "rpc", payload], timeout, tries, sem)
        if result.get("stale"):
            raise LeaseLost(f"{vm.id}: stale lease")
        return result

    def _tick(self, vm: VM, msg: dict) -> dict:
        if vm.http:
            try:
                reply = vm.http.rpc("tick", self.batch, msg, timeout=60)
                if reply.get("stale"):
                    raise LeaseLost(f"{vm.id}: stale lease")
                return reply
            except Exception as e:
                if isinstance(e, LeaseLost):
                    raise
                log(f"{vm.id}: its warm agent ({str(e)[:160]}): by boat exec")
                vm.http = None
        b64 = base64.b64encode(json.dumps(msg).encode()).decode()
        parts = [b64[k:k + 100_000] for k in range(0, len(b64), 100_000)]
        return self._agent(vm, ["tick", self.batch, *parts], 60, tries=1, sem=_tick_sem)

    def _upload(self, vm: VM, dst: str, data: bytes | None = None):
        data = self.tree.tar if data is None else data
        err, retry = "", R.Retry(4)  # (4 tries; done only where the VM holds every byte: rules.boat.Retry)
        while retry.outcome == "trying":
            k = retry.tries
            try:
                with _sem:
                    p = subprocess.run([BOAT, "ssh", vm.id, "--", f"mkdir -p {VM_ROOT} && cat > {dst}.part && mv {dst}.part {dst} && wc -c < {dst}"], input=data, capture_output=True, timeout=600, env=ENV)
                if retry.attempt(p.returncode == 0 and p.stdout.decode().strip().endswith(str(len(data)))) == "ok":
                    return
                err = p.stderr.decode()[-300:]
            except Exception as e:
                err = repr(e)
                retry.attempt(False)
            time.sleep(2 + 3 * k)
        raise RuntimeError(f"upload to {vm.id} failed: {err}")

    def _serve(self, vm: VM, caches: list[dict]):
        self.tok = secrets.token_hex(16)
        port = 8765
        mv = "".join(f" && {{ [ ! -f {VM_ROOT}/cache-{c['name']}-{self.batch}.tar.gz ] || mv {VM_ROOT}/cache-{c['name']}-{self.batch}.tar.gz {VM_ROOT}/srv/{self.tok}/cache-{c['name']}.tar.gz; }}" for c in caches)
        exec_json(vm.id, ["bash", "-c", f"mkdir -p {VM_ROOT}/srv/{self.tok} && cp {VM_ROOT}/in-{self.batch}.tar.gz {VM_ROOT}/srv/{self.tok}/tree.tar.gz{mv} && echo '{{}}'"], 60)
        self._agent(vm, ["serve", f"{VM_ROOT}/srv", str(port)], 60, ship=True)
        h = bj("host", vm.id, str(port), "--localhost", "--private")
        u = urllib.parse.urlparse(h["url"])
        self.seed_url = urllib.parse.urlunparse(u._replace(path=f"/{self.tok}/tree.tar.gz"))
        for c in caches:
            self.cache_urls[c["name"]] = urllib.parse.urlunparse(u._replace(path=f"/{self.tok}/cache-{c['name']}.tar.gz"))
        # (the URL carries boat's token: it never goes to a log or a file)

    # -- running
    def run(self, items: list[dict], on_done, on_start=None, on_lost=None, max_wait: int = 0):
        """Runs items ({name, kind, weight, est, ...spec}) longest first over the session's VMs, as they come up.

        An item whose VM died (or whose runner vanished) runs again once on another VM; lost again, it goes to
        on_lost(item, reason) (else it's done with exit -1). With max_wait, if no VM is ready that many seconds after
        the start, or every VM is gone, what hasn't run goes to on_lost too."""
        from .rules.agent_capabilities import required_agent_capabilities
        for it in items:
            required_agent_capabilities(it)
            if "runtime" in it:
                raise Unavailable("runtime pins require the scheduler; refusing the legacy unverified path")
            if it.get("enforce_memory") is True:
                raise Unavailable("memory enforcement requires the scheduler; refusing the legacy unverified path")
        queue = sorted(items, key=lambda it: -it.get("est", 60))
        # (each item's fate: taken, lost and run again once with no fallback, finished exactly once: rules.boat.LocalItem)
        fates = {it.get("orig", it["name"]): R.LocalItem(on_lost is not None) for it in items}
        fate_of = lambda it: fates[it.get("orig", it["name"])]
        end = R.RunEnd()  # (when a run with no VM left revives, gives up or raises: rules.boat.RunEnd)
        lock = threading.Lock()
        left = {"n": len(items)}
        self._enough = lambda without=None: len(queue) <= sum(v.slots for v in list(self.vms.values()) if not v.dead and not v.released and v is not without)
        started_threads, active, revived = set(), set(), False
        all_done = threading.Event()
        t_start = time.time()
        if not items:
            return

        def take(vm: VM, free: int) -> list[dict]:
            """The longest items that fit the VM's free slots. A VM of a bigger type than the batch's own takes the
            items that need it first, and others only once none of those waits."""
            got = []
            with lock:
                bigs_wait = any(it.get("vm_type") == vm.type for it in queue)
                for it in list(queue):
                    if free <= 0:
                        break
                    if it["weight"] > free or it["weight"] > vm.slots:
                        continue
                    if bigs_wait and it.get("vm_type") != vm.type:
                        continue
                    queue.remove(it)
                    fate_of(it).take()
                    got.append(it)
                    free -= it["weight"]
            return got

        def finish(it: dict, row: dict | None, lost_reason: str = "", client=None):
            orig = {**it, "name": it.get("orig", it["name"])}
            try:
                if row is None:
                    if on_lost:
                        on_lost(orig, lost_reason)
                    else:
                        on_done(orig, {"name": orig["name"], "code": -1, "seconds": -1, "error": lost_reason})
                else:
                    row = self._download_results(row, lambda ref, offset, count: self._result_chunk(client, row.get('batch', self.batch), row['name'], ref, offset, count)) if client is not None else row
                    on_done(orig, {**row, "name": orig["name"]})
            finally:
                with lock:
                    left["n"] -= 1
                    if left["n"] <= 0:
                        all_done.set()

        def lose(it: dict, reason: str):
            """To on_lost (Modal) when there is one; else once more on another VM, under a name of its own there. With no worker left to take it
            (the others finished and let their VMs go), one comes back on a VM the batch still holds, else a VM is
            leased or started for it."""
            k = it.get("try", 0) + 1
            fate = fate_of(it)
            fate.done(True)
            if fate.state == "queued":  # (with a fallback, Modal takes it at once: boat is never retried in a loop)
                with lock:
                    queue.insert(0, {**it, "try": k, "orig": it.get("orig", it["name"]), "name": f"{it.get('orig', it['name'])}~r{k}"})
                log(f"{it.get('orig', it['name'])}: {reason}; once more on boat")
                held = [v for v in list(self.vms.values()) if not v.dead and not v.released]
                if not any(v.id in active for v in held):
                    back = [v for v in self.ready if v in held]
                    with self.cv:
                        if back:
                            started_threads.discard(back[0].id)
                            self.cv.notify_all()
                    if not back:
                        self._go(self.ensure, len(held) + 1)
            else:
                finish(it, None, reason)

        def worker(vm: VM):
            active.add(vm.id)
            try:
                work(vm)
            finally:
                active.discard(vm.id)

        def work(vm: VM):
            running: dict[str, tuple[dict, float]] = {}
            unsent: list[dict] = []  # starts whose tick got no answer: sent again (the agent ignores one it has)
            acks: list[str] = []
            fails = 0
            while not self.stop.is_set():
                free = vm.slots - sum(it["weight"] for it, _ in running.values())
                starts = take(vm, free) if free > 0 else []
                if not starts and not running and not unsent and (not acks or not queue):  # (acks alone at the end: not worth a round trip)
                    with lock:
                        empty = not queue
                    if empty:
                        if self.early and vm is not self.seed_vm and not vm.dead:
                            self._let_go(vm)
                        break
                    time.sleep(0.5 if vm.http else 2)  # what's left doesn't fit this VM now
                    continue
                now = time.time()
                for it in starts:
                    running[it["name"]] = (it, now)
                    if on_start:
                        on_start(it, vm)
                unsent += [{k: v for k, v in it.items() if k not in ("est", "weight", "vm_type", "meta", "try", "orig", "timing")} | {"cpus": it.get("cpus") or it["weight"] * SLOT_CPU} for it in starts]
                try:
                    r = self._tick(vm, {"start": unsent, "acks": acks, "wait": 20 if running else 0})
                    if env("VERBOSE") == "2":
                        log(f"{vm.id}: tick: {len(r.get('done', []))} done, {len(r.get('running', []))} running")
                    fails = 0
                    acks, unsent = [], []
                except Exception as e:
                    if isinstance(e, LeaseLost):
                        vm.dead = True
                        # Expected fencing is not an item/VM failure. Undo admission
                        # locally; never signal the old job or stop the new holder.
                        with lock:
                            for it, _ in running.values():
                                fate = fate_of(it)
                                fate.fenced()
                                queue.append(it)
                        return
                    fails += 1
                    log(f"{vm.id}: tick failed ({fails}): {str(e)[:200]}")
                    if "not found (404)" in str(e):  # the VM is gone (deleted): no use asking again
                        fails = 8
                    if fails >= 8:
                        vm.dead = True
                        log(f"{vm.id}: given up")
                        for name, (it, _) in list(running.items()):
                            lose(it, f"boat VM {vm.id} lost")
                        return
                    time.sleep(min(30, 3 * fails))
                    continue
                for row in r.get("done", []):
                    name = row["name"]
                    acks.append(name)
                    if name in running:
                        it, t = running.pop(name)
                        vm.busy.append([t, time.time()])
                        if row.get("lost"):
                            lose(it, f"its runner on {vm.id} vanished")
                        else:
                            fate_of(it).done(False)
                            finish(it, row, client=vm.client)

        def spawn_workers():
            while not all_done.is_set() and not self.stop.is_set():
                with self.cv:
                    for vm in self.ready:
                        if vm.id not in started_threads and not vm.released and not vm.dead:
                            started_threads.add(vm.id)
                            threading.Thread(target=worker, args=(vm,), daemon=True).start()
                    self.cv.wait(0.5)

        def give_up(reason: str):
            with lock:
                rest = list(queue)
                queue.clear()
            for it in rest:
                fate_of(it).give_up()
                finish(it, None, reason)

        def topup():
            """More work than slots: VMs other batches released (still up, so no start) join this one as they come,
            and, when the batch got fewer than it asked for because no start was left, its share once starts are
            back (asked at most once a minute, no more than it lacks)."""
            asked = 0.0
            while not all_done.wait(10) and not self.stop.is_set():
                with lock:
                    q = [it for it in queue if not it.get("vm_type")]
                alive = [v for v in list(self.vms.values()) if not v.dead and not v.released and v.type == self.vm_type]
                short = len(q) - sum(v.slots for v in alive)
                if short <= 0:
                    continue
                per = TYPES[self.vm_type][0] // SLOT_CPU
                want = math.ceil(short / per) if not self.cap else min(math.ceil(short / per), self.cap - len(alive))
                if want <= 0:
                    continue
                got = self.lease_up(want, self.vm_type)
                if not got and self.short and time.time() - asked > 60:
                    asked = time.time()
                    with contextlib.suppress(Exception):
                        k = min(budget(starts_left(limits()), self.max_starts), math.ceil(short / per))
                        if k > 0:
                            log(f"starts are back: {k} VMs more")
                            self.ensure(len(alive) + k)

        sp = threading.Thread(target=spawn_workers, daemon=True)
        sp.start()
        if not OLD:
            threading.Thread(target=topup, daemon=True).start()
        if max_wait and self.short and not self.vms:
            log(f"no start to be had: waiting up to --max-wait ({max_wait} s) for a VM another batch releases, then Modal")
        while not all_done.wait(0.25):
            if self.stop.is_set():
                raise KeyboardInterrupt
            # (no VM in hand, never one or every one let go or lost, items left and nothing coming up: one more VM
            # before giving up; rules.boat.RunEnd)
            held = any(not v.dead and not v.released for v in list(self.vms.values()))
            coming = any(t.is_alive() for t in self.threads)
            over = bool(max_wait) and time.time() - t_start > max_wait
            act = end.tick(bool(self.vms), bool(max_wait), held, bool(queue), coming, on_lost is not None, over, bool(self.ready))
            if act == "revive":
                revived = True
                self._go(self.ensure, 1)
                continue
            if max_wait and self.ready and time.time() - t_start > max_wait and not any(t.is_alive() for t in self.threads):
                # items that need a bigger VM than the batch's own, none of which came (no start for it): not on boat
                with lock:
                    have = {v.type for v in list(self.vms.values()) if not v.dead}
                    stuck = [it for it in queue if it.get("vm_type") and it["vm_type"] not in have]
                    for it in stuck:
                        queue.remove(it)
                for it in stuck:
                    fate_of(it).give_up()
                    finish(it, None, f"no boat {it['vm_type']} VM to be had" + (" (no starts left)" if self.short else ""))
            if act == "give_up" and over and not self.ready:
                give_up(f"no boat VM ready after {max_wait} s" + (" (no starts left; none released by another batch meanwhile)" if self.short else "")
                        + (f" ({self.last_error})" if self.last_error else ""))
                if not any(t.is_alive() for t in self.threads) or all_done.is_set():
                    break
            elif act == "give_up":
                give_up(("every boat VM lost" if self.vms else "no boat VM to be had") + (f" ({self.last_error})" if self.last_error else ""))
            elif act == "raise":
                raise Unavailable("no VM left to run on" + (f" ({self.last_error})" if self.last_error else ""))
        all_done.wait(5)
        self._enough = None

    @staticmethod
    def _result_chunk(client, bid, name, ref, offset, count):
        resultfiles.validate_ref(ref)
        chunk = client.rpc('resultpart', bid, name, ref['sha256'], offset, count, timeout=120)
        if not isinstance(chunk, dict) or chunk.get('offset') != offset or chunk.get('bytes') != ref['bytes'] or chunk.get('sha256') != ref['sha256']:
            raise ValueError('invalid result chunk receipt')
        data = base64.b64decode(chunk.get('data', ''), validate=True)
        if len(data) > count:
            raise ValueError('oversized result chunk')
        return data

    def _download_results(self, row, reader):
        directory = pathlib.Path(getattr(self, 'output_dir', config.HOME / 'results' / self.batch))
        return clientresults.download(row, reader, directory)

    def call(self, module: str, fn: str, args, extra: dict | None = None, limit: int = 600):
        """One function of a runner on one of the session's VMs (the first ready), its result."""
        out = {}
        self.run([{"name": f"call-{fn}-{int(time.time() * 1000)}", "kind": "py", "module": module, "fn": fn, "args": args, "limit": limit, "weight": 1, "est": 1, **(extra or {})}],
                 lambda it, row: out.update(row))
        if "result" in out:
            return out["result"]
        if 'result_ref' in out or 'row_ref' in out:
            return {k: v for k, v in out.items() if k in ('result_ref', 'result_file', 'row_ref', 'row_file', 'output_files_ref', 'output_files_dir', 'output_files_manifest')}
        raise RuntimeError(f"{fn} failed on boat: {str(out.get('error', out))[-2000:]}")

    # -- collecting and closing
    def collect(self) -> dict[str, dict]:
        """Every item's out and err (their tails, as Modal's runner keeps), by name (a VM let go early: collected then)."""
        ts = [threading.Thread(target=self._collect, args=(vm,)) for vm in self.ready if not vm.dead and vm.id not in self.collected]
        [t.start() for t in ts]
        [t.join() for t in ts]
        return self.logs

    def _collect(self, vm: VM):
        try:
            data = None
            if vm.http:
                with contextlib.suppress(Exception):
                    data = vm.http.raw("logs", self.batch, timeout=120)
            if data is None:
                first = self._agent(vm, ["collect", self.batch, "0"], 120)
                parts = [base64.b64decode(first["data"])]
                for k in range(1, first["parts"]):
                    parts.append(base64.b64decode(self._agent(vm, ["collect", self.batch, str(k)], 120)["data"]))
                data = b"".join(parts)
            with tarfile.open(fileobj=io.BytesIO(data)) as tf:
                for m in tf.getmembers():
                    name, f = m.name.rsplit("/", 1)
                    with self.cv:
                        self.logs.setdefault(name, {})[f] = tf.extractfile(m).read().decode("utf-8", "replace")
            self.collected.add(vm.id)
        except Exception as e:
            log(f"{vm.id}: collect failed: {e}")

    def _let_go(self, vm: VM):
        """A VM with nothing left to take in this batch (its items done, none queued): its logs collected, it goes
        back to the pool now (up for the idle time, or the declared demand's), so a batch still running takes it."""
        vm.letting_go = threading.current_thread()
        ttl = self.warm_ttls([vm], held=sum(1 for v in list(self.vms.values()) if not v.dead and v.t_end is None) + 1) if self.idle is None else {}
        t = ttl.get(vm.id, self.idle if self.idle is not None else IDLE)
        self._collect(vm)
        shrink_ttl(vm.id, max(t, 30), vm.lease)
        vm.released, vm.t_end = True, time.time()
        if env("VERBOSE") == "2":
            log(f"{vm.id}: let go")
        self._release([vm.id], idle=t, up={vm.id}, ttl=ttl)

    def harvest_caches(self):
        """The build caches this batch filled, from the VM that has the most of each, kept here so the next batch
        ships them to the VMs that lack them."""
        for c in self.caches:
            try:
                self._harvest(c)
            except Exception as e:
                log(f"cache {c['name']}: {str(e)[:200]}")

    def _harvest(self, c: dict):
        outs = self.proj.cache_outputs(c)
        have = len(cache_files(c, c["key"]))
        if have >= len(outs):
            return
        names: dict[str, list] = {}

        def ls(vm):
            with contextlib.suppress(Exception):
                if vm.http:
                    names[vm.id] = vm.http.rpc("cacheout", c["name"], c["key"], timeout=30)["names"]
                else:
                    names[vm.id] = self._agent(vm, ["cacheout", c["name"], c["key"], "-"], 30, tries=1)["names"]
        ts = [threading.Thread(target=ls, args=(vm,)) for vm in self.ready if not vm.dead]
        [t.start() for t in ts]
        [t.join(60) for t in ts]
        if not names:
            return
        best = max(names, key=lambda i: len(names[i]))
        if len(names[best]) <= have:
            return
        vm = self.vms[best]
        try:
            data = None
            if vm.http:
                with contextlib.suppress(Exception):
                    data = vm.http.raw("cacheout", c["name"], c["key"], "tar", timeout=300)
            if data is None:
                first = self._agent(vm, ["cacheout", c["name"], c["key"], "0"], 120)
                parts = [base64.b64decode(first["data"])]
                for k in range(1, first["parts"]):
                    parts.append(base64.b64decode(self._agent(vm, ["cacheout", c["name"], c["key"], str(k)], 120)["data"]))
                data = b"".join(parts)
            d = cache_local(c, c["key"])
            with tarfile.open(fileobj=io.BytesIO(data)) as tf:
                d.mkdir(parents=True, exist_ok=True)
                tf.extractall(d, filter="data")
            log(f"kept {len(names[best])} files of cache {c['name']} {c['key'][:12]} from {vm.id}")
        except Exception as e:
            log(f"{vm.id}: cache {c['name']}: {str(e)[:200]}")

    harvest_oleans = harvest_caches  # (gdp-wired's older name)

    def _heartbeat(self):
        while not self.stop.wait(60):
            for vm in list(self.vms.values()):
                if not vm.dead and vm.t_end is None and vm.lease:
                    with contextlib.suppress(Exception):
                        renewed = lease_vm(vm.id, "renew", vm.lease)
                        if not renewed.get("ok") or renewed.get("stale"):
                            vm.dead = True
                            continue
                        boat("extend", vm.id, "--ttl", str(BACKSTOP), "--json", check=False)

    def close(self, kill: bool = True) -> dict:
        """Kills what still runs, releases every VM (stopped after the idle time, or deleted on teardown), and returns
        the batch's figures: VMs, VM-seconds held, seconds busy, idle VM-seconds and boat's dollars."""
        self.stop.set()
        for _ in range(600):  # a creation request in flight: its VMs join the pool before we let go
            if self.creating == 0:
                break
            time.sleep(1)
        end = time.time() + 120  # (bring-ups in flight end first: none may prepare a VM after the batch let it go)
        for t in list(self.threads):
            t.join(timeout=max(0.0, end - time.time()))
        for v in list(self.vms.values()):  # (a VM being given back early: that ends first, so it isn't wound twice)
            if v.letting_go is not None and v.letting_go is not threading.current_thread():
                v.letting_go.join(timeout=60)
        vms = [v for v in list(self.vms.values()) if not v.skipped]
        keep = [v for v in vms if not v.released]  # (those let go early are the pool's already)
        idle = self.idle if self.idle is not None else IDLE
        self.idle_used = idle
        ttl = self.warm_ttls([v for v in keep if not v.dead]) if self.idle is None and not self.teardown else {}

        def wind(vm: VM):
            if kill and vm.t_ready and not vm.dead:
                done = False
                if vm.http:
                    with contextlib.suppress(Exception):
                        vm.http.rpc("kill", self.batch, timeout=30)
                        done = True
                if not done:
                    with contextlib.suppress(Exception):
                        self._agent(vm, ["kill", self.batch], 60)
            # The shared seed server/data belong to the VM; TTL cleans it.
            # Never name-kill a process another owner could now be using.
            vm.t_end = time.time()
            if self.teardown:
                if delete_vm(vm.id, vm.lease):
                    forget_agent(vm.id)
            elif ttl.get(vm.id, idle) <= 0:
                stop_vm(vm.id, vm.lease)
            else:
                shrink_ttl(vm.id, ttl.get(vm.id, idle), vm.lease)
        ts = [threading.Thread(target=wind, args=(vm,)) for vm in keep]
        [t.start() for t in ts]
        [t.join(timeout=400) for t in ts]
        self._release([v.id for v in keep], deleted=self.teardown, idle=idle, up={v.id for v in keep if not v.dead}, ttl=ttl)
        held = sum((v.t_end or time.time()) - v.t_request for v in vms if v.t_running)  # (a VM that never ran bills nothing)
        busy = sum(union(v.busy) for v in vms)
        ready_held = sum((v.t_end or time.time()) - v.t_ready for v in vms if v.t_ready)
        up = [v.t_running - v.t_request for v in vms if v.t_running]
        prep = [v.t_ready - v.t_running for v in vms if v.t_ready and v.t_running]
        starts = self.counts["created"] + self.counts["resumed"]
        day = None
        if starts:
            with contextlib.suppress(Exception):  # (every client's starts of the day, before and after; a rolling day can drop some meanwhile)
                self._day0.join(10) if self._day0.is_alive() else None
                day = f"{self.day0} to {limits()['starts']['day']['used']}" if self.day0 is not None else None
        s = {"vms": len(vms), **self.counts, "account_day": day, "to_running_s": [round(min(up), 1), round(sorted(up)[len(up) // 2], 1), round(max(up), 1)] if up else None,
             "prep_s": [round(min(prep), 1), round(sorted(prep)[len(prep) // 2], 1), round(max(prep), 1)] if prep else None,
             "tree_from_seed": sum(1 for v in vms if v.src == "seed"), "types": sorted({v.type for v in vms}), "held_s": round(held, 1), "ready_held_s": round(ready_held, 1),
             "busy_s": round(busy, 1), "idle_vm_s": round(ready_held - busy, 1), "bring_up_s": round(held - ready_held, 1),
             "idle_tail_s": 0 if self.teardown else sum(max(0, ttl.get(v.id, idle)) for v in vms if not v.dead), "idle_s": idle,
             "starts": starts, "kept_for_demand": len(ttl), "let_go_early": sum(1 for v in vms if v.released),
             "unused": sum(1 for v in vms if v.t_running and not v.busy), "project": self.proj.name}
        ledger(batch=self.batch, label=self.label, project=self.proj.name, starts=s["starts"], created=s["created"], resumed=s["resumed"], warm=s["leased_running"],
               agents=s["warm"], vms=len(vms), slots=sum(v.slots for v in vms if v.t_ready), types=s["types"], kept=len(ttl), early=s["let_go_early"], unused=s["unused"],
               failed=self.counts["failed"], wall_s=round(time.time() - self.t0, 1), acct=bool(starts))
        return s

    def warm_ttls(self, vms: list, held: int = 0) -> dict:
        """For a client that declared more work (demand()): how long each released VM stays up, LONG for as many as
        it held at once in the last LONG s (its recent demand) less those already kept up for it; the rest IDLE."""
        name, now = client(), time.time()
        with self.reg.locked() as d:
            dm = declared(d, name)
            if not dm:
                return {}
            dm["recent"] = [r for r in dm.get("recent", []) if r[0] > now - LONG] + [[round(now), held or len(vms)]]
            kept = sum(1 for v in d["vms"].values() if v.get("owner") == name and lease_free(v) and v.get("until", 0) > now + IDLE)
            k = max(0, max(n for _, n in dm["recent"]) - kept)
            return {v.id: R.release_ttl(True, IDLE, False) for v in vms[:k]}  # (rules.boat.release_ttl: LONG is IDLE)

    def _release(self, ids: list[str], deleted: bool = False, idle: int = 0, up: set | None = None, ttl: dict | None = None):
        """Lets go of the batch's VMs (only those still leased to it: one it gave back early may be another's now);
        `until` is when boat stops one left up, so another batch may lease it without a start before then."""
        now = time.time()
        with self.reg.locked() as d:
            for i in ids:
                v = d["vms"].get(i)
                if v is None:
                    continue
                e, t = entry(v, now, self.batch), (ttl or {}).get(i, idle)
                if v.get("lease") and v["lease"].get("batch") != self.batch:
                    e.lease = "theirs"  # (another batch's, even one whose holder died: reap's to release)
                if not e.release(now, int(t), bool(up and i in up), deleted):  # (rules.boat.Entry.release)
                    continue
                if deleted:
                    d["vms"].pop(i)
                else:
                    v["lease"] = None
                    v["last_used"] = now
                    v["until"] = e.until
                    v["owner"] = client() if i in (ttl or {}) else None
                    if up and i in up:
                        v["state"] = "running"

    def dollars(self, ids: list[str] | None = None) -> float:
        """What boat bills for this batch's VMs since it began (`boat usage`, every VM at once)."""
        return usage_since(ids or list(self.vms), self.t0 - 5)


HANDOFF_QUIET = 60  # (seconds a client asks again quietly while the scheduler doesn't answer: a handoff's window)


def image_pins(operation: str = "list", *, images: list[str] | None = None,
               kind: str | None = None, reference: str | None = None) -> dict:
    """Register packet/GO references or inspect pins on an existing controller.

    Call before publishing a packet/GO, and require the durable acknowledgement.
    This never starts a local controller and never deletes an image.
    """
    if operation not in ("list", "register"):
        raise ValueError("image pin operation must be list or register")
    msg = {}
    if operation == "register":
        if kind not in ("packet", "go") or not isinstance(reference, str) or not reference.strip():
            raise ValueError("registration requires kind packet/go and a stable reference")
        if not isinstance(images, list) or not images:
            raise ValueError("registration requires a nonempty exact image list")
        msg = {"images": images, "kind": kind, "reference": reference}
    elif images is not None or kind is not None or reference is not None:
        raise ValueError("list does not accept registration fields")
    from . import scheduler
    conn = scheduler.current()
    if conn is None:
        raise Unavailable("no running scheduler; refusing local startup for image pins")
    caps = conn.req("/capabilities", {})
    if type(caps.get("image_pins_protocol")) is not int or caps["image_pins_protocol"] != 1:
        raise Unavailable("scheduler lacks durable image-pin capability")
    return conn.req("/image-pins/" + operation, msg)


def _validate_strict_options(strict_once: bool, max_attempts: int):
    from .rules.owned_lifecycle import attempt_cap_supported
    if type(strict_once) is not bool:
        raise ValueError("strict_once must be boolean")
    if type(max_attempts) is not int or not attempt_cap_supported(True, max_attempts):
        raise ValueError("max_attempts must be exactly the integer 1; only single-attempt opt-in is supported")


def _validate_durable_item(item: dict):
    """Validate opt-in fields without adding defaults to legacy job payloads."""
    if "distinct_vm_group_scope" in item:
        if item["distinct_vm_group_scope"] != "durable":
            raise ValueError("distinct_vm_group_scope must be durable when supplied")
        group = item.get("distinct_vm_group")
        if not isinstance(group, str) or not group.strip():
            raise ValueError("durable scope requires a nonblank distinct_vm_group")
    if "distinct_vm_group_generation" in item:
        generation = item["distinct_vm_group_generation"]
        if item.get("distinct_vm_group_scope") != "durable":
            raise ValueError("distinct_vm_group_generation requires durable scope")
        if not isinstance(generation, str) or not generation.strip():
            raise ValueError("distinct_vm_group_generation must be a nonblank string")


def _require_durable_capability(caps: dict):
    if not (caps.get("distinct_vm_group_scope") == "durable"
            and caps.get("durable_distinct_vm_group_protocol") == 1):
        raise Unavailable("scheduler lacks durable distinct_vm_group capability; refusing unconstrained submission")


def distinct_group(operation: str, group: str, *, generation: str | None = None, ttl_s: float | None = None) -> dict:
    """Manage durable group metadata on an existing scheduler, never starting a local daemon."""
    if operation not in ("open", "get", "close"):
        raise ValueError("distinct group operation must be open, get or close")
    if not isinstance(group, str) or not group.strip():
        raise ValueError("group must be a nonblank string")
    msg = {"group": group}
    if operation == "close":
        if not isinstance(generation, str) or not generation.strip():
            raise ValueError("close requires a nonblank generation")
        if isinstance(ttl_s, bool) or not isinstance(ttl_s, (int, float)) or not math.isfinite(ttl_s) or ttl_s < 0:
            raise ValueError("close requires finite ttl_s >= 0")
        msg.update(generation=generation, ttl_s=ttl_s)
    elif generation is not None or ttl_s is not None:
        raise ValueError("generation and ttl_s are only valid for close")
    from . import scheduler
    conn = scheduler.current()
    if conn is None:
        raise Unavailable("no running scheduler; refusing local startup for durable group operation")
    _require_durable_capability(conn.req("/capabilities", {}))
    return conn.req("/distinct-groups/" + operation, msg)


def distinct_groups_main(argv=None) -> int:
    """JSON CLI: python3 -m supercomputer.boat distinct-groups open|get|close GROUP."""
    import argparse
    parser = argparse.ArgumentParser(prog="python3 -m supercomputer.boat distinct-groups")
    parser.add_argument("operation", choices=("open", "get", "close"))
    parser.add_argument("group")
    parser.add_argument("--generation")
    parser.add_argument("--ttl-s", type=float)
    args = parser.parse_args(argv)
    try:
        result = distinct_group(args.operation, args.group, generation=args.generation, ttl_s=args.ttl_s)
    except (ValueError, Unavailable) as exc:
        result = {"ok": False, "error": str(exc)}
    print(json.dumps(result, sort_keys=True))
    return 1 if result.get("ok") is False else 0

class Scheduled(Session):
    """A batch through the scheduler (scheduler.py): its items submitted with a priority class, its rows back as they
    finish; the scheduler holds the VMs. The same interface as Session for its callers (boat_jobs, BoatEx, the
    treadmill's batches)."""

    scheduled = True

    def __init__(self, label: str, klass: str | int | None = None, strict_once: bool = False, max_attempts: int = 1, **kw):
        _validate_strict_options(strict_once, max_attempts)
        self.strict_once, self.max_attempts = strict_once, max_attempts
        self.custody = {}
        self._custody_lock = threading.Lock()
        self._custody_hashes = {}
        self._admission_lock = threading.Lock()
        self._strict_items = {}
        self._strict_placements = {}
        super().__init__(label, **kw)
        from . import scheduler
        self.sched = scheduler
        self.klass = scheduler.klass(klass)
        self.vm_ids: set[str] = set()
        self.run_names: dict[str, str] = {}
        self.client = None
        self.result = {}
        self.bids: list[str] = []  # (every batch id this session's runs submitted: a fresh one per run)
        self._bid_lock = threading.Lock()

    def ensure(self, n: int, vm_type: str | None = None):
        return

    def lease_up(self, k: int, vm_type: str) -> int:
        return 0

    def _image_for_scheduler(self):
        """The scheduler starts VMs of this image but never builds one: a batch for an image boat doesn't have waited,
        silently, for VMs nobody could start (11-13: a new worktree's batches queued for an hour). So it is checked
        here and built when missing, as `supercomputer image --build` would (SUPERCOMPUTER_NO_BUILD=1: the batch
        stops with the reason instead). Boat's API unreachable: left to the scheduler, which reports that itself."""
        if getattr(self, "_image_ok", False) or not getattr(self, "image", None):
            return  # (checked already; or a session made without __init__, as tests make them: nothing to check)
        try:
            ready = named_snapshots(60).get(self.image, {}).get("status") == "ready"
        except Exception as e:
            log(f"(image check skipped: {str(e)[:160]})")
            return
        if not ready:
            if env("NO_BUILD"):
                raise Unavailable(f"boat image {self.image} isn't built: supercomputer image --build (SUPERCOMPUTER_NO_BUILD is set)")
            log(f"image {self.image} isn't built yet (a new repo or toolchain): building it now, about a minute, once")
            with image_build_lock(self.image), getattr(self, "_image_lock", None) or threading.Lock():
                self.image = ensure_image(True)
        self._image_ok = True

    def _custody_event(self, bid: str, event: str, **details):
        # Full history is durable; only the last 64 events live in result rows.
        # The digest binds ALL bytes, not just the visible tail. No payload/env/token
        # or raw transport response is written to this journal.
        if not bid or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-" for c in bid) or bid.startswith("."):
            raise ValueError("invalid custody batch id")
        if not hasattr(self, "_custody_lock"):
            self._custody_lock, self._custody_hashes, self.custody = threading.Lock(), {}, {}
        with self._custody_lock:
            directory = config.LIVE / "custody"
            missing_directories, parent = [], directory
            while not parent.exists():
                missing_directories.append(parent)
                parent = parent.parent
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            for created in reversed(missing_directories):
                fd = os.open(created.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            path = directory / f"{bid}.jsonl"
            journal = self.custody.get(bid)
            if journal is None:
                journal = {"batch": bid, "max_attempts": 1, "reconnect_count": 0,
                           "event_count": 0, "events": [], "file": str(path)}
                digest = hashlib.sha256()
                if path.exists():
                    with path.open("rb") as previous:
                        for line in previous:
                            old = json.loads(line)
                            digest.update(line)
                            journal["event_count"] += 1
                            journal["reconnect_count"] += old["event"] in ("reconnect", "transport_reconnect")
                            journal["events"] = (journal["events"] + [old])[-64:]
                self.custody[bid], self._custody_hashes[bid] = journal, digest
            record = {"event": event, "time": time.time(), "sequence": journal["event_count"] + 1, **details}
            data = (json.dumps(record, sort_keys=True) + "\n").encode()
            new_file = not path.exists()
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "ab") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            if new_file:
                fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            self._custody_hashes[bid].update(data)
            journal["sha256"] = self._custody_hashes[bid].hexdigest()
            journal["event_count"] += 1
            journal["reconnect_count"] += event in ("reconnect", "transport_reconnect")
            journal["events"] = (journal["events"] + [record])[-64:]
            return journal

    def _remember_placement(self, bid: str, placement: dict, identity=None, name=None):
        if not isinstance(placement, dict):
            return
        identity = identity or placement.get("identity")
        name = name or placement.get("name")
        keys = ("run_name", "vm", "boot_id")
        if (not isinstance(identity, str) or not identity or not isinstance(name, str)
                or type(placement.get("attempt")) is not int or placement["attempt"] != 1
                or any(not isinstance(placement.get(k), str) or not placement[k] for k in keys)):
            return
        if not hasattr(self, "_strict_placements"):
            self._strict_placements = {}
        record = {"name": name, "launch_identity": identity, "attempt": 1, **{k: placement[k] for k in keys}}
        known = self._strict_placements.setdefault(bid, {})
        if (placement.get("identity") is not None and placement["identity"] != identity) or (name in known and known[name] != record):
            if not known.get(name, {}).get('conflict') and not getattr(self, '_restoring_custody', False):
                self._custody_event(bid, 'placement_conflict', name=name)
            known[name] = {"conflict": True}
        else:
            changed = known.get(name) != record
            if changed and not getattr(self, "_restoring_custody", False):
                self._custody_event(bid, "placement_binding", placement=record)
            known[name] = record

    def _valid_close(self, bid: str, receipt, snapshot) -> bool:
        if not isinstance(receipt, dict):
            return False
        if (receipt.get("batch") != bid or type(receipt.get("max_attempts")) is not int or receipt["max_attempts"] != 1
                or any(receipt.get(k) is not True for k in ("ok", "complete", "cleanup_complete", "strict_once", "admission_closed", "journals_retained"))
                or receipt.get("pending") != []):
            return False
        names = getattr(self, "_strict_items", {}).get(bid, set())
        attempts, unlaunched = receipt.get("attempts"), receipt.get("unlaunched")
        if not isinstance(attempts, list) or not isinstance(unlaunched, list) or not all(isinstance(n, str) for n in unlaunched):
            return False
        if any(not isinstance(a, dict) for a in attempts):
            return False
        actual = [a.get("name") for a in attempts]
        identities = [a.get("launch_identity") for a in attempts]
        if (not all(isinstance(n, str) for n in actual) or len(actual + unlaunched) != len(set(actual + unlaunched))
                or set(actual + unlaunched) != names
                or not all(isinstance(i, str) and i for i in identities) or len(identities) != len(set(identities))):
            return False
        snapshot_ok = (isinstance(snapshot, dict) and snapshot.get("ok") is True and snapshot.get("batch") == bid
                       and snapshot.get("strict_once") is True and type(snapshot.get("max_attempts")) is int and snapshot["max_attempts"] == 1)
        if snapshot_ok and (not isinstance(snapshot.get("placements"), list)
                            or any(not isinstance(p, dict) for p in snapshot["placements"])):
            return False
        if snapshot_ok:
            for placement in snapshot["placements"]:
                self._remember_placement(bid, placement)
        known = getattr(self, "_strict_placements", {}).get(bid, {})
        if not snapshot_ok and any(name not in known for name in names):
            return False
        if any(name in known for name in unlaunched):
            return False
        for attempt in attempts:
            expected = known.get(attempt["name"])
            if (type(attempt.get("attempt")) is not int or attempt["attempt"] != 1
                    or not expected or expected.get("conflict") or attempt.get("cleanup_complete") is not True
                    or any(attempt.get(k) != v for k, v in expected.items())):
                return False
        return True

    def _strict_row(self, bid: str, row: dict) -> dict:
        # Share the bounded journal so close/reconnect evidence remains visible to
        # Python callers holding rows; preserve controller custody independently.
        custody = self.custody[bid]
        self._remember_placement(bid, row.get("placement", {}), row.get("launch_identity"), row.get("name"))
        return {**row, **({"controller_custody": row["custody"]} if "custody" in row else {}),
                "batch": bid, "strict_once": True, "max_attempts": 1, "custody": custody}

    @contextlib.contextmanager
    def _transport_scope(self, bid: str, client=None):
        c = self.client if client is None else client
        with _CUSTODY_TRANSPORT_LOCK:
            observers = vars(c).get("_custody_observers")
            if observers is None:
                local, previous = threading.local(), getattr(c, "transport_event", None)
                def dispatch(event, **details):
                    stack = getattr(local, "stack", [])
                    callback = stack[-1] if stack else previous
                    if callback:
                        callback(event, **details)
                observers = {"local": local, "dispatch": dispatch}
                c._custody_observers = observers
                c.transport_event = dispatch
        def record(event, **details):
            try:
                return self._custody_event(bid, event, **details)
            except Exception as exc:
                raise CustodyError("transport custody persistence failed") from exc
        local = observers["local"]
        if not hasattr(local, "stack"):
            local.stack = []
        local.stack.append(record)
        try:
            yield
        finally:
            local.stack.pop()

    def _request(self, bid: str, path: str, msg, **kw):
        client = self.client
        if bid in getattr(self, "custody", {}):
            with self._transport_scope(bid, client):
                return client.req(path, msg, **kw)
        return client.req(path, msg, **kw)

    def run(self, items: list[dict], on_done, on_start=None, on_lost=None, max_wait: int = 0, *,
            strict_once: bool | None = None, max_attempts: int | None = None):
        return self._run(items, on_done, on_start, on_lost, max_wait,
                         strict_once=strict_once, max_attempts=max_attempts)

    def _run(self, items: list[dict], on_done, on_start=None, on_lost=None, max_wait: int = 0, *,
             strict_once: bool | None = None, max_attempts: int | None = None):
        strict_once = getattr(self, "strict_once", False) if strict_once is None else strict_once
        max_attempts = getattr(self, "max_attempts", 1) if max_attempts is None else max_attempts
        _validate_strict_options(strict_once, max_attempts)
        if not hasattr(self, "_admission_lock"):
            self._admission_lock = threading.Lock()
        if getattr(self, "stop", None) is not None and self.stop.is_set():
            raise RuntimeError("session is closed; no new admission is permitted")
        if not items:
            return
        from .rules.agent_capabilities import required_agent_capabilities
        for it in items:
            _validate_durable_item(it)
            required_agent_capabilities(it)
        def next_bid():
            with self._bid_lock:  # one id per intentional run, never per reconnect
                bid = self.batch if not self.bids else time.strftime("%Y%m%d-%H%M%S") + "-" + "".join(random.choice("abcdefghijklmnopqrstuvwxyz0123456789") for _ in range(6))
                self.bids.append(bid)
                self.batch = bid
                return bid
        if strict_once:
            bid = next_bid()
            self.client_sha256 = getattr(self, "client_sha256", _CLIENT_SHA256)
            self._custody_event(bid, "preflight_attempt", client_sha256=self.client_sha256)
            self.custody[bid]["client_sha256"] = self.client_sha256
        self.client = self.client or self.sched.connect()
        if strict_once or any("cpu_model" in it or "distinct_vm_group" in it or required_agent_capabilities(it) for it in items):
            caps = self._request(bid, "/capabilities", {}) if strict_once else self.client.req("/capabilities", {})
            if strict_once:
                self.custody[bid]["controller_code"] = caps.get("code")
                self._custody_event(bid, "preflight_complete", client_sha256=self.client_sha256, controller_code=caps.get("code"))
            if strict_once and not (type(caps.get("strict_once_protocol")) is int and caps["strict_once_protocol"] == 1
                                    and "strict_once" in caps.get("submit_flags", [])):
                raise Unavailable("scheduler lacks strict_once_protocol:1; refusing a retrying submission")
            if any("runtime" in it for it in items) and (type(caps.get("runtime_pins_protocol")) is not int or caps["runtime_pins_protocol"] != 1):
                raise Unavailable("scheduler lacks runtime_pins_protocol:1; refusing unverified runtime pins")
            if any(it.get('enforce_memory') is True for it in items) and (type(caps.get('resource_limits_protocol')) is not int or caps['resource_limits_protocol'] != 1):
                raise Unavailable('scheduler lacks resource_limits_protocol:1; refusing unenforced memory')
            if any(required_agent_capabilities(it) for it in items) and caps.get("required_agent_capabilities_protocol") != 1:
                raise Unavailable("scheduler lacks required_agent_capabilities_protocol:1; refusing unverified agents")
            if any(it.get("distinct_vm_group_scope") == "durable" for it in items):
                _require_durable_capability(caps)
            if any("distinct_vm_group" in it for it in items) and not (caps.get("distinct_vm_group") is True and caps.get("distinct_vm_group_protocol") == 1):
                raise Unavailable("scheduler lacks distinct_vm_group capability; refusing unconstrained submission")
            if any("cpu_model" in it for it in items) and caps.get("cpu_model_exact") is not True:
                raise Unavailable("scheduler lacks exact cpu_model capability; refusing unconstrained submission")
        self._image_for_scheduler()
        by = {it["name"]: it for it in items}
        log(f"submission root: {getattr(self.tree, 'root', '<prebuilt tree>')} ({len(self.tree.files)} files, {len(self.tree.links)} links)")
        upload_client = self.client
        with self._transport_scope(bid, upload_client) if strict_once else contextlib.nullcontext():
            upload_client.push_tree(self.tree)
            upload_client.push_caches(self.caches, self.proj.cache_outputs)
        if not strict_once:
            bid = next_bid()
        msg = {"batch": bid, "klass": self.klass, "client": client(), "label": self.label, "image": self.image, "vm_type": self.vm_type,
               "tree": ({"id": self.tree.id} if not strict_once and getattr(self.client, "remote", None) and self.tree.id in self.client._known().get("trees", [])
                        else {"id": self.tree.id, "files": self.tree.files, "links": self.tree.links}) | {"gits": getattr(self.tree, "gits", {})}, "ctx_b64": self.ctx_b64,
               "caches": [{"name": c["name"], "key": c["key"], "local": str(cache_local(c, c["key"])), "outputs": self.proj.cache_outputs(c)} for c in self.caches],
               "on_lost": on_lost is not None, "max_wait": max_wait, "warm_only": self.warm_only, "timing": env("TIMING") not in ("", "0"),
               "vm_type_named": bool(getattr(self, "vm_named", False) or env("VM_NAMED")), "items": items}
        if strict_once:
            msg.update(strict_once=True, max_attempts=1, on_lost=False)
            self._custody_event(bid, "admission_attempt", items=sorted(by))
        if any("distinct_vm_group" in it for it in items):
            msg["placement_contract"] = 1
        def admit():
            with self._admission_lock:
                if self.stop.is_set():
                    raise RuntimeError("session is closed; no new admission is permitted")
                if not hasattr(self, "_legacy_attempted"):
                    self._legacy_attempted = set()
                self._legacy_attempted.add(bid)
                self.submitted = True
                response = self._request(bid, "/submit", msg)
                if isinstance(response, dict) and (response.get("missing") or response.get("ok") is False):
                    self._legacy_attempted.discard(bid)
                    self.submitted = bool(self._legacy_attempted or getattr(self, "_strict_attempted", ()))
                return response
        def submit():
            r = admit()
            if isinstance(r, dict) and r.get("missing"):
                self.client.push_tree(self.tree)
                r = admit()
            if isinstance(r, dict) and r.get("ok") is False:
                raise RuntimeError(f"the scheduler refused batch {bid}: {r.get('error')}")
        if strict_once:
            # An unanswered submit may already have launched work. Never submit it again:
            # reconnect/poll this admission, including after loss of the controller's reply.
            def attempt():
                with self._admission_lock:
                    if self.stop.is_set():
                        raise RuntimeError("session is closed; no new admission is permitted")
                    if not hasattr(self, "_strict_attempted"):
                        self._strict_attempted, self._strict_items, self._strict_placements = set(), {}, {}
                    self._strict_attempted.add(bid)
                    self._strict_items[bid] = set(by)
                    self.submitted = True
                    try:
                        response = self._request(bid, "/submit", msg)
                    except CustodyError:
                        raise
                    except Exception as exc:
                        self._custody_event(bid, "ambiguous_submit", error=type(exc).__name__)
                        return None
                    if isinstance(response, dict) and (response.get("missing") or response.get("ok") is False):
                        self._strict_attempted.discard(bid)
                        self.submitted = bool(self._strict_attempted or getattr(self, "_legacy_attempted", ()))
                    return response
            r = attempt()
            if isinstance(r, dict) and r.get("missing"):
                # Explicit pre-admission refusal, not an ambiguous transport failure.
                self._custody_event(bid, "missing_tree_refusal")
                with self._transport_scope(bid):
                    self.client.push_tree(self.tree)
                r = attempt()
            if isinstance(r, dict) and r.get("ok") is False:
                self._strict_attempted.discard(bid)
                self.submitted = bool(self._strict_attempted or getattr(self, "_legacy_attempted", ()))
                self._custody_event(bid, "admission_refused")
                raise RuntimeError(f"the scheduler refused batch {bid}: {r.get('error')}")
            if isinstance(r, dict) and r.get("ok") is True:
                self._custody_event(bid, "admission_ack")
        else:
            try:
                submit()
            except Exception:
                if "files" in msg["tree"]:
                    raise
                msg["tree"] = {"id": self.tree.id, "files": self.tree.files, "links": self.tree.links}  # (the controller lost it: whole)
                submit()
        self.submitted = True
        since, left, reread = 0, set(by), 0
        reports = R.Reports(len(left))  # (every item reported once, none dropped at the end: rules.boat.Reports)
        while left:
            if self.stop.is_set():
                raise KeyboardInterrupt
            try:
                r = self._request(bid, "/poll", {"batch": bid, "since": since, "wait": 15}, timeout=60)
                t_err, said = None, False
            except CustodyError:
                raise
            except Exception as e:  # (the scheduler restarting, the forward reconnecting: asked again, for up to 15 minutes)
                if strict_once:
                    self._custody_event(bid, "reconnect", error=type(e).__name__)
                t_err = locals().get("t_err") or time.time()
                if time.time() - t_err > 900:
                    raise
                # (a handoff: the old scheduler exits, the next one listens about a second later; quiet unless it lasts
                # past HANDOFF_QUIET, asked again every second meanwhile: a handoff is invisible to clients)
                if time.time() - t_err > HANDOFF_QUIET and not locals().get("said"):
                    said = True
                    log(f"the scheduler didn't answer for {time.time() - t_err:.0f} s ({str(e)[:120]}); asking again")
                time.sleep(1 if time.time() - t_err < HANDOFF_QUIET else 5)
                with contextlib.suppress(Exception):
                    if getattr(self.client, "remote", None):  # (the controller's: its again, never this machine's)
                        self.sched._remote_ok.clear()
                        self.client = self.sched._remote() or self.client
                    elif not strict_once and time.time() - t_err > 10 and self.sched.rules.local_recovery_allowed(self.sched.controller_configured()):
                        # Recover a crashed LOCAL owner only. A controller appearing
                        # during an old client's lifetime must never revive a rival.
                        self.client = self.sched.ensure_running()
                continue
            for row in r.get("rows", []):
                row = self._download_results(row, lambda ref, offset, count: self._request(bid, '/results', {'batch':bid, 'ref':ref, 'offset':offset, 'length':count}, timeout=120, raw=True))
                since += 1
                it = by.get(row["name"])
                if it is None or not reports.row(row["name"] in left):
                    continue
                left.discard(row["name"])
                if row.get("vm"):
                    self.vm_ids.add(row["vm"])
                    self.run_names[row.get("run_name", row["name"])] = row["name"]
                if row.get("lost") and on_lost and not strict_once and not it.get("cpu_model") and not required_agent_capabilities(it) and it.get("distinct_vm_group_scope") != "durable":
                    on_lost(it, row.get("error", "lost"))
                else:
                    on_done(it, self._strict_row(bid, row) if strict_once else row)
            if r.get("done") and not r.get("rows"):
                # (done with items unreported: their rows read again from the first, a few times, before they count as
                # lost; a scheduler before rules.Delivery said done with a row not yet delivered)
                if left and reread < 5:
                    reread += 1
                    if strict_once:
                        self._custody_event(bid, "terminal_rows_reread", attempt=reread)
                    since = 0
                    time.sleep(2)
                    continue
                break
        # (the scheduler done with items it never reported: reported now, lost, to the fallback or as failures)
        if reports.end():
            for name in sorted(left):
                why = "the scheduler finished the batch without its row"
                if on_lost and not strict_once and not required_agent_capabilities(by[name]) and by[name].get("distinct_vm_group_scope") != "durable":
                    on_lost(by[name], why)
                else:
                    row = {"name": name, "code": -1, "seconds": -1, "error": why}
                    if strict_once:
                        row.update(strict_once=True, status="LOST", terminal=True, identity_unavailable=True)
                        row = self._strict_row(bid, row)
                    on_done(by[name], row)
            left.clear()

    def collect(self) -> dict[str, dict]:
        if not self.client or not self.vm_ids:
            return self.logs
        for bid in list(self.bids) or [self.batch]:  # (every run's batch)
            strict_once = bid in getattr(self, "custody", {})
            try:
                retry = R.Retry(12)  # (through a restart of the scheduler: same admission)
                while True:
                    try:
                        data = self._request(bid, "/logs", {"batch": bid, "vms": sorted(self.vm_ids)}, timeout=600, raw=True)
                        retry.attempt(True)
                        break
                    except CustodyError:
                        raise
                    except Exception as exc:
                        if strict_once:
                            self._custody_event(bid, "reconnect", path="/logs", error=type(exc).__name__)
                        if retry.attempt(False) == "failed":
                            raise
                        time.sleep(5)
                with tarfile.open(fileobj=io.BytesIO(data)) as tf:
                    for m in tf.getmembers():
                        name, f = m.name.rsplit("/", 1)
                        self.logs.setdefault(self.run_names.get(name, name), {})[f] = tf.extractfile(m).read().decode("utf-8", "replace")
            finally:
                pass
        return self.logs

    def harvest_caches(self):
        """The caches the batch's VMs filled, kept here (from one of its VMs, by its warm agent; the controller's
        VMs aren't this machine's to reach)."""
        if self.client is not None and getattr(self.client, "remote", None):
            return
        for c in self.caches:
            outs = self.proj.cache_outputs(c)
            if len(cache_files(c, c["key"])) >= len(outs):
                continue
            for v in sorted(self.vm_ids):
                cl = agent_client(v)
                with contextlib.suppress(Exception):
                    if cl and len(cl.rpc("cacheout", c["name"], c["key"], timeout=30)["names"]) > len(cache_files(c, c["key"])):
                        data = cl.raw("cacheout", c["name"], c["key"], "tar", timeout=300)
                        d = cache_local(c, c["key"])
                        d.mkdir(parents=True, exist_ok=True)
                        with tarfile.open(fileobj=io.BytesIO(data)) as tf:
                            tf.extractall(d, filter="data")
                        break

    harvest_oleans = harvest_caches

    def close(self, kill: bool = True) -> dict:
        self.stop.set()
        if not hasattr(self, "_admission_lock"):
            self._admission_lock = threading.Lock()
        # Fence only admission, never wait for the polling loop. A submit already
        # in flight settles its ownership before we snapshot what must be reaped.
        with self._admission_lock:
            bids = list(getattr(self, "bids", None) or [self.batch])
            attempted = set(getattr(self, "_strict_attempted", set()))
            submitted = getattr(self, "submitted", False)
        r = {}
        strict_bids = set(getattr(self, "custody", {}))
        strict_once = bool(strict_bids) or getattr(self, "strict_once", False)
        receipts, validation = {}, {}
        if self.client and submitted:
            for bid in bids:
                if bid in strict_bids:
                    if bid not in attempted:
                        continue
                    self._custody_event(bid, "close_attempt")
                    try:
                        receipt = self._request(bid, "/close", {"batch": bid}, timeout=60)
                    except Exception as exc:
                        receipt = {"batch": bid, "complete": False, "cleanup_complete": False, "error": type(exc).__name__}
                    receipts[bid] = receipt
                    snapshot = None
                    try:
                        snapshot = self._request(bid, "/snapshot", {"batch": bid}, timeout=60)
                    except Exception as exc:
                        self._custody_event(bid, "snapshot_error", error=type(exc).__name__)
                    validation[bid] = self._valid_close(bid, receipt, snapshot)
                    self._custody_event(bid, "close_receipt" if validation[bid] else "close_failure",
                                        cleanup_complete=validation[bid],
                                        receipt_sha256=hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest())
                    continue
                # Legacy cleanup retry behavior is unchanged.
                retry = R.Retry(6)  # (rules.boat.Retry)
                while retry.outcome == "trying":
                    try:
                        x = self.client.req("/close", {"batch": bid}, timeout=60)
                        r = {k: (r.get(k, 0) + v if isinstance(v, (int, float)) and not isinstance(v, bool) else v) for k, v in x.items()} if r else x
                        retry.attempt(True)
                    except Exception:
                        retry.attempt(False)
                        time.sleep(5)
        z = {k: 0 for k in ("created", "resumed", "leased_running", "warm", "topped_up", "not_resumed", "failed", "held_s", "ready_held_s", "busy_s", "idle_vm_s",
                            "bring_up_s", "idle_tail_s", "kept_for_demand", "let_go_early", "unused", "tree_from_seed")}
        self.result = {**z, "vms": r.get("vms", len(self.vm_ids)), "starts": r.get("starts", 0), "preempted": r.get("preempted", 0), "scheduled": r.get("class", self.sched.NAMES[self.klass]),
                       "account_day": None, "types": [self.vm_type], "idle_s": IDLE, "project": self.proj.name}
        if strict_once:
            self.result.update(batches=receipts, custody=getattr(self, "custody", {}),
                               cleanup_validation=validation,
                               cleanup_complete=bool(receipts) and all(validation.values()))
        return self.result

    def recover_close(self, bid=None, *, timeout=30, attempts=3):
        """Bounded read-only validation of an earlier strict close; never cancel again."""
        bid = bid or self.batch
        if bid not in getattr(self, "_strict_items", {}) or not self._strict_items[bid]:
            raise ValueError("recovery requires the original strict admission item bindings")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0
                or type(attempts) is not int or not 1 <= attempts <= 16):
            raise ValueError("recovery needs a positive finite timeout and 1..16 attempts")
        deadline = time.monotonic() + timeout
        receipt, valid, made = {}, False, 0
        for attempt in range(1, attempts + 1):
            if time.monotonic() >= deadline:
                break
            made = attempt
            self._custody_event(bid, "close_recovery_attempt", attempt=attempt)
            snapshot = None
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                receipt = self._request(bid, "/close-receipt", {"batch": bid, "close_recovery": 1}, timeout=remaining)
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    try:
                        snapshot = self._request(bid, "/snapshot", {"batch": bid, "close_recovery": 1}, timeout=remaining)
                    except Exception as exc:
                        self._custody_event(bid, "close_recovery_snapshot_error", error=type(exc).__name__)
                valid = time.monotonic() <= deadline and self._valid_close(bid, receipt, snapshot)
            except Exception as exc:
                self._custody_event(bid, "close_recovery_error", error=type(exc).__name__)
            self._custody_event(bid, "close_recovery_receipt" if valid else "close_recovery_failure",
                                attempt=attempt, cleanup_complete=valid,
                                receipt_sha256=hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest())
            if valid:
                break
            remaining = deadline - time.monotonic()
            if attempt < attempts and remaining > 0:
                time.sleep(min(0.2 * attempt, remaining))
        report = {"batch": bid, "read_only": True, "cleanup_complete": valid,
                  "attempts": made, "receipt": receipt,
                  "exhausted": not valid}
        self._custody_event(bid, "close_recovery_complete", cleanup_complete=valid, attempts=made)
        if isinstance(getattr(self, "result", None), dict) and "cleanup_validation" in self.result:
            self.result["batches"][bid] = receipt
            self.result["cleanup_validation"][bid] = valid
            self.result["cleanup_complete"] = bool(self.result["cleanup_validation"]) and all(self.result["cleanup_validation"].values())
        return report

    def dollars(self, ids=None):
        return None


def recover_close(bid, *, client=None, timeout=30, attempts=3):
    """Resume from this client's durable custody, without constructing a session.

    Old journals lacking original item bindings are deliberately insufficient.
    The caller can still audit their retained receipts, but must not certify a
    different admission by accepting names supplied only by the controller.
    """
    from . import scheduler
    scheduler.Scheduler._valid_batch(bid)
    path = config.LIVE / "custody" / f"{bid}.jsonl"
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise ValueError("custody is not a regular file")
        events = []
        for sequence, line in enumerate(source, 1):
            event = json.loads(line)
            if not line.endswith(b'\n') or event.get('sequence') != sequence:
                raise ValueError("custody is torn or non-contiguous")
            events.append(event)
    admissions = [e for e in events if e.get('event') == 'admission_attempt']
    if (len(admissions) != 1 or not isinstance(admissions[0].get('items'), list)
            or not admissions[0]['items'] or any(not isinstance(n, str) for n in admissions[0]['items'])
            or len(set(admissions[0]['items'])) != len(admissions[0]['items'])
            or any(e.get('event') == 'admission_refused' for e in events)):
        raise ValueError("custody lacks one original strict admission binding")
    resumed = object.__new__(Scheduled)
    resumed.client = client if client is not None else scheduler.current()
    if resumed.client is None:
        raise RuntimeError("no running scheduler; recovery never starts one")
    resumed._strict_items = {bid: set(admissions[0]['items'])}
    resumed._strict_placements = {}
    resumed._restoring_custody = True
    for event in events:
        if event.get('event') == 'placement_binding':
            placement = event.get('placement', {})
            resumed._remember_placement(bid, placement, placement.get('launch_identity'))
        elif event.get('event') == 'placement_conflict':
            resumed._strict_placements.setdefault(bid, {})[event['name']] = {'conflict': True}
    resumed._restoring_custody = False
    return resumed.recover_close(bid, timeout=timeout, attempts=attempts)


def session(label: str, klass: str | int | None = None, strict_once: bool = False, max_attempts: int = 1, **kw) -> Session:
    """A batch's session; strict_once permits zero or one physical attempt, never a retry.

    Strict ownership requires a capable scheduler. Without it, fail rather than
    silently use the legacy pool or another provider.
    """
    _validate_strict_options(strict_once, max_attempts)
    from . import scheduler
    if strict_once and not scheduler.enabled():
        raise Unavailable("strict_once requires the scheduler; refusing the legacy retrying path")
    return Scheduled(label, klass=klass, strict_once=strict_once, max_attempts=max_attempts, **kw) if scheduler.enabled() else Session(label, **kw)


def scheduler_holds(image: str) -> bool:
    """Whether the scheduler holds a VM of this image now (an interactive batch gets a slot of it in seconds)."""
    from . import scheduler
    if not scheduler.enabled():
        return False
    with contextlib.suppress(Exception):
        c = scheduler.current()
        return bool(c and c.req("/can", {"image": image}, timeout=10).get("held"))
    return False


def usage_since(ids: list[str], t: float) -> float:
    since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
    got: list[float] = []

    def one(i):
        with contextlib.suppress(Exception):
            got.append(bj("usage", i, "--since", since)["dollars"])
    ts = [threading.Thread(target=one, args=(i,)) for i in ids]
    [x.start() for x in ts]
    [x.join(120) for x in ts]
    return round(sum(got), 5)


def union(iv: list[list[float]]) -> float:
    tot, end = 0.0, -1e18
    for a, b in sorted(iv):
        if b <= end:
            continue
        tot += b - max(a, end)
        end = b
    return tot


@contextlib.contextmanager
def signals_as_interrupt():
    """SIGTERM and SIGHUP end the batch as Ctrl-C does, so its finally runs (kill, release)."""
    def h(sig, frame):
        raise KeyboardInterrupt(f"signal {sig}")
    old = {s: signal.signal(s, h) for s in (signal.SIGTERM, signal.SIGHUP) if threading.current_thread() is threading.main_thread()}
    try:
        yield
    finally:
        for s, o in old.items():
            signal.signal(s, o)


# ----------------------------------------------------------------------------------------------------------- sizing
def plan_vms(items: list[dict], slots_per_vm: int, override: int = 0, cap: int = 0) -> int:
    """Enough VMs that the longest item bounds the batch (no more help), fewer when VMs come at the plan's start rate:
    with W slot-seconds of work, r starts a second and c slots a VM, n VMs arriving over n/r seconds finish by about
    W/(c n) + n/(2r), least at n = sqrt(2 r W / c). boat counts a resume as a start, as a creation is: only the pool's
    VMs still up (within their idle time) come without one. Of the starts left this hour and day, a batch takes its
    budget() (a quarter, at most MAX_STARTS or `cap`), so a big batch runs longer on fewer VMs (two slots each, longest first) instead of
    spending the hour; more VMs join it as other batches release theirs (Session.run's top-up)."""
    if override:
        return override
    if not items:
        return 0
    W = sum(it["weight"] * it.get("est", 60) for it in items)
    L = max(it.get("est", 60) for it in items)
    # (and a started VM must have work for a few times its bring-up, BRING_UP: the bar's TIP baseline on boat asked 50
    # VMs for 150 s of work each, brought up in 159 s on average, and 40% of their ready time was idle)
    useful = max(1, math.ceil(W / (slots_per_vm * max(L, 3 * BRING_UP))))
    useful = min(useful, sum(it["weight"] for it in items) // max(1, slots_per_vm) + 1, len(items))
    try:
        lim = limits()
        now = time.time()
        up = sum(1 for v in Registry().read()["vms"].values() if lease_free(v) and v.get("state") in RUNNING and v.get("until", now + 60) > now + 20)
        rate = max(lim["currentLimits"]["creationRatePerMinute"], 1) / 60
        by_rate = up + math.ceil(math.sqrt(2 * rate * W / slots_per_vm))
        room = lim["currentLimits"]["activeSandboxes"] - lim["activeSandboxes"] + up
        starts = max(0, starts_left(lim))
        n = R.plan_vms(useful, by_rate, room, up, budget(starts, cap), cap, True)  # (rules.boat.plan_vms)
        if n < min(useful, by_rate, room, up + starts):
            log(f"{up} VMs up and free, {budget(starts, cap)} starts of the {starts} left: {n} VMs (the old policy: {min(useful, by_rate, room, up + starts)})")
    except Exception:
        n = R.plan_vms(useful, 0, 0, 0, 0, cap, False)
    return n


def durations_index() -> pathlib.Path:
    return LIVE / "durations" / f"{project().name}.json"


def note_durations(rows: dict):
    """Each job's seconds, by name, for the next batch's order and sizing (LIVE/durations/<repo>.json)."""
    with contextlib.suppress(Exception):
        p = durations_index()
        p.parent.mkdir(parents=True, exist_ok=True)
        d = json.loads(p.read_text()) if p.exists() else {}
        d.update({n: r["seconds"] for n, r in rows.items() if isinstance(r.get("seconds"), (int, float)) and r["seconds"] >= 0})
        tmp = p.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(d))
        tmp.rename(p)


def last_durations(out: pathlib.Path, names: set[str], explicit: str = "") -> dict[str, float]:
    """Each job's seconds in the last run that has them: --durations, else <out>/summary.tsv, else the repo's index
    of every run's times (LIVE/durations), else the newest summary.tsv under runs/ (here and in the main checkout)
    sharing most of the names (a search of runs/ only when the index lacks most of them)."""
    def tsv(c: pathlib.Path) -> dict:
        try:
            rows = [l.split("\t") for l in c.read_text().splitlines() if l.strip()]
            return {r[0]: float(r[2]) for r in rows if len(r) >= 3 and float(r[2]) >= 0}
        except Exception:
            return {}
    # (the source: rules.boat.durations_source, each only where it covers at least half the jobs; a tiny batch
    # searches no runs: its order hardly matters)
    half = lambda d: bool(d) and 2 * len(names & set(d)) >= len(names)
    ex = tsv(pathlib.Path(explicit)) if explicit else {}
    o = tsv(out / "summary.tsv")
    ix = {}
    with contextlib.suppress(Exception):
        ix = json.loads(durations_index().read_text())
    src = R.durations_source(half(ex), half(o), half(ix), len(names), True)
    if src != "search":
        return {"explicit": ex, "out": o, "index": ix}.get(src, {})
    found = []
    for root in {project().root / "runs", config.main_checkout(project().root) / "runs"}:
        if root.exists():
            found += list(root.glob("**/summary.tsv"))
    found.sort(key=lambda p: -p.stat().st_mtime)
    for c in found[:200]:
        d = tsv(c)
        if half(d):
            return d
    return {}


def preflight(want: int = 1, vm_type: str = VM_TYPE, starts: bool = True) -> tuple[str | None, dict]:
    """Can boat serve a batch now? None and what it can give, or the reason it can't: its API, the account's billing,
    the image, or no VM to be had (the pool's free VMs, and what the plan lets this batch start). A warm VM with its
    agent is enough, with no API call."""
    # (each verdict rules.boat.preflight's, asked with what is known so far: a stage it decides at needs no later API call)
    starts_check = starts
    v = lambda **k: R.preflight(**{"warm": 0, "api_ok": True, "can_start": True, "credit": True, "image_ok": True, "can": 1, "starts_check": starts_check, "starts": 1, "room": 1, "leased": 0, "old_policy": OLD, **k})
    k = warm_agents(vm_type)
    if v(warm=k) == "ok" and k:
        return None, {"warm": k}
    try:
        lim = limits()
    except Exception as e:
        if v(api_ok=False) == "api":
            return f"boat API unreachable ({str(e)[:160]})", {}
        raise
    if v(can_start=not (lim.get("billingStatus") not in (None, "active") or lim.get("blockedReason") or lim.get("startBlockedReason") or not lim.get("canStart", True))) == "billing":
        return f"boat can't start VMs ({lim.get('billingStatus')}, {lim.get('blockedReason') or lim.get('startBlockedReason')})", {}
    if v(credit=not (not lim.get("unlimited") and lim.get("creditBalanceSeconds", 1) <= 0 and lim.get("packBalanceSeconds", 0) <= 0)) == "credit":
        return "boat credit used up", {}
    img = image_name()
    try:
        snap = named_snapshots().get(img)
    except Exception as e:
        return f"boat API unreachable ({str(e)[:160]})", {}
    if v(image_ok=bool(snap and snap.get("status") == "ready")) == "image":
        return f"boat image {img} isn't there (supercomputer image --build)", {}
    try:
        with Registry().locked() as d:
            reconcile(d, sandboxes())
            free = [v for v in d["vms"].values() if v["snapshot"] == img and v["type"] == vm_type and lease_free(v)]
            leased = sum(1 for v in d["vms"].values() if v["snapshot"] == img and v["type"] == vm_type and not lease_free(v))
    except Exception as e:
        return f"boat API unreachable ({str(e)[:160]})", {}
    up = sum(1 for v in free if v.get("state") in RUNNING)
    room = lim["currentLimits"]["activeSandboxes"] - lim["activeSandboxes"]
    st = lim["starts"]
    # (a resume is a start to boat, as a creation is; the minute's starts come back within the minute, so only the
    # old policy counts them out)
    starts = max(0, min([starts_left(lim)] + ([st["minute"]["remaining"]] if OLD else [])))
    resumable = min(len(free) - up, max(0, room), starts)
    creatable = min(starts - resumable, max(0, room - resumable))
    can = up + resumable + creatable
    info = {"free_up": up, "free_stopped": len(free) - up, "active_room": room, "starts_now": starts, "can": can, "leased": leased}
    verdict = v(can=can, starts=starts, room=room, leased=leased)
    # (no VM to be had and the scheduler not deciding: the reason; or, other batches holding VMs with no start left but
    # room, a wait within --max-wait for one they release (still up, so no start), sooner and cheaper than Modal)
    why = {"full": "boat at its active-VM limit", "no_starts": "no boat starts left this hour or day", "none": "no boat VM to be had"}.get(verdict)
    if verdict == "wait":
        info["wait"] = True
        log(f"no boat starts left this hour or day; {leased} VMs leased by other batches: waiting within --max-wait for one they release")
        return None, info
    return why, info


def preflight3(want: int = 1, vm_type: str = VM_TYPE, starts: bool = True) -> tuple[str | None, dict]:
    """preflight(), asked again on boat's API errors (3 tries, 15 and 45 s apart): Modal takes a batch only once boat
    has failed it 3 times (the user, 10-31). A missing image or billing block is no failure to retry: said at once."""
    asks = R.Preflight3()  # (rules.boat.Preflight3: Modal for boat's API errors only after 3 tries)
    for k in range(3):
        reason, info = preflight(want, vm_type, starts)
        if asks.ask("ok" if reason is None else "api" if "API unreachable" in reason else "none") != "asking":
            return (reason, info) if reason is None or "API unreachable" not in reason else (f"{reason} (3 tries)", info)
        if k < 2:
            log(f"{reason}: boat again in {15 * 3 ** k} s ({k + 1} of 3)")
            time.sleep(15 * 3 ** k)
    return f"{reason} (3 tries)", info


def warm_up(vm_type: str = VM_TYPE) -> int:
    """The pool's VMs of the repo's image and a type that are up, free and not about to stop (a batch leases them
    without a start)."""
    now, img = time.time(), image_name()
    # (those Entry.lease_live would lease: rules.boat)
    return sum(1 for v in Registry().read()["vms"].values() if entry(v).lease_live(now, v["type"] == vm_type and v["snapshot"] == img, v.get("state") in RUNNING))


def warm_agents(vm_type: str = VM_TYPE) -> int:
    """The VMs up and free with a warm agent recorded, not about to stop (a batch reaches them with no boat API call,
    its TTL extended in the background within WARM_MARGIN)."""
    if not WARM:
        return 0
    now, img = time.time(), image_name()
    # (those Entry.lease_warm would lease: rules.boat)
    return sum(1 for i, v in Registry().read()["vms"].items() if entry(v).lease_warm(now, v["type"] == vm_type and v["snapshot"] == img, bool(read_agent(i))))


def job_items(spec: list[dict], pre: str, cmd, vm_type: str, past: dict) -> tuple[list[dict], list[str]]:
    """A jobs file's jobs as items, in its summary's order (its resource groups, in the order they first appear). A
    job's "image" is what the repo's config says it is on boat (a PATH and env on the shared image)."""
    med = sorted(past.values())[len(past) // 2] if past else 60
    cap_slots = TYPES[vm_type][0] // SLOT_CPU
    images = project().images
    hook = project().hook("job_item")
    items, order = [], []
    groups: dict[tuple, list] = {}
    for j in spec:
        groups.setdefault((j.get("cpu", 2.0), j.get("memory", 4096), j.get("timeout", 900)), []).append(j)
    for (cpu, memory, timeout), js in groups.items():
        for j in js:
            _validate_durable_item(j)
            from .rules.agent_capabilities import required_agent_capabilities
            required_agent_capabilities(j)
            w = R.weight(math.ceil(cpu), math.ceil(memory))  # (rules.boat.weight: 2 vCPU and 4096 MB a slot)
            it = {"name": j["name"], "kind": "sh", "cmd": pre + cmd(j["cmd"]), "cwd": j.get("cwd", "."), "limit": timeout - 30, "weight": w, "est": past.get(j["name"], med)}
            if "distinct_vm_group" in j:
                group = j["distinct_vm_group"]
                if not isinstance(group, str) or not group.strip():
                    raise ValueError("distinct_vm_group must be a nonblank string")
                it["distinct_vm_group"] = group
            for field in ("distinct_vm_group_scope", "distinct_vm_group_generation"):
                if field in j:
                    it[field] = j[field]
            if "cpu_model" in j:
                model = j["cpu_model"]
                if not isinstance(model, str) or not model.strip():
                    raise ValueError("cpu_model must be a nonblank exact model string")
                if not (j.get("vm_type") or env("VM_NAMED")):
                    raise ValueError("cpu_model requires an explicit vm_type")
                it["cpu_model"] = model
            if "cpu_model_max_fruitless" in j:
                limit = j["cpu_model_max_fruitless"]
                if "cpu_model" not in j or type(limit) is not int or limit < 1:
                    raise ValueError("cpu_model_max_fruitless requires cpu_model and a positive integer")
                it["cpu_model_max_fruitless"] = limit
            if j.get("env"):
                it["env"] = dict(j["env"])
            if j.get("timing") or env("TIMING") not in ("", "0"):
                # (a timing job: nothing else on its VM while it runs. The scheduler places it on an empty VM; a batch
                # holding its own VMs gives it every slot of one, its own CPUs still pinned as asked)
                it["timing"] = True
                it["cpus"] = w * SLOT_CPU
            im = images.get(j.get("image", ""), None) if j.get("image") else None
            if im:
                it.update(path=list(im.get("path", [])), env={**im.get("env", {}), **it.get("env", {})})
            if hook:
                it = hook(it, j) or it
            if it.get("timing") and not __import__("supercomputer.scheduler", fromlist=["enabled"]).enabled():
                it["weight"] = cap_slots
            it["memory"] = int(math.ceil(memory))
            named = j.get("vm_type") or env("VM_NAMED")  # (a job's own "vm_type", or the run's --vm-type: a placement constraint)
            if named:
                if named not in TYPES or TYPES[named][0] // SLOT_CPU < w or not R.fits_memory(it["memory"], TYPES[named][1], MEM_OVERHEAD):
                    raise SystemExit(f"{j['name']} asks for {cpu} vCPU and {memory} MB on a {named} VM: it doesn't hold that ({MEM_OVERHEAD} MB of each is its OS's)")
                it["vm_type"] = named
            elif w > cap_slots or not R.fits_memory(it["memory"], TYPES[vm_type][1], MEM_OVERHEAD):
                # (the smallest type with its slots whose usable memory holds it: 16384 MB is an xlarge's, a large's
                # 16 GB less its OS, agent and filesystem doesn't hold it: 11-01, 5 of 48 Lean checks OOM-killed)
                big = next((t for t, (c, m) in sorted(TYPES.items(), key=lambda x: x[1]) if c // SLOT_CPU >= w and R.fits_memory(it["memory"], m, MEM_OVERHEAD)), None)
                if not big:
                    raise SystemExit(f"{j['name']} asks for {cpu} vCPU and {memory} MB: no boat VM type has that usable ({MEM_OVERHEAD} MB of each is its OS's)")
                it["vm_type"] = big
            # Hooks may rebuild the execution spec, but cannot silently erase a user pin.
            for field in ("cpu_model", "cpu_model_max_fruitless", "distinct_vm_group", "distinct_vm_group_scope", "distinct_vm_group_generation", "required_agent_capabilities", "runtime", "enforce_memory", "memory_limit_mode"):
                if field in j:
                    it[field] = j[field]
            _validate_durable_item(it)
            required_agent_capabilities(it)
            items.append(it)
            order.append(j["name"])
    return items, order


def boat_jobs(spec: list[dict], out: str, pre: str, cmd, vms: int = 0, idle: int | None = None, teardown: bool = False, durations: str = "", vm_type: str = VM_TYPE,
              max_vms: int = 0, on_lost=None, max_wait: int = 0, on_done=None, tar: bytes | None = None, klass: str | None = None, strict_once: bool = False, max_attempts: int = 1) -> tuple[dict, dict]:
    """supercomputer/run.py's jobs on the pool: {name: {code, seconds, out, err}} for those that ran, and the batch's figures.
    A job lost twice (its VM died) goes to on_lost(name, reason), as does every job when boat can't serve the batch."""
    _validate_strict_options(strict_once, max_attempts)
    if strict_once:
        from .fanout import _strict_native
        _strict_native(spec, "boat")
    outdir = pathlib.Path(out)
    past = last_durations(outdir, {j["name"] for j in spec}, durations)
    items, order = job_items(spec, pre, cmd, vm_type, past)
    cap_slots = TYPES[vm_type][0] // SLOT_CPU
    rows: dict[str, dict] = {}
    sess = None
    summary = {}
    # a tiny batch (auto only): on a VM already up, else on Modal; a start for it would buy little
    slot_s = int(sum(it["weight"] * it["est"] for it in items))
    small = len(items) <= TINY[0] or slot_s < TINY[1] * 60
    # (with the scheduler, a tiny batch goes to boat too, as an interactive check: Modal only after boat fails 3 times)
    sched_on = __import__("supercomputer.scheduler", fromlist=["enabled"]).enabled()
    if strict_once and not sched_on:
        raise Unavailable("strict_once requires the scheduler; refusing the legacy retrying path")
    from .rules.agent_capabilities import required_agent_capabilities
    if any("cpu_model" in it or "distinct_vm_group" in it or required_agent_capabilities(it) for it in items) and not sched_on:
        raise Unavailable("placement constraints require the scheduler; refusing the legacy unconstrained path")
    tiny = on_lost is not None and small and not sched_on
    try:
        remote = bool(getattr(__import__("supercomputer.scheduler", fromlist=["controller"]), "controller")())
        # (rules.boat.route: a tiny batch with a fallback, the scheduler off, on a VM already up, else Modal)
        if tiny and R.route(len(items), slot_s, True, sched_on, remote, bool(remote or warm_up(vm_type) or warm_agents(vm_type) or scheduler_holds(image_name()))) == "modal":
            ledger(batch=None, label=f"run {outdir.name}", starts=0, vms=0, slots=0, jobs=len(items), to="modal", why="tiny, no VM up", acct=False)
            raise Unavailable(f"a tiny batch ({len(items)} jobs): no boat VM up, and a start isn't worth it")
        with signals_as_interrupt():
            # (its class: --class or $SUPERCOMPUTER_CLASS; else a small batch is interactive, a bigger one a submission)
            given = klass or env("CLASS")  # (rules.boat.batch_class: given, else interactive when small, a submission else)
            sess = session(f"run {outdir.name}", klass=given or R.batch_class(False, "submission", small), idle=idle, teardown=teardown, vm_type=vm_type,
                           build=on_lost is None, tar=tar, **({"strict_once": True, "max_attempts": max_attempts} if strict_once else {}))
            sess.output_dir = outdir
            sess.max_starts = max_vms
            sess.warm_only = tiny
            sess.early = not teardown
            plain = [it for it in items if not it.get("vm_type")]
            bigs: dict[str, int] = {}
            for it in items:
                if it.get("vm_type"):
                    bigs[it["vm_type"]] = bigs.get(it["vm_type"], 0) + 1
            if getattr(sess, "scheduled", False):  # (the scheduler sizes it)
                n = 0
            elif small and not vms and (tiny or warm_agents(vm_type)):  # (the VMs up: no API call to size it)
                n = max(1, min(max(warm_up(vm_type), warm_agents(vm_type)), math.ceil(sum(it["weight"] for it in plain) / cap_slots)))
            else:
                n = plan_vms(plain, cap_slots, vms, max_vms)
            log(f"{len(items)} jobs " + (f"to the scheduler ({sess.sched.NAMES[sess.klass]})" if getattr(sess, "scheduled", False) else f"on {n} {vm_type} VM(s)")
                + "".join(f" + {k} {t}" for t, k in bigs.items()) + f" (times from {'the last run' if past else 'a guess'})")
            sess.ensure(n, vm_type)
            for t, k in bigs.items():
                sess.ensure(k, t)

            if env("VERBOSE"):
                log("running")

            def done(it, row):
                rows[it["name"]] = row
                if on_done:
                    on_done(it["name"], row)
            t_run = time.time()
            sess.run(items, done, on_lost=(lambda it, why: on_lost(it["name"], why)) if on_lost and not strict_once else None, max_wait=max_wait)
            t_jobs = time.time()
            if env("VERBOSE"):
                log("jobs done")
            logs = sess.collect()
            t_logs = time.time()
            for name in list(rows):
                lg = logs.get(name, {})
                for k in range(1, 3):  # a job that ran again: its last run's logs
                    lg = logs.get(f"{name}~r{k}", lg)
                o, e = lg.get("out", ""), lg.get("err", "")
                if rows[name].get("error"):
                    e += ("\n" if e else "") + str(rows[name]["error"])
                rows[name].update(out=o, err=e)
            sess.harvest_caches()
            note_durations(rows)
            sess.phases = {"jobs_s": round(t_jobs - t_run, 1), "collect_s": round(t_logs - t_jobs, 1), "harvest_s": round(time.time() - t_logs, 1)}
    except Unavailable as e:
        if strict_once or on_lost is None or any("runtime" in it or it.get("required_agent_capabilities") for it in items):
            raise  # A capability refusal is never permission for a blind Modal fallback.
        for it in items:
            if it["name"] not in rows:
                on_lost(it["name"], str(e))
    finally:
        if sess:
            t_close = time.time()
            if env("VERBOSE"):
                log("closing")
            summary = sess.close()
            if env("VERBOSE"):
                log("closed")
            summary["phases"] = {**getattr(sess, "phases", {}), "close_s": round(time.time() - t_close, 1)}
            # (boat's dollars ask boat once a VM: not for a batch only on warm VMs, whose seconds are in the figures)
            summary["dollars_so_far"] = sess.dollars() if summary.get("warm", 0) < summary.get("vms", 0) else None
            summary["batch"] = sess.batch
            summary["image"] = sess.image
            if strict_once:
                import shutil
                destination = outdir / "custody"
                destination.mkdir(parents=True, exist_ok=True, mode=0o700)
                for bid, journal in summary.get("custody", {}).items():
                    copied = destination / f"{bid}.jsonl"
                    shutil.copyfile(journal["file"], copied)
                    copied.chmod(0o600)
                    journal["export_file"] = str(copied)
                with (outdir / "results.json").open("w") as output:
                    json.dump({"rows": rows, "cleanup": summary}, output, indent=1)
    return rows, summary


def summary_line(s: dict) -> str:
    if s.get("scheduled"):
        return f"boat (the scheduler, class {s['scheduled']}): {s['vms']} VMs, {s['starts']} starts charged to this batch" + (f", {s['preempted']} of its items preempted and run again" if s.get("preempted") else "")
    return (f"boat: {s['vms']} VMs ({s['created']} created, {s['resumed']} resumed, {s['leased_running']} already up"
            + (f", {s['topped_up']} of them joining mid-batch" if s.get("topped_up") else "") + (f", {s['not_resumed']} leased but not worth a start" if s.get("not_resumed") else "")
            + f"; {s.get('starts', s['created'] + s['resumed'])} starts" + (f", the account's day {s['account_day']}" if s.get("account_day") else "") + (f", {s['warm']} by its warm agent" if s.get("warm") else "") + "), "
            + (f"${s['dollars_so_far']} so far (boat usage), " if s.get("dollars_so_far") is not None else "")
            + f"{s['idle_vm_s']} idle VM-s of {s['ready_held_s']} held ready, {s['bring_up_s']} VM-s bringing up"
            + (f", then up to {s['idle_tail_s']} VM-s idle before they stop" if s.get("idle_tail_s") else ""))


# ----------------------------------------------------------------------------------------------------------- the CLI
def status():
    reg = Registry()
    with reg.locked() as d:
        reconcile(d, sandboxes())
        vms = d["vms"]
    by = {}
    for i, v in vms.items():
        k = (v["snapshot"], v["type"], v.get("state"), "leased" if not lease_free(v) else "free")
        by.setdefault(k, []).append(i)
    print(f"pool ({reg.path}): {len(vms)} VMs; {project().name}'s image now {image_name()}")
    for k, ids in sorted(by.items()):
        warm = sum(1 for i in ids if read_agent(i)) if k[2] in RUNNING else 0
        print(f"  {len(ids):4} {k[1]:8} {k[2] or '?':10} {k[3]:6} {k[0]}" + (f" ({warm} with a warm agent)" if warm else ""))
    sched_status(vms)
    return vms


def sched_status(vms: dict | None = None):
    """The scheduler's side: the policy generation and code in force, each agent's code, anything still on an older
    generation or code, and the pool's up VMs against their owners (an up VM no live owner holds: an orphan)."""
    from . import scheduler
    try:
        c = scheduler.current()
        st = c.req("/status", {}, timeout=20) if c else None
    except Exception as e:
        st = None
        print(f"scheduler: no answer ({str(e)[:100]})")
    if st and st.get("policy"):
        pol = st["policy"]
        print(f"scheduler pid {st['pid']}: code {st['code']} (installed {st['code_installed']}), policy generation {pol['generation']} "
              f"(floor {pol['floor']}, max {pol['max_vms']}, hold {pol['hold_s'] / 60:.0f} min, gate horizon {pol['gate_horizon_s'] / 60:.0f} min, idle {pol['idle_s']} s, boat tries {pol['boat_tries']})")
        ag = {}
        for h in st.get("agents", {}).values():
            ag[h or "?"] = ag.get(h or "?", 0) + 1
        print(f"  agents by code: {ag} (current {st['agent_code']}); stale: {st['stale']}")
        held = {v["id"] for v in st["vms"]}
        vms = vms if vms is not None else Registry().read()["vms"]
        images = {v.get("snapshot") for v in vms.values()}
        try:
            live = states(0)
        except Exception as e:
            print(f"  boat list failed: {str(e)[:120]}")
            return
        if True:
            up = [i for i, s in live.items() if s.get("state") in RUNNING and s.get("name") in images]
            # (an orphan: up, not the scheduler's, and no live lease; only a VM released a moment ago, with no lease and
            # its `until` ahead, is not one)
            orphans = [i for i in up if i not in held and lease_free(vms.get(i, {})) and not (not vms.get(i, {}).get("lease") and vms.get(i, {}).get("until", 0) > time.time())]
            print(f"  pool VMs up: {len(up)}; held by the scheduler: {len(held)}; held by other live clients: {sum(1 for i in up if i not in held and not lease_free(vms.get(i, {})))}; "
                  f"orphans (no live owner): {len(orphans)} {orphans[:8]}")


def reap():
    """Releases leases whose client died (killing what they left running), stops free VMs still up past their idle,
    and deletes build VMs whose client is gone."""
    reg = Registry()
    now = time.time()
    stop, kill = [], []
    with reg.locked() as d:
        reconcile(d, sandboxes())
        from . import scheduler as _sch
        sched_up = bool(_sch.enabled() and _sch.current())
        for i, v in d["vms"].items():
            l = v.get("lease")
            # (rules.boat.Entry.reap: a scheduler before the running one held it, the running one's to adopt; a dead
            # holder's lease released, what it left killed, the VM stopped; a free VM up past the idle time its batch
            # gave it (`until`; boat stops it then by itself, this is the backstop), or, for one released before
            # `until` was kept, 120 s after its release, stopped)
            what = entry(v, now).reap(now, bool(l and l.get("batch") == "scheduler"), sched_up)
            if what == "kill":
                kill.append((i, l["batch"]))
                v["lease"] = None
                v["last_used"] = now
            elif what == "stop":
                stop.append(i)
        for name in [n for n in d.get("demand", {}) if not declared(d, n)]:  # a client gone: what it kept up stops soon
            for i in undeclare(name, d):
                threading.Thread(target=shrink_ttl, args=(i, IDLE)).start()
                print(f"{i}: kept for {name}, which declares no more work: stops in {IDLE} s")
    # (in parallel, each call bounded: reap never hangs on one VM)
    for i, b in kill:
        print(f"released local cache for {i} (client gone); batch {b} drains under VM TTL")
    ts = [threading.Thread(target=stop_vm, args=(i,), daemon=True) for i in set(stop)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(90)
    for i in set(stop):
        print(f"requested fenced stop of idle {i}")
    for t in LIVE.glob("boat-build-*.json"):
        b = json.loads(t.read_text())
        if not pid_alive(b["pid"], b.get("host", "")):
            if delete_vm(b["vm"]):
                t.unlink()
                print(f"deleted orphan build VM {b['vm']}")
    live = sandboxes()
    up = [i for i, s in live.items() if s.get("state") in RUNNING]
    mine = set(reg.read()["vms"])
    print(f"{len(up)} boat VMs up ({len([i for i in up if i in mine])} of the pool)" + "".join(f"\n  not in the pool: {i} {live[i].get('name')}" for i in up if i not in mine))


def cold():
    """Stops every free VM that's up now (their idle time cut short), so the next batch finds the pool cold: before a
    measurement of what a batch starts."""
    with Registry().locked() as d:
        reconcile(d, sandboxes())
        up = [i for i, v in d["vms"].items() if entry(v).cold()]  # (rules.boat.Entry.cold)
        for i in up:
            d["vms"][i]["until"] = 0
    ts = [threading.Thread(target=stop_vm, args=(i,)) for i in up]
    [t.start() for t in ts]
    [t.join() for t in ts]
    print(f"stopped {len(up)} free VMs")


def teardown(everything: bool = False, vm_type: str = ""):
    """Deletes the pool's free VMs that come from an older image (all free VMs with --all, or all of a --type)."""
    img, p = image_name(), project()
    gone = []
    with Registry().locked() as d:
        reconcile(d, sandboxes())
        for i, v in list(d["vms"].items()):
            # (rules.boat.Entry.teardown: this repo's older images only; another repo's free VMs only with --all or --type)
            if entry(v).teardown(everything, v["snapshot"].startswith((f"{p.prefix}-", f"{p.final_prefix}-")), v["snapshot"] == img, bool(vm_type) and v["type"] == vm_type):
                gone.append(i)
    deleted = []
    def rm(i):
        if delete_vm(i):
            deleted.append(i)
        else:
            log(f"{i}: no accepted delete fence; left to TTL")
    ts = [threading.Thread(target=rm, args=(i,)) for i in gone]
    [t.start() for t in ts]
    [t.join() for t in ts]
    with Registry().locked() as d:
        for i in deleted:
            d["vms"].pop(i, None)
            forget_agent(i)
    print(f"deleted {len(deleted)} VMs")


def starts_summary(since: str = "24h"):
    """The ledger since a time (ISO 8601 UTC, or 24h, 7d): our starts by day and by client, the batches that went to
    Modal for want of a start, and now: ours this hour and day against the ceiling, the account's, the other's."""
    import datetime
    t = time.time() - float(since[:-1]) * {"m": 60, "h": 3600, "d": 86400}[since[-1]] if since[-1] in "mhd" else datetime.datetime.fromisoformat(since.replace("Z", "+00:00")).timestamp()
    rows = [r for r in (json.loads(l) for l in LEDGER.read_text().splitlines() if l.strip()) if r["t"] >= t] if LEDGER.exists() else []
    by_day, by_client = {}, {}
    for r in rows:
        for k, key in ((by_day, r["utc"][:10]), (by_client, r.get("client", "?"))):
            e = k.setdefault(key, [0, 0, 0, 0])
            e[0] += 1
            e[1] += r.get("starts", 0)
            e[2] += r.get("vms", 0) - r.get("starts", 0) if r.get("to") != "modal" else 0
            e[3] += r.get("to") == "modal"
    print(f"{len(rows)} batches since {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(t))}, {sum(r.get('starts', 0) for r in rows)} starts ({LEDGER})")
    for name, k in (("day", by_day), ("client", by_client)):
        for key, (n, st, warm, modal) in sorted(k.items()):
            print(f"  {name} {key:28} {n:4} batches {st:5} starts {warm:5} VMs without one {modal:4} to Modal")
    m = ours()
    with contextlib.suppress(Exception):
        lim = limits()
        st, eff = lim["starts"], effective(lim)
        print(f"now: ours {m[60]} this minute, {m[3600]} this hour, {m[86400]} today; the account {st['hour']['used']} this hour, {st['day']['used']} today"
              f" (free {st['hour']['limit']} / {st['day']['limit']}, in force {eff['hour']['limit']} / {eff['day']['limit']}); others {st['day']['used'] - m[86400]} today;"
              f" we may start {max(0, starts_left(lim, m))} now")
        print(extra_starts(lim))


def extra_starts(lim: dict | None = None) -> str:
    """What the starts past the plan's free ones cost: boat takes them from the wallet first (its pack balance; auto
    top-up refills it, the user's own setting), then bills the invoice (keepGoing). Today's dollars from the samples
    every call keeps (LIVE/extra-starts.jsonl): the wallet's drops (a refill is no spend) and the invoice's growth.
    Information, no threshold (the user, 10-31: "No limits, but be smart with our money")."""
    lim = lim or limits()
    kg = lim.get("keepGoing") or {}
    d = float(kg.get("usedDollars") or 0)
    w = float(lim.get("packBalanceDollars") or 0)
    samples = LIVE / "extra-starts.jsonl"
    day = time.strftime("%Y-%m-%d", time.gmtime())
    rows = []
    with contextlib.suppress(Exception):
        rows = [json.loads(l) for l in samples.read_text().splitlines() if l.strip()]
    if not rows or time.time() - rows[-1]["t"] > 300 or rows[-1].get("wallet") != w or rows[-1].get("dollars") != d:
        with contextlib.suppress(Exception):
            with open(samples, "a") as f:
                f.write(json.dumps({"t": round(time.time()), "day": day, "dollars": d, "wallet": w}) + "\n")
        rows.append({"t": time.time(), "day": day, "dollars": d, "wallet": w})
    today = [r for r in rows if r.get("day") == day]
    inv = max(0.0, today[-1]["dollars"] - today[0]["dollars"]) if today else 0.0
    ws = [r["wallet"] for r in today if "wallet" in r]
    wal = sum(max(0.0, a - b) for a, b in zip(ws, ws[1:]))
    return (f"extra starts {'on' if kg.get('enabled') else 'off'}: ${wal + inv:.2f} today (${wal:.2f} from the wallet, now ${w:.2f}; ${inv:.2f} on the invoice), "
            f"${d:.2f} on this month's invoice (boat's cap ${kg.get('capDollars')}{', reached' if kg.get('capReached') else ''})")


if __name__ == "__main__":
    import sys
    if sys.argv[1:2] != ["distinct-groups"]:
        raise SystemExit("usage: python3 -m supercomputer.boat distinct-groups open|get|close GROUP [--generation GEN --ttl-s N]")
    raise SystemExit(distinct_groups_main(sys.argv[2:]))
