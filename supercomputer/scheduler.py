"""The scheduler: one queue for all of the pool's work, by priority class, and the only client that holds pool VMs for
it. Every batch (`supercomputer run`, a bar's launches, a runner's calls, the treadmill's rounds) is a tenant: it
submits its items and gets its rows back; the scheduler decides which VM runs what, when VMs come and go.

    1 interactive  small checks: a free slot, else a filler's or a sweep's item killed (queued again) at once
    2 gate         bars and suites: the same, then the VMs that finish the gate within GATE_HORIZON
    3 submission   lane and terra experiments, the queue: slots as lower items end (a filler's after SUB_PREEMPT s);
                   up to SUB_VMS VMs' worth per client at once
    4 sweep        the treadmill's chains over unsolved tasks
    5 filler       soundness fuzz and mutants: keeps held VMs busy, never a reason to start one

Round-robin across clients within a class. Capacity comes from a warm slot, then a lower class's slot, then a free
start, then a paid one (Modal only on boat's errors, a missing image or the cap). Growth is demand-driven (_plan): the
queued slot-time against what the held pool clears within break-even, gates against GATE_HORIZON, each new VM with 3x its
bring-up of work, at most BRINGUP_MAX coming up at once and MAX_VMS (a technical ceiling). No money caps: "no limits,
but be smart with our money" (the user, 10-31). The pool shrinks damped: VMs
above the floor (the treadmill's hint) run filler for HOLD s after their last real work, then go (stopped 60 s later).
A circuit breaker stops every start when most recent starts don't come up (or boat's front fails most agents at once),
and a watchdog pauses starts when the controller itself is loaded. README, "Scheduling policy".

    supercomputer scheduler serve [--idle-exit 120]    (started by the first client that needs it; exits when idle)
    supercomputer scheduler status

It listens on 127.0.0.1 with a token (LIVE/scheduler/token, 0600): on the controller VM (controller.py), or on this
machine as the fallback. Its queue (each batch, its rows, its placements, each item's output) and its accounts
(LIVE/scheduler/metrics/) are on disk, so a restart loses no result.
"""
import base64
import concurrent.futures
import collections
import contextlib
import copy
import fcntl
import hashlib
import hmac
import http.server
import io
import json
import itertools
import math
import os
import pathlib
import re
import secrets
import selectors
import socket
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request

from . import blobcache, boat, canary, config, health, history, lease_diagnostics, memorypressure, prelaunch, resultfiles, resultjournal, resultjson, rules, statebackup, statusview
from .rules.prelaunch import prelaunch_can_retry, prelaunch_not_started, prelaunch_transport_retryable
from .durable_groups import DurableGroups, active as durable_active
from .boat import AgentClient, AgentError, Registry
from .agent import AGENT_CAPABILITIES as BUILD_AGENT_CAPABILITIES
from .rules.owned_lifecycle import (OwnedLifecycle, retention_supported, attempt_cap_supported,
                                    strict_transport_attempts, requeue_after_loss)
from .rules.agent_capabilities import (agent_capability_satisfies, agent_capabilities_match,
    agent_upgrade_allowed, normalize_agent_capabilities, required_agent_capabilities)


class AgentCapabilityError(RuntimeError):
    """Required agent protocols could not be verified before payload launch."""


CLASSES = {"interactive": 1, "gate": 2, "submission": 3, "sweep": 4, "filler": 5}
NAMES = {v: k for k, v in CLASSES.items()}
DIR = config.LIVE / "scheduler"
BLOBS = DIR / "blobs"
PORT = int(os.environ.get("SUPERCOMPUTER_SCHEDULER_PORT", "8790"))
ORPHAN_S = 21600  # a batch no client has polled for this long is closed (its client is gone; a laptop asleep comes back sooner)
def _cpu_now() -> float:
    """This thread's CPU seconds (0 where the clock has none: the simulator's)."""
    f = getattr(time, "thread_time", None)
    return f() if f else 0.0


def _proc_cpu() -> float:
    f = getattr(time, "process_time", None)
    return f() if f else 0.0


STARTUP_WAIT_S = 120.0  # bounded time for fenced warm-pool takeover before the HTTP port opens
FULL_S = 5.0  # a full pass at most this often (a batch submitted or closed: at once); light passes between
STRAY_S = 120  # a run no item owns counts as stray (health) only past this: a kill retried once (60 s) and the ticks' lag; sooner it's a kill landing
CLIENT = "scheduler"


class OwnedAgentRefused(RuntimeError):
    pass


class PrelaunchNotStarted(RuntimeError):
    """Preparation ended with complete durable evidence that no start was requested."""


class OwnedAgentClient:
    """Same authenticated transport; distinct command legacy daemons must refuse."""
    def __init__(self, client, version):
        self.client, self.version = client, version

    def rpc(self, command, *args, **kwargs):
        nonce = secrets.token_hex(16)
        request = {"protocol": 1, "version": self.version, "nonce": nonce,
                   "command": command, "args": list(args)}
        reply = self.client.rpc("owned_rpc", request, **kwargs)
        if (not isinstance(reply, dict) or reply.get("ok") is not True
                or type(reply.get("protocol")) is not int or reply["protocol"] != 1
                or reply.get("version") != self.version or reply.get("nonce") != nonce
                or not isinstance(reply.get("features"), dict)
                or reply["features"].get("pidfd") is not True
                or reply["features"] != {"pidfd": True, "private_pid_namespace": "verified-before-payload"}
                or not isinstance(reply.get("result"), dict)):
            raise OwnedAgentRefused("owned RPC source/nonce/features refused")
        result = reply["result"]
        if command == "vtick" and (result.get("err") or not isinstance(result.get("done"), list)
                                    or not isinstance(result.get("running"), list)):
            raise OwnedAgentRefused("owned tick result missing or refused; no legacy fallback")
        return result

    def put(self, route, data):
        encoded = base64.b64encode(data).decode()
        if route == "/put":
            return self.rpc("put", encoded)
        if route.startswith("/cache?"):
            from urllib.parse import parse_qs, urlparse
            query = parse_qs(urlparse(route).query)
            return self.rpc("cacheput", query["name"][0], query["key"][0], encoded)
        raise OwnedAgentRefused("unsupported owned upload")

def log(*a):
    print(f"[scheduler {time.strftime('%H:%M:%S')}]", *a, file=sys.stderr, flush=True)


def klass(name: str | int | None, default: str = "submission") -> int:
    if isinstance(name, int):
        return name
    return CLASSES.get((name or os.environ.get("SUPERCOMPUTER_CLASS") or default).lower(), CLASSES[default])


def _env(k: str, d):
    return type(d)(os.environ.get(f"SUPERCOMPUTER_{k}") or d)


# The policy's knobs (README, "Scheduling policy"); $SUPERCOMPUTER_<KNOB> overrides each.
HTTP_REQUEST_TIMEOUT_S = 15.0  # Slow/partial peers must not retain request threads indefinitely.
HTTP_POLL_MAX_WAIT_S = 5.0
HTTP_POLL_PEER_CHECK_S = 0.25
CPU_MODEL_MAX_FRUITLESS = int(_env("CPU_MODEL_MAX_FRUITLESS", 12))  # load guard per image/type/model, not money
BRINGUP_MAX = _env("BRINGUP", 12)  # VMs being brought up at once (the scheduler's own load guard); the rest wait their turn
GATE_HORIZON = _env("GATE_HORIZON", 900.0)  # a gate gets the VMs that finish it within this (its items' last times)
MAX_VMS = int(os.environ.get("SUPERCOMPUTER_MAX_VMS") or 0) or 10 ** 9  # none by default (the user: "no limit"); set, a technical ceiling
HOLD = _env("HOLD", 2700.0)  # above-floor filler hold; its default-VM cost also values the shared start allowance
# Opportunity prices, not spend limits: free starts consume a shared allowance.
START_DOLLARS = _env("START_DOLLARS", 0.03)  # first paid-start bracket (higher brackets: $0.05, $0.07)
VM_HOUR_DOLLARS = _env("VM_HOUR_DOLLARS", 0.036)  # per default-VM hour, like the digest
# Explicit legacy replay only, never a policy knob or an automatic live-policy fallback.
BREAK_EVEN_OVERRIDE_S = _env("HORIZON", 0.0)
if not math.isfinite(BREAK_EVEN_OVERRIDE_S) or BREAK_EVEN_OVERRIDE_S < 0:
    raise ValueError("SUPERCOMPUTER_HORIZON must be a finite positive replay override, or 0 to disable")
SUB_VMS = _env("SUB_VMS", 8)  # a submission client's VMs' worth of slots at once
BOAT_TRIES = _env("BOAT_TRIES", 3)  # boat failures (a VM lost, a row missing, no VM ready) before an item goes to Modal
DRAIN_STEP = _env("DRAIN_STEP", 0.25)  # past their hold, the VMs above the floor shed by at most this share (rounded up) a window
DRAIN_EVERY = _env("DRAIN_EVERY", 300.0)  # the staged drain's window: until this long after its last shed
FLOOR_ALWAYS = _env("FLOOR_ALWAYS", 0) not in (0, "0", "")  # the floor held with no treadmill alive too
FLOOR_DEFAULT = int(_env("FLOOR_DEFAULT", 1))  # an image without a treadmill keeps this many VMs between bursts (no churn)...
FLOOR_IDLE = float(_env("FLOOR_IDLE", 2700.0))  # ...while it had real work within this; past it none (a project that stopped)
FLOOR_BY_IMAGE: dict = {}  # an image's own floor, replacing either (the policy's floor_by_image)
DISK_MIN_MB = int(_env("DISK_MIN_MB", 10240))  # a VM with less free disk (or a tenth of a smaller disk) takes nothing (rules.disk_ok)
DISK_GRACE_S = 120  # its agent's cleaning's time to free it; past it, a VM still low is deleted once empty (rules.disk_replaced)
SUB_PREEMPT = _env("SUB_PREEMPT", 45.0)  # a submission item waiting this long takes a filler's slot (else at item ends; filler is the cheapest work to kill)
PREP_MAX = _env("PREP_MAX", 2)  # trees and caches the controller itself sends at once (VMs pull from peers first)
BREAK_READY = _env("BREAK_READY", 480.0)  # a start not ready within this counts as failed
BREAK_WINDOW, BREAK_MIN, BREAK_RATIO = 1200.0, 4, 0.5  # over 50% of at least 4 recent starts failed: no starts
BREAK_BACKOFF, BREAK_MAX = 300.0, 3600.0  # the first pause, doubled at each trip in a row
SHED_LOAD = _env("SHED_LOAD", 1.5)  # load per CPU past which the controller sheds (no starts, one preparation at a time)
WATCHDOG = os.environ.get("SUPERCOMPUTER_WATCHDOG", "1" if sys.platform.startswith("linux") else "0") not in ("0", "")

# ---------------------------------------------------------------------------------------------------- the policy
# One source of policy: LIVE/policy.json, with a generation. The scheduler reads it every tick (by its mtime), so a
# change applies to VMs already up and items already running within one tick, with no restart; the knobs above are
# only its defaults ($SUPERCOMPUTER_<KNOB> at the first run). `supercomputer policy [set k=v ...]`.
POLICY = config.LIVE / "policy.json"


def policy_defaults() -> dict:
    return {"start_breaker_enabled": False, "floor": _env("FLOOR", 8), "max_vms": 0 if MAX_VMS >= 10 ** 9 else MAX_VMS, "hold_s": HOLD, "gate_horizon_s": GATE_HORIZON, "start_dollars": START_DOLLARS, "vm_hour_dollars": VM_HOUR_DOLLARS, "idle_s": boat.IDLE,
            "ttl_s": boat.BACKSTOP, "bringup": BRINGUP_MAX, "sub_vms": SUB_VMS, "sub_preempt_s": SUB_PREEMPT, "boat_tries": BOAT_TRIES, "classes": list(CLASSES),
            "drain_step": DRAIN_STEP, "drain_every_s": DRAIN_EVERY, "floor_always": bool(FLOOR_ALWAYS),
            "floor_default": FLOOR_DEFAULT, "floor_idle_s": FLOOR_IDLE, "floor_by_image": dict(FLOOR_BY_IMAGE),
            "cpu_model_max_fruitless": CPU_MODEL_MAX_FRUITLESS, "history_retention_hours": 48.0, "blob_cache_bytes": 8 * 1024 ** 3, "blob_cache_grace_s": 3600, "blob_cache_max_age_s": 7 * 86400}


def read_policy() -> dict:
    with contextlib.suppress(Exception):
        p = json.loads(POLICY.read_text())
        p.pop("horizon_s", None)  # retired: old policy files do not restore a growth time knob
        return {**policy_defaults(), **p}
    return {**policy_defaults(), "generation": 0}


def write_policy(changes: dict) -> dict:
    """The policy with `changes`, its generation one more; written whole, then renamed in (never half a file)."""
    p = read_policy()
    pol = rules.Policy(max(0, int(p.get("generation", 0))))  # (rules.Policy.write: a knob it doesn't have refused)
    unknown = [k for k in changes if k not in policy_defaults()]
    if not pol.write(not unknown):
        raise SystemExit(f"no policy knob {unknown[0]} (knobs: {', '.join(policy_defaults())})")
    for k, v in changes.items():
        if k == "history_retention_hours" and isinstance(v, bool):
            raise ValueError("history_retention_hours must be numeric")
        d = policy_defaults()[k]
        if isinstance(d, dict):  # (floor_by_image={"img": 0}, or img=n,img2=m)
            v = v if isinstance(v, dict) else json.loads(v) if str(v).strip().startswith("{") else {a: int(b) for a, b in (x.split("=", 1) for x in str(v).split(",") if x)}
            p[k] = {str(a): int(b) for a, b in v.items()}
            continue
        p[k] = (str(v).lower() in ("1", "true", "yes")) if isinstance(d, bool) else type(d)(v) if not isinstance(d, list) else v
    rules.break_even_s(float(p["hold_s"]), float(p["start_dollars"]), float(p["vm_hour_dollars"]))
    if p["blob_cache_bytes"] < 0 or p["blob_cache_grace_s"] < 60 or p["blob_cache_max_age_s"] < p["blob_cache_grace_s"]:
        raise SystemExit("blob cache requires bytes >= 0, grace >= 60s, and max age >= grace")
    if p["cpu_model_max_fruitless"] < 1:
        raise ValueError("cpu_model_max_fruitless must be positive")
    if not math.isfinite(p["history_retention_hours"]) or p["history_retention_hours"] < 0:
        raise ValueError("history_retention_hours must be finite and nonnegative")
    p["generation"] = pol.generation
    p["t"] = round(time.time(), 1)
    POLICY.parent.mkdir(parents=True, exist_ok=True)
    tmp = POLICY.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(p, indent=1))
    tmp.rename(POLICY)
    return p


def apply_policy(p: dict):
    global MAX_VMS, HOLD, GATE_HORIZON, START_DOLLARS, VM_HOUR_DOLLARS, BRINGUP_MAX, SUB_VMS, SUB_PREEMPT, BOAT_TRIES, DRAIN_STEP, DRAIN_EVERY, FLOOR_ALWAYS, FLOOR_DEFAULT, FLOOR_IDLE, FLOOR_BY_IMAGE
    MAX_VMS = rules.max_vms_of(int(p.get("max_vms") or 0))  # (0 or unset: no limit)
    HOLD, GATE_HORIZON = float(p["hold_s"]), float(p["gate_horizon_s"])
    START_DOLLARS, VM_HOUR_DOLLARS = float(p.get("start_dollars", START_DOLLARS)), float(p.get("vm_hour_dollars", VM_HOUR_DOLLARS))
    BRINGUP_MAX, SUB_VMS, SUB_PREEMPT, BOAT_TRIES = int(p["bringup"]), int(p["sub_vms"]), float(p["sub_preempt_s"]), int(p["boat_tries"])
    boat.IDLE, boat.LONG, boat.BACKSTOP = int(p["idle_s"]), int(p["idle_s"]), int(p["ttl_s"])
    DRAIN_STEP, DRAIN_EVERY = min(1.0, max(0.01, float(p.get("drain_step", 0.25)))), max(1.0, float(p.get("drain_every_s", 300.0)))
    FLOOR_ALWAYS = p.get("floor_always") in (True, 1, "1", "true", "True")
    FLOOR_DEFAULT, FLOOR_IDLE = max(0, int(p.get("floor_default", 1))), max(0.0, float(p.get("floor_idle_s", 2700.0)))
    FLOOR_BY_IMAGE = {str(a): int(b) for a, b in (p.get("floor_by_image") or {}).items()}


_code_cache: dict = {}


def installed_source() -> pathlib.Path:
    """Explicit install handoff, including processes running a different checkout."""
    try:
        source = pathlib.Path(json.loads((DIR / "install.json").read_text())["source"])
        if source.is_absolute() and (source / "supercomputer" / "scheduler.py").is_file():
            return source
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return config.SRC


def prepare_snapshot(pkg: pathlib.Path) -> dict:
    """Compile and import the exact snapshot before publishing or pausing work."""
    pkg = pkg.resolve()
    env = {**os.environ, "PYTHONPATH": str(pkg.parent)}
    commands = [
        [sys.executable, "-P", "-m", "compileall", "-q", str(pkg)],
        [sys.executable, "-P", "-c",
         "import pathlib, sys, supercomputer.scheduler as s; "
         "assert pathlib.Path(s.__file__).resolve() == pathlib.Path(sys.argv[1]).resolve(); "
         "import json; print(json.dumps(s.Scheduler.capabilities() if hasattr(s, 'Scheduler') and hasattr(s.Scheduler, 'capabilities') else {}))",
         str(pkg / "scheduler.py")],
    ]
    for command in commands:
        result = subprocess.run(command, cwd=str(pkg.parent), env=env,
                                capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            raise RuntimeError(f"snapshot compile/import failed: {(result.stderr or result.stdout)[-1000:]}")
    capabilities = json.loads(result.stdout.strip().splitlines()[-1])
    return {"python": sys.executable, "cache_tag": sys.implementation.cache_tag,
            "cpu_model_exact": capabilities.get("cpu_model_exact") is True,
            "distinct_vm_group": capabilities.get("distinct_vm_group") is True and capabilities.get("distinct_vm_group_protocol") == 1,
            "durable_distinct_vm_group": capabilities.get("distinct_vm_group_scope") == "durable" and capabilities.get("durable_distinct_vm_group_protocol") == 1,
            "result_files_protocol": capabilities.get("result_files_protocol") == 1,
            "required_agent_capabilities_protocol": capabilities.get("required_agent_capabilities_protocol") == 1,
            "runtime_pins_protocol": type(capabilities.get("runtime_pins_protocol")) is int and capabilities["runtime_pins_protocol"] == 1,
            "resource_limits_protocol": type(capabilities.get("resource_limits_protocol")) is int and capabilities["resource_limits_protocol"] == 1}


def stage_install() -> dict:
    """Publish immutable installed code for the next safe point; starts no service."""
    DIR.mkdir(parents=True, exist_ok=True)
    source = pathlib.Path(__file__).resolve().parent
    target = code_hash(source)
    saved = canary.Canary(DIR).snapshot(target, source)
    if saved is None or code_hash(saved) != target:
        raise RuntimeError("staged source changed while snapshotting; retry after install completes")
    prepared = prepare_snapshot(saved)
    # The live parent keeps serving throughout the expensive conversion. Do not
    # publish the target (or change an old-readable journal) until it completes.
    migration = resultjournal.prepare_directory(DIR / "queue")
    if migration["errors"]:
        raise RuntimeError(f"result-journal preparation failed: {migration['errors'][:3]}")
    prepared["result_journals"] = {k: v for k, v in migration.items() if k != "receipts"}
    log(f"prepared {migration['prepared']} result journals ({migration['source_bytes']} bytes) "
        f"in {migration['elapsed_s']:.3f}s; {migration['caches_reused']} reused")
    # Pin the immutable snapshot, not a package/checkout an installer can replace.
    manifest = {"source": str(saved.parent), "snapshot": str(saved.parent), "code": target, "prepared": prepared}
    tmp = DIR / f"install.{os.getpid()}.tmp"
    tmp.write_text(json.dumps(manifest))
    tmp.replace(DIR / "install.json")
    active = None
    try:
        active = Client().req("/status", {}, timeout=3).get("code")
    except Exception:
        pass  # stage must never create a scheduler just to report a hash
    return {"staged": target, "active": active, "pending": active is not None and active != target}


def code_hash(pkg: pathlib.Path | None = None) -> str:
    """The installed tool's code (config.SRC's, whatever this process runs: a hand-back runs a kept copy): a hash of
    its modules (by their mtimes, rehashed only when one changed)."""
    root = pkg or installed_source() / "supercomputer"
    files = sorted(root.rglob("*.py"))
    key = (str(root), tuple((str(f.relative_to(root)), f.stat().st_mtime_ns, f.stat().st_size) for f in files))
    if _code_cache.get(str(pkg)) != key:
        h = hashlib.sha256()
        for f in files:
            h.update(str(f.relative_to(root)).encode() + b"\0" + f.read_bytes() + b"\0")
        _code_cache[str(pkg)], _code_cache[f"{pkg}#"] = key, h.hexdigest()[:12]
    return _code_cache[f"{pkg}#"]


def own_code() -> str:
    """The code this process runs (its own package's files, read once: they may be replaced under it)."""
    if "own" not in _code_cache:
        _code_cache["own"] = code_hash(pathlib.Path(__file__).parent)
    return _code_cache["own"]


def jlines(p: pathlib.Path) -> list[dict]:
    """A JSON-lines file, its lines that parse (a crash mid-append leaves at most a torn last line, skipped)."""
    out = []
    for l in (p.read_text().splitlines() if p.exists() else []):
        with contextlib.suppress(Exception):
            out.append(json.loads(l))
    return out


def vm_slots(vm_type: str) -> int:
    return max(1, boat.TYPES[vm_type][0] // boat.SLOT_CPU)


def vm_mult(vm_type: str) -> float:
    return boat.TYPES[vm_type][0] / boat.TYPES["default"][0]


def pct(xs: list, q: float):
    if not xs:
        return None
    s = sorted(xs)
    return round(s[min(len(s) - 1, int(q * len(s)))], 1)


# ------------------------------------------------------------------------------------------------------ the state
class Item:
    def __init__(self, batch, spec: dict):
        # Validate on construction too: recovery rebuilds Items without submit().
        required_agent_capabilities(spec)
        self.batch, self.spec = batch, spec
        self.name = spec["name"]
        named = rules.placement_type(getattr(batch, "vm_named", False), batch.vm_type, spec.get("vm_type") or "")
        if named and not spec.get("vm_type"):  # (a type its batch named: its placement constraint, starts and reuse within it)
            spec = self.spec = {**spec, "vm_type": named}
        self.weight = int(spec.get("weight", 1))
        self.memory = int(spec.get("memory") or 0)  # (MB it asks, as its job said (job_items: 4096 by default); none said: its slots, no more)
        self.est = float(spec.get("est", 60))
        self.owned = OwnedLifecycle(batch.strict_once)
        self.start_done = threading.Event()
        self.start_done.set()
        self.launch_identity = None
        self.last_placement = None
        self.terminal_reap = None  # durable native proof captured before result ACK / VM release
        self.state = "queued"  # queued | starting | running | preempting | done | cancel_pending
        self.vm = None
        self.tries = 0
        self.placed = 0  # Admission exists before any journal entry; a live snapshot may place it first.
        self.t_created = time.time()
        self.t_queued = time.time()
        self.head = False  # (preempted: back at the head of its class)
        self.t_start = None
        self.run_name = self.name  # (the name on the VM: a retry has a name of its own there)
        # a timing item: nothing else on its VM while it runs (its own CPUs pinned as asked, the rest of the VM idle)
        self.timing = bool(spec.get("timing")) or batch.timing
        # (a timing item takes the smallest free VM that fits it, any type, warm first; only when none is free is one
        # made, of the smallest type that fits it: rules.timing_vcpus. 06:50-07:02: 43 small VMs created for timing
        # items while large ones of the image were free, churn 40)
        self.need_type = None
        if not self.timing and not spec.get("vm_type") and not boat.R.fits_memory(self.memory, boat.TYPES.get(batch.vm_type, (0, 1 << 30))[1], boat.MEM_OVERHEAD):
            # (its batch's type can't hold its memory: the smallest that can, with its slots; the plan starts those)
            self.need_type = next((t for t, (c, m) in sorted(boat.TYPES.items(), key=lambda x: x[1]) if c // boat.SLOT_CPU >= self.weight and boat.R.fits_memory(self.memory, m, boat.MEM_OVERHEAD)), batch.vm_type)
        if self.timing and not spec.get("vm_type"):
            v = rules.timing_vcpus(self.weight * boat.SLOT_CPU)
            # (the smallest type with its vCPUs whose usable memory holds it: rules.boat.fits_memory; a 4 GB VM's 2 GB
            # usable doesn't hold a slot's 4 GB, lane Y's OOM kills)
            self.need_type = next((t for t, (c, m) in sorted(boat.TYPES.items(), key=lambda x: x[1]) if c >= v and boat.R.fits_memory(self.memory, m, boat.MEM_OVERHEAD)), batch.vm_type)
        self.peers = 0  # the most other items that ran on its VM beside it
        # its fate (rules.ItemFate): placed, done, boat's failures of it (infrastructure, not its own exit code, one per
        # VM, the VMs in its avoid set), BOAT_TRIES of them, then Modal; the policy generation it follows
        self.fate = rules.ItemFate(BOAT_TRIES)
        self.not_before = 0.0  # (queued again in an outage: not before this)

    fails = property(lambda self: self.fate.fails)
    avoid = property(lambda self: self.fate.avoid)
    gen = property(lambda self: self.fate.gen)


class Batch:
    def __init__(self, msg: dict):
        self.id = msg["batch"]
        self.strict_once = bool(msg.get("strict_once", False))
        self.max_attempts = msg.get("max_attempts", 1)
        if self.strict_once and (type(self.max_attempts) is not int or not attempt_cap_supported(True, self.max_attempts)):
            raise ValueError("strict_once requires max_attempts=1")
        self.result_files_protocol = msg.get('result_files_protocol', 0)
        self.retain_journals = bool(msg.get("retain_journals", False))
        if not retention_supported(self.strict_once, self.retain_journals):
            raise ValueError("retain_journals requires strict_once ownership")
        self.admission_closed = bool(msg.get("admission_closed", False))
        self.cache_generation = msg.get("cache_generation")
        self.klass = int(msg["klass"])
        self.client, self.label = msg.get("client", "?"), msg.get("label", "")
        self.image, self.vm_type = msg["image"], msg.get("vm_type", boat.VM_TYPE)
        self.tree = msg["tree"]  # {id, files, links}; a tree the controller keeps: its id alone
        if "files" not in self.tree:
            self.tree = json.loads((DIR / "trees" / f"{self.tree['id']}.json").read_text())
        self.ctx_b64 = msg["ctx_b64"]
        self.caches = [{**c, "local": str(config.LIVE / c["name"] / c["key"])} for c in msg.get("caches", [])]  # (this host's copies)
        self.on_lost = bool(msg.get("on_lost")) and not self.strict_once
        self.max_wait = float(msg.get("max_wait", 0) or 0)
        self.warm_only = bool(msg.get("warm_only"))
        self.timing = bool(msg.get("timing"))
        self.vm_named = bool(msg.get("vm_type_named"))  # (its type named by its client: each item's placement constraint)
        self.distinct_vms: dict[str, set[str]] = {}
        self.has_distinct = any(s.get("distinct_vm_group") for s in msg["items"])
        self.items = {s["name"]: Item(self, s) for s in msg["items"]}
        self.rows: list[dict] = []
        self.t0 = float(msg.get("t0") or time.time())
        self.t_first = None  # its first item placed
        self.t_placed = None  # its last item placed (nothing of it queued any more)
        self.vms_used: set[str] = set()
        self.starts = 0
        self.preempted = 0
        self.slot_s = 0.0
        self.closed = bool(msg.get("cancellation_complete", False))
        self.cv = threading.Condition()

    def deliver(self, row: dict):
        row = resultfiles.Store(DIR / 'queue' / f'{self.id}.results').compact_row(row)
        with self.cv:
            self.rows.append(row)
            with contextlib.suppress(Exception):
                with open(DIR / "queue" / f"{self.id}.rows", "a") as f:
                    f.write(json.dumps(row) + "\n")
            self.cv.notify_all()

    def left(self) -> int:
        return sum(1 for it in self.items.values() if it.state != "done")

    def scale(self) -> float:
        """What its items took against their estimates, once 3 finished (never below 1: estimates aren't trusted
        to be generous, a slow batch grows the pool)."""
        return max(1.0, self.obs_s / self.obs_est) if getattr(self, "obs_n", 0) >= 3 and self.obs_est > 0 else 1.0


def mem_fits(it, vm_type: str) -> bool:
    """The item's memory within the VM type's usable memory (rules.boat.fits_memory: nominal less MEM_OVERHEAD)."""
    return boat.R.fits_memory(getattr(it, "memory", 0), boat.TYPES.get(vm_type, (0, 1 << 30))[1], boat.MEM_OVERHEAD)


def durable_item(it):
    return it.spec.get("distinct_vm_group_scope") == "durable"


def distinct_key(it):
    name = it.spec.get("distinct_vm_group")
    if not name:
        return None
    if durable_item(it):
        return (("durable", name, it.spec.get("distinct_vm_group_generation")), name)
    return (it.batch.id, name)


def distinct_live(it):
    if not durable_item(it):
        return True
    registry = getattr(it.batch, "durable_registry", None)
    return registry is not None and registry.allowed(it.spec["distinct_vm_group"],
                it.spec.get("distinct_vm_group_generation"), "", time.time())


def distinct_used(it):
    if not durable_item(it):
        return it.batch.distinct_vms.get(it.spec.get("distinct_vm_group", ""), ())
    registry = getattr(it.batch, "durable_registry", None)
    record = registry.load().get(it.spec["distinct_vm_group"]) if registry else None
    return record["used"] if record and record["generation"] == it.spec.get("distinct_vm_group_generation") else ()

def durable_receipt(it):
    if not durable_item(it):
        return {}
    placement = it.last_placement or {}
    return {"placement": {"distinct_vm_group": it.spec["distinct_vm_group"],
            "distinct_vm_group_scope": "durable", "distinct_vm_group_generation": it.spec.get("distinct_vm_group_generation"),
            "vm": placement.get("vm"), "boot_id": placement.get("boot_id"), "attempt": placement.get("attempt", 0)}}

def fit_shape(it) -> tuple:
    """All a VM's fit for the item reads (rules.vm_fits): its image, class, timing, weight, VM type, avoid set, memory.
    Items of one shape fit the same VMs: a pass's memo, preemption's and health's waste group by it."""
    return (it.batch.image, it.batch.klass, it.timing, it.weight, it.spec.get("vm_type") or "", frozenset(it.avoid) if it.avoid else None, getattr(it, "memory", 0), it.spec.get("cpu_model", ""),
            ((*distinct_key(it), frozenset(distinct_used(it)), distinct_live(it)) if durable_item(it) else
             (it.batch.id, it.spec["distinct_vm_group"], frozenset(it.batch.distinct_vms.get(it.spec["distinct_vm_group"], ())))) if it.spec.get("distinct_vm_group") else None,
            tuple(sorted(required_agent_capabilities(it.spec).items())))


def vm_fits(shape: tuple, vm, retired: set) -> bool:
    """rules.vm_fits for an item of `shape` on the VM: the one test of placement, preemption and waste."""
    image, _k, _t, _w, vtype, avoid, mem, model, distinct, required = shape
    return not getattr(vm, "quarantine", "") and not getattr(vm, "upgrading", False) and agent_capabilities_match(dict(required), getattr(vm, "agent_capabilities", {})) and (not distinct or len(distinct) != 4 or distinct[3]) and rules.distinct_vm_allowed(distinct is not None, vm.id in (distinct[2] if distinct else ())) and rules.cpu_matches(model, getattr(vm, "model", "")) and rules.vm_fits(vm.image == image and vm.ready and not vm.dead, not vtype or vtype == vm.type, mem, boat.TYPES.get(vm.type, (0, 1 << 30))[1], boat.MEM_OVERHEAD,
                         bool(avoid) and vm.id in avoid, getattr(vm, "prep_fails", 0), vm.id in retired, not vm.failing, not getattr(vm, "disk_low", False))


class SchedulerAgentClient:
    """Catch lease refusal on every scheduler RPC, before any fallback or kill."""
    def __init__(self, client, scheduler, vm):
        self.client, self.scheduler, self.vm_state = client, scheduler, vm
        if isinstance(client, AgentClient):
            client.result_store = scheduler._result_store

    def __getattr__(self, name):
        return getattr(self.client, name)

    def _call(self, method, *args, **kwargs):
        if self.vm_state.dead:
            raise AgentError("stale lease: VM relinquished")
        try:
            result = getattr(self.client, method)(*args, **kwargs)
        except Exception as error:
            if any(word in str(error).lower() for word in ("stale", "403", "400")):
                self.scheduler._drop_stale(self.vm_state, source='client.exception', error=error,
                                           operation=lease_diagnostics.operation(method, args))
            raise
        if isinstance(result, dict) and (result.get("stale") or isinstance(result.get("result"), dict) and result["result"].get("stale")):
            self.scheduler._drop_stale(self.vm_state, source='client.stale_reply', reply=result,
                                       operation=lease_diagnostics.operation(method, args))
            raise AgentError("stale lease: VM relinquished")
        return result

    def rpc(self, *args, **kwargs):
        return self._call("rpc", *args, **kwargs)

    def ping(self, *args, **kwargs):
        result = self._call("ping", *args, **kwargs)
        self.scheduler._record_agent_ping(self.vm_state, result)
        return result

    def put(self, *args, **kwargs):
        return self._call("put", *args, **kwargs)

    def raw(self, *args, **kwargs):
        return self._call("raw", *args, **kwargs)


class VMState:
    def __init__(self, id: str, image: str, vm_type: str, how: str):
        self.id, self.image, self.type, self.how = id, image, vm_type, how
        self.slots = vm_slots(vm_type)
        self.client: AgentClient | None = None
        self.lease: dict = {}
        self.t_renew = 0.0
        self.ttl = boat.BACKSTOP
        self.ready = False
        self.dead = False
        self.prepared: set[str] = set()
        self.preparing: dict[str, threading.Event] = {}
        self.trees: set[str] = set()  # tree ids on it (a peer for other VMs' pulls)
        self.caches: set[tuple] = set()  # (name, key) of build caches it has whole
        self.running: dict[tuple, Item] = {}
        self.finishing: dict[tuple, Item] = {}  # terminal on the agent; logs/row commit still pending
        self.model = ""
        self.starts: list[dict] = []
        self.acks: list[dict] = []
        self.idle_since = time.time()
        self.t_lease = time.time()
        self.t_ready = None
        self.t_real = time.time()  # its last real (non-filler) work: a VM above the floor goes HOLD s after it
        self.draining = False  # (past its hold: no new filler; released once empty)
        self.failing = 0  # ticks failed in a row
        self.quarantine, self.quarantine_since = "", 0.0
        self.repair_lock = threading.Lock()
        self.recovery_result = {}
        self.disk: dict = {}  # its agent's last word on its disk ({free_mb, total_mb}), with each tick
        self.disk_low = False  # (below rules.disk_ok's threshold: nothing placed there)
        self.t_disk = time.time()  # (that word's time; an empty VM first asked 5 min after it came, its ticks telling before)
        self.t_low = 0.0  # (low since)
        self.gen = 0  # (the policy generation it last followed)
        self.agent = ""  # its agent's code hash
        self.agent_capabilities: dict[str, int] = {}  # actual ping inventory, never inferred from the hash
        self.upgrading = False  # owned under Scheduler.lock; excludes admission through the whole network upgrade
        self.t_agent = 0.0
        self.busy_s = 0.0
        self.lock = threading.Lock()

    def used(self) -> int:
        """Slots taken: a timing item takes them all; runs the agent reports that this scheduler doesn't hold (an older
        run of a re-placed item, a run a handoff didn't take back) take theirs too, until they end (B11)."""
        return min(self.slots, (self.slots if any(it.timing for it in self.running.values()) else sum(it.weight for it in self.running.values())) + getattr(self, "foreign", 0))


class Scheduler:
    def __init__(self, idle_exit: float = 120):
        DIR.mkdir(parents=True, exist_ok=True)
        BLOBS.mkdir(parents=True, exist_ok=True)
        (DIR / "metrics").mkdir(parents=True, exist_ok=True)
        self.batches: dict[str, Batch] = {}
        self.vms: dict[str, VMState] = {}
        self.lock = threading.RLock()
        self.wake = threading.Event()
        self.stop = threading.Event()
        self.idle_exit = idle_exit
        self.t_idle = self.t_up = time.time()
        self.hints: dict[str, dict] = {}  # client -> {image, base_vms, until, pid}
        self._hints_saved: dict = {}
        self.gate = boat.StartGate()
        self.creating: dict[tuple[str, str], int] = {}  # (image, type) -> VMs asked of boat, not yet in self.vms
        self.backoff: dict[str, float] = {}
        self.asking: set[str] = set()
        self.prep_sem = threading.BoundedSemaphore(PREP_MAX)
        self.token = secrets.token_urlsafe(24)
        self.agent_b64 = base64.b64encode((pathlib.Path(boat.__file__).parent / "agent.py").read_bytes()).decode()
        from . import agent as agent_mod
        self.agent_version = agent_mod.VERSION
        # the circuit breaker, the watchdog, the plan, the day's metrics (all kept across restarts)
        self.cpu_constraints = self._constraints_load()
        self.starts_log: list[dict] = []  # {t, vm, ok (None while coming up)}
        self.resume_quarantine: dict[str, dict] = {}  # failed stopped-VM resumes; never auto-reacquired or adopted
        self.br = {"open_until": 0.0, "backoff": BREAK_BACKOFF, "half": False, "t_trip": 0.0, "reason": "", "trips": 0}
        self.shed = False
        self.load = {}
        self.alerts: list[dict] = []
        self.target: dict[str, dict] = {}  # image -> the plan's last numbers
        self.bring_up = float(boat.BRING_UP)  # measured, an average
        self.wave = {"on": False, "t": 0.0, "t_end": 0.0}
        self.held_before: dict = {}  # (the VMs the previous scheduler held, their hold clocks: all taken back)
        self.est_hist: dict = {}  # (seconds an item of a (label, function) took, an average kept across batches)
        self.class_obs: dict = {}  # (the last seconds items of each class took)
        self.m = self._metrics_load()
        self.hints.update(self._hints_saved)
        self.health = health.Health(DIR / "metrics" / "health.json")  # (the targets' records: health.py; they outlive a handoff)
        self.code = own_code()
        self.canary = canary.Canary(DIR)  # (canary.py: this code kept for a hand-back; a handoff to it watched 10 min)
        self.canary.snapshot(self.code, pathlib.Path(__file__).parent)
        self.pol = read_policy()
        if not POLICY.exists():
            self.pol = write_policy({})
        apply_policy(self.pol)
        self.pol_mtime = POLICY.stat().st_mtime_ns if POLICY.exists() else 0
        log(f"code {self.code}, policy generation {self.pol['generation']}")
        self.reload()
        self._status_view = statusview.PublishedStatus({"pid": os.getpid(), "code": self.code})
        self._status_timings = collections.deque(maxlen=20000)
        self._status_refresh_lock = threading.Lock()
        if time.time() < self.br["open_until"]:
            self.rejudge_breaker(source="startup", actor="controller", reason="startup/handoff under current rules")
        self._refresh_status(blocking=True)
        if not self.batches and not self.recovery_blocked:  # no adoption while ownership is uncertain
            self.adopting = True
            threading.Thread(target=lambda: (time.sleep(5), self._adopt()), daemon=True).start()

    # -- the day's metrics
    def _metrics_new(self, day: str) -> dict:
        return {"day": day, "vmh": 0.0, "vmh_filler_hold": 0.0, "vmh_by_class": {}, "starts": {n: {"free": 0, "paid": 0} for n in CLASSES}, "preempted": {n: 0 for n in CLASSES},
                "placed": {n: 0 for n in CLASSES}, "placed_warm": 0, "waits": {n: [] for n in CLASSES}, "hold_ends": 0, "max_binds": 0, "breaker_trips": 0,
                "gaps": [], "waves": 0, "peak_vms": 0, "not_ready": 0, "batches": [], "boat_fails": {n: 0 for n in CLASSES}, "to_modal": {n: 0 for n in CLASSES}}

    def _metrics_load(self) -> dict:
        day = time.strftime("%Y-%m-%d", time.gmtime())
        with contextlib.suppress(Exception):
            st = json.loads((DIR / "metrics" / "state.json").read_text())
            self.br.update(st.get("breaker", {}))
            self.bring_up = float(st.get("bring_up", self.bring_up))
            self.wave.update(st.get("wave", {}))
            self.starts_log = [s for s in st.get("starts_log", []) if time.time() - s["t"] < BREAK_WINDOW]
            self.held_before = st.get("held", {})
            self.resume_quarantine = st.get("resume_quarantine", {})
            self.est_hist = st.get("est", {})
            self.class_obs = {int(k): v for k, v in st.get("class_obs", {}).items()}
            self._drains_saved = st.get("drains", {})  # (the staged drain's window outlives a handoff: B4)
            self._hints_saved = {k: h for k, h in st.get("hints", {}).items() if h.get("until", 0) > time.time()}  # (the treadmill's hint too: a new scheduler's floor at once, not at its next hint)
        with contextlib.suppress(Exception):
            m = json.loads((DIR / "metrics" / f"{day}.json").read_text())
            base = self._metrics_new(day)
            for k, v in base.items():
                m.setdefault(k, v)
            return m
        return self._metrics_new(day)

    def _h(self):
        return getattr(self, "health", None) or health.Null()

    def health_check(self, minutes: float = 30) -> dict:
        """The targets over the last `minutes` (health.py), from this scheduler's records."""
        self._flush_status_timings()
        return self._h().check(time.time(), minutes) or {}

    def _metrics_save(self):
        self._flush_status_timings()
        self._h().save()
        self.m["constraints"] = self._constraint_status()
        with contextlib.suppress(Exception):
            p = DIR / "metrics" / f"{self.m['day']}.json"
            tmp = p.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.m))
            tmp.rename(p)
            st = {"breaker": self.br, "bring_up": self.bring_up, "wave": self.wave, "starts_log": self.starts_log[-200:],
                  "resume_quarantine": getattr(self, "resume_quarantine", {}),
                  "held": {v.id: {"t_real": v.t_real, "draining": v.draining, "lease": v.lease, "ttl": v.ttl, "quarantine": v.quarantine, "quarantine_since": v.quarantine_since} for v in list(self.vms.values()) if not v.dead},
                  "est": self.est_hist, "class_obs": {str(k): v[-200:] for k, v in self.class_obs.items()},
                  "hints": {k: h for k, h in self.hints.items() if h.get("until", 0) > time.time()},
                  "drains": {img: {"open": d.open, "t_last": d.t_last, "share": d.share, "quota": d.quota, "released": d.released}
                             for img, d in self.__dict__.get("_drains", {}).items()}}
            tmp = DIR / "metrics" / "state.tmp"
            tmp.write_text(json.dumps(st))
            tmp.rename(DIR / "metrics" / "state.json")

    def alert(self, msg: str):
        a = {"t": round(time.time(), 1), "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "msg": msg}
        self.alerts = (self.alerts + [a])[-20:]
        log(f"ALERT: {msg}")
        with contextlib.suppress(Exception):
            with open(DIR / "alerts.jsonl", "a") as f:
                f.write(json.dumps(a) + "\n")

    @staticmethod
    def _scheduled_closes() -> set[str]:
        """Durable scheduler close authority, independent of stray queue files."""
        closed = set()
        if boat.LEDGER.exists():
            with boat.LEDGER.open() as f:
                for line in f:
                    try:
                        r = json.loads(line)
                        if isinstance(r, dict) and r.get("scheduled") is True and "wall_s" in r and "slot_s" in r and isinstance(r.get("batch"), str):
                            closed.add(Scheduler._valid_batch(r["batch"]))
                    except (ValueError, TypeError):
                        continue  # another process may still be appending the tail
        return closed

    @staticmethod
    def _read_client_closes() -> set[str]:
        # Until reconciliation succeeds, an extant file remains unresolved.
        q = DIR / "queue"
        archived = {p.stem for p in q.glob("*.retired")} | {p.stem for p in q.glob("*.closed")}
        return {bid for bid in Scheduler._scheduled_closes() | archived
                if not (q / f"{bid}.json").exists()}

    def _quarantine_closed_admissions(self):
        """Archive resurrected closes before reload or the handoff manifest.

        The ledger is close authority, not a completion marker. Preserve every
        byte; never replay a closed ID or hide an unclassified open admission.
        """
        with self.lock, blobcache.locked(BLOBS):
            closed = self._scheduled_closes()
            # New closed sidecars are retention, not resurrected admissions.
            archived = {p.stem for p in (DIR / "queue").glob("*.retired")}
            self._durably_closed = closed | archived | getattr(self, "_durably_closed", set())
            closed -= archived
            q = DIR / "queue"
            for bid in archived:
                active = q / f'{bid}.json'
                if active.exists():
                    try:
                        record = history.load(active)
                        if record.get('closed_history') is not None:
                            history.metadata(record, bid)
                            continue  # A legitimate interrupted close, completed by reload.
                    except (ValueError, OSError):
                        pass
                    closed.add(bid)  # A resurrected active record, never replay it.
            evidence = {}
            for path in q.iterdir() if q.exists() else ():
                if path.suffix in (".json", ".rows", ".placed", ".cancel", ".logs"):
                    bid = path.stem
                    if bid in archived and path.suffix != ".json":
                        continue  # Preserve intentional retained evidence.
                    if rules.quarantine_closed_admission(True, bid in closed):
                        evidence.setdefault(bid, []).append(path)
            affected = set(evidence) | (set(self.batches) & closed)
            incoming = set()
            if affected and (DIR / "handoff.json").exists():
                snap = json.loads((DIR / "handoff.json").read_text())
                parent = int(snap.get("from_pid", 0))
                if (parent and parent != os.getpid() and time.time() - float(snap.get("t", 0)) < 600
                        and boat.pid_alive(parent, socket.gethostname())):
                    incoming = {it["batch"] for v in snap.get("vms", []) for it in v.get("items", [])}
            for bid in affected:
                b = self.batches.get(bid)
                held = any(it.batch.id == bid for v in self.vms.values()
                           for it in list(v.running.values()) + list(getattr(v, "finishing", {}).values()))
                if bid in incoming or held or (b is not None and not b.closed and b.left()):
                    raise RuntimeError(f"closed admission {bid} has running or queued ownership; quarantine refused")
            for bid in sorted(affected):
                paths = evidence.get(bid, [])
                if paths:
                    parent = DIR / "quarantine"
                    parent.mkdir(exist_ok=True)
                    stamp = time.strftime("closed-%Y%m%dT%H%M%S", time.gmtime())
                    archive = parent / f"{stamp}-{time.time_ns()}-{secrets.token_hex(4)}"
                    archive.mkdir()
                    # Admission first: even a crash midway cannot replay it.
                    for path in sorted(paths, key=lambda p: (p.suffix != ".json", p.name)):
                        os.replace(path, archive / path.name)
                    for directory in (archive, parent, q, DIR):
                        fd = os.open(directory, os.O_RDONLY)
                        try:
                            os.fsync(fd)
                        finally:
                            os.close(fd)
                    log(f"{bid}: quarantined durably closed admission and evidence in {archive}")
                self.batches.pop(bid, None)
                getattr(self, "recovery_errors", {}).pop(bid, None)

    def _unclassified(self, bid: str) -> bool:
        if bid in getattr(self, "recovery_errors", {}):
            return True
        if bid in self.batches or bid in getattr(self, "_client_closed", set()):
            return False
        try:
            self._valid_batch(bid)
        except ValueError:
            return True
        return (DIR / "queue" / f"{bid}.json").exists()

    # -- tenants
    def reload(self):
        """The queue as it was before a restart (the controller's service restarted, its VM recovered): every batch
        still open, its delivered rows kept, the rest queued again (what ran on VMs this process no longer holds runs
        again)."""
        self._reload_started = time.monotonic()
        self._journal_compact_pending = set()
        self._journal_load_receipts = {}
        self._journal_compaction_errors = {}
        qd = DIR / "queue"
        self.recovery_errors = {}
        self.recovery_blocked = True
        self._quarantine_closed_admissions()
        self._client_closed = self._read_client_closes()
        for f in sorted(qd.glob("*.json")) if qd.exists() else []:
            if f.stem in self._client_closed:
                continue
            b = None
            try:
                record = json.loads(f.read_text())
                if record.get("closed_history") is not None:
                    history.finish(qd, f.stem, record)
                    continue
                b = Batch(record)
                self._attach_durable(b)
                if self._valid_batch(b.id) != f.stem:
                    raise ValueError("admission id does not match its file")
                enhanced = b.strict_once or b.retain_journals or b.admission_closed or b.has_distinct
                rows, journal = resultjournal.load_with_receipt(
                    qd / f"{b.id}.rows", self._result_store(b.id), enhanced)
                self._journal_load_receipts[b.id] = journal
                if journal["needs_compaction"]:
                    self._journal_compact_pending.add(b.id)
                for r in rows:
                    if r.get("name") in b.items:
                        b.items[r["name"]].state = "done"
                        b.items[r["name"]].fate.delivered()
                b.rows = rows
                placed = self._read_journal(qd / f"{b.id}.placed", enhanced)
                if enhanced:
                    for pl in placed:
                        if not all(k in pl for k in ("name", "run_name", "vm", "image")) or pl["name"] not in b.items:
                            raise ValueError("invalid owned placement journal")
                for pl in placed:  # cumulative admission survives even a crash before prepare
                    prior = b.items.get(pl.get("name"))
                    if prior:
                        prior.owned.placements += 1
                        prior.launch_identity = pl.get("identity")
                        prior.last_placement = pl
                        if durable_item(prior):
                            record = b.durable_registry.load()[prior.spec["distinct_vm_group"]]
                            if pl.get("distinct_vm_group_generation") != prior.spec["distinct_vm_group_generation"]:
                                raise ValueError("durable placement generation mismatch")
                            if record["generation"] == prior.spec["distinct_vm_group_generation"]:
                                stored = record["used"].get(pl["vm"], {})
                                if any(stored.get(k) != pl.get(k) for k in ("identity", "vm", "boot_id", "attempt", "distinct_vm_group", "distinct_vm_group_generation")):
                                    raise ValueError("durable membership missing or mismatched")
                        b.vms_used.add(pl["vm"])
                        group = pl.get("distinct_vm_group") or prior.spec.get("distinct_vm_group")
                        if group and not durable_item(prior):
                            b.distinct_vms.setdefault(group, set()).add(pl["vm"])
                for it in b.items.values():
                    if b.strict_once and it.owned.placements > 1:
                        raise ValueError("strict admission has multiple placements; quarantined")
                    it.owned.closed = b.admission_closed
                for receipt in self._read_journal(qd / f"{b.id}.cancel", enhanced):
                    it = b.items.get(receipt.get("name"))
                    if receipt.get("phase") == "terminal":
                        if it and self._matching_terminal_reap(it, receipt):
                            it.terminal_reap = receipt
                        continue  # evidence is not an admission-close fence or a result row
                    if it and receipt.get("admission_closed") is True and receipt.get("identity") == it.launch_identity:
                        it.owned.cancel()
                        if it.state != "done":
                            it.state = "cancel_pending"
                    if (it and it.owned.closed and
                            ((receipt.get("matched") is True and self._matching_receipt(
                                receipt.get("receipt"), b.id, receipt.get("run_name"), it.launch_identity))
                             or (it.owned.placements == 0 and receipt.get("unplaced") is True))):
                        it.owned.confirm(True)
                        it.state = "done"
                        it.fate.delivered()
                for pl in placed:  # (the last placement of each item not done: taken back below)
                    it = b.items.get(pl["name"])
                    if it and it.state != "done":
                        it.reattach = pl
                        it.fate.reloaded(pl["vm"])
                        it.placed = int(pl["run_name"].rsplit("~s", 1)[1]) if "~s" in pl["run_name"] else 1
                self.batches[b.id] = b  # Publish only a completely decoded admission.
            except Exception as e:
                # Keep the ID reservation, not partially decoded items. Never let a parser
                # exception turn a durable admission into an unowned worker run.
                self.recovery_errors[f.stem] = repr(e)
                log(f"{f.stem}: recovery quarantined, ownership retained: {e!r}")
        # A candidate must leave the old parent's admission files and exact
        # group snapshot intact until commit. Migrate only after activation.
        self._history_pending = {b.id for b in self.batches.values()
                                 if b.strict_once and b.closed and all(it.owned.reaped for it in b.items.values())}
        self._client_closed = self._read_client_closes()
        self.recovery_blocked = bool(self.recovery_errors)
        if self.batches:
            log(f"{len(self.batches)} batches back from before the restart")
        # (every VM the previous scheduler held, taken in one write of the registry before this one serves: a handoff's
        # (its snapshot, the old one waiting for this one's word) or a restart's (its leases, their holder gone);
        # rules.take_over. Before, VM by VM: 14:33:45-14:36:54, 43 of 225 held at 14:33, 176 with no live owner)
        self._take_pool()
        self._reload_ms = max(0, math.ceil((time.monotonic() - self._reload_started) * 1000))
        if self.batches and not self.recovery_blocked:
            self.reattaching, self.t_reattach = True, time.time()  # (no VM leased or started until the first pass took back what runs where: rules.plan_waits, one tick after a handoff, 60 s at most)
            threading.Thread(target=self._reattach, daemon=True).start()

    def _take_pool(self):
        """At the start, before serving: every VM the previous scheduler held, in one write of the registry
        (rules.take_over), each held here at once, ready, as the handoff's snapshot (or the registry and the hold
        clocks) says; its items joined to it by re-attach's first pass at once (it holds the VM), then checked against
        its agent's word (_reconcile_taken). A handoff's old scheduler is told what was taken (handoff.ack): it exits
        only when this one holds every VM it held (rules.handoff_done). Unoffered registry extras wait
        for ordinary post-commit adoption; their provider latency is not a readiness dependency."""
        now, me, host = time.time(), os.getpid(), socket.gethostname()
        snap = {}
        with contextlib.suppress(Exception):  # (a handoff's snapshot only while its writer lives, waiting for this one's word: an
            sn = json.loads((DIR / "handoff.json").read_text())  # older one (15:16: a hand-back's, 9 min old) is not what runs now
            fp = int(sn.get("from_pid", 0))
            if now - float(sn.get("t", 0)) < 600 and fp and fp != me and boat.pid_alive(fp, socket.gethostname()):
                snap = sn
        with contextlib.suppress(Exception):
            (DIR / "handoff.json").rename(DIR / "handoff.used.json")  # (read once: never taken for a later start's)
        old = int(snap.get("from_pid") or 0)
        info = {v["id"]: v for v in snap.get("vms", [])}
        # Refresh closes at the handoff, not at an observer's earlier baseline.
        closed = self._read_client_closes()
        for bid in closed:
            self.batches.pop(bid, None)
            self.recovery_errors.pop(bid, None)
        self._client_closed = closed
        loaded = set(self.batches)
        # Old snapshots have no batch manifest. Preserve compatibility while
        # guarding at least all disk admissions and every batch with held runs.
        expected = set(snap["batches"]) if "batches" in snap else {
            f.stem for f in (DIR / "queue").glob("*.json")} | {
            x["batch"] for v in info.values() for x in v.get("items", [])}
        self.recovery, what, why = self.canary.admissions(self.code, expected, loaded, closed, self.recovery_errors)
        self.recovery["manifest_complete"] = "batches" in snap
        expected_groups = snap.get("distinct_vm_groups", {})
        ack_groups = self._distinct_snapshot()
        try:
            # A prior failed candidate may already have archived these batches.
            # Verify their immutable evidence; do not reload or replay them.
            for bid in expected_groups.keys() & closed:
                retained = history.distinct_groups(DIR / 'queue', bid)
                if retained != expected_groups[bid]:
                    raise ValueError(f'{bid}: retained distinct history mismatch')
                ack_groups[bid] = retained
        except (OSError, ValueError, KeyError, TypeError) as exc:
            self.recovery["ready"] = False
            self.recovery["retained_history_error"] = str(exc)
        if not rules.distinct_history_matches(bool(expected_groups), ack_groups == expected_groups):
            self.recovery["ready"] = False
            self.recovery["distinct_vm_groups"] = "history mismatch"
        durable = self._durable_snapshot()
        if not rules.distinct_history_matches(bool(durable) or bool(snap.get("durable_distinct_vm_groups")),
                                             durable == snap.get("durable_distinct_vm_groups", {}) if snap else True):
            self.recovery["ready"] = False
            self.recovery["durable_distinct_vm_groups"] = "history mismatch"
        self.recovery_blocked = not self.recovery["ready"]
        # A guard-aware parent owns the failed-ACK rollback. Starting a second
        # hand-back here would race its recovery and spawn two replacements.
        self._recovery_decision = ("alert", why) if what and snap.get("admission_guard") else (what, why)

        taken, missing, reg, starting, deferred_extras = [], [], {}, [], []
        retired = self.retired()
        registry_started = time.monotonic()
        with Registry().locked() as d:
            for vid, v in d["vms"].items():
                l = v.get("lease") or {}
                if vid in self.vms or vid in retired:
                    continue
                pid = int(l.get("pid") or 0)
                if not rules.take_over(l.get("batch") == "scheduler", l.get("host") == host, pid == me, boat.pid_alive(pid, l.get("host", "")) if pid else False, bool(old) and pid == old):
                    continue
                if not rules.handoff_registry_vm(bool(snap), vid in info):
                    deferred_extras.append(vid)
                    continue  # Include stopped extras: provider state can change after the snapshot.
                if vid not in info and v.get("state") not in boat.RUNNING:
                    continue  # (a lease left on a VM boat had stopped: nothing to hold)
                v["lease"] = {**l, "pid": me, "host": host, "batch": "scheduler", "since": now, "label": "scheduler (taken over)"}
                taken.append(vid)
                reg[vid] = dict(v)
            missing = [vid for vid in info if vid not in taken]
        self._registry_load_ms = max(0, math.ceil((time.monotonic() - registry_started) * 1000))
        def acquire(vid):
            # Do not serialize network round trips over a whole warm pool before
            # opening the scheduler port. Each VM has its own independent fence.
            i, r = info.get(vid, {}), reg[vid]
            cl = boat.agent_client(vid)
            vm = VMState(vid, i.get("image") or r.get("snapshot"), i.get("type") or r.get("type") or boat.VM_TYPE, "warm")
            if cl is None and not self.recovery_blocked:
                return cl, vm, False
            hb = self.held_before.get(vid, {})
            vm.lease = i.get("lease") or hb.get("lease") or (boat.read_agent(vid) or {}).get("lease") or {}
            vm.ttl = max(boat.BACKSTOP, int(i.get("ttl") or hb.get("ttl") or 0))
            if (snap and not self.recovery_blocked and cl is not None and i.get("lease") == vm.lease
                    and vm.lease.get("owner", vm.lease.get("holder")) == boat.owner_id()
                    and vm.lease.get("vm_id") == vid and vm.lease.get("boot")
                    and type(vm.lease.get("epoch")) is int and vm.lease["epoch"] > 0):
                # The live same-machine parent handed us its exact capability.
                # Do not put O(pool) network renewals on the four-second switch.
                # Every worker RPC still verifies this fence on the VM.
                cl.lease, vm.t_renew = vm.lease, time.monotonic()
                return cl, vm, True
            try:
                if not vm.lease and not self.recovery_blocked:
                    # This path has already proven a dead local scheduler in our
                    # registry. Never use it for foreign/unsuffixed discovery.
                    old_agent = boat.read_agent(vid) or {}
                    migrated = boat.bootstrap_agent(vid, self.agent_b64, old_agent.get("token") or boat.owner_key())
                    if migrated.get("ok") is True:
                        vm.lease = (boat.read_agent(vid) or {}).get("lease") or {}
                        cl = boat.agent_client(vid)
                valid = cl is not None and self._renew_lease(vm)
            except Exception:
                valid = False
            if valid:
                cl.lease = vm.lease
                boat.remember_lease(vid, vm.lease)
                boat.rename_leased_vm(vid, vm.image, vm.lease)
            return cl, vm, valid

        with concurrent.futures.ThreadPoolExecutor(32) as ex:
            acquired = dict(zip(taken, ex.map(acquire, taken)))
        for vid in list(taken):
            i, hb = info.get(vid, {}), self.held_before.get(vid, {})
            cl, vm, valid = acquired[vid]
            if not valid:
                taken.remove(vid)
                if vid in info:
                    missing.append(vid)  # only offered VMs can fail the handoff
                if cl is None and not self.recovery_blocked:
                    with Registry().locked() as d:
                        if vid in d["vms"] and (d["vms"][vid].get("lease") or {}).get("pid") == me:
                            d["vms"][vid]["lease"] = None
                continue
            cl = SchedulerAgentClient(cl, self, vm)
            vm.client, vm.slots = cl, int(i.get("slots") or vm.slots)
            vm.prepared, vm.trees = set(i.get("prepared", [])), set(i.get("trees", []))
            vm.caches = {tuple(c) for c in i.get("caches", [])}
            vm.t_real = float(i.get("t_real") or hb.get("t_real") or now)
            vm.draining = bool(i.get("draining", hb.get("draining", False)))
            vm.gen, vm.agent, vm.t_agent = int(i.get("gen", 0)), i.get("agent", ""), 0.0
            vm.agent_capabilities = normalize_agent_capabilities(i.get("agent_capabilities"))
            vm.model = i.get("model", "")
            vm.quarantine = i.get("quarantine", hb.get("quarantine", ""))
            vm.quarantine_since = i.get("quarantine_since", hb.get("quarantine_since", 0.0))
            vm.ready, vm.t_ready, vm.idle_since = True, now, now
            if snap:  # (a handoff: its items exactly as the old one held them there, no record older than that)
                for x in i.get("items", []):
                    b = self.batches.get(x["batch"])
                    it = b.items.get(x["name"]) if b else None
                    if it is None or it.state == "done" or b.closed:
                        continue
                    it.state = rules.taken_item_state(x.get("state", "running"))
                    it.vm, it.run_name, it.t_start, it.taken_over, it.reattach = vm, x["run_name"], now, True, None
                    it.last_placement = x.get("placement") or it.last_placement
                    it.launch_identity = x.get("launch_identity") or it.launch_identity
                    suffix = it.run_name.rpartition("~s")[2]
                    it.placed = max(it.placed, int(x.get("placed", 0)), int(suffix) if suffix.isdigit() else 1)
                    it.fate.reloaded(vid)
                    it.fate.reattached(vid)
                    vm.running[(b.id, x["run_name"])] = it
                    if it.state == "starting":
                        if b.strict_once:
                            it.state = "running"  # Observe only: never resend a strict start after restart.
                        else:
                            starting.append((it, vm))  # Legacy agents deduplicate the same run name.
            self.vms[vid] = vm
        expected_items = {(v["id"], x["batch"], x["name"], x["run_name"]): x.get("launch_identity")
                          for v in info.values() for x in v.get("items", []) if x["batch"] not in closed}
        actual_items = {(v.id, it.batch.id, it.name, it.run_name): it.launch_identity
                        for v in self.vms.values() for it in v.running.values()}
        items_ready = not snap or expected_items == actual_items
        if not items_ready:
            self.recovery_blocked = True
            self.recovery["ready"] = False
            self.recovery["items_ready"] = False
        deferred = bool(snap.get("admission_guard") and old)
        self._handoff_deferred = (old, list(starting)) if deferred else None
        if not self.recovery_blocked and not deferred:
            for vid in taken:
                self._worker(self.vms[vid])
        if snap:  # (what the records place on the VMs handed over but the old one no longer held there: queued, as it was)
            for b in self.batches.values():
                for it in b.items.values():
                    if getattr(it, "reattach", None) and it.reattach.get("vm") in self.vms and it.state == "queued":
                        it.reattach = None
        if not self.recovery_blocked and not deferred:
            for it, vm in starting:
                threading.Thread(target=self._start_on, args=(it, vm), daemon=True).start()
        if taken:  # (their TTLs renewed now, as re-attach did each: the heartbeat's next is 5 min away, and a few handoffs
            def extend(vms=tuple(self.vms.get(vid) for vid in taken)):  # hold objects, not mutable dictionary lookups
                def one(vm):
                    if vm is not None and not vm.dead:
                        boat.boat("extend", vm.id, "--ttl", str(max(boat.BACKSTOP, vm.ttl)), "--json", check=False)
                with concurrent.futures.ThreadPoolExecutor(16) as ex:
                    list(ex.map(one, vms))
            threading.Thread(target=extend, daemon=True).start()
        self._handoff = {"t": now, "from_pid": old, "held": sorted(info), "taken": taken, "missing": missing, "snapshot": bool(snap), "checks": 0, "deferred_extras": sorted(deferred_extras)}
        if snap:
            self._handoff_ack = {"pid": me, "t": now, "of": snap.get("t"), "taken": taken, "missing": missing,
                                 "admissions": self.recovery, "items_ready": items_ready,
                                 "distinct_vm_groups": ack_groups,
                                 **({"durable_distinct_vm_groups": self._durable_snapshot()} if durable else {}),
                                 "registry_load_ms": self._registry_load_ms,
                                 "reload_ms": max(0, math.ceil((time.monotonic() - getattr(self, "_reload_started", time.monotonic())) * 1000))}
            if not os.environ.get("SUPERCOMPUTER_HANDOFF_FDS"):
                self._publish_handoff_ack()
        if taken or missing:
            log(f"took over {len(taken)} VMs in one write of the registry ({'the handoff of pid ' + str(old) if snap else 'the previous scheduler gone'}); "
                f"{len(missing)} not taken{': ' + str(missing[:6]) if missing else ''}")
        self._h().record_handoff(now, len(info) if snap else len(taken), len(taken), len(missing) if snap else 0, 0, 0)

    def _compact_result_journals(self):
        """Publish prepared projections only after activation, with all writers fenced.

        Preparation leaves old-readable journals intact so a rejected candidate
        cannot strand its parent. The live rows include any tail/recovery rows.
        A failed atomic replacement preserves the original and its valid cache.
        """
        pending = getattr(self, "_journal_compact_pending", set())
        if not pending:
            return
        with getattr(self, "_request_lock", self.lock), self.lock:
            for bid in list(pending):
                batch = self.batches.get(bid)
                path = DIR / "queue" / f"{bid}.rows"
                if batch is None or not path.exists():
                    pending.discard(bid)
                    continue
                try:
                    with batch.cv:
                        receipt = resultjournal.compact(path, batch.rows, bounded=True)
                    log(f"{bid}: compacted {receipt['source_bytes']} journal bytes "
                        f"to {receipt['compact_bytes']} after activation")
                    pending.discard(bid)
                except (OSError, ValueError) as exc:
                    self._journal_compaction_errors[bid] = str(exc)
                    log(f"{bid}: journal compaction deferred, original retained: {exc}")

    def _activate_handoff(self) -> bool:
        """A loaded candidate remains passive until its live parent commits by exiting."""
        deferred = getattr(self, "_handoff_deferred", None)
        guarded = deferred is not None
        parent_alive = bool(guarded and boat.pid_alive(deferred[0], socket.gethostname()))
        if not rules.handoff_activate(parent_alive, guarded):
            return False
        if rules.journal_compaction_allowed(parent_alive, guarded, self.recovery_blocked):
            self._compact_result_journals()
        if deferred is None:
            return True
        _, starting = deferred
        self._handoff_deferred = None
        if not self.recovery_blocked:
            for vm in self.vms.values():
                self._worker(vm)
            for it, vm in starting:
                threading.Thread(target=self._start_on, args=(it, vm), daemon=True).start()
        return True

    def _publish_handoff_ack(self):
        """Publish only after the inherited listener is serving and state is attached."""
        ack = getattr(self, "_handoff_ack", None)
        if ack is not None:
            tmp = DIR / "handoff.ack.tmp"
            tmp.write_text(json.dumps(ack))
            tmp.rename(DIR / "handoff.ack")

    def _reconcile_taken(self, vms: list):
        """The VMs taken over, each asked once what runs there: an item held as running there that its agent neither
        runs nor has finished is queued again (no strike: it never ran, or its row is gone with the old scheduler)."""
        def one(vm):
            try:
                r = vm.client.rpc("vtick", {"wait": 0}, timeout=30)
            except Exception:
                return 0  # (its ticks decide: failing, repaired, or lost by boat's word)
            seen = {(x["batch"], x["name"]) for x in r.get("running", [])} | {(x["batch"], x["name"]) for x in r.get("done", [])}
            if r.get("cpus"):
                vm.slots = max(1, int(r["cpus"]) // boat.SLOT_CPU)
            n = 0
            with self.lock:
                for key, it in list(vm.running.items()):
                    if (not it.batch.admission_closed and not it.owned.closed and key not in seen
                            and it.state in ("running", "preempting") and getattr(it, "taken_over", False)):
                        vm.running.pop(key, None)
                        it.fate.requeued()
                        it.state, it.vm, it.t_queued, it.head = "queued", None, time.time(), True
                        n += 1
            return n
        with concurrent.futures.ThreadPoolExecutor(32) as ex:
            n = sum(ex.map(one, vms))
        if n:
            log(f"take-over: {n} items held as running on the VMs taken over that their agents don't know: queued again")
        self.wake.set()

    def _handoff_check(self, now: float):
        """Strict ownership checks at +15/+30s; idle-only observations every 15s during the canary.
        Idle counts require continuous placeable demand past SUB_PREEMPT, not a single reconciliation gap.
        Readiness, admission/run identities and immediate missing-VM/orphan verdicts remain unchanged.
        """
        decision = getattr(self, "_recovery_decision", None)
        if decision and decision[0]:
            self._recovery_decision = None
            what, why = decision
            self.alert(why)
            if what == "handback":
                self.handoff(back_to=self.canary.read().get("from"))
            return
        if getattr(self, "recovery_blocked", False):
            return  # keep every lease and result until classification is safe
        h = getattr(self, "_handoff", None)
        if not h or now - h.get("sample_at", h["t"]) < 15:
            return
        if now - h["t"] > canary.WATCH_S and self.canary.read().get("state") != "watching":
            return
        h["sample_at"] = now
        if h["checks"] >= 2:
            # Do not reinterpret intentional later pool releases as missing handoff VMs.
            with self.lock:
                live = [v for v in self.vms.values() if not v.dead]
                idle = self._handoff_idle_slots(now, live)
            self._h().record_handoff(now, len(h["held"]) or len(h["taken"]), len(live), 0, 0, idle)
            return
        h["checks"] += 1
        held = {vid for vid, v in self.vms.items() if not v.dead}
        missing = [vid for vid in h["held"] if vid not in held and self._boat_up(vid)]
        me, host, orphans = os.getpid(), socket.gethostname(), 0
        orphan_ids = []
        with contextlib.suppress(Exception):
            for vid, v in Registry().read().get("vms", {}).items():
                if not rules.handoff_orphan_scope(vid in h.get("deferred_extras", ()), vid in self.vms):
                    continue  # A pre-existing unoffered hold is not an orphan created by this handoff.
                l = v.get("lease") or {}
                pid = int(l.get("pid") or 0)
                if l.get("batch") == "scheduler" and l.get("host") == host and pid and pid != me and not boat.pid_alive(pid, host) \
                        and v.get("state") in boat.RUNNING:
                    orphans += 1
                    orphan_ids.append(vid)
        with self.lock:
            live = [v for v in self.vms.values() if not v.dead]
            idle = self._handoff_idle_slots(now, live)
        self._h().record_handoff(now, len(h["held"]) or len(h["taken"]), len(held), len(missing), orphans, idle)
        log(f"handoff check {h['checks']}: {len(held)} VMs held, {len(missing)} of the handoff's not held, {orphans} orphans, {idle} slots persistently idle beside real work past sub_preempt_s; orphan_ids={sorted(orphan_ids)}")
        what, why = self.canary.handoff_verdict(self.code, len(missing) + orphans, idle, f"{len(missing)} VMs not taken, {orphans} orphans, {idle} slots idle beside waiting real work")
        if what == "alert":
            self.alert(why)
        if what == "handback":
            d = self.canary.read()
            self.alert(why)
            self.handoff(back_to=d.get("from"))

    def _handoff_idle_slots(self, now: float, live: list) -> int:
        """A conservative lower bound on slots continuously idle beside eligible work for SUB_PREEMPT.
        A placement, missing demand, or a missing VM resets its age; growing idle capacity cannot inherit age.
        The ordinary waste sampler and its queue-age/15-second rules are unchanged.
        """
        cur: dict = {}
        self._waste(now, live, grace=0.0, idle_only=True, by_vm=cur)
        previous = self.__dict__.get("_handoff_idle_state", {})
        state, total = {}, 0
        for vid, (_, slots, _) in cur.items():
            since, held, observed = previous.get(vid, (now, slots, now))
            vm = self.vms.get(vid)
            if vm is None or getattr(vm, "t_placed_real", 0) > observed:
                since, held = now, slots
            else:
                held = min(held, slots)
            state[vid] = (since, held, now)
            total += rules.handoff_idle_slots(held, now - since, SUB_PREEMPT)
        self._handoff_idle_state = state
        return total

    def _handoff_vm_occupied(self, vm: VMState) -> bool:
        """Unknown ownership and unfinished output are as binding as a live run."""
        if (getattr(self, "recovery_blocked", False) or vm.running
                or getattr(vm, "finishing", {}) or getattr(vm, "starts", [])
                or getattr(vm, "preparing", {}) or getattr(vm, "foreign", 0)):
            return True
        return any(it.state != "done" and (getattr(it, "vm", None) is vm
                   or (getattr(it, "reattach", None) or {}).get("vm") == vm.id)
                   for b in self.batches.values() for it in b.items.values())

    def _prune_handoff_pool(self, observed: dict):
        """Forget empty, known-unavailable holds before offering the pool.

        Caller holds self.lock: placement cannot race the occupancy recheck.
        This only drops this process's advisory registry entry, never a VM-side
        lease, worker, or provider resource. An absent listing entry is unknown.
        """
        pruned = {}
        for vm in list(self.vms.values()):
            state = (observed.get(vm.id) or {}).get("state")
            reason = ("dead" if vm.dead else state if state in ("stopped", "deleted", "error")
                      else "no agent" if vm.ready and vm.client is None else "")
            if rules.handoff_required(self._handoff_vm_occupied(vm), bool(reason)):
                continue
            with Registry().locked() as d:
                entry = d["vms"].get(vm.id)
                lease = (entry or {}).get("lease") or {}
                if (lease.get("pid") == os.getpid() and lease.get("host") == socket.gethostname()
                        and lease.get("batch") == "scheduler"):
                    entry["lease"] = None
                    if state in ("stopped", "deleted", "error"):
                        entry["state"] = state
            self._drop_stale(vm, source='handoff.prune', error=reason)  # occupied was false
            pruned[vm.id] = reason
        self._handoff_pruned = pruned
        if pruned:
            log(f"handoff: pruned {len(pruned)} empty unavailable holds: {pruned}")

    def _groups(self):
        if not hasattr(self, "_durable_registry"):
            self._durable_registry = DurableGroups(DIR / "durable-groups.json")
        return self._durable_registry

    def _durable_snapshot(self):
        return self._groups().snapshot()

    def _attach_durable(self, b):
        for it in b.items.values():
            if durable_item(it):
                if not isinstance(it.spec.get("distinct_vm_group_generation"), str) or not it.spec["distinct_vm_group_generation"]:
                    raise ValueError("durable admission lacks its pinned generation")
                b.durable_registry = self._groups()
                if not hasattr(self, "_durable_batch_ids"):
                    self._durable_batch_ids = set()
                self._durable_batch_ids.add(b.id)
                if it.spec["distinct_vm_group"] not in b.durable_registry.load():
                    raise ValueError("durable admission registry is missing")

    def _bind_durable(self, msg):
        with self.lock:
            items = []
            for spec in msg["items"]:
                if spec.get("distinct_vm_group_scope") == "durable":
                    record = self._groups().open(spec["distinct_vm_group"], time.time(), spec.get("distinct_vm_group_generation"))
                    spec = {**spec, "distinct_vm_group_generation": record["generation"]}
                items.append(spec)
            return {**msg, "items": items}

    def group_open(self, name):
        with self.lock:
            if getattr(self, "recovery_blocked", False):
                raise ValueError("recovery quarantined")
            record = self._groups().open(name, time.time())
            return {"ok": True, **record, "active": durable_active(record, time.time())}

    def group_get(self, name):
        with self.lock:
            DurableGroups.name(name)
            record = self._durable_snapshot().get(name)
            return {"ok": True, **record, "active": durable_active(record, time.time())} if record else {"ok": False, "error": "no such durable group"}

    def group_close(self, name, generation, ttl_s):
        with self.lock:
            if getattr(self, "recovery_blocked", False):
                raise ValueError("recovery quarantined")
            record = self._groups().close(name, generation, ttl_s, time.time())
            self.wake.set()
            return {"ok": True, **record, "active": durable_active(record, time.time())}

    def _distinct_snapshot(self) -> dict:
        return {b.id: {g: sorted(ids) for g, ids in b.distinct_vms.items()}
                for b in self.batches.values() if getattr(b, "distinct_vms", {})}

    def _handoff_snapshot(self) -> dict:
        """What the next scheduler takes at once: every VM held here and ready, with what it knows of it."""
        self._quarantine_closed_admissions()
        durable = self._durable_snapshot()
        vms = []
        for v in list(self.vms.values()):
            if (v.dead or not v.ready) and not self._handoff_vm_occupied(v):
                continue
            vms.append({"id": v.id, "image": v.image, "type": v.type, "slots": v.slots, "prepared": sorted(v.prepared), "trees": sorted(v.trees),
                        "model": v.model, "caches": sorted([list(c) for c in v.caches]), "lease": v.lease, "ttl": v.ttl, "t_real": v.t_real, "draining": v.draining, "gen": v.gen, "agent": v.agent,
                        "agent_capabilities": dict(v.agent_capabilities),
                        "quarantine": v.quarantine, "quarantine_since": v.quarantine_since,
                        "items": [{"batch": it.batch.id, "name": it.name, "run_name": it.run_name,
                                   "state": "running" if it.state == "finishing" else it.state,
                                   "placement": it.last_placement, "launch_identity": it.launch_identity, "placed": it.placed}
                                  for it in list(v.running.values()) + list(getattr(v, "finishing", {}).values())]})
        return {"from_pid": os.getpid(), "t": time.time(), "from": self.code, "vms": vms,
                "batches": sorted(set(self.batches) | set(getattr(self, "recovery_errors", {}))),
                "distinct_vm_groups": self._distinct_snapshot(),
                **({"durable_distinct_vm_groups": durable} if durable else {}),
                "admission_guard": True, "unavailable": dict(getattr(self, "_handoff_pruned", {}))}

    def _reattach(self):
        """Items placed before a restart and not done: their VMs leased again (their leases were this host's dead
        process's) and their items taken back where they run, so each result comes back; an item whose VM can't be
        had, or that no longer runs there, is queued again (under a name of its own, so a late finish of the old run
        is never taken for it)."""
        by_vm: dict[str, list] = {}
        for b in list(self.batches.values()):
            for it in b.items.values():
                pl = getattr(it, "reattach", None)
                if pl and it.state != "done":
                    by_vm.setdefault(pl["vm"], []).append((it, pl))
        n = 0
        # (each VM's tries by rules.VMLife.reattach: a pass, and one more 30 s later; a VM boat reports stopped none
        # more; then adoption decides)
        pending = dict(by_vm)
        lives = {vid: self._life(None, time.time(), lease="dead", items=len(its)) for vid, its in by_vm.items()}
        self.reattach_pending = set(pending)
        self.reattach_why, self.reattach_dead = {}, set()
        while pending and not self.stop.is_set():
            # (each VM's try at once, 16 at a time: 121 VMs one by one took 6.5 minutes at the 23:40 handoff, their
            # VMs waiting unowned by adoption's count the while)
            tries = [vid for vid in pending if lives[vid].pending]
            with concurrent.futures.ThreadPoolExecutor(16) as ex:
                n += sum(ex.map(lambda vid: self._reattach_vm(vid, pending[vid]), tries))
            for vid in tries:
                if vid in self.reattach_dead:
                    lives[vid].boat_stopped()
                if vid in getattr(self, "reattach_empty", set()):  # (nothing to take back there: its items queued again, no strike)
                    pending.pop(vid, None)
                    continue
                lives[vid].reattach(agent_ok=vid in self.vms)
                if not lives[vid].pending:
                    if vid not in self.vms and vid not in self.reattach_dead:
                        continue  # (its tries over: left to adoption, its items queued again below)
                    gone = pending.pop(vid)
                    if vid in self.reattach_dead:  # (its items back to the queue at once: one real boat failure each)
                        with self.lock:
                            for it, _ in gone:
                                if it.state == "queued":
                                    self._boat_failed(it, vid, f"its VM {vid} stopped across the restart")
            with self.lock:
                self.reattaching = False
                self.reattach_pending = {vid for vid in pending if lives[vid].pending}
            if not self.reattach_pending:
                break
            log(f"re-attach: {len(pending)} VMs not taken back yet; again in 30 s: {dict(list(self.reattach_why.items())[:4])}")
            self.stop.wait(30)
        with self.lock:  # (VMs not taken back: their items queued again; a strike only for a VM boat reports gone, rules.prepare_failed)
            for vid, its in pending.items():
                what = rules.prepare_failed(vid in self.reattach_dead or not self._boat_up(vid), True, self._outage())
                for it, _ in its:
                    if it.state == "queued":
                        if what == "lost":
                            self._boat_failed(it, vid, f"its VM {vid} not reached after the restart")
                        else:  # (its agent hung on a VM boat reports up: ours to repair, no strike; B2)
                            it.head = True
        self.reattach_pending = set()
        if n or pending:
            log(f"re-attached {n} items running on {len(by_vm) - len(pending)} VMs since before the restart; {len(pending)} VMs left to adoption, their items queued again")
        taken = [self.vms[vid] for vid in getattr(self, "_handoff", {}).get("taken", []) if vid in self.vms]
        if taken:  # (the VMs taken at once hold their items as their records say; each agent's word corrects that, in the background)
            threading.Thread(target=self._reconcile_taken, args=(taken,), daemon=True).start()
        if not getattr(self, "adopting", False):
            self.adopting = True
            threading.Thread(target=self._adopt, daemon=True).start()
        self.wake.set()

    def _life(self, vm: "VMState | None", now: float, lease: str = "ours", items: int = 0, hold: float | None = None) -> rules.VMLife:
        """A VM as rules.VMLife sees it, for one of its decisions (hold, release, re-attach, adoption): a VM held here
        from its VMState, else one up whose holder is `lease`. `hold`: how long a floor VM may idle (HOLD's, or for an
        image without a treadmill floor_idle_s: its default floor's VM waits between bursts)."""
        life = rules.VMLife(int(HOLD if hold is None else hold), int(boat.IDLE), int(boat.BACKSTOP))
        life.leased(now)
        if vm is not None:
            life.t_real, life.draining, life.running, life.gen = vm.t_real, vm.draining, len(vm.running), vm.gen
        elif lease == "dead":
            life.holder_died(items)
        else:
            life.lease = lease
        return life

    def _reattach_vm(self, vid: str, its: list) -> int:
        n = 0
        held = self.vms.get(vid)
        if held is not None and held.ready and not held.dead:  # (in hand already: its items joined to it)
            with self.lock:
                for it, pl in its:
                    if it.state in ("queued", "cancel_pending"):
                        it.taken_over = vid in getattr(self, "_handoff", {}).get("taken", [])  # (its agent's word checked after: _reconcile_taken)
                        it.state, it.vm, it.run_name, it.t_start = "running", held, pl["run_name"], time.time()
                        it.fate.reattached(vid)
                        held.running[(it.batch.id, pl["run_name"])] = it
                        held.prepared.add(it.batch.id)
                        n += 1
            return n
        if True:
            cl = boat.agent_client(vid)
            cached = (boat.read_agent(vid) or {}).get("lease") or self.held_before.get(vid, {}).get("lease")
            try:
                grant = boat.lease_vm(vid, "renew", lease=cached) if cached else {}
                if not grant.get("ok") or grant.get("stale") or not grant.get("boot"):
                    self.reattach_empty = getattr(self, "reattach_empty", set()) | {vid}
                    return 0
            except boat.LeaseProtocolLost:
                self.alert(f"{vid}: lease protocol lost; old client wins, left untouched")
                self.reattach_empty = getattr(self, "reattach_empty", set()) | {vid}
                return 0
            except Exception:
                return 0
            if cl:
                cl.lease = grant
            ok, why, pong = False, "no agent recorded", {}
            try:
                pong = cl.ping() if cl is not None else {}
                ok = pong.get("version") is not None
            except Exception as e:
                why = str(e)[:120]
            if not ok and cl is not None and "is stopped" not in why and self._boat_up(vid):
                # (boat reports it up, its agent doesn't answer: repaired in place with its token, so its items, still
                # running in sessions of their own, are taken back rather than run again)
                with contextlib.suppress(Exception):
                    boat.bootstrap_agent(vid, self.agent_b64, cl.token, 60, 2, lease=grant)
                    pong = cl.ping(timeout=15)
                    ok = pong.get("version") is not None
                    if ok:
                        log(f"{vid}: its agent didn't answer after the restart: repaired in place")
            if not ok:
                self.reattach_why[vid] = why
                if "is stopped" in why or "not found" in why.lower():
                    self.reattach_dead.add(vid)  # (boat says it's stopped: it will never answer)
            if ok:
                with Registry().locked() as d:
                    v = d["vms"].get(vid)
                    if not (v and (boat.lease_free(v) or (v.get("lease") or {}).get("pid") == os.getpid())):
                        self.reattach_why[vid] = f"its lease: {(v or {}).get('lease')}"
                    if v and (boat.lease_free(v) or (v.get("lease") or {}).get("pid") == os.getpid()):
                        v["lease"] = {"pid": os.getpid(), "host": socket.gethostname(), "batch": "scheduler", "since": time.time(), "label": "scheduler (re-attached)"}
                    else:
                        ok = False
            if not ok:
                return 0
            ttl = max(boat.BACKSTOP, int(self.held_before.get(vid, {}).get("ttl", 0)))
            threading.Thread(target=boat.boat, args=("extend", vid, "--ttl", str(ttl), "--json"), kwargs={"check": False}, daemon=True).start()
            pl0 = its[0][1]
            vm = VMState(vid, pl0["image"], pl0.get("type", boat.VM_TYPE), "warm")
            cl = SchedulerAgentClient(cl, self, vm)
            vm.client, vm.lease, vm.t_renew, vm.ttl = cl, grant, time.monotonic(), ttl
            self._record_agent_ping(vm, pong)
            running, finished, ticked = set(), set(), False
            with contextlib.suppress(Exception):
                r = cl.rpc("vtick", {"wait": 0}, timeout=30)
                ticked = True
                vm.slots = max(1, int(r.get("cpus") or boat.TYPES[vm.type][0]) // boat.SLOT_CPU)
                self._model_report(vm, r.get("model", ""))
                running = {(x["batch"], x["name"]) for x in r.get("running", [])}
                finished = {(x["batch"], x["name"]) for x in r.get("done", []) if not x.get("preempted")}  # (a preempted row: nothing to take back; B5)
            with self.lock:
                for it, pl in its:
                    key = (it.batch.id, pl["run_name"])
                    if it.state in ("queued", "cancel_pending") and (key in running or key in finished or it.owned.closed):
                        it.state, it.vm, it.run_name, it.t_start = "running", vm, pl["run_name"], time.time()
                        it.fate.reattached(vid)
                        vm.running[key] = it
                        vm.prepared.add(it.batch.id)
                        n += 1
                if ticked and not vm.running:  # (nothing of ours runs there any more, its filler preempted by a drain: left to adoption; B5)
                    with Registry().locked() as d:
                        v = d["vms"].get(vid)
                        if v and (v.get("lease") or {}).get("pid") == os.getpid():
                            v["lease"] = None
                    self.reattach_empty = getattr(self, "reattach_empty", set()) | {vid}
                    return 0
                vm.ready = True
                vm.t_ready = time.time()
                vm.idle_since = time.time()
                self.vms[vid] = vm
            self._worker(vm)
        return n

    @staticmethod
    def _read_journal(path, strict):
        if not strict:
            # Old takeovers could append JSON null for a starting legacy item.
            # These are not placements. Strict journals still reject them below.
            return [r for r in jlines(path) if r is not None]
        if not path.exists():
            return []
        raw = path.read_text()
        if raw and not raw.endswith("\n"):
            raise ValueError("owned journal has a torn tail; admission quarantined")
        records = [json.loads(line) for line in raw.splitlines()]
        if any(not isinstance(record, dict) for record in records):
            raise ValueError("invalid owned journal; admission quarantined")
        return records

    @staticmethod
    def _result_store(bid):
        Scheduler._valid_batch(bid)
        return resultfiles.Store(DIR / 'queue' / f'{bid}.results')

    @classmethod
    def _read_result_journal(cls, path, strict):
        if not path.exists():
            return []
        return resultjournal.load(path, cls._result_store(path.stem), strict)

    @staticmethod
    def _valid_batch(bid):
        if not isinstance(bid, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,159}", bid):
            raise ValueError("invalid batch id")
        return bid

    @classmethod
    def _validate_submission(cls, msg):
        if not isinstance(msg, dict):
            raise ValueError("submission must be an object")
        enhanced = bool(msg.get("strict_once") or msg.get("retain_journals"))
        if enhanced:
            cls._valid_batch(msg.get("batch"))
        for flag in ("strict_once", "retain_journals"):
            if flag in msg and type(msg[flag]) is not bool:
                raise ValueError(f"{flag} must be boolean")
        if not retention_supported(bool(msg.get("strict_once")), bool(msg.get("retain_journals"))):
            raise ValueError("retain_journals requires strict_once ownership")
        if "max_attempts" in msg and not msg.get("strict_once"):
            raise ValueError("max_attempts requires strict_once")
        cap = msg.get("max_attempts", 1)
        if msg.get("strict_once") and (type(cap) is not int or not attempt_cap_supported(True, cap)):
            raise ValueError("strict_once requires max_attempts=1")
        names = []
        for spec in msg.get("items", []):
            if not isinstance(spec, dict):
                raise ValueError("item must be an object")
            name = spec.get("name")
            required_agent_capabilities(spec)
            if "cpu_model_max_fruitless" in spec:
                limit = spec["cpu_model_max_fruitless"]
                if "cpu_model" not in spec or type(limit) is not int or limit < 1:
                    raise ValueError("cpu_model_max_fruitless requires cpu_model and a positive integer")
            if "cpu_model" in spec:
                if not isinstance(spec["cpu_model"], str) or not spec["cpu_model"].strip():
                    raise ValueError("cpu_model must be a nonblank exact model string")
                if not (spec.get("vm_type") or (msg.get("vm_type_named") and msg.get("vm_type"))):
                    raise ValueError("cpu_model requires an explicit vm_type")
            if "distinct_vm_group_scope" in spec:
                if spec["distinct_vm_group_scope"] != "durable" or not spec.get("distinct_vm_group"):
                    raise ValueError("distinct_vm_group_scope must be durable and requires distinct_vm_group")
                DurableGroups.name(spec["distinct_vm_group"])
            if "distinct_vm_group_generation" in spec and (spec.get("distinct_vm_group_scope") != "durable" or
                    not isinstance(spec["distinct_vm_group_generation"], str) or not spec["distinct_vm_group_generation"].strip()):
                raise ValueError("distinct_vm_group_generation requires durable scope and a nonblank generation")
            if "distinct_vm_group" in spec:
                if not isinstance(spec["distinct_vm_group"], str) or not spec["distinct_vm_group"].strip():
                    raise ValueError("distinct_vm_group must be a nonblank string")
                cls._valid_batch(name)
            if enhanced:
                cls._valid_batch(name)
            if "launch_identity" in spec or "strict_once" in spec or "max_attempts" in spec:
                raise ValueError("reserved scheduler ownership fields in item")
            names.append(name)
        grouped = any(s.get("distinct_vm_group") for s in msg.get("items", []))
        if grouped and (type(msg.get("placement_contract")) is not int or msg["placement_contract"] != 1):
            raise ValueError("distinct_vm_group requires placement_contract=1 from a group-aware client")
        if (enhanced or grouped) and len(names) != len(set(names)):
            raise ValueError("duplicate item names")
        if any(k in msg for k in ("admission_closed", "cancellation_complete", "launch_identity", "closed_history")):
            raise ValueError("reserved scheduler ownership fields in submission")

    @staticmethod
    def _append_owned(bid, suffix, record):
        qd = DIR / "queue"
        qd.mkdir(parents=True, exist_ok=True)
        with open(qd / f"{bid}.{suffix}", "a") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
            os.fsync(f.fileno())
        fd = os.open(qd, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _persist_fence(self, b):
        path = DIR / "queue" / f"{b.id}.json"
        record = json.loads(path.read_text())
        record["admission_closed"] = True
        record["retain_journals"] = True
        record["cancellation_complete"] = b.closed
        tmp = path.with_suffix(".cancel.tmp")
        with open(tmp, "w") as f:
            json.dump(record, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _owned_failed(self, it, why, *, not_started=False):
        if it.state == "done" and any(r.get("name") == it.name for r in it.batch.rows):
            return
        # `lost` is a legacy retry request. A strict LOST is terminal, never that flag.
        evidence = durable_receipt(it)
        if it.batch.strict_once:
            it.owned.cancel()  # A late report must not reopen a terminal attempt.
            placement = dict(it.last_placement or {})
            evidence = {"status": "LOST", "terminal": True, "max_attempts": it.batch.max_attempts,
                        "attempts": it.owned.placements, "placement": placement,
                        "vm": placement.get("vm"), "boot_id": placement.get("boot_id"),
                        "attempt": placement.get("attempt", 0),
                        "cleanup_complete": it.owned.reaped,
                        "identity_unavailable": not bool(placement.get("vm") and
                            placement.get("boot_id") and placement.get("identity"))}
        if it.batch.strict_once and not_started:
            audit = self._prelaunch_evidence(it)
            if prelaunch_not_started(audit['complete'], audit['reserved'], audit['start_requested']):
                evidence.update(status='NOT_STARTED', prelaunch_evidence=audit)
        diagnostic = getattr(it.vm, 'lease_relinquishment', None)
        if isinstance(diagnostic, dict):
            evidence['lease_relinquishment'] = diagnostic
        self._deliver_owned(it.batch, {"name": it.name, "code": -1, "seconds": -1,
                          "error": why, "strict_once": it.batch.strict_once,
                          "launch_identity": it.launch_identity, **evidence})
        it.state = "done"
        it.fate.delivered()

    def _deliver_owned(self, b, row):
        # File publication precedes the terminal journal and worker ACK.
        if b.strict_once and row.get("name") in b.items:
            it = b.items[row["name"]]
            placement = dict(it.last_placement or {})
            row = {"strict_once": True, "max_attempts": b.max_attempts,
                   "attempts": it.owned.placements, "attempt": placement.get("attempt", 0),
                   "launch_identity": it.launch_identity, "vm": placement.get("vm"),
                   "boot_id": placement.get("boot_id"), "placement": placement,
                   "terminal": True, "identity_unavailable": not bool(
                       placement.get("vm") and placement.get("boot_id") and placement.get("identity")),
                   "custody": {"protocol": 1, "controller_code": getattr(self, "code", None),
                               "agent_version": placement.get("agent_version"),
                               "max_attempts": b.max_attempts},
                   "status": "CANCELLED" if row.get("cancelled") else ("SUCCEEDED" if row.get("code") == 0 else "FAILED"),
                   **row}
            audit = self._prelaunch_evidence(it)
            if audit['complete'] and audit['failures']:
                row['prelaunch_failures'] = audit['failures']
        row = self._result_store(b.id).compact_row(row)
        # Callers hold self.lock: first terminal result wins without replacement.
        with b.cv:
            if any(r.get("name") == row.get("name") for r in b.rows):
                return
            self._append_owned(b.id, "rows", row)
            b.rows.append(row)
            b.cv.notify_all()

    @staticmethod
    def capabilities():
        return {"scope": "native-single-vm", "runtime_verified": True, "code": code_hash(),
                "submit_flags": ["strict_once", "retain_journals", "max_attempts"],
                "strict_once_protocol": 1, "strict_once_max_attempts": 1,
                "batch_fetch_protocol": 1, "closed_history_protocol": 1, "close_recovery_protocol": 1, "worker_recovery_protocol": 1, "breaker_rejudge_protocol": 1, "start_breaker_opt_in": True,
                "required_agent_capabilities_protocol": 1, "runtime_pins_protocol": 1, "resource_limits_protocol": 1,
                "item_constraints": ["cpu_model", "distinct_vm_group", "distinct_vm_group_scope", "distinct_vm_group_generation", "required_agent_capabilities", "runtime", "enforce_memory", "memory_limit_mode"], "cpu_model_exact": True, "distinct_vm_group": True, "distinct_vm_group_protocol": 1,
                "distinct_vm_group_scope": "durable", "durable_distinct_vm_group_protocol": 1,
                "result_files_protocol": 1, "output_files_protocol": 1,
                "result_inline_bytes": resultfiles.INLINE_BYTES,
                "poll_max_bytes": resultfiles.POLL_BYTES + 2048,
                "retention_requires_strict_once": True,
                "paths": ["/cancel", "/snapshot", "/close-receipt", "/capabilities", "/batch/info", "/batch/rows", "/distinct-groups/open", "/distinct-groups/get", "/distinct-groups/close"],
                "namespace_reap_guarantee": "requires matching agent receipt; not inferred from version"}

    @classmethod
    def _safe_snapshot(cls, value):
        if isinstance(value, dict):
            return {k: cls._safe_snapshot(v) for k, v in value.items()
                    if not any(s in k.lower() for s in ("token", "secret", "password", "cookie", "authorization", "ctx_b64", "env"))}
        if isinstance(value, list):
            return [cls._safe_snapshot(v) for v in value]
        return value

    def snapshot(self, bid):
        self._valid_batch(bid)
        with self.lock:
            b = self.batches.get(bid)
            if bid in getattr(self, "recovery_errors", {}):
                return {"ok": False, "batch": bid, "owned": True, "error": "admission quarantined; journals and ownership retained"}
            if b is None:
                retained = history.read(DIR / "queue", bid)
                if retained is not None:
                    return history.payload(DIR / "queue", bid, "snapshot.json", retained)
                exists = (DIR / "queue" / f"{bid}.json").exists()
                return {"ok": False, "error": "admission quarantined; journals retained" if exists else "no such batch or history expired"}
            return {"ok": True, "batch": bid, "strict_once": b.strict_once,
                    "max_attempts": b.max_attempts,
                    "retain_journals": b.retain_journals or b.strict_once, "admission_closed": b.admission_closed,
                    "closed": b.closed,
                    "placements": jlines(DIR / "queue" / f"{bid}.placed"),
                    "cancellation_receipts": self._safe_snapshot(jlines(DIR / "queue" / f"{bid}.cancel")),
                    "rows": [self._safe_snapshot(r) for r in b.rows],
                    "items": [{"name": it.name, "state": it.state,
                               "placements": it.owned.placements, "reaped": it.owned.reaped,
                               "start_inflight": it.owned.inflight, "admission_closed": it.owned.closed,
                               "result_error": getattr(it, "result_error", None)} for it in b.items.values()]}

    def batch_info(self, bid):
        """Bounded, read-only admission metadata for fetching completed results."""
        self._valid_batch(bid)
        with self.lock:
            b = self.batches.get(bid)
            if b is None:
                retained = history.read(DIR / "queue", bid)
                return ({"ok": True, **retained} if retained is not None else
                        {"ok": False, "batch": bid, "error": "no such batch or history expired"})
            return {"ok": True, "batch": bid, "generation": b.cache_generation,
                    "admitted_at": b.t0, "items": len(b.items), "rows": len(b.rows),
                    "complete": b.left() == 0, "closed": b.closed,
                    "strict_once": b.strict_once, "admission_closed": b.admission_closed,
                    "journals_retained": b.retain_journals or b.strict_once}

    @staticmethod
    def _close_receipt(b):
        items = list(b.items.values())
        complete = b.admission_closed and all(it.owned.reaped for it in items)
        return {"ok": complete, "complete": complete, "cleanup_complete": complete,
                "admission_closed": b.admission_closed, "journals_retained": True,
                "strict_once": b.strict_once, "max_attempts": b.max_attempts,
                "attempts": [{"name": it.name, "attempt": (it.last_placement or {}).get("attempt", it.owned.placements),
                              "run_name": (it.last_placement or {}).get("run_name"),
                              "vm": (it.last_placement or {}).get("vm"),
                              "boot_id": (it.last_placement or {}).get("boot_id"),
                              "launch_identity": it.launch_identity,
                              "cleanup_complete": it.owned.reaped}
                             for it in items if it.owned.placements],
                "unlaunched": [it.name for it in items if not it.owned.placements],
                "pending": [it.name for it in items if not it.owned.reaped], "batch": b.id}

    def close_receipt(self, bid):
        """Read evidence only. Never cancel, reap, submit, or touch a heartbeat."""
        self._valid_batch(bid)
        with self.lock:
            b = self.batches.get(bid)
            if b is not None:
                return self._close_receipt(b)
            retained = history.read(DIR / "queue", bid)
            if retained is not None:
                return history.payload(DIR / "queue", bid, "close.json", retained)
            return {"ok": False, "batch": bid, "cleanup_complete": False,
                    "error": "close evidence missing or expired"}

    def _retain_closed(self, b, receipt):
        """Called under the scheduler lock; preserve a durable non-execution fence."""
        before = b.closed
        info, snapshot = self.batch_info(b.id), self.snapshot(b.id)
        b.closed = True
        try:
            retained = history.capture(DIR / "queue", b.id, info, snapshot, receipt,
                                       read_policy()["history_retention_hours"])
        except Exception:
            # Before the durable fence, nothing closed. After it, never reopen.
            path = DIR / "queue" / f"{b.id}.json"
            try:
                fenced = history.load(path).get("closed_history") is not None
            except FileNotFoundError:
                fenced = (DIR / "queue" / f"{b.id}.closed").exists()
            except (ValueError, OSError):
                fenced = True  # Unreadable ownership is never permission to launch.
            b.closed = before or fenced
            raise
        self.batches.pop(b.id, None)
        self._durably_closed = getattr(self, "_durably_closed", set()) | {b.id}
        return retained

    @staticmethod
    def _matching_receipt(receipt, bid, name, identity):
        return (isinstance(receipt, dict) and receipt.get("ok") is True
                and receipt.get("reaped") is True and receipt.get("namespace_absent") is True
                and identity is not None and receipt.get("identity") == identity
                and receipt.get("batch") == bid and receipt.get("name") == name)

    @classmethod
    def _matching_terminal_reap(cls, it, evidence):
        """Only a native terminal receipt for this exact persisted placement is reusable."""
        pl = it.last_placement or {}
        return (isinstance(evidence, dict) and evidence.get("phase") == "terminal"
                and evidence.get("matched") is True and evidence.get("name") == it.name
                and it.owned.placements == 1 and pl.get("identity") == it.launch_identity
                and isinstance(pl.get("boot_id"), str) and bool(pl["boot_id"])
                and type(pl.get("attempt")) is int
                and all(k in pl and type(evidence.get(k)) is type(pl[k]) and evidence[k] == pl[k]
                        for k in ("run_name", "vm", "boot_id", "attempt", "identity"))
                and cls._matching_receipt(evidence.get("receipt"), it.batch.id,
                                          pl.get("run_name"), it.launch_identity))

    def cancel(self, bid, wait_seconds=10):
        self._valid_batch(bid)
        if getattr(self, "recovery_blocked", False):
            return {"ok": False, "complete": False, "error": "recovery quarantined; ownership held"}
        if isinstance(wait_seconds, bool) or not isinstance(wait_seconds, (float, int)) or not math.isfinite(wait_seconds) or not 0 <= wait_seconds <= 60:
            raise ValueError("wait_seconds must be between 0 and 60")
        deadline = time.monotonic() + wait_seconds
        with self.lock:
            b = self.batches.get(bid)
            if b is None:
                return self.close_receipt(bid)
            if not b.strict_once:
                return {"ok": False, "complete": False, "batch": bid,
                        "admission_closed": False,
                        "error": "owned cancellation requires strict_once; legacy close remains available"}
            # Set memory fence even on disk failure: fail closed in this process.
            b.admission_closed = True
            b.retain_journals = True
            for it in b.items.values():
                it.owned.cancel()
            with blobcache.locked(BLOBS):
                self._persist_fence(b)  # Serialize with completion-marker replacement too.
            items = list(b.items.values())
        for it in items:
            remaining = max(0, deadline - time.monotonic())
            if not it.start_done.wait(remaining):
                continue
            with self.lock:
                if it.owned.reaped:
                    continue
                vm = it.vm
                pl = it.last_placement
                if vm is None and pl:
                    vm = self.vms.get(pl["vm"])
                name = pl["run_name"] if pl and it.vm is None else it.run_name
                identity = it.launch_identity
                placed = it.owned.placements > 0
                cached = self._matching_terminal_reap(it, it.terminal_reap)
                receipt = it.terminal_reap["receipt"] if cached else None
            if receipt is None and placed and vm is not None and identity:
                try:
                    remaining = max(0, deadline - time.monotonic())
                    receipt = vm.client.rpc("cancelitem", bid, name, remaining, identity,
                                            timeout=max(1, remaining + 2))
                except Exception:
                    receipt = {"ok": False, "error": "cancel RPC failed or timed out"}
            matched = self._matching_receipt(receipt, bid, name, identity)
            with self.lock:
                # Persist evidence first; never acknowledge a reap whose journal failed.
                self._append_owned(bid, "cancel", {"name": it.name, "run_name": name,
                    "identity": identity, "matched": matched, "unplaced": not placed,
                    "source": "terminal-reap" if cached else "cancel-rpc", "receipt": {
                        k: receipt[k] for k in ("ok", "reaped", "namespace_absent", "identity", "batch", "name", "error") if k in receipt
                    } if isinstance(receipt, dict) else None, "t": time.time()})
                if it.owned.confirm(matched):
                    if vm is not None:
                        vm.running.pop((bid, name), None)
                    if it.state != "done" or not any(r.get("name") == it.name for r in b.rows):
                        it.state = "done"
                        it.fate.delivered()
                        self._deliver_owned(b, {"name": it.name, "code": -1, "seconds": -1,
                                                "cancelled": True, "launch_identity": identity})
                elif it.state != "done":
                    it.state = "cancel_pending"
        with self.lock:
            complete = all(it.owned.reaped for it in items)
            if complete:
                # Retention is not standing demand. Publish closed only after its
                # durable marker succeeds; a pending cancellation keeps its holds.
                b.closed = True
                try:
                    with blobcache.locked(BLOBS):
                        self._persist_fence(b)
                except Exception:
                    b.closed = False
                    raise
            self.wake.set()
            attempts = [{"name": it.name, "run_name": (it.last_placement or {}).get("run_name"),
                         "attempt": (it.last_placement or {}).get("attempt", it.owned.placements),
                         "vm": (it.last_placement or {}).get("vm"),
                         "boot_id": (it.last_placement or {}).get("boot_id"),
                         "launch_identity": it.launch_identity, "cleanup_complete": it.owned.reaped}
                        for it in items if it.owned.placements]
            receipt = self._close_receipt(b)
            if complete:
                with blobcache.locked(BLOBS):
                    self._retain_closed(b, receipt)
            return receipt

    def submit(self, msg: dict) -> dict:
        with self.lock:
            return self._submit_locked(msg)

    def _submit_locked(self, msg: dict) -> dict:
        if getattr(self, "recovery_blocked", False):
            return {"ok": False, "error": "recovery quarantined; ownership held"}
        self._full_now = True
        self._validate_submission(msg)
        msg = {**msg, 'result_files_protocol': 1}
        b = Batch(msg)
        if not hasattr(self, "_durably_closed"):
            self._durably_closed = self._scheduled_closes()
        if b.id in self._durably_closed or (DIR / "queue" / f"{b.id}.retired").exists() or (DIR / "queue" / f"{b.id}.closed").exists():
            return {"ok": False, "error": "durably closed batch id cannot be reused"}
        with self.lock:  # (rules.Submit: a batch with items queued or running is never replaced; the same submit again keeps it)
            old = self.batches.get(b.id)
            if old is None and (DIR / "queue" / f"{b.id}.json").exists():
                return {"ok": False, "error": "persisted admission exists; reload or repair required"}
            if old and (getattr(old, "strict_once", False) or getattr(old, "retain_journals", False) or getattr(old, "admission_closed", False)):
                return {"ok": False, "error": "durable owned batch id cannot be reused"}
            rule = rules.Submit()
            rule.exists, rule.live = old is not None, old is not None and not old.closed and old.left() > 0
            what = rule.admit(old is not None and set(old.items) == set(b.items))
        if what == "keep":
            # Name-only idempotency must never acknowledge a newly pinned job
            # while retaining an older, unpinned admission (or changed pins).
            for name, item in b.items.items():
                previous = old.items[name].spec
                if ("runtime" in item.spec or "runtime" in previous) and item.spec.get("runtime") != previous.get("runtime"):
                    return {"ok": False, "error": "runtime pins differ from the existing admission; use a new batch id"}
                from .agent import normalize_memory_limit
                if normalize_memory_limit(item.spec) != normalize_memory_limit(previous):
                    return {"ok": False, "error": "memory enforcement differs from the existing admission; use a new batch id"}
            return {"ok": True, "kept": True}
        if what == "refuse":
            log(f"{b.id} ({b.client}): a submit under the id of a batch with {old.left()} items live refused (never replaced)")
            return {"ok": False, "error": f"batch {b.id} has items queued or running: a submit never replaces it"}
        qd = DIR / "queue"
        qd.mkdir(parents=True, exist_ok=True)
        tmp = qd / f".{b.id}.json.tmp"  # (whole, then renamed in: a crash never leaves half a batch)
        with blobcache.locked(BLOBS):
            missing = [k for k in set(b.tree["files"].values()) if not (BLOBS / k).is_file()]
            if missing:
                return {"ok": False, "error": "tree blobs evicted before admission; upload again", "missing": missing}
            if any(durable_item(it) for it in b.items.values()):
                msg = self._bind_durable(msg)
                b = Batch(msg)
                self._attach_durable(b)
            b.cache_generation = secrets.token_hex(16)
            admission = json.dumps({**msg, "t0": b.t0, "cache_generation": b.cache_generation,
                                    "cache_completed_generation": None})
            if getattr(b, "strict_once", False) or getattr(b, "retain_journals", False) or any(durable_item(it) for it in b.items.values()):
                with open(tmp, "w") as f:
                    f.write(admission)
                    f.flush()
                    os.fsync(f.fileno())
            else:
                tmp.write_text(admission)
            tmp.rename(qd / f"{b.id}.json")
            if getattr(b, "strict_once", False) or getattr(b, "retain_journals", False) or any(durable_item(it) for it in b.items.values()):
                fd = os.open(qd, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        with self.lock:
            self.batches[b.id] = b
        log(f"{b.id} ({NAMES[b.klass]}, {b.client}): {len(b.items)} items on {b.image}")
        self.wake.set()
        return {"ok": True}

    def batch_rows(self, bid: str, since: int) -> dict:
        """Read a page without renewing the admission heartbeat or waiting."""
        self._valid_batch(bid)
        if type(since) is not int or since < 0:
            raise ValueError("result cursor must be a nonnegative integer")
        if bid in self.batches:
            return self.poll(bid, since, 0, touch=False)
        with self.lock:
            if bid not in self.batches:
                retained = history.read(DIR / "queue", bid)
                if retained is not None:
                    rows = history.rows_page(DIR / "queue", bid, since, retained)
                    return {"rows": rows, "done": retained["complete"] and since + len(rows) >= retained["rows"],
                            "next_since": since + len(rows), "running": 0, "queued": 0}
        return self.poll(bid, since, 0, touch=False)

    def poll(self, bid: str, since: int, wait: float, peer_closed=None, *, touch=True) -> dict:
        wait = rules.http_poll_wait(wait, HTTP_POLL_MAX_WAIT_S)
        b = self.batches.get(bid)
        if b is None:
            if getattr(self, "recovery_blocked", False) or self._unclassified(bid):
                # The client must keep waiting through quarantine/hand-back.
                # done=True here loses every result without killing a worker.
                self.stop.wait(min(max(wait, 0), 1.0))
                return {"rows": [], "done": False, "running": 0, "queued": 0, "recovering": True}
            return {"rows": [], "done": True, "err": "no such batch"}
        if touch:
            b.t_poll = time.time()
        with b.cv:
            # (rules.Delivery: done to the client only once every item's row is in and it has read them all; it waits
            # while a row may still come, an item done with its row not yet delivered among them)
            names = {r.get("name") for r in b.rows} & set(b.items)
            deadline = time.monotonic() + wait
            while len(b.rows) <= since and len(names) < len(b.items) and not self.stop.is_set():
                if peer_closed is not None and peer_closed():
                    raise ConnectionAbortedError("poll peer disconnected")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                b.cv.wait(min(remaining, HTTP_POLL_PEER_CHECK_S))
                names = {r.get("name") for r in b.rows} & set(b.items)
            rows = resultfiles.page(b.rows, since) if since >= 0 else []
            d = rules.Delivery(len(b.items))
            d.finished = len(b.items) - b.left()
            d.rows = min(len(names), d.finished)
            done = d.poll_page(since, len(rows))
        running = sum(1 for it in b.items.values() if it.state in ("starting", "running"))
        return {"rows": rows, "done": done, "next_since": since + len(rows), "running": running, "queued": sum(1 for it in b.items.values() if it.state == "queued")}

    def result_part(self, bid, ref, offset=0, length=resultfiles.CHUNK_BYTES):
        """Authenticated, bounded reads from controller-owned immutable files."""
        self._valid_batch(bid)
        q = DIR / "queue"
        if bid not in self.batches and ((q / f"{bid}.retired").exists() or (q / f"{bid}.closed").exists()):
            if history.read(q, bid) is None:
                raise ValueError("closed history missing or expired")
        return self._result_store(bid).read(ref, offset, length)

    @staticmethod
    def batch_of(m) -> str | None:
        """The batch a request names: "batch", or "id" as an older record had it; None when it names none (an empty
        or cut body: 09:50, three /close answered KeyError('batch'), 500s counted as our errors)."""
        return (m.get("batch") or m.get("id")) if isinstance(m, dict) else None

    def close(self, bid: str) -> dict:
        self._valid_batch(bid)
        if getattr(self, "recovery_blocked", False):
            return {"ok": False, "error": "recovery quarantined; ownership held"}
        kill = []
        with self.lock:
            b = self.batches.get(bid)
            retain = b is not None and (b.retain_journals or b.strict_once or b.admission_closed)
            if not retain:
                self._full_now = True
                if b is None:
                    retained = history.read(DIR / "queue", bid)
                    if retained is not None and retained.get("strict_once") is True:
                        return history.payload(DIR / "queue", bid, "close.json", retained)
                    return {}
                # Same lock order as submit/cancel. Completion must finish its
                # replacement before deletion, never republish after close.
                with blobcache.locked(BLOBS):
                    receipt = {"batch": bid, "closed": True, "strict_once": False,
                               "cleanup_complete": False, "note": "legacy asynchronous close; no namespace attestation"}
                    self._retain_closed(b, receipt)
                    # Archive first; only then release active item ownership.
                    for it in b.items.values():  # (a client that gives up: what still runs is killed, what's queued goes)
                        if it.state in ("starting", "running") and it.vm:
                            kill.append((it.vm, it.run_name))
                        it.state = "done"
                        it.fate.delivered()
                    b.closed = True
                    self.batches.pop(bid, None)
                    self._durably_closed = getattr(self, "_durably_closed", set()) | {bid}
        if retain:
            return self.cancel(bid)
        for vm, rn in kill:  # (the kills outside the lock, in the background: status never waits on them; one lost, the VM's tick kills it later)
            threading.Thread(target=self._kill_run, args=(vm, b.id, rn), daemon=True).start()
        s = {"vms": len(b.vms_used), "starts": b.starts, "preempted": b.preempted, "class": NAMES[b.klass], "wall_s": round(time.time() - b.t0, 1)}
        first = round(b.t_first - b.t0, 1) if b.t_first else None
        placed = round(b.t_placed - b.t0, 1) if b.t_placed else None
        with self.lock:
            self.m["batches"] = (self.m["batches"] + [{"id": b.id, "class": NAMES[b.klass], "client": b.client, "items": len(b.items), "starts": b.starts,
                                                       "vms": len(b.vms_used), "first_s": first, "placed_s": placed, "wall_s": s["wall_s"], "t": round(time.time())}])[-2000:]
        boat.ledger(batch=b.id, label=b.label, client=b.client, klass=NAMES[b.klass], starts=b.starts, vms=len(b.vms_used), slots=0, scheduled=True, slot_s=round(b.slot_s, 1),
                    preempted=b.preempted, wall_s=s["wall_s"], first_s=first, placed_s=placed, items=len(b.items), acct=bool(b.starts))
        self.wake.set()
        return s

    def logs(self, bid: str, vms: list[str], *, require_complete: bool = False) -> bytes:
        """The batch's logs: each item's, kept here as it finished (they outlive a restart), and the VMs' for any
        not kept, merged (one tar.gz)."""
        out = io.BytesIO()
        kept = DIR / "queue" / f"{bid}.logs"
        have, included = set(), set()
        # (an item's log is the log of the run that delivered its row: a VM's log of another run of a delivered item, a
        # first try lost there, is left out; the client maps every run of an item to its name, the last one read won)
        b = self.batches.get(bid)
        rows = list(b.rows) if b else self._read_result_journal(DIR / "queue" / f"{bid}.rows", False)
        delivered = {r.get("name"): r.get("run_name") or r.get("name") for r in rows if r.get("name")}
        if require_complete:
            retained = history.read(DIR / "queue", bid) if b is None else None
            if (b is None and retained is None) or vms:
                raise ValueError("complete retained logs require an existing admission and no worker fallback")
            expected = {r.get("run_name") or r.get("name") for r in rows if r.get("vm") or r.get("run_name")}
            for name in expected:
                if not isinstance(name, str) or not name or name in (".", "..") or any(c in name for c in ("/", "\\", "\0")):
                    raise ValueError("invalid retained log run name")
                for stream in ("out", "err"):
                    path = kept / name / stream
                    if path.is_symlink() or not path.is_file():
                        raise ValueError(f"retained logs incomplete: {name}/{stream}")
        with tarfile.open(fileobj=out, mode="w:gz") as tf:
            for d in sorted(kept.iterdir()) if kept.exists() else []:
                for f in ("out", "err"):
                    if (d / f).exists():
                        data = (d / f).read_bytes()
                        ti = tarfile.TarInfo(f"{d.name}/{f}")
                        ti.size = len(data)
                        tf.addfile(ti, io.BytesIO(data))
                        included.add((d.name, f))
                have.add(d.name)
            for v in vms:
                vm = self.vms.get(v)
                cl = vm.client if vm and vm.client else boat.agent_client(v)  # (a VM no longer held: its agent still answers)
                if not cl:
                    continue
                with contextlib.suppress(Exception):
                    data = cl.raw("logs", bid, timeout=120)
                    with tarfile.open(fileobj=io.BytesIO(data)) as src:
                        for m in src.getmembers():
                            rn = m.name.split("/")[0]
                            base = rn.rsplit("~s", 1)[0]
                            if rn not in have and delivered.get(base, rn) == rn:
                                tf.addfile(m, src.extractfile(m))
        if require_complete and any((name, stream) not in included for name in expected for stream in ("out", "err")):
            raise ValueError("retained logs changed during snapshot")
        return out.getvalue()

    def hint(self, msg: dict) -> dict:
        """A tenant's standing demand (the treadmill: the pool's floor, while it lives)."""
        self.hints[msg["client"]] = {**msg, "until": time.time() + float(msg.get("ttl", 300))}
        self.wake.set()
        return {"ok": True}

    def floor(self, image: str) -> int:
        """rules.image_floor: the policy's floor while a treadmill (a tenant's hint) keeps work coming for the image, or
        floor_always; else the default floor (1) while the image had real work within floor_idle_s; floor_by_image's
        number for an image instead of either."""
        now = time.time()
        tm = FLOOR_ALWAYS or self.treadmill_alive(image)
        busy = any(b.image == image and b.klass <= 4 and not b.closed for b in list(self.batches.values()))
        last = now if busy else max([v.t_real for v in list(self.vms.values()) if v.image == image and not v.dead] or [0.0])
        return rules.image_floor(tm, int(self.pol["floor"]), FLOOR_DEFAULT, FLOOR_BY_IMAGE.get(image, -1), now - (last or 0.0), FLOOR_IDLE)

    def treadmill_alive(self, image: str) -> bool:
        """The treadmill registered for the image: its hint alive, or a batch of a sweep or filler of the image open."""
        now = time.time()
        return any(h["until"] > now and h.get("image") == image for h in self.hints.values()) or any(b.klass >= 4 and b.image == image and not b.closed for b in list(self.batches.values()))

    def _drain(self, image: str) -> rules.Drain:
        """The image's staged drain (rules.Drain), its step and window the policy's, re-read live."""
        d = self.__dict__.setdefault("_drains", {}).get(image)
        step, every = max(1, min(100, round(100 * DRAIN_STEP))), max(1, int(DRAIN_EVERY))
        if d is None or (d.step_pct, d.every_s) != (step, every):
            nd = rules.Drain(step, every)
            saved = self.__dict__.get("_drains_saved", {}).pop(image, None) if d is None else None
            if d is not None:
                nd.open, nd.t_last, nd.share, nd.quota, nd.released = d.open, d.t_last, d.share, d.quota, d.released
            elif saved:  # (the previous scheduler's window: a handoff opens none afresh)
                nd.open, nd.t_last, nd.share, nd.quota, nd.released = bool(saved["open"]), float(saved["t_last"]), int(saved["share"]), int(saved["quota"]), int(saved["released"])
            d = self._drains[image] = nd
        return d

    def status_response(self) -> bytes:
        """The HTTP hot path: immutable bytes plus honest age; no scheduler/health/I/O locks."""
        started = time.monotonic()
        view = getattr(self, '_status_view', None)
        if view is None:  # incomplete test/boot object: never build synchronously for a reader
            view = statusview.PublishedStatus({'pid': os.getpid(), 'code': getattr(self, 'code', None)})
        raw = view.response()
        timings = getattr(self, '_status_timings', None)
        if timings is not None:
            timings.append((time.time(), time.monotonic() - started))
        return raw

    def status(self) -> dict:
        """Compatibility for in-process callers; HTTP uses status_response directly."""
        return json.loads(self.status_response())

    def _flush_status_timings(self):
        timings = getattr(self, '_status_timings', None)
        if timings is None:
            return
        for _ in range(20000):
            try:
                stamp, elapsed = timings.popleft()
            except IndexError:
                break
            self._h().record_status(stamp, elapsed)

    def _refresh_status(self, blocking=False) -> bool:
        """One publisher, never a request: capture memory under the lock, encode off it."""
        guard = getattr(self, '_status_refresh_lock', None)
        if guard is None or not guard.acquire(blocking=False):
            return False
        try:
            self._flush_status_timings()
            # Filesystem work belongs to the publisher, not the response path or
            # Scheduler.lock. A slow read leaves the last view and age intact.
            installed = code_hash()
            blobs = blobcache.last_report(BLOBS)
            if not self.lock.acquire(blocking=blocking):
                return False
            try:
                started_ms, captured_at = time.monotonic_ns() // 1000000, time.time()
                self._status_installed_code = installed
                r = copy.deepcopy(self._status_read())
                r['recovery'] = copy.deepcopy(getattr(self, 'recovery', {'errors': getattr(self, 'recovery_errors', {})}))
                r['recovery_blocked'] = getattr(self, 'recovery_blocked', False)
                pending = getattr(self, '_handoff_pending', None)
                gate = self._handoff_gate()
                r['handoff_pending'] = ({'target': pending['target'], 'generation': pending['generation'],
                    'phase': 'starts' if gate.freezing else 'pool',
                    'age_s': round(max(0, time.monotonic() - gate.started), 3),
                    'remaining_s': round(max(0, gate.deadline - time.monotonic()), 3)} if pending else None)
            finally:
                self.lock.release()
            r['blob_cache'] = blobs
            cache = getattr(self, '_state_archive', None)
            r['state_backup'] = cache.status() if cache is not None else {}
            last = getattr(self._h(), 'last', None) or {}
            r['health'] = {'ok': last.get('ok'),
                           'breached': sorted(k for k, v in last.get('targets', {}).items() if not v['ok']),
                           'client_4xx': self._h().client_errors(captured_at) or {'count': 0, 'by_status': {}, 'recent': []},
                           'memory': self._h().memory_status(captured_at)}
            return self._status_view.publish(r, started_ms, captured_at)
        except Exception as exc:
            self._status_view.failed(exc)
            return False
        finally:
            guard.release()

    def _start_status_publisher(self):
        if getattr(self, '_status_publisher', None) is not None:
            return
        def publish():
            while not self.stop.is_set():
                self._refresh_status()
                if self.stop.wait(1.0):
                    return
        self._status_publisher = threading.Thread(target=publish, name='status-publisher', daemon=True)
        self._status_publisher.start()

    def _status_read(self) -> dict:
        """Capture live scheduler state for the publisher while Scheduler.lock is held."""
        now = time.time()
        if True:
            live = [v for v in self.vms.values() if not v.dead]
            per, qk = {}, {}
            for b in list(self.batches.values()):  # (the queue counted once, not once a class)
                qk[b.klass] = qk.get(b.klass, 0) + sum(1 for it in b.items.values() if it.state == "queued")
            for n, k in CLASSES.items():
                q = qk.get(k, 0)
                held = sum(1 for v in live if any(x.batch.klass == k for x in v.running.values()))
                w = self.m["waits"].get(n, [])
                per[n] = {"queued": q, "vms": held, "wait_p50": pct(w, 0.5), "wait_p95": pct(w, 0.95), "starts_free": self.m["starts"][n]["free"],
                          "starts_paid": self.m["starts"][n]["paid"], "preempted": self.m["preempted"].get(n, 0), "placed": self.m["placed"].get(n, 0),
                          "boat_fails": self.m["boat_fails"].get(n, 0), "to_modal": self.m["to_modal"].get(n, 0)}
            starts = sum(s["free"] + s["paid"] for s in self.m["starts"].values())
            items = sum(self.m["placed"].values())
            return {"pid": os.getpid(), "batches": [{"id": b.id, "class": NAMES[b.klass], "client": b.client, "left": b.left(),
                                                     "running": sum(1 for it in b.items.values() if it.state in ("starting", "running"))} for b in self.batches.values()],
                    "vms": [{"id": v.id, "image": v.image, "type": v.type, "ready": v.ready and not v.quarantine, "model": v.model, "used": v.used(), "slots": v.slots, "draining": v.draining, "real_ago": round(now - v.t_real), "gen": v.gen, "agent": v.agent,
                             "agent_capabilities": dict(getattr(v, "agent_capabilities", {})), "agent_upgrading": bool(getattr(v, "upgrading", False)),
                             "quarantined": bool(v.quarantine), "quarantine_reason": v.quarantine,
                             "quarantine_since": v.quarantine_since, "failing": v.failing,
                             "classes": sorted({NAMES[it.batch.klass] for it in v.running.values()}), "foreign": getattr(v, "foreign", 0),
                             "runs": [{"batch": it.batch.id, "name": it.run_name, "class": NAMES[it.batch.klass]} for it in v.running.values()]} for v in live],
                    "constraints": self._constraint_status(),
                    "classes": per,
                    "pool": {"vms": len(live), "ready": sum(1 for v in live if v.ready and not v.quarantine), "quarantined": sum(bool(v.quarantine) for v in live), "coming": sum(self.creating.values()) + sum(1 for v in live if not v.ready),
                             "target": {k: v for k, v in self.target.items()}, "max": None if MAX_VMS >= 10 ** 9 else MAX_VMS, "floor": {img: self.floor(img) for img in {v.image for v in live}}},
                    "day": {"day": self.m["day"], "vmh": round(self.m["vmh"], 2), "vmh_filler_hold": round(self.m["vmh_filler_hold"], 2),
                            "starts": starts, "items_placed": items, "starts_per_100_items": round(100 * starts / items, 1) if items else None,
                            "items_placed_warm": self.m["placed_warm"], "max_binds": self.m.get("max_binds", 0), "hold_ends": self.m["hold_ends"], "peak_vms": self.m["peak_vms"],
                            "breaker_trips": self.m["breaker_trips"], "not_ready": self.m["not_ready"]},
                    "breaker": {**self.br, "enabled": self._breaker_enabled(), "half": self._breaker_enabled() and self.br["half"],
                                 "open": now < rules.breaker_deadline(self._breaker_enabled(), self.br["open_until"]),
                                 "open_for_s": max(0, round(rules.breaker_deadline(self._breaker_enabled(), self.br["open_until"]) - now))},
                    "resume_quarantine": dict(getattr(self, "resume_quarantine", {})),
                    "shed": self.shed, "load": self.load, "bring_up_s": round(self.bring_up, 1), "alerts": self.alerts[-5:],
                    "policy": self.pol, "hot_handoff": 1, "code": self.code, "code_installed": getattr(self, "_status_installed_code", self.code), "agent_code": self.agent_version, "stale": self.stale(),
                    "agents": {v.id: v.agent for v in live},
                    "knobs": {"bringup": BRINGUP_MAX, "break_even_override_s": BREAK_EVEN_OVERRIDE_S or None, "start_dollars": START_DOLLARS, "vm_hour_dollars": VM_HOUR_DOLLARS, "gate_horizon_s": GATE_HORIZON, "hold_s": HOLD, "sub_vms": SUB_VMS, "max_vms": None if MAX_VMS >= 10 ** 9 else MAX_VMS, "floor": self.pol["floor"]},
                    # (the old keys, for older clients)
                    "starts_by_class": {n: self.m["starts"][n]["free"] + self.m["starts"][n]["paid"] for n in CLASSES}, "preempted": sum(self.m["preempted"].values()),
                    "hints": {k: {kk: vv for kk, vv in h.items() if kk != "pid"} for k, h in self.hints.items() if h["until"] > now}}

    # -- the loop
    def run(self):
        self._start_status_publisher()
        # ACK means loaded and serving, not permission to mutate the pool.
        # Until the parent exits it may reject us and resume its original state.
        while not self._activate_handoff():
            if self.stop.wait(0.05):
                return
        threading.Thread(target=self._heartbeat, daemon=True).start()
        threading.Thread(target=self._sampler, daemon=True).start()
        t_loop = time.time()
        while not self.stop.is_set():
            self.wake.wait(0.5)
            self.wake.clear()
            if getattr(self, "handing_off", False):
                t_loop = time.time()
                continue
            if getattr(self, "recovery_blocked", False):
                self._handoff_check(time.time())
                continue
            if rules.woke(time.time() - t_loop, 60):  # (this machine slept: every VM checked, its TTL renewed, orphans adopted, now)
                self.t_wake = time.time()
                log(f"woke after {time.time() - t_loop:.0f} s asleep: TTLs renewed, adoption now, VMs boat stopped meanwhile let go with no strike to their items")
                for v in list(self.vms.values()):
                    if not v.dead:
                        threading.Thread(target=boat.boat, args=("extend", v.id, "--ttl", str(max(boat.BACKSTOP, v.ttl)), "--json"), kwargs={"check": False}, daemon=True).start()
                if not getattr(self, "adopting", False):
                    self.adopting = True
                    threading.Thread(target=self._adopt, daemon=True).start()
            t_loop = time.time()
            c0 = _cpu_now()
            with contextlib.suppress(Exception):
                self._policy_tick()
            self._blob_cache_tick()
            self._history_tick()
            t_s = time.time()
            # (a full pass (the queue ordered, preemption, waits, the plan, the release) at most every FULL_S, or at once
            # when a batch comes or goes; between them a light one places what's queued on the slots items freed, in the
            # last full pass's order: 08:50, a full pass at every finished item, 3.2 s of a core each, held the lock the
            # ticks need)
            # (and never sooner than 4 times the last full pass took: its share of a core a fifth at most, however big the queue)
            since = time.time() - getattr(self, "_t_full", 0.0)
            light = getattr(self, "_q_full", None) is not None and (since < 4 * getattr(self, "_dt_full", 0.0) or (not getattr(self, "_full_now", True) and since < FULL_S))
            try:
                self.schedule(light=light)
            except Exception as e:
                log(f"schedule: {e!r}"[:400])
            finally:
                self._q_pass = None
                self._cpu_add("pass light" if light else "pass", c0)
                if not light:
                    self._dt_full = _cpu_now() - c0 if _cpu_now() else time.time() - t_s
            dt = time.time() - t_s
            if dt > 0.02:  # (a long pass (a big queue) is followed by a pause 3 times as long: the passes at most a quarter of a core,
                self.stop.wait(min(1.5, 3 * dt))  # its threads and status answered; 08:14, 59% of a core at 180 VMs and 8.8k items queued)
            else:  # (passes at most 4 a second: the wakes of items finishing together are one pass; 09:19, 15 light passes a second)
                self.stop.wait(max(0.0, 0.25 - dt))
            # (a batch still running waits 6 hours for its client, a finished one a day: looked at once a minute)
            if time.time() - getattr(self, "_t_abandon", 0) >= 60:
                self._t_abandon = time.time()
                for b in [b for b in list(self.batches.values()) if rules.abandoned(time.time(), getattr(b, "t_poll", b.t0), b.left(), ORPHAN_S)]:
                    log(f"{b.id}: no client has asked for it in {ORPHAN_S} s: closed")  # (a client gone for good)
                    self.close(b.id)
            with self.lock:
                busy = bool(self.batches) or any(v.running for v in self.vms.values()) or not self._pool_settled()
            if not busy:  # (a floor VM held: the scheduler stays to keep it; exiting releases every VM, the floor's too)
                busy = any(self.floor(img) > 0 for img in {v.image for v in list(self.vms.values()) if not v.dead})
            if busy:
                self.t_idle = time.time()
            elif self.idle_exit and time.time() - self.t_idle > self.idle_exit:
                log(f"nothing queued for {self.idle_exit} s: exiting")
                break
        while getattr(self, "handing_off", False):  # (a handoff: its thread exits this process once the next one holds every VM;
            time.sleep(1)  # never shutdown()'s let-go meanwhile: before, it raced the handoff's exit, VMs let go to adoption)
        self.shutdown()

    def _history_tick(self):
        """One finite maintenance worker; at most 128 manifests per minute."""
        if self.stop.is_set() or getattr(self, "handing_off", False) or getattr(self, "_history_busy", False):
            return
        deferred = getattr(self, '_handoff_deferred', None)
        guarded = deferred is not None
        alive = bool(guarded and boat.pid_alive(deferred[0], socket.gethostname()))
        from .rules.history import history_migration_allowed
        if not history_migration_allowed(guarded, alive, getattr(self, 'recovery_blocked', False)):
            return
        if time.time() - getattr(self, "_history_at", 0) < 60:
            return
        self._history_at, self._history_busy = time.time(), True
        def run():
            try:
                for bid in sorted(getattr(self, '_history_pending', set()))[:128]:
                    with self.lock:
                        if getattr(self, 'handing_off', False):
                            break
                        b = self.batches.get(bid)
                        if b is not None and b.closed and all(it.owned.reaped for it in b.items.values()):
                            self._retain_closed(b, self._close_receipt(b))
                        self._history_pending.discard(bid)
                # Reclaim crash leftovers only after the manifest moved last.
                garbage = DIR / "queue" / '.expired'
                for trash in itertools.islice(garbage.iterdir() if garbage.exists() else (), 128):
                    if (trash / (trash.name + '.closed')).is_file():
                        history.purge(trash)
                iterator = getattr(self, "_history_scan", None)
                if iterator is None:
                    iterator = self._history_scan = iter((DIR / "queue").glob("*.closed"))
                for _ in range(128):
                    path = next(iterator, None)
                    if path is None:
                        self._history_scan = None
                        break
                    with self.lock:
                        if getattr(self, "handing_off", False):
                            break
                        trash = history.collect(DIR / "queue", path.stem, detach=True)
                    if trash:
                        history.purge(trash)
            except Exception as exc:
                self._history_scan = None
                log(f"closed history: {exc!r}")
            finally:
                self._history_busy = False
        threading.Thread(target=run, daemon=True).start()

    def _blob_cache_tick(self):
        """One bounded maintenance worker, never on the placement thread or during handoff."""
        if self.stop.is_set() or getattr(self, "handing_off", False) or getattr(self, "adopting", False):
            return
        if getattr(self, "_blob_cache_busy", False) or time.time() - getattr(self, "_blob_cache_at", 0) < 300:
            return
        self._blob_cache_at, self._blob_cache_busy = time.time(), True
        policy = dict(self.pol)

        def run():
            try:
                with self.lock:
                    batches = list(self.batches.values())
                for batch in batches:
                    blobcache.completed(BLOBS, batch.id, getattr(batch, "cache_generation", None), batch.left())
                report = blobcache.collect(BLOBS, int(policy["blob_cache_bytes"]), float(policy["blob_cache_grace_s"]),
                                           float(policy["blob_cache_max_age_s"]),
                                           cancelled=lambda: self.stop.is_set() or getattr(self, "handing_off", False))
                if report.get("removed_files") or report.get("error"):
                    log(f"blob cache: {report}")
            except Exception as e:
                log(f"blob cache: {e!r}")
            finally:
                self._blob_cache_busy = False
        threading.Thread(target=run, daemon=True).start()

    def _policy_tick(self):
        """The policy file read again when it changed: its knobs in force at once, for every VM and item held (their
        TTLs renewed to the new ttl_s, holds and growth by the new numbers); each VM and running item stamped with the
        generation it now follows."""
        mt = POLICY.stat().st_mtime_ns if POLICY.exists() else 0
        if mt != self.pol_mtime:
            self.pol_mtime = mt
            new = read_policy()
            if new.get("generation") != self.pol.get("generation"):
                diff = {k: (self.pol.get(k), v) for k, v in new.items() if self.pol.get(k) != v and k not in ("t", "generation")}
                self.pol = new
                apply_policy(new)
                log(f"policy generation {new['generation']} in force: {diff}")
                if "ttl_s" in diff:
                    # A policy reduction must not blindly shorten a running VM.
                    # Its old backstop survives until the fenced release path.
                    for v in list(self.vms.values()):
                        if not v.dead:
                            v.ttl = max(v.ttl, boat.BACKSTOP)
                            threading.Thread(target=boat.boat, args=("extend", v.id, "--ttl", str(v.ttl), "--json"), kwargs={"check": False}, daemon=True).start()
        g = self.pol.get("generation", 0)
        for v in list(self.vms.values()):
            v.gen = g
            for x in list(v.running.values()):
                x.fate.follow(g)  # (rules.ItemFate.follow)

    def stale(self) -> dict:
        """What isn't on the policy generation or the code in force (zero one tick after a change)."""
        g = self.pol.get("generation", 0)
        live = [v for v in self.vms.values() if not v.dead]
        return {"vms_old_policy": sum(1 for v in live if v.gen != g), "items_old_policy": sum(1 for v in live for x in v.running.values() if x.gen != g),
                "agents_old_code": sorted(v.id for v in live if v.ready and v.agent and v.agent != self.agent_version)}

    def queued(self) -> list[Item]:
        """The queue in placement order: by class; within a class, round-robin across clients (each client's next item
        ranked by what it already runs in that class), then by batch age, longest first."""
        # (an item re-attach is still to take back where it runs isn't placed elsewhere meanwhile: B10, 02:24-02:55 a
        # 10-min gate item placed again at each of 5 handovers, its runs left running beside new items)
        pend = getattr(self, "reattach_pending", None) or set()
        for b in list(self.batches.values()):
            if b.admission_closed:
                continue
            for it in b.items.values():
                if (b.strict_once and it.owned.placements and it.state == "queued"
                        and not (getattr(it, "reattach", None) and it.reattach.get("vm") in pend)):
                    self._owned_failed(it, "strict_once: prior placement could not be recovered")
        q = [it for b in list(self.batches.values()) if not b.admission_closed for it in b.items.values() if it.state == "queued" and not it.owned.closed
             and not (pend and getattr(it, "reattach", None) and it.reattach.get("vm") in pend)]
        run: dict = {}
        for v in list(self.vms.values()):
            for x in list(v.running.values()):
                run[(x.batch.klass, x.batch.client)] = run.get((x.batch.klass, x.batch.client), 0) + 1
        groups: dict = {}
        for it in q:
            groups.setdefault((it.batch.klass, it.batch.client), []).append(it)
        # (each client's items sorted, then the clients merged: n log(clients), not one sort of every item)
        import heapq
        lanes = []
        for (k, c), its in groups.items():
            its.sort(key=lambda it: (not it.head, it.batch.t0, -it.est))
            base = run.get((k, c), 0)
            lanes.append([((k, base + i, its[i].batch.t0, n), its[i]) for i in range(len(its)) for n in (len(lanes),)])
        return [it for _, it in heapq.merge(*lanes, key=lambda x: x[0])]

    def _pass_queue(self) -> list:
        """The queue as this pass of schedule() ordered it, what's still queued of it (the plan and the release read
        it again rather than sort 26,000 items three times a pass)."""
        q = getattr(self, "_q_pass", None)
        if q is None:
            q = self.queued()
        return [it for it in q if it.state == "queued" and distinct_live(it)]

    def schedule(self, light: bool = False):
        if getattr(self, "recovery_blocked", False):
            return
        now = time.time()
        self._med = {}
        with self.lock:
            if getattr(self, "handing_off", False) or not self._handoff_gate().placement_allowed():
                return
            for bid in tuple(getattr(self, "_durable_batch_ids", ())):
                b = self.batches.get(bid)
                if b is None:
                    self._durable_batch_ids.discard(bid)
                    continue
                for it in b.items.values():
                    if it.state == "queued" and durable_item(it) and not distinct_live(it):
                        self._owned_failed(it, "durable group generation expired or replaced; no placement or fallback")
            if light:
                q = [it for it in self._q_full if it.state == "queued"]
            else:
                self._full_now, self._t_full = False, now
                q = self.queued()
                self._q_full = q
            self._q_pass, self._any_queued = q, bool(q)
            # 1. place queued items on free slots, highest class first (none while draining for a deploy); a submission
            # client up to SUB_VMS VMs' worth of slots at once
            sub_run: dict = {}
            for v in self.vms.values():
                for x in v.running.values():
                    if x.batch.klass == 3:
                        sub_run[x.batch.client] = sub_run.get(x.batch.client, 0) + x.weight
            free = sum(max(0, v.slots - v.used()) for v in self.vms.values() if v.ready and not v.dead)
            # (an item of a shape no VM took this pass is passed over: a pass only takes slots, so none takes the next of
            # that shape either; 08:41, ~1 s a pass, each of 8.8k items looked for a VM among 181 while 3 slots nothing fit
            # stayed free. An item a VM is set aside for, or one backing off, is its own shape)
            mine = {id(r[0]) for v in self.vms.values() for r in (getattr(v, "reserved", None),) if r and self._held(v, None, now)}
            no_vm: set = set()

            def shape_of(it):  # (fit_shape: all a VM's fit reads, its memory too; 17:10-18:47 a memory-blind memo passed over items that fit)
                return None if id(it) in mine or now < it.not_before else fit_shape(it)
            # (a submission client's share binds only against a waiter that could take the slot (rules.contends): of its
            # image, of a shape a VM took this pass (or not yet tried), not backing off; of a higher class, or another
            # submission client under its own share. Past every share, the slots go to the submissions before sweeps and
            # filler: 03:45, three weight-4 clients at 8 VMs each while most of 126 VMs ran sweeps; 11-01, any waiting
            # gate of any image, placeable or not, held them at their share)
            waiting = {}
            for x in q:
                if x.batch.klass <= 3:
                    waiting.setdefault((x.batch.image, x.batch.klass, x.batch.client), x)
            under = lambda c, x: rules.within_share(3, sub_run.get(c, 0), x.weight, SUB_VMS * vm_slots(x.batch.vm_type), True)
            cont: dict = {}  # (each client's answer once until a submission's placement moves the shares, or a shape finds no VM)

            def placeable(x):
                if now < x.not_before:
                    return False
                sh = shape_of(x)
                return sh is None or sh not in no_vm

            def contended(it):
                key = (it.batch.image, it.batch.client)
                if key not in cont:
                    cont[key] = any(rules.contends(img == it.batch.image, placeable(x), k, c != it.batch.client, k == 3 and under(c, x))
                                    for (img, k, c), x in waiting.items())
                return cont[key]
            self._contended = contended  # (health's waste: a submission held by its share is not one placeable)
            for it in ([] if getattr(self, "draining", False) else q):
                if free <= 0:
                    break
                shape = shape_of(it)
                if shape is not None and shape in no_vm:
                    continue
                if not rules.within_share(it.batch.klass, sub_run.get(it.batch.client, 0), it.weight, SUB_VMS * vm_slots(it.batch.vm_type), contended(it)):
                    continue
                vm = self._slot(it)
                if vm is None and shape is not None:
                    no_vm.add(shape)
                    cont.clear()
                if vm and self._place(it, vm):
                    free -= vm.slots if it.timing else it.weight
                    if it.batch.klass == 3:
                        sub_run[it.batch.client] = sub_run.get(it.batch.client, 0) + it.weight
                        cont.clear()
            q = [it for it in q if it.state == "queued"]
            if light:
                return
            for b in self.batches.values():
                if b.t_first and not b.t_placed and not any(it.state == "queued" for it in b.items.values()):
                    b.t_placed = now
            # 2. preemption: interactive and gate items take fillers' and sweeps' slots at once; a submission's waiting
            # past SUB_PREEMPT a filler's (else it gets slots as items end)
            pp = rules.PreemptPass()  # (an item that finds nothing lower to take: those of its shape after it are passed over, no
            for it in [it for it in q if it.batch.klass <= 4]:  # other; before, the first such item ended the pass for all)
                if it.batch.klass == 3 and not rules.within_share(3, sub_run.get(it.batch.client, 0), 0, SUB_VMS * vm_slots(it.batch.vm_type) - 1, contended(it)):
                    continue
                shape = f"{rules.victims(it.batch.klass, now - it.t_created, SUB_PREEMPT)}|{fit_shape(it)}"
                if not pp.admit(shape):
                    continue
                back = now < it.not_before  # (backing off: it doesn't try, nor passes its shape over for the rest, rules.PreemptPass.after)
                pp.after(shape, back, not back and self._preempt_for(it, now))
            # 3. items that waited past their batch's max_wait with no VM ready: one of boat's failures of the item only
            # when boat failed a start of its image during that wait (refused, or not ready within BREAK_READY); a wait
            # the scheduler made itself (the breaker, a backoff, the bring-up cap) is not boat failing it, and the item
            # keeps its place. BOAT_TRIES of boat's failures, then back to its client for Modal
            for it in q:
                b = it.batch
                if b.on_lost and b.max_wait and now - it.t_queued > b.max_wait and not any(v.ready and v.image == b.image for v in self.vms.values()):
                    strike = it.fate.waited(self._start_failed(b.image, it.t_queued, now))  # (rules.ItemFate.waited)
                    if strike != "none":
                        it.t_queued = now
                        self._boat_failed(it, None, f"no boat VM ready within {int(b.max_wait)} s, a start of {b.image} failed in it", strike)
            # 4. capacity: the plan (demand-driven), within the bring-up cap, the breaker and the watchdog
            want = self._plan(now)
        for key, (n, k, b, allow) in want.items():
            image, vm_type = key[:2]
            constraint = key[2] if len(key) > 2 else ""
            distinct = key[3] if len(key) > 3 else None
            with self.lock:
                # The plan may predate a handoff intent. Reserve under the same lock
                # as its growth fence; every accepted reservation drains normally.
                if (n <= 0 or time.time() <= self.backoff.get(image, 0) or image in self.asking
                        or not self._handoff_gate().growth_allowed()):
                    continue
                self.asking.add(image)
            args = (image, vm_type, n, k, b, allow, constraint) + ((distinct,) if distinct else ())
            threading.Thread(target=self._ask, args=args, daemon=True).start()
        self._release_idle()

    def retired(self) -> set:
        """VMs to retire (LIVE/scheduler/retire, one id a line): nothing new placed there, let go once empty, never
        adopted nor leased again (an operator's: a VM whose tokens leaked, deleted once it's let go)."""
        if time.time() - getattr(self, "_retire_t", 0) < 2:  # (the file looked at every 2 s, not at every VM of every pass:
            return self._retire  # 94% of the scheduler's time at 200 VMs, a stat per VM per pass)
        self._retire_t = time.time()
        p = DIR / "retire"
        mt = p.stat().st_mtime_ns if p.exists() else 0
        if getattr(self, "_retire_mt", None) != mt:
            self._retire_mt = mt
            self._retire = set(p.read_text().split()) if p.exists() else set()
        return self._retire

    def _slot(self, it: Item) -> VMState | None:
        best = None
        now = time.time()
        retired = self.retired()
        shape = fit_shape(it)
        if now < it.not_before:
            return None
        for vm in self.vms.values():
            # (rules.vm_fits, the one test placement, preemption and waste read: its image and type, its memory, never a
            # VM that failed it, one quarantined, retired or not answering; then rules.may_slot: it fits in the slots;
            # a timing item only on an empty VM; no filler past a VM's hold; a VM set aside for another, its need)
            if not vm_fits(shape, vm, retired):
                continue
            if not rules.may_slot(it.batch.klass, it.timing, it.weight, vm.slots - vm.used(), len(vm.running), vm.draining,
                                  self._held(vm, it, now), True, getattr(vm, "prep_fails", 0)):
                continue
            # (a batch's VM, then the fullest: whole VMs stay free for timing items and for release)
            score = (it.batch.id in vm.prepared, it.batch.tree["id"] in vm.trees, -(vm.slots - vm.used()))
            if best is None or score > best[0]:
                best = (score, vm)
        return best[1] if best else None

    def _place(self, it: Item, vm: VMState) -> bool:
        with self.lock:
            return self._place_locked(it, vm)

    def _place_locked(self, it: Item, vm: VMState) -> bool:
        """The item placed on the VM, where its fate allows it (rules.ItemFate.place: never a VM boat reports stopped
        or one that failed it)."""
        if getattr(self, "handing_off", False) or not self._handoff_gate().placement_allowed():
            return False
        if it.batch.admission_closed or it.owned.closed:
            if it.batch.strict_once and any(r.get("name") == it.name for r in it.batch.rows):
                it.state = "done"
                it.fate.delivered()
            return False
        if getattr(vm, "upgrading", False) or not agent_capabilities_match(
                required_agent_capabilities(it.spec), getattr(vm, "agent_capabilities", {})):
            return False
        if it.batch.strict_once and not vm.lease.get("boot"):
            return False  # A strict physical attempt must have an authoritative boot identity.
        if it.batch.strict_once and it.owned.placements:
            self._owned_failed(it, "strict_once: prior placement cannot be replaced")
            return False
        group = it.spec.get("distinct_vm_group", "")
        if not distinct_live(it) or not rules.distinct_vm_allowed(bool(group), vm.id in distinct_used(it)):
            return False
        if not rules.cpu_matches(it.spec.get("cpu_model", ""), vm.model):
            return False
        if it.spec.get("cpu_model"):
            self._constraint(it.batch.image, vm.type, it.spec["cpu_model"])
        if not it.fate.place(vm.id, vm.dead or not vm.ready or getattr(vm, "stopped", False)):
            if it.fate.state != "queued" and it.state == "queued" and not getattr(it, "_fate_alerted", False):
                it._fate_alerted = True  # (every move of an item's state moves its fate too: this is a bug to report)
                self.alert(f"{it.batch.id}: {it.name}: queued, its fate {it.fate.state}: not placed")
            return False
        if not it.owned.place():
            return False
        it.launch_identity = secrets.token_hex(24)
        it.start_done.clear()
        it.last_placement = {"name": it.name,
            "run_name": it.name if not getattr(it, "placed", 0) else f"{it.name}~s{it.placed + 1}",
            "vm": vm.id, "image": vm.image, "type": vm.type, "cpu": vm.model,
            "identity": it.launch_identity, "t": time.time(),
            "agent_version": vm.agent, "agent_capabilities": dict(vm.agent_capabilities)}
        if it.batch.strict_once:
            it.last_placement.update(boot_id=vm.lease.get("boot", ""),
                                     attempt=it.owned.placements, max_attempts=it.batch.max_attempts)
        if group:
            it.last_placement.update(distinct_vm_group=group, boot_id=vm.lease.get("boot", ""))
        if durable_item(it):
            it.last_placement.update(batch=it.batch.id, distinct_vm_group_scope="durable",
                distinct_vm_group_generation=it.spec["distinct_vm_group_generation"],
                attempt=getattr(it, "placed", 0) + 1)
        # Write-ahead admission: never launch on failure to persist the placement.
        try:
            if durable_item(it):
                # A journal failure may consume a VM, but never permits reuse.
                it.batch.durable_registry.reserve(it.last_placement, time.time())
            if it.batch.strict_once or it.batch.retain_journals or group:
                self._append_owned(it.batch.id, "placed", it.last_placement)
            if it.batch.strict_once:
                self._append_owned(it.batch.id, 'attempts', {
                    **self._prelaunch_binding(it), 'event': 'prelaunch_reserved',
                    'protocol': 1, 'time': time.time()})
            if group and not durable_item(it):
                it.batch.distinct_vms.setdefault(group, set()).add(vm.id)
        except Exception:
            it.owned.start_settled()
            it.start_done.set()
            self._owned_failed(it, "placement journal could not be persisted")
            return False
        now = time.time()
        first = getattr(it, "placed", 0) == 0
        it.state, it.vm, it.t_start = "starting", vm, now
        it.tries += 1
        it.placed = getattr(it, "placed", 0) + 1
        it.head = False
        it.fate.follow(self.pol.get("generation", 0))  # (placed under the policy in force)
        for rv in {vm, getattr(it, "rsv_vm", None)}:  # (its reservation ends where it is placed, here or elsewhere)
            if rv is not None and getattr(rv, "reserved", None) and rv.reserved[0] is it:
                rv.reserved = None
        it.rsv_vm = None
        it.run_name = it.name if it.placed == 1 else f"{it.name}~s{it.placed}"  # (a name of its own on the VM each time: an agent ignores a start it has)
        vm.running[(it.batch.id, it.run_name)] = it
        it.batch.vms_used.add(vm.id)
        it.batch.t_first = it.batch.t_first or now
        if it.batch.klass <= 3:
            vm.t_placed_real = now  # (health's waste: a VM real work goes on as its slots free is no waste of them)
        if it.batch.klass <= 4:
            if vm.t_real and now - vm.t_real >= 60 and not any(x.batch.klass <= 4 for x in vm.running.values() if x is not it):
                self.m["vm_gaps"] = (self.m.get("vm_gaps", []) + [round(now - vm.t_real)])[-2000:]  # (a VM's gap between real work: the hold's test)
            vm.t_real, vm.draining = now, False
        if first:
            n = NAMES[it.batch.klass]
            self._h().record_placed(now)
            self.m["placed"][n] = self.m["placed"].get(n, 0) + 1
            self.m["waits"][n] = (self.m["waits"].get(n, []) + [round(now - it.t_created, 2)])[-1000:]
            if vm.t_ready and vm.t_ready <= it.t_created:
                self.m["placed_warm"] += 1
        for x in vm.running.values():
            x.peers = max(x.peers, len(vm.running) - 1)
        threading.Thread(target=self._start_on, args=(it, vm), daemon=True).start()
        return True

    def _attempt_audit(self, vm, starts, event):
        # An intent/transport response is not proof of physical execution. Keep
        # it beside placements so ordinary retry history remains auditable too.
        with self.lock:
            recorded = 0
            for start in starts:
                bid, name = start.get("batch"), start.get("spec", {}).get("name")
                b = self.batches.get(bid)
                if b is None or b.closed:
                    continue  # Closed audit snapshots are immutable.
                it = vm.running.get((bid, name))
                if it is None:
                    continue
                if b.strict_once and event == 'start_requested':
                    binding = self._prelaunch_binding(it)
                    if (it.vm is not vm or binding['vm'] != vm.id
                            or binding['boot_id'] != (vm.lease or {}).get('boot')
                            or start.get('spec', {}).get('launch_identity') != it.launch_identity):
                        raise OwnedAgentRefused('strict start identity changed before launch')
                    audit = self._prelaunch_evidence(it)
                    if not prelaunch_not_started(audit['complete'], audit['reserved'], audit['start_requested']):
                        raise OwnedAgentRefused('strict start audit incomplete or start already requested')
                self._append_owned(bid, "attempts", {"event": event, "time": time.time(),
                    "batch": bid, "name": it.name, "run_name": name, "vm": vm.id,
                    "boot_id": (vm.lease or {}).get("boot"),
                    "launch_identity": it.launch_identity, "strict_once": b.strict_once})
                recorded += 1
            return recorded == len(starts)

    @staticmethod
    def _prelaunch_binding(it):
        p = it.last_placement or {}
        return {'batch': it.batch.id, 'name': it.name, 'run_name': p.get('run_name'),
                'vm': p.get('vm'), 'boot_id': p.get('boot_id'),
                'launch_identity': p.get('identity')}

    def _prelaunch_evidence(self, it):
        return prelaunch.read(DIR / 'queue' / f'{it.batch.id}.attempts', self._prelaunch_binding(it))

    def _strict_prepare(self, it: Item, vm: VMState) -> bool:
        """Retry preparation only; this function cannot invoke a payload start.

        Keep the original placement, nonce and physical-attempt reservation. Every
        retry binds a complete audit prefix; a missing/torn journal or any durable
        start_requested fails closed. BOAT_TRIES limits preparation calls, not
        physical attempts. Cancellation and ownership loss never widen this path.
        """
        while True:
            with self.lock:
                if it.batch.admission_closed or it.owned.closed:
                    it.state = 'cancel_pending'
                    return False
                binding = self._prelaunch_binding(it)
                if binding['vm'] != vm.id or binding['boot_id'] != (vm.lease or {}).get('boot'):
                    raise OwnedAgentRefused('strict prelaunch VM/boot identity changed')
                audit = self._prelaunch_evidence(it)
                if not prelaunch_not_started(audit['complete'], audit['reserved'], audit['start_requested']):
                    raise OwnedAgentRefused('strict prelaunch audit incomplete or start already requested')
                if self.stop.is_set():
                    raise PrelaunchNotStarted('controller stopped before payload launch')
            try:
                self._verify_agent_capabilities(it, vm)
                self._owned_client(vm).rpc('capabilities')
                self._prepare(it.batch, vm)
                self._verify_agent_capabilities(it, vm)
                return True
            except AgentError as error:
                transient = prelaunch_transport_retryable(
                    isinstance(error, boat.AgentTransportError),
                    error.status if isinstance(error, boat.AgentHTTPError) else 0)
                if not transient or vm.dead:
                    raise
                with self.lock:
                    audit = self._prelaunch_evidence(it)
                    if not prelaunch_not_started(audit['complete'], audit['reserved'], audit['start_requested']):
                        raise
                    diagnostic = lease_diagnostics.event(vm.id, 'prelaunch', client=vm.client, error=error)
                    failure = audit['failures'] + 1
                    self._append_owned(it.batch.id, 'attempts', {
                        **self._prelaunch_binding(it), 'event': 'prelaunch_not_started',
                        'phase': 'prelaunch', 'terminal': True, 'status': 'NOT_STARTED',
                        'prelaunch_failure': failure, 'time': time.time(), 'audit': audit,
                        'error': diagnostic['error']})
                    retry = prelaunch_can_retry(audit['complete'], audit['reserved'],
                        audit['start_requested'], transient, it.batch.admission_closed or it.owned.closed,
                        self.stop.is_set(), failure, max(1, BOAT_TRIES))
                if not retry:
                    raise PrelaunchNotStarted(diagnostic['error']['message']) from error
                self.stop.wait(min(failure, 3))

    def _start_on(self, it: Item, vm: VMState):
        try:
            self._start_on_inner(it, vm)
        finally:
            with self.lock:
                it.owned.start_settled()
                it.start_done.set()

    def _start_on_inner(self, it: Item, vm: VMState):
        if getattr(self, "recovery_blocked", False):
            return
        b, rn = it.batch, it.run_name
        try:
            with self.lock:
                if b.admission_closed or it.owned.closed:
                    it.state = "cancel_pending"
                    return
            if b.strict_once:
                if not self._strict_prepare(it, vm):
                    return
            else:
                self._verify_agent_capabilities(it, vm)
                self._prepare(b, vm)
            vm.prep_fails = 0  # (prepared: a candidate again; rules.may_slot)
            spec = {k: v for k, v in it.spec.items() if k not in ("est", "weight", "vm_type", "meta", "try", "orig", "timing")} | {"cpus": it.weight * boat.SLOT_CPU, "name": it.run_name, "launch_identity": it.launch_identity}
            if b.strict_once:
                spec["strict_once"] = True
            if not (b.strict_once or b.retain_journals):
                spec.pop("launch_identity", None)
            if not b.strict_once:
                self._verify_agent_capabilities(it, vm)
            with self.lock:  # (its batch closed, the item preempted or moved while the batch was prepared here: never started.
                if it.state != "starting" or it.vm is not vm or it.run_name != rn or b.closed or b.admission_closed or it.owned.closed:  # Their kill found nothing to kill: a run
                    if b.admission_closed or it.owned.closed:
                        it.state = "cancel_pending"
                        return  # Keep its placement held until a bound reap receipt.
                    vm.running.pop((b.id, rn), None)  # stale preparation, never launch
                    if it.state == "preempting" and it.vm is vm and not b.closed:
                        it.state, it.vm, it.t_queued, it.head = "queued", None, time.time(), True
                        it.fate.preempted()
                        it.tries -= 1
                    self.wake.set()
                    return
                if getattr(vm, "upgrading", False) or not agent_capabilities_match(required_agent_capabilities(it.spec), vm.agent_capabilities):
                    raise AgentCapabilityError("agent capability inventory changed before launch")
                it.state = "running"
            if not (b.strict_once or b.retain_journals or it.spec.get("distinct_vm_group")):
                # Legacy placements join the same close barrier as start intents.
                with self.lock:
                    if b.closed:
                        return
                    if (not isinstance(it.last_placement, dict) or it.last_placement.get("run_name") != rn
                            or it.last_placement.get("vm") != vm.id or it.last_placement.get("name") != it.name):
                        it.last_placement = {"name": it.name, "run_name": rn, "vm": vm.id,
                                             "image": vm.image, "type": vm.type,
                                             "identity": it.launch_identity, "t": it.t_start or time.time(),
                                             "agent_version": vm.agent, "agent_capabilities": dict(vm.agent_capabilities)}
                    self._append_owned(b.id, "placed", it.last_placement)
            vm.fails = 0
            # (the start goes at once, beside the worker's long poll: a placed item doesn't wait for that poll to end)
            cl = self._owned_client(vm) if b.strict_once else vm.client
            starts = [{"batch": b.id, "spec": spec}]
            if not self._attempt_audit(vm, starts, "start_requested"):
                return  # The admission closed before the request; do not start it.
            r = cl.rpc("vtick", {"start": starts, "wait": 0}, timeout=60)
            self._attempt_audit(vm, starts, "start_response")
            if b.strict_once and r.get("err"):
                raise OwnedAgentRefused("owned preparation lost; placement held")
            if r.get("err") and "isn't prepared here" in str(r["err"]):  # (its preparation gone from the VM: prepared again, no strike)
                self._unprepare(vm, b.id)
                with self.lock:
                    if b.admission_closed or it.owned.closed:
                        it.state = "cancel_pending"
                        return
                    vm.running.pop((b.id, it.run_name), None)
                    if it.state != "done":
                        it.fate.requeued()
                        it.state, it.vm, it.head = "queued", None, True
                self.wake.set()
                return
            if r.get("err"):
                raise RuntimeError(r["err"])
            for row in r.get("done", []):
                self._accept_finished(vm, row)
        except PrelaunchNotStarted as e:
            with self.lock:
                it.result_error = 'strict preparation NOT_STARTED; cleanup retained: ' + str(e)[:200]
                self._owned_failed(it, it.result_error, not_started=True)
            self.wake.set()
        except AgentCapabilityError as e:
            with self.lock:
                vm.running.pop((b.id, rn), None)
                self._owned_failed(it, "required agent capabilities could not be verified; payload not launched: " + str(e))
            self.wake.set()
        except Exception as e:
            if vm.dead:
                return  # A stale fence already requeued without a strike.
            if b.strict_once:
                with self.lock:
                    it.result_error = "strict protocol/launch uncertain; cleanup retained: " + str(e)[:200]
                    self._owned_failed(it, it.result_error)
                self.wake.set()
                return  # no preparation fallback, VM repair, provider probe, retry or replacement
            with self.lock:
                if b.admission_closed or it.owned.closed:
                    it.state = "cancel_pending"
                    return
            log(f"{vm.id}: {b.id}: prepare failed ({str(e)[:200]})")
            gone = "is stopped" in str(e) or not self._boat_up(vm.id)
            unanswered = "didn't answer" in str(e) or "502" in str(e)
            # (rules.prepare_failed: boat stopped it, the VM lost; an agent not answering on a VM boat reports up is our
            # repair to make, no strike against the item, nor in an outage; a preparation that fails on an answering
            # agent is one)
            what = rules.prepare_failed(gone, unanswered, self._outage(), "lacked" in str(e) and "made again" in str(e))
            if not str(e).startswith("its preparation failed") and "made again" not in str(e):  # (3 in a row: no more placements there, rules.may_slot; a preparation counted
                vm.prep_fails = getattr(vm, "prep_fails", 0) + 1  # once, not once per item waiting on it: 4 waiters quarantined a VM for one 3-s outage, B9)
            if what == "lost":  # (boat stopped it: nothing more is placed there)
                self._note_lease_relinquish(vm, source='prepare.provider_lost', error=e)
                self._lost_vm(vm)
            elif unanswered:
                threading.Thread(target=self._repair, args=(vm,), daemon=True).start()
            with self.lock:
                if b.admission_closed or it.owned.closed:
                    it.state = "cancel_pending"
                    return
                vm.running.pop((b.id, it.run_name), None)
                if it.state != "done":
                    if what == "requeue":  # (no failure charged to the item: back off, no retry hot)
                        it.state, it.vm, it.not_before = "queued", None, time.time() + 60
                        it.fate.requeued()
                    else:
                        self._boat_failed(it, vm.id, f"prepare on {vm.id} failed: {str(e)[:120]}")
            # (a VM boat reports up is never let go for failing: it is repaired in place; "lost" is boat's word only)
        self.wake.set()

    def _peer(self, vm: VMState, tree: str | None = None, cache: tuple | None = None) -> VMState | None:
        """A ready VM (not this one) that has the tree or the cache whole: the one this VM pulls from."""
        for v in list(self.vms.values()):
            if v is vm or v.dead or not v.ready or not v.client or v.failing or v.quarantine:
                continue
            if (tree and tree in v.trees) or (cache and cache in v.caches):
                return v
        return None

    def _prepare(self, b: Batch, vm: VMState):
        """The batch's directory on the VM: its tree synced by hash, pulled by the VM from a peer VM that has it (the
        work on the VMs, not here), else sent from the blob store here (PREP_MAX at once); its caches the same way."""
        # (rules.Preparation: a prepared batch is never prepared again; one being prepared waited for; a failed one not
        # kept, so the next placement here prepares again)
        prep = rules.Preparation()
        for _ in range(3):
            with vm.lock:
                prep.state = "prepared" if b.id in vm.prepared else "preparing" if b.id in vm.preparing else "none"
                what = prep.begin()
                if what == "ready":
                    return
                first = what == "prepare"
                ev = vm.preparing[b.id] = threading.Event() if first else vm.preparing[b.id]
                if first:
                    vm.__dict__.setdefault("prep_err", {}).pop(b.id, None)  # (this preparation's failure, not an older one's)
            if first:
                break
            ev.wait(600)
            # (rules.Preparation.waited: its failure's own words (an agent that didn't answer is no strike for the waiters
            # either; B8); unprepared with no failure recorded (the VM dropped it, the wait ran out): prepared again, never
            # a strike: 11-01 11:09-12:21, 5,748 strikes with no reason, 1,107 items to Modal)
            err = getattr(vm, "prep_err", {}).get(b.id, "")
            w = prep.waited(b.id in vm.prepared, bool(err))
            if w == "ready":
                return
            if w == "fail":
                raise RuntimeError(f"its preparation failed: {err}")
        else:
            raise AgentError(f"{vm.id}: the batch's preparation there never ended (3 waits)")
        try:
            cl = self._owned_client(vm) if b.strict_once else vm.client
            t = b.tree
            version = cl.version if b.strict_once else self.agent_version
            r = cl.rpc("begin", version, b.id, t["id"], b.ctx_b64, timeout=60)
            if r.get("ok"):
                r = r["prep"]
            else:
                if b.strict_once and (r.get("version") != version or r.get("need") != "manifest"):
                    raise OwnedAgentRefused("strict begin refused: no fallback preparation")
                r = cl.rpc("tree", t["id"], t["files"], t["links"], timeout=120)
                if not r.get("ok") and r.get("missing"):
                    peer, pulled = self._peer(vm, tree=t["id"]), False
                    if peer:
                        with contextlib.suppress(Exception):
                            pr = cl.rpc("pull", peer.client.url, peer.client.token, r["missing"], timeout=900)
                            pulled = pr.get("ok") and not pr.get("missing")
                    if not pulled:
                        with self.prep_sem:
                            while self.shed and not self.stop.is_set():  # (the controller loaded: one send at a time, later)
                                time.sleep(2)
                            miss = cl.rpc("have", sorted(set(t["files"].values())), timeout=120).get("missing", r["missing"])
                            for i in range(0, len(miss), 800):
                                cl.put("/put", blob_tar(miss[i:i + 800]))
                    r = cl.rpc("tree", t["id"], t["files"], t["links"], timeout=120)
                if not r.get("ok"):
                    raise AgentError(f"tree: {str(r)[:200]}")
                r = cl.rpc("prep", b.id, f"tree:{t['id']}", b.ctx_b64, t["id"], "", "-", timeout=300)
            if not r.get("ok"):
                raise AgentError(f"prep: {str(r)[:200]}")
            vm.trees.add(t["id"])
            for c in b.caches:
                local = pathlib.Path(c["local"])
                names = sorted(f.name for f in local.iterdir() if f.name in set(c["outputs"])) if local.exists() else []
                have = set(r.get("caches", {}).get(c["name"], []))
                if set(names) - have:
                    peer = self._peer(vm, cache=(c["name"], c["key"]))
                    got = set()
                    if peer:
                        with contextlib.suppress(Exception):
                            got = set(cl.rpc("pullcache", peer.client.url, peer.client.token, c["name"], c["key"], timeout=600).get("got", []))
                    if set(names) - have - got:
                        with self.prep_sem:
                            buf = io.BytesIO()
                            with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=1) as tf:
                                for n in sorted(set(names) - have - got):
                                    tf.add(local / n, arcname=n)
                            cl.put(f"/cache?name={c['name']}&key={c['key']}", buf.getvalue())
                if names:
                    vm.caches.add((c["name"], c["key"]))
            self._verify_tree(vm, b, t)
            vm.prepared.add(b.id)
        except Exception as e:
            vm.__dict__.setdefault("prep_err", {})[b.id] = str(e)[:200]
            raise
        finally:
            with vm.lock:  # (a failed preparation isn't kept: the next item of the batch placed here tries again; one that
                prep.done(b.id in vm.prepared)  # ended either way is no longer being made: never left marked running, for
                if vm.preparing.get(b.id) is ev:  # a waiter to find finished and unprepared once the VM drops the batch)
                    vm.preparing.pop(b.id, None)
            ev.set()

    def _verify_tree(self, vm: VMState, b, t: dict):
        """The batch's tree on the VM, each file its client's manifest names a git blob for checked to be that blob
        before any job of it runs there (agent.tree_verified): a mismatch is ours (a wrong key on the client's side),
        never run (11-02: research/termination/lean-termination.ts shipped as main's old content)."""
        gits = t.get("gits") or {}
        if not gits:
            return
        try:
            cl = self._owned_client(vm) if b.strict_once else vm.client
            r = cl.rpc("verify", t["id"], gits, timeout=120)
        except Exception as e:
            if not b.strict_once and "no command verify" in str(e):  # (an agent from before: upgraded in place between items, checked then)
                return
            raise
        if not r.get("ok"):
            bad = r.get("bad") or []
            with vm.lock:
                vm.prepared.discard(b.id)
            if r.get("partial"):  # (the VM's tree lacked files: dropped there, made again at the next preparation; no strike)
                raise RuntimeError(f"its tree here lacked {len(bad)} files of its manifest ({bad[:2]}): dropped, made again")
            raise RuntimeError(f"its tree isn't the commit's: {len(bad)} files aren't the git blobs named ({bad[:2]}): stale content, ours")

    def _unprepare(self, vm: VMState, bid: str):
        """The VM dropped the batch (rules.Preparation.dropped): the next placement there prepares it again; a finished
        preparation's mark goes with it (a running one is left to end)."""
        with vm.lock:
            vm.prepared.discard(bid)
            ev = vm.preparing.get(bid)
            if ev is not None and ev.is_set():
                vm.preparing.pop(bid, None)

    def _victimize(self, v: VMState, x: Item, why: str):
        if x.batch.admission_closed:
            return
        x.state = "preempting"
        self.m["preempted"][NAMES[x.batch.klass]] = self.m["preempted"].get(NAMES[x.batch.klass], 0) + 1
        x.batch.preempted += 1
        threading.Thread(target=self._kill, args=(v, x), daemon=True).start()

    def _preempt_for(self, it: Item, now: float) -> bool:
        """A slot for a higher-class item, taken at once from a lower class: interactive checks and gates kill a
        filler's or a sweep's item (queued again at the head of its class); a submission waits for items to end, and
        after SUB_PREEMPT s takes a filler's. A timing item gets a whole VM of lower classes emptied. False when there
        is nothing to take."""
        if now - getattr(it, "claimed", 0) < 20:
            return True
        k = it.batch.klass
        # (rules.victims: interactive checks and gates take a filler's or a sweep's slot, a submission a filler's after
        # SUB_PREEMPT s, nobody a submission's)
        victims = {"filler_sweep": (5, 4), "filler": (5,), "none": ()}[rules.victims(k, now - it.t_created, SUB_PREEMPT)]
        if not victims:
            return False
        if (it.timing or it.weight > 1) and now < it.not_before:
            return False  # (backing off: no VM set aside for an item that can't take it yet)
        if it.timing or it.weight > 1:
            # (an item that needs several slots or a whole VM (B15: weight-4 submissions behind filler and sweeps, none
            # placed for an hour): slots on one VM set aside for it, the nearest to empty first (its lower-class items
            # killed, what it may not take left to end; nothing new placed in the slots it needs, rules.reservation_holds),
            # never a full VM drained while a nearly empty one waits; slots held so are exclusive work in health's busy.
            # A VM running its own class or higher is a candidate too when the slots past those fit it
            # (rules.may_set_aside; 11-01 13:25, a 2-slot gate item found none where gates ran beside filler everywhere)
            rv = getattr(it, "rsv_vm", None)
            if not (rv is not None and not rv.dead and rv.ready and getattr(rv, "reserved", None) and rv.reserved[0] is it and now < rv.reserved[1]):
                rv = None
            keep_of = lambda v: sum(x.weight for x in v.running.values() if x.batch.klass not in victims or x.state == "starting") + getattr(v, "foreign", 0)
            if rv is not None and not keep_of(rv):
                # (set aside already: one VM an item, never another every 20 s; a lower item that was starting there
                # when it was set aside is killed once it runs)
                self._make_room(rv, it, rv.reserved[2] if len(rv.reserved) > 2 else rv.slots, victims)
                it.claimed = now
                return True
            best = None
            retired, shape = self.retired(), fit_shape(it)
            for v in self.vms.values():
                if not vm_fits(shape, v, retired):  # (the one fit: rules.vm_fits)
                    continue
                if self._held(v, it, now):
                    continue  # (set aside for another item still waiting)
                need = max(v.slots, it.weight) if it.timing else it.weight  # (a timing item: the whole VM, and one it fits in:
                xs = list(v.running.values())  # 10:35-14:25, a 2-slot timing sweep item set aside 1-slot VMs 100 times, never placed)
                # (what it may not take, and lower items starting there (killed once they run), left to end; lower items
                # being killed already are slots coming free)
                keep = keep_of(v)
                if not rules.may_set_aside(need, v.slots, keep, any(x.batch.klass <= k for x in xs)):
                    continue
                score = (keep, sum(x.weight for x in xs if x.batch.klass in victims))
                if best is None or score < best[0]:
                    best = (score, v, need)
            if rv is not None and (best is None or best[1] is rv or not rules.reservation_moves(keep_of(rv), best[0][0])):
                # (waiting there for what it may not take to end, and no VM nearer to empty: it stays)
                self._make_room(rv, it, rv.reserved[2] if len(rv.reserved) > 2 else rv.slots, victims)
                it.claimed = now
                return True
            if best is None:
                return False
            if rv is not None:  # (a VM nearer to empty came: the item moves there, its old one free to all)
                rv.reserved = None
            v, need = best[1], best[2]
            it.claimed = now
            v.reserved, it.rsv_vm = (it, now + (120 if not best[0][0] else 900), need), v
            kill = self._make_room(v, it, need, victims)
            log(f"{NAMES[k]} {it.batch.id}: {v.id} set aside for a{' timing' if it.timing else ' ' + str(it.weight) + '-slot'} item ({len(kill)} lower-class items queued again, {best[0][0]} slots to end)")
            return True
        for victim_class in victims:
            retired, shape = self.retired(), fit_shape(it)
            for v in [v for v in self.vms.values() if vm_fits(shape, v, retired)]:
                held = self._held(v, it, now)  # (slots set aside for another waiting item: not this one's to take)
                for x in list(v.running.values()):
                    if x.batch.klass != victim_class or x.state != "running" or v.slots - v.used() + x.weight - held < it.weight:
                        continue
                    it.claimed = now
                    self._victimize(v, x, NAMES[k])
                    return True
        return False

    def _make_room(self, v: VMState, it: Item, need: int, victims: tuple) -> list:
        """The lower items running on a VM set aside for `it` killed until its need is free (or coming free)."""
        free_now = v.slots - v.used() + sum(x.weight for x in v.running.values() if x.state == "preempting")
        kill = []
        for x in sorted([x for x in v.running.values() if x.batch.klass in victims and x.state == "running"], key=lambda x: (-x.batch.klass, x.weight)):
            if free_now >= need:
                break
            kill.append(x)
            free_now += x.weight
        for x in kill:
            self._victimize(v, x, "timing" if it.timing else "wide")
        return kill

    def _held(self, vm: VMState, it, now: float) -> int:
        """The slots of the VM a reservation holds against `it` (None: against anyone): its item's need while that item
        is still queued, before its deadline (rules.reservation_holds); a reservation that holds nothing is dropped."""
        r = getattr(vm, "reserved", None)
        if not r:
            return 0
        other = r[0]
        queued = other.state == "queued" and not other.batch.closed
        n = rules.reservation_holds(other is it, queued, now, r[1], r[2] if len(r) > 2 else vm.slots, it is not None and it.batch.klass < other.batch.klass)
        if not queued or now >= r[1]:
            vm.reserved = None  # (its item ran elsewhere, is done or gone, or the deadline passed)
        return n

    def _cancel_strict_item(self, vm, it):
        """Preemption is terminal for one strict item; uncertain reap holds its slot."""
        b = it.batch
        with self.lock:
            if it.owned.reaped:
                return
            it.owned.cancel()
            it.state = "cancel_pending"
            self._append_owned(b.id, "cancel", {"name": it.name, "run_name": it.run_name,
                "identity": it.launch_identity, "admission_closed": True, "t": time.time()})
        if not it.start_done.wait(60):
            with self.lock:
                self._owned_failed(it, "strict_once: preempted with start still in flight; cleanup unconfirmed")
            return
        receipt = None
        try:
            receipt = vm.client.rpc("cancelitem", b.id, it.run_name, 10,
                                    it.launch_identity, timeout=12)
        except Exception:
            receipt = {"ok": False, "error": "strict preemption reap failed or timed out"}
        matched = self._matching_receipt(receipt, b.id, it.run_name, it.launch_identity)
        with self.lock:
            self._append_owned(b.id, "cancel", {"name": it.name, "run_name": it.run_name,
                "identity": it.launch_identity, "matched": matched, "receipt": self._safe_snapshot(receipt), "t": time.time()})
            if it.owned.confirm(matched):
                vm.running.pop((b.id, it.run_name), None)
            self._owned_failed(it, "strict_once: preempted; " +
                               ("matching namespace reap confirmed" if matched else "cleanup remains unconfirmed"))
        self.wake.set()

    def _kill(self, vm: VMState, it: Item):
        if it.batch.strict_once:
            return self._cancel_strict_item(vm, it)
        with contextlib.suppress(Exception):
            vm.client.rpc("killitem", it.batch.id, it.run_name, timeout=30)
        self.wake.set()

    # -- the plan
    @staticmethod
    def est_key(it: Item) -> str:
        sp = it.spec
        return f"{it.batch.label.split(' ')[0]}|{sp.get('module', '')}|{sp.get('fn') or it.name.rstrip('0123456789-~s')}"

    def est_of(self, it: Item) -> float:
        """An item's expected seconds: its own estimate when it has one (scaled by what its batch's items took);
        one of a second or less (a runner's call: `est` 1) from the history of its (label, function), kept across
        batches and restarts, else the median of what its class's items took lately, else a minute."""
        e = it.est
        if e > 1:
            return e * it.batch.scale()
        h = self.est_hist.get(self.est_key(it))
        if h:
            return float(h)
        med = self.__dict__.setdefault("_med", {})  # (a class's median, once a pass: a sort of 200 for each of 8.8k items was a pass's)
        if it.batch.klass not in med:
            obs = sorted(self.class_obs.get(it.batch.klass, []))
            med[it.batch.klass] = float(obs[len(obs) // 2]) if obs else 60.0
        return med[it.batch.klass]

    def _learn(self, it: Item, seconds: float):
        k = self.est_key(it)
        self.est_hist[k] = round(seconds if k not in self.est_hist else 0.7 * self.est_hist[k] + 0.3 * seconds, 2)
        self.class_obs[it.batch.klass] = (self.class_obs.get(it.batch.klass, []) + [round(seconds, 2)])[-200:]

    def _handoff_gate(self):
        gate = getattr(self, "_handoff_gate_state", None)
        if gate is None:
            gate = self._handoff_gate_state = rules.HandoffDrain()
        return gate

    def _clear_handoff_pending(self, generation):
        if self._handoff_gate().finish(generation):
            pending = getattr(self, "_handoff_pending", None)
            if pending is not None and pending["generation"] == generation:
                self._handoff_pending = None
            self.wake.set()

    def _pool_settled(self) -> bool:
        claiming = (getattr(self, "_acquiring", 0) or getattr(self, "_adopt_claims", 0)
                    or getattr(self, "reattaching", False) or getattr(self, "reattach_pending", ()))
        return rules.handoff_pool_settled(bool(getattr(self, "asking", ()) or claiming),
            sum(getattr(self, "creating", {}).values()),
            sum(not v.ready for v in self.vms.values() if not v.dead))

    def _in_flight(self) -> int:
        return sum(self.creating.values()) + sum(1 for v in self.vms.values() if not v.dead and not v.ready)

    def _plan(self, now: float) -> dict:
        """VMs to start per image, demand-driven (README, "Scheduling policy"):
        - the deficit: queued slot-time (every class but filler; a submission client's capped at SUB_VMS VMs) less
          what the pool held (and coming) clears within break-even, its running real work deducted;
          only a whole marginal VM of work earns an economic start;
        - a gate's (and an interactive check's) own need, exempt from that horizon: the VMs that finish what's queued
          of them within GATE_HORIZON, after the slots free now or held by lower classes (fillers, sweeps);
        - an interactive check alone starts at most one VM, and none while any VM of its image is held;
        - each new VM needs 3x its bring-up time of queued work;
        - bounded by the bring-up cap (in flight), MAX_VMS (counted when it binds), the breaker and the watchdog. No
          VM-hour or dollar cap."""
        out = {}
        groups: dict = {}
        for it in self._pass_queue():
            group = distinct_key(it)
            groups.setdefault((it.batch.image, it.spec.get("vm_type") or getattr(it, "need_type", None) or it.batch.vm_type, it.spec.get("cpu_model", ""), group,
                               tuple(sorted(required_agent_capabilities(it.spec).items()))), []).append(it)
        if rules.plan_waits(getattr(self, "reattaching", False), time.time() - getattr(self, "t_reattach", 0.0), bool(getattr(self, "_handoff", {}).get("taken")), 15.0):
            return {}
        live = [v for v in self.vms.values() if not v.dead]
        # (the starts: rules.Pool.grow's, from the pool as it is: VMs held and ready, starts in flight (asked of boat or
        # coming up), the breaker; within max_vms and the bring-up cap, none while the breaker is open but one probe)
        pool, in_flight = rules.Pool(max(1, MAX_VMS), max(1, BRINGUP_MAX), int(BREAK_BACKOFF), int(BREAK_MAX)), self._in_flight()
        pool.held, pool.coming = len(live) + sum(self.creating.values()) - in_flight, in_flight
        pool.open_until = rules.breaker_deadline(self._breaker_enabled(), float(self.br["open_until"]))
        pool.half = self._breaker_enabled() and bool(self.br["half"])
        pool.probing = pool.half and any(s["ok"] is None for s in self.starts_log if s["t"] > self.br["t_trip"])
        self.target = {}
        # Advisory only: acquisition rechecks the registry under its lease lock.
        registry = Registry().read() if any(key[2] for key in groups) else {"vms": {}}
        fresh_reserved = {}  # one speculative window per image/type/model, across distinct groups
        for (image, vm_type, model, distinct, required), its in groups.items():
            # A worker can wait one full scheduling pass after becoming ready.
            S, bu = vm_slots(vm_type), max(30.0, self.bring_up) + FULL_S
            B = BREAK_EVEN_OVERRIDE_S or rules.break_even_s(HOLD, START_DOLLARS, VM_HOUR_DOLLARS, vm_mult(vm_type))
            used_vms = distinct_used(its[0]) if distinct else set()
            held = [v for v in live if not v.quarantine and v.image == image and v.type == vm_type and rules.cpu_matches(model, v.model)
                    and rules.distinct_vm_allowed(bool(distinct), v.id in used_vms)
                    and (not required or (not getattr(v, "upgrading", False)
                         and (not v.ready or agent_capabilities_match(dict(required), v.agent_capabilities))))]
            coming = self.creating.get((image, vm_type), 0)
            if model:
                draw = self._constraint(image, vm_type, model)
                pending = self._constraint_pending(draw)
                unresolved = sum(not v.ready and getattr(v, "cpu_constraint", "") == model
                                 and v.image == image and v.type == vm_type for v in live)
                speculative = sum(not v.ready and getattr(v, "cpu_constraint", "") == model
                                  and not getattr(v, "cpu_known_resume", False)
                                  and v.image == image and v.type == vm_type for v in live)
                draw_pending = coming + max(pending, speculative)
                coming = 0  # unverified draws are not promised matching CPU capacity
                reusable = self._known_cpu_reuse(registry, image, vm_type, model, used_vms)
                if not coming and not unresolved:
                    for it in its:
                        if (draw["fruitless"] >= self._constraint_limit(it) and not reusable
                                and not any(vm_fits(fit_shape(it), v, self.retired()) for v in held)):
                            self._constraint_refused(it, draw)
                    its = [it for it in its if it.state == "queued"]
                    if not its:
                        continue
                limit = max(self._constraint_limit(it) for it in its)
            real = [it for it in its if it.batch.klass <= 4]
            if not real:
                continue
            scale = {}

            def sz(it, S=S):  # (an item's slot-time; its batch's scale read once a pass, not once an item)
                e = it.est
                if e > 1:
                    b = it.batch
                    if b not in scale:
                        scale[b] = b.scale()
                    e *= scale[b]
                else:
                    e = self.est_of(it)
                return (S if it.timing or distinct else it.weight) * e
            work, subs = 0.0, {}
            for it in real:
                if it.batch.klass == 3:
                    subs[it.batch.client] = subs.get(it.batch.client, 0.0) + sz(it)
                else:
                    work += sz(it)
            contended = getattr(self, "_contended", None)
            first = {}
            for it in real:
                if it.batch.klass == 3:
                    first.setdefault(it.batch.client, it)
            for c, w in subs.items():  # (a client's work past its share is demand while its share doesn't bind: the same
                held_c = sum(x.weight for v in held for x in v.running.values() if x.batch.klass == 3 and x.batch.client == c)
                binds = contended(first[c]) if contended is not None and c in first else (len(subs) > 1 or any(it.batch.klass <= 2 for it in real))
                work += rules.sub_demand(w, max(0, SUB_VMS * S - held_c) * B, binds)  # contention placement reads, rules.contends)
            busy = sum(x.weight * min(B, max(0.0, self.est_of(x) - (now - (x.t_start or now)))) for v in held for x in v.running.values() if x.batch.klass <= 4)
            cap = max(0.0, sum(rules.usable_slots(v.slots, getattr(v, "foreign", 0), False) * B for v in held) + coming * S * B - busy)
            n_def = rules.economic_vms(work, cap, S * B)
            hi = [it for it in real if it.batch.klass <= 2]
            n_hi = 0
            if hi:
                # A deadline, not a fresh horizon every tick: bringing up VMs
                # consumes the gate's time too. Keep a feasible item-sized tail.
                item_s = max(sz(it) / (S if it.timing or distinct else it.weight) for it in hi)
                remaining = max(0.0, min(GATE_HORIZON, min(it.batch.t0 + GATE_HORIZON - now for it in hi)))
                H = rules.gate_window(remaining, bu, item_s)
                ready_H = max(remaining, item_s)  # a ready VM does not get the new VM's bring-up extension
                hw = sum(sz(it) for it in hi)
                durations = {sz(it) for it in hi} if not distinct and all(not it.timing and it.weight == 1 for it in hi) else set()
                whole = next(iter(durations)) if len(durations) == 1 else 0.0
                new_window = max(60.0, item_s, H - bu)  # preserve one full job despite float cancellation
                new_capacity = S * (math.floor(new_window / whole) * whole if whole else new_window)
                # the held VMs' slot-time within H a gate can have: free slots and lower classes' at once, the rest
                # as their items (interactive, gate, submission) end; VMs still coming up, after their bring-up
                cap_hi = 0.0
                exclusive = any(it.timing for it in hi)
                retired = self.retired()
                for v in held:
                    # The same shape/health exclusions as placement. Coming workers have no
                    # agent inventory yet; ready workers' foreign runs have no known end.
                    if v.ready and not any(vm_fits(fit_shape(it), v, retired) for it in hi):
                        continue
                    slots = rules.usable_slots(v.slots, getattr(v, "foreign", 0), exclusive)
                    left = [min(ready_H, max(0.0, self.est_of(x) - (now - (x.t_start or now))))
                            for x in v.running.values() if x.batch.klass <= 3]
                    if exclusive:
                        taken = slots * max(left, default=0.0)  # one whole VM, not fragments on several
                    else:
                        taken = sum((v.slots if x.timing else x.weight) * min(ready_H, max(0.0, self.est_of(x) - (now - (x.t_start or now))))
                                    for x in v.running.values() if x.batch.klass <= 3)
                    if whole and v.ready:
                        # Uniform one-slot jobs cannot use fractional future gaps.
                        # Count complete jobs after each occupied slot is released.
                        running = [x for x in v.running.values() if x.batch.klass <= 3]
                        used = sum(v.slots if x.timing else x.weight for x in running)
                        # Compare existing and new workers at the same feasible
                        # completion horizon; an existing slot freed during boot
                        # beats buying an otherwise-unused new worker.
                        fits = max(0, slots - used) * math.floor(H / whole)
                        fits += sum((v.slots if x.timing else x.weight) * math.floor(
                            max(0.0, H - max(0.0, self.est_of(x) - (now - (x.t_start or now)))) / whole)
                            for x in running)
                        cap_hi += fits * whole
                    elif whole:
                        cap_hi += slots * math.floor(new_window / whole) * whole
                    else:
                        cap_hi += max(0.0, slots * (ready_H if v.ready else max(0.0, H - bu)) - taken)
                cap_hi += coming * new_capacity
                n_hi = rules.gate_vms(hw, cap_hi, new_capacity)
                if all(it.batch.klass == 1 for it in hi):  # (a check alone: one VM, and only when none is held)
                    n_hi = 0 if (held or coming) else 1
            want = max(n_def, n_hi)
            ready = [v for v in held if v.ready]
            stuck = bool(ready) and any(all(v.id in it.avoid for v in ready) for it in real)  # (B3: every held VM failed it)
            want = rules.stuck_wants(want, stuck, coming + sum(1 for v in held if not v.ready))
            # each new VM: 3x its bring-up time of queued work
            useful = math.floor(sum(sz(it) for it in real) / (S * 3 * bu))
            # A guessed end is not a service lane for a new exclusive batch. Give
            # each unserved timing batch a lane, reusing a free/preemptible worker
            # first. Once it is running, the horizon sizes its remaining work.
            timing = {it.batch: it for it in hi if it.timing}
            unserved = [it for b, it in timing.items() if not any(x.batch is b for v in held for x in v.running.values())]
            available = {}
            for it in unserved:
                available[it] = []
                low = {"filler_sweep": (5, 4), "filler": (5,), "none": ()}[rules.victims(it.batch.klass, now - it.t_created, SUB_PREEMPT)]
                for v in held:
                    if (vm_fits(fit_shape(it), v, self.retired()) and not getattr(v, "foreign", 0)
                            and not self._held(v, it, now) and all(x.batch.klass in low for x in v.running.values())):
                        available[it].append(v.id)
            # Match distinct lanes to actual placement shapes. Their union can
            # hide a batch that cannot use any of another batch's free workers.
            lanes = {}
            def reuse(it, seen):
                for vid in available[it]:
                    if vid not in seen:
                        seen.add(vid)
                        if vid not in lanes or reuse(lanes[vid], seen):
                            lanes[vid] = it
                            return True
                return False
            reused = sum(reuse(it, set()) for it in unserved)
            lane_need = rules.exclusive_lanes(len(unserved), reused, coming + sum(1 for v in held if not v.ready))
            stranded = bool(hi) and cap_hi <= 0 and not coming
            want = max(want, lane_need, int(stranded))
            useful = max(useful, lane_need, n_hi)  # deadline capacity is not an economic admission
            if (not held and not coming) or stuck or stranded:  # (work with nowhere to go: one VM, whatever it amounts to)
                want, useful = max(want, 1), max(useful, 1)
            n = min(want, useful)
            # (reuse before create, across types: a VM of the image of another type, empty or running only what this
            # work may take (filler; a gate's, sweeps too), with the slots and the usable memory its items need, is one
            # of its before any start, a larger one too (rules.starts_beside_free; 20:46, default VMs started for a wave))
            loose = [it for it in real if not it.spec.get("vm_type") and not it.timing]
            if loose and n > 0:
                kmin = min(it.batch.klass for it in loose)
                low = {"filler_sweep": (5, 4), "filler": (5,), "none": ()}[rules.victims(kmin, 10 ** 9, SUB_PREEMPT)]
                w_max, mem_max = max(it.weight for it in loose), max(it.memory for it in loose)
                retired = self.retired()
                spare = 0  # (slots of other types' VMs this work may have: empty, or on what it may take; a larger VM's too)
                for v in live:
                    if getattr(v, "upgrading", False) or not agent_capabilities_match(dict(required), v.agent_capabilities) or not rules.cpu_matches(model, v.model):
                        continue
                    if not rules.distinct_vm_allowed(bool(distinct), v.id in used_vms) or v.image != image or v.type == vm_type or not v.ready or v.dead or v.id in retired or v.failing or getattr(v, "disk_low", False) or getattr(v, "prep_fails", 0) >= 3 \
                            or v.slots < w_max or not boat.R.fits_memory(mem_max, boat.TYPES.get(v.type, (0, 0))[1], boat.MEM_OVERHEAD):
                        continue
                    spare += max(0, v.slots - sum(x.weight for x in v.running.values() if x.batch.klass not in low) - getattr(v, "foreign", 0))
                free_any = spare // max(1, S)  # (in VMs of the group's type)
                n = rules.starts_beside_free(n, free_any)
            # (timing items: a free VM of any type that fits one is its, before any start; a start only for those left)
            tim = [it for it in real if it.timing and not it.spec.get("vm_type")]
            if tim:
                need = vm_slots(vm_type)
                kmin = min(it.batch.klass for it in tim)
                low = {"filler_sweep": (5, 4), "filler": (5,), "none": ()}[rules.victims(kmin, 10 ** 9, SUB_PREEMPT)]
                # (a VM of the image that fits one and runs only what a timing item may take (filler, sweeps) is one it
                # gets by emptying it, never a reason to start one)
                free_fit = sum(1 for v in live if not getattr(v, "upgrading", False) and agent_capabilities_match(dict(required), v.agent_capabilities) and rules.cpu_matches(model, v.model) and rules.distinct_vm_allowed(bool(distinct), v.id in used_vms) and v.image == image and v.ready and not getattr(v, "foreign", 0) and not getattr(v, "disk_low", False) and v.slots >= need and all(mem_fits(it, v.type) for it in tim[:1])
                               and v.type != vm_type and all(x.batch.klass in low for x in v.running.values())
                               and (not self._held(v, None, now) or v.reserved[0] in tim))  # (set aside for one of them: theirs)
                # (the starts this work earns: the VMs its other items fill, and one per timing item no free VM takes;
                # a start past those, with such a VM free, is needless: 08:17, a submission's own starts were counted)
                other = [it for it in real if it not in tim]
                ordinary = rules.economic_vms(sum(sz(it) for it in other), 0.0, S * B)
                if other and not held and not coming:
                    ordinary = max(ordinary, 1)  # retain the service VM exception
                urgent = rules.gate_vms(sum(sz(it) for it in other if it.batch.klass <= 2), 0.0, S * max(60.0, H - bu)) if hi else 0
                earned = max(ordinary, urgent) + max(0, len(tim) - free_fit)
                # Reusable timing work must not inflate a mixed group either;
                # ordinary work pays break-even, while gates keep their deadline.
                n = min(n, earned)
                if n > earned and free_fit:
                    self._h().record_needless(now, n - earned, image, vm_type)
            tgt = {"held": len(held), "coming": coming, "deficit_vms": n_def, "gate_vms": n_hi, "want": n, "work_slot_h": round(work / 3600, 2), "break_even_s": B, "growth_source": "legacy override" if BREAK_EVEN_OVERRIDE_S else "economics", "why": ""}
            fresh_target = False
            if model:
                draw_key = (image, vm_type, model)
                needs_cpu = lane_need > 0 or any(self._slot(it) is None for it in real)
                fresh = rules.cpu_parallel_draws(needs_cpu, BRINGUP_MAX, draw_pending + fresh_reserved.get(draw_key, 0), draw["fruitless"], limit)
                fresh_target = fresh > 0
                n = fresh if fresh_target else rules.cpu_capacity(n, len(reusable), draw["fruitless"], draw_pending, limit)
                tgt["want"] = n
                tgt["fresh_draws"] = fresh
                tgt["speculative_pending"] = draw_pending
                tgt["known_resumes_pending"] = sum(not v.ready and getattr(v, "cpu_known_resume", False)
                    and v.image == image and v.type == vm_type and getattr(v, "cpu_constraint", "") == model for v in live)
                tgt["known_cpu_reuse"] = len(reusable)
                tgt["cpu_model"] = model
                tgt["fruitless"] = draw["fruitless"]
            if required and not agent_capabilities_match(dict(required), BUILD_AGENT_CAPABILITIES):
                n, tgt["why"] = 0, "required agent capabilities unavailable in this build"
            if n > 0:
                tgt["why"] = "breaker" if now < pool.open_until else "probe" if pool.half else ""
                if self.shed:
                    n, tgt["why"] = 0, "shed"
                else:
                    at_max = pool.max_vms - pool.held - pool.coming < n
                    # Known resumes may take minutes and do not consume the fresh-start window.
                    # Keep the physical max-vms bound intact while exempting them from bringup.
                    exempt = sum(not v.ready and getattr(v, "cpu_known_resume", False) for v in live) if fresh_target else 0
                    n = min(n, max(0, pool.max_vms - pool.held - pool.coming))
                    pool.coming = max(0, pool.coming - exempt)
                    got = pool.grow(n, now)
                    pool.coming += exempt
                    if fresh_target:
                        fresh_reserved[draw_key] = fresh_reserved.get(draw_key, 0) + got
                    if got < n and not tgt["why"]:
                        if at_max and now - getattr(self, "_max_t", 0) > 300:
                            self._max_t = now
                            self.m["max_binds"] = self.m.get("max_binds", 0) + 1
                            log(f"growth held at the ceiling of {MAX_VMS} VMs ({n} wanted)")
                        tgt["why"] = "max VMs" if at_max else "bring-up cap"
                    n = got
            if distinct:
                tgt["distinct_vm_group"] = distinct[1]
                tgt["distinct_vms_used"] = sorted(used_vms)
            self.target[f"{image}/{vm_type}" + (f"/{model}" if model else "") + (f"/{distinct[0]}/{distinct[1]}" if distinct else "")] = {**tgt, "start": n, "target": len(held) + coming + tgt["want"]}  # (per image and type: 06:53, a small timing pool's numbers stood for the whole pool)
            if n <= 0:
                continue
            first = min(real, key=lambda it: (it.batch.klass, it.batch.t0)).batch
            out[(image, vm_type, model, distinct) if distinct else (image, vm_type, model) if model else (image, vm_type)] = (n, first.klass, first, not first.warm_only)
        return out

    # -- CPU constraints: additive durable state; old handoff readers ignore it.
    @staticmethod
    def _constraints_load():
        path = DIR / "cpu-constraints.json"
        return json.loads(path.read_text()) if path.exists() else {}

    def _constraints_save(self):
        path = DIR / "cpu-constraints.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.cpu_constraints))
        tmp.replace(path)

    def _constraint(self, image, vm_type, model):
        if not hasattr(self, "cpu_constraints"):
            self.cpu_constraints = {}
        key = json.dumps([image, vm_type, model])
        return self.cpu_constraints.setdefault(key, {"image": image, "vm_type": vm_type, "cpu_model": model,
                                                    "fruitless": 0, "draws": [], "observed": {}})

    def _constraint_result(self, vm_id, model, complete_known=True):
        changed = False
        for c in getattr(self, "cpu_constraints", {}).values():
            for d in c["draws"]:
                if d["vm"] == vm_id and not d["done"]:
                    if d.get("known_resume") and not complete_known:
                        continue  # exempt only after the entire resume becomes ready
                    d.update(model=model, done=True)
                    c["observed"][vm_id] = model
                    d["counted"] = rules.cpu_draw_counted(d.get("known_resume", False), c["cpu_model"], model)
                    # A verified known resume neither spends nor replenishes the speculative budget.
                    if d["counted"]:
                        c["fruitless"] = rules.cpu_streak(c["fruitless"], model == c["cpu_model"])
                        if d.get("known_resume"):
                            log(f"{vm_id}: known CPU resume {'mismatched: ' + model if model else 'failed/unknown'}; "
                                f"expected {c['cpu_model']}; counts as a fruitless draw ({c['fruitless']})")
                    changed = True
                elif d["vm"] == vm_id and d["done"] and not d["model"] and model:
                    # A late model report completes an earlier unknown observation.
                    d["model"] = model
                    c["observed"][vm_id] = model
                    if model == c["cpu_model"] and not d.get("known_resume", False):
                        c["fruitless"] = 0
                    changed = True
        if changed:
            self._constraints_save()

    def _constraint_pending(self, c):
        for d in c["draws"]:
            if not d["done"] and time.time() - d["t"] > BREAK_READY:
                self._constraint_result(d["vm"], "")
        return sum(not d["done"] and not d.get("known_resume", False) for d in c["draws"])

    def _known_cpu_reuse(self, registry, image, vm_type, model, used=()):
        """Known unleased capacity, never proof of its CPU after a new boot.

        Registry state may lag the release TTL; acquisition reconciles it before
        selecting a stopped/warm worker. No new or unknown draw inherits this allowance.
        """
        observed = self._constraint(image, vm_type, model)["observed"]
        retired = self.retired()
        return {i for i, v in registry.get("vms", {}).items()
                if observed.get(i) == model and v.get("snapshot") == image and v.get("type") == vm_type
                and v.get("state") in (boat.RUNNING | {"stopped"}) and boat.lease_free(v)
                and i not in self.vms and i not in retired and i not in used
                and i not in getattr(self, "resume_quarantine", {})}

    def _model_report(self, vm, model):
        if not isinstance(model, str) or not model.strip():
            return
        with self.lock:
            vm.model = model
            for it in list(vm.running.values()) + list(vm.finishing.values()):
                if it.last_placement and not it.last_placement.get("cpu"):
                    it.last_placement["cpu"] = model
            changed = False
            for c in getattr(self, "cpu_constraints", {}).values():
                if c["image"] == vm.image and c["vm_type"] == vm.type and c["observed"].get(vm.id) != model:
                    c["observed"][vm.id] = model
                    changed = True
            self._constraint_result(vm.id, model, complete_known=vm.ready)
            if changed:
                self._constraints_save()

    def _constraint_limit(self, it):
        return it.spec.get("cpu_model_max_fruitless", int(self.pol.get("cpu_model_max_fruitless", CPU_MODEL_MAX_FRUITLESS)))

    def _constraint_status(self):
        out = []
        for c in getattr(self, "cpu_constraints", {}).values():
            models = {}
            for d in c["draws"]:
                if d.get("known_resume") and not d.get("counted", False):
                    continue
                label = d["model"] or ("unknown" if d["done"] else "pending")
                models[label] = models.get(label, 0) + 1
            waiting = [it for b in self.batches.values() if b.image == c["image"] for it in b.items.values()
                       if it.state == "queued" and it.spec.get("vm_type") == c["vm_type"] and it.spec.get("cpu_model") == c["cpu_model"]]
            out.append({**c, "models": models,
                        "limit": max((self._constraint_limit(it) for it in waiting), default=int(self.pol.get("cpu_model_max_fruitless", CPU_MODEL_MAX_FRUITLESS))),
                        "requested_limits": sorted({self._constraint_limit(it) for it in waiting}),
                        "matching_vms": [v.id for v in self.vms.values() if v.ready and not v.dead and not v.quarantine and v.image == c["image"]
                                         and v.type == c["vm_type"] and v.model == c["cpu_model"]],
                        "waiting_items": len(waiting)})
        return out

    def _constraint_refused(self, it, c):
        if it.state != "queued":
            return
        row = {"name": it.name, "code": -1, "seconds": -1, "unplaceable_by_constraint": True,
               "error": "unplaceable-by-constraint: consecutive fruitless CPU draws exhausted",
               "constraint": next(x for x in self._constraint_status() if x["image"] == c["image"]
                                  and x["vm_type"] == c["vm_type"] and x["cpu_model"] == c["cpu_model"])}
        row["constraint"] = json.loads(json.dumps(row["constraint"]))  # freeze the refusal receipt
        row["constraint"]["limit"] = self._constraint_limit(it)
        if it.batch.strict_once:
            self._deliver_owned(it.batch, row)
        else:
            it.batch.deliver(row)
        it.state = "done"
        it.fate.delivered()

    # -- VMs
    def _ask(self, image, vm_type, n, k, b, allow, model="", distinct=None):
        try:
            got = self._acquire(image, vm_type, n, k, b, allow, model, distinct, _reserved=True)
            if not got:
                self.backoff[image] = time.time() + 15  # (nothing to be had now: ask boat again in a while, not every loop)
        except Exception as e:
            log(f"acquire: {str(e)[:200]}")
            self.backoff[image] = time.time() + 15
        finally:
            with self.lock:
                self.asking.discard(image)
            self.wake.set()

    def _acquire(self, image: str, vm_type: str, n: int, k: int, b: Batch, allow: bool = True, model: str = "", distinct=None, *, _reserved: bool = False) -> int:
        with self.lock:
            # Only _ask consumes its prior reservation; sharing its image grants nothing.
            if not self._handoff_gate().growth_allowed() and not _reserved:
                return 0
            self._acquiring = getattr(self, "_acquiring", 0) + 1
        try:
            return self._acquire_reserved(image, vm_type, n, k, b, allow, model, distinct)
        finally:
            with self.lock:
                self._acquiring -= 1
            self.wake.set()

    def _acquire_reserved(self, image: str, vm_type: str, n: int, k: int, b: Batch, allow: bool = True, model: str = "", distinct=None) -> int:
        """n VMs: warm free ones of the pool first (no start), then resumed or created, free starts before paid ones
        (every class may pay: the user, 10-31, a paid start and prepaid VM-hours beat Modal past about 2 minutes of
        work), paced by the StartGate (a sixth of what's left a minute, boat's minute limit), each with its warm
        agent. Modal only on boat's API errors, a missing image, or the extra starts' cap. How many are coming."""
        try:
            lim = boat.limits() if allow else None
            starts = max(0, boat.starts_left(lim)) if allow else 0
        except Exception:
            starts = 0
        budget = min(starts, boat.budget(starts))
        mine = []
        observed = self._constraint(image, vm_type, model)["observed"] if model else {}
        used = set()
        if distinct:
            if isinstance(distinct[0], tuple):
                _, name, generation = distinct[0]
                if not b.durable_registry.allowed(name, generation, "", time.time()):
                    return 0
                used = b.durable_registry.load()[name]["used"]
            else:
                used = b.distinct_vms.get(distinct[1], set())
        with Registry().locked() as d:
            with contextlib.suppress(Exception):
                boat.reconcile(d, boat.sandboxes())
            now = time.time()
            free = [i for i, v in d["vms"].items() if v["snapshot"] == image and v["type"] == vm_type and boat.lease_free(v) and i not in self.vms and i not in self.retired() and i not in getattr(self, "resume_quarantine", {}) and (not model or i not in observed or observed[i] == model) and rules.distinct_vm_allowed(bool(distinct), i in used)]
            up = [i for i in free if d["vms"][i].get("state") in boat.RUNNING and d["vms"][i].get("until", 0) > now + boat.WARM_MARGIN]
            stopped = [i for i in free if i not in up and d["vms"][i].get("state") == "stopped"]
            known = self._known_cpu_reuse(d, image, vm_type, model, used) if model else set()
            stopped.sort(key=lambda i: i not in known)  # known resumes before speculative draws
            if model:
                c = self._constraint(image, vm_type, model)
                limits = [self._constraint_limit(it) for batch in self.batches.values()
                          if batch.image == image for it in batch.items.values()
                          if it.state == "queued" and it.spec.get("cpu_model") == model
                          and (it.spec.get("vm_type") or batch.vm_type) == vm_type]
                limit = max(limits, default=int(self.pol.get("cpu_model_max_fruitless", CPU_MODEL_MAX_FRUITLESS)))
                pending = self.creating.get((image, vm_type), 0) + max(self._constraint_pending(c),
                    sum(not v.dead and not v.ready and not getattr(v, "cpu_known_resume", False)
                        and v.image == image and v.type == vm_type and getattr(v, "cpu_constraint", "") == model
                        for v in self.vms.values()))
                fresh = rules.cpu_parallel_draws(bool(limits), BRINGUP_MAX, pending, c["fruitless"], limit)
                if fresh > 0:
                    stopped = []  # a stale matching cache must not serialize fresh candidates
                    n = min(n, len(up) + fresh)
                else:
                    n = min(n, len(up) + rules.cpu_capacity(max(0, n - len(up)),
                            sum(i in known for i in stopped), c["fruitless"], pending, limit))
            acq = rules.Acquire()  # warm first; resumes/creations still obey the provider start budget
            acq.plan(max(0, n), len(up), len(stopped), max(0, budget))
            pick = up[:acq.warm] + stopped[:acq.resumes]
            for i in pick:
                d["vms"][i]["lease"] = {"pid": os.getpid(), "host": socket.gethostname(), "batch": "scheduler", "since": now, "label": f"scheduler ({NAMES[k]})"}
                d["vms"][i].pop("released", None)
                mine.append((i, i in up, i in known))
        create = acq.creates
        for i, warm, known_cpu in mine:
            kw = {"cpu_known_resume": True} if known_cpu and not warm else {}
            self._bring(i, image, vm_type, "warm" if warm else "resumed", k, b, model=model, **kw)
        if create > 0:
            with self.lock:
                self.creating[(image, vm_type)] = self.creating.get((image, vm_type), 0) + create
            threading.Thread(target=self._create, args=(image, vm_type, create, k, b, model), daemon=True).start()
        return len(mine) + max(0, create)

    def _bring(self, i: str, image: str, vm_type: str, how: str, k: int, b: Batch, lease: dict | None = None, nonce: str = "", model: str = "", cpu_known_resume: bool = False):
        vm = VMState(i, image, vm_type, how)
        vm.lease, vm.birth_nonce = lease or {}, nonce
        vm.cpu_constraint, vm.cpu_known_resume = model, cpu_known_resume
        with self.lock:
            if i in self.vms:  # (a VM is brought up once at a time: a second bring-up would replace its agent's token)
                return
            self.vms[i] = vm
        threading.Thread(target=self._up, args=(vm, k, b), daemon=True).start()

    def _count_start(self, k: int, b: Batch, got: int, paid: int, ids: list[str], vm_type: str = "", model: str = "", known_resume: bool = False, how: str = ""):
        with self.lock:
            n = NAMES[k]
            self.m["starts"][n]["paid"] += paid
            self.m["starts"][n]["free"] += got - paid
            b.starts += got
            for i in ids:
                if model:
                    self._constraint(b.image, vm_type, model)["draws"].append({"vm": i, "t": time.time(), "model": None, "done": False, "how": how,
                        **({"known_resume": True} if known_resume else {})})
                    self._constraints_save()
                self.starts_log.append({"t": time.time(), "vm": i, "ok": None, "image": b.image})
                self._h().record_start(time.time(), b.image, i, vm_type or (self.vms[i].type if i in self.vms else ""))
            self.starts_log = [s for s in self.starts_log if time.time() - s["t"] < 2 * BREAK_WINDOW]

    def _start_failed(self, image: str, since: float, now: float) -> bool:
        """Whether boat failed a start of this image since then: refused, or not ready within BREAK_READY."""
        return any(s.get("image") == image and s["t"] >= since and (s["ok"] is False or (s["ok"] is None and now - s["t"] > BREAK_READY))
                   for s in self.starts_log)

    def _start_done(self, vm_id: str, ok: bool):
        with self.lock:
            self._constraint_result(vm_id, self.vms[vm_id].model if vm_id in self.vms and ok else "")
            for s in self.starts_log:
                if s["vm"] == vm_id and s["ok"] is None:
                    s["ok"] = ok
            if not ok:
                self.m["not_ready"] += 1
                vm = self.vms.get(vm_id)
                if vm is not None and getattr(vm, "how", "") == "resumed":
                    vm.quarantine = "resume failed: " + getattr(vm, "start_failure", "not ready")
                    vm.quarantine_since = time.time()
                    self.__dict__.setdefault("resume_quarantine", {})[vm_id] = {
                        "t": vm.quarantine_since, "reason": vm.quarantine,
                        "image": vm.image, "type": vm.type}
                    for s in self.starts_log:
                        if s["vm"] == vm_id and s["ok"] is False:
                            s["error"] = vm.quarantine
                    log(f"{vm_id}: quarantined from future acquisition and adoption: {vm.quarantine}")
                    self._metrics_save()

    def _up(self, vm: VMState, k: int, b: Batch):
        started = False
        try:
            if vm.how == "resumed":
                got, paid = self.gate.take_split(1, self.stop)
                if not got:
                    raise RuntimeError("no start left")
                # Override the released VM's 60s TTL in the resume itself;
                # extending after bootstrap is too late if hydration/claim takes longer.
                self._count_start(k, b, 1, paid, [vm.id], vm.type, getattr(vm, "cpu_constraint", ""),
                                  known_resume=getattr(vm, "cpu_known_resume", False), how="resumed")
                started = True
                out = boat.boat("resume", vm.id, "--ttl", str(vm.ttl), "--json", check=False, timeout=300)
                if boat.R.resume_replaces(boat.resume_missing(out), False):
                    boat.note_starts(-1)
                    boat.forget_missing(vm.id, "scheduler")
                    vm.dead = True
                    log(f"{vm.id}: boat no longer has it; replanning on fresh capacity")
                    return
                if "rate_limited" in out:
                    boat.note_starts(-1)
                    raise RuntimeError("boat refused the resume")
            started = started or vm.how == "created"
            if not boat.wait_running([vm.id], int(BREAK_READY)):
                raise RuntimeError("never came up")
            old = boat.read_agent(vm.id) or {}
            cached = vm.lease or old.get("lease") or self.held_before.get(vm.id, {}).get("lease")
            boot = {}
            if vm.how in ("created", "resumed") or (not cached and vm.how != "adopt"):
                tok = old.get("token") or boat.owner_key()
                boot = boat.bootstrap_agent(vm.id, self.agent_b64, tok, 90, nonce=getattr(vm, "birth_nonce", ""))
                # Even a lost daemon response may have persisted the successful claim.
                old = boat.read_agent(vm.id) or {}
                cached = old.get("lease")
                claim = boat.lease_vm(vm.id, "renew", lease=cached) if cached else boot
            else:
                claim = boat.lease_vm(vm.id, "renew", lease=cached) if cached else boat.lease_vm(vm.id, "claim")
            if not claim.get("ok") or claim.get("stale") or not claim.get("boot") or not claim.get("epoch"):
                detail = {label: {key: reply[key] for key in
                          ("ok", "stale", "busy", "birth", "valid_boot", "draining", "state", "error") if key in reply}
                          for label, reply in (("bootstrap", boot), ("claim", claim)) if reply}
                vm.start_failure = "lease claim refused or incomplete: " + json.dumps(detail, sort_keys=True)
                if started:
                    self._start_done(vm.id, False)
                    started = False
                self._drop_stale(vm, source='up.claim_refused', reply=claim, operation='lease/renew')
                return
            vm.lease = claim
            boat.remember_lease(vm.id, claim)
            if vm.how != "created":
                boat.rename_leased_vm(vm.id, vm.image, claim)
            vm.t_renew = time.monotonic()
            boat.boat("extend", vm.id, "--ttl", str(vm.ttl), "--json", check=False)
            cl = boat.agent_client(vm.id)
            ok, pong = False, {}
            if cl:
                cl.lease = vm.lease
                with contextlib.suppress(Exception):
                    pong = cl.ping()
                    ok = pong.get("version") == self.agent_version
            if not ok:
                if not boat.wait_running([vm.id], int(BREAK_READY)):
                    raise RuntimeError("never came up")
                tok = (boat.read_agent(vm.id) or {}).get("token") or old.get("token") or boat.owner_key()
                r = boat.bootstrap_agent(vm.id, self.agent_b64, tok, 90, lease=vm.lease, nonce=getattr(vm, "birth_nonce", ""))
                if not r.get("ok"):
                    raise RuntimeError(f"agent: {r}")
                old = boat.read_agent(vm.id) or {}
                url = old.get("url") or boat.bj("host", vm.id, str(boat.AGENT_PORT), "--localhost", "--private")["url"]
                cl = AgentClient(vm.id, url, tok, lease=vm.lease)
                try:
                    pong = cl.ping()
                except AgentError:
                    url = boat.bj("host", vm.id, str(boat.AGENT_PORT), "--localhost", "--private")["url"]
                    cl = AgentClient(vm.id, url, tok, lease=vm.lease)
                    pong = cl.ping()
                boat.write_agent(vm.id, {"url": url, "token": tok, "t": time.time(), "version": self.agent_version, "lease": vm.lease})
            cl = SchedulerAgentClient(cl, self, vm)
            vm.client = cl
            self._record_agent_ping(vm, pong)
            vm.t_agent = time.time()
            r = cl.rpc("vtick", {"wait": 0}, timeout=30)
            if r.get("stale"):
                self._drop_stale(vm, source='up.stale_reply', reply=r, operation='rpc/vtick')
                return
            vm.slots = max(1, int(r.get("cpus") or boat.TYPES[vm.type][0]) // boat.SLOT_CPU)
            self._model_report(vm, r.get("model", ""))
            self._foreign(vm, r)  # (inventory before advertising this worker as usable)
            if getattr(vm, "cpu_constraint", ""):
                with self.lock:
                    self._constraint(vm.image, vm.type, vm.cpu_constraint)["observed"][vm.id] = vm.model
                    self._constraints_save()
            with self.lock:
                vm.ready = True
                vm.t_ready = vm.idle_since = vm.t_real = time.time()
                if vm.id in self.held_before:
                    vm.t_real = float(self.held_before[vm.id].get("t_real", vm.t_real))
                if started:
                    self._start_done(vm.id, True)
                    started = False
                    self.bring_up = 0.8 * self.bring_up + 0.2 * (vm.t_ready - vm.t_lease)
                discard = rules.cpu_discard_candidate(getattr(vm, "cpu_constraint", ""), vm.model, len(vm.running), len(vm.finishing), getattr(vm, "foreign", 0))
                if discard:
                    vm.dead, vm.ready = True, False  # never advertise a doomed candidate to placement
            if discard:
                log(f"{vm.id}: CPU candidate {vm.model} != {vm.cpu_constraint}; releasing empty owned VM")
                self._let_go(vm, stop=True)
                return
            self._worker(vm)
            log(f"{vm.id} ({vm.how}) ready for {NAMES[k]} in {vm.t_ready - vm.t_lease:.0f} s" if vm.how != "adopt" else f"{vm.id} adopted (agent {vm.agent})")
        except boat.LeaseProtocolLost as error:
            self.alert(f"{vm.id}: lease protocol lost; old client wins, relinquished without stopping")
            self._drop_stale(vm, source='up.protocol_lost', error=error)
        except Exception as e:
            if vm.dead:
                return
            log(f"{vm.id}: {str(e)[:200]}")
            vm.start_failure = str(e)[:200]
            if started:
                self._start_done(vm.id, False)
                started = False
            vm.dead = True
            self._let_go(vm, stop=True)
        finally:
            if started and not vm.ready:
                self._start_done(vm.id, False)
            self.wake.set()

    def _create(self, image: str, vm_type: str, n: int, k: int, b: Batch, model: str = ""):
        try:
            got, paid = self.gate.take_split(n, self.stop)
            if got <= 0:
                return
            try:
                with boat.new_vm_args(image) as (birth_args, nonce):
                    out = boat.boat("new", "--from", image, *birth_args, "--type", vm_type, "--count", str(got), "--ttl", str(boat.BACKSTOP), "--no-env", "--no-monitoring", "--json", timeout=600, check=False)
            except Exception as e:  # (boat new that never answered (06:5x: a 600 s timeout for a small VM): no VM, the starts given back,
                log(f"boat new --from {image} --type {vm_type}: {str(e)[:160]}; backing off 120 s")  # its image's starts backed off; a VM
                out = ""  # made all the same is up with no owner: adoption takes it in (work queued) or stops it)
                self.backoff[image] = time.time() + 120
            ids = []
            for line in out.splitlines():
                with contextlib.suppress(Exception):
                    e = json.loads(line)
                    if e.get("id") and e["id"] not in ids and e.get("event") != "error":
                        ids.append(e["id"])
            if len(ids) < got:
                boat.note_starts(-(got - len(ids)))
                if not ids:
                    self.alert(f"boat new --from {image} made no VM ({out.strip()[-200:]})")
            self._count_start(k, b, len(ids), min(paid, len(ids)), ids, vm_type, model, how="created")
            with Registry().locked() as d:
                for i in ids:
                    d["vms"][i] = {"type": vm_type, "snapshot": image, "created": time.time(), "last_used": time.time(), "state": "starting",
                                   "lease": {"pid": os.getpid(), "host": socket.gethostname(), "batch": "scheduler", "since": time.time(), "label": f"scheduler ({NAMES[k]})"}}
            for i in ids:
                self._bring(i, image, vm_type, "created", k, b, nonce=nonce, model=model)
        finally:
            with self.lock:
                self.creating[(image, vm_type)] = max(0, self.creating.get((image, vm_type), 0) - n)
            self.wake.set()

    def _note_lease_relinquish(self, vm: VMState, *, source, error=None, reply=None, operation=''):
        """Keep the initiating evidence before a dead wrapper hides it; never gate a fence on logging."""
        with self.lock:
            prior = getattr(vm, 'lease_relinquishment', None)
            if isinstance(prior, dict) and vm.dead:
                return prior  # only an actual relinquishment freezes its first cause
            try:
                diagnostic = lease_diagnostics.event(vm.id, source, lease=getattr(vm, 'lease', None),
                                                      client=getattr(vm, 'client', None),
                                                      error=error, reply=reply, operation=operation)
            except Exception as failure:
                diagnostic = {'event': 'lease_relinquish', 'vm': vm.id, 'source': source,
                              'diagnostic_error': type(failure).__name__}
            diagnostic['t'] = time.time()
            vm.lease_relinquishment = diagnostic
            with contextlib.suppress(Exception):
                log('LEASE_RELINQUISH', json.dumps(diagnostic, sort_keys=True))
            return diagnostic

    def _drop_stale(self, vm: VMState, *, source='unspecified', error=None, reply=None, operation=''):
        """Lease loss is not a VM fault: no stop, kill, or item strike."""
        with self.lock:
            self._note_lease_relinquish(vm, source=source, error=error, reply=reply, operation=operation)
            vm.dead, vm.ready = True, False
            if self.vms.get(vm.id) is vm:
                self.vms.pop(vm.id, None)
            for key, it in list(vm.running.items()):
                vm.running.pop(key, None)
                if it.batch.admission_closed or it.owned.closed:
                    if it.state != "done":
                        it.state = "cancel_pending"
                    continue
                it.fate.vm_lost(vm.id, True)
                if requeue_after_loss(it.batch.strict_once):
                    it.state, it.vm, it.head = "queued", None, True
                else:
                    self._owned_failed(it, "strict_once: worker lease lost; no replacement")
            vm.starts = []
        self.wake.set()

    def _renew_lease(self, vm: VMState) -> bool:
        def lost(source, *, error=None, reply=None):
            if getattr(self, "handing_off", False):
                self._note_lease_relinquish(vm, source=source, error=error, reply=reply, operation='lease/renew')
                self._handoff_lease_lost = True  # keep frozen identities; the parent must not commit
            else:
                self._drop_stale(vm, source=source, error=error, reply=reply, operation='lease/renew')
        if not vm.lease:
            lost('renew.missing_fence')
            return False
        try:
            r = boat.lease_vm(vm.id, "renew", lease=vm.lease)
        except boat.LeaseProtocolLost as error:
            self.alert(f"{vm.id}: lease protocol lost; old client wins, relinquished without stopping")
            lost('renew.protocol_lost', error=error)
            return False
        if not r.get("ok") or r.get("stale"):
            lost('renew.refused', reply=r)
            return False
        vm.t_renew = time.monotonic()
        return True

    def _boat_up(self, vm_id: str) -> bool:
        """boat's own word on a VM: up (a VM boat reports up is never called lost by us)."""
        with contextlib.suppress(Exception):
            return boat.states(10).get(vm_id, {}).get("state") in boat.RUNNING
        return True  # (boat's list not to be had: no verdict, so not lost)

    def _reclaim_ghosts(self, vm):
        """Observe/journal confirmed legacy ghosts; deliver terminal LOST, never retry here."""
        from . import workerprobe
        with self.lock:
            runs = [{"batch": it.batch.id, "name": it.run_name, "strict": it.batch.strict_once}
                    for it in vm.running.values() if it.state == "running"]
        if not runs:
            return {"protocol": 1, "runs": []}
        receipt = workerprobe.audit(vm, runs, reclaim=True)
        directory = DIR / "worker-recovery" / vm.id
        directory.mkdir(parents=True, exist_ok=True)
        write_private(directory / f"{time.time_ns()}.json", receipt)
        expected = {(r["batch"], r["name"]) for r in runs}
        for row in receipt.get("runs", []):
            key = (row.get("batch"), row.get("name"))
            if key not in expected or row.get("reclaimed") is not True:
                continue
            result = row.get("result") or {}
            with self.lock:
                it = vm.running.get(key)
                if it is None or it.batch.strict_once:
                    continue
            if (result.get("batch"), result.get("name")) != key or result.get("ghost_reclaimed") is not True:
                raise ValueError("ghost receipt identity mismatch")
            if self._finished_legacy(vm, result):
                log(f"{vm.id}: {key[0]}/{key[1]}: LOST; recorded process and descendants absent, no result")
        return receipt

    def _repair(self, vm: VMState, *, reclaim_ghosts=False) -> bool:
        """Keep live/unknown ownership intact; reclaim proven ghosts before a normal bootstrap."""
        if any(it.batch.strict_once for it in vm.running.values()):
            return False  # no legacy audit can discharge strict ownership
        if vm.running and not reclaim_ghosts and getattr(vm, "agent", "") != self.agent_version:
            return False  # busy cross-version recovery is an explicit operator action
        repair_lock = vm.__dict__.setdefault("repair_lock", threading.Lock())
        if not repair_lock.acquire(blocking=False):
            return False
        fenced = False
        try:
            with self.lock:
                if (getattr(vm, "upgrading", False) or not self._handoff_gate().growth_allowed()
                        or getattr(self, "handing_off", False)):
                    return False
                vm.upgrading, fenced = True, True
            if time.time() - getattr(vm, "t_repair", 0) < 30:
                return False
            vm.t_repair = time.time()
            vm.recovery_result = {"vm": vm.id, "target_agent": self.agent_version, "ok": False}
            if any(it.batch.strict_once for it in vm.running.values()):
                vm.recovery_result["error"] = "unresolved strict ownership"
                return False
            if not self._renew_lease(vm):
                return False
            if vm.running and reclaim_ghosts:
                vm.recovery_result["audit"] = self._reclaim_ghosts(vm)
            if not rules.worker_repair_allowed(False, bool(vm.running), getattr(vm, "agent", "") == self.agent_version):
                vm.recovery_result["error"] = "live or unknown legacy ownership"
                return False
            boot = boat.bootstrap_agent(vm.id, self.agent_b64, vm.client.token, 60, 2, lease=vm.lease)
            if not boot.get("ok"):
                vm.recovery_result["error"] = "bootstrap refused"
                return False
            pong = vm.client.ping(timeout=15)
            if pong.get("ok") and pong.get("version") == self.agent_version:
                self._record_agent_ping(vm, pong)
                log(f"{vm.id}: its agent didn't answer: repaired in place ({len(vm.running)} live/unknown items retained)")
                vm.failing, vm.fails = 0, 0
                vm.recovery_result["ok"] = True
                return True
            vm.recovery_result["error"] = "target agent did not answer"
            return False
        except Exception as error:
            vm.recovery_result["error"] = type(error).__name__
            return False
        finally:
            if fenced:
                with self.lock:
                    vm.upgrading = False
            repair_lock.release()

    def recover_worker(self, vm_id, expected_agent):
        """Exact-worker administrative recovery through the controller's ordinary repair path."""
        if not isinstance(vm_id, str) or not isinstance(expected_agent, str) or expected_agent != self.agent_version:
            raise ValueError("exact VM and current expected_agent are required")
        with self.lock:
            vm = self.vms.get(vm_id)
            if vm is None or vm.dead:
                raise ValueError("worker is not held by this controller")
            if (getattr(self, "handing_off", False) or getattr(self, "recovery_blocked", False)
                    or not self._handoff_gate().growth_allowed()):
                raise ValueError("controller ownership is not stable")
        repaired = self._repair(vm, reclaim_ghosts=True)
        return {**vm.recovery_result, "vm": vm.id, "target_agent": self.agent_version, "ok": repaired}

    def _outage(self) -> bool:
        """boat's front failing most VMs' agents at once (the 2026-10-31 incident: every tick 502): no VM is at fault,
        none is let go, and no start is made until it passes."""
        rv = self._outage_vms()
        return rules.outage(len(rv), sum(1 for v in rv if v.failing))

    def _outage_vms(self):
        return [v for v in self.vms.values()
                if rules.worker_outage_member(v.ready, v.dead, bool(getattr(v, "quarantine", "")))]

    # -- the VMs' ticks: a fixed pool of pollers over every VM (POLLERS threads whatever the pool's size), each VM due
    # at its next tick in a heap; never a thread per VM (07:02: 356 threads, 82-94% of a core at 181 VMs, every idle
    # VM's thread waking 5 times a second)
    POLLERS = 32  # (a tick waits for nothing: at 200 VMs about 100 calls a second; a hung agent holds one poller TICK_S at most)
    TICK_S = 20

    def _cpu_add(self, part: str, c0: float):
        """This thread's CPU since c0 charged to a part of the scheduler (status: load.cpu_s, load.cpu_n)."""
        c = self.__dict__.setdefault("_cpu", {})
        n = self.__dict__.setdefault("_cpu_n", {})
        c[part] = c.get(part, 0.0) + _cpu_now() - c0
        n[part] = n.get(part, 0) + 1

    def _poll_add(self, vm: VMState, delay: float = 0.0):
        import heapq
        with self._pcv():
            heapq.heappush(self._pheap, (time.time() + delay, next(self._pseq), vm))
            self._pcond.notify()

    def _pcv(self):
        if not hasattr(self, "_pcond"):
            import itertools
            self._pcond, self._pheap, self._pseq = threading.Condition(), [], itertools.count()
            self._pollers = [threading.Thread(target=self._poller, name=f"poller-{k}", daemon=True) for k in range(self.POLLERS)]
            for t in self._pollers:
                t.start()
        return self._pcond

    def _poller(self):
        import heapq
        while not self.stop.is_set():
            with self._pcond:
                while not self.stop.is_set():
                    if self._pheap and self._pheap[0][0] <= time.time():
                        _, _, vm = heapq.heappop(self._pheap)
                        break
                    self._pcond.wait(min(1.0, max(0.01, self._pheap[0][0] - time.time())) if self._pheap else 1.0)
                else:
                    return
            if vm.dead or self.vms.get(vm.id) is not vm and not vm.running:
                continue
            c0 = _cpu_now()
            try:
                delay = self._tick(vm)
            except Exception as e:
                log(f"{vm.id}: tick: {e!r}"[:300])
                delay = 5.0
            self._cpu_add("tick", c0)
            if delay is not None and not vm.dead:
                self._poll_add(vm, delay)

    def _worker(self, vm: VMState):
        """The VM taken into the pollers' heap (its ticks: its starts and acks out, every batch's finished items back)."""
        vm._fails = 0
        self._poll_add(vm)

    def _tick(self, vm: VMState) -> float | None:
        """One tick of the VM (a call that waits for nothing): the seconds until its next, None when it's done."""
        if vm.dead:
            return None
        if getattr(self, "handing_off", False):
            return 0.5
        if time.monotonic() - vm.t_renew >= 60:
            try:
                if not self._renew_lease(vm):
                    return None
            except Exception as error:
                # No placement while authority cannot be verified. A stopped VM
                # cannot renew at all: do not pin its work/capacity forever.
                vm.ready = False
                if not self._boat_up(vm.id):
                    self._note_lease_relinquish(vm, source='renew.transport_provider_down',
                                              error=error, operation='lease/renew')
                    self._lost_vm(vm)
                    return None
                return 5.0
            vm.ready = True
        with vm.lock:
            starts, acks = vm.starts, vm.acks
            vm.starts, vm.acks = [], []
        if time.time() - vm.t_agent > 600 and not vm.running and not starts:
            with contextlib.suppress(Exception):
                self._check_agent(vm)
        if vm.dead:
            return None
        if not vm.quarantine and not vm.running and not starts and not acks and not getattr(vm, "foreign", 0) and time.time() - getattr(vm, "t_inventory", 0) < 30:
            if vm.ready is False:
                return None
            if vm.client and time.time() - vm.t_disk >= (30 if vm.disk_low else 300):  # (its disk asked by ping every 5 min, 30 s while low:
                vm.t_disk = time.time()  # a vtick of an empty VM would read its runs mid-handoff)
                with contextlib.suppress(Exception):
                    self._disk(vm, vm.client.ping().get("disk") or {})
            return 1.0  # (nothing of ours there: no call, looked at again in a second)
        busy_queue = getattr(self, "_any_queued", True)  # (the last pass's word: no scan of every item at every tick of every VM)
        strict_starts = [st for st in starts if st.get("spec", {}).get("strict_once") is True]
        strict_acks = [ack for ack in acks if "launch_identity" in ack]
        starts = [st for st in starts if st not in strict_starts]
        acks = [ack for ack in acks if ack not in strict_acks]
        owned_done = []
        if strict_starts or strict_acks:
            try:
                strict_result = self._owned_client(vm).rpc(
                    "vtick", {"start": strict_starts, "acks": strict_acks, "wait": 0}, timeout=self.TICK_S)
                owned_done = strict_result.get("done", [])
            except Exception as error:
                if vm.dead:
                    return None
                with self.lock:
                    for st in strict_starts:
                        item = vm.running.get((st["batch"], st["spec"]["name"]))
                        if item:
                            item.result_error = str(error)
                            self._owned_failed(item, "strict_once: start reply uncertain; cleanup retained: " + str(error)[:200])
                with vm.lock:
                    vm.acks = strict_acks + vm.acks  # unknown reply never discards acknowledgements
        try:
            r = vm.client.rpc("vtick", {"start": starts, "acks": acks, "wait": 0}, timeout=self.TICK_S)
            if r.get("stale"):
                self._drop_stale(vm, source='tick.stale_reply', reply=r, operation='rpc/vtick')
                return None
            r["done"] = owned_done + r.get("done", [])
            vm._fails, vm.failing = 0, 0
            if vm.quarantine:
                log(f"{vm.id}: worker quarantine cleared by a successful tick")
                vm.quarantine, vm.quarantine_since = "", 0.0
        except Exception as e:
            if "stale" in str(e).lower() or "403" in str(e) or "400" in str(e):
                self.alert(f"{vm.id}: lease protocol lost or refused; relinquished without stopping")
                self._drop_stale(vm, source='tick.exception', error=e, operation='rpc/vtick')
                return None
            fails = vm._fails = getattr(vm, "_fails", 0) + 1
            vm.failing = fails
            if isinstance(e, boat.WorkerEndpointUnavailable) and not vm.quarantine:
                vm.quarantine = "worker-local agent connection refused"
                vm.quarantine_since = time.time()
                log(f"{vm.id}: quarantined: {vm.quarantine}")
            outage = self._outage()
            if "is stopped" in str(e) and rules.vm_lost(fails, outage, False, True):  # (boat stopped it: let go at once, its items to other VMs)
                log(f"{vm.id}: boat reports it stopped")
                self._note_lease_relinquish(vm, source='tick.provider_stopped', error=e, operation='rpc/vtick')
                self._lost_vm(vm)
                return None
            if fails >= 2 and fails % 4 == 2:  # (its agent repaired in place, its items running on)
                if self._repair(vm):
                    vm._fails = 0
                    return 0.0
                if vm.dead:
                    return None
            with vm.lock:
                vm.starts = starts + vm.starts
                vm.acks = acks + vm.acks
            if outage and time.time() >= self.br["open_until"]:
                self.trip(f"boat's front: the agents of {sum(1 for v in self._outage_vms() if v.failing)} of {len(self._outage_vms())} eligible VMs don't answer ({str(e)[:80]})")
            if fails <= 3 or fails % 10 == 0:
                log(f"{vm.id}: tick failed ({fails}{', an outage' if outage else ''}): {str(e)[:160]}")
            # ("lost": rules.vm_lost, boat reports it stopped or gone; boat asked only past the failures' threshold)
            if rules.vm_lost(fails, outage, fails < (90 if outage else 12) or self._boat_up(vm.id), False):
                self._note_lease_relinquish(vm, source='tick.provider_lost', error=e, operation='rpc/vtick')
                self._lost_vm(vm)
                return None
            return float(min(20, 2 * fails))
        if self.stop.is_set():
            return None  # The successor owns observations after the frozen snapshot.
        if r.get("err"):  # (a batch's directory gone from the VM: prepared again)
            self._unprepare(vm, r.get("batch"))
            with self.lock:
                for key, it in list(vm.running.items()):
                    if not it.batch.admission_closed and not it.owned.closed and it.batch.id == r.get("batch") and it.state == "running":
                        vm.running.pop(key)
                        it.state, it.vm = "queued", None
                        it.fate.requeued()
            self.wake.set()
            return 0.5
        self._model_report(vm, r.get("model", ""))
        self._disk(vm, r.get("disk") or {})
        self._foreign(vm, r)
        for row in r.get("done", []):
            self._accept_finished(vm, row)
        if r.get("done") or vm.acks:
            return 0.5  # (its acks out soon)
        # A drained queue can still have an urgent final result in flight.
        urgent = any(it.batch.klass <= 2 for it in vm.running.values())
        return 2.0 if (busy_queue or urgent or not vm.running) and not self.shed else 10.0

    def _disk(self, vm: VMState, d: dict):
        """The agent's word on its disk with each tick (an agent before 11-02 says nothing: as fit as before): below
        rules.disk_ok's threshold nothing is placed there; its agent cleans its scratch meanwhile (agent.clean_disk)."""
        if "free_mb" not in d:
            return
        now = time.time()
        low = not rules.disk_ok(int(d["free_mb"]), int(d.get("total_mb", 0)), DISK_MIN_MB)
        if low != vm.disk_low:
            log(f"{vm.id}: disk {'low' if low else 'free again'}: {d['free_mb'] / 1024:.1f} GB free of {d.get('total_mb', 0) / 1024:.0f}"
                + (": nothing placed there while its agent cleans its scratch" if low else ""))
            vm.t_low = now if low else 0.0
        vm.disk, vm.disk_low, vm.t_disk = d, low, now

    def _foreign(self, vm: VMState, r: dict):
        """Runs the agent reports on the VM that this scheduler doesn't hold there: their slots counted taken (B11);
        one of an item done, or now running under another name, killed (a run nobody will read)."""
        if vm.dead or not vm.lease:
            return
        with self.lock:
            mine = set(vm.running)
            n, stale, stray, now = 0, [], 0, time.time()
            first, seen = getattr(vm, "_unowned", {}), {}  # (each unowned run's first sighting: stray only past STRAY_S)
            for x in r.get("running", []):
                if x.get("owner") != boat.owner_id():
                    n += vm.slots  # Unknown/foreign shape reserves capacity; never kill it.
                    continue
                key = (x.get("batch"), x.get("name"))
                if key in mine:
                    if vm.running[key].batch.closed:  # (its client closed the batch, its kill lost: killed again, its slot ours till then)
                        stale.append(key)
                    continue
                if self._unclassified(key[0]):
                    n += vm.slots  # unknown shape: reserve the whole VM, never kill it
                    continue
                b = self.batches.get(key[0])
                it = b.items.get(str(key[1]).rsplit("~s", 1)[0]) if b else None
                if it is not None and it.state == "queued" and it.run_name == key[1] and it.fate.place(vm.id, False):
                    it.state, it.vm = "running", vm  # (its own last run, there: taken back, not run again)
                    it.t_start = it.t_start or time.time()
                    vm.running[key] = it
                    vm.prepared.add(b.id)
                    log(f"{vm.id}: {b.id}: {key[1]}: its run found there: taken back")
                    continue
                n += it.weight if it else 1
                seen[key] = first.get(key, now)
                if now - seen[key] > STRAY_S:
                    stray += it.weight if it else 1
                if b is None or it is None or it.state == "done" or (it.state in ("starting", "running") and it.run_name != key[1]):
                    stale.append(key)  # (a run nobody reads: of a batch closed, of an item done or running under another name)
            changed = n != getattr(vm, "foreign", 0)
            vm.foreign, vm.stray, vm._unowned = n, stray, seen
            vm.t_inventory = now
            if changed:
                log(f"{vm.id}: foreign inventory {n} slots: {sorted(seen)}")
                self.wake.set()
        killed = self.__dict__.setdefault("_killed_stale", {})
        for bid, name in stale:
            if time.time() - killed.get((vm.id, bid, name), 0) > 60:  # (a kill lost (its agent hung) asked again a minute later)
                if (vm.id, bid, name) not in killed:
                    log(f"{vm.id}: {bid}: {name}: a run no item owns (its batch closed, its item done or running elsewhere): killed")
                killed[(vm.id, bid, name)] = time.time()
                threading.Thread(target=self._kill_run, args=(vm, bid, name), daemon=True).start()

    def _kill_run(self, vm: VMState, bid: str, name: str):
        if vm.dead or not vm.lease or getattr(self, "recovery_blocked", False) or self._unclassified(bid):
            return
        with self.lock:
            b = self.batches.get(bid)
            if b and b.strict_once:
                it = next((x for x in b.items.values() if x.run_name == name), None)
                strict = True
            else:
                strict = False
        if strict:
            if it is not None:
                self._cancel_strict_item(vm, it)
            return  # Unknown strict identity: never fall back to a legacy kill.
        with contextlib.suppress(Exception):
            vm.client.rpc("killitem", bid, name, timeout=20)

    def _accept_finished(self, vm, row):
        if getattr(self, "recovery_blocked", False) or self._unclassified(row["batch"]):
            return False  # leave results on the agent until their admission is decoded
        ack = {"batch": row["batch"], "name": row["name"]}
        if "launch_identity" in row:
            ack["launch_identity"] = row["launch_identity"]
        b = self.batches.get(row["batch"])
        if b is None:
            held = vm.running.get((row["batch"], row["name"]))
            b = held.batch if held is not None else None
        if b is not None and not b.strict_once:
            if getattr(self, "handing_off", False):
                return False
            if not self._finished_legacy(vm, row):
                return False
            with vm.lock:
                vm.acks.append(ack)
            return True
        if not self._finished(vm, row):
            return False
        with vm.lock:
            vm.acks.append(ack)
        return True

    def _result_identity_matches(self, it, row):
        if not it.batch.strict_once:
            return True
        if it.launch_identity is not None and row.get("launch_identity") == it.launch_identity:
            it.result_error = None
            return True
        # An old done.json is not evidence about the admitted launch. Keep its
        # actual placement held, never replace it or acknowledge the stale row.
        it.result_error = "strict result launch_identity missing or mismatched; placement held"
        return False

    def _finished_logs(self, vm, bid, run_name):
        with contextlib.suppress(Exception):
            lg = vm.client.rpc("itemlogs", bid, run_name, timeout=60)
            d = DIR / "queue" / f"{bid}.logs" / run_name
            d.mkdir(parents=True, exist_ok=True)
            for f in ("out", "err"):
                (d / f).write_text(lg.get(f, ""))

    def _fetch_result_files(self, vm, row):
        """Durably import complete payloads before delivery/ACK; no size refusal."""
        store = self._result_store(row['batch'])
        def ensure(ref):
            resultfiles.validate_ref(ref)
            try:
                store.read(ref, 0, 0)
                return
            except FileNotFoundError:
                pass
            def read(offset, count):
                part = vm.client.rpc('resultpart', row['batch'], row['name'], ref['sha256'], offset, count, timeout=120)
                if (not isinstance(part, dict) or part.get('offset') != offset
                        or part.get('bytes') != ref['bytes'] or part.get('sha256') != ref['sha256']):
                    raise ValueError('invalid result chunk receipt')
                data = base64.b64decode(part.get('data', ''), validate=True)
                if len(data) > count:
                    raise ValueError('oversized result chunk')
                return data
            store.import_ref(ref, read)
        for key in ('row_ref', 'result_ref', 'output_files_ref'):
            if key in row:
                ensure(row[key])
        if 'output_files_ref' in row:
            with store.open(row['output_files_ref']) as source:
                for entry in resultjson.iter_array(source):
                    if not isinstance(entry, dict) or not isinstance(entry.get('path'), str):
                        raise ValueError('invalid output manifest entry')
                    ensure({k: v for k, v in entry.items() if k != 'path'})
        return store.compact_row(row)

    def _finished_ghost(self, vm, row):
        """Commit terminal legacy loss before releasing a recorded slot; never requeue."""
        key = (row["batch"], row["name"])
        with self.lock:
            if getattr(self, "handing_off", False):
                return False
            it = vm.running.get(key)
            if it is None:
                return key not in vm.finishing
            if it.batch.strict_once:
                return False
            done = {**{k: v for k, v in row.items() if k != "batch"}, "name": it.name,
                    "vm": vm.id, "run_name": it.run_name, "placement": dict(it.last_placement or {})}
            self._deliver_owned(it.batch, done)  # fsynced; an error leaves ownership/occupancy intact
            vm.running.pop(key)
            it.state = "done"
            it.fate.finished()
            if it.t_start:
                vm.busy_s += time.time() - it.t_start
            vm.idle_since = time.time()
        self.wake.set()
        return True

    def _finished_legacy(self, vm: VMState, row: dict):
        if row.get("ghost_reclaimed") and row.get("lost"):
            return self._finished_ghost(vm, row)
        if getattr(self, "handing_off", False):
            return False
        row = self._fetch_result_files(vm, row)
        # Terminal runs release capacity immediately, but remain in the
        # handoff inventory until their row is committed and safe to ACK.
        key = (row["batch"], row["name"])
        pending = vm.__dict__.setdefault("finishing", {})
        with self.lock:
            if getattr(self, "handing_off", False):
                return False
            it = vm.running.pop(key, None)
            if it is None:
                return key not in pending  # another poller is still committing it
            if it.t_start:
                vm.busy_s += time.time() - it.t_start
            vm.idle_since = time.time()
            b = it.batch
            if b.closed:
                return True
            if row.get("preempted"):
                it.state, it.vm, it.t_queued, it.head = "queued", None, time.time(), True  # (back at the head of its class)
                it.fate.preempted()
                it.tries -= 1
            elif row.get("lost"):
                # (rules.lost_row: the VM dropped the batch's tree, its preparation here is gone, our doing: prepared
                # again, the item queued with no strike; any other loss a strike, its agent's err said)
                err = str(row.get("err", ""))[:160]
                what = rules.lost_row("tree isn't on" in err, it.state == "preempting")
                if what == "preempted":  # (its kill raced its report: the preemption it is, no strike)
                    it.state, it.vm, it.t_queued, it.head = "queued", None, time.time(), True
                    it.fate.preempted()
                    it.tries -= 1
                elif what == "reprepare":
                    self._unprepare(vm, b.id)
                    it.fate.requeued()
                    it.state, it.vm, it.t_queued, it.head = "queued", None, time.time(), True
                    log(f"{vm.id}: {b.id}: {it.name}: its row lost ({err}): the batch prepared again there, no strike")
                else:
                    self._boat_failed(it, vm.id, f"its row lost on {vm.id}: {err or 'no reason given'}")
            else:
                it.state = "finishing"
                pending[key] = it
                it.fate.finished()
                b.slot_s += max(0.0, float(row.get("seconds", 0) or 0)) * it.weight
                if float(row.get("seconds", -1) or -1) > 0:
                    self._learn(it, float(row["seconds"]))
                    b.obs_s, b.obs_est, b.obs_n = getattr(b, "obs_s", 0.0) + float(row["seconds"]), getattr(b, "obs_est", 0.0) + it.est, getattr(b, "obs_n", 0) + 1
                done = {**{k: v for k, v in row.items() if k != "batch"}, "name": it.name, "vm": vm.id, "run_name": it.run_name,
                        "placement": {**{k: (it.last_placement or {})[k] for k in ("distinct_vm_group", "boot_id", "distinct_vm_group_scope", "distinct_vm_group_generation", "attempt", "agent_version", "agent_capabilities") if k in (it.last_placement or {})},
                                      "vm": vm.id, "type": vm.type, "cpu": (it.last_placement or {}).get("cpu") or vm.model, "vcpus": it.weight * boat.SLOT_CPU, "vm_vcpus": boat.TYPES[vm.type][0],
                                      "exclusive": it.timing, "co_tenants": it.peers}}
        self.wake.set()
        if it.state != "finishing":
            return True
        self._finished_logs(vm, b.id, it.run_name)
        with self.lock:
            if getattr(self, "handing_off", False):
                return False  # successor sees pending as a terminal, unacknowledged run
            b.deliver(done)  # same lock as the snapshot; ACK only after this commit
            it.state = "done"
            pending.pop(key, None)
        self.wake.set()
        return True

    def _finished(self, vm: VMState, row: dict):
        """Return True only when the row may be acknowledged to this worker."""
        b = self.batches.get(row["batch"])
        if b is None:
            held = vm.running.get((row["batch"], row["name"]))
            b = held.batch if held is not None else None
        if b is not None and not b.strict_once:
            return self._finished_legacy(vm, row)
        if getattr(self, "handing_off", False):
            return False
        key = (row["batch"], row["name"])
        with self.lock:
            it = vm.running.get(key)
            if it is None:
                b = self.batches.get(key[0])
                if b and b.strict_once:
                    prior = next((x for x in b.items.values() if x.run_name == key[1]), None)
                    return bool(prior and prior.last_placement
                                and prior.last_placement.get("vm") == vm.id
                                and self._result_identity_matches(prior, row)
                                and any(r.get("name") == prior.name for r in b.rows))
                return True
            if not self._result_identity_matches(it, row):
                return False
            b = it.batch
            if b.admission_closed or it.owned.closed:
                return False  # only a bound cancelitem receipt can release this hold
        row = self._fetch_result_files(vm, row)
        # Owned cancellation may win during output I/O: its placement/state
        # stays held until the identity/fence-checked atomic terminal commit.
        receipt = None
        if not row.get("preempted") and not row.get("lost"):
            self._finished_logs(vm, b.id, key[1])
            with self.lock:
                if (getattr(self, "handing_off", False) or b.closed or b.admission_closed or it.owned.closed
                        or vm.running.get(key) is not it):
                    return False
                identity = it.launch_identity
            # A terminal payload is not namespace teardown. Ask the existing bound
            # native barrier while this worker is still reachable, before ACK frees
            # the placement and shrink may release its VM. Failure is not proof and
            # must not suppress or rewrite an otherwise valid payload result.
            try:
                receipt = vm.client.rpc("cancelitem", b.id, key[1], 1, identity, timeout=3)
            except Exception:
                pass
        with self.lock:
            if (getattr(self, "handing_off", False) or b.closed or b.admission_closed or it.owned.closed
                    or vm.running.get(key) is not it):
                return False
            if not self._result_identity_matches(it, row):
                return False
            if row.get("preempted") or row.get("lost"):
                self._owned_failed(it, "strict_once: preempted or lost placement")
            else:
                if self._matching_receipt(receipt, b.id, key[1], it.launch_identity):
                    # Bind the observed RPC worker, not a relabelled copy of the
                    # expected placement. Unknown/changed boot or VM is not proof.
                    evidence = {"phase": "terminal", "name": it.name, "matched": True,
                                "run_name": key[1], "vm": vm.id, "boot_id": vm.lease.get("boot"),
                                "attempt": it.owned.placements, "identity": it.launch_identity,
                                "receipt": {k: receipt[k] for k in
                                            ("ok", "reaped", "namespace_absent", "identity", "batch", "name")},
                                "t": time.time()}
                    if self._matching_terminal_reap(it, evidence):
                        self._append_owned(b.id, "cancel", evidence)  # fsync before result / ACK
                        it.terminal_reap = evidence
                done = {**{k: v for k, v in row.items() if k != "batch"}, "name": it.name, "vm": vm.id, "run_name": it.run_name,
                        "placement": {**{k: (it.last_placement or {})[k] for k in ("distinct_vm_group", "boot_id", "distinct_vm_group_scope", "distinct_vm_group_generation", "attempt", "agent_version", "agent_capabilities") if k in (it.last_placement or {})},
                                      "vm": vm.id, "type": vm.type, "cpu": (it.last_placement or {}).get("cpu") or vm.model, "vcpus": it.weight * boat.SLOT_CPU, "vm_vcpus": boat.TYPES[vm.type][0],
                                      "exclusive": it.timing, "co_tenants": it.peers}}
                # Journal before releasing the placement, and under the same lock
                # as cancellation. Never expose a done-without-row window.
                self._deliver_owned(b, done)
                it.state = "done"
                it.fate.finished()
                b.slot_s += max(0.0, float(row.get("seconds", 0) or 0)) * it.weight
                if float(row.get("seconds", -1) or -1) > 0:
                    self._learn(it, float(row["seconds"]))
                    b.obs_s, b.obs_est, b.obs_n = getattr(b, "obs_s", 0.0) + float(row["seconds"]), getattr(b, "obs_est", 0.0) + it.est, getattr(b, "obs_n", 0) + 1
            vm.running.pop(key, None)
            if it.t_start:
                vm.busy_s += time.time() - it.t_start
            vm.idle_since = time.time()
        self.wake.set()
        return True

    def _boat_failed(self, it: Item, vm_id: str | None, why: str, strike: str | None = None):
        """One of boat's failures of an item (a VM lost or stopped, its row missing, its preparation failed, its VM not
        reached after a restart; or, as `strike`, a wait in which boat failed a start of its image): queued again, never
        on a VM that failed it, until boat has failed it BOAT_TRIES times; then its row says lost (its client runs it
        once on Modal, or reports it failed). The decision is rules.ItemFate's (one strike per VM, charged only to an
        item last placed on that VM). Under self.lock."""
        if it.batch.admission_closed or it.owned.closed:
            if it.state != "done":
                it.state = "cancel_pending"
            return
        if it.batch.strict_once:
            self._owned_failed(it, why)
            return
        n = NAMES[it.batch.klass]
        if strike is None:
            strike = it.fate.boat_failed(vm_id) if vm_id else "none"
        if strike == "none":  # (one VM's trouble is one strike for an item, however many ways it shows)
            if it.state != "done" and it.fate.state == "queued":  # (its fate the word: a stale report of a VM it has left
                it.state, it.vm, it.head = "queued", None, True  # leaves it where it is now; B13: queued, its fate placed, never placed)
            return
        self.m["boat_fails"][n] = self.m["boat_fails"].get(n, 0) + 1
        self._h().record_strike(time.time(), f"{it.batch.id}/{it.name}", vm_id, why)
        if strike == "again":
            it.state, it.vm, it.t_queued, it.head = "queued", None, time.time(), True
            log(f"{it.batch.id}: {it.name}: boat failed it ({it.fails} of {BOAT_TRIES}): {why}; again on boat")
            return
        it.state = "done"
        if durable_item(it) or required_agent_capabilities(it.spec):
            self._owned_failed(it, f"boat failed it {it.fails} times (last: {why}); no fallback")
            return
        self.m["to_modal"][n] = self.m["to_modal"].get(n, 0) + 1
        self._h().record_modal(time.time(), f"{it.batch.id}/{it.name}", it.fails, len(it.avoid), BOAT_TRIES)
        log(f"{it.batch.id}: {it.name}: boat failed it {it.fails} times ({why}): back to its client" + (" for Modal" if it.batch.on_lost else ""))
        threading.Thread(target=it.batch.deliver, args=({"name": it.name, "code": -1, "seconds": -1, "lost": True, "boat_fails": it.fails,
                                                         "error": f"boat failed it {it.fails} times (last: {why})"},), daemon=True).start()

    def _lost_vm(self, vm: VMState):
        if vm.dead:
            return
        log(f"{vm.id}: lost")
        with self.lock:
            vm.dead = True
            # (rules.slept_through, ItemFate.vm_lost: a VM boat stopped while this machine slept is no strike)
            slept = rules.slept_through(time.time(), getattr(self, "t_wake", 0), 900)
            for key, it in list(vm.running.items()):
                if it.batch.admission_closed or it.owned.closed:
                    if it.state != "done":
                        it.state = "cancel_pending"
                    continue
                vm.running.pop(key)
                strike = it.fate.vm_lost(vm.id, slept)
                if slept and requeue_after_loss(it.batch.strict_once):
                    it.state, it.vm, it.head = "queued", None, True
                else:
                    self._boat_failed(it, vm.id, f"boat VM {vm.id} lost", strike)
        self._let_go(vm, stop=True)
        self.wake.set()

    def _release_idle(self):
        """The pool shrinks, damped: the floor (the treadmill's hint) stays while there's work for it; a VM above it
        runs filler for HOLD s after its last real work (real work takes its slots at once), then drains (its filler
        killed and queued again, no new filler) and is released. A VM with nothing to run is released at once: busy,
        never parked (boat's TOS-6). Released VMs stop IDLE (60) s later unless another batch leases them first.
        During this candidate's pending canary, voluntary holds/drains wait for its verdict; explicit retirement and
        disk replacement still run, and real-work preemption is unaffected."""
        if getattr(self, "recovery_blocked", False):
            return
        c = getattr(self, "canary", None)
        verdict = c.read() if c is not None else {}
        allow_retirement = rules.voluntary_retirement_allowed(verdict.get("state") == "watching",
                                                               verdict.get("to") == getattr(self, "code", None))
        now = time.time()
        with self.lock:
            if getattr(self, "handing_off", False) or not self._handoff_gate().placement_allowed():
                return
            q = self._pass_queue()
            q_classes: dict = {}
            for it in q:
                q_classes.setdefault(it.batch.image, set()).add(it.batch.klass)
            for image in {v.image for v in self.vms.values()}:
                vs = [v for v in self.vms.values() if v.image == image and v.ready and not v.dead]
                for v in vs:
                    if any(x.batch.klass <= 4 for x in v.running.values()):
                        v.t_real = now
                vs.sort(key=lambda v: -v.t_real)
                above = vs[self.floor(image):]
                drains = self._drain(image)
                for v in vs:  # (each VM's hold: rules.VMLife.hold_tick; past it, its drain on its turn: rules.Drain.turn)
                    life = self._life(v, now)
                    real = any(x.batch.klass <= 4 for x in v.running.values())
                    rq = bool(q_classes.get(image, set()) - {5})  # (real work of the image queued: no drain)
                    ripe = allow_retirement and v in above and not v.draining and not real and not rq and now - v.t_real > HOLD
                    drain = life.hold_tick(now, int(self.pol.get("generation", 0)), real, v in above, ripe and drains.turn(now, len(above)), rq)
                    v.t_real, v.draining, v.gen = life.t_real, life.draining, life.gen
                    if drain:
                        self.m["hold_ends"] += 1
                        fill = [x for x in v.running.values() if x.batch.klass == 5 and x.state == "running"]
                        log(f"{v.id}: {HOLD / 60:.0f} min on filler only, above the floor of {self.floor(image)}: released ({len(fill)} filler items queued again)")
                        for x in fill:
                            x.state = "preempting"
                            threading.Thread(target=self._kill, args=(v, x), daemon=True).start()
            # (released: rules.VMLife.release, nothing running on it and nothing of its image queued, or draining with
            # only filler queued; within the floor not while the treadmill is alive (but idle for HOLD s); above it on
            # its turn of the staged drain)
            retired = self.retired()
            go = [v for v in self.vms.values() if v.id in retired and v.ready and not v.dead and not v.running and not getattr(v, "foreign", 0)]  # (retired, empty: nothing of ours nor anyone's runs there)
            for v in go:
                v.why_go = "retired, empty"
            full = [v for v in self.vms.values() if v not in go and v.ready and not v.dead and not v.starts
                    and rules.disk_replaced(v.disk_low, not v.running and not getattr(v, "foreign", 0), int(now - v.t_low) if v.disk_low else 0, DISK_GRACE_S)]
            for v in full:  # (rules.disk_replaced: its disk still low past its agent's cleaning, empty: deleted, the next start a new VM)
                v.why_go, v.replace = f"its disk stays full ({v.disk.get('free_mb', 0) / 1024:.1f} GB free of {v.disk.get('total_mb', 0) / 1024:.0f}): deleted, replaced by a new VM", True
            go += full
            if go:
                log(f"retired, empty: let go {[v.id for v in go]}")
            for image in {v.image for v in self.vms.values()}:
                vs = sorted([v for v in self.vms.values() if v.image == image and v.ready and not v.dead], key=lambda v: -v.t_real)
                tm = FLOOR_ALWAYS or self.treadmill_alive(image)
                n_floor = self.floor(image)
                held = n_floor > 0  # (a treadmill's floor, or the default floor of an image with recent work)
                for i, v in enumerate(vs):
                    if not allow_retirement or v.starts or now - v.idle_since <= 3 or v in go:
                        continue
                    life, within = self._life(v, now, hold=None if tm else max(HOLD, FLOOR_IDLE)), i < n_floor
                    rq = bool(q_classes.get(image, set()) - {5})
                    fq = rules.filler_coming(5 in q_classes.get(image, set()), tm, now - v.t_real, HOLD)  # (a live treadmill's filler is seconds away)
                    if not life.releasable(rq, fq, within, held, now - v.idle_since):
                        continue
                    turn = within or v.draining or self._drain(image).turn(now, len(vs) - n_floor)
                    if life.release(now, rq, fq, within, held, now - v.idle_since, turn):
                        go.append(v)
                        v.why_go = (f"{'within the floor' if within else 'above the floor'} of {n_floor}, {'draining past its hold' if v.draining else 'nothing to run'}"
                                    f" (idle {now - v.idle_since:.0f} s, last real work {now - v.t_real:.0f} s ago; {'filler' if fq else 'nothing'} queued, treadmill {'alive' if tm else 'none'})")
            for v in go:
                v.ready = False
                # Adoption must not revive our old lease while release I/O is in flight.
                # Keep it out of self.vms so it is not promised as incoming capacity.
                self.__dict__.setdefault("_releasing", set()).add(v.id)
                self.vms.pop(v.id, None)
        def finish_release(v):
            try:
                self._let_go(v)
            finally:
                with self.lock:
                    self._releasing.discard(v.id)
        for v in go:
            threading.Thread(target=finish_release, args=(v,), daemon=True).start()

    def _let_go(self, vm: VMState, stop: bool = False, keep: bool = False):
        if getattr(self, "recovery_blocked", False):
            return
        if not vm.lease:
            self._drop_stale(vm, source='release.missing_fence')
            return
        ttl = max(boat.BACKSTOP, vm.ttl) if keep else boat.IDLE
        try:
            if keep:
                if not self._renew_lease(vm):
                    return
                boat.boat("extend", vm.id, "--ttl", str(ttl), "--json", check=False)
                # Same-machine handoff retains its epoch and running jobs.
            elif getattr(vm, "replace", False):
                if not boat.delete_vm(vm.id, lease=vm.lease):
                    self._drop_stale(vm, source='release.delete_refused')
                    return
            elif stop:
                if not boat.stop_vm(vm.id, lease=vm.lease):
                    self._drop_stale(vm, source='release.stop_refused')
                    return
            else:
                # The helper reserves provider capacity before taking the stop
                # fence, so queueing cannot outlive STOP_S and shrink a new holder.
                if not boat.shrink_ttl(vm.id, ttl, vm.lease):
                    self._drop_stale(vm, source='release.shrink_refused')
                    return
        except Exception as e:
            log(f"{vm.id}: release not confirmed; left to TTL: {str(e)[:160]}")
            self._drop_stale(vm, source='release.exception', error=e)
            return
        now = time.time()
        with Registry().locked() as d:
            v = d["vms"].get(vm.id)
            if v and (v.get("lease") or {}).get("pid") == os.getpid():
                if getattr(vm, "replace", False):
                    d["vms"].pop(vm.id, None)
                else:
                    v["lease"], v["last_used"] = None, now
                    v["until"] = 0 if stop else now + ttl
                    v["released"], v["owner"] = now, None
        with self.lock:
            if self.vms.get(vm.id) is vm:
                self.vms.pop(vm.id, None)

    # -- the breaker, the watchdog, the day's accounts
    def _breaker_enabled(self) -> bool:
        """Start throttling is an explicit opt-in; per-worker correctness guards are independent."""
        return getattr(self, "pol", {}).get("start_breaker_enabled") is True

    def _breaker_rejudge_evidence(self, now: float) -> dict:
        """Retain historical evidence if incomplete; quarantined workers never count as a current outage."""
        quarantine = getattr(self, "resume_quarantine", {})
        if not self._breaker_enabled():
            return {"enabled": False, "known": True, "judged": 0, "bad": 0, "outage": False,
                    "minimum": max(3, BREAK_MIN), "historical": None, "quarantined": sorted(quarantine)}
        def judged(rows, at):
            return [s for s in rows if s.get("ok") is not None or at - s["t"] > BREAK_READY]
        def counts(rows):
            eligible = [s for s in rows if s.get("vm") not in quarantine]
            return len(eligible), sum(not s.get("ok") for s in eligible)
        current = judged([s for s in self.starts_log if 0 <= now - s["t"] < BREAK_WINDOW], now)
        n, bad = counts(current)
        saved = self.br.get("evidence", {})
        known, historical, outage = False, None, self._outage()
        reason = self.br.get("reason", "")
        match = re.fullmatch(r"(\d+) of the last (\d+) starts not ready within \d+ min", reason)
        if saved.get("kind") == "readiness" and isinstance(saved.get("starts"), list):
            historical, known = counts(saved["starts"]), True
        elif match:
            old_bad, old_n = map(int, match.groups())
            if 0 <= old_bad <= old_n:
                known, historical = True, (old_n, old_bad)
                at = self.br["t_trip"]
                rows = judged([s for s in self.starts_log if 0 <= at - s["t"] < BREAK_WINDOW], at)
                # A partial/expired journal must not erase a genuine multi-start trip.
                if (len(rows), sum(not s.get("ok") for s in rows)) == historical:
                    historical = counts(rows)
        else:
            front = re.match(r"boat's front: the agents of (\d+) of (\d+) (?:eligible )?VMs don't answer", reason)
            if front:
                failed, total = map(int, front.groups())
                if 0 <= failed <= total:
                    known, outage = True, outage or rules.outage(total, failed)
        need = max(3, BREAK_MIN)
        if historical and historical[0] >= need and 2 * historical[1] > historical[0]:
            n, bad = historical
        return {"enabled": True, "known": known, "judged": n, "bad": bad, "outage": outage,
                "minimum": need, "historical": historical, "quarantined": sorted(quarantine)}

    def rejudge_breaker(self, *, source: str = "operator", actor: str = "operator", reason: str = "") -> dict:
        """One audited policy decision for both startup/handoff and the authenticated operator RPC."""
        if source not in ("operator", "startup") or not isinstance(reason, str) or not reason.strip() or len(reason) > 512:
            raise ValueError("a nonempty audit reason (at most 512 characters) is required")
        if not isinstance(actor, str) or not actor.strip() or len(actor) > 128:
            raise ValueError("a nonempty audit actor (at most 128 characters) is required")
        with self.lock:
            if source == "operator" and (getattr(self, "handing_off", False) or getattr(self, "recovery_blocked", False)):
                raise ValueError("controller ownership is not stable")
            now, before = time.time(), copy.deepcopy(self.br)
            evidence = self._breaker_rejudge_evidence(now)
            b = self._breaker()
            decision = b.rejudge(now, evidence["judged"], evidence["bad"], evidence["outage"], evidence["known"])
            after = {**before, "open_until": b.open_until, "half": b.half, "backoff": b.backoff}
            if decision == "closed":
                after["reason"] = ""
            record = {"protocol": 1, "t": now, "source": source, "actor": actor, "reason": reason,
                      "code": getattr(self, "code", ""), "pid": os.getpid(), "decision": decision,
                      "inherited": before, "evidence": evidence, "after": after}
            # Fail before touching live state if the durable audit cannot be written.
            audit = DIR / "breaker-rejudge.jsonl"
            with config.private_umask():
                with audit.open("a") as f:
                    f.write(json.dumps(record, sort_keys=True) + "\n")
                    f.flush()
                    os.fsync(f.fileno())
            self.br.update(after)
            self._metrics_save()
            log("breaker rejudge: " + json.dumps(record, sort_keys=True))
        if decision == "closed":
            self.wake.set()
        if hasattr(self, "_status_view"):
            self._refresh_status(blocking=True)
        return {"ok": True, **record, "breaker": {**after, "open": now < after["open_until"]}}

    def trip(self, reason: str, *, evidence: dict | None = None):
        """The circuit breaker opens: no start (free or paid) for its backoff, doubled at each trip in a row; then one
        start as a probe, and starts again only once a probe came up."""
        if not self._breaker_enabled():
            return
        now = time.time()
        with self.lock:
            b = self._breaker()
            pause = b.backoff
            if b.trip(now) != "tripped":  # (rules.Breaker.trip: none while open)
                return
            self.br.update(open_until=b.open_until, t_trip=now, half=b.half, reason=reason, trips=b.trips, backoff=b.backoff, last_reason=reason,
                           evidence=copy.deepcopy(evidence or {}),
                           history=(self.br.get("history", []) + [{"t": round(now, 1), "reason": reason}])[-20:])  # (every trip's why, kept past its close)
            self.m["breaker_trips"] += 1
        self.alert(f"circuit breaker open for {pause / 60:.0f} min: {reason}")
        self._metrics_save()

    def _breaker_eval(self, now: float):
        if not self._breaker_enabled():
            return
        recent = [s for s in self.starts_log if s["t"] > self.br["t_trip"] and now - s["t"] < BREAK_WINDOW
                  and s.get("vm") not in getattr(self, "resume_quarantine", {})]
        judged = [s for s in recent if s["ok"] is not None or now - s["t"] > BREAK_READY]
        bad = [s for s in judged if not s["ok"]]
        b = self._breaker()
        what = b.judge(now, len(judged), len(bad), any(s["ok"] for s in recent))  # (rules.Breaker.judge: over half of enough starts)
        if what == "closed":
            with self.lock:
                self.br.update(half=b.half, backoff=b.backoff, reason="")
            log("circuit breaker closed: a probe start came up")
        elif what == "tripped":
            self.trip(f"{len(bad)} of the last {len(judged)} starts not ready within {BREAK_READY / 60:.0f} min",
                      evidence={"kind": "readiness", "starts": copy.deepcopy(judged)})

    def _breaker(self) -> rules.Breaker:
        """The breaker as rules.Breaker holds it (its state kept in self.br, across restarts)."""
        b = rules.Breaker(int(BREAK_BACKOFF), int(BREAK_MAX), BREAK_MIN)
        b.open_until, b.half = float(self.br["open_until"]), bool(self.br["half"])
        b.backoff, b.trips = min(int(BREAK_MAX), max(int(BREAK_BACKOFF), int(self.br["backoff"]))), int(self.br.get("trips", 0))
        return b

    def _watch(self):
        """The controller's own load: past SHED_LOAD per CPU, or under 15% of its memory free, it sheds (no starts,
        the controller's own sends one at a time, slower ticks) until the load is back under one per CPU."""
        ncpu = os.cpu_count() or 1
        l1 = os.getloadavg()[0]
        mem = None
        with contextlib.suppress(Exception):
            mi = {l.split(":")[0]: int(l.split()[1]) for l in open("/proc/meminfo")}
            mem = mi["MemAvailable"] / mi["MemTotal"]
        rss = None
        with contextlib.suppress(Exception):
            rss = next(int(l.split()[1]) // 1024 for l in open("/proc/self/status") if l.startswith("VmRSS"))
        now = time.time()
        if not hasattr(self, "_memory_sampler"):
            self._memory_sampler = memorypressure.Sampler()
        memory_sample = self._memory_sampler.sample()
        self._h().record_memory(now, memory_sample)
        for message in self._h().memory_tick(now) or []:
            self.alert(message)
        self.load = {"memory": memory_sample, "load1": round(l1, 2), "cpus": ncpu, "mem_free": round(mem, 2) if mem is not None else None, "rss_mb": rss, "threads": threading.active_count(),
                     "threads_by": dict(collections.Counter(re.sub(r"^Thread-\d+ \((.*)\)$", r"\1", re.sub(r"-\d+$", "", t.name)) for t in threading.enumerate()).most_common(12)),
                     "cpu_s": {k: round(v, 2) for k, v in sorted(self.__dict__.get("_cpu", {}).items(), key=lambda kv: -kv[1])[:16]},
                     "cpu_n": dict(self.__dict__.get("_cpu_n", {})), "cpu_total_s": round(_proc_cpu(), 2), "up_s": round(time.time() - getattr(self, "t_up", time.time()), 1)}
        if not WATCHDOG:
            return
        wd = rules.Watchdog(int(SHED_LOAD * 1000))  # (rules.Watchdog: past SHED_LOAD per CPU or 15% memory; back under 1 and 25%)
        wd.shed = self.shed
        shed = wd.sample(int(1000 * l1 / ncpu), int(100 * mem) if mem is not None else 100)
        if shed and not self.shed:
            self.shed = True
            self.alert(f"the controller is loaded (load {l1:.1f} on {ncpu} CPUs, {100 * (mem or 0):.0f}% memory free, {threading.active_count()} threads): starts paused")
        elif not shed and self.shed:
            self.shed = False
            log(f"the controller's load is back (load {l1:.1f}): starts again")

    def _waste(self, now: float, live: list, grace: float | None = None, idle_only: bool = False, by_vm: dict | None = None) -> dict:
        """Per image, the slots held by filler (or a sweep, against a gate) or idle while real work (interactive, gate,
        submission, sweep) that could take them has waited past SUB_PREEMPT (health's waste: want 0). A waiter counts when it
        isn't backing off, a submission within its share (or uncontended), and the slots fit it on a VM it may take:
        its image and type, a VM that hasn't failed it, not quarantined nor retired, past the slots set aside for
        another waiting item, a timing item's VM with nothing of its class or higher. Each such slot is counted once,
        given to the waiters in class order (11-01 13:25-13:40: 60 slots idle on VMs set aside for items that had run
        elsewhere, counted busy, while 1,500 gate items waited)."""
        retired = self.retired()
        cap: dict = {}  # vm id -> [idle, filler, sweep] slots it could give
        for v in live:
            if not v.ready or v.dead or v.id in retired or getattr(v, "prep_fails", 0) >= 3 or v.failing or getattr(v, "_fails", 0):
                continue  # (a VM whose agent doesn't answer: nothing can be placed there till it does)
            held = self._held(v, None, now)  # (slots set aside for a waiting item: its idle ones first, then the filler
            free = max(0, v.slots - v.used())  # and sweeps it is emptying for that item: none of them another's to have;
            idle = max(0, free - held)  # 00:50:39, an xlarge's filler counted beside a submission, set aside for a gate)
            over = max(0, held - free)
            fil = 0 if idle_only else sum(x.weight for x in v.running.values() if x.batch.klass == 5 and x.state == "running")
            swp = 0 if idle_only else sum(x.weight for x in v.running.values() if x.batch.klass == 4 and x.state == "running")
            fil, over = max(0, fil - over), max(0, over - fil)
            swp = max(0, swp - over)
            cap[v.id] = [idle, fil, swp]
        groups: dict = {}
        contended = getattr(self, "_contended", None)
        sub_run: dict = {}
        for v in live:
            for x in v.running.values():
                if x.batch.klass == 3:
                    sub_run[x.batch.client] = sub_run.get(x.batch.client, 0) + x.weight
        pend = getattr(self, "reattach_pending", None) or set()
        for b in self.batches.values():
            if b.klass > 4 or b.closed:  # (sweeps too: real work above filler, the treadmill's frontier; the coordinator, 11-01 20:20)
                continue
            for it in b.items.values():
                if it.state != "queued" or now - it.t_queued < (SUB_PREEMPT if grace is None else grace) or now < it.not_before:
                    continue
                if pend and getattr(it, "reattach", None) and it.reattach.get("vm") in pend:
                    continue  # (re-attach is still to take it back where it runs: not a waiter, B10)
                if b.klass == 3 and contended is not None and not rules.within_share(3, sub_run.get(b.client, 0), it.weight, SUB_VMS * vm_slots(b.vm_type), contended(it)):
                    continue
                key = fit_shape(it)
                groups[key] = groups.get(key, 0) + 1
        out: dict = {}
        by_id = {v.id: v for v in live}
        for shape, n in sorted(groups.items(), key=lambda g: g[0][1]):
            image, k, timing, weight, vtype, avoid, mem, model, used, required = shape
            for vid, c in cap.items():
                if n <= 0:
                    break
                v = by_id[vid]
                if not vm_fits(shape, v, retired) or v.slots < weight:  # (the one fit, as placement reads it; and its slots:
                    continue  # 00:36:10, a 1-slot VM counted beside a 2-slot timing item)
                if timing:
                    if any(x.batch.klass <= k for x in v.running.values()) or self._held(v, None, now) or sum(c) < v.slots:
                        continue
                    took = [c[0], c[1], c[2] if k <= 2 else 0]
                    if sum(took) < v.slots:
                        continue
                    out[image] = out.get(image, 0) + v.slots
                    if by_vm is not None:
                        by_vm[vid] = (image, by_vm.get(vid, (image, 0, ""))[1] + v.slots, f"{NAMES[k]}/timing w{weight}" + (f" {mem} MB" if mem else ""))
                    cap[vid] = [0, 0, 0]
                    n -= 1
                    continue
                avail = c[0] + c[1] + (c[2] if k <= 2 else 0)
                m = min(n, avail // weight, 1) if used is not None else min(n, avail // weight)
                if m <= 0:
                    continue
                need = m * weight
                out[image] = out.get(image, 0) + need
                if by_vm is not None:
                    by_vm[vid] = (image, by_vm.get(vid, (image, 0, ""))[1] + need, f"{NAMES[k]} w{weight}" + (f" {mem} MB" if mem else ""))
                for i in (0, 1, 2) if k <= 2 else (0, 1):
                    d = min(c[i], need)
                    c[i] -= d
                    need -= d
                n -= m
        return out

    def _waste_held(self, key: str, now: float, live: list, **kw) -> dict:
        """Per image, the slots _waste counts on a VM now and at the last measure under `key` too (the lesser of the
        two): a slot held on filler or idle for a measure's interval beside waiting real work, not one an item freed
        an instant before the next pass fills it (16:06-16:10: 1-2 slots freed between passes, ~40 items a second
        ending on 600 slots, counted as waste and a handoff's idle, the canary handing back for 2 of them)."""
        cur: dict = {}
        self._waste(now, live, by_vm=cur, **kw)
        prev = self.__dict__.setdefault("_waste_prev", {}).get(key, {})
        t_prev = self.__dict__.setdefault("_waste_t", {}).get(key, now)
        self._waste_prev[key], self._waste_t[key] = cur, now
        out: dict = {}
        for vid, (image, n, why) in cur.items():
            if vid in prev:
                v0 = self.vms.get(vid)
                k = rules.waste_held(n, prev[vid][1], bool(v0) and getattr(v0, "t_placed_real", 0) > t_prev)
                out[image] = out.get(image, 0) + k
                if k and now - getattr(self, "_t_waste_log", 0) > 300:  # (what waited and where: once in 5 min)
                    self._t_waste_log = now
                    v = self.vms.get(vid)
                    log(f"waste: {k} slots of {vid} ({v.type if v else '?'}, {[NAMES[x.batch.klass] for x in v.running.values()] if v else []}) on filler or idle a measure long beside {why} waiting")
        return out

    def _sampler(self):
        """Every 5 s: the watchdog and the breaker; every 15 s the day's accounts (VM-hours, those on filler above the
        floor, waves of real work and the gaps between them), saved every minute."""
        t_acc, t_save, t_adopt = time.time(), time.time(), 0.0
        while not self.stop.wait(5):
            if getattr(self, "handing_off", False):
                continue
            now = time.time()
            self._flush_status_timings()
            with contextlib.suppress(Exception):
                # (a new install, unchanged for 60 s, that imports: rules.Install; no handoff while files are being
                # written, none of a broken install)
                back = self.canary.handback_due(self.code, now)
                if back:
                    self.handoff(back_to=back)
                elif pending := getattr(self, "_handoff_pending", None):
                    if pending["back_to"] or code_hash() == pending["target"]:
                        self.handoff(back_to=pending["back_to"])
                    else:
                        # A changed/withdrawn install must earn stability anew.
                        with self.lock:
                            self._clear_handoff_pending(pending["generation"])
                else:
                    h = code_hash()
                    inst = self.__dict__.setdefault("_install", rules.Install(self.code, 60))
                    ripe = h != self.code and inst.seen == h and now - inst.t_seen >= inst.stable_s and self.canary.retry_ready(h, now)
                    if inst.tick(now, h, ripe and self._imports(h)) == "handoff":
                        self.handoff()
            if now - t_adopt > 60 and not getattr(self, "adopting", False):
                t_adopt = now
                self.adopting = True
                threading.Thread(target=self._adopt, daemon=True).start()
            with contextlib.suppress(Exception):
                self._watch()
            try:  # (a tick and two after a take-over: what it held, orphans, idle slots; the canary's word at once)
                self._handoff_check(now)
            except Exception as e:
                self._h().record_error(now, f"handoff check: {e!r}")
            with contextlib.suppress(Exception):
                self._breaker_eval(now)
            if now - t_acc < 15:
                continue
            dt, t_acc = now - t_acc, now
            with contextlib.suppress(Exception):
                day = time.strftime("%Y-%m-%d", time.gmtime())
                with self.lock:
                    if day != self.m["day"]:
                        self._metrics_save()
                        self.m = self._metrics_new(day)
                        self.m["waves"] = int(self.wave["on"])  # (a wave going on at midnight is the new day's too: 0 waves all day under one)
                    live = [v for v in self.vms.values() if not v.dead]
                    self.m["peak_vms"] = max(self.m["peak_vms"], len(live))
                    for v in live:
                        h = vm_mult(v.type) * dt / 3600
                        self.m["vmh"] += h
                        cls = min([x.batch.klass for x in v.running.values()] or [0])
                        key = NAMES.get(cls, "idle")
                        self.m["vmh_by_class"][key] = self.m["vmh_by_class"].get(key, 0.0) + h
                    for image in {v.image for v in live}:
                        vs = sorted([v for v in live if v.image == image], key=lambda v: -v.t_real)
                        for v in vs[self.floor(image):]:
                            if v.running and all(x.batch.klass == 5 for x in v.running.values()):
                                self.m["vmh_filler_hold"] += vm_mult(v.type) * dt / 3600
                    per = {}
                    for v in live:
                        if v.ready:
                            p = per.setdefault(v.image, {"slots": 0, "busy": 0, "queued": 0, "stray": 0})
                            p["slots"] += v.slots
                            p["stray"] += min(v.slots, getattr(v, "stray", 0))  # (slots held by runs no item owns past a kill's retry: a leak, not the lag of a kill landing)
                            if getattr(v, "disk_low", False):  # (health's disk: VMs held below rules.disk_ok's threshold)
                                p.setdefault("disk_low", []).append([v.id, round(v.disk.get("free_mb", 0) / 1024, 1)])
                            p["busy"] += v.slots if any(x.timing and x.batch.klass <= 4 for x in v.running.values()) else sum(x.weight for x in v.running.values() if x.batch.klass <= 4)
                            held = min(self._held(v, None, now), max(0, v.slots - v.used()))  # (slots held empty for a waiting
                            if held:  # wide or timing item: busy by design, only while that item waits, only its need)
                                p["busy"] += held
                                p["exclusive"] = p.get("exclusive", 0) + held
                    contended, sub_held = getattr(self, "_contended", None), {}
                    for v in live:
                        for x in v.running.values():
                            if x.batch.klass == 3:
                                sub_held[x.batch.client] = sub_held.get(x.batch.client, 0) + x.weight
                    for b in self.batches.values():
                        if b.klass <= 4 and b.image in per:
                            for it in b.items.values():
                                if it.state == "queued":
                                    per[b.image]["queued"] += 1  # (and the slots it needs: busy's demand, health)
                                    share_ok = b.klass != 3 or contended is None or rules.within_share(3, sub_held.get(b.client, 0), it.weight, SUB_VMS * vm_slots(b.vm_type), contended(it))
                                    per[b.image]["qslots"] = per[b.image].get("qslots", 0) + rules.busy_demand(b.klass, now - it.t_queued, SUB_PREEMPT, vm_slots(b.vm_type) if it.timing else it.weight, share_ok)
                                    qb = per[b.image].setdefault("q_by", {})  # (the queue's make-up by class: busy's demand read)
                                    qb[NAMES[b.klass]] = qb.get(NAMES[b.klass], 0) + (vm_slots(b.vm_type) if it.timing else it.weight)
                    try:  # (a fault in the measure never costs the sample)
                        for image, w in self._waste_held("sample", now, live).items():
                            if image in per:
                                per[image]["waste"] = w
                    except Exception as e:
                        self._h().record_error(now, f"waste: {e!r}")
                    self._h().record_sample(now, per)
                    real = any(it.state != "done" and b.klass <= 3 for b in self.batches.values() for it in b.items.values())
                    if real and not self.wave["on"]:
                        if self.wave.get("t_end"):
                            self.m["gaps"] = (self.m["gaps"] + [round(now - self.wave["t_end"])])[-500:]
                        self.wave.update(on=True, t=now)
                        self.m["waves"] += 1
                    elif not real and self.wave["on"]:
                        self.wave.update(on=False, t_end=now)
            if now - t_save > 60:
                t_save = now
                with contextlib.suppress(Exception):  # (the targets checked every minute: an alert after 2 checks in a row breached)
                    self._h().prune(now)
                    for a in self._h().tick(now, 30) or []:
                        self.alert(a)
                threading.Thread(target=self._billing, daemon=True).start()  # (boat's billing, read once a minute: health's billing target)
                with contextlib.suppress(Exception):  # (a canary: new code that breaks a target goes back to the previous)
                    what, why = self.canary.tick(self.code, now, self._h())
                    if what == "passed":
                        log(f"canary: {why}")
                    elif what == "handback":
                        self.alert(f"canary: {why}")
                        self.handoff(back_to=self.canary.read().get("from"))
                self._metrics_save()

    def rollback(self, code: str | None = None) -> dict:
        """supercomputer rollback: the canary's own hand-back, asked for (to `code`, else the newest whose canary passed);
        every VM and item kept; the install left as it is, this code rejected until another install."""
        to, why = self.canary.rollback_to(self.code, code)
        if not to:
            return {"ok": False, "error": why}
        self.alert(f"rollback: code {self.code} -> {to} (asked for): handing the pool back")
        threading.Thread(target=lambda: (time.sleep(1), self.handoff(back_to=to)), daemon=True).start()
        return {"ok": True, "from": self.code, "to": to}

    def _billing(self):
        with contextlib.suppress(Exception):
            self._h().record_billing(time.time(), health.billing_problems(boat.limits()))

    def _reject_handoff_candidate(self, proc, previous_leases: dict):
        """Reap before reclaiming its local records; never steal a foreign lease."""
        if proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.kill(proc.pid, 9)
        proc.wait(timeout=5)
        with Registry().locked() as d:
            for vid, entry in d["vms"].items():
                lease, before = entry.get("lease") or {}, previous_leases.get(vid)
                ours_before = (before or {}).get("pid") == os.getpid() and (before or {}).get("host") == socket.gethostname()
                child = (lease.get("pid") == proc.pid and lease.get("host") == socket.gethostname()
                         and lease.get("batch") == "scheduler")
                if child or (not lease and ours_before and vid in self.vms):
                    entry["lease"] = before

    def _resume_handoff(self, previous_files: dict):
        """Restore the old process, not a third successor, after rejected boots."""
        try:
            for name, data in previous_files.items():
                path = DIR / name
                if data is None:
                    path.unlink(missing_ok=True)
                else:
                    tmp = path.with_name(path.name + ".restore")
                    tmp.write_bytes(data)
                    tmp.replace(path)
            tmp = DIR / "pid.restore"
            tmp.write_text(str(os.getpid()))
            tmp.replace(DIR / "pid")
        finally:
            self.handing_off, self.released_lock = False, False
            self._full_now = True
            self.wake.set()
        _notify_service(os.getpid())
        log(f"handoff: old scheduler resumed (pid {os.getpid()}); listener, loops and holds retained")

    def handoff(self, back_to: str | None = None):
        """Transfer the listener and lock, never wait for running jobs to finish.

        A target-specific, five-minute growth fence drains existing acquisition
        and upgrade reservations while ready-worker placement keeps running.
        Only the final strict-start fence pauses placement. Expiry reopens both
        gates without dropping ownership or rejecting otherwise valid code.
        """
        new = back_to or code_hash()
        with self.lock:
            if getattr(self, "_handoff_preparing", False):
                return
            self._handoff_preparing = True
            pending = getattr(self, "_handoff_pending", None)
            if pending and (pending["target"] != new or pending["back_to"] != back_to):
                self._clear_handoff_pending(pending["generation"])
                pending = None
            generation = pending["generation"] if pending else None
        accepted, quiesced, active, keep_pending = False, False, None, False
        previous_files, previous_leases = {}, {}
        try:
            clock = time.monotonic()
            if pending and self._handoff_gate().expired(clock):
                self._handoff_retry = (new, back_to, clock + 60)
                with self.lock:
                    blockers = {"asking": sorted(getattr(self, "asking", ())),
                                "creating": sum(getattr(self, "creating", {}).values()),
                                "booting": sorted(v.id for v in self.vms.values() if not v.dead and not v.ready),
                                "upgrading": sorted(v.id for v in self.vms.values() if getattr(v, "upgrading", False)),
                                "acquiring": getattr(self, "_acquiring", 0),
                                "adoption_claims": getattr(self, "_adopt_claims", 0),
                                "reattaching": bool(getattr(self, "reattaching", False)),
                                "reattach_pending": sorted(getattr(self, "reattach_pending", ())),
                                "strict_starts": [(it.batch.id, it.name) for v in self.vms.values()
                                                  for it in v.running.values()
                                                  if it.batch.strict_once and not it.start_done.is_set()]}
                self.alert(f"handoff to {new}: bounded drain expired; growth and placement resume, retry in 60s; {blockers}")
                return
            retry = getattr(self, "_handoff_retry", None)
            if retry and retry[:2] == (new, back_to) and clock < retry[2]:
                return
            if pending is None:
                if not back_to:
                    if not self._imports(new):
                        return
                    candidate = self.canary.snapshot(new, installed_source() / "supercomputer")
                    if candidate is None or code_hash(candidate) != new:
                        self.alert("handoff refused: candidate source changed while snapshotting")
                        return
                candidates = [new, self.code] if new != self.code else [new]
                if not all((DIR / "code" / c / "supercomputer").is_dir() for c in candidates):
                    self.alert("handoff refused: saved rollback code is missing")
                    return
                # Prepare once per immutable target, before installing its fence.
                try:
                    prepared = {target: prepare_snapshot(DIR / "code" / target / "supercomputer") for target in candidates}
                except (RuntimeError, OSError, subprocess.TimeoutExpired) as error:
                    self.alert(f"handoff refused: snapshot preparation failed: {error}")
                    return
            else:
                candidates, prepared = pending["candidates"], pending["prepared"]
            # Observe provider state while the old scheduler still places work.
            # A failed/partial listing never authorizes pruning an unseen VM.
            observed = {}
            with contextlib.suppress(Exception):
                observed = boat.states(0)
            log(f"code {self.code} -> {new}: handing the pool over (listener, lock, VMs and items retained)")
            # Imports, source copies and provider observation happened while placement continued.
            request_lock = getattr(self, "_request_lock", self.lock)
            with request_lock, self.lock:
                required_runtime = any("runtime" in it.spec
                                       for b in self.batches.values() for it in b.items.values())
                if required_runtime and not all(prepared[target].get("runtime_pins_protocol") is True for target in candidates):
                    self.alert("handoff refused: runtime pins require a runtime-aware successor")
                    return
                required_memory = any(it.spec.get("enforce_memory") is True
                                      for b in self.batches.values() for it in b.items.values())
                if required_memory and not all(prepared[target].get("resource_limits_protocol") is True for target in candidates):
                    self.alert("handoff refused: memory enforcement requires a resource-aware successor")
                    return
                required_agents = any(required_agent_capabilities(it.spec) and it.state != "done"
                                      for b in self.batches.values() for it in b.items.values())
                if required_agents and not all(prepared[target].get("required_agent_capabilities_protocol") for target in candidates):
                    self.alert("handoff refused: live agent requirements need a capability-aware successor")
                    return
                required_cpu = any(it.spec.get("cpu_model") and it.state != "done"
                                   for b in self.batches.values() for it in b.items.values())
                if not all(rules.cpu_handoff_allowed(required_cpu, prepared[target].get("cpu_model_exact") is True) for target in candidates):
                    self.alert("handoff refused: live CPU constraints require an exact-model capable successor")
                    return
                required_distinct = any(it.spec.get("distinct_vm_group") and it.state != "done"
                                        for b in self.batches.values() for it in b.items.values())
                if not all(rules.distinct_history_matches(required_distinct, prepared[target].get("distinct_vm_group") is True) for target in candidates):
                    self.alert("handoff refused: live distinct VM groups require a group-aware successor")
                    return
                required_results = any(getattr(b, 'result_files_protocol', 0) for b in self.batches.values()) or any((DIR / 'queue').glob('*.results'))
                if required_results and not all(prepared[target].get('result_files_protocol') is True for target in candidates):
                    self.alert('handoff refused: result-file admissions require a file-reference-aware successor')
                    return
                required_durable = bool(self._durable_snapshot()) or any(durable_item(it) for b in self.batches.values() for it in b.items.values())
                if not all(rules.distinct_history_matches(required_durable, prepared[target].get("durable_distinct_vm_group") is True) for target in candidates):
                    self.alert("handoff refused: durable distinct VM groups require a durable-group-aware successor")
                    return
                if pending is None:
                    generation = self._handoff_gate().begin(time.monotonic())
                    pending = self._handoff_pending = {"target": new, "back_to": back_to,
                        "generation": generation, "candidates": candidates, "prepared": prepared}
                self._prune_handoff_pool(observed)
                if (not self._pool_settled()
                        or any(getattr(v, "upgrading", False) for v in self.vms.values())):
                    # No new acquisition or upgrade can extend this cohort.
                    keep_pending = True
                    log("handoff pending: draining existing acquisitions, boots and upgrades; ready placement continues")
                    return
                self._handoff_gate().freeze()
                if any(it.batch.strict_once and not it.start_done.is_set()
                       for v in self.vms.values() for it in v.running.values()):
                    keep_pending = True
                    log("handoff pending: final placement fence waits for existing strict launches")
                    return
                previous_files = {name: (DIR / name).read_bytes() if (DIR / name).exists() else None
                                  for name in ("handoff.json", "handoff.used.json", "handoff.ack")}
                registry_started = time.monotonic()
                with Registry().locked() as d:
                    previous_leases = {vid: v.get("lease") for vid, v in d["vms"].items()}
                registry_ms = max(getattr(self, "_registry_load_ms", 0),
                                  math.ceil((time.monotonic() - registry_started) * 1000))
                self._handoff_lease_lost = False
                self.handing_off, quiesced = True, True
                # A reversible pause, not stop: all existing loops stay alive.
                self._metrics_save()
                snap = self._handoff_snapshot()
            if not back_to:
                self.canary.handing_off(self.code, new, time.time())
            inherited = hasattr(self, "_srv") and hasattr(self, "_lockf")
            fds = (self._srv.socket.fileno(), self._lockf.fileno()) if inherited else ()
            # Keep the original accept loop alive for health/status even while a
            # candidate hangs. Both processes reject mutations until committed.
            self.released_lock = True  # simulator selection only; the shared flock stays locked
            held = {v["id"] for v in snap["vms"]}
            item_count = sum(len(v.get("items", [])) for v in snap["vms"])
            load_ms = max(registry_ms, getattr(self, "_reload_ms", 0))
            budget_s = rules.handoff_budget(len(held), item_count, load_ms) / 1000
            snap["readiness_budget"] = {"seconds": budget_s, "vms": len(held),
                                        "items": item_count, "load_ms": load_ms}
            for attempt, target in enumerate(candidates):
                snap["t"] = time.time()
                tmp = DIR / "handoff.json.tmp"
                tmp.write_text(json.dumps(snap))
                tmp.rename(DIR / "handoff.json")
                (DIR / "handoff.ack").unlink(missing_ok=True)
                path = str(DIR / "code" / target)
                env = {**os.environ, "PYTHONPATH": path, "SUPERCOMPUTER_HOME": str(config.HOME),
                       "SUPERCOMPUTER_SRC": str(installed_source())}
                if inherited:
                    env["SUPERCOMPUTER_HANDOFF_FDS"] = ",".join(map(str, fds))
                with open(DIR / "log", "a") as lf:
                    proc = subprocess.Popen([sys.executable, "-P", "-m", "supercomputer", "scheduler", "serve",
                                             "--idle-exit", str(self.idle_exit), "--takeover"],
                                            stdin=subprocess.DEVNULL, stdout=lf, stderr=lf,
                                            start_new_session=True, close_fds=True, pass_fds=fds,
                                            env=env, cwd=str(config.HOME))
                active = proc
                ack, deadline = None, time.monotonic() + budget_s
                log(f"handoff to {target}: readiness budget {budget_s:.3f}s "
                    f"({len(held)} VMs, {item_count} items, measured load {load_ms}ms)")
                while not rules.handoff_expired(time.monotonic(), deadline):
                    with contextlib.suppress(Exception):
                        a = json.loads((DIR / "handoff.ack").read_text())
                        if a.get("of") == snap["t"]:
                            ack = a
                            break
                    if proc.poll() is not None:
                        break
                    time.sleep(0.05)
                if ack and rules.handoff_ready(
                        rules.handoff_pool_ready(
                            held <= set(ack.get("taken", [])) and not (held & set(ack.get("missing", []))),
                            self._handoff_lease_lost),
                        ack.get("admissions", {}).get("ready") is True,
                        ack.get("items_ready") is True and rules.distinct_history_matches(bool(snap.get("distinct_vm_groups")),
                            ack.get("distinct_vm_groups") == snap.get("distinct_vm_groups")) and
                        rules.distinct_history_matches(bool(snap.get("durable_distinct_vm_groups")),
                            ack.get("durable_distinct_vm_groups") == snap.get("durable_distinct_vm_groups"))):
                    _notify_service(ack["pid"])
                    log(f"handoff: serving pid {ack['pid']} holds every VM and running identity; exiting")
                    accepted = True
                    os._exit(0)
                expired = ack is None and rules.handoff_expired(time.monotonic(), deadline)
                # A failed candidate must not run shutdown(), release or race rollback.
                self._reject_handoff_candidate(proc, previous_leases)
                active = None
                retryable = expired or self._handoff_lease_lost
                exit_code = proc.poll()
                why = (f"handoff to {target} readiness deadline after {budget_s:.3f}s (retryable)" if expired
                       else f"handoff to {target} lost lease authority before commit (retryable)" if self._handoff_lease_lost
                       else f"handoff to {target} exited {exit_code} before readiness ACK (stderr: {DIR / 'log'})" if ack is None and exit_code is not None
                       else f"handoff to {target} failed readiness: {ack!r}")
                self.alert(why)
                if target != self.code:
                    self.canary.handoff_failed(target, why, retryable, time.time())
            self.alert("handoff blocked: candidate and saved code failed; resuming old scheduler")
        finally:
            try:
                if rules.handoff_resume(accepted, quiesced):
                    if active is not None:
                        self._reject_handoff_candidate(active, previous_leases)
                    self._resume_handoff(previous_files)
            finally:
                with self.lock:
                    if not keep_pending and generation is not None:
                        self._clear_handoff_pending(generation)
                    self._handoff_preparing = False

    def _imports(self, new: str) -> bool:
        """Whether the installed code imports (a broken install is never handed the pool: an alert, once)."""
        r = subprocess.run([sys.executable, "-c", "import supercomputer.scheduler"], cwd=str(installed_source()), capture_output=True, text=True, timeout=60,
                           env={**os.environ, "PYTHONPATH": str(installed_source())})
        if r.returncode != 0 and getattr(self, "_bad_code", "") != new:
            self._bad_code = new
            self.alert(f"code {new} installed but doesn't import: no handoff ({r.stderr.strip()[-200:]})")
        return r.returncode == 0

    def _adopt(self):
        """Ask the VM, not our registry. Unheld VMs expire by TTL, never by us."""
        try:
            with self.lock:
                if getattr(self, "recovery_blocked", False) or not self._handoff_gate().growth_allowed():
                    return
            handoff = getattr(self, "_handoff", {})
            guarded = bool(handoff.get("snapshot"))
            while not rules.handoff_activate(guarded and boat.pid_alive(handoff["from_pid"], socket.gethostname()), guarded):
                if self.stop.wait(0.05):
                    return
            if self.stop.is_set():
                return
            live = boat.sandboxes()
            now = time.time()
            with self.lock:
                queued = {it.batch.image for it in self.queued() if it.batch.klass <= 4}
            silent = self.__dict__.setdefault("_adopt_silent", {})
            for i, sb in live.items():
                name = sb.get("name", "")
                if (sb.get("state") not in boat.RUNNING or not name.endswith("~l")
                        or name[:-2] not in queued or i in self.vms or i in self.retired()
                        or i in getattr(self, "_releasing", ()) or i in getattr(self, "resume_quarantine", {})):
                    continue
                try:
                    health = boat.agent_health(i)
                    who = health.get("lease")
                    if not isinstance(who, dict):
                        self.alert(f"{i}: lease protocol missing; old client wins, left untouched")
                        continue  # Legacy protocol: old client wins, no bootstrap war.
                    silent.pop(i, None)
                    holder = who.get("holder") or who.get("owner")
                    if holder and not who.get("lapsed") and who.get("state") not in ("released",):
                        continue
                except Exception:
                    # An HTTP outage is not permission to steal a known holder.
                    first = silent.setdefault(i, time.monotonic())
                    if time.monotonic() - first < BREAK_READY:
                        continue
                    cached = Registry().read().get("vms", {}).get(i, {})
                    if cached.get("lease"):
                        continue
                    # A missing local cache proves nothing about another machine.
                    # Exec may discover a holder, but never permits stealing a
                    # held lease (even lapsed) during an HTTP outage.
                    try:
                        remote = boat.lease_vm(i, "who")
                    except Exception:
                        continue
                    if not remote.get("ok"):
                        continue
                    if (remote.get("holder") or remote.get("owner")) and remote.get("valid_boot", True) and remote.get("state") != "released":
                        continue
                with self.lock:
                    if not self._handoff_gate().growth_allowed():
                        break
                    self._adopt_claims = getattr(self, "_adopt_claims", 0) + 1
                try:
                    try:
                        claim = boat.lease_vm(i, "claim")
                    except Exception:
                        continue
                    if (not claim.get("ok") or claim.get("stale") or claim.get("pending")
                            or not claim.get("boot") or not claim.get("epoch")):
                        continue  # Draining previous owner's jobs is not available capacity.
                    boat.remember_lease(i, claim)
                    with Registry().locked() as d:
                        v = d["vms"].setdefault(i, {"type": sb.get("type", boat.VM_TYPE), "snapshot": name[:-2], "created": now})
                        v.update({"last_used": now, "state": sb["state"], "lease": {
                            "pid": os.getpid(), "host": socket.gethostname(), "batch": "scheduler", "since": now, "label": "scheduler (adopted)"}})
                    self._bring(i, name[:-2], v["type"], "adopt", 5, None, lease=claim)
                finally:
                    with self.lock:
                        self._adopt_claims -= 1
                    self.wake.set()
        except Exception as e:
            log(f"adoption: {str(e)[:200]}")
        finally:
            self.adopting = False
            self.wake.set()

    def _owned_client(self, vm: VMState):
        # Busy agents keep their exact wire version until their safe-point upgrade.
        return OwnedAgentClient(vm.client, getattr(vm, "agent", "") or self.agent_version)

    def _record_agent_ping(self, vm: VMState, reply: dict):
        """Cache only what this agent actually advertised, including unknown old agents."""
        with self.lock:
            vm.agent = reply.get("version") if isinstance(reply.get("version"), str) else ""
            vm.agent_capabilities = normalize_agent_capabilities(reply.get("agent_capabilities"))

    def _verify_agent_capabilities(self, it: Item, vm: VMState):
        required = required_agent_capabilities(it.spec)
        if not required:
            return  # Legacy jobs retain their existing preparation and launch path.
        try:
            reply = vm.client.ping()
        except Exception as error:
            raise AgentCapabilityError(f"agent ping failed: {error}") from error
        if not reply.get("ok") or reply.get("stale"):
            raise AgentCapabilityError("agent ping refused")
        self._record_agent_ping(vm, reply)
        with self.lock:
            if (vm.dead or vm.upgrading or not vm.agent
                    or not agent_capabilities_match(required, vm.agent_capabilities)):
                raise AgentCapabilityError("agent did not advertise the required protocol versions")
            placement = it.last_placement or {}
            if placement.get("agent_version") != vm.agent:
                raise AgentCapabilityError("agent version changed after placement")

    def _check_agent(self, vm: VMState):
        """Atomically fence empty-agent upgrades against placement, never network under lock."""
        with self.lock:
            if vm.dead or not agent_upgrade_allowed(len(vm.running) + getattr(vm, "foreign", 0),
                    len(getattr(vm, "starts", [])), getattr(vm, "upgrading", False)):
                return
            old = vm.agent
        # Read-only probing does not remove a current worker from placement.
        # A job may win during this I/O; the second atomic check then forbids upgrade.
        if not self._renew_lease(vm):
            return
        r = vm.client._call("ping") if isinstance(vm.client, SchedulerAgentClient) else vm.client.ping()
        if r.get("stale"):
            self._drop_stale(vm, source='upgrade.stale_ping', reply=r, operation='rpc/ping')
            return
        with self.lock:
            stopped = getattr(self, "stop", None)
            if ((stopped is not None and stopped.is_set()) or getattr(self, "handing_off", False)
                    or getattr(self, "_handoff_deferred", None) or not self._handoff_gate().growth_allowed()):
                return  # Read-only probing may overlap handoff; never mutate after its freeze.
            if vm.dead or vm.agent != old:
                return  # A newer observation already superseded this probe.
            if r.get("version") != old and old:
                self.alert(f"{vm.id}: agent changed outside holder; old client wins, relinquished")
                self._drop_stale(vm, source='upgrade.agent_changed', reply=r, operation='rpc/ping')
                return
            self._record_agent_ping(vm, r)
            vm.t_agent = time.time()
            if vm.agent == self.agent_version:
                return  # Matching hashes alone never populate capabilities.
            if not agent_upgrade_allowed(len(vm.running) + getattr(vm, "foreign", 0),
                    len(getattr(vm, "starts", [])), getattr(vm, "upgrading", False)):
                return
            vm.upgrading = True
        try:
            try:
                upgraded = vm.client.rpc("upgrade", self.agent_b64, str(boat.AGENT_PORT), vm.client.token, timeout=30)
                if upgraded.get("ok") is False:
                    vm.t_agent = 0
                    return
            except Exception:
                vm.t_agent = 0
                return  # Ambiguous replies never authorize bootstrap overwrite.
            for _ in range(30):
                time.sleep(1)
                with contextlib.suppress(Exception):
                    reply = vm.client.ping()
                    if reply.get("version") == self.agent_version:
                        self._record_agent_ping(vm, reply)
                        log(f"{vm.id}: agent {old} -> {vm.agent} (upgraded while empty)")
                        boat.write_agent(vm.id, {**(boat.read_agent(vm.id) or {}), "version": vm.agent})
                        return
            vm.t_agent = 0
        finally:
            with self.lock:
                vm.upgrading = False
            self.wake.set()

    def _heartbeat(self):
        while not self.stop.wait(60):
            # Readiness can outlast a lease: keep authority alive while placement
            # and completion ACKs pause. Renewal does not change the capability.
            for v in list(self.vms.values()):
                # _bring registers booting VMs before their first grant. An
                # unclaimed placeholder has not lost a lease; _up owns it.
                if not v.dead and v.lease:
                    with contextlib.suppress(Exception):
                        if self._renew_lease(v):
                            v.ttl = max(boat.BACKSTOP, v.ttl)
                            boat.boat("extend", v.id, "--ttl", str(v.ttl), "--json", check=False)

    def shutdown(self):
        """A restart (a deploy, the service restarted): VMs running items keep them (their TTL renewed, their leases
        let go for the next process to take back with their items); the others are released."""
        self.stop.set()
        self._metrics_save()
        for v in list(self.vms.values()):
            self._let_go(v, keep=rules.keep_across_restart(len(v.running), v.dead))


# --------------------------------------------------------------------------------------------- the blob store
def blob_tar(keys: list[str]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tf:
        for k in keys:
            data = blobcache.read(BLOBS, k)
            ti = tarfile.TarInfo(k)
            ti.size = len(data)
            ti.mode = 0o755 if k.endswith("x") else 0o644
            tf.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


def store_blobs(tree) -> int:
    """A tree's files into the blob store by key (the ones it lacks), so the scheduler ships what the client saw."""
    BLOBS.mkdir(parents=True, exist_ok=True)
    by = {}
    for f, k in tree.files.items():
        by.setdefault(k, f)
    n = 0
    missing = set(blobcache.have(BLOBS, list(by)))
    for k, f in by.items():
        if k not in missing:
            continue
        data = tree.read(f)
        if data is None:
            raise RuntimeError(f"{f} changed while the batch shipped it: run again")
        blobcache.put(BLOBS, k, data)
        n += 1
    return n



# --------------------------------------------------------------------------------------- a remote client's uploads
def _state_export(s) -> bytes:
    from . import treadmill

    @contextlib.contextmanager
    def snapshot_guard():
        # Only pin queue generations and complete journal prefixes here. Directory
        # traversal, file payload reads, gzip and delivery all run outside these locks.
        with s._request_lock:
            with s.lock:
                yield {"closed": {b.id for b in s.batches.values() if b.closed}}

    metadata = {}
    return s._state_archive.get(lambda: statebackup.build(config.LIVE, treadmill.use(), snapshot_guard, metadata, file_backed=True), metadata)


def raw_post(path: str, body: bytes):
    """The controller's uploads: blobs (a tar named by key), a cache's files, a queue submission, the repos' git."""
    from urllib.parse import parse_qs, urlparse
    q = {k: v[0] for k, v in parse_qs(urlparse(path).query).items()}
    p = path.split("?")[0]
    if p == "/blobs/put":
        n = 0
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:*") as tf:
            for m in tf.getmembers():
                data = tf.extractfile(m).read()
                blobcache.put(BLOBS, m.name, data)
                n += 1
        return ({"stored": n},)
    if p == "/cache/put":
        d = config.LIVE / q["name"] / q["key"]
        d.mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:*") as tf:
            tf.extractall(d, filter="data")
        return ({"ok": True},)
    if p == "/queue/add":  # (a jobs file and its tree, for the treadmill here: as `supercomputer queue add` writes)
        from . import treadmill
        tm = treadmill.use()
        meta = json.loads(q["meta"])
        d = tm / "subs" / meta["name"]
        if d.exists():
            raise ValueError(f"{meta['name']} exists: another --name")
        d.mkdir(parents=True)
        with tarfile.open(fileobj=io.BytesIO(body), mode="r:") as tf:
            (d / "tree.tar.gz").write_bytes(tf.extractfile("tree.tar.gz").read())
            (d / "jobs.json").write_bytes(tf.extractfile("jobs.json").read())
        (d / "meta.json").write_text(json.dumps(meta, indent=1))
        return ({"queued": meta["name"]},)
    if p == "/git":  # (bundles of the repos the treadmill follows: main, and the submodules' commits)
        repo = pathlib.Path(q["repo"]).expanduser()
        b = DIR / f"in-{os.getpid()}-{threading.get_ident()}.bundle"
        b.write_bytes(body)
        try:
            r = subprocess.run(["git", "-C", str(repo), "fetch", "-q", str(b), q.get("refspec", "+main:main")], capture_output=True, text=True, timeout=600)
            if r.returncode != 0:
                raise RuntimeError(r.stderr[-400:])
            if q.get("checkout"):
                subprocess.run(["git", "-C", str(repo), "reset", "-q", "--hard", q["checkout"]], capture_output=True, timeout=120)
        finally:
            b.unlink(missing_ok=True)
        return ({"ok": True},)
    raise ValueError(p)


def queue_ls() -> dict:
    from . import treadmill
    tm = treadmill.use()
    subs = sorted((json.loads(p.read_text()) for p in (tm / "subs").glob("*/meta.json")), key=lambda m: m["t"]) if (tm / "subs").exists() else []
    st = json.loads((tm / "status.json").read_text()) if (tm / "status.json").exists() else {}
    return {"subs": subs[-40:], "status": st}


def queue_results(name: str) -> dict:
    from . import treadmill
    d = treadmill.use() / "subs" / name
    meta = json.loads((d / "meta.json").read_text())
    out = {"meta": meta, "summary": (d / "out" / "summary.tsv").read_text() if (d / "out" / "summary.tsv").exists() else ""}
    out["logs"] = {f.stem: f.read_text()[-20000:] for f in sorted((d / "out").glob("*.log"))} if (d / "out").exists() else {}
    return out


def _notify_service(pid: int | None = None):
    """Let systemd follow the inherited-listener child (NotifyAccess=all)."""
    address = os.environ.get("NOTIFY_SOCKET")
    if not address:
        return
    if address.startswith("@"):
        address = "\0" + address[1:]
    with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
        sock.connect(address)
        sock.sendall((f"MAINPID={pid}\nREADY=1" if pid else "READY=1").encode())


# --------------------------------------------------------------------------------------------- the server
def serve(idle_exit: float = 120, takeover: bool = False):
    DIR.mkdir(parents=True, exist_ok=True)
    inherited = os.environ.pop("SUPERCOMPUTER_HANDOFF_FDS", "")
    listen_fd, lock_fd = map(int, inherited.split(",")) if inherited else (-1, -1)
    lockf = os.fdopen(lock_fd, "a") if inherited else open(DIR / "serve.lock", "a")
    for k in range(0 if inherited else (120 if takeover else 1)):  # (one scheduler per pool: the lock; a takeover waits for the old one to go)
        try:
            fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except OSError:
            if k == (119 if takeover else 0):
                print("the scheduler is running already", file=sys.stderr)
                return
            time.sleep(0.5)
    os.environ["SUPERCOMPUTER_CLIENT"] = CLIENT
    # _take_pool defers its ACK until this process actually serves.
    if inherited:
        os.environ["SUPERCOMPUTER_HANDOFF_FDS"] = inherited
    s = Scheduler(idle_exit)
    os.environ.pop("SUPERCOMPUTER_HANDOFF_FDS", None)
    if inherited and s.recovery_blocked:
        s._publish_handoff_ack()
        return  # no worker, placement or release under uncertain ownership
    s._request_lock = threading.RLock()
    s._state_archive = statebackup.ArchiveCache()
    if (DIR / "token").exists() and (DIR / "token").read_text().strip():  # (the same token across restarts: clients keep theirs)
        s.token = (DIR / "token").read_text().strip()
    with config.private_umask():
        (DIR / "token").write_text(s.token)
    (DIR / "pid").write_text(str(os.getpid()))

    class H(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        timeout = HTTP_REQUEST_TIMEOUT_S  # StreamRequestHandler applies this before headers/body are read.

        def log_message(self, *a):
            pass

        def _send(self, code, obj=None, raw=None):
            if isinstance(obj, (bytes, bytearray)):
                obj, raw = None, bytes(obj)
            body = raw if raw is not None else json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Length", str(len(body)))
            if code == 200 and self.path.partition("?")[0] in ("/logs", "/batch/rows"):
                self.send_header("X-SC-Content-SHA256", hashlib.sha256(body).hexdigest())
            if self.close_connection:
                self.send_header("Connection", "close")
            self.end_headers()
            if isinstance(body, statebackup.FilePayload):
                with body.open() as source:
                    while True:
                        chunk = source.read(resultfiles.CHUNK_BYTES)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
            else:
                self.wfile.write(body)

        def _peer_closed(self):
            conn = getattr(self, "connection", None)
            if conn is None:  # handler-only fixtures have no transport
                return False
            try:
                # select.select rejects valid high FDs after the NOFILE raise.
                with selectors.DefaultSelector() as probe:
                    probe.register(conn, selectors.EVENT_READ)
                    readable = bool(probe.select(0))
                closed = readable and conn.recv(1, socket.MSG_PEEK | getattr(socket, "MSG_DONTWAIT", 0)) == b""
            except BlockingIOError:
                return False
            except (OSError, ValueError):
                closed = True
            if closed:
                self.close_connection = True
            return closed

        def do_GET(self):
            self._send(200, {"ok": True, "pid": os.getpid()})

        @staticmethod
        def _response(code, obj=None, raw=None):
            # Freeze the response while the mutation is serialized, but never
            # write to a potentially slow client under the mutation lock.
            if isinstance(obj, (bytes, bytearray)):
                obj, raw = None, bytes(obj)
            return code, raw if raw is not None else json.dumps(obj, separators=(',', ':')).encode()

        def do_POST(self):
            c0 = _cpu_now()
            body = None
            try:
                # Network input is untrusted latency, not part of a mutation.
                try:
                    n = int(self.headers.get("Content-Length", "0") or 0)
                    if n < 0:
                        raise ValueError("negative content length")
                    body = self.rfile.read(n)
                except TimeoutError:
                    self.close_connection = True
                    response = self._response(408, {"error": "request body timed out"})
                except ValueError:
                    self.close_connection = True
                    response = self._response(400, {"error": "invalid content length"})
                else:
                    if len(body) != n:
                        self.close_connection = True
                        response = self._response(408, {"error": "incomplete request body"})
                    elif not hmac.compare_digest(self.headers.get("X-SC-Token", ""), s.token):
                        response = self._response(403, {"error": "token"})
                    elif not rules.http_request_serialized(self.path in ("/poll", "/status", "/health", "/logs", "/capabilities", "/results", "/batch/info", "/batch/rows")
                                                           or self.path.split("?")[0] == "/state"):
                        response = self._post(body)
                    else:
                        with s._request_lock:
                            if not rules.http_mutation_allowed(s.stop.is_set(), bool(getattr(s, "handing_off", False)),
                                                               bool(getattr(s, "_handoff_deferred", None))):
                                self.close_connection = True
                                response = self._response(503, {"error": "scheduler handoff; retry the same endpoint"})
                            else:
                                response = self._post(body)
                if response is not None:
                    if rules.http_client_fault(response[0]):
                        path = self.path.split("?")[0][:120]
                        peer = str(getattr(self, "client_address", ("unknown",))[0])[:80]
                        agent = self.headers.get("User-Agent", "")[:120]
                        declared = self.headers.get("Content-Length", "0")[:32]
                        read = len(body) if body is not None else None
                        self._h_client_fault(response[0], path, declared, read, peer, agent)
                    self._send(response[0], raw=response[1])
            finally:
                s._cpu_add("http " + self.path.split("?")[0], c0)

        @staticmethod
        def _h_client_fault(status, path, declared, read, peer, agent):
            s._h().record_client_error(time.time(), status, path, declared, read, peer, agent)
            log(f"{path}: client HTTP {status} (read {read!r}, Content-Length {declared!r}, peer {peer!r}, user-agent {agent!r})")

        def _post(self, body):
            if self.path.split("?")[0] == "/state":  # legacy binary/empty bodies are deliberately ignored
                try:
                    return self._response(200, raw=_state_export(s))
                except Exception as e:
                    return self._response(500, {"error": repr(e)[-800:]})
            raw_paths = {"/blobs/put", "/cache/put", "/queue/add", "/git"}
            if self.path.split("?")[0] in raw_paths:
                try:
                    return self._response(200, *raw_post(self.path, body))
                except Exception as e:
                    return self._response(500, {"error": repr(e)[-800:]})
            try:
                try:
                    m = json.loads(body or b"{}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # Bad client input is not a scheduler failure. Keep the
                    # catch around decoding only: an internal JSON failure in
                    # a handler must still trip health/canary below.
                    self.close_connection = True
                    return self._response(400, {"error": "invalid JSON request"})
                p = self.path
                if p == "/capabilities":
                    return self._response(200, s.capabilities())
                if p in ("/distinct-groups/open", "/distinct-groups/get", "/distinct-groups/close"):
                    try:
                        if not isinstance(m, dict):
                            raise ValueError("durable group request must be an object")
                        if p.endswith("/open"):
                            result = s.group_open(m.get("group"))
                        elif p.endswith("/get"):
                            result = s.group_get(m.get("group"))
                        else:
                            result = s.group_close(m.get("group"), m.get("generation"), m.get("ttl_s"))
                    except (ValueError, TypeError) as e:
                        return self._response(400, {"error": str(e)})
                    return self._response(200, result)
                if p == "/batch/info":
                    try:
                        return self._response(200, s.batch_info(s.batch_of(m)))
                    except (ValueError, TypeError) as e:
                        return self._response(400, {"error": str(e)})
                if p == "/batch/rows":
                    try:
                        return self._response(200, s.batch_rows(s.batch_of(m), m.get("since", 0)))
                    except (ValueError, TypeError) as e:
                        return self._response(400, {"error": str(e)})
                if p in ("/cancel", "/snapshot", "/close-receipt"):
                    bid = s.batch_of(m)
                    try:
                        s._valid_batch(bid)
                        result = (s.snapshot(bid) if p == "/snapshot" else s.close_receipt(bid)
                                  if p == "/close-receipt" else s.cancel(bid, m.get("wait_seconds", 10)))
                    except (ValueError, TypeError) as e:
                        return self._response(400, {"error": str(e)})
                    return self._response(200, result)
                if p == '/results':
                    try:
                        return self._response(200, raw=s.result_part(m.get('batch'), m.get('ref'), m.get('offset', 0), m.get('length', resultfiles.CHUNK_BYTES)))
                    except (ValueError, TypeError) as e:
                        return self._response(400, {'error': str(e)})
                    except FileNotFoundError:
                        return self._response(404, {'error': 'result file not found'})
                if p == "/submit":
                    return self._response(200, s.submit(m))
                if p in ("/poll", "/close", "/logs") and s.batch_of(m) is None:  # (a request naming no batch: the client's, cut or old; answered, not ours)
                    log(f"{p}: a request naming no batch ({len(body)} bytes, keys {sorted(m)[:6] if isinstance(m, dict) else type(m).__name__}): nothing done")
                    return self._response(400, {"error": "no batch named"})
                if p == "/poll":
                    return self._response(200, s.poll(s.batch_of(m), int(m.get("since", 0)), float(m.get("wait", 15)), peer_closed=self._peer_closed))
                if p == "/close":
                    return self._response(200, s.close(s.batch_of(m)))
                if p == "/logs":
                    if m.get("require_complete") is True:
                        try:
                            return self._response(200, raw=s.logs(s.batch_of(m), m.get("vms", []), require_complete=True))
                        except (ValueError, OSError) as e:
                            return self._response(409, {"error": str(e)})
                    return self._response(200, raw=s.logs(s.batch_of(m), m.get("vms", [])))
                if p == "/breaker/rejudge":
                    try:
                        if m.get("protocol") != 1:
                            raise ValueError("breaker rejudge protocol 1 required")
                        return self._response(200, s.rejudge_breaker(actor=m.get("actor", "operator"), reason=m.get("reason", "")))
                    except (ValueError, TypeError, OSError) as e:
                        return self._response(400, {"error": str(e)})
                if p == "/workers/recover":
                    try:
                        return self._response(200, s.recover_worker(m.get("vm"), m.get("expected_agent")))
                    except (ValueError, TypeError) as e:
                        return self._response(400, {"error": str(e)})
                if p == "/hint":
                    return self._response(200, s.hint(m))
                if p == "/status":
                    return self._response(200, raw=s.status_response())
                if p == "/health":
                    return self._response(200, s.health_check(float(m.get("minutes", 30))))
                if p == "/rollback":
                    return self._response(200, s.rollback(m.get("code") or None))
                if p == "/reload":
                    threading.Thread(target=s.handoff, daemon=True).start()
                    return self._response(200, {"ok": True, "code": s.code, "installed": code_hash()})
                if p == "/blobs/have":  # (a remote client: which of its tree's files the controller lacks)
                    return self._response(200, {"missing": blobcache.have(BLOBS, m["keys"])})
                if p == "/drain":  # (a deploy: no new item placed; what runs goes on, and comes back after the restart)
                    s.draining = bool(m.get("on", True))
                    running = sum(len(v.running) for v in s.vms.values())
                    return self._response(200, {"draining": s.draining, "running": running})
                if p == "/tree/put":  # (a tree's manifest, kept by id: later batches of it send the id alone)
                    (DIR / "trees").mkdir(parents=True, exist_ok=True)
                    with blobcache.locked(BLOBS):
                        dest = DIR / "trees" / f"{m['tree']['id']}.json"
                        tmp = dest.with_suffix(f".{threading.get_ident()}.tmp")
                        tmp.write_text(json.dumps(m["tree"]))
                        os.replace(tmp, dest)
                    return self._response(200, {"ok": True})
                if p == "/cache/have":
                    d = config.LIVE / m["name"] / m["key"]
                    return self._response(200, {"names": sorted(f.name for f in d.iterdir()) if d.exists() else []})
                if p == "/queue/ls":
                    return self._response(200, queue_ls())
                if p == "/queue/results":
                    return self._response(200, queue_results(m["name"]))
                if p == "/ledger":
                    lp = config.LIVE / "boat-starts.jsonl"
                    lines = lp.read_text().splitlines()[-int(m.get("n", 2000)):] if lp.exists() else []
                    return self._response(200, {"lines": lines})
                if p == "/can":  # can an interactive batch of this image start now, with no start? (a VM held or warm)
                    held = any(v.ready and not v.dead and not v.quarantine and v.image == m["image"] for v in s.vms.values())
                    return self._response(200, {"held": held})
                return self._response(404, {"error": p})
            except Exception as e:
                if health.client_gone(e):  # (its client gone mid-answer: nothing to answer, no error of ours; 02:38 a canary handed back for one)
                    return
                s._h().record_error(time.time(), f"{self.path}: {e!r}")
                return self._response(500, {"error": repr(e)[-800:]})

    class Srv(http.server.ThreadingHTTPServer):
        daemon_threads = True
        request_queue_size = 128

        def handle_error(self, request, client_address):  # (a client gone mid-answer, a poll cut by a restart: quiet)
            if not isinstance(sys.exc_info()[1], (BrokenPipeError, ConnectionResetError, TimeoutError)):
                super().handle_error(request, client_address)

    for k in range(60):  # (the old scheduler's port, freed as it exits)
        try:
            if inherited:
                srv = Srv(("127.0.0.1", PORT), H, bind_and_activate=False)
                srv.socket.close()
                srv.socket = socket.socket(fileno=listen_fd)
                srv.server_address = srv.socket.getsockname()
            else:
                srv = Srv(("127.0.0.1", PORT), H)
            break
        except OSError:
            if k == 59:
                raise
            time.sleep(0.5)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    s._srv, s._lockf = srv, lockf  # same open descriptions across handoff, including the flock
    if not inherited:
        _notify_service()
    s._publish_handoff_ack()
    hook = threading.excepthook

    def died(a):  # (a thread of the scheduler dead of an exception: health's errors; a client gone isn't one)
        if not health.client_gone(a.exc_value):
            s._h().record_error(time.time(), f"thread {getattr(a.thread, 'name', '?')}: {a.exc_type.__name__}: {a.exc_value}")
        hook(a)
    threading.excepthook = died
    log(f"serving on 127.0.0.1:{PORT} (pid {os.getpid()})")
    with boat.signals_as_interrupt():
        try:
            s.run()
        except KeyboardInterrupt:
            s.shutdown()
    srv.shutdown()
    (DIR / "pid").unlink(missing_ok=True)


# --------------------------------------------------------------------------------------------- the client
class Client:
    """The scheduler: this machine's (127.0.0.1), or the controller's on boat (LIVE/controller.json: its boat URL,
    boat's cookie and our token, 0600) when it answers."""

    def __init__(self, remote: dict | None = None):
        self.remote, self.port = None, PORT
        if remote:
            self.token = remote["token"]
            if remote.get("transport", "forward") == "forward":  # (boat forward: TCP over boat's ssh)
                self.port = persistent_forward(remote["vm"]) or forward(remote["vm"])
                self.remote = "forward"
            else:
                self.remote = AgentClient(remote["vm"], remote["url"], remote["token"], remote.get("cookie", ""))
        else:
            self.token = (DIR / "token").read_text().strip()

    def req(self, path: str, msg, timeout: float = 60, raw: bool = False, body: bytes | None = None, decode=None):
        data_in = body if body is not None else json.dumps(msg).encode()
        if isinstance(self.remote, AgentClient):
            observer = getattr(self, "transport_event", None)
            extra = {"transport_event": observer} if observer is not None else {}
            code, data, _ = self.remote._req("POST", path, data_in, timeout, "application/octet-stream" if body is not None else "application/json",
                                             **({"decode": decode} if decode is not None else {}), **extra)
            if code != 200:
                raise RuntimeError(f"the controller: {path}: {code} {data[:300]!r}")
        else:
            data = self._local(path, data_in, timeout, **({"decode": decode} if decode is not None else {}))
        return data if raw or decode is not None else json.loads(data)

    def _local(self, path: str, body: bytes, timeout: float, decode=None):
        """A request over a kept-alive connection (to this machine's scheduler, or through the forward: a new
        connection through boat's ssh costs a round trip of its own)."""
        import http.client
        last = None
        strict_submit = False
        if path in ("/submit", "/snapshot", "/close-receipt"):
            with contextlib.suppress(ValueError, TypeError, AttributeError):
                msg = json.loads(body)
                strict_submit = ((path == "/submit" and msg.get("strict_once") is True) or
                    (path in ("/snapshot", "/close-receipt") and type(msg.get("close_recovery")) is int and msg["close_recovery"] == 1))
        attempts = strict_transport_attempts(strict_submit, 3)
        for k in range(attempts):
            c = getattr(self, "_conns", None)
            if c is None:
                self._conns = c = []
            conn = c.pop() if c else http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
            conn.timeout = timeout
            if conn.sock is not None:
                conn.sock.settimeout(timeout)
            try:
                conn.request("POST", path, body=body, headers={"X-SC-Token": self.token, "Content-Type": "application/json"})
                r = conn.getresponse()
                try:
                    data = decode(r) if decode is not None and r.status == 200 else r.read()
                except BaseException:
                    conn.close()
                    raise
                c.append(conn)
                if r.status != 200:
                    raise RuntimeError(f"the scheduler: {path}: {r.status} {data[:300]!r}")
                return data
            except (OSError, http.client.HTTPException) as e:
                last = e
                with contextlib.suppress(Exception):
                    conn.close()
                observer = getattr(self, "transport_event", None)
                if observer is not None:
                    observer("transport_reconnect" if k + 1 < attempts else "transport_error",
                             path=path, attempt=k + 1, error=type(e).__name__)
                if k + 1 < attempts:
                    time.sleep(0.2 * (k + 1))
        raise RuntimeError(f"the scheduler didn't answer ({last})")

    def _known(self) -> dict:
        p = FWD_DIR / "known.json"
        with contextlib.suppress(Exception):
            return json.loads(p.read_text())
        return {"trees": [], "caches": []}

    def _know(self, kind: str, key: str):
        with contextlib.suppress(Exception):
            k = self._known()
            k[kind] = (k.get(kind, []) + [key])[-50:]
            FWD_DIR.mkdir(parents=True, exist_ok=True)
            (FWD_DIR / "known.json").write_text(json.dumps(k))

    def push_tree(self, tree) -> int:
        """Revalidate cached tree contents: a known manifest is not a blob-retention promise."""
        if not self.remote:
            return store_blobs(tree)
        keys = sorted(set(tree.files.values()))
        missing = self.req("/blobs/have", {"keys": keys}, timeout=120)["missing"]
        by = {}
        for f, k in tree.files.items():
            by.setdefault(k, f)
        for i in range(0, len(missing), 400):
            buf = io.BytesIO()
            with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=6) as tf:
                for k in missing[i:i + 400]:
                    data = tree.read(by[k])
                    if data is None or hashlib.sha256(data).hexdigest() != k.rstrip("x"):
                        raise RuntimeError(f"{by[k]} changed while the batch shipped it: run again")
                    ti = tarfile.TarInfo(k)
                    ti.size = len(data)
                    tf.addfile(ti, io.BytesIO(data))
            self.req("/blobs/put", None, timeout=600, body=buf.getvalue())
        self.req("/tree/put", {"tree": {"id": tree.id, "files": tree.files, "links": tree.links}}, timeout=300)
        self._know("trees", tree.id)
        return len(missing)

    def push_caches(self, caches: list[dict], outputs) -> None:
        """A build cache's files here that the controller lacks for their key."""
        if not self.remote:
            return
        for c in caches:
            local = config.LIVE / c["name"] / c["key"]
            names = sorted(f.name for f in local.iterdir() if f.name in set(outputs(c))) if local.exists() else []
            tag = f"{c['name']}/{c['key']}/{len(names)}"
            if tag in self._known().get("caches", []):
                continue
            have = set(self.req("/cache/have", {"name": c["name"], "key": c["key"]}).get("names", []))
            if not set(names) - have:
                self._know("caches", tag)
            if set(names) - have:
                buf = io.BytesIO()
                with tarfile.open(fileobj=buf, mode="w:gz", compresslevel=1) as tf:
                    for n in sorted(set(names) - have):
                        tf.add(local / n, arcname=n)
                self.req(f"/cache/put?name={c['name']}&key={c['key']}", None, timeout=600, body=buf.getvalue())

    def keep(self):
        if isinstance(self.remote, AgentClient):
            with contextlib.suppress(Exception):
                c = json.loads((config.LIVE / "controller.json").read_text())
                if self.remote.cookie and c.get("cookie") != self.remote.cookie:
                    c["cookie"] = self.remote.cookie
                    write_private(config.LIVE / "controller.json", c)


FWD_DIR = config.LIVE / "forward"
FWD_PORT = int(os.environ.get("SUPERCOMPUTER_FORWARD_PORT", "8791"))
FWD_IDLE = float(os.environ.get("SUPERCOMPUTER_FORWARD_IDLE", "1200"))  # the supervisor exits after this long unused


def _healthy(port: int, timeout: float = 3) -> bool:
    with contextlib.suppress(Exception):
        urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=timeout).read()
        return True
    return False


def persistent_forward(vm: str) -> int | None:
    """The long-lived forward to the controller (one for every command on this machine, started by the first that
    needs it): its port, or None when it can't be had (the caller forwards for itself). Each use touches its clock."""
    FWD_DIR.mkdir(parents=True, exist_ok=True)
    (FWD_DIR / "last-use").touch()
    if _healthy(FWD_PORT, 2):
        return FWD_PORT
    with contextlib.suppress(Exception):
        pid = int((FWD_DIR / "pid").read_text())
        os.kill(pid, 0)
        for _ in range(40):  # (a supervisor starting or reconnecting: a moment)
            if _healthy(FWD_PORT, 2):
                return FWD_PORT
            time.sleep(0.25)
        return None
    home = str(pathlib.Path(boat.__file__).resolve().parent.parent)
    env = {**os.environ, "PYTHONPATH": home}
    with open(FWD_DIR / "log", "a") as lf:
        subprocess.Popen([sys.executable, "-P", "-m", "supercomputer", "scheduler", "forward", vm], stdin=subprocess.DEVNULL, stdout=lf, stderr=lf,
                         start_new_session=True, close_fds=True, env=env, cwd=home)
    for _ in range(60):
        if _healthy(FWD_PORT, 2):
            return FWD_PORT
        time.sleep(0.25)
    return None


def forward_supervisor(vm: str):
    """The supervisor: `boat forward` to the controller's scheduler on FWD_PORT, kept up (started again when it drops
    or stops answering), exiting once no command has used it for FWD_IDLE seconds. One at a time (a lock)."""
    lockf = open(FWD_DIR / "lock", "a")
    try:
        fcntl.flock(lockf, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return
    (FWD_DIR / "pid").write_text(str(os.getpid()))
    p = None
    stop = threading.Event()

    def bye(*a):
        stop.set()
    import signal as _sig
    _sig.signal(_sig.SIGTERM, bye)
    _sig.signal(_sig.SIGHUP, bye)
    try:
        fails = 0
        while not stop.is_set():
            if p is None or p.poll() is not None:
                p = subprocess.Popen([boat.BOAT, "forward", vm, "--remote", str(PORT), "--local", str(FWD_PORT)], stdin=subprocess.DEVNULL,
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=boat.ENV)
                log(f"forward to {vm} on 127.0.0.1:{FWD_PORT} (pid {p.pid})")
                for _ in range(60):
                    if _healthy(FWD_PORT, 2) or p.poll() is not None:
                        break
                    time.sleep(0.25)
            if stop.wait(10):
                break
            if not _healthy(FWD_PORT, 5):
                fails += 1
                if fails >= 2:  # (dropped: a new one)
                    log("the forward stopped answering: again")
                    p.terminate()
                    with contextlib.suppress(Exception):
                        p.wait(5)
                    p, fails = None, 0
            else:
                fails = 0
            with contextlib.suppress(Exception):
                if time.time() - (FWD_DIR / "last-use").stat().st_mtime > FWD_IDLE:
                    log(f"unused for {int(FWD_IDLE)} s: exiting")
                    break
    finally:
        if p is not None and p.poll() is None:
            p.terminate()
            with contextlib.suppress(Exception):
                p.wait(5)
        (FWD_DIR / "pid").unlink(missing_ok=True)


_fwd: dict = {}


def forward(vm: str) -> int:
    """A local port forwarded to the controller's scheduler (`boat forward`, TCP over boat's ssh), for the life of
    this process: stopped at its exit by the Popen we hold."""
    if vm in _fwd:
        return _fwd[vm][1]
    import atexit
    import socket as so
    with so.socket() as t:
        t.bind(("127.0.0.1", 0))
        port = t.getsockname()[1]
    # (through a watchdog that ends the forward when this process is gone, however it ends: no forward outlives its client)
    watch = ("import os,signal,subprocess,sys,time\np=subprocess.Popen(sys.argv[2:])\n"
             "signal.signal(signal.SIGTERM, lambda *a: (p.terminate(), sys.exit(0)))\n"
             "while p.poll() is None:\n  try: os.kill(int(sys.argv[1]),0)\n  except OSError: p.terminate(); break\n  time.sleep(2)\n")
    p = subprocess.Popen([sys.executable, "-c", watch, str(os.getpid()), boat.BOAT, "forward", vm, "--remote", str(PORT), "--local", str(port)],
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=boat.ENV, start_new_session=True)
    atexit.register(lambda: (p.terminate(), p.wait(5)))
    _fwd[vm] = (p, port)
    for _ in range(150):
        with contextlib.suppress(Exception):
            urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2).read()
            return port
        if p.poll() is not None:
            break
        time.sleep(0.2)
    raise RuntimeError("boat forward to the controller didn't come up")


def write_private(path: pathlib.Path, obj: dict):
    with config.private_umask():
        tmp = path.with_suffix(f".{os.getpid()}.tmp")
        tmp.write_text(json.dumps(obj))
        tmp.rename(path)


_remote_ok: dict = {}


def controller_configured() -> bool:
    """Even unreadable controller configuration prevents an implicit local owner."""
    return (config.LIVE / "controller.json").exists() or os.environ.get("SUPERCOMPUTER_CONTROLLER") == "1" or bool(controller())


def controller() -> dict | None:
    """A configured controller is the default; only explicit LOCAL=1 opts out."""
    if os.environ.get("SUPERCOMPUTER_LOCAL") == "1":
        return None
    with contextlib.suppress(Exception):
        c = json.loads((config.LIVE / "controller.json").read_text())
        if c.get("url") and c.get("token"):
            return c
    return None


def connect() -> "Client":
    """The scheduler a batch goes to: the controller on boat when it answers (checked once a process), else this
    machine's, started if need be. A configured controller never silently falls
    back; SUPERCOMPUTER_LOCAL=1 is the only explicit local opt-in."""
    r = _remote()
    if r:
        return r
    if not rules.local_start_allowed(controller_configured(), os.environ.get("SUPERCOMPUTER_LOCAL") == "1"):
        raise SystemExit("the controller on boat doesn't answer: configured controller unavailable; not starting this machine's scheduler beside it")
    return ensure_running()


def _remote() -> "Client | None":
    c = controller()
    if not c:
        return None
    if "ok" not in _remote_ok:
        try:
            cl = Client(c)
            if not _healthy(cl.port, 3):  # (the forward answers: the scheduler behind it does)
                cl.req("/status", {}, timeout=15)
            _remote_ok["ok"] = cl
            cl.keep()
        except Exception as e:
            _remote_ok.pop("ok", None)  # a transient handoff is not a sticky fallback decision
            raise RuntimeError(f"configured controller unavailable; no local fallback ({str(e)[:120]})") from e
    return _remote_ok["ok"]


def running() -> bool:
    if _remote():
        return True
    try:
        pid = int((DIR / "pid").read_text())
        os.kill(pid, 0)
        urllib.request.urlopen(f"http://127.0.0.1:{PORT}/health", timeout=2).read()
        return True
    except Exception:
        return False


def current() -> "Client | None":
    """The scheduler if one runs (the controller's, else this machine's), started by no one."""
    r = _remote()
    if r:
        return r
    return Client() if running() else None


def ensure_running() -> Client:
    """The scheduler, started (detached, its log in LIVE/scheduler/log) if no client started it yet."""
    if not rules.local_start_allowed(controller_configured(), os.environ.get("SUPERCOMPUTER_LOCAL") == "1"):
        remote = _remote()
        if remote is None:
            raise RuntimeError("configured controller unavailable; refusing local scheduler startup")
        return remote
    if not running():
        DIR.mkdir(parents=True, exist_ok=True)
        home = str(pathlib.Path(boat.__file__).resolve().parent.parent)
        env = {**os.environ, "PYTHONPATH": home + (":" + os.environ["PYTHONPATH"] if os.environ.get("PYTHONPATH") else "")}
        env.pop("SUPERCOMPUTER_CONFIG", None)
        with open(DIR / "log", "a") as lf:
            subprocess.Popen([sys.executable, "-P", "-m", "supercomputer", "scheduler", "serve"], stdin=subprocess.DEVNULL, stdout=lf, stderr=lf,
                             start_new_session=True, close_fds=True, env=env, cwd=home)
        deadline = time.monotonic() + STARTUP_WAIT_S
        while time.monotonic() < deadline:
            if running():
                break
            time.sleep(0.1)
        else:
            raise RuntimeError(f"the scheduler didn't start: {DIR / 'log'}")
    return Client()


def enabled() -> bool:
    return os.environ.get("SUPERCOMPUTER_SCHEDULER", "1") not in ("0", "")


def main(argv: list[str]):
    import argparse
    ap = argparse.ArgumentParser(prog="supercomputer scheduler")
    ap.add_argument("cmd", choices=["serve", "status", "capabilities", "stage", "stop", "forward", "unforward"])
    ap.add_argument("vm", nargs="?", default="")
    ap.add_argument("--idle-exit", type=float, default=120, help="seconds idle before it exits (0: never, the controller's)")
    ap.add_argument("--takeover", action="store_true", help="(a handoff: wait for the old scheduler's lock)")
    a = ap.parse_args(argv)
    if a.cmd == "serve":
        serve(a.idle_exit, a.takeover)
    elif a.cmd == "stage":
        print(json.dumps(stage_install(), sort_keys=True))
    elif a.cmd == "capabilities":
        cl = _remote() if controller_configured() and os.environ.get("SUPERCOMPUTER_LOCAL") != "1" else current()
        # Read-only and fail closed: never start or silently probe a competing local scheduler.
        if cl is None:
            raise SystemExit("scheduler not running")
        print(json.dumps(cl.req("/capabilities", {}), sort_keys=True))
    elif a.cmd == "status":
        c = controller()
        if c:
            print(f"the controller on boat: {c['vm']}")
        print(json.dumps(connect().req("/status", {}), indent=1) if (c or running()) else "not running")
    elif a.cmd == "forward":
        forward_supervisor(a.vm or (controller() or {}).get("vm"))
    elif a.cmd == "unforward":
        with contextlib.suppress(Exception):
            os.kill(int((FWD_DIR / "pid").read_text()), 15)
            print("the forward supervisor stops")
    elif a.cmd == "stop":
        if (DIR / "pid").exists():
            os.kill(int((DIR / "pid").read_text()), 15)
            print("stopping")
