"""The controller: the scheduler and the treadmill on one small boat VM of their own, so they keep running while this
machine sleeps. This machine's `supercomputer` commands reach the scheduler over boat's token-gated URL and a token
of ours (LIVE/controller.json, 0600); when it doesn't answer, they use a scheduler here (the local fallback).

    supercomputer controller setup [--vm ID] [--vms 40]    ship the tool and the repo, hand it VMs, start its services
    supercomputer controller status | sync | logs [scheduler|treadmill] | restart | stop

On the VM (a `small` one, no auto-stop: it runs the scheduler continuously, which is its work):
- ~/sc/tool (this tool), ~/sc/live (its registry, starts ledger, blobs, caches, the scheduler's token), ~/sc/treadmill
  (the treadmill's state), ~/sc/repos/<repo> (the repo the treadmill follows, its main and submodules synced from here);
- two systemd services, `supercomputer-scheduler` and `supercomputer-treadmill`, enabled at boot and restarted on
  failure: a VM that boat recovers or that is stopped and resumed comes back with both, its disk (the state) kept;
- the boat API key `supercomputer-scheduler` (scoped to the sandbox actions the scheduler needs) in the VM's memory
  only (/run/sc-boat/boat-key, tmpfs, 0700/0600), read into the services' $BOAT_API_KEY at their start; never
  `boat login` (its config file is captured by boat's own snapshots of the VM); a restarted VM gets a rotated one
  (`rekey`). Revoke: `boat api-key revoke <id>`.

Durability: the state lives on the VM's disk, which a stop, a resume and a recover keep; `sync` (run by `run` and
`queue` at most hourly) also copies it here (LIVE/controller-state/, the last five), without secrets or blobs.
"""
import contextlib
import fcntl
import io
import json
import os
import pathlib
import subprocess
import sys
import tarfile
import threading
import time

from . import boat, config, scheduler
from .rules import ops

HOME = config.HOME
SC = "/home/user/sc"
UNITS = {
    "scheduler": "python3 -P -m supercomputer scheduler serve --idle-exit 0",
    "treadmill": "python3 supercomputer/treadmill.py run --vms {vms} --no-modal",
}


def info() -> dict:
    p = config.LIVE / "controller.json"
    return json.loads(p.read_text()) if p.exists() else {}


def ssh(vm: str, cmd: str, stdin: bytes | None = None, timeout: int = 900) -> tuple[int, str]:
    """A command on the VM over `boat ssh` (its stdin from here), again on boat's own transient errors."""
    out, code = "", -1
    tries = ops.Retries(5)  # (at most 5 tries, on boat's transient errors only)
    while tries.another():
        p = subprocess.run([boat.BOAT, "ssh", vm, "--", cmd], input=stdin, capture_output=True, timeout=timeout, env=boat.ENV)
        out, code = (p.stdout + p.stderr).decode("utf-8", "replace"), p.returncode
        if not any(e in out[:600] for e in ("could not reach the Boat API", "timed out preparing", "request failed (50")):
            tries.succeeded()
            return code, out
        time.sleep(10 * tries.tries)
    return code, out


def tool_tar() -> bytes:
    """The tool's source (as git sees it in a checkout, else the installed package) and its known repos' configs
    (HOME/repos), never live/."""
    src = config.SRC
    if (src / ".git").exists():
        files = subprocess.run(["git", "-C", str(src), "ls-files", "-co", "--exclude-standard"], capture_output=True, text=True, check=True).stdout.split()
    else:
        pkg = src / "supercomputer"
        files = [str(p.relative_to(src)) for p in sorted(pkg.rglob("*")) if p.is_file() and "__pycache__" not in p.parts]
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for f in files:
            if not f.startswith(("live/", "repos/")):
                tf.add(src / f, arcname=f)
        if (HOME / "repos").is_dir():
            for p in sorted((HOME / "repos").iterdir()):
                if p.is_file() and p.suffix in (".toml", ".py"):
                    tf.add(p, arcname=f"repos/{p.name}")
    return buf.getvalue()


def bundle(repo: pathlib.Path, refs: list[str]) -> bytes:
    return subprocess.run(["git", "-C", str(repo), "bundle", "create", "-", *refs], capture_output=True, check=True, timeout=600).stdout


# Each service bounded (systemd), so the controller's own work can never starve sshd and boat's health checks (the
# 2026-10-31 incident: a small VM at 100% with 22 VMs coming up, 70 failed /health checks in a row). The scheduler's
# watchdog sheds (no starts) well before these bite.
# Percentages are relative to physical RAM (systemd.resource-control(5)), not
# the old fixed 4G/5G caps that throttled a healthy larger controller.
SCHEDULER_MEMORY_LIMITS = "MemoryHigh=70%\nMemoryMax=82%"
LIMITS = {"scheduler": f"CPUQuota=250%\n{SCHEDULER_MEMORY_LIMITS}\nTasksMax=1500\nNice=5\nLimitNOFILE=524288:524288",
          "treadmill": "CPUQuota=100%\nMemoryHigh=1536M\nMemoryMax=2G\nTasksMax=400\nNice=10"}


def unit(name: str, cmd: str, cwd: str, env: dict) -> str:
    envs = "\n".join(f"Environment={k}={v}" for k, v in env.items())
    notify = "Type=notify\nNotifyAccess=all\nKillMode=process\n" if name == "scheduler" else ""
    return f"""[Unit]
Description=supercomputer {name}
After=network-online.target
[Service]
{notify}User=user
WorkingDirectory={cwd}
{envs}
ExecStartPre=+/bin/bash -c 'mkdir -p /run/sc-boat && chown user:user /run/sc-boat && chmod 700 /run/sc-boat'
ExecStart=/bin/bash -lc 'export BOAT_API_KEY="$$(cat /run/sc-boat/boat-key 2>/dev/null)"; exec {cmd} >> {SC}/{name}.log 2>&1'
Restart=always
RestartSec=10
{LIMITS.get(name, "")}
[Install]
WantedBy=multi-user.target
"""


def setup(vm: str, vms: int = 40, floor: int = 8, only: str = ""):
    """Everything on the controller VM, then its URL here (0600). Idempotent: run again to update the tool."""
    p = config.project()
    main = config.main_checkout(p.root)
    name = p.name
    print(f"controller {vm}: the tool, {name} at main, its submodules, the treadmill's state, {vms} pool VMs")
    # (a boat login file on the VM's disk would be in its snapshots: none, ever; the key lives in tmpfs only)
    code, out = ssh(vm, f"rm -f ~/.config/ascii/boat/config.json; mkdir -p {SC}/tool {SC}/live {SC}/treadmill {SC}/repos && tar xzf - -C {SC}/tool && echo TOOL-OK", tool_tar())
    assert "TOOL-OK" in out, out[-400:]
    repo = f"{SC}/repos/{name}"
    subs = subprocess.run(["git", "-C", str(main), "config", "--file", ".gitmodules", "--get-regexp", "path"], capture_output=True, text=True).stdout.split()[1::2]
    code, out = ssh(vm, f"test -d {repo}/.git && echo HAVE || echo NONE")
    if "NONE" in out:
        code, out = ssh(vm, f"cd {SC}/repos && cat > /tmp/main.bundle && git clone -q -b main /tmp/main.bundle {name} && rm /tmp/main.bundle && echo CLONE-OK", bundle(main, ["main"]), 1800)
        assert "CLONE-OK" in out, out[-400:]
        for sp in subs:
            sub = main / sp
            head = subprocess.run(["git", "-C", str(sub), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
            code, out = ssh(vm, f"mkdir -p {SC}/repos/{name}-sub && cat > /tmp/s.bundle && git clone -q --no-checkout /tmp/s.bundle {SC}/repos/{name}-sub/{sp.replace('/', '_')} && rm /tmp/s.bundle"
                                f" && git -C {repo} config submodule.{sp}.url {SC}/repos/{name}-sub/{sp.replace('/', '_')} && echo SUB-OK", bundle(sub, ["--all"]), 1800)
            assert "SUB-OK" in out, out[-400:]
        code, out = ssh(vm, f"cd {repo} && git -c protocol.file.allow=always submodule update --init -q && ~/.bun/bin/bun install --frozen-lockfile > /dev/null 2>&1; echo DONE $?", timeout=1800)
    else:
        sync_git(vm)
    # the treadmill's state (its results, rounds, queue), the first time
    code, out = ssh(vm, f"test -f {SC}/treadmill/cache.jsonl && echo HAVE || echo NONE")
    tm = main / "runs" / "treadmill"
    if ops.copy_state("NONE" not in out) and tm.exists():  # (never over the controller's own)
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for f in sorted(tm.rglob("*")):
                rel = f.relative_to(tm)
                if f.is_file() and rel.parts[0] not in ("trees", "unpacked") and not (rel.parts[0] == "subs" and f.name == "tree.tar.gz" and (f.parent / "out").exists()):
                    tf.add(f, arcname=str(rel))
        code, out = ssh(vm, f"tar xzf - -C {SC}/treadmill && rm -f {SC}/treadmill/treadmill.pid && echo TM-OK", buf.getvalue(), 1800)
        assert "TM-OK" in out, out[-400:]
    handover(vm, vms)
    env = {"HOME": "/home/user", "SUPERCOMPUTER_HOME": f"{SC}/tool", "SUPERCOMPUTER_LIVE": f"{SC}/live", "SUPERCOMPUTER_TREADMILL": f"{SC}/treadmill",
           "PYTHONPATH": f"{SC}/tool", "BOAT_NO_UPDATE": "1", "BOAT_BIN": "/home/user/.ascii/bin/boat", "PYTHONUNBUFFERED": "1",
           # (the scheduler's policy's first defaults, README "Scheduling policy"; after that, LIVE/policy.json rules: no VM
           # ceiling, 10 coming up at once, a 45-minute hold above the floor, gates sized to 15 minutes; a floor of 8)
           "SUPERCOMPUTER_BRINGUP": "10", "SUPERCOMPUTER_HOLD": "2700",
           "SUPERCOMPUTER_GATE_HORIZON": "900", "SUPERCOMPUTER_WATCHDOG": "1",
           "SUPERCOMPUTER_TREADMILL_SUBS_VMS": "8",
           "PATH": "/home/user/.bun/bin:/home/user/.elan/bin:/home/user/.ascii/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
    units = {f"supercomputer-{n}": unit(n, c.format(vms=floor), f"{SC}/tool" if n == "scheduler" else repo, env) for n, c in UNITS.items()}
    script = "set -e\n" + "".join(f"sudo tee /etc/systemd/system/{u}.service > /dev/null <<'EOF'\n{t}EOF\n" for u, t in units.items())
    # The running scheduler imports stable source and transfers its listener.
    # Never turn an unsupported first-upgrade endpoint into a destructive restart.
    reload = (f"T={SC}/live/scheduler/token; "
              f"curl --fail --silent --show-error -m 10 -H \"X-SC-Token: $(cat $T)\" "
              "-d '{}' http://127.0.0.1:8790/reload || "
              "echo 'reload endpoint unavailable; leaving running scheduler to automatic install pickup'\n")
    if only == "treadmill":  # (the tool's files and the treadmill only: the scheduler, and every batch in it, untouched)
        script += "sudo systemctl daemon-reload\nsudo systemctl restart supercomputer-treadmill\necho UNITS-OK\n"
    else:
        script += "sudo systemctl daemon-reload\nsudo systemctl enable --now supercomputer-scheduler\n" + reload + "sudo systemctl enable --now supercomputer-treadmill\necho UNITS-OK\n"
    code, out = ssh(vm, "bash -s", script.encode(), 300)
    assert "UNITS-OK" in out, out[-600:]
    for _ in range(30):
        code, tok = ssh(vm, f"cat {SC}/live/scheduler/token 2>/dev/null")
        if code == 0 and tok.strip():
            break
        time.sleep(2)
    url = boat.bj("host", vm, str(scheduler.PORT), "--localhost", "--private")["url"]
    scheduler.write_private(config.LIVE / "controller.json", {**info(), "vm": vm, "url": url, "token": tok.strip(), "repo": name, "t_setup": time.time()})
    print(f"controller {vm}: up; this machine's commands go there (LIVE/controller.json)")


def stage(vm: str):
    """Ship code and future-start limits; no service restart or pool changes."""
    current = scheduler.Client(info()).req("/status", {}, timeout=10)
    if current.get("hot_handoff") != 1:
        raise RuntimeError("controller has no hot-handoff protocol; refusing a mixed-version rollout")
    target = scheduler.code_hash(pathlib.Path(__file__).parent)
    release = f"{SC}/releases/{target}"
    command = ("set -e; sudo install -d /etc/systemd/system/supercomputer-scheduler.service.d; "
               "printf '[Service]\nLimitNOFILE=524288:524288\n' | "
               "sudo tee /etc/systemd/system/supercomputer-scheduler.service.d/10-nofile.conf >/dev/null; "
               # Use a fixed printf format: percentage directives are literal data.
               f"printf '%s' '[Service]\n{SCHEDULER_MEMORY_LIMITS}\n' | "
               "sudo tee /etc/systemd/system/supercomputer-scheduler.service.d/20-memory.conf >/dev/null; "
               # Leave emergency /run/systemd overrides and the running service alone.
               "sudo systemctl daemon-reload; "
               f"mkdir -p {release}; tar xzf - -C {release}; "
               f"cd {release}; PYTHONPATH={release} SUPERCOMPUTER_SRC={release} "
               f"SUPERCOMPUTER_HOME={SC}/tool SUPERCOMPUTER_LIVE={SC}/live "
               "SUPERCOMPUTER_CONTROLLER=0 python3 -P -m supercomputer scheduler stage")
    code, out = ssh(vm, command, tool_tar())
    if code:
        raise RuntimeError(f"controller staging failed: {out[-400:]}")
    receipts = [json.loads(line) for line in out.splitlines() if line.startswith('{"active"')]
    if not receipts or receipts[-1].get("staged") != target:
        raise RuntimeError("controller did not attest the staged source hash")
    print(json.dumps({"machine": vm, **receipts[-1]}, sort_keys=True))


def _publish_hints(vm: str, hints: dict) -> bool:
    """Copy discovery hints, never keys, fences, or authoritative ownership."""
    script = """import json,os,pathlib,fcntl
L=pathlib.Path(os.path.expanduser('~/sc/live'))
L.mkdir(parents=True,exist_ok=True)
m=json.load(__import__('sys').stdin)
with open(L/'boat-pool.lock','a') as lk:
 fcntl.flock(lk,2)
 p=L/'boat-pool.json'
 d=json.loads(p.read_text()) if p.exists() else {'vms':{}}
 for i,v in m.items():
  d.setdefault('vms',{}).setdefault(i,v)
 tmp=p.with_suffix('.handover.tmp')
 tmp.write_text(json.dumps(d,indent=1))
 tmp.replace(p)
print('HINTS-OK')
"""
    import base64
    encoded = base64.b64encode(script.encode()).decode()
    code, out = ssh(vm, f"python3 -c \"import base64;exec(base64.b64decode('{encoded}'))\"", json.dumps(hints).encode())
    return code == 0 and "HINTS-OK" in out


def handover(vm: str, n: int):
    """Advertise stopped capacity; the receiver must claim it on the VM after resume."""
    image = boat.image_name()
    hints = {}
    for i, v in boat.Registry().read().get("vms", {}).items():
        if v.get("snapshot") == image and v.get("type") == boat.VM_TYPE and boat.lease_free(v) and v.get("state") == "stopped":
            hints[i] = {k: x for k, x in v.items() if k not in ("lease", "owner")}
            hints[i]["lease"] = None
            if len(hints) >= n:
                break
    if n <= 0:
        hints = {}
    ok = _publish_hints(vm, hints) if hints else True
    print(f"advertised {len(hints) if ok else 0} stopped VMs; receiver claims on resume (no keys transferred)")


def handover_warm(vm: str, owner: str = "treadmill") -> int:
    """Release our idle warm leases and advertise them, without exporting agent keys."""
    now, hints = time.time(), {}
    for i, v in boat.Registry().read().get("vms", {}).items():
        if not (boat.lease_free(v) and v.get("state") in boat.RUNNING and v.get("until", 0) > now + 30
                and (v.get("owner") == owner or not owner)):
            continue
        lease = (boat.read_agent(i) or {}).get("lease")
        if not lease:
            continue
        try:
            answer = boat.lease_vm(i, "release", lease=lease)
        except Exception:
            continue
        if not answer.get("ok") or answer.get("stale"):
            continue
        hints[i] = {k: x for k, x in v.items() if k not in ("lease", "owner")}
        hints[i]["lease"] = None
    if not hints or not _publish_hints(vm, hints):
        return 0
    print(f"advertised {len(hints)} released warm VMs (no agent keys transferred)")
    return len(hints)


def rekey(vm: str) -> str:
    """The controller's boat API key rotated (`boat api-key rotate`: the old secret stops working) and its new secret
    put in the VM's memory only: /run/sc-boat (tmpfs, 0700; the boat CLI's login dir links there), never its disk, so
    no snapshot of the VM ever holds it. After a reboot of the VM the key is gone and this runs again (maybe_sync, or
    `supercomputer controller rekey`). The secret passes through this process's memory, never a file or a log here."""
    kid = info().get("key_id")
    if not kid:
        raise SystemExit("no key id recorded (LIVE/controller.json key_id)")
    p = subprocess.run([boat.BOAT, "api-key", "rotate", kid, "--json"], capture_output=True, text=True, timeout=120, env=boat.ENV)
    if p.returncode != 0:
        raise RuntimeError(f"rotate failed: {p.stderr[-200:]}")
    secret = json.loads(p.stdout.strip().splitlines()[-1])["secret"]
    # (no `boat login`: it writes the token into ~/.config/ascii/boat/config.json, which boat's own file watcher
    # captures into the VM's snapshots. The services read the key from tmpfs into $BOAT_API_KEY at their start.)
    code, out = ssh(vm, "set -e; sudo mkdir -p /run/sc-boat; sudo chown user:user /run/sc-boat; chmod 700 /run/sc-boat; umask 077; "
                        "rm -rf ~/.config/ascii/boat /run/sc-boat/config.json; mkdir -p ~/.config/ascii/boat; "
                        "cat > /run/sc-boat/boat-key; chmod 600 /run/sc-boat/boat-key; rm -f ~/.sc/controller/boat-key; "
                        "BOAT_API_KEY=\"$(cat /run/sc-boat/boat-key)\" BOAT_NO_UPDATE=1 ~/.ascii/bin/boat limits --json > /dev/null && echo REKEY-OK && "
                        "sudo systemctl restart supercomputer-scheduler supercomputer-treadmill", secret.encode(), 300)
    del secret
    if "REKEY-OK" not in out:
        raise RuntimeError(f"rekey on the VM: {out[-300:]}")
    scheduler.write_private(config.LIVE / "controller.json", {**info(), "t_rekey": time.time()})
    return kid


def boat_ok(vm: str) -> bool:
    code, out = ssh(vm, "BOAT_API_KEY=\"$(cat /run/sc-boat/boat-key 2>/dev/null)\" BOAT_NO_UPDATE=1 ~/.ascii/bin/boat limits --json > /dev/null 2>&1 && echo AUTH-OK || echo AUTH-NO", timeout=120)
    return "AUTH-OK" in out


def sync_git(vm: str | None = None):
    """The repo's main (and its submodules' commits) to the controller, when it moved since the last sync."""
    c = info()
    vm = vm or c.get("vm")
    p = config.project()
    if not vm or c.get("repo", p.name) != p.name:
        return
    main = config.main_checkout(p.root)
    head = subprocess.run(["git", "-C", str(main), "rev-parse", "main"], capture_output=True, text=True).stdout.strip()
    if not ops.sync_due(True, True, bool(head), head == c.get("main")):
        return
    repo = f"{SC}/repos/{p.name}"
    subs = subprocess.run(["git", "-C", str(main), "config", "--file", ".gitmodules", "--get-regexp", "path"], capture_output=True, text=True).stdout.split()[1::2]
    for sp in subs:
        code, out = ssh(vm, f"cat > /tmp/s.bundle && git -C {SC}/repos/{p.name}-sub/{sp.replace('/', '_')} fetch -q /tmp/s.bundle '+refs/*:refs/*' ; rm -f /tmp/s.bundle; echo OK", bundle(main / sp, ["--all"]), 1800)
    code, out = ssh(vm, f"cat > /tmp/m.bundle && git -C {repo} fetch -q /tmp/m.bundle +main:refs/remotes/laptop/main && git -C {repo} reset -q --hard laptop/main"
                        f" && git -C {repo} -c protocol.file.allow=always submodule update --init -q; rm -f /tmp/m.bundle; echo SYNC-OK", bundle(main, ["main"]), 1800)
    if "SYNC-OK" in out:
        scheduler.write_private(config.LIVE / "controller.json", {**info(), "main": head})
        print(f"controller: {p.name} main at {head[:12]}", file=sys.stderr)


# Bound the entire download, including client setup, retries and slow-drip reads.
# A socket timeout alone cannot do that; the isolated child is killed and reaped
# by subprocess.run on expiry. The parent keeps the single-flight lock throughout.
BACKUP_TIMEOUT = 30
_BACKUP_REQUEST_CODE = """
import json, signal, sys
c, timeout = json.load(sys.stdin)
# Also bound an orphan if the short-lived command exits with its daemon thread.
signal.setitimer(signal.ITIMER_REAL, timeout)
sys.path.insert(0, sys.argv[1])
from supercomputer import scheduler
data = scheduler.Client(c).req('/state', None, timeout=timeout, raw=True, body=b'')
sys.stdout.buffer.write(data)
"""


def _backup_request(c: dict, lock_fd: int | None = None) -> bytes:
    # No credentials in argv or logs; even a transport exception's stderr stays
    # captured. Use this package's source, not an unrelated installed version.
    return subprocess.run(
        [sys.executable, "-c", _BACKUP_REQUEST_CODE, str(pathlib.Path(__file__).resolve().parent.parent)],
        input=json.dumps([c, BACKUP_TIMEOUT]).encode(), capture_output=True,
        timeout=BACKUP_TIMEOUT, check=True,
        # Keep single-flight protection if the parent command exits first.
        pass_fds=() if lock_fd is None else (lock_fd,),
    ).stdout


def backup(force: bool = False):
    """Best-effort single-flight backup; failures consume the hourly attempt too.

    Keep the lock through download and retention. Forced sync bypasses cooldown,
    never an in-flight backup. A separate record avoids racing controller metadata.
    """
    with contextlib.suppress(Exception):
        config.LIVE.mkdir(parents=True, exist_ok=True)
        with config.private_umask():
            lock = (config.LIVE / "controller-backup.lock").open("a")
        with lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            c = info()
            record = config.LIVE / "controller-backup.json"
            previous = json.loads(record.read_text()) if record.exists() else {}
            # Respect the old success timestamp when upgrading, but a missing
            # timestamp is not a real attempt at epoch zero.
            last = previous.get("t_attempt", c.get("t_backup") or None)
            now = time.time()
            if not ops.backup_attempt_due(bool(c.get("url")), False, last is not None,
                                          now, float(last or 0), force):
                return
            attempt = {**previous, "t_attempt": now}
            scheduler.write_private(record, attempt)
            data = _backup_request(c, lock.fileno())
            d = config.LIVE / "controller-state"
            d.mkdir(parents=True, exist_ok=True)
            # Unique even for two explicit syncs within one second.
            name = time.strftime("%Y%m%d-%H%M%S") + f"-{time.time_ns():020d}.tgz"
            with config.private_umask():
                (d / name).write_bytes(data)
            for old in sorted(d.glob("*.tgz"))[:-5]:
                old.unlink()
            scheduler.write_private(record, {**attempt, "t_success": time.time()})


def maybe_sync():
    """Before a command that talks to the controller: main synced if it moved, the state copied and the key checked
    (a VM restarted since has none: rekeyed) at most hourly."""
    if not info().get("url") or not scheduler.controller():
        return
    with contextlib.suppress(Exception):
        sync_git()
    # (the key checked at most hourly, whether the backups succeed or not: rules.ops.Cadence; keyed to the last
    # backup's time it ran before every command while backups failed)
    cad = ops.Cadence(float(info().get("t_backup", 0)), float(info().get("t_check", 0)))
    if cad.check_due(time.time()):
        scheduler.write_private(config.LIVE / "controller.json", {**info(), "t_check": cad.t_check})
        with contextlib.suppress(Exception):
            if not boat_ok(info()["vm"]):
                print("controller: its boat key is gone (a restart): rekeyed", file=sys.stderr)
                rekey(info()["vm"])
    # Only backup becomes asynchronous: sync and key-check cadence are unchanged.
    with contextlib.suppress(Exception):
        threading.Thread(target=backup, name="controller-backup", daemon=True).start()


def main(argv: list[str]):
    import argparse
    ap = argparse.ArgumentParser(prog="supercomputer controller")
    ap.add_argument("cmd", choices=["setup", "stage", "status", "sync", "logs", "restart", "stop", "handover", "rekey", "breaker"])
    ap.add_argument("what", nargs="?", default="scheduler")
    ap.add_argument("--reason", default="", help="audit reason required for breaker rejudge (never force-closes)")
    ap.add_argument("--vm", default="")
    ap.add_argument("--vms", type=int, default=40, help="pool VMs the controller gets from this registry")
    ap.add_argument("--floor", type=int, default=8, help="the treadmill's floor")
    ap.add_argument("--only", default="", choices=["", "treadmill"], help="restart only this service (a deploy that leaves the scheduler running)")
    a = ap.parse_args(argv)
    vm = a.vm or info().get("vm")
    if not vm:
        raise SystemExit("no controller VM: supercomputer controller setup --vm <id>")
    if a.cmd == "breaker":
        if a.what != "rejudge" or not a.reason.strip():
            ap.error("use: supercomputer controller breaker rejudge --reason 'operator reason'")
        client = scheduler.Client(info())
        if client.req("/capabilities", {}).get("breaker_rejudge_protocol") != 1:
            raise SystemExit("controller does not support audited breaker rejudge")
        result = client.req("/breaker/rejudge", {"protocol": 1, "actor": os.environ.get("USER", "operator"), "reason": a.reason})
        print(json.dumps(result, indent=1))
        if not result.get("ok"):
            raise SystemExit(1)
    elif a.cmd == "setup":
        setup(vm, a.vms, a.floor, a.only)
    elif a.cmd == "stage":
        stage(vm)
    elif a.cmd == "rekey":
        print(f"rotated key {rekey(vm)}; its new secret is in the controller's memory only")
    elif a.cmd == "handover":
        handover(vm, a.vms)
    elif a.cmd == "sync":
        sync_git(vm)
        backup(force=True)
        print("synced; state copied to", config.LIVE / "controller-state")
    elif a.cmd == "status":
        code, out = ssh(vm, "systemctl is-active supercomputer-scheduler supercomputer-treadmill; uptime")
        print(f"controller {vm}:\n{out.strip()}")
        with contextlib.suppress(Exception):
            st = scheduler.Client(info()).req("/status", {}, timeout=20)
            print(f"scheduler: {len(st['batches'])} batches, {len(st['vms'])} VMs, starts by class {st['starts_by_class']}, {st['preempted']} preempted")
            if st.get("pool"):
                print(f"pool: {st['pool']}\nday: {st['day']}\nbreaker: {st['breaker']}\nshed: {st['shed']}, load {st['load']}")
                for n, c in st["classes"].items():
                    print(f"  {n:12} {c}")
                for a in st.get("alerts", []):
                    print(f"  ALERT {a['utc']}: {a['msg']}")
            q = scheduler.Client(info()).req("/queue/ls", {}, timeout=20)
            el = q.get("status", {}).get("elastic", {})
            print(f"treadmill: {len(q.get('status', {}).get('batches', []))} batches, elastic {el}")
    elif a.cmd == "logs":
        code, out = ssh(vm, f"tail -40 {SC}/{a.what}.log")
        print(out)
    elif a.cmd == "restart":
        print(ssh(vm, "sudo systemctl restart supercomputer-scheduler supercomputer-treadmill && echo restarted")[1].strip())
    elif a.cmd == "stop":  # (the services stopped; the VM stays, so its state does: `boat stop <vm>` to stop it too)
        print(ssh(vm, "sudo systemctl stop supercomputer-treadmill supercomputer-scheduler && echo stopped")[1].strip())
