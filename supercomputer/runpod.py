"""RunPod pods as the supercomputer's GPU backend: one pod per batch, prepared once (the repo's [gpu] setup), the
batch's jobs run on it one after another, the pod deleted when the batch ends however it ends. Modal takes over what
RunPod can't serve.

    supercomputer run --jobs jobs.json --out <dir>        jobs that say "gpu": a pod (else Modal), the rest on boat
    supercomputer runpod reap | status

Two kinds of work:
- a jobs file's jobs that declare a GPU ("gpu": a RunPod type such as "NVIDIA L4", or true): the repo's tree (as git
  sees it) at /work/<name> on the pod, each job's command under `timeout -s KILL`, its output and exit code back as
  a boat or Modal job's (gpu_jobs);
- a GPU runner's function (a Modal runner's, imported with a stub `modal`, as boat's py items are): call().

The GPU types are tried in order (the job's, then [gpu].gpus, $SUPERCOMPUTER_RUNPOD_GPUS, else GPUS below) until one is in
stock, in the secure cloud then the community cloud ($SUPERCOMPUTER_RUNPOD_CLOUDS). Before any job, the repo's [gpu]
check must pass (default `nvidia-smi -L` naming a GPU), else the pod is given up. Fallback to Modal (auto): before the
batch (the API unreachable, the balance under $SUPERCOMPUTER_RUNPOD_MIN_BALANCE, no GPU in the list), and when the
pod fails (its preparation or its connection, after one more pod); a job's own failure is its result, not a fallback.
At most $SUPERCOMPUTER_RUNPOD_MAX_TRIES pods (3) that never come up, and $SUPERCOMPUTER_RUNPOD_MAX_WAIT seconds (900) of
trying, before Modal takes the batch. $SUPERCOMPUTER_RUNPOD_FAIL=create|prepare|job forces a failure there, to test the
fallback.

Cost hygiene: the pod's name and this client's pid are in LIVE/runpod-<name>.json from before the pod exists until
it's deleted; the pod is deleted in a finally (Ctrl-C, SIGTERM and SIGHUP included); `supercomputer reap` deletes a
pod whose client is gone, and any pod of ours (sc- or gdpw-) with no ticket.
"""
import contextlib
import json
import os
import pathlib
import random
import signal
import socket
import subprocess
import sys
import tempfile
import time

from . import config
from .rules import fallback, ops
from .config import project

HERE = pathlib.Path(__file__).resolve().parent
GPUS = ["NVIDIA GeForce RTX 4090", "NVIDIA L4", "NVIDIA RTX A5000", "NVIDIA GeForce RTX 3090", "NVIDIA RTX A4000", "NVIDIA RTX 2000 Ada Generation",
        "NVIDIA RTX PRO 4000 Blackwell", "NVIDIA A40", "NVIDIA L40S", "NVIDIA L40", "NVIDIA RTX 6000 Ada Generation", "NVIDIA GeForce RTX 5090"]
# a RunPod type's nearest Modal GPU (else [gpu].modal_gpu, else L4)
MODAL_GPU = {"NVIDIA L4": "L4", "NVIDIA A40": "A10G", "NVIDIA L40S": "L40S", "NVIDIA A100 80GB PCIe": "A100-80GB", "NVIDIA H100 80GB HBM3": "H100", "NVIDIA GeForce RTX 4090": "L4"}
KEY = pathlib.Path.home() / ".runpod" / "ssh" / "runpodctl-ssh-key"
TEMPLATE = "runpod-ubuntu-2404"
BASE = r"""
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -q > /dev/null
apt-get install -y -q curl ca-certificates unzip python3 > /dev/null
"""


def _env(name: str, default: str = "") -> str:
    a, b = os.environ.get(f"SUPERCOMPUTER_RUNPOD_{name}"), os.environ.get(f"GDPW_RUNPOD_{name}")
    return {1: a, 2: b}.get(ops.precedence(bool(a), bool(b))) or default  # (the first given wins: rules.ops.precedence)


class NoCapacity(RuntimeError):
    """No pod of any GPU type in the list (out of stock, or never reachable)."""


def log(*a):
    print("[runpod]", *a, file=sys.stderr, flush=True)


def live() -> pathlib.Path:
    return config.live_dir()


def rp(*args, timeout=120, check=True) -> str:
    p = subprocess.run(["runpodctl", *args], capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"runpodctl {' '.join(args[:2])}: {(p.stderr or p.stdout)[-500:]}")
    return p.stdout


def last_json(text: str):
    """The JSON object a runpodctl command printed after its progress lines."""
    i = text.find("{")
    while i >= 0:
        try:
            return json.loads(text[i:])
        except json.JSONDecodeError:
            i = text.find("{", i + 1)
    raise ValueError(f"no JSON in: {text[-300:]}")


def balance() -> float:
    return float(json.loads(rp("user"))["clientBalance"])


def preflight() -> str | None:
    """None if RunPod can take a batch now, else why not (its API, the balance under the floor)."""
    floor = float(_env("MIN_BALANCE", "5"))
    try:
        b = balance()
    except Exception as e:
        return f"runpod API unreachable ({str(e)[:160]})"
    if not fallback.runpod_ready(True, b, floor):
        return f"runpod balance ${b:.2f} under ${floor:.2f}"
    return None


def gpu_cfg() -> dict:
    return project().gpu or {}


class Pod:
    def __init__(self, label: str, gpus: list[str] | None = None, disk: int = 20):
        cfg = gpu_cfg()
        self.gpus = gpus or [g for g in _env("GPUS").split(",") if g] or cfg.get("gpus") or GPUS
        self.name = f"{cfg.get('pod_prefix', 'sc')}-{label}-{''.join(random.choice('abcdefghijklmnopqrstuvwxyz0123456789') for _ in range(6))}"
        self.disk = int(cfg.get("disk", disk))
        self.id = None
        self.info = {}
        self.ticket = live() / f"runpod-{self.name}.json"
        self.cm = pathlib.Path(tempfile.gettempdir()) / f"sc-rp-{os.getpid()}-{random.randint(0, 1 << 30)}"
        self.t_create = None
        self.t_end = None
        self.vulkan = ""
        self.check_out = ""
        self.cost = {}

    def _ticket(self):
        self.ticket.write_text(json.dumps({"name": self.name, "id": self.id, "pid": os.getpid(), "host": socket.gethostname(), "since": time.time()}))

    def __enter__(self):
        self._ticket()
        self.balance0 = balance()
        errs = []
        clouds = [c for c in _env("CLOUDS", "SECURE,COMMUNITY").split(",") if c]
        deadline = time.time() + float(_env("MAX_WAIT", "900"))
        pc = fallback.PodCreation(int(_env("MAX_TRIES", "3")), deadline)  # (pods made that never came up: at most MAX_TRIES)
        for cloud, g in [(c, g) for c in clouds for g in self.gpus]:
            if not pc.another(time.time()):  # (no more of them, Modal instead)
                errs.append(f"gave up after {pc.made} pods that never came up and {round(time.time() - (deadline - float(_env('MAX_WAIT', '900'))))} s")
                break
            self.t_create = time.time()
            if _env("FAIL") == "create":
                errs.append(f"{g} ({cloud.lower()}): forced (SUPERCOMPUTER_RUNPOD_FAIL=create)")
                continue
            # (no --wait: runpodctl 3.0.1 waits for a banner on the public port 22 itself, which RunPod maps to another
            # port, so it timed out on pods that answered ssh; ready() polls the mapped port instead)
            p = subprocess.run(["runpodctl", "pod", "create", "--name", self.name, "--gpu-id", g, "--cloud-type", cloud, "--template-id", TEMPLATE, "--container-disk-in-gb", str(self.disk),
                                "--env", json.dumps({"NVIDIA_DRIVER_CAPABILITIES": "all"})], capture_output=True, text=True, timeout=180)
            out = p.stdout + p.stderr
            try:
                d = last_json(p.stdout)
            except ValueError:
                d = {}
            if p.returncode == 0 and d.get("id"):
                self.id = d["id"]
                self._ticket()
                d = self.ready(d["id"], float(_env("READY_WAIT", "360")))
            if p.returncode == 0 and d.get("id") and d.get("ssh"):
                self.id, self.info = d["id"], d
                self._ticket()
                log(f"pod {self.id} ({g}, {cloud.lower()} cloud, ${d.get('costPerHr')}/h) up after {round(time.time() - self.t_create, 1)} s")
                return self
            # made but never reachable (or half made): gone before the next type
            for q in json.loads(rp("pod", "list", "--all", check=False) or "[]"):
                if q.get("name") == self.name:
                    self.id = q["id"]
                    self._delete()
                    pc.never_up()
            self.id = None
            errs.append(f"{g} ({cloud.lower()}): {out.strip().splitlines()[-1][-160:] if out.strip() else p.returncode}")
            log(f"no {g} in {cloud.lower()} cloud: {errs[-1][-200:]}")
        self.ticket.unlink(missing_ok=True)
        raise NoCapacity("no GPU pod: " + " | ".join(errs[:6]) + (f" (and {len(errs) - 6} more)" if len(errs) > 6 else ""))

    def ready(self, pod: str, wait: float) -> dict:
        """The pod once its ssh answers on its mapped port (its `pod get`), else {} after `wait` seconds."""
        t0 = time.time()
        while time.time() - t0 < wait:
            with contextlib.suppress(Exception):
                d = json.loads(rp("pod", "get", pod, check=False) or "{}")
                s = d.get("ssh") or {}
                if s.get("ip") and s.get("port"):
                    self.info = d
                    q = subprocess.run([*self.ssh_args()[:-1], "-o", "ConnectTimeout=10", self.ssh_args()[-1], "true"], capture_output=True, timeout=30)
                    if q.returncode == 0:
                        return {**d, "id": pod}
            time.sleep(5)
        self.info = {}
        return {}

    def ssh_args(self) -> list[str]:
        s = self.info["ssh"]
        return ["ssh", "-i", str(KEY), "-p", str(s["port"]), "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "-o", "LogLevel=ERROR",
                "-o", "ServerAliveInterval=15", "-o", "ControlMaster=auto", "-o", f"ControlPath={self.cm}", "-o", "ControlPersist=120", f"root@{s['ip']}"]

    def sh(self, cmd: str, timeout: int = 600, stdin: bytes | None = None) -> tuple[int, str]:
        p = subprocess.run([*self.ssh_args(), cmd], input=stdin, capture_output=True, timeout=timeout)
        return p.returncode, (p.stdout + p.stderr).decode("utf-8", "replace")

    def sh2(self, cmd: str, timeout: int = 600) -> tuple[int, str, str]:
        p = subprocess.run([*self.ssh_args(), cmd], capture_output=True, timeout=timeout)
        return p.returncode, p.stdout.decode("utf-8", "replace"), p.stderr.decode("utf-8", "replace")

    def envs(self) -> str:
        e = {"PATH": "/root/.bun/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "NVIDIA_DRIVER_CAPABILITIES": "all", **gpu_cfg().get("env", {})}
        return "export " + " ".join(f"{k}={sh_quote(v)}" for k, v in e.items()) + "; "

    def prepare(self, tree: bytes | None = None):
        """The pod's setup (the repo's [gpu] setup on the base), its GPU check, the tree at /work/<name>, the agent."""
        t0 = time.time()
        probes = ops.Retries(10)  # sshd answers before the container is quite ready
        while probes.another():
            code, out = self.sh("true", 60)
            if code == 0:
                probes.succeeded()
                break
            time.sleep(5)
        if _env("FAIL") == "prepare":
            raise RuntimeError("forced (SUPERCOMPUTER_RUNPOD_FAIL=prepare)")
        cfg = gpu_cfg()
        code, out = self.sh(BASE + cfg.get("setup", "") + "\necho PREP-OK", 900)
        if "PREP-OK" not in out:
            raise RuntimeError(f"pod prep failed: {out[-1500:]}")
        check, expect = cfg.get("check", "nvidia-smi -L"), cfg.get("check_expect", "GPU")
        code, chk = self.sh(self.envs() + check, 120)
        if expect not in chk:
            raise RuntimeError(f"the GPU check failed on {self.id}: {chk[-800:]}")
        self.check_out = self.vulkan = chk.strip()
        name = project().name
        if tree is None:
            from . import boat
            tree = boat.Tree().tar
        code, out = self.sh(f"rm -rf /work/{name} && mkdir -p /work/{name} /root/sc && tar xzf - -C /work/{name} && echo TREE-OK", 600, stdin=tree)
        if "TREE-OK" not in out:
            raise RuntimeError(f"tree upload failed: {out[-800:]}")
        code, out = self.sh("cat > /root/sc/agent.py", 60, stdin=(HERE / "agent.py").read_bytes())
        log(f"pod {self.id} prepared in {round(time.time() - t0, 1)} s; " + " | ".join(l.strip() for l in self.check_out.splitlines()[:3]))

    def job(self, j: dict, pre: str, cmd) -> dict:
        """A jobs file's job on the pod: {code, seconds, out, err}; an exception if the pod itself failed."""
        if _env("FAIL") == "job":
            raise RuntimeError("forced (SUPERCOMPUTER_RUNPOD_FAIL=job)")
        limit = int(j.get("timeout", 900)) - 30
        name = project().name
        t0 = time.time()
        line = self.envs() + f"cd /work/{name}/{j.get('cwd', '.')} && timeout -s KILL {limit} bash -c {sh_quote(pre + cmd(j['cmd']))}"
        code, out, err = self.sh2(line, limit + 120)
        secs = round(time.time() - t0, 2)
        what = fallback.job_outcome(code, bool(out), "Connection" in err, secs, limit)
        if what == "pod_lost":  # (ssh lost the pod, not the job's own exit)
            raise RuntimeError(f"pod {self.id} lost: {err[-300:]}")
        if what == "timeout":
            code, err = 124, err + f"\ntimed out after {limit} s"
        return {"code": code, "seconds": secs, "out": out[-400_000:], "err": err[-100_000:]}

    def call(self, runner: str, fn: str, args: list, timeout: int = 900) -> dict:
        """A runner's function on the pod, as its Modal function calls it."""
        name = project().name
        mod = str(pathlib.Path(runner).resolve().relative_to(project().root))
        py = ("import json, sys; sys.path.insert(0, '/root/sc'); from agent import import_runner; "
              f"m = import_runner('/work/{name}/{mod}'); r = m.{fn}(*json.loads(sys.argv[1])); print('\\nSC-RESULT ' + json.dumps(r))")
        cmd = self.envs() + f"rm -f /tmp/result.json; cd /work/{name} && python3 -c {sh_quote(py)} {sh_quote(json.dumps(args))}"
        code, out = self.sh(cmd, timeout + 60)
        tail = out.rsplit("SC-RESULT ", 1)
        if len(tail) < 2:
            raise RuntimeError(f"{fn} on the pod failed ({code}): {out[-2000:]}")
        return json.loads(tail[1].strip().splitlines()[0])

    def _delete(self):
        if not self.id:
            return
        rounds = ops.Retries(5)
        while rounds.another():
            rp("pod", "delete", self.id, check=False)
            time.sleep(2)
            if not any(p.get("id") == self.id for p in json.loads(rp("pod", "list", "--all") or "[]")):
                rounds.succeeded()
                return
        log(f"pod {self.id} still listed after delete: check `runpodctl pod list --all`")

    def __exit__(self, *exc):
        self.t_end = time.time()
        with contextlib.suppress(Exception):
            subprocess.run([*self.ssh_args()[:-1], "-O", "exit", self.ssh_args()[-1]], capture_output=True, timeout=10)
        self._delete()
        self.ticket.unlink(missing_ok=True)
        time.sleep(5)
        with contextlib.suppress(Exception):
            self.balance1 = balance()
        up = round(self.t_end - (self.t_create or self.t_end), 1)
        self.cost = {"pod": self.id, "gpu": self.info.get("gpuId"), "cost_per_hr": self.info.get("costPerHr"), "held_s": up,
                     "balance_before": self.balance0, "balance_after": getattr(self, "balance1", None),
                     "balance_delta": round(self.balance0 - self.balance1, 4) if hasattr(self, "balance1") else None}
        log(f"pod {self.id} deleted after {up} s; balance {self.balance0} -> {getattr(self, 'balance1', '?')}")
        return False


def sh_quote(s: str) -> str:
    return "'" + str(s).replace("'", "'\\''") + "'"


@contextlib.contextmanager
def signals_as_interrupt():
    def h(sig, frame):
        raise KeyboardInterrupt(f"signal {sig}")
    old = {}
    with contextlib.suppress(ValueError):
        old = {s: signal.signal(s, h) for s in (signal.SIGTERM, signal.SIGHUP)}
    try:
        yield
    finally:
        for s, o in old.items():
            signal.signal(s, o)


def call(runner: str, fn: str, args: list, gpus: str = "", label: str = "gpu", timeout: int = 900) -> dict:
    """One call on a pod of its own (created, prepared, the call, deleted)."""
    with signals_as_interrupt():
        pod = Pod(label, [g for g in gpus.split(",") if g] or None)
        with pod:
            pod.prepare()
            t0 = time.time()
            r = pod.call(runner, fn, args, timeout)
            r = {**r, "runpod": {"seconds": round(time.time() - t0, 1), "vulkan": pod.vulkan}}
        r["runpod"].update(pod.cost)
        return r


def gpu_list(jobs: list[dict]) -> list[str] | None:
    """The jobs' own GPU types first (a "gpu" that names one), then the rest of the list."""
    named = []
    for j in jobs:
        g = j.get("gpu")
        if isinstance(g, str) and g not in named:
            named.append(g)
    if not named:
        return None
    base = [g for g in _env("GPUS").split(",") if g] or gpu_cfg().get("gpus") or GPUS
    return named + [g for g in base if g not in named]


def gpu_jobs(jobs: list[dict], out: str, pre: str, cmd, backend: str = "auto", on_done=None, gpu_modal=None) -> tuple[dict, dict, dict]:
    """A batch's GPU jobs: on one RunPod pod (auto, runpod), on Modal when RunPod can't (auto) or when asked (modal).
    Returns {name: row}, {name: backend}, {name: why it moved}."""
    rows, where, moved = {}, {}, {}
    todo = list(jobs)
    reason = None
    if backend in ("auto", "runpod"):
        reason = preflight() if backend == "auto" else None
        if reason is None:
            tries = fallback.GpuTries()  # (two pods at most; none after "no capacity")
            while tries.next_pod():
                try:
                    with signals_as_interrupt():
                        pod = Pod("batch", gpu_list(todo))
                        with pod:
                            pod.prepare()
                            for j in list(todo):
                                r = pod.job(j, pre, cmd)
                                rows[j["name"]] = r
                                where[j["name"]] = "runpod"
                                todo.remove(j)
                                if on_done:
                                    on_done(j["name"], r)
                    with contextlib.suppress(Exception):
                        pathlib.Path(out).mkdir(parents=True, exist_ok=True)
                        (pathlib.Path(out) / "runpod.json").write_text(json.dumps({**pod.cost, "check": pod.check_out}, indent=1))
                    tries.ended("ok")
                except NoCapacity as e:
                    tries.ended("no_capacity")
                    reason = f"runpod has no GPU in the list ({str(e)[:300]})"
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    tries.ended("failed")
                    reason = f"runpod pod failed ({str(e)[:300]})"
                    log(reason + ("; once more on a new pod" if tries.pods < 2 else ""))
        if todo and fallback.gpu_leftover(backend) == "failed":
            for j in todo:
                rows[j["name"]] = {"code": -1, "seconds": -1, "out": "", "err": reason or "never ran"}
                where[j["name"]] = "runpod"
            return rows, where, moved
    if todo:
        why = reason or "asked for Modal"
        log(f"{len(todo)} GPU jobs on Modal ({why})")
        moved.update({j["name"]: why for j in todo})
        got = (gpu_modal or modal_gpu_jobs)(todo, pre, cmd, on_done)
        rows.update(got)
        where.update({n: "modal" for n in got})
    return rows, where, moved


def modal_gpu_jobs(jobs: list[dict], pre: str, cmd, on_done) -> dict:
    """GPU jobs on Modal, in the supercomputer's own Modal app for the repo (modal_run.py: the repo's [gpu] setup)."""
    from . import fanout, modal_run
    with fanout.ModalCtx(modal_run.app):
        return modal_run.modal_jobs(jobs, pre, cmd, on_done, gpu=True)


def reap():
    pods = json.loads(rp("pod", "list", "--all") or "[]")
    tickets = {}
    for t in live().glob("runpod-*.json"):
        d = json.loads(t.read_text())
        tickets[d["name"]] = (t, d)
    from .boat import pid_alive
    for p in pods:
        name = p.get("name", "")
        if not name.startswith(("sc-", "gdpw-")):
            continue
        t = tickets.get(name)
        if fallback.pod_reap(True, bool(t), bool(t) and pid_alive(t[1]["pid"], t[1].get("host") or socket.gethostname())) == "keep":
            print(f"live: pod {p['id']} {name} (client {t[1]['pid']})")
            continue
        rp("pod", "delete", p["id"], check=False)
        print(f"deleted pod {p['id']} {name} ({'client gone' if t else 'no ticket'})")
        if t:
            t[0].unlink(missing_ok=True)
    for name, (t, d) in tickets.items():
        if not any(p.get("name") == name for p in pods) and not pid_alive(d["pid"], d.get("host") or socket.gethostname()):
            t.unlink(missing_ok=True)
    print(f"{len(json.loads(rp('pod', 'list') or '[]'))} pods up")
