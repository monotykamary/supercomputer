"""The supercomputer's own Modal app for a repo that has none (its supercomputer.toml names no [modal] module), and
for GPU jobs: the image from the repo's config ([modal].image, [gpu].image), the tree as git sees it at /src/<name>,
copied to /work/<name> for each job (as on boat), one container per job.

Imported with $SUPERCOMPUTER_ROOT (or the current directory) naming the repo. Every function has scaledown_window=2
and a timeout sized to its job; a job's command is killed inside the container first, so its output still comes back.
"""
import os
import pathlib
import time

import modal

from . import config

LOCAL = modal.is_local()
if LOCAL:
    P = config.project() if config._current else config.use(os.environ.get("SUPERCOMPUTER_ROOT") or None)
    NAME, PREFIX = P.name, P.prefix
else:  # (in the container: the tree is at /src/<name>, named by the image's environment)
    P = None
    NAME, PREFIX = os.environ.get("SUPERCOMPUTER_REPO", "repo"), os.environ.get("SUPERCOMPUTER_PREFIX", "sc")


def _allowed():
    """The tree's files (what git sees, minus the repo's ignore patterns) and their directories: Modal's ignore()."""
    from . import boat
    files = set(boat.tree_files(P.root))
    dirs = {str(pathlib.PurePosixPath(f).parent) for f in files}
    dirs |= {str(p) for f in list(dirs) for p in pathlib.PurePosixPath(f).parents}

    def ignore(p) -> bool:
        p = pathlib.Path(p)
        rel = str(p.relative_to(P.root)) if p.is_absolute() else str(p)
        return rel not in files and rel not in dirs
    return ignore


def _image(spec: dict, gpu: bool = False) -> modal.Image:
    img = modal.Image.debian_slim(python_version="3.12").apt_install("curl", "ca-certificates", "git", "unzip", "xz-utils", *spec.get("apt", []))
    if spec.get("bun", True):  # (bun by default, as the boat base image's repos mostly want; "bun": false leaves it out)
        img = img.run_commands("curl -fsSL https://bun.sh/install | bash -s bun-v1.4.3")
    if spec.get("run"):
        img = img.run_commands(*spec["run"])
    env = {"PATH": "/root/.bun/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "SUPERCOMPUTER_REPO": NAME, "SUPERCOMPUTER_PREFIX": PREFIX, **spec.get("env", {})}
    return img.env(env).add_local_dir(P.root, f"/src/{NAME}", ignore=_allowed())


app = modal.App(f"supercomputer-{PREFIX}")
if LOCAL:
    cpu_image = _image(P.modal.get("image", {}))
    gpu_image = _image(P.gpu.get("image", P.gpu.get("modal_image", {})), gpu=True)
    GPU = P.gpu.get("modal_gpu", "L4")
else:
    cpu_image = gpu_image = modal.Image.debian_slim(python_version="3.12")
    GPU = "L4"


def _run(name: str, cmd: str, cwd: str, limit: int) -> dict:
    import shutil
    import subprocess
    work = pathlib.Path(f"/work/{NAME}")
    if not work.exists():
        shutil.copytree(f"/src/{NAME}", work, symlinks=True)
    t0 = time.time()
    try:
        p = subprocess.run(["bash", "-c", cmd], cwd=work / cwd, capture_output=True, encoding="utf-8", errors="replace", timeout=limit)
        code, out, err = p.returncode, p.stdout, p.stderr
    except subprocess.TimeoutExpired as e:
        code, out, err = 124, (e.stdout or b"").decode("utf-8", "replace") if isinstance(e.stdout, bytes) else (e.stdout or ""), f"timed out after {limit} s"
    return {"name": name, "code": code, "seconds": round(time.time() - t0, 2), "out": out[-400_000:], "err": err[-100_000:]}


@app.function(image=cpu_image, scaledown_window=2, cpu=2.0, memory=4096, timeout=960)
def sh(name: str, cmd: str, cwd: str = ".", limit: int = 870) -> dict:
    return _run(name, cmd, cwd, limit)


@app.function(image=gpu_image, gpu=GPU, scaledown_window=2, cpu=2.0, memory=8192, timeout=960)
def sh_gpu(name: str, cmd: str, cwd: str = ".", limit: int = 870) -> dict:
    return _run(name, cmd, cwd, limit)


def modal_jobs(spec: list[dict], pre: str, cmd, on_done, gpu: bool = False) -> dict:
    """The jobs on Modal, inside the running app: {name: {code, seconds, out, err}} (as a repo's own modal_jobs)."""
    groups: dict[tuple, list] = {}
    for j in spec:
        groups.setdefault((j.get("cpu", 2.0), j.get("memory", 8192 if gpu else 4096), j.get("timeout", 900), j.get("gpu") if gpu else None), []).append(j)
    calls, rows = [], {}
    try:
        for (cpu, memory, timeout, g), js in groups.items():
            opts = dict(cpu=cpu, memory=memory, timeout=timeout + 60)
            if gpu:
                opts["gpu"] = _modal_gpu(g)
            fn = (sh_gpu if gpu else sh).with_options(**opts)
            calls += [(j["name"], fn.spawn(j["name"], pre + cmd(j["cmd"]), j.get("cwd", "."), timeout - 30), timeout) for j in js]
        for name, c, timeout in calls:
            try:
                r = c.get(timeout=timeout + 180)
            except Exception as e:
                r = {"name": name, "code": -1, "seconds": -1, "out": "", "err": repr(e)}
            rows[name] = r
            on_done(name, r)
    finally:
        for _, c, _ in calls:
            try:
                c.cancel()
            except Exception:
                pass
    return rows


def _modal_gpu(g) -> str:
    from .rules import ops
    from .runpod import MODAL_GPU
    what = ops.modal_gpu(isinstance(g, str), isinstance(g, str) and g in MODAL_GPU, isinstance(g, str) and g.isupper() and " " not in g)
    return MODAL_GPU[g] if what == "mapped" else g if what == "as_is" else GPU
