"""Where the supercomputer keeps its state, and what a repo tells it (supercomputer.toml, and a hooks module it names).

The state shared by every repo on this machine (the boat account, its start limits and its pool are one): LIVE,
~/.local/share/supercomputer/live by default ($SUPERCOMPUTER_LIVE, or $GDPW_LIVE as before): the pool's registry
(boat-pool.json, under boat-pool.lock), the starts ledger (boat-starts.jsonl), Modal app tickets, RunPod pod tickets,
the store's manifests, image build logs and the build caches brought back from VMs.

A repo's file, at its root (or $SUPERCOMPUTER_CONFIG; a worktree with none reads its main checkout's):

    name = "myrepo"                  # the tree is /src/<name> (read-only) and /work/<name> (a job's own copy) on a VM
    [boat]
    prefix = "myr"                   # its image's snapshots: <prefix>-<layer>-<hash>, the image <final_prefix>-<hash>
    final_prefix = "myr-img"
    image_salt = "myr-image-v1"
    vm_type = "large"
    path = ["~/.bun/bin"]            # before the PATH of every job on a VM
    env = { JAVA_HOME = "/usr/lib/jvm/temurin-17" }  # in every job's environment on a VM (a job's own env, an image's, win)
    mounts = ["/bench"]              # directories a job's runner mounts (made on each VM)
    ignore = ["**/node_modules/**"]  # left out of the shipped tree, on top of .gitignore
    layers = [{name = "tools", script = "sudo apt-get install -y ..."}]   # or hooks.layers(root)
    [boat.images.runner]             # a job's "image": what it gets on boat (a PATH and env on the shared image)
    path = ["/usr/lib/llvm-18/bin"]
    env = {JAVA_HOME = "/usr/lib/jvm/temurin-17"}
    [[cache]]                        # a build cache keyed by the hash of its inputs: copied into a job's tree before
    name = "olean"                   #   it runs, what it built saved after, kept on each VM and here
    dir = "lean-kernel"
    modules = ["Cert", "Kernel"]
    inputs = "{m}.lean"
    outputs = "{m}.olean"
    salt = "leanprover/lean4:v4.34.0\\n"
    modal_volume = "gdp-wired-olean" # (optional) where Modal's jobs keep it: fetched when this machine has less
    [modal]
    module = "supercomputer/run.py"  # the repo's own Modal app: `app`, `modal_jobs(spec, pre, cmd, on_done)`
    image = {apt = ["curl"], run = ["curl -fsSL https://bun.sh/install | bash"], env = {PATH = "..."}}  # else this
    [gpu]
    image = {apt = [...], run = [...]}   # the GPU jobs' setup (RunPod runs `run` once per pod, Modal builds it)
    hooks = "supercomputer/hooks.py"     # optional: layers(root), job_item(item, job), ...
"""
import contextlib
import hashlib
import importlib.util
import os
import pathlib
import subprocess
import sys
import tomllib

# SRC: the folder holding the installed `supercomputer` package (a checkout, or a uv tool's site-packages);
# $SUPERCOMPUTER_SRC when this process runs another copy of the code (a hand-back runs a kept copy: SRC is still the
# install). HOME: the state's folder ($SUPERCOMPUTER_HOME; else a checkout that already holds live/ beside its code,
# as installs before the uv package did; else $XDG_DATA_HOME/supercomputer, ~/.local/share/supercomputer).
SRC = pathlib.Path(os.environ.get("SUPERCOMPUTER_SRC") or pathlib.Path(__file__).resolve().parent.parent)


def _home() -> pathlib.Path:
    if os.environ.get("SUPERCOMPUTER_HOME"):
        return pathlib.Path(os.environ["SUPERCOMPUTER_HOME"])
    if (SRC / "live").is_dir():
        return SRC
    return pathlib.Path(os.environ.get("XDG_DATA_HOME") or pathlib.Path.home() / ".local" / "share") / "supercomputer"


HOME = _home()
LIVE =pathlib.Path(os.environ.get("SUPERCOMPUTER_LIVE") or os.environ.get("GDPW_LIVE") or HOME / "live")
VERSION = "1"


def env(name: str, default: str = "") -> str:
    """$SUPERCOMPUTER_<name>, else gdp-wired's older $GDPW_BOAT_<name>, else the default."""
    from .rules import ops
    a, b = os.environ.get(f"SUPERCOMPUTER_{name}") or "", os.environ.get(f"GDPW_BOAT_{name}") or ""
    return {1: a, 2: b, 3: default}[ops.precedence(a != "", b != "")]  # (the first given wins: rules.ops.precedence)


def git_root(d: pathlib.Path) -> pathlib.Path:
    try:
        return pathlib.Path(subprocess.run(["git", "-C", str(d), "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True).stdout.strip())
    except Exception:
        return d.resolve()


def select_root(root: pathlib.Path | str | None = None) -> pathlib.Path:
    """Implicit CWD means its git root; an explicit boundary must never be widened.

    A nested directory without its own repository is refused rather than shipping
    its enclosing checkout. Standalone directories remain exact (callers that need
    git may reject them separately). Git worktrees' .git files are supported.
    """
    if root is None:
        return git_root(pathlib.Path(os.getcwd()))
    requested = pathlib.Path(root).resolve()
    if (requested / ".git").exists():
        return requested  # own repository/worktree boundary; no git fork on cached trees
    enclosing = git_root(requested).resolve()
    if enclosing != requested:
        raise ValueError(
            f"explicit tree root {requested} is inside enclosing git root {enclosing}; "
            "refusing to widen the submitted tree. Use a repository rooted at the "
            "requested directory, or explicitly select the enclosing root."
        )
    return requested


def main_checkout(d: pathlib.Path) -> pathlib.Path:
    try:
        common = subprocess.run(["git", "-C", str(d), "rev-parse", "--path-format=absolute", "--git-common-dir"], capture_output=True, text=True, check=True).stdout.strip()
        return pathlib.Path(common).parent
    except Exception:
        return d


DEFAULT_IGNORE = ["**/node_modules/**", "**/__pycache__/**", "**/.venv/**", "**/runs/**", "runs/**"]


class Project:
    """A repo as the supercomputer sees it: its root (what's shipped), its config and its hooks."""

    def __init__(self, root: pathlib.Path, cfg: dict | None = None, cfg_path: pathlib.Path | None = None):
        self.root = pathlib.Path(root).resolve()
        self.cfg = cfg or {}
        self.cfg_path = cfg_path
        c, b = self.cfg, self.cfg.get("boat", {})
        self.name = c.get("name") or self.root.name
        self.prefix = b.get("prefix") or "".join(ch for ch in self.name.lower() if ch.isalnum())[:10] or "sc"
        self.final_prefix = b.get("final_prefix") or f"{self.prefix}-img"
        self.image_salt = b.get("image_salt") or f"{self.prefix}-image-v1"
        self.vm_type = env("VM", b.get("vm_type", "large"))
        self.path = [p.replace("~", "$HOME") for p in b.get("path", [])]
        self.env = {str(k): str(v) for k, v in b.get("env", {}).items()}  # (every item's on boat: e.g. a JAVA_HOME the image has, before its older javac)
        self.mounts = list(b.get("mounts", []))
        self.ignore = list(b.get("ignore", DEFAULT_IGNORE))
        self.images = b.get("images", {})
        self.caches = [dict(x) for x in c.get("cache", [])]
        self.modal = c.get("modal", {})
        self.gpu = c.get("gpu", {})
        self.hooks = None
        h = c.get("hooks")
        if h:
            p = (cfg_path.parent if cfg_path else self.root) / h
            if p.exists():
                spec = importlib.util.spec_from_file_location(f"sc_hooks_{hashlib.sha256(str(p).encode()).hexdigest()[:8]}", p)
                self.hooks = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(self.hooks)

    def hook(self, name: str, default=None):
        return getattr(self.hooks, name, default) if self.hooks else default

    # -- the image
    def layer_scripts(self) -> list[tuple[str, str]]:
        f = self.hook("layers")
        if f:
            return list(f(self.root))
        return [(l["name"], l.get("script", "")) for l in self.cfg.get("boat", {}).get("layers", [])] or [("base", BASE_LAYER)]

    # -- the caches
    def cache_key(self, cache: dict, read=None) -> str:
        """sha256 of the salt and every input (module name, then its bytes), 32 hex digits: one key per version of
        the sources (and the salt names the toolchain)."""
        h = hashlib.sha256(cache.get("salt", "").encode())
        for m in cache.get("modules", []):
            rel = f"{cache['dir']}/{cache['inputs'].format(m=m)}"
            if read:
                data = read(rel)
            else:
                p = self.root / rel
                data = p.read_bytes() if p.exists() else b""
            h.update(m.encode() + b"\0" + (data or b"") + b"\0")
        return h.hexdigest()[:32]

    def cache_outputs(self, cache: dict) -> list[str]:
        return [cache["outputs"].format(m=m) for m in cache.get("modules", [])]


BASE_LAYER = r"""
sudo apt-get update -y -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q curl unzip git ca-certificates xz-utils zstd bzip2 coreutils python3-pip build-essential
"""


def find_config(root: pathlib.Path) -> pathlib.Path | None:
    if os.environ.get("SUPERCOMPUTER_CONFIG"):
        p = pathlib.Path(os.environ["SUPERCOMPUTER_CONFIG"])
        return p if p.exists() else None
    for d in (root, main_checkout(root)):
        p = d / "supercomputer.toml"
        if p.exists():
            return p
    # a repo the tool knows (its config kept here, for checkouts that don't carry it yet): by its main checkout's name
    p = HOME / "repos" / f"{main_checkout(root).name}.toml"
    return p if p.exists() else None


_current: Project | None = None


def use(root: pathlib.Path | str | None = None) -> Project:
    """Select a project without silently widening an explicit tree boundary."""
    global _current
    r = select_root(root)
    p = find_config(r)
    cfg = tomllib.loads(p.read_text()) if p else {}
    _current = Project(r, cfg, p)
    return _current


def project() -> Project:
    return _current or use()


def set_project(p: Project):
    global _current
    _current = p


@contextlib.contextmanager
def private_umask():
    old = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(old)


def live_dir() -> pathlib.Path:
    LIVE.mkdir(parents=True, exist_ok=True)
    return LIVE


