"""Every client path to Modal through rules.fallback.lost_route: work reaches Modal only once the scheduler failed it 3
times on distinct VMs (its lost row's boat_fails), or when Modal owns it; boat couldn't take a batch (Unavailable: an
image, a tree, the scheduler) means boat again, never Modal (11-01 05:03: the treadmill's sweep chc 300:clausal, 1 item
on Modal for "boat couldn't run them"). `python3 tests/test_fallback_paths.py` (no boat, no Modal: both faked)."""
import os
import sys
import tempfile
import types

os.environ["SUPERCOMPUTER_LIVE"] = tempfile.mkdtemp(prefix="sc-fallback-")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from supercomputer import boat, fanout  # noqa: E402
from supercomputer.rules import fallback  # noqa: E402

fanout.time.sleep = lambda s: None  # (its waits between passes: none here)
fanout.fname = lambda fn: "fn"
THREE = "boat failed it 3 times (last: its row lost on bx_a)"


def test_lost_route():
    assert fallback.lost_route(False, 3, 3, True) == "modal"
    assert fallback.lost_route(False, 3, 3, False) == "failed"
    assert fallback.lost_route(False, 0, 3, True) == "boat"
    assert fallback.lost_route(True, 0, 3, True) == "modal"
    assert boat.boat_fails_of(THREE) == 3 and boat.boat_fails_of("the scheduler finished the batch without its row") == 0


class Fn:
    def __init__(self):
        self.modal = []

    def starmap(self, args, **k):
        self.modal += list(args)
        return iter([("modal", a) for a in args])

    def remote(self, *a):
        self.modal.append(a)
        return ("modal", a)


def autoex(boats):
    """An AutoEx whose BoatEx sessions are `boats` in turn (an exception class: boat couldn't take the batch)."""
    seq = iter(boats)

    def mk(*a, **k):
        b = next(seq)
        if isinstance(b, type) and issubclass(b, Exception):
            raise b("boat image sc-x isn't built")
        return b
    fanout.BoatEx = mk
    fanout.ModalCtx = lambda app: types.SimpleNamespace(__enter__=lambda: None)
    ex = object.__new__(fanout.AutoEx)
    ex.app, ex.modal, ex.bx, ex.where, ex.why, ex.reason = None, None, None, {"boat": 0, "modal": 0}, {}, None
    ex.fb = fallback.AutoFallback(3)
    ex._mk = lambda: mk()
    ex._modal = lambda: None
    try:
        ex.bx = ex._mk()
    except boat.Unavailable as e:
        ex.reason = str(e)
    return ex


class Boat:
    """A BoatEx: each pass, the calls in `lose` come back lost with `why`, once."""

    def __init__(self, lose: dict):
        self.lose = dict(lose)

    def starmap(self, fn, args, on_lost=None):
        for a in args:
            if a in self.lose:
                on_lost(a, self.lose.pop(a))
            else:
                yield ("boat", a)


def test_unavailable_never_reaches_modal():
    ex = autoex([boat.Unavailable] * 4)
    fn = Fn()
    out = list(ex.starmap(fn, [(1,), (2,)]))
    assert fn.modal == [] and all(isinstance(r, RuntimeError) for r in out), out


def test_a_call_lost_without_three_failures_goes_back_to_boat():
    fn = Fn()
    ex = autoex([Boat({(2,): "the scheduler finished the batch without its row", (3,): THREE})])
    out = list(ex.starmap(fn, [(1,), (2,), (3,)]))
    assert fn.modal == [(3,)], fn.modal
    assert ("boat", (2,)) in out and ("boat", (1,)) in out, out


def test_run_jobs_never_moves_a_refused_batch_to_modal():
    calls = {"modal": []}
    pf = iter([("boat image sc-x isn't built", {}), (None, {}), (None, {})])
    boat.preflight3 = lambda *a, **k: next(pf)
    fallback.preflight_on = lambda *a: True

    def boat_jobs(jobs, *a, on_lost=None, **k):
        got = {}
        for j in jobs:
            if j["name"] == "lost3":
                on_lost(j["name"], THREE)
            else:
                got[j["name"]] = {"code": 0, "seconds": 1, "out": "", "err": ""}
        return got, {}
    boat.boat_jobs = boat_jobs
    fanout.ModalCtx = lambda app: types.SimpleNamespace(__enter__=lambda s=None: None, __exit__=lambda *a: None)

    class Ctx:
        def __init__(self, app):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False
    fanout.ModalCtx = Ctx
    fanout.project = lambda: types.SimpleNamespace(images={})
    out = tempfile.mkdtemp()

    def modal_jobs(jobs, on_done):
        calls["modal"] += [j["name"] for j in jobs]
        return {j["name"]: {"code": 0, "seconds": 1, "out": "", "err": ""} for j in jobs}
    with __import__("contextlib").suppress(SystemExit):
        fanout.run_jobs(None, [{"name": "ok", "cmd": "true"}, {"name": "lost3", "cmd": "true"}], out, "", lambda c: c, modal_jobs, backend="auto")
    assert calls["modal"] == ["lost3"], calls


def test_the_treadmills_batch_sends_to_modal_only_three_failures():
    from supercomputer import treadmill as T
    T.TM = __import__("pathlib").Path(tempfile.mkdtemp())
    modal, rows = [], {}

    class Sess:
        scheduled, vms, stop = True, {}, types.SimpleNamespace(set=lambda: None)

        def ensure(self, n):
            pass

        def run(self, items, row, on_lost=None, max_wait=0):
            for it in items:
                if it["name"] == "three":
                    on_lost(it, THREE)
                elif it["name"] == "norow":
                    on_lost(it, "the scheduler finished the batch without its row")
                else:
                    row(it, {"code": 0})

        def collect(self):
            return {}

        def close(self):
            return {}
    boat.session = lambda *a, **k: Sess()
    T.on_modal = lambda todo, root, row: modal.extend(it["name"] for it in todo) or {}
    tm = types.SimpleNamespace(max_wait=0, modal=True, main=__import__("pathlib").Path("."))
    items = [{"name": n, "weight": 1} for n in ("ok", "three", "norow")]
    b = T.Batch(tm, "sweep x", "a", items, 1, lambda it, r: rows.__setitem__(it["name"], r), root=__import__("pathlib").Path("."))
    b.thread.join(10)
    assert modal == ["three"], modal
    assert rows["norow"].get("retry"), rows["norow"]


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_"):
            f()
            n += 1
            print(f"ok {k}")
    print(f"fallback paths: {n} of {n}")
