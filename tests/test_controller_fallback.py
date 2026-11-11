"""A client set to use the controller never starts this machine's scheduler when the controller doesn't answer."""
import json
import os
import pathlib
import sys
import tempfile

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
live = tempfile.mkdtemp(prefix="sc-fallback-")
os.environ["SUPERCOMPUTER_LIVE"] = live
from supercomputer import scheduler  # noqa: E402

started = []
scheduler.ensure_running = lambda: started.append(1) or "local"
scheduler._remote = lambda: None
pathlib.Path(live, "controller.json").write_text(json.dumps({"url": "https://example.invalid", "token": "t"}))


def run(env):
    for k in ("SUPERCOMPUTER_CONTROLLER", "SUPERCOMPUTER_CONTROLLER_FALLBACK", "SUPERCOMPUTER_LOCAL"):
        os.environ.pop(k, None)
    os.environ.update(env)
    started.clear()
    try:
        return scheduler.connect(), started[:]
    except SystemExit as e:
        return f"exit: {str(e)[:40]}", started[:]


r, s = run({"SUPERCOMPUTER_CONTROLLER": "1"})
assert r.startswith("exit: the controller on boat doesn't answer") and not s, (r, s)
r, s = run({"SUPERCOMPUTER_CONTROLLER": "1", "SUPERCOMPUTER_CONTROLLER_FALLBACK": "1"})
assert r.startswith("exit:") and not s, (r, s)  # old FALLBACK is no longer permission
r, s = run({})  # the file alone selects the controller
assert r.startswith("exit:") and not s, (r, s)
r, s = run({"SUPERCOMPUTER_LOCAL": "1"})
assert r == "local" and s == [1], (r, s)
pathlib.Path(live, "controller.json").unlink()
r, s = run({})
assert r == "local" and s == [1], (r, s)
print("ok")
