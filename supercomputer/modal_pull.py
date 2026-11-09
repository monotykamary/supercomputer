"""A store pull from a Modal container, as a GPU job would pull a cache from boat (store.pull: ranged, resumed,
checked by sha256): the test of the mirror path.

    modal run modal_pull.py --name <store name> --url <served URL> [--only <glob>]
"""
import time

import modal

app = modal.App("supercomputer-pull-test")


@app.function(image=modal.Image.debian_slim(python_version="3.12").add_local_python_source("supercomputer"), scaledown_window=2, timeout=1800, cpu=2.0, memory=4096)
def pull(name: str, url: str, only: str = "") -> dict:
    import pathlib
    from supercomputer import store
    from supercomputer.rules.integrity import Manifest
    t0 = time.time()
    m = store.pull(name, "/tmp/out", url, pathlib.Path("/tmp/cache"), 8, only)
    size = sum(v[0] for v in m.values())
    differ = sum(1 for k, v in m.items() if store.sha256(pathlib.Path("/tmp/out") / k) != v[1])
    ok = Manifest().verify(differ, len(m), len(m))  # (every file again against its manifest's sha256)
    return {"files": len(m), "bytes": size, "seconds": round(time.time() - t0, 1), "sha256_ok": ok, "mb_s": round(size / 1e6 / max(1e-9, time.time() - t0), 1)}


@app.local_entrypoint()
def main(name: str, url: str, only: str = ""):
    print(pull.remote(name, url, only))
