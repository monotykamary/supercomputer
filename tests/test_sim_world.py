"""Simulation fixture contracts; run on Boat with python3 tests/test_sim_world.py.

Named images are provider inventory, not a projection of currently running VMs.
The tests here make no provider calls and never launch the scheduler.
"""
import json
import pathlib
import sys
from types import SimpleNamespace

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tests.sim.harness import IMAGE, Sim


def test_snapshot_catalog():
    sim = Sim(str(pathlib.Path(__file__).resolve().parent.parent))
    try:
        names = lambda: {s["name"] for s in json.loads(sim.w.cli_snapshots([]))["named"]}
        assert not sim.w.vms
        assert names() == {IMAGE}, "a cold pool still has its configured image"
        vm = sim.w.warm_vm("other-image")
        assert names() == {IMAGE, "other-image"}
        vm.state = "deleted"
        assert names() == {IMAGE, "other-image"}, "deleting a VM does not delete its snapshot"
        sim.w.vms.clear()
        assert names() == {IMAGE, "other-image"}
        assert "missing-image" not in names(), "unknown images must not be fabricated"
        assert sim.w.cli_new(["new", "--from", "missing-image"]) == ("snapshot missing-image not found",)
        assert not sim.w.vms, "a missing snapshot must not create a worker"
    finally:
        sim.k.shutdown()
        sim.cleanup()


def waste_sample(*, ready=True, starting=False, stale=False):
    from tests.scenarios import _waste_watch

    sim = Sim(str(pathlib.Path(__file__).resolve().parent.parent))
    try:
        vm = sim.w.warm_vm(IMAGE)
        pending = {"start": SimpleNamespace(weight=4, state="starting" if starting else "running",
                   batch=SimpleNamespace(id="b"), run_name="start")} if starting or stale else {}
        held = SimpleNamespace(ready=ready, dead=False, upgrading=False, running=pending,
                               reserved=("ghost", sim.k.now + 600))
        sim.w.live_sched = lambda: (sim.k.world, SimpleNamespace(vms={vm.id: held}))
        sim.w.batches["b"] = {"t_end": None}
        sim.w.items[("b", "queued")] = {"submitted": sim.k.now, "delivered": None,
            "running": 0, "klass": "gate", "image": IMAGE, "timing": False, "weight": 1}
        rec = _waste_watch(sim, grace=0, every=10)
        sim.run(30)
        return rec
    finally:
        sim.k.shutdown()
        sim.cleanup()


def test_waste_availability():
    assert waste_sample()["worst"] == 1, "a ghost reservation on a ready VM must still be detected"
    assert waste_sample(ready=False)["worst"] == 0, "a bootstrapping VM is not placeable idle capacity"
    assert waste_sample(starting=True)["worst"] == 0, "a dispatched start already owns its slots"
    assert waste_sample(stale=True)["worst"] == 1, "a stale running claim without a process must not hide idle slots"


if __name__ == "__main__":
    test_snapshot_catalog()
    test_waste_availability()
    print("PASS snapshot catalog and waste availability contracts")
