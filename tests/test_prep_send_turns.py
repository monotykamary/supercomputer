"""A VM's preparation never waits behind other VMs' large sends for what it can get at once. 2026-11-16 22:56-23:20:
gate 4qjq8b's 719 MB tree was placed on ~50 VMs at once (none a peer yet) and the controller sent it two at a time
(PREP_MAX) at 1-3 MB/s through boat's front; every other start queued in prep_sem behind them: p3's tc64tq waited
20 min in "starting" for a tree missing 12 small blobs, 113 _start_on threads blocked, and 4qjq8b's own later items
waited to be sent what a peer by then held whole. Now a small send goes at once, and a large one waiting its turn is
served by a peer that gained the files meanwhile. Run on boat, like all test_*.py."""
import hashlib
import pathlib
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from supercomputer import scheduler as S


class Agent:
    """A VM's agent as _prepare sees it: the tree missing its blobs until they are put or pulled."""

    def __init__(self, keys):
        self.keys, self.complete, self.calls, self.puts = list(keys), False, [], 0

    def rpc(self, cmd, *args, timeout=60, **_):
        self.calls.append(cmd)
        if cmd == "begin":
            return {"ok": False}
        if cmd == "tree":
            return {"ok": True} if self.complete else {"ok": False, "missing": self.keys}
        if cmd == "have":
            return {"missing": [] if self.complete else self.keys}
        if cmd == "pull":
            self.complete = True
            return {"ok": True, "missing": []}
        if cmd == "prep":
            return {"ok": True}
        raise AssertionError(cmd)

    def put(self, route, data):
        self.puts += 1
        self.complete = True
        return {"stored": len(self.keys)}


def vm_of(name, agent):
    return SimpleNamespace(id=name, lock=threading.Lock(), prepared=set(), preparing={}, trees=set(), caches=set(),
                           client=agent, dead=False, ready=True, failing=0, quarantine="")


class PrepSendTurns(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        blobs = pathlib.Path(tmp.name) / "blobs"
        blobs.mkdir()
        self.keys = []
        for i in range(12):
            data = f"blob {i}".encode()
            key = hashlib.sha256(data).hexdigest()
            (blobs / key).write_bytes(data)
            self.keys.append(key)
        for p in (patch.object(S, "BLOBS", blobs), patch.object(S, "PREP_PEER_POLL_S", 0.02, create=True)):
            p.start()
            self.addCleanup(p.stop)
        s = S.Scheduler.__new__(S.Scheduler)
        s.prep_sem, s.shed, s.stop, s.vms, s.agent_version = threading.BoundedSemaphore(2), False, threading.Event(), {}, "v"
        self.s = s
        # (both turns held: two other VMs' large sends in progress, as at 22:58)
        self.assertTrue(s.prep_sem.acquire(blocking=False) and s.prep_sem.acquire(blocking=False))
        self.addCleanup(lambda: [s.prep_sem.release() for _ in range(2 - s.prep_sem._value)])
        self.batch = SimpleNamespace(id="20261116-225818-tc64tq", strict_once=False, ctx_b64="", caches=[],
                                     tree={"id": "a657c6bb2a50", "files": {f"f{i}": k for i, k in enumerate(self.keys)},
                                           "links": {}, "gits": {}})

    def prepare(self, vm, within=5.0):
        error = []
        def run():
            try:
                self.s._prepare(self.batch, vm)
            except Exception as e:  # pragma: no cover - reported below
                error.append(e)
        th = threading.Thread(target=run, daemon=True)
        th.start()
        th.join(within)
        self.assertFalse(th.is_alive(), "preparation still waiting for a send turn")
        self.assertEqual(error, [])

    def test_a_small_send_never_waits_behind_large_ones(self):
        agent = Agent(self.keys)
        vm = vm_of("bx_urmssde3", agent)
        self.prepare(vm)
        self.assertEqual(agent.puts, 1)
        self.assertIn(self.batch.id, vm.prepared)
        self.assertEqual(self.s.prep_sem._value, 0)  # (it never took a turn, nor gave one back)

    def test_a_large_send_waiting_its_turn_pulls_from_a_peer_that_gained_the_tree(self):
        agent = Agent(self.keys)
        vm = vm_of("bx_8f4yedv5", agent)
        peer = vm_of("bx_a5ak5jy9", Agent([]))
        peer.client.url, peer.client.token = "https://peer", "t"
        self.s.vms = {"bx_8f4yedv5": vm, "bx_a5ak5jy9": peer}
        threading.Timer(0.1, lambda: peer.trees.add(self.batch.tree["id"])).start()
        with patch.object(S, "PREP_SMALL_BYTES", 0, create=True):
            self.prepare(vm)
        self.assertEqual(agent.puts, 0)
        self.assertEqual(agent.calls.count("pull"), 1)
        self.assertIn(self.batch.id, vm.prepared)

    def test_a_large_send_with_no_peer_waits_for_its_turn_and_gives_it_back(self):
        agent = Agent(self.keys)
        vm = vm_of("bx_v5z8yq93", agent)
        threading.Timer(0.1, self.s.prep_sem.release).start()
        with patch.object(S, "PREP_SMALL_BYTES", 0, create=True):
            self.prepare(vm)
        self.assertEqual(agent.puts, 1)
        self.assertEqual(self.s.prep_sem._value, 1)  # (the turn it was given, returned)


if __name__ == "__main__":
    unittest.main()
