"""One build-cache key, one immutable value. 2026-11-16: MLDR p2's RadixBinderExpansion, one Lean cache key
(supercomputer key 079119b6...): the controller held olean f9663bde (02:35), bx_3te75ff2 its own d56e67c0 (03:07),
bx_d5zp6qmn its own 5d216f81 (11:50); Lean wrote different bytes for identical inputs, every VM kept its own build
(a file there was never replaced) and the controller's /cache/put overwrote. Now the controller's first write wins,
verified by its bytes, and a VM's differing copy gives way to it. Run on boat, like all test_*.py."""
import hashlib
import io
import pathlib
import tarfile
import tempfile
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from supercomputer import agent as A
from supercomputer import scheduler as S


def tar_of(files: dict) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class CacheCoherence(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)

    def test_the_controller_keeps_its_first_write(self):
        d = self.root / "bapa-lean-v1-slot" / "key"
        self.assertEqual(S.cache_put(d, tar_of({"m.lcache": b"first"}))["kept"], ["m.lcache"])
        r = S.cache_put(d, tar_of({"m.lcache": b"second"}))
        self.assertEqual(r["refused"], ["m.lcache"])
        self.assertEqual((d / "m.lcache").read_bytes(), b"first")
        self.assertEqual(S.cache_put(d, tar_of({"m.lcache": b"first"}))["kept"], ["m.lcache"])
        self.assertEqual(sorted(p.name for p in d.iterdir()), ["m.lcache"])  # (no temporary left behind)

    def test_a_vm_copy_gives_way_only_to_the_authority(self):
        with patch.object(A, "ROOT", self.root):
            def put(data, authority=False):
                t = self.root / "in.tar.gz"
                t.write_bytes(tar_of({"m.lcache": data}))
                return A.cachein("c", "k", str(t), authority)
            put(b"own build")
            self.assertEqual(put(b"another build")["got"], [])
            self.assertEqual(A.cache_hashes("c", "k"), {"m.lcache": sha(b"own build")})
            self.assertEqual(put(b"authority", authority=True)["got"], ["m.lcache"])
            self.assertEqual(A.cache_hashes("c", "k"), {"m.lcache": sha(b"authority")})
            self.assertEqual(put(b"authority", authority=True)["got"], [])  # (the same bytes: nothing to do)

    def _sync(self, local_files, there, vm_tar=None):
        local = self.root / "live" / "c" / "k"
        local.mkdir(parents=True, exist_ok=True)
        for n, data in local_files.items():
            (local / n).write_bytes(data)
        s = S.Scheduler.__new__(S.Scheduler)
        s.prep_sem = __import__("threading").Semaphore(1)
        cl, vm = Mock(), Mock()
        vm.client.raw.return_value = vm_tar
        c = {"name": "c", "key": "k", "outputs": ["a.lcache", "b.lcache"]}
        names = sorted(local_files)
        s._cache_sync(cl, vm, c, local, names, there)
        return cl, vm, local

    def test_prepare_replaces_a_vm_copy_that_differs_from_the_authority(self):
        cl, _, _ = self._sync({"a.lcache": b"authority"}, {"a.lcache": sha(b"vm's own")})
        route, body = cl.put.call_args.args
        self.assertIn("authority=1", route)
        with tarfile.open(fileobj=io.BytesIO(body)) as tf:
            self.assertEqual(tf.extractfile("a.lcache").read(), b"authority")

    def test_prepare_sends_nothing_when_the_vm_already_holds_the_authoritys_bytes(self):
        cl, _, _ = self._sync({"a.lcache": b"authority"}, {"a.lcache": sha(b"authority")})
        cl.put.assert_not_called()

    def test_a_vm_build_no_one_published_becomes_the_keys_value(self):
        cl, vm, local = self._sync({}, {"b.lcache": sha(b"first build")}, tar_of({"b.lcache": b"first build"}))
        self.assertEqual((local / "b.lcache").read_bytes(), b"first build")
        cl.put.assert_not_called()  # (the VM's bytes are the value now: nothing to replace there)


if __name__ == "__main__":
    unittest.main()
