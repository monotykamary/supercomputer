"""11-17: ~30 retained strict batches whose VMs boat had long stopped or deleted asked for a cancel receipt every
minute for days (14,506 journal lines in one), never closed: no agent of their boot was left to answer. boat's
inventory saying that boot is over (the VM stopped, or absent from a complete listing) is the evidence
(rules.boot_ended); a VM still running, or a partial listing, is not. Run on boat, like all test_*.py."""
import unittest
from unittest.mock import patch

import test_owned_scheduler as fixture
from test_owned_scheduler import S


class BootEndedCleanup(unittest.TestCase):
    setUp = fixture.OwnedSchedulerTests.setUp
    write_batch = fixture.OwnedSchedulerTests.write_batch
    place_without_thread = fixture.OwnedSchedulerTests.place_without_thread

    def gone(self, listing):
        self.place_without_thread()
        # (a restart later: the VM is no longer held, the item's last placement names it)
        self.s.vms.pop(self.vm.id)
        self.vm.dead = True
        self.s._inv_cache = None
        return patch.object(S.boat, "sandboxes", return_value=listing)

    def listing(self, vms, complete=True):
        out = S.boat.Listing(vms)
        out.complete = complete
        return out

    def test_deleted_vm_completes_cleanup(self):
        with self.gone(self.listing({})):
            r = self.s.cancel(self.b.id, 0)
        self.assertTrue(r["cleanup_complete"], r)
        self.assertFalse(self.vm.client.rpc.called)
        self.assertNotIn(self.b.id, self.s.batches)

    def test_stopped_vm_completes_cleanup(self):
        with self.gone(self.listing({self.vm.id: {"id": self.vm.id, "state": "stopped"}})):
            r = self.s.cancel(self.b.id, 0)
        self.assertTrue(r["cleanup_complete"], r)

    def test_running_vm_or_partial_listing_is_no_evidence(self):
        with self.gone(self.listing({self.vm.id: {"id": self.vm.id, "state": "running"}})):
            r = self.s.cancel(self.b.id, 0)
        self.assertFalse(r.get("complete") or r.get("cleanup_complete"), r)
        self.s._inv_cache = None
        with patch.object(S.boat, "sandboxes", return_value=self.listing({}, complete=False)):
            r = self.s.cancel(self.b.id, 0)
        self.assertFalse(r.get("complete") or r.get("cleanup_complete"), r)
        self.s._inv_cache = None
        with patch.object(S.boat, "sandboxes", side_effect=RuntimeError("boat down")):
            r = self.s.cancel(self.b.id, 0)
        self.assertFalse(r.get("complete") or r.get("cleanup_complete"), r)

    def test_rule(self):
        R = S.rules
        assert R.boot_ended(False, True, False) and R.boot_ended(True, True, True) and R.boot_ended(True, False, True)
        assert not R.boot_ended(False, False, False) and not R.boot_ended(True, True, False)


if __name__ == "__main__":
    unittest.main()
