"""11-17: ~30 retained strict batches whose VMs boat had long stopped or deleted were asked for a cancel receipt
every minute for days (14,506 journal lines in one, a log line each a minute). No agent of their boot is left to
answer; their cleanup is never fabricated (strict_once_vm_loss: cleanup_complete stays false), but boat's inventory
saying those boots are over (rules.boot_ended) makes the cleanup worker ask hourly, not every minute. A running VM, a
partial or failed listing is not that. Run on boat, like all test_*.py."""
import time
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

    def test_gone_vm_is_never_a_fabricated_cleanup(self):
        with self.gone(self.listing({})):
            r = self.s.cancel(self.b.id, 0)
            self.assertFalse(r.get("cleanup_complete") or r.get("complete"), r)
            self.assertIn(self.b.id, self.s.batches)
            self.assertTrue(self.s._cleanup_unaskable(self.b))

    def test_stopped_vm_is_unaskable(self):
        with self.gone(self.listing({self.vm.id: {"id": self.vm.id, "state": "stopped"}})):
            self.s.cancel(self.b.id, 0)
            self.assertTrue(self.s._cleanup_unaskable(self.b))

    def test_running_vm_or_partial_listing_is_still_asked(self):
        with self.gone(self.listing({self.vm.id: {"id": self.vm.id, "state": "running"}})):
            self.s.cancel(self.b.id, 0)
            self.assertFalse(self.s._cleanup_unaskable(self.b))
        self.s._inv_cache = None
        with patch.object(S.boat, "sandboxes", return_value=self.listing({}, complete=False)):
            self.assertFalse(self.s._cleanup_unaskable(self.b))
        self.s._inv_cache = None
        with patch.object(S.boat, "sandboxes", side_effect=RuntimeError("boat down")):
            self.assertFalse(self.s._cleanup_unaskable(self.b))

    def test_abandon_worker_asks_hourly_not_every_minute(self):
        with self.gone(self.listing({})):
            self.s.cancel(self.b.id, 0)
            calls = []
            self.s.cancel = lambda *a, **k: calls.append(a) or {}
            self.s.close = lambda *a, **k: calls.append(a) or {}
            self.s.handing_off = False
            for _ in range(2):
                self.s._t_abandon = 0
                with patch.object(S.threading, "Thread", lambda target, **k: type("T", (), {"start": lambda self: target()})()):
                    self.s._abandon_tick()
            self.assertEqual(calls, [])
            self.assertGreater(self.s._abandon_quiet[self.b.id], time.time() + 3000)

    def test_rule(self):
        R = S.rules
        assert R.boot_ended(False, True, False) and R.boot_ended(True, True, True) and R.boot_ended(True, False, True)
        assert not R.boot_ended(False, False, False) and not R.boot_ended(True, True, False)


if __name__ == "__main__":
    unittest.main()
