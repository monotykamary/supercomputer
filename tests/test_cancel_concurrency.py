"""11-16 ~13:08-13:20 on the controller: (1) the client's strict /close and the cleanup worker's retry cancelled one batch
at once; the first retired it (admission renamed to its closed archive), the second's fence write raised
FileNotFoundError: health's errors, the canary handing a sound code back. (2) A strict /close waited on its workers'
cancel RPCs while holding the request lock every HTTP mutation takes: submissions, /cache/have and /blobs/have queued
behind it (191 request threads), nothing admitted. Run on boat, like all test_*.py."""
import threading
import unittest

import test_owned_scheduler as fixture


class CancelConcurrency(unittest.TestCase):
    setUp = fixture.OwnedSchedulerTests.setUp
    write_batch = fixture.OwnedSchedulerTests.write_batch
    place_without_thread = fixture.OwnedSchedulerTests.place_without_thread
    good_receipt = fixture.OwnedSchedulerTests.good_receipt

    def test_two_cancels_of_one_batch_both_return_its_receipt(self):
        self.place_without_thread()
        entered, release = threading.Event(), threading.Event()
        calls = []

        def rpc(*args, **kwargs):
            calls.append(threading.current_thread().name)
            if threading.current_thread().name == "slow":
                entered.set()
                release.wait(5)
            return self.good_receipt()
        self.vm.client.rpc.side_effect = rpc
        out, errors = {}, []

        def slow():
            try:
                out["slow"] = self.s.cancel(self.b.id)
            except Exception as e:  # (before: FileNotFoundError from the fence write)
                errors.append(e)
        t = threading.Thread(target=slow, name="slow")
        t.start()
        self.assertTrue(entered.wait(5))
        out["fast"] = self.s.cancel(self.b.id)
        release.set()
        t.join(10)
        self.assertEqual(errors, [])
        self.assertTrue(out["fast"]["cleanup_complete"])
        self.assertTrue(out["slow"]["cleanup_complete"], out["slow"])

    def test_cancel_rpcs_run_outside_the_request_lock(self):
        self.place_without_thread()
        self.s._request_lock = threading.RLock()
        free = []

        def rpc(*args, **kwargs):
            got = []

            def try_lock():  # (another request thread: can it take the request lock meanwhile?)
                ok = self.s._request_lock.acquire(timeout=1)
                got.append(ok)
                if ok:
                    self.s._request_lock.release()
            probe = threading.Thread(target=try_lock)
            probe.start()
            probe.join(3)
            free.append(bool(got and got[0]))
            return self.good_receipt()
        self.vm.client.rpc.side_effect = rpc
        with self.s._request_lock:  # (as the HTTP handler holds it for /close)
            receipt = self.s.close(self.b.id)
            self.assertTrue(self.s._request_lock._is_owned())  # (held again when close returns)
        self.assertEqual(free, [True])
        self.assertTrue(receipt["cleanup_complete"])


if __name__ == "__main__":
    unittest.main()
