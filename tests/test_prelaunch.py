"""Launch-audit parser regressions; run on the isolated Boat test gate."""
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import prelaunch
from supercomputer.rules.prelaunch import prelaunch_can_retry, prelaunch_not_started


class PrelaunchAuditTests(unittest.TestCase):
    def setUp(self):
        self.binding = dict(batch='batch', name='item', run_name='item', vm='vm',
                            boot_id='boot', launch_identity='nonce')
        self.raw = self.line(event='prelaunch_reserved', protocol=1)

    def line(self, **record):
        return (json.dumps({**self.binding, **record}) + '\n').encode()

    def append_failure(self, raw):
        proof = prelaunch.inspect(raw, self.binding)
        return raw + self.line(event='prelaunch_not_started', phase='prelaunch',
                               terminal=True, status='NOT_STARTED',
                               prelaunch_failure=proof['failures'] + 1, audit=proof)

    def test_complete_reserved_prefix_proves_no_start(self):
        p = prelaunch.inspect(self.raw, self.binding)
        self.assertTrue(prelaunch_not_started(p['complete'], p['reserved'], p['start_requested']))

    def test_start_request_without_response_already_forbids_retry(self):
        p = prelaunch.inspect(self.raw + self.line(event='start_requested'), self.binding)
        self.assertTrue(p['complete'])
        self.assertTrue(p['start_requested'])
        self.assertFalse(prelaunch_can_retry(True, True, p['start_requested'], True, False, False, 1, 3))

    def test_missing_torn_or_malformed_evidence_fails_closed(self):
        for raw in (b'', self.raw[:-1], self.raw + b'{', self.raw + b'{}\n', b'[]\n',
                    self.line(event='start_requested'), self.raw + b'\xff\n'):
            with self.subTest(raw=raw):
                self.assertFalse(prelaunch.inspect(raw, self.binding)['complete'])

    def test_duplicate_keys_and_unknown_identity_are_ambiguous(self):
        duplicate = self.raw.replace(b'"event": "prelaunch_reserved"',
                                     b'"event": "start_requested", "event": "prelaunch_reserved"')
        for raw in (duplicate, self.raw + b'{"name":"another"}\n',
                    self.line(event='prelaunch_reserved', protocol=True),
                    self.raw + self.line(event='prelaunch_not_started', audit='not an object')):
            self.assertFalse(prelaunch.inspect(raw, self.binding)['complete'])

    def test_bound_failure_prefixes_remain_complete(self):
        raw = self.append_failure(self.raw)
        raw = self.append_failure(raw)
        p = prelaunch.inspect(raw, self.binding)
        self.assertTrue(p['complete'])
        self.assertEqual(p['failures'], 2)
        self.assertFalse(p['start_requested'])
        self.assertTrue(prelaunch_can_retry(True, True, False, True, False, False, 2, 3))
        self.assertFalse(prelaunch_can_retry(True, True, False, True, False, False, 3, 3))

    def test_failure_after_request_and_corrupted_prefix_are_unknown(self):
        raw = self.append_failure(self.raw)
        self.assertFalse(prelaunch.inspect(raw.replace(b'prefix_sha256', b'wrong_sha256'), self.binding)['complete'])
        requested = self.raw + self.line(event='start_requested')
        self.assertFalse(prelaunch.inspect(self.append_failure(requested), self.binding)['complete'])

    def test_identity_changes_and_duplicate_request_are_unknown(self):
        for raw in (self.raw + self.line(event='start_requested', vm='other'),
                    self.raw + self.line(event='prelaunch_reserved', protocol=1),
                    self.raw + self.line(event='start_response'),
                    self.raw + self.line(event='start_requested') + self.line(event='start_requested')):
            self.assertFalse(prelaunch.inspect(raw, self.binding)['complete'])

    def test_unrelated_records_are_in_the_bound_prefix(self):
        raw = self.raw + self.line(event='start_requested', name='other', run_name='other')
        self.assertTrue(prelaunch.inspect(self.append_failure(raw), self.binding)['complete'])


if __name__ == '__main__':
    unittest.main()
