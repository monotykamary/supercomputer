"""Client-only durable placement contract; all RPC and transport calls are fakes."""
import contextlib
import io
import json
import pathlib
import sys
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import boat, scheduler

CAPS = {"distinct_vm_group": True, "distinct_vm_group_protocol": 1,
        "distinct_vm_group_scope": "durable", "durable_distinct_vm_group_protocol": 1}
FIELDS = {"distinct_vm_group": "experiment/rep", "distinct_vm_group_scope": "durable",
          "distinct_vm_group_generation": "epoch-1"}


def session(caps=CAPS, replies=None):
    s = object.__new__(boat.Scheduled)
    s.client = Mock()
    responses = iter(replies or [{"ok": True}])
    def req(path, msg, **kw):
        if path == "/capabilities":
            return caps
        if path == "/submit":
            return next(responses)
        if path == "/poll":
            return {"done": True, "rows": [{"name": "one", "code": 0}]}
        raise AssertionError(path)
    s.client.req.side_effect = req
    s.client.remote = None
    s.tree = SimpleNamespace(id="tree", files={}, links={}, gits={})
    s.caches, s.proj = [], SimpleNamespace(cache_outputs=lambda c: [])
    s._bid_lock, s.bids, s.batch = threading.Lock(), [], "batch"
    s.klass, s.label, s.image, s.vm_type = 2, "test", "img", "large"
    s.ctx_b64, s.warm_only = "", False
    s.stop, s.vm_ids, s.run_names = threading.Event(), set(), {}
    return s


class DurableClient(unittest.TestCase):
    def items(self, fields=FIELDS, hook=None):
        proj = SimpleNamespace(images={}, hook=lambda name: hook)
        with patch.object(boat, "project", return_value=proj):
            return boat.job_items([{"name": "one", "cmd": "true", **fields}], "", lambda c: c, "large", {})[0]

    def test_hook_cannot_erase_durable_identity(self):
        items = self.items(hook=lambda it, j: {"name": it["name"], "cmd": it["cmd"]})
        for key, value in FIELDS.items():
            self.assertEqual(items[0].get(key), value)

    def test_hook_sees_durable_identity(self):
        def hook(it, job):
            for key, value in FIELDS.items():
                self.assertEqual(it.get(key), value)
            return it
        self.items(hook=hook)

    def test_unpinned_generation_is_not_invented_client_side(self):
        fields = {k: v for k, v in FIELDS.items() if k != "distinct_vm_group_generation"}
        item = self.items(fields)[0]
        self.assertNotIn("distinct_vm_group_generation", item)
        s = session()
        s.run([item], Mock())
        msg = next(c.args[1] for c in s.client.req.call_args_list if c.args[0] == "/submit")
        self.assertNotIn("distinct_vm_group_generation", msg["items"][0])

    def test_invalid_direct_item_refused_before_transport(self):
        s = session()
        with self.assertRaises(ValueError):
            s.run([{"name": "one", **FIELDS, "distinct_vm_group_generation": ""}], Mock())
        s.client.req.assert_not_called()
        s.client.push_tree.assert_not_called()

    def test_lifecycle_no_scheduler_does_not_start_one(self):
        with patch.object(scheduler, "current", return_value=None), patch.object(scheduler, "ensure_running") as spawn:
            with self.assertRaises(boat.Unavailable):
                boat.distinct_group("open", "g")
            spawn.assert_not_called()

    def test_cli_preserves_refusal_and_nonzero_exit(self):
        reply = {"ok": False, "error": "expired generation"}
        with patch.object(boat, "distinct_group", return_value=reply), contextlib.redirect_stdout(io.StringIO()) as output:
            code = boat.distinct_groups_main(["close", "g", "--generation", "old", "--ttl-s", "0"])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(output.getvalue()), reply)

    def test_invalid_scope_and_generation(self):
        for fields in ({"distinct_vm_group_scope": "durable"},
                       {"distinct_vm_group": "g", "distinct_vm_group_scope": "batch"},
                       {"distinct_vm_group": "g", "distinct_vm_group_generation": "e"},
                       {**FIELDS, "distinct_vm_group_generation": " "},
                       {**FIELDS, "distinct_vm_group_generation": 1}):
            with self.subTest(fields=fields), self.assertRaises(ValueError):
                self.items(fields)

    def test_old_jobs_remain_batch_scoped(self):
        item = self.items({"distinct_vm_group": "g"})[0]
        self.assertNotIn("distinct_vm_group_scope", item)
        self.assertNotIn("distinct_vm_group_generation", item)

    def test_durable_cap_required_before_transport(self):
        for caps in ({}, {"distinct_vm_group": True, "distinct_vm_group_protocol": 1},
                     {**CAPS, "distinct_vm_group_scope": True},
                     {**CAPS, "durable_distinct_vm_group_protocol": 2}):
            s = session(caps)
            with self.subTest(caps=caps), self.assertRaises(boat.Unavailable):
                s.run([{"name": "one", **FIELDS}], Mock())
            s.client.push_tree.assert_not_called()
            self.assertFalse(any(c.args[0] == "/submit" for c in s.client.req.call_args_list))

    def test_missing_tree_retry_preserves_identity(self):
        s = session(replies=[{"missing": True}, {"ok": True}])
        s.run([{"name": "one", **FIELDS}], Mock())
        submits = [c.args[1] for c in s.client.req.call_args_list if c.args[0] == "/submit"]
        self.assertEqual(len(submits), 2)
        for msg in submits:
            self.assertEqual(msg["placement_contract"], 1)
            for key, value in FIELDS.items():
                self.assertEqual(msg["items"][0][key], value)

    def test_durable_lost_does_not_call_fallback(self):
        s = session()
        original = s.client.req.side_effect
        s.client.req.side_effect = lambda path, msg, **kw: ({"rows": [{"name": "one", "lost": True, "code": -1}], "done": True} if path == "/poll" else original(path, msg, **kw))
        done, lost = Mock(), Mock()
        s.run([{"name": "one", **FIELDS}], done, on_lost=lost)
        lost.assert_not_called()
        done.assert_called_once()

    def test_lifecycle_wrapper_and_cli(self):
        client = Mock()
        record = {"ok": True, "group": "g", "generation": "e", "active": True}
        client.req.side_effect = lambda path, msg: CAPS if path == "/capabilities" else record
        with patch.object(scheduler, "current", return_value=client), patch.object(scheduler, "ensure_running") as spawn:
            for op, kw in (("open", {}), ("get", {}), ("close", {"generation": "e", "ttl_s": 60})):
                self.assertEqual(boat.distinct_group(op, "g", **kw), record)
                self.assertEqual(client.req.call_args.args, ("/distinct-groups/" + op, {"group": "g", **kw}))
            with contextlib.redirect_stdout(io.StringIO()) as output:
                boat.distinct_groups_main(["close", "g", "--generation", "e", "--ttl-s", "60"])
            self.assertEqual(json.loads(output.getvalue()), record)
            spawn.assert_not_called()

    def test_lifecycle_refuses_invalid_close_and_old_server(self):
        for kw in ({"generation": "", "ttl_s": 0}, {"generation": "e", "ttl_s": -1},
                   {"generation": "e", "ttl_s": float("inf")}, {"generation": "e", "ttl_s": float("nan")}):
            with self.assertRaises(ValueError):
                boat.distinct_group("close", "g", **kw)
        client = Mock()
        client.req.return_value = {"distinct_vm_group": True, "distinct_vm_group_protocol": 1}
        with patch.object(scheduler, "current", return_value=client), self.assertRaises(boat.Unavailable):
            boat.distinct_group("open", "g")
        self.assertEqual(client.req.call_count, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
