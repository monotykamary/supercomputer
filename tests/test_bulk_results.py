"""Bounded scheduler ingestion, paging and result lifetime (no live VMs)."""
import base64
import json
import pathlib
import tempfile
import unittest
from unittest.mock import Mock, patch
from test_wiring import S, batch, sched, vm
from supercomputer import resultfiles, resultjson


class BulkResults(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.patch = patch.object(S, 'DIR', self.root)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        (self.root / 'queue').mkdir()
        self.s = sched()

    def test_polls_are_contiguous_byte_bounded_and_not_prematurely_done(self):
        b = batch(self.s, n=500)
        for name, it in b.items.items():
            it.state = 'done'
            b.deliver({'name':name,'code':0,'result':'x'*5000})
        seen, since = [], 0
        while True:
            response = self.s.poll(b.id, since, 0)
            self.assertLessEqual(len(json.dumps(response, separators=(',', ':')).encode()), resultfiles.POLL_BYTES + 2048)
            rows = response['rows']
            self.assertLessEqual(len(rows), resultfiles.POLL_ROWS)
            seen.extend(r['name'] for r in rows)
            since += len(rows)
            self.assertEqual(response['next_since'], since)
            if response['done']:
                self.assertEqual(since, 500)
                break
            self.assertTrue(rows)
        self.assertEqual(seen, list(b.items))

    def test_large_delivered_row_is_complete_on_disk_not_in_retained_rows(self):
        b = batch(self.s)
        row = {'name':'i0','code':0,'result':{'value':'y'*300000}}
        b.deliver(row)
        self.assertNotIn('result', b.rows[0])
        ref = b.rows[0]['row_ref']
        store = self.s._result_store(b.id)
        with store.open(ref) as stream:
            self.assertEqual(json.load(stream), row)
        self.assertEqual(self.s._read_result_journal(self.root/'queue'/'b1.rows', False), b.rows)

    def test_output_files_are_imported_before_terminal_commit(self):
        source = resultfiles.Store(self.root/'worker')
        data = self.root/'binary'
        data.write_bytes(bytes(range(256))*2000)
        file_ref = source.put_file(data, format='file')
        manifest = source.put_json([{'path':'nested/artifact.bin', **file_ref}])
        b = batch(self.s)
        worker = vm(self.s, 'A')
        self.s._start_on = lambda *a: None
        self.assertTrue(self.s._place(b.items['i0'], worker))
        calls=[]
        def rpc(cmd, bid, name, sha, offset, count, **kw):
            self.assertEqual(cmd, 'resultpart')
            self.assertNotEqual(b.items['i0'].state, 'done')
            ref = manifest if sha==manifest['sha256'] else file_ref
            chunk=source.read(ref, offset, count)
            calls.append((offset, count))
            return {'data':base64.b64encode(chunk).decode(),'offset':offset,'bytes':ref['bytes'],'sha256':sha,'eof':offset+len(chunk)==ref['bytes']}
        worker.client=Mock()
        worker.client.rpc.side_effect=rpc
        self.s._finished_logs=Mock()
        self.assertTrue(self.s._finished(worker, {'batch':b.id,'name':'i0','code':0,'seconds':1,'output_files_ref':manifest}))
        self.assertEqual(b.items['i0'].state, 'done')
        self.assertTrue(calls)
        self.assertTrue(all(count<=resultfiles.CHUNK_BYTES for _,count in calls))
        with self.s._result_store(b.id).open(file_ref) as stream:
            self.assertEqual(stream.read(), data.read_bytes())

    def test_corrupt_transfer_keeps_placement_unacknowledged(self):
        b = batch(self.s)
        worker = vm(self.s, 'A')
        self.s._start_on = lambda *a: None
        self.s._place(b.items['i0'], worker)
        ref={'version':1,'format':'json','bytes':5,'sha256':'a'*64}
        worker.client=Mock()
        worker.client.rpc.return_value={'data':base64.b64encode(b'wrong').decode(),'offset':0,'bytes':5,'sha256':'a'*64}
        with self.assertRaises(ValueError):
            self.s._finished(worker, {'batch':b.id,'name':'i0','code':0,'result_ref':ref})
        self.assertIn((b.id,'i0'), worker.running)
        self.assertEqual(b.rows, [])

    def test_legacy_large_response_preserves_full_raw_row(self):
        import io
        original={'batch':'b1','name':'i0','code':0,'result':{'blob':'z'*300000}}
        response=resultjson.load_response(io.BytesIO(json.dumps({'done':[original],'running':[]}).encode()), self.s._result_store)
        row=response['done'][0]
        self.assertNotIn('result', row)
        with self.s._result_store('b1').open(row['row_ref']) as stream:
            self.assertEqual(json.load(stream), original)

    def test_strict_result_journal_keeps_corruption_quarantine(self):
        import io
        store=self.s._result_store('b1')
        for content in (b'\n', b'null\n', b'{"name":"i0"}'):
            with self.assertRaises(ValueError):
                resultjson.load_journal(io.BytesIO(content),store,True)
        self.assertEqual(resultjson.load_journal(io.BytesIO(b'\nnull\n'),store,False),[])

    def test_only_owned_polls_use_the_row_decoder(self):
        import io
        client=S.boat.AgentClient('test','https://localhost','secret',lease={})
        client.result_store=self.s._result_store
        for command in ('resultpart','vtick'):
            result={'data':'x'*100000} if command=='resultpart' else {
                'done':[{'batch':'b1','name':'i0','result':'x'*100000}], 'running':[]}
            reply={'ok':True,'result':result}
            wire=json.dumps(reply).encode()
            def req(*args, **kwargs):
                decode=kwargs.get('decode')
                return 200, decode(io.BytesIO(wire)) if decode else wire, 'application/json'
            with patch.object(client,'_req',side_effect=req):
                received=client.rpc('owned_rpc',{'command':command})
            if command=='resultpart':
                self.assertEqual(received,reply)
            else:
                row=received['result']['done'][0]
                self.assertNotIn('result',row)
                with self.s._result_store('b1').open(row['row_ref']) as source:
                    self.assertEqual(json.load(source),result['done'][0])

    def test_negative_poll_cursor_does_not_skip_or_finish(self):
        b=batch(self.s)
        b.items['i0'].state='done'
        b.deliver({'name':'i0','code':0})
        response=self.s.poll('b1', -1, 0)
        self.assertFalse(response['done'])
        self.assertEqual(response['rows'], [])


if __name__=='__main__': unittest.main()
