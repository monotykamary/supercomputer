"""Read-only batch fetch contract; run on supercomputer, never a live admission."""
import contextlib
import copy
import hashlib
import http.client
import http.server
import io
import json
import pathlib
import sys
import tempfile
import tarfile
import threading
import types
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from supercomputer import batchfetch as F, scheduler as S, resultfiles
from supercomputer.boat import AgentClient
from test_http_admission import handler_class

BID = 'batch-fetch-fixture'


class Response(io.BytesIO):
    def __init__(self, data, headers):
        super().__init__(data)
        self.headers = headers

    def getheader(self, name, default=None):
        return self.headers.get(name, default)

    def read(self, count=-1):
        return super().read(min(count, 997) if count >= 0 else 997)


class FakeClient:
    """Only allow fetch's read-only wire surface; all payloads use the real Store."""
    def __init__(self, root):
        self.store = resultfiles.Store(root / 'server-payloads')
        self.result_ref = self.store.put_json({'large': 'x' * 18000})
        self.row_ref = self.store.put_json({'name': 'one', 'extra': 'y' * 17000})
        source = root / 'binary'
        self.file_bytes = bytes(range(256)) * 40
        source.write_bytes(self.file_bytes)
        self.file_ref = self.store.put_file(source, format='file')
        self.manifest = self.store.put_json([dict(path='nested/data.bin', **self.file_ref)])
        self.rows = [dict(name='one', result_ref=self.result_ref, row_ref=self.row_ref,
                          output_files_ref=self.manifest), dict(name='two', result=42)]
        self.info = dict(ok=True, batch=BID, generation='admission-one', admitted_at=123,
                         items=2, rows=2, complete=True, closed=False)
        self.caps = {'batch_fetch_protocol': 1}
        self.calls = []
        self.log_bytes = b'opaque archive bytes: never extract ../escape' * 100
        self.log_headers = {'X-SC-Content-SHA256': hashlib.sha256(self.log_bytes).hexdigest(),
                            'Content-Length': str(len(self.log_bytes))}
        self.pages = None
        self.page_headers = {}
        self.corrupt_ref = None

    def req(self, path, msg, timeout=60, raw=False, decode=None):
        self.calls.append((path, copy.deepcopy(msg), raw, decode is not None))
        if path == '/capabilities':
            return copy.deepcopy(self.caps)
        if path == '/batch/info':
            return copy.deepcopy(self.info)
        if path == '/batch/rows':
            assert msg['batch'] == BID
            n = msg['since']
            if self.pages is not None:
                page = copy.deepcopy(self.pages[n])
            else:
                page = dict(rows=self.rows[n:n + 1], next_since=min(n + 1, len(self.rows)),
                            done=n >= len(self.rows))
            if decode is None:
                return page
            data = json.dumps(page).encode()
            headers = {'X-SC-Content-SHA256': hashlib.sha256(data).hexdigest(),
                       'Content-Length': str(len(data)), **self.page_headers}
            return decode(Response(data, headers))
        if path == '/results':
            assert raw and msg['batch'] == BID
            assert 0 < msg['length'] <= resultfiles.CHUNK_BYTES
            if msg['ref'] == self.corrupt_ref:
                return b''
            return self.store.read(msg['ref'], msg['offset'], min(msg['length'], 997))
        if path == '/logs':
            assert msg == {'batch': BID, 'vms': [], 'require_complete': True} and decode is not None
            return decode(Response(self.log_bytes, self.log_headers))
        raise AssertionError('mutating/provider request: ' + path)


class BatchFetchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = pathlib.Path(self.temp.name)
        self.out = self.root / 'out'
        self.client = FakeClient(self.root)
        # Any attempt to bootstrap a scheduler is a contract violation.
        for name in ('ensure_running', 'connect'):
            if hasattr(S, name):
                self.enterContext(patch.object(S, name, side_effect=AssertionError(name)))

    def fetch(self):
        return F.fetch(BID, self.out, client=self.client, timeout=5)

    def assert_unpublished(self):
        self.assertFalse((self.out / 'fetch.json').exists())
        self.assertFalse((self.out / 'rows.jsonl').exists())
        if self.out.exists():
            self.assertFalse(list(self.out.rglob('.fetch-*')))

    def test_closed_audit_payloads_are_verified_and_downloaded(self):
        from supercomputer import history
        self.client.info['closed'] = True
        refs = {name: self.client.store.put_json({'audit': name}) for name in history.AUDIT_NAMES}
        self.client.info['audit'] = refs
        result = self.fetch()
        self.assertEqual(set(result['audit']), set(refs))
        for name, item in result['audit'].items():
            data = pathlib.Path(item['file']).read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(), refs[name]['sha256'])
            self.assertEqual(json.loads(data), {'audit': name})
        self.assertTrue(result['read_only'])
        self.assertTrue(all(p in ('/capabilities','/batch/info','/batch/rows','/results','/logs')
                            for p, *_ in self.client.calls))

    def test_pages_short_chunks_and_all_reference_kinds(self):
        before = copy.deepcopy(self.client.info)
        result = self.fetch()
        self.assertTrue(result['read_only'])
        self.assertFalse(result['closed_by_fetch'])
        self.assertEqual(self.client.info, before)
        self.assertEqual(result['rows'], 2)
        data = (self.out / 'rows.jsonl').read_bytes()
        self.assertEqual(hashlib.sha256(data).hexdigest(), result['rows_sha256'])
        rows = [json.loads(line) for line in data.splitlines()]
        self.assertEqual([row['name'] for row in rows], ['one', 'two'])
        for key, ref in [('result_file', self.client.result_ref), ('row_file', self.client.row_ref)]:
            self.assertEqual(hashlib.sha256(pathlib.Path(rows[0][key]).read_bytes()).hexdigest(), ref['sha256'])
        self.assertEqual((self.out / 'one.files/nested/data.bin').read_bytes(), self.client.file_bytes)
        self.assertEqual((self.out / 'logs.tar.gz').read_bytes(), self.client.log_bytes)
        self.assertEqual(json.loads((self.out / 'fetch.json').read_text()), result)
        self.assertEqual(len(result['pages']), 3)
        for page in result['pages']:
            self.assertEqual(hashlib.sha256(pathlib.Path(page['file']).read_bytes()).hexdigest(), page['sha256'])
            self.assertTrue(page['verified'])
        self.assertGreater(sum(p == '/results' for p, *_ in self.client.calls), 20)
        self.assertEqual([m['since'] for p, m, *_ in self.client.calls if p == '/batch/rows'], [0, 1, 2])
        self.assertEqual({p for p, *_ in self.client.calls},
                         {'/capabilities', '/batch/info', '/batch/rows', '/results', '/logs'})

    def test_missing_and_incomplete_fail_before_creating_output(self):
        for info in ({'ok': False, 'batch': BID}, dict(self.client.info, complete=False),
                     dict(self.client.info, rows=1)):
            with self.subTest(info=info):
                self.client.info = info
                with self.assertRaises(RuntimeError):
                    self.fetch()
                self.assertFalse(self.out.exists())
                self.assertFalse(any(p in ('/batch/rows', '/logs') for p, *_ in self.client.calls))

    def test_unknown_protocol_is_refused(self):
        for protocol in (None, 0, 2, True, '1'):
            with self.subTest(protocol=protocol):
                self.client.caps = {'batch_fetch_protocol': protocol}
                with self.assertRaisesRegex(RuntimeError, 'batch_fetch_protocol'):
                    self.fetch()
                self.assertFalse(self.out.exists())
        self.assertTrue(all(p == '/capabilities' for p, *_ in self.client.calls))

    def test_duplicate_cursor_missing_rows_and_unfinished_pages(self):
        cases = [
            {0: dict(rows=[{'name': 'x'}, {'name': 'x'}], next_since=2)},
            {0: dict(rows=[{'name': 'x'}], next_since=2)},
            {0: dict(rows=[{'name': 'x'}], next_since=True)},
            {0: dict(rows=[], done=True)},
            {0: dict(rows=[], done=False)},
            {0: dict(rows='invalid', done=True)},
            {0: dict(error='missing')},
        ]
        for i, pages in enumerate(cases):
            with self.subTest(pages=pages):
                self.out = self.root / ('bad-page-' + str(i))
                self.client.pages = pages
                with self.assertRaises((ValueError, RuntimeError)):
                    self.fetch()
                self.assert_unpublished()

    def test_log_sha_length_and_truncation_fail_without_final_archive(self):
        headers = dict(self.client.log_headers)
        for i, changes in enumerate(({'X-SC-Content-SHA256': '0' * 64},
                                     {'X-SC-Content-SHA256': ''},
                                     {'Content-Length': str(len(self.client.log_bytes) + 1)},
                                     {'Content-Length': '1'}, {'Content-Length': 'bad'})):
            with self.subTest(changes=changes):
                self.out = self.root / ('bad-log-' + str(i))
                self.client.log_headers = dict(headers, **changes)
                with self.assertRaises(ValueError):
                    self.fetch()
                self.assert_unpublished()
                self.assertFalse((self.out / 'logs.tar.gz').exists())

    def test_row_page_sha_and_byte_count_are_checked_before_parsing(self):
        for i, changes in enumerate(({'X-SC-Content-SHA256': '0' * 64},
                                     {'X-SC-Content-SHA256': ''},
                                     {'Content-Length': '1'},
                                     {'Content-Length': str(resultfiles.POLL_BYTES + resultfiles.INLINE_BYTES + 1025)})):
            with self.subTest(changes=changes):
                self.out = self.root / ('bad-page-stream-' + str(i))
                self.client.page_headers = changes
                with self.assertRaises(ValueError):
                    self.fetch()
                self.assert_unpublished()
                self.assertFalse(any(p == '/results' for p, *_ in self.client.calls))
                self.client.calls.clear()

    def test_reference_truncation_never_publishes_file(self):
        self.client.corrupt_ref = self.client.file_ref
        with self.assertRaises(ValueError):
            self.fetch()
        self.assert_unpublished()
        self.assertFalse((self.out / 'one.files/nested/data.bin').exists())
        self.assertFalse((self.out / '.payloads' / (self.client.file_ref['sha256'] + '.payload')).exists())

    def test_resume_reuses_verified_payloads_and_rejects_other_generation(self):
        good = dict(self.client.log_headers)
        self.client.log_headers['X-SC-Content-SHA256'] = '0' * 64
        with self.assertRaises(ValueError):
            self.fetch()
        self.assert_unpublished()
        self.client.log_headers = good
        self.client.calls.clear()
        self.fetch()
        self.assertFalse(any(p == '/results' for p, *_ in self.client.calls))
        self.client.info['generation'] = 'admission-two'
        self.client.calls.clear()
        with self.assertRaisesRegex(ValueError, 'different admission'):
            self.fetch()
        self.assertFalse(any(p in ('/results', '/batch/rows', '/logs') for p, *_ in self.client.calls))

    def test_resume_rejects_same_length_corrupt_cached_json(self):
        good = dict(self.client.log_headers)
        for kind in ('result_ref', 'row_ref'):
            with self.subTest(kind=kind):
                self.out = self.root / ('corrupt-' + kind)
                self.client.log_headers = dict(good, **{'X-SC-Content-SHA256': '0' * 64})
                with self.assertRaises(ValueError):
                    self.fetch()
                ref = getattr(self.client, kind)
                payload = self.out / '.payloads' / (ref['sha256'] + '.payload')
                original = payload.read_bytes()
                payload.write_bytes(b'!' + original[1:])
                self.client.log_headers = good
                self.client.calls.clear()
                with self.assertRaisesRegex(ValueError, 'SHA-256 mismatch'):
                    self.fetch()
                self.assert_unpublished()
                self.assertFalse(any(p == '/results' for p, *_ in self.client.calls))
                self.assertEqual(payload.read_bytes(), b'!' + original[1:])

    def test_invalid_inventory_and_admission_time_are_refused(self):
        good = dict(self.client.info)
        for field, value in (('items', True), ('rows', -1), ('generation', ''),
                             ('admitted_at', True), ('admitted_at', float('nan')),
                             ('admitted_at', float('inf')), ('admitted_at', '123')):
            with self.subTest(field=field, value=value):
                self.client.info = dict(good, **{field: value})
                with self.assertRaises((ValueError, RuntimeError)):
                    self.fetch()
                self.assertFalse(self.out.exists())

    def test_admission_change_during_fetch_never_publishes_rows(self):
        original = self.client.req
        count = 0
        def request(path, *args, **kwargs):
            nonlocal count
            value = original(path, *args, **kwargs)
            if path == '/batch/info':
                count += 1
                if count == 2:
                    value['generation'] = 'replacement'
            return value
        self.client.req = request
        with self.assertRaisesRegex(RuntimeError, 'admission changed'):
            self.fetch()
        self.assert_unpublished()

    def test_failed_repeat_invalidates_old_success_receipt(self):
        self.fetch()
        self.assertTrue((self.out / 'fetch.json').exists())
        self.client.log_bytes = b'new archive snapshot'
        self.client.log_headers = {
            'X-SC-Content-SHA256': hashlib.sha256(self.client.log_bytes).hexdigest(),
            'Content-Length': str(len(self.client.log_bytes))}
        original, calls = self.client.req, 0
        def request(path, *args, **kwargs):
            nonlocal calls
            value = original(path, *args, **kwargs)
            if path == '/batch/info':
                calls += 1
                if calls == 2:
                    value['generation'] = 'changed'
            return value
        self.client.req = request
        with self.assertRaisesRegex(RuntimeError, 'admission changed'):
            self.fetch()
        self.assertFalse((self.out / 'fetch.json').exists())
        self.assertEqual((self.out / 'logs.tar.gz').read_bytes(), self.client.log_bytes)

    def test_complete_logs_reject_missing_streams_but_allow_unstarted_rows(self):
        owner = object.__new__(S.Scheduler)
        owner.batches = {BID: types.SimpleNamespace(rows=[
            {'name': 'job', 'vm': 'vm-fixture', 'run_name': 'job~s1'}])}
        directory = self.root / 'queue' / (BID + '.logs') / 'job~s1'
        with patch.object(S, 'DIR', self.root):
            with self.assertRaisesRegex(ValueError, 'logs incomplete'):
                owner.logs(BID, [], require_complete=True)
            directory.mkdir(parents=True)
            (directory / 'out').write_bytes(b'')
            with self.assertRaisesRegex(ValueError, 'job~s1/err'):
                owner.logs(BID, [], require_complete=True)
            (directory / 'err').write_bytes(b'')
            archive = owner.logs(BID, [], require_complete=True)
            with tarfile.open(fileobj=io.BytesIO(archive)) as saved:
                self.assertEqual(saved.getnames(), ['job~s1/out', 'job~s1/err'])
            with self.assertRaisesRegex(ValueError, 'no worker fallback'):
                owner.logs(BID, ['vm-fixture'], require_complete=True)
            open_archive = tarfile.open
            def disappear(*args, **kwargs):
                (directory / 'err').unlink()
                (directory / 'out').unlink()
                directory.rmdir()
                directory.parent.rmdir()
                return open_archive(*args, **kwargs)
            with patch.object(S.tarfile, 'open', side_effect=disappear):
                with self.assertRaisesRegex(ValueError, 'changed during snapshot'):
                    owner.logs(BID, [], require_complete=True)
            owner.batches[BID].rows = [{'name': 'never-started', 'code': -1}]
            with tarfile.open(fileobj=io.BytesIO(owner.logs(BID, [], require_complete=True))) as saved:
                self.assertEqual(saved.getnames(), [])

    def test_symlink_root_and_resume_paths_do_not_escape(self):
        outside = self.root / 'outside'
        outside.mkdir()
        sentinel = outside / 'sentinel'
        sentinel.write_bytes(b'untouched')
        for component in ('root', 'batch.json', '.fetch.lock', 'rows.jsonl', 'logs.tar.gz',
                          '.payloads', 'pages', 'one.files', 'nested', 'leaf'):
            with self.subTest(component=component):
                self.out = self.root / ('link-' + component.replace('/', '-'))
                if component == 'root':
                    self.out.symlink_to(outside, target_is_directory=True)
                else:
                    self.out.mkdir()
                    (self.out / 'batch.json').write_text(json.dumps({'batch': BID, 'generation': 'admission-one'}))
                    target = self.out / component
                    if component in ('nested', 'leaf'):
                        target = self.out / 'one.files/nested'
                        target.parent.mkdir()
                        if component == 'leaf':
                            target.mkdir()
                            target = target / 'data.bin'
                    if target.exists():
                        target.unlink()
                    target.symlink_to(outside if component in ('one.files', '.payloads', 'pages', 'nested') else sentinel,
                                      target_is_directory=component in ('one.files', '.payloads', 'pages', 'nested'))
                with self.assertRaises((ValueError, OSError)):
                    self.fetch()
                self.assertFalse((self.out / 'fetch.json').exists())
                self.assertEqual(sentinel.read_bytes(), b'untouched')
                self.assertEqual(sorted(p.name for p in outside.iterdir()), ['sentinel'])

    def test_no_current_scheduler_does_not_start_one(self):
        with patch.object(S, 'current', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'never starts'):
                F.fetch(BID, self.out)
        self.assertFalse(self.out.exists())

    def test_cli_dispatches_fetch_and_reports_failure(self):
        from supercomputer import __main__ as cli
        with patch.object(cli.config, 'use'), patch.object(F, 'fetch', return_value={'complete': True}) as fetch, \
             contextlib.redirect_stdout(io.StringIO()) as stdout:
            cli.main(['batch', 'fetch', BID, '--out', str(self.out), '--timeout', '7'])
        fetch.assert_called_once_with(BID, str(self.out), timeout=7.0)
        self.assertEqual(json.loads(stdout.getvalue()), {'complete': True})
        with patch.object(cli.config, 'use'), patch.object(F, 'fetch', side_effect=RuntimeError('incomplete')):
            with self.assertRaisesRegex(SystemExit, 'batch fetch: incomplete'):
                cli.main(['batch', 'fetch', BID, '--out', str(self.out)])

    def test_batch_info_is_read_only_in_memory(self):
        batch = types.SimpleNamespace(cache_generation='g', t0=123, items={'x': object()},
                                      rows=[{'name': 'x'}], closed=False, strict_once=True,
                                      admission_closed=False, retain_journals=False, left=lambda: 0)
        owner = object.__new__(S.Scheduler)
        owner.lock = threading.RLock()
        owner.batches = {BID: batch}
        before = dict(vars(batch))
        result = owner.batch_info(BID)
        self.assertEqual(vars(batch), before)
        self.assertEqual(result['generation'], 'g')
        self.assertTrue(result['complete'])
        self.assertFalse(result['closed'])
        self.assertTrue(result['journals_retained'])
        self.assertFalse(owner.batch_info('absent')['ok'])

    def test_batch_rows_never_renews_heartbeat_and_legacy_poll_still_does(self):
        owner = object.__new__(S.Scheduler)
        batch = types.SimpleNamespace(rows=[{'name': 'x', 'ok': True}],
            items={'x': types.SimpleNamespace(state='done')},
            cv=threading.Condition(), t_poll=123.0, left=lambda: 0)
        owner.batches, owner.stop = {BID: batch}, threading.Event()
        before = dict(vars(batch))
        self.assertEqual(owner.batch_rows(BID, 0)['rows'], batch.rows)
        self.assertTrue(owner.batch_rows(BID, 1)['done'])
        self.assertEqual(vars(batch), before)
        for cursor in (-1, True, '0'):
            with self.assertRaises(ValueError):
                owner.batch_rows(BID, cursor)
            self.assertEqual(vars(batch), before)
        owner.poll(BID, 0, 0)
        self.assertGreater(batch.t_poll, 123.0)

    def test_native_http_and_agent_decode_forwarding(self):
        fake = self.client
        class NoMutationLock:
            def __enter__(self):
                raise AssertionError('read-only route acquired mutation lock')
            def __exit__(self, *args):
                pass
        owner = types.SimpleNamespace(token='fixture-token', stop=threading.Event(),
                                      _request_lock=NoMutationLock(), _cpu_add=lambda *a: None,
                                      batch_of=S.Scheduler.batch_of)
        owner._h = lambda: types.SimpleNamespace(record_error=Mock(), record_client_error=Mock())
        owner.capabilities = lambda: fake.caps
        owner.batch_info = lambda bid: fake.info
        owner.batch_rows = lambda bid, since: fake.req('/batch/rows', dict(batch=bid, since=since))
        owner.result_part = lambda bid, ref, offset, length: fake.req('/results', dict(batch=bid, ref=ref, offset=offset, length=length), raw=True)
        owner.logs = lambda bid, vms, *, require_complete=False: fake.log_bytes if require_complete else b"unexpected legacy request"
        for name in ('submit', 'close', 'cancel'):
            setattr(owner, name, Mock(side_effect=AssertionError(name)))
        server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler_class(owner))
        server.daemon_threads = True
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            for mode in ('local', 'agent'):
                with self.subTest(mode=mode):
                    client = object.__new__(S.Client)
                    client.port, client.token, client.remote = server.server_port, owner.token, None
                    agent = None
                    if mode == 'agent':
                        agent = object.__new__(AgentClient)
                        agent.token, agent.cookie, agent.base, agent.query = owner.token, '', '', ''
                        agent.lock = threading.Lock()
                        agent._get = lambda timeout: http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=timeout)
                        agent._put_back = lambda conn: conn.close()
                        client.remote = agent
                    self.out = self.root / ('http-' + mode)
                    try:
                        result = F.fetch(BID, self.out, client=client, timeout=5)
                        self.assertTrue(result['logs']['verified'])
                        self.assertEqual(result['logs']['sha256'], hashlib.sha256(fake.log_bytes).hexdigest())
                    finally:
                        for conn in getattr(client, '_conns', []):
                            conn.close()
            for name in ('submit', 'close', 'cancel'):
                getattr(owner, name).assert_not_called()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())


if __name__ == '__main__':
    unittest.main()
