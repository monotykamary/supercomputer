"""Native-only 1 GiB regression. Run explicitly, not in the short unit gate."""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import resource
import shutil
import tempfile
from unittest.mock import Mock, patch
from test_wiring import S, batch, sched, vm
from supercomputer import agent, clientresults, resultfiles, resultjson

GIB = 1 << 30
CHUNK = 1 << 18

def rss():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1024 if os.uname().sysname == 'Linux' else 1)

class Joined:
    def __init__(self, prefix, source, suffix):
        self.parts = [io.BytesIO(prefix), source, io.BytesIO(suffix)]
        self.at = 0
    def read(self, n):
        assert 0 <= n <= 65536, n
        out = bytearray()
        while len(out) < n and self.at < len(self.parts):
            part = self.parts[self.at].read(n-len(out))
            if not part:
                self.at += 1
            else:
                out.extend(part)
        return bytes(out)

with tempfile.TemporaryDirectory(prefix='sc-gib-') as temporary:
    root=Path(temporary)
    job=root/'job'; job.mkdir()
    raw=job/'result.json'
    digest=hashlib.sha256()
    with raw.open('wb') as output:
        for part in (b'{"blob":"',):
            output.write(part); digest.update(part)
        block=b'x'*CHUNK
        left=GIB-len(b'{"blob":"')-len(b'"}')
        while left:
            piece=block[:min(left,CHUNK)]
            output.write(piece); digest.update(piece); left-=len(piece)
        output.write(b'"}');digest.update(b'"}')
    assert raw.stat().st_size==GIB
    baseline=rss()
    # An accidental whole decode/copy fails deterministically as well as the RSS assertion.
    resource.setrlimit(resource.RLIMIT_AS, (768<<20,768<<20))
    row={'batch':'b1','name':'i0','code':0,'seconds':1}
    agent._read_result(job,row)
    assert 'result' not in row and row['result_ref']['bytes']==GIB
    assert row['result_ref']['sha256']==digest.hexdigest()
    (job/'outputs').mkdir()
    os.link(raw,job/'outputs'/'artifact.json')
    agent._collect_outputs(job,row)
    manifest_ref=row['output_files_ref']
    controller=root/'controller'; (controller/'queue').mkdir(parents=True)
    with patch.object(S,'DIR',controller):
        s=sched(); b=batch(s); worker=vm(s,'A')
        def read_agent(cmd,bid,name,sha,offset,count,**kwargs):
            assert cmd=='resultpart' and count<=resultfiles.CHUNK_BYTES
            path=job/(sha+'.payload')
            with path.open('rb') as source:
                source.seek(offset); data=source.read(count)
            return {'offset':offset,'sha256':sha,'bytes':path.stat().st_size,'data':base64.b64encode(data).decode()}
        worker.client=Mock(); worker.client.rpc.side_effect=read_agent
        compact=s._fetch_result_files(worker,row)
        b.items['i0'].state='done'; b.deliver(compact)
        for _ in range(20):
            answer=s.poll('b1',0,0)
            assert len(json.dumps(answer))<4096
        restored=s._read_result_journal(controller/'queue'/'b1.rows',False)
        assert restored==b.rows
        downloaded=clientresults.download(b.rows[0],lambda ref,offset,count:s.result_part('b1',ref,offset,count),root/'client')
        final=Path(downloaded['output_files_dir'])/'artifact.json'
        got=hashlib.sha256()
        with final.open('rb') as source:
            for data in iter(lambda:source.read(CHUNK),b''):got.update(data)
        assert final.stat().st_size==GIB and got.hexdigest()==digest.hexdigest()
        # Old workers must not bypass the bound through an inline JSON response.
        with raw.open('rb') as source:
            wire=Joined(b'{"done":[{"batch":"b2","name":"i0","code":0,"result":',source,b'}],"running":[]}')
            legacy=resultjson.load_response(wire,s._result_store)
        assert 'result' not in legacy['done'][0]
        assert legacy['done'][0]['row_ref']['bytes']>GIB
    peak=rss(); delta=peak-baseline
    print(json.dumps({'payload_bytes':GIB,'baseline_rss_bytes':baseline,'peak_rss_bytes':peak,'rss_growth_bytes':delta,'max_allowed_growth_bytes':128<<20,'sha256':digest.hexdigest(),'output_equal':True,'legacy_inline_bounded':True}))
    assert delta<128<<20,(baseline,peak,delta)
