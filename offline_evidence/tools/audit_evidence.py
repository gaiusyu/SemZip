#!/usr/bin/env python3
"""Check shipped training evidence only; no API, compiler or generated-code execution."""
from pathlib import Path
import collections,hashlib,json
ROOT=Path(__file__).resolve().parents[1]
def sha(b):return hashlib.sha256(b).hexdigest()
def jb(x):return (json.dumps(x,indent=2,sort_keys=True,ensure_ascii=False,allow_nan=False)+'\n').encode()
def main():
    p=json.loads((ROOT/'PROVENANCE.json').read_text());assert p['api_calls']==0 and not p['full_response_to_program_replay_verified']
    seen=set();samples_present=0
    for m in p['mapping']:
        rel=Path(m['artifact_file']);assert not rel.is_absolute() and '..' not in rel.parts and str(rel) not in seen;seen.add(str(rel))
        # Training samples are LogHub excerpts and are not redistributed; tools/rebuild_samples.py rebuilds them from the
        # public logs (and verifies them). They are checked here only when present.
        if m['kind']=='sample' and not (ROOT/rel).exists():continue
        data=(ROOT/rel).read_bytes();assert len(data)==m['bytes'] and sha(data)==m['sanitized_sha256']
        if m['kind']=='sample':assert sha(data)==m['original_sha256'] and m['byte_identical_to_original'] is True;samples_present+=1
        elif m['kind']=='request':
            v=json.loads(data);assert sha(jb(v))==m['original_body_canonical_sha256'];assert sha(jb([x['content'] for x in v['messages']]))==m['message_contents_sha256']
        elif m['kind']=='response':
            v=json.loads(data);assert sha(jb([x['message']['content'] for x in v['choices']]))==m['message_contents_sha256']
    requests=0;tokens=0;traces=0
    for d in p['datasets']:
        rows=sorted((ROOT/d['dataset']/'exchanges').glob('*.request.json'));responses=sorted((ROOT/d['dataset']/'exchanges').glob('*.response.json'));assert len(rows)==len(responses)==d['requests']==d['responses']
        sums=collections.Counter();finishes=collections.Counter()
        for req,res in zip(rows,responses):
            assert req.stem.split('.')[0]==res.stem.split('.')[0]
            x=json.loads(res.read_text())
            for k in ['prompt_tokens','completion_tokens','total_tokens']:sums[k]+=x['usage'][k]
            finishes.update(c['finish_reason'] for c in x['choices'])
        assert dict(sums)==d['usage'] and dict(finishes)==d['finish_reasons']
        if (ROOT/d['dataset']/'sample.log').exists():
            sample=(ROOT/d['dataset']/'sample.log').read_bytes();assert len(sample)==d['sample_bytes'];assert sample.count(b'\n')+int(bool(sample) and not sample.endswith(b'\n'))==d['sample_physical_records']
        count=len(json.loads((ROOT/d['dataset']/'compiler_trace.json').read_text()));assert count==d['compiler_trace_records'];traces+=count;requests+=len(rows);tokens+=sums['total_tokens']
    assert len(p['datasets'])==16 and requests==202 and traces==869 and tokens==1238005
    print(json.dumps({'status':'PASS_EVIDENCE_CONSISTENCY','datasets':16,'requests':requests,'responses':requests,'tokens':tokens,'compiler_records':traces,'training_samples_present':samples_present,'api_calls':0,'response_to_trainer_replay_verified':False}))
if __name__=='__main__':main()
