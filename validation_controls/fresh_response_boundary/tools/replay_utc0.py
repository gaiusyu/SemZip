#!/usr/bin/env python3
"""Bounded response-content replay at call_llm; stop immediately after original plan export."""
from pathlib import Path
import argparse,copy,hashlib,json,locale,os,re,socket,sys,time,urllib.request,traceback

class PlanExported(BaseException):pass

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def canonical(x):return hashlib.sha256(json.dumps(x,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def read(p):return json.loads(p.read_text())
def deny(*args,**kwargs):raise RuntimeError('Network and archive creation are forbidden in response-content control')
def parse_response(value):
    content=value['choices'][0]['message']['content'].strip()
    if content.startswith('```'):
        lines=content.splitlines()
        if lines and lines[0].startswith('```'):lines=lines[1:]
        if lines and lines[-1].startswith('```'):lines=lines[:-1]
        content='\n'.join(lines).strip()
    try:payload=json.loads(content)
    except json.JSONDecodeError:payload=json.loads(re.sub(r'\\(?!["\\/bfnrtu])',r'\\\\',content))
    if isinstance(payload,dict):payload=dict(payload);payload.setdefault('_proposal_cache_mode','api')
    return payload

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--source',type=Path,required=True);p.add_argument('--evidence',type=Path,required=True);p.add_argument('--dataset',required=True);p.add_argument('--expected-plan',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    source=a.source.resolve();evidence=a.evidence.resolve();out=a.output.resolve();assert not out.exists();out.mkdir()
    d=a.dataset;dataset=evidence/d;sample=dataset/'sample.log';expected=a.expected_plan.resolve();config=read(evidence/'configuration.json');sample_config=config['sampling_configuration_by_dataset'][d]
    source_before={str(q.relative_to(source)):sha(q) for q in source.rglob('*') if q.is_file() and '__pycache__' not in q.parts and q.suffix!='.pyc'}
    expected_sha=sha(expected);sample_sha=sha(sample);calls=[];misses=[];recordpairs={};files={str(sample):sample_sha,str(expected):expected_sha}
    for request in sorted((dataset/'exchanges').glob('*.request.json')):
        response=request.with_name(request.name.replace('.request.json','.response.json'));body=read(request);r=read(response)
        assert body['model']=='gpt-4o' and body['temperature']==0 and body['max_tokens']==16384 and [m['role'] for m in body['messages']]==['system','user']
        prompt=body['messages'][-1]['content'];assert prompt not in recordpairs
        recordpairs[prompt]={'payload':parse_response(r),'request_sha256':sha(request),'response_sha256':sha(response),'call_id':request.name.split('.')[0]}
        files[str(request)]=sha(request);files[str(response)]=sha(response)
    for key in list(os.environ):
        if key.startswith(('PARE_LLM_','YUNWU_','SEMZIP_R53_')) or key=='SEMZIP_R54_PLAN':os.environ.pop(key,None)
    os.environ.update(PARE_NO_BENEFIT_FIXED_CODEC='1',PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA='1',TZ='UTC0',LC_ALL='C',PYTHONHASHSEED='0');time.tzset();locale.setlocale(locale.LC_ALL,'C')
    socket.socket.connect=deny;socket.socket.connect_ex=deny;socket.create_connection=deny;urllib.request.urlopen=deny
    sys.path[:0]=[str(source/'trainer'),str(source)]
    import semzip_pure as s
    import run_blocks_pure as runner
    from pure_config import PureConfig
    cfg=PureConfig(workers=1,model='gpt-4o',offline_prefix_ratio=1.0,offline_min_prefix_lines=100000,offline_api_workers=1,offline_max_llm_calls=40)
    for name in ['workers','model','offline_prefix_ratio','offline_min_prefix_lines','offline_api_workers','offline_max_llm_calls','offline_topk','offline_batch_query','offline_batch_examples']:
        assert getattr(cfg,name)==sample_config[name],name
    def replay(prompt,args,cache_path):
        if prompt not in recordpairs:
            misses.append(hashlib.sha256(prompt.encode()).hexdigest());raise RuntimeError('Unrecorded prompt; no response substitution or API allowed')
        rec=recordpairs[prompt];assert not any(c['call_id']==rec['call_id'] for c in calls),'Repeated prompt was not repeated in original records'
        calls.append({k:rec[k] for k in ['call_id','request_sha256','response_sha256']});calls[-1]['prompt_sha256']=hashlib.sha256(prompt.encode()).hexdigest()
        payload=copy.deepcopy(rec['payload']);cache_path.parent.mkdir(parents=True,exist_ok=True);cache_path.write_text(json.dumps(payload,indent=2,ensure_ascii=False)+'\n')
        stats=s.get_llm_runtime_stats(args);stats['cached_response_replays']=len(calls)
        return payload
    s.call_llm=replay;s.create_archive=deny
    original_writer=s.write_replay_plan
    def finish(*args,**kwargs):original_writer(*args,**kwargs);raise PlanExported()
    s.write_replay_plan=finish
    extra=['--offline-family-topk-query','--offline-family-topk',str(cfg.offline_topk),'--offline-family-topk-min-support','1']
    if cfg.offline_batch_query:extra.append('--offline-family-batch-query')
    extra += ['--offline-family-batch-max-examples',str(cfg.offline_batch_examples),'--offline-family-api-workers',str(cfg.offline_api_workers),'--llm-template-index-cache','--llm-function-trie-cache','--llm-function-trigger-cache','--api-retries','0','--model','gpt-4o','--temperature','0']
    argv=runner.compressor_command(sample,d,out/'work',out/'cache',max_llm_calls=40,force_llm=True,replay_plan_out=True,extra_flags=extra)[2:]
    args=s.parse_args(argv);start=time.perf_counter();error=None;exported=False
    try:s.compress(args)
    except PlanExported:exported=True
    except Exception:error=traceback.format_exc()
    result={'dataset':d,'status':'FAILED','boundary':'call_llm response-content, not HTTP/provider replay','api_calls':0,'sample_sha256':sample_sha,'expected_plan_sha256':expected_sha,'recorded_responses':len(recordpairs),'consumed_responses':len(calls),'unmatched_prompt_sha256':misses,'calls':calls,'plan_export_reached':exported,'new_complete_archives':len(list(out.rglob('archive.tar.xz'))),'new_storage_fits':0,'sample_auxiliary_core_files_written_before_plan_export':True,'diagnostic_replay_seconds_not_original_training':time.perf_counter()-start,'error':error,'source_sha256':source_before}
    if exported:
        plan=out/'work/replay_plan.json';actual=read(plan);old=read(expected)
        result.update(replayed_plan_sha256=sha(plan),same_parsed_plan=actual==old,same_plan_file_bytes=sha(plan)==expected_sha,canonical_plan_sha256=canonical(actual),original_canonical_plan_sha256=canonical(old))
        result['status']='PASS_IDENTICAL' if actual==old and not misses and len(calls)==len(recordpairs) and result['new_complete_archives']==0 else 'DIFFERENT'
    result['source_unchanged']=source_before=={str(q.relative_to(source)):sha(q) for q in source.rglob('*') if q.is_file() and '__pycache__' not in q.parts and q.suffix!='.pyc'}
    result['evidence_unchanged']=all(sha(Path(name))==value for name,value in files.items())
    assert result['source_unchanged'] and result['evidence_unchanged']
    (out/'result.json').write_text(json.dumps(result,indent=2,sort_keys=True)+'\n')
    print(json.dumps({k:result.get(k) for k in ['dataset','status','recorded_responses','consumed_responses','same_parsed_plan','same_plan_file_bytes','new_complete_archives','diagnostic_replay_seconds_not_original_training']}),flush=True)
if __name__=='__main__':main()
