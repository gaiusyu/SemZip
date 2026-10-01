#!/usr/bin/env python3
"""Export reviewed original training records through an explicit field whitelist.

This packages evidence; it does not call a model, execute generated code, compile a
program, or claim the saved responses were replayed through the complete trainer.
"""
import argparse,collections,hashlib,json,os,re,shutil,tempfile
from pathlib import Path

REQUEST_KEYS={'model','response_format','messages','temperature','max_tokens'}
TRACE_KEYS={'family_id','support','source_tag','meaning','regex','replacement','status','reasons','kind','semantic_key','tag','store_group','failed_status','repair_category','repair_candidates','context_projector_values'}
DESIGN_KEYS={'model','temperature','output_max_tokens','max_calls_per_dataset','http_retries','independent_trainings','datasets','training_original_blocks','block_size','physical_line_delimiter','deployment','short_files','selection','training_failure','storage_failure','online_failure_policy','source_hashes'}
MARKERS={'user_home':r'/(?:Users|home)/[^/\s]+/','credential_syntax':r'(?i)(?:bearer\s+[A-Za-z0-9]|sk-[A-Za-z0-9]{16})'}
# Site-specific private mount, host and service patterns are supplied at export time as a JSON object
# {label: regex} in EXPORT_PRIVATE_MARKERS; they are deliberately not published with this artifact.
MARKERS.update(json.loads(os.environ.get('EXPORT_PRIVATE_MARKERS','{}')))

def check(ok,why):
    if not ok:raise ValueError(why)
def sha(data):return hashlib.sha256(data).hexdigest()
def jbytes(value):return (json.dumps(value,indent=2,sort_keys=True,ensure_ascii=False,allow_nan=False)+'\n').encode()
def read(path):return json.loads(path.read_text())
def scan(data):
    text=data.decode('utf-8',errors='replace')
    for label,pattern in MARKERS.items():check(not re.search(pattern,text),'Manual review required: '+label)
def numeric_tree(value):
    if isinstance(value,dict):return {key:numeric_tree(val) for key,val in value.items()}
    check(type(value) is int and value>=0,'Unexpected usage value');return value

def export(args,stage):
    summaries=read(args.training_summary);sampling=read(args.sampling_summary)
    rows=summaries['datasets'];expected={r['dataset']:r for r in rows}
    check(len(expected)==16,'Expected sixteen training summaries')
    source=args.originals;mapping=[];counts=[];requesttotal=0;tracetotal=0;content_total=0
    def put(rel,data,original,kind,**extra):
        scan(data);target=stage/rel;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
        record={'artifact_file':rel,'kind':kind,'original_sha256':sha(original),'sanitized_sha256':sha(data),'bytes':len(data),**extra};mapping.append(record)
    configuration=read(source/'TRAINING_DESIGN.json')
    check(DESIGN_KEYS.issubset(configuration) and configuration['datasets']==[r['dataset'] for r in rows],'Training design inventory differs')
    clean_config={key:configuration[key] for key in DESIGN_KEYS}
    # Measured replay records hold the actual PureConfig values used for sampling.
    sampleconfigs={r['dataset']:r['configuration'] for r in sampling['datasets']}
    check(set(sampleconfigs)==set(expected),'Sampling configuration inventory differs')
    clean_config['sampling_configuration_by_dataset']=sampleconfigs
    clean_config['source']='Original synthesis design; sampling configurations from separately audited original-sample replay'
    put('configuration.json',jbytes(clean_config),(source/'TRAINING_DESIGN.json').read_bytes(),'configuration')
    for summary in rows:
        dataset=summary['dataset'];d=source/'training'/dataset
        sample=d/'training_samples'/(dataset+'.log');original=sample.read_bytes()
        physical=original.count(b'\n')+int(bool(original) and not original.endswith(b'\n'))
        check(sha(original)==summary['sample_sha256'] and physical==summary['sample_physical_lines'],'Sample identity differs')
        check(summary['sample_original_LF_membership_verified'] is True,'Sample original-record membership missing')
        put(dataset+'/sample.log',original,original,'sample',byte_identical_to_original=True,physical_records=physical)
        reqs=sorted(d.rglob('*.request.json'),key=lambda p:(read(p)['recorded_at_utc'],p.name))
        response_files=set(d.rglob('*.response.json'));paired=set();usages=collections.Counter();phases=collections.Counter();finishes=collections.Counter()
        check(len(reqs)==summary['api_totals']['requests']==summary['api_totals']['responses'],'Request/response counts differ')
        for index,p in enumerate(reqs):
            response=p.with_name(p.name[:-len('.request.json')]+'.response.json');check(response in response_files,'Unpaired API response');paired.add(response)
            request=read(p);raw_response=read(response);body=request['body']
            check(set(body)==REQUEST_KEYS and body['model']==configuration['model'] and body['temperature']==configuration['temperature'] and body['max_tokens']==configuration['output_max_tokens'],'Unknown request schema or configuration')
            check(body['response_format']=={'type':'json_object'},'Unexpected response format')
            check([m['role'] for m in body['messages']]==['system','user'] and all(set(m)=={'role','content'} and isinstance(m['content'],str) for m in body['messages']),'Unexpected message schema')
            clean_request={key:body[key] for key in REQUEST_KEYS}
            phase='repair' if '_verifier_repair' in p.parts else 'proposal';phases[phase]+=1
            call=f'{index+1:03d}'
            prefix=dataset+'/exchanges/'+call
            put(prefix+'.request.json',jbytes(clean_request),p.read_bytes(),'request',phase=phase,request_body_unmodified=True,original_body_canonical_sha256=sha(jbytes(body)),message_contents_sha256=sha(jbytes([m['content'] for m in body['messages']])))
            choices=[]
            for c in raw_response['choices']:
                msg=c['message'];check(msg['role']=='assistant' and isinstance(msg['content'],str),'Unexpected response message')
                check(not msg.get('reasoning_content'),'Nonempty reasoning field requires separate review')
                check(isinstance(c['finish_reason'],str) and type(c['index']) is int,'Unexpected choice status')
                choices.append({'index':c['index'],'finish_reason':c['finish_reason'],'message':{'role':'assistant','content':msg['content']}})
                finishes[c['finish_reason']]+=1;content_total+=len(msg['content'].encode())
            usage=numeric_tree(raw_response['usage'])
            clean_response={'model':raw_response['model'],'choices':choices,'usage':usage}
            put(prefix+'.response.json',jbytes(clean_response),response.read_bytes(),'response',response_content_unmodified=True,message_contents_sha256=sha(jbytes([c['message']['content'] for c in choices])))
            for key in ['prompt_tokens','completion_tokens','total_tokens']:usages[key]+=usage[key]
        check(paired==response_files,'Unexpected unpaired response files')
        for key in ['prompt_tokens','completion_tokens','total_tokens']:check(usages[key]==summary['api_totals'][key],'Usage totals differ')
        check(phases['proposal']==summary['api_totals']['proposal_requests'] and phases['repair']==summary['api_totals']['repair_requests'],'Proposal/repair totals differ')
        check(dict(finishes)==summary['finish_reasons'],'Finish status totals differ')
        trace=d/'training_work'/dataset/'function_compile_trace.json';records=read(trace)
        check(isinstance(records,list) and all(isinstance(r,dict) and set(r)<=TRACE_KEYS for r in records),'Unreviewed compiler trace field')
        # All currently observed trace keys are public compiler fields; retain each
        # record (including rejections/repairs), preserving every value unchanged.
        put(dataset+'/compiler_trace.json',jbytes(records),trace.read_bytes(),'compiler_trace',all_record_values_unmodified=True,records=len(records))
        counts.append({'dataset':dataset,'samples':1,'sample_bytes':len(original),'sample_physical_records':physical,'requests':len(reqs),'responses':len(paired),'proposal_requests':phases['proposal'],'repair_requests':phases['repair'],'compiler_trace_records':len(records),'usage':dict(usages),'finish_reasons':dict(finishes)})
        requesttotal+=len(reqs);tracetotal+=len(records)
    check(requesttotal==202,'Expected all 202 original exchanges')
    scope={'status':'PASS_EVIDENCE_EXPORT_ONLY','datasets':counts,'samples':16,'requests':202,'responses':202,'compiler_traces':16,'compiler_trace_records':tracetotal,'response_content_bytes':content_total,'api_calls':0,'generated_program_execution':False,'full_response_to_program_replay_verified':False,'sample_bytes_identical_to_original':True,'request_body_and_response_content_unmodified':True,'all_compiler_records_retained':True,'private_marker_scan_hits':0,'stripped_request_fields':['schema_version','recorded_at_utc','cache_file','endpoint','method','headers'],'stripped_response_fields':['id','created','object','system_fingerprint','service_tier','provider_specific_fields','empty_reasoning_content'],'limitations':['Original request/response files are not shipped and sanitized files are not byte-identical to them.','Transport metadata is removed; model-visible request bodies and response content are unmodified.','This is an evidence and pairing/usage check, not a full offline response replay through the compiler.','Targeted private-marker scanning is not a formal anonymity proof.'],'source_summary_sha256':sha(args.training_summary.read_bytes()),'sampling_summary_sha256':sha(args.sampling_summary.read_bytes()),'mapping':mapping}
    (stage/'PROVENANCE.json').write_bytes(jbytes(scope))
    return scope

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--originals',type=Path,required=True);p.add_argument('--training-summary',type=Path,required=True);p.add_argument('--sampling-summary',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    check(not a.output.exists(),'Output must not exist');check(a.output.parent.is_dir(),'Output parent missing')
    with tempfile.TemporaryDirectory(prefix='reviewed-evidence-',dir=a.output.parent) as tmp:
        stage=Path(tmp)/'evidence';stage.mkdir();scope=export(a,stage)
        (stage/'tools').mkdir();shutil.copyfile(__file__,stage/'tools/export_training_evidence.py')
        check(not a.output.exists(),'Output appeared during export');os.rename(stage,a.output)
    print(json.dumps({k:scope[k] for k in ['status','samples','requests','responses','compiler_traces','compiler_trace_records','api_calls','full_response_to_program_replay_verified']}))

if __name__=='__main__':main()
