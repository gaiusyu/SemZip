"""Export retained historical observations, never run experiments or API calls.

The private development evidence root is supplied explicitly. Output is a
whitelisted numeric/provenance subset, not a model-training replay package.
"""
from pathlib import Path
from collections import Counter
import argparse, hashlib, json, math


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def model_label(value):
    lower = value.lower()
    if 'qwen3-coder' in lower:
        return 'Qwen3-Coder-480B-A35B'
    if 'qwen3-32b' in lower:
        return 'Qwen3-32B'
    labels = {'gpt-4o': 'GPT-4o', 'gpt-4o-mini': 'GPT-4o-mini',
              'deepseek-v3.2': 'DeepSeek-V3.2', 'gemini-2.5-pro': 'Gemini-2.5-Pro'}
    return labels[value]


def keep(obj, keys):
    return {key: obj[key] for key in keys if key in obj}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args(); root=args.evidence_root; out=args.output; out.mkdir(parents=True,exist_ok=True)
    origins = {}; records = []

    def read(path, role, cohort=None, dataset=None):
        path=Path(path)
        if path not in origins:
            identity={'id':'source-%04d' % (len(records)+1), 'sha256':sha(path), 'role':role}
            if cohort:identity['cohort']=cohort
            if dataset:identity['dataset']=dataset
            records.append(identity); origins[path]=identity
        return json.loads(path.read_text()), origins[path]

    def campaign(prefix):
        matches=[p for p in root.glob(prefix+'_*') if p.is_dir()]
        assert len(matches)==1,(prefix,len(matches))
        return matches[0]

    def save(name, payload):
        (out/name).write_text(json.dumps(payload,indent=2,ensure_ascii=False)+'\n')

    def numeric_totals(obj):
        return keep(obj,('raw_bytes','archive_bytes','ratio','encode_seconds'))

    def summary(obj):
        result=keep(obj,('raw_bytes','archive_bytes','semantic_archive_bytes','delog_archive_bytes',
                        'compression_ratio','raw_sha256','decoded_sha256','decoded_bytes','sha_pass',
                        'block_size','workers','online_compression_seconds','decode_seconds','plan_sha256',
                        'compressor_sha256','llm_api_calls','verified_blocks','decoded_blocks','failed_blocks'))
        if 'archive_bytes' in result:
            assert result['archive_bytes']>0
            assert math.isclose(result['compression_ratio'],result['raw_bytes']/result['archive_bytes'],rel_tol=1e-12)
        result['active_semantic_blocks']=sum(bool(b.get('streams')) for b in obj.get('blocks',[]))
        result['admitted_semantic_values']=sum(sum(b.get('spec_counts',{}).values()) for b in obj.get('blocks',[]))
        return result

    combined, combined_id=read(root/'fse_followup_20260923/audited_evidence.json','combined historical audit','explicit_and_append_only')
    defaults, defaults_id=read(campaign('r62')/'audited_results.json','backbone audit','gateway_default')
    explicit=[]; default=[]; runtime=[]
    for r in combined['backbones']:
        row=keep(r,('dataset','repeat','status','training_seconds','requests','responses','finish_reasons',
                    'sample_sha256','first_messages_sha256','retained_plan_entries','active_semantic_blocks',
                    'usage_as_reported','ratio','raw_bytes','archive_bytes','semantic_values','semantic_bytes',
                    'residual_bytes','independent_decode_pass','failure_stage'))
        row.update(model=model_label(r['model']),cohort='explicit_output_cap',output_max_tokens=16384,
                   source_record=combined_id['id'],original_audit_row=len(explicit),
                   configured_identifier_sha256=hashlib.sha256(r['model'].encode()).hexdigest())
        folder=root/r['campaign']/'results'/r['dataset']/r['model']/('repeat_%d'%r['repeat'])
        result,rid=read(folder/'result.json','original backbone trial','explicit_output_cap',r['dataset'])
        row['trial_source_record']=rid['id']
        if r['status']=='PASS':
            row['heldout_block_observations']=[keep(b,('index','raw_bytes','raw_sha256','semantic_archive_bytes','spec_counts')) for b in result['heldout']['blocks']]
            assert len(row['heldout_block_observations'])==4 and row['independent_decode_pass']
        explicit.append(row)
    for index,r in enumerate(defaults['rows']):
        row=keep(r,('dataset','repeat','status','training_seconds','sampled_lines','sample_sha256',
                    'first_prompt_sha256','completion_tokens_per_response','requests','successful_responses',
                    'http_attempts','prompt_tokens','completion_tokens','api_seconds','finish_reasons',
                    'non_api_synthesis_seconds','retained_plan_entries','accepted_rules','heldout_raw_bytes',
                    'heldout_archive_bytes','heldout_ratio','heldout_sha_pass','independent_archive_decode','failure_stage'))
        row.update(model=model_label(r['model']),cohort='gateway_default',output_max_tokens=None,
                   output_limit_scope='Parameter absent; service default not measured or equated across models',
                   source_record=defaults_id['id'],original_audit_row=index,
                   configured_identifier_sha256=hashlib.sha256(r['model'].encode()).hexdigest())
        folder=campaign('r62')/'results'/r['dataset']/r['model']/('repeat_%d'%r['repeat'])
        result,rid=read(folder/'result.json','original backbone trial','gateway_default',r['dataset'])
        row['trial_source_record']=rid['id']
        row['original_trial_status']=result['status']
        if (folder/'recovery_empty_semantics/result.json').exists():
            result,recovery_id=read(folder/'recovery_empty_semantics/result.json','empty-stream harness replay','gateway_default',r['dataset'])
            row['evaluated_trial_source_record']=recovery_id['id']
            row['harness_correction']='Empty semantic directory handling; original synthesis/plan retained; no new model generation.'
        if r['status']=='PASS':
            row['heldout_block_observations']=[keep(b,('index','raw_bytes','raw_sha256','semantic_archive_bytes','spec_counts')) for b in result['heldout']['blocks']]
            row['active_semantic_blocks']=sum(bool(b.get('streams')) for b in result['heldout']['blocks'])
            assert len(row['heldout_block_observations'])==4 and row['heldout_sha_pass']
        default.append(row)
    assert len(explicit)==36 and len(default)==18
    assert sum(r['status']=='PASS' for r in explicit)==35 and sum(r['status']=='PASS' for r in default)==17
    assert sum(r['status']=='PASS' and r['active_semantic_blocks']==0 for r in explicit)==4
    for prefix,cohort in [('r62','gateway_default'),('r64','explicit_qwen_coder'),('r66','explicit_other_five')]:
        protocol,pid=read(campaign(prefix)/'protocol.json','backbone protocol',cohort)
        runtime.append({'cohort':cohort,'source_record':pid['id'],
                        'contract':keep(protocol,('independent_training_repeats','train_original_blocks','test_original_blocks',
                          'block_lines','preserve_original_tail','max_llm_calls_per_training','api_workers','api_retries','temperature','runtime_workers','output_max_tokens')),
                        'source_file_sha256':protocol['source_sha256']})
    save('backbone_trials.json',{'status':'COMPLETE','cohorts':{'explicit_output_cap':explicit,'gateway_default':default},
         'counts':{'unique_trainings':54,'explicit_successes':35,'explicit_failures':1,'explicit_zero_stream_successes':4,
                   'default_successes':17,'default_failures':1},
         'training_time_scope':'Original synthesis subprocess including model service and verification; excludes prefix extraction, sampling, storage fitting and online encoding.',
         'runtime_cohorts':runtime,
         'limits':'Two independent trainings per cell. Descriptive observations; model labels are public families, not immutable provider versions. Qwen3-32B thinking was disabled. Equal requested limits do not equalize inference compute. Historical guard/cache behavior differs from R71.'})

    append=[];append_blocks=[]
    for index,e in enumerate(combined['evolution']):
        folder=root/e['folder'];dataset=e['dataset']
        report,pid=read(folder/'pilot/report.json','append-only report','append_only',dataset)
        blocks,bid=read(folder/'pilot/blocks.json','append-only chronological blocks','append_only',dataset)
        row=keep(e,('dataset','blocks','heldout','update_calls','update_seconds','scope','archive_saving_percent','harness_corrected'))
        row.update(source_record=combined_id['id'],original_audit_row=index,report_source_record=pid['id'],
                   status=report['status'],updates=[keep(u,('completed_through','api_call_cap','status','new_rules','seconds','trigger')) for u in e['updates']])
        row['archive_only_reconstruction']=report.get('archive_only_full_sha_pass',report.get('archive_only_prefix_sha_pass'))
        assert row['status']=='PASS' and all(row['archive_only_reconstruction'].values())
        append.append(row)
        for b in blocks:
            block={'dataset':dataset,'source_record':bid['id'], 'index':b['index'],
                   'in_training_prefix':b.get('in_training_prefix',b['index']==0),
                   'deployment_version_ordinal':0,'frozen':summary(b['frozen']),'evolving':summary(b['evolving'])}
            block.update(keep(b,('coverage','eligible_groups','physical_lines')))
            assert block['frozen']['raw_sha256']==block['evolving']['raw_sha256']
            assert block['frozen']['archive_bytes']==block['evolving']['archive_bytes']
            append_blocks.append(block)
    assert len(append)==9 and len(append_blocks)==75
    assert sum(len(r['updates']) for r in append)==8
    assert sum(r['update_calls'] for r in append)==46
    save('append_only_evolution.json',{'status':'COMPLETE','datasets':append,'blocks':append_blocks,
         'counts':{'datasets':9,'original_block_pairs':75,'triggered_attempts':8,'published_updates':0,
                   'update_calls':46,'update_seconds':sum(r['update_seconds'] for r in append)},
         'scope':'Historical append-only protocol, including no-trigger and rejected updates. Android uses corrected physical-LF monitoring evidence. Do not pool with replacement-gate or R71 strict deployment.'})

    gated,gid=read(campaign('r67')/'audited_results.json','gated evolution audit','gated_replacement')
    design,did=read(campaign('r67')/'DESIGN.json','gated evolution design','gated_replacement')
    sources,sid=read(campaign('r67')/'source_hashes.json','gated runtime hashes','gated_replacement')
    gated_rows=[];gated_blocks=[]
    for index,r in enumerate(gated['datasets']):
        dataset=r['dataset'];folder=campaign('r67')/dataset
        update,uid=read(folder/'update.json','gated candidate/update','gated_replacement',dataset)
        blocks,bid=read(folder/'blocks.json','gated chronological blocks','gated_replacement',dataset)
        report,rid=read(folder/'report.json','gated complete report','gated_replacement',dataset)
        initial,iid=read(folder/'initial_identity.json','gated original plan/policy','gated_replacement',dataset)
        row=keep(r,('dataset','blocks','future_blocks','triggered','trigger','outcome','calls','training_seconds',
                    'generation_fit_seconds','gate_seconds','future','heldout','paired_experiment_wall_seconds',
                    'future_archive_saving_percent','future_failures','future_failed_attempt_wall_seconds',
                    'validation_candidate_bytes','validation_control_bytes','validation_frozen_bytes','validation_timings',
                    'eligible_groups','candidate_training_streams'))
        row.update(source_record=gid['id'],original_audit_row=index,update_source_record=uid['id'],
                   report_source_record=rid['id'],initial_source_record=iid['id'],initial_identity=initial,
                   independent_archive_only_decode_pass=report['independent_archive_only_decode_pass'],
                   prefix_sha256=report['prefix_sha256'])
        row['update']=keep(update,('status','training_blocks','validation_blocks','future_blocks','triggered','novelty_trigger',
            'size_trigger','candidate_sha256','candidate_storage_sha256','size_gate_pass','speed_gate_pass','validation_timings',
            'generation_fit_seconds','gate_seconds'))
        row['error_record_present']=bool(update.get('error') or r.get('error'))
        if row['error_record_present']:
            row['error_classification']='duplicate_candidate_tags' if 'Duplicate candidate tags' in str(update.get('error')) else 'generation_or_validation_failure'
        row['unpublished_candidate_future_measured']=False
        row['candidate_future_scope']='Observed deployed candidate only after publication; no counterfactual future replay for rejected candidates.'
        gated_rows.append(row)
        for b in blocks:
            block=keep(b,('index','physical_lines','sha256','version','coverage','eligible_groups','failed_attempt_seconds'))
            block.update(dataset=dataset,source_record=bid['id'])
            for mode in ('frozen','deployed','storage_control'):
                if mode in b:block[mode]=summary(b[mode])
            block['candidate_failure_record_present']='candidate_error' in b
            block['storage_control_failure_record_present']='storage_control_error' in b
            assert block['deployed']['sha_pass'] and block['deployed']['raw_sha256']==b['sha256']
            gated_blocks.append(block)
    assert len(gated_rows)==7 and len(gated_blocks)==62
    assert sum(r['triggered'] for r in gated_rows)==6 and sum(r['outcome']=='PUBLISHED' for r in gated_rows)==1
    save('gated_evolution.json',{'status':'COMPLETE','datasets':gated_rows,'blocks':gated_blocks,
         'counts':{'datasets':7,'deployed_original_blocks':62,'triggered':6,'published':1,'requests':sum(r['calls'] for r in gated_rows)},
         'protocol_source_record':did['id'],'source_hash_record':sid['id'],'source_file_sha256':sources,
         'contract':keep(design,('block_lines','tail','selection','initial','trigger','candidate','fit','validation','publication','future','scope','units')),
         'historical_state_scope':'Each compression/decode benchmark invocation used a new CLI process. The block-1/block-2 observer reused runtime imports in a dataset process without the later R71 data-cache reset. No retrospective R71 cold-guard guarantee is claimed.'})

    controls=[]
    for prefix,dataset in [('r57','OpenSSH'),('r58','Android')]:
        folder=campaign(prefix)
        script=folder/'repair_control.py'
        # Source is hashed, never executed: its hook reads model message content
        # from retained response JSON and rejects any unmatched prompt.
        script_record={'id':'source-%04d'%(len(records)+1),'sha256':sha(script),
                       'role':'recorded model-response hook source','cohort':'historical_repair_control','dataset':dataset}
        records.append(script_record)
        original=folder/'initial/training_work'/dataset/'replay_plan.json'
        original_plan,oid=read(original,'original pilot plan','historical_repair_control',dataset)
        response_origins={}
        for request in sorted((folder/'initial').rglob('*.request.json')):
            request_body,qid=read(request,'retained replay request identity','historical_repair_control',dataset)
            response_path=request.with_name(request.name.replace('.request.json','.response.json'))
            response,saved_id=read(response_path,'retained replay response identity','historical_repair_control',dataset)
            prompt=request_body['body']['messages'][-1]['content']
            content=response['choices'][0]['message']['content'].strip()
            if content.startswith('```'):content='\n'.join(content.splitlines()[1:-1])
            parsed=json.loads(content)
            response_origins[hashlib.sha256(prompt.encode()).hexdigest()]={
                'request_source_record':qid['id'],'response_source_record':saved_id['id'],
                'canonical_response_content_sha256':hashlib.sha256(json.dumps(parsed,sort_keys=True,separators=(',',':')).encode()).hexdigest()}
        for condition in ('replay','no_llm_repair'):
            report,rid=read(folder/'repair_control'/condition/'report.json','repair control observation','historical_repair_control',dataset)
            plan,pid=read(folder/'repair_control'/condition/'work/replay_plan.json','repair control plan','historical_repair_control',dataset)
            assert plan==original_plan and report['same_saved_plan'] and report['actual_api_calls']==0
            controls.append({'dataset':dataset,'condition':condition,'actual_api_calls':0,'same_parsed_plan':True,
                'spec_count':report['spec_count'],'original_spec_count':report['original_spec_count'],
                'model_response_content_replays':len(report['replayed_responses']),
                'replayed_prompt_sha256':[x['prompt_sha256'] for x in report['replayed_responses']],
                'matched_response_origins':[response_origins[x['prompt_sha256']] for x in report['replayed_responses']],
                'repair':keep(report['repair'],('attempts','candidate_items','errors','skipped_ineligible')),
                'source_record':rid['id'],'script_source_record':script_record['id'],'original_plan_source_record':oid['id'],
                'replayed_plan_source_record':pid['id'],'canonical_plan_sha256':hashlib.sha256(json.dumps(plan,sort_keys=True,separators=(',',':')).encode()).hexdigest()})
    save('repair_response_controls.json',{'status':'COMPLETE','rows':controls,
         'scope':'R57/R58 only: recorded model message contents parsed as JSON and returned at call_llm for exact prompt matches. New/unmatched requests raise. Fresh output/cache directories; deterministic compiler repairs remain. This does not test HTTP transport/parsing replay or fresh-16 R71 training replay.',
         'interpretation':'Disabling LLM repair leaves both parsed plans unchanged in these two fixed-response cases (one and thirteen original repair calls). No general claim that repair is unnecessary.'})
    save('SOURCE_RECORDS.json',{'schema':'anonymous-historical-source-map-v1','records':records,
         'scope':'Each opaque record maps a package row to the SHA-256 of its original development evidence file. Original private filesystem paths, hostnames, gateway aliases and request/response IDs are not exported. Sanitized output bytes have their own FILES.json identities.'})
    save('VALIDATION.json',{'status':'PASS','new_api_calls':0,'new_compression_runs':0,'rows':{'backbone_trials':54,'append_only_datasets':9,'append_only_block_pairs':75,'gated_datasets':7,'gated_deployed_blocks':62,'repair_conditions':4},
         'original_source_records':len(records),'exporter_sha256':sha(Path(__file__)),
         'scope':'Whitelisted export and existing-ledger/parsed-plan equality checks only; no new model or compressor execution.'})
    print(json.dumps({'status':'PASS','source_records':len(records),'backbone_trials':54,'append_only_block_pairs':75,'gated_blocks':62}))

if __name__=='__main__':main()
