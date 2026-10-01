#!/usr/bin/env python3
"""Validate final records, export a numeric/hash whitelist and generate paper outputs.

No API, codec, raw-file access, filesystem discovery or private-workspace import.
All output goes to a new explicit directory after every input passes validation.
"""
from pathlib import Path
import argparse,hashlib,json,math,os,re,statistics,sys,tempfile

SCHEMA='semzip.anonymous.final-results.v1'
DATASETS=['Linux','Proxifier','Apache','Zookeeper','Mac','HealthApp','HPC','Hadoop','OpenStack','OpenSSH','Android','BGL','HDFS','Spark','Windows','Thunderbird']
METHODS=['semzip','delog','loglite','gzip6','xz6','zstd3']
PAIRED=['Linux','HPC','OpenSSH','Android']
FORMAL_VERSION='R68-FORMAL-R71-OUTER-CLI-V3'
FORMAL_SCOPE='Parent subprocess launch through exit for separate encode/decode CLIs; includes imports, pools, block CLI startup, input split/read, output files, required guard/reset and implementation-native hashes, cleanup and summary serialization. Independent inventory/archive hashes outside these timers. Controller imports excluded; buffered I/O, no fsync/cache drop.'
FORMAL_STATES={'PASS','FAILED','FAILED_PROCESS_INTERRUPTED','FAILED_NO_RESULT','FAILED_PROCESS_EXIT','FAILED_POST_CAMPAIGN_ARCHIVE_AUDIT','NOT_TIMED_FIRST_PASS_FAILED'}
ROUTES=['python_inverse_archive_route','python_checked_auto_storage','generic_auto_codec','other_explicit_schema']
LABELS={'semzip':'SemZip','delog':'DeLog','loglite':'LogLite-BL-wide+tail+reservefix','gzip6':'gzip -6 -n','xz6':'XZ -6 -T1','zstd3':'Zstd -3 -T1'}

class Invalid(ValueError):pass

def need(condition,message):
    if not condition:raise Invalid(message)

def number(value,label,positive=False,integer=False):
    need(type(value) in (int,float) and math.isfinite(value),label+': finite number required')
    need(value>0 if positive else value>=0,label+': out of range')
    need(not integer or type(value) is int,label+': integer required')
    return value

def digest(value,label='digest'):
    need(isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value),label+': SHA-256 required')
    return value

def same(a,b,label):
    need(math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-10),label+': inconsistent numeric evidence')

def pairs(rows,fields,expected,label):
    result={}
    for row in rows:
        key=tuple(row[k] for k in fields)
        need(key not in result,label+': duplicate identity')
        result[key]=row
    need(set(result)==set(expected),label+': incomplete or unexpected inventory')
    return result

def json_hash(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
def file_hash(path):return hashlib.sha256(path.read_bytes()).hexdigest()

def read(path,label,hashes=None):
    need(path.is_file(),label+': required input is missing')
    def unique(items):
        out={}
        for key,value in items:
            need(key not in out,label+': duplicate JSON key')
            out[key]=value
        return out
    def bad_constant(value):raise Invalid(label+': nonfinite JSON constant')
    raw=path.read_bytes()
    if hashes is not None:hashes[label]=hashlib.sha256(raw).hexdigest()
    return json.loads(raw.decode('utf-8'),object_pairs_hook=unique,parse_constant=bad_constant)

def size_snapshot(data):
    need(data['status']=='COMPLETE','snapshot: final COMPLETE export required; initial/in-progress snapshots rejected')
    need(data['expected_datasets']==DATASETS,'snapshot: fixed sixteen-dataset order changed')
    need(set(data['methods'])==set(METHODS) and len(data['methods'])==6,'snapshot: method inventory changed')
    matrix=pairs(data['rows'],('dataset','method'),[(d,m) for d in DATASETS for m in METHODS],'size matrix')
    out=[]
    for d in DATASETS:
        identities=set()
        for m in METHODS:
            row=matrix[d,m];need(row['status'] in {'PASS','FAIL','FAILED'},'size matrix: nonterminal status')
            clean={'dataset':d,'method':m,'status':row['status']}
            if row['status']=='PASS':
                for key in ['raw_bytes','archive_bytes','blocks']:clean[key]=number(row[key],key,True,True)
                clean['raw_sha256']=digest(row['raw_sha256']);clean['ratio']=clean['raw_bytes']/clean['archive_bytes']
                same(clean['ratio'],row['ratio'],'size ratio');identities.add((clean['raw_bytes'],clean['raw_sha256'],clean['blocks']))
                clean['sha_pass']=True  # Provenance: this is an audited COMPLETE exporter record.
                clean['diagnostic_encode_seconds']=number(row['first_pass_internal_encode_seconds'],'diagnostic encode seconds')
                clean['diagnostic_decode_seconds']=number(row['first_pass_internal_decode_seconds'],'diagnostic decode seconds')
                if m=='semzip':
                    clean['publication_sha256']=digest(row['publication_sha256'])
                    clean['result_sha256']=digest(row['result_sha256'])
                    for key in ['semantic_bytes','residual_bytes','fallback_blocks']:clean[key]=number(row[key],key,False,True)
                    need(clean['fallback_blocks']<=clean['blocks'],'too many fallback blocks')
                    need(clean['semantic_bytes']+clean['residual_bytes']<clean['archive_bytes'],'missing counted file manifest')
                    h=row['heldout'];clean['heldout']={k:number(h[k],k,False,True) for k in ['raw_bytes','archive_bytes','manifest_bytes']}
                    need((clean['blocks']>1)==(h['raw_bytes']>0),'suffix scope inconsistent')
                    need(h['raw_bytes']<clean['raw_bytes'] and h['archive_bytes']<clean['archive_bytes'],'invalid suffix cost')
                    need(h['archive_bytes']>0 if h['raw_bytes'] else h['archive_bytes']==0,'invalid suffix denominator')
                    clean['heldout']['ratio']=h['raw_bytes']/h['archive_bytes'] if h['raw_bytes'] else None
                    activity=[]
                    for spec in row['observed_program_activity']:
                        need(isinstance(spec['tag'],str) and re.fullmatch(r'[A-Za-z0-9_]{1,80}',spec['tag']), 'program tag is not a safe identifier')
                        need(spec['storage_and_validation_route'] in ROUTES,'unknown observed program route')
                        activity.append({'index':number(spec['index'],'rule index',False,True),'tag':spec['tag'],
                            'route':spec['storage_and_validation_route'],
                            'admitted_matches':number(spec['admitted_matches'],'matches',False,True),
                            'stored_values':number(spec['stored_values'],'stored values',False,True)})
                    clean['program_activity']=activity
                    if h['raw_bytes']:same(clean['heldout']['ratio'],h['ratio'],'suffix ratio')
            else:
                # Free-form errors, traceback, paths and codec stdout are never copied.
                clean['failure_record_sha256']=json_hash(row.get('failure_record',{'status':row['status']}))
            out.append(clean)
        need(len(identities)<=1,'size matrix: methods disagree on original input identity')
    training=pairs(data['training'],('dataset',),[(d,) for d in DATASETS],'training')
    trained=[]
    for d in DATASETS:
        row=training[d,];need(row['training_status']==row['storage_status']=='PASS','training/fit evidence incomplete')
        scope=row.get('scope','')
        need('excluding preceding prefix scanning/sampling' in scope and 'not total offline latency' in scope,'training: stale or ambiguous sampling scope; re-export with the corrected exporter')
        seconds=number(row['training_seconds'],'synthesis seconds');fit=number(row['corrected_fit_process_seconds'],'fit seconds')
        same(seconds+fit,row['training_plus_corrected_fit_seconds'],'recorded component sum')
        counts=row['programs']['counts'];specs=number(counts['specs'],'spec count',False,True)
        routes={route:number(counts.get('route:'+route,0),'route count',False,True) for route in ROUTES}
        need(sum(routes.values())==specs,'unclassified deployment route')
        api={k:number(row['api'][k],k,False,True) for k in ['requests','responses','prompt_tokens','completion_tokens','total_tokens']}
        item={'dataset':d,'training_status':'PASS','storage_status':'PASS',
              'original_storage_status':row['original_storage_status'],
              'sampled_lines':number(row['sampled_lines'],'sample lines',False,True),'training_records':number(row['training_records'],'training records',True,True),
              'synthesis_subprocess_seconds_excluding_sampling':seconds,'corrected_fit_process_seconds':fit,
              'recorded_synthesis_plus_corrected_fit_seconds':seconds+fit,
              'original_training_and_old_fit_wall_seconds':number(row['original_training_and_old_fit_wall_seconds'],'original offline wall'),
              'api':api,'specs':specs,'routes':routes,
              'python_rules':number(counts.get('op:python_exec',0),'Python rules',False,True),
              'python_code_exactly_in_recorded_proposal':number(counts.get('python_code_exactly_in_recorded_proposal',0),'proposal code count',False,True),
              'plan_sha256':digest(row['plan_sha256']),'policy_sha256':digest(row['policy_sha256'])}
        need(item['original_storage_status'] in {'PASS','FAIL','FAILED'},'unrecognized original storage status')
        need(item['sampled_lines']<=item['training_records']<=100000,'training exceeds original block zero')
        need(item['python_code_exactly_in_recorded_proposal']<=item['python_rules']<=specs,'invalid program counts')
        trained.append(item)
    # Preserve block identities and explicitly descriptive task timers.
    semrows={r['dataset']:r for r in out if r['method']=='semzip' and r['status']=='PASS'}
    expected=[(d,i) for d,r in semrows.items() for i in range(r['blocks'])]
    blockmap=pairs(data['semzip_blocks'],('dataset','index'),expected,'main block ledger')
    blocks=[]
    for d,i in expected:
        b=blockmap[d,i]
        item={'dataset':d,'index':i,'raw_sha256':digest(b['raw_sha256']),
              'raw_bytes':number(b['raw_bytes'],'block bytes',True,True),'records':number(b['records'],'block records',True,True),
              'semantic_bytes':number(b['semantic_bytes'],'semantic bytes',True,True),
              'residual_bytes':number(b['residual_bytes'],'residual bytes',True,True),'fallback':b['fallback'], **{k:number(b[k],k) for k in ['semantic_stage_seconds','inverse_guard_seconds','block_boundary_clear_seconds','attempt_boundary_clear_seconds']}}
        need(type(b['fallback']) is bool and item['records']<=100000,'invalid block state')
        need(i==semrows[d]['blocks']-1 or item['records']==100000,'physical original block size changed')
        blocks.append(item)
    for d,r in semrows.items():
        subset=[b for b in blocks if b['dataset']==d]
        need(sum(b['raw_bytes'] for b in subset)==r['raw_bytes'],'block total mismatch')
        need(sum(b['semantic_bytes'] for b in subset)==r['semantic_bytes'],'semantic total mismatch')
        need(sum(b['residual_bytes'] for b in subset)==r['residual_bytes'],'residual total mismatch')
        need(sum(b['fallback'] for b in subset)==r['fallback_blocks'],'fallback total mismatch')
        validate_main_suffix(r,subset)
    for row in out:
        if row['method']=='semzip' and row['status']=='PASS':
            t=next(t for t in trained if t['dataset']==row['dataset'])
            need(len(row['program_activity'])==t['specs'],'observed program inventory differs')
            need({v['index'] for v in row['program_activity']}==set(range(t['specs'])),'observed rule identity differs')
    return out,trained,blocks

def validate_main_suffix(row,blocks):
    """Main suffix includes its own manifest; R70 suffix counters exclude it."""
    tail=[b for b in blocks if b['index']>0]
    raw=sum(number(b['raw_bytes'],'suffix block raw bytes',True,True) for b in tail)
    payload=sum(number(b['semantic_bytes'],'suffix semantic bytes',True,True)+number(b['residual_bytes'],'suffix residual bytes',True,True) for b in tail)
    heldout=row['heldout'];manifest=number(heldout['manifest_bytes'],'suffix manifest bytes',False,True)
    need(number(heldout['raw_bytes'],'suffix raw bytes',False,True)==raw,'main suffix raw bytes differ from block ledger')
    need(number(heldout['archive_bytes'],'suffix archive bytes',False,True)==payload+manifest,'main suffix archive differs from block payloads plus its manifest')
    need(manifest>0 if tail else manifest==0,'main suffix manifest does not match empty/nonempty suffix')
    if raw:same(heldout['ratio'],raw/(payload+manifest),'main suffix block-ledger ratio')
    else:need(heldout['ratio'] is None,'one-block main file has a suffix ratio')
    return raw,payload

def formal_records(summary,state,design,size_rows):
    need(summary['version']==FORMAL_VERSION,'formal: wrong version')
    need(summary['timing_scope']==FORMAL_SCOPE,'formal: outer-process timing scope changed')
    need(summary['all_planned_rows_retained'] is True and summary['no_replacement_trials'] is True,'formal: complete fixed schedule required')
    need(state['status'] in {'PASS','COMPLETE_WITH_FAILURES'} and state['hash_lock_unchanged'] is True,'formal: missing final terminal/hash-lock evidence')
    audit=state['post_campaign_archive_audit'];need(audit['status'] in {'PASS','FAIL'},'formal: final archive audit missing')
    if audit['status']=='PASS':need(not audit['changed_results'],'formal: archive audit contradiction')
    else:need(bool(audit['changed_results']),'formal: failed archive audit lacks affected records')
    need(design['version']==FORMAL_VERSION and design['workers']==4 and design['block_records']==100000 and design['repeats']==3,'formal design protocol changed')
    need(design['datasets']==DATASETS[:12] and set(design['methods'])==set(METHODS),'formal design inventory changed')
    need(design['timing_scope']==FORMAL_SCOPE,'formal design timing scope changed')
    need([(r['dataset'],r['method'],r['repeat']) for r in state['completed']]==[(r['dataset'],r['method'],r['repeat']) for r in design['schedule']],'formal trial schedule differs from frozen design')
    small=DATASETS[:12];expected=[(d,m,i) for d in small for m in METHODS for i in [1,2,3]]
    trials=pairs(state['completed'],('dataset','method','repeat'),expected,'formal 216-row schedule')
    source_cells=pairs(summary['cells'],('dataset','method'),[(d,m) for d in small for m in METHODS],'formal 72-cell matrix')
    main={(r['dataset'],r['method']):r for r in size_rows};cells=[];runs=[]
    refs=pairs(design['references'],('dataset',),[(d,) for d in DATASETS],'formal original references')
    for (d,m),row in main.items():
        if row['status']=='PASS':need(row['raw_sha256']==refs[d,]['raw_sha256'] and row['raw_bytes']==int(refs[d,]['raw_bytes']),'formal references disagree with original-file identity')
    need(set(design['first_pass'])=={d+'/'+m for d in small for m in METHODS},'formal first-pass inventory differs')
    planned={(r['dataset'],r['method'],r['repeat']):r for r in design['schedule']}
    for d in small:
        for m in METHODS:
            c=source_cells[d,m];rr=[trials[d,m,i] for i in [1,2,3]]
            first=design['first_pass'][d+'/'+m]
            need(first['status']==main[d,m]['status'],'formal design first-pass terminal status differs')
            if first['status']=='PASS':
                baseline=main[d,m]
                need(first['raw_sha256']==baseline['raw_sha256'] and first['raw_bytes']==baseline['raw_bytes'] and first['archive_bytes']==baseline['archive_bytes'],'formal design first-pass identity differs')
                if m=='semzip':need(baseline['publication_sha256'] in design['lock']['files'].values(),'formal first-pass publication is outside identity lock')
                need(json_hash(first['archive_files'])==digest(first['archive_digest']),'formal first-pass archive ledger digest differs')
                need(sum(v['bytes'] for v in first['archive_files'].values())==first['archive_bytes'],'formal first-pass archive ledger bytes differ')
            need(c['expected_observations']==3 and c['statuses']==[r['status'] for r in rr],'formal: statuses/order differ')
            need(all(r['status'] in FORMAL_STATES for r in rr),'formal: pending/aborted/unknown trial state')
            passed=[r for r in rr if r['status']=='PASS'];need(c['successful_observations']==len(passed),'formal: pass count differs')
            bytes_values=[];digests=[];metrics={k:[] for k in ['encode_MB_per_s','decode_MB_per_s']}
            for r in rr:
                need(planned[d,m,r['repeat']]['eligible']==(first['status']=='PASS'),'formal schedule eligibility differs')
                need((r['status']=='NOT_TIMED_FIRST_PASS_FAILED')==(first['status']!='PASS'),'formal failed cell was timed or eligible cell skipped')
                row={'dataset':d,'method':m,'trial':r['repeat'],'status':r['status'],'timing_kind':'formal',
                     'sha_pass':False,'raw_bytes':None,'archive_bytes':None,'encode_process_wall_seconds':None,'decode_process_wall_seconds':None,'fallback_blocks':None}
                if r['status']=='PASS':
                    baseline=main[d,m];need(baseline['status']=='PASS','formal passed but first-pass row failed')
                    first=design['first_pass'][d+'/'+m]
                    need(first['status']=='PASS' and first['raw_sha256']==baseline['raw_sha256'] and first['raw_bytes']==baseline['raw_bytes'] and first['archive_bytes']==baseline['archive_bytes'],'formal design first-pass identity differs')
                    digest(r['archive_digest'])
                    need(json_hash(r['archive_files'])==r['archive_digest'] and sum(v['bytes'] for v in r['archive_files'].values())==r['archive_bytes'],'formal trial archive ledger differs')
                    need(r['archive_only_decode'] is True,'formal: archive-only decode missing')
                    need(r['raw_bytes']==baseline['raw_bytes'],'formal: original input size differs')
                    number(r['archive_bytes'],'formal archive bytes',True,True)
                    need(digest(r['decoded_sha256'])==digest(r['raw_sha256'])==baseline['raw_sha256'],'formal: decoded identity differs')
                    restored=r['restored_output_metadata_audit']
                    before,after=restored['metadata_before'],restored['metadata_after']
                    changed=[key for key in before if before[key]!=after[key]]
                    need(set(before)==set(after)=={'path','bytes','mtime_ns','device','inode'} and
                         set(changed)<={'mtime_ns'} and set(restored['metadata_changed_fields'])==set(changed) and
                         before['bytes']==after['bytes']==r['raw_bytes'],'formal: decoded metadata differs beyond mtime')
                    row['restored_mtime_changed']=bool(changed)
                    row.update(raw_bytes=r['raw_bytes'],archive_bytes=r['archive_bytes'],sha_pass=True,raw_sha256=r['raw_sha256'],archive_digest=digest(r['archive_digest']))
                    bytes_values.append(row['archive_bytes']);digests.append(row['archive_digest'])
                    for phase in ['encode','decode']:
                        seconds=number(r[phase+'_process']['seconds'],phase+' process seconds',True)
                        need(r[phase+'_process']['returncode']==0,'formal: successful phase has nonzero exit')
                        row[phase+'_process_wall_seconds']=seconds
                        rate=r['raw_bytes']/1e6/seconds;same(rate,r[phase+'_MB_per_s'],'formal process rate')
                        metrics[phase+'_MB_per_s'].append(rate)
                    if m=='semzip':
                        need(r['publication_sha256']==baseline['publication_sha256'] and baseline['publication_sha256'] in design['lock']['files'].values(),'formal publication is not the original locked deployment')
                        row['fallback_blocks']=number(r['implementation_diagnostics']['semantic_fallback_blocks'],'formal fallback count',False,True)
                runs.append(row)
            need(c['archive_bytes']==bytes_values and c['archive_digests']==digests,'formal summary archive list differs')
            consistent=len(passed)==3 and len(set(bytes_values))==len(set(digests))==1
            need(c['three_repeat_archives_identical']==consistent,'formal archive consistency claim differs')
            first_agrees=c['all_successful_archives_equal_first_pass']
            computed_first=bool(passed) and all(r['archive_digest']==design['first_pass'][d+'/'+m].get('archive_digest') for r in passed)
            need(first_agrees==computed_first,'formal first-pass parity claim differs')
            need(type(first_agrees) is bool,'formal first-pass parity flag missing')
            valid=consistent and first_agrees
            expected_status='PASS' if valid else ('NOT_TIMED_FIRST_PASS_FAILED' if main[d,m]['status']!='PASS' else 'INCOMPLETE_OR_INCONSISTENT')
            need(c['status']==expected_status,'formal summary terminal status differs')
            item={'dataset':d,'method':m,'status':c['status'],'statuses':c['statuses'],'expected_observations':3,
                  'successful_observations':len(passed),'archive_bytes':bytes_values,'archive_digests':digests,
                  'three_repeat_archives_identical':consistent,'all_successful_archives_equal_first_pass':first_agrees}
            for metric,values in metrics.items():
                v=c[metric];need(len(v['observations'])==len(values),'formal throughput observations missing')
                for observed,computed in zip(v['observations'],values):same(observed,computed,'formal throughput')
                med=statistics.median(values) if valid else None
                need(v['median'] is None if med is None else v['median'] is not None,'formal median eligibility differs')
                if med is not None:same(v['median'],med,'formal median')
                if values:same(v['min'],min(values),'formal min');same(v['max'],max(values),'formal max')
                else:need(v['min'] is None and v['max'] is None,'formal missing observations have ranges')
                item[metric]={'observations':values,'median':med,'min':min(values) if values else None,'max':max(values) if values else None}
            cells.append(item)
    need((state['status']=='PASS')==all(c['status']=='PASS' for c in cells),'formal terminal state conflicts with failures')
    changed=[r for r in state['completed'] if r['status']=='FAILED_POST_CAMPAIGN_ARCHIVE_AUDIT']
    need((audit['status']=='PASS')==(not changed) and set(audit['changed_results'])=={r['result_path'] for r in changed},'formal final audit disagrees with failure records')
    return cells,runs

def paired_records(rows,parity,size_rows,main_blocks,training):
    need([r['dataset'] for r in rows]==PAIRED,'paired: fixed cohort/order differs')
    need(parity['status']=='PASS' and parity['blocks_checked']==29,'paired: completed 29-block parity evidence required')
    need(parity['all_latent_and_native_payloads_identical'] is True and parity['all_manifests_byte_identical'] is True,'paired: production payload/manifest mismatch')
    need(parity['both_r70_branches_existing_full_and_block_SHA_certificates_retained'] is True,'paired: independent branch certificates missing')
    pd=pairs(parity['datasets'],('dataset',),[(d,) for d in PAIRED],'paired parity inventory')
    main={(r['dataset'],r['method']):r for r in size_rows};mb={(b['dataset'],b['index']):b for b in main_blocks}
    clean=[];certificates=[]
    plans={r['dataset']:r['plan_sha256'] for r in training}
    for r in rows:
        d=r['dataset'];p=pd[d,];m=main[d,'semzip']
        need(r['status']=='PASS' and r['cache_boundary_verified'] is True and r['no_new_api_calls'] is True,'paired: unfinished/uncertified result')
        need(m['status']=='PASS' and r['blocks']==p['blocks']==m['blocks'],'paired: main block inventory mismatch')
        need(digest(r['raw_sha256'])==p['raw_sha256']==m['raw_sha256'],'paired: original file identity mismatch')
        need(digest(r['plan_sha256'])==p['plan_sha256']==plans[d],'paired: plan identity mismatch')
        need(digest(p['source_evidence_sha256']['r71_result_json'])==m['result_sha256'],'paired: production result certificate changed')
        branches={}
        for branch in ['latent','surface']:
            b=r['branches'][branch];item={k:number(b[k],k,False,True) for k in ['archive_bytes','semantic_archive_bytes','residual_archive_bytes','manifest_bytes','suffix_block_archive_bytes']}
            need(item['archive_bytes']==sum(item[k] for k in ['semantic_archive_bytes','residual_archive_bytes','manifest_bytes']),'paired: complete archive accounting mismatch')
            need(item['archive_bytes']==p['r70_totals'][branch]['complete_archive_bytes'],'paired: parity cost differs')
            need(digest(b['full_decode_sha256'])==p['r70_totals'][branch]['certified_full_decode_sha256']==r['raw_sha256'],'paired: branch full decode SHA mismatch')
            item['full_decode_sha256']=b['full_decode_sha256'];item['storage_policy_sha256']=digest(b['storage_policy_sha256']);branches[branch]=item
        need(branches['latent']['archive_bytes']==p['r71_complete_archive_bytes']==m['archive_bytes'],'paired: latent main cost differs')
        need(branches['latent']['residual_archive_bytes']==branches['surface']['residual_archive_bytes'],'paired: shared residual cost differs')
        need(branches['latent']['manifest_bytes']==branches['surface']['manifest_bytes'],'paired: manifest cost differs')
        need(p['all_manifest_bytes_identical'] is True and p['manifests']['latent']==p['manifests']['surface']==p['manifests']['r71'],'paired: manifest ledger differs')
        digest(p['manifests']['r71']['sha256'])
        need(p['manifests']['r71']['bytes']==branches['latent']['manifest_bytes'],'paired: manifest cost and ledger differ')
        manifest=p['manifest_json_values']['latent']
        need(manifest==p['manifest_json_values']['surface'] and json_hash(manifest)==p['manifests']['r71']['sha256'],'paired: canonical manifest differs from recorded identity')
        need(len(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode('utf-8'))==branches['latent']['manifest_bytes'],'paired: canonical manifest length differs')
        suffix_manifest=dict(manifest,block_count=r['blocks']-1,semantic_archives=[f'block_{i:05d}.semantic.tar.xz' for i in range(r['blocks']-1)])
        suffix_manifest_bytes=len(json.dumps(suffix_manifest,sort_keys=True,separators=(',',':')).encode('utf-8')) if r['blocks']>1 else 0
        need(m['heldout']['manifest_bytes']==suffix_manifest_bytes,'paired: main suffix manifest differs from canonical sequential reconstruction')
        details=pairs(p['blocks_detail'],('index',),[(i,) for i in range(r['blocks'])],'paired block certificates')
        branch_sizes={name:{'semantic':0,'native':0,'suffix':0} for name in branches}
        raw_total=0;raw_suffix=0
        for i in range(r['blocks']):
            b=details[i,];need(b['latent_semantic_identical'] is True and b['native_shared_identical_to_production'] is True,'paired: nonidentical block payload')
            need(digest(b['raw_sha256'])==mb[d,i]['raw_sha256'],'paired: main original-block SHA mismatch')
            raw=number(b['raw_bytes'],'paired block raw bytes',True,True)
            need(raw==mb[d,i]['raw_bytes'],'paired: main original-block size mismatch')
            raw_total+=raw;raw_suffix+=raw if i else 0
            cert={'dataset':d,'index':i,'raw_sha256':b['raw_sha256'],'branches':{}}
            for branch in ['latent','surface']:
                info=b['r70_branches'][branch]
                need(digest(info['certified_archive_only_full_decode_sha256'])==digest(info['certified_archive_only_semantic_decode_sha256'])==b['raw_sha256'],'paired: missing branch block decode proof')
                need(info['native']==b['r71_native'],'paired: shared native payload differs')
                if branch=='latent':need(info['semantic']==b['r71_semantic'],'paired: latent semantic payload differs')
                for kind in ['semantic','native']:
                    count=number(info[kind]['bytes'],'paired '+kind+' block bytes',True,True)
                    branch_sizes[branch][kind]+=count
                    if i:branch_sizes[branch]['suffix']+=count
                need(info['native']['bytes']==mb[d,i]['residual_bytes'],'paired: production residual size differs')
                if branch=='latent':need(info['semantic']['bytes']==mb[d,i]['semantic_bytes'],'paired: production semantic size differs')
                cert['branches'][branch]={'decoded_sha256':b['raw_sha256'],'semantic_sha256':digest(info['semantic']['sha256']),'native_sha256':digest(info['native']['sha256'])}
            certificates.append(cert)
        need(raw_total==r['raw_bytes']==m['raw_bytes'] and raw_suffix==r['raw_suffix_bytes'],'paired: original full/suffix size differs from block ledger')
        for branch,item in branches.items():
            counts=branch_sizes[branch];totals=p['r70_totals'][branch]
            need(counts['semantic']==item['semantic_archive_bytes'] and counts['native']==item['residual_archive_bytes'],'paired: component totals differ from block ledger')
            need(counts['suffix']==item['suffix_block_archive_bytes'],'paired: suffix payload differs from block ledger')
            need(counts['semantic']+counts['native']==totals['block_payload_sum_bytes'] and totals['manifest_bytes']==item['manifest_bytes'],'paired: parity aggregate differs from block ledger')
        main_raw,main_payload=validate_main_suffix(m,[mb[d,i] for i in range(r['blocks'])])
        need(main_raw==raw_suffix and main_payload==branches['latent']['suffix_block_archive_bytes'],'paired: main suffix payload differs from latent branch')
        gain=100*(1-branches['latent']['archive_bytes']/branches['surface']['archive_bytes']);same(gain,r['full_archive_saving_pct'],'paired saving')
        suffix=100*(1-branches['latent']['suffix_block_archive_bytes']/branches['surface']['suffix_block_archive_bytes']) if r['blocks']>1 else None
        if suffix is None:need(r['heldout_archive_saving_pct'] is None,'paired: one-block heldout claim')
        else:same(suffix,r['heldout_archive_saving_pct'],'paired suffix saving')
        clean.append({'dataset':d,'blocks':r['blocks'],'raw_sha256':r['raw_sha256'],'plan_sha256':r['plan_sha256'],'branches':branches,'full_archive_saving_pct':gain,'suffix_payload_saving_pct':suffix})
    return clean,certificates

def sampling_records(data,training,blocks):
    need(data['status']=='PASS' and data['api_calls']==0 and data['new_trainings']==0 and data['original_training_modified'] is False,'sampling: separate zero-API replay evidence required')
    need(data['interpretation'].startswith('Separate additional 16-dataset sampling replay, never added to original training.json.seconds as if observed during original training.'),'sampling: original timer must not include separate replay')
    need(number(data['control_finished_unix'],'sampling end') >= number(data['control_started_unix'],'sampling start'),'sampling: invalid observation interval')
    index=pairs(data['datasets'],('dataset',),[(d,) for d in DATASETS],'sampling replay')
    train={r['dataset']:r for r in training};blockmap={(b['dataset'],b['index']):b for b in blocks};rows=[]
    for d in DATASETS:
        r=index[d,];t=train[d]
        need(r['status']=='PASS' and r['api_calls']==0 and r['sample_byte_exact'] is True,'sampling replay failed')
        need(r['timing_scope'].startswith('Additional replay, not recorded original sampling time.'),'sampling: replay timer mislabeled')
        need(digest(r['sample_sha256'])==digest(r['original_sample_sha256']),'sampling bytes changed')
        need(r['sample_physical_records']==t['sampled_lines'] and r['training_physical_records']==r['scanned_records']==t['training_records'],'sampling physical records differ')
        if (d,0) in blockmap:need(digest(r['original_training_block_sha256'])==blockmap[d,0]['raw_sha256'],'sampling training input differs')
        same(r['original_synthesis_subprocess_seconds_excluding_sampling'],t['synthesis_subprocess_seconds_excluding_sampling'],'sampling provenance synthesis timer')
        row={'dataset':d,'sample_sha256':r['sample_sha256'],'training_block_sha256':digest(r['original_training_block_sha256']),
             'sample_physical_records':t['sampled_lines'],'training_physical_records':t['training_records'],
             **{k:number(r[k],k) for k in ['line_count_scan_seconds','sample_writer_seconds_including_second_scan','scan_plus_sample_replay_seconds']}}
        same(row['line_count_scan_seconds']+row['sample_writer_seconds_including_second_scan'],row['scan_plus_sample_replay_seconds'],'sampling timer sum');rows.append(row)
    for source,key in [('line_count_scan_seconds_sum','line_count_scan_seconds'),('sample_writer_seconds_sum','sample_writer_seconds_including_second_scan'),('scan_plus_sample_replay_seconds_sum','scan_plus_sample_replay_seconds')]:same(data[source],sum(r[key] for r in rows),'sampling aggregate')
    return {'scope':'Separate zero-API sampling replay; not original observed sampling time and never added as observed total offline latency. Shared host; prior hash reads; no cache drop.','datasets':rows}

def validate(snapshot,formal,state,design,representation,parity,sampling):
    sizes,training,blocks=size_snapshot(snapshot)
    cells,runs=formal_records(formal,state,design,sizes)
    paired,certs=paired_records(representation,parity,sizes,blocks,training)
    return {'schema':SCHEMA,'protocol':{'datasets':DATASETS,'methods':METHODS,'method_labels':LABELS,'formal_datasets':DATASETS[:12],'formal_repeats':3,'block_records':100000,'workers':4,
        'formal_version':FORMAL_VERSION,'formal_timing_scope':FORMAL_SCOPE,
        'training_scope':'Original synthesis subprocess excludes preceding scanning/sampling. Corrected fit is a separate process measurement. Their sum is component cost, not observed end-to-end offline latency. Sampling replay is not merged.'},
        'sampling_replay':sampling_records(sampling,training,blocks),'size_rows':sizes,'training':training,'main_blocks':blocks,'formal_cells':cells,'formal_runs':runs,'representation':paired,'paired_block_certificates':certs}

def write(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ['snapshot','formal-summary','formal-status','formal-design','representation','parity','sampling-replay']:parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--validate-only',action='store_true')
    args=parser.parse_args();paths={name:getattr(args,name.replace('-','_')) for name in ['snapshot','formal-summary','formal-status','formal-design','representation','parity','sampling-replay']}
    try:
        need(not args.output.exists(),'output: choose a new directory; existing artifacts are never overwritten')
        input_hashes={}
        inputs={key:read(path,key,input_hashes) for key,path in paths.items()}
        need(inputs['formal-status']['config_sha256']==input_hashes['formal-design'],'formal: status does not bind the supplied frozen DESIGN file')
        locked=set(inputs['formal-design']['lock']['files'].values())
        need(input_hashes['representation'] in locked,'paired: result file is outside the frozen formal identity lock')
        need(digest(inputs['parity']['r70_design_sha256']) in locked and digest(inputs['parity']['r70_terminal_status_sha256']) in locked,'paired: parity refers to a different frozen campaign')
        result=validate(inputs['snapshot'],inputs['formal-summary'],inputs['formal-status'],inputs['formal-design'],inputs['representation'],inputs['parity'],inputs['sampling-replay'])
        need(input_hashes=={key:file_hash(path) for key,path in paths.items()},'inputs changed during validation')
        result['input_sha256']=input_hashes
        result['integration_script_sha256']=file_hash(Path(__file__))
        if args.validate_only:
            print(json.dumps({'status':'PASS','validated_only':True,'size_cells':96,'formal_cells':72,'formal_rows':216,'paired_blocks':29}));return 0
        from generate_paper_outputs import generate
        need(args.output.parent.is_dir(),'output parent must already exist')
        with tempfile.TemporaryDirectory(prefix='final-results-',dir=args.output.parent) as tmp:
            stage=Path(tmp)/'bundle';stage.mkdir()
            write(stage/'anonymous_results.json',result)
            eligible={(r['dataset'],r['method']):r['status']=='PASS' for r in result['formal_cells']}
            # The legacy simple report interface does not check archive digests.
            # Preserve its observed trial status while making ineligible cells
            # impossible to mistake for valid formal medians in that interface.
            report_runs=[dict(r,observed_trial_status=r['status'],status='INELIGIBLE_CELL' if r['status']=='PASS' and not eligible[r['dataset'],r['method']] else r['status']) for r in result['formal_runs']]
            write(stage/'final_runs.json',{'runs':report_runs})
            generate(result,stage)
            files={str(p.relative_to(stage)):{'sha256':file_hash(p),'bytes':p.stat().st_size} for p in sorted(stage.rglob('*')) if p.is_file()}
            write(stage/'FILES.json',{'schema':SCHEMA,'files':files})
            need(input_hashes=={key:file_hash(path) for key,path in paths.items()},'inputs changed during output generation')
            need(not args.output.exists(),'output appeared during generation')
            os.rename(stage,args.output)
        print(json.dumps({'status':'PASS','size_cells':96,'formal_cells':72,'formal_rows':216,'paired_blocks':29}));return 0
    except (Invalid,KeyError,TypeError,json.JSONDecodeError,OSError,ImportError) as exc:
        print('VALIDATION_FAILED: '+str(exc),file=sys.stderr);return 2

if __name__=='__main__':raise SystemExit(main())
