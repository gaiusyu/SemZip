#!/usr/bin/env python3
"""Summarize existing independent block archives, without reading logs/archives.

This is a suffix archive-cost view, not a new encoding or timing experiment.
Only an existing COMPLETE snapshot plus complete terminal first-pass ledgers can
produce TeX. Missing, failed, invalid and one-block observations remain explicit.
"""
from pathlib import Path
import argparse,datetime,hashlib,json,math,os,statistics,tempfile

DATASETS=['Linux','Proxifier','Apache','Zookeeper','Mac','HealthApp','HPC','Hadoop','OpenStack','OpenSSH','Android','BGL','HDFS','Spark','Windows','Thunderbird']
METHODS=['semzip','delog','loglite','gzip6','xz6','zstd3']
TERMINAL={'PASS','FAIL','FAILED'}
LABELS={'semzip':'SemZip','delog':'DeLog','loglite':'LogLite-BL*','gzip6':'gzip6','xz6':'XZ6','zstd3':'Zstd3'}
VERSION='R71-NONTRAINING-SUFFIX-COST-V1'
MANIFEST_VERSION='SEMZIP-SEMANTIC-UTC-C-V1-20260915'

def need(ok,message):
    if not ok:raise ValueError(message)

def count(value,name,positive=False):
    need(type(value) is int and value>=(1 if positive else 0),name+': invalid integer')
    return value

def sha_bytes(value):return hashlib.sha256(value).hexdigest()
def digest(value):return sha_bytes(json.dumps(value,sort_keys=True,separators=(',',':')).encode())
def sha(value):
    need(isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value),'Invalid SHA-256')
    return value

def manifest_size(dataset,blocks):
    # Exact frozen R71 writer schema: no inferred per-dataset header choice.
    value={'version':MANIFEST_VERSION,'dataset':dataset,'block_size':100000,
           'block_count':blocks,'semantic_archives':[f'block_{i:05d}.semantic.tar.xz' for i in range(blocks)]}
    return len(json.dumps(value,sort_keys=True,separators=(',',':')).encode('utf-8'))

def read(path,label,inputs,availability):
    if not path.is_file():
        inputs[label]=None;availability[label]='MISSING';return None
    raw=path.read_bytes();inputs[label]=sha_bytes(raw);availability[label]='PRESENT'
    def pairs(items):
        result={}
        for key,value in items:
            need(key not in result,'Duplicate JSON key in '+label);result[key]=value
        return result
    def bad(value):raise ValueError('Nonfinite JSON constant in '+label)
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=bad)

def block_identity(block,external):
    result={k:block[k] for k in ['index','raw_bytes','records','raw_sha256']}
    count(result['index'],'block index');count(result['raw_bytes'],'block raw bytes',True)
    count(result['records'],'block records',True);sha(result['raw_sha256'])
    need(result['records']<=100000,'Block exceeds 100000 physical records')
    if external:need(block['decoded_sha256']==result['raw_sha256'],'External block decoding identity differs')
    return result

def successful(row,method,reference,blocks,snapshot_row):
    n=int(reference['blocks']);external=method!='semzip'
    need(row['raw_sha256']==reference['raw_sha256'] and row['raw_bytes']==int(reference['raw_bytes']),'Original file identity differs from reference')
    need(len(blocks)==n and [b['index'] for b in blocks]==list(range(n)),'Block indices differ from original full-file partition')
    identities=[block_identity(b,external) for b in blocks]
    need(all(b['records']==100000 for b in identities[:-1]),'Original non-final block size changed')
    need(sum(b['raw_bytes'] for b in identities)==row['raw_bytes'],'Raw block total differs')
    if external:
        need(row['trial']==1 and row['block_records']==100000,'Not the fixed first-pass block protocol')
        need(row['roundtrip']=='byte-exact' and row['archive_only_decode'] is True and row['decoded_sha256']==row['raw_sha256'],'Full external reconstruction certificate differs')
        for b in blocks:count(b['archive_bytes'],'block archive bytes',True);sha(b['archive_sha256'])
        need(sum(b['archive_bytes'] for b in blocks)==row['archive_bytes'],'Complete external archive total differs')
        need(sum(b['records'] for b in blocks)==row['records'],'External record total differs')
        payload=sum(b['archive_bytes'] for b in blocks[1:]);manifest=0
        archive_items=[{'index':b['index'],'archive_bytes':b['archive_bytes'],'archive_sha256':b['archive_sha256']} for b in blocks[1:]]
        if snapshot_row['status']=='PASS':
            need(snapshot_row['raw_sha256']==row['raw_sha256'] and snapshot_row['raw_bytes']==row['raw_bytes'] and snapshot_row['archive_bytes']==row['archive_bytes'] and snapshot_row['blocks']==n,'Snapshot and external first-pass identity differ')
            need(snapshot_row['codec_spec']==row['codec_spec'],'Snapshot and external method configuration differ')
    else:
        need(row['blocks']==n,'Main block count differs')
        for b in blocks:count(b['semantic_bytes'],'semantic block bytes',True);count(b['residual_bytes'],'residual block bytes',True)
        need(sum(b['semantic_bytes'] for b in blocks)==row['semantic_bytes'] and sum(b['residual_bytes'] for b in blocks)==row['residual_bytes'],'Main component totals differ')
        need(row['archive_bytes']>row['semantic_bytes']+row['residual_bytes'],'Main file manifest missing')
        need(row['archive_bytes']-row['semantic_bytes']-row['residual_bytes']==manifest_size(row['dataset'],n),'Complete manifest size does not match frozen R71 serialization')
        sha(row['result_sha256']);sha(row['publication_sha256'])
        payload=sum(b['semantic_bytes']+b['residual_bytes'] for b in blocks[1:])
        h=row['heldout'];manifest=count(h['manifest_bytes'],'recomputed suffix manifest bytes')
        need(h['raw_bytes']==sum(b['raw_bytes'] for b in blocks[1:]),'Main raw suffix differs from block ledger')
        need(h['archive_bytes']==payload+manifest,'Main suffix differs from block payloads plus recomputed manifest')
        need(manifest>0 if n>1 else manifest==0,'Main suffix manifest inconsistent with block count')
        need(manifest==(manifest_size(row['dataset'],n-1) if n>1 else 0),'Suffix manifest size does not match frozen sequential serialization')
        if n>1:need(type(h['ratio']) in (int,float) and math.isclose(h['ratio'],h['raw_bytes']/h['archive_bytes'],rel_tol=1e-12),'Main suffix ratio differs')
        else:need(h['ratio'] is None,'One-block file claims a suffix')
        archive_items=[{k:b[k] for k in ['index','semantic_bytes','residual_bytes']} for b in blocks[1:]]
    raw=sum(b['raw_bytes'] for b in blocks[1:]);records=sum(b['records'] for b in blocks[1:])
    return {'suffix_raw_bytes':raw,'suffix_records':records,'suffix_payload_bytes':payload,
            'suffix_manifest_bytes':manifest,'suffix_archive_bytes':payload+manifest,
            'suffix_ratio':raw/(payload+manifest) if raw else None,
            'suffix_block_cost_ledger_sha256':digest(archive_items)},identities

def build(snapshot,references,ledgers,input_sha256,availability):
    need(snapshot is not None and references is not None,'Snapshot and reference are required')
    need(snapshot['status'] in {'COMPLETE','IN_PROGRESS'},'Unexpected snapshot state')
    need([r['dataset'] for r in references]==DATASETS and snapshot['expected_datasets']==DATASETS,'Fixed original dataset order differs')
    need(len(references)==16 and set(snapshot['methods'])==set(METHODS),'Dataset/method inventory differs')
    refs={r['dataset']:r for r in references}
    for r in references:need(int(r['blocks'])>0 and int(r['raw_bytes'])>0,'Invalid reference sizes');sha(r['raw_sha256'])
    matrix={(r['dataset'],r['method']):r for r in snapshot['rows']}
    need(len(matrix)==len(snapshot['rows'])==96 and set(matrix)=={(d,m) for d in DATASETS for m in METHODS},'Snapshot is not the fixed 96-cell matrix')
    external={};ledger_complete=bool(ledgers) and all(x is not None and x.get('complete') is True for x in ledgers)
    for ledger in ledgers:
        if ledger is None:continue
        for row in ledger['runs']:
            key=(row['dataset'],row['codec'])
            need(key[0] in DATASETS and key[1] in METHODS[1:] and row['trial']==1,'Unexpected external first-pass identity')
            need(key not in external,'Duplicate external attempt; do not select a winner or hide a failure')
            external[key]=row
    main_blocks={d:sorted([b for b in snapshot['semzip_blocks'] if b['dataset']==d],key=lambda b:b['index']) for d in DATASETS}
    rows=[];certificates=[];problems=[]
    for d in DATASETS:
        ref=refs[d];n=int(ref['blocks']);dataset_rows=[];verified={}
        for method in METHODS:
            original=matrix[d,method];source=original if method=='semzip' else external.get((d,method))
            row={'dataset':d,'method':method,'original_blocks':n,'suffix_first_block_index':1 if n>1 else None,'suffix_last_block_index':n-1 if n>1 else None,'suffix_blocks':max(0,n-1),'snapshot_status':original['status'],'source_status':source.get('status') if source else None}
            if source is None:row['status']='MISSING_LEDGER'
            elif source['status'] not in TERMINAL:row['status']='PENDING'
            elif source['status']!='PASS':
                row.update(status=source['status'],failure_record_sha256=digest(source.get('failure_record',source)))
            else:
                try:
                    need(original['status'] in {'PASS','PENDING'},'Snapshot failure conflicts with successful external ledger')
                    if snapshot['status']=='COMPLETE':need(original['status']=='PASS','Final snapshot and source status differ')
                    costs,identity=successful(source,method,ref,main_blocks[d] if method=='semzip' else source['blocks'],original)
                    row.update(costs,status='PASS');verified[method]=identity
                    if method!='semzip':row['method_configuration_sha256']=digest(source['codec_spec'])
                except (ValueError,KeyError,TypeError,ZeroDivisionError) as exc:
                    row.update(status='INVALID_EVIDENCE',reason=str(exc));problems.append({'dataset':d,'method':method,'reason':str(exc)})
            if source and source['status'] in TERMINAL and original['status'] in TERMINAL and source['status']!=original['status']:
                row.update(status='INVALID_EVIDENCE',reason='Terminal snapshot/source statuses differ');problems.append({'dataset':d,'method':method,'reason':row['reason']})
            row['observation_status']=row['status']
            if n==1:row['status']='EXCLUDED_ONE_BLOCK'
            dataset_rows.append(row)
        identities=list(verified.values())
        consistent=not identities or all(x==identities[0] for x in identities)
        if not consistent:
            problems.append({'dataset':d,'reason':'Methods disagree on original block index/raw SHA/bytes/records'})
            for r in dataset_rows:
                if r['observation_status']=='PASS':r['observation_status']='INVALID_EVIDENCE';r['status']='INVALID_EVIDENCE' if n>1 else 'EXCLUDED_ONE_BLOCK'
                for key in ['suffix_ratio','suffix_archive_bytes','suffix_payload_bytes','suffix_manifest_bytes']:r.pop(key,None)
        elif identities:
            tail=identities[0][1:]
            certificate={'dataset':d,'original_blocks':n,'verified_methods':list(verified),'suffix_raw_bytes':sum(x['raw_bytes'] for x in tail),'suffix_records':sum(x['records'] for x in tail),'blocks':tail,'scope':'Existing recorded block certificates; no fresh suffix-file SHA or decoding is claimed'}
            certificate['raw_block_ledger_sha256']=digest(tail);certificates.append(certificate)
            for r in dataset_rows:r['suffix_raw_bytes']=certificate['suffix_raw_bytes'];r['suffix_records']=certificate['suffix_records'];r['raw_block_ledger_sha256']=certificate['raw_block_ledger_sha256']
        rows.extend(dataset_rows)
    eligible=[d for d in DATASETS if int(refs[d]['blocks'])>1]
    for method in METHODS[1:]:
        identities={r['method_configuration_sha256'] for r in rows if r['method']==method and 'method_configuration_sha256' in r}
        if len(identities)>1:problems.append({'method':method,'reason':'External method configuration varies across datasets'})
    terminal=all(r['observation_status'] in TERMINAL for r in rows)
    ready=snapshot['status']=='COMPLETE' and ledger_complete and len(external)==80 and terminal and not problems
    aggregates={}
    for method in METHODS:
        selected=[r for r in rows if r['method']==method and r['dataset'] in eligible]
        passed=[r for r in selected if r['status']=='PASS']
        item={'expected_count':len(eligible),'pass_count':len(passed),'eligible_datasets':eligible,'complete':ready and len(passed)==len(eligible),'nonpassing_datasets':[r['dataset'] for r in selected if r['status']!='PASS']}
        if item['complete']:
            item.update(arithmetic_mean_ratio=statistics.mean(r['suffix_ratio'] for r in passed),total_raw_bytes=sum(r['suffix_raw_bytes'] for r in passed),total_archive_bytes=sum(r['suffix_archive_bytes'] for r in passed))
            item['corpus_ratio']=item['total_raw_bytes']/item['total_archive_bytes']
        aggregates[method]=item
    return {'version':VERSION,'status':'INVALID_EVIDENCE' if problems else 'COMPLETE' if ready else 'IN_PROGRESS','paper_ready':ready,'snapshot_status':snapshot['status'],'fixed_nontraining_cohort':eligible,'excluded_one_block_datasets':[d for d in DATASETS if d not in eligible],'methods':METHODS,'rows':rows,'block_identity_certificates':certificates,'aggregates':aggregates,'problems':problems,'input_sha256':input_sha256,'input_availability':availability,'script_sha256':sha_bytes(Path(__file__).read_bytes()),'scope':'Suffix archive-cost view from original blocks 1 onward. SemZip includes its recorded reconstructed sequential suffix manifest; external methods retain the existing counted independent-file convention, without fabricated headers. Payload costs are also retained, distinct from manifest-inclusive main suffix costs. No new compression, decoding, API call, throughput, or fresh raw/archive hash audit.'}

def render(data,out):
    need(data['paper_ready'],'Final TeX requires complete terminal input ledgers')
    lookup={(r['dataset'],r['method']):r for r in data['rows']}
    lines=[r'\begin{table*}[t]\centering\small',r'\caption{Nontraining suffix archive-cost view on the fixed '+str(len(data['fixed_nontraining_cohort']))+r' original multi-block files. Every method uses the same original blocks 1 onward. Ratios count SemZip\textquotesingle s reconstructed suffix manifest and each external method\textquotesingle s existing independent archive files. No suffix is recompressed or separately timed. One-block files are excluded. Failed observations receive no ratio or reduced-denominator aggregate. *LogLite-BL-wide+tail+reservefix.}',r'\label{tab:external-suffix}',r'\begin{tabular}{lrrrrrr}\toprule','Dataset & '+' & '.join(LABELS[m] for m in METHODS)+r' \\ \midrule']
    for d in data['fixed_nontraining_cohort']:
        lines.append(d+' & '+' & '.join(f"{lookup[d,m]['suffix_ratio']:.2f}" if lookup[d,m]['status']=='PASS' else r'\textsc{fail}' for m in METHODS)+r' \\')
    for label,key in [('Arithmetic mean ('+str(len(data['fixed_nontraining_cohort']))+')','arithmetic_mean_ratio'),('Total suffix raw / archive','corpus_ratio')]:
        lines.append(label+' & '+' & '.join(f"{data['aggregates'][m][key]:.2f}" if data['aggregates'][m]['complete'] else '--' for m in METHODS)+r' \\')
    (out/'suffix_comparison_table.tex').write_text('\n'.join(lines+[r'\bottomrule\end{tabular}\end{table*}'])+'\n')
    detail=[r'\begin{longtable}{llrrrrl}',r'\caption{Suffix costs in bytes. Payload is the sum of existing selected block archive files; SemZip additionally counts its reconstructed suffix manifest. A failure is a recorded terminal outcome, not a zero.}\\',r'\toprule Dataset & Method & Raw & Payload & Manifest & Archive & Status \\ \midrule\endhead']
    for row in data['rows']:
        if row['dataset'] not in data['fixed_nontraining_cohort']:continue
        values=[f"{row[k]:,}" if row['status']=='PASS' else '--' for k in ['suffix_raw_bytes','suffix_payload_bytes','suffix_manifest_bytes','suffix_archive_bytes']]
        detail.append(' & '.join([row['dataset'],LABELS[row['method']]]+values+[row['status'].replace('_',r'\_')])+r' \\')
    (out/'suffix_cost_details.tex').write_text('\n'.join(detail+[r'\bottomrule\end{longtable}'])+'\n')
    failures=sum(r['status'] in {'FAIL','FAILED'} for r in data['rows'] if r['dataset'] in data['fixed_nontraining_cohort'])
    prose=f"The suffix comparison retains the same {len(data['fixed_nontraining_cohort'])} multi-block original files for all six methods and excludes {len(data['excluded_one_block_datasets'])} one-block files."
    paired=[(lookup[d,'semzip'],lookup[d,'delog']) for d in data['fixed_nontraining_cohort']
            if lookup[d,'semzip']['status']==lookup[d,'delog']['status']=='PASS']
    if paired:
        wins=sum(s['suffix_archive_bytes']<b['suffix_archive_bytes'] for s,b in paired)
        prose+=f" SemZip has smaller suffix archives than DeLog on {wins}/{len(paired)} jointly valid files."
    sem,de=data['aggregates']['semzip'],data['aggregates']['delog']
    if sem['complete'] and de['complete']:
        delta=100*(sem['total_archive_bytes']/de['total_archive_bytes']-1)
        prose+=f" Their respective arithmetic means are {sem['arithmetic_mean_ratio']:.2f}$\\times$ and {de['arithmetic_mean_ratio']:.2f}$\\times$, and corpus ratios are {sem['corpus_ratio']:.2f}$\\times$ and {de['corpus_ratio']:.2f}$\\times$."
        prose+=(' SemZip\'s summed suffix archive bytes are unchanged relative to DeLog.' if delta==0 else
                f" SemZip's summed suffix archive bytes are {abs(delta):.2f}\\% {'larger' if delta>0 else 'smaller'} than DeLog.")
    prose+=" The suffix and sixteen-file means use different dataset cohorts; their difference does not isolate the effect of removing training blocks."
    if failures:
        prose+=f" All {failures} failed suffix observations retain their complete-file failure; partial block success does not replace it."
    prose+=" This view uses existing independently checked block payloads, with SemZip's suffix manifest counted. It has no separate suffix timing; matched-representation suffix savings instead compare payloads only."
    (out/'suffix_comparison_text.tex').write_text(prose+'\n')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot',type=Path,required=True);parser.add_argument('--reference',type=Path,required=True)
    parser.add_argument('--external-results',type=Path,action='append',required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--emit-tex',action='store_true')
    args=parser.parse_args();need(not args.output.exists() and args.output.parent.is_dir(),'Output must be a new directory under an existing parent')
    paths={'snapshot':args.snapshot,'reference':args.reference,**{'external_results_'+str(i):p for i,p in enumerate(args.external_results)}}
    identities={};availability={};inputs={key:read(path,key,identities,availability) for key,path in paths.items()}
    result=build(inputs['snapshot'],inputs['reference'],[inputs['external_results_'+str(i)] for i in range(len(args.external_results))],identities,availability)
    need(identities=={key:sha_bytes(path.read_bytes()) if path.is_file() else None for key,path in paths.items()},'Inputs changed while auditing')
    with tempfile.TemporaryDirectory(prefix='suffix-cost-',dir=args.output.parent) as temp:
        stage=Path(temp)/'outputs';stage.mkdir()
        if args.emit_tex and result['paper_ready']:render(result,stage)
        result['output_sha256']={p.name:sha_bytes(p.read_bytes()) for p in stage.iterdir()}
        (stage/'suffix_comparison.json').write_text(json.dumps(result,indent=2,sort_keys=True,allow_nan=False)+'\n')
        need(identities=={key:sha_bytes(path.read_bytes()) if path.is_file() else None for key,path in paths.items()},'Inputs changed before output publication')
        need(not args.output.exists(),'Output appeared during generation');os.rename(stage,args.output)
    print(json.dumps({'status':result['status'],'paper_ready':result['paper_ready'],'suffix_datasets':len(result['fixed_nontraining_cohort']),'rows':len(result['rows']),'output':str(args.output)}))
    return 2 if result['status']=='INVALID_EVIDENCE' or args.emit_tex and not result['paper_ready'] else 0

if __name__=='__main__':raise SystemExit(main())
