#!/usr/bin/env python3
"""Portable offline replay of frozen SemZip deployments (main pooled selection or SemZip-1), without model calls.

Deployment sets: `main` = the paper's SemZip deployments (deployments/main_pool/, metadata/deployments_main_pool.json);
`semzip1` = the single greedy synthesis SemZip-1 (deployments/<Dataset>/, metadata/deployments.json).
Both run through the same frozen guarded runtime (frozen/guarded_backend_v2.py) and frozen source/."""
from pathlib import Path
import argparse,hashlib,json,os,subprocess,sys,time

ROOT=Path(__file__).resolve().parent
DEPLOYMENT_SETS={'main':'metadata/deployments_main_pool.json','semzip1':'metadata/deployments.json','r71':'metadata/deployments.json'}
def read(p):return json.loads(Path(p).read_text())
def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(1048576),b''):h.update(b)
    return h.hexdigest()
def save(p,value):p.write_text(json.dumps(value,indent=2)+'\n')
def verify_frozen(source,allow_rebuilt):
    p=read(ROOT/'metadata/source_provenance.json')
    for path,digest in p['source_files'].items():
        if sha(source/path)!=digest:
            # Rebuilt binaries are explicitly labeled and never claimed to
            # retain the original binary hash. All nonbinary source must match.
            original=ROOT/'source'/path
            if not (allow_rebuilt and original.read_bytes()[:4]==b'\x7fELF'):
                raise RuntimeError('Frozen source hash mismatch: '+path)
    for path,digest in p['wrappers'].items():
        if sha(ROOT/'frozen'/path)!=digest:raise RuntimeError('Frozen wrapper hash mismatch: '+path)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['encode','decode','roundtrip','verify-input','verify-deployments'])
    parser.add_argument('--deployment-set',choices=sorted(DEPLOYMENT_SETS),default='main',
                        help='main: paper SemZip (pooled selection, default); semzip1 (alias r71): single greedy synthesis')
    parser.add_argument('--dataset')
    parser.add_argument('--input',type=Path)
    parser.add_argument('--archive',type=Path)
    parser.add_argument('--result',type=Path)
    parser.add_argument('--output',type=Path)
    parser.add_argument('--workers',type=int,default=4,choices=range(1,5))
    parser.add_argument('--source-root',type=Path,default=ROOT/'source')
    parser.add_argument('--allow-rebuilt-binaries',action='store_true')
    args=parser.parse_args()
    deployments=read(ROOT/DEPLOYMENT_SETS[args.deployment_set])['datasets']
    if args.mode=='verify-deployments':
        # Hash-only check of the frozen source/guards and every program/policy of the selected set; runs no codec.
        verify_frozen(args.source_root.resolve(),args.allow_rebuilt_binaries)
        bad=[d['dataset'] for d in deployments if sha(ROOT/d['program'])!=d['program_sha256'] or sha(ROOT/d['storage'])!=d['storage_sha256']]
        print(json.dumps({'status':'FAIL' if bad else 'PASS','deployment_set':args.deployment_set,'datasets':len(deployments),'mismatch':bad}))
        if bad:raise SystemExit(2)
        return
    if args.mode!='decode':
        if args.dataset not in {d['dataset'] for d in deployments} or args.input is None:
            parser.error('encode, roundtrip and verify-input require a known --dataset and --input')
        row=next(d for d in deployments if d['dataset']==args.dataset)
        expected=next(d for d in read(ROOT/'metadata/raw_datasets.json')['datasets'] if d['dataset']==args.dataset)
        if args.input.stat().st_size!=expected['raw_bytes'] or sha(args.input)!=expected['sha256']:
            raise RuntimeError('Input differs from the complete original dataset. Never normalize or truncate it.')
        if args.mode=='verify-input':
            print(json.dumps({'status':'PASS','dataset':args.dataset,'raw_sha256':expected['sha256']}));return
    if args.result is None:parser.error('--result NEW_DIRECTORY is required')
    result=args.result.resolve();result.mkdir(parents=True,exist_ok=False)
    source=args.source_root.resolve()
    verify_frozen(source,args.allow_rebuilt_binaries)
    environment=dict(os.environ)
    for key in list(environment):
        if key.startswith(('PARE_','YUNWU_','SEMZIP_')) or key.endswith('_API_KEY'):
            environment.pop(key)
    # Frozen replay never invokes the trainer. Removing API configuration is
    # defense in depth, not an assertion that arbitrary imported source is safe.
    command=[sys.executable,str(ROOT/'frozen/guarded_backend_v2.py')]
    base=['--semzip-source',str(source/'runtime'),'--workers',str(args.workers)]
    report={'version':'portable-replay-v2','deployment_set':args.deployment_set,'status':'RUNNING','mode':args.mode,
            'api_calls':0,'block_records':100000,'workers':args.workers,
            'rebuilt_binaries_allowed':args.allow_rebuilt_binaries,
            'actual_native_binary_sha256':{name:sha(source/'backend'/name) for name in ['Delog_plan_compress','decompress']},
            'timing_scope':'Diagnostic outer process wall, includes startup and frozen internal guards/hashes; external input and output audits excluded. Not the formal paper campaign.'}
    if args.mode!='decode':
        plan=ROOT/row['program'];policy=ROOT/row['storage']
        if sha(plan)!=row['program_sha256'] or sha(policy)!=row['storage_sha256']:
            raise RuntimeError('Frozen program/policy differs from its publication')
        environment['SEMZIP_R54_PLAN']=str(policy)
        with (result/'encode.log').open('wb') as log:
            start=time.perf_counter()
            subprocess.run(command+['encode','--input',str(args.input.resolve()),'--dataset',args.dataset,
                '--plan',str(plan),'--result',str(result/'encode')]+base,env=environment,
                stdout=log,stderr=subprocess.STDOUT,check=True)
            report['encode_process_wall_seconds']=time.perf_counter()-start
        encoded=read(result/'encode/summary.json')
        archive=Path(encoded['archive_dir'])
        report.update(dataset=args.dataset,raw_bytes=encoded['raw_bytes'],raw_sha256=encoded['raw_sha256'],
            archive_bytes=encoded['archive_bytes'],compression_ratio=encoded['compression_ratio'],
            fallback_blocks=encoded['semantic_fallback_blocks'],
            program_sha256=row['program_sha256'],storage_sha256=row['storage_sha256'])
    else:
        if args.archive is None or args.output is None:parser.error('decode requires --archive and --output')
        archive=args.archive.resolve()
    if args.mode in ('decode','roundtrip'):
        destination=args.output.resolve() if args.output else result/'restored.owned.log'
        destination.parent.mkdir(parents=True,exist_ok=True)
        environment['SEMZIP_R54_PLAN']=str(result/'INTENTIONALLY_MISSING_POLICY')
        with (result/'decode.log').open('wb') as log:
            start=time.perf_counter()
            subprocess.run(command+['decode','--archive',str(archive),'--output',str(destination),
                '--result',str(result/'decode')]+base,env=environment,
                stdout=log,stderr=subprocess.STDOUT,check=True)
            report['decode_process_wall_seconds']=time.perf_counter()-start
        decoded=read(result/'decode/summary.json')
        actual=sha(destination)
        if actual!=decoded['decoded_sha256']:raise RuntimeError('Materialized output SHA disagrees with decoder')
        if args.mode=='roundtrip' and (actual!=expected['sha256'] or destination.stat().st_size!=expected['raw_bytes']):
            raise RuntimeError('Independent archive decode differs from original bytes')
        report.update(decoded_sha256=actual,decoded_bytes=destination.stat().st_size,
                      archive_only_decode=True,materialized_output_sha_pass=True)
    report['archive_files']={str(p.relative_to(archive)):{'bytes':p.stat().st_size,'sha256':sha(p)}
                             for p in sorted(archive.rglob('*')) if p.is_file()}
    report['status']='PASS';save(result/'result.json',report)
    print(json.dumps({k:v for k,v in report.items() if k!='archive_files'},indent=2))

if __name__=='__main__':main()
