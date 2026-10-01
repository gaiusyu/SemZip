"""Fresh fixed-program full-file results; complete materialized decode and audit."""
from pathlib import Path
import hashlib,json,os,subprocess,sys,time,traceback
ROOT=Path(__file__).resolve().parent
SOURCE=ROOT/'source'

def sha(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda:f.read(2**20),b''):h.update(b)
    return h.hexdigest()

def save(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+'.pending')
    temp.write_text(json.dumps(value,indent=2)+'\n');os.replace(temp,path)

def trial(dataset,phase):
    import guarded_backend_v2 as guard
    out=ROOT/'results'/dataset/phase
    assert not out.exists()
    publication=json.loads((ROOT/'training'/dataset/'publication.json').read_text())
    plan=Path(publication['deployed_plan']);storage=Path(publication['storage'])
    assert sha(plan)==publication['deployed_plan_sha256']
    assert sha(storage)==publication['storage_sha256']
    for key in list(os.environ):
        if key.startswith(('PARE_LLM_','YUNWU_','SEMZIP_R53_')):os.environ.pop(key)
    os.environ['SEMZIP_R54_PLAN']=str(storage)
    raw=ROOT.parent/'data/loghub1/raw'/(dataset+'.log')
    started=time.time()
    summary=guard.encode(raw,plan,SOURCE/'runtime',out,dataset,100000,4)
    save(out/'encode_summary.json',summary)
    restored=out/'restored.owned.log'
    env=dict(os.environ,SEMZIP_R54_PLAN=str(out/'nonexistent-external-policy.json'))
    with (out/'decode_process.log').open('w') as log:
        start=time.perf_counter()
        subprocess.run([sys.executable,str(ROOT/'guarded_backend_v2.py'),'decode',
             '--archive',summary['archive_dir'],'--output',str(restored),
             '--semzip-source',str(SOURCE/'runtime'),'--result',str(out/'decode'),
             '--workers','4'],env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        decode_process_seconds=time.perf_counter()-start
    decoded=json.loads((out/'decode/summary.json').read_text())
    assert decoded['decoded_sha256']==summary['raw_sha256']
    assert decoded['decoded_bytes']==summary['raw_bytes']
    audit_start=time.perf_counter()
    # Independent reading of the materialized restored file retains original
    # physical LF boundaries, including the final unterminated record.
    full=hashlib.sha256();blocks=[]
    with restored.open('rb') as stream:
        for original in summary['blocks']:
            h=hashlib.sha256();size=records=0
            for _ in range(100000):
                line=stream.readline()
                if not line:break
                h.update(line);full.update(line);size+=len(line);records+=1
            assert h.hexdigest()==original['raw_sha256'] and size==original['raw_bytes']
            blocks.append({'index':original['index'],'sha256':h.hexdigest(),'bytes':size,'records':records})
        assert not stream.read(1)
    assert full.hexdigest()==summary['raw_sha256']
    archive=Path(summary['archive_dir'])
    archive_files={str(p.relative_to(archive)):{'bytes':p.stat().st_size,'sha256':sha(p)}
                   for p in archive.rglob('*') if p.is_file()}
    assert sum(r['bytes'] for r in archive_files.values())==summary['archive_bytes']
    assert sha(plan)==publication['deployed_plan_sha256'] and sha(storage)==publication['storage_sha256']
    ref=next(r for r in json.loads((ROOT/'reference.json').read_text()) if r['dataset']==dataset)
    assert summary['raw_sha256']==ref['raw_sha256'] and summary['raw_bytes']==int(ref['raw_bytes'])
    assert len(blocks)==int(ref['blocks'])
    suffix_raw=sum(b['raw_bytes'] for b in summary['blocks'][1:])
    # Accounting for a standalone suffix uses unchanged independent block
    # payloads, sequentially renamed, and a normal manifest for that count.
    # Every payload's decoded bytes were already checked above.
    suffix_names={f'chunk_{i}.tar.xz' for i in range(1,len(blocks))}
    suffix_names|={f'semantic/block_{i:05d}.semantic.tar.xz' for i in range(1,len(blocks))}
    suffix_bytes=sum(v['bytes'] for k,v in archive_files.items() if k in suffix_names)
    suffix_manifest_bytes=0
    if suffix_raw:
        manifest=json.loads((archive/'semantic_manifest.json').read_text())
        manifest.update(block_count=len(blocks)-1,
             semantic_archives=[f'block_{i:05d}.semantic.tar.xz' for i in range(len(blocks)-1)])
        suffix_manifest_bytes=len(json.dumps(manifest,separators=(',',':'),sort_keys=True).encode('utf-8'))
        suffix_bytes+=suffix_manifest_bytes
    result={'status':'PASS','dataset':dataset,'phase':phase,'publication':publication,
            'started_unix':started,'finished_unix':time.time(),'encode':summary,'decode':decoded,
            'decode_process_seconds':decode_process_seconds,'audit_seconds':time.perf_counter()-audit_start,
            'independent_materialized_file_sha_pass':True,'block_audit':blocks,'archive_files':archive_files,
            'encode_MB_per_s':summary['raw_bytes']/summary['online_compression_seconds']/1e6,
            'decode_MB_per_s':summary['raw_bytes']/decoded['decode_seconds']/1e6,
            'heldout':{'raw_bytes':suffix_raw,'archive_bytes':suffix_bytes,'manifest_bytes':suffix_manifest_bytes,
                       'ratio':suffix_raw/suffix_bytes if suffix_raw else None,
                       'scope':'Original blocks 1 onward; same independently verified block payloads and recomputed sequential suffix manifest. A payload-cost view, not a separately timed suffix encoding. No held-out claim for one-block files'}}
    save(out/'result.json',result)
    restored.unlink()
    print(json.dumps({'dataset':dataset,'phase':phase,'status':'PASS','ratio':summary['compression_ratio'],
                       'fallback_blocks':summary['semantic_fallback_blocks'],'heldout_ratio':result['heldout']['ratio']}),flush=True)

def main():
    # This launch must follow completed guard regression; the assertion is
    # supplied by the launcher after reading the actual test report.
    assert (ROOT/'GUARD_APPROVED.json').is_file()
    assert not (ROOT/'main_status.json').exists()
    rows=json.loads((ROOT/'reference.json').read_text())
    status={'status':'RUNNING','started_unix':time.time(),'completed':[]}
    save(ROOT/'main_status.json',status)
    save(ROOT/'MAIN_DESIGN.json',{'datasets':[r['dataset'] for r in rows],
         'version':'R71 same fresh first-block gpt-4o + corrected exact fixed storage + cold semantic archive guard + block-local caches',
         'workers':4,'block_records':100000,'first_pass':'Size/correctness only; possible concurrent external first-pass work',
         'formal_timing':'Separate serialized schedule after setup/training/size campaign completes',
         'source_hashes':{str(p.relative_to(SOURCE)):sha(p) for p in SOURCE.rglob('*')
                          if p.is_file() and '__pycache__' not in p.parts and p.suffix!='.pyc'},
         'guard_sha256':sha(ROOT/'guarded_backend_v2.py'),'cache_boundary_sha256':sha(ROOT/'runtime_cache_boundary.py'),'base_guard_sha256':sha(ROOT/'guarded_backend.py'),'campaign_sha256':sha(Path(__file__)),
         'decode_output':'complete materialized file; independent audit afterward'})
    for row in rows:
        dataset=row['dataset'];status.update(dataset=dataset,phase='wait_for_offline_publication');save(ROOT/'main_status.json',status)
        while not (ROOT/'training'/dataset/'publication.json').is_file():
            state=json.loads((ROOT/'training_status.json').read_text())
            if state['status']!='RUNNING':raise RuntimeError('Offline publication missing after training stopped')
            time.sleep(10)
        status['phase']='first_pass';save(ROOT/'main_status.json',status)
        log=ROOT/'results'/dataset/'first_pass_process.log';log.parent.mkdir(parents=True,exist_ok=True)
        try:
            with log.open('w') as stream:
                subprocess.run([sys.executable,'-u',__file__,'trial',dataset,'first_pass'],
                               stdout=stream,stderr=subprocess.STDOUT,check=True)
            data=json.loads((log.parent/'first_pass/result.json').read_text())
            brief={'dataset':dataset,'status':'PASS','ratio':data['encode']['compression_ratio'],
                   'fallback_blocks':data['encode']['semantic_fallback_blocks']}
        except Exception:
            brief={'dataset':dataset,'status':'FAILED','error':traceback.format_exc()}
            save(log.parent/'first_pass_failure.json',brief)
        status['completed'].append(brief);save(ROOT/'main_status.json',status);print(json.dumps(brief),flush=True)
    status.update(status='PASS' if all(r['status']=='PASS' for r in status['completed']) else 'COMPLETE_WITH_FAILURES',
                  finished_unix=time.time());save(ROOT/'main_status.json',status)

if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='trial':trial(sys.argv[2],sys.argv[3])
    else:main()
