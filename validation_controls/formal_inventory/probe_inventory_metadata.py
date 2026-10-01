"""Read retained failed outputs and probe fresh owned files; no codec is run."""
from pathlib import Path
import argparse, hashlib, json, os, subprocess, sys, time
import codec_baseline as cb

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.mkdir(exist_ok=False)
    root=Path(__file__).resolve().parent
    report={'scope':'Read-only retained-output checks plus newly owned metadata probes; zero codecs or model calls',
            'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'codec_baseline_sha256':hashlib.sha256(Path(cb.__file__).read_bytes()).hexdigest(),
            'retained':[],'fresh':[]}
    original_identity=cb.file_identity
    def observe(path):
        seen=[]
        def logged(q):
            d=original_identity(q);seen.append(d);return d
        cb.file_identity=logged
        try:
            inv=cb.inventory(path)
            row={'status':'PASS','inventory':inv}
        except Exception as exc:row={'status':'FAILED','error':str(exc)}
        finally:cb.file_identity=original_identity
        row['identities']=seen
        row['metadata_changes']={k:[seen[0][k],seen[-1][k]] for k in seen[0] if seen[0][k]!=seen[-1][k]} if seen else {}
        return row
    for rel in ['000_Linux_semzip_r1','010_Proxifier_loglite_r1']:
        folder=root/'formal_v2'/rel
        decoded=folder/'restored.owned.log' if 'semzip' in rel else folder/'work/roundtrip.owned.log'
        expected=json.loads((folder/'input_inventory.json').read_text())
        record={'trial':rel,'expected_sha256':expected['raw_sha256'],'expected_bytes':expected['raw_bytes'],**observe(decoded)}
        record['current_full_and_blocks_match']=record['status']=='PASS' and record['inventory']['raw_sha256']==expected['raw_sha256'] and record['inventory']['blocks']==expected['blocks']
        report['retained'].append(record)
    writer="import pathlib,sys; p=pathlib.Path(sys.argv[1]); n=int(sys.argv[2]); p.write_bytes((b'0123456789abcdef\\n'*(n//17+1))[:n])"
    for repeat in range(4):
        for size in [4096,262144,2349686,2420791]:
            for mode in ['immediate','fsync_before_scan']:
                file=a.output/('owned_%s_%s_%s.bin'%(repeat,size,mode))
                subprocess.run([sys.executable,'-c',writer,str(file),str(size)],check=True)
                if mode=='fsync_before_scan':
                    with file.open('rb') as stream:os.fsync(stream.fileno())
                row={'repeat':repeat,'size':size,'mode':mode,**observe(file)}
                expected=hashlib.sha256((b'0123456789abcdef\n'*(size//17+1))[:size]).hexdigest()
                row['expected_sha256']=expected
                if row['status']=='PASS':row['expected_bytes_match']=row['inventory']['raw_sha256']==expected
                report['fresh'].append(row)
                file.unlink()
    report['finished_unix']=time.time()
    (a.output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'retained_byte_pass':sum(x['current_full_and_blocks_match'] for x in report['retained']),
                      'fresh_checks':len(report['fresh']),'fresh_inventory_failures':sum(x['status']!='PASS' for x in report['fresh']),
                      'metadata_changes':[x['metadata_changes'] for x in report['fresh'] if x['metadata_changes']]}))

if __name__=='__main__':main()
