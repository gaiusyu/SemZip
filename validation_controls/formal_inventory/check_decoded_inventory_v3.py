"""Content-integrity controls for the new post-decode auditor; no codecs."""
from pathlib import Path
import argparse, hashlib, json, subprocess, sys, time
import codec_baseline as cb
import formal_campaign_v3 as formal

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    a.output.mkdir(exist_ok=False);root=Path(__file__).resolve().parent
    report={'scope':'Auditor controls only; no formal trial, codec, model or replacement measurement',
            'script_sha256':formal.sha(Path(__file__)),'formal_script_sha256':formal.sha(Path(formal.__file__)),
            'retained':[],'fresh':[],'corruption_controls':[]}
    for rel in ['000_Linux_semzip_r1','010_Proxifier_loglite_r1']:
        folder=root/'formal_v2'/rel; expected=json.loads((folder/'input_inventory.json').read_text())
        decoded=folder/'restored.owned.log' if 'semzip' in rel else folder/'work/roundtrip.owned.log'
        observed=formal.inventory_decoded_output(decoded)
        match=observed['raw_sha256']==expected['raw_sha256'] and observed['blocks']==expected['blocks']
        assert match
        report['retained'].append({'trial':rel,'full_and_block_identity_match':match,'observed':observed})
    writer="import pathlib,sys; n=int(sys.argv[2]); pathlib.Path(sys.argv[1]).write_bytes((b'0123456789abcdef\\n'*(n//17+1))[:n])"
    for index in range(32):
        size=[4096,262144,2349686,2420791][index%4];file=a.output/('owned_%s.bin'%index)
        subprocess.run([sys.executable,'-c',writer,str(file),str(size)],check=True)
        observed=formal.inventory_decoded_output(file)
        expected=hashlib.sha256((b'0123456789abcdef\n'*(size//17+1))[:size]).hexdigest()
        assert observed['raw_sha256']==expected
        report['fresh'].append({'index':index,'bytes':size,'expected_sha256':expected,
                                'full_hash_matches':True,'observed':observed})
        file.unlink()
    original=b'first record\nsecond record\nlast without LF'
    reference_file=a.output/'reference.owned.bin';reference_file.write_bytes(original)
    expected=cb.inventory(reference_file)
    for label,payload in [('substitution',original.replace(b'second',b'SECOND')),('truncation',original[:-1]),('append',original+b'\n')]:
        file=a.output/(label+'.owned.bin');file.write_bytes(payload)
        actual=formal.inventory_decoded_output(file)
        accepted=actual['raw_sha256']==expected['raw_sha256'] and actual['blocks']==expected['blocks']
        assert not accepted
        report['corruption_controls'].append({'case':label,'comparison_rejected':not accepted,'observed_sha256':actual['raw_sha256']})
        file.unlink()
    reference_file.unlink()
    report['status']='PASS';report['finished_unix']=time.time()
    (a.output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'status':'PASS','retained_byte_pass':len(report['retained']),'fresh_byte_pass':len(report['fresh']),
                      'fresh_recorded_metadata_changes':sum(bool(x['observed']['metadata_changed_fields']) for x in report['fresh']),
                      'corruptions_rejected':len(report['corruption_controls'])}))

if __name__=='__main__':main()
