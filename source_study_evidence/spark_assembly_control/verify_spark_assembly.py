#!/usr/bin/env python3
"""Read-only verification of two historical Spark assembly candidates.

No extraction, assembled-file output, network, API or codec benchmark. For
nonlexical tar order, verify each historical payload segment against actual tar
member SHA-256 first; this binds the hash inputs to the original tar bytes.
"""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import sys
import tarfile
import time

CHUNK = 8 * 1024 * 1024
HIST_BYTES = 2941228156
HIST_SHA = 'f4aca8610fc015b4c33dd88865608e127adf44a713d0379447ee537e5da880d5'

def save(path, value):
    path.write_text(json.dumps(value,indent=2)+'\n')

def identity(path):
    s=path.stat()
    return {'bytes':s.st_size,'mtime_ns':s.st_mtime_ns,'inode':s.st_ino}

def run(args):
    started=time.perf_counter()
    args.output.mkdir(parents=True,exist_ok=True)
    if not args.archive.is_file() or not args.historical.is_file():
        raise FileNotFoundError('Both existing inputs are required; downloading is prohibited.')
    reference=json.loads(args.reference.read_text())
    ref=next(r for r in reference['datasets'] if r['dataset']=='Spark')
    members=ref['members']
    names=[r['path'] for r in members]
    if names!=sorted(names) or len(names)!=len(set(names)):
        raise ValueError('Reference is not a unique lexical member order.')
    expected={r['path']:r for r in members}
    before={k:identity(p) for k,p in [('archive',args.archive),('historical',args.historical)]}
    archive_sha=hashlib.sha256(); archive_md5=hashlib.md5(); compressed_bytes=0
    with args.archive.open('rb') as f:
        for b in iter(lambda:f.read(CHUNK),b''):
            archive_sha.update(b); archive_md5.update(b); compressed_bytes+=len(b)
    archive_identity_pass=(archive_sha.hexdigest()==ref['archive_sha256'] and
                           'md5:'+archive_md5.hexdigest()==ref['official_checksum'] and
                           compressed_bytes==ref['archive_bytes'])
    if not archive_identity_pass:
        save(args.output/'summary.json',{'status':'INPUT_ARCHIVE_IDENTITY_MISMATCH',
             'archive_sha256':archive_sha.hexdigest(),'archive_md5':archive_md5.hexdigest(),
             'archive_bytes':compressed_bytes,'reference_archive_sha256':ref['archive_sha256']})
        return 1
    print('Archive SHA-256/MD5/bytes PASS',flush=True)
    actual={}; tar_order=[]; unexpected=[]
    direct_plain=hashlib.sha256(); direct_lf=hashlib.sha256(); plain_bytes=0
    with tarfile.open(args.archive,'r|gz') as archive:
        for entry in archive:
            if entry.isdir(): continue
            if not entry.isfile():
                unexpected.append({'name':entry.name,'type':'non-regular'}); continue
            name=PurePosixPath(entry.name).as_posix()
            if not name.endswith('.log'):
                unexpected.append({'name':name,'type':'non-log-regular'}); continue
            if name in actual: raise ValueError('Duplicate archive member: '+name)
            tar_order.append(name)
            h=hashlib.sha256(); count=size=0; tail=b''
            source=archive.extractfile(entry)
            with source:
                for b in iter(lambda:source.read(CHUNK),b''):
                    h.update(b); size+=len(b); count+=b.count(b'\n'); tail=b[-1:]
                    direct_plain.update(b); direct_lf.update(b); plain_bytes+=len(b)
            direct_lf.update(b'\n')
            rec={'path':name,'bytes':size,'sha256':h.hexdigest(),
                 'newline_count':count,'ends_with_lf':tail==b'\n'}
            known=expected.get(name)
            rec['reference_pass']=known is not None and all(rec[k]==known[k] for k in ['bytes','sha256','newline_count','ends_with_lf'])
            actual[name]=rec
            if len(actual)%1000==0:print('Read archive members:',len(actual),flush=True)
    tar_order_lexical=tar_order==sorted(tar_order)
    member_set_pass=set(actual)==set(expected) and not unexpected
    member_hash_pass=member_set_pass and all(r['reference_pass'] for r in actual.values())
    # This pass independently hashes the entire historical file. Segment reading
    # is merely a hypothesis being checked, and never changes any source byte.
    hist_h=hashlib.sha256(); hist_bytes=0
    reconstructed_plain=hashlib.sha256(); reconstructed_lf=hashlib.sha256()
    candidate_plain_bytes=candidate_lf_bytes=0
    segment_rows=[]
    with args.historical.open('rb') as f:
        for i,expected_member in enumerate(members):
            size=expected_member['bytes']; remaining=size; h=hashlib.sha256(); observed=0
            while remaining:
                b=f.read(min(CHUNK,remaining))
                if not b:break
                h.update(b); hist_h.update(b); hist_bytes+=len(b)
                reconstructed_plain.update(b); reconstructed_lf.update(b)
                candidate_plain_bytes+=len(b); candidate_lf_bytes+=len(b)
                observed+=len(b); remaining-=len(b)
            sep=f.read(1)
            hist_h.update(sep); hist_bytes+=len(sep)
            reconstructed_lf.update(b'\n'); candidate_lf_bytes+=1
            rec=actual.get(expected_member['path'])
            payload_pass=(rec is not None and observed==rec['bytes'] and h.hexdigest()==rec['sha256'])
            segment_rows.append({'index':i,'path':expected_member['path'],
                                 'observed_payload_bytes':observed,'observed_payload_sha256':h.hexdigest(),
                                 'matches_actual_tar_member':payload_pass,'observed_separator_hex':sep.hex(),
                                 'separator_is_single_lf':sep==b'\n'})
        extra=0
        for b in iter(lambda:f.read(CHUNK),b''):
            hist_h.update(b); hist_bytes+=len(b); extra+=len(b)
    historical_identity_pass=hist_bytes==HIST_BYTES and hist_h.hexdigest()==HIST_SHA
    mapping_pass=member_hash_pass and extra==0 and all(r['matches_actual_tar_member'] and r['separator_is_single_lf'] for r in segment_rows)
    # Direct tar bytes define the candidates when tar order is lexical. Otherwise
    # member-wise verified historical segments bind the candidate hashes to the
    # exact same tar payload bytes, under SHA-256 content identity.
    candidate_source='direct lexical tar member stream' if tar_order_lexical else 'historical segments each verified against actual tar member bytes by SHA-256'
    plain_hash=direct_plain.hexdigest() if tar_order_lexical else reconstructed_plain.hexdigest()
    lf_hash=direct_lf.hexdigest() if tar_order_lexical else reconstructed_lf.hexdigest()
    candidates=[
        {'name':'lexical_exact_bytes_no_separator','bytes':plain_bytes if tar_order_lexical else candidate_plain_bytes,
         'sha256':plain_hash,'expected_bytes':ref['raw_bytes'],'expected_sha256':ref['sha256'],
         'expected_identity':'R71 Spark','candidate_source':candidate_source,
         'verified_tar_payload_binding':member_hash_pass and (tar_order_lexical or mapping_pass)},
        {'name':'lexical_member_bytes_then_single_LF_including_last','bytes':plain_bytes+len(tar_order) if tar_order_lexical else candidate_lf_bytes,
         'sha256':lf_hash,'expected_bytes':HIST_BYTES,'expected_sha256':HIST_SHA,
         'expected_identity':'historical source-study Spark','candidate_source':candidate_source,
         'verified_tar_payload_binding':member_hash_pass and (tar_order_lexical or mapping_pass)}]
    for row in candidates:
        row['match']=row['verified_tar_payload_binding'] and row['bytes']==row['expected_bytes'] and row['sha256']==row['expected_sha256']
    after={k:identity(p) for k,p in [('archive',args.archive),('historical',args.historical)]}
    unchanged=before==after
    passed=archive_identity_pass and member_hash_pass and historical_identity_pass and mapping_pass and unchanged and all(r['match'] for r in candidates)
    report={'status':'PASS' if passed else 'CANDIDATE_OR_IDENTITY_MISMATCH',
            'scope':'Local read-only full-content hashing and member-boundary validation, not a compression experiment.',
            'inputs':{'archive':{'bytes':compressed_bytes,'sha256':archive_sha.hexdigest(),'md5':archive_md5.hexdigest(),'reference_identity_pass':archive_identity_pass},
                      'historical':{'bytes':hist_bytes,'sha256':hist_h.hexdigest(),'expected_bytes':HIST_BYTES,'expected_sha256':HIST_SHA,'identity_pass':historical_identity_pass},
                      'reference_ledger_sha256':hashlib.sha256(args.reference.read_bytes()).hexdigest()},
            'member_count':len(actual),'reference_member_count':len(members),'tar_order_is_lexical':tar_order_lexical,
            'member_set_pass':member_set_pass,'every_tar_member_hash_matches_reference':member_hash_pass,
            'unexpected_archive_entries':unexpected,'historical_extra_bytes':extra,
            'historical_verified_member_and_single_lf_mapping':mapping_pass,
            'candidates':candidates,'source_input_stat_unchanged':unchanged,
            'source_stats_before':before,'source_stats_after':after,
            'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'elapsed_seconds':time.perf_counter()-started,'network_accesses':0,'api_calls':0,'codecs_run':0,
            'assembled_output_files_created':0,
            'conclusion':('The historical input is reproduced by lexical original .log member bytes plus exactly one LF after each member, including the last. Existing original member-ending LFs remain unchanged.' if passed else 'No recipe claim is established by this result.')}
    save(args.output/'actual_archive_members.json',{'members':[actual[k] for k in sorted(actual)],'tar_order':tar_order})
    save(args.output/'historical_member_boundary_checks.json',{'rows':segment_rows})
    save(args.output/'summary.json',report)
    print(json.dumps({k:report[k] for k in ['status','member_count','tar_order_is_lexical','historical_verified_member_and_single_lf_mapping','candidates','elapsed_seconds']}),flush=True)
    return 0 if passed else 1

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--archive',type=Path,required=True)
    parser.add_argument('--historical',type=Path,required=True)
    parser.add_argument('--reference',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    raise SystemExit(run(args))
