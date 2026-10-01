"""Verify the exported historical evidence without model/benchmark execution."""
from pathlib import Path
import hashlib,json,math

ROOT=Path(__file__).resolve().parent

def read(name):return json.loads((ROOT/name).read_text())

def main():
    inventory=read('FILES.json')
    actual={str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file() and p.name!='FILES.json' and '__pycache__' not in p.parts}
    assert actual==set(inventory['files'])
    for name,identity in inventory['files'].items():
        content=(ROOT/name).read_bytes()
        assert len(content)==identity['bytes'] and hashlib.sha256(content).hexdigest()==identity['sha256']
    backbone=read('backbone_trials.json');explicit=backbone['cohorts']['explicit_output_cap'];default=backbone['cohorts']['gateway_default']
    assert len(explicit)==36 and len(default)==18
    assert sum(r['status']=='PASS' for r in explicit)==35 and sum(r['status']=='PASS' for r in default)==17
    assert sum(r['status']=='PASS' and r['active_semantic_blocks']==0 for r in explicit)==4
    for cohort in (explicit,default):
        assert len({(r['dataset'],r['model'],r['repeat']) for r in cohort})==len(cohort)
    for row in explicit:
        if row['status']=='PASS':assert math.isclose(row['ratio'],row['raw_bytes']/row['archive_bytes'],rel_tol=1e-12)
        else:assert 'ratio' not in row
    for row in default:
        if row['status']=='PASS':assert math.isclose(row['heldout_ratio'],row['heldout_raw_bytes']/row['heldout_archive_bytes'],rel_tol=1e-12)
        else:assert 'heldout_ratio' not in row
    append=read('append_only_evolution.json')
    assert len(append['datasets'])==9 and len(append['blocks'])==75
    assert sum(len(r['updates']) for r in append['datasets'])==8 and sum(r['update_calls'] for r in append['datasets'])==46
    for block in append['blocks']:
        assert block['frozen']['raw_sha256']==block['evolving']['raw_sha256']
        assert block['frozen']['archive_bytes']==block['evolving']['archive_bytes']
        assert block['frozen']['sha_pass'] and block['evolving']['sha_pass']
    gated=read('gated_evolution.json')
    assert len(gated['datasets'])==7 and len(gated['blocks'])==62
    assert sum(r['triggered'] for r in gated['datasets'])==6 and sum(r['outcome']=='PUBLISHED' for r in gated['datasets'])==1
    assert sum(r['calls'] for r in gated['datasets'])==45
    for row in gated['datasets']:
        a,b=row['future']['frozen'],row['future']['deployed']
        assert math.isclose(row['future_archive_saving_percent'],100*(1-b['archive_bytes']/a['archive_bytes']),abs_tol=1e-10)
        if row['dataset']=='Windows':assert not row['unpublished_candidate_future_measured'] and row['outcome']=='REJECTED_VALIDATION_SIZE'
    repair=read('repair_response_controls.json')['rows'];assert len(repair)==4
    expected={('OpenSSH','replay'):6,('OpenSSH','no_llm_repair'):5,('Android','replay'):17,('Android','no_llm_repair'):4}
    for row in repair:
        assert row['same_parsed_plan'] and row['actual_api_calls']==0
        assert row['model_response_content_replays']==expected[row['dataset'],row['condition']]
    print(json.dumps({'status':'PASS','files':len(actual),'backbone_trials':54,'append_only_pairs':75,'gated_blocks':62,'new_api_calls':0,'new_compression_runs':0}))

if __name__=='__main__':main()
