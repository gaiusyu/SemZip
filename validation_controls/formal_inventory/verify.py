"""Check saved metadata only; no benchmark or remote execution."""
from pathlib import Path
from collections import Counter
import hashlib
import json

ROOT = Path(__file__).resolve().parent
def load(name):
    return json.loads((ROOT / name).read_text())
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

manifest = load('FILES.json')
assert set(manifest) == {str(p.relative_to(ROOT)) for p in ROOT.rglob('*')
                         if p.is_file() and p.name != 'FILES.json'}
assert all(sha(ROOT / name) == value for name, value in manifest.items())
provenance = load('PROVENANCE.json')
assert all(sha(ROOT / name) == value for name, value in provenance['exact_original_source_sha256'].items())
assert sha(ROOT / 'formal_campaign_v3.py') == provenance['final_controller_sha256']
trials = load('superseded_trials.json')
rows = trials['rows']
assert len(rows) == 216 and [r['index'] for r in rows] == list(range(216))
assert dict(Counter(r['outcome'] for r in rows)) == trials['outcome_counts'] == {
    'PASS_SUPERSEDED_CAMPAIGN': 29, 'POST_DECODE_METADATA_AUDIT_FAILURE': 2,
    'INTERRUPTED_DURING_CAMPAIGN_STOP': 1, 'NOT_STARTED_AFTER_CAMPAIGN_STOP': 184}
assert len({(r['dataset'], r['method'], r['repeat']) for r in rows}) == 216
controls = load('controls.json')
old = controls['old_inventory_fresh_files']
assert len(old) == 32
failed = [r for r in old if r['status'] == 'FAILED']
assert len(failed) == 1 and set(failed[0]['metadata_changes']) == {'mtime_ns'}
before, after = failed[0]['metadata_changes']['mtime_ns']
assert after - before == -5000
retained = controls['retained_output_checks']
assert len(retained) == 2 and all(r['full_and_block_identity_match'] for r in retained)
fresh = controls['corrected_inventory_fresh_files']
assert len(fresh) == 32 and all(r['full_hash_matches'] and r['expected_sha256'] == r['observed_sha256'] for r in fresh)
corrupt = controls['corruption_controls']
assert {r['case'] for r in corrupt} == {'substitution', 'truncation', 'append'}
assert all(r['comparison_rejected'] for r in corrupt)
branches = controls['synthetic_metadata_branches']
assert {r['field'] for r in branches} == {'mtime_ns', 'inode', 'bytes'}
assert all(r['actual_acceptance'] == r['expected_acceptance'] == (r['field'] == 'mtime_ns') for r in branches)
print(json.dumps({'status': 'PASS', 'scope': 'Saved metadata and source hashes only',
                  'superseded_rows': len(rows), 'outcomes': trials['outcome_counts']}))
