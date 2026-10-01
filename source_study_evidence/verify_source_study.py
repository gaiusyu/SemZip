#!/usr/bin/env python3
"""Check saved metadata, source identities and arithmetic. No API/source scan/codec."""
import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from pathlib import Path


def require(value, label):
    if not value:
        raise ValueError(label)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def close(a, b):
    return math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-12)


def wilson(successes, trials):
    z = 1.959963984540054
    p = successes/trials
    den = 1+z*z/trials
    center = (p+z*z/(2*trials))/den
    margin = z*math.sqrt(p*(1-p)/trials+z*z/(4*trials*trials))/den
    return center-margin, center+margin


def verify(root, check_manifest=True):
    def js(p): return json.loads((root/p).read_text())
    def rows(p):
        with (root/p).open(newline='', encoding='utf-8') as f:
            return list(csv.DictReader(f))
    if check_manifest:
        manifest = js('FILES.json')['files']
        actual = {str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p.name != 'FILES.json'}
        require(actual == set(manifest), 'file set differs from frozen manifest')
        for name, rec in manifest.items():
            require((root/name).stat().st_size == rec['bytes'] and digest(root/name) == rec['sha256'], 'file identity: '+name)
    provenance = js('SOURCE_RECORDS.json')['sources']
    require(len({r['source_id'] for r in provenance}) == len(provenance), 'duplicate source id')
    by_export = {r['export_path']: r for r in provenance}
    require(len(by_export) == len(provenance), 'duplicate exported source path')
    for rec in provenance:
        require(digest(root/rec['export_path']) == rec['export_sha256'], 'source mapping exported hash')
        require((root/rec['export_path']).stat().st_size == rec['export_bytes'], 'source mapping exported bytes')
        if rec['byte_identical']:
            require(rec['original_sha256'] == rec['export_sha256'] and rec['original_bytes'] == rec['export_bytes'], 'unchanged source mapping')
    summary = js('q1/source_summary.json')
    audited = js('q1/audited_source_summary.json')
    repos = rows('q1/source_by_repository.csv')
    candidates = rows('q1/source_candidates.csv')
    require(len(repos) == len({r['repository'] for r in repos}) == 2781, 'repository identities')
    require(sum(int(r['logging_calls']) for r in repos) == summary['logging_calls'] == 285134, 'logging call count')
    require(sum(int(r['java_files']) for r in repos) == summary['selected_production_like_java_files'], 'production-like source-file count')
    require(sum(int(r['parse_error_files']) for r in repos) == summary['files_with_parse_errors'] == 56, 'parse error count')
    require(summary['selected_production_like_java_files'] - summary['files_with_parse_errors'] == summary['parse_clean_java_files'] == 113427, 'parse-clean count')
    require(summary['java_files'] - summary['excluded_likely_test_files'] == summary['selected_production_like_java_files'], 'test-filter accounting')
    require(len(candidates) == len({r['call_id'] for r in candidates}) == summary['broad_candidates'] == 24108, 'candidate union count')
    def labels(row): return set(filter(None, row['categories'].split(';')))
    def validate_call(row):
        text = '\0'.join([row['repository'], row['archive_path'], row['line'], row['call_text']])
        require(hashlib.sha256(text.encode()).hexdigest()[:16] == row['call_id'], 'call identity')
        require(row['archive_path'].startswith('LogBench-O/repos/'), 'non-public archive member')
        require(row['strict_candidate'] in ('True', 'False'), 'strict flag')
        require((row['strict_candidate'] == 'True') == bool(labels(row) - {'explicit_object_rendering'}), 'strict union rubric')
    by_repo = defaultdict(Counter)
    for row in candidates:
        validate_call(row)
        by_repo[row['repository']]['broad_candidates'] += 1
        by_repo[row['repository']]['strict_candidates'] += int(row['strict_candidate'] == 'True')
        by_repo[row['repository']].update(labels(row))
    for row in repos:
        for key in ['broad_candidates', 'strict_candidates', *summary['category_counts']]:
            require(int(row[key]) == by_repo[row['repository']][key], 'per-repository candidate/category counts')
    require(sum(int(r['strict_candidates']) for r in repos) == summary['strict_candidates'] == 15659, 'strict call count')
    strict_repos = sum(int(r['strict_candidates']) > 0 for r in repos)
    require(strict_repos == 1520, 'strict repository count')
    for key, count in summary['category_counts'].items():
        require(sum(int(r[key]) for r in repos) == count, 'category census count: '+key)
    for prefix in ['strict', 'broad']:
        require(close(summary[prefix+'_fraction'], summary[prefix+'_candidates']/summary['logging_calls']), 'candidate fraction')
    for key in summary:
        if key != 'status': require(summary[key] == audited[key], 'audited census changed: '+key)
    sample = rows('q1/audit_sample.csv')
    reviewed = rows('q1/audit_reviewed.csv')
    decision = js('q1/audit_decisions.json')
    audit = js('q1/audit_summary.json')
    require(len(sample) == len({r['call_id'] for r in sample}) == 499, 'sample cardinality')
    require(digest(root/'q1/audit_sample.csv') == decision['sample_sha256'] == audit['holdout_sha256'], 'audit sample hash binding')
    require(digest(root/'code/analyze_logbench_source_census.py') == decision['detector_sha256'], 'audit detector hash binding')
    decisions = {r['call_id']: r for r in decision['decisions']}
    reviewed_map = {r['call_id']: r for r in reviewed}
    require(len(decisions) == len(decision['decisions']) == len(reviewed_map) == len(reviewed) == len(sample), 'decision/review cardinality')
    require(set(decisions) == set(reviewed_map) == {r['call_id'] for r in sample}, 'decision/review coverage')
    candidate_map = {r['call_id']: r for r in candidates}
    false_pos, false_neg = [], []
    for row in sample:
        validate_call(row)
        rec = decisions[row['call_id']]
        observed = reviewed_map[row['call_id']]
        require(len(rec['manual_labels']) == len(set(rec['manual_labels'])) and set(rec['manual_labels']) <= set(summary['category_counts']), 'recorded review labels')
        require(rec['reviewer_notes'].strip(), 'empty review note')
        for key in candidate_map[candidates[0]['call_id']]:
            require(row[key] == observed[key], 'review modifies source evidence')
            if labels(row): require(row[key] == candidate_map[row['call_id']][key], 'sample differs from candidate ledger')
        require(observed['manual_labels'].split(';') == rec['manual_labels'] if rec['manual_labels'] else observed['manual_labels'] == '', 'reviewed labels vs decisions')
        require(observed['reviewer_notes'] == rec['reviewer_notes'], 'reviewed notes vs decisions')
        require(observed['is_true_candidate'] == ('true' if rec['manual_labels'] else 'false'), 'reviewed candidate flag')
        auto, manual = labels(row), set(rec['manual_labels'])
        false_pos += [{'call_id':row['call_id'],'label':x} for x in sorted(auto-manual)]
        false_neg += [{'call_id':row['call_id'],'label':x} for x in sorted(manual-auto)]
    require(false_pos == audit['false_positive_labels'] and false_neg == audit['false_negative_labels'], 'review disagreement lists')
    require(audited['audit_summary'] == 'audit_summary.json', 'audited summary reference')
    require(sum(bool(labels(r)) for r in sample) == audit['candidate_rows'] == 449, 'candidate sample count')
    require(sum(bool(labels(r)) and bool(decisions[r['call_id']]['manual_labels']) for r in sample) == audit['candidate_rows_with_confirmed_labels'], 'confirmed candidate sample count')
    by_stratum = {r['stratum']: r for r in rows('q1/audit_by_stratum.csv')}
    for rec in audit['per_stratum']:
        selected = [r for r in sample if rec['stratum'] in labels(r)]
        confirmed = sum(rec['stratum'] in decisions[r['call_id']]['manual_labels'] for r in selected)
        require(len(selected) == rec['sampled'] and confirmed == rec['confirmed'] and len(selected) >= 50, 'audit stratum counts')
        require(close(rec['observed_precision'], confirmed/len(selected)), 'audit observed agreement')
        lo, hi = wilson(confirmed, len(selected))
        require(close(lo, rec['wilson_95_low']) and close(hi, rec['wilson_95_high']), 'audit Wilson arithmetic')
        for key in rec:
            require(rec[key] == by_stratum[rec['stratum']][key] if key == 'stratum' else close(rec[key], by_stratum[rec['stratum']][key]), 'audit stratum CSV consistency')
    negatives = [r for r in sample if not labels(r)]
    missed = sum(bool(decisions[r['call_id']]['manual_labels']) for r in negatives)
    require(len(negatives) == audit['negative_rows'] == audit['negative_probe']['sampled'] == 50, 'negative probe count')
    require(missed == audit['negative_probe']['missed_under_frozen_rubric'] == 1, 'negative probe misses')
    require(close(audit['negative_probe']['observed_miss_rate'], missed/len(negatives)), 'negative probe rate')
    lo, hi = wilson(missed,len(negatives))
    require(close(lo,audit['negative_probe']['wilson_95_low']) and close(hi,audit['negative_probe']['wilson_95_high']), 'negative Wilson arithmetic')
    require(js('REVIEW_PROVENANCE.json')['status'] == 'HUMAN_REVIEW_NOT_ESTABLISHED', 'review attribution boundary')
    paired = js('q2/summary.json')
    paired_csv = {r['dataset']:r for r in rows('q2/summary.csv')}
    catalog = js('q2/family_catalog.json')
    families = rows('q2/families.csv')
    require(len(paired) == len(paired_csv) == 4 and {r['dataset'] for r in paired} == {'Spark','Zookeeper','OpenStack','OpenSSH'}, 'paired cohort')
    require(len(catalog) == len(families) == 23, 'family count')
    cat = {r['id']:r for r in catalog}
    require(len(cat) == 23 and {r['family_id'] for r in families} == set(cat), 'family identity coverage')
    for row in families:
        origin = cat[row['family_id']]
        for key in ['dataset','codec','representation_class','recovery_semantics','source_file','source_lines','revision']:
            require(row[key] == origin[key], 'catalog/family agreement: '+key)
        require(origin['repository'].startswith('https://github.com/') and len(origin['revision']) == 40, 'public source commit')
        require(close(row['surface_to_latent'], int(row['surface_bytes'])/int(row['latent_stream_bytes'])), 'family contraction')
    for row in paired:
        for key, value in row.items():
            actual = paired_csv[row['dataset']][key]
            require(str(value) == actual if isinstance(value, (str,bool)) else close(value,actual), 'paired JSON/CSV agreement: '+key)
        subset = [r for r in families if r['dataset'] == row['dataset']]
        require(len(subset) == row['source_grounded_families'], 'per-system family count')
        for small,big in [('occurrences','matched_occurrences'),('surface_bytes','surface_bytes'),('raw_span_stream_bytes','raw_span_stream_bytes'),('latent_stream_bytes','latent_stream_bytes')]:
            require(sum(int(r[small]) for r in subset) == row[big], 'family sum: '+big)
        for result,num,den in [('surface_fraction','surface_bytes','raw_bytes'),('surface_to_latent','surface_bytes','latent_stream_bytes'),('raw_span_to_latent_stream','raw_span_stream_bytes','latent_stream_bytes')]:
            require(close(row[result], row[num]/row[den]), 'paired metric: '+result)
        for codec in ['gzip','xz']:
            for label,num,den in [('separation','raw','raw_span'),('representation','raw_span','latent'),('end_to_end','raw','latent')]:
                require(close(row[codec+'_'+label+'_gain'],row[num+'_'+codec+'_bytes']/row[den+'_'+codec+'_bytes']), 'archive diagnostic gain')
        for mode in ['raw_span','latent']:
            require(row[mode+'_sha_pass'] is True and row[mode+'_decoded_bytes'] == row['raw_bytes'] and row[mode+'_sha256'] == row['raw_sha256'], 'historical decode ledger consistency')
    hashes = js('q2/archive_hashes.json')['records']
    hash_map = {r['artifact']:r['sha256'] for r in hashes}
    require(len(hash_map) == len(hashes) == 15, 'recorded archive/hash ledger')
    for name in ['summary.json','summary.csv','families.csv']:
        require(hash_map[name] == by_export['q2/'+name]['original_sha256'], 'original metadata hash ledger')
    require({name for name in hash_map if name.endswith('.xz')} == {r['dataset']+'/'+name for r in paired for name in ['raw.log.xz','raw_span.tar.xz','latent.tar.xz']}, '12 archive hash identities')
    run = js('q2/run_manifest.json')
    require(run['inputs'] == [{k:r[k] for k in ('dataset','raw_bytes','raw_sha256')} for r in paired], 'run input identities')
    for filename, value in run['code_sha256'].items():
        require(value == digest(root/('q2/' if filename.endswith('.json') else 'code/')/filename), 'run frozen source hash')
    return {'status':'PASS', 'scope':'Saved metadata/file identities and arithmetic only; no raw-corpus scan, API call, compression, or archive re-decoding.',
            'source_records':len(provenance),'repositories':len(repos),'logging_calls':summary['logging_calls'],
            'candidate_rows':len(candidates),'strict_calls':summary['strict_candidates'],'strict_repositories':strict_repos,
            'recorded_audit_rows':len(sample),'recorded_false_positive_labels':len(false_pos),'recorded_missed_labels':len(false_neg),
            'human_review':'NOT_ESTABLISHED','paired_datasets':len(paired),'source_grounded_families':len(families),
            'recorded_archive_hashes':12,'experiments_run':0,'api_calls':0}

if __name__ == '__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parent)
    parser.add_argument('--before-freeze',action='store_true',help='Check records before FILES.json is created.')
    parser.add_argument('--output',type=Path,help='Optional validation-report path; normally omit to keep package frozen.')
    args=parser.parse_args()
    report=verify(args.root,not args.before_freeze)
    if args.output: args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))
