#!/usr/bin/env python3
"""Export preserved source-study records; never scan sources or run compressors."""
import argparse
import csv
import hashlib
import io
import json
from pathlib import Path

Q1 = 'results/logbench_source_census_final_holdout_20260713'
Q2 = 'results/autodl_full_source_grounded_official_20260712'
Q1_FILES = ['source_summary.json', 'audited_source_summary.json',
            'source_by_repository.csv', 'source_candidates.csv', 'audit_sample.csv',
            'audit_decisions.json', 'audit_reviewed.csv', 'audit_summary.json',
            'audit_by_stratum.csv']
CODE_FILES = ['analyze_logbench_source_census.py', 'finalize_source_audit.py',
              'test_source_census_rules.py', 'requirements_source_census.txt',
              'source_grounded_representation_study.py']

def sha(data):
    return hashlib.sha256(data).hexdigest()

def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')

def export(study, out):
    out.mkdir(parents=True, exist_ok=True)
    records = []
    def save(src, dst, transform=None, scope='byte-identical public-source evidence'):
        raw = (study / src).read_bytes()
        data = transform(raw) if transform else raw
        target = out / dst
        if target.exists() and target.read_bytes() != data:
            raise ValueError('refusing to overwrite different exported evidence: ' + dst)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        records.append({'source_id': f'source-{len(records)+1:03d}',
                        'role': dst, 'original_basename': Path(src).name,
                        'original_bytes': len(raw), 'original_sha256': sha(raw),
                        'export_path': dst, 'export_bytes': len(data),
                        'export_sha256': sha(data), 'byte_identical': raw == data,
                        'scope': scope})
    def json_bytes(obj):
        return (json.dumps(obj, indent=2, ensure_ascii=False) + '\n').encode()
    for name in Q1_FILES:
        save(Q1 + '/' + name, 'q1/' + name)
    for name in CODE_FILES:
        save(name, 'code/' + name)
    save('family_catalog.json', 'q2/family_catalog.json')
    save(Q2 + '/families.csv', 'q2/families.csv')
    def clean_row(row):
        result = dict(row)
        result['input'] = 'inputs/Q2/' + row['dataset'] + '.log'
        result['execution_label'] = 'historical_source_grounded_full_20260712'
        return result
    save(Q2 + '/summary.json', 'q2/summary.json',
         lambda raw: json_bytes([clean_row(r) for r in json.loads(raw)]),
         'Only input location and private execution label replaced; all numeric/null/status/hash fields retained.')
    def clean_csv(raw):
        rows = list(csv.DictReader(io.StringIO(raw.decode())))
        output = io.StringIO(newline='')
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(clean_row(r) for r in rows)
        return output.getvalue().encode()
    save(Q2 + '/summary.csv', 'q2/summary.csv', clean_csv,
         'Only input location and private execution label replaced; numerical text retained.')
    def clean_hashes(raw):
        rows = []
        for line in raw.decode().splitlines():
            digest, name = line.split(maxsplit=1)
            bits = Path(name).parts
            relative = '/'.join(bits[-2:]) if bits[-1].endswith('.xz') else bits[-1]
            rows.append({'artifact': relative, 'sha256': digest,
                         'scope': 'recorded historical original bytes, not newly rehashed archive'})
        return json_bytes({'records': rows, 'archive_payloads_included': False})
    save(Q2 + '/archive_sha256.txt', 'q2/archive_hashes.json', clean_hashes,
         'Recorded hashes preserved; original execution directory removed. Summary hashes refer to original unsanitized records.')
    summary = json.loads((out / 'q2/summary.json').read_text())
    manifest = {
        'cohort': 'historical_source_grounded_full_20260712',
        'date': '2026-07-12', 'recorded_status': 'completed; historical gates passed',
        'environment': {'os': 'Ubuntu 22.04.1 LTS', 'python': '3.10.8',
                        'cpu': 'Intel Xeon (server CPU)', 'reported_execution_cpus': 32,
                        'gzip_level': 9, 'python_lzma_preset': 6},
        'code_sha256': {'source_grounded_representation_study.py': sha((study / CODE_FILES[-1]).read_bytes()),
                        'family_catalog.json': sha((study / 'family_catalog.json').read_bytes())},
        'inputs': [{k:r[k] for k in ('dataset','raw_bytes','raw_sha256')} for r in summary],
        'scope': 'Whitelisted historical run-manifest fields; no fresh execution or archive audit during export.',
        'omitted': ['private execution label', 'remote working directory', 'machine-local command input paths'],
        'block_contract': 'Historical whole-file paired-field control; not the R71 100000-record deployment. Numeric previous state spans each family stream.',
        'metric_scope': 'Uncompressed length-framed rendered fields divided by latent streams; residual text, catalog, and archive headers are excluded from this stream metric. Complete gzip/xz sizes remain separate diagnostics.'}
    save('AUTODL_RUN_MANIFEST_20260712.md', 'q2/run_manifest.json', lambda raw: json_bytes(manifest),
         'Whitelist export of historical environment, immutable inputs and frozen source identities; private locations omitted.')
    provenance = {
        'status': 'HUMAN_REVIEW_NOT_ESTABLISHED',
        'evidence': '499 explicit row decisions bind to the frozen sample hash and detector hash; finalizer requires complete decisions.',
        'recorded_protocol': json.loads((out/'q1/audit_decisions.json').read_text())['review_protocol'],
        'decision_field_names': ['call_id', 'manual_labels', 'reviewer_notes'],
        'limitation': 'The preserved records have no reviewer identity, signed attribution, or independent evidence distinguishing human from automated annotation. Field names and protocol prose do not establish human review.',
        'interpretation': 'Reported precision is agreement with the preserved row labels, conditional on their correctness. Finite agreement cannot establish zero false positives over the census or a mathematical lower bound on true prevalence.',
        'not_claimed': ['independent human annotation', 'complete detector recall', 'population prevalence across all software', 'compression benefit of each detected operation']}
    write_json(out/'REVIEW_PROVENANCE.json', provenance)
    write_json(out/'SOURCE_RECORDS.json', {
        'schema': 'source-study-original-to-export-map-v1',
        'scope': 'Original basenames, opaque source IDs and SHA identities map to anonymous relative exports. No private source-root path is included.',
        'public_identifiers_retained': ['open-source repository names', 'public source paths and lines', 'source snippets', 'public GitHub URLs and commits', 'LogBench archive-member paths'],
        'sources': records})
    return len(records)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--study-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parent)
    args = parser.parse_args()
    print(json.dumps({'exported_source_records': export(args.study_root, args.output),
                      'experiments_run': 0, 'api_calls': 0}))
