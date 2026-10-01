#!/usr/bin/env python3
"""Run the fixed saved-response control using only this artifact's inputs.

This invokes the original trainer through the existing response-content hook;
it does not implement another trainer or fit storage policies. Outputs are new
and must lie outside the artifact. Expected historical differences are retained.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

DATASETS = ['Linux', 'Proxifier', 'Apache', 'Zookeeper', 'Mac', 'HealthApp',
            'HPC', 'Hadoop', 'OpenStack', 'OpenSSH', 'Android', 'BGL', 'HDFS',
            'Spark', 'Windows', 'Thunderbird']
COMPARE = ['status', 'sample_sha256', 'expected_plan_sha256',
           'recorded_responses', 'consumed_responses', 'unmatched_prompt_sha256',
           'calls', 'plan_export_reached', 'replayed_plan_sha256',
           'same_parsed_plan', 'same_plan_file_bytes', 'canonical_plan_sha256',
           'original_canonical_plan_sha256', 'api_calls',
           'new_complete_archives', 'new_storage_fits']


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def inventory(root):
    return {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob('*'))
            if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc'}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--artifact-root', type=Path,
                   default=Path(__file__).resolve().parents[3])
    p.add_argument('--output', required=True, type=Path)
    a = p.parse_args()
    root, out = a.artifact_root.resolve(), a.output.resolve()
    if out == root or root in out.parents:
        p.error('--output must be outside the artifact')
    if out.exists():
        p.error('--output must not already exist')
    deployments = json.loads((root / 'metadata/deployments.json').read_text())
    if [r['dataset'] for r in deployments['datasets']] != DATASETS:
        raise ValueError('Expected the fixed sixteen-dataset deployment inventory')
    references = root / 'validation_controls/fresh_response_boundary/results.json'
    prior = json.loads(references.read_text())
    by_key = {(r['condition'], r['dataset']): r for r in prior['rows']}
    schedule = [('uniform_utc0', d, 'UTC0') for d in DATASETS]
    schedule += [('explanatory_shanghai', d, 'Asia/Shanghai') for d in ['Linux', 'Mac']]
    if set(by_key) != {(c, d) for c, d, _ in schedule} or len(prior['rows']) != 18:
        raise ValueError('The prior evidence must retain all eighteen fixed outcomes')
    for r in deployments['datasets']:
        if sha(root / r['program']) != r['program_sha256']:
            raise ValueError('Deployment program identity changed: ' + r['dataset'])
    before = {name: inventory(root / name)
              for name in ['source', 'deployments', 'offline_evidence']}
    out.mkdir(parents=True)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(('PARE_LLM_', 'YUNWU_', 'SEMZIP_R53_'))
           and k != 'SEMZIP_R54_PLAN'}
    env.update(PYTHONHASHSEED='0', PYTHONDONTWRITEBYTECODE='1', TZ='UTC0', LC_ALL='C')
    rows = []
    tools = Path(__file__).resolve().parent
    for condition, dataset, timezone in schedule:
        target = out / condition / dataset
        target.parent.mkdir(exist_ok=True)
        hook = tools / ('replay_utc0.py' if timezone == 'UTC0'
                        else 'replay_explicit_timezone.py')
        argv = [sys.executable, str(hook), '--source', str(root / 'source'),
                '--evidence', str(root / 'offline_evidence'), '--dataset', dataset,
                '--expected-plan', str(root / 'deployments' / dataset / 'program.json'),
                '--output', str(target)]
        if timezone != 'UTC0':
            argv += ['--timezone', timezone]
        timed_out = False
        with (target.parent / (dataset + '.log')).open('wb') as log:
            try:
                proc = subprocess.run(argv, env=env, stdout=log, stderr=subprocess.STDOUT,
                                      timeout=120, check=False)
                returncode = proc.returncode
            except subprocess.TimeoutExpired:
                returncode, timed_out = None, True
        result_path = target / 'result.json'
        actual = json.loads(result_path.read_text()) if result_path.exists() else {}
        row = {k: actual[k] for k in COMPARE if k in actual}
        row.update(condition=condition, dataset=dataset, explicit_timezone=timezone,
                   subprocess_returncode=returncode, timed_out=timed_out,
                   status=actual.get('status', 'FAILED_NO_RESULT'),
                   source_unchanged=actual.get('source_unchanged', False),
                   evidence_unchanged=actual.get('evidence_unchanged', False))
        row['differing_fields_from_prior_control'] = [
            k for k in COMPARE if actual.get(k) != by_key[(condition, dataset)].get(k)]
        row['agrees_with_prior_control'] = bool(
            returncode == 0 and not timed_out and actual.get('source_unchanged')
            and actual.get('evidence_unchanged')
            and not row['differing_fields_from_prior_control'])
        if result_path.exists():
            row['result_sha256'] = sha(result_path)
        rows.append(row)
        print(json.dumps({k: row[k] for k in ['condition', 'dataset', 'status',
                                            'agrees_with_prior_control']}), flush=True)
    unchanged = all(inventory(root / name) == value for name, value in before.items())
    result = {
        'schema_version': 1,
        'status': 'PASS_PRIOR_OUTCOMES_REPRODUCED' if unchanged and all(
            r['agrees_with_prior_control'] for r in rows) else 'DIFFERENT_OR_FAILED',
        'scope': 'Packaged source, saved samples and exchanges; call_llm response-content boundary only',
        'local_os': platform.system(), 'python_version': platform.python_version(),
        'prior_evidence_sha256': sha(references),
        'source_inventory_sha256': before['source'],
        'scripts_sha256': {p.name: sha(p) for p in sorted(tools.glob('*.py'))},
        'protected_inputs_unchanged': unchanged,
        'uniform_utc0_exact_plan_matches': sum(r['status'] == 'PASS_IDENTICAL' for r in rows[:16]),
        'uniform_utc0_differences': sum(r['status'] == 'DIFFERENT' for r in rows[:16]),
        'explanatory_shanghai_exact_plan_matches': sum(r['status'] == 'PASS_IDENTICAL' for r in rows[16:]),
        'all_18_outcomes_retained': len(rows) == 18,
        'original_training_timezone_explicitly_pinned': False,
        'rows': rows,
    }
    (out / 'summary.json').write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
    print(result['status'], flush=True)
    return 0 if result['status'] == 'PASS_PRIOR_OUTCOMES_REPRODUCED' else 1


if __name__ == '__main__':
    sys.exit(main())
