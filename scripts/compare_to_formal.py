#!/usr/bin/env python3
"""Compare a replay.py result directory with the recorded formal run of the same deployment.

usage: python3 scripts/compare_to_formal.py RESULT_DIR [--deployment-set main|semzip1]
Checks raw identity, total archive bytes and every archive member (bytes and SHA-256) against the
formal record (main: evidence/selection/results/formal/pool/<Dataset>.json). The SemZip-1 reference
(results/final/anonymous_results.json) records no per-member list; for it only raw identity and total
archive bytes are compared and members are reported as not recorded. Read-only."""
import argparse, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('result', type=Path)
    ap.add_argument('--deployment-set', default='main', choices=['main', 'semzip1'])
    a = ap.parse_args()
    run = json.loads((a.result/'result.json').read_text())
    d = run['dataset']
    if a.deployment_set == 'main':
        ref = json.loads((ROOT/'evidence/selection/results/formal/pool'/f'{d}.json').read_text())
        ref_raw, ref_bytes, ref_files = ref['encode']['raw_sha256'], ref['encode']['archive_bytes'], ref['archive_files']
    else:
        rows = json.loads((ROOT/'results/final/anonymous_results.json').read_text())['size_rows']
        r = next(x for x in rows if x['method'] == 'semzip' and x['dataset'] == d)
        ref_raw, ref_bytes, ref_files = r.get('raw_sha256'), r['archive_bytes'], r.get('archive_files', {})
    got = run['archive_files']
    members_recorded = bool(ref_files)
    diff = sorted(k for k in set(got) | set(ref_files)
                  if (got.get(k) or {}).get('sha256') != (ref_files.get(k) or {}).get('sha256')) if members_recorded else []
    out = {'dataset': d, 'deployment_set': a.deployment_set,
           'raw_sha256_equal': ref_raw is None or ref_raw == run['raw_sha256'],
           'archive_bytes': run['archive_bytes'], 'recorded_archive_bytes': ref_bytes,
           'archive_bytes_equal': run['archive_bytes'] == ref_bytes,
           'members_compared': len(ref_files) if members_recorded else 'not recorded',
           'members_differing': len(diff), 'first_differing': diff[:10],
           'decoded_sha_pass': run.get('materialized_output_sha_pass', False)}
    out['status'] = 'IDENTICAL' if out['archive_bytes_equal'] and not diff and out['raw_sha256_equal'] else 'DIFFERENT'
    print(json.dumps(out, indent=1))
    sys.exit(0 if out['status'] == 'IDENTICAL' else 1)


if __name__ == '__main__':
    main()
