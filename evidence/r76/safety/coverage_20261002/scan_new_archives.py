#!/usr/bin/env python3
"""R76 track E coverage extension (2026-10-02): scan the code actually embedded in the formal archives of the new
publications (R76-C variance 8, R76-G unseen 4, R76-B2 library 16) with the UNCHANGED safety/v2/scan_archives.py
(tar index + metadata.json read in memory, nothing extracted, nothing compiled or executed; both policies).
scan_archives.FORMAL is pointed at each run's formal root in turn; its main() is otherwise called unchanged.
Output: archive_scan/<label>.json per archive set + archive_scan_summary.json.
"""
import glob
import json
import sys
import time
from pathlib import Path

V2 = Path('<WORKDIR>/r76_additional_20260929/safety/v2')
sys.path.insert(0, str(V2))
import pyexec_validator as v  # noqa: E402
import static_check as sc  # noqa: E402
import scan_archives as sa  # noqa: E402

EXPECTED = {'pyexec_validator.py': '676fe26bd8c017fd74aa51b83221c81d341189740e85194ba283df2312447060',
            'static_check.py': 'a1d9fdeaa2b58d946179650eae9f36c27c1e2e0d6ee6ab56fa827edd0fd84fc3',
            'scan_archives.py': '3de6ac2d1942e744cafad01058b7eecdd88fe9cc4ac3685f41d3c2f68dd07832'}  # SHA-256 of ../v2/*.py as published in this copy (as-run values not listed)
for mod in (v, sc, sa):
    got = sc.sha_file(mod.__file__)
    if Path(mod.__file__).resolve().parent != V2.resolve() or not got.startswith(EXPECTED[Path(mod.__file__).name]):
        raise SystemExit('unexpected module file %s %s' % (mod.__file__, got))

R76 = Path('<WORKDIR>/r76_additional_20260929')
HERE = Path(__file__).resolve().parent
OUT = HERE / 'archive_scan'


def targets():
    out = []
    for track, base, reps in (('R76C_variance', R76 / 'variance/runs', 'r[23]'), ('R76G_unseen', R76 / 'unseen/runs', 'r1')):
        for rep in sorted(glob.glob(str(base / '*' / reps))):
            rep = Path(rep)
            d = rep.parent.name
            out.append((track, '%s_%s_%s' % (track, d, rep.name), rep / 'runs/formal/pool', d, rep / 'runs/publish/pool' / d / 'program.json'))
    lib = R76 / 'library'
    for d in sc.DATASETS:
        out.append(('R76B2_library(hand-written)', 'R76B2_library_%s' % d, lib / 'runs/formal/lib', d, lib / 'runs/publish/lib' / d / 'program.json'))
    # small archive sets first, Thunderbird last
    return sorted(out, key=lambda t: len(json.loads((t[2] / t[3] / 'archive/semantic_manifest.json').read_text())['semantic_archives']))


def main():
    t0 = time.time()
    OUT.mkdir(exist_ok=True)
    plan_check = json.loads((HERE / 'coverage_check_result.json').read_text())
    plan_keys = {x['code_sha256'] + ':' + x['kind'] for x in plan_check['verdicts']}
    rows = {}
    for track, label, formal_root, d, program in targets():
        out = OUT / (label + '.json')
        if not out.exists():
            sa.FORMAL = formal_root
            sa.main(str(out), [d])
        res = json.loads(out.read_text())
        ds = res['datasets'][d]
        pub_keys = {v.code_sha256(c) + ':' + kind for _, kind, c, _ in sc.extract_codes(json.loads(program.read_text()))}
        arch_keys = set(ds['unique_codes'])
        rows[label] = {'track': track, 'dataset': d, 'blocks': ds['blocks'], 'blocks_with_code': ds['blocks_with_code'],
                       'unique_codes': len(arch_keys), 'strict_rejected_unique': res['unique_codes_rejected_strict'],
                       'template_rejected_unique': res['unique_codes_rejected_template'],
                       'blocks_rejected_strict': ds['n_blocks_with_rejected_strict'],
                       'blocks_rejected_template': ds['n_blocks_with_rejected_template'],
                       'tar_member_problems': len(ds['tar_member_problems']), 'stream_kinds': ds['stream_kinds'],
                       'archive_codes_not_in_publication': sorted(arch_keys - pub_keys),
                       'archive_codes_not_in_plan_check': sorted(arch_keys - plan_keys),
                       'publication_codes_not_in_archive': sorted(pub_keys - arch_keys),
                       'scan_seconds': res['seconds']}
        print(label, json.dumps({k: rows[label][k] for k in ('blocks', 'unique_codes', 'blocks_rejected_strict',
                                                             'blocks_rejected_template', 'tar_member_problems')}),
              round(time.time() - t0), 's', flush=True)
    tot = {}
    for r in rows.values():
        t = tot.setdefault(r['track'], {'archive_sets': 0, 'blocks': 0, 'blocks_rejected_strict': 0,
                                        'blocks_rejected_template': 0, 'tar_member_problems': 0,
                                        'archive_codes_not_in_plan_check': 0})
        t['archive_sets'] += 1
        for k in ('blocks', 'blocks_rejected_strict', 'blocks_rejected_template', 'tar_member_problems'):
            t[k] += r[k]
        t['archive_codes_not_in_plan_check'] += len(r['archive_codes_not_in_plan_check'])
    summary = {'validator_version': v.VERSION, 'validator_sha256': sc.sha_file(v.__file__),
               'scan_archives_sha256': sc.sha_file(sa.__file__), 'script_sha256': sc.sha_file(__file__),
               'seconds': time.time() - t0, 'totals_by_track': tot, 'archive_sets': rows}
    (HERE / 'archive_scan_summary.json').write_text(json.dumps(summary, indent=1) + '\n')
    print(json.dumps(tot, indent=1))


if __name__ == '__main__':
    main()
