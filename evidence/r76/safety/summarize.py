#!/usr/bin/env python3
"""R76 track E: summarize the validator-enforced full decode runs (read-only on run outputs).

Reads full_20260929/status.json (V1 validator, pre-registered) and v2/full_v2_20260929/status.json
(V2 validator) if present, prints a markdown table and writes SAFETY_SUMMARY.json next to this file.
Usage: python3 summarize.py
"""
import json
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUNS = [('V1', HERE / 'full_20260929'), ('V2', HERE / 'v2' / 'full_v2_20260929')]
ORDER = ['Linux', 'Proxifier', 'Apache', 'Zookeeper', 'Mac', 'HealthApp', 'HPC', 'Hadoop', 'OpenSSH', 'OpenStack',
         'Android', 'BGL', 'HDFS', 'Spark', 'Windows', 'Thunderbird']


def load(root):
    p = root / 'status.json'
    if not p.exists():
        return None
    return json.loads(p.read_text())


def main():
    out = {'generated_unix': time.time(), 'runs': {}}
    lines = []
    for label, root in RUNS:
        st = load(root)
        if st is None:
            out['runs'][label] = {'status': 'NOT STARTED', 'root': str(root)}
            lines.append('\n%s (%s): not started\n' % (label, root))
            continue
        rows = st['rows']
        by = {}
        for r in rows:
            by.setdefault(r['dataset'], {})[r['policy']] = r
        done = (root / 'VERIFY_SUMMARY.json').exists()
        tab = []
        lines.append('\n%s validator run: %s  (%s)\n' % (label, 'FINISHED' if done else 'IN PROGRESS', root))
        lines.append('| dataset | blocks | raw bytes | archive bytes | ratio | strict | template (post-hoc) | '
                     'decode s | codes validated | exec calls |')
        lines.append('|---|---:|---:|---:|---:|---|---|---:|---:|---:|')
        for d in ORDER:
            if d not in by:
                lines.append('| %s | | | | | pending | | | | |' % d)
                continue
            s, t = by[d].get('strict'), by[d].get('template')
            final = t if (s and s['status'] != 'PASS' and t) else s
            row = {'dataset': d, 'blocks': s['blocks'], 'raw_bytes': s['raw_bytes'], 'archive_bytes': s['archive_bytes'],
                   'ratio': s['ratio'], 'strict': s['status'], 'strict_error': s.get('decode_error'),
                   'template': t['status'] if t else None,
                   'sha_pass_final': final['status'] == 'PASS' and final['decoded_sha256'] == final['expected_sha256'],
                   'decoded_sha256': final.get('decoded_sha256'), 'expected_sha256': final.get('expected_sha256'),
                   'decode_seconds': final.get('decode_seconds'), 'unique_codes_validated': final.get('unique_codes_validated'),
                   'exec_calls_total': final.get('exec_calls_total'),
                   'blocks_with_template_wrapped_code': final.get('blocks_with_template_wrapped_code')}
            tab.append(row)
            lines.append('| %s | %d | %d | %d | %.2f | %s | %s | %s | %s | %s |' % (
                d, row['blocks'], row['raw_bytes'], row['archive_bytes'], row['ratio'], row['strict'],
                row['template'] or '-', '%.1f' % row['decode_seconds'] if row['decode_seconds'] else '-',
                row['unique_codes_validated'], row['exec_calls_total']))
        n = len(tab)
        agg = {'datasets_done': n, 'strict_pass': [r['dataset'] for r in tab if r['strict'] == 'PASS'],
               'strict_fail': [r['dataset'] for r in tab if r['strict'] != 'PASS'],
               'template_pass': [r['dataset'] for r in tab if r['template'] == 'PASS'],
               'template_fail': [r['dataset'] for r in tab if r['template'] not in (None, 'PASS')],
               'final_sha_pass': sum(r['sha_pass_final'] for r in tab),
               'raw_bytes': sum(r['raw_bytes'] for r in tab), 'archive_bytes': sum(r['archive_bytes'] for r in tab),
               'blocks': sum(r['blocks'] for r in tab)}
        lines.append('\nstrict PASS %d/%d; strict FAIL (fail-closed) %s; template PASS %s; final SHA pass %d/%d; '
                     '%d blocks, %d raw bytes' % (len(agg['strict_pass']), n, agg['strict_fail'], agg['template_pass'],
                                                  agg['final_sha_pass'], n, agg['blocks'], agg['raw_bytes']))
        out['runs'][label] = {'finished': done, 'root': str(root), 'aggregate': agg, 'rows': tab}
    (HERE / 'SAFETY_SUMMARY.json').write_text(json.dumps(out, indent=1) + '\n')
    print('\n'.join(lines))


if __name__ == '__main__':
    main()
