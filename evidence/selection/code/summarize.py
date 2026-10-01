"""Collect QG / pool / held-out / formal results into runs/SUMMARY.json and print compact tables."""
import json, statistics as st
from pathlib import Path
DS = 'Linux,Proxifier,Apache,Zookeeper,Mac,HealthApp,HPC,Hadoop,OpenStack,OpenSSH,Android,BGL,HDFS,Spark,Windows,Thunderbird'.split(',')
C = ['c0', 'c1', 'c2', 'c3', 'c4', 'g1']
def J(p): p = Path(p); return json.loads(p.read_text()) if p.exists() else None
out = {'qg': {}, 'pool': {}, 'heldout': {}, 'formal': {}}
print('== training block 0 complete archive bytes: raw -> QG-V1 (per candidate)')
for d in DS:
    row = {}
    for c in C:
        q = J(f'runs/qg/{c}/{d}/qg_report.json')
        if q: row[c] = (q['original_train_archive_bytes'], q['selected_train_archive_bytes'])
        elif Path(f'runs/qg/{c}/{d}/CANDIDATE_UNUSABLE.json').exists(): row[c] = 'unusable'
    out['qg'][d] = row
    p = J(f'runs/pool/{d}/pool_report.json')
    if p: out['pool'][d] = {'start': p['start'], 'start_bytes': p['start_train_bytes'], 'selected': p['selected_train_bytes'], 'evals': p['evaluations']}
    cells = ' '.join(f'{c}:{v[0]}->{v[1]}' if isinstance(v, tuple) else f'{c}:{v}' for c, v in row.items())
    print(f'{d:12s} {cells}' + (f"  | POOL {out['pool'][d]['selected']}" if d in out['pool'] else ''))
print('\n== held-out sample ratio (raw / qg1 per candidate; pool)')
for d in DS:
    row = {}
    for c in C:
        for v in ['raw', 'qg1']:
            r = J(f'runs/heldout/{c}_{v}/{d}/result.json')
            if r: row[f'{c}_{v}'] = round(r['ratio'], 3)
    r = J(f'runs/heldout/pool_pool/{d}/result.json')
    if r: row['pool'] = round(r['ratio'], 3)
    out['heldout'][d] = row
    if row: print(f'{d:12s}', row)
print('\n== formal complete-file ratios')
for ver in ['qg1c0', 'pool']:
    vals = {}
    for d in DS:
        r = J(f'runs/formal/{ver}/{d}/result.json')
        if r: vals[d] = {'ratio': r['encode']['compression_ratio'], 'heldout': r['heldout']['ratio'], 'fallback': r['encode']['semantic_fallback_blocks'],
                         'enc_MBps_firstpass': r['encode_MB_per_s'], 'archive_bytes': r['encode']['archive_bytes']}
    out['formal'][ver] = vals
    if vals:
        print(ver, {d: round(v['ratio'], 2) for d, v in vals.items()}, 'mean' if len(vals) == 16 else f'n={len(vals)}', round(st.mean(v['ratio'] for v in vals.values()), 3))
Path('runs/SUMMARY.json').write_text(json.dumps(out, indent=1))
