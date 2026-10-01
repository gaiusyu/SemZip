"""Stability analysis of single LLM syntheses vs QG-V1 vs QG-POOL (reads ../dev_results).
Held-out sample for multi-block files (<=8 evenly spaced original blocks >=1); block-0 (in-sample) for one-block files.
Outputs: stability.json, stability_table.tex, stability.pdf/png"""
import json, statistics as st, math
from pathlib import Path
H = Path(__file__).resolve().parent; D = H.parent/'dev_results'
DS = 'Linux,Proxifier,Apache,Zookeeper,Mac,HealthApp,HPC,Hadoop,OpenStack,OpenSSH,Android,BGL,HDFS,Spark,Windows,Thunderbird'.split(',')
POOLED = ['c0', 'c1', 'c2', 'c3', 'c4']
def J(p): p = D/p; return json.loads(p.read_text()) if p.exists() else None
rows = {}
for d in DS:
    s = J(f'heldout/{d}.sample.json') or {}
    heldout = bool(s.get('indices'))
    row = {'dataset': d, 'scope': 'heldout' if heldout else 'block0', 'raw': {}, 'qg1': {}, 'pool': None, 'train_qg1': {}, 'train_raw': {}}
    for c in POOLED + ['g1']:
        q = J(f'runs/qg/{c}/{d}/qg_report.json')
        if q:
            row['train_raw'][c] = q['original_train_archive_bytes']; row['train_qg1'][c] = q['selected_train_archive_bytes']
        elif (D/f'runs/qg/{c}/{d}/CANDIDATE_UNUSABLE.json').exists():
            row['train_raw'][c] = row['train_qg1'][c] = q['empty_train_archive_bytes'] if q else None
        for v in ['raw', 'qg1']:
            if heldout:
                r = J(f'runs/heldout/{c}_{v}/{d}/result.json')
                if r: row[v][c] = r['archive_bytes']
            elif q:
                row[v][c] = row['train_raw'][c] if v == 'raw' else row['train_qg1'][c]
    p = J(f'runs/pool/{d}/pool_report.json')
    if heldout:
        r = J(f'runs/heldout/pool_pool/{d}/result.json'); row['pool'] = r['archive_bytes'] if r else None
    elif p: row['pool'] = p['selected_train_bytes']
    if p:
        row['pool_start'] = p['start']
        # best-of-K single candidate chosen by block-0 cost after QG-V1 (training-only choice)
        row['bestofk'] = row['qg1'].get(p['start'])
    rows[d] = row
def spread(vals):
    vals = [v for v in vals if v]
    return (max(vals) / min(vals) - 1) * 100 if len(vals) >= 2 else None
summary = {}
for d, r in rows.items():
    raw = [r['raw'].get(c) for c in POOLED]; q = [r['qg1'].get(c) for c in POOLED]
    r['raw_spread_pct'] = spread(raw); r['qg1_spread_pct'] = spread(q)
    if r['raw'].get('c0') and r['raw'].get('g1'):
        r['greedy_repeat_diff_pct'] = (r['raw']['g1'] / r['raw']['c0'] - 1) * 100
    if r['pool'] and all(raw):
        r['pool_vs_raw_median_pct'] = (r['pool'] / st.median(raw) - 1) * 100
        r['pool_vs_raw_best_pct'] = (r['pool'] / min(raw) - 1) * 100
        r['pool_vs_c0_pct'] = (r['pool'] / raw[0] - 1) * 100
(H/'stability.json').write_text(json.dumps(rows, indent=1))
ok = [r for r in rows.values() if r['raw_spread_pct'] is not None and r['qg1_spread_pct'] is not None]
if ok:
    summary = {'datasets': len(ok), 'median_raw_spread_pct': st.median(r['raw_spread_pct'] for r in ok),
               'median_qg1_spread_pct': st.median(r['qg1_spread_pct'] for r in ok),
               'max_raw_spread_pct': max(r['raw_spread_pct'] for r in ok), 'max_qg1_spread_pct': max(r['qg1_spread_pct'] for r in ok)}
    print(json.dumps(summary, indent=1))
for d, r in rows.items():
    print(f"{d:12s} {r['scope']:7s} raw_spread={r['raw_spread_pct'] and round(r['raw_spread_pct'],1)} qg1_spread={r['qg1_spread_pct'] and round(r['qg1_spread_pct'],1)}"
          f" pool_vs_c0={r.get('pool_vs_c0_pct') and round(r['pool_vs_c0_pct'],2)} pool_vs_best_raw={r.get('pool_vs_raw_best_pct') and round(r['pool_vs_raw_best_pct'],2)}"
          f" greedy_repeat={r.get('greedy_repeat_diff_pct') and round(r['greedy_repeat_diff_pct'],2)}")
# figure: archive size relative to the pool deployment (100% = pool); lower is better
try:
    import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
    ds = [d for d in DS if rows[d]['pool'] and all(rows[d]['raw'].get(c) for c in POOLED)]
    if ds:
        fig, ax = plt.subplots(figsize=(7.2, 2.6))
        for i, d in enumerate(ds):
            r = rows[d]; base = r['pool']
            for j, c in enumerate(POOLED):
                ax.scatter(i - 0.12, 100 * r['raw'][c] / base, s=16, facecolors='none', edgecolors='#C7253E', lw=0.9, zorder=3, label='single synthesis' if (i == 0 and j == 0) else None)
                if r['qg1'].get(c):
                    ax.scatter(i + 0.12, 100 * r['qg1'][c] / base, s=14, color='#3F6CCB', zorder=3, label='single synthesis + QG-V1' if (i == 0 and j == 0) else None)
        ax.axhline(100, color='#16875B', lw=1.2, label='QG-POOL (selected)')
        ax.set_xticks(range(len(ds))); ax.set_xticklabels(ds, rotation=40, ha='right', fontsize=7)
        ax.set_ylabel('archive bytes vs. pool (%)', fontsize=8); ax.tick_params(axis='y', labelsize=7)
        ax.set_yscale('log'); ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f'{v:g}'))
        ax.legend(fontsize=6.5, frameon=False, ncol=3, loc='upper right'); ax.grid(axis='y', alpha=0.25)
        fig.tight_layout(); fig.savefig(H/'stability.pdf'); fig.savefig(H/'stability.png', dpi=160)
        print('figure written', len(ds), 'datasets')
except Exception as e:
    print('figure skipped:', e)
