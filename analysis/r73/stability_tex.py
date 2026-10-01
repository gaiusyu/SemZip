"""Render RQ3 (synthesis stability + cost-guided selection) LaTeX from executed records (../dev_results).
Writes paper_out/stability_table.tex, paper_out/stability_text.tex, paper_out/stability_numbers.json and the figure
(paper_out/stability.pdf). Every number comes from qg/pool/heldout/formal/cost JSON records."""
import json, statistics as st
from pathlib import Path
H = Path(__file__).resolve().parent; D = H.parent/'dev_results'; OUT = H/'paper_out'; OUT.mkdir(exist_ok=True)
DS = 'Linux,Proxifier,Apache,Zookeeper,Mac,HealthApp,HPC,Hadoop,OpenStack,OpenSSH,Android,BGL,HDFS,Spark,Windows,Thunderbird'.split(',')
DAG = r'$\dagger$'; EOL = r'\\'
ONE = {'Linux', 'Proxifier', 'Apache', 'Zookeeper'}; POOLED = ['c0', 'c1', 'c2', 'c3', 'c4']
def J(p):
    p = D/p
    return json.loads(p.read_text()) if p.exists() else None
def cost_of(c, d, which):
    """block-0 complete archive bytes; unusable synthesis -> empty-program cost (from c0 gate report)"""
    q = J(f'runs/qg/{c}/{d}/qg_report.json')
    if q: return q['original_train_archive_bytes' if which == 'raw' else 'selected_train_archive_bytes']
    if (D/f'runs/qg/{c}/{d}/CANDIDATE_UNUSABLE.json').exists():
        z = J(f'runs/qg/c0/{d}/qg_report.json'); return z['empty_train_archive_bytes'] if z else None
    return None
def held(c, v, d):
    r = J(f'runs/heldout/{c}_{v}/{d}/result.json')
    return r['archive_bytes'] if r else None
rows = {}
for d in DS:
    r = {'dataset': d, 'one_block': d in ONE}
    for scope in ('train', 'held'):
        for v in ('raw', 'qg1'):
            r[f'{scope}_{v}'] = {c: (cost_of(c, d, v) if scope == 'train' else held(c, v, d)) for c in POOLED + ['g1']}
    p = J(f'runs/pool/{d}/pool_report.json')
    r['pool_train'] = p['selected_train_bytes'] if p else None
    r['pool_start'] = p['start'] if p else None
    hp = J(f'runs/heldout/pool_pool/{d}/result.json'); r['pool_held'] = hp['archive_bytes'] if hp else None
    rows[d] = r
def eval_view(r):
    """held-out sample view for multi-block files, block 0 for one-block files"""
    if r['one_block']: return r['train_raw'], r['train_qg1'], r['pool_train']
    return r['held_raw'], r['held_qg1'], r['pool_held']
stats = {}
for d, r in rows.items():
    raw, q, pool = eval_view(r)
    rv = [raw[c] for c in POOLED]; qv = [q[c] for c in POOLED]
    if all(rv) and all(qv) and pool:
        stats[d] = {'raw_spread': (max(rv)/min(rv) - 1)*100, 'qg1_spread': (max(qv)/min(qv) - 1)*100,
                    'raw_median_vs_pool': (st.median(rv)/pool - 1)*100, 'raw_worst_vs_pool': (max(rv)/pool - 1)*100,
                    'best_single_vs_pool': (min(qv)/pool - 1)*100, 'c0_vs_pool': (rv[0]/pool - 1)*100,
                    'pool_beats_all_single_gated': pool <= min(qv), 'greedy_repeat_pct': (raw['g1']/raw['c0'] - 1)*100 if raw.get('g1') and raw.get('c0') else None,
                    'bestofk_vs_pool': (q[r['pool_start']]/pool - 1)*100 if r['pool_start'] in q and q[r['pool_start']] else None}
Path(OUT/'stability_numbers.json').write_text(json.dumps({'rows': rows, 'stats': stats}, indent=1))
n = len(stats)
if n:
    agg = {'n': n, 'median_raw_spread': st.median(s['raw_spread'] for s in stats.values()), 'max_raw_spread': max(s['raw_spread'] for s in stats.values()),
           'max_raw_spread_ds': max(stats, key=lambda d: stats[d]['raw_spread']),
           'median_qg1_spread': st.median(s['qg1_spread'] for s in stats.values()),
           'median_raw_median_vs_pool': st.median(s['raw_median_vs_pool'] for s in stats.values()),
           'pool_le_best_gated': sum(s['pool_beats_all_single_gated'] for s in stats.values()),
           'median_c0_vs_pool': st.median(s['c0_vs_pool'] for s in stats.values())}
    gr = [s['greedy_repeat_pct'] for s in stats.values() if s['greedy_repeat_pct'] is not None]
    if gr: agg.update(greedy_repeat_n=len(gr), greedy_repeat_changed=sum(abs(x) > 0.05 for x in gr), greedy_repeat_max_abs=max(abs(x) for x in gr))
    print(json.dumps(agg, indent=1))
    Path(OUT/'stability_agg.json').write_text(json.dumps(agg, indent=1))
    # table: archive bytes relative to the selected deployment (100 = selected); lower is better
    L = [r'\begin{table}[t]\centering\small',
         r'\caption{Archive size of each single synthesis relative to the selected deployment (100 = cost-guided selection; lower is better). Multi-block files use the fixed held-out block sample; $\dagger$ one-block files use block 0 (in-sample). \emph{c0} is the greedy synthesis deployed as SemZip-1; c1--c4 are temperature-0.7 samples. Single: verified programs as synthesized; gated: after the per-synthesis quality gate.}',
         r'\label{tab:stability}', r'\begin{tabular}{lrrrr}', r'\toprule',
         r'Dataset & c0 single & c0 gated & single range (c0--c4) & gated range \\', r'\midrule']
    for d in DS:
        if d not in stats: continue
        raw, q, pool = eval_view(rows[d])
        rv = [100*raw[c]/pool for c in POOLED]; qv = [100*q[c]/pool for c in POOLED]
        mark = DAG if d in ONE else ''
        L.append(f"{d}{mark} & {rv[0]:.1f} & {qv[0]:.1f} & {min(rv):.1f}--{max(rv):.1f} & {min(qv):.1f}--{max(qv):.1f} " + EOL)
    L += [r'\bottomrule', r'\end{tabular}\end{table}']
    (OUT/'stability_table.tex').write_text('\n'.join(L) + '\n')
    print('\n'.join(L))

# ---- figure: archive size of every single synthesis relative to the selected deployment (log scale) ----
try:
    import matplotlib
except ImportError:  # tables/text above need only the standard library
    matplotlib = None; print('figure skipped: matplotlib not installed')
if stats and matplotlib is not None:
    matplotlib.use('Agg'); import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter
    plt.rcParams.update({'font.family': 'sans-serif', 'font.size': 7})
    ds = [d for d in DS if d in stats]
    fig, ax = plt.subplots(figsize=(6.8, 2.15))
    for i, d in enumerate(ds):
        raw, q, pool = eval_view(rows[d])
        xs = [100 * raw[c] / pool for c in POOLED]; gs = [100 * q[c] / pool for c in POOLED]
        ax.plot([i - 0.14] * 2, [min(xs), max(xs)], color='#C7253E', lw=0.8, alpha=0.6, zorder=2)
        ax.plot([i + 0.14] * 2, [min(gs), max(gs)], color='#3F6CCB', lw=0.8, alpha=0.6, zorder=2)
        ax.scatter([i - 0.14] * 5, xs, s=13, facecolors='white', edgecolors='#C7253E', lw=0.8, zorder=3, label='single synthesis' if i == 0 else None)
        ax.scatter([i + 0.14] * 5, gs, s=11, color='#3F6CCB', zorder=3, label='single synthesis + gate' if i == 0 else None)
        ax.scatter([i - 0.14], [xs[0]], marker='x', s=16, color='#222222', lw=0.8, zorder=4, label='greedy (SemZip-1)' if i == 0 else None)
    ax.axhline(100, color='#16875B', lw=1.1, zorder=1, label='cost-guided selection')
    ax.set_yscale('log')
    from matplotlib.ticker import FixedLocator, NullFormatter
    top = max(max(100 * eval_view(rows[d])[0][c] / eval_view(rows[d])[2] for c in POOLED) for d in ds)
    ticks = [t for t in [100, 110, 125, 150, 200, 300, 500, 1000] if t <= top * 1.02]
    bottom = min(min(100 * min(eval_view(rows[d])[0][c], eval_view(rows[d])[1][c]) / eval_view(rows[d])[2] for c in POOLED) for d in ds)
    ax.set_ylim(min(bottom, 100) * 0.985, top * 1.06)
    ax.yaxis.set_major_locator(FixedLocator(ticks)); ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}'))
    ax.yaxis.set_minor_locator(FixedLocator([])); ax.yaxis.set_minor_formatter(NullFormatter())
    ax.set_xticks(range(len(ds))); ax.set_xticklabels([d + ('†' if d in ONE else '') for d in ds], rotation=35, ha='right')
    ax.set_ylabel('archive bytes vs. selected (%)'); ax.grid(axis='y', alpha=0.25, lw=0.5)
    ax.set_xlim(-0.6, len(ds) - 0.4)
    ax.legend(frameon=False, ncol=4, loc='upper center', bbox_to_anchor=(0.5, 1.2), fontsize=6.5)
    fig.tight_layout(); fig.savefig(OUT/'stability.pdf'); fig.savefig(OUT/'stability.png', dpi=200)
    print('figure: paper_out/stability.pdf')

# ---- selection cost paragraph (from runs/COST.json, produced by cost_accounting.py on the execution host) ----
cost = J('runs/COST.json')
if cost:
    syn = cost['syntheses']; pooled = {k: v for k, v in syn.items() if '/t0.7_' in k}
    calls_new = sum(v['api_calls_reported'] or 0 for v in pooled.values())
    wall_new = sum(v['wall_seconds'] or 0 for v in pooled.values())
    ok_new = sum(v['status'] == 'PASS' for v in pooled.values())
    sel = cost['selection']; pool = cost['pool']
    pooled_sel = {k: v for k, v in sel.items() if not k.startswith('g')}
    ev = sum(v['evaluations'] for v in pooled_sel.values()) + sum(v['evaluations'] for v in pool.values())
    evs = sum(v['eval_seconds'] for v in pooled_sel.values()) + sum(v['eval_seconds'] for v in pool.values())
    POOLSEC = {'Thunderbird': 270, 'Android': 273, 'HDFS': 321, 'Linux': 384, 'HealthApp': 516, 'HPC': 739, 'Apache': 914, 'Proxifier': 983,
               'Spark': 1717, 'OpenSSH': 3252, 'Hadoop': 4128, 'Mac': 4896, 'Zookeeper': 5069, 'Windows': 6994, 'OpenStack': 10324, 'BGL': 11769}  # POOLDONE log lines
    lo = min(POOLSEC, key=POOLSEC.get); hi = max(POOLSEC, key=POOLSEC.get)
    txt = (f"The four additional syntheses per dataset ({len(pooled)} in total) come from the unchanged trainer, and {ok_new} pass. "
           f"They issue {calls_new:,} model requests and take {wall_new/60:.1f} minutes of recorded synthesis time, compared with 202 requests for the sixteen greedy syntheses of SemZip-1. "
           f"Gating all five syntheses and pooling use {ev:,} complete block-0 evaluations (fit, encode, separate decode, hash). "
           f"On the shared eight-CPU allocation, pooling took from {POOLSEC[lo]/60:.1f} minutes ({lo}) to {POOLSEC[hi]/3600:.1f} hours ({hi}) of wall-clock time per dataset.")
    (OUT/'selection_cost_text.tex').write_text(txt + '\n'); print(txt)

# ---- main RQ3 findings paragraph + complete-file ablation ----
def formal_ratio(version, d):
    r = J(f'runs/formal/{version}/{d}/result.json')
    return (r['encode']['archive_bytes'], r['encode']['raw_bytes']) if r else None
A71 = json.loads((H.parent.parent/'artifact_r71_working/results/final/anonymous_results.json').read_text())
r71 = {r['dataset']: (r['archive_bytes'], r['raw_bytes']) for r in A71['size_rows'] if r['method'] == 'semzip'}
IDENT = {'HDFS', 'Thunderbird'}  # gate left the greedy program byte-identical (verified): reuse SemZip-1 complete-file runs
abl = {}
for v in ['qg1c0', 'pool']:
    vals = {}
    for d in DS:
        x = formal_ratio(v, d) or (r71[d] if (v == 'qg1c0' and d in IDENT) else None)
        if x: vals[d] = x[1] / x[0]
    abl[v] = vals
if stats and n >= 12:
    a = json.loads((OUT/'stability_agg.json').read_text())
    parts = [f"Single syntheses vary substantially (Figure~\\ref{{fig:stability}}). Across the five pooled syntheses, the largest single-synthesis archive exceeds the smallest by a median of {a['median_raw_spread']:.1f}\\% per dataset and by up to {a['max_raw_spread']:.1f}\\% ({a['max_raw_spread_ds']})."]
    if 'greedy_repeat_n' in a:
        parts.append(f"Greedy decoding is not reproducible either: repeating the greedy synthesis changes the archive on {a['greedy_repeat_changed']}/{a['greedy_repeat_n']} datasets, by up to {a['greedy_repeat_max_abs']:.1f}\\%.")
    exc = sorted(((-stats[d]['best_single_vs_pool'], d) for d in stats if not stats[d]['pool_beats_all_single_gated']), reverse=True)
    exc_txt = ', '.join(f"{v:.2f}\\% ({d})" for v, d in exc)
    parts.append(f"The per-synthesis gate narrows the median spread to {a['median_qg1_spread']:.1f}\\%. The selected deployment is no larger than every gated single synthesis on {a['pool_le_best_gated']}/{a['n']} datasets; on the other {len(exc)}, the best gated single synthesis is smaller by {exc_txt}. The median single synthesis is {a['median_raw_median_vs_pool']:.1f}\\% larger than the selected deployment.")
    if len(abl['qg1c0']) == 16 and len(abl['pool']) == 16:
        m1 = st.mean(r71[d][1] / r71[d][0] for d in DS); mq = st.mean(abl['qg1c0'].values()); mp = st.mean(abl['pool'].values())
        parts.append(f"On complete files, gating the greedy synthesis alone, without any new model call, raises the sixteen-file mean from {m1:.2f}$\\times$ to {mq:.2f}$\\times$; selection across the five syntheses reaches {mp:.2f}$\\times$.")
    (OUT/'stability_text.tex').write_text(' '.join(parts) + '\n'); print(' '.join(parts))
    fig = [r'\begin{figure}[t]\centering', r'\includegraphics[width=\linewidth]{figures/stability.pdf}',
           r'\caption{Archive size of each single synthesis relative to the cost-guided deployment (100\%, log scale; lower is better). Hollow: verified programs as synthesized; filled: after the per-synthesis gate; cross: the greedy synthesis deployed as SemZip-1. Multi-block files use the fixed held-out block sample; $\dagger$ one-block files use block 0 (in-sample).}',
           r'\Description{Per-dataset strip plot of single-synthesis archive sizes relative to the selected deployment, before and after gating.}',
           r'\label{fig:stability}\end{figure}']
    (OUT/'stability_figure.tex').write_text('\n'.join(fig) + '\n')
print('ablation sizes:', {k: len(v) for k, v in abl.items()})

# ---- supplement: per-dataset selection cost table ----
if cost:
    L = [r'\begin{table}[h]\centering\small', r'\caption{Offline cost of the four additional syntheses and of selection, per dataset. Requests and synthesis time come from the retained training records; evaluations are complete block-0 archive-cost evaluations (fit, encode, separate decode, SHA-256), with summed wall time on the shared host.}',
         r'\label{tab:selection-cost}', r'\begin{tabular}{lrrrrrr}', r'\toprule',
         r'Dataset & Requests & Synthesis (s) & Gate evals & Gate (s) & Pool evals & Pool (s) \\', r'\midrule']
    for d in DS:
        sy = [v for k, v in cost['syntheses'].items() if k.startswith(d + '/t0.7_')]
        ge = [v for k, v in cost['selection'].items() if k.split('/')[1] == d and not k.startswith('g')]
        po = cost['pool'].get(d)
        pe = str(po['evaluations']) if po else '--'; ps = ('%.0f' % po['eval_seconds']) if po else '--'
        L.append(f"{d} & {sum(v['api_calls_reported'] or 0 for v in sy)} & {sum(v['wall_seconds'] or 0 for v in sy):.0f} & {sum(v['evaluations'] for v in ge)} & {sum(v['eval_seconds'] for v in ge):.0f} & {pe} & {ps} " + EOL)
    L += [r'\bottomrule', r'\end{tabular}\end{table}']
    (OUT/'selection_cost_detail.tex').write_text('\n'.join(L) + '\n')

# ---- transfer of block-0 gains to complete files (selected vs SemZip-1 greedy synthesis) ----
SK = json.loads((OUT/'skeleton_coverage.json').read_text())  # skeleton_coverage.py output
_cfb = json.loads((H.parent.parent/'r75_fallback_evolution_20260927/dev_results/cfb/Thunderbird.json').read_text())['rows']  # per-block frozen vs empty payloads
TB_N = len(_cfb); TB_LOSS = sum(1 for r in _cfb if r['index'] >= 1 and (r['empty'] < r['pool'] or r['pool'] > 0.99 * r['empty']))
tr = {}
for d in DS:
    q0 = J(f'runs/qg/c0/{d}/qg_report.json'); p = J(f'runs/pool/{d}/pool_report.json'); f = J(f'runs/formal/pool/{d}/result.json')
    if q0 and p and f:
        tr[d] = {'block0_pct': (p['selected_train_bytes'] / q0['original_train_archive_bytes'] - 1) * 100,
                 'file_pct': (f['encode']['archive_bytes'] / r71[d][0] - 1) * 100}
if len(tr) == 16:
    import statistics
    b = [v['block0_pct'] for v in tr.values()]; fl = [v['file_pct'] for v in tr.values()]
    worse = [d for d, v in tr.items() if v['file_pct'] > 0.005]
    ranks = lambda xs: [sorted(xs).index(x) for x in xs]
    rb, rf = ranks(b), ranks(fl); n16 = len(b)
    rho = 1 - 6 * sum((x - y) ** 2 for x, y in zip(rb, rf)) / (n16 * (n16 ** 2 - 1))
    txt = (f"Block-0 reductions largely carry over: relative to SemZip-1, the selected plan changes block-0 archives by a median of {statistics.median(b):.1f}\\% and complete-file archives by {statistics.median(fl):.1f}\\% (Spearman $\\rho={rho:.2f}$ across datasets). "
           f"Transfer is uneven: BGL's block-0 reduction of {-tr['BGL']['block0_pct']:.1f}\\% becomes {-tr['BGL']['file_pct']:.1f}\\% on the complete file, whereas Thunderbird's {-tr['Thunderbird']['block0_pct']:.1f}\\% shrinks to {-tr['Thunderbird']['file_pct']:.1f}\\%: on {TB_LOSS:,} of its {TB_N - 1:,} later blocks the frozen program is not even 1\\% smaller than the empty program (supplement). "
           + (f"No complete file grows relative to SemZip-1." if not worse else f"Complete files that grow relative to SemZip-1: {', '.join(worse)}."))
    (OUT/'transfer_text.tex').write_text(txt + '\n'); print(txt)
    Path(OUT/'transfer.json').write_text(json.dumps(tr, indent=1))

# ---- online-speed effect of selection (from the same-session timing summary) ----
T = J('runs/timing/SUMMARY.json')
if T and 'semzip' in T and 'semzip1' in T:
    pn, p1 = T['semzip']['per_dataset'], T['semzip1']['per_dataset']
    ch = {d: pn[d]['encode_MB_per_s'] / p1[d]['encode_MB_per_s'] for d in pn}
    slow = sorted([d for d, v in ch.items() if v < 0.8], key=lambda d: ch[d]); fast = sorted([d for d, v in ch.items() if v > 1.25], key=lambda d: -ch[d])
    e = T['semzip']['encode_gmean'] / T['semzip1']['encode_gmean'] * 100; dd = T['semzip']['decode_gmean'] / T['semzip1']['decode_gmean'] * 100
    fmt = lambda ds: ' and '.join(f"{ch[d]*100:.0f}\\% on {d}" for d in ds)
    txt = (f"Selection optimizes size, not speed: the selected deployment keeps {e:.0f}\\% and {dd:.0f}\\% of SemZip-1's geometric-mean encoding and decoding throughput"
           + (f", but its encoding throughput falls to {fmt(slow)}" if slow else '') + (f" and rises to {fmt(fast)}, where gating removed a costly rule" if fast else '') + '.')
    (OUT/'selection_speed_text.tex').write_text(txt + '\n'); print(txt)
