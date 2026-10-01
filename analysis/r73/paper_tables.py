"""Build manuscript tables for the cost-guided-selection version from executed records only.
Inputs : ../dev_results/runs/formal/<version>/<D>/result.json (new SemZip, formal complete-file runs)
         artifact_r71_working/results/final/anonymous_results.json (R71 SemZip + R68 external baselines, audited)
Outputs: generated/*.tex + numbers.json in ./paper_out"""
import json, math, statistics as st, sys
from pathlib import Path
H = Path(__file__).resolve().parent; ROOT = H.parent.parent
DEV = H.parent/'dev_results'; OUT = H/'paper_out'; OUT.mkdir(exist_ok=True)
A = json.loads((ROOT/'artifact_r71_working/results/final/anonymous_results.json').read_text())
DS = A['protocol']['datasets']; ONE = {'Linux', 'Proxifier', 'Apache', 'Zookeeper'}
EVO = json.loads((H/'paper_out'/'evo_numbers.json').read_text())['evo_archive_bytes'] if (H/'paper_out'/'evo_numbers.json').exists() else {}
EXT = ['delog', 'loglite', 'gzip6', 'xz6', 'zstd3']
LABEL = {'delog': 'DeLog', 'loglite': 'LogLite-BL*', 'gzip6': 'gzip6', 'xz6': 'XZ6', 'zstd3': 'Zstd3'}
size = {}
for r in A['size_rows']: size.setdefault(r['method'], {})[r['dataset']] = r
def new_rows(version):
    out = {}
    for d in DS:
        p = DEV/'runs/formal'/version/d/'result.json'
        if p.exists():
            r = json.loads(p.read_text())
            assert r['status'] == 'PASS' and r['independent_materialized_file_sha_pass']
            out[d] = {'archive_bytes': r['encode']['archive_bytes'], 'raw_bytes': r['encode']['raw_bytes'],
                      'ratio': r['encode']['compression_ratio'], 'heldout': r['heldout'], 'fallback': r['encode']['semantic_fallback_blocks']}
    return out
def agg(rows):
    ok = [d for d in DS if d in rows]
    if len(ok) < len(DS): return None
    return {'mean': st.mean(rows[d]['ratio'] for d in DS),
            'corpus': sum(rows[d]['raw_bytes'] for d in DS) / sum(rows[d]['archive_bytes'] for d in DS),
            'total_archive': sum(rows[d]['archive_bytes'] for d in DS)}
def main(version='pool'):
    sem = new_rows(version); r71 = {d: size['semzip'][d] for d in DS}
    numbers = {'version': version, 'datasets_done': len(sem)}
    evo = {d: dict(sem[d], archive_bytes=EVO.get(d, sem[d]['archive_bytes']), ratio=sem[d]['raw_bytes'] / EVO.get(d, sem[d]['archive_bytes'])) for d in sem}
    for name, rows in [('new', sem), ('evo', evo), ('r71', r71)] + [(m, size[m]) for m in EXT]:
        numbers[name] = agg(rows)
    if len(sem) == len(DS):
        dl = size['delog']
        numbers['wins_vs_delog'] = sum(sem[d]['archive_bytes'] < dl[d]['archive_bytes'] for d in DS)
        numbers['total_vs_delog_pct'] = (numbers['new']['total_archive'] / numbers['delog']['total_archive'] - 1) * 100
        numbers['wins_vs_r71'] = sum(sem[d]['archive_bytes'] < r71[d]['archive_bytes'] for d in DS)
        numbers['evo_wins_vs_delog'] = sum(evo[d]['archive_bytes'] < dl[d]['archive_bytes'] for d in DS)
        numbers['evo_total_vs_delog_pct'] = (numbers['evo']['total_archive'] / numbers['delog']['total_archive'] - 1) * 100
        numbers['per_dataset'] = {d: {'new': sem[d]['ratio'], 'r71': r71[d]['ratio'], 'delog': dl[d]['ratio']} for d in DS}
        # suffix (multi-block files)
        multi = [d for d in DS if d not in ONE]
        numbers['suffix_new_mean'] = st.mean(sem[d]['heldout']['ratio'] for d in multi)
        numbers['suffix_r71_mean'] = st.mean(r71[d]['heldout']['ratio'] for d in multi if r71[d].get('heldout') and r71[d]['heldout'].get('ratio'))
        numbers['suffix_delog_mean'] = None  # computed in texts() from the audited suffix comparison
        lines = [r'\begin{table*}[t]\centering\small',
                 r'\caption{Complete original-file compression ratios (raw / all archive bytes). SemZip: frozen cost-guided deployment trained on block 0; bold: best of frozen SemZip and external methods (not SemZip+Evo). SemZip+Evo adds optional online evolution (RQ4): each block stores the smallest payload among published programs and the empty program; new programs are published only on Spark and Thunderbird. SemZip-1: single-synthesis ablation. $\dagger$: one block, in-sample. *Disclosed LogLite adaptation.}',
                 r'\label{tab:external-ratios}', r'\setlength{\tabcolsep}{3.4pt}', r'\begin{tabular}{lrrrrrrrr}', r'\toprule',
                 r'Dataset & SemZip & SemZip+Evo & SemZip-1 & DeLog & LogLite-BL* & gzip6 & XZ6 & Zstd3 \\', r'\midrule']
        for d in DS:
            vals = [sem[d]['ratio'], evo[d]['ratio'], r71[d]['ratio']] + [size[m][d]['ratio'] for m in EXT]
            best = max(vals[:1] + vals[3:])
            cells = []
            for i, v in enumerate(vals):
                s = f'{v:.2f}'
                cells.append(r'\textbf{' + s + '}' if (i not in (1, 2) and v == best) else s)
            lines.append(d + (r'$\dagger$' if d in ONE else '') + ' & ' + ' & '.join(cells) + r' \\')
        lines += [r'\midrule', 'Arithmetic mean (16) & ' + ' & '.join(f'{numbers[k]["mean"]:.2f}' for k in ['new', 'evo', 'r71'] + EXT) + r' \\',
                  'Total raw / total archive & ' + ' & '.join(f'{numbers[k]["corpus"]:.2f}' for k in ['new', 'evo', 'r71'] + EXT) + r' \\',
                  r'\bottomrule', r'\end{tabular}\end{table*}']
        (OUT/'external_ratio_table.tex').write_text('\n'.join(lines) + '\n')
    (OUT/'numbers.json').write_text(json.dumps(numbers, indent=1))
    print(json.dumps({k: v for k, v in numbers.items() if k != 'per_dataset'}, indent=1))
if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'pool')

def texts(version='pool', timing=None):
    """Main-text numeric paragraphs. timing: dict method -> {'encode_gmean','decode_gmean'} from the same-session campaign."""
    sem = new_rows(version); r71 = {d: size['semzip'][d] for d in DS}; dl = size['delog']
    assert len(sem) == len(DS), 'formal complete-file results incomplete'
    n = agg(sem); nd = agg(dl); n1 = agg(r71)
    wins = sum(sem[d]['archive_bytes'] < dl[d]['archive_bytes'] for d in DS)
    tot = (n['total_archive'] / nd['total_archive'] - 1) * 100
    losses = [(d, (sem[d]['archive_bytes'] / dl[d]['archive_bytes'] - 1) * 100) for d in DS if sem[d]['archive_bytes'] > dl[d]['archive_bytes']]
    def fmt_losses(ls):
        return ', '.join(f'{d} ({p:.2f}\\% more archive bytes)' for d, p in ls) if ls else 'none'
    ext = [f"The cost-guided deployment has a sixteen-file arithmetic mean of {n['mean']:.2f}$\\times$ and a corpus ratio of {n['corpus']:.2f}$\\times$, compared with {n1['mean']:.2f}$\\times$ and {n1['corpus']:.2f}$\\times$ for the single-synthesis SemZip-1.",
           f"It produces smaller archives than independent DeLog on {wins}/16 jointly valid files.",
           f"Summed complete archive bytes change by {tot:+.2f}\\% relative to DeLog.",
           f"Files with larger archives than DeLog: {fmt_losses(losses)}."]
    evo = {d: dict(sem[d], archive_bytes=EVO.get(d, sem[d]['archive_bytes']), ratio=sem[d]['raw_bytes'] / EVO.get(d, sem[d]['archive_bytes'])) for d in DS}
    ne = agg(evo); ewins = sum(evo[d]['archive_bytes'] < dl[d]['archive_bytes'] for d in DS); etot = (ne['total_archive'] / nd['total_archive'] - 1) * 100
    if EVO:
        ext.append(f"With online evolution (RQ4): mean {ne['mean']:.2f}$\\times$, corpus {ne['corpus']:.2f}$\\times$, "
                   f"{ewins}/16 files smaller than DeLog, summed bytes {f'{etot:+.2f}'.replace('-', '$-$')}\\% "
                   f"(Thunderbird accounts for {100*(sem['Thunderbird']['archive_bytes'] - evo['Thunderbird']['archive_bytes'])/(n['total_archive'] - ne['total_archive']):.0f}\\% of the byte reduction).")
    (OUT/'external_results_text.tex').write_text('\n'.join(ext) + '\n')
    multi = [d for d in DS if d not in ONE]
    SUF = json.loads((ROOT/'artifact_r71_working/results/suffix/suffix_comparison.json').read_text())
    sdl = {r['dataset']: r for r in SUF['rows'] if r['method'] == 'delog'}
    for d in multi:
        assert sdl[d]['status'] == 'PASS' and sdl[d]['suffix_raw_bytes'] == sem[d]['heldout']['raw_bytes'], d
    sw = sum(sem[d]['heldout']['archive_bytes'] < sdl[d]['suffix_archive_bytes'] for d in multi)
    s_new = sum(sem[d]['heldout']['archive_bytes'] for d in multi); s_dl = sum(sdl[d]['suffix_archive_bytes'] for d in multi)
    s_raw = sum(sem[d]['heldout']['raw_bytes'] for d in multi)
    dl_suffix_mean = st.mean(sdl[d]['suffix_ratio'] for d in multi)
    suffix = (f"The suffix comparison retains the same {len(multi)} multi-block original files and excludes {len(ONE)} one-block files. "
              f"SemZip has smaller suffix archives than DeLog on {sw}/{len(multi)} jointly valid files. Their respective arithmetic means are "
              f"{st.mean(sem[d]['heldout']['ratio'] for d in multi):.2f}$\\times$ and {dl_suffix_mean:.2f}$\\times$, and corpus ratios are "
              f"{s_raw/s_new:.2f}$\\times$ and {s_raw/s_dl:.2f}$\\times$. SemZip's summed suffix archive bytes are {abs(s_new/s_dl-1)*100:.2f}\\% {'larger' if s_new > s_dl else 'smaller'} than DeLog's. "
              "The suffix and sixteen-file means use different dataset cohorts; selection and storage fitting never read these suffix blocks.")
    (OUT/'suffix_comparison_text.tex').write_text(suffix + '\n')
    blocks = sum(len(json.loads((DEV/'runs/formal'/version/d/'result.json').read_text())['block_audit']) for d in DS)
    fb = sum(sem[d]['fallback'] for d in DS)
    dep = (f"The cost-guided deployment independently restores 16/16 complete original files and all {blocks:,} blocks in those files; "
           f"the block guard takes the predetermined recovery on {fb} blocks.")
    (OUT/'deployment_results_text.tex').write_text(dep + '\n')
    speed = ''
    if timing and 'semzip' in timing and 'delog' in timing:
        e = timing['semzip']['encode_gmean'] / timing['delog']['encode_gmean'] * 100; dd = timing['semzip']['decode_gmean'] / timing['delog']['decode_gmean'] * 100
        speed = f" Its geometric-mean encoding and decoding throughputs are {e:.1f}\\% and {dd:.1f}\\% of DeLog's on the twelve-file timing cohort."
    sa = json.loads((OUT/'stability_agg.json').read_text()) if (OUT/'stability_agg.json').exists() else None
    stab = (f"Five independent syntheses per dataset differ in archive size by a median of {sa['median_raw_spread']:.1f}\\%; selection raises the sixteen-file mean from {n1['mean']:.2f}$\\times$ for the greedy synthesis alone to {n['mean']:.2f}$\\times$. ") if sa else ''
    ab = (stab + f"The selected deployment has a corpus ratio of {n['corpus']:.2f}$\\times$, produces smaller archives than DeLog on {wins}/16 files, "
          f"and differs from DeLog by {tot:+.2f}\\% in summed archive bytes." + speed
          + (f" A separately evaluated online evolution stage fires on two files and recovers Thunderbird timestamp rules "
             f"({sem['Thunderbird']['ratio']:.2f}$\\times$ to {evo['Thunderbird']['ratio']:.2f}$\\times$; DeLog {dl['Thunderbird']['ratio']:.2f}$\\times$); the sixteen-file mean becomes {ne['mean']:.2f}$\\times$." if EVO else ''))
    (OUT/'abstract_results.tex').write_text(ab + '\n')
    sp = ''
    if timing and 'semzip' in timing and 'delog' in timing:
        sp = f", at {timing['semzip']['encode_gmean']/timing['delog']['encode_gmean']*100:.1f}\\% and {timing['semzip']['decode_gmean']/timing['delog']['decode_gmean']*100:.1f}\\% of DeLog's encoding and decoding throughput"
    concl = (f"The frozen deployment averages {n['mean']:.2f}$\\times$ (corpus {n['corpus']:.2f}$\\times$), beating DeLog on {wins}/16 files"
             + (f" at {timing['semzip']['encode_gmean']/timing['delog']['encode_gmean']*100:.1f}\\%/{timing['semzip']['decode_gmean']/timing['delog']['decode_gmean']*100:.1f}\\% of its encoding/decoding throughput" if timing and 'semzip' in timing and 'delog' in timing else '')
             + (f"; with online evolution (RQ4), the mean is {ne['mean']:.2f}$\\times$ and {ewins}/16 files beat DeLog." if EVO else '.')
             + " Null outcomes and failures are retained.")
    (OUT/'conclusion_results.tex').write_text(concl + '\n')
    print(ext, suffix, dep, ab, sep='\n')

def speed_table(summary_path=DEV/'runs/timing/SUMMARY.json'):
    """Formal throughput table from the same-session serialized timing campaign (all methods)."""
    T = json.loads(Path(summary_path).read_text())
    order = [('semzip', 'SemZip'), ('semzip1', 'SemZip-1'), ('delog', 'DeLog'), ('loglite', 'LogLite-BL*'), ('gzip6', 'gzip6'), ('xz6', 'XZ6'), ('zstd3', 'Zstd3')]
    L = [r'\begin{table}[t]\centering\small',
         r'\caption{Formal throughput: geometric means of per-file three-run medians over all twelve files no larger than BGL (decimal MB/s of original input), measured for all methods except SemZip+Evo in one serialized session. Separate outer-CLI timers include startup and complete output. Per-file ranges are in the supplement.}',
         r'\label{tab:external-speed}', r'\begin{tabular}{lrr}', r'\toprule', r'Method & Encode & Decode \\', r'\midrule']
    for k, lab in order:
        if k in T: L.append(f"{lab} & {T[k]['encode_gmean']:.2f} & {T[k]['decode_gmean']:.2f} " + r'\\')
    L += [r'\bottomrule', r'\end{tabular}\end{table}']
    (OUT/'external_speed_table.tex').write_text('\n'.join(L) + '\n')
    per = T['semzip']['per_dataset']; dl = T['delog']['per_dataset']
    enc_w = sum(per[d]['encode_MB_per_s'] > dl[d]['encode_MB_per_s'] for d in per); dec_w = sum(per[d]['decode_MB_per_s'] > dl[d]['decode_MB_per_s'] for d in per)
    e = T['semzip']['encode_gmean'] / T['delog']['encode_gmean'] * 100; dd = T['semzip']['decode_gmean'] / T['delog']['decode_gmean'] * 100
    e1 = T['semzip']['encode_gmean'] / T['semzip1']['encode_gmean'] * 100; d1 = T['semzip']['decode_gmean'] / T['semzip1']['decode_gmean'] * 100
    txt = (f"Across the twelve-file cohort, SemZip's geometric-mean encoding and decoding throughputs are {e:.1f}\\% and {dd:.1f}\\% of DeLog's; "
           + (f"it is slower than DeLog on every file in both directions. " if enc_w == 0 and dec_w == 0 else f"it has higher per-file medians on {enc_w}/12 encoding and {dec_w}/12 decoding comparisons. ")
           + "These shared-host comparisons are descriptive, not significance tests.")
    (OUT/'external_speed_text.tex').write_text(txt + '\n'); print(txt)
    (OUT/'external_speed_table.tex').write_text('\n'.join(L) + '\n' + txt + '\n')
    return T

def tradeoff_figure(summary_path=DEV/'runs/timing/SUMMARY.json', version='pool'):
    try:
        import matplotlib
    except ImportError:  # the tables need only the standard library
        print('tradeoff figure skipped: matplotlib not installed'); return
    matplotlib.use('Agg'); import matplotlib.pyplot as plt
    T = json.loads(Path(summary_path).read_text()); sem = new_rows(version)
    ds = list(T['delog']['per_dataset'].keys())
    ratio = lambda rows: math.exp(st.mean(math.log(rows[d]['raw_bytes'] / rows[d]['archive_bytes']) for d in ds))
    rows = {'semzip': sem, 'semzip1': size['semzip']}
    rows.update({m: size[m] for m in EXT})
    sty = {'semzip': ('SemZip', '#b3323f', 'o', True), 'semzip1': ('SemZip-1', '#b3323f', 'o', False), 'delog': ('DeLog', '#2870a5', 's', True),
           'loglite': ('LogLite-BL*', '#8b5aa5', '^', True), 'gzip6': ('gzip6', '#757575', 'D', True), 'xz6': ('XZ6', '#198475', 'v', True), 'zstd3': ('Zstd3', '#c68a2f', 'P', True)}
    fig, axes = plt.subplots(1, 2, figsize=(5.6, 2.35), sharey=True)
    for ax, key, title in zip(axes, ['encode_gmean', 'decode_gmean'], ['(a) Encoding tradeoff', '(b) Decoding tradeoff']):
        for m, (lab, col, mk, filled) in sty.items():
            if m not in T: continue
            ax.scatter(T[m][key], ratio(rows[m]), s=34, marker=mk, color=col if filled else 'white', edgecolors=col, linewidths=1.1, label=lab, zorder=3)
        ax.set_xscale('log'); ax.set_title(title, fontsize=9, loc='left'); ax.grid(alpha=0.25, lw=0.5)
        from matplotlib.ticker import FixedLocator, FuncFormatter, NullFormatter
        ax.xaxis.set_major_locator(FixedLocator([1, 2, 5, 10, 20, 50])); ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f'{v:g}'))
        ax.xaxis.set_minor_formatter(NullFormatter())
        ax.set_xlabel('Throughput (MB/s; log scale)', fontsize=7.5); ax.tick_params(labelsize=7)
        for sp in ('top', 'right'): ax.spines[sp].set_visible(False)
    axes[0].set_ylabel('Compression ratio', fontsize=7.5)
    h, l = axes[0].get_legend_handles_labels()
    fig.legend(h, l, loc='lower center', ncol=4, frameon=False, fontsize=7, bbox_to_anchor=(0.5, -0.02))
    fig.tight_layout(rect=(0, 0.13, 1, 1)); fig.savefig(OUT/'external_tradeoff.pdf'); fig.savefig(OUT/'external_tradeoff.png', dpi=200)
    print('tradeoff figure written')

def selected_formal_table(version='pool'):
    """Supplement: per-file complete-file outcomes of the selected deployment (with SemZip-1 for reference)."""
    L = [r'\begin{longtable}{lrrrrrrr}', r'\caption{Selected deployment: complete-file outcomes. Suffix: original blocks 1 onward with the recomputed suffix manifest (R71 accounting); -- for one-block files. Recov.: blocks that took the predetermined recovery. Every file passed archive-only decoding and an independent materialized-file block audit.}\label{tab:selected-formal}\\',
         r'\toprule', r'Dataset & Blocks & Raw bytes & Archive bytes & Ratio & Suffix ratio & Recov. & SemZip-1 \\', r'\midrule\endfirsthead',
         r'\caption[]{(continued)}\\', r'\toprule', r'Dataset & Blocks & Raw bytes & Archive bytes & Ratio & Suffix ratio & Recov. & SemZip-1 \\', r'\midrule\endhead']
    for d in DS:
        r = json.loads((DEV/'runs/formal'/version/d/'result.json').read_text())
        e = r['encode']; h = r['heldout']
        L.append(f"{d} & {len(r['block_audit'])} & {e['raw_bytes']:,} & {e['archive_bytes']:,} & {e['compression_ratio']:.2f} & "
                 + (f"{h['ratio']:.2f}" if h['ratio'] else '--') + f" & {e['semantic_fallback_blocks']} & {size['semzip'][d]['ratio']:.2f} " + r'\\')
    L += [r'\bottomrule', r'\end{longtable}']
    (OUT/'selected_formal_table.tex').write_text('\n'.join(L) + '\n')

def timing_detailed_table(summary_path=DEV/'runs/timing/SUMMARY.json'):
    """Supplement: per-file three-run medians (min--max) for every method, same session."""
    T = json.loads(Path(summary_path).read_text())
    rows = []
    for p in sorted((DEV/'runs/timing').glob('*/result.json')):
        rows.append(json.loads(p.read_text()))
    order = [('semzip', 'SemZip'), ('semzip1', 'SemZip-1'), ('delog', 'DeLog'), ('loglite', 'LogLite-BL*'), ('gzip6', 'gzip6'), ('xz6', 'XZ6'), ('zstd3', 'Zstd3')]
    L = [r'\begin{longtable}{llrrrr}', r'\caption{Formal throughput cells (decimal MB/s): median and range of three clean serialized trials per file and method. A trial was repeated when another workload in the same eight-CPU allocation used more than 0.5 cores during its encode or decode phase; repeated attempts are retained in the artifact.}\label{tab:timing-cells}\\',
         r'\toprule', r'Dataset & Method & Encode median & Encode range & Decode median & Decode range \\', r'\midrule\endfirsthead',
         r'\caption[]{(continued)}\\', r'\toprule', r'Dataset & Method & Encode median & Encode range & Decode median & Decode range \\', r'\midrule\endhead']
    ds = list(T['delog']['per_dataset'].keys())
    for d in ds:
        for k, lab in order:
            best = {}
            for r in rows:
                if r['dataset'] == d and r['method'] == k and r['status'] == 'PASS' and r.get('contaminated') is False:
                    best[r['repeat']] = r
            obs = list(best.values())
            if len(obs) != 3: continue
            en = sorted(o['encode_MB_per_s'] for o in obs); de = sorted(o['decode_MB_per_s'] for o in obs)
            L.append(f"{d if k == 'semzip' else ''} & {lab} & {en[1]:.2f} & {en[0]:.2f}--{en[2]:.2f} & {de[1]:.2f} & {de[0]:.2f}--{de[2]:.2f} " + r'\\')
        L.append(r'\midrule')
    L[-1] = r'\bottomrule'; L.append(r'\end{longtable}')
    retried = sum(1 for r in rows if r.get('contaminated'))
    (OUT/'timing_detailed_table.tex').write_text('\n'.join(L) + '\n' + f'{retried} attempts were repeated because of concurrent load; no clean observation was discarded.\n')
