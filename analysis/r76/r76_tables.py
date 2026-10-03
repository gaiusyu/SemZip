"""R76 manuscript inputs: extended external comparison, attribution ladder, repeats, representation control, library,
safety. Every value comes from executed, SHA-verified records (fetched by ../fetch.sh); missing cells print as '--'
and are reported in r76_numbers.json['missing'] so no table silently drops a method.
Usage: python3 r76_tables.py  -> paper_out/*.tex + paper_out/r76_numbers.json"""
import json, statistics as st
from pathlib import Path
H = Path(__file__).resolve().parent; R = H.parent; ROOT = R.parent; Q = ROOT/'r73_quality_gate_20260927'
DEV = R/'dev_results'/'r76'; OUT = H/'paper_out'; OUT.mkdir(exist_ok=True)
DS = ['Android','Apache','BGL','Hadoop','HDFS','HealthApp','HPC','Linux','Mac','OpenSSH','OpenStack','Proxifier','Spark','Thunderbird','Windows','Zookeeper']
ONE = {'Apache','Linux','Proxifier','Zookeeper'}; MULTI = [d for d in DS if d not in ONE]
LARGE = ['HDFS','Spark','Windows','Thunderbird']
missing = []
def jl(p): return json.loads(Path(p).read_text())
A = jl(ROOT/'artifact_r71_working/results/final/anonymous_results.json')
size = {}
for r in A['size_rows']: size.setdefault(r['method'], {})[r['dataset']] = r
SUF = jl(ROOT/'artifact_r71_working/results/suffix/suffix_comparison.json')
suf = {}
for r in SUF['rows']:
    if r.get('status') == 'PASS': suf.setdefault(r['method'], {})[r['dataset']] = r
REF = {r['dataset']: r for r in jl(ROOT/'r71_strict_main_20260923/reference.json')} if (ROOT/'r71_strict_main_20260923/reference.json').exists() else {}

# ---------- per-method cell loaders: {dataset: {'raw','arch','suf_raw','suf_arch'}} (byte-exact archive-only decode only) ----------
def cell(raw, arch, suf_raw=None, suf_arch=None, **kw): return dict(raw=raw, arch=arch, suf_raw=suf_raw, suf_arch=suf_arch, **kw)
M = {}
def formal_dir(root):
    o = {}
    for d in DS:
        p = Path(root)/d/'result.json'
        if not p.exists(): continue
        r = jl(p)
        if r.get('status') != 'PASS' or not r.get('independent_materialized_file_sha_pass'): continue
        o[d] = cell(r['encode']['raw_bytes'], r['encode']['archive_bytes'], r['heldout']['raw_bytes'] or None, r['heldout']['archive_bytes'] or None)
    return o
M['semzip'] = formal_dir(Q/'dev_results/runs/formal/pool')
M['gated'] = formal_dir(Q/'dev_results/runs/formal/qg1c0')
for d in ('HDFS', 'Thunderbird'):  # gate leaves the R71 program unchanged (results_md.py IDENT)
    x = size['semzip'][d]; M['gated'][d] = cell(x['raw_bytes'], x['archive_bytes'], x['heldout']['raw_bytes'], x['heldout']['archive_bytes'])
M['semzip1'] = {d: cell(x['raw_bytes'], x['archive_bytes'], (x.get('heldout') or {}).get('raw_bytes'), (x.get('heldout') or {}).get('archive_bytes')) for d, x in size['semzip'].items()}
E = jl(R/'dev_results/empty/empty_summary.json')
M['empty'] = {d: cell(E[d]['raw_bytes'], E[d]['archive_bytes'], E[d]['heldout']['raw_bytes'] or None, E[d]['heldout']['archive_bytes'] or None) for d in DS if E[d]['sha_pass']}
M['library'] = formal_dir(DEV/'library/runs/formal/lib')
def b0(p):
    af = jl(p)['archive_files']; return af['chunk_0.tar.xz']['bytes'] + af['semantic/block_00000.semantic.tar.xz']['bytes']
# post-hoc (change log 2026-09-30): empty-program floor decided on block 0 only; never changes SemZip (its block-0 payload
# is below the empty program's on all 16 files, asserted below)
B0 = {}
for d in DS:
    B0[d] = {'semzip': b0(Q/'dev_results/runs/formal/pool'/d/'result.json'), 'empty': b0(DEV/'attribution/runs/formal/empty'/d/'result.json')}
    lp = DEV/'library/runs/formal/lib'/d/'result.json'
    if lp.exists() and d in M['library']: B0[d]['library'] = b0(lp)
    assert B0[d]['semzip'] < B0[d]['empty'], d
M['library_floor'] = {d: (M['library'][d] if B0[d]['library'] < B0[d]['empty'] else M['empty'][d]) for d in DS if 'library' in B0[d]}
for m in ('delog', 'loglite', 'gzip6', 'xz6', 'zstd3'):
    M[m] = {d: cell(x['raw_bytes'], x['archive_bytes'], suf.get(m, {}).get(d, {}).get('suffix_raw_bytes'), suf.get(m, {}).get(d, {}).get('suffix_archive_bytes')) for d, x in size[m].items()}
def codec(name):
    o = {}
    for d in DS:
        ps = sorted((DEV/'codecs/full_20260929'/d/name).glob('trial_001/attempt_*/result.json'))
        rs = [jl(p) for p in ps]; rs = [r for r in rs if r.get('status') == 'PASS' and r.get('roundtrip') == 'byte-exact' and r.get('archive_only_decode')]
        if not rs: continue
        r = rs[-1]; bl = r['blocks']
        extra = r['archive_bytes'] - sum(b['archive_bytes'] for b in bl)  # dictionary counted once per file
        sraw = sum(b['raw_bytes'] for b in bl[1:]) if len(bl) > 1 else None
        sarch = (sum(b['archive_bytes'] for b in bl[1:]) + extra) if len(bl) > 1 else None
        o[d] = cell(r['raw_bytes'], r['archive_bytes'], sraw, sarch)
    return o
for c in ('xz9e', 'zstd19', 'zstd22long', 'zstd19dict', 'delog_generic'): M[c] = codec(c)
def lr():
    o, nat = {}, {}
    for d in DS:
        p = DEV/'baselines/logreducer/full'/d/'status.json'
        if not p.exists(): continue
        r = jl(p)
        if r.get('status') == 'PASS' and r.get('full_sha_match') and r.get('roundtrip') == 'byte-exact':
            o[d] = cell(r['raw_bytes'], r['archive_bytes'], r.get('suffix_raw_bytes'), r.get('suffix_archive_bytes'))
        if r.get('native_bytes'):
            nat[d] = dict(ratio=(r['raw_bytes']/r['native_bytes']) if r.get('status') == 'PASS' else None, exact=bool(r.get('native_full_sha_match')), status=r.get('status'), failed=r.get('blocks_sha_fail', 0), blocks=r.get('n_blocks'))
        elif r.get('status') not in (None, 'RUNNING'): nat[d] = dict(ratio=None, exact=False, status=r.get('status'))
    return o, nat
M['logreducer'], NAT_LR = lr()
def ls():
    o, nat = {}, {}
    for d in DS:
        p = DEV/'baselines/logshrink/full_official'/d/'result.json'
        if not p.exists(): continue
        r = jl(p); rp, nv = r['repaired'], r['native']
        if rp.get('lossless') and rp.get('full_sha256_match') and not r.get('failed_blocks') and not r.get('partial_first_blocks_only'):
            o[d] = cell(r['raw_bytes'], rp['archive_bytes'], nv.get('suffix_raw_bytes'), rp.get('suffix_archive_bytes'))
        nat[d] = dict(ratio=nv.get('ratio') if not r.get('failed_blocks') else None, exact=bool(nv.get('lossless')),
                      failed_blocks=len(r.get('failed_blocks') or []), blocks=r.get('blocks'))
    return o, nat
M['logshrink'], NAT_LS = ls()
UD_ALL = ['NASA', 'ClarkNet', 'USask', 'Calgary']
def lnx():
    """R76-H LogNexus (ISSTA 2026, = arXiv LogPrism): paper per-dataset thresholds, +R separator correction."""
    o, nat = {}, {}
    for d in DS + UD_ALL:
        ps = sorted((DEV/'baselines/lognexus/full_paper').glob(f'*/{d}/result.json'))
        if not ps: continue
        r = jl(ps[-1])
        if r.get('status') == 'PASS' and r.get('full_sha_match') and r.get('roundtrip') == 'byte-exact':
            o[d] = cell(r['raw_bytes'], r['archive_bytes'], r.get('suffix_raw_bytes') or None, r.get('suffix_archive_bytes') or None)
        nat[d] = dict(ratio=r.get('native_compression_ratio'), suffix=r.get('native_suffix_ratio'), exact=r.get('native_status') == 'PASS',
                      token_equal=r.get('native_blocks_token_equal'), blocks=r.get('n_blocks'), tau=r.get('tau'), status=r.get('status'),
                      fallback=r.get('residual_full_fallback_blocks'), native_bytes=r.get('native_bytes'), archive_bytes=r.get('archive_bytes'),
                      raw=r.get('raw_bytes'), dropped=r.get('residual_dropped_records'), patched=r.get('residual_patched_records'))
    return o, nat
LN_ALL, NAT_LN = lnx()
U_SEMZIP = {d: jl(DEV/'unseen/summary_all.json')['datasets'][d]['ratio'] for d in UD_ALL} if (DEV/'unseen/summary_all.json').exists() else {}
M['lognexus'] = {d: c for d, c in LN_ALL.items() if d in DS}
LN_PAPER_MEAN = 88.202  # artifact/reference/paper_aggregate_results.csv (RQ1, dataset-specific thresholds)
DENUM = {}
for d in DS:
    ps = sorted((DEV/'baselines/denum/full_enconly'/d/'denum').glob('trial_001/attempt_*/result.json'))
    if ps:
        r = jl(ps[-1])
        if r.get('status') == 'ENCODE_ONLY_UNVERIFIED' and r.get('raw_bytes') == size['delog'][d]['raw_bytes']:
            DENUM[d] = r['raw_bytes']/r['archive_bytes']
LABEL = {'semzip': r'\tool', 'delog': 'DeLog', 'lognexus': 'LogNexus+R*', 'logshrink': 'LogShrink+R*', 'logreducer': 'LogReducer+R*', 'loglite': 'LogLite-BL*',
         'gzip6': 'gzip6', 'xz6': 'XZ6', 'xz9e': 'XZ9e', 'zstd19': 'Zstd19', 'zstd3': 'Zstd3', 'zstd22long': 'Zstd22L', 'zstd19dict': 'Zstd19+D',
         'empty': 'Empty', 'library': 'Library', 'library_floor': 'Lib.+floor', 'semzip1': 'SemZip-1', 'gated': 'Gated', 'delog_generic': 'DeLog-gen'}
# raw-identity check across every loaded cell
for m, rows in M.items():
    for d, c in rows.items():
        assert c['raw'] == size['delog'][d]['raw_bytes'], (m, d)
def ratio(m, d): c = M[m].get(d); return c['raw']/c['arch'] if c else None
def sratio(m, d): c = M[m].get(d); return (c['suf_raw']/c['suf_arch']) if (c and c['suf_arch']) else None
def agg(m):
    have = [d for d in DS if d in M[m]]
    a = {'n': len(have)}
    if len(have) == len(DS):
        rs = [ratio(m, d) for d in DS]
        a.update(mean=st.mean(rs), geomean=st.geometric_mean(rs), corpus=sum(M[m][d]['raw'] for d in DS)/sum(M[m][d]['arch'] for d in DS),
                 total=sum(M[m][d]['arch'] for d in DS))
    sm = [d for d in MULTI if sratio(m, d)]
    if len(sm) == len(MULTI):
        a.update(suffix_mean=st.mean(sratio(m, d) for d in MULTI), suffix_geomean=st.geometric_mean([sratio(m, d) for d in MULTI]))
    return a
N = {'aggregate': {m: agg(m) for m in M}}
dl_total = N['aggregate']['delog']['total']
for m in M:
    a = N['aggregate'][m]
    if 'total' in a: a['total_vs_delog_pct'] = 100*(a['total']/dl_total - 1)
    a['semzip_smaller_on'] = sum(M['semzip'][d]['arch'] < M[m][d]['arch'] for d in DS if d in M[m]) if m != 'semzip' else None
    a['semzip_suffix_smaller_on'] = sum(M['semzip'][d]['suf_arch'] < M[m][d]['suf_arch'] for d in MULTI if d in M[m] and M[m][d]['suf_arch']) if m != 'semzip' else None
    if len([d for d in DS if d in M[m]]) < len(DS): missing.append(f"{m}: {sorted(set(DS) - set(M[m]))}")

def fmt(v, nd=2): return '--' if v is None else f'{v:.{nd}f}'
def bold_row(vals):
    best = max(v for v in vals if v is not None)
    return [('\\textbf{' + fmt(v) + '}') if (v is not None and abs(v - best) < 1e-9) else fmt(v) for v in vals]

# ---------- Table: extended external comparison (main) ----------
MAIN = ['semzip', 'delog', 'lognexus', 'logshrink', 'logreducer', 'loglite', 'gzip6', 'xz9e', 'zstd19']
L = [r'\begin{table*}[t]\centering\footnotesize',
     r'\caption{Complete original-file compression ratios (raw / all archive bytes, every cell decoded from its archive alone with a matching SHA-256). '
     r'\tool\ is the frozen deployment trained on block 0. Bold marks the best in the row. *Adapted method. LogNexus, LogShrink, and LogReducer as released do not restore their input byte-exactly (LogNexus guarantees the whitespace-separated token sequence), so +R stores a counted correction (supplement). LogNexus uses its paper\textquoteright s per-dataset thresholds, and LogLite-BL uses a disclosed fixed adaptation. '
     r'Denum is excluded because its released format cannot restore its input (supplement), and LogFold has no public implementation. XZ6 and Zstd3 are in the supplement. $\dagger$ marks one block, in-sample. Suffix covers blocks 1 onward of the 12 multi-block files. A dash means the tool failed on at least one block (supplement).}',
     r'\label{tab:external-ratios}', r'\setlength{\tabcolsep}{2.7pt}', r'\begin{tabular}{l' + 'r'*len(MAIN) + '}', r'\toprule',
     'Dataset & ' + ' & '.join(LABEL[m] for m in MAIN) + r' \\', r'\midrule']
for d in DS:
    L.append((d + (r'$^\dagger$' if d in ONE else '')) + ' & ' + ' & '.join(bold_row([ratio(m, d) for m in MAIN])) + r' \\')
L.append(r'\midrule')
for key, name in (('mean', 'Mean'), ('geomean', 'Geomean'), ('corpus', 'Corpus'), ('suffix_mean', 'Suffix mean'), ('suffix_geomean', 'Suffix geom.')):
    L.append(name + ' & ' + ' & '.join(bold_row([N['aggregate'][m].get(key) for m in MAIN])) + r' \\')
L.append(r'\tool\ smaller & & ' + ' & '.join(f"{N['aggregate'][m]['semzip_smaller_on']}/{len([d for d in DS if d in M[m]])}" for m in MAIN[1:]) + r' \\')
L += [r'\bottomrule', r'\end{tabular}\end{table*}']
(OUT/'external_ratio_table.tex').write_text('\n'.join(L) + '\n')

# ---------- Table: attribution ladder (RQ2) ----------
RQ2 = jl(DEV/'rq2/RESULTS.json'); rq2 = {r['dataset']: r for r in RQ2}
NOPROG = {d for d, r in rq2.items() if r['surface_bytes'] == r['latent_bytes']}
LAD = ['empty', 'library', 'library_floor', 'semzip1', 'gated', 'semzip', 'delog_generic', 'delog']
L = [r'\begin{table*}[t]\centering\small',
     r'\caption{Attribution on complete files (ratios, archive-only decoding with matching SHA-256 for every cell). Empty is the same pipeline and backend with no program (residual processing only). Library is a frozen, hand-written, dataset-agnostic program library (timestamps, IPv4, sizes, percentages, hex, decimals) through the same gate, storage fitting, and runtime, with no LLM. In Lib.+floor (post hoc), the empty program replaces the library plan when it is smaller on block 0, a rule that leaves \tool\ unchanged. SemZip-1 is one greedy synthesis. Gated is SemZip-1 after the quality gate. \tool\ uses five syntheses and cost-guided selection. DeLog-gen is official DeLog without its per-dataset regular expressions. Latent is the archive saving of latent over literal storage of exactly the same matched spans (matched replay). s marks 20 systematically sampled blocks. n/a means every deployed program stores its spans as exact strings, so both branches coincide.}',
     r'\label{tab:attribution}', r'\setlength{\tabcolsep}{3.0pt}', r'\begin{tabular}{l' + 'r'*len(LAD) + 'r}', r'\toprule',
     'Dataset & ' + ' & '.join(LABEL[m] for m in LAD) + r' & Latent \\', r'\midrule']
for d in DS:
    r = rq2.get(d)
    lat = 'n/a' if d in NOPROG else (('--' if not r else f"{r['saving_pct']:.1f}\\%") + ('$^s$' if r and r['scope'] != 'complete' else ''))
    L.append((d + (r'$^\dagger$' if d in ONE else '')) + ' & ' + ' & '.join(fmt(ratio(m, d)) for m in LAD) + f' & {lat}' + r' \\')
L.append(r'\midrule')
for key, name in (('mean', 'Mean'), ('geomean', 'Geomean'), ('corpus', 'Corpus')):
    L.append(name + ' & ' + ' & '.join(fmt(N['aggregate'][m].get(key)) for m in LAD) + r' & \\')
L.append(r'Smaller than DeLog & ' + ' & '.join((f"{sum(M[m][d]['arch'] < M['delog'][d]['arch'] for d in DS if d in M[m])}/{len([d for d in DS if d in M[m]])}") for m in LAD[:-1]) + r' & -- & \\')
L += [r'\bottomrule', r'\end{tabular}\end{table*}']
(OUT/'attribution_table.tex').write_text('\n'.join(L) + '\n')
comp = [r for r in RQ2 if r['scope'] == 'complete' and r['dataset'] not in NOPROG]
N['rq2'] = {'complete_surface': sum(r['surface_bytes'] for r in RQ2 if r['scope'] == 'complete'), 'complete_latent': sum(r['latent_bytes'] for r in RQ2 if r['scope'] == 'complete'),
            'informative': sorted(set(rq2) - NOPROG), 'noprog': sorted(NOPROG),
            'per_dataset': {d: {'saving_pct': rq2[d]['saving_pct'], 'heldout_saving_pct': rq2[d].get('heldout_saving_pct'), 'scope': rq2[d]['scope']} for d in rq2},
            'min_informative': min((rq2[d]['saving_pct'], d) for d in rq2 if d not in NOPROG), 'max_informative': max((rq2[d]['saving_pct'], d) for d in rq2 if d not in NOPROG)}
N['rq2']['complete_saving_pct'] = 100*(1 - N['rq2']['complete_latent']/N['rq2']['complete_surface'])

# ---------- repeats (RQ3) ----------
REP = {}
for d in ('HPC', 'HDFS', 'Hadoop', 'Spark'):
    rows = [('r1', M['semzip'][d])]
    for rep in ('r2', 'r3'):
        p = DEV/'variance/runs'/d/rep/'runs/formal/pool'/d/'result.json'
        if p.exists():
            r = jl(p)
            if r['status'] == 'PASS' and r['independent_materialized_file_sha_pass']:
                rows.append((rep, cell(r['encode']['raw_bytes'], r['encode']['archive_bytes'], r['heldout']['raw_bytes'], r['heldout']['archive_bytes'])))
        else: missing.append(f'variance {d} {rep}')
    dl, dls = M['delog'][d]['arch'], M['delog'][d]['suf_arch']
    REP[d] = {'ratios': [c['raw']/c['arch'] for _, c in rows], 'suffix_ratios': [c['suf_raw']/c['suf_arch'] for _, c in rows],
              'vs_delog_pct': [100*(c['arch']/dl - 1) for _, c in rows], 'suffix_vs_delog_pct': [100*(c['suf_arch']/dls - 1) for _, c in rows],
              'wins': sum(c['arch'] < dl for _, c in rows), 'suffix_wins': sum(c['suf_arch'] < dls for _, c in rows), 'n': len(rows),
              'delog_ratio': ratio('delog', d)}
N['repeats'] = REP
L = [r'\begin{table}[t]\centering\small', r'\caption{Three complete pipeline runs (five fresh syntheses, gate, selection, storage fitting, complete-file encoding and archive-only decoding) on the four files where \tool\ led DeLog by 2--6\%. Run 1 is the reported deployment. Bytes vs.\ DeLog lists complete file / suffix.}',
     r'\label{tab:repeats}', r'\setlength{\tabcolsep}{4pt}', r'\begin{tabular}{lrrrrr}', r'\toprule', r'Dataset & Run 1 & Run 2 & Run 3 & DeLog & Bytes vs.\ DeLog \\', r'\midrule']
for d, x in REP.items():
    rs = x['ratios'] + [None]*(3 - len(x['ratios']))
    rng = f"{min(x['vs_delog_pct']):+.1f} to {max(x['vs_delog_pct']):+.1f}\\% / {min(x['suffix_vs_delog_pct']):+.1f} to {max(x['suffix_vs_delog_pct']):+.1f}\\%".replace('-', '$-$')
    L.append(f"{d} & " + ' & '.join(fmt(v) for v in rs) + f" & {fmt(x['delog_ratio'])} & {rng}" + r' \\')
L += [r'\bottomrule', r'\end{tabular}\end{table}']
(OUT/'repeats_table.tex').write_text('\n'.join(L) + '\n')

# ---------- supplement: other codec settings and non-lossless natives ----------
SUP = ['xz6', 'zstd3', 'zstd22long', 'zstd19dict', 'delog_generic']
L = [r'\setlength{\tabcolsep}{3pt}', r'\begin{longtable}{l' + 'r'*(len(SUP) + 5) + '}', r'\caption{Supplementary external measurements. The left part shows further general-purpose settings (Zstd19+D uses a dictionary trained on block 0 and counted once per file) and DeLog without its dataset-specific regular expressions, all byte-exact from archives alone. Zstd22L is zstd --ultra -22 --long=27. The right part shows compression-only ratios of LogShrink and LogReducer as released (not byte-exact, with exact files marked e), the number of LogShrink blocks that failed to encode or decode, and Denum\textquoteright s compression-only ratio (its format cannot restore the input, so it is not losslessly verifiable).}\label{tab:supp-external}\\',
     r'\toprule', 'Dataset & ' + ' & '.join(LABEL[m] for m in SUP) + r' & LS native & LS failed & LR native & LR status & Denum \\', r'\midrule\endfirsthead', r'\toprule', 'Dataset & ' + ' & '.join(LABEL[m] for m in SUP) + r' & LS native & LS failed & LR native & LR status & Denum \\', r'\midrule\endhead']
for d in DS:
    ls_, lr_ = NAT_LS.get(d, {}), NAT_LR.get(d, {})
    L.append(d + ' & ' + ' & '.join(fmt(ratio(m, d)) for m in SUP) + ' & ' +
             (fmt(ls_.get('ratio')) + ('$^e$' if ls_.get('exact') else '') if ls_ else '--') + ' & ' + (str(ls_.get('failed_blocks', '--')) if ls_ else '--') + ' & ' +
             (fmt(lr_.get('ratio')) + ('$^e$' if lr_.get('exact') else '') if lr_ else '--') + ' & ' + (lr_.get('status') or '--') + ' & ' + fmt(DENUM.get(d)) + r' \\')
L += [r'\bottomrule', r'\end{longtable}']
(OUT/'supp_external.tex').write_text('\n'.join(L) + '\n')
N['native'] = {'logshrink': NAT_LS, 'logreducer': NAT_LR}
N['missing'] = missing
N['per_dataset'] = {d: {m: ratio(m, d) for m in M} for d in DS}
N['suffix_per_dataset'] = {d: {m: sratio(m, d) for m in M} for d in MULTI}
(OUT/'r76_numbers.json').write_text(json.dumps(N, indent=1, default=str))
print('missing:', *missing, sep='\n  ')
for m in M:
    a = N['aggregate'][m]
    print(f"{m:14s} n={a['n']:2d} " + ' '.join(f"{k}={v:.2f}" for k, v in a.items() if isinstance(v, float)) + f"  semzip_smaller={a.get('semzip_smaller_on')}")
print('RQ2 complete saving %.2f%%' % N['rq2']['complete_saving_pct'], 'noprog', N['rq2']['noprog'])
for d, x in REP.items(): print(d, [round(v, 2) for v in x['ratios']], 'wins', x['wins'], '/', x['n'], 'suffix wins', x['suffix_wins'])

# ======================= generated prose (numbers only from N) =======================
def x(v, nd=2): return f'{v:.{nd}f}$\\times$'
def pct(v, nd=2): return (f'{v:+.{nd}f}\\%').replace('-', '$-$')
ag = N['aggregate']; S = ag['semzip']; DL = ag['delog']
TB_share = 100*M['semzip']['Thunderbird']['raw']/sum(M['semzip'][d]['raw'] for d in DS)
wins_dl = ag['delog']['semzip_smaller_on']; swins_dl = ag['delog']['semzip_suffix_smaller_on']
lossy_done = {m: sorted(M[m]) for m in ('logshrink', 'logreducer')}
def beat_all(m): return all(M['semzip'][d]['arch'] < M[m][d]['arch'] for d in M[m])
ls_fail = {d: v for d, v in NAT_LS.items() if v.get('failed_blocks')}
lr_fail = {d: v for d, v in NAT_LR.items() if v.get('status') == 'FAIL'}
def names(ds):
    ds = list(ds)
    return ds[0] if len(ds) == 1 else (', '.join(ds[:-1]) + ' and ' + ds[-1] if ds else 'none')
PUBDL = {'Android': 30.354, 'Apache': 59.648, 'BGL': 45.682, 'Hadoop': 79.626, 'HDFS': 29.013, 'HealthApp': 55.934, 'HPC': 46.842, 'Linux': 30.626, 'Mac': 43.687, 'OpenSSH': 106.012,
         'OpenStack': 23.754, 'Proxifier': 30.784, 'Spark': 61.788, 'Thunderbird': 68.647, 'Windows': 541.569, 'Zookeeper': 154.926}  # DeLog paper (arXiv 2601.15084v2), Table 5
pubdiff = [100*(PUBDL[d]/ratio('delog', d) - 1) for d in DS]
pub_w = sum(ratio('semzip', d) > PUBDL[d] for d in DS)
pub_g = 100*(st.geometric_mean([ratio('semzip', d) for d in DS])/st.geometric_mean(list(PUBDL.values())) - 1)
pub_sum = 100*(sum(M['semzip'][d]['arch'] for d in DS)/sum(M['semzip'][d]['raw']/PUBDL[d] for d in DS) - 1)
N['delog_published'] = {'diff_min': min(pubdiff), 'diff_max': max(pubdiff), 'mean_pub': st.mean(PUBDL.values()), 'semzip_wins': pub_w, 'geomean_pct': pub_g, 'summed_pct': pub_sum}
t = []
t.append(f"We first ask how \\tool\\ compares with existing compressors. On complete files (Table~\\ref{{tab:external-ratios}}), it has the largest ratio on {sum(1 for d in DS if max((ratio(m, d) or 0) for m in MAIN) == ratio('semzip', d))}/16 files. "
         f"Against DeLog, the strongest baseline by geometric mean, it is smaller on {wins_dl}/16 files and on {swins_dl}/12 held-out suffixes, which no selection or fitting step reads. "
         f"Its geometric-mean ratio is {x(S['geomean'])} versus {x(DL['geomean'])} ({x(S['suffix_geomean'])} versus {x(DL['suffix_geomean'])} on suffixes), and its arithmetic mean is {x(S['mean'])} versus {x(DL['mean'])}. "
         f"However, its summed bytes are {abs(S['total_vs_delog_pct']):.2f}\\% larger than DeLog's because of Thunderbird. This file holds {TB_share:.1f}\\% of all raw bytes and is the one file where DeLog wins "
         f"({x(ratio('semzip', 'Thunderbird'))} versus {x(ratio('delog', 'Thunderbird'))})."
         + (f" The same file makes \\tool's summed bytes {100*(S['total']/ag['lognexus']['total'] - 1):.2f}\\% larger than LogNexus+R's, whose Thunderbird archive is {100*(1 - M['lognexus']['Thunderbird']['arch']/M['semzip']['Thunderbird']['arch']):.1f}\\% smaller." if 'total' in ag.get('lognexus', {}) and ag['lognexus']['total'] < S['total'] else ''))
dp = N['delog_published']
t.append(f"DeLog's released implementation, run as its own benchmark script does, reproduces our DeLog numbers exactly on Linux and BGL, but its paper reports ratios {abs(dp['diff_min']):.1f}\\% lower to {dp['diff_max']:.1f}\\% higher per file (mean {dp['mean_pub']:.2f}$\\times$). Against these published ratios, \\tool\\ has the higher ratio on {dp['semzip_wins']}/16 files (geometric mean {pct(dp['geomean_pct'], 1)}) and {dp['summed_pct']:.1f}\\% larger summed bytes (supplement).")
LN_DS = [d for d in DS if d in NAT_LN and NAT_LN[d].get('ratio')]
LN_SENT = ''
if LN_DS:
    ln_nat_mean = st.mean(NAT_LN[d]['ratio'] for d in LN_DS) if len(LN_DS) == len(DS) else None
    ln_tok_all = all(NAT_LN[d]['token_equal'] == NAT_LN[d]['blocks'] for d in LN_DS)
    ln_exact = [d for d in LN_DS if NAT_LN[d]['exact']]
    ln_beat_nat = sum(M['semzip'][d]['arch'] < NAT_LN[d]['native_bytes'] for d in LN_DS)
    ln_r = [d for d in DS if d in M['lognexus']]
    N['lognexus'] = {'native_mean': ln_nat_mean, 'paper_mean': LN_PAPER_MEAN, 'token_all': ln_tok_all, 'exact_files': ln_exact,
                     'semzip_smaller_than_native': ln_beat_nat, 'n_native': len(LN_DS),
                     'semzip_smaller_than_r': sum(M['semzip'][d]['arch'] < M['lognexus'][d]['arch'] for d in ln_r), 'n_r': len(ln_r),
                     'losses_r': [d for d in ln_r if M['semzip'][d]['arch'] >= M['lognexus'][d]['arch']],
                     'losses_native': [d for d in LN_DS if M['semzip'][d]['arch'] >= NAT_LN[d]['native_bytes']]}
    nl = N['lognexus']
    LN_SENT = (f"LogNexus~\\cite{{lognexus}} restores the whitespace-separated token sequence rather than the bytes. Run as released with its paper\\textquoteright s per-dataset thresholds, "
               + (f"it averages {x(ln_nat_mean)} on our inputs, and its paper reports {x(LN_PAPER_MEAN)}. " if ln_nat_mean else '')
               + ("Every block passes its token check, " if ln_tok_all else "Some blocks fail its token check, ")
               + ((f"but only {names(ln_exact)} {'is' if len(ln_exact) == 1 else 'are'} restored byte-exactly. ") if ln_exact else "and no file is restored byte-exactly. ")
               + f"\\tool\\ is smaller than LogNexus+R on {nl['semzip_smaller_than_r']}/{nl['n_r']} files"
               + (f" (not on {names(nl['losses_r'])})" if nl['losses_r'] else '')
               + f" and smaller than even its uncorrected archives on {ln_beat_nat}/{len(LN_DS)}"
               + (f" (not on {names(nl['losses_native'])})" if nl['losses_native'] else '') + ". ")
t.append(f"LogShrink and LogReducer as released do not restore most files byte-exactly because they drop carriage returns, spaces, and leading zeros. Their +R variants therefore store a counted per-line correction. "
         f"\\tool\\ is smaller than LogShrink+R on {ag['logshrink']['semzip_smaller_on']}/{ag['logshrink']['n']} and than LogReducer+R on {ag['logreducer']['semzip_smaller_on']}/{ag['logreducer']['n']} reconstructed files"
         + (f". LogShrink could not encode or decode every block of {names(sorted(ls_fail))}" if ls_fail else '')
         + (f", and LogReducer+R failed the SHA check on part of {names(sorted(lr_fail))}" if lr_fail else '') + ". "
         + LN_SENT +
         f"High-effort general-purpose settings remain far behind, with XZ9e averaging {x(ag['xz9e']['mean'])} and Zstd19 {x(ag['zstd19']['mean'])}. The supplement reports a block-0 Zstd dictionary and the Zstd ultra setting.")
(OUT/'rq1_text.tex').write_text('\n'.join(t) + '\n')

e, sz1, g, lf = ag['empty'], ag['semzip1'], ag['gated'], ag['library_floor']
dlw = lambda m: sum(M[m][d]['arch'] < M['delog'][d]['arch'] for d in DS if d in M[m])
lib_below_empty = [d for d in M['library'] if M['library'][d]['arch'] > M['empty'][d]['arch']]
lib_improves = [d for d in M['library_floor'] if M['library_floor'][d]['arch'] < M['empty'][d]['arch']]
t = []
t.append(f"Table~\\ref{{tab:attribution}} separates the contributions. With the same pipeline, backend, storage fitting, and runtime but no program, the empty program averages {x(e['mean'])} (geometric mean {x(e['geomean'])}) and is smaller than DeLog on only {dlw('empty')}/16 files. "
         f"The deployed programs reduce summed archive bytes by {100*(1-S['total']/e['total']):.1f}\\% and account for {sum(1 for d in DS if M['semzip'][d]['arch'] < M['delog'][d]['arch'] and not M['empty'][d]['arch'] < M['delog'][d]['arch'])} of \\tool's {wins_dl} wins over DeLog. "
         f"One greedy synthesis (SemZip-1) reaches {x(sz1['mean'])} but is larger than the empty program on {names([d for d in DS if M['semzip1'][d]['arch'] > M['empty'][d]['arch']])}. The quality gate removes such rules ({x(g['mean'])}, {dlw('gated')}/16 files smaller than DeLog), and selection across five syntheses adds the rest ({x(S['mean'])}, {dlw('semzip')}/16).")
t.append(f"We also pass a frozen, hand-written library of common renderers (timestamps, IPv4, sizes, percentages, hexadecimal and decimal numbers) through the same gate and runtime. It does not reproduce this gain and is larger than the empty program on {len(lib_below_empty)}/{len(M['library'])} files. "
         f"The gate removes one rule group at a time and never compares against the empty program. We then add a post-hoc empty-program floor decided on block 0, which leaves \\tool\\ unchanged. The floored library improves on the empty program on {len(lib_improves)} files ({', '.join(lib_improves)}) but keeps the larger library plan on BGL. The equally single-candidate Gated plan is smaller than it on {sum(M['gated'][d]['arch'] < M['library_floor'][d]['arch'] for d in M['library_floor'])}/{len(M['library_floor'])} files. "
         f"Official DeLog without its per-dataset regular expressions (DeLog-gen) is smaller than \\tool\\ on {sum(M['delog_generic'][d]['arch'] < M['semzip'][d]['arch'] for d in M['delog_generic'])}/{len(M['delog_generic'])} files.")
r2 = N['rq2']
t.append(f"\\paragraph{{Where programs do not help.}} Thunderbird is the one LogHub file where a byte-exact baseline is smaller (RQ1). Its lines carry one instant as epoch seconds, a date, and a syslog clock. LogNexus ships hand-written expressions for these fields, and DeLog falls to 60.34$\\times$ without its own. However, the deployed program parses none of them into a value and is only 0.3\\% smaller than the empty program. The exploratory update with timestamp rules (RQ5) still stays below LogNexus+R's 79.27$\\times$. Where programs store no latent value, as on HDFS, HPC, and Thunderbird, the residual backend decides. Because programs are inverted after that backend decodes, they could precede another log compressor. We did not test it, and gains need not add up where rules overlap.\n\n"
         f"The matched replay (last column) stores exactly the same matched spans either literally or as latent values, so it separates representation from field separation. Latent storage saves {min(v['saving_pct'] for d, v in r2['per_dataset'].items() if d not in r2['noprog'] and v['saving_pct'] > 0):.1f}--{r2['max_informative'][0]:.1f}\\% on {sum(1 for d, v in r2['per_dataset'].items() if d not in r2['noprog'] and v['saving_pct'] > 0)} of the {len(r2['informative'])} files where the two forms differ, "
         f"and on Windows' sampled blocks the latent form is {abs(r2['per_dataset']['Windows']['saving_pct']):.2f}\\% larger. Over the {sum(1 for r in RQ2 if r['scope'] == 'complete')} fully replayed files, literal storage of the separated spans is {100*(1 - N['rq2']['complete_surface']/sum(M['empty'][r['dataset']]['arch'] for r in RQ2 if r['scope'] == 'complete')):.1f}\\% smaller than the empty program, and latent storage saves a further {N['rq2']['complete_saving_pct']:.2f}\\%. Thus, after a strong backend, field separation carries most of the gain, and the 3.67--14.03$\\times$ contraction of \\secref{{sec:motivation}} survives only as a smaller, file-dependent share.")
(OUT/'rq2_text.tex').write_text('\n'.join(t) + '\n')

rp = N['repeats']
allw = all(v['wins'] == v['n'] and v['suffix_wins'] == v['n'] for v in rp.values())
spread = max(100*(max(v['ratios'])/min(v['ratios'])-1) for v in rp.values())
t = [f"To test whether the narrow wins survive a fresh draw of the whole pipeline, we rerun it twice more on the four files where \\tool\\ led DeLog by 2--6\\% (Table~\\ref{{tab:repeats}}). Each rerun uses five new syntheses, gate, selection, storage fitting, and complete-file encoding. "
     + (f"All {sum(v['n'] for v in rp.values())} runs are smaller than DeLog on both complete file and suffix" if allw else f"{sum(v['wins'] for v in rp.values())}/{sum(v['n'] for v in rp.values())} runs are smaller than DeLog on the complete file")
     + f", and complete-file ratios differ across runs by at most {spread:.1f}\\%. We did not rerun Thunderbird, which decides summed bytes. Selection thus reproduces these four comparisons, although individual syntheses differ widely."]
(OUT/'rq3_repeats_text.tex').write_text('\n'.join(t) + '\n')
print('texts written')

# ---------- byte-weighted throughput from the R73 formal session (used by abstract and RQ4) ----------
TS = jl(Q/'dev_results/runs/timing/SUMMARY.json')
TWELVE = [d for d in DS if d not in LARGE]
def bw(m, key):
    per = TS[m]['per_dataset']; b = {d: M['semzip'][d]['raw'] for d in TWELVE}
    return sum(b.values())/1e6 / sum(b[d]/1e6/per[d][key] for d in TWELVE)
N['throughput_bw'] = {m: {'encode': bw(m, 'encode_MB_per_s'), 'decode': bw(m, 'decode_MB_per_s')} for m in ('semzip', 'delog', 'xz6')}
tb = N['throughput_bw']

# ---------- abstract / conclusion numbers ----------
OTHERS = [(m, n) for m, n in (('logshrink', 'LogShrink+R'), ('logreducer', 'LogReducer+R'), ('loglite', 'LogLite-BL'), ('xz9e', 'XZ9e')) if M[m]]
beaten = [n for m, n in OTHERS if beat_all(m)]
ab = (f"On sixteen LogHub files, each decoded byte-exactly from its archive alone, \\tool\\ is smaller than DeLog on {swins_dl} of 12 held-out suffixes (geometric-mean ratio {S['suffix_geomean']:.1f}$\\times$ versus {DL['suffix_geomean']:.1f}$\\times$) and {wins_dl} of 16 complete files, "
      + (f"and smaller than {names(beaten)} (+R: released tools plus a counted correction) on every file they restore. " if len(beaten) == len(OTHERS) else f"and smaller than the other log compressors on most files they restore. ")
      + f"summed bytes are {S['total_vs_delog_pct']:.2f}\\% larger than DeLog's because \\tool\\ loses on the largest file. "
      f"Without its programs the same pipeline reaches a geometric mean of {e['geomean']:.1f}$\\times$ instead of {S['geomean']:.1f}$\\times$. Field separation provides most of this gain and latent values a smaller, file-dependent share. "
      + ("Two further complete runs keep all four narrow wins over DeLog. " if allw else "")
      + "ENCODE_SPEED_PLACEHOLDER")
(OUT/'abstract_results.tex').write_text(ab + '\n')
ALLCMP = [m for m in ('lognexus', 'logshrink', 'logreducer', 'loglite', 'gzip6', 'xz6', 'xz9e', 'zstd3', 'zstd19', 'zstd22long', 'zstd19dict', 'delog_generic') if M[m]]
allbeat = all(beat_all(m) for m in ALLCMP)
LNC = N.get('lognexus', {})
co = (f"It is smaller than DeLog on {wins_dl}/16 LogHub files and {swins_dl}/12 held-out suffixes"
      + (f" and than LogNexus+R on {LNC['semzip_smaller_than_r']}/{LNC['n_r']} files" if LNC.get('n_r') else '')
      + (" and, on LogHub, smaller than every other evaluated compressor on every file that compressor restores" if allbeat else "") + f". Because of Thunderbird, its summed bytes are {abs(S['total_vs_delog_pct']):.2f}\\% larger than DeLog's"
      + (f" and {100*(S['total']/ag['lognexus']['total'] - 1):.2f}\\% larger than LogNexus+R's" if 'total' in ag.get('lognexus', {}) and ag['lognexus']['total'] < S['total'] else '')
      + ". Its encoder is also slower. "
      f"Without its programs, the same pipeline's geometric mean is {e['geomean']:.2f}$\\times$, not {S['geomean']:.2f}$\\times$, and a hand-written library does not substitute for them.")
(OUT/'conclusion_results.tex').write_text(co + '\n')
N['missing'] = missing
(OUT/'r76_numbers.json').write_text(json.dumps(N, indent=1, default=str))
print('abstract/conclusion/cost written; missing now:', missing)

# ======================= supplement section (R76) =======================
def esc(s): return s.replace('\\', r'\textbackslash{}').replace('_', r'\_').replace('%', r'\%').replace('&', r'\&').replace('#', r'\#').replace('$', r'\$').replace('{', r'\{').replace('}', r'\}').replace('~', r'\textasciitilde{}').replace('^', r'\^{}')
U = [r'\section{Additional baselines and controls}\label{sec:r76}\sloppy',
     r'All experiments in this section were specified in a design document written before any of their results existed. Later changes are appended to that document as dated change-log entries and are stated below. Every compression cell decodes from its archive alone and matches per-block and full-file SHA-256 digests of the original inputs.']
# --- log-specific baselines
U += [r'\subsection{Log-specific baselines}',
      r'The +R correction deviates from the pre-registered adaptation rule (recorded in the change log). It was recorded before any new-baseline ratio comparison and was not applied to Denum. LogShrink (commit 59ce494) and LogReducer (commit 4000541) run their official pipelines independently on every 100,000-line block, including their own sampling and template training on that block (samplers seeded with 0). Their decoders read only the block archive, which contains the native payload, every model file the official restore reads, and, for the +R variants, a per-line correction. '
      r'As released, both tools drop carriage returns, strip leading and trailing whitespace, collapse runs of header spaces, and drop leading zeros. LogShrink also truncates integers wider than 32 bits (e.g., HDFS block identifiers). The correction is computed from the original block and the tool\textquoteright s own decoded output only, compressed with LZMA, and counted. '
      r'LogShrink\textquoteright s official decoder additionally needed five fixes that change no archive byte (empty failed-log files, column-file ordering, header-file ordering, whole-line template matching, and delimiter recovery). LogShrink uses its shipped per-dataset header lengths, while LogReducer uses its generic defaults. '
      + '. '.join([f"LogReducer+R failed the SHA check on {v['failed']} of {d}\\textquoteright s {v['blocks']} blocks" for d, v in sorted(NAT_LR.items()) if v.get('status') == 'FAIL'] + ['LogShrink could not encode or decode ' + ' and '.join(f"{v['failed_blocks']} of {d}\\textquoteright s {v['blocks']} blocks" for d, v in sorted(NAT_LS.items()) if v.get('failed_blocks'))] * any(v.get('failed_blocks') for v in NAT_LS.values())) + '. These files have no valid ratio for the affected tool. '
      r'The official DeLog build (commit 64a074f) and DeLog-gen differ only in the per-dataset regular-expression map (emptied in DeLog-gen). The decoders are identical.',
      r'\input{generated/supp_external.tex}']
DP = jl(DEV/'baselines/denum/proofs/SUMMARY.json')
def win(a, b):
    i = next(k for k, (u, v) in enumerate(zip(a, b)) if u != v); st_ = max(0, i - 25)
    return ('...' if st_ else '') + a[st_:i + 25].rstrip('\r')
ex = [r for r in DP if r['dataset'] in ('Apache', 'HDFS', 'BGL')]
U += [r'\paragraph{Denum.} The released Denum compressor (commit a3a6975) replaces each match of its per-dataset regular expressions by a tag and stores the concatenated digits as one integer, without group widths or separator positions. Its Python decoder also drops carriage returns and leading zeros. '
      f"For each of the sixteen datasets we changed one line of block 0 by moving a digit across a group boundary. In all {sum(r['all_archive_members_identical'] for r in DP)} cases the original and the modified block produce byte-identical archive members, so no decoder can restore both. Examples are "
      + (lambda xs: ', '.join(xs[:-1]) + ', and ' + xs[-1] if len(xs) > 1 else ''.join(xs))([r"\texttt{" + esc(win(r['original_line'], r['modified_line'])) + r"} vs.\ \texttt{" + esc(win(r['modified_line'], r['original_line'])) + '}' for r in ex]) + '. Denum is therefore excluded from the lossless comparison.']
PUB_ALL = jl(ROOT/'results/published_baseline_reference_20260915.json')
PUB = [p for p in PUB_ALL if p['method'] == 'LogFold']
PRISM = {r['dataset']: r['reported_ratio'] for p in PUB_ALL if p['method'] == 'LogPrism' for r in p['rows']}
if NAT_LN:
    U += [r'\paragraph{LogNexus (LogPrism as a preprint).} The LogPrism preprint (arXiv 2601.17482) was published at ISSTA 2026 as LogNexus by the same authors, with an artifact on Zenodo (record 21021398, and the later record 21281836 changes only documentation, which we verified file by file). '
          r'We built its released source unchanged (GCC 10 instead of the artifact\textquoteright s GCC 11 container) and ran its default serial configuration (\texttt{LogNexus\_compress <block> <dataset> 100000 1 1 1 <tau>}) independently on every 100,000-line block, with the per-dataset thresholds of its paper tables (0.02, its untuned default, for the four unseen sources, whose names select no built-in rules). Every file it writes is stored and counted, and its own decompressor reads only the block archive. '
          r'Its artifact claims, and we confirm, restoration of the whitespace-separated token sequence only. It collapses whitespace runs and may omit blank lines. LogNexus+R adds a counted, dataset-agnostic, LZMA-compressed correction that holds the omitted blank lines with their positions, and for every other differing line the separators that differ when the token lists agree (the LogReducer+R line patch otherwise). '
          r'This correction is cheaper for LogNexus than the line patch of the other +R variants. It was fixed after a probe on Linux only, before any other LogNexus result (change log). Reported ratios in the LogPrism preprint are listed for reference' + (f' (mean {st.mean(PRISM[d] for d in DS):.2f}$\\times$ on the sixteen LogHub files). The accepted paper\\textquoteright s mean of {LN_PAPER_MEAN:.2f}$\\times$ quoted in the main text is from its artifact (README and reference/paper\\_aggregate\\_results.csv).' if all(d in PRISM for d in DS) else '.'),
          r'\begin{longtable}{lrrrrrrr}\caption{LogNexus as released (native, not byte-exact) and with the counted correction (+R, byte-exact from the archive alone), next to \tool. The Token column counts blocks whose restored token sequence equals the original. Corr.\ is correction bytes as a share of the +R archive. Preprint is the ratio reported in LogPrism v2 (inputs unverified).}\label{tab:lognexus}\\\toprule',
          r'Dataset & $\tau$ & Native & Token & +R & Corr. & Preprint & \tool\ \\\midrule']
    for d in DS + UD_ALL:
        v = NAT_LN.get(d)
        if not v: continue
        rr = (LN_ALL[d]['raw']/LN_ALL[d]['arch']) if d in LN_ALL else None
        corr = (100*(1 - v['native_bytes']/v['archive_bytes'])) if (v.get('native_bytes') and v.get('archive_bytes')) else None
        sz = ratio('semzip', d) if d in DS else U_SEMZIP.get(d)
        U.append(f"{d} & {v['tau']} & {fmt(v['ratio'])} & {v['token_equal']}/{v['blocks']} & {fmt(rr)} & {fmt(corr, 1)}\\% & {fmt(PRISM.get(d))} & {fmt(sz)}" + r' \\')
    U += [r'\bottomrule\end{longtable}']
U += [r'\paragraph{Published numbers without an implementation.} LogFold (ICSE 2026) provides no public implementation (checked on 2 October 2026, when its repository held only a license and README). Its paper reports the following ratios for 100,000-line chunks. Input identities are not verified and these numbers are not matched observations.',
      r'\begin{longtable}{l' + 'r'*(len(PUB) + 1) + r'}\caption{Published ratios (literature context only) next to \tool.}\label{tab:published}\\\toprule',
      'Dataset & ' + ' & '.join(p['method'] + ' (ICSE 2026, arXiv ' + p['source_version'] + ')' for p in PUB) + r' & \tool\ \\\midrule']
pubrows = {p['method']: {r['dataset']: r['reported_ratio'] for r in p['rows']} for p in PUB}
for d in DS:
    U.append(d + ' & ' + ' & '.join(fmt(pubrows[p['method']].get(d)) for p in PUB) + f" & {fmt(ratio('semzip', d))}" + r' \\')
U += [r'\bottomrule\end{longtable}']
N['logfold_published_above_semzip'] = [d for d in DS if pubrows['LogFold'].get(d, 0) > ratio('semzip', d)]
U += [r'\paragraph{Released DeLog implementation versus published ratios.} Running the released DeLog (commit 64a074f) on whole files with the command line of its own benchmark script (text mode, 100,000-line blocks, four threads, frequency threshold 0, LZMA, normal mode) gives 27.60$\times$ on Linux and 40.33$\times$ on BGL, identical to our block adapter, whereas the DeLog paper reports 30.63$\times$ and 45.68$\times$. All tables use the measured values. The published ratios are listed here.',
      r'\begin{longtable}{lrrrr}\caption{DeLog measured on our inputs versus published (literature only).}\label{tab:delog-published}\\\toprule', r'Dataset & Measured & Published & Difference & \tool\ \\\midrule']
for d in DS:
    U.append(f"{d} & {fmt(ratio('delog', d))} & {fmt(PUBDL[d])} & {pct(100*(PUBDL[d]/ratio('delog', d) - 1), 1)} & {fmt(ratio('semzip', d))}" + r' \\')
U += [r'\bottomrule\end{longtable}']
# --- library
LR_ = {r['dataset']: r for r in jl(DEV/'library/library_report.json')}
U += [r'\subsection{Program-library control}',
      r'The library proposer replaces only the LLM call of the trainer. Compilation, verification, the quality gate, storage fitting, the runtime, and archive-only decoding are unchanged, and it makes no API call. Its program types were frozen before any library result. They cover syslog, ISO-8601, \texttt{YYYY-MM-DD HH:MM:SS} (with optional milliseconds), \texttt{YY/MM/DD}, \texttt{YYYYMMDD HHMMSS}, clock-time, and ten-digit epoch timestamps, plus IPv4 and IPv4:port, hexadecimal numbers, sizes with B/KB/MB/GB units, percentages, and decimal integers and fixed-point numbers with width and leading zeros. A type is proposed for a sampled group when it matches one of the group\textquoteright s examples (at most eight per group). With one candidate per dataset, pooled selection reduces to the gated plan. '
      r'The gate removes one rule group at a time and only on a strict decrease, and never compares with the empty program. On HealthApp, where no library timestamp matches, fourteen generic integer rules leave block 0 3.4 times larger than the empty program. The post-hoc floor (not pre-registered) deploys the empty program when it is smaller on block 0. \tool\textquoteright s deployed plans are below the empty program on block 0 for all sixteen files.',
      r'\begin{longtable}{lrrrrr}\caption{Program-library control with block-0 payload bytes and complete-file ratios.}\label{tab:library}\\\toprule',
      r'Dataset & Library B0 & Empty B0 & Library & Lib.+floor & \tool\ \\\midrule']
for d in DS:
    b = B0.get(d, {}); r = LR_.get(d, {})
    U.append(f"{d} & {format(b['library'], ',') if isinstance(b.get('library'), int) else '--'} & {format(b['empty'], ',')} & {fmt(ratio('library', d))} & {fmt(ratio('library_floor', d))} & {fmt(ratio('semzip', d))}" + r' \\')
U += [r'\bottomrule\end{longtable}']
# --- matched replay
U += [r'\subsection{Matched replay on the deployed plans}',
      r'Both branches execute the deployed matcher once per block and share its spans, rule order, placeholders, residual archive (byte-identical), and semantic XZ preset. The surface branch stores each replaced literal with identity reconstruction and generic field storage and fits its own block-0 storage policy. The latent branch reproduces the formal \tool\ archives byte for byte. Large files use the twenty blocks $\mathrm{round}(i(N-1)/19)$.',
      r'\begin{longtable}{llrrrr}\caption{Matched replay with surface and latent archive bytes.}\label{tab:replay16}\\\toprule', r'Dataset & Scope & Blocks & Surface & Latent & Saving \\\midrule']
for r in RQ2:
    U.append(f"{r['dataset']} & {r['scope']} & {r['blocks']}/{r['file_blocks']} & {r['surface_bytes']:,} & {r['latent_bytes']:,} & " + ('n/a' if r['dataset'] in NOPROG else f"{r['saving_pct']:.2f}\\%") + r' \\')
U += [r'\bottomrule\end{longtable}']
# --- reruns
U += [r'\subsection{Complete reruns}', r'Each rerun draws five fresh syntheses (greedy and four at temperature 0.7) from the same GPT-4o gateway with the unchanged trainer and budgets, then gates, selects, fits storage, and encodes and decodes the complete file. Run 1 is the reported deployment and is not replaced.',
      r'\begin{longtable}{lrrrrrr}\caption{Complete reruns with ratios and archive bytes relative to DeLog.}\label{tab:reruns}\\\toprule', r'Dataset & Run & Ratio & Suffix ratio & vs.\ DeLog & Suffix vs.\ DeLog & \\\midrule']
for d, v in REP.items():
    for k in range(v['n']):
        U.append(f"{d} & {k+1} & {fmt(v['ratios'][k])} & {fmt(v['suffix_ratios'][k])} & {pct(v['vs_delog_pct'][k])} & {pct(v['suffix_vs_delog_pct'][k])} & " + r'\\')
U += [r'\bottomrule\end{longtable}']
# --- safety
SC = jl(DEV/'safety/static_check_result.json')
U += [r'\subsection{Decode-time safety}',
      r'\textbf{Audit.} The runtime executes \texttt{python\_exec} code with \texttt{exec(compile(...))} after an allow-list check of syntax nodes, in a namespace limited to \texttt{datetime}, \texttt{timedelta}, \texttt{int}, \texttt{str}, \texttt{len}, \texttt{float}, \texttt{round}, \texttt{abs}, \texttt{min}, \texttt{max}, and a restricted import shim. Context programs also receive the \texttt{re} module. Names beginning with a double underscore are rejected. Code runs in the decoding worker process with the decoder\textquoteright s privileges and no resource limits, and semantic archives are unpacked without member-path filtering. '
      r'Four crafted archives derived from the Apache archive test this boundary. They are an injected import (rejected by the runtime), a format string reading \texttt{\{0.\_\_class\_\_\}} (accepted), a context program reading \texttt{re.enum.sys} (accepted, top level executed), and a tar member \texttt{../} path (not executed against the unprotected decoder).',
      f"\\textbf{{Check.}} The enforced check rejects imports, names and attributes beginning with an underscore outside the runtime\\textquoteright s fixed wrapper, reflective built-ins, format fields, and module attributes outside an allow-list, and preflights tar members. It rejects all four crafted archives. Over {SC['unique_codes_checked']} distinct programs from all syntheses, deployments, and evolution runs, the only strict-rule findings ({SC['error_rule_counts_over_unique_codes'].get('E_UNDERSCORE', 0)} underscore names in {SC['unique_codes_template_wrapped']} programs) come from the runtime\\textquoteright s own fixed wrapper, which the check recognizes exactly. LLM-written code has no violation. A later pass with the same validator over the repeated greedy syntheses, the complete reruns, and the unseen-source runs adds 45 distinct LLM-written programs (134 in total) and again finds no violation outside the fixed wrapper. The eleven hand-written library programs also pass. With the check enforced before any program is compiled, all sixteen archives (3,797 blocks) decode byte-exactly."]
(OUT/'supp_r76.tex').write_text('\n\n'.join(U) + '\n')
print('supplement section written')

# ======================= RQ4 speed table (R73 session + R76 sessions T2/T3) =======================
T2P, T3P = DEV/'timing/runs/T2/SUMMARY_T2.json', DEV/'timing/runs/T3/SUMMARY_T3.json'
T2 = jl(T2P) if T2P.exists() else None; T3 = jl(T3P) if T3P.exists() else None
SPEED = [('semzip', 'semzip'), ('delog', 'delog'), ('loglite', 'loglite'), ('gzip6', 'gzip6'), ('xz6', 'xz6'), ('zstd3', 'zstd3'),
         ('xz9e', 'xz9e'), ('zstd19', 'zstd19'), ('logshrink', 'logshrink_r'), ('logreducer', 'logreducer_r')]
LABEL.setdefault('semzip1', 'SemZip-1')
def tget(S_, m, k):
    if not S_ or m is None or m not in S_['methods']: return None
    return S_['methods'][m].get(k)
if T3:  # matched subset: datasets where every T3 method is lossless in all repeats
    common = [d for d in LARGE if all(d in T3['methods'][m]['per_dataset'] and T3['methods'][m]['per_dataset'][d]['lossless_all_repeats'] for m in T3['methods'])]
    def t3bw(m, ph):
        per = T3['methods'][m]['per_dataset']; return sum(per[d]['raw_bytes'] for d in common) / sum(per[d][ph + '_seconds'] for d in common) / 1e6 if common else None
    N['t3_common'] = common
HASB = bool(T2)
L = [r'\begin{table}[t]\centering\small',
     r'\caption{Throughput in decimal MB/s of original input (encode/decode, medians of three clean serialized trials, four block workers). '
     r'Small is the geometric mean over the twelve files no larger than BGL (one session). '
     + (r'Session B: a later session on the same files for the new baselines, with \tool\ and DeLog re-timed as anchors, geometric mean. ' if HASB else '')
     + (f"Large is byte-weighted over twenty systematically sampled blocks each of {names(N.get('t3_common', []))}, the larger files on which every method restores all sampled blocks (all four larger files are in the text and supplement)" if T3 else 'Large is not timed') + r'. A dash means not timed. LogNexus+R was added after the timing sessions and was not timed.}',
     r'\label{tab:external-speed}', r'\setlength{\tabcolsep}{4pt}', r'\begin{tabular}{l' + 'rr' * (3 if HASB else 2) + '}', r'\toprule',
     r' & \multicolumn{2}{c}{Small}' + (r' & \multicolumn{2}{c}{Session B}' if HASB else '') + r' & \multicolumn{2}{c}{Large} \\',
     r'Method & Enc. & Dec.' + (r' & Enc. & Dec.' if HASB else '') + r' & Enc. & Dec. \\', r'\midrule']
for m, tm in SPEED:
    a_e = TS.get(m, {}).get('encode_gmean'); a_d = TS.get(m, {}).get('decode_gmean')
    b_e = tget(T2, tm, 'encode_gmean'); b_d = tget(T2, tm, 'decode_gmean')
    l_e = t3bw(tm, 'encode') if (T3 and tm in T3['methods']) else None
    l_d = t3bw(tm, 'decode') if (T3 and tm in T3['methods']) else None
    vals = (a_e, a_d) + ((b_e, b_d) if HASB else ()) + (l_e, l_d)
    L.append(LABEL[m] + ' & ' + ' & '.join(fmt(v) for v in vals) + r' \\')
L += [r'\bottomrule', r'\end{tabular}\end{table}']
(OUT/'speed_table_r76.tex').write_text('\n'.join(L) + '\n')
if T3:
    TT = [r'\subsection{Large-file throughput}', r'The four larger files were timed on twenty systematically spaced original blocks each (block indices $\mathrm{round}(i(N-1)/19)$, byte-identical to the originals), three clean serialized trials per method and file with four block workers, in a session separate from the twelve-file session. Median MB/s of original input. A dash means not losslessly restored in all repeats (timed only).',
          r'\begin{longtable}{lrrrrrrrr}\caption{Large-file throughput per sampled file (encode/decode MB/s).}\label{tab:t3-perfile}\\\toprule', r'Method & \multicolumn{2}{c}{HDFS} & \multicolumn{2}{c}{Spark} & \multicolumn{2}{c}{Windows} & \multicolumn{2}{c}{Thunderbird} \\\midrule']
    for m, tm in SPEED:
        if tm not in T3['methods']: continue
        per = T3['methods'][tm]['per_dataset']; cells = []
        for d in LARGE:
            e_ = per.get(d)
            cells += ([fmt(e_['encode_MB_per_s']), fmt(e_['decode_MB_per_s'])] if (e_ and e_['lossless_all_repeats']) else ['--', '--'])
        TT.append(LABEL[m] + ' & ' + ' & '.join(cells) + r' \\')
    TT += [r'\bottomrule\end{longtable}']
    (OUT/'supp_timing.tex').write_text('\n\n'.join(TT) + '\n')
t = [f"Across the twelve smaller files, \\tool's geometric-mean encoding and decoding throughputs are {100*TS['semzip']['encode_gmean']/TS['delog']['encode_gmean']:.1f}\\% and {100*TS['semzip']['decode_gmean']/TS['delog']['decode_gmean']:.1f}\\% of DeLog's, and it is slower than DeLog on every file in both directions. "
     f"Geometric means favor small files, where process start-up dominates both tools. Weighted by bytes, \\tool\\ encodes at {tb['semzip']['encode']:.2f} and decodes at {tb['semzip']['decode']:.2f}~MB/s versus {tb['delog']['encode']:.2f} and {tb['delog']['decode']:.2f}~MB/s for DeLog."]
if T2:
    sb = lambda m, k: T2['methods'][m].get(k)
    t.append(f"In session B, LogShrink+R encodes at {sb('logshrink_r', 'encode_gmean'):.2f} and LogReducer+R at {sb('logreducer_r', 'encode_gmean'):.2f}~MB/s (geometric mean, \\tool\\ {sb('semzip', 'encode_gmean'):.2f}, DeLog {sb('delog', 'encode_gmean'):.2f}).")
else:
    pass  # T2 stopped by design (change log 2026-10-01 21:20); Small column = R73 session A
if T3:
    se, de, sd, dd = t3bw('semzip', 'encode'), t3bw('delog', 'encode'), t3bw('semzip', 'decode'), t3bw('delog', 'decode')
    a4 = {m: {k: T3['methods'][m][k] for k in ('encode_byte_weighted_MB_per_s', 'decode_byte_weighted_MB_per_s')} for m in ('semzip', 'delog')}
    s4e, d4e = a4['semzip']['encode_byte_weighted_MB_per_s'], a4['delog']['encode_byte_weighted_MB_per_s']
    s4d, d4d = a4['semzip']['decode_byte_weighted_MB_per_s'], a4['delog']['decode_byte_weighted_MB_per_s']
    t.append(f"On twenty sampled blocks of each of the four larger files, which hold 98\\% of all bytes, \\tool\\ encodes at {s4e:.2f} and decodes at {s4d:.2f}~MB/s versus {d4e:.2f} and {d4d:.2f}~MB/s for DeLog ({100*s4e/d4e:.1f}\\% and {100*s4d/d4d:.1f}\\%, byte-weighted). "
             f"The Large column of Table~\\ref{{tab:external-speed}} uses only {names(common)}, where every method restores its blocks. There, LogShrink+R and LogReducer+R encode at {t3bw('logshrink_r', 'encode'):.2f} and {t3bw('logreducer_r', 'encode'):.2f}~MB/s.")
    N['t3_semzip_vs_delog'] = {'encode_pct': 100*se/de, 'decode_pct': 100*sd/dd, 'encode_pct_all4': 100*s4e/d4e, 'decode_pct_all4': 100*s4d/d4d}
    N['slowdown_range'] = (d4e/s4e, tb['delog']['encode']/tb['semzip']['encode'])
t.append("Construction is a one-time offline cost per log source. Five syntheses, the gate, and selection amortize over every later block, and archival storage is written once and restored rarely. "
         + (f"For sources where it is smaller, \\tool\\ suits cold archives that tolerate an encoder {N['slowdown_range'][0]:.1f}$\\times$ (four larger files) to {N['slowdown_range'][1]:.1f}$\\times$ (twelve smaller files) slower than DeLog's, byte-weighted. " if 'slowdown_range' in N else "For sources where it is smaller, \\tool\\ suits cold archives that tolerate a slower encoder. ") + "For hot or near-line logs, DeLog's faster encoding is the better trade.")
(OUT/'cost_text.tex').write_text('\n'.join(t) + '\n')
ab_txt = (OUT/'abstract_results.tex').read_text()
sp = (f"Encoding is {N['slowdown_range'][0]:.1f}--{N['slowdown_range'][1]:.1f}$\\times$ slower than DeLog's (byte-weighted, large and small files)." if 'slowdown_range' in N
      else f"On the twelve completely timed files, encoding runs at {100*tb['semzip']['encode']/tb['delog']['encode']:.1f}--{100*TS['semzip']['encode_gmean']/TS['delog']['encode_gmean']:.1f}\\% of DeLog's throughput (byte-weighted and geometric mean).")
(OUT/'abstract_results.tex').write_text(ab_txt.replace('ENCODE_SPEED_PLACEHOLDER', sp))
N['missing'] = missing
(OUT/'r76_numbers.json').write_text(json.dumps(N, indent=1, default=str))
print('speed table written; T2', bool(T2), 'T3', bool(T3))

# ======================= R76-G: unseen sources =======================
UG = DEV/'unseen'
if (UG/'summary_all.json').exists():
    U_ = jl(UG/'summary_all.json'); UD = ['NASA', 'ClarkNet', 'USask', 'Calgary']
    def lr_u(d):
        p_ = UG/'baselines/logreducer'/d/'status.json'
        if not p_.exists(): return None
        r = jl(p_); return (r['raw_bytes']/r['archive_bytes']) if (r.get('status') == 'PASS' and r.get('full_sha_match')) else None
    def ls_u(d):
        p_ = UG/'baselines/logshrink'/d/'result.json'
        if not p_.exists(): return None
        r = jl(p_); rp = r.get('repaired', {})
        return rp.get('ratio') if (rp.get('lossless') and rp.get('full_sha256_match') and not r.get('failed_blocks')) else None
    UC = ['semzip', 'delog', 'lognexus', 'logshrink', 'logreducer', 'loglite', 'xz9e']
    rows = {}
    for d in UD:
        v = U_['datasets'][d]; b = v['baselines']
        rows[d] = {'semzip': v['ratio'], 'delog': b['delog']['ratio'], 'lognexus': (LN_ALL[d]['raw']/LN_ALL[d]['arch']) if d in LN_ALL else None, 'loglite': b['loglite']['ratio'], 'xz9e': b['xz9e']['ratio'], 'zstd19': b['zstd19']['ratio'],
                   'logshrink': ls_u(d), 'logreducer': lr_u(d), 'lines': v['lines'], 'suffix': v['suffix_ratio'], 'delog_suffix': b['delog']['suffix_ratio']}
    ag_ = U_['aggregate_vs_delog']
    L = [r'\begin{table}[t]\centering\footnotesize',
         r'\caption{Unseen sources, namely four public web-server access logs never used during development (complete-file ratios, every cell decoded from its archive alone with matching SHA-256). The pipeline, prompts, and constants are unchanged, and DeLog has no hand-written rules for these sources. A dash means the tool failed on at least one block.}',
         r'\label{tab:unseen}', r'\setlength{\tabcolsep}{3.2pt}', r'\begin{tabular}{lr' + 'r'*len(UC) + '}', r'\toprule',
         r'Source & Lines & ' + ' & '.join(LABEL[m] for m in UC) + r' \\', r'\midrule']
    for d in UD:
        L.append(f"{d} & {rows[d]['lines']/1e6:.2f}M & " + ' & '.join(bold_row([rows[d][m] for m in UC])) + r' \\')
    L += [r'\bottomrule', r'\end{tabular}\end{table}']
    (OUT/'unseen_table.tex').write_text('\n'.join(L) + '\n')
    lossu = [f"{LABEL[m]} on {d}" for d in UD for m in ('lognexus', 'logshrink', 'logreducer') if rows[d][m] and rows[d][m] > rows[d]['semzip']]
    dlp = [100*(rows[d]['semzip']/rows[d]['delog'] - 1) for d in UD]
    cal = [(m, 100*(rows['Calgary'][m]/rows['Calgary']['semzip'] - 1)) for m in ('logshrink', 'logreducer') if rows['Calgary'][m]]
    lnu = [100*(rows[d]['semzip']/rows[d]['lognexus'] - 1) for d in UD if rows[d]['lognexus']]  # % more LogNexus+R bytes than SemZip
    t = [f"To test sources outside the development benchmark, we run the unchanged pipeline on four public web-server access logs (Table~\\ref{{tab:unseen}}), a format absent from the sixteen files. "
         f"\\tool\\ is smaller than DeLog on {ag_['wins_vs_delog']}/4 files and {ag_['suffix_wins_vs_delog']}/4 suffixes, with {min(dlp):.1f}--{max(dlp):.1f}\\% higher ratios and {abs(100*ag_['total_bytes_difference_semzip_minus_delog']/ag_['total_delog_bytes']):.1f}\\% fewer summed bytes. It also beats LogLite-BL and every general-purpose setting on every file (supplement). "
         f"The adapted baselines are closer. On Calgary, LogShrink+R and LogReducer+R are {min(v for _, v in cal):.1f}--{max(v for _, v in cal):.1f}\\% smaller than \\tool. On the 24 of USask's 25 blocks that LogShrink+R encodes, it is 1.3\\% smaller. "
         f"LogShrink+R fails on ClarkNet and USask, and LogReducer+R on USask (supplement)."
         + ((f" LogNexus+R, with its untuned default threshold, is {min(lnu):.1f}--{max(lnu):.1f}\\% larger than \\tool\\ on all four." if all(v > 0 for v in lnu) else
             f" LogNexus+R, with its untuned default threshold, is smaller than \\tool\\ on {sum(v < 0 for v in lnu)} of {len(lnu)}.") if lnu else '')]
    (OUT/'unseen_text.tex').write_text('\n'.join(t) + '\n')
    N['unseen'] = {'rows': rows, 'aggregate': ag_, 'losses': lossu}
    ab_txt = (OUT/'abstract_results.tex').read_text()
    if 'never used during development' not in ab_txt:
        ab_txt = ab_txt.replace(' Without its programs', f" On four web-server logs never used during development it is smaller than DeLog (which has no rules for them) on all four, with {abs(100*ag_['total_bytes_difference_semzip_minus_delog']/ag_['total_delog_bytes']):.1f}\\% fewer summed bytes, although LogShrink+R and LogReducer+R are 0.5--0.6\\% smaller on one. Without its programs", 1)
        (OUT/'abstract_results.tex').write_text(ab_txt)
    US = [r'\subsection{Unseen sources}', r'The four logs are the LBL Internet Traffic Archive HTTP access logs NASA (July 1995), ClarkNet (28 August 1995), USask, and Calgary, selected by a fixed rule (every server log with at least 700,000 lines, one file per server) before any result. They are decompressed unchanged. The SemZip pipeline differs from the main runs only in reading the input identities of these files. '
          r'LogShrink+R (default header length) fails on ClarkNet (its decoder crashes on nine blocks containing bare carriage returns) and on USask (its encoder fails on block 2). LogReducer+R fails on USask (the official encoder crashes on block 2). On the 24 USask blocks both tools encode, LogShrink+R is 1.3\% smaller and LogReducer+R 0.2\% larger than \tool\ on the same blocks.',
          r'\begin{longtable}{lrrrrrrrrrrr}\caption{Unseen sources. Complete-file ratios of every method except LogNexus+R (Table~\ref{tab:lognexus}), and suffix ratios (blocks 1 onward) of \tool\ and DeLog. A dash means the tool failed on at least one block.}\label{tab:unseen-suffix}\\\toprule', r'Source & \tool\ & DeLog & LS+R & LR+R & LogLite & gzip6 & XZ6 & XZ9e & Zstd3 & Zstd19 & Suffix S/D \\\midrule']
    for d in UD:
        b_ = U_['datasets'][d]['baselines']
        US.append(f"{d} & {fmt(rows[d]['semzip'])} & {fmt(rows[d]['delog'])} & {fmt(rows[d]['logshrink'])} & {fmt(rows[d]['logreducer'])} & " + ' & '.join(fmt(b_[c]['ratio']) for c in ('loglite', 'gzip6', 'xz6', 'xz9e', 'zstd3', 'zstd19')) + f" & {fmt(rows[d]['suffix'])}/{fmt(rows[d]['delog_suffix'])}" + r' \\')
    US += [r'\bottomrule\end{longtable}']
    (OUT/'supp_unseen.tex').write_text('\n\n'.join(US) + '\n')
    (OUT/'r76_numbers.json').write_text(json.dumps(N, indent=1, default=str))
    print('unseen section written; losses', lossu)

# ======================= abstract results (SE style, few numbers; written last) =======================
EXACT = ['delog'] + ALLCMP  # every byte-exact baseline measured on LogHub
def best_other(d):
    return min(M[m][d]['arch'] for m in EXACT if d in M[m])
lh_best = [d for d in DS if M['semzip'][d]['arch'] < best_other(d)]
lh_miss = [d for d in DS if d not in lh_best]
if (DEV/'unseen/summary_all.json').exists():
    def un_others(d):
        o = [rows[d][m] for m in UC[1:] if rows[d][m]]
        return o + [b_['ratio'] for b_ in U_['datasets'][d]['baselines'].values() if b_.get('ratio')]
    un_best = [d for d in UD if all(rows[d]['semzip'] > r_ for r_ in un_others(d))]
    un_miss = [d for d in UD if d not in un_best]
else:
    un_best, un_miss = [], UD
k = len(lh_best) + len(un_best); miss = lh_miss + un_miss
ln_named = 'LogNexus' if M['lognexus'] else None
N['abstract'] = {'smallest_on': k, 'of': len(DS) + len(UD), 'not_smallest': miss}
ab = (f"We evaluate \\tool\\ on sixteen LogHub logs and four web-server logs unseen in development, and require every archive to decode byte for byte on its own. "
      f"Among DeLog, general-purpose compressors, and byte-exact adaptations of {'LogNexus, ' if ln_named else ''}LogShrink and LogReducer, \\tool\\ produces the smallest archive on {k} of {len(DS) + len(UD)} logs."
      + ((f" The exceptions are {names([d for d in miss if d != 'Thunderbird'] + ['the largest log, Thunderbird, which dominates summed bytes'])}." if 'Thunderbird' in miss else f" The exceptions are {names(miss)}.") if 0 < len(miss) <= 2 else '')
      + f" Yet \\tool\\ encodes {N['slowdown_range'][0]:.0f}--{N['slowdown_range'][1]:.0f}$\\times$ more slowly than DeLog (byte-weighted). "
      f"Without the synthesized programs, the geometric-mean ratio of the same pipeline falls from {S['geomean']:.1f}$\\times$ to {ag['empty']['geomean']:.1f}$\\times$. "
      f"We find that most of this gain comes from separating rendered fields, and storing them as values adds less.")
(OUT/'abstract_results.tex').write_text(ab + '\n')
(OUT/'r76_numbers.json').write_text(json.dumps(N, indent=1, default=str))
print('abstract:', ab)
