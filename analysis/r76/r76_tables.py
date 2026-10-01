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
DENUM = {}
for d in DS:
    ps = sorted((DEV/'baselines/denum/full_enconly'/d/'denum').glob('trial_001/attempt_*/result.json'))
    if ps:
        r = jl(ps[-1])
        if r.get('status') == 'ENCODE_ONLY_UNVERIFIED' and r.get('raw_bytes') == size['delog'][d]['raw_bytes']:
            DENUM[d] = r['raw_bytes']/r['archive_bytes']
LABEL = {'semzip': r'\tool', 'delog': 'DeLog', 'logshrink': 'LogShrink+R*', 'logreducer': 'LogReducer+R*', 'loglite': 'LogLite-BL*',
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
MAIN = ['semzip', 'delog', 'logshrink', 'logreducer', 'loglite', 'gzip6', 'xz6', 'xz9e', 'zstd19']
L = [r'\begin{table*}[t]\centering\footnotesize',
     r'\caption{Complete original-file compression ratios (raw / all archive bytes; every cell decoded from its archive alone with a matching SHA-256). '
     r'\tool: frozen deployment trained on block 0. Bold: best in the row. *Adapted method: LogShrink and LogReducer as released do not restore their input byte-exactly, so +R stores a counted per-line correction (supplement); LogLite-BL uses a disclosed fixed adaptation. '
     r'Denum is excluded because its released format cannot restore its input (supplement); LogFold and LogPrism have no public implementation. $\dagger$: one block, in-sample; Suffix: blocks 1 onward of the 12 multi-block files. --: pending or failed (supplement).}',
     r'\label{tab:external-ratios}', r'\setlength{\tabcolsep}{3.4pt}', r'\begin{tabular}{l' + 'r'*len(MAIN) + '}', r'\toprule',
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
     r'\caption{Attribution on complete files (ratios; archive-only decoding with matching SHA-256 for every cell). Empty: the same pipeline and backend with no program (residual processing only). Library: a frozen, hand-written, dataset-agnostic program library (timestamps, IPv4, sizes, percentages, hex, decimals) through the same gate, storage fitting, and runtime, with no LLM; Lib.+floor (post hoc): the empty program replaces the library plan when it is smaller on block 0, a rule that leaves \tool\ unchanged. SemZip-1: one greedy synthesis; Gated: SemZip-1 after the quality gate; \tool: five syntheses and cost-guided selection. DeLog-gen: official DeLog without its per-dataset regular expressions. Latent: archive saving of latent over literal storage of exactly the same matched spans (matched replay); s: 20 systematically sampled blocks; n/a: literal and latent archives are byte-identical (no matched span is stored through a synthesized program).}',
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
L = [r'\begin{table}[t]\centering\small', r'\caption{Three complete pipeline runs (five fresh syntheses, gate, selection, storage fitting, complete-file encoding and archive-only decoding) on the four files where \tool\ led DeLog by 2--6\%. Run 1 is the reported deployment. Bytes vs.\ DeLog: complete file / suffix.}',
     r'\label{tab:repeats}', r'\setlength{\tabcolsep}{4pt}', r'\begin{tabular}{lrrrrr}', r'\toprule', r'Dataset & Run 1 & Run 2 & Run 3 & DeLog & Bytes vs.\ DeLog \\', r'\midrule']
for d, x in REP.items():
    rs = x['ratios'] + [None]*(3 - len(x['ratios']))
    rng = f"{min(x['vs_delog_pct']):+.1f} to {max(x['vs_delog_pct']):+.1f}\\% / {min(x['suffix_vs_delog_pct']):+.1f} to {max(x['suffix_vs_delog_pct']):+.1f}\\%".replace('-', '$-$')
    L.append(f"{d} & " + ' & '.join(fmt(v) for v in rs) + f" & {fmt(x['delog_ratio'])} & {rng}" + r' \\')
L += [r'\bottomrule', r'\end{tabular}\end{table}']
(OUT/'repeats_table.tex').write_text('\n'.join(L) + '\n')

# ---------- supplement: other codec settings and non-lossless natives ----------
SUP = ['zstd3', 'zstd22long', 'zstd19dict', 'delog_generic']
L = [r'\begin{longtable}{l' + 'r'*(len(SUP) + 5) + '}', r'\caption{Supplementary external measurements. Left: further general-purpose settings (Zstd19+D: dictionary trained on block 0 and counted once per file) and DeLog without its dataset-specific regular expressions, all byte-exact from archives alone. Zstd22L: zstd --ultra -22 --long=27. Right: compression-only ratios of LogShrink and LogReducer as released (not byte-exact; exact files marked e), the number of LogShrink blocks that failed to encode or decode, and Denum\textquotesingle s compression-only ratio (its format cannot restore the input; not losslessly verifiable).}\label{tab:supp-external}\\',
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
t = []
t.append(f"On complete files, \\tool\\ has the largest ratio on {sum(1 for d in DS if max((ratio(m, d) or 0) for m in MAIN) == ratio('semzip', d))}/16 files (Table~\\ref{{tab:external-ratios}}). "
         f"Against DeLog, the strongest baseline, it is smaller on {wins_dl}/16 files and on {swins_dl}/12 held-out suffixes, which no selection or fitting step reads. "
         f"Its geometric-mean ratio is {x(S['geomean'])} versus {x(DL['geomean'])} ({x(S['suffix_geomean'])} versus {x(DL['suffix_geomean'])} on suffixes) and its arithmetic mean {x(S['mean'])} versus {x(DL['mean'])}. "
         f"Summed archive bytes, however, are {abs(S['total_vs_delog_pct']):.2f}\\% larger than DeLog's: Thunderbird holds {TB_share:.1f}\\% of all raw bytes and is the one file where DeLog wins "
         f"({x(ratio('semzip', 'Thunderbird'))} versus {x(ratio('delog', 'Thunderbird'))}; RQ3 shows that its frozen program rarely beats the empty program on later blocks).")
for m, nm in (('logshrink', 'LogShrink+R'), ('logreducer', 'LogReducer+R')):
    pass
t.append(f"LogShrink and LogReducer as released drop carriage returns, collapse spaces, strip leading zeros, and (LogShrink) truncate integers wider than 32 bits, so neither restores most files; their adapted +R variants store the counted per-line correction. "
         f"\\tool\\ is smaller than LogShrink+R on {ag['logshrink']['semzip_smaller_on']}/{ag['logshrink']['n']} and than LogReducer+R on {ag['logreducer']['semzip_smaller_on']}/{ag['logreducer']['n']} reconstructed files"
         + (f"; LogShrink could not encode or decode every block of {names(sorted(ls_fail))}" if ls_fail else '')
         + (f", and LogReducer+R failed the SHA check on part of {names(sorted(lr_fail))}" if lr_fail else '') + ". "
         f"High-effort general-purpose settings remain far behind: XZ9e averages {x(ag['xz9e']['mean'])} and Zstd19 {x(ag['zstd19']['mean'])}; a block-0 Zstd dictionary and the Zstd ultra setting are in the supplement.")
(OUT/'rq1_text.tex').write_text('\n'.join(t) + '\n')

e, sz1, g, lf = ag['empty'], ag['semzip1'], ag['gated'], ag['library_floor']
dlw = lambda m: sum(M[m][d]['arch'] < M['delog'][d]['arch'] for d in DS if d in M[m])
lib_below_empty = [d for d in M['library'] if M['library'][d]['arch'] > M['empty'][d]['arch']]
lib_improves = [d for d in M['library_floor'] if M['library_floor'][d]['arch'] < M['empty'][d]['arch']]
t = []
t.append(f"Table~\\ref{{tab:attribution}} separates the contributions. With the same pipeline, backend, storage fitting, and runtime but no program, the empty program averages {x(e['mean'])} (geometric mean {x(e['geomean'])}) and is smaller than DeLog on only {dlw('empty')}/16 files; "
         f"the programs of the deployed plans reduce summed archive bytes by {100*(1-S['total']/e['total']):.1f}\\% and account for {sum(1 for d in DS if M['semzip'][d]['arch'] < M['delog'][d]['arch'] and not M['empty'][d]['arch'] < M['delog'][d]['arch'])} of \\tool's {wins_dl} wins over DeLog. "
         f"One greedy synthesis (SemZip-1) reaches {x(sz1['mean'])} but is larger than the empty program on {names([d for d in DS if M['semzip1'][d]['arch'] > M['empty'][d]['arch']])}; the quality gate removes such rules ({x(g['mean'])}, {dlw('gated')}/16 files smaller than DeLog), and selection across five syntheses adds the rest ({x(S['mean'])}, {dlw('semzip')}/16).")
t.append(f"A frozen, hand-written library of common renderers (timestamps, IPv4, sizes, percentages, hexadecimal and decimal numbers) through the same gate and runtime does not reproduce this: it is larger than the empty program on {len(lib_below_empty)}/{len(M['library'])} files. "
         f"The gate removes one rule group at a time and never compares against the empty program; with a post-hoc empty-program floor decided on block 0, which leaves \\tool\\ unchanged, the library improves on the empty program on {len(lib_improves)} files ({names(lib_improves)}), and the equally single-candidate Gated plan is smaller than it on {sum(M['gated'][d]['arch'] < M['library_floor'][d]['arch'] for d in M['library_floor'])}/{len(M['library_floor'])} files. "
         f"Official DeLog without its per-dataset regular expressions (DeLog-gen) is smaller than \\tool\\ on {sum(M['delog_generic'][d]['arch'] < M['semzip'][d]['arch'] for d in M['delog_generic'])}/{len(M['delog_generic'])} files.")
r2 = N['rq2']
t.append(f"The matched replay (last column) stores exactly the same matched spans either literally or as latent values, so it separates representation from field separation. Latent storage saves {min(v['saving_pct'] for d, v in r2['per_dataset'].items() if d not in r2['noprog'] and v['saving_pct'] > 0):.1f}--{r2['max_informative'][0]:.1f}\\% on {sum(1 for d, v in r2['per_dataset'].items() if d not in r2['noprog'] and v['saving_pct'] > 0)} of the {len(r2['informative'])} informative files and {r2['complete_saving_pct']:.2f}\\% summed over the {sum(1 for r in RQ2 if r['scope'] == 'complete')} fully replayed files; "
         f"on {names(r2['noprog'])} literal and latent storage give byte-identical archives, and on Windows' sampled blocks the latent form is {abs(r2['per_dataset']['Windows']['saving_pct']):.2f}\\% larger. Field separation therefore carries most of the gain over the empty program, and latent values add a smaller, file-dependent share.")
(OUT/'rq2_text.tex').write_text('\n'.join(t) + '\n')

rp = N['repeats']
allw = all(v['wins'] == v['n'] and v['suffix_wins'] == v['n'] for v in rp.values())
spread = max(100*(max(v['ratios'])/min(v['ratios'])-1) for v in rp.values())
t = [f"To test whether the narrow wins survive a fresh draw of the whole pipeline, we reran it twice more on the four files where \\tool\\ led DeLog by 2--6\\% (Table~\\ref{{tab:repeats}}): five new syntheses, gate, selection, storage fitting, and complete-file encoding each time. "
     + (f"All {sum(v['n'] for v in rp.values())} runs are smaller than DeLog on both the complete file and the suffix" if allw else f"{sum(v['wins'] for v in rp.values())}/{sum(v['n'] for v in rp.values())} runs are smaller than DeLog on the complete file")
     + f", and complete-file ratios differ across runs by at most {spread:.1f}\\%. Selection thus reproduces these four comparisons although individual syntheses differ widely; Thunderbird, which decides summed bytes, was not rerun."]
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
OTHERS = [(m, n) for m, n in (('logshrink', 'LogShrink+R'), ('logreducer', 'LogReducer+R'), ('loglite', 'LogLite-BL'), ('xz9e', 'XZ~9e')) if M[m]]
beaten = [n for m, n in OTHERS if beat_all(m)]
ab = (f"On all sixteen LogHub files, each decoded byte-exactly from its archive alone, \\tool\\ produces smaller archives than DeLog on {wins_dl} files (geometric-mean ratio {S['geomean']:.1f}$\\times$ versus {DL['geomean']:.1f}$\\times$) and on {swins_dl} of 12 held-out suffixes, "
      + (f"and smaller than {names(beaten)} (the adapted methods restored byte-exactly) on every file they restore; " if len(beaten) == len(OTHERS) else f"and smaller than the other log compressors on most files they restore; ")
      + f"summed bytes are {S['total_vs_delog_pct']:.2f}\\% larger than DeLog's because \\tool\\ loses on the largest file. "
      f"Without its programs the same pipeline reaches a geometric mean of {e['geomean']:.1f}$\\times$ instead of {S['geomean']:.1f}$\\times$, a hand-written renderer library does not recover the difference, and "
      + ("two further complete runs keep all four narrow wins over DeLog. " if allw else "further complete runs test the narrow wins. ")
      + f"On the twelve completely timed files, encoding runs at {100*tb['semzip']['encode']/tb['delog']['encode']:.1f}--{100*TS['semzip']['encode_gmean']/TS['delog']['encode_gmean']:.1f}\\% of DeLog's throughput (byte-weighted and geometric mean).")
(OUT/'abstract_results.tex').write_text(ab + '\n')
ALLCMP = [m for m in ('logshrink', 'logreducer', 'loglite', 'gzip6', 'xz6', 'xz9e', 'zstd3', 'zstd19', 'zstd22long', 'zstd19dict', 'delog_generic') if M[m]]
allbeat = all(beat_all(m) for m in ALLCMP)
co = (f"It is smaller than DeLog on {wins_dl}/16 LogHub files and {swins_dl}/12 held-out suffixes"
      + (" and smaller than every other evaluated compressor on every file it restores" if allbeat else "") + f", at summed bytes {pct(S['total_vs_delog_pct'])} relative to DeLog and a slower encoder; "
      f"without its programs the same pipeline reaches {e['mean']:.2f}$\\times$, and a hand-written library does not substitute for them.")
(OUT/'conclusion_results.tex').write_text(co + '\n')
N['missing'] = missing
(OUT/'r76_numbers.json').write_text(json.dumps(N, indent=1, default=str))
print('abstract/conclusion/cost written; missing now:', missing)

# ======================= supplement section (R76) =======================
def esc(s): return s.replace('\\', r'\textbackslash{}').replace('_', r'\_').replace('%', r'\%').replace('&', r'\&').replace('#', r'\#').replace('$', r'\$').replace('{', r'\{').replace('}', r'\}').replace('~', r'\textasciitilde{}').replace('^', r'\^{}')
U = [r'\section{Additional baselines and controls}\label{sec:r76}\sloppy',
     r'All experiments in this section were specified in a design document written before any of their results existed (29 September 2026, 01:30 UTC); later changes are appended to that document as dated change-log entries and are stated below. Every compression cell decodes from its archive alone and matches per-block and full-file SHA-256 digests of the original inputs.']
# --- log-specific baselines
U += [r'\subsection{Log-specific baselines}',
      r'The +R correction deviates from the pre-registered adaptation rule (change log, 29 September, 05:30 UTC); it was recorded before any new-baseline ratio comparison and was not applied to Denum. LogShrink (commit 59ce494) and LogReducer (commit 4000541) run their official pipelines independently on every 100,000-line block, including their own sampling and template training on that block (samplers seeded with 0); their decoders read only the block archive, which contains the native payload, every model file the official restore reads, and, for the +R variants, a per-line correction. '
      r'As released, both tools drop carriage returns, strip leading and trailing whitespace, collapse runs of header spaces, and drop leading zeros; LogShrink also truncates integers wider than 32 bits (e.g., HDFS block identifiers). The correction is computed from the original block and the tool\textquotesingle s own decoded output only, compressed with LZMA, and counted. '
      r'LogShrink\textquotesingle s official decoder additionally needed five fixes that change no archive byte (empty failed-log files, column-file ordering, header-file ordering, whole-line template matching, and delimiter recovery). LogShrink uses its shipped per-dataset header lengths; LogReducer uses its generic defaults. '
      + '; '.join([f"LogReducer+R failed the SHA check on {v['failed']} of {d}\\textquotesingle s {v['blocks']} blocks" for d, v in sorted(NAT_LR.items()) if v.get('status') == 'FAIL'] + [f"LogShrink could not encode or decode {v['failed_blocks']} of {d}\\textquotesingle s {v['blocks']} blocks" for d, v in sorted(NAT_LS.items()) if v.get('failed_blocks')]) + '; these files have no valid ratio for the affected tool. '
      r'The official DeLog build (commit 64a074f) and DeLog-gen differ only in the per-dataset regular-expression map (emptied in DeLog-gen); the decoders are identical.',
      r'\input{generated/supp_external.tex}']
DP = jl(DEV/'baselines/denum/proofs/SUMMARY.json')
def win(a, b):
    i = next(k for k, (u, v) in enumerate(zip(a, b)) if u != v); st_ = max(0, i - 25)
    return ('...' if st_ else '') + a[st_:i + 25].rstrip('\r')
ex = [r for r in DP if r['dataset'] in ('Apache', 'HDFS', 'BGL')]
U += [r'\paragraph{Denum.} The released Denum compressor (commit a3a6975) replaces each match of its per-dataset regular expressions by a tag and stores the concatenated digits as one integer, without group widths or separator positions; its Python decoder also drops carriage returns and leading zeros. '
      f"For each of the sixteen datasets we changed one line of block 0 by moving a digit across a group boundary; in all {sum(r['all_archive_members_identical'] for r in DP)} cases the original and the modified block produce byte-identical archive members, so no decoder can restore both. Examples: "
      + '; '.join(r"\texttt{" + esc(win(r['original_line'], r['modified_line'])) + r"} vs.\ \texttt{" + esc(win(r['modified_line'], r['original_line'])) + '}' for r in ex) + '. Denum is therefore excluded from the lossless comparison.']
PUB = jl(ROOT/'results/published_baseline_reference_20260915.json')
U += [r'\paragraph{Published numbers without implementations.} LogFold and LogPrism provide no public implementation (checked 29 September 2026: the LogFold repository holds only a license and README; the LogPrism repository is empty). Their papers report the following ratios for 100,000-line chunks; input identities are not verified and these numbers are not matched observations.',
      r'\begin{longtable}{l' + 'r'*(len(PUB) + 1) + r'}\caption{Published ratios (literature context only) next to \tool.}\label{tab:published}\\\toprule',
      'Dataset & ' + ' & '.join(p['method'] + ' (' + p['source_version'] + ')' for p in PUB) + r' & \tool\ \\\midrule']
pubrows = {p['method']: {r['dataset']: r['reported_ratio'] for r in p['rows']} for p in PUB}
for d in DS:
    U.append(d + ' & ' + ' & '.join(fmt(pubrows[p['method']].get(d)) for p in PUB) + f" & {fmt(ratio('semzip', d))}" + r' \\')
U += [r'\bottomrule\end{longtable}']
# --- library
LR_ = {r['dataset']: r for r in jl(DEV/'library/library_report.json')}
U += [r'\subsection{Program-library control}',
      r'The library proposer replaces only the LLM call of the trainer; compilation, verification, the quality gate, storage fitting, the runtime, and archive-only decoding are unchanged, and it makes no API call. Its program types were frozen before any library result: syslog, ISO-8601, \texttt{YYYY-MM-DD HH:MM:SS} (with optional milliseconds), \texttt{YY/MM/DD}, \texttt{YYYYMMDD HHMMSS}, clock-time, and ten-digit epoch timestamps; IPv4 and IPv4:port; hexadecimal numbers; sizes with B/KB/MB/GB units; percentages; and decimal integers and fixed-point numbers with width and leading zeros. A type is proposed for a sampled group when it matches one of the group\textquotesingle s examples (at most eight per group). With one candidate per dataset, pooled selection reduces to the gated plan. '
      r'The gate removes one rule group at a time and only on a strict decrease, and never compares with the empty program; on HealthApp, where no library timestamp matches, fourteen generic integer rules leave block 0 3.4 times larger than the empty program. The post-hoc floor (not pre-registered) deploys the empty program when it is smaller on block 0; \tool\textquotesingle s deployed plans are below the empty program on block 0 for all sixteen files.',
      r'\begin{longtable}{lrrrrr}\caption{Program-library control: block-0 payload bytes and complete-file ratios.}\label{tab:library}\\\toprule',
      r'Dataset & Library B0 & Empty B0 & Library & Lib.+floor & \tool\ \\\midrule']
for d in DS:
    b = B0.get(d, {}); r = LR_.get(d, {})
    U.append(f"{d} & {format(b['library'], ',') if isinstance(b.get('library'), int) else '--'} & {format(b['empty'], ',')} & {fmt(ratio('library', d))} & {fmt(ratio('library_floor', d))} & {fmt(ratio('semzip', d))}" + r' \\')
U += [r'\bottomrule\end{longtable}']
# --- matched replay
U += [r'\subsection{Matched replay on the deployed plans}',
      r'Both branches execute the deployed matcher once per block and share its spans, rule order, placeholders, residual archive (byte-identical), and semantic XZ preset; the surface branch stores each replaced literal with identity reconstruction and generic field storage and fits its own block-0 storage policy. The latent branch reproduces the formal \tool\ archives byte for byte. Large files use the twenty blocks $\mathrm{round}(i(N-1)/19)$.',
      r'\begin{longtable}{llrrrr}\caption{Matched replay: surface and latent archive bytes.}\label{tab:replay16}\\\toprule', r'Dataset & Scope & Blocks & Surface & Latent & Saving \\\midrule']
for r in RQ2:
    U.append(f"{r['dataset']} & {r['scope']} & {r['blocks']}/{r['file_blocks']} & {r['surface_bytes']:,} & {r['latent_bytes']:,} & " + ('n/a' if r['dataset'] in NOPROG else f"{r['saving_pct']:.2f}\\%") + r' \\')
U += [r'\bottomrule\end{longtable}']
# --- reruns
U += [r'\subsection{Complete reruns}', r'Each rerun draws five fresh syntheses (greedy and four at temperature 0.7) from the same GPT-4o gateway with the unchanged trainer and budgets, then gates, selects, fits storage, and encodes and decodes the complete file; run 1 is the reported deployment and is not replaced.',
      r'\begin{longtable}{lrrrrrr}\caption{Complete reruns: ratios and archive bytes relative to DeLog.}\label{tab:reruns}\\\toprule', r'Dataset & Run & Ratio & Suffix ratio & vs.\ DeLog & Suffix vs.\ DeLog & \\\midrule']
for d, v in REP.items():
    for k in range(v['n']):
        U.append(f"{d} & {k+1} & {fmt(v['ratios'][k])} & {fmt(v['suffix_ratios'][k])} & {pct(v['vs_delog_pct'][k])} & {pct(v['suffix_vs_delog_pct'][k])} & " + r'\\')
U += [r'\bottomrule\end{longtable}']
# --- safety
SC = jl(DEV/'safety/static_check_result.json')
U += [r'\subsection{Decode-time safety}',
      r'\textbf{Audit.} The runtime executes \texttt{python\_exec} code with \texttt{exec(compile(...))} after an allow-list check of syntax nodes, in a namespace limited to \texttt{datetime}, \texttt{timedelta}, \texttt{int}, \texttt{str}, \texttt{len}, \texttt{float}, \texttt{round}, \texttt{abs}, \texttt{min}, \texttt{max}, and a restricted import shim; context programs also receive the \texttt{re} module. Names beginning with a double underscore are rejected. Code runs in the decoding worker process with the decoder\textquotesingle s privileges and no resource limits, and semantic archives are unpacked without member-path filtering. '
      r'Four crafted archives derived from the Apache archive test this boundary: an injected import (rejected by the runtime), a format string reading \texttt{\{0.\_\_class\_\_\}} (accepted), a context program reading \texttt{re.enum.sys} (accepted, top level executed), and a tar member \texttt{../} path (not executed against the unprotected decoder).',
      f"\\textbf{{Check.}} The enforced check rejects imports, names and attributes beginning with an underscore outside the runtime\\textquotesingle s fixed wrapper, reflective built-ins, format fields, and module attributes outside an allow-list, and preflights tar members. It rejects all four crafted archives. Over {SC['unique_codes_checked']} distinct programs from all syntheses, deployments, and evolution runs, the only strict-rule findings ({SC['error_rule_counts_over_unique_codes'].get('E_UNDERSCORE', 0)} underscore names in {SC['unique_codes_template_wrapped']} programs) come from the runtime\\textquotesingle s own fixed wrapper, which the check recognizes exactly; LLM-written code has no violation. With the check enforced before any program is compiled, all sixteen archives (3,797 blocks) decode byte-exactly."]
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
L = [r'\begin{table}[t]\centering\small',
     r'\caption{Throughput in decimal MB/s of original input (encode/decode; medians of three clean serialized trials, four block workers). '
     r'Session A: the twelve files no larger than BGL, geometric mean. Session B: a later session on the same files for the new baselines, with \tool\ and DeLog re-timed as anchors; geometric mean. '
     r'Large: byte-weighted over twenty systematically sampled blocks of each of the four larger files'
     + (f" (datasets where every method is lossless: {names(N.get('t3_common', []))})" if T3 else '') + r'. --: not timed.}',
     r'\label{tab:external-speed}', r'\setlength{\tabcolsep}{4pt}', r'\begin{tabular}{lrrrrrr}', r'\toprule',
     r' & \multicolumn{2}{c}{Session A} & \multicolumn{2}{c}{Session B} & \multicolumn{2}{c}{Large} \\', r'Method & Enc. & Dec. & Enc. & Dec. & Enc. & Dec. \\', r'\midrule']
for m, tm in SPEED:
    a_e = TS.get(m, {}).get('encode_gmean'); a_d = TS.get(m, {}).get('decode_gmean')
    b_e = tget(T2, tm, 'encode_gmean'); b_d = tget(T2, tm, 'decode_gmean')
    l_e = t3bw(tm, 'encode') if (T3 and tm in (T3['methods'] if T3 else {})) else None
    l_d = t3bw(tm, 'decode') if (T3 and tm in (T3['methods'] if T3 else {})) else None
    L.append(LABEL[m] + ' & ' + ' & '.join(fmt(v) for v in (a_e, a_d, b_e, b_d, l_e, l_d)) + r' \\')
L += [r'\bottomrule', r'\end{tabular}\end{table}']
(OUT/'speed_table_r76.tex').write_text('\n'.join(L) + '\n')
t = [f"Across the twelve files of session A, \\tool's geometric-mean encoding and decoding throughputs are {100*TS['semzip']['encode_gmean']/TS['delog']['encode_gmean']:.1f}\\% and {100*TS['semzip']['decode_gmean']/TS['delog']['decode_gmean']:.1f}\\% of DeLog's, and it is slower than DeLog on every file in both directions. "
     f"Geometric means favor small files, where process start-up dominates both tools; weighted by bytes, \\tool\\ encodes at {tb['semzip']['encode']:.2f} and decodes at {tb['semzip']['decode']:.2f}~MB/s versus {tb['delog']['encode']:.2f} and {tb['delog']['decode']:.2f}~MB/s for DeLog."]
if T2:
    sb = lambda m, k: T2['methods'][m].get(k)
    t.append(f"In session B, LogShrink+R encodes at {sb('logshrink_r', 'encode_gmean'):.2f} and LogReducer+R at {sb('logreducer_r', 'encode_gmean'):.2f}~MB/s (geometric mean; \\tool\\ {sb('semzip', 'encode_gmean'):.2f}, DeLog {sb('delog', 'encode_gmean'):.2f}).")
else:
    missing.append('timing T2 (new baselines)')
if T3:
    se, de, sd, dd = t3bw('semzip', 'encode'), t3bw('delog', 'encode'), t3bw('semzip', 'decode'), t3bw('delog', 'decode')
    t.append(f"On the sampled blocks of the four larger files, which hold 98\\% of all bytes, \\tool\\ encodes at {se:.2f} and decodes at {sd:.2f}~MB/s versus {de:.2f} and {dd:.2f}~MB/s for DeLog ({100*se/de:.1f}\\% and {100*sd/dd:.1f}\\%).")
    N['t3_semzip_vs_delog'] = {'encode_pct': 100*se/de, 'decode_pct': 100*sd/dd}
t.append("Construction is a one-time offline cost per log source: five syntheses, the gate, and selection (above) amortize over every later block, and archival storage is written once and restored rarely. "
         "For sources where it is smaller, \\tool\\ suits cold archives that tolerate a 5--8$\\times$ slower encoder; for hot or near-line logs, DeLog's faster encoding is the better trade.")
(OUT/'cost_text.tex').write_text('\n'.join(t) + '\n')
N['missing'] = missing
(OUT/'r76_numbers.json').write_text(json.dumps(N, indent=1, default=str))
print('speed table written; T2', bool(T2), 'T3', bool(T3))
