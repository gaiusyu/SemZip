"""R76 B1 attribution ladder: empty program -> SemZip-1 (R71 c0) -> gated c0 (qg1c0) -> pooled selection, with DeLog.
Complete files and suffixes (blocks 1..). Output: ladder.json + printed table."""
import json, statistics as st
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]; H = ROOT/'r73_quality_gate_20260927'; R = ROOT/'r76_additional_20260929'
DS = ['Android','Apache','BGL','Hadoop','HDFS','HealthApp','HPC','Linux','Mac','OpenSSH','OpenStack','Proxifier','Spark','Thunderbird','Windows','Zookeeper']
ONE = {'Apache','Linux','Proxifier','Zookeeper'}
A = json.loads((ROOT/'artifact_r71_working/results/final/anonymous_results.json').read_text())
size = {}
for r in A['size_rows']: size.setdefault(r['method'], {})[r['dataset']] = r
SUF = json.loads((ROOT/'artifact_r71_working/results/suffix/suffix_comparison.json').read_text())
sdl = {r['dataset']: r for r in SUF['rows'] if r['method'] == 'delog'}
IDENT = {'HDFS', 'Thunderbird'}  # gate leaves the R71 program unchanged; qg1c0 = SemZip-1 (same as results_md.py)
def formal(v):
    o = {}
    for d in DS:
        if v == 'qg1c0' and d in IDENT:
            x = size['semzip'][d]; o[d] = {'raw': x['raw_bytes'], 'arch': x['archive_bytes'], 'suf_raw': x['heldout']['raw_bytes'], 'suf_arch': x['heldout']['archive_bytes']}; continue
        r = json.loads((H/'dev_results/runs/formal'/v/d/'result.json').read_text()); assert r['status']=='PASS' and r['independent_materialized_file_sha_pass']
        o[d] = {'raw': r['encode']['raw_bytes'], 'arch': r['encode']['archive_bytes'], 'suf_raw': r['heldout']['raw_bytes'], 'suf_arch': r['heldout']['archive_bytes']}
    return o
E = json.loads((R/'dev_results/empty/empty_summary.json').read_text())
emp = {d: {'raw': E[d]['raw_bytes'], 'arch': E[d]['archive_bytes'], 'suf_raw': E[d]['heldout']['raw_bytes'], 'suf_arch': E[d]['heldout']['archive_bytes']} for d in DS}
assert all(E[d]['sha_pass'] and E[d]['status']=='PASS' for d in DS)
pool, qg = formal('pool'), formal('qg1c0')
r71 = {d: {'raw': size['semzip'][d]['raw_bytes'], 'arch': size['semzip'][d]['archive_bytes'],
           'suf_raw': (size['semzip'][d].get('heldout') or {}).get('raw_bytes'), 'suf_arch': (size['semzip'][d].get('heldout') or {}).get('archive_bytes')} for d in DS}
dl = {d: {'raw': size['delog'][d]['raw_bytes'], 'arch': size['delog'][d]['archive_bytes'],
          'suf_raw': sdl[d]['suffix_raw_bytes'] if d in sdl else None, 'suf_arch': sdl[d]['suffix_archive_bytes'] if d in sdl else None} for d in DS}
M = {'empty': emp, 'semzip1': r71, 'gated_c0': qg, 'pool': pool, 'delog': dl}
for d in DS: assert len({M[m][d]['raw'] for m in M}) == 1, d
out = {'per_dataset': {}, 'aggregate': {}}
for d in DS:
    out['per_dataset'][d] = {m: {'ratio': M[m][d]['raw']/M[m][d]['arch'], 'archive_bytes': M[m][d]['arch'],
                                 'suffix_ratio': (M[m][d]['suf_raw']/M[m][d]['suf_arch']) if (d not in ONE and M[m][d]['suf_arch']) else None} for m in M}
multi = [d for d in DS if d not in ONE]
for m in M:
    tot = sum(M[m][d]['arch'] for d in DS); raw = sum(M[m][d]['raw'] for d in DS)
    out['aggregate'][m] = {'mean': st.mean(M[m][d]['raw']/M[m][d]['arch'] for d in DS), 'geomean': st.geometric_mean([M[m][d]['raw']/M[m][d]['arch'] for d in DS]),
        'corpus': raw/tot, 'total_archive': tot, 'total_vs_delog_pct': 100*(tot/sum(dl[d]['arch'] for d in DS)-1),
        'wins_vs_delog': sum(M[m][d]['arch'] < dl[d]['arch'] for d in DS),
        'suffix_mean': st.mean(M[m][d]['suf_raw']/M[m][d]['suf_arch'] for d in multi) if all(M[m][d]['suf_arch'] for d in multi) else None,
        'suffix_geomean': st.geometric_mean([M[m][d]['suf_raw']/M[m][d]['suf_arch'] for d in multi]) if all(M[m][d]['suf_arch'] for d in multi) else None,
        'suffix_wins_vs_delog': sum(M[m][d]['suf_arch'] < dl[d]['suf_arch'] for d in multi) if all(M[m][d]['suf_arch'] for d in multi) else None}
(R/'analysis/ladder.json').write_text(json.dumps(out, indent=1))
print(f"{'Dataset':12s}" + ''.join(f'{m:>11s}' for m in M) + '   | suffix: empty pool delog')
for d in DS:
    p = out['per_dataset'][d]
    print(f'{d:12s}' + ''.join(f"{p[m]['ratio']:11.2f}" for m in M) + ('   | ' + ' '.join(f"{p[m]['suffix_ratio']:.2f}" for m in ('empty','pool','delog')) if d not in ONE else ''))
for m, a in out['aggregate'].items(): print(m, {k: (round(v, 3) if isinstance(v, float) else v) for k, v in a.items()})
