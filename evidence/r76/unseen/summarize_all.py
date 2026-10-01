#!/usr/bin/env python3
"""R76-G read-only summary: SemZip r1 + main baselines (summary.json from rep.py summary) + optional LogReducer+R /
LogShrink+R (L=5).  Writes summary_all.json and prints a markdown table.  Lossless = archive-only decode with
per-block and full-file SHA-256 equal to inputs.json; anything else is listed with its failure, never as a ratio win."""
import json
from pathlib import Path

U = Path(__file__).resolve().parent
S = json.loads((U/'summary.json').read_text())
ORDER = ['NASA', 'ClarkNet', 'USask', 'Calgary']

def lr(D):
    p = U/'baselines'/'logreducer'/D/'result.json'
    if not p.exists(): return None
    r = json.loads(p.read_text())
    return {'status': r['status'], 'lossless': r['status'] == 'PASS' and r['full_sha_match'],
            'archive_bytes': r['archive_bytes'], 'ratio': r['compression_ratio'], 'suffix_ratio': r['suffix_ratio'],
            'blocks_sha_pass': r['blocks_sha_pass'], 'blocks_sha_fail': r['blocks_sha_fail'],
            'native_bytes': r['native_bytes'], 'native_ratio': r['native_compression_ratio'], 'native_status': r['native_status'],
            'residual_bytes': r['residual_bytes'], 'residual_full_fallback_blocks': r['residual_full_fallback_blocks'],
            'source': str(p)}

def ls(D):
    p = U/'baselines'/'logshrink'/'campaign.json'
    if not p.exists(): return None
    r = json.loads(p.read_text())['datasets'].get(D)
    if not r or 'repaired' not in r: return {'status': 'ERROR', 'lossless': False, 'error': (r or {}).get('error')}
    rep, nat = r['repaired'], r['native']
    return {'status': 'PASS' if rep['lossless'] else 'FAIL', 'lossless': rep['lossless'], 'archive_bytes': rep['archive_bytes'],
            'ratio': rep['ratio'], 'suffix_ratio': rep['suffix_ratio'], 'lossless_blocks': rep['lossless_blocks'],
            'blocks': r['blocks'], 'blocks_status_ok': r['blocks_status_ok'], 'residual_bytes': rep['residual_bytes'],
            'native_bytes': nat['archive_bytes'], 'native_ratio': nat['ratio'], 'native_lossless': nat['lossless'],
            'native_lossless_blocks': nat['lossless_blocks'], 'L': 5, 'source': str(p)}

out = {'datasets': {}, 'aggregate_vs_delog': S['aggregate_vs_delog']}
rows = []
for D in ORDER:
    d = S['datasets'][D]; sz = d['archive_bytes']
    opt = {'logreducer_R': lr(D), 'logshrink_R_L5': ls(D)}
    for k, v in opt.items():
        if v and v.get('archive_bytes') and v['lossless']:
            v['semzip_minus_this_bytes'] = sz - v['archive_bytes']; v['semzip_wins'] = sz < v['archive_bytes']
    out['datasets'][D] = dict(d, optional=opt)
    b = d['baselines']
    def cell(x): return f"{x['ratio']:.3f} / {x['suffix_ratio']:.3f}"
    def ocell(v):
        if not v: return 'not run'
        if not v['lossless']: return f"FAIL ({v['ratio']:.3f} not lossless)" if v.get('ratio') else 'FAIL'
        return f"{v['ratio']:.3f} / {v['suffix_ratio']:.3f}"
    rows.append(f"| {D} | {d['raw_bytes']:,} | {d['ratio']:.3f} / {d['suffix_ratio']:.3f} | {cell(b['delog'])} | "
                f"{b['delog']['semzip_minus_this_bytes']:+,} ({b['delog']['semzip_vs_this_pct']:+.2f} %) | {cell(b['loglite'])} | "
                f"{cell(b['xz9e'])} | {cell(b['zstd19'])} | {cell(b['xz6'])} | {cell(b['gzip6'])} | {cell(b['zstd3'])} | "
                f"{ocell(opt['logreducer_R'])} | {ocell(opt['logshrink_R_L5'])} |")
(U/'summary_all.json').write_text(json.dumps(out, indent=1) + '\n')
print('| D | raw B | SemZip full / suffix | DeLog | SemZip - DeLog B | LogLite-BL | xz9e | zstd19 | xz6 | gzip6 | zstd3 | LogReducer+R | LogShrink+R (L=5) |')
print('|---|---:|---|---|---:|---|---|---|---|---|---|---|---|')
print('\n'.join(rows))
a = S['aggregate_vs_delog']
print(f"\nvs DeLog: full wins {a['wins_vs_delog']}/{a['n_complete_vs_delog']}, suffix wins {a['suffix_wins_vs_delog']}/{a['n_complete_vs_delog']}, "
      f"total SemZip {a['total_semzip_bytes']:,} B vs DeLog {a['total_delog_bytes']:,} B (diff {a['total_bytes_difference_semzip_minus_delog']:+,} B, "
      f"{100*a['total_bytes_difference_semzip_minus_delog']/a['total_delog_bytes']:+.2f} %)")
