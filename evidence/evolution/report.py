#!/usr/bin/env python3
"""R75 report: full-file evolution (bank-min, pre-registered) vs frozen pool, frozen+fallback, latest-only variant, DeLog."""
import json, glob
from pathlib import Path
from collections import Counter
R75 = Path(__file__).resolve().parent
QG = Path('<WORKSPACE>/r73_quality_gate_20260927/r73_qg')
R68 = Path('<WORKSPACE>/r68_external_20260923/first_pass')
def J(p): return json.loads(Path(p).read_text())
out = {}
for d, base in [('Thunderbird', R75), ('Spark', R75/'spark_run')]:
    s = J(base/'state_full.json'); f = J(QG/'runs/formal/pool'/d/'result.json')
    af = f.get('archive_files') or f['encode']['archive_files']; man = af['semantic_manifest.json']['bytes']
    raw = f['encode']['raw_bytes']; n = len(f['encode']['blocks'])
    dl = J(R68/d/'delog/trial_001/attempt_001/result.json'); D = {b['index']: b['archive_bytes'] for b in dl['blocks']}
    cfb = J(R75/'cfb'/f'{d}.json'); rows = {r['index']: r for r in cfb['rows']}
    ch = {int(k): v for k, v in s['choice'].items()}
    # latest-only variant: after each publication use only the newest plan (no empty, no older plans); needs its per-block cost
    costs = {}
    for cj in glob.glob(str(base/'enc'/'full_p*_part*'/'costs.json')):
        pid = Path(cj).parent.name.split('_')[1]
        costs.setdefault(pid, {}).update({int(k): v['total'] for k, v in J(cj).items()})
    bank = s['bank']
    def newest(b):
        av = [e for e in bank if e['from'] <= b]; return av[-1]['id']
    latest = sum(rows[b]['pool'] if newest(b) == 'p0' else costs[newest(b)][b] for b in range(n)) + man
    evo = sum(v['chosen_total'] for v in ch.values()) + man
    frz = cfb['min_archive_bytes']; pool = f['encode']['archive_bytes']; delog = dl['archive_bytes']
    first = min((e['from'] for e in bank if e['id'] != 'p0'), default=None)
    fut = [b for b in range(n) if first is not None and b >= first]
    fe = sum(ch[b]['chosen_total'] for b in fut); ff = sum(min(rows[b]['pool'], rows[b]['empty']) for b in fut); fd = sum(D[b] for b in fut)
    asm = J(base/'asm_full'/'assemble_result.json') if (base/'asm_full'/'assemble_result.json').exists() else None
    upd = []
    for a in s['attempts']:
        u = J(Path(a['update_dir'])/'update_result.json')
        upd.append({'block': a['block'], 'passed': a.get('passed'), 'api_calls': sum((v.get('api_calls') or 0) for v in u['syntheses'].values()),
                    'update_seconds': round(u.get('total_update_seconds') or (u.get('synthesis_seconds', 0) + u.get('gate_seconds', 0) + u.get('pool_seconds', 0))),
                    'specs': [sp.get('tag') for sp in J(u['plan'])['specs']] if u.get('plan') else None})
    encsec = {}
    for m in glob.glob(str(base/'enc'/'full_*'/'meta.json')):
        mm = J(m); encsec[mm['pid']] = encsec.get(mm['pid'], 0) + mm['encode_wall_seconds']
    out[d] = {'blocks': n, 'raw_bytes': raw,
              'ratio': {'pool_frozen': raw/pool, 'frozen_plus_fallback': raw/frz, 'evolution_bankmin': raw/evo, 'evolution_latest_only': raw/latest, 'delog': raw/delog},
              'bytes': {'pool_frozen': pool, 'frozen_plus_fallback': frz, 'evolution_bankmin': evo, 'evolution_latest_only': latest, 'delog': delog},
              'evo_vs_frozen_fb_pct': 100*(evo/frz-1), 'evo_vs_delog_pct': 100*(evo/delog-1), 'latest_vs_frozen_fb_pct': 100*(latest/frz-1),
              'future_from_block': first, 'future_evo_vs_frozen_fb_pct': 100*(fe/ff-1) if fut else None, 'future_evo_vs_delog_pct': 100*(fe/fd-1) if fut else None,
              'blocks_evo_smaller_than_delog': sum(1 for b in range(n) if ch[b]['chosen_total'] < D[b]),
              'choices': dict(Counter(v['chosen'] for v in ch.values())), 'attempts': upd, 'published': len(bank) - 1,
              'extra_encode_seconds_by_plan': {k: round(v) for k, v in encsec.items()}, 'assembled': asm}
(R75/'R75_REPORT.json').write_text(json.dumps(out, indent=2) + '\n')
for d, r in out.items():
    print(d, {k: round(v, 2) for k, v in r['ratio'].items()}, 'evo vs frozen+fb %.2f%%' % r['evo_vs_frozen_fb_pct'], 'vs DeLog %.2f%%' % r['evo_vs_delog_pct'],
          'latest-only vs frozen+fb %.2f%%' % r['latest_vs_frozen_fb_pct'])
    print('   future from', r['future_from_block'], 'evo vs frozen+fb %.2f%%' % r['future_evo_vs_frozen_fb_pct'], 'vs DeLog %.2f%%' % r['future_evo_vs_delog_pct'],
          'blocks smaller than DeLog', r['blocks_evo_smaller_than_delog'], '/', r['blocks'], r['choices'])
    print('   attempts', r['attempts'])
    print('   extra encode sec', r['extra_encode_seconds_by_plan'], 'assembled', r['assembled'])
