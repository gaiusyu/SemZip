#!/usr/bin/env python3
"""(c) measurement: per-block min(pool payload, SemZip empty-program payload) for full files (rules-compliant fallback;
no upstream DeLog regexes). Empty-program encode through the unchanged frozen guard; pool bytes from the formal run.
Usage: cfb.py DS1,DS2,...   -> cfb/<DS>.json"""
import json, os, sys, shutil, time
from pathlib import Path
QG = Path(os.environ['QG_HOME']); sys.path.insert(0, str(QG)); import lib
R75 = Path(__file__).resolve().parent
for d in sys.argv[1].split(','):
    out = R75/'cfb'/f'{d}.json'
    if out.exists(): continue
    ed = R75/'cfb_work'/d; shutil.rmtree(ed, ignore_errors=True); ed.mkdir(parents=True)
    plan, pol = ed/'extraction.json', ed/'storage.json'
    plan.write_text(json.dumps({'version': 1, 'dataset': d, 'specs': [], 'placeholders': {}}))
    pol.write_text(json.dumps({'version': 1, 'dataset': d, 'online_search': False, 'recipe': {'generic_representation': 'file_alias'},
        'programs': [], 'column_numeric': {}, 'numeric_modes': {}, 'subfield_ids': {}, 'unseen_column_numeric_default': 'delta'}))
    t = time.time(); s = lib._encode_child(QG/'raw'/f'{d}.log', plan, pol, ed/'encode', d, 4); sec = time.time() - t
    arch = Path(s['archive_dir'])
    f = json.loads((QG/'runs/formal/pool'/d/'result.json').read_text()); af = f.get('archive_files') or f['encode']['archive_files']
    rows = []
    for b, fb in zip(s['blocks'], f['encode']['blocks']):
        i = b['index']; assert b['raw_sha256'] == fb['raw_sha256']
        e = (arch/f'chunk_{i}.tar.xz').stat().st_size + (arch/'semantic'/f'block_{i:05d}.semantic.tar.xz').stat().st_size
        p = af[f'chunk_{i}.tar.xz']['bytes'] + fb['semantic_archive_bytes']
        rows.append({'index': i, 'pool': p, 'empty': e})
    man = af['semantic_manifest.json']['bytes']
    pool_total = sum(r['pool'] for r in rows) + man; mix_total = sum(min(r['pool'], r['empty']) for r in rows) + man
    raw = f['encode']['raw_bytes']
    res = {'dataset': d, 'blocks': len(rows), 'empty_wins': sum(1 for r in rows if r['empty'] < r['pool']),
           'pool_archive_bytes': pool_total, 'formal_pool_archive_bytes': f['encode']['archive_bytes'],
           'empty_archive_bytes': sum(r['empty'] for r in rows) + man, 'min_archive_bytes': mix_total,
           'pool_ratio': raw / pool_total, 'min_ratio': raw / mix_total, 'saving_pct': 100 * (1 - mix_total / pool_total),
           'empty_encode_seconds': sec, 'rows': rows}
    lib.save(out, res); shutil.rmtree(ed/'encode'/'archive', ignore_errors=True)
    print(d, 'blocks', len(rows), 'empty_wins', res['empty_wins'], 'pool', round(res['pool_ratio'], 2), '-> min', round(res['min_ratio'], 2),
          'saving%', round(res['saving_pct'], 3), 'sec', round(sec), flush=True)
