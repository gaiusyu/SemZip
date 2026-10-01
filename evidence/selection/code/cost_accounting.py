"""Offline cost accounting for the K-synthesis campaign and cost-guided selection (reads retained records only).
Per synthesis: HTTP records, status codes, usage tokens, LiteLLM-reported cost header, training wall seconds.
Per selection: number of archive-cost evaluations and their summed seconds. Output: runs/COST.json"""
import json, glob
from pathlib import Path
out = {'syntheses': {}, 'selection': {}, 'pool': {}}
for tj in sorted(Path('train_k').glob('*/t*_k*/training.json')):
    d = tj.parent.parent.name; tag = tj.parent.name; t = json.loads(tj.read_text())
    recs = glob.glob(str(tj.parent/'llm_caches'/d/'shared'/'_api_records'/'*'/'*.http.json'))
    calls = ok = 0; tokens = {'prompt': 0, 'completion': 0, 'total': 0}; cost = 0.0
    for h in recs:
        calls += 1; x = json.loads(Path(h).read_text()); ok += int(x.get('http_status') == 200)
        try: cost += float(x.get('response_headers', {}).get('x-litellm-response-cost') or 0)
        except ValueError: pass
        rp = Path(h.replace('.http.json', '.response.json'))
        if rp.exists():
            try:
                u = json.loads(rp.read_text()).get('usage') or {}
                tokens['prompt'] += u.get('prompt_tokens', 0); tokens['completion'] += u.get('completion_tokens', 0); tokens['total'] += u.get('total_tokens', 0)
            except Exception: pass
    out['syntheses'][f'{d}/{tag}'] = {'status': t.get('status'), 'api_calls_reported': t.get('api_calls'), 'http_records': calls, 'http_200': ok,
                                     'tokens': tokens, 'litellm_cost_usd': round(cost, 4), 'wall_seconds': t.get('wall_seconds'),
                                     'temperature': t.get('temperature')}
for rep in sorted(Path('runs/qg').glob('*/*/qg_report.json')):
    r = json.loads(rep.read_text()); c = rep.parent.parent.name; d = rep.parent.name
    secs = sum(e.get('seconds', 0) for e in r['eval_log'] if not e.get('cached'))
    out['selection'][f'{c}/{d}'] = {'evaluations': r['evaluations'], 'eval_seconds': round(secs, 1)}
for rep in sorted(Path('runs/pool').glob('*/pool_report.json')):
    r = json.loads(rep.read_text()); d = rep.parent.name
    cache = json.loads((rep.parent/'eval_cache.json').read_text()) if (rep.parent/'eval_cache.json').exists() else {}
    out['pool'][d] = {'evaluations': r['evaluations'], 'eval_seconds': round(sum(v.get('seconds', 0) for v in cache.values()), 1)}
tot = {'syntheses': len(out['syntheses']), 'http_records': sum(v['http_records'] for v in out['syntheses'].values()),
       'tokens': sum(v['tokens']['total'] for v in out['syntheses'].values()),
       'litellm_cost_usd': round(sum(v['litellm_cost_usd'] for v in out['syntheses'].values()), 2),
       'synthesis_wall_seconds': round(sum(v['wall_seconds'] or 0 for v in out['syntheses'].values()), 1),
       'selection_evaluations': sum(v['evaluations'] for v in out['selection'].values()) + sum(v['evaluations'] for v in out['pool'].values()),
       'selection_eval_seconds': round(sum(v['eval_seconds'] for v in out['selection'].values()) + sum(v['eval_seconds'] for v in out['pool'].values()), 1)}
out['totals'] = tot
Path('runs/COST.json').write_text(json.dumps(out, indent=1)); print(json.dumps(tot, indent=1))
