#!/usr/bin/env python3
"""QG-POOL: select a frozen plan from a POOL of verified LLM proposals by training-block archive cost.

base plan  + extra proposal plans (e.g. K independent first-block syntheses)
moves      : add an extra tag-group, remove a tag-group, replace an overlapping base group by an extra one
objective  : complete archive bytes of training block 0 (native residual + semantic + manifest), SHA-verified
search     : best-improvement hill climbing, then the QG-V1 compile/elimination pass on the result.
"""
import sys, json, copy, re
from pathlib import Path
import lib
from qg_select import Evaluator, remove_tag, variants, batch_cost

def groups(plan):
    ep = plan.get('execution_plan', {})
    out = {}
    for s in plan['specs']:
        g = out.setdefault(s['tag'], {'specs': [], 'global': s['tag'] in ep.get('global_tags', []), 'families': []})
        g['specs'].append(s)
    for f in ep.get('local_families', []):
        for t in f.get('tags', []):
            if t in out: out[t]['families'].append(f['key'])
    return out

def add_group(plan, tag, g):
    p = copy.deepcopy(plan)
    for s in g['specs']:
        s = copy.deepcopy(s); s['tag'] = tag; p['specs'].append(s)
    p['placeholders'][tag] = f'<{tag}>'
    ep = p.setdefault('execution_plan', {})
    if g['global']: ep.setdefault('global_tags', []).append(tag)
    fams = ep.setdefault('local_families', [])
    for key in g['families']:
        for f in fams:
            if f['key'] == key: f['tags'].append(tag); break
        else: fams.append({'key': key, 'tags': [tag]})
    p['spec_count'] = len(p['specs'])
    return p

def spans(pattern, lines):
    try: rx = re.compile(pattern)
    except Exception: return set()
    out = set()
    for i, l in enumerate(lines):
        for m in rx.finditer(l): out.add((i, m.start(), m.end()))
    return out

def overlap(a, b):
    if not a or not b: return False
    # any shared character position counts; compare by line-level interval intersection
    by = {}
    for (i, s, e) in b: by.setdefault(i, []).append((s, e))
    hit = sum(1 for (i, s, e) in a if any(s < e2 and s2 < e for (s2, e2) in by.get(i, [])))
    return hit >= 0.1 * min(len(a), len(b))

def main(ds, base_path, extra_paths, train, work):
    work = Path(work); work.mkdir(parents=True, exist_ok=True)
    base = json.loads(Path(base_path).read_text())
    lines = Path(train).read_bytes().decode('latin-1').split('\n')[:5000]
    pool = {}
    for k, ep in enumerate(extra_paths):
        P = json.loads(Path(ep).read_text())
        for t, g in groups(P).items():
            if any(s['pattern'].startswith(('residual-', 'numeric-lattice:', 'slot-fission', 'line-transducer:')) for s in g['specs']):
                g['stage_residual'] = True
            pool[f'X{k}{t}'] = g
    ev = Evaluator(ds, train, work)
    cur = copy.deepcopy(base); best = ev.cost(cur, 'base')
    sp = {t: set().union(*[spans(s['pattern'], lines) for s in g['specs']]) for t, g in pool.items()}
    moves_log = []
    for it in range(30):
        cg = groups(cur)
        bs = {t: set().union(*[spans(s['pattern'], lines) for s in g['specs']]) for t, g in cg.items()}
        cands = []
        for t, g in pool.items():
            if t in cg: continue
            cands.append((f'add:{t}', add_group(cur, t, g)))
            for b in cg:
                if b.startswith('X'): continue
                if overlap(bs[b], sp[t]):
                    cands.append((f'replace:{b}->{t}', add_group(remove_tag(cur, b), t, g)))
        for b in cg:
            cands.append((f'remove:{b}', remove_tag(cur, b)))
        cs = batch_cost(ev, cands, 3)
        scored = [(c, lab, p) for c, (lab, p) in zip(cs, cands)]
        c, lab, p = min(scored, key=lambda x: (x[0], x[1]))
        moves_log.append({'iter': it, 'best_move': lab, 'before': best, 'after': c, 'accepted': c < best,
                          'n_candidates': len(cands)})
        print('ITER', it, lab, best, '->', c, flush=True)
        if c < best: cur, best = p, c
        else: break
    # compile variants on the final set
    for i in range(len(cur['specs'])):
        for name, v in variants(cur['specs'][i]):
            cand = copy.deepcopy(cur); cand['specs'][i] = v
            c = ev.cost(cand, f'A:{cur["specs"][i]["tag"]}:{name}')
            moves_log.append({'stage': 'compile', 'tag': cur['specs'][i]['tag'], 'variant': name, 'before': best, 'after': c, 'accepted': c < best})
            if c < best: cur, best = cand, c
    out = work / 'selected_plan.json'; out.write_text(json.dumps(cur, indent=1))
    lib.save(work / 'pool_report.json', {'version': 'QG-POOL-V1', 'dataset': ds, 'base': str(base_path), 'extras': [str(e) for e in extra_paths],
             'base_train_bytes': ev.cost(base, 'base'), 'selected_train_bytes': best, 'selected_tags': [s['tag'] for s in cur['specs']],
             'moves': moves_log, 'evaluations': ev.n, 'llm_calls': 0})
    print('POOL', ds, 'done', best)

if __name__ == '__main__':
    ds, base, extras, train, work = sys.argv[1], sys.argv[2], sys.argv[3].split(','), sys.argv[4], sys.argv[5]
    main(ds, base, extras, train, work)
