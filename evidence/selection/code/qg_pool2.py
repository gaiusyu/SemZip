#!/usr/bin/env python3
"""QG-POOL-V2: cost-guided selection from K independent LLM syntheses (training block 0 only).

candidates : per-candidate QG-V1 plans (each already compile-checked and pruned on block 0)
start      : the candidate with the smallest complete training-block archive (ties -> lower candidate index)
moves      : add a rule group from another candidate, replace an overlapping start-plan group, remove a group
search     : batch-greedy rounds -- evaluate every move in parallel, then apply improving moves in gain order,
             re-evaluating each on the updated plan and accepting only strict improvements
finish     : generic compile variants, then backward elimination (same Stage B rule as QG-V1)
objective  : complete archive bytes of block 0 (native residual + semantic + manifest), every evaluation SHA-verified
No LLM call, no held-out data, no dataset-specific switch. Budget limits are fixed for all datasets.
Usage: qg_pool2.py DATASET CANDS_JSON TRAIN WORKDIR [THREADS]
CANDS_JSON = [{"cid": "c0", "plan": path, "train_cost": int}, ...]"""
import sys, json, copy, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import lib
from qg_select import Evaluator, remove_tag, variants
from qg_pool import groups, add_group, spans, overlap

MAX_ROUNDS, MAX_EVALS = 4, 600
SIG_KEYS = ('pattern', 'program', 'kind', 'semantic_class', 'value_type', 'store_group', 'context_group',
            'context_policy', 'context_ref', 'full_match_groups', 'general', 'order_hint', 'replacement')

def group_sig(g):
    specs = sorted(json.dumps({k: s.get(k) for k in SIG_KEYS}, sort_keys=True) for s in g['specs'])
    return json.dumps([specs, g['global'], sorted(json.dumps(f) for f in g['families'])])

def apply_move(cur, move, pool):
    cg = groups(cur)
    if move[0] == 'add':
        t = move[1]
        return None if t in cg else add_group(cur, t, pool[t])
    if move[0] == 'remove':
        b = move[1]
        return None if b not in cg else remove_tag(cur, b)
    _, b, t = move
    return None if (b not in cg or t in cg) else add_group(remove_tag(cur, b), t, pool[t])

def label(m): return m[0] + ':' + '->'.join(m[1:])

def main(ds, cands, train, work, threads=4):
    work = Path(work); work.mkdir(parents=True, exist_ok=True)
    ev = Evaluator(ds, train, work)
    def pmap(items):
        """Evaluate distinct plans in parallel (identical plans share one cache key; never race on one directory)."""
        if not items: return []
        from qg_select import canon
        uniq = {}
        for lab, plan in items: uniq.setdefault(canon(plan), (lab, plan))
        with ThreadPoolExecutor(threads) as ex:
            list(ex.map(lambda it: ev.cost(it[1], it[0]), list(uniq.values())))
        return [ev.cost(plan, lab) for lab, plan in items]  # cache hits
    cands = sorted(cands, key=lambda c: (c['train_cost'], int(c['cid'][1:])))
    start = cands[0]
    cur = json.loads(Path(start['plan']).read_text())
    best = ev.cost(cur, f'start:{start["cid"]}')
    start_reproduced = (best == start['train_cost'])
    base_sigs = {group_sig(g) for g in groups(cur).values()}
    pool, seen, dup = {}, set(base_sigs), 0
    for c in cands[1:]:
        P = json.loads(Path(c['plan']).read_text())
        for t, g in groups(P).items():
            sg = group_sig(g)
            if sg in seen: dup += 1; continue
            key = f'X{c["cid"][1:]}{t}'
            while key in pool or key in groups(cur): key += '_'
            seen.add(sg); pool[key] = g
    lines = Path(train).read_bytes().decode('latin-1').split('\n')[:5000]
    psp = {t: set().union(*[spans(s['pattern'], lines) for s in g['specs']]) for t, g in pool.items()}
    log = []
    for rnd in range(MAX_ROUNDS):
        if ev.n >= MAX_EVALS: log.append({'round': rnd, 'stop': 'eval budget'}); break
        cg = groups(cur)
        csp = {b: set().union(*[spans(s['pattern'], lines) for s in g['specs']]) for b, g in cg.items()}
        moves = [('add', t) for t in pool if t not in cg]
        moves += [('replace', b, t) for t in pool if t not in cg for b in cg if b not in pool and overlap(csp[b], psp[t])]
        moves += [('remove', b) for b in cg]
        valid = [(m, p) for m, p in ((m, apply_move(cur, m, pool)) for m in moves) if p is not None]
        costs = pmap([(f'R{rnd}:{label(m)}', p) for m, p in valid])
        scored = sorted([(c, label(m), m) for (m, _), c in zip(valid, costs)], key=lambda x: (x[0], x[1]))
        improving = [x for x in scored if x[0] < best]
        entry = {'round': rnd, 'n_moves': len(valid), 'n_improving': len(improving), 'before': best, 'accepted': []}
        if not improving:
            entry['after'] = best; log.append(entry); break
        for c, lab, m in improving:
            if ev.n >= MAX_EVALS: entry['stop'] = 'eval budget'; break
            p = apply_move(cur, m, pool)
            if p is None: continue
            c2 = ev.cost(p, f'R{rnd}:apply:{label(m)}')
            if c2 < best:
                cur, best = p, c2; entry['accepted'].append({'move': label(m), 'screen_cost': c, 'applied_cost': c2})
        entry['after'] = best; log.append(entry)
        if not entry['accepted']: break
    # finish 1: compile variants (sequential acceptance, speculative parallel pre-evaluation)
    pend = [(i, n, v) for i in range(len(cur['specs'])) for n, v in variants(cur['specs'][i])]
    pre = []
    for (i, n, v) in pend:
        cand = copy.deepcopy(cur); cand['specs'][i] = v; pre.append((f'F:A:{cur["specs"][i]["tag"]}:{n}', cand))
    pmap(pre)
    for (i, n, v) in pend:
        cand = copy.deepcopy(cur); cand['specs'][i] = v
        c = ev.cost(cand, f'F:A:{cur["specs"][i]["tag"]}:{n}')
        log.append({'stage': 'compile', 'tag': cur['specs'][i]['tag'], 'variant': n, 'before': best, 'after': c, 'accepted': c < best})
        if c < best: cur, best = cand, c
    # finish 2: backward elimination (same rule as QG-V1 Stage B)
    while ev.n < MAX_EVALS:
        tags = sorted({s['tag'] for s in cur['specs']})
        if not tags: break
        cl = [(f'F:B:-{t}', remove_tag(cur, t)) for t in tags]
        cs = pmap(cl)
        c, t, p = min([(c, t, p) for c, t, (_, p) in zip(cs, tags, cl)], key=lambda x: (x[0], x[1]))
        log.append({'stage': 'eliminate', 'tag': t, 'before': best, 'after': c, 'accepted': c < best})
        if c < best: cur, best = p, c
        else: break
    out = work / 'selected_plan.json'; out.write_text(json.dumps(cur, indent=1))
    rep = {'version': 'QG-POOL-V2', 'dataset': ds, 'candidates': cands, 'start': start['cid'], 'start_train_bytes': start['train_cost'], 'start_cost_reproduced': start_reproduced,
           'pool_groups': len(pool), 'duplicate_groups_skipped': dup, 'selected_train_bytes': best,
           'selected_tags': [s['tag'] for s in cur['specs']], 'selected_plan_sha256': lib.sha(out),
           'rounds': log, 'evaluations': ev.n, 'max_rounds': MAX_ROUNDS, 'max_evals': MAX_EVALS, 'llm_calls': 0,
           'training_block_sha256': lib.sha(train)}
    lib.save(work / 'pool_report.json', rep)
    return rep

if __name__ == '__main__':
    ds, cj, train, work = sys.argv[1:5]
    th = int(sys.argv[5]) if len(sys.argv) > 5 else 4
    t0 = time.time()
    r = main(ds, json.loads(Path(cj).read_text()), train, work, th)
    print('POOL', ds, 'start', r['start'], r['start_train_bytes'], '->', r['selected_train_bytes'], 'pool', r['pool_groups'],
          'dup', r['duplicate_groups_skipped'], 'evals', r['evaluations'], 'sec', round(time.time() - t0))
