#!/usr/bin/env python3
"""QG-V1 with parallel evaluation (identical decisions to qg_select.select; only evaluation order/concurrency differ).
Stage A compile variants, Stage B best-improvement backward elimination over tags, Stage C interaction re-add.
Objective: complete training-block archive bytes (native residual + semantic + manifest), each evaluation SHA-verified.
Usage: qg_select2.py DATASET PLAN TRAIN WORKDIR [THREADS]"""
import sys, json, copy, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import lib
from qg_select import Evaluator, remove_tag, restore_tag, variants

def select(dataset, plan_path, train, work, threads=4):
    work = Path(work); work.mkdir(parents=True, exist_ok=True)
    original = json.loads(Path(plan_path).read_text())
    ev = Evaluator(dataset, train, work)
    def pmap(items):
        """Evaluate distinct plans in parallel (identical plans share one cache key; never race on one directory)."""
        if not items: return []
        from qg_select import canon
        uniq = {}
        for lab, plan in items: uniq.setdefault(canon(plan), (lab, plan))
        with ThreadPoolExecutor(threads) as ex:
            list(ex.map(lambda it: ev.cost(it[1], it[0]), list(uniq.values())))
        return [ev.cost(plan, lab) for lab, plan in items]  # cache hits
    empty = copy.deepcopy(original)
    for t in sorted({s['tag'] for s in original['specs']}): empty = remove_tag(empty, t)
    c0, ce = pmap([('original', copy.deepcopy(original)), ('empty_residual_only', empty)])
    decisions = []; cur = copy.deepcopy(original); best = c0
    # Stage A: sequential acceptance semantics preserved; variants of a spec depend on the current plan only through
    # that spec, so every (spec, variant) candidate built from the current plan is pre-evaluated in parallel only when
    # no earlier variant has been accepted (otherwise the remaining ones are re-evaluated on the updated plan).
    i = 0
    pending = [(i, name, v) for i in range(len(cur['specs'])) for name, v in variants(cur['specs'][i])]
    pre = {}
    if pending:
        items = []
        for (i, name, v) in pending:
            cand = copy.deepcopy(cur); cand['specs'][i] = v; items.append((f'A:{cur["specs"][i]["tag"]}:{name}', cand))
        for (key, c) in zip(pending, pmap(items)): pre[(key[0], key[1])] = c
    changed = False
    for (i, name, v) in pending:
        cand = copy.deepcopy(cur); cand['specs'][i] = v
        c = ev.cost(cand, f'A:{cur["specs"][i]["tag"]}:{name}')  # cached unless an earlier variant was accepted
        decisions.append({'stage': 'A', 'spec_index': i, 'tag': cur['specs'][i]['tag'], 'variant': name,
                          'before': best, 'after': c, 'accepted': c < best})
        if c < best: cur, best = cand, c
    # Stage B
    removed = []
    while True:
        tags = sorted({s['tag'] for s in cur['specs']})
        if not tags: break
        cands = [(f'B:-{t}', remove_tag(cur, t)) for t in tags]
        costs = pmap(cands)
        trials = [(c, t, cand) for c, t, (_, cand) in zip(costs, tags, cands)]
        c, t, cand = min(trials, key=lambda x: (x[0], x[1]))
        decisions.append({'stage': 'B', 'tag': t, 'before': best, 'after': c, 'accepted': c < best,
                          'all': {tt: cc for cc, tt, _ in trials}})
        if c < best: cur, best = cand, c; removed.append(t)
        else: break
    # Stage C (sequential semantics: each re-add is tested on the then-current plan)
    for t in list(removed):
        cand = restore_tag(cur, original, t)
        c = ev.cost(cand, f'C:+{t}')
        decisions.append({'stage': 'C', 'tag': t, 'before': best, 'after': c, 'accepted': c < best})
        if c < best: cur, best = cand, c; removed.remove(t)
    out = work / 'selected_plan.json'; out.write_text(json.dumps(cur, indent=1))
    report = {'version': 'QG-V1', 'implementation': 'qg_select2 (parallel evaluation, same decisions)', 'dataset': dataset,
              'input_plan': str(plan_path), 'input_plan_sha256': lib.sha(plan_path),
              'training_block_sha256': lib.sha(train), 'selected_plan_sha256': lib.sha(out),
              'original_train_archive_bytes': c0, 'empty_train_archive_bytes': ce,
              'selected_train_archive_bytes': best, 'removed_tags': removed,
              'kept_tags': [s['tag'] for s in cur['specs']], 'evaluations': ev.n,
              'decisions': decisions, 'eval_log': ev.log, 'llm_calls': 0}
    lib.save(work / 'qg_report.json', report)
    return report

if __name__ == '__main__':
    ds, plan, train, work = sys.argv[1:5]
    th = int(sys.argv[5]) if len(sys.argv) > 5 else 4
    t = time.time()
    r = select(ds, Path(plan), Path(train), Path(work), th)
    print('QG', ds, r['original_train_archive_bytes'], '->', r['selected_train_archive_bytes'], 'removed', r['removed_tags'],
          'evals', r['evaluations'], 'sec', round(time.time() - t))
