#!/usr/bin/env python3
"""QG-V1: offline quality gate for LLM-proposed reversible programs.

Input : an already verified replay plan (LLM proposals after the existing compiler/verifier)
        and the original first training block (same data the storage policy is fitted on).
Output: a frozen plan chosen ONLY by complete training-block archive cost
        (native residual + semantic streams + manifest), each evaluation SHA-verified.

Stage A  unified compile: for every spec, try the runtime's existing generic width/layout
         compilers (width_agnostic_numeric_replay_spec, generalized_timestamp_width_spec);
         keep a variant only if the complete archive gets smaller.
Stage B  archive-gain admission: backward elimination over tags; repeatedly drop the tag whose
         removal shrinks the complete archive most; stop when no removal helps (ties keep the rule).
Stage C  interaction check: try re-adding each dropped tag to the final set once.
No LLM call, no dataset-specific switch, no held-out data.
"""
import sys, json, copy, hashlib, time
from pathlib import Path
import lib

sys.path[:0] = [str(lib.RUNTIME), str(lib.SRC)]
import semzip_pure as sp  # runtime copy (same helpers used by the historical HealthApp diagnosis)
sys.path.insert(0, str(lib.SRC / 'trainer'))

def canon(plan):
    return hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()

def remove_tag(plan, tag):
    p = copy.deepcopy(plan)
    p['specs'] = [s for s in p['specs'] if s['tag'] != tag]
    p['placeholders'].pop(tag, None)
    ep = p.get('execution_plan', {})
    if 'global_tags' in ep:
        ep['global_tags'] = [t for t in ep['global_tags'] if t != tag]
    fams = []
    for f in ep.get('local_families', []):
        f = dict(f); f['tags'] = [t for t in f.get('tags', []) if t != tag]
        if f['tags']: fams.append(f)
    if 'local_families' in ep: ep['local_families'] = fams
    p['spec_count'] = len(p['specs'])
    return p

def restore_tag(plan, original, tag):
    """Re-insert every original spec/placeholder/routing entry of `tag` at its original position."""
    keep = {s['tag'] for s in plan['specs']} | {tag}
    p = copy.deepcopy(original)
    # start from original and remove everything not in keep, but keep plan's (possibly compiled) spec bodies
    for t in sorted({s['tag'] for s in original['specs']} - keep):
        p = remove_tag(p, t)
    cur = {}
    for s in plan['specs']: cur.setdefault(s['tag'], []).append(s)
    idx = {}
    out = []
    for s in p['specs']:
        if s['tag'] != tag and s['tag'] in cur:
            i = idx.get(s['tag'], 0); idx[s['tag']] = i + 1
            out.append(cur[s['tag']][i] if i < len(cur[s['tag']]) else s)
        else:
            out.append(s)
    p['specs'] = out
    p['spec_count'] = len(out)
    return p

def variants(spec):
    out = []
    cs = sp.spec_from_plan(spec)
    try:
        w = sp.width_agnostic_numeric_replay_spec(cs)
        if w is not cs:
            out.append(('width_agnostic', sp.spec_to_plan(w)))
    except Exception as e:
        pass
    try:
        g = sp.generalized_timestamp_width_spec(cs)
        if g is not None:
            out.append(('timestamp_width', sp.spec_to_plan(g)))
    except Exception:
        pass
    # keep untouched plan fields that spec_to_plan does not emit
    res = []
    for name, v in out:
        merged = dict(spec); merged.update(v)
        if json.dumps(merged, sort_keys=True) != json.dumps(spec, sort_keys=True):
            res.append((name, merged))
    return res

import threading
_LOCK = threading.Lock()

class Evaluator:
    def __init__(self, dataset, train, work):
        self.dataset, self.train, self.work = dataset, Path(train), Path(work)
        self.cfile = self.work / 'eval_cache.json'
        self.cache = json.loads(self.cfile.read_text()) if self.cfile.exists() else {}
        self.n = len(self.cache)
        self.log = []
    def cost(self, plan, label):
        key = canon(plan)
        if key in self.cache:
            e = self.cache[key]; self.log.append(dict(e, label=label, cached=True))
            return e['archive_bytes']
        with _LOCK: self.n += 1
        d = self.work / f'eval_{key[:12]}'
        if d.exists(): import shutil; shutil.rmtree(d)
        d.mkdir(parents=True)
        pf = d / 'plan.json'; pf.write_text(json.dumps(plan, indent=1))
        t = time.time()
        try:
            c, _ = lib.train_cost(self.train, pf, d / 'tc', self.dataset)
        except Exception as e:
            c = float('inf'); (d / 'error.txt').write_text(str(e)[-4000:])
        row = {'eval': self.n, 'key': key[:12], 'label': label, 'tags': [s['tag'] for s in plan['specs']],
               'archive_bytes': c if c != float('inf') else None, 'seconds': round(time.time() - t, 2)}
        if c == float('inf'): row['archive_bytes'] = 10**18
        with _LOCK:
            self.cache[key] = row
            tmp = self.cfile.with_suffix('.tmp'); tmp.write_text(json.dumps(self.cache)); tmp.replace(self.cfile)
        self.log.append(row); print(json.dumps(row), flush=True)
        c = row['archive_bytes']
        return c

def batch_cost(ev, items, par=3):
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(par) as ex:
        return list(ex.map(lambda it: ev.cost(it[1], it[0]), items))

def select(dataset, plan_path, train, work):
    work = Path(work); work.mkdir(parents=True, exist_ok=True)
    original = json.loads(Path(plan_path).read_text())
    ev = Evaluator(dataset, train, work)
    decisions = []
    cur = copy.deepcopy(original)
    c0 = ev.cost(cur, 'original'); best = c0
    empty = copy.deepcopy(original)
    for t in sorted({s['tag'] for s in original['specs']}): empty = remove_tag(empty, t)
    ce = ev.cost(empty, 'empty_residual_only')
    # Stage A
    for i in range(len(cur['specs'])):
        for name, v in variants(cur['specs'][i]):
            cand = copy.deepcopy(cur); cand['specs'][i] = v
            c = ev.cost(cand, f'A:{cur["specs"][i]["tag"]}:{name}')
            decisions.append({'stage': 'A', 'spec_index': i, 'tag': cur['specs'][i]['tag'], 'variant': name,
                              'before': best, 'after': c, 'accepted': c < best})
            if c < best: cur, best = cand, c
    # Stage B
    removed = []
    while True:
        tags = sorted({s['tag'] for s in cur['specs']})
        trials = []
        for t in tags:
            cand = remove_tag(cur, t)
            trials.append((ev.cost(cand, f'B:-{t}'), t, cand))
        if not trials: break
        c, t, cand = min(trials, key=lambda x: (x[0], x[1]))
        decisions.append({'stage': 'B', 'tag': t, 'before': best, 'after': c, 'accepted': c < best,
                          'all': {tt: cc for cc, tt, _ in trials}})
        if c < best:
            cur, best = cand, c; removed.append(t)
        else:
            break
    # Stage C
    for t in list(removed):
        cand = restore_tag(cur, original, t)
        c = ev.cost(cand, f'C:+{t}')
        decisions.append({'stage': 'C', 'tag': t, 'before': best, 'after': c, 'accepted': c < best})
        if c < best:
            cur, best = cand, c; removed.remove(t)
    out = work / 'selected_plan.json'
    out.write_text(json.dumps(cur, indent=1))
    report = {'version': 'QG-V1', 'dataset': dataset, 'input_plan_sha256': lib.sha(plan_path),
              'training_block_sha256': lib.sha(train), 'selected_plan_sha256': lib.sha(out),
              'original_train_archive_bytes': c0, 'empty_train_archive_bytes': ce,
              'selected_train_archive_bytes': best, 'removed_tags': removed,
              'kept_tags': [s['tag'] for s in cur['specs']], 'evaluations': ev.n,
              'decisions': decisions, 'eval_log': ev.log, 'llm_calls': 0}
    lib.save(work / 'qg_report.json', report)
    return report

if __name__ == '__main__':
    ds, plan, train, work = sys.argv[1:5]
    r = select(ds, Path(plan), Path(train), Path(work))
    print('QG', ds, r['original_train_archive_bytes'], '->', r['selected_train_archive_bytes'], 'removed', r['removed_tags'])
