"""Phase E: stability evaluation on a fixed held-out block sample (never seen by synthesis/selection).
Sample: up to 8 evenly spaced original 100k-line blocks from blocks 1..n-1 (all of them when n<=9), concatenated in order;
blocks are compressed independently, so the sample archive = sum of those blocks' payloads + one manifest.
Every (candidate, variant) uses its own block-0-fitted fixed policy (the policy produced by the selection evaluation).
Usage: heldout_eval.py JOBS WORKERS_PER_JOB DS1,DS2,...  (variants: raw/qg1 for c0..c4,g1 and pool when available)"""
import json, sys, shutil, hashlib, threading, traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import lib
from qg_select import canon
REF = Path('<WORKSPACE>/r71_strict_main_20260923/reference.json')
NBLK = {r['dataset']: int(r['blocks']) for r in json.loads(REF.read_text())}
LOCK = threading.Lock()

def sample_indices(n, m=8):
    k = min(m, n - 1)
    if k <= 0: return []
    if k == 1: return [1]
    return sorted({round(1 + i * (n - 2) / (k - 1)) for i in range(k)})

def make_sample(d):
    js = Path('heldout')/f'{d}.sample.json'
    if js.exists(): return json.loads(js.read_text())
    idx = sample_indices(NBLK[d]); want = set(idx)
    if not idx:
        info = {'dataset': d, 'blocks': NBLK[d], 'indices': [], 'note': 'single-block file: in-sample only'}
        lib.save(js, info); return info
    out = Path('heldout')/f'{d}.sample.log'; tmp = out.with_suffix('.tmp'); h = hashlib.sha256(); lines = size = 0
    with open(Path('raw')/(d + '.log'), 'rb') as f, open(tmp, 'wb') as w:
        b = 0
        while b <= max(idx):
            keep = b in want
            for _ in range(100000):
                line = f.readline()
                if not line: break
                if keep: w.write(line); h.update(line); lines += 1; size += len(line)
            b += 1
    tmp.replace(out)
    info = {'dataset': d, 'blocks': NBLK[d], 'indices': idx, 'sample_lines': lines, 'sample_bytes': size, 'sample_sha256': h.hexdigest()}
    lib.save(js, info); return info

def cand_raw_plan(d, cid):
    if cid == 'c0': return lib.ART/'deployments'/d/'program.json'
    t, k = ('0.7', cid[1:]) if cid.startswith('c') else ('0.0', cid[1:])
    tj = Path(f'train_k/{d}/t{t}_k{k}/training.json')
    if not tj.exists(): return None
    r = json.loads(tj.read_text()); p = Path(str(r.get('replay_plan') or ''))
    return p if (r.get('status') == 'PASS' and p.is_file()) else 'EMPTY'

def policy_for(plan_path, workdirs):
    key = canon(json.loads(Path(plan_path).read_text()))[:12]
    for w in workdirs:
        p = Path(w)/f'eval_{key}'/'tc'/'storage'/'storage.json'
        if p.is_file(): return p
    return None

def run_one(d, cid, variant, workers):
    out = Path('runs/heldout')/f'{cid}_{variant}'/d
    if (out/'result.json').exists(): return json.loads((out/'result.json').read_text())
    info = make_sample(d)
    if not info['indices']: return None
    qdir = Path('runs/qg')/cid/d
    if variant == 'raw':
        plan = cand_raw_plan(d, cid)
        if plan is None: return None
        if plan == 'EMPTY':
            plan = Path('runs/heldout')/'empty_plans'/f'{d}.json'
            if not plan.exists(): lib.save(plan, {'version': 1, 'dataset': d, 'specs': []})
    elif variant == 'qg1':
        plan = qdir/'selected_plan.json'
        if not plan.exists(): return None
    else:
        plan = Path('runs/pool')/d/'selected_plan.json'; qdir = Path('runs/pool')/d
        if not plan.exists(): return None
    pol = policy_for(plan, [qdir, Path('runs/qg')/'c0'/d])
    if out.exists(): shutil.rmtree(out)
    out.mkdir(parents=True)
    if pol is None:
        pol = lib.fit(Path('train')/(d + '.block0.log'), plan, out/'fit')
    r = lib.roundtrip(Path('heldout')/f'{d}.sample.log', plan, pol, out/'rt', d, workers)
    res = {'dataset': d, 'candidate': cid, 'variant': variant, 'plan': str(plan), 'plan_sha256': lib.sha(plan),
           'policy': str(pol), 'policy_sha256': lib.sha(pol), 'sample': info, 'archive_bytes': r['archive_bytes'],
           'raw_bytes': r['raw_bytes'], 'ratio': r['compression_ratio'], 'sha_pass': r['sha_pass'],
           'semantic_fallback_blocks': r['semantic_fallback_blocks'], 'per_block': r['per_block'], 'manifest_bytes': r['manifest_bytes']}
    lib.save(out/'result.json', res)
    shutil.rmtree(out/'rt'/'encode', ignore_errors=True)
    return res

if __name__ == '__main__':
    J, W = int(sys.argv[1]), int(sys.argv[2]); DS = sys.argv[3].split(',')
    for d in DS: make_sample(d)
    jobs = [(d, c, v) for d in DS for c in ['c0', 'c1', 'c2', 'c3', 'c4', 'g1'] for v in ['raw', 'qg1']] + [(d, 'pool', 'pool') for d in DS]
    def go(j):
        try:
            r = run_one(*j, W)
            if r: print('HO', j[0], j[1], j[2], round(r['ratio'], 3), r['sha_pass'], flush=True)
        except Exception:
            print('HOFAIL', j, traceback.format_exc()[-800:], flush=True)
    with ThreadPoolExecutor(J) as ex: list(ex.map(go, jobs))
    print('[HELDOUT PASS DONE]', flush=True)
