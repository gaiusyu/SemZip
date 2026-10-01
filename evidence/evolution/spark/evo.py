#!/usr/bin/env python3
"""R75-EVO: pre-registered Thunderbird evolution on window blocks 0..99 (DEV_DESIGN_R75_zh.md, written 2026-09-27 23:40 CST
before any R75 result). Plan bank + empty program, per-block min; byte-based trigger; each update = the unchanged main
training pipeline (5 fresh syntheses T=0 x1 + T=0.7 x4 -> QG-V1 -> QG-POOL-V2 -> storage fit) on the trigger block b;
validation on block b+1; publication effective from block b+2. Time is counted in blocks (synthesis latency ignored).
Usage (env QG_ART/QG_HOME/PYTHONHASHSEED; cwd = this dir):  evo.py prep | run | assemble | report"""
import json, os, sys, subprocess, shutil, time, hashlib, itertools
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
QG = Path(os.environ['QG_HOME']); sys.path.insert(0, str(QG)); import lib
R75 = Path(__file__).resolve().parent
D = 'Spark'
WIN = R75/'window'/'Thunderbird.w102.log'
NWIN, NALL = 100, 102
R69 = Path('<WORKSPACE>/r69_fresh_main_20260923')
R68 = Path('<WORKSPACE>/r68_external_20260923/first_pass/Thunderbird/delog/trial_001/attempt_001/result.json')
POOL_PLAN = QG/'runs/publish/pool/Spark/program.json'
POOL_POL = QG/'runs/publish/pool/Spark/storage/storage.json'
EMPTY_PLAN, EMPTY_POL = R75/'empty/extraction.json', R75/'empty/storage.json'
# ---- pre-registered protocol constants ----
LOOK, NEED, MINBLK = 5, 3, 3          # trigger: >=3 losses among the last 5 completed blocks (>=1), need >=3 blocks
LOSS_MARGIN = 0.01                    # loss: best published plan saves <1% vs the empty program on that block
GAP_PUB, COOL_REJ = 10, 10            # >=10 blocks since last publication; 10-block cooldown after a rejected attempt
VAL_MARGIN = 0.01                     # candidate must be >=1% below min(bank U empty) on block b+1
MAXPUB, MAXATT = 5, 8
SYN_THREADS_QG, POOL_THREADS = 1, 4
STATE = R75/'state.json'

def log(*a): print(time.strftime('%H:%M:%S'), *a, flush=True)
def jload(p): return json.loads(Path(p).read_text())

def range_file(a, b):
    p = R75/'ranges'/f'r{a:03d}_{b:03d}.log'
    if p.exists(): return p
    p.parent.mkdir(parents=True, exist_ok=True); tmp = p.with_suffix('.tmp')
    with open(WIN, 'rb') as f, open(tmp, 'wb') as w:
        for line in itertools.islice(f, a*100000, b*100000): w.write(line)
    os.replace(tmp, p); return p

def enc(label, plan, pol, a, b=NALL):
    """Encode window blocks a..b-1 with (plan, pol) through the unchanged frozen guard; keep archive; per-block bytes."""
    out = R75/'enc'/label; cj = out/'costs.json'
    if cj.exists(): return {int(k): v for k, v in jload(cj).items()}
    if out.exists(): shutil.rmtree(out)
    out.mkdir(parents=True); rf = range_file(a, b); t = time.time()
    s = lib._encode_child(rf, plan, pol, out/'encode', D, 4)
    arch = Path(s['archive_dir']); costs = {}
    for blk in s['blocks']:
        i = blk['index']; g = a + i
        chunk = arch/f'chunk_{i}.tar.xz'; sem = arch/'semantic'/f'block_{i:05d}.semantic.tar.xz'
        costs[g] = {'native': chunk.stat().st_size, 'semantic': sem.stat().st_size,
                    'total': chunk.stat().st_size + sem.stat().st_size, 'chunk': str(chunk), 'sem': str(sem),
                    'raw_sha256': blk['raw_sha256'], 'semantic_fallback': blk.get('semantic_guard', {}).get('fallback_used')}
    meta = {'label': label, 'plan': str(plan), 'plan_sha256': lib.sha(plan), 'policy': str(pol), 'policy_sha256': lib.sha(pol),
            'first_block': a, 'end_block': b, 'encode_wall_seconds': time.time() - t, 'archive_dir': str(arch)}
    lib.save(out/'meta.json', meta); lib.save(cj, {str(k): v for k, v in costs.items()})
    log('ENC', label, 'blocks', a, b, 'sec', round(time.time() - t))
    return costs

def prep():
    delog = {b['index']: b['archive_bytes'] for b in jload(R68)['blocks'] if b['index'] < NALL}
    lib.save(R75/'delog_costs.json', {str(k): v for k, v in delog.items()})
    ce = enc('empty', EMPTY_PLAN, EMPTY_POL, 0)
    cp = enc('pool', POOL_PLAN, POOL_POL, 0)
    formal = jload(QG/'runs/formal/pool/Thunderbird/result.json')
    af = formal.get('archive_files') or formal['encode']['archive_files']
    mism = [i for i in range(NALL) if cp[i]['native'] != af[f'chunk_{i}.tar.xz']['bytes']
            or cp[i]['semantic'] != formal['encode']['blocks'][i]['semantic_archive_bytes']]
    shas = [i for i in range(NALL) if cp[i]['raw_sha256'] != formal['encode']['blocks'][i]['raw_sha256']]
    rep = {'pool_vs_formal_byte_mismatch_blocks': mism, 'raw_sha_mismatch_blocks': shas,
           'window_totals': {k: sum(c[i]['total'] for i in range(NWIN)) for k, c in [('empty', ce), ('pool', cp)]},
           'delog_window_total': sum(delog[i] for i in range(NWIN)),
           'min_pool_empty_window_total': sum(min(cp[i]['total'], ce[i]['total']) for i in range(NWIN)),
           'empty_wins_blocks': sum(1 for i in range(NWIN) if ce[i]['total'] < cp[i]['total'])}
    lib.save(R75/'prep_report.json', rep); log('PREP', json.dumps(rep))

def pipeline(b):
    """Unchanged main training pipeline on block b. Returns (plan, policy, info) or (None, None, info)."""
    wd = R75/'upd'/f'b{b:03d}'; done = wd/'update_result.json'
    if done.exists():
        r = jload(done); return (Path(r['plan']) if r.get('plan') else None, Path(r['policy']) if r.get('policy') else None, r)
    (wd/'train').mkdir(parents=True, exist_ok=True); (wd/'r69root/inputs'/D).mkdir(parents=True, exist_ok=True)
    (wd/'logs').mkdir(exist_ok=True)
    tr = wd/'train'/f'{D}.block0.log'
    if not tr.exists(): shutil.copy(range_file(b, b + 1), tr)
    for link, target in [(wd/'r69root/source', R69/'source'), (wd/'r69root/inputs'/D/'train.log', tr)]:
        if not link.exists(): link.symlink_to(target)
    env = dict(os.environ, R69_ROOT=str(wd/'r69root'))
    t0 = time.time(); info = {'block': b, 'train_sha256': lib.sha(tr)}
    runs = [('c0', 0.0, 1)] + [(f'c{k}', 0.7, k) for k in range(1, 5)]
    def synth(r):
        cid, T, k = r; out = wd/'train_k'/D/f't{T}_k{k}'
        if not (out/'training.json').exists():
            if out.exists(): shutil.rmtree(out)
            with open(wd/'logs'/f'synth_{cid}.log', 'w') as lf:
                subprocess.run([sys.executable, '-u', str(QG/'train_k.py'), D, str(T), str(out)], env=env, stdout=lf, stderr=subprocess.STDOUT)
        tj = out/'training.json'
        return cid, (jload(tj) if tj.exists() else {'status': 'NO_TRAINING_JSON'})
    with ThreadPoolExecutor(5) as ex: syn = dict(ex.map(synth, runs))
    info['synthesis_seconds'] = time.time() - t0
    info['syntheses'] = {cid: {k: r.get(k) for k in ('status', 'api_calls', 'replay_plan', 'error', 'wall_seconds')} for cid, r in syn.items()}
    def gate(cid):
        r = syn[cid]; plan = Path(str(r.get('replay_plan') or '')); out = wd/'runs/qg'/cid/D
        if r.get('status') != 'PASS' or not plan.is_file(): return cid, None
        if not (out/'qg_report.json').exists():
            if out.exists(): shutil.rmtree(out)
            with open(wd/'logs'/f'qg_{cid}.log', 'w') as lf:
                subprocess.run([sys.executable, '-u', str(QG/'qg_select2.py'), D, str(plan), str(tr), str(out), str(SYN_THREADS_QG)],
                               stdout=lf, stderr=subprocess.STDOUT)
        if not (out/'qg_report.json').exists(): return cid, None
        q = jload(out/'qg_report.json')
        return cid, {'cid': cid, 'plan': str((out/'selected_plan.json').resolve()), 'train_cost': q['selected_train_archive_bytes']}
    t1 = time.time()
    with ThreadPoolExecutor(5) as ex: gated = dict(ex.map(gate, [r[0] for r in runs]))
    info['gate_seconds'] = time.time() - t1
    cands = [c for c in gated.values() if c]; info['usable_candidates'] = [c['cid'] for c in cands]
    info['gated_train_costs'] = {c['cid']: c['train_cost'] for c in cands}
    if not cands:
        info['outcome'] = 'NO_USABLE_CANDIDATE'; lib.save(done, info); return None, None, info
    pw = wd/'runs/pool'/D; t2 = time.time()
    if not (pw/'pool_report.json').exists():
        pw.mkdir(parents=True, exist_ok=True); cj = pw/'candidates.json'; cj.write_text(json.dumps(cands, indent=1))
        with open(wd/'logs'/'pool.log', 'w') as lf:
            subprocess.run([sys.executable, '-u', str(QG/'qg_pool2.py'), D, str(cj), str(tr), str(pw), str(POOL_THREADS)],
                           stdout=lf, stderr=subprocess.STDOUT)
    info['pool_seconds'] = time.time() - t2
    if not (pw/'pool_report.json').exists():
        info['outcome'] = 'POOL_FAILED'; lib.save(done, info); return None, None, info
    info['pool_report'] = {k: v for k, v in jload(pw/'pool_report.json').items() if not isinstance(v, (list, dict))}
    with open(wd/'logs'/'publish.log', 'w') as lf:
        subprocess.run([sys.executable, str(QG/'publish.py'), 'evo', D, 'runs/pool/Spark/selected_plan.json', 'runs/pool/Spark'],
                       cwd=wd, stdout=lf, stderr=subprocess.STDOUT)
    pub = wd/'runs/publish/evo'/D/'publication.json'
    if not pub.exists():
        info['outcome'] = 'PUBLISH_FAILED'; lib.save(done, info); return None, None, info
    p = jload(pub); info['publication'] = p
    info.update(outcome='CANDIDATE', plan=str(wd/'runs/publish/evo'/D/'program.json'), policy=p['storage'],
                total_update_seconds=time.time() - t0)
    lib.save(done, info); return Path(info['plan']), Path(info['policy']), info

def run():
    st = jload(STATE) if STATE.exists() else {'bank': [{'id': 'p0', 'enc': 'pool', 'from': 0, 'plan': str(POOL_PLAN), 'policy': str(POOL_POL)}],
                                              'attempts': [], 'choice': {}, 'next_block': 0, 'last_pub_block': 0, 'cool_until': -1}
    costs = {'empty': enc('empty', EMPTY_PLAN, EMPTY_POL, 0)}
    for e in st['bank']: costs[e['id']] = enc(e['enc'], Path(e['plan']), Path(e['policy']), 0 if e['id'] == 'p0' else e['from'] - 1)
    def best_plan(b):
        av = [(costs[e['id']][b]['total'], i, e['id']) for i, e in enumerate(st['bank']) if e['from'] <= b]
        return min(av)
    def loss(b):
        c = st['choice'][str(b)]; return c['chosen'] == 'empty' or c['best_plan_total'] > (1 - LOSS_MARGIN) * c['empty_total']
    for b in range(st['next_block'], NWIN):
        bp, _, bid = best_plan(b); ce = costs['empty'][b]['total']
        chosen = bid if bp <= ce else 'empty'
        st['choice'][str(b)] = {'chosen': chosen, 'best_plan': bid, 'best_plan_total': bp, 'empty_total': ce,
                                'chosen_total': min(bp, ce), 'bank_size': sum(1 for e in st['bank'] if e['from'] <= b)}
        trig = None
        if b >= 1:
            win = list(range(max(1, b - LOOK + 1), b + 1))
            nloss = sum(1 for k in win if loss(k)) if len(win) >= MINBLK else 0
            npub = len(st['bank']) - 1; natt = len(st['attempts'])
            ok = (len(win) >= MINBLK and nloss >= NEED and b - st['last_pub_block'] >= GAP_PUB and b >= st['cool_until']
                  and npub < MAXPUB and natt < MAXATT and b + 2 <= NWIN - 1)
            trig = {'window': win, 'losses': nloss, 'fired': ok}
            st['choice'][str(b)]['trigger'] = trig
            if ok:
                log('TRIGGER at block', b, trig)
                plan, pol, info = pipeline(b)
                att = {'block': b, 'outcome': info.get('outcome'), 'update_dir': str(R75/'upd'/f'b{b:03d}')}
                if plan is not None:
                    cid = f'cand_b{b:03d}'; cc = enc(cid, plan, pol, b + 1)
                    cur = min([costs[e['id']][b + 1]['total'] for e in st['bank'] if e['from'] <= b + 1] + [costs['empty'][b + 1]['total']])
                    att.update(validation_block=b + 1, candidate_total=cc[b + 1]['total'], current_min_total=cur,
                               passed=cc[b + 1]['total'] <= (1 - VAL_MARGIN) * cur)
                    if att['passed']:
                        pid = f'p{len(st["bank"])}'; costs[pid] = cc
                        st['bank'].append({'id': pid, 'enc': cid, 'from': b + 2, 'plan': str(plan), 'policy': str(pol), 'trained_on': b})
                        st['last_pub_block'] = b + 2; att['published_as'] = pid; att['effective_from'] = b + 2
                    else:
                        st['cool_until'] = b + COOL_REJ
                else:
                    st['cool_until'] = b + COOL_REJ
                st['attempts'].append(att); log('ATTEMPT', json.dumps(att))
        st['next_block'] = b + 1; lib.save(STATE, st)
    log('RUN DONE', 'attempts', len(st['attempts']), 'published', len(st['bank']) - 1)

def assemble():
    st = jload(STATE); asm = R75/'asm'
    if asm.exists(): shutil.rmtree(asm)
    arch = asm/'archive'; (arch/'semantic').mkdir(parents=True)
    labels = {'empty': 'empty', **{e['id']: e['enc'] for e in st['bank']}}
    for b in range(NWIN):
        c = jload(R75/'enc'/labels[st['choice'][str(b)]['chosen']]/'costs.json')[str(b)]
        shutil.copy(c['chunk'], arch/f'chunk_{b}.tar.xz'); shutil.copy(c['sem'], arch/'semantic'/f'block_{b:05d}.semantic.tar.xz')
    ref = jload(R75/'enc/pool/encode/archive/semantic_manifest.json')
    man = dict(ref, block_count=NWIN, semantic_archives=[f'semantic/block_{b:05d}.semantic.tar.xz' for b in range(NWIN)])
    if isinstance(ref.get('semantic_archives'), list) and ref['semantic_archives'] and not str(ref['semantic_archives'][0]).startswith('semantic/'):
        man['semantic_archives'] = [f'block_{b:05d}.semantic.tar.xz' for b in range(NWIN)]
    (arch/'semantic_manifest.json').write_text(json.dumps(man, sort_keys=True, separators=(',', ':')))
    total = sum(p.stat().st_size for p in arch.rglob('*') if p.is_file())
    restored = asm/'restored.log'; t = time.time()
    lib.decode(arch, restored, asm/'decode', 4)
    ok = lib.sha(restored) == lib.sha(range_file(0, NWIN)); restored.unlink()
    r = {'archive_bytes': total, 'sha_pass': ok, 'decode_seconds': time.time() - t, 'manifest': man}
    lib.save(asm/'assemble_result.json', r); log('ASSEMBLE', total, 'sha_pass', ok)

if __name__ == '__main__':
    {'prep': prep, 'run': run, 'assemble': assemble}[sys.argv[1]]()
