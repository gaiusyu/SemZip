#!/usr/bin/env python3
"""R75-EVO full-file extension (pre-registered continuation: window future-block saving >= 1%).
Same protocol constants and the same update pipeline as evo.py (imported); only the horizon changes to all 2113
Thunderbird blocks. p0 (pool) payloads/costs come from the formal pool run (identical bytes verified on blocks 0..101);
the empty program and newly published programs are encoded lazily per 100-block part through the unchanged guard.
Usage: evo_full.py split | run | assemble"""
import json, os, sys, shutil, time, itertools
from pathlib import Path
import evo
from evo import lib, log, jload, D, QG, R75
RAW = QG/'raw'/'Thunderbird.log'; SEG = R75/'segments'; NB = 2113
FORMAL = QG/'runs/formal/pool/Thunderbird'; STATE = R75/'state_full.json'

def part(k): return SEG/f'part_{k:03d}.log'
def split():
    if (SEG/'DONE').exists(): return
    shutil.rmtree(SEG, ignore_errors=True); lib.split_parts(RAW, SEG, 100); (SEG/'DONE').write_text('ok'); log('SPLIT done')
def block_file(a, b):
    """lines of blocks a..b-1 (must lie in one part)."""
    k = a // 100; assert (b - 1) // 100 == k
    p = R75/'ranges_full'/f'r{a:04d}_{b:04d}.log'
    if p.exists(): return p
    p.parent.mkdir(parents=True, exist_ok=True); tmp = p.with_suffix('.tmp')
    with open(part(k), 'rb') as f, open(tmp, 'wb') as w:
        for line in itertools.islice(f, (a - 100*k)*100000, (b - 100*k)*100000): w.write(line)
    os.replace(tmp, p); return p
evo.range_file = lambda a, b: block_file(a, b)   # pipeline() extracts its training block through this

def formal_costs():
    r = jload(FORMAL/'result.json'); af = r.get('archive_files') or r['encode']['archive_files']
    arch = FORMAL/'archive'; out = {}
    for blk in r['encode']['blocks']:
        i = blk['index']
        out[i] = {'total': af[f'chunk_{i}.tar.xz']['bytes'] + blk['semantic_archive_bytes'], 'chunk': str(arch/f'chunk_{i}.tar.xz'),
                  'sem': str(arch/'semantic'/f'block_{i:05d}.semantic.tar.xz'), 'raw_sha256': blk['raw_sha256']}
    return out

class Costs:
    def __init__(self, st):
        self.c = {'p0': formal_costs()}; self.st = st
    def get(self, pid, b):
        if pid not in self.c: self.c[pid] = {}
        if b not in self.c[pid]:
            k = b // 100
            if pid == 'empty': plan, pol = evo.EMPTY_PLAN, evo.EMPTY_POL
            else:
                e = next(e for e in self.st['bank'] + self.st.get('pending', []) if e['id'] == pid); plan, pol = e['plan'], e['policy']
            label = f'full_{pid}_part{k:03d}'; out = R75/'enc'/label; cj = out/'costs.json'
            if not cj.exists():
                if out.exists(): shutil.rmtree(out)
                out.mkdir(parents=True); t = time.time()
                s = lib._encode_child(part(k), Path(plan), Path(pol), out/'encode', D, 4); arch = Path(s['archive_dir']); cs = {}
                for blk in s['blocks']:
                    i = blk['index']; g = 100*k + i
                    ch = arch/f'chunk_{i}.tar.xz'; se = arch/'semantic'/f'block_{i:05d}.semantic.tar.xz'
                    cs[g] = {'total': ch.stat().st_size + se.stat().st_size, 'chunk': str(ch), 'sem': str(se), 'raw_sha256': blk['raw_sha256']}
                lib.save(out/'meta.json', {'pid': pid, 'plan': str(plan), 'plan_sha256': lib.sha(plan), 'policy_sha256': lib.sha(pol),
                                            'part': k, 'encode_wall_seconds': time.time() - t})
                lib.save(cj, {str(g): v for g, v in cs.items()}); log('ENC', label, 'sec', round(time.time() - t))
            self.c[pid].update({int(g): v for g, v in jload(cj).items()})
        return self.c[pid][b]

def run():
    st = jload(STATE) if STATE.exists() else {'bank': [{'id': 'p0', 'from': 0, 'plan': str(evo.POOL_PLAN), 'policy': str(evo.POOL_POL)}],
        'attempts': [], 'choice': {}, 'next_block': 0, 'last_pub_block': 0, 'cool_until': -1}
    C = Costs(st)
    def loss(b):
        c = st['choice'][str(b)]; return c['chosen'] == 'empty' or c['best_plan_total'] > (1 - evo.LOSS_MARGIN) * c['empty_total']
    for b in range(st['next_block'], NB):
        av = [(C.get(e['id'], b)['total'], i, e['id']) for i, e in enumerate(st['bank']) if e['from'] <= b]
        bp, _, bid = min(av); ce = C.get('empty', b)['total']
        chosen = bid if bp <= ce else 'empty'
        st['choice'][str(b)] = {'chosen': chosen, 'best_plan': bid, 'best_plan_total': bp, 'empty_total': ce, 'chosen_total': min(bp, ce),
                                'bank_size': len(av)}
        if b >= 1:
            win = list(range(max(1, b - evo.LOOK + 1), b + 1))
            nloss = sum(1 for k in win if loss(k)) if len(win) >= evo.MINBLK else 0
            npub, natt = len(st['bank']) - 1, len(st['attempts'])
            ok = (len(win) >= evo.MINBLK and nloss >= evo.NEED and b - st['last_pub_block'] >= evo.GAP_PUB and b >= st['cool_until']
                  and npub < evo.MAXPUB and natt < evo.MAXATT and b + 2 <= NB - 1)
            st['choice'][str(b)]['trigger'] = {'window': win, 'losses': nloss, 'fired': ok}
            if ok:
                log('TRIGGER at block', b)
                plan, pol, info = evo.pipeline(b)
                att = {'block': b, 'outcome': info.get('outcome'), 'update_dir': str(R75/'upd'/f'b{b:03d}')}
                if plan is not None:
                    pid = f'p{len(st["bank"])}'; st['pending'] = [{'id': pid, 'from': b + 2, 'plan': str(plan), 'policy': str(pol), 'trained_on': b}]
                    cand = C.get(pid, b + 1)['total']
                    cur = min([C.get(e['id'], b + 1)['total'] for e in st['bank'] if e['from'] <= b + 1] + [C.get('empty', b + 1)['total']])
                    att.update(validation_block=b + 1, candidate_total=cand, current_min_total=cur, passed=cand <= (1 - evo.VAL_MARGIN) * cur)
                    if att['passed']:
                        st['bank'].append(st['pending'][0]); st['last_pub_block'] = b + 2; att.update(published_as=pid, effective_from=b + 2)
                    else:
                        st['cool_until'] = b + evo.COOL_REJ; C.c.pop(pid, None)
                    st['pending'] = []
                else:
                    st['cool_until'] = b + evo.COOL_REJ
                st['attempts'].append(att); log('ATTEMPT', json.dumps(att))
        st['next_block'] = b + 1
        if b % 25 == 0 or b == NB - 1: lib.save(STATE, st)
    lib.save(STATE, st); log('RUN DONE attempts', len(st['attempts']), 'published', len(st['bank']) - 1)

def assemble():
    st = jload(STATE); C = Costs(st); asm = R75/'asm_full'
    if asm.exists(): shutil.rmtree(asm)
    arch = asm/'archive'; (arch/'semantic').mkdir(parents=True)
    for b in range(NB):
        c = C.get(st['choice'][str(b)]['chosen'], b)
        os.link(c['chunk'], arch/f'chunk_{b}.tar.xz'); os.link(c['sem'], arch/'semantic'/f'block_{b:05d}.semantic.tar.xz')
    shutil.copy(FORMAL/'archive'/'semantic_manifest.json', arch/'semantic_manifest.json')
    total = sum(p.stat().st_size for p in arch.rglob('*') if p.is_file())
    restored = asm/'restored.log'; t = time.time(); lib.decode(arch, restored, asm/'decode', 4)
    ok = lib.sha(restored) == jload(FORMAL/'result.json')['encode']['raw_sha256']; restored.unlink()
    r = {'archive_bytes': total, 'sha_pass': ok, 'decode_seconds': time.time() - t, 'raw_bytes': jload(FORMAL/'result.json')['encode']['raw_bytes']}
    r['ratio'] = r['raw_bytes'] / total; lib.save(asm/'assemble_result.json', r); log('ASSEMBLE', json.dumps(r))

if __name__ == '__main__':
    {'split': split, 'run': run, 'assemble': assemble}[sys.argv[1]]()
