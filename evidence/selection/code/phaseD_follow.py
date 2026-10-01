"""Phase D (0 API): QG-POOL-V2 per dataset once all pooled candidates (c0..cK) have QG-V1 results.
Pooled candidates are pre-registered: c0 (R69/R71 greedy synthesis) and c1..cK (T=0.7). g1 is never pooled.
Usage: phaseD_follow.py THREADS K DS_ORDER"""
import json, os, subprocess, sys, time
from pathlib import Path
TH, K = sys.argv[1], int(sys.argv[2]); DS = sys.argv[3].split(',')
def ready(d):
    cands = []
    for k in range(K + 1):
        q = Path('runs/qg')/f'c{k}'/d
        if (q/'qg_report.json').exists():
            r = json.loads((q/'qg_report.json').read_text())
            cands.append({'cid': f'c{k}', 'plan': str((q/'selected_plan.json').resolve()), 'train_cost': r['selected_train_archive_bytes']})
        elif (q/'CANDIDATE_UNUSABLE.json').exists() or (q/'FAILED.txt').exists():
            continue
        else:
            return None
    return cands
while True:
    left = [d for d in DS if not (Path('runs/pool')/d/'pool_report.json').exists() and not (Path('runs/pool')/d/'FAILED.txt').exists()]
    if not left: print('[PHASE D DONE]', flush=True); break
    ran = False
    for d in left:
        c = ready(d)
        if c is None: continue
        w = Path('runs/pool')/d; w.mkdir(parents=True, exist_ok=True)
        try: fd = os.open(str(w/'LANE.lock'), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError: continue
        os.write(fd, str(os.getpid()).encode()); os.close(fd)
        cj = w/'candidates.json'; cj.write_text(json.dumps(c, indent=1))
        t0 = time.time()
        with open(f'logs/pool_{d}.log', 'w') as log:
            p = subprocess.run([sys.executable, '-u', 'qg_pool2.py', d, str(cj), f'train/{d}.block0.log', str(w), TH], stdout=log, stderr=subprocess.STDOUT)
        if (w/'pool_report.json').exists():
            r = json.loads((w/'pool_report.json').read_text())
            print('POOLDONE', d, 'start', r['start'], r['start_train_bytes'], '->', r['selected_train_bytes'], 'evals', r['evaluations'],
                  'sec', round(time.time() - t0), 'reproduced', r['start_cost_reproduced'], flush=True)
        else:
            (w/'FAILED.txt').write_text(f'rc={p.returncode}'); print('POOLFAIL', d, flush=True)
        (w/'LANE.lock').unlink(missing_ok=True)
        ran = True; break
    if not ran: time.sleep(60)
