"""Phase C (0 API): QG-V1 on every fresh candidate as soon as its synthesis finishes.
Candidate ids: c{k} <- train_k/<D>/t0.7_k{k};  g1 <- train_k/<D>/t0.0_k1 (greedy repeat, diagnostic).
A failed/empty synthesis is recorded (empty-program candidate), never retried.
Usage: phaseC_follow.py THREADS"""
import json, os, subprocess, sys, time
from pathlib import Path
TH = sys.argv[1]
ORDER = sys.argv[2].split(',') if len(sys.argv) > 2 else None
ONLY = set(ORDER) if ORDER else None
def cands():
    rows = []
    for tj in Path('train_k').glob('*/t*_k*/training.json'):
        d = tj.parent.parent.name; tk = tj.parent.name  # t0.7_k1
        if ONLY and d not in ONLY: continue
        t, k = tk[1:].split('_k')
        cid = f'c{k}' if t == '0.7' else f'g{k}'
        # pooled candidates first (dataset priority order, then k), diagnostics (g*) last
        rows.append(((0 if t == '0.7' else 1), ORDER.index(d) if ORDER else 0, int(k), d, cid, tj))
    for _, _, _, d, cid, tj in sorted(rows, key=lambda r: r[:4]):
        yield d, cid, tj
while True:
    work_left = False
    for d, cid, tj in cands():
        out = Path('runs/qg')/cid/d
        if (out/'qg_report.json').exists() or (out/'CANDIDATE_UNUSABLE.json').exists() or (out/'FAILED.txt').exists(): continue
        r = json.loads(tj.read_text()); plan = Path(str(r.get('replay_plan') or ''))
        if r.get('status') != 'PASS' or not plan.is_file():
            out.mkdir(parents=True, exist_ok=True)
            (out/'CANDIDATE_UNUSABLE.json').write_text(json.dumps({'dataset': d, 'candidate': cid, 'training_status': r.get('status'),
                'replay_plan': str(plan), 'policy': 'retained as failed synthesis; empty-program candidate'}, indent=1))
            print('UNUSABLE', cid, d, r.get('status'), flush=True); continue
        out.mkdir(parents=True, exist_ok=True)
        try: fd = os.open(str(out/'LANE.lock'), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError: continue
        os.write(fd, str(os.getpid()).encode()); os.close(fd)
        work_left = True
        t0 = time.time()
        with open(f'logs/qgC_{cid}_{d}.log', 'w') as log:
            p = subprocess.run([sys.executable, '-u', 'qg_select2.py', d, str(plan), f'train/{d}.block0.log', str(out), TH],
                               stdout=log, stderr=subprocess.STDOUT)
        ok = (out/'qg_report.json').exists()
        if ok:
            q = json.loads((out/'qg_report.json').read_text())
            print('QGC', cid, d, q['original_train_archive_bytes'], '->', q['selected_train_archive_bytes'], 'removed', q['removed_tags'],
                  'evals', q['evaluations'], 'sec', round(time.time()-t0), flush=True)
        else:
            (out/'FAILED.txt').write_text(f'rc={p.returncode}; see logs/qgC_{cid}_{d}.log'); print('QGCFAIL', cid, d, flush=True)
        (out/'LANE.lock').unlink(missing_ok=True)
        break  # rescan (new candidates may have arrived; keeps FIFO-ish order)
    st = json.loads(Path('runs/phaseB_status.json').read_text()) if Path('runs/phaseB_status.json').exists() else {}
    pending = [c for c in cands() if not (Path('runs/qg')/c[1]/c[0]/'qg_report.json').exists() and not (Path('runs/qg')/c[1]/c[0]/'LANE.lock').exists()
               and not (Path('runs/qg')/c[1]/c[0]/'CANDIDATE_UNUSABLE.json').exists() and not (Path('runs/qg')/c[1]/c[0]/'FAILED.txt').exists()]
    if 'finished' in st and not pending:
        print('[PHASE C DONE]', flush=True); break
    if not work_left: time.sleep(30)
