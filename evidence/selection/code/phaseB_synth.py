"""Phase B (gateway API): independent first-block syntheses with the unchanged R69 trainer/prompts/sampler/budget.
Jobs: T=0.7 k=1..K for every dataset (pool candidates c1..cK), then one T=0 repeat (diagnostic only, not pooled).
Failures are retained (no automatic retry): an existing output dir without training.json is recorded as incomplete.
Usage: phaseB_synth.py LANES K"""
import json, subprocess, sys, time, threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
LANES, K = int(sys.argv[1]), int(sys.argv[2])
DS = 'Windows,Zookeeper,OpenSSH,Hadoop,Apache,Thunderbird,Spark,Mac,BGL,HealthApp,HDFS,Linux,Android,Proxifier,HPC,OpenStack'.split(',')
jobs = [(d, '0.7', k) for k in range(1, K + 1) for d in DS] + [(d, '0.0', 1) for d in DS]
status_path = Path('runs/phaseB_status.json'); lock = threading.Lock()
status = {'started': time.time(), 'jobs': len(jobs), 'done': {}, 'skipped': {}}
def save():
    tmp = status_path.with_suffix('.tmp'); tmp.write_text(json.dumps(status, indent=1)); tmp.replace(status_path)
def run(job):
    d, t, k = job; out = Path(f'train_k/{d}/t{t}_k{k}'); key = f'{d}/t{t}_k{k}'
    if (out / 'training.json').exists():
        r = json.loads((out / 'training.json').read_text())
        with lock: status['done'][key] = {'status': r.get('status'), 'api_calls': r.get('api_calls'), 'preexisting': True}; save()
        return
    if out.exists():
        with lock: status['skipped'][key] = 'incomplete earlier attempt retained; not retried'; save()
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with open(f'logs/synth_{d}_t{t}_k{k}.log', 'w') as log:
        p = subprocess.run([sys.executable, '-u', 'train_k.py', d, t, str(out)], stdout=log, stderr=subprocess.STDOUT)
    r = json.loads((out / 'training.json').read_text()) if (out / 'training.json').exists() else {}
    with lock:
        status['done'][key] = {'rc': p.returncode, 'status': r.get('status'), 'api_calls': r.get('api_calls'),
                               'replay_plan': r.get('replay_plan'), 'seconds': round(time.time() - t0, 1)}
        save()
    print('SYNTH', key, p.returncode, r.get('status'), r.get('api_calls'), round(time.time() - t0), flush=True)
save()
with ThreadPoolExecutor(LANES) as ex:
    list(ex.map(run, jobs))
status['finished'] = time.time(); save(); print('[PHASE B DONE]', flush=True)
