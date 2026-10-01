#!/usr/bin/env python3
"""R76-B2 driver: LIBRARY-V1 proposer -> unchanged QG-V1 gate (qg_select2.py) -> publish.py (storage refit on block 0)
-> formal_full.py (complete-file encode, archive-only decode, per-block + full SHA). One candidate only, so QG-POOL-V2
pooling reduces to the gated plan (same as R73 version 'qg1c0'); the pool step is therefore not run.
Idempotent: a finished stage (result file present) is skipped; a stage that failed is recorded in runs/status/<D>.json and
is NOT retried automatically.  Datasets run strictly one after another.
Usage (cwd = this dir): run_library.py STAGES DS1,DS2,...   STAGES = comma list of train,gate,publish,formal"""
import json, os, subprocess, sys, time
from pathlib import Path
HERE = Path(__file__).resolve().parent
os.chdir(HERE)
R73 = Path('<WORKDIR>/r73_quality_gate_20260927/r73_qg')
os.environ.update(QG_HOME=str(HERE), QG_ART=str(R73/'art'), PYTHONHASHSEED='0', PYTHONDONTWRITEBYTECODE='1')
for k in list(os.environ):
    if k.startswith(('PARE_LLM_', 'YUNWU_')) or k.endswith('_API_KEY'): os.environ.pop(k)
GATE_THREADS = os.environ.get('R76_GATE_THREADS', '3')
VERSION = 'lib'

def log(*a): print(time.strftime('%m-%d %H:%M:%S'), *a, flush=True)
def jload(p): return json.loads(Path(p).read_text())
def status(d, **kv):
    p = HERE/'runs/status'/f'{d}.json'; p.parent.mkdir(parents=True, exist_ok=True)
    s = jload(p) if p.exists() else {}; s.update(kv); p.write_text(json.dumps(s, indent=1))
def run(cmd, logname):
    t = time.time()
    with open(HERE/'logs'/logname, 'w') as lf:
        rc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT).returncode
    return rc, time.time() - t

def train(d):
    out = HERE/'train_lib'/d; tj = out/'training.json'
    if tj.exists(): return jload(tj)
    if out.exists(): status(d, train='INCOMPLETE_EARLIER_ATTEMPT_NOT_RETRIED'); return None
    rc, sec = run([sys.executable, '-u', 'train_lib.py', d, str(out)], f'train_{d}.log')
    r = jload(tj) if tj.exists() else {'status': 'NO_TRAINING_JSON'}
    status(d, train=r.get('status'), train_rc=rc, train_seconds=round(sec, 1), api_calls=r.get('api_calls'),
           library_stats=r.get('library_stats'))
    log('TRAIN', d, rc, r.get('status'), 'api_calls', r.get('api_calls'), 'sec', round(sec, 1))
    return r

def gate(d, r):
    out = HERE/'runs/qg'/VERSION/d; rep = out/'qg_report.json'
    if rep.exists(): return jload(rep)
    if (out/'FAILED.txt').exists(): return None
    plan = Path(str((r or {}).get('replay_plan') or ''))
    if not r or r.get('status') != 'PASS' or not plan.is_file():
        out.mkdir(parents=True, exist_ok=True)
        (out/'FAILED.txt').write_text('training unusable: ' + json.dumps({k: (r or {}).get(k) for k in ('status', 'error')}))
        status(d, gate='TRAINING_UNUSABLE'); log('GATE-SKIP', d, 'training unusable'); return None
    rc, sec = run([sys.executable, '-u', 'qg_select2.py', d, str(plan), f'train/{d}.block0.log', str(out), GATE_THREADS], f'qg_{d}.log')
    if not rep.exists():
        (out/'FAILED.txt').write_text(f'rc={rc}; see logs/qg_{d}.log'); status(d, gate='FAILED', gate_rc=rc); log('GATE-FAIL', d); return None
    q = jload(rep)
    status(d, gate='PASS', gate_seconds=round(sec, 1), gate_evaluations=q['evaluations'],
           candidate_specs=len(jload(plan)['specs']), candidate_tags=len(q['kept_tags']) + len(q['removed_tags']),
           kept_tags=q['kept_tags'], removed_tags=q['removed_tags'], train_bytes_candidate=q['original_train_archive_bytes'],
           train_bytes_empty=q['empty_train_archive_bytes'], train_bytes_selected=q['selected_train_archive_bytes'])
    log('GATE', d, q['original_train_archive_bytes'], '->', q['selected_train_archive_bytes'], 'empty', q['empty_train_archive_bytes'],
        'kept', q['kept_tags'], 'removed', q['removed_tags'], 'evals', q['evaluations'], 'sec', round(sec))
    return q

def publish(d):
    pub = HERE/'runs/publish'/VERSION/d/'publication.json'
    if pub.exists(): return jload(pub)
    sel = HERE/'runs/qg'/VERSION/d
    rc, sec = run([sys.executable, 'publish.py', VERSION, d, str(sel/'selected_plan.json'), str(sel)], f'publish_{d}.log')
    if not pub.exists(): status(d, publish='FAILED', publish_rc=rc); log('PUBLISH-FAIL', d); return None
    p = jload(pub); status(d, publish='PASS', policy_identical_to_selection_fit=p.get('policy_identical_to_selection_fit'),
                           published_specs=p.get('spec_count'))
    log('PUBLISH', d, 'specs', p.get('spec_count'), 'policy_identical', p.get('policy_identical_to_selection_fit'))
    return p

def formal(d, p):
    res = HERE/'runs/formal'/VERSION/d/'result.json'
    if res.exists(): return jload(res)
    if (HERE/'runs/formal'/VERSION/d).exists():
        status(d, formal='INCOMPLETE_EARLIER_ATTEMPT_NOT_RETRIED'); return None
    pubd = HERE/'runs/publish'/VERSION/d
    rc, sec = run([sys.executable, '-u', 'formal_full.py', VERSION, d, str(pubd/'program.json'), p['storage']], f'formal_{d}.log')
    if not res.exists():
        status(d, formal='FAILED', formal_rc=rc); log('FORMAL-FAIL', d, rc); return None
    r = jload(res); e = r['encode']
    status(d, formal=r['status'], raw_bytes=e['raw_bytes'], archive_bytes=e['archive_bytes'], ratio=e['compression_ratio'],
           semantic_fallback_blocks=e.get('semantic_fallback_blocks'), full_sha_pass=r['independent_materialized_file_sha_pass'],
           raw_sha256=e['raw_sha256'], heldout=r['heldout'], formal_seconds=round(sec, 1))
    log('FORMAL', d, 'archive', e['archive_bytes'], 'raw', e['raw_bytes'], 'ratio', round(e['compression_ratio'], 3),
        'sha', r['independent_materialized_file_sha_pass'], 'sec', round(sec))
    return r

def run_dataset(d, stages):
    if True:
        r = train(d) if 'train' in stages else (jload(HERE/'train_lib'/d/'training.json') if (HERE/'train_lib'/d/'training.json').exists() else None)
        q = gate(d, r) if 'gate' in stages else None
        if 'publish' in stages and (HERE/'runs/qg'/VERSION/d/'qg_report.json').exists():
            p = publish(d)
            if 'formal' in stages and p: formal(d, p)

if __name__ == '__main__':
    stages = set(sys.argv[1].split(',')); DS = sys.argv[2].split(',')
    for d in DS:
        # per-dataset O_EXCL lock so that a second lane (e.g. publish,formal only) can run safely; a locked dataset is skipped
        lk = HERE/'runs/locks'/f'{d}.lock'; lk.parent.mkdir(parents=True, exist_ok=True)
        fd = None
        while fd is None:
            try: fd = os.open(str(lk), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if 'gate' not in stages: break      # publish/formal-only lane: leave it to the lane holding the lock
                time.sleep(30)                      # main lane: wait, never drop a dataset
        if fd is None: log('SKIP-LOCKED', d); continue
        os.write(fd, str(os.getpid()).encode()); os.close(fd)
        try: run_dataset(d, stages)
        finally: lk.unlink()
    log('[DRIVER DONE]', sorted(stages), DS)

