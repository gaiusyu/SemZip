#!/usr/bin/env python3
"""R76-G optional baselines (design: "时间允许时再加 LogShrink+R（默认 L=5）和 LogReducer+R"), run only AFTER the
main queue (SemZip x4 + main baselines) has finished, so they never delay or share CPU with the main results.
Total worker budget stays <= 4: LogReducer+R --workers 2 and LogShrink+R --workers 2, run concurrently.

  LogReducer+R : baselines/logreducer/lr_run2.py, unchanged (its own --input/--output), output unseen/baselines/logreducer
  LogShrink+R  : unseen/baselines/logshrink_adapter/ls_run_unseen.py (R76-G copy of ls_run.py: paths + generic L=5 only),
                 LogShrink track venv/code read-only, output unseen/baselines/logshrink

Resumable: rerun the same command after a container restart (stale locks are moved aside only when the recorded pid is
not a live runner).  If the main queue has not finished by GIVE_UP, nothing is started and SKIPPED.json is written.
Usage (cwd = unseen/): optional_after.py"""
import calendar, json, os, subprocess, sys, threading, time
from pathlib import Path

U = Path('<WORKDIR>/r76_additional_20260929/unseen')
B = U.parent/'baselines'
GIVE_UP = calendar.timegm(time.strptime('2026-10-02 22:00', '%Y-%m-%d %H:%M'))   # UTC
ORDER = ['Calgary', 'ClarkNet', 'NASA', 'USask']
STATE = U/'baselines'/'optional_status.json'

def now(): return time.strftime('%Y-%m-%d %H:%M:%S')
def log(*a): print(now(), *a, flush=True)
LOCK = threading.Lock()
def save(d):
    with LOCK:
        t = STATE.with_suffix('.tmp'); t.write_text(json.dumps(d, indent=1) + '\n'); os.replace(t, STATE)

def main_queue_finished():
    try: q = json.loads((U/'queue_status.json').read_text())
    except Exception: return False
    return bool(q.get('finished'))

def clear_stale_lock(out, marker):
    lock = out/'RUNNING.lock'
    if not lock.exists(): return
    pid = json.loads(lock.read_text()).get('pid')
    try: alive = marker in Path(f'/proc/{pid}/cmdline').read_bytes().decode(errors='ignore')
    except Exception: alive = False
    if alive: raise SystemExit(f'{out}: runner pid {pid} still alive')
    lock.rename(out/f'RUNNING.lock.stale_{int(time.time())}')

def run(name, cmd, env, logf, st):
    t0 = time.time(); st[name] = {'cmd': cmd, 'started': now()}; save(st)
    with open(logf, 'a') as lf:
        lf.write(f'=== {now()} {" ".join(cmd)}\n'); lf.flush()
        rc = subprocess.run(cmd, cwd=str(U), env=env, stdout=lf, stderr=subprocess.STDOUT).returncode
    st[name].update(rc=rc, finished=now(), seconds=round(time.time() - t0)); save(st); log(name, 'rc', rc)

def main():
    st = json.loads(STATE.read_text()) if STATE.exists() else {}
    st['waiting_since'] = st.get('waiting_since') or now(); save(st)
    while not main_queue_finished():
        if time.time() > GIVE_UP:
            st['status'] = 'SKIPPED_TIME'; save(st); (U/'baselines'/'SKIPPED.json').write_text(json.dumps(
                {'at': now(), 'reason': 'main queue not finished by 2026-10-02 22:00 UTC'}) + '\n'); log('skipped'); return
        time.sleep(300)
    log('main queue finished; starting optional baselines')
    env = {k: v for k, v in os.environ.items() if not (k.startswith(('PARE_', 'YUNWU_', 'SEMZIP_')) or k.endswith('_API_KEY'))}
    env.update(PYTHONHASHSEED='0', LANG='en_US.UTF-8', LC_ALL='en_US.UTF-8')
    jobs = []
    lr_out = U/'baselines'/'logreducer'
    lr = ['python3', '-u', str(B/'logreducer'/'lr_run2.py'), '--output', str(lr_out), '--workers', '2']
    if lr_out.exists(): clear_stale_lock(lr_out, 'lr_run2.py'); lr.append('--resume')
    for D in ORDER: lr += ['--input', f'{D}={(U/"raw"/f"{D}.log").resolve()}']
    ls_out = U/'baselines'/'logshrink'
    ls = [str(B/'logshrink'/'venv'/'bin'/'python'), '-u', str(U/'baselines'/'logshrink_adapter'/'ls_run_unseen.py'),
          '--datasets', *ORDER, '--output', str(ls_out), '--workers', '2', '--L-mode', 'generic', '--resume']
    for name, cmd in [('logreducer_R', lr), ('logshrink_R_L5', ls)]:
        th = threading.Thread(target=run, args=(name, cmd, env, U/'logs'/f'{name}.log', st)); th.start(); jobs.append(th)
    for th in jobs: th.join()
    st['status'] = 'FINISHED'; st['finished'] = now(); save(st); log('[OPTIONAL DONE]')

if __name__ == '__main__':
    main()
