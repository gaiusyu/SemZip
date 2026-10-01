#!/usr/bin/env python3
"""R76-G (unseen log sources): the unchanged R73 main ("pool") SemZip pipeline on four LBL ITA HTTP access logs
(DEV_DESIGN_R76_zh.md section R76-G, pre-registered 2026-10-01 13:20 UTC).  Adapted from the R76 variance driver
(variance/rep.py, kept as rep_variance_v3.py.orig); every adaptation is marked "R76-G" and listed
in README.md.  One run (r1) per dataset on original block 0:

  5 fresh syntheses, unchanged R69 trainer via train_k.py (gateway GPT-4o, 40-call budget, 0 retries):
      c0 T=0.0 (t0.0_k1), c1..c4 T=0.7 (t0.7_k1..k4); a failed synthesis is retained, never retried
  -> QG-V1 per usable candidate (qg_select2.py)            [0 API]
  -> QG-POOL-V2 over the gated candidates (qg_pool2.py)     [0 API; block-0 complete-archive cost]
  -> publish.py pool (storage refit on block 0, determinism check against the selection fit)
  -> formal_full.py pool (full-file encode, separate archive-only decode, per-block + full-file SHA-256).

All scripts in code/ are byte-identical copies of variance/code (= r73_qg, code/SHA256SUMS.r73_originals) except
formal_full.py: worker count from FORMAL_WORKERS (variance deviation) and, R76-G, the raw-identity reference read from
FORMAL_REF (= unseen/reference.json, built from unseen/inputs.json) instead of r71 reference.json.
Nothing outside unseen/ is written.  The credential file is read only by train_k.py (never printed); the key scan
below only reports a count.

Usage (cwd = unseen/):
  rep.py dryrun D R                      print exact commands/env, create nothing, call nothing
  rep.py run D R [SLOTS]                 one run, at most SLOTS heavy child processes at a time (default 2)
  rep.py queue SLOTS LANES D:R,... [B,...]  several runs (LANES at once) + optional baseline lane for datasets B, one SLOTS budget
  rep.py summary                         summary.json (+ table) with the unseen baselines (if finished)
"""
import collections, contextlib, json, os, shutil, statistics, subprocess, sys, threading, time, traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path('<WORKDIR>')
V = ROOT/'r76_additional_20260929'/'unseen'                   # R76-G: own track dir
CODE = V/'code'
Q73 = ROOT/'r73_quality_gate_20260927'/'r73_qg'
R69 = ROOT/'r69_fresh_main_20260923'
INPUTS = V/'inputs.json'                                            # R76-G: inventory replaces r68/r71 references
CRED = Path('<API_CONFIG_FILE>')
DATASETS = ('NASA', 'ClarkNet', 'USask', 'Calgary')                 # R76-G
RUNS = [('c0', 0.0, 1)] + [(f'c{k}', 0.7, k) for k in range(1, 5)]   # identical to R75 evo.pipeline / R73 c0..c4
GATE_THREADS = 1                                                     # qg_select2 threads per candidate (R75 value)
POOL_THREADS = int(os.environ.get('VAR_POOL_THREADS', '3'))
FORMAL_WORKERS = int(os.environ.get('VAR_FORMAL_WORKERS', '3'))
SYNTH_PARALLEL = int(os.environ.get('UNSEEN_SYNTH_PARALLEL', '3'))  # R76-G: at most 3 syntheses at once (API bound)
PY = sys.executable

ENV = dict(os.environ, QG_ART=str(Q73/'art'), QG_HOME=str(CODE), PYTHONHASHSEED='0', PYTHONDONTWRITEBYTECODE='1',
           R69_ROOT=str(V/'r69root'), FORMAL_REF=str(V/'reference.json'))
for k in [k for k in ENV if k.startswith(('PARE_', 'YUNWU_', 'SEMZIP_')) or k.endswith('_API_KEY')]:
    ENV.pop(k)
SHOWN_ENV = ('QG_ART', 'QG_HOME', 'PYTHONHASHSEED', 'PYTHONDONTWRITEBYTECODE', 'R69_ROOT', 'FORMAL_REF')
SYNTH_SEM = threading.BoundedSemaphore(SYNTH_PARALLEL)

def now(): return time.strftime('%Y-%m-%d %H:%M:%S')
def log(*a): sys.stdout.write(' '.join(map(str, (now(),) + a)) + '\n'); sys.stdout.flush()   # one write per line (threads)
def jload(p): return json.loads(Path(p).read_text())
def jsave(p, d):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True); t = p.with_suffix(p.suffix + '.tmp')
    t.write_text(json.dumps(d, indent=1, default=str) + '\n'); os.replace(t, p)
def sha(p):
    import hashlib
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()

class Slots:
    """FIFO counting budget: at most n heavy child processes (synthesis=1, gate=threads, pool=threads, formal=workers)."""
    def __init__(self, n): self.n, self.free, self.cv, self.q = n, n, threading.Condition(), collections.deque()
    @contextlib.contextmanager
    def hold(self, k):
        k = max(1, min(k, self.n)); tok = object()
        with self.cv:
            self.q.append(tok)
            self.cv.wait_for(lambda: self.q[0] is tok and self.free >= k)
            self.q.popleft(); self.free -= k; self.cv.notify_all()
        try: yield k
        finally:
            with self.cv: self.free += k; self.cv.notify_all()

SLOTS = Slots(2)

def paths(D, R):
    wd = V/'runs'/D/R
    return dict(wd=wd, train=wd/'train'/f'{D}.block0.log', raw=wd/'raw'/f'{D}.log', logs=wd/'logs',
                state=wd/'repeat.json', pool=wd/'runs'/'pool'/D, pub=wd/'runs'/'publish'/'pool'/D,
                formal=wd/'runs'/'formal'/'pool'/D)

def inv(D):
    """R76-G: inputs.json record of dataset D (sha256, bytes, lines, blocks, block-0 file and sha)."""
    return jload(INPUTS)['datasets'][D]

def setup(D, R):
    """Workdir mirrors r73_qg: train/<D>.block0.log -> block 0 (first 100,000 lines) of the unseen raw file,
    raw/<D>.log -> unseen/raw/<D>.log, r69root = {source -> R69/source, inputs/<D>/train.log -> the same block 0}.
    R76-G: block 0 is unseen/train/<D>/train.log (written once by prep_inputs.py, sha in inputs.json)."""
    p = paths(D, R); wd = p['wd']; rec = inv(D)
    block0 = Path(rec['block0_path']).resolve(); raw = Path(rec['raw_path']).resolve()
    for d in [wd/'train', wd/'raw', p['logs'], V/'r69root'/'inputs'/D]: d.mkdir(parents=True, exist_ok=True)
    links = [(p['train'], block0), (p['raw'], raw),
             (V/'r69root'/'source', (R69/'source').resolve()), (V/'r69root'/'inputs'/D/'train.log', block0)]
    for link, target in links:
        if link.is_symlink() or link.exists():
            assert Path(os.readlink(link)) == target, (link, target)
        else:
            link.symlink_to(target)
    assert p['raw'].resolve() == (V/'raw'/f'{D}.log').resolve()
    return p

def block0_sha(D):
    return inv(D)['block0_sha256']

def commands(D, R):
    p = paths(D, R); wd = p['wd']; out = {}
    for cid, T, k in RUNS:
        o = wd/'train_k'/D/f't{T}_k{k}'
        out[f'synth_{cid}'] = [PY, '-u', str(CODE/'train_k.py'), D, str(T), str(o)]
        out[f'gate_{cid}'] = [PY, '-u', str(CODE/'qg_select2.py'), D, f'<{o}/training.json:replay_plan>', str(p['train']),
                              str(wd/'runs'/'qg'/cid/D), str(GATE_THREADS)]
    out['pool'] = [PY, '-u', str(CODE/'qg_pool2.py'), D, str(p['pool']/'candidates.json'), str(p['train']), str(p['pool']), str(POOL_THREADS)]
    out['publish (cwd=wd)'] = [PY, str(CODE/'publish.py'), 'pool', D, f'runs/pool/{D}/selected_plan.json', f'runs/pool/{D}']
    out[f'formal (cwd=wd, FORMAL_WORKERS={FORMAL_WORKERS})'] = [PY, '-u', str(CODE/'formal_full.py'), 'pool', D,
                                                                 f'runs/publish/pool/{D}/program.json', '<publication.json:storage>']
    return out

def dryrun(D, R):
    p = paths(D, R)
    print('workdir', p['wd'], '(exists)' if p['wd'].exists() else '(not created)')
    print('env:', {k: ENV[k] for k in SHOWN_ENV}, '| PARE_/YUNWU_/SEMZIP_/*_API_KEY stripped; train_k.py loads the credential file itself')
    rec = inv(D)
    print('links: train ->', Path(rec['block0_path']).resolve(), '| raw ->', Path(rec['raw_path']).resolve(),
          '| r69root/source ->', (R69/'source').resolve())
    print('train block sha256', sha(rec['block0_path']), '| inputs.json block-0 sha256', block0_sha(D))
    for name, cmd in commands(D, R).items(): print(f'[{name}]', ' '.join(cmd))
    print('slots: synthesis 1 each (5 queued), gate', GATE_THREADS, 'each, pool', POOL_THREADS, ', formal', FORMAL_WORKERS)

def run_cmd(cmd, logf, k, cwd=None, env=None):
    with SLOTS.hold(k):
        t0 = time.time()
        with open(logf, 'a') as lf:
            lf.write(f'=== {now()} {" ".join(cmd)}\n'); lf.flush()
            rc = subprocess.run(cmd, cwd=cwd, env=env or ENV, stdout=lf, stderr=subprocess.STDOUT).returncode
        return rc, t0, time.time()

def key_leak_count(paths_):
    """Count occurrences of the API key in the given files (reports a number only; the key is never printed)."""
    try: key = json.loads(CRED.read_text()).get('api_key', '')
    except Exception: return None
    if not key: return None
    n = 0
    for f in paths_:
        try: n += Path(f).read_bytes().count(key.encode())
        except Exception: pass
    return n

def repeat(D, R):
    p = setup(D, R); wd = p['wd']
    st = jload(p['state']) if p['state'].exists() else {'dataset': D, 'repeat': R, 'created': now(), 'syntheses': {}, 'gates': {}}
    if st.get('status') in ('DONE', 'FAILED'): log('SKIP', D, R, st['status']); return st
    lock = threading.Lock()
    def save():
        with lock: jsave(p['state'], st)
    pool_threads, formal_workers = min(POOL_THREADS, SLOTS.n), min(FORMAL_WORKERS, SLOTS.n)   # never exceed the slot budget
    if st.get('started'): st.setdefault('restarts', []).append({'at': now(), 'driver_sha256': sha(__file__), 'slots': SLOTS.n})
    st.update(started=st.get('started') or now(), env={k: ENV[k] for k in SHOWN_ENV},
              code_sha256={f.name: sha(f) for f in sorted(CODE.glob('*.py'))}, driver_sha256=sha(__file__),
              train_block_sha256=sha(p['train']), inputs_block0_sha256=block0_sha(D), raw=str(p['raw'].resolve()),
              synth_parallel=SYNTH_PARALLEL,
              slots=SLOTS.n, gate_threads=GATE_THREADS, pool_threads=pool_threads, formal_workers=formal_workers)
    assert st['train_block_sha256'] == st['inputs_block0_sha256'], 'training block is not original block 0'
    save(); log('START', D, R)
    # ---- 1. syntheses (gateway API) ----
    def synth(r):
        cid, T, k = r; out = wd/'train_k'/D/f't{T}_k{k}'; tj = out/'training.json'
        rec = st['syntheses'].get(cid, {'T': T, 'k': k, 'dir': str(out)})
        if not tj.exists() and out.exists() and 'finished_unix' not in rec:
            rec.update(status='INCOMPLETE_RETAINED', note='output dir without training.json found at (re)start; not retried (R73 rule)')
        elif not tj.exists() and 'finished_unix' not in rec:
            out.parent.mkdir(parents=True, exist_ok=True)
            with SYNTH_SEM:   # R76-G: at most SYNTH_PARALLEL syntheses at once (inside the shared slot budget)
                rc, t0, t1 = run_cmd([PY, '-u', str(CODE/'train_k.py'), D, str(T), str(out)], p['logs']/f'synth_{cid}.log', 1, cwd=wd)
            rec.update(rc=rc, started_unix=t0, finished_unix=t1, started=time.strftime('%F %T', time.localtime(t0)),
                       driver_seconds=round(t1 - t0, 1))
            log('SYNTH', D, R, cid, 'rc', rc, 'sec', round(t1 - t0))
        if tj.exists():
            r_ = jload(tj)
            rec.update({k_: r_.get(k_) for k_ in ('status', 'api_calls', 'replay_plan', 'error', 'wall_seconds', 'temperature', 'training_sha256')})
        elif 'status' not in rec:
            rec['status'] = 'NO_TRAINING_JSON'
        with lock: st['syntheses'][cid] = rec
        save()
    with ThreadPoolExecutor(len(RUNS)) as ex: list(ex.map(synth, RUNS))
    sy = st['syntheses']
    st['api_calls_total'] = sum(int(sy[c].get('api_calls') or 0) for c in sy)
    ts = [sy[c]['started_unix'] for c in sy if 'started_unix' in sy[c]]; te = [sy[c]['finished_unix'] for c in sy if 'finished_unix' in sy[c]]
    st['synthesis_wall_seconds'] = round(max(te) - min(ts), 1) if ts and te else None
    st['synthesis_pass'] = [c for c, _, _ in RUNS if sy[c].get('status') == 'PASS' and Path(str(sy[c].get('replay_plan') or '')).is_file()]
    save()
    # ---- 2. QG-V1 per candidate (0 API) ----
    def gate(cid):
        r = sy[cid]; plan = Path(str(r.get('replay_plan') or '')); out = wd/'runs'/'qg'/cid/D
        g = {'dir': str(out)}
        if cid not in st['synthesis_pass']:
            out.mkdir(parents=True, exist_ok=True)
            if not (out/'CANDIDATE_UNUSABLE.json').exists():
                jsave(out/'CANDIDATE_UNUSABLE.json', {'dataset': D, 'candidate': cid, 'training_status': r.get('status'),
                      'replay_plan': str(plan), 'policy': 'retained as failed synthesis; not pooled (R73 phaseC/phaseD rule)'})
            g['status'] = 'UNUSABLE'
        elif (out/'qg_report.json').exists() or not (out/'FAILED.txt').exists():
            if not (out/'qg_report.json').exists():
                rc, t0, t1 = run_cmd([PY, '-u', str(CODE/'qg_select2.py'), D, str(plan), str(p['train']), str(out), str(GATE_THREADS)],
                                     p['logs']/f'qg_{cid}.log', GATE_THREADS, cwd=wd)
                g.update(rc=rc, seconds=round(t1 - t0, 1))
                if not (out/'qg_report.json').exists():
                    (out/'FAILED.txt').write_text(f'rc={rc}; see logs/qg_{cid}.log')
            if (out/'qg_report.json').exists():
                q = jload(out/'qg_report.json')
                g.update(status='PASS', original=q['original_train_archive_bytes'], empty=q['empty_train_archive_bytes'],
                         selected=q['selected_train_archive_bytes'], removed=q['removed_tags'], kept=q['kept_tags'],
                         evaluations=q['evaluations'], plan=str((out/'selected_plan.json').resolve()))
                log('GATE', D, R, cid, g['original'], '->', g['selected'], 'evals', g['evaluations'])
            else:
                g['status'] = 'FAILED'
        else:
            g['status'] = 'FAILED'
        with lock: st['gates'][cid] = g
        save()
    with ThreadPoolExecutor(len(RUNS)) as ex: list(ex.map(gate, [c for c, _, _ in RUNS]))
    cands = [{'cid': c, 'plan': st['gates'][c]['plan'], 'train_cost': st['gates'][c]['selected']}
             for c, _, _ in RUNS if st['gates'][c].get('status') == 'PASS']
    st['candidates_passed'] = [c['cid'] for c in cands]; save()
    if not cands:
        st.update(status='FAILED', outcome='NO_USABLE_CANDIDATE', finished=now()); save(); log('NO CANDIDATE', D, R); return st
    # ---- 3. QG-POOL-V2 (0 API) ----
    pw = p['pool']
    if not (pw/'pool_report.json').exists() and not (pw/'FAILED.txt').exists():
        pw.mkdir(parents=True, exist_ok=True); cj = pw/'candidates.json'
        if not cj.exists(): cj.write_text(json.dumps(cands, indent=1))
        assert jload(cj) == cands
        rc, t0, t1 = run_cmd([PY, '-u', str(CODE/'qg_pool2.py'), D, str(cj), str(p['train']), str(pw), str(pool_threads)],
                             p['logs']/'pool.log', pool_threads, cwd=wd)
        st['pool_run'] = {'rc': rc, 'seconds': round(t1 - t0, 1), 'threads': pool_threads}
        if not (pw/'pool_report.json').exists(): (pw/'FAILED.txt').write_text(f'rc={rc}; see logs/pool.log')
    if not (pw/'pool_report.json').exists():
        st.update(status='FAILED', outcome='POOL_FAILED', finished=now()); save(); log('POOL FAILED', D, R); return st
    pr = jload(pw/'pool_report.json')
    st['pool'] = {k: pr[k] for k in ('start', 'start_train_bytes', 'start_cost_reproduced', 'pool_groups', 'duplicate_groups_skipped',
                                     'selected_train_bytes', 'selected_tags', 'selected_plan_sha256', 'evaluations')}
    st['block0_cost'] = pr['selected_train_bytes']; save()
    log('POOL', D, R, 'start', pr['start'], pr['start_train_bytes'], '->', pr['selected_train_bytes'], 'evals', pr['evaluations'])
    # ---- 4. publish (storage refit on block 0) ----
    pubj = p['pub']/'publication.json'
    if not pubj.exists():
        rc, t0, t1 = run_cmd([PY, str(CODE/'publish.py'), 'pool', D, f'runs/pool/{D}/selected_plan.json', f'runs/pool/{D}'],
                             p['logs']/'publish.log', 1, cwd=wd)
        if not pubj.exists():
            st.update(status='FAILED', outcome='PUBLISH_FAILED', publish_rc=rc, finished=now()); save(); return st
    pub = jload(pubj); st['publication'] = pub; save()
    # ---- 5. formal full-file encode + archive-only decode + SHA ----
    fd = p['formal']; fres = fd/'result.json'; ffail = fd.parent/f'{D}.FAILED'
    if not fres.exists() and not ffail.exists():
        if fd.exists():   # driver died mid-run earlier (no FAILED marker): deterministic stage, move aside and rerun
            aside = wd/'runs'/'formal_interrupted'/f'{D}_{int(time.time())}'; aside.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(fd), str(aside)); st.setdefault('formal_interrupted', []).append(str(aside))
        env = dict(ENV, FORMAL_WORKERS=str(formal_workers))
        rc, t0, t1 = run_cmd([PY, '-u', str(CODE/'formal_full.py'), 'pool', D, str(p['pub']/'program.json'), pub['storage']],
                             p['logs']/'formal.log', formal_workers, cwd=wd, env=env)
        st['formal_run'] = {'rc': rc, 'seconds': round(t1 - t0, 1), 'workers': formal_workers}
        if not fres.exists():
            ffail.parent.mkdir(parents=True, exist_ok=True); ffail.write_text(f'rc={rc}; see logs/formal.log')
    if not fres.exists():
        st.update(status='FAILED', outcome='FORMAL_FAILED', finished=now()); save(); log('FORMAL FAILED', D, R); return st
    f = jload(fres); e = f['encode']
    st['formal'] = {'status': f['status'], 'archive_bytes': e['archive_bytes'], 'raw_bytes': e['raw_bytes'],
                    'ratio': e['compression_ratio'], 'blocks': len(e['blocks']), 'semantic_fallback_blocks': e['semantic_fallback_blocks'],
                    'sha_pass': bool(f['status'] == 'PASS' and f['independent_materialized_file_sha_pass']
                                     and f['decode']['decoded_sha256'] == e['raw_sha256']),
                    'raw_sha256': e['raw_sha256'], 'decoded_sha256': f['decode']['decoded_sha256'],
                    'heldout_archive_bytes': f['heldout']['archive_bytes'], 'heldout_ratio': f['heldout']['ratio'],
                    'encode_MB_per_s': f['encode_MB_per_s'], 'decode_MB_per_s': f['decode_MB_per_s'], 'workers': f.get('workers'),
                    'wall_seconds': round(f['finished_unix'] - f['started_unix'], 1)}
    # R76-G: DeLog/baseline comparison is done in summary() from unseen/baselines (no r68 numbers exist for these files)
    st['api_key_occurrences_in_logs'] = key_leak_count(list(p['logs'].glob('*.log')) + list(wd.glob('train_k/*/*/training.json')))
    st.update(status='DONE', outcome='PASS', finished=now()); save()
    log('DONE', D, R, 'archive', e['archive_bytes'], 'ratio', round(e['compression_ratio'], 4), 'sha', st['formal']['sha_pass'],
        'suffix_ratio', round(f['heldout']['ratio'] or 0, 4))
    return st

BASE = V/'baselines'/'main'          # R76-G: unseen baselines, one codec_baseline_r76.py output root per dataset
BASE_CODECS = ('delog', 'loglite', 'gzip6', 'xz6', 'xz9e', 'zstd3', 'zstd19')
HARNESS = V/'baselines'/'harness'/'codec_baseline_r76.py'   # byte-identical copy of codecs/codec_baseline_r76.py
R76 = ROOT/'r76_additional_20260929'; R68A = ROOT/'r68_external_20260923'/'artifacts'
BASE_ARGS = ['--codecs', *BASE_CODECS, '--exe', 'zstd=' + str(R76/'codecs'/'tools'/'zstd-1.5.6'/'programs'/'zstd'),
             '--delog-dir', str(R68A/'DeLog'), '--loglite-executable', str(R68A/'LogLite-B-wide'/'build'/'loglite-B-wide'),
             '--workers', '1', '--trials', '1']

def base_cmd(D):
    out = BASE/D
    cmd = [PY, '-u', str(HARNESS), '--input', f'{D}={(V/"raw"/f"{D}.log").resolve()}', '--output', str(out)] + BASE_ARGS
    if out.exists():
        cmd.append('--resume')
        # an attempt dir without result.json = interrupted (container restart), not a codec failure -> new attempt
        if any(not (a/'result.json').exists() for a in out.glob(f'{D}/*/trial_001/attempt_*')): cmd.append('--retry-failed')
        lock = out/'RUNNING.lock'
        if lock.exists():   # the harness never removes a lock it did not create; remove only if its pid is not a live harness
            pid = jload(lock).get('pid')
            try: alive = 'codec_baseline_r76.py' in Path(f'/proc/{pid}/cmdline').read_bytes().decode(errors='ignore')
            except Exception: alive = False
            assert not alive, f'baseline harness still running for {D} (pid {pid})'
            (out/f'RUNNING.lock.stale_{int(time.time())}').write_bytes(lock.read_bytes()); lock.unlink()
    return cmd

def baseline_lane(datasets, qs, qlock, qstate):
    """R76-G: main baselines (DeLog official, LogLite-BL, gzip6, xz6, xz9e, zstd3, zstd19), one harness process per dataset
    with 1 CLI worker, holding 1 slot of the shared budget.  Resumable (harness --resume; verified PASS runs reused)."""
    for D in datasets:
        st = BASE/D/'status.json'
        if st.exists() and jload(st).get('status') == 'PASS':
            log('BASELINES SKIP', D, 'PASS'); continue
        (V/'logs').mkdir(exist_ok=True)
        with qlock: qs['running'][f'baselines:{D}'] = now(); jsave(qstate, qs)
        rc, t0, t1 = run_cmd(base_cmd(D), V/'logs'/f'baselines_{D}.log', 1, cwd=V)
        res = jload(st) if st.exists() else {'status': 'NO_STATUS'}
        log('BASELINES', D, 'rc', rc, res.get('status'), f"{res.get('passed')}/{res.get('runs')}", 'sec', round(t1 - t0))
        with qlock:
            qs['running'].pop(f'baselines:{D}', None); qs['done'][f'baselines:{D}'] = {'at': now(), 'rc': rc, 'result': res}
            jsave(qstate, qs)

def queue(spec, lanes, baselines=None):
    todo = collections.deque(tuple(s.split(':')) for s in spec.split(','))
    for D, R in todo: assert D in DATASETS and R == 'r1', (D, R)   # R76-G: one run per dataset
    for D, R in todo: setup(D, R)          # create shared r69root links before lanes start (no race)
    qstate = V/'queue_status.json'; lock = threading.Lock()
    qs = {'started': now(), 'slots': SLOTS.n, 'lanes': lanes, 'spec': spec, 'baselines': baselines, 'done': {}, 'running': {},
          'driver_sha256': sha(__file__)}
    jsave(qstate, qs)
    def lane(i):
        while True:
            with lock:
                if not todo: return
                D, R = todo.popleft(); qs['running'][f'{D}:{R}'] = now(); jsave(qstate, qs)
            try: st = repeat(D, R); res = st.get('status'), st.get('outcome')
            except Exception: res = ('EXCEPTION', traceback.format_exc()[-2000:]); log('EXCEPTION', D, R, res[1])
            with lock:
                qs['running'].pop(f'{D}:{R}', None); qs['done'][f'{D}:{R}'] = {'at': now(), 'result': res}; jsave(qstate, qs)
    def blane():
        try: baseline_lane(baselines, qs, lock, qstate)
        except Exception: log('EXCEPTION baselines', traceback.format_exc()[-2000:])
    with ThreadPoolExecutor(lanes + (1 if baselines else 0)) as ex:
        fs = [ex.submit(lane, i) for i in range(lanes)] + ([ex.submit(blane)] if baselines else [])
        for f in fs: f.result()
    qs['finished'] = now(); jsave(qstate, qs)
    try: summary()
    except Exception: log('summary failed', traceback.format_exc()[-1000:])
    log('[QUEUE DONE]')

def base_result(D, codec):
    """Latest PASS attempt of one baseline run (harness layout <root>/<D>/<D>/<codec>/trial_001/attempt_*/result.json)."""
    for rj in sorted((BASE/D/D/codec/'trial_001').glob('attempt_*/result.json'), reverse=True):
        r = jload(rj)
        if r.get('status') == 'PASS':
            return {'archive_bytes': r['archive_bytes'], 'suffix_archive_bytes': sum(b['archive_bytes'] for b in r['blocks'][1:]),
                    'blocks': len(r['blocks']), 'source': str(rj)}
    return None

def summary():
    """R76-G: SemZip run r1 per dataset vs the unseen baselines (full ratio, suffix ratio = blocks >= 1)."""
    rows = {}
    for D in DATASETS:
        rec = inv(D); raw = rec['raw_bytes']
        sp = paths(D, 'r1')['state']; s = jload(sp) if sp.exists() else {}; fm = s.get('formal', {})
        row = {'raw_bytes': raw, 'lines': rec['lines'], 'blocks': rec['blocks'], 'status': s.get('status', 'NOT_STARTED' if not s else 'RUNNING'),
               'outcome': s.get('outcome'), 'synthesis_pass': s.get('synthesis_pass'), 'api_calls_total': s.get('api_calls_total'),
               'candidates_passed': s.get('candidates_passed'), 'block0_cost': s.get('block0_cost'),
               'selected_tags': (s.get('pool') or {}).get('selected_tags'), 'sha_pass': fm.get('sha_pass'),
               'archive_bytes': fm.get('archive_bytes'), 'ratio': fm.get('ratio'),
               'suffix_raw_bytes': raw - rec['block_bytes'][0], 'suffix_archive_bytes': fm.get('heldout_archive_bytes'),
               'suffix_ratio': fm.get('heldout_ratio'), 'semantic_fallback_blocks': fm.get('semantic_fallback_blocks'), 'baselines': {}}
        for c in BASE_CODECS:
            b = base_result(D, c)
            if not b: continue
            b['ratio'] = raw / b['archive_bytes']; b['suffix_ratio'] = row['suffix_raw_bytes'] / b['suffix_archive_bytes'] if b['suffix_archive_bytes'] else None
            if row['archive_bytes'] and row['sha_pass']:
                b['semzip_minus_this_bytes'] = row['archive_bytes'] - b['archive_bytes']
                b['semzip_vs_this_pct'] = 100.0 * (row['archive_bytes'] - b['archive_bytes']) / b['archive_bytes']
                b['semzip_wins'] = row['archive_bytes'] < b['archive_bytes']
                if row['suffix_archive_bytes']:
                    b['semzip_suffix_minus_this_bytes'] = row['suffix_archive_bytes'] - b['suffix_archive_bytes']
                    b['semzip_suffix_wins'] = row['suffix_archive_bytes'] < b['suffix_archive_bytes']
            row['baselines'][c] = b
        rows[D] = row
    done = [r for r in rows.values() if r['archive_bytes'] and r['sha_pass'] and 'delog' in r['baselines']]
    agg = {'n_complete_vs_delog': len(done), 'wins_vs_delog': sum(r['baselines']['delog']['semzip_wins'] for r in done),
           'suffix_wins_vs_delog': sum(bool(r['baselines']['delog'].get('semzip_suffix_wins')) for r in done),
           'total_semzip_bytes': sum(r['archive_bytes'] for r in done),
           'total_delog_bytes': sum(r['baselines']['delog']['archive_bytes'] for r in done)}
    agg['total_bytes_difference_semzip_minus_delog'] = agg['total_semzip_bytes'] - agg['total_delog_bytes']
    jsave(V/'summary.json', {'datasets': rows, 'aggregate_vs_delog': agg, 'written': now()})
    for D, r in rows.items():
        cells = ' '.join(f"{c}:{b['archive_bytes']}({b['ratio']:.2f})" for c, b in r['baselines'].items())
        print(f"{D:8s} semzip {r['archive_bytes'] or r['status']} ratio {r['ratio'] and round(r['ratio'], 3)} "
              f"suffix {r['suffix_ratio'] and round(r['suffix_ratio'], 3)} sha {r['sha_pass']} | {cells}")
    print('aggregate vs DeLog:', agg)

if __name__ == '__main__':
    os.chdir(V)
    cmd = sys.argv[1]
    if cmd == 'dryrun': dryrun(sys.argv[2], sys.argv[3])
    elif cmd == 'run':
        SLOTS = Slots(int(sys.argv[4]) if len(sys.argv) > 4 else 2); repeat(sys.argv[2], sys.argv[3])
    elif cmd == 'queue':   # R76-G: optional 5th arg = comma-separated datasets for the baseline lane
        SLOTS = Slots(int(sys.argv[2])); queue(sys.argv[4], int(sys.argv[3]), sys.argv[5].split(',') if len(sys.argv) > 5 else None)
    elif cmd == 'summary': summary()
    else: sys.exit(__doc__)
