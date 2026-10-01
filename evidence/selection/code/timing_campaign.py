"""Serialized formal timing, same scope as R68-FORMAL-R71-OUTER-CLI-V3: parent-subprocess wall time of separate
encode and decode CLIs (imports, pools, input split/read, outputs, guards included); input inventory and output/archive
hash audits outside timers. Three rotating-order repeats over the twelve files no larger than BGL.
Methods: semzip (selected deployment), semzip1 (R71 single-synthesis deployment), and the R68 external methods through
R68's own codec-phase entry point and pinned codec specs. Usage: timing_campaign.py plan|run [DATASETS] [REPEATS]"""
import json, os, sys, time, hashlib, shutil, subprocess, statistics
from pathlib import Path
import lib
H = Path(__file__).resolve().parent
R68 = Path('<WORKSPACE>/r68_external_20260923')
sys.path.insert(0, str(R68))
import codec_baseline as cb
RAW = H/'raw'; OUT = H/'runs/timing'
DS12 = ['Linux', 'Proxifier', 'Apache', 'Zookeeper', 'Mac', 'HealthApp', 'HPC', 'Hadoop', 'OpenStack', 'OpenSSH', 'Android', 'BGL']
METHODS = ['semzip', 'semzip1', 'delog', 'loglite', 'gzip6', 'xz6', 'zstd3']
def load(p): return json.loads(Path(p).read_text())
def save(p, v):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True); t = p.with_name(p.name + '.pending'); t.write_text(json.dumps(v, indent=1)); os.replace(t, p)
def specs():
    out = {}
    for m in METHODS[2:]:
        d = sorted(R68.glob(f'formal_v3/*_Linux_{m}_r1'))[0]
        out[m] = load(d/'encode_request.json')['codec_spec']
    return out
def publications(datasets):
    pub = {}
    for d in datasets:
        p = load(H/'runs/publish/pool'/d/'publication.json')
        pub[('semzip', d)] = {'plan': str(H/'runs/publish/pool'/d/'program.json'), 'storage': p['storage']}
        pub[('semzip1', d)] = {'plan': str(lib.ART/'deployments'/d/'program.json'), 'storage': str(lib.ART/'deployments'/d/'storage.json')}
    return pub
def env_clean():
    env = dict(os.environ)
    for k in list(env):
        if k.startswith(('PARE_LLM_', 'YUNWU_', 'SEMZIP_R53_')) or k in ('GZIP', 'XZ_OPT', 'XZ_DEFAULTS', 'ZSTD_CLEVEL', 'ZSTD_NBTHREADS', 'SEMZIP_R54_PLAN'):
            env.pop(k, None)
    env['PATH'] = str(R68/'tools/extracted/usr/bin') + os.pathsep + env.get('PATH', '')
    return env
import resource
CG = Path('/sys/fs/cgroup/cpu/cpuacct.usage')
QUIET_CORES = 0.5
def cg_ns():
    try: return int(CG.read_text())
    except Exception: return None
def other_cores_now(window=5):
    '''CPU cores used in this container by processes outside the (idle) controller during a short window.'''
    a = cg_ns(); t0 = time.time(); time.sleep(window); b = cg_ns()
    return None if a is None else (b - a) / 1e9 / (time.time() - t0)
def wait_quiet(max_wait=6 * 3600):
    t0 = time.time()
    while True:
        oc = other_cores_now()
        if oc is None or oc < QUIET_CORES: return {'waited_seconds': round(time.time() - t0), 'other_cores_before': oc}
        if time.time() - t0 > max_wait: return {'waited_seconds': round(time.time() - t0), 'other_cores_before': oc, 'gave_up': True}
        time.sleep(30)
def timed(cmd, log, env):
    ru0 = resource.getrusage(resource.RUSAGE_CHILDREN); cg0 = cg_ns()
    with open(log, 'x') as s:
        t0 = time.perf_counter(); p = subprocess.run(cmd, stdout=s, stderr=subprocess.STDOUT, env=env); sec = time.perf_counter() - t0
    ru1 = resource.getrusage(resource.RUSAGE_CHILDREN); cg1 = cg_ns()
    own = (ru1.ru_utime + ru1.ru_stime) - (ru0.ru_utime + ru0.ru_stime)
    other = None if cg0 is None else (cg1 - cg0) / 1e9 - own
    return {'command': cmd, 'seconds': sec, 'returncode': p.returncode, 'load_average': list(os.getloadavg()),
            'own_cpu_seconds': own, 'other_cpu_seconds': other, 'other_cores': None if other is None else other / sec}
def audit(restored, inv):
    h = hashlib.sha256(); blocks = []
    with open(restored, 'rb') as f:
        for b in inv['blocks']:
            bh = hashlib.sha256(); size = n = 0
            for _ in range(100000):
                line = f.readline()
                if not line: break
                bh.update(line); h.update(line); size += len(line); n += 1
            blocks.append(bh.hexdigest() == b['raw_sha256'] and size == b['raw_bytes'])
        tail = f.read(1)
    return h.hexdigest() == inv['raw_sha256'] and all(blocks) and not tail
def schedule(datasets, repeats):
    rows = []
    for r in range(1, repeats + 1):
        for i, d in enumerate(datasets):
            off = (r - 1 + i) % len(METHODS)
            for m in METHODS[off:] + METHODS[:off]: rows.append({'dataset': d, 'method': m, 'repeat': r})
    return rows
RUN_TAG = time.strftime('%H%M%S')
def trial(idx, row, spec, pub, attempt=1):
    d, m = row['dataset'], row['method']; dest = OUT/f'{RUN_TAG}_{idx:03d}_{d}_{m}_r{row["repeat"]}_a{attempt}'
    if (dest/'result.json').exists(): return load(dest/'result.json')
    if dest.exists(): shutil.rmtree(dest)
    dest.mkdir(parents=True)
    inv = cb.inventory(RAW/(d + '.log')); env = env_clean(); work = dest/'work'
    res = dict(row, status='RUNNING', attempt=attempt, _dest=str(dest))
    try:
        if m.startswith('semzip'):
            pp = pub[(m, d)]; env['SEMZIP_R54_PLAN'] = pp['storage']
            enc = [sys.executable, str(lib.ART/'frozen/guarded_backend_v2.py'), 'encode', '--input', str(RAW/(d + '.log')), '--plan', pp['plan'],
                   '--dataset', d, '--semzip-source', str(lib.ART/'source/runtime'), '--result', str(work), '--block-size', '100000', '--workers', '4']
            res['encode'] = timed(enc, dest/'encode.log', env)
            assert res['encode']['returncode'] == 0, 'encode failed'
            es = load(work/'encode_summary.json'); archive = Path(es['archive_dir']); restored = dest/'restored.owned.log'
            env['SEMZIP_R54_PLAN'] = str(dest/'unavailable-external-policy.json')
            dec = [sys.executable, str(lib.ART/'frozen/guarded_backend_v2.py'), 'decode', '--archive', str(archive), '--output', str(restored),
                   '--semzip-source', str(lib.ART/'source/runtime'), '--result', str(dest/'decode'), '--workers', '4']
            res['decode'] = timed(dec, dest/'decode.log', env)
            res['archive_bytes'] = es['archive_bytes']; res['fallback_blocks'] = es['semantic_fallback_blocks']
        else:
            work.mkdir()
            save(dest/'encode_request.json', {'method': m, 'codec_spec': spec[m], 'dataset': d, 'attempt': str(work), 'inventory': inv})
            res['encode'] = timed([sys.executable, str(R68/'formal_campaign_v3.py'), 'codec-phase', 'encode', str(dest/'encode_request.json')], dest/'encode.log', env)
            assert res['encode']['returncode'] == 0, 'encode failed'
            save(dest/'decode_request.json', {'method': m, 'codec_spec': spec[m], 'dataset': d, 'attempt': str(work)})
            res['decode'] = timed([sys.executable, str(R68/'formal_campaign_v3.py'), 'codec-phase', 'decode', str(dest/'decode_request.json')], dest/'decode.log', env)
            restored = Path(load(work/'decode_phase.json')['restored'])
            res['archive_bytes'] = sum(p.stat().st_size for p in (work/'archives').rglob('*') if p.is_file())
        assert res['decode']['returncode'] == 0, 'decode failed'
        res['sha_pass'] = audit(restored, inv)
        assert res['sha_pass'], 'reconstruction failed'
        res.update(status='PASS', raw_bytes=inv['raw_bytes'], encode_MB_per_s=inv['raw_bytes']/res['encode']['seconds']/1e6,
                   decode_MB_per_s=inv['raw_bytes']/res['decode']['seconds']/1e6)
    except Exception as e:
        res.update(status='FAILED', error=repr(e))
    save(dest/'result.json', res)
    shutil.rmtree(work, ignore_errors=True); (dest/'restored.owned.log').unlink(missing_ok=True)
    return res
def main():
    global OUT
    mode = sys.argv[1]; ds = sys.argv[2].split(',') if len(sys.argv) > 2 else DS12; rep = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    if mode == 'smoke': OUT = H/'runs/timing_smoke'
    if mode == 'summary':
        return summarize(DS12, rep)
    sched = schedule(ds, rep); spec = specs(); pub = publications(ds)
    if mode == 'plan':
        print(len(sched), 'trials'); print(json.dumps(sched[:8])); return
    save(OUT/f'DESIGN_{RUN_TAG}.json', {'datasets': ds, 'repeats': rep, 'methods': METHODS, 'schedule': sched, 'codec_specs': spec,
                             'publications': {f'{m}/{d}': v for (m, d), v in pub.items()}, 'workers': 4, 'started_unix': time.time(),
                             'scope': 'R68-FORMAL-R71-OUTER-CLI-V3 timing scope; same host, one serialized session for all methods'})
    done_cells = {(r['dataset'], r['method'], r['repeat']) for r in (load(p) for p in OUT.glob('*/result.json'))
                  if r.get('status') == 'PASS' and r.get('contaminated') is False}
    for i, row in enumerate(sched):
        if (row['dataset'], row['method'], row['repeat']) in done_cells:
            print('TIMING', i, row['dataset'], row['method'], row['repeat'], 'already clean; skipped', flush=True); continue
        for attempt in range(1, 5):
            q = wait_quiet()
            r = trial(i, row, spec, pub, attempt)
            oc = max((r.get(ph) or {}).get('other_cores') or 0 for ph in ('encode', 'decode'))
            r['quiet_before'] = q; r['max_other_cores'] = oc; r['contaminated'] = oc > QUIET_CORES
            save(Path(r['_dest'])/'result.json', r)
            print('TIMING', i, row['dataset'], row['method'], row['repeat'], 'attempt', attempt, r['status'], round(r.get('encode_MB_per_s', 0), 3),
                  round(r.get('decode_MB_per_s', 0), 3), 'other_cores %.2f' % oc, 'CONTAMINATED' if r['contaminated'] else 'clean', flush=True)
            if r['status'] != 'PASS' or not r['contaminated']: break
    summarize(ds, rep)
def summarize(ds, rep):
    rows = [load(p) for p in sorted(OUT.glob('*/result.json'))]
    summ = {}
    for m in METHODS:
        per = {}
        for d in ds:
            best = {}
            for r in rows:  # one observation per repeat: the latest clean pass (reruns never add extra observations)
                if r['method'] == m and r['dataset'] == d and r['status'] == 'PASS' and r.get('contaminated') is False:
                    best[r['repeat']] = r
            obs = list(best.values())
            if len(obs) == rep:
                per[d] = {k: statistics.median(r[k] for r in obs) for k in ('encode_MB_per_s', 'decode_MB_per_s')}
        if len(per) == len(ds):
            g = lambda k: statistics.geometric_mean(per[d][k] for d in ds)
            summ[m] = {'encode_gmean': g('encode_MB_per_s'), 'decode_gmean': g('decode_MB_per_s'), 'per_dataset': per}
    save(OUT/'SUMMARY.json', summ); print('[TIMING DONE]', json.dumps({m: (round(v['encode_gmean'], 3), round(v['decode_gmean'], 3)) for m, v in summ.items()}), flush=True)
if __name__ == '__main__': main()
