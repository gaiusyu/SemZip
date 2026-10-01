#!/usr/bin/env python3
"""R76 formal timing (pre-registered design ../DEV_DESIGN_R76_zh.md, section D).

Copy of the R73 serialized timing campaign (r73_qg/timing_campaign.py): same quiet detector (cgroup cpuacct
minus own RUSAGE_CHILDREN, < 0.5 other cores), same timed() / audit() / rotating schedule, contaminated trials
retried (up to 4 attempts), resume by skipping clean cells, summary = latest clean result per repeat, median of 3.
Scope per phase = R68-FORMAL-R71-OUTER-CLI-V3: parent-subprocess wall time of a separate encode CLI (interpreter
start, imports, streaming read/split into exact 100,000-line blocks, 4 block workers with one native thread per
block, archive writes) and a separate archive-only decode CLI that writes the complete restored file. Input
inventory (cache warm-up), SHA audits, consistency checks and cleanup are outside every timer.

Sessions (never mixed in one median):
  T2  the 12 formal-cohort files (full files): semzip (R73 pool plan, anchor), delog (official, anchor),
      logreducer_r, logshrink_r, xz9e, zstd19
  T3  HDFS, Spark, Windows, Thunderbird, pre-registered 20-block sample: original blocks round(i*(N-1)/19),
      i = 0..19, concatenated byte-identically into samples/<D>.sample20.log (verified against the R68
      inventories). Methods: semzip, delog, loglite, gzip6, xz6, zstd3, xz9e, zstd19, logreducer_r,
      logshrink_r. Rates are MB/s on the SAMPLED bytes, not whole files.
logreducer_r / logshrink_r trials have three timed phases: encode (adapted method: native pipeline +
verification decode + residual), decode (official restore + residual application) and encode_native (native
pipeline alone, separate process; its archives are discarded after a consistency check vs the adapted run).

usage: timing_r76.py materialize | plan T2|T3 | run T2|T3 [REPEATS] | formal | smoke | summary T2|T3|smoke | status
"""
import sys
sys.dont_write_bytecode = True  # never write __pycache__ into the read-only R68/R73/R76 trees imported below
import hashlib
import json
import os
import platform
import py_compile
import resource
import shutil
import statistics
import subprocess
import time
import traceback
from pathlib import Path

H = Path(__file__).resolve().parent
R76 = H.parent
PROJ = R76.parent
R73 = PROJ / 'r73_quality_gate_20260927' / 'r73_qg'
R68 = PROJ / 'r68_external_20260923'
ART = R73 / 'art'
RAW = R73 / 'raw'
SAMPLES = H / 'samples'
RUNS = H / 'runs'
sys.path.insert(0, str(R68))
import codec_baseline as cb  # noqa: E402  R68 scan()/inventory() = the block law (read-only import)
sys.path.insert(0, str(H / 'vendor'))
import codec_baseline_r76 as cb76  # noqa: E402  byte-identical copy of ../codecs/codec_baseline_r76.py

DS12 = ['Linux', 'Proxifier', 'Apache', 'Zookeeper', 'Mac', 'HealthApp', 'HPC', 'Hadoop', 'OpenStack', 'OpenSSH',
        'Android', 'BGL']  # R73 order
DS_LARGE = ['HDFS', 'Spark', 'Windows', 'Thunderbird']
SESSIONS = {
    'T2': {'datasets': DS12, 'input': 'full',
           'methods': ['semzip', 'delog', 'logreducer_r', 'logshrink_r', 'xz9e', 'zstd19']},
    'T3': {'datasets': DS_LARGE, 'input': 'sample20',
           'methods': ['semzip', 'delog', 'loglite', 'gzip6', 'xz6', 'zstd3', 'xz9e', 'zstd19', 'logreducer_r',
                       'logshrink_r']},
}
SMOKE = [('T2', ['Linux', 'Proxifier'], 'full'), ('T3', ['HDFS'], 'mini2')]
R68_METHODS = ('delog', 'loglite', 'gzip6', 'xz6', 'zstd3')
R76_CODECS = ('xz9e', 'zstd19')
ADAPTED = {'logreducer_r': 'phase_lr.py', 'logshrink_r': 'phase_ls.py'}
ZSTD156 = R76 / 'codecs' / 'tools' / 'zstd-1.5.6' / 'programs' / 'zstd'
LS_PY = R76 / 'baselines' / 'logshrink' / 'venv' / 'bin' / 'python3'
CODEC_R76_ORIG = R76 / 'codecs' / 'codec_baseline_r76.py'
CODEC_R76_SHA = '208b41e86eb08b155e9eae7314b88da6d43b2155011d7dd41187df62b55a27f2'  # SHA-256 of ../codecs/codec_baseline_r76.py as published in this copy (as-run value not listed)
SAMPLE_K = 20
MINI_POS = (0, 19)  # smoke mini-sample = the first and last block of the HDFS 20-block sample
REPEATS = 3
WORKERS = 4
TERMINAL = ('PASS', 'SHA_FAIL')  # SHA_FAIL: all phases exited 0 but the archive-only output is not byte-exact
SCOPE = ('R68-FORMAL-R71-OUTER-CLI-V3 timing scope (as R73 timing_campaign.py); same host, one serialized '
         'session per table; 4 block workers for every method')


def load(p):
    return json.loads(Path(p).read_text())


def save(p, v):
    p = Path(p)
    p.parent.mkdir(parents=True, exist_ok=True)
    t = p.with_name(p.name + '.pending')
    t.write_text(json.dumps(v, indent=1))
    os.replace(t, p)


def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def require(cond, msg):
    if not cond:
        raise RuntimeError(msg)


# ------------------------------------------------------------------ T3 sample (pre-registered, section D)
def sample_indices(n, k=SAMPLE_K):
    return [round(i * (n - 1) / (k - 1)) for i in range(k)]  # i*(N-1)/19 never has fraction .5 (19 is odd)


def r68_inventory(d):
    return load(R68 / 'first_pass' / ('input_%s.json' % d))


BLOCK_KEYS = ('raw_bytes', 'lf_count', 'records', 'ends_with_lf', 'raw_sha256')


def write_sample(d, kind, idx):
    ref = r68_inventory(d)
    out, meta_path = SAMPLES / ('%s.%s.log' % (d, kind)), SAMPLES / ('%s.%s.json' % (d, kind))
    if meta_path.exists() and out.exists():
        meta = load(meta_path)
        require(meta['original_block_indices'] == idx and out.stat().st_size == meta['raw_bytes'],
                'existing sample differs from the pre-registered rule: %s' % out)
        print('sample exists', out.name, meta['raw_bytes'], flush=True)
        return meta
    src = Path(ref['source']['path'])
    require(src.stat().st_size == ref['raw_bytes'] == ref['source']['bytes'], 'source size differs from R68: %s' % d)
    offsets, acc = [], 0
    for b in ref['blocks']:
        offsets.append(acc)
        acc += b['raw_bytes']
    SAMPLES.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + '.part')
    out.unlink(missing_ok=True)  # an unverified leftover of an interrupted materialization
    blocks = []
    with open(src, 'rb') as f, open(tmp, 'wb') as w:
        for j, k in enumerate(idx):
            b = ref['blocks'][k]
            f.seek(offsets[k])
            data = f.read(b['raw_bytes'])
            ok = (len(data) == b['raw_bytes'] and hashlib.sha256(data).hexdigest() == b['raw_sha256']
                  and data.count(b'\n') == b['lf_count'])
            require(ok, '%s block %d at offset %d differs from the R68 inventory' % (d, k, offsets[k]))
            w.write(data)
            blocks.append(dict({key: b[key] for key in BLOCK_KEYS}, sample_index=j, original_index=k,
                               source_offset=offsets[k]))
    os.replace(tmp, out)
    # <MOUNT> (network file system) applies the server-side mtime lazily after close(); settle before the identity-guarded
    # inventory (same fix as ../codecs, cb76.settle_file: fsync, then wait until stat identity is stable for 2 s)
    settle = cb76.settle_file(out)
    inv = cb.inventory(out)  # re-split the sample with the protocol splitter: must give the same 20 blocks
    require(len(inv['blocks']) == len(idx), 'sample re-split block count differs')
    for got, want in zip(inv['blocks'], blocks):
        require(all(got[key] == want[key] for key in BLOCK_KEYS), 'sample re-split block differs: %s' % got)
    meta = {'dataset': d, 'kind': kind, 'rule': 'original 100k-line blocks round(i*(N-1)/19), i=0..19'
            + ('' if kind == 'sample20' else '; smoke mini-sample = positions %s of that list' % (MINI_POS,)),
            'N': len(ref['blocks']), 'original_block_indices': idx, 'source': ref['source'],
            'r68_inventory': str(R68 / 'first_pass' / ('input_%s.json' % d)),
            'r68_inventory_sha256': sha(R68 / 'first_pass' / ('input_%s.json' % d)),
            'file': str(out), 'raw_bytes': inv['raw_bytes'], 'raw_sha256': inv['raw_sha256'],
            'records': inv['records'], 'blocks': blocks, 'settle_seconds': settle,
            'verified': 'every block SHA-256/bytes/LF count equals the R68 inventory at its original offset, and '
                        'the protocol splitter re-splits the sample file into exactly these blocks',
            'created_unix': time.time()}
    save(meta_path, meta)
    print('sample written', out.name, len(idx), 'blocks', inv['raw_bytes'], 'bytes', flush=True)
    return meta


def materialize():
    for d in DS_LARGE:
        write_sample(d, 'sample20', sample_indices(len(r68_inventory(d)['blocks'])))
    idx = sample_indices(len(r68_inventory('HDFS')['blocks']))
    write_sample('HDFS', 'mini2', [idx[p] for p in MINI_POS])


def input_path(d, kind):
    return RAW / (d + '.log') if kind == 'full' else SAMPLES / ('%s.%s.log' % (d, kind))


def check_input(d, kind, inv):
    if kind == 'full':
        ref = r68_inventory(d)
        require(inv['raw_sha256'] == ref['raw_sha256'] and inv['blocks'] == ref['blocks'],
                'input differs from the R68 inventory: %s' % d)
    else:
        meta = load(SAMPLES / ('%s.%s.json' % (d, kind)))
        require(inv['raw_sha256'] == meta['raw_sha256'] and len(inv['blocks']) == len(meta['blocks'])
                and all(all(g[k] == w[k] for k in BLOCK_KEYS) for g, w in zip(inv['blocks'], meta['blocks'])),
                'sample differs from its verified manifest: %s' % d)


# ------------------------------------------------------------------ methods
def specs(methods):
    out = {}
    for m in methods:
        if m in R68_METHODS:  # pinned R68 codec specs, as R73 specs()
            dd = sorted(R68.glob('formal_v3/*_Linux_%s_r1' % m))[0]
            out[m] = load(dd / 'encode_request.json')['codec_spec']
        elif m in R76_CODECS:
            out[m] = cb76.Codec(m, executables={'zstd': str(ZSTD156)}).spec
    return out


def publications(datasets):
    pub = {}
    for d in datasets:
        base = R73 / 'runs' / 'publish' / 'pool' / d
        p = load(base / 'publication.json')
        pub[d] = {'plan': str(base / 'program.json'), 'storage': p['storage'],
                  'program_sha256': sha(base / 'program.json'), 'storage_sha256': sha(p['storage']),
                  'publication_sha256': sha(base / 'publication.json')}
    return pub


def env_clean(method):
    env = dict(os.environ)  # R73 env_clean(), plus no-bytecode-writes and (new methods) 1 native thread/block
    for k in list(env):
        if k.startswith(('PARE_LLM_', 'YUNWU_', 'SEMZIP_R53_')) or k in (
                'GZIP', 'XZ_OPT', 'XZ_DEFAULTS', 'ZSTD_CLEVEL', 'ZSTD_NBTHREADS', 'SEMZIP_R54_PLAN'):
            env.pop(k, None)
    env['PATH'] = str(R68 / 'tools/extracted/usr/bin') + os.pathsep + env.get('PATH', '')
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    if method in ADAPTED:
        env.update(OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1', NUMEXPR_NUM_THREADS='1',
                   LC_ALL='en_US.UTF-8', LANG='en_US.UTF-8')
    return env


def static_manifest():
    """Hashes of every runner/binary a trial executes (outside timers), plus self-checks."""
    require(sha(H / 'vendor' / 'codec_baseline_r76.py') == sha(CODEC_R76_ORIG) == CODEC_R76_SHA,
            'vendored codec_baseline_r76.py differs from ../codecs/codec_baseline_r76.py')
    py_compile.compile(str(H / 'vendor' / 'codec_baseline_r76.py'), doraise=True)  # pyc in OUR dir only
    sys.path.insert(0, str(R76 / 'baselines' / 'logreducer'))
    sys.path.insert(0, str(R76 / 'baselines' / 'logshrink' / 'adapter'))
    import lr_run2
    import ls_run
    import dis
    ops = lambda f: [(i.opname, repr(i.argval)) for i in dis.get_instructions(f)]  # constant values, not indices
    same = lambda f: ops(f) == ops(cb.scan)
    files = {'timing_r76.py': H / 'timing_r76.py', 'phase_lr.py': H / 'phase_lr.py', 'phase_ls.py': H / 'phase_ls.py',
             'phase_codec_r76.py': H / 'phase_codec_r76.py', 'vendor/codec_baseline_r76.py': H / 'vendor/codec_baseline_r76.py',
             'r76/baselines/logreducer/lr_run2.py': Path(lr_run2.__file__),
             'r76/baselines/logshrink/adapter/ls_run.py': Path(ls_run.__file__),
             'r76/baselines/logshrink/adapter/ls_segment.py': Path(ls_run.HERE) / 'ls_segment.py',
             'r76/baselines/logshrink/adapter/ls_seg_runner.py': Path(ls_run.HERE) / 'ls_seg_runner.py',
             'r68/formal_campaign_v3.py': R68 / 'formal_campaign_v3.py', 'r68/codec_baseline.py': R68 / 'codec_baseline.py',
             'r68/loglite_adapter.py': R68 / 'loglite_adapter.py',
             'r73/art/frozen/guarded_backend_v2.py': ART / 'frozen/guarded_backend_v2.py',
             'r73/art/frozen/guarded_backend.py': ART / 'frozen/guarded_backend.py',
             'r73/art/frozen/runtime_cache_boundary.py': ART / 'frozen/runtime_cache_boundary.py',
             'r73/timing_campaign.py (reference)': R73 / 'timing_campaign.py',
             'logreducer/THULR': lr_run2.LR_DIR / 'THULR', 'logreducer/Elastic': lr_run2.LR_DIR / 'Elastic',
             'zstd-1.5.6': ZSTD156}
    return {'sha256': {k: sha(v) for k, v in files.items()},
            'logshrink_static_hashes': ls_run.check_static(),  # also asserts OFFICIAL_L == shipped header_length
            'scan_bytecode_identical_to_r68': {'lr_run2.scan': same(lr_run2.scan), 'ls_run.scan': same(ls_run.scan),
                                               'codec_baseline_r76.scan': same(cb76.scan)},
            'python': sys.version, 'host': platform.node(), 'logshrink_python': str(LS_PY.resolve())}


# ------------------------------------------------------------------ R73 quiet detector and timer (copied)
CG = Path('/sys/fs/cgroup/cpu/cpuacct.usage')
QUIET_CORES = 0.5


def cg_ns():
    try:
        return int(CG.read_text())
    except Exception:
        return None


def other_cores_now(window=5):
    '''CPU cores used in this container by processes outside the (idle) controller during a short window.'''
    a = cg_ns(); t0 = time.time(); time.sleep(window); b = cg_ns()
    return None if a is None else (b - a) / 1e9 / (time.time() - t0)


def wait_quiet(max_wait=6 * 3600, note=None):
    t0 = time.time(); polls = 0
    while True:
        oc = other_cores_now()
        if oc is None or oc < QUIET_CORES:
            return {'waited_seconds': round(time.time() - t0), 'other_cores_before': oc}
        if time.time() - t0 > max_wait:
            return {'waited_seconds': round(time.time() - t0), 'other_cores_before': oc, 'gave_up': True}
        if polls % 20 == 0:  # (R76 addition) a heartbeat line about every 10 minutes while waiting
            print('WAIT quiet: other_cores %.2f, waited %ds%s' % (oc, time.time() - t0, note or ''), flush=True)
            heartbeat(state='waiting_for_quiet', other_cores=oc, waited_seconds=round(time.time() - t0), note=note)
        polls += 1
        time.sleep(30)


def timed(cmd, log, env):
    ru0 = resource.getrusage(resource.RUSAGE_CHILDREN); cg0 = cg_ns()
    with open(log, 'x') as s:
        started = time.time()
        t0 = time.perf_counter(); p = subprocess.run(cmd, stdout=s, stderr=subprocess.STDOUT, env=env); sec = time.perf_counter() - t0
    ru1 = resource.getrusage(resource.RUSAGE_CHILDREN); cg1 = cg_ns()
    own = (ru1.ru_utime + ru1.ru_stime) - (ru0.ru_utime + ru0.ru_stime)
    other = None if cg0 is None else (cg1 - cg0) / 1e9 - own
    return {'command': cmd, 'seconds': sec, 'returncode': p.returncode, 'load_average': list(os.getloadavg()),
            'started_unix': started, 'own_cpu_seconds': own, 'other_cpu_seconds': other,
            'other_cores': None if other is None else other / sec}


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


def schedule(datasets, methods, repeats):
    rows = []
    for r in range(1, repeats + 1):
        for i, d in enumerate(datasets):
            off = (r - 1 + i) % len(methods)
            for m in methods[off:] + methods[:off]:
                rows.append({'dataset': d, 'method': m, 'repeat': r})
    return rows


def phases(m):
    return ('encode', 'decode', 'encode_native') if m in ADAPTED else ('encode', 'decode')


HEARTBEAT = RUNS / 'STATUS.json'


def heartbeat(**kv):
    try:
        save(HEARTBEAT, dict(kv, pid=os.getpid(), updated_unix=time.time(), updated=time.strftime('%Y-%m-%d %H:%M:%S %z')))
    except Exception:
        pass


# ------------------------------------------------------------------ one trial
def archive_sum(root):
    return sum(p.stat().st_size for p in Path(root).rglob('*') if p.is_file())


def consistency(m, enc, nenc):
    """Native-only encode vs the native part of the adapted encode: same per-block pipeline results.
    7z stores file mtimes in its headers, so container bytes may differ by a few tens of bytes per block."""
    a = {b['index']: b for b in enc['blocks']}
    b = {x['index']: x for x in nenc['blocks']}
    if m == 'logreducer_r':
        keys = ('segments', 'templates', 'model_files', 'model_raw_bytes', 'read_logs', 'load_failed', 'match_failed',
                'templates_loaded')
        nb = lambda x: x.get('payload_7z_bytes', 0) + x.get('model_7z_bytes', 0)
        struct = lambda x: [x.get(k) for k in keys]
    else:
        nb = lambda x: x.get('lsz_bytes', 0)
        struct = lambda x: [x.get('encode', {}).get(k) for k in ('segment_line_counts', 'segments', 'model_raw_bytes')]
    same_status = all((a[i]['status'] == 'ENCODE_FAIL') == (b[i]['status'] == 'ENCODE_FAIL') for i in a)
    ok = [i for i in a if a[i]['status'] != 'ENCODE_FAIL' and b[i]['status'] != 'ENCODE_FAIL']
    return {'blocks': len(a), 'blocks_compared': len(ok), 'encode_fail_pattern_equal': same_status and set(a) == set(b),
            'structure_equal_all': all(struct(a[i]) == struct(b[i]) for i in ok),
            'native_bytes_adapted_run': sum(nb(a[i]) for i in ok), 'native_bytes_native_run': sum(nb(b[i]) for i in ok),
            'max_abs_block_native_byte_diff': max([abs(nb(a[i]) - nb(b[i])) for i in ok] or [0])}


def trial(sess_name, kind, idx, row, spec, pub, attempt, out, tag, formal):
    d, m = row['dataset'], row['method']
    dest = out / ('%s_%03d_%s_%s_r%d_a%d' % (tag, idx, d, m, row['repeat'], attempt))
    if dest.exists(): shutil.rmtree(dest)
    dest.mkdir(parents=True)
    src = input_path(d, kind)
    inv = cb.inventory(src)  # outside timers: warms the input cache and gives the reference hashes
    env = env_clean(m); work = dest / 'work'
    res = dict(row, status='RUNNING', attempt=attempt, _dest=str(dest), session=sess_name, formal=formal,
               input=str(src), input_kind=kind, sampled_blocks=(kind != 'full'), raw_bytes=inv['raw_bytes'],
               blocks=len(inv['blocks']), started_unix=time.time())
    try:
        check_input(d, kind, inv)
        if m == 'semzip':  # R73 code path, frozen R73 pool plan + policy, R71 guarded runtime
            pp = pub[d]; env['SEMZIP_R54_PLAN'] = pp['storage']
            enc = [sys.executable, str(ART / 'frozen/guarded_backend_v2.py'), 'encode', '--input', str(src), '--plan', pp['plan'],
                   '--dataset', d, '--semzip-source', str(ART / 'source/runtime'), '--result', str(work), '--block-size', '100000', '--workers', '4']
            res['encode'] = timed(enc, dest / 'encode.log', env)
            require(res['encode']['returncode'] == 0, 'encode failed')
            es = load(work / 'encode_summary.json'); archive = Path(es['archive_dir']); restored = dest / 'restored.owned.log'
            shutil.copy(work / 'encode_summary.json', dest / 'encode_summary.json')
            env['SEMZIP_R54_PLAN'] = str(dest / 'unavailable-external-policy.json')
            dec = [sys.executable, str(ART / 'frozen/guarded_backend_v2.py'), 'decode', '--archive', str(archive), '--output', str(restored),
                   '--semzip-source', str(ART / 'source/runtime'), '--result', str(dest / 'decode'), '--workers', '4']
            res['decode'] = timed(dec, dest / 'decode.log', env)
            res['archive_bytes'] = es['archive_bytes']; res['fallback_blocks'] = es['semantic_fallback_blocks']
        elif m in R68_METHODS or m in R76_CODECS:  # R68 codec-phase entry point / its R76 analogue
            entry = [str(R68 / 'formal_campaign_v3.py'), 'codec-phase'] if m in R68_METHODS else [str(H / 'phase_codec_r76.py')]
            work.mkdir()
            save(dest / 'encode_request.json', {'method': m, 'codec_spec': spec[m], 'dataset': d, 'attempt': str(work), 'inventory': inv})
            res['encode'] = timed([sys.executable] + entry + ['encode', str(dest / 'encode_request.json')], dest / 'encode.log', env)
            require(res['encode']['returncode'] == 0, 'encode failed')
            save(dest / 'decode_request.json', {'method': m, 'codec_spec': spec[m], 'dataset': d, 'attempt': str(work)})
            res['decode'] = timed([sys.executable] + entry + ['decode', str(dest / 'decode_request.json')], dest / 'decode.log', env)
            if (work / 'decode_phase.json').exists():
                restored = Path(load(work / 'decode_phase.json')['restored'])
            res['archive_bytes'] = archive_sum(work / 'archives')
        else:  # logreducer_r / logshrink_r
            py = str(LS_PY) if m == 'logshrink_r' else sys.executable
            script = str(H / ADAPTED[m])
            work.mkdir()
            req = {'method': m, 'dataset': d, 'attempt': str(work), 'inventory': inv, 'mode': 'adapted'}
            save(dest / 'encode_request.json', req)
            res['encode'] = timed([py, script, 'encode', str(dest / 'encode_request.json')], dest / 'encode.log', env)
            require(res['encode']['returncode'] == 0, 'encode failed')
            # the decoder gets only the archive directory and the block/archive order, never the input
            save(dest / 'decode_request.json', {'method': m, 'dataset': d, 'attempt': str(work)})
            res['decode'] = timed([py, script, 'decode', str(dest / 'decode_request.json')], dest / 'decode.log', env)
            require(res['decode']['returncode'] == 0, 'decode failed')
            enc_ph, dec_ph = load(work / 'encode_phase.json'), load(work / 'decode_phase.json')
            shutil.copy(work / 'encode_phase.json', dest / 'encode_phase.json')
            shutil.copy(work / 'decode_phase.json', dest / 'decode_phase.json')
            restored = Path(dec_ph['restored'])
            res['archive_bytes'] = archive_sum(work / 'archives')
            res['native_archive_bytes'] = sum(b.get('native_bytes', b.get('lsz_bytes', 0)) or 0 for b in enc_ph['blocks'])
            res['block_failures'] = {'encode': enc_ph['failed_blocks'], 'decode': dec_ph['failed_blocks']}
        require(res['decode']['returncode'] == 0, 'decode failed')
        a0 = time.perf_counter()
        res['sha_pass'] = audit(restored, inv)
        res['audit_seconds'] = time.perf_counter() - a0
        restored.unlink(missing_ok=True)
        if m in ADAPTED:  # third timed phase: the native pipeline alone, fresh process and workspace
            nwork = dest / 'work_native'; nwork.mkdir()
            save(dest / 'encode_native_request.json', dict(req, attempt=str(nwork), mode='native'))
            res['encode_native'] = timed([py, script, 'encode', str(dest / 'encode_native_request.json')], dest / 'encode_native.log', env)
            require(res['encode_native']['returncode'] == 0, 'native-only encode failed')
            nenc = load(nwork / 'encode_phase.json')
            shutil.copy(nwork / 'encode_phase.json', dest / 'encode_native_phase.json')
            res['native_encode_consistency'] = consistency(m, enc_ph, nenc)
        res['status'] = 'PASS' if res['sha_pass'] else 'SHA_FAIL'
        for ph in phases(m):
            res[ph + '_MB_per_s'] = inv['raw_bytes'] / res[ph]['seconds'] / 1e6
    except Exception as e:
        res.update(status='FAILED', error=repr(e), traceback=traceback.format_exc()[-3000:])
    res['finished_unix'] = time.time()
    save(dest / 'result.json', res)
    for w in (work, dest / 'work_native'):
        shutil.rmtree(w, ignore_errors=True)
    (dest / 'restored.owned.log').unlink(missing_ok=True)
    return res


# ------------------------------------------------------------------ session driver
LOCK = RUNS / 'DRIVER.lock'


def take_lock():
    RUNS.mkdir(parents=True, exist_ok=True)
    if LOCK.exists():
        try:
            pid = int(json.loads(LOCK.read_text())['pid'])
            alive = Path('/proc/%d/cmdline' % pid).exists() and b'timing_r76' in Path('/proc/%d/cmdline' % pid).read_bytes()
        except Exception:
            alive = False
        require(not alive, 'another timing_r76.py driver is running: %s' % LOCK.read_text())
        LOCK.unlink()  # our own lock, left by a dead driver of this directory
    with open(LOCK, 'x') as f:
        json.dump({'pid': os.getpid(), 'argv': sys.argv, 'started_unix': time.time()}, f)


def clean_terminal(r):
    return r.get('status') in TERMINAL and r.get('contaminated') is False and r.get('formal') is True


def run_session(name, repeats=REPEATS, datasets=None, methods=None, kind=None, out=None, formal=True, static=None):
    sess = SESSIONS[name]
    ds, ms, kind = datasets or sess['datasets'], methods or sess['methods'], kind or sess['input']
    out = out or RUNS / name
    sched = schedule(ds, ms, repeats); spec = specs(ms); pub = publications(ds) if 'semzip' in ms else {}
    tag = time.strftime('%Y%m%d%H%M%S')
    r76_full = load(R76 / 'codecs/full_20260929/manifest.json')['protocol']['codecs']
    save(out / ('DESIGN_%s.json' % tag), {
        'session': name, 'formal': formal, 'datasets': ds, 'methods': ms, 'input_kind': kind, 'repeats': repeats,
        'inputs': {d: str(input_path(d, kind)) for d in ds}, 'schedule': sched, 'codec_specs': spec,
        'r76_codec_specs_equal_codecs_full_run': {m: spec[m] == r76_full.get(m) for m in ms if m in R76_CODECS},
        'semzip_publications': pub, 'workers': WORKERS, 'quiet_cores': QUIET_CORES, 'static': static or static_manifest(),
        'env_added': {'all': {'PYTHONDONTWRITEBYTECODE': '1'},
                      'logreducer_r/logshrink_r': {'OMP_NUM_THREADS': '1', 'OPENBLAS_NUM_THREADS': '1', 'MKL_NUM_THREADS': '1',
                                                   'NUMEXPR_NUM_THREADS': '1', 'LC_ALL': 'en_US.UTF-8', 'LANG': 'en_US.UTF-8'}},
        'started_unix': time.time(), 'scope': SCOPE,
        'label': 'sampled blocks (20 per dataset), not whole files' if kind != 'full' else 'whole files'})
    done = {(r['dataset'], r['method'], r['repeat']) for r in (load(p) for p in out.glob('*/result.json')) if clean_terminal(r)}
    print('[SESSION %s] %d trials, %d clean cells already present, tag %s, formal=%s' % (name, len(sched), len(done), tag, formal), flush=True)
    for i, row in enumerate(sched):
        key = (row['dataset'], row['method'], row['repeat'])
        if key in done:
            print('TIMING', name, i, *key, 'already clean; skipped', flush=True); continue
        for attempt in range(1, 5 if formal else 2):
            q = wait_quiet(note=' (%s %d %s %s r%d)' % (name, i, *key)) if formal else {'skipped': 'smoke: quietness ignored'}
            heartbeat(state='trial', session=name, index=i, dataset=row['dataset'], method=row['method'], repeat=row['repeat'], attempt=attempt)
            r = trial(name, kind, i, row, spec, pub, attempt, out, tag, formal)
            oc = max((r.get(ph) or {}).get('other_cores') or 0 for ph in phases(row['method']))
            r['quiet_before'] = q; r['max_other_cores'] = oc; r['contaminated'] = oc > QUIET_CORES
            save(Path(r['_dest']) / 'result.json', r)
            print('TIMING', name, i, *key, 'attempt', attempt, r['status'], 'enc %.3f dec %.3f' % (r.get('encode_MB_per_s', 0), r.get('decode_MB_per_s', 0)),
                  ('enc_native %.3f' % r['encode_native_MB_per_s']) if 'encode_native_MB_per_s' in r else '',
                  'other_cores %.2f' % oc, 'CONTAMINATED' if r['contaminated'] else 'clean', r.get('error', ''), flush=True)
            if r['status'] == 'FAILED' or not r['contaminated']:
                break
    return summarize(name, out, ds, ms, repeats, formal)


def summarize(name, out, ds, ms, rep, formal=True):
    rows = sorted((load(p) for p in out.glob('*/result.json')), key=lambda r: r.get('finished_unix', 0))
    sampled = name == 'T3' or any(r.get('sampled_blocks') for r in rows)
    summ = {'session': name, 'formal': formal, 'repeats_required': rep, 'scope': SCOPE,
            'note': None if formal else 'SMOKE, NOT FORMAL: one repeat, machine not quiet (other jobs running)',
            'label': ('MB/s on the pre-registered 20 sampled 100k-line blocks per dataset (NOT whole files)' if sampled
                      else 'MB/s on whole files'),
            'statistic': 'per dataset: median over repeats of the latest clean (other_cores <= %.1f) terminal result per repeat; '
                         'geometric mean over datasets; byte-weighted = total bytes / total median seconds' % QUIET_CORES,
            'methods': {}}
    for m in ms:
        per, incomplete = {}, []
        for d in ds:
            best = {}
            for r in rows:  # smoke (formal=False) ignores quietness by design
                if r['method'] == m and r['dataset'] == d and r.get('status') in TERMINAL and bool(r.get('formal')) == formal \
                        and (r.get('contaminated') is False or not formal):
                    best[r['repeat']] = r
            obs = [best[k] for k in sorted(best)]
            if len(obs) < rep:
                incomplete.append(d); continue
            e = {'repeats': len(obs), 'lossless_all_repeats': all(o['status'] == 'PASS' for o in obs),
                 'raw_bytes': obs[0]['raw_bytes'], 'blocks': obs[0]['blocks'],
                 'archive_bytes_median': statistics.median(o.get('archive_bytes') or 0 for o in obs),
                 'trial_dirs': [Path(o['_dest']).name for o in obs]}
            if not e['lossless_all_repeats']:
                e['block_failures'] = obs[-1].get('block_failures')
            for ph in phases(m):
                e[ph + '_MB_per_s'] = statistics.median(o[ph + '_MB_per_s'] for o in obs)
                e[ph + '_seconds'] = statistics.median(o[ph]['seconds'] for o in obs)
            if m in ADAPTED:
                e['native_encode_consistency'] = [o.get('native_encode_consistency') for o in obs]
            per[d] = e
        good = [d for d in ds if d in per and per[d]['lossless_all_repeats']]
        entry = {'per_dataset': per, 'incomplete': incomplete,
                 'not_lossless_timed_only': [d for d in per if not per[d]['lossless_all_repeats']],
                 'aggregate_datasets': good, 'aggregate_complete': len(good) == len(ds)}
        if good:
            for ph in phases(m):
                entry[ph + '_gmean'] = statistics.geometric_mean(per[d][ph + '_MB_per_s'] for d in good)
                entry[ph + '_byte_weighted_MB_per_s'] = sum(per[d]['raw_bytes'] for d in good) / sum(per[d][ph + '_seconds'] for d in good) / 1e6
        summ['methods'][m] = entry
    tag = name if formal else 'smoke_' + name
    save(out / ('SUMMARY_%s.json' % tag), summ)
    lines = ['# %s timing summary (%s)' % (tag, summ['label']), '', summ['statistic'], '',
             '| method | datasets aggregated | encode gmean | decode gmean | encode_native gmean | encode byte-weighted | decode byte-weighted | not lossless (timed only) | incomplete |',
             '|---|---|---|---|---|---|---|---|---|']
    f = lambda v: '%.3f' % v if isinstance(v, (int, float)) else ''
    for m, v in summ['methods'].items():
        lines.append('| %s | %d/%d | %s | %s | %s | %s | %s | %s | %s |' % (
            m, len(v['aggregate_datasets']), len(ds), f(v.get('encode_gmean')), f(v.get('decode_gmean')), f(v.get('encode_native_gmean')),
            f(v.get('encode_byte_weighted_MB_per_s')), f(v.get('decode_byte_weighted_MB_per_s')),
            ','.join(v['not_lossless_timed_only']), ','.join(v['incomplete'])))
    lines += ['', '| dataset | method | raw MB | encode MB/s | decode MB/s | encode_native MB/s | lossless |', '|---|---|---|---|---|---|---|']
    for d in ds:
        for m, v in summ['methods'].items():
            e = v['per_dataset'].get(d)
            if e:
                lines.append('| %s | %s | %.1f | %s | %s | %s | %s |' % (d, m, e['raw_bytes'] / 1e6, f(e['encode_MB_per_s']), f(e['decode_MB_per_s']),
                                                                    f(e.get('encode_native_MB_per_s')), e['lossless_all_repeats']))
    (out / ('SUMMARY_%s.md' % tag)).write_text('\n'.join(lines) + '\n')
    print('[TIMING DONE %s]' % tag, json.dumps({m: (round(v.get('encode_gmean', 0), 3), round(v.get('decode_gmean', 0), 3),
                                                   len(v['aggregate_datasets'])) for m, v in summ['methods'].items()}), flush=True)
    return summ


def smoke():
    static = static_manifest()
    out = RUNS / 'smoke'
    for name, ds, kind in SMOKE:
        run_session(name, repeats=1, datasets=ds, kind=kind, out=out / name, formal=False, static=static)


def status():
    for name, sess in SESSIONS.items():
        out = RUNS / name
        rows = [load(p) for p in out.glob('*/result.json')]
        cells = {(r['dataset'], r['method'], r['repeat']) for r in rows if clean_terminal(r)}
        total = len(sess['datasets']) * len(sess['methods']) * REPEATS
        print(name, 'clean cells %d/%d' % (len(cells), total), 'trials run %d' % len(rows),
              'contaminated %d' % sum(1 for r in rows if r.get('contaminated')),
              'failed %d' % sum(1 for r in rows if r.get('status') == 'FAILED'))
    if HEARTBEAT.exists():
        print('heartbeat', HEARTBEAT.read_text())


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else 'status'
    if mode == 'materialize':
        return materialize()
    if mode == 'status':
        return status()
    if mode == 'plan':
        s = SESSIONS[sys.argv[2]]
        sched = schedule(s['datasets'], s['methods'], REPEATS)
        print(len(sched), 'trials'); print(json.dumps(sched[:12])); return
    if mode == 'summary':
        name = sys.argv[2]
        if name == 'smoke':
            for n, ds, kind in SMOKE:
                summarize(n, RUNS / 'smoke' / n, ds, SESSIONS[n]['methods'], 1, formal=False)
            return
        return summarize(name, RUNS / name, SESSIONS[name]['datasets'], SESSIONS[name]['methods'], REPEATS)
    take_lock()
    try:
        if mode == 'smoke':
            materialize(); smoke()
        elif mode == 'run':
            materialize()
            run_session(sys.argv[2], repeats=int(sys.argv[3]) if len(sys.argv) > 3 else REPEATS)
        elif mode == 'formal':  # T2 then T3, resumable: rerun the same command after any interruption
            materialize()
            static = static_manifest()
            for name in ('T2', 'T3'):
                run_session(name, static=static)
            heartbeat(state='finished')
            print('[FORMAL FINISHED]', flush=True)
        else:
            raise SystemExit(__doc__)
    finally:
        LOCK.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
