"""QG workbench helpers: fit storage on block 0, encode, independent decode, SHA check.
Uses the frozen R71 artifact source/guards unchanged (rebuilt aarch64 native binaries)."""
import os, sys, json, hashlib, subprocess, shutil, time
from pathlib import Path
ART = Path(os.environ.get('QG_ART', str(Path.home()/'work/art')))
SRC = ART/'source'
RUNTIME = SRC/'runtime'
QG = Path(os.environ.get('QG_HOME', str(Path.home()/'work/qg')))
FIT = QG/'fit_storage_v2.py'
sys.path.insert(0, str(ART/'frozen'))

def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()

def save(p, d):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True); p.write_text(json.dumps(d, indent=2) + '\n')

def clean_env(**extra):
    env = {k: v for k, v in os.environ.items() if not (k.startswith(('PARE_', 'YUNWU_', 'SEMZIP_')) or k.endswith('_API_KEY'))}
    env.update(extra); return env

def first_block(raw, out, n=100000):
    out = Path(out); out.parent.mkdir(parents=True, exist_ok=True)
    with open(raw, 'rb') as f, open(out, 'wb') as w:
        for _ in range(n):
            b = f.readline()
            if not b: break
            w.write(b)
    return out

def fit(train, plan, outdir):
    outdir = Path(outdir).resolve()
    env = clean_env(SEMZIP_FIT_SOURCE_ROOT=str(SRC))
    with open(str(outdir) + '.fit.log', 'w') as log:
        subprocess.run([sys.executable, str(FIT), str(train), str(plan), str(outdir)], env=env,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    return outdir/'storage.json'

def _encode_child(raw, plan, policy, result, dataset, workers):
    code = ("import sys,json;sys.path.insert(0,%r);import guarded_backend_v2 as g;from pathlib import Path;"
            "s=g.encode(Path(%r),Path(%r),Path(%r),Path(%r),%r,100000,%d);print('QGJSON'+json.dumps(s))"
            % (str(ART/'frozen'), str(raw), str(plan), str(RUNTIME), str(result), dataset, workers))
    env = clean_env(SEMZIP_R54_PLAN=str(policy))
    p = subprocess.run([sys.executable, '-c', code], env=env, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError('encode failed: ' + p.stderr[-3000:])
    line = [l for l in p.stdout.splitlines() if l.startswith('QGJSON')][-1]
    return json.loads(line[6:])

def decode(archive_dir, output, result, workers):
    env = clean_env(SEMZIP_R54_PLAN=str(Path(result).parent/'absent-policy.json'))
    with open(str(result) + '.log', 'w') as log:
        subprocess.run([sys.executable, str(ART/'frozen/guarded_backend_v2.py'), 'decode', '--archive', str(archive_dir),
                        '--output', str(output), '--semzip-source', str(RUNTIME), '--result', str(result),
                        '--workers', str(workers)], env=env, stdout=log, stderr=subprocess.STDOUT, check=True)

def roundtrip(raw, plan, policy, outdir, dataset, workers=4, keep=False):
    """Encode full file with fixed plan/policy, archive-only decode, SHA check. Returns summary dict."""
    outdir = Path(outdir).resolve(); raw = Path(raw).resolve(); plan = Path(plan).resolve(); policy = Path(policy).resolve()
    assert not outdir.exists(); outdir.mkdir(parents=True)
    t = time.time()
    s = _encode_child(raw, plan, policy, outdir/'encode', dataset, workers)
    restored = outdir/'restored.log'
    decode(s['archive_dir'], restored, outdir/'decode', workers)
    ok = sha(restored) == sha(raw)
    arch = Path(s['archive_dir'])
    per_block = []
    for b in s['blocks']:
        i = b['index']; nat = (arch/f'chunk_{i}.tar.xz').stat().st_size
        per_block.append({'index': i, 'raw_bytes': b['raw_bytes'], 'native_bytes': nat,
                          'semantic_bytes': b.get('semantic_archive_bytes', 0)})
    manifest_bytes = (arch/'semantic_manifest.json').stat().st_size
    res = {'dataset': dataset, 'plan': str(plan), 'plan_sha256': sha(plan), 'policy_sha256': sha(policy),
           'raw_bytes': s['raw_bytes'], 'archive_bytes': s['archive_bytes'], 'compression_ratio': s['compression_ratio'],
           'semantic_fallback_blocks': s.get('semantic_fallback_blocks'), 'blocks': len(s['blocks']),
           'sha_pass': ok, 'wall_seconds': time.time() - t, 'per_block': per_block, 'manifest_bytes': manifest_bytes,
           'online_compression_mbps': s.get('online_compression_mbps')}
    save(outdir/'result.json', res)
    restored.unlink()
    if not keep:
        shutil.rmtree(outdir/'encode'/'archive', ignore_errors=True)
    if not ok: raise RuntimeError('SHA mismatch ' + dataset)
    return res

def train_cost(train, plan, workdir, dataset):
    """Offline objective: fit storage on training block, encode it, return full archive bytes (+SHA verified)."""
    workdir = Path(workdir).resolve(); train = Path(train).resolve(); plan = Path(plan).resolve()
    if workdir.exists(): shutil.rmtree(workdir)
    workdir.mkdir(parents=True)
    pol = fit(train, plan, workdir/'storage')
    r = roundtrip(train, plan, pol, workdir/'rt', dataset, workers=1)
    for junk in ['transformed.log', 'input.tar.xz']:
        (workdir/'storage'/junk).unlink(missing_ok=True)
    return r['archive_bytes'], pol

def split_parts(raw, outdir, blocks_per_part, n=100000):
    """Split at original 100k-record boundaries (part k holds blocks k*B..k*B+B-1)."""
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    parts = []; k = 0
    with open(raw, 'rb') as f:
        while True:
            p = outdir/f'part_{k:03d}.log'
            cnt = 0
            with open(p, 'wb') as w:
                for _ in range(n*blocks_per_part):
                    b = f.readline()
                    if not b: break
                    w.write(b); cnt += 1
            if not cnt: p.unlink(); break
            parts.append(p); k += 1
    return parts
