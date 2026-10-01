"""Assemble + archive-only decode min(frozen pool, empty program) per block for the triggered files, from the existing
per-part empty-program encodes of the evolution runs and the formal pool archive (no re-encoding). Usage: asm_frozen_empty.py"""
import json, os, sys, shutil
from pathlib import Path
QG = Path(os.environ['QG_HOME']); sys.path.insert(0, str(QG)); import lib
R75 = Path(__file__).resolve().parent
for d, base in [('Spark', R75/'spark_run'), ('Thunderbird', R75)]:
    out = R75/'asm_frozen_empty'/d; res = out/'assemble_result.json'
    if res.exists(): print(d, 'done'); continue
    shutil.rmtree(out, ignore_errors=True); arch = out/'archive'; (arch/'semantic').mkdir(parents=True)
    f = json.loads((QG/'runs/formal/pool'/d/'result.json').read_text()); fa = QG/'runs/formal/pool'/d/'archive'
    af = f.get('archive_files') or f['encode']['archive_files']
    emp = {}
    for cj in sorted((base/'enc').glob('full_empty_part*/costs.json')): emp.update({int(k): v for k, v in json.loads(cj.read_text()).items()})
    switched = 0
    for fb in f['encode']['blocks']:
        i = fb['index']; e = emp[i]; assert e['raw_sha256'] == fb['raw_sha256']
        pp = af[f'chunk_{i}.tar.xz']['bytes'] + fb['semantic_archive_bytes']
        if e['total'] < pp:
            switched += 1; os.link(e['chunk'], arch/f'chunk_{i}.tar.xz'); os.link(e['sem'], arch/'semantic'/f'block_{i:05d}.semantic.tar.xz')
        else:
            os.link(fa/f'chunk_{i}.tar.xz', arch/f'chunk_{i}.tar.xz'); os.link(fa/'semantic'/f'block_{i:05d}.semantic.tar.xz', arch/'semantic'/f'block_{i:05d}.semantic.tar.xz')
    shutil.copy(fa/'semantic_manifest.json', arch/'semantic_manifest.json')
    total = sum(p.stat().st_size for p in arch.rglob('*') if p.is_file())
    restored = out/'restored.log'; lib.decode(arch, restored, out/'decode', 4)
    ok = lib.sha(restored) == f['encode']['raw_sha256']; restored.unlink()
    r = {'dataset': d, 'archive_bytes': total, 'sha_pass': ok, 'switched_blocks': switched, 'raw_bytes': f['encode']['raw_bytes'], 'ratio': f['encode']['raw_bytes'] / total}
    lib.save(res, r); print(json.dumps(r), flush=True)
