"""Assemble + archive-only decode min(frozen pool, empty program) per block for non-triggered files where the empty program wins
some block (same per-block rule as the evolution stage before any publication). Usage: asm_cfb.py DS1,DS2"""
import json, os, sys, shutil, time
from pathlib import Path
QG = Path(os.environ['QG_HOME']); sys.path.insert(0, str(QG)); import lib
R75 = Path(__file__).resolve().parent
for d in sys.argv[1].split(','):
    out = R75/'asm_cfb'/d; res = out/'assemble_result.json'
    if res.exists(): print(d, 'done'); continue
    shutil.rmtree(out, ignore_errors=True); out.mkdir(parents=True)
    ed = R75/'cfb_work'/d
    f = json.loads((QG/'runs/formal/pool'/d/'result.json').read_text()); fa = QG/'runs/formal/pool'/d/'archive'
    af = f.get('archive_files') or f['encode']['archive_files']
    t = time.time(); s = lib._encode_child(QG/'raw'/f'{d}.log', ed/'extraction.json', ed/'storage.json', out/'empty_encode', d, 4); enc_sec = time.time() - t
    ea = Path(s['archive_dir']); arch = out/'archive'; (arch/'semantic').mkdir(parents=True); switched = 0
    for b, fb in zip(s['blocks'], f['encode']['blocks']):
        i = b['index']; assert b['raw_sha256'] == fb['raw_sha256']
        pe = (ea/f'chunk_{i}.tar.xz').stat().st_size + (ea/'semantic'/f'block_{i:05d}.semantic.tar.xz').stat().st_size
        pp = af[f'chunk_{i}.tar.xz']['bytes'] + fb['semantic_archive_bytes']
        src = ea if pe < pp else fa; switched += pe < pp
        os.link(src/f'chunk_{i}.tar.xz', arch/f'chunk_{i}.tar.xz'); os.link(src/'semantic'/f'block_{i:05d}.semantic.tar.xz', arch/'semantic'/f'block_{i:05d}.semantic.tar.xz')
    shutil.copy(fa/'semantic_manifest.json', arch/'semantic_manifest.json')
    total = sum(p.stat().st_size for p in arch.rglob('*') if p.is_file())
    restored = out/'restored.log'; lib.decode(arch, restored, out/'decode', 4)
    ok = lib.sha(restored) == f['encode']['raw_sha256']; restored.unlink()
    r = {'dataset': d, 'archive_bytes': total, 'sha_pass': ok, 'switched_blocks': switched, 'raw_bytes': f['encode']['raw_bytes'],
         'ratio': f['encode']['raw_bytes'] / total, 'empty_encode_seconds': enc_sec}
    lib.save(res, r); shutil.rmtree(out/'empty_encode', ignore_errors=True); print(json.dumps(r), flush=True)
