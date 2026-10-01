"""Formal complete-file run for a frozen (program, storage policy) pair.
Mirrors R71 main_campaign.trial exactly (same frozen guard, same source runtime, 4 workers, separate archive-only decode
process with the external policy made unavailable, independent materialized-file block audit, suffix accounting).
Usage: formal_full.py VERSION DATASET PLAN POLICY   -> runs/formal/<VERSION>/<DATASET>/result.json"""
from pathlib import Path
import hashlib, json, os, subprocess, sys, time
import lib
FROZEN = lib.ART/'frozen'; SOURCE = lib.ART/'source'
sys.path.insert(0, str(FROZEN))
WORKERS = int(os.environ.get('FORMAL_WORKERS', '4'))  # R76 variance: worker count only (blocks are independent)
REF = Path('<WORKDIR>/r71_strict_main_20260923/reference.json')
def sha(path): return lib.sha(path)
def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.pending'); temp.write_text(json.dumps(value, indent=2) + '\n'); os.replace(temp, path)
def trial(version, dataset, plan, storage):
    import guarded_backend_v2 as guard
    plan, storage = Path(plan).resolve(), Path(storage).resolve()
    out = Path('runs/formal')/version/dataset; assert not out.exists(); out = out.resolve()
    plan_sha, storage_sha = sha(plan), sha(storage)
    for key in list(os.environ):
        if key.startswith(('PARE_LLM_', 'YUNWU_', 'SEMZIP_R53_')): os.environ.pop(key)
    os.environ['SEMZIP_R54_PLAN'] = str(storage)
    raw = Path('raw')/(dataset + '.log')
    started = time.time()
    summary = guard.encode(raw, plan, SOURCE/'runtime', out, dataset, 100000, WORKERS)
    save(out/'encode_summary.json', summary)
    restored = out/'restored.owned.log'
    env = dict(os.environ, SEMZIP_R54_PLAN=str(out/'nonexistent-external-policy.json'))
    with (out/'decode_process.log').open('w') as log:
        start = time.perf_counter()
        subprocess.run([sys.executable, str(FROZEN/'guarded_backend_v2.py'), 'decode', '--archive', summary['archive_dir'],
                        '--output', str(restored), '--semzip-source', str(SOURCE/'runtime'), '--result', str(out/'decode'),
                        '--workers', str(WORKERS)], env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
        decode_process_seconds = time.perf_counter() - start
    decoded = json.loads((out/'decode/summary.json').read_text())
    assert decoded['decoded_sha256'] == summary['raw_sha256'] and decoded['decoded_bytes'] == summary['raw_bytes']
    audit_start = time.perf_counter(); full = hashlib.sha256(); blocks = []
    with restored.open('rb') as stream:
        for original in summary['blocks']:
            h = hashlib.sha256(); size = records = 0
            for _ in range(100000):
                line = stream.readline()
                if not line: break
                h.update(line); full.update(line); size += len(line); records += 1
            assert h.hexdigest() == original['raw_sha256'] and size == original['raw_bytes']
            blocks.append({'index': original['index'], 'sha256': h.hexdigest(), 'bytes': size, 'records': records})
        assert not stream.read(1)
    assert full.hexdigest() == summary['raw_sha256']
    archive = Path(summary['archive_dir'])
    archive_files = {str(p.relative_to(archive)): {'bytes': p.stat().st_size, 'sha256': sha(p)} for p in archive.rglob('*') if p.is_file()}
    assert sum(r['bytes'] for r in archive_files.values()) == summary['archive_bytes']
    assert sha(plan) == plan_sha and sha(storage) == storage_sha
    ref = next(r for r in json.loads(REF.read_text()) if r['dataset'] == dataset)
    assert summary['raw_sha256'] == ref['raw_sha256'] and summary['raw_bytes'] == int(ref['raw_bytes'])
    assert len(blocks) == int(ref['blocks'])
    suffix_raw = sum(b['raw_bytes'] for b in summary['blocks'][1:])
    suffix_names = {f'chunk_{i}.tar.xz' for i in range(1, len(blocks))}
    suffix_names |= {f'semantic/block_{i:05d}.semantic.tar.xz' for i in range(1, len(blocks))}
    suffix_bytes = sum(v['bytes'] for k, v in archive_files.items() if k in suffix_names); suffix_manifest_bytes = 0
    if suffix_raw:
        manifest = json.loads((archive/'semantic_manifest.json').read_text())
        manifest.update(block_count=len(blocks)-1, semantic_archives=[f'block_{i:05d}.semantic.tar.xz' for i in range(len(blocks)-1)])
        suffix_manifest_bytes = len(json.dumps(manifest, separators=(',', ':'), sort_keys=True).encode('utf-8'))
        suffix_bytes += suffix_manifest_bytes
    result = {'status': 'PASS', 'version': version, 'dataset': dataset, 'plan': str(plan), 'plan_sha256': plan_sha,
              'storage': str(storage), 'storage_sha256': storage_sha, 'started_unix': started, 'finished_unix': time.time(),
              'encode': summary, 'decode': decoded, 'workers': WORKERS, 'decode_process_seconds': decode_process_seconds,
              'audit_seconds': time.perf_counter() - audit_start, 'independent_materialized_file_sha_pass': True,
              'block_audit': blocks, 'archive_files': archive_files,
              'encode_MB_per_s': summary['raw_bytes']/summary['online_compression_seconds']/1e6,
              'decode_MB_per_s': summary['raw_bytes']/decoded['decode_seconds']/1e6,
              'heldout': {'raw_bytes': suffix_raw, 'archive_bytes': suffix_bytes, 'manifest_bytes': suffix_manifest_bytes,
                          'ratio': suffix_raw/suffix_bytes if suffix_raw else None,
                          'scope': 'Original blocks 1 onward; same independently verified block payloads and recomputed sequential suffix manifest (R71 accounting).'}}
    save(out/'result.json', result); restored.unlink()
    print(json.dumps({'version': version, 'dataset': dataset, 'status': 'PASS', 'ratio': summary['compression_ratio'],
                      'fallback_blocks': summary['semantic_fallback_blocks'], 'heldout_ratio': result['heldout']['ratio']}), flush=True)
if __name__ == '__main__':
    trial(*sys.argv[1:5])
