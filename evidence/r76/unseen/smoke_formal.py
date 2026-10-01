#!/usr/bin/env python3
"""R76-G smoke of the adapted publish + formal path (0 API), run before any synthesis.
Uses the residual-only (empty) program, i.e. the same plan structure QG-V1 evaluates as 'empty_residual_only'
(copied from variance HPC r2 c0 eval_c0fa44e8d45f/plan.json with dataset renamed). It is NOT a SemZip result: it only
checks that block-0 storage fit, full-file encode, separate archive-only decode, per-block/full-file SHA-256, the
inputs.json/reference.json identity assertion (FORMAL_REF) and the suffix accounting work on an unseen file.
Usage (cwd = unseen/): smoke_formal.py DATASET [WORKERS]  -> smoke/<DATASET>_empty/"""
import json, os, subprocess, sys, time
from pathlib import Path
import rep

D = sys.argv[1]; W = sys.argv[2] if len(sys.argv) > 2 else '2'
wd = rep.V/'smoke'/f'{D}_empty'; rec = rep.inv(D)
for d in [wd/'train', wd/'raw', wd/'runs'/'pool'/D, wd/'logs']: d.mkdir(parents=True, exist_ok=True)
for link, target in [(wd/'train'/f'{D}.block0.log', Path(rec['block0_path']).resolve()), (wd/'raw'/f'{D}.log', Path(rec['raw_path']).resolve())]:
    if not link.is_symlink(): link.symlink_to(target)
    assert link.resolve() == target
plan = json.loads(Path('<WORKDIR>/r76_additional_20260929/variance/runs/HPC/r2/runs/qg/c0/HPC/eval_c0fa44e8d45f/plan.json').read_text())
assert plan['specs'] == [] and plan['placeholders'] == {} and plan['execution_plan']['global_tags'] == []
plan['dataset'] = D
pp = wd/'runs'/'pool'/D/'selected_plan.json'
if not pp.exists(): pp.write_text(json.dumps(plan, indent=1))
t = time.time()
with open(wd/'logs'/'publish.log', 'a') as lf:
    subprocess.run([rep.PY, str(rep.CODE/'publish.py'), 'pool', D, f'runs/pool/{D}/selected_plan.json'], cwd=wd, env=rep.ENV,
                   stdout=lf, stderr=subprocess.STDOUT, check=True)
pub = json.loads((wd/'runs'/'publish'/'pool'/D/'publication.json').read_text())
env = dict(rep.ENV, FORMAL_WORKERS=W)
with open(wd/'logs'/'formal.log', 'a') as lf:
    rc = subprocess.run([rep.PY, '-u', str(rep.CODE/'formal_full.py'), 'pool', D, f'runs/publish/pool/{D}/program.json', pub['storage']],
                        cwd=wd, env=env, stdout=lf, stderr=subprocess.STDOUT).returncode
res = wd/'runs'/'formal'/'pool'/D/'result.json'
out = {'dataset': D, 'rc': rc, 'seconds': round(time.time() - t, 1), 'workers': W, 'env': {k: rep.ENV[k] for k in rep.SHOWN_ENV}}
if res.exists():
    f = json.loads(res.read_text()); e = f['encode']
    out.update(status=f['status'], archive_bytes=e['archive_bytes'], raw_bytes=e['raw_bytes'], ratio=e['compression_ratio'],
               blocks=len(f['block_audit']), raw_sha256=e['raw_sha256'], decoded_sha256=f['decode']['decoded_sha256'],
               per_block_sha_pass=all(b['sha256'] == rb['sha256'] for b, rb in zip(f['block_audit'], rec['block_audit'])),
               inputs_sha_match=e['raw_sha256'] == rec['raw_sha256'], suffix_archive_bytes=f['heldout']['archive_bytes'],
               suffix_ratio=f['heldout']['ratio'], semantic_fallback_blocks=e['semantic_fallback_blocks'])
rep.jsave(wd/'smoke_result.json', out); print(json.dumps(out))
