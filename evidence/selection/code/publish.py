"""Publication step: copy a selected plan, refit its storage policy on original block 0 with the frozen fitter,
and check the policy is byte-identical to the one produced during selection (determinism check).
Usage: publish.py VERSION DATASET PLAN [SELECTION_WORKDIR]  -> runs/publish/<VERSION>/<DATASET>/{program.json,storage/storage.json,publication.json}"""
import json, shutil, sys, time
from pathlib import Path
import lib
from qg_select import canon
version, d, plan = sys.argv[1], sys.argv[2], Path(sys.argv[3])
sel = Path(sys.argv[4]) if len(sys.argv) > 4 else None
out = Path('runs/publish')/version/d
if (out/'publication.json').exists(): print('exists'); sys.exit()
if out.exists(): shutil.rmtree(out)
out.mkdir(parents=True)
shutil.copy(plan, out/'program.json')
t = time.time()
pol = lib.fit(Path('train')/(d + '.block0.log'), out/'program.json', out/'storage')
for junk in ['transformed.log', 'input.tar.xz']: (out/'storage'/junk).unlink(missing_ok=True)
same = None
if sel:
    key = canon(json.loads(plan.read_text()))[:12]
    ref = sel/f'eval_{key}'/'tc'/'storage'/'storage.json'
    same = ref.is_file() and ref.read_bytes() == pol.read_bytes()
pub = {'version': version, 'dataset': d, 'source_plan': str(plan), 'plan_sha256': lib.sha(out/'program.json'),
       'storage': str(pol.resolve()), 'storage_sha256': lib.sha(pol), 'fit_seconds': time.time() - t,
       'policy_identical_to_selection_fit': same, 'training_block_sha256': lib.sha(Path('train')/(d + '.block0.log')),
       'spec_count': len(json.loads((out/'program.json').read_text())['specs'])}
lib.save(out/'publication.json', pub); print(json.dumps(pub))
