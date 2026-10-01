#!/usr/bin/env python3
"""Harness check (0 API): re-run the R73 published HPC pool program/policy through variance/code/formal_full.py
(FORMAL_WORKERS=2) and compare every archive file (bytes + SHA-256) with the R73 formal result. Writes check_r73/."""
import json, os, subprocess, sys
from pathlib import Path
V = Path(__file__).resolve().parent
Q73 = Path('<WORKDIR>/r73_quality_gate_20260927/r73_qg')
D = sys.argv[1] if len(sys.argv) > 1 else 'HPC'
wd = V/'check_r73'; (wd/'raw').mkdir(parents=True, exist_ok=True)
link = wd/'raw'/f'{D}.log'
if not link.is_symlink(): link.symlink_to((Q73/'raw'/f'{D}.log').resolve())
pub = json.loads((Q73/'runs/publish/pool'/D/'publication.json').read_text())
env = dict(os.environ, QG_ART=str(Q73/'art'), QG_HOME=str(V/'code'), PYTHONHASHSEED='0', PYTHONDONTWRITEBYTECODE='1', FORMAL_WORKERS='2')
with open(wd/f'formal_{D}.log', 'w') as lf:
    rc = subprocess.run([sys.executable, '-u', str(V/'code/formal_full.py'), 'r73repro', D, str(Q73/'runs/publish/pool'/D/'program.json'),
                         pub['storage']], cwd=wd, env=env, stdout=lf, stderr=subprocess.STDOUT).returncode
mine = json.loads((wd/'runs/formal/r73repro'/D/'result.json').read_text())
ref = json.loads((Q73/'runs/formal/pool'/D/'result.json').read_text())
same = mine['archive_files'] == ref['archive_files']
out = {'dataset': D, 'rc': rc, 'mine_archive_bytes': mine['encode']['archive_bytes'], 'r73_archive_bytes': ref['encode']['archive_bytes'],
       'all_archive_files_identical': same, 'sha_pass': mine['status'] == 'PASS', 'workers_mine': mine.get('workers'), 'workers_r73': 4}
(wd/f'check_{D}.json').write_text(json.dumps(out, indent=1)); print(json.dumps(out))
