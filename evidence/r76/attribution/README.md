# R76 track B1: attribution ladder, empty-program rung

Pre-registered design: `../DEV_DESIGN_R76_zh.md`, section B1. The first rung of the attribution ladder runs the
complete SemZip pipeline with an **empty program**: no extraction spec, so every record goes to the DeLog-derived
residual backend (plus 368 B of semantic metadata per block). The other rungs are SemZip-1 (`results/final/`), the
gated c0 plan (`evidence/selection/results/formal/qg1c0/`) and the pooled selection (main result).

## Contents

| Path | What |
|---|---|
| `plans/<Dataset>/extraction.json`, `plans/<Dataset>/storage.json` | the empty program and its storage policy, one pair per dataset |
| `runs/formal/empty/<Dataset>/result.json` | formal complete-file run: encode, separate archive-only decode, per-block and full-file SHA-256 audit, suffix accounting (Thunderbird compacted, see the main README) |
| `empty_summary.json` | per-dataset summary used by `analysis/r76/ladder.py` and `r76_tables.py` |

The 16 plan/policy pairs are the empty program of `evidence/evolution/spark/empty/` with only the dataset name
changed. They were first regenerated for this copy and then compared with the files of the original run on the execution
host: all 32 files are byte-identical. They are the files that were run: their SHA-256 values equal `plan_sha256` and `storage_sha256` of every
formal record and of `empty_summary.json`. Check:

```sh
python3 - <<'PY'
import hashlib, json
from pathlib import Path
base = Path('evidence/r76/attribution')
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
for d, v in sorted(json.loads((base/'empty_summary.json').read_text()).items()):
    r = json.loads((base/'runs/formal/empty'/d/'result.json').read_text())
    assert sha(base/'plans'/d/'extraction.json') == v['plan_sha256'] == r['plan_sha256']
    assert sha(base/'plans'/d/'storage.json') == v['storage_sha256'] == r['storage_sha256']
print('16/16 plan and policy hashes match')
PY
```

## Driver

The runs used the unchanged formal driver `evidence/selection/code/formal_full.py` (4 workers, see its docstring) in the
original record layout described in the main README ("Replaying the formal driver itself"):

```sh
python3 formal_full.py empty <Dataset> plans/<Dataset>/extraction.json plans/<Dataset>/storage.json
```

Each run writes `runs/formal/empty/<Dataset>/result.json`. No model call is involved. `run_empty.sh` is the launcher that
ran the 16 datasets in this order with this command (the track's `formal_full.py` is a byte-identical copy of the R73
driver and is not repeated).
