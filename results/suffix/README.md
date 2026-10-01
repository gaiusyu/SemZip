# Nontraining suffix archive-cost view

These are all 96 saved method/dataset rows, including four explicitly excluded one-block datasets. The twelve eligible datasets use the same original blocks 1 onward for all methods. SemZip counts its reconstructed suffix manifest; external costs use their existing independent archive files. This is a cost view of previously checked blocks, not fresh encoding, a newly materialized suffix decode, or a timing experiment.

The exact exporter source, input hashes, block identity certificates, output hashes and generated TeX are included. Original full private-layout input ledgers are not required for rendering this saved record. From this directory, use a new output directory:

```sh
python3 -B - <<'PYCODE'
from pathlib import Path
import json
from export_suffix_comparison import render
out=Path('../suffix-rerendered')
out.mkdir()
render(json.loads(Path('suffix_comparison.json').read_text()),out)
PYCODE
```

This reproduces the three tables/text interfaces from the saved numeric record. It does not repeat the exporter's original input validation or perform codecs/API calls. Initial packaging verified byte-identical rendering of all three outputs. Complete original input SHA bindings remain in `suffix_comparison.json`.
