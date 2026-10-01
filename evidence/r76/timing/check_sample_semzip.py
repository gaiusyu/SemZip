#!/usr/bin/env python3
"""Read-only cross-check for sampled-block trials (T3 / smoke mini-sample): each SemZip sample block's semantic
archive bytes and verified SHA should equal those of the same original block in the R73 formal full-file run
(runs/formal/pool/<D>/encode_summary.json), i.e. the sample is encoded exactly as inside the whole file.
usage: check_sample_semzip.py TRIAL_DIR [TRIAL_DIR ...]"""
import sys
sys.dont_write_bytecode = True
import json
from pathlib import Path

R73 = Path(__file__).resolve().parent.parent.parent / 'r73_quality_gate_20260927' / 'r73_qg'


def main():
    for t in sys.argv[1:]:
        t = Path(t)
        r = json.loads((t / 'result.json').read_text())
        sample = json.loads((t / 'encode_summary.json').read_text())
        full = json.loads((R73 / 'runs/formal/pool' / r['dataset'] / 'encode_summary.json').read_text())
        by_sha = {b['raw_sha256']: b for b in full['blocks']}
        rows = []
        for b in sample['blocks']:
            f = by_sha.get(b['raw_sha256'])
            rows.append({'sample_block': b['index'], 'full_block': f and f['index'],
                         'semantic_archive_bytes': [b.get('semantic_archive_bytes'), f and f.get('semantic_archive_bytes')],
                         'equal': bool(f) and b.get('semantic_archive_bytes') == f.get('semantic_archive_bytes')})
        print(json.dumps({'trial': t.name, 'dataset': r['dataset'], 'blocks': len(rows),
                          'all_equal': all(x['equal'] for x in rows), 'rows': rows}))


if __name__ == '__main__':
    main()
