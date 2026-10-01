#!/usr/bin/env python3
"""Rebuild the SemZip-1 training samples (offline_evidence/<Dataset>/sample.log) from the public LogHub files.

The samples are verbatim excerpts of LogHub logs, so they are not redistributed. Each one is a deterministic function
of block 0 of its log file: the frozen trainer's LogBatcher-style sample writer
(`write_logbatcher_sample` in source/trainer/run_blocks_pure.py, called exactly as in its `train_dataset`) with the
sampling configuration recorded in offline_evidence/configuration.json. This script

  1. reads raw/<Dataset>.log (fetch and verify it first with scripts/prepare_data.py),
  2. takes block 0 (the first 100,000 LF-terminated records, or the whole file if it is shorter) and checks its SHA-256
     against `original_training_block_sha256` in metadata/sampling_replay_summary.json,
  3. runs the unchanged sample writer on that block, and
  4. requires the result to have the size and SHA-256 recorded for the sample in offline_evidence/PROVENANCE.json
     before writing offline_evidence/<Dataset>/sample.log.

No model call, compiler or generated program is executed.

usage: python3 offline_evidence/tools/rebuild_samples.py --raw-dir raw [--dataset Linux ...]
"""
import argparse, hashlib, json, math, sys, tempfile
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parents[2]
EVIDENCE = ROOT / 'offline_evidence'
sys.path.insert(0, str(ROOT / 'source' / 'trainer'))
from run_blocks_pure import write_logbatcher_sample  # noqa: E402  (frozen trainer, unchanged)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def block0(raw, block_lines):
    out = bytearray()
    with raw.open('rb') as stream:
        for index, line in enumerate(stream):
            if index >= block_lines:
                break
            out += line
    return bytes(out)


def rebuild(dataset, raw_dir, config, block_sha, sample_record):
    raw = raw_dir / f'{dataset}.log'
    if not raw.is_file():
        return {'dataset': dataset, 'status': 'MISSING_INPUT', 'input': str(raw)}
    cfg = config['sampling_configuration_by_dataset'][dataset]
    block = block0(raw, int(cfg['block_lines']))
    if sha(block) != block_sha[dataset]:
        return {'dataset': dataset, 'status': 'FAIL', 'reason': 'block 0 SHA-256 differs from the recorded training block'}
    with tempfile.TemporaryDirectory() as tmp:
        block_path = Path(tmp) / f'{dataset}.block0.log'
        block_path.write_bytes(block)
        total_lines = block.count(b'\n') + int(bool(block) and not block.endswith(b'\n'))
        # identical to train_dataset() in source/trainer/run_blocks_pure.py
        scanned = min(total_lines, max(1, math.ceil(total_lines * float(cfg['offline_prefix_ratio'])),
                                       int(cfg['offline_min_prefix_lines'])))
        out_path = Path(tmp) / 'sample.log'
        write_logbatcher_sample(block_path, out_path, max_families=int(cfg['offline_topk']),
                                per_family=int(cfg['offline_logbatcher_per_family']),
                                min_support=int(cfg['offline_logbatcher_min_support']), max_input_lines=scanned)
        sample = out_path.read_bytes()
    if len(sample) != sample_record['bytes'] or sha(sample) != sample_record['original_sha256']:
        return {'dataset': dataset, 'status': 'FAIL', 'reason': 'rebuilt sample differs from the recorded sample'}
    target = EVIDENCE / dataset / 'sample.log'
    target.write_bytes(sample)
    return {'dataset': dataset, 'status': 'PASS', 'bytes': len(sample), 'sha256': sha(sample), 'output': f'offline_evidence/{dataset}/sample.log'}


def main():
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--raw-dir', type=Path, required=True)
    parser.add_argument('--dataset', action='append', help='default: every dataset whose raw file is present')
    args = parser.parse_args()
    config = json.loads((EVIDENCE / 'configuration.json').read_text())
    provenance = json.loads((EVIDENCE / 'PROVENANCE.json').read_text())
    samples = {m['artifact_file'].split('/')[0]: m for m in provenance['mapping'] if m['kind'] == 'sample'}
    replay = json.loads((ROOT / 'metadata' / 'sampling_replay_summary.json').read_text())
    block_sha = {d['dataset']: d['original_training_block_sha256'] for d in replay['datasets']}
    datasets = args.dataset or sorted(d for d in samples if (args.raw_dir / f'{d}.log').is_file())
    rows = [rebuild(d, args.raw_dir, config, block_sha, samples[d]) for d in datasets]
    status = 'PASS' if rows and all(r['status'] == 'PASS' for r in rows) else 'FAIL'
    print(json.dumps({'status': status, 'samples': rows}, indent=1))
    if status != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
