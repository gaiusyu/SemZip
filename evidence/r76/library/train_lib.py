#!/usr/bin/env python3
"""R76-B2: one first-block 'synthesis' by the LIBRARY-V1 proposer instead of the LLM.
Copy of r73_qg/train_k.py: same R69 trainer source, sampler, prompts (built but not sent), call budget
and config.  Only change: the compressor subprocess is semzip_library_wrapper.py (which runs the
unchanged semzip_pure.py with call_llm replaced by the deterministic proposer).  The LLM credential
file is NOT read, provider env vars are removed, network is disabled inside the trainer: api_calls = 0.
Usage: train_lib.py DATASET OUTDIR"""
from pathlib import Path
import json, os, sys, time, hashlib
R69 = Path(os.environ.get('R69_ROOT', '<WORKDIR>/r69_fresh_main_20260923'))
SOURCE = R69/'source'
HERE = Path(__file__).resolve().parent

def sha_file(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def main(dataset, out):
    out = Path(out).resolve(); out.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(SOURCE/'trainer'))
    from pure_config import PureConfig
    import run_blocks_pure as runner
    for key in list(os.environ):
        if key.startswith(('PARE_LLM_', 'YUNWU_')) or key.endswith('_API_KEY'):
            os.environ.pop(key)
    os.environ['R76_LIB_TRAINER_DIR'] = str(SOURCE/'trainer')
    os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
    cfg = PureConfig(workers=1, model='gpt-4o', offline_prefix_ratio=1.0,
                     offline_min_prefix_lines=100000, offline_api_workers=1, offline_max_llm_calls=40)
    original = runner.compressor_command
    def library_command(*a, **k):
        cmd = original(*a, **k)
        assert cmd[1].endswith('semzip_pure.py'), cmd[:2]
        cmd[1] = str(HERE/'semzip_library_wrapper.py')
        return cmd + ['--api-retries', '0', '--model', 'no-llm-library-v1', '--temperature', '0']
    runner.compressor_command = library_command
    train = R69/'inputs'/dataset/'train.log'
    t = time.time()
    result = runner.train_dataset(dataset, train, out, cfg)
    work = out/'training_work'/dataset
    summ = json.loads((work/'summary.json').read_text()) if (work/'summary.json').is_file() else {}
    stats = summ.get('llm_runtime_stats', {})
    api_records = list((out/'llm_caches').rglob('_api_records'))
    import proposer
    result.update(proposer=proposer.LIBRARY_VERSION, proposer_sha256=sha_file(HERE/'proposer.py'),
                  wrapper_sha256=sha_file(HERE/'semzip_library_wrapper.py'),
                  trainer_semzip_pure_sha256=sha_file(SOURCE/'trainer'/'semzip_pure.py'),
                  library_stats={k: v for k, v in stats.items() if k.startswith(('library_', 'proposal_', 'api_'))},
                  api_record_dirs=len(api_records), temperature=None, wall_seconds=time.time()-t,
                  training_sha256=hashlib.sha256(train.read_bytes()).hexdigest())
    if int(result.get('api_calls', 0)) != 0 or api_records or int(stats.get('api_attempts', 0)) != 0:
        result['status'] = 'FAIL'; result['error'] = 'library run recorded an API attempt'
    (out/'training.json').write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps({k: result.get(k) for k in ('status', 'api_calls', 'replay_plan', 'error', 'library_stats')}, default=str))

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
