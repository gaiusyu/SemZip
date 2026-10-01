#!/usr/bin/env python3
"""One additional independent first-block synthesis (gateway GPT-4o) at a given temperature.
Same trainer source, sampler, prompts and call budget as R69; only temperature and output dir differ.
Usage: train_k.py DATASET TEMPERATURE OUTDIR"""
from pathlib import Path
import json, os, sys, time, hashlib
R69 = Path(os.environ.get('R69_ROOT', '<WORKSPACE>/r69_fresh_main_20260923'))
SOURCE = R69/'source'

def main(dataset, temperature, out):
    out = Path(out).resolve(); out.mkdir(parents=True, exist_ok=False)
    sys.path.insert(0, str(SOURCE/'trainer'))
    from pure_config import PureConfig
    import run_blocks_pure as runner
    config = json.loads(Path('<API_CONFIG_FILE>').read_text())
    os.environ.update(PARE_LLM_API_KEY=config['api_key'],
                      PARE_LLM_API_BASE=config['base_url'].rstrip('/') + '/chat/completions',
                      PARE_LLM_MODEL='gpt-4o')
    cfg = PureConfig(workers=1, model='gpt-4o', offline_prefix_ratio=1.0,
                     offline_min_prefix_lines=100000, offline_api_workers=1, offline_max_llm_calls=40)
    original = runner.compressor_command
    runner.compressor_command = lambda *a, **k: original(*a, **k) + [
        '--api-retries', '0', '--model', 'gpt-4o', '--temperature', str(temperature)]
    train = R69/'inputs'/dataset/'train.log'
    t = time.time()
    result = runner.train_dataset(dataset, train, out, cfg)
    result.update(temperature=temperature, wall_seconds=time.time()-t,
                  training_sha256=hashlib.sha256(train.read_bytes()).hexdigest())
    (out/'training.json').write_text(json.dumps(result, indent=2, default=str))
    print(json.dumps({k: result.get(k) for k in ('status', 'api_calls', 'replay_plan', 'error')}, default=str))

if __name__ == '__main__':
    main(sys.argv[1], float(sys.argv[2]), sys.argv[3])
