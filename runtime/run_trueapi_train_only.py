#!/usr/bin/env python3
"""Create a fresh offline SemZip replay plan through the configured LLM API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pure_config import PureConfig
from run_blocks_pure import train_dataset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--block-lines", type=int, default=100_000)
    parser.add_argument("--max-llm-calls", type=int, default=80)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.result_root.exists() and any(args.result_root.iterdir()):
        raise SystemExit(f"result root is not empty: {args.result_root}")

    config = PureConfig(
        workers=args.workers,
        block_lines=args.block_lines,
        offline_topk=40,
        offline_logbatcher_per_family=2,
        offline_logbatcher_min_support=50,
        offline_max_llm_calls=args.max_llm_calls,
        offline_batch_query=True,
        offline_prefix_ratio=0.2,
        offline_min_prefix_lines=args.block_lines,
    )
    result = train_dataset(
        args.dataset,
        args.input,
        args.result_root,
        config,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    if result["status"] != "PASS":
        return 1
    if int(result["api_calls"]) <= 0:
        print("ERROR: fresh training completed without a real API call")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
