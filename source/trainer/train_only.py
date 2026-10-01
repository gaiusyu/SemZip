#!/usr/bin/env python3
"""Produce the frozen offline plan without benchmarking the old full-file backend."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from execution_environment import EXECUTION_ENVIRONMENT, activate_execution_environment

import run_blocks_pure as runner
from pure_config import PureConfig
from run_semzip_classwise_proposal import VERSION_ID, parse_args


def run_training(args) -> list[dict[str, object]]:
    activate_execution_environment(EXECUTION_ENVIRONMENT)
    os.environ["PARE_LLM_MODEL"] = args.model
    os.environ["PARE_NO_BENEFIT_FIXED_CODEC"] = "1"
    os.environ["PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA"] = "1"
    os.environ["SEMZIP_BASELINE_ID"] = VERSION_ID
    config = PureConfig(
        workers=args.workers,
        block_lines=args.block_lines,
        offline_topk=40,
        offline_logbatcher_per_family=2,
        offline_logbatcher_min_support=50,
        offline_max_llm_calls=args.max_llm_calls,
        offline_prefix_ratio=0.2,
        offline_min_prefix_lines=args.block_lines,
    )
    object.__setattr__(config, "compress_extra_flags", [])
    result_root = Path(args.result_dir).resolve()
    result_root.mkdir(parents=True, exist_ok=False)
    rows = []
    for dataset in args.datasets:
        started = time.perf_counter()
        info = runner.train_dataset(
            dataset, Path(args.raw_dir).resolve() / f"{dataset}.log", result_root, config
        )
        elapsed = time.perf_counter() - started
        plan_path = Path(str(info["replay_plan"]))
        row = {
            "dataset": dataset,
            "stage": "offline-training-only",
            "execution_environment": dict(EXECUTION_ENVIRONMENT),
            "train_api_calls": info["api_calls"],
            "train_seconds": info["seconds"],
            "offline_scan_sample_train_seconds": elapsed,
            "train_sampled_lines": info["sampled_lines"],
            "train_scanned_lines": info["scanned_lines"],
            "train_total_lines": info["total_lines"],
            "training": info,
            "plan_sha256": hashlib.sha256(plan_path.read_bytes()).hexdigest()
            if plan_path.is_file() else None,
        }
        rows.append(row)
        (result_root / "training_summary.json").write_text(
            json.dumps(rows, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(json.dumps(row, sort_keys=True), flush=True)
        if info["status"] != "PASS" or not plan_path.is_file():
            raise RuntimeError(f"{dataset} offline training failed: {info['error']}")
    return rows


if __name__ == "__main__":
    run_training(parse_args())
