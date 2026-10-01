#!/usr/bin/env python3
"""Replay verified functions with regex-anchor guard and residual DEnum."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import run_blocks_pure as runner
from pure_config import PureConfig


METHOD_ID = "SEMZIP-REGEX-ANCHOR-GUARD-POSITION-DENUM-20260717"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=METHOD_ID)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--replay-plan", required=True)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--block-lines", type=int, default=100_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    raw_dir = Path(args.raw_dir)
    replay_plan = Path(args.replay_plan).resolve()
    result_dir = Path(args.result_dir)
    if result_dir.exists():
        shutil.rmtree(result_dir)
    result_dir.mkdir(parents=True)
    cache_dir = result_dir / "unused_llm_cache"
    cache_dir.mkdir()

    os.environ["PARE_WIDTH_AGNOSTIC_NUMERIC_REGEX"] = "1"
    os.environ["PARE_NUMERIC_LATTICE_MAX_INT_GROUPS"] = "8"
    os.environ["PARE_NUMERIC_LATTICE_MAX_FLOAT_GROUPS"] = "8"

    def reuse_training_plan(
        dataset: str,
        input_path: Path,
        result_root: Path,
        config: PureConfig,
    ) -> dict[str, object]:
        with input_path.open("rb") as source:
            total_lines = sum(1 for _line in source)
        return {
            "sampled_lines": 0,
            "total_lines": total_lines,
            "scanned_lines": 0,
            "prefix_ratio": 0.0,
            "seconds": 0.0,
            "status": "PASS",
            "api_calls": 0,
            "cache_dir": str(cache_dir),
            "replay_plan": str(replay_plan),
            "error": "",
        }

    runner.train_dataset = reuse_training_plan
    config = PureConfig(workers=args.workers, block_lines=args.block_lines)
    object.__setattr__(
        config,
        "compress_extra_flags",
        [
            "--disable-residual-dominance-planner",
            "--force-residual-numeric-lattice-admit",
            "--residual-auto-skip-low-templates",
            "0",
        ],
    )
    summary, blocks = runner.run_dataset(args.dataset, raw_dir, result_dir, config)
    runner.write_csv(result_dir / "summary.csv", [summary])
    runner.write_csv(result_dir / "blocks.csv", blocks)
    (result_dir / "summary.json").write_text(
        json.dumps([summary], indent=2, sort_keys=True), encoding="utf-8"
    )
    (result_dir / "experiment_manifest.json").write_text(
        json.dumps(
            {
                "method_id": METHOD_ID,
                "parent_plan": str(replay_plan),
                "dataset": args.dataset,
                "api_mode": "verified replay; no API",
                "workers": args.workers,
                "block_lines": args.block_lines,
                "semantic_filter": "fixed alphabetic literal or literal punctuation required",
                "residual_numeric": "8 position groups, then denum_feature",
                "oversized_integer": "exact string_mtf_rank via 18-digit guard",
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["block_status"] == "PASS" and summary["full_sha"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
