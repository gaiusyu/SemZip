#!/usr/bin/env python3
"""Run the first-20%-offline-training ablation of the frozen SemZip baseline."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


VARIANT_ID = "SEMZIP-WIDTHGUARD-OR20-IPV4PLAIN-FIRST20-20260712"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Run {VARIANT_ID}")
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--datasets", nargs="+", required=True)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--block-lines", type=int, default=100_000)
    parser.add_argument("--model", default=os.environ.get("PARE_LLM_MODEL", "gpt-4o"))
    parser.add_argument("--max-llm-calls", type=int, default=80)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent
    env = os.environ.copy()
    env["PARE_LLM_MODEL"] = args.model
    env["PARE_NO_BENEFIT_FIXED_CODEC"] = "1"
    env["PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA"] = "1"
    env["SEMZIP_BASELINE_ID"] = VARIANT_ID
    cmd = [
        sys.executable,
        str(root / "run_blocks_pure.py"),
        "--raw-dir",
        str(Path(args.raw_dir)),
        "--datasets",
        *args.datasets,
        "--result-dir",
        str(Path(args.result_dir)),
        "--workers",
        str(args.workers),
        "--block-lines",
        str(args.block_lines),
        "--offline-train-topk",
        "40",
        "--offline-train-logbatcher-per-family",
        "2",
        "--offline-train-logbatcher-min-support",
        "50",
        "--offline-train-max-llm-calls",
        str(args.max_llm_calls),
        "--offline-train-prefix-ratio",
        "0.2",
    ]
    return subprocess.run(cmd, cwd=root, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
