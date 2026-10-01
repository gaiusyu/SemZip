#!/usr/bin/env python3
"""Run the SEMZIP-WIDTHGUARD-OR20-IPV4PLAIN-20260707 baseline.

This wrapper intentionally exposes only benchmark-level knobs.  The algorithm
configuration lives in pure_args_defaults.json and the current code snapshot.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


BASELINE_ID = "SEMZIP-WIDTHGUARD-OR20-IPV4PLAIN-20260707"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Run {BASELINE_ID}")
    parser.add_argument("--raw-dir", required=True, help="Directory containing raw LogHub .log files")
    parser.add_argument("--datasets", nargs="+", required=True, help="Dataset names without .log suffix")
    parser.add_argument("--result-dir", required=True, help="Output result directory")
    parser.add_argument("--workers", type=int, default=8, help="Compression workers; paper runs use 8")
    parser.add_argument("--block-lines", type=int, default=100_000, help="Lines per block")
    parser.add_argument("--model", default=os.environ.get("PARE_LLM_MODEL", "gpt-4o"))
    parser.add_argument("--max-llm-calls", type=int, default=80, help="Offline training API call budget")
    parser.add_argument("--replay-only", action="store_true", help="Use existing cache/replay when available")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent
    env = os.environ.copy()
    env["PARE_LLM_MODEL"] = args.model
    # Current baseline fixed-codec route: no archive-level codec racing in the
    # online phase; structure features choose the codec path.
    env["PARE_NO_BENEFIT_FIXED_CODEC"] = "1"
    env["PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA"] = "1"
    env["SEMZIP_BASELINE_ID"] = BASELINE_ID
    max_calls = "0" if args.replay_only else str(args.max_llm_calls)
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
        max_calls,
    ]
    return subprocess.run(cmd, cwd=root, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
