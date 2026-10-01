#!/usr/bin/env python3
"""Run the frozen class-wise SemZip protocol on the LogHub-16 benchmark."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path


BENCHMARK_ID = "SEMZIP-CLASSWISE-FROZEN-4W-WALL-LOSSLESSFIX1-20260716"
DEFAULT_DATASETS = [
    "Apache",
    "Proxifier",
    "Linux",
    "Zookeeper",
    "Mac",
    "OpenSSH",
    "Android",
    "Hadoop",
    "HealthApp",
    "HPC",
    "OpenStack",
    "BGL",
    "HDFS",
    "Spark",
    "Windows",
    "Thunderbird",
]


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_dataset_summary(path: Path) -> dict[str, object] | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list) or len(payload) != 1 or not isinstance(payload[0], dict):
        return None
    return payload[0]


def is_complete(result_dir: Path) -> bool:
    summary = read_dataset_summary(result_dir / "summary.json")
    return bool(summary and summary.get("block_status") == "PASS" and summary.get("full_sha") == "PASS")


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def collect_results(root: Path, datasets: list[str]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    summaries: list[dict[str, object]] = []
    blocks: list[dict[str, object]] = []
    for dataset in datasets:
        dataset_root = root / "datasets" / dataset
        summary = read_dataset_summary(dataset_root / "summary.json")
        if summary is not None:
            summaries.append(summary)
        block_csv = dataset_root / "blocks.csv"
        if block_csv.is_file():
            with block_csv.open(newline="", encoding="utf-8") as handle:
                blocks.extend(dict(row) for row in csv.DictReader(handle))
    write_csv(root / "summary.csv", summaries)
    write_csv(root / "blocks.csv", blocks)
    (root / "summary.json").write_text(json.dumps(summaries, indent=2, sort_keys=True), encoding="utf-8")
    return summaries, blocks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=BENCHMARK_ID)
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--result-root", required=True)
    parser.add_argument("--datasets", nargs="+", default=DEFAULT_DATASETS)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--block-lines", type=int, default=100_000)
    parser.add_argument("--model", default=os.environ.get("PARE_LLM_MODEL", "gpt-4o"))
    parser.add_argument("--max-llm-calls", type=int, default=80)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.result_root)
    root.mkdir(parents=True, exist_ok=True)
    source_root = Path(__file__).resolve().parent
    raw_root = Path(args.raw_dir)
    manifest = {
        "benchmark_id": BENCHMARK_ID,
        "method_id": "SEMZIP-CLASSWISE-PROPOSAL-20260712",
        "workers": args.workers,
        "block_lines": args.block_lines,
        "model": args.model,
        "online_evolution": False,
        "offline_prefix_ratio": 0.2,
        "offline_min_prefix_lines": args.block_lines,
        "datasets": args.datasets,
        "core_hashes": {
            name: sha256_path(source_root / name)
            for name in (
                "semzip_pure.py",
                "pare_dataset_extract.py",
                "pare_rank_proto.py",
                "run_semzip_classwise_proposal.py",
            )
        },
        "inputs": {
            dataset: {
                "path": str(raw_root / f"{dataset}.log"),
                "bytes": (raw_root / f"{dataset}.log").stat().st_size,
            }
            for dataset in args.datasets
        },
    }
    (root / "benchmark_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    for dataset in args.datasets:
        dataset_root = root / "datasets" / dataset
        if is_complete(dataset_root):
            print(f"SKIP {dataset}: verified result already exists", flush=True)
            continue
        log_dir = root / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(source_root / "run_semzip_classwise_proposal.py"),
            "--raw-dir",
            str(raw_root),
            "--datasets",
            dataset,
            "--result-dir",
            str(dataset_root),
            "--workers",
            str(args.workers),
            "--block-lines",
            str(args.block_lines),
            "--model",
            args.model,
            "--max-llm-calls",
            str(args.max_llm_calls),
        ]
        print(f"START {dataset}", flush=True)
        started = time.perf_counter()
        with (log_dir / f"{dataset}.log").open("w", encoding="utf-8") as log:
            process = subprocess.run(command, cwd=source_root, stdout=log, stderr=subprocess.STDOUT)
        elapsed = time.perf_counter() - started
        collect_results(root, args.datasets)
        print(f"END {dataset} status={process.returncode} elapsed={elapsed:.3f}s", flush=True)
        if process.returncode != 0 or not is_complete(dataset_root):
            return process.returncode or 1

    summaries, _blocks = collect_results(root, args.datasets)
    return 0 if len(summaries) == len(args.datasets) else 1


if __name__ == "__main__":
    raise SystemExit(main())
