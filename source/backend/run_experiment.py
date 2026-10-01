#!/usr/bin/env python3
import argparse
import csv
import hashlib
import itertools
import json
import os
import subprocess
import tarfile
import time
from collections import defaultdict
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_blocks(raw_path: Path, decoded_path: Path, block_size: int) -> tuple[int, list[int]]:
    block_index = 0
    line_index = 0
    failed_blocks: list[int] = []
    raw_digest = hashlib.sha256()
    decoded_digest = hashlib.sha256()

    with raw_path.open("rb") as raw_stream, decoded_path.open("rb") as decoded_stream:
        for raw_line, decoded_line in itertools.zip_longest(raw_stream, decoded_stream):
            if raw_line is None or decoded_line is None:
                failed_blocks.append(block_index)
                break
            raw_digest.update(raw_line)
            decoded_digest.update(decoded_line)
            line_index += 1
            if line_index == block_size:
                if raw_digest.digest() != decoded_digest.digest():
                    failed_blocks.append(block_index)
                block_index += 1
                line_index = 0
                raw_digest = hashlib.sha256()
                decoded_digest = hashlib.sha256()

    if line_index:
        if raw_digest.digest() != decoded_digest.digest():
            failed_blocks.append(block_index)
        block_index += 1
    return block_index, failed_blocks


def aggregate_plan_stats(archives: list[Path]) -> dict[str, dict[str, int]]:
    totals: dict[str, dict[str, int]] = defaultdict(lambda: {"exact": 0, "fallback": 0})
    for archive_path in archives:
        with tarfile.open(archive_path, "r:xz") as archive:
            member = archive.getmember("plan_stats.csv")
            stream = archive.extractfile(member)
            if stream is None:
                raise RuntimeError(f"Missing plan_stats.csv in {archive_path}")
            rows = csv.DictReader(line.decode("utf-8") for line in stream)
            for row in rows:
                key = f"{row['rule']}:{row['tag']}"
                totals[key]["exact"] += int(row["exact_matches"])
                totals[key]["fallback"] += int(row["fallback_matches"])
    return dict(totals)


def run_logged(command: list[str], cwd: Path, log_path: Path) -> float:
    start = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.perf_counter() - start
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}); see {log_path}")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--block-size", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--api-mode", default="frozen-replay")
    args = parser.parse_args()

    project = Path(__file__).resolve().parent
    compressor = project / "Delog_plan_compress"
    decompressor = project / "decompress"
    raw_path = args.input.resolve()
    plan_path = args.plan.resolve()
    result_path = args.result.resolve()

    for required in (compressor, decompressor, raw_path, plan_path):
        if not required.exists():
            raise FileNotFoundError(required)
    if result_path.exists() and any(result_path.iterdir()):
        raise RuntimeError(f"Refusing to overwrite nonempty result directory: {result_path}")

    work = result_path / "work"
    log_dir = work / "Logs" / args.dataset
    log_dir.mkdir(parents=True, exist_ok=True)
    input_link = log_dir / f"{args.dataset}.log"
    if input_link.exists() or input_link.is_symlink():
        input_link.unlink()
    input_link.symlink_to(raw_path)

    compress_command = [
        str(compressor), args.dataset, "text", str(args.block_size),
        str(args.workers), "0", "lzma", "normal", str(plan_path),
    ]
    compress_seconds = run_logged(compress_command, work, result_path / "compress.log")

    archive_dir = work / "output" / args.dataset
    archives = sorted(archive_dir.glob("chunk_*.tar.xz"))
    if not archives:
        raise RuntimeError("Compressor produced no chunk archives")
    archive_bytes = sum(path.stat().st_size for path in archives)

    decoded_path = result_path / "decoded.log"
    decompress_command = [str(decompressor), str(archive_dir), str(decoded_path), str(args.workers)]
    decompress_seconds = run_logged(decompress_command, work, result_path / "decompress.log")

    raw_sha256 = sha256_file(raw_path)
    decoded_sha256 = sha256_file(decoded_path)
    block_count, failed_blocks = verify_blocks(raw_path, decoded_path, args.block_size)
    raw_bytes = raw_path.stat().st_size
    sha_pass = raw_sha256 == decoded_sha256 and not failed_blocks
    plan_stats = aggregate_plan_stats(archives)

    summary = {
        "version": "SEMZIP-DELOG-PLAN-BACKEND-V1-20260718",
        "dataset": args.dataset,
        "input": str(raw_path),
        "replay_plan": str(plan_path),
        "replay_plan_sha256": sha256_file(plan_path),
        "api_mode": args.api_mode,
        "block_size": args.block_size,
        "workers": args.workers,
        "raw_bytes": raw_bytes,
        "archive_bytes": archive_bytes,
        "compression_ratio": raw_bytes / archive_bytes,
        "online_compression_seconds": compress_seconds,
        "online_compression_mbps": raw_bytes / 1_000_000 / compress_seconds,
        "decompression_seconds": decompress_seconds,
        "chunk_archives": len(archives),
        "verified_blocks": block_count,
        "failed_blocks": failed_blocks,
        "raw_sha256": raw_sha256,
        "decoded_sha256": decoded_sha256,
        "sha_pass": sha_pass,
        "plan_stats": plan_stats,
        "compressor_sha256": sha256_file(compressor),
        "decompressor_sha256": sha256_file(decompressor),
    }
    (result_path / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if not sha_pass:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
