#!/usr/bin/env python3
"""Run SemZip function codecs followed by a regex-free DeLog residual backend."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import time
from pathlib import Path

from run_experiment import verify_blocks
from semantic_codec_frontend import (
    SEMANTIC_VERSION,
    decode_file_parallel,
    encode_file_parallel,
    sha256_file,
)


def run_logged(command: list[str], cwd: Path, log_path: Path) -> float:
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        completed = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
    elapsed = time.perf_counter() - started
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}); see {log_path}")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--semzip-source", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--block-size", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--api-mode", default="frozen-replay")
    parser.add_argument("--delog-residual-allgroup-or20", action="store_true")
    args = parser.parse_args()

    project = Path(__file__).resolve().parent
    compressor = project / "Delog_plan_compress"
    decompressor = project / "decompress"
    raw_path = args.input.resolve()
    plan_path = args.plan.resolve()
    semzip_source = args.semzip_source.resolve()
    result = args.result.resolve()
    for path in (compressor, decompressor, raw_path, plan_path, semzip_source):
        if not path.exists():
            raise FileNotFoundError(path)
    if result.exists() and any(result.iterdir()):
        raise RuntimeError(f"Refusing to overwrite nonempty result directory: {result}")
    result.mkdir(parents=True, exist_ok=True)

    work = result / "work"
    transformed_path = work / "transformed.log"
    intermediate_path = result / "decoded_residual.log"
    decoded_path = result / "decoded.log"
    staged_semantic_dir = work / "semantic_archives"
    log_dir = work / "Logs" / args.dataset
    log_dir.mkdir(parents=True, exist_ok=True)

    semantic_started = time.perf_counter()
    semantic_manifest = encode_file_parallel(
        raw_path,
        transformed_path,
        staged_semantic_dir,
        plan_path,
        semzip_source,
        args.block_size,
        args.workers,
        work / "semantic_encode_blocks",
    )
    semantic_seconds = time.perf_counter() - semantic_started
    input_link = log_dir / f"{args.dataset}.log"
    input_link.symlink_to(transformed_path)

    empty_plan = work / "empty_plan.json"
    empty_plan.write_text('{"version":1,"specs":[]}\n', encoding="utf-8")
    compress_command = [
        str(compressor),
        args.dataset,
        "text",
        str(args.block_size),
        str(args.workers),
        "0",
        "lzma",
        "normal",
        str(empty_plan),
    ]
    if args.delog_residual_allgroup_or20:
        compress_command.append("--residual-allgroup-or20")
    backend_seconds = run_logged(compress_command, work, result / "compress.log")

    archive_dir = work / "output" / args.dataset
    semantic_archive_dir = archive_dir / "semantic"
    shutil.copytree(staged_semantic_dir, semantic_archive_dir)
    package_manifest = {
        "version": SEMANTIC_VERSION,
        "dataset": args.dataset,
        "block_size": args.block_size,
        "block_count": semantic_manifest["block_count"],
        "semantic_archives": [
            block["semantic_archive"] for block in semantic_manifest["blocks"]
        ],
    }
    manifest_path = archive_dir / "semantic_manifest.json"
    manifest_path.write_text(
        json.dumps(package_manifest, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )

    decompress_command = [
        str(decompressor),
        str(archive_dir),
        str(intermediate_path),
        str(args.workers),
    ]
    backend_decode_seconds = run_logged(
        decompress_command,
        work,
        result / "decompress.log",
    )
    restore_started = time.perf_counter()
    inverse_manifest = decode_file_parallel(
        intermediate_path,
        decoded_path,
        semantic_archive_dir,
        semzip_source,
        args.block_size,
        args.workers,
        work / "semantic_decode_blocks",
    )
    semantic_decode_seconds = time.perf_counter() - restore_started

    raw_sha = sha256_file(raw_path)
    decoded_sha = sha256_file(decoded_path)
    block_count, failed_blocks = verify_blocks(raw_path, decoded_path, args.block_size)
    sha_pass = raw_sha == decoded_sha and not failed_blocks
    chunk_archives = sorted(archive_dir.glob("chunk_*.tar.xz"))
    semantic_archives = sorted(semantic_archive_dir.glob("block_*.semantic.tar.xz"))
    delog_archive_bytes = sum(path.stat().st_size for path in chunk_archives)
    semantic_archive_bytes = sum(path.stat().st_size for path in semantic_archives)
    archive_bytes = delog_archive_bytes + semantic_archive_bytes + manifest_path.stat().st_size
    raw_bytes = raw_path.stat().st_size
    online_seconds = semantic_seconds + backend_seconds

    summary = {
        "version": SEMANTIC_VERSION,
        "dataset": args.dataset,
        "input": str(raw_path),
        "archive_dir": str(archive_dir),
        "api_mode": args.api_mode,
        "delog_residual_allgroup_or20": args.delog_residual_allgroup_or20,
        "block_size": args.block_size,
        "workers": args.workers,
        "raw_bytes": raw_bytes,
        "archive_bytes": archive_bytes,
        "delog_archive_bytes": delog_archive_bytes,
        "semantic_archive_bytes": semantic_archive_bytes,
        "compression_ratio": raw_bytes / archive_bytes,
        "semantic_frontend_seconds": semantic_seconds,
        "delog_backend_seconds": backend_seconds,
        "online_compression_seconds": online_seconds,
        "online_compression_mbps": raw_bytes / 1_000_000 / online_seconds,
        "delog_decode_seconds": backend_decode_seconds,
        "semantic_decode_seconds": semantic_decode_seconds,
        "chunk_archives": len(chunk_archives),
        "semantic_archives": len(semantic_archives),
        "verified_blocks": block_count,
        "failed_blocks": failed_blocks,
        "raw_sha256": raw_sha,
        "decoded_sha256": decoded_sha,
        "sha_pass": sha_pass,
        "semantic_manifest": semantic_manifest,
        "inverse_manifest": inverse_manifest,
        "plan_sha256": sha256_file(plan_path),
        "compressor_sha256": sha256_file(compressor),
    }
    (result / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))
    if sha_pass:
        transformed_path.unlink(missing_ok=True)
        intermediate_path.unlink(missing_ok=True)
    else:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
