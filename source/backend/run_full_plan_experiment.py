#!/usr/bin/env python3
import argparse
import json
import shutil
import subprocess
import time
from pathlib import Path

from full_plan_frontend import process_file_parallel, sha256_file
from run_experiment import verify_blocks


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
    source_plan = args.plan.resolve()
    result = args.result.resolve()
    for path in (compressor, decompressor, raw_path, source_plan):
        if not path.exists():
            raise FileNotFoundError(path)
    if result.exists() and any(result.iterdir()):
        raise RuntimeError(f"Refusing to overwrite nonempty result directory: {result}")
    result.mkdir(parents=True, exist_ok=True)

    work = result / "work"
    transformed_path = work / "transformed.log"
    intermediate_path = result / "decoded_intermediate.log"
    decoded_path = result / "decoded.log"
    log_dir = work / "Logs" / args.dataset
    log_dir.mkdir(parents=True, exist_ok=True)

    preprocess_start = time.perf_counter()
    frontend_manifest = process_file_parallel(
        raw_path,
        transformed_path,
        source_plan,
        restore=False,
        block_size=args.block_size,
        workers=args.workers,
        temp_dir=work / "forward_blocks",
    )
    preprocess_seconds = time.perf_counter() - preprocess_start
    input_link = log_dir / f"{args.dataset}.log"
    input_link.symlink_to(transformed_path)

    empty_plan = work / "empty_plan.json"
    empty_plan.write_text('{"version":1,"specs":[]}\n', encoding="utf-8")
    compress_command = [
        str(compressor), args.dataset, "text", str(args.block_size), str(args.workers),
        "0", "lzma", "normal", str(empty_plan),
    ]
    backend_seconds = run_logged(compress_command, work, result / "compress.log")

    archive_dir = work / "output" / args.dataset
    archived_plan = archive_dir / "replay_plan.json"
    archived_manifest = archive_dir / "frontend_manifest.json"
    shutil.copy2(source_plan, archived_plan)
    archived_manifest.write_text(json.dumps(frontend_manifest, indent=2), encoding="utf-8")

    decompress_command = [str(decompressor), str(archive_dir), str(intermediate_path), str(args.workers)]
    backend_decode_seconds = run_logged(decompress_command, work, result / "decompress.log")
    restore_start = time.perf_counter()
    inverse_manifest = process_file_parallel(
        intermediate_path,
        decoded_path,
        archived_plan,
        restore=True,
        block_size=args.block_size,
        workers=args.workers,
        temp_dir=work / "inverse_blocks",
    )
    restore_seconds = time.perf_counter() - restore_start

    raw_sha = sha256_file(raw_path)
    decoded_sha = sha256_file(decoded_path)
    block_count, failed_blocks = verify_blocks(raw_path, decoded_path, args.block_size)
    sha_pass = raw_sha == decoded_sha and not failed_blocks
    chunk_archives = sorted(archive_dir.glob("chunk_*.tar.xz"))
    archive_bytes = sum(path.stat().st_size for path in chunk_archives)
    archive_bytes += archived_plan.stat().st_size + archived_manifest.stat().st_size
    raw_bytes = raw_path.stat().st_size
    online_seconds = preprocess_seconds + backend_seconds

    summary = {
        "version": "SEMZIP-DELOG-FULL-PLAN-BACKEND-V2-20260718",
        "dataset": args.dataset,
        "input": str(raw_path),
        "archive_dir": str(archive_dir),
        "api_mode": args.api_mode,
        "block_size": args.block_size,
        "workers": args.workers,
        "raw_bytes": raw_bytes,
        "archive_bytes": archive_bytes,
        "compression_ratio": raw_bytes / archive_bytes,
        "preprocess_seconds": preprocess_seconds,
        "delog_backend_seconds": backend_seconds,
        "online_compression_seconds": online_seconds,
        "online_compression_mbps": raw_bytes / 1_000_000 / online_seconds,
        "delog_decode_seconds": backend_decode_seconds,
        "inverse_seconds": restore_seconds,
        "chunk_archives": len(chunk_archives),
        "verified_blocks": block_count,
        "failed_blocks": failed_blocks,
        "raw_sha256": raw_sha,
        "decoded_sha256": decoded_sha,
        "sha_pass": sha_pass,
        "forward_spec_counts": frontend_manifest["spec_counts"],
        "inverse_spec_counts": inverse_manifest["spec_counts"],
        "plan_sha256": sha256_file(archived_plan),
        "compressor_sha256": sha256_file(compressor),
    }
    (result / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    if sha_pass:
        transformed_path.unlink(missing_ok=True)
        intermediate_path.unlink(missing_ok=True)
    else:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
