#!/usr/bin/env python3
"""Bound scratch to in-flight blocks; retain the existing semantic/native codecs."""
from __future__ import annotations

import argparse
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

from semantic_codec_frontend import (
    SEMANTIC_VERSION, _decode_block, _encode_block, sha256_file,
)


def _json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2), encoding="utf-8")


def _native(command: list[str], cwd: Path, log: Path) -> None:
    completed = subprocess.run(command, cwd=cwd, capture_output=True)
    output = completed.stdout + completed.stderr
    log.write_bytes(output)
    # The inherited decoder can report a per-chunk failure with exit status zero.
    if completed.returncode or b"Error processing chunk" in output:
        raise RuntimeError(f"Native command failed; inspect {log}")


def _line_shape(data: bytes) -> tuple[int, bool]:
    return data.count(b"\n") + int(bool(data) and not data.endswith(b"\n")), data.endswith(b"\n")


def _encode_job(job: dict) -> dict:
    raw = Path(job["raw"])
    archive = Path(job["archive"])
    index, dataset = job["index"], job["dataset"]
    semantic = archive / "semantic" / f"block_{index:05d}.semantic.tar.xz"
    with tempfile.TemporaryDirectory(dir=job["scratch"], prefix="encode-") as name:
        work = Path(name)
        transformed = work / "transformed.log"
        metadata = _encode_block((str(raw), str(transformed), str(semantic),
                                  job["plan"], job["runtime"]))
        original = raw.read_bytes()
        changed = transformed.read_bytes()
        if _line_shape(original) != _line_shape(changed):
            raise ValueError("Semantic transformation changed native block boundaries")
        raw_hash = hashlib.sha256(original).hexdigest()
        del original, changed
        logdir = work / "Logs" / dataset
        logdir.mkdir(parents=True)
        (logdir / f"{dataset}.log").symlink_to(transformed)
        empty = work / "empty_plan.json"
        empty.write_text('{"version":1,"specs":[]}\n', encoding="utf-8")
        _native([job["compressor"], dataset, "text", str(job["block_size"]), "1",
                 "0", "lzma", "normal", str(empty), "--residual-allgroup-or20"],
                work, Path(job["logs"]) / f"compress_{index:05d}.log")
        native_dir = work / "output" / dataset
        native_files = list(native_dir.glob("chunk_*.tar.xz"))
        if len(native_files) != 1 or native_files[0].name != "chunk_0.tar.xz":
            raise ValueError(f"Expected one native chunk for block {index}")
        shutil.move(str(native_files[0]), archive / f"chunk_{index}.tar.xz")
    raw.unlink()
    return {"index": index, "raw_sha256": raw_hash, **metadata}


def _decode_job(job: dict) -> bytes:
    archive, index = Path(job["archive"]), job["index"]
    with tempfile.TemporaryDirectory(dir=job["scratch"], prefix="decode-") as name:
        work = Path(name)
        source = work / "input"
        source.mkdir()
        chunk = f"chunk_{index}.tar.xz"
        (source / chunk).symlink_to(archive / chunk)
        transformed, decoded = work / "transformed.log", work / "decoded.log"
        _native([job["decompressor"], str(source), str(transformed), "1"], work,
                Path(job["logs"]) / f"decompress_{index:05d}.log")
        _decode_block((str(transformed), str(archive / "semantic" / job["semantic"]),
                       str(decoded), job["runtime"]))
        return decoded.read_bytes()


def encode(raw: Path, plan: Path, runtime: Path, result: Path, dataset: str,
           block_size: int, workers: int) -> dict:
    started = time.perf_counter()
    if not raw.is_file() or not raw.stat().st_size:
        raise ValueError("This bounded runner requires a nonempty input file")
    if json.loads(plan.read_text())["dataset"] != dataset:
        raise ValueError("Plan dataset and archive dataset disagree")
    if not dataset or Path(dataset).name != dataset or dataset in {".", ".."}:
        raise ValueError("Dataset must be a single safe filename component")
    if result.exists() and any(result.iterdir()):
        raise ValueError(f"Refusing to overwrite {result}")
    archive = result / "archive"
    (archive / "semantic").mkdir(parents=True)
    logs = result / "logs"
    logs.mkdir()
    source_hash = hashlib.sha256()
    blocks: list[dict] = []
    input_bytes = 0
    backend = Path(__file__).resolve().parent
    with tempfile.TemporaryDirectory(dir=result, prefix="scratch-") as name:
        scratch = Path(name)

        def jobs():
            nonlocal input_bytes
            with raw.open("rb") as source:
                index = 0
                while True:
                    part = scratch / f"block_{index:05d}.log"
                    count = 0
                    with part.open("wb") as target:
                        for _ in range(block_size):
                            line = source.readline()
                            if not line:
                                break
                            target.write(line)
                            source_hash.update(line)
                            input_bytes += len(line)
                            count += 1
                    if not count:
                        part.unlink()
                        return
                    yield {"index": index, "raw": str(part), "dataset": dataset,
                           "block_size": block_size, "archive": str(archive),
                           "scratch": name, "plan": str(plan), "runtime": str(runtime),
                           "compressor": str(backend / "Delog_plan_compress"), "logs": str(logs)}
                    index += 1

        iterator = iter(jobs())
        with ProcessPoolExecutor(max_workers=workers) as pool:
            pending = set()
            exhausted = False
            while pending or not exhausted:
                while not exhausted and len(pending) < workers:
                    job = next(iterator, None)
                    if job is None:
                        exhausted = True
                    else:
                        pending.add(pool.submit(_encode_job, job))
                if pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        blocks.append(future.result())
    blocks.sort(key=lambda block: block["index"])
    manifest = {"version": SEMANTIC_VERSION, "dataset": dataset,
                "block_size": block_size, "block_count": len(blocks),
                "semantic_archives": [b["semantic_archive"] for b in blocks]}
    (archive / "semantic_manifest.json").write_text(
        json.dumps(manifest, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    elapsed = time.perf_counter() - started
    semantic_bytes = sum(p.stat().st_size for p in (archive / "semantic").iterdir())
    native_bytes = sum(p.stat().st_size for p in archive.glob("chunk_*.tar.xz"))
    total = semantic_bytes + native_bytes + (archive / "semantic_manifest.json").stat().st_size
    return {"version": "SEMZIP-V6-EXPLICIT-EXECUTION-ENV-R36-20260915", "dataset": dataset,
            "archive_dir": str(archive), "raw_bytes": input_bytes, "archive_bytes": total,
            "semantic_archive_bytes": semantic_bytes, "delog_archive_bytes": native_bytes,
            "compression_ratio": input_bytes / total, "raw_sha256": source_hash.hexdigest(),
            "block_size": block_size, "workers": workers, "blocks": blocks,
            "online_compression_seconds": elapsed,
            "online_compression_mbps": input_bytes / 1_000_000 / elapsed,
            "online_compression_scope": "Encode function entry through scratch cleanup and final archive manifest; excludes imports, training, standalone decode and summary writing",
            "plan_sha256": sha256_file(plan), "compressor_sha256": sha256_file(backend / "Delog_plan_compress"),
            "max_inflight_blocks": workers, "llm_api_calls": 0,
            "api_mode": "same-plan execution-harness control"}


def decode(archive: Path, runtime: Path, result: Path, workers: int,
           output: Path | None = None, expected: list[dict] | None = None) -> dict:
    """Decode from archive+runtime only; expected hashes are checked by the caller."""
    started = time.perf_counter()
    manifest = json.loads((archive / "semantic_manifest.json").read_text())
    count = manifest["block_count"]
    if manifest["version"] != SEMANTIC_VERSION:
        raise ValueError("Unsupported semantic archive version")
    if expected is not None and len(expected) != count:
        raise ValueError("Verification block count disagrees with archive")
    names = [f"block_{i:05d}.semantic.tar.xz" for i in range(count)]
    if count < 1 or manifest["semantic_archives"] != names:
        raise ValueError("Unsupported or inconsistent semantic manifest")
    if sorted(p.name for p in archive.glob("chunk_*.tar.xz")) != sorted(
            f"chunk_{i}.tar.xz" for i in range(count)):
        raise ValueError("Missing or extra native chunks")
    if sorted(p.name for p in (archive / "semantic").iterdir()) != names:
        raise ValueError("Missing or extra semantic archives")
    result.mkdir(parents=True, exist_ok=True)
    logs = result / "decode_logs"
    logs.mkdir(exist_ok=True)
    backend = Path(__file__).resolve().parent
    digest, decoded_bytes, failed = hashlib.sha256(), 0, []
    target = output.open("xb") if output is not None else None
    try:
        with tempfile.TemporaryDirectory(dir=result, prefix="decode-scratch-") as scratch:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                pending = deque()
                next_index = 0
                for index in range(count):
                    while next_index < count and len(pending) < workers:
                        job = {"archive": str(archive), "index": next_index,
                               "semantic": names[next_index], "scratch": scratch,
                               "decompressor": str(backend / "decompress"),
                               "runtime": str(runtime), "logs": str(logs)}
                        pending.append(pool.submit(_decode_job, job))
                        next_index += 1
                    data = pending.popleft().result()
                    digest.update(data)
                    decoded_bytes += len(data)
                    if target is not None:
                        target.write(data)
                    if expected is not None and (
                        len(data) != expected[index]["raw_bytes"] or
                        hashlib.sha256(data).hexdigest() != expected[index]["raw_sha256"]
                    ):
                        failed.append(index)
        if target is not None:
            target.close()
            target = None
    finally:
        if target is not None:
            target.close()
    elapsed = time.perf_counter() - started
    return {"decoded_sha256": digest.hexdigest(), "decoded_bytes": decoded_bytes,
            "verified_blocks": count if expected is not None else 0,
            "decoded_blocks": count, "failed_blocks": failed,
            "decode_seconds": elapsed, "decode_mbps": decoded_bytes / 1_000_000 / elapsed,
            "decode_sink": "file" if output is not None else "ordered full-byte SHA sink"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("encode", "decode", "benchmark"))
    parser.add_argument("--input", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--dataset")
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--semzip-source", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--block-size", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if args.workers < 1 or args.block_size < 1:
        parser.error("workers and block size must be positive")
    result, runtime = args.result.resolve(), args.semzip_source.resolve()
    if args.mode == "decode":
        if args.archive is None or args.output is None:
            parser.error("decode requires --archive and --output")
        summary = decode(args.archive.resolve(), runtime, result, args.workers, args.output.resolve())
    else:
        if args.input is None or args.plan is None or not args.dataset:
            parser.error("encode/benchmark require --input, --plan and --dataset")
        summary = encode(args.input.resolve(), args.plan.resolve(), runtime, result,
                         args.dataset, args.block_size, args.workers)
        _json(result / "encode_summary.json", summary)
        if args.mode == "benchmark":
            summary.update(decode(Path(summary["archive_dir"]), runtime, result,
                                  args.workers, expected=summary["blocks"]))
            summary["sha_pass"] = (summary["raw_sha256"] == summary["decoded_sha256"] and
                                   summary["raw_bytes"] == summary["decoded_bytes"] and
                                   not summary["failed_blocks"])
    _json(result / "summary.json", summary)
    print(json.dumps({k: v for k, v in summary.items() if k != "blocks"}, indent=2))
    if summary.get("sha_pass") is False:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
