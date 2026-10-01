#!/usr/bin/env python3
"""R69 semantic-archive guard with a fixed residual-only recovery path.

This is an adapter around an unchanged run_bounded_semantic.py. Before the
native backend sees a transformed block, unpack its real semantic archive,
decode it using stored metadata, and compare every byte with the original.
Semantic contract failures use an empty extraction plan and an empty *fixed*
storage policy. There is no model call, storage search, or cross-block state.

The existing encoder timer includes both attempts, guard decoding and logging.
Native/backend failures and infrastructure errors fail closed. This is not a
new native archive codec or a proof against arbitrary backend/disk corruption;
the benchmark still independently decodes every complete archive and checks SHA.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import traceback

GUARD_VERSION = "R69-SEMANTIC-ARCHIVE-GUARD-1"


class SemanticArchiveMismatch(ValueError):
    """The actual written semantic archive cannot restore the original bytes."""


def _json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _configure(runtime: Path):
    backend = runtime.resolve().parent / "backend"
    for path in (runtime.resolve().parent, backend):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    bounded = importlib.import_module("run_bounded_semantic")
    frontend = importlib.import_module("semantic_codec_frontend")
    if Path(bounded.__file__).resolve().parent != backend:
        raise RuntimeError("Refusing to mix backend source roots in one process")
    if not hasattr(bounded, "_r69_original_encode_job"):
        bounded._r69_original_encode_job = bounded._encode_job
    # The worker wrapper installs the patch again after a multiprocessing spawn;
    # Linux fork workers inherit it. Every worker has process-local environment.
    bounded._encode_block = _guarded_encode_block
    bounded._encode_job = _guarded_encode_job
    return bounded, frontend


def _validate_fixed_policy() -> None:
    path = os.environ.get("SEMZIP_R54_PLAN")
    if not path:
        raise RuntimeError("R69 requires an explicit frozen SEMZIP_R54_PLAN")
    policy = json.loads(Path(path).read_text(encoding="utf-8"))
    if policy.get("online_search") is not False:
        raise ValueError("R69 storage policy must explicitly forbid online search")
    if policy["recipe"].get("numeric") == "adaptive":
        raise ValueError("R69 cannot run an adaptive numeric storage policy")
    if not isinstance(policy["programs"], list):
        raise ValueError("Invalid fixed storage program list")


def _failure_kind(exc: Exception) -> tuple[str, bool]:
    # Missing files, memory exhaustion, invalid source syntax, import failures,
    # and arbitrary runtime/system errors must not become apparent successes.
    if isinstance(exc, (OSError, MemoryError, ImportError, SyntaxError)):
        return "infrastructure_or_source_error", False
    if isinstance(exc, SemanticArchiveMismatch):
        return "semantic_archive_mismatch", True
    if isinstance(exc, IndexError):
        return "semantic_value_or_layout_index", True
    if isinstance(exc, (ValueError, KeyError, TypeError, OverflowError,
                        ZeroDivisionError, AssertionError)):
        return "semantic_contract_error", True
    return "unexpected_error", False


def _error(exc: Exception, phase: str) -> dict:
    kind, recoverable = _failure_kind(exc)
    return {"phase": phase, "classification": kind, "recoverable": recoverable,
            "exception": type(exc).__name__, "message": str(exc),
            "traceback": traceback.format_exc()}


@contextmanager
def _empty_policy(path: Path):
    previous = os.environ.get("SEMZIP_R54_PLAN")
    os.environ["SEMZIP_R54_PLAN"] = str(path)
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("SEMZIP_R54_PLAN", None)
        else:
            os.environ["SEMZIP_R54_PLAN"] = previous


def _attempt(job: tuple[str, str, str, str, str], decoded: Path) -> tuple[dict, float]:
    raw, transformed, archive, _plan, runtime = (Path(item) for item in job)
    bounded, frontend = _configure(runtime)
    metadata = frontend._encode_block(job)
    start = time.perf_counter()
    frontend._decode_block((str(transformed), str(archive), str(decoded), str(runtime)))
    original = raw.read_bytes()
    restored = decoded.read_bytes()
    if restored != original:
        raise SemanticArchiveMismatch("Actual semantic archive replay changed original bytes")
    if bounded._line_shape(original) != bounded._line_shape(transformed.read_bytes()):
        raise SemanticArchiveMismatch("Semantic replay changed physical LF block boundaries")
    metadata["semantic_archive_verified_sha256"] = hashlib.sha256(restored).hexdigest()
    return metadata, time.perf_counter() - start


def _guarded_encode_block(job: tuple[str, str, str, str, str]) -> dict:
    raw, transformed, archive, plan, runtime = (Path(item) for item in job)
    # Preflight is deliberately outside semantic recovery: a missing/corrupt
    # frozen policy must not silently turn an experiment into another method.
    _validate_fixed_policy()
    plan_data = json.loads(plan.read_text(encoding="utf-8"))
    dataset = plan_data["dataset"]
    event = {"version": GUARD_VERSION, "raw_block": raw.name,
             "plan_sha256": hashlib.sha256(plan.read_bytes()).hexdigest(),
             "fallback_used": False, "archive_verified": False,
             "failed_attempt_seconds": 0.0, "recovery_seconds": 0.0,
             "archive_guard_seconds": 0.0, "errors": []}
    # Logs are experiment evidence, never needed by the decoder or archive cost.
    event_path = archive.parent.parent.parent / "logs" / ("guard_" + raw.stem + ".json")
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(dir=transformed.parent, prefix="semantic-guard-") as name:
        work = Path(name)
        try:
            try:
                metadata, guard_seconds = _attempt(job, work / "decoded.log")
                event["archive_guard_seconds"] = guard_seconds
            except Exception as exc:
                event["failed_attempt_seconds"] = time.perf_counter() - started
                detail = _error(exc, "original_semantic_attempt")
                event["errors"].append(detail)
                if not detail["recoverable"]:
                    raise
                event["fallback_used"] = True
                # Remove partial payloads so an interrupted or buggy first
                # attempt cannot leave an extra file counted as a valid archive.
                archive.unlink(missing_ok=True)
                transformed.unlink(missing_ok=True)
                empty_plan = work / "empty_extraction.json"
                _json(empty_plan, {"version": 1, "dataset": dataset,
                                   "specs": [], "placeholders": {}})
                empty_storage = work / "empty_storage.json"
                _json(empty_storage, {
                    "version": 1, "dataset": dataset, "online_search": False,
                    "recipe": {"generic_representation": "file_alias"},
                    "programs": [], "column_numeric": {}, "numeric_modes": {},
                    "subfield_ids": {}, "unseen_column_numeric_default": "delta"})
                recovery_started = time.perf_counter()
                recovery_job = (str(raw), str(transformed), str(archive), str(empty_plan), str(runtime))
                try:
                    with _empty_policy(empty_storage):
                        metadata, guard_seconds = _attempt(recovery_job, work / "recovered.log")
                    event["archive_guard_seconds"] = guard_seconds
                except Exception as fallback_exc:
                    event["errors"].append(_error(fallback_exc, "residual_only_semantic_recovery"))
                    raise  # No recursion, no third candidate, no raw-format invention.
                finally:
                    event["recovery_seconds"] = time.perf_counter() - recovery_started
            event["archive_verified"] = True
        finally:
            event["semantic_stage_seconds"] = time.perf_counter() - started
            _json(event_path, event)
    # Full tracebacks are kept in the evidence log; compact decision records are
    # also part of the measured block summary, not the decoding manifest.
    metadata["semantic_guard"] = {
        **{key: value for key, value in event.items() if key != "errors"},
        "errors": [{key: value for key, value in item.items() if key != "traceback"}
                   for item in event["errors"]]}
    return metadata


def _guarded_encode_job(job: dict) -> dict:
    bounded, _frontend = _configure(Path(job["runtime"]))
    try:
        return bounded._r69_original_encode_job(job)
    except Exception as exc:
        detail = _error(exc, "block_worker_fail_closed")
        detail.update(block_index=job["index"], dataset=job["dataset"])
        _json(Path(job["logs"]) / f"worker_failure_{job['index']:05d}.json", detail)
        raise


def encode(raw: Path, plan: Path, runtime: Path, result: Path, dataset: str,
           block_size: int, workers: int) -> dict:
    bounded, _frontend = _configure(runtime)
    _validate_fixed_policy()
    summary = bounded.encode(raw, plan, runtime, result, dataset, block_size, workers)
    guards = [block["semantic_guard"] for block in summary["blocks"]]
    summary.update(
        guard_version=GUARD_VERSION,
        semantic_guard_verified_blocks=sum(item["archive_verified"] for item in guards),
        semantic_fallback_blocks=sum(item["fallback_used"] for item in guards),
        semantic_failed_attempt_seconds_sum=sum(item["failed_attempt_seconds"] for item in guards),
        semantic_recovery_seconds_sum=sum(item["recovery_seconds"] for item in guards),
        semantic_archive_guard_seconds_sum=sum(item["archive_guard_seconds"] for item in guards),
        guard_timing_note="Per-worker wall durations overlap; never add their sum to dataset wall time",
        online_compression_scope="Original bounded encode timer includes semantic attempts, actual archive inverse guard, residual-only recovery, native encoding, logs, scratch cleanup and manifest; excludes imports, offline work, standalone full decode and summary serialization",
        guard_boundary="Semantic archives are checked before native compression; native errors fail closed, full archive verification remains a separate benchmark decode",
        api_mode="frozen-plan-with-fixed-semantic-failure-recovery")
    return summary


def decode(archive: Path, runtime: Path, result: Path, workers: int,
           output: Path | None = None, expected: list[dict] | None = None) -> dict:
    bounded, _frontend = _configure(runtime)
    return bounded.decode(archive, runtime, result, workers, output, expected)


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
    parser.add_argument("--block-size", type=int, default=100000)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args()
    if args.workers < 1 or args.block_size != 100000:
        parser.error("R69 requires positive workers and exactly 100,000 physical lines per block")
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
            summary["sha_pass"] = (summary["raw_sha256"] == summary["decoded_sha256"]
                                   and summary["raw_bytes"] == summary["decoded_bytes"]
                                   and not summary["failed_blocks"])
    _json(result / "summary.json", summary)
    print(json.dumps({key: value for key, value in summary.items() if key != "blocks"}, indent=2))
    if summary.get("sha_pass") is False:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
