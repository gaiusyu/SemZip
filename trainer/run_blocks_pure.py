#!/usr/bin/env python3
"""Pure 100k-block runner for semantic_contract_filter_repair_v1.

The old benchmark runner supports many historical presets.  This runner keeps
one workflow only:

1. Build one LogBatcher-style training sample per dataset.
2. Train one shared LLM cache for that dataset.
3. Compress every 100k-line block with online LLM calls disabled.
4. Decode every archive and verify the concatenated SHA.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from pure_config import PureConfig


VALUE_HINT_RE_CHARS = set("./:;=_[]{}()<>@#%+-")


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def token_shape(token: str) -> str:
    out: list[str] = []
    index = 0
    while index < len(token):
        ch = token[index]
        if ch.isdigit():
            end = index + 1
            while end < len(token) and token[end].isdigit():
                end += 1
            out.append(f"D{end - index}")
            index = end
        elif ch.isalpha():
            end = index + 1
            while end < len(token) and token[end].isalpha():
                end += 1
            out.append(f"A{end - index}")
            index = end
        else:
            out.append(ch)
            index += 1
    return "".join(out)


def has_value_hint(token: str) -> bool:
    return any(ch.isdigit() or ch in VALUE_HINT_RE_CHARS for ch in token)


def punctuation_skeleton(line: str) -> str:
    out: list[str] = []
    previous = ""
    for ch in line.rstrip("\r\n"):
        current = ch if (not ch.isalnum() and not ch.isspace()) else " "
        if current == " ":
            previous = ""
            continue
        if current != previous:
            out.append(current)
            previous = current
    return "".join(out)


def logbatcher_token_mask(token: str) -> str:
    return token_shape(token) if has_value_hint(token) else token.lower()


def logbatcher_family_key(line: str) -> str:
    tokens = line.rstrip("\r\n").split()
    masked = [logbatcher_token_mask(token) for token in tokens]
    stable = [mask for mask in masked if "D" not in mask]
    return f"len={len(tokens)}|stable={' '.join(stable[:8])}|punct={punctuation_skeleton(line)}"


def logbatcher_line_features(line: str) -> set[str]:
    tokens = line.rstrip("\r\n").split()
    features = {logbatcher_token_mask(token) for token in tokens}
    features.add("P:" + punctuation_skeleton(line))
    return features


def write_logbatcher_sample(
    input_path: Path,
    output_path: Path,
    *,
    max_families: int,
    per_family: int,
    min_support: int,
    max_input_lines: int | None = None,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    family_stats: dict[str, dict[str, object]] = {}
    with input_path.open("rb") as source:
        for line_index, raw_line in enumerate(source):
            if max_input_lines is not None and line_index >= max_input_lines:
                break
            line = raw_line.decode("latin-1")
            key = logbatcher_family_key(line)
            stats = family_stats.setdefault(key, {"support": 0, "examples": [], "features": []})
            stats["support"] = int(stats["support"]) + 1
            examples = stats["examples"]
            features_list = stats["features"]
            if not isinstance(examples, list) or not isinstance(features_list, list):
                continue
            if len(examples) >= max(1, per_family):
                continue
            features = logbatcher_line_features(line)
            if any(raw_line == previous for previous in examples):
                continue
            if not features_list:
                examples.append(raw_line)
                features_list.append(features)
                continue
            min_distance = 1.0
            for previous in features_list:
                union = len(features | previous) or 1
                min_distance = min(min_distance, 1.0 - len(features & previous) / union)
            if min_distance > 0.15 or len(examples) + 1 >= max(1, per_family):
                examples.append(raw_line)
                features_list.append(features)

    ranked = sorted(
        (
            (int(stats["support"]), key, stats)
            for key, stats in family_stats.items()
            if int(stats["support"]) >= max(1, min_support)
        ),
        key=lambda item: (-item[0], item[1]),
    )
    if max_families > 0:
        ranked = ranked[:max_families]

    written = 0
    with output_path.open("wb") as target:
        for _support, _key, stats in ranked:
            examples = stats.get("examples", [])
            if not isinstance(examples, list):
                continue
            for raw_line in examples[: max(1, per_family)]:
                target.write(raw_line)
                written += 1
    return written


def split_blocks(
    input_path: Path,
    output_dir: Path,
    block_lines: int,
) -> tuple[list[dict[str, object]], str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    blocks: list[dict[str, object]] = []
    source_digest = hashlib.sha256()
    block_id = 0
    line_count = 0
    block_bytes = 0
    block_digest = hashlib.sha256()
    current_path: Path | None = None
    current = None
    with input_path.open("rb") as source:
        for line in source:
            if current is None:
                current_path = output_dir / f"block_{block_id:05d}.log"
                current = current_path.open("wb")
            current.write(line)
            source_digest.update(line)
            block_digest.update(line)
            block_bytes += len(line)
            line_count += 1
            if line_count == block_lines:
                current.close()
                blocks.append(
                    {
                        "block_id": block_id,
                        "path": str(current_path),
                        "raw_bytes": block_bytes,
                        "raw_sha256": block_digest.hexdigest(),
                    }
                )
                block_id += 1
                line_count = 0
                block_bytes = 0
                block_digest = hashlib.sha256()
                current_path = None
                current = None
    if current is not None and current_path is not None:
        current.close()
        blocks.append(
            {
                "block_id": block_id,
                "path": str(current_path),
                "raw_bytes": block_bytes,
                "raw_sha256": block_digest.hexdigest(),
            }
        )
    return blocks, source_digest.hexdigest()


def compressor_command(
    source: Path,
    dataset: str,
    output_dir: Path,
    cache_dir: Path,
    *,
    max_llm_calls: int,
    force_llm: bool,
    replay_plan_out: bool,
    semantic_replay_plan_cache: Path | None = None,
    numeric_lattice_decision_cache: Path | None = None,
    residual_schema_plan_in: Path | None = None,
    extra_flags: list[str] | None = None,
) -> list[str]:
    script = Path(__file__).with_name("semzip_pure.py")
    cmd = [
        sys.executable,
        str(script),
        "--input",
        str(source),
        "--dataset",
        dataset,
        "--output-dir",
        str(output_dir),
        "--llm-cache-dir",
        str(cache_dir),
        "--max-llm-calls",
        str(max_llm_calls),
        "--operation-profile",
        "--skip-internal-decode-verify",
    ]
    if force_llm:
        cmd.append("--force-llm")
    if replay_plan_out:
        cmd.extend(["--replay-plan-out", str(output_dir / "replay_plan.json")])
    if semantic_replay_plan_cache is not None and semantic_replay_plan_cache.is_file():
        cmd.extend(["--semantic-replay-plan-cache", str(semantic_replay_plan_cache)])
    if numeric_lattice_decision_cache is not None and numeric_lattice_decision_cache.is_file():
        cmd.extend(["--numeric-lattice-decision-cache", str(numeric_lattice_decision_cache)])
    if residual_schema_plan_in is not None and residual_schema_plan_in.is_file():
        cmd.extend(["--residual-schema-plan-in", str(residual_schema_plan_in)])
    if extra_flags:
        cmd.extend(extra_flags)
    return cmd


def run_command(cmd: list[str], cwd: Path) -> tuple[int, str, str, float]:
    env = os.environ.copy()
    env.setdefault("PYTHONHASHSEED", "0")
    # Pure v1 uses deterministic feature-to-codec routing.  Do not race codec
    # candidates by compressed size in the online stream-save path.
    env.setdefault("PARE_NO_BENEFIT_FIXED_CODEC", "1")
    env.setdefault("PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA", "1")
    start = time.perf_counter()
    proc = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True, env=env)
    return proc.returncode, proc.stdout, proc.stderr, time.perf_counter() - start


def decode_archive(archive_path: Path, decoded_path: Path, restore_root: Path) -> tuple[bool, str, float]:
    script = (
        "from pathlib import Path\n"
        "import semzip_pure as s\n"
        f"root=s.extract_archive(Path({str(archive_path)!r}), Path({str(restore_root)!r}))\n"
        f"Path({str(decoded_path)!r}).write_bytes(s.decode_archive_root(root).encode('latin-1'))\n"
    )
    start = time.perf_counter()
    proc = subprocess.run([sys.executable, "-c", script], cwd=Path(__file__).parent, text=True, capture_output=True)
    return proc.returncode == 0, proc.stderr[-1000:], time.perf_counter() - start


def train_dataset(dataset: str, input_path: Path, result_root: Path, config: PureConfig) -> dict[str, object]:
    sample_path = result_root / "training_samples" / f"{dataset}.log"
    cache_dir = result_root / "llm_caches" / dataset / "shared"
    work_dir = result_root / "training_work" / dataset
    with input_path.open("rb") as source:
        total_lines = sum(1 for _line in source)
    prefix_ratio = config.offline_prefix_ratio
    scanned_lines = min(
        total_lines,
        max(1, math.ceil(total_lines * prefix_ratio), config.offline_min_prefix_lines),
    )
    sampled = write_logbatcher_sample(
        input_path,
        sample_path,
        max_families=config.offline_topk,
        per_family=config.offline_logbatcher_per_family,
        min_support=config.offline_logbatcher_min_support,
        max_input_lines=scanned_lines,
    )
    extra_flags = [
        "--offline-family-topk-query",
        "--offline-family-topk",
        str(config.offline_topk),
        "--offline-family-topk-min-support",
        "1",
    ]
    if config.offline_batch_query:
        extra_flags.append("--offline-family-batch-query")
    extra_flags.extend(
        [
            "--offline-family-batch-max-examples",
            str(config.offline_batch_examples),
            "--offline-family-api-workers",
            str(config.offline_api_workers),
            "--llm-template-index-cache",
            "--llm-function-trie-cache",
            "--llm-function-trigger-cache",
        ]
    )
    cmd = compressor_command(
        sample_path,
        dataset,
        work_dir,
        cache_dir,
        max_llm_calls=config.offline_max_llm_calls,
        force_llm=True,
        replay_plan_out=True,
        semantic_replay_plan_cache=None,
        numeric_lattice_decision_cache=None,
        residual_schema_plan_in=None,
        extra_flags=extra_flags,
    )
    code, stdout, stderr, seconds = run_command(cmd, Path(__file__).parent)
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "stdout.log").write_text(stdout, encoding="utf-8", errors="ignore")
    (work_dir / "stderr.log").write_text(stderr, encoding="utf-8", errors="ignore")
    summary_path = work_dir / "summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.is_file() else {}
    return {
        "sampled_lines": sampled,
        "total_lines": total_lines,
        "scanned_lines": scanned_lines,
        "prefix_ratio": prefix_ratio,
        "seconds": seconds,
        "status": "PASS" if code == 0 else "FAIL",
        "api_calls": int(summary.get("llm_runtime_stats", {}).get("api_calls", 0)),
        "cache_dir": str(cache_dir),
        "replay_plan": str(work_dir / "replay_plan.json"),
        "error": stderr[-1000:] if code else "",
    }


def compress_block(job: dict[str, object]) -> dict[str, object]:
    dataset = str(job["dataset"])
    block_id = int(job["block_id"])
    block_path = Path(str(job["block_path"]))
    block_root = Path(str(job["block_root"]))
    cache_dir = Path(str(job["cache_dir"]))
    replay_plan = Path(str(job["replay_plan"])) if str(job.get("replay_plan", "")) else None
    replay_scope = str(job.get("replay_scope", "semantic"))
    extra_flags = list(job.get("extra_flags", []))
    block_root.mkdir(parents=True, exist_ok=True)
    work_dir = block_root / "work"
    cmd = compressor_command(
        block_path,
        dataset,
        work_dir,
        cache_dir,
        max_llm_calls=0,
        force_llm=False,
        replay_plan_out=True,
        semantic_replay_plan_cache=replay_plan,
        numeric_lattice_decision_cache=replay_plan if replay_scope == "full" else None,
        residual_schema_plan_in=replay_plan if replay_scope == "full" else None,
        extra_flags=extra_flags,
    )
    code, stdout, stderr, seconds = run_command(cmd, Path(__file__).parent)
    (block_root / "stdout.log").write_text(stdout, encoding="utf-8", errors="ignore")
    (block_root / "stderr.log").write_text(stderr, encoding="utf-8", errors="ignore")
    raw_bytes = int(job["raw_bytes"])
    raw_sha256 = str(job["raw_sha256"])
    if code != 0:
        return {
            "dataset": dataset,
            "block_id": block_id,
            "raw_bytes": raw_bytes,
            "archive_bytes": 0,
            "ratio": 0.0,
            "seconds": seconds,
            "mbps": raw_bytes / 1_000_000.0 / max(seconds, 1e-9),
            "decode_seconds": 0.0,
            "decode_mbps": 0.0,
            "status": "COMPRESS_FAIL",
            "raw_sha256": raw_sha256,
            "error": stderr[-1000:],
        }

    summary = json.loads((work_dir / "summary.json").read_text(encoding="utf-8"))
    block_path.unlink()
    return {
        "dataset": dataset,
        "block_id": block_id,
        "raw_bytes": raw_bytes,
        "archive_bytes": int(summary.get("archive_bytes", 0)),
        "ratio": raw_bytes / max(1, int(summary.get("archive_bytes", 0))),
        "seconds": seconds,
        "mbps": raw_bytes / 1_000_000.0 / max(seconds, 1e-9),
        "decode_seconds": 0.0,
        "decode_mbps": 0.0,
        "status": "COMPRESSED",
        "raw_sha256": raw_sha256,
        "template_count": int(summary.get("template_count", 0)),
        "accepted_functions": int(summary.get("accepted_functions", 0)),
        "llm_calls": int(summary.get("llm_calls", 0)),
        "llm_api_calls": int(summary.get("llm_runtime_stats", {}).get("api_calls", 0)),
        "stage_seconds": json.dumps(summary.get("stage_seconds", {}), sort_keys=True),
        "operation_timings": json.dumps(summary.get("operation_timings", {}), sort_keys=True),
        "error": "",
    }


def verify_block(job: dict[str, object]) -> dict[str, object]:
    row = dict(job["row"])
    block_root = Path(str(job["block_root"]))
    decoded_path = block_root / "decoded.log"
    restore_root = block_root / "restore"
    archive_path = block_root / "work" / "archive.tar.xz"
    ok, decode_error, decode_seconds = decode_archive(archive_path, decoded_path, restore_root)
    status = "RESTORE_FAIL"
    if ok:
        status = "PASS" if sha256_path(decoded_path) == str(row["raw_sha256"]) else "SHA_FAIL"
        shutil.rmtree(restore_root, ignore_errors=True)
    row.update(
        {
            "decode_seconds": decode_seconds,
            "decode_mbps": int(row["raw_bytes"]) / 1_000_000.0 / max(decode_seconds, 1e-9),
            "status": status,
            "error": decode_error if status != "PASS" else "",
        }
    )
    return row


def run_dataset(dataset: str, raw_dir: Path, result_root: Path, config: PureConfig) -> tuple[dict[str, object], list[dict[str, object]]]:
    input_path = raw_dir / f"{dataset}.log"
    if not input_path.is_file():
        raise FileNotFoundError(input_path)
    train_info = train_dataset(dataset, input_path, result_root, config)
    if train_info["status"] != "PASS":
        raise RuntimeError(f"offline training failed for {dataset}: {train_info['error']}")

    block_dir = result_root / "blocks" / dataset
    online_wall_start = time.perf_counter()
    split_start = time.perf_counter()
    blocks, source_sha256 = split_blocks(input_path, block_dir, config.block_lines)
    split_seconds = time.perf_counter() - split_start
    jobs = [
        {
            "dataset": dataset,
            "block_id": int(block["block_id"]),
            "block_path": str(block["path"]),
            "block_root": str(result_root / "work" / dataset / f"block_{int(block['block_id']):05d}"),
            "raw_bytes": int(block["raw_bytes"]),
            "raw_sha256": str(block["raw_sha256"]),
            "cache_dir": train_info["cache_dir"],
            "replay_plan": train_info["replay_plan"],
            "replay_scope": config.sample_train_replay_scope,
            "extra_flags": getattr(config, "compress_extra_flags", []),
        }
        for block in blocks
    ]
    rows: list[dict[str, object]] = []
    compression_wall_start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=config.workers) as pool:
        futures = [pool.submit(compress_block, job) for job in jobs]
        for future in as_completed(futures):
            rows.append(future.result())
    compression_wall_seconds = time.perf_counter() - compression_wall_start
    online_wall_seconds = time.perf_counter() - online_wall_start
    rows.sort(key=lambda row: int(row["block_id"]))

    verify_jobs = [
        {
            "row": row,
            "block_root": str(result_root / "work" / dataset / f"block_{int(row['block_id']):05d}"),
        }
        for row in rows
        if row["status"] == "COMPRESSED"
    ]
    verified_rows = [row for row in rows if row["status"] != "COMPRESSED"]
    decode_wall_start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=config.workers) as pool:
        futures = [pool.submit(verify_block, job) for job in verify_jobs]
        for future in as_completed(futures):
            verified_rows.append(future.result())
    decode_wall_seconds = time.perf_counter() - decode_wall_start
    rows = sorted(verified_rows, key=lambda row: int(row["block_id"]))

    restored_digest = hashlib.sha256()
    reconstructed_complete = True
    for row in rows:
        decoded_path = result_root / "work" / dataset / f"block_{int(row['block_id']):05d}" / "decoded.log"
        if row["status"] != "PASS" or not decoded_path.is_file():
            reconstructed_complete = False
            continue
        with decoded_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                restored_digest.update(chunk)
        decoded_path.unlink()

    restored_sha256 = restored_digest.hexdigest() if reconstructed_complete else ""
    if all(row["status"] != "COMPRESS_FAIL" for row in rows):
        shutil.rmtree(block_dir, ignore_errors=True)

    raw_bytes = sum(int(row["raw_bytes"]) for row in rows)
    archive_bytes = sum(int(row["archive_bytes"]) for row in rows)
    block_cpu_seconds = sum(float(row["seconds"]) for row in rows)
    decode_cpu_seconds = sum(float(row["decode_seconds"]) for row in rows)
    block_status = "PASS" if all(row["status"] == "PASS" for row in rows) else "FAIL"
    full_sha = "PASS" if reconstructed_complete and restored_sha256 == source_sha256 else "FAIL"
    summary = {
        "dataset": dataset,
        "blocks": len(rows),
        "raw_bytes": raw_bytes,
        "archive_bytes": archive_bytes,
        "ratio": raw_bytes / max(1, archive_bytes),
        "workers": config.workers,
        "block_lines": config.block_lines,
        "split_seconds": split_seconds,
        "compression_wall_seconds": compression_wall_seconds,
        "online_wall_seconds": online_wall_seconds,
        "online_wall_mbps": raw_bytes / 1_000_000.0 / max(online_wall_seconds, 1e-9),
        "block_cpu_seconds_sum": block_cpu_seconds,
        "block_cpu_mbps": raw_bytes / 1_000_000.0 / max(block_cpu_seconds, 1e-9),
        "decode_wall_seconds": decode_wall_seconds,
        "decode_wall_mbps": raw_bytes / 1_000_000.0 / max(decode_wall_seconds, 1e-9),
        "decode_cpu_seconds_sum": decode_cpu_seconds,
        "block_status": block_status,
        "full_sha": full_sha,
        "source_sha256": source_sha256,
        "restored_sha256": restored_sha256,
        "online_api_calls": sum(int(row.get("llm_api_calls", 0)) for row in rows),
        "train_seconds": float(train_info["seconds"]),
        "train_api_calls": int(train_info["api_calls"]),
        "train_sampled_lines": int(train_info["sampled_lines"]),
        "train_total_lines": int(train_info["total_lines"]),
        "train_scanned_lines": int(train_info["scanned_lines"]),
        "train_prefix_ratio": float(train_info["prefix_ratio"]),
        "train_status": train_info["status"],
    }
    return summary, rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--datasets", nargs="+", required=True)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--block-lines", type=int, default=100_000)
    parser.add_argument("--offline-train-topk", type=int, default=40)
    parser.add_argument("--offline-train-logbatcher-per-family", type=int, default=2)
    parser.add_argument("--offline-train-logbatcher-min-support", type=int, default=50)
    parser.add_argument("--offline-train-max-llm-calls", type=int, default=80)
    parser.add_argument("--offline-train-familywise-query", action="store_true")
    parser.add_argument("--offline-train-prefix-ratio", type=float, default=1.0)
    parser.add_argument("--offline-train-min-prefix-lines", type=int, default=0)
    parser.add_argument("--compress-extra-flag", action="append", default=[])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 0.0 < args.offline_train_prefix_ratio <= 1.0:
        raise SystemExit("--offline-train-prefix-ratio must be in (0, 1]")
    if args.offline_train_min_prefix_lines < 0:
        raise SystemExit("--offline-train-min-prefix-lines must be non-negative")
    config = PureConfig(
        workers=args.workers,
        block_lines=args.block_lines,
        offline_topk=args.offline_train_topk,
        offline_logbatcher_per_family=args.offline_train_logbatcher_per_family,
        offline_logbatcher_min_support=args.offline_train_logbatcher_min_support,
        offline_max_llm_calls=args.offline_train_max_llm_calls,
        offline_batch_query=not args.offline_train_familywise_query,
        offline_prefix_ratio=args.offline_train_prefix_ratio,
        offline_min_prefix_lines=args.offline_train_min_prefix_lines,
    )
    object.__setattr__(config, "compress_extra_flags", list(args.compress_extra_flag))
    result_root = Path(args.result_dir)
    if result_root.exists():
        shutil.rmtree(result_root)
    result_root.mkdir(parents=True, exist_ok=True)

    summaries: list[dict[str, object]] = []
    block_rows: list[dict[str, object]] = []
    for dataset in args.datasets:
        summary, rows = run_dataset(dataset, Path(args.raw_dir), result_root, config)
        summaries.append(summary)
        block_rows.extend(rows)
        print(json.dumps(summary, sort_keys=True), flush=True)

    write_csv(result_root / "summary.csv", summaries)
    write_csv(result_root / "blocks.csv", block_rows)
    (result_root / "summary.json").write_text(json.dumps(summaries, indent=2, sort_keys=True), encoding="utf-8")
    ok = all(row["full_sha"] == "PASS" and row["block_status"] == "PASS" for row in summaries)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
