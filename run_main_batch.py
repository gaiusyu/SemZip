#!/usr/bin/env python3
"""Run and record one cold-start SemZip V6 paper experiment batch."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_lines(path: Path) -> int:
    with path.open("rb") as handle:
        return sum(1 for _line in handle)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def run_logged(
    command: list[str],
    *,
    cwd: Path,
    log_path: Path,
    env: dict[str, str],
) -> float:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    with log_path.open("w", encoding="utf-8") as log:
        log.write("COMMAND=" + json.dumps(command) + "\n")
        log.flush()
        completed = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    elapsed = time.perf_counter() - started
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed with status {completed.returncode}; see {log_path}"
        )
    return elapsed


def first_dict_with_key(value: Any, key: str) -> dict[str, Any] | None:
    if isinstance(value, dict):
        if key in value:
            return value
        for nested in value.values():
            found = first_dict_with_key(nested, key)
            if found is not None:
                return found
    elif isinstance(value, list):
        for nested in value:
            found = first_dict_with_key(nested, key)
            if found is not None:
                return found
    return None


def cache_usage(cache_dir: Path) -> dict[str, Any]:
    prompt_tokens = 0
    completion_tokens = 0
    total_tokens = 0
    models: set[str] = set()
    response_files = 0
    usage_files = 0
    provider_cost_usd = 0.0
    provider_cost_files = 0
    for path in sorted(cache_dir.rglob("*.response.json")):
        response_files += 1
        try:
            payload = read_json(path)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        usage_owner = first_dict_with_key(payload, "usage")
        if usage_owner is not None and isinstance(usage_owner.get("usage"), dict):
            usage = usage_owner["usage"]
            input_value = usage.get("prompt_tokens", usage.get("input_tokens"))
            output_value = usage.get("completion_tokens", usage.get("output_tokens"))
            total_value = usage.get("total_tokens")
            if input_value is not None and output_value is not None:
                input_count = int(input_value)
                output_count = int(output_value)
                prompt_tokens += input_count
                completion_tokens += output_count
                total_tokens += int(total_value) if total_value is not None else input_count + output_count
                usage_files += 1
        model_owner = first_dict_with_key(payload, "model")
        if model_owner is not None and isinstance(model_owner.get("model"), str):
            models.add(model_owner["model"])
        cost_value: Any = None
        for cost_key in ("cost_usd", "total_cost_usd", "cost"):
            cost_owner = first_dict_with_key(payload, cost_key)
            if cost_owner is not None:
                cost_value = cost_owner.get(cost_key)
                break
        if not isinstance(cost_value, bool):
            try:
                provider_cost_usd += float(cost_value)
                provider_cost_files += 1
            except (TypeError, ValueError):
                pass
    return {
        "response_files": response_files,
        "response_files_with_usage": usage_files,
        "input_tokens": prompt_tokens,
        "output_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "actual_model_ids": ";".join(sorted(models)),
        "provider_cost_usd": provider_cost_usd,
        "response_files_with_provider_cost": provider_cost_files,
    }


def validate_api_provenance(dataset: str, usage: dict[str, Any], api_calls: int) -> None:
    if int(usage["response_files"]) != api_calls:
        raise RuntimeError(
            f"{dataset} API provenance mismatch: {usage['response_files']} response "
            f"envelopes for {api_calls} successful API calls"
        )
    if int(usage["response_files_with_usage"]) != api_calls:
        raise RuntimeError(
            f"{dataset} API provenance incomplete: usage exists in "
            f"{usage['response_files_with_usage']} of {api_calls} response envelopes"
        )
    if not str(usage["actual_model_ids"]):
        raise RuntimeError(f"{dataset} API provenance has no provider-returned model ID")


def trace_counts(path: Path) -> dict[str, int]:
    if not path.is_file():
        return {"proposed": 0, "admitted": 0, "rejected": 0}
    payload = read_json(path)
    rows = payload if isinstance(payload, list) else []
    admitted = sum(1 for row in rows if row.get("status") == "admitted")
    return {
        "proposed": len(rows),
        "admitted": admitted,
        "rejected": len(rows) - admitted,
    }


def source_hashes(root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for child in (root / "trainer", root / "runtime", root / "backend"):
        for path in sorted(child.rglob("*")):
            if not path.is_file() or path.name.startswith("."):
                continue
            if path.suffix in {".pyc", ".o"} or "__pycache__" in path.parts:
                continue
            hashes[str(path.relative_to(root))] = sha256_path(path)
    return hashes


def command_output(command: list[str]) -> str:
    completed = subprocess.run(command, text=True, capture_output=True)
    return completed.stdout.strip() if completed.returncode == 0 else ""


def timed_command(command: list[str], metrics_path: Path) -> list[str]:
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    return ["/usr/bin/time", "-v", "-o", str(metrics_path), *command]


def time_metrics(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {"peak_rss_mb": "", "cpu_percent": ""}
    if not path.is_file():
        return result
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        key, separator, value = line.strip().partition(":")
        if not separator:
            continue
        if key == "Maximum resident set size (kbytes)":
            result["peak_rss_mb"] = int(value.strip()) / 1024.0
        elif key == "Percent of CPU this job got":
            result["cpu_percent"] = value.strip()
    return result


def v6_command(
    python: str,
    backend: Path,
    runtime: Path,
    dataset: str,
    raw_path: Path,
    plan_path: Path,
    result_path: Path,
    block_lines: int,
    workers: int,
    api_mode: str,
) -> list[str]:
    return [
        python,
        str(backend / "run_semantic_codec_experiment.py"),
        "--dataset",
        dataset,
        "--input",
        str(raw_path),
        "--plan",
        str(plan_path),
        "--semzip-source",
        str(runtime),
        "--result",
        str(result_path),
        "--block-size",
        str(block_lines),
        "--workers",
        str(workers),
        "--api-mode",
        api_mode,
        "--delog-residual-allgroup-or20",
    ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--result-root", type=Path, required=True)
    parser.add_argument("--datasets", nargs="+", required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("configs") / "main.yaml",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parent
    trainer = root / "trainer"
    runtime = root / "runtime"
    backend = root / "backend"
    config = read_json(args.config.resolve())
    result_root = args.result_root.resolve()
    raw_dir = args.raw_dir.resolve()
    if result_root.exists() and any(result_root.iterdir()):
        raise RuntimeError(f"refusing to reuse nonempty result root: {result_root}")
    result_root.mkdir(parents=True, exist_ok=True)
    if not os.environ.get("YUNWU_API_KEY"):
        raise RuntimeError("YUNWU_API_KEY is required for cold-start discovery")

    workers = int(config["workers"])
    block_lines = int(config["block_lines"])
    model = str(config["model"])
    max_calls = int(config["offline_discovery"]["maximum_api_calls"])
    trials = int(config["online"]["speed_trials"])
    env = os.environ.copy()
    env["PARE_LLM_MODEL"] = model
    env["PYTHONHASHSEED"] = "0"

    started_at = datetime.now(timezone.utc).isoformat()
    code_commit = command_output(["git", "rev-parse", "HEAD"])
    manifest = {
        "campaign_id": config["campaign_id"],
        "method_id": config["method_id"],
        "started_at_utc": started_at,
        "host": platform.node(),
        "platform": platform.platform(),
        "python": sys.version,
        "cpu": command_output(["bash", "-lc", "lscpu | tr '\\n' ';'"]),
        "memory": command_output(["bash", "-lc", "free -h | tr '\\n' ';'"]),
        "compiler": command_output(["g++", "--version"]).splitlines()[:1],
        "raw_dir": str(raw_dir),
        "result_root": str(result_root),
        "datasets": args.datasets,
        "command": [sys.executable, *sys.argv],
        "config_path": str(args.config.resolve()),
        "config_sha256": sha256_path(args.config.resolve()),
        "code_commit": code_commit,
        "source_hashes": source_hashes(root),
        "environment": {
            "PARE_LLM_MODEL": model,
            "PYTHONHASHSEED": "0",
            "YUNWU_API_KEY": "<redacted-present>",
        },
    }
    (result_root / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    run_logged(
        ["bash", "build.sh"],
        cwd=backend,
        log_path=result_root / "logs" / "build.log",
        env=env,
    )

    dataset_rows: list[dict[str, Any]] = []
    ratio_rows: list[dict[str, Any]] = []
    speed_rows: list[dict[str, Any]] = []
    synthesis_rows: list[dict[str, Any]] = []

    for dataset in args.datasets:
        raw_path = raw_dir / f"{dataset}.log"
        if not raw_path.is_file():
            raise FileNotFoundError(raw_path)
        dataset_rows.append(
            {
                "dataset": dataset,
                "path": str(raw_path),
                "raw_bytes": raw_path.stat().st_size,
                "lines": count_lines(raw_path),
                "sha256": sha256_path(raw_path),
            }
        )
        write_csv(result_root / "dataset_manifest.csv", dataset_rows)

        discovery = result_root / "discovery" / dataset
        train_command = [
            sys.executable,
            str(trainer / "run_semzip_classwise_proposal.py"),
            "--raw-dir",
            str(raw_dir),
            "--datasets",
            dataset,
            "--result-dir",
            str(discovery),
            "--workers",
            str(workers),
            "--block-lines",
            str(block_lines),
            "--model",
            model,
            "--max-llm-calls",
            str(max_calls),
        ]
        run_logged(
            train_command,
            cwd=trainer,
            log_path=result_root / "logs" / f"{dataset}.discovery.log",
            env=env,
        )
        discovery_summary = read_json(discovery / "summary.json")[0]
        plan_source = discovery / "training_work" / dataset / "replay_plan.json"
        train_work = discovery / "training_work" / dataset
        train_summary = read_json(train_work / "summary.json")
        api_calls = int(discovery_summary.get("train_api_calls", 0))
        if api_calls <= 0:
            raise RuntimeError(f"{dataset} cold discovery made no real API calls")
        usage = cache_usage(discovery / "llm_caches" / dataset)
        validate_api_provenance(dataset, usage, api_calls)
        if not plan_source.is_file():
            raise FileNotFoundError(plan_source)
        plan_dir = result_root / "plans" / dataset
        plan_dir.mkdir(parents=True, exist_ok=True)
        plan_path = plan_dir / "replay_plan.json"
        shutil.copy2(plan_source, plan_path)

        main_result = result_root / "main" / dataset
        run_logged(
            v6_command(
                sys.executable,
                backend,
                runtime,
                dataset,
                raw_path,
                plan_path,
                main_result,
                block_lines,
                workers,
                "cold-api-plan-frozen-replay",
            ),
            cwd=backend,
            log_path=result_root / "logs" / f"{dataset}.main.log",
            env=env,
        )
        main_summary = read_json(main_result / "summary.json")
        if not main_summary.get("sha_pass"):
            raise RuntimeError(f"{dataset} main V6 run failed SHA verification")

        ratio_rows.append(
            {
                "dataset": dataset,
                "method": config["method_id"],
                "config": config["campaign_id"],
                "blocks": main_summary["verified_blocks"],
                "raw_bytes": main_summary["raw_bytes"],
                "archive_bytes": main_summary["archive_bytes"],
                "ratio": main_summary["compression_ratio"],
                "workers": workers,
                "all_blocks_restored": not main_summary["failed_blocks"],
                "full_sha256": main_summary["sha_pass"],
                "config_hash": manifest["config_sha256"],
                "code_commit": code_commit,
                "plan_hash": main_summary["plan_sha256"],
                "compressor_hash": main_summary["compressor_sha256"],
            }
        )
        write_csv(result_root / "main_ratio.csv", ratio_rows)

        warmup = result_root / "speed" / dataset / "warmup"
        run_logged(
            v6_command(
                sys.executable,
                backend,
                runtime,
                dataset,
                raw_path,
                plan_path,
                warmup,
                block_lines,
                workers,
                "frozen-plan-speed-warmup",
            ),
            cwd=backend,
            log_path=result_root / "logs" / f"{dataset}.speed_warmup.log",
            env=env,
        )
        for trial in range(1, trials + 1):
            trial_result = result_root / "speed" / dataset / f"trial_{trial}"
            metrics_path = result_root / "metrics" / f"{dataset}.speed_{trial}.time.txt"
            run_logged(
                timed_command(
                    v6_command(
                        sys.executable,
                        backend,
                        runtime,
                        dataset,
                        raw_path,
                        plan_path,
                        trial_result,
                        block_lines,
                        workers,
                        "frozen-plan-speed-trial",
                    ),
                    metrics_path,
                ),
                cwd=backend,
                log_path=result_root / "logs" / f"{dataset}.speed_{trial}.log",
                env=env,
            )
            trial_summary = read_json(trial_result / "summary.json")
            if not trial_summary.get("sha_pass"):
                raise RuntimeError(f"{dataset} speed trial {trial} failed SHA verification")
            measured_resources = time_metrics(metrics_path)
            speed_rows.extend(
                [
                    {
                        "dataset": dataset,
                        "method": config["method_id"],
                        "trial": trial,
                        "phase": "encode",
                        "raw_bytes": trial_summary["raw_bytes"],
                        "seconds": trial_summary["online_compression_seconds"],
                        "throughput_mbps": trial_summary["online_compression_mbps"],
                        "workers": workers,
                        "peak_rss_mb": measured_resources["peak_rss_mb"],
                        "cpu_percent": measured_resources["cpu_percent"],
                        "code_commit": code_commit,
                        "full_sha256": trial_summary["sha_pass"],
                    },
                    {
                        "dataset": dataset,
                        "method": config["method_id"],
                        "trial": trial,
                        "phase": "decode",
                        "raw_bytes": trial_summary["raw_bytes"],
                        "seconds": trial_summary["delog_decode_seconds"]
                        + trial_summary["semantic_decode_seconds"],
                        "throughput_mbps": trial_summary["raw_bytes"]
                        / 1_000_000
                        / max(
                            trial_summary["delog_decode_seconds"]
                            + trial_summary["semantic_decode_seconds"],
                            1e-9,
                        ),
                        "workers": workers,
                        "peak_rss_mb": measured_resources["peak_rss_mb"],
                        "cpu_percent": measured_resources["cpu_percent"],
                        "code_commit": code_commit,
                        "full_sha256": trial_summary["sha_pass"],
                    },
                ]
            )
            write_csv(result_root / "speed.csv", speed_rows)

        traces = trace_counts(train_work / "function_compile_trace.json")
        repair_stats = train_summary.get("verifier_repair_stats", {})
        repair_calls = int(repair_stats.get("attempts", 0) or 0)
        cost_complete = int(usage["response_files_with_provider_cost"]) == api_calls
        synthesis_rows.append(
            {
                "dataset": dataset,
                "model_requested": model,
                "model_actual": usage["actual_model_ids"],
                "temperature": config["temperature"],
                "sample_hash": sha256_path(
                    discovery / "training_samples" / f"{dataset}.log"
                ),
                "prompt_hashes": ";".join(
                    sha256_path(path)
                    for path in sorted(
                        (discovery / "llm_caches" / dataset).rglob("*.prompt.txt")
                    )
                ),
                "proposer_calls": max(0, api_calls - repair_calls),
                "repair_calls": repair_calls,
                "total_api_calls": api_calls,
                "response_envelopes": usage["response_files"],
                "usage_records": usage["response_files_with_usage"],
                "input_tokens": usage["input_tokens"],
                "output_tokens": usage["output_tokens"],
                "total_tokens": usage["total_tokens"],
                "cost_usd": usage["provider_cost_usd"] if cost_complete else "",
                "cost_source": "provider_response" if cost_complete else "not_returned_by_provider",
                "request_record_hashes": ";".join(
                    sha256_path(path)
                    for path in sorted(
                        (discovery / "llm_caches" / dataset).rglob("*.request.json")
                    )
                ),
                "response_record_hashes": ";".join(
                    sha256_path(path)
                    for path in sorted(
                        (discovery / "llm_caches" / dataset).rglob("*.response.raw.txt")
                    )
                ),
                "offline_seconds": discovery_summary["train_seconds"],
                "sampled_lines": discovery_summary["train_sampled_lines"],
                "scanned_lines": discovery_summary["train_scanned_lines"],
                "proposed": traces["proposed"],
                "compiled_and_admitted_trace": traces["admitted"],
                "rejected_trace": traces["rejected"],
                "accepted": train_summary.get("accepted_functions", 0),
                "plan_bytes": plan_path.stat().st_size,
                "plan_hash": sha256_path(plan_path),
                "final_ratio": main_summary["compression_ratio"],
                "full_sha256": main_summary["sha_pass"],
            }
        )
        write_csv(result_root / "synthesis.csv", synthesis_rows)

        encode_values = [
            float(row["throughput_mbps"])
            for row in speed_rows
            if row["dataset"] == dataset and row["phase"] == "encode"
        ]
        decode_values = [
            float(row["throughput_mbps"])
            for row in speed_rows
            if row["dataset"] == dataset and row["phase"] == "decode"
        ]
        progress = {
            "dataset": dataset,
            "ratio": main_summary["compression_ratio"],
            "archive_bytes": main_summary["archive_bytes"],
            "sha_pass": main_summary["sha_pass"],
            "encode_median_mbps": statistics.median(encode_values),
            "encode_range_mbps": [min(encode_values), max(encode_values)],
            "decode_median_mbps": statistics.median(decode_values),
            "decode_range_mbps": [min(decode_values), max(decode_values)],
            "train_seconds": discovery_summary["train_seconds"],
            "api_calls": api_calls,
            "accepted_functions": train_summary.get("accepted_functions", 0),
        }
        (result_root / f"{dataset}.complete.json").write_text(
            json.dumps(progress, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(json.dumps(progress, sort_keys=True), flush=True)

    manifest["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    manifest["status"] = "PASS"
    (result_root / "run_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
