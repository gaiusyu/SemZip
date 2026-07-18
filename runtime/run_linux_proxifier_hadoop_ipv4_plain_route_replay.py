#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path


ROOT = Path("/root/autodl-tmp/semzip_python_exec_ipv4_plain_route_20260707_candidate")
BASE = Path("/root/autodl-tmp/semzip_residual_distinct_or_avglen_20260706_candidate/results_apache_linux_proxifier_hadoop_or20_trueapi_20260706")
DEFAULTS_PATH = ROOT / "pure_args_defaults.json"
RAW_DIR = Path("/root/autodl-tmp/logcompose-exp/data/raw")
OUT = ROOT / "results_linux_proxifier_hadoop_ipv4_plain_route_replay_20260707"
DATASETS = ("Linux", "Proxifier", "Hadoop")


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def decode_archive(archive_path: Path, decoded_path: Path, restore_root: Path) -> float:
    script = (
        "from pathlib import Path\n"
        "import semzip_pure as s\n"
        f"root=s.extract_archive(Path({str(archive_path)!r}), Path({str(restore_root)!r}))\n"
        f"Path({str(decoded_path)!r}).write_bytes(s.decode_archive_root(root).encode('latin-1'))\n"
    )
    start = time.perf_counter()
    subprocess.run([sys.executable, "-c", script], cwd=ROOT, check=True)
    return time.perf_counter() - start


def block_job(job: dict[str, object]) -> dict[str, object]:
    dataset = str(job["dataset"])
    block = Path(str(job["block"]))
    block_id = int(block.stem.split("_")[-1])
    work_root = OUT / "work" / dataset / f"block_{block_id:05d}"
    work = work_root / "work"
    work.mkdir(parents=True, exist_ok=True)
    replay = BASE / "training_work" / dataset / "replay_plan.json"
    cache = BASE / "llm_caches" / dataset / "shared"
    env = os.environ.copy()
    env["PYTHONHASHSEED"] = "0"
    env["PARE_NO_BENEFIT_FIXED_CODEC"] = "1"
    env["PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA"] = "1"
    env["PARE_NUMERIC_LATTICE_MIN_GAIN"] = "192"
    env["PARE_LINE_TRANSDUCER_FAST_GAIN"] = "512"
    cmd = [
        sys.executable,
        str(ROOT / "semzip_pure.py"),
        "--input",
        str(block),
        "--dataset",
        dataset,
        "--output-dir",
        str(work),
        "--llm-cache-dir",
        str(cache),
        "--max-llm-calls",
        "0",
        "--operation-profile",
        "--skip-internal-decode-verify",
        "--semantic-replay-plan-cache",
        str(replay),
        "--replay-plan-out",
        str(work / "replay_plan.json"),
    ]
    start = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, env=env)
    online_seconds = time.perf_counter() - start
    (work_root / "stdout.log").write_text(proc.stdout, encoding="utf-8", errors="ignore")
    (work_root / "stderr.log").write_text(proc.stderr, encoding="utf-8", errors="ignore")
    raw_bytes = block.stat().st_size
    if proc.returncode != 0:
        return {
            "dataset": dataset,
            "block_id": block_id,
            "raw_bytes": raw_bytes,
            "archive_bytes": 0,
            "ratio": 0.0,
            "online_seconds": online_seconds,
            "online_mbps": raw_bytes / 1_000_000.0 / max(online_seconds, 1e-9),
            "decode_seconds": 0.0,
            "status": "FAIL",
            "error": proc.stderr[-1000:],
        }
    summary = json.loads((work / "summary.json").read_text(encoding="utf-8"))
    decoded = work_root / "decoded.log"
    decode_seconds = decode_archive(work / "archive.tar.xz", decoded, work_root / "restore")
    status = "PASS" if sha256_path(decoded) == sha256_path(block) else "SHA_FAIL"
    archive_bytes = int(summary["archive_bytes"])
    return {
        "dataset": dataset,
        "block_id": block_id,
        "raw_bytes": raw_bytes,
        "archive_bytes": archive_bytes,
        "ratio": raw_bytes / max(1, archive_bytes),
        "online_seconds": online_seconds,
        "online_mbps": raw_bytes / 1_000_000.0 / max(online_seconds, 1e-9),
        "decode_seconds": decode_seconds,
        "status": status,
        "accepted_functions": int(summary.get("accepted_functions", 0)),
        "template_count": int(summary.get("template_count", 0)),
    }


def summarize(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    summaries: list[dict[str, object]] = []
    for dataset in DATASETS:
        ds_rows = [row for row in rows if row["dataset"] == dataset]
        restored = OUT / "restored" / f"{dataset}.log"
        restored.parent.mkdir(parents=True, exist_ok=True)
        with restored.open("wb") as handle:
            for row in sorted(ds_rows, key=lambda item: int(item["block_id"])):
                decoded = OUT / "work" / dataset / f"block_{int(row['block_id']):05d}" / "decoded.log"
                if decoded.is_file():
                    handle.write(decoded.read_bytes())
        raw = sum(int(row["raw_bytes"]) for row in ds_rows)
        archive = sum(int(row["archive_bytes"]) for row in ds_rows)
        online = sum(float(row["online_seconds"]) for row in ds_rows)
        decode = sum(float(row["decode_seconds"]) for row in ds_rows)
        raw_path = RAW_DIR / f"{dataset}.log"
        summaries.append(
            {
                "dataset": dataset,
                "blocks": len(ds_rows),
                "raw_bytes": raw,
                "archive_bytes": archive,
                "ratio": raw / max(1, archive),
                "online_seconds": online,
                "online_mbps": raw / 1_000_000.0 / max(online, 1e-9),
                "decode_seconds": decode,
                "block_status": "PASS" if ds_rows and all(row["status"] == "PASS" for row in ds_rows) else "FAIL",
                "full_sha": "PASS" if restored.is_file() and sha256_path(restored) == sha256_path(raw_path) else "FAIL",
                "train_seconds": 0.0,
                "train_api_calls": 0,
            }
        )
    return summaries


def main() -> int:
    original = json.loads(DEFAULTS_PATH.read_text(encoding="utf-8"))
    updated = dict(original)
    updated["residual_variable_stream_mode"] = "context_shape"
    updated["residual_variable_value_penalty"] = 20
    updated["residual_variable_min_distinct"] = 30
    updated["residual_variable_min_score"] = 0
    updated["residual_variable_shape_gate_min_collapse"] = 2000
    updated["residual_variable_shape_gate_min_collapse_pct"] = 20
    OUT.mkdir(parents=True, exist_ok=True)
    try:
        DEFAULTS_PATH.write_text(json.dumps(updated, indent=2, sort_keys=True), encoding="utf-8")
        jobs = []
        for dataset in DATASETS:
            for block in sorted((BASE / "blocks" / dataset).glob("block_*.log")):
                jobs.append({"dataset": dataset, "block": str(block)})
        rows: list[dict[str, object]] = []
        with ProcessPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(block_job, job) for job in jobs]
            for future in as_completed(futures):
                rows.append(future.result())
                write_csv(OUT / "blocks_so_far.csv", rows)
        rows.sort(key=lambda row: (str(row["dataset"]), int(row["block_id"])))
        write_csv(OUT / "blocks.csv", rows)
        summaries = summarize(rows)
        write_csv(OUT / "summary.csv", summaries)
        (OUT / "summary.json").write_text(json.dumps(summaries, indent=2, sort_keys=True), encoding="utf-8")
        for summary in summaries:
            print(json.dumps(summary, sort_keys=True), flush=True)
        return 0 if all(row["full_sha"] == "PASS" and row["block_status"] == "PASS" for row in summaries) else 1
    finally:
        DEFAULTS_PATH.write_text(json.dumps(original, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
