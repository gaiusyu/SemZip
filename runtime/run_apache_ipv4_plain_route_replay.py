#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path("/root/autodl-tmp/semzip_python_exec_ipv4_plain_route_20260707_candidate")
BASE = Path("/root/autodl-tmp/semzip_residual_distinct_or_avglen_20260706_candidate/results_apache_linux_proxifier_hadoop_or20_trueapi_20260706")
DEFAULTS_PATH = ROOT / "pure_args_defaults.json"
OUT = ROOT / "results_apache_ipv4_plain_route_replay_20260707"
RAW = Path("/root/autodl-tmp/logcompose-exp/data/raw/Apache.log")


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def main() -> int:
    original = json.loads(DEFAULTS_PATH.read_text(encoding="utf-8"))
    updated = dict(original)
    updated["residual_variable_stream_mode"] = "context_shape"
    updated["residual_variable_value_penalty"] = 20
    updated["residual_variable_min_distinct"] = 30
    updated["residual_variable_min_score"] = 0
    updated["residual_variable_shape_gate_min_collapse"] = 2000
    updated["residual_variable_shape_gate_min_collapse_pct"] = 20

    block = BASE / "blocks" / "Apache" / "block_00000.log"
    replay = BASE / "training_work" / "Apache" / "replay_plan.json"
    work = OUT / "work" / "Apache" / "block_00000" / "work"
    work.mkdir(parents=True, exist_ok=True)

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
        "Apache",
        "--output-dir",
        str(work),
        "--llm-cache-dir",
        str(BASE / "llm_caches" / "Apache" / "shared"),
        "--max-llm-calls",
        "0",
        "--operation-profile",
        "--skip-internal-decode-verify",
        "--semantic-replay-plan-cache",
        str(replay),
        "--replay-plan-out",
        str(work / "replay_plan.json"),
    ]
    try:
        DEFAULTS_PATH.write_text(json.dumps(updated, indent=2, sort_keys=True), encoding="utf-8")
        start = time.perf_counter()
        proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True, env=env)
        online_seconds = time.perf_counter() - start
        OUT.mkdir(parents=True, exist_ok=True)
        (OUT / "stdout.log").write_text(proc.stdout, encoding="utf-8", errors="ignore")
        (OUT / "stderr.log").write_text(proc.stderr, encoding="utf-8", errors="ignore")
        if proc.returncode != 0:
            print(proc.stderr[-2000:])
            return proc.returncode
        summary = json.loads((work / "summary.json").read_text(encoding="utf-8"))
        decoded = OUT / "decoded.log"
        decode_seconds = decode_archive(work / "archive.tar.xz", decoded, OUT / "restore")
        raw_bytes = block.stat().st_size
        archive_bytes = int(summary["archive_bytes"])
        full_sha = "PASS" if sha256_path(decoded) == sha256_path(RAW) else "FAIL"
        result = {
            "dataset": "Apache",
            "config": "python_exec_ipv4_plain_route_replay",
            "blocks": 1,
            "raw_bytes": raw_bytes,
            "archive_bytes": archive_bytes,
            "ratio": raw_bytes / max(1, archive_bytes),
            "online_seconds": online_seconds,
            "online_mbps": raw_bytes / 1_000_000.0 / max(online_seconds, 1e-9),
            "decode_seconds": decode_seconds,
            "full_sha": full_sha,
            "train_seconds": 0.0,
            "train_api_calls": 0,
            "accepted_functions": int(summary.get("accepted_functions", 0)),
            "template_count": int(summary.get("template_count", 0)),
        }
        (OUT / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(result, sort_keys=True))
        return 0 if full_sha == "PASS" else 1
    finally:
        DEFAULTS_PATH.write_text(json.dumps(original, indent=2, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
