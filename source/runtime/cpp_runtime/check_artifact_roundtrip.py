#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def build_tool() -> Path:
    compiler = os.environ.get("CXX") or shutil.which("clang++") or shutil.which("g++")
    if not compiler:
        raise RuntimeError("no C++ compiler found; set CXX or install clang++/g++")
    exe = ROOT / "stream_roundtrip"
    subprocess.run(
        [
            compiler,
            "-std=c++17",
            "-O2",
            "-Wall",
            "-Wextra",
            "semzip_codecs.cpp",
            "stream_roundtrip.cpp",
            "-o",
            str(exe),
        ],
        cwd=ROOT,
        check=True,
    )
    return exe


def stream_entries(metadata: dict[str, object]) -> list[dict[str, object]]:
    dataset_extract = metadata.get("dataset_extract", {})
    if not isinstance(dataset_extract, dict):
        return []
    streams = dataset_extract.get("streams", [])
    return [entry for entry in streams if isinstance(entry, dict)]


def run_supported(exe: Path, extract_dir: Path, entry: dict[str, object]) -> int:
    kind = str(entry.get("kind", ""))
    tag = str(entry.get("tag", ""))
    checked = 0

    if kind == "ipv4_plain":
        subprocess.run([str(exe), "ipv4_plain", str(extract_dir / f"{tag}.ipv4.bin")], check=True)
        checked += 1
    elif kind in {"int_delta", "hms_delta", "month_day_hms_delta"}:
        subprocess.run([str(exe), "delta", str(extract_dir / f"{tag}.delta.bin")], check=True)
        checked += 1
    elif kind == "string":
        subprocess.run([str(exe), "string", str(extract_dir / f"{tag}.strings.bin")], check=True)
        checked += 1
    elif kind == "string_mtf_rank":
        subprocess.run(
            [str(exe), "string_mtf_rank", str(extract_dir / f"{tag}.rank.bin"), str(extract_dir / f"{tag}.literal.bin")],
            check=True,
        )
        checked += 1
    elif kind == "open_function" and (extract_dir / f"{tag}.ofdelta.bin").exists():
        subprocess.run([str(exe), "delta", str(extract_dir / f"{tag}.ofdelta.bin")], check=True)
        checked += 1

    nested = entry.get("python_exec_string_codec")
    if isinstance(nested, dict) and str(nested.get("kind", "")) == "string_mtf_rank":
        subprocess.run(
            [
                str(exe),
                "string_mtf_rank",
                str(extract_dir / str(nested["rank_file"])),
                str(extract_dir / str(nested["literal_file"])),
            ],
            check=True,
        )
        checked += 1

    return checked


def main() -> int:
    parser = argparse.ArgumentParser(description="Run C++ roundtrip checks on supported SemZip stream files.")
    parser.add_argument("compressed_dir", help="Path to unpacked compressed/ directory containing metadata.json and dataset_extract/")
    args = parser.parse_args()

    compressed_dir = Path(args.compressed_dir)
    metadata_path = compressed_dir / "metadata.json"
    extract_dir = compressed_dir / "dataset_extract"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    exe = build_tool()

    checked = 0
    skipped: list[str] = []
    for entry in stream_entries(metadata):
        before = checked
        checked += run_supported(exe, extract_dir, entry)
        if checked == before:
            skipped.append(f"{entry.get('tag')}:{entry.get('kind')}")

    if checked == 0:
        raise RuntimeError("no supported streams found")
    print(f"artifact roundtrip PASS checked={checked} skipped={len(skipped)}")
    if skipped:
        print("skipped=" + ",".join(skipped))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
