#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


def build_tool() -> Path:
    compiler = os.environ.get("CXX") or shutil.which("clang++") or shutil.which("g++")
    if not compiler:
        raise RuntimeError("no C++ compiler found; set CXX or install clang++/g++")
    pcre2_config = shutil.which("pcre2-config")
    cflags: list[str] = []
    libs = ["-lpcre2-8"]
    if pcre2_config:
        cflags = shlex.split(subprocess.check_output([pcre2_config, "--cflags"], text=True).strip())
        libs = shlex.split(subprocess.check_output([pcre2_config, "--libs8"], text=True).strip())
    exe = ROOT / "pcre2_match_dump"
    subprocess.run(
        [
            compiler,
            "-std=c++17",
            "-O2",
            "-Wall",
            "-Wextra",
            *cflags,
            "pcre2_match_dump.cpp",
            *libs,
            "-o",
            str(exe),
        ],
        cwd=ROOT,
        check=True,
    )
    return exe


def load_specs(replay_plan_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(replay_plan_path.read_text(encoding="utf-8"))
    specs = payload.get("specs", [])
    if not isinstance(specs, list):
        return []
    return [spec for spec in specs if isinstance(spec, dict)]


def real_regex_specs(specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for spec in specs:
        pattern = spec.get("pattern")
        if not isinstance(pattern, str) or not pattern:
            continue
        if pattern.startswith("residual-variable:"):
            continue
        out.append(spec)
    return out


def python_matches(pattern: str, lines: list[str]) -> list[tuple[int, tuple[int, int], tuple[tuple[int, int], ...]]]:
    regex = re.compile(pattern)
    matches: list[tuple[int, tuple[int, int], tuple[tuple[int, int], ...]]] = []
    for line_index, line in enumerate(lines):
        for match in regex.finditer(line):
            groups: list[tuple[int, int]] = []
            for group in range(1, len(match.groups()) + 1):
                start = match.start(group)
                end = match.end(group)
                groups.append((start, end))
            matches.append((line_index, match.span(0), tuple(groups)))
    return matches


def parse_span(text: str) -> tuple[int, int]:
    left, right = text.split(":", 1)
    return int(left), int(right)


def pcre2_matches(exe: Path, pattern: str, input_path: Path, tmp: Path) -> list[tuple[int, tuple[int, int], tuple[tuple[int, int], ...]]]:
    pattern_path = tmp / "pattern.re"
    pattern_path.write_text(pattern, encoding="utf-8")
    proc = subprocess.run(
        [str(exe), str(pattern_path), str(input_path.resolve())],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or f"pcre2_match_dump failed with {proc.returncode}")
    matches: list[tuple[int, tuple[int, int], tuple[tuple[int, int], ...]]] = []
    for raw_line in proc.stdout.splitlines():
        fields = raw_line.split("\t")
        if len(fields) < 3:
            raise RuntimeError(f"bad pcre2 output: {raw_line!r}")
        line_index = int(fields[0])
        match_span = parse_span(fields[1])
        group_count = int(fields[2])
        groups = tuple(parse_span(field) for field in fields[3:])
        if group_count != len(groups) + 1:
            raise RuntimeError(f"bad group count in pcre2 output: {raw_line!r}")
        matches.append((line_index, match_span, groups))
    return matches


def read_input_lines(input_path: Path) -> list[str]:
    lines = input_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return [line[:-1] if line.endswith("\r") else line for line in lines]


def compare_one(exe: Path, spec: dict[str, Any], input_path: Path, lines: list[str], tmp: Path) -> dict[str, Any]:
    pattern = str(spec["pattern"])
    tag = str(spec.get("tag", ""))
    try:
        py = python_matches(pattern, lines)
    except re.error as exc:
        return {"tag": tag, "pattern": pattern, "status": "python_re_error", "error": str(exc)}
    try:
        pc = pcre2_matches(exe, pattern, input_path, tmp)
    except Exception as exc:
        return {"tag": tag, "pattern": pattern, "status": "pcre2_error", "error": str(exc)}
    if py == pc:
        return {"tag": tag, "pattern": pattern, "status": "PASS", "matches": len(py)}
    first_diff = None
    for index, (left, right) in enumerate(zip(py, pc)):
        if left != right:
            first_diff = {"index": index, "python": left, "pcre2": right}
            break
    if first_diff is None:
        first_diff = {"python_count": len(py), "pcre2_count": len(pc)}
    return {
        "tag": tag,
        "pattern": pattern,
        "status": "MISMATCH",
        "python_matches": len(py),
        "pcre2_matches": len(pc),
        "first_diff": first_diff,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare SemZip replay-plan regex matches between Python re and C++ PCRE2.")
    parser.add_argument("replay_plan", help="Path to replay_plan.json")
    parser.add_argument("input_log", help="Raw block log used by the replay plan")
    parser.add_argument("--json-out", default="", help="Optional path for detailed JSON report")
    args = parser.parse_args()

    replay_plan = Path(args.replay_plan)
    input_log = Path(args.input_log)
    specs = real_regex_specs(load_specs(replay_plan))
    lines = read_input_lines(input_log)
    exe = build_tool()

    with tempfile.TemporaryDirectory(prefix="semzip_pcre2_parity_") as tmp_name:
        tmp = Path(tmp_name)
        results = [compare_one(exe, spec, input_log, lines, tmp) for spec in specs]

    passed = sum(1 for item in results if item["status"] == "PASS")
    failed = len(results) - passed
    print(f"PCRE2 replay parity checked={len(results)} passed={passed} failed={failed}")
    for item in results:
        suffix = f" matches={item.get('matches')}" if item["status"] == "PASS" else f" error={item.get('error', item.get('first_diff'))}"
        print(f"{item['status']}\t{item.get('tag')}\t{item.get('pattern')}{suffix}")
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
