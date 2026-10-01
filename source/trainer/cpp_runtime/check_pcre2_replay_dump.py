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
    exe = ROOT / "pcre2_replay_dump"
    subprocess.run(
        [
            compiler,
            "-std=c++17",
            "-O2",
            "-Wall",
            "-Wextra",
            *cflags,
            "pcre2_replay_dump.cpp",
            *libs,
            "-o",
            str(exe),
        ],
        cwd=ROOT,
        check=True,
    )
    return exe


def load_plan(path: Path) -> tuple[list[dict[str, Any]], dict[str, str]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    specs = [
        item
        for item in payload.get("specs", [])
        if isinstance(item, dict)
        and isinstance(item.get("pattern"), str)
        and not str(item.get("pattern", "")).startswith("residual-variable:")
    ]
    placeholders = {str(k): str(v) for k, v in dict(payload.get("placeholders", {})).items()}
    return specs, placeholders


def write_spec_file(path: Path, specs: list[dict[str, Any]], placeholders: dict[str, str]) -> list[dict[str, Any]]:
    usable: list[dict[str, Any]] = []
    lines: list[str] = []
    for spec in specs:
        tag = str(spec.get("tag", ""))
        placeholder = placeholders.get(tag)
        if not tag or not placeholder:
            continue
        pattern = str(spec["pattern"])
        replacement = str(spec.get("replacement", "{placeholder}"))
        if "\n" in pattern or "\n" in replacement or "\t" in pattern or "\t" in replacement:
            continue
        store_group = int(spec.get("store_group", 1) or 0)
        context_group = int(spec.get("context_group", 0) or 0)
        lines.extend([
            "SPEC",
            tag,
            pattern,
            placeholder,
            replacement,
            str(store_group),
            str(context_group),
            "END",
        ])
        usable.append(spec)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return usable


def replacement_format(replacement: str, placeholder: str, match: re.Match[str]) -> str:
    if replacement == "{placeholder}":
        return placeholder
    out: list[str] = []
    cursor = 0
    for token_match in re.finditer(r"\{(placeholder|group\d+)\}", replacement):
        out.append(replacement[cursor:token_match.start()])
        token = token_match.group(1)
        if token == "placeholder":
            out.append(placeholder)
        else:
            group_value = match.group(int(token[5:]))
            out.append("" if group_value is None else str(group_value))
        cursor = token_match.end()
    out.append(replacement[cursor:])
    return "".join(out)


def stored_value_for_match(spec: dict[str, Any], raw_value: str, match: re.Match[str]) -> str:
    context_group = int(spec.get("context_group", 0) or 0)
    store_group = int(spec.get("store_group", 1) or 0)
    if context_group > 0 and context_group != store_group:
        context_value = match.group(context_group)
        return json.dumps([raw_value, "" if context_value is None else context_value], separators=(",", ":"), ensure_ascii=False)
    return raw_value


def python_replay(text: str, specs: list[dict[str, Any]], placeholders: dict[str, str]) -> tuple[str, list[dict[str, str]]]:
    lines = text.splitlines(keepends=True)
    values: list[dict[str, str]] = []
    transformed_lines: list[str] = []
    for line in lines:
        transformed = line
        for spec in specs:
            tag = str(spec.get("tag", ""))
            placeholder = placeholders[tag]
            pattern = re.compile(str(spec["pattern"]), flags=re.MULTILINE)
            store_group = int(spec.get("store_group", 1) or 0)
            protected = [
                placeholders[str(other.get("tag", ""))]
                for other in specs
                if str(other.get("tag", "")) != tag and str(other.get("tag", "")) in placeholders
            ]

            def replace(match: re.Match[str]) -> str:
                whole = match.group(0)
                if any(existing in whole for existing in protected):
                    return whole
                raw_value = match.group(store_group)
                if raw_value is None:
                    return whole
                values.append({"tag": tag, "value": stored_value_for_match(spec, raw_value, match)})
                return replacement_format(str(spec.get("replacement", "{placeholder}")), placeholder, match)

            transformed = pattern.sub(replace, transformed)
        transformed_lines.append(transformed)
    return "".join(transformed_lines), values


def read_jsonl(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            item = json.loads(line)
            rows.append({"tag": str(item["tag"]), "value": str(item["value"])})
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare C++ PCRE2 replay dump against a Python replay-equivalent implementation.")
    parser.add_argument("replay_plan")
    parser.add_argument("input_log")
    parser.add_argument("--json-out", default="")
    args = parser.parse_args()

    plan_path = Path(args.replay_plan)
    input_path = Path(args.input_log)
    specs, placeholders = load_plan(plan_path)
    text = input_path.read_bytes().decode("latin-1")
    exe = build_tool()

    with tempfile.TemporaryDirectory(prefix="semzip_pcre2_replay_") as tmp_name:
        tmp = Path(tmp_name)
        spec_path = tmp / "specs.txt"
        usable_specs = write_spec_file(spec_path, specs, placeholders)
        cpp_text_path = tmp / "cpp_transformed.txt"
        cpp_values_path = tmp / "cpp_values.jsonl"
        subprocess.run(
            [str(exe), str(spec_path), str(input_path.resolve()), str(cpp_text_path), str(cpp_values_path)],
            cwd=ROOT,
            check=True,
        )
        py_text, py_values = python_replay(text, usable_specs, placeholders)
        cpp_text = cpp_text_path.read_bytes().decode("latin-1")
        cpp_values = read_jsonl(cpp_values_path)

    result = {
        "specs": len(usable_specs),
        "python_values": len(py_values),
        "cpp_values": len(cpp_values),
        "transformed_equal": py_text == cpp_text,
        "values_equal": py_values == cpp_values,
    }
    if py_text != cpp_text:
        limit = min(len(py_text), len(cpp_text))
        diff_at = next((index for index in range(limit) if py_text[index] != cpp_text[index]), limit)
        result["first_text_diff"] = {
            "offset": diff_at,
            "python": py_text[diff_at:diff_at + 120],
            "cpp": cpp_text[diff_at:diff_at + 120],
        }
    if py_values != cpp_values:
        limit = min(len(py_values), len(cpp_values))
        diff_at = next((index for index in range(limit) if py_values[index] != cpp_values[index]), limit)
        result["first_value_diff"] = {
            "index": diff_at,
            "python": py_values[diff_at] if diff_at < len(py_values) else None,
            "cpp": cpp_values[diff_at] if diff_at < len(cpp_values) else None,
        }

    print(json.dumps(result, ensure_ascii=False, indent=2))
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0 if result["transformed_equal"] and result["values_equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
