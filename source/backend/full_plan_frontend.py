#!/usr/bin/env python3
import argparse
import base64
import concurrent.futures
import hashlib
import json
import math
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


MARKER_PREFIX = "SEMZIPQX"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def alpha_id(index: int) -> str:
    result = ""
    value = index + 1
    while value:
        value, remainder = divmod(value - 1, 26)
        result = chr(ord("A") + remainder) + result
    return result


def alpha_index(value: str) -> int:
    result = 0
    for char in value:
        result = result * 26 + ord(char) - ord("A") + 1
    return result - 1


def encode_scalar(value: Any) -> tuple[str, str]:
    if isinstance(value, bool):
        return "BOOLEAN", "T" if value else "F"
    if isinstance(value, int):
        return "INTEGER", str(value)
    if isinstance(value, float):
        return "FLOAT", repr(value)
    if isinstance(value, str):
        payload = base64.urlsafe_b64encode(value.encode("utf-8", "surrogatepass")).decode("ascii").rstrip("=")
        return "STRING", "X" + payload
    payload = json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    encoded = base64.urlsafe_b64encode(payload.encode("utf-8")).decode("ascii").rstrip("=")
    return "JSON", "X" + encoded


def decode_scalar(kind: str, token: str) -> Any:
    if kind == "BOOLEAN":
        return token == "T"
    if kind == "INTEGER":
        return int(token)
    if kind == "FLOAT":
        return float(token)
    if not token.startswith("X"):
        raise ValueError(f"Invalid encoded {kind} token")
    payload = token[1:]
    payload += "=" * ((4 - len(payload) % 4) % 4)
    decoded = base64.urlsafe_b64decode(payload.encode("ascii"))
    if kind == "STRING":
        return decoded.decode("utf-8", "surrogatepass")
    if kind == "JSON":
        return json.loads(decoded.decode("utf-8"))
    raise ValueError(f"Unsupported marker scalar kind: {kind}")


SAFE_BUILTINS = {
    "__import__": __import__,
    "abs": abs,
    "bool": bool,
    "dict": dict,
    "enumerate": enumerate,
    "float": float,
    "int": int,
    "len": len,
    "list": list,
    "max": max,
    "min": min,
    "range": range,
    "round": round,
    "str": str,
    "sum": sum,
    "tuple": tuple,
    "zip": zip,
}


@dataclass
class CompiledSpec:
    index: int
    pattern: re.Pattern[str]
    replacement: str
    store_group: int
    group_regex: re.Pattern[str] | None
    forward: Callable[[list[str]], dict[str, list[Any]]]
    inverse: Callable[[dict[str, list[Any]]], str]

    @property
    def spec_id(self) -> str:
        return alpha_id(self.index)

    @property
    def begin(self) -> str:
        return f"{MARKER_PREFIX}BEGIN{self.spec_id}"

    @property
    def end(self) -> str:
        return f"{MARKER_PREFIX}END{self.spec_id}"


def compile_specs(plan: dict[str, Any]) -> list[CompiledSpec]:
    compiled: list[CompiledSpec] = []
    for index, spec in enumerate(plan.get("specs", [])):
        pattern_text = spec.get("pattern") or spec.get("regex")
        if not pattern_text:
            raise ValueError(f"Spec {index} has no regex pattern")
        replacement = spec.get("replacement", "{placeholder}")
        if "{placeholder}" not in replacement:
            raise ValueError(f"Spec {index} has no placeholder route")

        program = spec.get("program") or {"op": "auto_codec"}
        if program.get("op") == "auto_codec":
            forward = lambda groups: {"stored": [groups[0]], "layout": []}
            inverse = lambda record: str(record["stored"][0])
            group_regex = None
        else:
            code = program.get("code")
            if not code:
                raise ValueError(f"Spec {index} is not executable python_exec/auto_codec")
            namespace: dict[str, Any] = {
                "__builtins__": SAFE_BUILTINS,
                "datetime": datetime,
                "math": math,
                "re": re,
            }
            exec(compile(code, f"<semzip-plan-{index}>", "exec"), namespace)
            forward = namespace.get("forward")
            inverse = namespace.get("inverse")
            if not callable(forward) or not callable(inverse):
                raise ValueError(f"Spec {index} has no callable forward/inverse")
            group_regex = re.compile(program["group_regex"]) if program.get("group_regex") else None

        compiled.append(
            CompiledSpec(
                index=index,
                pattern=re.compile(pattern_text),
                replacement=replacement,
                store_group=int(spec.get("store_group", 0)),
                group_regex=group_regex,
                forward=forward,
                inverse=inverse,
            )
        )
    return compiled


def expand_replacement(replacement: str, placeholder: str, match: re.Match[str]) -> str:
    result = replacement.replace("{placeholder}", placeholder)
    for index in range(0, len(match.groups()) + 1):
        value = match.group(index) or ""
        result = result.replace(f"{{group{index}}}", value)
    return result


def make_marker(spec: CompiledSpec, record: dict[str, list[Any]]) -> str:
    parts = [spec.begin]
    for channel, values in (("STORED", record.get("stored", [])), ("LAYOUT", record.get("layout", []))):
        for field_index, value in enumerate(values):
            kind, token = encode_scalar(value)
            field_marker = f"{MARKER_PREFIX}{channel}{kind}{spec.spec_id}Q{alpha_id(field_index)}"
            parts.extend((field_marker, token))
    parts.append(spec.end)
    return " " + " ".join(parts) + " "


def groups_for_forward(spec: CompiledSpec, match: re.Match[str]) -> list[str]:
    try:
        selected = match.group(spec.store_group)
    except IndexError as error:
        raise ValueError(f"Spec {spec.index} store_group is out of range") from error
    if selected is None:
        selected = ""
    if spec.group_regex is None:
        return [selected]
    parsed = spec.group_regex.fullmatch(selected)
    if parsed is None:
        raise ValueError(f"Spec {spec.index} group_regex rejected {selected!r}")
    return [value or "" for value in parsed.groups()]


def transform_line(line: str, specs: list[CompiledSpec], counts: list[int]) -> str:
    if MARKER_PREFIX in line:
        raise ValueError(f"Input collides with reserved marker prefix {MARKER_PREFIX}")

    accepted: list[tuple[int, int, CompiledSpec, re.Match[str]]] = []
    occupied: list[tuple[int, int]] = []
    for spec in specs:
        for match in spec.pattern.finditer(line):
            start, end = match.span()
            if end <= start or any(start < other_end and end > other_start for other_start, other_end in occupied):
                continue
            occupied.append((start, end))
            accepted.append((start, end, spec, match))

    if not accepted:
        return line

    result: list[str] = []
    cursor = 0
    for start, end, spec, match in sorted(accepted, key=lambda item: item[0]):
        groups = groups_for_forward(spec, match)
        record = spec.forward(groups)
        if not isinstance(record, dict) or not isinstance(record.get("stored"), list) or not isinstance(record.get("layout", []), list):
            raise ValueError(f"Spec {spec.index} forward returned an invalid record")
        record.setdefault("layout", [])
        restored_value = str(spec.inverse(record))
        if expand_replacement(spec.replacement, restored_value, match) != match.group(0):
            raise ValueError(f"Spec {spec.index} failed byte-exact forward/inverse validation")
        result.append(line[cursor:start])
        result.append(expand_replacement(spec.replacement, make_marker(spec, record), match))
        cursor = end
        counts[spec.index] += 1
    result.append(line[cursor:])
    return "".join(result)


FIELD_RE = re.compile(
    rf"^{MARKER_PREFIX}(STORED|LAYOUT)(INTEGER|STRING|FLOAT|BOOLEAN|JSON)([A-Z]+)Q([A-Z]+)$"
)


def restore_marker(spec: CompiledSpec, payload: str) -> str:
    tokens = payload.split()
    if len(tokens) % 2:
        raise ValueError(f"Malformed marker payload for spec {spec.index}")
    channels: dict[str, dict[int, Any]] = {"STORED": {}, "LAYOUT": {}}
    for index in range(0, len(tokens), 2):
        marker, value_token = tokens[index], tokens[index + 1]
        parsed = FIELD_RE.fullmatch(marker)
        if parsed is None or parsed.group(3) != spec.spec_id:
            raise ValueError(f"Unexpected field marker {marker!r} for spec {spec.index}")
        channel, kind, _, field_id = parsed.groups()
        channels[channel][alpha_index(field_id)] = decode_scalar(kind, value_token)
    record = {
        "stored": [value for _, value in sorted(channels["STORED"].items())],
        "layout": [value for _, value in sorted(channels["LAYOUT"].items())],
    }
    return str(spec.inverse(record))


def restore_line(line: str, specs: list[CompiledSpec], counts: list[int]) -> str:
    result = line
    for spec in specs:
        marker_pattern = re.compile(rf" {re.escape(spec.begin)} (.*?) {re.escape(spec.end)} ")

        def replace(match: re.Match[str]) -> str:
            counts[spec.index] += 1
            return restore_marker(spec, match.group(1))

        result = marker_pattern.sub(replace, result)
    if MARKER_PREFIX in result:
        raise ValueError("Unconsumed SemZip marker after inverse replay")
    return result


def process_file(input_path: Path, output_path: Path, plan_path: Path, restore: bool) -> dict[str, Any]:
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    specs = compile_specs(plan)
    counts = [0 for _ in specs]
    operation = restore_line if restore else transform_line
    line_count = 0
    with input_path.open("r", encoding="utf-8", errors="surrogateescape", newline="") as source, output_path.open(
        "w", encoding="utf-8", errors="surrogateescape", newline=""
    ) as target:
        for line in source:
            target.write(operation(line, specs, counts))
            line_count += 1
    return {
        "mode": "restore" if restore else "transform",
        "input": str(input_path),
        "output": str(output_path),
        "plan": str(plan_path),
        "plan_sha256": sha256_file(plan_path),
        "line_count": line_count,
        "spec_counts": counts,
    }


def _process_block_job(arguments: tuple[Path, Path, Path, bool]) -> dict[str, Any]:
    return process_file(*arguments)


def process_file_parallel(
    input_path: Path,
    output_path: Path,
    plan_path: Path,
    restore: bool,
    block_size: int,
    workers: int,
    temp_dir: Path,
) -> dict[str, Any]:
    temp_dir.mkdir(parents=True, exist_ok=False)
    input_blocks: list[Path] = []
    output_blocks: list[Path] = []
    current_output = None
    try:
        with input_path.open("r", encoding="utf-8", errors="surrogateescape", newline="") as source:
            for line_index, line in enumerate(source):
                if line_index % block_size == 0:
                    if current_output is not None:
                        current_output.close()
                    block_index = line_index // block_size
                    input_block = temp_dir / f"input_{block_index:06d}.log"
                    output_block = temp_dir / f"output_{block_index:06d}.log"
                    input_blocks.append(input_block)
                    output_blocks.append(output_block)
                    current_output = input_block.open("w", encoding="utf-8", errors="surrogateescape", newline="")
                current_output.write(line)
        if current_output is not None:
            current_output.close()
            current_output = None

        jobs = [(source, target, plan_path, restore) for source, target in zip(input_blocks, output_blocks)]
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            summaries = list(executor.map(_process_block_job, jobs))

        with output_path.open("wb") as target:
            for output_block in output_blocks:
                with output_block.open("rb") as source:
                    shutil.copyfileobj(source, target, length=8 * 1024 * 1024)

        spec_count = len(summaries[0]["spec_counts"]) if summaries else 0
        counts = [sum(summary["spec_counts"][index] for summary in summaries) for index in range(spec_count)]
        return {
            "mode": "restore" if restore else "transform",
            "input": str(input_path),
            "output": str(output_path),
            "plan": str(plan_path),
            "plan_sha256": sha256_file(plan_path),
            "line_count": sum(summary["line_count"] for summary in summaries),
            "block_count": len(summaries),
            "workers": workers,
            "spec_counts": counts,
        }
    finally:
        if current_output is not None:
            current_output.close()
        shutil.rmtree(temp_dir, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("transform", "restore"))
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("plan", type=Path)
    args = parser.parse_args()
    summary = process_file(args.input, args.output, args.plan, args.mode == "restore")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
