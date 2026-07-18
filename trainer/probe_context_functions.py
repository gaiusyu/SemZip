#!/usr/bin/env python3
"""Regenerate one family's functions without running compression."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import semzip_pure


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-prompt", required=True)
    parser.add_argument("--family-report", required=True)
    parser.add_argument("--family-id", type=int, required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--model", default="gpt-4o")
    return parser.parse_args()


def extract_examples(source_prompt: str) -> list[str]:
    examples_section = source_prompt.split("Required value-like slots", 1)[0]
    return re.findall(r"^Example \d+: (.+)$", examples_section, flags=re.MULTILINE)


def collect_functions(payload: dict[str, object]) -> list[dict[str, object]]:
    functions: list[dict[str, object]] = []
    header = payload.get("header")
    if isinstance(header, dict) and isinstance(header.get("functions"), list):
        functions.extend(item for item in header["functions"] if isinstance(item, dict))
    for key in ("functions", "class_1_functions", "class_2_functions", "class_3_functions"):
        items = payload.get(key)
        if isinstance(items, list):
            functions.extend(item for item in items if isinstance(item, dict))
    return functions


def main() -> int:
    cli = parse_args()
    root = Path(__file__).resolve().parent
    source_prompt = Path(cli.source_prompt).read_text(encoding="utf-8")
    examples = extract_examples(source_prompt)
    if not examples:
        raise RuntimeError("no examples found in source prompt")

    report = json.loads(Path(cli.family_report).read_text(encoding="utf-8"))
    family_record = next(item for item in report if int(item["family_id"]) == cli.family_id)
    key = tuple(str(family_record["key"]).split())
    family = semzip_pure.Family(
        family_id=cli.family_id,
        key=key,
        key_set=set(key),
        examples=examples,
    )
    prompt = semzip_pure.build_prompt(
        family,
        "OpenSSH",
        len(examples),
        whole_line_program_cache=True,
        open_arithmetic_programs=True,
        slot_complete_planner=True,
        generic_token_shapes=True,
        function_first_prompt=True,
        free_form_program_prompt=True,
        free_form_transducer_sketch_prompt=True,
        semantic_focus_program_prompt=True,
        semantic_rich_examples_prompt=True,
        semantic_three_class_program_prompt=True,
        strict_three_class_context_schema=True,
    )

    defaults = json.loads((root / "pure_args_defaults.json").read_text(encoding="utf-8"))
    defaults.update(
        model=cli.model,
        temperature=0.0,
        force_llm=True,
        timeout=240.0,
        api_retries=2,
        llm_cache_only=False,
        llm_family_cache_fallback=False,
        llm_global_cache_fallback=False,
    )
    api_args = argparse.Namespace(**defaults)
    output_dir = Path(cli.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    response_path = output_dir / "response.json"
    payload = semzip_pure.call_llm(prompt, api_args, response_path)

    summary = []
    for item in collect_functions(payload):
        summary.append(
            {
                "tag": item.get("tag"),
                "class": item.get("class"),
                "value_type": item.get("value_type"),
                "target_role": item.get("target_role"),
                "context": item.get("context"),
                "context_ref": item.get("context_ref"),
                "context_group": item.get("context_group"),
                "context_policy": item.get("context_policy"),
                "entropy_context": item.get("entropy_context"),
                "source_separation": item.get("source_separation"),
                "regex": item.get("regex"),
                "replacement": item.get("replacement"),
            }
        )
    (output_dir / "function_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(json.dumps(semzip_pure.get_llm_runtime_stats(api_args), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
