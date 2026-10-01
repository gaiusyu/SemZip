#!/usr/bin/env python3
"""Streaming template-cache + LLM function prototype for Apache.

Version IDs:
- V-stream-template-llm-apache-r1: family-local extraction only.
- V-stream-template-llm-apache-r2: optional canonical global streams first,
  then family-local residual extraction.

This is intentionally a small experimental driver.  It processes the input
line-by-line, maps each line to a fuzzy template family, asks the configured LLM
API only when a family becomes frequent enough, verifies the proposed ordered
functions, and stores the transformed lines as an ID+Mapping stream plus
lossless side streams.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import lzma
import math
import os
import re
import shutil
import shlex
import subprocess
import string
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

import pare_dataset_extract as dataset_extract
import pare_rank_proto as base

try:
    import _pare_cpp_accel
except Exception:
    _pare_cpp_accel = None


DEFAULT_API_BASE = "https://yunwu.ai/v1/chat/completions"
DEFAULT_MODEL = "gpt-4o"
VERSION_ID_R1 = "V-stream-template-llm-apache-r1"
VERSION_ID_R2 = "V-stream-template-llm-apache-r2"
VERSION_ID_R3 = "V-stream-template-llm-apache-r3-relation"
VERSION_ID_R4 = "V-stream-template-llm-whole-line-r4"
VERSION_ID_R5 = "V-stream-template-llm-semantic-registry-r5"
VERSION_ID_R6 = "V-stream-template-llm-semantic-global-replay-r6"
VERSION_ID_R7 = "V-stream-template-llm-validated-program-reindex-r7"
VERSION_ID_R8 = "V-stream-template-llm-residual-slot-transducer-r8"


@dataclass
class CandidateSpec:
    tag: str
    pattern: str
    replacement: str
    program: dict[str, object]
    store_group: int
    kind: str
    semantic_key: str = ""
    general: bool = False
    order_hint: str = ""
    semantic_class: str = ""
    value_type: str = ""
    context_group: int = 0
    context_policy: str = ""
    proposal_tag: str = ""
    context_ref: str = ""
    full_match_groups: bool = False


@dataclass
class Family:
    family_id: int
    key: tuple[str, ...]
    key_set: set[str]
    examples: list[str] = field(default_factory=list)
    line_indexes: list[int] = field(default_factory=list)
    active_specs: list[CandidateSpec] = field(default_factory=list)
    queried: bool = False
    rejected_reason: str = ""
    next_llm_probe_support: int = 0
    online_router_score: float = 0.0


@dataclass
class ProgramCandidate:
    family_id: int
    specs: list[CandidateSpec]
    support: int
    key: tuple[str, ...]


def spec_to_plan(spec: CandidateSpec) -> dict[str, object]:
    return {
        "tag": spec.tag,
        "pattern": spec.pattern,
        "replacement": spec.replacement,
        "program": spec.program,
        "store_group": spec.store_group,
        "kind": spec.kind,
        "semantic_key": spec.semantic_key,
        "general": spec.general,
        "order_hint": spec.order_hint,
        "semantic_class": spec.semantic_class,
        "value_type": spec.value_type,
        "context_group": spec.context_group,
        "context_policy": spec.context_policy,
        "proposal_tag": spec.proposal_tag,
        "context_ref": spec.context_ref,
        "full_match_groups": spec.full_match_groups,
    }


def spec_from_plan(payload: dict[str, object]) -> CandidateSpec:
    program = payload.get("program", {})
    if not isinstance(program, dict):
        program = {}
    return CandidateSpec(
        tag=str(payload["tag"]),
        pattern=str(payload["pattern"]),
        replacement=str(payload["replacement"]),
        program=program,
        store_group=int(payload.get("store_group", 1)),
        kind=str(payload.get("kind", "auto")),
        semantic_key=str(payload.get("semantic_key", "")),
        general=bool(payload.get("general", False)),
        order_hint=str(payload.get("order_hint", "")),
        semantic_class=str(payload.get("semantic_class", "")),
        value_type=str(payload.get("value_type", "")),
        context_group=int(payload.get("context_group", 0) or 0),
        context_policy=str(payload.get("context_policy", "")),
        proposal_tag=str(payload.get("proposal_tag", "")),
        context_ref=str(payload.get("context_ref", "")),
        full_match_groups=bool(payload.get("full_match_groups", False)),
    )


def load_replay_plan(path: str | None, dataset: str) -> tuple[list[CandidateSpec], dict[str, str]] | None:
    if not path:
        return None
    plan_path = Path(path)
    if not plan_path.is_file():
        return None
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    if str(payload.get("dataset", "")) != dataset:
        return None
    placeholders = {
        str(tag): str(placeholder)
        for tag, placeholder in dict(payload.get("placeholders", {})).items()
    }
    specs = [spec_from_plan(item) for item in list(payload.get("specs", [])) if isinstance(item, dict)]
    if os.environ.get("PARE_WIDTH_AGNOSTIC_NUMERIC_REGEX", "0") == "1":
        specs = [width_agnostic_numeric_replay_spec(spec) for spec in specs]
    # A verified empty plan is a valid residual-only deployment stage.
    if specs and not placeholders:
        return None
    return specs, placeholders


def load_replay_payload(path: str | None, dataset: str) -> dict[str, Any] | None:
    if not path:
        return None
    plan_path = Path(path)
    if not plan_path.is_file():
        return None
    try:
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if str(payload.get("dataset", "")) != dataset:
        return None
    return payload


def is_stage_residual_spec(spec: CandidateSpec) -> bool:
    pattern = spec.pattern
    return pattern.startswith((
        "residual-variable:",
        "residual-variable-plan:",
        "numeric-lattice:",
        "slot-fission:",
        "slot-fission-parent:",
        "line-transducer:",
        "residual-line:",
    ))


def load_slot_fission_decision_plan(path: str | None, dataset: str) -> list[dict[str, object]]:
    if not path:
        return []
    plan_path = Path(path)
    if not plan_path.is_file():
        return []
    try:
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if str(payload.get("dataset", "")) != dataset:
        return []
    plan = payload.get("slot_fission_decisions", [])
    if not isinstance(plan, list):
        return []
    return [item for item in plan if isinstance(item, dict)]


def load_numeric_lattice_decision_plan(path: str | None, dataset: str) -> list[dict[str, object]]:
    if not path:
        return []
    plan_path = Path(path)
    if not plan_path.is_file():
        return []
    try:
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if str(payload.get("dataset", "")) != dataset:
        return []
    plan = payload.get("numeric_lattice_decisions", [])
    if not isinstance(plan, list):
        return []
    return [item for item in plan if isinstance(item, dict)]


def load_residual_schema_plan_from_replay(path: str | None, dataset: str) -> dict[str, Any] | None:
    if not path:
        return None
    plan_path = Path(path)
    if not plan_path.is_file():
        return None
    try:
        payload = json.loads(plan_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if str(payload.get("dataset", "")) != dataset:
        return None
    plan = payload.get("residual_schema_plan")
    if not isinstance(plan, dict):
        return None
    passes = plan.get("passes")
    if not isinstance(passes, list):
        return None
    return plan


def write_replay_plan(
    path: str | None,
    dataset: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    slot_fission_decisions: list[dict[str, object]] | None = None,
    numeric_lattice_decisions: list[dict[str, object]] | None = None,
    residual_schema_plan: dict[str, Any] | None = None,
    execution_plan: dict[str, Any] | None = None,
) -> None:
    if not path:
        return
    plan_path = Path(path)
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "dataset": dataset,
        "spec_count": len(specs),
        "specs": [spec_to_plan(spec) for spec in specs],
        "placeholders": {
            tag: placeholders[tag]
            for tag in sorted({spec.tag for spec in specs})
            if tag in placeholders
        },
    }
    if slot_fission_decisions:
        payload["slot_fission_decisions"] = slot_fission_decisions
    if numeric_lattice_decisions:
        payload["numeric_lattice_decisions"] = numeric_lattice_decisions
    if residual_schema_plan and residual_schema_plan.get("passes"):
        payload["residual_schema_plan"] = residual_schema_plan
    if execution_plan:
        payload["execution_plan"] = execution_plan
    plan_path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n", encoding="utf-8")


def clone_spec(spec: CandidateSpec) -> CandidateSpec:
    return spec_from_plan(spec_to_plan(spec))


def global_rescue_signature(spec: CandidateSpec) -> tuple[str, str, int, str, str]:
    return (
        spec.pattern,
        spec.replacement,
        spec.store_group,
        spec.kind,
        json.dumps(spec.program, sort_keys=True),
    )


def load_global_rescue_decision_cache(path: str | None, dataset: str) -> list[CandidateSpec]:
    """Load previously admitted global-rescue specs from an encoder-side plan.

    The cache is intentionally narrow: it only restricts the expensive
    candidate scan to value-rescue programs that were admitted in a prior
    verified run. It does not bypass block-level archive restore/SHA checks.
    """
    if not path:
        return []
    loaded = load_replay_plan(path, dataset)
    if loaded is None:
        return []
    specs, _placeholders = loaded
    rescue_signatures = {
        global_rescue_signature(spec)
        for spec in global_value_rescue_candidates()
    }
    cached: list[CandidateSpec] = []
    seen: set[tuple[str, str, int, str, str]] = set()
    for spec in specs:
        signature = global_rescue_signature(spec)
        if signature not in rescue_signatures or signature in seen:
            continue
        cached.append(clone_spec(spec))
        seen.add(signature)
    return cached


@lru_cache(maxsize=8192)
def compiled_multiline_regex(pattern: str) -> re.Pattern[str]:
    """Cache deterministic regex compilation for verifier/replay hot loops."""
    return re.compile(pattern, flags=re.MULTILINE)


def read_text_lossless(path: Path) -> str:
    # Path.read_text() performs universal-newline translation.  Logs may use
    # CRLF, and byte-for-byte losslessness requires preserving those bytes.
    return path.read_bytes().decode("latin-1")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def operation_profile_enabled(args: argparse.Namespace) -> bool:
    return bool(getattr(args, "operation_profile", False))


@contextmanager
def feature_codec_prefilter_scope(enabled: bool):
    previous = bool(getattr(dataset_extract, "FEATURE_CODEC_PREFILTER", False))
    if enabled != previous:
        dataset_extract.set_feature_codec_prefilter(enabled)
    try:
        yield
    finally:
        if enabled != previous:
            dataset_extract.set_feature_codec_prefilter(previous)


def record_operation_timing(
    args: argparse.Namespace,
    name: str,
    elapsed: float,
    count: int = 1,
) -> None:
    if not operation_profile_enabled(args):
        return
    timings = getattr(args, "_operation_timings", None)
    if timings is None:
        timings = {}
        setattr(args, "_operation_timings", timings)
    entry = timings.setdefault(name, {"seconds": 0.0, "count": 0})
    entry["seconds"] = float(entry["seconds"]) + elapsed
    entry["count"] = int(entry["count"]) + count


def operation_timings_info(args: argparse.Namespace, raw_bytes: int) -> dict[str, dict[str, float | int]]:
    timings = getattr(args, "_operation_timings", {}) or {}
    raw_mb = raw_bytes / 1_000_000.0
    result: dict[str, dict[str, float | int]] = {}
    for name, entry in sorted(timings.items()):
        seconds = float(entry.get("seconds", 0.0))
        count = int(entry.get("count", 0))
        result[name] = {
            "seconds": round(seconds, 6),
            "count": count,
            "avg_ms": round((seconds * 1000.0 / count), 6) if count else 0.0,
            "effective_mbps": round(raw_mb / seconds, 6) if seconds > 0 else 0.0,
        }
    return result


TEMPLATE_TOKEN_RE = re.compile(r"\S+")
PLACEHOLDER_TOKEN_RE = re.compile(r"<[A-Za-z][A-Za-z0-9_:-]{0,48}>")


def is_ascii_alpha(ch: str) -> bool:
    return ("A" <= ch <= "Z") or ("a" <= ch <= "z")


def strip_non_alpha_edges(token: str) -> str:
    start = 0
    end = len(token)
    while start < end and not is_ascii_alpha(token[start]):
        start += 1
    while end > start and not is_ascii_alpha(token[end - 1]):
        end -= 1
    return token[start:end]


def _template_key_py(line: str) -> tuple[str, ...]:
    """Keep only alphabetic anchor words; digit/special-heavy content is omitted."""
    body = line.rstrip("\r\n")
    words: list[str] = []
    for match in TEMPLATE_TOKEN_RE.finditer(body):
        stripped = strip_non_alpha_edges(match.group(0))
        if stripped and stripped.isalpha():
            words.append(stripped.lower())
    return tuple(words)


@lru_cache(maxsize=262_144)
def template_key(line: str) -> tuple[str, ...]:
    if _pare_cpp_accel is not None and os.environ.get("PARE_CPP_TEMPLATE_KEYS", "0") in {"1", "true", "True"}:
        try:
            key = _pare_cpp_accel.template_key(line)
            if key is not None:
                return key
        except Exception:
            pass
    return _template_key_py(line)


def template_keys_batch(lines: list[str]) -> list[tuple[str, ...]]:
    if _pare_cpp_accel is not None and os.environ.get("PARE_CPP_TEMPLATE_KEYS", "0") in {"1", "true", "True"}:
        try:
            keys = _pare_cpp_accel.template_keys(lines)
            if (
                isinstance(keys, list)
                and len(keys) == len(lines)
                and all(key is not None for key in keys)
            ):
                return keys
        except Exception:
            pass
    return [template_key(line) for line in lines]


def percentile_int(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = int(round((len(ordered) - 1) * percentile))
    idx = max(0, min(len(ordered) - 1, idx))
    return ordered[idx]


def analyze_family_match_rare_anchor(lines: list[str]) -> tuple[list[tuple[str, ...]], dict[str, Any]]:
    """Decide whether rare-anchor prefiltering is likely safe and useful.

    The prefilter is only a candidate-retrieval accelerator. Family matching
    still uses the original Jaccard threshold after retrieval, so this scan
    estimates when limiting retrieval to rare anchors is unlikely to hide the
    right family while removing a large amount of redundant candidate work.
    """
    keys = [template_key(line) for line in lines]
    line_count = len(keys)
    key_counts = Counter(keys)
    unique_keys = len(key_counts)
    key_lengths = [len(key) for key in keys]

    anchor_line_counts: Counter[str] = Counter()
    anchor_family_counts: Counter[str] = Counter()
    for key, support in key_counts.items():
        anchors = set(key)
        for anchor in anchors:
            anchor_line_counts[anchor] += support
            anchor_family_counts[anchor] += 1

    family_anchor_line_fanouts: list[int] = []
    family_anchor_family_fanouts: list[int] = []
    rare3_line_fanouts: list[int] = []
    rare3_family_fanouts: list[int] = []
    for key in key_counts:
        anchors = set(key)
        if not anchors:
            continue
        line_fanouts = sorted(anchor_line_counts[anchor] for anchor in anchors)
        family_fanouts = sorted(anchor_family_counts[anchor] for anchor in anchors)
        family_anchor_line_fanouts.extend(line_fanouts)
        family_anchor_family_fanouts.extend(family_fanouts)
        rare3_line_fanouts.append(sum(line_fanouts[:3]))
        rare3_family_fanouts.append(sum(family_fanouts[:3]))

    unique_ratio = (unique_keys / line_count) if line_count else 0.0
    p90_key_len = percentile_int(key_lengths, 0.90)
    median_line_fanout = percentile_int(family_anchor_line_fanouts, 0.50)
    median_family_fanout = percentile_int(family_anchor_family_fanouts, 0.50)
    median_rare3_line_fanout = percentile_int(rare3_line_fanouts, 0.50)
    median_rare3_family_fanout = percentile_int(rare3_family_fanouts, 0.50)

    # Long, anchor-rich templates with many distinct anchor words are the case
    # where rare-anchor retrieval helps most. Short or low-anchor logs often
    # need broad matching to preserve cross-family reuse.
    enable = (
        line_count >= 10_000
        and unique_keys >= 500
        and len(anchor_family_counts) >= 1_000
        and p90_key_len >= 14
        and median_line_fanout < 20_000
    )
    stats: dict[str, Any] = {
        "line_count": line_count,
        "unique_keys": unique_keys,
        "unique_ratio": round(unique_ratio, 6),
        "distinct_anchors": len(anchor_family_counts),
        "avg_key_len": round((sum(key_lengths) / max(1, len(key_lengths))), 6),
        "p90_key_len": p90_key_len,
        "median_anchor_line_fanout": median_line_fanout,
        "median_anchor_family_fanout": median_family_fanout,
        "median_rare3_line_fanout": median_rare3_line_fanout,
        "median_rare3_family_fanout": median_rare3_family_fanout,
        "decision_enable": bool(enable),
        "decision_rule": (
            "line_count>=10000 and unique_keys>=500 and distinct_anchors>=1000 "
            "and p90_key_len>=14 and median_anchor_line_fanout<20000"
        ),
    }
    return keys, stats


def jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    if len(a) <= len(b):
        intersection = sum(1 for item in a if item in b)
    else:
        intersection = sum(1 for item in b if item in a)
    union = len(a) + len(b) - intersection
    return intersection / union if union else 1.0


def match_family(
    families: list[Family],
    key: tuple[str, ...],
    exact_index: dict[tuple[str, ...], Family],
    threshold: float,
    anchor_index: dict[str, list[Family]] | None = None,
    rare_anchor_limit: int = 0,
) -> Family | None:
    exact = exact_index.get(key)
    if exact is not None:
        return exact
    key_set = set(key)
    if anchor_index is not None:
        # Jaccard similarity above zero requires at least one shared anchor.
        # Using an inverted index keeps high-template-cardinality logs from
        # degenerating into an O(lines * families) scan.
        indexed: dict[int, Family] = {}
        anchors = list(key_set)
        if rare_anchor_limit > 0 and len(anchors) > rare_anchor_limit:
            anchors.sort(key=lambda anchor: len(anchor_index.get(anchor, ())))
            anchors = anchors[:rare_anchor_limit]
        for anchor in anchors:
            for family in anchor_index.get(anchor, []):
                indexed[family.family_id] = family
        candidate_families = indexed.values()
    else:
        candidate_families = families
    best: Family | None = None
    best_score = 0.0
    key_len = max(1, len(key))
    for family in candidate_families:
        if abs(len(family.key) - len(key)) / key_len > 0.45:
            continue
        score = jaccard(key_set, family.key_set)
        if score > best_score:
            best_score = score
            best = family
    if best is not None and best_score >= threshold:
        return best
    return None


def token_shape(token: str, semantic_presets: bool = True) -> str:
    if re.fullmatch(r"\d+", token):
        return f"D{len(token)}"
    if semantic_presets and re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", token):
        return "IPv4"
    if semantic_presets and re.fullmatch(r"\d{2}:\d{2}:\d{2}(?:[:.]\d+)?", token):
        return "TIME"
    if semantic_presets and re.fullmatch(r"\d{8}-\d{2}:\d{2}:\d{2}:\d{3}", token):
        return "HEALTH_TS"
    out: list[str] = []
    last = ""
    count = 0
    for ch in token:
        if ch.isdigit():
            kind = "D"
        elif ch.isalpha():
            kind = "A"
        else:
            kind = ch
        if kind == last:
            count += 1
        else:
            if last:
                out.append(f"{last}{count}" if last in {"A", "D"} else last)
            last = kind
            count = 1
    if last:
        out.append(f"{last}{count}" if last in {"A", "D"} else last)
    return "".join(out)[:80]


def example_token_table(
    examples: list[str],
    max_examples: int = 6,
    semantic_token_shapes: bool = True,
) -> str:
    rows: list[str] = []
    for example_index, example in enumerate(examples[:max_examples]):
        tokens = re.findall(r"\S+", example.rstrip("\r\n"))
        rows.append(f"Example {example_index}: {example.rstrip()}")
        rows.append(
            "Tokens: "
            + " | ".join(
                f"S{token_index}={token!r}:{token_shape(token, semantic_presets=semantic_token_shapes)}"
                for token_index, token in enumerate(tokens[:48])
            )
        )
    return "\n".join(rows)


def slot_complete_value_like(token: str) -> bool:
    """Return whether a token should receive an explicit slot action.

    This is intentionally broad and dataset-blind.  It does not decide that a
    token must be extracted; it only asks the proposer to account for tokens
    whose bytes are likely to fragment templates or side streams.
    """
    if not token:
        return False
    if re.fullmatch(r"<[A-Za-z][A-Za-z0-9_:-]{0,48}>", token):
        return False
    if any(ch.isdigit() for ch in token):
        return True
    if len(token) <= 2:
        return False
    has_alpha = any(ch.isalpha() for ch in token)
    has_special = any((not ch.isalnum()) for ch in token)
    if has_alpha and has_special:
        return True
    return False


def slot_complete_required_slots(
    examples: list[str],
    max_examples: int = 6,
    semantic_token_shapes: bool = True,
) -> tuple[str, int]:
    """Build a compact value-slot table for slot-complete LLM planning."""
    rows: list[str] = []
    count = 0
    for example_index, example in enumerate(examples[:max_examples]):
        tokens = re.findall(r"\S+", example.rstrip("\r\n"))
        slot_items: list[str] = []
        for token_index, token in enumerate(tokens[:64]):
            if not slot_complete_value_like(token):
                continue
            slot_items.append(
                f"S{token_index}={token!r}:{token_shape(token, semantic_presets=semantic_token_shapes)}"
            )
            count += 1
        if slot_items:
            rows.append(f"Example {example_index}: " + " | ".join(slot_items))
    if not rows:
        rows.append("No value-like token slots were detected in the sampled examples.")
    return "\n".join(rows), count


def select_family_prompt_examples(
    family: Family,
    original_lines: list[str],
    max_examples: int,
) -> list[str]:
    """Choose representative examples for an offline family-level LLM query.

    The online prototype kept only the first few examples.  For an offline
    top-k family pass, first-only samples can miss late variants in the same
    family.  This sampler remains deterministic and dataset-blind: it takes
    evenly spaced family members, keeps byte-identical lines only once, and
    preserves the selected order.
    """
    if max_examples <= 0 or not family.line_indexes:
        return []
    if len(family.line_indexes) <= max_examples:
        candidate_positions = list(range(len(family.line_indexes)))
    else:
        candidate_positions = []
        denom = max(1, max_examples - 1)
        for offset in range(max_examples):
            candidate_positions.append(round(offset * (len(family.line_indexes) - 1) / denom))
        candidate_positions.extend([0, len(family.line_indexes) // 2, len(family.line_indexes) - 1])

    selected: list[str] = []
    seen: set[str] = set()
    for pos in candidate_positions:
        if pos < 0 or pos >= len(family.line_indexes):
            continue
        line = original_lines[family.line_indexes[pos]]
        if line in seen:
            continue
        seen.add(line)
        selected.append(line)
        if len(selected) >= max_examples:
            break

    if len(selected) < max_examples:
        stride = max(1, len(family.line_indexes) // max(1, max_examples * 4))
        for pos in range(0, len(family.line_indexes), stride):
            line = original_lines[family.line_indexes[pos]]
            if line in seen:
                continue
            seen.add(line)
            selected.append(line)
            if len(selected) >= max_examples:
                break
    return selected


def family_value_signal_stats(
    family: Family,
    observed_lines: list[str] | None = None,
    max_examples: int = 8,
) -> dict[str, float]:
    """Dataset-blind features for deciding whether an API proposal is worth it.

    The router is intentionally not a semantic recognizer.  It only measures
    generic evidence that a family still contains high-entropy changing bytes:
    repeated support, value-like tokens, digit/symbol density, and line length.
    The LLM remains responsible for deciding what those bytes mean.
    """
    examples = (observed_lines if observed_lines is not None else family.examples)[:max_examples]
    token_count = 0
    value_tokens = 0
    value_token_values: list[str] = []
    digit_chars = 0
    symbol_chars = 0
    total_chars = 0
    for example in examples:
        body = example.rstrip("\r\n")
        for token in re.findall(r"\S+", body):
            token = PLACEHOLDER_TOKEN_RE.sub("", token)
            if not token:
                continue
            token_count += 1
            if slot_complete_value_like(token):
                value_tokens += 1
                value_token_values.append(token)
            digit_chars += sum(1 for ch in token if ch.isdigit())
            symbol_chars += sum(1 for ch in token if not ch.isalnum())
            total_chars += len(token)
    line_count = max(1, len(examples))
    value_diversity = len(set(value_token_values)) / max(1, len(value_token_values))
    return {
        "support": float(len(family.line_indexes)),
        "key_len": float(len(family.key)),
        "avg_tokens": token_count / line_count,
        "avg_value_tokens": value_tokens / line_count,
        "value_token_ratio": value_tokens / max(1, token_count),
        "value_diversity": value_diversity,
        "digit_density": digit_chars / max(1, total_chars),
        "symbol_density": symbol_chars / max(1, total_chars),
        "avg_line_chars": total_chars / line_count,
    }


def online_evolution_score(
    family: Family,
    observed_lines: list[str] | None = None,
) -> tuple[float, dict[str, float]]:
    return 0.0, {}


def family_program_proxy_gain(validation_lines: list[str], specs: list[CandidateSpec]) -> int:
    if not specs:
        return -10**9
    try:
        return program_validation_proxy_cost(validation_lines, []) - program_validation_proxy_cost(validation_lines, specs)
    except Exception:
        return -10**9


def select_family_program_bundle(
    validation_lines: list[str],
    candidates: list[CandidateSpec],
    args: argparse.Namespace,
) -> tuple[list[CandidateSpec], str]:
    return [], "program bundle MDL disabled in pure path"


def bump_family_probe_support(current_support: int, args: argparse.Namespace) -> int:
    growth = max(args.llm_min_support, int(max(1, current_support) * 0.50))
    return current_support + growth


def open_arithmetic_instruction() -> str:
    return (
        "Open executable transducer mode:\n"
        "- Prefer LLM-generated executable Python transforms for structured numeric fields, including timestamps, dates, durations, sizes, tuple-like IDs, and compound numeric tokens.\n"
        "- The transform must define forward(groups) and inverse(record). forward receives the captured value bytes; inverse reconstructs exactly the captured bytes from the stored integer.\n"
        "- A timestamp does not need true Unix time. A monotonic packed integer is acceptable if inverse renders the exact original bytes and deltas are locally small.\n"
        "- For YYYY-MM-DD HH:MM:SS,mmm, write Python that parses the seven numeric fields, packs them into one integer, and inverse-unpacks with the same zero padding and punctuation.\n"
        "- For bracketed or prefixed fields, capture only the value span and preserve stable punctuation in replacement groups.\n"
        "- Stable text is an anchor, not a side-stream value. Never store the same bytes twice: if a captured group is represented by {placeholder}, do not also keep that group as {groupN} in replacement.\n\n"
    )


def regex_has_literal_ascii_space(pattern: str) -> bool:
    """Return True when an LLM regex still contains a raw ASCII space byte.

    The paper-facing invariant is intentionally simple: generated regex pattern
    strings must not contain literal spaces.  A space run must be captured as a
    group such as ``( +)`` so the exact layout can be preserved in replacement
    text or in the executable function's layout stream.
    """
    index = 0
    while index < len(pattern):
        if pattern[index] != " ":
            index += 1
            continue
        run_start = index
        while index < len(pattern) and pattern[index] == " ":
            index += 1
        run_end = index
        # The only allowed literal spaces in a generated regex are the spaces
        # being captured by a layout group, e.g. "( +)" or "(  +)".
        if (
            run_start > 0
            and pattern[run_start - 1] == "("
            and run_end < len(pattern)
            and pattern[run_end] == "+"
            and run_end + 1 < len(pattern)
            and pattern[run_end + 1] == ")"
        ):
            continue
        return True
    return False


def regex_capture_literal_space_runs(pattern: str) -> str | None:
    """Return a regex variant where every literal space run is captured.

    This is a deterministic layout repair for LLM proposals.  It does not add a
    new recognizer: it only broadens spaces already written by the LLM so exact
    space lengths can be stored in the generated function layout.
    """
    out: list[str] = []
    index = 0
    escaped = False
    in_class = False
    changed = False
    while index < len(pattern):
        ch = pattern[index]
        if escaped:
            out.append(ch)
            escaped = False
            index += 1
            continue
        if ch == "\\":
            out.append(ch)
            escaped = True
            index += 1
            continue
        if ch == "[":
            in_class = True
            out.append(ch)
            index += 1
            continue
        if ch == "]":
            in_class = False
            out.append(ch)
            index += 1
            continue
        if ch == " " and not in_class:
            while index < len(pattern) and pattern[index] == " ":
                index += 1
            if index < len(pattern) and pattern[index] in "+*?":
                index += 1
            elif index < len(pattern) and pattern[index] == "{":
                end = index + 1
                while end < len(pattern) and pattern[end] != "}":
                    end += 1
                if end < len(pattern) and pattern[end] == "}":
                    index = end + 1
            out.append("( +)")
            changed = True
            continue
        out.append(ch)
        index += 1
    return "".join(out) if changed else None


def strip_outer_regex_anchors(pattern: str) -> str:
    inner = pattern
    if inner.startswith("^"):
        inner = inner[1:]
    if inner.endswith("$") and not inner.endswith(r"\$"):
        inner = inner[:-1]
    return inner


def rename_python_exec_entrypoints(code: str) -> str | None:
    renamed, forward_count = re.subn(r"\bdef\s+forward\s*\(", "def _orig_forward(", code, count=1)
    renamed, inverse_count = re.subn(r"\bdef\s+inverse\s*\(", "def _orig_inverse(", renamed, count=1)
    if forward_count != 1 or inverse_count != 1:
        return None
    return renamed


def space_layout_python_exec_program(
    pattern: str,
    program: dict[str, object],
    *,
    accept_existing_space_groups: bool = False,
) -> dict[str, object] | None:
    """Wrap a python_exec program so variable spaces/widths are layout bytes.

    The wrapper stores the original program's value plus two display-only
    layouts derived from the matched bytes: space-run lengths and digit-run
    widths.  During inverse it first calls the original renderer, then rewrites
    whitespace runs and digit widths to match the original captured text.
    """
    if str(program.get("op", "")) != "python_exec" or program.get("group_regex"):
        return None
    code = str(program.get("code", ""))
    renamed = rename_python_exec_entrypoints(code)
    if not renamed:
        return None
    if accept_existing_space_groups and not regex_has_literal_ascii_space(pattern):
        patched_pattern = pattern
    else:
        patched_pattern = regex_capture_literal_space_runs(pattern)
    if not patched_pattern:
        return None
    group_regex = "^(" + strip_outer_regex_anchors(patched_pattern) + ")$"
    wrapper = r'''

def _space_lengths(text):
    result = []
    i = 0
    while i < len(text):
        if text[i] == ' ':
            start = i
            while i < len(text) and text[i] == ' ':
                i = i + 1
            result.append(i - start)
        else:
            i = i + 1
    return result

def _digit_widths(text):
    result = []
    i = 0
    while i < len(text):
        if text[i].isdigit():
            start = i
            while i < len(text) and text[i].isdigit():
                i = i + 1
            result.append(i - start)
        else:
            i = i + 1
    return result

def _apply_space_lengths(text, lengths):
    out = ''
    i = 0
    j = 0
    while i < len(text):
        if text[i] == ' ':
            start = i
            while i < len(text) and text[i] == ' ':
                i = i + 1
            count = i - start
            if j < len(lengths):
                count = int(lengths[j])
            out = out + (' ' * count)
            j = j + 1
        else:
            out = out + text[i]
            i = i + 1
    return out

def _apply_digit_widths(text, widths):
    out = ''
    i = 0
    j = 0
    while i < len(text):
        if text[i].isdigit():
            start = i
            while i < len(text) and text[i].isdigit():
                i = i + 1
            raw = text[start:i]
            width = len(raw)
            if j < len(widths):
                width = int(widths[j])
            value = int(raw)
            out = out + str(value).zfill(width)
            j = j + 1
        else:
            out = out + text[i]
            i = i + 1
    return out

def forward(groups):
    raw = groups[0]
    base = _orig_forward([raw])
    base_layout = base.get('layout', [])
    spaces = _space_lengths(raw)
    widths = _digit_widths(raw)
    layout = [len(base_layout)]
    for item in base_layout:
        layout.append(int(item))
    layout.append(len(spaces))
    for item in spaces:
        layout.append(int(item))
    layout.append(len(widths))
    for item in widths:
        layout.append(int(item))
    return {'stored': base.get('stored', []), 'layout': layout}

def inverse(record):
    layout = record.get('layout', [])
    index = 0
    base_count = int(layout[index])
    index = index + 1
    base_layout = layout[index:index + base_count]
    index = index + base_count
    space_count = int(layout[index])
    index = index + 1
    spaces = layout[index:index + space_count]
    index = index + space_count
    width_count = int(layout[index])
    index = index + 1
    widths = layout[index:index + width_count]
    text = _orig_inverse({'stored': record.get('stored', []), 'layout': base_layout})
    text = _apply_space_lengths(text, spaces)
    text = _apply_digit_widths(text, widths)
    return text
'''
    return {
        "op": "python_exec",
        "code": renamed + wrapper,
        "group_regex": group_regex,
        "_space_layout_repair": True,
    }


def deterministic_space_layout_repair_item(item: dict[str, Any]) -> dict[str, Any] | None:
    pattern = str(item.get("regex", ""))
    replacement = str(item.get("replacement", "{placeholder}"))
    program = llm_item_program(item)
    if replacement != "{placeholder}" or not regex_has_literal_ascii_space(pattern):
        return None
    repaired_pattern = regex_capture_literal_space_runs(pattern)
    if not repaired_pattern:
        return None
    repaired_program = space_layout_python_exec_program(pattern, program)
    if repaired_program is None:
        return None
    repaired = dict(item)
    repaired["regex"] = repaired_pattern
    repaired["program"] = repaired_program
    repaired["_repair_origin"] = "deterministic_space_layout"
    return repaired


def build_bare_space_repair_prompt(
    *,
    dataset: str,
    family: Family,
    item: dict[str, Any],
    max_examples: int,
) -> str:
    examples = family.examples[:max_examples]
    schema = {
        "functions": [
            {
                "tag": item.get("tag", "R"),
                "meaning": item.get("meaning", "repaired function"),
                "class": item.get("class", item.get("semantic_class", "")),
                "regex": "Python re regex with no literal ASCII space characters",
                "replacement": "local replacement containing {placeholder} and optional {groupN}",
                "program": {
                    "op": "python_exec",
                    "code": (
                        "def forward(groups):\n"
                        "    return {'stored': [groups[0]], 'layout': []}\n\n"
                        "def inverse(record):\n"
                        "    return str(record['stored'][0])"
                    ),
                },
            }
        ]
    }
    return (
        f"You are repairing one reversible extraction function for {dataset} logs.\n"
        "The previous function was rejected because its regex pattern contains literal ASCII space characters.\n"
        "Return JSON only, with this schema:\n"
        + json.dumps(schema, indent=2, ensure_ascii=False)
        + "\n\n"
        "Repair rules:\n"
        "- Do not change the intended field unless the original function is unrecoverable.\n"
        "- The repaired regex pattern string must contain no literal ASCII space characters.\n"
        "- The top-level response must be {\"functions\": [...]} even if there is only one repaired function.\n"
        "- Every repaired function replacement must include {placeholder}; otherwise the compressor has no route to the side stream.\n"
        "- Match every space run with a captured group such as '( +)'.\n"
        "- If a captured space group is stable context outside the placeholder value, preserve it in replacement with {groupN}.\n"
        "- If a captured space group is part of the placeholder value, forward(groups) must store its exact length in record['layout'] and inverse(record) must reinsert exactly that many spaces.\n"
        "- For a composed multi-group value such as a timestamp, prefer replacement '{placeholder}'. Do not replace it with '{group1}{group2}...'; that keeps the changing value in the main template and prevents compression.\n"
        "- For date/time-like spans, store one integer-like timestamp value in record['stored'][0] when possible, and store only display layout such as space lengths or widths in record['layout'].\n"
        "- Never consume bytes that inverse(record) cannot reconstruct exactly.\n"
        "- Do not use imports/from-import statements, augmented assignment, comprehensions, eval, exec, file/network/system calls, or double-underscore names in python_exec code.\n"
        "- If exact conversion is too risky, use a raw python_exec function that stores the captured value bytes exactly, but still obey the no-literal-space regex rule.\n\n"
        "Good syslog-style timestamp repair:\n"
        "{\n"
        "  \"functions\": [{\n"
        "    \"tag\": \"timestamp\",\n"
        "    \"meaning\": \"timestamp prefix\",\n"
        "    \"class\": \"class_1_composed_numeric\",\n"
        "    \"regex\": \"^([A-Z][a-z]{2})( +)(\\\\d{1,2})( +)(\\\\d{2}:\\\\d{2}:\\\\d{2})\",\n"
        "    \"replacement\": \"{placeholder}\",\n"
        "    \"program\": {\"op\": \"python_exec\", \"code\": \"def forward(groups):\\n    months = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}\\n    hh, mm, ss = groups[4].split(':')\\n    v = (((months[groups[0]] * 32 + int(groups[2])) * 24 + int(hh)) * 60 + int(mm)) * 60 + int(ss)\\n    return {'stored': [v], 'layout': [len(groups[1]), len(groups[3])]}\\n\\ndef inverse(record):\\n    names = ['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']\\n    v = int(record['stored'][0])\\n    ss = v % 60; v = v // 60\\n    mm = v % 60; v = v // 60\\n    hh = v % 24; v = v // 24\\n    day = v % 32; month = v // 32\\n    return names[month] + (' ' * record['layout'][0]) + str(day) + (' ' * record['layout'][1]) + f'{hh:02d}:{mm:02d}:{ss:02d}'\"}\n"
        "  }]\n"
        "}\n\n"
        "Rejected function:\n"
        + json.dumps(item, indent=2, ensure_ascii=False)
        + "\n\n"
        "Family examples:\n"
        + "".join(f"{index + 1}. {line}" for index, line in enumerate(examples))
    )


def bare_space_repair_payload_for_item(
    *,
    family: Family,
    item: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, Any]:
    prompt = build_bare_space_repair_prompt(
        dataset=args.dataset,
        family=family,
        item=item,
        max_examples=args.examples_per_family,
    )
    item_hash = hashlib.sha256(
        json.dumps(item, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]
    cache_path = (
        Path(args.llm_cache_dir)
        / f"bare_space_repair_family_{family.family_id:04d}_{item_hash}_{prompt_hash}.json"
    )
    payload = call_llm(prompt, args, cache_path)
    if isinstance(payload, dict):
        payload = dict(payload)
        payload.setdefault("_proposal_cache_mode", "bare_space_repair")
    return payload


def build_verifier_repair_prompt(
    *,
    dataset: str,
    family: Family,
    item: dict[str, Any],
    status: str,
    reasons: list[str],
    validation_lines: list[str],
    repair_category: str,
) -> str:
    examples = validation_lines[: min(6, len(validation_lines))]
    failed_payload = json.dumps(item, indent=2, ensure_ascii=False)
    reason_payload = json.dumps({"status": status, "reasons": reasons}, indent=2, ensure_ascii=False)
    example_payload = "\n".join(f"- {line.rstrip()}" for line in examples)
    if repair_category == "context_projector":
        category_guidance = (
            "Repair category: context_projector.\n"
            "Keep the target regex, replacement, and reversible target program unless the verifier\n"
            "explicitly says they are invalid. Repair context.code so it defines deterministic\n"
            "project_context(line, groups), uses only literal safe re.search/re.match/re.fullmatch,\n"
            "and returns the actual per-line context value as str/int or None. Context is routing\n"
            "metadata and must not be required by inverse(record).\n"
        )
    elif repair_category == "raw_variable_misclassification":
        category_guidance = (
            "Repair category: raw_variable_misclassification.\n"
            "The failed item may be a raw variable. Class 3 is allowed through either of two\n"
            "evidence routes. Route A: the captured value has an intrinsic visible structure\n"
            "containing at least two non-alphanumeric separator occurrences, and the regex\n"
            "encodes that structure explicitly, as in x.x.x.x or /a/b. Route B: a plain string\n"
            "is immediately introduced by a stable, unambiguous field-name literal that is\n"
            "present in the regex, as in user( +)([A-Za-z0-9]+) or username=([A-Za-z0-9]+).\n"
            "Whitespace alone and generic words such as for, from, to, or by are not field-name\n"
            "anchors. Never emit an unanchored broad capture such as ([A-Za-z0-9]+), (\\w+),\n"
            "or ([^ ]+). When either evidence route holds, output class_3_common_variable,\n"
            "value_type string, and a raw identity python_exec program. Otherwise leave the\n"
            "value to residual.\n"
        )
    elif repair_category == "semantic_equivalent_display":
        category_guidance = (
            "Repair category: semantic_equivalent_display.\n"
            "The failed item probably contains multiple textual displays of the same semantic\n"
            "value, for example '1228 bytes (1.19 KB) sent'. Capture the whole display span\n"
            "that belongs to one value, store one canonical integer plus only the layout needed\n"
            "for byte-exact reconstruction, and make inverse(record) rebuild the captured text.\n"
            "Do not split the equivalent displays into unrelated small numeric streams.\n"
        )
    else:
        category_guidance = (
            "Repair category: unknown.\n"
            "If the function cannot be repaired within the schema, return an empty functions list.\n"
        )
    return (
        f"You are a verifier-repair agent for {dataset} log compression.\n"
        "Repair exactly one rejected reversible extraction function.\n"
        "Return exact JSON only: {\"functions\": [ ... ]}.\n"
        "\n"
        "Goal:\n"
        "- Keep the original semantic intent when possible.\n"
        "- Make the function pass a strict byte-exact verifier.\n"
        "- Do not add dataset-specific constants beyond what appears in the examples and failed regex.\n"
        "- Prefer the smallest safe repair: fix class/value_type, regex boundary, replacement, or python_exec code.\n"
        f"{category_guidance}"
        "\n"
        "Required function schema:\n"
        "{\n"
        "  \"tag\": \"short_name\",\n"
        "  \"meaning\": \"short meaning\",\n"
        "  \"class\": \"class_1_composed_numeric | class_2_formatted_numeric | class_3_common_variable\",\n"
        "  \"value_type\": \"numeric | string\",\n"
        "  \"target_role\": \"log_timestamp | ordinary_value\",\n"
        "  \"context\": null,\n"
        "  \"regex\": \"Python regex with explicit capture groups\",\n"
        "  \"replacement\": \"template containing {placeholder} and any needed {groupN}\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    ...\\n\\ndef inverse(record):\\n    ...\"\n"
        "  }\n"
        "}\n"
        "\n"
        "Python execution contract:\n"
        "- forward(groups) receives the captured value groups selected by the compiler.\n"
        "- It returns {'stored': [...], 'layout': [...]}.\n"
        "- inverse(record) reconstructs exactly the placeholder text from record['stored'] and record['layout'].\n"
        "- For raw variables, use this executable pattern:\n"
        "  def forward(groups):\n"
        "      return {'stored': [groups[0]], 'layout': []}\n"
        "  def inverse(record):\n"
        "      return str(record['stored'][0])\n"
        "- For numeric variables, store int values, not strings.\n"
        "- Preserve exact spaces by capturing them as ( +) and either keeping them as {groupN} in replacement or storing layout.\n"
        "- When repairing context, replace null with {\"code\": \"def project_context(line, groups): ...\"}; the function returns the actual context value or None.\n"
        "\n"
        "Verifier failure:\n"
        f"{reason_payload}\n"
        "\n"
        "Failed function:\n"
        f"{failed_payload}\n"
        "\n"
        "Family examples:\n"
        f"{example_payload}\n"
    )


def verifier_repair_category_for_item(
    item: dict[str, Any],
    *,
    status: str,
    reasons: list[str],
) -> str:
    """Allow repair only for failure modes that repair is meant to solve.

    The repair agent should not be a second general proposer.  It is only a
    verifier-facing patcher for two cases that are both valuable and easy to
    validate: raw variables that were misclassified as numeric/semantic, and
    multi-display values where the same value appears in several textual forms.
    """
    if item.get("_verifier_repair_origin"):
        return ""
    if status in {
        "rejected_context_projector_schema",
        "rejected_context_projector_safety",
        "rejected_context_projector_validation",
    }:
        return "context_projector"
    payload = {
        "tag": item.get("tag", ""),
        "meaning": item.get("meaning", ""),
        "class": item.get("class", ""),
        "value_type": item.get("value_type", ""),
        "regex": item.get("regex", ""),
        "replacement": item.get("replacement", ""),
        "program": item.get("program", {}),
        "status": status,
        "reasons": reasons,
    }
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True).lower()
    pattern = str(item.get("regex", "")).lower()
    meaning_tag = " ".join(
        str(item.get(key, "")).lower()
        for key in ("tag", "meaning", "class", "value_type")
    )

    byte_display_terms = ("bytes", " kb", " mb", " gb", "kib", "mib", "gib")
    if status in {
        "rejected_semantic_three_class",
        "rejected_no_verified_program",
        "rejected_validation",
        "rejected_semantic_numeric_only",
    }:
        has_byte_count = "bytes" in blob
        has_secondary_unit = any(term in blob for term in byte_display_terms[1:]) or "(?:" in pattern and "bytes" in pattern
        if has_byte_count and has_secondary_unit:
            return "semantic_equivalent_display"

    if status != "rejected_semantic_three_class":
        return ""

    # Raw-variable repair is intentionally narrower than "anything stringy".
    # Endpoint/port/status repairs fragmented Proxifier in earlier trials, so
    # keep them out unless the proposal is clearly just a raw variable.
    excluded_terms = (
        "endpoint",
        "hostport",
        "host_port",
        "port",
        "status",
        "bitness",
        "bytes",
        "duration",
        "lifetime",
        "sent",
        "received",
    )
    if any(term in meaning_tag for term in excluded_terms):
        return ""
    if re.search(r":\\d|\(\?:.*:\\d|:\\\\d|port\\b", pattern):
        return ""
    raw_terms = (
        "ipv4",
        "ip address",
        "client ip",
        "source ip",
        "remote ip",
        "rhost",
        "host name",
        "hostname",
        "domain",
        "reverse dns",
        "path",
        "file path",
        "directory",
        "username",
        "user name",
        "session id",
        "sessionid",
        "request id",
        "opaque id",
        "hash",
    )
    pattern_markers = (
        r"\\d\{1,3\}\\.",
        r"\.",
        r"/",
        r"\\s\+",
    )
    if any(term in meaning_tag for term in raw_terms):
        return "raw_variable_misclassification"
    if any(marker in pattern for marker in pattern_markers) and any(
        term in blob for term in ("ip", "host", "path", "session", "user", "domain")
    ):
        return "raw_variable_misclassification"
    return ""


def verifier_repair_payload_for_item(
    *,
    family: Family,
    item: dict[str, Any],
    status: str,
    reasons: list[str],
    validation_lines: list[str],
    args: argparse.Namespace,
    repair_category: str,
) -> dict[str, Any]:
    prompt = build_verifier_repair_prompt(
        dataset=args.dataset,
        family=family,
        item=item,
        status=status,
        reasons=reasons,
        validation_lines=validation_lines,
        repair_category=repair_category,
    )
    item_hash = hashlib.sha256(
        json.dumps(item, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()[:16]
    reason_hash = hashlib.sha256(
        json.dumps(
            {"status": status, "reasons": reasons, "repair_category": repair_category},
            sort_keys=True,
            ensure_ascii=False,
        ).encode("utf-8")
    ).hexdigest()[:12]
    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:12]
    cache_path = (
        Path(args.llm_cache_dir)
        / "_verifier_repair"
        / f"family_{family.family_id:04d}_{item_hash}_{reason_hash}_{prompt_hash}.json"
    )
    payload = call_llm(prompt, args, cache_path)
    if isinstance(payload, dict):
        payload = dict(payload)
        payload.setdefault("_proposal_cache_mode", "verifier_repair")
    return payload


def duplicate_storage_repair_variants(replacement: str) -> list[str]:
    """Repair sketches that keep and store the same changing bytes.

    A common LLM slip is ``Killed process {group1} {placeholder}``: the literal
    phrase is useful as an anchor, but the captured number must appear either as
    the placeholder value or as preserved text, never both.  The verifier tests
    every repair, so invalid variants are naturally discarded.
    """
    variants = [replacement]
    group_refs = sorted(set(re.findall(r"{group[1-6]}", replacement)))
    for group_ref in group_refs:
        escaped = re.escape(group_ref)
        repairs = [
            re.sub(escaped + r"\s+{placeholder}", "{placeholder}", replacement, count=1),
            re.sub(r"{placeholder}\s+" + escaped, "{placeholder}", replacement, count=1),
            replacement.replace(group_ref + "{placeholder}", "{placeholder}", 1),
            replacement.replace("{placeholder}" + group_ref, "{placeholder}", 1),
        ]
        for repaired in repairs:
            if repaired not in variants:
                variants.append(repaired)
    return variants


def python_exec_group_regex_variant(pattern: str, program: dict[str, object]) -> dict[str, object] | None:
    """Let a generated Python transform consume all regex capture groups.

    CandidateSpec stores one raw value.  For multi-group semantic transforms,
    the least invasive representation is to store match.group(0), attach the
    regex as group_regex, and let dataset_extract replay the capture groups into
    forward(groups).  The exact-replay verifier still decides whether this is
    legal for the proposed replacement.
    """
    if str(program.get("op", "")) != "python_exec" or not program.get("code"):
        return None
    if program.get("group_regex"):
        return None
    variant = dict(program)
    variant["group_regex"] = f"^(?:{pattern})$"
    return variant


def raw_python_exec_program() -> dict[str, object]:
    """Return the open-function representation for exact raw storage.

    Paper-facing fusion treats even opaque/raw extractions as LLM-generated
    executable functions.  The downstream codec can still choose rank/delta/raw
    for the resulting string stream; this helper only removes the fixed
    ``auto_codec`` operator from proposal/repair paths.
    """
    return {
        "op": "python_exec",
        "code": (
            "def forward(groups):\n"
            "    return {'stored': [groups[0]], 'layout': []}\n"
            "\n"
            "def inverse(record):\n"
            "    return str(record['stored'][0])"
        ),
    }


def python_exec_stores_single_integer(
    matches: list[re.Match[str]],
    program: dict[str, object],
    store_group: int,
    *,
    max_matches: int = 128,
    require_empty_layout: bool = True,
    replacement: str | None = None,
) -> bool:
    """Validate the semantic-numeric-only contract for an LLM program.

    This experimental mode intentionally narrows the LLM's job: propose only
    reversible semantic transforms whose side stream is a single integer.  Raw
    strings, paths, IPs, and opaque identifiers are left to residual/template
    side streams instead of entering as LLM raw identity functions.
    """
    if str(program.get("op", "")) != "python_exec":
        return False
    checked = 0
    for match in matches[:max_matches]:
        try:
            raw_value = match.group(store_group)
            if raw_value is None:
                continue
            record = dataset_extract.run_python_exec_forward(raw_value, program)
            stored = record.get("stored", [])
            layout = record.get("layout", [])
            if len(stored) != 1:
                return False
            value = stored[0]
            if isinstance(value, bool) or not isinstance(value, int):
                return False
            if require_empty_layout and layout:
                return False
            rendered = dataset_extract.run_python_exec_inverse(value, program, layout=list(layout))
            if replacement is None:
                if rendered != raw_value:
                    return False
            else:
                try:
                    if dataset_extract._safe_replacement_format(replacement, rendered, match) != match.group(0):
                        return False
                except Exception:
                    return False
            checked += 1
        except Exception:
            return False
    return checked > 0


def python_exec_is_semantic_three_class(
    matches: list[re.Match[str]],
    program: dict[str, object],
    store_group: int,
    semantic_class: str = "",
    value_type: str = "",
    *,
    max_matches: int = 128,
    replacement: str | None = None,
) -> bool:
    """Validate the three-class LLM function contract.

    The three classes are:
    1. composed semantic attributes stored as one integer;
    2. formatted/unit numeric values stored as one integer;
    3. common system variables stored as exact raw strings.

    Dataset- or family-local arbitrary substrings are not distinguished here;
    the prompt and later MDL/registry gates decide usefulness.  Numeric classes
    must really store an integer scalar; raw identity is accepted only for the
    common-variable class.  Layout bytes are allowed for numeric classes because
    they carry display-only information such as variable spaces or widths.
    """
    semantic_class = semantic_class.strip().lower()
    value_type = value_type.strip().lower().replace("-", "_")
    if semantic_class in {"class_1_composed_numeric", "class_2_formatted_numeric"} or value_type in {"numeric", "integer", "int"}:
        return python_exec_stores_single_integer(
            matches,
            program,
            store_group,
            max_matches=max_matches,
            require_empty_layout=False,
            replacement=replacement,
        )
    if semantic_class == "class_3_common_variable" or value_type in {"string", "raw"}:
        if dataset_extract.python_exec_is_raw_identity(
            matches[:max_matches],
            program,
            store_group,
        ):
            return True
        if replacement is not None:
            return python_exec_full_groups_direct_value(
                matches,
                program,
                replacement,
                max_matches=max_matches,
            )
        return False
    if python_exec_stores_single_integer(
        matches,
        program,
        store_group,
        max_matches=max_matches,
        require_empty_layout=True,
        replacement=replacement,
    ):
        return True
    return dataset_extract.python_exec_is_raw_identity(
        matches[:max_matches],
        program,
        store_group,
    )


def python_exec_full_groups_direct_value(
    matches: list[re.Match[str]],
    program: dict[str, object],
    replacement: str,
    *,
    max_matches: int = 128,
) -> bool:
    """Accept Class-3 raw variables whose code consumes the full regex groups.

    The stored value must be an exact string that can be inserted at
    {placeholder} to reconstruct the whole regex match.  This keeps decoding
    simple: the side stream stores the placeholder value directly.
    """
    if str(program.get("op", "")) != "python_exec":
        return False
    checked = 0
    for match in matches[:max_matches]:
        try:
            record = dataset_extract.run_python_exec_forward_groups(list(match.groups()), program)
            if record.get("layout"):
                return False
            stored = record.get("stored", [])
            if len(stored) != 1 or not isinstance(stored[0], str):
                return False
            rendered = dataset_extract.run_python_exec_inverse(stored[0], program)
            if rendered != stored[0]:
                return False
            if dataset_extract._safe_replacement_format(replacement, rendered, match) != match.group(0):
                return False
            checked += 1
        except Exception:
            return False
    return checked > 0


def build_whole_line_prompt(
    family: Family,
    dataset: str,
    max_examples: int,
    open_arithmetic_programs: bool = False,
    slot_complete_planner: bool = False,
    generic_token_shapes: bool = False,
    function_first_prompt: bool = False,
    free_form_program_prompt: bool = False,
    free_form_transducer_sketch_prompt: bool = False,
    purpose_only_program_prompt: bool = False,
    semantic_focus_program_prompt: bool = False,
    semantic_rich_examples_prompt: bool = False,
    family_header_program_prompt: bool = False,
    semantic_numeric_only_program_prompt: bool = False,
    semantic_three_class_program_prompt: bool = False,
    strict_three_class_context_schema: bool = False,
) -> str:
    examples = family.examples[:max_examples]
    semantic_token_shapes = not generic_token_shapes
    required_slots_text, required_slot_count = slot_complete_required_slots(
        examples,
        max_examples=max_examples,
        semantic_token_shapes=semantic_token_shapes,
    )
    if purpose_only_program_prompt:
        program_schema: dict[str, object] = {
            "intent": "what information is stored and how exact rendering is recovered",
            "parse": "optional: how to parse the captured bytes",
            "stored_value": "raw bytes, exact scalar, tuple, or value derived from another captured value",
            "render": "optional: exact inverse rendering rule",
        }
    elif free_form_program_prompt:
        program_schema = {
            "intent": "reversible transform program; examples include raw stream, datetime-derived text, or integer packing",
            "op": "your deterministic operation name",
            "parameters": {},
            "code": "required real Python code defining both forward(groups) and inverse(record)",
        }
    else:
        program_schema = {"op": "auto_codec"}
    schema = {
        "header": {
            "has_unified_header": True,
            "reason": "whether the sampled lines share a repeated prefix/header whose changing fields should be side streams",
            "functions": [
                {
                    "tag": "H0",
                    "meaning": "header timestamp/component/thread/etc.",
                    "class": "class_1_composed_numeric | class_2_formatted_numeric | class_3_common_variable",
                    "value_type": "numeric for class_1/class_2, string for class_3",
                    "target_role": "log_timestamp | ordinary_value",
                    "context": None,
                    "general": True,
                    "order_hint": "global_composed | family_structured | global_atomic",
                    "regex": "Python re regex for one complete header field or composed header span",
                    "replacement": "local replacement containing {placeholder} and optional {group1}",
                    "program": program_schema,
                }
            ],
        },
        "slot_actions": [
            {
                "slot": "S0 or S0+S1",
                "action": "COMPOSE_FIELDS_TO_INT | STREAM_RAW | UINT_DELTA | DICT_RANK | KEEP_IN_MAIN",
                "meaning": "semantic role if known, otherwise structural role",
                "reason": "why this slot is extracted, composed, or kept",
            }
        ],
        "functions": [
                {
                    "tag": "TS",
                    "meaning": "timestamp/date-time/etc.",
                    "class": "class_1_composed_numeric | class_2_formatted_numeric | class_3_common_variable",
                    "value_type": "numeric for class_1/class_2, string for class_3",
                    "target_role": "log_timestamp | ordinary_value",
                    "context": None,
                    "general": True,
                    "order_hint": "global_composed | family_structured | global_atomic",
                "regex": "Python re regex scoped to this line family",
                "replacement": "literal replacement containing {placeholder} and optional {group1}",
                "program": program_schema,
            }
        ]
    }
    shape_contract = (
        "The token shapes below are generic character-run signatures only. They are not field-type labels. "
        "Infer all meanings and reversible transformations from the raw examples and local context.\n\n"
        if generic_token_shapes
        else ""
    )
    meaning_contract = (
        "The 'meaning' field should be your inferred stable stream name. Do not copy a shape label as the meaning.\n"
        if function_first_prompt
        else "The 'meaning' field should be a stable semantic stream name, not a vague label. Good examples: timestamp, ipv4_client, log_level, absolute_path, process_id, port, healthapp_report_tuple.\n"
    )
    reversible_transducer_contract = (
        "Reversible transducer contract:\n"
        "- Think of each function as a four-stage reversible transducer, not just a regex.\n"
        "- Stage 1 capture: regex selects exactly one local span to rewrite. The span is match.group(0); capture groups are only parse inputs.\n"
        "- Stage 2 transform: forward(groups) converts the captured variable bytes into stored side-stream values and optional layout side information.\n"
        "- Stage 3 inverse: inverse(record) reconstructs exactly the placeholder text from record['stored'] and record['layout']; the decoder will not have the original regex groups.\n"
        "- Stage 4 route: replacement places {placeholder} back inside the local matched span. It must not contain unrelated constants from outside match.group(0).\n"
        "- The compressor stores only the values returned by forward(). If inverse(record) needs width, spaces, sign, unit spelling, optional parentheses, or display variants, forward() must store that information in record['stored'] or record['layout'].\n"
        "- Regex-space contract: the regex pattern string must contain no literal ASCII space characters at all. To match spaces, always write a capture group such as '( +)'.\n"
        "- If a captured space group is outside the placeholder value, preserve it in replacement with {groupN}. If a captured space group is part of the placeholder value, forward(groups) must store its exact length in layout and inverse(record) must reinsert exactly that many spaces.\n"
        "- Do not use an uncaptured '\\\\s+' as a silent wildcard. If '\\\\s+' is needed, it must be a capture group and its exact length must be preserved through {groupN} or stored in layout.\n"
        "- For date/time-like spans, every space run between month/day/time/year fields must be captured. A regex with an untracked plain space may pass matched-example reconstruction but fail to cover sibling layouts such as 'Dec 10 06:55:46' versus 'Jan  1 00:12:28'.\n"
        "- If a numeric field may have leading zeros or variable width, inverse(record) must preserve the width by storing width/layout or by storing a representation from which the width is derivable.\n"
        "- If a function cannot satisfy this exact inverse contract, prefer a raw python_exec function that stores the complete captured string unchanged, or do not propose the function.\n\n"
    )
    useful_examples = (
        "General transformation hints:\n"
        "- A numeric unit such as '1.23KB' can be stored as an exact integer if value_regex, value_expr, and render recover the bytes exactly.\n"
        "- In this implementation, express that exact integer transform as python_exec code instead of naming a fixed compressor operation.\n"
        "- Unit-bearing values often have spelling variants such as '123 bytes', '123 bytes (1.20 KB)', '322 KB', '1.5 MB', or '0.01 GB'. If a regex consumes the optional unit display, the function must either store the complete formatted span or store every changing subfield needed to render the exact original bytes.\n"
        "- Bad reversible design: regex '(\\d+) bytes(?: \\([^)]+\\))? sent' with replacement '{placeholder} bytes sent', because it uses bare spaces and consumes the parenthesized display without reconstructing it. Safe design: capture all spaces, e.g. '(( +))(\\d+)( +)bytes(( +)\\([^)]+\\))?(( +)sent)' with replacement '{group1}{placeholder}{group4}bytes{group5}{group7}', or provide an exact parse/render program for both the plain and parenthesized forms.\n"
        "- A multi-token date/time span can be composed into one integer stream if render can reconstruct every token exactly, including punctuation, month names, or weekday text.\n"
        "- If weekday text such as Fri/Sat/Sun is determined by the date/time and timezone, do not store it as an independent value stream; derive it during rendering from the stored timestamp-like value.\n\n"
        if function_first_prompt
        else (
            "Useful executable examples:\n"
            "- HealthApp timestamp '20171223-22:15:29:606': capture the complete timestamp value, write python_exec code that parses yyyymmdd, hh, mm, ss, mmm, packs them into one integer, and inverse renders the exact zero-padded string.\n"
            "- Apache timestamp inside brackets: capture the complete timestamp value, write python_exec code that uses datetime.strptime/strftime or explicit month arithmetic, and verify that inverse returns exactly the captured bytes.\n\n"
        )
    )
    semantic_focus_text = (
        "Open semantic-program objective:\n"
        "- Focus your limited functions on spans whose format or meaning is hard for a generic numeric residual coder to infer.\n"
        "- Important targets are not predefined field types; they are evidence patterns: adjacent tokens that together form one value, strings with embedded numbers and separators, identifiers with stable affixes, values with units, and text that can be derived from another captured value.\n"
        "- A plain isolated integer usually does not need a custom LLM function unless its local context gives it a stable role or it participates in a larger reversible transform. The compressor has a generic residual numeric coder for leftover simple numbers.\n"
        "- Prefer one reversible function for a complete semantic span over several substring functions. For example, if a weekday token, month token, day token, time token, and year token together determine one timestamp, store one timestamp-like value and derive the weekday during rendering.\n"
        "- If a token such as Fri/Sat/Sun is predictable from a date/time value and timezone, propose a program that records the date/time and rendering rule rather than a separate weekday stream.\n"
        "- If a field looks like a session id, endpoint, compound numeric tuple, domain with numeric components, path with variable components, or unit-bearing value, describe the reversible transformation that preserves its exact bytes.\n"
        "- If no compact parse/render relation is apparent, still use python_exec: store the captured string unchanged and have inverse return it unchanged.\n\n"
        if semantic_focus_program_prompt
        else ""
    )
    semantic_rich_examples_text = (
        "Illustrative sketches only, not an allowed-type list:\n"
        "- If a line contains 'Fri Jun 07 12:00:01 2024', one function may capture the whole date-time span and render 'Fri' from the parsed date/time and timezone instead of storing 'Fri' separately.\n"
        "- If a value is '1.23KB' or '42ms', a program may capture the numeric pieces and unit, store an exact integer-like scalar, and render the original spelling exactly.\n"
        "- If a value is 'session 0x15af3c' or 'sid=721348', a program may use the literal context to locate the changing id while storing only the changing id bytes or exact transformed scalar.\n"
        "- If a value is 'host-204-100-200-022.example.net:443', a program may capture the complete endpoint-like span first, then split or transform internal subfields only when exact rendering is possible.\n\n"
        if semantic_focus_program_prompt and semantic_rich_examples_prompt
        else ""
    )
    header_prompt_text = (
        "Header discovery task:\n"
        "- Before proposing message-body functions, decide whether the examples share a repeated log header near the beginning of the line.\n"
        "- A header is a repeated prefix that identifies time, level, component, thread, process, host, or source context before the free-form message body.\n"
        "- The header is not necessarily whitespace-delimited. Use punctuation and brackets as boundaries when needed, e.g. '[component name]' may contain spaces and should be captured with bracket-aware regex rather than token-by-token splitting.\n"
        "- If the header contains changing fields, put their extraction functions in header.functions. These functions must use executable python_exec code. For opaque values, write a raw python_exec that stores and returns the captured string unchanged.\n"
        "- If the header contains a composed date/time such as 'Fri Jun 07 12:00:01 2024', prefer one executable timestamp transform over separate weekday/month/time streams. Weekday text should be derived by inverse rendering when possible.\n"
        "- Header constants should remain literal; only changing header values should become side streams.\n"
        "- Also copy executable header functions into the top-level functions list if your JSON consumer does not support header.functions. The compressor will merge both lists and validate exact reconstruction.\n\n"
        if family_header_program_prompt
        else ""
    )
    semantic_numeric_only_text = (
        "Semantic numeric-only experiment:\n"
        "- In this mode, the LLM should NOT propose raw extraction functions for paths, IP addresses, usernames, hostnames, opaque ids, or arbitrary message substrings. Those leftover variables will be handled by deterministic template-position residual side streams after your functions run.\n"
        "- Propose a function only when the captured bytes can be transformed into one integer and rendered back exactly from that integer.\n"
        "- Target class A: composition. Several adjacent fields together represent one numeric semantic attribute. Example: weekday + month + day + time + year can be converted into epoch microseconds; inverse renders the original date/time text exactly from that integer.\n"
        "- Target class B: semantic numeric value. One value with units or formatting represents one integer. Example: '1.23KB' can be converted into an exact byte or centi-unit integer; inverse renders '1.23KB' exactly.\n"
        "- Every accepted function must use program {\"op\":\"python_exec\", \"code\":\"...\"}; proposals without executable forward(groups) and inverse(record) code are rejected.\n"
        "- The generated code must define forward(groups) and inverse(record). forward(groups) receives regex capture groups and must return {'stored': [one_integer], 'layout': []}. inverse(record) receives record['stored'][0] and must return the exact captured bytes. Do not return strings in stored. Do not use layout in this experiment.\n"
        "- Use only simple verifier-safe Python: assignments, if/else, arithmetic, int(), str(), len(), zfill(), indexing, and small dictionaries are okay. Do not use list(), list/dict/set comprehensions, map(), lambda, imports, generators, or dynamic eval/exec.\n"
        "- If a candidate needs to store an opaque string to be reversible, do not propose it here. Let the residual compressor handle it.\n"
        "- Mark these functions as general=true because they are global semantic transforms, not template-local raw variables.\n\n"
        "Executable example 1, composed timestamp:\n"
        "{\n"
        "  \"tag\": \"timestamp_epoch\",\n"
        "  \"meaning\": \"weekday_month_day_time_year_as_epoch_microseconds\",\n"
        "  \"class\": \"class_1_composed_numeric\",\n"
        "  \"general\": true,\n"
        "  \"regex\": \"\\\\[([A-Z][a-z]{2})( +)([A-Z][a-z]{2})( +)(\\\\d{1,2})( +)(\\\\d{2}:\\\\d{2}:\\\\d{2})( +)(\\\\d{4})\\\\]\",\n"
        "  \"replacement\": \"[{placeholder}]\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    weekday, sp1, month, sp2, day, sp3, hms, sp4, year = groups\\n    text = weekday + ' ' + month + ' ' + day + ' ' + hms + ' ' + year\\n    dt = datetime.strptime(text, '%a %b %d %H:%M:%S %Y')\\n    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': [len(sp1), len(sp2), len(sp3), len(sp4), len(day)]}\\n\\ndef inverse(record):\\n    micros = int(record['stored'][0])\\n    layout = record.get('layout', [])\\n    s1, s2, s3, s4, day_width = layout\\n    dt = datetime.fromtimestamp(micros / 1000000)\\n    weekday = dt.strftime('%a')\\n    month = dt.strftime('%b')\\n    day = str(dt.day).zfill(int(day_width))\\n    hms = dt.strftime('%H:%M:%S')\\n    year = dt.strftime('%Y')\\n    return weekday + (' ' * int(s1)) + month + (' ' * int(s2)) + day + (' ' * int(s3)) + hms + (' ' * int(s4)) + year\"\n"
        "  }\n"
        "}\n\n"
        "Executable example 2, unit-bearing value:\n"
        "{\n"
        "  \"tag\": \"size_centi_kb\",\n"
        "  \"meaning\": \"decimal_kilobyte_value_as_centi_units\",\n"
        "  \"class\": \"class_2_formatted_numeric\",\n"
        "  \"general\": true,\n"
        "  \"regex\": \"(\\\\d+)\\\\.(\\\\d{2})KB\",\n"
        "  \"replacement\": \"{placeholder}\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    whole, frac = groups\\n    v = int(whole) * 100 + int(frac)\\n    return {'stored': [v], 'layout': []}\\n\\ndef inverse(record):\\n    v = int(record['stored'][0])\\n    return str(v // 100) + '.' + str(v % 100).zfill(2) + 'KB'\"\n"
        "  }\n"
        "}\n\n"
        "Executable example 3, equivalent byte-size display:\n"
        "{\n"
        "  \"tag\": \"sent_bytes_display\",\n"
        "  \"meaning\": \"byte_count_with_optional_kb_mb_gb_display\",\n"
        "  \"class\": \"class_1_composed_numeric\",\n"
        "  \"general\": true,\n"
        "  \"regex\": \"(\\\\d+)( +)bytes(?:(( +)\\\\((\\\\d+)(?:\\\\.(\\\\d+))?( +)(KB|MB|GB)\\\\)))?( +)sent\",\n"
        "  \"replacement\": \"{placeholder}\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    byte_text, sp1, display_span, sp2, whole, frac, sp3, unit, sp4 = groups\\n    has_display = 1 if display_span else 0\\n    sp2_len = 0\\n    frac_width = 0\\n    sp3_len = 0\\n    unit_code = 0\\n    if display_span:\\n        sp2_len = len(sp2)\\n        frac_width = len(frac or '')\\n        sp3_len = len(sp3)\\n        unit_code = {'KB': 1, 'MB': 2, 'GB': 3}.get(unit, 0)\\n    layout = [len(sp1), has_display, len(sp4), sp2_len, frac_width, sp3_len, unit_code]\\n    return {'stored': [int(byte_text)], 'layout': layout}\\n\\ndef inverse(record):\\n    v = int(record['stored'][0])\\n    layout = record.get('layout', [])\\n    sp1_len = int(layout[0])\\n    has_display = int(layout[1])\\n    sp4_len = int(layout[2])\\n    display = ''\\n    if has_display:\\n        unit_names = {1: 'KB', 2: 'MB', 3: 'GB'}\\n        sp2_len = int(layout[3])\\n        frac_width = int(layout[4])\\n        sp3_len = int(layout[5])\\n        unit_code = int(layout[6])\\n        divisor = 1024\\n        if unit_code == 2:\\n            divisor = 1048576\\n        if unit_code == 3:\\n            divisor = 1073741824\\n        scale = 1\\n        if frac_width == 1:\\n            scale = 10\\n        if frac_width == 2:\\n            scale = 100\\n        if frac_width == 3:\\n            scale = 1000\\n        if frac_width == 4:\\n            scale = 10000\\n        shown = v * scale // divisor\\n        whole = shown // scale\\n        frac_value = shown % scale\\n        frac = ('.' + str(frac_value).zfill(frac_width)) if frac_width else ''\\n        display = (' ' * sp2_len) + '(' + str(whole) + frac + (' ' * sp3_len) + unit_names[unit_code] + ')'\\n    return str(v) + (' ' * sp1_len) + 'bytes' + display + (' ' * sp4_len) + 'sent'\"\n"
        "  }\n"
        "}\n\n"
        if semantic_numeric_only_program_prompt
        else ""
    )
    semantic_three_class_text = (
        "Three-class LLM function experiment:\n"
        "- The LLM should replace manually predefined common-variable regexes and semantic converters. Propose only these three classes of reversible python_exec functions.\n"
        "- Class 1, composed/equivalent numeric attribute: several adjacent fields together describe one numeric semantic value, or the same value is displayed in multiple equivalent forms. Example: weekday + month + day + time + year -> epoch microseconds; inverse renders the original timestamp text exactly. Example: '1234 bytes (1.19 KB)' describes one byte-size value with a derived display form; store the canonical integer value plus small layout/display information needed to render the exact original text.\n"
        "- Class 2, semantic numeric value: one displayed value represents one exact integer and is likely to form a compressible numeric stream. Example: '1.23KB', '42ms', '123 bytes (1.20 KB)', packet sizes, durations, counters, stable ports, stable process/thread ids, slots, or nearby quantities when inverse can render the whole matched value exactly. A value may be Class 2 even when forward(groups) is simply int(value), if it has a stable semantic role but appears under unstable textual contexts that would fragment residual fallback streams. Do not use Class 2 for plain decimal/hex identifiers that merely happen to parse as numbers but look random or high-entropy; deterministic fallback or Class 3 raw streams are better for opaque ids.\n"
        "- Class 3, bounded system variable: admit an exact string through either of two evidence routes. Route A, intrinsic structure: the captured bytes contain at least two non-alphanumeric structural-separator occurrences, and the regex explicitly encodes that structure; examples include IPv4 x.x.x.x, multi-level paths /a/b, multi-label domains a.b.example, UUID-like a-b-c-d, and URLs. Route B, explicit role anchor: a plain string is immediately introduced by a stable, unambiguous field-name literal that appears in the regex; examples include user Alice and username=Alice. For Route B, preserve the literal and exact spacing in replacement and store only the value. Whitespace alone and generic words such as for, from, to, or by are not field-name anchors. Never propose an unanchored broad capture such as ([A-Za-z0-9]+), (\\w+), or ([^ ]+). Store an admitted Class 3 value exactly as a string. Do not use raw for ordinary low-entropy numeric quantities such as timestamps, durations, sizes, counters, or repeated/local ports when a compact reversible numeric transform is clear.\n"
        "- Every function must include a \"class\" field with exactly one of: \"class_1_composed_numeric\", \"class_2_formatted_numeric\", or \"class_3_common_variable\".\n"
        "- Context principle: some values should not be compressed as one global stream because their distribution is controlled by another value in the same physical log line. For such a target, attach executable context.code that defines project_context(line, groups) and returns the actual context value for that line. Otherwise set context=null.\n"
        "- If a captured span is date/time-like, duration-like, size-like, counter-like, port-like, process/thread-like, slot-like, or otherwise expected to have locality/repetition after numeric conversion, prefer Class 1 or Class 2 over Class 3. If multiple adjacent fields are redundant displays of the same quantity, prefer Class 1 and store one canonical integer plus layout/display flags rather than separate raw streams. If a value is parseable but semantically an opaque random identifier, do not force it into Class 1/2 just because parsing is possible.\n"
        "- Strategy switch for numbers: leave anonymous/simple numbers to deterministic residual coders, but extract semantically stable numbers whose surrounding words vary across templates. For example, a port number near an IP/host should be a Class 2 function with meaning like \"port\" or \"client_port\", and its context projector may return the actual IP/host value.\n"
        "- The context does not need to be adjacent to the target value and does not need to be captured by the target regex. project_context receives the complete original physical line and the target regex groups.\n"
        "- Every function must label the stored target with target_role. Use target_role=log_timestamp only when the stored target itself is the primary timestamp of the log record. Use target_role=ordinary_value for every other stored target.\n"
        "- A log_timestamp target normally uses context=null. For every ordinary_value target, examine the other fields anywhere in the same log line and provide context.code when another field predicts or partitions the target value's distribution.\n"
        "- The log_timestamp exception applies only when the stored target itself is the primary log timestamp. It never prevents an IP address, host, component, or other field from serving as context for another target. In particular, when storing a port from a line that also contains its IP/host, the port is ordinary_value and the IP/host should be considered as its context.\n"
        "- Values whose entropy depends on another field should return that field's concrete value through project_context even when it is not adjacent: ports may be conditioned on an IP/host; process/thread ids on component/program; byte counts or durations on event type; paths/session ids/domains on operation. This is a routing hint available to all three classes.\n"
        "- Every function must be program {\"op\":\"python_exec\", \"code\":\"...\"}. For Classes 1 and 2, forward(groups) must return {'stored': [one_integer], 'layout': [optional_small_integers]}. For Class 3, forward(groups) must return {'stored': [groups[0]], 'layout': []} or equivalent exact raw identity, and inverse(record) must return the exact captured string.\n"
        "- Use only simple verifier-safe Python in python_exec code: assignments, if/else, arithmetic, int(), str(), len(), zfill(), indexing, and small dictionaries are okay. Do not use list(), list/dict/set comprehensions, map(), lambda, imports, generators, or dynamic eval/exec.\n"
        "- Layout is exactly for display-only bytes such as variable spaces, zero-padding widths, or similar reversible formatting details. Do not fall back to raw string just because a date/time has variable spaces such as 'Dec 10 06:55:46' versus 'Jan  1 00:12:28'; store the timestamp-like integer in stored[0] and store only the needed space counts or widths in layout.\n"
        "- For yearless syslog-like timestamps, pack month, day, hour, minute, and second into one monotonic integer. If the original text uses one or two spaces before the day, capture that spacing and store the exact display spacing in layout so inverse(record) can render the original bytes exactly.\n"
        "- A timestamp candidate that hard-codes untracked spaces between month/day/time fields is incomplete. Regex pattern strings must contain no literal ASCII space characters; use captured '( +)' groups plus layout instead.\n"
        "- Prefer general=true for Class 1 and Class 3 when the same pattern can recur across templates. Use general=false for clearly family-local message variables.\n"
        "- Keep stable context as literals in regex/replacement; store only the changing value once. Do not duplicate captured bytes in both {placeholder} and {groupN}.\n"
        "- Role-specific values must be anchored by their stable role words. For example, a connection duration should be matched near the literal 'lifetime' rather than by a broad time-like regex that can also match the header time.\n"
        "- If a raw string satisfies neither Class 3 evidence route, do not propose an LLM function for it; residual template-position streams will handle it. Numeric-looking values should become raw only when their role is opaque/high-entropy identity rather than quantity and they satisfy the intrinsic-structure route or a truly explicit field-name anchor.\n\n"
        "Executable example 1, composed timestamp integer:\n"
        "{\n"
        "  \"tag\": \"timestamp_epoch\",\n"
        "  \"meaning\": \"weekday_month_day_time_year_as_epoch_microseconds\",\n"
        "  \"class\": \"class_1_composed_numeric\",\n"
        "  \"value_type\": \"numeric\",\n"
        "  \"target_role\": \"log_timestamp\",\n"
        "  \"context\": null,\n"
        "  \"general\": true,\n"
        "  \"regex\": \"\\\\[([A-Z][a-z]{2})( +)([A-Z][a-z]{2})( +)(\\\\d{1,2})( +)(\\\\d{2}:\\\\d{2}:\\\\d{2})( +)(\\\\d{4})\\\\]\",\n"
        "  \"replacement\": \"[{placeholder}]\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    weekday, sp1, month, sp2, day, sp3, hms, sp4, year = groups\\n    text = weekday + ' ' + month + ' ' + day + ' ' + hms + ' ' + year\\n    dt = datetime.strptime(text, '%a %b %d %H:%M:%S %Y')\\n    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': [len(sp1), len(sp2), len(sp3), len(sp4), len(day)]}\\n\\ndef inverse(record):\\n    micros = int(record['stored'][0])\\n    layout = record.get('layout', [])\\n    s1, s2, s3, s4, day_width = layout\\n    dt = datetime.fromtimestamp(micros / 1000000)\\n    weekday = dt.strftime('%a')\\n    month = dt.strftime('%b')\\n    day = str(dt.day).zfill(int(day_width))\\n    hms = dt.strftime('%H:%M:%S')\\n    year = dt.strftime('%Y')\\n    return weekday + (' ' * int(s1)) + month + (' ' * int(s2)) + day + (' ' * int(s3)) + hms + (' ' * int(s4)) + year\"\n"
        "  }\n"
        "}\n\n"
        "Executable example 2, formatted numeric integer:\n"
        "{\n"
        "  \"tag\": \"size_centi_kb\",\n"
        "  \"meaning\": \"decimal_kilobyte_value_as_centi_units\",\n"
        "  \"class\": \"class_2_formatted_numeric\",\n"
        "  \"value_type\": \"numeric\",\n"
        "  \"target_role\": \"ordinary_value\",\n"
        "  \"context\": null,\n"
        "  \"general\": true,\n"
        "  \"regex\": \"(\\\\d+)\\\\.(\\\\d{2})KB\",\n"
        "  \"replacement\": \"{placeholder}\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    whole, frac = groups\\n    return {'stored': [int(whole) * 100 + int(frac)], 'layout': []}\\n\\ndef inverse(record):\\n    v = int(record['stored'][0])\\n    return str(v // 100) + '.' + str(v % 100).zfill(2) + 'KB'\"\n"
        "  }\n"
        "}\n\n"
        "Executable example 3, equivalent byte-size display:\n"
        "{\n"
        "  \"tag\": \"sent_bytes_display\",\n"
        "  \"meaning\": \"byte_count_with_optional_kb_mb_gb_display\",\n"
        "  \"class\": \"class_1_composed_numeric\",\n"
        "  \"value_type\": \"numeric\",\n"
        "  \"target_role\": \"ordinary_value\",\n"
        "  \"context\": null,\n"
        "  \"general\": true,\n"
        "  \"regex\": \"(\\\\d+)( +)bytes(?:(( +)\\\\((\\\\d+)(?:\\\\.(\\\\d+))?( +)(KB|MB|GB)\\\\)))?( +)sent\",\n"
        "  \"replacement\": \"{placeholder}\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    byte_text, sp1, display_span, sp2, whole, frac, sp3, unit, sp4 = groups\\n    has_display = 1 if display_span else 0\\n    sp2_len = 0\\n    frac_width = 0\\n    sp3_len = 0\\n    unit_code = 0\\n    if display_span:\\n        sp2_len = len(sp2)\\n        frac_width = len(frac or '')\\n        sp3_len = len(sp3)\\n        unit_code = {'KB': 1, 'MB': 2, 'GB': 3}.get(unit, 0)\\n    layout = [len(sp1), has_display, len(sp4), sp2_len, frac_width, sp3_len, unit_code]\\n    return {'stored': [int(byte_text)], 'layout': layout}\\n\\ndef inverse(record):\\n    v = int(record['stored'][0])\\n    layout = record.get('layout', [])\\n    sp1_len = int(layout[0])\\n    has_display = int(layout[1])\\n    sp4_len = int(layout[2])\\n    display = ''\\n    if has_display:\\n        unit_names = {1: 'KB', 2: 'MB', 3: 'GB'}\\n        sp2_len = int(layout[3])\\n        frac_width = int(layout[4])\\n        sp3_len = int(layout[5])\\n        unit_code = int(layout[6])\\n        divisor = 1024\\n        if unit_code == 2:\\n            divisor = 1048576\\n        if unit_code == 3:\\n            divisor = 1073741824\\n        scale = 1\\n        if frac_width == 1:\\n            scale = 10\\n        if frac_width == 2:\\n            scale = 100\\n        if frac_width == 3:\\n            scale = 1000\\n        if frac_width == 4:\\n            scale = 10000\\n        shown = v * scale // divisor\\n        whole = shown // scale\\n        frac_value = shown % scale\\n        frac = ('.' + str(frac_value).zfill(frac_width)) if frac_width else ''\\n        display = (' ' * sp2_len) + '(' + str(whole) + frac + (' ' * sp3_len) + unit_names[unit_code] + ')'\\n    return str(v) + (' ' * sp1_len) + 'bytes' + display + (' ' * sp4_len) + 'sent'\"\n"
        "  }\n"
        "}\n\n"
        "Executable example 4, common variable raw stream:\n"
        "{\n"
        "  \"tag\": \"client_ipv4\",\n"
        "  \"meaning\": \"recurring_client_ipv4_address\",\n"
        "  \"class\": \"class_3_common_variable\",\n"
        "  \"value_type\": \"string\",\n"
        "  \"target_role\": \"ordinary_value\",\n"
        "  \"context\": null,\n"
        "  \"general\": true,\n"
        "  \"regex\": \"\\\\[client( +)((?:\\\\d{1,3}\\\\.){3}\\\\d{1,3})\\\\]\",\n"
        "  \"replacement\": \"[client{group1}{placeholder}]\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    return {'stored': [groups[1]], 'layout': []}\\n\\ndef inverse(record):\\n    return str(record['stored'][0])\"\n"
        "  }\n"
        "}\n\n"
        "Executable example 5, plain value with an explicit role anchor:\n"
        "{\n"
        "  \"tag\": \"user_name\",\n"
        "  \"meaning\": \"user_field_value\",\n"
        "  \"class\": \"class_3_common_variable\",\n"
        "  \"value_type\": \"string\",\n"
        "  \"target_role\": \"ordinary_value\",\n"
        "  \"context\": null,\n"
        "  \"general\": false,\n"
        "  \"regex\": \"user( +)([A-Za-z][A-Za-z0-9._-]*)\",\n"
        "  \"replacement\": \"user{group1}{placeholder}\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    return {'stored': [groups[1]], 'layout': []}\\n\\ndef inverse(record):\\n    return str(record['stored'][0])\"\n"
        "  }\n"
        "}\n\n"
        if semantic_three_class_program_prompt and strict_three_class_context_schema
        else ""
    )
    strict_three_class_context_text = (
        "Direct per-line context schema:\n"
        "- Every function must include these fields: tag, class, value_type, target_role, context, regex, replacement, program.\n"
        "- class must be one of class_1_composed_numeric, class_2_formatted_numeric, class_3_common_variable.\n"
        "- value_type must be numeric for class 1 and class 2. Use string for class 3 raw variables.\n"
        "- target_role must be log_timestamp only when the stored target itself is the primary timestamp of the log record. Every other stored target, including ports and IP addresses, uses ordinary_value.\n"
        "- Use context=null when no other field in the same physical line should partition the target's distribution. A log_timestamp normally uses context=null.\n"
        "- Otherwise context must be an object with executable code defining project_context(line, groups). line is the complete original physical log line; groups are the target regex capture groups.\n"
        "- project_context must return the actual context value for this concrete line as str or int, or None when absent. Return an IP such as '173.234.31.186', not a label such as 'source_ip'.\n"
        "- The target regex does not need to capture the context. The projector may locate an adjacent or non-adjacent field anywhere in line with a literal safe regex.\n"
        "- Context is routing metadata only. forward(groups) and inverse(record) must still reconstruct the target exactly without context. Numeric targets use delta separately inside each returned context value; string targets use dictionary ranks separately inside each returned context value.\n"
        "- project_context must be deterministic, must not import modules, and may use the provided re.search/re.match/re.fullmatch with a literal regex.\n"
        "- Adjacent example: in \"10.0.0.5:2181\", the port target may return 10.0.0.5 because ports behind the same endpoint host often have a local distribution.\n"
        "- Non-adjacent example: in \"Failed password from 173.234.31.186 port 38926 ssh2\", the port target may return 173.234.31.186 even though words separate the two values.\n\n"
        "Direct context projector example; emit one self-contained target function:\n"
        "{\n"
        "  \"tag\": \"source_port\",\n"
        "  \"meaning\": \"port_number_conditioned_by_source_ip_in_same_line\",\n"
        "  \"class\": \"class_2_formatted_numeric\",\n"
        "  \"value_type\": \"numeric\",\n"
        "  \"target_role\": \"ordinary_value\",\n"
        "  \"context\": {\n"
        "    \"code\": \"def project_context(line, groups):\\n    m = re.search(r'from( +)((?:\\\\d{1,3}\\\\.){3}\\\\d{1,3})', line)\\n    if m:\\n        return m.group(2)\\n    return None\"\n"
        "  },\n"
        "  \"general\": true,\n"
        "  \"regex\": \"port( +)(\\\\d+)\",\n"
        "  \"replacement\": \"port{group1}{placeholder}\",\n"
        "  \"program\": {\n"
        "    \"op\": \"python_exec\",\n"
        "    \"code\": \"def forward(groups):\\n    return {'stored': [int(groups[1])], 'layout': []}\\n\\ndef inverse(record):\\n    return str(int(record['stored'][0]))\"\n"
        "  }\n"
        "}\n\n"
        if semantic_three_class_program_prompt
        else ""
    )
    transducer_sketch_text = (
        "Free-form transducer sketch guidance:\n"
        "- You are not choosing from a fixed type list. You are writing a small reversible parse/render sketch for the value span you decide to extract.\n"
        "- Preferred open form: emit program {\"op\":\"python_exec\", \"code\":\"...\"} when you can express the exact transform as Python forward(groups) and inverse(record).\n"
        "- In python_exec, forward(groups) receives the regex capture groups. If the regex has one captured value, groups[0] is that value. If the regex captures a composed span such as weekday, month, day, time, and year, groups contains those pieces in order.\n"
        "- forward(groups) returns {'stored':[one_integer], 'layout':[]}; inverse(record) receives that integer as record['stored'][0] and returns the exact placeholder text. Use this for timestamp-like, duration-like, unit-like, and compound numeric values that can be rendered exactly.\n"
        "- Do not write import statements. The sandbox already provides datetime, timedelta, re, int, str, len, float, round, abs, min, and max.\n"
        "- The backend accepts python_exec values as integer streams or exact string streams, and it can store small layout byte columns for variable spaces or display widths.\n"
        "- If the captured bytes can be exactly represented by numeric pieces, write python_exec code that parses those pieces, stores one integer, and inverse-renders the exact original bytes.\n"
        "- If the captured bytes are a date/time with month names, weekday text, timezone text, or fractional seconds, write python_exec code that parses the full date/time span and inverse-renders the exact original bytes.\n"
        "- If the captured bytes contain derived text, such as Fri/Sat/Sun implied by a date, keep the whole span in one function and describe the derivation in program parameters rather than creating a separate stream for the derived token.\n"
        "- A simple number is high-value when a stable literal label gives it a role. For example, 'Notification: 7 (component.role)' should store only 7 in a stream named by the surrounding role, not leave 7 for a broad residual integer stream.\n"
        "- A stable key/value phrase such as 'cache.timeout set to 4000' should keep the phrase structure literal and store the changing key or value only when it varies across examples.\n"
        "- Bracketed thread or context labels such as '[worker:host:2181]' may be extracted as one raw context stream if they repeatedly disturb the main template.\n"
        "- If no exact parse/render relation is apparent, make program a raw python_exec string transform and let the codec router choose a backend from the observed values.\n"
        "- Avoid safe-but-weak raw extraction for numeric formats that you can render exactly from captured pieces.\n\n"
        if free_form_program_prompt and free_form_transducer_sketch_prompt
        else ""
    )
    return (
        "You are synthesizing a complete reversible extraction program for one log-line template.\n"
        "The goal is exact-lossless log compression. The decoder will NOT call an LLM; every byte consumed by a regex must be recoverable from constants, preserved groups, or a side stream.\n\n"
        + shape_contract
        + header_prompt_text
        + semantic_numeric_only_text
        + semantic_three_class_text
        + strict_three_class_context_text
        + reversible_transducer_contract
        + semantic_focus_text
        + semantic_rich_examples_text
        + transducer_sketch_text
        +
        "Important: this is whole-line program synthesis, not single-token extraction.\n"
        "- You may combine adjacent tokens into one value if they form one semantic variable, e.g. a timestamp made of date + time tokens.\n"
        "- You may also split one textual region into several side streams when it contains multiple independent variables, e.g. 'REPORT : a b c d'.\n"
        "- You decide which spans should enter side streams. Low-entropy constants should remain as literals in the main template.\n"
        "- A changing span with clear boundaries is usually not a constant, even if it is human-readable message content. Paths, domains, hostnames, endpoints, bracketed identifiers, key=value payloads, unit-bearing values, and stable-affix ids often disturb the main template and should be proposed as reversible side streams instead of left in the template.\n"
        "- Before writing each function, fill source_separation. This is a machine-readable audit, not a long chain-of-thought. Use decision=merge_equivalent only when the captured pieces are one semantic quantity or one piece is exactly derivable from another. Use decision=split_context_value when a textual span contains independent sources and one source predicts/groups the other. Use decision=raw_single_source only for one bounded opaque variable. Use decision=leave_to_residual when the LLM should not create a function.\n"
        "- Use source separation before writing functions: combine adjacent fields only when they are one semantic source or one field is losslessly derived from the other. Split fields with different entropy, update behavior, or prediction context. For example, an IP/host and a port are different sources; a path and a byte count are different sources; a process name and PID are different sources.\n"
        "- Contrastive source-separation example. Bad: store '10.10.34.12:3888' as one raw endpoint string; this repeats the IP many times and mixes address entropy with port entropy. Good: capture IP as context and store only the port as numeric value, with source_separation.decision='split_context_value', stored_source='port', context_source='ip'.\n"
        "- Contrastive source-separation example. Good merge: '1234 bytes (1.20 KB)' has two displays of one byte-size value, so use source_separation.decision='merge_equivalent' and store one canonical integer plus layout/display flags.\n"
        "- Prefer context-scoped functions over broad global regexes. For example, use 'REPORT : (\\d+) (\\d+) ...' rather than '\\b\\d+\\b'.\n"
        "- If a regex consumes several changing subfields and the program stores only one group, the other changing bytes would be unrecoverable. In that case create separate functions, or capture the whole span as one raw reversible stream.\n"
        "- Unit-bearing or display-value variants are high-value Class 1 candidates when they are redundant displays of the same semantic quantity. You may merge '123 bytes' and '123 bytes (1.20 KB)' only if forward stores enough information for inverse to reconstruct the complete original span exactly, including whether the decorated display exists, decimal width, unit spelling, and spacing.\n"
        "- Prefer exact complete spans over scalarization when examples mix plain and decorated spellings. Scalarization is only correct when inverse reconstructs the complete matched span, not merely the first number.\n"
        "- Do not duplicate storage. A changing byte span must be either preserved in the main template or stored in exactly one side stream, never both.\n"
        "- Literal context may be used to locate a value but should remain literal. Because regexes cannot contain bare spaces, for 'Killed process 28473' use regex 'Killed( +)process( +)(\\d+)', replacement 'Killed{group1}process{group2}{placeholder}', and store only group3='28473'. Never output duplicated storage such as 'Killed{group1}process{group2}{group3}{placeholder}'.\n\n"
        + (
            "Slot-complete planning contract:\n"
            "- The required slot table below lists value-like tokens observed in the examples.\n"
            "- Every required slot must appear in slot_actions exactly once, either alone or as part of a composed span such as S0+S1.\n"
            "- A slot action may be KEEP_IN_MAIN, but only when you believe extracting it will not reduce compressed size or it is stable enough as literal structure.\n"
            "- KEEP_IN_MAIN is a high bar, not a safe default. If a slot varies across examples and has punctuation, separators, slashes, dots, brackets, units, digits, or stable key/value context, prefer STREAM_RAW, UINT_DELTA, DICT_RANK, or a composed function unless you can explain why the literal template becomes smaller by keeping it.\n"
            "- Do not mark a changing path, host, endpoint, id, or key=value payload as KEEP_IN_MAIN merely because it is semantically meaningful to humans. Meaningful changing bytes should usually be side-streamed; the MDL/admission stage will reject unhelpful streams.\n"
            "- If several adjacent slots form one value, such as a syslog timestamp or date+time field, use one composed slot action and one composed function.\n"
            "- If a span contains several independent values, such as 'REPORT : a b c d', either create separate functions or one raw tuple stream; do not leave unaccounted digits.\n"
            "- slot_actions are a planning audit. functions are the executable extraction program. The executable functions must still be reversible.\n\n"
            if slot_complete_planner
            else ""
        )
        +
        "Replacement locality contract:\n"
        "- A function rewrites only the substring matched by its own regex, not the whole log line.\n"
        "- The replacement must reconstruct match.group(0) using {placeholder}, preserved local groups, and local literals from the regex match.\n"
        "- Never put unrelated line suffix/prefix text into replacement. For a line 'TS - INFO [X] - msg', a timestamp function must use replacement '{placeholder}', not '{placeholder} - INFO [X] - msg'.\n"
        "- Never reference {groupN} unless group N exists in the same regex and is preserved inside match.group(0).\n"
        "- If a stable context phrase is only used to locate the value, include it inside the regex and replacement only when it is part of the matched substring.\n\n"
        + meaning_contract
        +
        "If a timestamp consists of multiple adjacent tokens, combine them into one timestamp function whenever exact reconstruction is possible.\n\n"
        "Return JSON only, with this schema:\n"
        + json.dumps(schema, indent=2)
        + "\n\n"
        + (
            (
                "Program synthesis guidance:\n"
                "- Do not choose from a named operation list. Write the parse/render idea as a small JSON sketch; the compiler will infer a safe implementation or reject it.\n"
                "- Strongest form: {\"op\":\"python_exec\", \"code\":\"def forward(groups):\\n    text = groups[0]\\n    ...\\n    return {'stored': [integer_value], 'layout': []}\\n\\ndef inverse(record):\\n    v = int(record['stored'][0])\\n    ...\\n    return exact_original_text\"}. This is real Python code, not prose. Do not include import statements.\n"
                "- Raw reversible storage sketch: emit python_exec code that stores the captured bytes exactly as a string; downstream codec chooses rank/delta/raw from those values.\n"
                "- Exact scalar sketch: emit python_exec code. Use it when numeric pieces, units, separators, or fixed-width fields can be rendered byte-exactly.\n"
                "- Date/time sketch: emit python_exec code that parses and renders the complete span. Use it when text such as Fri/Sat/Sun is determined by the captured date/time and timezone/layout.\n"
                "- Unit/display variants such as '123 bytes', '123 bytes (1.20 KB)', '322 KB', '1.5 MB', and '0.01 GB' must be byte-exact. Either store the complete formatted value span as raw reversible storage, or capture every changing subfield and render every spelling exactly.\n"
                "- Variable-looking text is not limited to digits. If a word, dotted key, all-caps state, hex id, bracket label, or short phrase changes inside the same stable left/right literal frame, make a narrow reversible stream for the changing bytes.\n"
                "- Stable-frame examples are patterns such as 'KEY set', 'set to VALUE', 'to VALUE at', 'session ID with', and '[CONTEXT]'. These examples are grammar cues, not field types or dataset rules.\n"
                "- Self-delimiting strings with separators such as '/', '.', ':', '-', '_', '=', '[...]', or '(...)' are strong candidates for raw reversible extraction when they vary across examples. Do not leave them in the main template only because they look like paths, hosts, domains, or ids.\n"
                "- If these sketches are insufficient, propose another deterministic parse/render JSON with enough information to recover the original matched bytes exactly; unsupported sketches will be rejected.\n\n"
            )
            if purpose_only_program_prompt
            else (
                "Program synthesis guidance:\n"
                "- Do not treat the following examples as a closed field-recognizer list. Your job is to describe the simplest deterministic reversible transform for the matched value.\n"
                "- Executable Python example: {\"op\":\"python_exec\", \"code\":\"def forward(groups):\\n    text = groups[0]\\n    dt = datetime.strptime(text, '%Y-%m-%d %H:%M:%S,%f')\\n    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': []}\\n\\ndef inverse(record):\\n    micros = int(record['stored'][0])\\n    dt = datetime.fromtimestamp(micros / 1000000)\\n    return dt.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]\"}. The verifier executes forward and inverse and rejects unsafe or non-exact code.\n"
                "- Raw fallback example: {\"op\":\"python_exec\", \"code\":\"def forward(groups):\\n    return {'stored': [groups[0]], 'layout': []}\\n\\ndef inverse(record):\\n    return str(record['stored'][0])\"} stores the captured value exactly when no compact scalar transform is safe.\n"
                "- Do not emit named built-in datetime, integer, or raw operations. If you want datetime, integer, or raw-string semantics, emit python_exec code that parses and renders explicitly.\n"
                "- If these examples are insufficient, you may propose a new deterministic program JSON with enough parameters to parse and render exactly; unsafe or unsupported programs will be rejected by the verifier/compiler.\n\n"
            )
            if free_form_program_prompt
            else (
                (open_arithmetic_instruction() if open_arithmetic_programs else "")
                + "Executable program forms:\n"
                "- Raw python_exec: {\"op\":\"python_exec\", \"code\":\"def forward(groups):\\n    return {'stored': [groups[0]], 'layout': []}\\n\\ndef inverse(record):\\n    return str(record['stored'][0])\"} stores the captured raw value exactly; safest for paths, users, enums, domains, and compound raw spans.\n"
                "- {\"op\":\"python_exec\", \"code\":\"...\"}: real Python code defining forward(groups) and inverse(record). Use this for exact date/time, unit, duration, tuple, and compound numeric transforms.\n\n"
                "- python_exec code must not import modules. The verifier already provides datetime, re, math, and calendar-like helpers. Any import/from-import statement is rejected.\n"
                "- Use only the verifier-aligned Python subset: ordinary assignments, arithmetic, indexing/slicing, simple if/for/while, int/str/len/float/round/abs/min/max, datetime/timedelta/re helpers, f-strings, lists, tuples, and dicts.\n"
                "- Do not use augmented assignment such as +=, -=, *=, //=, or %=; write ordinary assignment instead. Example: write `v = v // 1000`, not `v //= 1000`.\n"
                "- Do not use comprehensions, lambda, try/except, with, class definitions, file/network/system calls, eval/exec/compile, globals/locals/vars, or double-underscore names.\n"
                "- python_exec forward(groups) must return a dict exactly shaped like {'stored': [one_value], 'layout': [optional_small_integers]}. Use layout for variable spaces or widths needed by inverse(record).\n"
                "- For date/time-like class_1 functions, stored[0] must be an integer timestamp-like scalar. Layout should carry only display details such as variable spaces, day width, or padding needed for exact rendering.\n"
                "- python_exec inverse(record) must read record['stored'] and return the exact original captured bytes as a string.\n"
                "- Do not return datetime objects, ISO strings, tuples, or plain integers from forward(). Do not make inverse(record) expect a datetime object directly.\n\n"
            )
        )
        + useful_examples
        +
        "Hard constraints:\n"
        "- Regex must be Python re compatible and under 220 characters.\n"
        "- Regex pattern strings must contain no literal ASCII space characters. Match every space run with a captured '( +)' group. Preserve captured context spaces in replacement with {groupN}, or store placeholder-internal space lengths in layout.\n"
        "- Replacement must include {placeholder}. It may preserve stable context with {group1}, {group2}, etc.\n"
        "- If {placeholder} stores group N, replacement must not also contain {groupN}. Anchors like 'Killed process' help find the number but are not stored again.\n"
        "- Replacement is local to the regex match. It must not contain the rest of the line, a concrete timestamp from an example, or unrelated constants outside match.group(0).\n"
        "- A timestamp regex should usually have replacement '{placeholder}' and should store all consumed timestamp spacing in layout. An endpoint regex like 'client( +)(/IP:PORT)' should usually have replacement 'client{group1}{placeholder}' only if 'client' and the captured space are inside the regex match.\n"
        "- Avoid broad tail captures like (.+) or (.*). Avoid a bare '\\b\\d+\\b' unless it is tightly scoped by literal context.\n"
        "- Do not leave changing self-delimiting spans in the template by default. If a path, host, endpoint, dotted identifier, bracketed id, or key=value payload changes inside a stable local frame, propose a narrow reversible function and let verifier/MDL decide admission.\n"
        "- If a value appears with optional units, parentheses, or sibling units such as KB/MB/GB, generate a reversible variant-aware function: either store the complete formatted value span, or capture and render all pieces exactly. Never consume an optional suffix that is not preserved or rendered.\n"
        "- If functions overlap, order most specific first, left-to-right where possible.\n"
        "- Do not hardcode one example's changing values as literals.\n"
        + (
            "- Prefer complete coverage of value-like slots; if a slot is not extracted, mark it KEEP_IN_MAIN in slot_actions with a compression reason.\n\n"
            if slot_complete_planner
            else "- Prefer a small number of high-value functions over extracting every minor token.\n\n"
        )
        +
        f"Dataset: {dataset}\n"
        f"Template anchor key: {' '.join(family.key)}\n"
        "Examples and token shapes:\n"
        + example_token_table(
            examples,
            max_examples=max_examples,
            semantic_token_shapes=semantic_token_shapes,
        )
        + (
            "\n\nRequired value-like slots to account for "
            f"({required_slot_count} sampled occurrences):\n"
            + required_slots_text
            + "\n"
            if slot_complete_planner
            else ""
        )
        + "\n"
    )


def build_prompt(
    family: Family,
    dataset: str,
    max_examples: int,
    whole_line_program_cache: bool = False,
    open_arithmetic_programs: bool = False,
    slot_complete_planner: bool = False,
    generic_token_shapes: bool = False,
    function_first_prompt: bool = False,
    free_form_program_prompt: bool = False,
    free_form_transducer_sketch_prompt: bool = False,
    purpose_only_program_prompt: bool = False,
    semantic_focus_program_prompt: bool = False,
    semantic_rich_examples_prompt: bool = False,
    family_header_program_prompt: bool = False,
    semantic_numeric_only_program_prompt: bool = False,
    semantic_three_class_program_prompt: bool = False,
    strict_three_class_context_schema: bool = False,
) -> str:
    if whole_line_program_cache:
        return build_whole_line_prompt(
            family,
            dataset,
            max_examples,
            open_arithmetic_programs=open_arithmetic_programs,
            slot_complete_planner=slot_complete_planner,
            generic_token_shapes=generic_token_shapes,
            function_first_prompt=function_first_prompt,
            free_form_program_prompt=free_form_program_prompt,
            free_form_transducer_sketch_prompt=free_form_transducer_sketch_prompt,
            purpose_only_program_prompt=purpose_only_program_prompt,
            semantic_focus_program_prompt=semantic_focus_program_prompt,
            semantic_rich_examples_prompt=semantic_rich_examples_prompt,
            family_header_program_prompt=family_header_program_prompt,
            semantic_numeric_only_program_prompt=semantic_numeric_only_program_prompt,
            semantic_three_class_program_prompt=semantic_three_class_program_prompt,
            strict_three_class_context_schema=strict_three_class_context_schema,
        )
    examples = family.examples[:max_examples]
    if purpose_only_program_prompt:
        program_schema: dict[str, object] = {
            "intent": "what information is stored and how exact rendering is recovered",
            "parse": "optional: how to parse the captured bytes",
            "stored_value": "raw bytes, exact scalar, tuple, or value derived from another captured value",
            "render": "optional: exact inverse rendering rule",
        }
    elif free_form_program_prompt:
        program_schema = {
            "intent": "reversible transform program; examples include raw stream, datetime-derived text, or integer packing",
            "op": "your deterministic operation name",
            "parameters": {},
        }
    else:
        program_schema = {"op": "auto_codec"}
    schema = {
        "family_analysis": {
            "has_composable_tokens": True,
            "composable_groups": [
                {
                    "name": "timestamp_or_other_composed_value",
                    "tokens": ["weekday", "month", "day", "time"],
                    "reason": "several tokens together represent one value",
                    "target_value": "one integer-like value",
                    "must_emit_function": True,
                }
            ],
            "has_semantic_equivalent_tokens": True,
            "semantic_equivalent_groups": [
                {
                    "name": "same_value_multiple_displays",
                    "tokens": ["1234 bytes", "1.20 KB"],
                    "reason": "different displays of the same underlying value",
                    "target_value": "one canonical integer-like value",
                    "must_emit_function": True,
                }
            ],
            "raw_variable_groups": [
                {
                    "name": "intrinsically_structured_variable",
                    "tokens": ["10.0.0.5", "/var/log/app", "api.example.net", "550e8400-e29b-41d4-a716-446655440000"],
                    "reason": "the captured value itself contains at least two explicit structural separators encoded by the regex",
                    "must_emit_function": True,
                },
                {
                    "name": "explicitly_role_anchored_plain_variable",
                    "tokens": ["user Alice", "username=Alice"],
                    "reason": "a plain value is immediately introduced by a stable field-name literal encoded by the regex; whitespace or a generic preposition is not enough",
                    "must_emit_function": True,
                }
            ],
        },
        "header": {
            "has_unified_header": True,
            "reason": "whether a repeated line prefix should be extracted as header side streams",
            "functions": [
                {
                    "tag": "H0",
                    "meaning": "header field",
                    "class": "class_1_composed_numeric | class_2_formatted_numeric | class_3_common_variable",
                    "value_type": "numeric for class_1/class_2, string for class_3",
                    "target_role": "log_timestamp | ordinary_value",
                    "context": None,
                    "source_separation": {
                        "decision": "merge_equivalent | split_context_value | raw_single_source | leave_to_residual",
                        "stored_source": "what this function stores",
                        "context_source": "what groups the stored value, or null",
                        "why_not_merge_raw": "why independent entropy sources are not stored as one raw string",
                    },
                    "regex": "Python re regex for one complete header field or composed header span",
                    "replacement": "local replacement containing {placeholder} and optional {group1}",
                    "program": program_schema,
                }
            ],
        },
        "functions": [
            {
                "tag": "T",
                "meaning": "short semantic name",
                "class": "class_1_composed_numeric | class_2_formatted_numeric | class_3_common_variable",
                "value_type": "numeric for class_1/class_2, string for class_3",
                "target_role": "log_timestamp | ordinary_value",
                "context": None,
                "source_separation": {
                    "decision": "merge_equivalent | split_context_value | raw_single_source | leave_to_residual",
                    "stored_source": "what this function stores",
                    "context_source": "what groups the stored value, or null",
                    "why_not_merge_raw": "why independent entropy sources are not stored as one raw string",
                },
                "regex": "Python re regex for one value span or one narrow context span",
                "replacement": "literal replacement containing {placeholder} and optional {group1}",
                "program": program_schema,
            }
        ]
    }
    return (
        f"You are generating ordered reversible extraction functions for one {dataset} log-line family.\n"
        "The compressor is streaming: if this family is seen again, the functions will be executed in JSON order.\n"
        "The decoder never calls an LLM; every extracted value must be recoverable from side streams.\n\n"
        "First fill family_analysis before writing functions. This is a short machine-readable plan, not chain-of-thought.\n"
        "- has_composable_tokens means several tokens/pieces jointly represent one value, such as weekday + month + day + time + year forming one timestamp-like value.\n"
        "- has_semantic_equivalent_tokens means two displays represent the same value, such as '1234 bytes' and '(1.20 KB)' describing the same byte count.\n"
        "- If has_composable_tokens=true, at least one function should implement the best composable_group unless exact reconstruction is impossible.\n"
        "- If has_semantic_equivalent_tokens=true, at least one function should implement the best semantic_equivalent_group unless exact reconstruction is impossible.\n"
        "- raw_variable_groups are Class 3 candidates through either of two evidence routes: (A) the captured value itself contains at least two non-alphanumeric separator occurrences explicitly encoded by the regex; or (B) a plain value is immediately introduced by a stable, unambiguous field-name literal explicitly present in the regex, such as user( +)([A-Za-z0-9]+) or username=([A-Za-z0-9]+). Whitespace alone and generic words such as for, from, to, or by do not qualify. Unanchored broad captures such as ([A-Za-z0-9]+), (\\w+), or ([^ ]+) must be left to residual.\n"
        "- This planning step is meant to prevent skipping high-value fusion opportunities before regex writing.\n\n"
        + (
            "First decide whether this family has a unified log header. A header is a repeated prefix containing time, level, component, thread, process, host, or context fields before the free-form message. It may use punctuation or brackets rather than spaces. Put executable header extraction functions in header.functions. Use python_exec code for both compact exact parse/render transforms and raw exact string storage.\n\n"
            if family_header_program_prompt
            else ""
        )
        + "Return JSON only, with this schema:\n"
        + json.dumps(schema, indent=2)
        + "\n\n"
        + (
            (
                "Program synthesis guidance:\n"
                "- Do not choose from a named operation list. Prefer real python_exec code that defines forward(groups) and inverse(record); unsupported sketches are rejected.\n"
                "- Raw reversible storage sketch: {\"intent\":\"store the captured bytes exactly; downstream codec chooses rank/delta/raw\"}.\n"
                "- Exact scalar sketch: use python_exec code when numeric pieces, units, separators, or fixed-width fields can be rendered byte-exactly.\n"
                "- Date/time sketch: use python_exec code when text such as Fri/Sat/Sun is determined by the captured date/time and timezone/layout.\n"
                "- Variable-looking text is not limited to digits. If a word, dotted key, all-caps state, hex id, bracket label, or short phrase changes inside the same stable left/right literal frame, make a narrow reversible stream for the changing bytes.\n"
                "- Stable-frame examples are patterns such as 'KEY set', 'set to VALUE', 'to VALUE at', 'session ID with', and '[CONTEXT]'. These examples are grammar cues, not field types or dataset rules.\n"
                "- If these sketches are insufficient, propose another deterministic parse/render JSON with enough information to recover the original matched bytes exactly.\n\n"
            )
            if purpose_only_program_prompt
            else (
                "Program synthesis guidance:\n"
                "- Do not treat the following examples as a closed field-recognizer list. Your job is to describe the simplest deterministic reversible transform for the matched value.\n"
                "- Raw-string example: {\"op\":\"python_exec\", \"code\":\"def forward(groups):\\n    return {'stored': [groups[0]], 'layout': []}\\n\\ndef inverse(record):\\n    return str(record['stored'][0])\"} stores the captured value exactly and lets the downstream codec choose rank/delta/raw.\n"
                "- Datetime-code example with one captured value: {\"op\":\"python_exec\", \"code\":\"def forward(groups):\\n    text = groups[0]\\n    dt = datetime.strptime(text, '%a %b %d %H:%M:%S %Y')\\n    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': []}\\n\\ndef inverse(record):\\n    micros = int(record['stored'][0])\\n    dt = datetime.fromtimestamp(micros / 1000000)\\n    return dt.strftime('%a %b %d %H:%M:%S %Y')\"}. This stores one timestamp-like scalar; weekday text is derived during rendering.\n"
                "- Datetime-code example with several captured groups: if regex captures weekday, month, day, time, and year separately, forward(groups) may read weekday, month, day, time, year = groups and pack them into one timestamp integer; inverse(record) renders the exact placeholder text.\n"
                "- Arithmetic-code example: {\"op\":\"python_exec\", \"code\":\"def forward(groups):\\n    text = groups[0]\\n    m = re.fullmatch(r'(\\\\d+)\\\\.(\\\\d+)KB', text)\\n    v = int(m.group(1)) * 1000 + int(m.group(2))\\n    return {'stored': [v], 'layout': []}\\n\\ndef inverse(record):\\n    v = int(record['stored'][0])\\n    return f'{v // 1000}.{v % 1000:03d}KB'\"}. Use this style only when inverse is byte-exact for the examples.\n"
                "- Variant example: if examples include both '123 bytes sent' and '123 bytes (1.20 KB) sent', do not consume '(1.20 KB)' unless it is stored or rendered. Prefer storing the complete value span '123 bytes (1.20 KB)' as the placeholder, or provide a parse/render program that reconstructs the optional display exactly.\n"
                "- If these examples are insufficient, you may propose a new deterministic program JSON with enough parameters to parse and render exactly; unsafe or unsupported programs will be rejected by the verifier/compiler.\n\n"
            )
            if free_form_program_prompt
            else (
                (open_arithmetic_instruction() if open_arithmetic_programs else "")
                + "Executable program forms:\n"
                "- Raw python_exec: safest default for opaque bytes; stores the captured raw value exactly and lets the codec selector choose rank/delta/raw.\n"
                "- {\"op\":\"python_exec\", \"code\":\"...\"}: real Python code defining forward(groups) and inverse(record). Use this for exact date/time, unit, duration, tuple, and compound numeric transforms.\n\n"
            )
        )
        + (
            "Free-form transducer sketch guidance:\n"
            "- You are not choosing from a fixed type list. You are writing a reversible parse/render sketch for the value span.\n"
        "- If the value can be exactly represented by numeric pieces, write python_exec code that parses those pieces, stores one integer, and inverse-renders exactly.\n"
        "- If the value is date/time-like with month names, weekday text, timezone text, or fractional seconds, write python_exec code that parses the full span and inverse-renders exactly.\n"
        "- If text such as Fri/Sat/Sun is derivable from a captured date/time, keep one composed function and describe the derivation rather than creating a separate weekday stream.\n"
        "- If a stable literal label gives a simple number a role, such as 'Notification: 7 (component.role)', store only the changing number in a role-specific stream instead of leaving it to a broad residual integer stream.\n"
        "- For stable key/value phrases such as 'cache.timeout set to 4000', keep stable words as literals and store only the key or value when it varies across examples.\n"
        "- Bracketed thread/context labels such as '[worker:host:2181]' may be extracted as one raw context stream if they repeatedly disturb the main template.\n"
        "- If no exact parse/render relation is apparent, use raw python_exec string storage.\n\n"
            if free_form_program_prompt and free_form_transducer_sketch_prompt
            else ""
        )
        + (
            "Open semantic-program objective:\n"
            "- Focus on spans whose format or meaning is hard for a generic residual coder: multi-token values, strings with embedded numbers/separators, stable-affix identifiers, unit-bearing values, and values derivable from another captured value.\n"
            "- A plain isolated integer usually does not need a custom LLM function unless its context gives it a stable role or it participates in a larger reversible transform.\n"
            "- If weekday text is determined by a nearby date/time value and timezone, store the date/time relation and derive weekday during rendering instead of creating a weekday stream.\n"
            "- If no exact deterministic parse/render relation is apparent, use raw python_exec string storage.\n\n"
            if semantic_focus_program_prompt
            else ""
        )
        + (
            "Illustrative sketches only, not an allowed-type list:\n"
            "- A weekday token may be derived from a captured date/time span and timezone during rendering.\n"
            "- A unit-bearing numeric string may be transformed into an exact integer-like scalar and rendered back byte-exactly.\n"
            "- A stable-affix identifier may use context to locate the changing id while storing only the changing part or an exact transformed scalar.\n\n"
            if semantic_focus_program_prompt and semantic_rich_examples_prompt
            else ""
        )
        + "Hard constraints:\n"
        + (
            "- Do not spend function budget on a broad all-digit regex when a simple residual numeric coder can handle the leftover number. Prefer complete formatted spans and semantic dependencies.\n"
            if semantic_focus_program_prompt
            else "- Use python_exec only when every changing byte consumed by the regex is parsed and rendered exactly. If a numeric-looking value has unit text, optional display text, parentheses, alternate units, or any sibling subfield, prefer one complete formatted span with raw python_exec unless you can render all variants byte-exactly.\n"
        )
        + "- Regex must be Python re compatible and under 220 characters.\n"
        "- Regex pattern strings must contain no literal ASCII space characters. Match every space run with a captured '( +)' group. Preserve captured context spaces in replacement with {groupN}, or store placeholder-internal space lengths in layout.\n"
        "- Replacement must include {placeholder}. It may also preserve captured context with {group1}, {group2}, etc.\n"
        "- Do not duplicate storage. If {placeholder} stores group N, replacement must not also contain {groupN}. For 'Killed process 28473', use regex 'Killed( +)process( +)(\\d+)' and replacement 'Killed{group1}process{group2}{placeholder}', not a replacement that repeats the stored number.\n"
        "- Replacement is local to the regex match. It must not contain the rest of the line, a concrete timestamp from an example, or unrelated constants outside match.group(0).\n"
        "- A timestamp regex should usually have replacement '{placeholder}' and should store all consumed timestamp spacing in layout. An endpoint regex like 'client( +)(/IP:PORT)' should usually have replacement 'client{group1}{placeholder}' only if 'client' and the captured space are inside the regex match.\n"
        "- Do not use regex backreference syntax in replacement. Use {group1} syntax.\n"
        "- Do not consume changing bytes unless they are either the placeholder value or preserved as {groupN}.\n"
        "- Optional unit displays, parentheses, sibling units such as KB/MB/GB, and spaces are changing bytes too. Bad: regex '(\\d+) bytes(?: \\([^)]+\\))? sent' with replacement '{placeholder} bytes sent'. Good: regex '(( +))(\\d+)( +)bytes(( +)\\([^)]+\\))?(( +)sent)' with replacement '{group1}{placeholder}{group4}bytes{group5}{group7}', or an exact parse/render program for all variants.\n"
        "- Avoid broad tail captures like (.+) or (.*).\n"
        + (
            "- Good targets are complete formatted spans, repeated high-entropy identifiers, and fields whose text can be deterministically derived from another extracted value.\n"
            if semantic_focus_program_prompt
            else "- Good Apache targets: bracketed timestamp, log level, client IPv4 after [client ...], absolute paths, child ids, scoreboard slots, numeric counts.\n"
        )
        + "- If functions overlap, order from most specific to most general.\n\n"
        f"Dataset: {dataset}\n"
        f"Template anchor key: {' '.join(family.key)}\n"
        "Examples:\n"
        + "\n".join(f"- {example.rstrip()}" for example in examples)
        + "\n"
    )


def build_family_batch_prompt(
    family_examples: list[tuple[Family, list[str]]],
    dataset: str,
    max_examples: int,
    open_arithmetic_programs: bool = False,
    slot_complete_planner: bool = False,
    generic_token_shapes: bool = False,
    function_first_prompt: bool = False,
    family_header_program_prompt: bool = False,
    semantic_three_class_program_prompt: bool = False,
    strict_three_class_context_schema: bool = False,
) -> str:
    """Build one API prompt for multiple offline-selected families.

    The output is a map from family_id to the same per-family program shape used
    by compile_family_specs().  Verification remains per-family after the batch
    response is parsed, so a bad family proposal cannot silently affect another
    family.
    """
    program_schema = {
        "op": "python_exec",
        "code": "required real Python code defining both forward(groups) and inverse(record)",
    }
    family_schema = {
        "family_id": 0,
        "family_analysis": {
            "has_composable_tokens": True,
            "composable_groups": [
                {
                    "name": "timestamp_or_other_composed_value",
                    "tokens": ["weekday", "month", "day", "time"],
                    "reason": "several tokens together represent one value",
                    "target_value": "one integer-like value",
                    "must_emit_function": True,
                }
            ],
            "has_semantic_equivalent_tokens": True,
            "semantic_equivalent_groups": [
                {
                    "name": "same_value_multiple_displays",
                    "tokens": ["1234 bytes", "1.20 KB"],
                    "reason": "different displays of the same underlying value",
                    "target_value": "one canonical integer-like value",
                    "must_emit_function": True,
                }
            ],
            "raw_variable_groups": [
                {
                    "name": "intrinsically_structured_variable",
                    "tokens": ["10.0.0.5", "/var/log/app", "api.example.net", "550e8400-e29b-41d4-a716-446655440000"],
                    "reason": "the captured value itself contains at least two explicit structural separators encoded by the regex",
                    "must_emit_function": True,
                },
                {
                    "name": "explicitly_role_anchored_plain_variable",
                    "tokens": ["user Alice", "username=Alice"],
                    "reason": "a plain value is immediately introduced by a stable field-name literal encoded by the regex; whitespace or a generic preposition is not enough",
                    "must_emit_function": True,
                }
            ],
        },
        "header": {
            "has_unified_header": True,
            "reason": "whether this family has a repeated prefix/header",
            "functions": [
                {
                    "tag": "H0",
                    "meaning": "header field",
                    "class": "class_1_composed_numeric | class_2_formatted_numeric | class_3_common_variable",
                    "value_type": "numeric for class_1/class_2, string for class_3",
                    "target_role": "log_timestamp | ordinary_value",
                    "context": None,
                    "source_separation": {
                        "decision": "merge_equivalent | split_context_value | raw_single_source | leave_to_residual",
                        "stored_source": "what this function stores",
                        "context_source": "what groups the stored value, or null",
                        "why_not_merge_raw": "why independent entropy sources are not stored as one raw string",
                    },
                    "general": True,
                    "order_hint": "global_composed | family_structured | global_atomic",
                    "regex": "Python re regex scoped to this family",
                    "replacement": "local replacement containing {placeholder} and optional {group1}",
                    "program": program_schema,
                }
            ],
        },
        "slot_actions": [
            {
                "slot": "S0 or S0+S1",
                "action": "COMPOSE_FIELDS_TO_INT | STREAM_RAW | UINT_DELTA | DICT_RANK | KEEP_IN_MAIN",
                "meaning": "semantic role if known",
                "reason": "why this slot is extracted, composed, or kept",
            }
        ],
        "functions": [
            {
                "tag": "T",
                "meaning": "short semantic name",
                "class": "class_1_composed_numeric | class_2_formatted_numeric | class_3_common_variable",
                "value_type": "numeric for class_1/class_2, string for class_3",
                "target_role": "log_timestamp | ordinary_value",
                "context": None,
                "source_separation": {
                    "decision": "merge_equivalent | split_context_value | raw_single_source | leave_to_residual",
                    "stored_source": "what this function stores",
                    "context_source": "what groups the stored value, or null",
                    "why_not_merge_raw": "why independent entropy sources are not stored as one raw string",
                },
                "general": True,
                "order_hint": "global_composed | family_structured | global_atomic",
                "regex": "Python re regex for one value span or narrow context span",
                "replacement": "literal replacement containing {placeholder} and optional {group1}",
                "program": program_schema,
            }
        ],
    }
    function_template = family_schema.pop("functions")[0]
    family_schema["header"].pop("functions", None)
    family_schema["class_inventory"] = {
        "class_1_composed_numeric": ["semantic names found in this family, or an empty list"],
        "class_2_formatted_numeric": ["semantic names found in this family, or an empty list"],
        "class_3_common_variable": ["semantic names found in this family, or an empty list"],
    }
    family_schema["class_1_functions"] = [
        {
            **function_template,
            "class": "class_1_composed_numeric",
            "value_type": "numeric",
        }
    ]
    family_schema["class_2_functions"] = [
        {
            **function_template,
            "class": "class_2_formatted_numeric",
            "value_type": "numeric",
        }
    ]
    family_schema["class_3_functions"] = [
        {
            **function_template,
            "class": "class_3_common_variable",
            "value_type": "string",
        }
    ]
    requested_family_ids = [family.family_id for family, _examples in family_examples]
    parts = [
        f"You are generating reversible extraction programs for several {dataset} log-line families in one batch.",
        "Return JSON only. The top-level object must be {\"families\": [...]}.",
        f"Return exactly {len(requested_family_ids)} family objects, one for each input family_id in this exact set: {requested_family_ids}.",
        "Do not omit a family. If a family has no safe function, still return it with all three class function arrays empty.",
        "Each output family must preserve its input family_id exactly and follow this schema:",
        json.dumps({"families": [family_schema]}, indent=2),
        "",
        "Class-wise completion contract:",
        "- Inspect Class 1, Class 2, and Class 3 independently for every family before emitting JSON.",
        "- class_inventory must contain all three class keys and briefly name every candidate category found; use [] when none is present.",
        "- Put executable proposals only in class_1_functions, class_2_functions, or class_3_functions. Always emit all three arrays, even when empty.",
        "- Do not emit a flat functions array or header.functions. A changing header field belongs in the class array matching its semantics.",
        "- The class bucket is authoritative: Class 1 and Class 2 store numeric values; Class 3 stores exact strings.",
        "",
        "For every family, fill family_analysis before writing functions. This is a short machine-readable plan, not chain-of-thought.",
        "- has_composable_tokens means several tokens/pieces jointly represent one value, such as weekday + month + day + time + year forming one timestamp-like value.",
        "- has_semantic_equivalent_tokens means two displays represent the same value, such as '1234 bytes' and '(1.20 KB)' describing the same byte count.",
        "- If has_composable_tokens=true, at least one function should implement the best composable_group unless exact reconstruction is impossible.",
        "- If has_semantic_equivalent_tokens=true, at least one function should implement the best semantic_equivalent_group unless exact reconstruction is impossible.",
        "- raw_variable_groups are Class 3 candidates through either of two evidence routes: (A) the captured value itself contains at least two non-alphanumeric separator occurrences explicitly encoded by the regex; or (B) a plain value is immediately introduced by a stable, unambiguous field-name literal explicitly present in the regex, such as user( +)([A-Za-z0-9]+) or username=([A-Za-z0-9]+). Whitespace alone and generic words such as for, from, to, or by do not qualify. Unanchored broad captures such as ([A-Za-z0-9]+), (\\w+), or ([^ ]+) must be left to residual.",
        "- This planning step is meant to prevent skipping high-value fusion opportunities before regex writing.",
        "",
        *(
            [
                "Direct per-line context schema:",
                "- Every function must include tag, class, value_type, target_role, context, regex, replacement, and program.",
                "- class must be one of class_1_composed_numeric, class_2_formatted_numeric, class_3_common_variable.",
                "- value_type must be numeric for class_1_composed_numeric and class_2_formatted_numeric. Use string for class_3_common_variable.",
                "- target_role must be log_timestamp only when the stored target itself is the primary timestamp of the log record. Use ordinary_value for every other stored target.",
                "- Use context=null when no other field in the same physical line should partition the target's distribution. A log_timestamp normally uses context=null.",
                "- Otherwise context must contain code defining project_context(line, groups). It returns the actual per-line context value as str/int, or None when absent; it must not return a semantic label.",
                "- line is the complete original physical log line and groups are the target regex groups. The target regex does not need to capture the context, and the context may be adjacent or non-adjacent.",
                "- Context only routes an already reversible target. forward(groups) and inverse(record) must reconstruct the target without context. Numeric streams use delta within each returned context; string streams use dictionary ranks within each returned context.",
                "- project_context must be deterministic, must not import modules, and may use provided re.search/re.match/re.fullmatch with literal safe regexes.",
                "- Direct projector example for 'Failed password from 173.234.31.186 port 38926 ssh2': the source_port target regex is 'port( +)(\\\\d+)', its target program stores int(groups[1]), and context.code is: def project_context(line, groups): m = re.search(r'from( +)((?:\\\\d{1,3}\\\\.){3}\\\\d{1,3})', line); return m.group(2) if m else None.",
                "- The example returns the concrete IP bytes, not the words source_ip and not another function tag.",
                "- Before writing each function, fill source_separation. This is a machine-readable audit, not a long chain-of-thought. Use decision=merge_equivalent only when the captured pieces are one semantic quantity or one piece is exactly derivable from another. Use decision=split_context_value when a textual span contains independent sources and one source predicts/groups the other. Use decision=raw_single_source only for one bounded opaque variable. Use decision=leave_to_residual when the LLM should not create a function.",
                "- If a target's distribution depends on another field anywhere in the same line, attach a projector to the target. Example: source_port stores only the port and project_context returns the concrete source IP from the complete line.",
                "- Contrastive source-separation example. Bad: store '10.10.34.12:3888' as one raw endpoint string; this repeats the IP many times and mixes address entropy with port entropy. Good: capture IP as context and store only the port as numeric value, with source_separation.decision='split_context_value', stored_source='port', context_source='ip'.",
                "- Contrastive source-separation example. Good merge: '1234 bytes (1.20 KB)' has two displays of one byte-size value, so use source_separation.decision='merge_equivalent' and store one canonical integer plus layout/display flags.",
                "",
            ]
            if semantic_three_class_program_prompt and strict_three_class_context_schema
            else []
        ),
        "Executable program forms:",
        "- Raw python_exec: store the captured raw value exactly as a string; downstream codec chooses rank/delta/raw.",
        "- {\"op\":\"python_exec\", \"code\":\"...\"}: real Python code defining forward(groups) and inverse(record). Use this for exact date/time, unit, duration, tuple, and compound numeric transforms.",
        "- python_exec code must not import modules. The verifier already provides datetime, re, math, and calendar-like helpers. Any import/from-import statement is rejected.",
        "- Use only the verifier-aligned Python subset: ordinary assignments, arithmetic, indexing/slicing, simple if/for/while, int/str/len/float/round/abs/min/max, datetime/timedelta/re helpers, f-strings, lists, tuples, and dicts.",
        "- Do not use augmented assignment such as +=, -=, *=, //=, or %=; write ordinary assignment instead. Example: write `v = v // 1000`, not `v //= 1000`.",
        "- Do not use comprehensions, lambda, try/except, with, class definitions, file/network/system calls, eval/exec/compile, globals/locals/vars, or double-underscore names.",
        "- python_exec forward(groups) must return a dict exactly shaped like {'stored': [one_value], 'layout': [optional_small_integers]}. Use layout for variable spaces or widths needed by inverse(record).",
        "- python_exec inverse(record) must read record['stored'] and return the exact original captured bytes as a string.",
        "- Do not return datetime objects, ISO strings, tuples, or plain integers from forward(). Do not make inverse(record) expect a datetime object directly.",
        "",
        "Placeholder routing contract:",
        "- Use exactly one {placeholder} in every replacement. The decoder uses that route to choose this function's side stream.",
        "- If a function consumes several capture groups that together describe one attribute, they still map to one {placeholder}, not multiple placeholders.",
        "- If a span contains independent attributes, emit separate functions with separate tags.",
        "- Do not duplicate storage: bytes consumed into {placeholder} must not also remain as {groupN}.",
        "",
        "For each family:",
        "- Every function must include class and target_role. Use class_1_composed_numeric for multi-piece values that become one compressible semantic integer, class_2_formatted_numeric for text or formatted tokens that express numeric meaning and can be converted into one compressible integer, and class_3_common_variable for an exact string supported by either intrinsic structure or an explicit adjacent field-name anchor. Use target_role=log_timestamp only for the primary log timestamp; otherwise use ordinary_value.",
        "- If a date/time-like span is present, prefer class_1_composed_numeric and write python_exec that stores one integer timestamp-like value. Do not use class_3_common_variable or raw identity for a timestamp merely because spaces or display widths vary; store those reversible display details in layout.",
        "- Choose class_2 not only for visibly numeric tokens, but for non-numeric or formatted text that carries numeric meaning and has a deterministic numeric representation: units, durations, byte-size displays, ordinal/state counters, severity-like levels, port-like roles, process/thread ids, slots, or structured subfields. Do not classify random-looking session ids, hashes, request ids, or opaque hex strings as class_1/class_2 merely because they can be parsed as integers.",
        "- Strategy switch for numbers: leave anonymous/simple numbers to deterministic residual coders, but extract semantically stable numbers whose surrounding words vary across templates. A port near an IP/host should be a class_2 target whose context projector returns the concrete IP/host when that field predicts the port distribution.",
        "- For every ordinary_value target, ask whether another field anywhere in the same physical line predicts or partitions its distribution. If yes, provide context.code defining project_context(line, groups); otherwise use context=null.",
        "- project_context returns the actual value found on this line, not a field name or another function tag. A log_timestamp normally uses context=null.",
        "- Context routing is available to every class. Numeric targets use context-local delta and string targets use context-local dictionary ranks, while target reconstruction remains independent of context.",
        "- Prefer class_3_common_variable only after one evidence route passes. Intrinsic route: the captured value has at least two separator occurrences explicit in the regex. Role-anchor route: a plain value is immediately introduced by a stable, unambiguous field-name literal explicit in the regex, such as user or username=. A semantic explanation, whitespace, or a generic word such as for/from/to/by is insufficient. Numeric-looking opaque identifiers may use class_3 only when they satisfy one route and lack semantic magnitude/order.",
        "- First decide whether there is a unified log header. Headers can use brackets, punctuation, or component fields, not just whitespace.",
        "- Put changing header fields in header.functions.",
        "- Then propose normal value functions for hard formats such as session ids, stable-affix ids, derived date/time text, unit-bearing values, and compound numeric fields.",
        "- Prefer one reversible function for a complete real-world attribute over several substring functions. If adjacent tokens or neighboring pieces describe the same underlying value, capture the complete span and store one compact value when exact rendering is possible.",
        "- Do not split one attribute into independent streams just because it appears as several tokens or contains punctuation. A date/time-like attribute, unit-bearing display, bracketed identifier, endpoint-like text, or stable-affix id should be considered as a complete reversible span first.",
        "- If one displayed piece is derivable from another captured value, store the source value and render the derived text instead of creating a separate stream. If exact derivation is not safe, store the complete formatted span with raw python_exec.",
        "- Prefer executable python_exec for complete composed attributes when a deterministic parse/render relation is clear. Use raw python_exec when the exact bytes are opaque or when a compact inverse renderer is not safe.",
        "- Set general=true when the function likely applies across many families/templates, such as a repeated header attribute, address-like value, endpoint-like value, bracketed process/context id, port-like number, user/domain-like token, or other recurring value format. Set general=false for message-specific variables.",
        "- Set order_hint=global_composed for a global function that consumes a complete multi-piece attribute or starts at a repeated line prefix; set family_structured for context-scoped message variables; set global_atomic for simple intrinsic global values.",
        "- Constants remain literal. Store changing bytes once: either as {placeholder} or as preserved {groupN}, never both.",
        "- Unit-bearing variants must be byte-exact. If examples include both plain and displayed forms such as '123 bytes' and '123 bytes (1.20 KB)', do not consume the optional display and drop it from replacement. Store the complete formatted value span or render all captured pieces exactly.",
        "- Use numeric scalar programs only after checking that the scalar render reconstructs the whole matched value, including optional unit displays and alternate units.",
        "- Regexes must be Python re compatible, local to the matched substring, and under 220 characters.",
        "- Avoid broad tail captures like (.+) or (.*). Avoid bare all-number regexes unless tightly scoped by literals. For Class 3, forbid unanchored plain-token captures such as ([A-Za-z0-9]+), (\\w+), and ([^ ]+). A narrow plain-token capture is allowed only when a stable, unambiguous field-name literal immediately introduces it in the same regex; whitespace or generic for/from/to/by does not qualify.",
        "- If functions overlap, order most specific first.",
        "",
        "Executable examples, for style only and not a closed type list:",
        "Example exact raw value:",
        json.dumps(
            {
                "tag": "dot_value",
                "meaning": "recurring dotted value",
                "class": "class_3_common_variable",
                "general": True,
                "order_hint": "global_atomic",
                "regex": r"from ((?:\d{1,3}\.){3}\d{1,3})",
                "replacement": "from {placeholder}",
                "program": {
                    "op": "python_exec",
                    "code": "def forward(groups):\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])",
                },
            },
            ensure_ascii=False,
        ),
        "Example exact raw plain value with an explicit role anchor:",
        json.dumps(
            {
                "tag": "user_name",
                "meaning": "user_field_value",
                "class": "class_3_common_variable",
                "general": False,
                "order_hint": "family_structured",
                "regex": r"user( +)([A-Za-z][A-Za-z0-9._-]*)",
                "replacement": "user{group1}{placeholder}",
                "program": {
                    "op": "python_exec",
                    "code": "def forward(groups):\n    return {'stored': [groups[1]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])",
                },
            },
            ensure_ascii=False,
        ),
        "Example fixed-width integer:",
        json.dumps(
            {
                "tag": "bracket_number",
                "meaning": "recurring bracketed numeric id",
                "class": "class_2_formatted_numeric",
                "general": True,
                "order_hint": "global_composed",
                "regex": r"worker\[(\d{5})\]",
                "replacement": "worker[{placeholder}]",
                "program": {
                    "op": "python_exec",
                    "code": "def forward(groups):\n    text = groups[0]\n    return {'stored': [int(text)], 'layout': []}\n\ndef inverse(record):\n    value = int(record['stored'][0])\n    return str(value).zfill(5)",
                },
            },
            ensure_ascii=False,
        ),
        "Example composed date/time-like attribute with a fixed exact layout:",
        json.dumps(
            {
                "tag": "composed_time",
                "meaning": "complete repeated date/time-like prefix",
                "class": "class_1_composed_numeric",
                "general": True,
                "order_hint": "global_composed",
                "regex": r"^([A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2})",
                "replacement": "{placeholder}",
                "program": {
                    "op": "python_exec",
                    "code": "def forward(groups):\n    text = groups[0]\n    dt = datetime.strptime(text, '%b %d %H:%M:%S')\n    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': []}\n\ndef inverse(record):\n    micros = int(record['stored'][0])\n    dt = datetime.fromtimestamp(micros / 1000000)\n    return dt.strftime('%b %d %H:%M:%S')",
                },
            },
            ensure_ascii=False,
        ),
        "If exact reconstruction needs variable width, variable spaces, optional text, or any other layout bytes, either capture the complete span with raw python_exec, or emit a python_exec that stores the required layout bytes and inverse reconstructs exactly.",
        "",
    ]
    if open_arithmetic_programs:
        parts.append(open_arithmetic_instruction())
    if family_header_program_prompt:
        parts.append(
            "Header examples: a syslog/asctime prefix should usually be one python_exec timestamp transform; a bracketed component such as '[worker pool]' may be one raw python_exec header stream if it varies and disrupts the main template.\n"
        )
    if function_first_prompt:
        parts.append(
            "Focus function budget on complete formatted spans and semantic dependencies. Plain leftover numbers can be handled by generic residual coders unless local context gives them a stable role.\n"
        )

    semantic_token_shapes = not generic_token_shapes
    for family, examples in family_examples:
        family_examples_limited = examples[:max_examples]
        required_slots_text, required_count = slot_complete_required_slots(
            family_examples_limited,
            max_examples=max_examples,
            semantic_token_shapes=semantic_token_shapes,
        )
        parts.extend(
            [
                f"### FAMILY {family.family_id}",
                f"support={len(family.line_indexes)}",
                f"template_anchor_key={' '.join(family.key)}",
                "examples_and_shapes:",
                example_token_table(
                    family_examples_limited,
                    max_examples=max_examples,
                    semantic_token_shapes=semantic_token_shapes,
                ),
            ]
        )
        if slot_complete_planner:
            parts.extend(
                [
                    f"required_value_like_slots ({required_count} sampled occurrences):",
                    required_slots_text,
                ]
            )
        parts.append("")
    return "\n".join(parts)


CLASSWISE_FUNCTION_BUCKETS: tuple[tuple[str, str, str], ...] = (
    ("class_1_functions", "class_1_composed_numeric", "numeric"),
    ("class_2_functions", "class_2_formatted_numeric", "numeric"),
    ("class_3_functions", "class_3_common_variable", "string"),
)


def normalize_classwise_family_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Flatten class-wise LLM output into the compiler's existing function list."""
    result = dict(payload)
    has_classwise = any(key in result for key, _semantic_class, _value_type in CLASSWISE_FUNCTION_BUCKETS)
    if not has_classwise:
        return result

    functions: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key, semantic_class, value_type in CLASSWISE_FUNCTION_BUCKETS:
        raw_functions = result.get(key, [])
        if not isinstance(raw_functions, list):
            continue
        for raw_function in raw_functions:
            if not isinstance(raw_function, dict):
                continue
            function = dict(raw_function)
            function["class"] = semantic_class
            function["value_type"] = value_type
            identity = json.dumps(function, sort_keys=True, ensure_ascii=False)
            if identity in seen:
                continue
            seen.add(identity)
            functions.append(function)
    result["functions"] = functions
    return result


def payload_for_family_from_batch(payload: dict[str, Any], family_id: int) -> dict[str, Any]:
    families_payload = payload.get("families", [])
    if not isinstance(families_payload, list):
        return {"functions": [], "_proposal_cache_mode": str(payload.get("_proposal_cache_mode", "batch_bad_shape"))}
    for item in families_payload:
        if not isinstance(item, dict):
            continue
        try:
            item_family_id = int(item.get("family_id"))
        except Exception:
            continue
        if item_family_id == family_id:
            result = normalize_classwise_family_payload(item)
            result["_proposal_cache_mode"] = str(payload.get("_proposal_cache_mode", "batch"))
            return result
    return {"functions": [], "_proposal_cache_mode": "batch_missing_family"}


def get_llm_runtime_stats(args: argparse.Namespace) -> dict[str, Any]:
    stats = getattr(args, "_llm_runtime_stats", None)
    if stats is None:
        stats = {
            "proposal_events": 0,
            "cache_hits": 0,
            "family_fallback_hits": 0,
            "api_calls": 0,
            "api_seconds": 0.0,
        }
        setattr(args, "_llm_runtime_stats", stats)
    return stats


def call_llm(prompt: str, args: argparse.Namespace, cache_path: Path) -> dict[str, Any]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path = cache_path.with_suffix(".prompt.txt")
    stats = get_llm_runtime_stats(args)
    stats["proposal_events"] = int(stats.get("proposal_events", 0)) + 1
    if cache_path.is_file() and not args.force_llm:
        stats["cache_hits"] = int(stats.get("cache_hits", 0)) + 1
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            payload = dict(payload)
            payload.setdefault("_proposal_cache_mode", "exact_cache")
        return payload
    if getattr(args, "llm_family_cache_fallback", False) and not args.force_llm:
        family_prefix = "_".join(cache_path.stem.split("_")[:2])
        fallback_paths = sorted(
            cache_path.parent.glob(f"{family_prefix}_*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        functions: list[dict[str, Any]] = []
        seen: set[str] = set()
        for fallback_path in fallback_paths:
            if fallback_path == cache_path:
                continue
            try:
                payload = json.loads(fallback_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            for function in payload.get("functions", []):
                if not isinstance(function, dict):
                    continue
                key = json.dumps(function, sort_keys=True, ensure_ascii=False)
                if key in seen:
                    continue
                seen.add(key)
                functions.append(function)
        if functions:
            stats["family_fallback_hits"] = int(stats.get("family_fallback_hits", 0)) + 1
            return {
                "functions": functions,
                "_proposal_cache_mode": "family_fallback",
                "_proposal_cache_candidates": len(fallback_paths),
            }
    if getattr(args, "llm_global_cache_fallback", False) and not args.force_llm:
        fallback_paths = sorted(
            cache_path.parent.glob("*.json"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        functions: list[dict[str, Any]] = []
        seen: set[str] = set()
        max_functions = int(getattr(args, "llm_global_cache_fallback_max_functions", 0) or 0)
        for fallback_path in fallback_paths:
            if fallback_path == cache_path:
                continue
            try:
                payload = json.loads(fallback_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            for function in payload.get("functions", []):
                if not isinstance(function, dict):
                    continue
                key = json.dumps(function, sort_keys=True, ensure_ascii=False)
                if key in seen:
                    continue
                seen.add(key)
                functions.append(function)
                if max_functions > 0 and len(functions) >= max_functions:
                    break
            if max_functions > 0 and len(functions) >= max_functions:
                break
        if functions:
            stats["global_cache_fallback_hits"] = int(stats.get("global_cache_fallback_hits", 0)) + 1
            return {
                "functions": functions,
                "_proposal_cache_mode": "global_cache_fallback",
                "_proposal_cache_candidates": len(fallback_paths),
            }
    if getattr(args, "llm_cache_only", False):
        stats["cache_only_misses"] = int(stats.get("cache_only_misses", 0)) + 1
        return {
            "functions": [],
            "_proposal_cache_mode": "cache_only_miss",
        }
    prompt_path.write_text(prompt, encoding="utf-8")
    api_key = os.environ.get("PARE_LLM_API_KEY") or os.environ.get("YUNWU_API_KEY")
    if not api_key:
        raise RuntimeError("set PARE_LLM_API_KEY or YUNWU_API_KEY")
    body = {
        "model": args.model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": "Return exact JSON only. No markdown. Generate reversible log extraction functions.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": args.temperature,
    }
    request = urllib.request.Request(
        args.api_base,
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"},
        method="POST",
    )
    raw = ""
    max_attempts = 1 + max(0, int(getattr(args, "api_retries", 0) or 0))
    last_error = ""
    for attempt in range(max_attempts):
        api_start = time.perf_counter()
        stats["api_attempts"] = int(stats.get("api_attempts", 0)) + 1
        try:
            with urllib.request.urlopen(request, timeout=args.timeout) as response:
                raw = response.read().decode("utf-8")
            break
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            last_error = f"LLM API HTTP {exc.code}: {detail[:800]}"
        except urllib.error.URLError as exc:
            last_error = f"LLM API URL error: {exc.reason}"
        except OSError as exc:
            last_error = f"LLM API OS error: {exc}"
        finally:
            stats["api_seconds"] = float(stats.get("api_seconds", 0.0)) + (time.perf_counter() - api_start)
        stats["api_failures"] = int(stats.get("api_failures", 0)) + 1
        if attempt + 1 < max_attempts:
            time.sleep(min(2.0 * (attempt + 1), 5.0))
    if not raw:
        raise RuntimeError(last_error or "LLM API failed without response")
    stats["api_calls"] = int(stats.get("api_calls", 0)) + 1
    decoded = json.loads(raw)
    content = decoded["choices"][0]["message"]["content"].strip()
    if content.startswith("```"):
        lines = content.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].startswith("```"):
            lines = lines[:-1]
        content = "\n".join(lines).strip()
    try:
        payload = json.loads(content)
    except json.JSONDecodeError:
        repaired = re.sub(r'\\(?!["\\/bfnrtu])', r"\\\\", content)
        payload = json.loads(repaired)
    if isinstance(payload, dict):
        payload = dict(payload)
        payload.setdefault("_proposal_cache_mode", "api")
    cache_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return payload


def merge_proposal_payloads(*payloads: dict[str, Any]) -> dict[str, Any]:
    """Merge cached and fresh proposal functions without duplicating candidates."""
    merged_functions: list[dict[str, Any]] = []
    seen: set[str] = set()
    modes: list[str] = []
    for payload in payloads:
        mode = payload.get("_proposal_cache_mode")
        if isinstance(mode, str) and mode:
            modes.append(mode)
        for function in payload.get("functions", []):
            if not isinstance(function, dict):
                continue
            key = json.dumps(function, sort_keys=True, ensure_ascii=False)
            if key in seen:
                continue
            seen.add(key)
            merged_functions.append(function)
    return {
        "functions": merged_functions,
        "_proposal_cache_mode": "+".join(modes) if modes else "merged",
        "_proposal_cache_candidates": sum(
            int(payload.get("_proposal_cache_candidates", 0) or 0)
            for payload in payloads
        ),
    }


def template_index_payload_for_family(family: Family, args: argparse.Namespace) -> dict[str, Any] | None:
    return None


def function_trie_payload_for_family(family: Family, args: argparse.Namespace) -> dict[str, Any] | None:
    return None


def function_trigger_payload_for_family(family: Family, args: argparse.Namespace) -> dict[str, Any] | None:
    return None


def append_template_index_entry(
    args: argparse.Namespace,
    family: Family,
    specs: list[CandidateSpec],
    source: str,
) -> None:
    return


def next_tag(index: int) -> str:
    alphabet = string.ascii_uppercase
    if index < len(alphabet):
        return alphabet[index]
    return f"{alphabet[index % len(alphabet)]}{index // len(alphabet)}"


def version_id(args: argparse.Namespace) -> str:
    mode_suffix = "-open-arithmetic" if getattr(args, "open_arithmetic_programs", False) else ""
    if getattr(args, "slot_complete_planner", False):
        mode_suffix += "-slot-complete"
    if getattr(args, "generic_token_shapes", False):
        mode_suffix += "-generic-shapes"
    if getattr(args, "function_first_prompt", False):
        mode_suffix += "-function-first"
    if getattr(args, "free_form_program_prompt", False):
        mode_suffix += "-freeform-program"
    if getattr(args, "free_form_transducer_sketch_prompt", False):
        mode_suffix += "-transducer-sketch"
    if getattr(args, "purpose_only_program_prompt", False):
        mode_suffix += "-purpose-only-program"
    if getattr(args, "semantic_focus_program_prompt", False):
        mode_suffix += "-semantic-focus-program"
    if getattr(args, "semantic_rich_examples_prompt", False):
        mode_suffix += "-semantic-rich-examples"
    if getattr(args, "family_header_program_prompt", False):
        mode_suffix += "-family-header"
    if getattr(args, "semantic_numeric_only_program_prompt", False):
        mode_suffix += "-semantic-numeric-only"
    if getattr(args, "semantic_three_class_program_prompt", False):
        mode_suffix += "-semantic-three-class"
    if getattr(args, "residual_family_simple_mixed_context", False):
        mode_suffix += "-family-simple-mixedctx"
        if getattr(args, "residual_family_simple_context_side", "both") != "both":
            mode_suffix += f"-{getattr(args, 'residual_family_simple_context_side', 'both')}ctx"
        if getattr(args, "residual_family_simple_placeholder_context", False):
            mode_suffix += "-phctx"
    if getattr(args, "offline_family_topk_query", False):
        mode_suffix += "-offline-family-topk"
        if getattr(args, "offline_family_batch_query", False):
            mode_suffix += f"-batch{getattr(args, 'offline_family_batch_max_examples', 0)}"
    if getattr(args, "contextualize_broad_llm_regex", False):
        mode_suffix += "-contextualized-regex"
    if getattr(args, "program_mdl_compiler", False):
        mode_suffix += "-mdl-compiler"
    if getattr(args, "program_bundle_mdl_admission", False):
        mode_suffix += "-bundle-mdl"
    if getattr(args, "program_line_mdl_admission", False):
        mode_suffix += "-line-mdl"
    if getattr(args, "program_line_mdl_semantic_grace", False):
        mode_suffix += "-semantic-grace"
    if getattr(args, "cegis_repair", False):
        mode_suffix += "-cegis-repair"
    if getattr(args, "verifier_repair_agent", False):
        mode_suffix += "-verifier-repair"
    if getattr(args, "oracle_subordinate_time_auto", False):
        mode_suffix += "-subordinate-time-auto"
    if getattr(args, "placeholder_slot_fission", False):
        mode_suffix += "-slot-fission"
    if getattr(args, "residual_family_simple_planner", False):
        mode_suffix += "-family-simple-residual"
    if getattr(args, "post_merge_hex_streams", False):
        mode_suffix += "-hex-merge"
    if getattr(args, "compact_decoder_contract", False):
        mode_suffix += "-decoder-contract"
    if getattr(args, "residual_numeric_lattice", False):
        suffix = "-residual-numeric-lattice"
        if getattr(args, "residual_stream_coalesce", False):
            suffix += "-coalesced"
        return VERSION_ID_R8 + suffix + mode_suffix
    if getattr(args, "residual_stream_coalesce", False):
        return VERSION_ID_R8 + "-residual-variable-streams-coalesced" + mode_suffix
    if getattr(args, "residual_variable_stream_mode", "none") != "none":
        return VERSION_ID_R8 + "-residual-variable-streams" + mode_suffix
    if getattr(args, "residual_xsignature_streams", False):
        suffix = "-residual-xsignature-streams"
        if getattr(args, "residual_xsignature_use_signature_placeholder", False):
            suffix += "-sigplaceholder"
        return VERSION_ID_R8 + suffix + mode_suffix
    if args.residual_line_transducer:
        return VERSION_ID_R8 + mode_suffix
    if args.validated_program_reindex:
        return VERSION_ID_R7 + mode_suffix
    if args.semantic_global_replay:
        return VERSION_ID_R6 + mode_suffix
    if args.semantic_stream_registry:
        return VERSION_ID_R5 + mode_suffix
    if args.whole_line_program_cache:
        return VERSION_ID_R4 + mode_suffix
    if args.relation_streams:
        return VERSION_ID_R3 + mode_suffix
    return (VERSION_ID_R2 if args.canonical_global_fields else VERSION_ID_R1) + mode_suffix


def semantic_key_from_payload(
    item: dict[str, Any],
    pattern: str,
    replacement: str,
    program: dict[str, object],
    op: str,
) -> str:
    """Map independent template-local functions onto shared semantic streams.

    This is the contribution-first repair for whole-line TTPS: high-impact
    fields such as timestamps must become one large stream across templates,
    rather than dozens of tiny family-local streams.  The key is conservative:
    merge only obvious self-describing fields or identical numeric programs.
    """
    meaning = str(item.get("meaning", "")).strip().lower()
    pattern_l = pattern.lower()
    program_payload = json.dumps(program, sort_keys=True, separators=(",", ":"))
    if any(word in meaning for word in ["timeout", "time out", "lifetime", "duration", "elapsed", "latency"]):
        return f"duration:{program_payload}"
    if op in {"datetime_strptime_delta", "syslog_delta", "month_day_hms_delta", "hms_delta", "day_hms_delta"}:
        return f"time:{program_payload}"
    if op == "int_expr_delta" and (
        "timestamp" in meaning
        or "time" in meaning
        or "date" in meaning
        or "timestamp" in pattern_l
        or r"\d{2}:\d{2}" in pattern
    ):
        return f"time:{program_payload}"
    time_meaning = any(
        phrase in meaning
        for phrase in [
            "timestamp",
            "datetime",
            "date/time",
            "date-time",
            "date time",
            "time-like",
            "date-like",
            "syslog time",
        ]
    )
    if time_meaning or (
        r"\d{2}:\d{2}:\d{2}" in pattern and any(month in pattern for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    ):
        raw_time_shape = "generic"
        if (
            "%a" in program_payload
            or "%Y" in program_payload
            or re.search(r"\\w\{3\}.*\\w\{3\}", pattern)
            or re.search(r"\[A-Z\]\[a-z\].*\[A-Z\]\[a-z\]", pattern)
        ):
            raw_time_shape = "asctime_year"
        elif (
            "%b" in program_payload
            or "\\w{3}" in pattern
            or re.search(r"\[A-Z\]\[a-z\]\{2\}", pattern)
        ) and r"\d{2}:\d{2}:\d{2}" in pattern:
            raw_time_shape = "syslog"
        elif r"\d{4}" in pattern or "%Y" in program_payload:
            raw_time_shape = "year"
        return f"raw_time:{raw_time_shape}:{program_payload}:{replacement}"
    meaning_tokens = set(re.findall(r"[a-z0-9]+", meaning))
    pattern_has_ipv4_shape = (
        "(?:\\d{1,3}\\.){3}" in pattern
        or r"\d{1,3}\.\d{1,3}" in pattern
    )
    if (
        meaning in {"port", "client_port", "source_port"}
        or "port" in meaning_tokens
        or re.search(r"(?<![a-z0-9_])port(?![a-z0-9_])", pattern_l)
    ) and not pattern_has_ipv4_shape:
        # Context names may appear in the meaning (for example,
        # "port conditioned by source IP"). Classify the stored target from
        # its own pattern before considering the referenced context type.
        return "port:auto"
    if (
        "ipv4" in meaning
        or "ip address" in meaning
        or "ip" in meaning_tokens
        or pattern_has_ipv4_shape
    ):
        return "ipv4:auto"
    if (
        "host_port" in meaning
        or "host and port" in meaning
        or "host_and_port" in meaning
        or "ip and port" in meaning
        or "ip:port" in meaning
        or "proxy_address" in meaning
        or "destination_host" in meaning
        or (("ip" in meaning or "address" in meaning) and "port" in meaning)
        or re.search(r"(?:\\d|\[0-9\]).*\..*(?:\\d|\[0-9\]).*:.*(?:\\d|\[0-9\])", pattern)
    ):
        return "hostport:auto"
    if "bytes_sent" in meaning or ("bytes" in meaning and "sent" in meaning):
        return "bytes_sent:auto"
    if "bytes_received" in meaning or ("bytes" in meaning and "received" in meaning):
        return "bytes_received:auto"
    if "log_level" in meaning or meaning == "level" or "log level" in meaning:
        return "level:auto"
    if meaning in {"port", "client_port", "source_port"} or "port" in meaning_tokens or re.search(r"(?<![a-z0-9_])port(?![a-z0-9_])", pattern_l):
        return "port:auto"
    if "sshd\\[" in pattern_l or re.search(r"[A-Za-z_][\w.-]*\\\[", pattern):
        return "pid:auto"
    if "username" in meaning or meaning in {"user", "auth_user"}:
        return "user:auto"
    if "domain" in meaning or "reverse" in meaning or "getaddrinfo" in pattern_l:
        return "domain:auto"
    if "path" in meaning or pattern.startswith("/") or "[^\\s/]" in pattern and "/" in pattern:
        # Paths are context-sensitive: merging all paths into one global stream
        # hurt Apache, while template-slot path streams were beneficial in the
        # best baseline. Keep path functions local and let the MDL gate decide.
        return ""
    return ""


def llm_context_group(item: dict[str, Any], store_group: int) -> int:
    """Read the LLM-declared entropy context capture group, if any."""
    context_payload = item.get("context")
    candidates: list[Any] = [
        item.get("context_group"),
        item.get("context_capture"),
    ]
    if isinstance(context_payload, dict):
        candidates.extend([
            context_payload.get("group"),
            context_payload.get("context_group"),
            context_payload.get("capture"),
            context_payload.get("source"),
        ])
    elif context_payload is not None:
        candidates.append(context_payload)
    captures = item.get("captures")
    if isinstance(captures, dict):
        candidates.extend([
            captures.get("context"),
            captures.get("context_group"),
        ])
    entropy_context = item.get("entropy_context")
    if isinstance(entropy_context, dict) and bool(entropy_context.get("has_context", False)):
        candidates.extend([
            entropy_context.get("context_group"),
            entropy_context.get("context_capture"),
            entropy_context.get("context_source"),
        ])
    for raw in candidates:
        if raw is None:
            continue
        if isinstance(raw, int):
            group = raw
        else:
            text = str(raw).strip()
            match = re.fullmatch(r"(?:g|group)?(\d+)", text)
            if not match:
                continue
            group = int(match.group(1))
        if group > 0 and group != store_group:
            return group
    return 0


def normalize_context_ref(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"none", "null", "0", "false"}:
        return ""
    if re.fullmatch(r"(?:g|group)?\d+", text, flags=re.IGNORECASE):
        return ""
    return re.sub(r"[^a-z0-9_:.+-]+", "_", text.lower()).strip("_")


def llm_context_ref(item: dict[str, Any]) -> str:
    """Read a semantic reference to another function in the same proposal."""
    candidates: list[Any] = [item.get("context_ref")]
    context_payload = item.get("context")
    if isinstance(context_payload, dict):
        candidates.extend([
            context_payload.get("ref"),
            context_payload.get("tag"),
            context_payload.get("name"),
            context_payload.get("context_ref"),
            context_payload.get("source"),
        ])
    entropy_context = item.get("entropy_context")
    if isinstance(entropy_context, dict) and bool(entropy_context.get("has_context", False)):
        candidates.extend([
            entropy_context.get("context_ref"),
            entropy_context.get("context_name"),
            entropy_context.get("context_source"),
        ])
    for raw in candidates:
        ref = normalize_context_ref(raw)
        if ref:
            return ref
    return ""


def llm_context_projector_code(item: dict[str, Any]) -> tuple[str, str]:
    """Return the executable projector code and a schema error, if any."""
    payload = item.get("context")
    if payload is None:
        return "", ""
    if not isinstance(payload, dict):
        return "", "context must be null or an object containing code"
    code = payload.get("code")
    if not isinstance(code, str) or not code.strip():
        return "", "non-null context must contain executable code"
    return code.strip(), ""


def validate_direct_context_projector(
    spec: CandidateSpec,
    lines: list[str],
    max_matches: int = 128,
) -> tuple[bool, str, int]:
    """Validate a context projector without coupling it to reconstruction."""
    if not spec.program.get("context_code"):
        return True, "", 0
    try:
        regex = compiled_multiline_regex(spec.pattern)
    except Exception as exc:
        return False, f"context projector target regex failed: {exc}", 0
    checked = 0
    nonempty = 0
    try:
        for line in lines:
            for match in regex.finditer(line):
                groups = list(match.groups())
                first = dataset_extract.run_python_exec_context_projector(
                    line,
                    groups,
                    spec.program,
                )
                second = dataset_extract.run_python_exec_context_projector(
                    line,
                    groups,
                    spec.program,
                )
                if first != second:
                    return False, "context projector is nondeterministic", checked
                if first not in {None, ""}:
                    nonempty += 1
                checked += 1
                if checked >= max_matches:
                    break
            if checked >= max_matches:
                break
    except Exception as exc:
        return False, f"context projector execution failed: {exc}", checked
    if checked == 0:
        return False, "context projector target matched no validation values", 0
    if nonempty == 0:
        return False, "context projector returned no context value", checked
    return True, "", nonempty


def normalized_llm_semantic_class(item: dict[str, Any]) -> str:
    raw = str(item.get("class", item.get("semantic_class", ""))).strip().lower()
    raw = raw.replace("-", "_").replace(" ", "_")
    if raw in {"1", "class1", "class_1", "composed_numeric"}:
        return "class_1_composed_numeric"
    if raw in {"2", "class2", "class_2", "formatted_numeric", "semantic_numeric"}:
        return "class_2_formatted_numeric"
    if raw in {"3", "class3", "class_3", "common_variable", "raw_variable"}:
        return "class_3_common_variable"
    if raw in {
        "class_1_composed_numeric",
        "class_2_formatted_numeric",
        "class_3_common_variable",
    }:
        return raw
    return raw


def strict_context_candidate_kind(
    item: dict[str, Any],
    base_kind: str,
    context_group: int,
    context_ref: str = "",
    has_context_projector: bool = False,
) -> tuple[str, str, str]:
    semantic_class = normalized_llm_semantic_class(item)
    value_type = str(item.get("value_type", "")).strip().lower().replace("-", "_")
    if not value_type:
        value_type = "numeric" if semantic_class in {"class_1_composed_numeric", "class_2_formatted_numeric"} else "string"
    context_policy = "none"
    context_payload = item.get("context")
    if isinstance(context_payload, dict):
        context_policy = str(context_payload.get("policy", context_payload.get("context_policy", ""))).strip().lower()
    entropy_context = item.get("entropy_context")
    if isinstance(entropy_context, dict):
        context_policy = str(entropy_context.get("policy", entropy_context.get("context_policy", ""))).strip().lower()
    context_policy = str(item.get("context_policy", context_policy)).strip().lower() or (
        "direct_context_projector"
        if has_context_projector
        else ("group_by_context" if context_group or context_ref else "none")
    )
    # Direct projectors are materialized after semantic extraction, matching
    # the existing cross-function context path. Until then the target stream
    # keeps its ordinary kind so feature admission never sees half-built
    # [value, context] pairs.
    if context_group > 0:
        if semantic_class in {"class_1_composed_numeric", "class_2_formatted_numeric"} or value_type in {"numeric", "integer", "int"}:
            return "open_context_delta", value_type, context_policy
        return "open_context_dict", value_type, context_policy
    return base_kind, value_type, context_policy


def resolve_candidate_context_refs(specs: list[CandidateSpec]) -> None:
    """Bind proposal-level names to stable semantic keys when possible."""
    by_proposal_tag = {
        normalize_context_ref(spec.proposal_tag): spec
        for spec in specs
        if normalize_context_ref(spec.proposal_tag)
    }
    for spec in specs:
        ref = normalize_context_ref(spec.context_ref)
        if not ref:
            spec.context_ref = ""
            continue
        source = by_proposal_tag.get(ref)
        if source is None or source is spec:
            spec.context_ref = f"proposal:{ref}"
            continue
        if source.semantic_key:
            spec.context_ref = f"semantic:{source.semantic_key}"
        else:
            spec.context_ref = f"proposal:{normalize_context_ref(source.proposal_tag)}"


MONTH_NAMES_RE = re.compile(r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b")


def looks_like_time_payload(
    item: dict[str, Any],
    pattern: str,
    program: dict[str, object],
    sample_values: list[str] | None = None,
) -> bool:
    """Conservatively identify timestamp-like spans for oracle subordination.

    The open-arithmetic prompt can produce verifier-passing numeric sketches
    for fragments of a timestamp.  For timestamp-shaped spans, that is usually
    the wrong ownership boundary: the deterministic codec selector already has
    syslog/asctime/datetime codecs and can choose among them losslessly.
    """
    meaning = str(item.get("meaning", "")).strip().lower()
    if any(word in meaning for word in ["timestamp", "datetime", "date", "syslog"]):
        return True
    program_payload = json.dumps(program, sort_keys=True)
    if any(token in program_payload for token in ["%b", "%a", "%Y", "%H", "%M", "%S"]):
        return True
    has_hms = r"\d{2}:\d{2}:\d{2}" in pattern or re.search(r"\d\{2\}:\\d\{2\}:\\d\{2\}", pattern) is not None
    if has_hms and (
        MONTH_NAMES_RE.search(pattern)
        or "\\w{3}" in pattern
        or re.search(r"\[A-Z\]\\\[a-z\]", pattern) is not None
    ):
        return True
    if sample_values:
        sample = "\n".join(sample_values[:32])
        if re.search(r"\b\d{1,2}:\d{2}:\d{2}\b", sample) and (
            MONTH_NAMES_RE.search(sample) or re.search(r"\b\d{4}[-/]\d{2}[-/]\d{2}\b", sample)
        ):
            return True
    return False


def replacement_repair_variants(replacement: str) -> list[str]:
    variants = [replacement]
    if "{placeholder}" in replacement:
        return variants
    fields = re.findall(r"{(group[1-6])}", replacement)
    for field in fields:
        repaired = replacement.replace("{" + field + "}", "{placeholder}", 1)
        if repaired not in variants:
            variants.append(repaired)
    return variants


def python_exec_contract_repair_variants(program: dict[str, object]) -> list[tuple[dict[str, object], str]]:
    """Repair common LLM Python sketches into the executable archive contract.

    This operator does not invent a new recognizer or field meaning.  It only
    rewrites code already proposed by the LLM when the code clearly uses
    ``datetime.strptime``/``strftime`` but violates the local execution
    contract, e.g. by importing modules or returning a datetime/ISO string
    instead of ``{"stored": [...], "layout": []}``.
    """
    if str(program.get("op", "")) != "python_exec":
        return []
    code = program.get("code")
    if not isinstance(code, str) or not code.strip():
        return []
    variants: list[tuple[dict[str, object], str]] = []

    stripped_lines = [
        line
        for line in code.splitlines()
        if not re.match(r"\s*(?:from\s+\w+(?:\.\w+)*\s+import\s+.+|import\s+.+)\s*$", line)
    ]
    stripped_code = "\n".join(stripped_lines).strip()
    if stripped_code and stripped_code != code.strip():
        repaired = dict(program)
        repaired["code"] = stripped_code
        variants.append((repaired, "python_exec_strip_imports"))

    fmt_match = re.search(r"datetime\.strptime\([^,]+,\s*(['\"])(.*?)\1\s*\)", code, flags=re.DOTALL)
    if fmt_match:
        fmt = fmt_match.group(2)
        render_matches = re.findall(r"\.strftime\(\s*(['\"])(.*?)\1\s*\)", code, flags=re.DOTALL)
        render_fmt = render_matches[-1][1] if render_matches else fmt
        render_slice = ""
        if re.search(r"\.strftime\(\s*(['\"]).*?\1\s*\)\s*\[\s*:\s*-\s*3\s*\]", code, flags=re.DOTALL):
            render_slice = "[:-3]"
        contract_code = (
            "def forward(groups):\n"
            "    text = groups[0]\n"
            f"    dt = datetime.strptime(text, {fmt!r})\n"
            "    return {'stored': [int(dt.timestamp() * 1000000)], 'layout': []}\n"
            "\n"
            "def inverse(record):\n"
            "    micros = int(record['stored'][0])\n"
            "    dt = datetime.fromtimestamp(micros / 1000000)\n"
            f"    return dt.strftime({render_fmt!r}){render_slice}"
        )
        repaired = dict(program)
        repaired["code"] = contract_code
        variants.append((repaired, "python_exec_datetime_contract"))

    deduped: list[tuple[dict[str, object], str]] = []
    seen: set[str] = set()
    for repaired, origin in variants:
        key = json.dumps(repaired, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        deduped.append((repaired, origin))
    return deduped


def payload_repair_variants(
    replacement: str,
    program: dict[str, object],
) -> list[tuple[str, dict[str, object]]]:
    """Generate verifier-checked repairs for common LLM function slips.

    The LLM often recognizes a multi-token timestamp correctly but writes a
    replacement such as ``[{group1} {group2}]``.  That is a no-op for
    compression because no placeholder is inserted.  When the declared datetime
    format exactly describes the concatenated groups, we can lift the literal
    wrapper into the datetime program and store the full regex match as one
    reversible value.  The normal verifier still decides whether the repair is
    admissible.
    """
    variants: list[tuple[str, dict[str, object]]] = []
    op = str(program.get("op", "auto_codec"))
    if op == "datetime_strptime_delta" and "{placeholder}" not in replacement:
        fmt = str(program.get("format", ""))
        render_fmt = str(program.get("render_format", ""))
        if "{group1} {group2}" in replacement and " " in fmt and render_fmt:
            repaired_program = dict(program)
            repaired_program["format"] = replacement.replace("{group1} {group2}", fmt)
            repaired_program["render_format"] = replacement.replace("{group1} {group2}", render_fmt)
            variants.append(("{placeholder}", repaired_program))
    for replacement_variant in replacement_repair_variants(replacement):
        for duplicate_repair in duplicate_storage_repair_variants(replacement_variant):
            variants.append((duplicate_repair, program))
            for repaired_program, _origin in python_exec_contract_repair_variants(program):
                variants.append((duplicate_repair, repaired_program))
    deduped: list[tuple[str, dict[str, object]]] = []
    seen: set[tuple[str, str]] = set()
    for replacement_variant, program_variant in variants:
        key = (replacement_variant, json.dumps(program_variant, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        deduped.append((replacement_variant, program_variant))
    return deduped


def replacement_duplicates_stored_group(replacement: str, store_group: int) -> bool:
    """Return True when replacement keeps bytes that placeholder already stores."""
    if store_group == 0:
        return bool(re.search(r"{group[0-6]}", replacement))
    return f"{{group{store_group}}}" in replacement


def compiler_repair_variants(
    replacement: str,
    program: dict[str, object],
    enable_cegis_repair: bool = False,
) -> list[tuple[str, dict[str, object], str]]:
    """Compile one LLM proposal into verifier/MDL-ranked candidate programs.

    The LLM proposal is treated as a sketch, not a final decision.  The compiler
    keeps deterministic repairs separate from the original program so the MDL
    optimizer can prefer the cheapest byte-exact version.
    """
    variants: list[tuple[str, dict[str, object], str]] = []
    for replacement_variant, program_variant in payload_repair_variants(replacement, program):
        variants.append((replacement_variant, program_variant, "llm_or_deterministic_repair"))
        if (
            str(program_variant.get("op", "")) == "python_exec"
            and replacement_variant != "{placeholder}"
            and "{placeholder}" in replacement_variant
            and "{group" in replacement_variant
        ):
            # If inverse(record) renders the whole matched span, preserving
            # context groups in replacement duplicates bytes.  Try the
            # full-span route; exact replay verification must still pass.
            variants.append(("{placeholder}", program_variant, "full_span_placeholder_repair"))
        if enable_cegis_repair and str(program_variant.get("op", "auto_codec")) != "auto_codec":
            # Counterexample-guided fallback: if the parse/render program is
            # close but not exact, preserve the same span as raw bytes.  The
            # verifier and MDL gate decide whether this is worth admitting.
            variants.append((replacement_variant, raw_python_exec_program(), "cegis_raw_same_span_python_exec"))
    if enable_cegis_repair:
        variants.append(("{placeholder}", raw_python_exec_program(), "cegis_raw_whole_match_python_exec"))

    deduped: list[tuple[str, dict[str, object], str]] = []
    seen: set[tuple[str, str]] = set()
    for replacement_variant, program_variant, origin in variants:
        key = (replacement_variant, json.dumps(program_variant, sort_keys=True))
        if key in seen:
            continue
        seen.add(key)
        deduped.append((replacement_variant, program_variant, origin))
    return deduped


def normalize_llm_program_sketch(program: dict[str, object]) -> dict[str, object]:
    """Normalize open LLM program sketches into the verifier's safe DSL shape.

    The prompt intentionally does not expose a closed program-op list.  In
    practice, LLMs often return the right idea with harmless JSON/layout
    variation, e.g. ``{"op": "datetime_strptime_delta", "parameters": {...}}``
    instead of putting ``format`` at the top level.  This adapter is deliberately
    syntactic: it does not infer field semantics or add dataset-specific regexes.
    The normal verifier still checks parse/render exactness before admission.
    """
    normalized = dict(program)
    params = normalized.get("parameters")
    if isinstance(params, dict):
        for key, value in params.items():
            normalized.setdefault(str(key), value)
        normalized.pop("parameters", None)
    if isinstance(normalized.get("function"), dict):
        function_payload = normalized.pop("function")
        for key, value in function_payload.items():
            normalized.setdefault(str(key), value)
    if isinstance(normalized.get("code"), str) and normalized.get("code", "").strip():
        normalized["op"] = "python_exec"

    op = str(normalized.get("op", "auto_codec")).strip()
    op_key = op.lower().replace("-", "_").replace(" ", "_")
    op_aliases = {
        "raw": "python_exec",
        "raw_stream": "python_exec",
        "stream_raw": "python_exec",
        "string_stream": "python_exec",
        "dictionary_rank": "python_exec",
        "dict_rank": "python_exec",
        "uint_delta": "int_expr_delta",
        "int_delta": "int_expr_delta",
        "integer_delta": "int_expr_delta",
        "delta_int": "int_expr_delta",
    }
    normalized["op"] = op_aliases.get(op_key, op)
    if str(normalized.get("op")) == "auto_codec":
        normalized = raw_python_exec_program()
    if str(normalized.get("op")) == "python_exec" and not str(normalized.get("code", "")).strip():
        normalized = raw_python_exec_program()
    safe_ops = getattr(dataset_extract, "SAFE_OPEN_FUNCTION_OPS", set())
    if str(normalized.get("op")) not in safe_ops:
        normalized = raw_python_exec_program()

    if normalized.get("op") == "int_expr_delta":
        expr = str(normalized.get("value_expr", ""))
        expr_rewrites = {
            "int(match.group(0))": "g0",
            "int(match.group(1))": "g1",
            "match.group(0)": "g0",
            "match.group(1)": "g1",
            "int(value)": "v",
            "value": "v",
        }
        compact_expr = expr.replace(" ", "")
        normalized["value_expr"] = expr_rewrites.get(compact_expr, expr)
        render = str(normalized.get("render", ""))
        render_rewrites = {
            "str(value)": "{v}",
            "str(v)": "{v}",
            "value": "{v}",
            "v": "{v}",
            "{value}": "{v}",
        }
        normalized["render"] = render_rewrites.get(render.replace(" ", ""), render)
        if not normalized.get("value_regex") and normalized.get("value_expr") in {"g0", "int(g0)", "g1", "int(g1)"}:
            normalized["value_regex"] = r"\d+"

    return normalized


def llm_item_has_required_python_exec_contract(item: dict[str, Any]) -> bool:
    """Return True only when the LLM emitted executable forward/inverse code."""
    program = item.get("program")
    if not isinstance(program, dict):
        program = item.get("function")
    if not isinstance(program, dict):
        return False
    if str(program.get("op", "")).strip() != "python_exec":
        return False
    code = program.get("code")
    if not isinstance(code, str) or not code.strip():
        return False
    return "def forward(" in code and "def inverse(" in code


def llm_item_program(item: dict[str, Any]) -> dict[str, object]:
    program = item.get("program")
    if not isinstance(program, dict):
        program = item.get("function")
    if not isinstance(program, dict):
        program = raw_python_exec_program()
    return normalize_llm_program_sketch(program)


def sketch_implies_numeric_scalar(program: dict[str, object]) -> bool:
    """Return True when an open LLM sketch says the stored value is numeric.

    This keeps the prompt open-ended: the LLM does not need to name an internal
    codec op.  The compiler only upgrades raw storage when the sketch text asks
    for a scalar/integer representation and the verifier can prove exact
    parse/render on concrete matches.
    """
    try:
        text = json.dumps(program, ensure_ascii=False, sort_keys=True).lower()
    except Exception:
        text = str(program).lower()
    if any(marker in text for marker in ("raw bytes", "exact original", "dictionary", "lookup", "ranked")):
        return False
    return any(
        marker in text
        for marker in (
            "integer",
            "numeric",
            "number",
            "scalar",
            "delta",
            "monotonic",
            "counter",
        )
    )


def numeric_scalar_program_from_values(
    program: dict[str, object],
    values: list[str],
) -> dict[str, object] | None:
    """Compile an open numeric sketch into a byte-exact integer delta program."""
    if str(program.get("op", "auto_codec")) != "auto_codec":
        return None
    if not sketch_implies_numeric_scalar(program):
        return None
    sample_values = [value for value in values if value is not None]
    if len(sample_values) < 2:
        return None
    if not all(re.fullmatch(r"-?\d+", value) for value in sample_values):
        return None
    abs_values = [value[1:] if value.startswith("-") else value for value in sample_values]
    has_leading_zero = any(len(value) > 1 and value.startswith("0") for value in abs_values)
    render = "{v}"
    value_regex = r"^(-?\d+)$"
    if has_leading_zero:
        if any(value.startswith("-") for value in sample_values):
            return None
        widths = {len(value) for value in abs_values}
        if len(widths) != 1:
            return None
        width = next(iter(widths))
        value_regex = rf"^(\d{{{width}}})$"
        render = "{v:0" + str(width) + "d}"
    candidate = {
        "op": "int_expr_delta",
        "value_regex": value_regex,
        "value_expr": "g1",
        "render": render,
    }
    try:
        for value in sample_values[:256]:
            if dataset_extract.render_open_function_exact(value, candidate) != value:
                return None
    except Exception:
        return None
    return candidate


def contextualize_broad_llm_regex_items(
    item: dict[str, Any],
    validation_text: str,
    max_variants: int = 4,
) -> list[dict[str, Any]]:
    return []


def contextualize_value_capture_llm_regex_items(
    item: dict[str, Any],
    validation_text: str,
    max_variants: int = 8,
    min_support: int = 2,
) -> list[dict[str, Any]]:
    """Add stable literal context to regexes that already capture the value.

    ``contextualize_broad_llm_regex_items`` handles the value-only case by
    wrapping the whole pattern.  After internal-value repair, however, a useful
    candidate often already has the right stored group, e.g.
    ``0x([0-9a-fA-F]+)`` with replacement ``0x{placeholder}``.  This repair
    keeps that stored group intact and only extends the match/replacement with
    stable neighboring literals such as `` (n.zxid)``.  It is generic: the
    neighbors are learned from the validation sample, and verifier/MDL still
    decides whether the variant is admitted.
    """
    pattern = str(item.get("regex", ""))
    replacement = str(item.get("replacement", "{placeholder}"))
    normalized_program = llm_item_program(item)
    if str(normalized_program.get("op", "auto_codec")) != "auto_codec":
        return []
    if replacement.count("{placeholder}") != 1 or "{group" in replacement:
        return []
    if len(pattern) > 140 or "{placeholder}" in pattern:
        return []
    try:
        regex = re.compile(pattern, flags=re.MULTILINE)
        if regex.groups < 1:
            return []
        matches = list(regex.finditer(validation_text))[:2048]
    except Exception:
        return []
    if len(matches) < min_support * 2:
        return []

    left_counts: Counter[str] = Counter()
    right_counts: Counter[str] = Counter()
    both_counts: Counter[tuple[str, str]] = Counter()
    for match in matches:
        line_start = validation_text.rfind("\n", 0, match.start()) + 1
        line_end = validation_text.find("\n", match.end())
        if line_end < 0:
            line_end = len(validation_text)
        before = validation_text[line_start:match.start()]
        after = validation_text[match.end():line_end]
        left = ""
        right = ""
        left_match = re.search(r"([A-Za-z_][\w.:-]*[ \t]+)$", before)
        if left_match:
            left = left_match.group(1)
        right_match = re.match(
            r"([ \t]*(?:\([^ \t\r\n)]{1,64}\)|\[[^ \t\r\n\]]{1,64}\]|[A-Za-z_][\w.:-]*|[,;:]))",
            after,
        )
        if right_match:
            right = right_match.group(1)
        if left:
            left_counts[left] += 1
        if right:
            right_counts[right] += 1
        if left or right:
            both_counts[(left, right)] += 1

    variants: list[dict[str, Any]] = []
    seen_patterns: set[str] = set()

    def add_variant(left: str, right: str, support: int) -> None:
        if len(variants) >= max_variants:
            return
        if support < min_support:
            return
        if not left and not right:
            return
        variant_pattern = f"{re.escape(left)}{pattern}{re.escape(right)}"
        if variant_pattern == pattern or variant_pattern in seen_patterns:
            return
        seen_patterns.add(variant_pattern)
        variant = dict(item)
        variant["regex"] = variant_pattern
        variant["replacement"] = f"{left}{replacement}{right}"
        variant["program"] = normalized_program
        meaning = str(variant.get("meaning", "")).strip()
        variant["meaning"] = (meaning + " value-context").strip() or "value_context"
        variant["_value_contextualized_from"] = pattern
        variants.append(variant)

    for (left, right), support in both_counts.most_common(max_variants * 2):
        add_variant(left, right, support)
    for right, support in right_counts.most_common(max_variants):
        add_variant("", right, support)
    for left, support in left_counts.most_common(max_variants):
        add_variant(left, "", support)
    return variants


def _simple_regex_to_replacement_literal(pattern_with_placeholder: str) -> str | None:
    if "(?" in pattern_with_placeholder or "^" in pattern_with_placeholder or "$" in pattern_with_placeholder:
        return None
    if "[" in pattern_with_placeholder or "]" in pattern_with_placeholder:
        return None
    # Convert escaped literal punctuation back to replacement text.  This helper
    # is intentionally narrow; complex regexes fall back to the original verifier.
    return re.sub(r"\\(.)", r"\1", pattern_with_placeholder)


def internal_value_capture_llm_regex_items(item: dict[str, Any], max_variants: int = 4) -> list[dict[str, Any]]:
    """Repair whole-match placeholders that accidentally store stable context."""
    pattern = str(item.get("regex", ""))
    replacement = str(item.get("replacement", "{placeholder}"))
    normalized_program = llm_item_program(item)
    if str(normalized_program.get("op", "auto_codec")) != "auto_codec":
        return []
    if replacement != "{placeholder}":
        return []
    if len(pattern) > 140 or "{placeholder}" in pattern:
        return []

    atom_patterns = [
        "[0-9a-fA-F]+",
        "[0-9A-Fa-f]+",
        "-?\\d+",
        "\\d+",
    ]
    variants: list[dict[str, Any]] = []
    seen: set[str] = set()
    for atom in atom_patterns:
        start = pattern.find(atom)
        if start < 0:
            continue
        variant_pattern = pattern[:start] + f"({atom})" + pattern[start + len(atom):]
        if variant_pattern in seen:
            continue
        replacement_pattern = pattern[:start] + "{placeholder}" + pattern[start + len(atom):]
        replacement_literal = _simple_regex_to_replacement_literal(replacement_pattern)
        if not replacement_literal or replacement_literal == "{placeholder}":
            continue
        seen.add(variant_pattern)
        variant = dict(item)
        variant["regex"] = variant_pattern
        variant["replacement"] = replacement_literal
        variant["program"] = normalized_program
        meaning = str(variant.get("meaning", "")).strip()
        variant["meaning"] = (meaning + " internal-value").strip() or "internal_value"
        variant["_internal_value_from"] = pattern
        variants.append(variant)
        if len(variants) >= max_variants:
            break
    return variants


def open_function_side_cost(values: list[str], program: dict[str, object]) -> int:
    op = str(program.get("op", "auto_codec"))
    if op == "auto_codec":
        return min(cost for _kind, cost in dataset_extract.stream_codec_candidates(values))
    if op == "python_exec":
        stored_values: list[object] = []
        layout_bytes = bytearray()
        for value in values:
            record = dataset_extract.run_python_exec_forward(value, program)
            stored = record.get("stored", [])
            if len(stored) != 1:
                raise ValueError("python_exec side-cost expects one stored value")
            stored_values.append(stored[0])
            for layout_item in record.get("layout", []):
                layout_bytes.append(int(layout_item))
        if all(isinstance(item, int) for item in stored_values):
            int_values = [int(item) for item in stored_values]
            if dataset_extract.can_decode_delta_stream(int_values):
                parts = [dataset_extract.encode_delta_values(int_values)]
            else:
                parts = [dataset_extract.encode_varint_stream_bytes(int_values)]
        elif all(isinstance(item, (str, int, float)) for item in stored_values):
            string_values = [str(item) for item in stored_values]
            # Use the same codec-cost estimator as raw auto streams.  The
            # actual codec is chosen later by the normal archive builder; this
            # branch only prevents raw-identity python_exec functions from
            # being mis-scored as integers during proposal admission.
            return (
                min(cost for _kind, cost in dataset_extract.stream_codec_candidates(string_values))
                + dataset_extract.compressed_parts_cost([bytes(layout_bytes)] if layout_bytes else [])
                + len(lzma.compress(json.dumps(program, sort_keys=True).encode("utf-8")))
            )
        else:
            raise ValueError("python_exec stored value must be scalar")
        if layout_bytes:
            parts.append(bytes(layout_bytes))
        parts.append(json.dumps(program, sort_keys=True).encode("utf-8"))
        return dataset_extract.compressed_parts_cost(parts)
    encoded_values: list[int] = []
    layouts = bytearray()
    fraction_scale: int | None = None
    if op == "datetime_strptime_delta" and "%f" in str(program.get("format", "")):
        widths = [dataset_extract.datetime_fraction_width(value, program) for value in values]
        fraction_scale = max(widths) if widths else 6
    for value in values:
        if op == "syslog_delta":
            encoded_value, layout = dataset_extract.parse_syslog_timestamp(value)
            encoded_values.append(encoded_value)
            layouts.append(layout)
        elif op == "day_hms_delta":
            day, width, seconds = dataset_extract.parse_day_hms(value)
            encoded_values.append(day * 24 * 3600 + seconds)
            layouts.append(width)
        elif fraction_scale is not None:
            encoded_values.append(dataset_extract.parse_open_function_value(value, program, fraction_scale=fraction_scale))
            layouts.append(dataset_extract.datetime_fraction_width(value, program))
        else:
            encoded_values.append(dataset_extract.parse_open_function_value(value, program))
    if not dataset_extract.can_decode_delta_stream(encoded_values):
        return dataset_extract.compressed_parts_cost([dataset_extract.encode_string_stream_bytes(values)])
    parts = [dataset_extract.encode_delta_values(encoded_values)]
    if layouts:
        parts.append(bytes(layouts))
    parts.append(json.dumps(program, sort_keys=True).encode("utf-8"))
    return dataset_extract.compressed_parts_cost(parts)


def compiled_candidate_proxy_cost(
    matches: list[re.Match[str]],
    replacement: str,
    program: dict[str, object],
    store_group: int,
    placeholder: str,
) -> int:
    values: list[str] = []
    rendered_main = bytearray()
    for match in matches:
        raw_value = match.group(store_group)
        if raw_value is None:
            raise ValueError("empty store group")
        values.append(raw_value)
        rendered_value = dataset_extract.render_open_function_exact(raw_value, program)
        rendered = format_replacement_fast(replacement, placeholder, match)
        if "{placeholder}" in replacement:
            rendered = format_replacement_fast(replacement, placeholder, match)
        # Use the same replacement path as extraction would, with a literal
        # placeholder.  The verifier separately checks invertibility.
        rendered_main.extend(rendered.encode("latin-1", errors="ignore"))
    main_cost = len(lzma.compress(bytes(rendered_main)))
    side_cost = open_function_side_cost(values, program)
    metadata_cost = len(lzma.compress((replacement + "\n" + json.dumps(program, sort_keys=True)).encode("utf-8")))
    return main_cost + side_cost + metadata_cost


def program_validation_proxy_cost(lines: list[str], specs: list[CandidateSpec]) -> int:
    """Estimate whole-program cost on a held-out family sample.

    Per-function MDL is too myopic for log compression: extracting a span can be
    cheap locally while making the residual line dictionary much larger.  This
    proxy applies the ordered program exactly as the compressor would, then
    charges the residual ID+mapping core plus every side stream and the compact
    program metadata.  Admission therefore asks whether a new function reduces
    total family cost, not whether its own stream is compressible in isolation.
    """
    if not lines:
        return 0
    if not specs:
        return id_mapping_proxy_cost("".join(lines))
    placeholders = {spec.tag: f"__PARE_COST_{spec.tag}__" for spec in specs}
    values_by_tag: dict[str, list[str]] = {}
    transformed_lines: list[str] = []
    for line in lines:
        transformed_lines.append(apply_specs_to_line(line, specs, placeholders, values_by_tag))
    cost = id_mapping_proxy_cost("".join(transformed_lines))
    metadata_parts: list[bytes] = []
    for spec in specs:
        values = values_by_tag.get(spec.tag, [])
        if not values:
            continue
        cost += open_function_side_cost(values, spec.program)
        metadata_parts.append(
            json.dumps(
                {
                    "tag": spec.tag,
                    "pattern": spec.pattern,
                    "replacement": spec.replacement,
                    "program": spec.program,
                    "store_group": spec.store_group,
                    "semantic_key": spec.semantic_key,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    if metadata_parts:
        cost += dataset_extract.compressed_parts_cost(metadata_parts)
    return cost


def preferred_semantic_tag(semantic_key: str) -> str:
    if semantic_key.startswith(("time:", "raw_time:")):
        return "TS"
    if semantic_key.startswith("duration:"):
        return "DU"
    if semantic_key.startswith("ipv4:"):
        return "IP"
    if semantic_key.startswith("hostport:"):
        return "HP"
    if semantic_key.startswith("bytes_sent:"):
        return "BS"
    if semantic_key.startswith("bytes_received:"):
        return "BR"
    if semantic_key.startswith("level:"):
        return "LV"
    if semantic_key.startswith("port:"):
        return "PT"
    if semantic_key.startswith("pid:"):
        return "PD"
    if semantic_key.startswith("user:"):
        return "US"
    if semantic_key.startswith("domain:"):
        return "DN"
    if semantic_key.startswith("path:"):
        return "PA"
    return "S"


def allocate_unique_tag(preferred: str, used_tags: set[str]) -> str:
    if preferred not in used_tags:
        used_tags.add(preferred)
        return preferred
    index = 0
    while True:
        tag = f"{preferred}{index}"
        if tag not in used_tags:
            used_tags.add(tag)
            return tag
        index += 1


def semantic_seed_specificity(spec: CandidateSpec) -> int:
    pattern_l = spec.pattern.lower()
    score = sum(1 for ch in pattern_l if ch.isalnum())
    score -= 8 * pattern_l.count("[a-z")
    score -= 8 * pattern_l.count("\\w")
    score -= 4 * pattern_l.count(".+")
    score -= 2 * (pattern_l.count("+") + pattern_l.count("*"))
    if spec.semantic_key.startswith("level:") and any(level in pattern_l for level in ["notice", "error", "warn", "debug", "info"]):
        score += 100
    return score


def semantic_registry_representative_score(spec: CandidateSpec) -> int:
    score = semantic_seed_specificity(spec)
    if (
        (spec.context_ref or spec.program.get("context_code"))
        and spec.kind in {"auto", "open_function"}
        and dataset_extract.is_open_function_program_safe(spec.program)
    ):
        # Context routing is executable only while the target's verified
        # transform survives semantic deduplication. Prefer that seed over an
        # unrelated candidate carrying the same target semantic key.
        score += 1_000_000
    return score


def assign_semantic_registry_tags(
    specs: list[CandidateSpec],
    semantic_registry: dict[str, str],
    semantic_registry_specs: dict[str, CandidateSpec],
    used_tags: set[str],
) -> None:
    for spec in specs:
        if spec.semantic_key:
            existing = semantic_registry.get(spec.semantic_key)
            if existing is not None:
                spec.tag = existing
                current = semantic_registry_specs.get(spec.semantic_key)
                if current is None or semantic_registry_representative_score(spec) > semantic_registry_representative_score(current):
                    semantic_registry_specs[spec.semantic_key] = spec
                continue
            tag = allocate_unique_tag(preferred_semantic_tag(spec.semantic_key), used_tags)
            semantic_registry[spec.semantic_key] = tag
            spec.tag = tag
            semantic_registry_specs[spec.semantic_key] = spec
        else:
            spec.tag = allocate_unique_tag(next_tag(len(used_tags)), used_tags)


def semantic_admission_regex_score(spec: CandidateSpec) -> float:
    """Generic specificity score for deciding whether a function may go global."""
    features = function_router_features(spec)
    anchors = tuple(features["anchors"])
    punct = set(features["punct"])
    score = 0.0
    score += min(8.0, sum(min(len(anchor), 12) for anchor in anchors[:4]) / 4.0)
    score += min(4.0, len(punct) * 0.75)
    if features["needs_digit"]:
        score += 1.0
    if features["needs_alpha"]:
        score += 0.5
    broad_penalty = 0.0
    pattern = spec.pattern.lower()
    broad_penalty += 2.0 * pattern.count(".+")
    broad_penalty += 1.0 * pattern.count(".*")
    broad_penalty += 0.75 * pattern.count("\\w+")
    broad_penalty += 0.75 * pattern.count("[^")
    return score - broad_penalty


def semantic_registry_admission_score(family: Family, spec: CandidateSpec, args: argparse.Namespace) -> float:
    """Score global-promotion fitness using support and generic syntax only."""
    support = max(1, len(family.line_indexes))
    support_score = min(10.0, math.log2(support + 1.0))
    regex_score = semantic_admission_regex_score(spec)
    class_bonus = 0.0
    if semantic_key_class(spec.semantic_key):
        # The semantic key was proposed by the API/LLM, but this bonus is small:
        # promotion still needs support and regex evidence.
        class_bonus = 1.5
    replacement = spec.replacement
    context_bonus = 0.0
    before, marker, after = replacement.partition("{placeholder}")
    if marker:
        if before and not before.startswith("{group"):
            context_bonus += 0.5
        if after and not after.startswith("{group"):
            context_bonus += 0.5
    score = support_score + regex_score + class_bonus + context_bonus
    return score


def apply_semantic_registry_admission_router(
    family: Family,
    specs: list[CandidateSpec],
    args: argparse.Namespace,
) -> list[CandidateSpec]:
    if not getattr(args, "semantic_registry_admission_router", False):
        return specs
    stats = getattr(args, "_semantic_registry_admission_stats", None)
    if not isinstance(stats, dict):
        stats = {
            "enabled": 1,
            "specs": 0,
            "promoted": 0,
            "kept_local": 0,
            "score_sum": 0.0,
            "min_score": args.semantic_registry_admission_min_score,
        }
        setattr(args, "_semantic_registry_admission_stats", stats)
    routed: list[CandidateSpec] = []
    for spec in specs:
        stats["specs"] += 1
        if not spec.semantic_key:
            routed.append(spec)
            continue
        score = semantic_registry_admission_score(family, spec, args)
        stats["score_sum"] = float(stats.get("score_sum", 0.0)) + score
        if score >= args.semantic_registry_admission_min_score:
            stats["promoted"] += 1
            routed.append(spec)
        else:
            stats["kept_local"] += 1
            local_spec = clone_spec(spec)
            local_spec.semantic_key = ""
            routed.append(local_spec)
    return routed


def semantic_key_can_global_replay(semantic_key: str) -> bool:
    """Replay only high-confidence, high-contribution fields globally.

    Paths and raw strings may be intentionally template-scoped; timestamps,
    IPv4 addresses, and log levels are usually self-delimiting and are exactly
    the contribution modes that the best baseline exploits.
    """
    return semantic_key.startswith((
        "time:",
        "raw_time:",
        "duration:",
        "ipv4:",
        "hostport:",
        "bytes_sent:",
        "bytes_received:",
        "level:",
        "port:",
        "pid:",
        "user:",
        "domain:",
    ))


def semantic_key_class(semantic_key: str) -> str:
    """Coarse class used to arbitrate overlapping scout and LLM codecs.

    The cheap global scout proposes self-delimiting fields early, but those
    proposals are intentionally weak.  If the verifier has already accepted an
    LLM/cache codec for the same semantic class, the semantic codec should own
    the span because it was synthesized from the actual template family and can
    avoid fragmenting one field into several side streams.
    """
    if semantic_key.startswith(("time:", "raw_time:", "duration:")):
        return "time"
    if semantic_key.startswith("ipv4:"):
        return "ipv4"
    if semantic_key.startswith("hostport:"):
        return "hostport"
    if semantic_key.startswith(("bytes_sent:", "bytes_received:")):
        return "bytes"
    if semantic_key.startswith("level:"):
        return "level"
    if semantic_key.startswith("port:"):
        return "port"
    if semantic_key.startswith("pid:"):
        return "pid"
    if semantic_key.startswith("user:"):
        return "user"
    if semantic_key.startswith("domain:"):
        return "domain"
    return ""


def scout_spec_class(spec: CandidateSpec) -> str:
    """Coarse class for deterministic global-value-rescue candidates."""
    op = str(spec.program.get("op", ""))
    if spec.tag in {"TS", "AT", "DT"} or op in {
        "datetime_strptime_delta",
        "syslog_delta",
        "asctime_year_delta",
        "month_day_hms_delta",
        "hms_delta",
        "day_hms_delta",
    } or "datetime" in spec.pattern or r"\d{6,8}-" in spec.pattern:
        return "time"
    if spec.tag.startswith("IP") or "(?:\\d{1,3}\\.){3}" in spec.pattern:
        return "ipv4"
    return ""


def semantic_replay_order(semantic_key: str) -> tuple[int, str]:
    order = [
        ("time:", 0),
        ("raw_time:", 1),
        ("duration:", 2),
        ("hostport:", 3),
        ("ipv4:", 4),
        ("pid:", 5),
        ("port:", 6),
        ("bytes_sent:", 7),
        ("bytes_received:", 8),
        ("user:", 9),
        ("domain:", 10),
        ("level:", 11),
    ]
    for prefix, rank in order:
        if semantic_key.startswith(prefix):
            return rank, semantic_key
    return 100, semantic_key


def relax_timestamp_digit_quantifiers(pattern: str) -> str:
    """Make separator-delimited timestamp digit runs width agnostic.

    The generated timestamp parser remains the semantic validity guard.  This
    pass only prevents an observed display width such as two-digit days from
    becoming an accidental language restriction.  Exact widths are carried by
    the executable program's layout stream.
    """
    relaxed = re.sub(r"\\d\{\d+(?:,\d*)?\}", r"\\d+", pattern)
    relaxed = re.sub(r"\[0-9\]\{\d+(?:,\d*)?\}", r"[0-9]+", relaxed)
    return relaxed


def relax_literal_space_runs(pattern: str) -> str:
    """Broaden literal spaces without adding capture groups.

    Generated forward functions address regex groups by index. Replacing a
    literal space with ``( +)`` would shift those indexes, so replay uses the
    non-capturing equivalent ``[ ]+``. Exact space counts are carried by the
    layout dictionary rather than by regex groups.
    """
    out: list[str] = []
    index = 0
    escaped = False
    in_class = False
    while index < len(pattern):
        char = pattern[index]
        if escaped:
            out.append(char)
            escaped = False
            index += 1
            continue
        if char == "\\":
            out.append(char)
            escaped = True
            index += 1
            continue
        if char == "[":
            in_class = True
            out.append(char)
            index += 1
            continue
        if char == "]":
            in_class = False
            out.append(char)
            index += 1
            continue
        if char == " " and not in_class:
            run_start = index
            while index < len(pattern) and pattern[index] == " ":
                index += 1
            # Preserve an existing captured variable-space group such as
            # ``( +)``; it already has the intended language and group index.
            if run_start > 0 and pattern[run_start - 1] == "(" and index < len(pattern) and pattern[index] == "+":
                out.append(pattern[run_start:index])
                continue
            out.append("[ ]+")
            continue
        out.append(char)
        index += 1
    return "".join(out)


def literal_placeholder_affixes(replacement: str) -> tuple[str, str] | None:
    """Return stable bytes around one placeholder-only replacement route."""
    if replacement.count("{placeholder}") != 1 or re.search(r"\{group\d+\}", replacement):
        return None
    prefix, suffix = replacement.split("{placeholder}", 1)
    if "{" in prefix + suffix or "}" in prefix + suffix:
        return None
    return prefix, suffix


def width_agnostic_numeric_replay_spec(spec: CandidateSpec) -> CandidateSpec:
    """Generalize display layout while retaining one semantic value stream."""
    if spec.semantic_class not in {
        "class_1_composed_numeric",
        "class_2_formatted_numeric",
    }:
        return spec
    if spec.kind != "open_function" or str(spec.program.get("op", "")) != "python_exec":
        return spec
    if spec.store_group != 0:
        return spec
    affixes = literal_placeholder_affixes(spec.replacement)
    if affixes is None:
        return spec
    relaxed_pattern = relax_literal_space_runs(relax_timestamp_digit_quantifiers(spec.pattern))
    program = dict(spec.program)
    group_regex = str(program.get("group_regex", ""))
    relaxed_group_regex = (
        relax_literal_space_runs(relax_timestamp_digit_quantifiers(group_regex))
        if group_regex
        else group_regex
    )
    if relaxed_pattern == spec.pattern and relaxed_group_regex == group_regex:
        return spec
    program["group_regex"] = relaxed_group_regex
    program["_layout_polymorphic_shared_delta"] = True
    program["_layout_placeholder_prefix"] = affixes[0]
    program["_layout_placeholder_suffix"] = affixes[1]
    relaxed = clone_spec(spec)
    relaxed.pattern = relaxed_pattern
    relaxed.program = program
    return relaxed


def python_exec_reads_one_raw_group(program: dict[str, object]) -> bool:
    """Return whether a generated forward function consumes only groups[0]."""
    code = str(program.get("code", ""))
    if "groups[0]" not in code:
        return False
    if re.search(r"groups\[[1-9]\d*\]", code):
        return False
    if re.search(r"=\s*groups(?:\s|$)", code):
        return False
    return True


def generalized_timestamp_width_spec(spec: CandidateSpec) -> CandidateSpec | None:
    """Compile a time-like whole-span Python function into a width-safe form."""
    if semantic_key_class(spec.semantic_key) != "time":
        return None
    if spec.kind != "open_function" or spec.replacement != "{placeholder}" or spec.store_group != 0:
        return None
    if str(spec.program.get("op", "")) != "python_exec":
        return None
    relaxed_pattern = relax_timestamp_digit_quantifiers(spec.pattern)
    if relaxed_pattern == spec.pattern:
        return None

    program = dict(spec.program)
    code = str(program.get("code", ""))
    if "_apply_digit_widths" in code and "_apply_space_lengths" in code:
        group_regex = str(program.get("group_regex", ""))
        program["group_regex"] = (
            relax_timestamp_digit_quantifiers(group_regex)
            if group_regex
            else "^(" + strip_outer_regex_anchors(relaxed_pattern) + ")$"
        )
        program["_timestamp_width_generalized"] = True
    else:
        if not python_exec_reads_one_raw_group(program):
            return None
        program.pop("group_regex", None)
        wrapped = space_layout_python_exec_program(
            relaxed_pattern,
            program,
            accept_existing_space_groups=True,
        )
        if wrapped is None:
            return None
        wrapped["_timestamp_width_generalized"] = True
        program = wrapped

    generalized = clone_spec(spec)
    generalized.pattern = relaxed_pattern
    generalized.program = program
    if not dataset_extract.is_open_function_program_safe(generalized.program):
        return None
    return generalized


def semantic_key_allowed_by_args(semantic_key: str, args: argparse.Namespace) -> bool:
    if args.no_level_global_replay and semantic_key.startswith("level:"):
        return False
    return True


def looks_like_syslog_timestamp_seed(spec: CandidateSpec) -> bool:
    """Detect LLM-discovered month/day/time seeds that should anti-unify.

    The LLM still has to discover a time field first.  This helper only turns a
    narrow seed such as ``^(\w{3} \d{1,2} \d{2}:\d{2}:\d{2})`` into the
    standard syslog timestamp language so day/month variants do not splinter the
    same semantic stream.
    """
    if not spec.semantic_key.startswith(("time:", "raw_time:")):
        return False
    pattern = spec.pattern
    program_payload = json.dumps(spec.program, sort_keys=True)
    if "%Y" in program_payload or "\\d{4}" in pattern:
        return False
    has_hms = r"\d{2}:\d{2}:\d{2}" in pattern
    has_month = (
        "\\w{3}" in pattern
        or "(?:Jan" in pattern
        or any(month in pattern for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"])
    )
    return has_hms and has_month


def looks_like_month_day_hms_seed(spec: CandidateSpec) -> bool:
    if not spec.semantic_key.startswith(("time:", "raw_time:")):
        return False
    pattern = spec.pattern
    return r"\d{2}\.\d{2}" in pattern and r"\d{2}:\d{2}:\d{2}" in pattern


def looks_like_iso_fraction_timestamp_seed(spec: CandidateSpec) -> bool:
    """Detect ISO-like timestamps that should use numeric subsecond deltas.

    This is a verifier-guided repair path, not a dataset profile: the LLM/cache
    must first label a field as time-like, and the generalized scanner is only
    used when the seed pattern already exposes a four-digit year, HMS, and a
    fractional-second separator.
    """
    if not spec.semantic_key.startswith(("time:", "raw_time:")):
        return False
    pattern = spec.pattern
    program_payload = json.dumps(spec.program, sort_keys=True)
    has_date = r"\d{4}" in pattern or "%Y" in program_payload
    has_hms = r"\d{2}:\d{2}:\d{2}" in pattern or "%H:%M:%S" in program_payload
    has_fraction = r",\d" in pattern or r"\.\d" in pattern or "%f" in program_payload
    return has_date and has_hms and has_fraction


def global_replay_spec(spec: CandidateSpec) -> CandidateSpec:
    """Generalize a learned semantic seed into a dataset-independent scanner.

    This is deliberately narrower than a hand-written dataset profile: the LLM
    must first discover the semantic field, then the compressor anti-unifies
    obvious lexical variants of that field so high-impact streams are not lost
    because the first example happened to contain only one concrete value.
    """
    if spec.kind == "open_function" and dataset_extract.is_open_function_program_safe(spec.program):
        # In the executable-function mainline, the LLM program is the semantic
        # transform.  Do not replace it with a historical built-in codec during
        # global replay; doing so would turn the method back into predefined-op
        # selection and can also shadow the verified Python transform.
        generalized = generalized_timestamp_width_spec(spec)
        return generalized if generalized is not None else clone_spec(spec)
    if looks_like_syslog_timestamp_seed(spec):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"^(" + dataset_extract.SYSLOG_TS_RE + r")",
            replacement="{placeholder}",
            program={"op": "syslog_delta"},
            store_group=1,
            kind="open_function",
            semantic_key=spec.semantic_key,
        )
    if looks_like_month_day_hms_seed(spec):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"\[(\d{2}\.\d{2} \d{2}:\d{2}:\d{2})\]",
            replacement="[{placeholder}]",
            program={"op": "month_day_hms_delta"},
            store_group=1,
            kind="open_function",
            semantic_key=spec.semantic_key,
        )
    if looks_like_iso_fraction_timestamp_seed(spec):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{1,6})",
            replacement="{placeholder}",
            program={
                "op": "datetime_strptime_delta",
                "format": "%Y-%m-%d %H:%M:%S,%f",
                "render_format": "%Y-%m-%d %H:%M:%S,%f",
            },
            store_group=1,
            kind="open_function",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("duration:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(lifetime )(<\d+ sec|\d{1,2}:\d{2}(?::\d{2})?)",
            replacement="{group1}{placeholder}",
            program={"op": "auto_codec"},
            store_group=2,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("hostport:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(?<![\w.-])([A-Za-z0-9][A-Za-z0-9.-]*:\d{1,5})(?![\w.-])",
            replacement="{placeholder}",
            program={"op": "auto_codec"},
            store_group=1,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("bytes_sent:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(\s)(\d+ bytes(?: \([^)]+\))?)( sent,)",
            replacement="{group1}{placeholder}{group3}",
            program={"op": "auto_codec"},
            store_group=2,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("bytes_received:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(\s)(\d+ bytes(?: \([^)]+\))?)( received,)",
            replacement="{group1}{placeholder}{group3}",
            program={"op": "auto_codec"},
            store_group=2,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("level:"):
        # Apache showed that extracting every bracketed level can be negative:
        # [error] and [notice] are already cheap template anchors. Keep the
        # LLM-discovered seed unless a later MDL gate proves a broader rewrite.
        return spec
    if spec.semantic_key.startswith("ipv4:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(?<![\d.])((?:\d{1,3}\.){3}\d{1,3})(?![\d.])",
            replacement="{placeholder}",
            program={"op": "auto_codec"},
            store_group=1,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("port:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(?<!\w)(port\s+)(\d{1,5})(?=\D|$)",
            replacement="{group1}{placeholder}",
            program={"op": "auto_codec"},
            store_group=2,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("pid:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(\b[A-Za-z_][\w.-]*\[)(\d{1,8})(\]:)",
            replacement="{group1}{placeholder}{group3}",
            program={"op": "int_expr_delta", "value_regex": r"^(\d+)$", "value_expr": "g1", "render": "{v}"},
            store_group=2,
            kind="open_function",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("user:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"((?:invalid user |for invalid user |for ))([^\s\[]+)(?=(?: from| \[preauth\]))",
            replacement="{group1}{placeholder}",
            program={"op": "auto_codec"},
            store_group=2,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    if spec.semantic_key.startswith("domain:"):
        return CandidateSpec(
            tag=spec.tag,
            pattern=r"(getaddrinfo for )([^\s\[]+)( \[)",
            replacement="{group1}{placeholder}{group3}",
            program={"op": "auto_codec"},
            store_group=2,
            kind="auto",
            semantic_key=spec.semantic_key,
        )
    return spec


def semantic_replay_preempts_early_global(spec: CandidateSpec) -> bool:
    """Return whether a learned time function should block early global replay.

    A narrow learned timestamp regex can be perfectly reversible for the spans
    it matches while still missing sibling layouts such as ``Dec 10`` versus
    ``Jan  1``.  Therefore timestamp-like Python functions no longer preempt the
    layout-preserving early-global candidate merely because they pass local
    reconstruction.  The global/replay admission logic can still choose the
    learned function if it wins by coverage and cost.
    """
    if semantic_key_class(spec.semantic_key) != "time":
        return False
    if spec.kind != "open_function":
        return False
    op = str((spec.program or {}).get("op", ""))
    if op == "python_exec" and dataset_extract.is_open_function_program_safe(spec.program):
        return False
    return op in {
        "datetime_strptime_delta",
        "month_day_hms_delta",
        "hms_delta",
        "day_hms_delta",
    }


REGEX_ESCAPED_CLASS_PREFIXES = set("AbBdDsSwWZ")


def regex_literal_features(pattern: str) -> dict[str, object]:
    """Extract generic routing features from a regex without semantic labels.

    The router is only a cheap prefilter.  Exact replay validation remains the
    correctness boundary, so this intentionally uses conservative observable
    features: literal anchors, literal punctuation, and broad character needs.
    """
    anchors: list[str] = []
    punct: set[str] = set()
    buf: list[str] = []
    in_class = False
    escaped = False
    needs_digit = False
    needs_alpha = False

    def flush() -> None:
        if not buf:
            return
        token = "".join(buf).lower()
        if len(token) >= 3:
            anchors.append(token)
        buf.clear()

    for ch in pattern:
        if escaped:
            escaped = False
            if ch in REGEX_ESCAPED_CLASS_PREFIXES:
                if ch in {"d", "D"}:
                    needs_digit = True
                if ch in {"w", "W"}:
                    needs_alpha = True
                flush()
                continue
            if ch.isalnum():
                buf.append(ch)
            else:
                flush()
                punct.add(ch)
            continue
        if ch == "\\":
            escaped = True
            continue
        if in_class:
            if ch == "]":
                in_class = False
            elif ch.isdigit():
                needs_digit = True
            elif ch.isalpha():
                needs_alpha = True
            elif ch not in {"^", "-"}:
                punct.add(ch)
            flush()
            continue
        if ch == "[":
            in_class = True
            flush()
            continue
        if ch.isalnum():
            if ch.isdigit():
                needs_digit = True
            if ch.isalpha():
                needs_alpha = True
            buf.append(ch)
        else:
            flush()
            if ch not in {"(", ")", "?", "*", "+", "{", "}", "|", "^", "$", "."}:
                punct.add(ch)
    flush()
    return {
        "anchors": tuple(sorted(set(anchors), key=lambda item: (-len(item), item))),
        "punct": frozenset(punct),
        "needs_digit": needs_digit,
        "needs_alpha": needs_alpha,
    }


REGEX_SYNTAX_CHARS = frozenset(".^$*+?{}()|")
REGEX_CLASS_ESCAPES = frozenset("AbBdDsSwWZzGfnrtv")


def regex_has_fixed_alpha_or_structure(pattern: str) -> bool:
    """Require an observable literal anchor in an LLM-generated regex.

    Character classes and regex operators describe broad sets rather than
    bytes guaranteed to occur in a match. They therefore do not count. A
    literal alphabetic character outside a class, or literal non-whitespace
    punctuation such as ``:``, ``/``, ``-``, or an escaped ``.`` does count.
    """
    index = 0
    in_class = False
    while index < len(pattern):
        ch = pattern[index]
        if in_class:
            if ch == "\\":
                index += 2
                continue
            if ch == "]":
                in_class = False
            index += 1
            continue
        if ch == "[":
            in_class = True
            index += 1
            continue
        if ch == "\\":
            index += 1
            if index >= len(pattern):
                break
            escaped = pattern[index]
            if escaped == "x":
                index += 3
                continue
            if escaped == "u":
                index += 5
                continue
            if escaped == "U":
                index += 9
                continue
            if escaped == "N":
                closing = pattern.find("}", index + 1)
                index = len(pattern) if closing < 0 else closing + 1
                continue
            if escaped not in REGEX_CLASS_ESCAPES and not escaped.isdigit():
                if escaped.isalpha() or (not escaped.isalnum() and not escaped.isspace()):
                    return True
            index += 1
            continue
        if ch == "(" and index + 1 < len(pattern) and pattern[index + 1] == "?":
            index += 2
            if pattern.startswith("<=", index) or pattern.startswith("<!", index):
                index += 2
                continue
            if index < len(pattern) and pattern[index] in ":=!>":
                index += 1
                continue
            if index < len(pattern) and pattern[index] == "#":
                closing = pattern.find(")", index + 1)
                index = len(pattern) if closing < 0 else closing + 1
                continue
            while index < len(pattern) and pattern[index] in "aiLmsux-":
                index += 1
            if index < len(pattern) and pattern[index] in ":)":
                index += 1
            continue
        if ch.isalpha():
            return True
        if not ch.isalnum() and not ch.isspace() and ch not in REGEX_SYNTAX_CHARS:
            return True
        index += 1
    return False


def record_regex_structure_rejection(
    args: argparse.Namespace,
    *,
    source: str,
    tag: str,
    pattern: str,
) -> None:
    stats = getattr(args, "_regex_structure_anchor_stats", None)
    if not isinstance(stats, dict):
        stats = {
            "enabled": int(bool(getattr(args, "require_regex_structure_anchor", False))),
            "proposal_rejected": 0,
            "replay_rejected": 0,
            "rejected": [],
        }
        setattr(args, "_regex_structure_anchor_stats", stats)
    key = "proposal_rejected" if source == "proposal" else "replay_rejected"
    stats[key] = int(stats.get(key, 0)) + 1
    rejected = stats.setdefault("rejected", [])
    if isinstance(rejected, list) and len(rejected) < 128:
        rejected.append({"source": source, "tag": tag, "pattern": pattern})


def filter_replay_specs_by_regex_structure(
    specs: list[CandidateSpec],
    args: argparse.Namespace,
) -> list[CandidateSpec]:
    if not getattr(args, "require_regex_structure_anchor", False):
        return specs
    kept: list[CandidateSpec] = []
    for spec in specs:
        if is_stage_residual_spec(spec) or regex_has_fixed_alpha_or_structure(spec.pattern):
            kept.append(spec)
            continue
        record_regex_structure_rejection(
            args,
            source="replay",
            tag=spec.tag,
            pattern=spec.pattern,
        )
    return kept


def replacement_literal_features(replacement: str) -> dict[str, object]:
    scrubbed = (
        replacement
        .replace("{placeholder}", " ")
    )
    scrubbed = re.sub(r"\{group\d+\}", " ", scrubbed)
    anchors = tuple(
        sorted(
            {part.lower() for part in re.findall(r"[A-Za-z0-9_./:-]{3,}", scrubbed)},
            key=lambda item: (-len(item), item),
        )
    )
    punct = frozenset(ch for ch in scrubbed if ch in string.punctuation)
    return {
        "anchors": anchors,
        "punct": punct,
        "needs_digit": any(ch.isdigit() for ch in scrubbed),
        "needs_alpha": any(ch.isalpha() for ch in scrubbed),
    }


def function_router_features(spec: CandidateSpec) -> dict[str, object]:
    pattern_features = regex_literal_features(spec.pattern)
    replacement_features = replacement_literal_features(spec.replacement)
    anchors = tuple(
        sorted(
            set(pattern_features["anchors"]) | set(replacement_features["anchors"]),
            key=lambda item: (-len(item), item),
        )
    )
    punct = frozenset(set(pattern_features["punct"]) | set(replacement_features["punct"]))
    return {
        "anchors": anchors,
        "punct": punct,
        "needs_digit": bool(pattern_features["needs_digit"] or replacement_features["needs_digit"]),
        "needs_alpha": bool(pattern_features["needs_alpha"] or replacement_features["needs_alpha"]),
        "pattern_len": len(spec.pattern),
    }


def global_function_router_select(
    line: str,
    specs: list[CandidateSpec],
    feature_cache: dict[str, dict[str, object]],
    *,
    max_functions: int,
    min_score: float,
    fallback_full: bool,
) -> tuple[list[CandidateSpec], int, int]:
    """Select plausible global functions using only generic line/function features.

    Returns selected specs, scored candidate count, and whether full fallback was
    used.  The selected functions are still replay-verified later.
    """
    if not specs:
        return [], 0, 0
    lower_line = line.lower()
    line_punct = set(ch for ch in line if ch in string.punctuation)
    line_has_digit = any(ch.isdigit() for ch in line)
    line_has_alpha = any(ch.isalpha() for ch in line)
    scored: list[tuple[float, int, CandidateSpec]] = []
    for index, spec in enumerate(specs):
        features = feature_cache.get(spec.tag)
        if features is None:
            features = function_router_features(spec)
            feature_cache[spec.tag] = features
        anchors = tuple(features["anchors"])
        punct = set(features["punct"])
        anchor_hits = sum(1 for anchor in anchors if anchor in lower_line)
        anchor_score = 0.0
        if anchors:
            anchor_score = 6.0 * anchor_hits / max(1, min(len(anchors), 4))
            if anchor_hits == 0 and len(anchors[0]) >= 4:
                continue
        punct_overlap = len(punct & line_punct)
        punct_score = 0.0
        if punct:
            punct_score = 2.0 * punct_overlap / max(1, min(len(punct), 6))
            if punct_overlap == 0 and not anchors:
                continue
        shape_score = 0.0
        if features["needs_digit"]:
            if not line_has_digit:
                continue
            shape_score += 1.0
        if features["needs_alpha"]:
            if not line_has_alpha:
                continue
            shape_score += 0.5
        score = anchor_score + punct_score + shape_score + min(float(features["pattern_len"]) / 256.0, 1.0)
        if score >= min_score:
            scored.append((score, index, spec))
    if not scored:
        if fallback_full:
            return specs, 0, 1
        return [], 0, 0
    scored.sort(key=lambda item: (-item[0], item[1]))
    if max_functions > 0:
        scored = scored[:max_functions]
    return [spec for _, _, spec in scored], len(scored), 0


def replacement_context_priority(replacement: str) -> tuple[int, int, int]:
    """Prefer extractors whose replacement keeps stable context on both sides."""
    before, marker, after = replacement.partition("{placeholder}")
    if not marker:
        return (0, 0, 0)
    before_literal = re.sub(r"\{group\d+\}", "", before)
    after_literal = re.sub(r"\{group\d+\}", "", after)
    before_len = len(before_literal.strip())
    after_len = len(after_literal.strip())
    has_both = int(before_len > 0 and after_len > 0)
    return (has_both, before_len + after_len, after_len)


def global_replay_span_priority(spec: CandidateSpec, text: str, max_matches: int = 2048) -> tuple[int, int, int, int, float, int, str]:
    """Rank overlapping global extractors by the concrete span they consume.

    Generic scout functions can overlap.  For example, an IPv4 extractor can
    match inside a host:port endpoint, while a broad ``to X`` function can
    preempt a more specific ``to X at`` function.  Before replaying global
    functions, prefer extractors that preserve stable literal context on both
    sides of the placeholder, then functions that consume longer concrete spans.
    This remains dataset-blind: it uses only the verified replacement shape and
    observed match lengths.
    """
    total = 0
    count = 0
    max_span = 0
    try:
        regex = compiled_multiline_regex(spec.pattern)
        for match in regex.finditer(text):
            span_len = len(match.group(0))
            total += span_len
            max_span = max(max_span, span_len)
            count += 1
            if count >= max_matches:
                break
    except Exception:
        count = 0
    average_span = (total / count) if count else 0.0
    return (*replacement_context_priority(spec.replacement), max_span, average_span, len(spec.pattern), spec.tag)


def stream_proxy_score(values: list[str], placeholder: str) -> int:
    if not values:
        return -10**9
    raw_main_bytes = sum(len(value.encode("latin-1")) for value in values)
    placeholder_bytes = len(placeholder.encode("latin-1")) * len(values)
    try:
        side_cost = min(cost for _kind, cost in dataset_extract.stream_codec_candidates(values))
    except Exception:
        side_cost = len(lzma.compress("\n".join(values).encode("latin-1", errors="ignore")))
    return raw_main_bytes - placeholder_bytes - side_cost


CANONICAL_SYSLOG_DATETIME_RE = re.compile(r"^[A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2}$")


def specialize_global_rescue_codec(candidate: CandidateSpec, values: list[str]) -> None:
    """Choose the lightest exact codec for a globally rescued value stream."""
    if candidate.kind != "open_function":
        return
    if str((candidate.program or {}).get("op", "")) != "syslog_delta":
        return
    if values and all(CANONICAL_SYSLOG_DATETIME_RE.fullmatch(value) for value in values):
        candidate.program = {
            "op": "datetime_strptime_delta",
            "format": "%b %d %H:%M:%S",
            "render_format": "%b %d %H:%M:%S",
        }


def stream_proxy_is_stable(values: list[str], placeholder: str, min_score: int = 0) -> bool:
    """Reject streams whose gain is only an artifact of one slice of the block.

    This is a lightweight held-out MDL check.  It uses the same pre-compression
    stream proxy as the main local-stream gate, but requires the first and second
    halves to both clear the threshold.  It deliberately avoids archive-level
    profile racing while catching fragile, low-support, or overly broad local
    functions.
    """
    if len(values) < 4:
        return stream_proxy_score(values, placeholder) >= min_score
    mid = len(values) // 2
    first = values[:mid]
    second = values[mid:]
    return (
        stream_proxy_score(first, placeholder) >= min_score
        and stream_proxy_score(second, placeholder) >= min_score
    )


def values_are_numeric_like(values: list[str]) -> bool:
    if not values:
        return False
    sample = values[: min(len(values), 256)]
    return all(re.fullmatch(r"-?\d+", value) for value in sample)


@contextmanager
def proxy_lzma_preset_scope(preset_text: str | None):
    """Temporarily change proxy-only LZMA cost precision."""
    if not preset_text:
        yield
        return
    previous = os.environ.get("PARE_PROXY_LZMA_PRESET")
    os.environ["PARE_PROXY_LZMA_PRESET"] = preset_text
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("PARE_PROXY_LZMA_PRESET", None)
        else:
            os.environ["PARE_PROXY_LZMA_PRESET"] = previous


def choose_numeric_lattice_proxy_preset(transformed_text: str) -> tuple[str | None, dict[str, int]]:
    """Choose proxy precision from block structure, not dataset identity."""
    if not os.environ.get("PARE_NUMERIC_LATTICE_PROXY_PRESET_AUTO"):
        return None, {}
    stats = cheap_numeric_dominance_stats(transformed_text)
    before_templates = int(stats.get("before_templates", 0))
    line_count = int(len(dataset_extract.split_physical_lines(transformed_text)))
    # Very high template cardinality blocks are dominated by repeated numeric
    # structure.  Low-cardinality residuals can still contain semantically
    # important splits, so they keep the default proxy precision.
    if before_templates >= 80_000:
        preset = os.environ.get("PARE_NUMERIC_LATTICE_PROXY_PRESET_AUTO_VALUE", "1")
        return preset, {"before_templates": before_templates, "line_count": line_count}
    return None, {"before_templates": before_templates, "line_count": line_count}


def candidate_specificity(candidate: ProgramCandidate) -> tuple[int, int, int]:
    """Prefer programs that explain a richer local structure.

    The cache lookup is deliberately validation-first, but many programs can be
    valid on the same line.  A singleton constant extractor should not shadow a
    richer group containing path/numeric slots.  Specificity is therefore based
    on number of functions, pattern mass, and observed support.
    """
    return (
        len(candidate.specs),
        sum(len(spec.pattern) for spec in candidate.specs),
        candidate.support,
    )


def specs_match_line(line: str, specs: list[CandidateSpec]) -> bool:
    ok, _reason = validate_spec_program_on_lines([line], specs)
    return ok


def checked_apply_specs_to_line(
    line: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    protected_placeholders: list[str] | None = None,
) -> tuple[str, dict[str, list[str]]] | None:
    """Strict single-line replay used by cache/reindex scoring.

    This fuses the previous validate-then-apply pair for one line.  It returns
    the transformed line and the ordered side-stream appends, or ``None`` if
    the ordered program is not byte-exact on this concrete line.
    """
    if not specs:
        return None
    protected_placeholders = protected_placeholders or []
    spec_placeholders = {
        spec.tag: placeholders[spec.tag]
        for spec in specs
        if spec.tag in placeholders
    }
    if len(spec_placeholders) != len(specs):
        return None
    values_by_tag: dict[str, list[str]] = {}
    transformed = line
    try:
        for spec in specs:
            regex = compiled_multiline_regex(spec.pattern)
            placeholder = spec_placeholders[spec.tag]
            previous_counts = {
                existing: transformed.count(existing)
                for existing in protected_placeholders
            }

            def replace(match: re.Match[str]) -> str:
                if any(existing in match.group(0) for existing in protected_placeholders):
                    raise ValueError(f"function {spec.tag} consumed protected placeholder")
                raw_value = match.group(spec.store_group)
                if raw_value is None:
                    raise ValueError(f"empty store group for tag {spec.tag}")
                rendered_value = validation_render_value(spec, raw_value)
                if format_replacement_fast(spec.replacement, rendered_value, match) != match.group(0):
                    raise ValueError(f"non-invertible replacement for tag {spec.tag}")
                values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
                return format_replacement_fast(spec.replacement, placeholder, match)

            transformed = regex.sub(replace, transformed)
            for existing, previous_count in previous_counts.items():
                if transformed.count(existing) != previous_count:
                    return None
        for spec in specs:
            if not values_by_tag.get(spec.tag):
                return None
        if restore_validation_text(transformed, specs, spec_placeholders, values_by_tag) != line:
            return None
    except Exception:
        return None
    return transformed, values_by_tag


def validated_line_program_score(
    line: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    support: int,
    protected_placeholders: list[str] | None = None,
) -> float | None:
    protected_placeholders = protected_placeholders or []
    before_counts = {placeholder: line.count(placeholder) for placeholder in protected_placeholders}
    checked = checked_apply_specs_to_line(
        line,
        specs,
        placeholders,
        protected_placeholders=protected_placeholders,
    )
    if checked is None:
        return None
    transformed, values_by_tag = checked
    for placeholder, count in before_counts.items():
        if transformed.count(placeholder) != count:
            return None
    if any(
        placeholder in value
        for values in values_by_tag.values()
        for value in values
        for placeholder in protected_placeholders
    ):
        return None
    if not values_by_tag:
        return None
    longest_value = max((len(value) for values in values_by_tag.values() for value in values), default=0)
    if len(line) > 0 and longest_value / len(line) > 0.85:
        return None
    marginal_main_gain = len(line.encode("latin-1")) - len(transformed.encode("latin-1"))
    return marginal_main_gain + 4.0 * math.log1p(support) + 0.25 * len(specs)


def canonical_class(spec: CandidateSpec) -> str:
    pattern = spec.pattern.lower()
    if "a-za-z" in pattern and r"\d{2}:\d{2}:\d{2}" in pattern:
        return "apache_timestamp"
    if "client" in pattern and "0-9" in pattern and r"\." in pattern:
        return "client_ipv4"
    if "/" in pattern and (
        "file does not exist" in pattern
        or "directory index" in pattern
        or "script" in pattern
        or "[^\\s]+" in pattern
        or r"\w" in pattern
    ):
        return "absolute_path"
    if r"\[" in pattern and (
        "error|warn" in pattern
        or "notice|error" in pattern
        or "info|debug" in pattern
        or "notice" in pattern
    ):
        return "apache_level"
    return ""


def is_level_like_spec(spec: CandidateSpec) -> bool:
    if spec.semantic_key.startswith("level:"):
        return True
    if canonical_class(spec) == "apache_level":
        return True
    pattern_l = spec.pattern.lower()
    replacement_l = spec.replacement.lower()
    bracketed_alpha = (
        "\\[" in pattern_l
        and "\\]" in pattern_l
        and ("a-z" in pattern_l or "\\w" in pattern_l or "alpha" in pattern_l)
        and "\\d" not in pattern_l
        and "/" not in pattern_l
    )
    return (
        bracketed_alpha
        or (
            "\\[" in pattern_l
            and "\\]" in pattern_l
            and any(level in pattern_l or level in replacement_l for level in ["error", "notice", "warn", "debug", "info"])
        )
    )


def canonical_global_specs() -> list[CandidateSpec]:
    return []


def rebuild_with_canonical_globals(
    original_lines: list[str],
    original_text: str,
    families: list[Family],
    accepted_specs: list[CandidateSpec],
) -> tuple[list[str], dict[str, list[str]], dict[str, str], list[CandidateSpec], int, int]:
    return original_lines, {}, {}, accepted_specs, 0, 0


def compile_family_specs(
    family: Family,
    payload: dict[str, Any],
    transformed_lines: list[str],
    args: argparse.Namespace,
    tag_start: int,
) -> tuple[list[CandidateSpec], str]:
    payload = normalize_classwise_family_payload(payload)
    functions = payload.get("functions", [])
    if not isinstance(functions, list):
        functions = []
    header_payload = payload.get("header", {})
    header_functions: list[Any] = []
    if isinstance(header_payload, dict):
        raw_header_functions = header_payload.get("functions", [])
        if isinstance(raw_header_functions, list):
            header_functions = raw_header_functions
    if header_functions:
        functions = [*header_functions, *functions]
    if not functions:
        return [], "payload has no functions list"
    if getattr(args, "slot_complete_planner", False):
        slot_stats = getattr(args, "_slot_complete_stats", None)
        if not isinstance(slot_stats, dict):
            slot_stats = {
                "payloads": 0,
                "payloads_with_slot_actions": 0,
                "slot_actions": 0,
                "required_slot_occurrences": 0,
            }
            setattr(args, "_slot_complete_stats", slot_stats)
        slot_stats["payloads"] = int(slot_stats.get("payloads", 0)) + 1
        _, required_count = slot_complete_required_slots(
            family.examples,
            max_examples=args.examples_per_family,
            semantic_token_shapes=not getattr(args, "generic_token_shapes", False),
        )
        slot_stats["required_slot_occurrences"] = int(slot_stats.get("required_slot_occurrences", 0)) + required_count
        slot_actions = payload.get("slot_actions", [])
        if isinstance(slot_actions, list):
            slot_stats["slot_actions"] = int(slot_stats.get("slot_actions", 0)) + len(slot_actions)
            if slot_actions:
                slot_stats["payloads_with_slot_actions"] = int(slot_stats.get("payloads_with_slot_actions", 0)) + 1
    specs: list[CandidateSpec] = []
    bundle_candidates: list[CandidateSpec] = []
    bundle_mode = bool(getattr(args, "program_bundle_mdl_admission", False))
    validation_lines = [transformed_lines[index] for index in family.line_indexes[: args.verify_lines]]
    validation_text = "".join(validation_lines)
    rejected_reasons: list[str] = []
    source_functions = functions[: args.max_functions_per_family]
    if getattr(args, "contextualize_broad_llm_regex", False):
        contextual_stats = getattr(args, "_contextualize_broad_regex_stats", None)
        if not isinstance(contextual_stats, dict):
            contextual_stats = {
                "families": 0,
                "source_functions": 0,
                "variant_functions": 0,
                "internal_capture_variants": 0,
                "value_contextual_variants": 0,
            }
            setattr(args, "_contextualize_broad_regex_stats", contextual_stats)
        contextual_stats["families"] = int(contextual_stats.get("families", 0)) + 1
        contextual_stats["source_functions"] = int(contextual_stats.get("source_functions", 0)) + len(source_functions)
        expanded_functions: list[dict[str, Any]] = []
        for source_item in source_functions:
            if not isinstance(source_item, dict):
                continue
            internal_variants = internal_value_capture_llm_regex_items(source_item)
            contextual_stats["internal_capture_variants"] = int(
                contextual_stats.get("internal_capture_variants", 0)
            ) + len(internal_variants)
            value_contextual_variants: list[dict[str, Any]] = []
            for value_source_item in [*internal_variants, source_item]:
                value_contextual_variants.extend(
                    contextualize_value_capture_llm_regex_items(value_source_item, validation_text)
                )
            contextual_stats["value_contextual_variants"] = int(
                contextual_stats.get("value_contextual_variants", 0)
            ) + len(value_contextual_variants)
            variants = contextualize_broad_llm_regex_items(source_item, validation_text)
            contextual_stats["variant_functions"] = int(contextual_stats.get("variant_functions", 0)) + len(variants)
            expanded_functions.extend(value_contextual_variants)
            expanded_functions.extend(internal_variants)
            expanded_functions.extend(variants)
            expanded_functions.append(source_item)
        functions_to_compile = expanded_functions[: max(args.max_functions_per_family * 4, args.max_functions_per_family)]
    else:
        functions_to_compile = source_functions
    space_layout_expanded: list[Any] = []
    for item in functions_to_compile:
        if isinstance(item, dict):
            repaired_item = deterministic_space_layout_repair_item(item)
            if repaired_item is not None:
                space_layout_expanded.append(repaired_item)
        space_layout_expanded.append(item)
    functions_to_compile = space_layout_expanded
    function_trace = getattr(args, "_function_compile_trace", None)
    if not isinstance(function_trace, list):
        function_trace = []
        setattr(args, "_function_compile_trace", function_trace)
    verifier_repair_stats = getattr(args, "_verifier_repair_stats", None)
    if not isinstance(verifier_repair_stats, dict):
        verifier_repair_stats = {
            "attempts": 0,
            "candidate_items": 0,
            "errors": 0,
        }
        setattr(args, "_verifier_repair_stats", verifier_repair_stats)
    if getattr(args, "bare_space_repair", False):
        repaired_functions: list[Any] = []
        for item in functions_to_compile:
            if not isinstance(item, dict):
                continue
            pattern = str(item.get("regex", ""))
            if not regex_has_literal_ascii_space(pattern):
                repaired_functions.append(item)
                continue
            repair_trace: dict[str, Any] = {
                "family_id": family.family_id,
                "support": len(family.line_indexes),
                "source_tag": str(item.get("tag", "")),
                "meaning": str(item.get("meaning", "")),
                "regex": pattern,
                "replacement": str(item.get("replacement", "{placeholder}")),
                "status": "bare_space_repair_requested",
                "reasons": ["regex contains literal ASCII space"],
            }
            try:
                repair_payload = bare_space_repair_payload_for_item(
                    family=family,
                    item=item,
                    args=args,
                )
                repair_items = repair_payload.get("functions", [])
                if (
                    not isinstance(repair_items, list)
                    and isinstance(repair_payload.get("regex"), str)
                    and isinstance(repair_payload.get("program"), dict)
                ):
                    repair_items = [repair_payload]
                elif (
                    isinstance(repair_items, list)
                    and not repair_items
                    and isinstance(repair_payload.get("regex"), str)
                    and isinstance(repair_payload.get("program"), dict)
                ):
                    repair_items = [repair_payload]
                if not isinstance(repair_items, list):
                    repair_items = []
                accepted_repair_items = 0
                for repair_item in repair_items:
                    if not isinstance(repair_item, dict):
                        continue
                    repair_pattern = str(repair_item.get("regex", ""))
                    if regex_has_literal_ascii_space(repair_pattern):
                        continue
                    repaired = dict(repair_item)
                    repaired.setdefault("tag", item.get("tag", ""))
                    repaired.setdefault("meaning", item.get("meaning", ""))
                    repaired["_repair_origin"] = "bare_space_repair"
                    repaired_functions.append(repaired)
                    accepted_repair_items += 1
                repair_trace["status"] = (
                    "bare_space_repair_produced_candidates"
                    if accepted_repair_items
                    else "bare_space_repair_no_candidate"
                )
                repair_trace["repair_candidates"] = accepted_repair_items
            except Exception as exc:
                repair_trace["status"] = "bare_space_repair_error"
                repair_trace["reasons"].append(str(exc)[:300])
            function_trace.append(repair_trace)
        functions_to_compile = repaired_functions
    family_verifier_repairs = 0

    def maybe_enqueue_verifier_repair(
        *,
        item: dict[str, Any],
        status: str,
        reasons: list[str],
    ) -> None:
        nonlocal family_verifier_repairs
        if not getattr(args, "verifier_repair_agent", False):
            return
        if item.get("_verifier_repair_origin"):
            return
        max_total = int(getattr(args, "verifier_repair_max_total", 0) or 0)
        max_per_family = int(getattr(args, "verifier_repair_max_per_family", 0) or 0)
        if max_total > 0 and int(verifier_repair_stats.get("attempts", 0)) >= max_total:
            return
        if max_per_family > 0 and family_verifier_repairs >= max_per_family:
            return
        if status in {"pruned_constant_local_stream", "rejected_line_mdl"}:
            return
        repair_category = verifier_repair_category_for_item(item, status=status, reasons=reasons)
        if not repair_category:
            verifier_repair_stats["skipped_ineligible"] = int(
                verifier_repair_stats.get("skipped_ineligible", 0)
            ) + 1
            return
        verifier_repair_stats["attempts"] = int(verifier_repair_stats.get("attempts", 0)) + 1
        family_verifier_repairs += 1
        repair_trace: dict[str, Any] = {
            "family_id": family.family_id,
            "support": len(family.line_indexes),
            "source_tag": str(item.get("tag", "")),
            "meaning": str(item.get("meaning", "")),
            "regex": str(item.get("regex", "")),
            "replacement": str(item.get("replacement", "{placeholder}")),
            "status": "verifier_repair_requested",
            "failed_status": status,
            "repair_category": repair_category,
            "reasons": reasons,
        }
        try:
            repair_payload = verifier_repair_payload_for_item(
                family=family,
                item=item,
                status=status,
                reasons=reasons,
                validation_lines=validation_lines,
                args=args,
                repair_category=repair_category,
            )
            repair_items = repair_payload.get("functions", [])
            if (
                not isinstance(repair_items, list)
                and isinstance(repair_payload.get("regex"), str)
                and isinstance(repair_payload.get("program"), dict)
            ):
                repair_items = [repair_payload]
            if not isinstance(repair_items, list):
                repair_items = []
            accepted_repair_items = 0
            for repair_item in repair_items[: max(1, int(getattr(args, "verifier_repair_max_candidates", 2) or 2))]:
                if not isinstance(repair_item, dict):
                    continue
                repaired = dict(repair_item)
                repaired.setdefault("tag", item.get("tag", ""))
                repaired.setdefault("meaning", item.get("meaning", ""))
                repaired["_verifier_repair_origin"] = status
                repaired["_verifier_repair_category"] = repair_category
                functions_to_compile.append(repaired)
                accepted_repair_items += 1
            verifier_repair_stats["candidate_items"] = int(verifier_repair_stats.get("candidate_items", 0)) + accepted_repair_items
            repair_trace["status"] = (
                "verifier_repair_produced_candidates"
                if accepted_repair_items
                else "verifier_repair_no_candidate"
            )
            repair_trace["repair_candidates"] = accepted_repair_items
        except Exception as exc:
            verifier_repair_stats["errors"] = int(verifier_repair_stats.get("errors", 0)) + 1
            repair_trace["status"] = "verifier_repair_error"
            repair_trace["reasons"] = [*reasons, str(exc)[:300]]
        function_trace.append(repair_trace)

    item_index = 0
    while item_index < len(functions_to_compile):
        item = functions_to_compile[item_index]
        item_index += 1
        if not isinstance(item, dict):
            continue
        trace_record: dict[str, Any] = {
            "family_id": family.family_id,
            "support": len(family.line_indexes),
            "source_tag": str(item.get("tag", "")),
            "meaning": str(item.get("meaning", "")),
            "regex": str(item.get("regex", "")),
            "replacement": str(item.get("replacement", "{placeholder}")),
            "status": "started",
            "reasons": [],
        }
        pattern = str(item.get("regex", ""))
        replacement = str(item.get("replacement", "{placeholder}"))
        if (
            getattr(args, "semantic_three_class_program_prompt", False)
            and not llm_item_has_required_python_exec_contract(item)
        ):
            trace_record["status"] = "rejected_missing_python_exec_contract"
            trace_record["tag"] = str(item.get("tag", ""))
            trace_record["reasons"] = [
                "three-class mode requires program {'op':'python_exec','code':'...'} with def forward(groups) and def inverse(record)"
            ]
            maybe_enqueue_verifier_repair(
                item=item,
                status="rejected_missing_python_exec_contract",
                reasons=list(trace_record["reasons"]),
            )
            function_trace.append(trace_record)
            continue
        program = llm_item_program(item)
        context_projector_code = ""
        if getattr(args, "strict_three_class_context_schema", False):
            context_projector_code, context_schema_error = llm_context_projector_code(item)
            if context_schema_error:
                trace_record["status"] = "rejected_context_projector_schema"
                trace_record["reasons"] = [context_schema_error]
                maybe_enqueue_verifier_repair(
                    item=item,
                    status="rejected_context_projector_schema",
                    reasons=list(trace_record["reasons"]),
                )
                function_trace.append(trace_record)
                continue
        if not dataset_extract.is_open_llm_regex_safe(pattern):
            trace_record["status"] = "rejected_unsafe_regex"
            maybe_enqueue_verifier_repair(
                item=item,
                status="rejected_unsafe_regex",
                reasons=["regex failed safety check"],
            )
            function_trace.append(trace_record)
            continue
        if (
            getattr(args, "require_regex_structure_anchor", False)
            and not regex_has_fixed_alpha_or_structure(pattern)
        ):
            trace_record["status"] = "rejected_missing_regex_structure_anchor"
            trace_record["reasons"] = [
                "regex has neither a fixed alphabetic literal nor observable literal punctuation"
            ]
            record_regex_structure_rejection(
                args,
                source="proposal",
                tag=str(item.get("tag", "")),
                pattern=pattern,
            )
            function_trace.append(trace_record)
            continue
        verified_replacement = ""
        verified_program: dict[str, object] | None = None
        verified_store_group: int | None = None
        verified_origin = ""
        compiled_options: list[tuple[int, str, dict[str, object], int, str]] = []
        variants = (
            compiler_repair_variants(replacement, program, enable_cegis_repair=args.cegis_repair)
            if args.program_mdl_compiler
            else [
                (replacement_variant, program_variant, "first_valid")
                for replacement_variant, program_variant in payload_repair_variants(replacement, program)
            ]
        )
        group_program = python_exec_group_regex_variant(pattern, program)
        if group_program is not None and dataset_extract.is_open_function_program_safe(group_program):
            group_variants = [
                (replacement_variant, group_program, f"{origin}+python_exec_group_regex")
                for replacement_variant, _program_variant, origin in variants
            ]
            # Multi-capture python_exec functions need the full match replayed
            # through group_regex; otherwise forward(groups) sees only one
            # value and the later semantic contract check rejects a reversible
            # function.  Try these variants first in the non-MDL path.
            variants = [
                *group_variants,
                *variants,
            ]
        for replacement_variant, program_variant, origin in variants:
            if not dataset_extract.is_open_llm_replacement_safe(replacement_variant):
                continue
            if not dataset_extract.is_open_function_program_safe(program_variant):
                continue
            try:
                regex = re.compile(pattern, flags=re.MULTILINE)
                matches = list(regex.finditer(validation_text))
                if not matches:
                    continue
                store_group = dataset_extract.infer_open_function_store_group(matches[:128], program_variant, replacement_variant)
                if (
                    store_group is None
                    and str(program_variant.get("op", "")) == "python_exec"
                    and program_variant.get("group_regex")
                    and "{placeholder}" in replacement_variant
                ):
                    # group_regex variants intentionally store the whole match
                    # and replay its capture groups inside forward(groups).
                    # The generic inference routine cannot always infer that
                    # contract, so use group 0 and keep the normal verifier as
                    # the safety gate.
                    store_group = 0
                if store_group is None:
                    continue
                if (
                    getattr(args, "contextualize_broad_llm_regex", False)
                    and store_group == 0
                    and replacement_variant == "{placeholder}"
                    and internal_value_capture_llm_regex_items(
                        {
                            "regex": pattern,
                            "replacement": replacement_variant,
                            "program": program_variant,
                        },
                        max_variants=1,
                    )
                ):
                    rejected_reasons.append("whole-match placeholder stores stable regex context")
                    continue
                group_regex_python_exec = (
                    str(program_variant.get("op", "")) == "python_exec"
                    and bool(program_variant.get("group_regex"))
                )
                if (
                    replacement_duplicates_stored_group(replacement_variant, store_group)
                    and not group_regex_python_exec
                ):
                    rejected_reasons.append("replacement duplicated the stored group")
                    continue
                if args.program_mdl_compiler:
                    sample_match_values = [
                        match.group(store_group)
                        for match in matches[: min(len(matches), args.program_mdl_verify_matches)]
                        if match.group(store_group) is not None
                    ]
                    proxy_cost = compiled_candidate_proxy_cost(
                        matches[: min(len(matches), args.program_mdl_verify_matches)],
                        replacement_variant,
                        program_variant,
                        store_group,
                        "__PARE_MDL__",
                    )
                    compiled_options.append((proxy_cost, replacement_variant, program_variant, store_group, origin))
                    numeric_program = numeric_scalar_program_from_values(program_variant, sample_match_values)
                    if numeric_program is not None and dataset_extract.is_open_function_program_safe(numeric_program):
                        try:
                            numeric_proxy_cost = compiled_candidate_proxy_cost(
                                matches[: min(len(matches), args.program_mdl_verify_matches)],
                                replacement_variant,
                                numeric_program,
                                store_group,
                                "__PARE_MDL__",
                            )
                            compiled_options.append(
                                (
                                    numeric_proxy_cost,
                                    replacement_variant,
                                    numeric_program,
                                    store_group,
                                    f"{origin}+numeric_scalar_sketch",
                                )
                            )
                        except Exception:
                            pass
                    continue
            except Exception:
                continue
            verified_replacement = replacement_variant
            verified_program = program_variant
            verified_store_group = store_group
            verified_origin = origin
            break
        if args.program_mdl_compiler and compiled_options:
            non_repair_options = [
                option
                for option in compiled_options
                if not option[4].startswith("cegis_")
            ]
            if non_repair_options:
                compiled_options = non_repair_options
            if getattr(args, "semantic_three_class_program_prompt", False):
                try:
                    regex_for_contract = re.compile(pattern, flags=re.MULTILINE)
                    contract_matches = list(regex_for_contract.finditer(validation_text))[
                        : min(128, args.program_mdl_verify_matches)
                    ]
                except Exception:
                    contract_matches = []
                semantic_class_for_contract = normalized_llm_semantic_class(item)
                value_type_for_contract = str(item.get("value_type", "")).strip().lower().replace("-", "_")
                contract_options = [
                    option
                    for option in compiled_options
                    if python_exec_is_semantic_three_class(
                        contract_matches,
                        option[2],
                        option[3],
                        semantic_class_for_contract,
                        value_type_for_contract,
                        replacement=option[1],
                    )
                ]
                if contract_options:
                    compiled_options = contract_options
            compiled_options.sort(key=lambda item: item[0])
            _proxy_cost, verified_replacement, verified_program, verified_store_group, verified_origin = compiled_options[0]
        if verified_store_group is None or verified_program is None:
            trace_record["status"] = "rejected_no_verified_program"
            trace_record["reasons"] = rejected_reasons[-5:]
            maybe_enqueue_verifier_repair(
                item=item,
                status="rejected_no_verified_program",
                reasons=list(trace_record["reasons"]),
            )
            function_trace.append(trace_record)
            continue
        sample_values: list[str] = []
        try:
            regex = re.compile(pattern, flags=re.MULTILINE)
            sample_values = [
                match.group(verified_store_group)
                for match in regex.finditer(validation_text)
                if match.group(verified_store_group) is not None
            ][:128]
        except Exception:
            sample_values = []
        if (
            args.oracle_subordinate_time_auto
            and str(verified_program.get("op", "auto_codec")) == "int_expr_delta"
            and looks_like_time_payload(item, pattern, verified_program, sample_values)
        ):
            verified_program = {"op": "auto_codec"}
            verified_origin = f"{verified_origin}+subordinate_time_auto" if verified_origin else "subordinate_time_auto"
        if getattr(args, "semantic_numeric_only_program_prompt", False):
            try:
                regex = re.compile(pattern, flags=re.MULTILINE)
                numeric_matches = list(regex.finditer(validation_text))[: min(128, args.program_mdl_verify_matches)]
            except Exception:
                numeric_matches = []
            if not python_exec_stores_single_integer(
                numeric_matches,
                verified_program,
                verified_store_group,
                require_empty_layout=True,
            ):
                trace_record["status"] = "rejected_semantic_numeric_only"
                trace_record["kind"] = str(verified_program.get("op", ""))
                trace_record["tag"] = str(item.get("tag", ""))
                trace_record["reasons"] = [
                    "semantic-numeric-only mode accepts only reversible python_exec programs with exactly one integer stored value and no layout"
                ]
                maybe_enqueue_verifier_repair(
                    item=item,
                    status="rejected_semantic_numeric_only",
                    reasons=list(trace_record["reasons"]),
                )
                function_trace.append(trace_record)
                continue
        if getattr(args, "semantic_three_class_program_prompt", False):
            try:
                regex = re.compile(pattern, flags=re.MULTILINE)
                three_class_matches = list(regex.finditer(validation_text))[: min(128, args.program_mdl_verify_matches)]
            except Exception:
                three_class_matches = []
            semantic_class_for_contract = normalized_llm_semantic_class(item)
            value_type_for_contract = str(item.get("value_type", "")).strip().lower().replace("-", "_")
            if not python_exec_is_semantic_three_class(
                three_class_matches,
                verified_program,
                verified_store_group,
                semantic_class_for_contract,
                value_type_for_contract,
                replacement=verified_replacement,
            ):
                trace_record["status"] = "rejected_semantic_three_class"
                trace_record["kind"] = str(verified_program.get("op", ""))
                trace_record["tag"] = str(item.get("tag", ""))
                trace_record["reasons"] = [
                    "three-class mode requires class_1/class_2 python_exec programs to store one integer plus optional layout, and allows raw identity only for class_3"
                ]
                maybe_enqueue_verifier_repair(
                    item=item,
                    status="rejected_semantic_three_class",
                    reasons=list(trace_record["reasons"]),
                )
                function_trace.append(trace_record)
                continue
        if context_projector_code:
            verified_program = dict(verified_program)
            verified_program["context_code"] = context_projector_code
            if not dataset_extract.is_open_function_program_safe(verified_program):
                trace_record["status"] = "rejected_context_projector_safety"
                trace_record["kind"] = str(verified_program.get("op", ""))
                trace_record["tag"] = str(item.get("tag", ""))
                trace_record["reasons"] = [
                    "context.code must define safe project_context(line, groups) code"
                ]
                maybe_enqueue_verifier_repair(
                    item=item,
                    status="rejected_context_projector_safety",
                    reasons=list(trace_record["reasons"]),
                )
                function_trace.append(trace_record)
                continue
        candidate_kind = "auto" if str(verified_program.get("op", "auto_codec")) == "auto_codec" else "open_function"
        if str(verified_program.get("op", "")) == "python_exec":
            try:
                regex = re.compile(pattern, flags=re.MULTILINE)
                identity_matches = list(regex.finditer(validation_text))[:128]
                if dataset_extract.python_exec_is_raw_identity(
                    identity_matches,
                    verified_program,
                    verified_store_group,
                ):
                    candidate_kind = "auto"
                    verified_origin = (
                        f"{verified_origin}+python_exec_raw_identity_auto"
                        if verified_origin
                        else "python_exec_raw_identity_auto"
                    )
            except Exception:
                pass
        context_group = 0
        context_ref = ""
        semantic_class = normalized_llm_semantic_class(item)
        value_type = str(item.get("value_type", "")).strip().lower().replace("-", "_")
        context_policy = ""
        if getattr(args, "strict_three_class_context_schema", False):
            candidate_kind, value_type, context_policy = strict_context_candidate_kind(
                item,
                candidate_kind,
                context_group,
                context_ref,
                bool(context_projector_code),
            )
        tag = next_tag(tag_start + (len(bundle_candidates) if bundle_mode else len(specs)))
        semantic_key = semantic_key_from_payload(
            item,
            pattern,
            verified_replacement,
            verified_program,
            str(verified_program.get("op", "auto_codec")),
        )
        candidate = CandidateSpec(
            tag=tag,
            pattern=pattern,
            replacement=verified_replacement,
            program=verified_program,
            store_group=verified_store_group,
            kind=candidate_kind,
            semantic_key=semantic_key,
            general=bool(item.get("general", False)),
            order_hint=str(item.get("order_hint", "")),
            semantic_class=semantic_class,
            value_type=value_type,
            context_group=context_group,
            context_policy=context_policy,
            proposal_tag=normalize_context_ref(item.get("tag", "")),
            context_ref=context_ref,
        )
        context_ok, context_reason, context_value_count = validate_direct_context_projector(
            candidate,
            validation_lines,
        )
        if not context_ok:
            trace_record["status"] = "rejected_context_projector_validation"
            trace_record["kind"] = candidate.kind
            trace_record["tag"] = candidate.tag
            trace_record["reasons"] = [context_reason]
            maybe_enqueue_verifier_repair(
                item=item,
                status="rejected_context_projector_validation",
                reasons=[context_reason],
            )
            function_trace.append(trace_record)
            continue
        if context_projector_code:
            trace_record["context_projector_values"] = context_value_count
        if args.prune_constant_local_streams and candidate.kind == "auto" and not semantic_key_class(semantic_key):
            try:
                regex = compiled_multiline_regex(candidate.pattern)
                sample_values = [
                    match.group(candidate.store_group)
                    for match in regex.finditer(validation_text)
                    if match.group(candidate.store_group) is not None
                ]
            except Exception:
                sample_values = []
            if sample_values and len(set(sample_values)) <= 1:
                trace_record["status"] = "pruned_constant_local_stream"
                trace_record["kind"] = candidate.kind
                trace_record["semantic_key"] = candidate.semantic_key
                function_trace.append(trace_record)
                continue
        active_program = bundle_candidates if bundle_mode else specs
        ok, reason = validate_spec_program_on_lines(validation_lines, active_program + [candidate])
        if ok:
            if bundle_mode:
                bundle_candidates.append(candidate)
                fallback_ok, fallback_reason = validate_spec_program_on_lines(validation_lines, specs + [candidate])
                if not fallback_ok:
                    rejected_reasons.append(f"{tag}:fallback:{fallback_reason}")
                    continue
            line_mdl_required = args.program_line_mdl_admission
            if args.program_line_mdl_semantic_grace and (
                candidate.kind == "open_function"
                or (
                    bool(semantic_key_class(candidate.semantic_key))
                    and not getattr(args, "program_line_mdl_grace_open_function_only", False)
                )
            ):
                # Some functions are locally marginal but globally important:
                # timestamps, IP/port-like fields, and numeric transducers
                # amortize only after semantic registry/replay merges them
                # across many template families.  Keep the line-MDL gate for
                # low-semantic auto spans, where broad context extraction is
                # the dominant failure mode.
                line_mdl_required = False
            if line_mdl_required:
                current_cost = program_validation_proxy_cost(validation_lines, specs)
                candidate_cost = program_validation_proxy_cost(validation_lines, specs + [candidate])
                min_gain = args.program_line_mdl_min_gain
                if current_cost - candidate_cost < min_gain:
                    reason_text = f"{tag}:line-mdl gain {current_cost - candidate_cost} < {min_gain}"
                    rejected_reasons.append(reason_text)
                    trace_record["status"] = "rejected_line_mdl"
                    trace_record["kind"] = candidate.kind
                    trace_record["semantic_key"] = candidate.semantic_key
                    trace_record["tag"] = candidate.tag
                    trace_record["reasons"] = [reason_text]
                    function_trace.append(trace_record)
                    continue
            specs.append(candidate)
            trace_record["status"] = "admitted"
            trace_record["kind"] = candidate.kind
            trace_record["semantic_key"] = candidate.semantic_key
            trace_record["tag"] = candidate.tag
            trace_record["store_group"] = candidate.store_group
            function_trace.append(trace_record)
        else:
            reason_text = f"{tag}:{reason}"
            rejected_reasons.append(reason_text)
            trace_record["status"] = "rejected_validation"
            trace_record["kind"] = candidate.kind
            trace_record["semantic_key"] = candidate.semantic_key
            trace_record["tag"] = candidate.tag
            trace_record["reasons"] = [reason_text]
            maybe_enqueue_verifier_repair(
                item=item,
                status="rejected_validation",
                reasons=[reason_text],
            )
            function_trace.append(trace_record)
    if bundle_mode:
        bundle_specs, bundle_reason = select_family_program_bundle(validation_lines, bundle_candidates, args)
        if bundle_specs:
            try:
                fallback_cost = program_validation_proxy_cost(validation_lines, specs) if specs else 10**18
                bundle_cost = program_validation_proxy_cost(validation_lines, bundle_specs)
            except Exception:
                fallback_cost = 0
                bundle_cost = 1
            if not specs or (len(bundle_specs) > len(specs) and bundle_cost < fallback_cost):
                specs = bundle_specs
        elif not specs:
            detail_items = [bundle_reason] if bundle_reason else []
            detail_items.extend(rejected_reasons[:2])
            detail = "; ".join(item for item in detail_items if item)
            return [], "no bundle-mdl or fallback admitted functions" + (f" ({detail})" if detail else "")
    if not specs:
        detail = "; ".join(rejected_reasons[:3])
        return [], "no program-level verifier-passing functions" + (f" ({detail})" if detail else "")
    resolve_candidate_context_refs(specs)
    return specs, ""


def validation_render_value(spec: CandidateSpec, raw_value: str) -> str:
    """Render a stored side-stream value exactly as the decoder would."""
    if bool(spec.program.get("_layout_polymorphic_shared_delta", False)):
        return dataset_extract.render_layout_polymorphic_exact(raw_value, spec.program)
    return dataset_extract.render_open_function_exact(raw_value, spec.program)


def stored_value_for_match(spec: CandidateSpec, raw_value: str, match: re.Match[str]) -> str:
    """Return the value appended to the side stream for this concrete match.

    Strict context-aware functions store the reversible raw value together with
    the LLM-declared entropy context.  The decoder-side stream codec may group
    by that context, while exact replay still renders only the raw value.
    """
    if spec.context_group > 0:
        try:
            context_value = match.group(spec.context_group)
        except Exception:
            context_value = ""
        if context_value is not None and spec.context_group != spec.store_group:
            return json.dumps([raw_value, context_value], separators=(",", ":"), ensure_ascii=False)
    return raw_value


def raw_value_from_stored(spec: CandidateSpec, stored_value: str) -> str:
    if spec.context_group > 0 or spec.kind in {"open_context_delta", "open_context_dict"}:
        try:
            payload = json.loads(stored_value)
            if isinstance(payload, list) and payload:
                return str(payload[0])
        except Exception:
            pass
    return stored_value


def materialize_direct_context_projectors(
    original_text: str,
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    specs: list[CandidateSpec],
) -> dict[str, int]:
    """Evaluate each accepted target's context projector on its source line.

    Target extraction and exact reconstruction are already complete at this
    point. The projector only adds a routing key to each existing side-stream
    value, so a missing context can safely use the empty bucket.
    """
    stats = {
        "declared": 0,
        "activated": 0,
        "matched_values": 0,
        "missing_context_values": 0,
        "runtime_errors": 0,
        "alignment_errors": 0,
    }
    original_lines = dataset_extract.split_physical_lines(original_text)
    transformed_lines = dataset_extract.split_physical_lines(transformed_text)
    if len(original_lines) != len(transformed_lines):
        stats["alignment_errors"] = 1
        return stats

    for spec in specs:
        if not spec.program.get("context_code"):
            continue
        target_values = values_by_tag.get(spec.tag, [])
        placeholder = placeholders.get(spec.tag, "")
        if not target_values or not placeholder:
            continue
        stats["declared"] += 1
        try:
            regex = compiled_multiline_regex(spec.pattern)
        except Exception:
            stats["alignment_errors"] += 1
            continue

        contexts: list[str] = []
        value_index = 0
        aligned = True
        for original_line, transformed_line in zip(original_lines, transformed_lines):
            occurrence_count = transformed_line.count(placeholder)
            if occurrence_count == 0:
                continue
            expected_values = target_values[value_index:value_index + occurrence_count]
            if len(expected_values) != occurrence_count:
                aligned = False
                break
            candidate_matches = list(regex.finditer(original_line))
            used_match_indexes: set[int] = set()
            search_start = 0
            for expected_value in expected_values:
                selected_index = -1
                for match_index in range(search_start, len(candidate_matches)):
                    if match_index in used_match_indexes:
                        continue
                    match = candidate_matches[match_index]
                    raw_value = match.group(spec.store_group)
                    if raw_value is None:
                        continue
                    if stored_value_for_match(spec, raw_value, match) == expected_value:
                        selected_index = match_index
                        break
                if selected_index < 0:
                    for match_index, match in enumerate(candidate_matches):
                        if match_index in used_match_indexes:
                            continue
                        raw_value = match.group(spec.store_group)
                        if raw_value is not None and stored_value_for_match(spec, raw_value, match) == expected_value:
                            selected_index = match_index
                            break
                if selected_index < 0:
                    contexts.append("")
                    stats["missing_context_values"] += 1
                    continue
                used_match_indexes.add(selected_index)
                search_start = selected_index + 1
                selected_match = candidate_matches[selected_index]
                try:
                    context_value = dataset_extract.run_python_exec_context_projector(
                        original_line,
                        list(selected_match.groups()),
                        spec.program,
                    )
                except Exception:
                    context_value = None
                    stats["runtime_errors"] += 1
                if context_value in {None, ""}:
                    contexts.append("")
                    stats["missing_context_values"] += 1
                else:
                    contexts.append(str(context_value))
                    stats["matched_values"] += 1
            value_index += occurrence_count

        if value_index != len(target_values) or len(contexts) != len(target_values):
            aligned = False
        if not aligned:
            stats["alignment_errors"] += 1
            continue
        if not any(contexts):
            continue
        values_by_tag[spec.tag] = [
            json.dumps(
                [raw_value_from_stored(spec, raw_value), context_value],
                separators=(",", ":"),
                ensure_ascii=False,
            )
            for raw_value, context_value in zip(target_values, contexts)
        ]
        if spec.semantic_class in {"class_1_composed_numeric", "class_2_formatted_numeric"} or spec.value_type in {
            "numeric",
            "integer",
            "int",
        }:
            spec.kind = "open_context_delta"
        else:
            spec.kind = "open_context_dict"
        spec.context_policy = "direct_context_projector"
        spec.context_group = 0
        spec.context_ref = ""
        stats["activated"] += 1
    return stats


def materialize_cross_function_contexts(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    specs: list[CandidateSpec],
) -> dict[str, int]:
    """Pair target values with a referenced function's value on the same line.

    The LLM names another function through ``context_ref``. The target regex no
    longer needs to capture that context itself. Placeholder order provides the
    deterministic link to concrete per-line values; exact target reconstruction
    remains independent of the context value.
    """
    stats = {
        "declared": 0,
        "resolved": 0,
        "activated": 0,
        "matched_values": 0,
        "missing_context_values": 0,
        "unresolved": 0,
    }
    specs_by_semantic: dict[str, CandidateSpec] = {}
    specs_by_proposal: dict[str, CandidateSpec] = {}
    specs_by_tag: dict[str, CandidateSpec] = {}
    for spec in specs:
        if spec.tag in values_by_tag:
            specs_by_tag.setdefault(spec.tag, spec)
            if spec.semantic_key:
                specs_by_semantic.setdefault(spec.semantic_key, spec)
            proposal_tag = normalize_context_ref(spec.proposal_tag)
            if proposal_tag:
                specs_by_proposal.setdefault(proposal_tag, spec)

    relations: dict[str, CandidateSpec] = {}
    for target in specs:
        if target.tag not in values_by_tag or not target.context_ref:
            continue
        stats["declared"] += 1
        ref = target.context_ref
        source: CandidateSpec | None = None
        if ref.startswith("semantic:"):
            source = specs_by_semantic.get(ref[len("semantic:"):])
        elif ref.startswith("proposal:"):
            source = specs_by_proposal.get(normalize_context_ref(ref[len("proposal:"):]))
        else:
            source = specs_by_proposal.get(normalize_context_ref(ref))
            if source is None:
                source = specs_by_semantic.get(ref)
        if source is None or source.tag == target.tag or source.tag not in placeholders:
            stats["unresolved"] += 1
            continue
        relations[target.tag] = source
        stats["resolved"] += 1

    if not relations:
        return stats

    relevant_tags = set(relations)
    relevant_tags.update(source.tag for source in relations.values())
    placeholder_to_tag = {
        placeholders[tag]: tag
        for tag in relevant_tags
        if tag in placeholders and placeholders[tag]
    }
    if not placeholder_to_tag:
        stats["unresolved"] += len(relations)
        return stats
    marker_pattern = re.compile(
        "|".join(re.escape(marker) for marker in sorted(placeholder_to_tag, key=len, reverse=True))
    )
    value_indexes = {tag: 0 for tag in relevant_tags}
    contexts_by_target = {tag: [] for tag in relations}

    for line in dataset_extract.split_physical_lines(transformed_text):
        events_by_tag: dict[str, list[tuple[int, str]]] = {}
        for match in marker_pattern.finditer(line):
            tag = placeholder_to_tag[match.group(0)]
            index = value_indexes[tag]
            values = values_by_tag.get(tag, [])
            if index >= len(values):
                continue
            source_spec = specs_by_tag.get(tag)
            value = values[index]
            if source_spec is not None:
                value = raw_value_from_stored(source_spec, value)
            events_by_tag.setdefault(tag, []).append((match.start(), value))
            value_indexes[tag] = index + 1

        for target_tag, source_spec in relations.items():
            target_events = events_by_tag.get(target_tag, [])
            if not target_events:
                continue
            source_events = events_by_tag.get(source_spec.tag, [])
            for ordinal, (target_pos, _target_value) in enumerate(target_events):
                context_value = ""
                if len(source_events) == 1:
                    context_value = source_events[0][1]
                elif len(source_events) == len(target_events):
                    context_value = source_events[ordinal][1]
                elif source_events:
                    context_value = min(source_events, key=lambda item: abs(item[0] - target_pos))[1]
                if context_value:
                    stats["matched_values"] += 1
                else:
                    stats["missing_context_values"] += 1
                contexts_by_target[target_tag].append(context_value)

    for target_tag, source_spec in relations.items():
        target = specs_by_tag[target_tag]
        target_values = values_by_tag.get(target_tag, [])
        context_values = contexts_by_target.get(target_tag, [])
        if len(context_values) != len(target_values) or not any(context_values):
            stats["unresolved"] += 1
            continue
        values_by_tag[target_tag] = [
            json.dumps(
                [raw_value_from_stored(target, raw_value), context_value],
                separators=(",", ":"),
                ensure_ascii=False,
            )
            for raw_value, context_value in zip(target_values, context_values)
        ]
        if target.semantic_class in {"class_1_composed_numeric", "class_2_formatted_numeric"} or target.value_type in {
            "numeric",
            "integer",
            "int",
        }:
            target.kind = "open_context_delta"
        else:
            target.kind = "open_context_dict"
        target.context_policy = "group_by_context_ref"
        target.context_group = 0
        stats["activated"] += 1
    return stats


def validation_render_stored_value(spec: CandidateSpec, stored_value: str) -> str:
    return validation_render_value(spec, raw_value_from_stored(spec, stored_value))


def replay_program_safe(spec: CandidateSpec) -> bool:
    """Guard final replay against legacy fixed-op candidates.

    The current open-function mainline admits only LLM-generated python_exec
    transforms and raw auto_codec streams.  Older global/replay helpers may
    still construct historical open_function specs such as syslog_delta; those
    must not enter paper-facing replay plans.
    """
    if spec.kind != "open_function":
        return True
    return dataset_extract.is_open_function_program_safe(spec.program)


LEADING_ZERO_DIGIT_RUN_RE = re.compile(r"(?<!\d)0\d+")


def trusted_match_preserves_exact_value(spec: CandidateSpec, raw_value: str, match: re.Match[str]) -> bool:
    """Prove that replacing this concrete match is byte-exact reversible.

    The trusted replay path is allowed to skip expensive *search* work, but it
    must not skip the lossless boundary.  A function can be valid for the family
    that produced it and still over-match another family during global replay.
    For example, a timestamp regex may optionally consume ``" *64"`` after an
    executable name while the replacement omits that suffix.  Such a match is
    destructive even though the timestamp value itself renders correctly.

    Therefore every concrete match accepted by the fast path must satisfy the
    same local contract as the slow verifier: decoding the stored value and
    applying the replacement template must recreate exactly ``match.group(0)``.
    """
    try:
        rendered_value = validation_render_value(spec, raw_value)
    except Exception:
        return False
    return format_replacement_fast(spec.replacement, rendered_value, match) == match.group(0)


@lru_cache(maxsize=4096)
def _parsed_replacement_template(replacement: str) -> tuple[tuple[str, int | str], ...] | None:
    parts: list[tuple[str, int | str]] = []
    cursor = 0
    for match in re.finditer(r"\{(placeholder|group\d+)\}", replacement):
        if match.start() > cursor:
            literal = replacement[cursor:match.start()]
            if "{" in literal or "}" in literal:
                return None
            parts.append(("literal", literal))
        token = match.group(1)
        if token == "placeholder":
            parts.append(("placeholder", ""))
        else:
            parts.append(("group", int(token[5:])))
        cursor = match.end()
    if cursor < len(replacement):
        literal = replacement[cursor:]
        if "{" in literal or "}" in literal:
            return None
        parts.append(("literal", literal))
    if not parts:
        return (("literal", replacement),)
    return tuple(parts)


def format_replacement_fast(replacement: str, placeholder: str, match: re.Match[str]) -> str:
    """Fast equivalent of dataset_extract._safe_replacement_format.

    The hot replay path formats simple templates such as
    ``{group1}{placeholder}{group3}`` millions of times.  Avoid building a
    per-match dictionary while preserving the same string output for accepted
    replacement syntax.
    """
    if replacement == "{placeholder}":
        return placeholder
    parsed = _parsed_replacement_template(replacement)
    if parsed is None:
        return dataset_extract._safe_replacement_format(replacement, placeholder, match)
    out: list[str] = []
    for kind, payload in parsed:
        if kind == "literal":
            out.append(str(payload))
        elif kind == "placeholder":
            out.append(placeholder)
        else:
            group_value = match.group(int(payload))
            out.append(str(group_value))
    return "".join(out)


def restore_validation_text(
    transformed_text: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    values_by_tag: dict[str, list[str]],
) -> str:
    if not placeholders:
        return transformed_text
    tag_by_placeholder = {placeholder: tag for tag, placeholder in placeholders.items()}
    spec_by_tag = {spec.tag: spec for spec in specs}
    indices = {tag: 0 for tag in values_by_tag}
    pattern = re.compile("|".join(re.escape(value) for value in sorted(placeholders.values(), key=len, reverse=True)))

    def replace(match: re.Match[str]) -> str:
        placeholder = match.group(0)
        tag = tag_by_placeholder[placeholder]
        values = values_by_tag.get(tag, [])
        index = indices.get(tag, 0)
        if index >= len(values):
            raise ValueError(f"value stream exhausted for tag {tag}")
        indices[tag] = index + 1
        return validation_render_stored_value(spec_by_tag[tag], values[index])

    restored = pattern.sub(replace, transformed_text)
    for tag, values in values_by_tag.items():
        if indices.get(tag, 0) != len(values):
            raise ValueError(f"unused values for tag {tag}")
    return restored


def semantic_stream_feature_gate_accepts(
    spec: CandidateSpec,
    values: list[str],
    placeholder: str,
    args: argparse.Namespace,
) -> tuple[bool, str, dict[str, object]]:
    """Lightweight feature gate for already verified semantic streams.

    This is intentionally not an MDL/proxy-compression selector.  It mirrors the
    residual "is this stream likely homogeneous enough to materialize" question
    with cheap support/diversity/byte-reduction features.
    """
    raw_values = [raw_value_from_stored(spec, value) for value in values]
    support = len(raw_values)
    distinct = len(set(raw_values))
    min_support = int(os.environ.get("PARE_SEMANTIC_STREAM_MIN_SUPPORT", "0") or 0)
    if min_support <= 0:
        min_support = max(20, int(getattr(args, "llm_min_support", 20) or 20))
    min_distinct = int(os.environ.get("PARE_SEMANTIC_STREAM_MIN_DISTINCT", "2") or 2)
    min_score = int(os.environ.get("PARE_SEMANTIC_STREAM_MIN_SCORE", str(getattr(args, "residual_variable_min_score", 0))) or 0)
    raw_main = sum(len(value.encode("latin-1", errors="ignore")) for value in raw_values)
    cheap_score = raw_main - (4 * support)
    stats = {
        "support": support,
        "distinct": distinct,
        "raw_main": raw_main,
        "cheap_score": cheap_score,
        "kind": spec.kind,
        "op": str(spec.program.get("op", "")) if isinstance(spec.program, dict) else "",
    }
    if support < min_support:
        return False, "support", stats
    if distinct < min_distinct:
        return False, "distinct", stats

    numeric_success = 0
    if spec.kind == "open_function" and isinstance(spec.program, dict) and str(spec.program.get("op", "")) != "auto_codec":
        sample = raw_values[: min(64, len(raw_values))]
        for raw_value in sample:
            try:
                dataset_extract.parse_open_function_value(raw_value, spec.program)
                numeric_success += 1
            except Exception:
                pass
        if sample and numeric_success >= max(1, int(0.9 * len(sample))):
            stats["numeric_sample_success"] = numeric_success
            return True, "numeric_function", stats

    if cheap_score <= min_score:
        return False, "cheap_score", stats
    return True, "variable_feature", stats


def apply_semantic_stream_feature_gate(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, object]]:
    if os.environ.get("PARE_SEMANTIC_STREAM_FEATURE_GATE", "0") != "1":
        return transformed_text, values_by_tag, placeholders, accepted_specs, {"enabled": 0}
    spec_by_tag = {spec.tag: spec for spec in accepted_specs}
    rejected: dict[str, str] = {}
    accepted = 0
    rejected_details: list[dict[str, object]] = []
    for tag, values in values_by_tag.items():
        spec = spec_by_tag.get(tag)
        placeholder = placeholders.get(tag, "")
        if spec is None or not placeholder:
            continue
        keep, reason, stats = semantic_stream_feature_gate_accepts(spec, values, placeholder, args)
        detail = {"tag": tag, "reason": reason, **stats}
        if keep:
            accepted += 1
        else:
            rejected[tag] = reason
            if len(rejected_details) < 64:
                rejected_details.append(detail)
    if not rejected:
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "enabled": 1,
            "accepted": accepted,
            "rejected": 0,
            "rejected_details": [],
        }

    tag_by_placeholder = {placeholder: tag for tag, placeholder in placeholders.items()}
    skipped_indices = {tag: 0 for tag in rejected}
    pattern = re.compile("|".join(re.escape(value) for value in sorted(placeholders.values(), key=len, reverse=True)))

    def replace(match: re.Match[str]) -> str:
        placeholder = match.group(0)
        tag = tag_by_placeholder[placeholder]
        if tag not in rejected:
            return placeholder
        index = skipped_indices[tag]
        values = values_by_tag.get(tag, [])
        if index >= len(values):
            raise ValueError(f"semantic feature gate exhausted stream {tag}")
        skipped_indices[tag] = index + 1
        return validation_render_stored_value(spec_by_tag[tag], values[index])

    gated_text = pattern.sub(replace, transformed_text)
    for tag, index in skipped_indices.items():
        if index != len(values_by_tag.get(tag, [])):
            raise ValueError(f"semantic feature gate unused values for {tag}")
    kept_values = {
        tag: values
        for tag, values in values_by_tag.items()
        if tag not in rejected
    }
    kept_placeholders = {
        tag: placeholder
        for tag, placeholder in placeholders.items()
        if tag not in rejected
    }
    kept_specs = [
        spec
        for spec in accepted_specs
        if spec.tag not in rejected
    ]
    try:
        restored = restore_validation_text(gated_text, kept_specs, kept_placeholders, kept_values)
        if restored != original_text:
            raise ValueError("semantic feature gate restore mismatch")
    except Exception as exc:
        raise ValueError(f"semantic feature gate rejected an unsafe stream set: {exc}") from exc
    return gated_text, kept_values, kept_placeholders, kept_specs, {
        "enabled": 1,
        "accepted": len(kept_values),
        "rejected": len(rejected),
        "rejected_details": rejected_details,
    }


def validate_spec_program_on_lines(lines: list[str], specs: list[CandidateSpec]) -> tuple[bool, str]:
    """Verify the ordered LLM program, not just each regex in isolation.

    Whole-line synthesis can propose overlapping functions. A later regex may
    consume a placeholder inserted by an earlier regex, producing side-stream
    values that the decoder can never use. This gate simulates the complete
    extract-and-restore path on held-out lines before admitting a function.
    """
    if not lines or not specs:
        return False, "empty validation set"
    placeholders = {spec.tag: f"__PARE_VALID_{spec.tag}__" for spec in specs}
    values_by_tag: dict[str, list[str]] = {}
    transformed_lines: list[str] = []
    try:
        for line in lines:
            transformed = line
            for spec in specs:
                regex = compiled_multiline_regex(spec.pattern)
                placeholder = placeholders[spec.tag]
                previous_counts = {
                    tag: transformed.count(existing_placeholder)
                    for tag, existing_placeholder in placeholders.items()
                    if tag != spec.tag
                }

                def replace(match: re.Match[str]) -> str:
                    raw_value = match.group(spec.store_group)
                    if raw_value is None:
                        raise ValueError(f"empty store group for tag {spec.tag}")
                    rendered_value = validation_render_value(spec, raw_value)
                    if format_replacement_fast(spec.replacement, rendered_value, match) != match.group(0):
                        raise ValueError(f"non-invertible replacement for tag {spec.tag}")
                    values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
                    return format_replacement_fast(spec.replacement, placeholder, match)

                transformed = regex.sub(replace, transformed)
                for tag, previous_count in previous_counts.items():
                    if transformed.count(placeholders[tag]) < previous_count:
                        raise ValueError(f"function {spec.tag} consumed earlier placeholder {tag}")
            transformed_lines.append(transformed)
        transformed_text = "".join(transformed_lines)
        for spec in specs:
            if not values_by_tag.get(spec.tag):
                return False, f"no surviving matches for tag {spec.tag}"
        restored = restore_validation_text(transformed_text, specs, placeholders, values_by_tag)
        original = "".join(lines)
        if restored != original:
            return False, "program-level roundtrip mismatch"
    except Exception as exc:
        return False, str(exc)[:160]
    return True, ""


def apply_specs_to_line(
    line: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    values_by_tag: dict[str, list[str]],
    trusted_fast_path: bool = False,
    skip_placeholder_protection: bool = False,
    skip_exact_value_check: bool = False,
) -> str:
    """Apply accepted specs while preserving the exact-replay invariant.

    Even for globally replayed semantic scanners, the regex match is not a trust
    boundary.  Each concrete match must be renderable back to the exact bytes it
    consumed before the placeholder/value pair is admitted.
    """
    transformed = line
    for spec in specs:
        regex = compiled_multiline_regex(spec.pattern)
        placeholder = placeholders[spec.tag]
        protected_placeholders = [] if (trusted_fast_path and skip_placeholder_protection) else [
            existing
            for tag, existing in placeholders.items()
            if tag != spec.tag
        ]

        def replace(match: re.Match[str]) -> str:
            if protected_placeholders and any(existing in match.group(0) for existing in protected_placeholders):
                return match.group(0)
            raw_value = match.group(spec.store_group)
            if raw_value is None:
                return match.group(0)
            if trusted_fast_path:
                if not trusted_match_preserves_exact_value(spec, raw_value, match):
                    return match.group(0)
                values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
                return format_replacement_fast(spec.replacement, placeholder, match)
            try:
                rendered_value = validation_render_value(spec, raw_value)
            except Exception:
                return match.group(0)
            if format_replacement_fast(spec.replacement, rendered_value, match) != match.group(0):
                return match.group(0)
            values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
            return format_replacement_fast(spec.replacement, placeholder, match)

        transformed = regex.sub(replace, transformed)
    return transformed


def apply_specs_to_original_line(
    line: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    values_by_tag: dict[str, list[str]],
    trusted_fast_path: bool = False,
    skip_placeholder_protection: bool = False,
    skip_exact_value_check: bool = False,
) -> str:
    """Fast path for family-scan replay on raw input lines.

    All placeholders are collision-free with the original text, so a raw line
    can only contain placeholders inserted by earlier specs in this same call.
    Protecting only sibling placeholders preserves the exact-replay invariant
    while avoiding an O(all_streams) protected-list rebuild for every line.
    """
    if len(specs) == 1:
        spec = specs[0]
        regex = compiled_multiline_regex(spec.pattern)
        placeholder = placeholders[spec.tag]

        def replace_single(match: re.Match[str]) -> str:
            raw_value = match.group(spec.store_group)
            if raw_value is None:
                return match.group(0)
            if trusted_fast_path:
                if not trusted_match_preserves_exact_value(spec, raw_value, match):
                    return match.group(0)
                values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
                return format_replacement_fast(spec.replacement, placeholder, match)
            try:
                rendered_value = validation_render_value(spec, raw_value)
            except Exception:
                return match.group(0)
            if format_replacement_fast(spec.replacement, rendered_value, match) != match.group(0):
                return match.group(0)
            values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
            return format_replacement_fast(spec.replacement, placeholder, match)

        return regex.sub(replace_single, line)

    transformed = line
    sibling_placeholders = {spec.tag: placeholders[spec.tag] for spec in specs}
    for spec in specs:
        regex = compiled_multiline_regex(spec.pattern)
        placeholder = placeholders[spec.tag]
        protected_placeholders = [] if (trusted_fast_path and skip_placeholder_protection) else [
            existing
            for tag, existing in sibling_placeholders.items()
            if tag != spec.tag
        ]

        def replace(match: re.Match[str]) -> str:
            if protected_placeholders and any(existing in match.group(0) for existing in protected_placeholders):
                return match.group(0)
            raw_value = match.group(spec.store_group)
            if raw_value is None:
                return match.group(0)
            if trusted_fast_path:
                if not trusted_match_preserves_exact_value(spec, raw_value, match):
                    return match.group(0)
                values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
                return format_replacement_fast(spec.replacement, placeholder, match)
            try:
                rendered_value = validation_render_value(spec, raw_value)
            except Exception:
                return match.group(0)
            if format_replacement_fast(spec.replacement, rendered_value, match) != match.group(0):
                return match.group(0)
            values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
            return format_replacement_fast(spec.replacement, placeholder, match)

        transformed = regex.sub(replace, transformed)
    return transformed


def apply_specs_to_original_text(
    text: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    values_by_tag: dict[str, list[str]],
    trusted_fast_path: bool = False,
    skip_placeholder_protection: bool = False,
    skip_exact_value_check: bool = False,
) -> str:
    """Apply trusted specs to raw text without a Python loop over lines.

    This is only for replaying already verified specs on raw input text.  A match
    spanning a newline is rejected so the transformation remains equivalent to
    line-wise replay for log records.
    """
    transformed = text
    sibling_placeholders = {spec.tag: placeholders[spec.tag] for spec in specs}
    for spec in specs:
        regex = compiled_multiline_regex(spec.pattern)
        placeholder = placeholders[spec.tag]
        protected_placeholders = [] if (trusted_fast_path and skip_placeholder_protection) else [
            existing
            for tag, existing in sibling_placeholders.items()
            if tag != spec.tag
        ]

        def replace(match: re.Match[str]) -> str:
            if "\n" in match.group(0):
                return match.group(0)
            if protected_placeholders and any(existing in match.group(0) for existing in protected_placeholders):
                return match.group(0)
            raw_value = match.group(spec.store_group)
            if raw_value is None:
                return match.group(0)
            if trusted_fast_path:
                if not trusted_match_preserves_exact_value(spec, raw_value, match):
                    return match.group(0)
                values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
                return format_replacement_fast(spec.replacement, placeholder, match)
            try:
                rendered_value = validation_render_value(spec, raw_value)
            except Exception:
                return match.group(0)
            if format_replacement_fast(spec.replacement, rendered_value, match) != match.group(0):
                return match.group(0)
            values_by_tag.setdefault(spec.tag, []).append(stored_value_for_match(spec, raw_value, match))
            return format_replacement_fast(spec.replacement, placeholder, match)

        transformed = regex.sub(replace, transformed)
    return transformed


def _build_pcre2_replay_dump_tool() -> Path:
    root = Path(__file__).resolve().parent
    runtime_dir = root / "cpp_runtime"
    exe = runtime_dir / "pcre2_replay_dump"
    source = runtime_dir / "pcre2_replay_dump.cpp"
    if exe.is_file() and exe.stat().st_mtime >= source.stat().st_mtime:
        return exe
    compiler = os.environ.get("CXX") or shutil.which("clang++") or shutil.which("g++")
    if not compiler:
        raise RuntimeError("no C++ compiler found for PCRE2 replay")
    pcre2_config = shutil.which("pcre2-config")
    cflags: list[str] = []
    libs = ["-lpcre2-8"]
    if pcre2_config:
        cflags = shlex.split(subprocess.check_output([pcre2_config, "--cflags"], text=True).strip())
        libs = shlex.split(subprocess.check_output([pcre2_config, "--libs8"], text=True).strip())
    subprocess.run(
        [
            compiler,
            "-std=c++17",
            "-O2",
            "-Wall",
            "-Wextra",
            *cflags,
            str(source.name),
            *libs,
            "-o",
            str(exe),
        ],
        cwd=runtime_dir,
        check=True,
    )
    return exe


def _write_pcre2_replay_spec_file(path: Path, specs: list[CandidateSpec], placeholders: dict[str, str]) -> list[CandidateSpec]:
    usable: list[CandidateSpec] = []
    lines: list[str] = []
    for spec in specs:
        if is_stage_residual_spec(spec):
            continue
        placeholder = placeholders.get(spec.tag)
        if not placeholder:
            continue
        if any("\n" in field or "\t" in field for field in (spec.pattern, spec.replacement, placeholder, spec.tag)):
            continue
        usable.append(spec)
        lines.extend(
            [
                "SPEC",
                spec.tag,
                spec.pattern,
                placeholder,
                spec.replacement,
                str(spec.store_group),
                str(spec.context_group),
                "END",
            ]
        )
    path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    return usable


def _read_pcre2_replay_values(path: Path) -> dict[str, list[str]]:
    values_by_tag: dict[str, list[str]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        tag = str(item["tag"])
        value = str(item["value"])
        values_by_tag.setdefault(tag, []).append(value)
    return values_by_tag


def apply_specs_to_original_text_cpp_pcre2(
    text: str,
    specs: list[CandidateSpec],
    placeholders: dict[str, str],
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], list[CandidateSpec], dict[str, object]] | None:
    """Run parity-proven replay specs through C++/PCRE2.

    This is intentionally limited to the replay-plan data plane.  Training,
    verifier, and python_exec safety checks remain in Python.
    """
    if not getattr(args, "cpp_pcre2_replay", False):
        return None
    started = time.perf_counter()
    try:
        exe = _build_pcre2_replay_dump_tool()
        with tempfile.TemporaryDirectory(prefix="semzip_cpp_pcre2_replay_") as tmp_name:
            tmp = Path(tmp_name)
            input_path = tmp / "input.log"
            spec_path = tmp / "specs.txt"
            transformed_path = tmp / "transformed.log"
            values_path = tmp / "values.jsonl"
            input_path.write_bytes(text.encode("latin-1"))
            usable_specs = _write_pcre2_replay_spec_file(spec_path, specs, placeholders)
            if not usable_specs:
                return None
            subprocess.run(
                [str(exe), str(spec_path), str(input_path), str(transformed_path), str(values_path)],
                cwd=Path(__file__).resolve().parent / "cpp_runtime",
                check=True,
            )
            transformed_text = transformed_path.read_bytes().decode("latin-1")
            values_by_tag = _read_pcre2_replay_values(values_path)
        stats: dict[str, object] = {
            "enabled": 1,
            "used": 1,
            "specs": len(usable_specs),
            "values": sum(len(values) for values in values_by_tag.values()),
            "seconds": time.perf_counter() - started,
        }
        if os.environ.get("SEMZIP_CPP_PCRE2_REPLAY_VERIFY", "0") in {"1", "true", "True"}:
            py_values_by_tag: dict[str, list[str]] = {}
            py_text = apply_specs_to_original_text(
                text,
                usable_specs,
                {spec.tag: placeholders[spec.tag] for spec in usable_specs},
                py_values_by_tag,
                trusted_fast_path=True,
                skip_placeholder_protection=getattr(args, "trusted_replay_skip_placeholder_protection", False),
                skip_exact_value_check=getattr(args, "trusted_replay_skip_exact_value_check", False),
            )
            if py_text != transformed_text or py_values_by_tag != values_by_tag:
                raise ValueError("C++ PCRE2 replay did not match Python replay")
            stats["verified_against_python"] = 1
        return transformed_text, values_by_tag, usable_specs, stats
    except Exception as exc:
        if os.environ.get("SEMZIP_CPP_PCRE2_REPLAY_FALLBACK", "0") in {"1", "true", "True"}:
            return None
        raise RuntimeError(f"C++ PCRE2 replay failed: {exc}") from exc


def placeholder_regex(placeholders: dict[str, str]) -> re.Pattern[str]:
    escaped = [re.escape(value) for value in sorted(placeholders.values(), key=len, reverse=True)]
    return re.compile("|".join(escaped))


def discover_relation_candidates(
    transformed_text: str,
    placeholders: dict[str, str],
    values_by_tag: dict[str, list[str]],
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    return []


def render_fragment_with_stream_values(
    fragment: str,
    placeholders: dict[str, str],
    values_by_tag: dict[str, list[str]],
    indices: dict[str, int],
) -> str:
    tag_by_placeholder = {placeholder: tag for tag, placeholder in placeholders.items()}
    pieces: list[str] = []
    cursor = 0
    for match in placeholder_regex(placeholders).finditer(fragment):
        pieces.append(fragment[cursor:match.start()])
        tag = tag_by_placeholder[match.group(0)]
        index = indices.get(tag, 0)
        values = values_by_tag[tag]
        if index >= len(values):
            raise ValueError(f"Value stream exhausted while rendering relation fragment for tag {tag}")
        pieces.append(values[index])
        indices[tag] = index + 1
        cursor = match.end()
    pieces.append(fragment[cursor:])
    return "".join(pieces)


def apply_one_relation_candidate(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    left_tag: str,
    right_tag: str,
    literal: str,
) -> tuple[str, dict[str, list[str]], int]:
    return transformed_text, values_by_tag, 0


def relation_pair_delta_possible(values: list[str], literal: str) -> bool:
    return False


def apply_relation_streams(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], int]:
    return transformed_text, values_by_tag, placeholders, accepted_specs, 0


def apply_residual_line_transducer(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    """Extract remaining unstable template slots after LLM semantic functions.

    This is the algorithmic bridge between the LLM-composed source separation
    and the old high-ratio line transducer: once high-confidence fields have
    been replaced, the residual stream is scanned for token positions whose
    local context is stable but whose values vary.  Admission is by a proxy
    codelength gate, not by dataset profile or archive-level racing.
    """
    lines = dataset_extract.split_physical_lines(transformed_text)
    if not lines:
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_slots": 0,
            "candidate_count": 0,
            "admitted_slots": 0,
        }

    line_count = max(1, len(lines))
    min_support = args.residual_line_min_support
    if min_support <= 0:
        min_support = max(20, min(200, line_count // 1000))
    alpha_contexts = dataset_extract._line_transducer_discover_alpha_contexts(
        lines,
        min_support=min_support,
        min_distinct=args.residual_line_alpha_min_distinct,
        max_top_ratio=args.residual_line_alpha_max_top_ratio,
    )

    family_values: dict[tuple[tuple[str, ...], int, str], list[str]] = {}
    candidate_slots = 0
    for line in lines:
        matches = list(dataset_extract._template_cache_line_tokens(line))
        tokens = [match.group(0) for match in matches]
        if not tokens:
            continue
        template_key = dataset_extract._line_transducer_template_key(tokens, alpha_contexts)
        for slot_index, token in enumerate(tokens):
            if not dataset_extract._line_transducer_is_value_candidate(tokens, slot_index, alpha_contexts):
                continue
            shape = dataset_extract._line_transducer_token_shape(token)
            family_key = (template_key, slot_index, shape)
            if family_key not in family_values:
                candidate_slots += 1
            family_values.setdefault(family_key, []).append(token)

    used_tags = set(placeholders) | {spec.tag for spec in accepted_specs}
    candidates: list[dataset_extract.LineTransducerCandidate] = []
    tag_index = 0
    for family_key, values in family_values.items():
        count = len(values)
        distinct = len(set(values))
        if count < min_support or distinct < 2:
            continue
        tag = allocate_unique_tag(f"LT{tag_index}", used_tags)
        tag_index += 1
        spec = dataset_extract._template_cache_candidate_spec(tag, values, family_key)
        if spec is None and args.residual_line_allow_string_slots:
            # The older transducer skipped slots whose best side codec was a
            # plain string stream.  After LLM semantic extraction, however,
            # those slots often dominate residual template entropy (e.g.,
            # OpenSSH usernames and reverse-DNS names).  Let the MDL score,
            # not the value type, decide whether moving them out of the main
            # template stream is worthwhile.
            pattern = "template-slot:" + json.dumps(
                {
                    "slot": family_key[1],
                    "shape": family_key[2],
                    "template": list(family_key[0])[:64],
                },
                ensure_ascii=True,
                separators=(",", ":"),
            )
            spec = dataset_extract.ExtractSpec(tag=tag, pattern=pattern, kind="auto")
        if spec is None:
            continue
        placeholder = dataset_extract.choose_placeholder(original_text + transformed_text, args.dataset, spec.tag)
        score = dataset_extract._line_transducer_candidate_score(values, placeholder, spec)
        if score <= args.residual_line_min_score:
            continue
        candidates.append((score, count, distinct, family_key, spec, placeholder))

    candidates.sort(key=lambda item: (item[0], item[1], -item[2]), reverse=True)
    selected = candidates[: args.residual_line_max_specs]
    if not selected:
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_slots": candidate_slots,
            "candidate_count": len(candidates),
            "admitted_slots": 0,
        }

    residual_text, residual_values, _line_specs_meta = dataset_extract._line_transducer_apply_selected(
        lines,
        alpha_contexts,
        selected,
        {},
    )
    for tag, values in residual_values.items():
        if values:
            values_by_tag[tag] = values
    for _score, _count, _distinct, _family_key, spec, placeholder in selected:
        if not residual_values.get(spec.tag):
            continue
        placeholders[spec.tag] = placeholder
        program: dict[str, object] = {}
        if spec.context_tag:
            try:
                program = json.loads(spec.context_tag)
            except Exception:
                program = {}
        accepted_specs.append(
            CandidateSpec(
                tag=spec.tag,
                pattern=spec.pattern,
                replacement=spec.replacement or "",
                program=program,
                store_group=spec.store_group,
                kind=spec.kind,
                semantic_key="",
            )
        )
    return residual_text, values_by_tag, placeholders, accepted_specs, {
        "candidate_slots": candidate_slots,
        "candidate_count": len(candidates),
        "admitted_slots": len(selected),
    }


def id_mapping_proxy_cost(text: str) -> int:
    """Approximate the cost of the exact residual line-ID core.

    The real archive stores templates and IDs in separate files before the
    outer tar.xz pass.  This proxy is deliberately cheap enough for admission
    decisions but still captures the key Linux failure mode: residual values
    exploding the number and size of unique line templates.
    """
    templates, ids = build_id_mapping(text)
    id_bytes = bytearray()
    for template_id in ids:
        id_bytes.extend(base.encode_varint(template_id))
    return dataset_extract.compressed_parts_cost([
        dataset_extract.encode_string_stream_bytes(templates),
        bytes(id_bytes),
    ])


def residual_template_proxy_cost(lines: list[str]) -> int:
    templates: list[str] = []
    ids: list[int] = []
    id_by_line: dict[str, int] = {}
    for line in lines:
        template_id = id_by_line.get(line)
        if template_id is None:
            template_id = len(templates)
            id_by_line[line] = template_id
            templates.append(line)
        ids.append(template_id)
    id_bytes = bytearray()
    for template_id in ids:
        id_bytes.extend(base.encode_varint(template_id))
    return dataset_extract.compressed_parts_cost([
        dataset_extract.encode_string_stream_bytes(templates),
        bytes(id_bytes),
    ])


@dataclass(frozen=True)
class ResidualVariableLinePlan:
    line: str
    spans: tuple[tuple[int, int], ...]
    tokens: tuple[str, ...]
    group_keys: tuple[object | None, ...]


def residual_context_should_ignore_tags() -> bool:
    return os.environ.get("PARE_RESIDUAL_IGNORE_TAG_CONTEXT", "0") in {"1", "true", "True"}


def residual_context_anchor_token(
    tokens: tuple[str, ...],
    start_index: int,
    step: int,
    protected_placeholders: set[str],
) -> str:
    """Return the nearest context anchor, optionally skipping semantic tags."""
    if not residual_context_should_ignore_tags():
        if 0 <= start_index < len(tokens):
            return dataset_extract._line_transducer_anchor_token(tokens[start_index])
        return "<BOL>" if step < 0 else "<EOL>"

    index = start_index
    while 0 <= index < len(tokens):
        token = tokens[index]
        if token not in protected_placeholders and not dataset_extract._line_transducer_is_placeholder(token):
            return dataset_extract._line_transducer_anchor_token(token)
        index += step
    return "<BOL>" if step < 0 else "<EOL>"


def residual_token_special_signature(token: str) -> str:
    """Group residual variables by the exact non-alphanumeric skeleton of the token."""
    signature = "".join(ch for ch in token if not ch.isalnum())
    return signature or "<NO_SPECIAL>"


def prepare_residual_variable_line_plans(
    lines: list[str],
    alpha_contexts: set[tuple[str, str, str]],
    mode: str,
    protected_placeholders: set[str],
    min_family_support: int = 0,
) -> list[ResidualVariableLinePlan]:
    """Compile residual-variable slots once, then reuse them for scoring.

    Candidate admission may replay dozens of possible residual streams over the
    same block.  The group key for a token depends only on the line, mode, and
    alpha-context set, not on the specific candidate being scored, so compiling
    it once preserves the exact decision logic while avoiding repeated regex
    tokenization and shape checks.
    """
    plans: list[ResidualVariableLinePlan] = []
    plan_cache: dict[str, ResidualVariableLinePlan] = {}
    family_support: Counter[tuple[str, ...]] = Counter()
    line_family_cache: dict[str, tuple[str, ...]] = {}
    if min_family_support > 0:
        for line in lines:
            cached_family = line_family_cache.get(line)
            if cached_family is None:
                matches_for_family = dataset_extract._template_cache_line_tokens(line)
                tokens_for_family = [match.group(0) for match in matches_for_family]
                cached_family = dataset_extract._line_transducer_template_key(tokens_for_family, alpha_contexts) if tokens_for_family else ()
                line_family_cache[line] = cached_family
            family_support[cached_family] += 1

    for line in lines:
        cached = plan_cache.get(line)
        if cached is not None:
            plans.append(cached)
            continue
        matches = dataset_extract._template_cache_line_tokens(line)
        tokens = tuple(match.group(0) for match in matches)
        if not tokens:
            plan = ResidualVariableLinePlan(line=line, spans=(), tokens=(), group_keys=())
            plan_cache[line] = plan
            plans.append(plan)
            continue
        token_list = list(tokens)
        template_key = (
            line_family_cache.get(line)
            if min_family_support > 0
            else dataset_extract._line_transducer_template_key(token_list, alpha_contexts)
        )
        if template_key is None:
            template_key = dataset_extract._line_transducer_template_key(token_list, alpha_contexts)
        family_is_large_enough = (
            min_family_support <= 0
            or family_support.get(template_key, 0) > min_family_support
        )
        spans: list[tuple[int, int]] = []
        group_keys: list[object | None] = []
        for slot_index, match in enumerate(matches):
            token = tokens[slot_index]
            spans.append((match.start(), match.end()))
            if (
                not family_is_large_enough
                or
                token in protected_placeholders
                or not dataset_extract._line_transducer_is_value_candidate(token_list, slot_index, alpha_contexts)
            ):
                group_keys.append(None)
                continue
            shape = dataset_extract._line_transducer_token_shape(token)
            if mode == "global":
                group_key: object = ("global",)
            elif mode == "shape":
                group_key = ("shape", shape)
            elif mode == "context":
                left = residual_context_anchor_token(tokens, slot_index - 1, -1, protected_placeholders)
                right = residual_context_anchor_token(tokens, slot_index + 1, 1, protected_placeholders)
                group_key = ("context", left, shape, right)
            elif mode == "left_context_shape":
                left = residual_context_anchor_token(tokens, slot_index - 1, -1, protected_placeholders)
                group_key = ("left_context_shape", left, shape)
            elif mode == "left_context_special":
                left = residual_context_anchor_token(tokens, slot_index - 1, -1, protected_placeholders)
                special_signature = residual_token_special_signature(token)
                group_key = ("left_context_special", left, special_signature)
            else:
                group_key = ("template", template_key)
            group_keys.append(group_key)
        plan = ResidualVariableLinePlan(
            line=line,
            spans=tuple(spans),
            tokens=tokens,
            group_keys=tuple(group_keys),
        )
        plan_cache[line] = plan
        plans.append(plan)
    return plans


def residual_variable_apply_prepared_groups_to_lines(
    line_plans: list[ResidualVariableLinePlan],
    selected_groups: dict[object, tuple[str, str]],
) -> tuple[list[str], dict[str, list[str]]]:
    values_by_tag: dict[str, list[str]] = {
        tag: []
        for tag, _placeholder in selected_groups.values()
    }
    transformed_lines: list[str] = []
    line_cache: dict[str, tuple[str, dict[str, list[str]]]] = {}
    for plan in line_plans:
        cached = line_cache.get(plan.line)
        if cached is not None:
            cached_line, cached_values = cached
            transformed_lines.append(cached_line)
            for tag, values in cached_values.items():
                values_by_tag[tag].extend(values)
            continue
        if not plan.tokens:
            transformed_lines.append(plan.line)
            line_cache[plan.line] = (plan.line, {})
            continue
        pieces: list[str] = []
        cursor = 0
        line_values: dict[str, list[str]] = {}
        for slot_index, token in enumerate(plan.tokens):
            start, end = plan.spans[slot_index]
            pieces.append(plan.line[cursor:start])
            group_key = plan.group_keys[slot_index]
            selected = selected_groups.get(group_key) if group_key is not None else None
            if selected is None:
                pieces.append(token)
            else:
                tag, placeholder = selected
                line_values.setdefault(tag, []).append(token)
                pieces.append(placeholder)
            cursor = end
        pieces.append(plan.line[cursor:])
        transformed_line = "".join(pieces)
        transformed_lines.append(transformed_line)
        for tag, values in line_values.items():
            values_by_tag[tag].extend(values)
        line_cache[plan.line] = (transformed_line, line_values)
    return transformed_lines, values_by_tag


def residual_variable_apply_prepared_groups(
    line_plans: list[ResidualVariableLinePlan],
    selected_groups: dict[object, tuple[str, str]],
) -> tuple[str, dict[str, list[str]]]:
    transformed_lines, values_by_tag = residual_variable_apply_prepared_groups_to_lines(
        line_plans,
        selected_groups,
    )
    return "".join(transformed_lines), values_by_tag


def apply_residual_line_bundle_stream(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    """Put all remaining residual value-like tokens into one ordered stream."""
    op_start = time.perf_counter()
    lines = dataset_extract.split_physical_lines(transformed_text)
    record_operation_timing(args, "residual_variable.line_bundle.split_lines", time.perf_counter() - op_start, count=len(lines))
    if not lines:
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_groups": 0,
            "admitted_groups": 0,
            "values": 0,
        }

    min_support = int(getattr(args, "residual_variable_min_support", 0) or 0)
    if min_support <= 0:
        min_support = max(20, min(200, len(lines) // 1000))
    protected_placeholders = set(placeholders.values())

    op_start = time.perf_counter()
    alpha_contexts = dataset_extract._line_transducer_discover_alpha_contexts(
        lines,
        min_support=min_support,
        min_distinct=args.residual_line_alpha_min_distinct,
        max_top_ratio=args.residual_line_alpha_max_top_ratio,
    )
    record_operation_timing(args, "residual_variable.line_bundle.discover_alpha_contexts", time.perf_counter() - op_start)

    used_tags = set(placeholders) | {spec.tag for spec in accepted_specs}
    tag = allocate_unique_tag("RB", used_tags)
    placeholder = dataset_extract.choose_placeholder(original_text + transformed_text, args.dataset, tag)
    bundled_lines: list[str] = []
    bundle_values: list[str] = []
    changed_lines = 0

    op_start = time.perf_counter()
    for line in lines:
        matches = dataset_extract._template_cache_line_tokens(line)
        if not matches:
            bundled_lines.append(line)
            continue
        tokens = [match.group(0) for match in matches]
        pieces: list[str] = []
        cursor = 0
        changed = False
        for slot_index, match in enumerate(matches):
            token = tokens[slot_index]
            pieces.append(line[cursor:match.start()])
            if (
                token in protected_placeholders
                or dataset_extract._line_transducer_is_placeholder(token)
                or not dataset_extract._line_transducer_is_value_candidate(tokens, slot_index, alpha_contexts)
            ):
                pieces.append(token)
            else:
                pieces.append(placeholder)
                bundle_values.append(token)
                changed = True
            cursor = match.end()
        pieces.append(line[cursor:])
        bundled_lines.append("".join(pieces))
        if changed:
            changed_lines += 1
    record_operation_timing(args, "residual_variable.line_bundle.replay", time.perf_counter() - op_start, count=len(lines))

    if not bundle_values:
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_groups": 1,
            "admitted_groups": 0,
            "values": 0,
            "changed_lines": 0,
        }

    values_by_tag[tag] = bundle_values
    placeholders[tag] = placeholder
    accepted_specs.append(
        CandidateSpec(
            tag=tag,
            pattern="residual-line-bundle",
            replacement="{placeholder}",
            program={"op": "auto_codec"},
            store_group=1,
            kind="auto",
            semantic_key="residual_line_bundle",
        )
    )
    return "".join(bundled_lines), values_by_tag, placeholders, accepted_specs, {
        "candidate_groups": 1,
        "admitted_groups": 1,
        "values": len(bundle_values),
        "changed_lines": changed_lines,
    }


def residual_schema_key_to_json(group_key: object) -> object:
    if isinstance(group_key, tuple):
        return [residual_schema_key_to_json(item) for item in group_key]
    return group_key


def residual_schema_key_from_json(raw: object) -> object:
    if isinstance(raw, list):
        return tuple(residual_schema_key_from_json(item) for item in raw)
    return raw


def load_residual_schema_plan(path: str) -> dict[str, Any] | None:
    if not path:
        return None
    plan_path = Path(path)
    if not plan_path.is_file():
        return None
    data = json.loads(plan_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return None
    passes = data.get("passes")
    if not isinstance(passes, list):
        return None
    return data


def capture_residual_schema_pass(
    args: argparse.Namespace,
    mode: str,
    alpha_contexts: set[tuple[str, str, str]],
    selected: list[tuple[int, int, object, dataset_extract.ExtractSpec, str]],
) -> None:
    capture = getattr(args, "_residual_schema_plan_capture", None)
    if not isinstance(capture, list):
        return
    capture.append({
        "mode": mode,
        "alpha_contexts": [list(item) for item in sorted(alpha_contexts)],
        "groups": [
            {
                "group_key": residual_schema_key_to_json(group_key),
                "tag_base": spec.tag,
            }
            for _score, _count, group_key, spec, _placeholder in selected
        ],
    })


def residual_schema_apply_context_groups_fast(
    lines: list[str],
    selected_groups: dict[object, tuple[str, str]],
    alpha_contexts: set[tuple[str, str, str]],
    protected_placeholders: set[str],
) -> tuple[str, dict[str, list[str]]]:
    values_by_tag: dict[str, list[str]] = {
        tag: []
        for tag, _placeholder in selected_groups.values()
    }
    if (
        _pare_cpp_accel is not None
        and os.environ.get("PARE_CPP_RESIDUAL_CONTEXT_REPLAY", "1") in {"1", "true", "True"}
        and not residual_context_should_ignore_tags()
    ):
        cpp_groups: list[tuple[str, str, str, str, str]] = []
        cpp_supported = True
        for group_key, (tag, placeholder) in selected_groups.items():
            if (
                isinstance(group_key, tuple)
                and len(group_key) == 4
                and group_key[0] == "context"
                and all(isinstance(part, str) for part in group_key[1:])
            ):
                _kind, left, shape, right = group_key
                cpp_groups.append((left, shape, right, tag, placeholder))
            else:
                cpp_supported = False
                break
        if cpp_supported and cpp_groups:
            try:
                replayed = _pare_cpp_accel.residual_context_replay(
                    lines,
                    cpp_groups,
                    list(alpha_contexts),
                    list(protected_placeholders),
                )
                if replayed is not None:
                    replayed_text, replayed_values = replayed
                    if isinstance(replayed_text, str) and isinstance(replayed_values, dict):
                        return replayed_text, {
                            str(tag): list(values)
                            for tag, values in replayed_values.items()
                        }
            except Exception:
                pass
    transformed_lines: list[str] = []
    line_cache: dict[str, tuple[str, dict[str, tuple[str, ...]]]] = {}
    for line in lines:
        cached = line_cache.get(line)
        if cached is not None:
            transformed_line, cached_values = cached
            transformed_lines.append(transformed_line)
            for tag, values in cached_values.items():
                values_by_tag[tag].extend(values)
            continue
        matches = dataset_extract._template_cache_line_tokens(line)
        if not matches:
            transformed_lines.append(line)
            line_cache[line] = (line, {})
            continue
        tokens = tuple(match.group(0) for match in matches)
        pieces: list[str] = []
        cursor = 0
        line_values: dict[str, list[str]] = {}
        changed = False
        for slot_index, token in enumerate(tokens):
            start, end = matches[slot_index].start(), matches[slot_index].end()
            pieces.append(line[cursor:start])
            selected: tuple[str, str] | None = None
            if token not in protected_placeholders:
                is_digit_or_special = dataset_extract._line_transducer_is_digit_or_special_value(token)
                is_alpha_candidate = False
                if not is_digit_or_special and dataset_extract._line_transducer_is_plain_alpha(token):
                    alpha_key = dataset_extract._line_transducer_alpha_context_key(list(tokens), slot_index)
                    is_alpha_candidate = alpha_key in alpha_contexts
                if is_digit_or_special or is_alpha_candidate:
                    left = residual_context_anchor_token(tokens, slot_index - 1, -1, protected_placeholders)
                    right = residual_context_anchor_token(tokens, slot_index + 1, 1, protected_placeholders)
                    shape = dataset_extract._line_transducer_token_shape(token)
                    selected = selected_groups.get(("context", left, shape, right))
            if selected is None:
                pieces.append(token)
            else:
                tag, placeholder = selected
                pieces.append(placeholder)
                line_values.setdefault(tag, []).append(token)
                changed = True
            cursor = end
        pieces.append(line[cursor:])
        transformed_line = "".join(pieces) if changed else line
        transformed_lines.append(transformed_line)
        cached_values = {tag: tuple(values) for tag, values in line_values.items()}
        for tag, values in line_values.items():
            values_by_tag[tag].extend(values)
        if len(line_cache) < 262_144:
            line_cache[line] = (transformed_line, cached_values)
    return "".join(transformed_lines), values_by_tag


def residual_schema_apply_context_groups_text_fast(
    transformed_text: str,
    selected_groups: dict[object, tuple[str, str]],
    alpha_contexts: set[tuple[str, str, str]],
    protected_placeholders: set[str],
) -> tuple[str, dict[str, list[str]]]:
    if (
        _pare_cpp_accel is not None
        and hasattr(_pare_cpp_accel, "residual_context_replay_text")
        and os.environ.get("PARE_CPP_RESIDUAL_CONTEXT_REPLAY_TEXT", "0") in {"1", "true", "True"}
        and not residual_context_should_ignore_tags()
    ):
        cpp_groups: list[tuple[str, str, str, str, str]] = []
        cpp_supported = True
        for group_key, (tag, placeholder) in selected_groups.items():
            if (
                isinstance(group_key, tuple)
                and len(group_key) == 4
                and group_key[0] == "context"
                and all(isinstance(part, str) for part in group_key[1:])
            ):
                _kind, left, shape, right = group_key
                cpp_groups.append((left, shape, right, tag, placeholder))
            else:
                cpp_supported = False
                break
        if cpp_supported and cpp_groups:
            try:
                replayed = _pare_cpp_accel.residual_context_replay_text(
                    transformed_text,
                    cpp_groups,
                    list(alpha_contexts),
                    list(protected_placeholders),
                )
                if replayed is not None:
                    replayed_text, replayed_values = replayed
                    if isinstance(replayed_text, str) and isinstance(replayed_values, dict):
                        return replayed_text, {
                            str(tag): list(values)
                            for tag, values in replayed_values.items()
                        }
            except Exception:
                pass
    return residual_schema_apply_context_groups_fast(
        dataset_extract.split_physical_lines(transformed_text),
        selected_groups,
        alpha_contexts,
        protected_placeholders,
    )


def apply_residual_schema_plan(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
    plan: dict[str, Any],
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    total_candidate_groups = 0
    total_admitted_groups = 0
    total_values = 0
    pass_count = 0
    skipped_groups = 0
    for pass_item in plan.get("passes", []):
        if not isinstance(pass_item, dict):
            continue
        mode = str(pass_item.get("mode", ""))
        if mode not in {"global", "shape", "context", "left_context_shape", "left_context_special", "template"}:
            continue
        raw_alpha_contexts = pass_item.get("alpha_contexts", [])
        alpha_contexts = {
            tuple(item)  # type: ignore[arg-type]
            for item in raw_alpha_contexts
            if isinstance(item, list) and len(item) == 3 and all(isinstance(part, str) for part in item)
        }
        group_items = pass_item.get("groups", [])
        if not isinstance(group_items, list) or not group_items:
            continue
        used_tags = set(placeholders) | {spec.tag for spec in accepted_specs}
        placeholder_source_text = original_text + transformed_text
        selected_groups: dict[object, tuple[str, str]] = {}
        group_specs: dict[object, dataset_extract.ExtractSpec] = {}
        for index, group_item in enumerate(group_items):
            if not isinstance(group_item, dict):
                continue
            group_key = residual_schema_key_from_json(group_item.get("group_key"))
            tag_base = str(group_item.get("tag_base", f"RP{index}"))
            tag = allocate_unique_tag(tag_base, used_tags)
            used_tags.add(tag)
            placeholder = dataset_extract.choose_placeholder(placeholder_source_text, args.dataset, tag)
            selected_groups[group_key] = (tag, placeholder)
            group_specs[group_key] = dataset_extract.ExtractSpec(
                tag=tag,
                pattern="residual-variable-plan:" + json.dumps(group_key, ensure_ascii=True, default=str),
                kind="auto",
                store_group=1,
                replacement="{placeholder}",
            )
        total_candidate_groups += len(selected_groups)
        if not selected_groups:
            continue

        if mode == "context":
            op_start = time.perf_counter()
            transformed_text, residual_values = residual_schema_apply_context_groups_text_fast(
                transformed_text,
                selected_groups,
                alpha_contexts,
                set(placeholders.values()),
            )
            record_operation_timing(args, f"residual_schema_plan.{mode}.fast_replay", time.perf_counter() - op_start)
        else:
            op_start = time.perf_counter()
            lines = dataset_extract.split_physical_lines(transformed_text)
            protected_placeholders = set(placeholders.values())
            line_plans = prepare_residual_variable_line_plans(
                lines,
                alpha_contexts,
                mode,
                protected_placeholders,
                min_family_support=int(getattr(args, "residual_fallback_min_family_support", 0) or 0),
            )
            record_operation_timing(args, f"residual_schema_plan.{mode}.prepare_line_plans", time.perf_counter() - op_start, count=len(lines))
            op_start = time.perf_counter()
            transformed_text, residual_values = residual_variable_apply_prepared_groups(
                line_plans,
                selected_groups,
            )
            record_operation_timing(args, f"residual_schema_plan.{mode}.final_replay", time.perf_counter() - op_start, count=len(lines))

        admitted = 0
        value_count = 0
        for group_key, (tag, placeholder) in selected_groups.items():
            values = residual_values.get(tag, [])
            if not values:
                skipped_groups += 1
                continue
            spec = group_specs[group_key]
            values_by_tag[tag] = values
            placeholders[tag] = placeholder
            accepted_specs.append(
                CandidateSpec(
                    tag=tag,
                    pattern=spec.pattern,
                    replacement=spec.replacement or "",
                    program={},
                    store_group=spec.store_group,
                    kind=spec.kind,
                    semantic_key="",
                )
            )
            admitted += 1
            value_count += len(values)
        total_admitted_groups += admitted
        total_values += value_count
        pass_count += 1

    return transformed_text, values_by_tag, placeholders, accepted_specs, {
        "candidate_groups": total_candidate_groups,
        "admitted_groups": total_admitted_groups,
        "values": total_values,
        "schema_plan_used": int(pass_count > 0),
        "schema_plan_passes": pass_count,
        "schema_plan_skipped_groups": skipped_groups,
    }


def residual_variable_shape_gate_stats(
    transformed_text: str,
    placeholders: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, int]:
    """Cheaply estimate whether a shape-level residual pass can collapse lines.

    The expensive shape pass scores each candidate with a full replay and line
    dictionary proxy.  For planning we only need to know whether shape grouping
    has obvious main-stream upside.  This probe masks all eligible shape groups
    once and compares the unique residual-line count before/after.
    """
    op_start = time.perf_counter()
    lines = dataset_extract.split_physical_lines(transformed_text)
    record_operation_timing(args, "residual_variable.shape_gate.split_lines", time.perf_counter() - op_start)
    if not lines:
        return {
            "enabled": int(getattr(args, "residual_variable_shape_gate", False)),
            "run_shape": 0,
            "before_templates": 0,
            "after_templates": 0,
            "collapse": 0,
            "eligible_groups": 0,
            "eligible_values": 0,
            "raw_main": 0,
        }
    min_support = args.residual_variable_min_support
    if min_support <= 0:
        min_support = max(20, min(200, len(lines) // 1000))
    value_penalty = int(getattr(args, "residual_variable_value_penalty", 4) or 4)

    protected_placeholders = set(placeholders.values())
    op_start = time.perf_counter()
    alpha_contexts = dataset_extract._line_transducer_discover_alpha_contexts(
        lines,
        min_support=min_support,
        min_distinct=args.residual_line_alpha_min_distinct,
        max_top_ratio=args.residual_line_alpha_max_top_ratio,
    )
    record_operation_timing(args, "residual_variable.shape_gate.discover_alpha_contexts", time.perf_counter() - op_start)

    op_start = time.perf_counter()
    line_plans = prepare_residual_variable_line_plans(
        lines,
        alpha_contexts,
        "shape",
        protected_placeholders,
        min_family_support=int(getattr(args, "residual_fallback_min_family_support", 0) or 0),
    )
    record_operation_timing(args, "residual_variable.shape_gate.prepare_line_plans", time.perf_counter() - op_start, count=len(lines))

    groups: dict[object, list[str]] = {}
    group_line_indexes: dict[object, set[int]] = {}
    op_start = time.perf_counter()
    for line_index, plan in enumerate(line_plans):
        for token, group_key in zip(plan.tokens, plan.group_keys):
            if group_key is None:
                continue
            groups.setdefault(group_key, []).append(token)
            group_line_indexes.setdefault(group_key, set()).add(line_index)
    record_operation_timing(args, "residual_variable.shape_gate.collect_groups", time.perf_counter() - op_start, count=len(lines))

    eligible_groups: list[tuple[int, int, object, int]] = []
    raw_total = 0
    value_total = 0
    op_start = time.perf_counter()
    for group_key, values in groups.items():
        if len(values) < min_support:
            continue
        raw_main = sum(len(value.encode("latin-1")) for value in values)
        cheap_score = raw_main - (value_penalty * len(values))
        distinct_ok = len(set(values)) >= args.residual_variable_min_distinct
        avglen_ok = cheap_score > args.residual_variable_min_score
        if not (distinct_ok or avglen_ok):
            continue
        eligible_groups.append((cheap_score, raw_main, group_key, len(values)))
        raw_total += raw_main
        value_total += len(values)
    eligible_groups.sort(key=lambda item: (item[0], item[1]), reverse=True)
    shape_candidate_limit = (
        args.residual_variable_shape_max_candidate_groups
        if args.residual_variable_shape_max_candidate_groups > 0
        else args.residual_variable_max_candidate_groups
    )
    if shape_candidate_limit > 0:
        eligible_groups = eligible_groups[: shape_candidate_limit]
    record_operation_timing(args, "residual_variable.shape_gate.filter_eligible_groups", time.perf_counter() - op_start, count=len(groups))
    setattr(args, "_residual_variable_shape_precomputed", {
        "text_len": len(transformed_text),
        "text_prefix": transformed_text[:128],
        "lines": lines,
        "alpha_contexts": alpha_contexts,
        "line_plans": line_plans,
        "groups": groups,
        "group_line_indexes": group_line_indexes,
    })

    before_templates = len(set(lines))
    if not eligible_groups:
        return {
            "enabled": 1,
            "run_shape": 0,
            "before_templates": before_templates,
            "after_templates": before_templates,
            "collapse": 0,
            "eligible_groups": 0,
            "eligible_values": 0,
            "raw_main": 0,
        }

    selected_groups = {
        group_key: ("SG", f"<SG{index}>")
        for index, (_cheap_score, _raw_main, group_key, _count) in enumerate(eligible_groups)
    }
    op_start = time.perf_counter()
    masked_lines, _masked_values = residual_variable_apply_prepared_groups_to_lines(line_plans, selected_groups)
    after_templates = len(set(masked_lines))
    record_operation_timing(args, "residual_variable.shape_gate.mask_and_count_templates", time.perf_counter() - op_start, count=len(lines))
    collapse = max(0, before_templates - after_templates)
    run_shape = (
        collapse >= args.residual_variable_shape_gate_min_collapse
        and collapse * 100 >= before_templates * args.residual_variable_shape_gate_min_collapse_pct
    )
    return {
        "enabled": 1,
        "run_shape": int(run_shape),
        "before_templates": before_templates,
        "after_templates": after_templates,
        "collapse": collapse,
        "eligible_groups": len(eligible_groups),
        "eligible_values": value_total,
        "raw_main": raw_total,
    }


def residual_variable_shape_pregate_stats(
    transformed_text: str,
    placeholders: dict[str, str],
    args: argparse.Namespace,
) -> dict[str, int]:
    """Cheap candidate-support probe before the full shape gate.

    This probe skips alpha-context discovery and only tests digit/special
    shapes that already satisfy the same support and cheap-gain gates as the
    full shape planner. If that deterministic subset cannot collapse the line
    dictionary, the expensive full shape gate is unlikely to pay for itself.
    """
    lines = dataset_extract.split_physical_lines(transformed_text)
    if not lines:
        return {
            "enabled": 1,
            "run_full_gate": 0,
            "before_templates": 0,
            "after_templates": 0,
            "collapse": 0,
        }
    min_support = args.residual_variable_min_support
    if min_support <= 0:
        min_support = max(20, min(200, len(lines) // 1000))
    value_penalty = int(getattr(args, "residual_variable_value_penalty", 4) or 4)

    protected_placeholders = set(placeholders.values())

    def compute_collapse(candidate_lines: list[str], support_floor: int) -> dict[str, int]:
        before_templates = len(set(candidate_lines))
        shape_values: dict[str, list[str]] = {}
        for line in candidate_lines:
            matches = dataset_extract._template_cache_line_tokens(line)
            for match in matches:
                token = match.group(0)
                if token in protected_placeholders or dataset_extract._line_transducer_is_placeholder(token):
                    continue
                if not dataset_extract._line_transducer_is_digit_or_special_value(token):
                    continue
                shape = dataset_extract._line_transducer_token_shape(token)
                shape_values.setdefault(shape, []).append(token)
        eligible_shapes: list[tuple[int, int, str, int]] = []
        for shape, values in shape_values.items():
            if len(values) < support_floor:
                continue
            raw_main = sum(len(value.encode("latin-1")) for value in values)
            cheap_score = raw_main - (value_penalty * len(values))
            distinct_ok = len(set(values)) >= args.residual_variable_min_distinct
            avglen_ok = cheap_score > args.residual_variable_min_score
            if not (distinct_ok or avglen_ok):
                continue
            eligible_shapes.append((cheap_score, raw_main, shape, len(values)))
        eligible_shapes.sort(key=lambda item: (item[0], item[1]), reverse=True)
        shape_candidate_limit = (
            args.residual_variable_shape_max_candidate_groups
            if args.residual_variable_shape_max_candidate_groups > 0
            else args.residual_variable_max_candidate_groups
        )
        if shape_candidate_limit > 0:
            eligible_shapes = eligible_shapes[: shape_candidate_limit]
        selected_shapes = {shape for _score, _raw_main, shape, _count in eligible_shapes}
        if not selected_shapes:
            return {
                "before_templates": before_templates,
                "after_templates": before_templates,
                "collapse": 0,
            }
        masked_lines: list[str] = []
        for line in candidate_lines:
            matches = dataset_extract._template_cache_line_tokens(line)
            if not matches:
                masked_lines.append(line)
                continue
            pieces: list[str] = []
            cursor = 0
            changed = False
            for match in matches:
                token = match.group(0)
                pieces.append(line[cursor:match.start()])
                if token in protected_placeholders or dataset_extract._line_transducer_is_placeholder(token):
                    pieces.append(token)
                elif (
                    dataset_extract._line_transducer_is_digit_or_special_value(token)
                    and dataset_extract._line_transducer_token_shape(token) in selected_shapes
                ):
                    pieces.append("<PG:" + dataset_extract._line_transducer_token_shape(token) + ">")
                    changed = True
                else:
                    pieces.append(token)
                cursor = match.end()
            pieces.append(line[cursor:])
            masked_lines.append("".join(pieces) if changed else line)
        after_templates = len(set(masked_lines))
        return {
            "before_templates": before_templates,
            "after_templates": after_templates,
            "collapse": max(0, before_templates - after_templates),
        }

    sample_lines = int(getattr(args, "residual_variable_shape_pregate_sample_lines", 0) or 0)
    sample_skip_pct = float(getattr(args, "residual_variable_shape_pregate_sample_skip_pct", 0.0) or 0.0)
    if sample_lines > 0 and sample_skip_pct > 0 and len(lines) > sample_lines:
        op_start = time.perf_counter()
        sample_count = max(1, min(sample_lines, len(lines)))
        sampled_lines = [lines[(index * len(lines)) // sample_count] for index in range(sample_count)]
        sample_support = max(8, min(min_support, sample_count // 64))
        sample_stats = compute_collapse(sampled_lines, sample_support)
        record_operation_timing(args, "residual_variable.shape_pregate_sample", time.perf_counter() - op_start, count=sample_count)
        sample_before = max(1, sample_stats["before_templates"])
        sample_pct = (sample_stats["collapse"] * 100.0) / sample_before
        projected_collapse = sample_stats["collapse"] * (len(lines) / sample_count)
        if (
            sample_pct < sample_skip_pct
            or projected_collapse < args.residual_variable_shape_gate_min_collapse * 0.5
        ):
            before_templates = len(set(lines))
            return {
                "enabled": 1,
                "run_full_gate": 0,
                "before_templates": before_templates,
                "after_templates": before_templates,
                "collapse": 0,
                "sampled": 1,
                "sample_before_templates": sample_stats["before_templates"],
                "sample_after_templates": sample_stats["after_templates"],
                "sample_collapse": sample_stats["collapse"],
            }

    before_templates = len(set(lines))
    shape_values: dict[str, list[str]] = {}
    for line in lines:
        matches = dataset_extract._template_cache_line_tokens(line)
        for match in matches:
            token = match.group(0)
            if token in protected_placeholders or dataset_extract._line_transducer_is_placeholder(token):
                continue
            if not dataset_extract._line_transducer_is_digit_or_special_value(token):
                continue
            shape = dataset_extract._line_transducer_token_shape(token)
            shape_values.setdefault(shape, []).append(token)
    eligible_shapes: list[tuple[int, int, str, int]] = []
    for shape, values in shape_values.items():
        if len(values) < min_support:
            continue
        raw_main = sum(len(value.encode("latin-1")) for value in values)
        cheap_score = raw_main - (value_penalty * len(values))
        distinct_ok = len(set(values)) >= args.residual_variable_min_distinct
        avglen_ok = cheap_score > args.residual_variable_min_score
        if not (distinct_ok or avglen_ok):
            continue
        eligible_shapes.append((cheap_score, raw_main, shape, len(values)))
    eligible_shapes.sort(key=lambda item: (item[0], item[1]), reverse=True)
    shape_candidate_limit = (
        args.residual_variable_shape_max_candidate_groups
        if args.residual_variable_shape_max_candidate_groups > 0
        else args.residual_variable_max_candidate_groups
    )
    if shape_candidate_limit > 0:
        eligible_shapes = eligible_shapes[: shape_candidate_limit]
    selected_shapes = {shape for _score, _raw_main, shape, _count in eligible_shapes}
    if not selected_shapes:
        return {
            "enabled": 1,
            "run_full_gate": 0,
            "before_templates": before_templates,
            "after_templates": before_templates,
            "collapse": 0,
        }
    masked_lines: list[str] = []
    for line in lines:
        matches = dataset_extract._template_cache_line_tokens(line)
        if not matches:
            masked_lines.append(line)
            continue
        pieces: list[str] = []
        cursor = 0
        changed = False
        for match in matches:
            token = match.group(0)
            pieces.append(line[cursor:match.start()])
            if token in protected_placeholders or dataset_extract._line_transducer_is_placeholder(token):
                pieces.append(token)
            elif (
                dataset_extract._line_transducer_is_digit_or_special_value(token)
                and dataset_extract._line_transducer_token_shape(token) in selected_shapes
            ):
                pieces.append("<PG:" + dataset_extract._line_transducer_token_shape(token) + ">")
                changed = True
            else:
                pieces.append(token)
            cursor = match.end()
        pieces.append(line[cursor:])
        masked_lines.append("".join(pieces) if changed else line)
    after_templates = len(set(masked_lines))
    collapse = max(0, before_templates - after_templates)
    run_full_gate = (
        collapse >= args.residual_variable_shape_gate_min_collapse
        and collapse * 100 >= before_templates * args.residual_variable_shape_gate_min_collapse_pct
    )
    return {
        "enabled": 1,
        "run_full_gate": int(run_full_gate),
        "before_templates": before_templates,
        "after_templates": after_templates,
        "collapse": collapse,
    }


def apply_residual_variable_streams(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    """Route leftover non-semantic variables into verified residual streams.

    This pass is intentionally downstream of LLM semantic extraction.  It asks a
    narrower question: after all meaningful programs have fired, are there still
    digit/special tokens that make the residual line dictionary large?  The
    grouping mode controls whether those leftovers share one stream, one stream
    per value shape, or one stream per residual template family.
    """
    mode = args.residual_variable_stream_mode
    if mode == "none":
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_groups": 0,
            "admitted_groups": 0,
            "values": 0,
        }
    if mode == "line_bundle":
        return apply_residual_line_bundle_stream(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
    residual_schema_plan = getattr(args, "_residual_schema_plan", None)
    if mode == "context_shape" and isinstance(residual_schema_plan, dict):
        return apply_residual_schema_plan(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
            plan=residual_schema_plan,
        )
    if mode == "context_shape":
        context_args = argparse.Namespace(**vars(args))
        context_args.residual_variable_stream_mode = "context"
        op_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            context_stats,
        ) = apply_residual_variable_streams(
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            original_text,
            context_args,
        )
        record_operation_timing(args, "residual_variable.context_pass_total", time.perf_counter() - op_start)
        shape_gate_stats = {
            "enabled": int(getattr(args, "residual_variable_shape_gate", False)),
            "run_shape": 1,
            "before_templates": 0,
            "after_templates": 0,
            "collapse": 0,
            "eligible_groups": 0,
            "eligible_values": 0,
            "raw_main": 0,
        }
        pregate_stats: dict[str, int] | None = None
        if getattr(args, "residual_variable_shape_gate", False):
            op_start = time.perf_counter()
            if getattr(args, "residual_variable_shape_pregate", False):
                pregate_stats = residual_variable_shape_pregate_stats(transformed_text, placeholders, args)
                record_operation_timing(args, "residual_variable.shape_pregate_total", time.perf_counter() - op_start)
                if not pregate_stats.get("run_full_gate", 0):
                    return transformed_text, values_by_tag, placeholders, accepted_specs, {
                        "candidate_groups": context_stats["candidate_groups"],
                        "admitted_groups": context_stats["admitted_groups"],
                        "values": context_stats["values"],
                        "shape_gate_enabled": 1,
                        "shape_gate_run_shape": 0,
                        "shape_gate_before_templates": pregate_stats["before_templates"],
                        "shape_gate_after_templates": pregate_stats["after_templates"],
                        "shape_gate_collapse": pregate_stats["collapse"],
                        "shape_gate_eligible_groups": 0,
                        "shape_gate_eligible_values": 0,
                        "shape_gate_raw_main": 0,
                        "shape_pregate_enabled": 1,
                        "shape_pregate_run_full_gate": 0,
                        "shape_pregate_before_templates": pregate_stats["before_templates"],
                        "shape_pregate_after_templates": pregate_stats["after_templates"],
                        "shape_pregate_collapse": pregate_stats["collapse"],
                    }
                op_start = time.perf_counter()
            shape_gate_stats = residual_variable_shape_gate_stats(transformed_text, placeholders, args)
            record_operation_timing(args, "residual_variable.shape_gate_total", time.perf_counter() - op_start)
            if not shape_gate_stats.get("run_shape", 0):
                return transformed_text, values_by_tag, placeholders, accepted_specs, {
                    "candidate_groups": context_stats["candidate_groups"],
                    "admitted_groups": context_stats["admitted_groups"],
                    "values": context_stats["values"],
                    "shape_gate_enabled": 1,
                    "shape_gate_run_shape": 0,
                    "shape_gate_before_templates": shape_gate_stats["before_templates"],
                    "shape_gate_after_templates": shape_gate_stats["after_templates"],
                    "shape_gate_collapse": shape_gate_stats["collapse"],
                    "shape_gate_eligible_groups": shape_gate_stats["eligible_groups"],
                    "shape_gate_eligible_values": shape_gate_stats["eligible_values"],
                    "shape_gate_raw_main": shape_gate_stats["raw_main"],
                    "shape_pregate_enabled": int(getattr(args, "residual_variable_shape_pregate", False)),
                    "shape_pregate_run_full_gate": 1,
                    "shape_pregate_before_templates": pregate_stats["before_templates"] if pregate_stats else 0,
                    "shape_pregate_after_templates": pregate_stats["after_templates"] if pregate_stats else 0,
                    "shape_pregate_collapse": pregate_stats["collapse"] if pregate_stats else 0,
                }
        shape_args = argparse.Namespace(**vars(args))
        shape_args.residual_variable_stream_mode = "shape"
        op_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            shape_stats,
        ) = apply_residual_variable_streams(
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            original_text,
            shape_args,
        )
        record_operation_timing(args, "residual_variable.shape_pass_total", time.perf_counter() - op_start)
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_groups": context_stats["candidate_groups"] + shape_stats["candidate_groups"],
            "admitted_groups": context_stats["admitted_groups"] + shape_stats["admitted_groups"],
            "values": context_stats["values"] + shape_stats["values"],
            "shape_gate_enabled": int(getattr(args, "residual_variable_shape_gate", False)),
            "shape_gate_run_shape": 1,
            "shape_gate_before_templates": shape_gate_stats["before_templates"],
            "shape_gate_after_templates": shape_gate_stats["after_templates"],
            "shape_gate_collapse": shape_gate_stats["collapse"],
            "shape_gate_eligible_groups": shape_gate_stats["eligible_groups"],
            "shape_gate_eligible_values": shape_gate_stats["eligible_values"],
            "shape_gate_raw_main": shape_gate_stats["raw_main"],
            "shape_pregate_enabled": int(getattr(args, "residual_variable_shape_pregate", False)),
            "shape_pregate_run_full_gate": int(getattr(args, "residual_variable_shape_pregate", False)),
            "shape_pregate_before_templates": pregate_stats["before_templates"] if pregate_stats else 0,
            "shape_pregate_after_templates": pregate_stats["after_templates"] if pregate_stats else 0,
            "shape_pregate_collapse": pregate_stats["collapse"] if pregate_stats else 0,
        }

    precomputed = getattr(args, "_residual_variable_shape_precomputed", None) if mode == "shape" else None
    use_precomputed = (
        isinstance(precomputed, dict)
        and precomputed.get("text_len") == len(transformed_text)
        and precomputed.get("text_prefix") == transformed_text[:128]
    )
    if use_precomputed:
        lines = precomputed["lines"]
        record_operation_timing(args, f"residual_variable.{mode}.reuse_shape_gate_precomputed", 0.0, count=len(lines))
    else:
        op_start = time.perf_counter()
        lines = dataset_extract.split_physical_lines(transformed_text)
        record_operation_timing(args, f"residual_variable.{mode}.split_lines", time.perf_counter() - op_start)
    if not lines:
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_groups": 0,
            "admitted_groups": 0,
            "values": 0,
        }

    protected_placeholders = set(placeholders.values())
    min_support = args.residual_variable_min_support
    if min_support <= 0:
        min_support = max(20, min(200, len(lines) // 1000))
    value_penalty = int(getattr(args, "residual_variable_value_penalty", 4) or 4)
    residual_fraction = float(os.environ.get("PARE_RESIDUAL_STREAM_MIN_BLOCK_FRACTION", "0") or 0)
    if residual_fraction > 0:
        min_support = max(min_support, int(math.ceil(len(lines) * residual_fraction)))
    if use_precomputed:
        alpha_contexts = precomputed["alpha_contexts"]
        line_plans = precomputed["line_plans"]
        groups = precomputed["groups"]
        group_line_indexes = precomputed["group_line_indexes"]
    else:
        op_start = time.perf_counter()
        alpha_contexts = dataset_extract._line_transducer_discover_alpha_contexts(
            lines,
            min_support=min_support,
            min_distinct=args.residual_line_alpha_min_distinct,
            max_top_ratio=args.residual_line_alpha_max_top_ratio,
        )
        record_operation_timing(args, f"residual_variable.{mode}.discover_alpha_contexts", time.perf_counter() - op_start)

        op_start = time.perf_counter()
        line_plans = prepare_residual_variable_line_plans(
            lines,
            alpha_contexts,
            mode,
            protected_placeholders,
            min_family_support=int(getattr(args, "residual_fallback_min_family_support", 0) or 0),
        )
        record_operation_timing(args, f"residual_variable.{mode}.prepare_line_plans", time.perf_counter() - op_start, count=len(lines))

        groups: dict[object, list[str]] = {}
        group_line_indexes: dict[object, set[int]] = {}
        op_start = time.perf_counter()
        for line_index, plan in enumerate(line_plans):
            for token, group_key in zip(plan.tokens, plan.group_keys):
                if group_key is None:
                    continue
                groups.setdefault(group_key, []).append(token)
                group_line_indexes.setdefault(group_key, set()).add(line_index)
        record_operation_timing(args, f"residual_variable.{mode}.collect_groups", time.perf_counter() - op_start, count=len(lines))

    needs_full_before_cost = (
        mode in {"global", "shape"}
        and not getattr(args, "residual_variable_feature_mdl", False)
        and not getattr(args, "residual_variable_local_impact_score", False)
    )
    before_cost = 0
    if needs_full_before_cost:
        op_start = time.perf_counter()
        before_cost = id_mapping_proxy_cost(transformed_text)
        record_operation_timing(args, f"residual_variable.{mode}.before_id_mapping_proxy", time.perf_counter() - op_start)
    used_tags = set(placeholders) | {spec.tag for spec in accepted_specs}
    candidates: list[tuple[int, int, object, CandidateSpec, str]] = []
    tag_index = 0
    eligible_groups: list[tuple[int, int, int, object, list[str]]] = []
    op_start = time.perf_counter()
    for group_key, values in groups.items():
        if len(values) < min_support:
            continue
        raw_main = sum(len(value.encode("latin-1")) for value in values)
        # Cheap upper-bound-ish priority: if replacing the values with a short
        # placeholder cannot save much main-stream entropy, do not spend time
        # running the expensive local line-dictionary proxy on this group.
        cheap_score = raw_main - (value_penalty * len(values))
        distinct_ok = len(set(values)) >= args.residual_variable_min_distinct
        avglen_ok = cheap_score > args.residual_variable_min_score
        if not (distinct_ok or avglen_ok):
            continue
        eligible_groups.append((cheap_score, raw_main, len(values), group_key, values))
    eligible_groups.sort(key=lambda item: (item[0], item[1]), reverse=True)
    candidate_group_limit = args.residual_variable_max_candidate_groups
    if mode == "shape" and args.residual_variable_shape_max_candidate_groups > 0:
        candidate_group_limit = args.residual_variable_shape_max_candidate_groups
    if candidate_group_limit > 0:
        eligible_groups = eligible_groups[: candidate_group_limit]
    record_operation_timing(args, f"residual_variable.{mode}.filter_eligible_groups", time.perf_counter() - op_start, count=len(groups))

    def evenly_sample_plans(source_plans: list[ResidualVariableLinePlan], limit: int) -> list[ResidualVariableLinePlan]:
        if limit <= 0 or len(source_plans) <= limit:
            return source_plans
        if limit == 1:
            return [source_plans[0]]
        last = len(source_plans) - 1
        return [
            source_plans[round(index * last / (limit - 1))]
            for index in range(limit)
        ]

    source_template_cost_cache: dict[object, int] = {}
    source_template_count_cache: dict[object, int] = {}

    def cached_residual_template_proxy_cost(lines_to_score: list[str], cache_key: object | None = None) -> int:
        if cache_key is None:
            return residual_template_proxy_cost(lines_to_score)
        # The same line slice can be scored under different proxy-LZMA presets.
        # Keep those cache entries separate so a fast frontier never pollutes the
        # exact confirmation pass.
        preset_key = os.environ.get("PARE_PROXY_LZMA_PRESET", "")
        scoped_cache_key = (preset_key, cache_key)
        cached = source_template_cost_cache.get(scoped_cache_key)
        if cached is not None:
            return cached
        cost = residual_template_proxy_cost(lines_to_score)
        if len(source_template_cost_cache) < 2048:
            source_template_cost_cache[scoped_cache_key] = cost
        return cost

    def cached_residual_template_count(lines_to_score: list[str], cache_key: object | None = None) -> int:
        if cache_key is None:
            return len(set(lines_to_score))
        cached = source_template_count_cache.get(cache_key)
        if cached is not None:
            return cached
        count = len(set(lines_to_score))
        if len(source_template_count_cache) < 2048:
            source_template_count_cache[cache_key] = count
        return count

    def scaled_template_gain(
        source_plans: list[ResidualVariableLinePlan],
        group_key: object,
        tag: str,
        placeholder: str,
        source_cache_key: object | None = None,
        sample_limit_override: int | None = None,
    ) -> int:
        source_lines = [plan.line for plan in source_plans]
        sample_limit = (
            int(getattr(args, "residual_variable_sample_score_lines", 0))
            if sample_limit_override is None
            else sample_limit_override
        )
        if sample_limit <= 0 or len(source_plans) <= sample_limit:
            trial_lines, _trial_values = residual_variable_apply_prepared_groups_to_lines(
                source_plans,
                {group_key: (tag, placeholder)},
            )
            if args.residual_variable_template_collapse_prefilter:
                before_count = cached_residual_template_count(source_lines, source_cache_key)
                after_count = len(set(trial_lines))
                if after_count >= before_count:
                    return -1
            return cached_residual_template_proxy_cost(source_lines, source_cache_key) - residual_template_proxy_cost(trial_lines)
        sampled_plans = evenly_sample_plans(source_plans, sample_limit)
        sampled_lines = [plan.line for plan in sampled_plans]
        trial_lines, _trial_values = residual_variable_apply_prepared_groups_to_lines(
            sampled_plans,
            {group_key: (tag, placeholder)},
        )
        sample_key = ("sample", source_cache_key, len(sampled_plans)) if source_cache_key is not None else None
        if args.residual_variable_template_collapse_prefilter:
            before_count = cached_residual_template_count(sampled_lines, sample_key)
            after_count = len(set(trial_lines))
            if after_count >= before_count:
                return -1
        sample_gain = cached_residual_template_proxy_cost(sampled_lines, sample_key) - residual_template_proxy_cost(trial_lines)
        return int(sample_gain * len(source_plans) / max(1, len(sampled_plans)))

    placeholder_source_text = original_text + transformed_text
    sample_shortlist_keys: set[object] | None = None
    sample_shortlist_size = int(getattr(args, "residual_variable_sample_shortlist", 0))
    sample_shortlist_lines = int(getattr(args, "residual_variable_sample_score_lines", 0))
    use_sample_shortlist = (
        sample_shortlist_size > 0
        and sample_shortlist_lines > 0
        and not getattr(args, "residual_variable_feature_mdl", False)
        and not getattr(args, "residual_variable_admit_eligible", False)
        and len(eligible_groups) > sample_shortlist_size
    )
    if use_sample_shortlist:
        sampled_scores: list[tuple[int, int, object]] = []
        op_start = time.perf_counter()
        for sample_index, (_cheap_score, _raw_main, support, group_key, values) in enumerate(eligible_groups):
            tag = f"RS{sample_index}"
            placeholder = dataset_extract.choose_placeholder(placeholder_source_text, args.dataset, tag)
            try:
                if mode in {"global", "shape"} and not getattr(args, "residual_variable_local_impact_score", False):
                    main_gain = scaled_template_gain(
                        line_plans,
                        group_key,
                        tag,
                        placeholder,
                        sample_limit_override=sample_shortlist_lines,
                    )
                else:
                    affected_indexes = sorted(group_line_indexes.get(group_key, set()))
                    affected_plans = [line_plans[index] for index in affected_indexes]
                    main_gain = scaled_template_gain(
                        affected_plans,
                        group_key,
                        tag,
                        placeholder,
                        tuple(affected_indexes),
                        sample_limit_override=sample_shortlist_lines,
                    )
            except Exception:
                main_gain = -1
            try:
                side_cost = stream_best_proxy_cost(values, tag=tag)
            except Exception:
                side_cost = dataset_extract.compressed_parts_cost([dataset_extract.encode_string_stream_bytes(values)])
            sampled_scores.append((main_gain - side_cost, support, group_key))
        sampled_scores.sort(key=lambda item: (item[0], item[1]), reverse=True)
        sample_shortlist_keys = {
            group_key
            for main_gain, _support, group_key in sampled_scores[:sample_shortlist_size]
        }
        record_operation_timing(
            args,
            f"residual_variable.{mode}.sample_shortlist",
            time.perf_counter() - op_start,
            count=len(eligible_groups),
        )
    for _cheap_score, raw_main, _support, group_key, values in eligible_groups:
        if sample_shortlist_keys is not None and group_key not in sample_shortlist_keys:
            continue
        tag = allocate_unique_tag(f"RV{tag_index}", used_tags)
        tag_index += 1
        placeholder = dataset_extract.choose_placeholder(placeholder_source_text, args.dataset, tag)
        spec = dataset_extract.ExtractSpec(
            tag=tag,
            pattern="residual-variable:" + json.dumps(group_key, ensure_ascii=True, default=str),
            kind="auto",
            store_group=1,
            replacement="{placeholder}",
        )
        if getattr(args, "residual_variable_admit_eligible", False):
            # Fast path: eligibility already checked support, distinctness, and
            # raw main-stream byte reduction.  Skip the expensive per-candidate
            # replay/proxy score; the final full-block replay and SHA verifier
            # still enforce byte-for-byte correctness.
            candidates.append((_cheap_score, len(values), group_key, spec, placeholder))
            continue
        op_start = time.perf_counter()
        if args.residual_variable_feature_mdl:
            placeholder_bytes = len(placeholder.encode("latin-1")) * len(values)
            distinct_ratio = len(set(values)) / max(1, len(values))
            entropy_bonus = int(raw_main * min(0.5, distinct_ratio))
            main_gain = raw_main - placeholder_bytes + entropy_bonus
        elif mode in {"global", "shape"} and not getattr(args, "residual_variable_local_impact_score", False):
            sample_limit = int(getattr(args, "residual_variable_sample_score_lines", 0))
            if sample_limit > 0 and len(lines) > sample_limit and not use_sample_shortlist:
                main_gain = scaled_template_gain(line_plans, group_key, tag, placeholder)
            else:
                trial_text, _trial_values = residual_variable_apply_prepared_groups(
                    line_plans, {group_key: (tag, placeholder)}
                )
                main_gain = before_cost - id_mapping_proxy_cost(trial_text)
        else:
            # Local-impact scoring treats a residual stream as an edit program:
            # only lines containing that group can change their ID-template
            # representation, so the admission proxy can score the affected
            # slice instead of replaying the whole block for every candidate.
            affected_indexes = sorted(group_line_indexes.get(group_key, set()))
            affected_plans = [line_plans[index] for index in affected_indexes]
            main_gain = scaled_template_gain(
                affected_plans,
                group_key,
                tag,
                placeholder,
                tuple(affected_indexes),
                sample_limit_override=0 if use_sample_shortlist else None,
            )
        record_operation_timing(args, f"residual_variable.{mode}.score_main_gain", time.perf_counter() - op_start)
        if main_gain <= args.residual_variable_min_score:
            continue
        try:
            op_start = time.perf_counter()
            side_cost = stream_best_proxy_cost(values, tag=spec.tag)
            record_operation_timing(args, f"residual_variable.{mode}.score_side_codec", time.perf_counter() - op_start)
        except Exception:
            op_start = time.perf_counter()
            side_cost = dataset_extract.compressed_parts_cost([dataset_extract.encode_string_stream_bytes(values)])
            record_operation_timing(args, f"residual_variable.{mode}.score_side_string_fallback", time.perf_counter() - op_start)
        score = main_gain - side_cost
        if score <= args.residual_variable_min_score:
            continue
        candidates.append((score, len(values), group_key, spec, placeholder))

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    selected = candidates[: args.residual_variable_max_streams]
    if not selected:
        return transformed_text, values_by_tag, placeholders, accepted_specs, {
            "candidate_groups": len(candidates),
            "admitted_groups": 0,
            "values": 0,
        }
    capture_residual_schema_pass(args, mode, alpha_contexts, selected)

    op_start = time.perf_counter()
    selected_groups = {
        group_key: (spec.tag, placeholder)
        for _score, _count, group_key, spec, placeholder in selected
    }
    transformed_text, residual_values = residual_variable_apply_prepared_groups(
        line_plans, selected_groups
    )
    record_operation_timing(args, f"residual_variable.{mode}.final_replay", time.perf_counter() - op_start, count=len(lines))
    admitted = 0
    value_count = 0
    op_start = time.perf_counter()
    for _score, _count, _group_key, spec, placeholder in selected:
        values = residual_values.get(spec.tag, [])
        if not values:
            continue
        values_by_tag[spec.tag] = values
        placeholders[spec.tag] = placeholder
        accepted_specs.append(
            CandidateSpec(
                tag=spec.tag,
                pattern=spec.pattern,
                replacement=spec.replacement or "",
                program={},
                store_group=spec.store_group,
                kind=spec.kind,
                semantic_key="",
            )
        )
        admitted += 1
        value_count += len(values)
    record_operation_timing(args, f"residual_variable.{mode}.commit_streams", time.perf_counter() - op_start, count=len(selected))

    return transformed_text, values_by_tag, placeholders, accepted_specs, {
        "candidate_groups": len(candidates),
        "admitted_groups": admitted,
        "values": value_count,
    }


def apply_residual_xsignature_streams(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    return transformed_text, values_by_tag, placeholders, accepted_specs, {"enabled": 0}


def apply_residual_family_simple_planner(
    transformed_lines: list[str],
    line_families: list[int],
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    return "".join(transformed_lines), values_by_tag, placeholders, accepted_specs, {"enabled": 0}


def residual_variable_apply_groups(
    lines: list[str],
    alpha_contexts: set[tuple[str, str, str]],
    selected_groups: dict[object, tuple[str, str]],
    mode: str,
    protected_placeholders: set[str],
) -> tuple[str, dict[str, list[str]]]:
    line_plans = prepare_residual_variable_line_plans(lines, alpha_contexts, mode, protected_placeholders)
    return residual_variable_apply_prepared_groups(line_plans, selected_groups)


def apply_residual_stream_coalescing(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    """Merge fragmented residual string streams when MDL says one pool wins.

    LLM functions should keep meaningful fields separate.  Residual LT/RV
    streams are different: they are the verifier-safe leftovers after semantic
    extraction.  On heterogeneous logs those leftovers can be split into many
    tiny categorical streams, losing the cross-position repetition that MTF/rank
    codecs exploit.  This pass proposes one pooled residual categorical stream,
    replays values in exact placeholder order, and accepts it only if the
    estimated main+side codelength improves.
    """
    candidate_tags: list[str] = []
    for spec in accepted_specs:
        tag = spec.tag
        if tag not in values_by_tag or tag not in placeholders:
            continue
        if not (tag.startswith("LT") or tag.startswith("RV")):
            continue
        values = values_by_tag.get(tag, [])
        if len(values) < 2 or values_are_numeric_like(values):
            continue
        candidate_tags.append(tag)

    stats = {
        "candidate_tags": len(candidate_tags),
        "admitted": 0,
        "values": 0,
        "score": 0,
    }
    if len(candidate_tags) < 2:
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats

    placeholder_to_tag = {
        placeholders[tag]: tag
        for tag in candidate_tags
    }
    merged_pattern = re.compile(
        "|".join(
            re.escape(placeholder)
            for placeholder in sorted(placeholder_to_tag, key=len, reverse=True)
        )
    )
    used_tags = set(placeholders) | {spec.tag for spec in accepted_specs}
    coalesced_tag = allocate_unique_tag("RC", used_tags)
    coalesced_placeholder = dataset_extract.choose_placeholder(
        original_text + transformed_text,
        args.dataset,
        coalesced_tag,
    )

    indexes = {tag: 0 for tag in candidate_tags}
    merged_values: list[str] = []
    pieces: list[str] = []
    cursor = 0
    for match in merged_pattern.finditer(transformed_text):
        pieces.append(transformed_text[cursor:match.start()])
        tag = placeholder_to_tag[match.group(0)]
        index = indexes[tag]
        tag_values = values_by_tag[tag]
        if index >= len(tag_values):
            return transformed_text, values_by_tag, placeholders, accepted_specs, stats
        merged_values.append(tag_values[index])
        indexes[tag] = index + 1
        pieces.append(coalesced_placeholder)
        cursor = match.end()
    pieces.append(transformed_text[cursor:])
    if any(indexes[tag] != len(values_by_tag[tag]) for tag in candidate_tags):
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats
    if len(merged_values) < 2:
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats

    coalesced_text = "".join(pieces)
    try:
        before_side = sum(
            stream_best_proxy_cost(values_by_tag[tag], tag=tag)
            for tag in candidate_tags
        )
        after_side = stream_best_proxy_cost(merged_values)
    except Exception:
        before_side = sum(
            dataset_extract.compressed_parts_cost([dataset_extract.encode_string_stream_bytes(values_by_tag[tag])])
            for tag in candidate_tags
        )
        after_side = dataset_extract.compressed_parts_cost([dataset_extract.encode_string_stream_bytes(merged_values)])

    before_main = id_mapping_proxy_cost(transformed_text)
    after_main = id_mapping_proxy_cost(coalesced_text)
    score = (before_main + before_side) - (after_main + after_side)
    stats["score"] = int(score)
    if score <= args.residual_stream_coalesce_min_score:
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats

    remove_tags = set(candidate_tags)
    new_values_by_tag = {
        tag: values
        for tag, values in values_by_tag.items()
        if tag not in remove_tags
    }
    new_placeholders = {
        tag: placeholder
        for tag, placeholder in placeholders.items()
        if tag not in remove_tags
    }
    new_specs = [
        spec
        for spec in accepted_specs
        if spec.tag not in remove_tags
    ]
    new_values_by_tag[coalesced_tag] = merged_values
    new_placeholders[coalesced_tag] = coalesced_placeholder
    new_specs.append(
        CandidateSpec(
            tag=coalesced_tag,
            pattern="residual-coalesced:string",
            replacement="{placeholder}",
            program={},
            store_group=1,
            kind="auto",
            semantic_key="",
        )
    )
    return coalesced_text, new_values_by_tag, new_placeholders, new_specs, {
        "candidate_tags": len(candidate_tags),
        "admitted": 1,
        "values": len(merged_values),
        "score": int(score),
    }


def cached_direct_codec_kind_for_values(tag: str | None, values: list[str]) -> str | None:
    if not tag:
        return None
    decision_cache = getattr(dataset_extract, "CODEC_DECISION_CACHE", {})
    if not isinstance(decision_cache, dict):
        return None
    decision = decision_cache.get(tag)
    if not isinstance(decision, dict):
        return None
    kind = str(decision.get("kind", ""))
    direct_kinds = getattr(dataset_extract, "DIRECT_CODEC_KINDS", set())
    if kind not in direct_kinds or kind == "open_function":
        return None
    try:
        if not dataset_extract.cached_codec_kind_is_safe(kind, values):
            return None
    except Exception:
        return None
    return kind


def stream_best_proxy_kind_cost(values: list[str], tag: str | None = None) -> tuple[str, int]:
    if not values:
        return "string", 0
    cached_kind = cached_direct_codec_kind_for_values(tag, values)
    try:
        candidates = dataset_extract.stream_codec_candidates(
            values,
            forced_kinds={cached_kind} if cached_kind else None,
        )
        if candidates:
            return min(candidates, key=lambda item: item[1])
    except Exception:
        pass
    return (
        "string",
        dataset_extract.compressed_parts_cost([
            dataset_extract.encode_string_stream_bytes(values)
        ]),
    )


def stream_best_proxy_cost(values: list[str], tag: str | None = None) -> int:
    return stream_best_proxy_kind_cost(values, tag=tag)[1]


def apply_placeholder_slot_fission(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    """Split heterogeneous value streams by verified placeholder slots.

    Residual numeric rescue can intentionally pool many digit occurrences into
    one robust stream.  That is good for template collapse, but bad when the
    pooled values interleave unrelated series, e.g. a timestamp field, a stable
    app id, and a REPORT tuple.  This pass treats the partition itself as an MDL
    decision: for each large numeric stream, split occurrences by
    (transformed-line template, placeholder occurrence index), literalize
    constant slots, and accept only if main+side proxy cost improves.
    """
    stats = {
        "candidate_tags": 0,
        "accepted_tags": 0,
        "split_streams": 0,
        "literalized_slots": 0,
        "values_moved": 0,
        "score": 0,
        "cache_hit": 0,
        "cache_reject_reason": "",
    }
    if not getattr(args, "placeholder_slot_fission", False):
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats

    used_tags = set(placeholders) | {spec.tag for spec in accepted_specs}
    spec_by_tag = {spec.tag: spec for spec in accepted_specs}
    decision_records = getattr(args, "_placeholder_slot_fission_decisions", None)
    if not isinstance(decision_records, list):
        decision_records = []
        setattr(args, "_placeholder_slot_fission_decisions", decision_records)
    lines_cache_text: str | None = None
    lines_cache: list[str] = []

    def current_lines() -> list[str]:
        nonlocal lines_cache_text, lines_cache
        if lines_cache_text != transformed_text:
            lines_cache_text = transformed_text
            lines_cache = dataset_extract.split_physical_lines(transformed_text)
        return lines_cache

    # Work tag-by-tag so every accepted rewrite remains independently
    # reversible and downstream context codecs can see the final placeholder
    # order.
    placeholder_context_re = re.compile(r"<[^>]+>")
    digit_context_re = re.compile(r"\d+")
    occurrence_key_cache: dict[tuple[str, str, int], object] = {}

    def occurrence_key(line: str, start: int, end: int, occurrence: int) -> object:
        if args.placeholder_slot_fission_key == "local_context":
            left = line[max(0, start - args.placeholder_slot_fission_context_chars):start]
            right = line[end:end + args.placeholder_slot_fission_context_chars]
            cache_key = (left, right, occurrence)
            cached = occurrence_key_cache.get(cache_key)
            if cached is not None:
                return cached
            norm_left = placeholder_context_re.sub("<P>", left)
            norm_right = placeholder_context_re.sub("<P>", right)
            norm_left = digit_context_re.sub("D", norm_left)
            norm_right = digit_context_re.sub("D", norm_right)
            key = (norm_left[-args.placeholder_slot_fission_context_chars:], norm_right[:args.placeholder_slot_fission_context_chars], occurrence)
            if len(occurrence_key_cache) < 262_144:
                occurrence_key_cache[cache_key] = key
            return key
        return (line, occurrence)

    before_main_cost_cache_text: str | None = None
    before_main_cost_cache_value = 0

    def current_before_main_cost() -> int:
        nonlocal before_main_cost_cache_text, before_main_cost_cache_value
        if before_main_cost_cache_text != transformed_text:
            before_main_cost_cache_text = transformed_text
            before_main_cost_cache_value = id_mapping_proxy_cost(transformed_text)
        return before_main_cost_cache_value

    def try_replay_decision_plan(plan_entries: list[dict[str, object]]) -> bool:
        nonlocal transformed_text, values_by_tag, placeholders, accepted_specs
        nonlocal lines_cache_text, before_main_cost_cache_text
        if not plan_entries:
            return False
        trial_text = transformed_text
        trial_values = {tag: list(values) for tag, values in values_by_tag.items()}
        trial_placeholders = dict(placeholders)
        trial_specs = list(accepted_specs)
        trial_used_tags = set(used_tags)
        replay_stats = {
            "candidate_tags": 0,
            "accepted_tags": 0,
            "split_streams": 0,
            "literalized_slots": 0,
            "values_moved": 0,
            "score": 0,
            "cache_hit": 1,
        }
        try:
            for entry in plan_entries:
                parent_tag = str(entry.get("parent_tag", ""))
                if not parent_tag or parent_tag not in trial_values:
                    continue
                parent_values = trial_values.get(parent_tag, [])
                parent_placeholder = trial_placeholders.get(parent_tag, "")
                if not parent_values or not parent_placeholder:
                    continue
                context_width = int(entry.get("context_width", args.placeholder_slot_fission_context_chars))
                route_entries = list(entry.get("routes", []))
                route_by_key: dict[object, dict[str, object]] = {}
                for route in route_entries:
                    if not isinstance(route, dict):
                        continue
                    key_payload = route.get("key")
                    if not isinstance(key_payload, list) or len(key_payload) != 3:
                        continue
                    key = (str(key_payload[0]), str(key_payload[1]), int(key_payload[2]))
                    route_by_key[key] = route
                    if route.get("kind") == "child":
                        child_tag = str(route.get("tag", ""))
                        child_placeholder = str(route.get("placeholder", ""))
                        if not child_tag or not child_placeholder:
                            raise ValueError("bad child route")
                        if child_tag in trial_used_tags and child_tag not in trial_values:
                            raise ValueError("child tag conflict")
                if not route_by_key:
                    continue

                pieces: list[str] = []
                value_index = 0
                matched = 0
                literalized = 0
                child_values: dict[str, list[str]] = defaultdict(list)
                child_placeholders: dict[str, str] = {}
                remaining_values: list[str] = []
                for line in dataset_extract.split_physical_lines(trial_text):
                    cursor = 0
                    occurrence = 0
                    while True:
                        pos = line.find(parent_placeholder, cursor)
                        if pos < 0:
                            break
                        if value_index >= len(parent_values):
                            raise ValueError("too many placeholder occurrences")
                        value = parent_values[value_index]
                        key = occurrence_key(line, pos, pos + len(parent_placeholder), occurrence)
                        route = route_by_key.get(key)
                        pieces.append(line[cursor:pos])
                        if route is None:
                            pieces.append(parent_placeholder)
                            remaining_values.append(value)
                        elif route.get("kind") == "literal":
                            literal = str(route.get("literal", ""))
                            if value != literal:
                                raise ValueError("literal mismatch")
                            pieces.append(literal)
                            matched += 1
                            literalized += 1
                        elif route.get("kind") == "child":
                            child_tag = str(route.get("tag", ""))
                            child_placeholder = str(route.get("placeholder", ""))
                            if child_tag in trial_values:
                                raise ValueError("child tag already materialized")
                            child_placeholders[child_tag] = child_placeholder
                            child_values[child_tag].append(value)
                            pieces.append(child_placeholder)
                            matched += 1
                        else:
                            raise ValueError("bad route kind")
                        value_index += 1
                        occurrence += 1
                        cursor = pos + len(parent_placeholder)
                    pieces.append(line[cursor:])
                if value_index != len(parent_values):
                    raise ValueError("unused parent values")
                if matched <= 0:
                    continue

                trial_text = "".join(pieces)
                if remaining_values:
                    trial_values[parent_tag] = remaining_values
                else:
                    trial_values.pop(parent_tag, None)
                    trial_placeholders.pop(parent_tag, None)
                existing_tags = {spec.tag for spec in trial_specs}
                for child_tag, vals in child_values.items():
                    if not vals:
                        continue
                    trial_values[child_tag] = vals
                    trial_placeholders[child_tag] = child_placeholders[child_tag]
                    trial_used_tags.add(child_tag)
                    if child_tag not in existing_tags:
                        trial_specs.append(
                            CandidateSpec(
                                tag=child_tag,
                                pattern=f"slot-fission:{parent_tag}",
                                replacement="{placeholder}",
                                program={},
                                store_group=1,
                                kind="auto",
                                semantic_key="",
                            )
                        )
                        existing_tags.add(child_tag)
                if remaining_values and parent_tag not in existing_tags:
                    trial_specs.append(
                        CandidateSpec(
                            tag=parent_tag,
                            pattern=f"slot-fission-parent:{parent_tag}",
                            replacement="{placeholder}",
                            program={},
                            store_group=1,
                            kind="auto",
                            semantic_key="",
                        )
                    )
                replay_stats["candidate_tags"] += 1
                replay_stats["accepted_tags"] += 1
                replay_stats["split_streams"] += len(child_values)
                replay_stats["literalized_slots"] += literalized
                replay_stats["values_moved"] += matched
            if replay_stats["accepted_tags"] <= 0:
                stats["cache_reject_reason"] = "no_matching_entries"
                return False
        except Exception as exc:
            stats["cache_reject_reason"] = str(exc)[:160]
            return False

        transformed_text = trial_text
        values_by_tag = trial_values
        placeholders = trial_placeholders
        accepted_specs = trial_specs
        lines_cache_text = None
        before_main_cost_cache_text = None
        stats.update(replay_stats)
        record_operation_timing(args, "placeholder_slot_fission.decision_cache_replay", 0.0, count=replay_stats["accepted_tags"])
        return True

    decision_cache_plan = load_slot_fission_decision_plan(
        getattr(args, "placeholder_slot_fission_decision_cache", ""),
        args.dataset,
    )
    if try_replay_decision_plan(decision_cache_plan):
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats

    for parent_tag in list(values_by_tag):
        parent_values = values_by_tag.get(parent_tag, [])
        parent_placeholder = placeholders.get(parent_tag)
        parent_spec = spec_by_tag.get(parent_tag)
        if (
            not parent_values
            or not parent_placeholder
            or parent_spec is None
            or len(parent_values) < args.placeholder_slot_fission_min_values
            or not values_are_numeric_like(parent_values)
        ):
            continue
        if args.placeholder_slot_fission_target == "global_fallback" and not parent_spec.pattern.startswith("numeric-lattice:global-fallback"):
            continue

        stats["candidate_tags"] += 1
        value_index = 0
        slot_indexes: dict[object, list[int]] = {}
        line_slot_keys: list[list[object | None]] = []
        op_start = time.perf_counter()
        lines = current_lines()
        for line in lines:
            cursor = 0
            occurrence = 0
            keys_for_line: list[tuple[str, int] | None] = []
            while True:
                pos = line.find(parent_placeholder, cursor)
                if pos < 0:
                    break
                key = occurrence_key(line, pos, pos + len(parent_placeholder), occurrence)
                keys_for_line.append(key)
                slot_indexes.setdefault(key, []).append(value_index)
                value_index += 1
                occurrence += 1
                cursor = pos + len(parent_placeholder)
            line_slot_keys.append(keys_for_line)
        if value_index != len(parent_values):
            continue
        record_operation_timing(args, "placeholder_slot_fission.scan_parent_slots", time.perf_counter() - op_start, count=len(lines))
        if sum(1 for keys in line_slot_keys if len(keys) >= 2) < args.placeholder_slot_fission_min_multislot_lines:
            continue

        selected: dict[object, tuple[str, str | None, list[int]]] = {}
        split_values_by_tag: dict[str, list[str]] = {}
        split_placeholders_by_tag: dict[str, str] = {}
        split_cost_by_tag: dict[str, int] = {}
        literalized = 0
        split_streams = 0
        op_start = time.perf_counter()
        for key, indexes in sorted(slot_indexes.items(), key=lambda item: len(item[1]), reverse=True):
            if len(indexes) < args.placeholder_slot_fission_min_support:
                continue
            values = [parent_values[index] for index in indexes]
            distinct = set(values)
            if len(distinct) <= 1 and args.placeholder_slot_fission_literalize_constants:
                selected[key] = ("", values[0], indexes)
                literalized += 1
                continue
            if len(distinct) < args.placeholder_slot_fission_min_distinct:
                continue
            if split_streams >= args.placeholder_slot_fission_max_streams_per_tag:
                break
            child_tag = allocate_unique_tag(f"{parent_tag}S", used_tags)
            child_placeholder = dataset_extract.choose_placeholder(
                original_text + transformed_text,
                args.dataset,
                child_tag,
            )
            selected[key] = (child_tag, None, indexes)
            split_values_by_tag[child_tag] = values
            split_placeholders_by_tag[child_tag] = child_placeholder
            split_streams += 1
        if split_values_by_tag:
            cost_workers = int(os.environ.get("PARE_SLOT_FISSION_COST_WORKERS", "1"))
            child_items = list(split_values_by_tag.items())

            def score_child(item: tuple[str, list[str]]) -> tuple[str, int]:
                tag, child_values = item
                _child_kind, child_cost = stream_best_proxy_kind_cost(child_values, tag=tag)
                return tag, child_cost

            if cost_workers > 1 and len(child_items) >= 4:
                with feature_codec_prefilter_scope(bool(getattr(args, "feature_codec_prefilter_residual_costs", False))):
                    with ThreadPoolExecutor(max_workers=cost_workers) as executor:
                        child_costs = list(executor.map(score_child, child_items))
            else:
                with feature_codec_prefilter_scope(bool(getattr(args, "feature_codec_prefilter_residual_costs", False))):
                    child_costs = [score_child(item) for item in child_items]
            split_cost_by_tag.update(child_costs)
        record_operation_timing(args, "placeholder_slot_fission.score_slots_and_children", time.perf_counter() - op_start, count=len(slot_indexes))

        if not selected:
            continue

        # Constant-anchor fission gate.  Pure side-stream repartitioning can be
        # expensive to score and often loses once model cost is included.  A
        # literalized slot is a cheap structural witness that the split also
        # simplifies the main stream, so this gate avoids costly rejected
        # candidates while preserving the high-value HealthApp/Mac cases that
        # are driven by constant slots.
        if (
            getattr(args, "placeholder_slot_fission_require_literalized", False)
            and literalized <= 0
        ):
            continue

        selected_indexes = {
            index
            for _child_tag, _literal, indexes in selected.values()
            for index in indexes
        }
        remaining_values = [
            value
            for index, value in enumerate(parent_values)
            if index not in selected_indexes
        ]

        routed_mode = args.placeholder_slot_fission_mode == "routed"
        if routed_mode:
            candidate_text = transformed_text
            record_operation_timing(args, "placeholder_slot_fission.build_candidate_text", 0.0, count=len(lines))
        else:
            op_start = time.perf_counter()
            pieces: list[str] = []
            value_index = 0
            for line, keys_for_line in zip(lines, line_slot_keys):
                cursor = 0
                occurrence = 0
                while True:
                    pos = line.find(parent_placeholder, cursor)
                    if pos < 0:
                        break
                    pieces.append(line[cursor:pos])
                    key = keys_for_line[occurrence]
                    replacement: str
                    chosen = selected.get(key) if key is not None else None
                    if chosen is None:
                        replacement = parent_placeholder
                    else:
                        child_tag, literal, _indexes = chosen
                        replacement = literal if literal is not None else split_placeholders_by_tag[child_tag]
                    pieces.append(replacement)
                    value_index += 1
                    occurrence += 1
                    cursor = pos + len(parent_placeholder)
                pieces.append(line[cursor:])
            candidate_text = "".join(pieces)
            record_operation_timing(args, "placeholder_slot_fission.build_candidate_text", time.perf_counter() - op_start, count=len(lines))

        op_start = time.perf_counter()
        before_main_cost = current_before_main_cost()
        after_main_cost = before_main_cost if routed_mode else id_mapping_proxy_cost(candidate_text)
        with feature_codec_prefilter_scope(bool(getattr(args, "feature_codec_prefilter_residual_costs", False))):
            before_cost = before_main_cost + stream_best_proxy_cost(parent_values, tag=parent_tag)
            after_cost = after_main_cost + stream_best_proxy_cost(remaining_values, tag=parent_tag)
        after_cost += sum(split_cost_by_tag.values())
        after_cost += args.placeholder_slot_fission_model_cost * (
            len(split_values_by_tag) + literalized
        )
        score = before_cost - after_cost
        record_operation_timing(args, "placeholder_slot_fission.score_candidate", time.perf_counter() - op_start)
        if score <= args.placeholder_slot_fission_min_score:
            continue

        stats["accepted_tags"] += 1
        stats["split_streams"] += len(split_values_by_tag)
        stats["literalized_slots"] += literalized
        stats["values_moved"] += len(selected_indexes)
        stats["score"] += int(score)
        decision_routes: list[dict[str, object]] = []
        for key, (child_tag, literal, _indexes) in selected.items():
            if literal is not None:
                decision_routes.append({
                    "key": list(key),
                    "kind": "literal",
                    "literal": literal,
                })
            else:
                decision_routes.append({
                    "key": list(key),
                    "kind": "child",
                    "tag": child_tag,
                    "placeholder": split_placeholders_by_tag[child_tag],
                })
        if decision_routes:
            decision_records.append({
                "parent_tag": parent_tag,
                "parent_placeholder": parent_placeholder,
                "context_width": args.placeholder_slot_fission_context_chars,
                "routes": decision_routes,
            })
        if routed_mode:
            child_ids: dict[str, str] = {}
            next_child_id = 0
            route_entries: list[dict[str, object]] = []
            for key, (child_tag, literal, _indexes) in selected.items():
                if literal is not None:
                    route_entries.append({
                        "key": list(key),
                        "kind": "literal",
                        "literal": literal,
                    })
                else:
                    if child_tag not in child_ids:
                        child_ids[child_tag] = str(next_child_id)
                        next_child_id += 1
                    route_entries.append({
                        "key": list(key),
                        "kind": "child",
                        "child": child_ids[child_tag],
                    })
            routed_spec = CandidateSpec(
                tag=parent_tag,
                pattern=f"routed-slot-fission:{parent_tag}",
                replacement="{placeholder}",
                program={
                    "mode": "local_context",
                    "context_width": args.placeholder_slot_fission_context_chars,
                    "routes": route_entries,
                },
                store_group=1,
                kind="routed_split",
                semantic_key="",
            )
            for spec_index, spec in enumerate(accepted_specs):
                if spec.tag == parent_tag:
                    accepted_specs[spec_index] = routed_spec
                    break
            else:
                accepted_specs.append(routed_spec)
            # The main text and parent stream remain unchanged.  The archive
            # writer will route parent values into child streams without
            # perturbing the line dictionary.
            continue

        transformed_text = candidate_text
        before_main_cost_cache_text = None
        lines_cache_text = None
        if remaining_values:
            values_by_tag[parent_tag] = remaining_values
        else:
            values_by_tag.pop(parent_tag, None)
            placeholders.pop(parent_tag, None)

        for child_tag, child_values in split_values_by_tag.items():
            values_by_tag[child_tag] = child_values
            placeholders[child_tag] = split_placeholders_by_tag[child_tag]
            accepted_specs.append(
                CandidateSpec(
                    tag=child_tag,
                    pattern=f"slot-fission:{parent_tag}",
                    replacement="{placeholder}",
                    program={},
                    store_group=1,
                    kind="auto",
                    semantic_key="",
                )
            )
        # Keep the parent spec if any parent values remain.  If not, it will be
        # ignored by serialization because its value stream is empty.
        if remaining_values and parent_tag not in spec_by_tag:
            accepted_specs.append(
                CandidateSpec(
                    tag=parent_tag,
                    pattern=f"slot-fission-parent:{parent_tag}",
                    replacement="{placeholder}",
                    program={},
                    store_group=1,
                    kind="auto",
                    semantic_key="",
                )
            )

    return transformed_text, values_by_tag, placeholders, accepted_specs, stats


def global_value_rescue_candidates() -> list[CandidateSpec]:
    return []


def select_global_value_specs_for_text(
    text: str,
    original_text: str,
    used_tags: set[str],
    args: argparse.Namespace,
    exclude_classes: set[str] | None = None,
    candidate_specs: list[CandidateSpec] | None = None,
    trust_cached_decision: bool = False,
) -> tuple[list[CandidateSpec], dict[str, int]]:
    return [], {"enabled": 0}


def apply_global_value_rescue(
    transformed_lines: list[str],
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
    exclude_classes: set[str] | None = None,
    candidate_specs: list[CandidateSpec] | None = None,
    trust_cached_decision: bool = False,
) -> tuple[list[str], dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    return transformed_lines, values_by_tag, placeholders, accepted_specs, {"enabled": 0}


def apply_residual_numeric_lattice(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    placeholders: dict[str, str],
    accepted_specs: list[CandidateSpec],
    original_text: str,
    args: argparse.Namespace,
) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
    """Apply a dataset-blind numeric lattice to residual digits.

    This pass is Denum-like but not dataset-profiled: it observes numeric
    occurrences left after LLM/global extraction, groups them by value kind and
    local numeric template, and admits the transformation only if an MDL proxy
    says the reduced main stream pays for the new side streams.
    """
    used_tags = set(placeholders) | {spec.tag for spec in accepted_specs}
    decision_cache = getattr(args, "_numeric_lattice_decision_cache", None)
    if decision_cache is None:
        decision_cache = load_numeric_lattice_decision_plan(
            getattr(args, "numeric_lattice_decision_cache", ""),
            args.dataset,
        )
        setattr(args, "_numeric_lattice_decision_cache", decision_cache)
        setattr(args, "_numeric_lattice_decision_index", 0)
    decision_index = int(getattr(args, "_numeric_lattice_decision_index", 0) or 0)
    forced_decision: dict[str, object] | None = None
    if isinstance(decision_cache, list) and decision_index < len(decision_cache):
        candidate = decision_cache[decision_index]
        if isinstance(candidate, dict):
            forced_decision = candidate
        setattr(args, "_numeric_lattice_decision_index", decision_index + 1)
    forced_groups = None
    forced_placeholders = None
    forced_route_tags = None
    if forced_decision:
        groups_payload = forced_decision.get("groups", [])
        specs_payload = forced_decision.get("specs", [])
        route_payload = forced_decision.get("route_tags", [])
        if isinstance(groups_payload, list):
            forced_groups = [item for item in groups_payload if isinstance(item, dict)]
        if isinstance(specs_payload, list):
            forced_placeholders = {
                str(item.get("tag")): str(item.get("placeholder"))
                for item in specs_payload
                if isinstance(item, dict) and item.get("tag") and item.get("placeholder")
            }
        if isinstance(route_payload, list):
            forced_route_tags = [str(item) for item in route_payload]
    op_start = time.perf_counter()
    proxy_preset, proxy_policy_stats = choose_numeric_lattice_proxy_preset(transformed_text)
    with feature_codec_prefilter_scope(bool(getattr(args, "feature_codec_prefilter_residual_costs", False))):
        with proxy_lzma_preset_scope(proxy_preset):
            lattice_text, lattice_values, lattice_metadata = dataset_extract.transform_text_numeric_lattice(
                transformed_text,
                dataset=args.dataset,
                used_tags=used_tags,
                fallback_mode=args.residual_numeric_lattice_fallback,
                frontier_topk=args.numeric_lattice_frontier_topk,
                fast_select=args.numeric_lattice_fast_select,
                onepass_exact_select=args.numeric_lattice_onepass_exact_select,
                skip_group_codec_cost=(
                    args.numeric_lattice_skip_group_codec_cost
                    or args.residual_numeric_lattice_feature_admit
                ),
                forced_groups=forced_groups,
                forced_placeholders=forced_placeholders,
                forced_route_tags=forced_route_tags,
            )
    record_operation_timing(args, "residual_numeric_lattice.transform_text", time.perf_counter() - op_start)
    stats = {
        "occurrences": int(lattice_metadata.get("numeric_lattice", {}).get("occurrences", 0)),
        "groups": len(lattice_metadata.get("numeric_lattice", {}).get("groups", [])),
        "admitted": 0,
        "values": 0,
        "score": 0,
        "side_cost": 0,
        "admission_margin": 0,
        "occurrences_per_kib": 0,
        "before_templates": 0,
        "after_templates": 0,
        "fast_select": int(args.numeric_lattice_fast_select),
        "fast_select_fallback": 0,
        "fast_select_reject_reason": "",
        "proxy_preset": int(proxy_preset) if proxy_preset is not None and proxy_preset.isdigit() else -1,
        "proxy_policy_before_templates": int(proxy_policy_stats.get("before_templates", 0)),
        "proxy_policy_line_count": int(proxy_policy_stats.get("line_count", 0)),
        "cache_hit": int(lattice_metadata.get("numeric_lattice", {}).get("forced_replay", 0)),
        "route_replay": int(lattice_metadata.get("numeric_lattice", {}).get("route_replay", 0)),
    }

    numeric_min_distinct = int(os.environ.get("PARE_RESIDUAL_NUMERIC_MIN_DISTINCT", "0") or 0)
    if numeric_min_distinct > 0 and lattice_values:
        spec_meta_by_tag = {
            str(spec_meta.get("tag")): spec_meta
            for spec_meta in lattice_metadata.get("specs", [])
            if isinstance(spec_meta, dict) and spec_meta.get("tag")
        }
        keep_tags = {
            tag
            for tag, values in lattice_values.items()
            if len(set(values)) >= numeric_min_distinct
        }
        reject_tags = set(lattice_values) - keep_tags
        stats["numeric_min_distinct"] = numeric_min_distinct
        stats["numeric_distinct_rejected_groups"] = len(reject_tags)
        stats["numeric_distinct_rejected_values"] = sum(len(lattice_values.get(tag, [])) for tag in reject_tags)
        if reject_tags:
            tag_by_placeholder = {
                str(spec_meta.get("placeholder")): tag
                for tag, spec_meta in spec_meta_by_tag.items()
                if tag in reject_tags and spec_meta.get("placeholder")
            }
            skipped_indices = {tag: 0 for tag in reject_tags}
            if tag_by_placeholder:
                pattern = re.compile("|".join(re.escape(value) for value in sorted(tag_by_placeholder, key=len, reverse=True)))

                def replace_numeric_reject(match: re.Match[str]) -> str:
                    placeholder = match.group(0)
                    tag = tag_by_placeholder[placeholder]
                    index = skipped_indices[tag]
                    values = lattice_values.get(tag, [])
                    if index >= len(values):
                        raise ValueError(f"numeric distinct gate exhausted stream {tag}")
                    skipped_indices[tag] = index + 1
                    return str(values[index])

                lattice_text = pattern.sub(replace_numeric_reject, lattice_text)
                for tag, index in skipped_indices.items():
                    if index != len(lattice_values.get(tag, [])):
                        raise ValueError(f"numeric distinct gate unused values for {tag}")
            lattice_values = {
                tag: values
                for tag, values in lattice_values.items()
                if tag in keep_tags
            }
            lattice_metadata["specs"] = [
                spec_meta
                for spec_meta in lattice_metadata.get("specs", [])
                if str(spec_meta.get("tag")) in keep_tags
            ]
            numeric_payload = lattice_metadata.get("numeric_lattice", {})
            if isinstance(numeric_payload, dict):
                numeric_payload["groups"] = [
                    group
                    for group in numeric_payload.get("groups", [])
                    if str(group.get("tag")) in keep_tags
                ]
                numeric_payload["route_tags"] = [
                    tag
                    for tag in numeric_payload.get("route_tags", [])
                    if str(tag) in keep_tags
                ]
            if not lattice_values:
                stats["fast_select_reject_reason"] = "numeric_min_distinct"
                return transformed_text, values_by_tag, placeholders, accepted_specs, stats

    def reject_or_exact_fallback(reason: str) -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec], dict[str, int]]:
        stats["fast_select_reject_reason"] = reason
        if (
            args.numeric_lattice_no_exact_fallback
            or not args.numeric_lattice_fast_select
            or getattr(args, "_numeric_lattice_exact_fallback_active", False)
        ):
            return transformed_text, values_by_tag, placeholders, accepted_specs, stats
        exact_args = argparse.Namespace(**vars(args))
        exact_args.numeric_lattice_fast_select = False
        exact_args._numeric_lattice_exact_fallback_active = True
        op_start = time.perf_counter()
        (
            exact_text,
            exact_values_by_tag,
            exact_placeholders,
            exact_specs,
            exact_stats,
        ) = apply_residual_numeric_lattice(
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            original_text,
            exact_args,
        )
        record_operation_timing(args, "residual_numeric_lattice.exact_fallback_total", time.perf_counter() - op_start)
        exact_stats["fast_select_fallback"] = 1
        exact_stats["fast_select_reject_reason"] = reason
        return exact_text, exact_values_by_tag, exact_placeholders, exact_specs, exact_stats

    if not lattice_values:
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats

    if stats["cache_hit"]:
        op_start = time.perf_counter()
        for tag, values in lattice_values.items():
            if values:
                values_by_tag[tag] = values
                stats["values"] += len(values)
        for spec_meta in lattice_metadata.get("specs", []):
            tag = str(spec_meta["tag"])
            if tag not in lattice_values or not lattice_values[tag]:
                continue
            placeholders[tag] = str(spec_meta["placeholder"])
            accepted_specs.append(
                CandidateSpec(
                    tag=tag,
                    pattern=str(spec_meta["pattern"]),
                    replacement=str(spec_meta.get("replacement", "")),
                    program={},
                    store_group=int(spec_meta.get("store_group", 1)),
                    kind=str(spec_meta.get("kind", "auto")),
                    semantic_key="",
                )
            )
        record_operation_timing(args, "residual_numeric_lattice.decision_cache_commit", time.perf_counter() - op_start, count=len(lattice_values))
        decision_records = getattr(args, "_numeric_lattice_decisions", None)
        if not isinstance(decision_records, list):
            decision_records = []
            setattr(args, "_numeric_lattice_decisions", decision_records)
        decision_records.append({
            "groups": lattice_metadata.get("numeric_lattice", {}).get("groups", []),
            "specs": lattice_metadata.get("specs", []),
            "route_tags": lattice_metadata.get("numeric_lattice", {}).get("route_tags", []),
        })
        stats["admitted"] = 1
        return lattice_text, values_by_tag, placeholders, accepted_specs, stats

    raw_kib = max(1.0, len(transformed_text.encode("latin-1")) / 1024.0)
    density = stats["occurrences"] / raw_kib
    stats["occurrences_per_kib"] = int(density)
    op_start = time.perf_counter()
    before_template_count = len(build_id_mapping(transformed_text)[0])
    after_template_count = len(build_id_mapping(lattice_text)[0])
    record_operation_timing(args, "residual_numeric_lattice.template_counts", time.perf_counter() - op_start)
    stats["before_templates"] = before_template_count
    stats["after_templates"] = after_template_count
    strong_template_collapse = has_strong_numeric_template_collapse(
        before_template_count,
        after_template_count,
        stats["occurrences"],
    )
    stats["strong_template_collapse"] = int(strong_template_collapse)
    if getattr(args, "force_residual_numeric_lattice_admit", False):
        op_start = time.perf_counter()
        for tag, values in lattice_values.items():
            if values:
                values_by_tag[tag] = values
                stats["values"] += len(values)
        for spec_meta in lattice_metadata.get("specs", []):
            tag = str(spec_meta["tag"])
            if tag not in lattice_values or not lattice_values[tag]:
                continue
            placeholders[tag] = str(spec_meta["placeholder"])
            accepted_specs.append(
                CandidateSpec(
                    tag=tag,
                    pattern=str(spec_meta["pattern"]),
                    replacement=str(spec_meta.get("replacement", "")),
                    program={},
                    store_group=int(spec_meta.get("store_group", 1)),
                    kind=str(spec_meta.get("kind", "auto")),
                    semantic_key="",
                )
            )
        record_operation_timing(args, "residual_numeric_lattice.force_commit", time.perf_counter() - op_start, count=len(lattice_values))
        stats["score"] = int((before_template_count - after_template_count) * max(1, stats["occurrences_per_kib"]))
        stats["side_cost"] = -1
        stats["admitted"] = 1
        return lattice_text, values_by_tag, placeholders, accepted_specs, stats
    if stats["occurrences"] < args.residual_numeric_lattice_min_occurrences and not strong_template_collapse:
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats
    if (
        density < args.residual_numeric_lattice_min_occurrences_per_kib
        and before_template_count < args.residual_numeric_lattice_min_templates
        and not strong_template_collapse
    ):
        return transformed_text, values_by_tag, placeholders, accepted_specs, stats
    if after_template_count > before_template_count:
        return reject_or_exact_fallback("after_template_increase")

    if args.residual_numeric_lattice_feature_admit:
        template_collapse = before_template_count - after_template_count
        feature_admit = (
            strong_template_collapse
            or density >= args.residual_numeric_lattice_min_occurrences_per_kib
            or stats["occurrences"] >= args.residual_numeric_lattice_min_occurrences
        )
        stats["score"] = int(template_collapse * max(1, stats["occurrences_per_kib"]))
        stats["side_cost"] = -1
        stats["admission_margin"] = 0
        stats["feature_admit"] = int(feature_admit)
        if not feature_admit:
            return reject_or_exact_fallback("feature_admit_gate")
        op_start = time.perf_counter()
        for tag, values in lattice_values.items():
            if values:
                values_by_tag[tag] = values
                stats["values"] += len(values)
        for spec_meta in lattice_metadata.get("specs", []):
            tag = str(spec_meta["tag"])
            if tag not in lattice_values or not lattice_values[tag]:
                continue
            placeholders[tag] = str(spec_meta["placeholder"])
            accepted_specs.append(
                CandidateSpec(
                    tag=tag,
                    pattern=str(spec_meta["pattern"]),
                    replacement=str(spec_meta.get("replacement", "")),
                    program={},
                    store_group=int(spec_meta.get("store_group", 1)),
                    kind=str(spec_meta.get("kind", "auto")),
                    semantic_key="",
                )
            )
        record_operation_timing(args, "residual_numeric_lattice.feature_commit", time.perf_counter() - op_start, count=len(lattice_values))
        decision_records = getattr(args, "_numeric_lattice_decisions", None)
        if not isinstance(decision_records, list):
            decision_records = []
            setattr(args, "_numeric_lattice_decisions", decision_records)
        decision_records.append({
            "groups": lattice_metadata.get("numeric_lattice", {}).get("groups", []),
            "specs": lattice_metadata.get("specs", []),
            "route_tags": lattice_metadata.get("numeric_lattice", {}).get("route_tags", []),
            "feature_admit": 1,
        })
        stats["admitted"] = 1
        return lattice_text, values_by_tag, placeholders, accepted_specs, stats

    op_start = time.perf_counter()
    with proxy_lzma_preset_scope(proxy_preset):
        before_main = id_mapping_proxy_cost(transformed_text)
        after_main = id_mapping_proxy_cost(lattice_text)
    record_operation_timing(args, "residual_numeric_lattice.main_proxy_cost", time.perf_counter() - op_start)
    if getattr(args, "force_residual_numeric_lattice_admit", False):
        op_start = time.perf_counter()
        for tag, values in lattice_values.items():
            if values:
                values_by_tag[tag] = values
                stats["values"] += len(values)
        for spec_meta in lattice_metadata.get("specs", []):
            tag = str(spec_meta["tag"])
            if tag not in lattice_values or not lattice_values[tag]:
                continue
            placeholders[tag] = str(spec_meta["placeholder"])
            accepted_specs.append(
                CandidateSpec(
                    tag=tag,
                    pattern=str(spec_meta["pattern"]),
                    replacement=str(spec_meta.get("replacement", "")),
                    program={},
                    store_group=int(spec_meta.get("store_group", 1)),
                    kind=str(spec_meta.get("kind", "auto")),
                    semantic_key="",
                )
            )
        record_operation_timing(args, "residual_numeric_lattice.force_commit", time.perf_counter() - op_start, count=len(lattice_values))
        stats["score"] = int(before_main - after_main)
        stats["side_cost"] = -1
        stats["admitted"] = 1
        return lattice_text, values_by_tag, placeholders, accepted_specs, stats
    side_cost_from_lattice = False
    if getattr(args, "numeric_lattice_reuse_selection_cost", False):
        group_cost_by_tag = {
            str(group.get("tag")): int(group.get("codec_cost", -1))
            for group in lattice_metadata.get("numeric_lattice", {}).get("groups", [])
            if int(group.get("codec_cost", -1)) >= 0
        }
        if lattice_values and all(tag in group_cost_by_tag for tag in lattice_values):
            side_cost = sum(group_cost_by_tag[tag] for tag in lattice_values)
            side_cost_from_lattice = True
            record_operation_timing(
                args,
                "residual_numeric_lattice.side_codec_cost_reused",
                0.0,
                count=len(lattice_values),
            )
        else:
            side_cost = 0
    if not side_cost_from_lattice:
        try:
            op_start = time.perf_counter()
            with feature_codec_prefilter_scope(bool(getattr(args, "feature_codec_prefilter_residual_costs", False))):
                with proxy_lzma_preset_scope(proxy_preset):
                    side_cost = sum(
                        stream_best_proxy_cost(values, tag=tag)
                        for tag, values in lattice_values.items()
                    )
            record_operation_timing(args, "residual_numeric_lattice.side_codec_cost", time.perf_counter() - op_start, count=len(lattice_values))
        except Exception:
            op_start = time.perf_counter()
            side_cost = sum(
                dataset_extract.compressed_parts_cost([dataset_extract.encode_string_stream_bytes(values)])
                for values in lattice_values.values()
            )
            record_operation_timing(args, "residual_numeric_lattice.side_string_fallback_cost", time.perf_counter() - op_start, count=len(lattice_values))
    stats["side_cost"] = int(side_cost)
    score = before_main - after_main - side_cost
    stats["score"] = int(score)
    if (
        after_template_count < args.residual_numeric_lattice_tiny_after_templates
        and score < args.residual_numeric_lattice_tiny_after_min_score
    ):
        return reject_or_exact_fallback("tiny_after_margin")
    admission_margin = max(
        args.residual_numeric_lattice_min_score,
        int(args.residual_numeric_lattice_kappa * side_cost),
    )
    stats["admission_margin"] = int(admission_margin)
    if score <= admission_margin:
        return reject_or_exact_fallback("score_margin")

    op_start = time.perf_counter()
    for tag, values in lattice_values.items():
        if values:
            values_by_tag[tag] = values
            stats["values"] += len(values)
    for spec_meta in lattice_metadata.get("specs", []):
        tag = str(spec_meta["tag"])
        if tag not in lattice_values or not lattice_values[tag]:
            continue
        placeholders[tag] = str(spec_meta["placeholder"])
        accepted_specs.append(
            CandidateSpec(
                tag=tag,
                pattern=str(spec_meta["pattern"]),
                replacement=str(spec_meta.get("replacement", "")),
                program={},
                store_group=int(spec_meta.get("store_group", 1)),
                kind=str(spec_meta.get("kind", "auto")),
                semantic_key="",
            )
        )
    record_operation_timing(args, "residual_numeric_lattice.commit_admitted_streams", time.perf_counter() - op_start, count=len(lattice_values))
    decision_records = getattr(args, "_numeric_lattice_decisions", None)
    if not isinstance(decision_records, list):
        decision_records = []
        setattr(args, "_numeric_lattice_decisions", decision_records)
    decision_records.append({
        "groups": lattice_metadata.get("numeric_lattice", {}).get("groups", []),
        "specs": lattice_metadata.get("specs", []),
        "route_tags": lattice_metadata.get("numeric_lattice", {}).get("route_tags", []),
    })
    stats["admitted"] = 1
    return lattice_text, values_by_tag, placeholders, accepted_specs, stats


def residual_state_proxy_cost(text: str, values_by_tag: dict[str, list[str]]) -> int:
    """Estimate the final residual cost used by the MDL residual planner."""
    stream_cost = 0
    for values in values_by_tag.values():
        if not values:
            continue
        try:
            stream_cost += min(cost for _kind, cost in dataset_extract.stream_codec_candidates(values))
        except Exception:
            stream_cost += dataset_extract.compressed_parts_cost([
                dataset_extract.encode_string_stream_bytes(values)
            ])
    return id_mapping_proxy_cost(text) + stream_cost


def has_strong_numeric_template_collapse(
    before_templates: int,
    after_templates: int,
    occurrences: int,
) -> bool:
    """Return whether residual digits are fragmenting the main template stream.

    Occurrence-count gates miss blocks where each residual number appears
    moderately often but creates a large number of one-off templates.  This
    predicate is intentionally dataset-blind: it only observes how much a
    reversible digit mask would shrink the transformed-line template space.
    The exact numeric lattice still has to pass its normal MDL admission and
    byte-for-byte archive verification before it is committed.
    """
    if occurrences <= 0 or before_templates <= 0:
        return False
    if after_templates >= before_templates:
        return False
    if before_templates >= 1000 and after_templates <= max(512, before_templates // 20):
        return True
    if before_templates - after_templates >= 2000 and after_templates * 4 <= before_templates:
        return True
    return False


def cheap_numeric_dominance_stats(text: str) -> dict[str, int]:
    """Cheap pre-pass that predicts whether numeric lattice should run first.

    Full numeric-lattice probing is expensive because it builds candidate
    streams and exact proxy costs.  For order selection we only need to know
    whether residual digits are dense and whether masking digit runs collapses
    the line template space.  The real transform is still run before admission
    when this predictor says numeric-first.
    """
    lines = dataset_extract.split_physical_lines(text)
    before_templates = len(set(lines))
    occurrences = 0
    masked_templates: set[str] = set()
    digit_run_re = re.compile(r"\d+")
    for line in lines:
        masked, count = digit_run_re.subn("<N>", line)
        occurrences += count
        masked_templates.add(masked)
    after_templates = len(masked_templates)
    raw_kib = max(1.0, len(text.encode("latin-1")) / 1024.0)
    density = int(occurrences / raw_kib)
    strong_template_collapse = has_strong_numeric_template_collapse(
        before_templates,
        after_templates,
        occurrences,
    )
    dominates = occurrences > 0 and (density >= 80 or strong_template_collapse)
    return {
        "occurrences": occurrences,
        "groups": 0,
        "admitted": int(dominates),
        "values": 0,
        "score": 0,
        "side_cost": 0,
        "admission_margin": 0,
        "occurrences_per_kib": density,
        "before_templates": before_templates,
        "after_templates": after_templates,
        "strong_template_collapse": int(strong_template_collapse),
    }


def should_run_post_variable_numeric_lattice(stats: dict[str, int]) -> bool:
    """Conservative post-variable gate for exact numeric lattice.

    After residual-variable extraction, many datasets still contain dense
    digits.  Running the exact lattice on every block is expensive, but skipping
    solely by density loses Mac-like cases where numeric masking collapses many
    residual templates.  This gate therefore runs the verifier whenever digits
    are dense, or when the digit-mask view shows a strong template collapse.
    """
    occurrences = int(stats.get("occurrences", 0))
    density = int(stats.get("occurrences_per_kib", 0))
    before_templates = int(stats.get("before_templates", 0))
    after_templates = int(stats.get("after_templates", before_templates))
    if occurrences <= 0:
        return False
    if density >= 80:
        return True
    if before_templates >= 1000 and after_templates * 4 <= before_templates:
        return True
    if after_templates <= 512 and before_templates >= 2 * max(1, after_templates):
        return True
    return False


def build_id_mapping(text: str) -> tuple[list[str], list[int]]:
    templates: list[str] = []
    id_by_line: dict[str, int] = {}
    ids: list[int] = []
    for line in dataset_extract.split_physical_lines(text):
        template_id = id_by_line.get(line)
        if template_id is None:
            template_id = len(templates)
            id_by_line[line] = template_id
            templates.append(line)
        ids.append(template_id)
    return templates, ids


def write_id_mapping(output_root: Path, templates: list[str], ids: list[int]) -> dict[str, Any]:
    mapping_dir = output_root / "id_mapping"
    mapping_dir.mkdir(parents=True, exist_ok=True)
    base.write_string_stream(mapping_dir / "templates.bin", templates)
    base.write_varint_stream(mapping_dir / "ids.bin", ids)
    return {
        "template_file": "id_mapping/templates.bin",
        "id_file": "id_mapping/ids.bin",
        "template_count": len(templates),
        "line_count": len(ids),
    }


def read_id_mapping(root: Path, metadata: dict[str, Any]) -> str:
    info = metadata.get("id_mapping", {})
    templates = base.read_string_stream(root / str(info.get("template_file", "id_mapping/templates.bin")))
    ids = base.read_varint_stream(root / str(info.get("id_file", "id_mapping/ids.bin")))
    return "".join(templates[index] for index in ids)


def write_rank_core(output_root: Path, text: str, numeric_radius: int) -> dict[str, Any]:
    rank_input = output_root / "rank_input.tmp"
    base.write_lossless_text(rank_input, text)
    rank_dir = output_root / "rank_core"
    encoder = base.RankModelEncoder(numeric_radius=numeric_radius)
    stats = encoder.compress_file(rank_input, rank_dir, archive_path=None)
    rank_input.unlink()
    return {
        "kind": "rank",
        "directory": "rank_core",
        "segments": stats.segments,
        "numeric_radius": numeric_radius,
    }


def read_rank_core(root: Path, metadata: dict[str, Any]) -> str:
    info = metadata["main_core"]
    return base.RankModelDecoder(root / str(info["directory"])).decode_text()


COMPACT_ARCHIVE_MAGIC = b"PAREC2\n"
COMPACT_ARCHIVE_GROUP_ORDER = {
    "ts": 0,
    "hp": 1,
    "ids": 2,
    "tmpl": 3,
    "str": 4,
    "num": 5,
    "rank": 6,
    "other": 7,
    "meta": 8,
}


def encode_archive_varint(value: int) -> bytes:
    out = bytearray()
    cur = int(value)
    while True:
        if cur < 0x80:
            out.append(cur)
            return bytes(out)
        out.append((cur & 0x7F) | 0x80)
        cur >>= 7


def decode_archive_varint(data: bytes, offset: int) -> tuple[int, int]:
    shift = 0
    cur = 0
    pos = offset
    while True:
        if pos >= len(data):
            raise ValueError("Truncated compact archive varint")
        byte = data[pos]
        pos += 1
        cur |= (byte & 0x7F) << shift
        if byte < 0x80:
            return cur, pos
        shift += 7


def compact_archive_group(path: str) -> str:
    if path == "metadata.json":
        return "meta"
    if path.startswith("id_mapping/templates"):
        return "tmpl"
    if path.startswith("id_mapping/ids"):
        return "ids"
    if "TS." in path:
        return "ts"
    if "HP." in path:
        return "hp"
    if path.endswith(".rank.bin") or path.endswith(".cdelta.bin"):
        return "rank"
    if path.endswith(".literal.bin") or path.endswith(".strings.bin"):
        return "str"
    if "delta" in path or "abs" in path:
        return "num"
    return "other"


def compact_archive_sort_key(path: str) -> tuple[int, str]:
    group = compact_archive_group(path)
    return (COMPACT_ARCHIVE_GROUP_ORDER.get(group, COMPACT_ARCHIVE_GROUP_ORDER["other"]), path)


def _stream_file_or_default(stream: dict[str, Any], key: str, default_suffix: str) -> str:
    tag = str(stream["tag"])
    return str(stream.get(key, f"{tag}.{default_suffix}"))


def compact_stream_files(stream: dict[str, Any]) -> list[str]:
    tag = str(stream["tag"])
    kind = str(stream["kind"])
    if kind == "string":
        return [_stream_file_or_default(stream, "file", "strings.bin")]
    if kind == "string_mtf_rank":
        return [
            _stream_file_or_default(stream, "rank_file", "rank.bin"),
            _stream_file_or_default(stream, "literal_file", "literal.bin"),
        ]
    if kind == "string_mtf_cdelta":
        return [
            _stream_file_or_default(stream, "rank_file", "cdelta.bin"),
            _stream_file_or_default(stream, "literal_file", "literal.bin"),
        ]
    if kind == "byte_phrase_delta":
        return [str(stream["file"]), str(stream["flag_file"]), str(stream["exception_file"])]
    if kind in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width"}:
        return [str(stream["file"]), str(stream["width_file"])]
    if kind == "size_from_prev_bytes":
        return [str(stream["flag_file"]), str(stream["literal_file"])]
    if kind == "relation_pair_delta":
        return [
            str(stream["left_file"]),
            str(stream["right_file"]),
            str(stream["left_width_file"]),
            str(stream["right_width_file"]),
        ]
    if kind == "hex_int_delta_width":
        return [str(stream["file"]), str(stream["width_file"]), str(stream["case_file"])]
    if kind == "hex_pair_delta":
        return [str(stream["left_file"]), str(stream["right_file"]), str(stream["width_file"]), str(stream["case_file"])]
    if kind == "ipv4_width":
        return [_stream_file_or_default(stream, "file", "ipv4w.bin")]
    if kind == "ipv4_plain":
        return [_stream_file_or_default(stream, "file", "ipv4.bin")]
    if kind == "compound_endpoint_mixed":
        keys = [
            "type_file", "ip_prefix_file", "ip_suffix_file", "ip_rank_file", "ip_literal_file",
            "ip_port_delta_file", "ip_port_width_file", "pair_left_file", "pair_right_file",
            "pair_width_file", "raw_file",
        ]
        return [str(stream[key]) for key in keys]
    if kind in {
        "hms_delta", "month_day_hms_delta", "asctime_year_delta", "apache_timestamp_delta",
        "int_delta", "int_delta_width", "int_abs_width", "affixed_int_delta", "colon_duration_delta",
    }:
        files = [_stream_file_or_default(stream, "file", "delta.bin")]
        for key in ("layout_file", "width_file"):
            if key in stream:
                files.append(str(stream[key]))
        return files
    if kind == "day_hms_delta":
        return [str(stream["file"]), str(stream["width_file"])]
    if kind == "syslog_delta":
        files = [str(stream["file"])]
        if "layout_file" in stream:
            files.append(str(stream["layout_file"]))
        return files
    if kind == "packed_datetime_delta":
        return [str(stream["file"]), str(stream["layout_file"])]
    if kind == "int_abs_fixed_width":
        return [_stream_file_or_default(stream, "file", "abs.bin")]
    if kind == "int_delta_fixed_width":
        return [_stream_file_or_default(stream, "file", "delta.bin")]
    if kind == "int_abs":
        return [_stream_file_or_default(stream, "file", "abs.bin")]
    if kind == "int_denum_bucket_delta":
        return [str(stream["id_file"]), str(stream["meta_file"]), str(stream["delta_file"])]
    if kind == "delimited_int_tuple_delta":
        return [str(value) for value in stream.get("column_files", [])] + [str(stream["width_file"])]
    if kind == "numeric_skeleton_tuple_delta":
        return [str(value) for value in stream.get("column_files", [])] + [str(stream["layout_file"])]
    if kind == "mixed_skeleton_delta":
        return (
            [str(value) for value in stream.get("numeric_files", [])]
            + [str(stream["layout_file"])]
            + [str(value) for value in stream.get("literal_rank_files", [])]
            + [str(value) for value in stream.get("literal_table_files", [])]
        )
    if kind == "shape_mixed_skeleton_delta":
        numeric_files = [
            str(value)
            for nested in stream.get("numeric_files", [])
            for value in nested
        ]
        return (
            [str(stream["shape_id_file"]), str(stream["shape_meta_file"])]
            + numeric_files
            + [str(value) for value in stream.get("layout_files", [])]
        )
    if kind == "uint16_split_width":
        return [str(stream["file"]), str(stream["width_file"])]
    if kind == "open_function":
        if stream.get("python_exec_value_type") == "str" and isinstance(stream.get("python_exec_string_codec"), dict):
            files = compact_stream_files(dict(stream["python_exec_string_codec"]))
            if "layout_file" in stream:
                files.append(str(stream["layout_file"]))
            return files
        if stream.get("open_numeric_codec") == "layout_shared_delta":
            return [
                str(stream["file"]),
                str(stream["layout_route_file"]),
                str(stream["layout_table_file"]),
            ]
        if stream.get("open_numeric_codec") == "int_denum_bucket_delta":
            return [str(stream["id_file"]), str(stream["meta_file"]), str(stream["delta_file"])]
        if stream.get("open_numeric_codec") == "modular_delta_width":
            files = [str(stream["quotient_file"]), str(stream["remainder_file"]), str(stream["width_file"])]
            if "literal_file" in stream:
                files.append(str(stream["literal_file"]))
            return files
        if stream.get("open_numeric_codec") == "zigzag_abs":
            files = [str(stream["file"])]
            if "layout_file" in stream:
                files.append(str(stream["layout_file"]))
            return files
        files = [_stream_file_or_default(stream, "file", "ofdelta.bin")]
        if "layout_file" in stream:
            files.append(str(stream["layout_file"]))
        return files
    if kind == "open_context_delta":
        return [
            str(stream["context_rank_file"]),
            str(stream["context_literal_file"]),
            str(stream["delta_file"]),
        ]
    if kind == "open_context_dict":
        return [
            str(stream["context_rank_file"]),
            str(stream["context_literal_file"]),
            str(stream["value_rank_file"]),
            str(stream["value_literal_file"]),
        ]
    if kind == "merged_string_mtf_cdelta_ref":
        return []
    if kind == "routed_split":
        files: list[str] = []
        for child in stream.get("children", []):
            files.extend(compact_stream_files(child))
        fallback = stream.get("fallback")
        if isinstance(fallback, dict):
            files.extend(compact_stream_files(fallback))
        return files
    raise ValueError(f"Unsupported compact archive stream kind {kind!r} for tag {tag!r}")


def compact_archive_paths_from_metadata(metadata: dict[str, Any]) -> list[str]:
    paths: list[str] = []
    main_core = metadata.get("main_core", {"kind": "id_mapping"})
    if str(main_core.get("kind", "id_mapping")) == "id_mapping":
        id_info = metadata.get("id_mapping", {})
        paths.extend([
            str(id_info.get("id_file", "id_mapping/ids.bin")),
            str(id_info.get("template_file", "id_mapping/templates.bin")),
        ])
    else:
        raise ValueError("Compact archive currently supports id_mapping main core")
    for stream in metadata.get("dataset_extract", {}).get("streams", []):
        paths.extend(f"dataset_extract/{name}" for name in compact_stream_files(stream))
    paths.append("metadata.json")
    return sorted(dict.fromkeys(paths), key=compact_archive_sort_key)


def compact_archive_should_use_extreme(metadata: dict[str, Any]) -> bool:
    """Choose the xz extreme flag from block-local compression features.

    Extreme mode is useful when the block is small or the separated streams are
    light enough that xz dominates neither latency nor energy.  Heavy residual
    value-stream blocks already spend most time in replay/stream encoding; for
    those blocks, non-extreme preset 6 preserves most ratio while improving
    throughput.  The rule deliberately avoids dataset names and publication
    bars, so it is usable as a frozen deployment policy rather than a benchmark
    selector.
    """
    if os.environ.get("PARE_COMPACT_ARCHIVE_AUTO_EXTREME", "0") not in {"1", "true", "True"}:
        return os.environ.get("PARE_COMPACT_ARCHIVE_EXTREME", "1") not in {"0", "false", "False"}
    try:
        original_size = int(metadata.get("original_size", 0) or 0)
    except Exception:
        original_size = 0
    stream_meta = (
        metadata.get("dataset_extract", {})
        if isinstance(metadata.get("dataset_extract", {}), dict)
        else {}
    )
    llm_meta = (
        stream_meta.get("stream_template_llm", {})
        if isinstance(stream_meta.get("stream_template_llm", {}), dict)
        else {}
    )
    try:
        accepted_functions = int(llm_meta.get("accepted_functions", 0) or 0)
    except Exception:
        accepted_functions = 0
    try:
        residual_stats = (
            llm_meta.get("residual_variable_stats", {})
            if isinstance(llm_meta.get("residual_variable_stats", {}), dict)
            else {}
        )
        residual_values = int(
            llm_meta.get("residual_variable_values", residual_stats.get("values", 0)) or 0
        )
    except Exception:
        residual_values = 0
    try:
        llm_calls = int(llm_meta.get("llm_calls", 0) or 0)
    except Exception:
        llm_calls = 0

    small_block = 0 < original_size <= 6 * 1024 * 1024
    cache_replay_block = (
        llm_calls == 0
        and 0 < original_size <= 12 * 1024 * 1024
        and accepted_functions <= 80
    )
    light_extraction = (
        original_size <= 12 * 1024 * 1024
        and accepted_functions <= 8
        and residual_values <= 5_000
    )
    return small_block or cache_replay_block or light_extraction


def compact_archive_should_use_fast_preset(metadata: dict[str, Any]) -> bool:
    """Return True when preset 5 is preferable to preset 6 for throughput.

    This is only consulted when auto preset selection is enabled.  The rule
    targets blocks whose source separation already produced many residual
    values or LLM-derived streams, while the block is small enough that LZMA
    effort can dominate wall time.  Large blocks keep preset 6 because their
    ratio margin is usually more valuable than the small archive-time saving.
    """
    if os.environ.get("PARE_COMPACT_ARCHIVE_AUTO_PRESET", "0") not in {"1", "true", "True"}:
        return False
    try:
        original_size = int(metadata.get("original_size", 0) or 0)
    except Exception:
        original_size = 0
    stream_meta = (
        metadata.get("dataset_extract", {})
        if isinstance(metadata.get("dataset_extract", {}), dict)
        else {}
    )
    llm_meta = (
        stream_meta.get("stream_template_llm", {})
        if isinstance(stream_meta.get("stream_template_llm", {}), dict)
        else {}
    )
    try:
        residual_stats = (
            llm_meta.get("residual_variable_stats", {})
            if isinstance(llm_meta.get("residual_variable_stats", {}), dict)
            else {}
        )
        residual_values = int(
            llm_meta.get("residual_variable_values", residual_stats.get("values", 0)) or 0
        )
    except Exception:
        residual_values = 0
    try:
        llm_calls = int(llm_meta.get("llm_calls", 0) or 0)
    except Exception:
        llm_calls = 0
    return (
        0 < original_size <= 10 * 1024 * 1024
        and (llm_calls > 0 or residual_values >= 50_000)
    )


def create_compact_archive(output_root: Path, archive_path: Path) -> int:
    metadata = json.loads((output_root / "metadata.json").read_text(encoding="utf-8"))
    policy_metadata = metadata
    if os.environ.get("PARE_COMPACT_ARCHIVE_AUTO_EXTREME", "0") in {"1", "true", "True"}:
        audit_path = output_root.parent / "metadata.audit.json"
        if audit_path.is_file():
            try:
                policy_metadata = json.loads(audit_path.read_text(encoding="utf-8"))
            except Exception:
                policy_metadata = metadata
    paths = compact_archive_paths_from_metadata(metadata)
    actual_paths = sorted(
        [str(path.relative_to(output_root)).replace("\\", "/") for path in output_root.rglob("*") if path.is_file()],
        key=compact_archive_sort_key,
    )
    if paths != actual_paths:
        missing = sorted(set(paths) - set(actual_paths))[:5]
        extra = sorted(set(actual_paths) - set(paths))[:5]
        raise ValueError(f"Compact archive path mismatch missing={missing} extra={extra}")
    payload = bytearray(COMPACT_ARCHIVE_MAGIC)
    payload.extend(encode_archive_varint(len(paths)))
    for rel_path in paths:
        data = (output_root / rel_path).read_bytes()
        payload.extend(encode_archive_varint(len(data)))
        payload.extend(data)
    preset = int(os.environ.get("PARE_COMPACT_ARCHIVE_PRESET", "6"))
    use_extreme = compact_archive_should_use_extreme(policy_metadata)
    if not use_extreme and compact_archive_should_use_fast_preset(policy_metadata):
        preset = min(preset, 5)
    if use_extreme:
        preset |= lzma.PRESET_EXTREME
    archive_path.write_bytes(lzma.compress(bytes(payload), preset=preset))
    return archive_path.stat().st_size


def create_archive(output_root: Path, archive_path: Path, *, compact_container: bool = False) -> int:
    if archive_path.exists():
        archive_path.unlink()
    if compact_container:
        return create_compact_archive(output_root, archive_path)

    def stable_tarinfo(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
        tarinfo.uid = 0
        tarinfo.gid = 0
        tarinfo.uname = ""
        tarinfo.gname = ""
        tarinfo.mtime = 0
        return tarinfo

    with lzma.open(archive_path, "wb") as compressed_stream:
        with tarfile.open(fileobj=compressed_stream, mode="w") as tar:
            tar.add(output_root, arcname=output_root.name, filter=stable_tarinfo)
    return archive_path.stat().st_size


def extract_archive(archive_path: Path, restore_root: Path) -> Path:
    if restore_root.exists():
        shutil.rmtree(restore_root)
    restore_root.mkdir(parents=True, exist_ok=True)
    decompressed = lzma.open(archive_path, "rb").read()
    if decompressed.startswith(COMPACT_ARCHIVE_MAGIC):
        pos = len(COMPACT_ARCHIVE_MAGIC)
        count, pos = decode_archive_varint(decompressed, pos)
        chunks: list[bytes] = []
        for _ in range(count):
            size, pos = decode_archive_varint(decompressed, pos)
            chunks.append(decompressed[pos:pos + size])
            pos += size
        if pos != len(decompressed):
            raise ValueError("Trailing bytes in compact archive")
        root = restore_root / "compressed"
        root.mkdir(parents=True, exist_ok=True)
        if not chunks:
            raise ValueError("Empty compact archive")
        metadata = json.loads(chunks[-1].decode("utf-8"))
        paths = compact_archive_paths_from_metadata(metadata)
        if len(paths) != len(chunks):
            raise ValueError(f"Compact archive path count mismatch paths={len(paths)} chunks={len(chunks)}")
        for rel_path, data in zip(paths, chunks):
            target = root / rel_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        return root
    with lzma.open(archive_path, "rb") as compressed_stream:
        with tarfile.open(fileobj=compressed_stream, mode="r") as tar:
            tar.extractall(restore_root)
    children = [path for path in restore_root.iterdir() if path.is_dir()]
    if len(children) != 1:
        raise ValueError(f"Expected one archive root, got {len(children)}")
    return children[0]


def decode_archive_root(root: Path) -> str:
    metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
    main_core = metadata.get("main_core", {"kind": "id_mapping"})
    if str(main_core.get("kind")) == "rank":
        transformed = read_rank_core(root, metadata)
    elif str(main_core.get("kind")) == "line_rank":
        main_text = read_rank_core(root, metadata)
        transformed = dataset_extract.expand_line_dictionary_text(main_text, root, metadata)
    else:
        transformed = read_id_mapping(root, metadata)
    return dataset_extract.restore_text(transformed, root / "dataset_extract", metadata["dataset_extract"])


def compact_decoder_metadata(metadata: dict[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    """Keep only the metadata required by the decoder inside the archive."""

    stats = {
        "removed_stream_keys": 0,
        "removed_root_keys": 0,
    }
    compact: dict[str, Any] = {}
    main_core = metadata.get("main_core", {"kind": "id_mapping"})
    if str(main_core.get("kind", "id_mapping")) != "id_mapping":
        compact["main_core"] = main_core
    else:
        stats["removed_root_keys"] += 1

    id_mapping = dict(metadata.get("id_mapping", {}))
    if (
        id_mapping.get("template_file", "id_mapping/templates.bin") != "id_mapping/templates.bin"
        or id_mapping.get("id_file", "id_mapping/ids.bin") != "id_mapping/ids.bin"
    ):
        compact["id_mapping"] = id_mapping
    elif id_mapping:
        stats["removed_root_keys"] += len(id_mapping)

    context_kinds = {"port_ip_delta_width", "port_ip_grouped_circular_delta_width", "size_from_prev_bytes"}
    keep_count_kinds = {
        "merged_string_mtf_cdelta_ref",
        "routed_split",
        "port_ip_delta_width",
        "port_ip_grouped_circular_delta_width",
        "size_from_prev_bytes",
        "relation_pair_delta",
        "byte_phrase_delta",
        "hex_int_delta_width",
        "hex_pair_delta",
        "int_denum_bucket_delta",
        "compound_endpoint_mixed",
        "shape_mixed_skeleton_delta",
        "uint16_split_width",
    }

    def remove_if_default(stream: dict[str, Any], key: str, value: str) -> None:
        if stream.get(key) == value:
            stream.pop(key)
            stats["removed_stream_keys"] += 1

    streams: list[dict[str, Any]] = []
    for source in metadata["dataset_extract"]["streams"]:
        stream = dict(source)
        tag = str(stream["tag"])
        kind = str(stream["kind"])
        if stream.get("placeholder") == f"<{tag}>":
            stream.pop("placeholder")
            stats["removed_stream_keys"] += 1
        needs_count = kind in keep_count_kinds or (
            kind == "open_function"
            and str(stream.get("open_numeric_codec", "")) in {
                "int_denum_bucket_delta",
                "modular_delta_width",
                "layout_shared_delta",
                "zigzag_abs",
            }
        )
        if not needs_count and "count" in stream:
            stream.pop("count")
            stats["removed_stream_keys"] += 1
        if stream.get("table_size") == 512:
            stream.pop("table_size")
            stats["removed_stream_keys"] += 1
        if kind not in context_kinds and "context_tag" in stream:
            stream.pop("context_tag")
            stats["removed_stream_keys"] += 1
        for audit_key in ("auto_candidate_count", "auto_cost", "auto_selected"):
            if audit_key in stream:
                stream.pop(audit_key)
                stats["removed_stream_keys"] += 1
        defaults = {
            "string": {"file": f"{tag}.strings.bin"},
            "string_mtf_rank": {"rank_file": f"{tag}.rank.bin", "literal_file": f"{tag}.literal.bin"},
            "string_mtf_cdelta": {"rank_file": f"{tag}.cdelta.bin", "literal_file": f"{tag}.literal.bin"},
            "ipv4_plain": {"file": f"{tag}.ipv4.bin"},
            "ipv4_width": {"file": f"{tag}.ipv4w.bin"},
            "open_function": {"file": f"{tag}.ofdelta.bin"},
            "int_delta": {"file": f"{tag}.delta.bin"},
            "int_delta_fixed_width": {"file": f"{tag}.delta.bin"},
            "int_abs_fixed_width": {"file": f"{tag}.abs.bin"},
            "int_abs": {"file": f"{tag}.abs.bin"},
        }.get(kind, {})
        for key, value in defaults.items():
            remove_if_default(stream, key, value)
        if (
            kind == "open_function"
            and "fraction_scale" in stream
            and "fraction_width" in stream
            and stream.get("fraction_scale") == stream.get("fraction_width")
        ):
            stream.pop("fraction_scale")
            stats["removed_stream_keys"] += 1
        streams.append(stream)

    compact["dataset_extract"] = {"streams": streams}
    return compact, stats


def compress(args: argparse.Namespace) -> dict[str, Any]:
    dataset_extract.set_feature_codec_prefilter(bool(args.feature_codec_prefilter))
    dataset_extract.load_codec_decision_cache(args.codec_decision_cache)
    dataset_extract.set_operation_profile(operation_profile_enabled(args))
    dataset_extract.reset_operation_timings()
    setattr(args, "_operation_timings", {})
    setattr(
        args,
        "_regex_structure_anchor_stats",
        {
            "enabled": int(bool(getattr(args, "require_regex_structure_anchor", False))),
            "proposal_rejected": 0,
            "replay_rejected": 0,
            "rejected": [],
        },
    )
    residual_schema_plan = load_residual_schema_plan(args.residual_schema_plan_in)
    if residual_schema_plan is None:
        residual_schema_plan = load_residual_schema_plan_from_replay(args.residual_schema_plan_in, args.dataset)
    if residual_schema_plan is not None:
        setattr(args, "_residual_schema_plan", residual_schema_plan)
    if args.residual_schema_plan_out or args.replay_plan_out:
        setattr(args, "_residual_schema_plan_capture", [])
    input_path = Path(args.input)
    original_text = read_text_lossless(input_path)
    original_lines = dataset_extract.split_physical_lines(original_text)
    output_root = Path(args.output_dir) / "compressed"
    archive_path = Path(args.output_dir) / "archive.tar.xz"
    if output_root.parent.exists():
        shutil.rmtree(output_root.parent)
    output_root.mkdir(parents=True, exist_ok=True)

    families: list[Family] = []
    line_families: list[Family] = []
    exact_index: dict[tuple[str, ...], Family] = {}
    transformed_lines = list(original_lines)
    values_by_tag: dict[str, list[str]] = {}
    placeholders: dict[str, str] = {}
    accepted_specs: list[CandidateSpec] = []
    semantic_registry: dict[str, str] = {}
    semantic_registry_specs: dict[str, CandidateSpec] = {}
    used_registry_tags: set[str] = set()
    family_anchor_index: dict[str, list[Family]] = {}
    llm_calls = 0
    created_families = 0
    removed_family_specs = 0
    removed_low_gain_specs = 0
    relation_streams = 0
    constant_pruned_local_streams = 0
    residual_line_stats = {
        "candidate_slots": 0,
        "candidate_count": 0,
        "admitted_slots": 0,
    }
    residual_numeric_lattice_stats = {
        "occurrences": 0,
        "groups": 0,
        "admitted": 0,
        "values": 0,
        "score": 0,
    }
    residual_variable_stats = {
        "candidate_groups": 0,
        "admitted_groups": 0,
        "values": 0,
        "shape_gate_enabled": 0,
        "shape_gate_run_shape": 0,
        "shape_gate_before_templates": 0,
        "shape_gate_after_templates": 0,
        "shape_gate_collapse": 0,
        "shape_gate_eligible_groups": 0,
        "shape_gate_eligible_values": 0,
        "shape_gate_raw_main": 0,
        "shape_pregate_enabled": 0,
        "shape_pregate_run_full_gate": 0,
        "shape_pregate_before_templates": 0,
        "shape_pregate_after_templates": 0,
        "shape_pregate_collapse": 0,
    }
    residual_xsignature_stats = {
        "occurrences": 0,
        "candidate_groups": 0,
        "admitted_groups": 0,
        "values": 0,
        "skipped_placeholder_tokens": 0,
    }
    residual_family_simple_stats = {
        "enabled": 0,
        "families": 0,
        "eligible_families": 0,
        "skipped_small_families": 0,
        "numeric_streams": 0,
        "mixed_streams": 0,
        "numeric_values": 0,
        "mixed_values": 0,
    }
    residual_stream_coalesce_stats = {
        "candidate_tags": 0,
        "admitted": 0,
        "values": 0,
        "score": 0,
    }
    placeholder_slot_fission_stats = {
        "candidate_tags": 0,
        "accepted_tags": 0,
        "split_streams": 0,
        "literalized_slots": 0,
        "values_moved": 0,
        "score": 0,
    }
    residual_planner_stats = {
        "enabled": 0,
        "choice": "",
        "numeric_then_variable_cost": 0,
        "variable_then_numeric_cost": 0,
        "variable_only_cost": 0,
        "operator_gate_enabled": 0,
        "operator_gate_admitted": 0,
        "operator_gate_rejected": 0,
        "operator_gate_baseline_cost": 0,
        "operator_gate_candidate_cost": 0,
        "operator_gate_score": 0,
        "operator_gate_margin": 0,
        "dominance_probe_admitted": 0,
        "dominance_probe_before_templates": 0,
        "dominance_probe_after_templates": 0,
        "dominance_probe_density": 0,
    }
    residual_auto_skip_stats = {
        "enabled": 0,
        "skipped": 0,
        "template_count": 0,
        "threshold": 0,
    }
    residual_stage_seconds: dict[str, float] = {}
    execution_plan_payload: dict[str, Any] | None = None
    global_value_rescue_stats = {
        "candidate_count": 0,
        "admitted": 0,
        "skipped_by_class": 0,
    }
    early_global_value_rescue_stats = {
        "candidate_count": 0,
        "admitted": 0,
        "skipped_by_class": 0,
        "shadowed_by_semantic": 0,
    }
    replay_plan_cache_stats = {
        "enabled": 0,
        "hit": 0,
        "specs": 0,
        "values": 0,
    }
    semantic_replay_plan_cache_stats = {
        "enabled": int(bool(args.semantic_replay_plan_cache)),
        "hit": 0,
        "specs": 0,
        "values": 0,
    }
    global_rescue_decision_cache_specs = load_global_rescue_decision_cache(
        args.global_rescue_decision_cache,
        args.dataset,
    )
    global_rescue_decision_cache_stats = {
        "enabled": int(bool(args.global_rescue_decision_cache)),
        "specs": len(global_rescue_decision_cache_specs),
    }
    stage_seconds: dict[str, float] = {}
    start = time.perf_counter()
    stage_checkpoint = start

    def mark_stage(name: str) -> None:
        nonlocal stage_checkpoint
        now = time.perf_counter()
        stage_seconds[name] = stage_seconds.get(name, 0.0) + (now - stage_checkpoint)
        stage_checkpoint = now

    def mark_residual_substage(name: str, started_at: float) -> None:
        residual_stage_seconds[name] = residual_stage_seconds.get(name, 0.0) + (time.perf_counter() - started_at)

    replay_payload = load_replay_payload(args.replay_plan_cache, args.dataset)
    execution_replay_plan_payload = (
        replay_payload.get("execution_plan")
        if isinstance(replay_payload, dict) and isinstance(replay_payload.get("execution_plan"), dict)
        else None
    )
    replay_plan = None if execution_replay_plan_payload else load_replay_plan(args.replay_plan_cache, args.dataset)
    if execution_replay_plan_payload:
        args.max_llm_calls = 0
        args.semantic_global_replay = True
        args.global_value_rescue = False
        args.canonical_global_fields = False
    if replay_plan is not None:
        plan_specs, plan_placeholders = replay_plan
        plan_specs = filter_replay_specs_by_regex_structure(plan_specs, args)
        plan_start = time.perf_counter()
        cpp_replay = apply_specs_to_original_text_cpp_pcre2(original_text, plan_specs, plan_placeholders, args)
        if cpp_replay is not None:
            transformed_text_from_plan, plan_values_by_tag, plan_specs, cpp_replay_stats = cpp_replay
            transformed_lines = dataset_extract.split_physical_lines(transformed_text_from_plan)
        else:
            cpp_replay_stats = {"enabled": int(bool(getattr(args, "cpp_pcre2_replay", False))), "used": 0}
            plan_values_by_tag: dict[str, list[str]] = {}
            transformed_lines = [
                apply_specs_to_original_line(
                    line,
                    plan_specs,
                    plan_placeholders,
                    plan_values_by_tag,
                    trusted_fast_path=True,
                    skip_placeholder_protection=args.trusted_replay_skip_placeholder_protection,
                    skip_exact_value_check=args.trusted_replay_skip_exact_value_check,
                )
                for line in original_lines
            ]
        values_by_tag = plan_values_by_tag
        placeholders = {spec.tag: plan_placeholders[spec.tag] for spec in plan_specs if spec.tag in plan_placeholders}
        accepted_specs = list(plan_specs)
        replay_plan_cache_stats = {
            "enabled": 1,
            "hit": 1,
            "specs": len(plan_specs),
            "values": sum(len(values) for values in plan_values_by_tag.values()),
            "cpp_pcre2_replay": cpp_replay_stats,
        }
        args.max_llm_calls = 0
        args.semantic_global_replay = False
        args.global_value_rescue = False
        args.canonical_global_fields = False
        args.residual_auto_skip_low_templates = 0
        args.residual_dominance_planner = False
        args.residual_mdl_planner = False
        args.residual_numeric_lattice = False
        args.residual_line_transducer = False
        args.residual_variable_stream_mode = "none"
        args.residual_stream_coalesce = False
        args.relation_streams = False
        args.placeholder_slot_fission = False
        stage_seconds["replay_plan_cache"] = time.perf_counter() - plan_start
        stage_checkpoint = time.perf_counter()
    else:
        semantic_replay_plan = load_replay_plan(args.semantic_replay_plan_cache, args.dataset)
        if semantic_replay_plan is not None:
            plan_specs, plan_placeholders = semantic_replay_plan
            semantic_specs = [spec for spec in plan_specs if not is_stage_residual_spec(spec)]
            semantic_specs = filter_replay_specs_by_regex_structure(semantic_specs, args)
            semantic_placeholders = {
                spec.tag: plan_placeholders[spec.tag]
                for spec in semantic_specs
                if spec.tag in plan_placeholders
            }
            if semantic_specs and semantic_placeholders:
                plan_start = time.perf_counter()
                if args.global_long_span_first:
                    semantic_specs = sorted(
                        semantic_specs,
                        key=lambda spec: global_replay_span_priority(spec, original_text),
                        reverse=True,
                    )
                cpp_replay = apply_specs_to_original_text_cpp_pcre2(original_text, semantic_specs, semantic_placeholders, args)
                if cpp_replay is not None:
                    transformed_text_from_plan, plan_values_by_tag, semantic_specs, cpp_replay_stats = cpp_replay
                else:
                    cpp_replay_stats = {"enabled": int(bool(getattr(args, "cpp_pcre2_replay", False))), "used": 0}
                    plan_values_by_tag: dict[str, list[str]] = {}
                    transformed_text_from_plan = apply_specs_to_original_text(
                        original_text,
                        semantic_specs,
                        semantic_placeholders,
                        plan_values_by_tag,
                        trusted_fast_path=True,
                        skip_placeholder_protection=args.trusted_replay_skip_placeholder_protection,
                        skip_exact_value_check=args.trusted_replay_skip_exact_value_check,
                    )
                transformed_lines = dataset_extract.split_physical_lines(transformed_text_from_plan)
                values_by_tag = plan_values_by_tag
                placeholders = {spec.tag: semantic_placeholders[spec.tag] for spec in semantic_specs if spec.tag in semantic_placeholders}
                accepted_specs = list(semantic_specs)
                semantic_replay_plan_cache_stats = {
                    "enabled": 1,
                    "hit": 1,
                    "specs": len(semantic_specs),
                    "values": sum(len(values) for values in plan_values_by_tag.values()),
                    "cpp_pcre2_replay": cpp_replay_stats,
                }
                if not args.semantic_replay_allow_llm_update:
                    args.max_llm_calls = 0
                args.semantic_global_replay = False
                args.global_value_rescue = False
                args.canonical_global_fields = False
                stage_seconds["semantic_replay_plan_cache"] = time.perf_counter() - plan_start
                stage_checkpoint = time.perf_counter()

    family_match_rare_anchor_auto_stats: dict[str, Any] = {}
    effective_family_match_rare_anchor_limit = args.family_match_rare_anchor_limit
    line_template_keys: list[tuple[str, ...]] | None = None
    if args.family_match_rare_anchor_auto:
        op_start = time.perf_counter()
        sample_size = min(len(original_lines), 8192)
        _sample_keys, sample_stats = analyze_family_match_rare_anchor(original_lines[:sample_size])
        sample_consider_full = (
            sample_stats.get("line_count", 0) >= 2048
            and sample_stats.get("unique_keys", 0) >= 200
            and sample_stats.get("distinct_anchors", 0) >= 500
            and sample_stats.get("p90_key_len", 0) >= 14
            and sample_stats.get("median_anchor_line_fanout", 0) < 2000
        )
        sample_short_sparse_enable = (
            sample_stats.get("line_count", 0) >= 2048
            and sample_stats.get("unique_keys", 0) >= 700
            and sample_stats.get("distinct_anchors", 0) >= 1000
            and 7 <= sample_stats.get("p90_key_len", 0) <= 12
            and sample_stats.get("median_anchor_line_fanout", 0) < 1000
        )
        family_match_rare_anchor_auto_stats = {
            **sample_stats,
            "sample": sample_stats,
            "sample_size": sample_size,
            "sample_consider_full_scan": bool(sample_consider_full),
            "sample_short_sparse_enable": bool(sample_short_sparse_enable),
            "sample_rule": (
                "sample_line_count>=2048 and unique_keys>=200 and "
                "distinct_anchors>=500 and p90_key_len>=14 and "
                "median_anchor_line_fanout<2000"
            ),
            "sample_short_sparse_rule": (
                "sample_line_count>=2048 and unique_keys>=700 and "
                "distinct_anchors>=1000 and 7<=p90_key_len<=12 and "
                "median_anchor_line_fanout<1000"
            ),
            "full_scan_run": False,
        }
        if effective_family_match_rare_anchor_limit <= 0 and sample_short_sparse_enable:
            effective_family_match_rare_anchor_limit = 5
        elif sample_consider_full:
            line_template_keys, full_stats = analyze_family_match_rare_anchor(original_lines)
            family_match_rare_anchor_auto_stats = {
                **full_stats,
                "sample": sample_stats,
                "full": full_stats,
                "sample_size": sample_size,
                "sample_consider_full_scan": True,
                "sample_short_sparse_enable": False,
                "sample_rule": family_match_rare_anchor_auto_stats["sample_rule"],
                "sample_short_sparse_rule": family_match_rare_anchor_auto_stats["sample_short_sparse_rule"],
                "full_scan_run": True,
            }
        if (
            effective_family_match_rare_anchor_limit <= 0
            and family_match_rare_anchor_auto_stats.get("decision_enable")
        ):
            effective_family_match_rare_anchor_limit = 3
        family_match_rare_anchor_auto_stats["configured_limit"] = args.family_match_rare_anchor_limit
        family_match_rare_anchor_auto_stats["effective_limit"] = effective_family_match_rare_anchor_limit
        elapsed = time.perf_counter() - op_start
        record_operation_timing(args, "family_scan.rare_anchor_auto_scan", elapsed)
        stage_seconds["family_match_rare_anchor_auto_scan"] = elapsed
        stage_checkpoint = time.perf_counter()

    if line_template_keys is None and _pare_cpp_accel is not None:
        op_start = time.perf_counter()
        line_template_keys = template_keys_batch(original_lines)
        record_operation_timing(args, "family_scan.cpp_template_keys_batch", time.perf_counter() - op_start, count=len(original_lines))

    op_profile = operation_profile_enabled(args)
    fuzzy_match_cache: dict[tuple[str, ...], tuple[int, Family]] = {}
    family_generation = 0

    def cached_match_family(key: tuple[str, ...], allow_fuzzy: bool = True) -> Family | None:
        exact = exact_index.get(key)
        if exact is not None:
            return exact
        if args.exact_family_match_only or not allow_fuzzy:
            return None
        cached = fuzzy_match_cache.get(key)
        if cached is not None and cached[0] == family_generation:
            return cached[1]
        family = match_family(
            families,
            key,
            exact_index,
            args.similarity_threshold,
            family_anchor_index,
            effective_family_match_rare_anchor_limit,
        )
        if family is not None:
            fuzzy_match_cache[key] = (family_generation, family)
        return family

    for line_index, line in enumerate(original_lines):
        if op_profile:
            op_start = time.perf_counter()
            if line_template_keys is None:
                key = template_key(line)
                record_operation_timing(args, "family_scan.template_key", time.perf_counter() - op_start)
            else:
                key = line_template_keys[line_index]
                record_operation_timing(args, "family_scan.template_key_reuse", time.perf_counter() - op_start)
            op_start = time.perf_counter()
            family = exact_index.get(key)
            if family is None:
                allow_fuzzy = (
                    args.fuzzy_family_line_budget <= 0
                    or line_index < args.fuzzy_family_line_budget
                )
                family = cached_match_family(key, allow_fuzzy=allow_fuzzy)
            record_operation_timing(args, "family_scan.match_family", time.perf_counter() - op_start)
        else:
            key = line_template_keys[line_index] if line_template_keys is not None else template_key(line)
            family = exact_index.get(key)
            if family is None:
                allow_fuzzy = (
                    args.fuzzy_family_line_budget <= 0
                    or line_index < args.fuzzy_family_line_budget
                )
                family = cached_match_family(key, allow_fuzzy=allow_fuzzy)
        if family is None:
            op_start = time.perf_counter() if op_profile else 0.0
            family = Family(created_families, key=key, key_set=set(key))
            families.append(family)
            exact_index[key] = family
            for anchor in family.key_set:
                family_anchor_index.setdefault(anchor, []).append(family)
            created_families += 1
            family_generation += 1
            if len(fuzzy_match_cache) > 200_000:
                fuzzy_match_cache.clear()
            if op_profile:
                record_operation_timing(args, "family_scan.create_family", time.perf_counter() - op_start)
        line_families.append(family)
        family.line_indexes.append(line_index)
        if len(family.examples) < args.examples_per_family:
            family.examples.append(line)
        if family.active_specs:
            if not args.defer_family_active_replay:
                op_start = time.perf_counter() if op_profile else 0.0
                transformed_lines[line_index] = apply_specs_to_original_line(
                    line,
                    family.active_specs,
                    placeholders,
                    values_by_tag,
                    trusted_fast_path=args.trusted_replay_fast_path,
                    skip_placeholder_protection=args.trusted_replay_skip_placeholder_protection,
                    skip_exact_value_check=args.trusted_replay_skip_exact_value_check,
                )
                if op_profile:
                    record_operation_timing(args, "family_scan.apply_active_specs", time.perf_counter() - op_start)
            continue
        if (
            not args.offline_family_topk_query
            and
            not family.queried
            and len(family.line_indexes) >= args.llm_min_support
            and len(family.line_indexes) >= family.next_llm_probe_support
            and llm_calls < args.max_llm_calls
            and len(family.key) >= args.min_anchor_words
        ):
            router_observed_lines = [
                transformed_lines[index]
                for index in family.line_indexes[: args.examples_per_family]
            ]
            if args.online_evolution_router:
                router_score, router_stats = online_evolution_score(family, router_observed_lines)
                family.online_router_score = router_score
                if router_score < args.online_evolution_min_score:
                    family.next_llm_probe_support = bump_family_probe_support(len(family.line_indexes), args)
                    family.rejected_reason = (
                        "online-router skip "
                        f"score={router_score:.3f} < {args.online_evolution_min_score:.3f}; "
                        f"next_support={family.next_llm_probe_support}; "
                        f"value_tokens={router_stats['avg_value_tokens']:.3f}; "
                        f"value_diversity={router_stats['value_diversity']:.4f}; "
                        f"digit_density={router_stats['digit_density']:.4f}"
                    )
                    stats = get_llm_runtime_stats(args)
                    stats["online_router_skips"] = int(stats.get("online_router_skips", 0)) + 1
                    stats["online_router_skip_score_sum"] = float(stats.get("online_router_skip_score_sum", 0.0)) + router_score
                    continue
                stats = get_llm_runtime_stats(args)
                stats["online_router_admits"] = int(stats.get("online_router_admits", 0)) + 1
                stats["online_router_admit_score_sum"] = float(stats.get("online_router_admit_score_sum", 0.0)) + router_score
            family.queried = True
            op_start = time.perf_counter() if op_profile else 0.0
            prompt = build_prompt(
                family,
                dataset=args.dataset,
                max_examples=args.examples_per_family,
                whole_line_program_cache=args.whole_line_program_cache,
                open_arithmetic_programs=args.open_arithmetic_programs,
                slot_complete_planner=args.slot_complete_planner,
                generic_token_shapes=args.generic_token_shapes,
                function_first_prompt=args.function_first_prompt,
                free_form_program_prompt=args.free_form_program_prompt,
                free_form_transducer_sketch_prompt=args.free_form_transducer_sketch_prompt,
                purpose_only_program_prompt=args.purpose_only_program_prompt,
                semantic_focus_program_prompt=args.semantic_focus_program_prompt,
                semantic_rich_examples_prompt=args.semantic_rich_examples_prompt,
                family_header_program_prompt=args.family_header_program_prompt,
                semantic_numeric_only_program_prompt=args.semantic_numeric_only_program_prompt,
                semantic_three_class_program_prompt=args.semantic_three_class_program_prompt,
                strict_three_class_context_schema=args.strict_three_class_context_schema,
            )
            if op_profile:
                record_operation_timing(args, "family_scan.build_prompt", time.perf_counter() - op_start)
            prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]
            cache_path = Path(args.llm_cache_dir) / f"family_{family.family_id:04d}_{prompt_hash}.json"
            try:
                op_start = time.perf_counter() if op_profile else 0.0
                payload: dict[str, Any] | None = None
                cache_payloads: list[dict[str, Any]] = []
                if cache_path.is_file() and not args.force_llm:
                    cache_payloads.append(call_llm(prompt, args, cache_path))
                template_payload = template_index_payload_for_family(family, args)
                trie_payload = function_trie_payload_for_family(family, args)
                trigger_payload = function_trigger_payload_for_family(family, args)
                cache_payloads.extend(
                    item
                    for item in [template_payload, trie_payload, trigger_payload]
                    if item is not None
                )
                if cache_payloads:
                    payload = merge_proposal_payloads(*cache_payloads)
                    modes = [
                        str(item.get("_proposal_cache_mode", "unknown"))
                        for item in cache_payloads
                    ]
                    payload["_proposal_cache_mode"] = "+".join(sorted(set(modes)))
                if payload is None:
                    payload = call_llm(prompt, args, cache_path)
                payload_mode_for_refresh = str(payload.get("_proposal_cache_mode", ""))
                payload_is_cache_proposal = any(
                    mode in payload_mode_for_refresh
                    for mode in [
                        "exact_cache",
                        "family_fallback",
                        "global_cache_fallback",
                        "template_index_cache",
                        "function_trie_cache",
                        "function_trigger_cache",
                    ]
                )
                if (
                    getattr(args, "llm_refresh_fallback_proposals", False)
                    and not getattr(args, "llm_cache_only", False)
                    and payload_is_cache_proposal
                ):
                    refresh_start = time.perf_counter() if op_profile else 0.0
                    old_force = args.force_llm
                    old_family_fallback = args.llm_family_cache_fallback
                    old_global_fallback = args.llm_global_cache_fallback
                    try:
                        args.force_llm = True
                        args.llm_family_cache_fallback = False
                        args.llm_global_cache_fallback = False
                        fresh_payload = call_llm(prompt, args, cache_path)
                    finally:
                        args.force_llm = old_force
                        args.llm_family_cache_fallback = old_family_fallback
                        args.llm_global_cache_fallback = old_global_fallback
                    payload = merge_proposal_payloads(payload, fresh_payload)
                    if op_profile:
                        record_operation_timing(
                            args,
                            "family_scan.refresh_fallback_with_api",
                            time.perf_counter() - refresh_start,
                        )
                if op_profile:
                    record_operation_timing(args, "family_scan.call_llm_or_cache", time.perf_counter() - op_start)
                llm_calls += 1
                op_start = time.perf_counter() if op_profile else 0.0
                validation_lines_for_router = [
                    transformed_lines[index]
                    for index in family.line_indexes[: args.verify_lines]
                ]
                specs, reason = compile_family_specs(
                    family,
                    payload,
                    transformed_lines,
                    args,
                    tag_start=len(accepted_specs),
                )
                payload_mode = str(payload.get("_proposal_cache_mode", "unknown"))
                cache_gain = family_program_proxy_gain(validation_lines_for_router, specs)
                should_refresh_cache = False
                refresh_kind = ""
                refresh_reason = ""
                payload_is_cache_proposal = any(
                    mode in payload_mode
                    for mode in [
                        "exact_cache",
                        "family_fallback",
                        "global_cache_fallback",
                        "template_index_cache",
                        "function_trie_cache",
                        "function_trigger_cache",
                    ]
                )
                if payload_is_cache_proposal and not args.llm_cache_only:
                    if args.online_evolution_refresh_on_cache_reject and not specs:
                        should_refresh_cache = True
                        refresh_kind = "cache_reject"
                        refresh_reason = "cache_reject"
                    elif (
                        args.online_evolution_refresh_on_weak_cache
                        and specs
                        and cache_gain < args.online_evolution_min_cache_gain
                    ):
                        should_refresh_cache = True
                        refresh_kind = "weak_cache"
                        refresh_reason = f"weak_cache_gain={cache_gain}"
                if (
                    should_refresh_cache
                ):
                    refresh_score = family.online_router_score
                    if not args.online_evolution_router:
                        refresh_score, _router_stats = online_evolution_score(family, router_observed_lines)
                    if refresh_score >= args.online_evolution_min_score:
                        refresh_start = time.perf_counter() if op_profile else 0.0
                        stats = get_llm_runtime_stats(args)
                        stats["online_router_cache_refreshes"] = int(
                            stats.get("online_router_cache_refreshes", 0)
                        ) + 1
                        stats[f"online_router_{refresh_kind}_refreshes"] = int(
                            stats.get(f"online_router_{refresh_kind}_refreshes", 0)
                        ) + 1
                        old_force = args.force_llm
                        old_family_fallback = args.llm_family_cache_fallback
                        old_global_fallback = args.llm_global_cache_fallback
                        try:
                            args.force_llm = True
                            args.llm_family_cache_fallback = False
                            args.llm_global_cache_fallback = False
                            fresh_payload = call_llm(prompt, args, cache_path)
                        finally:
                            args.force_llm = old_force
                            args.llm_family_cache_fallback = old_family_fallback
                            args.llm_global_cache_fallback = old_global_fallback
                        fresh_specs, fresh_reason = compile_family_specs(
                            family,
                            fresh_payload,
                            transformed_lines,
                            args,
                            tag_start=len(accepted_specs),
                        )
                        fresh_gain = family_program_proxy_gain(validation_lines_for_router, fresh_specs)
                        if fresh_specs and (
                            not specs
                            or fresh_gain >= cache_gain + args.online_evolution_min_fresh_gain_delta
                        ):
                            specs, reason = fresh_specs, fresh_reason
                            stats["online_router_cache_refresh_accepts"] = int(
                                stats.get("online_router_cache_refresh_accepts", 0)
                            ) + 1
                            stats["online_router_refresh_gain_delta_sum"] = int(
                                stats.get("online_router_refresh_gain_delta_sum", 0)
                            ) + int(fresh_gain - cache_gain)
                        elif fresh_reason:
                            reason = reason + " | fresh_api: " + fresh_reason
                            stats["online_router_cache_refresh_rejects"] = int(
                                stats.get("online_router_cache_refresh_rejects", 0)
                            ) + 1
                        if op_profile:
                            record_operation_timing(
                                args,
                                "family_scan.online_cache_reject_refresh",
                                time.perf_counter() - refresh_start,
                            )
                if op_profile:
                    record_operation_timing(args, "family_scan.compile_family_specs", time.perf_counter() - op_start)
                if args.no_level_streams:
                    specs = [
                        spec
                        for spec in specs
                        if not is_level_like_spec(spec)
                    ]
                specs = apply_semantic_registry_admission_router(family, specs, args)
                if specs:
                    if args.semantic_stream_registry:
                        assign_semantic_registry_tags(
                            specs,
                            semantic_registry,
                            semantic_registry_specs,
                            used_registry_tags,
                        )
                    append_template_index_entry(args, family, specs, source="online")
                    family.active_specs = specs
                    for spec in specs:
                        if spec.tag not in placeholders:
                            placeholders[spec.tag] = dataset_extract.choose_placeholder(original_text, args.dataset, spec.tag)
                    accepted_specs.extend(specs)
                    if not args.defer_family_active_replay:
                        op_start = time.perf_counter() if op_profile else 0.0
                        for buffered_index in family.line_indexes:
                            transformed_lines[buffered_index] = apply_specs_to_original_line(
                                original_lines[buffered_index],
                                family.active_specs,
                                placeholders,
                                values_by_tag,
                                trusted_fast_path=args.trusted_replay_fast_path,
                                skip_placeholder_protection=args.trusted_replay_skip_placeholder_protection,
                                skip_exact_value_check=args.trusted_replay_skip_exact_value_check,
                            )
                        if op_profile:
                            record_operation_timing(
                                args,
                                "family_scan.replay_buffered_family",
                                time.perf_counter() - op_start,
                                count=len(family.line_indexes),
                            )
                else:
                    family.rejected_reason = reason
            except Exception as exc:
                family.rejected_reason = str(exc)[:300]
    if args.offline_family_topk_query and llm_calls < args.max_llm_calls:
        offline_start = time.perf_counter()
        min_support = (
            args.offline_family_topk_min_support
            if args.offline_family_topk_min_support > 0
            else args.llm_min_support
        )
        topk_budget = args.offline_family_topk if args.offline_family_topk > 0 else args.max_llm_calls
        eligible_families = [
            family
            for family in families
            if (
                not family.queried
                and len(family.line_indexes) >= min_support
                and len(family.key) >= args.min_anchor_words
            )
        ]
        eligible_families.sort(
            key=lambda family: (
                len(family.line_indexes),
                family_value_signal_stats(family)["avg_value_tokens"],
                family_value_signal_stats(family)["value_diversity"],
            ),
            reverse=True,
        )
        offline_stats = {
            "enabled": True,
            "min_support": min_support,
            "topk_budget": topk_budget,
            "eligible_families": len(eligible_families),
            "queried_families": 0,
            "batch_queries": 0,
            "batch_sample_budget": args.offline_family_batch_max_examples,
            "accepted_families": 0,
            "accepted_specs": 0,
            "sample_strategy": "deterministic_evenly_spaced_distinct_members",
        }
        setattr(args, "_offline_family_topk_stats", offline_stats)

        selected_families: list[tuple[Family, list[str]]] = []
        for family in eligible_families[:topk_budget]:
            sampled_examples = select_family_prompt_examples(
                family,
                original_lines,
                args.examples_per_family,
            )
            if sampled_examples:
                family.examples = sampled_examples
            selected_families.append((family, sampled_examples or list(family.examples)))

        family_batches: list[list[tuple[Family, list[str]]]] = []
        if args.offline_family_batch_query:
            current_batch: list[tuple[Family, list[str]]] = []
            current_examples = 0
            max_batch_examples = max(1, args.offline_family_batch_max_examples)
            for family, examples in selected_families:
                example_count = max(1, len(examples[: args.examples_per_family]))
                if current_batch and current_examples + example_count > max_batch_examples:
                    family_batches.append(current_batch)
                    current_batch = []
                    current_examples = 0
                current_batch.append((family, examples))
                current_examples += example_count
            if current_batch:
                family_batches.append(current_batch)
        else:
            family_batches = [[item] for item in selected_families]

        prepared_batches: list[tuple[int, list[tuple[Family, list[str]]], str, Path]] = []
        for batch_index, family_batch in enumerate(family_batches):
            if len(prepared_batches) >= max(0, args.max_llm_calls - llm_calls):
                break
            try:
                if args.offline_family_batch_query and len(family_batch) > 1:
                    op_start = time.perf_counter() if op_profile else 0.0
                    prompt = build_family_batch_prompt(
                        family_batch,
                        dataset=args.dataset,
                        max_examples=args.examples_per_family,
                        open_arithmetic_programs=args.open_arithmetic_programs,
                        slot_complete_planner=args.slot_complete_planner,
                        generic_token_shapes=args.generic_token_shapes,
                        function_first_prompt=args.function_first_prompt,
                        family_header_program_prompt=args.family_header_program_prompt,
                        semantic_three_class_program_prompt=args.semantic_three_class_program_prompt,
                        strict_three_class_context_schema=args.strict_three_class_context_schema,
                    )
                    if op_profile:
                        record_operation_timing(args, "offline_topk.build_batch_prompt", time.perf_counter() - op_start)
                    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]
                    family_ids = "-".join(str(family.family_id) for family, _examples in family_batch[:8])
                    cache_path = Path(args.llm_cache_dir) / f"family_batch_{batch_index:04d}_{family_ids}_{prompt_hash}.json"
                else:
                    family, _examples = family_batch[0]
                    op_start = time.perf_counter() if op_profile else 0.0
                    prompt = build_prompt(
                        family,
                        dataset=args.dataset,
                        max_examples=args.examples_per_family,
                        whole_line_program_cache=args.whole_line_program_cache,
                        open_arithmetic_programs=args.open_arithmetic_programs,
                        slot_complete_planner=args.slot_complete_planner,
                        generic_token_shapes=args.generic_token_shapes,
                        function_first_prompt=args.function_first_prompt,
                        free_form_program_prompt=args.free_form_program_prompt,
                        free_form_transducer_sketch_prompt=args.free_form_transducer_sketch_prompt,
                        purpose_only_program_prompt=args.purpose_only_program_prompt,
                        semantic_focus_program_prompt=args.semantic_focus_program_prompt,
                        semantic_rich_examples_prompt=args.semantic_rich_examples_prompt,
                        family_header_program_prompt=args.family_header_program_prompt,
                        semantic_numeric_only_program_prompt=args.semantic_numeric_only_program_prompt,
                        semantic_three_class_program_prompt=args.semantic_three_class_program_prompt,
                        strict_three_class_context_schema=args.strict_three_class_context_schema,
                    )
                    if op_profile:
                        record_operation_timing(args, "offline_topk.build_prompt", time.perf_counter() - op_start)
                    prompt_hash = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]
                    cache_path = Path(args.llm_cache_dir) / f"family_{family.family_id:04d}_{prompt_hash}.json"

                prepared_batches.append((batch_index, family_batch, prompt, cache_path))
            except Exception as exc:
                for family, _examples in family_batch:
                    family.queried = True
                    family.rejected_reason = str(exc)[:300]

        def compile_offline_payload(family_batch: list[tuple[Family, list[str]]], batch_payload: dict[str, Any]) -> None:
                for family, _examples in family_batch:
                    family.queried = True
                    offline_stats["queried_families"] = int(offline_stats["queried_families"]) + 1
                    payload = (
                        payload_for_family_from_batch(batch_payload, family.family_id)
                        if args.offline_family_batch_query and len(family_batch) > 1
                        else batch_payload
                    )
                    op_start = time.perf_counter() if op_profile else 0.0
                    specs, reason = compile_family_specs(
                        family,
                        payload,
                        transformed_lines,
                        args,
                        tag_start=len(accepted_specs),
                    )
                    if op_profile:
                        record_operation_timing(args, "offline_topk.compile_family_specs", time.perf_counter() - op_start)
                    if args.no_level_streams:
                        specs = [spec for spec in specs if not is_level_like_spec(spec)]
                    specs = apply_semantic_registry_admission_router(family, specs, args)
                    if specs:
                        if args.semantic_stream_registry:
                            assign_semantic_registry_tags(
                                specs,
                                semantic_registry,
                                semantic_registry_specs,
                                used_registry_tags,
                            )
                        append_template_index_entry(args, family, specs, source="offline_topk")
                        family.active_specs = specs
                        for spec in specs:
                            if spec.tag not in placeholders:
                                placeholders[spec.tag] = dataset_extract.choose_placeholder(original_text, args.dataset, spec.tag)
                        accepted_specs.extend(specs)
                        offline_stats["accepted_families"] = int(offline_stats["accepted_families"]) + 1
                        offline_stats["accepted_specs"] = int(offline_stats["accepted_specs"]) + len(specs)
                        if not args.defer_family_active_replay:
                            replay_start = time.perf_counter() if op_profile else 0.0
                            for buffered_index in family.line_indexes:
                                transformed_lines[buffered_index] = apply_specs_to_original_line(
                                    original_lines[buffered_index],
                                    family.active_specs,
                                    placeholders,
                                    values_by_tag,
                                    trusted_fast_path=args.trusted_replay_fast_path,
                                    skip_placeholder_protection=args.trusted_replay_skip_placeholder_protection,
                                    skip_exact_value_check=args.trusted_replay_skip_exact_value_check,
                                )
                            if op_profile:
                                record_operation_timing(
                                    args,
                                    "offline_topk.replay_buffered_family",
                                    time.perf_counter() - replay_start,
                                    count=len(family.line_indexes),
                                )
                    else:
                        family.rejected_reason = reason

        api_workers = max(1, int(getattr(args, "offline_family_api_workers", 1) or 1))
        if api_workers <= 1 or len(prepared_batches) <= 1:
            for _batch_index, family_batch, prompt, cache_path in prepared_batches:
                try:
                    op_start = time.perf_counter() if op_profile else 0.0
                    batch_payload = call_llm(prompt, args, cache_path)
                    if op_profile:
                        record_operation_timing(args, "offline_topk.call_llm_or_cache", time.perf_counter() - op_start)
                    llm_calls += 1
                    offline_stats["batch_queries"] = int(offline_stats["batch_queries"]) + 1
                    compile_offline_payload(family_batch, batch_payload)
                except Exception as exc:
                    for family, _examples in family_batch:
                        family.queried = True
                        family.rejected_reason = str(exc)[:300]
        else:
            op_start = time.perf_counter() if op_profile else 0.0
            payloads_by_index: dict[int, dict[str, Any]] = {}
            errors_by_index: dict[int, str] = {}
            with ThreadPoolExecutor(max_workers=api_workers) as executor:
                future_to_batch = {
                    executor.submit(call_llm, prompt, args, cache_path): (
                        batch_index,
                        family_batch,
                    )
                    for batch_index, family_batch, prompt, cache_path in prepared_batches
                }
                for future, (batch_index, _family_batch) in future_to_batch.items():
                    try:
                        payloads_by_index[batch_index] = future.result()
                    except Exception as exc:
                        errors_by_index[batch_index] = str(exc)[:300]
            if op_profile:
                record_operation_timing(args, "offline_topk.parallel_call_llm_or_cache", time.perf_counter() - op_start)
            for batch_index, family_batch, _prompt, _cache_path in prepared_batches:
                if batch_index in errors_by_index:
                    for family, _examples in family_batch:
                        family.queried = True
                        family.rejected_reason = errors_by_index[batch_index]
                    continue
                llm_calls += 1
                offline_stats["batch_queries"] = int(offline_stats["batch_queries"]) + 1
                compile_offline_payload(family_batch, payloads_by_index[batch_index])
        offline_stats["seconds"] = time.perf_counter() - offline_start
        stage_seconds["offline_family_topk_query"] = offline_stats["seconds"]
    mark_stage("family_scan_compile")

    global_semantic_keys: set[str] = set()
    semantic_classes: set[str] = set()
    if args.semantic_global_replay:
        if execution_replay_plan_payload:
            args.family_first_replay = bool(execution_replay_plan_payload.get("family_first_replay", args.family_first_replay))
            args.global_long_span_first = bool(execution_replay_plan_payload.get("global_long_span_first", args.global_long_span_first))
            args.validated_program_reindex = bool(execution_replay_plan_payload.get("validated_program_reindex", args.validated_program_reindex))
            args.validated_program_family_cache = bool(execution_replay_plan_payload.get("validated_program_family_cache", args.validated_program_family_cache))
        if args.validated_program_family_cache_auto and not args.validated_program_family_cache:
            args.validated_program_family_cache = len(families) >= args.validated_program_family_cache_auto_min_families
        global_semantic_keys = {
            key
            for key in semantic_registry_specs
            if semantic_key_can_global_replay(key) and semantic_key_allowed_by_args(key, args)
        }
        if any(key.startswith("time:") for key in global_semantic_keys):
            global_semantic_keys = {
                key
                for key in global_semantic_keys
                if not key.startswith("raw_time:")
            }
        semantic_classes = {
            klass
            for klass in (semantic_key_class(key) for key in global_semantic_keys)
            if klass
        }

    early_global_specs: list[CandidateSpec] = []
    if args.global_value_rescue:
        semantic_shadow_tags = {
            spec.tag
            for spec in accepted_specs
            if semantic_key_class(spec.semantic_key) in semantic_classes
        }
        early_used_tags = (
            set(tag for tag in placeholders if tag not in semantic_shadow_tags)
            | {spec.tag for spec in accepted_specs if spec.tag not in semantic_shadow_tags}
        )
        # High-support self-delimiting values (e.g., syslog timestamps/IPs)
        # should win before template-local LLM programs can fragment them into
        # many small streams. The template index remains only a retrieval hint;
        # these programs are admitted by global support, proxy gain, and exact
        # replay validation below.
        early_global_specs, early_global_value_rescue_stats = select_global_value_specs_for_text(
            text=original_text,
            original_text=original_text,
            used_tags=early_used_tags,
            args=args,
            # Do not pre-filter verified global rescue candidates by semantic
            # class.  A semantic registry entry is a reusable hint, while this
            # selector still verifies global support, exact reconstruction, and
            # MDL/proxy benefit before materializing a stream.  Filtering here
            # can drop high-value fields such as syslog timestamps before the
            # stronger source-separation path gets a chance to run.
            exclude_classes=set(),
            candidate_specs=global_rescue_decision_cache_specs or None,
            trust_cached_decision=bool(global_rescue_decision_cache_specs),
        )
        unsafe_early_global_count = sum(1 for spec in early_global_specs if not replay_program_safe(spec))
        if unsafe_early_global_count:
            early_global_specs = [spec for spec in early_global_specs if replay_program_safe(spec)]
            early_global_value_rescue_stats["unsafe_filtered"] = unsafe_early_global_count

    if args.semantic_global_replay:
        execution_tag_to_spec: dict[str, CandidateSpec] = {}
        execution_global_tags: list[str] = []
        execution_local_family_tags: dict[tuple[str, ...], list[str]] = {}
        if execution_replay_plan_payload and replay_payload:
            loaded = load_replay_plan(args.replay_plan_cache, args.dataset)
            if loaded is not None:
                loaded_specs, loaded_placeholders = loaded
                execution_tag_to_spec = {spec.tag: spec for spec in loaded_specs}
                for tag, placeholder in loaded_placeholders.items():
                    placeholders.setdefault(tag, placeholder)
            execution_global_tags = [
                str(tag)
                for tag in execution_replay_plan_payload.get("global_tags", [])
                if str(tag) in execution_tag_to_spec
            ]
            raw_local = execution_replay_plan_payload.get("local_families", [])
            if isinstance(raw_local, list):
                for item in raw_local:
                    if not isinstance(item, dict):
                        continue
                    key_payload = item.get("key", [])
                    tags_payload = item.get("tags", [])
                    if not isinstance(key_payload, list) or not isinstance(tags_payload, list):
                        continue
                    tags = [str(tag) for tag in tags_payload if str(tag) in execution_tag_to_spec]
                    if tags:
                        execution_local_family_tags[tuple(str(part) for part in key_payload)] = tags

        if early_global_specs and semantic_classes:
            # Early global rescue has already passed full-line support, MDL/proxy
            # gain, and exact replay validation.  The semantic registry is only a
            # reuse hint, so it must not shadow a verified source separation that
            # may carry a stronger codec, e.g. syslog_delta instead of a generic
            # numeric skeleton for the same timestamp class.
            early_global_value_rescue_stats["shadowed_by_semantic"] = 0
        semantic_replay_specs = (
            {}
            if execution_replay_plan_payload
            else {
                key: global_replay_spec(semantic_registry_specs[key])
                for key in global_semantic_keys
            }
        )
        compact_semantic_classes = {
            klass
            for key, spec in semantic_replay_specs.items()
            for klass in [semantic_key_class(key)]
            if klass and semantic_replay_preempts_early_global(spec)
        }
        if compact_semantic_classes and early_global_specs:
            before_count = len(early_global_specs)
            early_global_specs = [
                spec
                for spec in early_global_specs
                if scout_spec_class(spec) not in compact_semantic_classes
            ]
            early_global_value_rescue_stats["preempted_by_compact_semantic"] = before_count - len(early_global_specs)
        early_global_classes = {
            klass
            for klass in (scout_spec_class(spec) for spec in early_global_specs)
            if klass
        }
        if early_global_classes:
            global_semantic_keys = {
                key
                for key in global_semantic_keys
                if semantic_key_class(key) not in early_global_classes
            }
        if execution_replay_plan_payload:
            global_specs = [
                execution_tag_to_spec[tag]
                for tag in execution_global_tags
                if tag in execution_tag_to_spec
            ]
        else:
            global_specs = early_global_specs + [
                semantic_replay_specs[key]
                for key in sorted(global_semantic_keys, key=semantic_replay_order)
            ]
        global_specs = [spec for spec in global_specs if replay_program_safe(spec)]
        if args.global_long_span_first:
            global_specs.sort(
                key=lambda spec: global_replay_span_priority(spec, original_text),
                reverse=True,
            )
        local_specs_by_family: dict[int, list[CandidateSpec]] = {}
        if execution_replay_plan_payload:
            for family in families:
                tags = execution_local_family_tags.get(tuple(family.key), [])
                local_specs = [
                    execution_tag_to_spec[tag]
                    for tag in tags
                    if tag in execution_tag_to_spec and replay_program_safe(execution_tag_to_spec[tag])
                ]
                if local_specs:
                    local_specs_by_family[family.family_id] = local_specs
        else:
            for family in families:
                local_specs = [
                    spec
                    for spec in family.active_specs
                    if spec.semantic_key not in global_semantic_keys
                    and semantic_key_class(spec.semantic_key) not in early_global_classes
                    and replay_program_safe(spec)
                ]
                if not local_specs:
                    continue
                local_specs_by_family[family.family_id] = local_specs
        if args.fast_constant_local_preprune and args.prune_constant_local_streams:
            # The family-scan pass has already collected local values for the
            # accepted specs.  If a local stream is provably constant there, we
            # can remove it before global replay and avoid a second full-block
            # replay whose only purpose would be dropping the same tag.
            global_tags = {spec.tag for spec in global_specs}
            prepruned_tags = {
                tag
                for specs in local_specs_by_family.values()
                for spec in specs
                for tag in [spec.tag]
                if tag not in global_tags
                and values_by_tag.get(tag)
                and len(set(values_by_tag.get(tag, []))) <= 1
            }
            if prepruned_tags:
                constant_pruned_local_streams += len(prepruned_tags)
                local_specs_by_family = {
                    family_id: [
                        spec
                        for spec in specs
                        if spec.tag not in prepruned_tags
                    ]
                    for family_id, specs in local_specs_by_family.items()
                }
                local_specs_by_family = {
                    family_id: specs
                    for family_id, specs in local_specs_by_family.items()
                    if specs
                }

        program_candidates = [
            ProgramCandidate(
                family_id=family.family_id,
                specs=local_specs_by_family[family.family_id],
                support=len(family.line_indexes),
                key=family.key,
            )
            for family in families
            if family.family_id in local_specs_by_family
        ]
        program_candidates.sort(key=candidate_specificity, reverse=True)

        def replay_with_allowed_tags(allowed_local_tags: set[str] | None = None) -> tuple[list[str], dict[str, list[str]], dict[str, str], list[CandidateSpec]]:
            replayed_lines = list(original_lines)
            replayed_values_by_tag: dict[str, list[str]] = {}
            replayed_placeholders: dict[str, str] = {}
            replayed_accepted_specs: list[CandidateSpec] = []
            global_apply_cache: dict[tuple[str, tuple[str, ...]], tuple[str, dict[str, tuple[str, ...]]]] = {}
            local_apply_cache: dict[tuple[str, tuple[str, ...]], tuple[str, dict[str, tuple[str, ...]]]] = {}
            line_specs_cache: dict[tuple[str, int], list[CandidateSpec]] = {}
            line_candidate_score_cache: dict[tuple[str, int], float | None] = {}
            filtered_specs_cache: dict[int, list[CandidateSpec]] = {}
            ordered_candidates_cache: dict[int, list[ProgramCandidate]] = {}
            global_router_feature_cache: dict[str, dict[str, object]] = {}
            global_router_stats = {
                "enabled": int(bool(args.global_function_feature_router)),
                "lines": 0,
                "full_candidates": 0,
                "selected_candidates": 0,
                "scored_candidates": 0,
                "fallback_full": 0,
            }

            def apply_specs_cached(
                line: str,
                specs: list[CandidateSpec],
                value_sink: dict[str, list[str]],
                cache: dict[tuple[str, tuple[str, ...]], tuple[str, dict[str, tuple[str, ...]]]],
            ) -> str:
                if not specs:
                    return line
                cache_key = (line, tuple(spec.tag for spec in specs))
                cached = cache.get(cache_key)
                if cached is None:
                    local_values: dict[str, list[str]] = {}
                    transformed_line = apply_specs_to_line(
                        line,
                        specs,
                        replayed_placeholders,
                        local_values,
                        trusted_fast_path=args.trusted_replay_fast_path,
                        skip_placeholder_protection=args.trusted_replay_skip_placeholder_protection,
                        skip_exact_value_check=args.trusted_replay_skip_exact_value_check,
                    )
                    cached_values = {
                        tag: tuple(values)
                        for tag, values in local_values.items()
                    }
                    cached = (transformed_line, cached_values)
                    if len(cache) < 262_144:
                        cache[cache_key] = cached
                transformed_line, cached_values = cached
                for tag, values in cached_values.items():
                    if values:
                        value_sink.setdefault(tag, []).extend(values)
                return transformed_line

            for spec in global_specs:
                replayed_placeholders[spec.tag] = dataset_extract.choose_placeholder(original_text, args.dataset, spec.tag)

            def apply_global_specs_to_lines() -> None:
                if not global_specs:
                    return
                for line_index, line in enumerate(replayed_lines):
                    specs_for_line = global_specs
                    if args.global_function_feature_router:
                        specs_for_line, scored_count, used_fallback = global_function_router_select(
                            line,
                            global_specs,
                            global_router_feature_cache,
                            max_functions=args.global_function_feature_router_max_functions,
                            min_score=args.global_function_feature_router_min_score,
                            fallback_full=args.global_function_feature_router_fallback_full,
                        )
                        global_router_stats["lines"] += 1
                        global_router_stats["full_candidates"] += len(global_specs)
                        global_router_stats["selected_candidates"] += len(specs_for_line)
                        global_router_stats["scored_candidates"] += scored_count
                        global_router_stats["fallback_full"] += used_fallback
                    replayed_lines[line_index] = apply_specs_cached(
                        line,
                        specs_for_line,
                        replayed_values_by_tag,
                        global_apply_cache,
                    )

            if not args.family_first_replay:
                replayed_accepted_specs.extend(global_specs)
                apply_global_specs_to_lines()

            local_seen: set[str] = set()
            for specs in local_specs_by_family.values():
                for spec in specs:
                    if allowed_local_tags is not None and spec.tag not in allowed_local_tags:
                        continue
                    if spec.tag not in replayed_placeholders:
                        replayed_placeholders[spec.tag] = dataset_extract.choose_placeholder(original_text, args.dataset, spec.tag)
                    if spec.tag not in local_seen:
                        replayed_accepted_specs.append(spec)
                        local_seen.add(spec.tag)
            if args.family_first_replay:
                replayed_accepted_specs.extend(global_specs)
            protected_global_placeholders = [
                replayed_placeholders[spec.tag]
                for spec in global_specs
            ]

            family_reindex_cache: dict[int, list[CandidateSpec]] = {}

            def line_local_specs(line: str, family: Family) -> list[CandidateSpec]:
                if not args.validated_program_reindex:
                    specs = local_specs_by_family.get(family.family_id, [])
                    if allowed_local_tags is not None:
                        specs = [spec for spec in specs if spec.tag in allowed_local_tags]
                    return specs
                family_key_set = family.key_set

                def candidate_specs_for_reindex(candidate: ProgramCandidate) -> list[CandidateSpec]:
                    cached_specs = filtered_specs_cache.get(candidate.family_id)
                    if cached_specs is not None:
                        return cached_specs
                    specs = candidate.specs
                    if allowed_local_tags is not None:
                        specs = [spec for spec in specs if spec.tag in allowed_local_tags]
                    if len(filtered_specs_cache) < 65_536:
                        filtered_specs_cache[candidate.family_id] = specs
                    return specs

                def ordered_reindex_candidates() -> list[ProgramCandidate]:
                    cached_candidates = ordered_candidates_cache.get(family.family_id)
                    if cached_candidates is not None:
                        return cached_candidates
                    direct_specs = local_specs_by_family.get(family.family_id, [])
                    if allowed_local_tags is not None:
                        direct_specs = [spec for spec in direct_specs if spec.tag in allowed_local_tags]
                    ordered: list[ProgramCandidate] = []
                    if direct_specs:
                        ordered.append(
                            ProgramCandidate(
                                family_id=family.family_id,
                                specs=direct_specs,
                                support=len(family.line_indexes),
                                key=family.key,
                            )
                        )
                    ordered.extend(
                        candidate
                        for candidate in program_candidates
                        if candidate.family_id != family.family_id
                    )
                    if len(ordered_candidates_cache) < 65_536:
                        ordered_candidates_cache[family.family_id] = ordered
                    return ordered

                if args.validated_program_family_cache:
                    cache_key = family.family_id
                    if cache_key in family_reindex_cache:
                        return family_reindex_cache[cache_key]
                    best_specs: list[CandidateSpec] = []
                    best_score: float | None = None
                    best_tiebreak: tuple[int, int, int] = (0, 0, 0)
                    tried_cross_family = 0
                    sample_indexes = family.line_indexes[: max(1, args.program_reindex_family_verify_lines)]
                    sample_lines = [replayed_lines[index] for index in sample_indexes]
                    for candidate in ordered_reindex_candidates():
                        if args.program_reindex_similarity_threshold > 0:
                            score = jaccard(family_key_set, set(candidate.key))
                            if score < args.program_reindex_similarity_threshold and candidate.family_id != family.family_id:
                                continue
                        if candidate.family_id != family.family_id:
                            tried_cross_family += 1
                            if (
                                args.program_reindex_max_candidates_per_line > 0
                                and tried_cross_family > args.program_reindex_max_candidates_per_line
                            ):
                                break
                        specs = candidate_specs_for_reindex(candidate)
                        if not specs:
                            continue
                        scores = [
                            score
                            for sample in sample_lines
                            if (
                                score := validated_line_program_score(
                                    sample,
                                    specs,
                                    replayed_placeholders,
                                    support=candidate.support,
                                    protected_placeholders=protected_global_placeholders,
                                )
                            )
                            is not None
                        ]
                        if not scores:
                            continue
                        coverage = len(scores) / max(1, len(sample_lines))
                        score_value = sum(scores) / max(1, len(sample_lines)) + 8.0 * coverage
                        tiebreak = (candidate.support, len(specs), sum(len(spec.pattern) for spec in specs))
                        if best_score is None or (score_value, tiebreak) > (best_score, best_tiebreak):
                            best_score = score_value
                            best_tiebreak = tiebreak
                            best_specs = specs
                    family_reindex_cache[cache_key] = best_specs
                    return best_specs

                if getattr(args, "program_reindex_template_cache", False):
                    line_cache_key: object = template_key(line)
                else:
                    line_cache_key = line
                exact_cache_key = (line_cache_key, family.family_id)
                cached_specs = line_specs_cache.get(exact_cache_key)
                if cached_specs is not None:
                    return cached_specs
                best_specs: list[CandidateSpec] = []
                best_score: float | None = None
                best_tiebreak: tuple[int, int, int] = (0, 0, 0)
                tried_cross_family = 0
                for candidate in ordered_reindex_candidates():
                    if args.program_reindex_similarity_threshold > 0:
                        score = jaccard(family_key_set, set(candidate.key))
                        if score < args.program_reindex_similarity_threshold and candidate.family_id != family.family_id:
                            continue
                    if candidate.family_id != family.family_id:
                        tried_cross_family += 1
                        if (
                            args.program_reindex_max_candidates_per_line > 0
                            and tried_cross_family > args.program_reindex_max_candidates_per_line
                            ):
                            break
                    specs = candidate_specs_for_reindex(candidate)
                    if not specs:
                        continue
                    score_cache_key = (line_cache_key, candidate.family_id)
                    if score_cache_key in line_candidate_score_cache:
                        score_value = line_candidate_score_cache[score_cache_key]
                    else:
                        score_value = validated_line_program_score(
                            line,
                            specs,
                            replayed_placeholders,
                            support=candidate.support,
                            protected_placeholders=protected_global_placeholders,
                        )
                        if len(line_candidate_score_cache) < 524_288:
                            line_candidate_score_cache[score_cache_key] = score_value
                    if score_value is None:
                        continue
                    tiebreak = (candidate.support, len(specs), sum(len(spec.pattern) for spec in specs))
                    if best_score is None or (score_value, tiebreak) > (best_score, best_tiebreak):
                        best_score = score_value
                        best_tiebreak = tiebreak
                        best_specs = specs
                if len(line_specs_cache) < 262_144:
                    line_specs_cache[exact_cache_key] = best_specs
                return best_specs

            # Preserve value-stream order: values for a shared tag must be
            # appended in the same order that the placeholder appears in text.
            for line_index, family in enumerate(line_families):
                specs = line_local_specs(replayed_lines[line_index], family)
                if not specs:
                    continue
                replayed_lines[line_index] = apply_specs_cached(
                    replayed_lines[line_index],
                    specs,
                    replayed_values_by_tag,
                        local_apply_cache,
                )
            if args.family_first_replay:
                apply_global_specs_to_lines()
            setattr(
                args,
                "_global_function_feature_router_stats",
                {
                    **global_router_stats,
                    "avg_selected_per_line": (
                        global_router_stats["selected_candidates"] / max(1, global_router_stats["lines"])
                    ),
                    "avg_full_per_line": (
                        global_router_stats["full_candidates"] / max(1, global_router_stats["lines"])
                    ),
                },
            )
            return replayed_lines, replayed_values_by_tag, replayed_placeholders, replayed_accepted_specs

        replayed_lines, replayed_values_by_tag, replayed_placeholders, replayed_accepted_specs = replay_with_allowed_tags()
        if args.prune_constant_local_streams:
            global_tags = {spec.tag for spec in global_specs}
            constant_local_tags = {
                tag
                for tag, values in replayed_values_by_tag.items()
                if tag not in global_tags and values and len(set(values)) <= 1
            }
            if constant_local_tags:
                keep_local_tags = {
                    spec.tag
                    for specs in local_specs_by_family.values()
                    for spec in specs
                    if spec.tag not in constant_local_tags
                }
                constant_pruned_local_streams = len(constant_local_tags)
                replayed_lines, replayed_values_by_tag, replayed_placeholders, replayed_accepted_specs = replay_with_allowed_tags(keep_local_tags)
        if args.min_local_stream_score > 0:
            global_tags = {spec.tag for spec in global_specs}
            spec_by_tag = {spec.tag: spec for spec in replayed_accepted_specs}
            keep_local_tags = {
                tag
                for tag, values in replayed_values_by_tag.items()
                if tag not in global_tags
                and len(set(values)) > 1
                and (
                    (
                        stream_proxy_score(values, replayed_placeholders[tag]) >= args.min_local_stream_score
                        and (
                            not args.heldout_local_stream_gate
                            or stream_proxy_is_stable(
                                values,
                                replayed_placeholders[tag],
                                args.heldout_local_stream_min_score,
                            )
                        )
                    )
                    or (
                        len(values) >= args.numeric_group_rescue_support
                        and spec_by_tag.get(tag) is not None
                        and spec_by_tag[tag].kind == "open_function"
                        and values_are_numeric_like(values)
                    )
                )
            }
            replayed_lines, replayed_values_by_tag, replayed_placeholders, replayed_accepted_specs = replay_with_allowed_tags(keep_local_tags)

        global_tags_for_plan = {spec.tag for spec in global_specs}
        materialized_tags_for_plan = {
            spec.tag
            for spec in replayed_accepted_specs
            if replayed_values_by_tag.get(spec.tag)
        }
        local_families_for_plan: list[dict[str, object]] = []
        for family in families:
            tags = [
                spec.tag
                for spec in local_specs_by_family.get(family.family_id, [])
                if spec.tag in materialized_tags_for_plan and spec.tag not in global_tags_for_plan
            ]
            if tags:
                local_families_for_plan.append({
                    "key": list(family.key),
                    "tags": tags,
                })
        execution_plan_payload = {
            "version": 1,
            "mode": "semantic_global_replay_scope_v1",
            "global_tags": [
                spec.tag
                for spec in global_specs
                if spec.tag in materialized_tags_for_plan
            ],
            "local_families": local_families_for_plan,
            "family_first_replay": bool(args.family_first_replay),
            "global_long_span_first": bool(args.global_long_span_first),
            "validated_program_reindex": bool(args.validated_program_reindex),
            "validated_program_family_cache": bool(args.validated_program_family_cache),
            "global_function_feature_router": bool(args.global_function_feature_router),
        }
        transformed_lines = replayed_lines
        values_by_tag = replayed_values_by_tag
        placeholders = replayed_placeholders
        accepted_specs = replayed_accepted_specs
    elif early_global_specs:
        for spec in early_global_specs:
            placeholders[spec.tag] = dataset_extract.choose_placeholder(original_text, args.dataset, spec.tag)
            accepted_specs.append(spec)
        transformed_lines = [
            apply_specs_to_line(
                line,
                early_global_specs,
                placeholders,
                values_by_tag,
                trusted_fast_path=args.trusted_replay_fast_path,
                skip_placeholder_protection=args.trusted_replay_skip_placeholder_protection,
                skip_exact_value_check=args.trusted_replay_skip_exact_value_check,
            )
            for line in original_lines
        ]

    if args.canonical_global_fields and args.dataset == "Apache":
        (
            transformed_lines,
            values_by_tag,
            placeholders,
            accepted_specs,
            removed_family_specs,
            removed_low_gain_specs,
        ) = rebuild_with_canonical_globals(
            original_lines=original_lines,
            original_text=original_text,
            families=families,
            accepted_specs=accepted_specs,
        )

    if args.global_value_rescue:
        (
            transformed_lines,
            values_by_tag,
            placeholders,
            accepted_specs,
            global_value_rescue_stats,
        ) = apply_global_value_rescue(
            transformed_lines=transformed_lines,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
            exclude_classes=semantic_classes,
            candidate_specs=global_rescue_decision_cache_specs or None,
            trust_cached_decision=bool(global_rescue_decision_cache_specs),
        )
    semantic_stream_feature_gate_stats: dict[str, object] = {"enabled": 0}
    if os.environ.get("PARE_SEMANTIC_STREAM_FEATURE_GATE", "0") == "1":
        gated_text = "".join(transformed_lines)
        (
            gated_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            semantic_stream_feature_gate_stats,
        ) = apply_semantic_stream_feature_gate(
            transformed_text=gated_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        transformed_lines = dataset_extract.split_physical_lines(gated_text)
    mark_stage("global_semantic_replay")

    transformed_text = "".join(transformed_lines)
    if getattr(args, "disable_residual_planning", False):
        residual_planner_stats["enabled"] = 0
        residual_planner_stats["choice"] = "disabled"
        args.residual_family_simple_planner = False
        args.residual_auto_skip_low_templates = 0
        args.residual_operator_mdl_gate = False
        args.residual_dominance_planner = False
        args.residual_mdl_planner = False
        args.residual_numeric_lattice = False
        args.residual_line_transducer = False
        args.residual_variable_stream_mode = "none"
        args.residual_xsignature_streams = False
        args.residual_stream_coalesce = False
        args.relation_streams = False
        args.placeholder_slot_fission = False
    if args.residual_family_simple_planner:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            residual_family_simple_stats,
        ) = apply_residual_family_simple_planner(
            transformed_lines=dataset_extract.split_physical_lines(transformed_text),
            line_families=line_families,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        transformed_lines = dataset_extract.split_physical_lines(transformed_text)
        residual_planner_stats["enabled"] = 1
        residual_planner_stats["choice"] = "family_simple"
        args.residual_dominance_planner = False
        args.residual_mdl_planner = False
        args.residual_numeric_lattice = False
        args.residual_line_transducer = False
        args.residual_variable_stream_mode = "none"
        args.residual_xsignature_streams = False
        args.residual_stream_coalesce = False
        args.relation_streams = False
        args.placeholder_slot_fission = False
        mark_residual_substage("family_simple_planner", residual_step_start)
    if args.residual_auto_skip_low_templates > 0:
        residual_auto_skip_stats["enabled"] = 1
        residual_auto_skip_stats["threshold"] = args.residual_auto_skip_low_templates
        semantic_template_count = len(set(transformed_lines))
        residual_auto_skip_stats["template_count"] = semantic_template_count
        if semantic_template_count <= args.residual_auto_skip_low_templates:
            residual_auto_skip_stats["skipped"] = 1
            args.residual_dominance_planner = False
            args.residual_mdl_planner = False
            args.residual_numeric_lattice = False
            args.residual_line_transducer = False
            args.residual_variable_stream_mode = "none"
            args.residual_xsignature_streams = False
            args.placeholder_slot_fission = False
            args.residual_stream_coalesce = False

    residual_operator_gate_snapshot = None
    if (
        args.residual_operator_mdl_gate
        and (
            args.residual_dominance_planner
            or args.residual_mdl_planner
            or args.residual_numeric_lattice
            or args.residual_line_transducer
            or args.residual_variable_stream_mode != "none"
            or args.residual_xsignature_streams
            or args.residual_stream_coalesce
        )
    ):
        op_start = time.perf_counter()
        residual_operator_gate_snapshot = {
            "text": transformed_text,
            "values": {tag: list(values) for tag, values in values_by_tag.items()},
            "placeholders": dict(placeholders),
            "specs": list(accepted_specs),
            "line_stats": dict(residual_line_stats),
            "numeric_stats": dict(residual_numeric_lattice_stats),
            "variable_stats": dict(residual_variable_stats),
            "xsignature_stats": dict(residual_xsignature_stats),
            "coalesce_stats": dict(residual_stream_coalesce_stats),
            "schema_capture_len": len(getattr(args, "_residual_schema_plan_capture", []) or []),
            "numeric_decisions_len": len(getattr(args, "_numeric_lattice_decisions", []) or []),
        }
        residual_planner_stats["operator_gate_enabled"] = 1
        residual_planner_stats["operator_gate_margin"] = int(args.residual_operator_mdl_gate_min_score)
        residual_planner_stats["operator_gate_baseline_cost"] = int(
            residual_state_proxy_cost(transformed_text, values_by_tag)
        )
        record_operation_timing(
            args,
            "residual_operator_mdl_gate.baseline_cost",
            time.perf_counter() - op_start,
        )

    if args.residual_dominance_planner:
        residual_planner_stats["enabled"] = 1
        probe_text, probe_values, probe_placeholders, probe_specs = (
            transformed_text,
            {tag: list(values) for tag, values in values_by_tag.items()},
            dict(placeholders),
            list(accepted_specs),
        )
        probe_numeric_stats = dict(residual_numeric_lattice_stats)
        if args.residual_numeric_lattice and args.fast_dominance_probe:
            residual_step_start = time.perf_counter()
            probe_numeric_stats = cheap_numeric_dominance_stats(probe_text)
            mark_residual_substage("cheap_dominance_probe", residual_step_start)
            if probe_numeric_stats.get("admitted", 0):
                residual_step_start = time.perf_counter()
                (
                    probe_text,
                    probe_values,
                    probe_placeholders,
                    probe_specs,
                    probe_numeric_stats,
                ) = apply_residual_numeric_lattice(
                    transformed_text=probe_text,
                    values_by_tag=probe_values,
                    placeholders=probe_placeholders,
                    accepted_specs=probe_specs,
                    original_text=original_text,
                    args=args,
                )
                mark_residual_substage("numeric_lattice_first_verified", residual_step_start)
        elif args.residual_numeric_lattice:
            residual_step_start = time.perf_counter()
            (
                probe_text,
                probe_values,
                probe_placeholders,
                probe_specs,
                probe_numeric_stats,
            ) = apply_residual_numeric_lattice(
                transformed_text=probe_text,
                values_by_tag=probe_values,
                placeholders=probe_placeholders,
                accepted_specs=probe_specs,
                original_text=original_text,
                args=args,
            )
            mark_residual_substage("dominance_probe_numeric_lattice", residual_step_start)
        residual_planner_stats["dominance_probe_admitted"] = int(probe_numeric_stats.get("admitted", 0))
        residual_planner_stats["dominance_probe_before_templates"] = int(probe_numeric_stats.get("before_templates", 0))
        residual_planner_stats["dominance_probe_after_templates"] = int(probe_numeric_stats.get("after_templates", 0))
        residual_planner_stats["dominance_probe_density"] = int(probe_numeric_stats.get("occurrences_per_kib", 0))
        before_templates = int(probe_numeric_stats.get("before_templates", 0))
        after_templates = int(probe_numeric_stats.get("after_templates", before_templates))
        numeric_density = int(probe_numeric_stats.get("occurrences_per_kib", 0))
        numeric_dominates = (
            bool(probe_numeric_stats.get("admitted", 0))
            and numeric_density >= 80
            and (
                after_templates <= 512
                or (before_templates > 0 and after_templates * 100 <= before_templates)
            )
        )
        if numeric_dominates:
            residual_planner_stats["choice"] = "numeric_then_variable"
            transformed_text = probe_text
            values_by_tag = probe_values
            placeholders = probe_placeholders
            accepted_specs = probe_specs
            residual_numeric_lattice_stats = probe_numeric_stats
            if args.residual_line_transducer:
                residual_step_start = time.perf_counter()
                (
                    transformed_text,
                    values_by_tag,
                    placeholders,
                    accepted_specs,
                    residual_line_stats,
                ) = apply_residual_line_transducer(
                    transformed_text=transformed_text,
                    values_by_tag=values_by_tag,
                    placeholders=placeholders,
                    accepted_specs=accepted_specs,
                    original_text=original_text,
                    args=args,
                )
                mark_residual_substage("line_transducer_after_numeric", residual_step_start)
            if args.residual_variable_stream_mode != "none":
                residual_step_start = time.perf_counter()
                (
                    transformed_text,
                    values_by_tag,
                    placeholders,
                    accepted_specs,
                    residual_variable_stats,
                ) = apply_residual_variable_streams(
                    transformed_text=transformed_text,
                    values_by_tag=values_by_tag,
                    placeholders=placeholders,
                    accepted_specs=accepted_specs,
                    original_text=original_text,
                    args=args,
                )
                mark_residual_substage("variable_streams_after_numeric", residual_step_start)
        else:
            residual_planner_stats["choice"] = "variable_then_numeric"
            if args.residual_line_transducer:
                residual_step_start = time.perf_counter()
                (
                    transformed_text,
                    values_by_tag,
                    placeholders,
                    accepted_specs,
                    residual_line_stats,
                ) = apply_residual_line_transducer(
                    transformed_text=transformed_text,
                    values_by_tag=values_by_tag,
                    placeholders=placeholders,
                    accepted_specs=accepted_specs,
                    original_text=original_text,
                    args=args,
                )
                mark_residual_substage("line_transducer_before_variable", residual_step_start)
            if args.residual_variable_stream_mode != "none":
                residual_step_start = time.perf_counter()
                (
                    transformed_text,
                    values_by_tag,
                    placeholders,
                    accepted_specs,
                    residual_variable_stats,
                ) = apply_residual_variable_streams(
                    transformed_text=transformed_text,
                    values_by_tag=values_by_tag,
                    placeholders=placeholders,
                    accepted_specs=accepted_specs,
                    original_text=original_text,
                    args=args,
                )
                mark_residual_substage("variable_streams_before_numeric", residual_step_start)
            if args.residual_numeric_lattice:
                residual_step_start = time.perf_counter()
                run_post_variable_numeric = True
                if args.fast_post_variable_numeric_gate:
                    residual_numeric_lattice_stats = cheap_numeric_dominance_stats(transformed_text)
                    residual_numeric_lattice_stats["post_variable_gate_skipped"] = int(
                        not should_run_post_variable_numeric_lattice(residual_numeric_lattice_stats)
                    )
                    mark_residual_substage("post_variable_numeric_gate", residual_step_start)
                    run_post_variable_numeric = should_run_post_variable_numeric_lattice(residual_numeric_lattice_stats)
                    residual_step_start = time.perf_counter()
                if run_post_variable_numeric:
                    (
                        transformed_text,
                        values_by_tag,
                        placeholders,
                        accepted_specs,
                        residual_numeric_lattice_stats,
                    ) = apply_residual_numeric_lattice(
                        transformed_text=transformed_text,
                        values_by_tag=values_by_tag,
                        placeholders=placeholders,
                        accepted_specs=accepted_specs,
                        original_text=original_text,
                        args=args,
                    )
                    mark_residual_substage("numeric_lattice_after_variable", residual_step_start)
        if args.residual_stream_coalesce:
            residual_step_start = time.perf_counter()
            (
                transformed_text,
                values_by_tag,
                placeholders,
                accepted_specs,
                residual_stream_coalesce_stats,
            ) = apply_residual_stream_coalescing(
                transformed_text=transformed_text,
                values_by_tag=values_by_tag,
                placeholders=placeholders,
                accepted_specs=accepted_specs,
                original_text=original_text,
                args=args,
            )
            mark_residual_substage("stream_coalesce", residual_step_start)

    if args.residual_mdl_planner:
        residual_planner_stats["enabled"] = 1

        def clone_residual_state() -> tuple[str, dict[str, list[str]], dict[str, str], list[CandidateSpec]]:
            return (
                transformed_text,
                {tag: list(values) for tag, values in values_by_tag.items()},
                dict(placeholders),
                list(accepted_specs),
            )

        def run_residual_candidate(order: tuple[str, ...]) -> dict[str, object]:
            cand_text, cand_values, cand_placeholders, cand_specs = clone_residual_state()
            cand_numeric_stats = dict(residual_numeric_lattice_stats)
            cand_variable_stats = dict(residual_variable_stats)
            cand_line_stats = dict(residual_line_stats)
            cand_coalesce_stats = dict(residual_stream_coalesce_stats)
            for step in order:
                if step == "numeric" and args.residual_numeric_lattice:
                    (
                        cand_text,
                        cand_values,
                        cand_placeholders,
                        cand_specs,
                        cand_numeric_stats,
                    ) = apply_residual_numeric_lattice(
                        transformed_text=cand_text,
                        values_by_tag=cand_values,
                        placeholders=cand_placeholders,
                        accepted_specs=cand_specs,
                        original_text=original_text,
                        args=args,
                    )
                elif step == "line" and args.residual_line_transducer:
                    (
                        cand_text,
                        cand_values,
                        cand_placeholders,
                        cand_specs,
                        cand_line_stats,
                    ) = apply_residual_line_transducer(
                        transformed_text=cand_text,
                        values_by_tag=cand_values,
                        placeholders=cand_placeholders,
                        accepted_specs=cand_specs,
                        original_text=original_text,
                        args=args,
                    )
                elif step == "variable" and args.residual_variable_stream_mode != "none":
                    (
                        cand_text,
                        cand_values,
                        cand_placeholders,
                        cand_specs,
                        cand_variable_stats,
                    ) = apply_residual_variable_streams(
                        transformed_text=cand_text,
                        values_by_tag=cand_values,
                        placeholders=cand_placeholders,
                        accepted_specs=cand_specs,
                        original_text=original_text,
                        args=args,
                    )
                elif step == "coalesce" and args.residual_stream_coalesce:
                    (
                        cand_text,
                        cand_values,
                        cand_placeholders,
                        cand_specs,
                        cand_coalesce_stats,
                    ) = apply_residual_stream_coalescing(
                        transformed_text=cand_text,
                        values_by_tag=cand_values,
                        placeholders=cand_placeholders,
                        accepted_specs=cand_specs,
                        original_text=original_text,
                        args=args,
                    )
            return {
                "text": cand_text,
                "values": cand_values,
                "placeholders": cand_placeholders,
                "specs": cand_specs,
                "numeric_stats": cand_numeric_stats,
                "variable_stats": cand_variable_stats,
                "line_stats": cand_line_stats,
                "coalesce_stats": cand_coalesce_stats,
                "cost": residual_state_proxy_cost(cand_text, cand_values),
            }

        candidate_orders: list[tuple[str, tuple[str, ...]]] = [
            ("variable_only", ("line", "variable", "coalesce")),
            ("variable_then_numeric", ("line", "variable", "numeric", "coalesce")),
            ("numeric_then_variable", ("numeric", "line", "variable", "coalesce")),
        ]
        residual_candidates = {
            name: run_residual_candidate(order)
            for name, order in candidate_orders
        }
        for name, candidate in residual_candidates.items():
            residual_planner_stats[f"{name}_cost"] = int(candidate["cost"])
        best_name, best_candidate = min(
            residual_candidates.items(),
            key=lambda item: (int(item[1]["cost"]), item[0]),
        )
        residual_planner_stats["choice"] = best_name
        transformed_text = str(best_candidate["text"])
        values_by_tag = best_candidate["values"]  # type: ignore[assignment]
        placeholders = best_candidate["placeholders"]  # type: ignore[assignment]
        accepted_specs = best_candidate["specs"]  # type: ignore[assignment]
        residual_numeric_lattice_stats = best_candidate["numeric_stats"]  # type: ignore[assignment]
        residual_variable_stats = best_candidate["variable_stats"]  # type: ignore[assignment]
        residual_line_stats = best_candidate["line_stats"]  # type: ignore[assignment]
        residual_stream_coalesce_stats = best_candidate["coalesce_stats"]  # type: ignore[assignment]

    if not args.residual_mdl_planner and not args.residual_dominance_planner and args.residual_numeric_lattice and not args.residual_numeric_lattice_after_variable:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            residual_numeric_lattice_stats,
        ) = apply_residual_numeric_lattice(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("numeric_lattice_before_variable", residual_step_start)
    if not args.residual_mdl_planner and not args.residual_dominance_planner and args.residual_line_transducer:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            residual_line_stats,
        ) = apply_residual_line_transducer(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("line_transducer", residual_step_start)
    if not args.residual_mdl_planner and not args.residual_dominance_planner and args.residual_variable_stream_mode != "none":
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            residual_variable_stats,
        ) = apply_residual_variable_streams(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("variable_streams", residual_step_start)
    if not args.residual_mdl_planner and not args.residual_dominance_planner and args.residual_numeric_lattice and args.residual_numeric_lattice_after_variable:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            residual_numeric_lattice_stats,
        ) = apply_residual_numeric_lattice(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("numeric_lattice_after_variable", residual_step_start)
    if not args.residual_mdl_planner and not args.residual_dominance_planner and args.residual_stream_coalesce:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            residual_stream_coalesce_stats,
        ) = apply_residual_stream_coalescing(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("stream_coalesce", residual_step_start)

    if args.residual_xsignature_streams:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            residual_xsignature_stats,
        ) = apply_residual_xsignature_streams(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("xsignature_streams", residual_step_start)

    if residual_operator_gate_snapshot is not None:
        op_start = time.perf_counter()
        candidate_cost = int(residual_state_proxy_cost(transformed_text, values_by_tag))
        baseline_cost = int(residual_planner_stats.get("operator_gate_baseline_cost", 0))
        score = baseline_cost - candidate_cost
        margin = int(args.residual_operator_mdl_gate_min_score)
        residual_planner_stats["operator_gate_candidate_cost"] = candidate_cost
        residual_planner_stats["operator_gate_score"] = int(score)
        if score <= margin:
            transformed_text = str(residual_operator_gate_snapshot["text"])
            values_by_tag = {
                str(tag): list(values)
                for tag, values in residual_operator_gate_snapshot["values"].items()
            }
            placeholders = dict(residual_operator_gate_snapshot["placeholders"])
            accepted_specs = list(residual_operator_gate_snapshot["specs"])
            residual_line_stats = dict(residual_operator_gate_snapshot["line_stats"])
            residual_numeric_lattice_stats = dict(residual_operator_gate_snapshot["numeric_stats"])
            residual_variable_stats = dict(residual_operator_gate_snapshot["variable_stats"])
            residual_xsignature_stats = dict(residual_operator_gate_snapshot["xsignature_stats"])
            residual_stream_coalesce_stats = dict(residual_operator_gate_snapshot["coalesce_stats"])
            capture = getattr(args, "_residual_schema_plan_capture", None)
            if isinstance(capture, list):
                del capture[int(residual_operator_gate_snapshot["schema_capture_len"]):]
            numeric_decisions = getattr(args, "_numeric_lattice_decisions", None)
            if isinstance(numeric_decisions, list):
                del numeric_decisions[int(residual_operator_gate_snapshot["numeric_decisions_len"]):]
            residual_planner_stats["operator_gate_rejected"] = 1
            residual_planner_stats["operator_gate_admitted"] = 0
            if residual_planner_stats.get("choice"):
                residual_planner_stats["choice"] = str(residual_planner_stats["choice"]) + "+gate_reject"
            else:
                residual_planner_stats["choice"] = "semantic_only"
        else:
            residual_planner_stats["operator_gate_admitted"] = 1
            residual_planner_stats["operator_gate_rejected"] = 0
        record_operation_timing(
            args,
            "residual_operator_mdl_gate.candidate_cost",
            time.perf_counter() - op_start,
        )
    if args.relation_streams:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            relation_streams,
        ) = apply_relation_streams(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("relation_streams", residual_step_start)
    if args.placeholder_slot_fission:
        residual_step_start = time.perf_counter()
        (
            transformed_text,
            values_by_tag,
            placeholders,
            accepted_specs,
            placeholder_slot_fission_stats,
        ) = apply_placeholder_slot_fission(
            transformed_text=transformed_text,
            values_by_tag=values_by_tag,
            placeholders=placeholders,
            accepted_specs=accepted_specs,
            original_text=original_text,
            args=args,
        )
        mark_residual_substage("placeholder_slot_fission", residual_step_start)
    direct_context_projector_stats = materialize_direct_context_projectors(
        original_text,
        transformed_text,
        values_by_tag,
        placeholders,
        accepted_specs,
    )
    cross_function_context_stats = materialize_cross_function_contexts(
        transformed_text,
        values_by_tag,
        placeholders,
        accepted_specs,
    )
    mark_stage("residual_planning")
    templates: list[str] = []
    ids: list[int] = []
    id_metadata: dict[str, Any] = {}
    line_dict_metadata: dict[str, Any] | None = None
    if args.main_core == "rank":
        op_start = time.perf_counter()
        main_core_metadata = write_rank_core(output_root, transformed_text, args.rank_numeric_radius)
        record_operation_timing(args, "main_core.write_rank_core", time.perf_counter() - op_start)
    elif args.main_core == "line_rank":
        op_start = time.perf_counter()
        main_text, line_dict_metadata, line_dict_templates, line_dict_side_ids = dataset_extract.build_line_dictionary_text(
            transformed_text,
            min_support=args.line_dict_min_support,
            max_entries=args.line_dict_max_entries,
            id_mode=args.line_dict_id_mode,
        )
        record_operation_timing(args, "main_core.build_line_dictionary_text", time.perf_counter() - op_start)
        op_start = time.perf_counter()
        main_core_metadata = write_rank_core(output_root, main_text, args.rank_numeric_radius)
        record_operation_timing(args, "main_core.write_line_rank_core", time.perf_counter() - op_start)
        main_core_metadata["kind"] = "line_rank"
        if line_dict_metadata is not None:
            op_start = time.perf_counter()
            dataset_extract.save_line_dictionary(output_root, line_dict_templates, line_dict_side_ids)
            record_operation_timing(args, "main_core.save_line_dictionary", time.perf_counter() - op_start)
    else:
        op_start = time.perf_counter()
        templates, ids = build_id_mapping(transformed_text)
        record_operation_timing(args, "main_core.build_id_mapping", time.perf_counter() - op_start)
        op_start = time.perf_counter()
        id_metadata = write_id_mapping(output_root, templates, ids)
        record_operation_timing(args, "main_core.write_id_mapping", time.perf_counter() - op_start)
        main_core_metadata = {"kind": "id_mapping"}
    mark_stage("main_core_build")
    specs_meta: list[dict[str, object]] = []
    serialized_tags: set[str] = set()
    for spec in accepted_specs:
        if not values_by_tag.get(spec.tag) or spec.tag in serialized_tags:
            continue
        serialized_tags.add(spec.tag)
        specs_meta.append(
            {
                "tag": spec.tag,
                "kind": spec.kind,
                "placeholder": placeholders[spec.tag],
                "pattern": spec.pattern,
                "store_group": spec.store_group,
                "replacement": spec.replacement,
                "context_tag": json.dumps(spec.program, sort_keys=True)
                if spec.kind in {"open_function", "open_context_delta", "open_context_dict", "relation_pair_delta", "routed_split"}
                else "",
                "semantic_class": spec.semantic_class,
                "value_type": spec.value_type,
                "context_group": spec.context_group,
                "context_policy": spec.context_policy,
                "context_ref": spec.context_ref,
            }
        )
    residual_schema_capture = getattr(args, "_residual_schema_plan_capture", [])
    residual_schema_payload = (
        {"passes": residual_schema_capture}
        if isinstance(residual_schema_capture, list) and residual_schema_capture
        else None
    )
    write_replay_plan(
        args.replay_plan_out,
        args.dataset,
        [spec for spec in accepted_specs if values_by_tag.get(spec.tag)],
        placeholders,
        getattr(args, "_placeholder_slot_fission_decisions", []),
        getattr(args, "_numeric_lattice_decisions", []),
        residual_schema_payload,
        execution_plan_payload,
    )
    extract_metadata: dict[str, Any] = {
        "dataset": args.dataset,
        "profile": "open_function_v1",
        "compact_stream_metadata": not args.keep_stream_patterns,
        "post_merge_hex_streams": args.post_merge_hex_streams,
        "specs": specs_meta,
        "stream_template_llm": {
            "version_id": version_id(args),
            "families": len(families),
            "llm_calls": llm_calls,
            "accepted_functions": len(specs_meta),
            "similarity_threshold": args.similarity_threshold,
            "llm_min_support": args.llm_min_support,
            "offline_family_topk_query": args.offline_family_topk_query,
            "offline_family_topk": args.offline_family_topk,
            "offline_family_topk_min_support": args.offline_family_topk_min_support,
            "offline_family_batch_query": args.offline_family_batch_query,
            "offline_family_batch_max_examples": args.offline_family_batch_max_examples,
            "offline_family_topk_stats": getattr(args, "_offline_family_topk_stats", {}),
            "family_header_program_prompt": args.family_header_program_prompt,
            "semantic_numeric_only_program_prompt": args.semantic_numeric_only_program_prompt,
            "semantic_three_class_program_prompt": args.semantic_three_class_program_prompt,
            "require_regex_structure_anchor": args.require_regex_structure_anchor,
            "regex_structure_anchor_stats": getattr(args, "_regex_structure_anchor_stats", {}),
            "strict_three_class_context_schema": args.strict_three_class_context_schema,
            "direct_context_projector_stats": direct_context_projector_stats,
            "cross_function_context_stats": cross_function_context_stats,
            "verifier_repair_agent": getattr(args, "verifier_repair_agent", False),
            "verifier_repair_stats": getattr(args, "_verifier_repair_stats", {}),
            "canonical_global_fields": args.canonical_global_fields,
            "removed_family_specs": removed_family_specs,
            "removed_low_gain_specs": removed_low_gain_specs,
            "constant_pruned_local_streams": constant_pruned_local_streams,
            "relation_streams": relation_streams,
            "global_value_rescue": args.global_value_rescue,
            "early_global_value_rescue_stats": early_global_value_rescue_stats,
            "global_value_rescue_stats": global_value_rescue_stats,
            "semantic_stream_feature_gate_stats": semantic_stream_feature_gate_stats,
            "residual_numeric_lattice": args.residual_numeric_lattice,
            "numeric_lattice_fast_select": args.numeric_lattice_fast_select,
            "residual_numeric_lattice_stats": residual_numeric_lattice_stats,
            "residual_mdl_planner": args.residual_mdl_planner,
            "residual_dominance_planner": args.residual_dominance_planner,
            "fast_dominance_probe": args.fast_dominance_probe,
            "fast_post_variable_numeric_gate": args.fast_post_variable_numeric_gate,
            "residual_operator_mdl_gate": args.residual_operator_mdl_gate,
            "residual_operator_mdl_gate_min_score": args.residual_operator_mdl_gate_min_score,
            "residual_planner_stats": residual_planner_stats,
            "residual_auto_skip_stats": residual_auto_skip_stats,
            "residual_line_transducer": args.residual_line_transducer,
            "residual_line_stats": residual_line_stats,
            "residual_variable_stream_mode": args.residual_variable_stream_mode,
            "residual_variable_sample_score_lines": args.residual_variable_sample_score_lines,
            "residual_variable_feature_mdl": args.residual_variable_feature_mdl,
            "residual_variable_admit_eligible": args.residual_variable_admit_eligible,
            "residual_variable_shape_gate": args.residual_variable_shape_gate,
            "residual_variable_stats": residual_variable_stats,
            "residual_xsignature_streams": args.residual_xsignature_streams,
            "residual_xsignature_use_signature_placeholder": args.residual_xsignature_use_signature_placeholder,
            "residual_xsignature_stats": residual_xsignature_stats,
            "residual_family_simple_planner": args.residual_family_simple_planner,
            "residual_family_simple_min_support": args.residual_family_simple_min_support,
            "residual_family_simple_numeric_policy": args.residual_family_simple_numeric_policy,
            "residual_family_simple_min_numeric_width": args.residual_family_simple_min_numeric_width,
            "residual_family_simple_max_numeric_width": args.residual_family_simple_max_numeric_width,
            "residual_family_simple_mixed_policy": args.residual_family_simple_mixed_policy,
            "residual_family_simple_mixed_min_token_length": args.residual_family_simple_mixed_min_token_length,
            "residual_family_simple_mixed_context": args.residual_family_simple_mixed_context,
            "residual_family_simple_placeholder_context": args.residual_family_simple_placeholder_context,
            "residual_family_simple_context_side": args.residual_family_simple_context_side,
            "residual_family_simple_stats": residual_family_simple_stats,
            "residual_schema_plan_in": bool(args.residual_schema_plan_in),
            "residual_schema_plan_out": bool(args.residual_schema_plan_out),
            "replay_plan_cache": bool(args.replay_plan_cache),
            "replay_plan_out": bool(args.replay_plan_out),
            "replay_plan_cache_stats": replay_plan_cache_stats,
            "semantic_replay_plan_cache": bool(args.semantic_replay_plan_cache),
            "semantic_replay_plan_cache_stats": semantic_replay_plan_cache_stats,
            "global_rescue_decision_cache": bool(args.global_rescue_decision_cache),
            "global_rescue_decision_cache_stats": global_rescue_decision_cache_stats,
            "residual_stream_coalesce": args.residual_stream_coalesce,
            "residual_stream_coalesce_stats": residual_stream_coalesce_stats,
            "placeholder_slot_fission": args.placeholder_slot_fission,
            "placeholder_slot_fission_target": args.placeholder_slot_fission_target,
            "placeholder_slot_fission_mode": args.placeholder_slot_fission_mode,
        "placeholder_slot_fission_stats": placeholder_slot_fission_stats,
        "placeholder_slot_fission_decision_count": len(getattr(args, "_placeholder_slot_fission_decisions", []) or []),
            "post_merge_hex_streams": args.post_merge_hex_streams,
            "semantic_stream_registry": args.semantic_stream_registry,
            "semantic_registry_admission_router": args.semantic_registry_admission_router,
            "semantic_registry_admission_min_score": args.semantic_registry_admission_min_score,
            "semantic_registry_admission_stats": getattr(args, "_semantic_registry_admission_stats", {}),
            "semantic_global_replay": args.semantic_global_replay,
            "family_first_replay": args.family_first_replay,
            "global_long_span_first": args.global_long_span_first,
            "global_function_feature_router": args.global_function_feature_router,
            "global_function_feature_router_max_functions": args.global_function_feature_router_max_functions,
            "global_function_feature_router_min_score": args.global_function_feature_router_min_score,
            "global_function_feature_router_fallback_full": args.global_function_feature_router_fallback_full,
            "global_function_feature_router_stats": getattr(args, "_global_function_feature_router_stats", {}),
            "no_level_global_replay": args.no_level_global_replay,
            "no_level_streams": args.no_level_streams,
            "prune_constant_local_streams": args.prune_constant_local_streams,
            "min_local_stream_score": args.min_local_stream_score,
            "heldout_local_stream_gate": args.heldout_local_stream_gate,
            "heldout_local_stream_min_score": args.heldout_local_stream_min_score,
            "semantic_registry_entries": len(semantic_registry),
            "validated_program_reindex": args.validated_program_reindex,
            "program_reindex_similarity_threshold": args.program_reindex_similarity_threshold,
            "program_reindex_max_candidates_per_line": args.program_reindex_max_candidates_per_line,
            "program_reindex_template_cache": args.program_reindex_template_cache,
            "validated_program_family_cache": args.validated_program_family_cache,
            "program_reindex_family_verify_lines": args.program_reindex_family_verify_lines,
            "feature_codec_prefilter": args.feature_codec_prefilter,
            "stage_seconds": {key: round(value, 6) for key, value in stage_seconds.items()},
            "residual_stage_seconds": {key: round(value, 6) for key, value in residual_stage_seconds.items()},
            "operation_profile": args.operation_profile,
            "operation_timings": operation_timings_info(args, len(original_text.encode("latin-1"))),
            "dataset_extract_operation_timings": dataset_extract.operation_timings_info(len(original_text.encode("latin-1"))),
            "regex_compile_cache": compiled_multiline_regex.cache_info()._asdict(),
            "open_function_cache": dataset_extract.open_function_cache_info(),
        },
    }
    if not args.operation_profile:
        # These diagnostics are useful in profile runs but do not participate in
        # decoding.  Keeping them out of normal archives trims metadata bytes and
        # avoids serializing large cache counters on the hot path.
        stream_template_meta = extract_metadata["stream_template_llm"]
        if isinstance(stream_template_meta, dict):
            stream_template_meta.pop("operation_timings", None)
            stream_template_meta.pop("dataset_extract_operation_timings", None)
            stream_template_meta.pop("regex_compile_cache", None)
            stream_template_meta.pop("open_function_cache", None)
    if args.residual_schema_plan_out:
        plan_capture = getattr(args, "_residual_schema_plan_capture", [])
        if isinstance(plan_capture, list):
            plan_path = Path(args.residual_schema_plan_out)
            plan_path.parent.mkdir(parents=True, exist_ok=True)
            plan_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "dataset": args.dataset,
                        "source_block": input_path.name,
                        "passes": plan_capture,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
    op_start = time.perf_counter()
    dataset_extract.save_extract_streams(output_root, extract_metadata, values_by_tag, transformed_text)
    record_operation_timing(args, "stream_save.save_extract_streams_total", time.perf_counter() - op_start)
    mark_stage("stream_save")
    metadata = {
        "version_id": version_id(args),
        "dataset": args.dataset,
        "input_file": input_path.name,
        "original_size": input_path.stat().st_size,
        "transformed_size": len(transformed_text.encode("latin-1")),
        "main_core": main_core_metadata,
        "id_mapping": id_metadata,
        "dataset_extract": extract_metadata,
    }
    if line_dict_metadata is not None:
        metadata["line_dictionary"] = line_dict_metadata
    compact_decoder_stats: dict[str, int] = {"removed_stream_keys": 0, "removed_root_keys": 0}
    archive_metadata = metadata
    if args.compact_decoder_contract:
        (Path(args.output_dir) / "metadata.audit.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        archive_metadata, compact_decoder_stats = compact_decoder_metadata(metadata)
        (output_root / "metadata.json").write_text(
            json.dumps(archive_metadata, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
    else:
        (output_root / "metadata.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    mark_stage("metadata_write")
    archive_start = time.perf_counter()
    archive_bytes = create_archive(output_root, archive_path, compact_container=args.compact_archive_container)
    stage_seconds["archive_create"] = stage_seconds.get("archive_create", 0.0) + (time.perf_counter() - archive_start)
    record_operation_timing(args, "archive.create_tar_xz", stage_seconds.get("archive_create", 0.0))
    stage_checkpoint = time.perf_counter()
    encode_seconds = time.perf_counter() - start

    original_bytes = original_text.encode("latin-1")
    decoded_bytes = b""
    decode_seconds = 0.0
    internal_full_sha = "SKIPPED"
    if not args.skip_internal_decode_verify:
        decode_start = time.perf_counter()
        archived_root = extract_archive(archive_path, Path(args.output_dir) / "restore")
        decoded_text = decode_archive_root(archived_root)
        decode_seconds = time.perf_counter() - decode_start
        record_operation_timing(args, "decode_verify.extract_and_decode_archive", decode_seconds)
        decoded_bytes = decoded_text.encode("latin-1")
        exact = decoded_bytes == original_bytes
        if not exact:
            raise ValueError("Strict restore failed")
        internal_full_sha = "PASS" if sha256_bytes(decoded_bytes) == sha256_bytes(original_bytes) else "FAIL"
    stage_seconds["decode_verify"] = decode_seconds

    family_report = [
        {
            "family_id": family.family_id,
            "support": len(family.line_indexes),
            "queried": family.queried,
            "accepted_functions": len(family.active_specs),
            "online_router_score": round(family.online_router_score, 6),
            "next_llm_probe_support": family.next_llm_probe_support,
            "rejected_reason": family.rejected_reason,
            "key": " ".join(family.key[:40]),
            "example": family.examples[0].rstrip("\r\n") if family.examples else "",
        }
        for family in sorted(families, key=lambda fam: len(fam.line_indexes), reverse=True)[: args.report_families]
    ]
    (Path(args.output_dir) / "family_report.json").write_text(
        json.dumps(family_report, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    function_compile_trace = getattr(args, "_function_compile_trace", [])
    if isinstance(function_compile_trace, list):
        (Path(args.output_dir) / "function_compile_trace.json").write_text(
            json.dumps(function_compile_trace, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    result = {
        "version_id": version_id(args),
        "dataset": args.dataset,
        "raw_bytes": len(original_bytes),
        "archive_bytes": archive_bytes,
        "compression_ratio": len(original_bytes) / max(1, archive_bytes),
        "families": len(families),
        "llm_calls": llm_calls,
        "llm_runtime_stats": getattr(args, "_llm_runtime_stats", {
            "proposal_events": 0,
            "cache_hits": 0,
            "family_fallback_hits": 0,
            "api_calls": 0,
            "api_seconds": 0.0,
        }),
        "offline_family_topk_query": args.offline_family_topk_query,
        "offline_family_topk": args.offline_family_topk,
        "offline_family_topk_min_support": args.offline_family_topk_min_support,
        "offline_family_batch_query": args.offline_family_batch_query,
        "offline_family_batch_max_examples": args.offline_family_batch_max_examples,
        "offline_family_topk_stats": getattr(args, "_offline_family_topk_stats", {}),
        "slot_complete_planner": args.slot_complete_planner,
        "generic_token_shapes": args.generic_token_shapes,
        "function_first_prompt": args.function_first_prompt,
        "free_form_program_prompt": args.free_form_program_prompt,
        "free_form_transducer_sketch_prompt": args.free_form_transducer_sketch_prompt,
        "purpose_only_program_prompt": args.purpose_only_program_prompt,
        "semantic_focus_program_prompt": args.semantic_focus_program_prompt,
        "semantic_rich_examples_prompt": args.semantic_rich_examples_prompt,
        "semantic_numeric_only_program_prompt": args.semantic_numeric_only_program_prompt,
        "semantic_three_class_program_prompt": args.semantic_three_class_program_prompt,
        "require_regex_structure_anchor": args.require_regex_structure_anchor,
        "regex_structure_anchor_stats": getattr(args, "_regex_structure_anchor_stats", {}),
        "strict_three_class_context_schema": args.strict_three_class_context_schema,
        "direct_context_projector_stats": direct_context_projector_stats,
        "cross_function_context_stats": cross_function_context_stats,
        "verifier_repair_agent": getattr(args, "verifier_repair_agent", False),
        "verifier_repair_stats": getattr(args, "_verifier_repair_stats", {}),
        "contextualize_broad_llm_regex": args.contextualize_broad_llm_regex,
        "contextualize_broad_regex_stats": getattr(args, "_contextualize_broad_regex_stats", {
            "families": 0,
            "source_functions": 0,
            "variant_functions": 0,
            "internal_capture_variants": 0,
        }),
        "slot_complete_stats": getattr(args, "_slot_complete_stats", {
            "payloads": 0,
            "payloads_with_slot_actions": 0,
            "slot_actions": 0,
            "required_slot_occurrences": 0,
        }),
        "program_bundle_mdl_admission": args.program_bundle_mdl_admission,
        "program_bundle_mdl_stats": getattr(args, "_program_bundle_stats", {
            "families": 0,
            "full_accepts": 0,
            "greedy_accepts": 0,
            "rejects": 0,
            "candidate_specs": 0,
            "accepted_specs": 0,
            "full_gain_sum": 0,
        }),
        "accepted_functions": len(specs_meta),
        "removed_family_specs": removed_family_specs,
        "removed_low_gain_specs": removed_low_gain_specs,
        "constant_pruned_local_streams": constant_pruned_local_streams,
        "relation_streams": relation_streams,
        "replay_plan_cache": bool(args.replay_plan_cache),
        "replay_plan_cache_hit": replay_plan_cache_stats["hit"],
        "replay_plan_cache_specs": replay_plan_cache_stats["specs"],
        "replay_plan_cache_values": replay_plan_cache_stats["values"],
        "semantic_replay_plan_cache": bool(args.semantic_replay_plan_cache),
        "semantic_replay_plan_cache_hit": semantic_replay_plan_cache_stats["hit"],
        "semantic_replay_plan_cache_specs": semantic_replay_plan_cache_stats["specs"],
        "semantic_replay_plan_cache_values": semantic_replay_plan_cache_stats["values"],
        "global_rescue_decision_cache": bool(args.global_rescue_decision_cache),
        "global_rescue_decision_cache_specs": global_rescue_decision_cache_stats["specs"],
        "global_value_rescue": args.global_value_rescue,
        "early_global_value_rescue_candidate_count": early_global_value_rescue_stats["candidate_count"],
        "early_global_value_rescue_admitted": early_global_value_rescue_stats["admitted"],
        "early_global_value_rescue_skipped_by_class": early_global_value_rescue_stats.get("skipped_by_class", 0),
        "early_global_value_rescue_shadowed": early_global_value_rescue_stats.get("shadowed_by_semantic", 0),
        "global_value_rescue_candidate_count": global_value_rescue_stats["candidate_count"],
        "global_value_rescue_admitted": global_value_rescue_stats["admitted"],
        "global_value_rescue_skipped_by_class": global_value_rescue_stats.get("skipped_by_class", 0),
        "residual_numeric_lattice": args.residual_numeric_lattice,
        "numeric_lattice_fast_select": args.numeric_lattice_fast_select,
        "residual_numeric_lattice_occurrences": residual_numeric_lattice_stats.get("occurrences", 0),
        "residual_numeric_lattice_occurrences_per_kib": residual_numeric_lattice_stats.get("occurrences_per_kib", 0),
        "residual_numeric_lattice_before_templates": residual_numeric_lattice_stats.get("before_templates", 0),
        "residual_numeric_lattice_after_templates": residual_numeric_lattice_stats.get("after_templates", 0),
        "residual_numeric_lattice_groups": residual_numeric_lattice_stats.get("groups", 0),
        "residual_numeric_lattice_admitted": residual_numeric_lattice_stats.get("admitted", 0),
        "residual_numeric_lattice_values": residual_numeric_lattice_stats.get("values", 0),
        "residual_numeric_lattice_score": residual_numeric_lattice_stats.get("score", 0),
        "residual_numeric_lattice_side_cost": residual_numeric_lattice_stats.get("side_cost", 0),
        "residual_numeric_lattice_admission_margin": residual_numeric_lattice_stats.get("admission_margin", 0),
        "residual_numeric_lattice_cache_hit": residual_numeric_lattice_stats.get("cache_hit", 0),
        "numeric_lattice_fast_select": args.numeric_lattice_fast_select,
        "numeric_lattice_fast_select_fallback": residual_numeric_lattice_stats.get("fast_select_fallback", 0),
        "numeric_lattice_fast_select_reject_reason": residual_numeric_lattice_stats.get("fast_select_reject_reason", ""),
        "numeric_lattice_proxy_preset": residual_numeric_lattice_stats.get("proxy_preset", -1),
        "numeric_lattice_proxy_policy_before_templates": residual_numeric_lattice_stats.get("proxy_policy_before_templates", 0),
        "numeric_lattice_proxy_policy_line_count": residual_numeric_lattice_stats.get("proxy_policy_line_count", 0),
        "residual_mdl_planner": args.residual_mdl_planner,
        "residual_dominance_planner": args.residual_dominance_planner,
        "fast_dominance_probe": args.fast_dominance_probe,
        "fast_post_variable_numeric_gate": args.fast_post_variable_numeric_gate,
        "residual_operator_mdl_gate": args.residual_operator_mdl_gate,
        "residual_operator_gate_admitted": residual_planner_stats.get("operator_gate_admitted", 0),
        "residual_operator_gate_rejected": residual_planner_stats.get("operator_gate_rejected", 0),
        "residual_operator_gate_baseline_cost": residual_planner_stats.get("operator_gate_baseline_cost", 0),
        "residual_operator_gate_candidate_cost": residual_planner_stats.get("operator_gate_candidate_cost", 0),
        "residual_operator_gate_score": residual_planner_stats.get("operator_gate_score", 0),
        "residual_mdl_planner_choice": residual_planner_stats.get("choice", ""),
        "residual_dominance_probe_admitted": residual_planner_stats.get("dominance_probe_admitted", 0),
        "residual_dominance_probe_before_templates": residual_planner_stats.get("dominance_probe_before_templates", 0),
        "residual_dominance_probe_after_templates": residual_planner_stats.get("dominance_probe_after_templates", 0),
        "residual_dominance_probe_density": residual_planner_stats.get("dominance_probe_density", 0),
        "residual_mdl_planner_numeric_then_variable_cost": residual_planner_stats.get("numeric_then_variable_cost", 0),
        "residual_mdl_planner_variable_then_numeric_cost": residual_planner_stats.get("variable_then_numeric_cost", 0),
        "residual_mdl_planner_variable_only_cost": residual_planner_stats.get("variable_only_cost", 0),
        "residual_line_transducer": args.residual_line_transducer,
        "residual_line_candidate_slots": residual_line_stats["candidate_slots"],
        "residual_line_candidate_count": residual_line_stats["candidate_count"],
        "residual_line_admitted_slots": residual_line_stats["admitted_slots"],
        "residual_variable_stream_mode": args.residual_variable_stream_mode,
        "residual_variable_sample_score_lines": args.residual_variable_sample_score_lines,
        "residual_variable_feature_mdl": args.residual_variable_feature_mdl,
        "residual_variable_admit_eligible": args.residual_variable_admit_eligible,
        "residual_variable_template_collapse_prefilter": args.residual_variable_template_collapse_prefilter,
        "residual_variable_shape_max_candidate_groups": args.residual_variable_shape_max_candidate_groups,
        "residual_variable_candidate_groups": residual_variable_stats["candidate_groups"],
        "residual_variable_admitted_groups": residual_variable_stats["admitted_groups"],
        "residual_variable_values": residual_variable_stats["values"],
        "residual_variable_shape_gate": args.residual_variable_shape_gate,
        "residual_variable_shape_gate_run_shape": residual_variable_stats.get("shape_gate_run_shape", 0),
        "residual_variable_shape_gate_before_templates": residual_variable_stats.get("shape_gate_before_templates", 0),
        "residual_variable_shape_gate_after_templates": residual_variable_stats.get("shape_gate_after_templates", 0),
        "residual_variable_shape_gate_collapse": residual_variable_stats.get("shape_gate_collapse", 0),
        "residual_variable_shape_gate_eligible_groups": residual_variable_stats.get("shape_gate_eligible_groups", 0),
        "residual_variable_shape_gate_eligible_values": residual_variable_stats.get("shape_gate_eligible_values", 0),
        "residual_variable_shape_gate_raw_main": residual_variable_stats.get("shape_gate_raw_main", 0),
        "residual_variable_shape_pregate": args.residual_variable_shape_pregate,
        "residual_variable_shape_pregate_run_full_gate": residual_variable_stats.get("shape_pregate_run_full_gate", 0),
        "residual_variable_shape_pregate_before_templates": residual_variable_stats.get("shape_pregate_before_templates", 0),
        "residual_variable_shape_pregate_after_templates": residual_variable_stats.get("shape_pregate_after_templates", 0),
        "residual_variable_shape_pregate_collapse": residual_variable_stats.get("shape_pregate_collapse", 0),
        "residual_xsignature_streams": args.residual_xsignature_streams,
        "residual_xsignature_use_signature_placeholder": args.residual_xsignature_use_signature_placeholder,
        "residual_xsignature_occurrences": residual_xsignature_stats.get("occurrences", 0),
        "residual_xsignature_candidate_groups": residual_xsignature_stats.get("candidate_groups", 0),
        "residual_xsignature_admitted_groups": residual_xsignature_stats.get("admitted_groups", 0),
        "residual_xsignature_values": residual_xsignature_stats.get("values", 0),
        "residual_xsignature_skipped_placeholder_tokens": residual_xsignature_stats.get("skipped_placeholder_tokens", 0),
        "residual_stream_coalesce": args.residual_stream_coalesce,
        "residual_stream_coalesce_candidate_tags": residual_stream_coalesce_stats["candidate_tags"],
        "residual_stream_coalesce_admitted": residual_stream_coalesce_stats["admitted"],
        "residual_stream_coalesce_values": residual_stream_coalesce_stats["values"],
        "residual_stream_coalesce_score": residual_stream_coalesce_stats["score"],
        "placeholder_slot_fission": args.placeholder_slot_fission,
        "placeholder_slot_fission_candidate_tags": placeholder_slot_fission_stats["candidate_tags"],
        "placeholder_slot_fission_accepted_tags": placeholder_slot_fission_stats["accepted_tags"],
        "placeholder_slot_fission_split_streams": placeholder_slot_fission_stats["split_streams"],
        "placeholder_slot_fission_literalized_slots": placeholder_slot_fission_stats["literalized_slots"],
        "placeholder_slot_fission_values_moved": placeholder_slot_fission_stats["values_moved"],
        "placeholder_slot_fission_score": placeholder_slot_fission_stats["score"],
        "placeholder_slot_fission_cache_hit": placeholder_slot_fission_stats.get("cache_hit", 0),
        "placeholder_slot_fission_cache_reject_reason": placeholder_slot_fission_stats.get("cache_reject_reason", ""),
        "post_merge_hex_streams": args.post_merge_hex_streams,
        "post_merge_hex_stream_stats": extract_metadata.get("post_merge_hex_stream_stats", {}),
        "compact_decoder_contract": args.compact_decoder_contract,
        "compact_archive_container": args.compact_archive_container,
        "compact_decoder_contract_stats": compact_decoder_stats,
        "semantic_stream_registry": args.semantic_stream_registry,
        "semantic_global_replay": args.semantic_global_replay,
        "family_first_replay": args.family_first_replay,
        "global_long_span_first": args.global_long_span_first,
        "no_level_global_replay": args.no_level_global_replay,
        "no_level_streams": args.no_level_streams,
        "prune_constant_local_streams": args.prune_constant_local_streams,
        "fast_constant_local_preprune": args.fast_constant_local_preprune,
        "min_local_stream_score": args.min_local_stream_score,
        "heldout_local_stream_gate": args.heldout_local_stream_gate,
        "heldout_local_stream_min_score": args.heldout_local_stream_min_score,
        "semantic_registry_entries": len(semantic_registry),
        "validated_program_reindex": args.validated_program_reindex,
        "program_reindex_similarity_threshold": args.program_reindex_similarity_threshold,
        "program_reindex_max_candidates_per_line": args.program_reindex_max_candidates_per_line,
        "validated_program_family_cache_auto": args.validated_program_family_cache_auto,
        "validated_program_family_cache_auto_min_families": args.validated_program_family_cache_auto_min_families,
        "validated_program_family_cache": args.validated_program_family_cache,
        "program_reindex_family_verify_lines": args.program_reindex_family_verify_lines,
        "exact_family_match_only": args.exact_family_match_only,
        "fuzzy_family_line_budget": args.fuzzy_family_line_budget,
        "family_match_rare_anchor_limit": args.family_match_rare_anchor_limit,
        "family_match_rare_anchor_auto": args.family_match_rare_anchor_auto,
        "family_match_rare_anchor_effective_limit": effective_family_match_rare_anchor_limit,
        "family_match_rare_anchor_auto_stats": family_match_rare_anchor_auto_stats,
        "feature_codec_prefilter": args.feature_codec_prefilter,
        "stage_seconds": {key: round(value, 6) for key, value in stage_seconds.items()},
        "residual_stage_seconds": {key: round(value, 6) for key, value in residual_stage_seconds.items()},
        "operation_profile": args.operation_profile,
        "operation_timings": operation_timings_info(args, len(original_bytes)),
        "dataset_extract_operation_timings": dataset_extract.operation_timings_info(len(original_bytes)),
        "stage_family_scan_compile_seconds": stage_seconds.get("family_scan_compile", 0.0),
        "stage_global_semantic_replay_seconds": stage_seconds.get("global_semantic_replay", 0.0),
        "stage_residual_planning_seconds": stage_seconds.get("residual_planning", 0.0),
        "stage_main_core_build_seconds": stage_seconds.get("main_core_build", 0.0),
        "stage_stream_save_seconds": stage_seconds.get("stream_save", 0.0),
        "stage_metadata_write_seconds": stage_seconds.get("metadata_write", 0.0),
        "stage_archive_create_seconds": stage_seconds.get("archive_create", 0.0),
        "stage_decode_verify_seconds": stage_seconds.get("decode_verify", 0.0),
        "regex_compile_cache": compiled_multiline_regex.cache_info()._asdict(),
        "open_function_cache": dataset_extract.open_function_cache_info(),
        "codec_decision_cache": dataset_extract.codec_decision_cache_info(),
        "canonical_global_fields": args.canonical_global_fields,
        "template_count": len(templates) if args.main_core == "id_mapping" else 0,
        "line_count": len(ids) if args.main_core == "id_mapping" else len(original_lines),
        "line_dictionary_count": int(line_dict_metadata.get("count", 0)) if line_dict_metadata else 0,
        "line_dictionary_replaced_lines": int(line_dict_metadata.get("replaced_lines", 0)) if line_dict_metadata else 0,
        "main_core": args.main_core,
        "encode_seconds": encode_seconds,
        "decode_seconds": decode_seconds,
        "full_sha": internal_full_sha,
        "archive_path": str(archive_path),
    }
    (Path(args.output_dir) / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with (Path(args.output_dir) / "summary.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(result.keys()))
        writer.writeheader()
        writer.writerow(result)
    return result


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the pure semantic_contract_filter_repair_v1 CLI.

    The research driver used 222 CLI flags.  The pure version freezes that
    matrix into pure_args_defaults.json and only exposes the block-local values
    that must change between runs.
    """

    parser = argparse.ArgumentParser(description="Pure semantic_contract_filter_repair_v1 block compressor")
    parser.add_argument("--input", required=True)
    parser.add_argument("--dataset", default="Apache")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--llm-cache-dir", required=True)
    parser.add_argument("--max-llm-calls", type=int, default=0)
    parser.add_argument("--replay-plan-out", default="")
    parser.add_argument("--api-base", default=os.environ.get("PARE_LLM_API_BASE", DEFAULT_API_BASE))
    parser.add_argument("--model", default=os.environ.get("PARE_LLM_MODEL", DEFAULT_MODEL))
    parser.add_argument("--temperature", type=float, default=0.0)
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--api-retries", type=int, default=2)
    parser.add_argument("--force-llm", action="store_true")
    parser.add_argument("--operation-profile", action="store_true")
    parser.add_argument("--skip-internal-decode-verify", action="store_true")
    parser.add_argument("--replay-plan-cache", default="")
    parser.add_argument("--semantic-replay-plan-cache", default="")
    parser.add_argument("--numeric-lattice-decision-cache", default="")
    parser.add_argument("--residual-schema-plan-in", default="")
    parser.add_argument("--offline-family-topk-query", action="store_true")
    parser.add_argument("--offline-family-topk", type=int, default=0)
    parser.add_argument("--offline-family-topk-min-support", type=int, default=0)
    parser.add_argument("--offline-family-batch-query", action="store_true")
    parser.add_argument("--offline-family-batch-max-examples", type=int, default=100)
    parser.add_argument("--offline-family-api-workers", type=int, default=1)
    parser.add_argument("--llm-template-index-cache", action="store_true")
    parser.add_argument("--llm-function-trie-cache", action="store_true")
    parser.add_argument("--llm-function-trigger-cache", action="store_true")
    parser.add_argument("--verifier-repair-agent", action="store_true")
    parser.add_argument("--verifier-repair-max-total", type=int, default=0)
    parser.add_argument("--verifier-repair-max-per-family", type=int, default=0)
    parser.add_argument("--verifier-repair-max-candidates", type=int, default=2)
    parser.add_argument("--cpp-pcre2-replay", action="store_true")
    parser.add_argument("--residual-variable-stream-mode", default="")
    parser.add_argument("--residual-numeric-lattice-fallback", default="")
    parser.add_argument("--disable-residual-dominance-planner", action="store_true")
    parser.add_argument("--disable-residual-planning", action="store_true")
    parser.add_argument("--residual-auto-skip-low-templates", type=int, default=-1)
    parser.add_argument("--residual-fallback-min-family-support", type=int, default=-1)
    parser.add_argument("--force-residual-numeric-lattice-admit", action="store_true")
    parsed = parser.parse_args(argv)

    defaults_path = Path(__file__).with_name("pure_args_defaults.json")
    base_defaults = json.loads(defaults_path.read_text(encoding="utf-8"))
    defaults = dict(base_defaults)
    default_verifier_repair_agent = bool(defaults.get("verifier_repair_agent", False))
    default_verifier_repair_max_total = int(defaults.get("verifier_repair_max_total", 0) or 0)
    default_verifier_repair_max_per_family = int(defaults.get("verifier_repair_max_per_family", 0) or 0)
    default_verifier_repair_max_candidates = int(defaults.get("verifier_repair_max_candidates", 2) or 2)
    defaults.update(vars(parsed))
    if parsed.residual_variable_stream_mode:
        defaults["residual_variable_stream_mode"] = parsed.residual_variable_stream_mode
    else:
        defaults["residual_variable_stream_mode"] = base_defaults.get("residual_variable_stream_mode", "context_shape")
    if parsed.residual_numeric_lattice_fallback:
        defaults["residual_numeric_lattice_fallback"] = parsed.residual_numeric_lattice_fallback
    else:
        defaults["residual_numeric_lattice_fallback"] = base_defaults.get("residual_numeric_lattice_fallback", "denum_feature")
    if parsed.disable_residual_dominance_planner:
        defaults["residual_dominance_planner"] = False
    if parsed.disable_residual_planning:
        defaults["disable_residual_planning"] = True
    if parsed.residual_auto_skip_low_templates >= 0:
        defaults["residual_auto_skip_low_templates"] = int(parsed.residual_auto_skip_low_templates)
    if parsed.residual_fallback_min_family_support >= 0:
        defaults["residual_fallback_min_family_support"] = int(parsed.residual_fallback_min_family_support)
    defaults["force_residual_numeric_lattice_admit"] = bool(parsed.force_residual_numeric_lattice_admit)

    # These flags are semantic_contract_filter_repair_v1 policy, not user-tuned
    # runtime choices.  Keep them fixed even if the tiny CLI default is false.
    defaults["operation_profile"] = True
    defaults["skip_internal_decode_verify"] = True
    defaults["compact_decoder_contract"] = True
    defaults["compact_archive_container"] = True
    defaults["force_llm"] = bool(parsed.force_llm)
    defaults["verifier_repair_agent"] = bool(parsed.verifier_repair_agent or default_verifier_repair_agent)
    defaults["verifier_repair_max_total"] = (
        int(parsed.verifier_repair_max_total)
        if int(parsed.verifier_repair_max_total) > 0
        else default_verifier_repair_max_total
    )
    defaults["verifier_repair_max_per_family"] = (
        int(parsed.verifier_repair_max_per_family)
        if int(parsed.verifier_repair_max_per_family) > 0
        else default_verifier_repair_max_per_family
    )
    defaults["verifier_repair_max_candidates"] = (
        int(parsed.verifier_repair_max_candidates)
        if int(parsed.verifier_repair_max_candidates) > 0
        else default_verifier_repair_max_candidates
    )
    return argparse.Namespace(**defaults)

def main() -> int:
    result = compress(parse_args())
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
