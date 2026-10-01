#!/usr/bin/env python3
"""Dataset-specific DeLog-style regex extraction for PARE prototypes.

The regex profiles are migrated from DeLog's ``PatternRecognizer``.  The PARE
side differs in two ways:

* the transformed main text uses collision-free placeholders;
* extracted values are stored in typed side streams and restored by placeholder
  order, so the decoder does not need to rerun regex matching.

Supported profiles in this file focus on the current strict benchmark set:
Apache, Linux, OpenSSH, and Proxifier.
"""

from __future__ import annotations

import argparse
import ast
import atexit
import hashlib
import json
import lzma
import math
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
import threading
import time
import zlib
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path

import pare_rank_proto as base
import pare_template_slot_proto as slot_proto

try:
    import _pare_cpp_accel
except Exception:
    _pare_cpp_accel = None


MONTH_LENGTHS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_TO_INDEX = {name: index + 1 for index, name in enumerate(MONTH_NAMES)}
WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
WEEKDAY_TO_INDEX = {name: index for index, name in enumerate(WEEKDAY_NAMES)}
SYSLOG_TS_RE = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}"
NAIVE_DATETIME_EPOCH = datetime(1970, 1, 1)

# Numeric side streams use a 63-bit varint envelope in the decoder.  Short
# numeric fields such as ports, PIDs, and millisecond timestamps benefit from
# delta coding; very long digit strings are usually opaque IDs and can break the
# varint envelope.  The selector uses the practical threshold, while the writer
# keeps a hard safety guard in case a fixed/profiled spec bypasses selection.
NUMERIC_DELTA_PRACTICAL_MAX_DIGITS = 16
NUMERIC_DELTA_SAFE_MAX_DIGITS = 18


def zigzag_encode(value: int) -> int:
    return (value << 1) ^ (value >> 63)


def zigzag_decode(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def choose_placeholder(text: str, dataset: str, tag: str) -> str:
    candidates = [
        f"<{tag}>",
        f"~{tag}~",
        f"@{tag}@",
        f"<@PARE_{dataset}_{tag}@>",
        f"<#PARE_{dataset}_{tag}#>",
        f"{{PARE_{dataset}_{tag}}}",
        f"__PARE_{dataset}_{tag}__",
    ]
    for candidate in candidates:
        if candidate not in text:
            return candidate
    raise ValueError(f"Could not find collision-free placeholder for {dataset}/{tag}")


def choose_line_dictionary_prefix(text: str) -> str:
    for candidate in ["\x1e", "\x1d", "\x1c", "\x1b"]:
        if candidate not in text:
            return candidate
    raise ValueError("Could not find collision-free line dictionary prefix")


def split_line_body_and_ending(line: str) -> tuple[str, str]:
    if line.endswith("\r\n"):
        return line[:-2], "\r\n"
    if line.endswith("\n"):
        return line[:-1], "\n"
    return line, ""


def split_physical_lines(text: str) -> list[str]:
    """Split only on real LF bytes, not on other Unicode line separators."""
    if not text:
        return []
    lines: list[str] = []
    start = 0
    while True:
        newline_index = text.find("\n", start)
        if newline_index == -1:
            if start < len(text):
                lines.append(text[start:])
            break
        lines.append(text[start:newline_index + 1])
        start = newline_index + 1
    return lines


def build_line_dictionary_text(
    text: str,
    min_support: int,
    max_entries: int,
    id_mode: str = "inline",
) -> tuple[str, dict[str, object] | None, list[str], list[int]]:
    """Replace frequent exact transformed lines with collision-free line ids.

    This is intentionally exact-line based.  It does not learn templates from
    the raw log; it only factors out repeated lines after regex extraction has
    already replaced high-entropy fields with placeholders.  The dictionary is
    therefore fully reversible and independent of the typed side streams.
    """
    if id_mode not in {"inline", "side_varint"}:
        raise ValueError(f"Unsupported line dictionary id mode {id_mode!r}")
    if min_support <= 1 or max_entries <= 0:
        return text, None, [], []

    lines = split_physical_lines(text)
    if not lines:
        return text, None, [], []

    prefix = choose_line_dictionary_prefix(text)
    counts = Counter(lines)
    candidates = [
        line
        for line, count in counts.items()
        if count >= min_support and len(line.encode("latin-1")) > len(prefix) + 1
    ]
    candidates.sort(key=lambda line: (counts[line] * len(line.encode("latin-1")), counts[line]), reverse=True)

    templates: list[str] = []
    line_to_id: dict[str, int] = {}
    for line in candidates:
        if len(templates) >= max_entries:
            break
        candidate_id = len(templates)
        marker = f"{prefix}{candidate_id}\n"
        line_bytes = len(line.encode("latin-1"))
        marker_bytes = len(marker.encode("latin-1"))
        # Require positive raw-byte gain after paying one stored template copy.
        approximate_gain = line_bytes * (counts[line] - 1) - marker_bytes * counts[line]
        if approximate_gain <= 0:
            continue
        line_to_id[line] = candidate_id
        templates.append(line)

    if not templates:
        return text, None, [], []

    encoded_lines: list[str] = []
    side_ids: list[int] = []
    replaced_count = 0
    for line in lines:
        template_id = line_to_id.get(line)
        if template_id is None:
            encoded_lines.append(line)
            continue
        _body, ending = split_line_body_and_ending(line)
        if id_mode == "side_varint":
            encoded_lines.append(f"{prefix}{ending or ''}")
            side_ids.append(template_id)
        else:
            encoded_lines.append(f"{prefix}{template_id}{ending or ''}")
        replaced_count += 1

    metadata = {
        "enabled": True,
        "prefix": prefix,
        "template_file": "line_dictionary/templates.bin",
        "id_mode": id_mode,
        "count": len(templates),
        "replaced_lines": replaced_count,
        "line_count": len(lines),
        "min_support": min_support,
        "max_entries": max_entries,
    }
    if id_mode == "side_varint":
        metadata["id_file"] = "line_dictionary/ids.bin"
    return "".join(encoded_lines), metadata, templates, side_ids


def save_line_dictionary(output_root: Path, templates: list[str], side_ids: list[int]) -> None:
    line_dict_dir = output_root / "line_dictionary"
    line_dict_dir.mkdir()
    base.write_string_stream(line_dict_dir / "templates.bin", templates)
    if side_ids:
        base.write_varint_stream(line_dict_dir / "ids.bin", side_ids)


def expand_line_dictionary_text(text: str, compressed_root: Path, metadata: dict[str, object]) -> str:
    line_dict_meta = metadata.get("line_dictionary")
    if not isinstance(line_dict_meta, dict) or not line_dict_meta.get("enabled"):
        return text

    prefix = str(line_dict_meta["prefix"])
    templates = base.read_string_stream(compressed_root / str(line_dict_meta["template_file"]))
    expected_count = int(line_dict_meta["count"])
    if len(templates) != expected_count:
        raise ValueError(f"Line dictionary expected {expected_count} templates, decoded {len(templates)}")
    id_mode = str(line_dict_meta.get("id_mode", "inline"))
    side_ids: list[int] = []
    side_id_index = 0
    if id_mode == "side_varint":
        side_ids = base.read_varint_stream(compressed_root / str(line_dict_meta["id_file"]))
    elif id_mode != "inline":
        raise ValueError(f"Unsupported line dictionary id mode {id_mode!r}")

    out_lines: list[str] = []
    for line in split_physical_lines(text):
        body, _ending = split_line_body_and_ending(line)
        if id_mode == "side_varint" and body == prefix:
            if side_id_index >= len(side_ids):
                raise ValueError("Line dictionary id stream exhausted")
            template_id = side_ids[side_id_index]
            side_id_index += 1
            if template_id < 0 or template_id >= len(templates):
                raise ValueError(f"Bad line dictionary id {template_id}")
            out_lines.append(templates[template_id])
        elif id_mode == "inline" and body.startswith(prefix) and body[len(prefix):].isdigit():
            template_id = int(body[len(prefix):])
            if template_id < 0 or template_id >= len(templates):
                raise ValueError(f"Bad line dictionary id {template_id}")
            out_lines.append(templates[template_id])
        else:
            out_lines.append(line)
    if id_mode == "side_varint" and side_id_index != len(side_ids):
        raise ValueError("Unused line dictionary ids")
    return "".join(out_lines)


@dataclass(frozen=True)
class ExtractSpec:
    tag: str
    pattern: str
    kind: str
    store_group: int = 0
    replacement: str | None = None
    context_tag: str | None = None


DELOG_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": [
        ExtractSpec("I", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("T", r"\d{2} \d{2}:\d{2}:\d{2}", "day_hms_delta"),
    ],
    "Linux": [
        ExtractSpec("A", r"rhost=([^\s]+)", "string"),
        ExtractSpec("B", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("T", r"\d{2}:\d{2}:\d{2}", "hms_delta"),
    ],
    "OpenSSH": [
        ExtractSpec("A", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("T", r"\d+ \d{2}:\d{2}:\d{2}", "day_hms_delta"),
    ],
    "Proxifier": [
        ExtractSpec("T", r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}", "month_day_hms_delta"),
    ],
}


TYPED_V2_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": DELOG_PROFILES["Apache"],
    "Linux": [
        ExtractSpec("T", SYSLOG_TS_RE, "syslog_delta"),
        ExtractSpec("A", r"rhost=([^\s]+)", "string", store_group=1, replacement="rhost={placeholder}"),
        ExtractSpec("B", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("P", r"\[(\d+)\]", "int_delta_width", store_group=1, replacement="[{placeholder}]"),
        ExtractSpec("Q", r"\bpid=(\d+)\b", "int_delta_width", store_group=1, replacement="pid={placeholder}"),
    ],
    "OpenSSH": [
        ExtractSpec("T", SYSLOG_TS_RE, "syslog_delta"),
        ExtractSpec("A", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("P", r"sshd\[(\d+)\]", "int_delta_width", store_group=1, replacement="sshd[{placeholder}]"),
        ExtractSpec("O", r"\bport (\d+)\b", "int_delta_width", store_group=1, replacement="port {placeholder}"),
        ExtractSpec("D", r"(?<![\w-])(?:[A-Za-z0-9-]+\.){2,}[A-Za-z0-9-]+(?![\w-])", "string"),
    ],
    "Proxifier": [
        ExtractSpec("T", r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}", "month_day_hms_delta"),
        ExtractSpec("H", r"(?<![\w.-])(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+:\d+(?![\w.-])", "string"),
        ExtractSpec("B", r"\b(\d+) bytes\b", "int_delta_width", store_group=1, replacement="{placeholder} bytes"),
        ExtractSpec("L", r"<(\d+)\ssec", "int_delta_width", store_group=1, replacement="<{placeholder} sec"),
        ExtractSpec("D", r"\b\d{2}:\d{2}(?::\d{2})?\b", "colon_duration_delta"),
        ExtractSpec("S", r"\(\d+(?:\.\d+)?\s[KMGT]?B\)", "string"),
    ],
}


TYPED_V3_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": TYPED_V2_PROFILES["Apache"],
    "Linux": TYPED_V2_PROFILES["Linux"],
    "OpenSSH": [
        ExtractSpec("T", SYSLOG_TS_RE, "syslog_delta"),
        ExtractSpec("A", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("P", r"sshd\[(\d+)\]", "int_delta_width", store_group=1, replacement="sshd[{placeholder}]"),
        ExtractSpec("O", r"\bport (\d+)\b", "uint16_split_width", store_group=1, replacement="port {placeholder}"),
        ExtractSpec("D", r"(?<![\w-])(?:[A-Za-z0-9-]+\.){2,}[A-Za-z0-9-]+(?![\w-])", "string"),
    ],
    "Proxifier": [
        ExtractSpec("T", r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}", "month_day_hms_delta"),
        ExtractSpec("L", r"<(\d+)\ssec", "int_delta_width", store_group=1, replacement="<{placeholder} sec"),
        ExtractSpec(
            "D",
            r"lifetime (\d{2}:\d{2}(?::\d{2})?)\b",
            "colon_duration_delta",
            store_group=1,
            replacement="lifetime {placeholder}",
        ),
    ],
}


TYPED_V4_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": TYPED_V3_PROFILES["Apache"],
    "Linux": TYPED_V3_PROFILES["Linux"],
    "OpenSSH": [
        ExtractSpec("T", SYSLOG_TS_RE, "syslog_delta"),
        ExtractSpec("A", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("P", r"sshd\[(\d+)\]", "int_delta_width", store_group=1, replacement="sshd[{placeholder}]"),
        ExtractSpec(
            "O",
            r"\bport (\d+)\b",
            "port_ip_delta_width",
            store_group=1,
            replacement="port {placeholder}",
            context_tag="A",
        ),
        ExtractSpec("D", r"(?<![\w-])(?:[A-Za-z0-9-]+\.){2,}[A-Za-z0-9-]+(?![\w-])", "string"),
    ],
    "Proxifier": TYPED_V3_PROFILES["Proxifier"],
}


TYPED_V6_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": TYPED_V4_PROFILES["Apache"],
    "Linux": TYPED_V4_PROFILES["Linux"],
    "OpenSSH": [
        ExtractSpec("T", SYSLOG_TS_RE, "syslog_delta"),
        ExtractSpec("A", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "ipv4_width"),
        ExtractSpec("P", r"sshd\[(\d+)\]", "int_delta_width", store_group=1, replacement="sshd[{placeholder}]"),
        ExtractSpec(
            "O",
            r"\bport (\d+)\b",
            "port_ip_grouped_circular_delta_width",
            store_group=1,
            replacement="port {placeholder}",
            context_tag="A",
        ),
        ExtractSpec("D", r"(?<![\w-])(?:[A-Za-z0-9-]+\.){2,}[A-Za-z0-9-]+(?![\w-])", "string"),
    ],
    "Proxifier": TYPED_V4_PROFILES["Proxifier"],
}


TYPED_V8_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": TYPED_V6_PROFILES["Apache"],
    "Linux": TYPED_V6_PROFILES["Linux"],
    "OpenSSH": TYPED_V6_PROFILES["OpenSSH"],
    "Proxifier": [
        ExtractSpec("T", r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}", "month_day_hms_delta"),
        ExtractSpec("B", r"\b(\d+) bytes\b", "int_abs_width", store_group=1, replacement="{placeholder} bytes"),
        ExtractSpec("L", r"<(\d+)\ssec", "int_delta_width", store_group=1, replacement="<{placeholder} sec"),
        ExtractSpec(
            "D",
            r"lifetime (\d{2}:\d{2}(?::\d{2})?)\b",
            "colon_duration_delta",
            store_group=1,
            replacement="lifetime {placeholder}",
        ),
    ],
}


TYPED_V9_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": TYPED_V8_PROFILES["Apache"],
    "Linux": TYPED_V8_PROFILES["Linux"],
    "OpenSSH": TYPED_V8_PROFILES["OpenSSH"],
    "Proxifier": [
        ExtractSpec("T", r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}", "month_day_hms_delta"),
        ExtractSpec("H", r"(?<![\w.-])(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+:\d+(?![\w.-])", "string_mtf_rank"),
        ExtractSpec("B", r"\b(\d+) bytes\b", "int_abs_width", store_group=1, replacement="{placeholder} bytes"),
        ExtractSpec("L", r"<(\d+)\ssec", "int_delta_width", store_group=1, replacement="<{placeholder} sec"),
        ExtractSpec(
            "D",
            r"lifetime (\d{2}:\d{2}(?::\d{2})?)\b",
            "colon_duration_delta",
            store_group=1,
            replacement="lifetime {placeholder}",
        ),
        ExtractSpec("S", r"\(\d+(?:\.\d+)?\s[KMGT]?B\)", "string_mtf_rank"),
    ],
}


TYPED_V10_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": TYPED_V9_PROFILES["Apache"],
    "Linux": TYPED_V9_PROFILES["Linux"],
    "OpenSSH": TYPED_V9_PROFILES["OpenSSH"],
    "Proxifier": [
        ExtractSpec("T", r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}", "month_day_hms_delta"),
        ExtractSpec("H", r"(?<![\w.-])(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+:\d+(?![\w.-])", "string_mtf_rank"),
        ExtractSpec("B", r"\b(\d+) bytes\b", "int_abs", store_group=1, replacement="{placeholder} bytes"),
        ExtractSpec("L", r"<(\d+)\ssec", "int_delta_width", store_group=1, replacement="<{placeholder} sec"),
        ExtractSpec(
            "D",
            r"lifetime (\d{2}:\d{2}(?::\d{2})?)\b",
            "colon_duration_delta",
            store_group=1,
            replacement="lifetime {placeholder}",
        ),
        ExtractSpec("S", r"\(\d+(?:\.\d+)?\s[KMGT]?B\)", "string_mtf_rank"),
    ],
}


AUTO_V0_PROFILES: dict[str, list[ExtractSpec]] = {
    dataset: [
        ExtractSpec(
            tag=spec.tag,
            pattern=spec.pattern,
            kind="auto",
            store_group=spec.store_group,
            replacement=spec.replacement,
            context_tag=None,
        )
        for spec in specs
    ]
    for dataset, specs in TYPED_V10_PROFILES.items()
}


AUTO_MDL_V1_PROFILES: dict[str, list[ExtractSpec]] = {
    "Apache": [
        ExtractSpec(
            "T",
            r"\[(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{2}\s+\d{2}:\d{2}:\d{2}\s+\d{4}\]",
            "auto",
        ),
        ExtractSpec("I", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "auto"),
    ],
    "Linux": AUTO_V0_PROFILES["Linux"],
    "OpenSSH": AUTO_V0_PROFILES["OpenSSH"],
    "Proxifier": AUTO_V0_PROFILES["Proxifier"],
}


PROFILE_MAP = {
    "delog": DELOG_PROFILES,
    "typed_v2": TYPED_V2_PROFILES,
    "typed_v3": TYPED_V3_PROFILES,
    "typed_v4": TYPED_V4_PROFILES,
    "typed_v6": TYPED_V6_PROFILES,
    "typed_v8": TYPED_V8_PROFILES,
    "typed_v9": TYPED_V9_PROFILES,
    "typed_v10": TYPED_V10_PROFILES,
    "auto_v0": AUTO_V0_PROFILES,
    "auto_mdl_v1": AUTO_MDL_V1_PROFILES,
}


AUTO_DISCOVERED_PROFILES = {
    "auto_discovered_v1",
    "auto_discovered_v2",
    "auto_llm_v1",
    "auto_llm_open_v1",
    "open_function_v1",
    "auto_template_cache_v1",
    "auto_template_cache_v2",
    "auto_line_transducer_v1",
    "auto_line_transducer_v2",
    "auto_line_transducer_v3",
    "auto_cache_lattice_v1",
    "auto_cache_lattice_v2",
    "auto_cache_lattice_v3",
    "auto_cache_lattice_v4",
    "auto_cache_lattice_v5",
    "auto_cache_lattice_v6",
    "auto_cache_lattice_v7",
    "auto_atis_v1",
    "auto_struct_v0",
    "auto_struct_v1",
    "auto_struct_v2",
    "auto_struct_v3",
    "auto_struct_v4",
    "auto_struct_v5",
    "auto_struct_v6",
    "auto_struct_v7",
    "auto_struct_v8",
    "auto_struct_mdl_v1",
    "auto_struct_mdl_v2",
    "auto_struct_mdl_v3",
    "auto_struct_mdl_v6",
}


FEATURE_CODEC_PREFILTER = False
CODEC_DECISION_CACHE: dict[str, dict[str, object]] = {}
CODEC_DECISION_CACHE_SOURCE = ""
CODEC_DECISION_CACHE_HITS = 0
CODEC_DECISION_CACHE_MISSES = 0
CODEC_DECISION_CACHE_SKIPS = 0
OPERATION_PROFILE = False
OPERATION_TIMINGS: dict[str, dict[str, float | int]] = {}
STREAM_CODEC_CANDIDATE_CACHE: dict[tuple[str, str, str, int, str], list[tuple[str, int]]] = {}
STREAM_CODEC_CANDIDATE_CACHE_HITS = 0
STREAM_CODEC_CANDIDATE_CACHE_MISSES = 0
STREAM_CODEC_CANDIDATE_CACHE_SKIPS = 0


def set_feature_codec_prefilter(enabled: bool) -> None:
    """Enable cheap feature-based codec shortlist selection for experiments.

    The shortlist only controls which encoder-side candidates are measured.
    Every admitted stream is still serialized normally and verified by an
    end-to-end decode, so a bad feature decision can hurt ratio but not
    losslessness.
    """
    global FEATURE_CODEC_PREFILTER
    FEATURE_CODEC_PREFILTER = enabled


def set_operation_profile(enabled: bool) -> None:
    global OPERATION_PROFILE
    OPERATION_PROFILE = enabled


def reset_operation_timings() -> None:
    OPERATION_TIMINGS.clear()
    reset_stream_codec_candidate_cache_stats()


def record_operation_timing(name: str, elapsed: float, count: int = 1) -> None:
    if not OPERATION_PROFILE:
        return
    entry = OPERATION_TIMINGS.setdefault(name, {"seconds": 0.0, "count": 0})
    entry["seconds"] = float(entry["seconds"]) + elapsed
    entry["count"] = int(entry["count"]) + count


def operation_timings_info(raw_bytes: int) -> dict[str, dict[str, float | int]]:
    raw_mb = raw_bytes / 1_000_000.0
    result: dict[str, dict[str, float | int]] = {}
    for name, entry in sorted(OPERATION_TIMINGS.items()):
        seconds = float(entry.get("seconds", 0.0))
        count = int(entry.get("count", 0))
        result[name] = {
            "seconds": round(seconds, 6),
            "count": int(count),
            "avg_ms": round(seconds * 1000.0 / count, 6) if count else 0.0,
            "effective_mbps": round(raw_mb / seconds, 6) if seconds > 0 else 0.0,
        }
    return result


def reset_stream_codec_candidate_cache_stats() -> None:
    global STREAM_CODEC_CANDIDATE_CACHE_HITS
    global STREAM_CODEC_CANDIDATE_CACHE_MISSES
    global STREAM_CODEC_CANDIDATE_CACHE_SKIPS
    STREAM_CODEC_CANDIDATE_CACHE_HITS = 0
    STREAM_CODEC_CANDIDATE_CACHE_MISSES = 0
    STREAM_CODEC_CANDIDATE_CACHE_SKIPS = 0


def _stream_codec_candidate_cache_key(values: list[str], forced_kinds: set[str] | None) -> tuple[str, str, str, int, str] | None:
    if not values:
        return None
    min_values = int(os.environ.get("PARE_STREAM_CODEC_CACHE_MIN_VALUES", "16"))
    if len(values) < min_values:
        return None
    digest = hashlib.sha1()
    digest.update(str(len(values)).encode("ascii"))
    digest.update(b"\0")
    for value in values:
        data = value.encode("latin-1", errors="ignore")
        digest.update(len(data).to_bytes(4, "little", signed=False))
        digest.update(data)
    forced_key = ",".join(sorted(forced_kinds)) if forced_kinds is not None else "*"
    return (
        os.environ.get("PARE_PROXY_LZMA_PRESET", ""),
        "1" if FEATURE_CODEC_PREFILTER else "0",
        forced_key,
        len(values),
        digest.hexdigest(),
    )


def load_codec_decision_cache(path: str | Path | None) -> None:
    """Load verified stream codec decisions from a previous metadata file.

    This is an experiment-speed diagnostic: the cache is only consulted for
    streams whose tag reappears exactly in the current block.  The output still
    goes through the normal writer and full decode verification, so a stale
    decision can fail or lose ratio but cannot silently break losslessness.
    """
    global CODEC_DECISION_CACHE
    global CODEC_DECISION_CACHE_SOURCE
    global CODEC_DECISION_CACHE_HITS
    global CODEC_DECISION_CACHE_MISSES
    global CODEC_DECISION_CACHE_SKIPS

    CODEC_DECISION_CACHE = {}
    CODEC_DECISION_CACHE_SOURCE = ""
    CODEC_DECISION_CACHE_HITS = 0
    CODEC_DECISION_CACHE_MISSES = 0
    CODEC_DECISION_CACHE_SKIPS = 0
    if not path:
        return
    cache_path = Path(path)
    CODEC_DECISION_CACHE_SOURCE = str(cache_path)
    if not cache_path.is_file():
        return
    payload = json.loads(cache_path.read_text(encoding="utf-8"))
    streams = payload.get("dataset_extract", {}).get("streams", [])
    if not isinstance(streams, list):
        return
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        tag = str(stream.get("tag", ""))
        kind = str(stream.get("kind", ""))
        if not tag or not kind:
            continue
        decision: dict[str, object] = {"kind": kind}
        if stream.get("context_tag"):
            decision["context_tag"] = str(stream["context_tag"])
        if kind == "open_function" and stream.get("program"):
            decision["program"] = str(stream["program"])
            decision["open_numeric_codec"] = str(stream.get("open_numeric_codec", "delta"))
        CODEC_DECISION_CACHE[tag] = decision


def codec_decision_cache_info() -> dict[str, object]:
    return {
        "source": CODEC_DECISION_CACHE_SOURCE,
        "entries": len(CODEC_DECISION_CACHE),
        "hits": CODEC_DECISION_CACHE_HITS,
        "misses": CODEC_DECISION_CACHE_MISSES,
        "skips": CODEC_DECISION_CACHE_SKIPS,
    }


def cached_open_numeric_codec(tag: str) -> str | None:
    decision = CODEC_DECISION_CACHE.get(tag)
    if not isinstance(decision, dict):
        return None
    if str(decision.get("kind", "")) != "open_function":
        return None
    return str(decision.get("open_numeric_codec", "delta"))


def _all_values_match(values: list[str], pattern: str) -> bool:
    return all(re.fullmatch(pattern, value) for value in values)


def _int_abs_is_reversible(values: list[str]) -> bool:
    for value in values:
        if not re.fullmatch(r"\d+", value):
            return False
        if str(int(value)) != value:
            return False
    return True


def _fixed_width_int_is_reversible(values: list[str]) -> bool:
    if not values or not _all_values_match(values, r"\d+"):
        return False
    widths = {len(value) for value in values}
    return len(widths) == 1


PACKED_DATETIME_RE = re.compile(r"^(\d{6,8})-(\d{1,2}):(\d{1,2}):(\d{1,2}):(\d{1,6})$")


def packed_datetime_is_reversible(values: list[str]) -> bool:
    return bool(values) and all(PACKED_DATETIME_RE.fullmatch(value) for value in values)


def cached_codec_kind_is_safe(kind: str, values: list[str], context_tag: str | None = None) -> bool:
    """Check cheap preconditions before trusting a cached codec decision."""
    if kind in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width"}:
        for value in values:
            if not re.fullmatch(r"\d+", value):
                return False
            try:
                parsed = int(value)
            except ValueError:
                return False
            if parsed < 0 or parsed > 65535:
                return False
        return True
    if kind == "size_from_prev_bytes":
        return _all_values_match(values, r"\d+(?:\.\d+)?\s*[KMGT]?B")
    if kind == "int_abs":
        return _int_abs_is_reversible(values)
    if kind in {"int_abs_fixed_width", "int_delta_fixed_width"}:
        return _fixed_width_int_is_reversible(values)
    if kind == "int_delta":
        return _int_abs_is_reversible(values)
    if kind in {"int_abs_width", "int_delta_width", "uint16_split_width", "int_denum_bucket_delta"}:
        return _all_values_match(values, r"\d+")
    if kind == "hms_delta":
        return _all_values_match(values, r"\d{2}:\d{2}:\d{2}")
    if kind == "month_day_hms_delta":
        return _all_values_match(values, r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}")
    if kind == "day_hms_delta":
        return _all_values_match(values, r"\d{1,2} \d{2}:\d{2}:\d{2}")
    if kind == "packed_datetime_delta":
        return packed_datetime_is_reversible(values)
    if kind == "affixed_int_delta":
        return parse_affixed_int_shape(values) is not None
    if kind == "delimited_int_tuple_delta":
        return parse_delimited_int_tuple_shape(values) is not None
    if kind == "numeric_skeleton_tuple_delta":
        return parse_numeric_skeleton_shape(values) is not None
    if kind == "mixed_skeleton_delta":
        return parse_mixed_skeleton_shape(values) is not None
    if kind == "shape_mixed_skeleton_delta":
        return should_probe_shape_mixed_skeleton(values)
    if kind == "ipv4_plain":
        for value in values:
            parts = value.split(".")
            if len(parts) != 4:
                return False
            try:
                if ".".join(str(int(part)) for part in parts) != value:
                    return False
            except ValueError:
                return False
        return True
    if kind == "hex_int_delta_width":
        return _all_values_match(values, r"[0-9A-Fa-f]+")
    if kind == "hex_pair_delta":
        return _all_values_match(values, r"0x(?:[0-9a-f]+|[0-9A-F]+)/0x(?:[0-9a-f]+|[0-9A-F]+)")
    return True


CONTEXT_CODEC_KINDS = {
    "port_ip_delta_width",
    "port_ip_grouped_circular_delta_width",
    "size_from_prev_bytes",
}

DIRECT_CODEC_KINDS = {
    "string",
    "string_mtf_rank",
    "string_mtf_cdelta",
    "byte_phrase_delta",
    "hex_int_delta_width",
    "hex_pair_delta",
    "ipv4_width",
    "ipv4_plain",
    "compound_endpoint_mixed",
    "hms_delta",
    "day_hms_delta",
    "month_day_hms_delta",
    "syslog_delta",
    "asctime_year_delta",
    "apache_timestamp_delta",
    "packed_datetime_delta",
    "colon_duration_delta",
    "affixed_int_delta",
    "delimited_int_tuple_delta",
    "numeric_skeleton_tuple_delta",
    "mixed_skeleton_delta",
    "shape_mixed_skeleton_delta",
    "int_abs",
    "int_delta",
    "int_abs_width",
    "int_delta_width",
    "int_abs_fixed_width",
    "int_delta_fixed_width",
    "uint16_split_width",
    "int_denum_bucket_delta",
    "open_function",
    "relation_pair_delta",
}


def cached_codec_decision_spec(
    spec: ExtractSpec,
    values_by_tag: dict[str, list[str]],
    placeholders_by_tag: dict[str, str],
) -> ExtractSpec | None:
    global CODEC_DECISION_CACHE_HITS
    global CODEC_DECISION_CACHE_MISSES
    global CODEC_DECISION_CACHE_SKIPS

    if not CODEC_DECISION_CACHE:
        return None
    decision = CODEC_DECISION_CACHE.get(spec.tag)
    if decision is None:
        CODEC_DECISION_CACHE_MISSES += 1
        return None
    kind = str(decision.get("kind", ""))
    context_tag = decision.get("context_tag")
    if kind in CONTEXT_CODEC_KINDS:
        if not isinstance(context_tag, str):
            CODEC_DECISION_CACHE_SKIPS += 1
            return None
        if context_tag == spec.tag or context_tag not in values_by_tag or context_tag not in placeholders_by_tag:
            CODEC_DECISION_CACHE_SKIPS += 1
            return None
        if not cached_codec_kind_is_safe(kind, values_by_tag[spec.tag], context_tag=context_tag):
            CODEC_DECISION_CACHE_SKIPS += 1
            return None
    elif kind in DIRECT_CODEC_KINDS:
        if not cached_codec_kind_is_safe(kind, values_by_tag[spec.tag]):
            CODEC_DECISION_CACHE_SKIPS += 1
            return None
        if kind == "open_function":
            program = decision.get("program") or context_tag
            if not isinstance(program, str):
                CODEC_DECISION_CACHE_SKIPS += 1
                return None
            try:
                payload = json.loads(program)
                if not all(render_open_function_exact(value, payload) == value for value in values_by_tag[spec.tag]):
                    CODEC_DECISION_CACHE_SKIPS += 1
                    return None
            except Exception:
                CODEC_DECISION_CACHE_SKIPS += 1
                return None
            context_tag = program
        else:
            context_tag = None
    else:
        CODEC_DECISION_CACHE_SKIPS += 1
        return None
    CODEC_DECISION_CACHE_HITS += 1
    return ExtractSpec(
        tag=spec.tag,
        pattern=spec.pattern,
        kind=kind,
        store_group=spec.store_group,
        replacement=spec.replacement,
        context_tag=context_tag if isinstance(context_tag, str) else None,
    )


WEEKDAY_RE = r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)"
MONTH_RE = r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"


def regex_count(pattern: str, text: str) -> int:
    return sum(1 for _ in re.finditer(pattern, text, flags=re.MULTILINE))


def regex_distinct_group_count(pattern: str, text: str, group: int = 1, cap: int = 1024) -> int:
    values: set[str] = set()
    for match in re.finditer(pattern, text, flags=re.MULTILINE):
        values.add(match.group(group))
        if len(values) >= cap:
            return len(values)
    return len(values)


def _autostruct_shape_key(text: str, generalize_alpha: bool = True) -> str:
    """Return a low-level lexical shape, without semantic labels."""
    out: list[str] = []
    index = 0
    for match in re.finditer(r"\d+|[A-Za-z]+|\s+|.", text, flags=re.DOTALL):
        part = match.group(0)
        if part.isdigit():
            out.append(f"D{len(part)}")
        elif part.isalpha():
            out.append(f"A{len(part)}" if generalize_alpha else part)
        elif part.isspace():
            out.append(f"S{len(part)}")
        else:
            out.append(part)
        index = match.end()
    if index != len(text):
        out.append(text[index:])
    return "|".join(out)


def _autostruct_regex_from_sample(sample: str, generalize_alpha: bool = True, digit_flexible: bool = False) -> str:
    """Compile a sampled lexical shape into a conservative regex fragment."""
    parts: list[str] = []
    for match in re.finditer(r"\d+|[A-Za-z]+|\s+|.", sample, flags=re.DOTALL):
        part = match.group(0)
        if part.isdigit():
            parts.append(r"\d+" if digit_flexible else rf"\d{{{len(part)}}}")
        elif part.isalpha() and generalize_alpha:
            parts.append(rf"[A-Za-z]{{{len(part)}}}")
        elif part.isspace():
            parts.append(r"\s+")
        else:
            parts.append(re.escape(part))
    return "".join(parts)


def _iter_line_prefixes(line: str, max_tokens: int = 5) -> list[str]:
    prefixes: list[str] = []
    token_count = 0
    last_end = 0
    for match in re.finditer(r"\S+", line):
        token_count += 1
        last_end = match.end()
        prefixes.append(line[:last_end])
        if token_count >= max_tokens:
            break
    return prefixes


def _autostruct_candidate_cost(
    pattern: str,
    text: str,
    placeholder_len: int = 3,
    cap: int = 50000,
    codec_aware: bool = False,
) -> tuple[int, int, int, int]:
    values: list[str] = []
    total_len = 0
    count = 0
    distinct: set[str] = set()
    regex = re.compile(pattern, flags=re.MULTILINE)
    for match in regex.finditer(text):
        value = match.group(1)
        count += 1
        total_len += len(value.encode("latin-1", errors="ignore"))
        distinct.add(value)
        if len(values) < cap:
            values.append(value)
    if count == 0:
        return 0, 0, 0, 0
    if codec_aware:
        try:
            side_cost_sample = min(cost for _kind, cost in stream_codec_candidates(values))
        except Exception:
            side_cost_sample = compressed_parts_cost([encode_string_stream_bytes(values)])
    else:
        side_cost_sample = compressed_parts_cost([encode_string_stream_bytes(values)])
    if count > len(values) and values:
        # Scale the sampled side-stream cost. This is only for candidate pruning;
        # the archive writer later computes the real codec and verification.
        side_cost = int(side_cost_sample * count / len(values))
    else:
        side_cost = side_cost_sample
    gross_saved = total_len - count * placeholder_len
    estimated_gain = gross_saved - side_cost - 96
    return estimated_gain, count, len(distinct), total_len // max(1, count)


def _autostruct_best_codec_ratio(pattern: str, text: str, cap: int = 50000) -> tuple[str, float]:
    values: list[str] = []
    total_len = 0
    count = 0
    regex = re.compile(pattern, flags=re.MULTILINE)
    for match in regex.finditer(text):
        value = match.group(1)
        count += 1
        total_len += len(value.encode("latin-1", errors="ignore"))
        if len(values) < cap:
            values.append(value)
    if not values or total_len == 0:
        return "none", 1.0
    try:
        best_kind, best_cost = min(stream_codec_candidates(values), key=lambda item: item[1])
    except Exception:
        best_kind = "string"
        best_cost = compressed_parts_cost([encode_string_stream_bytes(values)])
    if count > len(values):
        best_cost = int(best_cost * count / len(values))
    return best_kind, best_cost / max(1, total_len)


def discover_autostruct_specs(
    text: str,
    codec_aware: bool = False,
    factorization_guard: bool = False,
    prefix_competition: bool = False,
    paired_prefix_inner: bool = False,
    numeric_phrases: bool = False,
    relaxed_numeric_phrases: bool = False,
    colon_terminal_tokens: bool = False,
    duration_phrases: bool = False,
    paren_quantities: bool = False,
    joint_context_promote: bool = False,
    max_specs: int = 10,
) -> list[ExtractSpec]:
    """Infer reversible extraction specs from lexical structure only.

    This profile intentionally avoids semantic recognizer names such as
    timestamp, IPv4, host:port, PID, or domain. It proposes regexes from line
    prefix shapes, stable numeric wrappers, and whole-token delimiter skeletons.
    """
    line_count = max(1, text.count("\n") + (0 if text.endswith("\n") else 1))
    min_common = max(20, min(line_count // 20, 1000))
    candidates: list[tuple[int, str, str, int, str | None]] = []
    seen_patterns: set[str] = set()

    # P1: repeated line-prefix shapes. This rediscovers timestamp-like prefixes
    # as common lexical programs rather than as named timestamp regexes.
    prefix_counts: Counter[str] = Counter()
    prefix_sample: dict[str, str] = {}
    prefix_candidates: list[tuple[int, float, int, str, str, int, str | None]] = []
    for line in split_physical_lines(text):
        body, _ending = split_line_body_and_ending(line)
        if paired_prefix_inner and body.startswith("["):
            close_index = body.find("]")
            if 3 <= close_index <= 48:
                inner = body[1:close_index]
                if any(ch.isdigit() for ch in inner):
                    key = "BRACKET|" + _autostruct_shape_key(inner, generalize_alpha=True)
                    prefix_counts[key] += 1
                    prefix_sample.setdefault(key, "[" + inner + "]")
        for prefix in _iter_line_prefixes(body, max_tokens=5):
            if not any(ch.isdigit() for ch in prefix):
                continue
            if len(prefix) < 8 or len(prefix) > 48:
                continue
            key = _autostruct_shape_key(prefix, generalize_alpha=True)
            prefix_counts[key] += 1
            prefix_sample.setdefault(key, prefix)
    for key, count in prefix_counts.most_common(16):
        if count < min_common:
            continue
        sample = prefix_sample[key]
        if key.startswith("BRACKET|") and sample.startswith("[") and sample.endswith("]"):
            inner_sample = sample[1:-1]
            pattern = r"(?m)^\[(" + _autostruct_regex_from_sample(inner_sample, generalize_alpha=True) + r")\]"
            replacement = "[{placeholder}]"
        else:
            pattern = r"(?m)^(" + _autostruct_regex_from_sample(sample, generalize_alpha=True) + r")"
            replacement = "{placeholder}"
        if pattern in seen_patterns:
            continue
        gain, real_count, distinct, avg_len = _autostruct_candidate_cost(pattern, text, codec_aware=codec_aware)
        if factorization_guard:
            best_kind, best_ratio = _autostruct_best_codec_ratio(pattern, text)
            if best_kind in {"string", "string_mtf_rank"} and best_ratio > 0.10:
                continue
        else:
            best_kind, best_ratio = _autostruct_best_codec_ratio(pattern, text) if prefix_competition else ("unknown", 1.0)
        if real_count >= min_common and distinct >= 2 and avg_len >= 8 and gain > 0:
            seen_patterns.add(pattern)
            modelable = 1 if best_kind not in {"string", "string_mtf_rank", "unknown"} else 0
            if prefix_competition:
                prefix_candidates.append((modelable, -best_ratio, gain, pattern, replacement, 1, "prefix-shape"))
            else:
                candidates.append((gain, pattern, replacement, 1, "prefix-shape"))

    if prefix_competition and prefix_candidates:
        prefix_candidates.sort(reverse=True)
        kept = 0
        for _modelable, _neg_ratio, gain, pattern, replacement, store_group, source in prefix_candidates:
            # Prefixes overlap by construction. Preserve the internal
            # factorization order instead of letting raw-length gain move a
            # longer, less factored prefix ahead of the timestamp-like prefix.
            candidates.append((10_000_000 - kept, pattern, replacement, store_group, source))
            kept += 1
            if kept >= 3:
                break

    # Token sample for the next two proposers.
    token_counts: Counter[str] = Counter()
    token_sample: dict[str, str] = {}
    numeric_wrapper_counts: Counter[tuple[str, str]] = Counter()
    numeric_wrapper_values: dict[tuple[str, str], set[str]] = {}
    for match in re.finditer(r"\S+", text):
        token = match.group(0)
        if len(token) > 96:
            continue
        digit_runs = list(re.finditer(r"\d+", token))
        if digit_runs:
            shape = _autostruct_shape_key(token, generalize_alpha=True)
            token_counts[shape] += 1
            token_sample.setdefault(shape, token)
        if len(digit_runs) == 1:
            run = digit_runs[0]
            prefix = token[:run.start()]
            suffix = token[run.end():]
            value = run.group(0)
            if len(prefix) + len(suffix) >= 2 and len(value) >= 2:
                key = (prefix, suffix)
                numeric_wrapper_counts[key] += 1
                if len(numeric_wrapper_values.setdefault(key, set())) < 2048:
                    numeric_wrapper_values[key].add(value)

    # P2: stable single-number wrappers such as "sshd[24200]:" or "uid=1000".
    # The literal wrapper is learned from the corpus; the system does not know
    # what a PID or UID is.
    numeric_candidates: list[tuple[int, tuple[str, str]]] = []
    for key, count in numeric_wrapper_counts.items():
        if count < max(50, min_common // 2):
            continue
        distinct = len(numeric_wrapper_values.get(key, set()))
        if distinct < min(8, max(2, count // 40)):
            continue
        prefix, suffix = key
        wrapper_len = len(prefix) + len(suffix)
        numeric_candidates.append((count * max(1, wrapper_len), key))
    numeric_candidates.sort(reverse=True)
    for _score, (prefix, suffix) in numeric_candidates[:8]:
        pattern = r"(?<!\S)" + re.escape(prefix) + r"(\d+)" + re.escape(suffix) + r"(?!\S)"
        replacement = prefix + "{placeholder}" + suffix
        if pattern in seen_patterns:
            continue
        gain, real_count, distinct, avg_len = _autostruct_candidate_cost(pattern, text, codec_aware=codec_aware)
        if real_count >= max(50, min_common // 2) and distinct >= 2 and avg_len >= 2 and gain > 0:
            seen_patterns.add(pattern)
            candidates.append((gain, pattern, replacement, 1, "numeric-wrapper"))

    # P2b: learned numeric phrases, such as "<n> unit", "word <n>", and
    # "<n unit". The unit/context words are learned from the corpus; there is
    # no built-in byte/duration vocabulary here.
    if numeric_phrases:
        phrase_counts: Counter[tuple[str, str, str]] = Counter()
        phrase_values: dict[tuple[str, str, str], set[str]] = {}

        def add_phrase(kind: str, left: str, right: str, value: str) -> None:
            key = (kind, left, right)
            phrase_counts[key] += 1
            if len(phrase_values.setdefault(key, set())) < 2048:
                phrase_values[key].add(value)

        for match in re.finditer(r"\b(\d{1,8})\s+([A-Za-z][A-Za-z0-9_-]{1,24})\b", text):
            value, word = match.groups()
            add_phrase("num_word", "", word, value)
        for match in re.finditer(r"\b([A-Za-z][A-Za-z0-9_-]{1,24})\s+(\d{1,8})\b", text):
            word, value = match.groups()
            if word in MONTH_NAMES or word in {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}:
                continue
            add_phrase("word_num", word, "", value)
        for match in re.finditer(r"(?<!\S)([<>=])(\d{1,8})\s+([A-Za-z][A-Za-z0-9_-]{1,24})\b", text):
            punct, value, word = match.groups()
            add_phrase("punct_num_word", punct, word, value)
        if duration_phrases:
            for match in re.finditer(r"\b([A-Za-z][A-Za-z0-9_-]{1,24})\s+(\d{1,2}:\d{2}(?::\d{2})?)\b", text):
                word, value = match.groups()
                if word in MONTH_NAMES or word in {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}:
                    continue
                add_phrase("word_duration", word, "", value)

        phrase_candidates: list[tuple[int, tuple[str, str, str]]] = []
        for key, count in phrase_counts.items():
            if count < max(50, min_common // 2):
                continue
            distinct = len(phrase_values.get(key, set()))
            if distinct < min(4, max(1, count // 100)):
                continue
            phrase_candidates.append((count * max(1, distinct), key))
        phrase_candidates.sort(reverse=True)
        for _score, (kind, left, right) in phrase_candidates[:8]:
            if kind == "num_word":
                pattern = r"\b(\d{1,8})\s+" + re.escape(right) + r"\b"
                replacement = "{placeholder} " + right
            elif kind == "word_num":
                pattern = r"\b" + re.escape(left) + r"\s+(\d{1,8})\b"
                replacement = left + " {placeholder}"
            elif kind == "word_duration":
                pattern = r"\b" + re.escape(left) + r"\s+(\d{1,2}:\d{2}(?::\d{2})?)\b"
                replacement = left + " {placeholder}"
            else:
                pattern = r"(?<!\S)" + re.escape(left) + r"(\d{1,8})\s+" + re.escape(right) + r"\b"
                replacement = left + "{placeholder} " + right
            if pattern in seen_patterns:
                continue
            gain, real_count, distinct, avg_len = _autostruct_candidate_cost(pattern, text, codec_aware=True)
            min_gain = -2 * real_count if relaxed_numeric_phrases else 0
            if real_count >= max(50, min_common // 2) and distinct >= 1 and avg_len >= 1 and gain > min_gain:
                seen_patterns.add(pattern)
                score = gain
                if joint_context_promote and kind == "num_word":
                    # Relation-aware ordering: a following parenthesized
                    # quantity may be predictable from this numeric-unit
                    # field, but the conditional selector can only discover
                    # that if the context field appears first.
                    score += 7_000_000
                elif joint_context_promote and kind == "word_duration":
                    score += 4_000_000
                candidates.append((score, pattern, replacement, 1, "numeric-phrase"))

    # P2d: parenthesized quantities such as "(4.22 KB)". This is a pure
    # delimiter/number/unit shape. If a byte-count stream appears earlier,
    # the existing conditional selector can later discover that the quantity
    # is predictable from the byte count without naming this pattern as size.
    if paren_quantities:
        pattern = r"(?<!\S)(\(\d+(?:\.\d+)?\s+[A-Za-z]{1,4}\))(?!\S)"
        gain, real_count, distinct, avg_len = _autostruct_candidate_cost(pattern, text, codec_aware=True)
        if real_count >= max(50, min_common // 2) and distinct >= 2 and avg_len >= 5 and gain > -real_count:
            seen_patterns.add(pattern)
            score = gain + (3_000_000 if joint_context_promote else 0)
            candidates.append((score, pattern, "{placeholder}", 1, "paren-quantity"))

    # P2c: generic colon-terminal delimited tokens, e.g. "name:1234".
    # This is a structural program over punctuation, not a host/port semantic
    # recognizer. It competes by codelength like every other proposal.
    if colon_terminal_tokens:
        pattern = r"(?<!\S)([A-Za-z0-9_.-]+:\d+)(?!\S)"
        gain, real_count, distinct, avg_len = _autostruct_candidate_cost(pattern, text, codec_aware=True)
        if real_count >= max(50, min_common // 2) and distinct >= 2 and avg_len >= 6 and gain > 0:
            seen_patterns.add(pattern)
            candidates.append((gain, pattern, "{placeholder}", 1, "colon-terminal-token"))

    # P3: whole-token delimiter skeletons. This catches dotted/colon/slash
    # structures without naming IP addresses, domains, ports, or paths.
    token_shape_candidates: list[tuple[int, str]] = []
    for shape, count in token_counts.items():
        if count < max(50, min_common // 2):
            continue
        sample = token_sample[shape]
        if not any(ch.isdigit() for ch in sample):
            continue
        if not any(ch in sample for ch in ".:/[]()<>=-_"):
            continue
        if len(sample) < 6:
            continue
        token_shape_candidates.append((count * len(sample), shape))
    token_shape_candidates.sort(reverse=True)
    for _score, shape in token_shape_candidates[:10]:
        sample = token_sample[shape]
        pattern = r"(?<!\S)(" + _autostruct_regex_from_sample(sample, generalize_alpha=True, digit_flexible=True) + r")(?!\S)"
        if pattern in seen_patterns:
            continue
        gain, real_count, distinct, avg_len = _autostruct_candidate_cost(pattern, text, codec_aware=codec_aware)
        if real_count >= max(50, min_common // 2) and distinct >= 2 and avg_len >= 6 and gain > 0:
            seen_patterns.add(pattern)
            candidates.append((gain, pattern, "{placeholder}", 1, "whole-token-shape"))

    candidates.sort(reverse=True, key=lambda item: item[0])
    specs: list[ExtractSpec] = []
    tag_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for index, (_gain, pattern, replacement, store_group, _source) in enumerate(candidates[:max_specs]):
        tag = tag_alphabet[index] if index < len(tag_alphabet) else f"X{index}"
        specs.append(ExtractSpec(tag, pattern, "auto", store_group=store_group, replacement=replacement))
    if os.environ.get("PARE_OPEN_FUNCTION_MDL_SELECT", "0") == "1":
        specs = select_open_function_specs_by_proxy(text, dataset, specs)
    return specs


def retag_extract_specs(specs: list[ExtractSpec]) -> list[ExtractSpec]:
    tag_alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    retagged: list[ExtractSpec] = []
    for index, spec in enumerate(specs):
        tag = tag_alphabet[index] if index < len(tag_alphabet) else f"X{index}"
        retagged.append(ExtractSpec(
            tag=tag,
            pattern=spec.pattern,
            kind=spec.kind,
            store_group=spec.store_group,
            replacement=spec.replacement,
            context_tag=spec.context_tag,
        ))
    return retagged


def transform_text_with_specs(
    text: str,
    dataset: str,
    profile: str,
    profile_specs: list[ExtractSpec],
) -> tuple[str, dict[str, list[str]], dict[str, object]]:
    transformed = text
    values_by_tag: dict[str, list[str]] = {}
    specs_meta: list[dict[str, object]] = []

    for spec in profile_specs:
        existing_placeholders = [
            str(spec_meta["placeholder"])
            for spec_meta in specs_meta
        ]
        placeholder = choose_placeholder(text, dataset, spec.tag)
        values: list[str] = []
        regex = re.compile(spec.pattern, flags=re.MULTILINE)
        previous_transformed = transformed

        def replace(match: re.Match[str]) -> str:
            values.append(match.group(spec.store_group))
            if spec.replacement is not None:
                return _safe_replacement_format(spec.replacement, placeholder, match)
            return placeholder

        transformed = regex.sub(replace, transformed)
        if profile in AUTO_DISCOVERED_PROFILES and values and existing_placeholders:
            # A later open function must not rewrite placeholders introduced by
            # earlier functions.  Broad lexical regexes such as
            # \b[A-Za-z0-9_.-]+\b can otherwise match the inside of "<T>",
            # leaving side-stream values with no placeholder at decode time.
            rewrites_existing_placeholder = any(
                transformed.count(existing_placeholder) != previous_transformed.count(existing_placeholder)
                for existing_placeholder in existing_placeholders
            )
            captures_existing_placeholder = any(
                existing_placeholder in value
                for value in values
                for existing_placeholder in existing_placeholders
            )
            if captures_existing_placeholder or rewrites_existing_placeholder:
                transformed = previous_transformed
                continue
        if profile in AUTO_DISCOVERED_PROFILES and not values:
            continue
        values_by_tag[spec.tag] = values
        specs_meta.append({
            "tag": spec.tag,
            "kind": spec.kind,
            "placeholder": placeholder,
            "pattern": spec.pattern,
            "store_group": spec.store_group,
            "replacement": spec.replacement or "",
            "context_tag": spec.context_tag or "",
        })

    metadata = {
        "dataset": dataset,
        "profile": profile,
        "specs": specs_meta,
    }
    return transformed, values_by_tag, metadata


def rank_main_proxy_cost(text: str, numeric_radius: int = 8) -> int:
    encoder = base.RankModelEncoder(numeric_radius=numeric_radius)
    encoder.encode_text(text)
    pattern_payload = {
        str(pattern_id): {
            "pattern": info.pattern,
            "run_lengths": info.run_lengths,
            "separators": info.separators,
        }
        for pattern_id, info in encoder.numeric_pattern_infos.items()
    }
    parts = [
        encode_varint_stream_bytes(encoder.shape_rank_stream),
        bytes(encoder.shape_literal_stream),
        encode_varint_stream_bytes(encoder.string_rank_stream),
        encode_string_stream_bytes(encoder.string_literal_stream),
        encode_varint_stream_bytes(encoder.numeric_pattern_rank_stream),
        encode_varint_stream_bytes(encoder.numeric_rank_stream),
        encode_string_stream_bytes(encoder.numeric_literal_stream),
        json.dumps(pattern_payload, sort_keys=True).encode("utf-8"),
    ]
    metadata = {
        "version": 1,
        "numeric_radius": numeric_radius,
        "table_size": encoder.table_size,
        "shape_count": base.SHAPE_COUNT,
        "numeric_pattern_count": len(encoder.numeric_pattern_infos),
    }
    return compressed_parts_cost(parts) + len(lzma.compress(json.dumps(metadata, sort_keys=True).encode("utf-8")))


def autostruct_archive_proxy_cost(
    text: str,
    dataset: str,
    specs: list[ExtractSpec],
    main_proxy: str = "lzma",
) -> tuple[int, int]:
    transformed, values_by_tag, metadata = transform_text_with_specs(
        text,
        dataset=dataset,
        profile="auto_struct_mdl_v1",
        profile_specs=retag_extract_specs(specs),
    )
    main_text, line_dict_metadata, line_dict_templates, line_dict_side_ids = build_line_dictionary_text(
        transformed,
        min_support=2,
        max_entries=8192,
        id_mode="side_varint",
    )
    # Use an archive-level proxy after exact line-dictionary factoring, plus
    # selected side-stream costs and metadata. `rank` is slower but closer to
    # the deployed PARE RankModel; `lzma` is kept as an ablation.
    if main_proxy == "rank":
        cost = rank_main_proxy_cost(main_text)
    else:
        cost = len(lzma.compress(main_text.encode("latin-1")))
    if line_dict_metadata is not None:
        cost += compressed_parts_cost([
            encode_string_stream_bytes(line_dict_templates),
            encode_varint_stream_bytes(line_dict_side_ids),
        ])
        cost += len(json.dumps(line_dict_metadata, sort_keys=True).encode("utf-8"))

    specs_by_tag = {spec.tag: spec for spec in specs_from_metadata(metadata)}
    placeholders_by_tag = {
        str(spec_meta["tag"]): str(spec_meta["placeholder"])
        for spec_meta in metadata["specs"]
    }
    prior_tags: list[str] = []
    active_streams = 0
    for spec_meta in metadata["specs"]:
        tag = str(spec_meta["tag"])
        spec = specs_by_tag[tag]
        selected_spec = spec
        if not values_by_tag.get(tag):
            continue
        if spec.kind == "auto":
            selected, auto_meta = select_auto_stream_spec(
                spec,
                values_by_tag=values_by_tag,
                placeholders_by_tag=placeholders_by_tag,
                transformed_text=transformed,
                candidate_context_tags=prior_tags,
            )
            cost += int(auto_meta["auto_cost"])
            selected_spec = selected
            if selected.context_tag is not None:
                cost += 24
        else:
            values = values_by_tag[tag]
            cost += min(cost for _kind, cost in stream_codec_candidates(values))
        active_streams += 1
        # Context codecs such as grouped port deltas need their context stream
        # to be directly enumerable during restore.  Do not allow later streams
        # to use a conditional/context-coded stream as their own context.
        if selected_spec.kind not in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width", "size_from_prev_bytes"}:
            prior_tags.append(tag)
    cost += len(lzma.compress(json.dumps(metadata, sort_keys=True).encode("utf-8")))
    cost += 48 * active_streams
    return cost, active_streams


def select_open_function_specs_by_proxy(text: str, dataset: str, specs: list[ExtractSpec]) -> list[ExtractSpec]:
    """Greedily keep only verifier-passing functions with positive MDL proxy gain.

    This is intentionally generic: it does not know field names.  It estimates
    whether a proposed reversible source separation reduces total codelength
    after line-dictionary factoring, rank-model proxy compression, side-stream
    coding, and metadata overhead.
    """
    selected: list[ExtractSpec] = []
    current_cost, _active = autostruct_archive_proxy_cost(text, dataset, selected, main_proxy="lzma")
    for spec in specs:
        try:
            proposal_cost, _active = autostruct_archive_proxy_cost(text, dataset, selected + [spec], main_proxy="lzma")
        except Exception:
            continue
        # A small margin prevents accepting tiny, metadata-sensitive changes.
        if proposal_cost + 128 < current_cost:
            selected.append(spec)
            current_cost = proposal_cost
    return selected


def discover_autostruct_mdl_specs(
    text: str,
    dataset: str | None = None,
    main_proxy: str = "lzma",
) -> list[ExtractSpec]:
    sample_limit = 550_000 if main_proxy == "rank" else 1_250_000
    sample = text[:sample_limit] if len(text) > sample_limit else text
    pool = discover_autostruct_specs(
        sample,
        codec_aware=True,
        prefix_competition=True,
        paired_prefix_inner=True,
        numeric_phrases=True,
        relaxed_numeric_phrases=True,
        colon_terminal_tokens=True,
        max_specs=24 if main_proxy == "rank" else 32,
    )
    if not pool:
        return []

    selected: list[ExtractSpec] = []
    remaining = pool[:]
    current_cost, _streams = autostruct_archive_proxy_cost(sample, dataset or "", selected, main_proxy=main_proxy)
    # Greedy archive-level search over sets of transformations. This is still a
    # prototype, but it lets locally-negative fields survive if the full archive
    # proxy improves after main-stream and line-dictionary effects are included.
    max_selected = 10 if main_proxy == "rank" else 14
    while remaining and len(selected) < max_selected:
        best_index = -1
        best_cost = current_cost
        best_streams = 0
        for index, candidate in enumerate(remaining):
            trial_specs = selected + [candidate]
            try:
                trial_cost, trial_streams = autostruct_archive_proxy_cost(
                    sample,
                    dataset or "",
                    trial_specs,
                    main_proxy=main_proxy,
                )
            except Exception:
                continue
            if trial_cost + 16 < best_cost:
                best_index = index
                best_cost = trial_cost
                best_streams = trial_streams
        if best_index < 0:
            break
        selected.append(remaining.pop(best_index))
        current_cost = best_cost
        if best_streams == 0:
            break
    return retag_extract_specs(selected)


def active_specs_after_transform(
    text: str,
    dataset: str,
    profile: str,
    specs: list[ExtractSpec],
) -> list[ExtractSpec]:
    _transformed, values_by_tag, _metadata = transform_text_with_specs(
        text,
        dataset=dataset,
        profile=profile,
        profile_specs=specs,
    )
    return [spec for spec in specs if values_by_tag.get(spec.tag)]


def discover_autostruct_mdl_augmented_specs(text: str, dataset: str | None = None) -> list[ExtractSpec]:
    """Safely augment the strongest stat-only structure set with MDL search.

    The v1/v2 archive-level searches can replace good hierarchical structures
    with locally attractive but harmful alternatives. This variant uses v6 as a
    deterministic lower-bound structure, then only adds candidates that improve
    both train and held-out archive proxies.
    """
    sample_limit = 1_200_000
    sample = text[:sample_limit] if len(text) > sample_limit else text
    split = max(1, len(sample) // 2)
    train = sample[:split]
    valid = sample[split:] if split < len(sample) else sample[:split]

    baseline_specs = discover_autostruct_specs(
        train,
        codec_aware=True,
        prefix_competition=True,
        paired_prefix_inner=True,
        numeric_phrases=True,
        relaxed_numeric_phrases=True,
        colon_terminal_tokens=True,
        max_specs=10,
    )
    selected = active_specs_after_transform(train, dataset or "", "auto_struct_mdl_v3", baseline_specs)
    selected_patterns = {spec.pattern for spec in selected}

    pool = discover_autostruct_specs(
        sample,
        codec_aware=True,
        prefix_competition=True,
        paired_prefix_inner=True,
        numeric_phrases=True,
        relaxed_numeric_phrases=True,
        colon_terminal_tokens=True,
        max_specs=40,
    )
    remaining = [spec for spec in pool if spec.pattern not in selected_patterns]

    current_train, _ = autostruct_archive_proxy_cost(train, dataset or "", selected, main_proxy="lzma")
    current_valid, _ = autostruct_archive_proxy_cost(valid, dataset or "", selected, main_proxy="lzma")
    while remaining and len(selected) < 14:
        best_index = -1
        best_train = current_train
        best_valid = current_valid
        best_joint = current_train + current_valid
        for index, candidate in enumerate(remaining):
            trial = selected + [candidate]
            try:
                train_cost, _ = autostruct_archive_proxy_cost(train, dataset or "", trial, main_proxy="lzma")
                valid_cost, _ = autostruct_archive_proxy_cost(valid, dataset or "", trial, main_proxy="lzma")
            except Exception:
                continue
            if train_cost + 16 >= current_train:
                continue
            if valid_cost + 8 >= current_valid:
                continue
            joint = train_cost + valid_cost
            if joint < best_joint:
                best_index = index
                best_train = train_cost
                best_valid = valid_cost
                best_joint = joint
        if best_index < 0:
            break
        selected.append(remaining.pop(best_index))
        current_train = best_train
        current_valid = best_valid

    return retag_extract_specs(selected)


def discover_autostruct_joint_beam_specs(text: str, dataset: str | None = None) -> list[ExtractSpec]:
    """Beam-search extraction sets so correlated fields can be accepted jointly.

    Unlike v8, this function does not hard-code the final ordering with a large
    score bonus. It asks a broad lexical proposer for candidates, then searches
    over ordered candidate sets using the same archive proxy used by the side
    stream selector. Keeping a few temporarily worse states lets the next field
    provide context and turn a locally bad extraction into a global win.
    """
    sample_limit = 1_800_000
    sample = text[:sample_limit] if len(text) > sample_limit else text
    pool = discover_autostruct_specs(
        sample,
        codec_aware=True,
        prefix_competition=True,
        paired_prefix_inner=True,
        numeric_phrases=True,
        relaxed_numeric_phrases=True,
        colon_terminal_tokens=True,
        duration_phrases=True,
        paren_quantities=True,
        joint_context_promote=False,
        max_specs=48,
    )
    if not pool:
        return []

    cost_cache: dict[tuple[str, ...], int] = {}

    def state_key(specs: list[ExtractSpec]) -> tuple[str, ...]:
        return tuple(spec.pattern for spec in specs)

    def score(specs: list[ExtractSpec]) -> int:
        key = state_key(specs)
        cached = cost_cache.get(key)
        if cached is not None:
            return cached
        cost, _streams = autostruct_archive_proxy_cost(sample, dataset or "", specs, main_proxy="lzma")
        cost_cache[key] = cost
        return cost

    base_cost = score([])
    beam: list[tuple[int, list[ExtractSpec]]] = [(base_cost, [])]
    best_cost = base_cost
    best_specs: list[ExtractSpec] = []
    beam_width = 8
    max_selected = 14
    tolerance = max(2048, base_cost // 200)

    for _layer in range(max_selected):
        expansions: list[tuple[int, list[ExtractSpec]]] = []
        seen: set[tuple[str, ...]] = set()
        for _cost, specs in beam:
            selected_patterns = {spec.pattern for spec in specs}
            for candidate in pool:
                if candidate.pattern in selected_patterns:
                    continue
                trial = specs + [candidate]
                key = state_key(trial)
                if key in seen:
                    continue
                seen.add(key)
                try:
                    trial_cost = score(trial)
                except Exception:
                    continue
                if trial_cost < best_cost:
                    best_cost = trial_cost
                    best_specs = trial
                # Keep near-miss states so a later context-dependent stream can
                # compensate the temporary cost increase.
                if trial_cost <= best_cost + tolerance:
                    expansions.append((trial_cost, trial))
        if not expansions:
            break
        expansions.sort(key=lambda item: (item[0], len(item[1])))
        beam = expansions[:beam_width]

    return retag_extract_specs(best_specs)


@lru_cache(maxsize=262144)
def _template_cache_is_value_like(token: str) -> bool:
    """Return whether a token is worth routing through a template-slot cache.

    The first prototype intentionally focuses on digit-bearing tokens.  Pure
    punctuation/path strings can be valuable too, but they are also the easiest
    way to recreate the over-broad regex failures we are trying to avoid.
    """
    if re.search(r"<[A-Z][A-Z0-9]{0,3}>", token):
        return False
    return any(ch.isdigit() for ch in token)


@lru_cache(maxsize=262144)
def _template_cache_value_shape(token: str) -> str:
    if re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", token):
        return "IPV4"
    if re.fullmatch(r"\d{1,2}:\d{2}:\d{2}", token):
        return "HMS"
    if re.fullmatch(r"\d+", token):
        return "D+"
    parts: list[str] = []
    for match in re.finditer(r"\d+|[A-Za-z]+|\s+|.", token, flags=re.DOTALL):
        part = match.group(0)
        if part.isdigit():
            parts.append("D+")
        elif part.isalpha():
            parts.append("A")
        elif part.isspace():
            parts.append("S")
        else:
            parts.append(part)
    return "".join(parts)[:64]


def _template_cache_stable_token(token: str) -> str:
    if _template_cache_is_value_like(token):
        return f"<{_template_cache_value_shape(token)}>"
    if len(token) <= 40:
        return token
    return f"<LIT{len(token)}>"


@lru_cache(maxsize=262144)
def _template_cache_line_tokens(line: str) -> tuple[re.Match[str], ...]:
    return tuple(re.finditer(r"\S+", line))


def _template_cache_template_key(line: str) -> tuple[str, ...]:
    return tuple(_template_cache_stable_token(match.group(0)) for match in _template_cache_line_tokens(line))


def _template_cache_family_key(line: str, slot_index: int) -> tuple[tuple[str, ...], int, str]:
    tokens = _template_cache_line_tokens(line)
    token = tokens[slot_index].group(0)
    return (_template_cache_template_key(line), slot_index, _template_cache_value_shape(token))


def _template_cache_literal_safe(text: str) -> bool:
    return "{" not in text and "}" not in text


def _template_cache_infer_fixed_numeric_program(values: list[str]) -> dict[str, object] | None:
    """Infer a reversible int_expr_delta program for one token-shaped family.

    This is the deterministic counterpart of the LLM function proposal step:
    if every value has the same non-digit skeleton, the slot can be represented
    by one integer and rendered back exactly from the frozen program.
    """
    if len(values) < 2:
        return None
    split_values = [re.split(r"(\d+)", value) for value in values]
    first = split_values[0]
    if not any(part.isdigit() for part in first):
        return None
    if any(len(parts) != len(first) for parts in split_values):
        return None

    digit_positions = [index for index, part in enumerate(first) if part.isdigit()]
    if not digit_positions or len(digit_positions) > 4:
        return None

    for index, literal in enumerate(first):
        if index in digit_positions:
            continue
        if not _template_cache_literal_safe(literal):
            return None
        if any(parts[index] != literal for parts in split_values):
            return None

    widths_by_pos: dict[int, set[int]] = {
        index: {len(parts[index]) for parts in split_values}
        for index in digit_positions
    }
    fixed_widths = {index: next(iter(widths)) for index, widths in widths_by_pos.items() if len(widths) == 1}
    has_leading_zero = {
        index: any(len(parts[index]) > 1 and parts[index].startswith("0") for parts in split_values)
        for index in digit_positions
    }
    if any(has_leading_zero[index] and index not in fixed_widths for index in digit_positions):
        return None
    if len(digit_positions) > 1 and len(fixed_widths) != len(digit_positions):
        return None

    regex_parts: list[str] = ["^"]
    render_parts: list[str] = []
    expr_terms: list[str] = []
    remaining_width = sum(fixed_widths.get(index, 0) for index in digit_positions)
    group_index = 1
    for part_index, part in enumerate(first):
        if part_index not in digit_positions:
            regex_parts.append(re.escape(part))
            render_parts.append(part)
            continue
        width = fixed_widths.get(part_index)
        if width is None:
            regex_parts.append(r"(\d+)")
            render_parts.append("{v}")
            expr_terms.append(f"g{group_index}")
        else:
            regex_parts.append(rf"(\d{{{width}}})")
            remaining_width -= width
            scale = 10 ** remaining_width
            if scale == 1:
                expr_terms.append(f"g{group_index}")
            else:
                expr_terms.append(f"g{group_index} * {scale}")
            if len(digit_positions) == 1:
                fmt = f"0{width}d" if has_leading_zero[part_index] else "d"
                render_parts.append("{v" + (f":{fmt}" if fmt != "d" else "") + "}")
            else:
                piece_expr = f"(v // {scale}) % {10 ** width}" if scale != 1 else f"v % {10 ** width}"
                fmt = f"0{width}d"
                render_parts.append("{" + piece_expr + f":{fmt}" + "}")
        group_index += 1
    regex_parts.append("$")

    if len(digit_positions) == 1 and digit_positions[0] not in fixed_widths:
        value_expr = expr_terms[0]
    else:
        value_expr = " + ".join(expr_terms)

    program = {
        "op": "int_expr_delta",
        "value_regex": "".join(regex_parts),
        "value_expr": value_expr,
        "render": "".join(render_parts),
    }

    try:
        for value in values[: min(len(values), 256)]:
            if render_open_function_exact(value, program) != value:
                return None
    except Exception:
        return None
    return program


def _template_cache_candidate_spec(tag: str, values: list[str], family_key: tuple[tuple[str, ...], int, str]) -> ExtractSpec | None:
    distinct = len(set(values))
    if distinct < 2:
        return None
    pattern = "template-slot:" + json.dumps(
        {
            "slot": family_key[1],
            "shape": family_key[2],
            "template": list(family_key[0])[:64],
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )

    program = _template_cache_infer_fixed_numeric_program(values)
    raw_cost = compressed_parts_cost([encode_string_stream_bytes(values)])
    best_kind, best_cost = min(stream_codec_candidates(values), key=lambda item: item[1])

    if program is not None and any(not value.isdigit() for value in values):
        try:
            open_values = [parse_open_function_value(value, program) for value in values]
            open_cost = compressed_parts_cost([encode_delta_values(open_values)])
        except Exception:
            open_cost = raw_cost + 1
        if open_cost + CONDITIONAL_CODEC_MODEL_COST + 128 < raw_cost:
            return ExtractSpec(
                tag=tag,
                pattern=pattern,
                kind="open_function",
                store_group=0,
                replacement=None,
                context_tag=json.dumps(program, sort_keys=True),
            )

    if best_kind == "string":
        return None
    if best_cost + CONDITIONAL_CODEC_MODEL_COST + 128 >= raw_cost:
        return None

    return ExtractSpec(tag=tag, pattern=pattern, kind="auto")


def _template_cache_phrase_specs() -> list[ExtractSpec]:
    bracket_datetime_program = {
        "op": "datetime_strptime_delta",
        "format": "[%a %b %d %H:%M:%S %Y]",
        "render_format": "[%a %b %d %H:%M:%S %Y]",
    }
    return [
        ExtractSpec(
            "M0",
            rf"\[{WEEKDAY_RE}\s+{MONTH_RE}\s+\d{{2}}\s+\d{{2}}:\d{{2}}:\d{{2}}\s+\d{{4}}\]",
            "open_function",
            context_tag=json.dumps(bracket_datetime_program, sort_keys=True),
        ),
        ExtractSpec(
            "M1",
            rf"(?m)^({MONTH_RE}\s+\d{{1,2}}\s+\d{{2}}:\d{{2}}:\d{{2}})",
            "syslog_delta",
            store_group=1,
            replacement="{placeholder}",
        ),
        ExtractSpec(
            "M2",
            r"\[(\d{2}\.\d{2} \d{2}:\d{2}:\d{2})\]",
            "month_day_hms_delta",
            store_group=1,
            replacement="[{placeholder}]",
        ),
        ExtractSpec("M3", r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "auto"),
        ExtractSpec(
            "M4",
            (
                r"(?<![\w.-])"
                r"(?:(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+|(?:\d{1,3}\.){3}\d{1,3})"
                r":\d{1,5}(?![\w.-])"
            ),
            "auto",
        ),
    ]


def transform_text_template_cache(
    text: str,
    dataset: str,
    profile: str = "auto_template_cache_v1",
) -> tuple[str, dict[str, list[str]], dict[str, object]]:
    """Template-indexed value extraction.

    This realizes the user's cache idea without making the decoder depend on a
    live cache: the encoder discovers frequent template-slot families, freezes
    admitted codecs as metadata, and replaces only those exact slots.
    """
    working_text = text
    initial_values_by_tag: dict[str, list[str]] = {}
    initial_specs_meta: list[dict[str, object]] = []
    if profile == "auto_template_cache_v2":
        working_text, initial_values_by_tag, initial_metadata = transform_text_with_specs(
            text,
            dataset=dataset,
            profile=profile,
            profile_specs=_template_cache_phrase_specs(),
        )
        initial_specs_meta = list(initial_metadata["specs"])

    lines = split_physical_lines(working_text)
    line_count = max(1, len(lines))
    min_support = int(os.environ.get("PARE_TEMPLATE_CACHE_MIN_SUPPORT", str(max(20, min(200, line_count // 1000)))))
    max_specs = int(os.environ.get("PARE_TEMPLATE_CACHE_MAX_SPECS", "28"))

    family_values: dict[tuple[tuple[str, ...], int, str], list[str]] = {}
    for line in lines:
        tokens = _template_cache_line_tokens(line)
        if not tokens:
            continue
        template_key = _template_cache_template_key(line)
        for slot_index, match in enumerate(tokens):
            token = match.group(0)
            if not _template_cache_is_value_like(token):
                continue
            family_key = (template_key, slot_index, _template_cache_value_shape(token))
            family_values.setdefault(family_key, []).append(token)

    candidates: list[tuple[int, int, tuple[tuple[str, ...], int, str], ExtractSpec]] = []
    used_tags: set[str] = set()
    next_tag = 0
    for family_key, values in family_values.items():
        count = len(values)
        if count < min_support:
            continue
        distinct = len(set(values))
        if distinct < 2:
            continue
        tag = f"Q{next_tag}"
        while tag in used_tags:
            next_tag += 1
            tag = f"Q{next_tag}"
        spec = _template_cache_candidate_spec(tag, values, family_key)
        if spec is None:
            continue
        used_tags.add(tag)
        next_tag += 1
        # Prefer high-support, compact families.  Very high distinctness often
        # means random IDs where the main stream may be better left to PARE.
        candidates.append((count, distinct, family_key, spec))

    candidates.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    selected = candidates[:max_specs]
    spec_by_family = {family_key: spec for _count, _distinct, family_key, spec in selected}

    values_by_tag: dict[str, list[str]] = dict(initial_values_by_tag)
    values_by_tag.update({spec.tag: [] for spec in spec_by_family.values()})
    placeholders = {
        spec.tag: choose_placeholder(text, dataset, spec.tag)
        for spec in spec_by_family.values()
    }

    transformed_lines: list[str] = []
    for line in lines:
        tokens = _template_cache_line_tokens(line)
        if not tokens:
            transformed_lines.append(line)
            continue
        template_key = _template_cache_template_key(line)
        pieces: list[str] = []
        cursor = 0
        for slot_index, match in enumerate(tokens):
            pieces.append(line[cursor:match.start()])
            token = match.group(0)
            family_key = (template_key, slot_index, _template_cache_value_shape(token))
            spec = spec_by_family.get(family_key)
            if spec is None:
                pieces.append(token)
            else:
                values_by_tag[spec.tag].append(token)
                pieces.append(placeholders[spec.tag])
            cursor = match.end()
        pieces.append(line[cursor:])
        transformed_lines.append("".join(pieces))

    specs_meta = initial_specs_meta + [
        {
            "tag": spec.tag,
            "kind": spec.kind,
            "placeholder": placeholders[spec.tag],
            "pattern": spec.pattern,
            "store_group": spec.store_group,
            "replacement": spec.replacement or "",
            "context_tag": spec.context_tag or "",
        }
        for _count, _distinct, _family_key, spec in selected
        if values_by_tag.get(spec.tag)
    ]
    metadata = {
        "dataset": dataset,
        "profile": profile,
        "template_cache": {
            "min_support": min_support,
            "max_specs": max_specs,
            "candidate_families": len(candidates),
            "selected_families": len(specs_meta),
        },
        "specs": specs_meta,
    }
    return "".join(transformed_lines), values_by_tag, metadata


@lru_cache(maxsize=262144)
def _line_transducer_is_placeholder(token: str) -> bool:
    """Avoid treating already-inserted extract placeholders as new variables."""
    return bool(re.search(r"<[A-Z][A-Z0-9]{0,3}>", token))


def _cache_lattice_header_shape(token: str) -> str:
    """Generalized token shape for discovering recurring line headers."""
    if _line_transducer_is_placeholder(token):
        return token
    if any(ch.isdigit() for ch in token):
        return _template_cache_value_shape(token)
    if re.fullmatch(r"[A-Z]", token):
        return "A1"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*:", token):
        return "A:"
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", token):
        return "A"
    return token.lower()[:32]


def _cache_lattice_header_prefix_length(tokens: list[str]) -> int:
    """Return a likely structured-header length, or zero if none is present."""
    max_scan = min(len(tokens), 10)
    for index in range(3, max_scan):
        token = tokens[index]
        if token.endswith(":") and any(ch.isalpha() for ch in token):
            return index + 1
    return 0


def _cache_lattice_mmdd_millis_program() -> dict[str, object]:
    day_ms = 24 * 60 * 60 * 1000
    return {
        "op": "int_expr_delta",
        "value_regex": r"^(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})$",
        "value_expr": "(((((g1 * 100 + g2) * 24 + g3) * 60 + g4) * 60 + g5) * 1000 + g6)",
        "render": (
            "{(v // " + str(day_ms) + ") // 100:02d}-"
            "{(v // " + str(day_ms) + ") % 100:02d} "
            "{((v % " + str(day_ms) + ") // 3600000):02d}:"
            "{((v % 3600000) // 60000):02d}:"
            "{((v % 60000) // 1000):02d}."
            "{(v % 1000):03d}"
        ),
    }


NUMERIC_LATTICE_VALUE_RE = re.compile(r"\b\d+\.\d+\b|\b\d+\b")


class NumericLatticeOccurrence:
    """Lightweight numeric occurrence record.

    HealthApp-sized blocks can create close to one million occurrences.  A
    normal dataclass instance carries a per-object ``__dict__`` on Python 3.9,
    which makes the numeric-lattice scan and family construction memory-heavy.
    ``__slots__`` keeps the record byte-identical semantically while reducing
    allocation and attribute-access overhead.
    """

    __slots__ = (
        "line_index",
        "start",
        "end",
        "value",
        "kind",
        "value_slot",
        "template_key",
        "prefix_key",
        "left_anchor",
    )

    def __init__(
        self,
        line_index: int,
        start: int,
        end: int,
        value: str,
        kind: str,
        value_slot: int,
        template_key: str,
        prefix_key: str,
        left_anchor: str = "",
    ) -> None:
        self.line_index = line_index
        self.start = start
        self.end = end
        self.value = value
        self.kind = kind
        self.value_slot = value_slot
        self.template_key = template_key
        self.prefix_key = prefix_key
        self.left_anchor = left_anchor


def _numeric_lattice_value_kind(value: str) -> str:
    return "float" if "." in value else "int"


def _numeric_lattice_shape(value: str) -> str:
    return "<F>" if _numeric_lattice_value_kind(value) == "float" else "<I>"


def _numeric_lattice_line_tokens(line: str) -> list[str]:
    return [match.group(0) for match in _template_cache_line_tokens(line)]


def _numeric_lattice_normalize_token(token: str) -> str:
    if _line_transducer_is_placeholder(token):
        return token
    if NUMERIC_LATTICE_VALUE_RE.search(token):
        return NUMERIC_LATTICE_VALUE_RE.sub(lambda match: _numeric_lattice_shape(match.group(0)), token)
    if len(token) > 40:
        return f"<LIT{len(token)}>"
    return token


def _numeric_lattice_left_anchor_for_match(
    line: str,
    token_matches: list[re.Match[str]],
    match_start: int,
) -> str:
    """Return a compact left context key for numeric fallback routing."""
    previous_token = "<BOL>"
    for token_match in token_matches:
        token = token_match.group(0)
        if token_match.start() <= match_start < token_match.end():
            local_prefix = line[token_match.start():match_start]
            if local_prefix:
                return _numeric_lattice_normalize_token(local_prefix)
            return _numeric_lattice_normalize_token(previous_token)
        if token_match.end() <= match_start:
            previous_token = token
            continue
        break
    return _numeric_lattice_normalize_token(previous_token)


def _numeric_lattice_template_key(line: str) -> str:
    tokens = [_numeric_lattice_normalize_token(token) for token in _numeric_lattice_line_tokens(line)]
    payload = "\x1f".join(tokens[:96])
    return hashlib.sha1(payload.encode("latin-1", errors="ignore")).hexdigest()[:12]


def _numeric_lattice_prefix_key(line: str, max_tokens: int = 4) -> str:
    tokens = [_numeric_lattice_normalize_token(token) for token in _numeric_lattice_line_tokens(line)[:max_tokens]]
    payload = "\x1f".join(tokens)
    return hashlib.sha1(payload.encode("latin-1", errors="ignore")).hexdigest()[:12]


def _numeric_lattice_template_and_prefix_key(line: str, max_prefix_tokens: int = 4) -> tuple[str, str]:
    tokens = [_numeric_lattice_normalize_token(match.group(0)) for match in _template_cache_line_tokens(line)]
    return _numeric_lattice_keys_from_normalized_tokens(tokens, max_prefix_tokens=max_prefix_tokens)


def _numeric_lattice_keys_from_normalized_tokens(tokens: list[str], max_prefix_tokens: int = 4) -> tuple[str, str]:
    template_payload = "\x1f".join(tokens[:96])
    prefix_payload = "\x1f".join(tokens[:max_prefix_tokens])
    return (
        hashlib.sha1(template_payload.encode("latin-1", errors="ignore")).hexdigest()[:12],
        hashlib.sha1(prefix_payload.encode("latin-1", errors="ignore")).hexdigest()[:12],
    )


def _numeric_lattice_best_stream_cost(values: list[str]) -> int:
    if not values:
        return 0
    forced_kinds: set[str] | None = None
    if (
        os.environ.get("PARE_NUMERIC_LATTICE_SCORING_NUMERIC_ONLY", "0") == "1"
        and all(re.fullmatch(r"-?\d+", value) for value in values[: min(len(values), 4096)])
    ):
        forced_kinds = {
            "int_abs",
            "int_abs_width",
            "int_abs_fixed_width",
            "int_delta_width",
            "int_delta_fixed_width",
            "uint16_split_width",
            "int_denum_bucket_delta",
        }
    max_sample = int(os.environ.get("PARE_NUMERIC_LATTICE_COST_SAMPLE", "20000"))
    if len(values) > max_sample:
        step = max(1, len(values) // max_sample)
        sample = values[::step][:max_sample]
        sample_cost = min(stream_codec_candidates(sample, forced_kinds=forced_kinds), key=lambda item: item[1])[1]
        return int(sample_cost * (len(values) / max(1, len(sample))))
    return min(stream_codec_candidates(values, forced_kinds=forced_kinds), key=lambda item: item[1])[1]


def _varint_len(value: int) -> int:
    count = 1
    while value >= 0x80:
        value >>= 7
        count += 1
    return count


def _numeric_lattice_surrogate_gain(values: list[str]) -> int:
    """Cheap feature-MDL score used only to shortlist exact split candidates.

    The exact codec still decides admission.  This surrogate estimates whether
    a family has the properties that our downstream codecs exploit: compact
    numeric width, small deltas, or low categorical entropy.  It intentionally
    uses bounded samples so candidate ranking is fast and dataset-independent.
    """
    if not values:
        return 0
    max_sample = int(os.environ.get("PARE_NUMERIC_LATTICE_FRONTIER_SAMPLE", "4096"))
    if len(values) > max_sample:
        step = max(1, len(values) // max_sample)
        sample = values[::step][:max_sample]
        scale = len(values) / max(1, len(sample))
    else:
        sample = values
        scale = 1.0

    raw_cost = sum(len(value.encode("latin-1", errors="ignore")) + 1 for value in sample)
    distinct_cost = len(set(sample)) * max(1, sum(len(value) for value in sample) // max(1, len(sample)))
    numeric_cost: int | None = None
    try:
        if all(re.fullmatch(r"-?\d+(?:\.\d+)?", value) for value in sample):
            encoded_values: list[int] = []
            widths_cost = 0
            for value in sample:
                if "." in value:
                    encoded_values.append(int(value.replace(".", "")))
                    widths_cost += 2
                else:
                    encoded_values.append(int(value))
                    widths_cost += 1
            delta_cost = 0
            last_value: int | None = None
            for value in encoded_values:
                delta = value if last_value is None else value - last_value
                delta_cost += _varint_len(zigzag_encode(delta))
                last_value = value
            numeric_cost = delta_cost + widths_cost
    except Exception:
        numeric_cost = None

    model_cost = 32
    best_cost = min(raw_cost, distinct_cost)
    if numeric_cost is not None:
        best_cost = min(best_cost, numeric_cost)
    return int((raw_cost - best_cost - model_cost) * scale)


def _numeric_lattice_tag(prefix: str, index: int, used_tags: set[str]) -> str:
    for tag in [f"{prefix}{index}", f"{prefix}{index % 1000:03d}"]:
        if len(tag) <= 4 and tag not in used_tags:
            used_tags.add(tag)
            return tag
    cursor = 0
    while True:
        tag = f"Y{cursor}"
        if len(tag) <= 4 and tag not in used_tags:
            used_tags.add(tag)
            return tag
        cursor += 1


def _numeric_lattice_occurrences(lines: list[str]) -> list[NumericLatticeOccurrence]:
    occurrences: list[NumericLatticeOccurrence] = []
    token_seconds = 0.0
    regex_seconds = 0.0
    profile = OPERATION_PROFILE
    for line_index, line in enumerate(lines):
        op_start = time.perf_counter() if profile else 0.0
        token_matches = list(_template_cache_line_tokens(line))
        normalized_tokens = [_numeric_lattice_normalize_token(match.group(0)) for match in token_matches]
        template_key_value, prefix_key_value = _numeric_lattice_keys_from_normalized_tokens(normalized_tokens)
        if profile:
            token_seconds += time.perf_counter() - op_start
        value_slot = 0
        op_start = time.perf_counter() if profile else 0.0
        for match in NUMERIC_LATTICE_VALUE_RE.finditer(line):
            occurrences.append(NumericLatticeOccurrence(
                line_index=line_index,
                start=match.start(),
                end=match.end(),
                value=match.group(0),
                kind=_numeric_lattice_value_kind(match.group(0)),
                value_slot=value_slot,
                template_key=template_key_value,
                prefix_key=prefix_key_value,
                left_anchor=_numeric_lattice_left_anchor_for_match(line, token_matches, match.start()),
            ))
            value_slot += 1
        if profile:
            regex_seconds += time.perf_counter() - op_start
    if profile:
        record_operation_timing("numeric_lattice.occurrences.tokenize_template", token_seconds, count=len(lines))
        record_operation_timing("numeric_lattice.occurrences.regex_scan", regex_seconds, count=len(lines))
    return occurrences


def _numeric_lattice_occurrences_for_route_replay(lines: list[str]) -> list[NumericLatticeOccurrence]:
    occurrences: list[NumericLatticeOccurrence] = []
    regex_seconds = 0.0
    profile = OPERATION_PROFILE
    for line_index, line in enumerate(lines):
        value_slot = 0
        token_matches = list(_template_cache_line_tokens(line))
        op_start = time.perf_counter() if profile else 0.0
        for match in NUMERIC_LATTICE_VALUE_RE.finditer(line):
            occurrences.append(NumericLatticeOccurrence(
                line_index=line_index,
                start=match.start(),
                end=match.end(),
                value=match.group(0),
                kind=_numeric_lattice_value_kind(match.group(0)),
                value_slot=value_slot,
                template_key="",
                prefix_key="",
                left_anchor=_numeric_lattice_left_anchor_for_match(line, token_matches, match.start()),
            ))
            value_slot += 1
        if profile:
            regex_seconds += time.perf_counter() - op_start
    if profile:
        record_operation_timing("numeric_lattice.route_replay.regex_scan", regex_seconds, count=len(lines))
    return occurrences


def _numeric_lattice_select_groups(
    occurrences: list[NumericLatticeOccurrence],
    kind: str,
    min_support: int,
    max_groups: int,
    fallback_mode: str = "global",
    frontier_topk: int = 0,
    fast_select: bool = False,
    onepass_exact_select: bool = False,
    skip_group_codec_cost: bool = False,
) -> tuple[dict[int, str], list[dict[str, object]]]:
    profile = OPERATION_PROFILE
    select_start = time.perf_counter() if profile else 0.0
    kind_occurrences = [item for item in occurrences if item.kind == kind]
    if not kind_occurrences:
        return {}, []

    op_start = time.perf_counter() if profile else 0.0
    index_by_occurrence = {id(item): index for index, item in enumerate(kind_occurrences)}
    values = [item.value for item in kind_occurrences]
    assigned = [False] * len(kind_occurrences)
    selected: list[tuple[str, list[int], int, str]] = []

    family_members: dict[tuple[str, str, int], list[int]] = {}
    for item in kind_occurrences:
        index = index_by_occurrence[id(item)]
        # Exact slot families capture stable message templates. Prefix-slot
        # families let repeated headers such as timestamp pid tid level merge
        # across many message bodies without naming Android or PID explicitly.
        family_members.setdefault(("slot", item.template_key, item.value_slot), []).append(index)
        family_members.setdefault(("prefix", item.prefix_key, item.value_slot), []).append(index)
    if profile:
        record_operation_timing(f"numeric_lattice.select.{kind}.build_families", time.perf_counter() - op_start, count=len(kind_occurrences))

    op_start = time.perf_counter() if profile else 0.0
    candidate_items: list[tuple[int, tuple[str, str, int], list[int]]] = []
    total = len(kind_occurrences)
    for family_key, indices in family_members.items():
        if len(indices) < min_support or len(indices) >= total:
            continue
        if len({values[index] for index in indices}) < 2:
            continue
        candidate_items.append((len(indices), family_key, indices))
    candidate_items.sort(reverse=True, key=lambda item: item[0])
    candidate_items = candidate_items[: int(os.environ.get("PARE_NUMERIC_LATTICE_MAX_CANDIDATES", "32"))]
    if profile:
        record_operation_timing(f"numeric_lattice.select.{kind}.candidate_filter", time.perf_counter() - op_start, count=len(candidate_items))
    if frontier_topk > 0 and len(candidate_items) > frontier_topk:
        op_start = time.perf_counter() if profile else 0.0
        scored_candidate_items: list[tuple[int, int, tuple[str, str, int], list[int]]] = []
        for _count, family_key, indices in candidate_items:
            gain_proxy = _numeric_lattice_surrogate_gain([values[index] for index in indices])
            scored_candidate_items.append((gain_proxy, len(indices), family_key, indices))
        scored_candidate_items.sort(reverse=True, key=lambda item: (item[0], item[1]))
        candidate_items = [
            (support, family_key, indices)
            for _gain_proxy, support, family_key, indices in scored_candidate_items[:frontier_topk]
        ]
        if profile:
            record_operation_timing(f"numeric_lattice.select.{kind}.frontier_score", time.perf_counter() - op_start, count=len(candidate_items))

    model_cost = int(os.environ.get("PARE_NUMERIC_LATTICE_MODEL_COST", "96"))
    min_gain = int(os.environ.get("PARE_NUMERIC_LATTICE_MIN_GAIN", "192"))
    residual_fraction = float(os.environ.get("PARE_RESIDUAL_STREAM_MIN_BLOCK_FRACTION", "0") or 0)
    cost_workers = int(os.environ.get("PARE_NUMERIC_LATTICE_COST_WORKERS", "1"))
    cost_cache: dict[tuple[int, ...], int] = {}
    cost_cache_lock = threading.Lock() if cost_workers > 1 else None
    max_cost_sample = int(os.environ.get("PARE_NUMERIC_LATTICE_COST_SAMPLE", "20000"))
    lazy_complement_cost = os.environ.get("PARE_NUMERIC_LATTICE_LAZY_COMPLEMENT", "0") == "1"

    def cost_cache_key(indices: list[int]) -> tuple[int, ...]:
        if len(indices) <= max_cost_sample:
            return (0, *indices)
        step = max(1, len(indices) // max_cost_sample)
        sampled_indices = indices[::step][:max_cost_sample]
        return (1, len(indices), step, *sampled_indices)

    def best_stream_cost_for_indices(indices: list[int]) -> int:
        if not indices:
            return 0
        if len(indices) > max_cost_sample:
            step = max(1, len(indices) // max_cost_sample)
            sample = [values[indices[pos]] for pos in range(0, len(indices), step)][:max_cost_sample]
            sample_cost = min(stream_codec_candidates(sample), key=lambda item: item[1])[1]
            return int(sample_cost * (len(indices) / max(1, len(sample))))
        return _numeric_lattice_best_stream_cost([values[index] for index in indices])

    def cost_for_indices(indices: list[int]) -> int:
        if not indices:
            return 0
        key = cost_cache_key(indices)
        if cost_cache_lock is None:
            cached = cost_cache.get(key)
            if cached is not None:
                return cached
            cost = best_stream_cost_for_indices(indices)
            cost_cache[key] = cost
            return cost
        with cost_cache_lock:
            cached = cost_cache.get(key)
            if cached is not None:
                return cached
        cost = best_stream_cost_for_indices(indices)
        with cost_cache_lock:
            cost_cache[key] = cost
        return cost

    def cost_for_complement_indices(global_indices: list[int], excluded: set[int], remaining_len: int) -> int:
        """Score a complement stream without materializing it when sampled.

        For large streams, ``best_stream_cost_for_indices`` already uses
        ``indices[::step][:max_cost_sample]`` and scales the result.  Building
        the full complement for every candidate therefore wastes time and
        memory.  This helper emits the same sampled complement sequence in one
        pass, while preserving exact scoring for small complements.
        """
        if remaining_len <= 0:
            return 0
        if remaining_len <= max_cost_sample:
            return cost_for_indices([index for index in global_indices if index not in excluded])
        step = max(1, remaining_len // max_cost_sample)
        sampled_indices: list[int] = []
        remaining_seen = 0
        for index in global_indices:
            if index in excluded:
                continue
            if remaining_seen % step == 0:
                sampled_indices.append(index)
                if len(sampled_indices) >= max_cost_sample:
                    break
            remaining_seen += 1
        if not sampled_indices:
            return 0
        sample_cost = cost_for_indices(sampled_indices)
        return int(sample_cost * (remaining_len / max(1, len(sampled_indices))))

    if fast_select:
        op_start = time.perf_counter() if profile else 0.0
        scored_candidate_items: list[tuple[int, int, tuple[str, str, int], list[int]]] = []
        for support, family_key, indices in candidate_items:
            gain_proxy = _numeric_lattice_surrogate_gain([values[index] for index in indices])
            scored_candidate_items.append((gain_proxy, support, family_key, indices))
        scored_candidate_items.sort(reverse=True, key=lambda item: (item[0], item[1]))
        for gain_proxy, _support, family_key, raw_indices in scored_candidate_items:
            if len(selected) >= max_groups:
                break
            indices = [index for index in raw_indices if not assigned[index]]
            if len(indices) < min_support or gain_proxy < min_gain:
                continue
            family_kind, family_hash, value_slot = family_key
            label = f"{family_kind}:{family_hash}:{value_slot}"
            for index in indices:
                assigned[index] = True
            selected.append((label, indices, int(gain_proxy), family_kind))
        if profile:
            record_operation_timing(f"numeric_lattice.select.{kind}.fast_select", time.perf_counter() - op_start, count=len(selected))

    if onepass_exact_select and not fast_select:
        op_start = time.perf_counter() if profile else 0.0
        global_indices = [index for index, is_assigned in enumerate(assigned) if not is_assigned]
        global_index_set = set(global_indices)
        before_cost = cost_for_indices(global_indices)
        scored_splits: list[tuple[int, int, str, list[int], str]] = []
        for _count, family_key, raw_indices in candidate_items:
            indices = [index for index in raw_indices if index in global_index_set]
            if len(indices) < min_support:
                continue
            index_set = set(indices)
            remaining = [index for index in global_indices if index not in index_set]
            if not remaining:
                continue
            if lazy_complement_cost:
                remaining_len = len(global_indices) - len(index_set)
                remaining_cost = cost_for_complement_indices(global_indices, index_set, remaining_len)
            else:
                remaining_cost = cost_for_indices(remaining)
            split_cost = (
                remaining_cost
                + cost_for_indices(indices)
                + model_cost
            )
            gain = before_cost - split_cost
            if gain < min_gain:
                continue
            family_kind, family_hash, value_slot = family_key
            label = f"{family_kind}:{family_hash}:{value_slot}"
            scored_splits.append((gain, len(indices), label, indices, family_kind))
        scored_splits.sort(reverse=True, key=lambda item: (item[0], item[1]))
        for gain, _support, label, raw_indices, family_kind in scored_splits:
            if len(selected) >= max_groups:
                break
            indices = [index for index in raw_indices if not assigned[index]]
            if len(indices) < min_support:
                continue
            for index in indices:
                assigned[index] = True
            selected.append((label, indices, gain, family_kind))
        if profile:
            record_operation_timing(f"numeric_lattice.select.{kind}.onepass_exact", time.perf_counter() - op_start, count=len(selected))

    exact_greedy_seconds = 0.0
    exact_greedy_rounds = 0
    exact_frontier_topk = int(os.environ.get("PARE_NUMERIC_LATTICE_EXACT_FRONTIER_TOPK", "0"))
    while not fast_select and not onepass_exact_select and len(selected) < max_groups:
        op_start = time.perf_counter() if profile else 0.0
        global_indices = [index for index, is_assigned in enumerate(assigned) if not is_assigned]
        if not global_indices:
            break
        exact_score_sample = int(os.environ.get("PARE_NUMERIC_LATTICE_EXACT_SCORE_SAMPLE", "0"))
        sampled_scoring = exact_score_sample > 0 and len(global_indices) > exact_score_sample
        if sampled_scoring:
            if exact_score_sample == 1:
                scoring_global_indices = [global_indices[0]]
            else:
                last_global = len(global_indices) - 1
                scoring_global_indices = [
                    global_indices[round(pos * last_global / (exact_score_sample - 1))]
                    for pos in range(exact_score_sample)
                ]
            scoring_global_set = set(scoring_global_indices)
            scoring_scale = len(global_indices) / max(1, len(scoring_global_indices))
            before_cost = cost_for_indices(scoring_global_indices)
        else:
            scoring_global_indices = global_indices
            scoring_global_set = set(global_indices)
            scoring_scale = 1.0
            before_cost = cost_for_indices(global_indices)
        best_gain = 0
        best_item: tuple[str, list[int], int, str] | None = None

        round_candidate_items = candidate_items
        if exact_frontier_topk > 0 and len(candidate_items) > exact_frontier_topk:
            frontier_start = time.perf_counter() if profile else 0.0
            frontier_items: list[tuple[int, int, tuple[int, tuple[str, str, int], list[int]]]] = []
            for item in candidate_items:
                _count, _family_key, raw_indices = item
                indices = [index for index in raw_indices if not assigned[index]]
                if len(indices) < min_support:
                    continue
                gain_proxy = _numeric_lattice_surrogate_gain([values[index] for index in indices])
                frontier_items.append((gain_proxy, len(indices), item))
            frontier_items.sort(reverse=True, key=lambda item: (item[0], item[1]))
            round_candidate_items = [item for _gain_proxy, _support, item in frontier_items[:exact_frontier_topk]]
            if profile:
                record_operation_timing(
                    f"numeric_lattice.select.{kind}.exact_frontier",
                    time.perf_counter() - frontier_start,
                    count=len(round_candidate_items),
                )

        def evaluate_candidate(item: tuple[int, tuple[str, str, int], list[int]]) -> tuple[int, tuple[str, str, int], list[int]] | None:
            _count, family_key, raw_indices = item
            indices = [index for index in raw_indices if not assigned[index]]
            if len(indices) < min_support:
                return None
            if sampled_scoring:
                sampled_indices = [index for index in indices if index in scoring_global_set]
                sample_min_support = max(8, int(min_support / max(1.0, scoring_scale)))
                if len(sampled_indices) < sample_min_support:
                    return None
                index_set = set(sampled_indices)
                remaining = [index for index in scoring_global_indices if index not in index_set]
                scored_indices = sampled_indices
                if not remaining:
                    return None
                remaining_cost = cost_for_indices(remaining)
            else:
                index_set = set(indices)
                remaining = [index for index in global_indices if index not in index_set]
                scored_indices = indices
                if not remaining:
                    return None
                if lazy_complement_cost:
                    remaining_len = len(global_indices) - len(index_set)
                    remaining_cost = cost_for_complement_indices(global_indices, index_set, remaining_len)
                else:
                    remaining_cost = cost_for_indices(remaining)
            split_cost = (
                remaining_cost
                + cost_for_indices(scored_indices)
                + model_cost
            )
            gain = int((before_cost - split_cost) * scoring_scale)
            return gain, family_key, indices

        if cost_workers > 1 and len(round_candidate_items) >= 4:
            with ThreadPoolExecutor(max_workers=cost_workers) as executor:
                evaluated = list(executor.map(evaluate_candidate, round_candidate_items))
        else:
            evaluated = [evaluate_candidate(item) for item in round_candidate_items]
        for result in evaluated:
            if result is None:
                continue
            gain, family_key, indices = result
            if gain > best_gain:
                family_kind, family_hash, value_slot = family_key
                label = f"{family_kind}:{family_hash}:{value_slot}"
                best_gain = gain
                best_item = (label, indices, gain, family_kind)
        if best_item is None or best_gain < min_gain:
            break
        _label, indices, _gain, _family_kind = best_item
        for index in indices:
            assigned[index] = True
        selected.append(best_item)
        if profile:
            exact_greedy_seconds += time.perf_counter() - op_start
        exact_greedy_rounds += 1
    if profile and not fast_select and not onepass_exact_select:
        record_operation_timing(
            f"numeric_lattice.select.{kind}.exact_greedy",
            exact_greedy_seconds,
            count=max(1, exact_greedy_rounds),
        )

    op_start = time.perf_counter() if profile else 0.0
    assignment: dict[int, str] = {}
    groups_meta: list[dict[str, object]] = []
    for group_index, (label, indices, gain, family_kind) in enumerate(selected):
        tag = f"{kind[0].upper()}{group_index}"
        groups_meta.append({
            "tag": tag,
            "kind": kind,
            "family": label,
            "family_kind": family_kind,
            "count": len(indices),
            "gain": gain,
            "codec_cost": -1 if skip_group_codec_cost else cost_for_indices(indices),
        })
        for index in indices:
            occurrence = kind_occurrences[index]
            assignment[id(occurrence)] = tag
    if profile:
        record_operation_timing(f"numeric_lattice.select.{kind}.materialize_selected", time.perf_counter() - op_start, count=len(selected))

    if kind == "int" and fallback_mode == "denum_feature":
        op_start = time.perf_counter() if profile else 0.0
        fallback_counts: Counter[str] = Counter()
        fallback_family: dict[str, str] = {}
        fallback_indices_by_tag: dict[str, list[int]] = {}
        for index, occurrence in enumerate(kind_occurrences):
            if assigned[index]:
                continue
            try:
                width, first_digit = denum_bucket_key(occurrence.value)
                fallback_tag = f"DB{width}_{first_digit}"
                fallback_family[fallback_tag] = f"denum-width:{width}:first:{first_digit}"
            except Exception:
                fallback_tag = "G1"
                fallback_family[fallback_tag] = "global-fallback"
            fallback_counts[fallback_tag] += 1
            fallback_indices_by_tag.setdefault(fallback_tag, []).append(index)
        for fallback_tag, fallback_count in sorted(fallback_counts.items()):
            if residual_fraction > 0 and fallback_count < min_support:
                continue
            for index in fallback_indices_by_tag.get(fallback_tag, []):
                assignment[id(kind_occurrences[index])] = fallback_tag
            groups_meta.append({
                "tag": fallback_tag,
                "kind": kind,
                "family": fallback_family[fallback_tag],
                "family_kind": "denum-feature" if fallback_tag != "G1" else "global",
                "count": fallback_count,
                "gain": 0,
                "codec_cost": -1 if skip_group_codec_cost else cost_for_indices(fallback_indices_by_tag.get(fallback_tag, [])),
            })
        if profile:
            record_operation_timing(f"numeric_lattice.select.{kind}.fallback_denum", time.perf_counter() - op_start, count=len(fallback_counts))
    elif kind == "int" and fallback_mode == "left_context_delta":
        op_start = time.perf_counter() if profile else 0.0
        fallback_counts: Counter[str] = Counter()
        fallback_family: dict[str, str] = {}
        fallback_indices_by_tag: dict[str, list[int]] = {}
        tag_by_family: dict[tuple[str, int], str] = {}
        for index, occurrence in enumerate(kind_occurrences):
            if assigned[index]:
                continue
            width = len(occurrence.value.lstrip("-"))
            family_key = (occurrence.left_anchor or "<BOL>", width)
            fallback_tag = tag_by_family.get(family_key)
            if fallback_tag is None:
                fallback_tag = f"LC{len(tag_by_family)}"
                tag_by_family[family_key] = fallback_tag
                fallback_family[fallback_tag] = f"left-context:{family_key[0]}:width:{family_key[1]}"
            assignment[id(occurrence)] = fallback_tag
            fallback_counts[fallback_tag] += 1
            fallback_indices_by_tag.setdefault(fallback_tag, []).append(index)
        for fallback_tag, fallback_count in sorted(fallback_counts.items()):
            groups_meta.append({
                "tag": fallback_tag,
                "kind": kind,
                "family": fallback_family[fallback_tag],
                "family_kind": "left-context-width",
                "count": fallback_count,
                "gain": 0,
                "codec_cost": -1 if skip_group_codec_cost else cost_for_indices(fallback_indices_by_tag.get(fallback_tag, [])),
            })
        if profile:
            record_operation_timing(f"numeric_lattice.select.{kind}.fallback_left_context", time.perf_counter() - op_start, count=len(fallback_counts))
    else:
        op_start = time.perf_counter() if profile else 0.0
        fallback_tag = "G0" if kind == "float" else "G1"
        fallback_count = 0
        fallback_indices: list[int] = []
        for index, occurrence in enumerate(kind_occurrences):
            if not assigned[index]:
                assignment[id(occurrence)] = fallback_tag
                fallback_count += 1
                fallback_indices.append(index)
        groups_meta.append({
            "tag": fallback_tag,
            "kind": kind,
            "family": "global-fallback",
            "family_kind": "global",
            "count": fallback_count,
            "gain": 0,
            "codec_cost": -1 if skip_group_codec_cost else cost_for_indices(fallback_indices),
        })
        if profile:
            record_operation_timing(f"numeric_lattice.select.{kind}.fallback_global", time.perf_counter() - op_start, count=fallback_count)
    if profile:
        record_operation_timing(f"numeric_lattice.select.{kind}.total", time.perf_counter() - select_start, count=len(kind_occurrences))
    return assignment, groups_meta


def transform_text_numeric_lattice(
    text: str,
    dataset: str,
    used_tags: set[str],
    fallback_mode: str = "global",
    frontier_topk: int = 0,
    fast_select: bool = False,
    onepass_exact_select: bool = False,
    skip_group_codec_cost: bool = False,
    forced_groups: list[dict[str, object]] | None = None,
    forced_placeholders: dict[str, str] | None = None,
    forced_route_tags: list[str] | None = None,
) -> tuple[str, dict[str, list[str]], dict[str, object]]:
    profile = OPERATION_PROFILE
    total_start = time.perf_counter() if profile else 0.0
    op_start = time.perf_counter() if profile else 0.0
    lines = split_physical_lines(text)
    if profile:
        record_operation_timing("numeric_lattice.split_lines", time.perf_counter() - op_start, count=len(lines))
    line_count = max(1, len(lines))
    min_support = int(os.environ.get("PARE_NUMERIC_LATTICE_MIN_SUPPORT", str(max(64, line_count // 40))))
    residual_fraction = float(os.environ.get("PARE_RESIDUAL_STREAM_MIN_BLOCK_FRACTION", "0") or 0)
    if residual_fraction > 0:
        min_support = max(min_support, int(math.ceil(line_count * residual_fraction)))
    max_int_groups = int(os.environ.get("PARE_NUMERIC_LATTICE_MAX_INT_GROUPS", os.environ.get("PARE_NUMERIC_LATTICE_MAX_GROUPS", "8")))
    max_float_groups = int(os.environ.get("PARE_NUMERIC_LATTICE_MAX_FLOAT_GROUPS", os.environ.get("PARE_NUMERIC_LATTICE_MAX_GROUPS", "8")))
    max_groups = max(max_int_groups, max_float_groups)
    route_replay_requested = bool(forced_groups and forced_route_tags)
    if (
        route_replay_requested
        and forced_placeholders
        and _pare_cpp_accel is not None
        and os.environ.get("PARE_CPP_NUMERIC_ROUTE_REPLAY", "0") in {"1", "true", "True"}
    ):
        op_start = time.perf_counter() if profile else 0.0
        forced_tag_to_group: dict[str, dict[str, object]] = {}
        forced_replay_supported = True
        for group in forced_groups or []:
            tag = str(group.get("tag", ""))
            kind = str(group.get("kind", ""))
            if not tag or kind not in {"int", "float"} or tag in used_tags:
                forced_replay_supported = False
                break
            forced_tag_to_group[tag] = dict(group)
        route_tags = [str(tag) for tag in forced_route_tags or []]
        if forced_replay_supported and route_tags and all(tag in forced_tag_to_group for tag in route_tags):
            try:
                replayed = _pare_cpp_accel.numeric_route_replay(
                    lines,
                    route_tags,
                    {str(tag): str(placeholder) for tag, placeholder in forced_placeholders.items()},
                )
            except Exception:
                replayed = None
            if replayed is not None:
                transformed_text, values_by_tag = replayed
                counts: Counter[str] = Counter(route_tags)
                groups_meta: list[dict[str, object]] = []
                tag_specs: dict[str, ExtractSpec] = {}
                placeholders: dict[str, str] = {}
                for tag, count in counts.items():
                    group = dict(forced_tag_to_group[tag])
                    group["count"] = int(count)
                    groups_meta.append(group)
                    used_tags.add(tag)
                    tag_specs[tag] = ExtractSpec(
                        tag=tag,
                        pattern="numeric-lattice:" + str(group["family"]),
                        kind="auto",
                    )
                    placeholders[tag] = str(forced_placeholders[tag])
                specs_meta = [
                    {
                        "tag": tag,
                        "kind": spec.kind,
                        "placeholder": placeholders[tag],
                        "pattern": spec.pattern,
                        "store_group": spec.store_group,
                        "replacement": spec.replacement or "",
                        "context_tag": spec.context_tag or "",
                    }
                    for tag, spec in tag_specs.items()
                    if values_by_tag.get(tag)
                ]
                if profile:
                    record_operation_timing("numeric_lattice.cpp_route_replay", time.perf_counter() - op_start, count=len(route_tags))
                    record_operation_timing("numeric_lattice.total", time.perf_counter() - total_start, count=len(route_tags))
                return transformed_text, values_by_tag, {
                    "specs": specs_meta,
                    "numeric_lattice": {
                        "occurrences": len(route_tags),
                        "groups": groups_meta,
                        "fallback_mode": fallback_mode,
                        "min_support": min_support,
                        "max_int_groups": max_int_groups,
                        "max_float_groups": max_float_groups,
                        "frontier_topk": frontier_topk,
                        "fast_select": fast_select,
                        "onepass_exact_select": onepass_exact_select,
                        "forced_replay": 1,
                        "route_replay": 1,
                        "route_tags": route_tags,
                        "cpp_route_replay": 1,
                    },
                }
    op_start = time.perf_counter() if profile else 0.0
    if route_replay_requested:
        occurrences = _numeric_lattice_occurrences_for_route_replay(lines)
    else:
        occurrences = _numeric_lattice_occurrences(lines)
    if profile:
        record_operation_timing("numeric_lattice.occurrences.total", time.perf_counter() - op_start, count=len(occurrences))
    if not occurrences:
        return text, {}, {"specs": [], "numeric_lattice": {"occurrences": 0, "groups": []}}

    assignment: dict[int, str] = {}
    groups_meta: list[dict[str, object]] = []
    forced_replay = bool(forced_groups)
    route_replay = False
    if forced_replay:
        op_start = time.perf_counter() if profile else 0.0
        forced_by_kind: dict[str, list[dict[str, object]]] = defaultdict(list)
        forced_tag_to_group: dict[str, dict[str, object]] = {}
        for group in forced_groups or []:
            tag = str(group.get("tag", ""))
            kind = str(group.get("kind", ""))
            if not tag or kind not in {"int", "float"} or tag in used_tags:
                forced_replay = False
                break
            forced_by_kind[kind].append(dict(group))
            forced_tag_to_group[tag] = dict(group)
        if (
            forced_replay
            and forced_route_tags
            and forced_placeholders
            and _pare_cpp_accel is not None
        ):
            route_tags = [str(tag) for tag in forced_route_tags]
            if all(tag in forced_tag_to_group for tag in route_tags):
                try:
                    replayed = _pare_cpp_accel.numeric_route_replay(
                        lines,
                        route_tags,
                        {str(tag): str(placeholder) for tag, placeholder in forced_placeholders.items()},
                    )
                except Exception:
                    replayed = None
                if replayed is not None:
                    transformed_text, values_by_tag = replayed
                    counts: Counter[str] = Counter(route_tags)
                    groups_meta = []
                    tag_specs: dict[str, ExtractSpec] = {}
                    placeholders: dict[str, str] = {}
                    for tag, count in counts.items():
                        group = dict(forced_tag_to_group[tag])
                        group["count"] = int(count)
                        groups_meta.append(group)
                        used_tags.add(tag)
                        tag_specs[tag] = ExtractSpec(
                            tag=tag,
                            pattern="numeric-lattice:" + str(group["family"]),
                            kind="auto",
                        )
                        placeholders[tag] = str(forced_placeholders[tag])
                    specs_meta = [
                        {
                            "tag": tag,
                            "kind": spec.kind,
                            "placeholder": placeholders[tag],
                            "pattern": spec.pattern,
                            "store_group": spec.store_group,
                            "replacement": spec.replacement or "",
                            "context_tag": spec.context_tag or "",
                        }
                        for tag, spec in tag_specs.items()
                        if values_by_tag.get(tag)
                    ]
                    if profile:
                        record_operation_timing("numeric_lattice.cpp_route_replay", time.perf_counter() - op_start, count=len(route_tags))
                        record_operation_timing("numeric_lattice.total", time.perf_counter() - total_start, count=len(route_tags))
                    return transformed_text, values_by_tag, {
                        "specs": specs_meta,
                        "numeric_lattice": {
                            "occurrences": len(route_tags),
                            "groups": groups_meta,
                            "fallback_mode": fallback_mode,
                            "min_support": min_support,
                            "max_int_groups": max_int_groups,
                            "max_float_groups": max_float_groups,
                            "frontier_topk": frontier_topk,
                            "fast_select": fast_select,
                            "onepass_exact_select": onepass_exact_select,
                            "forced_replay": 1,
                            "route_replay": 1,
                            "route_tags": route_tags,
                            "cpp_route_replay": 1,
                        },
                    }
        if forced_replay and forced_route_tags:
            route_tags = [str(tag) for tag in forced_route_tags]
            if len(route_tags) == len(occurrences) and all(tag in forced_tag_to_group for tag in route_tags):
                counts: Counter[str] = Counter(route_tags)
                for occurrence, tag in zip(occurrences, route_tags):
                    assignment[id(occurrence)] = tag
                groups_meta = []
                for tag, count in counts.items():
                    item = dict(forced_tag_to_group[tag])
                    item["count"] = int(count)
                    groups_meta.append(item)
                route_replay = True
            else:
                forced_replay = False
        if forced_replay and not route_replay:
            for kind in ("float", "int"):
                kind_occurrences = [item for item in occurrences if item.kind == kind]
                if not kind_occurrences:
                    continue
                assigned_kind = [False] * len(kind_occurrences)
                fallback_groups: list[dict[str, object]] = []
                for group in forced_by_kind.get(kind, []):
                    family = str(group.get("family", ""))
                    family_kind = str(group.get("family_kind", ""))
                    tag = str(group.get("tag", ""))
                    if family == "global-fallback" or family_kind == "global":
                        fallback_groups.append(group)
                        continue
                    matched = 0
                    for index, occurrence in enumerate(kind_occurrences):
                        if assigned_kind[index]:
                            continue
                        slot_label = f"slot:{occurrence.template_key}:{occurrence.value_slot}"
                        prefix_label = f"prefix:{occurrence.prefix_key}:{occurrence.value_slot}"
                        if family == slot_label or family == prefix_label:
                            assignment[id(occurrence)] = tag
                            assigned_kind[index] = True
                            matched += 1
                    if matched:
                        item = dict(group)
                        item["count"] = matched
                        groups_meta.append(item)
                remaining = [
                    occurrence
                    for index, occurrence in enumerate(kind_occurrences)
                    if not assigned_kind[index]
                ]
                if remaining:
                    if not fallback_groups:
                        forced_replay = False
                        break
                    fallback = dict(fallback_groups[0])
                    fallback_tag = str(fallback.get("tag", "G0" if kind == "float" else "G1"))
                    for occurrence in remaining:
                        assignment[id(occurrence)] = fallback_tag
                    fallback["count"] = len(remaining)
                    groups_meta.append(fallback)
        if profile:
            record_operation_timing("numeric_lattice.forced_group_replay", time.perf_counter() - op_start, count=len(groups_meta))

    if not forced_replay:
        assignment = {}
        groups_meta = []
        for kind, max_groups in (("float", max_float_groups), ("int", max_int_groups)):
            op_start = time.perf_counter() if profile else 0.0
            kind_assignment, kind_groups = _numeric_lattice_select_groups(
                occurrences,
                kind=kind,
                min_support=min_support,
                max_groups=max_groups,
                fallback_mode=fallback_mode,
                frontier_topk=frontier_topk,
                fast_select=fast_select,
                onepass_exact_select=onepass_exact_select,
                skip_group_codec_cost=skip_group_codec_cost,
            )
            assignment.update(kind_assignment)
            groups_meta.extend(kind_groups)
            if profile:
                record_operation_timing(f"numeric_lattice.select.{kind}.wrapper", time.perf_counter() - op_start, count=len(kind_groups))

    op_start = time.perf_counter() if profile else 0.0
    tag_remap: dict[str, str] = {}
    tag_specs: dict[str, ExtractSpec] = {}
    for group in groups_meta:
        raw_tag = str(group["tag"])
        if forced_replay:
            tag = raw_tag
            used_tags.add(tag)
        elif raw_tag in {"G0", "G1"} and raw_tag not in used_tags:
            tag = raw_tag
            used_tags.add(tag)
        else:
            tag = _numeric_lattice_tag(raw_tag[0], len(tag_remap), used_tags)
        tag_remap[raw_tag] = tag
        tag_specs[tag] = ExtractSpec(
            tag=tag,
            pattern="numeric-lattice:" + str(group["family"]),
            kind="auto",
        )
        group["tag"] = tag

    values_by_tag: dict[str, list[str]] = {tag: [] for tag in tag_specs}
    placeholders = {
        tag: (forced_placeholders or {}).get(tag, choose_placeholder(text, dataset, tag))
        for tag in tag_specs
    }
    if profile:
        record_operation_timing("numeric_lattice.tag_setup", time.perf_counter() - op_start, count=len(tag_specs))
    op_start = time.perf_counter() if profile else 0.0
    occurrences_by_line: dict[int, list[NumericLatticeOccurrence]] = {}
    for occurrence in occurrences:
        occurrences_by_line.setdefault(occurrence.line_index, []).append(occurrence)
    if profile:
        record_operation_timing("numeric_lattice.group_occurrences_by_line", time.perf_counter() - op_start, count=len(occurrences_by_line))

    op_start = time.perf_counter() if profile else 0.0
    transformed_lines: list[str] = []
    for line_index, line in enumerate(lines):
        line_occurrences = occurrences_by_line.get(line_index)
        if not line_occurrences:
            transformed_lines.append(line)
            continue
        pieces: list[str] = []
        cursor = 0
        for occurrence in sorted(line_occurrences, key=lambda item: item.start):
            pieces.append(line[cursor:occurrence.start])
            raw_tag = assignment.get(id(occurrence))
            if raw_tag is None:
                pieces.append(line[occurrence.start:occurrence.end])
                cursor = occurrence.end
                continue
            tag = tag_remap[raw_tag]
            values_by_tag[tag].append(occurrence.value)
            pieces.append(placeholders[tag])
            cursor = occurrence.end
        pieces.append(line[cursor:])
        transformed_lines.append("".join(pieces))
    if profile:
        record_operation_timing("numeric_lattice.rewrite_lines", time.perf_counter() - op_start, count=len(lines))

    specs_meta = [
        {
            "tag": tag,
            "kind": spec.kind,
            "placeholder": placeholders[tag],
            "pattern": spec.pattern,
            "store_group": spec.store_group,
            "replacement": spec.replacement or "",
            "context_tag": spec.context_tag or "",
        }
        for tag, spec in tag_specs.items()
        if values_by_tag.get(tag)
    ]
    if profile:
        record_operation_timing("numeric_lattice.total", time.perf_counter() - total_start, count=len(occurrences))
    route_tags_out = [
        tag_remap[assignment[id(occurrence)]]
        for occurrence in occurrences
        if id(occurrence) in assignment
    ]
    return "".join(transformed_lines), values_by_tag, {
        "specs": specs_meta,
        "numeric_lattice": {
            "occurrences": len(occurrences),
            "min_support": min_support,
            "fallback_mode": fallback_mode,
            "frontier_topk": frontier_topk,
            "fast_select": fast_select,
            "onepass_exact_select": onepass_exact_select,
            "max_groups_per_kind": max_groups,
            "forced_replay": int(forced_replay),
            "route_replay": int(route_replay),
            "route_tags": route_tags_out,
            "groups": groups_meta,
        },
    }


def _cache_lattice_header_field_specs(
    family_key: tuple[str, ...],
    slot_values: dict[tuple[int, ...], list[str]],
    text: str,
    dataset: str,
    max_specs: int,
    skip_single_numeric: bool = False,
    skip_all_single_slots: bool = False,
) -> list[tuple[tuple[int, ...], ExtractSpec, str, int]]:
    """Infer reversible header fields from one high-support prefix family."""
    candidates: list[tuple[int, int, tuple[int, ...], ExtractSpec, str]] = []
    used_slots: set[int] = set()
    next_tag = 0

    # First let adjacent date+time tokens become one semantic field. This is
    # the deterministic form of the user's "LLM may combine variables" idea.
    for index in range(len(family_key) - 1):
        if family_key[index] == "D+-D+" and family_key[index + 1] == "D+:D+:D+.D+":
            field = (index, index + 1)
            values = slot_values.get(field, [])
            if len(values) < 2 or len(set(values)) < 2:
                continue
            program = _cache_lattice_mmdd_millis_program()
            try:
                for value in values[: min(len(values), 512)]:
                    if render_open_function_exact(value, program) != value:
                        raise ValueError("bad round trip")
            except Exception:
                continue
            tag = f"C{next_tag}"
            next_tag += 1
            spec = ExtractSpec(
                tag=tag,
                pattern="cache-lattice-header:" + json.dumps(
                    {"template": list(family_key), "slots": list(field), "kind": "mmdd_hms_millis"},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                kind="open_function",
                context_tag=json.dumps(program, sort_keys=True),
            )
            placeholder = choose_placeholder(text, dataset, tag)
            score = _line_transducer_candidate_score(values, placeholder, spec)
            if score > 128:
                candidates.append((score + len(values) * 8, len(values), field, spec, placeholder))
                used_slots.update(field)

    for index, shape in enumerate(family_key):
        if index in used_slots:
            continue
        if skip_all_single_slots:
            continue
        if skip_single_numeric and shape == "D+":
            continue
        field = (index,)
        values = slot_values.get(field, [])
        if len(values) < 2 or len(set(values)) < 2:
            continue
        tag = f"C{next_tag}"
        next_tag += 1
        spec = _template_cache_candidate_spec(tag, values, (family_key, index, shape))
        if spec is None:
            continue
        placeholder = choose_placeholder(text, dataset, tag)
        score = _line_transducer_candidate_score(values, placeholder, spec)
        # Header fields can unlock line-dictionary reuse, so keep a slightly
        # lower gate than generic residual slots while still requiring benefit.
        if score > -256:
            candidates.append((score, len(values), field, spec, placeholder))

    candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return [
        (field, spec, placeholder, score)
        for score, _count, field, spec, placeholder in candidates[:max_specs]
    ]


def transform_text_cache_lattice(
    text: str,
    dataset: str,
    profile: str = "auto_cache_lattice_v1",
) -> tuple[str, dict[str, list[str]], dict[str, object]]:
    """Template-indexed cache lattice with a learned global header stage.

    The first stage performs an offline census over line-prefix templates and
    freezes high-support slot programs. It is a verifier-friendly stand-in for
    batched LLM cache synthesis: slots may be single values or combined fields,
    and every accepted field is materialized as a deterministic side stream.
    """
    lines = split_physical_lines(text)
    line_count = max(1, len(lines))
    min_support = int(os.environ.get("PARE_CACHE_LATTICE_MIN_SUPPORT", str(max(100, line_count // 20))))
    max_families = int(os.environ.get("PARE_CACHE_LATTICE_MAX_FAMILIES", "3"))
    max_header_specs = int(os.environ.get("PARE_CACHE_LATTICE_MAX_HEADER_SPECS", "16"))

    family_counts: Counter[tuple[str, ...]] = Counter()
    tokenized_lines: list[tuple[list[re.Match[str]], tuple[str, ...] | None]] = []
    for line in lines:
        matches = _template_cache_line_tokens(line)
        tokens = [match.group(0) for match in matches]
        prefix_len = _cache_lattice_header_prefix_length(tokens)
        if prefix_len:
            family_key = tuple(_cache_lattice_header_shape(token) for token in tokens[:prefix_len])
            family_counts[family_key] += 1
            tokenized_lines.append((matches, family_key))
        else:
            tokenized_lines.append((matches, None))

    selected_families = [
        family
        for family, count in family_counts.most_common(max_families)
        if count >= min_support
    ]

    values_by_family: dict[tuple[str, ...], dict[tuple[int, ...], list[str]]] = {
        family: {} for family in selected_families
    }
    for line, (matches, family_key) in zip(lines, tokenized_lines):
        if family_key not in selected_families:
            continue
        tokens = [match.group(0) for match in matches]
        family_values = values_by_family[family_key]
        for index in range(len(family_key)):
            family_values.setdefault((index,), []).append(tokens[index])
        for index in range(len(family_key) - 1):
            if family_key[index] == "D+-D+" and family_key[index + 1] == "D+:D+:D+.D+":
                start = matches[index].start()
                end = matches[index + 1].end()
                family_values.setdefault((index, index + 1), []).append(line[start:end])

    selected_fields: dict[tuple[str, ...], list[tuple[tuple[int, ...], ExtractSpec, str, int]]] = {}
    used_tags: set[str] = set()
    for family_key, slot_values in values_by_family.items():
        fields = _cache_lattice_header_field_specs(
            family_key,
            slot_values,
            text=text,
            dataset=dataset,
            max_specs=max_header_specs,
            skip_single_numeric=(profile == "auto_cache_lattice_v3"),
            skip_all_single_slots=(profile in {"auto_cache_lattice_v4", "auto_cache_lattice_v5", "auto_cache_lattice_v6", "auto_cache_lattice_v7"}),
        )
        retagged: list[tuple[tuple[int, ...], ExtractSpec, str, int]] = []
        for field, spec, _placeholder, score in fields:
            tag_index = len(used_tags)
            tag = f"C{tag_index}"
            while tag in used_tags:
                tag_index += 1
                tag = f"C{tag_index}"
            used_tags.add(tag)
            spec = ExtractSpec(
                tag=tag,
                pattern=spec.pattern,
                kind=spec.kind,
                store_group=spec.store_group,
                replacement=spec.replacement,
                context_tag=spec.context_tag,
            )
            retagged.append((field, spec, choose_placeholder(text, dataset, tag), score))
        selected_fields[family_key] = retagged

    values_by_tag: dict[str, list[str]] = {
        spec.tag: []
        for fields in selected_fields.values()
        for _field, spec, _placeholder, _score in fields
    }

    transformed_lines: list[str] = []
    for line, (matches, family_key) in zip(lines, tokenized_lines):
        fields = selected_fields.get(family_key or ())
        if not fields:
            transformed_lines.append(line)
            continue
        field_by_start = {field[0]: (field, spec, placeholder) for field, spec, placeholder, _score in fields}
        pieces: list[str] = []
        cursor = 0
        index = 0
        while index < len(matches):
            item = field_by_start.get(index)
            if item is None:
                index += 1
                continue
            field, spec, placeholder = item
            start = matches[field[0]].start()
            end = matches[field[-1]].end()
            pieces.append(line[cursor:start])
            values_by_tag[spec.tag].append(line[start:end])
            pieces.append(placeholder)
            cursor = end
            index = field[-1] + 1
        pieces.append(line[cursor:])
        transformed_lines.append("".join(pieces))

    working_text = "".join(transformed_lines)
    header_specs_meta = [
        {
            "tag": spec.tag,
            "kind": spec.kind,
            "placeholder": placeholder,
            "pattern": spec.pattern,
            "store_group": spec.store_group,
            "replacement": spec.replacement or "",
            "context_tag": spec.context_tag or "",
        }
        for fields in selected_fields.values()
        for _field, spec, placeholder, _score in fields
        if values_by_tag.get(spec.tag)
    ]

    residual_specs = discover_auto_extract_specs(working_text, profile="auto_discovered_v2", dataset=dataset)
    working_text, residual_values, residual_metadata = transform_text_with_specs(
        working_text,
        dataset=dataset,
        profile=profile,
        profile_specs=residual_specs,
    )
    values_by_tag.update(residual_values)

    numeric_specs_meta: list[dict[str, object]] = []
    if profile in {"auto_cache_lattice_v2", "auto_cache_lattice_v3", "auto_cache_lattice_v4", "auto_cache_lattice_v5", "auto_cache_lattice_v6", "auto_cache_lattice_v7"}:
        if profile in {"auto_cache_lattice_v6", "auto_cache_lattice_v7"}:
            used_numeric_tags = {
                str(spec_meta["tag"])
                for spec_meta in header_specs_meta + list(residual_metadata["specs"])
            }
            working_text, numeric_values, numeric_metadata = transform_text_numeric_lattice(
                working_text,
                dataset=dataset,
                used_tags=used_numeric_tags,
                fallback_mode="denum_feature" if profile == "auto_cache_lattice_v7" else "global",
            )
            values_by_tag.update(numeric_values)
            numeric_specs_meta = list(numeric_metadata["specs"])
        elif profile == "auto_cache_lattice_v5":
            numeric_specs = [
                ExtractSpec("G0", r"\b\d+\.\d+\b", "auto"),
                ExtractSpec("G1", r"\b\d+\b", "auto"),
            ]
            working_text, numeric_values, numeric_metadata = transform_text_with_specs(
                working_text,
                dataset=dataset,
                profile=profile,
                profile_specs=numeric_specs,
            )
            values_by_tag.update(numeric_values)
            numeric_specs_meta = list(numeric_metadata["specs"])
        elif profile in {"auto_cache_lattice_v2", "auto_cache_lattice_v3", "auto_cache_lattice_v4"}:
            numeric_specs = [
                ExtractSpec("G0", r"(?<![\w<.])-?\d+\.\d+(?![\w>.])", "auto"),
                ExtractSpec("G1", r"(?<![\w<.])-?\d+(?![\w>.])", "auto"),
            ]
            working_text, numeric_values, numeric_metadata = transform_text_with_specs(
                working_text,
                dataset=dataset,
                profile=profile,
                profile_specs=numeric_specs,
            )
            values_by_tag.update(numeric_values)
            numeric_specs_meta = list(numeric_metadata["specs"])

    working_text, line_values, line_metadata = transform_text_line_transducer(
        working_text,
        dataset=dataset,
        profile="auto_line_transducer_v1",
    )
    values_by_tag.update(line_values)

    metadata = {
        "dataset": dataset,
        "profile": profile,
        "cache_lattice": {
            "min_support": min_support,
            "max_families": max_families,
            "selected_families": len(selected_families),
            "selected_header_fields": len(header_specs_meta),
            "family_counts": [
                {"count": count, "template": list(family)}
                for family, count in family_counts.most_common(max_families)
            ],
        },
        "specs": (
            header_specs_meta
            + list(residual_metadata["specs"])
            + numeric_specs_meta
            + list(line_metadata["specs"])
        ),
    }
    return working_text, values_by_tag, metadata


@lru_cache(maxsize=262144)
def _line_transducer_is_plain_alpha(token: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z][A-Za-z_-]{1,48}", token))


@lru_cache(maxsize=262144)
def _line_transducer_is_digit_or_special_value(token: str) -> bool:
    if _line_transducer_is_placeholder(token):
        return False
    if any(ch.isdigit() for ch in token):
        return True
    if not any(ch.isalnum() for ch in token):
        return False
    if not any(not ch.isalnum() for ch in token):
        return False
    # Pure bracketed log levels such as [error] are usually constants.  They
    # can still be extracted later if an alpha-variable context says so.
    stripped = token.strip("[](){}<>")
    if stripped.isalpha() and len(stripped) <= 16:
        return False
    return len(token) >= 3


@lru_cache(maxsize=262144)
def _line_transducer_anchor_token(token: str) -> str:
    if _line_transducer_is_placeholder(token):
        return token
    if _line_transducer_is_digit_or_special_value(token):
        return f"<{_template_cache_value_shape(token)}>"
    return token.lower()[:48]


def _line_transducer_alpha_context_key(tokens: list[str], index: int) -> tuple[str, str, str]:
    left = _line_transducer_anchor_token(tokens[index - 1]) if index > 0 else "<BOL>"
    right = _line_transducer_anchor_token(tokens[index + 1]) if index + 1 < len(tokens) else "<EOL>"
    return (left, "ALPHA", right)


def _line_transducer_discover_alpha_contexts(
    lines: list[str],
    min_support: int,
    min_distinct: int,
    max_top_ratio: float,
) -> set[tuple[str, str, str]]:
    if os.environ.get("PARE_DISABLE_RESIDUAL_ALPHA_CONTEXTS") == "1":
        return set()
    values_by_context: dict[tuple[str, str, str], Counter[str]] = {}
    for line in lines:
        tokens = [match.group(0) for match in _template_cache_line_tokens(line)]
        for index, token in enumerate(tokens):
            if not _line_transducer_is_plain_alpha(token):
                continue
            key = _line_transducer_alpha_context_key(tokens, index)
            values_by_context.setdefault(key, Counter())[token] += 1

    variable_contexts: set[tuple[str, str, str]] = set()
    for key, values in values_by_context.items():
        support = sum(values.values())
        if support < min_support or len(values) < min_distinct:
            continue
        if values.most_common(1)[0][1] / support > max_top_ratio:
            continue
        variable_contexts.add(key)
    return variable_contexts


def _line_transducer_is_value_candidate(
    tokens: list[str],
    index: int,
    alpha_contexts: set[tuple[str, str, str]],
) -> bool:
    token = tokens[index]
    if _line_transducer_is_digit_or_special_value(token):
        return True
    return (
        _line_transducer_is_plain_alpha(token)
        and _line_transducer_alpha_context_key(tokens, index) in alpha_contexts
    )


@lru_cache(maxsize=262144)
def _line_transducer_token_shape(token: str) -> str:
    if _line_transducer_is_plain_alpha(token):
        return "ALPHA"
    return _template_cache_value_shape(token)


def _line_transducer_template_key(
    tokens: list[str],
    alpha_contexts: set[tuple[str, str, str]],
) -> tuple[str, ...]:
    out: list[str] = []
    for index, token in enumerate(tokens):
        if _line_transducer_is_placeholder(token):
            out.append(token)
        elif _line_transducer_is_value_candidate(tokens, index, alpha_contexts):
            out.append(f"<{_line_transducer_token_shape(token)}>")
        else:
            out.append(token.lower()[:48])
    return tuple(out)


def _line_transducer_candidate_score(values: list[str], placeholder: str, spec: ExtractSpec) -> int:
    raw_side_cost = compressed_parts_cost([encode_string_stream_bytes(values)])
    if spec.kind == "open_function" and spec.context_tag:
        try:
            program = json.loads(spec.context_tag)
            numeric_values = [parse_open_function_value(value, program) for value in values]
            side_cost = compressed_parts_cost([encode_delta_values(numeric_values)])
        except Exception:
            side_cost = raw_side_cost
    else:
        side_cost = min(cost for _kind, cost in stream_codec_candidates(values))
    raw_main_bytes = sum(len(value.encode("latin-1")) for value in values)
    placeholder_main_bytes = len(placeholder.encode("latin-1")) * len(values)
    return (raw_main_bytes - placeholder_main_bytes) + (raw_side_cost - side_cost)


LineTransducerCandidate = tuple[int, int, int, tuple[tuple[str, ...], int, str], ExtractSpec, str]


def _line_transducer_apply_selected(
    lines: list[str],
    alpha_contexts: set[tuple[str, str, str]],
    selected: list[LineTransducerCandidate],
    initial_values_by_tag: dict[str, list[str]],
) -> tuple[str, dict[str, list[str]], list[dict[str, object]]]:
    spec_by_family = {
        family_key: spec
        for _score, _count, _distinct, family_key, spec, _placeholder in selected
    }
    placeholder_by_tag = {
        spec.tag: placeholder
        for _score, _count, _distinct, _family_key, spec, placeholder in selected
    }

    values_by_tag: dict[str, list[str]] = {
        tag: list(values)
        for tag, values in initial_values_by_tag.items()
    }
    for spec in spec_by_family.values():
        values_by_tag[spec.tag] = []

    transformed_lines: list[str] = []
    for line in lines:
        matches = _template_cache_line_tokens(line)
        tokens = [match.group(0) for match in matches]
        if not tokens:
            transformed_lines.append(line)
            continue
        template_key = _line_transducer_template_key(tokens, alpha_contexts)
        pieces: list[str] = []
        cursor = 0
        for slot_index, match in enumerate(matches):
            pieces.append(line[cursor:match.start()])
            token = match.group(0)
            if _line_transducer_is_value_candidate(tokens, slot_index, alpha_contexts):
                family_key = (template_key, slot_index, _line_transducer_token_shape(token))
                spec = spec_by_family.get(family_key)
            else:
                spec = None
            if spec is None:
                pieces.append(token)
            else:
                values_by_tag[spec.tag].append(token)
                pieces.append(placeholder_by_tag[spec.tag])
            cursor = match.end()
        pieces.append(line[cursor:])
        transformed_lines.append("".join(pieces))

    line_specs_meta = [
        {
            "tag": spec.tag,
            "kind": spec.kind,
            "placeholder": placeholder_by_tag[spec.tag],
            "pattern": spec.pattern,
            "store_group": spec.store_group,
            "replacement": spec.replacement or "",
            "context_tag": spec.context_tag or "",
        }
        for _score, _count, _distinct, _family_key, spec, _placeholder in selected
        if values_by_tag.get(spec.tag)
    ]
    return "".join(transformed_lines), values_by_tag, line_specs_meta


def _line_transducer_tag_stream_proxy_cost(
    spec: ExtractSpec,
    values_by_tag: dict[str, list[str]],
    placeholders_by_tag: dict[str, str],
    transformed_text: str,
    prior_tags: list[str],
) -> int:
    values = values_by_tag.get(spec.tag, [])
    if not values:
        return 0
    if spec.kind == "auto":
        _selected, auto_meta = select_auto_stream_spec(
            spec,
            values_by_tag=values_by_tag,
            placeholders_by_tag=placeholders_by_tag,
            transformed_text=transformed_text,
            candidate_context_tags=prior_tags,
        )
        return int(auto_meta["auto_cost"])
    if spec.kind == "open_function" and spec.context_tag:
        try:
            program = json.loads(spec.context_tag)
            numeric_values = [parse_open_function_value(value, program) for value in values]
            return compressed_parts_cost([encode_delta_values(numeric_values)])
        except Exception:
            pass
    return min(cost for _kind, cost in stream_codec_candidates(values))


def _line_transducer_proxy_cost(
    transformed_text: str,
    values_by_tag: dict[str, list[str]],
    specs_meta: list[dict[str, object]],
    dataset: str,
) -> int:
    main_text, line_dict_metadata, line_dict_templates, line_dict_side_ids = build_line_dictionary_text(
        transformed_text,
        min_support=2,
        max_entries=8192,
        id_mode="side_varint",
    )
    cost = len(lzma.compress(main_text.encode("latin-1")))
    if line_dict_metadata is not None:
        cost += compressed_parts_cost([
            encode_string_stream_bytes(line_dict_templates),
            encode_varint_stream_bytes(line_dict_side_ids),
        ])
        cost += len(lzma.compress(json.dumps(line_dict_metadata, sort_keys=True).encode("utf-8")))

    metadata = {"dataset": dataset, "profile": "auto_line_transducer_v3", "specs": specs_meta}
    specs_by_tag = {spec.tag: spec for spec in specs_from_metadata(metadata)}
    placeholders_by_tag = {
        str(spec_meta["tag"]): str(spec_meta["placeholder"])
        for spec_meta in specs_meta
    }

    prior_tags: list[str] = []
    active_streams = 0
    for spec_meta in specs_meta:
        tag = str(spec_meta["tag"])
        spec = specs_by_tag[tag]
        if values_by_tag.get(tag):
            active_streams += 1
            cost += _line_transducer_tag_stream_proxy_cost(
                spec,
                values_by_tag=values_by_tag,
                placeholders_by_tag=placeholders_by_tag,
                transformed_text=transformed_text,
                prior_tags=prior_tags,
            )
        prior_tags.append(tag)

    compact_metadata = {
        "dataset": dataset,
        "profile": "auto_line_transducer_v3",
        "specs": [
            {
                "tag": str(spec_meta["tag"]),
                "kind": str(spec_meta["kind"]),
                "placeholder": str(spec_meta["placeholder"]),
            }
            for spec_meta in specs_meta
        ],
        "line_dictionary": line_dict_metadata or {},
    }
    cost += len(lzma.compress(json.dumps(compact_metadata, sort_keys=True).encode("utf-8")))
    # Account for tar/member overhead and stream metadata that the lightweight
    # proxy otherwise underestimates. This deliberately biases admission toward
    # fewer, stronger per-family functions.
    cost += active_streams * int(os.environ.get("PARE_LINE_TRANSDUCER_STREAM_OVERHEAD", "96"))
    return cost


def _line_transducer_mdl_select(
    lines: list[str],
    alpha_contexts: set[tuple[str, str, str]],
    candidates: list[LineTransducerCandidate],
    initial_values_by_tag: dict[str, list[str]],
    initial_specs_meta: list[dict[str, object]],
    dataset: str,
    max_specs: int,
) -> list[LineTransducerCandidate]:
    pool_size = int(os.environ.get("PARE_LINE_TRANSDUCER_MDL_POOL", str(max_specs)))
    margin = int(os.environ.get("PARE_LINE_TRANSDUCER_MDL_MARGIN", "512"))
    pool = candidates[:max(0, pool_size)]
    selected: list[LineTransducerCandidate] = []

    transformed, values_by_tag, line_specs_meta = _line_transducer_apply_selected(
        lines,
        alpha_contexts,
        selected,
        initial_values_by_tag,
    )
    current_cost = _line_transducer_proxy_cost(
        transformed,
        values_by_tag,
        initial_specs_meta + line_specs_meta,
        dataset,
    )

    for candidate in pool:
        if len(selected) >= max_specs:
            break
        trial = selected + [candidate]
        transformed, values_by_tag, line_specs_meta = _line_transducer_apply_selected(
            lines,
            alpha_contexts,
            trial,
            initial_values_by_tag,
        )
        trial_cost = _line_transducer_proxy_cost(
            transformed,
            values_by_tag,
            initial_specs_meta + line_specs_meta,
            dataset,
        )
        if trial_cost + margin < current_cost:
            selected = trial
            current_cost = trial_cost
    return selected


def _line_transducer_fast_mdl_select(
    candidates: list[LineTransducerCandidate],
    max_specs: int,
) -> list[LineTransducerCandidate]:
    min_gain = int(os.environ.get("PARE_LINE_TRANSDUCER_FAST_GAIN", "512"))
    per_stream_overhead = int(os.environ.get("PARE_LINE_TRANSDUCER_STREAM_OVERHEAD", "96"))
    selected: list[LineTransducerCandidate] = []
    slots_per_template: Counter[tuple[tuple[str, ...], str]] = Counter()
    max_slots_per_template = int(os.environ.get("PARE_LINE_TRANSDUCER_MAX_SLOTS_PER_TEMPLATE", "8"))

    for score, count, distinct, family_key, spec, placeholder in candidates:
        template_key, _slot_index, shape = family_key
        if slots_per_template[(template_key, shape)] >= max_slots_per_template:
            continue
        # Treat each induced group as a model with its own side stream.  The
        # older v2 selector kept many marginal groups because it only used a
        # raw local score; this gate pays an explicit model/stream cost before
        # admitting the group.
        net_gain = score - per_stream_overhead
        if net_gain < min_gain:
            continue
        selected.append((score, count, distinct, family_key, spec, placeholder))
        slots_per_template[(template_key, shape)] += 1
        if len(selected) >= max_specs:
            break
    return selected


def transform_text_line_transducer(
    text: str,
    dataset: str,
    profile: str = "auto_line_transducer_v1",
) -> tuple[str, dict[str, list[str]], dict[str, object]]:
    """Line-family transducer prototype.

    A whole-line lexical template is the cache key.  Within each heavy line
    family, digit/special slots and statistically unstable alphabetic slots are
    routed into per-family value streams, then restored by placeholder order.
    This is the deterministic verifier-friendly lower bound for the planned
    LLM-generated line transducer cache.
    """
    if profile in {"auto_line_transducer_v2", "auto_line_transducer_v3"}:
        initial_profile_specs = discover_auto_extract_specs(text, profile="auto_discovered_v2", dataset=dataset)
    else:
        initial_profile_specs = _template_cache_phrase_specs()
    working_text, initial_values_by_tag, initial_metadata = transform_text_with_specs(
        text,
        dataset=dataset,
        profile=profile,
        profile_specs=initial_profile_specs,
    )
    initial_specs_meta = list(initial_metadata["specs"])

    lines = split_physical_lines(working_text)
    line_count = max(1, len(lines))
    min_support = int(os.environ.get("PARE_LINE_TRANSDUCER_MIN_SUPPORT", str(max(20, min(200, line_count // 1000)))))
    max_specs = int(os.environ.get("PARE_LINE_TRANSDUCER_MAX_SPECS", "64"))
    min_alpha_distinct = int(os.environ.get("PARE_LINE_TRANSDUCER_ALPHA_MIN_DISTINCT", "3"))
    max_alpha_top_ratio = float(os.environ.get("PARE_LINE_TRANSDUCER_ALPHA_MAX_TOP_RATIO", "0.92"))

    alpha_contexts = _line_transducer_discover_alpha_contexts(
        lines,
        min_support=min_support,
        min_distinct=min_alpha_distinct,
        max_top_ratio=max_alpha_top_ratio,
    )

    family_values: dict[tuple[tuple[str, ...], int, str], list[str]] = {}
    candidate_slots = 0
    for line in lines:
        matches = _template_cache_line_tokens(line)
        tokens = [match.group(0) for match in matches]
        if not tokens:
            continue
        template_key = _line_transducer_template_key(tokens, alpha_contexts)
        for slot_index, token in enumerate(tokens):
            if not _line_transducer_is_value_candidate(tokens, slot_index, alpha_contexts):
                continue
            shape = _line_transducer_token_shape(token)
            family_key = (template_key, slot_index, shape)
            if family_key not in family_values:
                candidate_slots += 1
            family_values.setdefault(family_key, []).append(token)

    candidates: list[tuple[int, int, int, tuple[tuple[str, ...], int, str], ExtractSpec, str]] = []
    next_tag = 0
    for family_key, values in family_values.items():
        count = len(values)
        distinct = len(set(values))
        if count < min_support or distinct < 2:
            continue
        tag = f"L{next_tag}"
        next_tag += 1
        spec = _template_cache_candidate_spec(tag, values, family_key)
        if spec is None:
            continue
        placeholder = choose_placeholder(text, dataset, spec.tag)
        score = _line_transducer_candidate_score(values, placeholder, spec)
        if score <= 128:
            continue
        candidates.append((score, count, distinct, family_key, spec, placeholder))

    candidates.sort(key=lambda item: (item[0], item[1], -item[2]), reverse=True)
    if profile == "auto_line_transducer_v3":
        if os.environ.get("PARE_LINE_TRANSDUCER_GLOBAL_MDL", "0") == "1":
            selected = _line_transducer_mdl_select(
                lines,
                alpha_contexts,
                candidates,
                initial_values_by_tag,
                initial_specs_meta,
                dataset,
                max_specs,
            )
            admission = "forward_mdl"
        else:
            selected = _line_transducer_fast_mdl_select(candidates, max_specs)
            admission = "fast_group_mdl"
    else:
        selected = candidates[:max_specs]
        admission = "top_score"

    transformed_text, values_by_tag, line_specs_meta = _line_transducer_apply_selected(
        lines,
        alpha_contexts,
        selected,
        initial_values_by_tag,
    )
    metadata = {
        "dataset": dataset,
        "profile": profile,
        "line_transducer": {
            "min_support": min_support,
            "max_specs": max_specs,
            "alpha_contexts": len(alpha_contexts),
            "candidate_slots": candidate_slots,
            "candidate_count": len(candidates),
            "admitted_slots": len(line_specs_meta),
            "admission": admission,
        },
        "specs": initial_specs_meta + line_specs_meta,
    }
    return transformed_text, values_by_tag, metadata


def discover_auto_extract_specs(
    text: str,
    profile: str = "auto_discovered_v1",
    dataset: str | None = None,
) -> list[ExtractSpec]:
    """Discover a high-performance extraction profile from generic candidates.

    This is deliberately not a dataset-name keyed profile. It emits a small DSL
    of regex-like extractors induced from generic lexical/time/numeric patterns.
    The resulting rules are serialized into the archive metadata, while the
    selected side-stream codecs are still chosen by codelength in
    ``select_auto_stream_spec``.
    """
    if profile == "auto_struct_v0":
        return discover_autostruct_specs(text, codec_aware=False)
    if profile == "auto_struct_v1":
        return discover_autostruct_specs(text, codec_aware=True)
    if profile == "auto_struct_v2":
        return discover_autostruct_specs(text, codec_aware=True, factorization_guard=True)
    if profile == "auto_struct_v3":
        return discover_autostruct_specs(text, codec_aware=True, prefix_competition=True)
    if profile == "auto_struct_v4":
        return discover_autostruct_specs(
            text,
            codec_aware=True,
            prefix_competition=True,
            paired_prefix_inner=True,
        )
    if profile == "auto_struct_v5":
        return discover_autostruct_specs(
            text,
            codec_aware=True,
            prefix_competition=True,
            paired_prefix_inner=True,
            numeric_phrases=True,
        )
    if profile == "auto_struct_v6":
        return discover_autostruct_specs(
            text,
            codec_aware=True,
            prefix_competition=True,
            paired_prefix_inner=True,
            numeric_phrases=True,
            relaxed_numeric_phrases=True,
            colon_terminal_tokens=True,
        )
    if profile == "auto_struct_v7":
        return discover_autostruct_specs(
            text,
            codec_aware=True,
            prefix_competition=True,
            paired_prefix_inner=True,
            numeric_phrases=True,
            relaxed_numeric_phrases=True,
            colon_terminal_tokens=True,
            duration_phrases=True,
            paren_quantities=True,
            max_specs=14,
        )
    if profile == "auto_struct_v8":
        return discover_autostruct_specs(
            text,
            codec_aware=True,
            prefix_competition=True,
            paired_prefix_inner=True,
            numeric_phrases=True,
            relaxed_numeric_phrases=True,
            colon_terminal_tokens=True,
            duration_phrases=True,
            paren_quantities=True,
            joint_context_promote=True,
            max_specs=14,
        )
    if profile == "auto_struct_mdl_v1":
        return discover_autostruct_mdl_specs(text, dataset=dataset)
    if profile == "auto_struct_mdl_v2":
        return discover_autostruct_mdl_specs(text, dataset=dataset, main_proxy="rank")
    if profile == "auto_struct_mdl_v3":
        return discover_autostruct_mdl_augmented_specs(text, dataset=dataset)
    if profile == "auto_struct_mdl_v6":
        return discover_autostruct_joint_beam_specs(text, dataset=dataset)

    line_count = max(1, text.count("\n") + (0 if text.endswith("\n") else 1))
    min_common = max(20, min(line_count // 20, 1000))
    specs: list[ExtractSpec] = []
    used_tags: set[str] = set()

    def add_spec(tag: str, pattern: str, kind: str = "auto", store_group: int = 0, replacement: str | None = None) -> None:
        if tag in used_tags:
            return
        used_tags.add(tag)
        specs.append(ExtractSpec(tag, pattern, kind, store_group=store_group, replacement=replacement))

    # Generic compound datetime candidates. The first catches bracketed full
    # datetimes such as Apache error logs. The second catches syslog-like line
    # prefixes such as OpenSSH/Linux logs. Both are generic datetime formats,
    # not dataset-name keyed profiles.
    bracket_datetime = (
        rf"\[{WEEKDAY_RE}\s+{MONTH_RE}\s+\d{{2}}\s+\d{{2}}:\d{{2}}:\d{{2}}\s+\d{{4}}\]"
    )
    syslog_prefix_datetime = rf"(?m)^({MONTH_RE}\s+\d{{1,2}}\s+\d{{2}}:\d{{2}}:\d{{2}})"
    if regex_count(bracket_datetime, text) >= min_common:
        add_spec("T", bracket_datetime)
    elif regex_count(syslog_prefix_datetime, text) >= min_common:
        add_spec("T", syslog_prefix_datetime, store_group=1, replacement="{placeholder}")

    if profile == "auto_llm_v1":
        for spec in load_llm_extract_specs(dataset or "", text):
            add_spec(spec.tag, spec.pattern, spec.kind, spec.store_group, spec.replacement)

    if profile == "auto_llm_open_v1":
        return load_open_llm_extract_specs(dataset or "", text)

    if profile == "open_function_v1":
        return load_open_function_specs(dataset or "", text)

    if profile in {"auto_discovered_v2", "auto_atis_v1"}:
        # Template/structure proposer: a closed recognizer for bracketed
        # MM.DD HH:MM:SS runs, common in Proxifier-style logs. This is still a
        # generic timestamp recognizer, not a dataset-name keyed rule.
        mmdd_hms = r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}"
        if regex_count(mmdd_hms, text) >= min_common:
            add_spec("T", mmdd_hms)

        # Residual proposer: extract host:port as one compound value before
        # IPv4/domain primitives can split it. This preserves correlation
        # between host and port for repetitive connection logs.
        hostport = (
            r"(?<![\w.-])"
            r"(?:(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+|(?:\d{1,3}\.){3}\d{1,3})"
            r":\d{1,5}(?![\w.-])"
        )
        if regex_count(hostport, text) >= max(20, min_common // 2):
            add_spec("H", hostport)

        # Residual proposer: byte-count and human-readable size fields. These
        # are low-entropy once separated, but they pollute the structural stream
        # when left inline.
        byte_count = r"\b(\d+) bytes\b"
        if (
            regex_count(byte_count, text) >= max(20, min_common // 2)
            and regex_distinct_group_count(byte_count, text) >= 2
        ):
            add_spec("B", byte_count, store_group=1, replacement="{placeholder} bytes")

        short_lifetime = r"<(\d+)\ssec"
        if (
            regex_count(short_lifetime, text) >= max(20, min_common // 2)
            and regex_distinct_group_count(short_lifetime, text) >= 2
        ):
            add_spec("L", short_lifetime, store_group=1, replacement="<{placeholder} sec")

        lifetime_duration = r"\blifetime (\d{2}:\d{2}(?::\d{2})?)\b"
        if (
            regex_count(lifetime_duration, text) >= max(20, min_common // 2)
            and regex_distinct_group_count(lifetime_duration, text) >= 2
        ):
            add_spec("R", lifetime_duration, store_group=1, replacement="lifetime {placeholder}")

        size_paren = r"\(\d+(?:\.\d+)?\s[KMGT]?B\)"
        if regex_count(size_paren, text) >= max(20, min_common // 2):
            add_spec("S", size_paren)

    # Generic IPv4 primitive. This is intentionally broad and not tied to any
    # log source. It stays early so later numeric candidates do not split IPs.
    ipv4_pattern = r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])"
    if regex_count(ipv4_pattern, text) >= max(5, min_common // 4):
        add_spec("A", ipv4_pattern)

    # Generic process-like bracketed integer: token[12345]. This recovers
    # OpenSSH's sshd[pid] without naming sshd.
    proc_pid_pattern = r"(?<=[A-Za-z_][A-Za-z0-9_.-])\[(\d+)\]"
    if regex_count(proc_pid_pattern, text) >= max(20, min_common // 2):
        add_spec("P", proc_pid_pattern, store_group=1, replacement="[{placeholder}]")

    # Data-discovered "word + integer" numeric fields. We only keep a few
    # high-support contexts and require broad numeric variation to avoid
    # extracting incidental constants. The concrete word is learned from the
    # corpus and serialized in the induced rule.
    word_int_counts: Counter[str] = Counter()
    word_int_values: dict[str, list[int]] = {}
    for match in re.finditer(r"\b([A-Za-z][A-Za-z0-9_-]{1,24})([ \t]+)(\d{1,8})\b", text):
        word, _space_text, value_text = match.groups()
        # Avoid common timestamp fragments already handled by compound time.
        if word in MONTH_NAMES or word in {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}:
            continue
        value = int(value_text)
        word_int_counts[word] += 1
        word_int_values.setdefault(word, []).append(value)

    numeric_candidates: list[tuple[int, int, str]] = []
    for word, count in word_int_counts.items():
        values = word_int_values[word]
        distinct = len(set(values))
        if count < max(50, min_common // 2):
            continue
        if distinct < min(8, max(2, count // 20)):
            continue
        if max(values) - min(values) < 8:
            continue
        numeric_candidates.append((count, distinct, word))
    numeric_candidates.sort(reverse=True)

    next_numeric = 0
    for _count, _distinct, word in numeric_candidates[:3]:
        tag = "O" if next_numeric == 0 else f"O{next_numeric}"
        next_numeric += 1
        escaped_word = re.escape(word)
        add_spec(
            tag,
            rf"\b{escaped_word}([ \t]+)(\d{{1,8}})\b",
            store_group=2,
            replacement=f"{word}" + "{group1}{placeholder}",
        )

    # Generic domain/host strings with at least two dots. This recovers reverse
    # DNS names in OpenSSH and similar host-like variables. It intentionally
    # runs after numeric-after-word discovery so port-like streams can choose IP
    # or PID contexts before categorical host contexts enter the candidate set.
    domain_pattern = r"(?<![\w-])(?:[A-Za-z0-9-]+\.){2,}[A-Za-z0-9-]+(?![\w-])"
    if regex_count(domain_pattern, text) >= max(20, min_common // 2):
        add_spec("D", domain_pattern)

    return specs


def load_atis_proposals(dataset: str) -> list[dict[str, object]]:
    proposal_path = os.environ.get("PARE_ATIS_PROPOSAL_JSON", "")
    if not proposal_path:
        return []
    path = Path(proposal_path)
    if not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    proposals = payload.get("proposals", []) if isinstance(payload, dict) else []
    if not isinstance(proposals, list):
        return []
    valid: list[dict[str, object]] = []
    for proposal in proposals:
        if not isinstance(proposal, dict):
            continue
        proposal_dataset = str(proposal.get("dataset", ""))
        if proposal_dataset and proposal_dataset != dataset:
            continue
        valid.append(proposal)
    return valid


def load_llm_extract_specs(dataset: str, text: str) -> list[ExtractSpec]:
    """Load allowlisted LLM-proposed recognizers.

    The LLM is allowed to propose only closed recognizer kinds. It cannot inject
    arbitrary regexes or decoder behavior. Unsupported proposals are ignored.
    """
    proposal_path = os.environ.get("PARE_LLM_PROPOSAL_JSON")
    proposal_dir = os.environ.get("PARE_LLM_PROPOSAL_DIR")
    path: Path | None = None
    if proposal_dir and dataset:
        candidate = Path(proposal_dir) / f"{dataset}.json"
        if candidate.is_file():
            path = candidate
    if path is None and proposal_path:
        candidate = Path(proposal_path)
        if candidate.is_file():
            path = candidate
    if path is None:
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    raw_proposals = payload.get("proposals", []) if isinstance(payload, dict) else []
    if not isinstance(raw_proposals, list):
        return []

    specs: list[ExtractSpec] = []
    used_tags: set[str] = set()
    for index, proposal in enumerate(raw_proposals):
        if not isinstance(proposal, dict):
            continue
        proposal_dataset = str(proposal.get("dataset", "")).strip()
        if proposal_dataset and dataset and proposal_dataset != dataset:
            continue
        kind = str(proposal.get("kind", "")).strip().upper()
        tag = str(proposal.get("tag", "")).strip().upper()
        if not re.fullmatch(r"[A-Z][A-Z0-9]{0,3}", tag):
            tag = default_tag_for_llm_kind(kind, index)
        if tag in used_tags:
            tag = f"X{index}"
        if not re.fullmatch(r"[A-Z][A-Z0-9]{0,3}", tag) or tag in used_tags:
            continue
        spec = llm_kind_to_extract_spec(kind, tag)
        if spec is None:
            continue
        if regex_count(spec.pattern, text) == 0:
            continue
        used_tags.add(tag)
        specs.append(spec)
    return specs


def _safe_replacement_format(replacement: str, placeholder: str, match: re.Match[str]) -> str:
    groups = {f"group{index}": match.group(index) for index in range(0, len(match.groups()) + 1)}
    return replacement.format(placeholder=placeholder, **groups)


def load_open_function_specs(dataset: str, text: str) -> list[ExtractSpec]:
    """Load verifier-gated open transformation functions.

    Each function is still a deterministic archive program: a regex identifies
    a byte span, a placeholder replaces that span in the main stream, and the
    side stream is encoded by the serialized function program.  The decoder
    never calls an LLM; it only interprets the small checked program stored in
    metadata.
    """
    proposal_dir = os.environ.get("PARE_OPEN_FUNCTION_DIR", "")
    proposal_path = os.environ.get("PARE_OPEN_FUNCTION_JSON", "")
    path: Path | None = None
    if proposal_dir:
        candidate = Path(proposal_dir) / f"{dataset}.json"
        if candidate.is_file():
            path = candidate
    if path is None and proposal_path:
        candidate = Path(proposal_path)
        if candidate.is_file():
            path = candidate
    if path is None:
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    functions = payload.get("functions", []) if isinstance(payload, dict) else []
    if not isinstance(functions, list):
        return []
    incremental_validate = os.environ.get("PARE_OPEN_FUNCTION_INCREMENTAL_VALIDATE", "0") == "1"
    specs: list[ExtractSpec] = []
    used_tags: set[str] = set()
    validation_text = text
    for index, item in enumerate(functions):
        if not isinstance(item, dict):
            continue
        tag = canonicalize_open_llm_tag(str(item.get("tag", f"F{index}")), used_tags, index)
        if tag is None:
            continue
        pattern = str(item.get("regex", ""))
        replacement = str(item.get("replacement", "{placeholder}"))
        if not is_open_llm_regex_safe(pattern) or not is_open_llm_replacement_safe(replacement):
            continue
        program = item.get("program", {})
        if not isinstance(program, dict) or not is_open_function_program_safe(program):
            continue
        try:
            regex = re.compile(pattern, flags=re.MULTILINE)
            validation_source = validation_text if incremental_validate else text
            matches = list(regex.finditer(validation_source))
            if not matches:
                continue
            _safe_replacement_format(replacement, "<X>", matches[0])
            # L4 verifier gate: reject proposals whose serialized program is
            # not an exact inverse on a bounded sample.  Full-file SHA
            # verification still remains the final authority.
            store_group = infer_open_function_store_group(matches, program, replacement)
            if store_group is None:
                raise ValueError("Open function has no exactly invertible group")
            if replacement_duplicates_stored_group(replacement, store_group):
                raise ValueError("Open function duplicates the stored group")
        except Exception:
            continue
        op = str(program.get("op", ""))
        compiled_kind = "auto" if op == "auto_codec" else "open_function"
        if op == "python_exec" and python_exec_is_raw_identity(matches, program, store_group):
            compiled_kind = "auto"
        used_tags.add(tag)
        if incremental_validate:
            placeholder = choose_placeholder(text, dataset, tag)

            def validation_replace(match: re.Match[str]) -> str:
                return _safe_replacement_format(replacement, placeholder, match)

            validation_text = regex.sub(validation_replace, validation_text)
        specs.append(ExtractSpec(
            tag=tag,
            pattern=pattern,
            kind=compiled_kind,
            store_group=store_group,
            replacement=replacement,
            context_tag=json.dumps(program, sort_keys=True) if compiled_kind == "open_function" else None,
        ))
    if os.environ.get("PARE_OPEN_FUNCTION_MDL_SELECT", "0") == "1":
        specs = select_open_function_specs_by_proxy(text, dataset, specs)
    return specs


def python_exec_is_raw_identity(matches: list[re.Match[str]], program: dict[str, object], store_group: int) -> bool:
    """Return True when a generated python_exec only stores the captured bytes.

    This is not a fixed recognizer.  It is a compiler lowering rule: LLM still
    supplies the regex and executable function, while the backend routes exact
    raw identity values through the same codec selector used by auto streams.
    """
    if str(program.get("op", "")) != "python_exec":
        return False
    checked = 0
    for match in matches[:64]:
        try:
            raw_value = match.group(store_group)
            record = run_python_exec_forward(raw_value, program)
            if record.get("layout"):
                return False
            stored = record.get("stored", [])
            if len(stored) != 1 or not isinstance(stored[0], str):
                return False
            if stored[0] != raw_value:
                return False
            if run_python_exec_inverse(stored[0], program) != raw_value:
                return False
            checked += 1
        except Exception:
            return False
    return checked > 0


SAFE_OPEN_FUNCTION_OPS = {
    "auto_codec",
    "datetime_strptime_delta",
    "syslog_delta",
    "asctime_year_delta",
    "month_day_hms_delta",
    "hms_delta",
    "day_hms_delta",
    "int_expr_delta",
    "python_exec",
}


def is_open_function_program_safe(program: dict[str, object]) -> bool:
    op = str(program.get("op", ""))
    if op not in SAFE_OPEN_FUNCTION_OPS:
        return False
    payload = json.dumps(program, sort_keys=True)
    max_payload = (
        12288
        if op == "python_exec" and program.get("context_code")
        else (8192 if op == "python_exec" else 2048)
    )
    if len(payload.encode("utf-8")) > max_payload:
        return False
    if op == "datetime_strptime_delta":
        return bool(program.get("format")) and bool(program.get("render_format"))
    if op == "auto_codec":
        return True
    if op == "int_expr_delta":
        return bool(program.get("value_regex")) and bool(program.get("value_expr")) and bool(program.get("render"))
    if op == "python_exec":
        code = program.get("code")
        if not isinstance(code, str) or not code.strip():
            return False
        try:
            compile_generated_python_exec(code)
            context_code = program.get("context_code")
            if context_code is not None:
                if not isinstance(context_code, str) or not context_code.strip():
                    return False
                compile_generated_context_projector(context_code)
        except Exception:
            return False
    return True


SAFE_PYTHON_EXEC_NODES = (
    ast.Module,
    ast.FunctionDef,
    ast.arguments,
    ast.arg,
    ast.Return,
    ast.Assign,
    ast.Expr,
    ast.If,
    ast.For,
    ast.While,
    ast.Break,
    ast.Continue,
    ast.Pass,
    ast.Load,
    ast.Store,
    ast.Name,
    ast.Constant,
    ast.List,
    ast.Tuple,
    ast.Dict,
    ast.Subscript,
    ast.Slice,
    ast.Index,
    ast.BinOp,
    ast.UnaryOp,
    ast.BoolOp,
    ast.Compare,
    ast.IfExp,
    ast.JoinedStr,
    ast.FormattedValue,
    ast.Call,
    ast.Attribute,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.Pow,
    ast.LShift,
    ast.RShift,
    ast.UAdd,
    ast.USub,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.And,
    ast.Or,
)


def safe_generated_python_import(name: str, globals=None, locals=None, fromlist=(), level: int = 0):
    # datetime.strptime imports _strptime lazily.  Generated code itself is
    # still forbidden from naming __import__ by the AST guard.
    if name in {"_strptime", "time"}:
        return __import__(name, globals, locals, fromlist, level)
    raise ImportError(f"import not allowed in generated function: {name}")


SAFE_PYTHON_EXEC_GLOBALS = {
    "datetime": datetime,
    "timedelta": timedelta,
    "int": int,
    "str": str,
    "len": len,
    "float": float,
    "round": round,
    "abs": abs,
    "min": min,
    "max": max,
    "__import__": safe_generated_python_import,
}
BLOCKED_PYTHON_EXEC_NAMES = {"open", "eval", "exec", "compile", "globals", "locals", "vars", "__import__", "input"}
COMPILED_PYTHON_EXEC_CACHE: dict[str, tuple[object, object]] = {}
COMPILED_CONTEXT_PROJECTOR_CACHE: dict[str, object] = {}


class _GeneratedPythonNormalizer(ast.NodeTransformer):
    """Rewrite common safe Python shorthands into the verifier's tiny subset."""

    def visit_AugAssign(self, node: ast.AugAssign) -> ast.AST:
        self.generic_visit(node)
        if not isinstance(node.target, ast.Name):
            return node
        target_load = ast.Name(id=node.target.id, ctx=ast.Load())
        value = ast.BinOp(left=target_load, op=node.op, right=node.value)
        return ast.copy_location(ast.Assign(targets=[node.target], value=value), node)


def compile_generated_python_exec(code: str) -> tuple[object, object]:
    cached = COMPILED_PYTHON_EXEC_CACHE.get(code)
    if cached is not None:
        return cached
    tree = _GeneratedPythonNormalizer().visit(ast.parse(code))
    tree = ast.fix_missing_locations(tree)
    for node in ast.walk(tree):
        if not isinstance(node, SAFE_PYTHON_EXEC_NODES):
            raise ValueError(f"unsafe generated Python node: {type(node).__name__}")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ValueError("imports are not allowed in generated Python functions")
        if isinstance(node, ast.Name) and (node.id.startswith("__") or node.id in BLOCKED_PYTHON_EXEC_NAMES):
            raise ValueError(f"unsafe generated Python name: {node.id}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise ValueError(f"unsafe generated Python attribute: {node.attr}")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in BLOCKED_PYTHON_EXEC_NAMES:
            raise ValueError(f"unsafe generated Python call: {node.func.id}")
    env: dict[str, object] = {"__builtins__": SAFE_PYTHON_EXEC_GLOBALS}
    env.update(SAFE_PYTHON_EXEC_GLOBALS)
    exec(compile(tree, "<llm_python_exec>", "exec"), env, env)
    forward = env.get("forward")
    inverse = env.get("inverse")
    if not callable(forward) or not callable(inverse):
        raise ValueError("generated Python must define forward(groups) and inverse(record)")
    compiled = (forward, inverse)
    COMPILED_PYTHON_EXEC_CACHE[code] = compiled
    return compiled


def compile_generated_context_projector(code: str) -> object:
    """Compile an LLM-generated per-line context projection function.

    The projector may inspect the complete log line and the target regex groups,
    but it cannot mutate the target value or participate in reconstruction.
    """
    cached = COMPILED_CONTEXT_PROJECTOR_CACHE.get(code)
    if cached is not None:
        return cached
    tree = _GeneratedPythonNormalizer().visit(ast.parse(code))
    tree = ast.fix_missing_locations(tree)
    for node in ast.walk(tree):
        if not isinstance(node, SAFE_PYTHON_EXEC_NODES):
            raise ValueError(f"unsafe context projector Python node: {type(node).__name__}")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            raise ValueError("imports are not allowed in generated context projectors")
        if isinstance(node, ast.Name) and (node.id.startswith("__") or node.id in BLOCKED_PYTHON_EXEC_NAMES):
            raise ValueError(f"unsafe generated Python name: {node.id}")
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            raise ValueError(f"unsafe generated Python attribute: {node.attr}")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in BLOCKED_PYTHON_EXEC_NAMES:
            raise ValueError(f"unsafe generated Python call: {node.func.id}")
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "re"
        ):
            if node.func.attr not in {"search", "match", "fullmatch"}:
                raise ValueError(f"unsupported context projector regex operation: {node.func.attr}")
            if not node.args or not isinstance(node.args[0], ast.Constant) or not isinstance(node.args[0].value, str):
                raise ValueError("context projector regex must be a literal string")
            if not is_open_llm_regex_safe(str(node.args[0].value)):
                raise ValueError("unsafe context projector regex")
    context_globals = dict(SAFE_PYTHON_EXEC_GLOBALS)
    context_globals["re"] = re
    env: dict[str, object] = {"__builtins__": context_globals}
    env.update(context_globals)
    exec(compile(tree, "<llm_context_projector>", "exec"), env, env)
    projector = env.get("project_context")
    if not callable(projector):
        raise ValueError("generated context code must define project_context(line, groups)")
    COMPILED_CONTEXT_PROJECTOR_CACHE[code] = projector
    return projector


def run_python_exec_context_projector(
    line: str,
    groups: list[str],
    program: dict[str, object],
) -> str | None:
    context_code = program.get("context_code")
    if not isinstance(context_code, str) or not context_code.strip():
        return None
    projector = compile_generated_context_projector(context_code)
    projected = projector(line, list(groups))
    if projected is None:
        return None
    if isinstance(projected, bool) or not isinstance(projected, (str, int)):
        raise ValueError("project_context(line, groups) must return str, int, or None")
    value = str(projected)
    if len(value.encode("utf-8")) > 1024:
        raise ValueError("context projector value exceeds 1024 bytes")
    return value


def run_python_exec_forward(raw_value: str, program: dict[str, object]) -> dict[str, object]:
    code = str(program["code"])
    forward, _inverse = compile_generated_python_exec(code)
    groups = [raw_value]
    group_regex = program.get("group_regex")
    if isinstance(group_regex, str) and group_regex:
        match = re.fullmatch(group_regex, raw_value)
        if match is None:
            raise ValueError("python_exec group_regex did not match raw value")
        groups = list(match.groups()) or [raw_value]
    record = forward(groups)
    if not isinstance(record, dict):
        raise ValueError("forward(groups) must return a dict")
    stored = record.get("stored", [])
    layout = record.get("layout", [])
    if not isinstance(stored, list) or not isinstance(layout, list):
        raise ValueError("forward(groups) must return list fields stored/layout")
    if len(stored) != 1:
        raise ValueError("python_exec currently supports exactly one stored scalar")
    normalized_layout: list[int] = []
    for item in layout:
        value = int(item)
        if value < 0 or value > 255:
            raise ValueError("python_exec layout entries must fit in one byte")
        normalized_layout.append(value)
    return {"stored": stored, "layout": normalized_layout}


def run_python_exec_forward_groups(groups: list[str], program: dict[str, object]) -> dict[str, object]:
    code = str(program["code"])
    forward, _inverse = compile_generated_python_exec(code)
    record = forward(list(groups))
    if not isinstance(record, dict):
        raise ValueError("forward(groups) must return a dict")
    stored = record.get("stored", [])
    layout = record.get("layout", [])
    if not isinstance(stored, list) or not isinstance(layout, list):
        raise ValueError("forward(groups) must return list fields stored/layout")
    if len(stored) != 1:
        raise ValueError("python_exec currently supports exactly one stored scalar")
    normalized_layout: list[int] = []
    for item in layout:
        value = int(item)
        if value < 0 or value > 255:
            raise ValueError("python_exec layout entries must fit in one byte")
        normalized_layout.append(value)
    return {"stored": stored, "layout": normalized_layout}


def run_python_exec_inverse(value: object, program: dict[str, object], layout: list[int] | None = None) -> str:
    code = str(program["code"])
    _forward, inverse = compile_generated_python_exec(code)
    rendered = inverse({"stored": [value], "layout": list(layout or [])})
    if not isinstance(rendered, str):
        raise ValueError("inverse(record) must return a string")
    return rendered


def open_function_program(spec: ExtractSpec | dict[str, object]) -> dict[str, object]:
    if isinstance(spec, ExtractSpec):
        raw = spec.context_tag or "{}"
    else:
        raw = str(spec.get("program", "{}"))
    payload = json.loads(raw)
    if not isinstance(payload, dict) or not is_open_function_program_safe(payload):
        raise ValueError("Unsafe open function program")
    return payload


def safe_eval_int_expr(expr: str, variables: dict[str, int | str]) -> int:
    tree = ast.parse(expr, mode="eval")

    def eval_node(node: ast.AST) -> int:
        if isinstance(node, ast.Expression):
            return eval_node(node.body)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, int):
                return int(node.value)
            if isinstance(node.value, str) and node.value.isdigit():
                return int(node.value)
            raise ValueError("Bad constant in open function expression")
        if isinstance(node, ast.Name):
            value = variables[node.id]
            return int(value)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = eval_node(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.FloorDiv, ast.Mod, ast.Pow, ast.LShift, ast.RShift)):
            left = eval_node(node.left)
            right = eval_node(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Pow):
                if right < 0 or right > 16:
                    raise ValueError("Unsafe exponent in open function expression")
                return left ** right
            if isinstance(node.op, ast.LShift):
                if right < 0 or right > 63:
                    raise ValueError("Unsafe left shift in open function expression")
                return left << right
            if isinstance(node.op, ast.RShift):
                if right < 0 or right > 63:
                    raise ValueError("Unsafe right shift in open function expression")
                return left >> right
            if right == 0:
                raise ValueError("Division by zero in open function expression")
            if isinstance(node.op, ast.FloorDiv):
                return left // right
            return left % right
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "int" and len(node.args) in {1, 2}:
            if len(node.args) == 1:
                return eval_node(node.args[0])
            raw_node, base_node = node.args
            if not isinstance(raw_node, ast.Name):
                raise ValueError("int(x, base) only accepts a captured group")
            base = eval_node(base_node)
            if base not in {2, 8, 10, 16}:
                raise ValueError("Unsupported int base in open function expression")
            return int(str(variables[raw_node.id]), base)
        raise ValueError("Unsafe open function expression")

    return eval_node(tree)


def render_int_expr_template(template: str, value: int) -> str:
    def replace(match: re.Match[str]) -> str:
        expr = match.group(1)
        fmt = match.group(2) or ""
        rendered_value = safe_eval_int_expr(expr, {"v": value})
        if fmt:
            return ("{:" + fmt + "}").format(rendered_value)
        return str(rendered_value)

    return re.sub(r"\{([^{}:]+)(?::([^{}]+))?\}", replace, template)


def _datetime_format_fraction_regex(fmt: str) -> re.Pattern[str]:
    """Build a narrow matcher that locates the %f field in a strptime format."""
    parts: list[str] = []
    fraction_seen = False
    index = 0
    directive_patterns = {
        "Y": r"\d{4}",
        "y": r"\d{2}",
        "m": r"\d{1,2}",
        "d": r"\d{1,2}",
        "H": r"\d{1,2}",
        "I": r"\d{1,2}",
        "M": r"\d{1,2}",
        "S": r"\d{1,2}",
        "j": r"\d{1,3}",
        "b": r"[A-Za-z]+",
        "B": r"[A-Za-z]+",
        "a": r"[A-Za-z]+",
        "A": r"[A-Za-z]+",
        "p": r"[AP]M",
        "z": r"(?:Z|[+-]\d{2}:?\d{2})",
        "Z": r"[A-Za-z_+/-]+",
    }
    while index < len(fmt):
        char = fmt[index]
        if char != "%" or index + 1 >= len(fmt):
            parts.append(re.escape(char))
            index += 1
            continue
        directive = fmt[index + 1]
        if directive == "%":
            parts.append(re.escape("%"))
        elif directive == "f":
            if fraction_seen:
                parts.append(r"\d{1,6}")
            else:
                parts.append(r"(?P<fraction>\d{1,6})")
                fraction_seen = True
        else:
            parts.append(directive_patterns.get(directive, r".+?"))
        index += 2
    return re.compile("".join(parts))


def datetime_fraction_width(raw_value: str, program: dict[str, object]) -> int:
    fmt = str(program.get("format", ""))
    if "%f" not in fmt:
        return 0
    match = _datetime_format_fraction_regex(fmt).fullmatch(raw_value)
    if match is None:
        return 6
    return len(match.group("fraction"))


def _datetime_seconds_since_epoch(dt: datetime) -> int:
    if dt.tzinfo is not None:
        return int(dt.replace(microsecond=0).timestamp())
    delta = dt.replace(microsecond=0) - NAIVE_DATETIME_EPOCH
    return delta.days * 24 * 3600 + delta.seconds


def parse_datetime_strptime_value(
    raw_value: str,
    program: dict[str, object],
    fraction_scale: int | None = None,
) -> int:
    fmt = str(program["format"])
    if fmt == "%a %b %d %H:%M:%S %Y":
        try:
            dt = datetime(
                int(raw_value[20:24]),
                MONTH_TO_INDEX[raw_value[4:7]],
                int(raw_value[8:10]),
                int(raw_value[11:13]),
                int(raw_value[14:16]),
                int(raw_value[17:19]),
            )
            return int(dt.timestamp())
        except Exception:
            pass
    dt = datetime.strptime(raw_value, fmt)
    if "%f" not in fmt:
        return int(dt.timestamp())
    scale_digits = 6 if fraction_scale is None else int(fraction_scale)
    if scale_digits < 1 or scale_digits > 6:
        raise ValueError(f"Bad datetime fraction scale {scale_digits}")
    seconds = _datetime_seconds_since_epoch(dt)
    fractional_units = dt.microsecond // (10 ** (6 - scale_digits))
    return seconds * (10 ** scale_digits) + fractional_units


def render_datetime_strptime_value(
    value: int,
    program: dict[str, object],
    fraction_width: int | None = None,
    fraction_scale: int | None = None,
) -> str:
    fmt = str(program.get("format", ""))
    render_fmt = str(program["render_format"])
    if "%f" not in fmt and "%f" not in render_fmt:
        return datetime.fromtimestamp(value).strftime(render_fmt)
    scale_digits = 6 if fraction_scale is None else int(fraction_scale)
    if scale_digits < 1 or scale_digits > 6:
        raise ValueError(f"Bad datetime fraction scale {scale_digits}")
    seconds, fractional_units = divmod(value, 10 ** scale_digits)
    microsecond = fractional_units * (10 ** (6 - scale_digits))
    dt = NAIVE_DATETIME_EPOCH + timedelta(seconds=seconds, microseconds=microsecond)
    rendered = dt.strftime(render_fmt)
    if "%f" in render_fmt and fraction_width is not None:
        width = int(fraction_width)
        if width < 1 or width > 6:
            raise ValueError(f"Bad datetime fraction width {width}")
        full_fraction = f"{microsecond:06d}"
        rendered = rendered.replace(full_fraction, full_fraction[:width], 1)
    return rendered


def _open_function_program_cache_key(program: dict[str, object]) -> str:
    return json.dumps(program, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


@lru_cache(maxsize=262144)
def _compiled_value_regex(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern)


@lru_cache(maxsize=262144)
def _parse_open_function_value_cached(
    raw_value: str,
    program_key: str,
    fraction_scale: int | None,
) -> int:
    return _parse_open_function_value_uncached(raw_value, json.loads(program_key), fraction_scale=fraction_scale)


@lru_cache(maxsize=262144)
def _render_open_function_exact_cached(raw_value: str, program_key: str) -> str:
    return _render_open_function_exact_uncached(raw_value, json.loads(program_key))


def open_function_cache_info() -> dict[str, dict[str, int]]:
    parse_info = _parse_open_function_value_cached.cache_info()._asdict()
    render_info = _render_open_function_exact_cached.cache_info()._asdict()
    regex_info = _compiled_value_regex.cache_info()._asdict()
    return {
        "parse": {key: int(value) for key, value in parse_info.items()},
        "render_exact": {key: int(value) for key, value in render_info.items()},
        "value_regex": {key: int(value) for key, value in regex_info.items()},
    }


def _parse_open_function_value_uncached(
    raw_value: str,
    program: dict[str, object],
    fraction_scale: int | None = None,
) -> int:
    op = str(program["op"])
    if op == "auto_codec":
        raise ValueError("auto_codec uses the raw extracted value stream")
    if op == "datetime_strptime_delta":
        return parse_datetime_strptime_value(raw_value, program, fraction_scale=fraction_scale)
    if op == "syslog_delta":
        return parse_syslog_timestamp(raw_value)[0]
    if op == "asctime_year_delta":
        return parse_asctime_year_timestamp(raw_value)
    if op == "month_day_hms_delta":
        return parse_month_day_hms(raw_value)
    if op == "hms_delta":
        return parse_hms(raw_value)
    if op == "day_hms_delta":
        day, _width, seconds = parse_day_hms(raw_value)
        return day * 24 * 3600 + seconds
    if op == "python_exec":
        record = run_python_exec_forward(raw_value, program)
        return int(record["stored"][0])
    if op == "int_expr_delta":
        value_regex = str(program["value_regex"])
        value_expr = str(program["value_expr"]).replace(" ", "")
        if value_regex == r"\d+" and value_expr in {"int(g0)", "g0"}:
            return int(raw_value)
        regex = _compiled_value_regex(str(program["value_regex"]))
        match = regex.fullmatch(raw_value)
        if match is None:
            raise ValueError(f"Open function value regex did not match {raw_value!r}")
        variables: dict[str, int | str] = {f"g{index}": match.group(index) for index in range(0, len(match.groups()) + 1)}
        variables.update({key: value for key, value in match.groupdict().items() if value is not None})
        return safe_eval_int_expr(str(program["value_expr"]), variables)
    raise ValueError(f"Unsupported open function op {op!r}")


def parse_open_function_value(
    raw_value: str,
    program: dict[str, object],
    fraction_scale: int | None = None,
) -> int:
    if str(program["op"]) == "auto_codec":
        raise ValueError("auto_codec uses the raw extracted value stream")
    return _parse_open_function_value_cached(
        raw_value,
        _open_function_program_cache_key(program),
        fraction_scale,
    )


def render_open_function_value(
    value: object,
    program: dict[str, object],
    fraction_width: int | None = None,
    fraction_scale: int | None = None,
    layout: list[int] | None = None,
) -> str:
    op = str(program["op"])
    if op == "auto_codec":
        raise ValueError("auto_codec uses the raw extracted value stream")
    if op == "datetime_strptime_delta":
        return render_datetime_strptime_value(
            value,
            program,
            fraction_width=fraction_width,
            fraction_scale=fraction_scale,
        )
    if op == "syslog_delta":
        return format_syslog_timestamp(value, int(program.get("layout", 18)))
    if op == "asctime_year_delta":
        return format_asctime_year_timestamp(value)
    if op == "month_day_hms_delta":
        return format_month_day_hms(value)
    if op == "hms_delta":
        return format_hms(value)
    if op == "day_hms_delta":
        width = int(program.get("day_width", 2))
        return format_day_hms(value, width)
    if op == "python_exec":
        return run_python_exec_inverse(value, program, layout=layout)
    if op == "int_expr_delta":
        return render_int_expr_template(str(program["render"]), value)
    raise ValueError(f"Unsupported open function op {op!r}")


def _render_open_function_exact_uncached(raw_value: str, program: dict[str, object]) -> str:
    op = str(program["op"])
    if op == "auto_codec":
        return raw_value
    if op == "python_exec":
        record = run_python_exec_forward(raw_value, program)
        return run_python_exec_inverse(record["stored"][0], program, layout=list(record.get("layout", [])))
    if op == "syslog_delta":
        value, layout = parse_syslog_timestamp(raw_value)
        return format_syslog_timestamp(value, layout)
    if op == "asctime_year_delta":
        return format_asctime_year_timestamp(parse_asctime_year_timestamp(raw_value))
    if op == "day_hms_delta":
        day, width, seconds = parse_day_hms(raw_value)
        return format_day_hms(day * 24 * 3600 + seconds, width)
    if op == "datetime_strptime_delta" and "%f" in str(program.get("format", "")):
        width = datetime_fraction_width(raw_value, program)
        value = _parse_open_function_value_uncached(raw_value, program, fraction_scale=6)
        return render_open_function_value(value, program, fraction_width=width, fraction_scale=6)
    value = _parse_open_function_value_uncached(raw_value, program)
    return render_open_function_value(value, program)


def render_open_function_exact(raw_value: str, program: dict[str, object]) -> str:
    if str(program["op"]) == "auto_codec":
        return raw_value
    return _render_open_function_exact_cached(raw_value, _open_function_program_cache_key(program))


def render_open_function_placeholder_values(
    raw_values: list[str],
    program: dict[str, object],
) -> list[str]:
    """Return the bytes that belong at the function's placeholder route.

    A function may capture its complete regex span while its replacement keeps
    stable prefix/suffix bytes in the main stream. String-codec fallbacks must
    therefore store inverse(forward(span)), not the complete captured span.
    """
    return [render_open_function_exact(value, program) for value in raw_values]


def infer_open_function_store_group(matches: list[re.Match[str]], program: dict[str, object], replacement: str) -> int | None:
    if not matches:
        return None
    group_count = len(matches[0].groups())
    for group_index in range(group_count + 1):
        ok = True
        for match in matches:
            raw_value = match.group(group_index)
            if raw_value is None:
                ok = False
                break
            try:
                rendered_value = render_open_function_exact(raw_value, program)
                if _safe_replacement_format(replacement, rendered_value, match) != match.group(0):
                    ok = False
                    break
            except Exception:
                ok = False
                break
        if ok:
            return group_index
    return None


def is_open_llm_regex_safe(pattern: str) -> bool:
    """Conservative static guard for LLM-proposed regexes.

    This is intentionally not a semantic whitelist. It rejects regex features
    that are dangerous or hard to audit, while still allowing the LLM to propose
    new lexical formats.
    """
    if not pattern or len(pattern) > 260:
        return False
    forbidden_fragments = [
        "(?P",
        "(?(",
        "\\1",
        "\\2",
        "\\3",
        "\\4",
        "\\5",
        "\\6",
        "\\7",
        "\\8",
        "\\9",
    ]
    if any(fragment in pattern for fragment in forbidden_fragments):
        return False
    # Avoid nested quantified groups such as (.*)+, a common ReDoS trigger.
    if re.search(r"\([^)]*[*+][^)]*\)[*+{]", pattern):
        return False
    return True


def is_open_llm_replacement_safe(replacement: str) -> bool:
    if not replacement or "{placeholder}" not in replacement or len(replacement) > 180:
        return False
    allowed = {"placeholder", "group0", "group1", "group2", "group3", "group4", "group5", "group6"}
    for field in re.findall(r"{([^{}]+)}", replacement):
        if field not in allowed:
            return False
    return True


def replacement_duplicates_stored_group(replacement: str, store_group: int) -> bool:
    """Reject replacements that store and preserve the same changing bytes.

    If the placeholder is reconstructed from group N, the template must not also
    keep {groupN}. Literal context is useful as an anchor, but duplicated value
    bytes hurt compression and can make the program semantically ambiguous.
    """
    if store_group == 0:
        return bool(re.search(r"{group[0-6]}", replacement))
    return f"{{group{store_group}}}" in replacement


def canonicalize_open_llm_tag(raw_tag: str, used_tags: set[str], fallback_index: int) -> str | None:
    """Repair harmless LLM tag collisions instead of dropping the extractor.

    The tag is only a stream identifier serialized into metadata. If an LLM
    proposes two useful numeric streams both named "O", keeping one and silently
    discarding the other loses compression opportunities. We therefore preserve
    the semantic prefix when possible and add a numeric suffix.
    """
    tag = raw_tag.strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9]{0,3}", tag):
        tag = f"X{fallback_index}"
    if tag not in used_tags and re.fullmatch(r"[A-Z][A-Z0-9]{0,3}", tag):
        return tag
    prefix_match = re.match(r"[A-Z]+", tag)
    prefix = prefix_match.group(0) if prefix_match else "X"
    prefix = prefix[:2] if prefix else "X"
    for suffix in range(1, 100):
        candidate = f"{prefix}{suffix}"
        if len(candidate) <= 4 and candidate not in used_tags:
            return candidate
    for suffix in range(fallback_index, fallback_index + 100):
        candidate = f"X{suffix}"
        if len(candidate) <= 4 and candidate not in used_tags:
            return candidate
    return None


def load_open_llm_extract_specs(dataset: str, text: str) -> list[ExtractSpec]:
    """Load open LLM-proposed extraction programs after verifier checks.

    The LLM can propose new regexes and replacements, but the compressor only
    accepts proposals that compile, match the current corpus, have safe
    replacement templates, and pass a small formatting smoke test.
    """
    proposal_path = os.environ.get("PARE_LLM_OPEN_PROPOSAL_JSON")
    proposal_dir = os.environ.get("PARE_LLM_OPEN_PROPOSAL_DIR")
    path: Path | None = None
    if proposal_dir and dataset:
        candidate = Path(proposal_dir) / f"{dataset}.json"
        if candidate.is_file():
            path = candidate
    if path is None and proposal_path:
        candidate = Path(proposal_path)
        if candidate.is_file():
            path = candidate
    if path is None:
        return []

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    programs = payload.get("programs", []) if isinstance(payload, dict) else []
    if not isinstance(programs, list):
        return []

    specs: list[ExtractSpec] = []
    used_tags: set[str] = set()
    for program in programs:
        if not isinstance(program, dict):
            continue
        program_dataset = str(program.get("dataset", "")).strip()
        if program_dataset and dataset and program_dataset != dataset:
            continue
        extractors = program.get("extractors", [])
        if not isinstance(extractors, list):
            continue
        for extractor_index, extractor in enumerate(extractors):
            if not isinstance(extractor, dict):
                continue
            tag = canonicalize_open_llm_tag(str(extractor.get("tag", "")), used_tags, len(specs) + extractor_index)
            if tag is None:
                continue
            pattern = str(extractor.get("regex", ""))
            replacement = str(extractor.get("replacement", "{placeholder}"))
            try:
                store_group = int(extractor.get("store_group", 0))
            except Exception:
                store_group = 0
            if not is_open_llm_regex_safe(pattern) or not is_open_llm_replacement_safe(replacement):
                continue
            try:
                regex = re.compile(pattern, flags=re.MULTILINE)
                match = regex.search(text)
            except Exception:
                continue
            if match is None:
                continue
            if store_group < 0 or store_group > len(match.groups()):
                continue
            try:
                _safe_replacement_format(replacement, "<X>", match)
            except Exception:
                continue
            used_tags.add(tag)
            specs.append(ExtractSpec(tag, pattern, "auto", store_group=store_group, replacement=replacement))

    slice_root = os.environ.get("PARE_OPEN_VERIFY_SLICE_ROOT", "")
    if slice_root and specs:
        try:
            import pare_open_verify

            cache_dir_text = os.environ.get("PARE_OPEN_VERIFY_CACHE_DIR", "")
            cache_dir = Path(cache_dir_text) if cache_dir_text else None
            force_recompute = os.environ.get("PARE_OPEN_VERIFY_NO_CACHE", "0") == "1"
            verified_specs, decisions, _cache_path = pare_open_verify.load_verified_specs_cached(
                dataset,
                specs,
                Path(slice_root),
                cache_dir=cache_dir,
                force_recompute=force_recompute,
            )
            report_dir = os.environ.get("PARE_OPEN_VERIFY_REPORT_DIR", "")
            if report_dir:
                pare_open_verify.write_report(Path(report_dir) / f"{dataset}.verify.json", dataset, verified_specs, decisions)
            return verified_specs
        except Exception as exc:
            if os.environ.get("PARE_OPEN_VERIFY_STRICT", "0") == "1":
                raise
            print(f"Open verifier disabled for {dataset}: {exc}")
    return specs


def default_tag_for_llm_kind(kind: str, index: int) -> str:
    tags = {
        "BRACKET_APACHE_DATETIME": "T",
        "SYSLOG_DATETIME": "T",
        "DATETIME_MMDD_HMS": "T",
        "IPV4": "A",
        "HOSTPORT": "H",
        "BYTE_COUNT": "B",
        "LT_SECONDS": "L",
        "LIFETIME_DURATION": "R",
        "SIZE_PAREN": "S",
        "PROCESS_PID": "P",
        "DOMAIN": "D",
    }
    return tags.get(kind, f"X{index}")


def llm_kind_to_extract_spec(kind: str, tag: str) -> ExtractSpec | None:
    """Map a closed DSL recognizer enum to an ExtractSpec."""
    if kind == "BRACKET_APACHE_DATETIME":
        return ExtractSpec(
            tag,
            rf"\[{WEEKDAY_RE}\s+{MONTH_RE}\s+\d{{2}}\s+\d{{2}}:\d{{2}}:\d{{2}}\s+\d{{4}}\]",
            "auto",
        )
    if kind == "SYSLOG_DATETIME":
        return ExtractSpec(
            tag,
            rf"(?m)^({MONTH_RE}\s+\d{{1,2}}\s+\d{{2}}:\d{{2}}:\d{{2}})",
            "auto",
            store_group=1,
            replacement="{placeholder}",
        )
    if kind == "DATETIME_MMDD_HMS":
        return ExtractSpec(tag, r"\d{2}\.\d{2} \d{2}:\d{2}:\d{2}", "auto")
    if kind == "IPV4":
        return ExtractSpec(tag, r"(?<![\d.])(?:\d+\.){3}\d+(?![\d.])", "auto")
    if kind == "HOSTPORT":
        return ExtractSpec(
            tag,
            (
                r"(?<![\w.-])"
                r"(?:(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+|(?:\d{1,3}\.){3}\d{1,3})"
                r":\d{1,5}(?![\w.-])"
            ),
            "auto",
        )
    if kind == "BYTE_COUNT":
        return ExtractSpec(tag, r"\b(\d+) bytes\b", "auto", store_group=1, replacement="{placeholder} bytes")
    if kind == "LT_SECONDS":
        return ExtractSpec(tag, r"<(\d+)\ssec", "auto", store_group=1, replacement="<{placeholder} sec")
    if kind == "LIFETIME_DURATION":
        return ExtractSpec(
            tag,
            r"\blifetime (\d{2}:\d{2}(?::\d{2})?)\b",
            "auto",
            store_group=1,
            replacement="lifetime {placeholder}",
        )
    if kind == "SIZE_PAREN":
        return ExtractSpec(tag, r"\(\d+(?:\.\d+)?\s[KMGT]?B\)", "auto")
    if kind == "PROCESS_PID":
        return ExtractSpec(tag, r"(?<=[A-Za-z_][A-Za-z0-9_.-])\[(\d+)\]", "auto", store_group=1, replacement="[{placeholder}]")
    if kind == "DOMAIN":
        return ExtractSpec(tag, r"(?<![\w-])(?:[A-Za-z0-9-]+\.){2,}[A-Za-z0-9-]+(?![\w-])", "auto")
    return None


def specs_from_metadata(metadata: dict[str, object]) -> list[ExtractSpec]:
    specs: list[ExtractSpec] = []
    for spec_meta in metadata["specs"]:
        replacement = str(spec_meta.get("replacement", ""))
        specs.append(ExtractSpec(
            tag=str(spec_meta["tag"]),
            pattern=str(spec_meta["pattern"]),
            kind=str(spec_meta["kind"]),
            store_group=int(spec_meta.get("store_group", 0)),
            replacement=replacement if replacement else None,
            context_tag=str(spec_meta["context_tag"]) if spec_meta.get("context_tag") else None,
        ))
    return specs


def parse_hms(token: str) -> int:
    hour, minute, second = [int(part) for part in token.split(":")]
    return hour * 3600 + minute * 60 + second


def format_hms(seconds: int) -> str:
    seconds %= 24 * 3600
    hour = seconds // 3600
    minute = (seconds % 3600) // 60
    second = seconds % 60
    return f"{hour:02d}:{minute:02d}:{second:02d}"


def parse_day_hms(token: str) -> tuple[int, int, int]:
    day_text, hms_text = token.split(" ", 1)
    return int(day_text), len(day_text), parse_hms(hms_text)


def format_day_hms(value: int, width: int) -> str:
    day = value // (24 * 3600)
    seconds = value % (24 * 3600)
    return f"{day:0{width}d} {format_hms(seconds)}"


def _packed_datetime_layout(date_width: int, hour_width: int, minute_width: int, second_width: int, fraction_width: int) -> int:
    if date_width < 6 or date_width > 8:
        raise ValueError(f"Bad packed datetime date width {date_width}")
    if hour_width < 1 or hour_width > 2:
        raise ValueError(f"Bad packed datetime hour width {hour_width}")
    if minute_width < 1 or minute_width > 2:
        raise ValueError(f"Bad packed datetime minute width {minute_width}")
    if second_width < 1 or second_width > 2:
        raise ValueError(f"Bad packed datetime second width {second_width}")
    if fraction_width < 1 or fraction_width > 8:
        raise ValueError(f"Bad packed datetime fraction width {fraction_width}")
    return (
        (date_width - 6)
        | ((hour_width - 1) << 2)
        | ((minute_width - 1) << 3)
        | ((second_width - 1) << 4)
        | ((fraction_width - 1) << 5)
    )


def _unpack_packed_datetime_layout(layout: int) -> tuple[int, int, int, int, int]:
    return (
        (layout & 0x03) + 6,
        ((layout >> 2) & 0x01) + 1,
        ((layout >> 3) & 0x01) + 1,
        ((layout >> 4) & 0x01) + 1,
        ((layout >> 5) & 0x07) + 1,
    )


def parse_packed_datetime(token: str, fraction_scale: int) -> tuple[int, int]:
    match = PACKED_DATETIME_RE.fullmatch(token)
    if match is None:
        raise ValueError(f"Bad packed datetime {token!r}")
    date_text, hour_text, minute_text, second_text, fraction_text = match.groups()
    if fraction_scale < len(fraction_text):
        raise ValueError(f"Bad packed datetime fraction scale {fraction_scale} for {token!r}")
    date_value = int(date_text)
    hour = int(hour_text)
    minute = int(minute_text)
    second = int(second_text)
    fraction = int(fraction_text)
    if hour > 99 or minute > 99 or second > 99:
        raise ValueError(f"Bad packed datetime component {token!r}")
    scale = 10 ** fraction_scale
    value = (((date_value * 24 + hour) * 60 + minute) * 60 + second) * scale + fraction
    layout = _packed_datetime_layout(
        len(date_text),
        len(hour_text),
        len(minute_text),
        len(second_text),
        len(fraction_text),
    )
    return value, layout


def format_packed_datetime(value: int, layout: int, fraction_scale: int) -> str:
    scale = 10 ** fraction_scale
    base, fraction = divmod(value, scale)
    base, second = divmod(base, 60)
    base, minute = divmod(base, 60)
    date_value, hour = divmod(base, 24)
    date_width, hour_width, minute_width, second_width, fraction_width = _unpack_packed_datetime_layout(layout)
    return (
        f"{date_value:0{date_width}d}-"
        f"{hour:0{hour_width}d}:"
        f"{minute:0{minute_width}d}:"
        f"{second:0{second_width}d}:"
        f"{fraction:0{fraction_width}d}"
    )


def encode_packed_datetime_delta(values: list[str]) -> tuple[bytes, bytes, int]:
    if not values:
        raise ValueError("Empty packed datetime stream")
    fraction_scale = max(len(PACKED_DATETIME_RE.fullmatch(value).group(5)) for value in values)  # type: ignore[union-attr]
    encoded_values: list[int] = []
    layouts = bytearray()
    for value in values:
        encoded_value, layout = parse_packed_datetime(value, fraction_scale)
        encoded_values.append(encoded_value)
        layouts.append(layout)
    return encode_delta_values(encoded_values), bytes(layouts), fraction_scale


def parse_month_day_hms(token: str) -> int:
    month_day, hms_text = token.split(" ", 1)
    month_text, day_text = month_day.split(".", 1)
    month = int(month_text)
    day = int(day_text)
    day_of_year = sum(MONTH_LENGTHS[:month - 1]) + (day - 1)
    return day_of_year * 24 * 3600 + parse_hms(hms_text)


def format_month_day_hms(value: int) -> str:
    day_of_year = value // (24 * 3600)
    seconds = value % (24 * 3600)
    month = 1
    remaining = day_of_year
    for month_length in MONTH_LENGTHS:
        if remaining < month_length:
            break
        remaining -= month_length
        month += 1
    day = remaining + 1
    return f"{month:02d}.{day:02d} {format_hms(seconds)}"


def parse_syslog_timestamp(token: str) -> tuple[int, int]:
    match = re.fullmatch(r"([A-Z][a-z]{2})(\s+)(\d{1,2})\s+(\d{2}:\d{2}:\d{2})", token)
    if not match:
        raise ValueError(f"Bad syslog timestamp {token!r}")
    month_name, spaces, day_text, hms_text = match.groups()
    month = MONTH_TO_INDEX[month_name]
    day = int(day_text)
    day_of_year = sum(MONTH_LENGTHS[:month - 1]) + (day - 1)
    value = day_of_year * 24 * 3600 + parse_hms(hms_text)
    zero_flag = 1 if day_text.startswith("0") else 0
    layout = (min(len(spaces), 15) & 0x0F) | ((min(len(day_text), 3) & 0x03) << 4) | (zero_flag << 6)
    return value, layout


def format_syslog_timestamp(value: int, layout: int) -> str:
    day_of_year = value // (24 * 3600)
    seconds = value % (24 * 3600)
    month_index = 1
    remaining = day_of_year
    for month_length in MONTH_LENGTHS:
        if remaining < month_length:
            break
        remaining -= month_length
        month_index += 1
    day = remaining + 1
    spaces = " " * (layout & 0x0F)
    day_width = (layout >> 4) & 0x03
    zero_flag = (layout >> 6) & 0x01
    if zero_flag:
        day_text = f"{day:0{max(day_width, 1)}d}"
    else:
        day_text = str(day)
    return f"{MONTH_NAMES[month_index - 1]}{spaces}{day_text} {format_hms(seconds)}"


def parse_apache_full_timestamp(token: str) -> int:
    return int(datetime.strptime(token[1:-1], "%a %b %d %H:%M:%S %Y").timestamp())


def format_apache_full_timestamp(value: int) -> str:
    return "[" + datetime.fromtimestamp(value).strftime("%a %b %d %H:%M:%S %Y") + "]"


def parse_asctime_year_timestamp(token: str) -> int:
    match = re.fullmatch(
        r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}) (\d{2}:\d{2}:\d{2}) (\d{4})",
        token,
    )
    if not match:
        raise ValueError(f"Bad asctime timestamp {token!r}")
    _weekday, month_name, day_text, hms_text, year_text = match.groups()
    dt = datetime(int(year_text), MONTH_TO_INDEX[month_name], int(day_text))
    return dt.toordinal() * 24 * 3600 + parse_hms(hms_text)


def parse_asctime_year_timestamp_with_layout(token: str) -> tuple[int, int]:
    match = re.fullmatch(
        r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun) (Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(\s+)(\d{1,2}) (\d{2}:\d{2}:\d{2}) (\d{4})",
        token,
    )
    if not match:
        raise ValueError(f"Bad asctime timestamp {token!r}")
    weekday_text, month_name, spaces, day_text, hms_text, year_text = match.groups()
    dt = datetime(int(year_text), MONTH_TO_INDEX[month_name], int(day_text))
    encoded = dt.toordinal() * 24 * 3600 + parse_hms(hms_text)
    # Bit 0 preserves Apache-like zero padding for single-digit days.
    # Bits 1..4 preserve the number of spaces between month and day.
    # Bits 5..7 preserve the original weekday token instead of trusting it.
    zero_padded = int(len(day_text) == 2 and day_text.startswith("0"))
    layout = zero_padded | (min(len(spaces), 15) << 1) | (WEEKDAY_TO_INDEX[weekday_text] << 5)
    return encoded, layout


def format_asctime_year_timestamp(value: int) -> str:
    day_ordinal = value // (24 * 3600)
    seconds = value % (24 * 3600)
    dt = datetime.fromordinal(day_ordinal)
    weekday = WEEKDAY_NAMES[dt.weekday()]
    month = MONTH_NAMES[dt.month - 1]
    return f"{weekday} {month} {dt.day:2d} {format_hms(seconds)} {dt.year:04d}"


def format_asctime_year_timestamp_with_layout(value: int, layout: int) -> str:
    day_ordinal = value // (24 * 3600)
    seconds = value % (24 * 3600)
    dt = datetime.fromordinal(day_ordinal)
    weekday = WEEKDAY_NAMES[(int(layout) >> 5) & 0x07]
    month = MONTH_NAMES[dt.month - 1]
    spaces = " " * max(1, (int(layout) >> 1) & 0x0F)
    if (int(layout) & 0x01) and dt.day < 10:
        day_text = f"{dt.day:02d}"
    else:
        day_text = str(dt.day) if dt.day >= 10 else f"{dt.day:>1d}"
    return f"{weekday} {month}{spaces}{day_text} {format_hms(seconds)} {dt.year:04d}"


def parse_int_width(token: str) -> tuple[int, int]:
    if not token.isdigit():
        raise ValueError(f"Bad integer token {token!r}")
    return int(token), len(token)


def numeric_digit_widths(values: list[str]) -> list[int] | None:
    if not values or not all(value.isdigit() for value in values):
        return None
    return [len(value) for value in values]


def exceeds_numeric_delta_width(values: list[str], max_digits: int) -> bool:
    widths = numeric_digit_widths(values)
    return widths is not None and max(widths, default=0) > max_digits


def format_int_width(value: int, width: int) -> str:
    return f"{value:0{width}d}"


AFFIXED_INT_RE = re.compile(r"^(.*?)(\d+)(\D*)$")


def parse_affixed_int_shape(values: list[str]) -> tuple[str, str] | None:
    if not values:
        return None
    first = AFFIXED_INT_RE.fullmatch(values[0])
    if first is None:
        return None
    prefix, _number, suffix = first.groups()
    for value in values:
        match = AFFIXED_INT_RE.fullmatch(value)
        if match is None or match.group(1) != prefix or match.group(3) != suffix:
            return None
    return prefix, suffix


def encode_affixed_int_delta(values: list[str]) -> tuple[bytes, bytes, str, str]:
    shape = parse_affixed_int_shape(values)
    if shape is None:
        raise ValueError("Not an affixed integer stream")
    prefix, suffix = shape
    encoded_values: list[int] = []
    widths = bytearray()
    for value in values:
        match = AFFIXED_INT_RE.fullmatch(value)
        if match is None:
            raise ValueError(f"Bad affixed integer value {value!r}")
        number_text = match.group(2)
        encoded_values.append(int(number_text))
        widths.append(len(number_text))
    return encode_delta_values(encoded_values), bytes(widths), prefix, suffix


def decode_affixed_int_delta(data: bytes, widths: bytes, prefix: str, suffix: str) -> list[str]:
    values = decode_delta_values(data)
    if len(values) != len(widths):
        raise ValueError("Bad affixed integer width stream")
    return [
        prefix + format_int_width(value, widths[index]) + suffix
        for index, value in enumerate(values)
    ]


INT_TUPLE_DELIMITERS = ("##", "|", ",", ":")


def parse_delimited_int_tuple_shape(values: list[str]) -> tuple[str, int] | None:
    if not values:
        return None
    for delimiter in INT_TUPLE_DELIMITERS:
        first_parts = values[0].split(delimiter)
        if len(first_parts) < 2 or not all(part.isdigit() for part in first_parts):
            continue
        arity = len(first_parts)
        ok = True
        for value in values:
            parts = value.split(delimiter)
            if len(parts) != arity or not all(part.isdigit() for part in parts):
                ok = False
                break
        if ok:
            return delimiter, arity
    return None


def encode_delimited_int_tuple_delta(values: list[str]) -> tuple[list[bytes], bytes, str, int]:
    shape = parse_delimited_int_tuple_shape(values)
    if shape is None:
        raise ValueError("Not a delimited integer tuple stream")
    delimiter, arity = shape
    columns: list[list[int]] = [[] for _ in range(arity)]
    widths = bytearray()
    for value in values:
        parts = value.split(delimiter)
        for index, part in enumerate(parts):
            columns[index].append(int(part))
            widths.append(len(part))
    return [encode_delta_values(column) for column in columns], bytes(widths), delimiter, arity


def decode_delimited_int_tuple_delta(column_data: list[bytes], widths: bytes, delimiter: str, arity: int) -> list[str]:
    columns = [decode_delta_values(data) for data in column_data]
    if len(columns) != arity:
        raise ValueError("Bad delimited integer tuple arity")
    count = len(columns[0]) if columns else 0
    if any(len(column) != count for column in columns):
        raise ValueError("Bad delimited integer tuple column length")
    if len(widths) != count * arity:
        raise ValueError("Bad delimited integer tuple width stream")
    out: list[str] = []
    cursor = 0
    for row in range(count):
        parts: list[str] = []
        for column_index in range(arity):
            parts.append(format_int_width(columns[column_index][row], widths[cursor]))
            cursor += 1
        out.append(delimiter.join(parts))
    return out


NUMERIC_FIELD_RE = re.compile(r"\d+(?:\.\d+)?")


def parse_numeric_skeleton_shape(values: list[str]) -> tuple[list[str], int] | None:
    if not values:
        return None
    first_numbers = NUMERIC_FIELD_RE.findall(values[0])
    if len(first_numbers) < 1:
        return None
    literals = NUMERIC_FIELD_RE.split(values[0])
    for value in values:
        numbers = NUMERIC_FIELD_RE.findall(value)
        if len(numbers) != len(first_numbers) or NUMERIC_FIELD_RE.split(value) != literals:
            return None
    return literals, len(first_numbers)


def parse_mixed_skeleton_shape(values: list[str]) -> int | None:
    """Return numeric-column arity when values share a stable digit-field count.

    Unlike numeric_skeleton_tuple_delta, this codec does not require identical
    literal text between numeric fields. Literal columns are encoded separately,
    so it can handle structured paths, bracketed Java symbols, node names, and
    key-value fragments whose non-numeric pieces vary from a small alphabet.
    """
    if not values:
        return None
    first_arity = len(NUMERIC_FIELD_RE.findall(values[0]))
    if first_arity < 1:
        return None
    for value in values:
        if len(NUMERIC_FIELD_RE.findall(value)) != first_arity:
            return None
    return first_arity


def should_probe_mixed_skeleton(values: list[str]) -> bool:
    arity = parse_mixed_skeleton_shape(values)
    if arity is None or arity < 2:
        return False
    sample = values[: min(len(values), 4096)]
    avg_len = sum(len(value) for value in sample) / max(1, len(sample))
    if avg_len < 24:
        return False
    # If all literal columns are identical, numeric_skeleton_tuple_delta is the
    # cheaper model. Probe mixed only when at least one literal position varies.
    literal_columns: list[set[str]] = [set() for _ in range(arity + 1)]
    for value in sample:
        literals = NUMERIC_FIELD_RE.split(value)
        if len(literals) != arity + 1:
            return False
        for index, literal in enumerate(literals):
            literal_columns[index].add(literal)
    return any(len(column) > 1 for column in literal_columns)


def should_probe_shape_mixed_skeleton(values: list[str]) -> bool:
    if not values:
        return False
    sample = values[: min(len(values), 4096)]
    avg_len = sum(len(value) for value in sample) / max(1, len(sample))
    if avg_len < 8:
        return False
    shapes: set[tuple[str, ...]] = set()
    for value in sample:
        if not NUMERIC_FIELD_RE.search(value):
            return False
        shapes.add(tuple(NUMERIC_FIELD_RE.split(value)))
        if len(shapes) > 256:
            return False
    if len(shapes) <= 1:
        return False
    return len(shapes) <= max(8, len(sample) // 8)


def _parse_decimal_numeric_text(text: str) -> tuple[int, int, int]:
    if "." in text:
        integer_text, fraction_text = text.split(".", 1)
        return int((integer_text or "0") + fraction_text), len(integer_text), len(fraction_text)
    return int(text), len(text), 0


def _format_decimal_numeric_text(value: int, integer_width: int, fraction_width: int) -> str:
    if fraction_width <= 0:
        return format_int_width(value, integer_width)
    digits = f"{value:0{integer_width + fraction_width}d}"
    integer_text = digits[:-fraction_width] or "0"
    fraction_text = digits[-fraction_width:]
    return integer_text + "." + fraction_text


def encode_numeric_skeleton_tuple_delta(values: list[str]) -> tuple[list[bytes], bytes, list[str], int]:
    shape = parse_numeric_skeleton_shape(values)
    if shape is None:
        raise ValueError("Not a numeric skeleton tuple stream")
    literals, arity = shape
    columns: list[list[int]] = [[] for _ in range(arity)]
    layouts = bytearray()
    for value in values:
        numbers = NUMERIC_FIELD_RE.findall(value)
        for index, number_text in enumerate(numbers):
            numeric_value, integer_width, fraction_width = _parse_decimal_numeric_text(number_text)
            if integer_width > 255 or fraction_width > 255:
                raise ValueError(f"Numeric skeleton width too large: {number_text!r}")
            columns[index].append(numeric_value)
            layouts.append(integer_width)
            layouts.append(fraction_width)
    return [encode_delta_values(column) for column in columns], bytes(layouts), literals, arity


def decode_numeric_skeleton_tuple_delta(column_data: list[bytes], layouts: bytes, literals: list[str], arity: int) -> list[str]:
    columns = [decode_delta_values(data) for data in column_data]
    if len(columns) != arity or len(literals) != arity + 1:
        raise ValueError("Bad numeric skeleton tuple metadata")
    count = len(columns[0]) if columns else 0
    if any(len(column) != count for column in columns):
        raise ValueError("Bad numeric skeleton tuple column length")
    if len(layouts) != count * arity * 2:
        raise ValueError("Bad numeric skeleton tuple layout stream")
    out: list[str] = []
    cursor = 0
    for row in range(count):
        parts = [literals[0]]
        for column_index in range(arity):
            integer_width = layouts[cursor]
            fraction_width = layouts[cursor + 1]
            cursor += 2
            parts.append(_format_decimal_numeric_text(columns[column_index][row], integer_width, fraction_width))
            parts.append(literals[column_index + 1])
        out.append("".join(parts))
    return out


def encode_mixed_skeleton_delta(values: list[str]) -> tuple[list[bytes], bytes, list[bytes], list[list[str]], int]:
    arity = parse_mixed_skeleton_shape(values)
    if arity is None:
        raise ValueError("Not a mixed skeleton stream")

    numeric_columns: list[list[int]] = [[] for _ in range(arity)]
    literal_columns: list[list[str]] = [[] for _ in range(arity + 1)]
    layouts = bytearray()
    for value in values:
        numbers = NUMERIC_FIELD_RE.findall(value)
        literals = NUMERIC_FIELD_RE.split(value)
        if len(numbers) != arity or len(literals) != arity + 1:
            raise ValueError("Mixed skeleton arity changed")
        for index, literal in enumerate(literals):
            literal_columns[index].append(literal)
        for index, number_text in enumerate(numbers):
            numeric_value, integer_width, fraction_width = _parse_decimal_numeric_text(number_text)
            if integer_width > 255 or fraction_width > 255:
                raise ValueError(f"Mixed skeleton width too large: {number_text!r}")
            numeric_columns[index].append(numeric_value)
            layouts.append(integer_width)
            layouts.append(fraction_width)

    numeric_data = [encode_delta_values(column) for column in numeric_columns]
    literal_rank_data: list[bytes] = []
    literal_tables: list[list[str]] = []
    for literals in literal_columns:
        ranks, literal_table = encode_string_mtf_cdelta(literals)
        literal_rank_data.append(encode_delta_values(ranks))
        literal_tables.append(literal_table)
    return numeric_data, bytes(layouts), literal_rank_data, literal_tables, arity


def decode_mixed_skeleton_delta(
    numeric_data: list[bytes],
    layouts: bytes,
    literal_rank_data: list[bytes],
    literal_tables: list[list[str]],
    arity: int,
) -> list[str]:
    numeric_columns = [decode_delta_values(data) for data in numeric_data]
    literal_columns = [
        decode_string_mtf_cdelta(decode_delta_values(rank_data), literal_table)
        for rank_data, literal_table in zip(literal_rank_data, literal_tables)
    ]
    if len(numeric_columns) != arity or len(literal_columns) != arity + 1:
        raise ValueError("Bad mixed skeleton metadata")
    count = len(numeric_columns[0]) if numeric_columns else len(literal_columns[0])
    if any(len(column) != count for column in numeric_columns):
        raise ValueError("Bad mixed skeleton numeric column length")
    if any(len(column) != count for column in literal_columns):
        raise ValueError("Bad mixed skeleton literal column length")
    if len(layouts) != count * arity * 2:
        raise ValueError("Bad mixed skeleton layout stream")

    out: list[str] = []
    cursor = 0
    for row in range(count):
        parts = [literal_columns[0][row]]
        for column_index in range(arity):
            integer_width = layouts[cursor]
            fraction_width = layouts[cursor + 1]
            cursor += 2
            parts.append(_format_decimal_numeric_text(numeric_columns[column_index][row], integer_width, fraction_width))
            parts.append(literal_columns[column_index + 1][row])
        out.append("".join(parts))
    return out


def encode_shape_mixed_metadata(shapes: list[tuple[str, ...]]) -> bytes:
    data = bytearray()
    data.extend(base.encode_varint(len(shapes)))
    for literals in shapes:
        data.extend(base.encode_varint(len(literals) - 1))
        for literal in literals:
            encoded = literal.encode("latin-1")
            data.extend(base.encode_varint(len(encoded)))
            data.extend(encoded)
    return bytes(data)


def decode_shape_mixed_metadata(data: bytes) -> list[tuple[str, ...]]:
    cursor = 0
    shape_count, cursor = base.decode_varint(data, cursor)
    shapes: list[tuple[str, ...]] = []
    for _ in range(shape_count):
        arity, cursor = base.decode_varint(data, cursor)
        literals: list[str] = []
        for _literal_index in range(arity + 1):
            length, cursor = base.decode_varint(data, cursor)
            literal = data[cursor:cursor + length].decode("latin-1")
            cursor += length
            literals.append(literal)
        shapes.append(tuple(literals))
    if cursor != len(data):
        raise ValueError("Trailing bytes in shape mixed metadata")
    return shapes


def encode_shape_mixed_skeleton_delta(values: list[str]) -> tuple[bytes, bytes, list[list[bytes]], list[bytes], int]:
    if not should_probe_shape_mixed_skeleton(values):
        raise ValueError("Not a shape mixed skeleton stream")
    shape_to_id: dict[tuple[str, ...], int] = {}
    shapes: list[tuple[str, ...]] = []
    shape_ids: list[int] = []
    numeric_columns: list[list[list[int]]] = []
    layouts: list[bytearray] = []

    for value in values:
        numbers = NUMERIC_FIELD_RE.findall(value)
        literals = tuple(NUMERIC_FIELD_RE.split(value))
        if not numbers or len(literals) != len(numbers) + 1:
            raise ValueError("Bad shape mixed skeleton value")
        shape_id = shape_to_id.get(literals)
        if shape_id is None:
            shape_id = len(shapes)
            shape_to_id[literals] = shape_id
            shapes.append(literals)
            numeric_columns.append([[] for _ in numbers])
            layouts.append(bytearray())
        elif len(numeric_columns[shape_id]) != len(numbers):
            raise ValueError("Shape mixed arity changed")
        shape_ids.append(shape_id)
        for index, number_text in enumerate(numbers):
            numeric_value, integer_width, fraction_width = _parse_decimal_numeric_text(number_text)
            if integer_width > 255 or fraction_width > 255:
                raise ValueError(f"Shape mixed width too large: {number_text!r}")
            numeric_columns[shape_id][index].append(numeric_value)
            layouts[shape_id].append(integer_width)
            layouts[shape_id].append(fraction_width)

    numeric_data = [[encode_delta_values(column) for column in columns] for columns in numeric_columns]
    return encode_varint_stream_bytes(shape_ids), encode_shape_mixed_metadata(shapes), numeric_data, [bytes(item) for item in layouts], len(shapes)


def decode_shape_mixed_skeleton_delta(
    shape_id_data: bytes,
    shape_meta_data: bytes,
    numeric_data: list[list[bytes]],
    layout_data: list[bytes],
    count: int,
) -> list[str]:
    shape_ids = decode_denum_bucket_ids(shape_id_data)
    if len(shape_ids) != count:
        raise ValueError(f"Shape id stream expected {count}, decoded {len(shape_ids)}")
    shapes = decode_shape_mixed_metadata(shape_meta_data)
    if len(numeric_data) != len(shapes) or len(layout_data) != len(shapes):
        raise ValueError("Bad shape mixed stream metadata")
    numeric_columns = [[decode_delta_values(data) for data in columns] for columns in numeric_data]
    layout_cursors = [0] * len(shapes)
    row_indexes = [0] * len(shapes)
    out: list[str] = []
    for shape_id in shape_ids:
        if shape_id < 0 or shape_id >= len(shapes):
            raise ValueError(f"Bad shape id {shape_id}")
        literals = shapes[shape_id]
        arity = len(literals) - 1
        columns = numeric_columns[shape_id]
        if len(columns) != arity:
            raise ValueError("Bad shape mixed arity")
        row_index = row_indexes[shape_id]
        cursor = layout_cursors[shape_id]
        parts = [literals[0]]
        for column_index in range(arity):
            if row_index >= len(columns[column_index]):
                raise ValueError("Shape mixed numeric column exhausted")
            if cursor + 1 >= len(layout_data[shape_id]):
                raise ValueError("Shape mixed layout exhausted")
            integer_width = layout_data[shape_id][cursor]
            fraction_width = layout_data[shape_id][cursor + 1]
            cursor += 2
            parts.append(_format_decimal_numeric_text(columns[column_index][row_index], integer_width, fraction_width))
            parts.append(literals[column_index + 1])
        layout_cursors[shape_id] = cursor
        row_indexes[shape_id] = row_index + 1
        out.append("".join(parts))
    for shape_id, columns in enumerate(numeric_columns):
        if any(row_indexes[shape_id] != len(column) for column in columns):
            raise ValueError("Unused shape mixed numeric values")
        if layout_cursors[shape_id] != len(layout_data[shape_id]):
            raise ValueError("Unused shape mixed layout bytes")
    return out


def denum_bucket_key(token: str) -> tuple[int, int]:
    """Denum-style integer feature: width plus first digit for long numbers."""
    if not token.isdigit():
        raise ValueError(f"Bad integer token {token!r}")
    width = len(token)
    if width <= 0 or width >= 15:
        raise ValueError(f"Unsupported integer width {width}")
    first_digit = int(token[0]) if width >= 4 else 10
    return width, first_digit


def encode_denum_bucket_meta(keys: list[tuple[int, int]], counts: list[int]) -> bytes:
    data = bytearray()
    data.extend(base.encode_varint(len(keys)))
    for (width, first_digit), count in zip(keys, counts):
        data.extend(base.encode_varint(width))
        data.extend(base.encode_varint(first_digit))
        data.extend(base.encode_varint(count))
    return bytes(data)


def decode_denum_bucket_meta(data: bytes) -> tuple[list[tuple[int, int]], list[int]]:
    cursor = 0
    bucket_count, cursor = base.decode_varint(data, cursor)
    keys: list[tuple[int, int]] = []
    counts: list[int] = []
    for _ in range(bucket_count):
        width, cursor = base.decode_varint(data, cursor)
        first_digit, cursor = base.decode_varint(data, cursor)
        count, cursor = base.decode_varint(data, cursor)
        keys.append((width, first_digit))
        counts.append(count)
    if cursor != len(data):
        raise ValueError("Trailing bytes in denum bucket metadata")
    return keys, counts


def encode_denum_bucket_delta(values: list[str]) -> tuple[bytes, bytes, bytes]:
    key_to_id: dict[tuple[int, int], int] = {}
    keys: list[tuple[int, int]] = []
    bucket_values: list[list[int]] = []
    bucket_ids: list[int] = []

    for token in values:
        key = denum_bucket_key(token)
        bucket_id = key_to_id.get(key)
        if bucket_id is None:
            bucket_id = len(keys)
            key_to_id[key] = bucket_id
            keys.append(key)
            bucket_values.append([])
        bucket_ids.append(bucket_id)
        bucket_values[bucket_id].append(int(token))

    delta_data = bytearray()
    counts: list[int] = []
    for nums in bucket_values:
        counts.append(len(nums))
        last_value: int | None = None
        for value in nums:
            delta = value if last_value is None else value - last_value
            delta_data.extend(base.encode_varint(zigzag_encode(delta)))
            last_value = value

    return (
        encode_varint_stream_bytes(bucket_ids),
        encode_denum_bucket_meta(keys, counts),
        bytes(delta_data),
    )


def decode_denum_bucket_ids(data: bytes) -> list[int]:
    ids: list[int] = []
    cursor = 0
    while cursor < len(data):
        bucket_id, cursor = base.decode_varint(data, cursor)
        ids.append(bucket_id)
    return ids


def decode_denum_bucket_delta(ids_data: bytes, meta_data: bytes, delta_data: bytes, count: int) -> list[str]:
    bucket_ids = decode_denum_bucket_ids(ids_data)
    if len(bucket_ids) != count:
        raise ValueError(f"Denum bucket id stream expected {count}, decoded {len(bucket_ids)}")

    keys, counts = decode_denum_bucket_meta(meta_data)
    bucket_values: list[list[str]] = []
    cursor = 0
    for (width, _first_digit), bucket_count in zip(keys, counts):
        nums: list[str] = []
        last_value: int | None = None
        for _ in range(bucket_count):
            if cursor >= len(delta_data):
                raise ValueError("Truncated denum delta stream")
            encoded, cursor = base.decode_varint(delta_data, cursor)
            delta = zigzag_decode(encoded)
            value = delta if last_value is None else last_value + delta
            nums.append(format_int_width(value, width))
            last_value = value
        bucket_values.append(nums)
    if cursor != len(delta_data):
        raise ValueError("Trailing bytes in denum delta stream")

    indexes = [0] * len(bucket_values)
    out: list[str] = []
    for bucket_id in bucket_ids:
        if bucket_id < 0 or bucket_id >= len(bucket_values):
            raise ValueError(f"Bad denum bucket id {bucket_id}")
        index = indexes[bucket_id]
        if index >= len(bucket_values[bucket_id]):
            raise ValueError(f"Denum bucket {bucket_id} exhausted")
        out.append(bucket_values[bucket_id][index])
        indexes[bucket_id] = index + 1
    for bucket_id, index in enumerate(indexes):
        if index != len(bucket_values[bucket_id]):
            raise ValueError(f"Unused denum bucket values for bucket {bucket_id}")
    return out


def encode_modular_delta_width(values: list[str]) -> tuple[int, bytes, bytes, bytes, str, list[str]]:
    parsed = [parse_int_width(value) for value in values]
    nums = [value for value, _width in parsed]
    widths = bytes(width for _value, width in parsed)
    best: tuple[int, int, bytes, str, list[str]] | None = None
    for period in [10, 30, 60, 100, 300, 600, 1000, 3600, 10000, 65536]:
        quotients = [value // period for value in nums]
        remainders = [value % period for value in nums]
        quotient_data = encode_delta_values(quotients)
        remainder_options: list[tuple[str, bytes, list[str], int]] = []
        remainder_varint = encode_varint_stream_bytes(remainders)
        remainder_options.append(("varint", remainder_varint, [], compressed_parts_cost([remainder_varint])))
        if period <= 256:
            remainder_raw = bytes(remainders)
            remainder_options.append(("byte", remainder_raw, [], compressed_parts_cost([remainder_raw])))
        ranks, literals = encode_string_mtf_rank([str(value) for value in remainders])
        rank_data = encode_varint_stream_bytes(ranks)
        literal_data = encode_string_stream_bytes(literals)
        remainder_options.append(("mtf_rank", rank_data, literals, compressed_parts_cost([rank_data, literal_data])))
        for mode, remainder_data, literals_for_mode, remainder_cost in remainder_options:
            total_cost = compressed_parts_cost([quotient_data, widths]) + remainder_cost
            if best is None or total_cost < best[0]:
                best = (total_cost, period, remainder_data, mode, literals_for_mode)
    if best is None:
        raise ValueError("No modular codec candidate")
    _cost, period, remainder_data, mode, literals = best
    quotient_data = encode_delta_values([value // period for value in nums])
    return period, quotient_data, remainder_data, widths, mode, literals


def decode_modular_delta_width(
    quotient_data: bytes,
    remainder_data: bytes,
    widths: bytes,
    period: int,
    remainder_mode: str,
    remainder_literals: list[str],
) -> list[str]:
    quotients = decode_delta_values(quotient_data)
    if remainder_mode == "byte":
        remainders = list(remainder_data)
    elif remainder_mode == "varint":
        remainders = decode_denum_bucket_ids(remainder_data)
    elif remainder_mode == "mtf_rank":
        ranks = decode_denum_bucket_ids(remainder_data)
        remainders = [int(value) for value in decode_string_mtf_rank(ranks, remainder_literals)]
    else:
        raise ValueError(f"Bad modular remainder mode {remainder_mode!r}")
    if not (len(quotients) == len(remainders) == len(widths)):
        raise ValueError("Bad modular stream length")
    return [
        format_int_width(quotients[index] * period + remainders[index], widths[index])
        for index in range(len(quotients))
    ]


def iter_placeholders(text: str, placeholders: list[str]):
    if not placeholders:
        return
    pattern = re.compile("|".join(re.escape(placeholder) for placeholder in sorted(placeholders, key=len, reverse=True)))
    yield from pattern.finditer(text)


def derive_context_counts(
    transformed_text: str,
    context_values: list[str],
    context_placeholder: str,
    target_placeholder: str,
) -> list[tuple[str, int]]:
    context_iter = iter(context_values)
    last_context: str | None = None
    counts: dict[str, int] = {}
    consumed_contexts = 0
    target_count = 0

    for match in iter_placeholders(transformed_text, [context_placeholder, target_placeholder]):
        placeholder = match.group(0)
        if placeholder == context_placeholder:
            last_context = next(context_iter)
            consumed_contexts += 1
            continue
        context_key = last_context if last_context is not None else ""
        counts[context_key] = counts.get(context_key, 0) + 1
        target_count += 1

    if consumed_contexts != len(context_values):
        raise ValueError(f"Context placeholder count mismatch: consumed {consumed_contexts}, expected {len(context_values)}")
    if target_count == 0:
        return []
    return list(counts.items())


def parse_colon_duration(token: str) -> tuple[int, int]:
    parts = [int(part) for part in token.split(":")]
    if len(parts) == 2:
        return parts[0] * 60 + parts[1], 2
    if len(parts) == 3:
        return parts[0] * 3600 + parts[1] * 60 + parts[2], 3
    raise ValueError(f"Bad colon duration token {token!r}")


def format_colon_duration(value: int, parts: int) -> str:
    if parts == 2:
        minute = value // 60
        second = value % 60
        return f"{minute:02d}:{second:02d}"
    if parts == 3:
        hour = value // 3600
        minute = (value % 3600) // 60
        second = value % 60
        return f"{hour:02d}:{minute:02d}:{second:02d}"
    raise ValueError(f"Bad colon duration part count {parts}")


def encode_port_ip_delta_width(
    transformed_text: str,
    port_values: list[str],
    ip_values: list[str],
    port_placeholder: str,
    ip_placeholder: str,
) -> tuple[bytes, bytes]:
    port_iter = iter(port_values)
    ip_iter = iter(ip_values)
    last_ip: str | None = None
    last_port_by_ip: dict[str, int] = {}
    deltas = bytearray()
    widths = bytearray()
    port_count = 0

    for match in iter_placeholders(transformed_text, [ip_placeholder, port_placeholder]):
        placeholder = match.group(0)
        if placeholder == ip_placeholder:
            last_ip = next(ip_iter)
            continue
        port_text = next(port_iter)
        port, width = parse_int_width(port_text)
        context_key = last_ip if last_ip is not None else ""
        previous = last_port_by_ip.get(context_key)
        delta = port if previous is None else port - previous
        deltas.extend(base.encode_varint(zigzag_encode(delta)))
        widths.append(width)
        last_port_by_ip[context_key] = port
        port_count += 1

    if port_count != len(port_values):
        raise ValueError(f"Encoded {port_count} context ports, expected {len(port_values)}")
    return bytes(deltas), bytes(widths)


def circular_uint16_delta(value: int, previous: int) -> int:
    raw_delta = (value - previous) & 0xFFFF
    if raw_delta >= 0x8000:
        return raw_delta - 0x10000
    return raw_delta


def encode_port_ip_grouped_circular_delta_width(
    transformed_text: str,
    port_values: list[str],
    ip_values: list[str],
    port_placeholder: str,
    ip_placeholder: str,
) -> tuple[bytes, bytes]:
    port_iter = iter(port_values)
    ip_iter = iter(ip_values)
    last_ip: str | None = None
    ports_by_ip: dict[str, list[tuple[int, int]]] = {}
    port_count = 0

    for match in iter_placeholders(transformed_text, [ip_placeholder, port_placeholder]):
        placeholder = match.group(0)
        if placeholder == ip_placeholder:
            last_ip = next(ip_iter)
            continue
        port_text = next(port_iter)
        port, width = parse_int_width(port_text)
        if port < 0 or port > 0xFFFF:
            raise ValueError(f"Port out of uint16 range: {port_text!r}")
        context_key = last_ip if last_ip is not None else ""
        ports_by_ip.setdefault(context_key, []).append((port, width))
        port_count += 1

    if port_count != len(port_values):
        raise ValueError(f"Encoded {port_count} grouped context ports, expected {len(port_values)}")

    deltas = bytearray()
    widths = bytearray()
    for sequence in ports_by_ip.values():
        previous: int | None = None
        for port, width in sequence:
            delta = port if previous is None else circular_uint16_delta(port, previous)
            deltas.extend(base.encode_varint(zigzag_encode(delta)))
            widths.append(width)
            previous = port
    return bytes(deltas), bytes(widths)


class PortIpDeltaWidthDecoder:
    def __init__(self, data: bytes, widths: bytes) -> None:
        self.data = data
        self.widths = widths
        self.cursor = 0
        self.width_cursor = 0
        self.last_port_by_ip: dict[str, int] = {}

    def next_value(self, last_ip: str | None) -> str:
        encoded, self.cursor = base.decode_varint(self.data, self.cursor)
        delta = zigzag_decode(encoded)
        context_key = last_ip if last_ip is not None else ""
        previous = self.last_port_by_ip.get(context_key)
        port = delta if previous is None else previous + delta
        if self.width_cursor >= len(self.widths):
            raise ValueError("Port width stream exhausted")
        width = self.widths[self.width_cursor]
        self.width_cursor += 1
        self.last_port_by_ip[context_key] = port
        return format_int_width(port, width)

    def assert_finished(self) -> None:
        if self.cursor != len(self.data):
            raise ValueError("Unused port-ip delta bytes")
        if self.width_cursor != len(self.widths):
            raise ValueError("Unused port-ip width bytes")


class GroupedPortIpCircularDeltaWidthDecoder:
    def __init__(self, data: bytes, widths: bytes, context_counts: list[tuple[str, int]]) -> None:
        self.values_by_context: dict[str, list[str]] = {}
        self.index_by_context: dict[str, int] = {}
        cursor = 0
        width_cursor = 0

        for context_key, count in context_counts:
            values: list[str] = []
            previous: int | None = None
            for _ in range(count):
                encoded_delta, cursor = base.decode_varint(data, cursor)
                delta = zigzag_decode(encoded_delta)
                if previous is None:
                    port = delta
                else:
                    port = (previous + delta) & 0xFFFF
                if port < 0 or port > 0xFFFF:
                    raise ValueError(f"Decoded port out of uint16 range: {port}")
                if width_cursor >= len(widths):
                    raise ValueError("Grouped port width stream exhausted")
                width = widths[width_cursor]
                width_cursor += 1
                values.append(format_int_width(port, width))
                previous = port
            self.values_by_context[context_key] = values
            self.index_by_context[context_key] = 0

        if cursor != len(data):
            raise ValueError("Unused grouped port delta bytes")
        if width_cursor != len(widths):
            raise ValueError("Unused grouped port width bytes")

    def next_value(self, last_ip: str | None) -> str:
        context_key = last_ip if last_ip is not None else ""
        values = self.values_by_context.get(context_key)
        if values is None:
            raise ValueError(f"No grouped port sequence for context {context_key!r}")
        index = self.index_by_context[context_key]
        if index >= len(values):
            raise ValueError(f"Grouped port sequence exhausted for context {context_key!r}")
        self.index_by_context[context_key] = index + 1
        return values[index]

    def assert_finished(self) -> None:
        for context_key, values in self.values_by_context.items():
            if self.index_by_context[context_key] != len(values):
                raise ValueError(f"Unused grouped port values for context {context_key!r}")


def encode_delta_values(values: list[int]) -> bytes:
    data = bytearray()
    last_value: int | None = None
    for value in values:
        delta = value if last_value is None else value - last_value
        data.extend(base.encode_varint(zigzag_encode(delta)))
        last_value = value
    return bytes(data)


VARINT63_MAX = (1 << 63) - 1


def can_decode_varint_stream(values: list[int]) -> bool:
    return all(0 <= value <= VARINT63_MAX for value in values)


def can_decode_delta_stream(values: list[int]) -> bool:
    last_value: int | None = None
    for value in values:
        delta = value if last_value is None else value - last_value
        encoded_delta = zigzag_encode(delta)
        if encoded_delta < 0 or encoded_delta > VARINT63_MAX:
            return False
        last_value = value
    return True


def has_oversized_open_numeric_values(raw_values: list[str], encoded_values: list[int]) -> bool:
    """Keep huge/generated numerics out of varint numeric codecs.

    The reversible open-function path can parse arbitrary precision Python ints,
    but the archive decoder intentionally uses a portable 63-bit varint envelope.
    A raw digit run longer than 18 digits, or an encoded absolute value outside
    that envelope, is safer as a dictionary string stream.
    """
    for value in raw_values:
        if any(len(match.group(0).lstrip("+-")) > 18 for match in re.finditer(r"[+-]?\d+", value)):
            return True
    for value in encoded_values:
        if zigzag_encode(int(value)) < 0 or zigzag_encode(int(value)) > VARINT63_MAX:
            return True
    return False


TIMESTAMP_OPEN_FUNCTION_OPS = {
    "datetime_strptime_delta",
    "syslog_delta",
    "asctime_year_delta",
    "month_day_hms_delta",
    "hms_delta",
    "day_hms_delta",
}


def is_timestamp_open_function(tag: str, program: dict[str, object]) -> bool:
    """Timestamp streams are always encoded as deltas.

    Timestamp values are usually high-cardinality, but their adjacent deltas are
    small.  Cardinality-based numeric routing should therefore never convert a
    timestamp stream to absolute zigzag or dictionary coding.
    """
    op = str(program.get("op", ""))
    if op in TIMESTAMP_OPEN_FUNCTION_OPS:
        return True
    upper_tag = tag.upper()
    return upper_tag == "TS" or upper_tag.startswith("TS_") or upper_tag.startswith("TIMESTAMP")


def decode_delta_values(data: bytes) -> list[int]:
    values: list[int] = []
    cursor = 0
    last_value: int | None = None
    while cursor < len(data):
        encoded, cursor = base.decode_varint(data, cursor)
        delta = zigzag_decode(encoded)
        value = delta if last_value is None else last_value + delta
        values.append(value)
        last_value = value
    return values


def encode_string_mtf_rank(values: list[str], table_size: int = 512) -> tuple[list[int], list[str]]:
    table: list[str] = []
    ranks: list[int] = []
    literals: list[str] = []
    for value in values:
        try:
            index = table.index(value)
        except ValueError:
            ranks.append(0)
            literals.append(value)
            table.insert(0, value)
            del table[table_size:]
            continue
        ranks.append(index + 1)
        table.pop(index)
        table.insert(0, value)
    return ranks, literals


def decode_string_mtf_rank(ranks: list[int], literals: list[str], table_size: int = 512) -> list[str]:
    table: list[str] = []
    values: list[str] = []
    literal_index = 0
    for rank in ranks:
        if rank == 0:
            if literal_index >= len(literals):
                raise ValueError("String MTF literal stream exhausted")
            value = literals[literal_index]
            literal_index += 1
            table.insert(0, value)
            del table[table_size:]
        else:
            index = rank - 1
            if index < 0 or index >= len(table):
                raise ValueError(f"Bad string MTF rank {rank}")
            value = table.pop(index)
            table.insert(0, value)
        values.append(value)
    if literal_index != len(literals):
        raise ValueError("Unused string MTF literals")
    return values


def encode_string_mtf_cdelta(values: list[str], table_size: int = 512) -> tuple[list[int], list[str]]:
    ranks, literals = encode_string_mtf_rank(values, table_size=table_size)
    return ranks, literals


def decode_string_mtf_cdelta(ranks: list[int], literals: list[str], table_size: int = 512) -> list[str]:
    return decode_string_mtf_rank(ranks, literals, table_size=table_size)


def encode_varint_stream_bytes(values: list[int]) -> bytes:
    data = bytearray()
    for value in values:
        data.extend(base.encode_varint(value))
    return bytes(data)


def encode_string_stream_bytes(values: list[str]) -> bytes:
    data = bytearray()
    for value in values:
        encoded = value.encode("latin-1")
        data.extend(base.encode_varint(len(encoded)))
        data.extend(encoded)
    return bytes(data)


_CPP_STREAM_WRITER_LOCK = threading.Lock()
_CPP_STREAM_WRITER_SERVER: subprocess.Popen[str] | None = None


def cpp_stream_writer_enabled() -> bool:
    return os.environ.get("SEMZIP_CPP_STREAM_WRITER", "0").strip().lower() in {"1", "true", "yes", "on", "server"}


def cpp_stream_writer_mode() -> str:
    mode = os.environ.get("SEMZIP_CPP_STREAM_WRITER", "0").strip().lower()
    if mode == "server":
        return "server"
    return "cli"


def ensure_cpp_stream_writer() -> Path:
    runtime_dir = Path(__file__).resolve().parent / "cpp_runtime"
    source_name = "stream_writer_server.cpp" if cpp_stream_writer_mode() == "server" else "stream_writer.cpp"
    exe_name = "stream_writer_server" if cpp_stream_writer_mode() == "server" else "stream_writer"
    exe = runtime_dir / exe_name
    if exe.exists():
        return exe
    with _CPP_STREAM_WRITER_LOCK:
        if exe.exists():
            return exe
        compiler = os.environ.get("CXX") or shutil.which("clang++") or shutil.which("g++")
        if not compiler:
            raise RuntimeError("SEMZIP_CPP_STREAM_WRITER=1 but no C++ compiler was found")
        subprocess.run(
            [
                compiler,
                "-std=c++17",
                "-O2",
                "-Wall",
                "-Wextra",
                "semzip_codecs.cpp",
                source_name,
                "-o",
                str(exe),
            ],
            cwd=runtime_dir,
            check=True,
        )
    return exe


def shutdown_cpp_stream_writer_server() -> None:
    global _CPP_STREAM_WRITER_SERVER
    proc = _CPP_STREAM_WRITER_SERVER
    _CPP_STREAM_WRITER_SERVER = None
    if proc is None or proc.poll() is not None:
        return
    try:
        assert proc.stdin is not None
        proc.stdin.write("QUIT\n")
        proc.stdin.flush()
        proc.wait(timeout=2)
    except Exception:
        proc.kill()


atexit.register(shutdown_cpp_stream_writer_server)


def ensure_cpp_stream_writer_server() -> subprocess.Popen[str]:
    global _CPP_STREAM_WRITER_SERVER
    proc = _CPP_STREAM_WRITER_SERVER
    if proc is not None and proc.poll() is None:
        return proc
    exe = ensure_cpp_stream_writer()
    proc = subprocess.Popen(
        [str(exe)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    _CPP_STREAM_WRITER_SERVER = proc
    return proc


def cpp_write_stream(kind: str, values: list[str], output_paths: list[Path]) -> bool:
    if not cpp_stream_writer_enabled():
        return False
    with tempfile.TemporaryDirectory(prefix="semzip_cpp_writer_", dir=str(output_paths[0].parent)) as tmp_name:
        input_path = Path(tmp_name) / "values.strings.bin"
        input_path.write_bytes(encode_string_stream_bytes(values))
        if cpp_stream_writer_mode() == "server":
            proc = ensure_cpp_stream_writer_server()
            assert proc.stdin is not None and proc.stdout is not None
            proc.stdin.write("\t".join(["WRITE", kind, str(input_path), *[str(path) for path in output_paths]]) + "\n")
            proc.stdin.flush()
            response = proc.stdout.readline().rstrip("\n")
            if response != "OK":
                raise RuntimeError(f"C++ stream_writer_server failed: {response}")
            return True
        exe = ensure_cpp_stream_writer()
        subprocess.run(
            [str(exe), kind, str(input_path), *[str(path) for path in output_paths]],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    return True


def compressed_parts_cost(parts: list[bytes]) -> int:
    proxy_codec = os.environ.get("PARE_PROXY_CODEC", "").strip().lower()
    if proxy_codec.startswith("zlib"):
        level_text = proxy_codec[4:].lstrip(":=")
        try:
            level = int(level_text) if level_text else 1
        except ValueError:
            level = 1
        level = max(0, min(9, level))
        return sum(len(zlib.compress(part, level)) for part in parts)
    preset_text = os.environ.get("PARE_PROXY_LZMA_PRESET", "")
    if preset_text:
        try:
            preset = int(preset_text)
        except ValueError:
            preset = None
        if preset is not None:
            return sum(len(lzma.compress(part, preset=preset)) for part in parts)
    return sum(len(lzma.compress(part)) for part in parts)


CONDITIONAL_CODEC_MODEL_COST = 64


def pack_flag_bits(flags: list[int]) -> bytes:
    data = bytearray()
    byte = 0
    bit_index = 0
    for flag in flags:
        if flag:
            byte |= 1 << bit_index
        bit_index += 1
        if bit_index == 8:
            data.append(byte)
            byte = 0
            bit_index = 0
    if bit_index:
        data.append(byte)
    return bytes(data)


def unpack_flag_bits(data: bytes, count: int) -> list[int]:
    flags: list[int] = []
    for byte in data:
        for bit_index in range(8):
            if len(flags) == count:
                return flags
            flags.append((byte >> bit_index) & 1)
    if len(flags) != count:
        raise ValueError("Truncated flag bitstream")
    return flags


def format_pretty_size_from_bytes(byte_count_text: str) -> str:
    byte_count = int(byte_count_text)
    units = [
        ("TB", 1024 ** 4),
        ("GB", 1024 ** 3),
        ("MB", 1024 ** 2),
        ("KB", 1024),
    ]
    for unit, scale in units:
        if byte_count >= scale:
            if byte_count >= 100 * scale:
                decimals = 0
            elif byte_count >= 10 * scale:
                decimals = 1
            else:
                decimals = 2
            factor = 10 ** decimals
            scaled = (byte_count * factor) // scale
            if decimals == 0:
                number = str(scaled)
            else:
                number = f"{scaled // factor}.{scaled % factor:0{decimals}d}"
            return f"({number} {unit})"
        return f"({byte_count} B)"


def encode_size_from_prev_bytes(
    transformed_text: str,
    size_values: list[str],
    byte_values: list[str],
    size_placeholder: str,
    byte_placeholder: str,
) -> tuple[bytes, list[str]]:
    byte_iter = iter(byte_values)
    size_iter = iter(size_values)
    last_byte: str | None = None
    flags: list[int] = []
    exceptions: list[str] = []
    size_count = 0
    consumed_bytes = 0

    for match in iter_placeholders(transformed_text, [byte_placeholder, size_placeholder]):
        placeholder = match.group(0)
        if placeholder == byte_placeholder:
            last_byte = next(byte_iter)
            consumed_bytes += 1
            continue
        size_text = next(size_iter)
        size_count += 1
        if last_byte is None:
            raise ValueError("SIZE placeholder appeared before BYTE_COUNT context")
        predicted = format_pretty_size_from_bytes(last_byte)
        if predicted == size_text:
            flags.append(0)
        else:
            flags.append(1)
            exceptions.append(size_text)

    if size_count != len(size_values):
        raise ValueError(f"Encoded {size_count} size values, expected {len(size_values)}")
    if consumed_bytes > len(byte_values):
        raise ValueError("Consumed too many byte-count values")
    return pack_flag_bits(flags), exceptions


class SizeFromPrevBytesDecoder:
    def __init__(self, flags: bytes, exceptions: list[str], count: int) -> None:
        self.flags = unpack_flag_bits(flags, count)
        self.exceptions = exceptions
        self.index = 0
        self.exception_index = 0

    def next_value(self, last_byte: str | None) -> str:
        if self.index >= len(self.flags):
            raise ValueError("Size-from-byte stream exhausted")
        if last_byte is None:
            raise ValueError("Size-from-byte context is missing")
        flag = self.flags[self.index]
        self.index += 1
        if flag:
            if self.exception_index >= len(self.exceptions):
                raise ValueError("Size-from-byte exception stream exhausted")
            value = self.exceptions[self.exception_index]
            self.exception_index += 1
            return value
        return format_pretty_size_from_bytes(last_byte)

    def assert_finished(self) -> None:
        if self.index != len(self.flags):
            raise ValueError("Unused size-from-byte flags")
        if self.exception_index != len(self.exceptions):
            raise ValueError("Unused size-from-byte exceptions")


BYTE_PHRASE_RE = re.compile(r"^(\d+) bytes(?: \(([^)]+)\))?$")


def _floor_3sig_unit(numerator: int, denominator: int) -> str:
    integer_part = numerator // denominator
    digits = len(str(max(1, integer_part)))
    decimals = max(0, 3 - digits)
    scale = 10 ** decimals
    scaled = numerator * scale // denominator
    whole = scaled // scale
    if decimals == 0:
        return str(whole)
    return f"{whole}.{scaled % scale:0{decimals}d}"


def derived_byte_phrase_suffix(byte_count: int) -> str:
    """Predict human-readable size text from byte count when possible.

    The codec remains lossless: mismatches are stored as exceptions and replayed
    verbatim.  The candidate is selected only if its measured compressed cost is
    below generic string/rank alternatives.
    """
    if byte_count <= 1024:
        return ""
    if byte_count >= 1024 * 1024:
        return _floor_3sig_unit(byte_count, 1024 * 1024) + " MB"
    return _floor_3sig_unit(byte_count, 1024) + " KB"


def parse_byte_phrase(value: str) -> tuple[int, str]:
    match = BYTE_PHRASE_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"Not a byte phrase: {value!r}")
    return int(match.group(1)), match.group(2) or ""


def format_byte_phrase(byte_count: int, suffix: str) -> str:
    if suffix:
        return f"{byte_count} bytes ({suffix})"
    return f"{byte_count} bytes"


def encode_byte_phrase_values(values: list[str]) -> tuple[bytes, bytes, list[str]]:
    numeric_values: list[int] = []
    flags: list[int] = []
    exceptions: list[str] = []
    for value in values:
        byte_count, suffix = parse_byte_phrase(value)
        numeric_values.append(byte_count)
        if suffix == derived_byte_phrase_suffix(byte_count):
            flags.append(0)
        else:
            flags.append(1)
            exceptions.append(suffix)
    return encode_delta_values(numeric_values), pack_flag_bits(flags), exceptions


def decode_byte_phrase_values(data: bytes, flags_data: bytes, exceptions: list[str], count: int) -> list[str]:
    numeric_values = decode_delta_values(data)
    flags = unpack_flag_bits(flags_data, count)
    if len(numeric_values) != count:
        raise ValueError("Bad byte phrase numeric stream length")
    exception_index = 0
    values: list[str] = []
    for byte_count, flag in zip(numeric_values, flags):
        if flag:
            if exception_index >= len(exceptions):
                raise ValueError("Byte phrase exception stream exhausted")
            suffix = exceptions[exception_index]
            exception_index += 1
        else:
            suffix = derived_byte_phrase_suffix(byte_count)
        values.append(format_byte_phrase(byte_count, suffix))
    if exception_index != len(exceptions):
        raise ValueError("Unused byte phrase exceptions")
    return values


HEX_INT_RE = re.compile(r"^[0-9a-fA-F]+$")
HEX_PAIR_RE = re.compile(r"^0x([0-9a-f]+|[0-9A-F]+)/0x([0-9a-f]+|[0-9A-F]+)$")


def parse_hex_int_width(value: str) -> tuple[int, int, int]:
    if HEX_INT_RE.fullmatch(value) is None:
        raise ValueError(f"Bad hex token {value!r}")
    letters = [ch for ch in value if ch.isalpha()]
    case_flag = 0
    if letters:
        if all(ch.islower() for ch in letters):
            case_flag = 0
        elif all(ch.isupper() for ch in letters):
            case_flag = 1
        else:
            raise ValueError(f"Mixed-case hex token is not supported by compact codec: {value!r}")
    return int(value, 16), len(value), case_flag


def format_hex_int_width(value: int, width: int, case_flag: int) -> str:
    rendered = f"{value:0{width}x}"
    return rendered.upper() if case_flag else rendered


def encode_hex_int_width(values: list[str]) -> tuple[bytes, bytes, bytes]:
    numeric_values: list[int] = []
    widths = bytearray()
    case_flags: list[int] = []
    for value in values:
        numeric_value, width, case_flag = parse_hex_int_width(value)
        numeric_values.append(numeric_value)
        if width > 255:
            raise ValueError("Hex width too large")
        widths.append(width)
        case_flags.append(case_flag)
    return encode_delta_values(numeric_values), bytes(widths), pack_flag_bits(case_flags)


def decode_hex_int_width(data: bytes, widths: bytes, case_flags_data: bytes, count: int) -> list[str]:
    numeric_values = decode_delta_values(data)
    if len(numeric_values) != count or len(widths) != count:
        raise ValueError("Bad hex int stream lengths")
    case_flags = unpack_flag_bits(case_flags_data, count)
    return [
        format_hex_int_width(numeric_values[index], widths[index], case_flags[index])
        for index in range(count)
    ]


def parse_hex_pair(value: str) -> tuple[int, int, int, int, int]:
    match = HEX_PAIR_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"Bad hex pair token {value!r}")
    left_text, right_text = match.groups()
    left, left_width, left_case = parse_hex_int_width(left_text)
    right, right_width, right_case = parse_hex_int_width(right_text)
    case_flag = 1 if left_case or right_case else 0
    return left, right, left_width, right_width, case_flag


def format_hex_pair(left: int, right: int, left_width: int, right_width: int, case_flag: int) -> str:
    left_text = format_hex_int_width(left, left_width, case_flag)
    right_text = format_hex_int_width(right, right_width, case_flag)
    prefix = "0X" if case_flag else "0x"
    return f"{prefix}{left_text}/{prefix}{right_text}"


def encode_hex_pair_delta(values: list[str]) -> tuple[bytes, bytes, bytes, bytes]:
    left_values: list[int] = []
    right_values: list[int] = []
    widths = bytearray()
    case_flags: list[int] = []
    for value in values:
        left, right, left_width, right_width, case_flag = parse_hex_pair(value)
        if left_width > 255 or right_width > 255:
            raise ValueError("Hex pair width too large")
        left_values.append(left)
        right_values.append(right)
        widths.extend([left_width, right_width])
        case_flags.append(case_flag)
    return encode_delta_values(left_values), encode_delta_values(right_values), bytes(widths), pack_flag_bits(case_flags)


def decode_hex_pair_delta(left_data: bytes, right_data: bytes, widths: bytes, case_flags_data: bytes, count: int) -> list[str]:
    left_values = decode_delta_values(left_data)
    right_values = decode_delta_values(right_data)
    if len(left_values) != count or len(right_values) != count or len(widths) != count * 2:
        raise ValueError("Bad hex pair stream lengths")
    case_flags = unpack_flag_bits(case_flags_data, count)
    values: list[str] = []
    for index in range(count):
        values.append(format_hex_pair(
            left_values[index],
            right_values[index],
            widths[index * 2],
            widths[index * 2 + 1],
            case_flags[index],
        ))
    return values


COMPOUND_IPV4_PORT_RE = re.compile(r"^(/?)((?:\d{1,3}\.){3}\d{1,3}):(\d+)(;?)$")
COMPOUND_INT_PAIR_RE = re.compile(r"^(\d+):(\d+)$")


def encode_compound_endpoint_mixed_parts(values: list[str]) -> dict[str, bytes]:
    """Encode a mixed endpoint-like stream by structural sub-codecs.

    LLM proposals often identify a useful compound span such as ``/IP:port``
    but do not split the subfields.  This codec keeps the LLM-discovered span
    intact at the program level while letting the deterministic compiler route
    recognizable substructures to better source models.  Values that do not
    match the generic endpoint grammar remain exact raw strings.
    """
    types = bytearray()
    ip_prefix_flags: list[int] = []
    ip_suffix_flags: list[int] = []
    ip_values: list[str] = []
    ip_port_values: list[int] = []
    ip_port_widths = bytearray()
    pair_left_values: list[int] = []
    pair_right_values: list[int] = []
    pair_widths = bytearray()
    raw_values: list[str] = []

    for value in values:
        ipv4_match = COMPOUND_IPV4_PORT_RE.fullmatch(value)
        if ipv4_match is not None:
            prefix, ip_text, port_text, suffix = ipv4_match.groups()
            port_value, port_width = parse_int_width(port_text)
            types.append(0)
            ip_prefix_flags.append(1 if prefix else 0)
            ip_suffix_flags.append(1 if suffix else 0)
            ip_values.append(ip_text)
            ip_port_values.append(port_value)
            ip_port_widths.append(port_width)
            continue
        pair_match = COMPOUND_INT_PAIR_RE.fullmatch(value)
        if pair_match is not None:
            left_text, right_text = pair_match.groups()
            left_value, left_width = parse_int_width(left_text)
            right_value, right_width = parse_int_width(right_text)
            types.append(1)
            pair_left_values.append(left_value)
            pair_right_values.append(right_value)
            pair_widths.append(left_width)
            pair_widths.append(right_width)
            continue
        types.append(2)
        raw_values.append(value)

    # The codec is only worth considering when it has a real endpoint signal;
    # otherwise it degenerates into a tagged string stream with extra overhead.
    if len(ip_values) < max(16, len(values) // 10):
        raise ValueError("not enough IPv4:port values for compound endpoint codec")

    ip_ranks, ip_literals = encode_string_mtf_rank(ip_values)
    last_port_by_ip: dict[str, int] = {}
    ip_port_deltas: list[int] = []
    for ip_text, port_value in zip(ip_values, ip_port_values):
        previous = last_port_by_ip.get(ip_text, 0)
        ip_port_deltas.append(port_value - previous)
        last_port_by_ip[ip_text] = port_value

    return {
        "types": bytes(types),
        "ip_prefix": pack_flag_bits(ip_prefix_flags),
        "ip_suffix": pack_flag_bits(ip_suffix_flags),
        "ip_rank": encode_varint_stream_bytes(ip_ranks),
        "ip_literal": encode_string_stream_bytes(ip_literals),
        "ip_port_delta": encode_delta_values(ip_port_deltas),
        "ip_port_width": bytes(ip_port_widths),
        "pair_left_delta": encode_delta_values(pair_left_values),
        "pair_right_delta": encode_delta_values(pair_right_values),
        "pair_width": bytes(pair_widths),
        "raw": encode_string_stream_bytes(raw_values),
    }


def decode_compound_endpoint_mixed(
    type_data: bytes,
    ip_prefix_data: bytes,
    ip_suffix_data: bytes,
    ip_ranks: list[int],
    ip_literals: list[str],
    ip_port_delta_data: bytes,
    ip_port_width_data: bytes,
    pair_left_delta_data: bytes,
    pair_right_delta_data: bytes,
    pair_width_data: bytes,
    raw_values: list[str],
    count: int,
) -> list[str]:
    if len(type_data) != count:
        raise ValueError("Bad compound endpoint type stream")
    ip_values = decode_string_mtf_rank(ip_ranks, ip_literals)
    ip_prefix_flags = unpack_flag_bits(ip_prefix_data, len(ip_values))
    ip_suffix_flags = unpack_flag_bits(ip_suffix_data, len(ip_values))
    ip_port_deltas = decode_delta_values(ip_port_delta_data)
    ip_port_widths = list(ip_port_width_data)
    if not (len(ip_values) == len(ip_prefix_flags) == len(ip_suffix_flags) == len(ip_port_deltas) == len(ip_port_widths)):
        raise ValueError("Bad compound endpoint IPv4:port substream")

    pair_left_values = decode_delta_values(pair_left_delta_data)
    pair_right_values = decode_delta_values(pair_right_delta_data)
    pair_widths = list(pair_width_data)
    if len(pair_widths) != 2 * len(pair_left_values) or len(pair_left_values) != len(pair_right_values):
        raise ValueError("Bad compound endpoint int-pair substream")

    out: list[str] = []
    ip_index = 0
    pair_index = 0
    raw_index = 0
    last_port_by_ip: dict[str, int] = {}
    for kind in type_data:
        if kind == 0:
            ip_text = ip_values[ip_index]
            previous = last_port_by_ip.get(ip_text, 0)
            port_value = previous + ip_port_deltas[ip_index]
            last_port_by_ip[ip_text] = port_value
            prefix = "/" if ip_prefix_flags[ip_index] else ""
            suffix = ";" if ip_suffix_flags[ip_index] else ""
            out.append(prefix + ip_text + ":" + format_int_width(port_value, ip_port_widths[ip_index]) + suffix)
            ip_index += 1
        elif kind == 1:
            left_width = pair_widths[pair_index * 2]
            right_width = pair_widths[pair_index * 2 + 1]
            out.append(
                format_int_width(pair_left_values[pair_index], left_width)
                + ":"
                + format_int_width(pair_right_values[pair_index], right_width)
            )
            pair_index += 1
        elif kind == 2:
            out.append(raw_values[raw_index])
            raw_index += 1
        else:
            raise ValueError(f"Bad compound endpoint type {kind}")
    if ip_index != len(ip_values) or pair_index != len(pair_left_values) or raw_index != len(raw_values):
        raise ValueError("Unused compound endpoint substream values")
    return out


def _codec_allowed_by_features(values: list[str]) -> set[str] | None:
    if not FEATURE_CODEC_PREFILTER or not values:
        return None
    sample = values[: min(len(values), 4096)]
    distinct_ratio = len(set(sample)) / max(1, len(sample))

    if all(re.fullmatch(r"-?\d+", value) for value in sample):
        widths = {len(value.lstrip("-")) for value in sample}
        allowed = {
            "string",
            "int_abs",
            "int_abs_width",
            "int_delta_width",
            "uint16_split_width",
        }
        if len(widths) == 1:
            allowed.update({"int_abs_fixed_width", "int_delta_fixed_width"})
        if len(values) >= 64:
            allowed.add("int_denum_bucket_delta")
        if distinct_ratio < 0.35:
            allowed.update({"string_mtf_rank", "string_mtf_cdelta"})
        return allowed

    if all(re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", value) for value in sample):
        return {"ipv4_width", "ipv4_plain", "string_mtf_rank", "string_mtf_cdelta"}

    if all(re.fullmatch(r"(?:0x)?[0-9A-Fa-f]+,?", value) for value in sample):
        return {"hex_int_delta_width", "hex_pair_delta", "string_mtf_rank", "string_mtf_cdelta"}

    if all(re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?(?:[:.]\d+)?", value) for value in sample):
        return {"hms_delta", "colon_duration_delta", "string_mtf_rank", "string_mtf_cdelta"}

    if all(re.fullmatch(r"\d{1,2}\s+\d{2}:\d{2}:\d{2}", value) for value in sample):
        return {"day_hms_delta", "string_mtf_rank", "string_mtf_cdelta"}

    if all(re.fullmatch(SYSLOG_TS_RE, value) for value in sample):
        return {"syslog_delta", "month_day_hms_delta", "string_mtf_rank", "string_mtf_cdelta"}

    if all(PACKED_DATETIME_RE.fullmatch(value) for value in sample):
        return {"packed_datetime_delta", "string_mtf_rank", "string_mtf_cdelta", "string"}

    if parse_delimited_int_tuple_shape(sample) is not None:
        return {"delimited_int_tuple_delta", "string_mtf_rank", "string_mtf_cdelta", "string"}

    if parse_affixed_int_shape(sample) is not None:
        return {"affixed_int_delta", "string_mtf_rank", "string_mtf_cdelta", "string"}

    if parse_numeric_skeleton_shape(sample) is not None:
        return {"numeric_skeleton_tuple_delta", "string_mtf_rank", "string_mtf_cdelta", "string"}

    if should_probe_mixed_skeleton(sample):
        return {"mixed_skeleton_delta", "string_mtf_rank", "string_mtf_cdelta", "string"}

    if should_probe_shape_mixed_skeleton(sample):
        return {"shape_mixed_skeleton_delta", "string_mtf_rank", "string_mtf_cdelta", "string"}

    if all("." in value and ":" in value and any(ch.isdigit() for ch in value) for value in sample):
        return {"compound_endpoint_mixed", "string_mtf_rank", "string_mtf_cdelta", "string"}

    allowed = {"string", "string_mtf_rank", "string_mtf_cdelta"}
    if any(any(ch.isdigit() for ch in value) for value in sample):
        allowed.add("byte_phrase_delta")
        if should_probe_mixed_skeleton(sample):
            allowed.add("mixed_skeleton_delta")
        if should_probe_shape_mixed_skeleton(sample):
            allowed.add("shape_mixed_skeleton_delta")
    return allowed


def stream_codec_candidates(values: list[str], forced_kinds: set[str] | None = None) -> list[tuple[str, int]]:
    global STREAM_CODEC_CANDIDATE_CACHE_HITS
    global STREAM_CODEC_CANDIDATE_CACHE_MISSES
    global STREAM_CODEC_CANDIDATE_CACHE_SKIPS
    cache_enabled = os.environ.get("PARE_STREAM_CODEC_CANDIDATE_CACHE", "0") == "1"
    cache_key = _stream_codec_candidate_cache_key(values, forced_kinds) if cache_enabled else None
    if cache_enabled and cache_key is not None:
        cached = STREAM_CODEC_CANDIDATE_CACHE.get(cache_key)
        if cached is not None:
            STREAM_CODEC_CANDIDATE_CACHE_HITS += 1
            if OPERATION_PROFILE:
                record_operation_timing("stream_codec_candidates.cache_hit", 0.0, count=1)
            return list(cached)
        STREAM_CODEC_CANDIDATE_CACHE_MISSES += 1
    elif cache_enabled:
        STREAM_CODEC_CANDIDATE_CACHE_SKIPS += 1

    candidates: list[tuple[str, int]] = []
    allowed = set(forced_kinds) if forced_kinds is not None else _codec_allowed_by_features(values)

    def wants(kind: str) -> bool:
        return allowed is None or kind in allowed

    if wants("string"):
        candidates.append(("string", compressed_parts_cost([encode_string_stream_bytes(values)])))

    if wants("string_mtf_rank"):
        ranks, literals = encode_string_mtf_rank(values)
        candidates.append((
            "string_mtf_rank",
            compressed_parts_cost([encode_varint_stream_bytes(ranks), encode_string_stream_bytes(literals)]),
        ))

    if wants("string_mtf_cdelta"):
        ranks, literals = encode_string_mtf_cdelta(values)
        candidates.append((
            "string_mtf_cdelta",
            compressed_parts_cost([encode_delta_values(ranks), encode_string_stream_bytes(literals)]),
        ))

    if wants("byte_phrase_delta"):
        try:
            byte_delta, byte_flags, byte_exceptions = encode_byte_phrase_values(values)
            candidates.append((
                "byte_phrase_delta",
                compressed_parts_cost([byte_delta, byte_flags, encode_string_stream_bytes(byte_exceptions)]),
            ))
        except Exception:
            pass

    if wants("hex_int_delta_width"):
        try:
            hex_delta, hex_widths, hex_cases = encode_hex_int_width(values)
            candidates.append(("hex_int_delta_width", compressed_parts_cost([hex_delta, hex_widths, hex_cases])))
        except Exception:
            pass

    if wants("hex_pair_delta"):
        try:
            left_delta, right_delta, widths, case_flags = encode_hex_pair_delta(values)
            candidates.append(("hex_pair_delta", compressed_parts_cost([left_delta, right_delta, widths, case_flags])))
        except Exception:
            pass

    if wants("ipv4_width"):
        try:
            candidates.append(("ipv4_width", compressed_parts_cost([encode_ipv4_width(values)])))
        except Exception:
            pass

    if wants("ipv4_plain"):
        try:
            candidates.append(("ipv4_plain", compressed_parts_cost([encode_ipv4_plain(values)])))
        except Exception:
            pass

    if wants("compound_endpoint_mixed"):
        try:
            endpoint_parts = encode_compound_endpoint_mixed_parts(values)
            candidates.append(("compound_endpoint_mixed", compressed_parts_cost(list(endpoint_parts.values()))))
        except Exception:
            pass

    if wants("hms_delta"):
        try:
            candidates.append(("hms_delta", compressed_parts_cost([
                encode_delta_values([parse_hms(value) for value in values])
            ])))
        except Exception:
            pass

    if wants("day_hms_delta"):
        try:
            encoded_values: list[int] = []
            widths = bytearray()
            for value in values:
                day, width, seconds = parse_day_hms(value)
                encoded_values.append(day * 24 * 3600 + seconds)
                widths.append(width)
            candidates.append(("day_hms_delta", compressed_parts_cost([encode_delta_values(encoded_values), bytes(widths)])))
        except Exception:
            pass

    if wants("month_day_hms_delta"):
        try:
            candidates.append(("month_day_hms_delta", compressed_parts_cost([
                encode_delta_values([parse_month_day_hms(value) for value in values])
            ])))
        except Exception:
            pass

    if wants("syslog_delta"):
        try:
            encoded_values: list[int] = []
            layouts = bytearray()
            for value in values:
                encoded_value, layout = parse_syslog_timestamp(value)
                encoded_values.append(encoded_value)
                layouts.append(layout)
            candidates.append(("syslog_delta", compressed_parts_cost([encode_delta_values(encoded_values), bytes(layouts)])))
        except Exception:
            pass

    if wants("asctime_year_delta"):
        try:
            encoded_values: list[int] = []
            layouts = bytearray()
            for value in values:
                encoded_value, layout = parse_asctime_year_timestamp_with_layout(value)
                encoded_values.append(encoded_value)
                layouts.append(layout)
            candidates.append(("asctime_year_delta", compressed_parts_cost([
                encode_delta_values(encoded_values),
                bytes(layouts),
            ])))
        except Exception:
            pass

    if wants("apache_timestamp_delta"):
        try:
            candidates.append(("apache_timestamp_delta", compressed_parts_cost([
                encode_delta_values([parse_apache_full_timestamp(value) for value in values])
            ])))
        except Exception:
            pass

    if wants("packed_datetime_delta"):
        try:
            delta_data, layout_data, _fraction_scale = encode_packed_datetime_delta(values)
            candidates.append(("packed_datetime_delta", compressed_parts_cost([delta_data, layout_data])))
        except Exception:
            pass

    if wants("affixed_int_delta"):
        try:
            delta_data, width_data, _prefix, _suffix = encode_affixed_int_delta(values)
            candidates.append(("affixed_int_delta", compressed_parts_cost([delta_data, width_data])))
        except Exception:
            pass

    if wants("delimited_int_tuple_delta"):
        try:
            column_data, width_data, _delimiter, _arity = encode_delimited_int_tuple_delta(values)
            candidates.append(("delimited_int_tuple_delta", compressed_parts_cost(column_data + [width_data])))
        except Exception:
            pass

    if wants("numeric_skeleton_tuple_delta"):
        try:
            column_data, layout_data, _literals, _arity = encode_numeric_skeleton_tuple_delta(values)
            candidates.append(("numeric_skeleton_tuple_delta", compressed_parts_cost(column_data + [layout_data])))
        except Exception:
            pass

    if wants("mixed_skeleton_delta"):
        try:
            numeric_data, layout_data, literal_rank_data, literal_tables, _arity = encode_mixed_skeleton_delta(values)
            literal_table_data = [encode_string_stream_bytes(table) for table in literal_tables]
            candidates.append((
                "mixed_skeleton_delta",
                compressed_parts_cost(numeric_data + [layout_data] + literal_rank_data + literal_table_data),
            ))
        except Exception:
            pass

    if wants("shape_mixed_skeleton_delta"):
        try:
            shape_id_data, shape_meta_data, numeric_data, layout_data, _shape_count = encode_shape_mixed_skeleton_delta(values)
            flat_numeric_data = [data for columns in numeric_data for data in columns]
            candidates.append((
                "shape_mixed_skeleton_delta",
                compressed_parts_cost([shape_id_data, shape_meta_data] + flat_numeric_data + layout_data),
            ))
        except Exception:
            pass

    if wants("colon_duration_delta"):
        try:
            encoded_values: list[int] = []
            layouts = bytearray()
            for value in values:
                encoded_value, parts = parse_colon_duration(value)
                encoded_values.append(encoded_value)
                layouts.append(parts)
            candidates.append(("colon_duration_delta", compressed_parts_cost([encode_delta_values(encoded_values), bytes(layouts)])))
        except Exception:
            pass

    if any(wants(kind) for kind in {
        "int_abs",
        "int_abs_width",
        "int_delta_width",
        "int_abs_fixed_width",
        "int_delta_fixed_width",
        "uint16_split_width",
    }):
        try:
            encoded_values: list[int] = []
            widths = bytearray()
            has_leading_zero = False
            for value in values:
                encoded_value, width = parse_int_width(value)
                encoded_values.append(encoded_value)
                widths.append(width)
                if width > 1 and value.startswith("0"):
                    has_leading_zero = True
            abs_safe = can_decode_varint_stream(encoded_values)
            delta_safe = can_decode_delta_stream(encoded_values)
            if wants("int_abs") and abs_safe and not has_leading_zero:
                candidates.append(("int_abs", compressed_parts_cost([encode_varint_stream_bytes(encoded_values)])))
            if wants("int_abs_width") and abs_safe:
                candidates.append(("int_abs_width", compressed_parts_cost([encode_varint_stream_bytes(encoded_values), bytes(widths)])))
            if wants("int_delta_width") and delta_safe:
                candidates.append(("int_delta_width", compressed_parts_cost([encode_delta_values(encoded_values), bytes(widths)])))
            if len(set(widths)) == 1:
                if wants("int_abs_fixed_width") and abs_safe:
                    candidates.append(("int_abs_fixed_width", compressed_parts_cost([encode_varint_stream_bytes(encoded_values)])))
                if wants("int_delta_fixed_width") and delta_safe:
                    candidates.append(("int_delta_fixed_width", compressed_parts_cost([encode_delta_values(encoded_values)])))
            if wants("uint16_split_width") and all(0 <= value <= 65535 for value in encoded_values):
                low_bytes = bytearray()
                high_bytes = bytearray()
                for value in encoded_values:
                    high_bytes.append(value >> 8)
                    low_bytes.append(value & 0xFF)
                candidates.append(("uint16_split_width", compressed_parts_cost([bytes(low_bytes + high_bytes), bytes(widths)])))
        except Exception:
            pass

    if wants("int_denum_bucket_delta"):
        try:
            if len(values) >= 64:
                bucket_ids, bucket_meta, bucket_deltas = encode_denum_bucket_delta(values)
                candidates.append(("int_denum_bucket_delta", compressed_parts_cost([bucket_ids, bucket_meta, bucket_deltas])))
        except Exception:
            pass

    if not candidates and allowed is not None and forced_kinds is None:
        previous = FEATURE_CODEC_PREFILTER
        set_feature_codec_prefilter(False)
        try:
            return stream_codec_candidates(values)
        finally:
            set_feature_codec_prefilter(previous)
    if cache_enabled and cache_key is not None and candidates:
        max_entries = int(os.environ.get("PARE_STREAM_CODEC_CANDIDATE_CACHE_MAX", "8192"))
        if len(STREAM_CODEC_CANDIDATE_CACHE) >= max_entries:
            STREAM_CODEC_CANDIDATE_CACHE.clear()
        STREAM_CODEC_CANDIDATE_CACHE[cache_key] = list(candidates)
        if OPERATION_PROFILE:
            record_operation_timing("stream_codec_candidates.cache_store", 0.0, count=1)
    return candidates


def select_auto_stream_spec(
    spec: ExtractSpec,
    values_by_tag: dict[str, list[str]],
    placeholders_by_tag: dict[str, str],
    transformed_text: str,
    candidate_context_tags: list[str],
) -> tuple[ExtractSpec, dict[str, object]]:
    values = values_by_tag[spec.tag]
    if os.environ.get("PARE_NO_BENEFIT_FIXED_CODEC", "0") == "1":
        def left_context_tag_for_target() -> str | None:
            target_placeholder = placeholders_by_tag.get(spec.tag)
            if not target_placeholder:
                return None
            context_placeholders = [
                (tag, placeholders_by_tag[tag])
                for tag in candidate_context_tags
                if tag != spec.tag and tag in values_by_tag and tag in placeholders_by_tag
            ]
            if not context_placeholders:
                return None
            total = 0
            hits: Counter[str] = Counter()
            for line in transformed_text.splitlines(True):
                cursor = 0
                while True:
                    target_index = line.find(target_placeholder, cursor)
                    if target_index < 0:
                        break
                    total += 1
                    best_tag: str | None = None
                    best_end = -1
                    for context_tag, context_placeholder in context_placeholders:
                        context_index = line.rfind(context_placeholder, 0, target_index)
                        if context_index < 0:
                            continue
                        context_end = context_index + len(context_placeholder)
                        if context_end <= best_end:
                            continue
                        between = line[context_end:target_index]
                        if between and any(ch.isalnum() or ch in "<>" for ch in between):
                            continue
                        best_tag = context_tag
                        best_end = context_end
                    if best_tag is not None:
                        hits[best_tag] += 1
                    cursor = target_index + len(target_placeholder)
            if total <= 0 or not hits:
                return None
            context_tag, count = hits.most_common(1)[0]
            if count * 4 < total * 3:
                return None
            return context_tag

        if values and all(re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", value) for value in values):
            try:
                encode_ipv4_plain(values)
                kind = "ipv4_plain"
            except Exception:
                kind = "string_mtf_rank"
        elif values and all(re.fullmatch(r"-?\d+", value) for value in values):
            if os.environ.get("PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA", "0") == "1":
                try:
                    numeric_values = [int(value) for value in values]
                    if all(0 <= value <= 65535 for value in numeric_values):
                        context_tag = left_context_tag_for_target()
                        if context_tag is not None:
                            encode_port_ip_grouped_circular_delta_width(
                                transformed_text=transformed_text,
                                port_values=values,
                                ip_values=values_by_tag[context_tag],
                                port_placeholder=placeholders_by_tag[spec.tag],
                                ip_placeholder=placeholders_by_tag[context_tag],
                            )
                            derive_context_counts(
                                transformed_text=transformed_text,
                                context_values=values_by_tag[context_tag],
                                context_placeholder=placeholders_by_tag[context_tag],
                                target_placeholder=placeholders_by_tag[spec.tag],
                            )
                            return (
                                ExtractSpec(
                                    tag=spec.tag,
                                    pattern=spec.pattern,
                                    kind="port_ip_grouped_circular_delta_width",
                                    store_group=spec.store_group,
                                    replacement=spec.replacement,
                                    context_tag=context_tag,
                                ),
                                {
                                    "auto_selected": True,
                                    "auto_no_benefit_fixed_codec": True,
                                    "auto_left_context_delta": True,
                                    "auto_context_tag": context_tag,
                                    "auto_cost": -1,
                                    "auto_candidate_count": 1,
                                },
                            )
                except Exception:
                    pass
            if os.environ.get("PARE_NO_BENEFIT_NUMERIC_ABS", "0") == "1":
                try:
                    parsed = [parse_int_width(value) for value in values]
                    widths = {width for _value, width in parsed}
                    encoded_values = [value for value, _width in parsed]
                    has_leading_zero = any(len(value) > 1 and value.startswith("0") for value in values)
                    if len(widths) == 1:
                        kind = "int_abs_fixed_width"
                    elif not has_leading_zero and can_decode_varint_stream(encoded_values):
                        kind = "int_abs"
                    else:
                        kind = "string_mtf_rank"
                except Exception:
                    kind = "string_mtf_rank"
            else:
                try:
                    parsed = [parse_int_width(value) for value in values]
                    widths = {width for _value, width in parsed}
                    encoded_values = [value for value, _width in parsed]
                    has_leading_zero = any(len(value) > 1 and value.startswith("0") for value in values)
                    if len(widths) == 1:
                        kind = "int_delta_fixed_width"
                    elif not has_leading_zero and can_decode_delta_stream(encoded_values):
                        kind = "int_delta"
                    else:
                        kind = "int_delta_width"
                except Exception:
                    kind = "string_mtf_rank"
        else:
            kind = "string_mtf_rank"
        return (
            ExtractSpec(
                tag=spec.tag,
                pattern=spec.pattern,
                kind=kind,
                store_group=spec.store_group,
                replacement=spec.replacement,
            ),
            {
                "auto_selected": True,
                "auto_no_benefit_fixed_codec": True,
                "auto_cost": -1,
                "auto_candidate_count": 1,
            },
        )
    if os.environ.get("PARE_TIMESTAMP_ONLY_DELTA", "0") == "1":
        return (
            ExtractSpec(
                tag=spec.tag,
                pattern=spec.pattern,
                kind="string_mtf_rank",
                store_group=spec.store_group,
                replacement=spec.replacement,
            ),
            {
                "auto_selected": True,
                "auto_timestamp_only_delta": True,
                "auto_cost": -1,
                "auto_candidate_count": 1,
            },
        )
    if os.environ.get("PARE_STRICT_SIMPLE_CODEC", "0") == "1":
        if values and all(re.fullmatch(r"\d+", value) for value in values):
            kind = "int_denum_bucket_delta"
        elif values and all(re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", value) for value in values):
            try:
                encode_ipv4_plain(values)
                kind = "ipv4_plain"
            except Exception:
                kind = "string_mtf_rank"
        else:
            kind = "string_mtf_rank"
        return (
            ExtractSpec(
                tag=spec.tag,
                pattern=spec.pattern,
                kind=kind,
                store_group=spec.store_group,
                replacement=spec.replacement,
            ),
            {
                "auto_selected": True,
                "auto_strict_simple_codec": True,
                "auto_cost": -1,
                "auto_candidate_count": 1,
            },
        )
    if os.environ.get("PARE_CLASS_FIXED_CODEC", "0") == "1":
        # Design-space probe: route class-3/raw variable streams directly to a
        # dictionary codec instead of racing all auto codecs.
        return (
            ExtractSpec(
                tag=spec.tag,
                pattern=spec.pattern,
                kind="string_mtf_rank",
                store_group=spec.store_group,
                replacement=spec.replacement,
            ),
            {
                "auto_selected": True,
                "auto_class_fixed_codec": True,
                "auto_cost": -1,
                "auto_candidate_count": 1,
            },
        )
    cached_spec = cached_codec_decision_spec(
        spec,
        values_by_tag=values_by_tag,
        placeholders_by_tag=placeholders_by_tag,
    )
    if cached_spec is not None:
        selection_meta: dict[str, object] = {
            "auto_selected": True,
            "auto_from_decision_cache": True,
            "auto_cost": -1,
            "auto_candidate_count": 1,
        }
        if cached_spec.context_tag is not None and cached_spec.kind in CONTEXT_CODEC_KINDS:
            selection_meta["auto_context_tag"] = cached_spec.context_tag
        return cached_spec, selection_meta

    fast_codec_mode = os.environ.get("PARE_FAST_CODEC_SELECT", "0")
    if fast_codec_mode in {"1", "hybrid", "numeric_context_hybrid"}:
        selected = fast_feature_select_auto_stream_spec(
            spec,
            values_by_tag=values_by_tag,
            placeholders_by_tag=placeholders_by_tag,
            transformed_text=transformed_text,
            candidate_context_tags=candidate_context_tags,
            mode=fast_codec_mode,
        )
        if selected is not None:
            selected_spec, selected_meta = selected
            return selected_spec, selected_meta

    candidates = stream_codec_candidates(values)
    slot_fission_base: str | None = None
    target_count = len(values)
    if spec.pattern.startswith("slot-fission:"):
        slot_fission_base = re.sub(r"\d+$", "", spec.tag)
    selected_context_tags = list(candidate_context_tags)
    if FEATURE_CODEC_PREFILTER and slot_fission_base is not None:
        scored_contexts: list[tuple[float, int, str]] = []
        for context_tag in candidate_context_tags:
            context_values = values_by_tag.get(context_tag)
            if context_values is None or context_tag == spec.tag:
                continue
            if re.sub(r"\d+$", "", context_tag) != slot_fission_base:
                continue
            context_count = len(context_values)
            if target_count <= 0 or context_count <= 0:
                continue
            ratio = context_count / target_count
            if ratio < 0.25 or ratio > 4.0:
                continue
            scored_contexts.append((abs(math.log(ratio)), -min(context_count, target_count), context_tag))
        close_contexts = [tag for _dist, _support, tag in sorted(scored_contexts)[:6]]
        large_contexts = [
            tag
            for _dist, _support, tag in sorted(scored_contexts, key=lambda item: (item[1], item[0]))[:4]
        ]
        selected_context_tags = list(dict.fromkeys(close_contexts + large_contexts))

    context_candidates: list[tuple[str, str, int]] = []
    for context_tag in selected_context_tags:
        try:
            context_values = values_by_tag.get(context_tag)
            if context_values is None:
                continue
            if context_tag == spec.tag:
                continue
            if slot_fission_base is not None:
                # Slot fission creates a family of sibling streams from one
                # parent placeholder.  Context codecs are useful inside that
                # family (e.g., REPORT_pos_4 predicted by REPORT_pos_1), but
                # trying every previously serialized stream is quadratic and
                # mostly noise.  This is a decode-safe search prune: rejected
                # contexts are only candidate predictors, never required data.
                if re.sub(r"\d+$", "", context_tag) != slot_fission_base:
                    continue
                context_count = len(context_values)
                if target_count <= 0 or context_count <= 0:
                    continue
                ratio = context_count / target_count
                if ratio < 0.25 or ratio > 4.0:
                    continue
            data, widths = encode_port_ip_grouped_circular_delta_width(
                transformed_text=transformed_text,
                port_values=values,
                ip_values=context_values,
                port_placeholder=placeholders_by_tag[spec.tag],
                ip_placeholder=placeholders_by_tag[context_tag],
            )
            # The encoder-side grouping may appear cheap even when the decoder
            # cannot derive the same context cardinalities from placeholder
            # order.  Validate the decoder contract before admitting the
            # context codec; otherwise exact restore can fail after selection.
            derive_context_counts(
                transformed_text=transformed_text,
                context_values=context_values,
                context_placeholder=placeholders_by_tag[context_tag],
                target_placeholder=placeholders_by_tag[spec.tag],
            )
            context_candidates.append((
                "port_ip_grouped_circular_delta_width",
                context_tag,
                compressed_parts_cost([data, widths]) + CONDITIONAL_CODEC_MODEL_COST,
            ))
        except Exception:
            pass

        try:
            context_values = values_by_tag.get(context_tag)
            if context_values is None:
                continue
            if slot_fission_base is not None:
                if re.sub(r"\d+$", "", context_tag) != slot_fission_base:
                    continue
                context_count = len(context_values)
                if target_count <= 0 or context_count <= 0:
                    continue
                ratio = context_count / target_count
                if ratio < 0.25 or ratio > 4.0:
                    continue
            flags, exceptions = encode_size_from_prev_bytes(
                transformed_text=transformed_text,
                size_values=values,
                byte_values=context_values,
                size_placeholder=placeholders_by_tag[spec.tag],
                byte_placeholder=placeholders_by_tag[context_tag],
            )
            if not exceptions or len(exceptions) < len(values):
                context_candidates.append((
                    "size_from_prev_bytes",
                    context_tag,
                    compressed_parts_cost([flags, encode_string_stream_bytes(exceptions)]) + CONDITIONAL_CODEC_MODEL_COST,
                ))
        except Exception:
            pass

    best_kind, best_cost = min(candidates, key=lambda item: item[1])
    best_context: str | None = None
    for kind, context_tag, cost in context_candidates:
        if cost < best_cost:
            best_kind = kind
            best_cost = cost
            best_context = context_tag

    selected = ExtractSpec(
        tag=spec.tag,
        pattern=spec.pattern,
        kind=best_kind,
        store_group=spec.store_group,
        replacement=spec.replacement,
        context_tag=best_context,
    )
    selection_meta = {
        "auto_selected": True,
        "auto_cost": best_cost,
        "auto_candidate_count": len(candidates) + len(context_candidates),
    }
    if best_context is not None:
        selection_meta["auto_context_tag"] = best_context
    return selected, selection_meta


def fast_feature_select_auto_stream_spec(
    spec: ExtractSpec,
    values_by_tag: dict[str, list[str]],
    placeholders_by_tag: dict[str, str],
    transformed_text: str,
    candidate_context_tags: list[str],
    mode: str = "1",
) -> tuple[ExtractSpec, dict[str, object]] | None:
    """Choose a stream codec from cheap value features, not compressed cost.

    The selector is deliberately conservative: it only chooses codecs whose
    encoder preconditions can be checked from the value stream itself.  It does
    not call ``compressed_parts_cost`` or try multiple codecs and keep the
    smallest one.  This is a paper-facing fast path for replacing expensive
    per-stream codec racing with an explainable feature-to-codec map.
    """
    values = values_by_tag.get(spec.tag, [])
    if not values:
        return None
    sample = values[: min(len(values), 4096)]
    unique_ratio = len(set(sample)) / max(1, len(sample))

    def make(kind: str, context_tag: str | None = None) -> tuple[ExtractSpec, dict[str, object]]:
        selected_spec = ExtractSpec(
            tag=spec.tag,
            pattern=spec.pattern,
            kind=kind,
            store_group=spec.store_group,
            replacement=spec.replacement,
            context_tag=context_tag,
        )
        meta: dict[str, object] = {
            "auto_selected": True,
            "auto_feature_selected": True,
            "auto_cost": -1,
            "auto_candidate_count": 1,
        }
        if context_tag is not None:
            meta["auto_context_tag"] = context_tag
        return selected_spec, meta

    def placeholder_locality_ratio(context_placeholder: str, target_placeholder: str, window: int = 48) -> float:
        total = 0
        local = 0
        for line in transformed_text.splitlines(True):
            cursor = 0
            while True:
                target_index = line.find(target_placeholder, cursor)
                if target_index < 0:
                    break
                total += 1
                left = max(0, target_index - window)
                right = min(len(line), target_index + len(target_placeholder) + window)
                if context_placeholder in line[left:right]:
                    local += 1
                cursor = target_index + len(target_placeholder)
        if total <= 0:
            return 0.0
        return local / total

    def integer_stream_raw_size(int_strings: list[str]) -> int:
        try:
            parsed = [parse_int_width(value) for value in int_strings]
            nums = [item[0] for item in parsed]
            widths = [item[1] for item in parsed]
        except Exception:
            return 1 << 60
        fixed_width = len(set(widths)) == 1
        has_leading_zero = any(width > 1 and value.startswith("0") for value, width in zip(int_strings, widths))
        candidates: list[int] = []
        try:
            if not has_leading_zero:
                candidates.append(len(encode_varint_stream_bytes(nums)))
            candidates.append(len(encode_varint_stream_bytes(nums)) + (0 if fixed_width and not has_leading_zero else len(widths)))
        except Exception:
            pass
        try:
            candidates.append(len(encode_delta_values(nums)) + (0 if fixed_width else len(widths)))
        except Exception:
            pass
        return min(candidates) if candidates else (1 << 60)

    def all_match(pattern: str) -> bool:
        return all(re.fullmatch(pattern, value) for value in values)

    # Context codec: integer ports often become highly compressible when routed
    # by a previously extracted IP/endpoint stream.  We admit the context codec
    # only when decoder context counts are derivable; this preserves SHA safety.
    if all_match(r"\d{1,5}"):
        try:
            int_values = [int(value) for value in sample]
            port_like = all(0 <= value <= 65535 for value in int_values)
        except Exception:
            port_like = False
        if port_like and candidate_context_tags:
            for context_tag in candidate_context_tags:
                if context_tag == spec.tag:
                    continue
                context_values = values_by_tag.get(context_tag)
                if not context_values:
                    continue
                context_sample = context_values[: min(len(context_values), 1024)]
                context_ip_like = all(
                    re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", value)
                    for value in context_sample
                )
                if not context_ip_like:
                    continue
                try:
                    context_data, context_widths = encode_port_ip_grouped_circular_delta_width(
                        transformed_text=transformed_text,
                        port_values=values,
                        ip_values=context_values,
                        port_placeholder=placeholders_by_tag[spec.tag],
                        ip_placeholder=placeholders_by_tag[context_tag],
                    )
                    derive_context_counts(
                        transformed_text=transformed_text,
                        context_values=context_values,
                        context_placeholder=placeholders_by_tag[context_tag],
                        target_placeholder=placeholders_by_tag[spec.tag],
                    )
                except Exception:
                    continue
                locality = placeholder_locality_ratio(
                    placeholders_by_tag[context_tag],
                    placeholders_by_tag[spec.tag],
                )
                if locality < 0.75:
                    continue
                context_raw_size = len(context_data) + len(context_widths)
                numeric_raw_size = integer_stream_raw_size(values)
                if context_raw_size >= int(0.85 * numeric_raw_size):
                    continue
                return make("port_ip_grouped_circular_delta_width", context_tag=context_tag)

    if all_match(r"(?:\d{1,3}\.){3}\d{1,3}"):
        return make("string_mtf_rank" if unique_ratio <= 0.70 else "string")

    if all_match(r"0[xX][0-9A-Fa-f]+"):
        try:
            encode_hex_int_width(values)
            return make("hex_int_delta_width")
        except Exception:
            return make("string_mtf_rank" if unique_ratio <= 0.70 else "string")

    if all_match(r"-?\d+"):
        if mode == "hybrid":
            return None
        if mode == "numeric_context_hybrid" and candidate_context_tags:
            for context_tag in candidate_context_tags:
                if context_tag == spec.tag:
                    continue
                context_values = values_by_tag.get(context_tag)
                if not context_values:
                    continue
                context_sample = context_values[: min(len(context_values), 1024)]
                context_ip_like = all(
                    re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", value)
                    for value in context_sample
                )
                if not context_ip_like:
                    continue
                try:
                    locality = placeholder_locality_ratio(
                        placeholders_by_tag[context_tag],
                        placeholders_by_tag[spec.tag],
                    )
                except Exception:
                    locality = 0.0
                if locality >= 0.75:
                    return None
        try:
            parsed = [parse_int_width(value) for value in values]
        except Exception:
            return make("string")
        nums = [item[0] for item in parsed]
        widths = [item[1] for item in parsed]
        if max(widths, default=0) > NUMERIC_DELTA_PRACTICAL_MAX_DIGITS:
            return make("string_mtf_rank" if unique_ratio <= 0.70 else "string")
        fixed_width = len(set(widths)) == 1
        has_leading_zero = any(width > 1 and value.startswith("0") for value, width in zip(values, widths))
        if len(nums) >= 2:
            deltas = [abs(nums[index] - nums[index - 1]) for index in range(1, len(nums))]
            small_delta_ratio = sum(1 for delta in deltas if delta <= 8) / max(1, len(deltas))
        else:
            small_delta_ratio = 0.0
        if fixed_width and small_delta_ratio >= 0.35:
            return make("int_delta_fixed_width")
        if fixed_width:
            return make("int_abs_fixed_width")
        if small_delta_ratio >= 0.35 or has_leading_zero:
            return make("int_delta_width")
        return make("int_abs")

    # Affixed integer values such as "sid=123" or "port:2181" are common log
    # residuals.  The exact encoder will store prefix/suffix once and delta-code
    # the numeric part.
    try:
        encode_affixed_int_delta(values)
        return make("affixed_int_delta")
    except Exception:
        pass

    if all("." in value and ":" in value and any(ch.isdigit() for ch in value) for value in values):
        try:
            encode_compound_endpoint_mixed_parts(values)
            return make("compound_endpoint_mixed")
        except Exception:
            pass

    if unique_ratio <= 0.70:
        return make("string_mtf_rank")
    return make("string")


def select_atis_stream_spec(
    spec: ExtractSpec,
    dataset: str,
    values_by_tag: dict[str, list[str]],
    placeholders_by_tag: dict[str, str],
    transformed_text: str,
    candidate_context_tags: list[str],
) -> tuple[ExtractSpec, dict[str, object]]:
    fallback_spec, fallback_meta = select_auto_stream_spec(
        spec,
        values_by_tag=values_by_tag,
        placeholders_by_tag=placeholders_by_tag,
        transformed_text=transformed_text,
        candidate_context_tags=candidate_context_tags,
    )
    best_spec = fallback_spec
    best_cost = int(fallback_meta["auto_cost"])
    best_meta = dict(fallback_meta)
    best_meta["atis_selected"] = False

    target_tag = spec.tag.upper()
    for proposal in load_atis_proposals(dataset):
        if str(proposal.get("target_tag", proposal.get("tag", ""))).upper() != target_tag:
            continue
        role = str(proposal.get("role", "")).upper()
        proposal_spec: ExtractSpec | None = None
        proposal_cost: int | None = None

        if role == "TIME_TUPLE_DELTA":
            time_kinds = {
                "hms_delta",
                "day_hms_delta",
                "month_day_hms_delta",
                "syslog_delta",
                "apache_timestamp_delta",
                "colon_duration_delta",
            }
            candidates = [
                (kind, cost)
                for kind, cost in stream_codec_candidates(values_by_tag[spec.tag])
                if kind in time_kinds
            ]
            if candidates:
                kind, cost = min(candidates, key=lambda item: item[1])
                proposal_spec = ExtractSpec(spec.tag, spec.pattern, kind, spec.store_group, spec.replacement)
                proposal_cost = cost + CONDITIONAL_CODEC_MODEL_COST

        elif role == "GROUPED_UINT16_DELTA_BY_CONTEXT":
            context_tag = str(proposal.get("source_tag", proposal.get("context_tag", ""))).upper()
            if context_tag in values_by_tag and context_tag in placeholders_by_tag:
                try:
                    data, widths = encode_port_ip_grouped_circular_delta_width(
                        transformed_text=transformed_text,
                        port_values=values_by_tag[spec.tag],
                        ip_values=values_by_tag[context_tag],
                        port_placeholder=placeholders_by_tag[spec.tag],
                        ip_placeholder=placeholders_by_tag[context_tag],
                    )
                    proposal_spec = ExtractSpec(
                        spec.tag,
                        spec.pattern,
                        "port_ip_grouped_circular_delta_width",
                        spec.store_group,
                        spec.replacement,
                        context_tag=context_tag,
                    )
                    proposal_cost = compressed_parts_cost([data, widths]) + CONDITIONAL_CODEC_MODEL_COST
                except Exception:
                    pass

        elif role == "PREDICT_SIZE_FROM_PREV":
            context_tag = str(proposal.get("source_tag", proposal.get("context_tag", ""))).upper()
            if context_tag in values_by_tag and context_tag in placeholders_by_tag:
                try:
                    flags, exceptions = encode_size_from_prev_bytes(
                        transformed_text=transformed_text,
                        size_values=values_by_tag[spec.tag],
                        byte_values=values_by_tag[context_tag],
                        size_placeholder=placeholders_by_tag[spec.tag],
                        byte_placeholder=placeholders_by_tag[context_tag],
                    )
                    proposal_spec = ExtractSpec(
                        spec.tag,
                        spec.pattern,
                        "size_from_prev_bytes",
                        spec.store_group,
                        spec.replacement,
                        context_tag=context_tag,
                    )
                    proposal_cost = compressed_parts_cost([flags, encode_string_stream_bytes(exceptions)]) + CONDITIONAL_CODEC_MODEL_COST
                except Exception:
                    pass

        elif role == "DICT_HINT":
            ranks, literals = encode_string_mtf_rank(values_by_tag[spec.tag])
            proposal_spec = ExtractSpec(spec.tag, spec.pattern, "string_mtf_rank", spec.store_group, spec.replacement)
            proposal_cost = compressed_parts_cost([
                encode_varint_stream_bytes(ranks),
                encode_string_stream_bytes(literals),
            ]) + CONDITIONAL_CODEC_MODEL_COST

        if proposal_spec is not None and proposal_cost is not None and proposal_cost <= best_cost + CONDITIONAL_CODEC_MODEL_COST:
            best_spec = proposal_spec
            best_cost = proposal_cost
            best_meta = {
                "auto_selected": True,
                "auto_cost": best_cost,
                "auto_candidate_count": int(fallback_meta.get("auto_candidate_count", 0)) + 1,
                "atis_selected": True,
                "atis_role": role,
                "atis_delta_vs_fallback": proposal_cost - int(fallback_meta["auto_cost"]),
            }
            if proposal_spec.context_tag is not None:
                best_meta["auto_context_tag"] = proposal_spec.context_tag

    return best_spec, best_meta


def encode_ipv4_width(tokens: list[str]) -> bytes:
    data = bytearray()
    for token in tokens:
        octets = token.split(".")
        if len(octets) != 4:
            raise ValueError(f"Bad IPv4 token {token!r}")
        width_mask = 0
        for index, octet_text in enumerate(octets):
            value = int(octet_text)
            if value < 0 or value > 255:
                raise ValueError(f"Bad IPv4 octet {octet_text!r} in {token!r}")
            width = len(octet_text)
            if width < 1 or width > 3:
                raise ValueError(f"Bad IPv4 octet width {width} in {token!r}")
            data.append(value)
            width_mask |= (width - 1) << (index * 2)
        data.append(width_mask)
    return bytes(data)


def encode_ipv4_plain(tokens: list[str]) -> bytes:
    data = bytearray()
    for token in tokens:
        octets = token.split(".")
        if len(octets) != 4:
            raise ValueError(f"Bad IPv4 token {token!r}")
        for octet_text in octets:
            value = int(octet_text)
            if value < 0 or value > 255 or str(value) != octet_text:
                raise ValueError(f"IPv4 token is not reversible without widths: {token!r}")
            data.append(value)
    return bytes(data)


def can_encode_ipv4_plain(tokens: list[str]) -> bool:
    if not tokens:
        return False
    try:
        encode_ipv4_plain(tokens)
        return True
    except Exception:
        return False


def decode_ipv4_plain(data: bytes) -> list[str]:
    if len(data) % 4 != 0:
        raise ValueError("Corrupt plain ipv4 stream")
    values: list[str] = []
    for cursor in range(0, len(data), 4):
        values.append(".".join(str(byte) for byte in data[cursor:cursor + 4]))
    return values


def decode_ipv4_width(data: bytes) -> list[str]:
    if len(data) % 5 != 0:
        raise ValueError("Corrupt ipv4_width stream")
    values: list[str] = []
    for cursor in range(0, len(data), 5):
        octet_values = data[cursor:cursor + 4]
        width_mask = data[cursor + 4]
        octets: list[str] = []
        for index, octet_value in enumerate(octet_values):
            width = ((width_mask >> (index * 2)) & 0x03) + 1
            octets.append(f"{octet_value:0{width}d}")
        values.append(".".join(octets))
    return values


def parse_context_pairs(values: list[str]) -> tuple[list[str], list[str]]:
    raw_values: list[str] = []
    context_values: list[str] = []
    for item in values:
        payload = json.loads(item)
        if not isinstance(payload, list) or len(payload) < 2:
            raise ValueError("Context-aware stream values must be [raw, context]")
        raw_values.append(str(payload[0]))
        context_values.append(str(payload[1]))
    return raw_values, context_values


def decode_varint_stream_values(data: bytes) -> list[int]:
    values: list[int] = []
    cursor = 0
    while cursor < len(data):
        value, cursor = base.decode_varint(data, cursor)
        values.append(value)
    return values


def encode_context_grouped_delta_values(numeric_values: list[int], context_values: list[str]) -> bytes:
    last_by_context: dict[str, int] = {}
    deltas: list[int] = []
    for value, context in zip(numeric_values, context_values):
        previous = last_by_context.get(context, 0)
        deltas.append(value - previous)
        last_by_context[context] = value
    return encode_varint_stream_bytes([zigzag_encode(delta) for delta in deltas])


def decode_context_grouped_delta_values(
    data: bytes,
    context_values: list[str],
    zigzag_encoded: bool = False,
) -> list[int]:
    last_by_context: dict[str, int] = {}
    values: list[int] = []
    for encoded_delta, context in zip(decode_varint_stream_values(data), context_values):
        delta = zigzag_decode(encoded_delta) if zigzag_encoded else encoded_delta
        value = last_by_context.get(context, 0) + delta
        values.append(value)
        last_by_context[context] = value
    if len(values) != len(context_values):
        raise ValueError("Bad context-grouped delta stream")
    return values


def encode_context_grouped_string_mtf(values: list[str], context_values: list[str]) -> tuple[bytes, list[str]]:
    tables: dict[str, list[str]] = {}
    ranks: list[int] = []
    literals: list[str] = []
    for value, context in zip(values, context_values):
        table = tables.setdefault(context, [])
        try:
            rank = table.index(value) + 1
            ranks.append(rank)
            table.insert(0, table.pop(rank - 1))
        except ValueError:
            ranks.append(0)
            literals.append(value)
            table.insert(0, value)
            if len(table) > 512:
                table.pop()
    return encode_varint_stream_bytes(ranks), literals


def decode_context_grouped_string_mtf(rank_data: bytes, literals: list[str], context_values: list[str]) -> list[str]:
    tables: dict[str, list[str]] = {}
    literal_index = 0
    values: list[str] = []
    ranks = decode_varint_stream_values(rank_data)
    if len(ranks) != len(context_values):
        raise ValueError("Bad context-grouped string rank stream")
    for rank, context in zip(ranks, context_values):
        table = tables.setdefault(context, [])
        if rank == 0:
            if literal_index >= len(literals):
                raise ValueError("Context string literal stream exhausted")
            value = literals[literal_index]
            literal_index += 1
            table.insert(0, value)
        else:
            index = rank - 1
            if index < 0 or index >= len(table):
                raise ValueError("Bad context string rank")
            value = table[index]
            table.insert(0, table.pop(index))
        if len(table) > 512:
            table.pop()
        values.append(value)
    if literal_index != len(literals):
        raise ValueError("Unused context string literals")
    return values


def digit_width_signature(value: str) -> tuple[int, ...]:
    return tuple(len(part) for part in re.findall(r"\d+", value))


def write_tag_stream(output_dir: Path, spec: ExtractSpec, values: list[str]) -> dict[str, object]:
    if (
        spec.kind in {
            "int_delta",
            "int_delta_width",
            "int_delta_fixed_width",
            "int_abs",
            "int_abs_width",
            "int_abs_fixed_width",
            "int_denum_bucket_delta",
        }
        and exceeds_numeric_delta_width(values, NUMERIC_DELTA_SAFE_MAX_DIGITS)
    ):
        return write_tag_stream(
            output_dir,
            ExtractSpec(
                tag=spec.tag,
                pattern=spec.pattern,
                kind="string_mtf_rank",
                store_group=spec.store_group,
                replacement=spec.replacement,
                context_tag=spec.context_tag,
            ),
            values,
        )

    if spec.kind == "string":
        base.write_string_stream(output_dir / f"{spec.tag}.strings.bin", values)
        return {"tag": spec.tag, "kind": spec.kind, "count": len(values), "file": f"{spec.tag}.strings.bin"}

    if spec.kind == "string_mtf_rank":
        rank_path = output_dir / f"{spec.tag}.rank.bin"
        literal_path = output_dir / f"{spec.tag}.literal.bin"
        if not cpp_write_stream("string_mtf_rank", values, [rank_path, literal_path]):
            ranks, literals = encode_string_mtf_rank(values)
            base.write_varint_stream(rank_path, ranks)
            base.write_string_stream(literal_path, literals)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "rank_file": f"{spec.tag}.rank.bin",
            "literal_file": f"{spec.tag}.literal.bin",
            "table_size": 512,
        }

    if spec.kind == "string_mtf_cdelta":
        ranks, literals = encode_string_mtf_cdelta(values)
        (output_dir / f"{spec.tag}.cdelta.bin").write_bytes(encode_delta_values(ranks))
        base.write_string_stream(output_dir / f"{spec.tag}.literal.bin", literals)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "rank_file": f"{spec.tag}.cdelta.bin",
            "literal_file": f"{spec.tag}.literal.bin",
            "table_size": 512,
        }

    if spec.kind == "byte_phrase_delta":
        data, flags, exceptions = encode_byte_phrase_values(values)
        (output_dir / f"{spec.tag}.byte_delta.bin").write_bytes(data)
        (output_dir / f"{spec.tag}.byte_flags.bin").write_bytes(flags)
        base.write_string_stream(output_dir / f"{spec.tag}.byte_exceptions.bin", exceptions)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.byte_delta.bin",
            "flag_file": f"{spec.tag}.byte_flags.bin",
            "exception_file": f"{spec.tag}.byte_exceptions.bin",
        }

    if spec.kind == "hex_int_delta_width":
        data, widths, case_flags = encode_hex_int_width(values)
        (output_dir / f"{spec.tag}.hex_delta.bin").write_bytes(data)
        (output_dir / f"{spec.tag}.hex_width.bin").write_bytes(widths)
        (output_dir / f"{spec.tag}.hex_case.bin").write_bytes(case_flags)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.hex_delta.bin",
            "width_file": f"{spec.tag}.hex_width.bin",
            "case_file": f"{spec.tag}.hex_case.bin",
        }

    if spec.kind == "hex_pair_delta":
        left_delta, right_delta, widths, case_flags = encode_hex_pair_delta(values)
        (output_dir / f"{spec.tag}.hex_left_delta.bin").write_bytes(left_delta)
        (output_dir / f"{spec.tag}.hex_right_delta.bin").write_bytes(right_delta)
        (output_dir / f"{spec.tag}.hex_width.bin").write_bytes(widths)
        (output_dir / f"{spec.tag}.hex_case.bin").write_bytes(case_flags)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "left_file": f"{spec.tag}.hex_left_delta.bin",
            "right_file": f"{spec.tag}.hex_right_delta.bin",
            "width_file": f"{spec.tag}.hex_width.bin",
            "case_file": f"{spec.tag}.hex_case.bin",
        }

    if spec.kind == "ipv4_width":
        (output_dir / f"{spec.tag}.ipv4w.bin").write_bytes(encode_ipv4_width(values))
        return {"tag": spec.tag, "kind": spec.kind, "count": len(values), "file": f"{spec.tag}.ipv4w.bin"}

    if spec.kind == "ipv4_plain":
        path = output_dir / f"{spec.tag}.ipv4.bin"
        if not cpp_write_stream("ipv4_plain", values, [path]):
            path.write_bytes(encode_ipv4_plain(values))
        return {"tag": spec.tag, "kind": spec.kind, "count": len(values), "file": f"{spec.tag}.ipv4.bin"}

    if spec.kind == "compound_endpoint_mixed":
        parts = encode_compound_endpoint_mixed_parts(values)
        file_keys = {
            "types": "type_file",
            "ip_prefix": "ip_prefix_file",
            "ip_suffix": "ip_suffix_file",
            "ip_rank": "ip_rank_file",
            "ip_literal": "ip_literal_file",
            "ip_port_delta": "ip_port_delta_file",
            "ip_port_width": "ip_port_width_file",
            "pair_left_delta": "pair_left_file",
            "pair_right_delta": "pair_right_file",
            "pair_width": "pair_width_file",
            "raw": "raw_file",
        }
        meta: dict[str, object] = {"tag": spec.tag, "kind": spec.kind, "count": len(values)}
        for part_name, meta_key in file_keys.items():
            filename = f"{spec.tag}.compound.{part_name}.bin"
            (output_dir / filename).write_bytes(parts[part_name])
            meta[meta_key] = filename
        return meta

    if spec.kind == "hms_delta":
        encoded_values = [parse_hms(value) for value in values]
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        return {"tag": spec.tag, "kind": spec.kind, "count": len(values), "file": f"{spec.tag}.delta.bin"}

    if spec.kind == "day_hms_delta":
        encoded_values: list[int] = []
        widths = bytearray()
        for value in values:
            day, width, seconds = parse_day_hms(value)
            encoded_values.append(day * 24 * 3600 + seconds)
            widths.append(width)
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        (output_dir / f"{spec.tag}.width.bin").write_bytes(bytes(widths))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.delta.bin",
            "width_file": f"{spec.tag}.width.bin",
        }

    if spec.kind == "month_day_hms_delta":
        encoded_values = [parse_month_day_hms(value) for value in values]
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        return {"tag": spec.tag, "kind": spec.kind, "count": len(values), "file": f"{spec.tag}.delta.bin"}

    if spec.kind == "syslog_delta":
        encoded_values: list[int] = []
        layouts = bytearray()
        for value in values:
            encoded_value, layout = parse_syslog_timestamp(value)
            encoded_values.append(encoded_value)
            layouts.append(layout)
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        constant_layout = layouts[0] if layouts and len(set(layouts)) == 1 else None
        if constant_layout is None:
            (output_dir / f"{spec.tag}.layout.bin").write_bytes(bytes(layouts))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.delta.bin",
            **(
                {"layout": int(constant_layout)}
                if constant_layout is not None
                else {"layout_file": f"{spec.tag}.layout.bin"}
            ),
        }

    if spec.kind == "asctime_year_delta":
        encoded_values: list[int] = []
        layouts = bytearray()
        for value in values:
            encoded_value, layout = parse_asctime_year_timestamp_with_layout(value)
            encoded_values.append(encoded_value)
            layouts.append(layout)
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        (output_dir / f"{spec.tag}.layout.bin").write_bytes(bytes(layouts))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.delta.bin",
            "layout_file": f"{spec.tag}.layout.bin",
        }

    if spec.kind == "int_delta_width":
        encoded_values: list[int] = []
        widths = bytearray()
        for value in values:
            encoded_value, width = parse_int_width(value)
            encoded_values.append(encoded_value)
            widths.append(width)
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        (output_dir / f"{spec.tag}.width.bin").write_bytes(bytes(widths))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.delta.bin",
            "width_file": f"{spec.tag}.width.bin",
        }

    if spec.kind == "int_delta":
        encoded_values = [parse_int_width(value)[0] for value in values]
        path = output_dir / f"{spec.tag}.delta.bin"
        if not cpp_write_stream("delta", [str(value) for value in encoded_values], [path]):
            path.write_bytes(encode_delta_values(encoded_values))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.delta.bin",
        }

    if spec.kind == "int_abs_width":
        encoded_values: list[int] = []
        widths = bytearray()
        for value in values:
            encoded_value, width = parse_int_width(value)
            encoded_values.append(encoded_value)
            widths.append(width)
        base.write_varint_stream(output_dir / f"{spec.tag}.abs.bin", encoded_values)
        (output_dir / f"{spec.tag}.width.bin").write_bytes(bytes(widths))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.abs.bin",
            "width_file": f"{spec.tag}.width.bin",
        }

    if spec.kind == "int_abs_fixed_width":
        encoded_values: list[int] = []
        fixed_width: int | None = None
        for value in values:
            encoded_value, width = parse_int_width(value)
            if fixed_width is None:
                fixed_width = width
            elif width != fixed_width:
                raise ValueError(f"Mixed width in fixed-width stream {spec.tag}")
            encoded_values.append(encoded_value)
        base.write_varint_stream(output_dir / f"{spec.tag}.abs.bin", encoded_values)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.abs.bin",
            "width": int(fixed_width or 0),
        }

    if spec.kind == "int_delta_fixed_width":
        encoded_values: list[int] = []
        fixed_width: int | None = None
        for value in values:
            encoded_value, width = parse_int_width(value)
            if fixed_width is None:
                fixed_width = width
            elif width != fixed_width:
                raise ValueError(f"Mixed width in fixed-width stream {spec.tag}")
            encoded_values.append(encoded_value)
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.delta.bin",
            "width": int(fixed_width or 0),
        }

    if spec.kind == "int_abs":
        encoded_values = [parse_int_width(value)[0] for value in values]
        base.write_varint_stream(output_dir / f"{spec.tag}.abs.bin", encoded_values)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.abs.bin",
        }

    if spec.kind == "int_denum_bucket_delta":
        bucket_ids, bucket_meta, bucket_deltas = encode_denum_bucket_delta(values)
        (output_dir / f"{spec.tag}.denum_ids.bin").write_bytes(bucket_ids)
        (output_dir / f"{spec.tag}.denum_meta.bin").write_bytes(bucket_meta)
        (output_dir / f"{spec.tag}.denum_delta.bin").write_bytes(bucket_deltas)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "id_file": f"{spec.tag}.denum_ids.bin",
            "meta_file": f"{spec.tag}.denum_meta.bin",
            "delta_file": f"{spec.tag}.denum_delta.bin",
        }

    if spec.kind == "apache_timestamp_delta":
        encoded_values = [parse_apache_full_timestamp(value) for value in values]
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        return {"tag": spec.tag, "kind": spec.kind, "count": len(values), "file": f"{spec.tag}.delta.bin"}

    if spec.kind == "packed_datetime_delta":
        delta_data, layout_data, fraction_scale = encode_packed_datetime_delta(values)
        (output_dir / f"{spec.tag}.packed_dt_delta.bin").write_bytes(delta_data)
        (output_dir / f"{spec.tag}.packed_dt_layout.bin").write_bytes(layout_data)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.packed_dt_delta.bin",
            "layout_file": f"{spec.tag}.packed_dt_layout.bin",
            "fraction_scale": fraction_scale,
        }

    if spec.kind == "affixed_int_delta":
        delta_data, width_data, prefix, suffix = encode_affixed_int_delta(values)
        (output_dir / f"{spec.tag}.affixed_int_delta.bin").write_bytes(delta_data)
        (output_dir / f"{spec.tag}.affixed_int_width.bin").write_bytes(width_data)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.affixed_int_delta.bin",
            "width_file": f"{spec.tag}.affixed_int_width.bin",
            "prefix": prefix,
            "suffix": suffix,
        }

    if spec.kind == "delimited_int_tuple_delta":
        column_data, width_data, delimiter, arity = encode_delimited_int_tuple_delta(values)
        column_files: list[str] = []
        for index, data in enumerate(column_data):
            filename = f"{spec.tag}.tuple_col{index}.delta.bin"
            (output_dir / filename).write_bytes(data)
            column_files.append(filename)
        (output_dir / f"{spec.tag}.tuple_width.bin").write_bytes(width_data)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "column_files": column_files,
            "width_file": f"{spec.tag}.tuple_width.bin",
            "delimiter": delimiter,
            "arity": arity,
        }

    if spec.kind == "numeric_skeleton_tuple_delta":
        column_data, layout_data, literals, arity = encode_numeric_skeleton_tuple_delta(values)
        column_files: list[str] = []
        for index, data in enumerate(column_data):
            filename = f"{spec.tag}.numskel_col{index}.delta.bin"
            (output_dir / filename).write_bytes(data)
            column_files.append(filename)
        (output_dir / f"{spec.tag}.numskel_layout.bin").write_bytes(layout_data)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "column_files": column_files,
            "layout_file": f"{spec.tag}.numskel_layout.bin",
            "literals": literals,
            "arity": arity,
        }

    if spec.kind == "mixed_skeleton_delta":
        numeric_data, layout_data, literal_rank_data, literal_tables, arity = encode_mixed_skeleton_delta(values)
        numeric_files: list[str] = []
        for index, data in enumerate(numeric_data):
            filename = f"{spec.tag}.mixed_num{index}.delta.bin"
            (output_dir / filename).write_bytes(data)
            numeric_files.append(filename)
        (output_dir / f"{spec.tag}.mixed_layout.bin").write_bytes(layout_data)
        literal_rank_files: list[str] = []
        literal_table_files: list[str] = []
        for index, rank_data in enumerate(literal_rank_data):
            rank_filename = f"{spec.tag}.mixed_lit{index}.rank.bin"
            literal_filename = f"{spec.tag}.mixed_lit{index}.literal.bin"
            (output_dir / rank_filename).write_bytes(rank_data)
            base.write_string_stream(output_dir / literal_filename, literal_tables[index])
            literal_rank_files.append(rank_filename)
            literal_table_files.append(literal_filename)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "numeric_files": numeric_files,
            "layout_file": f"{spec.tag}.mixed_layout.bin",
            "literal_rank_files": literal_rank_files,
            "literal_table_files": literal_table_files,
            "arity": arity,
        }

    if spec.kind == "shape_mixed_skeleton_delta":
        shape_id_data, shape_meta_data, numeric_data, layout_data, shape_count = encode_shape_mixed_skeleton_delta(values)
        (output_dir / f"{spec.tag}.shapemix_ids.bin").write_bytes(shape_id_data)
        (output_dir / f"{spec.tag}.shapemix_meta.bin").write_bytes(shape_meta_data)
        numeric_files: list[list[str]] = []
        for shape_index, columns in enumerate(numeric_data):
            column_files: list[str] = []
            for column_index, data in enumerate(columns):
                filename = f"{spec.tag}.shapemix_s{shape_index}_c{column_index}.delta.bin"
                (output_dir / filename).write_bytes(data)
                column_files.append(filename)
            numeric_files.append(column_files)
        layout_files: list[str] = []
        for shape_index, data in enumerate(layout_data):
            filename = f"{spec.tag}.shapemix_s{shape_index}.layout.bin"
            (output_dir / filename).write_bytes(data)
            layout_files.append(filename)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "shape_id_file": f"{spec.tag}.shapemix_ids.bin",
            "shape_meta_file": f"{spec.tag}.shapemix_meta.bin",
            "numeric_files": numeric_files,
            "layout_files": layout_files,
            "shape_count": shape_count,
        }

    if spec.kind == "uint16_split_width":
        high_bytes = bytearray()
        low_bytes = bytearray()
        widths = bytearray()
        for value in values:
            encoded_value, width = parse_int_width(value)
            if encoded_value < 0 or encoded_value > 65535:
                raise ValueError(f"Value out of uint16 range for tag {spec.tag}: {value!r}")
            high_bytes.append(encoded_value >> 8)
            low_bytes.append(encoded_value & 0xFF)
            widths.append(width)
        (output_dir / f"{spec.tag}.u16split.bin").write_bytes(bytes(low_bytes + high_bytes))
        (output_dir / f"{spec.tag}.width.bin").write_bytes(bytes(widths))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.u16split.bin",
            "width_file": f"{spec.tag}.width.bin",
        }

    if spec.kind == "colon_duration_delta":
        encoded_values: list[int] = []
        layouts = bytearray()
        for value in values:
            encoded_value, parts = parse_colon_duration(value)
            encoded_values.append(encoded_value)
            layouts.append(parts)
        (output_dir / f"{spec.tag}.delta.bin").write_bytes(encode_delta_values(encoded_values))
        (output_dir / f"{spec.tag}.layout.bin").write_bytes(bytes(layouts))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.delta.bin",
            "layout_file": f"{spec.tag}.layout.bin",
        }

    if spec.kind == "open_context_delta":
        program = open_function_program(spec)
        decoder_program = dict(program)
        decoder_program.pop("context_code", None)
        raw_values, context_values = parse_context_pairs(values)
        numeric_values = [parse_open_function_value(value, program) for value in raw_values]
        context_ranks, context_literals = encode_string_mtf_rank(context_values)
        delta_data = encode_context_grouped_delta_values(numeric_values, context_values)
        base.write_varint_stream(output_dir / f"{spec.tag}.ctx_rank.bin", context_ranks)
        base.write_string_stream(output_dir / f"{spec.tag}.ctx_literal.bin", context_literals)
        (output_dir / f"{spec.tag}.ctx_delta.bin").write_bytes(delta_data)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "context_rank_file": f"{spec.tag}.ctx_rank.bin",
            "context_literal_file": f"{spec.tag}.ctx_literal.bin",
            "delta_file": f"{spec.tag}.ctx_delta.bin",
            "delta_zigzag": True,
            "program": json.dumps(decoder_program, sort_keys=True),
        }

    if spec.kind == "open_context_dict":
        program = open_function_program(spec)
        decoder_program = dict(program)
        decoder_program.pop("context_code", None)
        raw_values, context_values = parse_context_pairs(values)
        rendered_values = [render_open_function_exact(value, program) for value in raw_values]
        context_ranks, context_literals = encode_string_mtf_rank(context_values)
        value_ranks, value_literals = encode_context_grouped_string_mtf(rendered_values, context_values)
        base.write_varint_stream(output_dir / f"{spec.tag}.ctx_rank.bin", context_ranks)
        base.write_string_stream(output_dir / f"{spec.tag}.ctx_literal.bin", context_literals)
        (output_dir / f"{spec.tag}.ctx_value_rank.bin").write_bytes(value_ranks)
        base.write_string_stream(output_dir / f"{spec.tag}.ctx_value_literal.bin", value_literals)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "context_rank_file": f"{spec.tag}.ctx_rank.bin",
            "context_literal_file": f"{spec.tag}.ctx_literal.bin",
            "value_rank_file": f"{spec.tag}.ctx_value_rank.bin",
            "value_literal_file": f"{spec.tag}.ctx_value_literal.bin",
            "program": json.dumps(decoder_program, sort_keys=True),
        }

    if spec.kind == "open_function":
        program = open_function_program(spec)
        op = str(program["op"])
        layouts = bytearray()
        python_exec_layout_arity = 0
        encoded_values: list[int] = []
        fraction_scale: int | None = None
        fixed_fraction_width: int | None = None
        if op == "datetime_strptime_delta" and "%f" in str(program.get("format", "")):
            widths = [datetime_fraction_width(value, program) for value in values]
            fraction_scale = max(widths) if widths else 6
            if widths and len(set(widths)) == 1:
                fixed_fraction_width = widths[0]
        if op == "python_exec":
            stored_values: list[object] = []
            layout_rows: list[list[int]] = []
            for value in values:
                record = run_python_exec_forward(value, program)
                stored_values.append(record["stored"][0])
                layout_rows.append([int(item) for item in record.get("layout", [])])
            layout_arities = {len(row) for row in layout_rows}
            if len(layout_arities) > 1:
                raise ValueError(f"Variable python_exec layout arity for tag {spec.tag}")
            python_exec_layout_arity = next(iter(layout_arities), 0)
            if python_exec_layout_arity:
                for row in layout_rows:
                    layouts.extend(row)
            if stored_values and all(isinstance(item, str) for item in stored_values):
                stored_strings = [str(item) for item in stored_values]
                if can_encode_ipv4_plain(stored_strings):
                    string_codec = "ipv4_plain"
                elif (
                    os.environ.get("PARE_NO_BENEFIT_FIXED_CODEC", "0") == "1"
                    or os.environ.get("PARE_CLASS_FIXED_CODEC", "0") == "1"
                    or os.environ.get("PARE_STRICT_SIMPLE_CODEC", "0") == "1"
                    or os.environ.get("PARE_TIMESTAMP_ONLY_DELTA", "0") == "1"
                ):
                    string_codec = "string_mtf_rank"
                else:
                    try:
                        string_codec, _string_cost = min(stream_codec_candidates(stored_strings), key=lambda item: item[1])
                    except Exception:
                        string_codec = "string"
                inner_meta = write_tag_stream(
                    output_dir,
                    ExtractSpec(
                        tag=spec.tag,
                        pattern=spec.pattern,
                        kind=string_codec,
                        store_group=spec.store_group,
                        replacement=spec.replacement,
                    ),
                    stored_strings,
                )
                if layouts:
                    (output_dir / f"{spec.tag}.oflayout.bin").write_bytes(bytes(layouts))
                return {
                    "tag": spec.tag,
                    "kind": spec.kind,
                    "count": len(values),
                    "python_exec_value_type": "str",
                    "python_exec_string_codec": inner_meta,
                    **({"layout_file": f"{spec.tag}.oflayout.bin", "layout_arity": python_exec_layout_arity} if layouts else {}),
                    "program": json.dumps(program, sort_keys=True),
                }
            encoded_values = [int(item) for item in stored_values]
        for value in values:
            if op == "python_exec":
                break
            if op == "syslog_delta":
                encoded_value, layout = parse_syslog_timestamp(value)
                encoded_values.append(encoded_value)
                layouts.append(layout)
            elif op == "asctime_year_delta":
                encoded_value, layout = parse_asctime_year_timestamp_with_layout(value)
                encoded_values.append(encoded_value)
                layouts.append(layout)
            elif op == "day_hms_delta":
                day, width, seconds = parse_day_hms(value)
                encoded_values.append(day * 24 * 3600 + seconds)
                layouts.append(width)
            elif fraction_scale is not None:
                encoded_values.append(parse_open_function_value(value, program, fraction_scale=fraction_scale))
                if fixed_fraction_width is None:
                    layouts.append(datetime_fraction_width(value, program))
            else:
                encoded_values.append(parse_open_function_value(value, program))
        timestamp_function = spec.tag.upper().startswith("TS") or is_timestamp_open_function(spec.tag, program)
        if (
            os.environ.get("PARE_TIMESTAMP_ONLY_DELTA", "0") == "1"
            and not spec.tag.upper().startswith("TS")
        ):
            placeholder_values = render_open_function_placeholder_values(values, program)
            return write_tag_stream(
                output_dir,
                ExtractSpec(
                    tag=spec.tag,
                    pattern=spec.pattern,
                    kind="string_mtf_rank",
                    store_group=spec.store_group,
                    replacement=spec.replacement,
                ),
                placeholder_values,
            )
        if os.environ.get("PARE_FUNCTION_SAME_WIDTH_DELTA", "0") == "1" and not timestamp_function:
            signatures = {digit_width_signature(value) for value in values}
            pure_numeric_delta = (
                os.environ.get("PARE_FUNCTION_PURE_NUMERIC_DELTA", "0") == "1"
                and values
                and all(re.fullmatch(r"\d+", value) for value in values)
            )
            if (
                not pure_numeric_delta
                and (not signatures or len(signatures) != 1 or next(iter(signatures)) == ())
            ):
                placeholder_values = render_open_function_placeholder_values(values, program)
                return write_tag_stream(
                    output_dir,
                    ExtractSpec(
                        tag=spec.tag,
                        pattern=spec.pattern,
                        kind="string_mtf_rank",
                        store_group=spec.store_group,
                        replacement=spec.replacement,
                    ),
                    placeholder_values,
                )
        value_scale = 1
        if op == "python_exec" and encoded_values:
            common_factor = 0
            for encoded_value in encoded_values:
                common_factor = math.gcd(common_factor, abs(int(encoded_value)))
            if common_factor > 1:
                encoded_values = [int(encoded_value) // common_factor for encoded_value in encoded_values]
                value_scale = int(common_factor)
        if not can_decode_delta_stream(encoded_values):
            # Some LLM-generated integer programs are reversible but produce
            # values/deltas beyond the decoder's portable varint envelope. Keep
            # the extraction lossless by falling back to an ordinary string
            # stream instead of emitting an undecodable numeric stream.
            placeholder_values = render_open_function_placeholder_values(values, program)
            base.write_string_stream(output_dir / f"{spec.tag}.strings.bin", placeholder_values)
            return {
                "tag": spec.tag,
                "kind": "string",
                "count": len(values),
                "file": f"{spec.tag}.strings.bin",
                "fallback_from": "open_function_delta_overflow",
            }
        delta_data = encode_delta_values(encoded_values)
        if (
            os.environ.get("PARE_NO_BENEFIT_NUMERIC_ABS", "0") == "1"
            and not timestamp_function
            and can_decode_varint_stream([zigzag_encode(int(value)) for value in encoded_values])
        ):
            zigzag_data = bytearray()
            for encoded_value in encoded_values:
                zigzag_data.extend(base.encode_varint(zigzag_encode(int(encoded_value))))
            (output_dir / f"{spec.tag}.open_zigzag_abs.bin").write_bytes(bytes(zigzag_data))
            if layouts:
                (output_dir / f"{spec.tag}.oflayout.bin").write_bytes(bytes(layouts))
            return {
                "tag": spec.tag,
                "kind": spec.kind,
                "count": len(values),
                "open_numeric_codec": "zigzag_abs",
                "file": f"{spec.tag}.open_zigzag_abs.bin",
                **({"layout_file": f"{spec.tag}.oflayout.bin"} if layouts else {}),
                **({"layout_arity": python_exec_layout_arity} if op == "python_exec" and python_exec_layout_arity else {}),
                **({"fraction_scale": fraction_scale} if fraction_scale is not None else {}),
                **({"fraction_width": fixed_fraction_width} if fixed_fraction_width is not None else {}),
                **({"value_scale": value_scale} if value_scale != 1 else {}),
                "program": json.dumps(program, sort_keys=True),
            }
        if (
            os.environ.get("PARE_NO_BENEFIT_FIXED_CODEC", "0") != "1"
            and os.environ.get("PARE_FUNCTION_UNIQUE_RATIO_CODEC", "0") == "1"
            and encoded_values
            and not timestamp_function
        ):
            if has_oversized_open_numeric_values(values, encoded_values):
                placeholder_values = render_open_function_placeholder_values(values, program)
                return write_tag_stream(
                    output_dir,
                    ExtractSpec(
                        tag=spec.tag,
                        pattern=spec.pattern,
                        kind="string_mtf_rank",
                        store_group=spec.store_group,
                        replacement=spec.replacement,
                    ),
                    placeholder_values,
                )
            unique_count = len(set(encoded_values))
            if unique_count * 10 > len(encoded_values):
                zigzag_data = bytearray()
                for encoded_value in encoded_values:
                    zigzag_data.extend(base.encode_varint(zigzag_encode(int(encoded_value))))
                if (
                    os.environ.get("PARE_FUNCTION_UNIQUE_RATIO_DELTA_GUARD", "0") == "1"
                    and len(delta_data) <= len(zigzag_data)
                ):
                    pass
                else:
                    (output_dir / f"{spec.tag}.open_zigzag_abs.bin").write_bytes(bytes(zigzag_data))
                    if layouts:
                        (output_dir / f"{spec.tag}.oflayout.bin").write_bytes(bytes(layouts))
                    return {
                        "tag": spec.tag,
                        "kind": spec.kind,
                        "count": len(values),
                        "open_numeric_codec": "zigzag_abs",
                        "file": f"{spec.tag}.open_zigzag_abs.bin",
                        **({"layout_file": f"{spec.tag}.oflayout.bin"} if layouts else {}),
                        **({"layout_arity": python_exec_layout_arity} if op == "python_exec" and python_exec_layout_arity else {}),
                        **({"fraction_scale": fraction_scale} if fraction_scale is not None else {}),
                        **({"fraction_width": fixed_fraction_width} if fixed_fraction_width is not None else {}),
                        **({"value_scale": value_scale} if value_scale != 1 else {}),
                        "program": json.dumps(program, sort_keys=True),
                    }
        if op == "syslog_delta" and layouts and len(set(layouts)) == 1:
            # A single syslog layout is common in fixed-width logs. Store the
            # layout once in metadata instead of writing one byte per value.
            program = dict(program)
            program["layout"] = int(layouts[0])
            layouts = bytearray()
        if (
            os.environ.get("PARE_NO_BENEFIT_FIXED_CODEC", "0") != "1"
            and os.environ.get("PARE_CLASS_FIXED_CODEC", "0") != "1"
            and os.environ.get("PARE_STRICT_SIMPLE_CODEC", "0") != "1"
            and not layouts
            and values
            and all(re.fullmatch(r"\d+", value) for value in values)
        ):
            delta_cost = compressed_parts_cost([delta_data])
            best_codec = "delta"
            best_cost = delta_cost
            best_payload: dict[str, object] = {}
            cached_numeric_codec = cached_open_numeric_codec(spec.tag)
            if cached_numeric_codec != "delta":
                try:
                    denum_ids, denum_meta, denum_delta = encode_denum_bucket_delta(values)
                    denum_cost = compressed_parts_cost([denum_ids, denum_meta, denum_delta])
                    if denum_cost < best_cost:
                        best_codec = "int_denum_bucket_delta"
                        best_cost = denum_cost
                        best_payload = {
                            "ids": denum_ids,
                            "meta": denum_meta,
                            "delta": denum_delta,
                        }
                except Exception:
                    pass
            if cached_numeric_codec not in {"delta", "int_denum_bucket_delta"}:
                try:
                    period, quotient_data, remainder_data, width_data, remainder_mode, remainder_literals = encode_modular_delta_width(values)
                    modular_parts = [quotient_data, remainder_data, width_data]
                    if remainder_mode == "mtf_rank":
                        modular_parts.append(encode_string_stream_bytes(remainder_literals))
                    modular_cost = compressed_parts_cost(modular_parts)
                    if modular_cost < best_cost:
                        best_codec = "modular_delta_width"
                        best_cost = modular_cost
                        best_payload = {
                            "period": period,
                            "quotient": quotient_data,
                            "remainder": remainder_data,
                            "widths": width_data,
                            "remainder_mode": remainder_mode,
                            "remainder_literals": remainder_literals,
                        }
                except Exception:
                    pass
            if best_cost + CONDITIONAL_CODEC_MODEL_COST < delta_cost:
                if best_codec == "int_denum_bucket_delta":
                    denum_ids = bytes(best_payload["ids"])
                    denum_meta = bytes(best_payload["meta"])
                    denum_delta = bytes(best_payload["delta"])
                    (output_dir / f"{spec.tag}.open_denum_ids.bin").write_bytes(denum_ids)
                    (output_dir / f"{spec.tag}.open_denum_meta.bin").write_bytes(denum_meta)
                    (output_dir / f"{spec.tag}.open_denum_delta.bin").write_bytes(denum_delta)
                    return {
                        "tag": spec.tag,
                        "kind": spec.kind,
                        "count": len(values),
                        "open_numeric_codec": "int_denum_bucket_delta",
                        "id_file": f"{spec.tag}.open_denum_ids.bin",
                        "meta_file": f"{spec.tag}.open_denum_meta.bin",
                        "delta_file": f"{spec.tag}.open_denum_delta.bin",
                        "program": json.dumps(program, sort_keys=True),
                    }
                if best_codec == "modular_delta_width":
                    quotient_data = bytes(best_payload["quotient"])
                    remainder_data = bytes(best_payload["remainder"])
                    width_data = bytes(best_payload["widths"])
                    remainder_mode = str(best_payload["remainder_mode"])
                    remainder_literals = [str(value) for value in best_payload.get("remainder_literals", [])]
                    (output_dir / f"{spec.tag}.open_modq_delta.bin").write_bytes(quotient_data)
                    (output_dir / f"{spec.tag}.open_modr.bin").write_bytes(remainder_data)
                    (output_dir / f"{spec.tag}.open_mod_width.bin").write_bytes(width_data)
                    literal_file = ""
                    if remainder_mode == "mtf_rank":
                        literal_file = f"{spec.tag}.open_mod_literal.bin"
                        base.write_string_stream(output_dir / literal_file, remainder_literals)
                    return {
                        "tag": spec.tag,
                        "kind": spec.kind,
                        "count": len(values),
                        "open_numeric_codec": "modular_delta_width",
                        "quotient_file": f"{spec.tag}.open_modq_delta.bin",
                        "remainder_file": f"{spec.tag}.open_modr.bin",
                        "width_file": f"{spec.tag}.open_mod_width.bin",
                        "period": int(best_payload["period"]),
                        "remainder_mode": remainder_mode,
                        **({"literal_file": literal_file} if literal_file else {}),
                        "program": json.dumps(program, sort_keys=True),
                    }
        delta_path = output_dir / f"{spec.tag}.ofdelta.bin"
        if not cpp_write_stream("delta", [str(value) for value in encoded_values], [delta_path]):
            delta_path.write_bytes(delta_data)
        if layouts:
            (output_dir / f"{spec.tag}.oflayout.bin").write_bytes(bytes(layouts))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "file": f"{spec.tag}.ofdelta.bin",
            **({"layout_file": f"{spec.tag}.oflayout.bin"} if layouts else {}),
            **({"layout_arity": python_exec_layout_arity} if op == "python_exec" and python_exec_layout_arity else {}),
            **({"fraction_scale": fraction_scale} if fraction_scale is not None else {}),
            **({"fraction_width": fixed_fraction_width} if fixed_fraction_width is not None else {}),
            **({"value_scale": value_scale} if value_scale != 1 else {}),
            "program": json.dumps(program, sort_keys=True),
        }

    if spec.kind == "relation_pair_delta":
        relation = json.loads(spec.context_tag or "{}")
        literal = str(relation["literal"])
        left_values: list[int] = []
        right_values: list[int] = []
        left_widths = bytearray()
        right_widths = bytearray()
        for value in values:
            left_text, right_text = value.split(literal, 1)
            left_value, left_width = parse_int_width(left_text)
            right_value, right_width = parse_int_width(right_text)
            left_values.append(left_value)
            right_values.append(right_value)
            left_widths.append(left_width)
            right_widths.append(right_width)
        (output_dir / f"{spec.tag}.left.delta.bin").write_bytes(encode_delta_values(left_values))
        (output_dir / f"{spec.tag}.right.delta.bin").write_bytes(encode_delta_values(right_values))
        (output_dir / f"{spec.tag}.left.width.bin").write_bytes(bytes(left_widths))
        (output_dir / f"{spec.tag}.right.width.bin").write_bytes(bytes(right_widths))
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values),
            "left_file": f"{spec.tag}.left.delta.bin",
            "right_file": f"{spec.tag}.right.delta.bin",
            "left_width_file": f"{spec.tag}.left.width.bin",
            "right_width_file": f"{spec.tag}.right.width.bin",
            "literal": literal,
        }

    if spec.kind == "port_ip_delta_width":
        raise ValueError("port_ip_delta_width requires transformed-text context")

    raise ValueError(f"Unsupported extraction kind {spec.kind!r}")


def write_context_tag_stream(
    output_dir: Path,
    spec: ExtractSpec,
    values_by_tag: dict[str, list[str]],
    placeholders_by_tag: dict[str, str],
    transformed_text: str,
) -> dict[str, object]:
    if spec.kind not in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width", "size_from_prev_bytes"}:
        raise ValueError(f"Unsupported context extraction kind {spec.kind!r}")
    if spec.context_tag is None:
        raise ValueError(f"Context tag is required for {spec.kind}")
    if spec.kind == "size_from_prev_bytes":
        flags, exceptions = encode_size_from_prev_bytes(
            transformed_text=transformed_text,
            size_values=values_by_tag[spec.tag],
            byte_values=values_by_tag[spec.context_tag],
            size_placeholder=placeholders_by_tag[spec.tag],
            byte_placeholder=placeholders_by_tag[spec.context_tag],
        )
        (output_dir / f"{spec.tag}.sizeflag.bin").write_bytes(flags)
        base.write_string_stream(output_dir / f"{spec.tag}.sizeliteral.bin", exceptions)
        return {
            "tag": spec.tag,
            "kind": spec.kind,
            "count": len(values_by_tag[spec.tag]),
            "flag_file": f"{spec.tag}.sizeflag.bin",
            "literal_file": f"{spec.tag}.sizeliteral.bin",
            "context_tag": spec.context_tag,
            "exceptions": len(exceptions),
        }
    if spec.kind == "port_ip_grouped_circular_delta_width":
        data, widths = encode_port_ip_grouped_circular_delta_width(
            transformed_text=transformed_text,
            port_values=values_by_tag[spec.tag],
            ip_values=values_by_tag[spec.context_tag],
            port_placeholder=placeholders_by_tag[spec.tag],
            ip_placeholder=placeholders_by_tag[spec.context_tag],
        )
    else:
        data, widths = encode_port_ip_delta_width(
            transformed_text=transformed_text,
            port_values=values_by_tag[spec.tag],
            ip_values=values_by_tag[spec.context_tag],
            port_placeholder=placeholders_by_tag[spec.tag],
            ip_placeholder=placeholders_by_tag[spec.context_tag],
        )
    (output_dir / f"{spec.tag}.ipdelta.bin").write_bytes(data)
    (output_dir / f"{spec.tag}.width.bin").write_bytes(widths)
    return {
        "tag": spec.tag,
        "kind": spec.kind,
        "count": len(values_by_tag[spec.tag]),
        "file": f"{spec.tag}.ipdelta.bin",
        "width_file": f"{spec.tag}.width.bin",
        "context_tag": spec.context_tag,
    }


def routed_split_context_key(line: str, start: int, end: int, occurrence: int, width: int) -> tuple[str, str, int]:
    left = line[max(0, start - width):start]
    right = line[end:end + width]
    left = re.sub(r"<[^>]+>", "<P>", left)
    right = re.sub(r"<[^>]+>", "<P>", right)
    left = re.sub(r"\d+", "D", left)
    right = re.sub(r"\d+", "D", right)
    return (left[-width:], right[:width], occurrence)


def _best_stream_spec_for_values(tag: str, values: list[str]) -> ExtractSpec:
    try:
        kind, _cost = min(stream_codec_candidates(values), key=lambda item: item[1])
    except Exception:
        kind = "string"
    return ExtractSpec(tag=tag, pattern="", kind=kind)


def write_routed_split_stream(
    output_dir: Path,
    spec: ExtractSpec,
    values: list[str],
    placeholder: str,
    transformed_text: str,
) -> dict[str, object]:
    route_plan = json.loads(spec.context_tag or "{}")
    if route_plan.get("mode") != "local_context":
        raise ValueError("Only local_context routed split is supported")
    width = int(route_plan.get("context_width", 24))
    routes = list(route_plan.get("routes", []))
    route_by_key: dict[tuple[str, str, int], dict[str, object]] = {
        (str(route["key"][0]), str(route["key"][1]), int(route["key"][2])): route
        for route in routes
    }
    child_values: dict[str, list[str]] = defaultdict(list)
    fallback_values: list[str] = []
    value_index = 0
    for line in split_physical_lines(transformed_text):
        cursor = 0
        occurrence = 0
        while True:
            pos = line.find(placeholder, cursor)
            if pos < 0:
                break
            if value_index >= len(values):
                raise ValueError(f"Routed split exhausted values for {spec.tag}")
            value = values[value_index]
            key = routed_split_context_key(line, pos, pos + len(placeholder), occurrence, width)
            route = route_by_key.get(key)
            if route is None:
                fallback_values.append(value)
            elif route.get("kind") == "literal":
                literal = str(route.get("literal", ""))
                if value != literal:
                    raise ValueError(f"Routed split literal mismatch for {spec.tag}: {value!r} != {literal!r}")
            elif route.get("kind") == "child":
                child_values[str(route["child"])].append(value)
            else:
                raise ValueError(f"Bad routed split route for {spec.tag}: {route!r}")
            value_index += 1
            occurrence += 1
            cursor = pos + len(placeholder)
    if value_index != len(values):
        raise ValueError(f"Unused routed split values for {spec.tag}: {len(values) - value_index}")

    child_meta: list[dict[str, object]] = []
    for child_id in sorted(child_values, key=lambda item: int(item) if item.isdigit() else item):
        vals = child_values[child_id]
        child_tag = f"{spec.tag}R{child_id}"
        entry = write_tag_stream(output_dir, _best_stream_spec_for_values(child_tag, vals), vals)
        entry["child"] = child_id
        child_meta.append(entry)

    fallback_meta: dict[str, object] | None = None
    if fallback_values:
        fallback_tag = f"{spec.tag}RF"
        fallback_meta = write_tag_stream(
            output_dir,
            _best_stream_spec_for_values(fallback_tag, fallback_values),
            fallback_values,
        )

    return {
        "tag": spec.tag,
        "kind": "routed_split",
        "count": len(values),
        "placeholder": placeholder,
        "mode": "local_context",
        "context_width": width,
        "routes": routes,
        "children": child_meta,
        **({"fallback": fallback_meta} if fallback_meta is not None else {}),
    }


def decode_routed_split_values(transformed_text: str, input_dir: Path, tag_meta: dict[str, object]) -> list[str]:
    placeholder = str(tag_meta["placeholder"])
    width = int(tag_meta.get("context_width", 24))
    routes = list(tag_meta.get("routes", []))
    route_by_key: dict[tuple[str, str, int], dict[str, object]] = {
        (str(route["key"][0]), str(route["key"][1]), int(route["key"][2])): route
        for route in routes
    }
    child_values: dict[str, list[str]] = {}
    child_indexes: dict[str, int] = {}
    for child_meta in tag_meta.get("children", []):
        child_id = str(child_meta["child"])
        child_values[child_id] = read_tag_stream(input_dir, child_meta)
        child_indexes[child_id] = 0
    fallback_meta = tag_meta.get("fallback")
    fallback_values = read_tag_stream(input_dir, fallback_meta) if isinstance(fallback_meta, dict) else []
    fallback_index = 0
    values: list[str] = []
    for line in split_physical_lines(transformed_text):
        cursor = 0
        occurrence = 0
        while True:
            pos = line.find(placeholder, cursor)
            if pos < 0:
                break
            key = routed_split_context_key(line, pos, pos + len(placeholder), occurrence, width)
            route = route_by_key.get(key)
            if route is None:
                if fallback_index >= len(fallback_values):
                    raise ValueError(f"Routed split fallback exhausted for {tag_meta['tag']}")
                values.append(fallback_values[fallback_index])
                fallback_index += 1
            elif route.get("kind") == "literal":
                values.append(str(route.get("literal", "")))
            elif route.get("kind") == "child":
                child_id = str(route["child"])
                index = child_indexes[child_id]
                vals = child_values[child_id]
                if index >= len(vals):
                    raise ValueError(f"Routed split child {child_id} exhausted for {tag_meta['tag']}")
                values.append(vals[index])
                child_indexes[child_id] = index + 1
            else:
                raise ValueError(f"Bad routed split route for {tag_meta['tag']}: {route!r}")
            occurrence += 1
            cursor = pos + len(placeholder)
    if fallback_index != len(fallback_values):
        raise ValueError(f"Unused routed split fallback values for {tag_meta['tag']}")
    for child_id, vals in child_values.items():
        if child_indexes[child_id] != len(vals):
            raise ValueError(f"Unused routed split child values for {tag_meta['tag']} child={child_id}")
    if len(values) != int(tag_meta["count"]):
        raise ValueError(f"Bad routed split value count for {tag_meta['tag']}: {len(values)}")
    return values


def read_tag_stream(input_dir: Path, tag_meta: dict[str, object]) -> list[str]:
    kind = str(tag_meta["kind"])
    tag = str(tag_meta["tag"])
    expected_count = int(tag_meta["count"]) if "count" in tag_meta else None
    count = expected_count if expected_count is not None else -1

    def file_name(key: str, default_suffix: str) -> str:
        return str(tag_meta.get(key, f"{tag}.{default_suffix}"))

    if kind == "merged_string_mtf_cdelta_ref":
        ranks = decode_delta_values((input_dir / str(tag_meta["rank_file"])).read_bytes())
        literals = base.read_string_stream(input_dir / str(tag_meta["literal_file"]))
        merged_values = decode_string_mtf_cdelta(
            ranks,
            literals,
            table_size=int(tag_meta.get("table_size", 512)),
        )
        offset = int(tag_meta["offset"])
        values = merged_values[offset:offset + count]
        if len(values) != count:
            raise ValueError(f"Bad merged stream slice for tag {tag}")
    elif kind == "string":
        values = base.read_string_stream(input_dir / file_name("file", "strings.bin"))
    elif kind == "string_mtf_rank":
        ranks = base.read_varint_stream(input_dir / file_name("rank_file", "rank.bin"))
        literals = base.read_string_stream(input_dir / file_name("literal_file", "literal.bin"))
        values = decode_string_mtf_rank(ranks, literals, table_size=int(tag_meta.get("table_size", 512)))
    elif kind == "string_mtf_cdelta":
        ranks = decode_delta_values((input_dir / file_name("rank_file", "cdelta.bin")).read_bytes())
        literals = base.read_string_stream(input_dir / file_name("literal_file", "literal.bin"))
        values = decode_string_mtf_cdelta(ranks, literals, table_size=int(tag_meta.get("table_size", 512)))
    elif kind == "byte_phrase_delta":
        values = decode_byte_phrase_values(
            (input_dir / str(tag_meta["file"])).read_bytes(),
            (input_dir / str(tag_meta["flag_file"])).read_bytes(),
            base.read_string_stream(input_dir / str(tag_meta["exception_file"])),
            count,
        )
    elif kind == "hex_int_delta_width":
        values = decode_hex_int_width(
            (input_dir / str(tag_meta["file"])).read_bytes(),
            (input_dir / str(tag_meta["width_file"])).read_bytes(),
            (input_dir / str(tag_meta["case_file"])).read_bytes(),
            count,
        )
    elif kind == "hex_pair_delta":
        values = decode_hex_pair_delta(
            (input_dir / str(tag_meta["left_file"])).read_bytes(),
            (input_dir / str(tag_meta["right_file"])).read_bytes(),
            (input_dir / str(tag_meta["width_file"])).read_bytes(),
            (input_dir / str(tag_meta["case_file"])).read_bytes(),
            count,
        )
    elif kind == "ipv4_width":
        values = decode_ipv4_width((input_dir / file_name("file", "ipv4w.bin")).read_bytes())
    elif kind == "ipv4_plain":
        values = decode_ipv4_plain((input_dir / file_name("file", "ipv4.bin")).read_bytes())
    elif kind == "compound_endpoint_mixed":
        values = decode_compound_endpoint_mixed(
            (input_dir / str(tag_meta["type_file"])).read_bytes(),
            (input_dir / str(tag_meta["ip_prefix_file"])).read_bytes(),
            (input_dir / str(tag_meta["ip_suffix_file"])).read_bytes(),
            base.read_varint_stream(input_dir / str(tag_meta["ip_rank_file"])),
            base.read_string_stream(input_dir / str(tag_meta["ip_literal_file"])),
            (input_dir / str(tag_meta["ip_port_delta_file"])).read_bytes(),
            (input_dir / str(tag_meta["ip_port_width_file"])).read_bytes(),
            (input_dir / str(tag_meta["pair_left_file"])).read_bytes(),
            (input_dir / str(tag_meta["pair_right_file"])).read_bytes(),
            (input_dir / str(tag_meta["pair_width_file"])).read_bytes(),
            base.read_string_stream(input_dir / str(tag_meta["raw_file"])),
            count,
        )
    elif kind == "hms_delta":
        values = [format_hms(value) for value in decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())]
    elif kind == "day_hms_delta":
        numeric_values = decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        widths = list((input_dir / str(tag_meta["width_file"])).read_bytes())
        if len(widths) != len(numeric_values):
            raise ValueError(f"Bad width stream for tag {tag}")
        values = [format_day_hms(value, widths[index]) for index, value in enumerate(numeric_values)]
    elif kind == "month_day_hms_delta":
        values = [
            format_month_day_hms(value)
            for value in decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        ]
    elif kind == "syslog_delta":
        numeric_values = decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        if "layout_file" in tag_meta:
            layouts = list((input_dir / str(tag_meta["layout_file"])).read_bytes())
            if len(layouts) != len(numeric_values):
                raise ValueError(f"Bad layout stream for tag {tag}")
        else:
            layouts = [int(tag_meta.get("layout", 18))] * len(numeric_values)
        values = [format_syslog_timestamp(value, layouts[index]) for index, value in enumerate(numeric_values)]
    elif kind == "asctime_year_delta":
        numeric_values = decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        if "layout_file" in tag_meta:
            layouts = list((input_dir / str(tag_meta["layout_file"])).read_bytes())
            if len(layouts) != len(numeric_values):
                raise ValueError(f"Bad asctime layout stream for tag {tag}")
            values = [
                format_asctime_year_timestamp_with_layout(value, layouts[index])
                for index, value in enumerate(numeric_values)
            ]
        else:
            values = [
                format_asctime_year_timestamp(value)
                for value in numeric_values
            ]
    elif kind == "int_delta_width":
        numeric_values = decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        widths = list((input_dir / str(tag_meta["width_file"])).read_bytes())
        if len(widths) != len(numeric_values):
            raise ValueError(f"Bad width stream for tag {tag}")
        values = [format_int_width(value, widths[index]) for index, value in enumerate(numeric_values)]
    elif kind == "int_delta":
        values = [str(value) for value in decode_delta_values((input_dir / file_name("file", "delta.bin")).read_bytes())]
    elif kind == "int_abs_width":
        numeric_values = base.read_varint_stream(input_dir / str(tag_meta["file"]))
        widths = list((input_dir / str(tag_meta["width_file"])).read_bytes())
        if len(widths) != len(numeric_values):
            raise ValueError(f"Bad width stream for tag {tag}")
        values = [format_int_width(value, widths[index]) for index, value in enumerate(numeric_values)]
    elif kind == "int_abs_fixed_width":
        numeric_values = base.read_varint_stream(input_dir / file_name("file", "abs.bin"))
        width = int(tag_meta["width"])
        values = [format_int_width(value, width) for value in numeric_values]
    elif kind == "int_delta_fixed_width":
        numeric_values = decode_delta_values((input_dir / file_name("file", "delta.bin")).read_bytes())
        width = int(tag_meta["width"])
        values = [format_int_width(value, width) for value in numeric_values]
    elif kind == "int_abs":
        values = [str(value) for value in base.read_varint_stream(input_dir / file_name("file", "abs.bin"))]
    elif kind == "int_denum_bucket_delta":
        values = decode_denum_bucket_delta(
            (input_dir / str(tag_meta["id_file"])).read_bytes(),
            (input_dir / str(tag_meta["meta_file"])).read_bytes(),
            (input_dir / str(tag_meta["delta_file"])).read_bytes(),
            count,
        )
    elif kind == "apache_timestamp_delta":
        values = [
            format_apache_full_timestamp(value)
            for value in decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        ]
    elif kind == "packed_datetime_delta":
        numeric_values = decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        layouts = list((input_dir / str(tag_meta["layout_file"])).read_bytes())
        if len(layouts) != len(numeric_values):
            raise ValueError(f"Bad packed datetime layout stream for tag {tag}")
        fraction_scale = int(tag_meta.get("fraction_scale", 3))
        values = [
            format_packed_datetime(value, layouts[index], fraction_scale)
            for index, value in enumerate(numeric_values)
        ]
    elif kind == "affixed_int_delta":
        values = decode_affixed_int_delta(
            (input_dir / str(tag_meta["file"])).read_bytes(),
            (input_dir / str(tag_meta["width_file"])).read_bytes(),
            str(tag_meta.get("prefix", "")),
            str(tag_meta.get("suffix", "")),
        )
    elif kind == "delimited_int_tuple_delta":
        column_files = list(tag_meta.get("column_files", []))
        values = decode_delimited_int_tuple_delta(
            [(input_dir / str(filename)).read_bytes() for filename in column_files],
            (input_dir / str(tag_meta["width_file"])).read_bytes(),
            str(tag_meta["delimiter"]),
            int(tag_meta["arity"]),
        )
    elif kind == "numeric_skeleton_tuple_delta":
        column_files = list(tag_meta.get("column_files", []))
        literals = [str(value) for value in tag_meta.get("literals", [])]
        values = decode_numeric_skeleton_tuple_delta(
            [(input_dir / str(filename)).read_bytes() for filename in column_files],
            (input_dir / str(tag_meta["layout_file"])).read_bytes(),
            literals,
            int(tag_meta["arity"]),
        )
    elif kind == "mixed_skeleton_delta":
        numeric_files = list(tag_meta.get("numeric_files", []))
        literal_rank_files = list(tag_meta.get("literal_rank_files", []))
        literal_table_files = list(tag_meta.get("literal_table_files", []))
        values = decode_mixed_skeleton_delta(
            [(input_dir / str(filename)).read_bytes() for filename in numeric_files],
            (input_dir / str(tag_meta["layout_file"])).read_bytes(),
            [(input_dir / str(filename)).read_bytes() for filename in literal_rank_files],
            [base.read_string_stream(input_dir / str(filename)) for filename in literal_table_files],
            int(tag_meta["arity"]),
        )
    elif kind == "shape_mixed_skeleton_delta":
        numeric_files_nested = list(tag_meta.get("numeric_files", []))
        layout_files = list(tag_meta.get("layout_files", []))
        values = decode_shape_mixed_skeleton_delta(
            (input_dir / str(tag_meta["shape_id_file"])).read_bytes(),
            (input_dir / str(tag_meta["shape_meta_file"])).read_bytes(),
            [
                [(input_dir / str(filename)).read_bytes() for filename in column_files]
                for column_files in numeric_files_nested
            ],
            [(input_dir / str(filename)).read_bytes() for filename in layout_files],
            count,
        )
    elif kind == "uint16_split_width":
        data = (input_dir / str(tag_meta["file"])).read_bytes()
        if len(data) != count * 2:
            raise ValueError(f"Bad uint16 split stream for tag {tag}")
        widths = list((input_dir / str(tag_meta["width_file"])).read_bytes())
        if len(widths) != count:
            raise ValueError(f"Bad width stream for tag {tag}")
        low_bytes = data[:count]
        high_bytes = data[count:]
        values = [
            format_int_width((high_bytes[index] << 8) | low_bytes[index], widths[index])
            for index in range(count)
        ]
    elif kind == "colon_duration_delta":
        numeric_values = decode_delta_values((input_dir / str(tag_meta["file"])).read_bytes())
        layouts = list((input_dir / str(tag_meta["layout_file"])).read_bytes())
        if len(layouts) != len(numeric_values):
            raise ValueError(f"Bad layout stream for tag {tag}")
        values = [format_colon_duration(value, layouts[index]) for index, value in enumerate(numeric_values)]
    elif kind == "open_context_delta":
        program = open_function_program(tag_meta)
        context_ranks = base.read_varint_stream(input_dir / str(tag_meta["context_rank_file"]))
        context_literals = base.read_string_stream(input_dir / str(tag_meta["context_literal_file"]))
        context_values = decode_string_mtf_rank(context_ranks, context_literals, table_size=512)
        numeric_values = decode_context_grouped_delta_values(
            (input_dir / str(tag_meta["delta_file"])).read_bytes(),
            context_values,
            zigzag_encoded=bool(tag_meta.get("delta_zigzag", False)),
        )
        values = [render_open_function_value(value, program) for value in numeric_values]
    elif kind == "open_context_dict":
        context_ranks = base.read_varint_stream(input_dir / str(tag_meta["context_rank_file"]))
        context_literals = base.read_string_stream(input_dir / str(tag_meta["context_literal_file"]))
        context_values = decode_string_mtf_rank(context_ranks, context_literals, table_size=512)
        values = decode_context_grouped_string_mtf(
            (input_dir / str(tag_meta["value_rank_file"])).read_bytes(),
            base.read_string_stream(input_dir / str(tag_meta["value_literal_file"])),
            context_values,
        )
    elif kind == "open_function":
        if tag_meta.get("python_exec_value_type") == "str":
            program = open_function_program(tag_meta)
            if isinstance(tag_meta.get("python_exec_string_codec"), dict):
                stored_values = read_tag_stream(input_dir, dict(tag_meta["python_exec_string_codec"]))
            else:
                stored_values = base.read_string_stream(input_dir / file_name("file", "pyexec_strings.bin"))
            layout_arity = int(tag_meta.get("layout_arity", 0))
            layout_rows: list[list[int]] = [[] for _ in stored_values]
            if "layout_file" in tag_meta:
                layout_data = list((input_dir / str(tag_meta["layout_file"])).read_bytes())
                if layout_arity <= 0 or len(layout_data) != len(stored_values) * layout_arity:
                    raise ValueError(f"Bad python_exec string layout stream for tag {tag}")
                layout_rows = [
                    layout_data[index * layout_arity:(index + 1) * layout_arity]
                    for index in range(len(stored_values))
                ]
            values = [
                run_python_exec_inverse(stored_value, program, layout=layout_rows[index])
                for index, stored_value in enumerate(stored_values)
            ]
        elif tag_meta.get("open_numeric_codec") == "zigzag_abs":
            program = open_function_program(tag_meta)
            numeric_values = [
                zigzag_decode(value)
                for value in base.read_varint_stream(input_dir / str(tag_meta["file"]))
            ]
            if "value_scale" in tag_meta:
                scale = int(tag_meta["value_scale"])
                numeric_values = [int(value) * scale for value in numeric_values]
            if "layout_file" in tag_meta:
                layouts = list((input_dir / str(tag_meta["layout_file"])).read_bytes())
                op = str(program["op"])
                if op == "python_exec":
                    layout_arity = int(tag_meta.get("layout_arity", 1))
                    if layout_arity <= 0 or len(layouts) != len(numeric_values) * layout_arity:
                        raise ValueError(f"Bad python_exec numeric layout stream for tag {tag}")
                    values = [
                        render_open_function_value(
                            value,
                            program,
                            layout=layouts[index * layout_arity:(index + 1) * layout_arity],
                        )
                        for index, value in enumerate(numeric_values)
                    ]
                elif len(layouts) != len(numeric_values):
                    raise ValueError(f"Bad open-function layout stream for tag {tag}")
                elif op == "syslog_delta":
                    values = [format_syslog_timestamp(value, layouts[index]) for index, value in enumerate(numeric_values)]
                elif op == "asctime_year_delta":
                    values = [
                        format_asctime_year_timestamp_with_layout(value, layouts[index])
                        for index, value in enumerate(numeric_values)
                    ]
                elif op == "day_hms_delta":
                    values = [format_day_hms(value, layouts[index]) for index, value in enumerate(numeric_values)]
                elif op == "datetime_strptime_delta" and "fraction_scale" in tag_meta:
                    scale = int(tag_meta["fraction_scale"])
                    values = [
                        render_open_function_value(
                            value,
                            program,
                            fraction_width=layouts[index],
                            fraction_scale=scale,
                        )
                        for index, value in enumerate(numeric_values)
                    ]
                else:
                    values = [render_open_function_value(value, program) for value in numeric_values]
            elif str(program["op"]) == "datetime_strptime_delta" and "fraction_width" in tag_meta:
                scale = int(tag_meta.get("fraction_scale", tag_meta["fraction_width"]))
                width = int(tag_meta["fraction_width"])
                values = [
                    render_open_function_value(
                        value,
                        program,
                        fraction_width=width,
                        fraction_scale=scale,
                    )
                    for value in numeric_values
                ]
            else:
                values = [render_open_function_value(value, program) for value in numeric_values]
        elif tag_meta.get("open_numeric_codec") == "int_denum_bucket_delta":
            values = decode_denum_bucket_delta(
                (input_dir / str(tag_meta["id_file"])).read_bytes(),
                (input_dir / str(tag_meta["meta_file"])).read_bytes(),
                (input_dir / str(tag_meta["delta_file"])).read_bytes(),
                count,
            )
        elif tag_meta.get("open_numeric_codec") == "modular_delta_width":
            remainder_literals = (
                base.read_string_stream(input_dir / str(tag_meta["literal_file"]))
                if "literal_file" in tag_meta
                else []
            )
            values = decode_modular_delta_width(
                (input_dir / str(tag_meta["quotient_file"])).read_bytes(),
                (input_dir / str(tag_meta["remainder_file"])).read_bytes(),
                (input_dir / str(tag_meta["width_file"])).read_bytes(),
                int(tag_meta["period"]),
                str(tag_meta["remainder_mode"]),
                remainder_literals,
            )
        else:
            program = open_function_program(tag_meta)
            numeric_values = decode_delta_values((input_dir / file_name("file", "ofdelta.bin")).read_bytes())
            if "value_scale" in tag_meta:
                scale = int(tag_meta["value_scale"])
                numeric_values = [int(value) * scale for value in numeric_values]
            if "layout_file" in tag_meta:
                layouts = list((input_dir / str(tag_meta["layout_file"])).read_bytes())
                op = str(program["op"])
                if op == "python_exec":
                    layout_arity = int(tag_meta.get("layout_arity", 1))
                    if layout_arity <= 0 or len(layouts) != len(numeric_values) * layout_arity:
                        raise ValueError(f"Bad python_exec numeric layout stream for tag {tag}")
                    values = [
                        render_open_function_value(
                            value,
                            program,
                            layout=layouts[index * layout_arity:(index + 1) * layout_arity],
                        )
                        for index, value in enumerate(numeric_values)
                    ]
                elif len(layouts) != len(numeric_values):
                    raise ValueError(f"Bad open-function layout stream for tag {tag}")
                elif op == "syslog_delta":
                    values = [format_syslog_timestamp(value, layouts[index]) for index, value in enumerate(numeric_values)]
                elif op == "asctime_year_delta":
                    values = [
                        format_asctime_year_timestamp_with_layout(value, layouts[index])
                        for index, value in enumerate(numeric_values)
                    ]
                elif op == "day_hms_delta":
                    values = [format_day_hms(value, layouts[index]) for index, value in enumerate(numeric_values)]
                elif op == "datetime_strptime_delta" and "fraction_scale" in tag_meta:
                    scale = int(tag_meta["fraction_scale"])
                    values = [
                        render_open_function_value(
                            value,
                            program,
                            fraction_width=layouts[index],
                            fraction_scale=scale,
                        )
                        for index, value in enumerate(numeric_values)
                    ]
                else:
                    values = [render_open_function_value(value, program) for value in numeric_values]
            elif str(program["op"]) == "datetime_strptime_delta" and "fraction_width" in tag_meta:
                scale = int(tag_meta.get("fraction_scale", tag_meta["fraction_width"]))
                width = int(tag_meta["fraction_width"])
                values = [
                    render_open_function_value(
                        value,
                        program,
                        fraction_width=width,
                        fraction_scale=scale,
                    )
                    for value in numeric_values
                ]
            else:
                values = [render_open_function_value(value, program) for value in numeric_values]
    elif kind == "relation_pair_delta":
        left_values = decode_delta_values((input_dir / str(tag_meta["left_file"])).read_bytes())
        right_values = decode_delta_values((input_dir / str(tag_meta["right_file"])).read_bytes())
        left_widths = list((input_dir / str(tag_meta["left_width_file"])).read_bytes())
        right_widths = list((input_dir / str(tag_meta["right_width_file"])).read_bytes())
        if not (len(left_values) == len(right_values) == len(left_widths) == len(right_widths) == count):
            raise ValueError(f"Bad relation-pair stream for tag {tag}")
        literal = str(tag_meta["literal"])
        values = [
            format_int_width(left_values[index], left_widths[index])
            + literal
            + format_int_width(right_values[index], right_widths[index])
            for index in range(count)
        ]
    elif kind in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width"}:
        raise ValueError(f"{kind} requires stateful restore")
    else:
        raise ValueError(f"Unsupported extraction kind {kind!r}")

    if expected_count is not None and len(values) != expected_count:
        raise ValueError(f"Tag {tag} expected {count} values, decoded {len(values)}")
    return values


def transform_text(text: str, dataset: str, profile: str = "delog") -> tuple[str, dict[str, list[str]], dict[str, object]]:
    if profile in {"auto_cache_lattice_v1", "auto_cache_lattice_v2", "auto_cache_lattice_v3", "auto_cache_lattice_v4", "auto_cache_lattice_v5", "auto_cache_lattice_v6", "auto_cache_lattice_v7"}:
        return transform_text_cache_lattice(text, dataset, profile=profile)
    if profile in {"auto_template_cache_v1", "auto_template_cache_v2"}:
        return transform_text_template_cache(text, dataset, profile=profile)
    if profile in {"auto_line_transducer_v1", "auto_line_transducer_v2", "auto_line_transducer_v3"}:
        return transform_text_line_transducer(text, dataset, profile=profile)
    if profile in AUTO_DISCOVERED_PROFILES:
        profile_specs = discover_auto_extract_specs(text, profile=profile, dataset=dataset)
    else:
        profiles = PROFILE_MAP[profile]
        if dataset not in profiles:
            raise ValueError(f"Unsupported {profile} extraction dataset {dataset!r}")
        profile_specs = profiles[dataset]
    transformed = text
    values_by_tag: dict[str, list[str]] = {}
    placeholders: dict[str, str] = {}
    specs_meta: list[dict[str, object]] = []

    for spec in profile_specs:
        existing_placeholders = list(placeholders.values())
        placeholder = choose_placeholder(text, dataset, spec.tag)
        placeholders[spec.tag] = placeholder
        values: list[str] = []
        regex = re.compile(spec.pattern, flags=re.MULTILINE)
        previous_transformed = transformed

        def replace(match: re.Match[str]) -> str:
            values.append(match.group(spec.store_group))
            if spec.replacement is not None:
                return _safe_replacement_format(spec.replacement, placeholder, match)
            return placeholder

        transformed = regex.sub(replace, transformed)
        if profile in AUTO_DISCOVERED_PROFILES and values and existing_placeholders:
            # A later open function must not rewrite placeholders introduced by
            # earlier functions.  Broad lexical regexes such as
            # \b[A-Za-z0-9_.-]+\b can otherwise match the inside of "<T>",
            # leaving side-stream values with no placeholder at decode time.
            rewrites_existing_placeholder = any(
                transformed.count(existing_placeholder) != previous_transformed.count(existing_placeholder)
                for existing_placeholder in existing_placeholders
            )
            captures_existing_placeholder = any(
                existing_placeholder in value
                for value in values
                for existing_placeholder in existing_placeholders
            )
            if captures_existing_placeholder or rewrites_existing_placeholder:
                transformed = previous_transformed
                placeholders.pop(spec.tag, None)
                continue
        if profile in AUTO_DISCOVERED_PROFILES and not values:
            placeholders.pop(spec.tag, None)
            continue
        values_by_tag[spec.tag] = values
        specs_meta.append({
            "tag": spec.tag,
            "kind": spec.kind,
            "placeholder": placeholder,
            "pattern": spec.pattern,
            "store_group": spec.store_group,
            "replacement": spec.replacement or "",
            "context_tag": spec.context_tag or "",
        })

    metadata = {
        "dataset": dataset,
        "profile": profile,
        "specs": specs_meta,
    }
    return transformed, values_by_tag, metadata


def save_extract_streams(
    output_root: Path,
    metadata: dict[str, object],
    values_by_tag: dict[str, list[str]],
    transformed_text: str,
) -> None:
    extract_dir = output_root / "dataset_extract"
    extract_dir.mkdir()
    stream_meta: list[dict[str, object]] = []
    profile = str(metadata.get("profile", "delog"))
    if profile in AUTO_DISCOVERED_PROFILES:
        specs_by_tag = {spec.tag: spec for spec in specs_from_metadata(metadata)}
    else:
        specs_by_tag = {spec.tag: spec for spec in PROFILE_MAP[profile][str(metadata["dataset"])]}
    placeholders_by_tag = {
        str(spec_meta["tag"]): str(spec_meta["placeholder"])
        for spec_meta in metadata["specs"]
    }
    prior_tags: list[str] = []
    for spec_meta in metadata["specs"]:
        tag = str(spec_meta["tag"])
        spec = specs_by_tag[tag]
        auto_meta: dict[str, object] = {}
        if spec.kind == "auto":
            op_start = time.perf_counter()
            if profile == "auto_atis_v1":
                spec, auto_meta = select_atis_stream_spec(
                    spec,
                    dataset=str(metadata["dataset"]),
                    values_by_tag=values_by_tag,
                    placeholders_by_tag=placeholders_by_tag,
                    transformed_text=transformed_text,
                    candidate_context_tags=prior_tags,
                )
                record_operation_timing("stream_save.select_atis_stream_spec", time.perf_counter() - op_start)
            else:
                spec, auto_meta = select_auto_stream_spec(
                    spec,
                    values_by_tag=values_by_tag,
                    placeholders_by_tag=placeholders_by_tag,
                    transformed_text=transformed_text,
                    candidate_context_tags=prior_tags,
                )
                record_operation_timing("stream_save.select_auto_stream_spec", time.perf_counter() - op_start)
        if spec.kind == "routed_split":
            op_start = time.perf_counter()
            stream_entry = write_routed_split_stream(
                extract_dir,
                spec,
                values_by_tag[tag],
                str(spec_meta["placeholder"]),
                transformed_text,
            )
            record_operation_timing("stream_save.write_routed_split_stream", time.perf_counter() - op_start)
        elif spec.kind in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width", "size_from_prev_bytes"}:
            op_start = time.perf_counter()
            stream_entry = write_context_tag_stream(
                extract_dir,
                spec,
                values_by_tag=values_by_tag,
                placeholders_by_tag=placeholders_by_tag,
                transformed_text=transformed_text,
            )
            record_operation_timing("stream_save.write_context_tag_stream", time.perf_counter() - op_start)
        else:
            op_start = time.perf_counter()
            stream_entry = write_tag_stream(extract_dir, spec, values_by_tag[tag])
            record_operation_timing(f"stream_save.write_tag_stream.{spec.kind}", time.perf_counter() - op_start)
        stream_entry["placeholder"] = str(spec_meta["placeholder"])
        if not metadata.get("compact_stream_metadata", False):
            stream_entry["pattern"] = str(spec_meta["pattern"])
        if spec.context_tag is not None:
            stream_entry["context_tag"] = spec.context_tag
        if profile == "auto_atis_v1":
            # Proposal attribution is useful as a sidecar artifact, but it is
            # not needed to decode. Keeping it out avoids archive-size
            # regressions when ATI-S selects the same physical codec as v2.
            auto_meta = {
                key: value
                for key, value in auto_meta.items()
                if not key.startswith("atis_")
            }
        stream_entry.update(auto_meta)
        stream_meta.append(stream_entry)
        # Only directly enumerable streams may be used as later conditional
        # codec contexts.  Nested grouped-port contexts cannot be reconstructed
        # from placeholder counts during restore.
        if spec.kind not in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width", "size_from_prev_bytes"}:
            prior_tags.append(tag)
    if metadata.get("post_merge_hex_streams", False):
        op_start = time.perf_counter()
        metadata["post_merge_hex_stream_stats"] = merge_hex_like_string_streams(
            extract_dir,
            stream_meta,
            values_by_tag,
        )
        record_operation_timing("stream_save.post_merge_hex_streams", time.perf_counter() - op_start)
    else:
        metadata["post_merge_hex_stream_stats"] = {
            "enabled": False,
            "candidate_tags": 0,
            "merged_tags": 0,
            "merged_values": 0,
            "proxy_gain": 0,
        }
    metadata["streams"] = stream_meta
    # The decoder reconstructs by placeholder order and does not need the
    # original matcher patterns. Keep only compact audit metadata in the
    # archive; the full induced rules are an encoder-side artifact.
    metadata["specs"] = [
        {
            "tag": str(spec_meta["tag"]),
            "kind": str(spec_meta["kind"]),
            "placeholder": str(spec_meta["placeholder"]),
        }
        for spec_meta in metadata["specs"]
    ]


HEX_LIKE_STREAM_RE = re.compile(r"0x[0-9A-Fa-f]+,?")


def merge_hex_like_string_streams(
    extract_dir: Path,
    stream_meta: list[dict[str, object]],
    values_by_tag: dict[str, list[str]],
) -> dict[str, object]:
    """Merge fragmented hexadecimal value streams into one shared MTF stream.

    This is a late, reversible serialization operator.  It does not change the
    transformed log or the placeholder order; each original tag becomes a slice
    into a shared encoded value sequence.  The admission test uses the same
    compressed-byte proxy as other stream-codec choices, and exact restore is
    still checked by the caller after archive creation.
    """

    candidates: list[tuple[dict[str, object], list[str]]] = []
    for entry in stream_meta:
        if str(entry.get("kind")) != "string":
            continue
        tag = str(entry.get("tag"))
        values = values_by_tag.get(tag, [])
        if len(values) < 64:
            continue
        if all(HEX_LIKE_STREAM_RE.fullmatch(value) for value in values):
            candidates.append((entry, values))

    stats: dict[str, object] = {
        "enabled": True,
        "candidate_tags": len(candidates),
        "merged_tags": 0,
        "merged_values": 0,
        "proxy_gain": 0,
    }
    if len(candidates) < 2:
        return stats

    separate_cost = sum(
        compressed_parts_cost([encode_string_stream_bytes(values)])
        for _entry, values in candidates
    )
    merged_values: list[str] = []
    for _entry, values in candidates:
        merged_values.extend(values)
    ranks, literals = encode_string_mtf_cdelta(merged_values)
    rank_payload = encode_delta_values(ranks)
    literal_payload = encode_string_stream_bytes(literals)
    merged_cost = compressed_parts_cost([rank_payload, literal_payload])
    metadata_cost = 64 + 8 * len(candidates)
    proxy_gain = separate_cost - merged_cost - metadata_cost
    stats.update({
        "merged_values": len(merged_values),
        "proxy_gain": proxy_gain,
    })
    if proxy_gain <= 0:
        return stats

    rank_file = "merged_hex_0.cdelta.bin"
    literal_file = "merged_hex_0.literal.bin"
    (extract_dir / rank_file).write_bytes(rank_payload)
    base.write_string_stream(extract_dir / literal_file, literals)

    offset = 0
    for entry, values in candidates:
        old_file = entry.get("file")
        if old_file:
            path = extract_dir / str(old_file)
            if path.exists():
                path.unlink()
        entry.pop("file", None)
        entry["kind"] = "merged_string_mtf_cdelta_ref"
        entry["rank_file"] = rank_file
        entry["literal_file"] = literal_file
        entry["table_size"] = 512
        entry["offset"] = offset
        offset += len(values)

    stats["merged_tags"] = len(candidates)
    return stats


def placeholder_for_tag_meta(tag_meta: dict[str, object]) -> str:
    tag = str(tag_meta["tag"])
    return str(tag_meta.get("placeholder", f"<{tag}>"))


def restore_text(transformed_text: str, extract_dir: Path, metadata: dict[str, object]) -> str:
    tag_by_placeholder: dict[str, str] = {}
    meta_by_tag: dict[str, dict[str, object]] = {}
    normal_values_by_tag: dict[str, list[str]] = {}
    normal_index_by_tag: dict[str, int] = {}
    context_decoders_by_tag: dict[str, PortIpDeltaWidthDecoder | GroupedPortIpCircularDeltaWidthDecoder | SizeFromPrevBytesDecoder] = {}
    last_value_by_tag: dict[str, str] = {}
    placeholders: list[str] = []

    for tag_meta in metadata["streams"]:
        tag = str(tag_meta["tag"])
        placeholder = str(tag_meta.get("placeholder", f"<{tag}>"))
        placeholders.append(placeholder)
        tag_by_placeholder[placeholder] = tag
        meta_by_tag[tag] = tag_meta
        kind = str(tag_meta["kind"])
        count = int(tag_meta.get("count", 0))
        if kind == "routed_split":
            normal_values_by_tag[tag] = decode_routed_split_values(transformed_text, extract_dir, tag_meta)
            normal_index_by_tag[tag] = 0
        elif kind in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width", "size_from_prev_bytes"}:
            if kind == "port_ip_delta_width":
                context_decoders_by_tag[tag] = PortIpDeltaWidthDecoder(
                    (extract_dir / str(tag_meta["file"])).read_bytes(),
                    (extract_dir / str(tag_meta["width_file"])).read_bytes(),
                )
            elif kind == "size_from_prev_bytes":
                context_decoders_by_tag[tag] = SizeFromPrevBytesDecoder(
                    (extract_dir / str(tag_meta["flag_file"])).read_bytes(),
                    base.read_string_stream(extract_dir / str(tag_meta["literal_file"])),
                    count,
                )
        else:
            normal_values_by_tag[tag] = read_tag_stream(extract_dir, tag_meta)
            normal_index_by_tag[tag] = 0

    if not placeholders:
        return transformed_text

    for tag, tag_meta in meta_by_tag.items():
        if str(tag_meta["kind"]) != "port_ip_grouped_circular_delta_width":
            continue
        context_tag = str(tag_meta["context_tag"])
        context_values = normal_values_by_tag.get(context_tag)
        if context_values is None:
            raise ValueError(f"Grouped port context tag {context_tag} must be restored from a normal value stream")
        context_counts = derive_context_counts(
            transformed_text=transformed_text,
            context_values=context_values,
            context_placeholder=str(meta_by_tag[context_tag].get("placeholder", f"<{context_tag}>")),
            target_placeholder=placeholder_for_tag_meta(tag_meta),
        )
        context_decoders_by_tag[tag] = GroupedPortIpCircularDeltaWidthDecoder(
            (extract_dir / str(tag_meta["file"])).read_bytes(),
            (extract_dir / str(tag_meta["width_file"])).read_bytes(),
            context_counts,
        )

    pattern = re.compile("|".join(re.escape(placeholder) for placeholder in sorted(placeholders, key=len, reverse=True)))

    def replace(match: re.Match[str]) -> str:
        placeholder = match.group(0)
        tag = tag_by_placeholder[placeholder]
        tag_meta = meta_by_tag[tag]
        kind = str(tag_meta["kind"])
        if kind in {"port_ip_delta_width", "port_ip_grouped_circular_delta_width", "size_from_prev_bytes"}:
            context_tag = str(tag_meta["context_tag"])
            value = context_decoders_by_tag[tag].next_value(last_value_by_tag.get(context_tag))
        else:
            index = normal_index_by_tag[tag]
            values = normal_values_by_tag[tag]
            if index >= len(values):
                raise ValueError(f"Value stream exhausted for tag {tag}")
            value = values[index]
            normal_index_by_tag[tag] = index + 1
        last_value_by_tag[tag] = value
        return value

    restored = pattern.sub(replace, transformed_text)
    for tag, values in normal_values_by_tag.items():
        if normal_index_by_tag[tag] != len(values):
            raise ValueError(f"Unused values for tag {tag}")
    for decoder in context_decoders_by_tag.values():
        decoder.assert_finished()
    return restored


def create_archive(output_dir: Path, archive_path: Path) -> int:
    if archive_path.exists():
        archive_path.unlink()

    def stable_tarinfo(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
        tarinfo.uid = 0
        tarinfo.gid = 0
        tarinfo.uname = ""
        tarinfo.gname = ""
        tarinfo.mtime = 0
        return tarinfo

    with lzma.open(archive_path, "wb") as compressed_stream:
        with tarfile.open(fileobj=compressed_stream, mode="w") as tar_stream:
            tar_stream.add(output_dir, arcname=output_dir.name, filter=stable_tarinfo)
    return archive_path.stat().st_size


def compress_file(
    input_path: str,
    output_dir: str,
    archive_path: str | None,
    dataset: str,
    core: str,
    numeric_radius: int,
    profile: str = "delog",
    line_dict_min_support: int = 0,
    line_dict_max_entries: int = 8192,
    line_dict_id_mode: str = "inline",
) -> int:
    input_file = Path(input_path)
    output_root = Path(output_dir)
    if output_root.exists():
        shutil.rmtree(output_root)

    original_text = base.read_lossless_text(input_file)
    transformed_text, values_by_tag, extract_metadata = transform_text(original_text, dataset, profile=profile)
    main_text, line_dict_metadata, line_dict_templates, line_dict_side_ids = build_line_dictionary_text(
        transformed_text,
        min_support=line_dict_min_support,
        max_entries=line_dict_max_entries,
        id_mode=line_dict_id_mode,
    )

    if core == "rank":
        encoder = base.RankModelEncoder(numeric_radius=numeric_radius)
    elif core == "slot":
        encoder = slot_proto.TemplateSlotRankModelEncoder(numeric_radius=numeric_radius)
    else:
        raise ValueError(f"Unsupported core {core!r}")

    encoder.encode_text(main_text)
    stats = encoder.save(input_file, output_root, archive_path=None)
    if line_dict_metadata is not None:
        save_line_dictionary(output_root, line_dict_templates, line_dict_side_ids)
    save_extract_streams(output_root, extract_metadata, values_by_tag, transformed_text)

    metadata_path = output_root / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["dataset_extract"] = extract_metadata
    if line_dict_metadata is not None:
        metadata["line_dictionary"] = line_dict_metadata
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

    archive_size = create_archive(output_root, Path(archive_path)) if archive_path else None
    encoded_size = sum(path.stat().st_size for path in output_root.rglob("*") if path.is_file())
    print(f"Original size        : {stats.original_size}")
    print(f"Transformed size     : {len(transformed_text.encode('latin-1'))}")
    print(f"Main-model size      : {len(main_text.encode('latin-1'))}")
    print(f"Encoded directory    : {encoded_size}")
    if archive_size is not None:
        print(f"Archive size         : {archive_size}")
        print(f"Compression ratio    : {stats.original_size / archive_size:.4f}x")
    print(f"Dataset extract      : {dataset}")
    print(f"Extract profile      : {profile}")
    print(f"Core                 : {core}")
    if line_dict_metadata is not None:
        print(
            "Line dictionary     : "
            f"{line_dict_metadata['count']} templates, "
            f"{line_dict_metadata['replaced_lines']} lines"
        )
    for spec in extract_metadata["streams"]:
        print(f"Extract {spec['tag']:<4} {spec['kind']:<20} {spec['count']}")
    return 0


def decompress_file(compressed_dir: str, output_path: str, original_path: str | None = None) -> int:
    compressed_root = Path(compressed_dir)
    metadata = json.loads((compressed_root / "metadata.json").read_text(encoding="utf-8"))
    if metadata.get("context_mode") == "template_slot_v1":
        decoder = slot_proto.TemplateSlotRankModelDecoder(compressed_root)
    else:
        decoder = base.RankModelDecoder(compressed_root)
    transformed_text = decoder.decode_text()
    transformed_text = expand_line_dictionary_text(transformed_text, compressed_root, metadata)
    extract_metadata = metadata["dataset_extract"]
    restored_text = restore_text(transformed_text, compressed_root / "dataset_extract", extract_metadata)
    base.write_lossless_text(output_path, restored_text)
    print(f"Decoded output       : {output_path}")
    if original_path is not None:
        original_text = base.read_lossless_text(original_path)
        exact_match = restored_text == original_text
        print(f"Exact match          : {'PASS' if exact_match else 'FAIL'}")
        return 0 if exact_match else 2
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DeLog-regex extraction wrapper for PARE rank/slot cores")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compress_parser = subparsers.add_parser("compress", help="Compress a log with dataset extraction")
    compress_parser.add_argument("input", help="Input log")
    compress_parser.add_argument("--dataset", choices=sorted(DELOG_PROFILES), required=True)
    compress_parser.add_argument("--core", choices=["rank", "slot"], default="rank")
    compress_parser.add_argument("--profile", choices=sorted(set(PROFILE_MAP) | AUTO_DISCOVERED_PROFILES), default="delog")
    compress_parser.add_argument("--output-dir", default="pare_dataset_extract_out")
    compress_parser.add_argument("--archive", default="pare_dataset_extract.tar.xz")
    compress_parser.add_argument("--numeric-radius", type=int, default=8)
    compress_parser.add_argument("--line-dict-min-support", type=int, default=0)
    compress_parser.add_argument("--line-dict-max-entries", type=int, default=8192)
    compress_parser.add_argument("--line-dict-id-mode", choices=["inline", "side_varint"], default="inline")

    decompress_parser = subparsers.add_parser("decompress", help="Decompress a dataset extraction archive directory")
    decompress_parser.add_argument("compressed_dir", help="Compressed directory")
    decompress_parser.add_argument("--output", default="pare_dataset_extract.decoded")
    decompress_parser.add_argument("--original", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command == "compress":
        archive_path = args.archive if args.archive else None
        return compress_file(
            args.input,
            args.output_dir,
            archive_path,
            args.dataset,
            args.core,
            args.numeric_radius,
            profile=args.profile,
            line_dict_min_support=args.line_dict_min_support,
            line_dict_max_entries=args.line_dict_max_entries,
            line_dict_id_mode=args.line_dict_id_mode,
        )
    return decompress_file(args.compressed_dir, args.output, args.original)


if __name__ == "__main__":
    raise SystemExit(main())
