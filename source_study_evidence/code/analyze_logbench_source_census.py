#!/usr/bin/env python3
"""Source-level census of representation-changing logging statements.

The input is the full LogBench-O source archive. Tree-sitter identifies Java
logging calls, while deliberately conservative rules identify explicit
rendering, conversion, derivation, and repeated-view evidence. Automatic
labels are candidates. A stratified sample is emitted for manual validation.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

from tree_sitter import Language, Node, Parser
import tree_sitter_java


LEVELS = {"trace", "debug", "info", "warn", "warning", "error", "fatal", "log"}
CATEGORIES = (
    "numeric_layout_format",
    "temporal_format",
    "human_readable_format",
    "radix_or_binary_encoding",
    "network_address_format",
    "structured_rendering",
    "explicit_object_rendering",
    "derived_view",
    "repeated_view",
)

NONTRIVIAL_FORMAT_SPEC = re.compile(
    r"%(?:\d+\$)?(?:[-#+ 0,(<]+\d*|\d+|\.\d+|\d+\.\d+)?(?:[tT])?[aAcCeEfFgGxXo](?![A-Za-z])"
)
TEMPORAL_PATTERN = re.compile(
    r"(?i)(?:new\s+SimpleDateFormat\s*\([^;]*?\)\.format\s*\(|"
    r"(?:[A-Za-z0-9_$]*(?:date|time|timestamp)[A-Za-z0-9_$]*(?:format|formatter)[A-Za-z0-9_$]*)\.format\s*\(|"
    r"(?:DateFormatUtils|FastDateFormat|TimeFormat|DateTimeFormatter)[A-Za-z0-9_$.()]*\.format\s*\(|"
    r"\.format\s*\(\s*DateTimeFormatter\.|formatDate\s*\(|formatTime\s*\(|"
    r"formatTimestamp\s*\(|toISOString\s*\(|toIsoString\s*\(|isoformat\s*\()"
)
UNIT_PATTERN = re.compile(
    r"(?i)(?:(?:bytes?ToString|byteDesc|humanReadable(?:ByteCount|Bytes|Size)?|formatBytes|"
    r"formatByteBuf|formatByte|formatSize|prettySize|sizeToHumanReadable|"
    r"byteCountToDisplaySize|bytesToHumanReadable|megabytesToString|formatDuration(?:Till|HMS|Words)?|"
    r"durationToString|msDurationToString|formatPercent|formatRate|"
    r"translateCodeToHumanReadableString)\s*\(|"
    r"\.(?:toHumanReadableString|getHumanReadable|getHumanReadableName)\s*\()"
)
RADIX_PATTERN = re.compile(
    r"(?i)(?:toHexString|toOctalString|toBinaryString|encodeHex|decodeHex|hexDump|"
    r"hexdump|base64\.(?:encode|decode)|base64encoder|base64decoder)"
)
NETWORK_PATTERN = re.compile(
    r"(?i)(?:getHostAddress|getRemoteSocketAddress|getLocalSocketAddress|"
    r"parseRemoteAddress|toHostString|inet_ntop)\s*\("
)
STRUCTURED_PATTERN = re.compile(
    r"(?i)(?:Arrays\.toString|deepToString|toJSONString|writeValueAsString|"
    r"jsonutils?\.|json\.encode|toJson\s*\(|getFormatted\s*\(|"
    r"String\.join|Collectors\.joining)"
)
OBJECT_TOSTRING_PATTERN = re.compile(r"\.toString\s*\(")
NUMERIC_TOSTRING_PATTERN = re.compile(
    r"(?i)(?:Integer|Long|Short|Byte|Float|Double|BigInteger|BigDecimal)\.toString\s*\("
)
TEST_FILENAME_PATTERN = re.compile(
    r"(?i)(?:Test|Tests|TestCase|IT|ITCase|IntegrationTest|Suite|Benchmark|Example|Demo)\.java$"
)
TEST_SOURCE_PATTERN = re.compile(
    rb"(?m)(?:^\s*import\s+(?:org\.junit|junit\.|org\.testng)|"
    rb"^\s*@(?:org\.junit\.)?Test\b|extends\s+TestCase\b)"
)


@dataclass
class CallRecord:
    call_id: str
    repository: str
    archive_path: str
    line: int
    receiver: str
    level: str
    categories: tuple[str, ...]
    strict_candidate: bool
    call_text: str

    def as_dict(self) -> dict[str, object]:
        return {
            "call_id": self.call_id,
            "repository": self.repository,
            "archive_path": self.archive_path,
            "line": self.line,
            "receiver": self.receiver,
            "level": self.level,
            "categories": ";".join(self.categories),
            "strict_candidate": self.strict_candidate,
            "call_text": self.call_text,
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def node_text(node: Node | None, source: bytes) -> str:
    if node is None:
        return ""
    return source[node.start_byte : node.end_byte].decode("utf-8", errors="replace")


def descendants(node: Node):
    stack = list(reversed(node.named_children))
    while stack:
        current = stack.pop()
        yield current
        stack.extend(reversed(current.named_children))


def descendants_outside_lambdas(node: Node):
    stack = list(reversed(node.named_children))
    while stack:
        current = stack.pop()
        if current.type == "lambda_expression":
            continue
        yield current
        stack.extend(reversed(current.named_children))


def logger_receiver(receiver: str, method: str) -> bool:
    if not receiver:
        return False
    tokens = re.findall(r"[A-Za-z_$][A-Za-z0-9_$]*", receiver)
    for token in tokens:
        lower = token.lower()
        if lower in {"log", "logger", "logging"} or "logger" in lower:
            return True
        if token.endswith("Log") or token.startswith("log") and len(token) > 3 and token[3].isupper():
            return True
        if token.endswith("LOG") or token.startswith("LOG"):
            return True
    return method == "log" and any("log" in token.lower() for token in tokens)


def direct_argument_texts(arguments: Node | None, source: bytes) -> list[str]:
    if arguments is None:
        return []
    return [node_text(child, source).strip() for child in arguments.named_children]


def normalize_expression(value: str) -> str:
    value = re.sub(r"\s+", "", value)
    while value.startswith("(") and value.endswith(")"):
        value = value[1:-1]
    return value


def has_derived_expression(arguments: Node | None, source: bytes) -> bool:
    if arguments is None:
        return False
    for candidate in descendants(arguments):
        if candidate.type != "binary_expression":
            continue
        operators = {child.type for child in candidate.children if not child.is_named}
        if not operators & {"-", "*", "/", "%", "<<", ">>", ">>>", "&", "|", "^"}:
            continue
        parent = candidate.parent
        selector_only = False
        while parent is not None and parent != arguments:
            if parent.type == "array_access":
                selector_only = True
                break
            if parent.type == "method_invocation":
                method = node_text(parent.child_by_field_name("name"), source)
                if method in {"get", "charAt", "substring", "subSequence", "subList", "elementAt"}:
                    selector_only = True
                    break
            parent = parent.parent
        if not selector_only:
            return True
    return False


def has_repeated_view(arguments: Node | None, source: bytes) -> bool:
    if arguments is None:
        return False
    children = list(arguments.named_children)
    if children and children[0].type in {"string_literal", "text_block"}:
        children = children[1:]
    normalized = [
        normalize_expression(node_text(child, source))
        for child in children
        if child.type not in {
            "comment", "string_literal", "text_block", "decimal_integer_literal",
            "true", "false", "null_literal"
        }
    ]
    normalized = [
        value for value in normalized
        if value and len(value) > 1 and value not in {"null", "true", "false"}
    ]
    if any(count > 1 for count in Counter(normalized).values()):
        return True

    direct = {
        normalize_expression(node_text(child, source))
        for child in children
        if child.type in {"identifier", "field_access", "array_access"}
    }
    for child in children:
        if normalize_expression(node_text(child, source)) in direct:
            continue
        for nested in descendants_outside_lambdas(child):
            if nested.type not in {"identifier", "field_access", "array_access"}:
                continue
            if normalize_expression(node_text(nested, source)) in direct:
                return True
    for child in descendants_outside_lambdas(arguments):
        if child.type != "method_invocation":
            continue
        nested_args = child.child_by_field_name("arguments")
        receiver = normalize_expression(node_text(child.child_by_field_name("object"), source))
        if receiver in direct or any(receiver.startswith(value + ".") for value in direct):
            return True
        for nested in nested_args.named_children if nested_args is not None else ():
            if normalize_expression(node_text(nested, source)) in direct:
                return True
    return False


def classify(call: Node, source: bytes) -> tuple[tuple[str, ...], bool]:
    categories: list[str] = []
    nested_calls = [node for node in descendants(call) if node.type == "method_invocation"]
    invocation_context = "\n".join(node_text(node, source) for node in nested_calls)
    arguments = call.child_by_field_name("arguments")
    has_value_expression = bool(
        arguments is not None
        and any(child.type not in {"string_literal", "text_block"} for child in arguments.named_children)
    )

    explicit_numeric_formatter = False
    for nested in nested_calls:
        name = node_text(nested.child_by_field_name("name"), source)
        if name not in {"format", "formatted", "printf"}:
            continue
        nested_arguments = nested.child_by_field_name("arguments")
        if nested_arguments is None:
            continue
        nested_literals = "\n".join(
            node_text(node, source)
            for node in descendants(nested_arguments)
            if node.type in {"string_literal", "text_block"}
        )
        nested_has_value = any(
            child.type not in {"string_literal", "text_block"}
            for child in nested_arguments.named_children
        )
        if nested_has_value and NONTRIVIAL_FORMAT_SPEC.search(nested_literals):
            explicit_numeric_formatter = True
            break

    if has_value_expression and explicit_numeric_formatter:
        categories.append("numeric_layout_format")
    if TEMPORAL_PATTERN.search(invocation_context):
        categories.append("temporal_format")
    if UNIT_PATTERN.search(invocation_context):
        categories.append("human_readable_format")
    if RADIX_PATTERN.search(invocation_context):
        categories.append("radix_or_binary_encoding")
    if NETWORK_PATTERN.search(invocation_context):
        categories.append("network_address_format")
    if STRUCTURED_PATTERN.search(invocation_context):
        categories.append("structured_rendering")
    explicit_to_string = False
    for nested in nested_calls:
        name = node_text(nested.child_by_field_name("name"), source)
        receiver = node_text(nested.child_by_field_name("object"), source)
        if name == "valueOf" and receiver == "String":
            explicit_to_string = True
            break
        if name != "toString":
            continue
        if re.fullmatch(r"(?:Integer|Long|Short|Byte|Float|Double|BigInteger|BigDecimal)", receiver):
            continue
        explicit_to_string = True
        break
    if explicit_to_string:
        categories.append("explicit_object_rendering")

    if has_derived_expression(arguments, source):
        categories.append("derived_view")
    if has_repeated_view(arguments, source):
        categories.append("repeated_view")

    categories = [name for name in CATEGORIES if name in categories]
    strict = any(name != "explicit_object_rendering" for name in categories)
    return tuple(categories), strict


def extract_calls(source: bytes, repository: str, archive_path: str, parser: Parser) -> tuple[list[CallRecord], bool]:
    tree = parser.parse(source)
    records: list[CallRecord] = []
    stack = [tree.root_node]
    while stack:
        node = stack.pop()
        stack.extend(reversed(node.named_children))
        if node.type != "method_invocation":
            continue
        name_node = node.child_by_field_name("name")
        object_node = node.child_by_field_name("object")
        level = node_text(name_node, source)
        receiver = node_text(object_node, source)
        if level.lower() not in LEVELS or not logger_receiver(receiver, level.lower()):
            continue
        categories, strict = classify(node, source)
        text = node_text(node, source)
        call_id = hashlib.sha256(
            f"{repository}\0{archive_path}\0{node.start_point.row + 1}\0{text}".encode("utf-8")
        ).hexdigest()[:16]
        records.append(
            CallRecord(
                call_id=call_id,
                repository=repository,
                archive_path=archive_path,
                line=node.start_point.row + 1,
                receiver=receiver,
                level=level,
                categories=categories,
                strict_candidate=strict,
                call_text=text,
            )
        )
    return records, tree.root_node.has_error


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def audit_sample(records: list[CallRecord], per_category: int, seed: int) -> list[dict[str, object]]:
    rng = random.Random(seed)
    selected: dict[str, CallRecord] = {}
    for category in CATEGORIES:
        pool = [record for record in records if category in record.categories]
        for record in rng.sample(pool, min(per_category, len(pool))):
            selected[record.call_id] = record
    # Include negative examples to estimate candidate recall on ordinary calls.
    negatives = [record for record in records if not record.categories]
    for record in rng.sample(negatives, min(per_category, len(negatives))):
        selected[record.call_id] = record
    rows = []
    for record in sorted(selected.values(), key=lambda item: item.call_id):
        row = record.as_dict()
        row.update({"manual_labels": "", "is_true_candidate": "", "reviewer_notes": ""})
        rows.append(row)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--audit-per-category", type=int, default=50)
    parser.add_argument("--seed", type=int, default=20260711)
    args = parser.parse_args()

    java = Language(tree_sitter_java.language())
    java_parser = Parser(java)
    records: list[CallRecord] = []
    repo_stats: dict[str, Counter[str]] = defaultdict(Counter)
    total_java_files = selected_java_files = excluded_test_files = 0
    parsed_java_files = files_with_calls = files_with_errors = 0

    with ZipFile(args.archive) as archive:
        names = [
            name
            for name in archive.namelist()
            if name.startswith("LogBench-O/repos/") and name.endswith(".java")
        ]
        total_java_files = len(names)
        for index, name in enumerate(names, 1):
            repository = name.split("/")[2]
            source = archive.read(name)
            if TEST_FILENAME_PATTERN.search(Path(name).name) or TEST_SOURCE_PATTERN.search(source):
                excluded_test_files += 1
                continue
            selected_java_files += 1
            file_records, has_error = extract_calls(source, repository, name, java_parser)
            parsed_java_files += 1
            files_with_errors += int(has_error)
            stats = repo_stats[repository]
            stats["java_files"] += 1
            stats["parse_error_files"] += int(has_error)
            if has_error:
                continue
            files_with_calls += int(bool(file_records))
            records.extend(file_records)
            stats["files_with_logging_calls"] += int(bool(file_records))
            stats["logging_calls"] += len(file_records)
            stats["strict_candidates"] += sum(record.strict_candidate for record in file_records)
            stats["broad_candidates"] += sum(bool(record.categories) for record in file_records)
            for record in file_records:
                for category in record.categories:
                    stats[category] += 1
            if index % 10000 == 0:
                print(
                    f"visited {index}/{total_java_files} files, selected={selected_java_files}, "
                    f"calls={len(records)}",
                    flush=True,
                )

    output = args.output_root
    output.mkdir(parents=True, exist_ok=True)
    candidate_rows = [record.as_dict() for record in records if record.categories]
    write_csv(output / "source_candidates.csv", candidate_rows, list(CallRecord.__annotations__) + [])

    repo_rows = []
    for repository, stats in sorted(repo_stats.items()):
        row: dict[str, object] = {"repository": repository}
        row.update({key: stats[key] for key in (
            "java_files", "parse_error_files", "files_with_logging_calls", "logging_calls",
            "strict_candidates", "broad_candidates", *CATEGORIES
        )})
        row["strict_fraction"] = stats["strict_candidates"] / stats["logging_calls"] if stats["logging_calls"] else 0.0
        row["broad_fraction"] = stats["broad_candidates"] / stats["logging_calls"] if stats["logging_calls"] else 0.0
        repo_rows.append(row)
    write_csv(output / "source_by_repository.csv", repo_rows)

    category_counts = Counter(category for record in records for category in record.categories)
    strict_count = sum(record.strict_candidate for record in records)
    broad_count = sum(bool(record.categories) for record in records)
    summary = {
        "archive": str(args.archive),
        "archive_sha256": sha256_file(args.archive),
        "repositories": len(repo_stats),
        "java_files": total_java_files,
        "selected_production_like_java_files": selected_java_files,
        "excluded_likely_test_files": excluded_test_files,
        "parsed_java_files": parsed_java_files,
        "parse_clean_java_files": parsed_java_files - files_with_errors,
        "files_with_parse_errors": files_with_errors,
        "files_with_logging_calls": files_with_calls,
        "logging_calls": len(records),
        "strict_candidates": strict_count,
        "strict_fraction": strict_count / len(records) if records else 0.0,
        "broad_candidates": broad_count,
        "broad_fraction": broad_count / len(records) if records else 0.0,
        "category_counts": {category: category_counts[category] for category in CATEGORIES},
        "candidate_definition": "strict excludes generic explicit object toString; broad includes it",
        "audit_seed": args.seed,
        "audit_per_category": args.audit_per_category,
        "test_filter": "filename plus JUnit/TestNG imports, @Test, and TestCase inheritance",
        "parse_policy": "files whose tree-sitter syntax tree contains errors are excluded from call counts",
        "status": "automatic_candidates_pending_manual_audit",
    }
    (output / "source_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    write_csv(output / "audit_sample.csv", audit_sample(records, args.audit_per_category, args.seed))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
