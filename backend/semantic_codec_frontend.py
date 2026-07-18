#!/usr/bin/env python3
"""SemZip semantic-function frontend with self-contained side-stream archives.

The frontend deliberately stops before SemZip residual planning. Accepted
function programs retain SemZip's exact stream codecs; only the placeholder-
bearing residual text is handed to an external main-stream backend.
"""

from __future__ import annotations

import concurrent.futures
import hashlib
import json
import lzma
import os
import shutil
import sys
import tarfile
import tempfile
from pathlib import Path
from typing import Any


SEMANTIC_VERSION = "SEMZIP-DELOG-ALLGROUP-OR20-ADMISSION-V6-20260718"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def split_binary_lines(input_path: Path, output_dir: Path, block_size: int) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    blocks: list[Path] = []
    block_stream = None
    try:
        with input_path.open("rb") as source:
            for line_index, line in enumerate(source):
                if line_index % block_size == 0:
                    if block_stream is not None:
                        block_stream.close()
                    block_path = output_dir / f"block_{len(blocks):05d}.log"
                    blocks.append(block_path)
                    block_stream = block_path.open("wb")
                block_stream.write(line)
    finally:
        if block_stream is not None:
            block_stream.close()
    if not blocks:
        block_path = output_dir / "block_00000.log"
        block_path.write_bytes(b"")
        blocks.append(block_path)
    return blocks


def concatenate_files(paths: list[Path], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as target:
        for path in paths:
            with path.open("rb") as source:
                shutil.copyfileobj(source, target, 8 * 1024 * 1024)


def _load_semzip_runtime(source_root: Path):
    source_text = str(source_root.resolve())
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
    import pare_dataset_extract as dataset_extract  # type: ignore
    import semzip_pure as semzip  # type: ignore

    return semzip, dataset_extract


def _stable_tarinfo(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
    tarinfo.uid = 0
    tarinfo.gid = 0
    tarinfo.uname = ""
    tarinfo.gname = ""
    tarinfo.mtime = 0
    return tarinfo


def create_semantic_archive(root: Path, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with lzma.open(archive_path, "wb", preset=6 | lzma.PRESET_EXTREME) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as tar:
            for path in sorted(item for item in root.rglob("*") if item.is_file()):
                tar.add(path, arcname=str(path.relative_to(root)), filter=_stable_tarinfo)


def extract_semantic_archive(archive_path: Path, restore_root: Path) -> None:
    restore_root.mkdir(parents=True, exist_ok=True)
    with lzma.open(archive_path, "rb") as compressed:
        with tarfile.open(fileobj=compressed, mode="r") as tar:
            tar.extractall(restore_root)


def _semantic_metadata(
    dataset: str,
    specs: list[Any],
    placeholders: dict[str, str],
    values_by_tag: dict[str, list[str]],
) -> dict[str, Any]:
    specs_meta: list[dict[str, Any]] = []
    serialized: set[str] = set()
    for spec in specs:
        if spec.tag in serialized or not values_by_tag.get(spec.tag):
            continue
        serialized.add(spec.tag)
        specs_meta.append(
            {
                "tag": spec.tag,
                "kind": spec.kind,
                "placeholder": placeholders[spec.tag],
                "pattern": spec.pattern,
                "store_group": spec.store_group,
                "replacement": spec.replacement,
                "context_tag": (
                    json.dumps(spec.program, sort_keys=True)
                    if spec.kind
                    in {
                        "open_function",
                        "open_context_delta",
                        "open_context_dict",
                        "relation_pair_delta",
                        "routed_split",
                    }
                    else ""
                ),
            }
        )
    return {
        "dataset": dataset,
        "profile": "open_function_v1",
        "compact_stream_metadata": True,
        "post_merge_hex_streams": False,
        "specs": specs_meta,
    }


def _stream_summary(stream: dict[str, Any]) -> dict[str, Any]:
    nested = stream.get("python_exec_string_codec")
    return {
        "tag": stream.get("tag"),
        "kind": stream.get("kind"),
        "count": stream.get("count", 0),
        "open_numeric_codec": stream.get("open_numeric_codec"),
        "string_codec": nested.get("kind") if isinstance(nested, dict) else None,
        "layout": bool(stream.get("layout_file")),
    }


def _encode_block(job: tuple[str, str, str, str, str]) -> dict[str, Any]:
    block_path_text, transformed_path_text, archive_path_text, plan_path_text, source_root_text = job
    block_path = Path(block_path_text)
    transformed_path = Path(transformed_path_text)
    archive_path = Path(archive_path_text)
    plan_path = Path(plan_path_text)
    source_root = Path(source_root_text)

    # These are the frozen mainline routes used by accepted function streams.
    os.environ["PARE_NO_BENEFIT_FIXED_CODEC"] = "1"
    os.environ["PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA"] = "1"

    semzip, dataset_extract = _load_semzip_runtime(source_root)
    plan_payload = json.loads(plan_path.read_text(encoding="utf-8"))
    dataset = str(plan_payload["dataset"])
    loaded = semzip.load_replay_plan(str(plan_path), dataset)
    if loaded is None:
        raise ValueError(f"Could not load replay plan {plan_path}")
    specs, plan_placeholders = loaded
    specs = [spec for spec in specs if not semzip.is_stage_residual_spec(spec)]
    rejected_structure_tags = [
        spec.tag
        for spec in specs
        if not semzip.regex_has_fixed_alpha_or_structure(spec.pattern)
    ]
    specs = [
        spec
        for spec in specs
        if semzip.regex_has_fixed_alpha_or_structure(spec.pattern)
    ]
    placeholders = {
        spec.tag: plan_placeholders[spec.tag]
        for spec in specs
        if spec.tag in plan_placeholders
    }
    specs = [spec for spec in specs if spec.tag in placeholders]

    original = block_path.read_bytes().decode("latin-1")
    values_by_tag: dict[str, list[str]] = {}
    transformed = semzip.apply_specs_to_original_text(
        original,
        specs,
        placeholders,
        values_by_tag,
        trusted_fast_path=True,
    )
    if semzip.restore_validation_text(transformed, specs, placeholders, values_by_tag) != original:
        raise ValueError(f"Semantic replay validation failed for {block_path.name}")

    direct_context_stats = semzip.materialize_direct_context_projectors(
        original,
        transformed,
        values_by_tag,
        placeholders,
        specs,
    )
    cross_context_stats = semzip.materialize_cross_function_contexts(
        transformed,
        values_by_tag,
        placeholders,
        specs,
    )
    metadata = _semantic_metadata(dataset, specs, placeholders, values_by_tag)

    transformed_path.parent.mkdir(parents=True, exist_ok=True)
    transformed_path.write_bytes(transformed.encode("latin-1"))
    with tempfile.TemporaryDirectory(prefix="semzip-semantic-") as temp_name:
        root = Path(temp_name)
        dataset_extract.save_extract_streams(root, metadata, values_by_tag, transformed)
        restored = dataset_extract.restore_text(transformed, root / "dataset_extract", metadata)
        if restored != original:
            raise ValueError(f"Semantic codec roundtrip failed for {block_path.name}")
        (root / "metadata.json").write_text(
            json.dumps(metadata, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        create_semantic_archive(root, archive_path)

    return {
        "block": block_path.name,
        "raw_bytes": block_path.stat().st_size,
        "transformed_bytes": transformed_path.stat().st_size,
        "semantic_archive": archive_path.name,
        "semantic_archive_bytes": archive_path.stat().st_size,
        "spec_counts": {
            tag: len(values)
            for tag, values in sorted(values_by_tag.items())
            if values
        },
        "streams": [_stream_summary(stream) for stream in metadata.get("streams", [])],
        "rejected_structure_tags": rejected_structure_tags,
        "direct_context": direct_context_stats,
        "cross_context": cross_context_stats,
    }


def encode_file_parallel(
    input_path: Path,
    transformed_path: Path,
    semantic_archive_dir: Path,
    plan_path: Path,
    source_root: Path,
    block_size: int,
    workers: int,
    temp_dir: Path,
) -> dict[str, Any]:
    raw_blocks = split_binary_lines(input_path, temp_dir / "raw", block_size)
    transformed_dir = temp_dir / "transformed"
    transformed_dir.mkdir(parents=True, exist_ok=True)
    semantic_archive_dir.mkdir(parents=True, exist_ok=True)
    jobs = [
        (
            str(block),
            str(transformed_dir / block.name),
            str(semantic_archive_dir / f"block_{index:05d}.semantic.tar.xz"),
            str(plan_path),
            str(source_root),
        )
        for index, block in enumerate(raw_blocks)
    ]
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        block_results = list(executor.map(_encode_block, jobs))
    transformed_blocks = [transformed_dir / block.name for block in raw_blocks]
    concatenate_files(transformed_blocks, transformed_path)
    return {
        "version": SEMANTIC_VERSION,
        "dataset": json.loads(plan_path.read_text(encoding="utf-8"))["dataset"],
        "block_size": block_size,
        "block_count": len(raw_blocks),
        "blocks": block_results,
        "input_sha256": sha256_file(input_path),
        "transformed_sha256": sha256_file(transformed_path),
    }


def _decode_block(job: tuple[str, str, str, str]) -> dict[str, Any]:
    transformed_path_text, archive_path_text, decoded_path_text, source_root_text = job
    transformed_path = Path(transformed_path_text)
    archive_path = Path(archive_path_text)
    decoded_path = Path(decoded_path_text)
    source_root = Path(source_root_text)
    _semzip, dataset_extract = _load_semzip_runtime(source_root)

    transformed = transformed_path.read_bytes().decode("latin-1")
    with tempfile.TemporaryDirectory(prefix="semzip-semantic-restore-") as temp_name:
        root = Path(temp_name)
        extract_semantic_archive(archive_path, root)
        metadata = json.loads((root / "metadata.json").read_text(encoding="utf-8"))
        restored = dataset_extract.restore_text(transformed, root / "dataset_extract", metadata)
    decoded_path.parent.mkdir(parents=True, exist_ok=True)
    decoded_path.write_bytes(restored.encode("latin-1"))
    return {
        "block": transformed_path.name,
        "decoded_bytes": decoded_path.stat().st_size,
    }


def decode_file_parallel(
    transformed_path: Path,
    output_path: Path,
    semantic_archive_dir: Path,
    source_root: Path,
    block_size: int,
    workers: int,
    temp_dir: Path,
) -> dict[str, Any]:
    transformed_blocks = split_binary_lines(transformed_path, temp_dir / "transformed", block_size)
    decoded_dir = temp_dir / "decoded"
    archives = sorted(semantic_archive_dir.glob("block_*.semantic.tar.xz"))
    if len(transformed_blocks) != len(archives):
        raise ValueError(
            f"Semantic block mismatch: transformed={len(transformed_blocks)} archives={len(archives)}"
        )
    jobs = [
        (
            str(block),
            str(archive),
            str(decoded_dir / block.name),
            str(source_root),
        )
        for block, archive in zip(transformed_blocks, archives)
    ]
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        block_results = list(executor.map(_decode_block, jobs))
    decoded_blocks = [decoded_dir / block.name for block in transformed_blocks]
    concatenate_files(decoded_blocks, output_path)
    return {
        "block_count": len(decoded_blocks),
        "blocks": block_results,
        "output_sha256": sha256_file(output_path),
    }
