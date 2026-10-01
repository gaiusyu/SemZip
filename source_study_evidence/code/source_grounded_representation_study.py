#!/usr/bin/env python3
"""Controlled source-grounded latent-representation compression study.

For exactly the same source-grounded spans, this script constructs a raw-span
archive and a latent archive. Both use the same placeholders and outer
compressors. Every measured archive is decoded and checked against the raw
input byte for byte.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import datetime as dt
import gzip
import hashlib
import json
import lzma
import os
import re
import shutil
import struct
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Iterator


TOKEN_PREFIX = b"\x1dSG"
TOKEN_SUFFIX = b"\x1e"


def encode_uvarint(value: int) -> bytes:
    if value < 0:
        raise ValueError(f"negative unsigned varint: {value}")
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def decode_uvarint(fh: BinaryIO) -> int:
    shift = 0
    value = 0
    while True:
        raw = fh.read(1)
        if not raw:
            raise EOFError("truncated varint")
        byte = raw[0]
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value
        shift += 7
        if shift > 4096:
            raise ValueError("varint too large")


def zigzag(value: int) -> int:
    return value * 2 if value >= 0 else -value * 2 - 1


def unzigzag(value: int) -> int:
    return value // 2 if value % 2 == 0 else -(value // 2) - 1


def encode_svarint(value: int) -> bytes:
    return encode_uvarint(zigzag(value))


def decode_svarint(fh: BinaryIO) -> int:
    return unzigzag(decode_uvarint(fh))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def selected_catalog_sha256(families: list["Family"]) -> str:
    payload = json.dumps(
        [family.spec for family in families], sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def copy_and_hash(source: Path, target: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    total = 0
    with source.open("rb") as src, target.open("wb") as dst:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            dst.write(chunk)
            digest.update(chunk)
            total += len(chunk)
    return total, digest.hexdigest()


@dataclass
class Family:
    index: int
    spec: dict[str, object]
    scope: re.Pattern[bytes]
    span: re.Pattern[bytes]

    @property
    def id(self) -> str:
        return str(self.spec["id"])

    @property
    def codec(self) -> str:
        return str(self.spec["codec"])

    @property
    def token(self) -> bytes:
        return TOKEN_PREFIX + f"{self.index:03d}".encode("ascii") + TOKEN_SUFFIX


@dataclass
class FamilyStats:
    occurrences: int = 0
    surface_bytes: int = 0
    raw_stream_bytes: int = 0
    latent_stream_bytes: int = 0


@dataclass
class EncodeState:
    family: Family
    raw_fh: BinaryIO
    latent_fh: BinaryIO
    previous: int = 0
    stats: FamilyStats = field(default_factory=FamilyStats)

    def append(self, surface: bytes) -> None:
        raw_record = encode_uvarint(len(surface)) + surface
        self.raw_fh.write(raw_record)
        latent_record, current = encode_latent(self.family.codec, surface, self.previous)
        self.latent_fh.write(latent_record)
        if current is not None:
            self.previous = current
        self.stats.occurrences += 1
        self.stats.surface_bytes += len(surface)
        self.stats.raw_stream_bytes += len(raw_record)
        self.stats.latent_stream_bytes += len(latent_record)


def datetime_to_millis(value: dt.datetime) -> int:
    return calendar.timegm(value.timetuple()) * 1000 + value.microsecond // 1000


UNIT_CODES = {b"B": 0, b"KB": 1, b"MB": 2, b"GB": 3, b"TB": 4}
UNIT_NAMES = {value: key for key, value in UNIT_CODES.items()}


def encode_latent(codec: str, surface: bytes, previous: int) -> tuple[bytes, int | None]:
    text = surface.decode("ascii")
    if codec == "datetime_yy_seconds":
        value = calendar.timegm(dt.datetime.strptime(text, "%y/%m/%d %H:%M:%S").timetuple())
        return encode_svarint(value - previous), value
    if codec == "datetime_iso_millis":
        value = datetime_to_millis(dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S,%f"))
        return encode_svarint(value - previous), value
    if codec == "datetime_iso_millis_dot":
        value = datetime_to_millis(dt.datetime.strptime(text, "%Y-%m-%d %H:%M:%S.%f"))
        return encode_svarint(value - previous), value
    if codec == "scaled_decimal_unit":
        match = re.fullmatch(rb"([0-9]+)\.([0-9]) (B|KB|MB|GB|TB)", surface)
        if not match:
            raise ValueError(f"bad scaled decimal: {surface!r}")
        scaled = int(match.group(1)) * 10 + int(match.group(2))
        unit = UNIT_CODES[match.group(3)]
        return encode_svarint(scaled - previous) + bytes([unit]), scaled
    if codec == "integer_unit":
        match = re.fullmatch(rb"([0-9]+)(MB|GB)", surface)
        if not match:
            raise ValueError(f"bad integer unit: {surface!r}")
        value = int(match.group(1))
        unit = UNIT_CODES[match.group(2)]
        return encode_svarint(value - previous) + bytes([unit]), value
    if codec == "hex_integer":
        match = re.fullmatch(rb"0x([0-9a-fA-F]+)", surface)
        if not match:
            raise ValueError(f"bad hex integer: {surface!r}")
        digits = match.group(1)
        value = int(digits, 16)
        normal = format(value, "x").encode("ascii")
        flags = (1 if any(65 <= c <= 70 for c in digits) else 0) | (2 if len(digits) != len(normal) else 0)
        record = encode_svarint(value - previous) + bytes([flags])
        if flags & 2:
            record += encode_uvarint(len(digits))
        return record, value
    if codec in {"ipv4", "ipv4_endpoint"}:
        endpoint = codec == "ipv4_endpoint"
        match = re.fullmatch(rb"/?([0-9]{1,3}(?:\.[0-9]{1,3}){3})(?::([0-9]+))?", surface)
        if not match or endpoint != bool(match.group(2)):
            raise ValueError(f"bad {codec}: {surface!r}")
        octets = [int(part) for part in match.group(1).split(b".")]
        if any(value > 255 for value in octets):
            raise ValueError(f"bad IPv4 octet: {surface!r}")
        record = bytes(octets)
        if endpoint:
            record += encode_uvarint(int(match.group(2)))
        return record, None
    raise ValueError(f"unknown codec: {codec}")


class LatentDecoder:
    def __init__(self, family: Family, path: Path):
        self.family = family
        self.fh = path.open("rb")
        self.previous = 0

    def next(self) -> bytes:
        codec = self.family.codec
        if codec in {"datetime_yy_seconds", "datetime_iso_millis", "datetime_iso_millis_dot"}:
            value = self.previous + decode_svarint(self.fh)
            self.previous = value
            if codec == "datetime_yy_seconds":
                return dt.datetime.utcfromtimestamp(value).strftime("%y/%m/%d %H:%M:%S").encode("ascii")
            stamp = dt.datetime.utcfromtimestamp(value / 1000)
            base = stamp.strftime("%Y-%m-%d %H:%M:%S")
            sep = "," if codec == "datetime_iso_millis" else "."
            return f"{base}{sep}{value % 1000:03d}".encode("ascii")
        if codec == "scaled_decimal_unit":
            scaled = self.previous + decode_svarint(self.fh)
            self.previous = scaled
            unit_raw = self.fh.read(1)
            if not unit_raw:
                raise EOFError("missing scaled-decimal unit")
            return f"{scaled // 10}.{scaled % 10} ".encode("ascii") + UNIT_NAMES[unit_raw[0]]
        if codec == "integer_unit":
            value = self.previous + decode_svarint(self.fh)
            self.previous = value
            unit_raw = self.fh.read(1)
            if not unit_raw:
                raise EOFError("missing integer unit")
            return str(value).encode("ascii") + UNIT_NAMES[unit_raw[0]]
        if codec == "hex_integer":
            value = self.previous + decode_svarint(self.fh)
            self.previous = value
            flag_raw = self.fh.read(1)
            if not flag_raw:
                raise EOFError("missing hex flags")
            flags = flag_raw[0]
            width = decode_uvarint(self.fh) if flags & 2 else 0
            digits = format(value, "X" if flags & 1 else "x")
            if width:
                digits = digits.zfill(width)
            return ("0x" + digits).encode("ascii")
        if codec in {"ipv4", "ipv4_endpoint"}:
            octets = self.fh.read(4)
            if len(octets) != 4:
                raise EOFError("truncated IPv4")
            value = b".".join(str(part).encode("ascii") for part in octets)
            if codec == "ipv4_endpoint":
                value = b"/" + value + b":" + str(decode_uvarint(self.fh)).encode("ascii")
            return value
        raise ValueError(f"unknown codec: {codec}")

    def close(self) -> None:
        trailing = self.fh.read(1)
        self.fh.close()
        if trailing:
            raise ValueError(f"trailing latent bytes for {self.family.id}")


class RawDecoder:
    def __init__(self, path: Path):
        self.fh = path.open("rb")

    def next(self) -> bytes:
        length = decode_uvarint(self.fh)
        value = self.fh.read(length)
        if len(value) != length:
            raise EOFError("truncated raw span")
        return value

    def close(self) -> None:
        trailing = self.fh.read(1)
        self.fh.close()
        if trailing:
            raise ValueError("trailing raw-span bytes")


def load_families(catalog_path: Path, dataset: str) -> list[Family]:
    specs = json.loads(catalog_path.read_text(encoding="utf-8"))
    selected = [spec for spec in specs if spec["dataset"] == dataset]
    families = []
    for index, spec in enumerate(selected):
        families.append(
            Family(
                index=index,
                spec=spec,
                scope=re.compile(str(spec["scope_regex"]).encode("ascii")),
                span=re.compile(str(spec["span_regex"]).encode("ascii")),
            )
        )
    if not families:
        raise ValueError(f"no catalog families for {dataset}")
    return families


def prepare_stage(stage: Path, families: list[Family], mode: str) -> dict[str, EncodeState]:
    (stage / "streams").mkdir(parents=True, exist_ok=True)
    states: dict[str, EncodeState] = {}
    for family in families:
        raw_fh = (stage.parent / "raw_span" / "streams" / f"{family.index:03d}.bin").open("wb")
        latent_fh = (stage.parent / "latent" / "streams" / f"{family.index:03d}.bin").open("wb")
        states[family.id] = EncodeState(family, raw_fh, latent_fh)
    return states


def transform_file(raw_path: Path, root: Path, families: list[Family]) -> tuple[dict[str, EncodeState], int, str]:
    raw_stage = root / "raw_span"
    latent_stage = root / "latent"
    (raw_stage / "streams").mkdir(parents=True, exist_ok=True)
    (latent_stage / "streams").mkdir(parents=True, exist_ok=True)
    states = prepare_stage(raw_stage, families, "raw_span")
    digest = hashlib.sha256()
    raw_bytes = 0
    main_raw = (raw_stage / "main.bin").open("wb")
    main_latent = (latent_stage / "main.bin").open("wb")
    try:
        with raw_path.open("rb") as src:
            for line_no, line in enumerate(src, 1):
                digest.update(line)
                raw_bytes += len(line)
                if TOKEN_PREFIX in line:
                    raise ValueError(f"reserved token prefix in raw input at line {line_no}")
                match_line = line.rstrip(b"\r\n")
                matches: list[tuple[int, int, Family, bytes]] = []
                for family in families:
                    if not family.scope.search(match_line):
                        continue
                    for match in family.span.finditer(match_line):
                        start, end = match.span("span")
                        matches.append((start, end, family, match.group("span")))
                matches.sort(key=lambda item: (item[0], item[1]))
                for previous_match, current in zip(matches, matches[1:]):
                    if current[0] < previous_match[1]:
                        raise ValueError(
                            f"overlap on line {line_no}: {previous_match[2].id} and {current[2].id}"
                        )
                output = bytearray()
                pos = 0
                for start, end, family, surface in matches:
                    output.extend(line[pos:start])
                    output.extend(family.token)
                    states[family.id].append(surface)
                    pos = end
                output.extend(line[pos:])
                main_raw.write(output)
                main_latent.write(output)
    finally:
        main_raw.close()
        main_latent.close()
        for state in states.values():
            state.raw_fh.close()
            state.latent_fh.close()
    return states, raw_bytes, digest.hexdigest()


def write_metadata(
    root: Path,
    families: list[Family],
    states: dict[str, EncodeState],
    raw_bytes: int,
    raw_sha: str,
    catalog_sha: str,
) -> None:
    common = {
        "version": 1,
        "raw_bytes": raw_bytes,
        "raw_sha256": raw_sha,
        "catalog_sha256": catalog_sha,
        "families": [
            {
                "index": family.index,
                "id": family.id,
                "codec": family.codec,
                "count": states[family.id].stats.occurrences,
            }
            for family in families
        ],
    }
    for mode in ("raw_span", "latent"):
        payload = dict(common)
        payload["mode"] = mode
        (root / mode / "metadata.json").write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")), encoding="utf-8"
        )


def deterministic_tar(stage: Path, tar_path: Path) -> None:
    with tarfile.open(tar_path, "w") as tf:
        for path in sorted(item for item in stage.rglob("*") if item.is_file()):
            arcname = path.relative_to(stage).as_posix()
            info = tf.gettarinfo(str(path), arcname)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            info.mode = 0o644
            with path.open("rb") as fh:
                tf.addfile(info, fh)


def gzip_file(source: Path, target: Path) -> None:
    with source.open("rb") as src, target.open("wb") as raw_out:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw_out, compresslevel=9, mtime=0) as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)


def xz_file(source: Path, target: Path, preset: int) -> None:
    with source.open("rb") as src, lzma.open(target, "wb", preset=preset) as dst:
        shutil.copyfileobj(src, dst, length=1024 * 1024)


def make_archives(raw_path: Path, root: Path, preset: int) -> dict[str, int]:
    sizes: dict[str, int] = {}
    raw_gz = root / "raw.log.gz"
    raw_xz = root / "raw.log.xz"
    gzip_file(raw_path, raw_gz)
    xz_file(raw_path, raw_xz, preset)
    sizes["raw_gzip_bytes"] = raw_gz.stat().st_size
    sizes["raw_xz_bytes"] = raw_xz.stat().st_size
    for mode in ("raw_span", "latent"):
        tar_path = root / f"{mode}.tar"
        deterministic_tar(root / mode, tar_path)
        gz_path = root / f"{mode}.tar.gz"
        xz_path = root / f"{mode}.tar.xz"
        gzip_file(tar_path, gz_path)
        xz_file(tar_path, xz_path, preset)
        sizes[f"{mode}_tar_bytes"] = tar_path.stat().st_size
        sizes[f"{mode}_gzip_bytes"] = gz_path.stat().st_size
        sizes[f"{mode}_xz_bytes"] = xz_path.stat().st_size
        tar_path.unlink()
    return sizes


def decode_archive(archive: Path, output: Path, families: list[Family]) -> tuple[int, str]:
    with tempfile.TemporaryDirectory(prefix="source-grounded-decode-") as tmp:
        stage = Path(tmp)
        with tarfile.open(archive, "r:xz") as tf:
            tf.extractall(stage)
        metadata = json.loads((stage / "metadata.json").read_text(encoding="utf-8"))
        mode = metadata["mode"]
        decoders = {}
        counts = {}
        for item in metadata["families"]:
            family = families[int(item["index"])]
            path = stage / "streams" / f"{family.index:03d}.bin"
            decoders[family.index] = RawDecoder(path) if mode == "raw_span" else LatentDecoder(family, path)
            counts[family.index] = 0
        token_pattern = re.compile(re.escape(TOKEN_PREFIX) + rb"([0-9]{3})" + re.escape(TOKEN_SUFFIX))
        digest = hashlib.sha256()
        total = 0
        with (stage / "main.bin").open("rb") as src, output.open("wb") as dst:
            for line in src:
                pieces = []
                pos = 0
                for match in token_pattern.finditer(line):
                    index = int(match.group(1))
                    pieces.append(line[pos : match.start()])
                    pieces.append(decoders[index].next())
                    counts[index] += 1
                    pos = match.end()
                pieces.append(line[pos:])
                restored = b"".join(pieces)
                dst.write(restored)
                digest.update(restored)
                total += len(restored)
        for item in metadata["families"]:
            index = int(item["index"])
            expected = int(item["count"])
            if counts[index] != expected:
                raise ValueError(f"count mismatch for family {index}: {counts[index]} != {expected}")
            decoders[index].close()
        return total, digest.hexdigest()


def run_dataset(dataset: str, raw_path: Path, catalog_path: Path, output_root: Path, preset: int) -> tuple[dict[str, object], list[dict[str, object]]]:
    families = load_families(catalog_path, dataset)
    root = output_root / dataset
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    states, raw_bytes, raw_sha = transform_file(raw_path, root, families)
    catalog_sha = selected_catalog_sha256(families)
    write_metadata(root, families, states, raw_bytes, raw_sha, catalog_sha)
    sizes = make_archives(raw_path, root, preset)

    verification = {}
    for mode in ("raw_span", "latent"):
        decoded = root / f"decoded_{mode}.log"
        decoded_bytes, decoded_sha = decode_archive(root / f"{mode}.tar.xz", decoded, families)
        verification[f"{mode}_decoded_bytes"] = decoded_bytes
        verification[f"{mode}_sha256"] = decoded_sha
        verification[f"{mode}_sha_pass"] = decoded_bytes == raw_bytes and decoded_sha == raw_sha
        if not verification[f"{mode}_sha_pass"]:
            raise RuntimeError(f"{dataset} {mode} failed byte-exact verification")
        decoded.unlink()

    surface_bytes = sum(state.stats.surface_bytes for state in states.values())
    raw_stream_bytes = sum(state.stats.raw_stream_bytes for state in states.values())
    latent_stream_bytes = sum(state.stats.latent_stream_bytes for state in states.values())
    summary: dict[str, object] = {
        "dataset": dataset,
        "input": str(raw_path),
        "raw_bytes": raw_bytes,
        "raw_sha256": raw_sha,
        "source_grounded_families": sum(state.stats.occurrences > 0 for state in states.values()),
        "matched_occurrences": sum(state.stats.occurrences for state in states.values()),
        "surface_bytes": surface_bytes,
        "surface_fraction": surface_bytes / raw_bytes if raw_bytes else 0.0,
        "raw_span_stream_bytes": raw_stream_bytes,
        "latent_stream_bytes": latent_stream_bytes,
        "surface_to_latent": surface_bytes / latent_stream_bytes if latent_stream_bytes else 0.0,
        "raw_span_to_latent_stream": raw_stream_bytes / latent_stream_bytes if latent_stream_bytes else 0.0,
        **sizes,
        "gzip_separation_gain": sizes["raw_gzip_bytes"] / sizes["raw_span_gzip_bytes"],
        "gzip_representation_gain": sizes["raw_span_gzip_bytes"] / sizes["latent_gzip_bytes"],
        "gzip_end_to_end_gain": sizes["raw_gzip_bytes"] / sizes["latent_gzip_bytes"],
        "xz_separation_gain": sizes["raw_xz_bytes"] / sizes["raw_span_xz_bytes"],
        "xz_representation_gain": sizes["raw_span_xz_bytes"] / sizes["latent_xz_bytes"],
        "xz_end_to_end_gain": sizes["raw_xz_bytes"] / sizes["latent_xz_bytes"],
        **verification,
        "execution_label": "local_smoke_not_paper_result",
    }
    detail_rows = []
    for family in families:
        stats = states[family.id].stats
        detail_rows.append(
            {
                "dataset": dataset,
                "family_id": family.id,
                "representation_class": family.spec["representation_class"],
                "recovery_semantics": family.spec["recovery_semantics"],
                "codec": family.codec,
                "occurrences": stats.occurrences,
                "surface_bytes": stats.surface_bytes,
                "raw_span_stream_bytes": stats.raw_stream_bytes,
                "latent_stream_bytes": stats.latent_stream_bytes,
                "surface_to_latent": stats.surface_bytes / stats.latent_stream_bytes if stats.latent_stream_bytes else 0.0,
                "source_file": family.spec["source_file"],
                "source_lines": family.spec["source_lines"],
                "revision": family.spec["revision"],
            }
        )
    return summary, detail_rows


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def parse_input(value: str) -> tuple[str, Path]:
    if "=" not in value:
        raise argparse.ArgumentTypeError("input must be DATASET=/path/to/log")
    dataset, path = value.split("=", 1)
    return dataset, Path(path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", action="append", required=True, type=parse_input)
    parser.add_argument("--catalog", type=Path, default=Path(__file__).with_name("family_catalog.json"))
    parser.add_argument("--output-root", type=Path, default=Path(__file__).with_name("results") / "representation_smoke")
    parser.add_argument("--xz-preset", type=int, default=6)
    parser.add_argument("--execution-label", default="local_smoke_not_paper_result")
    args = parser.parse_args()
    summaries = []
    details = []
    for dataset, path in args.input:
        summary, detail_rows = run_dataset(dataset, path, args.catalog, args.output_root, args.xz_preset)
        summary["execution_label"] = args.execution_label
        summaries.append(summary)
        details.extend(detail_rows)
        print(
            f"{dataset}: surface={summary['surface_fraction']:.2%} "
            f"stream_gain={summary['raw_span_to_latent_stream']:.3f} "
            f"xz_repr_gain={summary['xz_representation_gain']:.3f} "
            f"sha={summary['latent_sha_pass']}"
        )
    write_csv(args.output_root / "summary.csv", summaries)
    write_csv(args.output_root / "families.csv", details)
    (args.output_root / "summary.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
