#!/usr/bin/env python3
"""Apache-specific regex extraction wrapper around pare_rank_proto.

This experiment keeps the rank-centric core unchanged, but adds a lossless
pre-extraction pass for two Apache-friendly structured fields:

* full bracketed timestamps, e.g. [Thu Jun 09 06:07:04 2005]
* IPv4 addresses

The transformed text is then compressed by the base rank codec using compact
placeholder tokens. The extracted fields are stored in typed side streams.
"""

from __future__ import annotations

import argparse
import json
import lzma
import re
import shutil
import tarfile
from datetime import datetime
from pathlib import Path

import pare_rank_proto as base


APACHE_TIMESTAMP_RE = re.compile(
    r"\[(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) "
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) "
    r"\d{2} \d{2}:\d{2}:\d{2} \d{4}\]"
)
IPV4_RE = re.compile(r"(?<!\d)(?:\d{1,3}\.){3}\d{1,3}(?!\d)")


def zigzag_encode(value: int) -> int:
    return (value << 1) ^ (value >> 63)


def zigzag_decode(value: int) -> int:
    return (value >> 1) ^ -(value & 1)


def choose_placeholder(text: str, kind: str) -> str:
    candidates = [
        f"<@PARE_{kind}@>",
        f"<#PARE_{kind}#>",
        f"{{PARE_{kind}}}",
        f"__PARE_{kind}__",
    ]
    for candidate in candidates:
        if candidate not in text:
            return candidate
    raise ValueError(f"Could not find collision-free placeholder for {kind}")


def parse_apache_timestamp(token: str) -> int:
    return int(datetime.strptime(token[1:-1], "%a %b %d %H:%M:%S %Y").timestamp())


def encode_timestamp_stream(tokens: list[str]) -> bytes:
    values: list[int] = []
    last_ts: int | None = None
    for token in tokens:
        current = parse_apache_timestamp(token)
        delta = current if last_ts is None else current - last_ts
        values.append(zigzag_encode(delta))
        last_ts = current
    data = bytearray()
    for value in values:
        data.extend(base.encode_varint(value))
    return bytes(data)


def decode_timestamp_stream(data: bytes) -> list[str]:
    values: list[str] = []
    cursor = 0
    last_ts: int | None = None
    while cursor < len(data):
        encoded, cursor = base.decode_varint(data, cursor)
        delta = zigzag_decode(encoded)
        current = delta if last_ts is None else last_ts + delta
        values.append("[" + datetime.fromtimestamp(current).strftime("%a %b %d %H:%M:%S %Y") + "]")
        last_ts = current
    return values


def encode_ip_stream(tokens: list[str]) -> bytes:
    data = bytearray()
    for token in tokens:
        octets = token.split(".")
        if len(octets) != 4:
            raise ValueError(f"Bad IPv4 token {token!r}")
        for octet in octets:
            value = int(octet)
            if value < 0 or value > 255:
                raise ValueError(f"Bad IPv4 octet {octet!r} in {token!r}")
            data.append(value)
    return bytes(data)


def decode_ip_stream(data: bytes) -> list[str]:
    if len(data) % 4 != 0:
        raise ValueError("Corrupt IPv4 stream")
    values: list[str] = []
    for cursor in range(0, len(data), 4):
        octets = [str(byte) for byte in data[cursor:cursor + 4]]
        values.append(".".join(octets))
    return values


def transform_apache_text(
    text: str,
    extract_timestamps: bool = True,
    extract_ips: bool = True,
) -> tuple[str, list[str], list[str], str, str]:
    ts_placeholder = choose_placeholder(text, "TS")
    ip_placeholder = choose_placeholder(text, "IP")

    timestamps: list[str] = []
    ipv4s: list[str] = []

    def replace_timestamps(match: re.Match[str]) -> str:
        timestamps.append(match.group(0))
        return ts_placeholder

    def replace_ips(match: re.Match[str]) -> str:
        ipv4s.append(match.group(0))
        return ip_placeholder

    transformed = text
    if extract_timestamps:
        transformed = APACHE_TIMESTAMP_RE.sub(replace_timestamps, transformed)
    if extract_ips:
        transformed = IPV4_RE.sub(replace_ips, transformed)
    return transformed, timestamps, ipv4s, ts_placeholder, ip_placeholder


def restore_apache_text(
    transformed_text: str,
    timestamps: list[str],
    ipv4s: list[str],
    ts_placeholder: str,
    ip_placeholder: str,
) -> str:
    timestamp_iter = iter(timestamps)
    ip_iter = iter(ipv4s)
    pattern = re.compile(re.escape(ts_placeholder) + "|" + re.escape(ip_placeholder))

    def replace(match: re.Match[str]) -> str:
        token = match.group(0)
        if token == ts_placeholder:
            return next(timestamp_iter)
        return next(ip_iter)

    restored = pattern.sub(replace, transformed_text)
    try:
        next(timestamp_iter)
        raise ValueError("Unused timestamp values during restore")
    except StopIteration:
        pass
    try:
        next(ip_iter)
        raise ValueError("Unused IP values during restore")
    except StopIteration:
        pass
    return restored


def compress_file(
    input_path: str,
    output_dir: str,
    archive_path: str | None,
    numeric_radius: int,
    extract_timestamps: bool,
    extract_ips: bool,
) -> int:
    input_file = Path(input_path)
    output_root = Path(output_dir)
    if output_root.exists():
        shutil.rmtree(output_root)
    output_root.mkdir(parents=True)

    original_text = base.read_lossless_text(input_file)
    transformed_text, timestamps, ipv4s, ts_placeholder, ip_placeholder = transform_apache_text(
        original_text,
        extract_timestamps=extract_timestamps,
        extract_ips=extract_ips,
    )

    encoder = base.RankModelEncoder(numeric_radius=numeric_radius)
    encoder.encode_text(transformed_text)
    stats = encoder.save(input_file, output_root, archive_path=None)

    extra_dir = output_root / "apache_extract"
    extra_dir.mkdir()
    (extra_dir / "timestamps.bin").write_bytes(encode_timestamp_stream(timestamps))
    (extra_dir / "ipv4.bin").write_bytes(encode_ip_stream(ipv4s))

    metadata_path = output_root / "metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["apache_extract"] = {
        "timestamp_placeholder": ts_placeholder,
        "ip_placeholder": ip_placeholder,
        "timestamp_count": len(timestamps),
        "ip_count": len(ipv4s),
        "extract_timestamps": extract_timestamps,
        "extract_ips": extract_ips,
    }
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

    archive_size = None
    if archive_path:
        archive = Path(archive_path)
        if archive.exists():
            archive.unlink()
        with lzma.open(archive, "wb") as compressed_stream:
            with tarfile.open(fileobj=compressed_stream, mode="w") as tar_stream:
                tar_stream.add(output_root, arcname=output_root.name)
        archive_size = archive.stat().st_size

    encoded_size = sum(path.stat().st_size for path in output_root.rglob("*") if path.is_file())
    print(f"Original size        : {stats.original_size}")
    print(f"Transformed size     : {len(transformed_text.encode('latin-1'))}")
    print(f"Encoded directory    : {encoded_size}")
    if archive_size is not None:
        print(f"Archive size         : {archive_size}")
        print(f"Compression ratio    : {stats.original_size / archive_size:.4f}x")
    print(f"Timestamp matches    : {len(timestamps)}")
    print(f"IPv4 matches         : {len(ipv4s)}")
    print(f"Segments             : {stats.segments}")
    print(f"Shape literals       : {stats.shape_literals}")
    print(f"String literals      : {stats.string_literals}")
    print(f"Numeric literals     : {stats.numeric_literals}")
    print(f"Numeric patterns     : {stats.numeric_patterns}")
    return 0


def decompress_file(compressed_dir: str, output_path: str, original_path: str | None) -> int:
    compressed_root = Path(compressed_dir)
    metadata = json.loads((compressed_root / "metadata.json").read_text(encoding="utf-8"))
    extract_meta = metadata["apache_extract"]

    decoder = base.RankModelDecoder(compressed_root)
    transformed_text = decoder.decode_text()

    extra_dir = compressed_root / "apache_extract"
    timestamps = decode_timestamp_stream((extra_dir / "timestamps.bin").read_bytes())
    ipv4s = decode_ip_stream((extra_dir / "ipv4.bin").read_bytes())

    restored_text = restore_apache_text(
        transformed_text,
        timestamps,
        ipv4s,
        extract_meta["timestamp_placeholder"],
        extract_meta["ip_placeholder"],
    )
    base.write_lossless_text(output_path, restored_text)
    print(f"Decoded output       : {output_path}")
    if original_path is not None:
        original_text = base.read_lossless_text(original_path)
        exact_match = restored_text == original_text
        print(f"Exact match          : {'PASS' if exact_match else 'FAIL'}")
        return 0 if exact_match else 2
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apache regex extraction experiment over pare_rank_proto")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compress_parser = subparsers.add_parser("compress", help="Compress an Apache log")
    compress_parser.add_argument("input", help="Input file")
    compress_parser.add_argument("--output-dir", default="pare_rank_apache_extract_out", help="Output directory")
    compress_parser.add_argument("--archive", default="pare_rank_apache_extract.tar.xz", help="Archive path")
    compress_parser.add_argument("--numeric-radius", type=int, default=8, help="Numeric candidate radius")
    compress_parser.add_argument(
        "--mode",
        choices=["both", "ts-only", "ip-only"],
        default="both",
        help="Which Apache regex extractions to enable",
    )

    decompress_parser = subparsers.add_parser("decompress", help="Decompress an Apache archive")
    decompress_parser.add_argument("compressed_dir", help="Compressed directory")
    decompress_parser.add_argument("--output", default="pare_rank_apache_extract.decoded", help="Decoded file")
    decompress_parser.add_argument("--original", default=None, help="Optional original for exact verification")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command == "compress":
        archive_path = args.archive if args.archive else None
        return compress_file(
            args.input,
            args.output_dir,
            archive_path,
            args.numeric_radius,
            extract_timestamps=args.mode in {"both", "ts-only"},
            extract_ips=args.mode in {"both", "ip-only"},
        )
    return decompress_file(args.compressed_dir, args.output, args.original)


if __name__ == "__main__":
    raise SystemExit(main())
