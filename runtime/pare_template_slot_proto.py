#!/usr/bin/env python3
"""Template-slot context experiment for the PARE rank-model prototype.

This is a deliberately low-risk variant of ``pare_rank_proto``.  It preserves
the same lossless rank/literal streams, but changes how a token chooses its
predictor:

    previous token / line position  ->  current line template / slot position

Each line first emits a template rank.  A template miss stores a compact
signature literal; references use an adaptive move-to-front table.  After the
decoder sees the line template, every segment in the line uses
``(template_id, slot_index)`` to select the shape/string/numeric predictor.
"""

from __future__ import annotations

import argparse
import json
import lzma
import shutil
import tarfile
from pathlib import Path

import pare_rank_proto as base
import pare_rank_proto_apache_extract as apache_extract


TEMPLATE_SEP = "\x1f"


def split_lines_losslessly(text: str) -> list[str]:
    if not text:
        return []
    return text.splitlines(keepends=True)


def segment_template_signature(segment: str) -> str:
    shape = base.classify_shape(segment)
    return f"{shape}:{base.token_context_signature(segment)}"


def build_line_template_key(line_text: str) -> str:
    segments = base.tokenize_losslessly(line_text)
    if not segments:
        return "[EMPTY]"
    return TEMPLATE_SEP.join(segment_template_signature(segment) for segment in segments)


class TemplateSlotRankModelEncoder(base.RankModelEncoder):
    def __init__(self, numeric_radius: int = 8, table_size: int = base.MAX_TABLE_SIZE) -> None:
        super().__init__(numeric_radius=numeric_radius, table_size=table_size)
        self.template_rank_stream: list[int] = []
        self.template_literal_stream: list[str] = []
        self.template_table: list[int] = []
        self.template_to_id: dict[str, int] = {}
        self.next_template_id = 1
        self.current_template_id = 0
        self.slot_index = 0

    def encode_text(self, text: str) -> None:
        for line_text in split_lines_losslessly(text):
            self._encode_line_template(build_line_template_key(line_text))
            self.slot_index = 0
            for segment in base.tokenize_losslessly(line_text):
                self.encode_segment(segment)

    def _encode_line_template(self, template_key: str) -> None:
        template_id = self.template_to_id.get(template_key)
        if template_id is None:
            template_id = self.next_template_id
            self.next_template_id += 1
            self.template_to_id[template_key] = template_id
            self.template_rank_stream.append(0)
            self.template_literal_stream.append(template_key)
            base.insert_front_unique(self.template_table, template_id, self.table_size)
        else:
            try:
                index = self.template_table.index(template_id)
            except ValueError:
                self.template_rank_stream.append(0)
                self.template_literal_stream.append(template_key)
                base.insert_front_unique(self.template_table, template_id, self.table_size)
            else:
                self.template_rank_stream.append(index + 1)
                base.move_to_front(self.template_table, index)
        self.current_template_id = template_id

    def _shape_context(self) -> str:
        return f"T{self.current_template_id}|S{self.slot_index}"

    def _string_context(self, shape: int) -> str:
        return f"T{self.current_template_id}|S{self.slot_index}|H{shape}"

    def _numeric_context(self) -> str:
        return f"T{self.current_template_id}|S{self.slot_index}"

    def _update_state_after_token(self, token: str, shape: int) -> None:
        super()._update_state_after_token(token, shape)
        self.slot_index += 1

    def save(
        self,
        input_path: str | Path,
        output_dir: str | Path,
        archive_path: str | Path | None = None,
    ) -> base.CompressionStats:
        stats = super().save(input_path, output_dir, archive_path=None)
        output_dir = Path(output_dir)
        streams_dir = output_dir / "streams"
        base.write_varint_stream(streams_dir / "template_rank.bin", self.template_rank_stream)
        base.write_string_stream(streams_dir / "template_literal.bin", self.template_literal_stream)

        metadata_path = output_dir / "metadata.json"
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata["context_mode"] = "template_slot_v1"
        metadata["template_count"] = len(self.template_to_id)
        metadata["template_table_size"] = self.table_size
        metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")

        archive_size = None
        if archive_path is not None:
            archive_path = Path(archive_path)
            if archive_path.exists():
                archive_path.unlink()
            with lzma.open(archive_path, "wb") as compressed_stream:
                with tarfile.open(fileobj=compressed_stream, mode="w") as tar_stream:
                    tar_stream.add(output_dir, arcname=output_dir.name)
            archive_size = archive_path.stat().st_size

        encoded_size = sum(path.stat().st_size for path in output_dir.rglob("*") if path.is_file())
        return base.CompressionStats(
            original_size=stats.original_size,
            encoded_size=encoded_size,
            archive_size=archive_size,
            shape_literals=stats.shape_literals,
            string_literals=stats.string_literals,
            numeric_literals=stats.numeric_literals,
            numeric_patterns=stats.numeric_patterns,
            segments=stats.segments,
        )


class TemplateSlotRankModelDecoder(base.RankModelDecoder):
    def __init__(self, compressed_dir: str | Path) -> None:
        super().__init__(compressed_dir)
        streams_dir = self.compressed_dir / "streams"
        self.template_rank_stream = base.read_varint_stream(streams_dir / "template_rank.bin")
        self.template_literal_stream = base.read_string_stream(streams_dir / "template_literal.bin")
        self.template_rank_index = 0
        self.template_literal_index = 0
        self.template_table: list[int] = []
        self.template_to_id: dict[str, int] = {}
        self.next_template_id = 1
        self.current_template_id = 0
        self.slot_index = 0

    def _decode_line_template(self) -> None:
        rank = self._pop_varint(self.template_rank_stream, "template_rank_index")
        if rank == 0:
            template_key = self._pop_string(self.template_literal_stream, "template_literal_index")
            template_id = self.template_to_id.get(template_key)
            if template_id is None:
                template_id = self.next_template_id
                self.next_template_id += 1
                self.template_to_id[template_key] = template_id
            base.insert_front_unique(self.template_table, template_id, self.metadata["table_size"])
        else:
            index = rank - 1
            if index < 0 or index >= len(self.template_table):
                raise ValueError(f"Bad template rank {rank}")
            template_id = self.template_table[index]
            base.move_to_front(self.template_table, index)
        self.current_template_id = template_id
        self.slot_index = 0

    def _shape_context(self) -> str:
        return f"T{self.current_template_id}|S{self.slot_index}"

    def _string_context(self, shape: int) -> str:
        return f"T{self.current_template_id}|S{self.slot_index}|H{shape}"

    def _numeric_context(self) -> str:
        return f"T{self.current_template_id}|S{self.slot_index}"

    def _update_state_after_token(self, token: str, shape: int) -> None:
        super()._update_state_after_token(token, shape)
        self.slot_index += 1

    def decode_text(self) -> str:
        pieces: list[str] = []
        while self.shape_rank_index < len(self.shape_rank_stream):
            self._decode_line_template()
            while self.shape_rank_index < len(self.shape_rank_stream):
                shape = self._decode_shape()
                if shape == base.SHAPE_NUMERIC:
                    token = self._decode_numeric(shape)
                else:
                    token = self._decode_stringlike(shape)
                pieces.append(token)
                self._update_state_after_token(token, shape)
                if "\n" in token:
                    break
        return "".join(pieces)


def create_archive(output_dir: str | Path, archive_path: str | Path) -> int:
    output_dir = Path(output_dir)
    archive_path = Path(archive_path)
    if archive_path.exists():
        archive_path.unlink()
    with lzma.open(archive_path, "wb") as compressed_stream:
        with tarfile.open(fileobj=compressed_stream, mode="w") as tar_stream:
            tar_stream.add(output_dir, arcname=output_dir.name)
    return archive_path.stat().st_size


def compress_file(
    input_path: str,
    output_dir: str,
    archive_path: str | None,
    numeric_radius: int,
    apache_mode: str,
) -> int:
    input_file = Path(input_path)
    output_root = Path(output_dir)
    if output_root.exists():
        shutil.rmtree(output_root)

    original_text = base.read_lossless_text(input_file)
    if apache_mode == "none":
        transformed_text = original_text
        timestamps: list[str] = []
        ipv4s: list[str] = []
        ts_placeholder = ""
        ip_placeholder = ""
        extract_timestamps = False
        extract_ips = False
    else:
        extract_timestamps = apache_mode in {"both", "ts-only"}
        extract_ips = apache_mode in {"both", "ip-only"}
        transformed_text, timestamps, ipv4s, ts_placeholder, ip_placeholder = apache_extract.transform_apache_text(
            original_text,
            extract_timestamps=extract_timestamps,
            extract_ips=extract_ips,
        )

    encoder = TemplateSlotRankModelEncoder(numeric_radius=numeric_radius)
    encoder.encode_text(transformed_text)
    stats = encoder.save(input_file, output_root, archive_path=None)

    if apache_mode != "none":
        extra_dir = output_root / "apache_extract"
        extra_dir.mkdir()
        (extra_dir / "timestamps.bin").write_bytes(apache_extract.encode_timestamp_stream(timestamps))
        (extra_dir / "ipv4.bin").write_bytes(apache_extract.encode_ip_stream(ipv4s))

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

    archive_size = create_archive(output_root, archive_path) if archive_path else None
    encoded_size = sum(path.stat().st_size for path in output_root.rglob("*") if path.is_file())
    print(f"Original size        : {stats.original_size}")
    print(f"Transformed size     : {len(transformed_text.encode('latin-1'))}")
    print(f"Encoded directory    : {encoded_size}")
    if archive_size is not None:
        print(f"Archive size         : {archive_size}")
        print(f"Compression ratio    : {stats.original_size / archive_size:.4f}x")
    print(f"Templates            : {len(encoder.template_to_id)}")
    print(f"Segments             : {stats.segments}")
    print(f"Shape literals       : {stats.shape_literals}")
    print(f"String literals      : {stats.string_literals}")
    print(f"Numeric literals     : {stats.numeric_literals}")
    print(f"Numeric patterns     : {stats.numeric_patterns}")
    if apache_mode != "none":
        print(f"Timestamp matches    : {len(timestamps)}")
        print(f"IPv4 matches         : {len(ipv4s)}")
    return 0


def decompress_file(compressed_dir: str, output_path: str, original_path: str | None) -> int:
    compressed_root = Path(compressed_dir)
    metadata = json.loads((compressed_root / "metadata.json").read_text(encoding="utf-8"))
    decoder = TemplateSlotRankModelDecoder(compressed_root)
    transformed_text = decoder.decode_text()

    if "apache_extract" in metadata:
        extract_meta = metadata["apache_extract"]
        extra_dir = compressed_root / "apache_extract"
        timestamps = apache_extract.decode_timestamp_stream((extra_dir / "timestamps.bin").read_bytes())
        ipv4s = apache_extract.decode_ip_stream((extra_dir / "ipv4.bin").read_bytes())
        decoded_text = apache_extract.restore_apache_text(
            transformed_text,
            timestamps,
            ipv4s,
            extract_meta["timestamp_placeholder"],
            extract_meta["ip_placeholder"],
        )
    else:
        decoded_text = transformed_text

    base.write_lossless_text(output_path, decoded_text)
    print(f"Decoded output       : {output_path}")
    if original_path is not None:
        original_text = base.read_lossless_text(original_path)
        exact_match = decoded_text == original_text
        print(f"Exact match          : {'PASS' if exact_match else 'FAIL'}")
        return 0 if exact_match else 2
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Template-slot PARE rank-model experiment")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compress_parser = subparsers.add_parser("compress", help="Compress a log file")
    compress_parser.add_argument("input", help="Input file")
    compress_parser.add_argument("--output-dir", default="pare_template_slot_out", help="Output directory")
    compress_parser.add_argument("--archive", default="pare_template_slot.tar.xz", help="Archive path")
    compress_parser.add_argument("--numeric-radius", type=int, default=8, help="Numeric candidate radius")
    compress_parser.add_argument(
        "--apache-mode",
        choices=["none", "both", "ts-only", "ip-only"],
        default="none",
        help="Optional Apache timestamp/IP extraction mode",
    )

    decompress_parser = subparsers.add_parser("decompress", help="Decompress a compressed directory")
    decompress_parser.add_argument("compressed_dir", help="Compressed directory")
    decompress_parser.add_argument("--output", default="pare_template_slot.decoded", help="Decoded file")
    decompress_parser.add_argument("--original", default=None, help="Optional original for exact verification")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command == "compress":
        archive_path = args.archive if args.archive else None
        return compress_file(args.input, args.output_dir, archive_path, args.numeric_radius, args.apache_mode)
    return decompress_file(args.compressed_dir, args.output, args.original)


if __name__ == "__main__":
    raise SystemExit(main())
