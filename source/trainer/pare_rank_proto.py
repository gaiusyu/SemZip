#!/usr/bin/env python3
"""Prototype for a rank-model family symbolic lossless codec.

Core philosophy:
    token -> choose a causal prediction model -> encode rank -> escape on miss

This prototype keeps the design simple but explicit:

* exact byte losslessness through whitespace-preserving tokenization
* shape stream to tell the decoder which model family to use
* shape-conditioned generic string models
* pattern-conditioned numeric models with predictive candidate generation
* literal escape streams only when rank misses
"""

from __future__ import annotations

import argparse
import json
import lzma
import re
import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path


TOKEN_OR_WS_RE = re.compile(r"\S+|\s+")
HEX_RE = re.compile(r"^[0-9a-f]+$")
NUMERIC_RE = re.compile(r"^(?=.*\d)[^A-Za-z]+$")

MAX_TABLE_SIZE = 512
LINE_POS_BUCKETS = 16

SHAPE_WS = 0
SHAPE_NUMERIC = 1
SHAPE_HEX = 2
SHAPE_WORD = 3
SHAPE_MIXED = 4
SHAPE_OTHER = 5
SHAPE_COUNT = 6

SHAPE_NAMES = {
    SHAPE_WS: "ws",
    SHAPE_NUMERIC: "numeric",
    SHAPE_HEX: "hex",
    SHAPE_WORD: "word",
    SHAPE_MIXED: "mixed",
    SHAPE_OTHER: "other",
}


def read_lossless_text(path: str | Path) -> str:
    return Path(path).read_bytes().decode("latin-1")


def write_lossless_text(path: str | Path, text: str) -> None:
    Path(path).write_bytes(text.encode("latin-1"))


def encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("varint only supports non-negative integers")
    out = bytearray()
    current = value
    while True:
        chunk = current & 0x7F
        current >>= 7
        if current:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def decode_varint(data: bytes, cursor: int) -> tuple[int, int]:
    shift = 0
    value = 0
    while True:
        if cursor >= len(data):
            raise ValueError("Truncated varint")
        byte = data[cursor]
        cursor += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, cursor
        shift += 7
        if shift > 63:
            raise ValueError("Varint too large")


def write_string_stream(path: Path, values: list[str]) -> None:
    data = bytearray()
    for value in values:
        encoded = value.encode("latin-1")
        data.extend(encode_varint(len(encoded)))
        data.extend(encoded)
    path.write_bytes(data)


def read_string_stream(path: Path) -> list[str]:
    if not path.exists():
        return []
    data = path.read_bytes()
    values: list[str] = []
    cursor = 0
    while cursor < len(data):
        size, cursor = decode_varint(data, cursor)
        end = cursor + size
        if end > len(data):
            raise ValueError(f"Corrupt string stream at {path}")
        values.append(data[cursor:end].decode("latin-1"))
        cursor = end
    return values


def write_varint_stream(path: Path, values: list[int]) -> None:
    data = bytearray()
    for value in values:
        data.extend(encode_varint(value))
    path.write_bytes(data)


def read_varint_stream(path: Path) -> list[int]:
    if not path.exists():
        return []
    data = path.read_bytes()
    values: list[int] = []
    cursor = 0
    while cursor < len(data):
        value, cursor = decode_varint(data, cursor)
        values.append(value)
    return values


def tokenize_losslessly(text: str) -> list[str]:
    return TOKEN_OR_WS_RE.findall(text)


def line_pos_bucket(line_pos: int) -> int:
    return min(line_pos, LINE_POS_BUCKETS - 1)


def classify_shape(token: str) -> int:
    if token.isspace():
        return SHAPE_WS
    if re.fullmatch(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", token) or (
        len(token) in {8, 12, 16, 32} and HEX_RE.fullmatch(token)
    ):
        return SHAPE_HEX
    if NUMERIC_RE.fullmatch(token):
        return SHAPE_NUMERIC
    if token.isalpha():
        return SHAPE_WORD
    if any(ch.isalpha() for ch in token) and any(ch.isdigit() for ch in token):
        return SHAPE_MIXED
    return SHAPE_OTHER


def numeric_pattern(token: str) -> str:
    parts = re.split(r"(\d+)", token)
    out: list[str] = []
    for part in parts:
        if part.isdigit():
            out.append(f"D{len(part)}")
        else:
            out.append(part)
    return "".join(out)


def extract_numeric_value(token: str) -> tuple[int, list[int], list[str]]:
    digit_runs = re.findall(r"\d+", token)
    if not digit_runs:
        raise ValueError(f"No digit runs in numeric token {token!r}")
    run_lengths = [len(run) for run in digit_runs]
    separators = re.split(r"\d+", token)
    return int("".join(digit_runs)), run_lengths, separators


def restore_numeric_token(value: int, run_lengths: list[int], separators: list[str]) -> str:
    total_digits = sum(run_lengths)
    digits = f"{value:0{total_digits}d}"
    cursor = 0
    pieces: list[str] = []
    for index, separator in enumerate(separators):
        pieces.append(separator)
        if index < len(run_lengths):
            run_length = run_lengths[index]
            pieces.append(digits[cursor:cursor + run_length])
            cursor += run_length
    return "".join(pieces)


def token_context_signature(token: str) -> str:
    shape = classify_shape(token)
    if shape == SHAPE_WORD and len(token) <= 16:
        return token.lower()
    if shape == SHAPE_HEX:
        return "[HEX]"
    if shape == SHAPE_NUMERIC:
        return "[NUM:" + numeric_pattern(token) + "]"
    if shape == SHAPE_WS:
        if "\n" in token:
            return "[NL]"
        return "[WS]"

    pieces: list[str] = []
    last_kind = ""
    run_length = 0

    def flush() -> None:
        nonlocal last_kind, run_length
        if not last_kind:
            return
        pieces.append(f"{last_kind}{run_length}")
        last_kind = ""
        run_length = 0

    for char in token[:32]:
        if char.isalpha():
            kind = "A"
        elif char.isdigit():
            kind = "D"
        else:
            kind = "S"
        if kind == last_kind:
            run_length += 1
        else:
            flush()
            last_kind = kind
            run_length = 1
    flush()
    return "[SIG:" + "|".join(pieces) + "]"


def move_to_front(table: list, index: int) -> None:
    if index <= 0:
        return
    value = table.pop(index)
    table.insert(0, value)


def insert_front_unique(table: list, value, max_size: int) -> None:
    try:
        index = table.index(value)
    except ValueError:
        table.insert(0, value)
    else:
        move_to_front(table, index)
    if len(table) > max_size:
        del table[max_size:]


@dataclass
class NumericPatternInfo:
    pattern: str
    run_lengths: list[int]
    separators: list[str]


@dataclass
class NumericState:
    last_value: int | None = None
    last_delta: int = 0
    recent_values: list[int] | None = None

    def __post_init__(self) -> None:
        if self.recent_values is None:
            self.recent_values = []


@dataclass
class CompressionStats:
    original_size: int
    encoded_size: int
    archive_size: int | None
    shape_literals: int
    string_literals: int
    numeric_literals: int
    numeric_patterns: int
    segments: int


class RankModelEncoder:
    def __init__(self, numeric_radius: int = 8, table_size: int = MAX_TABLE_SIZE) -> None:
        self.numeric_radius = numeric_radius
        self.table_size = table_size

        self.shape_tables: dict[str, list[int]] = {}
        self.shape_rank_stream: list[int] = []
        self.shape_literal_stream: list[int] = []

        self.string_tables: dict[str, list[str]] = {}
        self.string_rank_stream: list[int] = []
        self.string_literal_stream: list[str] = []

        self.numeric_pattern_tables: dict[str, list[int]] = {}
        self.numeric_pattern_rank_stream: list[int] = []
        self.numeric_literal_stream: list[str] = []
        self.numeric_rank_stream: list[int] = []
        self.numeric_pattern_infos: dict[int, NumericPatternInfo] = {}
        self.numeric_pattern_to_id: dict[str, int] = {}
        self.next_numeric_pattern_id = 1
        self.numeric_states: dict[tuple[str, int], NumericState] = {}

        self.prev_shape = SHAPE_WS
        self.prev_non_ws_sig = "[BOS]"
        self.line_pos = 0

    def encode_text(self, text: str) -> None:
        for segment in tokenize_losslessly(text):
            self.encode_segment(segment)

    def encode_segment(self, token: str) -> None:
        shape = classify_shape(token)
        self._encode_shape(shape)

        if shape == SHAPE_NUMERIC:
            self._encode_numeric(token, shape)
        else:
            self._encode_stringlike(token, shape)

        self._update_state_after_token(token, shape)

    def _shape_context(self) -> str:
        return f"{self.prev_shape}|{line_pos_bucket(self.line_pos)}"

    def _string_context(self, shape: int) -> str:
        return f"{shape}|{line_pos_bucket(self.line_pos)}|{self.prev_non_ws_sig}"

    def _numeric_context(self) -> str:
        return f"{line_pos_bucket(self.line_pos)}|{self.prev_non_ws_sig}"

    def _encode_shape(self, shape: int) -> None:
        table = self.shape_tables.setdefault(self._shape_context(), [])
        try:
            index = table.index(shape)
        except ValueError:
            self.shape_rank_stream.append(0)
            self.shape_literal_stream.append(shape)
            insert_front_unique(table, shape, SHAPE_COUNT)
        else:
            self.shape_rank_stream.append(index + 1)
            move_to_front(table, index)

    def _encode_stringlike(self, token: str, shape: int) -> None:
        table = self.string_tables.setdefault(self._string_context(shape), [])
        try:
            index = table.index(token)
        except ValueError:
            self.string_rank_stream.append(0)
            self.string_literal_stream.append(token)
            insert_front_unique(table, token, self.table_size)
        else:
            self.string_rank_stream.append(index + 1)
            move_to_front(table, index)

    def _encode_numeric(self, token: str, shape: int) -> None:
        del shape
        pattern_key = numeric_pattern(token)
        context_key = self._numeric_context()
        pattern_table = self.numeric_pattern_tables.setdefault(context_key, [])

        try:
            pattern_id = self.numeric_pattern_to_id[pattern_key]
        except KeyError:
            pattern_id = self.next_numeric_pattern_id
            self.next_numeric_pattern_id += 1
            value, run_lengths, separators = extract_numeric_value(token)
            self.numeric_pattern_to_id[pattern_key] = pattern_id
            self.numeric_pattern_infos[pattern_id] = NumericPatternInfo(
                pattern=pattern_key,
                run_lengths=run_lengths,
                separators=separators,
            )
            self.numeric_pattern_rank_stream.append(0)
            self.numeric_literal_stream.append(token)
            insert_front_unique(pattern_table, pattern_id, self.table_size)
            state = self.numeric_states.setdefault((context_key, pattern_id), NumericState())
            self._update_numeric_state_from_literal(state, value)
            return

        try:
            index = pattern_table.index(pattern_id)
        except ValueError:
            self.numeric_pattern_rank_stream.append(0)
            self.numeric_literal_stream.append(token)
            insert_front_unique(pattern_table, pattern_id, self.table_size)
            value, _, _ = extract_numeric_value(token)
            state = self.numeric_states.setdefault((context_key, pattern_id), NumericState())
            self._update_numeric_state_from_literal(state, value)
            return

        self.numeric_pattern_rank_stream.append(index + 1)
        move_to_front(pattern_table, index)

        info = self.numeric_pattern_infos[pattern_id]
        state = self.numeric_states.setdefault((context_key, pattern_id), NumericState())
        value, _, _ = extract_numeric_value(token)
        candidates = self._generate_numeric_candidates(info, state)

        try:
            candidate_index = candidates.index(value)
        except ValueError:
            self.numeric_rank_stream.append(0)
            self.numeric_literal_stream.append(token)
            self._update_numeric_state_from_literal(state, value)
            return

        self.numeric_rank_stream.append(candidate_index + 1)
        self._update_numeric_state_from_literal(state, value)

    def _generate_numeric_candidates(self, info: NumericPatternInfo, state: NumericState) -> list[int]:
        candidates: list[int] = []
        seen: set[int] = set()

        def add(value: int | None) -> None:
            if value is None:
                return
            if value < 0:
                return
            if value in seen:
                return
            seen.add(value)
            candidates.append(value)

        last_value = state.last_value
        predicted = None if last_value is None else last_value + state.last_delta

        add(predicted)
        add(last_value)

        if predicted is not None:
            for delta in range(1, self.numeric_radius + 1):
                add(predicted + delta)
                add(predicted - delta)

        if last_value is not None:
            for delta in range(1, max(2, self.numeric_radius // 2) + 1):
                add(last_value + delta)
                add(last_value - delta)

        for recent_value in state.recent_values:
            add(recent_value)

        return candidates

    def _update_numeric_state_from_literal(self, state: NumericState, value: int) -> None:
        if state.last_value is not None:
            state.last_delta = value - state.last_value
        state.last_value = value
        insert_front_unique(state.recent_values, value, 16)

    def _update_state_after_token(self, token: str, shape: int) -> None:
        self.prev_shape = shape
        if shape == SHAPE_WS:
            if "\n" in token:
                self.line_pos = 0
            return
        self.prev_non_ws_sig = token_context_signature(token)
        self.line_pos += 1

    def save(
        self,
        input_path: str | Path,
        output_dir: str | Path,
        archive_path: str | Path | None = None,
    ) -> CompressionStats:
        input_path = Path(input_path)
        output_dir = Path(output_dir)
        if output_dir.exists():
            shutil.rmtree(output_dir)
        streams_dir = output_dir / "streams"
        streams_dir.mkdir(parents=True)

        write_varint_stream(streams_dir / "shape_rank.bin", self.shape_rank_stream)
        (streams_dir / "shape_literal.bin").write_bytes(bytes(self.shape_literal_stream))
        write_varint_stream(streams_dir / "string_rank.bin", self.string_rank_stream)
        write_string_stream(streams_dir / "string_literal.bin", self.string_literal_stream)
        write_varint_stream(streams_dir / "numeric_pattern_rank.bin", self.numeric_pattern_rank_stream)
        write_varint_stream(streams_dir / "numeric_rank.bin", self.numeric_rank_stream)
        write_string_stream(streams_dir / "numeric_literal.bin", self.numeric_literal_stream)

        pattern_payload = {
            str(pattern_id): {
                "pattern": info.pattern,
                "run_lengths": info.run_lengths,
                "separators": info.separators,
            }
            for pattern_id, info in self.numeric_pattern_infos.items()
        }
        (streams_dir / "numeric_patterns.json").write_text(
            json.dumps(pattern_payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        metadata = {
            "version": 1,
            "input_file": input_path.name,
            "original_size": input_path.stat().st_size,
            "numeric_radius": self.numeric_radius,
            "table_size": self.table_size,
            "shape_count": SHAPE_COUNT,
            "shape_names": SHAPE_NAMES,
            "numeric_pattern_count": len(self.numeric_pattern_infos),
        }
        (output_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True),
            encoding="utf-8",
        )

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
        return CompressionStats(
            original_size=input_path.stat().st_size,
            encoded_size=encoded_size,
            archive_size=archive_size,
            shape_literals=len(self.shape_literal_stream),
            string_literals=len(self.string_literal_stream),
            numeric_literals=len(self.numeric_literal_stream),
            numeric_patterns=len(self.numeric_pattern_infos),
            segments=len(self.shape_rank_stream),
        )

    def compress_file(
        self,
        input_path: str | Path,
        output_dir: str | Path,
        archive_path: str | Path | None = None,
    ) -> CompressionStats:
        input_path = Path(input_path)
        self.encode_text(read_lossless_text(input_path))
        return self.save(input_path, output_dir, archive_path)


class RankModelDecoder:
    def __init__(self, compressed_dir: str | Path) -> None:
        self.compressed_dir = Path(compressed_dir)
        self.metadata = json.loads((self.compressed_dir / "metadata.json").read_text(encoding="utf-8"))
        if self.metadata.get("version") != 1:
            raise ValueError("Unsupported prototype archive version")

        streams_dir = self.compressed_dir / "streams"
        self.shape_rank_stream = read_varint_stream(streams_dir / "shape_rank.bin")
        self.shape_literal_stream = list((streams_dir / "shape_literal.bin").read_bytes())
        self.string_rank_stream = read_varint_stream(streams_dir / "string_rank.bin")
        self.string_literal_stream = read_string_stream(streams_dir / "string_literal.bin")
        self.numeric_pattern_rank_stream = read_varint_stream(streams_dir / "numeric_pattern_rank.bin")
        self.numeric_rank_stream = read_varint_stream(streams_dir / "numeric_rank.bin")
        self.numeric_literal_stream = read_string_stream(streams_dir / "numeric_literal.bin")

        raw_patterns = json.loads((streams_dir / "numeric_patterns.json").read_text(encoding="utf-8"))
        self.numeric_pattern_infos: dict[int, NumericPatternInfo] = {
            int(pattern_id): NumericPatternInfo(
                pattern=payload["pattern"],
                run_lengths=list(payload["run_lengths"]),
                separators=list(payload["separators"]),
            )
            for pattern_id, payload in raw_patterns.items()
        }

        self.shape_tables: dict[str, list[int]] = {}
        self.string_tables: dict[str, list[str]] = {}
        self.numeric_pattern_tables: dict[str, list[int]] = {}
        self.numeric_states: dict[tuple[str, int], NumericState] = {}

        self.shape_rank_index = 0
        self.shape_literal_index = 0
        self.string_rank_index = 0
        self.string_literal_index = 0
        self.numeric_pattern_rank_index = 0
        self.numeric_rank_index = 0
        self.numeric_literal_index = 0

        self.prev_shape = SHAPE_WS
        self.prev_non_ws_sig = "[BOS]"
        self.line_pos = 0

    def _pop_varint(self, values: list[int], index_name: str) -> int:
        index = getattr(self, index_name)
        if index >= len(values):
            raise ValueError(f"Stream exhausted for {index_name}")
        value = values[index]
        setattr(self, index_name, index + 1)
        return value

    def _pop_string(self, values: list[str], index_name: str) -> str:
        index = getattr(self, index_name)
        if index >= len(values):
            raise ValueError(f"Stream exhausted for {index_name}")
        value = values[index]
        setattr(self, index_name, index + 1)
        return value

    def _shape_context(self) -> str:
        return f"{self.prev_shape}|{line_pos_bucket(self.line_pos)}"

    def _string_context(self, shape: int) -> str:
        return f"{shape}|{line_pos_bucket(self.line_pos)}|{self.prev_non_ws_sig}"

    def _numeric_context(self) -> str:
        return f"{line_pos_bucket(self.line_pos)}|{self.prev_non_ws_sig}"

    def decode_text(self) -> str:
        pieces: list[str] = []
        while self.shape_rank_index < len(self.shape_rank_stream):
            shape = self._decode_shape()
            if shape == SHAPE_NUMERIC:
                token = self._decode_numeric(shape)
            else:
                token = self._decode_stringlike(shape)
            pieces.append(token)
            self._update_state_after_token(token, shape)
        return "".join(pieces)

    def _decode_shape(self) -> int:
        table = self.shape_tables.setdefault(self._shape_context(), [])
        rank = self._pop_varint(self.shape_rank_stream, "shape_rank_index")
        if rank == 0:
            if self.shape_literal_index >= len(self.shape_literal_stream):
                raise ValueError("Shape literal stream exhausted")
            shape = self.shape_literal_stream[self.shape_literal_index]
            self.shape_literal_index += 1
            insert_front_unique(table, shape, SHAPE_COUNT)
            return shape
        index = rank - 1
        if index < 0 or index >= len(table):
            raise ValueError(f"Bad shape rank {rank}")
        shape = table[index]
        move_to_front(table, index)
        return shape

    def _decode_stringlike(self, shape: int) -> str:
        table = self.string_tables.setdefault(self._string_context(shape), [])
        rank = self._pop_varint(self.string_rank_stream, "string_rank_index")
        if rank == 0:
            token = self._pop_string(self.string_literal_stream, "string_literal_index")
            insert_front_unique(table, token, MAX_TABLE_SIZE)
            return token
        index = rank - 1
        if index < 0 or index >= len(table):
            raise ValueError(f"Bad string rank {rank}")
        token = table[index]
        move_to_front(table, index)
        return token

    def _decode_numeric(self, shape: int) -> str:
        del shape
        context_key = self._numeric_context()
        pattern_table = self.numeric_pattern_tables.setdefault(context_key, [])
        pattern_rank = self._pop_varint(self.numeric_pattern_rank_stream, "numeric_pattern_rank_index")

        if pattern_rank == 0:
            token = self._pop_string(self.numeric_literal_stream, "numeric_literal_index")
            pattern_key = numeric_pattern(token)
            pattern_id = None
            for existing_id, info in self.numeric_pattern_infos.items():
                if info.pattern == pattern_key:
                    pattern_id = existing_id
                    break
            if pattern_id is None:
                pattern_id = max(self.numeric_pattern_infos.keys(), default=0) + 1
                value, run_lengths, separators = extract_numeric_value(token)
                self.numeric_pattern_infos[pattern_id] = NumericPatternInfo(
                    pattern=pattern_key,
                    run_lengths=run_lengths,
                    separators=separators,
                )
            insert_front_unique(pattern_table, pattern_id, MAX_TABLE_SIZE)
            value, _, _ = extract_numeric_value(token)
            state = self.numeric_states.setdefault((context_key, pattern_id), NumericState())
            self._update_numeric_state_from_literal(state, value)
            return token

        pattern_index = pattern_rank - 1
        if pattern_index < 0 or pattern_index >= len(pattern_table):
            raise ValueError(f"Bad numeric pattern rank {pattern_rank}")
        pattern_id = pattern_table[pattern_index]
        move_to_front(pattern_table, pattern_index)

        info = self.numeric_pattern_infos[pattern_id]
        state = self.numeric_states.setdefault((context_key, pattern_id), NumericState())
        rank = self._pop_varint(self.numeric_rank_stream, "numeric_rank_index")
        if rank == 0:
            token = self._pop_string(self.numeric_literal_stream, "numeric_literal_index")
            value, _, _ = extract_numeric_value(token)
            self._update_numeric_state_from_literal(state, value)
            return token

        candidates = self._generate_numeric_candidates(info, state)
        index = rank - 1
        if index < 0 or index >= len(candidates):
            raise ValueError(f"Bad numeric rank {rank}")
        value = candidates[index]
        token = restore_numeric_token(value, info.run_lengths, info.separators)
        self._update_numeric_state_from_literal(state, value)
        return token

    def _generate_numeric_candidates(self, info: NumericPatternInfo, state: NumericState) -> list[int]:
        del info
        candidates: list[int] = []
        seen: set[int] = set()

        def add(value: int | None) -> None:
            if value is None:
                return
            if value < 0:
                return
            if value in seen:
                return
            seen.add(value)
            candidates.append(value)

        last_value = state.last_value
        predicted = None if last_value is None else last_value + state.last_delta

        add(predicted)
        add(last_value)

        if predicted is not None:
            for delta in range(1, self.metadata["numeric_radius"] + 1):
                add(predicted + delta)
                add(predicted - delta)

        if last_value is not None:
            for delta in range(1, max(2, self.metadata["numeric_radius"] // 2) + 1):
                add(last_value + delta)
                add(last_value - delta)

        for recent_value in state.recent_values:
            add(recent_value)

        return candidates

    def _update_numeric_state_from_literal(self, state: NumericState, value: int) -> None:
        if state.last_value is not None:
            state.last_delta = value - state.last_value
        state.last_value = value
        insert_front_unique(state.recent_values, value, 16)

    def _update_state_after_token(self, token: str, shape: int) -> None:
        self.prev_shape = shape
        if shape == SHAPE_WS:
            if "\n" in token:
                self.line_pos = 0
            return
        self.prev_non_ws_sig = token_context_signature(token)
        self.line_pos += 1

    def decode_to_file(self, output_path: str | Path) -> None:
        write_lossless_text(output_path, self.decode_text())


def compress_cli(input_path: str, output_dir: str, archive_path: str | None, numeric_radius: int) -> int:
    encoder = RankModelEncoder(numeric_radius=numeric_radius)
    stats = encoder.compress_file(input_path, output_dir, archive_path)
    print(f"Original size        : {stats.original_size}")
    print(f"Encoded directory    : {stats.encoded_size}")
    if stats.archive_size is not None:
        print(f"Archive size         : {stats.archive_size}")
        print(f"Compression ratio    : {stats.original_size / stats.archive_size:.4f}x")
    print(f"Segments             : {stats.segments}")
    print(f"Shape literals       : {stats.shape_literals}")
    print(f"String literals      : {stats.string_literals}")
    print(f"Numeric literals     : {stats.numeric_literals}")
    print(f"Numeric patterns     : {stats.numeric_patterns}")
    return 0


def decompress_cli(compressed_dir: str, output_path: str, original_path: str | None) -> int:
    decoder = RankModelDecoder(compressed_dir)
    decoded_text = decoder.decode_text()
    write_lossless_text(output_path, decoded_text)
    print(f"Decoded output       : {output_path}")
    if original_path is not None:
        original_text = read_lossless_text(original_path)
        exact_match = decoded_text == original_text
        print(f"Exact match          : {'PASS' if exact_match else 'FAIL'}")
        return 0 if exact_match else 2
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rank-model symbolic lossless codec prototype")
    subparsers = parser.add_subparsers(dest="command", required=True)

    compress_parser = subparsers.add_parser("compress", help="Compress a file")
    compress_parser.add_argument("input", help="Input file")
    compress_parser.add_argument("--output-dir", default="pare_rank_proto_out", help="Output directory")
    compress_parser.add_argument("--archive", default="pare_rank_proto.tar.xz", help="Archive path")
    compress_parser.add_argument("--numeric-radius", type=int, default=8, help="Numeric candidate radius")

    decompress_parser = subparsers.add_parser("decompress", help="Decompress a directory")
    decompress_parser.add_argument("compressed_dir", help="Compressed directory")
    decompress_parser.add_argument("--output", default="pare_rank_proto.decoded", help="Decoded file")
    decompress_parser.add_argument("--original", default=None, help="Optional original for exact verification")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command == "compress":
        archive_path = args.archive if args.archive else None
        return compress_cli(args.input, args.output_dir, archive_path, args.numeric_radius)
    return decompress_cli(args.compressed_dir, args.output, args.original)


if __name__ == "__main__":
    raise SystemExit(main())
