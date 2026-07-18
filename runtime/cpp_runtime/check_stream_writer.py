#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def build_tool() -> Path:
    compiler = os.environ.get("CXX") or shutil.which("clang++") or shutil.which("g++")
    if not compiler:
        raise RuntimeError("no C++ compiler found; set CXX or install clang++/g++")
    exe = ROOT / "stream_writer"
    subprocess.run(
        [
            compiler,
            "-std=c++17",
            "-O2",
            "-Wall",
            "-Wextra",
            "semzip_codecs.cpp",
            "stream_writer.cpp",
            "-o",
            str(exe),
        ],
        cwd=ROOT,
        check=True,
    )
    return exe


def main() -> int:
    import pare_dataset_extract as p
    import pare_rank_proto as base

    exe = build_tool()
    with tempfile.TemporaryDirectory(prefix="semzip_cpp_writer_") as tmp_name:
        tmp = Path(tmp_name)

        delta_values = ["10", "12", "7", "7", "1000", "-4"]
        delta_input = tmp / "delta.values.bin"
        delta_output = tmp / "D.delta.bin"
        delta_input.write_bytes(p.encode_string_stream_bytes(delta_values))
        subprocess.run([str(exe), "delta", str(delta_input), str(delta_output)], check=True)
        assert delta_output.read_bytes() == p.encode_delta_values([int(value) for value in delta_values])

        ip_values = ["10.0.0.1", "173.234.31.186", "255.255.255.255"]
        ip_input = tmp / "ip.values.bin"
        ip_output = tmp / "IP.ipv4.bin"
        ip_input.write_bytes(p.encode_string_stream_bytes(ip_values))
        subprocess.run([str(exe), "ipv4_plain", str(ip_input), str(ip_output)], check=True)
        assert ip_output.read_bytes() == p.encode_ipv4_plain(ip_values)

        string_values = ["alpha", "beta", "alpha", "gamma", "beta", "alpha"]
        mtf_input = tmp / "mtf.values.bin"
        rank_output = tmp / "S.rank.bin"
        literal_output = tmp / "S.literal.bin"
        mtf_input.write_bytes(p.encode_string_stream_bytes(string_values))
        subprocess.run([str(exe), "string_mtf_rank", str(mtf_input), str(rank_output), str(literal_output)], check=True)
        ranks, literals = p.encode_string_mtf_rank(string_values)
        assert rank_output.read_bytes() == b"".join(base.encode_varint(value) for value in ranks)
        assert literal_output.read_bytes() == p.encode_string_stream_bytes(literals)

    print("stream writer parity PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
