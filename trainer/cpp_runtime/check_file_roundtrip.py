#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PY_ROOT = ROOT.parent


def build_tool() -> Path:
    compiler = os.environ.get("CXX") or shutil.which("clang++") or shutil.which("g++")
    if not compiler:
        raise RuntimeError("no C++ compiler found; set CXX or install clang++/g++")
    exe = ROOT / "stream_roundtrip"
    subprocess.run(
        [
            compiler,
            "-std=c++17",
            "-O2",
            "-Wall",
            "-Wextra",
            "semzip_codecs.cpp",
            "stream_roundtrip.cpp",
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
    with tempfile.TemporaryDirectory(prefix="semzip_cpp_roundtrip_") as tmp_name:
        tmp = Path(tmp_name)

        delta_file = tmp / "D.delta.bin"
        delta_file.write_bytes(p.encode_delta_values([10, 12, 7, 7, 1000, -4]))
        subprocess.run([str(exe), "delta", str(delta_file)], check=True)

        ip_file = tmp / "IP.ipv4.bin"
        ip_file.write_bytes(p.encode_ipv4_plain(["10.0.0.1", "173.234.31.186", "255.255.255.255"]))
        subprocess.run([str(exe), "ipv4_plain", str(ip_file)], check=True)

        string_file = tmp / "S.strings.bin"
        string_file.write_bytes(p.encode_string_stream_bytes(["/var/www/a.php", "alpha", "", "beta beta"]))
        subprocess.run([str(exe), "string", str(string_file)], check=True)

        ranks, literals = p.encode_string_mtf_rank(["alpha", "beta", "alpha", "gamma", "beta", "alpha"])
        rank_file = tmp / "M.rank.bin"
        literal_file = tmp / "M.literal.bin"
        rank_file.write_bytes(b"".join(base.encode_varint(value) for value in ranks))
        literal_file.write_bytes(p.encode_string_stream_bytes(literals))
        subprocess.run([str(exe), "string_mtf_rank", str(rank_file), str(literal_file)], check=True)

    print("file roundtrip PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
