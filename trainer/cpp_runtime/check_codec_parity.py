#!/usr/bin/env python3
from __future__ import annotations

import subprocess
import os
import shutil
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PY_ROOT = ROOT.parent


def main() -> int:
    import pare_dataset_extract as p
    import pare_rank_proto as base

    numbers = [100, 101, 99, 105, -3, -3, 5000]
    ips = ["10.0.0.1", "173.234.31.186", "255.255.255.255"]
    strings = ["alpha", "beta", "alpha", "gamma", "beta", "alpha"]
    ranks, literals = p.encode_string_mtf_rank(strings)
    expected = {
        "delta_hex": p.encode_delta_values(numbers).hex(),
        "ipv4_hex": p.encode_ipv4_plain(ips).hex(),
        "string_stream_hex": p.encode_string_stream_bytes(strings).hex(),
        "mtf_rank_hex": b"".join(base.encode_varint(v) for v in ranks).hex(),
        "mtf_literal_hex": p.encode_string_stream_bytes(literals).hex(),
    }
    exe = ROOT / "codec_vectors"
    compiler = os.environ.get("CXX") or shutil.which("clang++") or shutil.which("g++")
    if not compiler:
        raise RuntimeError("no C++ compiler found; set CXX or install clang++/g++")
    subprocess.run(
        [compiler, "-std=c++17", "-O2", "-Wall", "-Wextra", "semzip_codecs.cpp", "codec_vectors.cpp", "-o", str(exe)],
        cwd=ROOT,
        check=True,
    )
    actual = {}
    for line in subprocess.check_output([str(exe)], text=True).splitlines():
        key, value = line.split("=", 1)
        actual[key] = value
    for key, value in expected.items():
        if actual.get(key) != value:
            raise AssertionError(f"{key} mismatch\npython={value}\ncpp={actual.get(key)}")
    print("codec parity PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
