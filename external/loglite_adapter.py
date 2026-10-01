#!/usr/bin/env python3
"""Independent-block wrapper for unmodified official LogLite-B + XZ level 6.

Format: one byte (0 = missing final LF, 1 = final LF, 2 = empty input),
followed by an XZ stream containing the official LogLite-B archive. Empty
inputs use just flag 2 because the upstream empty-bitset decoder underflows.
All flags are counted as archive bytes. No per-dataset rule or shared state.
Fresh CLI processes reconstruct solely from this archive and fixed executables.
Upstream commit: 68f851ef673ac6fa45f26513df08613151624bd2.
"""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


FORMAT = "LogLite-BL-tail-v1"
UPSTREAM_COMMIT = "68f851ef673ac6fa45f26513df08613151624bd2"


def _env():
    env = dict(os.environ)
    # Pin the second-stage arguments rather than inheriting ambient overrides.
    env.pop("XZ_OPT", None)
    env.pop("XZ_DEFAULTS", None)
    return env


def _run(argv, timeout, stdout=subprocess.PIPE):
    result = subprocess.run(
        [str(arg) for arg in argv], stdout=stdout, stderr=subprocess.PIPE,
        timeout=timeout, env=_env(), check=False,
    )
    if result.returncode:
        raise RuntimeError(
            "Command failed (%s): %r\n%s" % (
                result.returncode, argv,
                result.stderr.decode("utf-8", errors="replace")[-8000:],
            )
        )
    return result.stdout.decode("utf-8", errors="replace") if result.stdout else ""


def encode_block(input_path, archive_path, executable, xz="xz", timeout=600):
    """Encode one original block; return diagnostics, never use printed rates."""
    input_path, archive_path = Path(input_path).resolve(), Path(archive_path).resolve()
    executable = str(Path(executable).resolve())
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    input_size = input_path.stat().st_size
    if input_size:
        with input_path.open("rb") as source:
            source.seek(-1, os.SEEK_END)
            flag = int(source.read(1) == b"\n")
    else:
        flag = 2
    with tempfile.TemporaryDirectory(prefix="loglite-encode-", dir=archive_path.parent) as temp:
        temp = Path(temp)
        archive = temp / "archive.bin"
        logs = ""
        with archive.open("wb") as output:
            output.write(bytes([flag]))
            if flag != 2:
                intermediate = temp / "official.lite"
                logs = _run([
                    executable, "--compress", "--file-path", input_path,
                    "--com-output-path", intermediate,
                ], timeout)
                if not intermediate.is_file():
                    raise RuntimeError("LogLite-B returned without creating an archive")
                output.flush()
                _run([xz, "-6", "-T1", "--check=crc64", "-c", str(intermediate)],
                     timeout, stdout=output)
        archive.replace(archive_path)
    return {"format": FORMAT, "input_bytes": input_size,
            "archive_bytes": archive_path.stat().st_size, "tail_flag": flag,
            "upstream_stdout": logs, "upstream_commit": UPSTREAM_COMMIT}


def decode_block(archive_path, output_path, executable, xz="xz", timeout=600):
    """Decode using the stored one-byte flag; raw input is never accessed."""
    archive_path, output_path = Path(archive_path).resolve(), Path(output_path).resolve()
    executable = str(Path(executable).resolve())
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="loglite-decode-", dir=output_path.parent) as temp:
        temp = Path(temp)
        decoded = temp / "decoded.bin"
        with archive_path.open("rb") as source:
            header = source.read(1)
            if header not in (b"\x00", b"\x01", b"\x02"):
                raise ValueError("Invalid LogLite-BL tail flag")
            flag = header[0]
            if flag == 2:
                if source.read(1):
                    raise ValueError("Unexpected payload in empty LogLite-BL archive")
                decoded.write_bytes(b"")
                logs = ""
            else:
                compressed = temp / "payload.xz"
                with compressed.open("wb") as target:
                    shutil.copyfileobj(source, target)
                intermediate = temp / "official.lite"
                with intermediate.open("wb") as target:
                    _run([xz, "-d", "-T1", "-c", str(compressed)], timeout, stdout=target)
                logs = _run([
                    executable, "--decompress", "--file-path", intermediate,
                    "--decom-output-path", decoded,
                ], timeout)
                if not decoded.is_file() or not decoded.stat().st_size:
                    raise RuntimeError("LogLite-B returned without decoded records")
                with decoded.open("r+b") as result:
                    result.seek(-1, os.SEEK_END)
                    if result.read(1) != b"\n":
                        raise RuntimeError("Expected the upstream appended final LF")
                    if flag == 0:
                        result.truncate(decoded.stat().st_size - 1)
        decoded.replace(output_path)
    return {"format": FORMAT, "output_bytes": output_path.stat().st_size,
            "tail_flag": flag, "upstream_stdout": logs,
            "upstream_commit": UPSTREAM_COMMIT}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("encode", "decode"))
    parser.add_argument("input")
    parser.add_argument("output")
    parser.add_argument("--executable", required=True)
    parser.add_argument("--xz", default="xz")
    parser.add_argument("--timeout", type=float, default=600)
    args = parser.parse_args()
    fn = encode_block if args.mode == "encode" else decode_block
    print(json.dumps(fn(args.input, args.output, args.executable, args.xz, args.timeout)))


if __name__ == "__main__":
    main()
