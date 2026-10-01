#!/usr/bin/env python3
"""Bounded synthetic acceptance checks; never reads research datasets."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    root = args.output.resolve()
    root.mkdir(parents=True, exist_ok=False)
    script = Path(__file__).with_name("codec_baseline.py")
    spec = importlib.util.spec_from_file_location("codec_baseline", script)
    cb = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cb)
    fixtures = {
        "LF100001": (b"0000123 repeated log\r\x00payload\n" * 100001, [100000, 1], [True, True]),
        "Tail100001": (b"0000123 repeated log\r\x00payload\n" * 100000 + b"tail\r\x00without LF", [100000, 1], [True, False]),
        "Exact100000": (b"x\r\n" * 100000, [100000], [True]),
        "LongNoLF": (b"\r\x00xyz" * 500000, [1], [False]),
        "Empty": (b"", [], []),
    }
    command = [sys.executable, str(script), "--output", str(root / "runs"), "--workers", "2",
               "--trials", "1", "--keep-decoded", "--codecs", "gzip6", "xz6"]
    if shutil.which("zstd"):
        command += ["zstd3"]
    for name, (data, records, endings) in fixtures.items():
        path = root / (name + ".log")
        path.write_bytes(data)
        inv = cb.inventory(path)
        assert inv["raw_sha256"] == hashlib.sha256(data).hexdigest()
        assert [b["records"] for b in inv["blocks"]] == records
        assert [b["ends_with_lf"] for b in inv["blocks"]] == endings
        # Direct oracle: split on LF only, then group complete records by 100k.
        physical = data.split(b"\n")
        if physical[-1] == b"":
            physical.pop()
        oracle = []
        for i in range(0, len(physical), 100000):
            group = physical[i:i + 100000]
            raw = b"\n".join(group)
            if i + len(group) < len(physical) or data.endswith(b"\n"):
                raw += b"\n"
            oracle.append(hashlib.sha256(raw).hexdigest())
        assert [b["raw_sha256"] for b in inv["blocks"]] == oracle
        command += ["--input", name + "=" + str(path)]
    subprocess.run(command, check=True)
    results = json.loads((root / "runs" / "results.json").read_text())
    for result in results["runs"]:
        assert result["status"] == "PASS", result
        assert Path(result["decoded_file"]).read_bytes() == fixtures[result["dataset"]][0]
        assert sum(b["archive_bytes"] for b in result["blocks"]) == result["archive_bytes"]
    original_results = {str(p): p.read_bytes() for p in (root / "runs").glob("*/*/trial_*/attempt_*/result.json")}
    subprocess.run(command + ["--resume"], check=True)
    assert original_results == {str(p): p.read_bytes() for p in (root / "runs").glob("*/*/trial_*/attempt_*/result.json")}
    resumed = json.loads((root / "runs" / "results.json").read_text())
    assert all(r["reused_after_verification"] for r in resumed["runs"])
    # Existing output protection is a required provenance property.
    rejected = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert rejected.returncode != 0
    assert b"output already exists" in rejected.stderr
    # Tampered completed archive must fail resume instead of silently skipping.
    block = next(r for r in resumed["runs"] if r["blocks"])["blocks"][0]
    owner = next(r for r in resumed["runs"] if r["blocks"])
    archive = Path(owner["attempt"]) / block["archive"]
    original = archive.read_bytes()
    archive.write_bytes(original + b"tampered")
    rejected = subprocess.run(command + ["--resume"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    assert rejected.returncode != 0
    assert b"completed archive changed" in rejected.stderr
    archive.write_bytes(original)
    audit = {"status": "PASS", "runs": len(results["runs"]), "datasets": list(fixtures),
             "checks": ["100001-LF-records", "100001-records-unterminated-tail", "exact-100000-record-boundary",
                        "CRLF-and-NUL-preservation", "single-long-no-LF-record", "empty-file",
                        "archive-only-byte-exact-reconstruction", "complete-archive-byte-count",
                        "resume-verifies-without-overwrite", "refuse-existing-output", "detect-tampered-archive"],
             "script_sha256": cb.file_sha(script)}
    (root / "smoke_result.json").write_text(json.dumps(audit, indent=2) + "\n")
    print(json.dumps(audit))


if __name__ == "__main__":
    main()
