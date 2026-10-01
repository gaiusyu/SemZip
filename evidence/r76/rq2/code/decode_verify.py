#!/usr/bin/env python3
"""R76 B3: independent archive-only decode of one branch archive directory.

Runs in a fresh process after encoding. Reads ONLY the branch archive directory
(semantic_manifest.json, semantic/block_*.semantic.tar.xz, chunk_<i>.tar.xz) and
the unchanged runtime decoder: native `decompress` for the residual, then the
production semantic_codec_frontend._decode_block (extract archive, activate the
archived execution environment, restore_text). No plan, trace, or storage policy
is read. Data caches are cleared at every block entry/exit (R71 boundary).
Decoded bytes are compared per block with the expected SHA-256 recorded by the
encoder (blocks.json, itself checked against the R68/R73 inventories) and the
concatenation with the processed-block SHA in result.json.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

import representation_control_v2 as v2


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--dataset-dir", type=Path, required=True)
    p.add_argument("--branch", required=True, choices=v2.BRANCHES)
    args = p.parse_args()
    source = args.source.resolve()
    modules = v2.load_runtime(source)
    front = modules[2]
    ddir = args.dataset_dir.resolve()
    archive = ddir / args.branch / "archive"
    manifest = json.loads((archive / "semantic_manifest.json").read_text())
    expected_rows = {r["index"]: r for r in json.loads((ddir / "blocks.json").read_text())}
    expected_full = json.loads((ddir / "result.json").read_text())["raw_sha256_of_processed_blocks"]
    full = hashlib.sha256()
    rows = []
    ok = True
    with tempfile.TemporaryDirectory(prefix="r76-indep-decode-") as name:
        work = Path(name)
        for sem_name in manifest["semantic_archives"]:
            index = int(sem_name.split("_")[1].split(".")[0])
            v2.clear_state(modules, "independent_decode_entry")
            inp = work / ("in_%d" % index)
            inp.mkdir()
            (inp / "chunk_0.tar.xz").symlink_to(archive / ("chunk_%d.tar.xz" % index))
            transformed = work / ("transformed_%d.log" % index)
            decoded = work / ("decoded_%d.log" % index)
            log = work / ("decompress_%d.log" % index)
            proc = subprocess.run([str(source / "backend/decompress"), str(inp), str(transformed), "1"],
                                  cwd=str(work), capture_output=True)
            if proc.returncode or b"Error processing chunk" in proc.stdout + proc.stderr:
                raise RuntimeError("native decompress failed for block %d" % index)
            front._decode_block((str(transformed), str(archive / "semantic" / sem_name), str(decoded), str(source / "runtime")))
            v2.clear_state(modules, "independent_decode_exit")
            data = decoded.read_bytes()
            h = hashlib.sha256(data).hexdigest()
            full.update(data)
            exp = expected_rows[index]
            match = h == exp["raw_sha256"] and len(data) == exp["raw_bytes"]
            ok &= match
            rows.append({"index": index, "decoded_bytes": len(data), "decoded_sha256": h, "sha_pass": match})
            for f in (transformed, decoded):
                f.unlink()
    full_ok = full.hexdigest() == expected_full and len(rows) == len(expected_rows)
    out = {"branch": args.branch, "dataset_dir": str(ddir), "blocks": len(rows),
           "per_block_sha_pass": ok, "processed_concat_sha_pass": full_ok,
           "decoded_concat_sha256": full.hexdigest(), "rows": rows,
           "reads": "branch archive directory + unchanged runtime only; no plan/policy/trace",
           "status": "PASS" if ok and full_ok else "FAIL"}
    v2.dump(ddir / args.branch / "independent_decode.json", out)
    print(json.dumps({k: out[k] for k in ("branch", "blocks", "per_block_sha_pass", "processed_concat_sha_pass", "status")}), flush=True)
    return 0 if out["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
