#!/usr/bin/env python3
"""Adapter equivalence: run the unmodified official CLI on the WHOLE raw file exactly as the
README says (`./denum_compress DATASET 100000 1` with Logs/DATASET/DATASET.log), then compare
every native per-block archive output/DATASET/compressed<i>.xz with the adapter's independent
per-block archive block_<i>.tar.xz: member names, member contents (SHA-256) and member order.
usage: equiv_check.py DATASET RAW_FILE ADAPTER_ATTEMPT_DIR OUTDIR DENUM_BINARY"""
import hashlib
import json
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path


def members(path):
    out = []
    with tarfile.open(str(path)) as t:
        for m in t.getmembers():
            if m.isfile():
                out.append((m.name.split("/")[-1], hashlib.sha256(t.extractfile(m).read()).hexdigest(), m.size))
    return out


def main():
    ds, raw, att, outdir, binary = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5]
    work = outdir / "native_work"
    if work.exists():
        shutil.rmtree(str(work))
    (work / "Logs" / ds).mkdir(parents=True)
    (work / "output").mkdir()
    shutil.copyfile(str(raw), str(work / "Logs" / ds / (ds + ".log")))
    t0 = time.perf_counter()
    p = subprocess.run([binary, ds, "100000", "1"], cwd=str(work), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    wall = time.perf_counter() - t0
    res = json.load(open(att / "result.json"))
    report = {"dataset": ds, "native_exit": p.returncode, "native_wall_seconds": wall,
              "native_stdout": p.stdout.decode()[-600:], "native_stderr": p.stderr.decode()[-600:], "blocks": []}
    for b in res["blocks"]:
        i = b["index"]
        nat = work / "output" / ds / ("compressed%d.xz" % i)
        ada = att / b["archive"]
        if not nat.exists():
            report["blocks"].append({"index": i, "native_archive": "MISSING"})
            continue
        mn, ma = members(nat), members(ada)
        report["blocks"].append({
            "index": i, "native_archive_bytes": nat.stat().st_size, "adapter_archive_bytes": ada.stat().st_size,
            "member_count_native": len(mn), "member_count_adapter": len(ma),
            "member_contents_identical": sorted(mn) == sorted(ma),
            "member_order_identical": [x[0] for x in mn] == [x[0] for x in ma],
            "differing_members": sorted(set(x[0] for x in set(mn) ^ set(ma))),
            "member_bytes_total": sum(x[2] for x in mn)})
    report["all_blocks_member_identical"] = all(b.get("member_contents_identical") for b in report["blocks"])
    shutil.rmtree(str(work / "Logs"))
    (outdir / "equivalence.json").write_text(json.dumps(report, indent=1) + "\n")
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
