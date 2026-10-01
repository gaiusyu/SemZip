#!/usr/bin/env python3
"""Compare an encode-only campaign (codec_baseline_denum_enconly.py) with the smoke_v2
lossless-mode campaign, block by block: archive bytes (tolerance: the documented tar-header
timestamp noise, <= 0.73% per block), tar member names/order/SHA-256 (must be identical), and
the accepted-SIGABRT adapter notes. usage: check_enconly_smoke.py ENCONLY_DIR SMOKE_V2_DIR OUT_JSON DS..."""
import hashlib
import json
import os
import subprocess
import sys
import tempfile

TOL = 0.0073


def members(archive, scratch):
    d = tempfile.mkdtemp(dir=scratch)
    subprocess.run(["tar", "-xJf", archive, "-C", d], check=True)
    names = subprocess.run(["tar", "-tJf", archive], check=True, stdout=subprocess.PIPE).stdout.decode().split("\n")
    out = []
    for n in names:
        p = os.path.join(d, n)
        if n and os.path.isfile(p):
            with open(p, "rb") as f:
                out.append((n, hashlib.sha256(f.read()).hexdigest()))
    subprocess.run(["rm", "-rf", "--", d], check=True)
    return out


def main():
    enc_dir, v2_dir, out_json, datasets = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
    rows, ok = [], True
    with tempfile.TemporaryDirectory(prefix="check-", dir=enc_dir) as scratch:
        for ds in datasets:
            a = os.path.join(enc_dir, ds, "denum", "trial_001", "attempt_001")
            b = os.path.join(v2_dir, ds, "denum", "trial_001", "attempt_001")
            ra, rb = json.load(open(os.path.join(a, "result.json"))), json.load(open(os.path.join(b, "result.json")))
            for x, y in zip(ra["blocks"], rb["blocks"]):
                ma, mb = members(os.path.join(a, x["archive"]), scratch), members(os.path.join(b, y["archive"]), scratch)
                rel = abs(x["archive_bytes"] - y["archive_bytes"]) / y["archive_bytes"]
                na = os.path.exists(os.path.join(a, "diagnostics", "%06d" % x["index"], "encode.adapter_note"))
                nb = os.path.exists(os.path.join(b, "diagnostics", "%06d" % y["index"], "encode.adapter_note"))
                row = {"dataset": ds, "block": x["index"], "enconly_archive_bytes": x["archive_bytes"],
                       "smoke_v2_archive_bytes": y["archive_bytes"], "delta_bytes": x["archive_bytes"] - y["archive_bytes"],
                       "rel_delta": rel, "within_tolerance": rel <= TOL, "member_count": len(ma),
                       "members_identical": ma == mb, "adapter_note_enconly": na, "adapter_note_smoke_v2": nb}
                ok &= row["within_tolerance"] and row["members_identical"] and na == nb
                rows.append(row)
            ok &= len(ra["blocks"]) == len(rb["blocks"]) and ra["raw_sha256"] == rb["raw_sha256"]
    res = {"tolerance_rel_per_block": TOL, "all_ok": ok, "blocks": rows}
    with open(out_json, "x") as f:
        json.dump(res, f, indent=2)
        f.write("\n")
    print(json.dumps(res, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
