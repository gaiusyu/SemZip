#!/usr/bin/env python3
"""Progress / summary of an ls_run.py output directory.  usage: ls_status.py OUTPUT_DIR [--blocks]"""
import glob
import json
import os
import sys

R68 = "<WORKDIR>/r68_external_20260923/first_pass"
out = sys.argv[1]
ORDER = ["Proxifier", "Linux", "Apache", "Zookeeper", "HealthApp", "Hadoop", "Mac", "HPC", "OpenStack",
         "OpenSSH", "Android", "BGL", "HDFS", "Spark", "Windows", "Thunderbird"]
print("%-12s %9s %8s %8s %9s %9s %9s %8s %9s %9s" % ("dataset", "blocks", "ok", "nat_ok", "native_CR", "rep_CR",
                                                     "rep_ok", "fail", "enc_s/blk", "dec_s/blk"))
for ds in ORDER:
    d = os.path.join(out, ds)
    if not os.path.isdir(d):
        continue
    total = len(json.load(open(os.path.join(R68, "input_%s.json" % ds)))["blocks"])
    recs = []
    for p in sorted(glob.glob(os.path.join(d, "blocks", "block_*.json"))):
        try:
            recs.append(json.load(open(p)))
        except Exception:
            pass
    ok = [r for r in recs if r.get("status") == "ok"]
    raw = sum(r["raw_bytes"] for r in recs)
    nat = sum(r.get("lsz_bytes", 0) for r in recs)
    rep = nat + sum(r.get("res_bytes", 0) for r in recs)
    enc = [r["encode_seconds"] for r in recs if "encode_seconds" in r]
    dec = [r["decode_seconds"] for r in recs if "decode_seconds" in r]
    print("%-12s %4d/%-4d %8d %8d %9.3f %9.3f %9d %8d %9.1f %9.1f" % (
        ds, len(recs), total, len(ok), sum(1 for r in recs if r.get("native_lossless")),
        raw / nat if nat else 0, raw / rep if rep else 0, sum(1 for r in recs if r.get("repaired_lossless")),
        len(recs) - len(ok), sum(enc) / len(enc) if enc else 0, sum(dec) / len(dec) if dec else 0))
    res = os.path.join(d, "result.json")
    if os.path.exists(res):
        r = json.load(open(res))
        print("    result.json: native %.3f lossless=%s | repaired %.3f lossless=%s full_sha=%s | suffix nat %s rep %s"
              % (r["native"]["ratio"] or 0, r["native"]["lossless"], r["repaired"]["ratio"] or 0,
                 r["repaired"]["lossless"], r["repaired"]["full_sha256_match"], r["native"]["suffix_ratio"],
                 r["repaired"]["suffix_ratio"]))
    if "--blocks" in sys.argv:
        for r in recs:
            if r.get("status") != "ok":
                print("    block %d %s %s" % (r["index"], r.get("status"),
                                             (r.get("error") or r.get("native_decode_error") or "")[:200]))
