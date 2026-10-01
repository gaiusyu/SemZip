#!/usr/bin/env python3
"""Summarize a codec_baseline_denum.py campaign: archive bytes (compression-only; NOT a
lossless result when status is FAIL), round-trip verdict, and line-level exactness of the
official decoder output. usage: summarize_smoke.py CAMPAIGN_DIR [OUT_JSON]"""
import glob
import json
import os
import subprocess
import sys

camp = sys.argv[1]
here = os.path.dirname(os.path.abspath(__file__))
rows = []
for res in sorted(glob.glob(os.path.join(camp, "*", "denum", "trial_*", "attempt_*", "result.json"))):
    r = json.load(open(res))
    att = os.path.dirname(res)
    row = {"dataset": r["dataset"], "attempt": att, "status": r["status"], "error": r.get("error"),
           "raw_bytes": r["raw_bytes"], "records": r["records"], "raw_sha256": r["raw_sha256"],
           "blocks": len(r.get("blocks", [])), "archive_bytes": r.get("archive_bytes"),
           "encode_seconds": r.get("encode_seconds"), "decode_seconds": r.get("decode_seconds")}
    if row["archive_bytes"]:
        row["ratio_compression_only_unverified"] = r["raw_bytes"] / r["archive_bytes"]
    notes = glob.glob(os.path.join(att, "diagnostics", "*", "encode.adapter_note"))
    row["blocks_with_accepted_summary_abort"] = len(notes)
    dec = os.path.join(att, "roundtrip.owned.log")
    if os.path.exists(dec):
        d = json.loads(subprocess.check_output([sys.executable, os.path.join(here, "diff_roundtrip.py"),
                                                r["source"]["path"], dec, "3"]))
        row.update({k: d[k] for k in ("decoded_bytes", "decoded_sha256", "byte_exact", "original_lf", "decoded_lf",
                                      "original_cr", "lines_compared", "lines_exact", "first_mismatches")})
    rows.append(row)
out = json.dumps(rows, indent=1, ensure_ascii=False)
if len(sys.argv) > 2:
    open(sys.argv[2], "w").write(out + "\n")
for x in rows:
    print("%-10s %-4s blocks=%d raw=%d archive=%s ratio_unverified=%.3f dec_bytes=%s byte_exact=%s lines_exact=%s/%s CR=%s aborts_accepted=%d" % (
        x["dataset"], x["status"], x["blocks"], x["raw_bytes"], x["archive_bytes"],
        x.get("ratio_compression_only_unverified", 0), x.get("decoded_bytes"), x.get("byte_exact"),
        x.get("lines_exact"), x["records"], x.get("original_cr"), x["blocks_with_accepted_summary_abort"]))
