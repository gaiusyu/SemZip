#!/usr/bin/env python3
"""Summarize an R76 codec_baseline_r76.py campaign directory (read-only).

Per dataset x codec (latest attempt of trial 1): status, raw/archive bytes, full ratio,
suffix ratio (blocks with index >= 1; for zstd19dict the dictionary bytes are included
in the suffix archive because suffix blocks cannot be decoded without it), dictionary
bytes, encode/decode seconds, archive-only SHA verdict.  Also cross-checks every input
inventory (full + per-block SHA-256) against the R68 first-pass inventories.

usage: summarize.py CAMPAIGN_DIR [--r68 R68_FIRST_PASS_DIR] [--json OUT.json]
"""
import argparse
import json
from pathlib import Path


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("campaign", type=Path)
    p.add_argument("--r68", type=Path,
                   default=Path("<WORKDIR>/r68_external_20260923/first_pass"))
    p.add_argument("--json", type=Path)
    a = p.parse_args()
    manifest = load(a.campaign / "manifest.json")
    datasets = [x["dataset"] for x in manifest["protocol"]["inputs"]]
    codecs = list(manifest["protocol"]["codecs"])
    rows, checks = [], []
    for ds in datasets:
        inv_path = a.campaign / ("input_%s.json" % ds)
        r68_path = a.r68 / ("input_%s.json" % ds)
        if inv_path.exists() and r68_path.exists():
            mine, ref = load(inv_path), load(r68_path)
            same = (mine["raw_sha256"] == ref["raw_sha256"] and
                    [b["raw_sha256"] for b in mine["blocks"]] == [b["raw_sha256"] for b in ref["blocks"]] and
                    [b["raw_bytes"] for b in mine["blocks"]] == [b["raw_bytes"] for b in ref["blocks"]])
            checks.append({"dataset": ds, "inventory_matches_r68": same, "blocks": len(mine["blocks"])})
        for codec in codecs:
            trial = a.campaign / ds / codec / "trial_001"
            attempts = sorted(trial.glob("attempt_*")) if trial.exists() else []
            row = {"dataset": ds, "codec": codec, "status": "NOT_RUN", "attempts": len(attempts)}
            if attempts:
                rp = attempts[-1] / "result.json"
                if not rp.exists():
                    sp = attempts[-1] / "status.json"
                    row["status"] = "RUNNING/INTERRUPTED(%s)" % (load(sp).get("phase", "?") if sp.exists() else "?")
                else:
                    r = load(rp)
                    row["status"] = r["status"]
                    if r["status"] == "PASS":
                        blocks = r["blocks"]
                        dict_bytes = r.get("dictionary_bytes", 0)
                        suf = [b for b in blocks if b["index"] >= 1]
                        suf_raw = sum(b["raw_bytes"] for b in suf)
                        suf_arc = sum(b["archive_bytes"] for b in suf) + (dict_bytes if suf else 0)
                        row.update({
                            "raw_bytes": r["raw_bytes"], "archive_bytes": r["archive_bytes"],
                            "ratio": r["raw_bytes"] / r["archive_bytes"],
                            "dictionary_bytes": dict_bytes or None,
                            "suffix_raw_bytes": suf_raw or None, "suffix_archive_bytes": suf_arc or None,
                            "suffix_ratio": (suf_raw / suf_arc) if suf else None,
                            "blocks": len(blocks), "encode_seconds": r["encode_seconds"],
                            "decode_seconds": r["decode_seconds"],
                            "archive_only_decode": r.get("archive_only_decode"),
                            "full_sha_ok": r["decoded_sha256"] == r["raw_sha256"],
                            "block_sha_ok": all(b["decoded_sha256"] == b["raw_sha256"] for b in blocks),
                        })
                    else:
                        row["error"] = r.get("error")
            rows.append(row)
    hdr = ["dataset", "codec", "status", "raw_bytes", "archive_bytes", "ratio", "dictionary_bytes",
           "suffix_ratio", "blocks", "encode_seconds", "decode_seconds", "full_sha_ok", "block_sha_ok"]
    print("\t".join(hdr))
    for r in rows:
        out = []
        for k in hdr:
            v = r.get(k)
            out.append("%.3f" % v if isinstance(v, float) else ("" if v is None else str(v)))
        print("\t".join(out))
    for c in checks:
        print("inventory_vs_r68\t%(dataset)s\t%(inventory_matches_r68)s\tblocks=%(blocks)d" % c)
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump({"campaign": str(a.campaign), "rows": rows, "inventory_checks": checks}, f, indent=1)


if __name__ == "__main__":
    main()
