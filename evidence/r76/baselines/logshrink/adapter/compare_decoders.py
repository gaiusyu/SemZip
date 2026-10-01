#!/usr/bin/env python3
"""Decode saved .lsz archives with two decoder trees and compare residual size / mismatch counts.
usage: compare_decoders.py DEC_DIR_A DEC_DIR_B DATASET:BLOCK:ARCHIVE [...]"""
import json, shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import ls_run

def orig_block(ds, bi):
    inv = json.load(open(ls_run.R68_INV / ("input_%s.json" % ds)))
    off = sum(b["raw_bytes"] for b in inv["blocks"][:bi])
    with open(Path(ls_run.RAW_DIR / (ds + ".log")).resolve(), "rb") as f:
        f.seek(off)
        return f.read(inv["blocks"][bi]["raw_bytes"])

deca, decb = Path(sys.argv[1]), Path(sys.argv[2])
for spec in sys.argv[3:]:
    ds, bi, arch = spec.split(":")
    orig = orig_block(ds, int(bi))
    row = {"dataset": ds, "block": int(bi), "lsz": Path(arch).stat().st_size}
    for tag, d in (("A", deca), ("B", decb)):
        ls_run.DEC = d
        w = Path("/tmp/ls_cmp_%s_%s_%s" % (ds, bi, tag))
        shutil.rmtree(str(w), ignore_errors=True)
        w.mkdir(parents=True)
        try:
            p, info = ls_run.decode_block(Path(arch), w, w / "diag", 3600)
            dec = p.read_bytes()
            r, st = ls_run.build_residual(orig, dec)
            assert ls_run.apply_residual(r, dec) == orig
            a, b = orig.split(b"\n"), dec.split(b"\n")
            gen = sum(1 for x, y in zip(a, b) if x != y and x.strip(ls_run.WS) != y) if len(a) == len(b) else None
            row[tag] = {"res": len(r), "differing": st["differing_lines"], "general": gen, "fallback": st["fallback"],
                        "lossless": dec == orig}
        except Exception as e:
            row[tag] = {"error": str(e)[:200]}
        shutil.rmtree(str(w), ignore_errors=True)
    print(json.dumps(row), flush=True)
