#!/usr/bin/env python3
"""Debug helper: archive-only re-decode of one saved .lsz and print mismatch categories.
usage: dbg_redecode.py ARCHIVE.lsz DATASET BLOCK_INDEX [N_EXAMPLES]"""
import json, shutil, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import ls_run

arch, ds, bi = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
nex = int(sys.argv[4]) if len(sys.argv) > 4 else 6
inv = json.load(open(ls_run.R68_INV / ("input_%s.json" % ds)))
off = sum(b["raw_bytes"] for b in inv["blocks"][:bi])
src = Path(ls_run.RAW_DIR / (ds + ".log")).resolve()
with open(src, "rb") as f:
    f.seek(off)
    orig = f.read(inv["blocks"][bi]["raw_bytes"])
w = Path("/tmp/ls_dbg_%s_%d" % (ds, bi))
shutil.rmtree(str(w), ignore_errors=True)
w.mkdir(parents=True)
out, info = ls_run.decode_block(arch, w, w / "diag", 3600)
dec = out.read_bytes()
r, st = ls_run.build_residual(orig, dec)
print("residual", len(r), st, info)
a, b = orig.split(b"\n"), dec.split(b"\n")
n = 0
for x, y in zip(a, b):
    if x == y or x.strip() == y:
        continue
    n += 1
    if n <= nex:
        print(repr(x[:200]))
        print(repr(y[:200]))
        print()
print("general(non-strip) mismatches", n)
shutil.rmtree(str(w), ignore_errors=True)
