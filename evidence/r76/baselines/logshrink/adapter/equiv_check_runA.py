#!/usr/bin/env python3
"""Driver/adapter equivalence check on Proxifier: wrap the archive produced by the UNMODIFIED official
run.py (explore/runA, unseeded, whole-file driver; Proxifier is one 100k block) into the adapter's .lsz framing
with its template files, decode it with the same archive-only decoder, and compare with the adapter's own
archive for the same block. Prints sizes and residual statistics of both."""
import json, shutil, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import ls_run

T = ls_run.TRACK
runA = T / "explore" / "runA"
w = Path("/tmp/ls_equiv_runA")
shutil.rmtree(str(w), ignore_errors=True)
w.mkdir(parents=True)
tpl = runA / "code" / "python_compression" / "template" / "Proxifier"
subprocess.run("7z a " + str(w / "model") + " template.col head.format -m0=LZMA", cwd=str(tpl), shell=True,
               check=True, stdout=subprocess.DEVNULL)
pay = (runA / "out" / "Proxifier" / "0.7z").read_bytes()
blob, hl = ls_run.pack_archive(0, [pay], (w / "model.7z").read_bytes())
(w / "runA.lsz").write_bytes(blob)
orig = (ls_run.RAW_DIR / "Proxifier.log").resolve().read_bytes()
out = {}
for name, arch in [("official_run.py_unseeded", w / "runA.lsz"),
                   ("adapter_seed0", T / "smoke3" / "Proxifier" / "archives" / "block_000000.lsz")]:
    d = w / name
    d.mkdir()
    p, info = ls_run.decode_block(arch, d, d / "diag", 3600)
    dec = p.read_bytes()
    r, st = ls_run.build_residual(orig, dec)
    out[name] = {"lsz_bytes": arch.stat().st_size, "payload_7z_bytes": sum(len(x) for x in ls_run.unpack_archive(arch.read_bytes())[1]),
                 "residual_bytes": len(r), "residual_stats": st,
                 "repaired_ok": ls_run.apply_residual(r, dec) == orig}
print(json.dumps(out, indent=1))
shutil.rmtree(str(w), ignore_errors=True)
