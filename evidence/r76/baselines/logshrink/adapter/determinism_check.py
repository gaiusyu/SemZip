#!/usr/bin/env python3
"""Compare two .lsz archives of the same block produced by independent runs: are the extracted payload and
model files byte-identical (i.e. is any size difference only 7z header metadata such as file mtimes)?
usage: determinism_check.py A.lsz B.lsz"""
import hashlib, json, shutil, subprocess, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import ls_run

def extract(lsz, d):
    flags, pays, model = ls_run.unpack_archive(Path(lsz).read_bytes())
    files = {}
    for name, blob in [("seg%d" % k, p) for k, p in enumerate(pays)] + [("model", model)]:
        sub = d / name
        sub.mkdir(parents=True)
        (d / (name + ".7z")).write_bytes(blob)
        subprocess.run(["7za", "x", str(d / (name + ".7z")), "-o" + str(sub), "-y"], check=True,
                       stdout=subprocess.DEVNULL)
        for f in sorted(sub.rglob("*")):
            if f.is_file():
                files[name + "/" + str(f.relative_to(sub))] = hashlib.sha256(f.read_bytes()).hexdigest()
    return files, [len(p) for p in pays], len(model)

w = Path("/tmp/ls_determinism")
shutil.rmtree(str(w), ignore_errors=True)
fa, pa, ma = extract(sys.argv[1], w / "a")
fb, pb, mb = extract(sys.argv[2], w / "b")
diff = sorted(k for k in set(fa) | set(fb) if fa.get(k) != fb.get(k))
print(json.dumps({"a": sys.argv[1], "b": sys.argv[2], "payload_7z_bytes": [pa, pb], "model_7z_bytes": [ma, mb],
                  "files": [len(fa), len(fb)], "differing_files": diff[:20], "n_differing": len(diff)}, indent=1))
shutil.rmtree(str(w), ignore_errors=True)
