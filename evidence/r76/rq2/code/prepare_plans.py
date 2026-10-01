#!/usr/bin/env python3
"""R76 B3: freeze a read-only copy of the 16 R73 pool publications.

For every dataset copies r73 runs/publish/pool/<DS>/program.json to
plans/<DS>/extraction.json (the file name the R70 harness expects) and the
published storage policy to plans/<DS>/r73_published_storage.json (reference
only; the control refits both arms on block 0, exactly as R70 V2 does).
Checks the copied plan hash against publication.json and the R73 formal result.
Writes plans/PLAN_IDENTITY.json. Refuses to overwrite an existing plans dir.
"""
import hashlib
import json
import shutil
import sys
from pathlib import Path

DATASETS = ["Android", "Apache", "BGL", "Hadoop", "HDFS", "HealthApp", "HPC", "Linux", "Mac",
            "OpenSSH", "OpenStack", "Proxifier", "Spark", "Thunderbird", "Windows", "Zookeeper"]


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for data in iter(lambda: f.read(1 << 20), b""):
            h.update(data)
    return h.hexdigest()


def main(r73, out):
    r73, out = Path(r73).resolve(), Path(out)
    out.mkdir(parents=False, exist_ok=False)
    rows = []
    for d in DATASETS:
        pub_dir = r73 / "runs/publish/pool" / d
        pub = json.loads((pub_dir / "publication.json").read_text())
        formal = json.loads((r73 / "runs/formal/pool" / d / "result.json").read_text())
        src = pub_dir / "program.json"
        (out / d).mkdir()
        shutil.copyfile(src, out / d / "extraction.json")
        shutil.copyfile(pub_dir / "storage/storage.json", out / d / "r73_published_storage.json")
        plan_sha = file_sha(out / d / "extraction.json")
        if plan_sha != pub["plan_sha256"] or plan_sha != formal["plan_sha256"] or plan_sha != file_sha(src):
            raise ValueError("plan identity mismatch for " + d)
        storage_sha = file_sha(out / d / "r73_published_storage.json")
        if storage_sha != pub["storage_sha256"] or storage_sha != formal["storage_sha256"]:
            raise ValueError("storage identity mismatch for " + d)
        if json.loads((out / d / "extraction.json").read_text())["dataset"] != d:
            raise ValueError("plan dataset field mismatch for " + d)
        rows.append({"dataset": d, "source_program": str(src), "plan_sha256": plan_sha,
                     "r73_published_storage_sha256": storage_sha,
                     "r73_formal_result": str(r73 / "runs/formal/pool" / d / "result.json"),
                     "r73_formal_archive_bytes": formal["encode"]["archive_bytes"],
                     "r73_formal_blocks": len(formal["encode"]["blocks"]),
                     "r73_formal_raw_sha256": formal["encode"]["raw_sha256"]})
    (out / "PLAN_IDENTITY.json").write_text(json.dumps(rows, indent=2) + "\n")
    for p in sorted(out.rglob("*")):
        if p.is_file():
            p.chmod(0o444)
    print(json.dumps({"datasets": len(rows), "status": "PASS"}))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
