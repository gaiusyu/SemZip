#!/usr/bin/env python3
"""Write RESULTS.md / results_table.json from a finished run_all.py output dir (read-only on it)."""
import json
import sys
from pathlib import Path

ORDER = ["Android", "Apache", "BGL", "Hadoop", "HealthApp", "HPC", "Linux", "Mac", "OpenSSH", "OpenStack",
         "Proxifier", "Zookeeper", "HDFS", "Spark", "Windows", "Thunderbird"]


def main(out, dest):
    out = Path(out)
    summ = {r["dataset"]: r for r in json.loads((out / "summary.json").read_text())}
    rows = []
    for d in ORDER:
        if d not in summ:
            continue
        s = summ[d]
        row = dict(s)
        bpath = out / d / d / "blocks.json"
        if bpath.exists():
            blocks = json.loads(bpath.read_text())
            changed = {}
            for b in blocks:
                for c in b["branches"]["surface"]["changed_program_streams"]:
                    changed.setdefault(c["tag"], [c["from"], 0])[1] += c["count"]
            row["changed_streams"] = changed
            row["surface_bigger_blocks"] = sum(b["branches"]["surface"]["combined_block_archive_bytes"] > b["branches"]["latent"]["combined_block_archive_bytes"] for b in blocks)
            row["surface_smaller_blocks"] = sum(b["branches"]["surface"]["combined_block_archive_bytes"] < b["branches"]["latent"]["combined_block_archive_bytes"] for b in blocks)
        rows.append(row)
    Path(dest).with_suffix(".json").write_text(json.dumps(rows, indent=2) + "\n")
    L = ["# R76 B3 results: matched-span surface vs latent (R73 pool programs)", "",
         "Saving = 100*(1 - latent/surface), complete archive bytes (semantic + shared residual + manifest).",
         "Held-out = blocks 1.. (sampled files: the 19 non-zero sampled blocks). Residual archive is byte-identical in both arms.", "",
         "| Dataset | Scope | Blocks | Surface B | Latent B | Saving % | Held-out % | Sem. surface B | Sem. latent B | Residual B | Program streams replaced | Blocks latent smaller / larger | Latent = R73 main (blocks) | Archive-only decode |",
         "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        if "surface_bytes" not in r:
            L.append("| %s | %s | FAIL: %s |" % (r["dataset"], r["scope"], r.get("error")))
            continue
        ho = "%.2f" % r["heldout_saving_pct"] if r["heldout_saving_pct"] is not None else "n/a (1 block)"
        ch = ", ".join("%s(%s)" % (t, v[0]) for t, v in sorted(r.get("changed_streams", {}).items())) or "none"
        L.append("| %s | %s | %d/%d | %d | %d | %.2f | %s | %d | %d | %d | %s | %d / %d | %d/%d | %s |" % (
            r["dataset"], r["scope"], r["blocks"], r["file_blocks"], r["surface_bytes"], r["latent_bytes"], r["saving_pct"], ho,
            r["surface_semantic_bytes"], r["latent_semantic_bytes"], r["residual_bytes"], ch,
            r.get("surface_bigger_blocks", 0), r.get("surface_smaller_blocks", 0),
            r["r73_parity"]["latent_semantic_sha_equal_blocks"], r["blocks"],
            "PASS" if r["status"] == "PASS" else "FAIL (%s)" % r.get("independent_decode")))
    comp = [r for r in rows if r.get("scope") == "complete" and "surface_bytes" in r]
    if comp:
        s = sum(r["surface_bytes"] for r in comp)
        l = sum(r["latent_bytes"] for r in comp)
        L += ["", "Complete files (%d): surface %d B, latent %d B, total saving %.2f%%." % (len(comp), s, l, 100 * (1 - l / s))]
    samp = [r for r in rows if r.get("scope") == "sample20" and "surface_bytes" in r]
    if samp:
        s = sum(r["surface_bytes"] for r in samp)
        l = sum(r["latent_bytes"] for r in samp)
        L += ["Sampled files (%d x 20 blocks): surface %d B, latent %d B, total saving %.2f%%." % (len(samp), s, l, 100 * (1 - l / s))]
    Path(dest).write_text("\n".join(L) + "\n")
    print("\n".join(L))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
