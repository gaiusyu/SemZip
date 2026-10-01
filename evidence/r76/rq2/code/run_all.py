#!/usr/bin/env python3
"""R76 B3 campaign driver: one encode process per dataset (<= --workers concurrently),
followed by an independent archive-only decode process for each branch.

Usage (from the rq2 track dir):
  python3 -u run_all.py --output OUT --workers 3 --complete DS... [--sampled DS...]
Complete datasets: every original 100k block. Sampled datasets: 20 systematically
spaced blocks round(i*(N-1)/19). Refuses an existing OUT. Summarises into
OUT/summary.json and OUT/summary.md when all jobs finish.
"""
import argparse
import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = Path("<WORKDIR>")
SOURCE = ROOT / "r73_quality_gate_20260927/r73_qg/art/source"
RAW = ROOT / "data/loghub1/raw"
REF = ROOT / "r73_quality_gate_20260927/r73_qg/runs/formal/pool"
INV = ROOT / "r68_external_20260923/first_pass"
PLANS = HERE / "plans"
SAMPLE = 20


def run_job(out, dataset, sampled):
    dout = out / dataset
    log = out / "logs" / (dataset + ".log")
    cmd = [sys.executable, "-u", str(HERE / "representation_control_r76.py"), "--source", str(SOURCE),
           "--raw-root", str(RAW), "--plans-root", str(PLANS), "--reference-root", str(REF),
           "--dataset", dataset, "--output", str(dout)]
    if sampled:
        cmd += ["--sample", str(SAMPLE), "--inventory", str(INV / ("input_%s.json" % dataset))]
    t0 = time.time()
    with log.open("x") as f:
        f.write(json.dumps({"cmd": cmd}) + "\n")
        f.flush()
        enc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, cwd=str(HERE))
    status = {"dataset": dataset, "sampled": sampled, "encode_rc": enc.returncode, "encode_seconds": time.time() - t0}
    if enc.returncode == 0:
        for branch in ("latent", "surface"):
            t1 = time.time()
            dlog = out / "logs" / ("%s.decode_%s.log" % (dataset, branch))
            with dlog.open("x") as f:
                dec = subprocess.run([sys.executable, "-u", str(HERE / "decode_verify.py"), "--source", str(SOURCE),
                                      "--dataset-dir", str(dout / dataset), "--branch", branch],
                                     stdout=f, stderr=subprocess.STDOUT, cwd=str(HERE))
            status["decode_%s_rc" % branch] = dec.returncode
            status["decode_%s_seconds" % branch] = time.time() - t1
    status["status"] = "PASS" if enc.returncode == 0 and all(status.get("decode_%s_rc" % b) == 0 for b in ("latent", "surface")) else "FAIL"
    with (out / "campaign.jsonl").open("a") as f:
        f.write(json.dumps(status) + "\n")
    print(json.dumps(status), flush=True)
    return status


def summarize(out, jobs):
    rows = []
    for dataset, sampled in jobs:
        rpath = out / dataset / dataset / "result.json"
        row = {"dataset": dataset, "scope": "sample20" if sampled else "complete"}
        if not rpath.exists():
            row["status"] = "FAIL"
            fail = out / dataset / (dataset + ".failure.json")
            row["error"] = json.loads(fail.read_text()).get("error") if fail.exists() else "no result"
            rows.append(row)
            continue
        r = json.loads(rpath.read_text())
        dec = {b: json.loads((out / dataset / dataset / b / "independent_decode.json").read_text())["status"]
               if (out / dataset / dataset / b / "independent_decode.json").exists() else "MISSING" for b in ("latent", "surface")}
        row.update(status=r["status"] if all(v == "PASS" for v in dec.values()) else "FAIL",
                   blocks=r["blocks"], file_blocks=r["file_block_count"], raw_bytes=r["raw_bytes"],
                   surface_bytes=r["branches"]["surface"]["archive_bytes"], latent_bytes=r["branches"]["latent"]["archive_bytes"],
                   saving_pct=r["full_archive_saving_pct"],
                   surface_suffix_bytes=r["branches"]["surface"]["suffix_block_archive_bytes"],
                   latent_suffix_bytes=r["branches"]["latent"]["suffix_block_archive_bytes"],
                   heldout_saving_pct=r["heldout_archive_saving_pct"],
                   residual_bytes=r["branches"]["latent"]["residual_archive_bytes"],
                   surface_semantic_bytes=r["branches"]["surface"]["semantic_archive_bytes"],
                   latent_semantic_bytes=r["branches"]["latent"]["semantic_archive_bytes"],
                   independent_decode=dec, r73_parity=r["r73_parity"])
        rows.append(row)
    (out / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    lines = ["| Dataset | Scope | Blocks | Surface B | Latent B | Saving % | Held-out saving % | Latent=R73 blocks | Decode |",
             "|---|---|---|---|---|---|---|---|---|"]
    for r in rows:
        if r.get("status") != "PASS" and "surface_bytes" not in r:
            lines.append("| %s | %s | FAIL: %s |||||||" % (r["dataset"], r["scope"], r.get("error")))
            continue
        ho = "%.2f" % r["heldout_saving_pct"] if r["heldout_saving_pct"] is not None else "n/a"
        lines.append("| %s | %s | %d/%d | %d | %d | %.2f | %s | %d/%d | %s |" % (
            r["dataset"], r["scope"], r["blocks"], r["file_blocks"], r["surface_bytes"], r["latent_bytes"], r["saving_pct"], ho,
            r["r73_parity"]["latent_semantic_sha_equal_blocks"], r["blocks"],
            "PASS" if r["status"] == "PASS" else "FAIL"))
    (out / "summary.md").write_text("\n".join(lines) + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--workers", type=int, default=3)
    p.add_argument("--complete", nargs="*", default=[])
    p.add_argument("--sampled", nargs="*", default=[])
    p.add_argument("--order", nargs="*", default=[], help="scheduling order only (longest first); no effect on results")
    args = p.parse_args()
    if args.workers > 3:
        raise SystemExit("workers <= 3 for this track")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    (out / "logs").mkdir()
    jobs = [(d, False) for d in args.complete] + [(d, True) for d in args.sampled]
    if args.order:
        rank = {d: i for i, d in enumerate(args.order)}
        jobs.sort(key=lambda j: rank.get(j[0], len(rank)))
    (out / "launch.json").write_text(json.dumps({"jobs": jobs, "workers": args.workers, "started_unix": time.time(),
                                                  "source": str(SOURCE), "plans": str(PLANS), "reference": str(REF),
                                                  "raw_root": str(RAW), "inventory_root": str(INV), "sample": SAMPLE,
                                                  "api_calls": 0}, indent=2) + "\n")
    with ThreadPoolExecutor(args.workers) as ex:
        results = list(ex.map(lambda j: run_job(out, *j), jobs))
    summarize(out, jobs)
    status = {"status": "PASS" if all(r["status"] == "PASS" for r in results) else "COMPLETE_WITH_FAILURES",
              "passed": sum(r["status"] == "PASS" for r in results), "datasets": len(results), "finished_unix": time.time()}
    (out / "status.json").write_text(json.dumps(status, indent=2) + "\n")
    print(json.dumps(status), flush=True)


if __name__ == "__main__":
    main()
