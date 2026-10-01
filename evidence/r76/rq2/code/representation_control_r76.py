#!/usr/bin/env python3
"""R76 B3: matched-span surface vs latent representation control on the R73 pool programs.

One dataset per process. Every representation step is the UNCHANGED R70 V2 strict
harness, imported from the byte-identical copy representation_control_v2.py
(replay_once, branch_state, base_streams, verify_streams, fit_policy,
encode_semantic, encode_residual, native, iter_blocks, load_runtime,
cache boundary). Spans, placeholders, groups, residual handling, backend settings
and metadata accounting are therefore identical between the two arms, exactly as
in V2. This driver only changes:
  1. any dataset / plan (the R73 pool publication) instead of the fixed 4-cohort;
  2. an optional systematic block sample (original 100k-record blocks read by byte
     offset from the R68 block inventory, each SHA-checked against that inventory
     AND the R73 formal per-block SHA); block 0 is always the storage-fit block;
  3. diagnostics: per-block parity of the shared residual archive and the latent
     semantic archive against the R73 formal pool archive, and a comparison of the
     latent refitted storage policy with the R73 published policy. Diagnostics never
     change any archive or the result.
No API calls. Size/SHA study only; no throughput claim.
"""
from __future__ import annotations
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import traceback

import representation_control_v2 as v2

VERSION = "R76-B3-REPRESENTATION-CONTROL-POOL-20260929"
BRANCHES = v2.BRANCHES
POLICY_KEYS = ("recipe", "column_numeric", "numeric_modes", "subfield_ids",
               "unseen_column_numeric_default", "online_search", "programs")


def sample_indices(n, k):
    """round(i*(N-1)/(k-1)), i=0..k-1 (design section D)."""
    if n <= k:
        return list(range(n))
    idx = [round(i * (n - 1) / (k - 1)) for i in range(k)]
    if len(set(idx)) != k or idx[0] != 0 or idx[-1] != n - 1:
        raise ValueError("degenerate systematic sample")
    return idx


def load_reference(ref_dir):
    r = json.loads((ref_dir / "result.json").read_text())
    enc = r["encode"]
    return {"result": str(ref_dir / "result.json"), "archive_dir": Path(enc["archive_dir"]),
            "raw_sha256": enc["raw_sha256"], "raw_bytes": enc["raw_bytes"],
            "archive_bytes": enc["archive_bytes"], "plan_sha256": r["plan_sha256"],
            "storage_sha256": r["storage_sha256"],
            "block_sha": {b["index"]: b["raw_sha256"] for b in enc["blocks"]},
            "block_semantic_bytes": {b["index"]: b.get("semantic_archive_bytes") for b in enc["blocks"]},
            "blocks": len(enc["blocks"])}


def iter_selected(raw_path, indices, inventory):
    if indices is None:
        for index, raw in enumerate(v2.iter_blocks(raw_path)):
            yield index, raw
        return
    blocks = inventory["blocks"]
    if [b["index"] for b in blocks] != list(range(len(blocks))):
        raise ValueError("inventory blocks are not contiguous")
    offsets = [0]
    for b in blocks:
        offsets.append(offsets[-1] + b["raw_bytes"])
    if offsets[-1] != raw_path.stat().st_size or offsets[-1] != inventory["raw_bytes"]:
        raise ValueError("inventory byte total differs from raw file size")
    with raw_path.open("rb") as f:
        for index in indices:
            f.seek(offsets[index])
            data = f.read(blocks[index]["raw_bytes"])
            if len(data) != blocks[index]["raw_bytes"] or v2.sha(data) != blocks[index]["raw_sha256"]:
                raise ValueError("sampled block %d differs from inventory" % index)
            if data.count(b"\n") != blocks[index]["lf_count"]:
                raise ValueError("sampled block %d LF count differs from inventory" % index)
            if index and offsets[index] and index < len(blocks):
                f.seek(offsets[index] - 1)
                if f.read(1) != b"\n":
                    raise ValueError("sampled block %d does not start after an LF" % index)
            yield index, data


def policy_view(policy):
    return {k: policy.get(k) for k in POLICY_KEYS}


def run_dataset(raw_path, plan, dataset, output, source, modules, indices, inventory, ref, published_storage):
    output.mkdir(parents=True, exist_ok=False)
    (output / "shared_residual").mkdir()
    for branch in BRANCHES:
        (output / branch / "archive/semantic").mkdir(parents=True)
    frozen_hashes = {}
    rows = []
    full_raw = hashlib.sha256()
    full_decoded = {branch: hashlib.sha256() for branch in BRANCHES}
    for index, raw in iter_selected(raw_path, indices, inventory):
        if not rows and index != 0:
            raise ValueError("block 0 must be processed first (storage fit block)")
        if ref["block_sha"].get(index) != v2.sha(raw):
            raise ValueError("block %d SHA differs from the R73 formal pool block SHA" % index)
        v2.CACHE_EVENTS.clear()
        full_raw.update(raw)
        trace = v2.replay_once(raw, plan, modules)
        if trace["dataset"] != dataset:
            raise ValueError("plan dataset differs")
        row = {"index": index, "raw_bytes": len(raw), "raw_sha256": v2.sha(raw), "trace": trace["trace"],
               "lf_count": raw.count(b"\n"), "records": raw.count(b"\n") + int(not raw.endswith(b"\n")),
               "training": index == 0, "branches": {}}
        residual_path = output / "shared_residual" / ("chunk_%d.tar.xz" % index)
        row["residual"], decoded_residual = v2.encode_residual(trace["residual"], dataset, index, residual_path, source, "native")
        for branch in BRANCHES:
            base = output / branch
            archive = base / "archive/semantic" / ("block_%05d.semantic.tar.xz" % index)
            policy = base / "storage.json"
            info = v2.encode_semantic(trace, branch, raw, index, archive, policy, base / "fit.json", modules)
            if index == 0:
                frozen_hashes[branch] = v2.file_sha(policy)
            elif v2.file_sha(policy) != frozen_hashes[branch]:
                raise ValueError("storage policy changed after block0")
            target = base / "archive" / residual_path.name
            os.link(residual_path, target)
            with tempfile.TemporaryDirectory(prefix="r76-full-decode-") as tmp:
                unpacked = Path(tmp)
                modules[2].extract_semantic_archive(archive, unpacked)
                meta = json.loads((unpacked / "metadata.json").read_text())
                actual = v2.verify_streams(unpacked, meta, decoded_residual.decode("latin-1"), raw, modules)
            full_decoded[branch].update(actual)
            info["residual_archive_sha256"] = v2.file_sha(target)
            info["archive_only_full_decode_sha256"] = v2.sha(actual)
            info["combined_block_archive_bytes"] = info["semantic_archive_bytes"] + row["residual"]["bytes"]
            row["branches"][branch] = info
        # Diagnostics only: does the latent arm reproduce the frozen R73 main-result block?
        ref_sem = ref["archive_dir"] / "semantic" / ("block_%05d.semantic.tar.xz" % index)
        ref_res = ref["archive_dir"] / ("chunk_%d.tar.xz" % index)
        row["r73_parity"] = {
            "r73_residual_bytes": ref_res.stat().st_size if ref_res.exists() else None,
            "residual_sha_equal": ref_res.exists() and v2.file_sha(ref_res) == row["residual"]["sha256"],
            "r73_semantic_bytes": ref_sem.stat().st_size if ref_sem.exists() else None,
            "latent_semantic_sha_equal": ref_sem.exists() and v2.file_sha(ref_sem) == row["branches"]["latent"]["semantic_archive_sha256"],
            "surface_semantic_sha_equal_latent": row["branches"]["surface"]["semantic_archive_sha256"] == row["branches"]["latent"]["semantic_archive_sha256"]}
        row["cache_boundaries"] = copy.deepcopy(v2.CACHE_EVENTS)
        row["cache_boundary_verified"] = all(e["all_data_caches_empty"] for e in v2.CACHE_EVENTS)
        rows.append(row)
        v2.dump(output / "blocks.json", rows)
        print(json.dumps({"dataset": dataset, "block": index,
                          "latent": row["branches"]["latent"]["combined_block_archive_bytes"],
                          "surface": row["branches"]["surface"]["combined_block_archive_bytes"],
                          "r73_parity": {k: row["r73_parity"][k] for k in ("residual_sha_equal", "latent_semantic_sha_equal")},
                          "status": "PASS"}), flush=True)
    if not rows:
        raise ValueError("empty dataset")
    complete = indices is None
    if complete:
        if len(rows) != ref["blocks"] or full_raw.hexdigest() != ref["raw_sha256"]:
            raise ValueError("complete-file block count or SHA differs from the R73 formal reference")
    elif [r["index"] for r in rows] != indices:
        raise ValueError("processed block set differs from the prespecified sample")
    names = ["block_%05d.semantic.tar.xz" % r["index"] for r in rows]
    manifest = {"version": modules[2].SEMANTIC_VERSION, "dataset": dataset, "block_size": v2.BLOCK_RECORDS,
                "block_count": len(rows), "semantic_archives": names}
    if not complete:
        manifest["block_indices"] = [r["index"] for r in rows]
    totals = {}
    for branch in BRANCHES:
        archive_root = output / branch / "archive"
        (archive_root / "semantic_manifest.json").write_bytes(v2.encoded(manifest))
        total = sum(p.stat().st_size for p in archive_root.rglob("*") if p.is_file())
        block_total = sum(row["branches"][branch]["combined_block_archive_bytes"] for row in rows)
        if total != block_total + (archive_root / "semantic_manifest.json").stat().st_size:
            raise ValueError("archive accounting mismatch")
        if full_decoded[branch].hexdigest() != full_raw.hexdigest():
            raise ValueError("full decoded SHA mismatch")
        suffix_bytes = sum(row["branches"][branch]["combined_block_archive_bytes"] for row in rows[1:])
        totals[branch] = {"archive_bytes": total, "block_archive_bytes": block_total,
                          "semantic_archive_bytes": sum(r["branches"][branch]["semantic_archive_bytes"] for r in rows),
                          "residual_archive_bytes": sum(r["residual"]["bytes"] for r in rows),
                          "manifest_bytes": (archive_root / "semantic_manifest.json").stat().st_size,
                          "suffix_block_archive_bytes": suffix_bytes,
                          "full_decode_sha256": full_decoded[branch].hexdigest(),
                          "storage_policy_sha256": frozen_hashes[branch]}
    latent_policy = json.loads((output / "latent" / "storage.json").read_text())
    published = json.loads(Path(published_storage).read_text())
    parity = {"blocks": len(rows),
              "residual_sha_equal_blocks": sum(r["r73_parity"]["residual_sha_equal"] for r in rows),
              "latent_semantic_sha_equal_blocks": sum(r["r73_parity"]["latent_semantic_sha_equal"] for r in rows),
              "surface_equals_latent_semantic_blocks": sum(r["r73_parity"]["surface_semantic_sha_equal_latent"] for r in rows),
              "r73_block_archive_bytes_same_blocks": sum((r["r73_parity"]["r73_residual_bytes"] or 0) + (r["r73_parity"]["r73_semantic_bytes"] or 0) for r in rows),
              "latent_policy_equals_r73_published_on_policy_keys": policy_view(latent_policy) == policy_view(published),
              "r73_formal_archive_bytes_complete_file": ref["archive_bytes"]}
    s, l = totals["surface"], totals["latent"]
    result = {"status": "PASS", "version": VERSION, "v2_harness_version": v2.VERSION, "cache_boundary_verified": all(r["cache_boundary_verified"] for r in rows),
              "dataset": dataset, "scope_kind": "complete_file" if complete else "systematic_sample",
              "file_block_count": ref["blocks"], "block_indices": [r["index"] for r in rows],
              "blocks": len(rows), "raw_bytes": sum(r["raw_bytes"] for r in rows),
              "raw_sha256_of_processed_blocks": full_raw.hexdigest(), "plan_sha256": v2.file_sha(plan), "branches": totals,
              "raw_suffix_bytes": sum(r["raw_bytes"] for r in rows[1:]),
              "full_archive_saving_pct": 100 * (1 - l["archive_bytes"] / s["archive_bytes"]),
              "block_archive_saving_pct": 100 * (1 - l["block_archive_bytes"] / s["block_archive_bytes"]),
              "heldout_archive_saving_pct": (100 * (1 - l["suffix_block_archive_bytes"] / s["suffix_block_archive_bytes"])) if len(rows) > 1 else None,
              "surface_minus_latent_bytes": s["archive_bytes"] - l["archive_bytes"],
              "r73_parity": parity,
              "scope": ("Complete archive-size/SHA control" if complete else
                        "Systematic sample of original blocks round(i*(N-1)/19), i=0..19; sampled-block archive-size/SHA control")
                       + "; matched extraction trace; one shared residual; each arm independently refits storage on block 0 then freezes; no speed inference",
              "residual_backend": "native", "no_new_api_calls": True}
    v2.dump(output / "result.json", result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--raw-root", type=Path, required=True)
    p.add_argument("--plans-root", type=Path, required=True)
    p.add_argument("--reference-root", type=Path, required=True, help="R73 runs/formal/pool")
    p.add_argument("--dataset", required=True)
    p.add_argument("--sample", type=int, default=0, help="0 = complete file; k = systematic k-block sample")
    p.add_argument("--inventory", type=Path, help="R68 input_<DS>.json block inventory (sample mode)")
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    os.umask(0o022)
    args.source = args.source.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    modules = v2.load_runtime(args.source)
    hashes = v2.source_hashes(args.source)
    d = args.dataset
    raw_path = (args.raw_root / (d + ".log")).resolve()
    plan = args.plans_root / d / "extraction.json"
    ref = load_reference(args.reference_root / d)
    if v2.file_sha(plan) != ref["plan_sha256"]:
        raise ValueError("plan is not the R73 formal pool plan")
    inventory = None
    indices = None
    if args.sample:
        inventory = json.loads(args.inventory.read_text())
        if len(inventory["blocks"]) != ref["blocks"] or inventory["raw_sha256"] != ref["raw_sha256"]:
            raise ValueError("inventory differs from the R73 formal reference")
        indices = sample_indices(ref["blocks"], args.sample)
    here = Path(__file__).resolve().parent
    design = {"version": VERSION, "v2_harness_version": v2.VERSION, "dataset": d,
              "script_sha256": v2.file_sha(__file__), "v2_harness_sha256": v2.file_sha(v2.__file__),
              "cache_boundary_sha256": v2.file_sha(v2.boundary.__file__),
              "source": str(args.source), "source_hashes": hashes, "raw_path": str(raw_path),
              "plan": str(plan), "plan_sha256": v2.file_sha(plan), "reference": ref["result"],
              "block_records": v2.BLOCK_RECORDS, "storage_training_block": 0, "branches": BRANCHES,
              "sample": args.sample, "block_indices": indices,
              "inventory": str(args.inventory) if args.inventory else None,
              "inventory_sha256": v2.file_sha(args.inventory) if args.inventory else None,
              "backend": "native-residual-allgroup-or20; semantic-XZ6-extreme",
              "no_api": True, "no_throughput_claim": True, "driver_dir": str(here)}
    v2.dump(args.output / "DESIGN.json", design)
    try:
        result = run_dataset(raw_path, plan, d, args.output / d, args.source, modules, indices, inventory, ref,
                             args.plans_root / d / "r73_published_storage.json")
    except Exception as exc:
        result = {"dataset": d, "status": "FAIL", "error": str(exc), "traceback": traceback.format_exc(),
                  "cache_boundaries": copy.deepcopy(v2.CACHE_EVENTS)}
        v2.dump(args.output / (d + ".failure.json"), result)
    after = v2.source_hashes(args.source)
    v2.dump(args.output / "status.json", {"status": result["status"], "dataset": d, "version": VERSION,
                                          "source_unchanged": after == hashes})
    if after != hashes:
        raise ValueError("source changed during representation control")
    print(json.dumps({"dataset": d, "status": result["status"],
                      "surface": result.get("branches", {}).get("surface", {}).get("archive_bytes"),
                      "latent": result.get("branches", {}).get("latent", {}).get("archive_bytes"),
                      "saving_pct": result.get("full_archive_saving_pct")}), flush=True)
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
