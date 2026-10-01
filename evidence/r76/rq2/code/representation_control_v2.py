#!/usr/bin/env python3
"""Matched-replay representation control; no API and no source modifications.

This is a size/SHA study, not a throughput benchmark. Only block 0 fits storage.
The two branches share one actual extraction trace and one residual archive.
"""
from __future__ import annotations
import argparse
import copy
import dataclasses
import hashlib
import json
import lzma
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import traceback
import runtime_cache_boundary as boundary

VERSION = "R70-REPRESENTATION-CONTROL-V2-STRICT-20260923"
CACHE_EVENTS = []

def clear_state(modules, phase):
    runtime = Path(modules[0].__file__).resolve().parent
    event = boundary.clear(runtime, phase)
    CACHE_EVENTS.append(event)
    return event

BLOCK_RECORDS = 100000
COHORT = ["Linux", "HPC", "OpenSSH", "Android"]
BRANCHES = ["latent", "surface"]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for data in iter(lambda: f.read(1048576), b""):
            h.update(data)
    return h.hexdigest()


def source_hashes(source):
    return {str(p.relative_to(source)): file_sha(p) for p in sorted(source.rglob("*"))
            if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"}


def dump(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def encoded(value):
    # Match the production frontend's exact metadata JSON representation.
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def load_runtime(source):
    # Refuse preloaded modules from a different checkout rather than silently
    # creating a hybrid runtime through Python's module cache.
    source = Path(source).resolve()
    # No inherited experimental switch may silently change the two routes.
    # There are no API calls, so no SEMZIP environment credentials are needed.
    for key in list(os.environ):
        if key.startswith("PARE_") or key.startswith("SEMZIP_"):
            del os.environ[key]
    os.environ["PARE_NO_BENEFIT_FIXED_CODEC"] = "1"
    os.environ["PARE_NO_BENEFIT_LEFT_CONTEXT_DELTA"] = "1"
    for name, location in (("semzip_pure", source / "runtime"),
                           ("pare_dataset_extract", source / "runtime"),
                           ("semantic_codec_frontend", source / "backend")):
        if name in sys.modules and Path(sys.modules[name].__file__).resolve().parent != location:
            raise RuntimeError("runtime module already loaded from a different checkout: " + name)
    sys.path[:0] = [str(source / "runtime"), str(source / "backend"), str(source)]
    import semzip_pure as semzip
    import pare_dataset_extract as codec
    import semantic_codec_frontend as front
    front.activate_execution_environment(front.EXECUTION_ENVIRONMENT)
    return semzip, codec, front


def replay_once(raw, extraction, modules):
    clear_state(modules, "matched_trace_entry")
    try:
        return _replay_once(raw, extraction, modules)
    finally:
        clear_state(modules, "matched_trace_exit")


def _replay_once(raw, extraction, modules):
    """Run the unchanged production replay once; capture its admitted events."""
    semzip, codec, front = modules
    payload = json.loads(Path(extraction).read_text())
    loaded = semzip.load_replay_plan(str(extraction), payload["dataset"])
    if loaded is None:
        raise ValueError("could not load fixed extraction plan")
    specs, markers = loaded
    specs = [s for s in specs if not semzip.is_stage_residual_spec(s)]
    rejected = [s.tag for s in specs if not semzip.regex_has_fixed_alpha_or_structure(s.pattern)]
    specs = [s for s in specs if semzip.regex_has_fixed_alpha_or_structure(s.pattern) and s.tag in markers]
    markers = {s.tag: markers[s.tag] for s in specs}
    if len(set(markers.values())) != len(markers):
        raise ValueError("duplicate placeholders are unsupported, not silently renamed")
    original = raw.decode("latin-1")
    values = {}
    event_hash = hashlib.sha256()
    event_counts = {}
    route_surfaces = {}
    # This callback is called only after the real runtime admits a match.
    original_store = semzip.stored_value_for_match

    def store(spec, selected, match):
        stored = original_store(spec, selected, match)
        literal = semzip.validation_render_value(spec, selected)
        # The guard's replacement must recreate the entire matched span.
        if semzip.format_replacement_fast(spec.replacement, literal, match) != match.group(0):
            raise ValueError("admitted match did not recreate its original span")
        route_surfaces.setdefault(spec.tag, []).append(literal)
        event_counts[spec.tag] = event_counts.get(spec.tag, 0) + 1
        # Coordinates are in that rule's current intermediate text, not falsely
        # claimed to be positions in the original byte stream after prior edits.
        event_hash.update(encoded({"tag": spec.tag, "start": match.start(), "end": match.end(),
                                   "selected_sha": sha(selected.encode("latin-1")),
                                   "full_match_sha": sha(match.group(0).encode("latin-1")),
                                   "literal_sha": sha(literal.encode("latin-1"))}) + b"\n")
        return stored

    semzip.stored_value_for_match = store
    try:
        residual = semzip.apply_specs_to_original_text(original, specs, markers, values, trusted_fast_path=True)
    finally:
        semzip.stored_value_for_match = original_store
    if semzip.restore_validation_text(residual, specs, markers, values) != original:
        raise ValueError("production replay failed before representation fork")
    if raw.count(b"\n") != residual.count("\n") or raw.endswith(b"\n") != residual.endswith("\n"):
        raise ValueError("extraction changed physical block boundaries")
    before_context = {tag: list(v) for tag, v in values.items()}
    direct = semzip.materialize_direct_context_projectors(original, residual, values, markers, specs)
    cross = semzip.materialize_cross_function_contexts(residual, values, markers, specs)
    if semzip.restore_validation_text(residual, specs, markers, values) != original:
        raise ValueError("context materialization changed reconstruction")
    if event_counts != {k: len(v) for k, v in values.items()}:
        raise ValueError("context materialization changed matched-value counts")
    for spec in specs:
        surface = [semzip.validation_render_stored_value(spec, v) for v in values.get(spec.tag, [])]
        if surface != route_surfaces.get(spec.tag, []):
            raise ValueError("post-context values do not match admitted literal routes")
    return {"dataset": payload["dataset"], "specs": specs, "markers": markers,
            "values": values, "residual": residual, "route_surfaces": route_surfaces,
            "trace": {"event_sha256": event_hash.hexdigest(), "counts": event_counts,
                      "coordinate_scope": "rule-intermediate-text", "residual_sha256": sha(residual.encode("latin-1")),
                      "markers_sha256": sha(encoded(markers)), "precontext_values_sha256": sha(encoded(before_context)),
                      "grouped_values_sha256": sha(encoded(values)), "direct_context": direct,
                      "cross_context": cross, "rejected_structure_tags": rejected}}


def branch_state(trace, branch, modules):
    semzip, codec, front = modules
    specs = copy.deepcopy(trace["specs"])
    values = copy.deepcopy(trace["values"])
    changed = []
    if branch == "surface":
        for spec in specs:
            if not values.get(spec.tag):
                continue
            # Only streams whose serializer actually invokes the reversible
            # program change. Auto/other generic streams remain byte-identical.
            if spec.kind not in ("open_function", "open_context_delta", "open_context_dict"):
                continue
            if spec.program.get("op") == "auto_codec":
                continue
            literals = trace["route_surfaces"][spec.tag]
            old_kind = spec.kind
            if old_kind in ("open_context_delta", "open_context_dict"):
                _, contexts = codec.parse_context_pairs(values[spec.tag])
                if len(contexts) != len(literals):
                    raise ValueError("context cardinality changed")
                values[spec.tag] = [json.dumps([v, c], separators=(",", ":"), ensure_ascii=False)
                                    for v, c in zip(literals, contexts)]
                spec.kind = "open_context_dict"
            else:
                values[spec.tag] = list(literals)
                # Normal generic field-storage route. It may use lossless
                # width-aware integer/dictionary codecs; no semantic inverse.
                spec.kind = "auto"
            spec.program = {"op": "auto_codec"}
            spec.context_group = 0
            spec.context_ref = ""
            changed.append({"tag": spec.tag, "from": old_kind, "to": spec.kind,
                            "count": len(literals), "surface_sha256": sha(encoded(literals))})
    if semzip.restore_validation_text(trace["residual"], specs, trace["markers"], values) != semzip.restore_validation_text(
            trace["residual"], trace["specs"], trace["markers"], trace["values"]):
        raise ValueError("identity branch changed literal reconstruction")
    meta = front._semantic_metadata(trace["dataset"], specs, trace["markers"], values)
    return specs, values, meta, changed


def base_streams(root, trace, branch, modules):
    _, codec, _ = modules
    _, values, metadata, changed = branch_state(trace, branch, modules)
    root.mkdir()
    # Base writer + byte-identical aliases only. Generic representation search
    # is performed separately, once on block 0, then replaced by R54 replay.
    codec._r52_parent_save(root, metadata, values, trace["residual"])
    return metadata, changed


def verify_streams(root, metadata, residual, expected, modules):
    # Decode the archive's own metadata/streams after discarding encoder input
    # caches and compiled-callable globals. No fitted policy is required here.
    clear_state(modules, "semantic_inverse_entry")
    try:
        modules[2].activate_execution_environment(metadata.get("execution_environment"))
        restored = modules[1].restore_text(residual, root / "dataset_extract", metadata).encode("latin-1")
        if restored != expected:
            raise ValueError("semantic stream reconstruction differs from original bytes")
        return restored
    finally:
        clear_state(modules, "semantic_inverse_exit")


def fit_policy(base, metadata, residual, expected, dataset, modules):
    _, c, front = modules
    columns, modes = {}, {}
    original_column, original_numeric = c.r44_column_write, c.r47_numeric_recode
    original_sharing, original_alias = c.r47_share_subfields, c._r45_transform_base
    subfield_modes, active_sharing = {}, [None]

    def sharing(directory, meta, strategy):
        previous = active_sharing[0]
        active_sharing[0] = strategy
        try:
            return original_sharing(directory, meta, strategy)
        finally:
            active_sharing[0] = previous

    def alias(directory, meta, profile):
        strategy = active_sharing[0]
        if strategy is not None and not c._R54_ONLINE:
            logical = {}
            for item in c.r47_walk(meta):
                if item.get("kind") == "r44_dictionary":
                    entry = item["ids"]
                    if entry["file"].startswith("r47_subfield_ids_"):
                        prior = logical.setdefault(entry["file"], entry["mode"])
                        if prior != entry["mode"]:
                            raise ValueError("Conflicting modes for one logical subfield ID file")
            # Capture before physical aliases remove logical names. Speculative
            # strategies stay separate; publish only the selected strategy.
            subfield_modes[strategy] = logical
        return original_alias(directory, meta, profile)

    def column(directory, name, values):
        entry = original_column(directory, name, values)
        if entry:
            for col in entry["columns"]:
                if "numeric" in col:
                    n = col["numeric"]
                    columns[n["file"][:-4]] = n["mode"]
        return entry

    def numeric(directory, meta, mode):
        refs = []
        for item in c.r47_walk(meta):
            if item.get("kind") == "r44_dictionary":
                refs.append((item["ids"]["file"], item["ids"]))
            if isinstance(item.get("numeric"), dict):
                refs.append((item["numeric"]["file"], item["numeric"]))
        result = original_numeric(directory, meta, mode)
        modes[mode] = {name: entry["mode"] for name, entry in refs}
        return result

    policy = {"version": 1, "dataset": dataset, "training_raw_sha256": sha(expected),
              "training_block_index": 0, "online_search": False, "programs": [], "column_numeric": {},
              "numeric_modes": {}, "subfield_ids": {}, "unseen_column_numeric_default": "delta",
              "recipe": {"generic_representation": "file_alias"},
              "proposal_source": "fixed existing extraction; zero API; paired representation control"}
    if not (base / "dataset_extract").exists():
        if metadata.get("streams"):
            raise ValueError("nonempty stream metadata without a directory")
        return policy, {"empty": True}
    c.r44_column_write, c.r47_numeric_recode = column, numeric
    c.r47_share_subfields, c._r45_transform_base = sharing, alias
    try:
        with tempfile.TemporaryDirectory(prefix="r70-fit-") as tmp:
            trial = Path(tmp) / "adaptive"
            shutil.copytree(base, trial)
            trial_meta = copy.deepcopy(metadata)
            clear_state(modules, "fit_adaptive_entry")
            c.r49_transform(trial / "dataset_extract", trial_meta, "adaptive")
            # Boundary clearing resets diagnostic counters. Freeze decisions
            # before the deliberately cold inverse verification.
            recipe = {row["stage"]: row["selected"] for row in c.R47_STATS}
            observed = copy.deepcopy(c.R47_STATS)
            verify_streams(trial, trial_meta, residual, expected, modules)
            mode = recipe.get("numeric", "unchanged")
            policy.update(recipe=recipe, column_numeric=columns, numeric_modes=modes.get(mode, {}))
            if mode == "adaptive":
                recipe["numeric"] = "frozen"
            selected = recipe.get("subfield_sharing", "unchanged")
            if selected != "unchanged":
                if selected not in subfield_modes:
                    raise RuntimeError("Selected sharing strategy has no captured logical mode snapshot")
                policy["subfield_ids"] = copy.deepcopy(subfield_modes[selected])
            fixed = Path(tmp) / "fixed"
            shutil.copytree(base, fixed)
            fixed_meta = copy.deepcopy(metadata)
            c.r54_apply(fixed, fixed_meta, policy)
            verify_streams(fixed, fixed_meta, residual, expected, modules)
            frozen_size, adaptive_size = c.r47_cost(fixed, fixed_meta), c.r47_cost(trial, trial_meta)
            if frozen_size != adaptive_size:
                raise ValueError("block0 frozen replay differs from fitted storage candidate size")
            return policy, {"adaptive_semantic_archive_bytes": adaptive_size,
                            "frozen_semantic_archive_bytes": frozen_size, "decisions": observed,
                            "fitter_version": "LOGICAL-SUBFIELD-MODES-BEFORE-ALIAS-V2",
                            "subfield_modes_by_strategy": subfield_modes,
                            "selected_subfield_strategy": selected}
    finally:
        c.r44_column_write, c.r47_numeric_recode = original_column, original_numeric
        c.r47_share_subfields, c._r45_transform_base = original_sharing, original_alias


def encode_semantic(trace, branch, raw, block_index, archive, policy_path, fit_path, modules):
    clear_state(modules, "semantic_branch_entry_" + branch)
    try:
        return _encode_semantic(trace, branch, raw, block_index, archive, policy_path, fit_path, modules)
    finally:
        clear_state(modules, "semantic_branch_exit_" + branch)


def _encode_semantic(trace, branch, raw, block_index, archive, policy_path, fit_path, modules):
    _, codec, front = modules
    front.reset_archive_cache()
    with tempfile.TemporaryDirectory(prefix="r70-stream-") as name:
        root = Path(name) / "base"
        metadata, changed = base_streams(root, trace, branch, modules)
        verify_streams(root, metadata, trace["residual"], raw, modules)
        if block_index == 0:
            if policy_path.exists():
                raise ValueError("refusing to overwrite an existing fitted policy")
            policy, fit = fit_policy(root, metadata, trace["residual"], raw, trace["dataset"], modules)
            dump(policy_path, policy)
            dump(fit_path, fit)
        policy = json.loads(policy_path.read_text())
        codec.r54_apply(root, metadata, policy)
        verify_streams(root, metadata, trace["residual"], raw, modules)
        (root / "metadata.json").write_bytes(encoded(metadata))
        members = [{"name": str(p.relative_to(root)), "bytes": p.stat().st_size, "sha256": file_sha(p)}
                   for p in sorted(root.rglob("*")) if p.is_file()]
        front.create_semantic_archive(root, archive)
        # The original archive-only decoder path, with no plan/policy loaded.
        restored = Path(name) / "restored"
        front.extract_semantic_archive(archive, restored)
        archived_meta = json.loads((restored / "metadata.json").read_text())
        verify_streams(restored, archived_meta, trace["residual"], raw, modules)
        return {"semantic_archive_bytes": archive.stat().st_size, "semantic_archive_sha256": file_sha(archive),
                "metadata_member_bytes": (restored / "metadata.json").stat().st_size,
                "stream_member_bytes": sum(m["bytes"] for m in members if m["name"] != "metadata.json"),
                "members": members, "changed_program_streams": changed,
                "storage_policy_sha256": file_sha(policy_path), "streams": archived_meta.get("streams", []),
                "archive_only_semantic_decode_sha256": sha(raw)}


def native(command, cwd, log):
    with open(log, "xb") as out:
        proc = subprocess.run(command, cwd=str(cwd), stdout=out, stderr=subprocess.STDOUT, check=False)
    if proc.returncode or b"Error processing chunk" in Path(log).read_bytes():
        raise RuntimeError("native residual CLI failed; see " + str(log))


def encode_residual(residual, dataset, block_index, dest, source, mode):
    data = residual.encode("latin-1")
    if mode == "xz-smoke":
        dest.write_bytes(lzma.compress(data, preset=6 | lzma.PRESET_EXTREME))
        restored = lzma.decompress(dest.read_bytes())
    else:
        with tempfile.TemporaryDirectory(prefix="r70-native-", dir=str(dest.parent)) as name:
            work = Path(name)
            logdir = work / "Logs" / dataset
            logdir.mkdir(parents=True)
            (logdir / (dataset + ".log")).write_bytes(data)
            empty = work / "empty_plan.json"
            empty.write_text('{"version":1,"specs":[]}\n')
            native([str(source / "backend/Delog_plan_compress"), dataset, "text", "100000", "1", "0",
                    "lzma", "normal", str(empty), "--residual-allgroup-or20"], work,
                   dest.parent / ("block_%05d.encode.log" % block_index))
            files = list((work / "output" / dataset).glob("chunk_*.tar.xz"))
            if len(files) != 1 or files[0].name != "chunk_0.tar.xz":
                raise ValueError("expected one native residual chunk")
            shutil.copyfile(files[0], dest)
            dec = work / "decode-input"
            dec.mkdir()
            (dec / "chunk_0.tar.xz").symlink_to(dest.resolve())
            output = work / "decoded.log"
            native([str(source / "backend/decompress"), str(dec), str(output), "1"], work,
                   dest.parent / ("block_%05d.decode.log" % block_index))
            restored = output.read_bytes()
    if restored != data:
        raise ValueError("shared residual archive roundtrip differs")
    return {"bytes": dest.stat().st_size, "sha256": file_sha(dest), "decoded_sha256": sha(restored)}, restored


def iter_blocks(path):
    with Path(path).open("rb") as f:
        while True:
            rows = []
            for _ in range(BLOCK_RECORDS):
                line = f.readline()
                if not line:
                    break
                rows.append(line)
            if not rows:
                return
            yield b"".join(rows)


def run_dataset(raw_path, plan, dataset, output, source, modules, mode="native"):
    output.mkdir(parents=True, exist_ok=False)
    (output / "shared_residual").mkdir()
    for branch in BRANCHES:
        (output / branch / "archive/semantic").mkdir(parents=True)
    frozen_hashes = {}
    rows = []
    full_raw = hashlib.sha256()
    full_decoded = {branch: hashlib.sha256() for branch in BRANCHES}
    for index, raw in enumerate(iter_blocks(raw_path)):
        CACHE_EVENTS.clear()
        full_raw.update(raw)
        trace = replay_once(raw, plan, modules)
        if trace["dataset"] != dataset:
            raise ValueError("plan dataset differs")
        row = {"index": index, "raw_bytes": len(raw), "raw_sha256": sha(raw), "trace": trace["trace"],
               "lf_count": raw.count(b"\n"), "records": raw.count(b"\n") + int(not raw.endswith(b"\n")),
               "training": index == 0, "branches": {}}
        suffix = ".xz" if mode == "xz-smoke" else ".tar.xz"
        residual_path = output / "shared_residual" / ("chunk_%d%s" % (index, suffix))
        row["residual"], decoded_residual = encode_residual(trace["residual"], dataset, index, residual_path, source, mode)
        for branch in BRANCHES:
            base = output / branch
            archive = base / "archive/semantic" / ("block_%05d.semantic.tar.xz" % index)
            policy = base / "storage.json"
            info = encode_semantic(trace, branch, raw, index, archive, policy, base / "fit.json", modules)
            if index == 0:
                frozen_hashes[branch] = file_sha(policy)
            elif file_sha(policy) != frozen_hashes[branch]:
                raise ValueError("storage policy changed after block0")
            target = base / "archive" / residual_path.name
            os.link(residual_path, target)
            with tempfile.TemporaryDirectory(prefix="r70-full-decode-") as tmp:
                unpacked = Path(tmp)
                modules[2].extract_semantic_archive(archive, unpacked)
                meta = json.loads((unpacked / "metadata.json").read_text())
                actual = verify_streams(unpacked, meta, decoded_residual.decode("latin-1"), raw, modules)
            full_decoded[branch].update(actual)
            info["residual_archive_sha256"] = file_sha(target)
            info["archive_only_full_decode_sha256"] = sha(actual)
            info["combined_block_archive_bytes"] = info["semantic_archive_bytes"] + row["residual"]["bytes"]
            row["branches"][branch] = info
        row["cache_boundaries"] = copy.deepcopy(CACHE_EVENTS)
        row["cache_boundary_verified"] = all(e["all_data_caches_empty"] for e in CACHE_EVENTS)
        rows.append(row)
        dump(output / "blocks.json", rows)
        print(json.dumps({"dataset": dataset, "block": index,
                          "latent": row["branches"]["latent"]["combined_block_archive_bytes"],
                          "surface": row["branches"]["surface"]["combined_block_archive_bytes"], "status": "PASS"}), flush=True)
    if not rows:
        raise ValueError("empty research dataset is not part of the fixed cohort")
    manifest = {"version": modules[2].SEMANTIC_VERSION, "dataset": dataset, "block_size": BLOCK_RECORDS,
                "block_count": len(rows), "semantic_archives": ["block_%05d.semantic.tar.xz" % i for i in range(len(rows))]}
    totals = {}
    for branch in BRANCHES:
        archive_root = output / branch / "archive"
        (archive_root / "semantic_manifest.json").write_bytes(encoded(manifest))
        total = sum(p.stat().st_size for p in archive_root.rglob("*") if p.is_file())
        block_total = sum(row["branches"][branch]["combined_block_archive_bytes"] for row in rows)
        if total != block_total + (archive_root / "semantic_manifest.json").stat().st_size:
            raise ValueError("archive accounting mismatch")
        if full_decoded[branch].hexdigest() != full_raw.hexdigest():
            raise ValueError("full file decoded SHA mismatch")
        suffix_bytes = sum(row["branches"][branch]["combined_block_archive_bytes"] for row in rows[1:])
        totals[branch] = {"archive_bytes": total, "semantic_archive_bytes": sum(r["branches"][branch]["semantic_archive_bytes"] for r in rows),
                          "residual_archive_bytes": sum(r["residual"]["bytes"] for r in rows),
                          "manifest_bytes": (archive_root / "semantic_manifest.json").stat().st_size,
                          "suffix_block_archive_bytes": suffix_bytes,
                          "full_decode_sha256": full_decoded[branch].hexdigest(),
                          "storage_policy_sha256": frozen_hashes[branch]}
    result = {"status": "PASS", "version": VERSION, "cache_boundary_verified": True, "dataset": dataset, "blocks": len(rows), "raw_bytes": sum(r["raw_bytes"] for r in rows),
              "raw_sha256": full_raw.hexdigest(), "plan_sha256": file_sha(plan), "branches": totals,
              "raw_suffix_bytes": sum(r["raw_bytes"] for r in rows[1:]),
              "full_archive_saving_pct": 100 * (1 - totals["latent"]["archive_bytes"] / totals["surface"]["archive_bytes"]),
              "heldout_archive_saving_pct": (100 * (1 - totals["latent"]["suffix_block_archive_bytes"] / totals["surface"]["suffix_block_archive_bytes"])) if len(rows) > 1 else None,
              "scope": "Complete archive-size/SHA control; matched extraction trace; independently refitted block0 storage; no speed inference",
              "residual_backend": mode, "no_new_api_calls": True}
    dump(output / "result.json", result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", type=Path, required=True)
    p.add_argument("--raw-root", type=Path, required=True)
    p.add_argument("--plans-root", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    os.umask(0o022)
    args.source = args.source.resolve()
    args.output.mkdir(parents=True, exist_ok=False)
    modules = load_runtime(args.source)
    hashes = source_hashes(args.source)
    protocol = {"version": VERSION, "cache_boundary_sha256": file_sha(boundary.__file__), "cohort": COHORT, "block_records": BLOCK_RECORDS, "storage_training_block": 0,
                "branches": BRANCHES, "source_hashes": hashes, "script_sha256": file_sha(__file__),
                "backend": "native-residual-allgroup-or20; semantic-XZ6-extreme",
                "no_api": True, "no_throughput_claim": True,
                "plans": {d: file_sha(args.plans_root / d / "extraction.json") for d in COHORT}}
    dump(args.output / "DESIGN.json", protocol)
    results = []
    for dataset in COHORT:
        try:
            result = run_dataset(args.raw_root / (dataset + ".log"), args.plans_root / dataset / "extraction.json",
                                 dataset, args.output / dataset, args.source, modules)
        except Exception as exc:
            result = {"dataset": dataset, "status": "FAIL", "error": str(exc), "traceback": traceback.format_exc()}
            result["cache_boundaries"] = copy.deepcopy(CACHE_EVENTS)
            dump(args.output / (dataset + ".failure.json"), result)
        results.append(result)
        dump(args.output / "results.json", results)
    after = source_hashes(args.source)
    if after != hashes:
        raise ValueError("source changed during representation control")
    dump(args.output / "status.json", {"status": "PASS" if all(r["status"] == "PASS" for r in results) else "COMPLETE_WITH_FAILURES",
                                      "passed": sum(r["status"] == "PASS" for r in results), "datasets": len(results),
                                      "source_unchanged": True, "version": VERSION,
                                      "cache_boundary_sha256": file_sha(boundary.__file__)})


if __name__ == "__main__":
    main()
