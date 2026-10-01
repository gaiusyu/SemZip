#!/usr/bin/env python3
"""Validate and freeze an explicitly reviewed source-census holdout.

The detector is frozen before the holdout is drawn. This script requires a
separate row-level decision artifact, verifies that it covers the exact sample,
and computes label-level Wilson intervals. It never infers review decisions.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from pathlib import Path


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def wilson_interval(successes: int, trials: int, z: float = 1.959963984540054) -> tuple[float, float]:
    if trials == 0:
        return 0.0, 0.0
    p = successes / trials
    denominator = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / denominator
    margin = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials)) / denominator
    return center - margin, center + margin


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-root", type=Path, required=True)
    parser.add_argument("--decisions", type=Path, required=True)
    args = parser.parse_args()
    sample_path = args.result_root / "audit_sample.csv"
    actual_sha = sha256_file(sample_path)

    decision_document = json.loads(args.decisions.read_text(encoding="utf-8"))
    if decision_document.get("sample_sha256") != actual_sha:
        raise SystemExit(
            "holdout hash mismatch between audit sample and decision artifact: "
            f"{actual_sha} != {decision_document.get('sample_sha256')}"
        )
    decision_rows = decision_document.get("decisions")
    if not isinstance(decision_rows, list):
        raise SystemExit("decision artifact must contain a decisions list")
    decision_by_id = {}
    for decision in decision_rows:
        if not isinstance(decision, dict) or not decision.get("call_id"):
            raise SystemExit("every decision must be an object with call_id")
        call_id = decision["call_id"]
        if call_id in decision_by_id:
            raise SystemExit(f"duplicate review decision for {call_id}")
        decision_by_id[call_id] = decision

    with sample_path.open(newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    sample_ids = {row["call_id"] for row in rows}
    decision_ids = set(decision_by_id)
    if sample_ids != decision_ids:
        missing = sorted(sample_ids - decision_ids)
        extra = sorted(decision_ids - sample_ids)
        raise SystemExit(f"decision coverage mismatch: missing={missing}, extra={extra}")

    source_summary = json.loads((args.result_root / "source_summary.json").read_text(encoding="utf-8"))
    valid_categories = set(source_summary["category_counts"])
    reviewed = []
    categories = sorted({category for row in rows for category in row["categories"].split(";") if category})
    for row in rows:
        decision = decision_by_id[row["call_id"]]
        manual_labels = decision.get("manual_labels")
        notes = decision.get("reviewer_notes")
        if not isinstance(manual_labels, list) or not all(isinstance(label, str) for label in manual_labels):
            raise SystemExit(f"manual_labels must be a string list for {row['call_id']}")
        if len(manual_labels) != len(set(manual_labels)):
            raise SystemExit(f"duplicate manual label for {row['call_id']}")
        unknown = set(manual_labels) - valid_categories
        if unknown:
            raise SystemExit(f"unknown manual labels for {row['call_id']}: {sorted(unknown)}")
        if not isinstance(notes, str) or not notes.strip():
            raise SystemExit(f"reviewer_notes is required for {row['call_id']}")
        output = dict(row)
        output["manual_labels"] = ";".join(manual_labels)
        output["is_true_candidate"] = "true" if manual_labels else "false"
        output["reviewer_notes"] = notes
        reviewed.append(output)
    write_csv(args.result_root / "audit_reviewed.csv", reviewed)

    strata = []
    for category in categories:
        subset = [row for row in reviewed if category in row["categories"].split(";")]
        true_count = sum(category in row["manual_labels"].split(";") for row in subset)
        lower, upper = wilson_interval(true_count, len(subset))
        strata.append(
            {
                "stratum": category,
                "sampled": len(subset),
                "confirmed": true_count,
                "observed_precision": true_count / len(subset),
                "wilson_95_low": lower,
                "wilson_95_high": upper,
            }
        )
    negative_subset = [row for row in reviewed if not row["categories"]]
    missed = sum(bool(row["manual_labels"]) for row in negative_subset)
    miss_low, miss_high = wilson_interval(missed, len(negative_subset))
    write_csv(args.result_root / "audit_by_stratum.csv", strata)

    false_positive_labels = []
    false_negative_labels = []
    for row in reviewed:
        automatic = {label for label in row["categories"].split(";") if label}
        manual = {label for label in row["manual_labels"].split(";") if label}
        false_positive_labels.extend(
            {"call_id": row["call_id"], "label": label}
            for label in sorted(automatic - manual)
        )
        false_negative_labels.extend(
            {"call_id": row["call_id"], "label": label}
            for label in sorted(manual - automatic)
        )
    audit_summary = {
        "holdout_sha256": actual_sha,
        "holdout_seed": source_summary["audit_seed"],
        "candidate_rows": sum(bool(row["categories"]) for row in reviewed),
        "candidate_rows_with_confirmed_labels": sum(
            bool(row["categories"]) and bool(row["manual_labels"]) for row in reviewed
        ),
        "negative_rows": len(negative_subset),
        "false_positive_labels": false_positive_labels,
        "false_negative_labels": false_negative_labels,
        "per_stratum": strata,
        "negative_probe": {
            "sampled": len(negative_subset),
            "missed_under_frozen_rubric": missed,
            "observed_miss_rate": missed / len(negative_subset),
            "wilson_95_low": miss_low,
            "wilson_95_high": miss_high,
        },
        "scope": (
            "The audit validates visible source-level labels only. It does not claim that every "
            "candidate is profitable to compress, nor that the detector captures implicit or "
            "precomputed representations."
        ),
    }
    (args.result_root / "audit_summary.json").write_text(json.dumps(audit_summary, indent=2), encoding="utf-8")
    source_summary["status"] = "frozen_rules_with_explicit_reviewed_holdout"
    source_summary["audit_summary"] = "audit_summary.json"
    (args.result_root / "audited_source_summary.json").write_text(
        json.dumps(source_summary, indent=2), encoding="utf-8"
    )
    print(json.dumps(audit_summary, indent=2))


if __name__ == "__main__":
    main()
