#!/bin/bash
# Regenerate every manuscript table/text input from the saved records in evidence/ (no experiments, no API).
# usage: bash analysis/run_all.sh NEW_STAGE_DIR      (Python 3.9+; figures additionally need matplotlib and numpy,
#        and are skipped with a message when matplotlib is missing)
set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"; STAGE="${1:?usage: run_all.sh NEW_STAGE_DIR}"
python3 "$REPO/analysis/stage_layout.py" --out "$STAGE"
STAGE="$(cd "$STAGE" && pwd)"
cd "$STAGE/r73_quality_gate_20260927/analysis"
mkdir -p paper_out
python3 stability.py       > build_stability_json.log 2>&1 && cp stability.json paper_out/stability.json
python3 stability_tex.py   > build_stability.log 2>&1
python3 supplement_tex.py  > build_supp.log 2>&1
python3 evolution_tex.py   > build_evo.log 2>&1
python3 run_paper_tables.py > build_tables.log 2>&1
cd "$STAGE/r76_additional_20260929/analysis"
mkdir -p paper_out
python3 ladder.py > ladder.log 2>&1 && cp ladder.json paper_out/ladder.json
python3 r76_tables.py > build_r76.log 2>&1
echo "outputs: $STAGE/r73_quality_gate_20260927/analysis/paper_out  $STAGE/r76_additional_20260929/analysis/paper_out"
# byte-compare every recorded output in analysis/reference_outputs/{r73,r76} with the regenerated one
n=0; bad=0
for pair in "r73:$STAGE/r73_quality_gate_20260927/analysis/paper_out" "r76:$STAGE/r76_additional_20260929/analysis/paper_out"; do
  sub="${pair%%:*}"; out="${pair#*:}"
  for ref in "$REPO/analysis/reference_outputs/$sub"/*; do
    n=$((n + 1)); f="$(basename "$ref")"
    cmp -s "$ref" "$out/$f" || { bad=$((bad + 1)); echo "DIFFERS: $sub/$f"; }
  done
done
echo "byte-identical to analysis/reference_outputs/{r73,r76}: $((n - bad))/$n"
[ "$bad" -eq 0 ]
