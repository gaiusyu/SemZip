# Final results integration and saved rendering

The actual terminal seven-input integration passed for all 96 size cells, 72 formal cells, 216 formal observations and 29 paired blocks. Its output is shipped in `results/final/`. Frozen code, programs and policies are unchanged. The old initial snapshot is rejected because it is incomplete and uses a superseded sampling-time description.

## Inputs and invocation

Python 3.9+ is sufficient for validation. Generation additionally needs Matplotlib and NumPy (`python -m pip install -r requirements.txt`). Use explicit paths to these seven machine-readable inputs:

```
python integration/integrate_results.py \
  --snapshot /input/final_snapshot.json \
  --formal-summary /input/formal/summary.json \
  --formal-status /input/formal/status.json \
  --formal-design /input/formal/DESIGN.json \
  --representation /input/representation/results.json \
  --parity /input/payload_identity/observed.json \
  --sampling-replay /input/sampling/result.json \
  --output /output/new-final-results
```

Add `--validate-only` to check without writing. The destination must not exist. Inputs are never modified. A missing input, incomplete inventory, changed protocol, internally inconsistent digest, stale timing scope, or failed paired parity check exits nonzero. Recorded formal archive drift is retained as an ineligible cell with no median, not hidden by dropping its observations. All validation precedes output creation. Plotting errors clean the temporary staging directory; the final destination is committed only after every output succeeds. No raw file, codec, API or private workspace module is accessed.

`INPUT_CONTRACT.md` defines the seven inputs. `anonymous_results.schema.json` documents the minimal public output envelope. The Python validator implements the stricter arithmetic, inventory, schedule, timing and identity checks that JSON Schema cannot express.

## Outputs

- `anonymous_results.json`: explicit numeric/status/hash whitelist; no original filenames, absolute paths, commands, headers, endpoints, free-form errors, model prompts or model replies.
- `final_runs.json`: all 216 terminal observations, compatible with `scripts/report.py`; `observed_trial_status` preserves the original status. A reconstructed trial in a cell whose archives vary or disagree with first pass is marked `INELIGIBLE_CELL` for this simple report interface, so it cannot yield an invalid formal median. Unmodified statuses remain in `anonymous_results.json.formal_runs`.
- `generated/`: external compression and speed tables, all observation CSVs, training/sampling/deployment tables, representation control and data-derived prose, plus numeric provenance.
- `figures/`: PDF, SVG and PNG versions of the external ratio plot, formal throughput tradeoff and signed representation effects.
- `FILES.json`: output hashes and byte sizes. Input hashes and both generator hashes are preserved.

The generated scientific quantities reproduce the supplied validated evidence. This portable implementation does not promise byte-identical LaTeX wording or identical plotting-library output to a separately maintained manuscript generator. The output hashes and library versions make each generation auditable. TeX requires `booktabs`, `longtable` and `graphicx`.

A full-suite aggregate is emitted only when all 16 size cells for that method pass. A formal aggregate requires all 12 fixed-cohort cells with three valid, archive-consistent observations. Failed trials are retained, never replaced. Diagnostic first-pass/task timers are kept separate from formal outer-process latency. Separate zero-API sampling replay is never reported as originally measured total training latency. The LogLite label names the disclosed adaptation.

The component/suffix validation was checked on all 29 existing paired blocks and
the 14 complete main-file ledgers available at the time of the bounded audit.
Thirteen focused in-memory malformed-metadata checks were rejected across this
validator and the manuscript representation renderer: altered suffix counters
with recomputed percentages, shifted components with unchanged totals, altered
raw suffix sizes, altered main suffix archives/manifests, and a failed paired
status. No malformed record was saved as experimental evidence. The real
in-progress snapshot was still rejected as a final input; this does not validate
the complete terminal 216-trial integration path before it exists.

## Re-render the shipped numeric result

After the final result directory has been populated, a reader can regenerate
tables and figures from its anonymous numeric record without obtaining the seven
original experiment ledgers or accessing private workspace paths:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 integration/render_saved_results.py \
  --results results/final --output ../regenerated-results
```

Use the actual saved integration-output directory containing `FILES.json` if it
is located elsewhere. The command verifies every listed saved file, binds the
renderer to the recorded generator hash, and requires regenerated TeX/CSV outputs
to match their saved bytes. Figures are regenerated with the installed libraries;
plot binary equality across environments is not promised. The new output
includes a hash-bound `RENDER_CHECK.json`. Inputs are unchanged and an existing
output directory is refused. This repeats rendering from an already validated
record, not the seven-input measurement validation or a compression experiment.
The actual terminal-data rendering check passed: all 22 TeX/CSV outputs are
byte-identical. `metadata/FINAL_RESULT_REPRODUCTION.json` binds the saved manifest,
input record and renderer. No complete input was fabricated for this check.

## Scope limits

This tool integrates main results and the fixed four-dataset representation control. Historical backbone/evolution evidence is separately packaged under `historical_evidence/`; saved original LLM exchanges belong under `offline_evidence/`. Neither is silently merged with the final main protocol. The integration tool itself does not perform response replay, compression or independent reconstruction; it validates the existing certificates and arithmetic. Actual terminal-input integration and saved-record rendering both passed. The earlier `VALIDATION.json` is retained as a historical targeted-development check, not relabeled as the final integration run.

The initial full integration rejected a reporting-schema mismatch: the original sampling replay records `original_training_modified=false` and explicit separate-replay timing text, rather than a proposed `measured_during_original_training` field. The validator now requires the actual original fields and per-dataset scope text. No input evidence or measured value was changed.
