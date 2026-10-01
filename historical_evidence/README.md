# Historical experiment evidence (separate cohorts)

This is an unpublished numerical-evidence package exported from retained,
unrounded machine-readable records. It preserves failures, no-trigger cases,
rejected candidates and zero gains. It is **not** the R71 strict main experiment,
not a new experiment, and not a model-training replay bundle. No model API or
compressor was run to produce this export.

## Contents and denominators

| File | Retained observations |
|---|---|
| `backbone_trials.json` | All 36 explicit-output-cap and 18 service-default trainings, 54 unique trainings total |
| `append_only_evolution.json` | All 9 dataset pilots, 75 original block pairs, 8 attempted updates |
| `gated_evolution.json` | All 7 dataset outcomes, 62 deployed original blocks, 6 triggers and 1 publication |
| `repair_response_controls.json` | All four conditions: two datasets, response-content replay and no-LLM-repair control |
| `SOURCE_RECORDS.json` | Opaque mapping to original evidence SHA-256 identities |
| `VALIDATION.json` | Export scope and completeness checks |
| `FILES.json` | SHA-256 and size of each package file, excluding this self-referential inventory |

### Backbone study

The explicit cohort combines thirty new trainings from five model families with
all six previously measured Qwen3-Coder trials using the same 16,384-token request
ceiling and original sample/first-prompt identities. There are **35 successful
reconstructions, one failed reconstruction, and four successful trials with no
semantic stream activity**. The separate default-output cohort retains seventeen
successes and one first-block fitting failure. Neither failure receives a ratio.
Do not replace either original failure with a later R71 recovery result.

Four default-cohort observations originally hit an empty-semantic-directory
harness error. Their original trial status and source identity are retained
alongside the already-recorded corrected harness replay. Their initial model
responses and plans were not regenerated. This correction differs from the
layout-dependent reconstruction failure, which remains a failure.

Each generation uses original block 0; original blocks 1--4 are evaluated with
100,000 physical LF records per block and unchanged tails. Saved plan entries,
active semantic blocks, and compression benefit are distinct quantities. The
public model labels are descriptive families; private gateway aliases are
represented only by opaque hashes and do not pin immutable provider versions.
Qwen3-32B thinking was disabled for its non-streaming endpoint. Equal requested
limits do not equalize inference compute, reasoning modes or token accounting.
Two generations per cell support descriptive comparisons only.

`training_seconds` measures the original synthesis subprocess, including model
service and verification, excluding prefix extraction, sampling, storage fit and
online encoding. Usage is retained as reported; reasoning tokens must not be
blindly added to completion tokens. A failed cell cannot silently shrink the
denominator of an asserted complete-cohort mean.

### Append-only evolution

All nine pilots remain present, including OpenSSH's no-trigger outcome. Eight
attempts use 46 calls and 395.23581601679325 seconds and publish no new rules.
All 75 block pairs retain equal complete archive byte counts and reconstruction
records. The Android observer uses the historical corrected physical-LF
protocol. HPC's retained harness correction is identified in its row.

These initial plans and monitoring protocols predate R71. Initial block training
is reused; this package does not recast those plans as new R71 training. Online
seconds in the rows use their historical in-process timer and are not comparable
to the new formal outer-CLI benchmark without separate measurement.

### Gated replacement evolution

The seven datasets were selected before observing these results: all prior
pilots with at least five original blocks, using ten blocks or the complete
shorter file. Blocks 1 and 2 trigger; only block 2 supplies the new candidate and
storage fit; block 3 validates; blocks 4 onward test the frozen publication.
All trigger thresholds, rejection outcomes, candidate/old/refitted-storage byte
counts, available timing-gate observations, costs and future outcomes remain.

OpenSSH alone publishes. Its future archive bytes change from 273,629 to
266,633; the storage-only control remains 273,629. Windows is rejected because
its validation candidate fails the required improvement against refitted
storage. **Windows's rejected candidate was not evaluated on future blocks.**
Its observed deployed future remains the old program; zero deployed gain must
not be interpreted as a measured counterfactual candidate gain. The same
counterfactual limitation applies to other rejected candidates.

`generation_fit_seconds`, `training_seconds`, `gate_seconds` and total paired
wall time are overlapping scopes, not quantities to sum indiscriminately.
Per-block compression/decode uses a separate CLI process. However, the
block-1/block-2 observer reused runtime modules within the dataset process
without R71's later uniform data-cache reset. The R71 cold-guard guarantee is
not retroactively claimed for this historical cohort.

### Historical model-response controls

The earlier OpenSSH and Android scripts map exact final prompt strings to the
JSON content of retained raw model response records, return that content at the
`call_llm` boundary, and reject any unmatched request. This is distinct from
loading only a parsed proposal cache. Existing reports record six and seventeen
response-content replays, versus five and four with LLM repair disabled; all
four conditions reproduce their original **parsed plans** with zero new API
calls. Deterministic compiler repairs remain enabled. The export independently
compares the retained parsed plans and records canonical-plan SHA-256 values.

This verifies those two fixed-response construction cases, not HTTP transport
replay, future model determinism or R71's sixteen fresh training pipelines.
No original request/response identifiers or private response text are exported
in this historical subpackage.

## Provenance and verification

```sh
python3 historical_evidence/verify_historical.py
```

Each observation points to an opaque source-record ID with its original file
SHA-256; per-cohort runtime source hash maps remain separate. Original private
paths, hostnames, service aliases, credentials, request IDs and unrestricted
error/traceback text are omitted. Failure stages and normalized classifications
remain where supported by the records. Sanitized files have new identities in
`FILES.json`; they are not presented as byte-identical copies of their sources.

`export_historical.py --evidence-root PATH` is the provenance extraction source.
It requires the original research evidence tree, which is intentionally not
bundled here. The portable verifier only needs this subpackage. Re-extracting
into an existing package changes outputs and requires regenerating `FILES.json`.
The final main results and formal timing belong to the separate integration
package; this evidence must never stand in for missing main measurements.
