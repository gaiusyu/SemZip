# Preserved source-grounded study evidence

This subpackage supports the paper's source-code prevalence and paired-field
motivation study. It is separate from the R71 compression campaign, its fixed
100,000-record block contract, formal throughput trials, and generated LLM
programs. It contains historical observations and the original study scripts;
exporting and checking it did not run a source census, an API, or a compressor.
The full raw corpora and historical compressed payloads are not included here.

## Verify the saved evidence

From this directory, run:

```sh
python3 verify_source_study.py
```

The verifier uses only Python's standard library. It checks every file against
`FILES.json`, original-to-export hash bindings, per-repository/call counts,
source-call identity hashes, every sampled decision, label-agreement and Wilson
interval arithmetic, source-family identities, all Q2 numeric ratios, and the
recorded reconstruction/hash ledger. It does not re-establish the correctness of
recorded labels, inspect full original corpora, reconstruct old archives, or
prove human annotation. `VALIDATION.json` records the metadata check performed
when preparing this package.

## Q1: source-code census and recorded row audit

`q1/` contains all nine frozen machine-readable records, copied byte for byte:

- 2,781 repository rows and 285,134 recognized logging calls;
- 24,108 broader candidate rows, including all source snippets and public
  archive-member locations; their strict subset is 15,659 calls in 1,520
  repositories (5.4918038536% of calls and 54.6565983459% of repositories);
- 499 unique sampled calls and 499 explicit row decisions, plus reviewed rows
  and per-stratum/aggregate audit statistics.

The denominator includes the production-like repository corpus, including
repositories with no recognized logging call. Counts are observations under the
frozen AST rules, not a population estimate of all software. Categories overlap:
their counts must not be summed to estimate the strict or broader union.

The recorded audit has 449 candidate rows and 50 ordinary-call probes. Recorded
labels agree with all sampled detector labels; per-stratum Wilson 95% lower
bounds range from approximately 92.9% to 95.3%. The records add four labels missed
by the detector, one in the 50 ordinary-call probes. These are label-agreement
statistics conditional on the correctness of the recorded decisions.

**Human reviewer attribution is not established.** `REVIEW_PROVENANCE.json`
explains the boundary. The original files use field names such as
`manual_labels`, and their protocol prose refers to independent inspection.
Neither those strings nor the requirement to supply explicit decisions proves
human authorship or independence. The preserved records contain no reviewer
identity or independently verifiable attribution. This package therefore does
not label these decisions as confirmed human annotation. Agreement on a finite
sample also does not guarantee zero census-wide false positives or a mathematical
lower bound on true prevalence. Original prose and schema are retained for
provenance rather than silently rewritten as stronger evidence.

`code/analyze_logbench_source_census.py`, `code/finalize_source_audit.py`, and
`code/test_source_census_rules.py` are exact historical source bytes. The frozen
runtime dependencies are in `code/requirements_source_census.txt`. The finalizer
requires the sample hash and complete explicit decisions; it does not generate
the decisions. To reproduce a census, obtain the LogBench-O full-size source
archive associated with *Exploring the Effectiveness of LLMs in Automated Logging
Generation: An Empirical Study* (https://arxiv.org/abs/2307.05950). Its required
SHA-256 is:

```
5ce32ed761b451e6dd9566a892a10a6f7c261740f1ef61a45adbdce0e06ecf92
```

The official README now provides a documented full-size Drive entry; see
[SOURCE_ACCESS.md](SOURCE_ACCESS.md) for the exact public links and verification
boundary. The current remote ZIP was not downloaded or hashed. The historical
local SHA above does not certify that the current remote bytes match it. No
full-census rerun was performed while packaging. Use a new output directory;
the original finalizer writes its output files.

```sh
python3 code/analyze_logbench_source_census.py \
  --archive inputs/logbench_fullsize.zip --output-root replay/q1 \
  --audit-per-category 50 --seed 20260713
python3 code/finalize_source_audit.py \
  --result-root replay/q1 --decisions q1/audit_decisions.json
```

## Q2: paired source-to-output representation study

`q2/family_catalog.json` has 23 public source-grounded families with public
repository URLs, exact source commits, paths, line ranges, matching patterns,
codec identities, source expressions, and rendering semantics. `families.csv`
contains all 23 measured rows. `summary.json` and `summary.csv` contain all four
complete historical system rows, retaining unrounded counts, reconstruction
flags, and all gzip/XZ diagnostic sizes and ratios, including small gains.
Only private input locations and the execution label were replaced with neutral
relative names. `run_manifest.json` preserves a whitelist of the recorded
environment and immutable inputs.

The two representation streams use exactly the same matched spans and
placeholders. Metric boundaries are explicit:

- Coverage is `surface_bytes / raw_bytes`; `surface_bytes` excludes framing.
- The reported 3.67–14.03 ratio is
  `raw_span_stream_bytes / latent_stream_bytes`. The numerator includes a
  variable-length length prefix for each rendered field. The denominator
  includes the deterministic numeric encodings and rendering state.
- For Spark, those stream bytes are 530,143,198 / 37,786,126 = 14.0301018951.
  Using only its 497,902,648 unframed surface characters would give
  13.1768641220, which is a different metric retained as `surface_to_latent`.
- The stream metric excludes residual text, catalog/program definitions and
  archive headers. It is not a complete-archive compression ratio. Complete
  gzip/XZ sizes are separate historical diagnostics.

**This was a whole-file historical control.** Some numeric latent codecs use
previous-value differences and varints over an entire family stream. There was
no R71 100,000-record state reset in this diagnostic. It measures source-grounded
representation plus deterministic coding before an outer compressor, not a
strict-block deployment gain or an isolated causal effect of an LLM. Rounded
renderings recover sufficient rendering state, not information discarded by the
application formatter.

`archive_hashes.json` preserves 12 recorded compressed-archive SHA identities
and three original result-file hashes. These identities were copied from the
historical ledger; the large archive payloads were not rehashed or decoded
while packaging. Original summary hashes differ from the sanitized copies;
`SOURCE_RECORDS.json` binds both identities explicitly.

The public log collection is LogHub (https://github.com/logpai/loghub). The exact
required historical raw bytes and SHA values are in `run_manifest.json`. The
historical Spark file has 2,941,228,156 bytes and differs from the R71 assembly;
R71 download/assembly instructions must not silently substitute its smaller
Spark file. This subpackage preserves the historical identity but does not
claim that the current downloader reproduces that earlier assembly. A separate
full-content control now verifies the exact historical recipe: lexical original
member bytes plus one additional LF after each member, including the last.
Both complete candidate hashes and all 3,852 historical boundaries match; this
is stronger evidence than the earlier length-only clue. The original historical
script was not found, and the control did not revalidate the current remote
download. Details, reusable verifier and both exact identities are in
[SOURCE_ACCESS.md](SOURCE_ACCESS.md) and [the control evidence](spark_assembly_control/README.md). The raw
corpora must be supplied separately and checked against these identities before
claiming reproduction. The renderer uses only Python's standard library:

```sh
python3 code/source_grounded_representation_study.py \
  --catalog q2/family_catalog.json \
  --input Spark=inputs/Q2/Spark.log \
  --input Zookeeper=inputs/Q2/Zookeeper.log \
  --input OpenStack=inputs/Q2/OpenStack.log \
  --input OpenSSH=inputs/Q2/OpenSSH.log \
  --output-root replay/q2 --xz-preset 6 --execution-label independent_replay
```

These commands document an opt-in rerun and were not executed while packaging.
The historical renderer replaces an existing dataset subdirectory under its
output root; use a fresh `replay/q2` directory.

## Anonymization and identity mapping

`SOURCE_RECORDS.json` maps 20 original source-record SHA identities to their
exported file identities. Public repository names, public code snippets,
archive-member paths, public GitHub commits/URLs and source line numbers are
intentionally preserved because they are the evidence. The exporter never reads
API configuration. Machine input paths and the private execution label in Q2
summaries are replaced; machine-local directory components are removed from the
archive hash ledger; run-manifest fields are whitelisted. No original study
file was modified. Unchanged code and records retain their original hashes;
sanitized records receive distinct exported hashes.

`export_source_study.py` accepts an explicit private study root to recreate this
export and refuses to overwrite a different existing evidence file. It is a
metadata exporter, not a study runner. This historical subpackage is frozen
independently of pending R71 external/formal results and is not a declaration
that the entire submission package is final.
