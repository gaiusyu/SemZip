# Original offline training evidence

This package contains the identities of all 16 saved training samples (the samples themselves are rebuilt from the public logs, see below), 202 paired model requests and responses, 16 compiler traces with all 869 admission/rejection/repair records, and the original training design plus audited sampling configuration. The original synthesis and fit timing boundaries remain in the separate training/sampling ledgers; these exchanges do not introduce new timing measurements.

## Preserved and removed fields

- `DATASET/sample.log` (not redistributed): the original sampled bytes are verbatim LogHub excerpts, so this copy does not ship them. `tools/rebuild_samples.py --raw-dir raw` rebuilds each sample from the fetched LogHub file with the unchanged frozen sample writer (`write_logbatcher_sample` in `source/trainer/run_blocks_pure.py`) on block 0, after checking block 0 against its recorded SHA-256 (`metadata/sampling_replay_summary.json`), and writes it only if its size and SHA-256 equal the values recorded in `PROVENANCE.json`. The same sample lines also appear inside the recorded prompts. Existing independent checks certified each selected record's membership in original block zero.
- `DATASET/exchanges/NNN.request.json`: the exact model-visible request body: model alias, response format, system/user content, temperature and output limit. Sequential call numbers preserve original recorded order and replace timestamp-derived cache filenames.
- `DATASET/exchanges/NNN.response.json`: returned model string, choice index, finish status, assistant content and all observed usage counts, including nested reasoning-token counts. Every assistant content string is unchanged. Provider bookkeeping, transport IDs/timestamps, fingerprint/service fields and empty reasoning strings are removed. All 202 original reasoning-content fields were empty.
- `DATASET/compiler_trace.json`: every original trace row and value, including rejected and repaired candidates. Only reviewed compiler fields are allowed. JSON formatting is normalized; the original file bytes are not claimed identical.
- `configuration.json`: public synthesis/selection/fallback design and original source hashes, plus the independently audited sampling configuration. It excludes transport configuration and creation time.

Requests exclude headers, keys, endpoints, recorded times and cache paths. Responses exclude account/request identifiers and provider-specific metadata. No HTTP transcript, API cache wrapper or original raw request/response file is shipped. Therefore **the sanitized request/response files are not byte-identical raw API records**. Model aliases and returned model strings are preserved for transparency; they do not pin the provider's served implementation or hidden configuration.

`PROVENANCE.json` records every original file SHA and sanitized file SHA without exposing original private filenames. Per-message-content hashes additionally bind unmodified prompt and answer strings. The pairing, per-dataset request/repair counts, observed usage totals and finish reasons match the original training ledger. Requests and responses both total 202; observed usage totals 1,238,005 tokens. Samples contain 1,192 physical records. These are recorded counts, not estimated costs.

## What has been verified

The packaging check validates all pairings, counts, exact sample SHA, unchanged model-visible request bodies and response strings, and all trace rows. A targeted scan found no known private workspace/service or credential markers in the exported content. The scan is not a formal anonymity proof; public log text can contain the public corpus's original paths, IPs and hostnames.

This is an **evidence export**, not a verified replay of every saved response through the complete original synthesis/compiler pipeline. No model or generated program was executed during the evidence export. Fixed-program compression/decompression replay is separately covered by the artifact's portable replay checks. Do not conflate those scopes or claim fresh API calls reproduce an immutable service version.

`tools/export_training_evidence.py` reproduces this export from the selected private original records and explicit training/sampling summaries. Its original inputs are not part of the anonymous package. The tool requires a new output directory and never modifies inputs. `FILES.json` binds all delivered files in this subtree.

Run `python3 offline_evidence/tools/audit_evidence.py` from the artifact root to independently recompute the public pairing, sample (when rebuilt)/content hashes, all token/finish counts and compiler record counts. It executes no model or compiler and does not elevate the response-replay claim.

## Subsequent bounded replay controls

The evidence export above is unchanged. Separate controls in `validation_controls/fresh_response_boundary/` now retain all sixteen local UTC0/C outcomes: 14 exact plan matches and Linux/Mac differences. Two additional explicit Asia/Shanghai diagnostics reproduce those two original plan files. All 28 changed repair-feedback timestamps agree with that historical timezone offset. The original training launcher did not pin or capture its process TZ, and these explanatory controls do not repair that provenance gap.

The control uses exact saved response content at `call_llm`, executes the original trainer prefix on the saved sample, and stops immediately after the original plan writer. Sample auxiliary core files are written; no complete archive, new model call or storage fit occurs. It is not HTTP/provider replay or a uniform all-sixteen UTC0 success. The export's `full_response_to_program_replay_verified=false` remains appropriate for that stronger unqualified claim; the separate control results state the actual validated conditions.
