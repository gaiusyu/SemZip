# R76 track B2 — no-LLM rule-library control (LIBRARY-V1)

> **This copy.** Only the formal result records (`runs/formal/lib/<D>/result.json`; Thunderbird compacted, see the main
> README), the gate evaluation records (`runs/qg/lib/<D>/eval_*/`), the training summaries (`train_lib/`) and
> `library_report.json` are included. The published library plans and policies (`runs/publish/lib/<D>/`), the gate
> reports and selected plans, `FROZEN_SHA256.txt`, `scratch_trace/`, `runs/status/`, `full_chain.sh` and the logs stayed on
> the execution host, so the library row of the attribution ladder cannot be replayed from this copy.

Pre-registered design: `../DEV_DESIGN_R76_zh.md`, section B2 (written 2026-09-29 01:30 UTC, before any R76 result).
Question: if the LLM in the SemZip trainer is replaced by a fixed, hand-written, dataset-agnostic rule
library, and *everything downstream is unchanged* (compile/verify, QG-V1 quality gate, storage fitting,
publication, complete-file encode, archive-only decode), how many bytes does SemZip reach?

## What was done

1. **Library proposer** `proposer.py` (LIBRARY-V1). Exactly the frozen types of B2, nothing else:

   | library type | class (LLM schema) | regex (boundary-guarded, no literal spaces except `( +)`) | stored value / layout |
   |---|---|---|---|
   | `ts_syslog` `Mon DD HH:MM:SS` | class_1 | month name, `( +)`, `\d{1,2}`, `( +)`, `HH:MM:SS` | `(month*32+day)*86400+sec_of_day`; layout [space1, space2, day width] |
   | `ts_ymd_hms` `YYYY-MM-DD HH:MM:SS[,mmm/.mmm]` | class_1 | date, `( +)`, time, optional `[,.]\d{3}` | `(days_since_1970*86400+sod)*1000+ms`; layout [space run, sep code] |
   | `ts_iso8601` `YYYY-MM-DDTHH:MM:SS[.f{1,6}]` | class_1 | | `(days*86400+sod)*10^6+us`; layout [fraction width] |
   | `ts_yy_slash` `YY/MM/DD HH:MM:SS` | class_1 | | `days(20YY)*86400+sod`; layout [space run] |
   | `ts_compact` `YYYYMMDD HHMMSS` | class_1 | | `days*86400+sod`; layout [space run] |
   | `ts_hms` `HH:MM:SS[.f{1,6}]` | class_1 | | `sod*10^6+us`; layout [fraction width] |
   | `ts_epoch10` 10-digit Unix seconds | class_2 | `[1-9]\d{9}` | int |
   | `ipv4_port` IPv4:port | class_2 | `IP:(\d{1,5})`, replacement `{group1}:{placeholder}` | port int (no leading zeros, <=65535); context projector returns the IP (the schema's split_context_value form) |
   | `size_unit` B/KB/MB/GB with decimals | class_2 | `\d{1,12}(.\d{1,6})?( +)?(B|KB|MB|GB)` | digits as int; layout [int width, frac width, spaces, unit] |
   | `percentage` | class_2 | `\d{1,12}(.\d{1,6})?%` | digits as int; layout [int width, frac width] |
   | `hex_prefixed` `0x..` | class_2 | `0[xX][0-9a-fA-F]{1,15}` | int; layout [prefix, width, case] |
   | `hex_bare` | class_2 | 4-15 hex digits containing >=1 digit and >=1 letter, single case | int; layout [width, case] |
   | `fixed_point` | class_2 | `-?\d{1,12}\.\d{1,6}` | signed int of all digits; layout [sign, int width, frac width] (leading zeros kept) |
   | `decimal_int` | class_2 | `-?\d{1,18}` | signed int; layout [sign, width] (leading zeros kept) |
   | `ipv4` | class_3 | dotted quad | exact raw string (identity) |

   Every program is `python_exec` in the verifier subset (no imports, no comprehensions, no keyword
   arguments); every `forward` re-renders its own output and returns an invalid record (the trainer and the
   runtime then reject that match, which stays in the residual) unless the rendering is byte-identical. Timestamps
   use pure integer civil-date arithmetic (no locale/timezone dependence).
   *Emission rule (fixed):* for each sampled group the types are tried on the group's prompt examples in the
   order the compiler executes them (class 1, class 2, class 3, each in table order), masking earlier
   matches like the compiler's placeholder replay; a type is emitted iff it matches at least one example; at most 8
   (the trainer's `max_functions_per_family`) — lowest-priority types dropped first (never triggered in the traces so far).
   `meaning` strings are fixed per type (they feed the unchanged semantic-key heuristics: timestamps -> `raw_time`,
   ipv4 -> `ipv4:auto`, port -> `port:auto`, numbers -> none).
   The design was written from the prompt/schema text and the compiler/validator source only; no LLM-generated
   program of any dataset was opened.
   Unit test: `test_proposer.py` runs every type through the unchanged trainer validators
   (`is_open_llm_regex_safe`, `is_open_function_program_safe`, `python_exec_group_regex_variant`,
   `render_open_function_exact`, `python_exec_is_semantic_three_class`) plus a 20k-case timestamp fuzz (invalid dates
   are rejected, never mis-rendered).

2. **Plug-in** — only the LLM call is replaced.
   * `train_lib.py` = copy of `r73_qg/train_k.py` (same R69 trainer source, sampler, prompts, budget, config
     `PureConfig(workers=1, offline_prefix_ratio=1.0, offline_min_prefix_lines=100000, offline_api_workers=1,
     offline_max_llm_calls=40)`); the compressor subprocess is `semzip_library_wrapper.py` instead of `semzip_pure.py`
     with the identical command line (+ `--api-retries 0 --model no-llm-library-v1 --temperature 0`).
     The credential file is never read; provider env vars are removed.
   * `semzip_library_wrapper.py` imports the unchanged R69 `semzip_pure.py` and patches in memory only
     `call_llm` (-> proposer) and wraps `build_family_batch_prompt` / `build_prompt` (output unchanged) to remember
     which family ids/examples a prompt carries, so the proposer sees exactly the examples the LLM would see.
     Verifier-repair prompts get `{"functions": []}` (the library cannot repair). `urllib.request.urlopen` raises.
     Each proposal is written where the LLM response would be cached (`llm_caches/<D>/shared/*.json` + `.prompt.txt`).
   * `training.json` records `api_calls: 0`, `library_stats` (proposal counts), `api_record_dirs: 0`, and the SHA-256 of
     proposer/wrapper/trainer; the run is marked FAIL if any API attempt were recorded.

3. **Unchanged downstream** (verbatim copies, SHA-256 identical to `r73_qg/`): `lib.py`, `qg_select.py`,
   `qg_select2.py` (QG-V1 gate), `publish.py`, `fit_storage_v2.py`. `QG_ART` = `r73_qg/art` (frozen guard/runtime,
   read-only). `formal_full.py` is the R73 file with one change: worker count read from `R76_FORMAL_WORKERS`
   (default 4, runs use 3); archive bytes do not depend on it.
   **Pooling:** there is one candidate per dataset, so QG-POOL-V2 reduces to the gated plan; exactly like R73
   version `qg1c0`, `publish.py` is applied to `runs/qg/lib/<D>/selected_plan.json` and the pool step is not run.

4. **Driver** `run_library.py STAGES DS,...` (idempotent; failures recorded in `runs/status/<D>.json`, never
   retried automatically; per-dataset O_EXCL lock `runs/locks/<D>.lock` - a lane that includes `gate` waits for a
   locked dataset, a `publish,formal`-only lane skips it, so a second formal lane can be started safely with
   `R76_FORMAL_WORKERS=3 ../launch.sh logs/lane2.log python3 -u run_library.py publish,formal Spark,Windows,Thunderbird`): train -> `qg_select2.py D plan train/<D>.block0.log runs/qg/lib/<D> 3` ->
   `publish.py lib D ...` -> `formal_full.py lib D runs/publish/lib/<D>/program.json <storage>`
   (complete file, 100,000-line independent blocks, separate archive-only decode process with the external policy
   removed, per-block and full-file SHA-256, R71 reference check). `raw/` and `train/` are symlinks to `r73_qg/raw`,
   `r73_qg/train` (same inputs as the R73 main result).

Frozen hashes: `FROZEN_SHA256.txt` (code files; the README line there is informational only).
`scratch_trace/<D>/` holds a pre-gate compile-trace check of the frozen proposer on all 16 training samples
(train_lib.py only, no gate/compression; used to look for systematically rejected library programs - none were rejected).
Per-dataset progress/status: `runs/status/<D>.json`; logs: `logs/`.

## Commands

```
cd <WORKDIR>/r76_additional_20260929/library
export R76_GATE_THREADS=3 R76_FORMAL_WORKERS=3
AGNICE=5 ../launch.sh logs/smoke.log python3 -u run_library.py train,gate,publish,formal Proxifier,Linux,Apache,HealthApp
AGNICE=5 ../launch.sh logs/full.log ./full_chain.sh   # waits for the smoke driver, then all 16 small->large, then report.py
python3 report.py            # per-dataset table -> library_report.json
python3 test_proposer.py <WORKDIR>/r69_fresh_main_20260923/source/trainer
```

## Deviations / notes

* `size_unit` regex: the first draft used `( *)`; the trainer's literal-space rule allows only `( +)`, so it was
  changed to `(?:( +))?` before any gate/compression result existed (only compile traces had been looked at).
  Proposer frozen afterwards (`FROZEN_SHA256.txt`).
* The frozen type list has `YYYYMMDD HHMMSS` but not `YYMMDD HHMMSS`; HDFS's `081109 203615` header is therefore
  not a library timestamp (its pieces fall to `decimal_int`). Reported as is.
* Downstream behaviour observed in compile traces: `ipv4_port` candidates are admitted by the compiler but do not
  appear in the final replay plan (the unchanged semantic-registry/global-replay step keeps the `ipv4:auto` global
  spec); not modified.
* Formal runs use 3 workers instead of 4 (R73); bytes are independent of workers.

## Observations from the smoke run (reported, nothing changed afterwards)

* The unchanged QG-V1 gate never considers the empty plan as a candidate (it only reports its cost), and Stage B
  keeps a tag when its removal does not strictly shrink the archive. Local library tags whose family key never
  occurs in block 0 have exactly zero effect (Proxifier: removing B, C, D or H leaves 128413/116089 bytes), so the
  gate can stop above the empty program. Proxifier: gated library 116,089 B on block 0 vs empty 96,437 B.
  This is the pre-registered pipeline and is reported as is; the empty-program bytes are reported next to it
  (B1 / R75), no min(library, empty) selection is applied.
* HealthApp: no library timestamp matches its `YYYYMMDD-HH:MM:SS:mmm` header; the candidate consists of 14
  family-local `decimal_int` tags which make block 0 3.4x larger than the empty program (741 KB vs 217 KB) and
  slow the gate (about 2-3 min per evaluation).

## Results

`library_report.json` (written by `report.py`). Smoke (complete files, archive-only decode, SHA-256 per block and
full file):

| dataset | candidate tags | tags after gate | library bytes | ratio | SHA | empty program (R75 cfb) | R73 pool |
|---|---|---|---|---|---|---|---|
| Proxifier | 7 | 5 | 116,089 | 21.90 | pass | 96,437 | 70,361 |
| Linux | 11 | 9 | 79,545 | 29.54 | pass | 92,357 | 58,613 |
| Apache | 9 | 8 | 101,238 | 50.73 | pass | 120,890 | 69,646 |
