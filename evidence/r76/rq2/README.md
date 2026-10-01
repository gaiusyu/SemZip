# R76 track B3 (rq2): matched-span surface vs latent representation control on the R73 pool programs

> **This copy.** `plans/` (including `PLAN_IDENTITY.json`), the archives and the logs are not included (see
> `../CODE_PROVENANCE.md`); the plans are the main-result plans in `deployments/main_pool/`.

Pre-registered design: `../DEV_DESIGN_R76_zh.md`, section B, item **B3**:
the R70 matched-span surface/latent control, re-run on the pooled-selection (R73
`pool`) programs and extended to all 16 files; the 12 formally timed files as
complete files, and HDFS, Spark, Windows, Thunderbird on the 20 systematically
spaced blocks of section D, `round(i*(N-1)/19)`, i = 0..19.

No API calls. No source/runtime change. Size/SHA study only (no speed claim).

## What R70 did and why only 4 files

`r70_representation_control_20260923/` (read only). `DESIGN.md` fixed the cohort
in advance to **Linux, HPC, OpenSSH, Android**: the complete R60 program-control
cohort (including its unfavourable case), with the first R69 proposed plans
(SemZip-1 era). It was a cohort choice, not a technical limit. V2
(`representation_control_v2.py`, `VALIDATION_V2.md`, `results_v2_strict/`)
added strict cold-cache boundaries and logical sub-field mode capture.

## Method (identical to R70 V2)

For each original 100,000-record block, independently (no state across blocks):

1. Load the plan through the unchanged runtime; the production stage/structure
   filters and placeholders apply. Run the production replay **once**; a callback
   records the admitted literal of every match (it does not change the output).
2. Fork only the stream representation:
   * **latent**: the normal semantic program and normal codec path (= SemZip);
   * **surface**: for streams whose serializer invokes a program (`open_function`,
     `open_context_delta`, `open_context_dict`, op != `auto_codec`), store the exact
     original placeholder-replacement literal with the normal generic `auto`
     field-storage path (context streams keep their context keys, grouped literals).
     Generic `auto` streams are unchanged in both arms.
3. Both arms share the identical residual text, placeholder locations and ONE
   native residual archive (`Delog_plan_compress ... lzma normal <empty plan>
   --residual-allgroup-or20`), hard-linked into both arms.
4. Each arm independently fits storage on **block 0 only** (same fitter), then the
   policy is frozen (hash-checked) for every later block.
5. Semantic archive = canonical tar + Python LZMA `6|PRESET_EXTREME`; programs,
   layouts, dictionaries, stream metadata, tar/XZ headers all counted; plus the
   residual archives and the semantic manifest. Traces/fit ledgers are sidecars
   never read by the decoder.
6. Archive-only reconstruction per block and full SHA (see "Verification").

All of steps 1-6 are executed by the **byte-identical copy** of the R70 V2 harness
(`representation_control_v2.py`, sha256 `1334fd05...ae4208`, and
`runtime_cache_boundary.py`, sha256 `e2098cb5...adfb911`, both equal to the R70
originals). The new driver imports its functions; it does not re-implement them.

## Files

| File | Role |
|---|---|
| `representation_control_v2.py`, `runtime_cache_boundary.py` | unchanged R70 V2 copies |
| `prepare_plans.py` | copies `r73_qg/runs/publish/pool/<DS>/program.json` to `plans/<DS>/extraction.json` (+ the published storage policy as a reference); checks plan/storage SHA against `publication.json` and the R73 formal result; writes `plans/PLAN_IDENTITY.json`; files made read-only |
| `representation_control_r76.py` | per-dataset driver (one process per dataset) |
| `decode_verify.py` | independent archive-only decode in a fresh process |
| `run_all.py` | campaign driver (<= 3 dataset processes), summary |

Runtime/source: `r73_quality_gate_20260927/r73_qg/art/source` (the frozen R71 source
used by the R73 formal pool run). All files it shares with the R69 source used by
R70 are byte-identical (checked by sha256; only docs/unused scripts were removed).
Raw inputs: `data/loghub1/raw/<DS>.log` (symlinks resolved).

### Differences from the R70 V2 driver (driver only, not method)

* dataset/plan: any dataset; plan = R73 pool publication (hash-checked vs R73 formal);
* optional systematic sample: blocks are read by byte offset from the R68 block
  inventory `r68_external_20260923/first_pass/input_<DS>.json`; each sampled block
  must match that inventory's SHA and LF count **and** the R73 formal per-block SHA,
  and must start right after an LF. Block 0 (i = 0) is always the storage-fit block.
  For sampled files the semantic manifest additionally lists `block_indices`
  (counted in the bytes, a few dozen B);
* complete files: block count and full-file SHA must equal the R73 formal reference;
* diagnostics (never change a result): per-block SHA parity of the shared residual
  and of the latent semantic archive with the R73 formal pool archive, and whether
  the latent refitted storage policy equals the R73 published policy on the policy
  keys (`recipe`, `column_numeric`, `numeric_modes`, `subfield_ids`, ...).

### Verification

* in the encoder process (V2 unchanged): after cold cache clearing, each arm's
  written semantic archive is extracted and restored against the natively decoded
  shared residual; bytes must equal the original block; concatenated SHA must match;
* `decode_verify.py` (new, separate process, per arm): reads only the arm's archive
  directory (manifest, `semantic/block_*.semantic.tar.xz`, `chunk_<i>.tar.xz`) and
  the unchanged runtime: native `decompress` then the production
  `semantic_codec_frontend._decode_block`; per-block SHA vs `blocks.json` and the
  concatenation SHA vs `result.json`. No plan, policy or trace is read.
  Output `<DS>/<DS>/<arm>/independent_decode.json`.

A failure in any block makes the dataset FAIL (kept as a row); nothing is retried
with another plan.

## Commands (from this directory on the execution host)

```sh
python3 prepare_plans.py <WORKDIR>/r73_quality_gate_20260927/r73_qg plans
# smoke (complete files, 2 workers)
AGNICE=5 ../launch.sh logs/smoke.log python3 -u run_all.py --output smoke_20260929 --workers 2 \
  --complete Proxifier Linux Apache
# full pre-registered run (3 workers)
AGNICE=5 ../launch.sh logs/full_20260929.log python3 -u run_all.py --output full_20260929 --workers 3 \
  --complete Android Apache BGL Hadoop HealthApp HPC Linux Mac OpenSSH OpenStack Proxifier Zookeeper \
  --sampled HDFS Spark Windows Thunderbird \
  --order BGL Windows Thunderbird Android Spark HDFS OpenSSH HPC OpenStack Hadoop HealthApp Mac Apache Proxifier Linux Zookeeper
```

`--order` is scheduling only (longest first). Progress: `full_20260929/campaign.jsonl`
(one line per finished dataset), `full_20260929/logs/<DS>.log` (one line per block).
Final table: `full_20260929/summary.md` / `summary.json`; `status.json` when done.

## Output layout

`<OUT>/<DS>/DESIGN.json` (script, harness, boundary, source and plan hashes, block
indices), `<OUT>/<DS>/status.json`, `<OUT>/<DS>/<DS>/{result.json, blocks.json,
shared_residual/, latent/, surface/}`; each arm has `archive/` (the complete
decodable archive), `storage.json` (fitted policy, not needed to decode), `fit.json`,
`independent_decode.json`.

Metric: `full_archive_saving_pct = 100*(1 - latent/surface)` on complete archive
bytes (semantic + residual + manifest); `heldout_archive_saving_pct` = same on the
block archives from block 1 onward (for sampled files: the 19 non-zero sampled
blocks). `metadata_member_bytes`/`stream_member_bytes` are uncompressed tar members
and are not additive compressed components.

## Status (2026-09-29)

* Full pre-registered run `full_20260929/` **finished**: `status.json` = PASS, 16/16 datasets
  (launched 02:31, finished 04:16 UTC; BGL encode alone 6,027 s). No live process; nothing
  was relaunched or re-run.
* Smoke `smoke_20260929/` (Proxifier, Linux, Apache): PASS 3/3.
* `RESULTS.md` / `results_table.json` written after the run with
  `python3 results_table.py full_20260929 RESULTS.md` (read-only on the run directory).
* This directory is not a git repository; provenance is by sha256:

| File | sha256 |
|---|---|
| `representation_control_r76.py` (driver; equals `script_sha256` in every `<DS>/DESIGN.json`) | `957577228c55539a11c0c0e7946a82e17d40a0130f73a17b7de8947c3b046999` |
| `representation_control_v2.py` (R70 V2, unchanged) | `1334fd05a504e5ec3ff725c2aa163217daf02813bbe13890ff9f592156ae4208` |
| `runtime_cache_boundary.py` (R70, unchanged) | `e2098cb527c652aa0cd4464cffd0a16e7e7ca6ccff2c59474fc812069adfb911` |
| `decode_verify.py` | `f24266c4b4b2c17fe8c24fcba7feb4b0279bf03c4d712365e83197c184dbc1ce` |
| `run_all.py` | not listed (machine-specific path replaced in this copy) |
| `prepare_plans.py` | `59845bd1e6c8b60c305bad48e63048f542f58ab5c2bc1430a820c01695d84333` |
| `results_table.py` (post-run table only) | `9b3ea24bf842300f0637764a625770ed56ac85ddd903e50ed53982d95076a47a` |

Plan hashes per dataset: `plans/PLAN_IDENTITY.json` (not included in this copy; the plans are the main-result plans in `deployments/main_pool/`); runtime source file hashes: `<DS>/DESIGN.json`.

## Results

Full table (semantic/residual split, replaced streams, per-block wins): `RESULTS.md`.
Saving = `100*(1 - latent/surface)` on complete archive bytes. Ratio = raw bytes / archive bytes.
"SHA" = archive-only independent decode (`decode_verify.py`, fresh process) of both arms,
every block SHA and the concatenation SHA.

| Dataset | Scope | Blocks | Raw B | Surface B | Latent B | Saving % | Held-out % | Ratio surface | Ratio latent | SHA surface / latent |
|---|---|---|---|---|---|---|---|---|---|---|
| Android | complete | 16/16 | 192,270,829 | 5,602,902 | 5,565,790 | 0.66 | 0.68 | 34.32 | 34.55 | PASS / PASS |
| Apache | complete | 1/1 | 5,135,876 | 82,026 | 69,646 | 15.09 | n/a | 62.61 | 73.74 | PASS / PASS |
| BGL | complete | 48/48 | 743,185,031 | 14,680,374 | 14,203,694 | 3.25 | 3.28 | 50.62 | 52.32 | PASS / PASS |
| Hadoop | complete | 4/4 | 48,595,595 | 633,820 | 571,500 | 9.83 | 9.15 | 76.67 | 85.03 | PASS / PASS |
| HealthApp | complete | 3/3 | 23,529,930 | 433,917 | 395,273 | 8.91 | 8.57 | 54.23 | 59.53 | PASS / PASS |
| HPC | complete | 5/5 | 33,553,503 | 732,843 | 732,843 | 0.00* | 0.00* | 45.79 | 45.79 | PASS / PASS |
| Linux | complete | 1/1 | 2,349,686 | 63,725 | 58,613 | 8.02 | n/a | 36.87 | 40.09 | PASS / PASS |
| Mac | complete | 2/2 | 16,879,552 | 313,549 | 311,853 | 0.54 | 0.25 | 53.83 | 54.13 | PASS / PASS |
| OpenSSH | complete | 7/7 | 73,417,506 | 728,835 | 623,323 | 14.48 | 12.70 | 100.73 | 117.78 | PASS / PASS |
| OpenStack | complete | 3/3 | 61,442,082 | 2,481,449 | 2,434,257 | 1.90 | 2.02 | 24.76 | 25.24 | PASS / PASS |
| Proxifier | complete | 1/1 | 2,541,814 | 79,389 | 70,361 | 11.37 | n/a | 32.02 | 36.13 | PASS / PASS |
| Zookeeper | complete | 1/1 | 10,426,438 | 63,013 | 55,045 | 12.65 | n/a | 165.46 | 189.42 | PASS / PASS |
| HDFS | 20 sampled | 20/112 | 277,338,890 | 10,051,953 | 10,051,953 | 0.00* | 0.00* | 27.59 | 27.59 | PASS / PASS |
| Spark | 20 sampled | 20/333 | 170,382,248 | 2,887,699 | 2,876,067 | 0.40 | 0.13 | 59.00 | 59.24 | PASS / PASS |
| Windows | 20 sampled | 20/1147 | 465,577,918 | 767,784 | 773,208 | -0.71 | -0.78 | 606.39 | 602.14 | PASS / PASS |
| Thunderbird | 20 sampled | 20/2113 | 268,957,222 | 3,911,949 | 3,911,949 | 0.00* | 0.00* | 68.75 | 68.75 | PASS / PASS |

Totals: 12 complete files surface 25,895,842 B vs latent 25,092,198 B (3.10 % saving, byte-weighted,
dominated by BGL); 4 sampled files (20 blocks each) 17,619,385 B vs 17,613,177 B (0.04 %).
Raw bytes of sampled files are the bytes of the 20 sampled blocks, not of the whole file.

\* **Vacuous control, not "no benefit".** For HPC, HDFS and Thunderbird the R73 pool program
emits no stream whose serializer invokes a program (`open_function` / `open_context_*`); every
stream is a generic `auto` stream (`ipv4_plain`, `string_mtf_rank`, `affixed_int_delta`, ...).
The surface arm therefore replaces nothing and its semantic archive is byte-identical to the latent
one in every block (`surface_equals_latent_semantic_blocks` = all blocks). The comparison is only
informative for the other 13 files.

Other observations:

* Windows is the one file where latent is larger: 19 of 20 sampled blocks, +5,424 B in total
  (program streams `D`, `TS0`, `open_function`). Spark: small total gain (0.40 %), 0.13 % held-out,
  3/20 blocks identical, 14 latent-smaller, 3 latent-larger. BGL: latent smaller in 41/48 blocks,
  larger in 7. OpenSSH: 6 smaller / 1 larger.
* Largest relative gains are on the small files (Apache 15.1, OpenSSH 14.5, Zookeeper 12.7,
  Proxifier 11.4, Hadoop 9.8, HealthApp 8.9, Linux 8.0 %); absolute gain is largest on BGL (476,680 B).

### Additional read-only checks

* For all 12 complete files, sha256 of `data/loghub1/raw/<DS>.log` (symlink resolved) was
  recomputed and equals `decoded_concat_sha256` of both arms' `independent_decode.json`; the file
  size equals `raw_bytes`.
* For the 4 sampled files, `block_indices` equals `round(i*(N-1)/19)`, i = 0..19.
* The byte sum of each arm's `archive/` directory equals the reported surface/latent bytes (the
  hard-linked shared residual is counted in each arm).
* R73 parity diagnostics: in all 16 files the shared residual and the latent semantic archive are
  byte-identical to the R73 formal pool archive for every block, the latent refitted storage policy
  equals the R73 published policy on the policy keys, and for the 12 complete files the latent archive
  total equals the R73 formal archive bytes exactly. So the latent arm is the SemZip main result.
* All logs scanned: no traceback/error/warning lines.

## Deviations from the pre-registered design

* None in scope or method. The design's optional extra ("if time permits, also run the 4 large
  files completely") was not done; the 20-block samples are the pre-registered result.
* Relative to R70 V2 only the driver differs (listed above); the harness and cache-boundary modules
  are byte-identical copies.

## Open problems / caveats for the paper

* 3 of 16 files (HPC, HDFS, Thunderbird) give no information about representation (no program
  streams in the selected programs); report them as n/a rather than 0 %.
* Held-out saving is undefined for the four 1-block files (Apache, Proxifier, Linux, Zookeeper):
  there the storage fit and the evaluation share block 0.
* Size/SHA study only; encode/decode times in `campaign.jsonl` were measured under a shared, loaded
  machine with 3 parallel workers and must not be used as speed results.
