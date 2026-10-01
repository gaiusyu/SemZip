# SemZip: Synthesizing Reversible Programs for Log Compression — reproduction repository

This anonymous repository accompanies the paper *SemZip: Synthesizing Reversible Programs for Log
Compression*. It contains the frozen SemZip runtime, the sixteen deployed programs and storage
policies of the paper's main result, the recorded LLM syntheses and selection records behind them,
every complete-file result record used in the paper's tables, the adapters we wrote for the
baselines, and the scripts that regenerate the paper's tables from those records.

## What SemZip is

SemZip compresses logs in independent blocks of exactly 100,000 original LF-terminated records. Offline,
on block 0 of a log source only, an LLM (GPT-4o behind an OpenAI-compatible gateway) proposes small
programs that parse a field (a timestamp, an address, a size, ...) into a compact latent value and
render it back. A compiler/verifier accepts a program only if rendering reproduces every matched
span byte for byte. Five independent syntheses per source each pass a quality gate (compile variants and
backward elimination of rules), and a cost-guided selection starts from the best gated plan and adds,
replaces or removes rule groups taken from the other syntheses, accepting only moves that shrink the
complete, SHA-verified block-0 archive; a storage policy is then fitted on block 0. Online, the frozen
plan runs on every block with no model call: matched spans are
stored as latent streams, the rest goes to a DeLog-derived residual backend, and every block is
guarded by an actual archive inverse check with a fixed program-free recovery path. Decoding needs only
the archive.

## What is (and is not) in this repository

* **Included:** frozen runtime and guards (`source/`, `frozen/`, with the original Linux x86-64 native
  binaries), the 16 main deployments (`deployments/main_pool/`) and the 16 SemZip-1 deployments
  (`deployments/<Dataset>/`), all synthesis transcripts and selection records of the main result and of
  SemZip-1, all complete-file results, timing records, ablations, safety checks and the analysis generators.
  For the R76 repeats (`evidence/r76/variance/`) and the rule-library control (`evidence/r76/library/`) only
  the result records are included (see the track READMEs).
* **Not included:** raw logs (LogHub; fetched by `scripts/prepare_data.py` and verified by SHA-256),
  including the 16 SemZip-1 training samples, which are LogHub excerpts and are rebuilt from the fetched logs by
  `offline_evidence/tools/rebuild_samples.py`; third-party baseline sources (fetched at pinned commits by
  `external/fetch_baselines.sh`); compressed archives and work directories of the experiments; and any credential
  or private service configuration.
* LLM syntheses are **recorded, not regenerated**: model outputs are not byte-reproducible, so the
  repository ships the exact prompts, responses and resulting plans, and every downstream step
  (compilation, gate, selection, storage fitting, compression, decoding) is deterministic and replayable.

## Repository map

| Path | Contents |
|---|---|
| `README.md`, `LICENSE`, `THIRD_PARTY_NOTICES.md`, `MANIFEST_SHA256.txt` | this guide; license of this repository's code; third-party components and terms; SHA-256 of every file |
| `replay.py` | portable offline encode / archive-only decode / round-trip of a frozen deployment (no model calls) |
| `source/`, `frozen/` | frozen SemZip source snapshot (runtime, trainer, native backend + binaries) and the guarded runtime wrappers; hashes in `metadata/source_provenance.json` |
| `deployments/main_pool/<Dataset>/` | **main result**: `program.json` (selected synthesized plan), `storage.json` (block-0 storage policy), `publication.json` (publication record) |
| `deployments/<Dataset>/` | SemZip-1 (single greedy synthesis) program/policy pairs |
| `metadata/` | `deployments_main_pool.json`, `deployments.json` (SemZip-1), `raw_datasets.json` (LogHub URLs, member and file SHA-256), source provenance, training summaries |
| `scripts/` | `prepare_data.py` (fetch + verify LogHub), `compare_to_formal.py` (replay vs recorded archive), `audit_bundle.py`, `report.py` |
| `evidence/selection/` | main result: five syntheses per dataset with transcripts (`syntheses/`), quality gate (`selection/gate/`), pooled selection (`selection/pool/`), formal complete-file runs (`results/formal/pool`, `results/formal/qg1c0`), held-out evaluations, selection cost (`results/COST.json`), formal timing runs (`results/timing/`), and the drivers that produced them (`code/`) |
| `evidence/evolution/` | online evolution (per-block program-free fallback measurement, Thunderbird and Spark evolution runs, updates with their syntheses, pre-registered design `DEV_DESIGN_R75_zh.md`) |
| `evidence/r76/` | additional baselines and controls: `attribution/`, `baselines/{denum,logreducer,logshrink}/`, `codecs/`, `library/`, `rq2/` (matched replay), `variance/`, `safety/`, `timing/`, with pre-registered design `DEV_DESIGN_R76_zh.md`; provenance of every code file in `CODE_PROVENANCE.md` |
| `evidence/reference/` | complete-file reference identities (`r71_complete_file_reference.json`, see "Notes on this copy") and the published (literature-only) baseline numbers |
| `analysis/` | table/text generators (`r73/`, `r76/`), `stage_layout.py`, `run_all.sh`, recorded outputs (`reference_outputs/`) |
| `results/`, `integration/` | SemZip-1 and external-baseline complete-file results (`results/final/`), suffix (held-out) accounting (`results/suffix/`), their validator and renderers |
| `external/` | external-codec harness (`codec_baseline.py`), LogLite adapter + patch, pinned upstream identities (`manifest.json`, `SETUP.md`), `upstreams.conf` + `fetch_baselines.sh` |
| `offline_evidence/`, `validation_controls/`, `historical_*`, `source_study_evidence/` | SemZip-1 training transcripts and sample identities (samples rebuilt by `offline_evidence/tools/rebuild_samples.py`), replay/observer controls, historical development evidence, and the logging-source study |
| `docs/` | data assembly (`DATA.md`), result schema, and the detailed SemZip-1 replay contract (`R71_ARTIFACT_README.md`) |

## Requirements

* Linux x86-64 with Python 3.9+ and `libarchive`, `libpcre2-8`, XZ (Debian/Ubuntu:
  `python3 libarchive13 libpcre2-8-0 xz-utils`). The native binaries used by replay
  (`source/backend/Delog_plan_compress`, `source/backend/decompress`) are Linux x86-64 ELF. On another platform,
  rebuild them (`g++ libarchive-dev libpcre2-dev`; see *Rebuilding* below).
* `source/{runtime,trainer}/cpp_runtime/` also contains 14 macOS arm64 (Mach-O) executables. They are unused
  development builds of optional C++ helpers that are off by default (`SEMZIP_CPP_STREAM_WRITER=0`, no
  `--cpp-pcre2-replay`); keep them disabled for replay. They are part of the hash-checked frozen snapshot.
* Replay needs no Python package, network access, model service or credential.
* Regenerating tables needs only Python 3.9+; figures additionally need `matplotlib` and `numpy`
  (`integration/requirements.txt`) and are skipped with a message when matplotlib is missing.
* Disk: the inputs range from 2.3 MB (Linux) to 31.8 GB (Thunderbird); a round trip additionally needs
  free space for the restored copy (the input size) plus scratch. The paper's runs used four block workers.

## Quick checks without any data

```sh
python3 replay.py verify-deployments --deployment-set main      # frozen source + 16 main plans/policies
python3 replay.py verify-deployments --deployment-set semzip1   # 16 SemZip-1 plans/policies
python3 scripts/audit_bundle.py                                  # SemZip-1 base package inventory (MANIFEST.json)
bash analysis/run_all.sh ../semzip-analysis-stage                # regenerate every table/text from saved records
```

`run_all.sh` rebuilds the original record layout with symbolic links (`analysis/stage_layout.py`),
runs the R73 generators (selection, stability, evolution, external tables) and the R76 generators
(external ratios, attribution ladder, repeats, matched replay, library, safety, speed), and writes
`paper_out/` directories inside the stage. Compare them with `analysis/reference_outputs/{r73,r76}/`;
on the packaged records all 41 regenerated `.tex`/`.json` outputs are byte-identical to the recorded
ones (`skeleton_coverage.json` is seeded from the recorded output because it needs the raw logs; the
`paper_out/` directories additionally hold the figures). No experiment or model call is run.

## Fetch a LogHub dataset and verify it

`metadata/raw_datasets.json` lists, per dataset, the public archive URL (Zenodo record 8196385 /
LogHub), the archive SHA-256, the ordered member files with their SHA-256, and the SHA-256 and byte
count of the assembled raw file. `scripts/prepare_data.py` downloads (or takes) the archive, verifies
it, concatenates exactly the listed members without adding separators, and verifies the result:

```sh
python3 scripts/prepare_data.py --dataset Linux --archive downloads/Linux.tar.gz --raw-dir raw --download
python3 replay.py verify-input --dataset Linux --input raw/Linux.log
```

Inputs are bytes: LF ends a record; CRLF, a missing final LF and NUL bytes are preserved. Never
normalize or truncate an input; `replay.py` refuses any file whose size or SHA-256 differs.

## Compress and decompress with the frozen main plan

```sh
python3 replay.py roundtrip --deployment-set main --dataset Linux --input raw/Linux.log \
    --result runs/linux-main --workers 4
python3 scripts/compare_to_formal.py runs/linux-main          # archive vs the recorded formal run
```

`roundtrip` checks the frozen source/guard hashes and the plan/policy hashes
(`metadata/deployments_main_pool.json`), encodes the complete file in 100,000-record blocks with
`frozen/guarded_backend_v2.py` (the same guarded runtime and frozen x86-64 binaries used for the paper's
formal runs; the recorded `compressor_sha256` equals `source/backend/Delog_plan_compress`), then starts
a **separate archive-only decoder process** with the storage policy deliberately made unavailable, and
requires the materialized output to have the original size and SHA-256. `runs/linux-main/result.json`
lists every archive member with its size and SHA-256. `compare_to_formal.py` compares them with the
formal record `evidence/selection/results/formal/pool/<Dataset>.json` (for Linux: 2,349,686 raw bytes →
58,613 archive bytes, 40.09×; all sixteen expected sizes are in `metadata/deployments_main_pool.json`).

Decode an existing archive without the input or the plan:

```sh
python3 replay.py decode --archive runs/linux-main/encode/archive --output recovered/Linux.log \
    --result runs/linux-main-decode --workers 4
```

The same commands with `--deployment-set semzip1` replay SemZip-1 (single greedy synthesis); its reference
(`results/final/anonymous_results.json`) records raw identity and total archive bytes but no per-member list, so
`compare_to_formal.py` compares those two and reports the members as not recorded.
`replay.py` reports diagnostic process wall time only; the paper's throughput comes from the separately
scheduled serialized timing sessions (below). For Thunderbird and Windows a round trip takes hours.

**Rebuilding on another platform.** Copy `source/` to a new directory, run `bash build.sh` in its
`backend/`, and pass `--source-root <copy> --allow-rebuilt-binaries`. All non-binary source must still
match; rebuilt binaries are reported with their new hashes. Correctness (SHA) must hold; archive bytes
are expected but not guaranteed to equal the recorded ones with a different toolchain or `liblzma`.

**Replaying the formal driver itself.** `evidence/selection/code/formal_full.py` (via `lib.py`) is the
driver that produced the recorded formal runs; it calls the same `frozen/guarded_backend_v2.py` in
process and adds the per-block audit and suffix accounting. It expects the original layout
(`QG_ART`=a directory containing `source/` and `frozen/`, `raw/<Dataset>.log`, and the reference file
`evidence/reference/r71_complete_file_reference.json` in place of its `REF` path).

## How the evidence maps to the paper

| Paper element | Generator | Records |
|---|---|---|
| Complete-file ratios vs. baselines (external ratio table) and suffix ratios | `analysis/r76/r76_tables.py` → `external_ratio_table.tex`, `rq1_text.tex`, `supp_external.tex` | SemZip: `evidence/selection/results/formal/pool/`; DeLog, LogLite-BL, gzip6, XZ6, Zstd3: `results/final/anonymous_results.json`, `results/suffix/`; XZ9e, Zstd19, Zstd22-long, Zstd19+dict, DeLog-generic: `evidence/r76/codecs/full_20260929/`; LogShrink(+R): `evidence/r76/baselines/logshrink/full_official/`; LogReducer(+R): `evidence/r76/baselines/logreducer/full/`; Denum (lossiness proofs): `evidence/r76/baselines/denum/`; LogFold/LogPrism (literature only): `evidence/reference/published_baseline_reference_20260915.json` |
| Attribution ladder (empty program → SemZip-1 → gated → selection; library; DeLog-generic) | `analysis/r76/ladder.py`, `r76_tables.py` → `attribution_table.tex`, `rq2_text.tex` | empty program: `evidence/r76/attribution/`; SemZip-1: `results/final/`; gated c0: `evidence/selection/results/formal/qg1c0/`; library: `evidence/r76/library/` |
| Matched replay (latent vs. literal storage of the same matched spans) | `r76_tables.py` | `evidence/r76/rq2/` (`RESULTS.json`, per-dataset `DESIGN.json`, harness in `code/`) |
| Repeats of the complete pipeline (HPC, HDFS, Hadoop, Spark) | `r76_tables.py` → `repeats_table.tex`, `rq3_repeats_text.tex` | run 1: `evidence/selection/`; runs 2–3: `evidence/r76/variance/runs/<Dataset>/r{2,3}/` |
| Synthesis variability, quality gate, selection, block-0 → file transfer | `analysis/r73/stability_tex.py`, `supplement_tex.py` → `stability_*.tex`, `transfer_text.tex`, `supp_selection.tex` | `evidence/selection/syntheses/`, `selection/gate/`, `selection/pool/`, `results/heldout/`, `results/SUMMARY.json` |
| Selection cost (model calls, synthesis time, block-0 evaluations) | `stability_tex.py` → `selection_cost_*.tex` | `evidence/selection/results/COST.json` |
| Speed (serialized formal sessions, byte-weighted throughput) | `analysis/r73/paper_tables.py` (session A), `r76_tables.py` → `speed_table_r76.tex`, `cost_text.tex` | session A: `evidence/selection/results/timing/` and `supplementary/timing_SUMMARY.json`; new baselines / large-file samples: `evidence/r76/timing/` |
| Online evolution and per-block program-free fallback | `analysis/r73/evolution_tex.py` → `evolution_rq4_text.tex`, `evolution_supp.tex` | `evidence/evolution/` |
| Decode-time safety boundary of generated code | `r76_tables.py` (supplement) | `evidence/r76/safety/` (static AST allow-list validator V1/V2, archive scan, enforcing decoder, negative controls, full verification runs) |
| Logging-source study (motivation) | `source_study_evidence/verify_source_study.py` | `source_study_evidence/q1`, `q2` |
| SemZip-1 training transcripts and replay controls | — | `offline_evidence/`, `validation_controls/` |

All results are complete-file runs decoded from the archive alone in a separate process, with per-block
and full-file SHA-256 equal to the original; failures are retained in the records, not dropped.

## Baselines

We do not redistribute baseline source code. Set each URL in `external/upstreams.conf` to the repository of the
corresponding reference in the paper; `external/fetch_baselines.sh` then clones each upstream repository at the
commit used in the paper into `external/upstream/`:

| Baseline | Upstream (variable in `external/upstreams.conf`) | Commit | Our code in this repository |
|---|---|---|---|
| DeLog | `DELOG_REPO_URL` | `64a074f6b655` | `external/codec_baseline.py` (independent-block driver), `external/SETUP.md`; DeLog-generic: same source with an empty `regex_map` (`evidence/r76/codecs/README.md`) |
| LogLite (LogLite-BL) | `LOGLITE_REPO_URL` | `68f851ef673a` | `external/loglite_adapter.py`, `external/loglite_wide_reserve.patch` |
| LogShrink (+R) | `LOGSHRINK_REPO_URL` | `59ce49434eec` | `evidence/r76/baselines/logshrink/adapter/` (block driver, segmenter, decoder fixes generator) |
| LogReducer (+R) | `LOGREDUCER_REPO_URL` | `40005419022c` | `evidence/r76/baselines/logreducer/lr_run2.py` |
| Denum | `DENUM_REPO_URL` | `a3a697564e37` | `evidence/r76/baselines/denum/codec_baseline_denum*.py` |
| gzip / XZ / Zstd | system `gzip`, `xz` 5.2.5; zstd 1.5.6 release tarball (SHA-256 checked by the script) | — | `external/codec_baseline.py`, `evidence/r76/codecs/codec_baseline_r76.py` (+ `r68_to_r76_harness.diff`) |

Each track README under `evidence/r76/` gives the exact build commands, compiler versions, executable
hashes, flags, block protocol and every deviation (for example the counted per-line correction of the
"+R" variants, needed because the released LogShrink and LogReducer do not restore their input byte
for byte). All baselines use the same contract as SemZip: independent 100,000-record blocks, all
decoder-required bytes counted, archive-only decoding, SHA-256 audit.

## Re-running the offline synthesis (optional)

The trainer is `source/trainer/`; the synthesis driver is `evidence/selection/code/train_k.py`
(per dataset: the greedy T=0 synthesis plus four at T=0.7; at most 40 model calls each), followed by `qg_select2.py` (gate),
`qg_pool2.py` (selection), `publish.py` (storage fit) and `formal_full.py`. It needs an
OpenAI-compatible chat-completions endpoint serving GPT-4o; replace `<API_CONFIG_FILE>` in
`train_k.py` with a JSON file holding `base_url` and `api_key` (`train_k.py` exports them as `PARE_LLM_API_BASE`
and `PARE_LLM_API_KEY`). If you call the frozen trainer directly, always set `PARE_LLM_API_BASE` (or `--api-base`):
its built-in default endpoint is the third-party OpenAI-compatible service used for the SemZip-1 runs, and the
frozen source cannot be edited without breaking its recorded hashes. New syntheses will differ from the
recorded ones (model sampling and service versions are not pinned), which is why the paper's numbers
are tied to the recorded plans in `deployments/main_pool/` and transcripts in `evidence/selection/syntheses/`.

## Limitations

* LLM syntheses are recorded, not regenerated byte-identically; a model alias does not pin a served
  implementation. All post-synthesis steps are deterministic and replayable from the records.
* Raw logs are not redistributed; they are fetched from the public LogHub release and verified.
* The native binaries are Linux x86-64 builds. Other platforms need a rebuild (see above); archive
  bytes may then differ while the SHA-256 round trip must still hold.
* Timing in `replay.py` is diagnostic. The paper's throughput comes from serialized sessions on a shared
  host (`evidence/selection/results/timing/`); expect different absolute numbers on other machines. The formal
  R76 timing sessions (T2: new baselines on 12 files; T3: 20 sampled blocks of the four large files) had not
  finished when this repository was assembled: `evidence/r76/timing/` holds only smoke runs, `r76_tables.py`
  prints `--` for every Session B and large-file speed cell, and this copy backs no speed number of XZ9e,
  Zstd19, LogShrink+R or LogReducer+R.
* The LogReducer(+R) records (`evidence/r76/baselines/logreducer/full/`) are a snapshot taken during a resumed
  rerun; Zookeeper's valid PASS `result.json` is not used by the tables because its `status.json` reads
  `RUNNING`, and Thunderbird has no result yet (details in that track's README).
* Generated code is executed in-process at decode time; the frozen decoder trusts its archives. The
  safety track documents this boundary and provides a validating decoder (`evidence/r76/safety/`).
* Two R76 formal `result.json` files are stored in compacted form to keep the repository small (see "Notes on
  this copy").
* Code of the R76 tracks is checked against hashes recorded by the runs where such hashes exist; the rest is
  marked as mirror copies, and a few auxiliary scripts are not included (`evidence/r76/CODE_PROVENANCE.md`).
* Correctness evidence is finite validation of these inputs and programs, not a proof for arbitrary
  generated code or inputs.

## Notes on this copy

* Machine-specific absolute paths and host identifiers in records and code were replaced by placeholders in angle
  brackets (for example `<WORKDIR>`). A file edited this way is not byte-identical to the file as run; where a run recorded the hash of such a code file, the value is not
  listed (`evidence/r76/CODE_PROVENANCE.md`). All other recorded hashes are unchanged.
* The storage policies' free-text `proposal_source` field was edited in this copy. The runtime does not read it, so
  archives are unaffected. `storage_sha256` in `metadata/deployments.json` and `metadata/deployments_main_pool.json`
  is the hash of the published file; `storage_sha256_as_run` (main deployments) and
  `metadata/storage_policy_hashes.json` (every edited policy) give the hash recorded by the runs.
* Gated-c0 rung (`evidence/selection/results/formal/qg1c0/`): the plans are
  `evidence/selection/selection/gate/c0/<Dataset>/selected_plan.json`; the storage policies of that run are included
  for 9 of the 14 datasets (equal to the policies in `evidence/selection/deployments_selected/`), but not for
  Hadoop, Linux, Mac, Spark and Windows, whose policies were refitted on block 0 by `publish.py` and stayed on the
  execution host (`supplementary/publish_qg1c0/<Dataset>/` holds only `publication.json`).
* The SemZip-1 training samples (`offline_evidence/<Dataset>/sample.log`, 1,192 LogHub lines in total) are not
  shipped; `offline_evidence/tools/rebuild_samples.py --raw-dir raw` rebuilds them with the frozen sample writer
  and checks them against the recorded SHA-256. The same lines appear inside the recorded prompts.
* Two R76 formal records, `evidence/r76/attribution/runs/formal/empty/Thunderbird/result.json` and
  `evidence/r76/library/runs/formal/lib/Thunderbird/result.json` (5.8 MB and 6.9 MB), are compacted: every scalar
  field, the block-0 archive members and the manifest entry are kept, and per-block lists/maps with more than 50
  entries are replaced by their count and the SHA-256 of their canonical JSON (`_compaction` in each file gives
  the original size and SHA-256). The analysis generators read only retained fields. The 11 MB aggregate
  `evidence/r76/codecs/full_20260929/results.json` is omitted; the per-trial records it aggregates are included.
* `evidence/r76/attribution/plans/` was regenerated for this copy and is byte-identical to the plans that were
  run (hash check in `evidence/r76/attribution/README.md`).
* `evidence/reference/r71_complete_file_reference.json` is an earlier (R71) reference used by `formal_full.py`
  only for the raw identity, byte count and block count of each input. Its plan hashes and archive sizes refer to
  superseded development plans that are not shipped and are not the SemZip-1 or main-result deployments.
* `source/.gitignore` belongs to the frozen source snapshot and lists the two backend binaries; they are
  nevertheless tracked in this repository.

## Data and third-party terms

This repository's own code is under `LICENSE`. LogHub data keep their original license and citation terms.
`THIRD_PARTY_NOTICES.md` lists the third-party components: the DeLog-derived residual backend in
`source/backend/` (`compressor.cpp` modified, `decompressor.cpp` unchanged, and the two binaries built from them),
the MIT-licensed header-only libraries `json.hpp` and `BS_thread_pool.hpp`, and the LogLite patch. Baselines are
fetched from their upstream repositories under their own licenses.
