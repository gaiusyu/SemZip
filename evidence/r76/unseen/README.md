# R76-G: unseen log sources (four LBL ITA HTTP access logs)

> **This copy.** Included: the inputs' identities (`inputs.json`, `reference.json`), the summaries (`summary.json`,
> `summary_all.json`, `queue_status.json`), the drivers (`rep.py`, `prep_inputs.py`, `smoke_formal.py`,
> `optional_after.py`, `summarize_all.py`), the adapted `code/formal_full.py` with `code/formal_full_vs_variance.diff`
> (generated at packaging time) and `code/SHA256SUMS.r73_originals`, and per dataset in `runs/<D>/r1/`: `repeat.json`,
> the five syntheses (`train_k/<D>/<cand>/training.json`, `training_work/<D>/{replay_plan,summary}.json` and the
> model exchanges `llm_caches/<D>/shared/**/family_*.{prompt.txt,json}`), the gate and pool records
> (`runs/qg/c*/<D>/`, `runs/pool/<D>/`: reports, selected plans, evaluation caches, and per evaluation the block-0
> round-trip `result.json` and its decode summary), the published program and storage policy
> (`runs/publish/pool/<D>/`) and the formal complete-file record (`runs/formal/pool/<D>/result.json`). Baselines:
> every campaign and per-trial `result.json`/`status.json` of `baselines/main/`, `baselines/logreducer/`,
> `baselines/logshrink/` and `baselines/logreducer_probe/`, and the adapted runner
> `baselines/logshrink_adapter/ls_run_unseen.py`. Smoke: `smoke/<D>_empty/smoke_result.json` and its formal record.
>
> Not included: the logs themselves (`raw/`, `train/`, `r69root/`; fetch them from the URLs in `inputs.json` and check
> the SHA-256 recorded there), archives, per-block work files of the baselines (`blocks/`, `diagnostics/`), run logs,
> the per-candidate storage-fit work files (`tc/storage/`), the copies of each evaluated plan (`eval_*/plan.json`;
> `eval_cache.json` identifies every evaluated plan by its SHA-256 and rule tags), and the raw HTTP records of the model calls
> (`_api_records/`: raw request and response bodies plus the gateway's HTTP response headers; every prompt and the
> parsed model output are kept in the cache files). Byte-identical copies are not repeated: `baselines/harness/codec_baseline_r76.py`
> (= `../codecs/codec_baseline_r76.py` as run), `baselines/harness/loglite_adapter.py` (= `external/loglite_adapter.py`,
> SHA-256 `73fbc851b33ce3d15a47e6fadadcd47d91250eeb9e1adbdbdb563bcfc9a07b77`), `rep_variance_v3.py.orig`
> (= `../variance/rep.py` as run) and `code/*.py` other than `formal_full.py` (= `../variance/code/*.py`, see
> `../variance/README.md`). Times are given in UTC in this copy.

This track runs `../DEV_DESIGN_R76_zh.md`, section "R76-G：未参与开发的日志来源" (pre-registered before any result;
the section is stamped 2026-10-01 13:20 UTC, which the design's change log corrects to about 12:08 UTC, see section 7). Nothing outside `unseen/` is written. R68, R69, R71, R73, R75 and the other R76 tracks are only
read: their code, binaries and the R69 trainer source.

## 1. Inputs (`inputs.json`, `reference.json`, `raw/`, `train/`)

The selection rule is fixed by the design: every server log on the LBL Internet Traffic Archive with at least
700k lines, one file per server. The files are `https://ita.ee.lbl.gov/traces/<name>.gz`. They were downloaded on a
workstation and copied to the execution host byte for byte (`raw/<name>.gz`).

- `prep_inputs.py` checks each file with `gzip -t` and decompresses it unchanged to `raw/<D>.log`. There is no
  cleaning and no newline normalisation. It cross-checks the SHA-256 against `gzip -dc | sha256sum`.
- It writes block 0 (the first 100,000 LF records, by the same `readline` rule as `lib.first_block` and
  `formal_full.py`) to `train/<D>/train.log`.
- It records each file and block in `inputs.json`: SHA-256, byte counts, line counts and per-block SHA-256.
- `reference.json` is the same data in the row format of `r71_strict_main_20260923/reference.json`.

| D | source file | gz bytes | gz SHA-256 | raw bytes | raw SHA-256 | lines | blocks | block-0 SHA-256 |
|---|---|---:|---|---:|---|---:|---:|---|
| NASA | NASA_access_log_Jul95 | 20,676,672 | `199109ed…27488` | 205,242,368 | `96551161b5bdcaacbc3c17fa108191c478fb35dfe87895c16e34a8f6552bf29a` | 1,891,715 | 19 | `e090efe9…a8ec` |
| ClarkNet | clarknet_access_log_Aug28 | 21,756,573 | `bbd0e743…cb51` | 170,957,969 | `cfd095483ec3b8505f7090195c89f708a412111d1cd4eba3272212763759d516` | 1,654,882 | 17 | `54f555a4…c2b5` |
| USask | usask_access_log | 30,505,430 | `4210f0cf…c855` | 233,440,918 | `a68620b67a22faa41893737e23b8396bca1162029f26e0f7175bcd1547397f8d` | 2,408,625 | 25 | `c28f4c03…26a4` |
| Calgary | calgary_access_log | 5,435,681 | `273a2ad7…26d9` | 52,276,884 | `6c4e946b410ea8461209e60c562537157de6f277687d9ba0e0a57ea975f0b6bf` | 726,739 | 8 | `6cadad8a…cb9c` |

In total there are 6,681,961 lines and 69 independent 100k-line blocks. Full hashes are in `inputs.json`.

Byte facts, recorded and not altered:
- **NASA: the last record has no terminal LF** (1,891,714 LF bytes for 1,891,715 records).
- ClarkNet has 52 CR bytes.
- The non-ASCII byte counts are NASA 38, ClarkNet 8, USask 31 and Calgary 44.
- No file contains a NUL byte.

**Download note:** the workstation downloader used a 300 s curl timeout. It truncated `NASA_access_log_Jul95.gz` at
18,849,447 B and `usask_access_log.gz` at 22,388,391 B, and both failed `gunzip -t`. I completed both from the same
URL with an HTTP range resume (`curl -C -`; the server reports `Accept-Ranges: bytes` and Content-Length 20,676,672
and 30,505,430). A second, independent download made at the same time gave
identical SHA-256. Only files that pass `gunzip -t` and have the full Content-Length are used.

## 2. SemZip: the unchanged R73 main pipeline (`rep.py`, `code/`)

Per dataset there is one run, `runs/<D>/r1/`, on original block 0. The steps are those of `../variance/README.md`:

1. 5 fresh syntheses with `code/train_k.py`: R69 trainer, gpt-4o through the same OpenAI-compatible gateway, 40-call budget,
   0 retries. c0 uses T=0 and c1-c4 use T=0.7. A failed synthesis is retained and never retried.
2. QG-V1 (`qg_select2.py`, 1 thread per candidate).
3. QG-POOL-V2 (`qg_pool2.py`, `MAX_ROUNDS=4`, `MAX_EVALS=600`).
4. `publish.py`: storage refit on block 0 and the determinism check.
5. `formal_full.py`: complete-file encode with independent 100k-line blocks, then a separate archive-only decode
   process with the external policy made unavailable, with per-block and full-file SHA-256 on the materialised
   output and the suffix (block >= 1) accounting.

The prompts, sampler, budgets, temperatures, candidate order, gate and pool constants and the frozen guard/runtime
(`QG_ART=r73_qg/art`) are unchanged.

### Adaptations (only what datasets outside LogHub need)

1. **`code/formal_full.py`**: R71's `reference.json` has no rows for these files. The raw-identity reference is now
   read from `FORMAL_REF`, which the driver sets to `unseen/reference.json`. The default remains the r71 path. The
   assertions are the same: raw SHA-256, raw bytes and block count. The diff against `variance/code/formal_full.py`
   is one line (line 11). The variance deviation `FORMAL_WORKERS` is inherited, and this track uses 3.
   - `code/formal_full.py` as-run SHA-256: not listed (machine-specific path replaced in this copy); `code/formal_full_vs_variance.diff` shows the change.
   - All other `code/*.py` are byte-identical to `variance/code` (= r73_qg; see `code/SHA256SUMS.r73_originals`) and are
     not repeated in this copy.
2. **`rep.py`** is adapted from `variance/rep.py` (copy kept on the execution host as `rep_variance_v3.py.orig`,
   byte-identical to `../variance/rep.py` as run; not repeated here).
   Every change is marked `R76-G`:
   - `V = unseen/` and `DATASETS = (NASA, ClarkNet, USask, Calgary)`. Only repeat `r1` is accepted.
   - Training block: `train/<D>.block0.log` and `r69root/inputs/<D>/train.log` link to `train/<D>/train.log`. Its
     SHA-256 is asserted equal to `inputs.json` block 0, replacing the R68 DeLog block-0 hash. Raw input:
     `raw/<D>.log` links to `unseen/raw/<D>.log`.
   - The comparison with DeLog is moved from the formal step to `summary()`. R68 has no numbers for these files, so
     the summary uses this track's own baselines.
   - A synthesis cap is added: `UNSEEN_SYNTH_PARALLEL=3`. At most 3 syntheses run at once (the shared-host limit
     allows 2-3; the syntheses are API bound).
   - A baseline lane is added inside the same FIFO slot budget. It runs one harness process per dataset with
     1 worker and holds 1 slot. The SemZip resource costs are unchanged: synthesis 1, gate 1, pool 3, formal 3.
   - The total stays at 4 or fewer heavy child processes (`queue 4 ...`).
   - A new `summary()` gives the full ratio and the suffix ratio (blocks >= 1) against every baseline, plus the
     wins, losses and total byte difference against DeLog.
3. **R69 trainer input**: `r69root/source` links to R69 `source` (read-only, `PYTHONDONTWRITEBYTECODE=1`), and
   `r69root/inputs/<D>/train.log` is block 0, the same layout as variance/R75. The dataset name is passed as
   `--dataset <D>`, as with every LogHub file. It appears only as a label in the prompt text. The legacy
   name-keyed tables in the frozen source have no entries for these names, and nothing was modified:
   - `pare_dataset_extract` DELOG/TYPED profiles;
   - the `args.dataset == "Apache"` branch of `semzip_pure`.

## 3. Baselines (`baselines/`)

- **Harness:** `baselines/harness/codec_baseline_r76.py` is a byte-identical copy of `codecs/codec_baseline_r76.py`
  as run (as-run SHA-256 not listed (machine-specific path replaced in this copy)). It sits next to an identical
  copy of `r68_external_20260923/loglite_adapter.py` (`73fbc851…7b77`, = `external/loglite_adapter.py`). Neither copy is
  repeated here.
  - The protocol is the R68/R76 one: exact 100k-LF-record blocks, one fresh CLI process per block, all archive
    bytes counted, archive-only decode, and per-block plus full-file SHA-256.
  - The invocation uses `--input D=path`, because the file names are not `<D>.log` in a LogHub directory, with
    `--workers 1 --trials 1`.
  - Output goes to `baselines/main/<D>/`, one campaign per dataset.
- **Codecs:**
  - **DeLog (official)**: R68 binaries `artifacts/DeLog/Delog_compress` (`f4c62424…1739`, commit `64a074f6…`) and
    `decompress` (`b722179d…0831`), with the R68 adapter arguments
    `Delog_compress DATASET text 100000 1 0 lzma normal`.
    - Its `regex_map` is keyed by dataset name and has no entries for NASA, ClarkNet, USask or Calgary, so it runs
      with no hand-written rules. The encoder prints `Warning: No predefined patterns for logname '<D>'` on every
      block; this was verified on 8/8 Calgary blocks.
  - **LogLite-BL**: `artifacts/LogLite-B-wide/build/loglite-B-wide` (`2e207d69…64b4`, LogLite commit `68f851ef…`),
    the same binary and adapter as R68 `loglite_first_pass`. The final-LF marker byte is part of the counted archive.
  - **gzip6**: `/bin/gzip` 1.10 (`ba757f55…937e`).
  - **xz6 and xz9e**: `/usr/bin/xz` 5.2.5 (`927ef7f0…a1e9`).
  - **zstd3 and zstd19**: `codecs/tools/zstd-1.5.6/programs/zstd` (`77a325b2…85e2`).
- **Optional** (design: "时间允许时"): `optional_after.py` waits until the main queue has finished, so it never
  delays or shares CPU with the main results. It then runs two jobs concurrently, 2 + 2 workers. It gives up if the
  main queue is not done by 2026-10-02 22:00 UTC.
  - **LogReducer+R**: `baselines/logreducer/lr_run2.py`, unchanged (it has its own `--input`/`--output`), output
    `baselines/logreducer/`.
  - **LogShrink+R with generic L=5** (run.py's default; these names have no official `-L`):
    `baselines/logshrink_adapter/ls_run_unseen.py`, a copy of `baselines/logshrink/adapter/ls_run.py`. It uses that
    track's `code_r76` and venv read-only, and its output is `baselines/logshrink/`. The changes are marked
    `R76-G`; encoder, decoder, residual and audit code are unchanged:
    - helper-script dir pinned to the LogShrink adapter dir;
    - `RAW_DIR = unseen/raw`;
    - the inventory is `baselines/main/<D>/input_<D>.json`, from the same harness inventory function as R68;
    - `OFFICIAL_L.get(name)` in the label;
    - `--L-mode generic` only;
    - scratch goes to `baselines/logshrink_tmp`.
  - Both are adapted methods: their "+R" residual is counted. Native (unrepaired) ratios are reported as
    "not losslessly verified", the same labelling as in section A.

## 4. Smoke of the adapted formal path (0 API, before any synthesis)

`smoke_formal.py D 3` runs `publish.py` and then the adapted `formal_full.py` with the residual-only (empty) program.
This is the plan structure QG-V1 evaluates as `empty_residual_only`, taken from `variance/runs/HPC/r2` `eval_c0fa44e8d45f`
with only the dataset field renamed. It is not a SemZip result.

| D | status | archive B | ratio | blocks | archive-only decode SHA = inputs.json | per-block SHA | suffix ratio | fallback blocks | wall |
|---|---|---:|---:|---:|---|---|---:|---:|---:|
| Calgary | PASS | 2,726,621 | 19.173 | 8 | yes | 8/8 | 19.122 | 0 | 10.9 s |
| NASA (unterminated last record) | PASS | 10,330,033 | 19.869 | 19 | yes | 19/19 | 19.874 | 0 | 24.8 s |

The evidence is in `smoke/<D>_empty/smoke_result.json` and `smoke/<D>_empty/runs/formal/pool/<D>/result.json`.

## 5. Launch

```
cd unseen
python3 prep_inputs.py                                   # inputs.json, reference.json, raw/<D>.log, train/<D>/train.log
AGNICE=5 ../launch.sh logs/smoke_formal_Calgary.log python3 -u smoke_formal.py Calgary 3
AGNICE=5 ../launch.sh logs/smoke_formal_NASA.log python3 -u smoke_formal.py NASA 3
AGNICE=5 ../launch.sh logs/queue.log python3 -u rep.py queue 4 4 Calgary:r1,ClarkNet:r1,NASA:r1,USask:r1 Calgary,ClarkNet,NASA,USask
AGNICE=5 ../launch.sh logs/optional_after.log python3 -u optional_after.py
python3 rep.py summary                                   # summary.json (+ table); also written at queue end
```

- The main queue was launched at 2026-10-01 12:32:22 UTC, pid 490691, with autogroup nice 5.
- Progress files:
  - `logs/queue.log`;
  - `queue_status.json`;
  - `runs/<D>/r1/repeat.json`;
  - `logs/baselines_<D>.log`;
  - `baselines/main/<D>/status.json`.
- The optional waiter is pid 508169, logging to `logs/optional_after.log` and `baselines/optional_status.json`.

**Resume after a container restart.** Re-run the same `launch.sh` command. Completed steps are skipped and gates
resume from `eval_cache.json`.
- A synthesis that was in flight is kept as `INCOMPLETE_RETAINED` and is not retried (the R73 rule).
- The baseline lane moves a stale harness `RUNNING.lock` aside only when its pid is not a live
  `codec_baseline_r76.py`. It adds `--retry-failed` only when an attempt directory has no `result.json`, meaning it
  was interrupted. A genuine codec FAIL is kept and not retried.
- `optional_after.py` handles stale locks in the same way.

## 6. Results (all runs finished 2026-10-01 13:26 UTC)

`summary.json` comes from `rep.py summary`, and `summary_all.json` from `summarize_all.py`, which also includes the
optional baselines.

- Ratios are given as full file / suffix, where the suffix is original blocks 1 onward.
- SemZip suffix bytes = the verified block payloads plus the recomputed suffix manifest (R71 accounting). Baseline
  suffix bytes = their block archives 1 onward.
- Every entry with a number is lossless: it was decoded from its archive alone, and its per-block and full-file
  SHA-256 equal `inputs.json`.

| D | raw B | SemZip full / suffix | DeLog | SemZip - DeLog B | LogLite-BL | xz9e | zstd19 | xz6 | gzip6 | zstd3 | LogReducer+R | LogShrink+R (L=5) |
|---|---:|---|---|---:|---|---|---|---|---|---|---|---|
| NASA | 205,242,368 | 22.756 / 22.785 | 20.310 / 20.324 | -1,086,535 (-10.75 %) | 9.820 / 9.820 | 16.360 / 16.370 | 15.585 / 15.594 | 15.099 / 15.101 | 9.919 / 9.915 | 9.721 / 9.724 | 18.959 / 18.968 | 21.557 / 21.490 |
| ClarkNet | 170,957,969 | 19.120 / 19.128 | 17.035 / 17.046 | -1,094,291 (-10.90 %) | 9.142 / 9.139 | 14.443 / 14.449 | 13.703 / 13.708 | 13.824 / 13.827 | 7.853 / 7.857 | 9.305 / 9.311 | 15.378 / 15.378 | FAIL (12.682, not lossless) |
| USask | 233,440,918 | 16.654 / 16.668 | 15.276 / 15.286 | -1,264,306 (-8.27 %) | 8.633 / 8.630 | 13.598 / 13.599 | 12.861 / 12.864 | 13.130 / 13.132 | 7.648 / 7.645 | 8.586 / 8.587 | FAIL (17.348, not lossless) | FAIL (17.618, not lossless) |
| Calgary | 52,276,884 | 21.122 / 20.978 | 18.659 / 18.535 | -326,703 (-11.66 %) | 12.361 / 12.312 | 16.873 / 16.810 | 15.133 / 15.095 | 15.510 / 15.445 | 9.613 / 9.562 | 9.439 / 9.420 | **21.254 / 21.130** | **21.233 / 21.116** |

**Against DeLog** SemZip wins 4/4 on the full file and 4/4 on the suffix. Total bytes are 34,452,497 for SemZip and
38,224,332 for DeLog, a difference of -3,771,835 B (-9.87 %). SemZip is also smaller than every main baseline on
every file.

**Optional baselines** (adapted methods):
- **Calgary: both are smaller than SemZip.**
  - LogReducer+R: 2,459,634 B, which is 15,387 B or 0.62 % smaller than SemZip's 2,475,021 B.
  - LogShrink+R: 2,462,003 B, which is 13,018 B or 0.53 % smaller.
  - On the suffix, LogReducer+R is 2,135,093 B and LogShrink+R 2,136,530 B, against SemZip's 2,150,591 B.
- **NASA:** SemZip is smaller than both:
  - LogShrink+R: 9,520,957 B. NASA is the only file where LogShrink's native output is byte-exact (19/19 blocks,
    residual 0 B).
  - LogReducer+R: 10,825,364 B.
- **ClarkNet:** SemZip is smaller than LogReducer+R (11,116,818 B).

Failures, which are not counted as results:
- **LogReducer+R, USask: FAIL.** The official `THULR` segfaults (exit 139) on block 2 (raw SHA-256 `23fa171d…e896`).
  No archive was produced for that block, so 24/25 blocks pass and the full file fails.
  - The failure is deterministic. Re-running `lr_run2.py` on block 2 alone gives the same segfault; the evidence is
    in `baselines/logreducer_probe/`.
  - The 17.348 shown in the table excludes the missing block and is not a valid ratio.
- **LogShrink+R, ClarkNet: FAIL.** On 9 of 17 blocks the official `restore` raises `KeyError` in `recover_pat` on the
  tiny second segment that LogShrink's universal-newline segmentation creates from bare CR bytes. Examples:
  - block 0 is segmented as [100000, 5];
  - all 9 failing blocks contain CR bytes, and only 2 CR-containing blocks decode.

  The adapter's residual cannot repair a crashed native decode.
- **LogShrink+R, USask: FAIL.** On block 2 the LogShrink encoder itself raises an exception
  (`pandas.concat: No objects to concatenate`), so 24/25 blocks pass.

Native (unrepaired) LogReducer and LogShrink are not byte-exact on any file except LogShrink on NASA. Their native
ratios are in `summary_all.json`, labelled not losslessly verified.

**SemZip pipeline facts** (`runs/<D>/r1/repeat.json`):
- 20/20 syntheses PASS. API calls per dataset: Calgary 40, ClarkNet 15, NASA 10, USask 13. The API key occurs
  0 times in the logs.
- All 5 candidates passed into the pool on every file.
- Selected tags and block-0 cost:

  | D | selected tags | block-0 cost |
  |---|---|---|
  | Calgary | A, B, RV0, X0A | 324,555 B; empty program 367,459 B |
  | ClarkNet | IP, B, C, RV0, RV1 | 542,464 B; empty 609,636 B |
  | NASA | IP, B, C, RV0 | 495,712 B; empty 557,996 B |
  | USask | IP, B, RV0 | 592,777 B; empty 656,429 B |

- The publish refit was identical to the selection fit on all 4 files.
- Formal runs had 0 semantic fallback blocks.
- Wall time from the first synthesis to the formal end was 15-20 min per file. 3 workers were used, on a shared
  machine.

**Resource note:** at most 4 worker processes of this track ran at any time (UTC):
- 12:32-13:05, the main queue: 4 slots, with syntheses capped at 3 and baselines at 1 worker;
- 13:07-13:26, the optional baselines: 2 + 2 workers;
- 13:27, the single-block LogReducer probe: 1 worker.

The running `lr_run2.py` Thunderbird job (pid 447555) was not touched.

## 7. Deviations and notes

- **Timestamps.** The design section is stamped "预注册，2026-10-01 13:20 UTC" (in this copy). It was already present in
  `DEV_DESIGN_R76_zh.md` when this track read it at about 12:13 UTC by the workstation and execution-host clocks. All R76-G runs
  happened at 12:32-13:27 UTC on those clocks. The pre-registration text came before every result, but the stamp
  in the design file reads later than the runs; the design's change log corrects it (see `../DEV_DESIGN_R76_zh.md`).
- **Downloads.** Two of the files were truncated by the workstation downloader's timeout and were range-resumed; see
  section 1.
- **Driver additions.** `rep.py` adds a synthesis concurrency cap (3) and a baseline lane inside the shared slot
  budget. Neither changes any SemZip decision or byte.
- **Formal smoke.** The smoke used the residual-only program (0 API) rather than a real synthesis. It is evidence
  for the adapted formal path only and not a result.
- **Optional baselines.** LogShrink+R runs through an adapted copy of its runner (section 3). LogReducer+R runs
  through the unchanged `lr_run2.py`. The extra single-block LogReducer run (`baselines/logreducer_probe/`) is a
  determinism check of its USask crash and is not a result.
- **No retries.** Nothing was retried or re-run to obtain a better number. Every attempt directory is retained.
