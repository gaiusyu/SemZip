# R76 track `timing/`: formal timing of the new baselines (pre-registered design section D)

> **This copy.** **T3 finished** (2026-10-01; every one of the 120 trial cells has a clean terminal result, and
> LogReducer+R on Spark and LogShrink+R on Spark and Windows are timed only because they do not restore their blocks
> byte for byte, as expected): `runs/T3/` holds its design
> (`DESIGN_20261001053412.json`), one `result.json` per trial attempt (SemZip trials also `decode/summary.json`) and the
> summary `SUMMARY_T3.{json,md}`, which `analysis/r76/r76_tables.py` reads for the Large column of the speed table.
> **T2 is incomplete and not used**: it was stopped by design after 23 of 216 trials to free the host for the
> pre-registered unseen-source track (`../DEV_DESIGN_R76_zh.md`, section R76-G); `runs/T2/` keeps its two design files
> and the finished trial records for completeness only (`runs/T2/INCOMPLETE.md`), and there is no `SUMMARY_T2`. The
> speed table's Small column is therefore the earlier serialized session A (`evidence/selection/results/timing/`), and
> no small-file speed of XZ9e, Zstd19, LogShrink+R or LogReducer+R is reported. `runs/STATUS.json` is the heartbeat at
> the T2 stop. The launchers that resumed the sessions after host restarts (`start_after_runs.sh`, `start_after_lr.sh`,
> `start_after_lr2.sh`, `chain_20261001.sh`) are included; `samples/` (LogHub blocks), `vendor/` (a byte-identical copy
> of `../codecs/codec_baseline_r76.py`), `logs/` and the per-trial work files (requests, phase records, archives) are
> not. The smoke design and summary files (`runs/smoke/*/DESIGN_*.json`, `runs/smoke/*/SUMMARY_smoke_*.{json,md}`) are not
> included either; the smoke trial records are (`runs/smoke/{T2,T3}/<tag>_*/result.json`), and the table under "Smoke"
> lists every smoke trial. Times in this README are UTC. The driver named session tags (`<tag>` = `YYYYMMDDhhmmss` in
> run directory and `DESIGN_<tag>.json` names) after the execution host's local clock; in this copy every tag is the
> UTC time instead, and every reference to it (READMEs, `runs/T3/SUMMARY_T3.json` `trial_dirs`, the `_dest` and argument
> paths in the trial records) was renamed accordingly.

Pre-registered design: `../DEV_DESIGN_R76_zh.md`, section D (成本): "加入新基线，与 R73 `timing_campaign.py` 同协议——
12 个文件，3 次，串行，交替顺序，等机器空闲" and "4 个大文件：每个文件取 20 个等间隔块，块号为 round(i·(N−1)/19)，
i=0..19。所有方法串行计时 3 次，取中位数。写明这是抽样块，不是全文件。" plus "补充按字节加权的吞吐".
Everything this track writes is inside `timing/`. Other directories are only read (R68 harness/binaries/inventories,
R73 art + pool publications, R76 `codecs/`, `baselines/logreducer/`, `baselines/logshrink/` runners). No process of
any other job is paused, reniced or killed (unlike R73 `start_timing.sh`, which SIGSTOPped its own campaign jobs):
the driver only waits until the container is quiet.

## Files

| path | what |
|---|---|
| `timing_r76.py` | driver: sample materialization, sessions T2/T3, smoke, summary, status |
| `phase_lr.py` | LogReducer+R encode/decode CLI phases around `../baselines/logreducer/lr_run2.py` |
| `phase_ls.py` | LogShrink+R encode/decode CLI phases around `../baselines/logshrink/adapter/ls_run.py` |
| `phase_codec_r76.py` | xz9e/zstd19 codec phases (analogue of R68 `formal_campaign_v3.py codec-phase`) |
| `vendor/codec_baseline_r76.py` | byte-identical copy of `../codecs/codec_baseline_r76.py` (checked at every start) |
| `check_sample_semzip.py` | read-only check: SemZip per-block archive bytes on a sample = R73 whole-file run |
| `samples/<D>.sample20.{log,json}` | T3 inputs + verification manifests; `samples/HDFS.mini2.*` = smoke mini-sample |
| `runs/T2/`, `runs/T3/` | formal sessions: one dir per trial attempt, `DESIGN_<tag>.json`, `SUMMARY_T3.{json,md}` (T2 has no summary, `runs/T2/INCOMPLETE.md`) |
| `runs/smoke/` | smoke (NOT formal): one dir per trial with its `result.json`; its `DESIGN_*.json` and `SUMMARY_smoke_*.{json,md}` are not included in this copy |
| `runs/STATUS.json` | driver heartbeat (waiting for quiet / current trial) |
| `logs/` | `smoke.log`, `formal.log`, `materialize*.log` |

SHA-256 of the code: see "Hashes" at the end (also recorded in every `DESIGN_<tag>.json` → `static.sha256`, together
with the hashes of every runner and binary a trial executes).

## Protocol (identical to R73 `timing_campaign.py` / R68 formal V3 unless listed under Deviations)

- 100,000-line independent blocks (R68 `scan()` block law; `lr_run2.scan`, `ls_run.scan`, `codec_baseline_r76.scan`
  are checked to be instruction-identical to R68 `scan` at start-up), **4 block workers for every method**, one
  native thread per block (xz/zstd `-T1`, DeLog/LogLite single-thread, SemZip 4 worker processes, LR/LS one
  official subprocess chain per block).
- Per trial: `inventory()` of the input first (outside timers; warms the page cache and gives the reference hashes;
  must equal the R68 first-pass inventory, or the verified sample manifest), then a **separate encode CLI** and a
  **separate archive-only decode CLI**, each timed as parent-subprocess wall time (`timed()`, copied from R73):
  encode = interpreter start + imports + streaming read/split + block work + archive writes; decode = archive-only
  restore of the complete output file. The decoder gets only the archive directory and block order.
- SHA-256 audit of the restored file, per block and full file (`audit()`, copied from R73), outside timers; restored
  file and archives are deleted after the audit (as R73); per-phase JSON ledgers are kept.
- Quiet detector (copied from R73): before each attempt wait until the container's cgroup `cpuacct.usage` shows
  < 0.5 cores over a 5 s window while the driver sleeps (poll every 30 s; give up after 6 h and run anyway);
  during each timed phase `other_cores` = (cgroup CPU − own `RUSAGE_CHILDREN` CPU) / wall. A trial is contaminated if
  any phase has `other_cores` > 0.5; contaminated trials are retried (up to 4 attempts per invocation).
- Serialized (one method at a time), rotating method order: in repeat r, dataset i starts at method offset
  (r−1+i) mod M (R73 `schedule()`).
- 3 repeats; per dataset the median of the latest clean terminal result of each repeat (reruns never add
  observations); geometric mean over datasets; byte-weighted MB/s = Σ bytes / Σ median seconds (design D addition).
- Resume: rerun the same command; cells (dataset, method, repeat) with a clean terminal result are skipped.
- Sessions are never mixed: T2 and T3 have separate directories, schedules and summaries; smoke results carry
  `formal: false` and are never read by the formal summaries.

### Methods

| method | runner / code path | encode timed phase(s) | decode timed phase |
|---|---|---|---|
| `semzip` | R73 code path: `r73_qg/art/frozen/guarded_backend_v2.py` (R71 guarded runtime) with the frozen R73 **pool** plan `r73_qg/runs/publish/pool/<D>/program.json` + its storage policy (`SEMZIP_R54_PLAN`) | `encode` | `decode` (policy made unavailable) |
| `delog`, `loglite`, `gzip6`, `xz6`, `zstd3` | R68 `formal_campaign_v3.py codec-phase` with the pinned R68 specs (as R73) | `encode` | `decode` |
| `xz9e`, `zstd19` | `phase_codec_r76.py` = R68 codec-phase logic with the R76 harness (`xz -9e -T1`; zstd 1.5.6 `-19 -T1` built in `../codecs/tools`); specs equal those of the `../codecs` full run (checked, `DESIGN_*.json`) | `encode` | `decode` |
| `logreducer_r` | `phase_lr.py` → `lr_run2.encode_block` / `decode_block` (LogReducer @40005419 official pipeline per block + counted residual) | `encode` = adapted (native pipeline + verification decode with official LogRestore + residual); `encode_native` = native pipeline alone | `decode` = official LogRestore + residual application |
| `logshrink_r` | `phase_ls.py` → `ls_run.encode_block` + `pack_archive`, `ls_run.decode_block` + `build_residual` / `apply_residual` (LogShrink @59ce494, official L per dataset, `restore_r76.py` decoder) | `encode` = adapted (native + verification decode + residual); `encode_native` = native `.lsz` only | `decode` = official restore + residual |

For the two adapted methods both encode times are reported: `encode_MB_per_s` (the adapted method's full encode)
and `encode_native_MB_per_s` (native encode alone). The native-only encode is a **third timed phase in its own
process and workspace** (after the decode and the SHA audit), with the same splitter/4 workers; its archives are not
lossless by the pre-registered verdict, so they are not SHA-audited but checked against the adapted run
(`native_encode_consistency`: same per-block failure pattern, same templates/segments/THULR statistics/model files,
native container bytes, which may differ by a few tens of bytes per block because 7z stores file mtimes). For
LogReducer the native-only run calls the unchanged `lr_run2.encode_block` with its verification decode and residual
construction replaced by no-ops inside that process only (remaining extra work: one read of the block spool).

Per-block runner failures (e.g. official training crashing on a block) do not abort a phase: the phase completes,
the failure is listed in `block_failures`, the SHA audit fails and the trial status is `SHA_FAIL`. `SHA_FAIL` is a
terminal result (timed and repeated like `PASS`) and is reported separately as "timed only, not lossless"; it never
enters the lossless aggregates. `FAILED` (a phase exits non-zero / driver error) is retried on the next invocation.

## Sessions

- **T2** (full files, R73 order): Linux Proxifier Apache Zookeeper Mac HealthApp HPC Hadoop OpenStack OpenSSH Android
  BGL × `semzip delog logreducer_r logshrink_r xz9e zstd19` × 3 = 216 trials. SemZip and DeLog are re-timed here as
  anchors that relate this session to the R73 session (R73 medians are not mixed into it).
- **T3** (sampled blocks): HDFS Spark Windows Thunderbird × `semzip delog loglite gzip6 xz6 zstd3 xz9e zstd19
  logreducer_r logshrink_r` × 3 = 120 trials. **All T3 rates are MB/s on the 20 sampled blocks, not whole files.**

### T3 samples (`samples/`, created by `timing_r76.py materialize`)

Original block indices round(i·(N−1)/19), i = 0..19 (N from the R68 inventory; i·(N−1)/19 never has fraction .5):

| dataset | N | sampled original blocks | sample bytes | sample SHA-256 |
|---|---|---|---|---|
| HDFS | 112 | 0 6 12 18 23 29 35 41 47 53 58 64 70 76 82 88 93 99 105 111 | 277,338,890 | b5d25abdfea5ab43f5565b5337b863ab0781ba746473047a2f7a363d6403825a |
| Spark | 333 | 0 17 35 52 70 87 105 122 140 157 175 192 210 227 245 262 280 297 315 332 | 170,382,248 | 2b4df2ac4c3a4c1e2e650f03efdb9d3054cee0e4f44b46056b420e03f43fa4df |
| Windows | 1147 | 0 60 121 181 241 302 362 422 483 543 603 663 724 784 844 905 965 1025 1086 1146 | 465,577,918 | 668812d50a6bbe5cd11efffd2149dd4d7df80c384dd123e2f3c5b91c4a3ef2b6 |
| Thunderbird | 2113 | 0 111 222 333 445 556 667 778 889 1000 1112 1223 1334 1445 1556 1667 1779 1890 2001 2112 | 268,957,222 | e4625e647b05bda05f3d26339d3c8af58c60805556f0a9bd471b5b27420ae502 |
| HDFS mini2 (smoke only) | 112 | 0 111 | 24,999,077 | 7364ebc60d0e6d5e88d4f1e7adfa09f9ae98160e5f3e122af44d1abd524c4939 |

Each block was read at its R68-inventory byte offset of the original file and its SHA-256, byte count and LF count
checked against `r68_external_20260923/first_pass/input_<D>.json` before writing; the written file was then re-split
with the protocol splitter and every block (bytes, LF count, records, terminal LF, SHA-256) matched again. Every
trial re-inventories its input and compares it with the manifest. All four last blocks (the partial tail block of
each file, placed last) end with LF. `HDFS.mini2.log` (smoke only) = sample positions 0 and 19 (original blocks 0
and 111).

Known from the whole-file runs of the adapted methods (not rerun here, so not yet a timing result): on Spark
sampled blocks 35, 52 and 70 the official LogReducer and LogShrink training crash (no archive), and on Windows
sampled block 302 LogShrink's official restore fails; these trials are expected to end `SHA_FAIL` and will be
reported as timed-only. Thunderbird results of those whole-file runs were not finished when this was written.

## Smoke (non-formal; 1 repeat; quietness ignored while ~7 other cores were busy)

Command (2026-09-29 20:47 UTC, detached): `AGNICE=5 ../launch.sh logs/smoke.log python3 -u timing_r76.py smoke`
(code hashes as in "Hashes", recorded in the smoke design files, which are not included in this copy). T2 methods on Linux and Proxifier (whole files), T3
methods on `HDFS.mini2.log`. Every trial: separate encode and decode processes, archive-only decode, per-block and
full-file SHA audit outside the timers. The container was fully loaded (other_cores 6.3-7.9 in every phase), so every
trial is flagged `CONTAMINATED`; the seconds below are NOT timing results, only evidence that each path runs.

**22/22 trials PASS (archive-only decode, SHA pass).** Archive bytes match the whole-file runs of each track
(e.g. LogReducer+R Linux 91,194 vs 91,182, Proxifier 108,167 vs 108,152; LogShrink+R Linux native 82,752 vs 82,774;
xz9e/zstd19/DeLog identical to `../codecs` and R68; differences = 7z header mtimes). Native-only encodes: same
per-block structure as the adapted run in all 5 adapted trials, native bytes within 0-8 B per block.
`check_sample_semzip.py` on the HDFS mini-sample: SemZip semantic archive bytes of sample blocks 0/1 = R73 whole-file
blocks 0/111 (244,092 and 414,268 B), i.e. a sampled block is encoded exactly as inside the whole file.

| session | dataset | method | status/SHA | archive B | raw B | blocks | encode s | decode s | encode_native s | native consistency (max B diff) |
|---|---|---|---|---|---|---|---|---|---|---|
| T2 | Linux | semzip | PASS | 58,613 | 2,349,686 | 1 | 5.2 | 1.8 | | |
| T2 | Linux | delog | PASS | 85,140 | 2,349,686 | 1 | 1.8 | 1.2 | | |
| T2 | Linux | logreducer_r | PASS | 91,194 | 2,349,686 | 1 | 30.0 | 13.4 | 18.8 | equal (0) |
| T2 | Linux | logshrink_r | PASS | 85,476 | 2,349,686 | 1 | 51.6 | 23.8 | 30.6 | equal (1) |
| T2 | Linux | xz9e | PASS | 104,032 | 2,349,686 | 1 | 2.1 | 0.7 | | |
| T2 | Linux | zstd19 | PASS | 120,226 | 2,349,686 | 1 | 2.1 | 0.8 | | |
| T2 | Proxifier | semzip | PASS | 70,361 | 2,541,814 | 1 | 5.7 | 1.9 | | |
| T2 | Proxifier | delog | PASS | 84,180 | 2,541,814 | 1 | 1.4 | 1.4 | | |
| T2 | Proxifier | logreducer_r | PASS | 108,167 | 2,541,814 | 1 | 39.4 | 32.3 | 9.3 | equal (5) |
| T2 | Proxifier | logshrink_r | PASS | 105,959 | 2,541,814 | 1 | 125.7 | 57.9 | 68.8 | equal (8) |
| T2 | Proxifier | xz9e | PASS | 112,148 | 2,541,814 | 1 | 3.7 | 0.8 | | |
| T2 | Proxifier | zstd19 | PASS | 120,664 | 2,541,814 | 1 | 2.1 | 1.0 | | |
| T3-mini | HDFS | semzip | PASS | 850,950 | 24,999,077 | 2 | 10.4 | 2.5 | | |
| T3-mini | HDFS | delog | PASS | 873,024 | 24,999,077 | 2 | 3.0 | 1.5 | | |
| T3-mini | HDFS | loglite | PASS | 1,972,938 | 24,999,077 | 2 | 3.7 | 1.5 | | |
| T3-mini | HDFS | gzip6 | PASS | 2,587,055 | 24,999,077 | 2 | 1.6 | 0.8 | | |
| T3-mini | HDFS | xz6 | PASS | 1,544,804 | 24,999,077 | 2 | 9.8 | 1.3 | | |
| T3-mini | HDFS | zstd3 | PASS | 2,553,155 | 24,999,077 | 2 | 0.9 | 1.0 | | |
| T3-mini | HDFS | xz9e | PASS | 1,437,916 | 24,999,077 | 2 | 14.5 | 0.8 | | |
| T3-mini | HDFS | zstd19 | PASS | 1,584,848 | 24,999,077 | 2 | 12.1 | 0.9 | | |
| T3-mini | HDFS | logreducer_r | PASS | 1,311,944 | 24,999,077 | 2 | 19.3 | 7.9 | 11.1 | equal (4) |
| T3-mini | HDFS | logshrink_r | PASS | 1,890,788 | 24,999,077 | 2 | 68.4 | 25.6 | 36.0 | equal (7) |

(The smoke summaries `SUMMARY_smoke_*.{json,md}`, not included in this copy, repeat these as MB/s; their header line
about other_cores <= 0.5 does not apply to the smoke, whose JSON carries `"note": "SMOKE, NOT FORMAL"`.)

## Formal launch

Launched 2026-09-29 21:08 UTC, detached, autogroup nice 0:

```sh
cd <WORKDIR>/r76_additional_20260929/timing && \
AGNICE=0 <WORKDIR>/r76_additional_20260929/launch.sh \
  logs/formal.log python3 -u timing_r76.py formal
```

pid 875620 (`runs/DRIVER.lock`), log `logs/formal.log`, design `runs/T2/DESIGN_20260929210818.json`. It runs T2
(216 trials) and then T3 (120 trials); before every attempt it waits for the quiet condition (at launch the
container used ~6.5 other cores: the LogShrink/LogReducer/codecs/library Thunderbird runs and the Denum run).

- Progress: `python3 timing_r76.py status` (clean cells per session, heartbeat), `grep TIMING logs/formal.log`,
  `cat runs/STATUS.json`.
- Results: `runs/T2/SUMMARY_T2.{json,md}` after T2, `runs/T3/SUMMARY_T3.{json,md}` after T3 (also
  `python3 timing_r76.py summary T2|T3` at any time; incomplete cells are listed, never filled).
- Resume after any interruption: the same command with a new log name (`logs/formal_2.log`, ...); clean cells are
  skipped, a stale `runs/DRIVER.lock` of a dead driver is removed automatically.
- Stop: `kill -- -875620` (setsid made the driver a process-group leader; the group holds only this driver and its
  current trial's processes). Then resume as above; the interrupted trial has no result.json and is simply rerun.

Expected runtime once the machine is quiet (from the whole-file runs of each track, R73 timing and this smoke;
uncertain by about ±30%): T2 ≈ 2.4-2.7 h per repeat (LogShrink+R ≈ 1.5 h: Android/BGL dominate; LogReducer+R ≈
0.5 h; SemZip ≈ 9 min; xz9e ≈ 8 min; zstd19 ≈ 5 min; DeLog ≈ 1.5 min), so ≈ 7.5-8 h for 3 repeats; T3 ≈ 1.3-1.5 h per
repeat (LogShrink+R ≈ 40 min, LogReducer+R ≈ 16 min, xz9e ≈ 7 min, SemZip ≈ 8 min, the rest a few minutes), ≈ 4-4.5 h.
Total ≈ 11-13 h of quiet time plus retries of contaminated trials (each retry re-waits for quiet).

## Deviations and notes

1. **No pausing of other jobs.** R73 `start_timing.sh` SIGSTOPped its own campaign jobs during timing; R76 only
   waits for the quiet condition (rule: never touch processes this track did not start).
2. **`PYTHONDONTWRITEBYTECODE=1`** for every timed process (and `sys.dont_write_bytecode` in the driver/phases), so
   no `__pycache__` is written into the read-only R68/R73/R76 trees. Zero timing effect: every module those processes
   import already has a valid `.pyc` (checked; only never-imported scripts lack one). The vendored
   `codec_baseline_r76.py` is precompiled into `vendor/__pycache__` by the driver.
3. **Thread caps for the adapted methods only**: `OMP/OPENBLAS/MKL/NUMEXPR_NUM_THREADS=1` (one native thread per
   block; numpy's OpenBLAS would otherwise start one thread per host CPU = 119) and `LC_ALL=LANG=en_US.UTF-8` (what
   both runners require/set). The anchors and R68 methods keep exactly the R73 environment (`env_clean()`).
4. **Workspaces**: every method works in its trial directory on `<MOUNT>` (as R68/R73 timing). The LogShrink
   whole-file run used local `/tmp` scratch; here it uses the same filesystem as every other method.
5. **Native-only encode as a separate timed process** (see Methods) instead of a sum of per-block times.
6. **`SHA_FAIL` terminal status** (R73 only knew PASS/FAILED) so that deterministic per-block failures of the
   official tools are timed and reported instead of re-run forever; `FAILED` keeps R73's retry-on-resume behaviour.
7. Trial directories carry a full timestamp tag (R73: HH:MM:SS; both UTC in this copy) and "latest" is ordered by
   `finished_unix`.
8. Heartbeat: while waiting, a `WAIT quiet` line about every 10 minutes and `runs/STATUS.json`.
9. `zstd19` uses the R76 zstd 1.5.6 build (as in `../codecs`), `zstd3` the R68 zstd 1.4.8 (as in R68/R73).
10. Sample materialization settles the written file (fsync + stable stat for 2 s, `codec_baseline_r76.settle_file`)
    before the identity-guarded re-inventory: the first attempt hit the known `<MOUNT>` lazy-mtime race on
    Windows (`logs/materialize_try1_mtime_race.log`); the partial file was rewritten from scratch.
11. LogShrink phases run under the LogShrink venv interpreter (`venv/bin/python3` → `/usr/bin/python3.9`, the same
    binary as the system `python3`), as its own driver did.

## Hashes

| file | sha256 |
|---|---|
| `timing_r76.py` | not listed (machine-specific path replaced in this copy) |
| `phase_lr.py` | `dcfc9b7178a93646db3b7577e85acdba11ba2b90620d183a4ce8222a12b63caf` |
| `phase_ls.py` | `92218db24cc55277dfdfeb56edb95ce618035c9054bc7ec29ece7a191e4b5680` |
| `phase_codec_r76.py` | not listed (machine-specific path replaced in this copy) |
| `check_sample_semzip.py` | `4966eaf84011e39dc8741342f3911c00873a82afbe3320e5406864faa207893c` |
| `vendor/codec_baseline_r76.py` | = `../codecs/codec_baseline_r76.py`; not listed (machine-specific path replaced in this copy) |
| runner `../baselines/logreducer/lr_run2.py` | `bbf044c186b58bd40d0c38826df2a0a2b09add1074eee7d24024d0598c6ce549` (= its full-run manifest) |
| runner `../baselines/logshrink/adapter/ls_run.py` | not listed (machine-specific path replaced in this copy) |
| R68 `formal_campaign_v3.py` / `codec_baseline.py` | `cf4d58f0...9f87` / `0b0c15f6...c26d` |
| R73 `art/frozen/guarded_backend_v2.py` | `72bca1ca7c92f797d5a6a11d2300ad0bdee2ae209f972044b0fe3eb86c8b4d01` |
| R73 `timing_campaign.py` (copied logic) | not listed (machine-specific path replaced in this copy) |

The smoke and the formal launch used exactly these driver/phase files (`static.sha256` in each `DESIGN_*.json`; the
smoke design files are not included in this copy).

## Notes (2026-09-29 21:40 UTC)
- The first formal driver (pid 875620) was stopped before any trial ran, because its 6-hour give-up would spend contamination attempts while the Thunderbird runs of the other R76 tracks were still active. start_after_runs.sh now waits for those runs (LogReducer 1225310, codecs 1245963, LogShrink 1396946, library 1463284, Denum encode-only 731978) to exit and then execs the unchanged timing_r76.py formal (log logs/formal_after_runs.log).
- Not timed, by design: zstd22long, zstd19dict and delog_generic (supplementary codec settings/controls, compression-only interest), Denum (not losslessly verifiable), SemZip-1 (ablation already timed in R73).
- LogReducer+R and LogShrink+R keep their official 7-Zip LZMA commands, which run multithreaded (p7zip 16.02 default, up to 3 threads observed) and their residual step uses 4 threads in one Python process; both are disclosed deviations from 'one native thread per block' (the first favors the baselines).
- T3 cross-method statements must use a matched subset of datasets where every compared method is lossless (LogReducer+R and LogShrink+R fail on sampled Spark blocks; LogShrink+R on Windows block 302).
