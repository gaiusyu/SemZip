# R76 baseline track: LogReducer (USENIX FAST'21)

Track directory: `r76_additional_20260929/baselines/logreducer/`. Written 2026-09-29, before any full-run result.

> **This copy (final records).** `full/` holds the final record of every dataset. A resumed rerun of the full
> campaign, started 2026-10-01 00:04 UTC, re-ran Linux, Proxifier and Apache (`logs/full_resume_20261001.log`) and
> Zookeeper (11:52 UTC, `logs/full_resume_zk_20261001.log`: PASS, byte-exact archive-only decode, 93,001 B, 112.11x,
> the same bytes as its first run). For Thunderbird, a resume at 00:05 UTC (`logs/full_resume_tb_20261001.log`) wrote
> no result, and a relaunch with 8 workers was rejected by the driver's argument check before it started
> (`logs/full_resume_tb2_20261001.log`; `--workers` accepts 1-4). The final resume (`after_t2_lr_tb.sh`, 4 workers,
> `logs/after_t2_lr_tb.log`, `logs/full_resume_tb3_20261001.log`) ran from 12:11 to 19:59 UTC on 2026-10-01, before
> the 2026-10-02 04:00 UTC cut-off for sampled blocks (`../../DEV_DESIGN_R76_zh.md`, change log of 2026-09-29
> 05:30 UTC), reused the 2,113 encoded blocks and
> decoded every block from its archive alone. **Thunderbird: PASS**, 2,113/2,113 blocks and the full file with SHA-256
> equal to the original; 702,689,351 B, **45.24x** (suffix 45.25x); native part 638,308,535 B (49.80x, not
> byte-exact), residual 64,380,816 B, no full-fallback block; archive-only decode 27,345 s with 4 workers
> (`full/Thunderbird/result.json`). `full/summary.json` is the summary of that last invocation (Thunderbird only).
> Spark is a terminal FAIL (48 of 333 blocks not restored byte for byte), as expected, so LogReducer+R has a PASS
> record on 15 of the 16 files. `analysis/r76/r76_tables.py` reads `status.json`: the LogReducer+R cell for Spark and
> the aggregate rows that need all 16 files (mean, geometric mean, corpus, suffix) print `--`, and the "smaller" row
> counts the 15 files with a PASS record. Included from the resumed invocations: the run manifests
> (`full/manifest_<tag>.json`; the tag is the UTC time, equal to `created_at` inside) and the driver logs (`logs/`; one
> JSON line per dataset, plus the launcher's start/exit lines). The first full run's manifest, the input
> inventories, per-block records, diagnostics, block files, archives and all other logs are not included.

## Upstream and build

- Repository: the LogReducer repository cited in the paper (`LOGREDUCER_REPO_URL` in `external/upstreams.conf`). Pinned commit `40005419022c02454eca7b027d76b99b3dfad543`
  ("Update README.md", 2021-11-11). This was cloned on a workstation, packed as `src/LogReducer_40005419.tgz`, and copied to
  `src/LogReducer/`. The commit is recorded in `src/UPSTREAM_COMMIT`. The SHA-256 of every tracked source file was
  checked against that clone and all of them match. No upstream file is modified.
- Build: `cd src/LogReducer && make clean && make` with g++ (Debian 10.2.1-6). The build log is `src/build.log`.
  The only warning is upstream's `LengthSearch.cpp: no return statement`. Output binaries: `THULR`, `Elastic`, `Iddiff`,
  `Numdiff`, `Entropy`. Each run manifest records the SHA-256 of `THULR` and `Elastic`.
- Runtime environment: Python 3.9.2, pandas 2.3.2, numpy 1.26.4, six, p7zip 16.02 (`7za`), and a UTF-8 locale (the
  driver refuses to run under any other locale). pandas 2 prints `FutureWarning` for `pd.value_counts` in `header.py`.
  That warning is harmless.

## What the official pipeline is (README + code reading)

1. Training: `python3 training.py -I xx.log -T template/`. In its default mode `-m Nor`, training.py runs
   `sampler.py xx.log xx.log.sample 0.001`. That samples `max(50, 0.1% of 20-line chunks)` chunks of 20 lines, which is
   1,000 lines for a 100k-line block. It then learns the head format (`head.format`), templates (`template.col`) and
   per-template variable rules (`E<k>basic.rule`, `E<k>num.rule`, `E<k>string.rule`). All defaults are used:
   `-TL 0 -L 4 --IsMulti F`, generic head regex. No dataset-specific format string is used, and none is required.
2. Compression: `python3 LogReducer.py -I xx.log -T template/ -O out/`, in its default `Tot` mode. The file is read
   as ISO-8859-1 text with universal newlines. It is cut into 100,000-line segments `k.col` by `util.list_write`, which
   `str.strip()`s every line and writes it in UTF-8. Then for each segment it runs
   `./THULR -I seg/ -X k -Y k -O tmp/ -T template/ -E Z -D D -F template/head.format` followed by
   `7za a out/k.7z tmp/k/* -m0=LZMA`.
3. Decompression: `python3 LogRestore.py -I out/ -T template/ -O xx.log`. This runs `restore.py` on each `k.7z`
   (restore.py uses `Elastic`) and merges the results in Tot mode.

## Block protocol (driver `lr_run2.py`, schema `semzip.r76.logreducer.v2`)

- Blocks are exactly 100,000 LF records each, split by the verbatim R68 `scan()`/`inventory()` block law. Every
  block and the full file get a SHA-256 before encoding.
- Each block runs the whole official pipeline on its own, in fresh processes, including its own training on that
  block. No state is shared across blocks: templates, rules and the head format are all per block. Every artifact the
  decoder reads is stored inside the block archive and counted.
- Encoding steps for each block:
  1. Run the official `sampler.py` through `runpy` after `random.seed(0)`. This is the only change from `-m Nor`, which
     calls the same script unseeded. After that, run the official `training.py -I block.log -T tpl/ -m Sam`.
  2. Run the official Tot-mode segmentation code, copied verbatim from LogReducer.py lines 173-196 and run with the
     official `util.py`.
     **Block adaptation:** when a block has exactly 100,000 records, the official loop also writes an empty trailing
     `1.col`. We delete that empty segment. It holds no records. If it is kept, the official driver runs THULR on it
     (with `-TN 1`, the same process runs it after segment 0, and stale global state produced a 103 KB `1.7z` in
     exploration). A segment count above 1 would appear only if bare CRs split records. Each non-empty segment gets its
     own THULR process.
  3. Run the exact THULR and 7za commands of `LogReducer.py procFiles` for each segment.
  4. Build `model.7z` with `7za -m0=LZMA` over exactly the template-directory files that `restore.py` reads:
     `template.col`, `head.format`, and `E<k>basic.rule` for each k in template.col. Its size is counted.
- Archive layout: `[flags u8][nseg u8][nseg x u32][u32 model len] segments model.7z [u32 residual len] residual`.
  Flag bit0 means the original block has no terminal LF (the same decode alignment as the R68 harness uses).
- Archive-only decoding: the driver splits the archive, extracts `model.7z` into an empty `tpl/`, writes `k.7z`, and
  runs the official `python3 LogRestore.py -I in/ -T tpl/ -O dec/lr_restored.txt`. If LogRestore prints
  `Error Occur` or `.col does not exist`, the block is marked as a decode failure.

## Two reported numbers, one archive

- **native** is LogReducer as released. Its bytes are header + segments + model. It is byte-exact only if the
  LogRestore output (minus one trailing LF when flag bit0 is set) equals the original block. On the smoke sets it is
  NOT byte-exact on 4 of 5 datasets, because of the following upstream behaviors:
  - CR dropped from every CRLF record.
  - Leading and trailing whitespace stripped.
  - Head delimiters normalized to the most frequent one (for example, Linux `Jun  9` becomes `Jun 9`).
  - Leading zeros lost in numeric variables (for example, Proxifier `00:01` becomes `0:1` and Linux `03:28:22` becomes
    `3:28:22`).
  - Output transcoded from ISO-8859-1 to UTF-8.

  Following the pre-registered rule, native LogReducer is **"not losslessly verifiable"** and does not enter the
  lossless ratio comparison. Its ratio and loss statistics are reported (`native_*` fields).
  `native_blocks_equal_normalized_input` shows that the output is not even equal to the official normalized input.
  The losses therefore go beyond whitespace.
- **adapted (LogReducer+R)** is native plus a generic, dataset-agnostic residual whose bytes are all counted
  (`residual_bytes`). It is built at encode time like this:
  1. Decode the native part with the same archive-only decoder function.
  2. Map each restored record back with UTF-8 to ISO-8859-1. This is the exact inverse of the official reader and
     falls back to the raw bytes if the conversion fails.
  3. Regroup records that the official universal-newline reader split at bare CRs. The rule is fixed; the list of
     regrouped records is stored.
  4. For every record that still differs, store a patch: either a wrap (prefix + restored + suffix) or an edit
     (common prefix length, common suffix length, middle bytes), whichever is smaller.
  5. Serialize the patches with varints and compress with `lzma FORMAT_ALONE preset 9e`. If the line structure cannot
     be aligned, or the native decoder crashes, the residual falls back to the full block under LZMA
     (`full_fallback`). Every such block is counted and reported.

  `status: PASS` means the adapted output decoded from the archive alone matches the per-block and full-file SHA-256.
  Label the adapted variant as an adapted method (the same convention as the R68 LogLite wrapper). It is not the
  original system.
- Timing: `encode_seconds_wall` covers the whole adapted encoder, including the verification decode and the residual.
  `encode_seconds_native_sum` is the sum over blocks of the native-only encoder time (sample + train + segment + THULR
  + 7za + model). `decode_seconds_net` is the archive-only decode wall time; hashing and diff audit time are excluded.
  Parallelism is `--workers` blocks at a time. The restored file is hashed as a stream in block order and is not
  written to disk as one file. This timing scope is not the D-section formal timing protocol.

## Superseded first attempt (kept for audit)

`superseded_v1/` holds `lr_run.py` (v1) and its smoke outputs, from an earlier session of this track. v1 fed the raw
block bytes straight to THULR, which is Seg-style input without the official Tot normalization. `restore.py` then
crashed on non-UTF-8 bytes (Linux: `UnicodeDecodeError` on byte 0xf7). CRs also stayed inside tokens and broke
template matching (Proxifier match_failed 13,854 in v1 vs 1,062 in v2). v1 is not used for any result.

## Commands

Smoke tests (at most 2 concurrent processes, 1 worker each):

```
R=<WORKDIR>/data/loghub1/original
AGNICE=5 ../../launch.sh logs/smoke2_A.log python3 lr_run2.py --output smoke2_A --workers 1 \
  --input Proxifier=$R/Proxifier/Proxifier.log --input Linux=$R/Linux/Linux.log \
  --input Apache=$R/Apache/Apache.log --input Zookeeper=$R/Zookeeper/Zookeeper.log
AGNICE=5 ../../launch.sh logs/smoke2_B.log python3 lr_run2.py --output smoke2_B --workers 1 --input HealthApp=$R/HealthApp/HealthApp.log
```

Full run: 16 datasets, 3 workers, ascending size so the largest run last. Every input is `readlink -f` of
`data/loghub1/raw/<D>.log`, and the argument list is saved in `full_inputs.txt`.

```
AGNICE=5 ../../launch.sh logs/full.log python3 lr_run2.py --output full --workers 3 $(cat full_inputs.txt)
```

Resume after a crash: remove a stale `full/RUNNING.lock` only after confirming that its PID is dead, then add `--resume`.

Progress:
- `tail logs/full.log` prints one JSON line per finished dataset.
- `cat full/summary.json`
- `cat full/<D>/status.json` shows the phase and encoded/decoded block counts.
- `ls full/<D>/blocks | wc -l`

## Smoke results (v2, archive-only decode, 1 worker)

| dataset | raw bytes | native bytes | native ratio | native byte-exact | residual bytes | adapted bytes | adapted ratio | adapted SHA |
|---|---:|---:|---:|---|---:|---:|---:|---|
| Proxifier | 2,541,814 | 92,986 | 27.34 | no (21,329 lines differ) | 15,166 | 108,152 | 23.50 | PASS 1/1 + full |
| Linux | 2,349,686 | 85,811 | 27.38 | no (10,143) | 5,371 | 91,182 | 25.77 | PASS 1/1 + full |
| Apache | 5,135,876 | 127,939 | 40.14 | yes | 4 (empty) | 127,943 | 40.14 | PASS 1/1 + full |
| Zookeeper | 10,426,438 | 89,584 | 116.39 | no (74,380) | 3,417 | 93,001 | 112.11 | PASS 1/1 + full |
| HealthApp | 23,529,930 | 1,589,354 | 14.80 | no (253,395; 253,062 CR-only) | 1,843 | 1,591,197 | 14.79 | PASS 3/3 + full |

For reference, the R68 first-pass ratios on the same raw files are Proxifier 30.19 / 21.08, Linux 27.60 / 19.97,
Apache 59.77 / 28.38, Zookeeper 156.89 / 29.63 and HealthApp 51.40 / 15.83 (DeLog / xz -6).

## Known run-to-run noise

The official `7za a` stores file mtimes in its LZMA-compressed 7z header, so a rerun on identical input can differ by
a few tens of bytes per block. Templates, match counts and residual stay identical. For example, the Linux payload is
84,076 B in smoke2 and 84,092 B in the full run, and the model is 1,725 B vs 1,728 B. This is inherent to the
official container and was not "fixed".
