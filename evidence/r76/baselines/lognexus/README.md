# R76-H baseline track: LogNexus (ISSTA 2026; preprint title "LogPrism")

Pre-registered in `evidence/r76/DEV_DESIGN_R76_zh.md`, section R76-H, before any LogNexus compression result on
the evaluation files. The only later change (the correction format of LogNexus+R, made after a probe on Linux only)
is recorded in the change log of that section.

## Upstream and build

- LogNexus is the peer-reviewed version of the LogPrism preprint (arXiv 2601.17482; same four authors, same unified
  redundancy encoding). Its ISSTA 2026 artifact is archived on Zenodo, record 21021398
  (https://doi.org/10.5281/zenodo.21021398), file `LogNexus-issta26-ae-source.tar.gz`, SHA-256
  `6baececc1a52594ca419f1198bd996cd437615f5ae79dccdc2339da45ce81678`, Apache License 2.0. The later Zenodo record
  21281836 (SHA-256 `297dcc0a592cb5153c9a243bffb628a1f5343b58597e2c3e60ccf0fb5cbcc598`) differs only in
  `README.md`, `STATUS` and `artifact/MANIFEST.sha256`; the code, scripts and threshold table are byte-identical.
- No upstream source is redistributed here. `bash external/fetch_baselines.sh lognexus` downloads and verifies the
  tarball into `external/upstream/`. Then link or copy the extracted directory to
  `evidence/r76/baselines/lognexus/src/LogNexus-issta26-ae/` (the driver looks there) and run `make` inside it.
- We built the released source unchanged with g++ 10.2.1 on Debian 11 (`native_build.log`); the artifact's own
  container uses Ubuntu 22.04 with GCC 11. The artifact's smoke test passed on this build and reproduced its documented
  Linux ratio of 37.844 at threshold 0.02 (`artifact_smoke_test.log`).

## Protocol (driver `ln_run.py`, schema `semzip.r76.lognexus.v1`)

- Blocks are exactly 100,000 LF records, split by the same block law as every other R76 baseline (R68
  `scan()`/`inventory()`, copied verbatim). Each block and the full file are hashed before encoding.
- Each block runs the official CLI in its own process, with LogNexus's default serial configuration:
  `LogNexus_compress <block> <dataset> 100000 1 1 1 <tau>`. No state is shared across blocks.
- Dataset names: LogHub files use their official names, which select LogNexus's built-in per-dataset regular
  expressions. The four unseen access logs (R76-G) pass their own names, which select no built-in rule.
- Threshold `tau`: `configs/paper_thresholds.csv` of the artifact (the per-dataset values of its paper tables;
  `--profile paper`). Sources without a tuned value use the artifact's untuned default 0.02.
- Every file LogNexus writes into its output directory is stored in the block archive and counted. The archive also
  stores the dataset name, so decoding needs nothing but the archive. Decoding runs the official
  `LogNexus_decompress <archive_dir> <restored_dir> <dataset>` on the archive bytes only.

## Two reported numbers, one archive

- **native**: LogNexus as released. Its artifact claims, and we confirm, restoration of the whitespace-separated
  token sequence only (`bytes.split()`, the criterion of its `scripts/verify_token_roundtrip.py`); it collapses
  whitespace runs and may omit blank lines. `native_blocks_token_equal` counts blocks whose restored token sequence
  equals the original; `native_status` is `PASS` only if every block is byte-exact. Following the pre-registered
  rule, native LogNexus does not enter the lossless comparison.
- **adapted (LogNexus+R)**: native plus a generic, dataset-agnostic residual, compressed with LZMA (FORMAT_ALONE,
  preset 9e) and counted in `residual_bytes`. It is built at encode time from the original block and LogNexus's own
  decoded output only:
  1. blank or whitespace-only records that the restored text omits, stored with their positions and exact bytes;
  2. for every other differing record: if the whitespace-split token lists are equal, only the separators (leading,
     between tokens, trailing) that differ (patch type 3); otherwise the unchanged LogReducer+R wrap/edit patch
     (types 1/2);
  3. a full-block LZMA fallback if records cannot be aligned (`residual_full_fallback_blocks`; 0 in every run).
  This correction is cheaper for LogNexus than the line patch used for LogShrink+R and LogReducer+R.
  `status: PASS` means the adapted output, decoded from the archive alone, matches the per-block and full-file
  SHA-256. LogNexus+R is an adapted method, not the original system.

## Runs

| directory | inputs | outcome |
|---|---|---|
| `smoke/` | Linux, Proxifier, Apache, Zookeeper, HealthApp (harness check) | 5/5 PASS |
| `full_paper/rest/` | 14 LogHub files + NASA, ClarkNet, USask, Calgary | 18/18 PASS |
| `full_paper/win/` | Windows | PASS |
| `full_paper/tb/` | Thunderbird | PASS |

Every `result.json` holds the native and adapted sizes, ratios, suffix ratios (blocks 1 onward), token and SHA
checks, residual statistics and timing. Three drivers ran concurrently (3, 3 and 2 block workers) on an
8-CPU allocation, so the timing fields are not comparable with the formal speed protocol of the paper.
`logs/` holds the drivers' JSON-line progress output; `full_paper_*_inputs.txt` holds the exact argument lists.
Manifest file names were renamed to the UTC time of their `created_at` field (`evidence/r76/CODE_PROVENANCE.md`);
in the records, machine-specific paths and host names were replaced by placeholders (README, "Notes on this copy").

## Summary of results (complete-file ratios)

- Native mean over the 16 LogHub files: 88.17x (the LogNexus paper reports 88.202x for the same configuration).
- LogNexus+R vs SemZip: SemZip is smaller on 15/16 LogHub files and on all four unseen sources. On Thunderbird,
  LogNexus+R is 79.27x vs SemZip 64.86x; Thunderbird is 48.5% of all raw bytes, so LogNexus+R has 8.3% fewer
  summed bytes than SemZip.
- Native (not byte-exact) vs SemZip: SemZip is smaller on 14/16 (not on HPC or Thunderbird).

Regenerate the paper's tables with `bash analysis/run_all.sh <new-stage-dir>`.

## Commands

```
make -C src/LogNexus-issta26-ae                       # after fetching, see above
D=<directory with the LogHub files>
python3 ln_run.py --output smoke --profile paper --workers 2 --input Linux=$D/Linux/Linux.log ...
python3 ln_run.py --output full_paper/tb --profile paper --workers 3 --input Thunderbird=$D/Thunderbird/Thunderbird.log
```
