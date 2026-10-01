# DeLog: whole-file official CLI versus the block adapter (Linux, BGL)

This folder checks that the DeLog numbers in our tables are what the released DeLog produces on these inputs. It runs
the official DeLog binary on two complete LogHub files with the command line of DeLog's own benchmark script and
compares the result with our independent-block adapter (`external/codec_baseline.py`, `external/SETUP.md`). It is a
consistency check only: no table cell is computed from it. Change-log entry "DeLog 数字核对" in
`../DEV_DESIGN_R76_zh.md`.

## What was run

- Binary: `Delog_compress` built from the official DeLog source at commit `64a074f6b655` (`external/SETUP.md`),
  SHA-256 `f4c624240ebf2e54e10da925a9c904bd22ecdd300bc8492b78b25b4301d71739`, the same executable the block adapter
  uses (recorded in `../codecs/README.md` and in the `delog` records under `../codecs/smoke/`).
- Inputs: `Logs/<LogName>/<LogName>.log` were symbolic links to the LogHub files verified by `scripts/prepare_data.py`
  (`metadata/raw_datasets.json`): Linux 2,349,686 B (25,567 lines, SHA-256 `b5410783…d490`) and BGL 743,185,031 B
  (4,747,963 lines, SHA-256 `666130b1…d5a2`). The logs are not included.
- Command line (the parameters DeLog records itself in `experiment_results.csv`: text mode, 100,000-line chunks,
  4 threads, frequency threshold 0, LZMA, normal mode), run in a directory with DeLog's expected layout
  (`Logs/<LogName>/<LogName>.log`, archives written to `output/<LogName>/chunk_<i>.tar.xz`):

  ```text
  Delog_compress Linux text 100000 4 0 lzma normal
  Delog_compress BGL text 100000 4 0 lzma normal
  ```

  The block adapter instead calls `Delog_compress DATASET text 100000 1 0 lzma normal` once per spooled
  100,000-record block, in a fresh directory per block, and counts `output/DATASET/chunk_0.tar.xz`.

## Files

| File | Content |
|---|---|
| `experiment_results.csv` | the row DeLog appends after each run (its own ratio and speed); DeLog writes the `Timestamp` column in the host's local time, converted to UTC in this copy |
| `run_Linux.stdout.txt`, `run_BGL.stdout.txt` | standard output of the two runs (`run_<LogName>.log` on the execution host, renamed here so they are not mistaken for input logs; the final line is the shell's wall time) |
| `output_manifest.txt` | size and SHA-256 of every archive the runs wrote (`output/Linux/chunk_0.tar.xz`, `output/BGL/chunk_0..47.tar.xz`); the archives themselves are not included |

## Result

| File | whole-file CLI ratio (`experiment_results.csv`) | summed chunk bytes (`output_manifest.txt`) | block adapter archive bytes / ratio (`results/final/anonymous_results.json`) | DeLog paper, Table 5 |
|---|---:|---:|---:|---:|
| Linux | 27.598 | 85,140 (1 chunk) | 85,140 / 27.598 | 30.63 |
| BGL | 40.327 | 18,428,912 (48 chunks) | 18,428,912 / 40.327 | 45.68 |

The whole-file run writes exactly the bytes our block adapter counts (27.60x on Linux, 40.33x on BGL), so the
difference from the published ratios is not caused by our adaptation. All tables use the measured values; the
published ratios of all sixteen files are listed separately in the supplement (`analysis/r76/r76_tables.py`, table
"DeLog: measured on our inputs versus published"). This run checks size only; byte-exact restoration of DeLog archives
is checked by the block adapter's archive-only decode (`results/final/`).
