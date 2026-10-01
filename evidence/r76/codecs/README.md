# R76 track `codecs/`: high-effort general-purpose codecs + DeLog-generic

> **This copy.** Included: the harness, `summarize.py`, `check_generic_tags.py`, `full_20260929_launch.json`, the DeLog-generic
> patch `delog/generic_vs_official.compressor.diff` (DeLog-generic = official DeLog at the pinned commit with this patch
> applied to `compressor.cpp`) with its verification record `delog/VERIFY_generic_vs_official.txt`, and every per-trial
> record. Not included: `tools/` (the zstd 1.5.6 release tree and binary), the DeLog sources and binaries in
> `delog/official_rebuild/` and `delog/generic/`, `logs/`, the archives, and the aggregate `full_20260929/results.json`
> (an aggregate of the included per-trial `result.json` files). See `../CODE_PROVENANCE.md`.

Pre-registered design: `../DEV_DESIGN_R76_zh.md`, section A (外部基线): "DeLog-generic" and
"通用压缩器高压缩档". Nothing in this directory reads or writes outside `codecs/` except
read-only use of `data/loghub1/raw/*.log` and `r68_external_20260923/` (harness copy, DeLog
sources/binaries, first-pass inventories for cross-checking).

## Layout

| path | what |
|---|---|
| `codec_baseline_r76.py` | harness (copy of R68 `codec_baseline.py` + R76 codecs), as-run sha256 not listed (machine-specific path replaced in this copy) |
| `r68_codec_baseline.py.orig` | untouched copy of R68 harness (sha256 `0b0c15f6f33ce2180201f11e07606373417dd78087aef2f5a8125075e757c26d`, = R68 first-pass `script_sha256`) |
| `r68_to_r76_harness.diff` | `diff r68_codec_baseline.py.orig codec_baseline_r76.py` |
| `tools/` | zstd 1.5.6 release tarball, checksum, extracted tree, build log, binary |
| `delog/official_rebuild/` | official DeLog sources (from R68 artifacts) rebuilt here as a control |
| `delog/generic/` | DeLog-generic sources (empty `regex_map`) + binaries used by the run |
| `delog/generic_vs_official.compressor.diff`, `delog/VERIFY_generic_vs_official.txt` | source diff and verification record |
| `smoke/` | final smoke (4 datasets x 6 codecs, all PASS), `smoke/summary.json` |
| `smoke_v1_fsrace/`, `logs/smoke_v1_fsrace.log` | first smoke; 1 false audit failure, kept as evidence (see Deviations) |
| `full_20260929/`, `logs/full_20260929.log`, `full_20260929_launch.json` | the full 16-dataset run |
| `summarize.py` | read-only summary: full ratio, suffix ratio (blocks >= 1), dict bytes, SHA flags, inventory cross-check vs R68 |
| `check_generic_tags.py` | read-only check that no DeLog-generic archive contains a dataset-regex tag |

## 1. zstd 1.5.6 (the execution host has no zstd CLI)

- Downloaded from the official release:
  `https://github.com/facebook/zstd/releases/download/v1.5.6/zstd-1.5.6.tar.gz` and its
  `zstd-1.5.6.tar.gz.sha256`. SHA-256 `8c29e06cf42aacc1eafc4077ae2ec6c6fcb96a626157e0593d5e82a34fd403c1`
  equals the published checksum (`tools/official.sha256`), re-verified on the execution host.
- Built with the release Makefile (`make`, log `tools/zstd_build.log`; gcc 10.2.1). The extracted
  tree has zero source differences from a fresh extraction of the tarball (only build products
  added). Binary `tools/zstd-1.5.6/programs/zstd`, `zstd --version` =
  `*** Zstandard CLI (64-bit) v1.5.6, by Yann Collet ***`, sha256
  `77a325b22dc395a39a3f71324aa92e62a6303d0c6ce63d3a37c1e68cc14b85e2`. The binary embeds its
  absolute source path (assert strings), so a rebuild in another directory has a different hash;
  the binary hash above is the identity recorded in every result's `codec_spec`.
- xz is the system `/usr/bin/xz` (XZ Utils 5.2.5, sha256 recorded in `codec_spec`), same as R68.

## 2. Harness changes vs R68 (`r68_to_r76_harness.diff`)

Unchanged: exact 100,000-LF-record blocks (constant, no CLI override), one fresh CLI process per
block, all native archive bytes counted, archive-only decode, per-block + full-file SHA-256 audit,
input inventory, resume/verification logic, timing scopes. Added:

- `SCHEMA = semzip.external-cli.r76.v1`.
- Codecs (fixed flags, never chosen per dataset):
  - `xz9e`: `xz -9e -T1 -c -- BLOCK`; decode `xz -d -T1 -c`.
  - `zstd19`: `zstd -19 -T1 -q -c -- BLOCK`; decode `zstd -d -T1 -q -c` (R68 already defined it, never ran it).
  - `zstd22long`: `zstd --ultra -22 --long=27 -T1 -q -c -- BLOCK`; decode `zstd -d --long=27 -T1 -q -c`.
  - `zstd19dict`: see below.
  - `delog_generic`: the R68 DeLog adapter, unchanged, pointed at `delog/generic/` binaries
    (`Delog_compress DATASET text 100000 1 0 lzma normal`; decode `decompress ARCHIVEDIR OUTPUT 1`).
- `--exe zstd=PATH` (explicit executable instead of `$PATH` lookup) and `--delog-generic-dir`.
- `settle_file()` before the restored-file audit (outside every timer), see Deviations.

### zstd19dict protocol

Pre-registered: dictionary trained once per file on block 0 only. In `encode_all`, when block 0
(always the first block closed by the streaming splitter, before any block is submitted) is
spooled, `Codec.train_dictionary` splits it into exact byte samples of 1,000 LF records (last
sample = remainder, unterminated tail kept as-is) and runs
`zstd --train -T1 sample_00000 ... -o archives/dictionary.zdict` with zstd's default dictionary
size (no `--maxdict`; 112,640 B observed). Training time is inside `encode_seconds` and also
reported as `dictionary_train_seconds`. Every block (including block 0) is then compressed with
`zstd -19 -T1 -q -c -D archives/dictionary.zdict`. The dictionary file is part of the archive set:
`archive_bytes = block_archive_bytes + dictionary_bytes` (dictionary counted once per file;
`dictionary_bytes`, `dictionary_sha256` recorded separately). Decode uses only the archive set:
`zstd -d -T1 -q -c -D archives/dictionary.zdict`. This is the one declared cross-block state.

## 3. DeLog-generic

- Sources copied from `r68_external_20260923/artifacts/DeLog/` (official DeLog commit
  `64a074f6b6559fbfcd809f201fc3540442151749` per R68 DESIGN.md); `sha256sum -c SHA256SUMS` OK.
- `delog/generic/compressor.cpp`: the 16 `regex_map["<Dataset>"] = ...` lines (official lines
  98-113) are replaced by one comment line; the map stays declared and empty, so
  `PatternRecognizer` compiles no regex for any dataset name (stderr prints the official
  "No predefined patterns for logname" warning). `decompressor.cpp` and `BS_thread_pool.hpp` are
  byte-identical to official. Diff: `delog/generic_vs_official.compressor.diff` (-16/+1 lines).
- Built in both dirs with the official README commands:
  `g++ -std=c++17 -O3 -o Delog_compress compressor.cpp -lpcre2-8 -lstdc++fs -pthread -larchive`
  and `g++ -std=c++17 -O2 -o decompress decompressor.cpp -lstdc++fs -pthread -larchive`
  (g++ 10.2.1, libpcre2 10.36, libarchive 3.4.3).
- The unmodified rebuild is bit-identical to R68's official binaries (Delog_compress
  `f4c62424...1739`, decompress `b722179d...0831`), so the generic encoder
  (`bc357a51b83d0ea03310b0b0332f67bb989ccda7f3336879a9c9552491713a8a`) differs from official DeLog
  only by the regex-map source change; the generic decoder IS the official decoder binary.
- Settings identical to R68 DeLog (same adapter code path, same arguments, the real dataset name
  as `logname`). With an empty map the name only labels the internal template files; the
  decompressor's name-keyed handlers are reached only for bare regex tags (`<T>`, `<I>`, `<A>`...),
  which the generic encoder never emits. Verified: `check_generic_tags.py smoke` -> 0 such tags
  in generic archives vs 8 in the official ones; Apache details in
  `delog/VERIFY_generic_vs_official.txt` (official via same adapter reproduces R68 first-pass
  Apache 85,932 B exactly; generic 111,504 B; both archive-only SHA PASS).

## 4. Smoke (`smoke/`, final harness, workers 2)

```
cd codecs && python3 -u codec_baseline_r76.py --input-dir ../../data/loghub1/raw \
  --datasets Linux Proxifier Apache HealthApp --output smoke \
  --codecs xz9e zstd19 zstd22long zstd19dict delog_generic delog \
  --exe zstd=tools/zstd-1.5.6/programs/zstd \
  --delog-dir ../../r68_external_20260923/artifacts/DeLog --delog-generic-dir delog/generic \
  --workers 2 --trials 1          # (absolute paths used in the actual run, via launch.sh)
```

24/24 PASS (archive-only decode, full + per-block SHA-256); input inventories identical to R68
first-pass inventories. `delog` = official R68 binaries as an adapter control (bytes equal to
R68 first pass on all 4 files).

| dataset | codec | status | raw B | archive B | ratio | dict B | SHA |
|---|---|---|---|---|---|---|---|
| Linux | xz9e | PASS | 2349686 | 104032 | 22.586 |  | PASS |
| Linux | zstd19 | PASS | 2349686 | 120226 | 19.544 |  | PASS |
| Linux | zstd22long | PASS | 2349686 | 120142 | 19.558 |  | PASS |
| Linux | zstd19dict | PASS | 2349686 | 222102 | 10.579 | 112640 | PASS |
| Linux | delog_generic | PASS | 2349686 | 97764 | 24.034 |  | PASS |
| Linux | delog | PASS | 2349686 | 85140 | 27.598 |  | PASS |
| Proxifier | xz9e | PASS | 2541814 | 112148 | 22.665 |  | PASS |
| Proxifier | zstd19 | PASS | 2541814 | 120664 | 21.065 |  | PASS |
| Proxifier | zstd22long | PASS | 2541814 | 120295 | 21.130 |  | PASS |
| Proxifier | zstd19dict | PASS | 2541814 | 228671 | 11.116 | 112640 | PASS |
| Proxifier | delog_generic | PASS | 2541814 | 99212 | 25.620 |  | PASS |
| Proxifier | delog | PASS | 2541814 | 84180 | 30.195 |  | PASS |
| Apache | xz9e | PASS | 5135876 | 157260 | 32.659 |  | PASS |
| Apache | zstd19 | PASS | 5135876 | 177700 | 28.902 |  | PASS |
| Apache | zstd22long | PASS | 5135876 | 177067 | 29.005 |  | PASS |
| Apache | zstd19dict | PASS | 5135876 | 290615 | 17.672 | 112640 | PASS |
| Apache | delog_generic | PASS | 5135876 | 111504 | 46.060 |  | PASS |
| Apache | delog | PASS | 5135876 | 85932 | 59.767 |  | PASS |
| HealthApp | xz9e | PASS | 23529930 | 1245396 | 18.894 |  | PASS |
| HealthApp | zstd19 | PASS | 23529930 | 1439889 | 16.341 |  | PASS |
| HealthApp | zstd22long | PASS | 23529930 | 1439203 | 16.349 |  | PASS |
| HealthApp | zstd19dict | PASS | 23529930 | 1553665 | 15.145 | 112640 | PASS |
| HealthApp | delog_generic | PASS | 23529930 | 505368 | 46.560 |  | PASS |
| HealthApp | delog | PASS | 23529930 | 457764 | 51.402 |  | PASS |

## 5. Full run (`full_20260929/`)

Launched 2026-09-29 02:51 UTC, one job, detached with
`AGNICE=5 ../launch.sh logs/full_20260929.log python3 -u codec_baseline_r76.py ...` (exact
command and pid in `full_20260929_launch.json`): all 16 datasets in ascending size order
(Linux Proxifier Apache Zookeeper Mac HealthApp HPC Hadoop OpenStack OpenSSH Android BGL HDFS
Spark Windows Thunderbird), codecs `xz9e zstd19 zstd22long zstd19dict delog_generic`,
`--workers 3 --trials 1`, default `--order rotate` (codec order rotates per dataset; bytes are
order-independent). DeLog official is not re-run (R68 `first_pass` has it with the same binaries).

Progress: `tail logs/full_20260929.log`; `cat full_20260929/status.json`;
`python3 summarize.py full_20260929` (works mid-run). After completion:
`python3 summarize.py full_20260929 --json full_20260929/summary.json` and
`python3 check_generic_tags.py full_20260929 --every 10`.
If any run FAILs, resume with the identical command plus `--resume --retry-failed` (appends a new
attempt, keeps the failed one); report the failure either way.

## Deviations / notes

- **workers = 3** (task limit for this shared container) vs 4 in R68/SemZip timing. Bytes and SHA
  results are unaffected; this run's encode/decode seconds are NOT the formal timing (track D).
- **settle_file()** (new, outside timers): the first smoke (`smoke_v1_fsrace/`) reported
  HealthApp/zstd22long as FAIL "source changed during inventory: .../roundtrip.owned.log". The
  restored file's SHA-256 equals the original (`d9d76ad2...d29c`, checked by hand); the
  `<MOUNT>` network file system applied the server-side (microsecond) mtime after close(), during
  the audit, tripping the identity guard. The harness now fsyncs the restored file and waits
  until its stat identity is stable over 2 s before the audit (`restored_settle_seconds`
  recorded). Guard itself unchanged. The whole smoke was re-run with the final harness (all PASS).
  The failed v1 attempt is retained, not deleted.
- zstd19dict: dictionary bytes (112,640 B) are charged once per file, which dominates on the
  1-block files (e.g. Linux 10.58 vs zstd19 19.54). `summarize.py` suffix ratio (blocks >= 1)
  includes the dictionary, because suffix blocks cannot be decoded without it.
- zstd22long: zstd shrinks the window to the block size for inputs < 128 MiB, so `--long=27` is
  a fixed pre-registered setting, not a claim that 128 MiB windows are used.

## Status check 2026-09-29 13:45 UTC (nothing relaunched)

- The full run (pid 1245963, launched 02:51:16 UTC) is alive and was not touched (no kill/renice/relaunch).
- `python3 summarize.py full_20260929`: 75/80 runs finished, all PASS (archive-only decode,
  full-file and per-block SHA-256 OK) for the first 15 datasets (Linux .. Windows) x 5 codecs.
  Input inventories of all 16 files match R68 (Thunderbird = 2,113 blocks, 31,788,301,041 B).
- Thunderbird is in progress: `xz9e` started 10:21 UTC, 874/2,113 block archives at 13:45 UTC
  (3.4-4.1 blocks/min under load ~38); then zstd19, zstd22long, zstd19dict, delog_generic
  (rotate order). Rough ETA for the whole run: 2026-09-30 (UTC).
- `check_generic_tags.py full_20260929 --every 10` on the 15 finished datasets (178 archives):
  0 bare regex tags.
- Re-verified: `diff -r r68_external_20260923/artifacts/DeLog delog/generic` shows only the
  16 regex_map lines (98-113 -> one comment) in compressor.cpp, plus the generic build.log and the
  rebuilt Delog_compress binary; decompressor.cpp, BS_thread_pool.hpp, README.md, SHA256SUMS and
  the decompress binary (b722179d...0831) are identical to official. official_rebuild binaries are
  bit-identical to R68's. Every result's `codec_spec` records encoder sha256 bc357a51...3a8a.
- Correction to section 2 / Deviations: the zstd19dict dictionary is 112,640 B on 14 of the 15
  finished files but 44,301 B on Windows (zstd --train with default settings returned a smaller
  dictionary for Windows block 0). Same fixed protocol; the actual size is what is charged.
- Note: the decoder still contains official dataset-name-keyed handlers (decompressor.cpp
  lines 743-944, keyed on the logname); they are reachable only via bare regex tags, which the tag
  check above shows the generic encoder never emits.
