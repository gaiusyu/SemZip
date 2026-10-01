# R76 baseline track: Denum (ASE 2024), independent 100k-line blocks

> **This copy.** The adapters and all analysis scripts (`prove_lossy.py`, `check_enconly_smoke.py`, `equiv_check.py`,
> `diff_roundtrip.py`, `summarize_smoke.py`, `native_official.sh`) are included with their outputs (`proofs/SUMMARY.json`,
> the smoke checks). `src/` (the upstream Denum source), `bin/`, `pydeps/` and the logs are not (see
> `../../CODE_PROVENANCE.md`).

Track dir: `r76_additional_20260929/baselines/denum/`, working on 2026-09-29.
Design: `../../DEV_DESIGN_R76_zh.md`, section A. Status: **BLOCKED. No full run launched.**
Denum's official pipeline cannot restore the input byte for byte. The archive format loses
information for all 16 datasets, proven on block 0 of each. Under section A of the design,
Denum goes in the table as **"不可无损验证"** (no lossless verification possible). It stays
out of the lossless ratio comparison.

## Source and build

- Repository: the Denum repository cited in the paper (`DENUM_REPO_URL` in `external/upstreams.conf`), pinned commit
  `a3a697564e378643461e27ab92ea44b098424c35` (2025-04-15, "Update README.md"). Cloned on a workstation
  and copied to `src/Denum/` (`src/COMMIT_SHA`). File SHA-256 values, identical to the clone:
  - `denum_compress.cpp` b23a6ebf…72a2db
  - `Denum_python_package/decompress.py` c3becae2…7bc5c
  - `Denum_python_package/Denum_simplel.py` 8807c1cf…dde1
  - `lossy_check.py` 5bec5a61…bb48
- Build, using the command from the README exactly:
  `g++ -O3 -std=c++17 -o bin/denum_compress denum_compress.cpp -lboost_iostreams -lpthread -lpcre2-8`
  - Compiler: g++ 10.2.1 (Debian 11). Libraries: libpcre2-dev 10.36, libboost-iostreams 1.74.
  - `bin/denum_compress` sha256 `1f2a9406a9f68d6b8b58d69603182cbee8d4a2598366f7e17b7cec0869c885e8`.
  - A second build produced the same hash.
- Python decoder dependencies:
  - `pyppmd 1.2.0` installed with pip into `pydeps/` (used only on the PYTHONPATH of the decoder process).
  - `regex 2024.11.6` and `pandas 2.3.2` come from the system Python 3.9.2.
  - The decoder never calls pyppmd, but `Denum_simplel.py` imports it.

## What the repository actually provides

- `denum_compress.cpp` is a **compressor only**. There is no C++ decompressor.
  - CLI: `./denum_compress LOGNAME CHUNKSIZE MODE`. It reads `Logs/LOGNAME/LOGNAME.log` and
    splits it into CHUNKSIZE-line blocks.
  - Each block goes through `processLogBlock` in its own `std::async` thread.
  - Output per block: `output/LOGNAME/<i>/*` (tag streams `_X_.bin`, `LOGNAMEallids.bin`,
    `LOGNAMEallmapping.txt`, `LOGNAMEvariablesetids.bin`, `LOGNAMEvariablesetmapping.txt`), then
    `tar -cJf output/LOGNAME/compressed<i>.xz output/LOGNAME/<i>`.
  - The CLI's own "Achieved size" is the sum of the `compressed<i>.xz` files.
  - MODE 1 is the default Denum used for the paper's RQ1/RQ2.
  - LOGNAME selects a hand-written, dataset-specific entry in `regex_map`. An unknown name throws.
- `Denum_python_package/decompress.py` → `Denum_simplel.dataloader.decompress()` is the only
  official decoder.
  - It reads members with the C++ output names from `../decompress_output/LOGNAME/<chunkID>/` and
    loops over chunkID=1.. while `../Output/LOGNAME/<chunkID>` exists.
  - Nothing upstream extracts the C++ tar. `kernel_decompress` only targets the Python-format
    archive and is never called.
  - The README itself says that dataset tags other than Apache's IP "may encounter errors" and
    need user-written recovery functions.
- `Denum_python_package/compress.py` is a different, older Python compressor. It has generic
  rules, a different layout, and a hard-coded absolute input path. It is **not** the
  paper's C++ Denum, so it is not used.
- `lossy_check.py` deletes every space and newline before it compares lines. Its "Lossless!"
  verdict is therefore not byte exactness.

## Adapter (`codec_baseline_denum.py`)

The adapter is a copy of `r68_external_20260923/codec_baseline.py`. The protocol, block
scanner, inventory, archive-only decode, per-block and full SHA audit, and result JSON are all
unchanged. The only addition is codec `denum` plus `--denum-dir`.

- **Encode, per 100k-line block, in a fresh process and directory:**
  1. Stage the block as `Logs/DATASET/DATASET.log` and create an empty `output/`. The CLI's own
     mkdir is not recursive.
  2. Run `bin/denum_compress DATASET 100000 1`, exactly the README invocation, including the
     official dataset name.
  3. Require stdout to contain `Block 0 directory successfully compressed`, and require the output
     to be exactly `{0/, compressed0.xz}`.
  4. Move `compressed0.xz` byte for byte to `archives/block_NNNNNN.tar.xz`.
  - Counted bytes are the whole native archive. The decoder needs nothing else.
- **Known upstream defect:** for input whose line count is an exact multiple of 100000, `main()`
  asks for the size of a nonexistent `compressed<block_index>.xz` in its summary loop.
  - `std::filesystem` then throws and the process exits with SIGABRT (exit -6). This happens
    after every block's `tar` has returned and been reported.
  - Every full 100k block hits this. The adapter accepts only the exact signature: SIGABRT, plus
    that stderr message for `compressed1.xz`, plus exactly one success line.
  - Each acceptance writes `diagnostics/<block>/encode.adapter_note`. Core dumps are disabled for
    the child. Any other nonzero exit is a failure.
- **Decode, per block, from the archive only:**
  1. `tar -xJf` the block archive and check that its members are exactly `output/DATASET/0/*`.
  2. Move the members to `dec/decompress_output/DATASET/1/` and create an empty
     `dec/Output/DATASET/1/`.
  3. Copy the unmodified `decompress.py` and `Denum_simplel.py`, then run
     `python3 decompress.py DATASET` in `dec/Denum_python_package/`.
  4. `decompress_output/DATASET/1/DecompressedDATASET.log` is that block's output.
  - The harness concatenates the blocks in order and checks the full and per-block SHA-256.

## Commands run (execution host, PYTHONHASHSEED=0)

```
# smoke (final): 5 datasets, full files, workers 2
python3 codec_baseline_denum.py --input-dir <root>/data/loghub1/raw \
  --datasets Proxifier Linux Apache Zookeeper HealthApp --output <track>/smoke_v2 \
  --codecs denum --denum-dir <track> --workers 2 --trials 1 --keep-decoded
python3 summarize_smoke.py smoke_v2 smoke_v2/SUMMARY.json
# adapter equivalence: native whole-file CLI vs adapter blocks (member-level)
python3 equiv_check.py DS <raw DS.log> <track>/smoke_v2/DS/denum/trial_001/attempt_001 <track>/equivalence/DS <track>/bin/denum_compress
# format-loss proof on block 0 (first 100000 LF lines; SHA equals r68 input_DS.json block 0) of all 16 datasets
python3 prove_lossy.py DS <block0 file> <track>/proofs/DS <track>/bin/denum_compress
```

## Results

### Smoke test: all 5 FAIL the byte-exact round trip

The ratio below is compression only and **unverified**. It is not a lossless result.

| dataset | blocks | raw bytes | archive bytes | ratio (unverified) | decoded bytes | exact content lines |
|---|---|---|---|---|---|---|
| Proxifier | 1 | 2,541,814 | 92,944 | 27.35 | 2,467,405 | 0 / 21,329 |
| Linux | 1 | 2,349,686 | 76,152 | 30.86 | 2,339,840 | 6,728 / 25,567 |
| Apache | 1 | 5,135,876 | 87,564 | 58.65 | 4,874,293 | 4,478 / 56,482 |
| Zookeeper | 1 | 10,426,438 | 77,012 | 135.39 | 9,971,286 | 0 / 74,380 |
| HealthApp | 3 | 23,529,930 | 518,140 | 45.41 | 22,701,005 | 0 / 253,395 |

HealthApp blocks 0 and 1 have exactly 100k lines and hit the accepted upstream SIGABRT. Block 2
exits 0. Per-dataset details are in `smoke_v2/SUMMARY.json` and `smoke_v2/<DS>/.../result.json`.
The decoded outputs are kept as `roundtrip.owned.log`.

### Adapter equivalence (`equivalence/<DS>/equivalence.json`)

- The native whole-file run is `./denum_compress DS 100000 1` on the full raw file.
- For all 5 datasets and all 7 blocks, the tar members match the adapter's per-block archives:
  identical names, identical contents (SHA-256), identical order.
- Archive byte counts differed between the runs we compared by 4–1,428 B per block (at most
  0.73%, on HealthApp block 0). The members were identical in those cases.
- The uncompressed tar streams differ only in the header mtime/checksum fields (480 bytes).
  LZMA2's parsing then cascades that small change into a different compressed size.
- So the pipeline's archive bytes are not deterministic, but its content is.

### The archive format is lossy (`proofs/SUMMARY.json`, 16/16 LOSSY_FORMAT_PROVEN)

- A dataset rule in `regex_map` replaces its whole match with one tag. It stores
  `std::stoll(all digits of the match concatenated)`.
- Group widths, separator positions, leading zeros and optional groups are therefore not stored.
- For each dataset:
  1. Take block 0 and change one line by moving a digit across a group boundary inside one
     dataset-rule match. Call the result B'. It has the same length but different bytes.
  2. Run the unmodified binary on B and on B'.
  3. Result: **all archive members are byte-identical** in all 16 cases.
- Two different inputs therefore give the same archive content. No decoder, official or
  rewritten, can restore both.
- Examples, original → B':
  - Linux `127.0.0.1` → `12.70.0.1` (the IP rule);
  - HPC `5.5.226.0` → `5.52.26.0`;
  - OpenSSH `173.234.31.186` → `17.3234.31.186`;
  - Proxifier `[10.30 16:49:06]` → `[1.030 16:49:06]`;
  - Apache `09 06:07:04` → `09 0:607:04`;
  - Zookeeper and Hadoop `2015-…` → `201-5…`;
  - Spark `18:14:40` → `1:814:40`;
  - Thunderbird `00:05:01` → `0:005:01`.

### Official decoder defects, observed in the smoke outputs

These are separate from the format loss above.

1. **Dataset tags come back as the bare concatenated integer.**
   - `[10.30 16:49:06]` → `[1030164906]`.
   - `2015-07-29 17:41:41,536` → `20150729174141536`.
   - `20171223-22:15:29:606` → `20171223-221529606`.
   - Apache `[Thu Jun 09 06:07:04 2005]` → `[Thu Jun 9060704 2005]`.
2. **Generic number tags lose their leading zeros,** even though the tag encodes the length.
   - `Jun  9 06:06:20` → `Jun  9 6:6:20` (Linux).
3. **3-digit numbers get the wrong inverse delta.** The encoder stores tag `<c>` raw, but the
   decoder applies an inverse delta to it.
   - `QuorumPeerConfig@334` → `@435` (Zookeeper, 2nd line).
4. **Every CR is dropped.**
   - The decoder reads members in text mode with universal newlines and `.strip()`s variables.
   - Proxifier, Zookeeper and HealthApp are CRLF files. The first lines of block 0 show CRLF in
     Android, Hadoop, HDFS, HPC, Mac, OpenSSH, Spark and Windows too.
5. **A LF is added to a final line that has none** (Apache: 56,482 vs 56,481 LFs).
6. **Empty lines are dropped** (`store_content_with_ids`: `if (line.empty()) continue;`). This
   comes from reading the code; the smoke data did not trigger it.

## Deviations and decisions

- Following the task rule and design section A, **no full 16-dataset run was launched**,
  because the round trip is not byte exact.
- A compression-only campaign is possible if an explicitly labelled
  "unverified size" column. It needs only the encoder and about 65 GB of input. The rough
  estimate is a few hours at 3 workers, based on about 3 MB/s per block process. It has not
  been run.
- The adapter keeps Denum's official dataset-specific `regex_map`, as the paper's RQ1 did, and
  as the r68 DeLog adapter keeps DeLog's.
- The adapter does not modify the algorithm. The only glue is the SIGABRT acceptance, tar
  extraction, and the path layout for the decoder.

## Other files in this directory

- `smoke/`: first smoke attempt, kept as history. HealthApp failed at encode with SIGABRT
  there, which is how the upstream summary-loop defect was found. The adapter was then changed
  and `smoke_v2/` is the result of record.
- `native_official.sh` and `native/Apache/`: an earlier exploratory whole-file native run with
  the official decoder on Apache. That decode was also not byte exact (4,874,293 vs 5,135,876 B).
  `equivalence/` supersedes it.
- `diff_roundtrip.py`, `summarize_smoke.py`, `equiv_check.py`, `prove_lossy.py`: the analysis
  scripts. Each has a docstring.
- Script SHA-256 values at this write-up: `codec_baseline_denum.py` de4daaf4…21ff3,
  `prove_lossy.py` 2631a9ee…4767a, `equiv_check.py` 3d7a5e88…89d5.

## Encode-only run: compression-only size, NOT a lossless result (2026-09-29/30)

The previous sections prove that Denum cannot restore its input, so Denum stays out of the
lossless comparison. The paper reports only Denum's **compression-only ratio**, labelled
**"不可无损验证" (not losslessly verifiable)**. This run produces that number.

### Script: `codec_baseline_denum_enconly.py`

- It is a copy of `codec_baseline_denum.py` (sha256 de4daaf4…21ff3, unchanged).
- sha256 of the new script at launch: `6c1bf68ebbc72e738d0550b37bc2190829663fd3ad045285dcd9aa907f521135`.
- Only additions: `--encode-only` and `--reference-inventory-dir`. Without `--encode-only` it
  behaves like the original. `--encode-only` requires `--codecs denum` and
  `--reference-inventory-dir`, and it rejects `--keep-decoded`.
- **The encode is the same code path, unchanged:** `encode_all` → `Codec.encode` →
  `denum_encode`.
  - Fresh process and fresh directory per 100k-line block.
  - Official `bin/denum_compress DATASET 100000 1`.
  - Same exact-signature SIGABRT acceptance, which writes `encode.adapter_note`.
  - The whole `compressed0.xz` is moved byte for byte to `archives/block_NNNNNN.tar.xz` and counted.
- **Decoding is skipped entirely.** No tar extraction, no Python decoder, no roundtrip file.
- **Input checks, all hard failures:**
  - The fresh full inventory (source identity, full SHA-256, every block's bytes, LF count,
    records and SHA-256) must equal `r68_external_20260923/first_pass/input_<D>.json`.
  - During the timed encode, each spooled block is hashed as it is written. That SHA-256 must
    equal the inventory block SHA before the block goes to the encoder. It is recorded as
    `encoded_input_sha256`.
  - Source identity is checked before and after the encode.
- **Per-dataset result** (`<D>/denum/trial_001/attempt_001/result.json`, `mode: encode-only`):
  - `status: ENCODE_ONLY_UNVERIFIED`, `lossless_verified: false`, `roundtrip: not-attempted`, and
    `unverified_reason`, which points to `proofs/SUMMARY.json`.
  - `raw_bytes`, `archive_bytes`, `compression_ratio` = raw/archive, and `block_count`.
  - `block_archive_bytes`, the per-block list.
  - `suffix_blocks` [1, N-1], `suffix_raw_bytes`, `suffix_archive_bytes` and
    `suffix_compression_ratio`. These are null when N = 1.
  - `encode_seconds`, the full-file split plus encode wall with 2 workers. It excludes the
    SHA-of-archive audit. `encode_MB_per_s`.
  - Per block: inventory fields, `archive_bytes`, `archive_sha256`, `block_encode_seconds`
    (process wall), and `encoded_input_sha256`.
- **Campaign files:**
  - `status.json` is `ENCODE_ONLY_COMPLETE` when every dataset is `ENCODE_ONLY_UNVERIFIED`.
    Otherwise it is `COMPLETE_WITH_FAILURES`.
  - The manifest protocol records `encode_only`, the reference directory and the SHA-256 of each
    reference inventory file.
  - `--resume` reuses an encode-only result only after re-checking every archive's size and
    SHA-256 and the block records.

### Smoke (`smoke_enconly/`, Proxifier + HealthApp, workers 2)

```
python3 codec_baseline_denum_enconly.py --input-dir <root>/data/loghub1/raw --datasets Proxifier HealthApp \
  --output <track>/smoke_enconly --codecs denum --denum-dir <track> --workers 2 --trials 1 \
  --encode-only --reference-inventory-dir <root>/r68_external_20260923/first_pass
python3 check_enconly_smoke.py smoke_enconly smoke_v2 smoke_enconly/SMOKE_CHECK.json Proxifier HealthApp
```

Both datasets came back `ENCODE_ONLY_UNVERIFIED`, and both inventories equal R68. Comparison
with `smoke_v2`, block by block (`smoke_enconly/SMOKE_CHECK.json`, `all_ok: true`):

| dataset/block | enc-only B | smoke_v2 B | Δ | rel | tar members (count, identical) | SIGABRT note |
|---|---|---|---|---|---|---|
| Proxifier/0 | 93,088 | 92,944 | +144 | 0.15% | 50, yes | no / no |
| HealthApp/0 | 195,876 | 194,736 | +1,140 | 0.59% | 103, yes | yes / yes |
| HealthApp/1 | 197,452 | 197,624 | −172 | 0.09% | 105, yes | yes / yes |
| HealthApp/2 | 125,724 | 125,780 | −56 | 0.04% | 100, yes | no / no |

- Every delta is within the documented tar-timestamp noise (≤ 0.73% per block).
- The member names, order and SHA-256 values are identical, so the encode is the same.
- Totals:
  - Proxifier: 93,088 B, ratio 27.31 (smoke_v2: 27.35).
  - HealthApp: 519,052 B, ratio 45.33 (smoke_v2: 45.41); suffix [1,2] ratio 43.90.
- A `--resume` rerun reused both results after verification and created no new attempt.

### Full run (`full_enconly/`, launched 2026-09-29 20:30 UTC)

```
cd <track> && AGNICE=10 ../../launch.sh logs/enconly_full.log python3 codec_baseline_denum_enconly.py \
  --input-dir <root>/data/loghub1/raw --datasets Linux Proxifier Apache Zookeeper Mac HealthApp HPC \
  Hadoop OpenStack OpenSSH Android BGL HDFS Spark Windows Thunderbird --output <track>/full_enconly \
  --codecs denum --denum-dir <track> --workers 2 --trials 1 --encode-only \
  --reference-inventory-dir <root>/r68_external_20260923/first_pass
```

- The job was detached with `setsid` (pid 731978, autogroup nice 10) and runs with 2 workers.
  Datasets run from smallest to largest raw size, so Thunderbird is last.
- Raw inputs are the resolved `data/loghub1/raw/<D>.log` paths. Before launch, all 16 source
  identities matched the R68 inventories.
- Log: `logs/enconly_full.log`. Results: `full_enconly/results.json` and the per-dataset
  `result.json` files.
- **How to report the numbers:** use them only as a compression-only column labelled
  "不可无损验证 / not losslessly verifiable". Never put them next to the byte-exact ratios
  without that label.
- Archive bytes carry up to ±0.73% per-block tar-mtime noise.
- The encode times come from a shared, loaded machine (load ~40 on 119 CPUs) and are indicative only.
