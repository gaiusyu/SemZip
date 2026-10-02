# External baseline source and adapter setup

The exact label is **LogLite-BL-wide+tail+reservefix**. DeLog is the pinned official CLI with an independent-block driver. `manifest.json` records public source versions, source hashes, fixed flags and the recorded executable hashes. This directory was assembled from existing evidence; no new benchmark or binary rebuild was performed while packaging it.

## Shared input and archive contract

Read raw files as bytes. Split after every 100,000 physical LF-delimited records and preserve the original final tail, CRLF, NUL and other bytes. Start a fresh codec CLI process and fresh working directory for each block. Store complete native archives in block order. Include all decoder-required payload and wrapper bytes. Reconstruct every block and full file from archives alone and require exact SHA equality; retain failures. Never strip whitespace, normalize Unicode, drop rows, use cross-block dictionaries, or replace an unsuccessful input with a cleaned version.

The exact measured `codec_baseline.py` is included here, adjacent to its unchanged `loglite_adapter.py` import; its SHA matches the recorded first-pass driver. Each external CLI has one internal worker. The campaign may schedule up to four original blocks concurrently under the fixed shared protocol. After installing the generic CLIs and building the two pinned upstream binaries below, run from the artifact root:

```sh
python3 -B external/codec_baseline.py --input Linux=raw/Linux.log \
  --output ../external-linux-first-pass --workers 4 --trials 1 \
  --codecs gzip6 xz6 zstd3 delog loglite \
  --delog-dir external/vendor/DeLog \
  --loglite-executable external/vendor/LogLite-B/build/loglite-B-wide
```

The output directory must be new; do not use the optional failure-retry CLI to replace paper outcomes. `CODEC_BASELINES.md` documents this original driver and `smoke_codec_baseline.py` provides its independent synthetic acceptance checks. The driver produces first-pass diagnostic phase durations. Its `--trials` option alone does **not** recreate the paper's serialized outer-process V3 experiment. The V3 controller/source and its original directory-layout requirements are retained in `validation_controls/formal_inventory/`; final V3 observations are separately included in the numeric result package. The original first-pass driver retains its strict file-metadata auditor; V3's documented restored-output mtime correction belongs to its outer controller and does not silently modify these frozen source bytes.


## DeLog: public source and build

Fixed official source and README: the DeLog repository cited in the paper, at commit
`64a074f6b6559fbfcd809f201fc3540442151749`. Set `DELOG_REPO_URL` in `external/upstreams.conf` (or export it) before
running the commands below; `bash external/fetch_baselines.sh delog` performs the same clone and checkout.

Prerequisites include a C++17 compiler, PCRE2 headers/library, libarchive headers/library and POSIX threads. These are the upstream README commands. They document how to build the pinned source; they are not presented as a newly executed packaging-time build log.

From the artifact root:

```sh
mkdir -p external/upstream external/vendor/DeLog
git clone --no-checkout "$DELOG_REPO_URL" external/upstream/DeLog
git -C external/upstream/DeLog archive 64a074f6b6559fbfcd809f201fc3540442151749 compressor.cpp decompressor.cpp BS_thread_pool.hpp README.md | tar -x -C external/vendor/DeLog
cd external/vendor/DeLog
g++ -std=c++17 -O3 -o Delog_compress compressor.cpp -lpcre2-8 -lstdc++fs -pthread -larchive
g++ -std=c++17 -O2 -o decompress decompressor.cpp -lstdc++fs -pthread -larchive
```

Compare the exported source hashes with `manifest.json`. No source patch is applied. Keep the official dataset recognizers and pass the actual dataset name; this baseline has existing supplied rules.

For each independently spooled original block, set the working directory to a new temporary directory containing `Logs/DATASET/DATASET.log`. Invoke the absolute binary path with:

```text
Delog_compress DATASET text 100000 1 0 lzma normal
```

The only expected archive is `output/DATASET/chunk_0.tar.xz`. Retain it byte-for-byte. To restore that archived block in a separate workspace:

```text
decompress ARCHIVE_DIRECTORY OUTPUT_FILE 1
```

The second argument is an output filename. An `Error processing chunk` diagnostic is a failure even if the native exit code is zero. No DeLog tail correction or dataset-specific repair is added; byte mismatches remain failures. Standalone DeLog is separate from the DeLog-derived residual backend inside SemZip.

## LogLite-BL-wide+tail+reservefix

Fixed official source and build/CLI README: the LogLite repository cited in the paper, at commit
`68f851ef673ac6fa45f26513df08613151624bd2` (`LOGLITE_REPO_URL` in `external/upstreams.conf`).

The public repository contains case-distinct `LogLite-B` (bytes) and `LogLite-b` (bits). Export only the byte subtree without checking out the entire tree, so a case-insensitive filesystem cannot merge the two. The recorded host used GCC 10.2.1, Boost headers and an x86 CPU supporting AVX-512F. The build includes `-march=native` and `-mavx512f`; inspect `lscpu` before selecting a compatible reproduction host. This pinned build is not an ARM or generic-x86 portability configuration.

From the artifact root:

```sh
mkdir -p external/upstream external/vendor/LogLite-B
git clone --no-checkout "$LOGLITE_REPO_URL" external/upstream/LogLite
git -C external/upstream/LogLite archive 68f851ef673ac6fa45f26513df08613151624bd2:LogLite-B | tar -x -C external/vendor/LogLite-B
cd external/vendor/LogLite-B
patch -p1 < ../../loglite_wide_reserve.patch
mkdir -p build
g++ -Ofast -march=native -fdiagnostics-color=always -g ./src/compress/*.cc ./src/common/*.cc ./src/tools/*.cc -I ./src -o ./build/loglite-B-wide -mavx512f
```

The manifest lists all ten upstream and patched source hashes. Only `src/common/constants.h` changes:

- `HEAD_BIT_LEN`: 8 → 16 and `ORIGINAL_LENGTH_COUNT`: 15 → 23, exactly the large-input alternatives already commented in upstream source. One setting was fixed before the raw census and is used for all datasets.
- `Reserved_Memory`: 33 GiB → 0. The original eager reservation fails a small input under an 8 GiB address-space cap; this resource change allows the output string to grow with its actual data. It changes no data-dependent codec rule.

The adapter then runs the real upstream encoder and decoder, followed by the counted XZ6 wrapper. From the artifact root, for one already-spooled original block:

```sh
python3 external/loglite_adapter.py encode BLOCK.bin BLOCK.archive --executable external/vendor/LogLite-B/build/loglite-B-wide
python3 external/loglite_adapter.py decode BLOCK.archive RESTORED.bin --executable external/vendor/LogLite-B/build/loglite-B-wide
```

The archive starts with one byte: `0` means the original block lacked final LF, `1` means it ended with LF, and `2` means empty input. Nonempty payload is the exact official LogLite-B stream compressed with `xz -6 -T1 --check=crc64`. Since upstream appends LF to its final decoded record, flag 0 removes only that appended final byte. Flag 2 stores no payload and avoids the upstream empty-bitset underflow. All flags count toward compressed size. The wrapper never reads original input while decoding.

This adaptation **does not make arbitrary-byte correctness a theorem**. The retained `known_failure_nul` fixture contains `abcXdef\nabc\0def\n`; upstream and the wrapper decode its second NUL as `X`. It is a known failure, preserved without repair or raw fallback. The wide preflight passed 13 of 14 cases, including the former field-width overflows, and still failed this NUL case. The real-byte checks remain mandatory for every research dataset. Upstream `--test` executes both directions but its printed success/rate is not a byte-equivalence certificate and is not used as the study's timing measure.

## Other methods

The manifest records the checked public availability of LogFold, LogPrism, LogReducer, LogShrink, Denum, LogZip and CLP. LogPrism was later published as LogNexus (ISSTA 2026) with an artifact on Zenodo; it is fetched by `fetch_baselines.sh lognexus` and measured by `evidence/r76/baselines/lognexus/` (see its README). Availability limitations are not measurements and their paper-reported results must not enter a same-host measured comparison. Generic CLI profiles are fixed `gzip -6 -n`, `xz -6 -T1`, and `zstd -3 -T1`; do not choose levels by dataset outcomes.
