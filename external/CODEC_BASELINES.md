# R68 independent CLI codec baseline harness

`codec_baseline.py` requires Python 3.9+, the requested CLI executables, and no
Python packages. It does **not** launch a formal campaign merely by importing it.

## Fixed protocol

- Exactly **100,000 original LF-delimited physical records per block**. The final
  original tail is retained. CRLF, NUL, CR without LF, and missing final LF are
  copied literally. This block limit has no CLI override.
- Each block uses a fresh independent CLI process. There is no shared dictionary,
  history, trained state, or cross-block deduplication.
- Main generic baselines: `gzip -6 -n`, `xz -6 -T1`, `zstd -3 -T1`.
  `gzip -n` removes source filename and timestamp fields from its native header.
  `zstd19` is an explicitly separate sensitivity baseline; never choose a level
  per dataset after observing results.
- Retain the complete native archive for every block and sum its actual file
  sizes, including all native headers/footers. JSON experiment ledgers, diagnostic
  logs, and Unix directory metadata are not part of a compressed log archive.
  Fixed-width sequential filenames define block order without side information.
- All encoders have at most four simultaneous block processes. Native xz/zstd
  threads are fixed to one. `--workers` is recorded and must match SemZip.
- Encoding streams the source in 1 MiB chunks, retaining at most `workers` raw
  blocks in owned temporary workspaces. Decompression also bounds pending block
  spools and materializes **one complete restored file**. Peak temporary disk is
  approximately one raw dataset plus at most `workers` original blocks; retained
  compressed archives are additional. Input memory is independent of line size.
- Full input SHA-256 and every original block SHA-256 are inventoried before
  timing. After decoding from archives alone, full and block hashes must match.
  Every retained archive also receives a SHA-256 certificate. A failure is a
  result, never a reason to silently change the input, parameters, or dataset.

## Timing scope

`encode_seconds`: wall time from beginning to read/split the original input until
all independent CLI invocations complete and all native archives are closed. It
includes input read/split/spool writes, process startup, compression, archive
writes, and disposal of completed raw block spools. Thread-pool construction and
campaign setup are excluded; worker creation happens lazily inside this window.

`decode_seconds`: a separate wall timer includes CLI process startup, archive
read/decompression into block files, ordered concatenation into a complete raw
output file, and disposal of the block spools. The complete output is closed
before the timer stops. No original input or extraction plan is accessible to the
codec decoder. The driver knows only archive paths and their sequential order.

Input inventory, SHA audits of archives/restored bytes, manifest writes, and
cleanup of the final full restored output are outside both timers. Audit time is
reported separately. I/O is buffered: there is no fsync, cache dropping, or claim
of cold-cache performance. Results use decimal MB/s. The source-inventory pass
warms the input cache; shared-host scheduling noise remains a limitation.

These full CLI times include each codec's startup. **Do not directly compare
them with historical R59 in-process SemZip timers**, which excluded interpreter
imports and used a different timing scope. Run a matched SemZip outer CLI timer
and use the same block parallelism and materialized reconstruction policy.
Standalone DeLog, like the other baselines, has no offline training. Report
SemZip's training cost separately from its matched online encode/decode time.

## DeLog adapter

`--codecs delog --delog-dir DIR` expects unmodified official executables
`DIR/Delog_compress` and `DIR/decompress`. SHA-256 of both binaries is recorded.
Each input block gets an isolated working directory containing
`Logs/DATASET/DATASET.log`. The exact compressor arguments are:

```
Delog_compress DATASET text 100000 1 0 lzma normal
```

The original `output/DATASET/chunk_0.tar.xz` is moved byte-for-byte into the saved
archive set. The decoder is called with its archivedir, **output filename**, and
one worker. An `Error processing chunk` diagnostic is a failure even if the
official binary exits zero. Byte mismatches are failures too. This is labeled an
**official CLI independent-block adapter**, not the official whole-file driver.
The main campaign must first validate native driver/adapter equivalence on actual
data. Dataset names activate the official implementation's supplied rules and
must not be replaced with synthetic names for convenience.

## Optional LogLite adapter

`--codecs loglite --loglite-executable PATH` imports the adjacent
`loglite_adapter.py`. Its documented terminal-LF flag plus XZ6 wrapper is counted
in full. The binary, adapter, and xz hashes are recorded. Each CLI process remains
block-local. This is a separately labeled adapted method; its wrapper and source
limitations must be reported, not silently called the original default system.

## Run and resume

Example (the development host paths belong in a shell command, not this file):

```sh
python3 codec_baseline.py --input-dir RAW_DIR --datasets Apache HPC OpenSSH \
  --output NEW_CAMPAIGN_DIRECTORY --codecs gzip6 xz6 zstd3 \
  --workers 4 --trials 3 --order rotate
```

Methods rotate deterministically by dataset and trial; all runs are sequential
at the dataset/method level. No method gets extra repetitions selected by result.
Timing repetitions are distinct independent attempts and preserve all archives.
`--keep-decoded` retains complete raw reconstructions; the default removes only
the successful attempt's own reconstructed file after its audit. Official CLI
stdout/stderr diagnostics remain, except LogLite which follows its adapter's
diagnostic convention.

An existing output directory is rejected unless `--resume` is explicit. Resume
requires identical protocol, script/binary hashes, ordered inputs, source file
identity, and full input SHA. It rehashes all completed archives and validates
their saved decoded SHA certificates before skipping. It does not silently
repeat failures. `--resume --retry-failed` appends a fresh numbered attempt and
retains every previous failed/incomplete attempt. `result.json` in each attempt
is authoritative; the campaign `results.json` is a latest-attempt summary.
An exclusive `RUNNING.lock` prevents competing writers. If the host dies, inspect
the recorded PID/host and manually remove a proven stale lock before resuming.

## Synthetic acceptance checks

```sh
python3 smoke_codec_baseline.py --output NEW_SMOKE_DIRECTORY
```

This runs gzip/xz and zstd if installed on 100001-record LF and unterminated-tail
fixtures, exact boundaries, CRLF/NUL, a >1 MiB single no-LF record, and an empty
file. It checks full restoration, LF block metadata against an independent
oracle, archive-byte sums, protected resume, and tampered-archive rejection.
It does not test an installed official DeLog or LogLite binary; those adapters
need their own real CLI smoke and native-driver equivalence check on the host.
