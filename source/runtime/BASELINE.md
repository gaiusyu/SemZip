# SEMZIP-WIDTHGUARD-OR20-IPV4PLAIN-20260707

This directory is the clean-reference workspace for the current SemZip baseline.
It starts from the working implementation and freezes the current algorithmic
choices before deeper code deletion or C++ runtime work.

## Baseline Contract

- Parent: `SEMZIP-FULLGROUPS-WIDTHGUARD-20260706`.
- Residual variable admission: `distinct >= 30 OR avglen > 20`.
- Numeric safety: width guard remains enabled for oversized numeric streams.
- IPv4 routing: accepted `python_exec` string streams that are reversibly
  encodable as plain IPv4 are stored with `ipv4_plain`.
- Benchmark policy: raw `.log`, 100k-line blocks, 8 workers, block restore,
  full-file SHA verification.

## Current Files

- `semzip_pure.py`: block compressor, LLM proposal/replay, verifier, archive writer.
- `pare_dataset_extract.py`: stream codec library, restore helpers, legacy codec paths.
- `run_blocks_pure.py`: multi-block/dataset benchmark runner.
- `run_semzip_widthguard_or20_ipv4plain.py`: clean baseline runner.
- `pure_args_defaults.json`: frozen default algorithm configuration.

## Cleanup Plan

1. Keep this directory as the Python reference implementation.
2. Remove historical command-line knobs from `semzip_pure.py` after each removal
   is validated by replay on Apache, OpenSSH, Zookeeper, and Android.
3. Split large modules into `llm_training.py`, `verifier.py`,
   `semantic_runtime.py`, `residual_runtime.py`, and `codecs.py`.
4. Profile online compression.  Move only the stable data-plane parts to C++:
   tokenization, function replay, residual grouping, varint/delta, IPv4,
   string-rank/dictionary, and archive writing.
5. Keep LLM training, cache management, repair, and experiment orchestration in
   Python unless profiling shows otherwise.

## Do Not Do

- Do not overwrite the frozen source directories.
- Do not add dataset-specific regexes.
- Do not report SHA-failing runs as compression results.
- Do not count offline LLM training inside online MB/s.
