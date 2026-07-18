# SemZip Function Codecs + DeLog Residual Backend

This directory is the frozen backend of the promoted
`SEMZIP-DELOG-ALLGROUP-OR20-ADMISSION-V6-20260718` paper campaign. Pass
`--delog-residual-allgroup-or20` to apply one evidence gate to every DeLog
residual group: the dynamic support floor and
`distinct >= 30 OR avglen > 20`. Unlike V4, pure numeric groups are not
unconditionally admitted. SemZip function codecs remain unchanged. The user
promoted this exact V6 policy on 2026-07-18; no algorithm code was changed by
that promotion.

The main candidate is `SEMZIP-DELOG-SEMANTIC-CODEC-V3-20260718`:

1. `semantic_codec_frontend.py` executes accepted replay-plan functions and
   verifies every concrete replacement byte-for-byte.
2. Function-created streams keep the existing SemZip codecs: numeric and
   timestamp programs use delta plus required layout, IPv4 uses `ipv4_plain`,
   context-conditioned numeric values use `open_context_delta`, and string
   values use `string_mtf_rank`.
3. Only the placeholder-bearing residual/main text is sent to DeLog. DeLog
   runs with an empty recognizer plan, so none of its dataset-specific regexes
   are active.
4. Each block package contains a DeLog residual chunk and a self-contained
   SemZip semantic archive. Decoding does not read the encoder replay plan or
   a cache.

Run the complete candidate on AutoDL:

```bash
python run_semantic_codec_experiment.py \
  --dataset HPC \
  --input /root/autodl-tmp/logcompose-exp/data/raw/HPC.log \
  --plan /path/to/HPC/replay_plan.json \
  --semzip-source /root/autodl-tmp/semzip_regex_anchor_guard_position_denum_20260717 \
  --result /path/to/result/HPC
```

Package size includes every DeLog chunk, every semantic side-stream archive,
and the semantic manifest. Offline function generation and decompression are
excluded from online MB/s; function replay and both archive writers are
included.

`SEMZIP-DELOG-FULL-PLAN-BACKEND-V2-20260718` remains as an ablation that sends
typed function records through DeLog instead of preserving SemZip codecs.

## Recognizer-Only Ablation

This controlled hybrid removes DeLog's dataset-specific `regex_map` and loads
SemZip `replay_plan.json` recognizers instead. DeLog's generic token grouping,
tag streams, numeric delta/dictionary storage, archive writer, and decoder are
unchanged.

For every regex match, the adapter resolves the captured group that makes the
plan's `replacement` byte-exact. If no captured group satisfies that contract,
the complete match is stored under a separate fallback tag. This runtime check
makes every accepted replacement reversible without a dataset rule. Numeric
plan streams containing leading zeroes are dictionary-coded to preserve width.

The V1 adapter executes the plan's regex, capture, and replacement contract.
It does not interpret arbitrary `program.code` Python forward/inverse logic;
the selected raw capture is passed to DeLog's deterministic downstream codec.
That limitation is explicit so this experiment isolates replacing DeLog's
recognizers rather than silently reintroducing fixed semantic operators.

Build:

```bash
bash build.sh
```

Run the V1 ablation from AutoDL with raw logs, 100k-line blocks, and eight workers:

```bash
python run_experiment.py \
  --dataset HPC \
  --input /root/autodl-tmp/logcompose-exp/data/raw/HPC.log \
  --plan /path/to/HPC/replay_plan.json \
  --result /path/to/result/HPC
```
