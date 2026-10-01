# SemZip Lean Mainline Audit

Date: 2026-07-07

Baseline:

```text
SEMZIP-WIDTHGUARD-OR20-IPV4PLAIN-20260707
```

This note separates the publishable SemZip mainline from historical research
branches currently mixed into the Python prototype.

## Current Code Size

```text
semzip_pure.py:
  lines: 12,383
  functions: 245
  classes: 4
  argparse add_argument calls: 38
  unique os.environ.get keys: 22

pare_dataset_extract.py:
  lines: 10,241
  functions: 295
  classes: 6
  argparse add_argument calls: 13
  unique os.environ.get keys: 64

run_blocks_pure.py:
  lines: 512
  functions: 18

pure_config.py:
  lines: 136
```

Interpretation:

```text
The current implementation is a research prototype containing many historical
branches. The final mainline should not require 10k+ lines in one file.
```

## Mainline That Should Remain

The current SemZip story only needs the following modules.

```text
1. Offline family sampler
   raw log -> LogBatcher-style families -> sampled examples

2. LLM function proposer
   family examples -> structured python_exec extraction functions

3. Verifier and repair
   compile generated code;
   validate regex boundaries;
   validate forward/inverse exact reconstruction;
   validate ordered function replay.

4. Replay plan
   accepted functions + placeholders + context metadata.

5. Online semantic replay
   apply replay-plan functions to raw logs;
   build transformed main stream;
   collect values_by_tag.

6. Residual fallback
   width guard;
   residual variable gate:
     distinct >= 30 OR avglen > 20;
   IPv4 route to ipv4_plain.

7. Deterministic codecs
   timestamp/numeric delta;
   ipv4_plain;
   string / string_mtf_rank;
   id/template mapping;
   tar/xz packaging.

8. Decode and SHA verification
   archive -> original raw log exactly.
```

## Historical Branches That Should Not Be In The Lean Mainline

These are useful for ablations or archived experiments, but should not stay in
the clean mainline unless explicitly promoted.

```text
program_bundle_mdl_admission
cegis_repair variants beyond the current verifier/repair path
oracle_subordinate_time_auto
placeholder_slot_fission
residual_xsignature_streams
residual_line_transducer
relation_streams
global_value_rescue
multi-profile racing / oracle selectors
line-bundle fallback experiments
residual all-stream extraction experiments
dataset-specific legacy fixed-op recognizers as main method
large groups of PARE_* env toggles used only for sweeps
```

## C++ Runtime Direction

The C++ runtime should be small and data-plane focused.

Already implemented:

```text
cpp_runtime/semzip_codecs.cpp
cpp_runtime/stream_writer.cpp
cpp_runtime/stream_writer_server.cpp
cpp_runtime/pcre2_match_dump.cpp
cpp_runtime/pcre2_replay_dump.cpp
```

Current validation:

```text
Apache replay plan:
  PCRE2 regex span parity: PASS
  PCRE2 replay dump transformed_text parity: PASS
  PCRE2 replay dump values_by_tag parity: PASS
```

Next safe integration step:

```text
Use PCRE2 replay only for functions that pass replay-dump parity.
Keep Python verifier and python_exec execution in the training/validation path.
```

## LLM-Generated C++ Boundary

Directly asking the LLM to generate production C++ compression code is not the
recommended mainline.

Reason:

```text
The compressor must be strictly lossless.
Free-form generated C++ increases the trusted computing base:
  memory safety issues;
  undefined behavior;
  compiler/platform differences;
  hard-to-audit inverse logic;
  harder verifier sandboxing.
```

Recommended design:

```text
LLM generates:
  reversible extraction/transformation functions;
  regex boundary contracts;
  context grouping hints;
  compact structured IR / python_exec sketches.

Verifier freezes:
  accepted regex;
  placeholder/replacement contract;
  forward/inverse behavior;
  replay plan;
  stream codec metadata.

C++ executes:
  PCRE2 replay for parity-passing regex plans;
  deterministic codecs;
  archive data-plane writing;
  later, possibly a compiled restricted IR interpreter.
```

This gives a cleaner paper story:

```text
The LLM supplies semantic source-separation knowledge.
The system compiles verified knowledge into a deterministic high-speed runtime.
```

Future C++ generation can still be explored as an ablation:

```text
LLM -> restricted IR -> generated C++ from trusted templates
```

but not:

```text
LLM -> arbitrary C++ -> production compressor
```

## Proposed Final Code Shape

A clean implementation can be organized as:

```text
semzip_train.py          offline sampling, API calls, verifier, replay_plan
semzip_compress.py       online replay, residual fallback, archive writer
semzip_decompress.py     restore archive and verify
semzip_functions.py      python_exec compiler/verifier/repair
semzip_replay.py         Python fallback replay + C++ PCRE2 bridge
semzip_codecs.py         Python codec wrappers
cpp_runtime/             C++ codecs and PCRE2 replay
```

Expected size after cleanup:

```text
Python mainline:
  about 3k-5k lines

C++ runtime:
  about 1k-2k lines initially
```

The exact size depends on how much verifier/repair logic stays in Python, but
there is no technical reason for the final mainline to keep the current 20k+
combined Python lines.
