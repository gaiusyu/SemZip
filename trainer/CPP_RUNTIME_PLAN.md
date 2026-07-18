# SemZip C++ Runtime Plan

Goal:

```text
Keep LLM training and verifier logic in Python first.
Move the online compression/decompression data plane into C++ in stages.
Every stage must be compared against Python artifacts and preserve byte-for-byte restore.
```

## Current Python Hotspots From Apache Smoke

Input:

```text
Apache.log, 56,482 lines, 5,135,876 bytes
archive = 67,836 bytes
ratio = 75.7102x
online = 2.8287s wall / 1.8156 MB/s
encode_seconds in block summary = 2.6541s
SHA = PASS
```

Top timed online operations:

```text
0.5605s  stream_save.save_extract_streams_total
0.4438s  write_tag_stream.open_function
0.2921s  residual_variable.context_pass_total
0.2272s  family_scan.template_key
0.2243s  residual_variable.context.discover_alpha_contexts
0.0821s  stream_save.select_auto_stream_spec
0.0763s  family_scan.match_family
0.0661s  archive.create_tar_xz
```

Interpretation:

```text
The first C++ target should not be LZMA.  The larger bottleneck is the data
plane around line scanning, family/template key generation, semantic replay,
residual grouping, and stream codec writing.
```

## Stage 0: Codec Parity Library

Implement stable codec primitives in C++ and test them against Python:

```text
varint
zigzag
delta varint stream
IPv4 plain stream
string MTF rank
length-prefixed string stream
```

This stage does not replace Python compression yet.

## Stage 1: Artifact-Compatible Stream Codec Tool

Build a C++ command that can read stream files from a Python archive workdir and
round-trip selected streams:

```text
IP.ipv4.bin
*.delta.bin
*.rank.bin + *.literal.bin
*.strings.bin
```

The success criterion is exact decoded value parity with Python for selected
metadata entries.

## Stage 2: Online Data-Plane Replay

Use Python offline training to produce `replay_plan.json`.  C++ reads:

```text
raw block log
replay_plan.json
compact metadata contract
```

C++ then performs:

```text
family/template key generation
function matching for replayable regex/function specs
semantic placeholder replacement
residual variable grouping
numeric residual grouping
stream codec writing
main template/id mapping writing
```

Python still owns:

```text
LLM calls
repair
verifier
experiment orchestration
final cross-checks during development
```

## Stage 3: C++ Archive Writer And Decoder

After stream parity works, C++ writes the archive payload directly and provides a
C++ decoder for the same archive format.  The final acceptance criterion is:

```text
raw .log -> C++ archive -> C++ decode -> byte-identical raw .log
```

## Non-Negotiable Validation

Every C++ stage must report:

```text
dataset
raw bytes
archive bytes if applicable
SHA status
Python baseline artifact used
which modules are C++ and which remain Python
```

No SHA FAIL result can be treated as a compression result.
