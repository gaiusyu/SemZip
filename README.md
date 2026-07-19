# SemZip

> **SIGMOD submission artifact notice.** Our paper exceeded the 12-page limit.
> During the rushed final shortening process, we inadvertently removed some
> supporting content, including parts of the ablation study, and submitted an
> incomplete repository snapshot. If we are invited to submit a major revision,
> we will restore the missing experiments and their supporting artifacts.

SemZip is a lossless log compressor that uses an LLM during an offline
discovery stage to synthesize reversible semantic extraction programs. The
verified programs are frozen into a replay plan; online compression then runs
without an LLM. Residual text is compressed by a regex-free DeLog backend with
group-level admission.

This repository freezes the paper configuration:

```text
SEMZIP-DELOG-ALLGROUP-OR20-ADMISSION-V6-20260718
```

The artifact reads raw logs, writes self-contained archives, restores every
block without an API or external cache, and verifies the complete restored
file with SHA-256.

> Dataset notice: SemZip experiments use the non-commercial Loghub datasets.
> Download them from their official sources and follow their licenses. No
> benchmark logs or API credentials are included in this repository.

## Table of Contents

1. [Artifact Overview](#1-artifact-overview)
2. [Getting Started](#2-getting-started)
3. [Cold-Start Compression](#3-cold-start-compression)
4. [Cache-Only Replay](#4-cache-only-replay)
5. [Lossless Verification](#5-lossless-verification)
6. [Reproducing the Evaluation](#6-reproducing-the-evaluation)
7. [Method Summary](#7-method-summary)
8. [Artifact Guarantees and Limitations](#8-artifact-guarantees-and-limitations)

## 1. Artifact Overview

The repository contains four main components:

| Path | Purpose |
| --- | --- |
| `trainer/` | Offline sampling, LLM proposal and repair, compilation, and byte-exact verification. |
| `runtime/` | Frozen semantic-function replay and semantic side-stream codecs. |
| `backend/` | C++ residual compressor/decompressor and the composite SemZip runner. |
| `run_main_batch.py` | Reproducible cold-start, ratio, speed, decode, SHA, and provenance campaign. |

The single paper configuration is [`configs/main.yaml`](configs/main.yaml).
The file uses JSON syntax, which is a valid YAML subset and avoids an extra
Python YAML dependency.

The main pipeline is deliberately split into two stages:

1. **Offline discovery.** SemZip samples representative log families, queries
   the LLM, repairs invalid proposals, compiles candidate programs, and keeps
   only programs that reconstruct every validation example byte-for-byte.
2. **Online compression.** SemZip freezes the replay plan, applies the verified
   programs, encodes semantic streams, and sends only residual text to the C++
   backend. Online compression makes zero API calls and performs no evolution.

## 2. Getting Started

### 2.1 Hardware and Software

Recommended environment:

- Linux with at least 8 CPU cores and 16 GB RAM.
- Python 3.10 or newer.
- GCC/G++ 9 or newer with C++17 support.
- PCRE2 and libarchive development packages.
- `xz-utils` for archive inspection.

On Ubuntu/Debian:

```bash
sudo apt-get update
sudo apt-get install -y g++ libpcre2-dev libarchive-dev xz-utils
```

The Python implementation uses the standard library and does not require a
package installation step.

Run the artifact tests from the repository root:

```bash
PYTHONPATH=trainer python3 -m unittest discover -s tests -v
```

### 2.2 Build the C++ Backend

```bash
bash backend/build.sh
```

The command creates:

```text
backend/Delog_plan_compress
backend/decompress
```

### 2.3 Prepare Datasets

Download Loghub 1.0 from <https://github.com/logpai/loghub>. Download Loghub
2.0 from <https://zenodo.org/records/8275861> when a required full dataset is
not in Loghub 1.0.

Place original, unmodified log files in one directory using this layout:

```text
data/raw/Android.log
data/raw/Apache.log
data/raw/BGL.log
...
data/raw/Zookeeper.log
```

Do not use the 2k samples, parsed CSV files, or preprocessed logs. The main
protocol reads the raw bytes, preserves line order and whitespace, and divides
each file into 100,000-line blocks.

### 2.4 Configure the LLM API

Cold discovery uses an OpenAI-compatible chat-completions endpoint. Supply the
full endpoint and API key through environment variables; SemZip does not ship
with a provider-specific endpoint or credential:

```bash
export PARE_LLM_API_BASE='https://your-provider.example/v1/chat/completions'
export PARE_LLM_API_KEY='replace-with-your-key'
```

Set the requested model in [`configs/main.yaml`](configs/main.yaml). Do not
write the key into source code or configuration files. SemZip records the
endpoint for reproducibility and redacts authorization headers in provenance
records.

## 3. Cold-Start Compression

The following command performs a complete Apache run from an empty
dataset-specific cache:

```bash
python3 run_main_batch.py \
  --raw-dir data/raw \
  --result-root runs/apache_v6 \
  --datasets Apache
```

For each dataset, the runner performs:

1. dataset hashing and manifest creation;
2. offline sampling and real LLM discovery;
3. proposal compilation, repair, and byte-exact verification;
4. replay-plan freezing;
5. online compression with 100k-line blocks and 8 workers;
6. block-by-block decompression and full-file SHA-256 verification;
7. one speed warmup and three independent encode/decode trials.

The result directory must be empty. This prevents an earlier cache or archive
from being silently reused.

Important outputs include:

```text
runs/apache_v6/dataset_manifest.csv
runs/apache_v6/main_ratio.csv
runs/apache_v6/speed.csv
runs/apache_v6/synthesis.csv
runs/apache_v6/run_manifest.json
runs/apache_v6/plans/Apache/replay_plan.json
runs/apache_v6/main/Apache/summary.json
```

The complete archive size reported in `main_ratio.csv` includes semantic
streams, residual streams, templates, dictionaries, replay metadata, codec
metadata, block headers, and every byte needed for decoding.

## 4. Cache-Only Replay

After cold discovery, replay the frozen plan without any LLM call:

```bash
bash backend/build.sh

python3 backend/run_semantic_codec_experiment.py \
  --dataset Apache \
  --input data/raw/Apache.log \
  --plan runs/apache_v6/plans/Apache/replay_plan.json \
  --semzip-source runtime \
  --result runs/apache_replay \
  --block-size 100000 \
  --workers 8 \
  --api-mode frozen-replay \
  --delog-residual-allgroup-or20
```

This command runs the online path only. It applies the frozen functions,
creates the self-contained archive, decodes all blocks, concatenates the
restored bytes, and records the full-file SHA result in
`runs/apache_replay/summary.json`.

The replay path does not read an LLM cache and does not need an API key.

## 5. Lossless Verification

Every successful SemZip run performs two checks:

1. each 100k-line block is decoded independently;
2. all decoded blocks are concatenated and compared with the original file by
   SHA-256.

The summary must contain:

```json
{
  "failed_blocks": [],
  "sha_pass": true
}
```

You can independently compare hashes on Linux:

```bash
sha256sum data/raw/Apache.log runs/apache_replay/decoded.log
```

On macOS:

```bash
shasum -a 256 data/raw/Apache.log runs/apache_replay/decoded.log
```

The exact restored-file path is also recorded in the result summary. A result
with any failed block or a mismatching full-file SHA is invalid and must not be
reported as a compression result.

## 6. Reproducing the Evaluation

Run any subset of the 16-dataset suite in one cold-start campaign:

```bash
python3 run_main_batch.py \
  --raw-dir data/raw \
  --result-root runs/loghub16_v6 \
  --datasets \
    Android Apache BGL Hadoop HDFS HealthApp HPC Linux \
    Mac OpenSSH OpenStack Proxifier Spark Thunderbird Windows Zookeeper
```

The frozen protocol is:

```text
method: SEMZIP-DELOG-ALLGROUP-OR20-ADMISSION-V6-20260718
block size: 100,000 lines
workers: 8
offline prefix: first 20%, with a minimum of 100,000 lines
sampler: LogBatcher-style family sampling
model: gpt-4o
temperature: 0
online API calls: 0
online evolution: disabled
residual recognizers: empty; no dataset-specific DeLog regex profile
full-file verification: SHA-256
```

Compression ratio is computed as:

```text
sum(original raw bytes) / sum(complete archive bytes)
```

Online encode throughput excludes offline LLM discovery and decompression, but
includes semantic replay, residual processing, both archive writers, and file
I/O.

## 7. Method Summary

SemZip V6 has five steps:

1. **Representation-aware sampling.** The trainer scans an offline prefix,
   groups structurally similar lines, and selects representative examples.
2. **LLM program synthesis.** The LLM proposes reversible functions that can
   extract raw variables or convert semantically related surface forms into a
   compact latent value.
3. **Compile, verify, and repair.** SemZip compiles proposals under a restricted
   runtime, checks exact forward/inverse behavior, and sends failed candidates
   plus diagnostics to a bounded repair pass.
4. **Frozen semantic replay.** Verified programs transform the complete log.
   Numeric/timestamp values use reversible delta-style streams, canonical IPv4
   values use `ipv4_plain`, context-conditioned numeric values use local delta,
   and string values use MTF-rank dictionaries.
5. **Regex-free residual compression.** The C++ backend receives only residual
   text and uses no dataset-specific recognizer. A residual group is admitted
   when:

```text
support >= max(20, min(200, block_lines / 1000))
AND
(distinct >= 30 OR average_value_bytes > 20)
```

The archive stores the semantic metadata and all residual information required
to reverse these steps exactly.

## 8. Artifact Guarantees and Limitations

- API keys are read from the environment and are never written to the archive
  or provenance records.
- Online replay and decoding require no LLM, proposal cache, or hidden mapping.
- The backend starts with an empty DeLog recognizer plan; benchmark-specific
  DeLog regular expressions are not enabled.
- The LLM is stochastic at the service level even with temperature 0. The
  repository records prompts, provider responses, model IDs, token usage,
  accepted programs, plan hashes, and source hashes for auditing.
- Residual extraction follows the frozen all-group OR20 admission policy in
  [`configs/main.yaml`](configs/main.yaml).

For implementation identity and frozen component versions, see
[`VERSION.md`](VERSION.md).
