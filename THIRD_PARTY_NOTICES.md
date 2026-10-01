# Third-party notices

This repository's own code and documentation are covered by `LICENSE`. The components below keep their own terms.

## DeLog-derived residual backend (`source/backend/`)

The native residual backend of SemZip is derived from DeLog (the DeLog repository cited in the paper, commit
`64a074f6b6559fbfcd809f201fc3540442151749`; pinned in `external/fetch_baselines.sh` and `external/manifest.json`).

| File | Relation to DeLog at that commit |
|---|---|
| `source/backend/compressor.cpp` | modified (adds the plan-driven extraction and the residual options used by SemZip) |
| `source/backend/decompressor.cpp` | unchanged upstream file (SHA-256 `74c4cc609fcf227f334b37bfe72289b99911642dbcef86e1237e851c0a899a8a`) |
| `source/backend/BS_thread_pool.hpp` | unchanged upstream copy of a third-party header (see below) |
| `source/backend/Delog_plan_compress`, `source/backend/decompress` | Linux x86-64 executables built from the two `.cpp` files above with `source/backend/build.sh` (GCC, Debian 10.2.1-6), dynamically linked to libarchive (BSD license) and PCRE2 (BSD license) |

The DeLog-derived files and the executables built from them are included only so that the frozen SemZip runtime can be
replayed; they are frozen and hash-checked by `replay.py`. They may be used to reproduce and evaluate the results in
this repository; redistribution beyond that follows the terms of the DeLog repository.

## Header-only libraries (`source/backend/`)

* `json.hpp`: JSON for Modern C++ 3.12.0, Copyright (c) 2013-2025 Niels Lohmann, MIT license (header retained in the file).
* `BS_thread_pool.hpp`: BS::thread_pool 5.1.0, Copyright (c) 2021-2026 Barak Shoshany, MIT license (header retained in
  the file).

## Patches and adapters for baselines

No baseline source code is redistributed. `external/loglite_wide_reserve.patch` modifies LogLite and applies under
LogLite's own license; the adapters under `external/` and `evidence/r76/baselines/` are this repository's code and call
the upstream tools fetched by `external/fetch_baselines.sh`, which keep their own licenses.

## Data

LogHub data are not redistributed; they are fetched from the public LogHub release by `scripts/prepare_data.py` and keep
their original license and citation terms. Recorded model prompts, responses and synthesized programs contain short
excerpts of LogHub log lines, which belong to the original log producers.
