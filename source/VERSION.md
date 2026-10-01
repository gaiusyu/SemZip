# SEMZIP-V6-EXPLICIT-EXECUTION-ENV-R36-20260915

Child of R35; a correctness candidate, not a promoted version. Adds an explicit
UTC0/C-time-locale execution policy to training workers and semantic archives.
Decoder requires the recorded policy; legacy archives are not silently guessed.
Generated code, regexes and codecs remain unchanged. Numeric initial values and
metadata bytes may differ from the CST parent. See ../ROUND36.md for validation.
The descriptions below are parent history, not R36 results.

## Parent R35

Independent child of R32, NOT R33 or R34. Adds only
backend/run_bounded_semantic.py: up to eight original blocks in flight, unchanged
semantic worker and native per-block codec. Online scratch is block-bounded;
standalone decode accepts archives and runtime, not an external plan or log.
Archive byte equivalence and full-data results are pending. See ../ROUND35.md.
Everything below records inherited history, not a R35 result.

## Parent R32

R32 corrects benchmark scope only, not compression behavior: after argument
parsing through all package writes. Runtime remains byte-identical to R31.
backend/run_semantic_codec_experiment.py SHA256
eb1f2a0089611e029b0927439ec6d73f8888824cfc27d3755ca76028560cecfb.
run_main_batch.py SHA256
3541346b2736f120b410e726bf830e8de5bc491816731fef2e35e4e5c6c714d8.
See ../ROUND32.md. The descriptions below retain parent history.

Current R31 parent R30: pure padding helper identity fast path and immutable
tuple LRU, no generated function output cache or archive-format change.
Runtime SHA256 0a6cf004319f792cefc4c6edd64c80a9c28aa38c6a639100c4b8fef30684db1f.
See ../ROUND31.md. All text below describes parent history.

Current candidate: R30, parent R29. Explicit identity display complements and
constant layout metadata; generated code and matching unchanged. See ROUND30.md
in the parent goal directory. Runtime hash:
d8b5d0fd7da13fc43c20031c862bb3ecf40e7b68d336989f67601b5de531c980.
The descriptions below retain parent history.

Current candidate: parent R28, adds syntax-aware offline display-layout plan
lowering; the LLM program code remains unchanged. The layout writer now uses
exact decimal dictionary storage for integers outside its delta envelope, with
the same inverse/layout reconstruction. Other online routes remain unchanged. See
../ROUND29.md. The rest below records parent history, not the R29-only delta.

Research candidate, not the frozen mainline. Parent R27 prefix dictionary.
See ../ROUND28.md for the scope and validation status. R28 changes only
canonical program-key serialization into a bounded, typed content-keyed cache.
It inherits R20 train-only, R21 counterexample repair, R25 uniform string
routing and R27 prefix dictionary. It does not add new dataset-specific rules.
Runtime pare_dataset_extract.py SHA256:
2e0a992799ded106752d482f52f47ca7fa515f4eac3d0b6f90eba11ac040e707.

The following describes the ORIGINAL frozen source, not this candidate's
byte identity. Result attribution must use the candidate ID and source hashes.

## Historical Frozen Source Description

This paper-facing campaign freezes one complete SemZip configuration:

- `trainer/`: `SEMZIP-CLASS3-DUAL-EVIDENCE-ABLATION-20260716`, used only for
  cold offline discovery, API proposal/repair, compilation, and byte-exact
  verification.
- `runtime/`: `SEMZIP-REGEX-ANCHOR-GUARD-POSITION-DENUM-20260717`, the exact
  semantic replay runtime used by the historical V6 artifacts. It executes
  verified plans and provides the structure guard required by the V6 semantic
  frontend.
- `backend/`: `SEMZIP-DELOG-ALLGROUP-OR20-ADMISSION-V6-20260718`, used for
  frozen-plan semantic replay and residual compression.
- `configs/main.yaml`: the single experiment configuration.

The V6 residual rule is applied to every residual group. No DeLog
dataset-specific recognizer is enabled. No dataset-specific cache is reused.
Online compression uses zero API calls and does not evolve the replay plan.

The copied trainer, runtime, and backend are intentionally preserved
byte-for-byte from their source directories before this campaign was created.
The top-level runner only orchestrates and records experiments; it does not
change the compression algorithm.
