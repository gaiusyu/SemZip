# SEMZIP-V6-PAPER-MAIN-COLDSTART-20260718

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
