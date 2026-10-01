# Required inputs and anonymous JSON contract

Seven explicit JSON files are required. Unknown source fields are discarded rather than recursively copied. Duplicate keys, nonfinite JSON numbers and malformed digests are rejected. The public dataset names and method names are fixed, not inferred from directory names.

| Role | Minimum required evidence |
|---|---|
| snapshot | Corrected final export with `status=COMPLETE`; all 16 original datasets × 6 methods; terminal PASS/FAIL/FAILED rows; original bytes/SHA, complete archive bytes and block count for successes; first-pass diagnostic timers; 16 successful original syntheses/corrected fits; the corrected scope explicitly excludes preceding scanning/sampling and does not call the component sum total offline latency; original publication wall separately retained; complete SemZip block ledger and every saved rule's activity. |
| formal-summary | Version `R68-FORMAL-R71-OUTER-CLI-V3`; 72 cells (12 datasets × 6 methods); all planned observations retained; no replacement trials; per-cell three statuses, archive identities and throughput observations/median/range; exact frozen outer-process timing scope. |
| formal-status | Completed PASS or COMPLETE_WITH_FAILURES campaign; 216 unique terminal trials in frozen schedule order; unchanged identity lock; completed post-campaign archive audit; `config_sha256` equals the supplied DESIGN file; each successful trial's original and decoded SHA, all archive file sizes/SHA, process exit status and wall seconds. Interrupted/failed trials remain terminal observations, not replacement opportunities. |
| formal-design | Frozen 100000 physical LF records/block, four workers and three repeats; fixed twelve-dataset cohort through BGL; 216 scheduled identities; 72 first-pass archive identities and eligibility; sixteen original references, locked SemZip publications and representation inputs. |
| representation | Four results in the original fixed order Linux, HPC, OpenSSH, Android; both independently fitted branches, complete archive components, full decode SHA, plan/storage identities, full/suffix effects; cold cache-boundary verification and zero new API calls. |
| parity | Completed read-only audit of all 29 paired blocks; independent latent and surface SHA certificates; each latent semantic payload and shared native payload equal production; manifests byte-identical; complete costs equal production. |
| sampling-replay | Successful separately observed zero-API replay for all 16 datasets; sample SHA exactly equals saved original sample SHA; original training-block identity; three separate scan/writer/combined timings; original synthesis subprocess timer; `original_training_modified=false`, explicit separate-replay interpretation and per-dataset timing scope, and a valid observed control interval. |

Public output root `schema` is `semzip.anonymous.final-results.v1`. Arrays retain every terminal row. `size_rows` is 96 cells; `formal_cells` is 72 cells; `formal_runs` is 216 observations; `training` and `sampling_replay.datasets` each have 16 entries; `representation` has four entries; `paired_block_certificates` has 29 entries. `main_blocks` preserves all block identities for successful SemZip files.

Only dataset/method enums, booleans, validated finite numbers, public safe rule tags, fixed storage-route enums and SHA-256 digests are copied. Failure details are represented by status and an evidence-record hash, not by private free-form traceback. Missing or failed values are absent/null as defined by the validator; zero is never used as a surrogate for a failed compression or timer. Aggregate values are recomputed rather than copied.

The supplied representation results and parity audit's design/status digests must occur in the formal identity lock; each paired production certificate also binds the main snapshot result SHA.

Representation semantic/residual totals and suffix payload counters are recomputed
from the parity audit's per-block size ledger, not accepted merely because their
reported percentages divide consistently. Original full/suffix raw-byte totals
must match that ledger. Main suffix archives use original blocks 1 onward **plus
their recomputed sequential manifest**; the representation suffix effect uses
only the same block payloads. For the four paired files the validator also
reconstructs the suffix manifest from the hash-bound canonical full manifest and
checks its exact byte length. One-block files have zero suffix bytes and no suffix
ratio/effect. These are arithmetic/certificate checks, not new compression runs.

`input_sha256` binds all seven supplied files, and `integration_script_sha256` binds the validator. `generated/numeric_provenance.json` separately binds the rendering script, reports library versions and recomputed aggregates. SHA identities attest file relationships; they are not independently reproduced decoding by this integration tool.

V3 preserves separate outer-process timers and the complete 216-trial schedule.
The independent post-decode audit still requires the full original SHA, every
original block identity, and stable size/device/inode/path. Only a newly closed
output file's modification-time adjustment is recorded without rejecting an
otherwise exact reconstruction. The input identity audit remains strict. The
superseded V2 instrumentation run is retained separately and is never pooled
with the V3 trials. No retry or rescan replaces a failed V3 observation.
