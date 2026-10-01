# R76 track C: variability of the complete SemZip pipeline (HPC, Hadoop, HDFS, Spark)

> **This copy.** For runs r2 and r3 the records of every step are included: `repeat.json`; the five syntheses
> (`train_k/<D>/<cand>/training.json`, `training_work/<D>/{replay_plan,summary}.json` and the model exchanges
> `llm_caches/<D>/shared/**/family_*.{prompt.txt,json}`, i.e. every prompt and parsed response); the gate
> (`runs/qg/c*/<D>/`: `qg_report.json`, `selected_plan.json`, `eval_cache.json`, and per evaluation the block-0 round trip
> `eval_*/tc/rt/result.json` with its decode summary); the pool (`runs/pool/<D>/`: `candidates.json`, `pool_report.json`,
> `selected_plan.json`, `eval_cache.json` and the per-evaluation records); the published program and storage policy
> (`runs/publish/pool/<D>/`); and the formal record (`runs/formal/pool/<D>/result.json`). Code: `rep.py`,
> `check_r73_repro.py`, `code/formal_full.py` (the only changed driver), `code/SHA256SUMS.r73_originals` and the earlier
> driver versions `driver_versions/rep_v1_smoke.py` and `rep_v2.py` (`rep_v3.py` is byte-identical to `rep.py`). The other
> `code/*.py` are the unchanged R73 drivers published in `evidence/selection/code/` (as-run hashes in
> `code/SHA256SUMS.r73_originals`) and are not repeated. Not included: run logs, archives, the per-candidate storage-fit work files (`tc/storage/`), the copies of
> each evaluated plan (`eval_*/plan.json`; `eval_cache.json` identifies every evaluated plan by its SHA-256 and rule
> tags), the raw HTTP records of the model calls (`_api_records/`: raw request and response bodies plus the gateway's HTTP
> response headers; every prompt and the parsed model output are kept in the cache files), and
> the training blocks (`train/`, `raw/`, `r69root/`). Hashes of code files that contained machine-specific paths are
> withheld in `repeat.json` and `code/SHA256SUMS.r73_originals` (`../CODE_PROVENANCE.md`).

This track runs the pre-registered design in `../DEV_DESIGN_R76_zh.md`, section C (written 2026-09-29 01:30 UTC, before any
R76 result). For each of HPC, Hadoop, HDFS and Spark (the files where R73 pool leads DeLog by only 2-6 %), it runs
**two more complete repeats (r2, r3)** of the unchanged R73 main pipeline ("pool"). The R73 run is repeat r1 and is
not replaced. Nothing outside `variance/` is written. R73, R75 and the data are only read.

## What one repeat is

Everything is done on original block 0 only: the first 100,000 lines, i.e. the same `train.log` that R69 and R73
used. Its SHA-256 is checked against DeLog's block-0 `raw_sha256` in `r68_external_20260923/first_pass`.

1. **5 fresh syntheses** (an OpenAI-compatible gateway serving gpt-4o; endpoint and key loaded by `train_k.py` from a credential file that is never printed).
   Trainer, prompts, sampler and 40-call budget are the unchanged R69 ones (`train_k.py`). There are no HTTP retries.
   Each run gets a fresh output directory with its own fresh LLM cache (`force_llm=True`).
   - `c0`: T=0.0 (`train_k/<D>/t0.0_k1`)
   - `c1`-`c4`: T=0.7 (`t0.7_k1..k4`)

   These are exactly R75 `evo.pipeline(b)`'s `runs`. If a synthesis fails, or its output directory has no
   `training.json`, it is kept as unusable and never retried (the R73 phaseB/phaseC rule).
2. **QG-V1** on each usable candidate: `qg_select2.py D plan train out 1` (0 API). A gate that crashes gets
   `FAILED.txt`, is excluded, and is not retried.
3. **QG-POOL-V2** over the gated candidates, using the complete block-0 archive cost:
   `qg_pool2.py D candidates.json train out 3` (0 API). `candidates.json` is built exactly as in R73 `phaseD_follow.py`:
   `{cid, plan: runs/qg/<cid>/<D>/selected_plan.json, train_cost: selected_train_archive_bytes}` for c0..c4.
4. **Publish**: `publish.py pool D runs/pool/D/selected_plan.json runs/pool/D` refits storage on block 0 and runs the
   determinism check. It runs with cwd = the repeat directory, exactly as R73 `formal_queue2.py` does.
5. **Formal full file**: `formal_full.py pool D program.json storage.json` encodes the full file (100k-line
   independent blocks) and then decodes it in a separate archive-only process, with the external policy made
   unavailable. It checks the per-block SHA-256 and full-file SHA-256 on the materialized output, checks raw identity
   against `r71_strict_main_20260923/reference.json`, and does the suffix (held-out) accounting.

The candidate order, the tie rules and the evaluation budgets (pool `MAX_ROUNDS=4`, `MAX_EVALS=600`) are unchanged.
The thread and worker counts change only the evaluation order and speed, never the decisions or the bytes (see the
harness checks).

## Code

- `code/`: byte-identical copies of `r73_quality_gate_20260927/r73_qg/{lib,qg_select,qg_select2,qg_pool,qg_pool2,publish,train_k,fit_storage_v2,formal_full}.py`.
  Their SHA-256 values are in `code/SHA256SUMS.r73_originals`.
  **One deviation:** in `code/formal_full.py`, the worker count is read from `FORMAL_WORKERS` (default 4 as in R73),
  and the value is recorded in `result.json`. This track uses 3, because the shared-host cap is 3 or fewer full-file
  workers. Diff against R73: only the lines `WORKERS = ...`, `guard.encode(..., 100000, WORKERS)`,
  `'--workers', str(WORKERS)` and `'workers': WORKERS`.
- The environment matches R73 `launch.sh`: `QG_ART=r73_qg/art` (frozen guard, source runtime, native backend),
  `PYTHONHASHSEED=0`. There are three differences:
  - `QG_HOME=variance/code`. This only locates `fit_storage_v2.py`, which is an identical copy.
  - `PYTHONDONTWRITEBYTECODE=1`, so that no `__pycache__` is written into the read-only R69/R73 trees.
  - `R69_ROOT=variance/r69root`, where `source` points to R69 `source` and `inputs/<D>/train.log` points to the R69
    `train.log`. This is the same layout R75 used. The file contents are identical to R73's default `R69_ROOT`.
- The `PARE_`, `YUNWU_`, `SEMZIP_` and `*_API_KEY` variables are stripped from the driver environment.
- `rep.py` is the driver. Its commands:
  - `dryrun`: prints the commands.
  - `run D R SLOTS`: runs one repeat.
  - `queue SLOTS LANES D:R,...`: runs several repeats under one shared FIFO budget of at most SLOTS heavy child
    processes. A synthesis costs 1 slot, a gate 1, the pool 3 and a formal run 3.
  - `summary`: writes `summary.json`.

  The driver keeps no state across blocks and has no dataset-specific switch. Every repeat of every dataset uses the
  same commands and constants.
- `check_r73_repro.py`: the harness-equivalence check described below.

## Harness equivalence checks (0 API, done before any new synthesis)

- `check_r73/check_HPC.json`: the R73-published HPC pool program and policy were run through `code/formal_full.py`
  with `FORMAL_WORKERS=2`. The result is 732,843 archive bytes, the same as R73 (which used 4 workers). **All archive
  files are byte-identical** (same size and SHA-256), and the SHA check passes.
- `check_r73/check_qg_c0_HPC.json`: `code/qg_select2.py` was run on the R73 c0 HPC plan with 2 threads. It gives the
  same original / empty / selected bytes (260,227 / 195,087 / 192,651), the same removed tags [B, D], the same
  selected-plan SHA-256 and 12 evaluations, and the **decision list is identical**.

## Layout of one repeat: `runs/<D>/<R>/`

- `train/<D>.block0.log`: symlink to the R69 training block.
- `raw/<D>.log`: symlink to the same resolved raw file as `r73_qg/raw`.
- `train_k/<D>/t*_k*`: synthesis outputs. `training.json` holds `status`, `api_calls` and `wall_seconds`.
- `runs/qg/c*/<D>`: QG-V1 results.
- `runs/pool/<D>`: `candidates.json` and `pool_report.json`.
- `runs/publish/pool/<D>`: `program.json`, `storage/storage.json` and `publication.json`.
- `runs/formal/pool/<D>/result.json`: the full-file result. The archive is kept, and the restored file is deleted
  after the SHA check.
- `logs/`: one log per synthesis, gate, pool, publish and formal step.
- `repeat.json`: machine-readable summary of the repeat. It records the synthesis status, API calls and times, the
  gate results, the candidates that passed, the pool start and selected plan, the block-0 cost, the formal bytes,
  ratio and SHA, bytes versus DeLog, the code SHA-256 values, and the environment. It also records how many times the
  API key occurs in the logs; this is a count only and must be 0.

## Commands used

```
cd variance
PYTHONHASHSEED=0 python3 rep.py dryrun HPC r2                                  # no API
PYTHONHASHSEED=0 python3 check_r73_repro.py HPC                                # harness check, 0 API
AGNICE=5 ../launch.sh variance/logs/HPC_r2_smoke.log python3 -u rep.py run HPC r2 2        # smoke = real repeat r2
AGNICE=5 ../launch.sh variance/logs/queue.log python3 -u rep.py queue 4 3 <spec>           # remaining 7 repeats
python3 rep.py summary                                                          # summary.json + table
```

## Smoke test = HPC repeat r2 (real API calls, 2 slots)

- 5/5 syntheses PASS. API calls: c0 9, c1 13, c2 11, c3 11, c4 11, for 55 in total. The synthesis phase took 188 s of
  wall time (runs of 55-98 s each, 2 at a time).
- QG-V1 block-0 bytes, original -> selected (the empty program is 195,087):
  - c0: 195,087 -> 195,087
  - c1: 193,823 -> 193,823
  - c2: 195,087 -> 195,087
  - c3: 194,691 -> 193,823
  - c4: 194,887 -> 194,887
- All 5 candidates passed into the pool.
- POOL starts from c1 (193,823, start cost reproduced) and ends at **193,123** after 38 evaluations. There were
  7 pool groups, and 1 duplicate was skipped. The selected tags are [TS, H, X4DN]. The publish refit is identical to
  the selection fit.
- Formal full file: **733,727 B**, raw 33,553,503 B, ratio 45.730, 5 blocks, 0 fallback blocks.
  - SHA passes: archive-only decode, per-block and full-file.
  - Suffix: 540,725 B, ratio 45.18.
  - Versus DeLog (750,604 B): **-2.25 %**. R73 r1 was 732,843 B (-2.37 %).
- The API key occurs 0 times in the logs and in `training.json`.

**Operational deviation during the smoke test (no effect on decisions):** driver v1 clamped the pool's slot request
to the 2 smoke slots but still started `qg_pool2.py` with 3 threads. The v1 process group was killed (SIGKILL) at 02:42 UTC,
after all 5 syntheses and 4 of the 5 gates had finished, while the c3 gate was running and before any pool or
formal step. No synthesis was affected or repeated.

The driver was then fixed (v2: the effective pool threads and formal workers are `min(requested, slots)`) and
restarted at 02:43 UTC. The c3 gate resumed from its own evaluation cache (`eval_cache.json`, written atomically). QG
decisions are a deterministic function of the cached costs, and the interrupted evaluation was recomputed from
scratch. So the pool ran with 2 threads and the formal run with 2 workers.

Driver versions are kept in `driver_versions/` (v1 smoke, v2 fix, v3 = the thread-safe log line only).
`repeat.json:restarts` records the restart.

## Full queue (remaining 7 repeats)

Launched 2026-09-29 02:48 UTC, pid 1238525, autogroup nice 5:
`rep.py queue 4 3 Hadoop:r2,Spark:r2,HDFS:r2,HPC:r3,Hadoop:r3,Spark:r3,HDFS:r3`.

- There are at most 4 heavy child processes at any time.
- Up to 3 repeats are in flight at once.
- Resource costs: a synthesis takes 1 slot, a gate 1 slot (1 thread), the pool 3 slots (3 threads) and a formal run
  3 slots (3 workers).

Progress: `tail logs/queue.log; cat queue_status.json; python3 rep.py summary`.

The first wave (Hadoop r2, Spark r2, HDFS r2) finished its syntheses between 02:50 and 02:55 UTC: 15/15 PASS.
API calls:

| Repeat    | Total | c0 | c1 | c2 | c3 | c4 | Synthesis wall time |
|-----------|------:|---:|---:|---:|---:|---:|--------------------:|
| Hadoop r2 |    61 | 20 | 16 |  6 |  5 | 14 |               268 s |
| Spark r2  |    65 |  9 | 12 |  8 | 11 | 25 |               253 s |
| HDFS r2   |    44 | 12 |  4 |  6 | 12 | 10 |               226 s |

There are no writes into the R69/R73 trees (`find -newermt` is empty).

Rough ETA for the whole queue is about 10-14 h, to around 2026-09-29 17:00-19:00 UTC. This estimate uses the R73
timings with 4 slots on a shared machine:
- gates: Hadoop about 5 h of CPU per repeat, Spark about 1.5 h;
- pool: Hadoop about 1.1 h at 4 threads in R73, Spark about 0.5 h;
- formal: Spark about 0.7 h at 4 workers in R73.

These are new random syntheses, so the real time can differ.

## Results

(filled in when the runs finish; see `summary.json`)
