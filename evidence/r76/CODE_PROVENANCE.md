# Provenance of the R76 code files

The R76 tracks ran on the execution host. Their result records, READMEs and design files in this directory were
copied from that host. The code files listed below were copied read-only from the track directories on the execution
host after the runs and checked against the execution records:

* **recorded**: the SHA-256 of the file as run equals a hash written by the track itself into its records (full hash,
  or the hash prefix listed in the track README); the record is named in the last column.
* **host**: copied from the track directory on the execution host; the track wrote no hash of this file into its
  records. An earlier assembly of this repository had taken some of these files from a mirror of the track directories
  because the host was unreachable; every such file was re-checked against the host copy and is byte-identical to it
  (after the same placeholder replacement).

`sanitized = yes` means the file is not byte-identical to the file as run: machine-specific absolute paths in it were
replaced by placeholders such as `<WORKDIR>` or `<MOUNT>`, the private credential-file path by `<API_CONFIG_FILE>`, and
local-time stamps with an explicit zone were converted to UTC (and, in `timing/phase_codec_r76.py`,
`safety/v2/pyexec_validator.py`, `unseen/rep.py` and `variance/rep.py`, a hash prefix or a time-zone remark was removed
from a docstring or comment; `timing/timing_r76.py` pins the published copy of `codecs/codec_baseline_r76.py`, and
`safety/coverage_20261002/coverage_check.py` and `safety/coverage_20261002/scan_new_archives.py` pin the published
copies of the `safety/v2/` modules they import, whose as-run hashes they checked; two hash prefixes were also removed
from the docstring of the former). In
`unseen/optional_after.py` string literals changed, not only comments: the give-up deadline, a date-time string literal
that the file parsed in the execution host's local time (`time.mktime`), now holds the same instant written in UTC and
is parsed as UTC (`calendar.timegm`; `calendar` was added to the imports, and the comment says UTC), and the skip-reason
string literal written to `SKIPPED.json` (if the deadline passes) gives that UTC instant instead of the local one with
its zone name. The deadline instant, and therefore the behaviour, is unchanged (the deadline was not reached; the
optional baselines ran). The as-run SHA-256 of these files is not listed here, nor in the track records, READMEs and
hash lists that held it (there the value reads "withheld" or "not listed"); all other hashes in those records are
unchanged.

Generated at packaging time (not run): `codecs/r68_to_r76_harness.diff` (`diff` of the recorded R68 harness
`external/codec_baseline.py` and `codecs/codec_baseline_r76.py`) and `unseen/code/formal_full_vs_variance.diff` (`diff -u`
of `variance/code/formal_full.py` and `unseen/code/formal_full.py`). `codecs/delog/generic_vs_official.compressor.diff` is
the patch the codecs track itself wrote (DeLog-generic = official DeLog `compressor.cpp` at the pinned commit with this
patch applied); only its header timestamps were converted to UTC.

Byte-identical copies that the tracks kept of other code are not repeated here (identity checked on the host copies):

* `timing/vendor/codec_baseline_r76.py` and `unseen/baselines/harness/codec_baseline_r76.py` = `codecs/codec_baseline_r76.py`;
* `unseen/baselines/harness/loglite_adapter.py` = `external/loglite_adapter.py`;
* `variance/code/*.py` other than `formal_full.py`, `library/{lib,publish,qg_select,qg_select2,fit_storage_v2}.py` and
  `attribution/formal_full.py` = the R73 drivers in `evidence/selection/code/` (as-run hashes in
  `variance/code/SHA256SUMS.r73_originals`); `variance/code/formal_full.py` differs only by the worker count from
  `FORMAL_WORKERS` (`variance/README.md`);
* `unseen/code/*.py` other than `formal_full.py` = `variance/code/*.py` (`unseen/code/SHA256SUMS.r73_originals`);
* `variance/driver_versions/rep_v3.py` and `unseen/rep_variance_v3.py.orig` = `variance/rep.py`.

| file | provenance | sanitized | SHA-256 as run | record |
|---|---|---|---|---|
| `attribution/run_empty.sh` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `baselines/denum/check_enconly_smoke.py` | host | no | `55fce199bd91641036a93a797db5ff52838c8dde05f968893f81ff28b5f9507d` | - |
| `baselines/denum/codec_baseline_denum.py` | recorded | no | `de4daaf45478a501963c2d958ba2460eac860ce5411854a36101fefd00121ff3` | baselines/denum/smoke_v2/manifest.json (full) |
| `baselines/denum/codec_baseline_denum_enconly.py` | recorded | no | `6c1bf68ebbc72e738d0550b37bc2190829663fd3ad045285dcd9aa907f521135` | baselines/denum/README.md (full) |
| `baselines/denum/diff_roundtrip.py` | host | no | `66d48606aad2812c2774d7c7f2ebcc073e77150de474ee3169ce7e1f86ffb96b` | - |
| `baselines/denum/equiv_check.py` | recorded | no | `3d7a5e88642cbb79d63ac6bd38ddd840e14daa0bb43c4de5e541d9e8ee2989d5` | baselines/denum/README.md (prefix 3d7a5e88) |
| `baselines/denum/native_official.sh` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `baselines/denum/prove_lossy.py` | recorded | no | `2631a9ee3c02cb1d7e616b6be2a5b43d7a7b79d607fcfd1fc787acc54b54767a` | baselines/denum/README.md (prefix 2631a9ee) |
| `baselines/denum/summarize_smoke.py` | host | no | `4f9db17b71dd12ae6490f5ec6c0fa26096cb6ddd82b272ffce4cdbf9b333e7c5` | - |
| `baselines/lognexus/ln_run.py` | recorded | no | `a561dcddd3c36a08718cbb30f189587a93eb653192a71f3e6bffd87dca23ea88` | baselines/lognexus/full_paper/*/manifest_*.json (full) |
| `baselines/logreducer/after_t2_lr_tb.sh` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `baselines/logreducer/lr_run2.py` | recorded | no | `bbf044c186b58bd40d0c38826df2a0a2b09add1074eee7d24024d0598c6ce549` | timing/README.md (full) |
| `baselines/logshrink/adapter/compare_decoders.py` | host | no | `1de59bf8c2e07b8ed60d23edbdb7ea739bce21afa4ed02c6eadac3c4450eba8d` | - |
| `baselines/logshrink/adapter/dbg_redecode.py` | host | no | `3de4443dd93dc53317eeab2ecd5fd445c8549f4b928f920cd25c1c87e5bd9d6a` | - |
| `baselines/logshrink/adapter/determinism_check.py` | host | no | `8a19bc4047a455ce3bb19c599dc281f627d062a546cc9655835dc2431d1c90a0` | - |
| `baselines/logshrink/adapter/equiv_check_runA.py` | host | no | `974889623e058775320e9e0d4deb33d6ded43ef2a36ebb74cac909ada8a6e919` | - |
| `baselines/logshrink/adapter/ls_run.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | timing/README.md (withheld there too) |
| `baselines/logshrink/adapter/ls_seg_runner.py` | recorded | no | `1fb277853474c6413307a74c80140a4bf2e93eea66250be2db3a8bd46f64d157` | baselines/logshrink/probe1/campaign.json (full) |
| `baselines/logshrink/adapter/ls_segment.py` | recorded | no | `121a5e74a4e473c6c51a8edadb68b9f92d50f6922db77bfbb900cab390854a76` | baselines/logshrink/probe1/campaign.json (full) |
| `baselines/logshrink/adapter/ls_status.py` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `baselines/logshrink/adapter/make_restore_r76.py` | recorded | no | `4083d970c631d6f58a328fe670301c3368d2129881a3f02b278c6e13b4bf3018` | baselines/logshrink/full_official/campaign.json (full) |
| `codecs/check_generic_tags.py` | host | no | `661406f2aa2a77d639463cdfd8f4e114a1049425c07aa2c85ec2033211ddef5c` | - |
| `codecs/codec_baseline_r76.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | codecs/README.md (withheld there too) |
| `codecs/delog/generic_vs_official.compressor.diff` | host | yes | not listed (header timestamps converted to UTC in this copy; it contains no path) | - |
| `codecs/summarize.py` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `launch.sh` | host | no | `ebdec34e6d971163481bf8e41da0d23fdd7167154aa7b4edf7504bfb2eeeb06c` | - |
| `library/formal_full.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | library/FROZEN_SHA256.txt (withheld there too) |
| `library/full_chain.sh` | recorded | yes | not listed (machine-specific path replaced in this copy) | library/FROZEN_SHA256.txt (withheld there too) |
| `library/proposer.py` | recorded | no | `8280e9f5b0ab4720d4a659bfdebac2d7b1510327616021b06d75a2940dc44ca0` | library/FROZEN_SHA256.txt (full) |
| `library/report.py` | recorded | no | `c24cb2401ee093769a4a6d6fdf74f1917abacba210f178342f04f57734e47873` | library/FROZEN_SHA256.txt (full) |
| `library/run_library.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | library/FROZEN_SHA256.txt (withheld there too) |
| `library/semzip_library_wrapper.py` | recorded | no | `9f389face717060a2afc651b69547fa6335703d955d386c66e3f392abe354134` | library/FROZEN_SHA256.txt (full) |
| `library/test_proposer.py` | host | no | `de1af67586ad63b91698a555077e2fff5746347f4ba6fdafdba921f79449d7e1` | - |
| `library/train_lib.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | library/FROZEN_SHA256.txt (withheld there too) |
| `rq2/code/decode_verify.py` | recorded | no | `f24266c4b4b2c17fe8c24fcba7feb4b0279bf03c4d712365e83197c184dbc1ce` | rq2/README.md (full) |
| `rq2/code/prepare_plans.py` | recorded | no | `59845bd1e6c8b60c305bad48e63048f542f58ab5c2bc1430a820c01695d84333` | rq2/README.md (full) |
| `rq2/code/representation_control_r76.py` | recorded | no | `957577228c55539a11c0c0e7946a82e17d40a0130f73a17b7de8947c3b046999` | rq2/README.md (full) |
| `rq2/code/representation_control_v2.py` | recorded | no | `1334fd05a504e5ec3ff725c2aa163217daf02813bbe13890ff9f592156ae4208` | rq2/README.md (full) |
| `rq2/code/results_table.py` | recorded | no | `9b3ea24bf842300f0637764a625770ed56ac85ddd903e50ed53982d95076a47a` | rq2/README.md (full) |
| `rq2/code/run_all.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | rq2/README.md (withheld there too) |
| `rq2/code/runtime_cache_boundary.py` | recorded | no | `e2098cb527c652aa0cd4464cffd0a16e7e7ca6ccff2c59474fc812069adfb911` | rq2/README.md (full) |
| `safety/coverage_20261002/coverage_check.py` | recorded | yes | not listed (machine-specific path replaced and expected V2 module hashes pinned to the published copies in this copy) | safety/coverage_20261002/coverage_check_result.json (withheld there too) |
| `safety/coverage_20261002/scan_new_archives.py` | recorded | yes | not listed (machine-specific path replaced and expected V2 module hashes pinned to the published copies in this copy) | safety/coverage_20261002/archive_scan_summary.json (withheld there too) |
| `safety/enforced_decode.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/smoke/template/Zookeeper/result.json (withheld there too) |
| `safety/negative_controls.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/pyexec_validator.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/static_check_result.json (withheld there too) |
| `safety/reach_probe.py` | recorded | no | `02025104c60506519e7d12ad483ba53c1e446f33779eac9485c049fecbc7d048` | safety/README.md (prefix 02025104) |
| `safety/run_verify.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/scan_archives.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/static_check.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/static_check_result.json (withheld there too) |
| `safety/summarize.py` | recorded | no | `cf407a320bc4336b2d5a1b1c1740e6cbd119b509ad032bce1ef98f140004ddd3` | safety/README.md (prefix cf407a32) |
| `safety/test_enforcement.py` | recorded | no | `ddc0076678fb019c2b9ee07706d929096307ad6cee874ffeb47a2b344ad8e4b6` | safety/README.md (prefix ddc00766) |
| `safety/test_validator.py` | recorded | no | `60802e7b815c50597ed837369b9d665e9ecf3e86ee5330b1244bcc30cd4cdeb5` | safety/README.md (prefix 60802e7b) |
| `safety/v2/chain_after_v1.sh` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/v2/enforced_decode.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/v2/full_v2_20260929/template/OpenSSH/result.json (withheld there too) |
| `safety/v2/negative_controls.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/v2/pyexec_validator.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/v2/static_check_result.json (withheld there too) |
| `safety/v2/run_verify.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/v2/scan_archives.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/v2/static_check.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/static_check_result.json (withheld there too) |
| `safety/v2/test_enforcement.py` | recorded | no | `ae8aacb31e6718a4fc1f6575ca22298d2a8ddb2423cfe73bb8da18ceb42c2fa9` | safety/README.md (prefix ae8aacb3) |
| `safety/v2/test_validator.py` | recorded | no | `bec8a8d2c810d1d756a123d74147b9df9a9eb47e9f9708727eeab8c984cea206` | safety/README.md (prefix bec8a8d2) |
| `timing/chain_20261001.sh` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `timing/check_sample_semzip.py` | recorded | no | `4966eaf84011e39dc8741342f3911c00873a82afbe3320e5406864faa207893c` | timing/README.md (full) |
| `timing/phase_codec_r76.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | timing/README.md (withheld there too) |
| `timing/phase_lr.py` | recorded | no | `dcfc9b7178a93646db3b7577e85acdba11ba2b90620d183a4ce8222a12b63caf` | timing/README.md (full) |
| `timing/phase_ls.py` | recorded | no | `92218db24cc55277dfdfeb56edb95ce618035c9054bc7ec29ece7a191e4b5680` | timing/README.md (full) |
| `timing/start_after_lr.sh` | host | no | `71f985c5091a4232b765b27aaf7207ce4ab511b07c2193b1b6bad71113e90304` | - |
| `timing/start_after_lr2.sh` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `timing/start_after_runs.sh` | host | no | `6b85e5926d051f1da80b2d4361a6892e11314613e9fa8a24a13d524a13100560` | - |
| `timing/timing_r76.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | timing/README.md (withheld there too) |
| `unseen/baselines/logshrink_adapter/ls_run_unseen.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | unseen/baselines/logshrink/campaign.json (withheld there too) |
| `unseen/code/formal_full.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | unseen/README.md (withheld there too) |
| `unseen/optional_after.py` | host | yes | not listed (machine-specific path replaced and deadline literals converted to UTC in this copy) | - |
| `unseen/prep_inputs.py` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `unseen/rep.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | unseen/queue_status.json (withheld there too) |
| `unseen/smoke_formal.py` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `unseen/summarize_all.py` | host | no | `1beb28dfe8618114283f369f64fc8e94e0c9be565072c90d4e307154319ea690` | - |
| `variance/check_r73_repro.py` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `variance/code/formal_full.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | variance/runs/HDFS/r2/repeat.json (withheld there too) |
| `variance/driver_versions/rep_v1_smoke.py` | host | yes | not listed (machine-specific path replaced in this copy) | - |
| `variance/driver_versions/rep_v2.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | variance/runs/HPC/r2/repeat.json (withheld there too) |
| `variance/rep.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | variance/runs/HDFS/r2/repeat.json (withheld there too) |

## Time stamps and time-tagged names in this copy

The execution host kept its clock in a non-UTC local zone, and several tracks wrote local wall-clock times. So that this
copy does not reveal that zone, every such local time in the R76 records, READMEs and names was converted to the same
instant in UTC. Only these strings and names changed; every other value (including all `*_unix` fields and all
hashes) is unchanged, and no record lists a hash of a file changed here.

* `variance/runs/<D>/r{2,3}/repeat.json` (8 files) and `unseen/runs/<D>/r1/repeat.json` (4 files): the 60 per-synthesis
  `started` strings (each recorded next to its `started_unix`) and the repeat's `created`, `started`, `finished` and
  `restarts[].at` were naive local times (`YYYY-MM-DD hh:mm:ss`, written by `variance/rep.py` and `unseen/rep.py`); they
  are now ISO 8601 UTC strings (`YYYY-MM-DDThh:mm:ss+00:00`). The same conversion was applied to
  `variance/queue_status.json`, `unseen/queue_status.json`, `unseen/summary.json` (`written`) and
  `unseen/baselines/optional_status.json` (123 strings in 16 files). The drivers that wrote them still format local
  time; their code is unchanged in this respect.
* `timing/`: the session tag (`YYYYMMDDhhmmss`) in the trial directory names under `timing/runs/T2/`,
  `timing/runs/T3/`, `timing/runs/smoke/T2/` and `timing/runs/smoke/T3/` and in `timing/runs/T2/DESIGN_<tag>.json` and
  `timing/runs/T3/DESIGN_<tag>.json` is the UTC time (the driver used the local clock); every reference to it
  (`timing/README.md`, `timing/runs/T2/INCOMPLETE.md`, `timing/runs/T3/SUMMARY_T3.json` `trial_dirs`, and the `_dest`
  and argument paths inside the trial records) was renamed accordingly.
* `unseen/baselines/logreducer/manifest_<tag>.json` and `unseen/baselines/logreducer_probe/out/manifest_<tag>.json`
  (`baselines/logreducer/lr_run2.py` names them `YYYYMMDDThhmmss` in local time): the tag is the UTC time, equal to
  `created_at` inside.
* `baselines/logreducer/full/manifest_<tag>.json` (the four manifests of the resumed invocations; `lr_run2.py` names them
  `YYYYMMDDThhmmss` in local time): the tag is the UTC time, equal to `created_at` inside. `baselines/logreducer/logs/after_t2_lr_tb.log`:
  the two lines that `after_t2_lr_tb.sh` writes with the local time (`date "+%F %T"`) now begin with ISO 8601 UTC
  strings (`YYYY-MM-DDThh:mm:ss+00:00`); the driver logs next to it already wrote UTC.
* `baselines/lognexus/smoke/manifest_<tag>.json` and `baselines/lognexus/full_paper/{rest,tb,win}/manifest_<tag>.json`
  (`baselines/lognexus/ln_run.py` names them `YYYYMMDDThhmmss` in local time): the tag is the UTC time, equal to
  `created_at` inside.
* `delogcheck/experiment_results.csv`: the `Timestamp` column that DeLog writes in local time.
* `safety/README.md`: the clock times of the V1 and V2 runs.

## Not included in this copy

None of these is read by the analysis generators; the numbers come from the included records.

* Third-party sources, builds and binaries: the DeLog sources and binaries of `codecs/delog/official_rebuild/` and
  `codecs/delog/generic/`, the zstd release tree `codecs/tools/`, `baselines/{denum,lognexus,logreducer,logshrink}/src*`, `bin/`,
  `pydeps/`, the LogShrink copy `baselines/logshrink/code_r76/` (only our two added files would be ours; they are
  re-created by `baselines/logshrink/adapter/make_restore_r76.py`) and the virtual environments.
* Logs and their excerpts: `timing/samples/`, `unseen/raw/`, `unseen/train/`, `unseen/r69root/`, the training samples of
  the synthesis runs, `baselines/logreducer_probe/USask_block2.log`, and the inputs of `delogcheck/`.
* Archives, block spools, work and scratch directories, run logs (`logs/`, `*.log`; included only for the resumed
  LogReducer invocations, `baselines/logreducer/logs/`, and for the safety coverage extension,
  `safety/coverage_20261002/*.log`, which hold only status and summary lines), the per-candidate storage-fit work
  files (`tc/storage/`), the copies of each evaluated plan (`eval_*/plan.json`; `eval_cache.json` identifies every
  evaluated plan by its SHA-256 and rule tags) and the raw HTTP records of the model calls (`_api_records/`: raw request
  and response bodies and the gateway's HTTP response headers; prompts and parsed model output are in the cache files).
* `library/scratch_trace/` (a pre-gate compile-trace check, described in `library/README.md`).
* `codecs/full_20260929/results.json` (11,407,068 bytes): an aggregate of the included per-trial `result.json` files.

## Withheld verifier-repair prompts of the unseen-source runs

The verifier-repair prompt and response caches (`llm_caches/*/shared/_verifier_repair/`) of the four unseen-source
runs are not included. Their executed counterexamples print rendered timestamps in the execution host's local time,
which would reveal that zone. The syntheses' proposals, the final replay plans, gate and pool records, published
programs and all formal results are included; the withheld files are training-time repair exchanges only.
