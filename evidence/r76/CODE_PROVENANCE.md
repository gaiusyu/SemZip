# Provenance of the R76 code files

The R76 tracks ran on the execution host. Their result records, READMEs and design files in this directory were
copied from that host. The code files listed below were taken from mirrors of the track directories and checked
against the execution records:

* **recorded**: the SHA-256 of the file as run equals a hash written by the track itself into its records
  (full hash, or the hash prefix listed in the track README); the record is named in the last column.
* **mirror**: copied from a mirror of the track directory whose `README.md` is byte-identical to the final README on
  the execution host; the track wrote no hash of this file into the records that are packaged here.

`sanitized = yes` means the file is not byte-identical to the file as run: machine-specific absolute paths in it were
replaced by placeholders such as `<WORKDIR>` or `<MOUNT>` (and, in `timing/phase_codec_r76.py`, a hash prefix of such
a file was removed from a docstring; `timing/timing_r76.py` pins the published copy of `codecs/codec_baseline_r76.py`).
The as-run SHA-256 of these files is not listed here, nor in the track records and READMEs that held it (there the
value reads "withheld" or "not listed"); all other hashes in those records are unchanged. `codecs/r68_to_r76_harness.diff` was regenerated at
packaging time with `diff` from the recorded R68 harness (`external/codec_baseline.py`) and `codecs/codec_baseline_r76.py`.
Byte-identical copies that the tracks kept of other code are not repeated here: `timing/vendor/codec_baseline_r76.py`
= `codecs/codec_baseline_r76.py`; `variance/code/*.py` = `evidence/selection/code/*.py` except `formal_full.py`, whose
only change (worker count from `FORMAL_WORKERS`) is described in `variance/README.md`.

| file | provenance | sanitized | SHA-256 as run | record |
|---|---|---|---|---|
| `baselines/denum/codec_baseline_denum.py` | recorded | no | `de4daaf45478a501963c2d958ba2460eac860ce5411854a36101fefd00121ff3` | baselines/denum/smoke_v2/manifest.json (full) |
| `baselines/denum/codec_baseline_denum_enconly.py` | recorded | no | `6c1bf68ebbc72e738d0550b37bc2190829663fd3ad045285dcd9aa907f521135` | baselines/denum/README.md (full) |
| `baselines/logreducer/lr_run2.py` | recorded | no | `bbf044c186b58bd40d0c38826df2a0a2b09add1074eee7d24024d0598c6ce549` | timing/README.md (full) |
| `baselines/logshrink/adapter/compare_decoders.py` | mirror | no | `1de59bf8c2e07b8ed60d23edbdb7ea739bce21afa4ed02c6eadac3c4450eba8d` | - |
| `baselines/logshrink/adapter/dbg_redecode.py` | mirror | no | `3de4443dd93dc53317eeab2ecd5fd445c8549f4b928f920cd25c1c87e5bd9d6a` | - |
| `baselines/logshrink/adapter/determinism_check.py` | mirror | no | `8a19bc4047a455ce3bb19c599dc281f627d062a546cc9655835dc2431d1c90a0` | - |
| `baselines/logshrink/adapter/equiv_check_runA.py` | mirror | no | `974889623e058775320e9e0d4deb33d6ded43ef2a36ebb74cac909ada8a6e919` | - |
| `baselines/logshrink/adapter/ls_run.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | timing/README.md (withheld there too) |
| `baselines/logshrink/adapter/ls_seg_runner.py` | recorded | no | `1fb277853474c6413307a74c80140a4bf2e93eea66250be2db3a8bd46f64d157` | baselines/logshrink/probe1/campaign.json (full) |
| `baselines/logshrink/adapter/ls_segment.py` | recorded | no | `121a5e74a4e473c6c51a8edadb68b9f92d50f6922db77bfbb900cab390854a76` | baselines/logshrink/probe1/campaign.json (full) |
| `baselines/logshrink/adapter/ls_status.py` | mirror | yes | not listed (machine-specific path replaced in this copy) | - |
| `baselines/logshrink/adapter/make_restore_r76.py` | recorded | no | `4083d970c631d6f58a328fe670301c3368d2129881a3f02b278c6e13b4bf3018` | baselines/logshrink/full_official/campaign.json (full) |
| `codecs/codec_baseline_r76.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | codecs/README.md (withheld there too) |
| `library/formal_full.py` | mirror | yes | not listed (machine-specific path replaced in this copy) | - |
| `library/proposer.py` | mirror | no | `8280e9f5b0ab4720d4a659bfdebac2d7b1510327616021b06d75a2940dc44ca0` | - |
| `library/report.py` | mirror | no | `c24cb2401ee093769a4a6d6fdf74f1917abacba210f178342f04f57734e47873` | - |
| `library/run_library.py` | mirror | yes | not listed (machine-specific path replaced in this copy) | - |
| `library/semzip_library_wrapper.py` | mirror | no | `9f389face717060a2afc651b69547fa6335703d955d386c66e3f392abe354134` | - |
| `library/test_proposer.py` | mirror | no | `de1af67586ad63b91698a555077e2fff5746347f4ba6fdafdba921f79449d7e1` | - |
| `library/train_lib.py` | mirror | yes | not listed (machine-specific path replaced in this copy) | - |
| `rq2/code/decode_verify.py` | recorded | no | `f24266c4b4b2c17fe8c24fcba7feb4b0279bf03c4d712365e83197c184dbc1ce` | rq2/README.md (full) |
| `rq2/code/prepare_plans.py` | recorded | no | `59845bd1e6c8b60c305bad48e63048f542f58ab5c2bc1430a820c01695d84333` | rq2/README.md (full) |
| `rq2/code/representation_control_r76.py` | recorded | no | `957577228c55539a11c0c0e7946a82e17d40a0130f73a17b7de8947c3b046999` | rq2/README.md (full) |
| `rq2/code/representation_control_v2.py` | recorded | no | `1334fd05a504e5ec3ff725c2aa163217daf02813bbe13890ff9f592156ae4208` | rq2/README.md (full) |
| `rq2/code/results_table.py` | recorded | no | `9b3ea24bf842300f0637764a625770ed56ac85ddd903e50ed53982d95076a47a` | rq2/README.md (full) |
| `rq2/code/run_all.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | rq2/README.md (withheld there too) |
| `rq2/code/runtime_cache_boundary.py` | recorded | no | `e2098cb527c652aa0cd4464cffd0a16e7e7ca6ccff2c59474fc812069adfb911` | rq2/README.md (full) |
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
| `safety/v2/enforced_decode.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/v2/full_v2_20260929/template/OpenSSH/result.json (withheld there too) |
| `safety/v2/negative_controls.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/v2/pyexec_validator.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/v2/static_check_result.json (withheld there too) |
| `safety/v2/run_verify.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/v2/scan_archives.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/README.md (withheld there too) |
| `safety/v2/static_check.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | safety/static_check_result.json (withheld there too) |
| `safety/v2/test_enforcement.py` | recorded | no | `ae8aacb31e6718a4fc1f6575ca22298d2a8ddb2423cfe73bb8da18ceb42c2fa9` | safety/README.md (prefix ae8aacb3) |
| `safety/v2/test_validator.py` | recorded | no | `bec8a8d2c810d1d756a123d74147b9df9a9eb47e9f9708727eeab8c984cea206` | safety/README.md (prefix bec8a8d2) |
| `timing/check_sample_semzip.py` | recorded | no | `4966eaf84011e39dc8741342f3911c00873a82afbe3320e5406864faa207893c` | timing/README.md (full) |
| `timing/phase_codec_r76.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | timing/README.md (withheld there too) |
| `timing/phase_lr.py` | recorded | no | `dcfc9b7178a93646db3b7577e85acdba11ba2b90620d183a4ce8222a12b63caf` | timing/README.md (full) |
| `timing/phase_ls.py` | recorded | no | `92218db24cc55277dfdfeb56edb95ce618035c9054bc7ec29ece7a191e4b5680` | timing/README.md (full) |
| `timing/timing_r76.py` | recorded | yes | not listed (machine-specific path replaced in this copy) | timing/README.md (withheld there too) |
| `variance/check_r73_repro.py` | mirror | yes | not listed (machine-specific path replaced in this copy) | - |
| `variance/rep.py` | mirror | yes | not listed (machine-specific path replaced in this copy) | - |

## Referenced in the track READMEs but not included in this copy

These files stayed on the execution host and could not be collected when this repository was assembled. None is
read by the analysis generators; the numbers come from the included records.

* `attribution/`: the plans of the original run (regenerated byte-identically in `attribution/plans/`, see
  `attribution/README.md`); the driver is the unchanged `evidence/selection/code/formal_full.py`.
* `variance/`: per repeat, the synthesis transcripts, gate/pool reports, published plans and policies, `repeat.json`,
  `code/` and `driver_versions/` (see `variance/README.md`); `library/`: the published plans and policies, gate reports,
  `scratch_trace/` and `full_chain.sh` (see `library/README.md`); `timing/`: the formal sessions T2/T3 (not finished),
  `samples/` and `vendor/`; `rq2/plans/PLAN_IDENTITY.json`.
* `codecs/full_20260929/results.json` (11,407,068 bytes): an aggregate of the included per-trial `result.json` files.
* `codecs/`: `summarize.py`, `check_generic_tags.py`, `delog/generic_vs_official.compressor.diff`,
  `delog/VERIFY_generic_vs_official.txt` (DeLog-generic = official DeLog with an empty `regex_map`, README section 4).
* `baselines/denum/`: `prove_lossy.py`, `check_enconly_smoke.py` (their outputs `proofs/SUMMARY.json` and the smoke
  checks are included).
* `safety/v2/chain_after_v1.sh`; `library/FROZEN_SHA256.txt`; `variance/code/SHA256SUMS.r73_originals`.
