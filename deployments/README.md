# Frozen deployments

| Directory | Deployment | Index (hashes, replay selector) |
|---|---|---|
| `main_pool/<Dataset>/` | **SemZip (paper main result)**: five block-0 syntheses, per-synthesis quality gate, cost-guided selection across syntheses; storage policy fitted on block 0 | `metadata/deployments_main_pool.json`, `replay.py --deployment-set main` (default) |
| `<Dataset>/` | SemZip-1: the single greedy (T=0) synthesis, without gate or selection | `metadata/deployments.json`, `replay.py --deployment-set semzip1` |

`program.json` is the synthesized extraction plan (verified programs, regular expressions, store layout);
`storage.json` is the fixed storage policy read by the guarded runtime during encoding only (the decoder never
reads it); `publication.json` (main) records the plan/policy hashes and the block-0 training input.
The policies differ from the files used in the runs only in the free-text `proposal_source` field, which was edited in
this copy and is not read by the runtime (it reads only `recipe`, `programs`, `column_numeric`, `numeric_modes`,
`subfield_ids` and `online_search`), so archives are unaffected; `storage_sha256_as_run` gives the hash as run.
