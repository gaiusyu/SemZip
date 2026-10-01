# Saved-response boundary replay

The uniform local UTC0/C run retains all sixteen outcomes: 14 exact plan matches, Linux and Mac differ. All original source, samples, exchanges and deployed programs remain unchanged. No new API call, complete archive or storage-policy fit occurred.

Each dataset ran in a fresh local Python 3.9.6 process. The hook matches the complete original prompt, parses the saved response using the original parser logic, and returns a copied payload. Unmatched prompts raise; the original compiler may catch that repair error, so the control explicitly retains unmatched hashes and marks the resulting plan different. It does not substitute or generate a response. The original plan-writing function emits the plan before the control stops; sample auxiliary main-core files are written by the original trainer prefix.

| Dataset | UTC0 result | Consumed / recorded replies | Plan bytes match |
|---|---|---:|---|
| Linux | DIFFERENT | 13/17 | False |
| Proxifier | PASS_IDENTICAL | 3/3 | True |
| Apache | PASS_IDENTICAL | 16/16 | True |
| Zookeeper | PASS_IDENTICAL | 17/17 | True |
| Mac | DIFFERENT | 4/28 | False |
| HealthApp | PASS_IDENTICAL | 4/4 | True |
| HPC | PASS_IDENTICAL | 16/16 | True |
| Hadoop | PASS_IDENTICAL | 17/17 | True |
| OpenStack | PASS_IDENTICAL | 11/11 | True |
| OpenSSH | PASS_IDENTICAL | 6/6 | True |
| Android | PASS_IDENTICAL | 9/9 | True |
| BGL | PASS_IDENTICAL | 8/8 | True |
| HDFS | PASS_IDENTICAL | 6/6 | True |
| Spark | PASS_IDENTICAL | 18/18 | True |
| Windows | PASS_IDENTICAL | 22/22 | True |
| Thunderbird | PASS_IDENTICAL | 4/4 | True |

## Environment explanation

All 28 unmatched original repair records (4 Linux, 24 Mac) contain year-1900 timestamp counterexamples. Their saved microseconds differ from explicit UTC0 by one constant offset and match the historical (year-1900) offset of one fixed non-UTC zone. The zone name and the offset are withheld in this anonymous copy (double-blind review); the records read `<NON_UTC_ZONE>`. The read-only `timezone_feedback_diagnosis.json` lists every compared record (dataset, call, request SHA-256, counterexample) with its verdict. All proposal prompts matched; the divergence occurs in executed-counterexample feedback for repairs.

The original R69 training launcher does not set TZ/LC_ALL or call the execution-environment activator. Its runner copies the inherited environment and sets only the hash-seed/fixed-codec defaults. The saved training design contains no captured process timezone. Thus the original process timezone is not directly pinned by the training record. Numeric consistency with that zone is evidence about the counterexamples, not a substitute for a saved original environment snapshot. No current remote timezone is treated as original historical proof.

Two additional explanatory local runs in a new directory changed only the explicit control timezone to that non-UTC zone. Both Linux and Mac consume every recorded response and reproduce the original parsed plan and exact file SHA. The original UTC0 14/16 results remain preserved. These two diagnostic passes are not pooled into a uniform sixteen-file UTC0 success claim, nor do they erase the missing training-time timezone pin.

The four originally unconsumed Linux repairs change the eventual tag numbering; Linux still has eight saved entries but differs in the plan object. Mac similarly lacks original repairs in UTC0. This was not attributed to compression performance or repaired by selecting new candidates. API timeouts are bypassed at the response-content boundary; no timeout caused the observed mismatches.

This validates the model-response-content boundary and compiler behavior under the stated environment, not HTTP transport replay, an immutable model-provider version, or replay of the complete original sampling + network + fitting pipeline. The exact saved sample is the input; the independently audited sampler remains separate.

## Run using only this package

From the artifact root (Python 3.9 or newer, Unix `time.tzset`; for the two
explanatory conditions also the system timezone database):

```sh
PYTHONDONTWRITEBYTECODE=1 python3 validation_controls/fresh_response_boundary/tools/run_suite.py \
  --output ../response-boundary-control
```

The training samples are not redistributed: first fetch the LogHub files
(`scripts/prepare_data.py`) and rebuild the samples with
`python3 offline_evidence/tools/rebuild_samples.py --raw-dir raw`, which verifies
each rebuilt sample against its recorded SHA-256.

The output must be new and outside the artifact. The wrapper resolves source,
samples, exchanges and expected programs relative to the package; `--artifact-root`
can override that root. It sequentially runs the fixed sixteen UTC0 conditions
and the two separate non-UTC conditions, each in a fresh process with a
120-second bound. The non-UTC conditions need the zone's IANA name in the
environment variable `RESPONSE_BOUNDARY_NON_UTC_ZONE` (withheld in this anonymous
copy); without it they are reported as `NOT_RUN_ZONE_NOT_GIVEN` and a successful
run ends with `PASS_PRIOR_UTC0_OUTCOMES_REPRODUCED` instead of
`PASS_PRIOR_OUTCOMES_REPRODUCED`. Network entry points are disabled by the hook; unmatched
prompts raise instead of requesting a new answer. It stops at the unchanged
original plan writer, before complete archive creation or policy fitting.
Sample auxiliary core files and complete diagnostic logs remain in the output.

`package_replay_summary.json` records a real local run using the packaged source
and evidence. All eighteen outcomes, prompt consumption records and plan
hashes match the prior controls: 14 identical and two different under UTC0;
two identical under the separate non-UTC condition. The suite's PASS means
**those complete prior outcomes were reproduced**, not that all sixteen UTC0
plans matched. Source, deployed programs and input evidence remained unchanged.
Diagnostic elapsed times are not original training times or benchmark results.
That run used the suite before the zone name was moved to the environment
variable; in this copy the zone name in `results.json` and
`package_replay_summary.json` reads `<NON_UTC_ZONE>`, and the recorded SHA-256 of
the files edited for this (`results.json`, `tools/replay_explicit_timezone.py`,
`tools/run_suite.py`) reads "withheld".
