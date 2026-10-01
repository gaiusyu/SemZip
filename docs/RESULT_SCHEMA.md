# Table and figure interface

The validated final records are shipped in `results/final/`. The lightweight
report interface accepts a JSON object with `runs`, a list containing one record
per dataset/method/trial.
All runs, including failures, must remain present. Fields:

| Field | Meaning |
|---|---|
| `dataset`, `method`, `trial` | Stable identifiers; trial is an integer |
| `status` | `PASS` or an explicit failure state |
| `raw_bytes`, `archive_bytes` | Full original bytes and complete archive bytes |
| `sha_pass` | Independent materialized full-output verification |
| `encode_process_wall_seconds`, `decode_process_wall_seconds` | Common outer process-wall scope |
| `timing_kind` | `formal` for the serialized repeated campaign; otherwise `diagnostic` |
| `fallback_blocks` | Fixed semantic-recovery count, or null for a method without that branch |

Do not map historical inner timers to process-wall fields. Before aggregation,
all successful trials for a dataset/method must agree in raw and archive size.
The report interface emits per-run CSV and a summary table of complete-archive
ratio and median process-wall throughput. Figures use only supplied successful
size rows; failures remain in the CSV and summary table. Formal speed medians
are omitted unless every supplied trial in that group passes and uses the
formal scope. The strict integration pipeline enforces the fixed dataset/method/repetition
inventory before producing the shipped result. The lightweight report entry
was also run on the actual 216 observations and produced 72 groups.

```sh
python3 scripts/report.py --input results/final/final_runs.json --output generated
python3 scripts/report.py --input results/final/final_runs.json --output figures --plots
```

The optional `--plots` path writes a standalone PDF and SVG compression-ratio
plot with a logarithmic y-axis. Python's Matplotlib must be installed. The script
does not synthesize, select, or alter measurements.

## Offline cost fields

`metadata/training_summary.json` reports original synthesis subprocess time,
which excludes counting and sampling, and corrected storage-fit process time.
`metadata/sampling_replay_summary.json` reports a separate zero-API replay of
those sampling stages with exact sample hashes; it is not original measured
sampling duration. Original historical train-plus-old-fit wall time must remain
a distinct measurement. Do not silently sum mixed runs into a claimed observed
end-to-end original training time.
