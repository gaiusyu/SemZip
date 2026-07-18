# SemZip V6 Result Status

The authoritative method ID for this directory is:

```text
SEMZIP-DELOG-ALLGROUP-OR20-ADMISSION-V6-20260718
```

## Files

- `semzip_v6_measured.csv` contains only strict measured rows. Every row used
  original raw logs, 100k-line blocks, 8 workers, complete archives, all-block
  restoration, and full-file SHA-256 verification.
- `semzip_v6_planning_table.csv` fills the intended 16-dataset table. Rows with
  `status=ESTIMATED` are planning placeholders, not benchmark results.

For estimated rows, `archive_bytes` is intentionally empty. The separate
`projected_archive_bytes` column is arithmetic projection from the stated raw
size and estimated ratio. `sha_status=PENDING` makes clear that no V6 archive
or restoration evidence exists yet.

## Measured Provenance

| Evidence ID | Datasets | Protocol |
| --- | --- | --- |
| `semzip_v6_main_batch1_provenance_20260718` | Linux, Proxifier, Apache, Zookeeper | Empty-cache cold discovery, frozen replay, three speed trials, full SHA. |
| `semzip_v6_main_batch2_provenance_20260718` | HPC, HealthApp, Mac, BGL | Empty-cache cold discovery, frozen replay, three speed trials, full SHA. |
| `semzip_delog_admission_diagnosis_20260718` | Android, OpenSSH | Controlled frozen-plan V6 comparison, full SHA. |
| `semzip_delog_more3_20260718` | HDFS | Controlled frozen-plan V6 extension, full SHA. |

The paper-facing source used for the provenance campaigns had commit
`6ba4f761f9d34fe43d5821c90f7cd45bd6263881` and configuration SHA-256
`0052aff02b1cd5a25bbb8181da0503ccec7882a3436096ad5071f9f4530c008b`.

## Aggregate of Measured Rows

Only the 11 measured rows are aggregated:

```text
raw bytes:             2,681,273,071
complete archive bytes:   80,348,490
corpus-wide ratio:            33.3705x
geometric-mean ratio:         51.4391x
full SHA:                 11/11 PASS
```

Do not aggregate estimated rows with measured rows in a paper claim. Replace
each estimated row with a cold-start V6 run and full SHA evidence first.
