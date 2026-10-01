# Verified historical Spark reconstruction recipe

The full-content control passes. Reading the public archive's `.log` members
in archive-relative lexical path order and appending **one additional LF after
every member, including the last**, reproduces the exact historical Q2 Spark
bytes and SHA-256. Original member bytes, including their existing terminal
LFs, are preserved. This is an equivalent reconstruction recipe; the original
historical assembly script was not found.

| Direct archive stream | Bytes | Complete SHA-256 |
|---|---:|---|
| Original member bytes, no separator | 2,941,224,304 | `cefaf6e396850cfa5efed051ecb9b3636dd4048b5f230e15c33bfdfc937267bb` |
| Original member bytes plus one LF per member | 2,941,228,156 | `f4aca8610fc015b4c33dd88865608e127adf44a713d0379447ee537e5da880d5` |

## Evidence and boundaries

The already-existing local `Spark.tar.gz` was fully hashed: 183,474,743 bytes,
SHA-256 `f11e5df5a98ce25d0f80adb7631b022ae6f1193f510079387447ef4e0fdccef7`,
MD5 `31ddaff179f6c1ae5203770138156b17`. These match the preserved public-archive
ledger. The recorded public entry is [Spark.tar.gz on Zenodo](https://zenodo.org/records/8196385/files/Spark.tar.gz?download=1).
The current remote payload was not downloaded or revalidated by this control.

All 3,852 actual member sizes, SHA values, newline counts and final LF states
match the R71 member ledger. Actual tar order is lexical, so both complete
candidate hashes were accumulated directly from the original tar payload
stream. A separate complete read of the historical file verifies its SHA,
all 3,852 member payload hashes, each following LF and the absence of trailing
extra bytes. Thus the 3,852-byte difference is now explained by full-content
evidence, not inferred from equal counts.

`summary.json` retains both candidates and all outcomes. The two member ledgers
and `verify_spark_assembly.py` retain their exact original bytes. `PROVENANCE.json`
maps original to exported identities. The summary omits machine inode/mtime
values; the original command containing private absolute paths is not shipped.
The original Q2 records and input files remain unchanged.

Ordinary gzip/tar inflation was required to read the source archive. There were
zero compression-experiment invocations, model calls, network accesses or large
assembled output files. The recorded read duration is diagnostic, not a codec
throughput measurement. Neither this control nor packaging reran Q2 matching,
representation coding or compression.

## Verify the shipped metadata

From the artifact root:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 source_study_evidence/spark_assembly_control/verify_saved_control.py
python3 source_study_evidence/verify_source_study.py
```

These commands check the saved evidence and its bindings to both input ledgers.
They do not recompute the large-file hashes. The entire subtree is covered by
`source_study_evidence/FILES.json` and the root `MANIFEST.json`.

## Repeat the full-content control separately

Supply the archive and historical input separately, with a new output directory:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 source_study_evidence/spark_assembly_control/verify_spark_assembly.py \
  --archive inputs/Spark.tar.gz --historical inputs/Q2/Spark.log \
  --reference metadata/raw_datasets.json --output ../spark-assembly-control
```

This is the unchanged executed verifier. Its paths are explicit and portable;
it makes no network request and does not write an assembled corpus. It requires
both existing inputs to repeat the independent historical-file check. The
reconstruction rule above describes how a public-archive input can be assembled
before that check; verify its complete SHA rather than substituting R71 Spark.
This full-content command was not rerun when exporting the anonymous evidence.
