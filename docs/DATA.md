# Exact input reconstruction

Use the raw archives linked by the official
[Loghub repository](https://github.com/logpai/loghub) and
[Zenodo record 8196385](https://zenodo.org/records/8196385). The GitHub repository's
small parsing samples are not the complete benchmark inputs.

`metadata/raw_datasets.json` records the public archive URL and SHA-256, ordered
member names and individual hashes, final raw SHA-256 and byte count. The names
used here are Android_v1, HDFS_v1 and SSH (reported as OpenSSH).

`scripts/prepare_data.py` reads only the listed members. It writes their exact
bytes in manifest order and adds no separator. Hadoop uses 978 log members and
OpenStack uses three; other selection details are in the manifest. It verifies
the download, every selected member and the final combined file. Existing raw
files are not overwritten. Failed partial outputs are retained for diagnosis.

An LF byte terminates a physical record. CRLF remains CRLF, an unterminated final
record remains unterminated, embedded NULs remain bytes, and no Unicode decoding
is used during assembly. A published line-count label can differ from this
definition; the exact file hash and original LF boundaries determine the input.

All original data remains governed by its public source terms. Follow Loghub's
citation and research-use instructions. Full raw benchmark files are not
shipped here; the separate offline-evidence subtree includes saved training
samples.

## Historical source-study inputs

These R71 assembly instructions do not cover the older Q1/Q2 study inputs.
[Source-study access notes](../source_study_evidence/SOURCE_ACCESS.md) give the
official LogBench README-to-Drive full-size archive entry. Its current remote
payload was not downloaded or hashed; the historical local SHA is a required
identity, not a verified current-server match.

The older Q2 Spark file also has a distinct size and SHA. A separate full-content
control verifies that its exact bytes are reconstructed by the same lexical
original members with one extra LF after every member, including the last.
Both candidate whole-file hashes and all 3,852 historical member boundaries
match. This establishes an equivalent recipe, not discovery of the original
historical script or a fresh remote-download validation. See the
[control evidence and reusable verifier](../source_study_evidence/spark_assembly_control/README.md).
Do not use R71's no-separator assembly as a substitute for that historical input.
