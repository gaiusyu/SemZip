# Official input access and verification limits

Updated 2026-09-23 after the separate full-content Spark assembly control. This
note documents public entry points and input identities. Packaging itself did
not download or reread large inputs or rerun a study. The separate Spark control
did fully read the existing local archive and historical file, as described
below; Q1's remote payload remains unchecked.

## Q1: full-size LogBench-O source archive

Use the authors' [LogBench README, full-size download section](https://github.com/logpai/LogBench#download-original-crawling-logging-dataset).
It links the full collected dataset to this [officially linked Drive file](https://drive.google.com/file/d/13EV-rIFEwVrLGnpNIcpF3u9NSOh_gCNM/view?usp=sharing).
The source-input lookup recorded the Drive title as `LogBench-O.zip`. This full
collection is distinct from the small prefix ZIPs listed in the GitHub tree.
The [raw official README](https://raw.githubusercontent.com/logpai/LogBench/main/README.md)
was rechecked for this documentation update; the Drive preview was not
independently retrieved during this second check.

The preserved historical local archive has the following identity:

| Property | Historical local value |
|---|---|
| Bytes | 264,750,591 |
| SHA-256 | `5ce32ed761b451e6dd9566a892a10a6f7c261740f1ef61a45adbdce0e06ecf92` |

The official page does not provide that SHA. **The current remote payload was
not downloaded or hashed.** The historical local digest is the required
reproduction identity, not proof that the currently linked remote bytes match.
Neither the file title, rounded advertised size nor a successful HTTP/HEAD
request establishes byte identity. A successful complete download is not
certified by this note.

After obtaining the archive separately, verify its exact byte count and SHA-256
against the table before using it as the Q1 input. The census commands in
`README.md` require those exact bytes. No full-census rerun was performed while
preparing the anonymous package.

## Q2: verified recipe for the distinct historical Spark input

The public collection is [LogHub](https://github.com/logpai/loghub), whose
[Spark README](https://raw.githubusercontent.com/logpai/loghub/master/Spark/README.md)
describes its release. The corresponding public archive entry is
[Spark.tar.gz on Zenodo](https://zenodo.org/records/8196385/files/Spark.tar.gz?download=1).
Those public references alone do not establish the older single-file assembly.
A subsequent [full-content control](spark_assembly_control/README.md) now verifies
a public-archive reconstruction recipe whose bytes exactly match that input.

| Input | Bytes | Recorded SHA-256 |
|---|---:|---|
| Historical Q2 Spark | 2,941,228,156 | `f4aca8610fc015b4c33dd88865608e127adf44a713d0379447ee537e5da880d5` |
| R71 main Spark | 2,941,224,304 | `cefaf6e396850cfa5efed051ecb9b3636dd4048b5f230e15c33bfdfc937267bb` |

The older identity is retained in `q2/run_manifest.json` and the historical
summaries. The R71 identity and ordered member identities are separately
recorded in `../metadata/raw_datasets.json`. R71's documented assembly joins
the exact bytes of 3,852 `.log` members in archive-relative lexical path order,
without adding separators; the saved member metadata records an ending LF for
each member. For the historical input, preserve those exact member bytes and
append **one additional LF after every member, including the last**.

The control fully hashed an already-existing local public-source archive and
confirmed its recorded archive SHA/MD5 and all 3,852 member sizes, hashes and LF
states. Its actual tar order is lexical. Both complete candidate hashes were
accumulated directly from the tar member bytes; the extra-LF candidate matches
the historical identity above. A separate full historical-file read confirmed
its whole SHA and every member payload plus following LF, with no extra tail.
This full-content evidence explains the 3,852-byte difference; the earlier
metadata-only length comparison was insufficient on its own.

**The verified finding is an equivalent byte-exact reconstruction recipe, not
discovery of the original historical script.** The current remote archive was
not downloaded or revalidated by this control. It read a local copy with the
recorded public-archive identity, using ordinary gzip/tar inflation, and ran no
compression experiment or Q2 rerun. Existing input files were not modified.

Before reproducing Q2, verify the assembled historical input against its exact
SHA above. Do not substitute R71 Spark or transfer its coverage statistics. The historical whole-file
delta/varint diagnostic also remains separate from R71's strict 100,000-record
block contract.

## What the package verifiers establish

`verify_source_study.py` checks the saved records, file identities and arithmetic;
`scripts/audit_bundle.py` at the package root checks the package manifest and
targeted credential patterns. The new `spark_assembly_control/verify_saved_control.py`
also checks the saved member and boundary ledgers against both package input
identities. These metadata verifiers do not download remote payloads, check
current server bytes, reread the large Spark inputs or rerun the source study.
The original full-content verifier is supplied separately with explicit path
arguments for an opt-in repeat. Original Q2 evidence, code and experimental
inputs remain unchanged.
