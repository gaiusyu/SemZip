# R67 cold observer replay

PASS: every saved B1/B2 coverage field, eligible-group count and complete group object is unchanged after explicit cold cache/callable reset. All seven novelty/size/combined trigger decisions are identical.

This is an observer-only control with one worker, zero model calls and no new encoding, decoding, storage fitting or candidate generation. Original blocks, plans, group/decision records and source hashes remained unchanged. Newly measured control seconds are diagnostic and do not replace historical timing.

| Dataset | B1 eligible | B2 eligible | Trigger |
|---|---:|---:|---|
| HPC | 24 | 7 | True |
| OpenSSH | 3 | 4 | True |
| BGL | 26 | 17 | True |
| Android | 300 | 333 | True |
| Windows | 11 | 13 | True |
| Spark | 70 | 37 | True |
| HDFS | 9 | 9 | False |

The R67 semzip_pure, pare_dataset_extract and semantic_codec_frontend module bytes match the R69 runtime reviewed with the frozen boundary helper. The control verifies that the helper binds the actual modules used by observe.py, clears all listed input-derived LRUs and mutable callable dictionaries at entry/exit, and verifies empty caches after reset. Immutable program/regex caches remain as specified by the helper.

This finite equality control does not retroactively change the original campaign into the R71 strict process protocol or establish arbitrary future cache independence. It establishes equality for the exact fourteen observed blocks and original plans.
