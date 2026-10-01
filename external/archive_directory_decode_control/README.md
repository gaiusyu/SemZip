# Supplementary directory-only decode capability

All eight existing fixtures (11 archive blocks) decoded successfully with one worker and independent full-output SHA/byte/record verification. No new encoding, model calls or benchmark timing occurred. The child received an archive directory and fixed decoder/method configuration; it received no raw-input path, original SHA or encode-phase ledger. The parent compared materialized decoded output afterward.

The fixtures cover five tiny codec examples plus three two-block gzip/XZ/Zstd examples with an unterminated tail. This is finite capability evidence, not a claim about all inputs or malformed archives. Known LogLite NUL failures remain documented elsewhere. LogLite means the fixed LogLite-BL-wide+tail+reservefix adaptation.

**The frozen formal CLI still opens `encode_phase.json` as an orchestration index.** It uses only block index and archive filename, which this supplementary helper reconstructed from fixed numbered filenames and directory membership. This control did not change that CLI or relabel formal timing as a different implementation. Its child interface and source audit establish input independence; no physical filesystem sandbox was imposed.

Complete archive cost follows the predeclared multi-file convention: actual native file lengths, including headers/footers and the LogLite tail flag, are counted. Directory entries/filenames and an outer transport container are excluded. SemZip's actual manifest files remain counted. The qualification is retained rather than treating a wrapper ledger as compressed payload.

`evidence.json` preserves all eight numerical/hash rows, fixed binary and source hashes, original evidence file hashes, interface booleans and diagnostic-only durations. Private commands, PIDs, executable paths and settings are omitted. The original helper is identified by hash but not copied with private paths into this public evidence subtree.
