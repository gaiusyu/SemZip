# Superseded timing campaign and inventory correction

The original V2 campaign is preserved as 216 scheduled rows: 29 successful but superseded observations, two post-decode metadata-audit failures, one in-flight observation interrupted when the campaign stopped, and 184 observations never started. The controller originally grouped all 185 unfinished rows as NOT_RUN; the separate trial record identifies the interrupted observation. No V2 timing contributes to V3 statistics.

Both failed trials had successful encode/decode process exits. Subsequent whole-file and per-block hashes of their retained decoded outputs matched the inputs. A separate 32-file probe reproduced one strict-inventory rejection on an unchanged, closed file whose mtime moved backwards by 5 microseconds; its other identity fields were unchanged. The rejected read did not return a content hash, so the probe does not claim one.

V3 preserves the single post-decode scan and all timing boundaries, records before/after metadata, and permits only mtime differences. File size, path, device and inode must remain identical, and full content and every original block must match. Original raw-input inventory remains strict. There is no retry or delay. V3 restarts the complete 216-row schedule with the same algorithms, programs, policies and binaries.

`controls.json` retains the original observed probe, two later retained-output checks, 32 corrected-inventory content checks and substitution/truncation/append rejection controls. The three explicitly synthetic file-stat controls separately exercise acceptance of an mtime-only change and rejection of size/inode changes. They are software controls, not benchmark measurements. The corrected fresh-file checks happened to observe no mtime drift.

`PROVENANCE.json` binds original records and exact source copies by SHA-256. Private paths, process IDs, host observations and free-form traceback text are omitted from exported records. The preserved campaign/probe Python files require their original surrounding campaign layout and dependencies; they are source evidence, not standalone portable benchmark entry points. `verify.py` checks the packaged metadata and source bindings only; it does not rerun codecs, reproduce timing or authenticate remote execution.

Run from this directory: `python3 -B verify.py`.
