# Session T2: incomplete (stopped by design), not used by any table

Session T2 (the new baselines and the SemZip/DeLog anchors on the twelve smaller files; 216 trials planned, see
`DESIGN_20261001115239.json`) was started on 2026-10-01 after session T3 and stopped by design 23 trials in: the
pre-registered unseen-source track had priority on the shared host (`../../../DEV_DESIGN_R76_zh.md`, section R76-G,
"资源"). The stop is the last entry of the timing chain log (not included); `../STATUS.json` is the driver
heartbeat at that moment (trial 23, Zookeeper LogReducer+R, repeat 1, attempt 1, which has no result).

This directory holds, for completeness only:

- `DESIGN_20261001115239.json`: the design of this T2 run; `DESIGN_20260929210818.json`: the design written by the
  first formal launch on 2026-09-29, which was stopped before any trial ran (see `../../README.md`, "Notes").
- one `result.json` per finished trial attempt, trials 0-22 of repeat 1 (Linux, Proxifier and Apache complete for
  all six methods; Zookeeper for five), including the attempts that the driver itself marked contaminated and
  retried, and the SemZip decode summaries.

There is no `SUMMARY_T2.json`: no method has three repeats on any file. `analysis/r76/r76_tables.py` reads only
`SUMMARY_T2.json` for its Session B column and therefore omits that column; the Small column of the speed table is
the earlier serialized session A (`evidence/selection/results/timing/`). No number in the paper comes from this
directory.
