#!/bin/bash
# 2026-10-01 05:40 UTC, after the second container restart. Priority order: T3 timing (large files) -> LogReducer TB/ZK
# resume (8 workers, machine otherwise idle) -> T2 timing. Each step is resumable; summaries are written per session.
cd "$(dirname "$0")"
rm -f runs/DRIVER.lock
echo "$(date "+%F %T") T3 start"; python3 -u timing_r76.py run T3; python3 -u timing_r76.py summary T3; echo "$(date "+%F %T") T3 done"
cd <WORKDIR>/r76_additional_20260929/baselines/logreducer && rm -f full/RUNNING.lock
echo "$(date "+%F %T") LR TB resume"; python3 lr_run2.py --output full --workers 8 --resume --input Thunderbird=<WORKDIR>/data/loghub1/original/Thunderbird/Thunderbird.log > logs/full_resume_tb2_20261001.log 2>&1; echo "$(date "+%F %T") LR TB rc=$?"
rm -f full/RUNNING.lock; python3 lr_run2.py --output full --workers 4 --resume --input Zookeeper=<WORKDIR>/data/loghub1/original/Zookeeper/Zookeeper.log > logs/full_resume_zk_20261001.log 2>&1; echo "$(date "+%F %T") LR ZK rc=$?"
cd <WORKDIR>/r76_additional_20260929/timing && rm -f runs/DRIVER.lock
echo "$(date "+%F %T") T2 start"; python3 -u timing_r76.py run T2; python3 -u timing_r76.py summary T2; echo "$(date "+%F %T") T2 done; CHAIN DONE"
