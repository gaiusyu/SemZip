#!/bin/bash
# After the 2026-10-01 container restart: wait for the resumed LogReducer Thunderbird run (pid $1), redo the
# LogReducer Zookeeper resume interrupted at 00:05Z, then start the unchanged, resumable formal timing driver.
cd "$(dirname "$0")"
while kill -0 $1 2>/dev/null; do sleep 60; done
echo "$(date "+%F %T") LogReducer TB resume exited; redoing Zookeeper"
(cd <WORKDIR>/r76_additional_20260929/baselines/logreducer && rm -f full/RUNNING.lock && python3 lr_run2.py --output full --workers 4 --resume --input Zookeeper=<WORKDIR>/data/loghub1/original/Zookeeper/Zookeeper.log > logs/full_resume_zk_20261001.log 2>&1)
echo "$(date "+%F %T") Zookeeper rc=$?; starting timing_r76.py formal"
rm -f runs/DRIVER.lock
exec python3 -u timing_r76.py formal
