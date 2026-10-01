#!/bin/bash
# Wait for the timing chain (pid 805, now in T2), then resume LogReducer Thunderbird with the maximum 4 workers.
cd "$(dirname "$0")"
while kill -0 805 2>/dev/null; do sleep 60; done
rm -f full/RUNNING.lock
echo "$(date "+%F %T") LR TB resume (4 workers)"
python3 lr_run2.py --output full --workers 4 --resume --input Thunderbird=<WORKDIR>/data/loghub1/original/Thunderbird/Thunderbird.log > logs/full_resume_tb3_20261001.log 2>&1
echo "$(date "+%F %T") LR TB rc=$?"
