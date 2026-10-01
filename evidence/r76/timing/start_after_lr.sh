#!/bin/bash
# After the 2026-10-01 container restart: wait for the resumed LogReducer Thunderbird decode (pid given), then start
# the unchanged formal timing driver (resumable; no trial had run before the restart).
cd "$(dirname "$0")"
while kill -0 $1 2>/dev/null; do sleep 60; done
echo "$(date '+%F %T') LogReducer resume (pid $1) exited; starting timing_r76.py formal"
rm -f runs/DRIVER.lock
exec python3 -u timing_r76.py formal
