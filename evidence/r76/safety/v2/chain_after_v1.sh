#!/bin/bash
# R76-E V2: wait (read-only liveness check, no signal) for the V1 full verify (pid 1258137, started by this track)
# to exit, then run the V2-enforced archive-only decode of all 16 R73 pool archives, 3 workers, largest last.
set -u
cd <WORKDIR>/r76_additional_20260929/safety/v2
while kill -0 1258137 2>/dev/null; do sleep 30; done
echo "V1 finished; starting V2 at $(date)"
exec python3 -u run_verify.py all --root full_v2_20260929 --workers 3
