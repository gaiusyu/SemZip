#!/bin/bash
# Wait until the R76 long runs (LogReducer/LogShrink/codecs/library Thunderbird, Denum encode-only) have exited, then
# start the formal timing driver so that its contamination retries are not spent while those runs are still active.
cd "$(dirname "$0")"
for p in 1225310 1245963 1396946 1463284 731978; do
  while kill -0 $p 2>/dev/null; do sleep 60; done
  echo "$(date '+%F %T') run $p exited"
done
echo "$(date '+%F %T') all long runs exited; starting timing_r76.py formal"
exec python3 -u timing_r76.py formal
