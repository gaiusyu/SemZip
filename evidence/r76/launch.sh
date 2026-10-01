#!/bin/bash
# usage: [AGNICE=n] launch.sh LOGFILE cmd args...   (detached; survives ssh exit; runs in the CURRENT directory)
# setsid gives the job its own autogroup; AGNICE sets the autogroup nice (0..19) so it yields CPU to other groups.
export PYTHONHASHSEED=0
log=$1; shift
mkdir -p "$(dirname "$log")"
setsid nohup "$@" > "$log" 2>&1 < /dev/null &
pid=$!
if [ -n "$AGNICE" ]; then
  for i in 1 2 3 4 5; do sleep 1.2; echo "$AGNICE" > /proc/$pid/autogroup 2>/dev/null && break; done
fi
echo "launched pid $pid -> $log"
