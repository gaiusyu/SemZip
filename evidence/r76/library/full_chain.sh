#!/bin/bash
# R76-B2 full run: wait for the smoke driver (pid 1235281) to exit, then all 16 datasets small -> large, one at a time.
cd <WORKDIR>/r76_additional_20260929/library
export R76_GATE_THREADS=3 R76_FORMAL_WORKERS=3 PYTHONHASHSEED=0 PYTHONDONTWRITEBYTECODE=1
while kill -0 1235281 2>/dev/null; do sleep 30; done
echo "smoke driver finished; starting full run $(date)"
python3 -u run_library.py train,gate,publish,formal Proxifier,Linux,Apache,Zookeeper,Mac,HealthApp,HPC,Hadoop,OpenStack,OpenSSH,Android,BGL,HDFS,Spark,Windows,Thunderbird
python3 report.py
echo "FULL CHAIN DONE $(date)"
