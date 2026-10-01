#!/bin/bash
# B1 residual-only (empty program) formal complete-file runs; same frozen guard/runtime as R73 formal_full.py
export QG_ART=<WORKDIR>/r73_quality_gate_20260927/r73_qg/art QG_HOME=<WORKDIR>/r73_quality_gate_20260927/r73_qg PYTHONPATH=<WORKDIR>/r73_quality_gate_20260927/r73_qg PYTHONHASHSEED=0
cd <WORKDIR>/r76_additional_20260929/attribution
for d in Proxifier Linux Apache Zookeeper HealthApp Hadoop Mac OpenStack HPC OpenSSH Android BGL HDFS Spark Windows Thunderbird; do
  [ -f runs/formal/empty/$d/result.json ] && continue
  python3 formal_full.py empty $d plans/$d/extraction.json plans/$d/storage.json || echo FAIL $d
done
echo ALLDONE
