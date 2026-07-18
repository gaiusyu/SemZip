#!/usr/bin/env bash
set -euo pipefail

g++ -std=c++17 -O3 -DNDEBUG compressor.cpp -o Delog_plan_compress -lpcre2-8 -larchive -pthread
g++ -std=c++17 -O3 -DNDEBUG decompressor.cpp -o decompress -larchive -pthread
