#!/usr/bin/env python3
"""LogShrink run.py segmentation, verbatim, applied to one 100k-line block.

usage: ls_segment.py PYTHON_COMPRESSION_DIR INPUT_FILE SEG_DIR

The loop below is copied from run.py (commit 59ce494, "# segmenting" section) and calls the repository's own
utils.util_code.list_write. As in run.py it reads with encoding="ISO-8859-1" in text mode (universal newlines)
and list_write() writes str(line).strip() + "\n" with the locale encoding (UTF-8 here). Both are properties of
the official pipeline and are NOT altered; the decoder-side inverse transcoding is documented in ls_run.py.
Prints a JSON list with the line count of every segment file written.
"""
import json
import os
import sys

pc_dir, input_file, seg_path = sys.argv[1:4]
sys.path.insert(0, pc_dir)
from utils.util_code import list_write  # noqa: E402

blockSize = 100000
f = open(input_file, encoding="ISO-8859-1")
cou = 0
count = 0
buffer = []
counts = []
while True:
    line = f.readline()
    if not line:
        list_write(os.path.join(seg_path, str(cou) + ".col"), buffer, True)
        counts.append(len(buffer))
        break

    buffer.append(line)
    count += 1

    if count == blockSize:
        count = 0
        list_write(os.path.join(seg_path, str(cou) + ".col"), buffer, True)
        counts.append(len(buffer))
        buffer = []
        cou += 1
f.close()
print("SEGMENT_COUNTS " + json.dumps(counts))
