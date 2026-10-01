#!/usr/bin/env python3
"""Run LogShrink's per-segment step exactly as run.py procFiles() calls it.

usage: ls_seg_runner.py PYTHON_COMPRESSION_DIR SEGMENT_FILE COMPRESS_OUTDIR TEMPLATE_DIR/ DATASET SEGMENT_NO SEED

run.py (commit 59ce494) procFiles() calls, for segment i:
    logshrink.run(input_file=<seg>/<i>.col, compress_outdir=<tmp>/<i>, template_path=<tpl>/, ds=ds,
                  compressed_fn=str(i) + "_ls" + suffix, sample_rate=sample_rate, threshold=threshold,
                  n_candidate=n_candidate, h=h, re_mode=re_mode, parsing=parsing, encode_mode=encode_mode,
                  column_mode=column_mode, kernel=kernel, seq_sampling=seq_sampling, random_sam=random_sam, mt=mt)
with sample_rate = 0.1 hard-coded in run.py and the other values from its command line. The values below
are the paper's defaults (Sec. 5.1.4: theta=4, h=20, M=16), identical to the only active line of the
repository's run_shell.sh:  -E E -C -K lzma -V -S -P -wh 20 -th 4 -NC 16 -mt 0.5
The only addition is seeding Python's `random` and NumPy's global RNG (the official code is unseeded; the
paper averages 10 unseeded runs). Seeding does not change any algorithmic step.
"""
import os
import random
import sys

pc_dir, seg_file, outdir, tpl, ds, seg_no, seed = sys.argv[1:8]
os.chdir(pc_dir)                  # run.py is always started from python_compression/ (relative ./parser paths)
sys.path.insert(0, pc_dir)        # == sys.path[0] when "python3 run.py" is executed in that directory

import numpy as np                # noqa: E402
import logshrink                  # noqa: E402  (its imports append "../" exactly as under run.py)

random.seed(int(seed))
np.random.seed(int(seed))

logshrink.run(
    input_file=seg_file,
    compress_outdir=outdir,
    template_path=tpl,
    ds=ds,
    compressed_fn=str(seg_no) + "_ls" + ".7z",
    sample_rate=0.1,
    threshold=4,
    n_candidate=16,
    h=20,
    re_mode=True,
    parsing=True,
    encode_mode="E",
    column_mode=True,
    kernel="lzma",
    seq_sampling=True,
    random_sam=False,
    mt=0.5,
)
