#!/usr/bin/env python3
"""Create decompression/restore_r76.py and decompression/decompress_r76.py from the pristine restore.py and
decompress.py (commit 59ce494) with the R76 DECODER bug fixes. Every fix is an exact, asserted string
replacement; nothing else changes. The encoder and the archives are untouched.

usage: make_restore_r76.py PATH/TO/python_compression/decompression [--fixes 1,2,3,4,5]
"""
import sys
from pathlib import Path

dec = Path(sys.argv[1])
enabled = {1, 2, 3, 4, 5}
if "--fixes" in sys.argv:
    enabled = {int(x) for x in sys.argv[sys.argv.index("--fixes") + 1].split(",") if x}

RESTORE_FIXES = [
    (0, "restore_r76.py imports the patched recover_pat from decompress_r76.py (fix 5)",
     r"""from decompress import *
""",
     r"""from decompress_r76 import *  # R76: decompress.py + decoder bug fix 5
"""),
    # 1. load_log(): an empty load_failed.log / match_failed.log holds zero records; the original returned [""],
    #    so restore.py's own assert (eids == load_failed + match_failed + heads) failed on every such block.
    (1, "empty failed-log files",
     "    lines = load_str_array_raw(path)\n    \n    now_line = \"\"\n",  # blank line holds 4 spaces
     r"""    lines = load_str_array_raw(path)
    if not lines:  # R76 decoder bug fix 1: an empty failed-log file holds 0 records, not [""]
        return []
    now_line = ""
"""),
    # 2. restore_heads().getValue(): header column files head_var_col<i>.* were ordered with lexicographic
    #    sorted(), so with >= 11 header columns col10 is taken as column 2 etc. and headers are scrambled.
    (2, "numeric order of head_var_col<i> files",
     r"""        files = [file for file in os.listdir(path) if re.search(r'head_var_col\d+.', file)]
        files = sorted(files)
""",
     r"""        files = [file for file in os.listdir(path) if re.search(r'head_var_col\d+.', file)]
        # R76 decoder bug fix 2: numeric column order (lexicographic sorted() puts col10 before col2)
        files = sorted(files, key=lambda f: int(re.search(r'head_var_col(\d+)', f).group(1)))
"""),
    # 3. restore_heads(): the ENCODER (preprocess.read_parsing_result) loads THULR's Head<K>.head files in
    #    lexicographic order (Head0, Head1, Head10, Head2, ...), so with >= 11 THULR header columns the archive's
    #    header columns are stored in that order, while restore_heads() consumes them in THULR order K.
    #    The permutation is a deterministic function of the column count, so it is undone here.
    (3, "undo encoder's lexicographic Head<K>.head order",
     r"""    HeadValue = recover_pat(relations['h_pat'], tmpValue)
""",
     r"""    HeadValue = recover_pat(relations['h_pat'], tmpValue)
    # R76 decoder bug fix 3: undo the encoder's lexicographic Head<K>.head order (see make_restore_r76.py)
    _order = sorted(range(len(HeadValue)), key=lambda k: "Head%d.head" % k)
    HeadValue = {k: HeadValue[p] for p, k in enumerate(_order)}
"""),
    # 4. load_tempaltes(): a template-id line is recognised with re.search('\[\d+\]\n') and len < 10, so a short
    #    template CONTENT line such as " [1]" is taken as an id line and int(line[1:-2]) raises ValueError.
    #    THULR (parser/main.cpp loadTemplate) uses regex_match("\\[(\\d+)\\]") on the whole line; the decoder now
    #    uses the same whole-line rule. For every line on which the original did not crash the result is identical
    #    (a search match with int() succeeding implies the line is exactly "[<digits>]\n").
    (4, "whole-line template id match",
     r"""        if (re.search('\[\d+\]\\n',line) and len(line) < 10):
""",
     r"""        if (re.fullmatch('\[\d+\]\\n',line) and len(line) < 10):  # R76 decoder bug fix 4: whole-line id match
"""),
]

DECOMPRESS_FIXES = [
    # 5. recover_pat() (used for header AND variable patterns): the ENCODER's split_arr()
    #    (analyzer/property_miner.pattern_matcher_unit) does not advance past a delimiter found at offset 0
    #    ("pos == 0" branch): that character stays at the start of the extracted value and is also counted in the
    #    delimiter run (all_pos entry k >= 2). The original decoder emits value + whole run, duplicating those
    #    characters ("/10.250.19.102" -> "/10/.250.19.102"). Of a run of length k only its last character
    #    pat[pos+k-1] was consumed as a separator (the first k-1 are the value's own leading character, or, for a
    #    trailing run, lie inside the last value), so the decoder emits value + pat[pos+k-1:pos+k].
    #    Identical to the original whenever k == 1.
    (5, "recover_pat delimiter runs",
     r"""                buf = [str(buf[t]) + str(values[cur_idx][t]) + cur_pat[2][pos:pos+cur_pat[1][i]] for t in range(length)]
""",
     r"""                buf = [str(buf[t]) + str(values[cur_idx][t]) + cur_pat[2][pos+cur_pat[1][i]-1:pos+cur_pat[1][i]] for t in range(length)]  # R76 decoder bug fix 5
"""),
]


def apply(src, fixes):
    for num, _, old, new in fixes:
        if num and num not in enabled:
            continue
        if num == 0 and 5 not in enabled:
            continue
        if src.count(old) != 1:
            raise SystemExit("fix %d anchor not found exactly once:\n%s" % (num, old))
        src = src.replace(old, new)
    return src


(dec / "restore_r76.py").write_text(apply((dec / "restore.py").read_text(), RESTORE_FIXES))
(dec / "decompress_r76.py").write_text(apply((dec / "decompress.py").read_text(), DECOMPRESS_FIXES))
print("wrote restore_r76.py and decompress_r76.py with fixes", sorted(enabled))
