"""Held-out template coverage by block 0: share of held-out-sample lines whose masked token skeleton
(token count + first 12 tokens, digit-bearing tokens masked) occurs among block-0 lines.
Inputs (copied from the execution host): <D>.block0.log (original first 100k lines) and <D>.sample.log (fixed held-out sample).
Usage: skeleton_coverage.py DIR  -> prints JSON"""
import json, re, sys
from pathlib import Path
DIG = re.compile(r'\d')
def skel(line):
    toks = line.split()
    return (len(toks), tuple('<*>' if DIG.search(t) else t.lower() for t in toks[:12]))
out = {}
root = Path(sys.argv[1])
for d in ['Thunderbird', 'BGL', 'Windows', 'HDFS', 'Spark']:
    train = {skel(b.decode('latin-1')) for b in open(root/f'{d}.block0.log', 'rb')}
    n = hit = 0
    for b in open(root/f'{d}.sample.log', 'rb'):
        n += 1; hit += skel(b.decode('latin-1')) in train
    out[d] = round(hit / n * 100, 1)
print(json.dumps(out))
