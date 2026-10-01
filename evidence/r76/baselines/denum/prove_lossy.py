#!/usr/bin/env python3
"""Constructive losslessness check for the official Denum C++ encoder (commit a3a6975).

For one input block B this script
  1. emulates Denum's replace_and_group() (dataset regex_map entry, then the generic
     number pass) on each line, byte-for-byte, to find ONE line where re-grouping the
     digits of a dataset-rule match (e.g. "10.30 16:49:06" -> "10.3 016:49:06") leaves
     the replaced line and every per-tag number identical;
  2. writes B' = B with only that one line changed (B' != B, same length);
  3. runs the UNMODIFIED official CLI on B and on B' in fresh directories and compares
     every archive member (output/DATASET/0/*) byte-for-byte.
If all members are identical, B and B' map to the same archive content, so no decoder
(official or otherwise) can restore both: the archive format itself is lossy.

Emulation is only used to choose a candidate; the verdict comes from the real binary.
usage: prove_lossy.py DATASET BLOCK_FILE OUTDIR DENUM_BINARY
"""
import hashlib
import json
import os
import re
import resource
import shutil
import signal
import subprocess
import sys
import tarfile
from pathlib import Path

# Copied verbatim (as bytes regexes) from denum_compress.cpp LogProcessor::LogProcessor.
REGEX_MAP = {
    "Android": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?"], [b"<I>", b"<T>"]),
    "Apache": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d{2}) (\d+):(\d+):(\d+)"], [b"<I>", b"<T>"]),
    "BGL": ([rb"(\d+)-(\d+)-(\d+)-(\d+)\.(\d+)\.(\d+)", rb"(\d+):(\d+):(\d+)", rb"(\d+)\.(\d+)\.(\d+)"], [b"<E>", b"<T>", b"<F>"]),
    "Hadoop": ([rb"(\d+)\-(\d+)\-(\d+)", rb"(\d+):(\d+):(\d+),(\d+)"], [b"<D>", b"<T>"]),
    "HDFS": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+):(\d+):(\d+),(\d+)"], [b"<I>", b"<T>"]),
    "HealthApp": ([rb"(\d+):(\d+):(\d+):(\d+)"], [b"<T>"]),
    "HPC": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?"], [b"<I>", b"<T>"]),
    "Linux": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?"], [b"<I>", b"<T>"]),
    "Mac": ([rb"(\d+)-(\d+)-(\d+)-(\d+)", rb"(\d+):(\d+):(\d+)(?:\.(\d+))?"], [b"<D>", b"<T>"]),
    "OpenSSH": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?", rb"sshd\[(\d+)\]:"], [b"<I>", b"<T>", b"<S>"]),
    "OpenStack": ([rb"\.(\d+)-(\d+)-(\d+)_(\d+):(\d+):(\d+)", rb"(\d+)-(\d+)-(\d+).(\d+):(\d+):(\d+)\.(\d+)"], [b"<D>", b"<T>"]),
    "Proxifier": ([rb"(\d+)\.(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?"], [b"<T>"]),
    "Spark": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d{2})\/(\d{2})\/(\d{2}) (\d+):(\d+):(\d+)", rb"(\d+)\.(\d{1}) MB", rb"(\d+)\.(\d{1}) KB", rb"(\d+)\.(\d{1}) GB", rb"(\d+)\.(\d{1}) B"], [b"<I>", b"<T>", b"<M>", b"<K>", b"<G>", b"<B>"]),
    "Thunderbird": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+):(\d+):(\d+)", rb"(\d{4}})\.(\d+)\.(\d+)", rb"\[(\d+)\]:"], [b"<I>", b"<T>", b"<A>", b"<B>"]),
    "Windows": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)", rb"(\d+):(\d+):(\d+)"], [b"<I>", b"<T>", b"<D>"]),
    "Zookeeper": ([rb"(\d+)\.(\d+)\.(\d+)\.(\d+)", rb"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+),(\d+)", rb"(\d+):(\d+):(\d+)"], [b"<I>", b"<T>", b"<D>"]),
}
NUM = re.compile(rb"(?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9])")
ALPHA = b"abcdefghijklmnopqrstuvwxyz"
DIG = re.compile(rb"\d")


def emulate(line, pats, subs):
    """Return (replaced_line, [(tag, value), ...]) as denum_compress would build them."""
    cur = line
    vals = []
    for pat, sub in zip(pats, subs):
        out, last = [], 0
        for m in pat.finditer(cur):
            out.append(cur[last:m.start()] + sub)
            vals.append((sub, int(b"".join(DIG.findall(m.group(0))))))
            last = m.end()
        out.append(cur[last:])
        cur = b"".join(out)
    out, last = [], 0
    for m in NUM.finditer(cur):
        num = m.group(0)
        if len(num) < 15:
            key = b"<" + ALPHA[len(num) - 1:len(num)] + (ALPHA[num[0] - 48:num[0] - 47] if len(num) >= 4 else b"") + b">"
            vals.append((key, int(num)))
            out.append(cur[last:m.start()] + key)
        else:
            out.append(cur[last:m.start()] + num)
        last = m.end()
    out.append(cur[last:])
    return b"".join(out), vals


def candidates(line, pats):
    for pat in pats:
        for m in pat.finditer(line):
            spans = [m.span(g) for g in range(1, (m.re.groups or 0) + 1) if m.span(g)[0] >= 0]
            for (a0, a1), (b0, b1) in zip(spans, spans[1:]):
                if a1 - a0 >= 2 and a1 < b0:
                    # move the last digit of group k to the front of group k+1
                    yield line[:a1 - 1] + line[a1:b0] + line[a1 - 1:a1] + line[b0:]
                if b1 - b0 >= 2 and a1 < b0:
                    # move the first digit of group k+1 to the end of group k
                    yield line[:a1] + line[b0:b0 + 1] + line[a1:b0] + line[b0 + 1:]


def run_cli(binary, dataset, data, workdir):
    if workdir.exists():
        shutil.rmtree(str(workdir))
    (workdir / "Logs" / dataset).mkdir(parents=True)
    (workdir / "output").mkdir()
    (workdir / "Logs" / dataset / (dataset + ".log")).write_bytes(data)
    p = subprocess.run([binary, dataset, "100000", "1"], cwd=str(workdir), stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE, preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_CORE, (0, 0)))
    sig = ("cannot get file size: No such file or directory [output/%s/compressed1.xz]" % dataset).encode()
    # Same acceptance rule as the adapter: exit 0, or the upstream summary-loop SIGABRT that
    # happens on exactly-100000-line inputs after block 0's archive has been closed.
    if b"Block 0 directory successfully compressed" not in p.stdout or not (
            p.returncode == 0 or (p.returncode == -signal.SIGABRT and sig in p.stderr)):
        raise RuntimeError("denum_compress failed: %d %r" % (p.returncode, p.stderr[-300:]))
    arc = workdir / "output" / dataset / "compressed0.xz"
    members = {}
    with tarfile.open(str(arc)) as t:
        for mem in t.getmembers():
            if mem.isfile():
                members[mem.name] = hashlib.sha256(t.extractfile(mem).read()).hexdigest()
    return arc.stat().st_size, members


def main():
    dataset, block_file, outdir, binary = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
    outdir.mkdir(parents=True, exist_ok=True)
    pats = [re.compile(p) for p in REGEX_MAP[dataset][0]]
    subs = REGEX_MAP[dataset][1]
    data = block_file.read_bytes()
    lines = data.split(b"\n")
    found = None
    for i, line in enumerate(lines):
        base = emulate(line, pats, subs)
        for cand in candidates(line, pats):
            if cand != line and len(cand) == len(line) and b"\n" not in cand and emulate(cand, pats, subs) == base:
                found = (i, line, cand)
                break
        if found:
            break
    report = {"dataset": dataset, "block_file": str(block_file), "block_sha256": hashlib.sha256(data).hexdigest()}
    if not found:
        report["verdict"] = "NO_CANDIDATE_FOUND_BY_EMULATION"
        print(json.dumps(report, indent=1))
        return
    i, line, cand = found
    lines2 = list(lines)
    lines2[i] = cand
    data2 = b"\n".join(lines2)
    size1, mem1 = run_cli(binary, dataset, data, outdir / "B")
    size2, mem2 = run_cli(binary, dataset, data2, outdir / "B_prime")
    same = mem1 == mem2
    report.update({"line_index": i, "original_line": line.decode("latin-1"), "modified_line": cand.decode("latin-1"),
                   "modified_block_sha256": hashlib.sha256(data2).hexdigest(),
                   "archive_bytes_B": size1, "archive_bytes_B_prime": size2,
                   "member_count": len(mem1), "all_archive_members_identical": same,
                   "differing_members": sorted(k for k in set(mem1) | set(mem2) if mem1.get(k) != mem2.get(k)),
                   "verdict": "LOSSY_FORMAT_PROVEN" if same else "NOT_PROVEN_MEMBERS_DIFFER"})
    for d in ("B", "B_prime"):
        shutil.rmtree(str(outdir / d))
    (outdir / "proof.json").write_text(json.dumps(report, indent=1) + "\n")
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
