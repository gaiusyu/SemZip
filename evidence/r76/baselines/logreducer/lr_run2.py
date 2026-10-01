#!/usr/bin/env python3
"""R76 LogReducer (USENIX FAST'21, github.com/THUBear-wjy/LogReducer @ 40005419) baseline:
independent exact-100,000-line blocks, official pipeline per block, archive-only decoding.

Two results are reported from ONE archive per block (both decoded from the archive alone):

  native   = LogReducer official pipeline output (README: training.py, then LogReducer.py,
             decompression LogRestore.py). Its decoder writes normalized text (every record
             str.strip()-ed after ISO-8859-1 decoding, universal-newline split, UTF-8 output), so
             it is byte-exact only on some inputs. Verdict per the pre-registered rule.
  adapted  = native output + a generic, dataset-agnostic, counted residual ("LogReducer+R"):
             per-record inverse transcoding (UTF-8 -> ISO-8859-1, the inverse of the official
             reader) and an LZMA-compressed patch stream built at encode time by decoding the
             native archive with the official decoder and comparing with the original block.

Per block (fresh processes, no state shared across blocks):
  1. official sampler.py (seeded, via runpy) on the raw block -> block.log.sample
     (training.py -m Nor calls the same script unseeded with rate 0.001)
  2. official training.py -I block.log -T tpl/ -m Sam   (defaults -TL 0 -L 4 --IsMulti F)
  3. official LogReducer.py "Tot" segmentation code verbatim (ISO-8859-1 readline, util.list_write,
     -B 100000). A block with exactly 100,000 records makes the official loop also emit an empty
     trailing segment; that empty segment is removed (block adaptation, documented).
  4. per segment k: exact LogReducer.py procFiles commands, one THULR process per segment:
       ./THULR -I seg/ -X k -Y k -O tmp/ -T tpl/ -E Z -D D -F tpl/head.format
       7za a out/k.7z tmp/k/* -m0=LZMA
  5. model.7z = 7za -m0=LZMA of every template-dir file restore.py reads
     (template.col, head.format, E<k>basic.rule for each template k in template.col)
  6. verification decode of the native part (same function as the archive-only decoder) and
     residual construction.
  archive = header [flags u8][nseg u8][nseg x u32 len][u32 model len] + segments + model.7z
            + [u32 residual len] + residual
Archive-only decode: split archive, 7za x model.7z -> tpl/, write k.7z files, run the official
  decompressor  python3 LogRestore.py -I in/ -T tpl/ -O dec/lr_restored.txt  (restore.py per
  segment + Tot merge). native bytes = that output with one trailing LF removed when flag bit0
  says the original block had no terminal LF. adapted bytes = native + residual.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import json
import lzma
import os
from pathlib import Path
import platform
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import time
import traceback

BLOCK_RECORDS = 100000  # protocol constant, no CLI override
TOT_BLOCKSIZE = 100000  # LogReducer.py default -B
READ_BYTES = 1024 * 1024
SCHEMA = "semzip.r76.logreducer.v2"
HERE = Path(__file__).resolve().parent
LR_DIR = HERE / "src" / "LogReducer"
UPSTREAM_COMMIT = "40005419022c02454eca7b027d76b99b3dfad543"
SEED = 0
PY = "python3"
SEEDED_SAMPLER = ("import random, runpy, sys; random.seed(%d); "
                  "sys.argv = ['sampler.py'] + sys.argv[1:]; "
                  "runpy.run_path('sampler.py', run_name='__main__')" % SEED)
# LogReducer.py lines 173-196 ("Tot" mode segmentation), verbatim apart from argv plumbing.
TOT_SEGMENT = r'''
import os, sys, util
input_path, seg_path, blockSize = sys.argv[1], sys.argv[2], int(sys.argv[3])
f = open(input_path, encoding = "ISO-8859-1")
cou = 0
count = 0
buffer = []
while True:
    line = f.readline()
    if not line:
        util.list_write(os.path.join(seg_path, str(cou) + ".col"), buffer, True)
        break

    buffer.append(line)
    count += 1

    if count == blockSize:
        count = 0
        util.list_write(os.path.join(seg_path, str(cou) + ".col"), buffer, True)
        buffer = []
        cou += 1
'''
LZMA_RESIDUAL = dict(format=lzma.FORMAT_ALONE, preset=9 | lzma.PRESET_EXTREME)


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp.%d" % os.getpid())
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(str(tmp), str(path))


def file_sha(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(READ_BYTES)
            if not data:
                return h.hexdigest()
            h.update(data)


def file_identity(path):
    st = Path(path).stat()
    return {"path": str(Path(path).resolve()), "bytes": st.st_size,
            "mtime_ns": st.st_mtime_ns, "device": st.st_dev, "inode": st.st_ino}


# scan()/inventory(): verbatim copies of r68_external_20260923/codec_baseline.py (same block law).
def scan(source, segment, block_end):
    index = nbytes = nlfs = 0
    last = None
    with open(source, "rb") as f:
        while True:
            data = f.read(READ_BYTES)
            if not data:
                break
            start = 0
            while start < len(data):
                n = data.count(b"\n", start)
                needed = BLOCK_RECORDS - nlfs
                if n >= needed:
                    lo, hi = start + 1, len(data)
                    while lo < hi:
                        mid = (lo + hi) // 2
                        if data.count(b"\n", start, mid) >= needed:
                            hi = mid
                        else:
                            lo = mid + 1
                    end, n = lo, needed
                else:
                    end = len(data)
                piece = memoryview(data)[start:end]
                segment(piece)
                nbytes += len(piece)
                nlfs += n
                last = data[end - 1]
                start = end
                if nlfs == BLOCK_RECORDS:
                    block_end({"index": index, "raw_bytes": nbytes, "lf_count": nlfs,
                               "records": nlfs, "ends_with_lf": True})
                    index += 1
                    nbytes = nlfs = 0
                    last = None
    if nbytes:
        block_end({"index": index, "raw_bytes": nbytes, "lf_count": nlfs,
                   "records": nlfs + int(last != 10), "ends_with_lf": last == 10})


def inventory(source):
    before = file_identity(source)
    full = hashlib.sha256()
    state = [hashlib.sha256()]
    blocks = []

    def segment(piece):
        full.update(piece)
        state[0].update(piece)

    def end(meta):
        meta["raw_sha256"] = state[0].hexdigest()
        blocks.append(meta)
        state[0] = hashlib.sha256()

    scan(source, segment, end)
    if file_identity(source) != before:
        raise RuntimeError("source changed during inventory: %s" % source)
    return {"source": before, "raw_sha256": full.hexdigest(), "blocks": blocks,
            "raw_bytes": sum(b["raw_bytes"] for b in blocks),
            "records": sum(b["records"] for b in blocks)}


def clip(data, head=16384, tail=49152):
    if len(data) <= head + tail:
        return data
    return data[:head] + b"\n...[clipped %d bytes]...\n" % (len(data) - head - tail) + data[-tail:]


def run(cmd, diag, name, timeout, shell=False, cwd=None):
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=str(cwd or LR_DIR), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, shell=shell, check=False)
    dt = time.perf_counter() - t0
    with open(diag / (name + ".stdout"), "wb") as f:
        f.write(clip(proc.stdout))
    with open(diag / (name + ".stderr"), "wb") as f:
        f.write(clip(proc.stderr))
    if proc.returncode != 0:
        raise RuntimeError("%s exit %d; see %s" % (name, proc.returncode, diag / (name + ".stderr")))
    return dt, proc.stdout


def template_keys(template_col):
    # identical rule to restore.py load_tempaltes()
    keys = []
    with open(template_col, encoding="ISO-8859-1") as f:
        for line in f.readlines():
            if re.search(r'\[\d+\]\n', line) and len(line) < 10:
                num = int(line[1:-2])
                if num != 0:
                    keys.append(num)
    return keys


THULR_STATS = [("read_logs", rb"read logs: (\d+)"), ("load_failed", rb"load failed: (\d+)"),
               ("match_failed", rb"match failed: (\d+)"), ("templates_loaded", rb"read (\d+) templates")]


# ---------------------------------------------------------------- residual (generic, counted)
def varint(out, n):
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return


def read_varint(buf, pos):
    n = shift = 0
    while True:
        b = buf[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return n, pos


def records_of(data, ends_with_lf):
    recs = data.split(b"\n")
    if ends_with_lf:
        if recs[-1] != b"":
            raise ValueError("records_of: missing terminal LF")
        recs.pop()
    return recs


def inv_transcode(rec):
    """Inverse of the official reader (ISO-8859-1 decode, UTF-8 write); raw bytes if impossible."""
    try:
        return rec.decode("utf-8").encode("latin-1")
    except (UnicodeDecodeError, UnicodeEncodeError):
        return rec


def native_records(native_out):
    """native_out = LogRestore output (one LF after every restored record)."""
    recs = native_out.split(b"\n")
    if recs and recs[-1] == b"":
        recs.pop()
    return [inv_transcode(r) for r in recs]


def tot_pieces(rec):
    """How many lines the official ISO-8859-1 universal-newline readline makes of one LF record."""
    parts = rec.count(b"\r") + 1
    if rec.endswith(b"\r"):
        parts -= 1  # CRLF (or CR at EOF) terminates the record instead of splitting it
    return max(parts, 1)


WS = b" \t\r\n\x0b\x0c"


def build_residual(orig, ends_with_lf, native_out):
    """Return (residual_bytes, stats). orig = original block bytes."""
    O = records_of(orig, ends_with_lf)
    stats = {"records": len(O)}
    raw = bytearray()
    D = native_records(native_out) if native_out is not None else None
    counts = [tot_pieces(r) for r in O]
    if D is None or sum(counts) != len(D):
        stats.update({"full_fallback": True, "native_records": None if D is None else len(D),
                      "predicted_pieces": sum(counts)})
        raw.append(1)
        raw += orig
        return lzma.compress(bytes(raw), **LZMA_RESIDUAL), stats
    raw.append(0)
    regroup = [(i, c) for i, c in enumerate(counts) if c != 1]
    varint(raw, len(regroup))
    prev = -1
    for i, c in regroup:
        varint(raw, i - prev - 1)
        varint(raw, c)
        prev = i
    patches = bytearray()
    n_patch = n_wrap = n_wrap_ws = n_edit = 0
    pos = 0
    prev = -1
    for i, c in enumerate(counts):
        d = D[pos] if c == 1 else b"\r".join(D[pos:pos + c])
        pos += c
        o = O[i]
        if d == o:
            continue
        n_patch += 1
        varint(patches, i - prev - 1)
        prev = i
        cands = []
        p = o.find(d)
        if p >= 0:
            pre, suf = o[:p], o[p + len(d):]
            cands.append((len(pre) + len(suf), 1, pre, suf))
        a = 0
        m = min(len(o), len(d))
        while a < m and o[a] == d[a]:
            a += 1
        b = 0
        while b < m - a and o[len(o) - 1 - b] == d[len(d) - 1 - b]:
            b += 1
        mid = o[a:len(o) - b]
        cands.append((len(mid) + 2, 2, (a, b), mid))
        cands.sort(key=lambda x: (x[0], x[1]))
        _, typ, x, y = cands[0]
        patches.append(typ)
        if typ == 1:
            n_wrap += 1
            if not x.strip(WS) and not y.strip(WS):
                n_wrap_ws += 1
            varint(patches, len(x))
            patches += x
            varint(patches, len(y))
            patches += y
        else:
            n_edit += 1
            varint(patches, x[0])
            varint(patches, x[1])
            varint(patches, len(y))
            patches += y
    varint(raw, n_patch)
    raw += patches
    stats.update({"full_fallback": False, "regroup_records": len(regroup), "patched_records": n_patch,
                  "patch_wrap": n_wrap, "patch_wrap_whitespace_only": n_wrap_ws, "patch_edit": n_edit,
                  "residual_raw_bytes": len(raw)})
    if not regroup and not n_patch:
        return b"", stats
    return lzma.compress(bytes(raw), **LZMA_RESIDUAL), stats


def apply_residual(residual, ends_with_lf, native_out):
    if not residual:
        D = native_records(native_out)
        return b"\n".join(D) + (b"\n" if ends_with_lf else b"")
    raw = lzma.decompress(residual, format=lzma.FORMAT_ALONE)
    if raw[0] == 1:
        return bytes(raw[1:])
    pos = 1
    n_reg, pos = read_varint(raw, pos)
    regroup = {}
    prev = -1
    for _ in range(n_reg):
        gap, pos = read_varint(raw, pos)
        c, pos = read_varint(raw, pos)
        prev = prev + gap + 1
        regroup[prev] = c
    D = native_records(native_out)
    O = []
    dpos = 0
    i = 0
    while dpos < len(D):
        c = regroup.get(i, 1)
        O.append(D[dpos] if c == 1 else b"\r".join(D[dpos:dpos + c]))
        dpos += c
        i += 1
    if dpos != len(D):
        raise RuntimeError("regroup overrun")
    n_patch, pos = read_varint(raw, pos)
    prev = -1
    for _ in range(n_patch):
        gap, pos = read_varint(raw, pos)
        i = prev + gap + 1
        prev = i
        typ = raw[pos]
        pos += 1
        d = O[i]
        if typ == 1:
            n, pos = read_varint(raw, pos)
            pre = bytes(raw[pos:pos + n])
            pos += n
            n, pos = read_varint(raw, pos)
            suf = bytes(raw[pos:pos + n])
            pos += n
            O[i] = pre + d + suf
        elif typ == 2:
            a, pos = read_varint(raw, pos)
            b, pos = read_varint(raw, pos)
            n, pos = read_varint(raw, pos)
            mid = bytes(raw[pos:pos + n])
            pos += n
            O[i] = d[:a] + mid + (d[len(d) - b:] if b else b"")
        else:
            raise RuntimeError("bad patch type %d" % typ)
    if pos != len(raw):
        raise RuntimeError("residual trailing bytes")
    return b"\n".join(O) + (b"\n" if ends_with_lf else b"")


# ---------------------------------------------------------------- container
def pack(flags, segs, model, residual):
    head = struct.pack(">BB", flags, len(segs)) + b"".join(struct.pack(">I", len(s)) for s in segs) \
        + struct.pack(">I", len(model))
    native = head + b"".join(segs) + model
    return native + struct.pack(">I", len(residual)) + residual, len(native)


def unpack(data):
    flags, nseg = struct.unpack(">BB", data[:2])
    pos = 2
    lens = []
    for _ in range(nseg):
        lens.append(struct.unpack(">I", data[pos:pos + 4])[0])
        pos += 4
    mlen = struct.unpack(">I", data[pos:pos + 4])[0]
    pos += 4
    segs = []
    for n in lens:
        segs.append(data[pos:pos + n])
        pos += n
    model = data[pos:pos + mlen]
    pos += mlen
    rlen = struct.unpack(">I", data[pos:pos + 4])[0]
    pos += 4
    residual = data[pos:pos + rlen]
    if len(residual) != rlen or pos + rlen != len(data) or any(len(s) != n for s, n in zip(segs, lens)):
        raise RuntimeError("malformed archive")
    return flags, segs, model, residual


# ---------------------------------------------------------------- native decode (archive part only)
def decode_native(flags, segs, model, work, diag, timeout, tag):
    """Official decompressor on archive bytes only. Returns (native_out_bytes_raw, info)."""
    info = {}
    root = work / ("dec_" + tag)
    tpl, ind, dec = root / "tpl", root / "in", root / "dec"
    for d in (tpl, ind, dec):
        d.mkdir(parents=True)
    t0 = time.perf_counter()
    (root / "model.7z").write_bytes(model)
    proc = subprocess.run(["7za", "x", str(root / "model.7z"), "-o" + str(tpl), "-y"],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
    if proc.returncode != 0:
        raise RuntimeError("model extraction exit %d" % proc.returncode)
    for k, s in enumerate(segs):
        (ind / ("%d.7z" % k)).write_bytes(s)
    out = dec / "lr_restored.txt"
    order = ("python3 LogRestore.py -I " + str(ind) + "/ -T " + str(tpl) + "/ -O " + str(out))
    dt, stdout = run(order, diag, "restore_" + tag, timeout, shell=True)
    if b"Error Occur" in stdout or re.search(rb"\.col does not exist", stdout):
        raise RuntimeError("LogRestore reported a failed segment; see %s" % (diag / ("restore_" + tag + ".stdout")))
    if not out.is_file():
        raise RuntimeError("LogRestore produced no output")
    native = out.read_bytes()
    info["t_logrestore"] = dt
    info["t_native_decode"] = time.perf_counter() - t0
    shutil.rmtree(str(root), ignore_errors=True)
    return native, info


def native_view(native_out, flags):
    # decode alignment: restore.py writes '\n' after every record; flag bit0 = original had no final LF
    if flags & 1 and native_out.endswith(b"\n"):
        return native_out[:-1]
    return native_out


def encode_block(block_path, work, archive, diag, ends_with_lf, timeout):
    info = {}
    seg, tpl, tmp, out = work / "seg", work / "tpl", work / "out" / "tmp" / "LR_0", work / "out"
    for d in (seg, tpl, tmp):
        d.mkdir(parents=True)
    blk = work / "block.log"
    os.replace(str(block_path), str(blk))
    t_native0 = time.perf_counter()
    # 1. official sampler.py, seeded (training.py -m Nor would call it unseeded with rate 0.001)
    info["t_sample"], _ = run([PY, "-c", SEEDED_SAMPLER, str(blk), str(blk) + ".sample", "0.001"],
                              diag, "1_sampler", timeout)
    # 2. official training.py (README invocation) with -m Sam: sample already drawn in step 1
    info["t_train"], _ = run([PY, "training.py", "-I", str(blk), "-T", str(tpl) + "/", "-m", "Sam"],
                             diag, "2_training", timeout)
    # 3. official Tot-mode segmentation
    info["t_segment"], _ = run([PY, "-c", TOT_SEGMENT, str(blk), str(seg), str(TOT_BLOCKSIZE)],
                               diag, "3_segment", timeout)
    nseg = len(list(seg.glob("*.col")))
    segfiles = [seg / ("%d.col" % k) for k in range(nseg)]
    if nseg > 1 and segfiles[-1].stat().st_size == 0:
        segfiles[-1].unlink()
        segfiles.pop()
        info["dropped_empty_trailing_segment"] = True
    normalized = b"".join(p.read_bytes() for p in segfiles)
    info["segments"] = len(segfiles)
    # 4. exactly the two commands LogReducer.py procFiles issues per segment
    info["t_thulr"] = info["t_7z"] = 0.0
    segs = []
    for k in range(len(segfiles)):
        order = ("./THULR -I " + str(seg) + "/ -X %d -Y %d -O " % (k, k) + str(tmp) + "/ -T " + str(tpl) + "/"
                 " -E Z -D D -F " + os.path.join(str(tpl) + "/", "head.format"))
        dt, stdout = run(order, diag, "4_thulr_%d" % k, timeout, shell=True)
        info["t_thulr"] += dt
        if k == 0:
            for key, rx in THULR_STATS:
                m = re.search(rx, stdout)
                info[key] = int(m.group(1)) if m else None
        zp = out / ("%d.7z" % k)
        dt, _ = run("7za a " + str(zp) + " " + str(tmp) + "/%d/* -m0=LZMA" % k, diag, "5_7za_%d" % k,
                    timeout, shell=True)
        info["t_7z"] += dt
        segs.append(zp.read_bytes())
    # 5. decoder model: only files restore.py opens from the template directory
    keys = template_keys(tpl / "template.col")
    names = ["template.col", "head.format"] + ["E%dbasic.rule" % k for k in keys]
    for n in names:
        if not (tpl / n).is_file():
            raise RuntimeError("training did not produce decoder file %s" % n)
    model_path = work / "model.7z"
    t0 = time.perf_counter()
    proc = subprocess.run(["7za", "a", str(model_path), "-m0=LZMA"] + names, cwd=str(tpl),
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
    if proc.returncode != 0:
        raise RuntimeError("model 7za exit %d" % proc.returncode)
    info["t_model"] = time.perf_counter() - t0
    info["model_files"] = len(names)
    info["model_raw_bytes"] = sum((tpl / n).stat().st_size for n in names)
    info["templates"] = len(keys)
    model = model_path.read_bytes()
    flags = 0 if ends_with_lf else 1
    info["t_native_encode"] = time.perf_counter() - t_native0
    # 6. verification decode (same function as the archive-only decoder) + residual
    t0 = time.perf_counter()
    orig = blk.read_bytes()
    try:
        native_out, _ = decode_native(flags, segs, model, work, diag, timeout, "verify")
        info["verify_decode_error"] = None
    except Exception as exc:
        native_out = None
        info["verify_decode_error"] = str(exc)
    if native_out is not None:
        info["native_block_exact"] = native_view(native_out, flags) == orig
        info["native_equals_normalized_input"] = native_out == normalized
    residual, rstats = build_residual(orig, ends_with_lf, native_out)
    info["residual"] = rstats
    info["t_residual"] = time.perf_counter() - t0
    data, native_len = pack(flags, segs, model, residual)
    tmp_archive = archive.with_name(archive.name + ".part")
    with open(tmp_archive, "wb") as f:
        f.write(data)
    os.replace(str(tmp_archive), str(archive))
    info.update({"payload_7z_bytes": sum(len(s) for s in segs), "model_7z_bytes": len(model),
                 "native_bytes": native_len, "residual_bytes": len(data) - native_len,
                 "archive_bytes": len(data)})
    return info


def decode_block(archive, work, diag, timeout):
    """Archive-only decode. Returns (native_bytes, adapted_bytes, info)."""
    t0 = time.perf_counter()
    flags, segs, model, residual = unpack(archive.read_bytes())
    ends_with_lf = not flags & 1
    info = {}
    native_out = None
    try:
        native_out, info = decode_native(flags, segs, model, work, diag, timeout, "archive")
        info["native_decode_error"] = None
    except Exception as exc:
        info["native_decode_error"] = str(exc)
    t1 = time.perf_counter()
    try:
        adapted = apply_residual(residual, ends_with_lf, native_out) if (native_out is not None or (
            residual and lzma.decompress(residual, format=lzma.FORMAT_ALONE)[0] == 1)) else None
        info["adapted_decode_error"] = None if adapted is not None else "native decode failed"
    except Exception as exc:
        adapted = None
        info["adapted_decode_error"] = "%s: %s" % (type(exc).__name__, exc)
    info["t_residual_apply"] = time.perf_counter() - t1
    info["t_decode_block"] = time.perf_counter() - t0
    return (native_view(native_out, flags) if native_out is not None else None), adapted, info


def diff_summary(orig_bytes, dec_bytes):
    a = orig_bytes.split(b"\n")
    b = dec_bytes.split(b"\n")
    diff = sum(1 for x, y in zip(a, b) if x != y) + abs(len(a) - len(b))
    cr_only = sum(1 for x, y in zip(a, b) if x != y and x == y + b"\r")
    first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
    ex = {}
    if first < max(len(a), len(b)):
        ex = {"line": first,
              "orig": (a[first] if first < len(a) else b"<EOF>")[:240].decode("latin-1"),
              "decoded": (b[first] if first < len(b) else b"<EOF>")[:240].decode("latin-1")}
    return {"orig_lines": len(a), "decoded_lines": len(b), "differing_lines": diff,
            "differing_cr_only": cr_only, "first_diff": ex}


def run_dataset(name, source, out_root, workers, timeout, resume, keep_work):
    ds = out_root / name
    archives = ds / "archives"
    blocks_dir = ds / "blocks"
    diags = ds / "diagnostics"
    for d in (archives, blocks_dir, diags):
        d.mkdir(parents=True, exist_ok=True)
    inv_path = ds / "input_inventory.json"
    t_inv = time.perf_counter()
    inv = inventory(source)
    if inv_path.exists() and resume:
        old = json.load(open(inv_path))
        if old["raw_sha256"] != inv["raw_sha256"] or old["blocks"] != inv["blocks"]:
            raise RuntimeError("inventory changed on resume")
    atomic_json(inv_path, inv)
    t_inv = time.perf_counter() - t_inv
    result = {"schema": SCHEMA, "dataset": name, "status": "RUNNING", "started_at": utc(),
              "source": inv["source"], "raw_sha256": inv["raw_sha256"], "raw_bytes": inv["raw_bytes"],
              "records": inv["records"], "n_blocks": len(inv["blocks"]), "workers": workers,
              "inventory_seconds": t_inv}
    atomic_json(ds / "status.json", result)
    work_root = Path(tempfile.mkdtemp(prefix="work-", dir=str(ds)))

    # ---------------- encode phase ----------------
    enc_info = {}
    progress = {"enc": 0, "dec": 0}

    def enc_one(meta, raw_path, work):
        idx = meta["index"]
        diag = diags / ("%06d" % idx)
        if diag.exists():
            shutil.rmtree(str(diag))
        diag.mkdir()
        archive = archives / ("block_%06d.lrb" % idx)
        t0 = time.perf_counter()
        try:
            info = encode_block(raw_path, work, archive, diag, meta["ends_with_lf"], timeout)
            info["status"] = "OK"
        except Exception as exc:
            info = {"status": "ENCODE_FAIL", "error": str(exc), "traceback": traceback.format_exc()}
        info["block_encode_seconds"] = time.perf_counter() - t0
        info.update(meta)
        if info["status"] == "OK":
            info["archive_sha256"] = file_sha(archive)
        atomic_json(blocks_dir / ("%06d.enc.json" % idx), info)
        if not keep_work:
            shutil.rmtree(str(work), ignore_errors=True)
        progress["enc"] += 1
        if progress["enc"] % 10 == 0:
            atomic_json(ds / "status.json", dict(result, phase="encode", encoded_blocks=progress["enc"],
                                                 updated_at=utc()))
        return info

    done = set()
    if resume:
        for p in blocks_dir.glob("*.enc.json"):
            info = json.load(open(p))
            a = archives / ("block_%06d.lrb" % info["index"])
            if info.get("status") == "OK" and a.exists() and file_sha(a) == info["archive_sha256"] \
                    and info["raw_sha256"] == inv["blocks"][info["index"]]["raw_sha256"]:
                done.add(info["index"])
                enc_info[info["index"]] = info
    t_enc = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        pending = []
        cur = [None, None, None, None]
        state = {"idx": 0}

        def collect_one():
            info = pending.pop(0).result()
            enc_info[info["index"]] = info

        def segment(piece):
            if state["idx"] in done:
                return
            if cur[0] is None:
                while len(pending) >= workers:
                    collect_one()
                work = Path(tempfile.mkdtemp(prefix="b%06d-" % state["idx"], dir=str(work_root)))
                path = work / "input.raw"
                cur[:] = [open(path, "xb"), path, work, hashlib.sha256()]
            cur[0].write(piece)
            cur[3].update(piece)

        def end(meta):
            idx = meta["index"]
            state["idx"] = idx + 1
            if idx in done:
                return
            cur[0].close()
            expected = inv["blocks"][idx]
            if cur[3].hexdigest() != expected["raw_sha256"] or any(meta[k] != expected[k] for k in meta):
                raise RuntimeError("source block changed during encode")
            meta = dict(meta, raw_sha256=expected["raw_sha256"])
            pending.append(pool.submit(enc_one, meta, cur[1], cur[2]))
            cur[:] = [None, None, None, None]

        scan(source, segment, end)
        while pending:
            collect_one()
    t_enc = time.perf_counter() - t_enc
    enc_list = [enc_info[b["index"]] for b in inv["blocks"]]
    result.update({"encode_seconds_wall": t_enc, "resumed_blocks": len(done),
                   "encode_failures": sum(1 for e in enc_list if e["status"] != "OK")})
    atomic_json(ds / "status.json", dict(result, phase="decode", updated_at=utc()))

    # ---------------- decode phase (archives only) ----------------
    full_native = hashlib.sha256()
    full_adapted = hashlib.sha256()
    dec_list = []
    t_dec = time.perf_counter()
    blocks = inv["blocks"]
    offsets = []
    acc = 0
    for b in blocks:
        offsets.append(acc)
        acc += b["raw_bytes"]
    audit = {"seconds": 0.0, "kept": 0}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        def dec_one(b):
            idx = b["index"]
            archive = archives / ("block_%06d.lrb" % idx)
            diag = diags / ("%06d" % idx)
            diag.mkdir(exist_ok=True)
            work = Path(tempfile.mkdtemp(prefix="d%06d-" % idx, dir=str(work_root)))
            if not archive.exists():
                return idx, None, None, work, {"status": "NO_ARCHIVE"}
            t0 = time.perf_counter()
            try:
                nat, ada, info = decode_block(archive, work, diag, timeout)
                info["status"] = "DECODED"
            except Exception as exc:
                nat, ada, info = None, None, {"status": "DECODE_FAIL", "error": str(exc),
                                              "traceback": traceback.format_exc()}
            info["block_decode_seconds"] = time.perf_counter() - t0
            return idx, nat, ada, work, info

        pending = []

        def consume():
            idx, nat, ada, work, info = pending.pop(0).result()
            orig = blocks[idx]
            ta = time.perf_counter()  # hashing/audit below is outside the decode timer
            raw = None
            for tag, out, full in (("native", nat, full_native), ("adapted", ada, full_adapted)):
                if out is None:
                    info[tag + "_sha_match"] = False
                    continue
                full.update(out)
                info[tag + "_sha256"] = hashlib.sha256(out).hexdigest()
                info[tag + "_bytes"] = len(out)
                info[tag + "_sha_match"] = info[tag + "_sha256"] == orig["raw_sha256"]
                if not info[tag + "_sha_match"]:
                    if raw is None:
                        with open(source, "rb") as f:
                            f.seek(offsets[idx])
                            raw = f.read(orig["raw_bytes"])
                    info[tag + "_diff"] = diff_summary(raw, out)
                    if tag == "adapted" and audit["kept"] < 5:
                        keep = ds / "mismatch_outputs"
                        keep.mkdir(exist_ok=True)
                        (keep / ("%06d.adapted" % idx)).write_bytes(out)
                        audit["kept"] += 1
            info["block_sha_match"] = info["adapted_sha_match"]
            info["index"] = idx
            dec_list.append(info)
            atomic_json(blocks_dir / ("%06d.dec.json" % idx), info)
            if not keep_work:
                shutil.rmtree(str(work), ignore_errors=True)
            progress["dec"] += 1
            if progress["dec"] % 10 == 0:
                atomic_json(ds / "status.json", dict(result, phase="decode", decoded_blocks=progress["dec"],
                                                     updated_at=utc()))
            audit["seconds"] += time.perf_counter() - ta

        for b in blocks:
            pending.append(pool.submit(dec_one, b))
            if len(pending) >= workers:
                consume()
        while pending:
            consume()
    t_dec = time.perf_counter() - t_dec
    dec_full_native = full_native.hexdigest()
    dec_full_adapted = full_adapted.hexdigest()
    ok_enc = all(e["status"] == "OK" for e in enc_list)
    ad_mism = [d for d in dec_list if not d["adapted_sha_match"]]
    na_mism = [d for d in dec_list if not d["native_sha_match"]]
    adapted_ok = ok_enc and not ad_mism and dec_full_adapted == inv["raw_sha256"]
    native_ok = ok_enc and not na_mism and dec_full_native == inv["raw_sha256"]

    def ssum(key, rows):
        return sum(e.get(key, 0) or 0 for e in rows)

    suffix = [e for e in enc_list if e["index"] >= 1]
    suffix_raw = ssum("raw_bytes", suffix)
    res = [e.get("residual", {}) for e in enc_list]
    result.update({
        "status": "PASS" if adapted_ok else "FAIL",
        "roundtrip": "byte-exact" if adapted_ok else "NOT-LOSSLESS",
        "native_status": "PASS" if native_ok else "NOT-BYTE-EXACT",
        "archive_only_decode": True,
        "decoded_full_sha256_adapted": dec_full_adapted,
        "decoded_full_sha256_native": dec_full_native,
        "full_sha_match": dec_full_adapted == inv["raw_sha256"],
        "native_full_sha_match": dec_full_native == inv["raw_sha256"],
        "blocks_sha_pass": sum(1 for d in dec_list if d["adapted_sha_match"]),
        "blocks_sha_fail": len(ad_mism),
        "native_blocks_sha_pass": sum(1 for d in dec_list if d["native_sha_match"]),
        "native_blocks_sha_fail": len(na_mism),
        "native_blocks_equal_normalized_input": sum(1 for e in enc_list if e.get("native_equals_normalized_input")),
        "native_decode_failures": sum(1 for d in dec_list if d.get("native_decode_error")),
        "native_differing_lines_total": sum(d.get("native_diff", {}).get("differing_lines", 0) for d in na_mism),
        "native_differing_cr_only_total": sum(d.get("native_diff", {}).get("differing_cr_only", 0) for d in na_mism),
        "failed_block_indices": [d["index"] for d in ad_mism][:200],
        "archive_bytes": ssum("archive_bytes", enc_list),
        "native_bytes": ssum("native_bytes", enc_list),
        "residual_bytes": ssum("residual_bytes", enc_list),
        "payload_7z_bytes": ssum("payload_7z_bytes", enc_list),
        "model_7z_bytes": ssum("model_7z_bytes", enc_list),
        "suffix_raw_bytes": suffix_raw,
        "suffix_archive_bytes": ssum("archive_bytes", suffix),
        "suffix_native_bytes": ssum("native_bytes", suffix),
        "residual_full_fallback_blocks": sum(1 for r in res if r.get("full_fallback")),
        "residual_patched_records": sum(r.get("patched_records", 0) for r in res),
        "residual_patch_wrap_whitespace_only": sum(r.get("patch_wrap_whitespace_only", 0) for r in res),
        "residual_patch_edit": sum(r.get("patch_edit", 0) for r in res),
        "residual_regroup_records": sum(r.get("regroup_records", 0) for r in res),
        "encode_seconds_native_sum": ssum("t_native_encode", enc_list),
        "encode_seconds_residual_sum": ssum("t_residual", enc_list),
        "decode_seconds_wall": t_dec,
        "decode_audit_seconds": audit["seconds"],
        "decode_seconds_net": t_dec - audit["seconds"],
        "load_failed_total": ssum("load_failed", enc_list),
        "match_failed_total": ssum("match_failed", enc_list),
        "finished_at": utc(),
    })
    ab, nb = result["archive_bytes"], result["native_bytes"]
    result.update({
        "compression_ratio": inv["raw_bytes"] / ab if ab else None,
        "native_compression_ratio": inv["raw_bytes"] / nb if nb else None,
        "suffix_ratio": suffix_raw / result["suffix_archive_bytes"] if result["suffix_archive_bytes"] else None,
        "native_suffix_ratio": suffix_raw / result["suffix_native_bytes"] if result["suffix_native_bytes"] else None,
        "encode_MB_per_s": inv["raw_bytes"] / t_enc / 1e6 if t_enc else None,
        "decode_MB_per_s": inv["raw_bytes"] / result["decode_seconds_net"] / 1e6 if t_dec else None,
    })
    if not keep_work:
        shutil.rmtree(str(work_root), ignore_errors=True)
    atomic_json(ds / "result.json", result)
    atomic_json(ds / "status.json", result)
    return result


SUMMARY_KEYS = ("dataset", "status", "native_status", "raw_bytes", "archive_bytes", "native_bytes", "residual_bytes",
                "compression_ratio", "native_compression_ratio", "suffix_ratio", "native_suffix_ratio",
                "blocks_sha_pass", "blocks_sha_fail", "full_sha_match", "native_blocks_sha_pass",
                "native_blocks_equal_normalized_input", "native_differing_lines_total",
                "native_differing_cr_only_total", "residual_full_fallback_blocks", "encode_seconds_wall",
                "decode_seconds_net", "error")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", action="append", default=[], metavar="DATASET=PATH")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--workers", type=int, choices=range(1, 5), default=2)
    p.add_argument("--timeout", type=float, default=3600)
    p.add_argument("--resume", action="store_true")
    p.add_argument("--keep-work", action="store_true")
    a = p.parse_args()
    out = a.output.resolve()
    out.mkdir(parents=True, exist_ok=True)
    lock = out / "RUNNING.lock"
    with open(lock, "x") as f:
        json.dump({"pid": os.getpid(), "host": platform.node(), "started_at": utc()}, f)
    try:
        import locale
        manifest = {"schema": SCHEMA, "upstream": "https://github.com/THUBear-wjy/LogReducer",
                    "upstream_commit": UPSTREAM_COMMIT, "seed": SEED, "block_records": BLOCK_RECORDS,
                    "tot_blocksize": TOT_BLOCKSIZE, "residual_lzma": "FORMAT_ALONE preset 9e",
                    "workers": a.workers, "driver_sha256": file_sha(__file__),
                    "binaries": {n: file_sha(LR_DIR / n) for n in ("THULR", "Elastic")},
                    "python_sources": {n: file_sha(LR_DIR / n) for n in
                                       ("sampler.py", "training.py", "logloader.py", "header.py", "parser.py",
                                        "findrule.py", "util.py", "restore.py", "LogReducer.py", "LogRestore.py")},
                    "7za": subprocess.run(["7za"], stdout=subprocess.PIPE).stdout.decode(errors="replace")
                    .strip().splitlines()[0],
                    "python": sys.version, "preferred_encoding": locale.getpreferredencoding(),
                    "LC_ALL": os.environ.get("LC_ALL"), "LANG": os.environ.get("LANG"),
                    "host": platform.node(), "created_at": utc(), "inputs": a.input}
        if manifest["preferred_encoding"].lower().replace("-", "") != "utf8":
            raise RuntimeError("official scripts assume a UTF-8 locale; got %s" % manifest["preferred_encoding"])
        atomic_json(out / ("manifest_%s.json" % datetime.datetime.now().strftime("%Y%m%dT%H%M%S")), manifest)
        results = []
        for spec in a.input:
            name, path = spec.split("=", 1)
            print(json.dumps({"phase": "dataset", "dataset": name, "at": utc()}), flush=True)
            try:
                r = run_dataset(name, Path(path).resolve(), out, a.workers, a.timeout, a.resume, a.keep_work)
            except Exception as exc:
                r = {"dataset": name, "status": "ERROR", "error": str(exc), "traceback": traceback.format_exc()}
                atomic_json(out / name / "result.json", r) if (out / name).exists() else None
            results.append({k: r.get(k) for k in SUMMARY_KEYS})
            print(json.dumps(results[-1]), flush=True)
            atomic_json(out / "summary.json", {"updated_at": utc(), "results": results})
    finally:
        lock.unlink()


if __name__ == "__main__":
    main()
