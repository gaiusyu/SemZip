#!/usr/bin/env python3
"""R76 LogShrink (ICSE'24, github.com/IntelligentDDS/LogShrink @ 59ce4943) independent exact-100,000-line
block baseline with archive-only decoding and per-block + full-file SHA-256 audit.

Per block (fresh processes, nothing shared across blocks; cwd = code_r76/python_compression like run.py):
  1. ./parser/sampler.py <block> <block>.sample 0.001        official sampler, run via runpy after random.seed(0)
                                                              (training.py -m Nor would run it unseeded)
  2. ./parser/training.py -I <block> -T <tpl>/ -L <L> -m Sam  == run.py's training call; -m Sam only skips the
                                                              internal (unseeded) sampler call of step 1
  3. ls_segment.py: run.py's own segmentation loop + utils.util_code.list_write, verbatim
  4. ls_seg_runner.py per non-empty segment k: logshrink.run(...) with run.py's arguments and the paper
     defaults (-E E -C -K lzma -V -S -P -wh 20 -th 4 -NC 16 -mt 0.5, sample_rate 0.1), random/numpy seeded.
     LogShrink's own packer then writes <tmp>/<k>.7z (7z a ... -m0=LZMA).
  5. model.7z = "7z a model.7z -m0=LZMA template.col head.format" (the only template-dir files restore.py reads),
     i.e. the per-block trained templates are counted and packed with LogShrink's own packer command.
  native archive (.lsz) = ">BB" (flags, nseg) + ">I" len(k.7z) per segment + ">I" len(model.7z) + payloads + model
     flags bit0 = the original block has no terminal LF.
Decode (archive only, cwd = python_compression/decompression like decompress_run.py):
  split archive, 7za x model.7z, then per segment the official per-file command
     python3 ./restore_r76.py -I k.7z -O out_k -T tpl/ -t tmp_k/ -K lzma
  restore_r76.py == restore.py plus ONE decoder bug fix (load_log() returned [""] for an empty
  load_failed.log/match_failed.log, which makes restore.py's own assert fail on every block that has no
  load failure). Outputs are concatenated in segment order.
  Decode alignment (documented, tool-level, identical for all datasets):
     (a) run.py's segmentation decodes input as ISO-8859-1 and writes UTF-8; restore.py writes UTF-8. The
         restored bytes are mapped back with .decode("utf-8").encode("latin-1") when both steps succeed,
         otherwise the restored bytes are used unchanged;
     (b) restore.py appends LF to every record; if flag bit0 is set exactly one trailing LF is removed.
Native lossless  <=> aligned restored block SHA-256 == original block SHA-256.
Supplementary "repaired" variant (NOT pre-registered; labeled adapter-repaired): a residual file (.res)
  = xz(preset 6) of per-line corrections between the aligned restored block and the original block
  (format in build_residual). Empty .res means no correction. Repaired decode = native decode + patch;
  its result must match the per-block and full-file SHA-256.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import io
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
import threading
import time
import traceback

BLOCK_RECORDS = 100000  # protocol constant, no CLI override
READ_BYTES = 1024 * 1024
SCHEMA = "semzip.r76.logshrink.v1"
# R76-G copy (unseen/baselines/logshrink_adapter/ls_run_unseen.py) of baselines/logshrink/adapter/ls_run.py.
# Changes (all marked R76-G): helper scripts/code/venv are the LogShrink track's (read-only); raw files and
# inventories are unseen/; the unseen names have no official -L, so only generic L=5 (run.py default) is used;
# scratch goes to unseen/baselines/logshrink_tmp.  Encoder, decoder, residual (+R) and audit code are unchanged.
HERE = Path("<WORKDIR>/r76_additional_20260929/baselines/logshrink/adapter")  # R76-G
UNSEEN = Path("<WORKDIR>/r76_additional_20260929/unseen")  # R76-G
TRACK = HERE.parent
CODE = TRACK / "code_r76"
PC = CODE / "python_compression"
DEC = PC / "decompression"
VENV_BIN = TRACK / "venv" / "bin"
PY = str(VENV_BIN / "python3")
UPSTREAM_COMMIT = "59ce49434eec06c709c7e16027a46214a6c37961"
SEED = 0
PROJECT = Path("<WORKDIR>")
RAW_DIR = UNSEEN / "raw"  # R76-G
R68_INV = None  # R76-G: inventory = unseen/baselines/main/<D>/input_<D>.json (same harness inventory function as R68)

# Verbatim copy of header_length in exp_shell_scripts/run_logshrink.py (checked against the file at start-up).
OFFICIAL_L = {'Android': 6, 'Apache': 6, 'BGL': 9, 'Hadoop': 5, 'HDFS': 5, 'HealthApp': 3, 'HPC': 6,
              'Linux': 5, 'Mac': 5, 'OpenSSH': 5, 'OpenStack': 7, 'Proxifier': 4, 'Spark': 4,
              'Thunderbird': 9, 'Windows': 4, 'Zookeeper': 6}
GENERIC_L = 5  # run.py's argparse default for -L (used when no dataset-specific value is given)

SEEDED_SAMPLER = ("import random, runpy, sys; random.seed(%d); "
                  "sys.argv = ['sampler.py'] + sys.argv[1:]; "
                  "runpy.run_path('./parser/sampler.py', run_name='__main__')" % SEED)
THULR_STATS = [("read_logs", rb"read logs: (\d+)"), ("load_failed", rb"load failed: (\d+)"),
               ("match_failed", rb"match failed: (\d+)"), ("templates_loaded", rb"read (\d+) templates")]


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp.%d.%d" % (os.getpid(), threading.get_ident()))
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


# scan(): verbatim copy of r68_external_20260923/codec_baseline.py scan() (same block law).
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


def clip(data, head=4096, tail=12288):
    if len(data) <= head + tail:
        return data
    return data[:head] + b"\n...[clipped %d bytes]...\n" % (len(data) - head - tail) + data[-tail:]


def child_env():
    env = dict(os.environ)
    env["PATH"] = str(VENV_BIN) + os.pathsep + env.get("PATH", "")
    env["PYTHONHASHSEED"] = "0"
    env["LC_ALL"] = "en_US.UTF-8"
    env["LANG"] = "en_US.UTF-8"
    env.pop("PYTHONPATH", None)
    return env


def run(cmd, cwd, diag, name, timeout, shell=False):
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=child_env(),
                          timeout=timeout, shell=shell, check=False)
    dt = time.perf_counter() - t0
    diag.mkdir(parents=True, exist_ok=True)
    with open(diag / (name + ".stdout"), "wb") as f:
        f.write(clip(proc.stdout))
    with open(diag / (name + ".stderr"), "wb") as f:
        f.write(clip(proc.stderr))
    if proc.returncode != 0:
        raise RuntimeError("%s exit %d; see %s" % (name, proc.returncode, diag / (name + ".stderr")))
    return dt, proc.stdout


# ----------------------------------------------------------------------------------------------- archive
def pack_archive(flags, payloads, model):
    head = struct.pack(">BB", flags, len(payloads)) + b"".join(struct.pack(">I", len(p)) for p in payloads)
    head += struct.pack(">I", len(model))
    return head + b"".join(payloads) + model, len(head)


def unpack_archive(data):
    flags, nseg = struct.unpack(">BB", data[:2])
    pos = 2
    lens = []
    for _ in range(nseg):
        lens.append(struct.unpack(">I", data[pos:pos + 4])[0])
        pos += 4
    mlen = struct.unpack(">I", data[pos:pos + 4])[0]
    pos += 4
    payloads = []
    for n in lens:
        payloads.append(data[pos:pos + n])
        pos += n
    model = data[pos:pos + mlen]
    if len(model) != mlen or pos + mlen != len(data) or any(len(p) != n for p, n in zip(payloads, lens)):
        raise RuntimeError("malformed archive")
    return flags, payloads, model


# ----------------------------------------------------------------------------------------------- encode
def encode_block(block, work, L, diag, timeout, dataset):
    info = {"stages": {}}
    enc = work / "enc"
    seg = enc / "seg"
    tpl = enc / "tpl"
    tmp = enc / "tmp"
    for d in (seg, tpl, tmp):
        d.mkdir(parents=True)
    raw = enc / "block.log"
    os.link(str(block), str(raw))
    st = info["stages"]
    # 1-2. training (run.py: python3 ./parser/training.py -I input -T template_path -L header_length)
    st["sampler"], _ = run([PY, "-c", SEEDED_SAMPLER, str(raw), str(raw) + ".sample", "0.001"], PC, diag,
                           "1_sampler", timeout)
    st["training"], _ = run([PY, "./parser/training.py", "-I", str(raw), "-T", str(tpl) + "/", "-L", str(L),
                             "-m", "Sam"], PC, diag, "2_training", timeout)
    # 3. run.py segmentation (verbatim loop + official list_write)
    st["segment"], out = run([PY, str(HERE / "ls_segment.py"), str(PC), str(raw), str(seg)], PC, diag,
                             "3_segment", timeout)
    counts = json.loads(re.search(rb"SEGMENT_COUNTS (\[.*\])", out).group(1))
    info["segment_line_counts"] = counts
    nonempty = [k for k, c in enumerate(counts) if c > 0]
    info["skipped_empty_segments"] = [k for k, c in enumerate(counts) if c == 0]
    if not nonempty:
        raise RuntimeError("no non-empty segment")
    # 4. logshrink.run per segment (run.py procFiles call), LogShrink packer -> tmp/<k>.7z
    payloads = []
    st["logshrink"] = 0.0
    stats = []
    for k in nonempty:
        dt, out = run([PY, str(HERE / "ls_seg_runner.py"), str(PC), str(seg / ("%d.col" % k)), str(tmp / str(k)),
                       str(tpl) + "/", dataset, str(k), str(SEED)], PC, diag, "4_logshrink_seg%d" % k, timeout)
        st["logshrink"] += dt
        s = {"segment": k}
        for key, rx in THULR_STATS:
            m = re.search(rx, out)
            s[key] = int(m.group(1)) if m else None
        p = tmp / ("%d.7z" % k)
        if not p.is_file():
            raise RuntimeError("LogShrink packer produced no %s" % p)
        s["payload_files"] = len(os.listdir(str(tmp / str(k))))
        stats.append(s)
        payloads.append(p.read_bytes())
    info["segments"] = stats
    # 5. decoder model: the files restore.py opens from the template directory, LogShrink packer command
    names = ["template.col", "head.format"]
    for n in names:
        if not (tpl / n).is_file():
            raise RuntimeError("training did not produce decoder file %s" % n)
    st["model_7z"], _ = run("7z a " + str(enc / "model") + " template.col head.format -m0=LZMA", tpl, diag,
                            "5_model7z", timeout, shell=True)
    model = (enc / "model.7z").read_bytes()
    info["model_raw_bytes"] = sum((tpl / n).stat().st_size for n in names)
    info["model_7z_bytes"] = len(model)
    info["payload_7z_bytes"] = sum(len(p) for p in payloads)
    return info, payloads, model


# ----------------------------------------------------------------------------------------------- decode
def decode_block(archive, work, diag, timeout):
    """Archive-only decode of the native LogShrink archive. Returns (aligned_output_path, info)."""
    info = {"stages": {}}
    t0 = time.perf_counter()
    dec = work / "dec"
    tpl = dec / "tpl"
    tpl.mkdir(parents=True)
    flags, payloads, model = unpack_archive(archive.read_bytes())
    (dec / "model.7z").write_bytes(model)
    run(["7za", "x", str(dec / "model.7z"), "-o" + str(tpl), "-y"], dec, diag, "6_model_x", timeout)
    parts = []
    for k, p in enumerate(payloads):
        (dec / ("%d.7z" % k)).write_bytes(p)
        out = dec / ("%d.col" % k)
        order = ("python3 ./restore_r76.py -I " + str(dec / ("%d.7z" % k)) + " -O " + str(out) + " -T " +
                 str(tpl) + "/ -t " + str(dec / ("rtmp%d" % k)) + "/ -K lzma")
        run(order, DEC, diag, "7_restore_seg%d" % k, timeout, shell=True)
        if not out.is_file():
            raise RuntimeError("restore produced no output for segment %d" % k)
        parts.append(out)
    data = b"".join(p.read_bytes() for p in parts)
    info["restored_bytes_before_alignment"] = len(data)
    try:
        aligned = data.decode("utf-8").encode("latin-1")
        info["alignment_transcode"] = True
    except (UnicodeDecodeError, UnicodeEncodeError):
        aligned = data
        info["alignment_transcode"] = False
    if flags & 1 and aligned.endswith(b"\n"):
        aligned = aligned[:-1]
    outp = work / "native_decoded.bin"
    outp.write_bytes(aligned)
    info["stages"]["decode_total"] = time.perf_counter() - t0
    return outp, info


# ----------------------------------------------------------------------------------------------- residual
WS = b" \t\n\r\x0b\x0c"


def _uv(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _rv(buf, pos):
    n = shift = 0
    while True:
        b = buf[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        shift += 7
        if not b & 0x80:
            return n, pos


def build_residual(orig, dec):
    """Per-line correction list, xz preset 6. Empty bytes when dec == orig.

    body kind 0 (same LF-split line count): varint(n), then per differing line:
      varint(index delta), varint(len lead)+lead, varint(len trail)+trail   [lead/trail = ASCII whitespace that
      bytes.strip() removes from the original line], byte type:
        0 -> original = lead + decoded_line + trail
        1 -> core = decoded_line[:p] + mid + decoded_line[p+dlen:]; varint(p), varint(dlen), varint(len mid)+mid
    body kind 1 (line counts differ): the original block bytes verbatim (full fallback).
    """
    if orig == dec:
        return b"", {"differing_lines": 0, "strip_only": 0, "general": 0, "fallback": False}
    a = orig.split(b"\n")
    b = dec.split(b"\n")
    st = {"orig_lines": len(a), "decoded_lines": len(b)}
    if len(a) != len(b):
        body = b"\x01" + orig
        st.update({"differing_lines": None, "strip_only": 0, "general": 0, "fallback": True})
        return lzma.compress(body, preset=6), st
    recs = []
    prev = 0
    nstrip = ngen = 0
    for i, (o, d) in enumerate(zip(a, b)):
        if o == d:
            continue
        s1 = o.lstrip(WS)
        lead = o[:len(o) - len(s1)]
        core = s1.rstrip(WS)
        trail = s1[len(core):]
        r = [_uv(i - prev), _uv(len(lead)), lead, _uv(len(trail)), trail]
        prev = i
        if core == d:
            r.append(b"\x00")
            nstrip += 1
        else:
            p = 0
            m = min(len(core), len(d))
            while p < m and core[p] == d[p]:
                p += 1
            s = 0
            while s < m - p and core[len(core) - 1 - s] == d[len(d) - 1 - s]:
                s += 1
            dlen = len(d) - p - s
            mid = core[p:len(core) - s]
            r += [b"\x01", _uv(p), _uv(dlen), _uv(len(mid)), mid]
            ngen += 1
        recs.append(b"".join(r))
    body = b"\x00" + _uv(len(recs)) + b"".join(recs)
    st.update({"differing_lines": len(recs), "strip_only": nstrip, "general": ngen, "fallback": False})
    return lzma.compress(body, preset=6), st


def apply_residual(res, dec):
    if not res:
        return dec
    body = lzma.decompress(res)
    if body[0] == 1:
        return body[1:]
    lines = dec.split(b"\n")
    n, pos = _rv(body, 1)
    idx = 0
    for _ in range(n):
        delta, pos = _rv(body, pos)
        idx += delta
        ln, pos = _rv(body, pos)
        lead = body[pos:pos + ln]
        pos += ln
        ln, pos = _rv(body, pos)
        trail = body[pos:pos + ln]
        pos += ln
        typ = body[pos]
        pos += 1
        d = lines[idx]
        if typ == 0:
            core = d
        else:
            p, pos = _rv(body, pos)
            dlen, pos = _rv(body, pos)
            ln, pos = _rv(body, pos)
            mid = body[pos:pos + ln]
            pos += ln
            core = d[:p] + mid + d[p + dlen:]
        lines[idx] = lead + core + trail
    if pos != len(body):
        raise RuntimeError("residual trailing bytes")
    return b"\n".join(lines)


def diff_example(orig, dec, n_ws=2, n_general=5):
    a = orig.split(b"\n")
    b = dec.split(b"\n")
    ex = []
    nw = ng = 0
    for i, (x, y) in enumerate(zip(a, b)):
        if x == y:
            continue
        general = x.strip(WS) != y
        if (general and ng < n_general) or (not general and nw < n_ws):
            ex.append({"line": i, "kind": "general" if general else "whitespace", "orig": x[:240].decode("latin-1"),
                       "decoded": y[:240].decode("latin-1")})
            ng += general
            nw += not general
        if ng >= n_general and nw >= n_ws:
            break
    return ex


# ----------------------------------------------------------------------------------------------- driver
class Dataset:
    def __init__(self, name, out_root, tmp_root, workers, timeout, L, resume, keep_work, max_blocks=0):
        self.name = name
        self.src = Path(os.path.realpath(str(RAW_DIR / (name + ".log"))))
        self.ds = out_root / name
        self.arch = self.ds / "archives"
        self.bdir = self.ds / "blocks"
        self.diag = self.ds / "diagnostics"
        for d in (self.arch, self.bdir, self.diag):
            d.mkdir(parents=True, exist_ok=True)
        self.tmp = tmp_root / name
        if self.tmp.exists():
            shutil.rmtree(str(self.tmp))
        self.tmp.mkdir(parents=True)
        self.workers = workers
        self.timeout = timeout
        self.L = L
        self.resume = resume
        self.keep_work = keep_work
        self.max_blocks = max_blocks
        with open(UNSEEN / "baselines" / "main" / name / ("input_%s.json" % name)) as f:  # R76-G
            self.inv = json.load(f)
        self.lock = threading.Lock()
        self.sem = threading.BoundedSemaphore(workers + 2)
        self.ready = {}
        self.next_hash = 0
        self.h_native = hashlib.sha256()
        self.h_repaired = hashlib.sha256()
        self.native_stream_ok = True
        self.repaired_stream_ok = True
        self.results = {}

    def block_task(self, meta, block_path):
        i = meta["index"]
        work = self.tmp / ("b%06d" % i)
        diag = self.diag / ("block_%06d" % i)
        lsz = self.arch / ("block_%06d.lsz" % i)
        res = self.arch / ("block_%06d.res" % i)
        rec_path = self.bdir / ("block_%06d.json" % i)
        rec = {"index": i, "raw_bytes": meta["raw_bytes"], "records": meta["records"],
               "ends_with_lf": meta["ends_with_lf"], "raw_sha256": meta["raw_sha256"], "L": self.L,
               "started_at": utc()}
        native_out = repaired = None
        try:
            prev = None
            if self.resume and rec_path.exists():
                prev = json.load(open(rec_path))
                if not (prev.get("status") in ("ok", "native_decode_failed", "failed") and lsz.exists()
                        and prev.get("lsz_sha256") and prev.get("raw_sha256") == meta["raw_sha256"]
                        and prev.get("L") == self.L and file_sha(lsz) == prev.get("lsz_sha256")):
                    prev = None
            if prev is None:
                if diag.exists():
                    shutil.rmtree(str(diag))
                t0 = time.perf_counter()
                einfo, payloads, model = encode_block(block_path, work, self.L, diag, self.timeout, self.name)
                blob, head_len = pack_archive(0 if meta["ends_with_lf"] else 1, payloads, model)
                tmpf = lsz.with_name(lsz.name + ".part")
                tmpf.write_bytes(blob)
                os.replace(str(tmpf), str(lsz))
                rec["encode_seconds"] = time.perf_counter() - t0
                rec["encode"] = einfo
                rec["header_bytes"] = head_len
                rec["lsz_bytes"] = lsz.stat().st_size
                rec["lsz_sha256"] = file_sha(lsz)
                rec["resumed"] = False
            else:
                for k in ("encode_seconds", "encode", "header_bytes", "lsz_bytes", "lsz_sha256"):
                    rec[k] = prev[k]
                rec["resumed"] = True
            # archive-only native decode
            t0 = time.perf_counter()
            try:
                native_out, dinfo = decode_block(lsz, work, diag, self.timeout)
            except Exception as e:  # a failed decode is a reported result
                rec["native_decode_error"] = "%s: %s" % (type(e).__name__, e)
                rec["decode_seconds"] = time.perf_counter() - t0
                rec["status"] = "native_decode_failed"
                if res.exists():
                    res.unlink()
                return rec
            rec["decode_seconds"] = time.perf_counter() - t0
            rec["decode"] = dinfo
            dec = native_out.read_bytes()
            rec["native_decoded_sha256"] = hashlib.sha256(dec).hexdigest()
            rec["native_lossless"] = rec["native_decoded_sha256"] == meta["raw_sha256"]
            # residual (encoder side of the repaired variant): needs the original block. Always recomputed
            # (also on resume, where only the native LogShrink encode is reused).
            orig = block_path.read_bytes()
            if hashlib.sha256(orig).hexdigest() != meta["raw_sha256"]:
                raise RuntimeError("block spool changed")
            t0 = time.perf_counter()
            rbytes, rst = build_residual(orig, dec)
            rec["residual_seconds"] = time.perf_counter() - t0
            rec["residual_stats"] = rst
            if not rec["native_lossless"]:
                rec["mismatch_examples"] = diff_example(orig, dec)
            del orig
            tmpf = res.with_name(res.name + ".part")
            tmpf.write_bytes(rbytes)
            os.replace(str(tmpf), str(res))
            rec["res_bytes"] = len(rbytes)
            rec["res_sha256"] = file_sha(res)
            # repaired decode: archive-only native decode output + residual file only
            t0 = time.perf_counter()
            fixed = apply_residual(res.read_bytes(), dec)
            rec["patch_seconds"] = time.perf_counter() - t0
            rec["repaired_decoded_sha256"] = hashlib.sha256(fixed).hexdigest()
            rec["repaired_lossless"] = rec["repaired_decoded_sha256"] == meta["raw_sha256"]
            repaired = work / "repaired_decoded.bin"
            repaired.write_bytes(fixed)
            del fixed, dec
            rec["status"] = "ok"
            return rec
        except Exception as e:
            rec["status"] = "encode_failed" if "lsz_bytes" not in rec else "failed"
            rec["error"] = "%s: %s" % (type(e).__name__, e)
            rec["traceback"] = traceback.format_exc()[-4000:]
            return rec
        finally:
            rec["finished_at"] = utc()
            atomic_json(rec_path, rec)
            with self.lock:
                self.results[i] = rec
                self.ready[i] = (native_out, repaired)
                self._drain()

    def _drain(self):
        while self.next_hash in self.ready:
            i = self.next_hash
            native_out, repaired = self.ready.pop(i)
            rec = self.results[i]
            if native_out is not None and native_out.exists():
                with open(native_out, "rb") as f:
                    self.h_native.update(f.read())
            else:
                self.native_stream_ok = False
            if repaired is not None and repaired.exists():
                with open(repaired, "rb") as f:
                    self.h_repaired.update(f.read())
            else:
                self.repaired_stream_ok = False
            work = self.tmp / ("b%06d" % i)
            if not self.keep_work and work.exists():
                shutil.rmtree(str(work), ignore_errors=True)
            self.next_hash += 1
            self.sem.release()

    def run(self):
        started = utc()
        t_wall = time.perf_counter()
        exp_blocks = self.inv["blocks"]
        if file_identity(self.src)["bytes"] != self.inv["raw_bytes"]:
            raise RuntimeError("source size differs from R68 inventory")
        full = hashlib.sha256()
        state = {"f": None, "h": None, "path": None, "i": 0}
        futures = []
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.workers)

        class Stop(Exception):
            pass

        def segment(piece):
            if self.max_blocks and state["i"] >= self.max_blocks:
                raise Stop()
            if state["f"] is None:
                self.sem.acquire()
                i = state["i"]
                work = self.tmp / ("b%06d" % i)
                work.mkdir(parents=True)
                state["path"] = work / "block.log"
                state["f"] = open(state["path"], "wb")
                state["h"] = hashlib.sha256()
            state["f"].write(piece)
            state["h"].update(piece)
            full.update(piece)

        def end(meta):
            state["f"].close()
            meta["raw_sha256"] = state["h"].hexdigest()
            i = meta["index"]
            exp = exp_blocks[i]
            for k in ("raw_bytes", "records", "ends_with_lf", "raw_sha256"):
                if exp[k] != meta[k]:
                    raise RuntimeError("block %d %s differs from R68 inventory" % (i, k))
            futures.append(pool.submit(self.block_task, meta, state["path"]))
            state["f"] = None
            state["i"] += 1

        try:
            scan(self.src, segment, end)
            partial = False
        except Stop:
            partial = True
        if not partial and len(futures) != len(exp_blocks):
            raise RuntimeError("block count differs from R68 inventory")
        full_sha = None if partial else full.hexdigest()
        pool.shutdown(wait=True)
        for fu in futures:
            fu.result()
        wall = time.perf_counter() - t_wall
        return self.summarize(started, wall, full_sha, partial)

    def summarize(self, started, wall, full_sha, partial=False):
        recs = [self.results[i] for i in sorted(self.results)]
        raw = sum(r["raw_bytes"] for r in recs) if partial else self.inv["raw_bytes"]
        ok = [r for r in recs if r.get("status") == "ok"]
        allok = len(ok) == len(recs)
        native_bytes = sum(r.get("lsz_bytes", 0) for r in recs)
        rep_bytes = native_bytes + sum(r.get("res_bytes", 0) for r in recs)
        suffix = recs[1:]
        s_raw = sum(r["raw_bytes"] for r in suffix)
        s_nat = sum(r.get("lsz_bytes", 0) for r in suffix)
        s_rep = s_nat + sum(r.get("res_bytes", 0) for r in suffix)
        n_native_ok = sum(1 for r in recs if r.get("native_lossless"))
        n_rep_ok = sum(1 for r in recs if r.get("repaired_lossless"))
        native_full = (not partial) and self.native_stream_ok and self.h_native.hexdigest() == self.inv["raw_sha256"]
        rep_full = (not partial) and self.repaired_stream_ok and self.h_repaired.hexdigest() == self.inv["raw_sha256"]
        enc_s = sum(r.get("encode_seconds", 0) for r in recs)
        dec_s = sum(r.get("decode_seconds", 0) for r in recs)
        result = {
            "schema": SCHEMA, "dataset": self.name, "started_at": started, "finished_at": utc(),
            "partial_first_blocks_only": partial,
            "upstream": "github.com/IntelligentDDS/LogShrink", "upstream_commit": UPSTREAM_COMMIT,
            "header_length_L": self.L, "L_mode": "official" if self.L == OFFICIAL_L.get(self.name) else "generic",  # R76-G: .get
            "params": "-E E -C -K lzma -V -S -P -wh 20 -th 4 -NC 16 -mt 0.5 (sample_rate 0.1), seed %d" % SEED,
            "workers": self.workers, "source": file_identity(self.src), "raw_bytes": raw,
            "raw_sha256": self.inv["raw_sha256"], "stream_full_sha256": full_sha,
            "input_matches_r68_inventory": (full_sha == self.inv["raw_sha256"]) if not partial else "per-block only",
            "blocks": len(recs), "blocks_status_ok": len(ok),
            "failed_blocks": [{"index": r["index"], "status": r.get("status"),
                               "error": r.get("error") or r.get("native_decode_error")} for r in recs
                              if r.get("status") != "ok"],
            "native": {"archive_bytes": native_bytes, "ratio": raw / native_bytes if native_bytes else None,
                       "suffix_raw_bytes": s_raw, "suffix_archive_bytes": s_nat,
                       "suffix_ratio": s_raw / s_nat if s_nat else None,
                       "lossless_blocks": n_native_ok, "full_sha256_match": native_full,
                       "lossless": native_full and n_native_ok == len(recs) and allok,
                       "payload_7z_bytes": sum(r.get("encode", {}).get("payload_7z_bytes", 0) for r in recs),
                       "model_7z_bytes": sum(r.get("encode", {}).get("model_7z_bytes", 0) for r in recs),
                       "model_raw_bytes": sum(r.get("encode", {}).get("model_raw_bytes", 0) for r in recs),
                       "header_bytes": sum(r.get("header_bytes", 0) for r in recs)},
            "repaired": {"archive_bytes": rep_bytes, "ratio": raw / rep_bytes if rep_bytes else None,
                         "suffix_archive_bytes": s_rep, "suffix_ratio": s_raw / s_rep if s_rep else None,
                         "residual_bytes": rep_bytes - native_bytes, "lossless_blocks": n_rep_ok,
                         "full_sha256_match": rep_full, "lossless": rep_full and n_rep_ok == len(recs) and allok,
                         "fallback_blocks": sum(1 for r in recs if r.get("residual_stats", {}).get("fallback")),
                         "differing_lines": sum((r.get("residual_stats", {}).get("differing_lines") or 0)
                                                for r in recs)},
            "timing": {"wall_seconds_encode_decode_audit": wall,
                       "sum_block_encode_seconds": enc_s, "sum_block_decode_seconds": dec_s,
                       "sum_residual_seconds": sum(r.get("residual_seconds", 0) for r in recs),
                       "sum_patch_seconds": sum(r.get("patch_seconds", 0) for r in recs),
                       "encode_MB_per_s_single_worker": raw / enc_s / 1e6 if enc_s else None,
                       "decode_MB_per_s_single_worker": raw / dec_s / 1e6 if dec_s else None,
                       "resumed_blocks": sum(1 for r in recs if r.get("resumed"))},
            "host": {"node": platform.node(), "python": platform.python_version()},
        }
        atomic_json(self.ds / ("result_partial.json" if partial else "result.json"), result)
        if not self.keep_work:
            shutil.rmtree(str(self.tmp), ignore_errors=True)
        return result


def check_static():
    shipped = (PC / "exp_shell_scripts" / "run_logshrink.py").read_text()
    m = re.search(r"header_length = (\{.*?\})", shipped, re.S)
    import ast
    if ast.literal_eval(m.group(1)) != OFFICIAL_L:
        raise RuntimeError("OFFICIAL_L differs from shipped header_length")
    hashes = {}
    for rel in ["parser/THULR", "parser/Elastic", "decompression/restore.py", "decompression/restore_r76.py",
                "decompression/decompress.py", "decompression/decompress_r76.py", "logshrink.py", "compression/compress.py", "preprocess.py",
                "parser/training.py", "parser/sampler.py", "run.py", "analyzer/property_miner.py"]:
        hashes[rel] = file_sha(PC / rel)
    for rel in ["ls_run.py", "ls_seg_runner.py", "ls_segment.py", "make_restore_r76.py"]:
        hashes["adapter/" + rel] = file_sha(HERE / rel)
    hashes["unseen/ls_run_unseen.py"] = file_sha(Path(__file__).resolve())  # R76-G
    return hashes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", nargs="+", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--tmp-root", default=str(UNSEEN / "baselines" / "logshrink_tmp"))  # R76-G
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--timeout", type=int, default=6 * 3600)
    ap.add_argument("--L-mode", choices=["generic"], default="generic")  # R76-G: unseen names have no official L
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--keep-work", action="store_true")
    ap.add_argument("--max-blocks", type=int, default=0, help="timing probe: only the first N blocks (partial)")
    args = ap.parse_args()
    if args.workers < 1 or args.workers > 4:
        raise SystemExit("workers must be 1..4")
    out = Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    tmp_root = Path(args.tmp_root) / (out.name + "_%d" % os.getpid())
    camp = {"schema": SCHEMA, "started_at": utc(), "argv": sys.argv, "pid": os.getpid(),
            "static_hashes": check_static(), "datasets": {}}
    atomic_json(out / "campaign.json", camp)
    for name in args.datasets:
        L = OFFICIAL_L[name] if args.L_mode == "official" else GENERIC_L
        print("[%s] %s start L=%d" % (utc(), name, L), flush=True)
        try:
            r = Dataset(name, out, tmp_root, args.workers, args.timeout, L, args.resume, args.keep_work,
                        args.max_blocks).run()
            camp["datasets"][name] = {k: r[k] for k in ("blocks", "blocks_status_ok", "native", "repaired",
                                                         "timing", "header_length_L")}
            print("[%s] %s done native=%.3f (lossless=%s, %d/%d blocks) repaired=%.3f (lossless=%s) "
                  "enc=%.1fs dec=%.1fs" % (utc(), name, r["native"]["ratio"] or 0, r["native"]["lossless"],
                                          r["native"]["lossless_blocks"], r["blocks"],
                                          r["repaired"]["ratio"] or 0, r["repaired"]["lossless"],
                                          r["timing"]["sum_block_encode_seconds"],
                                          r["timing"]["sum_block_decode_seconds"]), flush=True)
        except Exception as e:
            camp["datasets"][name] = {"error": "%s: %s" % (type(e).__name__, e),
                                      "traceback": traceback.format_exc()[-4000:]}
            print("[%s] %s ERROR %s" % (utc(), name, e), flush=True)
        atomic_json(out / "campaign.json", camp)
    camp["finished_at"] = utc()
    atomic_json(out / "campaign.json", camp)


if __name__ == "__main__":
    main()
