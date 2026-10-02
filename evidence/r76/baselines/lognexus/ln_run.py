#!/usr/bin/env python3
"""R76-H LogNexus (ISSTA 2026; peer-reviewed version of arXiv 2601.17482 "LogPrism"; artifact
Zenodo 10.5281/zenodo.21021398, LogNexus-issta26-ae-source.tar.gz sha256 6baececc...) baseline:
independent exact-100,000-line blocks, official CLI per block, archive-only decoding.

Two results from ONE archive per block (both decoded from the archive alone):

  native   = LogNexus as released: LogNexus_compress <block> <dataset> 100000 1 1 1 <tau>, every file it
             writes into its output directory stored and counted; decoded with LogNexus_decompress.
             The artifact claims only token-stream restoration (whitespace normalized, blank lines may be
             omitted), so native is byte-exact only on some inputs. Verdict per the pre-registered rule;
             we also check token-stream equality (bytes.split(), the artifact verifier's criterion).
  adapted  = native + a generic, dataset-agnostic, counted residual ("LogNexus+R"), LZMA 9e:
             (a) blank / whitespace-only records the restored text omits: position + exact bytes;
             (b) per differing record: if the whitespace-split token lists are equal, only the
                 separators (leading, gaps, trailing) that differ (type 3); otherwise the unchanged
                 LogReducer+R wrap/edit patch (types 1/2);
             full-block LZMA fallback if records cannot be aligned (counted and reported).
archive = [flags u8][len u8][dataset name][nfiles u8]{[len u8][name][u32 size]}* files [u32 residual len] residual
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
READ_BYTES = 1024 * 1024
SCHEMA = "semzip.r76.lognexus.v1"
HERE = Path(__file__).resolve().parent
LN_DIR = HERE / "src" / "LogNexus-issta26-ae"
LN_BIN = LN_DIR / "build"
ZENODO_RECORD = "10.5281/zenodo.21021398"
SOURCE_TARBALL_SHA256 = "6baececc1a52594ca419f1198bd996cd437615f5ae79dccdc2339da45ce81678"
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
    proc = subprocess.run(cmd, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          timeout=timeout, shell=shell, check=False)
    dt = time.perf_counter() - t0
    with open(diag / (name + ".stdout"), "wb") as f:
        f.write(clip(proc.stdout))
    with open(diag / (name + ".stderr"), "wb") as f:
        f.write(clip(proc.stderr))
    if proc.returncode != 0:
        raise RuntimeError("%s exit %d; see %s" % (name, proc.returncode, diag / (name + ".stderr")))
    return dt, proc.stdout


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


LN_NAMES = ("Android", "Apache", "BGL", "Hadoop", "HDFS", "HealthApp", "HPC", "Linux", "Mac", "OpenSSH",
            "OpenStack", "Proxifier", "Spark", "Thunderbird", "Windows", "Zookeeper")
CFG = {}  # set per dataset in main(): {"ln_dataset": name passed to LogNexus, "tau": threshold string}
WS = b" \t\r\n\x0b\x0c"  # bytes.split() whitespace = token criterion of scripts/verify_token_roundtrip.py
TOK = re.compile(rb"[^ \t\r\n\x0b\x0c]+")


def load_thresholds():
    rows = {}
    with open(LN_DIR / "configs" / "paper_thresholds.csv", encoding="utf-8") as f:
        next(f)
        for line in f:
            name, tau = line.strip().split(",")
            rows[name] = tau
    return rows


# ---------------------------------------------------------------- residual (generic, counted)
def split_ws(rec):
    """rec -> (tokens, separators); len(separators) == len(tokens) + 1 (leading, gaps, trailing)."""
    toks, seps, pos = [], [], 0
    for m in TOK.finditer(rec):
        seps.append(rec[pos:m.start()])
        toks.append(m.group())
        pos = m.end()
    seps.append(rec[pos:])
    return toks, seps


def native_records(native_out):
    """native_out = LogNexus restored text (one LF after every restored record)."""
    recs = native_out.split(b"\n")
    if recs and recs[-1] == b"":
        recs.pop()
    return recs


def line_patch(o, d, patches):
    """Append one patch turning restored record d into original o. Returns patch type."""
    to, so = split_ws(o)
    td, sd = split_ws(d)
    if to == td:  # type 3: same token list, only separators differ -> store the differing separators
        diff = [(j, so[j]) for j in range(len(so)) if so[j] != sd[j]]
        patches.append(3)
        varint(patches, len(diff))
        prev = -1
        for j, s in diff:
            varint(patches, j - prev - 1)
            prev = j
            varint(patches, len(s))
            patches += s
        return 3
    # types 1/2: the LogReducer+R wrap/edit patch, unchanged
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
        varint(patches, len(x))
        patches += x
        varint(patches, len(y))
        patches += y
    else:
        varint(patches, x[0])
        varint(patches, x[1])
        varint(patches, len(y))
        patches += y
    return typ


def build_residual(orig, ends_with_lf, native_out):
    """Return (residual_bytes, stats). orig = original block bytes."""
    O = records_of(orig, ends_with_lf)
    stats = {"records": len(O)}
    raw = bytearray()
    D = native_records(native_out) if native_out is not None else None
    if D is not None and len(D) == len(O):
        keep = None
    elif D is not None and len(D) < len(O):
        keep = [i for i, r in enumerate(O) if r.strip(WS)]
        if len(keep) != len(D):
            keep = False
    else:
        keep = False
    if keep is False:
        stats.update({"full_fallback": True, "native_records": None if D is None else len(D)})
        raw.append(1)
        raw += orig
        return lzma.compress(bytes(raw), **LZMA_RESIDUAL), stats
    raw.append(0)
    # records LogNexus omitted (blank / whitespace-only): positions + exact bytes
    dropped = [] if keep is None else sorted(set(range(len(O))) - set(keep))
    varint(raw, len(dropped))
    prev = -1
    for i in dropped:
        varint(raw, i - prev - 1)
        prev = i
        varint(raw, len(O[i]))
        raw += O[i]
    kept = range(len(O)) if keep is None else keep
    patches = bytearray()
    n_patch = 0
    by_type = {1: 0, 2: 0, 3: 0}
    prev = -1
    for k, i in enumerate(kept):
        o, d = O[i], D[k]
        if d == o:
            continue
        n_patch += 1
        varint(patches, k - prev - 1)
        prev = k
        by_type[line_patch(o, d, patches)] += 1
    varint(raw, n_patch)
    raw += patches
    stats.update({"full_fallback": False, "dropped_records": len(dropped), "patched_records": n_patch,
                  "patch_separator": by_type[3], "patch_wrap": by_type[1], "patch_edit": by_type[2],
                  "residual_raw_bytes": len(raw)})
    if not dropped and not n_patch:
        return b"", stats
    return lzma.compress(bytes(raw), **LZMA_RESIDUAL), stats


def apply_residual(residual, ends_with_lf, native_out):
    if not residual:
        return b"\n".join(native_records(native_out)) + (b"\n" if ends_with_lf else b"")
    raw = lzma.decompress(residual, format=lzma.FORMAT_ALONE)
    if raw[0] == 1:
        return bytes(raw[1:])
    pos = 1
    n_drop, pos = read_varint(raw, pos)
    dropped = {}
    prev = -1
    for _ in range(n_drop):
        gap, pos = read_varint(raw, pos)
        prev = prev + gap + 1
        n, pos = read_varint(raw, pos)
        dropped[prev] = bytes(raw[pos:pos + n])
        pos += n
    D = native_records(native_out)
    n_patch, pos = read_varint(raw, pos)
    prev = -1
    for _ in range(n_patch):
        gap, pos = read_varint(raw, pos)
        k = prev + gap + 1
        prev = k
        typ = raw[pos]
        pos += 1
        d = D[k]
        if typ == 3:
            toks, seps = split_ws(d)
            n, pos = read_varint(raw, pos)
            j = -1
            for _ in range(n):
                g, pos = read_varint(raw, pos)
                j = j + g + 1
                ln, pos = read_varint(raw, pos)
                seps[j] = bytes(raw[pos:pos + ln])
                pos += ln
            D[k] = b"".join(s + t for s, t in zip(seps, toks)) + seps[-1]
        elif typ == 1:
            n, pos = read_varint(raw, pos)
            pre = bytes(raw[pos:pos + n])
            pos += n
            n, pos = read_varint(raw, pos)
            suf = bytes(raw[pos:pos + n])
            pos += n
            D[k] = pre + d + suf
        elif typ == 2:
            a, pos = read_varint(raw, pos)
            b, pos = read_varint(raw, pos)
            n, pos = read_varint(raw, pos)
            mid = bytes(raw[pos:pos + n])
            pos += n
            D[k] = d[:a] + mid + (d[len(d) - b:] if b else b"")
        else:
            raise RuntimeError("bad patch type %d" % typ)
    if pos != len(raw):
        raise RuntimeError("residual trailing bytes")
    O = []
    it = iter(D)
    total = len(D) + len(dropped)
    for i in range(total):
        O.append(dropped[i] if i in dropped else next(it))
    return b"\n".join(O) + (b"\n" if ends_with_lf else b"")


# ---------------------------------------------------------------- container
def pack(flags, ln_dataset, files, residual):
    """files = [(name, bytes)] = every file LogNexus wrote into its output directory (all counted)."""
    nm = ln_dataset.encode()
    head = struct.pack(">BB", flags, len(nm)) + nm + struct.pack(">B", len(files))
    body = b""
    for name, data in files:
        n = name.encode()
        head += struct.pack(">B", len(n)) + n + struct.pack(">I", len(data))
        body += data
    native = head + body
    return native + struct.pack(">I", len(residual)) + residual, len(native)


def unpack(data):
    flags, ln = struct.unpack(">BB", data[:2])
    pos = 2
    ln_dataset = data[pos:pos + ln].decode()
    pos += ln
    nfiles = data[pos]
    pos += 1
    meta = []
    for _ in range(nfiles):
        n = data[pos]
        pos += 1
        name = data[pos:pos + n].decode()
        pos += n
        meta.append((name, struct.unpack(">I", data[pos:pos + 4])[0]))
        pos += 4
    files = []
    for name, n in meta:
        files.append((name, data[pos:pos + n]))
        pos += n
    rlen = struct.unpack(">I", data[pos:pos + 4])[0]
    pos += 4
    residual = data[pos:pos + rlen]
    if len(residual) != rlen or pos + rlen != len(data) or any(len(b) != n for (_, b), (_, n) in zip(files, meta)):
        raise RuntimeError("malformed archive")
    if any("/" in name or name.startswith(".") for name, _ in files):
        raise RuntimeError("unsafe member name")
    return flags, ln_dataset, files, residual


# ---------------------------------------------------------------- native decode (archive part only)
def decode_native(flags, ln_dataset, files, work, diag, timeout, tag):
    """Official LogNexus_decompress on archive bytes only. Returns (native_out_bytes, info)."""
    info = {}
    root = work / ("dec_" + tag)
    arch = root / "archive"
    arch.mkdir(parents=True)
    t0 = time.perf_counter()
    for name, data in files:
        (arch / name).write_bytes(data)
    rest = root / "restored"
    dt, stdout = run([str(LN_BIN / "LogNexus_decompress"), str(arch), str(rest), ln_dataset], diag,
                     "decompress_" + tag, timeout, cwd=root)
    chunks = sorted(rest.glob("decompressed_chunk_*.log"), key=lambda p: int(p.stem.rsplit("_", 1)[1]))
    if not chunks:
        raise RuntimeError("LogNexus_decompress produced no chunk")
    native = b"".join(p.read_bytes() for p in chunks)
    info["restored_chunks"] = len(chunks)
    info["t_decompress"] = dt
    info["t_native_decode"] = time.perf_counter() - t0
    shutil.rmtree(str(root), ignore_errors=True)
    return native, info


def native_view(native_out, flags):
    # decode alignment: restored text has '\n' after every record; flag bit0 = original had no final LF
    if flags & 1 and native_out.endswith(b"\n"):
        return native_out[:-1]
    return native_out


def token_digest(data):
    return hashlib.sha256(b"\n".join(data.split())).hexdigest(), len(data.split())


def encode_block(block_path, work, archive, diag, ends_with_lf, timeout):
    info = {}
    blk = work / "block.log"
    os.replace(str(block_path), str(blk))
    ln_dataset, tau = CFG["ln_dataset"], CFG["tau"]
    t_native0 = time.perf_counter()
    # official chunked-mode CLI, serial default LogNexus: <log> <dataset> 100000 1 1 1 <tau>
    dt, stdout = run([str(LN_BIN / "LogNexus_compress"), str(blk), ln_dataset, str(BLOCK_RECORDS), "1", "1", "1",
                      tau], diag, "compress", timeout, cwd=work)
    info["t_compress"] = dt
    m = re.search(rb"Total Chunks:\s+(\d+)", stdout)
    info["ln_chunks"] = int(m.group(1)) if m else None
    outdir = work / "output_block_lognexus"
    names = sorted(p.name for p in outdir.iterdir())
    if any(not (outdir / n).is_file() for n in names):
        raise RuntimeError("unexpected non-file in LogNexus output directory")
    files = [(n, (outdir / n).read_bytes()) for n in names]
    flags = 0 if ends_with_lf else 1
    info["t_native_encode"] = time.perf_counter() - t_native0
    info["output_files"] = names
    # verification decode (same function as the archive-only decoder) + residual
    t0 = time.perf_counter()
    orig = blk.read_bytes()
    try:
        native_out, _ = decode_native(flags, ln_dataset, files, work, diag, timeout, "verify")
        info["verify_decode_error"] = None
    except Exception as exc:
        native_out = None
        info["verify_decode_error"] = str(exc)
    if native_out is not None:
        nv = native_view(native_out, flags)
        info["native_block_exact"] = nv == orig
        info["native_token_equal"] = token_digest(nv) == token_digest(orig)
    residual, rstats = build_residual(orig, ends_with_lf, native_out)
    info["residual"] = rstats
    info["t_residual"] = time.perf_counter() - t0
    data, native_len = pack(flags, ln_dataset, files, residual)
    tmp_archive = archive.with_name(archive.name + ".part")
    with open(tmp_archive, "wb") as f:
        f.write(data)
    os.replace(str(tmp_archive), str(archive))
    info.update({"payload_bytes": sum(len(b) for _, b in files), "native_bytes": native_len,
                 "residual_bytes": len(data) - native_len, "archive_bytes": len(data)})
    return info


def decode_block(archive, work, diag, timeout):
    """Archive-only decode. Returns (native_bytes, adapted_bytes, info)."""
    t0 = time.perf_counter()
    flags, ln_dataset, files, residual = unpack(archive.read_bytes())
    ends_with_lf = not flags & 1
    info = {}
    native_out = None
    try:
        native_out, info = decode_native(flags, ln_dataset, files, work, diag, timeout, "archive")
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
    nat = native_view(native_out, flags) if native_out is not None else None
    if nat is not None:
        info["native_token_sha256"], info["native_token_count"] = token_digest(nat)
    return nat, adapted, info


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
              "ln_dataset": CFG["ln_dataset"], "tau": CFG["tau"],
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
        archive = archives / ("block_%06d.lnb" % idx)
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
            a = archives / ("block_%06d.lnb" % info["index"])
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
            archive = archives / ("block_%06d.lnb" % idx)
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
                    if tag == "native":
                        info["native_token_equal"] = info.get("native_token_sha256") == token_digest(raw)[0]
                    if tag == "adapted" and audit["kept"] < 5:
                        keep = ds / "mismatch_outputs"
                        keep.mkdir(exist_ok=True)
                        (keep / ("%06d.adapted" % idx)).write_bytes(out)
                        audit["kept"] += 1
            if info.get("native_sha_match"):
                info["native_token_equal"] = True
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
        "native_blocks_token_equal": sum(1 for d in dec_list if d.get("native_token_equal")),
        "native_decode_failures": sum(1 for d in dec_list if d.get("native_decode_error")),
        "native_differing_lines_total": sum(d.get("native_diff", {}).get("differing_lines", 0) for d in na_mism),
        "native_differing_cr_only_total": sum(d.get("native_diff", {}).get("differing_cr_only", 0) for d in na_mism),
        "failed_block_indices": [d["index"] for d in ad_mism][:200],
        "archive_bytes": ssum("archive_bytes", enc_list),
        "native_bytes": ssum("native_bytes", enc_list),
        "residual_bytes": ssum("residual_bytes", enc_list),
        "payload_bytes": ssum("payload_bytes", enc_list),
        "suffix_raw_bytes": suffix_raw,
        "suffix_archive_bytes": ssum("archive_bytes", suffix),
        "suffix_native_bytes": ssum("native_bytes", suffix),
        "residual_full_fallback_blocks": sum(1 for r in res if r.get("full_fallback")),
        "residual_dropped_records": sum(r.get("dropped_records", 0) for r in res),
        "residual_patched_records": sum(r.get("patched_records", 0) for r in res),
        "residual_patch_separator": sum(r.get("patch_separator", 0) for r in res),
        "residual_patch_wrap": sum(r.get("patch_wrap", 0) for r in res),
        "residual_patch_edit": sum(r.get("patch_edit", 0) for r in res),
        "encode_seconds_native_sum": ssum("t_native_encode", enc_list),
        "encode_seconds_residual_sum": ssum("t_residual", enc_list),
        "decode_seconds_wall": t_dec,
        "decode_audit_seconds": audit["seconds"],
        "decode_seconds_net": t_dec - audit["seconds"],
        "ln_dataset": CFG["ln_dataset"],
        "tau": CFG["tau"],
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


SUMMARY_KEYS = ("dataset", "ln_dataset", "tau", "status", "native_status", "raw_bytes", "archive_bytes",
                "native_bytes", "residual_bytes", "compression_ratio", "native_compression_ratio", "suffix_ratio",
                "native_suffix_ratio", "blocks_sha_pass", "blocks_sha_fail", "full_sha_match",
                "native_blocks_sha_pass", "native_blocks_token_equal", "native_differing_lines_total",
                "residual_full_fallback_blocks", "encode_seconds_wall", "decode_seconds_net", "error")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", action="append", default=[], metavar="DATASET=PATH")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--profile", choices=("paper", "fixed0.02"), required=True,
                   help="paper: configs/paper_thresholds.csv per LogHub dataset (0.02 for others); fixed0.02: 0.02")
    p.add_argument("--workers", type=int, choices=range(1, 5), default=4)
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
        taus = load_thresholds()
        manifest = {"schema": SCHEMA, "upstream": "https://doi.org/" + ZENODO_RECORD,
                    "source_tarball_sha256": SOURCE_TARBALL_SHA256,
                    "block_records": BLOCK_RECORDS, "profile": a.profile, "paper_thresholds": taus,
                    "cli": "LogNexus_compress <block> <dataset> 100000 1 1 1 <tau>; "
                           "LogNexus_decompress <archive_dir> <restored_dir> <dataset>",
                    "residual_lzma": "FORMAT_ALONE preset 9e", "workers": a.workers,
                    "driver_sha256": file_sha(__file__),
                    "binaries": {n: file_sha(LN_BIN / n) for n in ("LogNexus_compress", "LogNexus_decompress")},
                    "sources": {n: file_sha(LN_DIR / n) for n in ("LogNexus_compress.cpp", "LogNexus_decompress.cpp",
                                                                  "Makefile", "configs/paper_thresholds.csv")},
                    "cxx": subprocess.run(["g++", "--version"], stdout=subprocess.PIPE).stdout.decode()
                    .splitlines()[0],
                    "python": sys.version, "host": platform.node(), "created_at": utc(), "inputs": a.input}
        atomic_json(out / ("manifest_%s.json" % datetime.datetime.now().strftime("%Y%m%dT%H%M%S")), manifest)
        results = []
        for spec in a.input:
            name, path = spec.split("=", 1)
            CFG["ln_dataset"] = name
            CFG["tau"] = taus[name] if (a.profile == "paper" and name in LN_NAMES) else "0.02"
            print(json.dumps({"phase": "dataset", "dataset": name, "tau": CFG["tau"], "at": utc()}), flush=True)
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
