#!/usr/bin/env python3
"""Independent, exact-LF-block, archive-only CLI compression benchmark (Python 3.9).

R76 copy of r68_external_20260923/codec_baseline.py (sha256 0b0c15f6...c26d).
No learned state or CLI processes are shared between blocks, with ONE declared
exception: codec zstd19dict trains a single zstd dictionary on block 0 of each
file; that dictionary file is stored in the archive set and its bytes are
counted once per file in archive_bytes (also reported separately).
Additional codecs: xz9e, zstd22long, zstd19dict, delog_generic (official DeLog
sources with an empty dataset-name regex_map). See README.md in this directory.
Source and archive hashes are audited outside the encode/decode timing windows.
"""
import argparse
import concurrent.futures
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import traceback

BLOCK_RECORDS = 100000  # Protocol constant. Deliberately not a CLI option.
READ_BYTES = 1024 * 1024
SCHEMA = "semzip.external-cli.r76.v1"
DICT_SAMPLE_RECORDS = 1000  # zstd19dict: block 0 is split into 1,000-LF-record samples.
CODECS = {
    "gzip6": {"exe": "gzip", "encode": ["-6", "-n", "-c"], "decode": ["-d", "-c"], "suffix": ".gz"},
    "xz6": {"exe": "xz", "encode": ["-6", "-T1", "-c"], "decode": ["-d", "-T1", "-c"], "suffix": ".xz"},
    "zstd3": {"exe": "zstd", "encode": ["-3", "-T1", "-q", "-c"], "decode": ["-d", "-T1", "-q", "-c"], "suffix": ".zst"},
    "zstd19": {"exe": "zstd", "encode": ["-19", "-T1", "-q", "-c"], "decode": ["-d", "-T1", "-q", "-c"], "suffix": ".zst"},
    # R76 high-effort generic settings (fixed; never chosen per dataset).
    "xz9e": {"exe": "xz", "encode": ["-9e", "-T1", "-c"], "decode": ["-d", "-T1", "-c"], "suffix": ".xz"},
    "zstd22long": {"exe": "zstd", "encode": ["--ultra", "-22", "--long=27", "-T1", "-q", "-c"],
                   "decode": ["-d", "--long=27", "-T1", "-q", "-c"], "suffix": ".zst"},
    "zstd19dict": {"exe": "zstd", "encode": ["-19", "-T1", "-q", "-c"], "decode": ["-d", "-T1", "-q", "-c"],
                   "suffix": ".zst", "mode": "block0-trained-dictionary-counted-once-per-file",
                   "train": ["--train", "-T1"], "sample_records": DICT_SAMPLE_RECORDS,
                   "dictionary_file": "dictionary.zdict", "dictionary_size": "zstd-default(--maxdict unset)"},
    "delog": {"suffix": ".tar.xz", "mode": "official-cli-independent-block-adapter"},
    "delog_generic": {"suffix": ".tar.xz", "mode": "official-cli-independent-block-adapter",
                      "variant": "official DeLog sources with empty dataset-name regex_map (R76 DeLog-generic)"},
    "loglite": {"suffix": ".lite.xz", "mode": "official-cli-independent-block-adapter"},
}
DELOG_CODECS = ("delog", "delog_generic")


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    tmp = path.with_name(path.name + ".tmp.%d" % os.getpid())
    with open(tmp, "x", encoding="utf-8") as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.write("\n")
    os.replace(str(tmp), str(path))


def read_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


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


def scan(source, segment, block_end):
    """Stream binary input. Only LF terminates a record; never normalize bytes.

    segment receives at most READ_BYTES bytes. block_end is called at exactly
    100000 LF delimiters, or at EOF for a nonempty original tail.
    """
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
                    # C-level count + binary search avoids retaining a line list.
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


def settle_file(path, window=2.0, max_rounds=30):
    """R76 addition, outside all timers.  The <MOUNT> network file system applies the
    server-side mtime of a freshly written file lazily after close(); in the first R76
    smoke this changed the restored file's mtime *during* its SHA inventory and the
    identity guard reported a false "source changed" failure although the restored
    bytes were exact.  Flush the owned restored file and wait until its stat identity
    is unchanged over `window` seconds before auditing it.  Returns seconds spent."""
    start = time.perf_counter()
    fd = os.open(str(path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    previous = file_identity(path)
    for _ in range(max_rounds):
        time.sleep(window)
        current = file_identity(path)
        if current == previous:
            return time.perf_counter() - start
        previous = current
    raise RuntimeError("restored file identity did not settle: %s" % path)


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


def run_process(command, cwd, stdout_path, stderr_path, timeout):
    # All command arguments are passed without a shell. Preserve diagnostics,
    # including official DeLog's occasionally successful exit on stderr errors.
    with open(stdout_path, "xb") as out, open(stderr_path, "xb") as err:
        proc = subprocess.run(command, cwd=str(cwd), stdout=out, stderr=err,
                              timeout=timeout, check=False)
    if proc.returncode != 0:
        raise RuntimeError("CLI exit %d; see %s" % (proc.returncode, stderr_path))
    with open(stderr_path, "rb") as f:
        error_seen = any(b"error processing chunk" in line.lower() for line in f)
    if error_seen:
        raise RuntimeError("CLI reported Error processing chunk despite exit 0: %s" % stderr_path)


class Codec:
    def __init__(self, name, delog_dir=None, loglite_executable=None, delog_generic_dir=None, executables=None):
        self.name = name
        self.spec = dict(CODECS[name])
        self.dictionary_mode = "dictionary_file" in self.spec
        if name == "delog_generic":
            delog_dir = delog_generic_dir
            if delog_dir is None:
                raise RuntimeError("delog_generic requires --delog-generic-dir")
        if name == "loglite":
            if loglite_executable is None:
                raise RuntimeError("loglite requires --loglite-executable")
            import loglite_adapter
            self.adapter = loglite_adapter
            self.encoder = self.decoder = Path(loglite_executable).resolve()
            if not self.encoder.is_file() or not os.access(self.encoder, os.X_OK):
                raise RuntimeError("missing LogLite executable: %s" % self.encoder)
            xz = shutil.which("xz")
            if xz is None:
                raise RuntimeError("LogLite wrapper requires xz")
            self.xz = str(Path(xz).resolve())
            self.spec.update({"executable": str(self.encoder), "executable_sha256": file_sha(self.encoder),
                              "adapter_sha256": file_sha(loglite_adapter.__file__),
                              "xz_executable": self.xz, "xz_sha256": file_sha(self.xz),
                              "wrapper": "one-byte-terminal-LF-flag-plus-XZ6-T1-CRC64-official-lite"})
        elif name in DELOG_CODECS:
            if delog_dir is None:
                raise RuntimeError("delog requires --delog-dir")
            root = Path(delog_dir).resolve()
            self.encoder = root / "Delog_compress"
            self.decoder = root / "decompress"
            for exe in (self.encoder, self.decoder):
                if not exe.is_file() or not os.access(exe, os.X_OK):
                    raise RuntimeError("missing executable: %s" % exe)
            self.spec.update({"encoder": str(self.encoder), "decoder": str(self.decoder),
                              "encoder_sha256": file_sha(self.encoder), "decoder_sha256": file_sha(self.decoder),
                              "encode_arguments": ["DATASET", "text", "100000", "1", "0", "lzma", "normal"],
                              "decode_arguments": ["ARCHIVE_DIRECTORY", "OUTPUT_FILE", "1"]})
            if (root / "compressor.cpp").is_file():
                self.spec["compressor_source_sha256"] = file_sha(root / "compressor.cpp")
        else:
            override = (executables or {}).get(self.spec["exe"])
            found = override if override is not None else shutil.which(self.spec["exe"])
            if found is None:
                raise RuntimeError("missing CLI executable: %s" % self.spec["exe"])
            if not Path(found).is_file() or not os.access(found, os.X_OK):
                raise RuntimeError("missing executable: %s" % found)
            self.encoder = self.decoder = Path(found).resolve()
            version = subprocess.run([str(self.encoder), "--version"], stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, check=False).stdout.decode("utf-8", "replace")
            self.spec.update({"executable": str(self.encoder), "executable_sha256": file_sha(self.encoder),
                              "version": version[:4096]})

    def train_dictionary(self, block0, dictionary, work, diagnostic, timeout):
        """zstd19dict only: split block 0 into exact 1,000-LF-record byte samples
        (the last sample keeps any shorter/unterminated tail) and run one
        `zstd --train -T1 SAMPLES -o dictionary` with zstd's default dictionary size."""
        if not self.dictionary_mode:
            raise RuntimeError("train_dictionary called for a non-dictionary codec")
        samples_dir = work / "dict-samples"
        samples_dir.mkdir()
        samples = []
        out = None
        count = 0
        with open(block0, "rb") as f:
            for line in f:  # binary iteration splits after b"\n" only; bytes untouched
                if out is None:
                    path = samples_dir / ("sample_%05d" % len(samples))
                    out = open(path, "xb")
                    samples.append(path)
                out.write(line)
                count += 1
                if count == self.spec["sample_records"]:
                    out.close()
                    out, count = None, 0
        if out is not None:
            out.close()
        if not samples:
            raise RuntimeError("block 0 is empty; cannot train a dictionary")
        diagnostic.mkdir(parents=True, exist_ok=True)
        run_process([str(self.encoder)] + self.spec["train"] + [str(p) for p in samples] + ["-o", str(dictionary)],
                    work, diagnostic / "train.stdout", diagnostic / "train.stderr", timeout)
        if not dictionary.is_file() or dictionary.stat().st_size == 0:
            raise RuntimeError("zstd --train produced no dictionary")
        info = {"samples": len(samples), "sample_bytes": sum(p.stat().st_size for p in samples)}
        shutil.rmtree(str(samples_dir))
        return info

    def encode(self, dataset, source, archive, work, diagnostic, timeout, dictionary=None):
        if self.name == "loglite":
            self.adapter.encode_block(source, archive, str(self.encoder), xz=self.xz, timeout=timeout)
            return
        if self.name not in DELOG_CODECS:
            extra = []
            if self.dictionary_mode:
                if dictionary is None or not Path(dictionary).is_file():
                    raise RuntimeError("dictionary codec without a trained dictionary")
                extra = ["-D", str(dictionary)]
            run_process([str(self.encoder)] + self.spec["encode"] + extra + ["--", str(source)],
                        work, archive, diagnostic / "encode.stderr", timeout)
            return
        staged = work / "Logs" / dataset / (dataset + ".log")
        staged.parent.mkdir(parents=True)
        os.replace(str(source), str(staged))
        command = [str(self.encoder), dataset, "text", "100000", "1", "0", "lzma", "normal"]
        run_process(command, work, diagnostic / "encode.stdout", diagnostic / "encode.stderr", timeout)
        outputs = sorted((work / "output" / dataset).glob("*.tar.xz"))
        if len(outputs) != 1 or outputs[0].name != "chunk_0.tar.xz":
            raise RuntimeError("DeLog did not produce exactly output/DATASET/chunk_0.tar.xz")
        # Preserve a complete byte-for-byte official chunk archive. No repack.
        os.rename(str(outputs[0]), str(archive))

    def decode(self, dataset, archive, destination, work, diagnostic, timeout, dictionary=None):
        if self.name == "loglite":
            self.adapter.decode_block(archive, destination, str(self.decoder), xz=self.xz, timeout=timeout)
            return
        if self.name not in DELOG_CODECS:
            extra = []
            if self.dictionary_mode:
                # The dictionary is read from the archive set itself.
                if dictionary is None or not Path(dictionary).is_file():
                    raise RuntimeError("dictionary codec archive set lacks its dictionary file")
                extra = ["-D", str(dictionary)]
            run_process([str(self.decoder)] + self.spec["decode"] + extra + ["--", str(archive)],
                        work, destination, diagnostic / "decode.stderr", timeout)
            return
        archives = work / "archives"
        archives.mkdir()
        # A hardlink does not alter archive bytes and is always on the same
        # filesystem as this attempt's workspace and retained archive.
        os.link(str(archive), str(archives / "chunk_0.tar.xz"))
        command = [str(self.decoder), str(archives), str(destination), "1"]
        run_process(command, work, diagnostic / "decode.stdout", diagnostic / "decode.stderr", timeout)
        if not destination.is_file():
            raise RuntimeError("DeLog decoder did not produce the requested output file")


def encode_all(source, inv, codec, dataset, attempt, workers, timeout):
    archives = attempt / "archives"
    diagnostics = attempt / "diagnostics"
    archives.mkdir()
    diagnostics.mkdir()
    completed = []
    extra = {}
    dictionary = archives / codec.spec["dictionary_file"] if codec.dictionary_mode else None
    with tempfile.TemporaryDirectory(prefix="encode-owned-", dir=str(attempt)) as td:
        temp = Path(td)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            pending = []
            current = [None, None, None]

            def collect_one():
                future = pending.pop(0)
                completed.append(future.result())

            def encode_block(meta, path, work):
                diag = diagnostics / ("%06d" % meta["index"])
                diag.mkdir()
                archive = archives / ("block_%06d%s" % (meta["index"], codec.spec["suffix"]))
                start = time.perf_counter()
                codec.encode(dataset, path, archive, work, diag, timeout, dictionary=dictionary)
                elapsed = time.perf_counter() - start
                size = archive.stat().st_size
                shutil.rmtree(str(work))  # This attempt's own bounded raw spool.
                return {"index": meta["index"], "archive": str(archive.relative_to(attempt)),
                        "archive_bytes": size, "block_encode_seconds": elapsed}

            def segment(piece):
                if current[0] is None:
                    # Never accumulate a queue of uncompressed blocks.
                    while len(pending) >= workers:
                        collect_one()
                    work = Path(tempfile.mkdtemp(prefix="block-", dir=str(temp)))
                    path = work / "input.log"
                    current[:] = [open(path, "xb"), path, work]
                current[0].write(piece)

            def end(meta):
                current[0].close()
                expected = inv["blocks"][meta["index"]]
                if any(meta[key] != expected[key] for key in meta):
                    raise RuntimeError("source block structure changed during timed encode")
                if dictionary is not None and meta["index"] == 0:
                    # Block 0 is always the first block closed and nothing has been
                    # submitted yet: train once, synchronously, inside the encode timer.
                    tstart = time.perf_counter()
                    info = codec.train_dictionary(current[1], dictionary, current[2],
                                                  diagnostics / "dictionary", timeout)
                    info["dictionary_train_seconds"] = time.perf_counter() - tstart
                    extra.update(info)
                pending.append(pool.submit(encode_block, meta, current[1], current[2]))
                current[:] = [None, None, None]

            start = time.perf_counter()
            try:
                scan(source, segment, end)
                while pending:
                    collect_one()
            finally:
                if current[0] is not None:
                    current[0].close()
            elapsed = time.perf_counter() - start
    if len(completed) != len(inv["blocks"]):
        raise RuntimeError("encoded block count mismatch")
    if dictionary is not None:
        extra.update({"dictionary_archive": str(dictionary.relative_to(attempt)),
                      "dictionary_bytes": dictionary.stat().st_size})
    return sorted(completed, key=lambda b: b["index"]), elapsed, extra


def decode_all(inv, codec, dataset, attempt, blocks, workers, timeout):
    # The output file is materialized once, then independently audited after
    # timing. Bounded per-block spool exists in addition to this full output.
    full_output = attempt / "roundtrip.owned.log"
    # zstd19dict: the decoder reads the dictionary from the archive set only.
    dictionary = attempt / "archives" / codec.spec["dictionary_file"] if codec.dictionary_mode else None
    with tempfile.TemporaryDirectory(prefix="decode-owned-", dir=str(attempt)) as td:
        temp = Path(td)
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            pending = []

            def decode_block(block):
                work = Path(tempfile.mkdtemp(prefix="block-", dir=str(temp)))
                output = work / "raw.log"
                diag = attempt / "diagnostics" / ("%06d" % block["index"])
                codec.decode(dataset, attempt / block["archive"], output, work, diag, timeout,
                             dictionary=dictionary)
                return output, work

            start = time.perf_counter()
            with open(full_output, "xb") as out:
                for block in blocks:
                    pending.append(pool.submit(decode_block, block))
                    if len(pending) >= workers:
                        path, work = pending.pop(0).result()
                        with open(path, "rb") as src:
                            shutil.copyfileobj(src, out, length=READ_BYTES)
                        shutil.rmtree(str(work))
                while pending:
                    path, work = pending.pop(0).result()
                    with open(path, "rb") as src:
                        shutil.copyfileobj(src, out, length=READ_BYTES)
                    shutil.rmtree(str(work))
            elapsed = time.perf_counter() - start
    return full_output, elapsed


def benchmark(inv, codec, dataset, trial, attempt, args):
    result = {"schema": SCHEMA, "status": "RUNNING", "started_at": utc(), "dataset": dataset,
              "codec": codec.name, "trial": trial, "workers": args.workers,
              "block_records": BLOCK_RECORDS, "source": inv["source"], "raw_sha256": inv["raw_sha256"],
              "raw_bytes": inv["raw_bytes"], "records": inv["records"], "codec_spec": codec.spec,
              "attempt": str(attempt), "timing_scope": "full-file-cli-split-and-materialized-output-v1"}
    atomic_json(attempt / "status.json", result)
    try:
        if file_identity(inv["source"]["path"]) != inv["source"]:
            raise RuntimeError("source identity changed before timed encode")
        blocks, encode_seconds, extra = encode_all(inv["source"]["path"], inv, codec, dataset, attempt,
                                                   args.workers, args.timeout)
        block_bytes = sum(b["archive_bytes"] for b in blocks)
        result.update({"blocks": blocks, "encode_seconds": encode_seconds,
                       "block_archive_bytes": block_bytes,
                       "archive_bytes": block_bytes + extra.get("dictionary_bytes", 0)})
        if codec.dictionary_mode:
            extra["dictionary_sha256"] = file_sha(attempt / extra["dictionary_archive"])
            result.update(extra)
            result["archive_components"] = "block archives + one dictionary file (counted once per file)"
        atomic_json(attempt / "status.json", dict(result, phase="decode"))
        restored, decode_seconds = decode_all(inv, codec, dataset, attempt, blocks, args.workers, args.timeout)
        result["decode_seconds"] = decode_seconds
        atomic_json(attempt / "status.json", dict(result, phase="audit"))
        result["restored_settle_seconds"] = settle_file(restored)
        audit_start = time.perf_counter()
        recovered = inventory(restored)
        if recovered["raw_sha256"] != inv["raw_sha256"] or recovered["blocks"] != inv["blocks"]:
            raise RuntimeError("byte-exact archive-only round trip failed (raw SHA/block identities differ)")
        for block, original in zip(blocks, inv["blocks"]):
            block.update(original)
            block["archive_sha256"] = file_sha(attempt / block["archive"])
            block["decoded_sha256"] = recovered["blocks"][block["index"]]["raw_sha256"]
        if file_identity(inv["source"]["path"]) != inv["source"]:
            raise RuntimeError("source changed during benchmark")
        result.update({"status": "PASS", "roundtrip": "byte-exact", "archive_only_decode": True,
                       "decoded_sha256": recovered["raw_sha256"], "audit_seconds": time.perf_counter() - audit_start,
                       "compression_ratio": inv["raw_bytes"] / result["archive_bytes"] if result["archive_bytes"] else None,
                       "encode_MB_per_s": inv["raw_bytes"] / encode_seconds / 1e6,
                       "decode_MB_per_s": inv["raw_bytes"] / decode_seconds / 1e6})
        if args.keep_decoded:
            result["decoded_file"] = str(restored)
        else:
            restored.unlink()  # Only the exact output created by this attempt.
    except Exception as exc:
        result.update({"status": "FAIL", "error": str(exc), "traceback": traceback.format_exc()})
        # Failed archives, diagnostics and any completed reconstruction remain.
    result["finished_at"] = utc()
    atomic_json(attempt / "result.json", result)
    atomic_json(attempt / "status.json", result)
    return result


def verify_completed(result, inv, codec, workers):
    if result["status"] != "PASS":
        raise RuntimeError("not a completed successful result")
    if (result.get("schema") != SCHEMA or result["codec_spec"] != codec.spec or result["workers"] != workers
            or result["raw_sha256"] != inv["raw_sha256"] or result["source"] != inv["source"]
            or result["block_records"] != BLOCK_RECORDS or result["decoded_sha256"] != inv["raw_sha256"]):
        raise RuntimeError("completed result protocol or input differs")
    if len(result["blocks"]) != len(inv["blocks"]):
        raise RuntimeError("completed result block count differs")
    root = Path(result["attempt"])
    total = 0
    for block, original in zip(result["blocks"], inv["blocks"]):
        if any(block.get(key) != value for key, value in original.items()):
            raise RuntimeError("completed result source-block record differs")
        path = root / block["archive"]
        if path.stat().st_size != block["archive_bytes"] or file_sha(path) != block["archive_sha256"]:
            raise RuntimeError("completed archive changed: %s" % path)
        if block["decoded_sha256"] != original["raw_sha256"]:
            raise RuntimeError("completed decode certificate differs")
        total += path.stat().st_size
    if codec.dictionary_mode:
        path = root / result["dictionary_archive"]
        if path.stat().st_size != result["dictionary_bytes"] or file_sha(path) != result["dictionary_sha256"]:
            raise RuntimeError("completed dictionary changed: %s" % path)
        total += path.stat().st_size
    if total != result["archive_bytes"]:
        raise RuntimeError("archive total differs")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", action="append", default=[], metavar="DATASET=PATH")
    p.add_argument("--input-dir", type=Path)
    p.add_argument("--datasets", nargs="+")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--codecs", nargs="+", choices=sorted(CODECS), default=["gzip6", "xz6", "zstd3"])
    p.add_argument("--workers", type=int, choices=range(1, 5), default=4)
    p.add_argument("--trials", type=int, default=1)
    p.add_argument("--order", choices=["input", "rotate"], default="rotate")
    p.add_argument("--delog-dir", type=Path)
    p.add_argument("--delog-generic-dir", type=Path,
                   help="directory with Delog_compress/decompress built from the empty-regex_map sources")
    p.add_argument("--exe", action="append", default=[], metavar="NAME=PATH",
                   help="explicit executable for a CLI name (e.g. zstd=/path/zstd) instead of PATH lookup")
    p.add_argument("--loglite-executable", type=Path)
    p.add_argument("--timeout", type=float, default=3600, help="per block CLI timeout seconds")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--retry-failed", action="store_true", help="append attempts; requires --resume")
    p.add_argument("--keep-decoded", action="store_true")
    a = p.parse_args()
    if a.trials < 1 or a.timeout <= 0 or len(set(a.codecs)) != len(a.codecs):
        p.error("positive trials/timeout and unique codecs are required")
    if a.retry_failed and not a.resume:
        p.error("--retry-failed requires --resume")
    if bool(a.input_dir) != bool(a.datasets):
        p.error("--input-dir and --datasets must be supplied together")
    inputs = list(a.input)
    if a.input_dir:
        inputs += [name + "=" + str(a.input_dir / (name + ".log")) for name in a.datasets]
    a.inputs = []
    for value in inputs:
        if "=" not in value:
            p.error("--input requires DATASET=PATH")
        name, path = value.split("=", 1)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) or name in (".", ".."):
            p.error("invalid dataset name")
        a.inputs.append((name, Path(path).resolve()))
    if not a.inputs or len(set(n for n, _ in a.inputs)) != len(a.inputs):
        p.error("at least one input and unique dataset names are required")
    a.executables = {}
    for value in a.exe:
        if "=" not in value:
            p.error("--exe requires NAME=PATH")
        name, path = value.split("=", 1)
        a.executables[name] = str(Path(path).resolve())
    return a


def main():
    args = parse_args()
    output = args.output.resolve()
    existed = output.exists()
    if existed and not args.resume:
        raise RuntimeError("output already exists; use --resume or a new output directory")
    output.mkdir(parents=True, exist_ok=True)
    # O_EXCL lock intentionally survives abnormal termination. Never guess that
    # another process is stale and remove its lock; inspect/remove it explicitly.
    lock = output / "RUNNING.lock"
    with open(lock, "x", encoding="utf-8") as f:
        json.dump({"pid": os.getpid(), "hostname": platform.node(), "started_at": utc()}, f)
    try:
        codecs = {name: Codec(name, args.delog_dir, args.loglite_executable,
                              delog_generic_dir=args.delog_generic_dir, executables=args.executables)
                  for name in args.codecs}
        protocol = {"schema": SCHEMA, "block_records": BLOCK_RECORDS, "workers": args.workers,
                    "trials": args.trials, "order": args.order, "timeout_per_block_seconds": args.timeout,
                    "codecs": {name: obj.spec for name, obj in codecs.items()},
                    "inputs": [{"dataset": name, "source": file_identity(path)} for name, path in args.inputs],
                    "script_sha256": file_sha(__file__), "decode_output": "materialized-file",
                    "keep_decoded": args.keep_decoded}
        manifest_path = output / "manifest.json"
        if manifest_path.exists():
            if read_json(manifest_path)["protocol"] != protocol:
                raise RuntimeError("existing campaign protocol differs; use a new output directory")
        else:
            if existed and any(p.name != lock.name for p in output.iterdir()):
                raise RuntimeError("refusing to claim a nonempty directory without a manifest")
            atomic_json(manifest_path, {"created_at": utc(), "protocol": protocol,
                                       "host": platform.node(), "platform": platform.platform(),
                                       "python": sys.version, "logical_cpus": os.cpu_count(),
                                       "timing_notes": "Full streaming split and CLI process wall, all archive writes; decode all CLI outputs and ordered full-file assembly. SHA/inventory audits excluded. OS buffered I/O; no fsync or cache drop. Historical R59 internal timers are not directly comparable."})
        inventories = {}
        for name, path in args.inputs:
            print(json.dumps({"phase": "inventory", "dataset": name}), flush=True)
            inv = inventory(path)
            inventories[name] = inv
            dest = output / ("input_%s.json" % name)
            if dest.exists():
                if read_json(dest) != inv:
                    raise RuntimeError("input inventory changed: %s" % name)
            else:
                atomic_json(dest, inv)
        schedule = []
        for trial in range(1, args.trials + 1):
            for di, (name, _) in enumerate(args.inputs):
                names = list(codecs)
                if args.order == "rotate":
                    offset = (trial - 1 + di) % len(names)
                    names = names[offset:] + names[:offset]
                schedule.extend((name, codec, trial) for codec in names)
        records = []
        for name, codec_name, trial in schedule:
            run = output / name / codec_name / ("trial_%03d" % trial)
            run.mkdir(parents=True, exist_ok=True)
            attempts = sorted(run.glob("attempt_*"))
            completed = None
            prior_failure = None
            for path in attempts:
                result_path = path / "result.json"
                if result_path.exists():
                    old = read_json(result_path)
                    if old["status"] == "PASS":
                        verify_completed(old, inventories[name], codecs[codec_name], args.workers)
                        completed = old
                    else:
                        prior_failure = old
                else:
                    prior_failure = {"status": "INTERRUPTED", "attempt": str(path),
                                     "dataset": name, "codec": codec_name, "trial": trial}
            if completed:
                result = dict(completed, reused_after_verification=True)
            elif prior_failure and not args.retry_failed:
                result = prior_failure
            else:
                attempt = run / ("attempt_%03d" % (len(attempts) + 1))
                attempt.mkdir()
                atomic_json(output / "status.json", {"status": "RUNNING", "dataset": name,
                            "codec": codec_name, "trial": trial, "attempt": str(attempt), "updated_at": utc()})
                print(json.dumps({"phase": "benchmark", "dataset": name, "codec": codec_name, "trial": trial}), flush=True)
                result = benchmark(inventories[name], codecs[codec_name], name, trial, attempt, args)
            records.append(result)
            atomic_json(output / "results.json", {"schema": SCHEMA, "updated_at": utc(), "runs": records,
                        "all_attempts_retained": True, "complete": len(records) == len(schedule)})
            print(json.dumps({"phase": "result", "dataset": name, "codec": codec_name, "trial": trial,
                              "status": result["status"], "compression_ratio": result.get("compression_ratio"),
                              "encode_seconds": result.get("encode_seconds"), "decode_seconds": result.get("decode_seconds"),
                              "error": result.get("error")}), flush=True)
        status = "PASS" if all(r["status"] == "PASS" for r in records) else "COMPLETE_WITH_FAILURES"
        atomic_json(output / "status.json", {"status": status, "finished_at": utc(), "runs": len(records),
                    "passed": sum(r["status"] == "PASS" for r in records)})
        return 0 if status == "PASS" else 1
    finally:
        lock.unlink()


if __name__ == "__main__":
    sys.exit(main())
