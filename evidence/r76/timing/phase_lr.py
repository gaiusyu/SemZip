#!/usr/bin/env python3
"""LogReducer+R timing phases. usage: phase_lr.py encode|decode REQUEST.json

Runner: ../baselines/logreducer/lr_run2.py (imported read-only, not modified; its per-block functions are the
method). Block/timing scope copied from R68 codec_baseline.encode_all/decode_all: the streaming LF splitter
(lr_run2.scan, a verbatim copy of R68 scan) spools each exact 100,000-record block, at most 4 blocks pending,
4 block workers; each block runs its official subprocesses one after another; archives go to attempt/archives.
The decoder reads only those archives (plus their order) and writes one complete restored file.

  encode, mode adapted : lr_run2.encode_block unchanged = official sampler/training/Tot segmentation/THULR/7za/
                         model + verification decode (official LogRestore) + residual; archive = native + residual
  encode, mode native  : the same function with its verification decode and residual construction replaced by
                         no-ops in THIS process only (archive = native part + empty residual). Timing only: the
                         remaining extra work is one read of the block spool.
  decode               : lr_run2.decode_block = official LogRestore on archive bytes + residual application.
Per-block runner failures are recorded (not raised) so that the phase completes; the driver's SHA audit then
fails the trial (status SHA_FAIL), which is reported as a result.
"""
import sys
sys.dont_write_bytecode = True
import concurrent.futures
import json
import os
import shutil
import tempfile
import time
from pathlib import Path

H = Path(__file__).resolve().parent
sys.path.insert(0, str(H.parent / 'baselines' / 'logreducer'))
import lr_run2 as lr  # noqa: E402

WORKERS = 4
TIMEOUT = 3600
READ_BYTES = 1024 * 1024


def load(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.pending.%d' % os.getpid())
    with temporary.open('x') as stream:
        json.dump(value, stream, indent=1, sort_keys=True)
        stream.write('\n')
    os.replace(str(temporary), str(path))


def err(exc):
    return '%s: %s' % (type(exc).__name__, exc)


class NativeOnly(Exception):
    pass


def _skip_verify(*args, **kwargs):
    raise NativeOnly('native-only timing run: verification decode not executed')


def _skip_residual(orig, ends_with_lf, native_out):
    return b'', {'skipped': 'native-only timing run'}


def encode_all(source, inv, attempt, native_only):
    if native_only:
        lr.decode_native = _skip_verify
        lr.build_residual = _skip_residual
    archives, diagnostics = attempt / 'archives', attempt / 'diagnostics'
    archives.mkdir()
    diagnostics.mkdir()
    completed = []
    with tempfile.TemporaryDirectory(prefix='encode-owned-', dir=str(attempt)) as td:
        temp = Path(td)
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            pending = []
            current = [None, None, None]

            def collect_one():
                completed.append(pending.pop(0).result())

            def encode_block(meta, path, work):
                diag = diagnostics / ('%06d' % meta['index'])
                diag.mkdir()
                archive = archives / ('block_%06d.lrb' % meta['index'])
                start = time.perf_counter()
                try:
                    info = lr.encode_block(path, work, archive, diag, meta['ends_with_lf'], TIMEOUT)
                    info['status'] = 'OK'
                except Exception as exc:
                    info = {'status': 'ENCODE_FAIL', 'error': err(exc)}
                info['block_encode_seconds'] = time.perf_counter() - start
                info['index'] = meta['index']
                info['archive'] = str(archive.relative_to(attempt)) if archive.is_file() else None
                shutil.rmtree(str(work), ignore_errors=True)  # this attempt's own bounded block spool
                return info

            def segment(piece):
                if current[0] is None:
                    while len(pending) >= WORKERS:  # never accumulate a queue of uncompressed blocks
                        collect_one()
                    work = Path(tempfile.mkdtemp(prefix='block-', dir=str(temp)))
                    path = work / 'input.log'
                    current[:] = [open(path, 'xb'), path, work]
                current[0].write(piece)

            def end(meta):
                current[0].close()
                expected = inv['blocks'][meta['index']]
                if any(meta[key] != expected[key] for key in meta):
                    raise RuntimeError('source block structure changed during timed encode')
                pending.append(pool.submit(encode_block, meta, current[1], current[2]))
                current[:] = [None, None, None]

            start = time.perf_counter()
            try:
                lr.scan(source, segment, end)
                while pending:
                    collect_one()
            finally:
                if current[0] is not None:
                    current[0].close()
            elapsed = time.perf_counter() - start
    if len(completed) != len(inv['blocks']):
        raise RuntimeError('encoded block count mismatch')
    return sorted(completed, key=lambda b: b['index']), elapsed


def decode_all(attempt, blocks):
    full_output = attempt / 'roundtrip.owned.log'
    infos = []
    with tempfile.TemporaryDirectory(prefix='decode-owned-', dir=str(attempt)) as td:
        temp = Path(td)
        with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
            pending = []

            def decode_block(block):
                work = Path(tempfile.mkdtemp(prefix='block-', dir=str(temp)))
                output = work / 'raw.log'
                info = {'index': block['index']}
                if not block['archive']:
                    info['status'] = 'NO_ARCHIVE'
                    return None, work, info
                diag = attempt / 'diagnostics' / ('%06d' % block['index'])
                diag.mkdir(parents=True, exist_ok=True)
                try:
                    _native, adapted, dinfo = lr.decode_block(attempt / block['archive'], work, diag, TIMEOUT)
                    info.update(dinfo)
                    if adapted is None:
                        info['status'] = 'ADAPTED_DECODE_FAIL'
                        return None, work, info
                    with open(output, 'xb') as f:
                        f.write(adapted)
                    info['status'] = 'OK'
                    return output, work, info
                except Exception as exc:
                    info.update(status='DECODE_FAIL', error=err(exc))
                    return None, work, info

            def drain(out):
                path, work, info = pending.pop(0).result()
                if path is not None:
                    with open(path, 'rb') as src:
                        shutil.copyfileobj(src, out, length=READ_BYTES)
                shutil.rmtree(str(work))
                infos.append(info)

            start = time.perf_counter()
            with open(full_output, 'xb') as out:
                for block in blocks:
                    pending.append(pool.submit(decode_block, block))
                    if len(pending) >= WORKERS:
                        drain(out)
                while pending:
                    drain(out)
            elapsed = time.perf_counter() - start
    return full_output, elapsed, infos


def main():
    phase, request = sys.argv[1], load(sys.argv[2])
    attempt = Path(request['attempt'])
    if phase == 'encode':
        inv = request['inventory']
        blocks, seconds = encode_all(inv['source']['path'], inv, attempt, request['mode'] == 'native')
        save(attempt / 'encode_phase.json', {'mode': request['mode'], 'blocks': blocks, 'inner_encode_seconds': seconds,
                                             'failed_blocks': [b['index'] for b in blocks if b['status'] != 'OK']})
    elif phase == 'decode':
        encoded = load(attempt / 'encode_phase.json')
        order = [{'index': b['index'], 'archive': b['archive']} for b in encoded['blocks']]
        restored, seconds, infos = decode_all(attempt, order)
        save(attempt / 'decode_phase.json', {'restored': str(restored), 'inner_decode_seconds': seconds, 'blocks': infos,
                                             'failed_blocks': [b['index'] for b in infos if b['status'] != 'OK']})
    else:
        raise SystemExit(__doc__)


if __name__ == '__main__':
    main()
