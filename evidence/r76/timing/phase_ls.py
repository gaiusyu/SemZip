#!/usr/bin/env python3
"""LogShrink+R timing phases. usage: <logshrink venv>/python3 phase_ls.py encode|decode REQUEST.json

Runner: ../baselines/logshrink/adapter/ls_run.py (imported read-only, not modified; its per-block functions are
the method, official header length L per dataset = ls_run.OFFICIAL_L). Block/timing scope copied from R68
codec_baseline.encode_all/decode_all: the streaming LF splitter (ls_run.scan, a verbatim copy of R68 scan) spools
each exact 100,000-record block, at most 4 blocks pending, 4 block workers; each block runs its official
subprocesses one after another; archives go to attempt/archives. The decoder reads only those archives (plus
their order) and writes one complete restored file.

  encode, mode adapted : ls_run.encode_block (official sampler/training/segmentation/logshrink.run/model.7z) +
                         pack_archive -> block.lsz, then the verification decode ls_run.decode_block on that archive
                         and ls_run.build_residual against the original block -> block.res (as ls_run.Dataset
                         .block_task, without its SHA audits, which the timing driver does outside the timers)
  encode, mode native  : ls_run.encode_block + pack_archive -> block.lsz only
  decode               : ls_run.decode_block (official restore_r76 per segment + alignment) + ls_run.apply_residual
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
sys.path.insert(0, str(H.parent / 'baselines' / 'logshrink' / 'adapter'))
import ls_run as ls  # noqa: E402

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


def write_atomic(path, data):
    tmp = path.with_name(path.name + '.part')
    tmp.write_bytes(data)
    os.replace(str(tmp), str(path))


def encode_all(source, inv, attempt, dataset, native_only):
    L = ls.OFFICIAL_L[dataset]
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
                i = meta['index']
                diag = diagnostics / ('%06d' % i)
                lsz, res = archives / ('block_%06d.lsz' % i), archives / ('block_%06d.res' % i)
                info = {'index': i, 'L': L}
                start = time.perf_counter()
                try:
                    einfo, payloads, model = ls.encode_block(path, work, L, diag, TIMEOUT, dataset)
                    blob, head_len = ls.pack_archive(0 if meta['ends_with_lf'] else 1, payloads, model)
                    write_atomic(lsz, blob)
                    info.update(status='OK', encode=einfo, header_bytes=head_len, lsz_bytes=len(blob))
                    info['native_encode_seconds'] = time.perf_counter() - start
                    if not native_only:
                        t0 = time.perf_counter()
                        try:
                            native_out, dinfo = ls.decode_block(lsz, work, diag, TIMEOUT)
                        except Exception as exc:  # ls_run: status native_decode_failed, no residual file
                            info.update(status='NATIVE_DECODE_FAIL', error=err(exc))
                        else:
                            info['verify_decode_seconds'] = time.perf_counter() - t0
                            t0 = time.perf_counter()
                            rbytes, rstats = ls.build_residual(path.read_bytes(), native_out.read_bytes())
                            write_atomic(res, rbytes)
                            info.update(residual_seconds=time.perf_counter() - t0, residual_stats=rstats, res_bytes=len(rbytes))
                except Exception as exc:
                    info.update(status='ENCODE_FAIL', error=err(exc))
                info['block_encode_seconds'] = time.perf_counter() - start
                info['lsz'] = str(lsz.relative_to(attempt)) if lsz.is_file() else None
                info['res'] = str(res.relative_to(attempt)) if res.is_file() else None
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
                ls.scan(source, segment, end)
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
                if not block['lsz']:
                    info['status'] = 'NO_ARCHIVE'
                    return None, work, info
                diag = attempt / 'diagnostics' / ('%06d' % block['index'])
                try:
                    native_out, dinfo = ls.decode_block(attempt / block['lsz'], work, diag, TIMEOUT)
                    info['decode'] = dinfo
                    if not block['res']:
                        info['status'] = 'NO_RESIDUAL'
                        return None, work, info
                    fixed = ls.apply_residual((attempt / block['res']).read_bytes(), native_out.read_bytes())
                    with open(output, 'xb') as f:
                        f.write(fixed)
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
        blocks, seconds = encode_all(inv['source']['path'], inv, attempt, request['dataset'], request['mode'] == 'native')
        save(attempt / 'encode_phase.json', {'mode': request['mode'], 'blocks': blocks, 'inner_encode_seconds': seconds,
                                             'failed_blocks': [b['index'] for b in blocks if b['status'] != 'OK']})
    elif phase == 'decode':
        encoded = load(attempt / 'encode_phase.json')
        order = [{'index': b['index'], 'lsz': b['lsz'], 'res': b['res']} for b in encoded['blocks']]
        restored, seconds, infos = decode_all(attempt, order)
        save(attempt / 'decode_phase.json', {'restored': str(restored), 'inner_decode_seconds': seconds, 'blocks': infos,
                                             'failed_blocks': [b['index'] for b in infos if b['status'] != 'OK']})
    else:
        raise SystemExit(__doc__)


if __name__ == '__main__':
    main()
