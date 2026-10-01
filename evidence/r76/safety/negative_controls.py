#!/usr/bin/env python3
"""R76 track E negative controls: tampered copies of the 1-block R73 Apache archive (own directory only).

NC1 import      : 'import os' prepended to one embedded python_exec code (the frozen runtime guard also rejects)
NC2 format_leak : top-level  leak = '{0.__class__}'.format(1)  added to one embedded code. The frozen runtime
                  guard ACCEPTS this (it only blocks '__' in Name/Attribute nodes, not inside format strings);
                  output bytes are unchanged. Expected: frozen decoder SHA-pass, enforced decoder fails closed.
NC3 tar_dotdot  : an extra tar member '../r76_nc3_escape.txt' in the semantic archive. Only the enforced decoder
                  is run on it (the frozen extractall would write outside its temp dir).
Each tampered archive is decoded with run_verify.py decode (policy strict); NC1/NC2 also with the frozen CLI.
Usage: negative_controls.py OUTDIR
"""
import io
import json
import lzma
import os
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

SAFETY = Path(__file__).resolve().parent
sys.path.insert(0, str(SAFETY))
from run_verify import FORMAL, INVENTORY, clean_env, sha_file  # noqa: E402

ART = Path('<WORKDIR>/r73_quality_gate_20260927/r73_qg/art')
SRC = FORMAL / 'Apache' / 'archive'


def stable(ti):
    ti.uid = ti.gid = 0
    ti.uname = ti.gname = ''
    ti.mtime = 0
    return ti


def rebuild(dst_archive, edit_metadata=None, extra_member=None):
    shutil.copytree(SRC, dst_archive)
    sem = dst_archive / 'semantic' / 'block_00000.semantic.tar.xz'
    with lzma.open(sem) as f, tarfile.open(fileobj=f, mode='r') as t:
        items = [(m, t.extractfile(m).read() if m.isreg() else None) for m in t.getmembers()]
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w') as tar:
        for m, data in items:
            if m.name == 'metadata.json' and edit_metadata:
                data = json.dumps(edit_metadata(json.loads(data)), separators=(',', ':'), sort_keys=True).encode()
            ti = tarfile.TarInfo(m.name)
            ti.size = len(data)
            ti.mode = m.mode
            tar.addfile(stable(ti), io.BytesIO(data))
        if extra_member:
            ti = tarfile.TarInfo(extra_member)
            payload = b'r76 negative control\n'
            ti.size = len(payload)
            tar.addfile(stable(ti), io.BytesIO(payload))
    sem.write_bytes(lzma.compress(buf.getvalue(), preset=6 | lzma.PRESET_EXTREME))


def edit_code(transform):
    def edit(md):
        for s in md['streams']:
            if s.get('kind') == 'open_function' and 'program' in s:
                prog = json.loads(s['program'])
                prog['code'] = transform(prog['code'])
                s['program'] = s['context_tag'] = json.dumps(prog, sort_keys=True)
                return md
        raise RuntimeError('no open_function stream')
    return edit


def main(out):
    out = Path(out).resolve()
    out.mkdir(parents=True, exist_ok=False)
    cases = {
        'NC1_import': dict(edit_metadata=edit_code(lambda c: 'import os\n' + c)),
        'NC2_format_leak': dict(edit_metadata=edit_code(lambda c: "leak = '{0.__class__}'.format(1)\n" + c)),
        'NC3_tar_dotdot': dict(extra_member='../r76_nc3_escape.txt'),
    }
    inv = json.loads((INVENTORY / 'input_Apache.json').read_text())
    results = {}
    for name, kw in cases.items():
        case = out / name
        rebuild(case / 'archive', **kw)
        r = {'archive_semantic_sha256': sha_file(case / 'archive/semantic/block_00000.semantic.tar.xz')}
        dec = case / 'enforced_strict'
        p = subprocess.run([sys.executable, str(SAFETY / 'run_verify.py'), 'decode', '--dataset', 'Apache', '--policy', 'strict',
                            '--out', str(dec), '--workers', '1', '--archive', str(case / 'archive')],
                           env=clean_env(case), capture_output=True, text=True)
        dr = json.loads((dec / 'decode_result.json').read_text())
        r['enforced'] = {'returncode': p.returncode, 'decode_status': dr['decode_status'], 'decode_error': dr['decode_error'],
                         'decoded_sha_matches_raw': (dr.get('decode_summary') or {}).get('decoded_sha256') == inv['raw_sha256']}
        if name != 'NC3_tar_dotdot':
            fz = case / 'frozen_decoder'
            restored = case / 'frozen_restored.log'
            p2 = subprocess.run([sys.executable, str(ART / 'frozen/guarded_backend_v2.py'), 'decode', '--archive',
                                 str(case / 'archive'), '--output', str(restored), '--semzip-source',
                                 str(ART / 'source/runtime'), '--result', str(fz), '--workers', '1'],
                                env=clean_env(case), capture_output=True, text=True)
            r['frozen'] = {'returncode': p2.returncode, 'stderr_tail': p2.stderr[-600:],
                           'decoded_sha_matches_raw': restored.exists() and sha_file(restored) == inv['raw_sha256']}
            if restored.exists():
                restored.unlink()
        results[name] = r
        print(name, json.dumps(r), flush=True)
    (out / 'negative_controls_result.json').write_text(json.dumps(results, indent=1) + '\n')


if __name__ == '__main__':
    main(sys.argv[1])
