#!/usr/bin/env python3
"""R76 track E: static scan of the code actually embedded in the 16 R73 pool archives.

For every semantic block archive (block_NNNNN.semantic.tar.xz) the tar index is read and
metadata.json is read into memory with tarfile.extractfile (nothing is extracted to disk,
no generated code is compiled or executed). Every embedded code string is checked with
pyexec_validator under both policies; tar members are checked for path/type safety.
Usage: scan_archives.py OUT_JSON [DATASET ...]
"""
import json
import lzma
import sys
import tarfile
import time
from pathlib import Path, PurePosixPath

import pyexec_validator as v
from static_check import extract_codes, sha_file, DATASETS

FORMAL = Path('<WORKDIR>/r73_quality_gate_20260927/r73_qg/runs/formal/pool')


def member_problem(m):
    name = m.name
    p = PurePosixPath(name)
    if name.startswith('/') or p.is_absolute():
        return 'absolute path'
    if any(part == '..' for part in p.parts):
        return 'parent traversal'
    if not (m.isreg() or m.isdir()):
        return 'non-regular member type %r' % m.type
    return None


def main(out, datasets):
    t0 = time.time()
    verdicts, per_ds = {}, {}
    for d in datasets:
        sem = FORMAL / d / 'archive' / 'semantic'
        manifest = json.loads((FORMAL / d / 'archive' / 'semantic_manifest.json').read_text())
        names = manifest['semantic_archives']
        rec = {'blocks': len(names), 'tar_member_problems': [], 'blocks_with_code': 0,
               'code_occurrences': 0, 'unique_codes': {}, 'blocks_with_rejected_strict': [],
               'blocks_with_rejected_template': [], 'stream_kinds': {}}
        for i, name in enumerate(names):
            with lzma.open(sem / name) as f, tarfile.open(fileobj=f, mode='r') as t:
                members = t.getmembers()
                for m in members:
                    prob = member_problem(m)
                    if prob:
                        rec['tar_member_problems'].append({'block': i, 'member': m.name, 'problem': prob})
                md = json.loads(t.extractfile('metadata.json').read().decode('utf-8'))
            for s in md.get('streams', []):
                k = str(s.get('kind'))
                rec['stream_kinds'][k] = rec['stream_kinds'].get(k, 0) + 1
            codes = list(extract_codes(md))
            # archive metadata stores each program twice (stream 'program' and 'context_tag'); count distinct per block
            distinct = {(v.code_sha256(c), kind): c for _, kind, c, _ in codes}
            rec['blocks_with_code'] += bool(distinct)
            rec['code_occurrences'] += len(distinct)
            bad_s = bad_t = False
            for (h, kind), code in distinct.items():
                key = h + ':' + kind
                if key not in verdicts:
                    rs = v.check(code, kind)
                    rt = v.check_policy(code, kind, 'template')
                    verdicts[key] = {'code_sha256': h, 'kind': kind, 'strict_ok': rs['ok'],
                                     'template_ok': rt['ok'], 'strict_error_rules': sorted({e['rule'] for e in rs['errors']}),
                                     'template_errors': rt['errors'], 'template_note': rt.get('template'),
                                     'strict_errors': rs['errors'], 'warnings': sorted({w['rule'] for w in rs['warnings']})}
                vd = verdicts[key]
                u = rec['unique_codes'].setdefault(key, {'blocks': 0, 'strict_ok': vd['strict_ok'], 'template_ok': vd['template_ok']})
                u['blocks'] += 1
                bad_s |= not vd['strict_ok']
                bad_t |= not vd['template_ok']
            if bad_s:
                rec['blocks_with_rejected_strict'].append(i)
            if bad_t:
                rec['blocks_with_rejected_template'].append(i)
        rec['n_blocks_with_rejected_strict'] = len(rec['blocks_with_rejected_strict'])
        rec['n_blocks_with_rejected_template'] = len(rec['blocks_with_rejected_template'])
        rec['blocks_with_rejected_strict'] = rec['blocks_with_rejected_strict'][:50]
        rec['blocks_with_rejected_template'] = rec['blocks_with_rejected_template'][:50]
        per_ds[d] = rec
        print(d, rec['blocks'], 'codes', len(rec['unique_codes']), 'strict-rejected blocks', rec['n_blocks_with_rejected_strict'],
              'template-rejected blocks', rec['n_blocks_with_rejected_template'], 'tar problems', len(rec['tar_member_problems']),
              round(time.time() - t0), 's', flush=True)
    result = {'validator_version': v.VERSION, 'validator_sha256': sha_file(v.__file__),
              'script_sha256': sha_file(__file__), 'seconds': time.time() - t0,
              'unique_codes': len(verdicts), 'unique_codes_rejected_strict': sum(not x['strict_ok'] for x in verdicts.values()),
              'unique_codes_rejected_template': sum(not x['template_ok'] for x in verdicts.values()),
              'datasets': per_ds, 'verdicts': verdicts}
    Path(out).write_text(json.dumps(result, indent=1) + '\n')


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:] or DATASETS)
