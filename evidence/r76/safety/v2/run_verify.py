#!/usr/bin/env python3
"""R76 track E step 5 driver: validator-enforced archive-only decode of the 16 R73 pool archives.
(V2 copy in safety/v2/: identical to ../run_verify.py except this line; imports ./enforced_decode.py -> V2 validator.)

  run_verify.py decode --dataset D --policy strict|template --out DIR [--workers 3] [--archive DIR]
  run_verify.py audit  --dataset D --out DIR [--archive DIR]
  run_verify.py all    --root DIR [--workers 3] [--datasets D ...]

`all` runs datasets sequentially, smallest raw size first (largest last), each in a fresh
interpreter: pre-registered policy 'strict' first; ONLY if strict fails closed because the validator
rejected archive code, the post-hoc secondary policy 'template' is run for that dataset into a
separate directory. No re-encoding; archives are read-only inputs.
The audit compares every decoded block (sha256, bytes, records) with the independent R68 block
inventory and the formal encode summary, the full-file sha with the inventory and R71 reference,
and re-hashes every archive file against the formal run's recorded archive_files.
"""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

SAFETY = Path(__file__).resolve().parent
ROOT = Path('<WORKDIR>')
FORMAL = ROOT / 'r73_quality_gate_20260927/r73_qg/runs/formal/pool'
INVENTORY = ROOT / 'r68_external_20260923/first_pass'
REFERENCE = ROOT / 'r71_strict_main_20260923/reference.json'
ORDER = ['Linux', 'Proxifier', 'Apache', 'Zookeeper', 'Mac', 'HealthApp', 'HPC', 'Hadoop', 'OpenStack', 'OpenSSH',
         'Android', 'BGL', 'HDFS', 'Spark', 'Windows', 'Thunderbird']  # ascending raw bytes


def sha_file(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def clean_env(out):
    env = {k: val for k, val in os.environ.items()
           if not (k.startswith(('PARE_', 'YUNWU_', 'SEMZIP_')) or k.endswith('_API_KEY'))}
    env['SEMZIP_R54_PLAN'] = str(Path(out).resolve() / 'nonexistent-external-policy.json')
    env['PYTHONHASHSEED'] = '0'
    return env


def cmd_decode(a):
    os.environ['R76_VALIDATOR_POLICY'] = a.policy
    sys.path.insert(0, str(SAFETY))
    import enforced_decode
    archive = a.archive or FORMAL / a.dataset / 'archive'
    sys.argv = ['enforced_decode', '--dataset', a.dataset, '--archive', str(archive), '--result', str(a.out),
                '--workers', str(a.workers), '--expected', str(INVENTORY / ('input_%s.json' % a.dataset))]
    enforced_decode.main()


def audit(dataset, out, archive=None, formal_dir=None):
    out = Path(out)
    archive = Path(archive) if archive else FORMAL / dataset / 'archive'
    formal_dir = Path(formal_dir) if formal_dir else FORMAL / dataset
    checks, fails = {}, []

    def chk(name, cond):
        checks[name] = bool(cond)
        if not cond:
            fails.append(name)

    dr = json.loads((out / 'decode_result.json').read_text())
    inv = json.loads((INVENTORY / ('input_%s.json' % dataset)).read_text())
    ref = next(r for r in json.loads(REFERENCE.read_text()) if r['dataset'] == dataset)
    enc = json.loads((formal_dir / 'encode_summary.json').read_text())
    formal = json.loads((formal_dir / 'result.json').read_text())
    s = dr.get('decode_summary') or {}
    chk('decode_completed', dr['decode_status'] == 'DECODED')
    chk('full_sha_eq_inventory', s.get('decoded_sha256') == inv['raw_sha256'])
    chk('full_sha_eq_r71_reference', s.get('decoded_sha256') == ref['raw_sha256'])
    chk('full_sha_eq_encode_summary', s.get('decoded_sha256') == enc['raw_sha256'])
    chk('full_bytes_eq_inventory', s.get('decoded_bytes') == inv['raw_bytes'])
    chk('decoder_failed_blocks_empty', s.get('failed_blocks') == [])
    chk('block_count', s.get('decoded_blocks') == len(inv['blocks']) == int(ref['blocks']) == len(enc['blocks']))
    logs = {}
    for p in (out / 'decode' / 'decode_logs').glob('r76_validator_*.json'):
        r = json.loads(p.read_text())
        logs[r['block_index']] = r
    block_rows, bad_blocks = [], []
    codes, exec_calls, tmpl_blocks, warn_rules = {}, 0, 0, {}
    for i, b in enumerate(inv['blocks']):
        r = logs.get(i)
        e = enc['blocks'][i] if i < len(enc['blocks']) else {}
        ok = (r is not None and r.get('status') == 'PASS' and r.get('decoded_sha256') == b['raw_sha256'] == e.get('raw_sha256')
              and r.get('decoded_bytes') == b['raw_bytes'] and r.get('decoded_records') == b['records']
              and (b['records'] == 100000 or i == len(inv['blocks']) - 1)
              and r.get('preflight_blocks') == 1 and r.get('rejections') == [] and r.get('policy') == dr['policy'])
        if not ok:
            bad_blocks.append(i)
        if r:
            exec_calls += r.get('exec_calls', 0)
            tmpl_blocks += any(c.get('template_wrapped') for c in r.get('validated_codes', []))
            for c in r.get('validated_codes', []):
                k = c['code_sha256'] + ':' + c['kind']
                codes.setdefault(k, {'blocks': 0, 'uses': 0, 'template_wrapped': c.get('template_wrapped'),
                                     'warnings': c.get('warnings')})
                codes[k]['blocks'] += 1
                codes[k]['uses'] += c.get('uses', 0)
        block_rows.append({'index': i, 'ok': ok, 'sha256': (r or {}).get('decoded_sha256'),
                           'bytes': (r or {}).get('decoded_bytes'), 'records': (r or {}).get('decoded_records'),
                           'validated_codes': len((r or {}).get('validated_codes', [])),
                           'exec_calls': (r or {}).get('exec_calls'),
                           'status': (r or {}).get('status'), 'message': (r or {}).get('message')})
    chk('every_block_logged_pass_sha_bytes_records_preflight', not bad_blocks and len(logs) == len(inv['blocks']))
    # archive identity: same files, bytes and sha as recorded by the formal R73 run
    files = {str(p.relative_to(archive)): {'bytes': p.stat().st_size, 'sha256': sha_file(p)}
             for p in archive.rglob('*') if p.is_file()}
    chk('archive_files_identical_to_formal_record', files == formal['archive_files'])
    archive_bytes = sum(x['bytes'] for x in files.values())
    chk('archive_bytes_eq_formal', archive_bytes == formal['encode']['archive_bytes'])
    first_fail = next((row for row in block_rows if row['status'] == 'FAIL'), None)
    res = {'dataset': dataset, 'policy': dr['policy'], 'status': 'PASS' if not fails else 'FAIL', 'failed_checks': fails,
           'checks': checks, 'decode_error': dr.get('decode_error'), 'first_failed_block': first_fail,
           'raw_bytes': inv['raw_bytes'], 'archive_bytes': archive_bytes,
           'ratio': inv['raw_bytes'] / archive_bytes if archive_bytes else None,
           'blocks': len(inv['blocks']), 'blocks_logged': len(logs), 'bad_blocks': bad_blocks[:100], 'n_bad_blocks': len(bad_blocks),
           'decoded_sha256': s.get('decoded_sha256'), 'expected_sha256': inv['raw_sha256'],
           'decode_seconds': s.get('decode_seconds'), 'decode_wall_seconds': dr.get('decode_wall_seconds'),
           'workers': dr.get('workers'), 'unique_codes_validated': len(codes), 'codes': codes,
           'blocks_with_template_wrapped_code': tmpl_blocks, 'exec_calls_total': exec_calls,
           'validator_version': dr.get('validator_version'), 'validator_sha256': dr.get('validator_sha256'),
           'enforced_decode_sha256': dr.get('enforced_decode_sha256'), 'frozen_guard_sha256': dr.get('frozen_guard_sha256'),
           'runtime_sha256': dr.get('runtime_sha256'), 'mp_start_method': dr.get('mp_start_method')}
    (out / 'result.json').write_text(json.dumps(res, indent=1) + '\n')
    (out / 'blocks.json').write_text(json.dumps(block_rows) + '\n')
    return res


def cmd_audit(a):
    print(json.dumps({k: val for k, val in audit(a.dataset, a.out, a.archive).items() if k != 'codes'}, indent=1))


def run_one(dataset, policy, out, workers):
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    t = time.time()
    with open(str(out) + '.log', 'w') as log:
        p = subprocess.run([sys.executable, str(Path(__file__).resolve()), 'decode', '--dataset', dataset, '--policy', policy,
                            '--out', str(out), '--workers', str(workers)], env=clean_env(out.parent),
                           stdout=log, stderr=subprocess.STDOUT)
    if not (out / 'decode_result.json').exists():
        return {'dataset': dataset, 'policy': policy, 'status': 'FAIL', 'failed_checks': ['no decode_result.json'],
                'returncode': p.returncode, 'wall': time.time() - t}
    res = audit(dataset, out)
    res['returncode'], res['wall'] = p.returncode, time.time() - t
    return res


def cmd_all(a):
    root = Path(a.root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    status_path = root / 'status.json'
    rows = []
    t0 = time.time()
    for d in a.datasets or ORDER:
        for policy in ('strict', 'template'):
            if policy == 'template':
                prev = rows[-1]
                rejected = prev['status'] == 'FAIL' and 'UnsafeGeneratedProgram' in json.dumps(
                    [prev.get('decode_error'), prev.get('first_failed_block')])
                if not rejected:
                    break
            out = root / policy / d
            if out.exists():
                raise SystemExit('refusing to overwrite ' + str(out))
            res = run_one(d, policy, out, a.workers)
            row = {k: res.get(k) for k in ('dataset', 'policy', 'status', 'failed_checks', 'decode_error', 'first_failed_block',
                                             'raw_bytes', 'archive_bytes', 'ratio', 'blocks', 'n_bad_blocks',
                                             'decoded_sha256', 'expected_sha256', 'decode_seconds', 'wall',
                                             'unique_codes_validated', 'blocks_with_template_wrapped_code', 'exec_calls_total',
                                             'returncode')}
            rows.append(row)
            status_path.write_text(json.dumps({'updated_unix': time.time(), 'elapsed': time.time() - t0, 'rows': rows}, indent=1) + '\n')
            print(json.dumps({k: row[k] for k in ('dataset', 'policy', 'status', 'failed_checks', 'wall')}), flush=True)
    strict = {r['dataset']: r['status'] for r in rows if r['policy'] == 'strict'}
    tmpl = {r['dataset']: r['status'] for r in rows if r['policy'] == 'template'}
    summary = {'track': 'R76-E', 'finished_unix': time.time(), 'elapsed_seconds': time.time() - t0, 'workers': a.workers,
               'strict_pass': sorted(k for k, v in strict.items() if v == 'PASS'),
               'strict_fail': sorted(k for k, v in strict.items() if v != 'PASS'),
               'template_run_for': sorted(tmpl), 'template_pass': sorted(k for k, v in tmpl.items() if v == 'PASS'),
               'template_fail': sorted(k for k, v in tmpl.items() if v != 'PASS'), 'rows': rows}
    (root / 'VERIFY_SUMMARY.json').write_text(json.dumps(summary, indent=1) + '\n')
    print(json.dumps({k: summary[k] for k in ('strict_pass', 'strict_fail', 'template_pass', 'template_fail', 'elapsed_seconds')}), flush=True)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('decode')
    p.add_argument('--dataset', required=True)
    p.add_argument('--policy', choices=('strict', 'template'), required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--workers', type=int, default=3)
    p.add_argument('--archive', type=Path)
    p.set_defaults(fn=cmd_decode)
    p = sub.add_parser('audit')
    p.add_argument('--dataset', required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--archive', type=Path)
    p.set_defaults(fn=cmd_audit)
    p = sub.add_parser('all')
    p.add_argument('--root', required=True)
    p.add_argument('--workers', type=int, default=3)
    p.add_argument('--datasets', nargs='*')
    p.set_defaults(fn=cmd_all)
    a = ap.parse_args()
    a.fn(a)


if __name__ == '__main__':
    main()
