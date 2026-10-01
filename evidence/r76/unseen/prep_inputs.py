#!/usr/bin/env python3
"""R76-G input preparation (run once, cwd = unseen/).
raw/<name>.gz (copied unchanged from the Mac download of https://ita.ee.lbl.gov/traces/<name>.gz)
  -> raw/<D>.log = exact gunzip output (no cleaning, no newline normalisation)
  -> train/<D>/train.log = original block 0 (first 100,000 LF records, same readline rule as lib.first_block)
  -> inputs.json (gz + raw SHA-256, bytes, LF count, records, 100k-record blocks with per-block SHA-256/bytes)
  -> reference.json (r71 reference.json row format, read by code/formal_full.py through FORMAL_REF)
Refuses to overwrite an existing raw/<D>.log, block-0 file or inputs.json with different content."""
import gzip, hashlib, json, os, subprocess, sys, time
from pathlib import Path

U = Path(__file__).resolve().parent
FILES = [('NASA', 'NASA_access_log_Jul95'), ('ClarkNet', 'clarknet_access_log_Aug28'),
         ('USask', 'usask_access_log'), ('Calgary', 'calgary_access_log')]
BLOCK = 100000
HIGH = bytes(range(128, 256))

def sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''): h.update(b)
    return h.hexdigest()

def main():
    out = {'design': 'DEV_DESIGN_R76_zh.md section R76-G (pre-registered 2026-10-01 13:20 UTC)',
           'source': 'LBL Internet Traffic Archive, https://ita.ee.lbl.gov/traces/<name>.gz',
           'block_lines': BLOCK, 'created': time.strftime('%Y-%m-%d %H:%M:%S %z'), 'datasets': {}}
    for D, name in FILES:
        gz = U/'raw'/f'{name}.gz'; raw = U/'raw'/f'{D}.log'
        t = subprocess.run(['gzip', '-t', str(gz)], capture_output=True, text=True)
        assert t.returncode == 0, (gz, t.stderr)
        tmp = raw.with_suffix('.log.tmp')
        with gzip.open(gz, 'rb') as src, open(tmp, 'wb') as dst:
            for b in iter(lambda: src.read(1 << 20), b''): dst.write(b)
        if raw.exists():
            assert sha(raw) == sha(tmp), f'{raw} exists with different content'
            tmp.unlink()
        else:
            os.replace(tmp, raw)
        # independent check: system gunzip -c gives the same bytes
        z = subprocess.run(f'gzip -dc {gz} | sha256sum', shell=True, capture_output=True, text=True, check=True).stdout.split()[0]
        full = hashlib.sha256(); blocks = []; lf = 0; nbytes = 0; cr = 0; nul = 0; nonascii = 0
        b0 = U/'train'/D/'train.log'; b0.parent.mkdir(parents=True, exist_ok=True); b0tmp = b0.with_suffix('.log.tmp')
        with open(raw, 'rb') as f, open(b0tmp, 'wb') as w0:
            while True:
                h = hashlib.sha256(); size = rec = 0
                for _ in range(BLOCK):
                    line = f.readline()
                    if not line: break
                    h.update(line); full.update(line); size += len(line); rec += 1
                    lf += line.endswith(b'\n'); cr += line.count(b'\r'); nul += line.count(b'\0')
                    nonascii += len(line) - len(line.translate(None, HIGH))
                    if not blocks: w0.write(line)
                if not rec: break
                blocks.append({'index': len(blocks), 'sha256': h.hexdigest(), 'bytes': size, 'records': rec})
                nbytes += size
        if b0.exists():
            assert sha(b0) == sha(b0tmp), f'{b0} exists with different content'
            b0tmp.unlink()
        else:
            os.replace(b0tmp, b0)
        rsha = full.hexdigest()
        assert rsha == sha(raw) == z and nbytes == raw.stat().st_size
        assert sha(b0) == blocks[0]['sha256'] and b0.stat().st_size == blocks[0]['bytes']
        rec = {'dataset': D, 'source_name': name, 'url': f'https://ita.ee.lbl.gov/traces/{name}.gz',
               'gz_path': str(gz), 'gz_bytes': gz.stat().st_size, 'gz_sha256': sha(gz), 'gzip_test': 'OK',
               'raw_path': str(raw), 'raw_bytes': nbytes, 'raw_sha256': rsha, 'raw_sha256_via_gzip_dc': z,
               'lf_count': lf, 'records': sum(b['records'] for b in blocks), 'lines': sum(b['records'] for b in blocks),
               'final_record_unterminated': lf != sum(b['records'] for b in blocks),
               'cr_bytes': cr, 'nul_bytes': nul, 'non_ascii_bytes': nonascii,
               'blocks': len(blocks), 'block_bytes': [b['bytes'] for b in blocks],
               'block0_path': str(b0), 'block0_sha256': blocks[0]['sha256'], 'block0_bytes': blocks[0]['bytes'],
               'suffix_raw_bytes': nbytes - blocks[0]['bytes'], 'block_audit': blocks}
        out['datasets'][D] = rec
        print(D, name, 'bytes', nbytes, 'lines', rec['lines'], 'blocks', len(blocks), 'sha', rsha, 'cr', cr, 'nul', nul,
              'nonascii', nonascii, 'unterminated_tail', rec['final_record_unterminated'], flush=True)
    ij = U/'inputs.json'
    if ij.exists():
        old = json.loads(ij.read_text())
        for D in out['datasets']:
            for k in ('raw_sha256', 'raw_bytes', 'blocks', 'block0_sha256', 'gz_sha256'):
                assert old['datasets'][D][k] == out['datasets'][D][k], (D, k)
        print('inputs.json exists and matches; not rewritten'); return
    ij.write_text(json.dumps(out, indent=1) + '\n')
    ref = [{'dataset': D, 'raw_bytes': str(r['raw_bytes']), 'raw_sha256': r['raw_sha256'], 'blocks': str(r['blocks']),
            'source': 'unseen/inputs.json'} for D, r in out['datasets'].items()]
    (U/'reference.json').write_text(json.dumps(ref, indent=1) + '\n')
    print('wrote inputs.json, reference.json')

if __name__ == '__main__':
    main()
