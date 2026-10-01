#!/usr/bin/env python3
"""Verify package bytes, relative deployment mapping, and common secret patterns."""
from pathlib import Path
import hashlib,json,re
ROOT=Path(__file__).resolve().parents[1]
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    manifest=json.loads((ROOT/'MANIFEST.json').read_text());fail=[]
    for name,digest in manifest['files'].items():
        p=ROOT/name
        if not p.is_file() or sha(p)!=digest:fail.append({'file':name,'reason':'hash_mismatch'})
    for d in json.loads((ROOT/'metadata/deployments.json').read_text())['datasets']:
        for field in ['program','storage']:
            name=Path(d[field])
            if name.is_absolute() or '..' in name.parts:fail.append({'file':str(name),'reason':'nonportable_mapping'})
            elif sha(ROOT/name)!=d[field+'_sha256']:fail.append({'file':str(name),'reason':'deployment_hash'})
    # Do not echo a matched token. Variable names in preserved source are not
    # credentials; only credential-shaped literal values are flagged here.
    secret=re.compile(rb'(?:sk-[A-Za-z0-9_-]{24,}|Bearer\s+[A-Za-z0-9_-]{24,})')
    for name in manifest['files']:
        p=ROOT/name
        if secret.search(p.read_bytes()):fail.append({'file':name,'reason':'possible_literal_credential'})
    print(json.dumps({'status':'FAIL' if fail else 'PASS','files':len(manifest['files']),
                      'failures':fail,'scope':'Frozen file hashes and targeted credential patterns; not a proof of anonymity'}))
    if fail:raise SystemExit(2)
if __name__=='__main__':main()
