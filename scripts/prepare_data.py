#!/usr/bin/env python3
"""Build a raw dataset from exact public archive members; preserve every byte."""
from pathlib import Path
import argparse,hashlib,json,os,tarfile,zipfile,urllib.request
ROOT=Path(__file__).resolve().parents[1]
def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for data in iter(lambda:f.read(1048576),b''):h.update(data)
    return h.hexdigest()
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset',required=True);parser.add_argument('--archive',type=Path,required=True)
    parser.add_argument('--raw-dir',type=Path,required=True)
    parser.add_argument('--download',action='store_true',help='Download the exact public archive if absent')
    args=parser.parse_args()
    datasets=json.loads((ROOT/'metadata/raw_datasets.json').read_text())['datasets']
    row=next(d for d in datasets if d['dataset']==args.dataset)
    if not args.archive.exists():
        if not args.download:parser.error('archive absent; supply a downloaded archive or --download')
        args.archive.parent.mkdir(parents=True,exist_ok=True)
        with urllib.request.urlopen(row['url']) as src,args.archive.open('xb') as dst:
            for data in iter(lambda:src.read(1048576),b''):dst.write(data)
    if digest(args.archive)!=row['archive_sha256']:raise RuntimeError('Public archive SHA mismatch')
    args.raw_dir.mkdir(parents=True,exist_ok=True);output=args.raw_dir/(args.dataset+'.log')
    if output.exists():raise RuntimeError('Refusing to overwrite existing raw file')
    partial=output.with_suffix('.log.partial')
    archive=zipfile.ZipFile(args.archive) if args.archive.suffix=='.zip' else tarfile.open(args.archive)
    full=hashlib.sha256();total=0
    with archive,partial.open('xb') as target:
        for member in row['members']:
            # Member names are frozen public archive-relative paths; no file
            # extraction and no traversal into a host filesystem occurs.
            source=archive.open(member['path']) if isinstance(archive,zipfile.ZipFile) else archive.extractfile(member['path'])
            h=hashlib.sha256();size=0
            with source:
                for data in iter(lambda:source.read(1048576),b''):
                    h.update(data);full.update(data);size+=len(data);total+=len(data);target.write(data)
            if h.hexdigest()!=member['sha256'] or size!=member['bytes']:
                raise RuntimeError('Archive member differs: '+member['path'])
    if full.hexdigest()!=row['sha256'] or total!=row['raw_bytes']:raise RuntimeError('Raw assembly SHA mismatch')
    os.rename(partial,output)
    print(json.dumps({'status':'PASS','dataset':args.dataset,'raw_bytes':total,'raw_sha256':full.hexdigest()}))
if __name__=='__main__':main()
