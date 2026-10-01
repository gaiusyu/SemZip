"""Fit on one original block; freeze logical subfield modes before file aliasing.

V2 changes only offline policy export. No source/runtime or online search change.
SEMZIP_FIT_SOURCE_ROOT may select the exact runtime for a uniformly refitted campaign.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tarfile
import tempfile
import time

ROOT=Path(__file__).resolve().parent
SOURCE=Path(os.environ.get('SEMZIP_FIT_SOURCE_ROOT', str(ROOT/'source'))).resolve()
sys.path[:0]=[str(SOURCE/'runtime'),str(SOURCE/'backend'),str(SOURCE)]
FITTER_VERSION='logical-subfield-modes-before-alias-v2'
import pare_dataset_extract as c
import semantic_codec_frontend as front

def digest(root,meta,transformed):
    # Context-dependent streams require the real ordered restoration path.
    # Hash every reconstructed byte rather than independently decoding streams.
    restored=c.restore_text(transformed,root/'dataset_extract',meta)
    return hashlib.sha256(restored.encode('latin-1')).hexdigest()

def fit(raw,extraction,out):
    os.umask(0o022)
    out.mkdir(exist_ok=False,parents=True)
    start=time.perf_counter()
    original_transform=c.r49_transform
    c.r49_transform=lambda d,m,p:c.deduplicate_block_files(d,m)
    try:
        encoded=front._encode_block((str(raw),str(out/'transformed.log'),str(out/'input.tar.xz'),str(extraction),str(SOURCE/'runtime')))
    finally:
        c.r49_transform=original_transform
    (out/'prepare.json').write_text(json.dumps(encoded,indent=2))
    transformed=(out/'transformed.log').read_bytes().decode('latin-1')
    column_modes={};numeric_modes={}
    subfield_modes={}
    active_sharing=[None]
    original_sharing=c.r47_share_subfields
    original_alias=c._r45_transform_base
    def sharing(directory, meta, strategy):
        previous=active_sharing[0]
        active_sharing[0]=strategy
        try:
            return original_sharing(directory,meta,strategy)
        finally:
            active_sharing[0]=previous
    def alias(directory, meta, profile):
        strategy=active_sharing[0]
        if strategy is not None and not c._R54_ONLINE:
            logical={}
            for obj in c.r47_walk(meta):
                if obj.get('kind')=='r44_dictionary':
                    entry=obj['ids']
                    if entry['file'].startswith('r47_subfield_ids_'):
                        prior=logical.setdefault(entry['file'],entry['mode'])
                        if prior!=entry['mode']:
                            raise ValueError('Conflicting modes for one logical subfield ID file')
            # Each speculative strategy has its own snapshot. Physical aliases
            # are introduced only after recording the logical mode decisions.
            subfield_modes[strategy]=logical
        return original_alias(directory,meta,profile)
    original_column=c.r44_column_write
    original_numeric=c.r47_numeric_recode
    def column(directory,name,values):
        entry=original_column(directory,name,values)
        if entry:
            for col in entry['columns']:
                if 'numeric' in col:
                    n=col['numeric'];column_modes[n['file'][:-4]]=n['mode']
        return entry
    def numeric(directory,meta,mode):
        refs=[]
        for o in c.r47_walk(meta):
            if o.get('kind')=='r44_dictionary':refs.append((o['ids']['file'],o['ids']))
            if isinstance(o.get('numeric'),dict):refs.append((o['numeric']['file'],o['numeric']))
        result=original_numeric(directory,meta,mode)
        numeric_modes[mode]={name:entry['mode'] for name,entry in refs}
        return result
    c.r44_column_write=column;c.r47_numeric_recode=numeric
    c.r47_share_subfields=sharing;c._r45_transform_base=alias
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tmp=Path(tmp);initial=tmp/'initial';initial.mkdir()
            with tarfile.open(out/'input.tar.xz') as a:a.extractall(initial)
            meta=json.loads((initial/'metadata.json').read_text());expected=hashlib.sha256(raw.read_bytes()).hexdigest()
            assert digest(initial,meta,transformed)==expected
            if not (initial/'dataset_extract').exists():
                assert not encoded['streams'] and transformed.encode('latin-1')==raw.read_bytes()
                plan={'version':1,'dataset':json.loads(extraction.read_text())['dataset'],
                      'online_search':False,'recipe':{'generic_representation':'file_alias'},
                      'programs':[],'column_numeric':{},'numeric_modes':{},'subfield_ids':{},
                      'unseen_column_numeric_default':'delta','training_raw_sha256':expected}
                (out/'storage.json').write_text(json.dumps(plan,indent=2))
                (out/'fit.json').write_text(json.dumps({'status':'PASS','raw_sha_pass':True,
                    'seconds':time.perf_counter()-start,'scope':'Empty verified semantic stream; fixed empty policy.'},indent=2))
                return
            trial=tmp/'trial';shutil.copytree(initial,trial);trial_meta=copy.deepcopy(meta)
            front.reset_archive_cache()
            original_transform(trial/'dataset_extract',trial_meta,'adaptive')
            assert digest(trial,trial_meta,transformed)==expected
            recipe={s['stage']:s['selected'] for s in c.R47_STATS}
            mode=recipe.get('numeric','unchanged')
            plan={'version':1,'dataset':json.loads(extraction.read_text())['dataset'],
                  'training_raw_sha256':hashlib.sha256(raw.read_bytes()).hexdigest(),
                  'recipe':recipe,'column_numeric':column_modes,'numeric_modes':numeric_modes.get(mode,{}),
                  'subfield_ids':{},'programs':[],'online_search':False,
                  'unseen_column_numeric_default':'delta','proposal_source':'fresh gateway API extraction; no extra storage programs'}
            if mode=='adaptive':recipe['numeric']='frozen'
            selected_sharing=recipe.get('subfield_sharing','unchanged')
            if selected_sharing!='unchanged':
                if selected_sharing not in subfield_modes:
                    raise RuntimeError('Selected sharing strategy has no captured logical mode snapshot')
                plan['subfield_ids']=copy.deepcopy(subfield_modes[selected_sharing])
            (out/'subfield_decisions.json').write_text(json.dumps({
                'fitter_version':FITTER_VERSION,'selected_strategy':selected_sharing,
                'modes_by_strategy':subfield_modes,'published_modes':plan['subfield_ids']},indent=2))
            fixed=tmp/'fixed';shutil.copytree(initial,fixed);fixed_meta=copy.deepcopy(meta)
            c.r54_apply(fixed,fixed_meta,plan)
            assert digest(fixed,fixed_meta,transformed)==expected
            fixed_bytes=c.r47_cost(fixed,fixed_meta);trial_bytes=c.r47_cost(trial,trial_meta)
            assert fixed_bytes==trial_bytes,(fixed_bytes,trial_bytes)
            (out/'storage.json').write_text(json.dumps(plan,indent=2))
            (out/'fit.json').write_text(json.dumps({'status':'PASS','fixed_semantic_bytes':fixed_bytes,
                'adaptive_semantic_bytes':trial_bytes,'seconds':time.perf_counter()-start,
                'raw_sha_pass':True,'recipe':recipe,'fitter_version':FITTER_VERSION},indent=2))
    finally:
        c.r44_column_write=original_column;c.r47_numeric_recode=original_numeric
        c.r47_share_subfields=original_sharing;c._r45_transform_base=original_alias

if __name__=='__main__':fit(*map(lambda s:Path(s).resolve(),sys.argv[1:4]))
