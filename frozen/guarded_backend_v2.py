#!/usr/bin/env python3
"""Semantic archive guard plus strict encode/decode block cache boundaries.

Imports the preserved R69 guard and source. All additions are in this adapter
and runtime_cache_boundary.py; neither historic source nor old guard is edited.
"""
from __future__ import annotations
from pathlib import Path
import hashlib
import json
import os
import time
import guarded_backend as previous
import runtime_cache_boundary as boundary

VERSION='R71-SEMANTIC-GUARD-STRICT-BLOCK-STATE-V2'
_previous_configure=previous._configure
_previous_encode=previous.encode
_previous_decode=previous.decode


def _configure(runtime):
    bounded,frontend=_previous_configure(runtime)
    if not hasattr(bounded,'_r71_original_decode_job'):
        bounded._r71_original_decode_job=bounded._decode_job
    bounded._encode_job=_encode_job
    bounded._decode_job=_decode_job
    return bounded,frontend


def _attempt(job,decoded):
    """Verify the written semantic archive with cold data/callable state.

    This reset is separate from the worker boundary: encoding this very block
    may have warmed a generated function's mutable globals. The inverse must
    not obtain its answer from those globals. Recovery uses the same path.
    """
    raw,transformed,archive,plan,runtime=(Path(item) for item in job)
    bounded,frontend=_configure(runtime)
    path=archive.parent.parent.parent/'logs'/('semantic_cache_'+raw.stem+'.json')
    trace={'version':VERSION,'plan_sha256':hashlib.sha256(plan.read_bytes()).hexdigest(),
           'raw_block':raw.name,'events':[],'stage':'attempt_entry','verified':False}
    try:
        trace['events'].append(boundary.clear(runtime,'semantic_attempt_entry'))
        trace['stage']='semantic_encode'
        metadata=frontend._encode_block(job)
        start=time.perf_counter()
        trace['stage']='cold_archive_inverse_entry'
        trace['events'].append(boundary.clear(runtime,'semantic_archive_inverse_entry'))
        trace['stage']='actual_archive_inverse'
        frontend._decode_block((str(transformed),str(archive),str(decoded),str(runtime)))
        original=raw.read_bytes()
        restored=decoded.read_bytes()
        if restored!=original:
            raise previous.SemanticArchiveMismatch('Actual semantic archive replay changed original bytes')
        if bounded._line_shape(original)!=bounded._line_shape(transformed.read_bytes()):
            raise previous.SemanticArchiveMismatch('Semantic replay changed physical LF block boundaries')
        trace['verified']=True
        trace['stage']='complete'
        metadata['semantic_archive_verified_sha256']=hashlib.sha256(restored).hexdigest()
        metadata['semantic_archive_cache_boundary']={
            'version':boundary.VERSION,'cold_inverse':True,
            'reset_count':len(trace['events']),
            'clear_seconds':sum(event['seconds'] for event in trace['events'])}
        return metadata,time.perf_counter()-start
    finally:
        # One append per attempted representation; preserve the failed first
        # attempt when the fixed residual-only recovery succeeds.
        traces=json.loads(path.read_text()) if path.exists() else {'attempts':[]}
        traces['attempts'].append(trace)
        previous._json(path,traces)


def _encode_job(job):
    runtime=Path(job['runtime'])
    bounded,_frontend=_configure(runtime)
    events=[boundary.clear(runtime,'encode_entry')]
    metadata=None
    try:
        metadata=previous._guarded_encode_job(job)
        return metadata
    finally:
        events.append(boundary.clear(runtime,'encode_exit'))
        trace={'version':VERSION,'pid':os.getpid(),'block_index':job['index'],
               'operation':'encode','events':events,
               'all_data_caches_empty_at_boundaries':all(event['all_data_caches_empty'] for event in events)}
        previous._json(Path(job['logs'])/f"cache_encode_{job['index']:05d}.json",trace)
        if metadata is not None:
            metadata['cache_boundary']={
                'version':boundary.VERSION,'entry_empty':True,'exit_empty':True,
                'clear_seconds':sum(event['seconds'] for event in events),
                'data_lru_count':sum(len(names) for names in boundary.DATA_LRU.values()),
                'mutable_dict_count':len(boundary.MUTABLE_DICTS)}


def _decode_job(job):
    runtime=Path(job['runtime'])
    bounded,_frontend=_configure(runtime)
    events=[boundary.clear(runtime,'decode_entry')]
    try:
        return bounded._r71_original_decode_job(job)
    finally:
        events.append(boundary.clear(runtime,'decode_exit'))
        previous._json(Path(job['logs'])/f"cache_decode_{job['index']:05d}.json",
            {'version':VERSION,'pid':os.getpid(),'block_index':job['index'],'operation':'decode',
             'events':events,'all_data_caches_empty_at_boundaries':
                 all(event['all_data_caches_empty'] for event in events)})


def encode(raw,plan,runtime,result,dataset,block_size,workers):
    summary=_previous_encode(raw,plan,runtime,result,dataset,block_size,workers)
    summary['guard_version']=VERSION
    summary['cache_boundary_version']=boundary.VERSION
    summary['cache_boundary_verified_blocks']=sum(
        block['cache_boundary']['entry_empty'] and block['cache_boundary']['exit_empty']
        for block in summary['blocks'])
    summary['online_compression_scope']+='; includes data-cache/callable clearing at every original block entry/exit, before each semantic attempt and before each actual archive inverse, plus trace logging'
    return summary


def decode(archive,runtime,result,workers,output=None,expected=None):
    summary=_previous_decode(archive,runtime,result,workers,output,expected)
    traces=sorted((result/'decode_logs').glob('cache_decode_*.json'))
    valid=[json.loads(path.read_text()) for path in traces]
    if len(valid)!=summary['decoded_blocks'] or not all(
            item['all_data_caches_empty_at_boundaries'] for item in valid):
        raise RuntimeError('Independent decode cache-boundary traces are missing')
    summary['cache_boundary_version']=boundary.VERSION
    summary['cache_boundary_verified_blocks']=len(valid)
    summary['decode_timing_scope']='Existing decode timer includes worker cache clearing, trace logging, native+semantic decode and output sink; excludes interpreter import and summary serialization'
    return summary


# The previous CLI dispatches through its module globals. Patching only that
# in-memory adapter keeps command compatibility and supports fork/spawn workers.
previous._configure=_configure
previous._attempt=_attempt
previous.encode=encode
previous.decode=decode

if __name__=='__main__':
    previous.main()
