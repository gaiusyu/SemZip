"""Explicit data-cache boundary for the existing immutable SemZip source.

Imports the actual worker runtime through its already loaded frontend. It never
loads a second copy of a module. Clear before and after each original encode or
decode block; costs belong inside that operation's existing timer.
"""
from __future__ import annotations
from pathlib import Path
import re
import sys
import time

VERSION = 'SEMZIP-STRICT-DATA-CACHE-BOUNDARY-V1-20260923'
DATA_LRU = {
    'semzip_pure': ('template_key',),
    'pare_dataset_extract': (
        '_template_cache_is_value_like', '_template_cache_value_shape', '_template_cache_line_tokens',
        '_line_transducer_is_placeholder', '_line_transducer_is_plain_alpha',
        '_line_transducer_is_digit_or_special_value', '_line_transducer_anchor_token',
        '_line_transducer_token_shape', '_parse_open_function_value_cached',
        '_render_open_function_exact_cached', '_zero_padding_index_map_cached')}
IMMUTABLE_LRU = {
    'semzip_pure': ('compiled_multiline_regex', '_parsed_replacement_template'),
    'pare_dataset_extract': ('_canonical_program_json_cached', '_compiled_value_regex')}
MUTABLE_DICTS = ('STREAM_CODEC_CANDIDATE_CACHE', 'CODEC_DECISION_CACHE',
                 'COMPILED_PYTHON_EXEC_CACHE', 'COMPILED_CONTEXT_PROJECTOR_CACHE')


def modules(runtime: Path):
    runtime = runtime.resolve()
    frontend = sys.modules.get('semantic_codec_frontend')
    if frontend is None:
        raise RuntimeError('Configure the existing frontend before clearing caches')
    semzip, extract = frontend._load_semzip_runtime(runtime)
    loaded = {'semzip_pure':semzip, 'pare_dataset_extract':extract}
    for name,module in loaded.items():
        if sys.modules.get(name) is not module or Path(module.__file__).resolve() != runtime/(name+'.py'):
            raise RuntimeError('Cache boundary cannot mix imported runtime roots')
    return frontend, loaded


def snapshot(runtime: Path):
    frontend, loaded = modules(runtime)
    extract = loaded['pare_dataset_extract']
    return {
        'data_lru':{module+'.'+name:getattr(loaded[module],name).cache_info()._asdict()
                    for module,names in DATA_LRU.items() for name in names},
        'immutable_lru':{module+'.'+name:getattr(loaded[module],name).cache_info()._asdict()
                         for module,names in IMMUTABLE_LRU.items() for name in names},
        'mutable_dict_entries':{name:len(getattr(extract,name)) for name in MUTABLE_DICTS},
        'semantic_archive_cache_entries':len(frontend._ARCHIVE_CACHE),
        'r47_stats_entries':len(extract.R47_STATS), 'r49_stats_entries':len(extract.R49_STATS),
        'operation_timing_entries':len(extract.OPERATION_TIMINGS),
        'cpp_writer_process_present':extract._CPP_STREAM_WRITER_SERVER is not None}


def clear(runtime: Path, phase: str):
    """Return counts only; no raw values, program source or log text is emitted."""
    start=time.perf_counter()
    frontend, loaded=modules(runtime)
    extract=loaded['pare_dataset_extract']
    if getattr(extract,'_R54_ONLINE',False):
        raise RuntimeError('Cannot clear state during an active storage operation')
    before=snapshot(runtime)
    for module,names in DATA_LRU.items():
        for name in names:
            getattr(loaded[module],name).cache_clear()
    for name in MUTABLE_DICTS:
        getattr(extract,name).clear()
    # Reset diagnostic counters and any optional old codec-decision state.
    extract.CODEC_DECISION_CACHE_SOURCE=''
    for name in ('CODEC_DECISION_CACHE_HITS','CODEC_DECISION_CACHE_MISSES','CODEC_DECISION_CACHE_SKIPS'):
        setattr(extract,name,0)
    extract.reset_operation_timings()
    extract.R47_STATS.clear()
    extract.R49_STATS.clear()
    extract._R54_PLAN={}
    frontend.reset_archive_cache()
    # The optional server is disabled in this campaign. Closing it at a block
    # boundary nevertheless prevents future configurations retaining request
    # buffers or callable/process state across blocks.
    extract.shutdown_cpp_stream_writer_server()
    # Our four explicitly immutable compiled-plan caches remain available.
    # Stdlib caches can also be reached by generated/projector code, so clear
    # these instead of assuming all their possible keys are fixed constants.
    re.purge()
    strptime=sys.modules.get('_strptime')
    if strptime is not None:
        with strptime._cache_lock:
            strptime._regex_cache.clear()
    after=snapshot(runtime)
    zero = (all(item['currsize']==0 for item in after['data_lru'].values()) and
            all(value==0 for value in after['mutable_dict_entries'].values()) and
            after['semantic_archive_cache_entries']==0 and
            after['r47_stats_entries']==0 and after['r49_stats_entries']==0 and
            after['operation_timing_entries']==0 and not after['cpp_writer_process_present'])
    if not zero:
        raise RuntimeError('Data-derived runtime state survived the explicit boundary')
    return {'version':VERSION,'phase':phase,'before':before,'after':after,
            'all_data_caches_empty':True,'seconds':time.perf_counter()-start}
