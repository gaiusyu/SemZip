#!/usr/bin/env python3
"""Serialized R71/external formal timing. `check` is read-only; `run` is explicit.

All formal rates use an outer subprocess wall for each encode and decode phase.
Frozen first-pass internal durations are diagnostic and never substituted.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time
import traceback

ROOT = Path(__file__).resolve().parent
SEMZIP = ROOT.parent / 'r71_strict_main_20260923'
RAW = ROOT.parent / 'data/loghub1/raw'
METHODS = ('semzip', 'delog', 'gzip6', 'xz6', 'zstd3', 'loglite')
COHORT = ('Linux', 'HPC', 'OpenSSH', 'Android')
TERMINAL = ('PASS', 'COMPLETE_WITH_FAILURES')
VERSION = 'R68-FORMAL-R71-OUTER-CLI-V3'
WORKERS = 4
REPEATS = 3
BLOCK_RECORDS = 100000


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def load(path):
    return json.loads(Path(path).read_text())


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def save(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.pending.%d' % os.getpid())
    with temporary.open('x') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')
    os.replace(str(temporary), str(path))


def source_paths():
    return sorted(p.absolute() for p in (SEMZIP / 'source').rglob('*')
                  if p.is_file() and '__pycache__' not in p.parts and p.suffix != '.pyc')


def environment():
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(('PARE_LLM_', 'YUNWU_', 'SEMZIP_R53_')) or key in (
                'GZIP', 'XZ_OPT', 'XZ_DEFAULTS', 'ZSTD_CLEVEL', 'ZSTD_NBTHREADS', 'SEMZIP_R54_PLAN'):
            env.pop(key, None)
    env['PATH'] = str(ROOT / 'tools/extracted/usr/bin') + os.pathsep + env.get('PATH', '')
    return env


def snapshot():
    """Low-rate numeric context; no other process command lines or interventions."""
    row = {'unix': time.time(), 'pid': os.getpid(), 'load_average': list(os.getloadavg())}
    if hasattr(os, 'sched_getaffinity'):
        row['affinity_cpu_count'] = len(os.sched_getaffinity(0))
    paths = ('/proc/stat', '/proc/loadavg', '/proc/pressure/cpu',
             '/sys/fs/cgroup/cpu.stat', '/sys/fs/cgroup/cpu.max',
             '/sys/fs/cgroup/cpuacct/cpuacct.usage', '/sys/fs/cgroup/cpu/cpu.stat',
             '/sys/fs/cgroup/memory.current', '/sys/fs/cgroup/memory/memory.usage_in_bytes')
    row['observations'] = {}
    for name in paths:
        try:
            text = Path(name).read_text()
            row['observations'][name] = text.splitlines()[0] if name == '/proc/stat' else text[:4096]
        except OSError:
            pass
    return row


def files_record(root):
    root = Path(root)
    return {str(p.relative_to(root)): {'bytes': p.stat().st_size, 'sha256': sha(p)}
            for p in sorted(root.rglob('*')) if p.is_file()}


def archived_record(data, method, verify=False):
    if method == 'semzip':
        records = data['archive_files']
        root = Path(data['encode']['archive_dir'])
        raw_bytes, raw_sha = data['encode']['raw_bytes'], data['encode']['raw_sha256']
        archive_bytes = data['encode']['archive_bytes']
    else:
        root = Path(data['attempt']) / 'archives'
        records = {str(Path(b['archive']).relative_to('archives')):
                   {'bytes': b['archive_bytes'], 'sha256': b['archive_sha256']} for b in data['blocks']}
        raw_bytes, raw_sha, archive_bytes = data['raw_bytes'], data['raw_sha256'], data['archive_bytes']
    require(sum(v['bytes'] for v in records.values()) == archive_bytes, 'archive size ledger mismatch')
    if verify:
        require(files_record(root) == records, 'first-pass archive changed: ' + str(root))
    return {'archive_bytes': archive_bytes, 'archive_digest': digest(records),
            'archive_files': records, 'raw_bytes': raw_bytes, 'raw_sha256': raw_sha}


def check_lock(lock):
    changes = []
    actual_files = {}
    for filename, expected in lock['files'].items():
        path = Path(filename)
        actual_files[filename] = sha(path) if path.is_file() else None
        if actual_files[filename] != expected:
            changes.append(filename)
    if [str(p) for p in source_paths()] != lock['source_inventory']:
        changes.append('R71 source inventory changed')
    require(not changes, 'Locked campaign inputs changed: ' + repr(changes))
    return {'files':actual_files, 'source_inventory':[str(p) for p in source_paths()], 'verified_unix':time.time()}


def indexed(rows, fields):
    answer = {}
    for row in rows:
        key = tuple(row[name] for name in fields)
        require(key not in answer, 'duplicate result cell: ' + repr(key))
        answer[key] = row
    return answer


def prepare(args):
    """Complete gate and identity checks; no writes and no encoding/decoding."""
    required = [ROOT/'first_pass/status.json', ROOT/'first_pass/results.json', ROOT/'first_pass/manifest.json',
                ROOT/'loglite_first_pass/status.json', ROOT/'loglite_first_pass/results.json',
                ROOT/'loglite_first_pass/manifest.json', SEMZIP/'main_status.json',
                SEMZIP/'training_status.json', SEMZIP/'reference.json', SEMZIP/'MAIN_DESIGN.json',
                SEMZIP/'GUARD_APPROVED.json', ROOT/'AUXILIARY_READY.json',
                args.representation_dir/'status.json', args.representation_dir/'results.json',
                args.representation_dir/'DESIGN.json', args.representation_script]
    required += [ROOT/'codec_baseline.py', ROOT/'loglite_adapter.py', Path(__file__).resolve(),
                 SEMZIP/'main_campaign.py', SEMZIP/'guarded_backend_v2.py',
                 SEMZIP/'guarded_backend.py', SEMZIP/'runtime_cache_boundary.py']
    require(all(p.is_file() for p in required), 'Missing prerequisites: ' + repr([str(p) for p in required if not p.is_file()]))
    for folder in (ROOT/'first_pass', ROOT/'loglite_first_pass', args.representation_dir):
        require(load(folder/'status.json')['status'] in TERMINAL, 'unfinished campaign: ' + str(folder))
    main = load(SEMZIP/'main_status.json')
    require(main['status'] in TERMINAL, 'R71 main unfinished')
    training = load(SEMZIP/'training_status.json')
    require(training['status'] == 'COMPLETE', 'R71 training unfinished')
    require(load(ROOT/'AUXILIARY_READY.json')['status'] == 'PASS', 'auxiliary release gate not PASS')
    approved = load(SEMZIP/'GUARD_APPROVED.json')
    require(approved['status'] == 'PASS', 'R71 guard release gate not PASS')
    for name, expected in approved['frozen_wrappers'].items():
        require(sha(SEMZIP/name) == expected, 'approved wrapper drift: ' + name)
    for name, expected in approved['actual_test_evidence'].items():
        require(sha(name) == expected, 'approved guard evidence drift: ' + name)
        required.append(Path(name))
    representation = load(args.representation_dir/'results.json')
    require(set(indexed(representation, ('dataset',))) == {(d,) for d in COHORT}, 'R70 v2 must retain all four outcomes')
    require(all(r['status'] in ('PASS', 'FAIL', 'FAILED') for r in representation), 'R70 v2 unfinished outcome')
    rep_design = load(args.representation_dir/'DESIGN.json')
    require(rep_design['script_sha256'] == sha(args.representation_script), 'R70 v2 script differs from executed design')
    require(rep_design['version'] == 'R70-REPRESENTATION-CONTROL-V2-STRICT-20260923', 'wrong representation-control version')
    rep_status = load(args.representation_dir/'status.json')
    require(rep_status['cache_boundary_sha256'] == sha(SEMZIP/'runtime_cache_boundary.py') == rep_design['cache_boundary_sha256'], 'R70/R71 cache-boundary versions differ')
    required.append(args.representation_script.parent/'runtime_cache_boundary.py')
    require(sha(required[-1]) == rep_design['cache_boundary_sha256'], 'R70 executed cache helper changed')
    require(load(args.representation_dir/'status.json').get('source_unchanged') is True, 'R70 v2 source changed')
    references = load(SEMZIP/'reference.json')
    names = [r['dataset'] for r in references]
    require(len(names) == 16 and len(set(names)) == 16, 'expected original sixteen-file reference')
    require(set(indexed(training['completed'], ('dataset',))) == {(d,) for d in names}, 'incomplete R71 training ledger')
    cap = next(int(r['raw_bytes']) for r in references if r['dataset'] == 'BGL')
    datasets = [r['dataset'] for r in references if int(r['raw_bytes']) <= cap]
    require(len(datasets) == 12 and 'BGL' in datasets, 'expected twelve files no larger than BGL')
    semrows = indexed(main['completed'], ('dataset',))
    require(set(semrows) == {(d,) for d in names}, 'R71 must retain all sixteen first-pass outcomes')
    require(all(r['status'] in ('PASS', 'FAIL', 'FAILED') for r in semrows.values()), 'unfinished R71 first-pass row')
    baselines = {}
    specs = {}
    for folder, methods in ((ROOT/'first_pass', METHODS[1:5]), (ROOT/'loglite_first_pass', ('loglite',))):
        result = load(folder/'results.json')['runs']
        cells = indexed(result, ('dataset', 'codec'))
        require(set(cells) == {(d,m) for d in names for m in methods}, 'incomplete baseline cells: ' + str(folder))
        require(all(r['status'] in ('PASS', 'FAIL', 'FAILED') for r in result), 'unfinished baseline record')
        manifest = load(folder/'manifest.json')['protocol']
        require(manifest['workers'] == WORKERS and manifest['block_records'] == BLOCK_RECORDS and manifest['trials'] == 1, 'baseline protocol changed')
        require(manifest['script_sha256'] == sha(ROOT/'codec_baseline.py'), 'baseline source changed since first pass')
        specs.update(manifest['codecs'])
        baselines.update(cells)
    native_keys = (('executable','executable_sha256'), ('encoder','encoder_sha256'),
                   ('decoder','decoder_sha256'), ('xz_executable','xz_sha256'))
    for method, spec in specs.items():
        for pathkey, hashkey in native_keys:
            if pathkey in spec:
                exe = Path(spec[pathkey])
                require(sha(exe) == spec[hashkey], 'baseline executable drift: ' + str(exe))
                required.append(exe)
        if 'adapter_sha256' in spec:
            require(spec['adapter_sha256'] == sha(ROOT/'loglite_adapter.py'), 'LogLite adapter drift')
    main_design = load(SEMZIP/'MAIN_DESIGN.json')
    for key, filename in (('guard_sha256','guarded_backend_v2.py'), ('base_guard_sha256','guarded_backend.py'),
                          ('cache_boundary_sha256','runtime_cache_boundary.py'), ('campaign_sha256','main_campaign.py')):
        require(main_design[key] == sha(SEMZIP/filename), 'R71 executed source mismatch: ' + key)
    sources = source_paths()
    require({str(p.relative_to((SEMZIP/'source').absolute())):sha(p) for p in sources} == main_design['source_hashes'], 'R71 source tree drift')
    required += sources
    publications = {}
    for dataset in names:
        path = SEMZIP/'training'/dataset/'publication.json'
        publication = load(path)
        for key, hashkey in (('deployed_plan','deployed_plan_sha256'), ('storage','storage_sha256')):
            actual = Path(publication[key])
            require(sha(actual) == publication[hashkey], 'publication payload drift: ' + dataset + '/' + key)
            required.append(actual)
        for key in ('original_training', 'original_publication', 'corrected_fit'):
            if key in publication:
                require(sha(publication[key]) == publication[key+'_sha256'], 'publication provenance drift: ' + key)
                required.append(Path(publication[key]))
        publications[dataset] = publication
        required.append(path)
    first = {}
    for dataset in datasets:
        ref = next(r for r in references if r['dataset'] == dataset)
        for method in METHODS:
            row = semrows[(dataset,)] if method == 'semzip' else baselines[(dataset,method)]
            item = {'status': row['status']}
            if row['status'] == 'PASS':
                if method == 'semzip':
                    path = SEMZIP/'results'/dataset/'first_pass/result.json'
                    row = load(path)
                    required.append(path)
                    require(row['publication'] == publications[dataset], 'first-pass publication differs')
                item.update(archived_record(row, method, verify=True))
                require(item['raw_sha256'] == ref['raw_sha256'] and item['raw_bytes'] == int(ref['raw_bytes']), 'first-pass raw identity mismatch')
            else:
                item['first_pass_failure'] = row
            first[dataset + '/' + method] = item
    require(args.resource_evidence, 'supply at least one completed resource-context evidence file')
    for path in args.resource_evidence:
        require(path.is_file(), 'resource evidence missing: ' + str(path))
        required.append(path)
    required.append(Path(sys.executable).absolute())
    lock = {'files': {str(p.absolute()):sha(p) for p in sorted(set(required))},
            'source_inventory': [str(p) for p in sources]}
    schedule = make_schedule(datasets, first)
    return {'version': VERSION, 'workers':WORKERS, 'block_records':BLOCK_RECORDS, 'repeats':REPEATS,
            'datasets':datasets, 'methods':METHODS, 'schedule':schedule, 'references':references,
            'codec_specs':specs, 'publications':publications, 'first_pass':first, 'lock':lock,
            'lock_digest':digest(lock), 'resource_evidence':[str(p.resolve()) for p in args.resource_evidence],
            'representation_dir':str(args.representation_dir.resolve()),
            'timing_scope':'Parent subprocess launch through exit for separate encode/decode CLIs; includes imports, pools, block CLI startup, input split/read, output files, required guard/reset and implementation-native hashes, cleanup and summary serialization. Independent inventory/archive hashes outside these timers. Controller imports excluded; buffered I/O, no fsync/cache drop.',
            'restored_output_audit':'Full and per-block SHA/bytes must match the original inventory; size/device/inode/path are stable across the audit. A newly closed output mtime may settle late and is recorded without failing byte-exact reconstruction. Input identity checks remain strict. No rescans or retries.',
            'supersedes_instrumentation_campaign':'formal_v2; all prior observations retained separately, none pooled into this 216-trial campaign',
            'old_timing_scope':'First-pass inner timers differ; no first-pass duration substitutes for a formal speed observation.',
            'failure_policy':'Three scheduled attempts for each first-pass-valid cell; never retry or replace observations. Known first-pass failures retain all three NOT_TIMED rows. New failures and hash inconsistencies remain visible.',
            'host_limit':'Serialized owned methods on a shared host; boundary resource samples and cited prior evidence do not prove isolation or absence of short-lived contention.'}


def make_schedule(datasets, first):
    schedule = []
    for repeat in range(1, REPEATS+1):
        for index, dataset in enumerate(datasets):
            offset = (repeat-1+index) % len(METHODS)
            for method in METHODS[offset:] + METHODS[:offset]:
                schedule.append({'dataset':dataset, 'method':method, 'repeat':repeat,
                                 'eligible':first[dataset+'/'+method]['status'] == 'PASS'})
    return schedule


def inventory_decoded_output(source):
    """Content audit after the decoder has exited; only mtime may settle late.

    The shared filesystem can revise a newly closed file's mtime during its
    first read. Preserve both observations. Size, device, inode and path must
    stay fixed, and the caller still requires exact original full/block hashes.
    No wait, rescan, fsync, codec retry or timed-phase change is introduced.
    """
    import codec_baseline as cb
    before = cb.file_identity(source)
    full, block_hash = hashlib.sha256(), [hashlib.sha256()]
    blocks = []
    def segment(piece):
        full.update(piece); block_hash[0].update(piece)
    def end(meta):
        meta['raw_sha256'] = block_hash[0].hexdigest()
        blocks.append(meta); block_hash[0] = hashlib.sha256()
    cb.scan(source, segment, end)
    after = cb.file_identity(source)
    changes = [key for key in before if before[key] != after[key]]
    require(set(changes) <= {'mtime_ns'}, 'decoded output identity changed beyond mtime')
    size = sum(row['raw_bytes'] for row in blocks)
    require(size == before['bytes'] == after['bytes'], 'decoded output size changed during audit')
    return {'source':before, 'raw_sha256':full.hexdigest(), 'blocks':blocks,
            'raw_bytes':size, 'records':sum(row['records'] for row in blocks),
            'metadata_before':before, 'metadata_after':after, 'metadata_changed_fields':changes}


def codec_phase(phase, request_path):
    """Fresh process; consume pinned settings without timing binary hash/version audits."""
    import codec_baseline as cb
    request = load(request_path)
    codec = cb.Codec.__new__(cb.Codec)
    codec.name, codec.spec = request['method'], request['codec_spec']
    spec = codec.spec
    codec.encoder = Path(spec.get('encoder', spec.get('executable', '')))
    codec.decoder = Path(spec.get('decoder', spec.get('executable', '')))
    if codec.name == 'loglite':
        import loglite_adapter
        codec.adapter, codec.xz = loglite_adapter, spec['xz_executable']
    attempt = Path(request['attempt'])
    if phase == 'encode':
        inv = request['inventory']
        blocks, seconds = cb.encode_all(inv['source']['path'], inv, codec, request['dataset'], attempt, WORKERS, 3600)
        save(attempt/'encode_phase.json', {'blocks':blocks, 'inner_encode_seconds':seconds})
    else:
        encoded = load(attempt/'encode_phase.json')
        restored, seconds = cb.decode_all({}, codec, request['dataset'], attempt, encoded['blocks'], WORKERS, 3600)
        save(attempt/'decode_phase.json', {'restored':str(restored), 'inner_decode_seconds':seconds})


def timed_process(command, log, env):
    resource_before = snapshot()
    with Path(log).open('x') as stream:
        started_unix = time.time()
        start = time.perf_counter()
        process = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, env=env, check=False)
        seconds = time.perf_counter() - start
        finished_unix = time.time()
    return {'started_unix':started_unix, 'finished_unix':finished_unix,
            'resource_before':resource_before, 'resource_after':snapshot(), 'command':command, 'seconds':seconds, 'returncode':process.returncode, 'log':str(log)}


def trial(config_path, index, destination):
    import codec_baseline as cb
    config, destination = load(config_path), Path(destination)
    item = config['schedule'][index]
    dataset, method = item['dataset'], item['method']
    destination.mkdir(exist_ok=False)
    result = dict(item, status='RUNNING', started_unix=time.time(), resource_before=snapshot())
    save(destination/'result.json', result)
    try:
        check_lock(config['lock'])
        inventory = cb.inventory(RAW/(dataset+'.log'))
        reference = next(r for r in config['references'] if r['dataset'] == dataset)
        require(inventory['raw_sha256'] == reference['raw_sha256'] and inventory['raw_bytes'] == int(reference['raw_bytes']), 'formal input identity differs')
        require(len(inventory['blocks']) == int(reference['blocks']), 'formal original block count differs')
        save(destination/'input_inventory.json', inventory)
        env = environment()
        attempt = destination/'work'
        publication = config['publications'][dataset]
        if method == 'semzip':
            env['SEMZIP_R54_PLAN'] = publication['storage']
            encode_cmd = [sys.executable,str(SEMZIP/'guarded_backend_v2.py'),'encode',
                          '--input',str(RAW/(dataset+'.log')), '--plan',publication['deployed_plan'],
                          '--dataset',dataset,'--semzip-source',str(SEMZIP/'source/runtime'),
                          '--result',str(attempt),'--block-size','100000','--workers','4']
        else:
            attempt.mkdir()
            request = {'method':method,'codec_spec':config['codec_specs'][method],
                       'dataset':dataset,'attempt':str(attempt),'inventory':inventory}
            save(destination/'encode_request.json', request)
            encode_cmd = [sys.executable,str(Path(__file__).resolve()),'codec-phase','encode',str(destination/'encode_request.json')]
        result['encode_process'] = timed_process(encode_cmd, destination/'encode.log', env)
        save(destination/'result.json', result)
        require(result['encode_process']['returncode'] == 0, 'encoder process failed')
        if method == 'semzip':
            encoded = load(attempt/'encode_summary.json')
            archive = Path(encoded['archive_dir'])
            restored = destination/'restored.owned.log'
            env['SEMZIP_R54_PLAN'] = str(destination/'unavailable-external-policy.json')
            decode_cmd = [sys.executable,str(SEMZIP/'guarded_backend_v2.py'),'decode',
                          '--archive',str(archive),'--output',str(restored),
                          '--semzip-source',str(SEMZIP/'source/runtime'),'--result',str(destination/'decode'),
                          '--workers','4']
        else:
            # This request contains no input path or raw inventory. Decoder uses only archives/settings.
            save(destination/'decode_request.json', {'method':method,'codec_spec':config['codec_specs'][method],
                                                     'dataset':dataset,'attempt':str(attempt)})
            decode_cmd = [sys.executable,str(Path(__file__).resolve()),'codec-phase','decode',str(destination/'decode_request.json')]
            archive, restored = attempt/'archives', attempt/'roundtrip.owned.log'
        result['decode_process'] = timed_process(decode_cmd, destination/'decode.log', env)
        save(destination/'result.json', result)
        require(result['decode_process']['returncode'] == 0, 'decoder process failed')
        audit_start = time.perf_counter()
        recovered = inventory_decoded_output(restored)
        require(recovered['raw_sha256'] == inventory['raw_sha256'] and recovered['blocks'] == inventory['blocks'], 'independent full-file/block reconstruction failed')
        require(cb.file_identity(RAW/(dataset+'.log')) == inventory['source'], 'source identity changed during trial')
        records = files_record(archive)
        archive_bytes = sum(r['bytes'] for r in records.values())
        if method == 'semzip':
            require(archive_bytes == encoded['archive_bytes'], 'SemZip archive size differs from encoder ledger')
            result['implementation_diagnostics'] = {'inner_encode_seconds':encoded['online_compression_seconds'],
                'inner_decode_seconds':load(destination/'decode/summary.json')['decode_seconds'],
                'semantic_fallback_blocks':encoded['semantic_fallback_blocks'], 'guard_version':encoded['guard_version'],
                'cache_boundary_version':encoded['cache_boundary_version']}
        else:
            result['implementation_diagnostics'] = {'inner_encode_seconds':load(attempt/'encode_phase.json')['inner_encode_seconds'],
                'inner_decode_seconds':load(attempt/'decode_phase.json')['inner_decode_seconds']}
        check_lock(config['lock'])
        result.update(status='PASS', raw_bytes=inventory['raw_bytes'], raw_sha256=inventory['raw_sha256'],
                      decoded_sha256=recovered['raw_sha256'], block_audit=recovered['blocks'],
                      restored_output_metadata_audit={k:recovered[k] for k in ('metadata_before','metadata_after','metadata_changed_fields')},
                      archive_bytes=archive_bytes, archive_dir=str(archive), archive_files=records, archive_digest=digest(records),
                      archive_only_decode=True, audit_seconds=time.perf_counter()-audit_start,
                      encode_MB_per_s=inventory['raw_bytes']/result['encode_process']['seconds']/1e6,
                      decode_MB_per_s=inventory['raw_bytes']/result['decode_process']['seconds']/1e6,
                      publication_sha256=sha(SEMZIP/'training'/dataset/'publication.json') if method == 'semzip' else None)
        restored.unlink()  # Only this trial's independently checked materialized output.
    except Exception:
        result.update(status='FAILED', error=traceback.format_exc())
    result.update(finished_unix=time.time(), resource_after=snapshot())
    save(destination/'result.json', result)
    return 0 if result['status'] == 'PASS' else 1


def summarize(config, completed):
    cells = []
    for dataset in config['datasets']:
        for method in METHODS:
            observations = [r for r in completed if r['dataset'] == dataset and r['method'] == method]
            passes = [r for r in observations if r['status'] == 'PASS']
            first = config['first_pass'][dataset+'/'+method]
            byte_values = [r['archive_bytes'] for r in passes]
            digests = [r['archive_digest'] for r in passes]
            valid_three = len(observations) == REPEATS and len(passes) == REPEATS
            consistent = valid_three and len(set(digests)) == 1 and len(set(byte_values)) == 1
            first_agrees = bool(passes) and all(r['archive_digest'] == first.get('archive_digest') for r in passes)
            row = {'dataset':dataset,'method':method,'expected_observations':REPEATS,
                   'statuses':[r['status'] for r in observations], 'successful_observations':len(passes),
                   'archive_bytes':byte_values, 'archive_digests':digests,
                   'three_repeat_archives_identical':consistent, 'all_successful_archives_equal_first_pass':first_agrees,
                   'status':'PASS' if consistent and first_agrees else ('NOT_TIMED_FIRST_PASS_FAILED' if first['status'] != 'PASS' else 'INCOMPLETE_OR_INCONSISTENT')}
            for metric in ('encode_MB_per_s','decode_MB_per_s'):
                values = [r[metric] for r in passes]
                row[metric] = {'observations':values,'median':statistics.median(values) if consistent and first_agrees else None,
                               'min':min(values) if values else None,'max':max(values) if values else None}
            cells.append(row)
    return {'version':VERSION,'cells':cells,'all_planned_rows_retained':len(completed) == len(config['schedule']),
            'timing_scope':config['timing_scope'],'no_replacement_trials':True,
            'note':'No median is published for fewer than three valid identical-archive repeats, or if the archive differs from the accepted first pass. All observations and errors remain in status.json and per-trial result.json.'}


def run(args):
    config = prepare(args)
    require(not args.output.exists(), 'refusing existing output: ' + str(args.output))
    require(args.output.parent.is_dir(), 'output parent directory must already exist')
    lock_path = ROOT/'formal_v3.RUNNING.lock'
    with lock_path.open('x') as stream:
        json.dump({'pid':os.getpid(),'output':str(args.output.resolve()),'started_unix':time.time()}, stream)
    try:
        args.output.mkdir()
    except BaseException:
        lock_path.unlink()
        raise
    state = {'status':'RUNNING','started_unix':time.time(),'completed':[], 'resource_before':snapshot()}
    try:
        save(args.output/'DESIGN.json', config)
        save(args.output/'lock_before.json', config['lock'])
        config_path = (args.output/'DESIGN.json').resolve()
        config_sha = sha(config_path)
        state['config_sha256'] = config_sha
        save(args.output/'status.json',state)
        for index, item in enumerate(config['schedule']):
            state['current'] = dict(item,index=index)
            save(args.output/'status.json',state)
            check_lock(config['lock'])
            require(sha(config_path) == config_sha, 'formal config changed')
            if not item['eligible']:
                result = dict(item,status='NOT_TIMED_FIRST_PASS_FAILED',first_pass=config['first_pass'][item['dataset']+'/'+item['method']])
            else:
                name = '%03d_%s_%s_r%d' % (index,item['dataset'],item['method'],item['repeat'])
                destination = (args.output/name).resolve()
                command = [sys.executable,'-u',str(Path(__file__).resolve()),'trial',str(config_path),str(index),str(destination)]
                with (args.output/(name+'.log')).open('x') as stream:
                    process = subprocess.run(command,stdout=stream,stderr=subprocess.STDOUT,env=environment(),check=False)
                if (destination/'result.json').exists():
                    result = load(destination/'result.json')
                    if result['status'] == 'RUNNING':
                        result.update(status='FAILED_PROCESS_INTERRUPTED',error='trial did not write a terminal result')
                else:
                    result = dict(item,status='FAILED_NO_RESULT',error='trial process produced no result')
                result.update(trial_process_returncode=process.returncode,result_path=str(destination/'result.json'))
                if process.returncode != 0 and result['status'] == 'PASS':
                    result['status'] = 'FAILED_PROCESS_EXIT'
            state['completed'].append(result)
            save(args.output/'status.json',state)
            save(args.output/'summary.json',summarize(config,state['completed']))
            print(json.dumps({k:result.get(k) for k in ('dataset','method','repeat','status')}),flush=True)
        check_lock(config['lock'])
        require(sha(config_path) == config_sha, 'formal config changed')
        # Re-read successful archives after all timed observations, never inside a phase timer.
        archive_changes = []
        for row in state['completed']:
            if row['status'] == 'PASS' and files_record(row['archive_dir']) != row['archive_files']:
                row['status'] = 'FAILED_POST_CAMPAIGN_ARCHIVE_AUDIT'
                archive_changes.append(row['result_path'])
        state['post_campaign_archive_audit'] = {'status':'PASS' if not archive_changes else 'FAIL',
                                                'changed_results':archive_changes}
        save(args.output/'lock_after.json',check_lock(config['lock']))
        require(sha(config_path) == config_sha, 'formal config changed during final archive audit')
        summary = summarize(config,state['completed'])
        save(args.output/'summary.json',summary)
        state['status'] = 'PASS' if all(r['status']=='PASS' for r in summary['cells']) else 'COMPLETE_WITH_FAILURES'
        state['hash_lock_unchanged'] = True
    except BaseException:
        state.update(status='ABORTED',error=traceback.format_exc(),hash_lock_unchanged=False)
        for item in config['schedule'][len(state['completed']):]:
            state['completed'].append(dict(item,status='NOT_RUN_CAMPAIGN_ABORTED'))
        save(args.output/'summary.json',summarize(config,state['completed']))
        raise
    finally:
        state.update(finished_unix=time.time(),resource_after=snapshot())
        save(args.output/'status.json',state)
        lock_path.unlink()  # Only this process's exclusive coordination file.
    return 0 if state['status'] == 'PASS' else 1


def main():
    if len(sys.argv)>1 and sys.argv[1]=='codec-phase':
        codec_phase(sys.argv[2],Path(sys.argv[3])); return 0
    if len(sys.argv)>1 and sys.argv[1]=='trial':
        return trial(Path(sys.argv[2]),int(sys.argv[3]),Path(sys.argv[4]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('check','run'))
    parser.add_argument('--representation-dir',type=Path,default=ROOT.parent/'r70_representation_control_20260923/results_v2_strict')
    parser.add_argument('--representation-script',type=Path,default=ROOT.parent/'r70_representation_control_20260923/representation_control_v2.py')
    parser.add_argument('--resource-evidence',type=Path,action='append',default=[])
    parser.add_argument('--output',type=Path,default=ROOT/'formal_v3')
    args = parser.parse_args()
    if args.action=='check':
        try:
            config=prepare(args)
            print(json.dumps({'status':'READY','version':VERSION,'datasets':config['datasets'],
                              'scheduled_rows':len(config['schedule']),'lock_digest':config['lock_digest'],
                              'resource_evidence':config['resource_evidence']},indent=2))
            return 0
        except Exception as exc:
            print(json.dumps({'status':'NOT_READY','error':str(exc)},indent=2)); return 2
    return run(args)


if __name__=='__main__':
    raise SystemExit(main())
