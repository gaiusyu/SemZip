#!/usr/bin/env python3
"""R76 track E step 4: archive-only decode with the static python_exec validator enforced.

V2 copy (safety/v2/): identical to ../enforced_decode.py except this docstring; it imports the V2
pyexec_validator.py that sits next to it (SAFETY = this directory).

New decode entry (this file). The frozen R71/R73 guard modules (guarded_backend.py,
guarded_backend_v2.py, runtime_cache_boundary.py) and runtime source are imported UNCHANGED from
r73_qg/art (sha-checked at start); nothing in them is edited. Before any worker starts, and again
(idempotently) inside every block job, three fail-closed checks are installed in-process:

  1. PREFLIGHT  semantic_codec_frontend.extract_semantic_archive is wrapped: tar members are checked
                (regular files/dirs only, no absolute path, no '..') BEFORE extraction, and after
                extraction every code string in metadata.json (stream 'program'/'context_tag' JSON,
                llm_multi_program 'program.code', any 'context_code') is validated with
                pyexec_validator. Runs before restore_text, i.e. before any generated code is compiled.
  2. COMPILE HOOK pare_dataset_extract.compile_generated_python_exec / compile_generated_context_projector
                (the only two places that exec generated code; program_extension reaches them through
                the module attribute) validate their argument before delegating to the frozen function.
  3. EXEC GUARD  the name `exec` inside pare_dataset_extract is shadowed by a guard that refuses to run
                unless called from inside a validated compile hook.

Any rejection raises UnsafeGeneratedProgram; the block job re-raises even if a caller swallowed it, so
the decode fails (no fallback, no retry). Policy (strict = pre-registered, template = post-hoc
secondary) comes from R76_VALIDATOR_POLICY so it also reaches spawned workers.

Usage: enforced_decode.py --dataset D --archive DIR --result DIR --workers N --policy strict|template
"""
from __future__ import annotations

import argparse
import builtins
import hashlib
import json
import lzma
import multiprocessing
import os
import sys
import tarfile
import time
from pathlib import Path, PurePosixPath

SAFETY = Path(__file__).resolve().parent
if str(SAFETY) not in sys.path:
    sys.path.insert(0, str(SAFETY))
import pyexec_validator as V  # noqa: E402
from static_check import extract_codes  # noqa: E402

ROOT = Path('<WORKDIR>')
ART = ROOT / 'r73_quality_gate_20260927/r73_qg/art'
FROZEN = ART / 'frozen'
RUNTIME = ART / 'source' / 'runtime'
FROZEN_SHA = {  # r71_strict_main_20260923/PUBLICATION_BINDING.json 'wrappers'
    'guarded_backend.py': '80b91d005d0476b1cfe6b53ea376de7d82d24caa29d4c0feb58460bc0fab32fc',
    'guarded_backend_v2.py': '72bca1ca7c92f797d5a6a11d2300ad0bdee2ae209f972044b0fe3eb86c8b4d01',
    'runtime_cache_boundary.py': 'e2098cb527c652aa0cd4464cffd0a16e7e7ca6ccff2c59474fc812069adfb911',
}
if str(FROZEN) not in sys.path:
    sys.path.insert(0, str(FROZEN))
import guarded_backend_v2 as v2  # noqa: E402  (also patches guarded_backend, exactly as the frozen CLI does)

POLICY = os.environ.get('R76_VALIDATOR_POLICY', 'strict')
if POLICY not in ('strict', 'template'):
    raise SystemExit('R76_VALIDATOR_POLICY must be strict or template')

STATE = {'installed_pid': None, 'validated': {}, 'events': [], 'rejections': [], 'in_compile': 0,
         'exec_calls': 0, 'preflight_blocks': 0, 'tar_members': 0}
_REAL_EXEC = builtins.exec
_ORIG_V2_DECODE_JOB = v2._decode_job


def sha_file(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def _validated(code, kind, where):
    # Hot path (called once per decoded value): keyed by the code string itself (str hash is cached).
    entry = STATE['validated'].get((code, kind)) if isinstance(code, str) else None
    if entry is not None:
        entry['uses'] += 1
        return
    h = V.code_sha256(code) if isinstance(code, str) else None
    try:
        report = V.enforce(code, kind, POLICY)
    except V.UnsafeGeneratedProgram as exc:
        STATE['rejections'].append({'where': where, 'kind': kind, 'code_sha256': h, 'error': str(exc)})
        raise
    STATE['validated'][(code, kind)] = {'code_sha256': h, 'kind': kind, 'first_where': where, 'uses': 1,
                                        'template_wrapped': 'template' in report,
                                        'warnings': sorted({w['rule'] for w in report['warnings']})}


def _tar_member_problem(m):
    p = PurePosixPath(m.name)
    if m.name.startswith('/') or p.is_absolute():
        return 'absolute path'
    if any(part == '..' for part in p.parts):
        return 'parent traversal'
    if not (m.isreg() or m.isdir()):
        return 'non-regular member type %r' % m.type
    return None


class UnsafeArchiveMember(ValueError):
    pass


def install(runtime: Path):
    """Idempotent per process. Loads the frozen runtime and installs the three checks."""
    bounded, frontend = v2._configure(runtime)
    _semzip, extract = frontend._load_semzip_runtime(runtime)
    if getattr(extract, '_r76_installed_pid', None) == os.getpid() and STATE['installed_pid'] == os.getpid():
        return extract, frontend
    if not getattr(extract, '_r76_installed', False):
        orig_py = extract.compile_generated_python_exec
        orig_ctx = extract.compile_generated_context_projector
        orig_extract = frontend.extract_semantic_archive

        def compile_generated_python_exec(code):
            _validated(code, 'python_exec', 'compile_generated_python_exec')
            STATE['in_compile'] += 1
            try:
                return orig_py(code)
            finally:
                STATE['in_compile'] -= 1

        def compile_generated_context_projector(code):
            _validated(code, 'context', 'compile_generated_context_projector')
            STATE['in_compile'] += 1
            try:
                return orig_ctx(code)
            finally:
                STATE['in_compile'] -= 1

        def guarded_exec(obj, globals_=None, locals_=None):
            if STATE['in_compile'] <= 0:
                STATE['rejections'].append({'where': 'exec_guard', 'error': 'exec outside a validated compile hook'})
                raise V.UnsafeGeneratedProgram('R76 exec guard: exec called outside a validated compile hook')
            STATE['exec_calls'] += 1
            return _REAL_EXEC(obj, globals_, locals_)

        def extract_semantic_archive(archive_path, restore_root):
            with lzma.open(archive_path, 'rb') as compressed, tarfile.open(fileobj=compressed, mode='r') as tar:
                members = tar.getmembers()
            for m in members:
                prob = _tar_member_problem(m)
                if prob:
                    STATE['rejections'].append({'where': 'tar_preflight', 'member': m.name, 'error': prob})
                    raise UnsafeArchiveMember('R76 tar preflight: %s: %r' % (prob, m.name))
            STATE['tar_members'] += len(members)
            orig_extract(archive_path, restore_root)
            metadata = json.loads((Path(restore_root) / 'metadata.json').read_text(encoding='utf-8'))
            for jpath, kind, code, _op in extract_codes(metadata):
                _validated(code, kind, 'preflight:' + jpath)
            STATE['preflight_blocks'] += 1

        extract.compile_generated_python_exec = compile_generated_python_exec
        extract.compile_generated_context_projector = compile_generated_context_projector
        extract.exec = guarded_exec  # module-global shadow of the builtin inside pare_dataset_extract
        frontend.extract_semantic_archive = extract_semantic_archive
        extract._r76_installed = True
    extract._r76_installed_pid = os.getpid()
    STATE['installed_pid'] = os.getpid()
    return extract, frontend


def _assert_installed(extract, frontend):
    ok = (extract.compile_generated_python_exec.__name__ == 'compile_generated_python_exec'
          and extract.compile_generated_python_exec.__module__ == __name__
          and extract.compile_generated_context_projector.__module__ == __name__
          and getattr(extract, 'exec', None) is not None and extract.exec.__module__ == __name__
          and frontend.extract_semantic_archive.__module__ == __name__)
    if not ok:
        raise RuntimeError('R76 enforcement hooks are not installed in this worker')


def r76_decode_job(job):
    """Replaces guarded_backend_v2._decode_job (picked up by v2._configure); one original block."""
    runtime = Path(job['runtime'])
    extract, frontend = install(runtime)
    _assert_installed(extract, frontend)
    STATE.update(validated={}, events=[], rejections=[], in_compile=0, exec_calls=0, preflight_blocks=0, tar_members=0)
    started = time.perf_counter()
    log = Path(job['logs']) / ('r76_validator_%05d.json' % job['index'])
    record = {'validator_version': V.VERSION, 'policy': POLICY, 'pid': os.getpid(), 'block_index': job['index']}
    try:
        data = _ORIG_V2_DECODE_JOB(job)
        if STATE['rejections']:
            raise V.UnsafeGeneratedProgram('R76: rejection recorded during block (possibly swallowed): %s'
                                           % STATE['rejections'][0])
        if STATE['preflight_blocks'] != 1:
            raise RuntimeError('R76: semantic archive preflight ran %d times (expected 1)' % STATE['preflight_blocks'])
        record.update(status='PASS', decoded_bytes=len(data), decoded_sha256=hashlib.sha256(data).hexdigest(),
                      decoded_lf=data.count(b'\n'), decoded_records=data.count(b'\n') + int(bool(data) and not data.endswith(b'\n')))
        return data
    except BaseException as exc:
        record.update(status='FAIL', exception=type(exc).__name__, message=str(exc)[:4000])
        raise
    finally:
        record.update(seconds=time.perf_counter() - started, rejections=STATE['rejections'],
                      validated_codes=sorted(STATE['validated'].values(), key=lambda x: (x['kind'], x['code_sha256'] or '')),
                      exec_calls=STATE['exec_calls'], preflight_blocks=STATE['preflight_blocks'],
                      tar_members_checked=STATE['tar_members'])
        log.write_text(json.dumps(record, indent=1) + '\n')


v2._decode_job = r76_decode_job  # v2._configure installs this into run_bounded_semantic for every job


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dataset', required=True)
    ap.add_argument('--archive', type=Path, required=True)
    ap.add_argument('--result', type=Path, required=True)
    ap.add_argument('--workers', type=int, default=3)
    ap.add_argument('--expected', type=Path, required=True, help='independent block inventory (r68 input_<D>.json)')
    a = ap.parse_args()
    if not 1 <= a.workers <= 3:
        ap.error('workers must be 1..3 for this track')
    result = a.result.resolve()
    if result.exists() and any(result.iterdir()):
        raise SystemExit('refusing to reuse non-empty result dir %s' % result)
    result.mkdir(parents=True, exist_ok=True)
    frozen = {name: sha_file(FROZEN / name) for name in FROZEN_SHA}
    if frozen != FROZEN_SHA:
        raise SystemExit('frozen guard sha mismatch: %s' % frozen)
    runtime_sha = {name: sha_file(RUNTIME / name) for name in ('pare_dataset_extract.py', 'semzip_pure.py')}
    runtime_sha.update({name: sha_file(RUNTIME.parent / name) for name in ('program_extension.py', 'execution_environment.py',
                                                                           'strict_plan.py', 'offline_recipe.py')})
    runtime_sha.update({'backend/' + name: sha_file(RUNTIME.parent / 'backend' / name) for name in
                        ('semantic_codec_frontend.py', 'run_bounded_semantic.py', 'decompress')})
    V.load_space_layout_template()  # sha-pinned; fails closed if the frozen template changed
    inventory = json.loads(a.expected.read_text())
    extract, frontend = install(RUNTIME)  # parent: before the pool forks
    _assert_installed(extract, frontend)
    env_start = multiprocessing.get_start_method()
    head = {'track': 'R76-E', 'dataset': a.dataset, 'policy': POLICY, 'validator_version': V.VERSION,
            'validator_sha256': sha_file(V.__file__), 'enforced_decode_sha256': sha_file(__file__),
            'frozen_guard_sha256': frozen, 'runtime_sha256': runtime_sha, 'archive': str(a.archive),
            'workers': a.workers, 'mp_start_method': env_start, 'started_unix': time.time(),
            'expected_inventory': str(a.expected), 'expected_raw_sha256': inventory['raw_sha256']}
    t0 = time.perf_counter()
    try:
        summary = v2.decode(a.archive.resolve(), RUNTIME, result / 'decode', a.workers, None, inventory['blocks'])
        status, err = 'DECODED', None
    except BaseException as exc:  # fail closed: record and exit non-zero
        summary, status, err = None, 'FAIL', '%s: %s' % (type(exc).__name__, str(exc)[:4000])
    head.update(decode_wall_seconds=time.perf_counter() - t0, finished_unix=time.time(), decode_status=status,
                decode_error=err, decode_summary=summary)
    (result / 'decode_result.json').write_text(json.dumps(head, indent=1) + '\n')
    print(json.dumps({k: head[k] for k in ('dataset', 'policy', 'decode_status', 'decode_error', 'decode_wall_seconds')}), flush=True)
    sys.exit(0 if status == 'DECODED' else 3)


if __name__ == '__main__':
    sys.exit('import-only module; use run_verify.py (keeps worker functions picklable by module name)')
