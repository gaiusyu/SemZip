#!/usr/bin/env python3
"""In-process checks of the enforcement hooks (no archive, no decode). Unsafe snippets are never executed:
the tests assert that the hook raises BEFORE the frozen compile function (and hence exec) runs."""
import json
import os
import sys
from pathlib import Path

os.environ.setdefault('R76_VALIDATOR_POLICY', 'strict')
sys.path.insert(0, str(Path(__file__).resolve().parent))
import enforced_decode as E  # noqa: E402
import pyexec_validator as V  # noqa: E402

extract, frontend = E.install(E.RUNTIME)
E._assert_installed(extract, frontend)
fails = []


def expect(name, cond):
    if not cond:
        fails.append(name)


# 1. exec guard refuses exec outside a validated compile hook
try:
    extract.exec('r76_probe = 1', {})
    fails.append('exec guard did not raise')
except V.UnsafeGeneratedProgram:
    pass
# 2. compile hook rejects before the frozen compile (no exec happens, frozen cache untouched)
before_exec, before_cache = E.STATE['exec_calls'], len(extract.COMPILED_PYTHON_EXEC_CACHE)
for bad in ["import os\ndef forward(g):\n    return {}\ndef inverse(r):\n    return ''\n",
            "def forward(g):\n    return {'stored': ['{0.__class__}'.format(1)], 'layout': []}\ndef inverse(r):\n    return ''\n",
            "def forward(g):\n    return {'stored': [g[0]._x], 'layout': []}\ndef inverse(r):\n    return ''\n"]:
    try:
        extract.compile_generated_python_exec(bad)
        fails.append('compile hook accepted unsafe code')
    except V.UnsafeGeneratedProgram:
        pass
expect('no exec for rejected code', E.STATE['exec_calls'] == before_exec)
expect('frozen compile cache untouched', len(extract.COMPILED_PYTHON_EXEC_CACHE) == before_cache)
# 3. the runtime's own safety predicate now also fails for such code (it calls the hooked function)
expect('open_function_program refuses', not extract.is_open_function_program_safe(
    {'op': 'python_exec', 'code': "import os\ndef forward(g):\n    return {}\ndef inverse(r):\n    return ''\n"}))
# 4. a safe program compiles through the hook, exactly one exec
E.STATE['rejections'] = []
ok_code = "def forward(groups):\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])"
fwd, inv = extract.compile_generated_python_exec(ok_code)
expect('safe program round-trips', inv(fwd(['abc'])) == 'abc')
expect('one exec for safe code', E.STATE['exec_calls'] == before_exec + 1)
# 5. program_extension (llm_multi_program) resolves the hooked function through the module attribute
import program_extension  # noqa: E402,F401
expect('program_extension reads hooked attribute', extract.read_tag_stream.__code__.co_names.count('compile_generated_python_exec') >= 1)
# 6. (V2) context projector reaching sys through re.enum is refused before compile, also via the runtime predicate
esc_ctx = "probe = re.enum.sys.version\ndef project_context(line, groups):\n    return None\n"
before_exec = E.STATE['exec_calls']
try:
    extract.compile_generated_context_projector(esc_ctx)
    fails.append('context hook accepted re.enum.sys escape')
except V.UnsafeGeneratedProgram:
    pass
expect('open_function_program refuses context escape', not extract.is_open_function_program_safe(
    {'op': 'python_exec', 'code': ok_code, 'context_code': esc_ctx}))
expect('no exec for rejected context code', E.STATE['exec_calls'] == before_exec)
expect('validator is V2', V.VERSION.startswith('R76-PYEXEC-ALLOWLIST-V2'))
extract.COMPILED_PYTHON_EXEC_CACHE.clear()
print(json.dumps({'status': 'PASS' if not fails else 'FAIL', 'failures': fails, 'policy': E.POLICY, 'validator_version': V.VERSION}, indent=1))
sys.exit(1 if fails else 0)
