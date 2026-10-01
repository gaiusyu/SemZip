#!/usr/bin/env python3
"""Unit tests for pyexec_validator (adversarial snippets are only parsed, never run)."""
import json
import sys
import pyexec_validator as v

OK_BODY = "\ndef forward(groups):\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n"


def rules(code, kind='python_exec'):
    r = v.check(code, kind)
    return {e['rule'] for e in r['errors']}, {w['rule'] for w in r['warnings']}, r['ok']


CASES = [
    # (label, code, kind, expected error rules subset, expected ok, expected warning subset)
    ('raw identity', OK_BODY, 'python_exec', set(), True, set()),
    ('datetime program', "def forward(groups):\n    dt = datetime.strptime(groups[0], '%Y')\n    return {'stored': [int(dt.timestamp())], 'layout': []}\n\ndef inverse(record):\n    return datetime.fromtimestamp(int(record['stored'][0])).strftime('%Y')\n", 'python_exec', set(), True, set()),
    ('import', 'import os' + OK_BODY, 'python_exec', {'E_IMPORT'}, False, set()),
    ('from import', 'from os import system' + OK_BODY, 'python_exec', {'E_IMPORT'}, False, set()),
    ('dunder import', "x = __import__('os')" + OK_BODY, 'python_exec', {'E_UNDERSCORE', 'E_FORBIDDEN_NAME'}, False, set()),
    ('dunder attr', "x = ().__class__" + OK_BODY, 'python_exec', {'E_UNDERSCORE'}, False, set()),
    ('single underscore name', "_x = 1" + OK_BODY, 'python_exec', {'E_UNDERSCORE'}, False, set()),
    ('single underscore def', "def _helper(a):\n    return a\n" + OK_BODY, 'python_exec', {'E_UNDERSCORE'}, False, set()),
    ('single underscore attr', "x = datetime._y" + OK_BODY, 'python_exec', {'E_UNDERSCORE'}, False, set()),
    ('generator frame', "x = (a for a in [1]).gi_frame.f_back" + OK_BODY, 'python_exec', {'E_GENERATOR', 'E_INTROSPECTION'}, False, set()),
    ('eval', "x = eval('1')" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('exec', "exec('x=1')" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, {'W_TOPLEVEL_STMT'}),
    ('compile', "c = compile('1','f','eval')" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('open', "f = open('/etc/passwd')" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('getattr', "x = getattr(1, 'real')" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('setattr ref', "f = setattr" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('globals', "g = globals()" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('vars', "g = vars()" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('breakpoint', "breakpoint()" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('memoryview', "m = memoryview(b'')" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('input', "m = input()" + OK_BODY, 'python_exec', {'E_FORBIDDEN_NAME'}, False, set()),
    ('global stmt', "def forward(groups):\n    global x\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', {'E_SCOPE'}, False, set()),
    ('nonlocal stmt', "def forward(groups):\n    y = 1\n    def g():\n        nonlocal y\n        return y\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', {'E_SCOPE'}, False, set()),
    ('class', "class A:\n    pass\n" + OK_BODY, 'python_exec', {'E_CLASS'}, False, set()),
    ('lambda', "f = lambda a: a" + OK_BODY, 'python_exec', {'E_LAMBDA'}, False, set()),
    ('listcomp', "x = [a for a in [1]]" + OK_BODY, 'python_exec', {'E_COMPREHENSION'}, False, set()),
    ('try', "try:\n    x = 1\nexcept Exception:\n    x = 2\n" + OK_BODY, 'python_exec', {'E_EXCEPT'}, False, set()),
    ('with', "with x:\n    pass\n" + OK_BODY, 'python_exec', {'E_WITH'}, False, set()),
    ('format traversal', "s = '{0.__class__}'.format(1)" + OK_BODY, 'python_exec', {'E_FORMAT'}, False, set()),
    ('format item', "s = '{0[x]}'.format(1)" + OK_BODY, 'python_exec', {'E_FORMAT'}, False, set()),
    ('dynamic format', "def forward(groups):\n    return {'stored': [groups[0].format(1)], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', {'E_FORMAT'}, False, set()),
    ('format ok', "s = '{:02d}-{}'.format(1, 2)" + OK_BODY, 'python_exec', set(), True, set()),
    ('format_map', "s = 'a'.format_map({})" + OK_BODY, 'python_exec', {'E_FORMAT'}, False, set()),
    ('mro', "m = int.mro()" + OK_BODY, 'python_exec', {'E_INTROSPECTION'}, False, set()),
    ('free name type', "t = type(1)" + OK_BODY, 'python_exec', {'E_FREE_NAME'}, False, set()),
    ('free name list', "t = list()" + OK_BODY, 'python_exec', {'E_FREE_NAME'}, False, set()),
    ('re only in context', "x = re.compile('a')" + OK_BODY, 'python_exec', {'E_FREE_NAME'}, False, set()),
    ('context ok', "def project_context(line, groups):\n    m = re.search('a', line)\n    return None\n", 'context', set(), True, set()),
    ('decorator', "@min\ndef helper(a):\n    return a\n" + OK_BODY, 'python_exec', {'E_DECORATOR'}, False, set()),
    ('starargs', "def forward(*groups):\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', {'E_STAR'}, False, set()),
    ('starcall', "x = max(*[1, 2])" + OK_BODY, 'python_exec', {'E_STAR'}, False, set()),
    ('missing inverse', "def forward(groups):\n    return {'stored': [groups[0]], 'layout': []}\n", 'python_exec', {'E_ENTRYPOINT'}, False, set()),
    ('syntax', "def forward(:\n", 'python_exec', {'E_SYNTAX'}, False, set()),
    ('while true no break (report only)', "def forward(groups):\n    while True:\n        pass\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', set(), True, {'W_WHILE', 'W_WHILE_TRUE_NO_BREAK'}),
    ('while true with break', "def forward(groups):\n    while True:\n        break\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', set(), True, {'W_WHILE'}),
    ('while true inner break only', "def forward(groups):\n    while True:\n        for a in [1]:\n            break\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', set(), True, {'W_WHILE_TRUE_NO_BREAK'}),
    ('nondeterministic', "def forward(groups):\n    x = datetime.now()\n    return {'stored': [groups[0]], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', set(), True, {'W_NONDETERMINISTIC'}),
    # --- V2 additions -------------------------------------------------------------------------
    ('V2 context re.enum.sys escape', "def project_context(line, groups):\n    m = re.enum.sys.modules\n    return None\n", 'context', {'E_RE_USE', 'E_FORBIDDEN_ATTR'}, False, set()),
    ('V2 context toplevel escape', "x = re.enum.sys.modules['os']\ndef project_context(line, groups):\n    return None\n", 'context', {'E_RE_USE', 'E_FORBIDDEN_ATTR'}, False, set()),
    ('V2 context re alias', "def project_context(line, groups):\n    r = re\n    return None\n", 'context', {'E_RE_USE'}, False, set()),
    ('V2 context re.compile', "def project_context(line, groups):\n    p = re.compile('a')\n    return None\n", 'context', {'E_RE_USE', 'E_FORBIDDEN_ATTR'}, False, set()),
    ('V2 context non-literal pattern', "def project_context(line, groups):\n    m = re.search(groups[0], line)\n    return None\n", 'context', {'E_RE_USE'}, False, set()),
    ('V2 context re as argument', "def project_context(line, re):\n    return None\n", 'context', {'E_RE_USE'}, False, set()),
    ('V2 context re rebound', "re = 1\ndef project_context(line, groups):\n    return None\n", 'context', {'E_RE_USE'}, False, set()),
    ('V2 context search ok', "def project_context(line, groups):\n    m = re.search(r'route\\s+=\\s+(0x\\w+)', line)\n    return m.group(1) if m else None\n", 'context', set(), True, set()),
    ('V2 context fullmatch ok', "def project_context(line, groups):\n    m = re.fullmatch('a', line)\n    return None\n", 'context', set(), True, set()),
    ('V2 attr sys', "x = datetime.sys" + OK_BODY, 'python_exec', {'E_FORBIDDEN_ATTR'}, False, set()),
    ('V2 attr eval', "def forward(groups):\n    return {'stored': [groups.eval], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])\n", 'python_exec', {'E_FORBIDDEN_ATTR'}, False, set()),
    ('V2 nested format field', "s = '{0:{1.__class__}}'.format(1, 2)" + OK_BODY, 'python_exec', {'E_FORMAT'}, False, set()),
    ('V2 nested format ok', "s = '{0:{1}}'.format(1, 2)" + OK_BODY, 'python_exec', set(), True, set()),
]


def main():
    failures = []
    for label, code, kind, want_err, want_ok, want_warn in CASES:
        got_err, got_warn, ok = rules(code, kind)
        if ok != want_ok or not want_err <= got_err or not want_warn <= got_warn or (want_ok and got_err):
            failures.append({'case': label, 'ok': ok, 'errors': sorted(got_err), 'warnings': sorted(got_warn),
                             'want_ok': want_ok, 'want_errors': sorted(want_err), 'want_warnings': sorted(want_warn)})
    try:
        v.enforce("import os" + OK_BODY)
        failures.append({'case': 'enforce must raise'})
    except v.UnsafeGeneratedProgram:
        pass
    # --- secondary 'template' policy -------------------------------------------------------
    tmpl = v.load_space_layout_template()
    llm = "def forward(groups):\n    return {'stored': [int(groups[0])], 'layout': []}\n\ndef inverse(record):\n    return str(record['stored'][0])"
    wrapped = llm.replace('def forward(', 'def _orig_forward(').replace('def inverse(', 'def _orig_inverse(') + tmpl
    extra = 0

    def expect(label, cond):
        if not cond:
            failures.append({'case': label})

    expect('template: wrapped rejected by strict', not v.check(wrapped)['ok'])
    expect('template: wrapped accepted by template', v.check_policy(wrapped, 'python_exec', 'template')['ok'])
    expect('template: trailing newline stripped still accepted', v.check_policy(wrapped.rstrip(), 'python_exec', 'template')['ok'])
    bad_prefix = "import os\n" + wrapped
    expect('template: bad prefix rejected', not v.check_policy(bad_prefix, 'python_exec', 'template')['ok'])
    bad_prefix2 = wrapped.replace("int(groups[0])", "int(groups[0].__len__())")
    expect('template: dunder in prefix rejected', not v.check_policy(bad_prefix2, 'python_exec', 'template')['ok'])
    tampered = wrapped.replace("    return text\n", "    return text + str(globals())\n")
    expect('template: tampered template rejected', not v.check_policy(tampered, 'python_exec', 'template')['ok'])
    appended = wrapped + "\nx = open('f')\n"
    expect('template: code after template rejected', not v.check_policy(appended, 'python_exec', 'template')['ok'])
    two = llm.replace('def forward(', 'def _orig_forward(') + tmpl  # inverse not renamed -> not a wrapper
    expect('template: malformed wrapper rejected', not v.check_policy(two, 'python_exec', 'template')['ok'])
    # the fixed template itself, with its six identifiers renamed, passes every strict rule
    t2 = tmpl
    import re as _re
    for name in v.TEMPLATE_IDENTIFIERS:
        t2 = _re.sub(r'\b%s\b' % name, 'tmpl' + name, t2)
    t2 = "def tmpl_orig_forward(groups):\n    return {'stored': [groups[0]], 'layout': []}\n\ndef tmpl_orig_inverse(record):\n    return str(record['stored'][0])\n" + t2
    r = v.check(t2)
    expect('template body strict-clean apart from its identifiers: %s' % r['errors'][:3], r['ok'])
    extra = 10
    # documented V1 gap (not a failure): the pre-registered V1 validator accepts the re.enum.sys escape
    import importlib.util as _iu
    _spec = _iu.spec_from_file_location('pyexec_validator_v1', str(__import__('pathlib').Path(v.__file__).resolve().parent.parent / 'pyexec_validator.py'))
    v1 = _iu.module_from_spec(_spec)
    _spec.loader.exec_module(v1)
    esc = "x = re.enum.sys.modules['os']\ndef project_context(line, groups):\n    return None\n"
    v1_gap = {'v1_version': v1.VERSION, 'v1_accepts_context_re_escape': v1.check(esc, 'context')['ok'],
              'v2_accepts_context_re_escape': v.check(esc, 'context')['ok'],
              'v1_accepts_nested_format_traversal': v1.check("s = '{0:{1.__class__}}'.format(1, 2)" + OK_BODY)['ok'],
              'v2_accepts_nested_format_traversal': v.check("s = '{0:{1.__class__}}'.format(1, 2)" + OK_BODY)['ok']}
    expect('V2 rejects escapes', not v1_gap['v2_accepts_context_re_escape'] and not v1_gap['v2_accepts_nested_format_traversal'])
    print(json.dumps({'validator_version': v.VERSION, 'cases': len(CASES) + 1 + extra,
                      'template_body_warnings': sorted({w['rule'] for w in r['warnings']}), 'v1_gap': v1_gap,
                      'failures': failures, 'status': 'PASS' if not failures else 'FAIL'}, indent=2))
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
