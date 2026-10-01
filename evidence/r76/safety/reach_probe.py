#!/usr/bin/env python3
"""R76 track E: which dangerous objects are reachable from the generated-code namespace through
attribute chains that the static validator permits (no leading underscore)? Pure introspection of
stdlib objects with getattr on NON-underscore names; no generated code is executed, nothing is called
except zero-risk constructors used to build instance roots.
Usage: reach_probe.py OUT_JSON"""
import builtins, json, re, sys, types
from datetime import datetime, timedelta

DANGER_FUNCS = {id(getattr(builtins, n)): n for n in ('eval', 'exec', 'compile', 'open', '__import__', 'globals',
                'locals', 'vars', 'getattr', 'setattr', 'delattr', 'input', 'breakpoint', 'memoryview', 'type', 'object')}
py_roots = {'datetime': datetime, 'timedelta': timedelta, 'int': int, 'str': str, 'len': len, 'float': float,
            'round': round, 'abs': abs, 'min': min, 'max': max,
            "''": '', '0': 0, '0.0': 0.0, '[]': [], '{}': {}, '()': (), "b''": b'', 'True': True, 'None': None,
            '{1}': {1}, 'datetime(2000,1,1)': datetime(2000, 1, 1), 'timedelta(1)': timedelta(1),
            'datetime.now().astimezone().tzinfo': datetime.now().astimezone().tzinfo,
            "''.split": ''.split, '[].append': [].append}
ctx_roots = dict(py_roots, **{'re': re, "re.search('a','a')": re.search('a', 'a'), "re.compile('a')": re.compile('a')})


def bfs(roots, depth=4):
    hits, seen = [], set()
    frontier = [(name, obj) for name, obj in roots.items()]
    for d in range(depth):
        nxt = []
        for path, obj in frontier:
            for attr in dir(obj):
                if attr.startswith('_'):
                    continue
                try:
                    val = getattr(obj, attr)
                except Exception:
                    continue
                p = path + '.' + attr
                if isinstance(val, types.ModuleType):
                    hits.append({'path': p, 'kind': 'module', 'module': val.__name__})
                elif id(val) in DANGER_FUNCS:
                    hits.append({'path': p, 'kind': 'dangerous_builtin', 'name': DANGER_FUNCS[id(val)]})
                if id(val) in seen:
                    continue
                seen.add(id(val))
                nxt.append((p, val))
        frontier = nxt
    return hits


res = {'python': sys.version.split()[0]}
for label, roots in (('python_exec', py_roots), ('context', ctx_roots)):
    hits = bfs(roots)
    res[label] = {'n_hits': len(hits), 'hits': hits[:200],
                  'first_attr_names_of_module_paths': sorted({h['path'].split('.')[1] if h['path'].count('.') >= 1 else h['path'] for h in hits})}
res['re_module_attrs_that_are_modules'] = sorted(k for k, v in vars(re).items() if isinstance(v, types.ModuleType) and not k.startswith('_'))
res['re.enum.sys is sys'] = getattr(getattr(re, 'enum', None), 'sys', None) is sys
open(sys.argv[1], 'w').write(json.dumps(res, indent=1) + '\n')
print(json.dumps({k: (v if not isinstance(v, dict) else {'n_hits': v['n_hits'], 'first': v['hits'][:12]}) for k, v in res.items()}, indent=1))
