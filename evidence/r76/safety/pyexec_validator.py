#!/usr/bin/env python3
"""R76 track E: static AST allow-list validator for LLM-generated ``python_exec`` code.

Pure static analysis. The code under test is only handed to ``ast.parse``; it is
never compiled to bytecode, imported or executed by this module.

Hard rules (any hit => the program is REJECTED; the enforcing decoder fails closed):
  E_SYNTAX         source does not parse
  E_NODE           AST node type outside the allow-list (ALLOWED_NODES). Named
                   sub-rules for the pre-registered bans:
                   E_IMPORT (Import/ImportFrom), E_SCOPE (Global/Nonlocal),
                   E_CLASS (ClassDef), E_LAMBDA (Lambda), E_GENERATOR
                   (GeneratorExp/Yield/YieldFrom/Await/async defs: frame access),
                   E_COMPREHENSION (List/Set/DictComp), E_EXCEPT (Try/Raise),
                   E_WITH (With), E_OTHER_NODE (everything else not allowed)
  E_UNDERSCORE     a name, function name, argument, keyword or attribute starts with '_'
  E_FORBIDDEN_NAME reference to exec/eval/compile/open/__import__/globals/locals/
                   vars/getattr/setattr/delattr/input/breakpoint/memoryview
                   (called or not)
  E_FREE_NAME      a name that is read but never bound in the program and is not in
                   the builtin allow-list (ALLOWED_BUILTINS; + ``re`` for context
                   projectors). The list equals the namespace the frozen runtime
                   actually provides (pare_dataset_extract.SAFE_PYTHON_EXEC_GLOBALS
                   minus __import__).
  E_INTROSPECTION  attribute that reaches frames/code/tracebacks or the MRO without a
                   leading underscore (gi_frame, f_globals, tb_frame, mro, ...)
  E_FORMAT         str.format/format_map attribute traversal: format_map at all, or
                   .format on a non-literal format string, or a literal format string
                   whose replacement field uses '.' or '[' (bypasses E_UNDERSCORE)
  E_DECORATOR      decorators (code executed at definition time; never needed)
  E_STAR           *args/**kwargs in calls or definitions (never needed)
  E_ENTRYPOINT     python_exec code must define top-level forward and inverse;
                   context code must define top-level project_context

Report-only warnings (never cause rejection, never used to change a program):
  W_WHILE_TRUE_NO_BREAK  `while <truthy constant>:` whose body has no break that
                         belongs to that loop
  W_WHILE                any while loop (unbounded iteration count; runtime has no
                         CPU/memory limit)
  W_TOPLEVEL_STMT        executable top-level statement other than def / assignment
                         (runs at load time, before forward/inverse are called)
  W_RECURSION            a function that calls itself by name
  W_NONDETERMINISTIC     datetime.now/today/utcnow (result depends on wall clock)
"""
from __future__ import annotations

import ast
import hashlib
import json
import string
import sys

VERSION = 'R76-PYEXEC-ALLOWLIST-V1-20260929'

# Exactly the names the frozen runtime puts in the exec namespace
# (pare_dataset_extract.py SAFE_PYTHON_EXEC_GLOBALS, lines 4475-4487), minus __import__,
# which is only there for datetime.strptime's lazy stdlib import.
ALLOWED_BUILTINS = frozenset({'datetime', 'timedelta', 'int', 'str', 'len', 'float',
                              'round', 'abs', 'min', 'max'})
# compile_generated_context_projector additionally binds `re` (lines 4568-4571).
CONTEXT_EXTRA_BUILTINS = frozenset({'re'})

FORBIDDEN_NAMES = frozenset({'exec', 'eval', 'compile', 'open', '__import__', 'globals',
                             'locals', 'vars', 'getattr', 'setattr', 'delattr', 'input',
                             'breakpoint', 'memoryview'})

# Attribute names that expose interpreter internals without a leading underscore.
FORBIDDEN_ATTRS = frozenset({
    'gi_frame', 'gi_code', 'gi_yieldfrom', 'gi_running',
    'cr_frame', 'cr_code', 'cr_await', 'cr_origin',
    'ag_frame', 'ag_code', 'ag_await',
    'f_back', 'f_globals', 'f_locals', 'f_builtins', 'f_code', 'f_trace',
    'tb_frame', 'tb_next', 'tb_lasti',
    'co_code', 'co_consts', 'co_names',
    'func_globals', 'func_code', 'im_func', 'im_self',
    'mro', 'format_map',
})

ALLOWED_NODES = (
    # structure
    ast.Module, ast.FunctionDef, ast.arguments, ast.arg, ast.Return, ast.Assign,
    ast.AugAssign, ast.Expr, ast.If, ast.For, ast.While, ast.Break, ast.Continue, ast.Pass,
    # expressions
    ast.Name, ast.Constant, ast.List, ast.Tuple, ast.Dict, ast.Set, ast.Subscript,
    ast.Slice, ast.BinOp, ast.UnaryOp, ast.BoolOp, ast.Compare, ast.IfExp, ast.JoinedStr,
    ast.FormattedValue, ast.Call, ast.keyword, ast.Attribute,
    # contexts
    ast.Load, ast.Store,
    # pure operators
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.FloorDiv, ast.Mod, ast.Pow, ast.LShift,
    ast.RShift, ast.BitAnd, ast.BitOr, ast.BitXor, ast.UAdd, ast.USub, ast.Not, ast.Invert,
    ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE, ast.In, ast.NotIn, ast.Is,
    ast.IsNot, ast.And, ast.Or,
)
if hasattr(ast, 'Index'):  # Python <= 3.8 wraps subscripts; harmless container node
    ALLOWED_NODES = ALLOWED_NODES + (ast.Index,)

_NAMED_BANS = [
    ((ast.Import, ast.ImportFrom), 'E_IMPORT'),
    ((ast.Global, ast.Nonlocal), 'E_SCOPE'),
    ((ast.ClassDef,), 'E_CLASS'),
    ((ast.Lambda,), 'E_LAMBDA'),
    ((ast.GeneratorExp, ast.Yield, ast.YieldFrom, ast.Await, ast.AsyncFunctionDef,
      ast.AsyncFor, ast.AsyncWith), 'E_GENERATOR'),
    ((ast.ListComp, ast.SetComp, ast.DictComp, ast.comprehension), 'E_COMPREHENSION'),
    ((ast.Try, ast.ExceptHandler, ast.Raise), 'E_EXCEPT'),
    ((ast.With, ast.withitem), 'E_WITH'),
    ((ast.Starred,), 'E_STAR'),
]

ENTRYPOINTS = {'python_exec': ('forward', 'inverse'), 'context': ('project_context',)}


class UnsafeGeneratedProgram(ValueError):
    """Raised by enforce(); decoding must stop (fail closed)."""


def code_sha256(code: str) -> str:
    return hashlib.sha256(code.encode('utf-8')).hexdigest()


def _pos(node):
    return getattr(node, 'lineno', None), getattr(node, 'col_offset', None)


def _truthy_constant(node) -> bool:
    return isinstance(node, ast.Constant) and bool(node.value)


def _loop_has_own_break(loop) -> bool:
    """True if a Break in the loop body belongs to this loop (not to a nested loop)."""
    stack = list(loop.body)
    while stack:
        node = stack.pop()
        if isinstance(node, ast.Break):
            return True
        if isinstance(node, (ast.For, ast.While, ast.AsyncFor, ast.FunctionDef,
                             ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)):
            continue  # a break in there belongs to another loop / scope
        stack.extend(ast.iter_child_nodes(node))
    return False


def _bound_names(tree) -> set:
    bound = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.name)
        elif isinstance(node, ast.arg):
            bound.add(node.arg)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            bound.add(node.id)
    return bound


def check(code, kind: str = 'python_exec') -> dict:
    """Return {'ok', 'errors', 'warnings', 'stats'}; never executes `code`."""
    if kind not in ENTRYPOINTS:
        raise ValueError('kind must be python_exec or context')
    errors, warnings = [], []

    def err(rule, node, detail):
        line, col = _pos(node) if node is not None else (None, None)
        errors.append({'rule': rule, 'line': line, 'col': col, 'detail': detail})

    def warn(rule, node, detail):
        line, col = _pos(node) if node is not None else (None, None)
        warnings.append({'rule': rule, 'line': line, 'col': col, 'detail': detail})

    stats = {'nodes': {}, 'builtins_used': [], 'attributes_used': [], 'while_loops': 0,
             'bytes': None}
    if not isinstance(code, str):
        err('E_SYNTAX', None, 'code is not a string: %s' % type(code).__name__)
        return {'ok': False, 'errors': errors, 'warnings': warnings, 'stats': stats}
    stats['bytes'] = len(code.encode('utf-8'))
    try:
        tree = ast.parse(code, mode='exec')
    except (SyntaxError, ValueError, MemoryError, RecursionError) as exc:
        err('E_SYNTAX', None, '%s: %s' % (type(exc).__name__, exc))
        return {'ok': False, 'errors': errors, 'warnings': warnings, 'stats': stats}

    allowed_free = ALLOWED_BUILTINS | (CONTEXT_EXTRA_BUILTINS if kind == 'context' else frozenset())
    bound = _bound_names(tree)
    node_counts, builtins_used, attrs_used = {}, set(), set()

    for node in ast.walk(tree):
        name = type(node).__name__
        node_counts[name] = node_counts.get(name, 0) + 1
        if not isinstance(node, ALLOWED_NODES):
            rule = 'E_OTHER_NODE'
            for types, named in _NAMED_BANS:
                if isinstance(node, types):
                    rule = named
                    break
            err(rule, node, 'disallowed node %s' % name)
        # identifiers
        if isinstance(node, ast.Name):
            if node.id.startswith('_'):
                err('E_UNDERSCORE', node, 'name %r' % node.id)
            if node.id in FORBIDDEN_NAMES:
                err('E_FORBIDDEN_NAME', node, 'name %r' % node.id)
            elif isinstance(node.ctx, ast.Load) and node.id not in bound:
                if node.id in allowed_free:
                    builtins_used.add(node.id)
                elif not node.id.startswith('_'):
                    err('E_FREE_NAME', node, 'name %r is not bound in the program and not an allow-listed builtin' % node.id)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.name.startswith('_'):
                err('E_UNDERSCORE', node, 'definition name %r' % node.name)
            if node.name in FORBIDDEN_NAMES:
                err('E_FORBIDDEN_NAME', node, 'definition shadows %r' % node.name)
            if getattr(node, 'decorator_list', None):
                err('E_DECORATOR', node, 'decorator on %r' % node.name)
        elif isinstance(node, ast.arg):
            if node.arg.startswith('_'):
                err('E_UNDERSCORE', node, 'argument %r' % node.arg)
            if node.arg in FORBIDDEN_NAMES:
                err('E_FORBIDDEN_NAME', node, 'argument shadows %r' % node.arg)
        elif isinstance(node, ast.arguments):
            if node.vararg is not None or node.kwarg is not None:
                err('E_STAR', node, '*args/**kwargs in definition')
        elif isinstance(node, ast.keyword):
            if node.arg is None:
                err('E_STAR', node, '**mapping in call')
            elif node.arg.startswith('_'):
                err('E_UNDERSCORE', node, 'keyword %r' % node.arg)
        elif isinstance(node, ast.Attribute):
            attrs_used.add(node.attr)
            if node.attr.startswith('_'):
                err('E_UNDERSCORE', node, 'attribute %r' % node.attr)
            elif node.attr in FORBIDDEN_ATTRS and node.attr != 'format_map':
                err('E_INTROSPECTION', node, 'attribute %r' % node.attr)
            if node.attr == 'format_map':
                err('E_FORMAT', node, 'format_map')
            elif node.attr == 'format':
                recv = node.value
                if not (isinstance(recv, ast.Constant) and isinstance(recv.value, str)):
                    err('E_FORMAT', node, '.format on a non-literal format string')
                else:
                    try:
                        fields = [f for _, f, _, _ in string.Formatter().parse(recv.value) if f]
                    except ValueError as exc:
                        fields = []
                        err('E_FORMAT', node, 'unparsable format string: %s' % exc)
                    for field in fields:
                        if '.' in field or '[' in field:
                            err('E_FORMAT', node, 'format field %r traverses attributes/items' % field)
            if node.attr in ('now', 'today', 'utcnow'):
                warn('W_NONDETERMINISTIC', node, 'attribute %r' % node.attr)
        elif isinstance(node, ast.While):
            stats['while_loops'] += 1
            warn('W_WHILE', node, 'while loop (unbounded iteration count)')
            if _truthy_constant(node.test) and not _loop_has_own_break(node):
                warn('W_WHILE_TRUE_NO_BREAK', node, 'while <truthy constant> without a break of its own')

    # recursion (report only)
    for fn in [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]:
        for sub in ast.walk(fn):
            if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Name)
                    and sub.func.id == fn.name and sub is not fn):
                warn('W_RECURSION', sub, 'function %r calls itself' % fn.name)
                break
    # top level
    for stmt in tree.body:
        if isinstance(stmt, ast.FunctionDef):
            continue
        if isinstance(stmt, (ast.Assign, ast.AugAssign)):
            continue
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Constant):
            continue  # docstring / bare literal
        warn('W_TOPLEVEL_STMT', stmt, 'top-level %s executes at load time' % type(stmt).__name__)
    top_defs = {s.name for s in tree.body if isinstance(s, ast.FunctionDef)}
    for entry in ENTRYPOINTS[kind]:
        if entry not in top_defs:
            err('E_ENTRYPOINT', None, 'missing top-level def %s' % entry)

    stats['nodes'] = dict(sorted(node_counts.items()))
    stats['builtins_used'] = sorted(builtins_used)
    stats['attributes_used'] = sorted(attrs_used)
    return {'ok': not errors, 'errors': errors, 'warnings': warnings, 'stats': stats}


# ---------------------------------------------------------------------------------------------
# Secondary policy 'template' (POST-HOC, added after the strict static check showed that every
# rejection came from the runtime's own deterministic space-layout repair wrapper; reported
# separately from the pre-registered 'strict' policy).
#
# semzip_pure.space_layout_python_exec_program (frozen runtime, semzip_pure.py:932-1067) builds
#     code = rename_python_exec_entrypoints(llm_code) + WRAPPER
# i.e. it renames the LLM's `def forward(`/`def inverse(` to `def _orig_forward(`/`def _orig_inverse(`
# and appends a fixed, non-LLM template that defines _space_lengths, _digit_widths,
# _apply_space_lengths, _apply_digit_widths, forward and inverse. Under 'template' a code string
# is accepted iff (a) it ends with that exact frozen template (sha-pinned; trailing whitespace of
# the whole string ignored), (b) the prefix contains exactly one `def _orig_forward(` and one
# `def _orig_inverse(`, and (c) the prefix with those two names mapped back to forward/inverse
# passes EVERY strict rule. Nothing else is relaxed; code without the template is checked strictly.
# ---------------------------------------------------------------------------------------------
FROZEN_SEMZIP_PURE = ('<WORKDIR>/'
                      'r73_quality_gate_20260927/r73_qg/art/source/runtime/semzip_pure.py')
SPACE_LAYOUT_TEMPLATE_SHA256 = 'f1389aec32de6fa3f3ebb78efc179b82daffaca45acd36c0c63142531c53e9e9'
TEMPLATE_IDENTIFIERS = ('_orig_forward', '_orig_inverse', '_space_lengths', '_digit_widths',
                        '_apply_space_lengths', '_apply_digit_widths')
_TEMPLATE = None


def load_space_layout_template(path: str = FROZEN_SEMZIP_PURE) -> str:
    """Read the `wrapper = r'''...'''` literal from the frozen source by AST (no import/exec)."""
    global _TEMPLATE
    if _TEMPLATE is None:
        tree = ast.parse(open(path, encoding='utf-8').read())
        fns = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
               and n.name == 'space_layout_python_exec_program']
        lits = [n.value.value for fn in fns for n in ast.walk(fn) if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == 'wrapper' for t in n.targets)
                and isinstance(n.value, ast.Constant) and isinstance(n.value.value, str)]
        if len(lits) != 1 or hashlib.sha256(lits[0].encode('utf-8')).hexdigest() != SPACE_LAYOUT_TEMPLATE_SHA256:
            raise RuntimeError('frozen space-layout template not found or its sha256 changed')
        _TEMPLATE = lits[0]
    return _TEMPLATE


def split_space_layout(code):
    """Return the LLM-written prefix with entrypoints mapped back, or None if not template-wrapped."""
    if not isinstance(code, str):
        return None
    tmpl = load_space_layout_template().rstrip()
    body = code.rstrip()
    if not body.endswith(tmpl):
        return None
    prefix = body[:-len(tmpl)]
    if prefix.count('def _orig_forward(') != 1 or prefix.count('def _orig_inverse(') != 1:
        return None
    return prefix.replace('def _orig_forward(', 'def forward(').replace('def _orig_inverse(', 'def inverse(')


def check_policy(code, kind: str = 'python_exec', policy: str = 'strict') -> dict:
    if policy == 'strict':
        return check(code, kind)
    if policy != 'template':
        raise ValueError('policy must be strict or template')
    if kind == 'python_exec':
        prefix = split_space_layout(code)
        if prefix is not None:
            report = check(prefix, kind)
            report['template'] = {'name': 'semzip_pure.space_layout_python_exec_program',
                                  'template_sha256': SPACE_LAYOUT_TEMPLATE_SHA256,
                                  'validated': 'LLM-written prefix (entrypoints un-renamed) under all strict rules'}
            return report
    return check(code, kind)


def enforce(code, kind: str = 'python_exec', policy: str = 'strict') -> dict:
    """Validate; raise UnsafeGeneratedProgram (fail closed) on any hard-rule violation."""
    report = check_policy(code, kind, policy)
    if not report['ok']:
        first = report['errors'][0]
        raise UnsafeGeneratedProgram('R76 validator (%s) rejected %s code sha256=%s: %d violation(s), first %s line %s: %s'
                                     % (policy, kind, code_sha256(code) if isinstance(code, str) else '?',
                                        len(report['errors']), first['rule'], first['line'], first['detail']))
    return report


if __name__ == '__main__':
    # usage: pyexec_validator.py FILE [python_exec|context] [strict|template]   (FILE holds raw source)
    src = open(sys.argv[1], encoding='utf-8').read()
    print(json.dumps(check_policy(src, sys.argv[2] if len(sys.argv) > 2 else 'python_exec',
                                  sys.argv[3] if len(sys.argv) > 3 else 'strict'), indent=2))
