#!/usr/bin/env python3
"""R76-B2 no-LLM rule-library proposer (LIBRARY-V1, frozen 2026-09-29 before any R76-B2 result).

A hand-written, dataset-agnostic, deterministic replacement for the LLM call of the SemZip trainer.
For every sampled log group (family) it returns the same JSON shape the LLM returns for the
class-wise batch prompt (``{"families": [{family_id, family_analysis, header, slot_actions,
class_inventory, class_1_functions, class_2_functions, class_3_functions}]}``) or, for a
single-family prompt, one family object without the ``families`` wrapper.

Library = exactly the program types frozen in DEV_DESIGN_R76_zh.md section B2:
  timestamps: syslog ``Mon DD HH:MM:SS``; ``YYYY-MM-DD HH:MM:SS`` with or without ``,mmm``/``.mmm``;
              ISO-8601 ``YYYY-MM-DDTHH:MM:SS[.fff]``; ``YY/MM/DD HH:MM:SS``; ``YYYYMMDD HHMMSS``;
              ``HH:MM:SS[.fff]``; 10-digit Unix epoch seconds;
  IPv4 and IPv4:port; hexadecimal numbers; sizes with units (B/KB/MB/GB, decimals allowed);
  percentages; decimal integers and fixed-point numbers (width and leading zeros recorded).
No other type is implemented.  Nothing here depends on the dataset name, and no LLM-generated
program of any dataset was consulted while writing it (only the prompt/schema text and the
compiler/validator source of the R69 trainer).

Emission rule (fixed): the library types are tried on the group's prompt examples in the order the
compiler will execute them (class 1, then class 2, then class 3, each in the fixed order below);
earlier matches are masked, as the compiler replaces them by placeholders.  A type is emitted for the
group iff its regex matches at least one example after masking.  At most MAX_FUNCTIONS (= the
trainer's max_functions_per_family, 8) are emitted; if more types apply, the lowest CAP_PRIORITY
types are dropped.  Every emitted candidate is then compiled/verified by the unchanged trainer.
"""
from __future__ import annotations

import re

LIBRARY_VERSION = "LIBRARY-V1"
MAX_FUNCTIONS = 8

# --------------------------------------------------------------------------------------------
# Shared code fragments (verifier subset: no imports, no comprehensions, no keyword args, no
# `in`/`not`/`is`; failure is signalled by returning an empty `stored` list, which the runtime
# rejects, so the match stays in the residual).
# --------------------------------------------------------------------------------------------
_CIVIL = """
def days_from_civil(y, m, d):
    y = y - (1 if m <= 2 else 0)
    era = (y if y >= 0 else y - 399) // 400
    yoe = y - era * 400
    mp = (m + 9) % 12
    doy = (153 * mp + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146097 + doe - 719468

def civil_from_days(z):
    z = z + 719468
    era = (z if z >= 0 else z - 146096) // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    y = y + (1 if m <= 2 else 0)
    return [y, m, d]

def two(v):
    return str(v).zfill(2)

def sod_text(t):
    return two(t // 3600) + ':' + two((t // 60) % 60) + ':' + two(t % 60)

def valid_hms(h, mi, s):
    return h < 24 and mi < 60 and s < 60
"""

_BAD = "{'stored': [], 'layout': []}"

CODE_TS_SYSLOG = """
MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

def month_index(name):
    i = 0
    while i < 12:
        if MONTHS[i] == name:
            return i
        i = i + 1
    return -1

def render(v, layout):
    t = v // 86400
    rest = v % 86400
    d = t % 32
    mo = t // 32
    return MONTHS[mo] + ' ' * int(layout[0]) + str(d).zfill(int(layout[2])) + ' ' * int(layout[1]) + sod_text(rest)

def forward(groups):
    if len(groups) != 7:
        return BAD
    mon, sp1, day, sp2, hh, mm, ss = groups
    mo = month_index(mon)
    d = int(day)
    h = int(hh)
    mi = int(mm)
    s = int(ss)
    if mo < 0 or d < 1 or d > 31 or len(sp1) > 255 or len(sp2) > 255:
        return BAD
    if valid_hms(h, mi, s) == False:
        return BAD
    v = (mo * 32 + d) * 86400 + h * 3600 + mi * 60 + s
    layout = [len(sp1), len(sp2), len(day)]
    if render(v, layout) != mon + sp1 + day + sp2 + hh + ':' + mm + ':' + ss:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_TS_YMD = """
SEPS = ['', ',', '.']

def render(v, layout):
    ms = v % 1000
    t = v // 1000
    days = t // 86400
    ymd = civil_from_days(days)
    text = str(ymd[0]).zfill(4) + '-' + two(ymd[1]) + '-' + two(ymd[2]) + ' ' * int(layout[0]) + sod_text(t % 86400)
    if int(layout[1]) > 0:
        text = text + SEPS[int(layout[1])] + str(ms).zfill(3)
    return text

def forward(groups):
    if len(groups) != 9:
        return BAD
    yy, mo, dd, sp, hh, mm, ss, fsep, frac = groups
    y = int(yy)
    m = int(mo)
    d = int(dd)
    h = int(hh)
    mi = int(mm)
    s = int(ss)
    if m < 1 or m > 12 or d < 1 or d > 31 or len(sp) > 255:
        return BAD
    if valid_hms(h, mi, s) == False:
        return BAD
    code = 0
    ms = 0
    tail = ''
    if fsep == ',':
        code = 1
    if fsep == '.':
        code = 2
    if code > 0:
        ms = int(frac)
        tail = fsep + frac
    v = ((days_from_civil(y, m, d) * 86400) + h * 3600 + mi * 60 + s) * 1000 + ms
    layout = [len(sp), code]
    if render(v, layout) != yy + '-' + mo + '-' + dd + sp + hh + ':' + mm + ':' + ss + tail:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_TS_ISO = """
def render(v, layout):
    us = v % 1000000
    t = v // 1000000
    ymd = civil_from_days(t // 86400)
    text = str(ymd[0]).zfill(4) + '-' + two(ymd[1]) + '-' + two(ymd[2]) + 'T' + sod_text(t % 86400)
    fw = int(layout[0])
    if fw > 0:
        text = text + '.' + str(us).zfill(6)[:fw]
    return text

def forward(groups):
    if len(groups) != 7:
        return BAD
    yy, mo, dd, hh, mm, ss, frac = groups
    y = int(yy)
    m = int(mo)
    d = int(dd)
    h = int(hh)
    mi = int(mm)
    s = int(ss)
    if m < 1 or m > 12 or d < 1 or d > 31:
        return BAD
    if valid_hms(h, mi, s) == False:
        return BAD
    fw = 0
    us = 0
    tail = ''
    if frac != None:
        fw = len(frac)
        us = int(frac) * 10 ** (6 - fw)
        tail = '.' + frac
    v = ((days_from_civil(y, m, d) * 86400) + h * 3600 + mi * 60 + s) * 1000000 + us
    layout = [fw]
    if render(v, layout) != yy + '-' + mo + '-' + dd + 'T' + hh + ':' + mm + ':' + ss + tail:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_TS_YY_SLASH = """
def render(v, layout):
    ymd = civil_from_days(v // 86400)
    return two(ymd[0] % 100) + '/' + two(ymd[1]) + '/' + two(ymd[2]) + ' ' * int(layout[0]) + sod_text(v % 86400)

def forward(groups):
    if len(groups) != 7:
        return BAD
    yy, mo, dd, sp, hh, mm, ss = groups
    y = 2000 + int(yy)
    m = int(mo)
    d = int(dd)
    h = int(hh)
    mi = int(mm)
    s = int(ss)
    if m < 1 or m > 12 or d < 1 or d > 31 or len(sp) > 255:
        return BAD
    if valid_hms(h, mi, s) == False:
        return BAD
    v = days_from_civil(y, m, d) * 86400 + h * 3600 + mi * 60 + s
    layout = [len(sp)]
    if render(v, layout) != yy + '/' + mo + '/' + dd + sp + hh + ':' + mm + ':' + ss:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_TS_COMPACT = """
def render(v, layout):
    ymd = civil_from_days(v // 86400)
    t = v % 86400
    return str(ymd[0]).zfill(4) + two(ymd[1]) + two(ymd[2]) + ' ' * int(layout[0]) + two(t // 3600) + two((t // 60) % 60) + two(t % 60)

def forward(groups):
    if len(groups) != 7:
        return BAD
    yy, mo, dd, sp, hh, mm, ss = groups
    y = int(yy)
    m = int(mo)
    d = int(dd)
    h = int(hh)
    mi = int(mm)
    s = int(ss)
    if m < 1 or m > 12 or d < 1 or d > 31 or len(sp) > 255:
        return BAD
    if valid_hms(h, mi, s) == False:
        return BAD
    v = days_from_civil(y, m, d) * 86400 + h * 3600 + mi * 60 + s
    layout = [len(sp)]
    if render(v, layout) != yy + mo + dd + sp + hh + mm + ss:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_TS_HMS = """
def render(v, layout):
    us = v % 1000000
    text = sod_text(v // 1000000)
    fw = int(layout[0])
    if fw > 0:
        text = text + '.' + str(us).zfill(6)[:fw]
    return text

def forward(groups):
    if len(groups) != 4:
        return BAD
    hh, mm, ss, frac = groups
    h = int(hh)
    mi = int(mm)
    s = int(ss)
    if valid_hms(h, mi, s) == False:
        return BAD
    fw = 0
    us = 0
    tail = ''
    if frac != None:
        fw = len(frac)
        us = int(frac) * 10 ** (6 - fw)
        tail = '.' + frac
    v = (h * 3600 + mi * 60 + s) * 1000000 + us
    layout = [fw]
    if render(v, layout) != hh + ':' + mm + ':' + ss + tail:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_EPOCH10 = """
def forward(groups):
    text = groups[len(groups) - 1]
    if len(text) != 10:
        return BAD
    v = int(text)
    if str(v) != text:
        return BAD
    return {'stored': [v], 'layout': []}

def inverse(record):
    return str(int(record['stored'][0]))
"""

CODE_PORT = """
def forward(groups):
    text = groups[len(groups) - 1]
    v = int(text)
    if str(v) != text or v > 65535:
        return BAD
    return {'stored': [v], 'layout': []}

def inverse(record):
    return str(int(record['stored'][0]))
"""

CONTEXT_PORT_IP = """
def project_context(line, groups):
    return groups[0]
"""

CODE_RAW = """
def forward(groups):
    return {'stored': [groups[0]], 'layout': []}

def inverse(record):
    return str(record['stored'][0])
"""

CODE_SIZE = """
UNITS = ['B', 'KB', 'MB', 'GB']

def render(v, layout):
    iw = int(layout[0])
    fw = int(layout[1])
    s = str(v).zfill(iw + fw)
    text = s[:iw]
    if fw > 0:
        text = text + '.' + s[iw:]
    return text + ' ' * int(layout[2]) + UNITS[int(layout[3])]

def forward(groups):
    if len(groups) != 4:
        return BAD
    ip, fp, sp, unit = groups
    if sp == None:
        sp = ''
    fw = 0
    digits = ip
    tail = ''
    if fp != None:
        fw = len(fp)
        digits = ip + fp
        tail = '.' + fp
    code = -1
    i = 0
    while i < 4:
        if UNITS[i] == unit:
            code = i
        i = i + 1
    if code < 0 or len(sp) > 255:
        return BAD
    v = int(digits)
    layout = [len(ip), fw, len(sp), code]
    if render(v, layout) != ip + tail + sp + unit:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_PERCENT = """
def render(v, layout):
    iw = int(layout[0])
    fw = int(layout[1])
    s = str(v).zfill(iw + fw)
    text = s[:iw]
    if fw > 0:
        text = text + '.' + s[iw:]
    return text + '%'

def forward(groups):
    if len(groups) != 2:
        return BAD
    ip, fp = groups
    fw = 0
    digits = ip
    tail = ''
    if fp != None:
        fw = len(fp)
        digits = ip + fp
        tail = '.' + fp
    v = int(digits)
    layout = [len(ip), fw]
    if render(v, layout) != ip + tail + '%':
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_HEX_PREFIXED = """
PREFIXES = ['0x', '0X']

def render(v, layout):
    h = f'{v:x}'.zfill(int(layout[1]))
    if int(layout[2]) == 1:
        h = h.upper()
    return PREFIXES[int(layout[0])] + h

def forward(groups):
    if len(groups) != 2:
        return BAD
    prefix, digits = groups
    pcode = 0
    if prefix == '0X':
        pcode = 1
    case = 0
    if digits != digits.lower():
        case = 1
    v = int(digits, 16)
    layout = [pcode, len(digits), case]
    if render(v, layout) != prefix + digits:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_HEX_BARE = """
def render(v, layout):
    h = f'{v:x}'.zfill(int(layout[0]))
    if int(layout[1]) == 1:
        h = h.upper()
    return h

def forward(groups):
    digits = groups[len(groups) - 1]
    case = 0
    if digits != digits.lower():
        case = 1
    v = int(digits, 16)
    layout = [len(digits), case]
    if render(v, layout) != digits:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_FIXED = """
def render(v, layout):
    iw = int(layout[1])
    fw = int(layout[2])
    s = str(abs(v)).zfill(iw + fw)
    sign = ''
    if int(layout[0]) == 1:
        sign = '-'
    return sign + s[:iw] + '.' + s[iw:]

def forward(groups):
    if len(groups) != 3:
        return BAD
    neg, ip, fp = groups
    n = 0
    v = int(ip + fp)
    if neg == '-':
        n = 1
        v = 0 - v
    layout = [n, len(ip), len(fp)]
    if render(v, layout) != neg + ip + '.' + fp:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""

CODE_INT = """
def render(v, layout):
    sign = ''
    if int(layout[0]) == 1:
        sign = '-'
    return sign + str(abs(v)).zfill(int(layout[1]))

def forward(groups):
    if len(groups) != 2:
        return BAD
    neg, digits = groups
    n = 0
    v = int(digits)
    if neg == '-':
        n = 1
        v = 0 - v
    layout = [n, len(digits)]
    if render(v, layout) != neg + digits:
        return BAD
    return {'stored': [v], 'layout': layout}

def inverse(record):
    return render(int(record['stored'][0]), record.get('layout', []))
"""


def _code(body: str, civil: bool = False) -> str:
    head = "BAD = " + _BAD + "\n"
    return (head + (_CIVIL if civil else "") + body).strip("\n")


C1, C2, C3 = "class_1_composed_numeric", "class_2_formatted_numeric", "class_3_common_variable"

# (name, class, meaning, regex, replacement, code, general, order_hint, context_code, cap_priority)
# cap_priority: lower = kept first when more than MAX_FUNCTIONS types apply to one group.
TYPES = [
    # ---- class 1: composed timestamps (one integer + display layout) ----
    ("ts_syslog", C1, "timestamp: syslog month day time",
     r"(?<![A-Za-z])(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)( +)(\d{1,2})( +)(\d{2}):(\d{2}):(\d{2})(?![\d:])",
     "{placeholder}", _code(CODE_TS_SYSLOG, True), True, "global_composed", None, 0),
    ("ts_ymd_hms", C1, "timestamp: YYYY-MM-DD HH:MM:SS with optional ,mmm or .mmm",
     r"(?<![\d.])(\d{4})-(\d{2})-(\d{2})( +)(\d{2}):(\d{2}):(\d{2})(?:([,.])(\d{3}))?(?![\d:])",
     "{placeholder}", _code(CODE_TS_YMD, True), True, "global_composed", None, 1),
    ("ts_iso8601", C1, "timestamp: ISO-8601 YYYY-MM-DDTHH:MM:SS with optional fraction",
     r"(?<![\d.])(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?![\d:])",
     "{placeholder}", _code(CODE_TS_ISO, True), True, "global_composed", None, 2),
    ("ts_yy_slash", C1, "timestamp: YY/MM/DD HH:MM:SS",
     r"(?<![\d/.])(\d{2})/(\d{2})/(\d{2})( +)(\d{2}):(\d{2}):(\d{2})(?![\d:])",
     "{placeholder}", _code(CODE_TS_YY_SLASH, True), True, "global_composed", None, 3),
    ("ts_compact", C1, "timestamp: YYYYMMDD HHMMSS",
     r"(?<![\w.])(\d{4})(\d{2})(\d{2})( +)(\d{2})(\d{2})(\d{2})(?![\w.])",
     "{placeholder}", _code(CODE_TS_COMPACT, True), True, "global_composed", None, 4),
    ("ts_hms", C1, "timestamp: time of day HH:MM:SS with optional fraction",
     r"(?<![\d:.])(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(?![\d:])",
     "{placeholder}", _code(CODE_TS_HMS, True), True, "global_composed", None, 5),
    # ---- class 2: formatted numeric values (one integer + width/format layout) ----
    ("ts_epoch10", C2, "timestamp: 10-digit unix epoch seconds",
     r"(?<![\w.])([1-9]\d{9})(?![\w.])",
     "{placeholder}", _code(CODE_EPOCH10), True, "global_atomic", None, 6),
    ("ipv4_port", C2, "port of ipv4:port endpoint",
     r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})(?![\d.])",
     "{group1}:{placeholder}", _code(CODE_PORT), True, "global_atomic", CONTEXT_PORT_IP.strip("\n"), 7),
    ("size_unit", C2, "size with unit B/KB/MB/GB",
     r"(?<![\w.])(\d{1,12})(?:\.(\d{1,6}))?(?:( +))?(B|KB|MB|GB)(?![\w.])",
     "{placeholder}", _code(CODE_SIZE), False, "family_structured", None, 9),
    ("percentage", C2, "percentage",
     r"(?<![\w.])(\d{1,12})(?:\.(\d{1,6}))?%",
     "{placeholder}", _code(CODE_PERCENT), False, "family_structured", None, 10),
    ("hex_prefixed", C2, "hexadecimal number with 0x prefix",
     r"(?<![\w.])(0[xX])([0-9a-fA-F]{1,15})(?![\w.])",
     "{placeholder}", _code(CODE_HEX_PREFIXED), False, "family_structured", None, 11),
    ("hex_bare", C2, "hexadecimal number",
     r"(?<![\w.:-])(?=[0-9a-fA-F]*[0-9])(?=[0-9a-fA-F]*[a-fA-F])([0-9a-fA-F]{4,15})(?![\w.:-])",
     "{placeholder}", _code(CODE_HEX_BARE), False, "family_structured", None, 12),
    ("fixed_point", C2, "fixed-point decimal number",
     r"(?<![\w.])(-?)(\d{1,12})\.(\d{1,6})(?![\w.])",
     "{placeholder}", _code(CODE_FIXED), False, "family_structured", None, 13),
    ("decimal_int", C2, "decimal integer",
     r"(?<![\w.])(-?)(\d{1,18})(?![\w.])",
     "{placeholder}", _code(CODE_INT), False, "family_structured", None, 14),
    # ---- class 3: common variable stored as exact string ----
    ("ipv4", C3, "ipv4 address",
     r"(?<![\d.])((?:\d{1,3}\.){3}\d{1,3})(?![\d.])",
     "{placeholder}", CODE_RAW.strip("\n"), True, "global_atomic", None, 8),
]
TYPE_NAMES = [t[0] for t in TYPES]
_COMPILED = [(t, re.compile(t[3])) for t in TYPES]
_MASK = "\x00"


def _function_item(t, role: str) -> dict:
    name, cls, meaning, regex, replacement, code, general, order_hint, context_code, _prio = t
    numeric = cls != C3
    return {
        "tag": "lib_" + name,
        "meaning": meaning,
        "class": cls,
        "value_type": "numeric" if numeric else "string",
        "target_role": role,
        "context": ({"code": context_code} if context_code else None),
        "source_separation": {
            "decision": ("split_context_value" if context_code else ("merge_equivalent" if cls == C1 else "raw_single_source")),
            "stored_source": meaning,
            "context_source": ("ipv4 address of the same endpoint" if context_code else None),
            "why_not_merge_raw": "fixed library type " + name,
        },
        "general": bool(general),
        "order_hint": order_hint,
        "regex": regex,
        "replacement": replacement,
        "program": {"op": "python_exec", "code": code},
    }


def _masker(replacement: str):
    """Apply the type's replacement template with the placeholder masked (mirrors the compiler's replay)."""
    def repl(m: re.Match) -> str:
        out = replacement.replace("{placeholder}", _MASK)
        for i in range(1, 7):
            out = out.replace("{group%d}" % i, (m.group(i) or "") if i <= len(m.groups()) else "")
        return out
    return repl


def propose_family(family_id: int, examples: list[str]) -> dict:
    """Library candidates for one group, in the LLM's class-wise family schema."""
    lines = [str(e).rstrip("\r\n") for e in examples]
    applicable = []  # (type, role)
    for t, rx in _COMPILED:
        hit = False
        starts = []
        new_lines = []
        for line in lines:
            ms = list(rx.finditer(line))
            if ms:
                hit = True
                starts.append(ms[0].start())
                line = rx.sub(_masker(t[4]), line)
            new_lines.append(line)
        lines = new_lines
        if hit:
            role = "log_timestamp" if (t[1] == C1 and all(s <= 1 for s in starts)) else "ordinary_value"
            applicable.append((t, role))
    # cap: keep the MAX_FUNCTIONS highest-priority types, preserving execution order
    if len(applicable) > MAX_FUNCTIONS:
        keep = sorted(applicable, key=lambda tr: tr[0][9])[:MAX_FUNCTIONS]
        keep_names = {tr[0][0] for tr in keep}
        applicable = [tr for tr in applicable if tr[0][0] in keep_names]
    buckets = {C1: [], C2: [], C3: []}
    for t, role in applicable:
        buckets[t[1]].append(_function_item(t, role))
    names = {cls: [f["tag"] for f in fs] for cls, fs in buckets.items()}
    return {
        "family_id": family_id,
        "family_analysis": {
            "has_composable_tokens": bool(buckets[C1]),
            "composable_groups": [{"name": n, "must_emit_function": True} for n in names[C1]],
            "has_semantic_equivalent_tokens": False,
            "semantic_equivalent_groups": [],
            "raw_variable_groups": [{"name": n, "must_emit_function": True} for n in names[C3]],
        },
        "header": {"has_unified_header": False, "reason": "library proposer: no header model"},
        "slot_actions": [],
        "class_inventory": {
            "class_1_composed_numeric": names[C1],
            "class_2_formatted_numeric": names[C2],
            "class_3_common_variable": names[C3],
        },
        "class_1_functions": buckets[C1],
        "class_2_functions": buckets[C2],
        "class_3_functions": buckets[C3],
        "_library_version": LIBRARY_VERSION,
        "_library_types": [t[0] for t, _ in applicable],
    }


def propose_batch(families: list[tuple[int, list[str]]]) -> dict:
    return {"families": [propose_family(fid, ex) for fid, ex in families], "_library_version": LIBRARY_VERSION}
