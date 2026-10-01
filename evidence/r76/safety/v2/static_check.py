#!/usr/bin/env python3
"""R76 track E step 3: statically check every published / gated program with pyexec_validator.

Read-only on all inputs. No generated code is compiled or executed. Output:
  static_check_result.json  (per-file records, per-code verdicts, every violation verbatim
                             with the offending source line, summary counts per source group)
Usage: static_check.py [OUT_JSON]
"""
import glob
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import pyexec_validator as v

ROOT = Path('<WORKDIR>')
R73 = ROOT / 'r73_quality_gate_20260927/r73_qg'
R71 = ROOT / 'r71_strict_main_20260923'
R75 = ROOT / 'r75_fallback_evolution_20260927'
DATASETS = ['Android', 'Apache', 'BGL', 'Hadoop', 'HDFS', 'HealthApp', 'HPC', 'Linux', 'Mac', 'OpenSSH',
            'OpenStack', 'Proxifier', 'Spark', 'Thunderbird', 'Windows', 'Zookeeper']


def sha_file(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def g(pattern):
    return sorted(glob.glob(str(pattern)))


def sources():
    """(group, path, note) for every program/policy file in scope."""
    out = []
    for d in DATASETS:
        out.append(('R73_pool_publication', R73 / 'runs/publish/pool' / d / 'program.json', d))
        out.append(('R73_pool_publication_storage', R73 / 'runs/publish/pool' / d / 'storage/storage.json', d))
    for d in DATASETS:
        pub = json.loads((R71 / 'training' / d / 'publication.json').read_text())
        out.append(('R71_SemZip1_publication', Path(pub['deployed_plan']), d))
        out.append(('R71_SemZip1_publication_storage', Path(pub['storage']), d))
    for p in g(R73 / 'art/deployments/*/program.json'):
        out.append(('R73_artifact_deployment(R71 copy)', Path(p), Path(p).parent.name))
    for p in g(R73 / 'runs/qg/c[0-9]/*/selected_plan.json'):
        out.append(('R73_gated_candidate_plan', Path(p), Path(p).parts[-3] + '/' + Path(p).parts[-2]))
    for p in g(R73 / 'runs/publish/qg1c0/*/program.json'):
        out.append(('R73_qg1c0_publication', Path(p), Path(p).parent.name))
    for p in g(R75 / 'upd/b*/runs/publish/evo/*/program.json') + g(R75 / 'spark_run/upd/b*/runs/publish/evo/*/program.json'):
        out.append(('R75_evolution_update_program', Path(p), str(Path(p).relative_to(R75))))
    for p in g(R75 / 'upd/b*/runs/publish/evo/*/storage/storage.json') + g(R75 / 'spark_run/upd/b*/runs/publish/evo/*/storage/storage.json'):
        out.append(('R75_evolution_update_storage', Path(p), str(Path(p).relative_to(R75))))
    for p in g(R75 / 'package/SemZip_Evolution_Addendum/*/updates/*/publication/program.json'):
        out.append(('R75_evolution_package_copy', Path(p), str(Path(p).relative_to(R75))))
    for p in g(R75 / 'upd/b*/runs/qg/c[0-9]/*/selected_plan.json') + g(R75 / 'spark_run/upd/b*/runs/qg/c[0-9]/*/selected_plan.json'):
        out.append(('R75_evolution_gated_candidate_plan', Path(p), str(Path(p).relative_to(R75))))
    for p in [R75 / 'empty/extraction.json', R75 / 'spark_run/empty/extraction.json'] + [Path(x) for x in g(R75 / 'cfb_work/*/extraction.json')]:
        out.append(('R75_empty_program', p, str(p.relative_to(R75))))
    for p in [R75 / 'empty/storage.json', R75 / 'spark_run/empty/storage.json'] + [Path(x) for x in g(R75 / 'cfb_work/*/storage.json')]:
        out.append(('R75_empty_program_storage', p, str(p.relative_to(R75))))
    return out


def extract_codes(obj, path='$'):
    """Yield (json_path, kind, code, op) for every executable code string (generic walk).

    * dict with str 'code'          -> python_exec code (op recorded; llm_multi_program has no op)
    * dict with str 'context_code'  -> context projector code
    * str values that are JSON objects (archive metadata stores programs as JSON strings) are parsed.
    """
    if isinstance(obj, dict):
        if isinstance(obj.get('code'), str):
            yield path + '.code', 'python_exec', obj['code'], obj.get('op')
        if isinstance(obj.get('context_code'), str):
            yield path + '.context_code', 'context', obj['context_code'], obj.get('op')
        for k, val in obj.items():
            if k in ('code', 'context_code'):
                continue
            yield from extract_codes(val, '%s.%s' % (path, k))
    elif isinstance(obj, list):
        for i, val in enumerate(obj):
            yield from extract_codes(val, '%s[%d]' % (path, i))
    elif isinstance(obj, str) and obj.startswith('{') and '"code"' in obj:
        try:
            parsed = json.loads(obj)
        except ValueError:
            return
        yield from extract_codes(parsed, path + '<json>')


def ops_in(obj, acc):
    if isinstance(obj, dict):
        if 'op' in obj and isinstance(obj['op'], str):
            acc[obj['op']] = acc.get(obj['op'], 0) + 1
        for val in obj.values():
            ops_in(val, acc)
    elif isinstance(obj, list):
        for val in obj:
            ops_in(val, acc)
    return acc


def with_lines(code, items):
    lines = code.splitlines()
    out = []
    for it in items:
        it = dict(it)
        ln = it.get('line')
        it['source_line'] = lines[ln - 1] if isinstance(ln, int) and 0 < ln <= len(lines) else None
        out.append(it)
    return out


def main(out_path):
    t0 = time.time()
    verdicts = {}   # code sha -> verdict (validation is a pure function of (code, kind))
    files = []
    for group, path, note in sources():
        rec = {'group': group, 'path': str(path), 'note': note}
        if not path.is_file():
            rec['status'] = 'MISSING'
            files.append(rec)
            continue
        data = json.loads(path.read_text())
        rec['file_sha256'] = sha_file(path)
        rec['spec_count'] = len(data.get('specs', [])) if isinstance(data, dict) else None
        rec['storage_program_count'] = len(data.get('programs', [])) if isinstance(data, dict) and isinstance(data.get('programs'), list) else None
        rec['ops'] = ops_in(data, {})
        codes = []
        for jpath, kind, code, op in extract_codes(data):
            key = v.code_sha256(code) + ':' + kind
            if key not in verdicts:
                r = v.check(code, kind)
                rt = v.check_policy(code, kind, 'template')
                verdicts[key] = {'code_sha256': v.code_sha256(code), 'kind': kind, 'ok': r['ok'],
                                 'errors': with_lines(code, r['errors']),
                                 'warnings': with_lines(code, r['warnings']),
                                 'template_ok': rt['ok'], 'template_wrapper_detected': 'template' in rt,
                                 'template_errors': rt['errors'] if 'template' in rt else None,
                                 'stats': r['stats'], 'code': code}
            codes.append({'json_path': jpath, 'kind': kind, 'op': op, 'code_sha256': v.code_sha256(code),
                          'ok': verdicts[key]['ok'], 'template_ok': verdicts[key]['template_ok']})
        rec['codes'] = codes
        rec['code_count'] = len(codes)
        rec['rejected_code_count'] = sum(not c['ok'] for c in codes)
        rec['rejected_code_count_template'] = sum(not c['template_ok'] for c in codes)
        rec['status'] = 'PASS' if not rec['rejected_code_count'] else 'REJECTED'
        rec['status_template'] = 'PASS' if not rec['rejected_code_count_template'] else 'REJECTED'
        files.append(rec)

    groups = {}
    for rec in files:
        s = groups.setdefault(rec['group'], {'files': 0, 'missing': 0, 'files_with_code': 0, 'files_rejected': 0,
                                             'code_occurrences': 0, 'rejected_occurrences': 0,
                                             'files_rejected_template': 0, 'rejected_occurrences_template': 0,
                                             'unique_codes': set(), 'unique_rejected': set(),
                                             'unique_rejected_template': set(), 'ops': {}})
        s['files'] += 1
        if rec['status'] == 'MISSING':
            s['missing'] += 1
            continue
        s['files_with_code'] += bool(rec['code_count'])
        s['files_rejected'] += rec['status'] == 'REJECTED'
        s['code_occurrences'] += rec['code_count']
        s['rejected_occurrences'] += rec['rejected_code_count']
        s['files_rejected_template'] += rec['status_template'] == 'REJECTED'
        s['rejected_occurrences_template'] += rec['rejected_code_count_template']
        for c in rec['codes']:
            s['unique_codes'].add(c['code_sha256'] + ':' + c['kind'])
            if not c['ok']:
                s['unique_rejected'].add(c['code_sha256'] + ':' + c['kind'])
            if not c['template_ok']:
                s['unique_rejected_template'].add(c['code_sha256'] + ':' + c['kind'])
        for op, n in rec['ops'].items():
            s['ops'][op] = s['ops'].get(op, 0) + n
    rule_counts, warn_counts, builtins, attrs, nodes = {}, {}, {}, {}, {}
    for vd in verdicts.values():
        for e in vd['errors']:
            rule_counts[e['rule']] = rule_counts.get(e['rule'], 0) + 1
        for w in vd['warnings']:
            warn_counts[w['rule']] = warn_counts.get(w['rule'], 0) + 1
        for b in vd['stats']['builtins_used']:
            builtins[b] = builtins.get(b, 0) + 1
        for a in vd['stats']['attributes_used']:
            attrs[a] = attrs.get(a, 0) + 1
        for n in vd['stats']['nodes']:
            nodes[n] = nodes.get(n, 0) + 1
    summary = {}
    for k, s in groups.items():
        summary[k] = dict(s, unique_codes=len(s['unique_codes']), unique_rejected=len(s['unique_rejected']),
                          unique_rejected_template=len(s['unique_rejected_template']))
    result = {
        'validator_version': v.VERSION, 'validator_sha256': sha_file(v.__file__),
        'script_sha256': sha_file(__file__), 'python': sys.version.split()[0],
        'seconds': time.time() - t0,
        'unique_codes_checked': len(verdicts),
        'unique_codes_rejected': sum(not x['ok'] for x in verdicts.values()),
        'unique_codes_rejected_template_policy': sum(not x['template_ok'] for x in verdicts.values()),
        'unique_codes_template_wrapped': sum(x['template_wrapper_detected'] for x in verdicts.values()),
        'error_rule_counts_over_unique_codes': dict(sorted(rule_counts.items())),
        'warning_rule_counts_over_unique_codes': dict(sorted(warn_counts.items())),
        'builtins_used_by_unique_codes': dict(sorted(builtins.items())),
        'attributes_used_by_unique_codes': dict(sorted(attrs.items())),
        'node_types_used_by_unique_codes': dict(sorted(nodes.items())),
        'summary_by_group': summary,
        'files': files,
        'verdicts': sorted(verdicts.values(), key=lambda x: (x['ok'], x['code_sha256'])),
    }
    Path(out_path).write_text(json.dumps(result, indent=1) + '\n')
    print(json.dumps({k: result[k] for k in ('unique_codes_checked', 'unique_codes_rejected',
                                               'unique_codes_rejected_template_policy', 'unique_codes_template_wrapped',
                                               'error_rule_counts_over_unique_codes',
                                               'warning_rule_counts_over_unique_codes',
                                               'builtins_used_by_unique_codes', 'summary_by_group')}, indent=1))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'static_check_result.json')
