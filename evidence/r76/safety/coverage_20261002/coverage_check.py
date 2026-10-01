#!/usr/bin/env python3
"""R76 track E coverage extension (2026-10-02): static check of the syntheses the 2026-09-29 check did not cover.

Runs the UNCHANGED V2 validator (safety/v2/pyexec_validator.py) under both policies
(pre-registered `strict`, post-hoc `template`) with the UNCHANGED V2 extraction/verdict code
(safety/v2/static_check.py: extract_codes / with_lines / ops_in, same per-code verdict
record). Both files are imported from safety/v2 (sha-checked below); nothing there is modified
(run with python3 -B so no bytecode is written next to them).

New scope (all read-only):
  * R73 g1: the 16 repeated greedy (T=0.0) syntheses train_k/<D>/t0.0_k1 (diagnostic repeat; QG skipped by design,
    the raw replay plan is what heldout_eval.py deploys for g1).
  * R76-C variance: 40 rerun syntheses (HPC/Hadoop/HDFS/Spark x r2,r3 x c0..c4) + gated plans + pool plans +
    the 8 published plans.
  * R76-G unseen: 20 syntheses (NASA/ClarkNet/USask/Calgary x c0..c4) + gated + pool + 4 published plans.
  * R76-B2 library: hand-written rule-library programs (no LLM): 16 replay plans, 16 gated plans, 16 publications.
    Reported separately from LLM-written code.
Supplementary groups (not in the 09-29 scope type list; reported separately): the code embedded in each synthesis'
training archive metadata (training_work/<D>/compressed/metadata.json) and every gate/pool evaluation plan
(eval_*/plan.json). The llm_caches directories are NOT read (they hold raw API records).

For every violation the origin is classified (the validator itself is not changed):
  runtime_fixed_wrapper     inside the appended frozen space-layout wrapper (pinned template, sha f1389aec...)
  runtime_entrypoint_rename `def _orig_forward(` / `def _orig_inverse(` in the prefix: the runtime's
                            deterministic rename of the LLM's forward/inverse (semzip_pure.py:924-929)
  program_body              code written by the LLM (or, for the library groups, by hand)
Output: OUT_JSON (default coverage_check_result.json) + violations_verbatim.txt next to it.
"""
import glob
import hashlib
import json
import sys
import time
from pathlib import Path

V2 = Path('<WORKDIR>/r76_additional_20260929/safety/v2')
sys.path.insert(0, str(V2))
import pyexec_validator as v  # noqa: E402
import static_check as sc  # noqa: E402

EXPECTED = {'pyexec_validator.py': '676fe26bd8c017fd74aa51b83221c81d341189740e85194ba283df2312447060',
            'static_check.py': 'a1d9fdeaa2b58d946179650eae9f36c27c1e2e0d6ee6ab56fa827edd0fd84fc3'}  # SHA-256 of ../v2/*.py as published in this copy (as-run values not listed)
for mod in (v, sc):
    got = sc.sha_file(mod.__file__)
    if Path(mod.__file__).resolve().parent != V2.resolve() or got != EXPECTED[Path(mod.__file__).name]:
        raise SystemExit('unexpected validator/static_check file: %s %s' % (mod.__file__, got))

ROOT = Path('<WORKDIR>')
R73 = ROOT / 'r73_quality_gate_20260927/r73_qg'
R76 = ROOT / 'r76_additional_20260929'
EARLIER = V2 / 'static_check_result.json'
LLM_DATASETS = sc.DATASETS


def g(pattern):
    return sorted(glob.glob(str(pattern)))


def synth_sources(prefix, tk_glob, label_root):
    """training.json -> its replay plan (+ the training-archive metadata, supplementary)."""
    out, meta = [], []
    for tj in g(tk_glob):
        tj = Path(tj)
        r = json.loads(tj.read_text())
        rp = Path(str(r.get('replay_plan') or ''))
        note = {'synthesis': str(tj.parent.relative_to(label_root)), 'training_status': r.get('status'),
                'api_calls': r.get('api_calls'), 'temperature': r.get('temperature')}
        out.append((prefix + '_synthesis_replay_plan', rp, note))
        for m in g(tj.parent / 'training_work/*/compressed/metadata.json'):
            meta.append((prefix + '_synthesis_training_archive_metadata[supp]', Path(m), note))
    return out, meta


def sources():
    main, supp = [], []
    # R73 g1 = train_k/<D>/t0.0_k1 (heldout_eval.cand_raw_plan: cid g1 -> t0.0_k1 replay_plan)
    a, b = synth_sources('R73_g1', R73 / 'train_k/*/t0.0_k1/training.json', R73)
    main += a
    supp += b
    for track, base, reps in (('R76C_variance', R76 / 'variance/runs', 'r[23]'), ('R76G_unseen', R76 / 'unseen/runs', 'r1')):
        for rep in g(base / '*' / reps):
            rep = Path(rep)
            d = rep.parent.name
            lab = '%s/%s' % (d, rep.name)
            a, b = synth_sources(track, rep / 'train_k' / d / 't*_k*' / 'training.json', base)
            main += a
            supp += b
            for p in g(rep / 'runs/qg/c[0-9]' / d / 'selected_plan.json'):
                main.append((track + '_gated_candidate_plan', Path(p), {'repeat': lab, 'cand': Path(p).parts[-3]}))
            main.append((track + '_pool_selected_plan', rep / 'runs/pool' / d / 'selected_plan.json', {'repeat': lab}))
            main.append((track + '_publication', rep / 'runs/publish/pool' / d / 'program.json', {'repeat': lab}))
            main.append((track + '_publication_storage', rep / 'runs/publish/pool' / d / 'storage/storage.json', {'repeat': lab}))
            for p in g(rep / 'runs/qg/c[0-9]' / d / 'eval_*/plan.json') + g(rep / 'runs/pool' / d / 'eval_*/plan.json'):
                supp.append((track + '_gate_or_pool_eval_plan[supp]', Path(p), {'repeat': lab}))
    lib = R76 / 'library'
    a, b = synth_sources('R76B2_library(hand-written)', lib / 'train_lib/*/training.json', lib)
    main += a
    supp += b
    for d in LLM_DATASETS:
        main.append(('R76B2_library(hand-written)_gated_plan', lib / 'runs/qg/lib' / d / 'selected_plan.json', {'dataset': d}))
        main.append(('R76B2_library(hand-written)_publication', lib / 'runs/publish/lib' / d / 'program.json', {'dataset': d}))
        main.append(('R76B2_library(hand-written)_publication_storage', lib / 'runs/publish/lib' / d / 'storage/storage.json', {'dataset': d}))
        for p in g(lib / 'runs/qg/lib' / d / 'eval_*/plan.json'):
            supp.append(('R76B2_library(hand-written)_gate_eval_plan[supp]', Path(p), {'dataset': d}))
    return [('main',) + s for s in main] + [('supplementary',) + s for s in supp]


def origin_of(code, err):
    """Classify where a violation sits (strict check of the full code string)."""
    prefix = v.split_space_layout(code)
    if prefix is None:
        return 'program_body'
    body = code.rstrip()
    raw_prefix_len = len(body) - len(v.load_space_layout_template().rstrip())
    ln, col = err.get('line'), err.get('col')
    if isinstance(ln, int) and ln >= 1:
        lines = code.split('\n')
        off = sum(len(x) + 1 for x in lines[:ln - 1]) + (col or 0)
        if off >= raw_prefix_len:
            return 'runtime_fixed_wrapper'
    if err['rule'] == 'E_UNDERSCORE' and err['detail'] in ("definition name '_orig_forward'", "definition name '_orig_inverse'"):
        return 'runtime_entrypoint_rename'
    return 'program_body'


def is_library_group(group):
    return group.startswith('R76B2_library')


def main(out_path):
    t0 = time.time()
    verdicts, files = {}, []
    for scope, group, path, note in sources():
        rec = {'scope': scope, 'group': group, 'path': str(path), 'note': note}
        if not path.is_file():
            rec['status'] = 'MISSING'
            files.append(rec)
            continue
        data = json.loads(path.read_text())
        rec['file_sha256'] = sc.sha_file(path)
        rec['spec_count'] = len(data.get('specs', [])) if isinstance(data, dict) else None
        rec['ops'] = sc.ops_in(data, {})
        codes = []
        for jpath, kind, code, op in sc.extract_codes(data):
            key = v.code_sha256(code) + ':' + kind
            if key not in verdicts:
                r = v.check(code, kind)
                rt = v.check_policy(code, kind, 'template')
                errs = sc.with_lines(code, r['errors'])
                for e in errs:
                    e['origin'] = origin_of(code, e)
                warns = sc.with_lines(code, r['warnings'])
                for w in warns:
                    w['origin'] = origin_of(code, w)
                verdicts[key] = {'code_sha256': v.code_sha256(code), 'kind': kind, 'ok': r['ok'],
                                 'errors': errs, 'warnings': warns,
                                 'template_ok': rt['ok'], 'template_wrapper_detected': 'template' in rt,
                                 'template_errors': rt['errors'] if 'template' in rt else None,
                                 'template_warnings': rt['warnings'] if 'template' in rt else None,
                                 'mentions_wrapper_identifiers': [t for t in v.TEMPLATE_IDENTIFIERS if t in code],
                                 'stats': r['stats'], 'code': code, 'groups': set(), 'scopes': set(),
                                 'author': set()}
            vd = verdicts[key]
            vd['groups'].add(group)
            vd['scopes'].add(scope)
            vd['author'].add('hand-written library' if is_library_group(group) else 'LLM (+runtime wrapper if wrapped)')
            codes.append({'json_path': jpath, 'kind': kind, 'op': op, 'code_sha256': vd['code_sha256'],
                          'ok': vd['ok'], 'template_ok': vd['template_ok']})
        rec['codes'] = codes
        rec['code_count'] = len(codes)
        rec['rejected_code_count'] = sum(not c['ok'] for c in codes)
        rec['rejected_code_count_template'] = sum(not c['template_ok'] for c in codes)
        rec['status'] = 'PASS' if not rec['rejected_code_count'] else 'REJECTED'
        rec['status_template'] = 'PASS' if not rec['rejected_code_count_template'] else 'REJECTED'
        files.append(rec)

    # earlier (2026-09-29, V2) result: 89 unique codes
    earlier = json.loads(EARLIER.read_text())
    earlier_keys = {x['code_sha256'] + ':' + x['kind'] for x in earlier['verdicts']}
    earlier_shas = {x['code_sha256'] for x in earlier['verdicts']}

    groups = {}
    for rec in files:
        s = groups.setdefault(rec['group'], {'scope': rec['scope'], 'files': 0, 'missing': 0, 'files_with_code': 0,
                                             'files_rejected': 0, 'files_rejected_template': 0,
                                             'code_occurrences': 0, 'rejected_occurrences': 0,
                                             'rejected_occurrences_template': 0,
                                             'u': set(), 'ur': set(), 'urt': set(), 'ops': {}})
        s['files'] += 1
        if rec['status'] == 'MISSING':
            s['missing'] += 1
            continue
        s['files_with_code'] += bool(rec['code_count'])
        s['files_rejected'] += rec['status'] == 'REJECTED'
        s['files_rejected_template'] += rec['status_template'] == 'REJECTED'
        s['code_occurrences'] += rec['code_count']
        s['rejected_occurrences'] += rec['rejected_code_count']
        s['rejected_occurrences_template'] += rec['rejected_code_count_template']
        for c in rec['codes']:
            k = c['code_sha256'] + ':' + c['kind']
            s['u'].add(k)
            if not c['ok']:
                s['ur'].add(k)
            if not c['template_ok']:
                s['urt'].add(k)
        for op, n in rec['ops'].items():
            s['ops'][op] = s['ops'].get(op, 0) + n
    summary = {}
    for k, s in groups.items():
        kinds = {}
        for key in s['u']:
            kinds[key.split(':')[1]] = kinds.get(key.split(':')[1], 0) + 1
        summary[k] = {'scope': s['scope'], 'files': s['files'], 'missing': s['missing'],
                      'files_with_code': s['files_with_code'], 'code_occurrences': s['code_occurrences'],
                      'unique_codes': len(s['u']), 'unique_by_kind': kinds,
                      'unique_already_in_earlier_89': len(s['u'] & earlier_keys),
                      'strict_files_rejected': s['files_rejected'], 'strict_unique_rejected': len(s['ur']),
                      'strict_rejected_occurrences': s['rejected_occurrences'],
                      'template_files_rejected': s['files_rejected_template'],
                      'template_unique_rejected': len(s['urt']),
                      'template_rejected_occurrences': s['rejected_occurrences_template'],
                      'strict_unique_rejected_shas': sorted(x.split(':')[0] for x in s['ur']),
                      'template_unique_rejected_shas': sorted(x.split(':')[0] for x in s['urt']),
                      'ops': s['ops']}

    def tally(keys):
        vs = [verdicts[k] for k in keys]
        origin_counts, rule_counts, warn_origin = {}, {}, {}
        for vd in vs:
            for e in vd['errors']:
                origin_counts[e['origin']] = origin_counts.get(e['origin'], 0) + 1
                rule_counts[e['rule']] = rule_counts.get(e['rule'], 0) + 1
            for w in vd['warnings']:
                kk = w['rule'] + '@' + w['origin']
                warn_origin[kk] = warn_origin.get(kk, 0) + 1
        return {'unique_codes': len(vs),
                'by_kind': {kd: sum(x['kind'] == kd for x in vs) for kd in ('python_exec', 'context')},
                'strict_accept': sum(x['ok'] for x in vs), 'strict_reject': sum(not x['ok'] for x in vs),
                'template_accept': sum(x['template_ok'] for x in vs), 'template_reject': sum(not x['template_ok'] for x in vs),
                'template_wrapped': sum(x['template_wrapper_detected'] for x in vs),
                'wrapper_identifiers_but_not_pinned_template': sum(bool(x['mentions_wrapper_identifiers']) and not x['template_wrapper_detected'] for x in vs),
                'strict_violations_total': sum(len(x['errors']) for x in vs),
                'strict_violations_by_origin': origin_counts, 'strict_violations_by_rule': rule_counts,
                'template_violations_total': sum(len(x['template_errors'] if x['template_wrapper_detected'] else x['errors']) for x in vs),
                'warnings_by_rule_and_origin': warn_origin,
                'already_in_earlier_89': sum((x['code_sha256'] + ':' + x['kind']) in earlier_keys for x in vs)}

    def keys_where(pred):
        return {k for k, x in verdicts.items() if pred(x)}

    main_keys = keys_where(lambda x: 'main' in x['scopes'])
    supp_only = keys_where(lambda x: 'main' not in x['scopes'])
    llm_keys = keys_where(lambda x: any(not is_library_group(gr) for gr in x['groups']))
    lib_keys = keys_where(lambda x: any(is_library_group(gr) for gr in x['groups']))
    per_source = {}
    for name, pref in (('R73_g1', 'R73_g1'), ('R76C_variance', 'R76C_variance'), ('R76G_unseen', 'R76G_unseen'),
                       ('R76B2_library(hand-written)', 'R76B2_library')):
        ks_main = keys_where(lambda x, p=pref: any(gr.startswith(p) and not gr.endswith('[supp]') for gr in x['groups']))
        ks_all = keys_where(lambda x, p=pref: any(gr.startswith(p) for gr in x['groups']))
        per_source[name] = {'main_scope': tally(ks_main), 'main_plus_supplementary': tally(ks_all),
                            'supplementary_only_codes': len(ks_all - ks_main)}
    new_llm_main = llm_keys & main_keys
    new_all = set(verdicts)
    grand = {
        'earlier_89_keys': len(earlier_keys),
        'new_unique_codes_all_groups': len(new_all),
        'new_unique_not_in_earlier': len(new_all - earlier_keys),
        'grand_total_unique_sha_kind_all': len(earlier_keys | new_all),
        'grand_total_unique_sha_all': len(earlier_shas | {k.split(':')[0] for k in new_all}),
        'grand_total_LLM_only_main_scope(earlier89 + new LLM main)': len(earlier_keys | new_llm_main),
        'grand_total_LLM_only_incl_supplementary': len(earlier_keys | llm_keys),
        'grand_total_main_scope_incl_library': len(earlier_keys | main_keys),
        'library_unique_codes': len(lib_keys),
        'library_codes_also_in_LLM_groups': len(lib_keys & llm_keys),
        'supplementary_only_codes': len(supp_only),
        'supplementary_only_codes_list': sorted(supp_only),
    }
    totals = {'all_new': tally(new_all), 'LLM_main': tally(new_llm_main), 'LLM_all': tally(llm_keys),
              'library_all': tally(lib_keys), 'new_not_in_earlier_89': tally(new_all - earlier_keys)}

    out_verdicts = []
    for key, vd in verdicts.items():
        x = dict(vd)
        x['groups'] = sorted(vd['groups'])
        x['scopes'] = sorted(vd['scopes'])
        x['author'] = sorted(vd['author'])
        x['in_earlier_89'] = key in earlier_keys
        out_verdicts.append(x)
    out_verdicts.sort(key=lambda x: (x['ok'], x['template_ok'], x['code_sha256']))
    result = {
        'what': 'R76 track E coverage extension 2026-10-02: unchanged V2 validator, strict + template',
        'validator_version': v.VERSION, 'validator_sha256': sc.sha_file(v.__file__),
        'static_check_module_sha256': sc.sha_file(sc.__file__), 'script_sha256': sc.sha_file(__file__),
        'template_sha256': v.SPACE_LAYOUT_TEMPLATE_SHA256, 'python': sys.version.split()[0],
        'earlier_result': {'path': str(EARLIER), 'sha256': sc.sha_file(EARLIER),
                           'unique_codes_checked': earlier['unique_codes_checked']},
        'seconds': time.time() - t0, 'files_read': len(files),
        'files_missing': [f['path'] for f in files if f.get('status') == 'MISSING'],
        'per_source': per_source, 'totals': totals, 'grand_total': grand,
        'summary_by_group': summary, 'files': files, 'verdicts': out_verdicts,
    }
    Path(out_path).write_text(json.dumps(result, indent=1) + '\n')

    # verbatim violation listing
    lines = ['# every strict / template violation of every unique code (V2 validator, unchanged).',
             '# columns: rule | origin | detail | line:col | offending source line', '']
    for x in out_verdicts:
        if x['ok'] and x['template_ok'] and not x['warnings']:
            continue
        lines.append('== code %s kind=%s strict=%s template=%s wrapped=%s author=%s in_earlier_89=%s' % (
            x['code_sha256'], x['kind'], 'ACCEPT' if x['ok'] else 'REJECT', 'ACCEPT' if x['template_ok'] else 'REJECT',
            x['template_wrapper_detected'], '/'.join(x['author']), x['in_earlier_89']))
        lines.append('   groups: ' + ', '.join(x['groups']))
        for e in x['errors']:
            lines.append('   STRICT  %s | %s | %s | %s:%s | %s' % (e['rule'], e['origin'], e['detail'], e['line'], e['col'], e['source_line']))
        if x['template_wrapper_detected']:
            for e in x['template_errors'] or []:
                lines.append('   TEMPLATE(prefix) %s | %s | %s:%s' % (e['rule'], e['detail'], e['line'], e['col']))
        elif not x['template_ok']:
            lines.append('   TEMPLATE: no pinned wrapper -> strict rules apply to the whole code (same errors as above)')
        for w in x['warnings']:
            lines.append('   warn    %s | %s | %s | %s:%s | %s' % (w['rule'], w['origin'], w['detail'], w['line'], w['col'], w['source_line']))
        lines.append('')
    (Path(out_path).parent / 'violations_verbatim.txt').write_text('\n'.join(lines) + '\n')
    print(json.dumps({'per_source': per_source, 'totals': totals,
                      'grand_total': {k: val for k, val in grand.items() if k != 'supplementary_only_codes_list'},
                      'files_missing': result['files_missing'], 'seconds': round(result['seconds'], 1)}, indent=1))


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'coverage_check_result.json')
