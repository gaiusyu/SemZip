#!/usr/bin/env python3
"""R76-B2 report: per dataset library rules admitted after the gate, archive bytes, SHA, and context columns
(empty program, R73 pool main result). Read-only on other tracks. Writes library_report.json and prints a table."""
import json
from pathlib import Path
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
R73 = ROOT/'r73_quality_gate_20260927/r73_qg'
R75 = ROOT/'r75_fallback_evolution_20260927'
ATT = HERE.parent/'attribution'
DS = 'Proxifier,Linux,Apache,Zookeeper,Mac,HealthApp,HPC,Hadoop,OpenStack,OpenSSH,Android,BGL,HDFS,Spark,Windows,Thunderbird'.split(',')
def j(p):
    try: return json.loads(Path(p).read_text())
    except Exception: return None
rows = []
for d in DS:
    r = {'dataset': d}
    t = j(HERE/'train_lib'/d/'training.json')
    if t: r.update(train_status=t.get('status'), api_calls=t.get('api_calls'), library_stats=t.get('library_stats'))
    if t and t.get('replay_plan') and Path(t['replay_plan']).is_file():
        p = j(t['replay_plan']); r.update(candidate_specs=len(p['specs']), candidate_tags=len({s['tag'] for s in p['specs']}),
                                         candidate_types=sorted({s.get('proposal_tag') or '(runtime)' for s in p['specs']}))
    q = j(HERE/'runs/qg/lib'/d/'qg_report.json')
    if q:
        r.update(gate_kept_tags=q['kept_tags'], gate_removed_tags=q['removed_tags'], gate_evaluations=q['evaluations'],
                 block0_bytes_candidate=q['original_train_archive_bytes'], block0_bytes_empty=q['empty_train_archive_bytes'],
                 block0_bytes_selected=q['selected_train_archive_bytes'])
        sp = j(HERE/'runs/qg/lib'/d/'selected_plan.json')
        r.update(rules_after_gate=len({s['tag'] for s in sp['specs']}), specs_after_gate=len(sp['specs']),
                 types_after_gate=sorted({s.get('proposal_tag') or '(runtime)' for s in sp['specs']}))
    f = j(HERE/'runs/formal/lib'/d/'result.json')
    if f:
        e = f['encode']
        r.update(formal_status=f['status'], raw_bytes=e['raw_bytes'], archive_bytes=e['archive_bytes'], ratio=e['compression_ratio'],
                 full_sha_pass=f['independent_materialized_file_sha_pass'], block_sha_checked=len(f['block_audit']),
                 semantic_fallback_blocks=e.get('semantic_fallback_blocks'), suffix_archive_bytes=f['heldout']['archive_bytes'],
                 suffix_ratio=f['heldout']['ratio'], encode_MB_per_s=f['encode_MB_per_s'], decode_MB_per_s=f['decode_MB_per_s'])
    pool = j(R73/'runs/formal/pool'/d/'result.json')
    if pool: r.update(r73_pool_archive_bytes=pool['encode']['archive_bytes'], r73_pool_suffix_bytes=pool['heldout']['archive_bytes'])
    cfb = j(R75/'cfb'/f'{d}.json')
    if cfb: r.update(r75_empty_program_archive_bytes=cfb['empty_archive_bytes'])
    ae = j(ATT/'runs/formal/empty'/d/'result.json')
    if ae: r.update(b1_empty_formal_archive_bytes=ae['encode']['archive_bytes'])
    rows.append(r)
(HERE/'library_report.json').write_text(json.dumps(rows, indent=1))
print(f"{'dataset':12s} {'cand':>4s} {'kept':>4s} {'lib_bytes':>11s} {'ratio':>8s} {'sha':>5s} {'empty(R75)':>11s} {'pool(R73)':>11s} {'lib/pool':>8s} {'lib/empty':>9s}")
for r in rows:
    a = r.get('archive_bytes'); e = r.get('r75_empty_program_archive_bytes'); p = r.get('r73_pool_archive_bytes')
    print(f"{r['dataset']:12s} {str(r.get('candidate_tags','')):>4s} {str(r.get('rules_after_gate','')):>4s} {str(a or ''):>11s} "
          f"{(format(r['ratio'], '.3f') if a else ''):>8s} {str(r.get('full_sha_pass','')):>5s} {str(e or ''):>11s} {str(p or ''):>11s} "
          f"{(format(a/p, '.4f') if a and p else ''):>8s} {(format(a/e, '.4f') if a and e else ''):>9s}")
