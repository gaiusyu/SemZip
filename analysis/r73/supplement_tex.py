"""Supplement: every synthesis, gate, pool and held-out observation (from ../dev_results). Writes paper_out/supp_selection.tex"""
import json
from pathlib import Path
H = Path(__file__).resolve().parent; D = H.parent/'dev_results'; OUT = H/'paper_out'; OUT.mkdir(exist_ok=True)
DS = 'Linux,Proxifier,Apache,Zookeeper,Mac,HealthApp,HPC,Hadoop,OpenStack,OpenSSH,Android,BGL,HDFS,Spark,Windows,Thunderbird'.split(',')
C = ['c0', 'c1', 'c2', 'c3', 'c4', 'g1']
def J(p):
    p = D/p
    return json.loads(p.read_text()) if p.exists() else None
R69_CALLS = {'Linux': 17, 'Proxifier': 3, 'Apache': 16, 'Zookeeper': 17, 'Mac': 28, 'HealthApp': 4, 'HPC': 16, 'Hadoop': 17, 'OpenStack': 11,
             'OpenSSH': 6, 'Android': 9, 'BGL': 8, 'HDFS': 6, 'Spark': 18, 'Windows': 22, 'Thunderbird': 4}  # R69 training_status.json (sum 202)
def tj(d, c):
    if c == 'c0': return {'status': 'PASS (R69)', 'api_calls': R69_CALLS[d]}
    t, k = ('0.7', c[1:]) if c[0] == 'c' else ('0.0', c[1:])
    return J(f'train_k/{d}/t{t}_k{k}/training.json') or {}
L = [r'\begin{longtable}{llrrrrrr}', r'\caption{All syntheses per dataset. Rules: verified rule groups before/after the quality gate. B0: complete block-0 archive bytes (single/gated). HO: held-out sample archive bytes (single/gated); -- for one-block files. c0 is the R69 greedy synthesis (SemZip-1); c1--c4 are temperature-0.7 samples; g1 is a repeated greedy synthesis that is never pooled or gated.}\label{tab:supp-selection}\\',
     r'\toprule', r'Dataset & Synth. & Calls & Rules & B0 single & B0 gated & HO single & HO gated \\', r'\midrule\endfirsthead',
     r'\caption[]{(continued)}\\', r'\toprule', r'Dataset & Synth. & Calls & Rules & B0 single & B0 gated & HO single & HO gated \\', r'\midrule\endhead']
for d in DS:
    for c in C:
        q = J(f'runs/qg/{c}/{d}/qg_report.json'); t = tj(d, c)
        if not q and not t: continue
        rules = '--'
        if q:
            kept = len(set(q['kept_tags'])); removed = len(q['removed_tags'])
            rules = f'{kept + removed}/{kept}'
        hs = J(f'runs/heldout/{c}_raw/{d}/result.json'); hg = J(f'runs/heldout/{c}_qg1/{d}/result.json')
        calls = t.get('api_calls')
        L.append(' & '.join([d if c == 'c0' else '', c, str(calls) if calls is not None else '--', rules,
                             f"{q['original_train_archive_bytes']:,}" if q else '--', f"{q['selected_train_archive_bytes']:,}" if q else '--',
                             f"{hs['archive_bytes']:,}" if hs else '--', f"{hg['archive_bytes']:,}" if hg else '--']) + r' \\')
    p = J(f'runs/pool/{d}/pool_report.json'); hp = J(f'runs/heldout/pool_pool/{d}/result.json')
    if p:
        acc = [a['move'] for r in p['rounds'] if 'accepted' in r and isinstance(r['accepted'], list) for a in r['accepted']]
        L.append(' & '.join(['', 'pool', '--', f"{len(set(p['selected_tags']))}", f"start {p['start']}", f"{p['selected_train_bytes']:,}", '--',
                             f"{hp['archive_bytes']:,}" if hp else '--']) + r' \\')
    L.append(r'\midrule')
L[-1] = r'\bottomrule'; L.append(r'\end{longtable}')
(OUT/'supp_selection.tex').write_text('\n'.join(L).replace('_', r'\_') + '\n')
print('\n'.join(L[:12]))
