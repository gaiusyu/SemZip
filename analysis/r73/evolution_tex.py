"""R75 online evolution: RQ4 paragraph, supplement section, and per-dataset SemZip+Evo bytes for the main table.
Inputs : ../../r75_fallback_evolution_20260927/dev_results/ (R75_REPORT.json, state files, update records, cfb per-block payloads,
         asm_cfb assembled per-block-minimum archives of non-triggered files, p1_block0.json)
Outputs: paper_out/evolution_rq4_text.tex, paper_out/evolution_supp.tex, paper_out/evo_numbers.json"""
import json
from pathlib import Path
H = Path(__file__).resolve().parent; OUT = H/'paper_out'; OUT.mkdir(exist_ok=True)
E = H.parent.parent/'r75_fallback_evolution_20260927'/'dev_results'
DS = 'Linux,Proxifier,Apache,Zookeeper,Mac,HealthApp,HPC,Hadoop,OpenStack,OpenSSH,Android,BGL,HDFS,Spark,Windows,Thunderbird'.split(',')  # main-table order
LOOK, NEED, MINBLK, LOSS, GAP = 5, 3, 3, 0.01, 10  # pre-registered constants (evo.py)
ELIG = GAP + 3  # earliest trigger at block 10, plus validation (b+1) and one served block (b+2)
def J(p): return json.loads(Path(p).read_text())
WORD = {2: 'two', 3: 'three', 4: 'four', 5: 'five', 6: 'six', 7: 'seven', 8: 'eight'}
def pct(v): return f'{v:+.2f}'.replace('-', '$-$') + r'\%'
REP = J(E/'R75_REPORT.json')
RUNS = {'Thunderbird': E, 'Spark': E/'spark_run'}

def replay(rows):
    """First trigger block of the frozen deployment under the pre-registered rule (bank = {p0}, gap from block 0)."""
    loss = [r['empty'] < r['pool'] or r['pool'] > (1 - LOSS)*r['empty'] for r in rows]
    n = len(rows); first = None
    for b in range(1, n):
        win = list(range(max(1, b - LOOK + 1), b + 1))
        if len(win) >= MINBLK and sum(loss[k] for k in win) >= NEED and b >= GAP and b + 2 <= n - 1:
            first = b; break
    return sum(loss[1:]), first

cfb = {d: J(E/'cfb'/f'{d}.json') for d in DS}
trig = {d: replay(sorted(cfb[d]['rows'], key=lambda r: r['index'])) for d in DS}
elig = [d for d in DS if cfb[d]['blocks'] >= ELIG]; short = [d for d in DS if cfb[d]['blocks'] < ELIG]
fired = [d for d in DS if trig[d][1] is not None]
assert sorted(fired) == sorted(RUNS) and all(d in elig for d in fired)
attempts = {}
for d, base in RUNS.items():
    st = J(base/'state_full.json'); rows = []
    for a in st['attempts']:
        u = J(base/'upd'/f"b{a['block']:03d}"/'update_result.json')
        calls = sum((v.get('api_calls') or 0) for v in u['syntheses'].values())
        sec = u.get('total_update_seconds') or sum(u.get(k, 0) for k in ('synthesis_seconds', 'gate_seconds', 'pool_seconds'))
        rows.append({'block': a['block'], 'calls': calls, 'seconds': sec, 'cand': a.get('candidate_total'), 'cur': a.get('current_min_total'),
                     'passed': bool(a.get('passed')), 'from': a.get('effective_from')})
    attempts[d] = rows

# ---- SemZip+Evo archive bytes: every block stores min over published programs and the empty program ----
evo_bytes, evo_src = {}, {}
for d in DS:
    if d in RUNS:
        a = REP[d]['assembled']; assert a['sha_pass']; evo_bytes[d] = a['archive_bytes']; evo_src[d] = 'evolution run'
    elif cfb[d]['empty_wins']:
        a = J(E/'asm_cfb'/d/'assemble_result.json'); assert a['sha_pass'] and a['archive_bytes'] == cfb[d]['min_archive_bytes']
        evo_bytes[d] = a['archive_bytes']; evo_src[d] = 'per-block program-free choice'
    else:
        evo_bytes[d] = cfb[d]['pool_archive_bytes']; evo_src[d] = 'identical to frozen'
(OUT/'evo_numbers.json').write_text(json.dumps({'evo_archive_bytes': evo_bytes, 'source': evo_src, 'triggered': fired,
                                                 'eligible': elig}, indent=1) + '\n')

T, S = REP['Thunderbird'], REP['Spark']; ta, sa = attempts['Thunderbird'], attempts['Spark']
tv, sv = ta[0], sa[0]
b0 = J(E/'p1_block0.json'); b0_gain = 100*(1 - b0['block0_p1_total']/b0['block0_p0_total'])
FE = {d: J(E/'asm_frozen_empty'/d/'assemble_result.json') for d in RUNS}
for d in RUNS: assert FE[d]['sha_pass'] and FE[d]['archive_bytes'] == REP[d]['bytes']['frozen_plus_fallback'], d
UR = {int(k): v for k, v in J(E/'spark_run'/'update_row_share.json')['update_row_share_by_block'].items()}
ur_att = [100*UR[a['block']] for a in attempts['Spark'][1:]]; ur_hi = sorted(b for b, v in UR.items() if v >= 0.9)
FORMAL = H.parent/'dev_results'/'runs'/'formal'/'pool'
FROZ_ENC = {d: J(FORMAL/d/'result.json')['encode']['online_compression_seconds'] for d in RUNS}
later = T['blocks'] - T['future_from_block']; chosen_new = T['choices']['p1']
rq4 = (
 r"\paragraph{Online evolution.} We designed Step 3 after the main comparison, in which Thunderbird is the only loss to DeLog, and after two weaker single-synthesis protocols "
 r"(no publication in nine append-only pilots; one in six triggered replacement attempts). "
 r"Its constants were recorded before any evolution run; we screened it on Thunderbird blocks 0--99, extended it to the full file by a prespecified rule, and then applied it unchanged to Spark. "
 f"It can fire only on the {WORD[len(elig)]} files with at least {ELIG} blocks and fires on Thunderbird (block {tv['block']}) and Spark (block {sv['block']}). "
 f"On Thunderbird, one update ({tv['calls']} LLM calls, {tv['seconds']/60:.0f} minutes) publishes a program with epoch-seconds and time-of-day rules that none of the five pooled block-0 syntheses proposed, is "
 f"{100*(1 - tv['cand']/tv['cur']):.1f}\\% smaller on validation block {tv['block'] + 1}, and raises the file from "
 f"{T['ratio']['pool_frozen']:.2f}$\\times$ to {T['ratio']['evolution_bankmin']:.2f}$\\times$ (DeLog {T['ratio']['delog']:.2f}$\\times$). "
 f"On Spark, the first attempt publishes; {WORD[len(sa) - 1]} later ones train on non-Spark \\texttt{{Update row}} blocks and yield empty candidates "
 f"({S['ratio']['pool_frozen']:.2f}$\\times$ to {S['ratio']['evolution_bankmin']:.2f}$\\times$). "
 f"A per-block program-free choice alone gives {T['ratio']['frozen_plus_fallback']:.2f}$\\times$ and {S['ratio']['frozen_plus_fallback']:.2f}$\\times$. "
 f"Mixed-program archives decode archive-only with matching SHA-256. The new Thunderbird program is also {b0_gain:.1f}\\% smaller on block 0 itself: the gain mainly reflects synthesis variability (a never-pooled greedy repeat on block 0 also proposed a time rule), not format drift."
)
(OUT/'evolution_rq4_text.tex').write_text(rq4 + '\n')

# ---- supplement ----
def longtable(spec, caption, label, header, rows):
    return ([r'\begin{longtable}{' + spec + '}', r'\caption{' + caption + r'}\label{' + label + r'}\\', r'\toprule', header + r' \\', r'\midrule\endfirsthead',
             r'\caption[]{(continued)}\\', r'\toprule', header + r' \\', r'\midrule\endhead'] + rows + [r'\bottomrule', r'\end{longtable}'])
L = [r'\section{Online evolution: complete-file records}\label{sec:supp-evolution}',
     r"\textbf{Chronology.} The protocol was designed after the main comparison (Thunderbird was the only file larger than DeLog's) and after the historical single-synthesis cohorts below. "
     r'Its numeric constants were recorded in a design note before any run; three implementation clarifications (trigger window from block 1, publication-gap anchoring, and requiring a served block) were added after the program-free measurements and before any evolution run; nothing was changed afterwards. It was first screened on Thunderbird blocks 0--99; the prespecified rule extended it to the full file '
     r'when blocks after the first publication shrank by at least 1\% relative to the frozen deployment with the program-free choice (they shrank by 22.2\%). '
     r'Running it unchanged on Spark was decided after that window result and before any Spark update; the other files were decided from the trigger replay below.',
     r'\par\textbf{Protocol.} A block is a \emph{loss} when the best published program is not at least 1\% smaller than the empty program (residual processing only) on that block. '
     r'An update triggers when at least 3 of the last 5 completed blocks (from block 1) are losses, at least 10 blocks after the latest publication (the initial deployment counts as block 0; an update counts from the block it serves first), '
     r'not within a 10-block cooldown after a rejected or failed attempt, and only if block $b+2$ exists, so that a publication serves at least one block (files with fewer than 13 blocks cannot trigger). Each update runs the unchanged training pipeline (one greedy and four temperature-0.7 fresh syntheses, gate, cost-guided selection, storage fit) on the trigger block $b$; '
     r'already published programs are not re-pooled. The candidate is encoded on block $b+1$ through the same semantic-archive guard and must be at least 1\% smaller than the minimum over published programs and the empty program; it serves blocks from $b+2$. '
     r'At most 5 publications and 8 attempts. Every block stores the smallest payload among the published programs and the empty program; blocks are self-describing, and assembled archives are decoded archive-only. '
     r'Time is counted in blocks: synthesis latency is not modeled.']
rows = []
for d in DS:
    c = cfb[d]; ft = ('n/a' if d in short else (str(trig[d][1]) if trig[d][1] is not None else '--'))
    rows.append(f"{d} & {c['blocks']} & {trig[d][0]} & {ft} & {c['pool_ratio']:.2f} & {c['min_ratio']:.2f} & {c['empty_encode_seconds']:,.0f} " + r'\\')
L += longtable('lrrrrrr', r'Trigger replay and program-free choice on every file. Loss blocks: blocks from 1 on where the frozen program is not at least 1\% smaller than the empty program. '
               rf'First trigger: first block at which the rule fires for the frozen deployment; n/a: fewer than {ELIG} blocks, so the rule cannot fire; --: eligible, never fires. '
               r'Ratios: frozen deployment and per-block minimum of frozen and empty program, assembled and decoded archive-only (SHA-256 match) on every file where they differ. Empty s: wall seconds of the full-file empty-program encode (four workers, shared host), the monitoring cost.',
               'tab:supp-evo-replay', r'Dataset & Blocks & Loss blocks & First trigger & Frozen & Frozen+empty & Empty s', rows)
rows = []
for d in ['Thunderbird', 'Spark']:
    for i, a in enumerate(attempts[d]):
        out = f"published from {a['from']}" if a['passed'] else 'rejected'
        rows.append(f"{d if i == 0 else ''} & {a['block']} & {a['calls']} & {a['seconds']:.0f} & {a['cand']:,} & {a['cur']:,} & {out} " + r'\\')
L += longtable('llrrrrl', r'Every update attempt. Bytes: candidate and current minimum (published programs and empty program) on validation block $b+1$. Seconds: synthesis, gate, selection and publication wall time. '
               rf'Spark blocks {ur_hi[0]}--{ur_hi[-1]} consist of at least 90\% non-Spark \texttt{{Update row}} records ({min(ur_att):.1f}--{max(ur_att):.1f}\% on the six rejected attempt blocks). '
               r'The six rejected candidates share a byte-identical empty extraction program with separately fitted storage policies; zero LLM calls means the block yielded no example group eligible for the proposer. '
               "Candidate encodes are cached per bank slot, so the last five reuse the first rejected candidate's validation encode; all six equal the current minimum.",
               'tab:supp-evo-attempts', r'Dataset & Block $b$ & LLM calls & Seconds & Candidate & Current min & Outcome', rows)
def encs(r, kind):
    pub = {f'p{i}' for i in range(1, r['published'] + 1)}
    e = r['extra_encode_seconds_by_plan']
    return e['empty'] if kind == 'empty' else sum(v for k, v in e.items() if k != 'empty' and ((k in pub) == (kind == 'pub')))
rowspec = [('Frozen deployment', lambda r: f"{r['ratio']['pool_frozen']:.2f}"), ('Frozen + empty program', lambda r: f"{r['ratio']['frozen_plus_fallback']:.2f}"),
           ('Evolution', lambda r: f"{r['ratio']['evolution_bankmin']:.2f}"), ('Evolution, newest only', lambda r: f"{r['ratio']['evolution_latest_only']:.2f}"),
           ('DeLog', lambda r: f"{r['ratio']['delog']:.2f}"), ('First served block', lambda r: str(r['future_from_block'])),
           ('Later bytes vs frozen+empty', lambda r: pct(r['future_evo_vs_frozen_fb_pct'])), ('Later bytes vs DeLog', lambda r: pct(r['future_evo_vs_delog_pct'])),
           ('Blocks smaller than DeLog', lambda r: f"{r['blocks_evo_smaller_than_delog']:,}/{r['blocks']:,}"),
           ('Assembled archive bytes', lambda r: f"{r['assembled']['archive_bytes']:,}"),
           ('Program-free encode seconds', lambda r: f"{encs(r, 'empty'):,}"), ('Published-program encode seconds', lambda r: f"{encs(r, 'pub'):,}"),
           ('Rejected-candidate encode seconds', lambda r: f"{encs(r, 'rej'):,}"),
           ('Frozen first-pass encode seconds', lambda r: f"{FROZ_ENC['Thunderbird' if r is T else 'Spark']:,.0f}")]
L += longtable('lrr', r'Complete-file outcomes of the triggered files; the first five rows are compression ratios. Evolution: per-block minimum over published programs and the empty program (assembled archive, archive-only decode, SHA-256 match). '
               r'Newest only: after each publication, only the newest program. Later: blocks from the first served block on. Encode seconds: additional wall time of the evolution runs, summed over the 100-block part encodes, '
               r'on the shared host concurrently with other jobs, not in the serialized timing session; frozen-program payloads are reused from the formal run. '
               rf'The new Thunderbird program is {b0_gain:.1f}\% smaller than the frozen one on block 0 ({b0["block0_p1_total"]:,} vs {b0["block0_p0_total"]:,} bytes).',
               'tab:supp-evo-results', r'Quantity & Thunderbird & Spark', [f"{lab} & {f(T)} & {f(S)} " + r'\\' for lab, f in rowspec])
L += ["A fallback to upstream DeLog chunks was not evaluated as a SemZip variant because it would import DeLog's per-dataset recognizer map. "
      r'The historical single-synthesis evolution cohorts follow unchanged in the next sections.']
(OUT/'evolution_supp.tex').write_text('\n'.join(L) + '\n')
print(rq4)
print('eligible:', elig, 'SemZip+Evo sources:', {d: s for d, s in evo_src.items() if s != 'identical to frozen'})
