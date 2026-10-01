#!/usr/bin/env python3
"""Rebuild the original development directory layout expected by the analysis generators.

The generators in analysis/r73 and analysis/r76 locate their inputs relative to their own file
location (e.g. ``Path(__file__).parent.parent/'dev_results'/...``). This script creates a NEW
directory that reproduces that layout with symbolic links into this repository's evidence/ tree,
and copies the generators into it (copies, because they resolve their own path).

    python3 analysis/stage_layout.py --out ../semzip-analysis-stage
    (or simply: bash analysis/run_all.sh ../semzip-analysis-stage, which stages and runs every generator)

No experiment, model call or compressor is run; only saved JSON records are read.
"""
import argparse, json, os, shutil, sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DS = ['Android', 'Apache', 'BGL', 'HDFS', 'HPC', 'Hadoop', 'HealthApp', 'Linux', 'Mac', 'OpenSSH',
      'OpenStack', 'Proxifier', 'Spark', 'Thunderbird', 'Windows', 'Zookeeper']


def link(src: Path, dst: Path, missing: list):
    if not src.exists():
        missing.append(str(src.relative_to(REPO))); return
    dst.parent.mkdir(parents=True, exist_ok=True)
    os.symlink(src, dst)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True, type=Path, help='new directory (must not exist)')
    a = ap.parse_args()
    out = a.out.resolve(); out.mkdir(parents=True, exist_ok=False)
    miss = []
    sel = REPO/'evidence/selection'; evo = REPO/'evidence/evolution'; r76 = REPO/'evidence/r76'

    # R71 artifact: the repository root *is* the R71 base layout (results/final, results/suffix, ...).
    os.symlink(REPO, out/'artifact_r71_working')
    link(REPO/'evidence/reference/r71_complete_file_reference.json', out/'r71_strict_main_20260923/reference.json', miss)
    link(REPO/'evidence/reference/published_baseline_reference_20260915.json', out/'results/published_baseline_reference_20260915.json', miss)

    # R73 (main result: syntheses, gate, pooled selection, formal runs, timing)
    q = out/'r73_quality_gate_20260927/dev_results'
    for v in ('pool', 'qg1c0'):
        for d in DS:
            p = sel/'results/formal'/v/f'{d}.json'
            if p.exists(): link(p, q/'runs/formal'/v/d/'result.json', miss)
    for c in sorted(x.name for x in (sel/'selection/gate').iterdir()):
        for d in DS:
            for f in ('qg_report.json', 'selected_plan.json', 'CANDIDATE_UNUSABLE.json'):
                p = sel/'selection/gate'/c/d/f
                if p.exists(): link(p, q/'runs/qg'/c/d/f, miss)
    for d in DS:
        for f in ('pool_report.json', 'candidates.json', 'selected_plan.json'):
            p = sel/'selection/pool'/d/f
            if p.exists(): link(p, q/'runs/pool'/d/f, miss)
    for cv in sorted(x.name for x in (sel/'results/heldout').iterdir()):
        for d in DS:
            p = sel/'results/heldout'/cv/f'{d}.json'
            if p.exists(): link(p, q/'runs/heldout'/cv/d/'result.json', miss)
    for p in sorted((sel/'results/heldout_samples').glob('*.sample.json')):
        link(p, q/'heldout'/p.name, miss)
    for d in DS:
        base = sel/'syntheses'/d
        if base.is_dir():
            for t in sorted(base.iterdir()):
                if (t/'training.json').exists(): link(t/'training.json', q/'train_k'/d/t.name/'training.json', miss)
    link(sel/'results/COST.json', q/'runs/COST.json', miss)
    link(sel/'results/SUMMARY.json', q/'runs/SUMMARY.json', miss)
    link(sel/'supplementary/timing_SUMMARY.json', q/'runs/timing/SUMMARY.json', miss)
    for p in sorted((sel/'results/timing').glob('*.json')):
        link(p, q/'runs/timing'/p.stem/'result.json', miss)
    for d in DS:
        p = REPO/'deployments/main_pool'/d/'publication.json'
        link(p, q/'runs/publish/pool'/d/'publication.json', miss)
        p = sel/'supplementary/publish_qg1c0'/d/'publication.json'
        if p.exists(): link(p, q/'runs/publish/qg1c0'/d/'publication.json', miss)

    # R75 (online evolution)
    e = out/'r75_fallback_evolution_20260927/dev_results'
    link(evo/'R75_REPORT.json', e/'R75_REPORT.json', miss)
    for d in DS:
        link(evo/'fallback_measurement'/f'{d}.json', e/'cfb'/f'{d}.json', miss)
        p = evo/'fallback_measurement/assembled'/d/'assemble_result.json'
        if p.exists(): link(p, e/'asm_cfb'/d/'assemble_result.json', miss)
    for name, sub, dst in (('Thunderbird', 'thunderbird', e), ('Spark', 'spark', e/'spark_run')):
        base = evo/sub
        link(base/'state_full.json', dst/'state_full.json', miss)
        for u in sorted((base/'updates').iterdir()):
            link(u/'update_result.json', dst/'upd'/u.name/'update_result.json', miss)
        link(base/'asm_frozen_empty/assemble_result.json', e/'asm_frozen_empty'/name/'assemble_result.json', miss)
        if (base/'asm_full/assemble_result.json').exists():
            link(base/'asm_full/assemble_result.json', dst/'asm_full/assemble_result.json', miss)
    link(evo/'thunderbird/p1_block0.json', e/'p1_block0.json', miss)
    link(evo/'spark/update_row_share.json', e/'spark_run/update_row_share.json', miss)

    # R76 (additional baselines and controls)
    r = out/'r76_additional_20260929'
    for t in sorted(x.name for x in r76.iterdir() if x.is_dir()):
        link(r76/t, r/'dev_results/r76'/t, miss)
    link(r76/'attribution/empty_summary.json', r/'dev_results/empty/empty_summary.json', miss)

    # generators (copied, not linked) and recorded intermediate outputs that need raw logs to regenerate
    for sub, dst in (('r73', out/'r73_quality_gate_20260927/analysis'), ('r76', r/'analysis')):
        dst.mkdir(parents=True, exist_ok=True)
        for p in sorted((REPO/'analysis'/sub).glob('*.py')):
            shutil.copy2(p, dst/p.name)
        (dst/'paper_out').mkdir(exist_ok=True)
    # skeleton_coverage.json needs the raw LogHub block-0 files (skeleton_coverage.py RAW_DIR); seed the recorded one
    shutil.copy2(REPO/'analysis/reference_outputs/r73/skeleton_coverage.json',
                 out/'r73_quality_gate_20260927/analysis/paper_out/skeleton_coverage.json')
    (out/'STAGE.json').write_text(json.dumps({'repository': str(REPO), 'missing_inputs': miss}, indent=1) + '\n')
    print(json.dumps({'stage': str(out), 'missing_inputs': miss}, indent=1))


if __name__ == '__main__':
    main()
