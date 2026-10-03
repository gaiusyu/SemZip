"""Evolution curves for RQ5: cumulative compression ratio over blocks, frozen deployment vs. update stage.

Inputs (repository layout):
  evidence/evolution/fallback_measurement/<Dataset>.json   per-block bytes of the frozen deployment ('pool')
  evidence/evolution/<dataset>/state_full.json              per-block bytes chosen by the update stage
  evidence/evolution/R75_REPORT.json                        complete-file ratios (assembled archives, DeLog)
  evidence/r76/codecs/full_20260929/<Dataset>/delog_generic/trial_001/attempt_001/result.json  per-block raw bytes
usage: python3 evolution_curves.py REPO_ROOT OUT.pdf
"""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42
import matplotlib.pyplot as plt

ROOT = Path(sys.argv[1]); OUT = Path(sys.argv[2])
EVO = ROOT/'evidence/evolution'
REP = json.loads((EVO/'R75_REPORT.json').read_text())


def series(ds):
    raw = [b['raw_bytes'] for b in json.loads(next((ROOT/'evidence/r76/codecs/full_20260929'/ds/'delog_generic').glob('trial_001/attempt_*/result.json')).read_text())['blocks']]
    frozen = [r['pool'] for r in sorted(json.loads((EVO/'fallback_measurement'/f'{ds}.json').read_text())['rows'], key=lambda r: r['index'])]
    st = json.loads((EVO/ds.lower()/'state_full.json').read_text())
    evo = [st['choice'][str(i)]['chosen_total'] for i in range(len(frozen))]
    assert len(raw) == len(frozen) == len(evo), ds
    pub = [a['effective_from'] for a in st['attempts'] if a.get('passed') and a.get('effective_from') is not None]
    tries = [a['block'] for a in st['attempts']]
    cr, cf, ce, xs, yf, ye = 0, 0, 0, [], [], []
    for i, (r, f, e) in enumerate(zip(raw, frozen, evo)):
        cr += r; cf += f; ce += e
        xs.append(i); yf.append(cr / cf); ye.append(cr / ce)
    return xs, yf, ye, pub, tries


fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.25))
for ax, ds in zip(axes, ['Thunderbird', 'Spark']):
    xs, yf, ye, pub, tries = series(ds)
    rep = REP[ds]['ratio']
    ax.plot(xs, ye, color='#C7253E', lw=1.4, label='SemZip with updates')
    ax.plot(xs, yf, color='#3F6CCB', lw=1.4, label='Frozen SemZip')
    ax.axhline(rep['delog'], color='#555555', lw=1.0, ls='--', label='DeLog (complete file)')
    ax.set_xlim(0, len(xs) - 1)
    top = max(max(yf), max(ye), rep['delog'])
    ax.set_ylim(0.8 * min(min(yf), min(ye)), 1.12 * top)
    for p in pub:
        ax.axvline(p, color='#7A4CB3', lw=0.9, ls=':')
        ax.annotate(f'update from block {p}', xy=(p, 1.10 * top), xytext=(4, 0), textcoords='offset points',
                    fontsize=6.5, color='#7A4CB3', va='top')
    ax.set_title(f'{ds} ({len(xs):,} blocks)', fontsize=8.5)
    ax.set_xlabel('Block index', fontsize=8)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.25, lw=0.5)
axes[0].set_ylabel('Cumulative ratio', fontsize=8)
h, l = axes[0].get_legend_handles_labels()
fig.legend(h, l, loc='upper center', ncol=3, fontsize=7, frameon=False, bbox_to_anchor=(0.5, 1.04))
fig.tight_layout(rect=(0, 0, 1, 0.93))
fig.savefig(OUT, bbox_inches='tight')
print('wrote', OUT)
