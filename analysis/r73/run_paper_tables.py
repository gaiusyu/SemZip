"""Driver equivalent to the inline step of the original build script: regenerate the R73 manuscript tables.
Run after stability_tex.py, supplement_tex.py and evolution_tex.py (they write paper_out/*.json read here)."""
import json
from pathlib import Path
import paper_tables as t
t.main('pool')
try:
    t.texts('pool', timing=json.loads(Path(t.DEV/'runs/timing/SUMMARY.json').read_text()) if (t.DEV/'runs/timing/SUMMARY.json').exists() else None)
except AssertionError as e:
    print('texts skipped:', e)
t.selected_formal_table()
if (t.DEV/'runs/timing/SUMMARY.json').exists():
    t.speed_table(); t.tradeoff_figure(); t.timing_detailed_table()
