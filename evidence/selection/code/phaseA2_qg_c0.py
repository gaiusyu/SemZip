"""Phase A (0 API), parallel-evaluation continuation: QG-V1 (qg_select2, same decisions) on R71 programs (c0).
Reuses each dataset's evaluation cache. A per-dataset O_EXCL lock prevents two lanes from working on one dataset.
Usage: phaseA2_qg_c0.py THREADS DS1,DS2,..."""
import os, sys, time, traceback
from pathlib import Path
import lib, qg_select2
th = int(sys.argv[1])
for d in sys.argv[2].split(','):
    work = Path('runs/qg/c0')/d
    if (work/'qg_report.json').exists(): continue
    work.mkdir(parents=True, exist_ok=True)
    try: fd = os.open(str(work/'LANE.lock'), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError: continue
    os.write(fd, str(os.getpid()).encode()); os.close(fd)
    t = time.time()
    try:
        r = qg_select2.select(d, lib.ART/'deployments'/d/'program.json', Path('train')/(d+'.block0.log'), work, th)
        print('QGDONE', d, r['original_train_archive_bytes'], '->', r['selected_train_archive_bytes'], 'removed', r['removed_tags'],
              'evals', r['evaluations'], 'sec', round(time.time()-t), flush=True)
    except Exception:
        (work/'FAILED.txt').write_text(traceback.format_exc()); print('QGFAIL', d, flush=True)
    finally:
        (work/'LANE.lock').unlink(missing_ok=True)
