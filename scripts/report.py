#!/usr/bin/env python3
"""Tables and optional scientific figures from explicitly supplied run records."""
from pathlib import Path
import argparse,csv,json,statistics
from collections import defaultdict
def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--plots',action='store_true');a=p.parse_args()
    rows=json.loads(a.input.read_text())['runs'];assert rows,'No measurement rows supplied'
    keys=['dataset','method','trial','status','raw_bytes','archive_bytes','sha_pass','timing_kind',
          'encode_process_wall_seconds','decode_process_wall_seconds','fallback_blocks']
    for r in rows:
        assert all(k in r for k in keys), 'Missing explicit result fields'
        if r['status']=='PASS':assert r['sha_pass'] and r['raw_bytes']>0 and r['archive_bytes']>0
    a.output.mkdir(parents=True,exist_ok=False)
    with (a.output/'runs.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader();w.writerows({k:r[k] for k in keys} for r in rows)
    groups=defaultdict(list)
    for r in rows:groups[r['dataset'],r['method']].append(r)
    summary=[]
    for (dataset,method),rr in sorted(groups.items()):
        assert len({r['trial'] for r in rr})==len(rr),'Duplicate trial'
        good=[r for r in rr if r['status']=='PASS']
        assert len({(r['raw_bytes'],r['archive_bytes']) for r in good})<=1,'Archive size differs across repeats'
        complete=len(good)==len(rr)
        row={'dataset':dataset,'method':method,'passed':len(good),'runs':len(rr),'ratio':None,
             'formal_encode_MB_s':None,'formal_decode_MB_s':None}
        if good:row['ratio']=good[0]['raw_bytes']/good[0]['archive_bytes']
        if complete and all(r['timing_kind']=='formal' for r in rr):
            for phase in ['encode','decode']:
                durations=[r[phase+'_process_wall_seconds'] for r in rr];assert all(x>0 for x in durations)
                row['formal_'+phase+'_MB_s']=rr[0]['raw_bytes']/1e6/statistics.median(durations)
        summary.append(row)
    with (a.output/'summary.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(summary[0]));w.writeheader();w.writerows(summary)
    if a.plots:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        datasets=sorted({r['dataset'] for r in summary});methods=sorted({r['method'] for r in summary})
        fig,ax=plt.subplots(figsize=(max(7,len(datasets)*.65),4));width=.8/len(methods)
        for mi,method in enumerate(methods):
            lookup={r['dataset']:r['ratio'] for r in summary if r['method']==method}
            xx=[i-.4+width*(mi+.5) for i,d in enumerate(datasets) if lookup.get(d) is not None]
            yy=[lookup[d] for d in datasets if lookup.get(d) is not None]
            ax.bar(xx,yy,width,label=method)
        ax.set_xticks(range(len(datasets)),datasets,rotation=45,ha='right');ax.set_yscale('log')
        ax.set_ylabel('Original bytes / complete archive bytes');ax.legend();fig.tight_layout()
        fig.savefig(a.output/'compression_ratio.pdf');fig.savefig(a.output/'compression_ratio.svg');plt.close(fig)
    print(json.dumps({'status':'PASS','supplied_runs':len(rows),'groups':len(summary)}))
if __name__=='__main__':main()
