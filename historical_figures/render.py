"""Regenerate historical figures from saved numeric inputs; no codecs or API."""
from pathlib import Path
import argparse,csv,json,hashlib
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator,ScalarFormatter
HERE=Path(__file__).resolve().parent
BLUE='#2864A5';ORANGE='#D97732';RED='#B43C43';GREEN='#247C63';GRAY='#68727E'
MODELS=['GPT-4o-mini','GPT-4o','DeepSeek-V3.2','Qwen3-32B','Qwen3-Coder-480B-A35B','Gemini-2.5-Pro']
LABELS=['GPT-4o-mini','GPT-4o','DeepSeek-V3.2','Qwen3-32B*','Qwen3-Coder-480B-A35B','Gemini-2.5-Pro']
def save(fig,name):
 for ext in ['pdf','png']:fig.savefig(OUT/f'{name}.{ext}',bbox_inches='tight')
 plt.close(fig)
finish=save
def export(name,rows):
 with (OUT/f'{name}.csv').open('w',newline='') as f:
  w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
table=export


def overview():
    d=D['storage'];assert d['status']=='PASS'
    rows=sorted(d['rows'],key=lambda r:r['dataset'].lower())
    table('storage_tradeoff',[{k:r[k] for k in ['dataset','raw_bytes','adaptive_ratio','fixed_ratio','archive_change_percent']} for r in rows])
    y=np.arange(len(rows));fig,ax=plt.subplots(1,2,figsize=(7.1,4.25),gridspec_kw={'width_ratios':[1.65,1]},sharey=True,layout='constrained')
    for i,r in enumerate(rows):
        ax[0].plot([r['fixed_ratio'],r['adaptive_ratio']],[i,i],color='#AAB0B8',lw=2)
    ax[0].scatter([r['adaptive_ratio'] for r in rows],y,s=38,facecolors='none',edgecolors=ORANGE,label='Block-adaptive',zorder=3)
    ax[0].scatter([r['fixed_ratio'] for r in rows],y,s=17,color=BLUE,label='Offline fixed',zorder=4)
    ax[0].set_xscale('log');ax[0].set_xticks([20,50,100,200,500]);ax[0].xaxis.set_major_formatter(ScalarFormatter())
    ax[0].set_yticks(y,[r['dataset'] for r in rows]);ax[0].invert_yaxis()
    ax[0].set_xlabel('Compression ratio (original / archive bytes, log scale)')
    ax[0].set_title('(a) Complete files, original 100k-line blocks',loc='left')
    ax[0].legend(loc='upper right');ax[0].grid(axis='x',alpha=.2)
    change=[r['archive_change_percent'] for r in rows]
    ax[1].barh(y,change,color=RED,height=.58)
    for i,v in enumerate(change):
        label='0' if v==0 else (f'+{v:.5f}%' if v<.001 else f'+{v:.2f}%')
        ax[1].text(v+.06,i,label,va='center',fontsize=7.5)
    ax[1].set_xlim(0,4);ax[1].set_xlabel('Fixed archive increase (%)')
    ax[1].set_title('(b) Cost of frozen storage choices',loc='left');ax[1].grid(axis='x',alpha=.2)
    finish(fig,'storage_tradeoff')

def throughput():
    d=D['storage']
    rows=[r for r in d['rows'] if r['formal_repeats_complete']]
    assert len(rows)==12
    fig,axes=plt.subplots(1,2,figsize=(7.1,3.1),sharey=True,layout='constrained')
    export=[]
    for ax,metric,title in zip(axes,['online_compression','decode'],['(a) Encoding','(b) Decoding']):
        for variant,offset,color,marker in [('adaptive',-.13,ORANGE,'o'),('fixed',.13,BLUE,'s')]:
            for i,r in enumerate(rows):
                trials=r[f'{variant}_{metric}_mbps_trials'];median=np.median(trials)
                ax.plot([min(trials),max(trials)],[i+offset]*2,color=color,lw=1.3)
                ax.scatter(trials,[i+offset]*3,color=color,s=8,alpha=.55)
                ax.scatter([median],[i+offset],s=23,color=color,marker=marker,label=variant if i==0 else None,zorder=4)
                for j,v in enumerate(trials):export.append({'dataset':r['dataset'],'metric':metric,'variant':variant,'trial':j+1,'original_MB_per_second':v})
        ax.set_xscale('log');ax.grid(axis='x',alpha=.2);ax.set_xlabel('Original MB/s (log scale)');ax.set_title(title,loc='left')
    axes[0].set_yticks(range(len(rows)),[r['dataset'] for r in rows]);axes[0].invert_yaxis()
    axes[0].legend(title='Median + all 3 trials',loc='upper right',fontsize=7,title_fontsize=8)
    axes[0].set_xticks([.5,1,2,5,10,20]);axes[1].set_xticks([2,5,10,20,40])
    for ax in axes:ax.xaxis.set_major_formatter(ScalarFormatter())
    table('throughput_trials',export);finish(fig,'throughput_trials')

def program_control():
    rows=D['program_control']
    fig,ax=plt.subplots(figsize=(6.7,2.25),layout='constrained')
    vals=[r['archive_reduction_pct'] for r in rows];y=np.arange(len(rows))
    ax.barh(y,vals,color=[GREEN if x>=0 else RED for x in vals],height=.55)
    for i,v in enumerate(vals):ax.text(max(v,0)+.6,i,f'{v:+.2f}%',va='center')
    ax.set_yticks(y,[r['dataset'] for r in rows]);ax.invert_yaxis();ax.axvline(0,color=GRAY,lw=.8)
    ax.set_xlim(-3,49);ax.set_xlabel('Archive reduction from learned-program layer (%)');ax.grid(axis='x',alpha=.2)
    ax.set_title('Complete files; same native residual backend',loc='left')
    table('program_control',[{'dataset':r['dataset'],'cohort':r['cohort'],'archive_reduction_pct':r['archive_reduction_pct'],
       'learned_archive_bytes':r['conditions']['learned']['archive_bytes'],'residual_only_archive_bytes':r['conditions']['no_learned']['archive_bytes']} for r in rows])
    finish(fig,'program_control')

def android_archive_breakdown():
    rows=D['android_breakdown']
    assert len({r['raw_sha256'] for r in rows if r['status']=='PASS'})==1
    assert all(r['other_metadata_bytes']>=0 for r in rows if r['status']=='PASS')
    fig,ax=plt.subplots(figsize=(6.7,2.65),layout='constrained');x=np.arange(len(rows));bottom=np.zeros(len(rows))
    for field,label,color in [('native_residual_archive_bytes','Native residual archive',BLUE),('semantic_archive_bytes','Semantic archive',ORANGE),('other_metadata_bytes','Other metadata',GRAY)]:
        values=np.array([r[field] if r['status']=='PASS' else np.nan for r in rows])/1e6
        ax.bar(x,values,bottom=bottom,label=label,color=color,width=.55);bottom+=values
    for i,r in enumerate(rows):
        if r['status']=='PASS':ax.text(i,bottom[i]+.025,f"{r['archive_bytes']/1e6:.3f} MB",ha='center',fontsize=8)
        else:ax.text(i,.25,'FAILED\nNo valid archive',ha='center',color=RED,fontsize=8)
    ax.set_xticks(x,[r['condition']+'\nTraining '+str(r['repeat']) for r in rows]);ax.set_ylabel('Complete archive (decimal MB)')
    ax.set_xlim(-.6,len(rows)-.4);ax.set_ylim(0,np.nanmax(bottom)*1.35);ax.legend(loc='upper center',ncol=3,fontsize=7,frameon=False)
    ax.grid(axis='y',alpha=.2);ax.set_title('Android: identical held-out bytes, independent learned plans',loc='left')
    table('android_archive_breakdown',rows);finish(fig,'qwen_android_breakdown')

def backbones():
 fig,axes=plt.subplots(1,3,figsize=(7.1,2.8),sharey=True,layout='constrained')
 limits=[(28,48),(99,121),(28,36)]
 for ax,ds,lim in zip(axes,['HPC','OpenSSH','Android'],limits):
  for i,model in enumerate(MODELS):
   rows=[r for r in D['backbones'] if r['dataset']==ds and r['model']==model]
   good=[r['ratio'] for r in rows if r['status']=='PASS']
   if len(good)==2:ax.plot(good,[i,i],color='#CBD0D5',lw=2,zorder=1)
   for r in rows:
    offset=-.11 if r['repeat']==1 else .11;marker='o' if r['repeat']==1 else 's'
    if r['status']=='PASS':
     active=bool(r['active_semantic_blocks'])
     ax.scatter(r['ratio'],i+offset,marker=marker,s=28,edgecolor=BLUE,facecolor=BLUE if active else 'white',zorder=3)
    else:ax.text(.02,i+offset,'FAIL (run 2)',transform=ax.get_yaxis_transform(),color=RED,va='center',fontsize=7)
  ax.set_xlim(*lim);ax.grid(axis='x',alpha=.2);ax.set_title(ds);ax.set_xlabel('Held-out compression ratio')
 axes[0].set_yticks(range(len(MODELS)),LABELS);axes[0].invert_yaxis()
 fig.suptitle('Two independent trainings per cell; common 16,384-token request ceiling',fontsize=9)
 fig.supxlabel('Circle: run 1; square: run 2; hollow: no semantic stream.  *Qwen3-32B: thinking disabled.',fontsize=7.4)
 save(fig,'backbone_six')
 export('backbone_trials',[{k:r.get(k,'') for k in ['dataset','model','repeat','status','ratio','requests','responses','training_seconds','retained_plan_entries','active_semantic_blocks','semantic_values','raw_bytes','archive_bytes']} for r in D['backbones']])

def cost():
 fig,axes=plt.subplots(1,3,figsize=(7.1,2.8),sharey=True,layout='constrained')
 summaries={r['model']:r for r in D['backbone_summary']}
 for i,m in enumerate(MODELS):
  r=summaries[m]
  values=[r['synthesis_seconds']/60,r['requests'],100*r['finish_reasons'].get('length',0)/r['responses']]
  for ax,v,col in zip(axes,values,[BLUE,GREEN,ORANGE]):
   ax.barh(i,v,color=col,height=.55);ax.text(v+.8,i,f'{v:.1f}' if ax!=axes[1] else str(int(v)),va='center',fontsize=7)
 for ax,title in zip(axes,['Synthesis minutes (sum of 6)','API requests (sum of 6)','Length stops / responses (%)']):
  ax.set_title(title,fontsize=8);ax.grid(axis='x',alpha=.2)
 axes[0].set_xlim(0,72);axes[1].set_xlim(0,145);axes[2].set_xlim(0,68)
 axes[0].set_yticks(range(6),LABELS);axes[0].invert_yaxis()
 fig.supxlabel('Includes failed calls/trials. Shared host and gateway; observed costs, not an isolated speed ranking.',fontsize=7.5)
 save(fig,'backbone_cost')

def evolution():
 order=['OpenSSH','Android','BGL','HPC','Hadoop','HealthApp','Windows','Spark','HDFS'];summary={r['dataset']:r for r in D['evolution']}
 fig,axes=plt.subplots(3,3,figsize=(7.1,5.4),layout='constrained');exported=[]
 fig2,axes2=plt.subplots(3,3,figsize=(7.1,5.4),layout='constrained')
 for ax,ax2,name in zip(axes.flat,axes2.flat,order):
  r=summary[name];blocks=D['append_blocks'][name]
  x=np.array([b['index'] for b in blocks]);raw=np.cumsum([b['frozen']['raw_bytes'] for b in blocks]);f=np.cumsum([b['frozen']['archive_bytes'] for b in blocks]);e=np.cumsum([b['evolving']['archive_bytes'] for b in blocks]);assert np.array_equal(f,e)
  ax.plot(x,raw/f,color=BLUE,lw=2.8,label='Frozen');ax.plot(x,raw/e,color=ORANGE,lw=1.3,ls='--',marker='o',ms=2.5,label='Evolution enabled')
  ax2.plot(x,[b['eligible_groups'] for b in blocks],color=GREEN,marker='.',lw=1.2);ax2.axhline(10,color=GRAY,ls=':',label='Trigger = 10')
  suffix='prefix' if r['scope']=='first 10 blocks' else 'full'
  for a in [ax,ax2]:
   a.set_title(f'{name} ({suffix})',loc='left');a.grid(alpha=.2);a.xaxis.set_major_locator(MaxNLocator(integer=True,nbins=4));a.set_xlabel('Completed original block')
   for u in r['updates']:a.axvline(u['completed_through'],color=RED,ls=':',lw=.9)
  ax.ticklabel_format(axis='y',style='plain',useOffset=False)
  ax.text(.98,.07,f'{raw[-1]/f[-1]:.2f}x',ha='right',transform=ax.transAxes,fontsize=7.5,bbox=dict(facecolor='white',edgecolor='none',alpha=.85,pad=1))
  for b in blocks:
   exported.append({'dataset':name,'original_block':b['index'],'raw_bytes':b['frozen']['raw_bytes'],'frozen_archive_bytes':b['frozen']['archive_bytes'],'evolving_archive_bytes':b['evolving']['archive_bytes'],'eligible_groups':b['eligible_groups'],'plan_version':b['version'],'update_calls_total':r['update_calls']})
 for ax in axes[:,0]:ax.set_ylabel('Cumulative held-out ratio')
 for ax in axes2[:,0]:ax.set_ylabel('Eligible residual groups')
 handles,labels=axes[0,0].get_legend_handles_labels();fig.legend(handles,labels,loc='outside upper center',ncol=2,frameon=False)
 # Legend owns the top margin; null-gain statement is in the manuscript caption.
 fig2.suptitle('Numeric residual groups; one update at most, vertical line = attempt',fontsize=10)
 save(fig,'evolution_nine');save(fig2,'evolution_coverage');export('evolution_blocks',exported)
 fig,axes=plt.subplots(1,2,figsize=(7.1,3.25),sharey=True,layout='constrained')
 for i,name in enumerate(order):
  r=summary[name]
  for ax,value in zip(axes,[r['update_calls'],r['update_seconds']]):
   ax.barh(i,value,color=GREEN if ax==axes[0] else BLUE,height=.55);ax.text(value+.5,i,str(value) if ax==axes[0] else f'{value:.2f}',va='center',fontsize=7)
 axes[0].set_yticks(range(9),order);axes[0].invert_yaxis();axes[0].set_xlim(0,21);axes[1].set_xlim(0,105)
 axes[0].set_xlabel('Actual update API calls');axes[1].set_xlabel('Update-stage seconds')
 for ax in axes:ax.grid(axis='x',alpha=.2)
 fig.suptitle('Eight update attempts, 46 calls, 395.24 seconds; no new rule published',fontsize=9)
 save(fig,'evolution_cost')
 export('evolution_summary',[{'dataset':r['dataset'],'scope':r['scope'],'blocks':r['blocks'],'heldout_ratio':r['heldout']['frozen']['ratio'],'archive_saving_percent':r['archive_saving_percent'],'updates':len(r['updates']),'update_calls':r['update_calls'],'update_seconds':r['update_seconds']} for r in D['evolution']])

def gated_figures():
    ds=D['gated']['datasets'];rows=D['gated']['blocks']
    plt.rcParams.update({'font.size':9,'axes.spines.top':False,'axes.spines.right':False,'pdf.fonttype':42})
    fig,axes=plt.subplots(2,4,figsize=(12,5.2));axes=axes.ravel()
    for ax,e in zip(axes,ds):
     rr=[x for x in rows if x['dataset']==e['dataset'] and x['block']>=1];xx=[x['block'] for x in rr]
     ax.plot(xx,[x['raw_bytes']/x['frozen_bytes'] for x in rr],'-o',ms=3,label='Frozen',color='#526477')
     ax.plot(xx,[x['raw_bytes']/x['deployed_bytes'] for x in rr],'--s',ms=3,label='Gated deployment',color='#da6b26')
     control=[x for x in rr if x['control_bytes'] is not None]
     if control:ax.plot([x['block'] for x in control],[x['raw_bytes']/x['control_bytes'] for x in control],':^',ms=3,label='Storage refit only',color='#26988c')
     ax.axvline(3.5,color='#aaaaaa',lw=1);ax.set_title(e['dataset']);ax.set_xlabel('Original block index');ax.set_ylabel('Compression ratio');ax.grid(alpha=.15)
    axes[-1].axis('off');handles,labels=axes[0].get_legend_handles_labels();axes[-1].legend(handles,labels,loc='center',frameon=False);axes[-1].text(.05,.15,'Future evaluation starts at block 4.\nOverlapping lines mean no update gain.',transform=axes[-1].transAxes,fontsize=9)
    fig.tight_layout();fig.savefig(OUT/'gated_evolution.pdf');fig.savefig(OUT/'gated_evolution.png',dpi=160);plt.close(fig)
    fig,ax=plt.subplots(figsize=(9,3));names=[e['dataset'] for e in ds];g=[e['future_archive_saving_percent'] for e in ds]
    ax.bar(names,g,color=['#21877a' if x>=0 else '#bc4b45' for x in g]);ax.axhline(0,color='black',lw=.6);ax.set_ylabel('Future archive saving (%)');ax.set_title('All seven prespecified datasets; zero includes no publication')
    spread=max([abs(x) for x in g]+[1]);ax.set_ylim(min([0]+g)-.2*spread,max([0]+g)+.3*spread)
    for i,v in enumerate(g):ax.text(i,v+(.03*spread if v>=0 else -.08*spread),f'{v:+.2f}%',ha='center',fontsize=9)
    fig.tight_layout();fig.savefig(OUT/'gated_gain.pdf');fig.savefig(OUT/'gated_gain.png',dpi=160);plt.close(fig)
    # The first published case in the prespecified dataset order is illustrative;
    # the right panel retains the full cohort, including every zero/regression.
    case=next((e for e in ds if e['outcome']=='PUBLISHED'),ds[0])
    fig,(left,right)=plt.subplots(1,2,figsize=(6.5,2.55),gridspec_kw={'width_ratios':[1,1.3]})
    rr=[x for x in rows if x['dataset']==case['dataset'] and x['block']>=1]
    for key,label,style,color in [('frozen_bytes','Frozen','-o','#526477'),('deployed_bytes','Deployed','--s','#da6b26')]:
     left.plot([x['block'] for x in rr],[x['raw_bytes']/x[key] for x in rr],style,color=color,ms=3,label=label)
    left.axvline(3.5,color='#aaaaaa',lw=.8);left.set_title(case['dataset']+' chronology');left.set_xlabel('Original block index');left.set_ylabel('Compression ratio');left.legend(fontsize=8,frameon=False);left.grid(alpha=.15)
    right.bar(names,g,color=['#21877a' if x>0 else '#bc4b45' if x<0 else '#82909b' for x in g]);right.axhline(0,color='black',lw=.6);right.set_title('All future outcomes');right.set_ylabel('Archive saving (%)');right.tick_params(axis='x',rotation=40,labelsize=7)
    right.set_ylim(min([0]+g)-.18*spread,max([0]+g)+.3*spread)
    for i,v in enumerate(g):right.text(i,v+(.03*spread if v>=0 else -.08*spread),f'{v:.2f}',ha='center',fontsize=7)
    fig.tight_layout(pad=.6);fig.savefig(OUT/'gated_overview.pdf');fig.savefig(OUT/'gated_overview.png',dpi=180);plt.close(fig)



def main():
 global OUT,D
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);args=p.parse_args()
 if args.output.exists():p.error('Output must not exist')
 D=json.loads((HERE/'plot_inputs.json').read_text());OUT=args.output;OUT.mkdir(parents=True)
 plt.rcParams.update({'font.family':'DejaVu Sans','font.size':9,'axes.titlesize':10,'axes.labelsize':9,'legend.fontsize':8,'xtick.labelsize':8,'ytick.labelsize':8,'pdf.fonttype':42,'ps.fonttype':42,'axes.spines.top':False,'axes.spines.right':False,'savefig.dpi':220,'axes.axisbelow':True})
 overview();throughput();program_control();android_archive_breakdown()
 plt.rcParams.update({'font.family':'DejaVu Sans','font.size':8.5,'axes.titlesize':9,'axes.labelsize':8.5,'legend.fontsize':7.5,'xtick.labelsize':8,'ytick.labelsize':8,'pdf.fonttype':42,'axes.spines.top':False,'axes.spines.right':False,'axes.axisbelow':True,'savefig.dpi':220})
 backbones();cost();evolution();gated_figures()
 sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
 report={'status':'PASS','scope':'Saved numeric historical plots; no measurement revalidation','plot_inputs_sha256':sha(HERE/'plot_inputs.json'),'renderer_sha256':sha(Path(__file__)),'matplotlib':matplotlib.__version__,'numpy':np.__version__,'outputs':{p.name:sha(p) for p in sorted(OUT.iterdir()) if p.is_file()}}
 (OUT/'RENDER_CHECK.json').write_text(json.dumps(report,indent=2,sort_keys=True)+'\n');print(json.dumps({'status':'PASS','files':len(report['outputs'])}))
if __name__=='__main__':main()
