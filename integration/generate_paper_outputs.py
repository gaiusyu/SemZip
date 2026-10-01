"""Portable tables/figures from the validated anonymous numeric schema only."""
from pathlib import Path
import csv,hashlib,json,math,statistics
from collections import Counter

ORDER=['semzip','delog','loglite','gzip6','xz6','zstd3']
LABEL={'semzip':'SemZip','delog':'DeLog','loglite':'LogLite-BL*','gzip6':'gzip6','xz6':'XZ6','zstd3':'Zstd3'}
COLORS=['#b3323f','#2870a5','#8b5aa5','#757575','#198475','#c68a2f']
ROUTES={'python_inverse_archive_route':'Python inverse','python_checked_auto_storage':'Python checked/generic','generic_auto_codec':'Generic codec','other_explicit_schema':'Explicit schema'}

def gm(values):return math.exp(statistics.mean(math.log(v) for v in values))
def esc(value):return str(value).replace('_',r'\_').replace('%',r'\%').replace('&',r'\&').replace('#',r'\#')
def n(value):return f'{value:,}' if isinstance(value,int) else f'{value:.2f}'
def text(path,lines):path.write_text('\n'.join(lines)+'\n')
def table(path,caption,columns,head,rows,label=None,long=False):
    if long:
        lines=[r'\begin{longtable}{'+columns+'}',r'\caption{'+caption+r'}\\',r'\toprule',head+r' \\ \midrule\endhead']
        lines += [' & '.join(map(str,row))+r' \\' for row in rows]
        lines += [r'\bottomrule\end{longtable}']
    else:
        lines=[r'\begin{table*}[t]\centering\small',r'\caption{'+caption+'}']
        if label:lines += [r'\label{'+label+'}']
        lines += [r'\begin{tabular}{'+columns+'}',r'\toprule',head+r' \\',r'\midrule']
        lines += [' & '.join(map(str,row))+r' \\' for row in rows]
        lines += [r'\bottomrule',r'\end{tabular}\end{table*}']
    text(path,lines)

def generate(data,out):
    # This entry point is called only after all raw inputs pass integrate_results.
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':8.5,'pdf.fonttype':42,'ps.fonttype':42,'svg.hashsalt':'semzip-final-results-v1'})
    g=out/'generated';f=out/'figures';g.mkdir();f.mkdir()
    datasets=data['protocol']['datasets'];matrix={(r['dataset'],r['method']):r for r in data['size_rows']}
    aggregates={};ratio_rows=[]
    for method in ORDER:
        passed=[matrix[d,method] for d in datasets if matrix[d,method]['status']=='PASS']
        record={'valid':len(passed),'expected':len(datasets),'complete':len(passed)==len(datasets)}
        if record['complete']:
            record.update(arithmetic_mean_ratio=statistics.mean(r['ratio'] for r in passed),
                geometric_mean_ratio=gm([r['ratio'] for r in passed]),
                total_archive_bytes=sum(r['archive_bytes'] for r in passed),total_raw_bytes=sum(r['raw_bytes'] for r in passed))
            record['corpus_ratio']=record['total_raw_bytes']/record['total_archive_bytes']
        aggregates[method]=record
    for dataset in datasets:
        valid=[matrix[dataset,m]['ratio'] for m in ORDER if matrix[dataset,m]['status']=='PASS'];best=max(valid) if valid else None
        cells=[]
        for method in ORDER:
            r=matrix[dataset,method]
            if r['status']!='PASS':cells.append(r'\textsc{fail}');continue
            value=f"{r['ratio']:.2f}";cells.append(r'\textbf{'+value+'}' if r['ratio']==best else value)
        one=matrix[dataset,'semzip'].get('blocks')==1
        ratio_rows.append([dataset+(r'$\dagger$' if one else '')]+cells)
    for label,key in [('Arithmetic mean (16)','arithmetic_mean_ratio'),('Total raw / total archive','corpus_ratio')]:
        ratio_rows.append([label]+[n(aggregates[m][key]) if aggregates[m]['complete'] else '--' for m in ORDER])
    table(g/'external_ratio_table.tex','Complete original-file ratios, raw bytes divided by all counted archive bytes. Failed cells remain visible and receive no reduced-denominator 16-file mean. $\dagger$: one-block in-sample file. *LogLite-BL-wide+tail+reservefix.','lrrrrrr','Dataset & '+' & '.join(LABEL[m] for m in ORDER),ratio_rows,'tab:external-ratios')
    with (g/'size_matrix.csv').open('w',newline='') as stream:
        keys=['dataset','method','status','raw_bytes','archive_bytes','ratio','blocks'];writer=csv.DictWriter(stream,fieldnames=keys);writer.writeheader();writer.writerows({k:r.get(k) for k in keys} for r in data['size_rows'])
    formal={(r['dataset'],r['method']):r for r in data['formal_cells']};small=data['protocol']['formal_datasets'];speed={};detail=[]
    for method in ORDER:
        cells=[formal[d,method] for d in small];ok=all(c['status']=='PASS' for c in cells)
        speed[method]={'complete':ok}
        if ok:
            speed[method].update({k:gm([c[k]['median'] for c in cells]) for k in ['encode_MB_per_s','decode_MB_per_s']})
            speed[method]['gmean_ratio']=gm([matrix[d,method]['ratio'] for d in small])
    table(g/'external_speed_table.tex','Formal throughput: geometric means of per-file three-run medians on the fixed 12-file cohort, MB/s. All 12 cells must pass for a method aggregate. Each phase uses the shared outer-process wall scope.','lrr','Method & Encode & Decode',[[LABEL[m]]+[n(speed[m][k]) if speed[m]['complete'] else '--' for k in ['encode_MB_per_s','decode_MB_per_s']] for m in ORDER],'tab:external-speed')
    for d in small:
        for m in ORDER:
            c=formal[d,m];values=[]
            for k in ['encode_MB_per_s','decode_MB_per_s']:
                v=c[k];values.append(f"{v['median']:.2f} [{v['min']:.2f}, {v['max']:.2f}]" if c['status']=='PASS' else esc(c['status']))
            detail.append([d,LABEL[m]]+values)
    table(g/'formal_detailed_table.tex','Every formal cell, including incomplete and failed cells. Valid entries show median [minimum, maximum], MB/s.','llrr','Dataset & Method & Encode & Decode',detail,long=True)
    with (g/'formal_runs.csv').open('w',newline='') as stream:
        keys=['dataset','method','trial','status','raw_bytes','archive_bytes','sha_pass','encode_process_wall_seconds','decode_process_wall_seconds','timing_kind','fallback_blocks'];w=csv.DictWriter(stream,fieldnames=keys);w.writeheader();w.writerows({k:r.get(k) for k in keys} for r in data['formal_runs'])
    tr=data['training'];total=lambda k:sum(r[k] for r in tr);api=lambda k:sum(r['api'][k] for r in tr)
    training_rows=[[r['dataset'],r['sampled_lines'],r['specs'],r['api']['requests'],n(r['api']['prompt_tokens']),n(r['api']['completion_tokens']),f"{r['synthesis_subprocess_seconds_excluding_sampling']:.1f}",f"{r['corrected_fit_process_seconds']:.1f}"] for r in tr]
    training_rows += [['Total',total('sampled_lines'),total('specs'),api('requests'),n(api('prompt_tokens')),n(api('completion_tokens')),f"{total('synthesis_subprocess_seconds_excluding_sampling'):.1f}",f"{total('corrected_fit_process_seconds'):.1f}"]]
    table(g/'training_cost_table.tex','One first-block synthesis per dataset plus uniform corrected fitting. Synthesis subprocess seconds exclude preceding scanning/sampling. Corrected fit is a separate process measurement; the component sum is not observed total offline latency.','lrrrrrrr','Dataset & Samples & Rules & Calls & Prompt tokens & Output tokens & Synthesis s & Fit s',training_rows,'tab:fresh-training-cost')
    table(g/'program_audit_table.tex',f"Static deployment routes of {total('specs')} saved rules. These are route counts, not invocation counts or causal compression gains.",'lr','Route & Rules',[[ROUTES[k],sum(r['routes'][k] for r in tr)] for k in ROUTES],'tab:program-routes')
    sampling=data['sampling_replay']['datasets'];sk=['line_count_scan_seconds','sample_writer_seconds_including_second_scan','scan_plus_sample_replay_seconds']
    sr=[[r['dataset']]+[f'{r[k]:.3f}' for k in sk] for r in sampling]+[['Total']+[f'{sum(r[k] for r in sampling):.3f}' for k in sk]]
    table(g/'sampling_replay_table.tex','Separate zero-API sampling replay in seconds, reproducing every original sample byte-for-byte. The writer includes its second scan. This shared-host replay is not the original unrecorded sampling time and is not merged into original offline latency.','lrrr','Dataset & Count scan & Sample writer & Total',sr,long=True)
    text(g/'training_cost_summary.tex',[f"The {len(tr)} original syntheses and corrected fits produce {total('specs')} saved rules from {total('sampled_lines')} sampled records, with {api('requests')} model requests and {api('total_tokens'):,} observed tokens. Synthesis subprocesses total {total('synthesis_subprocess_seconds_excluding_sampling'):.1f} seconds; corrected fitting totals {total('corrected_fit_process_seconds'):.1f} seconds. Their sum excludes preceding scanning/sampling and superseded fitting attempts and is not observed total offline latency.",f"Separate zero-API sampling replay reproduces all samples byte-for-byte in {sum(r['scan_plus_sample_replay_seconds'] for r in sampling):.2f} seconds. It is descriptive shared-host replay with prior hash reads and no cache drop."])
    table(g/'original_fit_costs.tex','Original publication wall time contains synthesis, sampling/startup and old fitting, including failures. Corrected fitting is a separate replay. The original publication and synthesis component must not be added.','llrrr','Dataset & Original fit & Original publication s & Synthesis s & Corrected fit s',[[r['dataset'],r['original_storage_status'],f"{r['original_training_and_old_fit_wall_seconds']:.3f}",f"{r['synthesis_subprocess_seconds_excluding_sampling']:.3f}",f"{r['corrected_fit_process_seconds']:.3f}"] for r in tr],long=True)
    blocks=data['main_blocks'];saved={r['dataset']:r['specs'] for r in tr};deployment=[];activity=[];guards=[]
    for d in datasets:
        r=matrix[d,'semzip']
        if r['status']!='PASS':deployment.append([d,esc(r['status'])]+['--']*7);continue
        bs=[b for b in blocks if b['dataset']==d];active=sum(x['stored_values']>0 for x in r['program_activity'])
        deployment.append([d,n(r['raw_bytes']),r['blocks'],n(r['semantic_bytes']),n(r['residual_bytes']),n(r['archive_bytes']-r['semantic_bytes']-r['residual_bytes']),r['fallback_blocks'],f'{active}/{saved[d]}',n(r['heldout']['ratio']) if r['heldout']['ratio'] is not None else '--'])
        for x in r['program_activity']:activity.append([d,esc(x['tag']),ROUTES[x['route']],n(x['admitted_matches']),n(x['stored_values'])])
        guards.append([d,f"{sum(b['inverse_guard_seconds'] for b in bs):.3f}",f"{sum(b['block_boundary_clear_seconds']+b['attempt_boundary_clear_seconds'] for b in bs):.3f}",r['fallback_blocks']])
    table(g/'deployment_accounting.tex','Complete deployment accounting. Semantic, residual and manifest bytes sum to each complete archive. Active rules have stored values; suffix cost is a verified payload view with a recomputed manifest, not a separately timed run.','lrrrrrrrr','Dataset & Raw bytes & Blocks & Semantic & Residual & Manifest & Recovery & Active & Suffix CR',deployment,long=True)
    table(g/'program_activity_detailed.tex','Every saved rule in successful complete deployments, including inactive rules. Activity is not a causal savings estimate.','lllrr','Dataset & Tag & Route & Admitted & Stored',activity,long=True)
    table(g/'guard_task_seconds.tex','Descriptive first-pass block-task seconds. Tasks overlap across workers, so sums are not file latency and cannot be subtracted from formal end-to-end time.','lrrr','Dataset & Inverse tasks s & Clear tasks s & Recovery',guards,long=True)
    table(g/'first_pass_detailed.tex','Complete-file first-pass sizes and descriptive inner timers. These are not the formal cross-method speed observations.','llrrr','Dataset & Method & Archive bytes/status & Encode s & Decode s',[[r['dataset'],LABEL[r['method']],n(r['archive_bytes']),f"{r['diagnostic_encode_seconds']:.3f}",f"{r['diagnostic_decode_seconds']:.3f}"] if r['status']=='PASS' else [r['dataset'],LABEL[r['method']],esc(r['status']),'--','--'] for r in data['size_rows']],long=True)
    paired=data['representation'];pr=[];components=[]
    for r in paired:
        b=r['branches'];pr.append([r['dataset'],n(b['surface']['archive_bytes']),n(b['latent']['archive_bytes']),f"{r['full_archive_saving_pct']:+.2f}",'--' if r['suffix_payload_saving_pct'] is None else f"{r['suffix_payload_saving_pct']:+.2f}"])
        for branch in ['latent','surface']:components.append([r['dataset'],branch]+[n(b[branch][k]) for k in ['semantic_archive_bytes','residual_archive_bytes','manifest_bytes']])
    table(g/'representation_table.tex','Matched-trace representation control with complete counted archives. Savings are 100(1-latent/surface); negative values are regressions. Suffix savings exclude training block zero and the shared file manifest; Linux has no suffix.','lrrrr','Dataset & Surface bytes & Latent bytes & Full saving (\\%) & Suffix saving (\\%)',pr,'tab:representation-control')
    table(g/'representation_components.tex','Counted representation components. Semantic archives already include compressed metadata and inverse programs.','llrrr','Dataset & Branch & Semantic & Residual & Manifest',components,long=True)
    text(g/'representation_results_text.tex',[f"All {len(paired)} pairs reconstruct exactly across {sum(r['blocks'] for r in paired)} original blocks. Their latent semantic payloads, native residual payloads and manifests match the corresponding main-run files byte-for-byte.",'Complete archive savings are '+', '.join(f"{r['dataset']} {r['full_archive_saving_pct']:+.2f}\\%" for r in paired)+'. Negative outcomes are retained. This control jointly changes the semantic representation and its compatible storage, not extraction locations or the residual; it does not establish LLM superiority or universal benefit.'])
    passed_main=[matrix[d,'semzip'] for d in datasets if matrix[d,'semzip']['status']=='PASS']
    text(g/'deployment_results_text.tex',[f"The strict deployment independently reconstructs {len(passed_main)}/16 original files and {len(blocks):,} blocks in those files. Its fixed block guard uses recovery on {sum(r['fallback_blocks'] for r in passed_main)} blocks. Failed files remain explicit outcomes; no online model call is added."])
    prose=[]
    if aggregates['semzip']['complete']:
        s=aggregates['semzip'];prose.append(f"The deployment has {s['arithmetic_mean_ratio']:.2f}$\\times$ arithmetic-mean compression and {s['corpus_ratio']:.2f}$\\times$ corpus compression across all sixteen complete files.")
        if aggregates['delog']['complete']:
            delta=100*(s['total_archive_bytes']/aggregates['delog']['total_archive_bytes']-1);wins=sum(matrix[d,'semzip']['archive_bytes']<matrix[d,'delog']['archive_bytes'] for d in datasets)
            comparison=('Summed archive bytes are unchanged relative to official DeLog.' if delta==0 else
                        f"Summed archive bytes are {abs(delta):.2f}\\% {'larger' if delta>0 else 'smaller'} than official DeLog.")
            prose.append(f"Complete archives are smaller than official DeLog on {wins}/16 files. {comparison} Regressions remain in the matrix.")
    else:prose.append('A complete sixteen-file SemZip ratio aggregate is unavailable because some deployments fail. The full matrix retains all outcomes.')
    prose.append('No reduced-denominator sixteen-file aggregate is assigned to an incomplete method. The LogLite comparison is the disclosed LogLite-BL-wide+tail+reservefix configuration.')
    summary=prose[:2] if aggregates['semzip']['complete'] and aggregates['delog']['complete'] else prose[:1]
    text(g/'external_results_text.tex',prose);text(g/'abstract_results.tex',summary);text(g/'conclusion_results.tex',prose)
    def save_figure(fig,name):
        fig.savefig(f/(name+'.pdf'),metadata={'CreationDate':None,'ModDate':None},bbox_inches='tight')
        fig.savefig(f/(name+'.svg'),metadata={'Date':None},bbox_inches='tight');fig.savefig(f/(name+'.png'),dpi=180,bbox_inches='tight');plt.close(fig)
    fig,axes=plt.subplots(1,2,figsize=(6.3,3.0),constrained_layout=True)
    for ax,key,title in zip(axes,['encode_MB_per_s','decode_MB_per_s'],['Encoding','Decoding']):
        for i,m in enumerate(ORDER):
            if speed[m]['complete']:ax.scatter(speed[m][key],speed[m]['gmean_ratio'],c=COLORS[i],s=45,marker=['o','s','^','D','v','P'][i],label=LABEL[m])
        ax.set_xscale('log');ax.set_xlabel('Throughput (MB/s; log scale)');ax.set_ylabel('Compression ratio');ax.set_title(title);ax.grid(alpha=.2)
    handles,labels=axes[0].get_legend_handles_labels()
    if handles:fig.legend(handles,labels,loc='lower center',bbox_to_anchor=(.5,-.10),ncol=3,frameon=False)
    save_figure(fig,'external_tradeoff')
    fig,ax=plt.subplots(figsize=(9,3.8));width=.8/6
    for j,m in enumerate(ORDER):
        good=[(i,matrix[d,m]['ratio']) for i,d in enumerate(datasets) if matrix[d,m]['status']=='PASS']
        ax.bar([i-.4+(j+.5)*width for i,_ in good],[v for _,v in good],width,color=COLORS[j],label=LABEL[m])
    ax.set_xticks(range(16),datasets,rotation=45,ha='right');ax.set_yscale('log');ax.set_ylabel('Raw / complete archive bytes');ax.legend(ncol=3);fig.tight_layout();save_figure(fig,'external_ratios')
    fig,ax=plt.subplots(figsize=(5.3,2.8));gains=[r['full_archive_saving_pct'] for r in paired]
    ax.bar([r['dataset'] for r in paired],gains,color=['#198475' if x>=0 else '#b3323f' for x in gains]);ax.axhline(0,color='black',linewidth=.6);ax.set_ylabel('Complete archive savings (%)');fig.tight_layout();save_figure(fig,'representation_effects')
    text(g/'external_figures.tex',[r'\begin{figure}[t]\centering',r'\includegraphics[width=\linewidth]{figures/external_tradeoff.pdf}',r'\caption{Geometric means of per-file ratios and formal three-run throughput medians on the fixed twelve-file cohort. Only methods with all twelve valid cells are plotted; failures remain in the tables.}',r'\label{fig:external-tradeoff}\end{figure}'])
    provenance={'schema':data['schema'],'input_sha256':data['input_sha256'],'aggregates':aggregates,'formal_aggregates':speed,
        'generator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'matplotlib_version':matplotlib.__version__,'numpy_version':__import__('numpy').__version__,'outputs_are_from_supplied_validated_data':True}
    (g/'numeric_provenance.json').write_text(json.dumps(provenance,indent=2,sort_keys=True)+'\n')
