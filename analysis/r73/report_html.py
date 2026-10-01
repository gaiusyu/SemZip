"""Static HTML results report (Chinese) generated from executed records. Output: paper_out/semzip_stability_report.html"""
import json, math, statistics as st, html
from pathlib import Path
H = Path(__file__).resolve().parent; D = H.parent/'dev_results'; OUT = H/'paper_out'
A = json.loads((H.parent.parent/'artifact_r71_working/results/final/anonymous_results.json').read_text())
DS = A['protocol']['datasets']; ONE = {'Linux', 'Proxifier', 'Apache', 'Zookeeper'}
size = {}
for r in A['size_rows']: size.setdefault(r['method'], {})[r['dataset']] = r
def J(p):
    p = D/p
    return json.loads(p.read_text()) if p.exists() else None
IDENT = {'HDFS', 'Thunderbird', 'OpenStack'}
def formal(v, d):
    r = J(f'runs/formal/{v}/{d}/result.json')
    if r: return r['encode']['raw_bytes'] / r['encode']['archive_bytes'], r['encode']['archive_bytes']
    if v == 'qg1c0' and d in IDENT: x = size['semzip'][d]; return x['raw_bytes'] / x['archive_bytes'], x['archive_bytes']
    return None
rows = []
for d in DS:
    r71 = size['semzip'][d]; dl = size['delog'][d]
    q = formal('qg1c0', d); p = formal('pool', d)
    rows.append({'d': d, 'r71': r71['raw_bytes'] / r71['archive_bytes'], 'r71b': r71['archive_bytes'], 'qg': q, 'pool': p,
                 'delog': dl['raw_bytes'] / dl['archive_bytes'], 'delogb': dl['archive_bytes'], 'raw': r71['raw_bytes']})
done = [r for r in rows if r['pool']]
full = len(done) == 16 and all(r['qg'] for r in rows)
mean = lambda k: st.mean((r[k][0] if isinstance(r[k], tuple) else r[k]) for r in rows) if all(r[k] for r in rows) else None
m71, mq, mp, md = mean('r71'), mean('qg'), mean('pool'), mean('delog')
wins = sum(r['pool'][1] < r['delogb'] for r in done)
tot_new = sum(r['pool'][1] for r in done); tot_dl = sum(r['delogb'] for r in done)
stab = json.loads((OUT/'stability_agg.json').read_text()) if (OUT/'stability_agg.json').exists() else {}
cost = J('runs/COST.json') or {}
timing = J('runs/timing/SUMMARY.json')
fmt = lambda x, n=2: '—' if x is None else f'{x:,.{n}f}'
def delta(new, old):
    if not new or not old: return ''
    v = (new / old - 1) * 100
    if abs(v) < 0.05: return '<span class="d flat">0.0%</span>'
    cls = 'up' if v > 0 else 'down'
    return f'<span class="d {cls}">{v:+.1f}%</span>'
trs = []
for r in rows:
    q = r['qg'][0] if r['qg'] else None; p = r['pool'][0] if r['pool'] else None
    best = max(x for x in [p, r['delog']] if x) if p else None
    trs.append('<tr>' + f'<th scope="row">{r["d"]}{"<sup>†</sup>" if r["d"] in ONE else ""}</th>'
               + f'<td>{fmt(r["r71"])}</td><td>{fmt(q)}{delta(q, r["r71"])}</td>'
               + f'<td class="{"win" if p and p > r["delog"] else ""}"><b>{fmt(p)}</b>{delta(p, r["r71"])}</td>'
               + f'<td>{fmt(r["delog"])}</td></tr>')
# SVG stability chart from stability_numbers.json
svg = ''
sn = OUT/'stability_numbers.json'
if sn.exists():
    S = json.loads(sn.read_text()); srows = S['rows']; stats = S['stats']
    ds = [d for d in DS if d in stats]
    W, Hh, L, T, B = 860, 300, 64, 24, 70
    vals = []
    for d in ds:
        r = srows[d]; one = r['one_block']
        raw = r['train_raw'] if one else r['held_raw']; q = r['train_qg1'] if one else r['held_qg1']; pool = r['pool_train'] if one else r['pool_held']
        vals.append((d, [100 * raw[c] / pool for c in ['c0', 'c1', 'c2', 'c3', 'c4']], [100 * q[c] / pool for c in ['c0', 'c1', 'c2', 'c3', 'c4']]))
    top = max(max(a + b) for _, a, b in vals); lo = 95
    y = lambda v: T + (Hh - T - B) * (1 - (math.log(v) - math.log(lo)) / (math.log(top * 1.03) - math.log(lo)))
    step = (W - L - 16) / max(1, len(vals))
    g = [f'<svg viewBox="0 0 {W} {Hh}" role="img" aria-label="各数据集单次合成相对最终选择的存档大小">']
    for t in [100, 110, 125, 150, 200, 300, 500]:
        if t <= top * 1.03:
            g.append(f'<line x1="{L}" x2="{W-8}" y1="{y(t):.1f}" y2="{y(t):.1f}" class="grid"/><text x="{L-8}" y="{y(t)+4:.1f}" class="tick" text-anchor="end">{t}%</text>')
    g.append(f'<line x1="{L}" x2="{W-8}" y1="{y(100):.1f}" y2="{y(100):.1f}" class="sel"/>')
    for i, (d, a, b) in enumerate(vals):
        x = L + step * (i + 0.5)
        g.append(f'<line x1="{x-7:.1f}" x2="{x-7:.1f}" y1="{y(min(a)):.1f}" y2="{y(max(a)):.1f}" class="rng raw"/>')
        g.append(f'<line x1="{x+7:.1f}" x2="{x+7:.1f}" y1="{y(min(b)):.1f}" y2="{y(max(b)):.1f}" class="rng gate"/>')
        for v in a: g.append(f'<circle cx="{x-7:.1f}" cy="{y(v):.1f}" r="3.6" class="pt raw"/>')
        for v in b: g.append(f'<circle cx="{x+7:.1f}" cy="{y(v):.1f}" r="3.2" class="pt gate"/>')
        g.append(f'<path d="M{x-11:.1f},{y(a[0])-4:.1f} l8,8 m0,-8 l-8,8" class="greedy"/>')
        g.append(f'<text x="{x:.1f}" y="{Hh-B+16}" class="lab" text-anchor="end" transform="rotate(-38 {x:.1f} {Hh-B+16})">{d}</text>')
    g.append('</svg>'); svg = '\n'.join(g)
calls = sum(v['api_calls_reported'] or 0 for k, v in cost.get('syntheses', {}).items() if '/t0.7_' in k)
speed_html = ''
if timing and 'semzip' in timing:
    order = [('semzip', 'SemZip（新）'), ('semzip1', 'SemZip-1（R71）'), ('delog', 'DeLog'), ('loglite', 'LogLite-BL*'), ('gzip6', 'gzip -6'), ('xz6', 'xz -6'), ('zstd3', 'zstd -3')]
    pn, p1 = timing['semzip']['per_dataset'], timing['semzip1']['per_dataset']
    rr = lambda d: pn[d]['encode_MB_per_s'] / p1[d]['encode_MB_per_s'] * 100
    speed_note = (f'<p>新版编码/解码吞吐为 DeLog 的 {timing["semzip"]["encode_gmean"]/timing["delog"]["encode_gmean"]*100:.1f}% / {timing["semzip"]["decode_gmean"]/timing["delog"]["decode_gmean"]*100:.1f}%，'
                  f'为 SemZip-1 的 {timing["semzip"]["encode_gmean"]/timing["semzip1"]["encode_gmean"]*100:.0f}% / {timing["semzip"]["decode_gmean"]/timing["semzip1"]["decode_gmean"]*100:.0f}%。'
                  f'选择只优化存档大小：HealthApp、BGL 的编码速度降到原来的 {rr("HealthApp"):.0f}% / {rr("BGL"):.0f}%，HPC 因删掉了代价高的规则提升到 {rr("HPC"):.0f}%。'
                  '12 个文件 × 7 种方法 × 3 次在同一时段串行测量；另一任务同时占用容器 CPU 时（超过 0.5 核）该次试验作废重测。</p>')
    speed_html = speed_note + '<table class="mini"><thead><tr><th>方法</th><th>编码 MB/s</th><th>解码 MB/s</th></tr></thead><tbody>' + ''.join(
        f'<tr><th scope="row">{lab}</th><td>{timing[k]["encode_gmean"]:.2f}</td><td>{timing[k]["decode_gmean"]:.2f}</td></tr>' for k, lab in order if k in timing) + '</tbody></table>'
page = f'''<title>SemZip 合成稳定性</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;500;700&family=Noto+Serif+SC:wght@600;700&family=IBM+Plex+Mono:wght@400;500&display=swap">
<style>
:root{{--bg:#F4F6F5;--paper:#FFFFFF;--ink:#16232A;--muted:#5A6A70;--rule:#D6DEDC;--accent:#0E7C74;--accent-soft:#E2F1EF;--amber:#A8660F;--blue:#2E6BB0;--raw:#B8323F;--gate:#2E6BB0;
--sans:"Noto Sans SC","PingFang SC","Microsoft YaHei",system-ui,sans-serif;--serif:"Noto Serif SC","Songti SC",serif;--mono:"IBM Plex Mono",ui-monospace,Menlo,monospace}}
@media (prefers-color-scheme:dark){{:root:not([data-theme="light"]){{color-scheme:dark;--bg:#0E1618;--paper:#152125;--ink:#E2EBE9;--muted:#93A6A3;--rule:#28393D;--accent:#3FC1B2;--accent-soft:#16312F;--amber:#E2A650;--blue:#79A9E4;--raw:#F07A84;--gate:#79A9E4}}}}
:root[data-theme="dark"]{{color-scheme:dark;--bg:#0E1618;--paper:#152125;--ink:#E2EBE9;--muted:#93A6A3;--rule:#28393D;--accent:#3FC1B2;--accent-soft:#16312F;--amber:#E2A650;--blue:#79A9E4;--raw:#F07A84;--gate:#79A9E4}}
body{{background:var(--bg);color:var(--ink);font:15px/1.75 var(--sans)}}
.wrap{{max-width:940px;margin:0 auto;padding-inline:16px;padding-block:28px 56px;display:flex;flex-direction:column;gap:28px}}
h1,h2{{font-family:var(--serif);text-wrap:balance;margin:0}} h1{{font-size:30px;line-height:1.25}} h2{{font-size:20px;border-top:1px solid var(--rule);padding-top:18px}}
p,li{{max-width:68ch}} .meta{{color:var(--muted);font-size:13px;letter-spacing:.02em}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px}}
.kpi{{background:var(--paper);border:1px solid var(--rule);border-radius:10px;padding:14px 16px;display:flex;flex-direction:column;gap:4px}}
.kpi .v{{font-family:var(--mono);font-size:26px;font-weight:500;font-variant-numeric:tabular-nums}} .kpi .k{{font-size:12.5px;color:var(--muted);letter-spacing:.03em}}
.kpi.main{{border-color:var(--accent);background:var(--accent-soft)}} .kpi.main .v{{color:var(--accent)}}
.scroll{{overflow-x:auto;background:var(--paper);border:1px solid var(--rule);border-radius:10px}}
table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}} th,td{{padding:7px 12px;text-align:right;border-bottom:1px solid var(--rule);white-space:nowrap}}
thead th{{font-size:12.5px;color:var(--muted);font-weight:500;letter-spacing:.03em}} tbody th{{text-align:left;font-weight:500}} td{{font-family:var(--mono);font-size:13.5px}}
tfoot td,tfoot th{{font-weight:700;border-bottom:none}} td.win b{{color:var(--accent)}}
.d{{display:inline-block;margin-left:8px;font-size:11.5px;min-width:52px}} .d.up{{color:var(--accent)}} .d.down{{color:var(--amber)}} .d.flat{{color:var(--muted)}}
.note{{font-size:13px;color:var(--muted)}} svg{{width:100%;height:auto;display:block}}
.grid{{stroke:var(--rule);stroke-width:1}} .sel{{stroke:var(--accent);stroke-width:2}} .tick,.lab{{fill:var(--muted);font:11px var(--sans)}}
.rng{{stroke-width:1.4;opacity:.55}} .rng.raw{{stroke:var(--raw)}} .rng.gate{{stroke:var(--gate)}}
.pt.raw{{fill:var(--paper);stroke:var(--raw);stroke-width:1.4}} .pt.gate{{fill:var(--gate)}} .greedy{{stroke:var(--ink);stroke-width:1.5;fill:none}}
.legend{{display:flex;flex-wrap:wrap;gap:16px;font-size:12.5px;color:var(--muted)}} .sw{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:-1px}}
ul{{margin:0;padding-left:1.2em;display:flex;flex-direction:column;gap:6px}} code{{font-family:var(--mono);font-size:12.5px;background:var(--accent-soft);padding:1px 5px;border-radius:4px;word-break:break-all}}
table.mini{{width:auto;min-width:320px}}
</style>
<div class="wrap">
<header style="display:flex;flex-direction:column;gap:8px">
<div class="meta">2026-09-27 · 开发机正式实验 · 16 个 LogHub 完整文件 · 每块 100,000 条原始行 · 仅用第 0 块训练与选择 · 全部独立解码逐字节还原</div>
<h1>SemZip 合成稳定性</h1>
<p>单次 LLM 合成的程序质量波动很大。新流程在第 0 块上做 5 次独立合成，逐个过质量门，再按完整存档成本跨合成选择规则；在线仍然执行一个冻结的程序。</p>
</header>
<section class="kpis">
<div class="kpi"><span class="k">R71 单次合成（16 数据集均值）</span><span class="v">{fmt(m71)}×</span></div>
<div class="kpi"><span class="k">+ 质量门（零新增 API）</span><span class="v">{fmt(mq)}×</span></div>
<div class="kpi main"><span class="k">5 次合成 + 成本选择（新主结果）</span><span class="v">{fmt(mp)}×</span></div>
<div class="kpi"><span class="k">DeLog（同一输入）</span><span class="v">{fmt(md)}×</span></div>
</section>
<section style="display:flex;flex-direction:column;gap:12px">
<h2>逐数据集压缩率</h2>
<p class="note">压缩率 = 原始字节 / 完整存档字节（含程序、元数据、manifest）。百分比为相对 R71 的变化；青色加粗表示优于 DeLog。† 单块文件，训练块即测试块。</p>
<div class="scroll"><table><thead><tr><th style="text-align:left">数据集</th><th>R71</th><th>+ 质量门</th><th>新主结果</th><th>DeLog</th></tr></thead>
<tbody>{"".join(trs)}</tbody>
<tfoot><tr><th style="text-align:left">16 数据集均值</th><td>{fmt(m71)}</td><td>{fmt(mq)}</td><td>{fmt(mp)}</td><td>{fmt(md)}</td></tr></tfoot></table></div>
<p class="note">新主结果优于 DeLog 的数据集：{wins}/{len(done)}；合计存档字节相对 DeLog：{(tot_new/tot_dl-1)*100:+.2f}%（R71 为 +4.42%）。{'' if full else f'（已完成 {len(done)} 个数据集）'}唯一落后的是 Thunderbird：它的留出块中只有 14% 的行与第 0 块共享日志骨架（BGL 76%，其余 86–100%），第 0 块上选出的程序在全文件上只比 R71 好 0.3%。</p>
</section>
<section style="display:flex;flex-direction:column;gap:12px">
<h2>单次合成有多不稳定</h2>
<p>每个点是一次独立合成部署后的存档大小，以最终选择为 100%。多块文件用固定的留出块样本（最多 8 个等距原始块，选择时从未读过），单块文件用第 0 块。空心：合成原样部署；实心：经过质量门；叉号：R71 采用的贪心合成。</p>
<div class="legend"><span><span class="sw" style="border:1.5px solid var(--raw)"></span>单次合成</span><span><span class="sw" style="background:var(--gate)"></span>单次合成 + 质量门</span><span>✕ 贪心合成（R71）</span><span><span class="sw" style="background:var(--accent);border-radius:2px;height:3px;width:14px"></span>成本选择 = 100%</span></div>
<div class="scroll" style="padding:8px 4px">{svg}</div>
<ul>
<li>5 次合成之间，最大与最小存档的差距中位数 {fmt(stab.get("median_raw_spread"),1)}%，最大 {fmt(stab.get("max_raw_spread"),1)}%（{stab.get("max_raw_spread_ds","—")}）；质量门把中位差距收窄到 {fmt(stab.get("median_qg1_spread"),1)}%。</li>
<li>贪心解码也不可复现：重复一次 T=0 合成，存档在 {stab.get("greedy_repeat_changed","—")}/{stab.get("greedy_repeat_n","—")} 个数据集上发生变化，最大 {fmt(stab.get("greedy_repeat_max_abs"),1)}%。例如 Thunderbird：R71 的贪心合成没有产生任何 LLM 程序，重复一次得到 16 条已验证规则。</li>
<li>成本选择在 {stab.get("pool_le_best_gated","—")}/{stab.get("n","—")} 个数据集上不大于所有经过质量门的单次合成；其余 4 个的差距为 OpenStack 1.6%、Mac 0.7%、HPC 与 Android 各 0.02%。</li>
<li>第 0 块上的收益基本能迁移到全文件（数据集间 Spearman ρ = 0.88）：BGL 第 0 块小 27.8%，全文件小 22.4%；Thunderbird 第 0 块小 3.3%，全文件只小 0.3%，原因是第 0 块对其后续格式代表性不足。没有任何数据集的全文件结果比 R71 差。</li>
</ul>
</section>
<section style="display:flex;flex-direction:column;gap:12px">
<h2>方法改动（全部离线，只读第 0 块）</h2>
<ul>
<li><b>质量门</b>：对每条规则尝试运行时已有的通用宽度/布局编译，完整存档变小才采用；再按规则组做反向淘汰，删掉让完整存档变大的规则；最后逐个试加回。修复了 HealthApp（漏宽度编译）、HPC（可逆但变大的规则）。</li>
<li><b>跨合成选择</b>：c0（R71 的贪心合成）+ 4 次 T=0.7 采样；从质量门后成本最低的候选出发，批量贪心地加入/替换/删除其他候选的规则组。BGL 训练块因此再小 12.8%，Windows 1.7%。</li>
<li>每次成本评估都是：第 0 块拟合存储策略 → 编码 → 独立进程解码 → SHA-256 比对。没有任何数据集开关，预算对 16 个数据集相同。</li>
</ul>
</section>
<section style="display:flex;flex-direction:column;gap:12px">
<h2>成本与速度</h2>
<p>新增 64 次合成共 {calls:,} 次模型请求（R71 的 16 次贪心合成为 202 次）。选择在训练块上完成，线上只执行一个冻结程序。</p>
{speed_html or '<p class="note">同一时段的正式测速（12 个不大于 BGL 的文件 × 7 种方法 × 3 次）正在进行，完成后更新。</p>'}
</section>
<footer class="note">数据：本仓库 <code>evidence/selection/</code>（经 <code>analysis/stage_layout.py</code> 映射为原始目录布局）。</footer>
</div>
'''
(OUT/'semzip_stability_report.html').write_text(page)
print('report written; datasets with pool formal:', len(done), 'full:', full)
