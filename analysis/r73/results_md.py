"""Chinese results summary (Markdown) from executed records -> ../deliverables/RESULTS_R74_zh.md"""
import json, statistics as st
from pathlib import Path
H = Path(__file__).resolve().parent; D = H.parent/'dev_results'; O = H.parent/'deliverables'; O.mkdir(exist_ok=True)
A = json.loads((H.parent.parent/'artifact_r71_working/results/final/anonymous_results.json').read_text())
DS = A['protocol']['datasets']; size = {}
for r in A['size_rows']: size.setdefault(r['method'], {})[r['dataset']] = r
J = lambda p: json.loads((D/p).read_text()) if (D/p).exists() else None
IDENT = {'HDFS', 'Thunderbird'}
def ratio(v, d):
    r = J(f'runs/formal/{v}/{d}/result.json')
    if r: return r['encode']['raw_bytes'] / r['encode']['archive_bytes'], r['encode']['archive_bytes']
    if v == 'qg1c0' and d in IDENT: return size['semzip'][d]['ratio'], size['semzip'][d]['archive_bytes']
rows = [(d, size['semzip'][d]['ratio'], ratio('qg1c0', d)[0], ratio('pool', d)[0], size['delog'][d]['ratio'], ratio('pool', d)[1], size['delog'][d]['archive_bytes']) for d in DS]
EVO = json.loads((H/'paper_out/evo_numbers.json').read_text())['evo_archive_bytes']
REP = json.loads((H.parent.parent/'r75_fallback_evolution_20260927/dev_results/R75_REPORT.json').read_text())
by = {r[0]: r for r in rows}  # dataset -> (d, r71, qg1c0, pool ratio, delog ratio, pool bytes, delog bytes)
evo_b = {d: EVO.get(d, by[d][5]) for d in DS}
evo_r = {d: by[d][3] * by[d][5] / evo_b[d] for d in DS}  # raw / evolution bytes
m = lambda i: st.mean(r[i] for r in rows)
stab = json.loads((H/'paper_out/stability_agg.json').read_text()); cost = J('runs/COST.json'); T = J('runs/timing/SUMMARY.json')
L = ['# SemZip 合成稳定性：开发机正式结果（2026-09-27）', '',
     '协议不变：每块 100,000 条原始行、所有状态块内、只用第 0 块做合成/选择/存储拟合、在线执行冻结程序；所有结果独立进程解码并逐块 SHA 校验。', '',
     '## 结论', '',
     f'- 新主结果（5 次合成 + 质量门 + 跨合成成本选择）：16 数据集算术平均 **{m(3):.2f}×**；仅质量门（R71 程序，零新增 API）{m(2):.2f}×；R71 单次合成 {m(1):.2f}×；DeLog {m(4):.2f}×。',
     f'- 优于 DeLog 的数据集 {sum(r[5] < r[6] for r in rows)}/16；合计存档字节相对 DeLog {(sum(r[5] for r in rows)/sum(r[6] for r in rows)-1)*100:+.2f}%（R71 为 +4.42%）。唯一落后：Thunderbird。',
     f'- 开启在线进化（可选阶段，只在触发时重新合成）：只有 Thunderbird（第 13 块）和 Spark（第 21 块）触发；Thunderbird {REP["Thunderbird"]["ratio"]["pool_frozen"]:.2f}× → **{evo_r["Thunderbird"]:.2f}×**，Spark {REP["Spark"]["ratio"]["pool_frozen"]:.2f}× → **{evo_r["Spark"]:.2f}×**；16 数据集均值 {st.mean(evo_r.values()):.2f}×，胜 DeLog {sum(evo_b[d] < by[d][6] for d in DS)}/16，合计字节相对 DeLog {(sum(evo_b.values())/sum(r[6] for r in rows)-1)*100:+.2f}%。逐块回退到本方法空程序几乎无收益（Thunderbird 65.06×），不进主方法。',
     f'- 单次合成不稳定：5 次合成之间存档差距中位数 {stab["median_raw_spread"]:.1f}%（最大 {stab["max_raw_spread"]:.1f}%，{stab["max_raw_spread_ds"]}）；质量门后收窄到 {stab["median_qg1_spread"]:.1f}%；重复贪心合成在 {stab["greedy_repeat_changed"]}/{stab["greedy_repeat_n"]} 个数据集上改变结果（最大 {stab["greedy_repeat_max_abs"]:.1f}%）。',
     '', '## 逐数据集压缩率', '', '| 数据集 | R71 | +质量门 | 新主结果 | +进化 | DeLog |', '|---|---:|---:|---:|---:|---:|']
for d, a, q, p, dl, _, _ in rows: L.append(f'| {d} | {a:.2f} | {q:.2f} | **{p:.2f}** | {evo_r[d]:.2f} | {dl:.2f} |')
L.append(f'| **均值** | {m(1):.2f} | {m(2):.2f} | **{m(3):.2f}** | {st.mean(evo_r.values()):.2f} | {m(4):.2f} |')
if T:
    L += ['', '## 正式测速（同一时段，12 个不大于 BGL 的文件，三次中位数的几何平均，MB/s）', '', '| 方法 | 编码 | 解码 |', '|---|---:|---:|']
    for k, lab in [('semzip', 'SemZip（新）'), ('semzip1', 'SemZip-1（R71）'), ('delog', 'DeLog'), ('loglite', 'LogLite-BL*'), ('gzip6', 'gzip6'), ('xz6', 'XZ6'), ('zstd3', 'Zstd3')]:
        if k in T: L.append(f'| {lab} | {T[k]["encode_gmean"]:.2f} | {T[k]["decode_gmean"]:.2f} |')
if cost:
    pooled = {k: v for k, v in cost['syntheses'].items() if '/t0.7_' in k}
    L += ['', '## 成本', '', f'- 新增 {len(pooled)} 次合成：{sum(v["api_calls_reported"] or 0 for v in pooled.values())} 次模型请求（R71 的 16 次贪心合成为 202 次），合成记录时间 {sum(v["wall_seconds"] or 0 for v in pooled.values())/60:.0f} 分钟。',
          f'- 质量门 + 池化共 {cost["totals"]["selection_evaluations"]:,} 次第 0 块完整评估（拟合→编码→独立解码→SHA）。']
L += ['', '## 证据位置', '', '- 选择与正式结果：本仓库 `evidence/selection/`（原始布局由 `analysis/stage_layout.py` 重建）',
      '- 在线进化：本仓库 `evidence/evolution/`（含预注册设计 `DEV_DESIGN_R75_zh.md`）',
      '- 分析脚本：`analysis/r73/`']
(O/'RESULTS_R74_zh.md').write_text('\n'.join(L) + '\n'); print('\n'.join(L[:12]))
