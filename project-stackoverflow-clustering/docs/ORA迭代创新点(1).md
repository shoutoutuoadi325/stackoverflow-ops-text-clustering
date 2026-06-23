# ORA 领域信号迭代创新点

## 1. 迭代结论

ORA 的创新路径是：

```text
普通 token -> ora_codes 结构化字段
-> 同主题候选的 ORA boost / rescue floor
-> 共享 ORA 码的跨主题主动补边
-> Hybrid 重复图
```

最终价值不只是“保留错误码”，而是用 Oracle 领域信号修复主题 blocking 的结构性漏检。

## 2. 代码实现

### 结构化抽取

`preprocess.py` 从加权后的 `raw_doc_text` 中抽取 `ora-\d{4,5}`，去重后写入数组列。该字段由 `feature_engineering.py` 透传。

### 同主题规则

`cluster_lsh_intra_topic.py` 对 LSH 已生成的候选计算完整 token Jaccard。共享 ORA 时：

```text
similarity = min(1.0, similarity_raw + 0.10)
```

代码同时支持 `ORA_RESCUE_FLOOR`。最终导出的 Baseline 样例中 `ora_rescued=true` 为 0，因此六组可独立量化的 ORA 增量主要来自跨主题阶段。

### 跨主题补边

`domain_rescue_graph.py` 按 ORA 码 join，只保留不同 topic 的问题。质量过滤实际为：

```text
token_jaccard >= 0.55
OR
(tags 有交集 AND token_jaccard >= 0.50)
```

参数名 `title_sim_floor` 是历史遗留，实际没有单独计算标题相似度。有效 ORA 码还要求文档频次位于 2 到 150 之间。

救援记录与 Baseline 合并后按无向 pair 去重，保留相似度更高的边，再计算连通分量。

## 3. 六组增量

| 运行 | Baseline 对/簇/问题 | 救援记录 | 净新增边 | Hybrid 对/簇/问题 | 净增问题 |
|---|---:|---:|---:|---:|---:|
| k50 | 43 / 39 / 81 | 3 | 3 | 46 / 42 / 87 | +6 |
| k100 | 41 / 37 / 77 | 3 | 3 | 44 / 40 / 83 | +6 |
| k150 | 38 / 36 / 74 | 6 | 4 | 42 / 40 / 82 | +8 |
| k150c2 | 38 / 36 / 74 | 5 | 4 | 42 / 40 / 82 | +8 |
| k200 | 37 / 35 / 72 | 7 | 5 | 42 / 40 / 82 | +10 |
| k200c2 | 38 / 36 / 74 | 5 | 4 | 42 / 40 / 82 | +8 |

救援记录按 ORA join 行计数；同一文档对共享多个 ORA 码时会重复。净新增边才是 Hybrid 图的真实增量。

## 4. 样例

- ORA-29280：UTL_FILE directory path/object。
- ORA-30926：MERGE/UPDATE stable row set。
- ORA-00906：missing left parenthesis。
- ORA-04063 / ORA-06508：package 编译与调用。
- ORA-28545 / ORA-02063：外部数据源与 DB link。

样例位于 `output/intra_topic_results/<label>/rescued_pairs_samples.csv`，并带有 `cross_topic=true` 与 `edge_source=ora_cross_topic_rescue`。

## 5. 分析结论

- 主题越细，Baseline 覆盖越低，ORA 跨主题补边的净贡献从 3 条提高到 5 条。
- K=150/200 的 Hybrid 最终稳定在 40 簇、82 个问题。
- ORA 补边没有让平均相似度明显坍塌，但没有人工逐边标签时不能直接声称 precision 提升。
- 相同 ORA 码不是充分条件；完整 token Jaccard 和标签重合共同控制质量。

## 6. 边界

- ORA boost 0.10、0.55/0.50 下限和频次 2-150 均为经验参数。
- 方法只适用于具有结构化错误码的 Oracle 领域。
- `rescued_pair_count` 命名容易让人误以为是唯一边数，报告必须同时给出净新增边。
- 现有评审集中 ORA match 只有 1 条，不能用它比较 ORA 子集 precision。

## 7. 答辩话术

> ORA 早期只是普通 token。最终版本把它结构化，并分成同主题相似度增强和跨主题主动补边两层。K=50 增加 3 条唯一边、覆盖多 6 个问题；K=200 增加 5 条唯一边、覆盖多 10 个问题。这个增量不大但方向明确，因为它只补主题分桶理论上永远看不到的跨主题边，而且每条边都能追溯共享错误码与 token 相似度。
