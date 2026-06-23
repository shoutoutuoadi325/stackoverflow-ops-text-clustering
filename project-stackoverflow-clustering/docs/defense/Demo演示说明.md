# 最终 Demo 演示说明

## 演示文件

```text
output/evaluation/final_intra_topic_sweep.csv
output/evaluation/hybrid_edge_overlap.csv
output/intra_topic_results/k50/hybrid_summary.csv
output/intra_topic_results/k50/hybrid_clusters_samples.csv
output/intra_topic_results/k50/rescued_pairs_samples.csv
```

## 推荐顺序

1. 总表：说明六组均来自 HDFS 导出。
2. k50 Baseline：43 对、39 簇、81 个问题。
3. k50 Hybrid：46 对、42 簇、87 个问题。
4. 最大簇：展示 4 个 ORA-00907 不同表述。
5. ORA 救援：展示 ORA-29280、ORA-30926、ORA-00906。
6. 边集合表：展示 k150/k200 完全收敛，以及 2x/4x 的 0.7872 Jaccard。

## 讲解注意

- `rescued_pair_count` 是救援记录，不一定是唯一 pair；使用 Hybrid-Baseline 作为净新增边。
- `title_sim_floor` 实际是完整 token Jaccard 下限。
- 不说 2x/4x 完全一致，只说聚合指标一致。
- 不把问题覆盖数称为 recall。
- 不说标签是纯人工标注；它们来自历史 LLM 辅助评审。

## 网页

可用 `python -m http.server 8765 --directory demo` 打开静态页面。若页面卡片仍是旧全局 LSH 数据，以最终 CSV 演示为准。
