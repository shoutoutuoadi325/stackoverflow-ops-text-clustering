# 主题内 LSH + ORA Hybrid 最终结果

结果来自 HDFS `/user/bigdata/stackoverflow/output/intra_topic/<label>/`，最终快照为提交 `a9ddaff`。

| label | K | 主题训练维度 | Baseline 对/簇/问题 | 救援记录 | 净新增边 | Hybrid 对/簇/问题 | 平均相似度 |
|---|---:|---:|---:|---:|---:|---:|---:|
| k50 | 50 | 262144 | 43 / 39 / 81 | 3 | 3 | 46 / 42 / 87 | 0.7313 |
| k100 | 100 | 262144 | 41 / 37 / 77 | 3 | 3 | 44 / 40 / 83 | 0.7328 |
| k150 | 150 | 65536 | 38 / 36 / 74 | 6 | 4 | 42 / 40 / 82 | 0.7557 |
| k150c2 | 150 | 131072 | 38 / 36 / 74 | 5 | 4 | 42 / 40 / 82 | 0.7574 |
| k200 | 200 | 65536 | 37 / 35 / 72 | 7 | 5 | 42 / 40 / 82 | 0.7557 |
| k200c2 | 200 | 131072 | 38 / 36 / 74 | 5 | 4 | 42 / 40 / 82 | 0.7574 |

同一压缩方案下 K=150/K=200 的 Hybrid 边完全相同。2x/4x 每组共同 37/42 条边，不能称为完全一致。

文件说明：

- `summary.csv`：Baseline 汇总。
- `hybrid_summary.csv`：Hybrid 汇总。
- `baseline_vs_hybrid.csv`：长表。
- `topic_metrics.csv`：逐 topic 计时和状态；elapsed 求和不是 wall-clock。
- `similar_pairs_samples.csv`：Baseline 全部边样例。
- `rescued_pairs_samples.csv`：按 ORA 码产生的救援记录。
- `clusters_samples.csv`、`hybrid_clusters_samples.csv`：簇成员。
