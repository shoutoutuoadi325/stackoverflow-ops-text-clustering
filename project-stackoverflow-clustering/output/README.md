# 输出说明

最终结果位于 `output/intra_topic_results/`，包含 k50、k100、k150、k150c2、k200、k200c2。

权威派生表：

- `evaluation/final_intra_topic_sweep.csv`：六组 Baseline/Hybrid 汇总。
- `evaluation/hybrid_edge_overlap.csv`：K 与压缩方案的边集合重叠。
- `intra_topic_results/<label>/summary.csv`：Baseline。
- `intra_topic_results/<label>/hybrid_summary.csv`：Hybrid。
- 其余 CSV：topic metrics、相似对、救援记录、Baseline/Hybrid 簇样例。

`rescued_pair_count` 是按 ORA join 生成的记录数，同一 pair 可能出现多次。净新增边使用 `hybrid_pair_count - baseline_pair_count`。

Raw Parquet 和完整日志保留在 HDFS。顶层旧全局 LSH 文件只作历史对照，不进入最终结论。
