# StackOverflow Oracle 运维文本聚类

本项目在三节点 Hadoop/Spark 集群上，对 152,758 条 StackOverflow Oracle 问题执行主题聚类、主题内相似问题发现和 ORA 跨主题救援。

## 最终结论

最终结果来自提交 `a9ddaff` 导出的六组 HDFS 派生表。

| 运行 | K | 主题训练维度 | Baseline 对/簇/问题 | 净新增边 | Hybrid 对/簇/问题 |
|---|---:|---:|---:|---:|---:|
| k50 | 50 | 262144 | 43 / 39 / 81 | 3 | 46 / 42 / 87 |
| k100 | 100 | 262144 | 41 / 37 / 77 | 3 | 44 / 40 / 83 |
| k150 | 150 | 65536 | 38 / 36 / 74 | 4 | 42 / 40 / 82 |
| k150c2 | 150 | 131072 | 38 / 36 / 74 | 4 | 42 / 40 / 82 |
| k200 | 200 | 65536 | 37 / 35 / 72 | 5 | 42 / 40 / 82 |
| k200c2 | 200 | 131072 | 38 / 36 / 74 | 4 | 42 / 40 / 82 |

主展示使用 k50 Hybrid，覆盖最高。工程折中使用 k150 4x：与 k200 的 42 条 Hybrid 边完全相同，说明 K=150 后收敛。

2x 与 4x 的聚合指标相同，但只有 37/42 条边重合，集合 Jaccard 为 0.7872。因此只能说宏观稳定，不能说压缩完全无损。

## 最终流水线

```text
JSON -> JSONL -> HDFS -> 清洗/Parquet
-> TF-IDF + binary HashingTF
-> BisectingKMeans 主题 blocking
-> 主题内 MinHashLSH
-> 相似边物化并重读
-> 连通分量
-> ORA 跨主题补边
-> Hybrid 图与派生 CSV
```

## 主要创新

1. 主题聚类真正参与相似候选生成，而不是独立展示。
2. ORA 从普通 token 升级为结构化字段、同主题 boost 和跨主题补边。
3. 主题训练向量可单独 folding，LSH 特征保持原分辨率。
4. 用边集合重叠同时评估 K 收敛和压缩扰动。
5. Parquet 物化截断 Spark lineage，避免连通分量反复重算 LSH。
6. 瘦 LSH join + 后置元数据回填，并只提交可审计派生表。

## 环境与数据

- OpenJDK 8u412
- Hadoop 3.3.6
- Spark 3.5.1 on YARN
- master、worker1、worker2
- 152,758 个问题、203,393 个回答、551,938 条评论

## 目录

| 路径 | 内容 |
|---|---|
| `src/` | 清洗、特征、主题聚类、主题内 LSH、ORA 救援 |
| `scripts/` | 集群提交与复现实验 |
| `conf/app.conf` | HDFS、Spark 和模型参数 |
| `output/intra_topic_results/` | 六组最终派生结果 |
| `output/evaluation/final_intra_topic_sweep.csv` | 最终总表 |
| `output/evaluation/hybrid_edge_overlap.csv` | 边集合重叠分析 |
| `docs/项目完成结果.md` | 完成状态与最终选择 |
| `docs/最终创新点与实验结果.md` | 创新点分析 |
| `docs/ORA迭代创新点(1).md` | ORA 专题 |
| `docs/完整实验报告.md` | 正式报告 |
| `docs/最终实验复现说明.md` | 复现步骤 |

## 复现入口

```bash
bash scripts/14_submit_cluster_intra_topic.sh k50 50
bash scripts/14_submit_cluster_intra_topic.sh k100 100
bash scripts/16_run_k150_k200_pipeline.sh
bash scripts/19_run_large_k_compression_controls.sh
```

运行 ORA 救援：

```bash
RESCUE_TITLE_SIM_FLOOR=0.55 RESCUE_TAG_SIM_FLOOR=0.50 \
  bash scripts/15_submit_domain_rescue.sh k50
```

`RESCUE_TITLE_SIM_FLOOR` 是历史参数名，代码实际计算完整 token Jaccard。

## 评测边界

现有 90 条标签来自历史 LLM-as-a-Judge 辅助评审，总 precision 0.6667，0.65 分组 0.8125。因缺少全量 ground truth，不计算 recall/F1，不声称全局最优。
