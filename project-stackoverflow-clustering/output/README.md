# 输出说明

`eda_summary.csv` 和 `top_tags.csv` 填入了实施方案中已经给出的全量数据统计，可直接用于报告初稿。

`cluster_samples.csv` 和 `similar_pair_samples.csv` 当前仅保留表头。正式聚类样例需要在 Hadoop/Spark 集群运行：

```bash
bash scripts/03_submit_eda.sh
bash scripts/04_submit_preprocess.sh
bash scripts/05_submit_cluster.sh
bash scripts/06_export_demo.sh
```

Spark 运行后，样例 CSV 会出现在 HDFS 输出旁边的 `*_samples_csv` 目录中。
