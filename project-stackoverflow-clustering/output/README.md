# 输出说明

`eda_summary.csv` 和 `top_tags.csv` 填入了实施方案中已经给出的全量数据统计，可直接用于报告初稿。

`cluster_samples.csv` 和 `similar_pair_samples.csv` 是报告与答辩可直接打开的稳定样例文件。导出脚本会优先从 `output/evaluation/sweep/yarn_sim065_ht2_bucket200/` 同步高召回样例；如果该目录不存在，则回退到 Spark local 或 `output/hdfs_output/` 中的样例目录。

```bash
bash scripts/03_submit_eda.sh
bash scripts/04_submit_preprocess.sh
bash scripts/05_submit_cluster.sh
bash scripts/06_export_demo.sh
bash scripts/06_export_evaluation.sh
```

也可以手动刷新顶层样例：

```bash
python src/sync_report_samples.py
```

Spark 运行后，原始样例 CSV 仍会保存在 HDFS 输出旁边的 `*_samples_csv` 目录中。
