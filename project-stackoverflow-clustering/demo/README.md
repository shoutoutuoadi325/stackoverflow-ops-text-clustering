# Demo 说明

Demo 展示已经导出的真实结果，不现场运行全量 Spark。正式结论以以下文件为准：

- `output/evaluation/final_intra_topic_sweep.csv`
- `output/evaluation/hybrid_edge_overlap.csv`
- `output/intra_topic_results/k50/`

推荐展示 k50 Hybrid 的 46 对、42 簇、87 个问题，以及 k150/k200 的边集合收敛。

启动：

```bash
python -m http.server 8765 --bind 127.0.0.1 --directory demo
```

若静态页面仍包含旧全局 LSH 卡片，直接展示最终 CSV，不使用旧卡片作为最终项目结果。
