# 答辩材料索引

本目录用于上台汇报和最终提交。

## 文件清单

| 文件 | 用途 |
|---|---|
| `运维文本聚类项目答辩.pptx` | 正式答辩 PPT，12 页 |
| `运维文本聚类项目答辩-contact-sheet.png` | PPT 缩略图预览，便于快速检查整体版式 |
| `答辩讲稿.md` | 逐页讲稿和可能被问到的问题 |
| `Demo演示说明.md` | 静态 Demo、命令行备用 Demo 和现场检查清单 |

## 推荐汇报节奏

1. PPT 讲解：6-8 分钟。
2. Demo 演示：2-3 分钟。
3. 问答重点：参数选择、precision/F1 边界、approxSimilarityJoin 全量压力测试。

## 核心答辩口径

项目完成了从真实 StackOverflow Oracle 数据到 HDFS 入湖、Spark 分布式处理、文本聚类、相似问题发现和可视化 Demo 的完整流程。

最终展示使用 `yarn_sim065_ht2_bucket200`，因为它在真实 YARN 全量数据上发现 16 对相似问题和 16 个重复簇，适合现场说明。`0.85` 结果用于高置信对照；`0.75/ht4/approx` 作为工程压力测试记录，不作为最终展示结果。

没有人工标签时，不声称 precision、recall 或 F1 最优。

## 关键证据文件

| 证据 | 路径 |
|---|---|
| 参数汇总 | `../../output/evaluation/parameter_sweep_summary.csv` |
| 相似问题样例 | `../../output/similar_pair_samples.csv` |
| 重复簇样例 | `../../output/cluster_samples.csv` |
| approx 全量压力测试 | `../verification/full_approx075_2026-06-14.md` |
| YARN smoke test | `../verification/v1_yarn_smoke_2026-06-14.md` |
