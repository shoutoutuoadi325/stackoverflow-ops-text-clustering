# 运维文本聚类项目 PPT 大纲

## 1. 项目背景

- StackOverflow Oracle 问答数量大，重复问题和近似问题较多。
- 运维检索与知识库建设需要把相同问题聚到一起。
- 项目目标：基于 Hadoop + Spark 实现问答文本聚类与问题去重。

## 2. 数据集介绍

- 来源：Stack Exchange Data Dump。
- 范围：标签为 `oracle-database` 的问题及其回答、评论。
- 规模：152758 个问题、203393 个回答、331642 条问题评论、220296 条回答评论。
- 数据结构：question、comments、answers、answer comments 嵌套 JSON。

## 3. 集群环境

- master：10.176.62.239。
- worker1：10.176.62.240。
- worker2：10.176.62.241。
- Hadoop 3.3.6、Spark 3.5.1、OpenJDK 8u412。
- HDFS 存储，YARN 调度，Spark on YARN 计算。

## 4. 系统架构

```text
JSON -> JSONL -> HDFS -> Spark 清洗 -> 特征工程
     -> 主题聚类 -> LSH 相似问题 -> 连通分量 -> Demo
```

建议配一张流程图，突出 HDFS、YARN、Spark ML 三部分。

## 5. 数据预处理

- 原始 JSON 大数组转换成 JSONL。
- Spark 读取 JSONL 并写出 Parquet。
- HTML 清洗、代码块处理、标签拼接。
- 文档构造：标题重复 3 次、正文重复 2 次、拼接标签和前 3 个高分回答。

## 6. EDA 结果

- 问题数：152758。
- 回答数：203393。
- 平均回答数：1.331。
- 最大回答数：34。
- 无回答问题数：29937。
- Top 标签：oracle-database、sql、plsql、oracle11g、java。

## 7. 文本特征工程

- RegexTokenizer 保留技术词、错误码、语言名和版本号。
- StopWordsRemover 去除英文停用词和领域通用词。
- HashingTF + IDF + Normalizer 构造主题聚类向量。
- binary HashingTF 构造 MinHashLSH 输入。

## 8. 主题聚类

- 算法：BisectingKMeans。
- 默认参数：k=50。
- 输出：cluster_id、doc_id、title、score、tags、cluster_top_terms。
- 评价：silhouette、簇大小分布、Top terms 可解释性。

## 9. 去重算法

- MinHashLSH 使用 Spark ML `approxSimilarityJoin` 生成距离阈值内候选。
- 对候选问题计算 token Jaccard distance / similarity 做二次校验。
- 默认平衡配置：jaccard_distance <= 0.25 且 similarity >= 0.75。
- 答辩展示配置：0.65/ht2/bucket200，可展示 16 对相似问题和 16 个重复簇。
- 严格高置信配置：0.85，用于说明 precision/recall 取舍。
- 全量 approxSimilarityJoin 压力测试：0.75/ht4/approx 在当前资源下出现长尾，约 29 分钟后主动停止，作为工程权衡说明。
- 使用连通分量把相似边合并成重复问题簇。

## 10. 聚类结果展示

- 展示一个代表性重复问题簇。
- 代表问题选择：同簇 score 最高，分数相同则标题更短。
- 展示相似问题标题和 similarity。
- 解释相似原因：关键词、错误码、SQL 语义、标签重合。

## 11. 系统 Demo

演示命令：

```bash
bash scripts/07_query_demo.sh --top-clusters --limit 10
bash scripts/07_query_demo.sh --keyword "analytic function" --limit 10
bash scripts/07_query_demo.sh --keyword "group by" --limit 10
bash scripts/07_query_demo.sh --question-id 28753859 --limit 10
```

演示内容：

- 输入问题 ID。
- 输出原问题、cluster_id、cluster_size、代表问题。
- 输出同簇问题和 pair similarity。

## 12. 性能与资源

- Spark executor：1 个 executor、4 cores、6g memory。
- shuffle partitions：96。
- MinHash tables：4。
- similarity threshold：0.75。
- LSH join strategy：代码支持 Spark ML approxSimilarityJoin；全量答辩展示使用 bucket join 复现实验结果。
- LSH 保留每个问题 Top 10 相似问题，避免 pair 过多。
- 中间结果全部写入 HDFS Parquet，便于断点重跑。

## 13. 遇到的问题

- 原始 JSON 是大数组，不能直接高效并行读取。
- HTML 与代码块噪声较多。
- 评论文本对主题有补充，但噪声较高。
- LSH 阈值过低会形成过大的簇。
- GraphFrames 额外依赖复杂，因此采用 DataFrame 迭代连通分量。

## 14. 项目总结

- 完成 Hadoop + Spark 大数据处理链路。
- 完成面向去重的文本清洗、特征工程和聚类。
- 主题聚类用于理解问题分布，LSH + 连通分量用于重复问题发现。
- Demo 可按 question_id 或关键词展示聚类结果。

## 15. 分工与改进

- 集群部署：Hadoop、YARN、Spark。
- 数据处理：JSONL、Parquet、EDA。
- 算法实现：特征工程、主题聚类、LSH、连通分量。
- 展示汇报：Demo、PPT、项目文档。
- 后续改进：加入 Sentence-BERT 向量、人工标注评估、Streamlit 页面 Demo。
