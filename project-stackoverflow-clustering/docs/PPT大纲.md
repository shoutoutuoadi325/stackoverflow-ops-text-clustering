# 最终答辩 PPT 大纲

## 1. 项目目标
三节点 Hadoop/Spark 集群，对 152,758 条 Oracle 问题进行主题组织和去重。

## 2. 数据与平台
JSON -> JSONL -> HDFS -> Parquet；Hadoop 3.3.6、Spark 3.5.1、OpenJDK 8u412。

## 3. 文本与特征
标题 3x、正文 2x、标签、Top 3 回答；TF-IDF 主题特征、binary HashingTF LSH 特征、结构化 ORA 码。

## 4. v1 问题
主题聚类和全局 LSH 平行，主题输出未参与去重；全局候选长尾。

## 5. 创新一：主题语义 Blocking
BisectingKMeans -> topic 内 approxSimilarityJoin -> token Jaccard -> 连通分量。

## 6. 创新二：ORA Hybrid 图
ORA 结构化、同主题 boost、跨主题补边。过滤使用完整 token Jaccard 0.55，或标签交集 + 0.50。

## 7. 六组结果
展示 Baseline 和 Hybrid 总表。重点：k50 = 46/42/87；k150 = 42/40/82。

## 8. 创新三：任务隔离式 Folding
只压缩 BisectingKMeans 训练向量，LSH 特征不压缩。4x 与 2x 聚合指标相同。

## 9. 创新四：边集合稳定性
k150/k200 同方案边集合完全一致；2x/4x 共同 37/42，Jaccard 0.7872。说明 K 收敛但压缩有微观扰动。

## 10. 创新五：Spark 工程优化
瘦 LSH join、Parquet 物化截断 lineage、localCheckpoint、大 topic 优先。

## 11. 典型样例
size=4 的 ORA-00907 簇；ORA-29280、ORA-30926、ORA-00906 跨主题救援。

## 12. 项目完成结果
完整集群、数据链路、六组实验、派生表、报告、复现脚本和 Demo。

## 13. 评测边界
90 条历史 LLM 辅助标签，总 precision 0.6667；无全量 ground truth，不报告 recall/F1。

## 14. 结论
k50 用于覆盖优先展示；k150 4x 为工程折中；k200 无额外收益。
