# 运维文本聚类项目最终效果与答辩说明文档

生成日期：2026-06-11

## 1. 对“最终效果是否最好”的判断

当前系统不能严格宣称为“全局最优模型”，原因是：

1. 真实 precision、recall、F1 必须依赖人工审核标签。目前 `evaluation/review_candidates.csv` 已生成 82 条人工审核候选，但 `label` 尚未填写，因此系统不能负责任地声称某组参数取得了最优 F1。
2. YARN 集群真实运行一组 MinHashLSH 参数耗时约 27 到 43 分钟。完整网格为 5 个相似度阈值、3 个 hash table 数、3 个 bucket size，共 45 组，全部跑完预计需要很长时间，当前没有强行伪造完整网格。
3. 课程项目更重要的是数据真实、流程完整、结果可复核、演示稳定，而不是只追求候选数量最大。候选数量过大可能引入大量误匹配，反而降低去重质量。

因此，当前结论应表述为：

> 当前系统已经达到“真实数据、真实 Spark/YARN 集群、可复核评测、可稳定 Demo 展示”的最优可交付状态；在没有人工标签的前提下，不声称全局最优 F1，只推荐高置信、可解释、可演示的参数与结果。

如果后续要进一步追求严格意义上的最优，需要完成两件事：

1. 人工标注 `evaluation/review_candidates.csv` 中的 `label` 字段，至少标注 50 到 100 对候选问题。
2. 在 YARN 集群空闲时继续运行完整参数网格，然后基于真实 precision 和候选覆盖量选择最终参数。

## 2. 当前最终交付效果

### 2.1 数据真实性

项目只使用本地真实数据集：

```text
C:\Users\30126\Desktop\stackoverflow-ops-text-clustering\StackOverFlow_Oracle_Database\oracle_database_questions.json
```

原始数据核验结果：

| 指标 | 数值 |
|---|---:|
| 文件大小 | 588,919,290 bytes |
| SHA256 | 799f1a59c6ae5618e7b64d4769f202131bd82d1e6a01f2ac42c6ac5a37fb861d |
| 问题数 | 152,758 |
| 回答数 | 203,393 |
| 问题评论数 | 331,642 |
| 回答评论数 | 220,296 |
| 平均每题回答数 | 1.331 |
| 最大回答数 | 34 |
| 无回答问题数 | 29,937 |
| 最高分问题 ID | 470542 |
| 最高问题分数 | 1,382 |

核验文件：

```text
output/validation/raw_data_validation.json
output/validation/raw_data_validation.csv
```

### 2.2 集群真实性

已使用实施方案文档中的账号连接真实三节点集群并完成复核：

| 节点 | IP | 角色 |
|---|---|---|
| master | 10.176.62.239 | NameNode、ResourceManager、Spark Driver |
| worker1 | 10.176.62.240 | DataNode、NodeManager |
| worker2 | 10.176.62.241 | DataNode、NodeManager |

集群复核结果：

| 项目 | 结果 |
|---|---|
| Hadoop | 3.3.6 |
| Spark | 3.5.1 |
| Java | OpenJDK 1.8.0_412 |
| HDFS DataNode | 2 个 live DataNodes |
| YARN NodeManager | 2 个 running NodeManagers |
| SparkPi | 成功，输出 `Pi is roughly 3.1367437891459637` |

复核记录：

```text
docs/verification/cluster_yarn_codex_2026-06-11.log
```

### 2.3 真实参数运行结果

最终参数汇总文件：

```text
output/evaluation/parameter_sweep_summary.csv
```

核心结果如下：

| 运行名 | 方法 | 阈值 | Hash 表 | bucket | 候选对 | 输出对 | 多文档簇 | 平均相似度 | 耗时 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| yarn_sim065_ht2_bucket200 | Spark MinHashLSH | 0.65 | 2 | 200 | 2,227,681 | 16 | 16 | 0.7489 | 1,665s |
| yarn_sim085_ht4_bucket300 | Spark MinHashLSH | 0.85 | 4 | 300 | 6,858,115 | 3 | 3 | 0.8684 | 2,603s |
| yarn_sim075_ht4_approx300 | Spark ML approxSimilarityJoin | 0.75 | 4 | 300 | 未完成 | 未完成 | 未完成 | 未完成 | 约 29 分钟后主动停止 |
| yarn_historical_baseline | Spark MinHashLSH | 0.75 | 4 | 300 | 未导出 | 2 | 2 | 0.8583 | 未记录 |
| spark_local_sim075_ht4_b300 | Spark local MinHashLSH | 0.75 | 4 | 300 | 未导出 | 10 | 9 | 0.8121 | 未记录 |
| spark_local_sim085_ht4_b300 | Spark local MinHashLSH | 0.85 | 4 | 300 | 未导出 | 3 | 3 | 0.8682 | 未记录 |
| local_title_tag_sim065 | 本地 title/tag 扫描 | 0.65 | - | 200 | 10,819 | 10,427 | 2,185 | 0.9719 | 12.532s |

推荐展示参数：

```text
yarn_sim065_ht2_bucket200
```

推荐原因：

1. 没有人工标签时，不能声称 Spark 参数取得最优 F1。
2. `yarn_sim065_ht2_bucket200` 来自真实 Spark on YARN 全量计算，发现 16 对相似问题和 16 个多文档簇，比严格 0.85 结果更适合现场展示。
3. `yarn_sim075_ht4_approx300` 验证了官方 `approxSimilarityJoin` 路线的工程压力：YARN smoke test 已通过，但全量 run 在当前资源下出现 executor 心跳超时和 stage 9 长尾，因此不作为现场展示主结果。
4. `local_title_tag_sim065` 可作为无 Spark 环境时的兜底 Demo 数据，但答辩优先讲 YARN 结果。
5. 答辩时应明确区分“高召回展示候选”和“严格高置信候选”：前者用于可视化展示，后者用于说明 precision/recall 取舍。

### 2.4 人工审核评测状态

已生成：

```text
evaluation/review_candidates.csv
```

当前状态：

| 指标 | 数值 |
|---|---:|
| 待审核样本 | 82 |
| 已标注样本 | 0 |
| precision | 待人工审核 |
| F1 | 待人工审核 |

说明：

系统没有伪造 precision/F1。只有人工填写 `label` 后，才会计算真实 precision。`label` 建议规则如下：

| label | 含义 |
|---|---|
| 1 | 两个问题本质相同或高度重复 |
| 0 | 两个问题不是重复问题 |
| 空 | 尚未审核 |

## 3. 图片内容逐项说明

图片中的内容是课程答辩和提交要求，分为两大部分：报告内容、提交材料。

### 3.1 图片文字完整转写

```text
报告内容

1. PPT讲解
   - 项目完成内容
   - 项目完成过程
   - 采用数据集
   - 从项目中得到的经验
   - 小组分工

2. Demo演示

提交材料

- 源代码/使用说明
- PPT
- 项目文档
```

### 3.2 图片结构说明

图片是一个课程项目答辩要求页，主要强调两个方面：

1. 现场报告需要讲什么。
2. 最终提交需要交哪些材料。

页面排版特点：

| 方面 | 说明 |
|---|---|
| 主标题 | 使用大号黑色中文字体，分别为“报告内容”和“提交材料” |
| 一级编号 | 使用红色编号 `1.`、`2.`，强调报告中的两个主要环节 |
| 项目符号 | 使用深红色圆点或方块作为层级标记 |
| 内容层级 | “报告内容”下分为 PPT 讲解和 Demo 演示；“提交材料”下分为源代码/使用说明、PPT、项目文档 |
| 答辩重点 | 既要求讲清楚项目过程，也要求现场演示系统效果 |

### 3.3 对图片中每项要求的项目对应关系

| 图片要求 | 本项目对应内容 |
|---|---|
| PPT 讲解：项目完成内容 | 已完成 Hadoop/HDFS/Spark/YARN 集群验证、原始数据核验、EDA、文本预处理、特征工程、主题聚类、相似问题发现、重复簇生成、本地 Demo |
| PPT 讲解：项目完成过程 | 可按“数据入湖 -> JSONL 转换 -> Spark EDA -> 文本清洗 -> TF-IDF/HashingTF -> MinHashLSH -> connected components -> 参数评测 -> Demo 展示”讲解 |
| PPT 讲解：采用数据集 | StackOverflow Oracle Database 问答数据，152,758 个问题，203,393 个回答，331,642 条问题评论，220,296 条回答评论 |
| PPT 讲解：从项目中得到的经验 | 大 JSON 需要先转 JSONL；HTML 和代码块会影响文本质量；LSH 参数会影响召回和精度；没有人工标签不能声称真实 F1；Demo 应使用离线导出数据保证稳定 |
| PPT 讲解：小组分工 | 可按“集群环境、数据处理、算法建模、评测优化、Demo 和文档”五类分工陈述 |
| Demo 演示 | 已完成 `demo/index.html`，也可通过 `http://127.0.0.1:8765/index.html` 展示 |
| 源代码/使用说明 | 源代码位于 `src/`，脚本位于 `scripts/`，使用说明位于 `README.md` |
| PPT | 已有 `docs/运维文本聚类项目汇报.pptx` 和 `docs/PPT大纲.md` |
| 项目文档 | 已有 `docs/完整实验报告.md`、`docs/项目实施文档.md`、本说明文档和集群复核记录 |

## 4. PPT 讲解建议

建议 PPT 按 12 到 15 页组织，覆盖图片中“PPT讲解”的所有要求。

### 第 1 页：项目标题

标题：

```text
StackOverflow Oracle 运维文本聚类与相似问题去重
```

说明：

本项目面向 StackOverflow Oracle 数据库相关问答，使用 Hadoop 和 Spark 实现文本聚类与重复问题发现，用于将本质相同但表达不同的问题聚到一起。

### 第 2 页：项目背景与目标

讲解要点：

1. StackOverflow 中大量数据库运维问题存在重复提问。
2. 人工查重效率低，问题标题、正文、标签、错误码可以作为文本相似信号。
3. 项目目标是构建基于 Spark 的文本聚类和相似问题发现流程。

### 第 3 页：数据集说明

讲解要点：

1. 数据来源：StackOverflow `oracle-database` 标签问答数据。
2. 原始文件：`oracle_database_questions.json`。
3. 规模：152,758 个问题、203,393 个回答、551,938 条评论。
4. 数据字段：question_id、title、body、tags、score、comments、answers。

### 第 4 页：数据真实性核验

讲解要点：

1. 原始文件大小：588,919,290 bytes。
2. SHA256：`799f1a59c6ae5618e7b64d4769f202131bd82d1e6a01f2ac42c6ac5a37fb861d`。
3. 核验脚本只读流式读取 JSON，不修改原始数据。
4. Demo 中所有数字来自核验文件或 Spark 导出文件。

### 第 5 页：集群环境

讲解要点：

1. 三台服务器：master、worker1、worker2。
2. HDFS 存储数据，YARN 调度资源，Spark on YARN 执行计算。
3. 已验证 SparkPi 成功，HDFS 有两个 live DataNode，YARN 有两个 NodeManager。

### 第 6 页：系统流程

推荐流程图：

```text
原始 JSON
  -> JSONL 转换
  -> HDFS 入湖
  -> Spark EDA
  -> 问答文本清洗
  -> 特征工程
  -> 主题聚类
  -> MinHashLSH 相似问题发现
  -> 连通分量重复簇
  -> 参数评测与 Demo 展示
```

### 第 7 页：数据预处理

讲解要点：

1. 原始 JSON 是大数组，不适合 Spark 并行读取，因此先转为 JSON Lines。
2. 清洗 HTML、实体符号、无效字符。
3. 构造 `doc_text`：标题加权、正文加权、标签、Top 回答。
4. 保留数据库术语和 ORA 错误码。

### 第 8 页：EDA 结果

讲解要点：

1. Top 标签：oracle-database、sql、plsql、oracle11g、java。
2. 高频错误码：ora-06512、ora-06550、ora-00904、ora-00933 等。
3. 最高分问题：question_id 470542。
4. EDA 证明数据规模和领域特征清晰。

### 第 9 页：算法设计

讲解要点：

1. 主题聚类：TF-IDF + BisectingKMeans。
2. 去重发现：token set + HashingTF(binary) + MinHashLSH。
3. 通过 Jaccard similarity 过滤候选对。
4. 使用 connected components 将相似边合并成重复簇。

### 第 10 页：参数评测

讲解要点：

1. 参数包括相似度阈值、Hash 表数量、bucket size。
2. 当前真实 YARN 运行了两组代表参数。
3. 低阈值召回更多，高阈值更保守。
4. 没有人工标签时，只展示无监督指标，不展示虚假的 precision/F1。

### 第 11 页：Demo 展示

讲解要点：

1. Demo 是静态页面，避免答辩现场依赖 Spark 实时查询。
2. 展示数据核验、EDA、参数扫描、重复簇、关键词检索、question_id 检索。
3. 推荐演示查询：
   - `analytic function`
   - `group by`
   - `28753859`

### 第 12 页：项目完成内容

可直接列出：

1. 完成真实数据核验。
2. 完成 Hadoop/Spark/YARN 集群验证。
3. 完成 EDA、预处理、特征工程。
4. 完成主题聚类和相似问题发现。
5. 完成参数对比和人工审核样本生成。
6. 完成本地 Demo 展示。

### 第 13 页：项目经验

可讲：

1. 大 JSON 文件需要转换为 JSONL。
2. 文本清洗质量直接影响聚类效果。
3. Spark LSH 参数对性能和结果数量影响明显。
4. 工程演示要优先保证稳定和可追溯。
5. 没有人工标签时，不能随意宣称准确率。

### 第 14 页：小组分工

建议分工模板：

| 成员 | 分工 |
|---|---|
| 成员 A | Hadoop、HDFS、YARN、Spark 环境部署与验证 |
| 成员 B | 数据集整理、JSONL 转换、EDA 分析 |
| 成员 C | 文本预处理、特征工程、主题聚类 |
| 成员 D | MinHashLSH 去重、参数评测、人工审核样本 |
| 成员 E | Demo 页面、PPT、项目文档和答辩演示 |

如实际人数不同，可以合并或调整。

### 第 15 页：总结

总结话术：

本项目完成了从真实 StackOverflow Oracle 数据到 HDFS 入湖、Spark 分布式处理、文本聚类、相似问题发现和可视化 Demo 的完整流程。系统没有伪造评测指标，所有关键数字都可追溯到原始数据核验文件、Spark/YARN 输出文件或人工审核文件，适合作为课程项目答辩和现场演示材料。

## 5. Demo 演示脚本

### 5.1 打开 Demo

方式一：直接打开文件：

```text
demo/index.html
```

方式二：使用本地 HTTP 服务：

```bash
cd C:\Users\30126\Desktop\stackoverflow-ops-text-clustering\project-stackoverflow-clustering
python -m http.server 8765 --bind 127.0.0.1 --directory demo
```

浏览器访问：

```text
http://127.0.0.1:8765/index.html
```

### 5.2 推荐演示顺序

1. 打开“总览”：展示问题数、回答数、无回答问题数、主题聚类 Silhouette、Top 标签、ORA 错误码。
2. 打开“核验”：展示原始文件路径、文件大小、SHA256、问题/回答/评论计数。
3. 打开“评测”：展示 Spark/YARN 参数结果、本地高召回结果、人工审核 pending 状态。
4. 打开“重复簇”：展示相似问题簇。
5. 打开“检索”：输入 `analytic function`、`group by`、`28753859`。

### 5.3 推荐讲解话术

```text
这里的 Demo 不是现场实时跑 Spark，而是读取 Spark 和本地核验流程已经导出的真实结果文件。
这样做的好处是答辩现场稳定，同时所有数字都可以追溯到真实输出。
例如 question_id=28753859 可以找到相似问题 28768945，说明系统可以把表达不同但语义接近的问题聚到一起。
```

## 6. 提交材料清单

对应图片中的“提交材料”，本项目应提交以下内容。

### 6.1 源代码/使用说明

源代码：

```text
src/
scripts/
conf/
demo/
```

使用说明：

```text
README.md
demo/README.md
docs/final_effect_and_presentation_guide.md
```

### 6.2 PPT

已有文件：

```text
docs/运维文本聚类项目汇报.pptx
docs/PPT大纲.md
```

建议 PPT 内容按本文第 4 节调整，确保覆盖图片中的所有报告要求。

### 6.3 项目文档

项目文档建议包含：

```text
docs/完整实验报告.md
docs/项目实施文档.md
docs/verification/cluster_yarn_codex_2026-06-11.log
docs/final_effect_and_presentation_guide.md
```

## 7. 最终答辩结论

推荐在答辩中这样表述：

```text
本项目完成了基于 Hadoop 和 Spark 的 StackOverflow Oracle 运维文本聚类系统。
数据来自真实 StackOverflow Oracle 问答 JSON 文件，已通过文件大小、SHA256 和对象计数核验。
集群侧完成了 HDFS、YARN、SparkPi 和 Spark MinHashLSH 参数运行验证。
系统支持 EDA 展示、参数对比、重复问题簇展示和关键词/question_id 检索。
对于准确率指标，我们没有伪造 precision/F1；只有人工审核样本标注后才计算真实 precision。
当前 Demo 选择高召回、可解释的真实结果用于展示，同时保留 YARN 真实运行结果证明分布式处理链路。
```

这份表述既说明了项目完成度，也避免了“没有人工标签却声称最优准确率”的风险。
