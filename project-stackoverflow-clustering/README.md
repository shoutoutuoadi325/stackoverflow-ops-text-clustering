# 运维文本聚类项目

本项目基于 StackOverflow `oracle-database` 问答数据，实现面向问题去重的文本聚类流程。数据存储使用 HDFS，计算使用 Spark on YARN，核心算法为文本清洗、TF-IDF 特征、BisectingKMeans 主题聚类、MinHashLSH 相似问题发现和连通分量重复问题簇生成。

## 目录结构

```text
project-stackoverflow-clustering/
  conf/app.conf                  # HDFS 路径、Spark 参数、算法参数
  scripts/                       # 集群执行脚本
  src/                           # PySpark 与本地辅助程序
  docs/                          # 项目文档、实验报告和 PPT
    verification/                # 集群、SparkPi 和 Demo 现场复核日志
    report_render/               # 实验报告渲染过程中的中间文件
  output/                        # 本地验证输出或 HDFS 导出结果
    hdfs_output/                 # 从 HDFS 导出的完整结果样例
    local_profile_sample/        # 本地小样本统计结果
    cluster_samples.csv          # 报告用重复簇样例，自动同步为非空 CSV
    similar_pair_samples.csv     # 报告用相似问题样例，自动同步为非空 CSV
  demo/                          # 静态 Demo 展示页
  data/                          # 本地数据软链接或拷贝目录
  requirements.txt               # Python 依赖说明
```

## 文件夹说明

| 路径 | 内容说明 |
|---|---|
| `conf/` | 项目配置目录，目前主要是 `app.conf`，集中配置项目部署路径、HDFS 输入输出路径、Spark 资源参数和模型参数，例如 `SIMILARITY_THRESHOLD`、`TOPIC_K`、`SHUFFLE_PARTITIONS`。 |
| `scripts/` | 集群运行脚本目录，按执行顺序从 `00_upload_raw.sh` 到 `07_query_demo.sh` 编排数据上传、JSONL 转换、HDFS 准备、EDA、预处理、聚类、结果导出和 Demo 查询。`common.sh` 封装公共配置加载和 `spark-submit` 参数。 |
| `src/` | 源代码目录，包含 JSON 转换、本地小样本统计、Spark EDA、预处理、特征工程、主题聚类、MinHashLSH 相似问题发现、连通分量重复簇生成和命令行 Demo 查询程序。 |
| `docs/` | 项目提交材料目录，包含项目实施文档、完整实验报告、PPT 大纲、正式汇报 PPT、项目收尾清单和现场复核记录。 |
| `docs/verification/` | 服务器现场复核日志目录，保存 `jps`、HDFS、YARN、SparkPi 和 Demo 命令验证结果，便于答辩前追溯环境状态。 |
| `docs/report_render/` | 实验报告渲染过程产生的中间文件目录，用于生成或检查 `.docx` 报告时留存临时材料。 |
| `output/` | 本地输出与样例结果目录，包含 JSONL 样例、EDA 摘要、标签统计、相似问题样例和重复簇样例。 |
| `output/hdfs_output/` | 从 HDFS 导出的 Spark 全量结果样例，包括 EDA、主题聚类、相似问题对和重复问题簇等输出目录。 |
| `output/local_profile_sample/` | 不依赖 Spark 的本地小样本统计结果，用于快速验证数据读取、字段解析和基础统计逻辑。 |
| `demo/` | 零前端依赖的本地展示界面，读取 `demo/data.js` 展示 EDA、主题聚类、相似问题对、重复簇和关键词检索。 |
| `data/` | 本地数据目录，通常用于放置或软链接 StackOverflow Oracle 原始数据；正式运行时由脚本上传到 HDFS。 |

## 环境要求

- OpenJDK 8u412
- Hadoop 3.3.6
- Spark 3.5.1 for Hadoop 3
- Python 3.8+
- 集群主机：`master`、`worker1`、`worker2`

正式运行前确认：

```bash
jps
hdfs dfsadmin -report
yarn node -list
spark-submit --master yarn --deploy-mode client \
  --class org.apache.spark.examples.SparkPi \
  /opt/bigdata/spark/examples/jars/spark-examples_2.12-3.5.1.jar 10
```

如果现场 YARN 资源分配等待较久，可以先用小内存参数做 SparkPi 连通性验证：

```bash
spark-submit --master yarn --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --num-executors 1 \
  --conf spark.dynamicAllocation.enabled=false \
  --class org.apache.spark.examples.SparkPi \
  /opt/bigdata/spark/examples/jars/spark-examples_2.12-3.5.1.jar 10
```

## 快速开始

在 master 上部署项目目录：

```bash
mkdir -p /opt/bigdata
cp -r project-stackoverflow-clustering /opt/bigdata/
cd /opt/bigdata/project-stackoverflow-clustering
```

如果部署路径或 HDFS 路径不同，先修改 `conf/app.conf`。

本仓库的本地数据集默认识别为项目同级目录：

```text
../StackOverFlow_Oracle_Database/oracle_database_questions.json
```

也可以通过环境变量覆盖：

```bash
LOCAL_DATA_DIR=/path/to/StackOverFlow_Oracle_Database bash scripts/01_json_to_jsonl.sh
```

上传原始 JSON：

```bash
bash scripts/00_upload_raw.sh \
  /opt/bigdata/project-stackoverflow-clustering/data/StackOverFlow_Oracle_Database/oracle_database_questions.json
```

转换 JSONL 并上传：

```bash
bash scripts/01_json_to_jsonl.sh
bash scripts/02_prepare_hdfs.sh
```

运行 Spark 处理流程：

```bash
bash scripts/03_submit_eda.sh
bash scripts/04_submit_preprocess.sh
bash scripts/05_submit_cluster.sh
```

如果希望 Demo 中召回更多相似问题候选，可运行更高召回配置。该脚本会复用已有特征，写入带后缀的新 HDFS 输出目录：

```bash
bash scripts/05_submit_cluster_recall.sh
```

Demo 查询：

```bash
bash scripts/07_query_demo.sh --top-clusters --limit 10
bash scripts/07_query_demo.sh --keyword "analytic function" --limit 10
bash scripts/07_query_demo.sh --keyword "group by" --limit 10
bash scripts/07_query_demo.sh --question-id 28753859 --limit 10
```

答辩现场建议优先演示 `--top-clusters`、`--keyword "group by"` 和 `--question-id 28753859`。服务器复核日志保存在 `docs/verification/`，正式汇报 PPT 为 `docs/运维文本聚类项目汇报.pptx`。

导出结果：

```bash
bash scripts/06_export_demo.sh
```

生成并打开本地 Demo 界面：

```bash
python src/validate_raw_data.py \
  --input ../StackOverFlow_Oracle_Database/oracle_database_questions.json
python src/build_review_candidates.py
python src/evaluate_review_labels.py
python src/summarize_sweep_results.py
python src/build_demo_assets.py
```

然后直接打开：

```text
demo/index.html
```

也可以使用脚本生成：

```bash
bash scripts/08_validate_raw_data.sh
bash scripts/09_build_demo_assets.sh
```

## 真实评测与参数扫描

为了避免把“结果数量更多”误当成“效果更好”，项目将评测拆成三层：

1. 原始数据核验：`src/validate_raw_data.py` 直接读取原始 JSON，输出文件大小、SHA256、问题数、回答数、评论数、最高分问题等到 `output/validation/`。
2. 参数扫描：`scripts/05_submit_cluster_sweep.sh` 按多组 LSH 参数写入独立 HDFS 输出目录，不覆盖基线结果。导出后运行 `scripts/06_export_evaluation.sh` 汇总到 `output/evaluation/parameter_sweep_summary.csv`。
3. 人工审核：`src/build_review_candidates.py` 生成 `evaluation/review_candidates.csv`。只有人工填写 `label` 后，`src/evaluate_review_labels.py` 才会计算 precision；未标注时 Demo 显示“待人工审核”，不伪造 F1 或准确率。

没有 Spark 集群时，可以运行本地真实数据兜底评测。该流程直接读取原始 JSON 的标题和标签，生成保守相似候选和阈值扫描结果，方法名标记为 `local_title_tag`，用于答辩展示和人工审核，不替代 Spark MinHashLSH 全量实验：

```bash
python src/local_similarity_sweep.py \
  --input ../StackOverFlow_Oracle_Database/oracle_database_questions.json
python src/summarize_sweep_results.py
python src/build_review_candidates.py
python src/evaluate_review_labels.py
python src/build_demo_assets.py
```

本机 Windows 已验证可用的 Spark local 复现脚本为：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/10_run_spark_local_verified.ps1
```

该脚本使用项目内 `.venv-spark`、JDK 17 和 `tools/hadoop/bin/winutils.exe`/`hadoop.dll`，生成 `output/spark_local_verified`，并把 Spark MinHashLSH 的 `0.75` 与 `0.85` 两组真实结果汇总进 `output/evaluation/parameter_sweep_summary.csv`。

参数扫描默认网格：

```text
similarity_threshold: 0.65 0.70 0.75 0.80 0.85
num_hash_tables: 2 4 8
max_bucket_size: 200 300 500
```

小规模 smoke run 可以覆盖环境变量：

```bash
SWEEP_SIMILARITIES="0.75 0.85" \
SWEEP_HASH_TABLES="2" \
SWEEP_BUCKET_SIZES="200" \
bash scripts/05_submit_cluster_sweep.sh
```

## 输出结果

HDFS 输出：

```text
/user/bigdata/stackoverflow/output/eda
/user/bigdata/stackoverflow/output/topic_clusters
/user/bigdata/stackoverflow/output/similar_pairs
/user/bigdata/stackoverflow/output/clusters
```

报告常用样例 CSV 会生成在：

```text
.../topic_clusters_samples_csv
.../similar_pairs_samples_csv
.../clusters_samples_csv
```

Spark 写 CSV 时会生成目录，其中实际数据文件通常为 `part-*.csv`。

## 本地验证

没有 Spark 的本机也可以做小样本验证：

```bash
python3 src/json_to_jsonl.py \
  ../StackOverFlow_Oracle_Database/oracle_database_questions.json \
  output/questions_sample.jsonl \
  --limit 100

python3 src/local_profile.py \
  ../StackOverFlow_Oracle_Database/oracle_database_questions.json \
  --output-dir output/local_profile_sample \
  --limit 100
```

本地验证只用于检查数据读取和统计逻辑，最终聚类结果以 Spark 集群输出为准。

## 算法说明

文档构造：

```text
doc_text = title * 3 + question_body * 2 + tags + top_3_answers
```

这么做是为了强化问题标题和正文，同时用高分回答补充语义。评论噪声较多，默认只用于 EDA，不进入主聚类特征。

主题聚类：

```text
RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF -> Normalizer -> BisectingKMeans
```

重复问题发现：

```text
RegexTokenizer -> StopWordsRemover -> binary HashingTF -> MinHashLSH
approxSimilarityJoin(distance_threshold) -> token Jaccard distance / similarity
jaccard_distance <= 0.25 且 similarity >= 0.75 -> similar_pairs
similar_pairs -> connected components -> duplicate clusters
```

默认配置采用 `SIMILARITY_THRESHOLD=0.75`、`MINHASH_TABLES=4` 和 Spark ML 原生 `approxSimilarityJoin`，让 `LSH_DISTANCE_THRESHOLD` 直接参与候选生成。YARN smoke test 已验证该路径可用；全量 `0.75/ht4/approx` 压力测试在当前资源下出现 join 长尾，约 29 分钟后主动停止。因此答辩展示建议使用 `scripts/05_submit_cluster_recall.sh` 的 `0.65/ht2/bucket200` 高召回配置；严格高置信分析可保留 `0.85` 结果作为 precision/recall 取舍说明。
