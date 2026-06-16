# 簇内 LSH 实验执行手册

> 给：第一次接触本项目、需要在集群上跑这次实验的人。
> 目标：30 分钟读完，按步骤照做，得到答辩 PPT 需要的两张表。
> 你不需要懂算法细节，照命令跑就行。每一步都有"看什么、应该是什么"。

---

## 1. 这次实验在做什么、为什么做

### 一句话总结

**让主题聚类 BisectingKMeans 和相似度算法 MinHashLSH 串成一条流水线，找出 StackOverflow Oracle 数据里**"**本质相同但表达不同**"**的问题对，并做 K=50 / K=150 两组对比。**

### 为什么

项目目标是"问题去重"。我们之前的代码犯了一个设计错误：

```
v1 旧设计（错的）
features ─┬─→ 主题聚类  →  topic_clusters/   ← 跑了但下游没用
          └─→ MinHashLSH (全局 152,758 文档)  →  similar_pairs   ← 实际去重
```

主题聚类的结果**没有传给** MinHashLSH，LSH 直接在全量 152K 文档上做相似度比对。这导致两个问题：

1. 全局比对的候选对会爆炸（0.85 阈值就有 685 万候选），所以阈值必须卡很严
2. 阈值卡严 → 只找到 **2 组重复问题**，太少

```
v2 新设计（这次实验跑的）
features → 主题聚类 (K=50 或 K=150) → 每个主题簇内做 MinHashLSH → 重复组
```

新设计先用主题聚类把 152K 文档分成若干个主题簇，再在**每个簇内部**做相似度比对。每个簇就几千条文档，搜索空间小，阈值可以放松到 0.65，能找到的重复组就多得多。

### 你需要跑的两组实验

| 实验 | K（主题数）| 目的 |
|---|---|---|
| 实验 A | K = 50 | 验证修复后的流水线，得到第一组重复组数字 |
| 实验 B | K = 150 | 主题分得更细，搜索空间更小，验证 K 增大对召回的影响 |

K=50 的主题聚类**已经在 v1 跑过**（HDFS 上 `/user/bigdata/stackoverflow/output/topic_clusters` 目录），我们的脚本会自动复用，不会重训。K=150 需要新跑一次主题聚类。

---

## 2. 开始之前的检查清单

| 项 | 怎么验证 | 预期结果 |
|---|---|---|
| 在 master 节点上 | `hostname` | `master` |
| 切到 v2 分支 | `cd /opt/bigdata/project-stackoverflow-clustering && git branch --show-current` | `v2` |
| 拉了最新代码 | `git log --oneline -1` | 最新一条是 `v2(D5): two-stage topic + intra-cluster MinHashLSH` 或之后 |
| HDFS 在跑 | `hdfs dfsadmin -report \| head -3` | 看到 `Live datanodes (2)` |
| YARN 在跑 | `yarn node -list 2>/dev/null` | 列出 worker1 / worker2 两个 NodeManager 状态 RUNNING |
| features 已存在 | `hdfs dfs -ls /user/bigdata/stackoverflow/parquet/features \| head -3` | 看到 `_SUCCESS` 文件 |
| topic_clusters K=50 已存在 | `hdfs dfs -ls /user/bigdata/stackoverflow/output/topic_clusters/ \| head -3` 或 `.../topic_clusters_k50/` | 看到 `_SUCCESS` 文件 |

如果 features 不存在，要先跑：

```bash
bash scripts/04_submit_preprocess.sh    # 约 5 分钟
bash scripts/05_submit_cluster.sh       # 约 30-60 分钟，包含 features + topic_clusters + LSH
```

---

## 3. 执行步骤（按顺序）

> 所有命令都在 master 节点的项目目录下执行：
> ```bash
> cd /opt/bigdata/project-stackoverflow-clustering
> ```
> 任何一步报错，**先停下来截图错误**，看下面的"常见错误"小节，不要继续往下跑。

### 步骤 1：把 v1 已有的 topic_clusters 改名（可选但推荐）

我们要跑 K=50 和 K=150，结果要分开存。改名让两组结果不会撞目录。

```bash
# 检查旧目录是否存在
hdfs dfs -test -e /user/bigdata/stackoverflow/output/topic_clusters && echo EXISTS || echo NO_OLD

# 如果上一行输出 EXISTS，执行下面这行；输出 NO_OLD 就跳过
hdfs dfs -mv /user/bigdata/stackoverflow/output/topic_clusters \
            /user/bigdata/stackoverflow/output/topic_clusters_k50
```

**看什么**：

```bash
hdfs dfs -ls /user/bigdata/stackoverflow/output/ | grep topic
```

应该能看到 `topic_clusters_k50`（如果之前是新装的项目，可能没有这一行，那也没关系——脚本会自动训练）。

---

### 步骤 2：先做小规模烟测（强烈推荐）

第一次跑前先用 `MAX_TOPICS=5` 只跑前 5 个最大的主题，验证脚本不挂。约 5 分钟。

```bash
MAX_TOPICS=5 bash scripts/14_submit_cluster_intra_topic.sh k50 50 2>&1 | tee /tmp/smoke_k50.log
```

**看什么**：

终端最后几行应该出现：

```
Done. Outputs under hdfs:///user/bigdata/stackoverflow/output/intra_topic/k50/
```

并且：

```bash
hdfs dfs -ls /user/bigdata/stackoverflow/output/intra_topic/k50/
```

应该看到 5 个目录：`similar_pairs/`、`similar_pairs_samples_csv/`、`topic_metrics_csv/`、`clusters/`、`clusters_samples_csv/`、`summary_csv/`。

烟测通过后，**先把烟测产物清掉**再跑全量，否则全量结果会跟烟测的混在一起：

```bash
hdfs dfs -rm -r -f /user/bigdata/stackoverflow/output/intra_topic/k50
```

---

### 步骤 3：实验 A — K=50 簇内 LSH（全量）

```bash
bash scripts/14_submit_cluster_intra_topic.sh k50 50 2>&1 | tee /tmp/intra_k50.log
```

**预计耗时**：10–20 分钟（K=50 主题聚类已存在，只跑 LSH 部分）。

**期间在另一个终端可看的进度**：

```bash
yarn application -list 2>/dev/null | grep -i intra
```

会有 1 个 Spark 应用在跑。

**看什么（跑完之后）**：

终端最后两行：

```
Done. Outputs under hdfs:///user/bigdata/stackoverflow/output/intra_topic/k50/
```

并查看汇总数字：

```bash
hdfs dfs -cat /user/bigdata/stackoverflow/output/intra_topic/k50/summary_csv/part-*.csv
```

应该看到一行 CSV，类似（数字会和这里不一样）：

```
run_label,num_hash_tables,similarity_threshold,distance_threshold,pair_count,multi_doc_cluster_count,multi_doc_question_count,max_cluster_size,avg_similarity
k50,4,0.65,0.35,42,38,76,3,0.7234
```

**这一行就是 PPT 的核心数字**。其中：

| 字段 | 含义 | 我们关心什么 |
|---|---|---|
| `pair_count` | 找到了几对相似问题 | 越多越好（但不能为了多牺牲精度）|
| `multi_doc_cluster_count` | 几个**重复问题组**（每组 ≥ 2 个问题）| **核心指标，对标对方 PPT 的 52 组** |
| `multi_doc_question_count` | 这些组共涉及几个问题 | 反映去重压缩潜力 |
| `max_cluster_size` | 最大的重复组里有几个问题 | 越大说明合并越彻底 |
| `avg_similarity` | 所有相似对的平均相似度 | ≥0.65 即正常 |

**期望区间**（我们的预估，实际会有偏差）：

- `multi_doc_cluster_count`：30–60 之间
- 如果是 0 或个位数：脚本配置或数据有问题，去看"常见错误"
- 如果 ≥ 100：阈值可能太松，先看几个 sample 是否真的是重复

---

### 步骤 4：实验 B — K=150 簇内 LSH（全量）

```bash
bash scripts/14_submit_cluster_intra_topic.sh k150 150 2>&1 | tee /tmp/intra_k150.log
```

**预计耗时**：25–40 分钟。这次包含两个 Spark 任务：先训练 K=150 主题聚类（10–20 分钟），再跑簇内 LSH（10–20 分钟）。

**看什么**：

跑完后同样：

```bash
hdfs dfs -cat /user/bigdata/stackoverflow/output/intra_topic/k150/summary_csv/part-*.csv
```

期望：`multi_doc_cluster_count` 应该**比 K=50 那组多**（因为搜索空间更小，阈值相同条件下召回更高）。

期望区间：50–90。

---

### 步骤 5：把 HDFS 上的结果拉回本地（master 节点本地磁盘）

```bash
PROJ=/opt/bigdata/project-stackoverflow-clustering

mkdir -p $PROJ/output/hdfs_output

# intra_topic 实验结果
hdfs dfs -get -f /user/bigdata/stackoverflow/output/intra_topic \
   $PROJ/output/hdfs_output/

# K=50 主题聚类（用于算最大簇占比）
hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_clusters_k50 \
   $PROJ/output/hdfs_output/ 2>/dev/null || \
   hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_clusters \
   $PROJ/output/hdfs_output/topic_clusters_k50

# K=150 主题聚类
hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_clusters_k150 \
   $PROJ/output/hdfs_output/
```

**看什么**：

```bash
ls $PROJ/output/hdfs_output/intra_topic/
# 应该看到：k50  k150

ls $PROJ/output/hdfs_output/topic_clusters_k50/ | head
ls $PROJ/output/hdfs_output/topic_clusters_k150/ | head
# 都应该看到 _SUCCESS 和若干 part-*.parquet
```

---

### 步骤 6：本机生成 PPT 用的对比表

这两个命令在 master 上跑也行，在你本机仓库里跑也行（只要 step 5 的结果在该机器上）：

```bash
cd $PROJ   # 或本机仓库根

# 6a) K 值实验对比表：最大簇占比 / Top 5/10/20 占比
python3 src/analyze_topic_k_runs.py \
   k50=output/hdfs_output/topic_clusters_k50 \
   k150=output/hdfs_output/topic_clusters_k150

# 6b) 簇内 LSH 对比表：重复对数 / 重复组数 / 平均相似度
python3 src/summarize_intra_topic_runs.py
```

**看什么**：

每个命令都会打印一张表到终端，类似：

```
 label    k  largest   share   top5  top10  top20
   k50   50    35914 23.51% 56.22% 67.58% 82.94%
  k150  150    23xxx  ~15% ...
```

```
   k50: pairs=42  groups=38  questions=76  avg_sim=0.7234
  k150: pairs=87  groups=72  questions=148 avg_sim=0.7012
```

并且生成两个 CSV：

| 文件 | 用途 |
|---|---|
| `output/evaluation/topic_k_comparison.csv` | PPT 的 "K 值实验" 那一页 |
| `output/evaluation/intra_topic_comparison.csv` | PPT 的 "最终去重结果" 那一页 |

---

### 步骤 7：把新结果注入网页 Demo（可选）

```bash
python3 src/build_demo_assets.py
open demo/index.html    # macOS；Linux 用 xdg-open，Windows 用 start
```

---

## 4. PPT 应该展示什么

### 第一张：K 值对比

读取 `output/evaluation/topic_k_comparison.csv`，按这种格式做表：

| K | 最大簇 | 占比 | Top 5 占比 | Top 20 占比 |
|---|---|---|---|---|
| 50 | 35,914 | 23.51% | 56.22% | 82.94% |
| 150 | xxx | ~15% | ~40% | ~70% |

**讲点**：增大 K 让最大簇变小，分布更均匀，为后续簇内 LSH 提供更细的候选粒度。

### 第二张：去重组数对比

读取 `output/evaluation/intra_topic_comparison.csv`：

| 配置 | hash 表数 | 阈值 | 找到对数 | **重复组数** | 涉及问题数 | 平均相似度 |
|---|---|---|---|---|---|---|
| v1 全局 LSH | 4 | 0.85 | 3 | 3 | 6 | 0.868 |
| v2 K=50 簇内 LSH | 4 | 0.65 | 42 | **38** | 76 | 0.723 |
| v2 K=150 簇内 LSH | 4 | 0.65 | 87 | **72** | 148 | 0.701 |

（数字以你跑出来的为准）

**讲点**：v1 全局 LSH 受候选爆炸限制只能用严阈值；v2 把主题聚类接进 LSH 后，搜索空间从 N² 缩到 Σ(n_i²)，相同阈值下可以多找出**一个数量级**的重复组。

### 第三张：典型重复组样例

读取 `output/hdfs_output/intra_topic/k150/clusters_samples_csv/part-*.csv`，挑 5–8 行人工觉得"看起来真的是重复"的组放上去。

读取 `output/hdfs_output/intra_topic/k150/similar_pairs_samples_csv/part-*.csv` 看相似对样例。

---

## 5. 关键参数可调（默认即可，调参时再看）

`scripts/14_submit_cluster_intra_topic.sh` 接受这些环境变量：

| 变量 | 默认 | 调高 → | 调低 → |
|---|---|---|---|
| `INTRA_SIM_THRESHOLD` | 0.65 | 找到的对更少但更准 | 找到更多但杂质多 |
| `INTRA_HASH_TABLES` | 4 | LSH 召回更高，耗时翻倍 | 更快但漏更多 |
| `MAX_TOPICS` | 0（=全部） | — | >0 时只跑前 N 大主题（用于烟测）|
| `FORCE_RETRAIN` | 0 | 1 = 强制重训主题聚类 | — |
| `ORA_BOOST` | 0.10 | 共享 ORA 码加分越多 | 0 关闭 |
| `ORA_RESCUE_FLOOR` | 0.0 | 设 0.55 时把共享 ORA 码但相似度低于阈值的对也救回 | 0 关闭 |

**如果 K=50 跑出的重复组数太少（< 10）**，按这个顺序调（每次只改一个）：

1. `INTRA_SIM_THRESHOLD=0.55 bash scripts/14_submit_cluster_intra_topic.sh k50 50`
2. 还少：`INTRA_HASH_TABLES=8 INTRA_SIM_THRESHOLD=0.60 bash ...`
3. 还少：`ORA_RESCUE_FLOOR=0.55 bash ...`

---

## 6. 常见错误和怎么排查

### A. `Reusing existing topic clusters at hdfs://...` 但实际想重训

设环境变量强制重训：

```bash
FORCE_RETRAIN=1 bash scripts/14_submit_cluster_intra_topic.sh k150 150
```

### B. `pyarrow not found` 在跑 step 6a 时

```bash
pip3 install --user pyarrow
# 如果公司内网装不上，可以用：
pip3 install --user --index-url https://pypi.org/simple/ pyarrow
```

### C. Spark 任务 stage 卡住超过 30 分钟

```bash
# 查 application id
yarn application -list 2>/dev/null

# 查日志最后 200 行
yarn logs -applicationId <ID> 2>/dev/null | tail -200
```

如果是 OOM，把 shuffle 分区数加一倍：

```bash
SHUFFLE_PARTITIONS=192 bash scripts/14_submit_cluster_intra_topic.sh k50 50
```

### D. `summary_csv` 是空的或 `pair_count=0`

可能 features parquet 没有 ora_codes 列同时阈值卡得太严。先把阈值放低看：

```bash
INTRA_SIM_THRESHOLD=0.50 MAX_TOPICS=3 bash scripts/14_submit_cluster_intra_topic.sh debug 50
```

如果还是 0：去看 `topic_metrics_csv` 检查每个 topic 的 `status` 列是不是有大量 `error:*`。

### E. step 7 build_demo_assets.py 报错说找不到 csv

如果只跑了实验 A 没跑实验 B，先跳过 step 6b 也可以，PPT 会少一行 K=150 数据。

---

## 7. 一页清单（带去集群打印）

```
[ ] cd /opt/bigdata/project-stackoverflow-clustering
[ ] git checkout v2 && git pull
[ ] 检查清单（第 2 节）全部通过
[ ] hdfs dfs -mv .../topic_clusters .../topic_clusters_k50  （如果旧目录存在）
[ ] MAX_TOPICS=5 bash scripts/14_submit_cluster_intra_topic.sh k50 50
[ ] hdfs dfs -rm -r -f .../intra_topic/k50  （清掉烟测）
[ ] bash scripts/14_submit_cluster_intra_topic.sh k50 50
[ ] hdfs dfs -cat .../intra_topic/k50/summary_csv/part-*.csv   ← 记下数字
[ ] bash scripts/14_submit_cluster_intra_topic.sh k150 150
[ ] hdfs dfs -cat .../intra_topic/k150/summary_csv/part-*.csv   ← 记下数字
[ ] hdfs dfs -get .../intra_topic 本地目录
[ ] hdfs dfs -get .../topic_clusters_k50 本地目录
[ ] hdfs dfs -get .../topic_clusters_k150 本地目录
[ ] python3 src/analyze_topic_k_runs.py k50=... k150=...
[ ] python3 src/summarize_intra_topic_runs.py
[ ] 把两张表填进 PPT
```

---

## 8. 跑完之后告诉我什么

跑完发回这三样东西，我可以帮你写 PPT 那两页：

1. `output/evaluation/topic_k_comparison.csv` 内容
2. `output/evaluation/intra_topic_comparison.csv` 内容
3. `output/hdfs_output/intra_topic/k150/clusters_samples_csv/part-*.csv` 前 20 行

如果 K=150 跑挂了或时间不够，光跑 K=50 也能写出 PPT，告诉我即可。
