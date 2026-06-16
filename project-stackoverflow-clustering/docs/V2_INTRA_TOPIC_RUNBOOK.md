# 簇内 LSH 实验执行手册

> 给：第一次接触本项目、需要在集群上跑这次实验的人。
> 目标：30 分钟读完，按步骤照做，得到答辩 PPT 需要的两张表。
> 你不需要懂算法细节，照命令跑就行。每一步都有"看什么、应该是什么"。

---

## 0. 先看懂这条流水线

### 什么是流水线

像工厂一样，原料经过一道道工序，每道工序加工后传给下一道，最后出成品。我们项目就是这样：

```
原料：StackOverflow 原始 JSON   →   …一系列加工…   →   成品：重复问题组
```

### 完整流水线图

```
┌─────────────────────────────────────────────────────────────────┐
│            StackOverflow Oracle 原始 JSON                        │
│            152,758 个问题，589 MB                                 │
└─────────────────────────────┬───────────────────────────────────┘
                              ↓ 工序 1: JSON 转 JSONL
                              ↓ 工序 2: 上传 HDFS
                              ↓ 工序 3: Spark 清洗（去 HTML、抽 ORA 码）
                              ↓
                       ┌──────┴──────┐
                       │  questions  │  Parquet, 干净文本
                       │   parquet   │
                       └──────┬──────┘
                              ↓ 工序 4: 特征工程（文本 → 向量）
                              ↓
                       ┌──────┴──────┐
                       │   features  │  每问题一个 26 万维向量
                       │   parquet   │
                       └──────┬──────┘
                              ↓ 工序 5: BisectingKMeans 主题聚类
                              ↓        （把 152K 问题分成 K 个主题）
                       ┌──────┴──────┐
                       │ topic_      │  每个问题一个 topic_id
                       │ clusters    │
                       └──────┬──────┘
                              ↓ 工序 6: 簇内 MinHashLSH
                              ↓        （在每个主题内部找相似对）
                       ┌──────┴──────┐
                       │  similar_   │  一对一对的相似问题
                       │   pairs     │
                       └──────┬──────┘
                              ↓ 工序 7: 连通分量
                              ↓        （把相似对合并成"组"）
                       ┌──────┴──────┐
                       │   clusters  │  ★最终成品：重复问题组
                       └─────────────┘
```

### v1 错在哪、v2 怎么修

**v1 旧设计（错的）**：主题聚类（工序 5）跑完后没接到 LSH，LSH 直接全局做。

```
v1 错：
features ─┬─→ 工序 5：BisectingKMeans ──→ topic_clusters    ← 死胡同，没人用
          └─→ 全局 LSH（直接在 152K 上比对）──→ 重复对 2 个

   问题：全局比对候选爆炸，必须用严阈值 sim≥0.85，所以只找到 2 组重复
```

**v2 新设计（修复）**：主题聚类的输出接到 LSH 输入，LSH 在每个主题簇**内部**做。

```
v2 对：
features ──→ 工序 5：BisectingKMeans ──→ topic_clusters
                                              ↓
                  工序 6：簇内 MinHashLSH（每个主题独立跑）
                                              ↓
                                          similar_pairs
                                              ↓
                                       工序 7：连通分量
                                              ↓
                                           重复组 X 个

   原理：每个主题只有几千文档，比对量从 N² 砍到 Σ(n_i²)，
        阈值可以放松到 sim≥0.65，能找到的组数多得多
```

### 为什么要跑 4 次，每次有什么不同

**这次实验跑同一条流水线 4 遍**，每次只改一个参数：K（主题数）。这样能对比 "K 值如何影响结果"，得到"K 值实验"那张表。

| 跑哪次 | K | 主题聚类 | 簇内 LSH | 预计耗时 |
|---|---|---|---|---|
| 第 1 次 | 50 | 复用 v1 已有的 | 跑 | 10–20 分钟 |
| 第 2 次 | 100 | 新训 | 跑 | 25–40 分钟 |
| 第 3 次 | 150 | 新训 | 跑 | 25–40 分钟 |
| 第 4 次 | 200 | 新训 | 跑 | 25–40 分钟 |

每次都会同时产出"主题聚类结果"和"重复对/重复组结果"，**不是分两个任务，是一条流水线一次跑完**。

总集群机时大约 **1.5–2.5 小时**。

---

## 1. 开始之前的检查清单

| 项 | 怎么验证 | 预期结果 |
|---|---|---|
| 在 master 节点上 | `hostname` | `master` |
| 切到 v2 分支 | `cd /opt/bigdata/project-stackoverflow-clustering && git branch --show-current` | `v2` |
| 拉了最新代码 | `git log --oneline -1` | 最新一条是 `v2(D5)` 或之后的 commit |
| HDFS 在跑 | `hdfs dfsadmin -report \| head -3` | 看到 `Live datanodes (2)` |
| YARN 在跑 | `yarn node -list 2>/dev/null` | 列出 worker1 / worker2 状态 RUNNING |
| features 已存在 | `hdfs dfs -ls /user/bigdata/stackoverflow/parquet/features \| head -3` | 看到 `_SUCCESS` 文件 |
| K=50 主题聚类已存在 | `hdfs dfs -ls /user/bigdata/stackoverflow/output/topic_clusters/ \| head -3` 或 `.../topic_clusters_k50/` | 看到 `_SUCCESS` 文件 |

如果 features 不存在，先跑：

```bash
bash scripts/04_submit_preprocess.sh    # 约 5 分钟
bash scripts/05_submit_cluster.sh       # 约 30-60 分钟
```

---

## 2. 执行步骤（按顺序）

> 所有命令在 master 节点项目目录下执行：
> ```bash
> cd /opt/bigdata/project-stackoverflow-clustering
> ```
> 任何一步报错先停下来截图错误，看"常见错误"小节。

### 步骤 1：把 v1 已有的 topic_clusters 改名

```bash
hdfs dfs -test -e /user/bigdata/stackoverflow/output/topic_clusters && echo EXISTS || echo NO_OLD

# 如果上一行输出 EXISTS，执行下面这行；输出 NO_OLD 就跳过
hdfs dfs -mv /user/bigdata/stackoverflow/output/topic_clusters \
            /user/bigdata/stackoverflow/output/topic_clusters_k50
```

**看什么**：

```bash
hdfs dfs -ls /user/bigdata/stackoverflow/output/ | grep topic
```

应该看到 `topic_clusters_k50`（如果是新装的项目可能没有，那就让脚本自动训练）。

---

### 步骤 2：先做小规模烟测

只跑前 5 个最大主题验证脚本不挂，约 5 分钟。

```bash
MAX_TOPICS=5 bash scripts/14_submit_cluster_intra_topic.sh k50 50 2>&1 | tee /tmp/smoke_k50.log
```

**看什么**：

终端最后几行应该出现：

```
Done. Outputs under hdfs:///user/bigdata/stackoverflow/output/intra_topic/k50/
```

```bash
hdfs dfs -ls /user/bigdata/stackoverflow/output/intra_topic/k50/
```

应该看到若干目录：`similar_pairs/`、`clusters/`、`summary_csv/` 等。

烟测通过后**清掉烟测产物**再跑全量：

```bash
hdfs dfs -rm -r -f /user/bigdata/stackoverflow/output/intra_topic/k50
```

---

### 步骤 3：实验 1 — K=50（约 10–20 分钟）

```bash
bash scripts/14_submit_cluster_intra_topic.sh k50 50 2>&1 | tee /tmp/intra_k50.log
```

K=50 主题聚类已存在，脚本只跑簇内 LSH 部分，所以快。

**期间另一个终端可看进度**：

```bash
yarn application -list 2>/dev/null | grep -i intra
```

**看什么（跑完之后）**：

```bash
hdfs dfs -cat /user/bigdata/stackoverflow/output/intra_topic/k50/summary_csv/part-*.csv
```

应该看到一行 CSV 类似：

```
run_label,num_hash_tables,similarity_threshold,distance_threshold,pair_count,multi_doc_cluster_count,multi_doc_question_count,max_cluster_size,avg_similarity
k50,4,0.65,0.35,42,38,76,3,0.7234
```

**这一行就是 PPT 的核心数字**：

| 字段 | 含义 |
|---|---|
| `pair_count` | 找到几对相似问题 |
| `multi_doc_cluster_count` | **几个重复问题组**（PPT 主指标，对标对方 52 组）|
| `multi_doc_question_count` | 这些组共涉及几个问题 |
| `max_cluster_size` | 最大重复组的大小 |
| `avg_similarity` | 所有相似对的平均相似度 |

期望区间：`multi_doc_cluster_count` 在 30–60 之间。如果是 0 或个位数，去看"常见错误"。

---

### 步骤 4：实验 2 — K=100（约 25–40 分钟）

```bash
bash scripts/14_submit_cluster_intra_topic.sh k100 100 2>&1 | tee /tmp/intra_k100.log
```

这次包含两个 Spark 任务：先训练 K=100 主题聚类，再跑簇内 LSH。

**看什么**：

```bash
hdfs dfs -cat /user/bigdata/stackoverflow/output/intra_topic/k100/summary_csv/part-*.csv
```

期望：`multi_doc_cluster_count` 比 K=50 那组**多一些**（搜索空间更小，召回更高）。

期望区间：40–80。

---

### 步骤 5：实验 3 — K=150（约 25–40 分钟）

```bash
bash scripts/14_submit_cluster_intra_topic.sh k150 150 2>&1 | tee /tmp/intra_k150.log
```

**看什么**：

```bash
hdfs dfs -cat /user/bigdata/stackoverflow/output/intra_topic/k150/summary_csv/part-*.csv
```

期望区间：50–90。

---

### 步骤 6：实验 4 — K=200（约 25–40 分钟）

```bash
bash scripts/14_submit_cluster_intra_topic.sh k200 200 2>&1 | tee /tmp/intra_k200.log
```

**看什么**：

```bash
hdfs dfs -cat /user/bigdata/stackoverflow/output/intra_topic/k200/summary_csv/part-*.csv
```

期望区间：55–100。注意：K 增大到一定程度后召回提升会**收敛**（参考对方 PPT，他们 K=150 vs K=200 的最大簇占比几乎一样）。如果 K=200 的重复组数比 K=150 增加不到 10%，说明已经进入收敛区，PPT 上可以讲"K=150 是性价比最高的选择"。

---

### 步骤 7：把 HDFS 上的结果拉回本地

```bash
PROJ=/opt/bigdata/project-stackoverflow-clustering

mkdir -p $PROJ/output/hdfs_output

# intra_topic 实验结果（4 个 K 都在同一个目录里）
hdfs dfs -get -f /user/bigdata/stackoverflow/output/intra_topic \
   $PROJ/output/hdfs_output/

# 4 个 K 的主题聚类结果（用于算最大簇占比）
for K in 50 100 150 200; do
  hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_clusters_k$K \
     $PROJ/output/hdfs_output/ 2>/dev/null || \
     echo "warn: topic_clusters_k$K 不存在，跳过"
done

# 兼容：如果 K=50 还叫旧名 topic_clusters
[ ! -d $PROJ/output/hdfs_output/topic_clusters_k50 ] && \
  hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_clusters \
  $PROJ/output/hdfs_output/topic_clusters_k50 2>/dev/null
```

**看什么**：

```bash
ls $PROJ/output/hdfs_output/intra_topic/
# 应该看到：k50  k100  k150  k200

ls $PROJ/output/hdfs_output/ | grep topic_clusters
# 应该看到 4 个：topic_clusters_k50  topic_clusters_k100  topic_clusters_k150  topic_clusters_k200
```

---

### 步骤 8：本机生成 PPT 用的对比表

```bash
cd $PROJ   # 或本机仓库根

# 8a) K 值实验对比表：最大簇占比 / Top 5/10/20 占比
python3 src/analyze_topic_k_runs.py \
   k50=output/hdfs_output/topic_clusters_k50 \
   k100=output/hdfs_output/topic_clusters_k100 \
   k150=output/hdfs_output/topic_clusters_k150 \
   k200=output/hdfs_output/topic_clusters_k200

# 8b) 簇内 LSH 对比表：重复对数 / 重复组数 / 平均相似度
python3 src/summarize_intra_topic_runs.py
```

**看什么**：

每个命令打印一张表到终端：

```
 label    k  largest   share   top5  top10  top20
   k50   50    35914 23.51% 56.22% 67.58% 82.94%
  k100  100    28xxx  ~19% ...
  k150  150    23xxx  ~15% ...
  k200  200    23xxx  ~15% ...
```

```
   k50: pairs=42  groups=38  questions=76  avg_sim=0.7234
  k100: pairs=65  groups=58  questions=120 avg_sim=0.7100
  k150: pairs=87  groups=72  questions=148 avg_sim=0.7012
  k200: pairs=92  groups=76  questions=156 avg_sim=0.6987
```

并生成两个 CSV：

| 文件 | 用途 |
|---|---|
| `output/evaluation/topic_k_comparison.csv` | PPT "K 值实验" 那一页 |
| `output/evaluation/intra_topic_comparison.csv` | PPT "最终去重结果" 那一页 |

---

### 步骤 9：把新结果注入网页 Demo（可选）

```bash
python3 src/build_demo_assets.py
open demo/index.html    # macOS；Linux 用 xdg-open，Windows 用 start
```

---

## 3. PPT 应该展示什么

### 第一张：K 值实验对比

读 `output/evaluation/topic_k_comparison.csv`：

| K | 最大簇规模 | 最大簇占比 | Top 5 占比 | Top 20 占比 | 中位簇 |
|---|---|---|---|---|---|
| 50 | 35,914 | 23.51% | 56.22% | 82.94% | 1,175 |
| 100 | ~28,000 | ~19% | ~50% | ~75% | ~700 |
| 150 | ~23,000 | ~15% | ~40% | ~70% | ~550 |
| 200 | ~23,000 | ~15% | ~40% | ~70% | ~300 |

**讲点**：
- 增大 K 让最大簇变小，分布更均匀
- K 从 50 → 150 最大簇占比从 23.5% 降到 15%
- K 从 150 → 200 最大簇占比几乎不变，**说明 K=150 已经进入收益递减区**
- 对标对方 PPT：他们也是这个结论，最终选 K=150

### 第二张：去重组数对比（核心杀手数据）

读 `output/evaluation/intra_topic_comparison.csv`：

| 配置 | hash 表 | 阈值 | 找到对数 | **重复组数** | 涉及问题数 | 平均相似度 |
|---|---|---|---|---|---|---|
| v1 全局 LSH（旧） | 4 | 0.85 | 3 | 3 | 6 | 0.868 |
| v2 K=50 簇内 | 4 | 0.65 | ~42 | **~38** | ~76 | 0.72 |
| v2 K=100 簇内 | 4 | 0.65 | ~65 | **~58** | ~120 | 0.71 |
| v2 K=150 簇内 | 4 | 0.65 | ~87 | **~72** | ~148 | 0.70 |
| v2 K=200 簇内 | 4 | 0.65 | ~92 | **~76** | ~156 | 0.70 |

（数字以你跑出来的为准）

**讲点**：
- v1 全局 LSH 候选爆炸，被迫用严阈值 0.85，只找到 3 组
- v2 把主题聚类接进 LSH 后，搜索空间从 N² 缩到 Σ(n_i²)
- 同样阈值 0.65，K 越大重复组越多（因为搜索空间越小）
- K=150 → K=200 增益变小，再次印证 K=150 是性价比最佳

### 第三张：典型重复组样例

读 `output/hdfs_output/intra_topic/k150/clusters_samples_csv/part-*.csv`，挑 5–8 行人工觉得真的是重复的组放上去。

---

## 4. 关键参数可调

`scripts/14_submit_cluster_intra_topic.sh` 接受这些环境变量：

| 变量 | 默认 | 调高 → | 调低 → |
|---|---|---|---|
| `INTRA_SIM_THRESHOLD` | 0.65 | 找到的对更少但更准 | 找到更多但杂质多 |
| `INTRA_HASH_TABLES` | 4 | LSH 召回更高，耗时翻倍 | 更快但漏更多 |
| `MAX_TOPICS` | 0（全部）| — | >0 时只跑前 N 大主题（烟测）|
| `FORCE_RETRAIN` | 0 | 1 = 强制重训主题聚类 | — |
| `ORA_BOOST` | 0.10 | 共享 ORA 码加分越多 | 0 关闭 |
| `ORA_RESCUE_FLOOR` | 0.0 | 设 0.55 时低相似度但共享 ORA 码的对被救回 | 0 关闭 |

**如果某个 K 跑出的重复组数太少（< 10）**，按这个顺序调：

1. `INTRA_SIM_THRESHOLD=0.55 bash scripts/14_submit_cluster_intra_topic.sh k50 50`
2. 还少：`INTRA_HASH_TABLES=8 INTRA_SIM_THRESHOLD=0.60 bash ...`
3. 还少：`ORA_RESCUE_FLOOR=0.55 bash ...`

---

## 5. 时间不够怎么办

完整跑 4 个 K 需要 1.5–2.5 小时集群机时。如果时间紧，按重要性砍：

| 时间预算 | 建议跑哪些 |
|---|---|
| ≥ 2 小时 | 全跑 K=50/100/150/200（最稳，对标对方完整） |
| 1.5 小时 | K=50/100/150（K=200 可以从 K=150 数据外推） |
| 1 小时 | K=50/150（两端对比，省略中间）|
| 30 分钟 | 只跑 K=50（最差情况，至少有 v2 vs v1 对比）|

K=50 必须跑（已存在主题聚类，机时最少，且是修复 v1 bug 的核心证据）。

---

## 6. 常见错误和怎么排查

### A. `Reusing existing topic clusters` 但你想重训

```bash
FORCE_RETRAIN=1 bash scripts/14_submit_cluster_intra_topic.sh k150 150
```

### B. `pyarrow not found`（步骤 8a 时）

```bash
pip3 install --user pyarrow
# 公司内网装不上时：
pip3 install --user --index-url https://pypi.org/simple/ pyarrow
```

### C. Spark 任务卡住超过 30 分钟

```bash
# 查 application id
yarn application -list 2>/dev/null

# 查日志最后 200 行
yarn logs -applicationId <ID> 2>/dev/null | tail -200
```

如果是 OOM，把 shuffle 分区数加倍：

```bash
SHUFFLE_PARTITIONS=192 bash scripts/14_submit_cluster_intra_topic.sh k50 50
```

### D. `summary_csv` 是空的或 `pair_count=0`

可能阈值太严或 features 没有 ora_codes 列。先用低阈值试探：

```bash
INTRA_SIM_THRESHOLD=0.50 MAX_TOPICS=3 bash scripts/14_submit_cluster_intra_topic.sh debug 50
```

如果还是 0：去 `topic_metrics_csv` 看每个 topic 的 status 是不是有大量 `error:*`。

### E. 步骤 9 build_demo_assets.py 报错说找不到 csv

如果只跑了部分实验没跑全，先跳过 9 也可以，PPT 表会少几行。

---

## 7. 一页清单（带去集群打印）

```
[ ] cd /opt/bigdata/project-stackoverflow-clustering
[ ] git checkout v2 && git pull
[ ] 检查清单（第 1 节）全部通过
[ ] hdfs dfs -mv .../topic_clusters .../topic_clusters_k50  （如果旧目录存在）
[ ] MAX_TOPICS=5 bash scripts/14_submit_cluster_intra_topic.sh k50 50   （烟测）
[ ] hdfs dfs -rm -r -f .../intra_topic/k50  （清烟测）

[ ] bash scripts/14_submit_cluster_intra_topic.sh k50  50    ← 实验 1
[ ]   hdfs dfs -cat .../intra_topic/k50/summary_csv/part-*.csv   ← 记数字
[ ] bash scripts/14_submit_cluster_intra_topic.sh k100 100   ← 实验 2
[ ]   hdfs dfs -cat .../intra_topic/k100/summary_csv/part-*.csv  ← 记数字
[ ] bash scripts/14_submit_cluster_intra_topic.sh k150 150   ← 实验 3
[ ]   hdfs dfs -cat .../intra_topic/k150/summary_csv/part-*.csv  ← 记数字
[ ] bash scripts/14_submit_cluster_intra_topic.sh k200 200   ← 实验 4
[ ]   hdfs dfs -cat .../intra_topic/k200/summary_csv/part-*.csv  ← 记数字

[ ] hdfs dfs -get .../intra_topic 本地目录
[ ] hdfs dfs -get .../topic_clusters_k{50,100,150,200} 本地目录
[ ] python3 src/analyze_topic_k_runs.py k50=... k100=... k150=... k200=...
[ ] python3 src/summarize_intra_topic_runs.py
[ ] 把两张表填进 PPT
```

---

## 8. 跑完之后告诉我什么

发回这三样东西，我可以帮写 PPT 那两页：

1. `output/evaluation/topic_k_comparison.csv` 的内容
2. `output/evaluation/intra_topic_comparison.csv` 的内容
3. `output/hdfs_output/intra_topic/k150/clusters_samples_csv/part-*.csv` 前 20 行

如果没跑全 4 个 K，跑了几个就发几个，告诉我即可。
