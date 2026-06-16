# v2 主题簇内 LSH（D5）— 怎么跑

这是修复"主题聚类没参与去重"那个设计错误的代码。修复后流程：

```
features → BisectingKMeans (K=50 / K=150) → 簇内 MinHashLSH (sim=0.65) → 重复组
```

K=50 的主题聚类 parquet 你们已经在 v1 跑过（HDFS 上的 `topic_clusters` 目录），脚本会自动复用，不会重训。K=150 需要重新跑一次主题聚类。

## 上集群执行（master 节点，v2 分支）

```bash
# 0) 确保 features 已经存在（v1 已经跑过；如果你想用 v2 的 ora_codes 再跑一次更稳）
#    跳过这一步也可以，cluster_lsh_intra_topic 会自动检测 ora_codes 列是否存在
bash scripts/04_submit_preprocess.sh   # 可选
bash scripts/05_submit_cluster.sh      # 可选；仅当上一步重跑时需要

# 1) 把 v1 已有的 topic_clusters 改名成带 K 标识，方便和 K=150 区分
hdfs dfs -test -e /user/bigdata/stackoverflow/output/topic_clusters && \
  hdfs dfs -mv /user/bigdata/stackoverflow/output/topic_clusters \
              /user/bigdata/stackoverflow/output/topic_clusters_k50 || true

# 2) K=50 簇内 LSH（复用已有 topic_clusters_k50，约 10-20 分钟）
bash scripts/14_submit_cluster_intra_topic.sh k50 50

# 3) K=150 主题聚类 + 簇内 LSH（重训主题聚类，约 25-40 分钟总）
bash scripts/14_submit_cluster_intra_topic.sh k150 150

# 4) 拉结果回本地
mkdir -p /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output/intra_topic
hdfs dfs -get -f /user/bigdata/stackoverflow/output/intra_topic \
  /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output/

mkdir -p /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output
hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_clusters_k50 \
  /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output/
hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_clusters_k150 \
  /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output/
```

## 烟测（先小规模验证脚本不挂）

K=50 全量预计 10-20 分钟，第一次跑前可以先用 `MAX_TOPICS` 限制跑前 5 个最大主题：

```bash
MAX_TOPICS=5 bash scripts/14_submit_cluster_intra_topic.sh k50 50
```

通了再跑全量（不带 `MAX_TOPICS`）。

## 拿到本地以后（本机执行）

```bash
cd /opt/bigdata/project-stackoverflow-clustering   # 或你本机仓库路径

# 5) 生成 K 值对比表（最大簇占比、Top 5 / 10 / 20 占比）
python3 src/analyze_topic_k_runs.py \
  k50=output/hdfs_output/topic_clusters_k50 \
  k150=output/hdfs_output/topic_clusters_k150

# 6) 生成簇内 LSH 对比表（重复对数、重复组数）
python3 src/summarize_intra_topic_runs.py

# 7) 把新结果注入 demo
python3 src/build_demo_assets.py
open demo/index.html
```

跑完会有两张关键 CSV，PPT 直接用：

| 文件 | 内容 |
|---|---|
| `output/evaluation/topic_k_comparison.csv` | K=50 vs K=150 的最大簇占比、Top N 占比 |
| `output/evaluation/intra_topic_comparison.csv` | K=50 vs K=150 的重复对数、重复组数、平均相似度 |

## 关键调参（按需）

`14_submit_cluster_intra_topic.sh` 接受这些环境变量：

| 变量 | 默认 | 含义 |
|---|---|---|
| `INTRA_SIM_THRESHOLD` | 0.65 | 簇内相似度阈值。可以调到 0.55 看候选量；0.75 看高精度 |
| `INTRA_HASH_TABLES` | 4 | MinHashLSH 的 hash 表数 |
| `MAX_TOPICS` | 0 | 0=所有主题；>0 时只跑前 N 个最大的主题 |
| `FORCE_RETRAIN` | 0 | 1=强制重训主题聚类，忽略已有 parquet |
| `ORA_BOOST` | 0.10 | 共享 ORA 码加分（来自 D3）|
| `ORA_RESCUE_FLOOR` | 0.0 | 0 时关闭 ORA 救回；建议设 0.55 |

## 为什么这一版能拿到比 v1 多得多的重复组

v1 全局 LSH 在 152K 文档上跑，候选数 6,858,115（0.85 阈值），所以阈值不能再松，结果只有 2 对。  
v2 K=50 簇内 LSH 总搜索空间 = Σ(n_i²) ≈ 全局的 1/15（按当前 K=50 簇大小估算）—— 同样阈值 0.65 也不会候选爆炸，能跑出大概 30-100 组重复（具体看实测）。  
K=150 进一步把搜索空间缩小到全局的 1/35 左右，可以容许更松的阈值或者更多的 hash 表，召回更高。

如果跑出来组数还不够多，第一档调整：

1. `INTRA_SIM_THRESHOLD=0.55`（最有效）
2. `INTRA_HASH_TABLES=8`（增加 LSH 召回，代价是耗时翻倍）
3. `ORA_RESCUE_FLOOR=0.55`（让共享 ORA 码的低相似度对被救回）

## 答辩叙事

PPT 的"算法设计"那一页改成：

> **v1 设计**：主题聚类和 LSH 各跑各的，LSH 全局做 → 受限于候选爆炸只能用 sim>=0.85 → 只找到 2 组重复  
> **v2 修复**：把主题聚类作为候选生成阶段，LSH 在每个主题簇内独立运行 → 搜索空间砍到 1/15 → 可以放宽到 sim>=0.65 → 重复组数 X 倍提升  
> **K 值实验**：K=50 vs K=150 的最大簇占比 23.5% → 15% 左右；重复组数 X → Y

这是真实的项目演进故事，不是事后包装的"创新点"。
