# v2 创新点 - 怎么跑

本分支实现了四个答辩创新点：D1 三层诚实评测、D2 标签分块 LSH、D3 ORA 错误码
强信号、D4 主题 × 去重双层视图。所有代码已写好；下面是你在集群和本机上需要
执行的命令。叙事和答辩话术见 `docs/创新点叙事与答辩话术.txt`。

## 在 master 节点上跑（HDFS / Spark on YARN）

把 v2 分支拉到 master 后，按下面顺序提交。每一步的脚本都已经存在，直接 bash
就可以。

```bash
# 1) 重跑预处理：现在会多生成 ora_codes 列
bash scripts/04_submit_preprocess.sh

# 2) 重跑特征工程：透传 ora_codes 到 features parquet
#    （脚本 05 包含 feature_engineering、topic_clustering、cluster_lsh、
#     connected_components 四个 spark-submit）
bash scripts/05_submit_cluster.sh

# 3) D3：抽 ORA 错误码统计 + 救回对样例
bash scripts/11_submit_ora_stats.sh

# 4) D2：标签分块 LSH（独立产物，不覆盖默认 similar_pairs）
#    建议先做一次小规模 smoke test，把 MAX_TAG_BUCKETS 设小一点：
MAX_TAG_BUCKETS=5 bash scripts/12_submit_cluster_partitioned.sh
#    smoke 通过后再跑完整的：
bash scripts/12_submit_cluster_partitioned.sh

# 5) D4：主题 × 去重双层视图
bash scripts/13_submit_topic_dedup_view.sh

# 6) 导出全部新输出到本地（更新 06_export_demo.sh 之外的目录可手动 hdfs dfs -get）
bash scripts/06_export_demo.sh
hdfs dfs -get -f /user/bigdata/stackoverflow/output/ora_codes \
    /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output/
hdfs dfs -get -f /user/bigdata/stackoverflow/output/ora_showcase \
    /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output/
hdfs dfs -get -f /user/bigdata/stackoverflow/output/topic_dedup_view_csv \
    /opt/bigdata/project-stackoverflow-clustering/output/hdfs_output/
hdfs dfs -get -f /user/bigdata/stackoverflow/output/similar_pairs_partitioned_metrics_csv \
    /opt/bigdata/project-stackoverflow-clustering/output/evaluation/
```

参数关键开关（`conf/app.conf` 已有默认值）：

- `ORA_BOOST=0.10` — 共享 ORA 码时给相似度的加分；设 0 关闭。
- `ORA_RESCUE_FLOOR=0.55` — 救回阈值；raw similarity 低于
  `SIMILARITY_THRESHOLD` 但 ≥ 此值且共享 ORA 码时被保留并打 `ora_rescued=true`。
- `MIN_TAG_BUCKET_SIZE=50` — 比这小的 tag 桶折叠进 `_misc`。
- `PRIMARY_TAG_EXCLUDE=oracle-database` — 选 primary_tag 时跳过的 tag。
- `MAX_TAG_BUCKETS` — 设大于 0 时只跑前 N 个最大的桶；smoke test 用。

## 在本机上跑（生成 review 候选 + 标注 + Demo）

```bash
# 1) 重新生成 review_candidates.csv，把 shared_ora_codes / ora_match 列带出来
python3 src/build_review_candidates.py

# 2) 交互式标注 50 条（约 25 分钟）
#    y=重复  n=不重复  ?=不确定  s=跳过  u=撤销  o <text>=补注  q=退出
python3 src/label_review_candidates_interactive.py --target 50

# 3) 重算 precision、Wilson 95% 区间，按阈值 / 相似度档 / ORA 命中分组
python3 src/evaluate_review_labels.py
cat output/evaluation/review_metrics.json

# 4) 重新生成 demo/data.js 注入新视图
python3 src/build_demo_assets.py
open demo/index.html  # macOS；Windows 用 start，Linux 用 xdg-open
```

打开后会看到三个新 tab：
- **主题 × 去重**：D4 的核心展示
- **ORA 强信号**：D3 的覆盖率 / 救回对 / 高相似匹配
- 评测 tab 里 review_metrics.json 已有 Wilson 区间和 by_ora_match 分档

## 答辩准备清单

跑完上面的步骤后，PPT 需要的素材都在仓库里：

| 创新点 | 核心数字 / 表 | 文件 |
|---|---|---|
| D1 评测 | 50 条 precision、Wilson CI、按阈值/ORA 分档 | `output/evaluation/review_metrics.json` |
| D2 分块 LSH | 每桶耗时 + 总跑通时间对比全局 long-tail | `output/evaluation/.../similar_pairs_partitioned_metrics_csv` + `docs/verification/full_approx075_2026-06-14.md` |
| D3 ORA 强信号 | 覆盖率、Top 20 码、救回对样例 | `output/hdfs_output/ora_codes/`、`output/hdfs_output/ora_showcase/` |
| D4 双层视图 | 重复率最高的 Top 主题 | `output/hdfs_output/topic_dedup_view_csv/part-*.csv` |

完整答辩话术在 `docs/创新点叙事与答辩话术.txt`。
