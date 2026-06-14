# Demo 演示说明

推荐现场使用静态 Demo 页面，命令行查询作为备用。静态 Demo 读取已经导出的真实结果文件，不依赖现场 Spark 作业，因此最稳定。

## 1. 上台前准备

进入项目目录：

```bash
cd /Users/zhiqizhang/development/stackoverflow-ops-text-clustering/project-stackoverflow-clustering
```

确认关键文件存在：

```bash
ls demo/index.html demo/data.js
ls output/similar_pair_samples.csv output/cluster_samples.csv
ls output/evaluation/parameter_sweep_summary.csv
```

启动本地静态服务：

```bash
python3 -m http.server 8765 --bind 127.0.0.1 --directory demo
```

浏览器打开：

```text
http://127.0.0.1:8765/index.html
```

如果不想启动服务，也可以直接打开：

```text
demo/index.html
```

但建议使用本地服务方式，浏览器兼容性更稳。

## 2. 推荐演示顺序

### 第一步：展示数据真实性

页面打开后先停留在顶部数据概览区域。

讲法：

> 这里展示的是原始数据核验结果，包括文件大小、SHA256、问题数、回答数和评论数。所有数字来自本地真实数据，不是手工填写。

重点强调：

- 问题数：152,758
- 回答数：203,393
- 总文本对象：908,089
- 数据来自 StackOverflow Oracle Database 问答

### 第二步：展示 EDA 和标签分布

滚动到 EDA 或 Top 标签区域。

讲法：

> 可以看到 oracle-database、sql、plsql、oracle11g、java 等标签占比较高，说明数据确实集中在数据库运维、SQL、PL/SQL 和 Java 连接 Oracle 这些技术场景。

### 第三步：展示参数扫描汇总

滚动到参数或模型结果区域，找到 `yarn_sim065_ht2_bucket200`、`yarn_sim085_ht4_bucket300` 等结果。

讲法：

> 我最终选择 0.65/ht2/bucket200 作为展示配置，因为它在真实 YARN 全量数据上发现了 16 对相似问题和 16 个重复簇；0.85 配置更严格，只发现 3 对，用来说明高置信和召回之间的取舍。

如果老师问为什么不说“最优”：

> 因为还没有人工标签，所以不能负责任地说某组参数 F1 最优。项目里已经生成了 82 条 review candidates，后续标注后才能计算真实 precision。

### 第四步：展示相似问题对

滚动到相似问题对区域，重点展示这些样例：

| 问题 1 | 问题 2 | similarity |
|---|---|---:|
| join with lookup and groupby | join with lookup and group by | 0.8667 |
| Correlation and analytic function | Correlation function over analytic function | 0.8500 |
| JDBC Thin Oracle 11g | JDBC Thin in Oracle 11g with java | 0.7742 |

讲法：

> 这些结果能直观看到标题语义、技术关键词和标签高度重合，所以适合解释 LSH 去重的实际效果。

### 第五步：使用搜索功能

在 Demo 页面的搜索框中尝试：

```text
analytic function
```

或输入 question id：

```text
28753859
```

讲法：

> 这里用关键词或 question_id 查询，可以快速定位到相似问题对和所在重复簇。比如 28753859 对应的标题是 Correlation and analytic function，它和另一个 analytic function 问题相似度为 0.85。

### 第六步：展示人工审核状态

滚动到 review candidates / precision 状态区域。

讲法：

> 当前 precision 显示为待人工审核，这是有意保留的真实状态。只有人工填写 label 后，脚本才会计算 precision 和 F1。

## 3. 命令行备用 Demo

如果浏览器展示出现问题，可以切换到命令行。

Top clusters：

```bash
bash scripts/07_query_demo.sh --top-clusters --limit 10
```

关键词查询：

```bash
bash scripts/07_query_demo.sh --keyword "analytic function" --limit 10
```

另一个关键词：

```bash
bash scripts/07_query_demo.sh --keyword "group by" --limit 10
```

按问题 ID 查询：

```bash
bash scripts/07_query_demo.sh --question-id 28753859 --limit 10
```

命令行讲法：

> 这个命令行 Demo 直接读取 HDFS 上的 clusters 和 similar_pairs 输出，适合证明 Spark 输出不是只做了静态页面，而是可以被程序查询。

## 4. 演示时不要做的事

不要现场重新跑全量 Spark 聚类：

```bash
bash scripts/05_submit_cluster.sh
```

原因：

- 全量 YARN 任务通常需要 20-40 分钟以上。
- approxSimilarityJoin 全量压力测试在当前资源下出现过长尾。
- 现场演示应展示已经复核过的真实输出，而不是临时等待集群任务。

不要声称 precision/F1 已经有数值：

- 当前 `output/evaluation/review_metrics.json` 显示 `pending_labels`。
- 没有人工 label 时，不能声称真实 precision。

## 5. 如果老师追问

### 问：Demo 是不是假数据？

答：

> 不是。Demo 读取的是项目脚本从真实数据、Spark/YARN 输出和核验文件生成的 `demo/data.js`。顶层样例 CSV 也已经同步为非空文件，评审可以直接打开 `output/similar_pair_samples.csv` 和 `output/cluster_samples.csv` 复核。

### 问：为什么不用 0.85 作为最终参数？

答：

> 0.85 更严格，平均相似度更高，但只发现 3 对，展示效果较弱。0.65 能展示 16 对和 16 个簇，更适合说明系统能发现相似问题。没有人工标签前，我不会说 0.65 的 F1 最优，只说它更适合高召回展示。

### 问：为什么全量 approx 没跑完？

答：

> 代码已经接入并通过了 YARN smoke test。但全量 `0.75/ht4/approx` 在当前资源下出现 executor 心跳超时和 stage 9 长尾，约 29 分钟后仍只完成 7/37 个 stage 9 任务，所以我把它作为工程压力测试记录下来，最终展示采用已完整跑完的 bucket 结果。

### 问：如何进一步提升？

答：

> 第一是补人工标签，计算真实 precision；第二是继续跑完整参数网格；第三是引入 Sentence-BERT 这类语义向量，提高对改写句和同义表达的召回。

## 6. 演示前 3 分钟检查清单

- PPT 能打开：[运维文本聚类项目答辩.pptx](运维文本聚类项目答辩.pptx)
- Demo 服务能打开：`http://127.0.0.1:8765/index.html`
- 搜索 `analytic function` 有结果
- 搜索 `28753859` 有结果
- `output/similar_pair_samples.csv` 不是空表
- `output/cluster_samples.csv` 不是空表
- 不要关闭本地 HTTP server 终端
