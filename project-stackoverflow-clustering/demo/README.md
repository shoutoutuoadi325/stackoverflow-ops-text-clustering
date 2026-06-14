# Demo 界面说明

这是一个零前端依赖的静态展示页，用于答辩或本地演示 StackOverflow Oracle 运维文本聚类结果。

生成数据：

```bash
python src/validate_raw_data.py --input ../StackOverFlow_Oracle_Database/oracle_database_questions.json
python src/build_review_candidates.py
python src/evaluate_review_labels.py
python src/local_similarity_sweep.py --input ../StackOverFlow_Oracle_Database/oracle_database_questions.json
python src/summarize_sweep_results.py
python src/build_demo_assets.py
```

打开页面：

```text
demo/index.html
```

页面包含：

- 原始数据文件大小、SHA256 和直接核验计数
- 全量 EDA 指标卡片
- Top 标签和 Oracle 错误码条形图
- 参数扫描汇总和推荐参数说明
- 主题聚类样例表
- 重复问题簇与相似问题对列表
- 人工审核样本与 precision 状态
- 关键词或 question_id 查询

如果重新运行了 Spark 聚类或更高召回参数实验，再执行一次 `python src/build_demo_assets.py` 即可刷新 `demo/data.js`。
