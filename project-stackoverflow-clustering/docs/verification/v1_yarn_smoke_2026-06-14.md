# v1 YARN Smoke Test - 2026-06-14

## Purpose

Validate the v1 clustering changes on the real three-node Hadoop/Spark cluster without overwriting the formal project outputs.

## Environment

| Item | Value |
|---|---|
| Host | master |
| Java | OpenJDK 1.8.0_412 |
| Hadoop | 3.3.6 |
| Spark | 3.5.1 |
| Remote test project | `/opt/bigdata/project-stackoverflow-clustering-v1-test` |
| HDFS smoke root | `/user/bigdata/stackoverflow/tmp/v1_smoke` |

Existing HDFS inputs were confirmed:

```text
/user/bigdata/stackoverflow/parquet/features
/user/bigdata/stackoverflow/parquet/questions
```

## Commands

Prepared a 14-question HDFS smoke subset containing known similar-pair IDs:

```bash
spark-submit --master yarn --deploy-mode client \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --num-executors 1 \
  /tmp/prepare_v1_smoke.py
```

Ran the new default LSH path with Spark ML `approxSimilarityJoin`:

```bash
spark-submit --master yarn --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 1 \
  src/cluster_lsh.py \
  --features hdfs:///user/bigdata/stackoverflow/tmp/v1_smoke/features \
  --output hdfs:///user/bigdata/stackoverflow/tmp/v1_smoke/similar_pairs_approx \
  --metrics-output hdfs:///user/bigdata/stackoverflow/tmp/v1_smoke/lsh_metrics_approx_csv \
  --distance-threshold 0.35 \
  --similarity-threshold 0.65 \
  --num-hash-tables 4 \
  --max-bucket-size 300 \
  --join-strategy approx \
  --top-n-per-doc 10 \
  --shuffle-partitions 4
```

Ran connected components on the LSH output:

```bash
spark-submit --master yarn --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 1 \
  src/connected_components.py \
  --questions hdfs:///user/bigdata/stackoverflow/tmp/v1_smoke/questions \
  --pairs hdfs:///user/bigdata/stackoverflow/tmp/v1_smoke/similar_pairs_approx \
  --output hdfs:///user/bigdata/stackoverflow/tmp/v1_smoke/clusters_approx_v2 \
  --metrics-output hdfs:///user/bigdata/stackoverflow/tmp/v1_smoke/cluster_metrics_approx_v2_csv \
  --iterations 4 \
  --shuffle-partitions 4
```

## YARN Applications

| Application | Purpose | Result |
|---|---|---|
| `application_1779864261706_0034` | Prepare HDFS smoke subset | Success |
| `application_1779864261706_0035` | `cluster_lsh.py --join-strategy approx` | Success |
| `application_1779864261706_0036` | Connected components before checkpoint cleanup | Success, with local checkpoint warning |
| `application_1779864261706_0037` | Connected components after checkpoint cleanup | Success |

## Results

LSH metrics:

```csv
similarity_threshold,distance_threshold,num_hash_tables,max_bucket_size,join_strategy,top_n_per_doc,min_token_count,input_count,candidate_pair_count,pre_topn_pair_count,pair_count,avg_similarity
0.65,0.35,4,300,approx,10,2,14,7,7,7,0.8094350327042577
```

Cluster metrics:

```csv
edge_count,multi_doc_cluster_count,multi_doc_question_count,max_cluster_size,avg_doc_similarity
7,7,14,2,0.8094350327042577
```

Representative similar pairs:

```csv
src,dst,similarity
67307641,67322152,0.8666666666666667
28753859,28768945,0.85
25541869,26797803,0.8260869565217391
9660235,9662930,0.8164893617021277
57530055,57530417,0.782608695652174
1321253,2369316,0.7741935483870968
70089553,70094984,0.75
```

## Follow-up Change From Smoke Test

The first connected-components run succeeded but Spark warned that `/tmp/stackoverflow-spark-checkpoints` looked like a local filesystem path under YARN. `src/connected_components.py` was updated to make `--checkpoint-dir` optional and no longer hard-code a local checkpoint directory. The second connected-components run completed successfully without that warning.
