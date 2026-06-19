# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This is a Spark on YARN distributed text-clustering pipeline for StackOverflow Oracle database Q&A deduplication. The project code lives under [project-stackoverflow-clustering/](project-stackoverflow-clustering/). The target cluster is 3 nodes: `master` (10.176.62.239), `worker1` (10.176.62.240), `worker2` (10.176.62.241).

## Commands

No traditional build/lint/test framework. Everything runs via `spark-submit` to YARN or locally.

**Local validation (no Spark cluster required):**

```bash
cd project-stackoverflow-clustering

# Validate raw data and produce stats (no Spark)
python3 src/local_profile.py ../StackOverFlow_Oracle_Database/oracle_database_questions.json --output-dir output/local_profile_sample

# JSONL conversion (streaming, no Spark)
python3 src/json_to_jsonl.py ../StackOverFlow_Oracle_Database/oracle_database_questions.json output/questions_sample.jsonl --limit 100
```

**Cluster execution (from master):**

Scripts are numbered by pipeline order in [scripts/](project-stackoverflow-clustering/scripts/). They source [conf/app.conf](project-stackoverflow-clustering/conf/app.conf) for HDFS paths and Spark parameters, and use [scripts/common.sh](project-stackoverflow-clustering/scripts/common.sh) for `spark_submit_project()`.

```bash
bash scripts/03_submit_eda.sh          # EDA statistics
bash scripts/04_submit_preprocess.sh   # preprocess.py — JSONL → cleaned Parquet
bash scripts/05_submit_cluster.sh      # cluster_lsh.py + connected_components.py
bash scripts/06_export_demo.sh         # Pull results from HDFS to local
bash scripts/07_query_demo.sh          # Interactive CLI query
```

**Evaluation sweep** (multi-parameter): `scripts/05_submit_cluster_sweep.sh`. Override via env vars: `SWEEP_SIMILARITIES="0.75 0.85" SWEEP_HASH_TABLES="2" SWEEP_BUCKET_SIZES="200"`.

## Architecture & pipeline flow

```
Raw JSON → JSONL → HDFS → preprocess.py (clean + doc text construction)
  → feature_engineering.py (Tokenizer → StopWords → HashingTF ×2 → IDF → Normalizer)
  → forks into two paths:
    1. topic_clustering.py: TF-IDF → BisectingKMeans → topic assignments
    2. cluster_lsh.py: binary HashingTF → MinHashLSH → similar_pairs
       → connected_components.py: iterative label propagation → duplicate clusters
```

### Source files ([src/](project-stackoverflow-clustering/src/))

| File | Role | Runs on |
|---|---|---|
| `preprocess.py` | JSONL → Parquet: HTML cleaning, doc text construction (title×3 + body×2 + tags + top_3_answers), extracts `ora_codes` array | YARN |
| `feature_engineering.py` | Builds both `term_features` (binary, for LSH) and `features` (TF-IDF+L2, for topic clustering) from clean_text | YARN |
| `topic_clustering.py` | TF-IDF → BisectingKMeans with multiple K candidates | YARN |
| `cluster_lsh.py` | MinHashLSH with two join strategies (`approx` via `approxSimilarityJoin` or `bucket` via self-join), ORA domain boost, optional rescue floor | YARN |
| `connected_components.py` | Iterative label propagation from similar-pair edges; outputs `(cluster_id, cluster_size, doc_id, representative_title, avg_similarity)` | YARN |
| `cluster_lsh_intra_topic.py` | Runs LSH within each topic partition independently, then merges results | YARN |
| `domain_rescue_graph.py` | Cross-topic ORA-code rescue edges added to intra-topic LSH graph (D3 innovation) | YARN |
| `cluster_lsh_partitioned.py` | Tag-partitioned LSH (D2 innovation): partitions by primary tag before LSH | YARN |
| `query_demo.py` | CLI: lookup by question_id, keyword search, top clusters display | YARN client |
| `local_profile.py` | Local-only: reads raw JSON directly for quick stats/profile (no Spark) | anywhere |
| `local_similarity_sweep.py` | Local-only: title+tag similarity sweep as fallback when Spark unavailable | anywhere |

### Innovations (current branch `feature/split-large-k-topics`)

- **D1 — ORA domain boost**: `cluster_lsh.py` adds `ora_boost` to Jaccard similarity when two docs share an ORA-XXXXX error code. "ORA-rescue" floor keeps pairs below the main threshold that share an ORA code.
- **D2 — Tag-partitioned LSH**: `cluster_lsh_partitioned.py` partitions docs by primary tag before running LSH, avoiding cross-domain false positives and reducing bucket skew.
- **D3 — Domain-aware hybrid graph**: `domain_rescue_graph.py` adds cross-topic edges for docs sharing ORA codes that intra-topic LSH missed, without re-running MinHashLSH.

### Configuration system

[conf/app.conf](project-stackoverflow-clustering/conf/app.conf) is sourced by bash scripts and defines:
- HDFS paths (`HDFS_BASE`, `HDFS_RAW`, `HDFS_QUESTIONS`, etc.)
- Spark resources (`DRIVER_MEMORY`, `EXECUTOR_MEMORY`, `NUM_EXECUTORS`, `SHUFFLE_PARTITIONS`)
- Algorithm params (`SIMILARITY_THRESHOLD`, `MINHASH_TABLES`, `LSH_JOIN_STRATEGY`, `TOPIC_K`, `ORA_BOOST`, `ORA_RESCUE_FLOOR`)

All values have `${VAR:-default}` overrides. Scripts source `common.sh`, which sources `app.conf` and provides `spark_submit_project`.

## Data

- Source: 152,758 StackOverflow questions tagged `oracle-database` (~589 MB JSON array)
- Location: `../StackOverFlow_Oracle_Database/oracle_database_questions.json` (relative to [project-stackoverflow-clustering/](project-stackoverflow-clustering/), gitignored)
- Doc text construction: `title×3 + question_body×2 + tags + top_3_answers` — emphasizes question content, supplements with high-score answers
- Comments are excluded from clustering features (noisy), only used in EDA

## Key design decisions

- Default LSH uses `approxSimilarityJoin` (`LSH_JOIN_STRATEGY=approx`); bucket self-join available when `max_bucket_size` skew control is needed
- Connected components uses iterative Spark DataFrame propagation (not GraphFrames) to avoid external JAR dependencies
- Default similarity threshold is 0.75; demo uses 0.65 for higher recall
- `shuffle_partitions=96` tuned for 3-node cluster with ~150k documents
- `SPARK_ADAPTIVE_ENABLED=true` for AQE skew handling
- `StorageLevel.MEMORY_AND_DISK` used on large intermediate DataFrames (pair generation)
