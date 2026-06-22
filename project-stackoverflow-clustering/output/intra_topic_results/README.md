# Intra-topic LSH + ORA domain-rescue results (K sweep)

Derived result tables exported from HDFS (`/user/bigdata/stackoverflow/output/intra_topic/<label>/`)
for the topic-clustering → intra-topic MinHashLSH → D3 ORA domain-rescue pipeline.
Only small CSV summary/sample tables are tracked here; raw parquet and full logs stay on HDFS/cluster.

All runs use similarity threshold 0.65, 4 MinHash tables, ORA boost 0.10, rescue floors title 0.55 / tag 0.50.

## Configs

| label | K | feature regime |
|---|---|---|
| `k50`  | 50  | full resolution (262144 dims) |
| `k100` | 100 | full resolution |
| `k150` | 150 | 4× vector compression (fold to 65536) |
| `k200` | 200 | 4× vector compression |
| `k150c2` | 150 | 2× vector compression (fold to 131072) |
| `k200c2` | 200 | 2× vector compression |

Vector compression = modulo feature folding applied to the BisectingKMeans training input only
(`topic_clustering.py: fold_vector`), to keep large-K clustering within driver memory. It does not
touch the features used for LSH.

## Headline numbers (baseline intra-topic LSH → hybrid with ORA rescue)

| K | baseline pairs / clusters / questions | max cluster | avg sim | hybrid questions | rescued |
|---|---|---|---|---|---|
| 50  | 43 / 39 / 81 | 4 | 0.726 | 87 | +6 |
| 100 | 41 / 37 / 77 | 4 | 0.728 | 83 | +6 |
| 150 | 38 / 36 / 74 | 3 | 0.759 | 82 | +8 |
| 200 | 37 / 35 / 72 | 3 | 0.753 | 82 | +10 |

Recall (coverage) falls and cluster purity (avg similarity) rises as K grows; ORA rescue contributes
more at higher K and stabilizes hybrid coverage at 82 for K>=150. 2× vs 4× compression makes no
meaningful difference to the final hybrid result (e.g. k150 == k150c2). On a recall↔precision
Pareto basis, K=150 is the recommended optimum (K=200 is dominated; K=50 is the recall-priority
alternative).

## Files per label

- `summary.csv` — baseline intra-topic LSH summary
- `hybrid_summary.csv` — baseline vs hybrid (with ORA rescue) summary
- `baseline_vs_hybrid.csv` — long-form method comparison
- `similar_pairs_samples.csv` — sample similar pairs with similarity scores
- `rescued_pairs_samples.csv` — cross-topic pairs recovered via shared ORA codes
- `clusters_samples.csv` / `hybrid_clusters_samples.csv` — sample clusters with titles and tags
- `topic_metrics.csv` — per-topic doc/pair counts and elapsed time (where available)
