# Repository guidance

## Final project

The active project is `project-stackoverflow-clustering/`. It deduplicates 152,758 StackOverflow Oracle questions with Spark on YARN.

## Final pipeline

```text
preprocess -> TF-IDF/binary HashingTF
-> BisectingKMeans topic blocking
-> intra-topic MinHashLSH
-> materialized edges + connected components
-> cross-topic ORA rescue
-> hybrid graph
```

## Sources of truth

- Completion: `docs/项目完成结果.md`
- Innovations: `docs/最终创新点与实验结果.md`
- ORA details: `docs/ORA迭代创新点(1).md`
- Results: `output/evaluation/final_intra_topic_sweep.csv`
- Edge overlap: `output/evaluation/hybrid_edge_overlap.csv`
- Raw derived tables: `output/intra_topic_results/`

## Result interpretation

k50 Hybrid is 46 pairs / 42 multi-document clusters / 87 questions. k150 4x is the engineering recommendation.

k150 and k200 have identical 42-edge Hybrid sets within the 4x regime. k150c2 and k200c2 are also identical within 2x. Across 4x vs 2x, only 37/42 edges overlap (set Jaccard 0.7872), so never claim compression results are fully identical.

`rescued_pair_count` counts ORA join records and can contain the same pair more than once. Net new edges equal hybrid_pair_count minus baseline_pair_count.

The domain rescue argument `title_sim_floor` is a legacy name: the implementation computes full token-set Jaccard, not title-only similarity.

## Reproducibility

The current branch includes the final run''s `topic_clustering.py --training-num-features` vector folding. Script 16 reproduces 4x K150/K200. Script 19 reproduces 2x k150c2/k200c2. Script 18 is a separate deterministic split fallback and must not be described as the source of the six final runs.

## Evaluation boundary

The 90 historical labels were LLM-assisted and lack per-row review notes. Use them as supporting evidence only. Do not report recall/F1 or claim global optimality.
