#!/usr/bin/env python3
"""Two-stage duplicate detection: topic clustering + intra-cluster MinHashLSH.

Memory-optimised variation: the LSH join inside each topic uses a slim
(doc_id, term_features) view so heavy metadata columns are not duplicated
across every candidate pair within the LSH internal cross-join.
"""

from __future__ import annotations

import argparse
import time
from typing import Iterable, List, Optional

from pyspark import StorageLevel
from pyspark.ml.feature import MinHashLSH
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    array_intersect,
    array_union,
    avg,
    col,
    concat_ws,
    count,
    least,
    length,
    lit,
    min as spark_min,
    row_number,
    size,
    when,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Two-stage topic + intra-cluster MinHashLSH.")
    parser.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument("--topics", default="hdfs:///user/bigdata/stackoverflow/output/topic_clusters")
    parser.add_argument(
        "--output",
        default="hdfs:///user/bigdata/stackoverflow/output/intra_topic",
    )
    parser.add_argument("--distance-threshold", type=float, default=0.35)
    parser.add_argument("--similarity-threshold", type=float, default=0.65)
    parser.add_argument("--num-hash-tables", type=int, default=4)
    parser.add_argument("--top-n-per-doc", type=int, default=10)
    parser.add_argument("--min-token-count", type=int, default=2)
    parser.add_argument(
        "--min-topic-size",
        type=int,
        default=2,
        help="Topics smaller than this are skipped (cannot form pairs).",
    )
    parser.add_argument(
        "--max-topics",
        type=int,
        default=0,
        help=(
            "If > 0, only run intra-LSH on the largest N topics. Useful for smoke "
            "tests before committing to a full run."
        ),
    )
    parser.add_argument(
        "--label",
        default="k50",
        help="Run label baked into output paths so multiple K runs do not collide.",
    )
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    parser.add_argument("--ora-boost", type=float, default=0.10)
    parser.add_argument("--ora-rescue-floor", type=float, default=0.0)
    parser.add_argument("--cc-iterations", type=int, default=15)
    return parser.parse_args()


def _enrich_candidates_intra(candidates: DataFrame, bucket_df: DataFrame, has_ora: bool, shuffle_partitions: int) -> DataFrame:
    """Join slim (src, dst) pairs back with the topic bucket to attach metadata."""
    meta_cols = ["doc_id", "title", "score", "tokens"]
    if has_ora:
        meta_cols.append("ora_codes")

    src_meta = bucket_df.select(
        *[col(c).alias(f"src_{c}" if c != "doc_id" else "src_doc_id") for c in meta_cols]
    )
    dst_meta = bucket_df.select(
        *[col(c).alias(f"dst_{c}" if c != "doc_id" else "dst_doc_id") for c in meta_cols]
    )

    select_exprs = [
        "src",
        "dst",
        col("src_title").alias("src_title"),
        col("dst_title").alias("dst_title"),
        col("src_score").alias("src_score"),
        col("dst_score").alias("dst_score"),
        col("src_tokens").alias("src_tokens"),
        col("dst_tokens").alias("dst_tokens"),
    ]
    if has_ora:
        select_exprs += [
            col("src_ora_codes").alias("src_ora_codes"),
            col("dst_ora_codes").alias("dst_ora_codes"),
        ]

    return (
        candidates.join(src_meta, candidates.src == col("src_doc_id"), "inner")
        .drop("src_doc_id")
        .join(dst_meta, candidates.dst == col("dst_doc_id"), "inner")
        .drop("dst_doc_id")
        .select(*select_exprs)
    )


def run_lsh_on_topic(
    bucket_df: DataFrame,
    args: argparse.Namespace,
    topic_id: int,
    has_ora: bool,
) -> Optional[DataFrame]:
    """Fit MinHashLSH on a single topic subset (two-phase: slim join → enrich)."""
    bucket_df = bucket_df.persist(StorageLevel.MEMORY_AND_DISK)
    doc_count = bucket_df.count()
    if doc_count < 2:
        bucket_df.unpersist()
        return None

    lsh = MinHashLSH(
        inputCol="term_features",
        outputCol="hashes",
        numHashTables=args.num_hash_tables,
    )
    model = lsh.fit(bucket_df)

    # Phase 1: slim LSH join — only (doc_id, term_features) through the cross-join.
    slim = bucket_df.select("doc_id", "term_features")
    joined = model.approxSimilarityJoin(slim, slim, args.distance_threshold, distCol="lsh_jaccard_distance")

    candidates = (
        joined.filter(col("datasetA.doc_id") < col("datasetB.doc_id"))
        .select(
            col("datasetA.doc_id").alias("src"),
            col("datasetB.doc_id").alias("dst"),
        )
        .dropDuplicates(["src", "dst"])
    )

    # Phase 2: enrich surviving candidates with metadata from the topic bucket.
    candidate_pairs = _enrich_candidates_intra(candidates, bucket_df, has_ora, args.shuffle_partitions)

    intersection_size = size(array_intersect(col("src_tokens"), col("dst_tokens")))
    union_size = size(array_union(col("src_tokens"), col("dst_tokens")))
    pairs = (
        candidate_pairs.withColumn("similarity_raw", intersection_size / union_size)
        .withColumn("jaccard_distance", lit(1.0) - col("similarity_raw"))
    )
    if has_ora:
        shared = array_intersect(col("src_ora_codes"), col("dst_ora_codes"))
        pairs = (
            pairs.withColumn("shared_ora_codes", shared)
            .withColumn("ora_match", size(col("shared_ora_codes")) > 0)
            .withColumn(
                "similarity",
                least(
                    lit(1.0),
                    col("similarity_raw") + when(col("ora_match"), lit(args.ora_boost)).otherwise(lit(0.0)),
                ),
            )
        )
    else:
        pairs = (
            pairs.withColumn("similarity", col("similarity_raw"))
            .withColumn("shared_ora_codes", lit(None).cast("array<string>"))
            .withColumn("ora_match", lit(False))
        )

    base_filter = (col("jaccard_distance") <= args.distance_threshold) & (
        col("similarity") >= args.similarity_threshold
    )
    if has_ora and args.ora_rescue_floor > 0:
        rescue_filter = (
            col("ora_match")
            & (col("similarity_raw") >= args.ora_rescue_floor)
            & (col("similarity_raw") < args.similarity_threshold)
        )
        keep_filter = base_filter | rescue_filter
    else:
        rescue_filter = lit(False)
        keep_filter = base_filter

    pairs = (
        pairs.withColumn(
            "ora_rescued",
            when((~base_filter) & rescue_filter, lit(True)).otherwise(lit(False)),
        )
        .filter(keep_filter)
        .withColumn("topic_id", lit(topic_id))
    )

    window = Window.partitionBy("src").orderBy(col("similarity").desc(), col("dst").asc())
    pairs = pairs.withColumn("rank", row_number().over(window)).filter(col("rank") <= args.top_n_per_doc).drop("rank")

    return pairs.drop("src_tokens", "dst_tokens")


def union_compatible(dfs: Iterable[DataFrame]) -> Optional[DataFrame]:
    out: Optional[DataFrame] = None
    for df in dfs:
        if df is None:
            continue
        out = df if out is None else out.unionByName(df, allowMissingColumns=True)
    return out


def build_connected_components(
    spark: SparkSession,
    edges: DataFrame,
    questions: DataFrame,
    iterations: int,
) -> DataFrame:
    """Iterative label-propagation connected components (memory-optimised).

    Checkpoints edge_labels each iteration to prevent lineage depth explosion.
    """
    labels = questions.select(col("doc_id"), col("doc_id").alias("cluster_id")).localCheckpoint(eager=True)
    edges = edges.select("src", "dst", "similarity").persist(StorageLevel.MEMORY_AND_DISK)
    edges.count()

    for _ in range(iterations):
        edge_labels = (
            edges.join(labels.select(col("doc_id").alias("src"), col("cluster_id").alias("src_cluster")), "src")
            .join(labels.select(col("doc_id").alias("dst"), col("cluster_id").alias("dst_cluster")), "dst")
            .select("src", "dst", least("src_cluster", "dst_cluster").alias("candidate_cluster"))
        )
        # Checkpoint edge_labels to break lineage — prevents plan depth explosion
        # across iterations which can cause driver memory pressure.
        edge_labels = edge_labels.localCheckpoint(eager=True)

        candidates = edge_labels.select(
            col("src").alias("doc_id"), col("candidate_cluster").alias("cluster_id")
        ).unionByName(
            edge_labels.select(col("dst").alias("doc_id"), col("candidate_cluster").alias("cluster_id"))
        )
        labels = (
            labels.unionByName(candidates)
            .groupBy("doc_id")
            .agg(spark_min("cluster_id").alias("cluster_id"))
            .localCheckpoint(eager=True)
        )

    cluster_sizes = labels.groupBy("cluster_id").agg(count("*").alias("cluster_size"))
    clustered = labels.join(questions, "doc_id").join(cluster_sizes, "cluster_id")

    rep_window = Window.partitionBy("cluster_id").orderBy(
        col("score").desc_nulls_last(), length("title").asc(), col("doc_id").asc()
    )
    representatives = (
        clustered.withColumn("rn", row_number().over(rep_window))
        .filter(col("rn") == 1)
        .select(
            "cluster_id",
            col("doc_id").alias("representative_id"),
            col("title").alias("representative_title"),
        )
    )
    edge_scores = edges.select(col("src").alias("doc_id"), "similarity").unionByName(
        edges.select(col("dst").alias("doc_id"), "similarity")
    )
    avg_similarity = edge_scores.groupBy("doc_id").agg(avg("similarity").alias("avg_similarity"))

    return (
        clustered.join(representatives, "cluster_id", "left")
        .join(avg_similarity, "doc_id", "left")
        .select(
            "cluster_id",
            "cluster_size",
            "doc_id",
            "title",
            "score",
            "tags",
            "representative_id",
            "representative_title",
            "avg_similarity",
        )
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName(f"stackoverflow-intra-topic-{args.label}").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    sc = spark.sparkContext
    if not sc.getCheckpointDir():
        sc.setCheckpointDir(f"hdfs:///user/bigdata/stackoverflow/checkpoints/intra_topic_{args.label}")

    raw = spark.read.parquet(args.features)
    has_ora = "ora_codes" in raw.columns
    keep_cols = ["doc_id", "title", "tags", "score", "tokens", "term_features"]
    if has_ora:
        keep_cols.append("ora_codes")
    features = raw.select(*keep_cols).filter(size(col("tokens")) >= args.min_token_count)

    # Topics: cluster_id from the BisectingKMeans output is our topic_id.
    topics = spark.read.parquet(args.topics).select(
        col("doc_id"), col("cluster_id").alias("topic_id")
    )
    joined = features.join(topics, "doc_id", "inner")

    # Topic size distribution decides which topics get a job and in what order
    # (largest first so YARN releases small executors faster).
    topic_sizes = joined.groupBy("topic_id").count().orderBy(col("count").desc()).collect()
    if args.max_topics > 0:
        active = [r["topic_id"] for r in topic_sizes[: args.max_topics]]
        skipped = topic_sizes[args.max_topics:]
    else:
        active = [r["topic_id"] for r in topic_sizes]
        skipped = []

    metrics_rows: List[tuple] = []
    bucket_outputs: List[DataFrame] = []
    for topic_id in active:
        topic_df = joined.filter(col("topic_id") == topic_id).select(*keep_cols)
        doc_count = topic_df.count()
        if doc_count < args.min_topic_size:
            metrics_rows.append((int(topic_id), int(doc_count), 0, 0.0, "too_small"))
            continue
        start = time.monotonic()
        try:
            pairs = run_lsh_on_topic(topic_df, args, int(topic_id), has_ora)
            elapsed = time.monotonic() - start
            if pairs is None:
                metrics_rows.append((int(topic_id), int(doc_count), 0, elapsed, "empty"))
                continue
            pair_count = pairs.count()
            bucket_outputs.append(pairs)
            metrics_rows.append((int(topic_id), int(doc_count), int(pair_count), elapsed, "ok"))
        except Exception as exc:  # noqa: BLE001
            elapsed = time.monotonic() - start
            metrics_rows.append((int(topic_id), int(doc_count), 0, elapsed, f"error:{type(exc).__name__}"))

    for r in skipped:
        metrics_rows.append((int(r["topic_id"]), int(r["count"]), 0, 0.0, "skipped"))

    edges = union_compatible(bucket_outputs)
    base_out = args.output.rstrip("/") + "/" + args.label
    pairs_out = base_out + "/similar_pairs"
    samples_out = base_out + "/similar_pairs_samples_csv"
    topic_metrics_out = base_out + "/topic_metrics_csv"
    clusters_out = base_out + "/clusters"
    clusters_samples_out = base_out + "/clusters_samples_csv"
    summary_out = base_out + "/summary_csv"

    if edges is None:
        edges = spark.createDataFrame(
            [],
            "src string, dst string, src_title string, dst_title string, src_score int, dst_score int, "
            "similarity_raw double, jaccard_distance double, shared_ora_codes array<string>, "
            "ora_match boolean, similarity double, ora_rescued boolean, topic_id int",
        )
    edges.write.mode("overwrite").parquet(pairs_out)
    if "similarity" in edges.columns:
        # CSV cannot serialise array columns; flatten shared_ora_codes to a
        # pipe-delimited string and drop array originals before writing.
        samples_view = edges.orderBy(col("similarity").desc()).limit(500)
        if "shared_ora_codes" in samples_view.columns:
            samples_view = samples_view.withColumn(
                "shared_ora_codes_text", concat_ws("|", col("shared_ora_codes"))
            ).drop("shared_ora_codes")
        samples_view.coalesce(1).write.mode("overwrite").option("header", True).csv(samples_out)

    spark.createDataFrame(
        metrics_rows,
        ["topic_id", "doc_count", "pair_count", "elapsed_seconds", "status"],
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(topic_metrics_out)

    # Connected components on the union edges -> the headline "X duplicate groups" number.
    # IMPORTANT: read edges back from the parquet we just wrote, not from the in-memory
    # union_compatible() DataFrame. Otherwise Spark re-evaluates the whole per-topic LSH
    # plan inside every connected-components iteration (15x), which scales to thousands
    # of stages with thousands of tasks each. Reading from parquet hard-cuts the lineage.
    questions = spark.read.parquet(args.topics).select(
        col("doc_id"), col("title"), col("score"), col("tags")
    )
    edges_reread = spark.read.parquet(pairs_out) if "similarity" in edges.columns else None
    if edges_reread is not None and edges_reread.head(1):
        edge_input = edges_reread.select("src", "dst", "similarity")
        clusters = build_connected_components(spark, edge_input, questions, args.cc_iterations)
        clusters.write.mode("overwrite").parquet(clusters_out)
        sample_output = clusters.withColumn("tags_text", concat_ws("|", col("tags"))).drop("tags")
        sample_output.filter(col("cluster_size") > 1).orderBy(
            col("cluster_size").desc(), col("cluster_id").asc()
        ).limit(2000).coalesce(1).write.mode("overwrite").option("header", True).csv(clusters_samples_out)
        multi = clusters.filter(col("cluster_size") > 1)
        multi_cluster_count = multi.select("cluster_id").distinct().count()
        multi_question_count = multi.count()
        max_size = multi.agg({"cluster_size": "max"}).collect()[0][0] or 0
    else:
        multi_cluster_count = 0
        multi_question_count = 0
        max_size = 0

    pair_total = edges.count() if "similarity" in edges.columns else 0
    avg_sim = (
        edges.agg(avg("similarity").alias("v")).collect()[0]["v"] or 0.0
        if "similarity" in edges.columns
        else 0.0
    )
    spark.createDataFrame(
        [
            (
                args.label,
                int(args.num_hash_tables),
                float(args.similarity_threshold),
                float(args.distance_threshold),
                int(pair_total),
                int(multi_cluster_count),
                int(multi_question_count),
                int(max_size),
                float(avg_sim),
            )
        ],
        [
            "run_label",
            "num_hash_tables",
            "similarity_threshold",
            "distance_threshold",
            "pair_count",
            "multi_doc_cluster_count",
            "multi_doc_question_count",
            "max_cluster_size",
            "avg_similarity",
        ],
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(summary_out)

    spark.stop()


if __name__ == "__main__":
    main()
