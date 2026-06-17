#!/usr/bin/env python3
"""Recover post-LSH outputs (clusters, samples CSVs, summary) from an
already-written similar_pairs parquet.

Why this exists
---------------
cluster_lsh_intra_topic.py used to crash at the CSV-write step when the
shared_ora_codes ARRAY<STRING> column was present, *after* successfully
writing the similar_pairs parquet. This recovery script picks up that
parquet and finishes the missing steps (samples csv, topic metrics csv if
available, connected components, clusters parquet/csv, summary csv) without
rerunning the expensive MinHashLSH stage.

Usage
-----
  spark-submit recover_intra_topic_outputs.py \
      --similar-pairs hdfs:///user/.../intra_topic/k50/similar_pairs \
      --topics hdfs:///user/.../topic_clusters_k50 \
      --output-base hdfs:///user/.../intra_topic/k50 \
      --label k50

Reads similar-pairs parquet that already contains topic_id, similarity,
shared_ora_codes, ora_match, ora_rescued. Writes:
  output-base/similar_pairs_samples_csv
  output-base/clusters
  output-base/clusters_samples_csv
  output-base/summary_csv
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    least,
    length,
    lit,
    min as spark_min,
    row_number,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recover post-LSH outputs from similar_pairs parquet.")
    parser.add_argument("--similar-pairs", required=True, help="HDFS path to existing similar_pairs parquet")
    parser.add_argument("--topics", required=True, help="HDFS path to topic_clusters parquet (for question metadata)")
    parser.add_argument("--output-base", required=True, help="Base HDFS path for new outputs (e.g. .../intra_topic/k50)")
    parser.add_argument("--label", default="k50", help="Run label written into summary_csv")
    parser.add_argument("--num-hash-tables", type=int, default=4)
    parser.add_argument("--similarity-threshold", type=float, default=0.65)
    parser.add_argument("--distance-threshold", type=float, default=0.35)
    parser.add_argument("--cc-iterations", type=int, default=15)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName(f"result-summary-{args.label}").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    sc = spark.sparkContext
    if not sc.getCheckpointDir():
        sc.setCheckpointDir(f"hdfs:///user/bigdata/stackoverflow/checkpoints/recover_{args.label}")

    base = args.output_base.rstrip("/")
    samples_out = base + "/similar_pairs_samples_csv"
    clusters_out = base + "/clusters"
    clusters_samples_out = base + "/clusters_samples_csv"
    summary_out = base + "/summary_csv"

    edges = spark.read.parquet(args.similar_pairs)
    pair_count = edges.count()
    print(f"Loaded {pair_count} similar pairs from {args.similar_pairs}")

    # Samples CSV. Flatten shared_ora_codes if present.
    samples_view = edges.orderBy(col("similarity").desc()).limit(500)
    if "shared_ora_codes" in samples_view.columns:
        samples_view = samples_view.withColumn(
            "shared_ora_codes_text", concat_ws("|", col("shared_ora_codes"))
        ).drop("shared_ora_codes")
    samples_view.coalesce(1).write.mode("overwrite").option("header", True).csv(samples_out)
    print(f"Wrote {samples_out}")

    # Connected components. Use questions parquet for metadata (title, score, tags).
    questions = spark.read.parquet(args.topics).select(
        col("doc_id"), col("title"), col("score"), col("tags")
    )
    edge_input = edges.select("src", "dst", "similarity")

    labels = questions.select(col("doc_id"), col("doc_id").alias("cluster_id")).localCheckpoint(eager=True)
    edge_input = edge_input.cache()
    edge_input.count()
    for _ in range(args.cc_iterations):
        edge_labels = (
            edge_input.join(
                labels.select(col("doc_id").alias("src"), col("cluster_id").alias("src_cluster")), "src"
            ).join(
                labels.select(col("doc_id").alias("dst"), col("cluster_id").alias("dst_cluster")), "dst"
            ).select("src", "dst", least("src_cluster", "dst_cluster").alias("candidate_cluster"))
        )
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
    edge_scores = edge_input.select(col("src").alias("doc_id"), "similarity").unionByName(
        edge_input.select(col("dst").alias("doc_id"), "similarity")
    )
    avg_similarity = edge_scores.groupBy("doc_id").agg(avg("similarity").alias("avg_similarity"))

    clusters = (
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

    clusters.write.mode("overwrite").parquet(clusters_out)
    sample_output = clusters.withColumn("tags_text", concat_ws("|", col("tags"))).drop("tags")
    sample_output.filter(col("cluster_size") > 1).orderBy(
        col("cluster_size").desc(), col("cluster_id").asc()
    ).limit(2000).coalesce(1).write.mode("overwrite").option("header", True).csv(clusters_samples_out)
    print(f"Wrote {clusters_out} and {clusters_samples_out}")

    multi = clusters.filter(col("cluster_size") > 1)
    multi_cluster_count = multi.select("cluster_id").distinct().count()
    multi_question_count = multi.count()
    max_size = multi.agg({"cluster_size": "max"}).collect()[0][0] or 0
    avg_sim_value = (
        edges.agg(avg("similarity").alias("v")).collect()[0]["v"] or 0.0
    )

    spark.createDataFrame(
        [
            (
                args.label,
                int(args.num_hash_tables),
                float(args.similarity_threshold),
                float(args.distance_threshold),
                int(pair_count),
                int(multi_cluster_count),
                int(multi_question_count),
                int(max_size),
                float(avg_sim_value),
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
    print(f"Wrote {summary_out}")
    print(
        f"\n=== {args.label} summary ===\n"
        f"pair_count = {pair_count}\n"
        f"multi_doc_cluster_count = {multi_cluster_count}\n"
        f"multi_doc_question_count = {multi_question_count}\n"
        f"max_cluster_size = {max_size}\n"
        f"avg_similarity = {avg_sim_value:.4f}\n"
    )

    spark.stop()


if __name__ == "__main__":
    main()
