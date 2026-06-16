#!/usr/bin/env python3
"""Build the topic-x-dedup cross view: per-topic deduplication report.

Topic clustering (BisectingKMeans) answers "what does the data look like?";
duplicate detection (MinHashLSH + connected components) answers "which
questions repeat?". Reporting them separately leaves the operationally useful
question unanswered: *which topics carry the most duplication, and how much
can we compress?*

This script joins the two outputs and emits a per-topic summary so the
defense can show a single table:

  topic_id | topic_size | duplicate_cluster_count | duplicate_question_count
           | dedup_ratio | top_terms | sample_duplicate_pair

Inputs
------
- topic_clusters parquet from topic_clustering.py (must contain
  cluster_id (treated as topic_id), doc_id, title, score, cluster_top_terms)
- clusters parquet from connected_components.py (must contain
  cluster_id, cluster_size, doc_id, representative_id, representative_title)

Outputs
-------
- <output> parquet: full per-topic rollup
- <output>_csv: same rollup as a coalesced single CSV file for the demo
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    collect_list,
    count,
    countDistinct,
    first,
    lit,
    row_number,
    struct,
    sum as spark_sum,
    when,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build topic-x-dedup cross view.")
    parser.add_argument("--topics", default="hdfs:///user/bigdata/stackoverflow/output/topic_clusters")
    parser.add_argument("--clusters", default="hdfs:///user/bigdata/stackoverflow/output/clusters")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/output/topic_dedup_view")
    parser.add_argument("--samples-per-topic", type=int, default=3)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-topic-dedup-view").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    # topic_clusters has cluster_id as the topic id; rename to avoid colliding
    # with the duplicate-cluster cluster_id from connected_components output.
    topics = spark.read.parquet(args.topics).select(
        col("cluster_id").alias("topic_id"),
        col("doc_id"),
        col("title").alias("doc_title"),
        col("score").alias("doc_score"),
        col("cluster_top_terms"),
    )

    dups_raw = spark.read.parquet(args.clusters).select(
        col("cluster_id").alias("dup_cluster_id"),
        col("cluster_size").alias("dup_cluster_size"),
        col("doc_id"),
        col("representative_id"),
        col("representative_title"),
    )

    joined = topics.join(dups_raw, "doc_id", "left")
    joined = joined.withColumn(
        "is_duplicate", when(coalesce(col("dup_cluster_size"), lit(1)) > 1, lit(1)).otherwise(lit(0))
    )

    topic_top_terms = topics.groupBy("topic_id").agg(first("cluster_top_terms").alias("top_terms"))

    rollup = (
        joined.groupBy("topic_id")
        .agg(
            countDistinct("doc_id").alias("topic_size"),
            countDistinct(when(col("is_duplicate") == 1, col("dup_cluster_id"))).alias("duplicate_cluster_count"),
            spark_sum("is_duplicate").alias("duplicate_question_count"),
        )
        .withColumn(
            "dedup_ratio",
            when(col("topic_size") > 0, col("duplicate_question_count") / col("topic_size")).otherwise(lit(0.0)),
        )
        .join(topic_top_terms, "topic_id", "left")
    )

    # Pick a few representative duplicate samples per topic so the demo can show
    # a concrete "this topic has these duplicates" example.
    sample_window = Window.partitionBy("topic_id").orderBy(col("dup_cluster_size").desc(), col("doc_id").asc())
    samples = (
        joined.filter(col("is_duplicate") == 1)
        .withColumn("rn", row_number().over(sample_window))
        .filter(col("rn") <= args.samples_per_topic)
        .select(
            "topic_id",
            struct(
                col("doc_id"),
                col("doc_title"),
                col("dup_cluster_id"),
                col("dup_cluster_size"),
                col("representative_id"),
                col("representative_title"),
            ).alias("sample"),
        )
        .groupBy("topic_id")
        .agg(collect_list("sample").alias("duplicate_samples"))
    )

    full = rollup.join(samples, "topic_id", "left").orderBy(col("dedup_ratio").desc(), col("topic_size").desc())
    full.write.mode("overwrite").parquet(args.output)

    # CSV for the static demo. Drop the nested array of structs because CSV
    # cannot represent it; emit a flat samples_text column instead so the
    # report can render a one-line preview.
    flat = full.withColumn(
        "samples_text",
        when(
            col("duplicate_samples").isNotNull(),
            col("duplicate_samples"),
        ).otherwise(lit(None)),
    )
    csv_view = flat.select(
        "topic_id",
        "topic_size",
        "duplicate_cluster_count",
        "duplicate_question_count",
        "dedup_ratio",
        "top_terms",
    )
    csv_view.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "_csv")

    spark.stop()


if __name__ == "__main__":
    main()
