#!/usr/bin/env python3
"""Build duplicate clusters from similar-pair edges."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import avg, col, concat_ws, count, length, least, min as spark_min, row_number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute connected components from similar pairs.")
    parser.add_argument("--questions", default="hdfs:///user/bigdata/stackoverflow/parquet/questions")
    parser.add_argument("--pairs", default="hdfs:///user/bigdata/stackoverflow/output/similar_pairs")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/output/clusters")
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-connected-components").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    spark.sparkContext.setCheckpointDir("/tmp/stackoverflow-spark-checkpoints")

    questions = spark.read.parquet(args.questions).select("doc_id", "title", "score", "tags")
    edges = spark.read.parquet(args.pairs).select("src", "dst", "similarity").cache()
    edges.count()
    labels = questions.select(col("doc_id"), col("doc_id").alias("cluster_id")).localCheckpoint(eager=True)

    for _ in range(args.iterations):
        edge_labels = (
            edges.join(labels.select(col("doc_id").alias("src"), col("cluster_id").alias("src_cluster")), "src")
            .join(labels.select(col("doc_id").alias("dst"), col("cluster_id").alias("dst_cluster")), "dst")
            .select("src", "dst", least("src_cluster", "dst_cluster").alias("candidate_cluster"))
        )
        candidates = edge_labels.select(col("src").alias("doc_id"), col("candidate_cluster").alias("cluster_id")).unionByName(
            edge_labels.select(col("dst").alias("doc_id"), col("candidate_cluster").alias("cluster_id"))
        )
        labels = labels.unionByName(candidates).groupBy("doc_id").agg(spark_min("cluster_id").alias("cluster_id")).localCheckpoint(
            eager=True
        )

    cluster_sizes = labels.groupBy("cluster_id").agg(count("*").alias("cluster_size"))
    clustered = labels.join(questions, "doc_id").join(cluster_sizes, "cluster_id")

    rep_window = Window.partitionBy("cluster_id").orderBy(col("score").desc_nulls_last(), length("title").asc(), col("doc_id").asc())
    representatives = (
        clustered.withColumn("rn", row_number().over(rep_window))
        .filter(col("rn") == 1)
        .select(
            "cluster_id",
            col("doc_id").alias("representative_id"),
            col("title").alias("representative_title"),
        )
    )

    edge_scores = edges.select(col("src").alias("doc_id"), "similarity").unionByName(edges.select(col("dst").alias("doc_id"), "similarity"))
    avg_similarity = edge_scores.groupBy("doc_id").agg(avg("similarity").alias("avg_similarity"))

    output = (
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

    output.write.mode("overwrite").parquet(args.output)
    sample_output = output.withColumn("tags_text", concat_ws("|", col("tags"))).drop("tags")
    sample_output.filter(col("cluster_size") > 1).orderBy(col("cluster_size").desc(), col("cluster_id").asc()).limit(1000).coalesce(
        1
    ).write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "_samples_csv")

    spark.stop()


if __name__ == "__main__":
    main()
