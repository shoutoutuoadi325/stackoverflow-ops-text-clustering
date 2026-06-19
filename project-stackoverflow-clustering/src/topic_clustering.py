#!/usr/bin/env python3
"""Topic clustering with BisectingKMeans."""

from __future__ import annotations

import argparse

from pyspark import StorageLevel
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, collect_list, concat_ws, count, explode, lit, row_number


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run topic clustering.")
    parser.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/output/topic_clusters")
    parser.add_argument("--k", type=int, default=50)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-topic-clustering").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    features = spark.read.parquet(args.features).persist(StorageLevel.MEMORY_AND_DISK_SER)
    model = BisectingKMeans(k=args.k, seed=args.seed, featuresCol="features", predictionCol="cluster_id").fit(features)
    predicted = model.transform(features).persist(StorageLevel.MEMORY_AND_DISK_SER)

    token_counts = predicted.select("cluster_id", explode("tokens").alias("token")).groupBy("cluster_id", "token").agg(
        count("*").alias("token_count")
    )
    token_window = Window.partitionBy("cluster_id").orderBy(col("token_count").desc(), col("token").asc())
    top_terms = token_counts.withColumn("rn", row_number().over(token_window)).filter(col("rn") <= 12)
    top_terms = top_terms.groupBy("cluster_id").agg(concat_ws(", ", collect_list("token")).alias("cluster_top_terms"))

    output = predicted.select("cluster_id", "doc_id", "title", "score", "tags").join(top_terms, "cluster_id", "left")
    output.write.mode("overwrite").parquet(args.output)
    sample_output = output.withColumn("tags_text", concat_ws("|", col("tags"))).drop("tags")
    sample_output.orderBy("cluster_id", col("score").desc()).limit(1000).coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(args.output.rstrip("/") + "_samples_csv")

    silhouette = ClusteringEvaluator(featuresCol="features", predictionCol="cluster_id").evaluate(predicted)
    spark.createDataFrame([(args.k, float(silhouette))], ["k", "silhouette"]).withColumn(
        "metric", lit("silhouette")
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "_metrics_csv")

    spark.stop()


if __name__ == "__main__":
    main()
