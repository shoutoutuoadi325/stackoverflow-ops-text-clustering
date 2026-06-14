#!/usr/bin/env python3
"""Find similar StackOverflow questions with MinHash LSH."""

from __future__ import annotations

import argparse

from pyspark.ml.feature import MinHashLSH
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    array_intersect,
    array_union,
    col,
    concat_ws,
    count,
    lit,
    posexplode,
    row_number,
    size,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate similar question pairs with MinHashLSH.")
    parser.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/output/similar_pairs")
    parser.add_argument("--metrics-output", help="Optional CSV output directory for run metrics.")
    parser.add_argument("--distance-threshold", type=float, default=0.35)
    parser.add_argument("--similarity-threshold", type=float, default=0.65)
    parser.add_argument("--num-hash-tables", type=int, default=8)
    parser.add_argument("--top-n-per-doc", type=int, default=10)
    parser.add_argument("--max-bucket-size", type=int, default=500)
    parser.add_argument("--min-token-count", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-lsh-clustering").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    features = spark.read.parquet(args.features).select(
        "doc_id", "title", "tags", "score", "tokens", "term_features"
    ).filter(size(col("tokens")) >= args.min_token_count).persist(StorageLevel.MEMORY_AND_DISK)
    input_count = features.count()

    lsh = MinHashLSH(
        inputCol="term_features",
        outputCol="hashes",
        numHashTables=args.num_hash_tables,
    )
    model = lsh.fit(features)

    hashed = model.transform(features).select("doc_id", "title", "score", "tokens", "hashes")
    buckets = hashed.select(
        "doc_id",
        "title",
        "score",
        "tokens",
        posexplode("hashes").alias("hash_table", "hash_value"),
    ).withColumn("hash_key", concat_ws(":", col("hash_table").cast("string"), col("hash_value").cast("string")))

    bucket_sizes = buckets.groupBy("hash_key").agg(count("*").alias("bucket_size")).filter(
        (col("bucket_size") > 1) & (col("bucket_size") <= args.max_bucket_size)
    )
    kept_bucket_count = bucket_sizes.count()
    buckets = (
        buckets.join(bucket_sizes.select("hash_key"), "hash_key")
        .repartition(args.shuffle_partitions, "hash_key")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    left = buckets.alias("left")
    right = buckets.alias("right")
    candidate_pairs = (
        left.join(right, "hash_key")
        .filter(col("left.doc_id") < col("right.doc_id"))
        .select(
            col("left.doc_id").alias("src"),
            col("right.doc_id").alias("dst"),
            col("left.title").alias("src_title"),
            col("right.title").alias("dst_title"),
            col("left.score").alias("src_score"),
            col("right.score").alias("dst_score"),
            col("left.tokens").alias("src_tokens"),
            col("right.tokens").alias("dst_tokens"),
        )
        .dropDuplicates(["src", "dst"])
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    candidate_pair_count = candidate_pairs.count()

    intersection_size = size(array_intersect(col("src_tokens"), col("dst_tokens")))
    union_size = size(array_union(col("src_tokens"), col("dst_tokens")))

    pairs = (
        candidate_pairs.select(
            "src",
            "dst",
            "src_title",
            "dst_title",
            "src_score",
            "dst_score",
            (lit(1.0) - (intersection_size / union_size)).alias("jaccard_distance"),
            (intersection_size / union_size).alias("similarity"),
        )
        .filter((col("jaccard_distance") <= args.distance_threshold) & (col("similarity") >= args.similarity_threshold))
    ).persist(StorageLevel.MEMORY_AND_DISK)
    pair_count = pairs.count()

    window = Window.partitionBy("src").orderBy(col("similarity").desc(), col("dst").asc())
    pairs = pairs.withColumn("rank", row_number().over(window)).filter(col("rank") <= args.top_n_per_doc).drop("rank")
    output_pair_count = pairs.count()

    pairs.write.mode("overwrite").parquet(args.output)
    pairs.orderBy(col("similarity").desc()).limit(500).coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(args.output.rstrip("/") + "_samples_csv")
    if args.metrics_output:
        similarity_stats = pairs.agg(avg("similarity").alias("avg_similarity")).collect()[0]
        spark.createDataFrame(
            [
                (
                    args.similarity_threshold,
                    args.distance_threshold,
                    args.num_hash_tables,
                    args.max_bucket_size,
                    args.top_n_per_doc,
                    args.min_token_count,
                    input_count,
                    kept_bucket_count,
                    candidate_pair_count,
                    pair_count,
                    output_pair_count,
                    float(similarity_stats.avg_similarity or 0.0),
                )
            ],
            [
                "similarity_threshold",
                "distance_threshold",
                "num_hash_tables",
                "max_bucket_size",
                "top_n_per_doc",
                "min_token_count",
                "input_count",
                "kept_bucket_count",
                "candidate_pair_count",
                "pre_topn_pair_count",
                "pair_count",
                "avg_similarity",
            ],
        ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.metrics_output)
    spark.stop()


if __name__ == "__main__":
    main()
