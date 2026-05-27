#!/usr/bin/env python3
"""Find similar StackOverflow questions with MinHash LSH."""

from __future__ import annotations

import argparse

from pyspark.ml.feature import MinHashLSH
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
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
    parser.add_argument("--distance-threshold", type=float, default=0.35)
    parser.add_argument("--similarity-threshold", type=float, default=0.65)
    parser.add_argument("--num-hash-tables", type=int, default=8)
    parser.add_argument("--top-n-per-doc", type=int, default=10)
    parser.add_argument("--max-bucket-size", type=int, default=500)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-lsh-clustering").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    features = spark.read.parquet(args.features).select(
        "doc_id", "title", "tags", "score", "tokens", "term_features"
    ).cache()

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
    buckets = buckets.join(bucket_sizes.select("hash_key"), "hash_key")

    left = buckets.alias("left")
    right = buckets.alias("right")
    intersection_size = size(array_intersect(col("left.tokens"), col("right.tokens")))
    union_size = size(array_union(col("left.tokens"), col("right.tokens")))

    pairs = (
        left.join(right, "hash_key")
        .filter(col("left.doc_id") < col("right.doc_id"))
        .select(
            col("left.doc_id").alias("src"),
            col("right.doc_id").alias("dst"),
            col("left.title").alias("src_title"),
            col("right.title").alias("dst_title"),
            col("left.score").alias("src_score"),
            col("right.score").alias("dst_score"),
            (lit(1.0) - (intersection_size / union_size)).alias("jaccard_distance"),
            (intersection_size / union_size).alias("similarity"),
        )
        .filter((col("jaccard_distance") <= args.distance_threshold) & (col("similarity") >= args.similarity_threshold))
        .dropDuplicates(["src", "dst"])
    )

    window = Window.partitionBy("src").orderBy(col("similarity").desc(), col("dst").asc())
    pairs = pairs.withColumn("rank", row_number().over(window)).filter(col("rank") <= args.top_n_per_doc).drop("rank")

    pairs.write.mode("overwrite").parquet(args.output)
    pairs.orderBy(col("similarity").desc()).limit(500).coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(args.output.rstrip("/") + "_samples_csv")
    spark.stop()


if __name__ == "__main__":
    main()
