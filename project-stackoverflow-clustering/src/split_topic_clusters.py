#!/usr/bin/env python3
"""Create stable large-K topic buckets by splitting existing topic clusters.

This avoids retraining high-dimensional BisectingKMeans for K=150/200. The
existing low-K topics are kept as the coarse semantic layer; large topics are
split deterministically by document metadata so downstream intra-topic LSH sees
smaller buckets without changing the original feature vectors used for LSH.
"""

from __future__ import annotations

import argparse
import math

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    array_join,
    col,
    collect_list,
    concat_ws,
    count,
    explode,
    lit,
    pmod,
    row_number,
    xxhash64,
)
from pyspark.sql.types import IntegerType, StructField, StructType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Split existing topic clusters to a larger target K.")
    parser.add_argument("--base-topics", required=True, help="Existing topic_clusters parquet, e.g. topic_clusters_k50")
    parser.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument("--output", required=True, help="Output topic_clusters parquet path")
    parser.add_argument("--target-k", type=int, required=True)
    parser.add_argument("--shuffle-partitions", type=int, default=96)
    return parser.parse_args()


def build_split_plan(size_rows: list[tuple[int, int]], target_k: int) -> list[tuple[int, int, int]]:
    """Return (base_cluster_id, split_count, offset) rows."""
    if not size_rows:
        return []
    if target_k < len(size_rows):
        raise ValueError(f"target_k={target_k} is smaller than base topic count={len(size_rows)}")

    total_docs = sum(size for _, size in size_rows)
    plan: list[dict] = []
    assigned = 0
    for cluster_id, size in size_rows:
        raw = size * target_k / total_docs
        base = max(1, int(math.floor(raw)))
        assigned += base
        plan.append({"cluster_id": cluster_id, "size": size, "split_count": base, "frac": raw - base})

    # Adjust exactly to target_k. Prefer splitting larger/high-fraction clusters.
    while assigned < target_k:
        candidate = max(plan, key=lambda r: (r["frac"], r["size"]))
        candidate["split_count"] += 1
        candidate["frac"] = 0.0
        assigned += 1
    while assigned > target_k:
        candidate = max((r for r in plan if r["split_count"] > 1), key=lambda r: (r["split_count"], r["size"]))
        candidate["split_count"] -= 1
        assigned -= 1

    rows: list[tuple[int, int, int]] = []
    offset = 0
    for row in sorted(plan, key=lambda r: r["cluster_id"]):
        rows.append((int(row["cluster_id"]), int(row["split_count"]), int(offset)))
        offset += int(row["split_count"])
    return rows


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName(f"stackoverflow-split-topics-k{args.target_k}").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    base_topics = spark.read.parquet(args.base_topics).select("doc_id", col("cluster_id").alias("base_cluster_id"))
    raw_features = spark.read.parquet(args.features)
    keep_cols = ["doc_id", "title", "score", "tags", "tokens"]
    if "ora_codes" in raw_features.columns:
        keep_cols.append("ora_codes")
    features = raw_features.select(*keep_cols)

    joined = base_topics.join(features, "doc_id", "inner")
    sizes = [(int(r["base_cluster_id"]), int(r["count"])) for r in joined.groupBy("base_cluster_id").count().collect()]
    plan_rows = build_split_plan(sizes, args.target_k)
    plan_schema = StructType(
        [
            StructField("base_cluster_id", IntegerType(), False),
            StructField("split_count", IntegerType(), False),
            StructField("cluster_offset", IntegerType(), False),
        ]
    )
    plan = spark.createDataFrame(plan_rows, plan_schema)

    split_source = joined.join(plan, "base_cluster_id", "inner")
    hash_cols = [col("doc_id"), col("title"), concat_ws("|", col("tags"))]
    if "ora_codes" in split_source.columns:
        hash_cols.append(array_join(col("ora_codes"), "|"))
    split = split_source.withColumn("split_bucket", pmod(xxhash64(*hash_cols), col("split_count")).cast("int"))
    predicted = split.withColumn("cluster_id", col("cluster_offset") + col("split_bucket")).persist(StorageLevel.MEMORY_AND_DISK_SER)

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

    actual_k = output.select("cluster_id").distinct().count()
    max_size = output.groupBy("cluster_id").count().agg({"count": "max"}).collect()[0][0]
    spark.createDataFrame(
        [(args.target_k, int(actual_k), int(max_size), "split_existing_topics")],
        ["target_k", "actual_k", "max_cluster_size", "method"],
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "_metrics_csv")

    print(f"Split topics written to {args.output}: target_k={args.target_k}, actual_k={actual_k}, max_size={max_size}")
    spark.stop()


if __name__ == "__main__":
    main()
