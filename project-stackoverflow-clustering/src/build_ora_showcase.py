#!/usr/bin/env python3
"""Curate ORA-rescued and ORA-matched pairs for the defense showcase.

Reads the LSH similar_pairs parquet produced by the updated `cluster_lsh.py`
(must contain `shared_ora_codes`, `ora_match`, `ora_rescued`, `similarity_raw`,
and `similarity` columns) and emits two CSVs that demonstrate the
domain-knowledge boost:

  - ora_rescued_pairs.csv: pairs whose raw similarity was below the main
    threshold but recovered because they share an ORA code. These are the
    "knowledge-recall" examples for the slides.
  - ora_match_top_pairs.csv: top high-similarity pairs that share an ORA code,
    used as "domain-confirmed" duplicates.

Both CSVs are written via coalesce(1) into directories so Spark on YARN does
not need a single-task driver collect.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, size


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ORA-driven pair showcase CSVs.")
    parser.add_argument("--pairs", default="hdfs:///user/bigdata/stackoverflow/output/similar_pairs")
    parser.add_argument(
        "--output",
        default="hdfs:///user/bigdata/stackoverflow/output/ora_showcase",
        help="Output directory; two CSV subdirectories will be created under it.",
    )
    parser.add_argument("--limit-rescued", type=int, default=200)
    parser.add_argument("--limit-matched", type=int, default=200)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-ora-showcase").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    pairs = spark.read.parquet(args.pairs)
    required_cols = {"shared_ora_codes", "ora_match", "ora_rescued", "similarity_raw", "similarity"}
    missing = required_cols - set(pairs.columns)
    if missing:
        raise SystemExit(
            "similar_pairs parquet is missing ORA columns: "
            f"{sorted(missing)}. Re-run cluster_lsh.py with the updated code first."
        )

    display_cols = [
        col("src"),
        col("dst"),
        col("src_title"),
        col("dst_title"),
        col("src_score"),
        col("dst_score"),
        col("similarity_raw"),
        col("similarity"),
        concat_ws("|", col("shared_ora_codes")).alias("shared_ora_codes"),
        col("ora_match"),
        col("ora_rescued"),
    ]

    rescued = (
        pairs.filter(col("ora_rescued"))
        .select(*display_cols)
        .orderBy(col("similarity").desc(), col("src").asc())
        .limit(args.limit_rescued)
    )
    rescued.coalesce(1).write.mode("overwrite").option("header", True).csv(
        args.output.rstrip("/") + "/ora_rescued_pairs_csv"
    )

    matched = (
        pairs.filter(col("ora_match") & (size(col("shared_ora_codes")) > 0) & (~col("ora_rescued")))
        .select(*display_cols)
        .orderBy(col("similarity").desc(), col("src").asc())
        .limit(args.limit_matched)
    )
    matched.coalesce(1).write.mode("overwrite").option("header", True).csv(
        args.output.rstrip("/") + "/ora_match_top_pairs_csv"
    )

    spark.stop()


if __name__ == "__main__":
    main()
