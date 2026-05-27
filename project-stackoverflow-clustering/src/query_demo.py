#!/usr/bin/env python3
"""Command-line demo for duplicate-question clusters."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query duplicate clusters.")
    parser.add_argument("--clusters", default="hdfs:///user/bigdata/stackoverflow/output/clusters")
    parser.add_argument("--pairs", default="hdfs:///user/bigdata/stackoverflow/output/similar_pairs")
    parser.add_argument("--question-id")
    parser.add_argument("--keyword")
    parser.add_argument("--top-clusters", action="store_true")
    parser.add_argument("--limit", type=int, default=10)
    return parser.parse_args()


def print_rows(title: str, rows) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    for row in rows:
        print(row)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-query-demo").getOrCreate()

    clusters = spark.read.parquet(args.clusters)
    pairs = spark.read.parquet(args.pairs)

    if args.question_id:
        q = clusters.filter(col("doc_id") == args.question_id).limit(1).collect()
        if not q:
            print(f"Question {args.question_id} not found.")
            spark.stop()
            return

        query = q[0]
        print("\nQuery Question")
        print("--------------")
        print(f"[{query.doc_id}] {query.title}")
        print(f"cluster_id={query.cluster_id}, cluster_size={query.cluster_size}")
        print(f"representative=[{query.representative_id}] {query.representative_title}")

        same_cluster = (
            clusters.filter((col("cluster_id") == query.cluster_id) & (col("doc_id") != args.question_id))
            .orderBy(col("avg_similarity").desc_nulls_last(), col("score").desc_nulls_last())
            .limit(args.limit)
            .select("doc_id", "title", "score", "avg_similarity")
            .collect()
        )
        print_rows("Same Cluster", [f"[{r.doc_id}] score={r.score}, avg_similarity={r.avg_similarity}: {r.title}" for r in same_cluster])

        similar = (
            pairs.filter((col("src") == args.question_id) | (col("dst") == args.question_id))
            .orderBy(col("similarity").desc())
            .limit(args.limit)
            .collect()
        )
        formatted = []
        for r in similar:
            other_id = r.dst if r.src == args.question_id else r.src
            other_title = r.dst_title if r.src == args.question_id else r.src_title
            formatted.append(f"[{other_id}] similarity={r.similarity:.3f}: {other_title}")
        print_rows("Similar Pairs", formatted)

    if args.keyword:
        keyword = args.keyword.lower()
        rows = (
            clusters.filter((col("cluster_size") > 1) & lower(col("title")).contains(keyword))
            .select("doc_id", "title", "cluster_id", "cluster_size")
            .orderBy(col("cluster_size").desc(), col("doc_id").asc())
            .limit(args.limit)
            .collect()
        )
        print_rows("Keyword Search", [f"[{r.doc_id}] cluster_size={r.cluster_size}: {r.title}" for r in rows])

    if args.top_clusters:
        rows = (
            clusters.select("cluster_id", "cluster_size", "representative_id", "representative_title")
            .filter(col("cluster_size") > 1)
            .distinct()
            .orderBy(col("cluster_size").desc(), col("cluster_id").asc())
            .limit(args.limit)
            .collect()
        )
        print_rows(
            "Top Duplicate Clusters",
            [f"cluster={r.cluster_id}, size={r.cluster_size}, representative=[{r.representative_id}] {r.representative_title}" for r in rows],
        )

    spark.stop()


if __name__ == "__main__":
    main()
