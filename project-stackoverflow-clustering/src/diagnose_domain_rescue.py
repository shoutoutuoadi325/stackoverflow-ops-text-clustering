#!/usr/bin/env python3
"""Funnel diagnostics for cross-topic ORA rescue (why rescued=0?)."""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_intersect,
    array_union,
    col,
    concat_ws,
    count,
    explode,
    greatest,
    least,
    lit,
    size,
    when,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--label", default="k50")
    p.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    p.add_argument(
        "--topics",
        default="hdfs:///user/bigdata/stackoverflow/output/topic_clusters_k50",
    )
    p.add_argument(
        "--baseline-pairs",
        default="hdfs:///user/bigdata/stackoverflow/output/intra_topic/k50/similar_pairs",
    )
    p.add_argument("--title-sim-floor", type=float, default=0.45)
    p.add_argument("--tag-rescue-floor", type=float, default=0.40)
    p.add_argument("--min-ora-doc-count", type=int, default=2)
    p.add_argument("--max-ora-doc-count", type=int, default=150)
    return p.parse_args()


def pair_key(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "pair_key",
        concat_ws("|", least(col("src"), col("dst")), greatest(col("src"), col("dst"))),
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName(f"diag-domain-rescue-{args.label}").getOrCreate()

    features = spark.read.parquet(args.features)
    has_ora = "ora_codes" in features.columns
    print(f"=== features columns has_ora_codes={has_ora} ===")
    if not has_ora:
        print("STOP: no ora_codes column in features parquet")
        spark.stop()
        return

    topics = spark.read.parquet(args.topics).select(col("doc_id"), col("cluster_id").alias("topic_id"))
    docs = (
        features.select("doc_id", "title", "score", "tags", "tokens", "ora_codes")
        .join(topics, "doc_id", "inner")
        .persist()
    )

    total_docs = docs.count()
    docs_with_ora = docs.filter(size(col("ora_codes")) > 0).count()
    print(f"docs_total={total_docs} docs_with_ora={docs_with_ora}")

    ora_docs = (
        docs.filter(size(col("ora_codes")) > 0)
        .select(
            "doc_id",
            "title",
            "tags",
            "tokens",
            "topic_id",
            explode("ora_codes").alias("ora_code"),
        )
        .persist()
    )
    ora_rows = ora_docs.count()
    distinct_ora = ora_docs.select("ora_code").distinct().count()
    print(f"ora_exploded_rows={ora_rows} distinct_ora_codes={distinct_ora}")

    ora_stats_all = ora_docs.groupBy("ora_code").agg(count("*").alias("doc_count"))
    ora_stats = ora_stats_all.filter(
        (col("doc_count") >= args.min_ora_doc_count) & (col("doc_count") <= args.max_ora_doc_count)
    )
    valid_codes = ora_stats.count()
    too_few = ora_stats_all.filter(col("doc_count") < args.min_ora_doc_count).count()
    too_many = ora_stats_all.filter(col("doc_count") > args.max_ora_doc_count).count()
    print(
        f"ora_codes_in_range[{args.min_ora_doc_count},{args.max_ora_doc_count}]={valid_codes} "
        f"too_few={too_few} too_many={too_many}"
    )

    left = ora_docs.alias("left")
    right = ora_docs.alias("right")
    joined = (
        left.join(ora_stats, "ora_code")
        .join(right, "ora_code")
        .filter(col("left.doc_id") < col("right.doc_id"))
        .select(
            col("left.doc_id").alias("src"),
            col("right.doc_id").alias("dst"),
            col("left.tokens").alias("src_tokens"),
            col("right.tokens").alias("dst_tokens"),
            col("left.tags").alias("src_tags"),
            col("right.tags").alias("dst_tags"),
            col("left.topic_id").alias("src_topic"),
            col("right.topic_id").alias("dst_topic"),
            col("ora_code").alias("shared_ora_code"),
        )
        .persist()
    )

    all_ora_pairs = joined.count()
    same_topic = joined.filter(col("src_topic") == col("dst_topic")).count()
    cross_topic = joined.filter(col("src_topic") != col("dst_topic")).count()
    print(f"ora_doc_pairs_all={all_ora_pairs} same_topic={same_topic} cross_topic={cross_topic}")

    ct = joined.filter(col("src_topic") != col("dst_topic")).persist()
    intersection_size = size(array_intersect(col("src_tokens"), col("dst_tokens")))
    union_size = size(array_union(col("src_tokens"), col("dst_tokens")))
    tag_overlap = size(array_intersect(col("src_tags"), col("dst_tags"))) > 0

    with_sim = ct.withColumn("similarity_raw", intersection_size / union_size).withColumn(
        "tag_overlap", tag_overlap
    )
    pass_title = with_sim.filter(col("similarity_raw") >= lit(args.title_sim_floor)).count()
    pass_tag = with_sim.filter(
        col("tag_overlap") & (col("similarity_raw") >= lit(args.tag_rescue_floor))
    ).count()
    pass_either = with_sim.filter(
        (col("similarity_raw") >= lit(args.title_sim_floor))
        | (col("tag_overlap") & (col("similarity_raw") >= lit(args.tag_rescue_floor)))
    ).count()
    print(
        f"cross_topic_pass title>={args.title_sim_floor}: {pass_title} "
        f"tag+sim>={args.tag_rescue_floor}: {pass_tag} either: {pass_either}"
    )

    # similarity distribution buckets for cross-topic ORA pairs
    buckets = (
        with_sim.withColumn(
            "bucket",
            when(col("similarity_raw") >= 0.75, "0.75+")
            .when(col("similarity_raw") >= 0.60, "0.60-0.75")
            .when(col("similarity_raw") >= 0.45, "0.45-0.60")
            .when(col("similarity_raw") >= 0.30, "0.30-0.45")
            .when(col("similarity_raw") >= 0.15, "0.15-0.30")
            .otherwise("<0.15"),
        )
        .groupBy("bucket")
        .count()
        .orderBy("bucket")
        .collect()
    )
    print("cross_topic_sim_buckets:", {r["bucket"]: r["count"] for r in buckets})

    rescued_candidates = with_sim.filter(
        (col("similarity_raw") >= lit(args.title_sim_floor))
        | (col("tag_overlap") & (col("similarity_raw") >= lit(args.tag_rescue_floor)))
    ).select("src", "dst")

    baseline = spark.read.parquet(args.baseline_pairs)
    baseline_keys = pair_key(baseline.select("src", "dst")).select("pair_key").distinct()
    rescued_pk = pair_key(rescued_candidates)
    overlap = rescued_pk.join(baseline_keys, "pair_key", "inner").count()
    after_anti = rescued_pk.join(baseline_keys, "pair_key", "left_anti").count()
    print(f"rescued_candidates={rescued_candidates.count()} overlap_with_baseline={overlap} after_left_anti={after_anti}")

    # sample near-miss cross-topic pairs (sim 0.30-0.45 with shared ORA)
    near_miss = (
        with_sim.filter(
            (col("similarity_raw") >= 0.30)
            & (col("similarity_raw") < args.title_sim_floor)
            & (~col("tag_overlap"))
        )
        .orderBy(col("similarity_raw").desc())
        .limit(5)
        .collect()
    )
    if near_miss:
        print("\n=== sample near-miss cross-topic ORA pairs (no tag overlap, sim 0.30-0.45) ===")
        for r in near_miss:
            print(
                f"  sim={r['similarity_raw']:.3f} ora={r['shared_ora_code']} "
                f"topics={r['src_topic']}/{r['dst_topic']} src={r['src']} dst={r['dst']}"
            )

    low_sim_tag = (
        with_sim.filter(col("tag_overlap") & (col("similarity_raw") < args.tag_rescue_floor))
        .orderBy(col("similarity_raw").desc())
        .limit(5)
        .collect()
    )
    if low_sim_tag:
        print("\n=== sample tag-overlap but sim below tag_rescue_floor ===")
        for r in low_sim_tag:
            print(
                f"  sim={r['similarity_raw']:.3f} ora={r['shared_ora_code']} "
                f"topics={r['src_topic']}/{r['dst_topic']}"
            )

    spark.stop()


if __name__ == "__main__":
    main()
