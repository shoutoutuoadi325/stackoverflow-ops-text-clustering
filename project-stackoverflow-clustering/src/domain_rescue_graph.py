#!/usr/bin/env python3
"""Domain-aware hybrid graph: intra-topic LSH + cross-topic ORA rescue.

Runs after cluster_lsh_intra_topic.py. Does not rerun MinHashLSH.

Memory-optimised: pre-filters both sides of the ORA self-join against
ora_stats so that only docs with valid ORA codes participate in the
cross-product. Uses MEMORY_AND_DISK for large persisted DataFrames.
"""

from __future__ import annotations

import argparse

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    array,
    array_intersect,
    array_union,
    avg,
    col,
    concat_ws,
    count,
    explode,
    greatest,
    least,
    length,
    lit,
    row_number,
    size,
    when,
)

from cluster_lsh_intra_topic import build_connected_components


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge intra-topic LSH pairs with cross-topic ORA rescue edges."
    )
    parser.add_argument("--label", default="k50")
    parser.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument(
        "--topics",
        default="hdfs:///user/bigdata/stackoverflow/output/topic_clusters_k50",
    )
    parser.add_argument("--baseline-pairs", default="")
    parser.add_argument("--output", default="")
    parser.add_argument(
        "--intra-topic-root",
        default="hdfs:///user/bigdata/stackoverflow/output/intra_topic",
    )
    parser.add_argument("--title-sim-floor", type=float, default=0.45)
    parser.add_argument("--tag-rescue-floor", type=float, default=0.40)
    parser.add_argument("--min-ora-doc-count", type=int, default=2)
    parser.add_argument("--max-ora-doc-count", type=int, default=150)
    parser.add_argument("--ora-boost", type=float, default=0.10)
    parser.add_argument("--cc-iterations", type=int, default=15)
    parser.add_argument("--shuffle-partitions", type=int, default=96)
    return parser.parse_args()


def pair_key(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "pair_key",
        concat_ws(
            "|",
            least(col("src"), col("dst")),
            greatest(col("src"), col("dst")),
        ),
    )


def empty_rescue_schema(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [],
        "src string, dst string, src_title string, dst_title string, src_score int, dst_score int, "
        "similarity_raw double, jaccard_distance double, shared_ora_codes array<string>, "
        "ora_match boolean, similarity double, ora_rescued boolean, topic_id int, "
        "edge_source string, cross_topic boolean",
    )


def build_cross_topic_rescue(
    docs: DataFrame,
    baseline_keys: DataFrame,
    *,
    title_sim_floor: float,
    tag_rescue_floor: float,
    min_ora_doc_count: int,
    max_ora_doc_count: int,
    ora_boost: float,
) -> DataFrame:
    spark = docs.sparkSession
    if "ora_codes" not in docs.columns:
        return empty_rescue_schema(spark)

    ora_docs = (
        docs.filter(size(col("ora_codes")) > 0)
        .select(
            "doc_id",
            "title",
            "score",
            "tags",
            "tokens",
            "topic_id",
            explode("ora_codes").alias("ora_code"),
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    ora_stats = (
        ora_docs.groupBy("ora_code")
        .agg(count("*").alias("doc_count"))
        .filter(
            (col("doc_count") >= min_ora_doc_count) & (col("doc_count") <= max_ora_doc_count)
        )
    )

    # Pre-filter both sides to only docs that share a valid ORA code.
    # This reduces the fan-out in the self-join: docs with invalid (too rare or
    # too common) ORA codes never participate in the cross-product.
    valid = ora_docs.join(ora_stats.select("ora_code"), "ora_code")
    left = valid.alias("left")
    right = valid.alias("right")

    joined = (
        left.join(right, "ora_code")
        .filter(col("left.doc_id") < col("right.doc_id"))
        .filter(col("left.topic_id") != col("right.topic_id"))
        .select(
            col("left.doc_id").alias("src"),
            col("right.doc_id").alias("dst"),
            col("left.title").alias("src_title"),
            col("right.title").alias("dst_title"),
            col("left.score").alias("src_score"),
            col("right.score").alias("dst_score"),
            col("left.tokens").alias("src_tokens"),
            col("right.tokens").alias("dst_tokens"),
            col("left.tags").alias("src_tags"),
            col("right.tags").alias("dst_tags"),
            col("left.topic_id").alias("topic_id"),
            col("ora_code").alias("shared_ora_code"),
        )
    )

    intersection_size = size(array_intersect(col("src_tokens"), col("dst_tokens")))
    union_size = size(array_union(col("src_tokens"), col("dst_tokens")))
    tag_overlap = size(array_intersect(col("src_tags"), col("dst_tags"))) > 0

    rescued = (
        joined.withColumn("similarity_raw", intersection_size / union_size)
        .filter(
            (col("similarity_raw") >= lit(title_sim_floor))
            | (tag_overlap & (col("similarity_raw") >= lit(tag_rescue_floor)))
        )
        .withColumn("shared_ora_codes", array(col("shared_ora_code")))
        .withColumn("ora_match", lit(True))
        .withColumn(
            "similarity",
            least(lit(1.0), col("similarity_raw") + lit(ora_boost)),
        )
        .withColumn("jaccard_distance", lit(1.0) - col("similarity_raw"))
        .withColumn("ora_rescued", lit(True))
        .withColumn("edge_source", lit("ora_cross_topic_rescue"))
        .withColumn("cross_topic", lit(True))
        .drop("src_tokens", "dst_tokens", "src_tags", "dst_tags", "shared_ora_code")
    )

    rescued = pair_key(rescued).join(baseline_keys, "pair_key", "left_anti").drop("pair_key")
    return rescued


def normalize_baseline(baseline: DataFrame) -> DataFrame:
    cols = baseline.columns
    out = baseline
    if "edge_source" not in cols:
        out = out.withColumn("edge_source", lit("lsh_intra"))
    if "cross_topic" not in cols:
        out = out.withColumn("cross_topic", lit(False))
    if "ora_rescued" not in cols:
        out = out.withColumn("ora_rescued", lit(False))
    return out


def dedupe_hybrid_edges(baseline: DataFrame, rescued: DataFrame) -> DataFrame:
    combined = normalize_baseline(baseline).unionByName(rescued, allowMissingColumns=True)
    window = Window.partitionBy(
        least(col("src"), col("dst")),
        greatest(col("src"), col("dst")),
    ).orderBy(col("similarity").desc(), col("edge_source").asc())

    return (
        combined.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )


def cluster_stats(clusters: DataFrame) -> tuple[int, int, int]:
    multi = clusters.filter(col("cluster_size") > 1)
    return (
        multi.select("cluster_id").distinct().count(),
        multi.count(),
        multi.agg({"cluster_size": "max"}).collect()[0][0] or 0,
    )


def main() -> None:
    args = parse_args()
    root = args.intra_topic_root.rstrip("/")
    label = args.label
    baseline_path = args.baseline_pairs or f"{root}/{label}/similar_pairs"
    output_base = args.output or f"{root}/{label}/hybrid"

    spark = SparkSession.builder.appName(f"stackoverflow-domain-rescue-{label}").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    sc = spark.sparkContext
    if not sc.getCheckpointDir():
        sc.setCheckpointDir(f"hdfs:///user/bigdata/stackoverflow/checkpoints/domain_rescue_{label}")

    baseline = spark.read.parquet(baseline_path)
    baseline_count = baseline.count()

    features = spark.read.parquet(args.features)
    keep = ["doc_id", "title", "score", "tags", "tokens"]
    if "ora_codes" in features.columns:
        keep.append("ora_codes")
    topics = spark.read.parquet(args.topics).select(
        col("doc_id"), col("cluster_id").alias("topic_id")
    )
    docs = features.select(*keep).join(topics, "doc_id", "inner")

    baseline_keys = pair_key(baseline.select("src", "dst")).select("pair_key").distinct()
    rescued = build_cross_topic_rescue(
        docs,
        baseline_keys,
        title_sim_floor=args.title_sim_floor,
        tag_rescue_floor=args.tag_rescue_floor,
        min_ora_doc_count=args.min_ora_doc_count,
        max_ora_doc_count=args.max_ora_doc_count,
        ora_boost=args.ora_boost,
    )
    rescued_count = rescued.count()

    hybrid = dedupe_hybrid_edges(baseline, rescued)
    hybrid_count = hybrid.count()

    pairs_out = output_base.rstrip("/") + "/hybrid_pairs"
    rescued_out = output_base.rstrip("/") + "/rescued_pairs"
    rescued_csv = output_base.rstrip("/") + "/rescued_pairs_samples_csv"
    clusters_out = output_base.rstrip("/") + "/hybrid_clusters"
    clusters_csv = output_base.rstrip("/") + "/hybrid_clusters_samples_csv"
    summary_out = output_base.rstrip("/") + "/hybrid_summary_csv"
    compare_out = output_base.rstrip("/") + "/baseline_vs_hybrid_csv"

    hybrid.write.mode("overwrite").parquet(pairs_out)
    if rescued_count:
        rescued.write.mode("overwrite").parquet(rescued_out)
        sample = rescued.orderBy(col("similarity").desc()).limit(500)
        sample = sample.withColumn(
            "shared_ora_codes_text", concat_ws("|", col("shared_ora_codes"))
        ).drop("shared_ora_codes")
        sample.coalesce(1).write.mode("overwrite").option("header", True).csv(rescued_csv)

    questions = topics.join(
        features.select("doc_id", "title", "score", "tags"), "doc_id", "inner"
    )
    edge_input = hybrid.select("src", "dst", "similarity")
    clusters = build_connected_components(spark, edge_input, questions, args.cc_iterations)
    clusters.write.mode("overwrite").parquet(clusters_out)

    sample_clusters = (
        clusters.withColumn("tags_text", concat_ws("|", col("tags")))
        .drop("tags")
        .filter(col("cluster_size") > 1)
        .orderBy(col("cluster_size").desc(), col("cluster_id").asc())
        .limit(2000)
    )
    sample_clusters.coalesce(1).write.mode("overwrite").option("header", True).csv(clusters_csv)

    group_count, question_count, max_size = cluster_stats(clusters)
    avg_sim = hybrid.agg(avg("similarity").alias("v")).collect()[0]["v"] or 0.0

    baseline_summary_path = f"{root}/{label}/summary_csv"
    baseline_groups = 0
    baseline_questions = 0
    try:
        bsum = spark.read.option("header", True).csv(baseline_summary_path)
        row = bsum.collect()[0].asDict()
        baseline_groups = int(row.get("multi_doc_cluster_count") or 0)
        baseline_questions = int(row.get("multi_doc_question_count") or 0)
    except Exception:  # noqa: BLE001
        baseline_clusters_path = f"{root}/{label}/clusters"
        if spark.read.parquet(baseline_clusters_path).head(1):
            bclusters = spark.read.parquet(baseline_clusters_path)
            baseline_groups, baseline_questions, _ = cluster_stats(bclusters)

    spark.createDataFrame(
        [
            (
                label,
                int(baseline_count),
                int(rescued_count),
                int(hybrid_count),
                int(baseline_groups),
                int(group_count),
                int(baseline_questions),
                int(question_count),
                int(max_size),
                float(avg_sim),
            )
        ],
        [
            "run_label",
            "baseline_pair_count",
            "rescued_pair_count",
            "hybrid_pair_count",
            "baseline_multi_doc_cluster_count",
            "hybrid_multi_doc_cluster_count",
            "baseline_multi_doc_question_count",
            "hybrid_multi_doc_question_count",
            "max_cluster_size",
            "avg_similarity",
        ],
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(summary_out)

    spark.createDataFrame(
        [
            (
                label,
                "intra_topic_lsh",
                int(baseline_count),
                int(baseline_groups),
                int(baseline_questions),
            ),
            (
                label,
                "domain_hybrid_graph",
                int(hybrid_count),
                int(group_count),
                int(question_count),
            ),
        ],
        [
            "run_label",
            "method",
            "pair_count",
            "multi_doc_cluster_count",
            "multi_doc_question_count",
        ],
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(compare_out)

    print(
        f"Done label={label}: baseline_pairs={baseline_count} rescued={rescued_count} "
        f"hybrid_pairs={hybrid_count} groups {baseline_groups}->{group_count}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
