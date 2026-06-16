#!/usr/bin/env python3
"""Tag-partitioned MinHashLSH for similar-question detection.

Motivation
----------
A global MinHashLSH self-join over 152K Oracle questions surfaces a stage-9
long-tail (see docs/verification/full_approx075_2026-06-14.md): a few hash
buckets contain >10K documents, the resulting candidate join explodes, and the
job times out after ~29 minutes on the course cluster.

This script splits the work along the natural domain boundary of Oracle Q&A
data: every question carries multiple StackOverflow tags, and the *primary*
tag (the first non-trivial tag, e.g. sql / plsql / java / jdbc) is a strong
indicator of the question's topic. We pick a primary tag per question, then
fit and run an independent MinHashLSH inside each tag bucket. The final
similar-pairs output is a UNION across all buckets.

Trade-off (must be stated in the report and slides)
---------------------------------------------------
+ Each per-tag job is far smaller; a single oversized hash bucket is bounded
  by the size of its tag, not by the full corpus.
+ Per-bucket metrics (size, candidate_pair_count, output_pair_count, elapsed)
  produce a credible "fixed the long tail" story for the defense.
- Cross-tag duplicates (e.g. the same question tagged 'sql' on one post and
  'plsql' on another) are missed. We accept this as a precision-favouring
  trade-off; the report quantifies the gap by also reporting how many global
  baseline pairs are recovered.

Inputs
------
- features parquet from feature_engineering.py (must contain `tags`,
  `term_features`, `tokens`; carries `ora_codes` if produced by the updated
  preprocess.py)

Outputs
-------
- <output> parquet: similar_pairs from every bucket (same schema as
  cluster_lsh.py output, plus a `primary_tag` column)
- <output>_samples_csv: top-similarity samples (coalesced)
- <metrics-output>: one row per primary_tag with timing + pair counts
"""

from __future__ import annotations

import argparse
import time
from typing import Iterable, List, Optional

from pyspark import StorageLevel
from pyspark.ml.feature import MinHashLSH
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    array_intersect,
    array_union,
    col,
    expr,
    least,
    lit,
    row_number,
    size,
    when,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tag-partitioned MinHashLSH similarity search.")
    parser.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument(
        "--output",
        default="hdfs:///user/bigdata/stackoverflow/output/similar_pairs_partitioned",
    )
    parser.add_argument("--metrics-output", help="Optional CSV output directory for per-bucket metrics.")
    parser.add_argument("--distance-threshold", type=float, default=0.35)
    parser.add_argument("--similarity-threshold", type=float, default=0.65)
    parser.add_argument("--num-hash-tables", type=int, default=4)
    parser.add_argument("--top-n-per-doc", type=int, default=10)
    parser.add_argument("--min-token-count", type=int, default=2)
    parser.add_argument(
        "--min-bucket-size",
        type=int,
        default=50,
        help="Tag buckets smaller than this are merged into a `_misc` bucket.",
    )
    parser.add_argument(
        "--exclude-tags",
        default="oracle-database",
        help="Comma-separated tags to skip when picking the primary tag (case-insensitive).",
    )
    parser.add_argument(
        "--max-buckets",
        type=int,
        default=0,
        help=(
            "If > 0, only process the largest N buckets. Useful for smoke tests; the "
            "remaining buckets are skipped and logged in metrics with status='skipped'."
        ),
    )
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    parser.add_argument("--ora-boost", type=float, default=0.10)
    parser.add_argument("--ora-rescue-floor", type=float, default=0.0)
    return parser.parse_args()


def assign_primary_tag(df: DataFrame, exclude: List[str]) -> DataFrame:
    """Pick the first tag that is not in `exclude`. Returns `_misc` when none."""
    excluded_array = "array(" + ", ".join(f"'{t.lower()}'" for t in exclude) + ")" if exclude else "array()"
    primary_expr = f"""
        coalesce(
            element_at(
                filter(
                    transform(coalesce(tags, array()), t -> lower(t)),
                    t -> NOT array_contains({excluded_array}, t)
                ),
                1
            ),
            '_misc'
        )
    """
    return df.withColumn("primary_tag", expr(primary_expr))


def run_lsh_on_bucket(
    spark: SparkSession,
    bucket_df: DataFrame,
    args: argparse.Namespace,
    primary_tag: str,
    has_ora: bool,
) -> Optional[DataFrame]:
    """Fit MinHashLSH on a single primary-tag subset and return its similar pairs."""
    bucket_df = bucket_df.persist(StorageLevel.MEMORY_AND_DISK)
    doc_count = bucket_df.count()
    if doc_count < 2:
        bucket_df.unpersist()
        return None

    lsh = MinHashLSH(
        inputCol="term_features",
        outputCol="hashes",
        numHashTables=args.num_hash_tables,
    )
    model = lsh.fit(bucket_df)

    base_cols = ["doc_id", "title", "score", "tokens", "term_features"]
    if has_ora:
        base_cols.append("ora_codes")
    left = bucket_df.select(*base_cols)
    right = bucket_df.select(*base_cols)
    joined = model.approxSimilarityJoin(left, right, args.distance_threshold, distCol="lsh_jaccard_distance")

    select_exprs = [
        col("datasetA.doc_id").alias("src"),
        col("datasetB.doc_id").alias("dst"),
        col("datasetA.title").alias("src_title"),
        col("datasetB.title").alias("dst_title"),
        col("datasetA.score").alias("src_score"),
        col("datasetB.score").alias("dst_score"),
        col("datasetA.tokens").alias("src_tokens"),
        col("datasetB.tokens").alias("dst_tokens"),
    ]
    if has_ora:
        select_exprs += [
            col("datasetA.ora_codes").alias("src_ora_codes"),
            col("datasetB.ora_codes").alias("dst_ora_codes"),
        ]

    candidate_pairs = (
        joined.filter(col("datasetA.doc_id") < col("datasetB.doc_id"))
        .select(*select_exprs)
        .dropDuplicates(["src", "dst"])
    )

    intersection_size = size(array_intersect(col("src_tokens"), col("dst_tokens")))
    union_size = size(array_union(col("src_tokens"), col("dst_tokens")))

    pairs = (
        candidate_pairs.withColumn("similarity_raw", intersection_size / union_size)
        .withColumn("jaccard_distance", lit(1.0) - col("similarity_raw"))
    )
    if has_ora:
        shared = array_intersect(col("src_ora_codes"), col("dst_ora_codes"))
        pairs = (
            pairs.withColumn("shared_ora_codes", shared)
            .withColumn("ora_match", size(col("shared_ora_codes")) > 0)
            .withColumn(
                "similarity",
                least(
                    lit(1.0),
                    col("similarity_raw") + when(col("ora_match"), lit(args.ora_boost)).otherwise(lit(0.0)),
                ),
            )
        )
    else:
        pairs = (
            pairs.withColumn("similarity", col("similarity_raw"))
            .withColumn("shared_ora_codes", lit(None).cast("array<string>"))
            .withColumn("ora_match", lit(False))
        )

    base_filter = (col("jaccard_distance") <= args.distance_threshold) & (
        col("similarity") >= args.similarity_threshold
    )
    if has_ora and args.ora_rescue_floor > 0:
        rescue_filter = (
            col("ora_match")
            & (col("similarity_raw") >= args.ora_rescue_floor)
            & (col("similarity_raw") < args.similarity_threshold)
        )
        keep_filter = base_filter | rescue_filter
    else:
        rescue_filter = lit(False)
        keep_filter = base_filter

    pairs = (
        pairs.withColumn(
            "ora_rescued",
            when((~base_filter) & rescue_filter, lit(True)).otherwise(lit(False)),
        )
        .filter(keep_filter)
        .withColumn("primary_tag", lit(primary_tag))
    )

    window = Window.partitionBy("src").orderBy(col("similarity").desc(), col("dst").asc())
    pairs = pairs.withColumn("rank", row_number().over(window)).filter(col("rank") <= args.top_n_per_doc).drop("rank")

    return pairs.drop("src_tokens", "dst_tokens")


def union_compatible(dfs: Iterable[DataFrame]) -> Optional[DataFrame]:
    out: Optional[DataFrame] = None
    for df in dfs:
        if df is None:
            continue
        out = df if out is None else out.unionByName(df, allowMissingColumns=True)
    return out


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-lsh-tag-partitioned").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    raw = spark.read.parquet(args.features)
    has_ora = "ora_codes" in raw.columns
    keep_cols = ["doc_id", "title", "tags", "score", "tokens", "term_features"]
    if has_ora:
        keep_cols.append("ora_codes")
    features = (
        raw.select(*keep_cols)
        .filter(size(col("tokens")) >= args.min_token_count)
    )

    exclude = [t.strip() for t in args.exclude_tags.split(",") if t.strip()]
    tagged = assign_primary_tag(features, exclude)

    # Buckets below `min_bucket_size` are folded into _misc so we do not waste a
    # job on tiny bucket-of-1 partitions.
    sizes = tagged.groupBy("primary_tag").count().collect()
    small_tags = {row["primary_tag"] for row in sizes if row["count"] < args.min_bucket_size}
    if small_tags:
        tagged = tagged.withColumn(
            "primary_tag",
            when(col("primary_tag").isin(list(small_tags)), lit("_misc")).otherwise(col("primary_tag")),
        )

    bucket_sizes = (
        tagged.groupBy("primary_tag").count().orderBy(col("count").desc()).collect()
    )
    if args.max_buckets > 0:
        active = [row["primary_tag"] for row in bucket_sizes[: args.max_buckets]]
        skipped = [row for row in bucket_sizes[args.max_buckets:]]
    else:
        active = [row["primary_tag"] for row in bucket_sizes]
        skipped = []

    metrics_rows = []
    bucket_outputs: List[DataFrame] = []
    for primary_tag in active:
        bucket_df = tagged.filter(col("primary_tag") == primary_tag).select(*keep_cols)
        doc_count = bucket_df.count()
        start = time.monotonic()
        try:
            pairs = run_lsh_on_bucket(spark, bucket_df, args, primary_tag, has_ora)
            elapsed = time.monotonic() - start
            if pairs is None:
                metrics_rows.append((primary_tag, doc_count, 0, elapsed, "empty"))
                continue
            pair_count = pairs.count()
            bucket_outputs.append(pairs)
            metrics_rows.append((primary_tag, doc_count, pair_count, elapsed, "ok"))
        except Exception as exc:  # noqa: BLE001 - we want to record per-bucket failures
            elapsed = time.monotonic() - start
            metrics_rows.append((primary_tag, doc_count, 0, elapsed, f"error:{type(exc).__name__}"))

    for row in skipped:
        metrics_rows.append((row["primary_tag"], int(row["count"]), 0, 0.0, "skipped"))

    combined = union_compatible(bucket_outputs)
    if combined is None:
        combined = spark.createDataFrame([], "src string, dst string")
    combined.write.mode("overwrite").parquet(args.output)

    if "similarity" in combined.columns:
        combined.orderBy(col("similarity").desc()).limit(500).coalesce(1).write.mode("overwrite").option(
            "header", True
        ).csv(args.output.rstrip("/") + "_samples_csv")

    if args.metrics_output:
        spark.createDataFrame(
            metrics_rows,
            ["primary_tag", "doc_count", "pair_count", "elapsed_seconds", "status"],
        ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.metrics_output)

    spark.stop()


if __name__ == "__main__":
    main()
