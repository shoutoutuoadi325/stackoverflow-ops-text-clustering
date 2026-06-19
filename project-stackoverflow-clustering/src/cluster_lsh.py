#!/usr/bin/env python3
"""Find similar StackOverflow questions with MinHash LSH.

Memory-optimised: the LSH join is performed on a slim (doc_id, term_features)
view so that heavy metadata columns (tokens, titles, ora_codes) are not
duplicated across every candidate pair inside the LSH internal cross-join.
Enrichment joins attach metadata only to the surviving candidate pairs.
"""

from __future__ import annotations

import argparse

from pyspark.ml.feature import MinHashLSH
from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    array_intersect,
    array_union,
    avg,
    col,
    concat_ws,
    count,
    least,
    lit,
    posexplode,
    row_number,
    size,
    when,
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
    parser.add_argument(
        "--join-strategy",
        choices=["approx", "bucket"],
        default="approx",
        help=(
            "Use Spark ML approxSimilarityJoin so --distance-threshold is applied during LSH candidate generation. "
            "Use bucket for the historical bucket self-join implementation with --max-bucket-size skew control."
        ),
    )
    parser.add_argument("--min-token-count", type=int, default=2)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    parser.add_argument(
        "--ora-boost",
        type=float,
        default=0.10,
        help=(
            "Domain boost added to similarity when two documents share at least one ORA-XXXXX "
            "error code. Set to 0 to disable the boost. The boosted similarity is capped at 1.0."
        ),
    )
    parser.add_argument(
        "--ora-rescue-floor",
        type=float,
        default=0.0,
        help=(
            "When > 0, pairs whose raw similarity is below --similarity-threshold but above this "
            "floor and that share an ORA code are kept as 'ORA-rescued' candidates. They are "
            "marked with ora_rescued=true in the output so the report can show domain-knowledge "
            "recall improvements without polluting the main similar_pairs result."
        ),
    )
    return parser.parse_args()


def _enrich_candidates(candidates, features, has_ora: bool, shuffle_partitions: int):
    """Join slim (src, dst) pairs back with features to attach metadata.

    This is the second phase of the two-phase LSH pattern: only the candidate
    pairs that survived LSH distance filtering carry the heavy token / title /
    ora_codes payload.
    """
    meta_cols = ["doc_id", "title", "score", "tokens"]
    if has_ora:
        meta_cols.append("ora_codes")

    src_meta = features.select(
        *[col(c).alias(f"src_{c}" if c != "doc_id" else "src_doc_id") for c in meta_cols]
    )
    dst_meta = features.select(
        *[col(c).alias(f"dst_{c}" if c != "doc_id" else "dst_doc_id") for c in meta_cols]
    )

    select_exprs = [
        "src",
        "dst",
        col("src_title").alias("src_title"),
        col("dst_title").alias("dst_title"),
        col("src_score").alias("src_score"),
        col("dst_score").alias("dst_score"),
        col("src_tokens").alias("src_tokens"),
        col("dst_tokens").alias("dst_tokens"),
    ]
    if has_ora:
        select_exprs += [
            col("src_ora_codes").alias("src_ora_codes"),
            col("dst_ora_codes").alias("dst_ora_codes"),
        ]

    return (
        candidates.join(src_meta, candidates.src == col("src_doc_id"), "inner")
        .drop("src_doc_id")
        .join(dst_meta, candidates.dst == col("dst_doc_id"), "inner")
        .drop("dst_doc_id")
        .select(*select_exprs)
        .repartition(shuffle_partitions, "src")
    )


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-lsh-clustering").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    raw_features = spark.read.parquet(args.features)
    has_ora = "ora_codes" in raw_features.columns
    base_cols = ["doc_id", "title", "tags", "score", "tokens", "term_features"]
    if has_ora:
        base_cols.append("ora_codes")
    features = raw_features.select(*base_cols).filter(size(col("tokens")) >= args.min_token_count).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    input_count = features.count()

    lsh = MinHashLSH(
        inputCol="term_features",
        outputCol="hashes",
        numHashTables=args.num_hash_tables,
    )
    model = lsh.fit(features)

    candidate_pairs = build_candidate_pairs(model, features, args).persist(StorageLevel.MEMORY_AND_DISK)
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
            *(["src_ora_codes", "dst_ora_codes"] if has_ora else []),
            (intersection_size / union_size).alias("token_similarity"),
        )
        .withColumn("jaccard_distance", lit(1.0) - col("token_similarity"))
        .withColumn("similarity_raw", col("token_similarity"))
        .drop("token_similarity")
    )

    if has_ora:
        shared_ora = array_intersect(col("src_ora_codes"), col("dst_ora_codes"))
        pairs = (
            pairs.withColumn("shared_ora_codes", shared_ora)
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
        pairs = pairs.withColumn("similarity", col("similarity_raw")).withColumn(
            "shared_ora_codes", lit(None).cast("array<string>")
        ).withColumn("ora_match", lit(False))

    # Drop token arrays immediately — no longer needed after similarity computation.
    pairs = pairs.drop("src_tokens", "dst_tokens")

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

    pairs = pairs.withColumn(
        "ora_rescued",
        when((~base_filter) & rescue_filter, lit(True)).otherwise(lit(False)),
    ).filter(keep_filter).persist(StorageLevel.MEMORY_AND_DISK)
    pair_count = pairs.count()

    window = Window.partitionBy("src").orderBy(col("similarity").desc(), col("dst").asc())
    pairs = pairs.withColumn("rank", row_number().over(window)).filter(col("rank") <= args.top_n_per_doc).drop("rank")
    output_pair_count = pairs.count()

    pairs.write.mode("overwrite").parquet(args.output)
    pairs.orderBy(col("similarity").desc()).limit(500).coalesce(1).write.mode("overwrite").option(
        "header", True
    ).csv(args.output.rstrip("/") + "_samples_csv")
    if args.metrics_output:
        agg_exprs = [avg("similarity").alias("avg_similarity")]
        if has_ora:
            agg_exprs += [
                avg(when(col("ora_match"), 1.0).otherwise(0.0)).alias("ora_match_rate"),
                avg(when(col("ora_rescued"), 1.0).otherwise(0.0)).alias("ora_rescued_rate"),
            ]
        similarity_stats = pairs.agg(*agg_exprs).collect()[0]
        ora_match_rate = float(similarity_stats["ora_match_rate"]) if has_ora else 0.0
        ora_rescued_rate = float(similarity_stats["ora_rescued_rate"]) if has_ora else 0.0
        spark.createDataFrame(
            [
                (
                    args.similarity_threshold,
                    args.distance_threshold,
                    args.num_hash_tables,
                    args.max_bucket_size,
                    args.join_strategy,
                    args.top_n_per_doc,
                    args.min_token_count,
                    args.ora_boost,
                    args.ora_rescue_floor,
                    input_count,
                    candidate_pair_count,
                    pair_count,
                    output_pair_count,
                    float(similarity_stats.avg_similarity or 0.0),
                    ora_match_rate,
                    ora_rescued_rate,
                )
            ],
            [
                "similarity_threshold",
                "distance_threshold",
                "num_hash_tables",
                "max_bucket_size",
                "join_strategy",
                "top_n_per_doc",
                "min_token_count",
                "ora_boost",
                "ora_rescue_floor",
                "input_count",
                "candidate_pair_count",
                "pre_topn_pair_count",
                "pair_count",
                "avg_similarity",
                "ora_match_rate",
                "ora_rescued_rate",
            ],
        ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.metrics_output)
    spark.stop()


def build_candidate_pairs(model: MinHashLSH, features, args: argparse.Namespace):
    if args.join_strategy == "bucket":
        return build_bucket_candidate_pairs(model, features, args)
    return build_approx_candidate_pairs(model, features, args)


def build_approx_candidate_pairs(model: MinHashLSH, features, args: argparse.Namespace):
    """Two-phase approx strategy: slim LSH join → enrichment join for metadata.

    Phase 1 runs approxSimilarityJoin on only (doc_id, term_features) so the
    heavy token/title/ora payloads are not duplicated across every candidate.
    Phase 2 joins surviving candidates back with features to attach metadata.
    """
    has_ora = "ora_codes" in features.columns

    # Phase 1: slim LSH join — only the minimum columns needed.
    slim = features.select("doc_id", "term_features")
    joined = model.approxSimilarityJoin(
        slim,
        slim,
        args.distance_threshold,
        distCol="lsh_jaccard_distance",
    )

    candidates = (
        joined.filter(col("datasetA.doc_id") < col("datasetB.doc_id"))
        .select(
            col("datasetA.doc_id").alias("src"),
            col("datasetB.doc_id").alias("dst"),
        )
        .dropDuplicates(["src", "dst"])
    )

    # Phase 2: enrich with metadata (tokens, titles, scores, ora_codes).
    return _enrich_candidates(candidates, features, has_ora, args.shuffle_partitions)


def build_bucket_candidate_pairs(model: MinHashLSH, features, args: argparse.Namespace):
    """Two-phase bucket strategy: slim hash explosion → enrichment join.

    Uses model.transform() + posexplode on hashes so that only doc_ids are
    shuffled during the bucket self-join. term_features is dropped after
    transform since hashes carry the needed information.
    """
    has_ora = "ora_codes" in features.columns

    # Phase 1: transform slim features → explode hashes → bucket self-join.
    slim = features.select("doc_id", "term_features")
    hashed = model.transform(slim).select("doc_id", posexplode("hashes").alias("hash_table", "hash_value"))

    buckets = hashed.withColumn(
        "hash_key", concat_ws(":", col("hash_table").cast("string"), col("hash_value").cast("string"))
    )

    bucket_sizes = buckets.groupBy("hash_key").agg(count("*").alias("bucket_size")).filter(
        (col("bucket_size") > 1) & (col("bucket_size") <= args.max_bucket_size)
    )
    buckets = (
        buckets.join(bucket_sizes.select("hash_key"), "hash_key")
        .repartition(args.shuffle_partitions, "hash_key")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    left = buckets.alias("left")
    right = buckets.alias("right")
    candidates = (
        left.join(right, "hash_key")
        .filter(col("left.doc_id") < col("right.doc_id"))
        .select(
            col("left.doc_id").alias("src"),
            col("right.doc_id").alias("dst"),
        )
        .dropDuplicates(["src", "dst"])
    )

    # Phase 2: enrich with metadata (tokens, titles, scores, ora_codes).
    return _enrich_candidates(candidates, features, has_ora, args.shuffle_partitions)


if __name__ == "__main__":
    main()
