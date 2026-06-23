#!/usr/bin/env python3
"""Topic clustering with BisectingKMeans."""

from __future__ import annotations

import argparse

from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import SparseVector, VectorUDT, Vectors
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, collect_list, concat_ws, count, explode, lit, row_number
from pyspark.sql.functions import udf


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run topic clustering.")
    parser.add_argument("--features", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/output/topic_clusters")
    parser.add_argument("--k", type=int, default=50)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    parser.add_argument(
        "--training-num-features",
        type=int,
        default=0,
        help=(
            "If > 0 and smaller than the stored TF-IDF dimension, fold sparse vectors "
            "to this many dimensions only for topic clustering. This keeps the K "
            "experiment semantics while reducing BisectingKMeans driver summaries."
        ),
    )
    parser.add_argument(
        "--skip-silhouette",
        action="store_true",
        help="Skip the expensive silhouette pass for large-K recovery runs.",
    )
    return parser.parse_args()


def fold_vector(vector, target_dim: int):
    """Fold a sparse vector by modulo hashing without changing the source features."""
    if target_dim <= 0 or vector is None or vector.size <= target_dim:
        return vector

    values: dict[int, float] = {}
    if isinstance(vector, SparseVector):
        iterator = zip(vector.indices, vector.values)
    else:
        iterator = ((idx, value) for idx, value in enumerate(vector.toArray()) if value)

    for idx, value in iterator:
        folded_idx = int(idx) % target_dim
        values[folded_idx] = values.get(folded_idx, 0.0) + float(value)

    return Vectors.sparse(target_dim, sorted(values.items()))


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-topic-clustering").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    raw_features = spark.read.parquet(args.features)
    features_col = "features"
    if args.training_num_features > 0:
        fold_udf = udf(lambda vector: fold_vector(vector, args.training_num_features), VectorUDT())
        features = raw_features.withColumn("topic_features", fold_udf(col("features"))).cache()
        features_col = "topic_features"
    else:
        features = raw_features.cache()

    model = BisectingKMeans(k=args.k, seed=args.seed, featuresCol=features_col, predictionCol="cluster_id").fit(features)
    predicted = model.transform(features).cache()

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

    silhouette = (
        float("nan")
        if args.skip_silhouette
        else ClusteringEvaluator(featuresCol=features_col, predictionCol="cluster_id").evaluate(predicted)
    )
    spark.createDataFrame([(args.k, float(silhouette))], ["k", "silhouette"]).withColumn(
        "metric", lit("silhouette")
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(args.output.rstrip("/") + "_metrics_csv")

    spark.stop()


if __name__ == "__main__":
    main()
