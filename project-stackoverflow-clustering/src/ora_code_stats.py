#!/usr/bin/env python3
"""Aggregate ORA-XXXXX error-code distribution from preprocessed questions.

Reads the preprocessed questions parquet (must contain an `ora_codes` array column,
produced by the updated `preprocess.py`) and writes:

  - <output>/ora_code_distribution_csv/part-*.csv: per-code question count + share
  - <output>/ora_code_summary_csv/part-*.csv: rollup (questions with at least one
    ORA code, total occurrences, distinct codes)

The CSVs are intentionally small so they can be committed to the repo for the
defense slides and demo.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, explode, lit, size, sum as spark_sum, when


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute ORA error-code statistics.")
    parser.add_argument("--questions", default="hdfs:///user/bigdata/stackoverflow/parquet/questions")
    parser.add_argument(
        "--output",
        default="hdfs:///user/bigdata/stackoverflow/output/ora_codes",
        help="Output directory. Two CSV subdirectories are written under it.",
    )
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-ora-stats").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    df = spark.read.parquet(args.questions)
    if "ora_codes" not in df.columns:
        raise SystemExit(
            "ora_codes column not found in questions parquet. Re-run preprocess.py "
            "with the updated code to populate the column before running this script."
        )

    df = df.select("doc_id", "ora_codes").persist()
    total_questions = df.count()
    questions_with_ora = df.filter(size(col("ora_codes")) > 0).count()

    exploded = df.select("doc_id", explode("ora_codes").alias("ora_code"))
    code_counts = (
        exploded.groupBy("ora_code")
        .agg(
            count("*").alias("occurrence_count"),
            countDistinct("doc_id").alias("question_count"),
        )
        .withColumn(
            "question_share",
            col("question_count") / lit(max(total_questions, 1)),
        )
        .orderBy(col("question_count").desc(), col("ora_code").asc())
    )

    distinct_codes = code_counts.count()
    total_occurrences = exploded.count()

    code_counts.limit(args.top_n).coalesce(1).write.mode("overwrite").option("header", True).csv(
        args.output.rstrip("/") + "/ora_code_distribution_csv"
    )

    spark.createDataFrame(
        [
            (
                int(total_questions),
                int(questions_with_ora),
                float(questions_with_ora) / max(total_questions, 1),
                int(distinct_codes),
                int(total_occurrences),
            )
        ],
        [
            "total_questions",
            "questions_with_ora_code",
            "ora_coverage_ratio",
            "distinct_ora_codes",
            "total_ora_occurrences",
        ],
    ).coalesce(1).write.mode("overwrite").option("header", True).csv(
        args.output.rstrip("/") + "/ora_code_summary_csv"
    )

    spark.stop()


if __name__ == "__main__":
    main()
