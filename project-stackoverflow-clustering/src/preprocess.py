#!/usr/bin/env python3
"""Build cleaned question documents for clustering."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array_distinct,
    array_join,
    col,
    concat_ws,
    expr,
    lower,
    regexp_replace,
    transform,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preprocess StackOverflow question JSONL.")
    parser.add_argument("--input", default="hdfs:///user/bigdata/stackoverflow/jsonl/questions.jsonl")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/parquet/questions")
    parser.add_argument("--top-answers", type=int, default=3)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def clean_text_expr(source_col):
    clean = lower(source_col)
    clean = regexp_replace(clean, r"<pre><code>[\s\S]*?</code></pre>", " codeblock ")
    clean = regexp_replace(clean, r"<[^>]+>", " ")
    clean = regexp_replace(clean, r"&[a-z]+;", " ")
    clean = regexp_replace(clean, r"[^a-z0-9_#.+\-/ ]", " ")
    clean = regexp_replace(clean, r"\s+", " ")
    return clean


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-preprocess").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    df = spark.read.json(args.input)
    df = df.withColumn("tags_text", array_join(col("tags"), " "))
    df = df.withColumn(
        "top_answers",
        expr(
            f"""
            transform(
              slice(
                reverse(array_sort(transform(
                  answers,
                  x -> named_struct('score', coalesce(x.score, 0), 'body', x.body)
                ))),
                1,
                {args.top_answers}
              ),
              x -> x.body
            )
            """
        ),
    )
    df = df.withColumn("answers_text", array_join(col("top_answers"), " "))
    df = df.withColumn(
        "raw_doc_text",
        concat_ws(
            " ",
            col("title"),
            col("title"),
            col("title"),
            col("body"),
            col("body"),
            col("tags_text"),
            col("answers_text"),
        ),
    )

    out = df.select(
        col("question_id").cast("string").alias("doc_id"),
        col("question_id").cast("string").alias("question_id"),
        col("title"),
        col("body"),
        col("tags"),
        col("score").cast("int").alias("score"),
        expr("case when answers is null then 0 else size(answers) end").alias("answer_count"),
        expr("case when comments is null then 0 else size(comments) end").alias("comment_count"),
        clean_text_expr(col("raw_doc_text")).alias("clean_text"),
        # ORA-XXXXX error codes are a domain-specific strong signal in Oracle Q&A.
        # We extract them as a structured array so the LSH stage can apply a domain
        # boost when two documents share the same code.
        array_distinct(
            transform(
                expr(r"regexp_extract_all(lower(coalesce(raw_doc_text, '')), '\\bora-\\d{4,5}\\b', 0)"),
                lambda c: lower(c),
            )
        ).alias("ora_codes"),
    )

    out.repartition(args.shuffle_partitions).write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
