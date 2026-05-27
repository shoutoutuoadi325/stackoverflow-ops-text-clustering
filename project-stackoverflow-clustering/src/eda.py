#!/usr/bin/env python3
"""Spark EDA for StackOverflow Oracle questions."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    desc,
    explode,
    expr,
    length,
    lower,
    max as spark_max,
    regexp_replace,
    split,
    sum as spark_sum,
    when,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate EDA outputs.")
    parser.add_argument("--input", default="hdfs:///user/bigdata/stackoverflow/jsonl/questions.jsonl")
    parser.add_argument("--input-format", choices=["jsonl", "parquet"], default="jsonl")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/output/eda")
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def add_counts(df):
    if "answers" in df.columns:
        df = df.withColumn("answer_count", expr("case when answers is null then 0 else size(answers) end"))
        df = df.withColumn("question_comment_count", expr("case when comments is null then 0 else size(comments) end"))
        df = df.withColumn(
            "answer_comment_count",
            expr("coalesce(aggregate(answers, 0, (acc, x) -> acc + coalesce(size(x.comments), 0)), 0)"),
        )
    else:
        df = df.withColumn("question_comment_count", col("comment_count"))
        df = df.withColumn("answer_comment_count", expr("0"))
    return df


def write_csv(df, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(path)


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-eda").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    df = spark.read.json(args.input) if args.input_format == "jsonl" else spark.read.parquet(args.input)
    df = add_counts(df).cache()

    summary_row = df.agg(
        count("*").alias("questions"),
        spark_sum("answer_count").alias("answers"),
        spark_sum("question_comment_count").alias("question_comments"),
        spark_sum("answer_comment_count").alias("answer_comments"),
        avg("answer_count").alias("avg_answers_per_question"),
        spark_max("answer_count").alias("max_answers_per_question"),
        spark_sum(when(col("answer_count") == 0, 1).otherwise(0)).alias("unanswered_questions"),
        spark_max("score").alias("max_question_score"),
    )
    summary = summary_row.selectExpr(
        "stack(8, "
        "'questions', cast(questions as string), "
        "'answers', cast(answers as string), "
        "'question_comments', cast(question_comments as string), "
        "'answer_comments', cast(answer_comments as string), "
        "'avg_answers_per_question', cast(round(avg_answers_per_question, 3) as string), "
        "'max_answers_per_question', cast(max_answers_per_question as string), "
        "'unanswered_questions', cast(unanswered_questions as string), "
        "'max_question_score', cast(max_question_score as string)"
        ") as (metric, value)"
    )
    write_csv(summary, args.output.rstrip("/") + "/eda_summary")

    top_tags = df.select(explode("tags").alias("tag")).groupBy("tag").agg(count("*").alias("count")).orderBy(desc("count"))
    write_csv(top_tags.limit(100), args.output.rstrip("/") + "/top_tags")

    score_dist = df.withColumn(
        "score_bucket",
        when(col("score") < 0, "<0")
        .when(col("score") == 0, "0")
        .when(col("score") <= 2, "1-2")
        .when(col("score") <= 5, "3-5")
        .when(col("score") <= 10, "6-10")
        .when(col("score") <= 50, "11-50")
        .otherwise(">50"),
    ).groupBy("score_bucket").agg(count("*").alias("count"))
    write_csv(score_dist.orderBy("score_bucket"), args.output.rstrip("/") + "/score_distribution")

    answer_score = df.groupBy("answer_count").agg(count("*").alias("question_count"), avg("score").alias("avg_score"))
    write_csv(answer_score.orderBy("answer_count"), args.output.rstrip("/") + "/answer_count_score_relation")

    text_col = lower(concat_ws(" ", col("title"), col("body")))
    clean = regexp_replace(text_col, r"<[^>]+>|&[a-z]+;", " ")
    clean = regexp_replace(clean, r"[^a-z0-9_#.+\-/ ]", " ")
    df_text = df.withColumn("eda_text", clean).withColumn("text_length", length("eda_text"))
    length_dist = df_text.withColumn(
        "length_bucket",
        when(col("text_length") < 200, "<200")
        .when(col("text_length") < 500, "200-499")
        .when(col("text_length") < 1000, "500-999")
        .when(col("text_length") < 2000, "1000-1999")
        .otherwise(">=2000"),
    ).groupBy("length_bucket").agg(count("*").alias("count"), avg("text_length").alias("avg_length"))
    write_csv(length_dist.orderBy("length_bucket"), args.output.rstrip("/") + "/text_length_distribution")

    tokens = df_text.select(explode(split(col("eda_text"), r"\s+")).alias("token")).filter(
        (length(col("token")) >= 3) & (~col("token").isin("the", "and", "for", "with", "that", "this", "oracle"))
    )
    write_csv(tokens.groupBy("token").agg(count("*").alias("count")).orderBy(desc("count")).limit(200), args.output.rstrip("/") + "/top_terms")
    write_csv(
        tokens.filter(col("token").rlike(r"^ora-[0-9]{5}$")).groupBy("token").agg(count("*").alias("count")).orderBy(desc("count")).limit(100),
        args.output.rstrip("/") + "/top_ora_codes",
    )

    top_question = df.orderBy(col("score").desc(), col("question_id").asc()).select("question_id", "title", "score").limit(1)
    write_csv(top_question, args.output.rstrip("/") + "/top_question")

    spark.stop()


if __name__ == "__main__":
    main()
