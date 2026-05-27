#!/usr/bin/env python3
"""Create token, MinHash, and TF-IDF features."""

from __future__ import annotations

import argparse

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Normalizer, RegexTokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size


DOMAIN_STOPWORDS = [
    "oracle",
    "database",
    "oracle-database",
    "question",
    "answer",
    "thanks",
    "please",
    "help",
    "using",
    "use",
    "want",
    "need",
    "problem",
    "issue",
    "error",
    "codeblock",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Spark ML text features.")
    parser.add_argument("--input", default="hdfs:///user/bigdata/stackoverflow/parquet/questions")
    parser.add_argument("--output", default="hdfs:///user/bigdata/stackoverflow/parquet/features")
    parser.add_argument("--num-features", type=int, default=1 << 18)
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("stackoverflow-feature-engineering").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    df = spark.read.parquet(args.input).filter(col("clean_text").isNotNull())
    tokenizer = RegexTokenizer(
        inputCol="clean_text",
        outputCol="raw_tokens",
        pattern=r"[^a-z0-9_#.+/-]+",
        gaps=True,
        minTokenLength=2,
    )
    remover = StopWordsRemover(
        inputCol="raw_tokens",
        outputCol="tokens",
        stopWords=StopWordsRemover.loadDefaultStopWords("english") + DOMAIN_STOPWORDS,
    )
    binary_tf = HashingTF(
        inputCol="tokens",
        outputCol="term_features",
        numFeatures=args.num_features,
        binary=True,
    )
    tf = HashingTF(
        inputCol="tokens",
        outputCol="tf_features",
        numFeatures=args.num_features,
        binary=False,
    )
    idf = IDF(inputCol="tf_features", outputCol="tfidf_features", minDocFreq=2)
    normalizer = Normalizer(inputCol="tfidf_features", outputCol="features", p=2.0)

    model = Pipeline(stages=[tokenizer, remover, binary_tf, tf, idf, normalizer]).fit(df)
    features = model.transform(df).filter(size(col("tokens")) >= 2)
    features.select(
        "doc_id",
        "title",
        "tags",
        "score",
        "answer_count",
        "comment_count",
        "clean_text",
        "tokens",
        "term_features",
        "features",
    ).write.mode("overwrite").parquet(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
