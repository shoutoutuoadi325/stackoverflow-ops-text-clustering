#!/usr/bin/env python3
"""Summarize Spark local verified Parquet outputs into evaluation CSV files."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds


def write_single_csv(path: Path, fieldnames: list[str], row: dict) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with (path / "part-00000.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)


def write_sample_pairs(path: Path, pairs_table, limit: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    table = pairs_table.sort_by([("similarity", "descending")]).slice(0, limit)
    rows = table.to_pylist()
    fieldnames = ["src", "dst", "src_title", "dst_title", "src_score", "dst_score", "similarity"]
    with (path / "part-00000.csv").open("w", encoding="utf-8", newline="", errors="ignore") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize Spark local verified results.")
    parser.add_argument("--pairs", type=Path, required=True)
    parser.add_argument("--clusters", type=Path, required=True)
    parser.add_argument("--output-run-dir", type=Path, default=Path("output/evaluation/sweep/spark_local_sim075_ht4_b300"))
    parser.add_argument("--similarity-threshold", default="0.75")
    parser.add_argument("--distance-threshold", default="0.25")
    parser.add_argument("--num-hash-tables", default="4")
    parser.add_argument("--max-bucket-size", default="300")
    parser.add_argument("--sample-limit", type=int, default=500)
    args = parser.parse_args()

    pairs_table = ds.dataset(str(args.pairs), format="parquet").to_table()
    clusters_table = ds.dataset(str(args.clusters), format="parquet").to_table(
        columns=["cluster_id", "cluster_size", "doc_id", "avg_similarity"]
    )

    pair_count = pairs_table.num_rows
    avg_similarity = pc.mean(pairs_table["similarity"]).as_py() if pair_count else 0.0
    multi_doc = clusters_table.filter(pc.greater(clusters_table["cluster_size"], 1))
    cluster_ids = set(multi_doc["cluster_id"].to_pylist())
    max_cluster_size = pc.max(multi_doc["cluster_size"]).as_py() if multi_doc.num_rows else 0
    avg_doc_similarity = pc.mean(multi_doc["avg_similarity"]).as_py() if multi_doc.num_rows else 0.0

    write_single_csv(
        args.output_run_dir / "lsh_metrics_csv",
        [
            "similarity_threshold",
            "distance_threshold",
            "num_hash_tables",
            "max_bucket_size",
            "top_n_per_doc",
            "min_token_count",
            "input_count",
            "kept_bucket_count",
            "candidate_pair_count",
            "pre_topn_pair_count",
            "pair_count",
            "avg_similarity",
        ],
        {
            "similarity_threshold": args.similarity_threshold,
            "distance_threshold": args.distance_threshold,
            "num_hash_tables": args.num_hash_tables,
            "max_bucket_size": args.max_bucket_size,
            "top_n_per_doc": "10",
            "min_token_count": "2",
            "input_count": "",
            "kept_bucket_count": "",
            "candidate_pair_count": "",
            "pre_topn_pair_count": "",
            "pair_count": pair_count,
            "avg_similarity": round(float(avg_similarity or 0.0), 6),
        },
    )
    write_single_csv(
        args.output_run_dir / "cluster_metrics_csv",
        ["edge_count", "multi_doc_cluster_count", "multi_doc_question_count", "max_cluster_size", "avg_doc_similarity"],
        {
            "edge_count": pair_count,
            "multi_doc_cluster_count": len(cluster_ids),
            "multi_doc_question_count": multi_doc.num_rows,
            "max_cluster_size": int(max_cluster_size or 0),
            "avg_doc_similarity": round(float(avg_doc_similarity or 0.0), 6),
        },
    )
    write_sample_pairs(args.output_run_dir / "similar_pairs_samples_csv", pairs_table, args.sample_limit)
    print(f"Wrote Spark verified summary to {args.output_run_dir}")


if __name__ == "__main__":
    main()
