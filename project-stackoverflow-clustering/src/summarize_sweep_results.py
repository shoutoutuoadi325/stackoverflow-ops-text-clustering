#!/usr/bin/env python3
"""Summarize exported parameter-sweep metrics."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


FIELDNAMES = [
    "run_name",
    "method",
    "similarity_threshold",
    "num_hash_tables",
    "max_bucket_size",
    "candidate_pair_count",
    "pair_count",
    "avg_similarity",
    "multi_doc_cluster_count",
    "multi_doc_question_count",
    "max_cluster_size",
    "avg_doc_similarity",
    "elapsed_seconds",
]


def read_first_csv(path: Path) -> dict[str, str]:
    files = sorted(path.glob("part-*.csv")) if path.is_dir() else []
    for file_path in files:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
            if rows:
                return rows[0]
    return {}


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build parameter_sweep_summary.csv.")
    parser.add_argument("--input-root", type=Path, default=Path("output/evaluation/sweep"))
    parser.add_argument("--local-summary", type=Path, default=Path("output/evaluation/local_title_sweep/parameter_sweep_summary.csv"))
    parser.add_argument("--output", type=Path, default=Path("output/evaluation/parameter_sweep_summary.csv"))
    args = parser.parse_args()

    rows: list[dict[str, str]] = []
    if args.input_root.exists():
        for run_dir in sorted(path for path in args.input_root.iterdir() if path.is_dir()):
            lsh = read_first_csv(run_dir / "lsh_metrics_csv")
            clusters = read_first_csv(run_dir / "cluster_metrics_csv")
            runtime = read_first_csv(run_dir / "runtime_csv")
            if not lsh and not clusters:
                continue
            rows.append(
                {
                    "run_name": run_dir.name,
                    "method": "spark_minhash_lsh",
                    "similarity_threshold": lsh.get("similarity_threshold", ""),
                    "num_hash_tables": lsh.get("num_hash_tables", ""),
                    "max_bucket_size": lsh.get("max_bucket_size", ""),
                    "candidate_pair_count": lsh.get("candidate_pair_count", ""),
                    "pair_count": lsh.get("pair_count", ""),
                    "avg_similarity": lsh.get("avg_similarity", ""),
                    "multi_doc_cluster_count": clusters.get("multi_doc_cluster_count", ""),
                    "multi_doc_question_count": clusters.get("multi_doc_question_count", ""),
                    "max_cluster_size": clusters.get("max_cluster_size", ""),
                    "avg_doc_similarity": clusters.get("avg_doc_similarity", ""),
                    "elapsed_seconds": runtime.get("elapsed_seconds", ""),
                }
            )

    for row in read_rows(args.local_summary):
        rows.append({field: row.get(field, "") for field in FIELDNAMES})

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {args.output} with {len(rows)} runs.")


if __name__ == "__main__":
    main()
