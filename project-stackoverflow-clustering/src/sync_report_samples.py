#!/usr/bin/env python3
"""Copy the best available Spark sample CSVs to stable top-level report paths."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


DEFAULT_PAIR_SOURCES = [
    "output/evaluation/sweep/yarn_sim065_ht2_bucket200/similar_pairs_samples_csv",
    "output/evaluation/sweep/spark_local_sim075_ht4_b300/similar_pairs_samples_csv",
    "output/hdfs_output/similar_pairs_samples_csv",
]

DEFAULT_CLUSTER_SOURCES = [
    "output/evaluation/sweep/yarn_sim065_ht2_bucket200/clusters_samples_csv",
    "output/evaluation/sweep/spark_local_sim075_ht4_b300/clusters_samples_csv",
    "output/hdfs_output/clusters_samples_csv",
]


def read_csv_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    files = sorted(path.glob("part-*.csv")) if path.is_dir() else ([path] if path.exists() else [])
    for file_path in files:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, escapechar="\\")
            rows = [dict(row) for row in reader]
            if reader.fieldnames and rows:
                return list(reader.fieldnames), rows
    return [], []


def first_non_empty(root: Path, candidates: list[str]) -> tuple[Path | None, list[str], list[dict[str, str]]]:
    for candidate in candidates:
        path = root / candidate
        fieldnames, rows = read_csv_rows(path)
        if rows:
            return path, fieldnames, rows
    return None, [], []


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]], limit: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows[:limit])


def sync_one(root: Path, candidates: list[str], output: Path, limit: int) -> None:
    source, fieldnames, rows = first_non_empty(root, candidates)
    if not source:
        print(f"No non-empty sample source found for {output}")
        return
    write_rows(root / output, fieldnames, rows, limit)
    print(f"Wrote {output} from {source.relative_to(root)} with {min(len(rows), limit)} rows.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Populate output/cluster_samples.csv and output/similar_pair_samples.csv.")
    parser.add_argument("--project-root", type=Path, default=Path("."))
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--pair-output", type=Path, default=Path("output/similar_pair_samples.csv"))
    parser.add_argument("--cluster-output", type=Path, default=Path("output/cluster_samples.csv"))
    args = parser.parse_args()

    root = args.project_root.resolve()
    sync_one(root, DEFAULT_PAIR_SOURCES, args.pair_output, args.limit)
    sync_one(root, DEFAULT_CLUSTER_SOURCES, args.cluster_output, args.limit)


if __name__ == "__main__":
    main()
