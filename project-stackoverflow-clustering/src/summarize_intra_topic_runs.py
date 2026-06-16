#!/usr/bin/env python3
"""Aggregate intra-topic LSH runs into a single PPT-ready comparison table.

Reads the `summary_csv` that each run of cluster_lsh_intra_topic.py writes
(one CSV per --label, e.g. k50 / k150) and emits a flat combined table:

  output/evaluation/intra_topic_comparison.csv

Schema:
  run_label, num_hash_tables, similarity_threshold,
  pair_count, multi_doc_cluster_count, multi_doc_question_count,
  max_cluster_size, avg_similarity

This file is the single source of truth for the "K=50 vs K=150 duplicate
groups" slide.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


def read_csv_dir(path: Path) -> list[dict]:
    rows: list[dict] = []
    files: Iterable[Path]
    if path.is_dir():
        files = sorted(path.glob("part-*.csv"))
    elif path.exists():
        files = [path]
    else:
        return rows
    for f in files:
        with f.open("r", encoding="utf-8", newline="") as h:
            rows.extend(dict(r) for r in csv.DictReader(h))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Combine intra-topic run summaries.")
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("output/hdfs_output/intra_topic"),
        help="Local mirror of hdfs:///user/bigdata/stackoverflow/output/intra_topic.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/evaluation/intra_topic_comparison.csv"),
    )
    args = parser.parse_args()

    if not args.input_root.exists():
        raise SystemExit(
            f"{args.input_root} does not exist. Run scripts/14_submit_cluster_intra_topic.sh "
            "and `hdfs dfs -get` the intra_topic directory before running this script."
        )

    rows: list[dict] = []
    for label_dir in sorted(p for p in args.input_root.iterdir() if p.is_dir()):
        summary = read_csv_dir(label_dir / "summary_csv")
        for r in summary:
            r.setdefault("run_label", label_dir.name)
            rows.append(r)

    if not rows:
        raise SystemExit("No summary_csv parts found under any label directory.")

    fields = [
        "run_label",
        "num_hash_tables",
        "similarity_threshold",
        "distance_threshold",
        "pair_count",
        "multi_doc_cluster_count",
        "multi_doc_question_count",
        "max_cluster_size",
        "avg_similarity",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as h:
        w = csv.DictWriter(h, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

    print(f"Wrote {args.output} with {len(rows)} rows.")
    for r in rows:
        print(
            f"  {r.get('run_label'):>6s}: pairs={r.get('pair_count'):>5}  "
            f"groups={r.get('multi_doc_cluster_count'):>5}  "
            f"questions={r.get('multi_doc_question_count'):>5}  "
            f"avg_sim={r.get('avg_similarity')}"
        )


if __name__ == "__main__":
    main()
