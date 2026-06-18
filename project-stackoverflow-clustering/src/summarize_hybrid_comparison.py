#!/usr/bin/env python3
"""Combine hybrid rescue summaries into one evaluation CSV for PPT."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable


def read_csv_dir(path: Path) -> list[dict]:
    if not path.exists():
        return []
    files: Iterable[Path] = sorted(path.glob("part-*.csv")) if path.is_dir() else [path]
    rows: list[dict] = []
    for f in files:
        with f.open("r", encoding="utf-8", newline="") as h:
            rows.extend(dict(r) for r in csv.DictReader(h))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize domain hybrid rescue runs.")
    parser.add_argument(
        "--input-root",
        type=Path,
        default=Path("output/hdfs_output/intra_topic"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/evaluation/hybrid_comparison.csv"),
    )
    args = parser.parse_args()

    rows: list[dict] = []
    compare_rows: list[dict] = []

    if not args.input_root.exists():
        raise SystemExit(f"{args.input_root} does not exist.")

    for label_dir in sorted(p for p in args.input_root.iterdir() if p.is_dir()):
        hybrid = label_dir / "hybrid"
        summary = read_csv_dir(hybrid / "hybrid_summary_csv")
        compare = read_csv_dir(hybrid / "baseline_vs_hybrid_csv")
        for r in summary:
            r.setdefault("run_label", label_dir.name)
            rows.append(r)
        compare_rows.extend(compare)

    if not rows:
        raise SystemExit("No hybrid_summary_csv found. Run scripts/15_submit_domain_rescue.sh first.")

    fields = [
        "run_label",
        "baseline_pair_count",
        "rescued_pair_count",
        "hybrid_pair_count",
        "baseline_multi_doc_cluster_count",
        "hybrid_multi_doc_cluster_count",
        "baseline_multi_doc_question_count",
        "hybrid_multi_doc_question_count",
        "max_cluster_size",
        "avg_similarity",
    ]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as h:
        w = csv.DictWriter(h, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

    compare_path = args.output.with_name("hybrid_baseline_compare.csv")
    compare_fields = [
        "run_label",
        "method",
        "pair_count",
        "multi_doc_cluster_count",
        "multi_doc_question_count",
    ]
    with compare_path.open("w", encoding="utf-8", newline="") as h:
        w = csv.DictWriter(h, fieldnames=compare_fields, lineterminator="\n")
        w.writeheader()
        for r in compare_rows:
            w.writerow({k: r.get(k, "") for k in compare_fields})

    print(f"Wrote {args.output} ({len(rows)} rows)")
    print(f"Wrote {compare_path} ({len(compare_rows)} rows)")
    for r in rows:
        print(
            f"  {r.get('run_label'):>6s}: rescued={r.get('rescued_pair_count'):>4}  "
            f"groups {r.get('baseline_multi_doc_cluster_count')} -> {r.get('hybrid_multi_doc_cluster_count')}"
        )


if __name__ == "__main__":
    main()
