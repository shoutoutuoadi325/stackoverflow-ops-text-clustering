#!/usr/bin/env python3
"""Create a human-review CSV from available similar-pair samples."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


FIELDNAMES = [
    "src",
    "dst",
    "src_title",
    "dst_title",
    "similarity",
    "threshold",
    "hash_tables",
    "shared_ora_codes",
    "ora_match",
    "label",
    "notes",
]


def read_rows(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    files = sorted(path.glob("part-*.csv")) if path.is_dir() else ([path] if path.exists() else [])
    for file_path in files:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            rows.extend(dict(row) for row in csv.DictReader(handle, escapechar="\\"))
    return rows


def infer_params(path: Path) -> tuple[str, str]:
    name = "_".join(path.parts)
    threshold = ""
    hash_tables = ""
    for part in name.replace("-", "_").split("_"):
        if part.startswith("sim") and part.removeprefix("sim")[:1].isdigit():
            threshold = part.removeprefix("sim")
            if threshold.startswith("0") and len(threshold) > 1:
                threshold = f"0.{threshold[1:]}"
            elif threshold and threshold.startswith("0") is False:
                threshold = f"0.{threshold}"
        if part.startswith("ht") and part.removeprefix("ht").isdigit():
            hash_tables = part.removeprefix("ht")
    if not threshold and "yarn_historical_baseline" in name:
        threshold = "0.75"
    if not hash_tables and "yarn_historical_baseline" in name:
        hash_tables = "4"
    return threshold, hash_tables


def read_existing_labels(path: Path) -> dict[tuple[str, str, str, str], dict[str, str]]:
    labels: dict[tuple[str, str, str, str], dict[str, str]] = {}
    if not path.exists():
        return labels
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle, escapechar="\\"):
            key = (row.get("src", ""), row.get("dst", ""), row.get("threshold", ""), row.get("hash_tables", ""))
            if row.get("label", "") or row.get("notes", ""):
                labels[key] = {
                    "label": row.get("label", ""),
                    "notes": row.get("notes", ""),
                }
    return labels


def main() -> None:
    parser = argparse.ArgumentParser(description="Build evaluation/review_candidates.csv.")
    parser.add_argument("--input-root", type=Path, default=Path("output/evaluation"))
    parser.add_argument("--baseline", type=Path, default=Path("output/hdfs_output/similar_pairs_samples_csv"))
    parser.add_argument("--local-samples", type=Path, default=Path("output/evaluation/local_title_sweep/similar_pairs_samples.csv"))
    parser.add_argument("--output", type=Path, default=Path("evaluation/review_candidates.csv"))
    parser.add_argument("--limit-per-source", type=int, default=50)
    args = parser.parse_args()

    sources = []
    if args.baseline.exists():
        sources.append(("0.85", "4", args.baseline))
    if args.local_samples.exists():
        sources.append(("local", "", args.local_samples))
    if args.input_root.exists():
        sources.extend(("", "", path) for path in sorted(args.input_root.glob("**/similar_pairs_samples_csv")))

    seen: set[tuple[str, str, str, str]] = set()
    existing_labels = read_existing_labels(args.output)
    out_rows: list[dict[str, str]] = []
    for default_threshold, default_hash_tables, source in sources:
        threshold, hash_tables = infer_params(source)
        threshold = threshold or default_threshold
        hash_tables = hash_tables or default_hash_tables
        rows = sorted(read_rows(source), key=lambda row: float(row.get("similarity") or 0), reverse=True)
        for row in rows[: args.limit_per_source]:
            key = (row.get("src", ""), row.get("dst", ""), threshold, hash_tables)
            if key in seen:
                continue
            seen.add(key)
            preserved = existing_labels.get(key, {})
            out_rows.append(
                {
                    "src": row.get("src", ""),
                    "dst": row.get("dst", ""),
                    "src_title": row.get("src_title", ""),
                    "dst_title": row.get("dst_title", ""),
                    "similarity": row.get("similarity", ""),
                    "threshold": threshold,
                    "hash_tables": hash_tables,
                    "shared_ora_codes": row.get("shared_ora_codes", ""),
                    "ora_match": row.get("ora_match", ""),
                    "label": preserved.get("label", ""),
                    "notes": preserved.get("notes", ""),
                }
            )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(out_rows)
    print(f"Wrote {args.output} with {len(out_rows)} rows.")


if __name__ == "__main__":
    main()
