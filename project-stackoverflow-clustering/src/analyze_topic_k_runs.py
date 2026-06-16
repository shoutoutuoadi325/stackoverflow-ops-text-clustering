#!/usr/bin/env python3
"""Analyse a topic_clusters parquet directory and emit cluster-size statistics.

Used to compare BisectingKMeans runs at different K (e.g. K=50 vs K=150)
in a single CSV that the PPT can show without further work.

Output schema:
  run_label, k, total_docs, largest_cluster_size, largest_cluster_share,
  top5_share, top10_share, top20_share, median_size, min_size,
  singleton_count, size_le_5_count

Reads parquet via pyarrow; falls back to fastparquet if pyarrow is missing.
"""

from __future__ import annotations

import argparse
import collections
import csv
import glob
from pathlib import Path
from statistics import median


def _read_cluster_ids(parquet_dir: Path) -> list:
    files = sorted(glob.glob(str(parquet_dir / "part-*.parquet")))
    if not files:
        raise SystemExit(f"No part-*.parquet found under {parquet_dir}")
    try:
        import pyarrow.parquet as pq

        out: list = []
        for f in files:
            t = pq.read_table(f, columns=["cluster_id"])
            out.extend(t["cluster_id"].to_pylist())
        return out
    except ImportError:
        try:
            import fastparquet

            out = []
            for f in files:
                pf = fastparquet.ParquetFile(f)
                df = pf.to_pandas(columns=["cluster_id"])
                out.extend(df["cluster_id"].tolist())
            return out
        except ImportError as exc:
            raise SystemExit(
                "Need pyarrow or fastparquet to read parquet. Try `pip install pyarrow`."
            ) from exc


def analyse(parquet_dir: Path, label: str) -> dict:
    ids = _read_cluster_ids(parquet_dir)
    total = len(ids)
    sizes = collections.Counter(ids)
    sorted_sizes = sorted(sizes.values(), reverse=True)
    return {
        "run_label": label,
        "k": len(sizes),
        "total_docs": total,
        "largest_cluster_size": sorted_sizes[0],
        "largest_cluster_share": round(sorted_sizes[0] / total, 4),
        "top5_share": round(sum(sorted_sizes[:5]) / total, 4),
        "top10_share": round(sum(sorted_sizes[:10]) / total, 4),
        "top20_share": round(sum(sorted_sizes[:20]) / total, 4),
        "median_size": int(median(sorted_sizes)),
        "min_size": min(sorted_sizes),
        "singleton_count": sum(1 for s in sorted_sizes if s == 1),
        "size_le_5_count": sum(1 for s in sorted_sizes if s <= 5),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare topic_clusters parquet runs by cluster-size stats.")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="One or more LABEL=PATH pairs, e.g. k50=output/hdfs_output/topic_clusters_k50",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("output/evaluation/topic_k_comparison.csv"),
    )
    args = parser.parse_args()

    rows = []
    for spec in args.inputs:
        if "=" not in spec:
            raise SystemExit(f"Bad input '{spec}', expected LABEL=PATH")
        label, path_str = spec.split("=", 1)
        rows.append(analyse(Path(path_str), label))

    fields = list(rows[0].keys())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as h:
        w = csv.DictWriter(h, fieldnames=fields, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {args.output}")
    print(f"{'label':>6s} {'k':>4s} {'largest':>8s} {'share':>7s} {'top5':>6s} {'top10':>6s} {'top20':>6s}")
    for r in rows:
        print(
            f"{r['run_label']:>6s} {r['k']:>4d} {r['largest_cluster_size']:>8d} "
            f"{r['largest_cluster_share']:>6.2%} {r['top5_share']:>5.2%} "
            f"{r['top10_share']:>5.2%} {r['top20_share']:>5.2%}"
        )


if __name__ == "__main__":
    main()
