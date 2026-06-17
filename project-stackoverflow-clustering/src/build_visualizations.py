#!/usr/bin/env python3
"""Build PPT-ready PNG charts from intra-topic experiment outputs.

Reads CSV summaries produced by:
  - src/analyze_topic_k_runs.py        -> output/evaluation/topic_k_comparison.csv
  - src/summarize_intra_topic_runs.py  -> output/evaluation/intra_topic_comparison.csv
  - similar_pairs_samples_csv (per K)  -> for similarity histogram
  - topic_metrics_csv (per K)          -> for per-topic timing

Writes:
  output/visualizations/
    fig1_k_value_max_cluster_share.png
    fig2_k_value_topN_share.png
    fig3_duplicate_groups_vs_k.png
    fig4_v1_vs_v2_comparison.png
    fig5_similarity_distribution.png
    fig6_topic_timing_skew.png

Run after both summarize scripts have produced their CSVs. Each chart has
sensible defaults but no hard-coded numbers, so it tolerates partial K runs.
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


PROJECT_GREEN = "#246b57"
PROJECT_BLUE = "#315f8c"
PROJECT_AMBER = "#a76223"
PROJECT_RED = "#9b3d36"
PALETTE = [PROJECT_GREEN, PROJECT_BLUE, PROJECT_AMBER, PROJECT_RED]


def _read_csv_anywhere(path: Path) -> list[dict[str, str]]:
    """Read either a flat CSV or a Spark CSV directory of part-*.csv files."""
    rows: list[dict[str, str]] = []
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


def _to_float(v: str | None) -> float:
    try:
        return float(v) if v not in (None, "") else 0.0
    except ValueError:
        return 0.0


def _to_int(v: str | None) -> int:
    try:
        return int(float(v)) if v not in (None, "") else 0
    except ValueError:
        return 0


def _label_to_k(label: str) -> int:
    """k50 -> 50, k150 -> 150."""
    s = label.strip().lstrip("k")
    return int(s) if s.isdigit() else 0


def _save(fig, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"Wrote {path}")


# ---------------------------------------------------------------------------
# Figure 1: K vs largest-cluster share (and Top-5/10/20)
# ---------------------------------------------------------------------------


def fig_k_max_cluster_share(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    rows = sorted(rows, key=lambda r: _label_to_k(r.get("run_label", "")))
    ks = [str(_to_int(r.get("k"))) for r in rows]
    shares = [_to_float(r.get("largest_cluster_share")) * 100 for r in rows]

    fig, ax = plt.subplots(figsize=(7, 4.5))
    bars = ax.bar(ks, shares, color=PROJECT_BLUE, edgecolor="white")
    for bar, val in zip(bars, shares):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.4,
            f"{val:.1f}%",
            ha="center",
            va="bottom",
            fontsize=11,
            color="#16201d",
        )
    ax.set_xlabel("K (BisectingKMeans 主题数)", fontsize=11)
    ax.set_ylabel("最大簇占比 (%)", fontsize=11)
    ax.set_title("K 值实验:最大簇占比随 K 增大的变化", fontsize=12, weight="bold")
    ax.set_ylim(0, max(shares) * 1.18)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    _save(fig, out)


def fig_k_top_share(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    rows = sorted(rows, key=lambda r: _label_to_k(r.get("run_label", "")))
    ks = [str(_to_int(r.get("k"))) for r in rows]
    top5 = [_to_float(r.get("top5_share")) * 100 for r in rows]
    top10 = [_to_float(r.get("top10_share")) * 100 for r in rows]
    top20 = [_to_float(r.get("top20_share")) * 100 for r in rows]

    x = range(len(ks))
    width = 0.26
    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.bar([i - width for i in x], top5, width, label="Top 5", color=PROJECT_GREEN)
    ax.bar(list(x), top10, width, label="Top 10", color=PROJECT_BLUE)
    ax.bar([i + width for i in x], top20, width, label="Top 20", color=PROJECT_AMBER)
    ax.set_xticks(list(x))
    ax.set_xticklabels(ks)
    ax.set_xlabel("K", fontsize=11)
    ax.set_ylabel("Top N 簇覆盖文档占比 (%)", fontsize=11)
    ax.set_title("K 值实验:头部簇集中度", fontsize=12, weight="bold")
    ax.legend(frameon=False, fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    _save(fig, out)


# ---------------------------------------------------------------------------
# Figure 2: duplicate groups vs K (the headline number)
# ---------------------------------------------------------------------------


def fig_duplicate_groups_vs_k(rows: list[dict], out: Path) -> None:
    if not rows:
        return
    rows = sorted(rows, key=lambda r: _label_to_k(r.get("run_label", "")))
    ks = [_label_to_k(r.get("run_label", "")) for r in rows]
    groups = [_to_int(r.get("multi_doc_cluster_count")) for r in rows]
    questions = [_to_int(r.get("multi_doc_question_count")) for r in rows]

    fig, ax = plt.subplots(figsize=(8, 4.8))
    ax.plot(ks, groups, marker="o", linewidth=2.5, color=PROJECT_GREEN, label="重复问题组数")
    ax.plot(ks, questions, marker="s", linewidth=2.5, color=PROJECT_BLUE, label="涉及问题总数")
    for k, g in zip(ks, groups):
        ax.annotate(str(g), (k, g), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=10, color=PROJECT_GREEN)
    for k, q in zip(ks, questions):
        ax.annotate(str(q), (k, q), textcoords="offset points", xytext=(0, -16), ha="center", fontsize=10, color=PROJECT_BLUE)
    ax.set_xlabel("K (主题数)", fontsize=11)
    ax.set_ylabel("数量", fontsize=11)
    ax.set_title("v2 簇内 LSH:重复组数随 K 增大的变化", fontsize=12, weight="bold")
    ax.set_xticks(ks)
    ax.legend(frameon=False, fontsize=10, loc="upper left")
    ax.grid(linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    _save(fig, out)


# ---------------------------------------------------------------------------
# Figure 3: v1 vs v2 horizontal bar
# ---------------------------------------------------------------------------


def fig_v1_v2_comparison(intra_rows: list[dict], out: Path) -> None:
    series = [("v1 全局 LSH (sim>=0.85)", 2, "#9b3d36")]
    for r in sorted(intra_rows, key=lambda x: _label_to_k(x.get("run_label", ""))):
        label = r.get("run_label", "")
        groups = _to_int(r.get("multi_doc_cluster_count"))
        if groups <= 0:
            continue
        series.append((f"v2 K={_label_to_k(label)} 簇内 LSH (sim>=0.65)", groups, PROJECT_GREEN))
    if len(series) < 2:
        return

    labels = [s[0] for s in series]
    vals = [s[1] for s in series]
    colors = [s[2] for s in series]

    fig, ax = plt.subplots(figsize=(9, 0.7 + 0.55 * len(series)))
    bars = ax.barh(labels[::-1], vals[::-1], color=colors[::-1], edgecolor="white")
    for bar, val in zip(bars, vals[::-1]):
        ax.text(
            bar.get_width() + max(vals) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            str(val),
            va="center",
            fontsize=11,
            color="#16201d",
        )
    ax.set_xlabel("找到的重复问题组数", fontsize=11)
    ax.set_title("v1 → v2 改进效果对比", fontsize=12, weight="bold")
    ax.set_xlim(0, max(vals) * 1.15)
    ax.grid(axis="x", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    _save(fig, out)


# ---------------------------------------------------------------------------
# Figure 4: similarity distribution histogram (from any one K's pairs)
# ---------------------------------------------------------------------------


def fig_similarity_distribution(intra_root: Path, out: Path) -> None:
    chosen = None
    for label in ("k150", "k200", "k100", "k50"):
        sim_dir = intra_root / label / "similar_pairs_samples_csv"
        rows = _read_csv_anywhere(sim_dir)
        if rows:
            chosen = (label, rows)
            break
    if not chosen:
        return
    label, rows = chosen
    sims = [_to_float(r.get("similarity")) for r in rows if r.get("similarity")]
    if not sims:
        return

    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    ax.hist(sims, bins=20, color=PROJECT_BLUE, edgecolor="white")
    ax.axvline(0.65, color=PROJECT_RED, linestyle="--", linewidth=1.2, label="阈值 0.65")
    ax.set_xlabel("相似度 (Jaccard)", fontsize=11)
    ax.set_ylabel("相似对数", fontsize=11)
    ax.set_title(f"相似对相似度分布 ({label.upper()})", fontsize=12, weight="bold")
    ax.legend(frameon=False, fontsize=10)
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    _save(fig, out)


# ---------------------------------------------------------------------------
# Figure 5: per-topic timing (data skew evidence)
# ---------------------------------------------------------------------------


def fig_topic_timing(intra_root: Path, out: Path) -> None:
    target = None
    for label in ("k150", "k200", "k100", "k50"):
        d = intra_root / label / "topic_metrics_csv"
        rows = _read_csv_anywhere(d)
        if rows:
            target = (label, rows)
            break
    if not target:
        return
    label, rows = target
    rows = [r for r in rows if r.get("status") == "ok"]
    rows.sort(key=lambda r: _to_float(r.get("elapsed_seconds")), reverse=True)
    rows = rows[:20]
    if not rows:
        return

    topics = [str(_to_int(r.get("topic_id"))) for r in rows]
    secs = [_to_float(r.get("elapsed_seconds")) for r in rows]
    sizes = [_to_int(r.get("doc_count")) for r in rows]

    fig, ax = plt.subplots(figsize=(9, 4.8))
    bars = ax.bar(range(len(topics)), secs, color=PROJECT_AMBER, edgecolor="white")
    ax.set_xticks(range(len(topics)))
    ax.set_xticklabels(topics, rotation=0, fontsize=9)
    ax.set_xlabel(f"topic_id (按耗时降序, {label.upper()})", fontsize=11)
    ax.set_ylabel("簇内 LSH 耗时 (秒)", fontsize=11)
    ax.set_title("簇内 LSH 耗时分布:大簇拖累整体", fontsize=12, weight="bold")
    for bar, sz in zip(bars, sizes):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(secs) * 0.01,
            f"{sz}",
            ha="center",
            va="bottom",
            fontsize=8,
            color="#65716c",
        )
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.set_axisbelow(True)
    _save(fig, out)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build PPT-ready charts.")
    parser.add_argument(
        "--topic-k-csv",
        type=Path,
        default=Path("output/evaluation/topic_k_comparison.csv"),
    )
    parser.add_argument(
        "--intra-csv",
        type=Path,
        default=Path("output/evaluation/intra_topic_comparison.csv"),
    )
    parser.add_argument(
        "--intra-root",
        type=Path,
        default=Path("output/hdfs_output/intra_topic"),
        help="Local mirror of hdfs:/.../output/intra_topic with k50/k100/k150/k200 subdirs.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("output/visualizations"),
    )
    args = parser.parse_args()

    topic_rows = _read_csv_anywhere(args.topic_k_csv)
    intra_rows = _read_csv_anywhere(args.intra_csv)

    fig_k_max_cluster_share(topic_rows, args.out_dir / "fig1_k_value_max_cluster_share.png")
    fig_k_top_share(topic_rows, args.out_dir / "fig2_k_value_topN_share.png")
    fig_duplicate_groups_vs_k(intra_rows, args.out_dir / "fig3_duplicate_groups_vs_k.png")
    fig_v1_v2_comparison(intra_rows, args.out_dir / "fig4_v1_vs_v2_comparison.png")
    fig_similarity_distribution(args.intra_root, args.out_dir / "fig5_similarity_distribution.png")
    fig_topic_timing(args.intra_root, args.out_dir / "fig6_topic_timing_skew.png")

    print(f"\nDone. Open {args.out_dir} to insert PNGs into the slides.")


if __name__ == "__main__":
    main()
