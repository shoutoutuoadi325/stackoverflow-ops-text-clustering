#!/usr/bin/env python3
"""Build static demo assets from exported Spark CSV results."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable


def read_csv_dir(path: Path, limit: int | None = None) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    files: Iterable[Path]
    if path.is_dir():
        files = sorted(path.glob("part-*.csv"))
    elif path.exists():
        files = [path]
    else:
        return rows

    for file_path in files:
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                rows.append({k: v for k, v in row.items()})
                if limit is not None and len(rows) >= limit:
                    return rows
    return rows


def to_int(value: str | None, default: int = 0) -> int:
    try:
        return int(float(value or default))
    except ValueError:
        return default


def to_float(value: str | None, default: float = 0.0) -> float:
    try:
        return float(value or default)
    except ValueError:
        return default


def metric_map(rows: list[dict[str, str]]) -> dict[str, int | float | str]:
    metrics: dict[str, int | float | str] = {}
    for row in rows:
        metric = row.get("metric", "")
        value = row.get("value", "")
        if not metric:
            continue
        number = to_float(value)
        metrics[metric] = int(number) if number.is_integer() else number
    return metrics


def read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv_file(path: Path, limit: int | None = None) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append({k: v for k, v in row.items()})
            if limit is not None and len(rows) >= limit:
                break
    return rows


def read_all_pair_samples(output_root: Path, evaluation_root: Path, limit: int = 500) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    sources = [(output_root / "similar_pairs_samples_csv", "spark_yarn_baseline")]
    sources.extend((path, path.parent.name) for path in sorted(evaluation_root.glob("**/similar_pairs_samples_csv")))
    local_sample = evaluation_root / "local_title_sweep" / "similar_pairs_samples.csv"
    if local_sample.exists():
        sources.append((local_sample, "local_title_tag"))

    seen: set[tuple[str, str, str]] = set()
    for source_path, source_name in sources:
        for row in read_csv_dir(source_path):
            key = (str(row.get("src", "")), str(row.get("dst", "")), source_name)
            if key in seen:
                continue
            seen.add(key)
            row = dict(row)
            row["source"] = source_name
            rows.append(row)

    rows.sort(key=lambda row: to_float(row.get("similarity")), reverse=True)
    return rows[:limit]


def choose_recommendation(sweep_rows: list[dict[str, str]], review_metrics: dict) -> dict[str, str | None]:
    if not sweep_rows:
        return {
            "status": "missing_sweep",
            "run_name": None,
            "reason": "尚未导出参数扫描结果；当前仅展示已有高置信样例。",
        }

    precision_by_threshold = {}
    for threshold, values in review_metrics.get("by_threshold", {}).items():
        precision = values.get("precision")
        if precision is not None:
            precision_by_threshold[str(threshold)] = float(precision)

    def coverage(row: dict[str, str]) -> int:
        return to_int(row.get("multi_doc_question_count")) or to_int(row.get("pair_count"))

    if precision_by_threshold:
        eligible = [
            row
            for row in sweep_rows
            if precision_by_threshold.get(str(row.get("similarity_threshold")), 0.0) >= 0.8
        ]
        if eligible:
            best = max(eligible, key=coverage)
            return {
                "status": "reviewed",
                "run_name": best.get("run_name"),
                "reason": "选择人工审核 precision >= 0.80 且覆盖问题数最多的参数组。",
            }

    high_confidence_rows = [row for row in sweep_rows if to_float(row.get("avg_similarity")) >= 0.95]
    candidates = high_confidence_rows or sweep_rows
    best = max(candidates, key=lambda row: (to_int(row.get("multi_doc_question_count")), to_int(row.get("pair_count"))))
    return {
        "status": "unsupervised",
        "run_name": best.get("run_name"),
        "reason": "尚无人工标签，选择平均相似度 >= 0.95 且覆盖问题数最多的真实计算参数组；不声称最优 F1。",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build demo/data.js for the static dashboard.")
    parser.add_argument("--output-root", default="output/hdfs_output")
    parser.add_argument("--validation", default="output/validation/raw_data_validation.json")
    parser.add_argument("--sweep-summary", default="output/evaluation/parameter_sweep_summary.csv")
    parser.add_argument("--review-candidates", default="evaluation/review_candidates.csv")
    parser.add_argument("--review-metrics", default="output/evaluation/review_metrics.json")
    parser.add_argument("--demo-dir", default="demo")
    parser.add_argument("--topic-limit", type=int, default=1000)
    args = parser.parse_args()

    output_root = Path(args.output_root)
    evaluation_root = Path("output/evaluation")
    demo_dir = Path(args.demo_dir)
    demo_dir.mkdir(parents=True, exist_ok=True)

    eda_root = output_root / "eda"
    sweep_summary = read_csv_file(Path(args.sweep_summary))
    review_metrics = read_json(Path(args.review_metrics))
    topic_dedup_rows = read_csv_dir(output_root / "topic_dedup_view_csv", 200)
    ora_summary_rows = read_csv_dir(output_root / "ora_codes" / "ora_code_summary_csv")
    ora_distribution_rows = read_csv_dir(output_root / "ora_codes" / "ora_code_distribution_csv", 30)
    ora_rescued_rows = read_csv_dir(output_root / "ora_showcase" / "ora_rescued_pairs_csv", 80)
    ora_matched_rows = read_csv_dir(output_root / "ora_showcase" / "ora_match_top_pairs_csv", 80)
    data = {
        "dataSources": {
            "sparkOutputRoot": str(output_root),
            "rawValidation": args.validation if Path(args.validation).exists() else None,
            "sweepSummary": args.sweep_summary if Path(args.sweep_summary).exists() else None,
            "reviewCandidates": args.review_candidates if Path(args.review_candidates).exists() else None,
            "reviewMetrics": args.review_metrics if Path(args.review_metrics).exists() else None,
        },
        "rawValidation": read_json(Path(args.validation)),
        "metrics": metric_map(read_csv_dir(eda_root / "eda_summary")),
        "topicMetric": read_csv_dir(output_root / "topic_clusters_metrics_csv")[:1],
        "topTags": read_csv_dir(eda_root / "top_tags", 20),
        "topTerms": read_csv_dir(eda_root / "top_terms", 20),
        "topOraCodes": read_csv_dir(eda_root / "top_ora_codes", 20),
        "scoreDistribution": read_csv_dir(eda_root / "score_distribution", 20),
        "lengthDistribution": read_csv_dir(eda_root / "text_length_distribution", 20),
        "topicSamples": read_csv_dir(output_root / "topic_clusters_samples_csv", args.topic_limit),
        "topicDedupView": topic_dedup_rows,
        "oraSummary": ora_summary_rows[0] if ora_summary_rows else {},
        "oraDistribution": ora_distribution_rows,
        "oraRescuedPairs": ora_rescued_rows,
        "oraMatchedPairs": ora_matched_rows,
        "similarPairs": read_all_pair_samples(output_root, evaluation_root, 500),
        "duplicateClusters": read_csv_dir(output_root / "clusters_samples_csv", 1000),
        "parameterSweep": sweep_summary,
        "reviewCandidates": read_csv_file(Path(args.review_candidates), 300),
        "reviewMetrics": review_metrics,
        "recommendation": choose_recommendation(sweep_summary, review_metrics),
    }

    for row in data["topicSamples"]:
        row["score"] = to_int(row.get("score"))
        row["cluster_id"] = to_int(row.get("cluster_id"))

    for row in data["similarPairs"]:
        row["similarity"] = round(to_float(row.get("similarity")), 4)
        row["src_score"] = to_int(row.get("src_score"))
        row["dst_score"] = to_int(row.get("dst_score"))

    for row in data["duplicateClusters"]:
        row["cluster_size"] = to_int(row.get("cluster_size"))
        row["score"] = to_int(row.get("score"))
        row["avg_similarity"] = round(to_float(row.get("avg_similarity")), 4)

    for row in data["parameterSweep"]:
        for key in [
            "candidate_pair_count",
            "pair_count",
            "multi_doc_cluster_count",
            "multi_doc_question_count",
            "max_cluster_size",
            "elapsed_seconds",
        ]:
            row[key] = to_int(row.get(key))
        for key in ["similarity_threshold", "avg_similarity", "avg_doc_similarity"]:
            row[key] = round(to_float(row.get(key)), 4)

    for row in data["topicDedupView"]:
        row["topic_id"] = to_int(row.get("topic_id"))
        row["topic_size"] = to_int(row.get("topic_size"))
        row["duplicate_cluster_count"] = to_int(row.get("duplicate_cluster_count"))
        row["duplicate_question_count"] = to_int(row.get("duplicate_question_count"))
        row["dedup_ratio"] = round(to_float(row.get("dedup_ratio")), 4)

    for collection in ("oraRescuedPairs", "oraMatchedPairs"):
        for row in data[collection]:
            row["similarity"] = round(to_float(row.get("similarity")), 4)
            row["similarity_raw"] = round(to_float(row.get("similarity_raw")), 4)
            row["src_score"] = to_int(row.get("src_score"))
            row["dst_score"] = to_int(row.get("dst_score"))

    output_path = demo_dir / "data.js"
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    output_path.write_text(f"window.DEMO_DATA = {payload};\n", encoding="utf-8")
    print(f"Wrote {output_path} with {len(data['topicSamples'])} topic rows and {len(data['similarPairs'])} similar pairs.")


if __name__ == "__main__":
    main()
