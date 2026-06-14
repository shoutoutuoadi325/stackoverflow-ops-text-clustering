#!/usr/bin/env python3
"""Local title/tag similarity sweep directly from the raw JSON dataset.

This is a no-Spark fallback for demo/evaluation evidence. It intentionally uses
conservative title/tag signals and reports itself as `local_title_tag`, so it
does not replace the Spark MinHashLSH pipeline.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import re
import time
from difflib import SequenceMatcher
from pathlib import Path

from json_to_jsonl import iter_questions


STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "by",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "over",
    "the",
    "to",
    "with",
    "using",
    "use",
    "oracle",
    "database",
    "sql",
}


def normalize_title(title: str) -> str:
    value = title.lower()
    value = value.replace("groupby", "group by").replace("orderby", "order by")
    value = re.sub(r"[^a-z0-9#+.\-/]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def title_tokens(title: str) -> tuple[str, ...]:
    tokens = [token for token in normalize_title(title).split() if len(token) >= 2 and token not in STOPWORDS]
    return tuple(dict.fromkeys(tokens))


def token_signature(tokens: tuple[str, ...]) -> str:
    return " ".join(sorted(tokens)[:8])


def tag_signature(tags: list[str]) -> str:
    useful = [tag for tag in tags if tag not in {"oracle-database", "database"}]
    return " ".join(sorted(useful)[:5])


def jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    if not union:
        return 0.0
    return len(left & right) / len(union)


def similarity(left: dict, right: dict) -> float:
    token_score = jaccard(left["tokens"], right["tokens"])
    tag_score = jaccard(left["tags"], right["tags"])
    sequence_score = SequenceMatcher(None, left["norm_title"], right["norm_title"]).ratio()
    return max(token_score, 0.72 * token_score + 0.18 * sequence_score + 0.10 * tag_score)


def component_stats(pairs: list[tuple[tuple[int, int], float]], threshold: float) -> tuple[int, int, int]:
    parent: dict[int, int] = {}

    def find(value: int) -> int:
        parent.setdefault(value, value)
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    for (left, right), score in pairs:
        if score >= threshold:
            union(left, right)

    clusters: dict[int, set[int]] = {}
    for value in list(parent):
        clusters.setdefault(find(value), set()).add(value)
    multi_doc_sizes = [len(values) for values in clusters.values() if len(values) > 1]
    return len(multi_doc_sizes), sum(multi_doc_sizes), max(multi_doc_sizes) if multi_doc_sizes else 0


def add_bucket(buckets: dict[str, list[int]], key: str, index: int) -> None:
    if key:
        buckets.setdefault(key, []).append(index)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local title/tag similarity threshold sweep.")
    parser.add_argument("--input", type=Path, default=Path("../StackOverFlow_Oracle_Database/oracle_database_questions.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("output/evaluation/local_title_sweep"))
    parser.add_argument("--thresholds", default="0.65,0.70,0.75,0.80,0.85")
    parser.add_argument("--max-bucket-size", type=int, default=200)
    parser.add_argument("--sample-limit", type=int, default=500)
    args = parser.parse_args()

    start = time.time()
    records: list[dict] = []
    buckets: dict[str, list[int]] = {}

    for item in iter_questions(args.input):
        title = str(item.get("title") or "")
        tags = [str(tag) for tag in (item.get("tags") or [])]
        tokens = title_tokens(title)
        record = {
            "doc_id": str(item.get("question_id") or ""),
            "title": title,
            "score": int(item.get("score") or 0),
            "tags": set(tags),
            "tokens": set(tokens),
            "norm_title": normalize_title(title),
        }
        index = len(records)
        records.append(record)
        add_bucket(buckets, f"title:{record['norm_title']}", index)
        add_bucket(buckets, f"tokens:{token_signature(tokens)}", index)
        add_bucket(buckets, f"tags:{tag_signature(tags)}:{token_signature(tokens[:4])}", index)

    pair_scores: dict[tuple[int, int], float] = {}
    skipped_large_buckets = 0
    for indexes in buckets.values():
        unique_indexes = sorted(set(indexes))
        if len(unique_indexes) < 2:
            continue
        if len(unique_indexes) > args.max_bucket_size:
            skipped_large_buckets += 1
            continue
        for left, right in itertools.combinations(unique_indexes, 2):
            score = similarity(records[left], records[right])
            if score >= 0.60:
                key = (left, right)
                if score > pair_scores.get(key, 0.0):
                    pair_scores[key] = score

    thresholds = [float(item.strip()) for item in args.thresholds.split(",") if item.strip()]
    args.output_dir.mkdir(parents=True, exist_ok=True)

    all_pairs = sorted(pair_scores.items(), key=lambda item: (-item[1], records[item[0][0]]["doc_id"], records[item[0][1]]["doc_id"]))
    sample_path = args.output_dir / "similar_pairs_samples.csv"
    with sample_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["src", "dst", "src_title", "dst_title", "src_score", "dst_score", "similarity"],
        )
        writer.writeheader()
        for (left, right), score in all_pairs[: args.sample_limit]:
            writer.writerow(
                {
                    "src": records[left]["doc_id"],
                    "dst": records[right]["doc_id"],
                    "src_title": records[left]["title"],
                    "dst_title": records[right]["title"],
                    "src_score": records[left]["score"],
                    "dst_score": records[right]["score"],
                    "similarity": round(score, 4),
                }
            )

    summary_path = args.output_dir / "parameter_sweep_summary.csv"
    elapsed = round(time.time() - start, 3)
    with summary_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
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
            ],
        )
        writer.writeheader()
        for threshold in thresholds:
            selected = [score for _, score in all_pairs if score >= threshold]
            cluster_count, question_count, max_cluster_size = component_stats(all_pairs, threshold)
            writer.writerow(
                {
                    "run_name": f"local_title_tag_sim{str(threshold).replace('.', '')}",
                    "method": "local_title_tag",
                    "similarity_threshold": threshold,
                    "num_hash_tables": "",
                    "max_bucket_size": args.max_bucket_size,
                    "candidate_pair_count": len(pair_scores),
                    "pair_count": len(selected),
                    "avg_similarity": round(sum(selected) / len(selected), 4) if selected else 0,
                    "multi_doc_cluster_count": cluster_count,
                    "multi_doc_question_count": question_count,
                    "max_cluster_size": max_cluster_size,
                    "avg_doc_similarity": round(sum(selected) / len(selected), 4) if selected else 0,
                    "elapsed_seconds": elapsed,
                }
            )

    print(f"loaded_records={len(records)}")
    print(f"candidate_pairs={len(pair_scores)}")
    print(f"skipped_large_buckets={skipped_large_buckets}")
    print(f"wrote {summary_path}")
    print(f"wrote {sample_path}")


if __name__ == "__main__":
    main()
