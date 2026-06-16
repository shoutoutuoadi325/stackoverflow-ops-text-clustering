#!/usr/bin/env python3
"""Compute human-review precision from review_candidates.csv.

We compute precision only over rows that the human has explicitly labelled.
Unlabelled rows are reported as `pending`. Recall is undefined here because
there is no known full-corpus duplicate set, so F1 is intentionally not
emitted -- the report explains why.

In addition to the global precision we emit two breakdowns to support the
defense narrative:

  - by_threshold: precision grouped by the LSH similarity threshold the
    candidate came from (0.65 / 0.75 / 0.85 / local). Lets us claim
    "higher threshold => higher precision" with concrete numbers.
  - by_similarity_bucket: precision grouped by raw similarity bucket
    [0.0,0.65) / [0.65,0.75) / [0.75,0.85) / [0.85,1.0]. Same idea but
    method-agnostic.
  - by_ora_match: precision split by whether the pair shares an ORA code
    (only meaningful when the review_candidates.csv carries a
    `shared_ora_codes` column from the updated cluster_lsh.py).

For every bucket we report a Wilson 95% confidence interval on precision so
the slides do not over-claim from tiny samples.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Iterable


TRUE_LABELS = {"1", "true", "yes", "y", "是", "相似", "duplicate"}
FALSE_LABELS = {"0", "false", "no", "n", "否", "不相似", "not_duplicate"}


def normalize_label(value: str) -> bool | None:
    label = value.strip().lower()
    if label in TRUE_LABELS:
        return True
    if label in FALSE_LABELS:
        return False
    return None


def wilson_interval(positive: int, total: int, z: float = 1.96) -> tuple[float | None, float | None]:
    if total == 0:
        return None, None
    p = positive / total
    denom = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denom
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denom
    return round(max(0.0, centre - margin), 4), round(min(1.0, centre + margin), 4)


def similarity_bucket(value: str) -> str:
    try:
        sim = float(value)
    except (TypeError, ValueError):
        return "unknown"
    if sim < 0.65:
        return "[0.00,0.65)"
    if sim < 0.75:
        return "[0.65,0.75)"
    if sim < 0.85:
        return "[0.75,0.85)"
    return "[0.85,1.00]"


def summarize(buckets: dict[str, dict[str, int]]) -> dict[str, dict]:
    out = {}
    for key, values in sorted(buckets.items()):
        labeled = values["labeled"]
        positive = values["positive"]
        precision = round(positive / labeled, 4) if labeled else None
        ci_low, ci_high = wilson_interval(positive, labeled)
        out[key] = {
            "labeled_count": labeled,
            "positive_count": positive,
            "precision": precision,
            "ci95_low": ci_low,
            "ci95_high": ci_high,
        }
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate review_candidates.csv labels.")
    parser.add_argument("--input", type=Path, default=Path("evaluation/review_candidates.csv"))
    parser.add_argument("--output", type=Path, default=Path("output/evaluation/review_metrics.json"))
    args = parser.parse_args()

    args.output.parent.mkdir(parents=True, exist_ok=True)
    if not args.input.exists():
        metrics = {"status": "missing_review_file", "labeled_count": 0, "precision": None, "f1": None}
        args.output.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Wrote {args.output}")
        return

    labeled = 0
    positive = 0
    by_threshold: dict[str, dict[str, int]] = {}
    by_similarity: dict[str, dict[str, int]] = {}
    by_ora: dict[str, dict[str, int]] = {}
    with args.input.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        has_ora_column = "shared_ora_codes" in (reader.fieldnames or [])
        for row in reader:
            parsed = normalize_label(row.get("label", ""))
            if parsed is None:
                continue
            labeled += 1
            positive += int(parsed)

            threshold = row.get("threshold", "unknown") or "unknown"
            bucket = by_threshold.setdefault(threshold, {"labeled": 0, "positive": 0})
            bucket["labeled"] += 1
            bucket["positive"] += int(parsed)

            sim_bucket = similarity_bucket(row.get("similarity", ""))
            sim_b = by_similarity.setdefault(sim_bucket, {"labeled": 0, "positive": 0})
            sim_b["labeled"] += 1
            sim_b["positive"] += int(parsed)

            if has_ora_column:
                shared = (row.get("shared_ora_codes") or "").strip()
                key = "ora_match" if shared else "no_ora_match"
                ora_b = by_ora.setdefault(key, {"labeled": 0, "positive": 0})
                ora_b["labeled"] += 1
                ora_b["positive"] += int(parsed)

    overall_precision = round(positive / labeled, 4) if labeled else None
    overall_low, overall_high = wilson_interval(positive, labeled)

    metrics = {
        "status": "labeled" if labeled else "pending_labels",
        "labeled_count": labeled,
        "positive_count": positive,
        "precision": overall_precision,
        "ci95_low": overall_low,
        "ci95_high": overall_high,
        "f1": None,
        "note": (
            "Recall has no ground-truth denominator over 152K questions, so we deliberately do not "
            "compute F1. precision is reported with a Wilson 95% confidence interval; with small "
            "samples the interval is wide and that width is part of the honest evaluation story."
        ),
        "by_threshold": summarize(by_threshold),
        "by_similarity_bucket": summarize(by_similarity),
        "by_ora_match": summarize(by_ora),
    }
    args.output.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {args.output} (labeled={labeled}, precision={overall_precision})")


if __name__ == "__main__":
    main()
