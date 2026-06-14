#!/usr/bin/env python3
"""Compute human-review precision only from explicitly labeled rows."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


TRUE_LABELS = {"1", "true", "yes", "y", "是", "相似", "duplicate"}
FALSE_LABELS = {"0", "false", "no", "n", "否", "不相似", "not_duplicate"}


def normalize_label(value: str) -> bool | None:
    label = value.strip().lower()
    if label in TRUE_LABELS:
        return True
    if label in FALSE_LABELS:
        return False
    return None


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
    with args.input.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            parsed = normalize_label(row.get("label", ""))
            if parsed is None:
                continue
            labeled += 1
            positive += int(parsed)
            threshold = row.get("threshold", "unknown")
            bucket = by_threshold.setdefault(threshold, {"labeled": 0, "positive": 0})
            bucket["labeled"] += 1
            bucket["positive"] += int(parsed)

    metrics = {
        "status": "ok" if labeled else "pending_labels",
        "labeled_count": labeled,
        "positive_count": positive,
        "precision": round(positive / labeled, 4) if labeled else None,
        "f1": None,
        "note": "F1 is not computed without a known recall denominator.",
        "by_threshold": {
            threshold: {
                "labeled_count": values["labeled"],
                "positive_count": values["positive"],
                "precision": round(values["positive"] / values["labeled"], 4) if values["labeled"] else None,
            }
            for threshold, values in sorted(by_threshold.items())
        },
    }
    args.output.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
