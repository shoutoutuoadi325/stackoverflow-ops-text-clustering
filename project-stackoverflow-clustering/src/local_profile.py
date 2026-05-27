#!/usr/bin/env python3
"""Local lightweight profiling for smoke tests and report seed CSVs."""

from __future__ import annotations

import argparse
import csv
import html
import re
from collections import Counter
from pathlib import Path

from json_to_jsonl import iter_questions


TOKEN_RE = re.compile(r"[a-z0-9_#.+/-]+")
TAG_STRIP_RE = re.compile(r"<[^>]+>")


def text_of(value: object) -> str:
    return "" if value is None else str(value)


def clean_text(value: str) -> str:
    return TAG_STRIP_RE.sub(" ", html.unescape(value.lower()))


def write_rows(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(header)
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile JSON locally without Spark.")
    parser.add_argument("input", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("output/local_profile"))
    parser.add_argument("--limit", type=int, default=0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tag_counts: Counter[str] = Counter()
    token_counts: Counter[str] = Counter()
    ora_counts: Counter[str] = Counter()
    score_buckets: Counter[str] = Counter()
    length_buckets: Counter[str] = Counter()

    questions = 0
    answers = 0
    question_comments = 0
    answer_comments = 0
    unanswered = 0
    max_answers = 0
    max_score = None
    top_question = ("", "", 0)

    for item in iter_questions(args.input):
        questions += 1
        item_answers = item.get("answers") or []
        item_comments = item.get("comments") or []
        answer_count = len(item_answers)
        score = int(item.get("score") or 0)

        answers += answer_count
        question_comments += len(item_comments)
        answer_comments += sum(len(answer.get("comments") or []) for answer in item_answers)
        unanswered += int(answer_count == 0)
        max_answers = max(max_answers, answer_count)
        if max_score is None or score > max_score:
            max_score = score
            top_question = (text_of(item.get("question_id")), text_of(item.get("title")), score)

        tag_counts.update(text_of(tag) for tag in item.get("tags") or [])
        if score < 0:
            score_buckets["<0"] += 1
        elif score == 0:
            score_buckets["0"] += 1
        elif score <= 2:
            score_buckets["1-2"] += 1
        elif score <= 5:
            score_buckets["3-5"] += 1
        elif score <= 10:
            score_buckets["6-10"] += 1
        elif score <= 50:
            score_buckets["11-50"] += 1
        else:
            score_buckets[">50"] += 1

        combined = clean_text(text_of(item.get("title")) + " " + text_of(item.get("body")))
        text_len = len(combined)
        if text_len < 200:
            length_buckets["<200"] += 1
        elif text_len < 500:
            length_buckets["200-499"] += 1
        elif text_len < 1000:
            length_buckets["500-999"] += 1
        elif text_len < 2000:
            length_buckets["1000-1999"] += 1
        else:
            length_buckets[">=2000"] += 1

        tokens = TOKEN_RE.findall(combined)
        token_counts.update(t for t in tokens if len(t) >= 3 and t not in {"the", "and", "for", "with", "that", "this", "oracle"})
        ora_counts.update(t for t in tokens if re.fullmatch(r"ora-[0-9]{5}", t))

        if args.limit and questions >= args.limit:
            break

    avg_answers = round(answers / questions, 3) if questions else 0
    summary_rows = [
        ["questions", questions],
        ["answers", answers],
        ["question_comments", question_comments],
        ["answer_comments", answer_comments],
        ["avg_answers_per_question", avg_answers],
        ["max_answers_per_question", max_answers],
        ["unanswered_questions", unanswered],
        ["max_question_score", max_score or 0],
    ]
    write_rows(args.output_dir / "eda_summary.csv", ["metric", "value"], summary_rows)
    write_rows(args.output_dir / "top_tags.csv", ["tag", "count"], [[k, v] for k, v in tag_counts.most_common(100)])
    write_rows(args.output_dir / "score_distribution.csv", ["score_bucket", "count"], sorted(score_buckets.items()))
    write_rows(args.output_dir / "text_length_distribution.csv", ["length_bucket", "count"], sorted(length_buckets.items()))
    write_rows(args.output_dir / "top_terms.csv", ["token", "count"], [[k, v] for k, v in token_counts.most_common(200)])
    write_rows(args.output_dir / "top_ora_codes.csv", ["token", "count"], [[k, v] for k, v in ora_counts.most_common(100)])
    write_rows(args.output_dir / "top_question.csv", ["question_id", "title", "score"], [list(top_question)])
    print(f"profiled {questions} questions into {args.output_dir}")


if __name__ == "__main__":
    main()
