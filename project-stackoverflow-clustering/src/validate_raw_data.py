#!/usr/bin/env python3
"""Validate raw StackOverflow JSON data and write traceable metrics."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from json_to_jsonl import iter_questions


def count_raw_records(input_path: Path) -> dict[str, Any]:
    questions = 0
    answers = 0
    question_comments = 0
    answer_comments = 0
    unanswered_questions = 0
    max_answers_per_question = 0
    max_question_score = None
    max_question_id = None
    top_tags: dict[str, int] = {}

    for item in iter_questions(input_path):
        questions += 1
        item_answers = item.get("answers") or []
        item_comments = item.get("comments") or []
        item_tags = item.get("tags") or []
        item_score = item.get("score") or 0

        answers += len(item_answers)
        question_comments += len(item_comments)
        answer_comments += sum(len(answer.get("comments") or []) for answer in item_answers)
        if not item_answers:
            unanswered_questions += 1
        max_answers_per_question = max(max_answers_per_question, len(item_answers))
        if max_question_score is None or item_score > max_question_score:
            max_question_score = item_score
            max_question_id = item.get("question_id")
        for tag in item_tags:
            top_tags[str(tag)] = top_tags.get(str(tag), 0) + 1

    avg_answers = round(answers / questions, 3) if questions else 0.0
    return {
        "questions": questions,
        "answers": answers,
        "question_comments": question_comments,
        "answer_comments": answer_comments,
        "avg_answers_per_question": avg_answers,
        "max_answers_per_question": max_answers_per_question,
        "unanswered_questions": unanswered_questions,
        "max_question_score": max_question_score or 0,
        "max_question_id": str(max_question_id or ""),
        "top_tags": [
            {"tag": tag, "count": count}
            for tag, count in sorted(top_tags.items(), key=lambda item: (-item[1], item[0]))[:20]
        ],
    }


def sha256_file(input_path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with input_path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def write_summary_csv(path: Path, validation: dict[str, Any]) -> None:
    metrics = validation["metrics"]
    rows = [
        ("source_path", validation["source_path"]),
        ("file_size_bytes", validation["file_size_bytes"]),
        ("sha256", validation["sha256"]),
        ("validated_at_utc", validation["validated_at_utc"]),
        ("questions", metrics["questions"]),
        ("answers", metrics["answers"]),
        ("question_comments", metrics["question_comments"]),
        ("answer_comments", metrics["answer_comments"]),
        ("avg_answers_per_question", metrics["avg_answers_per_question"]),
        ("max_answers_per_question", metrics["max_answers_per_question"]),
        ("unanswered_questions", metrics["unanswered_questions"]),
        ("max_question_score", metrics["max_question_score"]),
        ("max_question_id", metrics["max_question_id"]),
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate raw StackOverflow JSON data.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("../StackOverFlow_Oracle_Database/oracle_database_questions.json"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("output/validation"))
    args = parser.parse_args()

    input_path = args.input.resolve()
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    validation = {
        "source_path": str(input_path),
        "file_size_bytes": input_path.stat().st_size,
        "sha256": sha256_file(input_path),
        "validated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "metrics": count_raw_records(input_path),
    }

    json_path = args.output_dir / "raw_data_validation.json"
    csv_path = args.output_dir / "raw_data_validation.csv"
    json_path.write_text(json.dumps(validation, ensure_ascii=False, indent=2), encoding="utf-8")
    write_summary_csv(csv_path, validation)
    print(f"Wrote {json_path}")
    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()
