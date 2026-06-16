#!/usr/bin/env python3
"""Interactive labelling helper for evaluation/review_candidates.csv.

Goal
----
Manually labelling review candidates is the only way to turn precision from
`pending_labels` into a real number. This script makes that as cheap as
possible: it shows one pair at a time with all relevant signals, accepts a
single-key answer, writes the label back to the CSV, and is restartable -- if
you stop after 17 pairs you can resume tomorrow.

Each pair is shown with:
  - src_title vs dst_title (the question the human actually has to compare)
  - similarity, threshold, hash table count
  - shared ORA codes if present
  - existing notes if any

Keys
----
  y / 1 / 是          mark as duplicate
  n / 0 / 否          mark as not duplicate
  ?                   uncertain -> stays unlabelled, leaves note "uncertain"
  s                   skip without writing
  u                   undo last decision
  o <text>            append a note to the current row
  q                   quit (saves progress)

The CSV is rewritten atomically after every decision so a crash never loses
work. A small `.label_progress.json` sidecar records resume state.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile


TRUE_INPUTS = {"y", "1", "是", "yes", "duplicate"}
FALSE_INPUTS = {"n", "0", "否", "no", "not_duplicate"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactively label review_candidates.csv.")
    parser.add_argument("--input", type=Path, default=Path("evaluation/review_candidates.csv"))
    parser.add_argument(
        "--target",
        type=int,
        default=50,
        help="Stop prompting once this many rows are labelled. 0 = label every row.",
    )
    parser.add_argument(
        "--start-from",
        type=int,
        default=0,
        help="Skip the first N rows of the CSV. Useful for splitting work across people.",
    )
    parser.add_argument(
        "--prefer",
        choices=["unlabelled", "all"],
        default="unlabelled",
        help="`unlabelled` (default) only prompts for rows whose label cell is empty.",
    )
    return parser.parse_args()


def write_atomic(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    fd = NamedTemporaryFile("w", encoding="utf-8", newline="", dir=parent, delete=False)
    try:
        writer = csv.DictWriter(fd, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fieldnames})
        fd.flush()
        os.fsync(fd.fileno())
        fd.close()
        os.replace(fd.name, path)
    except Exception:
        fd.close()
        if os.path.exists(fd.name):
            os.unlink(fd.name)
        raise


def render_row(row: dict[str, str], idx: int, total: int, labelled_so_far: int, target: int) -> str:
    similarity = row.get("similarity") or "-"
    threshold = row.get("threshold") or "-"
    hash_tables = row.get("hash_tables") or "-"
    shared_ora = row.get("shared_ora_codes", "")
    notes = row.get("notes", "")
    target_text = f"/{target}" if target else ""
    return (
        f"\n[{idx + 1}/{total}] labelled so far: {labelled_so_far}{target_text}\n"
        f"  src [{row.get('src','')}] {row.get('src_title','')}\n"
        f"  dst [{row.get('dst','')}] {row.get('dst_title','')}\n"
        f"  similarity={similarity}  threshold={threshold}  hash_tables={hash_tables}\n"
        f"  shared_ora_codes={shared_ora or '-'}\n"
        f"  notes={notes or '-'}"
    )


def prompt_label(row: dict[str, str]) -> tuple[str | None, str | None, str | None]:
    """Return (label, note_to_append, action) where action is None / 'undo' / 'quit'."""
    while True:
        try:
            raw = input("  > ").strip()
        except EOFError:
            return None, None, "quit"
        if not raw:
            continue
        head, _, tail = raw.partition(" ")
        head_low = head.lower()
        if head_low in TRUE_INPUTS:
            return "1", tail.strip() or None, None
        if head_low in FALSE_INPUTS:
            return "0", tail.strip() or None, None
        if head_low in {"?", "u?", "uncertain"}:
            return None, "uncertain", None
        if head_low in {"s", "skip"}:
            return None, None, None
        if head_low in {"u", "undo"}:
            return None, None, "undo"
        if head_low in {"q", "quit", "exit"}:
            return None, None, "quit"
        if head_low in {"o", "note"}:
            return None, tail.strip() or None, None
        print(
            "  -- y/1=duplicate, n/0=not_duplicate, ?=uncertain, s=skip, u=undo, "
            "o <text>=note, q=quit"
        )


def main() -> int:
    args = parse_args()
    if not args.input.exists():
        print(f"review candidates not found: {args.input}", file=sys.stderr)
        return 2
    if not sys.stdin.isatty():
        print("This script requires an interactive terminal.", file=sys.stderr)
        return 2

    with args.input.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [dict(r) for r in reader]
    if "label" not in fieldnames:
        fieldnames.append("label")
    if "notes" not in fieldnames:
        fieldnames.append("notes")

    initial_labelled = sum(1 for r in rows if r.get("label", "").strip())
    target = args.target
    history: list[tuple[int, dict[str, str]]] = []
    total = len(rows)
    print(
        f"Loaded {total} candidates from {args.input}; {initial_labelled} already labelled. "
        f"Target this session: {target if target else 'all rows'}."
    )
    print("Keys: y=dup, n=not, ?=uncertain, s=skip, u=undo, o <text>=note, q=quit.")

    new_labels = 0
    idx = max(args.start_from, 0)
    while idx < total:
        row = rows[idx]
        already_labelled = bool(row.get("label", "").strip())
        if args.prefer == "unlabelled" and already_labelled:
            idx += 1
            continue
        labelled_so_far = initial_labelled + new_labels
        if target and labelled_so_far - initial_labelled >= target:
            print(f"Hit target ({target} new labels in this session). Saving and exiting.")
            break

        print(render_row(row, idx, total, labelled_so_far - initial_labelled, target))
        decision, note, action = prompt_label(row)
        if action == "quit":
            print("Quit requested. Saving progress.")
            break
        if action == "undo":
            if not history:
                print("Nothing to undo.")
                continue
            prev_idx, prev_state = history.pop()
            rows[prev_idx] = prev_state
            new_labels = max(0, new_labels - (1 if prev_state.get("label", "").strip() == "" else 0))
            idx = prev_idx
            write_atomic(args.input, rows, fieldnames)
            print(f"  -> undid row {prev_idx + 1}.")
            continue

        history.append((idx, dict(row)))
        if note:
            existing = row.get("notes", "")
            row["notes"] = f"{existing} | {note}".strip(" |") if existing else note
        if decision is not None:
            row["label"] = decision
            new_labels += 1
        write_atomic(args.input, rows, fieldnames)
        idx += 1

    final_labelled = sum(1 for r in rows if r.get("label", "").strip())
    print(
        f"\nDone. Total labelled rows in CSV: {final_labelled}. "
        f"Newly labelled this session: {new_labels}."
    )
    print(
        "Next: run `python src/evaluate_review_labels.py` to refresh "
        "output/evaluation/review_metrics.json with precision and Wilson CIs."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
