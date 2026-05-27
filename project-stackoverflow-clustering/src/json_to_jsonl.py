#!/usr/bin/env python3
"""Convert the StackOverflow JSON array to JSON Lines.

The production path uses ijson when it is installed. A built-in incremental
decoder fallback is included so small local smoke tests do not need extra
packages.
"""

from __future__ import annotations

import argparse
import json
from json import JSONDecodeError
from pathlib import Path
from typing import Iterator, Any


def iter_with_ijson(input_path: Path) -> Iterator[Any]:
    import ijson  # type: ignore

    with input_path.open("rb") as fin:
        yield from ijson.items(fin, "item")


def iter_with_builtin_decoder(input_path: Path, chunk_size: int = 1024 * 1024) -> Iterator[Any]:
    decoder = json.JSONDecoder()
    buffer = ""
    pos = 0
    started = False
    eof = False

    with input_path.open("r", encoding="utf-8") as fin:
        while True:
            if not eof and len(buffer) - pos < chunk_size:
                chunk = fin.read(chunk_size)
                if chunk:
                    buffer = buffer[pos:] + chunk
                    pos = 0
                else:
                    eof = True

            while pos < len(buffer) and buffer[pos].isspace():
                pos += 1

            if not started:
                if pos >= len(buffer):
                    if eof:
                        return
                    continue
                if buffer[pos] != "[":
                    raise ValueError("Input JSON must be an array.")
                started = True
                pos += 1
                continue

            while pos < len(buffer) and buffer[pos].isspace():
                pos += 1

            if pos < len(buffer) and buffer[pos] == ",":
                pos += 1
                continue

            if pos < len(buffer) and buffer[pos] == "]":
                return

            try:
                item, next_pos = decoder.raw_decode(buffer, pos)
            except JSONDecodeError:
                if eof:
                    raise
                if pos > 0:
                    buffer = buffer[pos:]
                    pos = 0
                continue

            yield item
            pos = next_pos


def iter_questions(input_path: Path) -> Iterator[Any]:
    try:
        yield from iter_with_ijson(input_path)
    except ModuleNotFoundError:
        yield from iter_with_builtin_decoder(input_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert a JSON array to JSONL.")
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--limit", type=int, default=0, help="Optional max records for smoke tests.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with args.output.open("w", encoding="utf-8") as fout:
        for item in iter_questions(args.input):
            fout.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
            count += 1
            if args.limit and count >= args.limit:
                break

    print(f"wrote {count} records to {args.output}")


if __name__ == "__main__":
    main()
