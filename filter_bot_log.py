#!/usr/bin/env python3
"""Stream-filter large weatherbot logs without loading them into memory."""

from __future__ import annotations

import argparse
import gzip
import re
from pathlib import Path
from typing import Iterable, TextIO


DIAGNOSTIC_TERMS = (
    "weatherrecord supervisor",
    "event cache refreshed",
    "timing refresh",
    "timing unavailable",
    "weatherrecord poll",
    "weatherrecord extreme",
    "clob momentum candidate",
    "clob momentum baseline",
    "clob momentum trigger",
    "price momentum",
    "tgftp window",
    "metar buy",
    "skip_buy",
    "order submitted",
    "order filled",
    "submit_buy",
    "ERROR",
    "CRITICAL",
    "Traceback",
    "Exception",
    "failed",
    "timeout",
    "timed out",
    "429",
)

TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})")


def open_log(path: Path) -> TextIO:
    if path.suffix.lower() == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def line_in_time_range(line: str, target_date: str, start: str, end: str) -> bool:
    match = TIMESTAMP_RE.match(line)
    if not match:
        return False
    line_date, line_time = match.groups()
    if target_date and line_date != target_date:
        return False
    if start and line_time < start:
        return False
    if end and line_time > end:
        return False
    return True


def subject_matches(line: str, subjects: Iterable[str]) -> bool:
    lowered = line.lower()
    return any(subject.lower() in lowered for subject in subjects if subject)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Input .log or .log.gz file")
    parser.add_argument("--output", type=Path, help="Output path; stdout when omitted")
    parser.add_argument("--date", default="", help="Date in YYYY-MM-DD")
    parser.add_argument("--start", default="", help="Start time HH:MM:SS")
    parser.add_argument("--end", default="", help="End time HH:MM:SS")
    parser.add_argument("--city", default="", help="City text to match")
    parser.add_argument("--station", default="", help="Station code to match")
    parser.add_argument("--all-matches", action="store_true", help="Keep every matching subject line")
    parser.add_argument("--include-global-errors", action="store_true", help="Also keep ERROR/Traceback lines in the selected time range")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    subjects = [args.city, args.station]
    if not any(subjects):
        raise SystemExit("Provide --city and/or --station")

    output: TextIO
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        output = args.output.open("w", encoding="utf-8", newline="")
    else:
        import sys

        output = sys.stdout

    matched = 0
    scanned = 0
    try:
        with open_log(args.input) as source:
            for line in source:
                scanned += 1
                if not line_in_time_range(line, args.date, args.start, args.end):
                    continue
                subject_hit = subject_matches(line, subjects)
                diagnostic_hit = any(term.lower() in line.lower() for term in DIAGNOSTIC_TERMS)
                global_error = args.include_global_errors and any(
                    marker in line for marker in (" ERROR ", " CRITICAL ", "Traceback", "Exception")
                )
                if global_error or (subject_hit and (args.all_matches or diagnostic_hit)):
                    output.write(line)
                    matched += 1
    finally:
        if args.output:
            output.close()

    print(f"scanned={scanned} matched={matched} output={args.output or 'stdout'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
