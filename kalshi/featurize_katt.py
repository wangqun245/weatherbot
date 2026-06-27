from __future__ import annotations

import argparse
from bisect import bisect_left
import csv
import json
import math
import re
import sys
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from featurize_metar_history import (  # noqa: E402
    BASE_FEATURE_COLUMNS,
    MetarRow,
    blank,
    decode_metar,
)


DEFAULT_INPUT = Path(__file__).resolve().parent / "data" / "KATT_nws_lst_daily_high.csv"
DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent
    / "data"
    / "KATT_nws_lst_daily_high_features_asos_context.csv"
)
NWS_LST = timezone(timedelta(hours=-6), name="CST")
MAX_6H_RE = re.compile(r"(?:^|\s)1([01])(\d{3})(?=\s|$)")
MIN_6H_RE = re.compile(r"(?:^|\s)2([01])(\d{3})(?=\s|$)")

REMOVED_POLYMARKET_CONTEXT = {
    column
    for column in BASE_FEATURE_COLUMNS
    if column == "is_extra_metar_report"
    or column.startswith("metar_obs_")
    or column.startswith("extra_metar_")
    or column.startswith("has_extra_metar_")
    or column.startswith("temp_f_change_from_")
}
BASE_COLUMNS = [
    column
    for column in BASE_FEATURE_COLUMNS
    if column not in REMOVED_POLYMARKET_CONTEXT
]
LATEST_ASOS_CONTEXT_COLUMNS = [
    "asos_6h_max_temp_f",
    "asos_6h_min_temp_f",
    "asos_6h_temp_range_f",
    "asos_6h_extrema_age_minutes",
    "asos_6h_max_minus_current_temp_f",
    "current_temp_minus_asos_6h_min_f",
    "has_asos_6h_extrema_context",
]
PREVIOUS_ASOS_CONTEXT_COLUMNS = [
    "asos_previous_6h_max_temp_f",
    "asos_previous_6h_min_temp_f",
    "asos_previous_6h_temp_range_f",
    "asos_previous_6h_extrema_age_minutes",
    "asos_previous_6h_max_minus_current_temp_f",
    "current_temp_minus_asos_previous_6h_min_f",
    "has_asos_previous_6h_extrema_context",
]
THIRD_ASOS_CONTEXT_COLUMNS = [
    "asos_third_6h_max_temp_f",
    "asos_third_6h_min_temp_f",
    "asos_third_6h_temp_range_f",
    "asos_third_6h_extrema_age_minutes",
    "asos_third_6h_max_minus_current_temp_f",
    "current_temp_minus_asos_third_6h_min_f",
    "has_asos_third_6h_extrema_context",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create KATT LightGBM features with ASOS six-hour extrema context."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--context-max-age-minutes",
        type=int,
        default=390,
        help="Forward-fill the latest 6-hour extrema for at most this many minutes.",
    )
    parser.add_argument("--lag-tolerance-minutes", type=int, default=30)
    parser.add_argument("--max-lag-hours", type=int, default=10)
    parser.add_argument("--max-asos-contexts", type=int, choices=(1, 2, 3), default=2)
    return parser.parse_args()


def signed_tenths_f(sign: str, digits: str) -> float:
    value_c = int(digits) / 10.0
    if sign == "1":
        value_c = -value_c
    return value_c * 9.0 / 5.0 + 32.0


def parse_six_hour_extrema(metar: str) -> tuple[float | None, float | None]:
    max_match = MAX_6H_RE.search(metar)
    min_match = MIN_6H_RE.search(metar)
    max_f = (
        signed_tenths_f(max_match.group(1), max_match.group(2))
        if max_match
        else None
    )
    min_f = (
        signed_tenths_f(min_match.group(1), min_match.group(2))
        if min_match
        else None
    )
    return max_f, min_f


def precise_temp_f(metar: str) -> float | None:
    match = re.search(r"(?:^|\s)T([01])(\d{3})[01]\d{3}(?=\s|$)", metar)
    if match is None:
        return None
    return signed_tenths_f(match.group(1), match.group(2))


def nearest_lag_temp(
    target: datetime,
    valid_times: list[datetime],
    temp_values: list[float | None],
    tolerance: timedelta,
) -> float | None:
    index = bisect_left(valid_times, target)
    candidates = []
    if index < len(valid_times):
        candidates.append(index)
    if index > 0:
        candidates.append(index - 1)
    best_value = None
    best_delta = None
    for candidate in candidates:
        value = temp_values[candidate]
        if value is None:
            continue
        delta = abs(valid_times[candidate] - target)
        if delta <= tolerance and (best_delta is None or delta < best_delta):
            best_value = value
            best_delta = delta
    return best_value


def read_rows(path: Path) -> list[MetarRow]:
    rows: list[MetarRow] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        for source in csv.DictReader(handle):
            valid_utc = datetime.strptime(source["valid"], "%Y-%m-%d %H:%M").replace(
                tzinfo=timezone.utc
            )
            rows.append(
                MetarRow(
                    daily_high_f=source["daily_high_f"],
                    station=source["station"],
                    valid_utc=valid_utc,
                    valid_text=source["valid"],
                    metar=source["metar"],
                )
            )
    return rows


def main() -> int:
    args = parse_args()
    lag_columns = [
        column
        for hours in range(1, args.max_lag_hours + 1)
        for column in (f"temp_f_lag_{hours}h", f"temp_f_change_{hours}h")
    ]
    asos_context_columns = [
        *LATEST_ASOS_CONTEXT_COLUMNS,
        *(
            PREVIOUS_ASOS_CONTEXT_COLUMNS
            if args.max_asos_contexts >= 2
            else []
        ),
        *(THIRD_ASOS_CONTEXT_COLUMNS if args.max_asos_contexts >= 3 else []),
    ]
    output_columns = [
        "daily_high_f",
        "station",
        "valid",
        "metar",
        *BASE_COLUMNS,
        *lag_columns,
        *asos_context_columns,
    ]
    rows = read_rows(args.input)
    if not rows:
        raise SystemExit(f"No processed KATT rows found in {args.input}")

    args.output.parent.mkdir(parents=True, exist_ok=True)
    stats: Counter = Counter(input_rows=len(rows))
    valid_times = [row.valid_utc for row in rows]
    temp_values = [precise_temp_f(row.metar) for row in rows]
    lag_tolerance = timedelta(minutes=args.lag_tolerance_minutes)
    extrema_history: deque[tuple[datetime, float, float]] = deque(
        maxlen=args.max_asos_contexts
    )

    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=output_columns, lineterminator="\n")
        writer.writeheader()

        for row in rows:
            decoded = decode_metar(row, "KATT", NWS_LST)
            decoded["station_id"] = 1
            current_temp_f = decoded.get("temp_f")
            for hours in range(1, args.max_lag_hours + 1):
                lag_value = nearest_lag_temp(
                    row.valid_utc - timedelta(hours=hours),
                    valid_times,
                    temp_values,
                    lag_tolerance,
                )
                decoded[f"temp_f_lag_{hours}h"] = lag_value
                decoded[f"temp_f_change_{hours}h"] = (
                    None
                    if current_temp_f is None or lag_value is None
                    else float(current_temp_f) - lag_value
                )
                if lag_value is None:
                    stats[f"rows_missing_temp_lag_{hours}h"] += 1
            parsed_max_f, parsed_min_f = parse_six_hour_extrema(row.metar)
            if parsed_max_f is not None and parsed_min_f is not None:
                extrema_history.append((row.valid_utc, parsed_max_f, parsed_min_f))
                stats["rows_with_new_asos_6h_extrema"] += 1

            temp_f = decoded.get("temp_f")
            latest = extrema_history[-1] if extrema_history else None
            previous = extrema_history[-2] if len(extrema_history) >= 2 else None
            third = extrema_history[-3] if len(extrema_history) >= 3 else None

            if latest is not None:
                latest_time, latest_max_f, latest_min_f = latest
                age_minutes = (row.valid_utc - latest_time).total_seconds() / 60.0
            else:
                latest_max_f = latest_min_f = age_minutes = None
            latest_valid = (
                age_minutes is not None
                and 0 <= age_minutes <= args.context_max_age_minutes
            )
            if latest_valid:
                decoded.update(
                    {
                        "asos_6h_max_temp_f": latest_max_f,
                        "asos_6h_min_temp_f": latest_min_f,
                        "asos_6h_temp_range_f": latest_max_f - latest_min_f,
                        "asos_6h_extrema_age_minutes": age_minutes,
                        "asos_6h_max_minus_current_temp_f": (
                            None if temp_f is None else latest_max_f - float(temp_f)
                        ),
                        "current_temp_minus_asos_6h_min_f": (
                            None if temp_f is None else float(temp_f) - latest_min_f
                        ),
                        "has_asos_6h_extrema_context": 1,
                    }
                )
                stats["rows_with_asos_6h_context"] += 1
            else:
                decoded["has_asos_6h_extrema_context"] = 0

            if args.max_asos_contexts >= 2 and previous is not None:
                previous_time, previous_max_f, previous_min_f = previous
                previous_age_minutes = (
                    row.valid_utc - previous_time
                ).total_seconds() / 60.0
            else:
                previous_max_f = previous_min_f = previous_age_minutes = None
            previous_valid = args.max_asos_contexts >= 2 and (
                previous_age_minutes is not None
                and 0 <= previous_age_minutes <= args.context_max_age_minutes * 2
            )
            if previous_valid:
                decoded.update(
                    {
                        "asos_previous_6h_max_temp_f": previous_max_f,
                        "asos_previous_6h_min_temp_f": previous_min_f,
                        "asos_previous_6h_temp_range_f": previous_max_f - previous_min_f,
                        "asos_previous_6h_extrema_age_minutes": previous_age_minutes,
                        "asos_previous_6h_max_minus_current_temp_f": (
                            None
                            if temp_f is None
                            else previous_max_f - float(temp_f)
                        ),
                        "current_temp_minus_asos_previous_6h_min_f": (
                            None
                            if temp_f is None
                            else float(temp_f) - previous_min_f
                        ),
                        "has_asos_previous_6h_extrema_context": 1,
                    }
                )
                stats["rows_with_previous_asos_6h_context"] += 1
            else:
                decoded["has_asos_previous_6h_extrema_context"] = 0

            if args.max_asos_contexts >= 3 and third is not None:
                third_time, third_max_f, third_min_f = third
                third_age_minutes = (
                    row.valid_utc - third_time
                ).total_seconds() / 60.0
            else:
                third_max_f = third_min_f = third_age_minutes = None
            third_valid = args.max_asos_contexts >= 3 and (
                third_age_minutes is not None
                and 0 <= third_age_minutes <= args.context_max_age_minutes * 3
            )
            if third_valid:
                decoded.update(
                    {
                        "asos_third_6h_max_temp_f": third_max_f,
                        "asos_third_6h_min_temp_f": third_min_f,
                        "asos_third_6h_temp_range_f": third_max_f - third_min_f,
                        "asos_third_6h_extrema_age_minutes": third_age_minutes,
                        "asos_third_6h_max_minus_current_temp_f": (
                            None if temp_f is None else third_max_f - float(temp_f)
                        ),
                        "current_temp_minus_asos_third_6h_min_f": (
                            None if temp_f is None else float(temp_f) - third_min_f
                        ),
                        "has_asos_third_6h_extrema_context": 1,
                    }
                )
                stats["rows_with_third_asos_6h_context"] += 1
            else:
                decoded["has_asos_third_6h_extrema_context"] = 0

            output_row = {
                "daily_high_f": row.daily_high_f,
                "station": row.station,
                "valid": row.valid_text,
                "metar": row.metar,
            }
            output_row.update(
                {
                    column: blank(decoded.get(column))
                    for column in BASE_COLUMNS + lag_columns + asos_context_columns
                }
            )
            writer.writerow(output_row)
            stats["output_rows"] += 1

    stats["removed_polymarket_context_features"] = len(REMOVED_POLYMARKET_CONTEXT)
    stats["feature_columns"] = (
        len(BASE_COLUMNS) + len(lag_columns) + len(asos_context_columns)
    )
    stats["lag_hours"] = args.max_lag_hours
    stats["max_asos_contexts"] = args.max_asos_contexts
    stats["lag_tolerance_minutes"] = args.lag_tolerance_minutes
    stats["context_max_age_minutes"] = args.context_max_age_minutes
    stats["timezone"] = "fixed CST (UTC-06:00), matching NWS LST climate day"
    stats["input_file"] = str(args.input)
    stats["output_file"] = str(args.output)
    summary_path = args.output.with_name("feature_summary.json")
    summary_path.write_text(
        json.dumps(dict(stats), indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"Wrote {stats['output_rows']:,} feature rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
